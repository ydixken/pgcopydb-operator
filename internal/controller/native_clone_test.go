package controller

import (
	"context"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type nativeCloneFinisher interface {
	finishNativeClone(context.Context, *v1beta1.Migration, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (ctrl.Result, error)
}

func nativeCloneFinish(t *testing.T, f *nativeCloneFixture) (ctrl.Result, error) {
	t.Helper()
	consumer, ok := any(f.r).(nativeCloneFinisher)
	if !ok {
		t.Fatal("plain clone completion is missing")
	}
	return consumer.finishNativeClone(context.Background(), f.m, f.m.DeepCopy(), f.worker, f.pvc)
}

func (f *nativeCloneFixture) current() *v1beta1.Migration {
	f.t.Helper()
	m := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), m); err != nil {
		f.t.Fatal(err)
	}
	return m
}

func TestNativeCloneOnlyCompletion(t *testing.T) {
	t.Run("report and original success suffice without follow authority", nativeCloneSuccess)
	t.Run("follow worker is refused before capture", nativeCloneFollowRefusal)
	t.Run("requested profile is freshly checked before Complete", nativeCloneRequestedProfile)
	for _, check := range []string{compareSchema, compareData} {
		t.Run("checked "+check+" mismatch reports warning and completes transfer", func(t *testing.T) {
			f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: check == compareSchema, Data: check == compareData})
			nativeInstallCompare(t, f, check, false)
			recorder := events.NewFakeRecorder(10)
			f.r.Recorder = recorder
			if _, err := nativeCloneFinish(t, f); err != nil {
				t.Fatal(err)
			}
			got := f.current()
			if got.Status.Phase != v1beta1.PhaseCompleted || !meta.IsStatusConditionFalse(got.Status.Conditions, v1beta1.ConditionVerified) || len(got.Status.Verification) != 1 || got.Status.Verification[0].Passed || got.Status.Verification[0].CompletedAt == nil {
				t.Fatal("checked mismatch was not a completed negative verification")
			}
			warning := false
			for len(recorder.Events) > 0 {
				if strings.Contains(<-recorder.Events, "Warning VerificationMismatch") {
					warning = true
				}
			}
			if !warning {
				t.Fatal("checked mismatch did not emit its warning event")
			}
		})
	}
	t.Run("new helper receipt returns without completion or second status patch", nativeCloneReceiptReturn)
	t.Run("historical Passed cannot replace current capture", nativeCloneHistoricalResult)
	t.Run("invalid report never publishes Complete", nativeCloneInvalidReport)
	t.Run("status conflict retains winning E and receipts", nativeCloneStatusConflict)
}

func nativeCloneSuccess(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	result, err := nativeCloneFinish(t, f)
	got := f.current()
	if err != nil || result != (ctrl.Result{}) || got.Status.Phase != v1beta1.PhaseCompleted || got.Status.CompletedAt == nil || !meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionComplete) || !meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionCloneCompleted) {
		t.Fatalf("phase=%s result=%v err=%v", got.Status.Phase, result, err)
	}
	if got.Status.NativeJobUID != string(f.worker.UID) || got.Status.NativePVCUID != string(f.pvc.UID) || got.Status.JobName != f.worker.Name || got.Status.Attempts != 1 || got.Status.Replication != nil || f.logs != 1 || f.patches != 1 {
		t.Fatal("plain clone changed execution receipts or consumed follow authority")
	}
}

func nativeCloneFollowRefusal(t *testing.T) {
	f := newNativeCloneFixture(t, true, nil)
	if _, err := nativeCloneFinish(t, f); err == nil || f.logs != 0 || f.patches != 0 {
		t.Fatal("follow reached plain clone completion")
	}
}

func nativeCloneRequestedProfile(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true, Data: true})
	nativeInstallCompare(t, f, compareSchema, true)
	nativeInstallCompare(t, f, compareData, true)
	if _, err := nativeCloneFinish(t, f); err != nil {
		t.Fatal(err)
	}
	got := f.current()
	if got.Status.Phase != v1beta1.PhaseCompleted || !meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionVerified) || len(got.Status.Verification) != 2 || f.logs != 3 || got.Status.NativeSchemaJobUID != "native-clone-schema-helper" || got.Status.NativeDataJobUID != "native-clone-data-helper" {
		t.Fatal("Complete did not retain the freshly checked profile")
	}
}

func nativeCloneReceiptReturn(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true})
	f.onCreate = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		obj.SetUID(nativeKnownSchemaHelper)
		return c.Create(ctx, obj, opts...)
	}
	result, err := nativeCloneFinish(t, f)
	got := f.current()
	if err != nil || result.RequeueAfter != pollInterval || f.patches != 1 || f.logs != 1 || got.Status.NativeSchemaJobUID != nativeKnownSchemaHelper || got.Status.Phase != v1beta1.PhaseVerifying || got.Status.CompletedAt != nil || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionComplete) {
		t.Fatalf("receipt persistence fell through: result=%v err=%v", result, err)
	}
}

func nativeCloneHistoricalResult(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Data: true})
	nativeInstallCompare(t, f, compareData, true)
	f.m.Status.Verification = []v1beta1.VerificationResult{{Check: compareData, Passed: true}}
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.outputs = nil
	if _, err := nativeCloneFinish(t, f); err == nil || f.patches != 0 || f.current().Status.Phase == v1beta1.PhaseCompleted {
		t.Fatal("historical verification completed transfer without current checked output")
	}
}

func nativeCloneInvalidReport(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	f.report.Copy.Tables.Done = nil
	f.encodeReport()
	if _, err := nativeCloneFinish(t, f); err == nil || f.patches != 0 || meta.IsStatusConditionTrue(f.current().Status.Conditions, v1beta1.ConditionComplete) {
		t.Fatal("missing counters reached Complete")
	}
}

func nativeCloneStatusConflict(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	f.patch = func(ctx context.Context, c client.Client, _ client.Object, _ client.Patch) error {
		winner := f.current()
		winner.Status.Replication = &v1beta1.ReplicationStatus{Endpos: "0/90"}
		if err := c.Status().Update(ctx, winner); err != nil {
			t.Fatal(err)
		}
		return nativeControlConflict()
	}
	if _, err := nativeCloneFinish(t, f); !apierrors.IsConflict(err) {
		t.Fatalf("status conflict was lost: %v", err)
	}
	got := f.current()
	if got.Status.Phase == v1beta1.PhaseCompleted || got.Status.CompletedAt != nil || got.Status.Replication.Endpos != "0/90" || got.Status.NativeJobUID != string(f.worker.UID) || got.Status.NativePVCUID != string(f.pvc.UID) {
		t.Fatal("conflict overwrote winning state")
	}
}
