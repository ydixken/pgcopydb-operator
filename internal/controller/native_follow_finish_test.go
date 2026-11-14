package controller

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type nativeFollowFinishConsumer interface {
	finishNativeFollow(context.Context, *v1beta1.Migration, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (ctrl.Result, error)
}

func nativeFinish(t *testing.T, f *nativeFinishFixture) (ctrl.Result, error) {
	t.Helper()
	consumer, ok := any(f.r).(nativeFollowFinishConsumer)
	if !ok {
		t.Fatal("native follow finalization is missing")
	}
	return consumer.finishNativeFollow(context.Background(), f.m, f.m.DeepCopy(), f.worker, f.pvc)
}

func assertNativeFinished(t *testing.T, f *nativeFinishFixture) {
	t.Helper()
	stored := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != v1beta1.PhaseCompleted || !meta.IsStatusConditionTrue(stored.Status.Conditions, v1beta1.ConditionComplete) || nativeControlEnd(stored) != lifecycleEnd || stored.Status.NativeJobUID != string(f.worker.UID) || stored.Status.NativePVCUID != string(f.pvc.UID) {
		t.Fatalf("final status lacks completion or original receipts: %+v", stored.Status)
	}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.worker), &batchv1.Job{}); err != nil {
		t.Fatal("completion lost retained original")
	}
}

func TestNativeFollowCleanupRecovery(t *testing.T) {
	t.Run("normal target verification cleanup and final proof", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		f.laterBegin = true
		if result, err := nativeFinish(t, f); err != nil || result.RequeueAfter != 0 {
			t.Fatalf("finish: %v %+v steps=%v", err, result, f.steps)
		}
		assertNativeFinished(t, f)
		active, cleanup, cleaned := slices.Index(f.steps, finishOrdinaryEvidenceKind), slices.Index(f.steps, jobKindCleanup), -1
		for i, step := range f.steps {
			if step == finishCleanedEvidenceKind {
				cleaned = i
			}
		}
		if active < 0 || cleanup <= active || cleaned <= cleanup {
			t.Fatalf("wrong authority/cleanup order: %v", f.steps)
		}
	})
	t.Run("new invocation after actual cleanup and final status conflict", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		f.patch = func(context.Context, client.Client, client.Object, client.Patch) error {
			return nativeControlConflict()
		}
		if _, err := nativeFinish(t, f); !apierrors.IsConflict(err) || !f.cleaned {
			t.Fatalf("expected conflict after cleanup: %v steps=%v", err, f.steps)
		}
		stored := &v1beta1.Migration{}
		if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored); err != nil {
			t.Fatal(err)
		}
		f.m = stored
		f.patch = nil
		f.steps = nil
		if _, err := nativeFinish(t, f); err != nil {
			t.Fatalf("fresh cleanup rejoin: %v steps=%v", err, f.steps)
		}
		assertNativeFinished(t, f)
		if slices.Contains(f.steps, jobKindCleanup) || slices.Contains(f.steps, finishOrdinaryEvidenceKind) {
			t.Fatalf("cleaned rejoin used active route: %v", f.steps)
		}
	})
	t.Run("unknown cleanup Create effect is not adopted", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		f.fault = "unknown cleanup Create"
		if _, err := nativeFinish(t, f); err == nil || !f.cleaned {
			t.Fatalf("unknown Create was hidden: %v", err)
		}
		if f.patches != 0 {
			t.Fatal("unknown Create published completion")
		}
		f.fault = ""
		f.steps = nil
		if _, err := nativeFinish(t, f); err != nil {
			t.Fatalf("fresh target proof cannot rejoin known sealed cleanup: %v", err)
		}
		assertNativeFinished(t, f)
		if slices.Contains(f.steps, jobKindCleanup) {
			t.Fatal("unknown cleanup effect was replayed")
		}
	})
}

func TestNativeFollowVerificationBeforeCleanup(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	result, err := nativeFinish(t, f)
	if err != nil || result.RequeueAfter == 0 || f.m.Status.NativeSchemaJobUID == "" {
		t.Fatalf("new verification receipt must return immediately: %v %+v created=%v steps=%v patches=%d", err, result, f.created, f.steps, f.patches)
	}
	if f.cleaned || slices.Contains(f.steps, jobKindCleanup) {
		t.Fatal("verification receipt patch fell through to cleanup")
	}
	if _, err := nativeFinish(t, f); err != nil {
		t.Fatalf("known helper recapture: %v", err)
	}
	assertNativeFinished(t, f)
}

func TestNativeFollowFinishRefusals(t *testing.T) {
	for _, fault := range []string{"wrong cleanup resource", "cleanup noise", finishProbeActiveFault, finishWrongEndFault, finishWrongWitnessFault, finishUnknownCommitFault} {
		t.Run(fault, func(t *testing.T) {
			f := newNativeFinishFixture(t)
			f.fault = fault
			if fault == finishWrongEndFault || fault == finishWrongWitnessFault || fault == finishUnknownCommitFault {
				f.cleaned = true
			}
			f.editResult = func(kind string, v map[string]any) {
				if kind != finishCleanedEvidenceKind {
					return
				}
				cp := v["checkpoint"].(map[string]any)
				switch fault {
				case finishWrongEndFault:
					cp["drain_end_lsn"] = lifecycleChangedEnd
				case finishWrongWitnessFault:
					cp["drain_replay_lsn"] = nativeTerminalEarlierLSN
				case finishUnknownCommitFault:
					v["target_closed"] = false
				}
			}
			if _, err := nativeFinish(t, f); err == nil {
				t.Fatal("unknown cleanup or completion became successful")
			}
			if f.patches != 0 {
				t.Fatal("refused authority patched completion")
			}
			if fault == finishProbeActiveFault && slices.Contains(f.steps, jobKindCleanup) {
				t.Fatal("running failed proof helper allowed cleanup")
			}
		})
	}
	t.Run("worker Pod replacement between report and proof", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		f.afterLogs = func() {
			if f.logs == 1 {
				f.pod.UID = "replacement-pod"
				f.replace(f.pod)
			}
		}
		if _, err := nativeFinish(t, f); err == nil || errors.Is(err, context.Canceled) {
			t.Fatal("lost original Pod binding")
		}
		if f.cleaned || f.patches != 0 {
			t.Fatal("replaced original authorized effects")
		}
	})
}

func TestNativeFollowFinishStaleStatus(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.cleaned = true
	f.m.Status.Phase = v1beta1.PhaseFailed
	f.r.setCondition(f.m, v1beta1.ConditionFailed, metav1.ConditionTrue, "StaleFailure", "Previous display state")
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeFinish(t, f); err != nil {
		t.Fatalf("exact cleaned proof should recover stale display status: %v", err)
	}
	assertNativeFinished(t, f)
}

func TestNativeFollowCompletionKeepsDataProgress(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.cleaned = true
	f.m.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	nativeInstallCompare(t, f.nativeCloneFixture, compareData, true)
	if _, err := nativeFinish(t, f); err != nil {
		t.Fatalf("checked verification: %v", err)
	}
	assertNativeFinished(t, f)
	stored := f.current()
	if !meta.IsStatusConditionTrue(stored.Status.Conditions, v1beta1.ConditionVerified) || len(stored.Status.Verification) != 1 || !stored.Status.Verification[0].Passed {
		t.Fatal("checked data verification was lost")
	}
	expectNativeDataProgress(t, stored, true, true)
}

func TestNativeFollowVerificationRecapture(t *testing.T) {
	for _, matched := range []bool{true, false} {
		t.Run(map[bool]string{true: "match", false: "checked mismatch"}[matched], func(t *testing.T) {
			f := newNativeFinishFixture(t)
			f.cleaned = true
			f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			nativeInstallCompare(t, f.nativeCloneFixture, compareSchema, matched)
			if _, err := nativeFinish(t, f); err != nil {
				t.Fatalf("checked verification: %v", err)
			}
			assertNativeFinished(t, f)
			stored := f.current()
			if meta.IsStatusConditionTrue(stored.Status.Conditions, v1beta1.ConditionVerified) != matched || len(stored.Status.Verification) != 1 || stored.Status.Verification[0].Passed != matched {
				t.Fatal("checked mismatch was lost or became an operational failure")
			}
			if !slices.Contains(f.deleted, finishVerificationJobKind) {
				t.Fatal("consumed pinned comparison helper was not disposed after durable status")
			}
		})
	}
	for _, fault := range []string{"cached Passed with bad output", "replaced pinned helper"} {
		t.Run(fault, func(t *testing.T) {
			f := newNativeFinishFixture(t)
			f.cleaned = true
			f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			job, pod := nativeInstallCompare(t, f.nativeCloneFixture, compareSchema, true)
			f.m.Status.Verification = []v1beta1.VerificationResult{{Check: compareSchema, Passed: true}}
			if err := f.store.Status().Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if fault == "cached Passed with bad output" {
				f.outputs[pod.Name] = []byte("unknown\n")
			} else {
				job.UID = "replacement-comparison"
				f.replace(job)
			}
			if _, err := nativeFinish(t, f); err == nil {
				t.Fatal("cached Passed or replacement helper supplied comparison authority")
			}
			if f.patches != 0 || slices.Contains(f.deleted, finishVerificationJobKind) {
				t.Fatal("unconsumed comparison was disposed or marked Complete")
			}
		})
	}
}

func TestNativeFollowCleanupRejoin(t *testing.T) {
	for _, state := range []string{"intent only", finishSlotRemovedCase, "publication removed", "absent slot before flag"} {
		t.Run(state, func(t *testing.T) {
			f := newNativeFinishFixture(t)
			f.partial = state
			f.laterBegin = true
			if _, err := nativeFinish(t, f); err != nil {
				t.Fatalf("partial state cannot rejoin original completed target: %v steps=%v", err, f.steps)
			}
			assertNativeFinished(t, f)
			proof, cleanup := slices.Index(f.steps, finishCleanupRejoinEvidence), slices.Index(f.steps, jobKindCleanup)
			if proof < 0 || cleanup <= proof {
				t.Fatalf("cleanup lacks fresh partial target authority: %v", f.steps)
			}
		})
	}
	t.Run("source reappearance is not inventory completion", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		f.partial = finishSlotRemovedCase
		f.editResult = func(kind string, v map[string]any) {
			if kind == finishCleanupRejoinEvidence {
				v["source_resources_checked"] = false
			}
		}
		if _, err := nativeFinish(t, f); err == nil || slices.Contains(f.steps, jobKindCleanup) || f.patches != 0 {
			t.Fatal("uncertain source cleanup state authorized an effect or completion")
		}
	})
}

func TestNativeFollowCleanupClosure(t *testing.T) {
	t.Run("changed obligations after checked cleanup", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		reads := 0
		f.editResult = func(kind string, v map[string]any) {
			if kind == finishOwnedInventoryKind {
				reads++
				if reads == 2 {
					v["resources"].([]any)[0].(map[string]any)["name"] = "different_owned_slot"
				}
			}
		}
		if _, err := nativeFinish(t, f); err == nil || f.patches != 0 {
			t.Fatal("changed cleanup obligations became completion")
		}
	})
	t.Run("retained witness changes after cleanup", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		f.editResult = func(kind string, v map[string]any) {
			if kind == finishCleanedEvidenceKind {
				v["checkpoint"].(map[string]any)["checkpoint_id"] = finishChangedToken
			}
		}
		if _, err := nativeFinish(t, f); err == nil || f.patches != 0 {
			t.Fatal("different retained witness became completion")
		}
	})
	t.Run("verification helper replacement after final patch survives", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		f.cleaned = true
		f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
		if err := f.store.Update(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		job, _ := nativeInstallCompare(t, f.nativeCloneFixture, compareSchema, true)
		f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
			if err := c.Status().Patch(ctx, obj, p); err != nil {
				return err
			}
			job.UID = "foreign-helper-after-completion"
			f.replace(job)
			return nil
		}
		if _, err := nativeFinish(t, f); err == nil {
			t.Fatal("replacement helper was silently disposed")
		}
		current := &batchv1.Job{}
		if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(job), current); err != nil || current.UID != job.UID {
			t.Fatal("replacement helper was deleted")
		}
		assertNativeFinished(t, f)
	})
}

func TestNativeFollowHelperReplacement(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.cleaned = true
	f.afterRelay = func(kind string) {
		if kind != finishCleanedEvidenceKind {
			return
		}
		jobs := &batchv1.JobList{}
		if err := f.store.List(context.Background(), jobs); err != nil {
			t.Error(err)
			return
		}
		for i := range jobs.Items {
			job := &jobs.Items[i]
			if finishJobKind(job.Name) == finishCleanedJobKind {
				job.UID = "foreign-cleaned-helper"
				f.replace(job)
			}
		}
	}
	if _, err := nativeFinish(t, f); err == nil || f.patches != 0 || slices.Contains(f.deleted, finishCleanedJobKind) {
		t.Fatal("replaced proof helper was consumed or deleted")
	}
}

func TestNativeCompletionHelperLifetimes(t *testing.T) {
	f := newNativeFinishFixture(t)
	if got, err := f.r.readNativeFollowTargetCompletion(context.Background(), f.m, f.worker, f.pvc, f.report); err != nil || got == nil {
		t.Fatalf("target completion: %v", err)
	}
	versions := 0
	for _, kind := range f.created {
		if kind == probeTestVersionCommand {
			versions++
		}
	}
	if versions != 2 {
		t.Fatalf("each helper needs its own current discovery: versions=%d creates=%v", versions, f.created)
	}
	if !slices.Contains(f.deleted, finishEndposKind) || !slices.Contains(f.deleted, "evidence") {
		t.Fatalf("consumed helpers retained: %v", f.deleted)
	}
}

func TestNativeVerificationCreateDiscovery(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if done, err := f.r.ensureNativeVerification(context.Background(), f.m, f.worker, f.pvc); err != nil || done || f.m.Status.NativeSchemaJobUID == "" {
		t.Fatalf("known comparison receipt: done=%t err=%v", done, err)
	}
	if len(f.created) != 2 || f.created[0] != probeTestVersionCommand || f.created[1] != finishVerificationJobKind {
		t.Fatalf("comparison Create lacks immediate discovery: %v", f.created)
	}
}

func TestNativePlainCloneCompletionHelperDisposal(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.m.Spec.Follow.Enabled = false
	f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.m.Status.Replication = nil
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	canonical, err := buildNativeWorkerJob(f.m, f.pvc, f.r.RPGMigrateRunnerImage, 1)
	if err != nil {
		t.Fatal(err)
	}
	canonical.UID = f.worker.UID
	canonical.OwnerReferences = f.worker.OwnerReferences
	canonical.Status = f.worker.Status
	if err := f.store.Update(context.Background(), canonical); err != nil {
		t.Fatal(err)
	}
	f.worker = canonical
	f.pod.Spec = *canonical.Spec.Template.Spec.DeepCopy()
	f.pod.Spec.NodeName = finishFixtureNode
	f.replace(f.pod)
	nativeInstallCompare(t, f.nativeCloneFixture, compareSchema, true)
	if _, err := f.r.finishNativeClone(context.Background(), f.m, f.m.DeepCopy(), f.worker, f.pvc); err != nil {
		t.Fatalf("plain no-E completion: %v", err)
	}
	if !slices.Contains(f.deleted, finishVerificationJobKind) || nativeControlEnd(f.current()) != "" {
		t.Fatal("plain clone retained consumed helper or invented E")
	}
}

func TestNativeVerificationCreateParentDeadline(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if done, err := f.r.ensureNativeVerification(ctx, f.m, f.worker, f.pvc); done || err == nil || len(f.created) != 0 || f.patches != 0 {
		t.Fatal("comparison renewed an insufficient parent budget or created a helper")
	}
}

func TestNativeCompletionHelperDiscoveryRefusal(t *testing.T) {
	for _, fault := range []string{"missing discovery", "empty capability profile"} {
		t.Run(fault, func(t *testing.T) {
			f := newNativeFinishFixture(t)
			f.fault = fault
			if fault == "missing discovery" {
				f.r.NativePreflight = nil
			}
			if _, err := nativeFinish(t, f); err == nil || f.patches != 0 || f.cleaned {
				t.Fatal("unavailable current profile authorized completion or cleanup")
			}
			for _, kind := range f.created {
				if kind != probeTestVersionCommand {
					t.Fatalf("unqualified profile created a lifecycle helper: %v", f.created)
				}
			}
		})
	}
}
