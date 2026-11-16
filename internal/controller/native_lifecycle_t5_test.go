package controller

// Regression tests for the remaining native lifecycle cells (leader decisions dh to dk): a failed
// pinned compare, a lost work volume outside deletion, fresh versus resumed attempts, and a
// target.synchronousCommit edit after admission.

import (
	"context"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// t5FailedCompare pins a schema compare Job of f that ended Failed and kept its pod.
func t5FailedCompare(t *testing.T, f *nativeFinishFixture) (*batchv1.Job, *corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	job, pod := nativeInstallCompare(t, f.nativeCloneFixture, compareSchema, true)
	job.Status = batchv1.JobStatus{Failed: 1, StartTime: job.Status.StartTime, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
	if err := f.store.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodFailed
	pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
	if err := f.store.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	f.m = f.current()
	return job, pod
}

func t5Blocked(m *v1beta1.Migration) *metav1.Condition {
	if c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionBlocked); c != nil && c.Status == metav1.ConditionTrue {
		return c
	}
	return nil
}

// dh: a pinned compare that ended Failed and kept its pod holds with Blocked=CompareFailed, naming the
// Job, on every pass without a rewrite; it is never read, dropped or recreated until someone deletes it.
// Its deletion is the "pinned, missing" row: the next passes unpin it and create a new compare.
func TestNativeFailedPinnedCompareHolds(t *testing.T) {
	f := t3SchemaFinish(t)
	reads := t3WatchReads(f.nativeCloneFixture)
	recorder := events.NewFakeRecorder(10)
	f.r.Recorder = recorder
	job, pod := t5FailedCompare(t, f)
	for pass := range 3 {
		if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
			t.Fatalf("pass %d with a failed pinned compare: %v", pass, err)
		}
		got := f.current()
		blocked := t5Blocked(got)
		if blocked == nil || blocked.Reason != v1beta1.ReasonCompareFailed || !strings.Contains(blocked.Message, "Job "+job.Name+" ") || f.patches != 1 {
			t.Fatalf("pass %d: no single CompareFailed hold naming %s: blocked=%+v patches=%d", pass, job.Name, blocked, f.patches)
		}
		if len(f.created) != 0 || len(f.deleted) != 0 || len(reads.compares) != 0 || got.Status.NativeSchemaJobUID != string(job.UID) || got.Status.Phase == v1beta1.PhaseCompleted {
			t.Fatalf("pass %d acted on the failed compare: created=%v deleted=%v read=%v pin=%q phase=%s", pass, f.created, f.deleted, reads.compares, got.Status.NativeSchemaJobUID, got.Status.Phase)
		}
	}
	if n := len(recorder.Events); n != 1 {
		t.Fatalf("the hold emitted %d events, want one on the transition", n)
	}
	for _, obj := range []client.Object{pod, job} {
		if err := f.store.Delete(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
			t.Fatalf("pass after the Job was deleted: %v", err)
		}
	}
	got := f.current()
	if pin := got.Status.NativeSchemaJobUID; pin == "" || pin == string(job.UID) || !slices.Contains(f.created, finishVerificationJobKind) || t5Blocked(got) != nil {
		t.Fatalf("deleting the failed compare did not create a new one: pin=%q created=%v blocked=%+v", pin, f.created, t5Blocked(got))
	}
}
