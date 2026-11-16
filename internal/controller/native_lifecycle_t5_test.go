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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

// The W and V states of the lost-volume table.
const (
	t5FailedWorker   = "failed worker"
	t5CompleteWorker = "complete worker"
	t5RunningWorker  = "running worker"
	t5PendingWorker  = "pending worker"
	t5ModifiedWorker = "modified running worker"
	t5WorkerGone     = "worker gone"
	t5ClaimGone      = "claim gone"
	t5ClaimDeleting  = "claim deleting"
	t5ClaimReplaced  = "claim replaced"
)

// t5LoseClaim puts the work PVC of f into one lost state of the V table.
func t5LoseClaim(t *testing.T, f *nativeDispositionFixture, state string) {
	t.Helper()
	ctx := context.Background()
	claim := &corev1.PersistentVolumeClaim{}
	if err := f.store.Get(ctx, client.ObjectKeyFromObject(f.pvc), claim); err != nil {
		t.Fatal(err)
	}
	if state == t5ClaimDeleting {
		controllerutil.AddFinalizer(claim, "kubernetes.io/pvc-protection")
		if err := f.store.Update(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.store.Delete(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if state == t5ClaimReplaced {
		claim.ResourceVersion, claim.UID = "", "replacement-claim"
		if err := f.store.Create(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
}

// t5Worker puts the recorded worker of f into one W state of the retry, finish or running path.
func t5Worker(t *testing.T, f *nativeDispositionFixture, state string) {
	t.Helper()
	switch state {
	case t5CompleteWorker:
		f.worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
		f.replace(f.worker)
		f.pod = nativeRecoveryTestPod(f.worker, 0)
		f.replace(f.pod)
	case t5RunningWorker:
		t4RunningWorker(t, f, true)
	case t5PendingWorker:
		t4RunningWorker(t, f, false)
	case t5ModifiedWorker:
		t4RunningWorker(t, f, true)
		f.worker.Labels = map[string]string{"t5-edit": "worker-label"}
		f.replace(f.worker)
	case t5WorkerGone:
		t4RemoveWorker(t, f)
	}
}

func t5WorkVolumeLost(t *testing.T, m *v1beta1.Migration) bool {
	t.Helper()
	failed := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
	return m.Status.Phase == v1beta1.PhaseFailed && failed != nil && failed.Status == metav1.ConditionTrue && failed.Reason == v1beta1.ReasonWorkVolumeLost &&
		strings.Contains(failed.Message, workPVCName(m))
}

// di: a work PVC lost past attempt 0 outside deletion (the V table's retry and finish cells) stops the
// worker first, then fails the Migration with WorkVolumeLost. Failed absorbs later passes and a suspend
// toggle (R6), and deletion still runs native disposition, which releases with CleanupFailed.
func TestNativeWorkVolumeLostFailsOutsideDeletion(t *testing.T) {
	for _, worker := range []string{t5FailedWorker, t5CompleteWorker, t5RunningWorker, t5PendingWorker, t5ModifiedWorker, t5WorkerGone} {
		for _, claim := range []string{t5ClaimGone, t5ClaimDeleting, t5ClaimReplaced} {
			t.Run(worker+", "+claim, func(t *testing.T) {
				f := newNativeDispositionFixture(t, true)
				w := t4Wrap(t, f)
				t5Worker(t, f, worker)
				t5LoseClaim(t, f, claim)
				for pass := 0; pass < 4 && !t5WorkVolumeLost(t, f.current()); pass++ {
					if _, err := f.dispatch(t); err != nil {
						t.Fatalf("pass %d: %v", pass, err)
					}
					// R1: Failed is written only once the worker has no engine left.
					live := &batchv1.Job{}
					if t5WorkVolumeLost(t, f.current()) && f.store.Get(context.Background(), client.ObjectKeyFromObject(f.worker), live) == nil && !nativeWorkerEnded(live) {
						t.Fatalf("pass %d failed the Migration while worker %s still runs", pass, live.Name)
					}
				}
				got := f.current()
				if !t5WorkVolumeLost(t, got) || f.workerCreates != 0 || f.cleanupCalled || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionComplete) {
					t.Fatalf("lost claim outside deletion: phase=%s failed=%+v creates=%d cleanup=%v", got.Status.Phase, meta.FindStatusCondition(got.Status.Conditions, v1beta1.ConditionFailed), f.workerCreates, f.cleanupCalled)
				}
				if (worker == t5RunningWorker || worker == t5ModifiedWorker || worker == t5PendingWorker) && !slices.Contains(w.order, "delete "+f.worker.Name) {
					t.Fatalf("the running worker was not stopped: %v", w.order)
				}
				recorder, _ := f.r.Recorder.(*events.FakeRecorder)
				warned := false
				for len(recorder.Events) > 0 {
					warned = warned || strings.Contains(<-recorder.Events, "Warning "+v1beta1.ReasonWorkVolumeLost)
				}
				if !warned {
					t.Fatal("no WorkVolumeLost warning event")
				}
				patches := f.patches
				t4Suspend(t, f)
				for range 2 {
					if _, err := f.dispatch(t); err != nil {
						t.Fatal(err)
					}
				}
				if got := f.current(); !t5WorkVolumeLost(t, got) || f.patches != patches || f.workerCreates != 0 {
					t.Fatalf("Failed did not absorb: phase=%s patches=%d creates=%d", got.Status.Phase, f.patches-patches, f.workerCreates)
				}
				t4Delete(t, f)
				if err := t4Passes(t, f, 3); err != nil || f.cleanupCalled {
					t.Fatalf("deletion of the failed Migration: err=%v cleanup=%v", err, f.cleanupCalled)
				}
				t4CleanupFailedReleased(t, f, w, v1beta1.ReasonWorkVolumeLost)
				if claim == t5ClaimReplaced && t4ClaimGone(f) {
					t.Fatal("a claim with another UID was deleted")
				}
			})
		}
	}
}

// di with R1: the worker Job is gone but its pod, found by its controller-uid label, still runs, so
// the lost claim waits with Blocked=WorkerPodRemains, and Failed follows only once the pod has ended.
func TestNativeWorkVolumeLostWaitsForWorkerPod(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	t4Wrap(t, f)
	t4RunningWorker(t, f, true)
	orphan := f.pod.DeepCopy()
	orphan.OwnerReferences = nil
	orphan.Labels[batchv1.ControllerUidLabel] = string(f.worker.UID)
	f.replace(orphan)
	if err := f.store.Delete(context.Background(), f.worker); err != nil {
		t.Fatal(err)
	}
	t5LoseClaim(t, f, t5ClaimGone)
	for pass := range 2 {
		if _, err := f.dispatch(t); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if got := f.current(); got.Status.Phase == v1beta1.PhaseFailed || t4Blocked(f) != v1beta1.ReasonWorkerPodRemains || !strings.Contains(t5Blocked(got).Message, orphan.Name) {
			t.Fatalf("pass %d beside a running worker pod: phase=%s blocked=%+v", pass, got.Status.Phase, t5Blocked(got))
		}
	}
	if err := f.store.Delete(context.Background(), orphan); err != nil {
		t.Fatal(err)
	}
	if _, err := f.dispatch(t); err != nil || !t5WorkVolumeLost(t, f.current()) || t5Blocked(f.current()) != nil {
		t.Fatalf("no WorkVolumeLost once the pod ended: err=%v phase=%s blocked=%+v", err, f.current().Status.Phase, t5Blocked(f.current()))
	}
}
