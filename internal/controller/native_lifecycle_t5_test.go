package controller

// Regression tests for the remaining native lifecycle cells (leader decisions dh to dk): a failed
// pinned compare, a lost work volume outside deletion, fresh versus resumed attempts, and a
// target.synchronousCommit edit after admission.

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

func t5WorkVolumeLost(m *v1beta1.Migration) bool {
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
				for pass := 0; pass < 4 && !t5WorkVolumeLost(f.current()); pass++ {
					if _, err := f.dispatch(t); err != nil {
						t.Fatalf("pass %d: %v", pass, err)
					}
					// R1: Failed is written only once the worker has no engine left.
					live := &batchv1.Job{}
					if t5WorkVolumeLost(f.current()) && f.store.Get(context.Background(), client.ObjectKeyFromObject(f.worker), live) == nil && !nativeWorkerEnded(live) {
						t.Fatalf("pass %d failed the Migration while worker %s still runs", pass, live.Name)
					}
				}
				got := f.current()
				if !t5WorkVolumeLost(got) || f.workerCreates != 0 || f.cleanupCalled || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionComplete) {
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
				if got := f.current(); !t5WorkVolumeLost(got) || f.patches != patches || f.workerCreates != 0 {
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
	if _, err := f.dispatch(t); err != nil || !t5WorkVolumeLost(f.current()) || t5Blocked(f.current()) != nil {
		t.Fatalf("no WorkVolumeLost once the pod ended: err=%v phase=%s blocked=%+v", err, f.current().Status.Phase, t5Blocked(f.current()))
	}
}

// t5ReceiptAttemptOne records the fixture's attempt 1 through the receipt code, as its Create would,
// so the fresh mark is whatever the receipt writes and not a hand-written status.
func t5ReceiptAttemptOne(t *testing.T, f *nativeDispositionFixture) {
	t.Helper()
	m := f.current()
	digest := m.Status.NativeJobSHA256
	m.Status.Attempts, m.Status.JobName, m.Status.NativeJobUID, m.Status.NativePVCUID, m.Status.NativeJobSHA256 = 0, "", "", "", ""
	m.Status.NativeFreshAttempt = 0
	if !f.r.applyNativeAttemptReceipt(m, f.worker, f.pvc, digest) {
		t.Fatal("attempt 1 receipt refused")
	}
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	f.m = f.current()
}

// The engine modes a native worker runs in.
const (
	t5Restart = "--restart"
	t5Resume  = "--resume"
)

// t5WorkerMode returns the engine mode of the worker Job of attempt n: --restart or --resume.
func t5WorkerMode(t *testing.T, f *nativeDispositionFixture, n int32) string {
	t.Helper()
	job := &batchv1.Job{}
	if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: jobName(f.m, n)}, job); err != nil {
		t.Fatalf("attempt %d worker: %v", n, err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	words := append(slices.Clone(c.Command), c.Args...)
	restart, resume := slices.Contains(words, t5Restart), slices.Contains(words, t5Resume)
	switch {
	case restart && !resume:
		return t5Restart
	case resume && !restart:
		return t5Resume
	}
	t.Fatalf("attempt %d worker has no single engine mode: %q", n, words)
	return ""
}

// t5RunToAttempt dispatches until attempt n exists, recording the stored fresh mark at its worker's Create.
func t5RunToAttempt(t *testing.T, f *nativeDispositionFixture, w *t4Writes, n int32) int32 {
	t.Helper()
	atCreate := int32(-1)
	w.create = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if obj.GetName() == jobName(f.m, n) && len(opts) == 0 {
			atCreate = f.current().Status.NativeFreshAttempt
		}
		return c.Create(ctx, obj, opts...)
	}
	for pass := 0; pass < 8 && f.current().Status.Attempts < n; pass++ {
		if _, err := f.dispatch(t); err != nil {
			t.Fatalf("pass %d towards attempt %d: %v", pass, n, err)
		}
	}
	if got := f.current().Status.Attempts; got != n || atCreate < 0 {
		t.Fatalf("attempt %d was not created: attempts=%d", n, got)
	}
	return atCreate
}

// t5FailWorkerBeforeStart ends the worker of the current attempt Failed with a pod that never ran a container, then removes that pod.
func t5FailWorkerBeforeStart(t *testing.T, f *nativeDispositionFixture) {
	t.Helper()
	job := &batchv1.Job{}
	if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: f.current().Status.JobName}, job); err != nil {
		t.Fatal(err)
	}
	job.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}}
	if err := f.store.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	pods := &corev1.PodList{}
	if err := f.store.List(context.Background(), pods); err != nil {
		t.Fatal(err)
	}
	for i := range pods.Items {
		if o := metav1.GetControllerOf(&pods.Items[i]); o != nil && o.UID == job.UID {
			if err := f.store.Delete(context.Background(), &pods.Items[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// dj: an attempt starts over with --restart exactly while no worker container of the Migration has been
// seen to start, whatever ended the attempts before it; once one is seen, every later attempt resumes.
// The mark is in the stored status before the next worker's Create.
func TestNativeFreshAttemptUntilAWorkerStarted(t *testing.T) {
	for _, tc := range []struct {
		name       string
		end        func(*testing.T, *nativeDispositionFixture)
		mode       string
		markAtNext int32 // the stored nativeFreshAttempt when attempt 2's worker is created
	}{
		{"failure before start, pod gone", func(t *testing.T, f *nativeDispositionFixture) {
			if err := f.store.Delete(context.Background(), f.pod); err != nil {
				t.Fatal(err)
			}
		}, t5Restart, 2},
		{"R7 drop of a counted attempt that never started", func(t *testing.T, f *nativeDispositionFixture) {
			t4RunningWorker(t, f, false)
			m := f.current()
			m.Status.RetryGate = &v1beta1.RetryGateStatus{FailedAttempt: 1, FailedAt: metav1.Now(), Failure: "counted before start", Exemption: v1beta1.RetryGateCounted}
			if err := f.store.Status().Update(context.Background(), m); err != nil {
				t.Fatal(err)
			}
		}, t5Restart, 2},
		{"suspend of a worker that never started", func(t *testing.T, f *nativeDispositionFixture) {
			t4RunningWorker(t, f, false)
			t4Suspend(t, f)
			for pass := 0; pass < 4 && f.current().Status.Phase != v1beta1.PhaseSuspended; pass++ {
				if _, err := f.dispatch(t); err != nil {
					t.Fatal(err)
				}
			}
			m := f.current()
			m.Spec.Suspend = false
			m.Generation++
			if err := f.store.Update(context.Background(), m); err != nil {
				t.Fatal(err)
			}
		}, t5Restart, 2},
		{"started, then failed", func(*testing.T, *nativeDispositionFixture) {}, t5Resume, 0},
		{"seen running, then failed and its pod vanished", func(t *testing.T, f *nativeDispositionFixture) {
			failed := f.worker.Status
			t4RunningWorker(t, f, true)
			if _, err := f.dispatch(t); err != nil {
				t.Fatal(err)
			}
			f.worker.Status = failed
			f.replace(f.worker)
			if err := f.store.Delete(context.Background(), f.pod); err != nil {
				t.Fatal(err)
			}
		}, t5Resume, 0},
		{"started, Job gone, its pod found by label only", func(t *testing.T, f *nativeDispositionFixture) {
			orphan := f.pod.DeepCopy()
			// Another controller, so only the controller-uid label ties the pod to the worker (the fixture's GC needs a controller).
			orphan.OwnerReferences[0].UID, orphan.OwnerReferences[0].Name = "unrelated-owner", "unrelated"
			orphan.Labels[batchv1.ControllerUidLabel] = string(f.worker.UID)
			f.replace(orphan)
			if err := f.store.Delete(context.Background(), f.worker); err != nil {
				t.Fatal(err)
			}
		}, t5Resume, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeDispositionFixture(t, true)
			w := t4Wrap(t, f)
			t5ReceiptAttemptOne(t, f)
			tc.end(t, f)
			if mark := t5RunToAttempt(t, f, w, 2); mark != tc.markAtNext {
				t.Fatalf("stored nativeFreshAttempt at attempt 2's Create = %d, want %d", mark, tc.markAtNext)
			}
			if mode := t5WorkerMode(t, f, 2); mode != tc.mode {
				t.Fatalf("attempt 2 runs %s, want %s", mode, tc.mode)
			}
		})
	}
}

// dj across attempts: a fresh attempt 2 that never started keeps attempt 3 fresh; once attempt 3's
// container is seen, attempt 4 resumes, and so does every attempt after it.
func TestNativeFreshAttemptChain(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	f.m.Spec.BackoffLimit = 5
	w := t4Wrap(t, f)
	m := f.current()
	m.Spec.BackoffLimit = 5
	if err := f.store.Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	t5ReceiptAttemptOne(t, f)
	if err := f.store.Delete(context.Background(), f.pod); err != nil {
		t.Fatal(err)
	}
	t5RunToAttempt(t, f, w, 2)
	if got := f.current().Status.NativeFreshAttempt; got != 3 || t5WorkerMode(t, f, 2) != t5Restart {
		t.Fatalf("after a fresh attempt 2: nativeFreshAttempt=%d mode=%s", got, t5WorkerMode(t, f, 2))
	}
	t5FailWorkerBeforeStart(t, f)
	t5RunToAttempt(t, f, w, 3)
	if got := f.current().Status.NativeFreshAttempt; got != 4 || t5WorkerMode(t, f, 3) != t5Restart {
		t.Fatalf("after a fresh attempt 3: nativeFreshAttempt=%d mode=%s", got, t5WorkerMode(t, f, 3))
	}
	// Attempt 3's container starts and the attempt fails with its pod kept.
	job := &batchv1.Job{}
	if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: jobName(f.m, 3)}, job); err != nil {
		t.Fatal(err)
	}
	job.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}}
	if err := f.store.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	pod := nativeRecoveryTestPod(job, 1)
	pod.UID = "attempt-3-pod"
	if err := f.store.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if mark := t5RunToAttempt(t, f, w, 4); mark != 3 || t5WorkerMode(t, f, 4) != t5Resume || f.current().Status.NativeFreshAttempt != 3 {
		t.Fatalf("after attempt 3 started: mark at create=%d mode=%s nativeFreshAttempt=%d", mark, t5WorkerMode(t, f, 4), f.current().Status.NativeFreshAttempt)
	}
	t5FailWorkerBeforeStart(t, f)
	if t5RunToAttempt(t, f, w, 5); t5WorkerMode(t, f, 5) != t5Resume {
		t.Fatalf("attempt 5 after a seen start runs %s", t5WorkerMode(t, f, 5))
	}
}

// dj status integrity: a fresh attempt past the next attempt is a status no pass wrote, so the pass refuses it.
func TestNativeFreshAttemptPastNextRefused(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	m := f.current()
	m.Status.NativeFreshAttempt = 3
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if _, err := f.dispatch(t); !errors.Is(err, errNativeReconcile) || f.workerCreates != 0 {
		t.Fatalf("nativeFreshAttempt=3 at attempt 1: err=%v creates=%d", err, f.workerCreates)
	}
}

type t5FailingPodList struct{ client.Reader }

func (t5FailingPodList) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errBoom
}

// dj: a pod list that fails is no evidence that no container started, so the start search ends the pass
// with an error and writes nothing, instead of letting the pass go on as if none had started.
func TestNativeWorkerStartSearchError(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	t5ReceiptAttemptOne(t, f)
	f.r.APIReader = t5FailingPodList{f.r.APIReader}
	m := f.current()
	handled, _, err := f.r.recordNativeWorkerStarted(context.Background(), m, m.DeepCopy(), nativeWorkerState{w: workerAdmitted, worker: f.worker})
	if !handled || err == nil || f.patches != 0 || f.current().Status.NativeFreshAttempt != 2 {
		t.Fatalf("failed pod list: handled=%v err=%v patches=%d fresh=%d", handled, err, f.patches, f.current().Status.NativeFreshAttempt)
	}
}

// dj, reported and not fixed: after a seen start every attempt resumes, so a container that started but
// left no engine state makes the next attempt refuse with state-missing (engine E1), a terminal BadState.
func TestNativeStartedWithoutStateIsTerminal(t *testing.T) {
	reason, terminal := nativeWorkerTerminalFailure(nativeFailureTestRecord("ERROR", "native:BadState: state-missing: native state does not exist"))
	if reason != nativeFailureBadState || !terminal {
		t.Fatalf("state-missing classified as %q terminal=%v", reason, terminal)
	}
}

// t5SetSynchronousCommit edits spec.target.synchronousCommit of a stored Migration after admission.
func t5SetSynchronousCommit(t *testing.T, store client.Client, m *v1beta1.Migration, value string) {
	t.Helper()
	m.Spec.Target.SynchronousCommit = value
	m.Generation++
	if err := store.Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func t5SynchronousCommitHeld(t *testing.T, m *v1beta1.Migration, action string) {
	t.Helper()
	b := t5Blocked(m)
	if b == nil || b.Reason != v1beta1.ReasonSynchronousCommitUnsupported || !strings.Contains(b.Message, "spec.target.synchronousCommit") || !strings.Contains(b.Message, action) {
		t.Fatalf("no SynchronousCommitUnsupported hold naming the field and %s: %+v", action, b)
	}
}

// dk: a target.synchronousCommit edit after admission holds every path that needs a native helper Job or
// a new attempt with Blocked=SynchronousCommitUnsupported, without a bare error and without a rewrite on
// later passes; unsetting the field lets the path go on. Suspend, the running attempt, a finish without a
// helper and a release without cleanup are unchanged (the controls below).

func TestNativeSynchronousCommitEditHoldsRetry(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	t5SetSynchronousCommit(t, f.store, f.current(), v1beta1.SynchronousCommitLocal)
	patches := 0
	for pass := range 4 {
		if _, err := f.dispatch(t); err != nil {
			t.Fatalf("retry pass %d: %v", pass, err)
		}
		if pass == 1 {
			patches = f.patches
		}
	}
	t5SynchronousCommitHeld(t, f.current(), "retry gate of attempt 2")
	// The gate still records the failed attempt, which needs no native Job (nothing else changes).
	if rg, held := f.current().Status.RetryGate, meta.FindStatusCondition(f.current().Status.Conditions, v1beta1.ConditionRetryHeld); rg == nil || rg.FailedAttempt != 1 || held == nil || held.Reason != reasonGateProbing {
		t.Fatalf("held retry lost the gate record: gate=%+v held=%+v", rg, held)
	}
	if f.patches != patches || f.retryHelpers != 0 || f.workerCreates != 0 || f.current().Status.Phase == v1beta1.PhaseFailed {
		t.Fatalf("held retry acted: patches=%d helpers=%d creates=%d phase=%s", f.patches-patches, f.retryHelpers, f.workerCreates, f.current().Status.Phase)
	}
	t5SetSynchronousCommit(t, f.store, f.current(), "")
	for pass := 0; pass < 6 && f.current().Status.Attempts < 2; pass++ {
		if _, err := f.dispatch(t); err != nil {
			t.Fatalf("pass %d after the field was unset: %v", pass, err)
		}
	}
	if got := f.current(); got.Status.Attempts != 2 || f.workerCreates != 1 || t5Blocked(got) != nil {
		t.Fatalf("unset field did not release the retry: attempts=%d creates=%d blocked=%+v", got.Status.Attempts, f.workerCreates, t5Blocked(got))
	}
}

func TestNativeSynchronousCommitEditHoldsFollowFinish(t *testing.T) {
	// No verification: every follow finish reads its completion evidence through native helper Jobs.
	f := newNativeFinishFixture(t)
	t5SetSynchronousCommit(t, f.store, f.current(), v1beta1.SynchronousCommitLocal)
	for pass := range 3 {
		if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
			t.Fatalf("finish pass %d: %v", pass, err)
		}
		if f.patches != 1 || len(f.created) != 0 {
			t.Fatalf("finish pass %d: patches=%d created=%v", pass, f.patches, f.created)
		}
	}
	t5SynchronousCommitHeld(t, f.current(), "native finish")
	t5SetSynchronousCommit(t, f.store, f.current(), "")
	for range 2 {
		if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
			t.Fatal(err)
		}
	}
	if t5Blocked(f.current()) != nil || len(f.created) == 0 {
		t.Fatalf("unset field did not release the finish: blocked=%+v created=%v", t5Blocked(f.current()), f.created)
	}
}

func TestNativeSynchronousCommitEditHoldsCloneCompare(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true})
	created := 0
	f.onCreate = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		created++
		return c.Create(ctx, obj, opts...)
	}
	t5SetSynchronousCommit(t, f.store, f.current(), v1beta1.SynchronousCommitLocal)
	for pass := range 3 {
		if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
			t.Fatalf("clone pass %d: %v", pass, err)
		}
	}
	t5SynchronousCommitHeld(t, f.current(), "native finish")
	if created != 0 || f.patches != 1 || f.current().Status.Phase == v1beta1.PhaseCompleted {
		t.Fatalf("held clone acted: created=%d patches=%d phase=%s", created, f.patches, f.current().Status.Phase)
	}
}

func TestNativeSynchronousCommitEditCloneWithoutCompareCompletes(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	t5SetSynchronousCommit(t, f.store, f.current(), v1beta1.SynchronousCommitLocal)
	f.m = f.current()
	if _, err := nativeCloneFinish(t, f); err != nil || f.current().Status.Phase != v1beta1.PhaseCompleted || t5Blocked(f.current()) != nil {
		t.Fatalf("a finish that needs no helper did not complete: err=%v phase=%s blocked=%+v", err, f.current().Status.Phase, t5Blocked(f.current()))
	}
}

func TestNativeSynchronousCommitEditHoldsDeletionCleanup(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	t5SetSynchronousCommit(t, f.store, f.current(), v1beta1.SynchronousCommitLocal)
	t4Delete(t, f)
	for pass := range 3 {
		if _, err := f.dispatch(t); err != nil {
			t.Fatalf("deletion pass %d: %v", pass, err)
		}
	}
	t5SynchronousCommitHeld(t, f.current(), "native cleanup")
	if f.cleanupCalled || t4Released(f) || w.failed != nil {
		t.Fatalf("held deletion acted: cleanup=%v released=%v condition=%+v", f.cleanupCalled, t4Released(f), w.failed)
	}
	t5SetSynchronousCommit(t, f.store, f.current(), "")
	if err := t4Passes(t, f, 3); err != nil || !f.cleanupCalled || !t4Released(f) {
		t.Fatalf("unset field did not release the deletion: err=%v cleanup=%v released=%v", err, f.cleanupCalled, t4Released(f))
	}
}

func TestNativeSynchronousCommitEditLostClaimDeletionReleases(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	t5SetSynchronousCommit(t, f.store, f.current(), v1beta1.SynchronousCommitLocal)
	t4RemoveWorker(t, f)
	t5LoseClaim(t, f, t5ClaimGone)
	t4Delete(t, f)
	if err := t4Passes(t, f, 2); err != nil || f.cleanupCalled {
		t.Fatalf("lost claim deletion: err=%v cleanup=%v", err, f.cleanupCalled)
	}
	t4CleanupFailedReleased(t, f, w, v1beta1.ReasonWorkVolumeLost)
}

func TestNativeSynchronousCommitEditSuspends(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	m := f.current()
	m.Spec.Suspend = true
	t5SetSynchronousCommit(t, f.store, m, v1beta1.SynchronousCommitLocal)
	if _, err := f.dispatch(t); err != nil || f.current().Status.Phase != v1beta1.PhaseSuspended || t5Blocked(f.current()) != nil {
		t.Fatalf("suspend with the field set: err=%v phase=%s blocked=%+v", err, f.current().Status.Phase, t5Blocked(f.current()))
	}
}

// The API server, not the fake client, validates what the T5 rules write: nativeFreshAttempt moved on,
// removed and lowered (minimum 2, no transition rule), the Blocked reasons, and Failed=WorkVolumeLost.
var _ = Describe("native T5 status writes on the API server", func() {
	It("admits each write and stores it as written", func(ctx SpecContext) {
		r := &MigrationReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(), Recorder: events.NewFakeRecorder(10)}
		m := validMigration("native-t5-status")
		m.Spec.Engine, m.Spec.Follow = v1beta1.MigrationEngineRPGMigrate, &v1beta1.FollowOptions{Enabled: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(context.Background(), m) })
		m.Status = v1beta1.MigrationStatus{Phase: v1beta1.PhasePending, NativeRunnerImage: qualifiedRunnerARM64}
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())
		patch := func(change func(*v1beta1.Migration)) {
			GinkgoHelper()
			desired := m.DeepCopy()
			change(desired)
			Expect(k8sClient.Status().Patch(ctx, desired, client.MergeFromWithOptions(m, client.MergeFromWithOptimisticLock{}))).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), m)).To(Succeed())
		}
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: jobName(m, 1), UID: "5a3c1e2d-7b4f-4c6a-9e8d-1f2a3b4c5d6e"}}
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{UID: "5a3c1e2d-7b4f-4c6a-9e8d-1f2a3b4c5d6f"}}
		patch(func(d *v1beta1.Migration) {
			Expect(r.applyNativeAttemptReceipt(d, job, pvc, strings.Repeat("a", 64))).To(BeTrue())
		})
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(m.Status.NativeFreshAttempt).To(Equal(int32(2)))
		patch(markNativeWorkerStarted)
		Expect(m.Status.NativeFreshAttempt).To(BeZero())
		patch(func(d *v1beta1.Migration) { d.Status.Attempts, d.Status.NativeFreshAttempt = 3, 4 })
		patch(markNativeWorkerStarted)
		Expect(m.Status.NativeFreshAttempt).To(Equal(int32(3)))
		for _, reason := range []string{v1beta1.ReasonCompareFailed, v1beta1.ReasonSynchronousCommitUnsupported} {
			_, _, err := r.holdNative(ctx, m, m.DeepCopy(), reason, "held by "+reason)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), m)).To(Succeed())
			Expect(t5Blocked(m)).NotTo(BeNil())
			Expect(t5Blocked(m).Reason).To(Equal(reason))
		}
		m.Status.JobName, m.Status.NativeJobUID = jobName(m, 3), "5a3c1e2d-7b4f-4c6a-9e8d-1f2a3b4c5d70"
		_, _, err := r.failNativeWorkVolumeLost(ctx, m, m.DeepCopy(), nativeMissingWorker(m))
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), m)).To(Succeed())
		Expect(t5WorkVolumeLost(m)).To(BeTrue())
		Expect(t5Blocked(m)).To(BeNil())
	})
})
