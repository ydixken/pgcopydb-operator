package controller

// Regression tests for the native worker state machine (design B T2): the review probes of
// lifecycle 1 (retry), 3, 4 (worker), 8 and 11 and design A's matching probes, inverted.

import (
	"context"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// dispatch runs one full reconcileNative pass, as the controller does, with the gate read paced past.
func (f *nativeDispositionFixture) dispatch(t *testing.T) (ctrl.Result, error) {
	t.Helper()
	f.m = f.current()
	f.elapseRetryGateRead(t)
	f.m = f.current()
	return nativeDispatch(t, f.r, f.m)
}

// Lifecycle 8 (TestReviewFailedRevivedBySuspendToggle): Failed is absorbing for suspend and resume.
func TestNativeFailedAbsorbsSuspendToggle(t *testing.T) {
	f := newNativeDispositionFixture(t, false)
	f.helperExits = true
	failedAt := jobFailedAt(f.worker)
	clock := failedAt
	f.r.now = func() time.Time { return clock }
	f.nativeRetryHold(t)
	for elapsed := time.Duration(0); elapsed < verdictDeadline; elapsed += 5 * time.Minute {
		clock = failedAt.Add(elapsed)
		f.nativeRetryHold(t)
	}
	clock = failedAt.Add(verdictDeadline)
	f.nativeRetryHold(t)
	live, _ := f.nativeRetryHold(t)
	expectNativeRetryGateUnobserved(t, live)
	for _, suspend := range []bool{true, false, false, false} {
		m := f.current()
		if m.Spec.Suspend != suspend {
			m.Spec.Suspend = suspend
			m.Generation++
			if err := f.store.Update(context.Background(), m); err != nil {
				t.Fatal(err)
			}
		}
		f.helperExits = false
		clock = time.Now()
		if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
			t.Fatalf("pass with suspend=%v: %v", suspend, err)
		}
		if got := f.current(); got.Status.Phase != v1beta1.PhaseFailed || got.Status.Attempts != 1 || f.workerCreates != 0 {
			t.Fatalf("Failed Migration revived with suspend=%v: phase=%s attempts=%d creates=%d", suspend, got.Status.Phase, got.Status.Attempts, f.workerCreates)
		}
	}
}

// Lifecycle 8 control: deletion of a Failed follow Migration still cleans up.
func TestNativeFailedFollowDeletionCleansUp(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	ctx := context.Background()
	m := f.current()
	m.Status.Phase = v1beta1.PhaseFailed
	r := &MigrationReconciler{}
	r.setCondition(m, v1beta1.ConditionFailed, "True", nativeFailureBadState, "terminal")
	if err := f.store.Status().Update(ctx, m); err != nil {
		t.Fatal(err)
	}
	m = f.current()
	controllerutil.AddFinalizer(m, finalizerName)
	if err := f.store.Update(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Delete(ctx, f.current()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.dispatch(t); err != nil || !f.cleanupCalled {
		t.Fatalf("deleting a Failed follow Migration skipped cleanup: %v cleanup=%v", err, f.cleanupCalled)
	}
}

// Lifecycle 11: with saved E, a starting retry worker requeues quietly instead of failing admission.
func TestNativeStartingWorkerWithSavedEndposRequeuesQuietly(t *testing.T) {
	f := nativeFollowFlowFixture(t)
	ctx := context.Background()
	m := f.current()
	m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: nativeControlTestEnd}
	if err := f.store.Status().Update(ctx, m); err != nil {
		t.Fatal(err)
	}
	worker := &batchv1.Job{}
	if err := f.store.Get(ctx, client.ObjectKeyFromObject(f.worker), worker); err != nil {
		t.Fatal(err)
	}
	worker.Status = batchv1.JobStatus{Active: 1, Ready: ptr.To(int32(0))}
	if err := f.store.Status().Update(ctx, worker); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := f.store.Get(ctx, client.ObjectKeyFromObject(f.pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	if err := f.store.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	m = f.current()
	result, err := f.r.reconcileNativeRunning(ctx, m, m.DeepCopy(), worker, f.pvc)
	if err != nil || result.RequeueAfter != pollInterval {
		t.Fatalf("starting worker with saved E: result=%+v err=%v", result, err)
	}
}
