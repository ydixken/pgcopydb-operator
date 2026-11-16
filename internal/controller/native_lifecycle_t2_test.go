package controller

// Regression tests for the native worker state machine (design B T2): the review probes of
// lifecycle 1 (retry), 3, 4 (worker), 8 and 11 and design A's matching probes, inverted.

import (
	"context"
	"errors"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/events"
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

// Lifecycle 1, retry part (TestReviewVanishedFailedWorkerPodRetry, TestDesignVanishedRetryTrace):
// a failed worker whose pod object is gone retries like one whose pod remains.
func TestNativeRetryAfterFailedWorkerPodVanished(t *testing.T) {
	for _, podGone := range []bool{false, true} {
		t.Run(map[bool]string{false: "pod present", true: "pod gone"}[podGone], func(t *testing.T) {
			f := newNativeDispositionFixture(t, true)
			if podGone {
				if err := f.store.Delete(context.Background(), f.pod); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			for range 3 {
				err = f.reconcile(t)
			}
			if got := f.current(); err != nil || got.Status.Attempts != 2 || f.workerCreates != 1 {
				t.Fatalf("retry did not start attempt 2: err=%v attempts=%d creates=%d", err, got.Status.Attempts, f.workerCreates)
			}
		})
	}
}

// W table, retry column, row "missing": a vanished worker Job counts and the gate starts the next attempt.
func TestNativeRetryAfterWorkerJobVanished(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	for _, obj := range []client.Object{f.pod, f.worker} {
		if err := f.store.Delete(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	for range 4 {
		if _, err = f.dispatch(t); err != nil {
			break
		}
	}
	got := f.current()
	if err != nil || got.Status.Attempts != 2 || f.workerCreates != 1 || got.Status.ExemptAttempts != 0 {
		t.Fatalf("gone worker did not count and retry: err=%v attempts=%d creates=%d exempt=%d", err, got.Status.Attempts, f.workerCreates, got.Status.ExemptAttempts)
	}
}

// Lifecycle 3 (TestReviewLostFirstReceiptWedges): a worker whose Create landed but whose receipt
// patch was lost is adopted under its intent, and no second worker is created.
func TestNativeLostFirstReceiptAdopted(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	ctx := context.Background()
	f.patch = nativeTestAfterIntent(func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
		return errors.New("lost reply")
	})
	if _, err := f.r.startAttempt(ctx, f.m, f.m.DeepCopy()); err == nil {
		t.Fatal("lost receipt admitted")
	}
	f.patch = nil
	f.r.APIReader = workerAttemptPodReader{workerAttemptReader{f}}
	worker := &batchv1.Job{}
	if err := f.store.Get(ctx, client.ObjectKey{Namespace: f.m.Namespace, Name: jobName(f.m, 1)}, worker); err != nil {
		t.Fatalf("worker was not created: %v", err)
	}
	for i := range 3 {
		if _, err := nativeDispatch(t, f.r, f.storedMigration()); errors.Is(err, errNativeReconcile) {
			t.Fatalf("pass %d refused the unrecorded canonical worker: %v", i, err)
		}
	}
	got := f.storedMigration()
	if got.Status.Attempts != 1 || got.Status.NativeJobUID != string(worker.UID) || got.Status.NativeWorkerIntent != "" || f.creates != 1 {
		t.Fatalf("worker was not adopted exactly once: attempts=%d uid=%s intent=%q creates=%d", got.Status.Attempts, got.Status.NativeJobUID, got.Status.NativeWorkerIntent, f.creates)
	}
}

// workerAttemptPodReader serves pod lists for passes after a worker exists, where the worker table looks
// for a started container (dj); every other read keeps the attempt fixture's guards.
type workerAttemptPodReader struct{ workerAttemptReader }

func (r workerAttemptPodReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	pods, ok := list.(*corev1.PodList)
	if !ok {
		r.f.t.Fatal("native attempt listed a resource other than pods")
		return errBoom
	}
	if err := r.f.store.List(ctx, pods, opts...); err != nil {
		return err
	}
	pods.ResourceVersion = "worker-attempt-snapshot"
	return nil
}

// Lifecycle 3 (TestReviewLostRetryReceiptDeletionCleansBesideUnrecordedWorker): deletion resolves
// the next-name worker first (R2), so no cleanup runs beside a worker whose receipt was lost.
func TestNativeDeletionResolvesNextWorkerFirst(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	ctx := context.Background()
	for range 2 {
		if err := f.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	f.lostPinPatch = true
	if err := f.reconcile(t); err == nil {
		t.Fatal("lost receipt admitted")
	}
	f.lostPinPatch = false
	next := &batchv1.Job{}
	if err := f.store.Get(ctx, client.ObjectKey{Namespace: f.m.Namespace, Name: jobName(f.m, 2)}, next); err != nil {
		t.Fatalf("unrecorded worker 2 absent: %v", err)
	}
	m := f.current()
	controllerutil.AddFinalizer(m, finalizerName)
	if err := f.store.Update(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Delete(ctx, f.current()); err != nil {
		t.Fatal(err)
	}
	_, _ = f.dispatch(t)
	still := f.store.Get(ctx, client.ObjectKeyFromObject(next), &batchv1.Job{}) == nil
	if f.cleanupCalled && still {
		t.Fatalf("deletion ran the stream cleanup Job while unrecorded worker %s still exists", next.Name)
	}
}

// Lifecycle 4, worker part (TestReviewSpecDriftWedgesRunningWorker, TestReviewSpecDriftFieldsAndRevert):
// an edit to a field the worker template reads applies from the next attempt and wedges nothing now.
func TestNativeSpecDriftToleratedOnRunningWorker(t *testing.T) {
	for _, field := range []string{"runner.resources", "runner.nodeSelector", "runner.tolerations", "runner.affinity", "clone.tableJobs", "target.synchronousCommit", "e2e label", "work-node annotation"} {
		t.Run(field, func(t *testing.T) {
			f, _, _ := nativeStreamingStatus(t)
			ctx := context.Background()
			m := f.current()
			controllerutil.AddFinalizer(m, finalizerName)
			switch field {
			case "runner.resources":
				m.Spec.Runner.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")}
			case "runner.nodeSelector":
				m.Spec.Runner.NodeSelector = map[string]string{nativeT2PoolKey: "db"}
			case "runner.tolerations":
				m.Spec.Runner.Tolerations = []corev1.Toleration{{Key: nativeT2TaintKey, Operator: corev1.TolerationOpExists}}
			case "runner.affinity":
				m.Spec.Runner.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: nativeT2PoolKey, Operator: corev1.NodeSelectorOpIn, Values: []string{"db"}}}}}}}}
			case "clone.tableJobs":
				m.Spec.Clone.TableJobs = 8
			case "target.synchronousCommit":
				m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
			case "e2e label":
				m.Labels = map[string]string{"pgcopydb-operator.io/feature-e2e-run": "drift"}
			case "work-node annotation":
				m.Annotations = map[string]string{"pgcopydb-operator.io/work-volume-node": "node-b"}
			}
			m.Generation++
			if err := f.store.Update(ctx, m); err != nil {
				t.Fatal(err)
			}
			if _, err := nativeDispatch(t, f.r, f.current()); errors.Is(err, errNativeReconcile) || meta.IsStatusConditionTrue(f.current().Status.Conditions, v1beta1.ConditionBlocked) {
				t.Fatalf("running pass refused or held after %s edit: %v", field, err)
			}
			if err := f.store.Delete(ctx, f.current()); err != nil {
				t.Fatal(err)
			}
			f.allowDrop = true
			if _, err := nativeDispatch(t, f.r, f.current()); errors.Is(err, errNativeReconcile) {
				t.Fatalf("deletion never reaches disposition after %s edit: %v", field, err)
			}
		})
	}
}

// Lifecycle 4: attempt n+1 is built from the edited spec while attempt n kept its admitted bytes.
func TestNativeNextAttemptUsesEditedSpec(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	ctx := context.Background()
	m := f.current()
	m.Spec.Runner.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("3Gi")}
	m.Generation++
	if err := f.store.Update(ctx, m); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := f.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	next := &batchv1.Job{}
	if err := f.store.Get(ctx, client.ObjectKey{Namespace: f.m.Namespace, Name: jobName(f.m, 2)}, next); err != nil {
		t.Fatalf("attempt 2 was not created after the edit: %v", err)
	}
	if got := next.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]; got.Cmp(resource.MustParse("3Gi")) != 0 {
		t.Fatalf("attempt 2 memory limit %s, want the edited 3Gi", got.String())
	}
}

// Lifecycle 4: an edit to the live worker Job itself holds the running worker with WorkerModified.
func TestNativeModifiedWorkerHeld(t *testing.T) {
	f, _, _ := nativeStreamingStatus(t)
	ctx := context.Background()
	f.r.Recorder = events.NewFakeRecorder(10)
	worker := &batchv1.Job{}
	if err := f.store.Get(ctx, client.ObjectKey{Namespace: f.m.Namespace, Name: f.m.Status.JobName}, worker); err != nil {
		t.Fatal(err)
	}
	worker.Labels["edited"] = nativeT2PoolKey
	if err := f.store.Update(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
		t.Fatalf("modified worker pass: %v", err)
	}
	blocked := meta.FindStatusCondition(f.current().Status.Conditions, v1beta1.ConditionBlocked)
	if blocked == nil || blocked.Status != "True" || blocked.Reason != v1beta1.ReasonWorkerModified {
		t.Fatalf("modified running worker was not held: %+v", blocked)
	}
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
