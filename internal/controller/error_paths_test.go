/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// API-error legs the envtest suites cannot reach: a real API server does not
// fail on demand, so these tests inject failures through interceptor clients
// and call the reconcile steps directly. Every leg asserts the error
// propagates instead of reading as success.

package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

var errBoom = errors.New("boom")

// workerJob names the attempt-1 worker the fixtures reference.
const workerJob = "m-run-1"

// workerPodName is the pod the Job controller would have made for workerJob.
const workerPodName = "w-0"

// failingReconciler wires a fake client with injected failures into a
// reconciler; objs seed the fake API state.
func failingReconciler(t *testing.T, fns interceptor.Funcs, objs ...client.Object) *MigrationReconciler {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	c := clientfake.NewClientBuilder().WithScheme(s).WithObjects(objs...).
		WithStatusSubresource(&v1beta1.Migration{}).WithInterceptorFuncs(fns).Build()
	return &MigrationReconciler{Client: c, Scheme: s, Recorder: events.NewFakeRecorder(20), RunnerImage: testRunnerImage}
}

// failStatusPatch injects a failure into every status write.
func failStatusPatch() interceptor.Funcs {
	return interceptor.Funcs{
		SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
			return errBoom
		},
	}
}

// failGetOf fails Get for the named objects only; everything else stays real.
func failGetOf(names ...string) interceptor.Funcs {
	return interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if slices.Contains(names, key.Name) {
				return errBoom
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}
}

func followPasswordMigration() *v1beta1.Migration {
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	return m
}

func namedJob(name string, conds ...batchv1.JobCondition) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Status:     batchv1.JobStatus{Conditions: conds},
	}
}

func completeJob(name string) *batchv1.Job {
	return namedJob(name, batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
}

func migrationRequest(m *v1beta1.Migration) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: m.Name, Namespace: m.Namespace}}
}

// TestReconcile_ConflictRequeuesSilently: a write conflict from an overlapping
// pass requeues without surfacing an error (the next pass converges).
func TestReconcile_ConflictRequeuesSilently(t *testing.T) {
	m := passwordMigration()
	conflict := apierrors.NewConflict(schema.GroupResource{Resource: "migrations"}, m.Name, errBoom)
	r := failingReconciler(t, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return conflict
		},
	})
	res, err := r.Reconcile(context.Background(), migrationRequest(m))
	if err != nil {
		t.Fatalf("a conflict must not surface as an error, got %v", err)
	}
	//nolint:staticcheck // Assert the intentional silent rate-limited Requeue result.
	if res != (ctrl.Result{Requeue: true}) {
		t.Fatalf("result = %+v, want a bare requeue", res)
	}
}

// TestReconcile_InvalidSpecFailsTerminally: a spec that cannot materialize
// connections flips Validated to InvalidSpec and fails the Migration for good.
func TestReconcile_InvalidSpecFailsTerminally(t *testing.T) {
	m := passwordMigration()
	m.Spec.Source.Host = ""
	m.Spec.Source.Username = ""
	r := failingReconciler(t, interceptor.Funcs{}, m)
	if _, err := r.reconcile(context.Background(), migrationRequest(m)); err != nil {
		t.Fatal(err)
	}
	got := &v1beta1.Migration{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: m.Name, Namespace: m.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != v1beta1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	cond := findCondition(got.Status.Conditions, v1beta1.ConditionValidated)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "InvalidSpec" {
		t.Fatalf("Validated condition = %+v, want False/InvalidSpec", cond)
	}
}

func findCondition(conds []metav1.Condition, name string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == name {
			return &conds[i]
		}
	}
	return nil
}

// TestReconcile_FinalizerPatchFailure: the finalizer write failing must abort
// the pass, or a follow migration could start without deletion protection.
func TestReconcile_FinalizerPatchFailure(t *testing.T) {
	m := followPasswordMigration()
	r := failingReconciler(t, interceptor.Funcs{
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return errBoom
		},
	}, m)
	if _, err := r.reconcile(context.Background(), migrationRequest(m)); !errors.Is(err, errBoom) {
		t.Fatalf("a finalizer patch failure must propagate, got %v", err)
	}
}

// TestReconcile_EnsureOwnedCreateFailure: the work PVC failing to create must
// abort the pass before any Job starts.
func TestReconcile_EnsureOwnedCreateFailure(t *testing.T) {
	m := passwordMigration()
	r := failingReconciler(t, interceptor.Funcs{
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			return errBoom
		},
	}, m)
	if _, err := r.reconcile(context.Background(), migrationRequest(m)); !errors.Is(err, errBoom) {
		t.Fatalf("a PVC create failure must propagate, got %v", err)
	}
}

// TestReconcile_WorkerJobGetFailure: a non-NotFound Get of the running worker
// Job is transient API trouble and must surface, not read as a vanished Job.
func TestReconcile_WorkerJobGetFailure(t *testing.T) {
	m := passwordMigration()
	m.Status.Attempts = 1
	m.Status.JobName = workerJob
	r := failingReconciler(t, failGetOf(workerJob), m)
	if _, err := r.reconcile(context.Background(), migrationRequest(m)); !errors.Is(err, errBoom) {
		t.Fatalf("a worker Job Get failure must propagate, got %v", err)
	}
}

// TestPreflightGate_Errors covers the gate's own error legs: the preflight
// Job unreadable, and the running-state status write failing.
func TestPreflightGate_Errors(t *testing.T) {
	m := passwordMigration()
	r := failingReconciler(t, failGetOf(preflightJobName(m)), m)
	if _, handled, err := r.preflightGate(context.Background(), m, m.DeepCopy()); !handled || !errors.Is(err, errBoom) {
		t.Fatalf("a preflight Get failure must propagate, got (handled=%v, %v)", handled, err)
	}

	m = passwordMigration()
	r = failingReconciler(t, failStatusPatch(), m, namedJob(preflightJobName(m)))
	if _, handled, err := r.preflightGate(context.Background(), m, m.DeepCopy()); !handled || !errors.Is(err, errBoom) {
		t.Fatalf("a running-gate status write failure must propagate, got (handled=%v, %v)", handled, err)
	}
}

// TestObserveRunningJob_LogFetchFailureDegrades: an unreadable pod log keeps
// the pass alive at the normal cadence; no marker seen, nothing reaped.
func TestObserveRunningJob_LogFetchFailureDegrades(t *testing.T) {
	m := followPasswordMigration()
	r := failingReconciler(t, interceptor.Funcs{}, m)
	r.Logs = &fakeLogs{tsErr: errBoom}
	res, err := r.observeRunningJob(context.Background(), m, m.DeepCopy(), namedJob(workerJob))
	if err != nil {
		t.Fatalf("an unreadable log must not fail the pass, got %v", err)
	}
	if res.RequeueAfter != pollInterval {
		t.Fatalf("requeue = %v, want the poll cadence %v", res.RequeueAfter, pollInterval)
	}
}

// TestObserveRunningJob_StatusPatchFailure: the observation status write
// failing must surface so the pass retries.
func TestObserveRunningJob_StatusPatchFailure(t *testing.T) {
	m := passwordMigration()
	r := failingReconciler(t, failStatusPatch(), m)
	if _, err := r.observeRunningJob(context.Background(), m, m.DeepCopy(), namedJob(workerJob)); !errors.Is(err, errBoom) {
		t.Fatalf("a status write failure must propagate, got %v", err)
	}
}

// TestReconcileFollowRunning_EarlyReturns: no sentinel wired and no sample
// both leave the previous replication status untouched.
func TestReconcileFollowRunning_EarlyReturns(t *testing.T) {
	m := followPasswordMigration()
	r := &MigrationReconciler{}
	r.reconcileFollowRunning(context.Background(), m, "j", true)
	if m.Status.Replication != nil {
		t.Fatal("nil sentinel must not touch replication status")
	}

	fake := &fakeSentinel{}
	r = &MigrationReconciler{Sentinel: fake}
	r.reconcileFollowRunning(context.Background(), m, "j", true)
	if m.Status.Replication != nil {
		t.Fatal("a nil sample must keep the previous replication status")
	}
	if fake.callCount() != 1 {
		t.Fatalf("sentinel calls = %d, want exactly the one read", fake.callCount())
	}
}

// supervisorDeathTail fabricates a runtime-timestamped log tail whose
// supervisor-death marker is age old.
func supervisorDeathTail(age time.Duration) []byte {
	ts := time.Now().Add(-age).UTC().Format(time.RFC3339Nano)
	return []byte(ts + " FATAL Terminating all processes in our process group\n")
}

// TestReapZombieWorker_GraceOutlivesOnePoll pins the reason zombieGrace is a
// constant of its own: a marker one poll old belongs to a worker still inside
// its termination window, and reaping that kills a pod that was shutting down
// correctly.
func TestReapZombieWorker_GraceOutlivesOnePoll(t *testing.T) {
	m := followPasswordMigration()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: workerPodName, Namespace: "ns", Labels: map[string]string{jobNameLabel: workerJob},
	}}
	r := failingReconciler(t, interceptor.Funcs{}, m, pod)
	res, handled, err := r.reapZombieWorker(context.Background(), m, namedJob(workerJob), supervisorDeathTail(pollInterval))
	if err != nil || !handled {
		t.Fatalf("a marker inside the grace must end the pass cleanly, got (handled=%v, %v)", handled, err)
	}
	if res.RequeueAfter != pollInterval {
		t.Fatalf("requeue = %v, want a re-check on the next poll %v", res.RequeueAfter, pollInterval)
	}
	if err := r.Get(context.Background(),
		types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &corev1.Pod{}); err != nil {
		t.Fatalf("the pod must survive its own termination grace, got %v", err)
	}
}

// TestReapZombieWorker_PodDeleteFailure: the reap failing to delete the zombie
// pod must surface so the next pass retries the reap.
func TestReapZombieWorker_PodDeleteFailure(t *testing.T) {
	m := followPasswordMigration()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: workerPodName, Namespace: "ns", Labels: map[string]string{jobNameLabel: workerJob},
	}}
	r := failingReconciler(t, interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return errBoom
		},
	}, m, pod)
	_, handled, err := r.reapZombieWorker(context.Background(), m, namedJob(workerJob), supervisorDeathTail(2*zombieGrace))
	if !handled || !errors.Is(err, errBoom) {
		t.Fatalf("a pod delete failure must propagate, got (handled=%v, %v)", handled, err)
	}
}

// TestDeleteJobPods_SkipsTerminatingPods: a pod already being deleted is left
// alone; deleting it again would only churn the API server.
func TestDeleteJobPods_SkipsTerminatingPods(t *testing.T) {
	now := metav1.Now()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: workerPodName, Namespace: "ns", Labels: map[string]string{jobNameLabel: workerJob},
		DeletionTimestamp: &now, Finalizers: []string{"test/keep"},
	}}
	r := failingReconciler(t, interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return errBoom
		},
	}, pod)
	if err := r.deleteJobPods(context.Background(), "ns", workerJob); err != nil {
		t.Fatalf("a terminating pod must be skipped, not re-deleted: %v", err)
	}
}

// TestReconcileSuspended_WorkerJobErrors covers the worker-side legs the
// preflight-side test already covers for the gate stage.
func TestReconcileSuspended_WorkerJobErrors(t *testing.T) {
	m := passwordMigration()
	m.Status.JobName = workerJob
	r := failingReconciler(t, interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return errBoom
		},
	}, m, namedJob(workerJob))
	if _, err := r.reconcileSuspended(context.Background(), m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("a worker Delete failure must propagate, got %v", err)
	}

	m = passwordMigration()
	m.Status.JobName = workerJob
	r = failingReconciler(t, failGetOf(workerJob), m)
	if _, err := r.reconcileSuspended(context.Background(), m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("a non-NotFound worker Get failure must propagate, got %v", err)
	}
}

// TestStartAttempt_Errors covers the four ways starting a worker can fail:
// the Job cannot build, cannot be owned, cannot be created, or the status
// write recording the attempt fails.
func TestStartAttempt_Errors(t *testing.T) {
	broken := passwordMigration()
	broken.Spec.Source.Host = ""
	broken.Spec.Source.Username = ""
	r := failingReconciler(t, interceptor.Funcs{}, broken)
	if _, err := r.startAttempt(context.Background(), broken, broken.DeepCopy()); err == nil {
		t.Fatal("an unbuildable Job must propagate")
	}

	m := passwordMigration()
	r = failingReconciler(t, interceptor.Funcs{}, m)
	r.Scheme = runtime.NewScheme() // owner reference needs the Migration GVK
	if _, err := r.startAttempt(context.Background(), m, m.DeepCopy()); err == nil || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("an owner-reference failure must propagate, got %v", err)
	}

	m = passwordMigration()
	r = failingReconciler(t, interceptor.Funcs{
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			return errBoom
		},
	}, m)
	if _, err := r.startAttempt(context.Background(), m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("a Job create failure must propagate, got %v", err)
	}

	m = passwordMigration()
	r = failingReconciler(t, failStatusPatch(), m)
	if _, err := r.startAttempt(context.Background(), m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("the attempt status write failing must propagate, got %v", err)
	}
}

// TestHandleFailedJob_RetryStatusPatchFailure: clearing jobName for the next
// attempt must not silently fail, or the retry never starts.
func TestHandleFailedJob_RetryStatusPatchFailure(t *testing.T) {
	m := passwordMigration()
	m.Spec.BackoffLimit = 2
	m.Status.Attempts = 1
	failed := namedJob(workerJob, batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue})
	r := failingReconciler(t, failStatusPatch(), m)
	if _, err := r.handleFailedJob(context.Background(), m, m.DeepCopy(), failed); !errors.Is(err, errBoom) {
		t.Fatalf("the retry status write failing must propagate, got %v", err)
	}
}

// TestEnsureOwned_OwnerRefFailure: a scheme that cannot resolve the owner's
// GVK fails the PVC's owner reference, and the pass with it.
func TestEnsureOwned_OwnerRefFailure(t *testing.T) {
	m := passwordMigration()
	r := failingReconciler(t, interceptor.Funcs{}, m)
	r.Scheme = runtime.NewScheme()
	if err := r.ensureOwned(context.Background(), m); err == nil {
		t.Fatal("an owner-reference failure must propagate")
	}
}

// TestEnsureOwned_FiltersConfigMapCreateFailure: the filters ConfigMap create
// failing must propagate even after the PVC create succeeded.
func TestEnsureOwned_FiltersConfigMapCreateFailure(t *testing.T) {
	m := passwordMigration()
	m.Spec.Clone.Filters = &v1beta1.Filters{ExcludeTableData: []string{"public.big"}}
	r := failingReconciler(t, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok {
				return errBoom
			}
			return cl.Create(ctx, obj, opts...)
		},
	}, m)
	if err := r.ensureOwned(context.Background(), m); !errors.Is(err, errBoom) {
		t.Fatalf("a ConfigMap create failure must propagate, got %v", err)
	}
}

// TestCreateStrictlyOwned_GetFailure: AlreadyExists routes into the ownership
// check; that check's Get failing must propagate, not read as adopted.
func TestCreateStrictlyOwned_GetFailure(t *testing.T) {
	m := passwordMigration()
	exists := apierrors.NewAlreadyExists(schema.GroupResource{Resource: "persistentvolumeclaims"}, workPVCName(m))
	r := failingReconciler(t, interceptor.Funcs{
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			return exists
		},
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errBoom
		},
	}, m)
	if err := r.createStrictlyOwned(context.Background(), m, buildWorkPVC(m)); !errors.Is(err, errBoom) {
		t.Fatalf("the ownership-check Get failing must propagate, got %v", err)
	}
}

// TestEnsureJob_Errors covers the child-Job plumbing: build failure, owner
// reference failure, and the create race where another pass won.
func TestEnsureJob_Errors(t *testing.T) {
	m := passwordMigration()
	r := failingReconciler(t, interceptor.Funcs{}, m)
	_, _, err := r.ensureJob(context.Background(), m, "child", func() (*batchv1.Job, error) {
		return nil, errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("a builder failure must propagate, got %v", err)
	}

	r = failingReconciler(t, interceptor.Funcs{}, m)
	r.Scheme = runtime.NewScheme()
	_, _, err = r.ensureJob(context.Background(), m, "child", func() (*batchv1.Job, error) {
		return namedJob("child"), nil
	})
	if err == nil {
		t.Fatal("an owner-reference failure must propagate")
	}

	exists := apierrors.NewAlreadyExists(schema.GroupResource{Resource: "jobs"}, "child")
	r = failingReconciler(t, interceptor.Funcs{
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			return exists
		},
	}, m)
	job, created, err := r.ensureJob(context.Background(), m, "child", func() (*batchv1.Job, error) {
		return namedJob("child"), nil
	})
	if job != nil || created || err != nil {
		t.Fatalf("losing the create race must read as (nil, false, nil), got (%v, %v, %v)", job, created, err)
	}
}

// TestFinishFollow_ErrorLegs covers every failure and pending leg of the
// post-drain flow: verify, cleanup, and verification each unreadable or
// pending with the status write failing.
func TestFinishFollow_ErrorLegs(t *testing.T) {
	newM := func() *v1beta1.Migration { return followPasswordMigration() }
	ctx := context.Background()

	m := newM()
	r := failingReconciler(t, failGetOf(verifyJobName(m)), m)
	if _, err := r.finishFollow(ctx, m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("verify leg: %v", err)
	}

	m = newM()
	r = failingReconciler(t, failStatusPatch(), m)
	if _, err := r.finishFollow(ctx, m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("verify-pending status leg: %v", err)
	}

	m = newM()
	r = failingReconciler(t, failGetOf(cleanupJobName(m)), m, completeJob(verifyJobName(m)))
	if _, err := r.finishFollow(ctx, m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("cleanup leg: %v", err)
	}

	m = newM()
	r = failingReconciler(t, failStatusPatch(), m, completeJob(verifyJobName(m)))
	if _, err := r.finishFollow(ctx, m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("cleanup-pending status leg: %v", err)
	}

	m = newM()
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	r = failingReconciler(t, failGetOf(compareJobName(m, compareSchema)), m,
		completeJob(verifyJobName(m)), completeJob(cleanupJobName(m)))
	if _, err := r.finishFollow(ctx, m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("verification leg: %v", err)
	}

	m = newM()
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	r = failingReconciler(t, failStatusPatch(), m,
		completeJob(verifyJobName(m)), completeJob(cleanupJobName(m)))
	if _, err := r.finishFollow(ctx, m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("verification-pending status leg: %v", err)
	}
}

// TestFinishClone_ErrorLegs: the clone-side verification legs, mirroring the
// follow flow above.
func TestFinishClone_ErrorLegs(t *testing.T) {
	m := passwordMigration()
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	r := failingReconciler(t, failGetOf(compareJobName(m, compareSchema)), m)
	if _, err := r.finishClone(context.Background(), m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("verification leg: %v", err)
	}

	m = passwordMigration()
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	r = failingReconciler(t, failStatusPatch(), m)
	if _, err := r.finishClone(context.Background(), m, m.DeepCopy()); !errors.Is(err, errBoom) {
		t.Fatalf("verification-pending status leg: %v", err)
	}
}

// TestReconcileDeletion_Errors: stopping the worker and running cleanup can
// each fail; both must keep the finalizer in place by surfacing the error.
func TestReconcileDeletion_Errors(t *testing.T) {
	newM := func() *v1beta1.Migration {
		m := followPasswordMigration()
		m.Finalizers = []string{finalizerName}
		m.Status.Attempts = 1
		m.Status.JobName = workerJob
		return m
	}

	m := newM()
	r := failingReconciler(t, interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return errBoom
		},
	}, m, namedJob(workerJob))
	if _, err := r.reconcileDeletion(context.Background(), m); !errors.Is(err, errBoom) {
		t.Fatalf("a worker Delete failure must propagate, got %v", err)
	}

	m = newM()
	r = failingReconciler(t, failGetOf(cleanupJobName(m)), m)
	if _, err := r.reconcileDeletion(context.Background(), m); !errors.Is(err, errBoom) {
		t.Fatalf("a cleanup failure must propagate, got %v", err)
	}
}

// TestMaxCatchupLag_Default pins the fallback for an unset
// spec.follow.maxCatchupLag.
func TestMaxCatchupLag_Default(t *testing.T) {
	if got := maxCatchupLagBytes(passwordMigration()); got != defaultMaxCatchupLag {
		t.Fatalf("default lag = %d, want %d", got, defaultMaxCatchupLag)
	}
}
