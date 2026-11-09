package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type nativeRecoveryFixture struct {
	t         *testing.T
	r         *MigrationReconciler
	store     client.WithWatch
	m         *v1beta1.Migration
	worker    *batchv1.Job
	pvc       *corev1.PersistentVolumeClaim
	pod       *corev1.Pod
	reads     int
	deletes   int
	bounded   bool
	beforeGet func(client.Object, int)
	onDelete  func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error
}

func newNativeRecoveryFixture(t *testing.T, attempt int32) *nativeRecoveryFixture {
	t.Helper()
	m, pvc := nativeIdentityObjects()
	m.Generation = 1
	m.ResourceVersion = "1"
	m.Spec.Source, m.Spec.Target = passwordMigration().Spec.Source, passwordMigration().Spec.Target
	m.Status.Attempts, m.Status.JobName = attempt, jobName(m, attempt)
	m.Status.Phase = v1beta1.PhaseCloning
	m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}
	pvc.Status.Phase = corev1.ClaimBound
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	worker, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, attempt)
	if err != nil || controllerutil.SetControllerReference(m, worker, scheme) != nil {
		t.Fatalf("build original worker: %v", err)
	}
	worker.UID = "recovery-original-job"
	worker.Status.Failed = 1
	worker.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	pod := nativeRecoveryTestPod(worker, 1)
	store := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(m, pvc, worker, pod).Build()
	f := &nativeRecoveryFixture{t: t, store: store, m: m, pvc: pvc, worker: worker, pod: pod}
	f.r = &MigrationReconciler{APIReader: nativeRecoveryTestReader{f: f}, Scheme: scheme, RPGMigrateRunnerImage: nativeEvidenceTestImage}
	f.r.Client = interceptor.NewClient(store, interceptor.Funcs{
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			t.Fatal("recovery guard created work")
			return errBoom
		},
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			t.Fatal("recovery guard patched status or metadata")
			return errBoom
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			f.deletes++
			options := &client.DeleteOptions{}
			for _, option := range opts {
				option.ApplyToDelete(options)
			}
			if obj.GetUID() != f.worker.UID || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != f.worker.UID || options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
				t.Fatal("native stop lost original UID or foreground propagation")
			}
			if f.onDelete != nil {
				return f.onDelete(ctx, c, obj, opts...)
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	f.r.Sentinel, f.r.Progress, f.r.Logs = &fakeSentinel{}, &fakeProgress{}, &fakeLogs{}
	t.Cleanup(func() {
		if f.r.Sentinel.(*fakeSentinel).callCount() != 0 || f.r.Progress.(*fakeProgress).counts() != 0 || f.r.Logs.(*fakeLogs).calls != 0 {
			t.Error("native recovery reached legacy observation or SQL")
		}
	})
	return f
}

func nativeRecoveryTestPod(worker *batchv1.Job, exitCode int32) *corev1.Pod {
	controller, block := true, true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: worker.Name + "-original", Namespace: worker.Namespace, UID: "recovery-original-pod",
		Labels: worker.Spec.Template.Labels, Annotations: worker.Spec.Template.Annotations,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: "Job", Name: worker.Name, UID: worker.UID, Controller: &controller, BlockOwnerDeletion: &block}},
	}, Spec: *worker.Spec.Template.Spec.DeepCopy()}
	pod.Spec.NodeName = "test-node"
	phase := corev1.PodSucceeded
	if exitCode != 0 {
		phase = corev1.PodFailed
	}
	image := pod.Spec.Containers[0].Image
	pod.Status = corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer, Image: image, ImageID: image, ContainerID: "containerd://original", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode, ContainerID: "containerd://original"}}}}}
	return pod
}

type nativeRecoveryTestReader struct{ f *nativeRecoveryFixture }

func (r nativeRecoveryTestReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if r.f.bounded {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 30*time.Second {
			r.f.t.Error("private recovery observation has no bounded deadline")
		}
	}
	r.f.reads++
	if r.f.beforeGet != nil {
		r.f.beforeGet(obj, r.f.reads)
	}
	return r.f.store.Get(ctx, key, obj, opts...)
}

func (r nativeRecoveryTestReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.f.store.List(ctx, list, opts...); err != nil {
		return err
	}
	list.(*corev1.PodList).ResourceVersion = "native-recovery-snapshot"
	return nil
}

func (f *nativeRecoveryFixture) replace(object client.Object) {
	f.t.Helper()
	old := object.DeepCopyObject().(client.Object)
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(object), old); err != nil {
		f.t.Fatal(err)
	}
	if err := f.store.Delete(context.Background(), old); err != nil {
		f.t.Fatal(err)
	}
	object.SetResourceVersion("")
	if err := f.store.Create(context.Background(), object); err != nil {
		f.t.Fatal(err)
	}
}

func TestNativeRecoveryRoutes(t *testing.T) {
	for _, attempt := range []int32{1, 2, 5} {
		t.Run(jobName(&v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: "attempt"}}, attempt), func(t *testing.T) {
			f := newNativeRecoveryFixture(t, attempt)
			if ok, err := f.r.nativeWorkerQuiescent(context.Background(), f.m, f.worker, f.pvc); err != nil || !ok {
				t.Fatalf("original failed worker was not quiescent: %t, %v", ok, err)
			}
			if f.deletes != 0 {
				t.Fatal("quiescence guard deleted its audit trail")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*nativeRecoveryFixture)
	}{
		{"missing job", func(f *nativeRecoveryFixture) { _ = f.store.Delete(context.Background(), f.worker) }},
		{"replacement job", func(f *nativeRecoveryFixture) { j := f.worker.DeepCopy(); j.UID = workerReplacementUID; f.replace(j) }},
		{"missing pod", func(f *nativeRecoveryFixture) { _ = f.store.Delete(context.Background(), f.pod) }},
		{"duplicate original pods", func(f *nativeRecoveryFixture) {
			p := f.pod.DeepCopy()
			p.Name, p.UID, p.ResourceVersion = "duplicate-original", "duplicate-original", ""
			if err := f.store.Create(context.Background(), p); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"foreign pod", func(f *nativeRecoveryFixture) {
			p := f.pod.DeepCopy()
			p.OwnerReferences[0].UID = workerForeignIdentity
			f.replace(p)
		}},
		{"restarted container", func(f *nativeRecoveryFixture) {
			p := f.pod.DeepCopy()
			p.Status.ContainerStatuses[0].RestartCount = 1
			f.replace(p)
		}},
		{"unknown container", func(f *nativeRecoveryFixture) {
			p := f.pod.DeepCopy()
			p.Status.ContainerStatuses[0].State.Terminated = nil
			f.replace(p)
		}},
		{"wrong image", func(f *nativeRecoveryFixture) {
			p := f.pod.DeepCopy()
			p.Status.ContainerStatuses[0].ImageID = string(workerReplacementUID)
			f.replace(p)
		}},
		{"replacement pvc", func(f *nativeRecoveryFixture) { p := f.pvc.DeepCopy(); p.UID = workerReplacementUID; f.replace(p) }},
		{"changed E", func(f *nativeRecoveryFixture) {
			m := f.m.DeepCopy()
			m.Status.Replication.Endpos = lifecycleChangedEnd
			f.replace(m)
		}},
		{"changed attempt", func(f *nativeRecoveryFixture) { m := f.m.DeepCopy(); m.Status.Attempts++; f.replace(m) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeRecoveryFixture(t, 2)
			tc.change(f)
			if ok, err := f.r.nativeWorkerQuiescent(context.Background(), f.m, f.worker, f.pvc); ok || err == nil {
				t.Fatalf("unobserved original execution was admitted: %t, %v", ok, err)
			}
		})
	}
}

func TestNativeRecoveryCrashBoundaries(t *testing.T) {
	for _, kind := range []string{workerReadMigration, workerReadJob, workerReadPVC, "original-pod"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeRecoveryFixture(t, 2)
			changed := false
			f.beforeGet = func(obj client.Object, n int) {
				if changed || n < 5 {
					return
				}
				changed = true
				switch kind {
				case workerReadMigration:
					m := f.m.DeepCopy()
					m.Status.RetryGate = &v1beta1.RetryGateStatus{FailedAttempt: 2, Exemption: v1beta1.RetryGateCounted}
					f.replace(m)
				case workerReadJob:
					j := f.worker.DeepCopy()
					j.UID = workerReplacementUID
					f.replace(j)
				case workerReadPVC:
					p := f.pvc.DeepCopy()
					p.UID = workerReplacementUID
					f.replace(p)
				case "original-pod":
					p := f.pod.DeepCopy()
					p.UID = workerReplacementUID
					f.replace(p)
				}
			}
			if ok, err := f.r.nativeWorkerQuiescent(context.Background(), f.m, f.worker, f.pvc); ok || err == nil || !changed {
				t.Fatalf("final readmission ignored replacement: %t, %v", ok, err)
			}
		})
	}
	t.Run("active original is not terminal", func(t *testing.T) {
		f := newNativeRecoveryFixture(t, 2)
		j := f.worker.DeepCopy()
		j.Status = batchv1.JobStatus{Active: 1}
		f.replace(j)
		f.worker = j
		if ok, err := f.r.nativeWorkerQuiescent(context.Background(), f.m, f.worker, f.pvc); ok || err != nil {
			t.Fatalf("active worker did not hold quiescence: %t, %v", ok, err)
		}
		f.bounded = true
		if _, err := f.r.nativeQuiescentWorker(context.Background(), f.m, f.worker, f.pvc); !errors.Is(err, errNativeRecoveryActive) {
			t.Fatal("private terminal consumer did not hold active worker")
		}
	})
	t.Run("terminating successful original is not quiescent", func(t *testing.T) {
		f := newNativeRecoveryFixture(t, 1)
		terminating := int32(1)
		j := f.worker.DeepCopy()
		j.Status = batchv1.JobStatus{Succeeded: 1, Terminating: &terminating, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
		f.replace(j)
		f.worker = j
		f.replace(nativeRecoveryTestPod(j, 0))
		if ok, _ := f.r.nativeWorkerQuiescent(context.Background(), f.m, f.worker, f.pvc); ok {
			t.Fatal("terminating original supplied quiescence")
		}
	})
}

func TestNativeSuspendResume(t *testing.T) {
	t.Run("same invocation positively observes original absence", func(t *testing.T) {
		f := newNativeRecoveryFixture(t, 2)
		f.m.Spec.Suspend = true
		f.replace(f.m)
		f.onDelete = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := c.Delete(ctx, f.pod); err != nil {
				return err
			}
			return c.Delete(ctx, obj, opts...)
		}
		if stopped, err := f.r.stopNativeWorker(context.Background(), f.m, f.worker, f.pvc); err != nil || !stopped || f.deletes != 1 {
			t.Fatalf("original foreground stop failed: %t, %v, deletes=%d", stopped, err, f.deletes)
		}
		if stopped, err := f.r.stopNativeWorker(context.Background(), f.m, f.worker, f.pvc); stopped || err == nil || f.deletes != 1 {
			t.Fatal("later missing-name observation reused vanished deletion authority")
		}
	})
	t.Run("successful original stays for completed Reader", func(t *testing.T) {
		f := newNativeRecoveryFixture(t, 1)
		j := f.worker.DeepCopy()
		j.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
		f.replace(j)
		f.worker = j
		if stopped, err := f.r.stopNativeWorker(context.Background(), f.m, f.worker, f.pvc); stopped || err == nil || f.deletes != 0 {
			t.Fatal("successful worker was deleted before evidence Reader")
		}
	})
	t.Run("ambiguous successful status retains original", func(t *testing.T) {
		f := newNativeRecoveryFixture(t, 1)
		j := f.worker.DeepCopy()
		j.Status = batchv1.JobStatus{Succeeded: 1}
		f.replace(j)
		f.worker = j
		if stopped, err := f.r.stopNativeWorker(context.Background(), f.m, f.worker, f.pvc); stopped || err == nil || f.deletes != 0 {
			t.Fatal("ambiguous completion was deleted before Reader")
		}
	})
	t.Run("unknown delete reply grants no absence", func(t *testing.T) {
		f := newNativeRecoveryFixture(t, 2)
		f.onDelete = func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return errors.New(lifecycleCanary)
		}
		if stopped, err := f.r.stopNativeWorker(context.Background(), f.m, f.worker, f.pvc); stopped || err == nil || strings.Contains(err.Error(), lifecycleCanary) {
			t.Fatal("unknown deletion became success or exposed diagnostics")
		}
	})
	t.Run("retained UID replacement is never deleted", func(t *testing.T) {
		f := newNativeRecoveryFixture(t, 2)
		j := f.worker.DeepCopy()
		j.UID = workerReplacementUID
		f.replace(j)
		if stopped, err := f.r.stopNativeWorker(context.Background(), f.m, f.worker, f.pvc); stopped || err == nil || f.deletes != 0 {
			t.Fatal("replacement was deleted by name")
		}
	})
}
