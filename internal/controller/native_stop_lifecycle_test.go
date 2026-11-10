package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	nativeStopFinalReplacement = "final live replacement"
	nativeStopMissingReceipt   = "missing receipt"
	nativeStopWrongReceipt     = "wrong receipt"
	nativeStopHalfReceipt      = "half receipt"
	nativeStopMissingJob       = "missing job"
	nativeStopMissingPod       = "missing pod"
	nativeStopAmbiguousPod     = "ambiguous pod"
	nativeStopReplacementJob   = "replacement job"
	nativeStopReplacementPVC   = "replacement pvc"
)

const nativeStopJobUID types.UID = "33333333-3333-4333-8333-333333333333"

type nativeTermFunc func(context.Context, string, string, podexec.NativeContainerBinding) error

func (f nativeTermFunc) RequestNativeTerm(ctx context.Context, ns, pod string, binding podexec.NativeContainerBinding) error {
	return f(ctx, ns, pod, binding)
}

func nativeRetainedStopFixture(t *testing.T) *nativeRecoveryFixture {
	t.Helper()
	f := newNativeRecoveryFixture(t, 2)
	f.worker.UID, f.worker.Generation = nativeStopJobUID, 1
	f.worker.Status = batchv1.JobStatus{Active: 1}
	f.pod = nativeRecoveryTestPod(f.worker, 1)
	f.pod.Status.Phase = corev1.PodRunning
	f.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	f.m.Status.NativeJobUID, f.m.Status.NativePVCUID = string(f.worker.UID), string(f.pvc.UID)
	f.replace(f.worker)
	f.replace(f.pod)
	f.replace(f.m)
	return f
}

func markNativeStopTerminal(f *nativeRecoveryFixture, success bool) {
	f.t.Helper()
	worker := f.worker.DeepCopy()
	worker.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
	exit := int32(1)
	if success {
		exit = 0
		worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	}
	f.replace(worker)
	f.replace(nativeRecoveryTestPod(worker, exit))
}

func TestNativeRetainedStop(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "reply", true: "lost reply"}[lost], func(t *testing.T) {
			f := nativeRetainedStopFixture(t)
			calls := 0
			f.r.NativeStop = nativeTermFunc(func(ctx context.Context, ns, pod string, binding podexec.NativeContainerBinding) error {
				calls++
				deadline, bounded := ctx.Deadline()
				if !bounded || time.Until(deadline) > 30*time.Second || ns != f.m.Namespace || pod != f.pod.Name || binding.PodUID != f.pod.UID || binding.ContainerID != f.pod.Status.ContainerStatuses[0].ContainerID || binding.RestartCount != 0 || binding.TemplateSHA256 == ([32]byte{}) || binding.ExecutionSHA256 == ([32]byte{}) {
					t.Fatal("TERM lost its bound original invocation")
				}
				markNativeStopTerminal(f, false)
				if lost {
					return errors.New("private request failure")
				}
				return nil
			})
			done, err := f.r.stopNativeRetained(context.Background(), f.m, f.worker, f.pvc)
			if err != nil || !done || calls != 1 || f.deletes != 0 {
				t.Fatalf("retained terminal observation done=%v err=%v calls=%d deletes=%d", done, err, calls, f.deletes)
			}
			for _, object := range []client.Object{&batchv1.Job{}, &corev1.Pod{}, &corev1.PersistentVolumeClaim{}} {
				key := client.ObjectKeyFromObject(f.worker)
				if _, ok := object.(*corev1.Pod); ok {
					key = client.ObjectKeyFromObject(f.pod)
				}
				if _, ok := object.(*corev1.PersistentVolumeClaim); ok {
					key = client.ObjectKeyFromObject(f.pvc)
				}
				if err := f.store.Get(context.Background(), key, object); err != nil {
					t.Fatal("retained stop discarded evidence", err)
				}
			}
			live := &v1beta1.Migration{}
			if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil || nativeControlEnd(live) != lifecycleEnd || live.Status.NativeJobUID != f.m.Status.NativeJobUID || live.Status.NativePVCUID != f.m.Status.NativePVCUID {
				t.Fatal("retained stop changed E or receipts", err)
			}
		})
	}
	t.Run("natural completion racing TERM supplies no completion status", func(t *testing.T) {
		f := nativeRetainedStopFixture(t)
		f.r.NativeStop = nativeTermFunc(func(context.Context, string, string, podexec.NativeContainerBinding) error {
			markNativeStopTerminal(f, true)
			return errBoom
		})
		if done, err := f.r.stopNativeRetained(context.Background(), f.m, f.worker, f.pvc); !done || err != nil {
			t.Fatalf("natural terminal quiescence was refused: %t %v", done, err)
		}
		live := &v1beta1.Migration{}
		if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live) != nil || live.Status.Phase != v1beta1.PhaseCloning {
			t.Fatal("quiescence claimed migration completion")
		}
	})
	t.Run("prior terminal needs no request", func(t *testing.T) {
		f := nativeRetainedStopFixture(t)
		markNativeStopTerminal(f, false)
		f.r.NativeStop = nativeTermFunc(func(context.Context, string, string, podexec.NativeContainerBinding) error {
			t.Fatal("already terminal worker was signaled")
			return nil
		})
		if done, err := f.r.stopNativeRetained(context.Background(), f.m, f.worker, f.pvc); !done || err != nil {
			t.Fatalf("terminal worker refused: %t %v", done, err)
		}
	})
}

func TestNativeRetainedStopIntent(t *testing.T) {
	for _, intent := range []string{probeSuspendCase, "delete"} {
		t.Run(intent, func(t *testing.T) {
			f := nativeRetainedStopFixture(t)
			f.m.Generation++
			if intent == probeSuspendCase {
				f.m.Spec.Suspend = true
			} else {
				f.m.Finalizers = []string{"test/retention"}
				f.m.DeletionTimestamp = &metav1.Time{Time: time.Unix(10, 0)}
			}
			f.replace(f.m)
			f.r.NativeStop = nativeTermFunc(func(context.Context, string, string, podexec.NativeContainerBinding) error {
				markNativeStopTerminal(f, false)
				return nil
			})
			if done, err := f.r.stopNativeRetained(context.Background(), f.m, f.worker, f.pvc); !done || err != nil || f.deletes != 0 {
				t.Fatalf("exact stop intent refused: %t %v deletes=%d", done, err, f.deletes)
			}
		})
	}
}

func changeNativeStopFixture(f *nativeRecoveryFixture, fault string) {
	f.t.Helper()
	switch fault {
	case nativeStopMissingReceipt, nativeStopWrongReceipt, nativeStopHalfReceipt:
		if fault == nativeStopWrongReceipt {
			f.m.Status.NativeJobUID = string(workerReplacementUID)
		} else {
			f.m.Status.NativeJobUID = ""
		}
		if fault == nativeStopMissingReceipt {
			f.m.Status.NativePVCUID = ""
		}
		f.replace(f.m)
	case nativeStopMissingJob:
		if err := f.store.Delete(context.Background(), f.worker); err != nil {
			f.t.Fatal(err)
		}
	case nativeStopMissingPod:
		if err := f.store.Delete(context.Background(), f.pod); err != nil {
			f.t.Fatal(err)
		}
	case nativeStopAmbiguousPod:
		p := f.pod.DeepCopy()
		p.Name += "-other"
		p.UID = workerReplacementUID
		p.ResourceVersion = ""
		if err := f.store.Create(context.Background(), p); err != nil {
			f.t.Fatal(err)
		}
	case nativeStopReplacementJob:
		j := f.worker.DeepCopy()
		j.UID = workerReplacementUID
		f.replace(j)
	case nativeStopReplacementPVC:
		p := f.pvc.DeepCopy()
		p.UID = workerReplacementUID
		f.replace(p)
	case "suspend generation":
		m := f.m.DeepCopy()
		m.Spec.Suspend = true
		m.Generation++
		f.replace(m)
	case "live read failure":
		f.r.APIReader = interceptor.NewClient(f.store, interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errBoom
		}})
	default:
		p := f.pod.DeepCopy()
		switch fault {
		case "replacement pod", nativeStopFinalReplacement:
			p.UID = workerReplacementUID
		case "replacement container":
			p.Status.ContainerStatuses[0].ContainerID = "containerd://replacement"
		case nativeControlRestartCase:
			p.Status.ContainerStatuses[0].RestartCount = 1
		case probeImageCase:
			p.Status.ContainerStatuses[0].ImageID = "unexpected"
		case "spec":
			p.Spec.Containers[0].Command = []string{"unexpected-worker-program"}
		}
		f.replace(p)
	}
}

func TestNativeRetainedStopRefusals(t *testing.T) {
	for _, fault := range []string{nativeStopMissingReceipt, nativeStopWrongReceipt, nativeStopHalfReceipt, nativeStopMissingJob, nativeStopMissingPod, nativeStopAmbiguousPod, nativeStopReplacementJob, nativeStopReplacementPVC, "replacement pod", "replacement container", nativeControlRestartCase, probeImageCase, "spec", "suspend generation", nativeStopFinalReplacement, "live read failure"} {
		t.Run(fault, func(t *testing.T) {
			f := nativeRetainedStopFixture(t)
			calls := 0
			change := func() { changeNativeStopFixture(f, fault) }
			f.r.NativeStop = nativeTermFunc(func(context.Context, string, string, podexec.NativeContainerBinding) error {
				calls++
				markNativeStopTerminal(f, false)
				if fault == nativeStopFinalReplacement {
					terminalReads := 0
					f.beforeGet = func(object client.Object, _ int) {
						if _, ok := object.(*corev1.Pod); ok {
							terminalReads++
						}
						if terminalReads == 2 {
							f.beforeGet = nil
							change()
						}
					}
				} else {
					change()
				}
				return nil
			})
			if fault == nativeStopMissingReceipt || fault == nativeStopWrongReceipt || fault == nativeStopHalfReceipt || fault == nativeStopMissingJob || fault == nativeStopMissingPod || fault == nativeStopAmbiguousPod {
				change()
			}
			done, err := f.r.stopNativeRetained(context.Background(), f.m, f.worker, f.pvc)
			if done || err == nil || calls > 1 || f.deletes != 0 {
				t.Fatalf("changed execution accepted: %t %v calls=%d deletes=%d", done, err, calls, f.deletes)
			}
		})
	}
}

func TestNativeRetainedStopTimeout(t *testing.T) {
	for _, requestError := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil reply", true: "failed reply"}[requestError], func(t *testing.T) {
			f := nativeRetainedStopFixture(t)
			calls := 0
			f.r.NativeStop = nativeTermFunc(func(context.Context, string, string, podexec.NativeContainerBinding) error {
				calls++
				if requestError {
					return errBoom
				}
				return nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			if done, err := f.r.stopNativeRetained(ctx, f.m, f.worker, f.pvc); done || err == nil || calls != 1 || f.deletes != 0 {
				t.Fatalf("request reply created stop authority: %t %v calls=%d deletes=%d", done, err, calls, f.deletes)
			}
		})
	}
}
