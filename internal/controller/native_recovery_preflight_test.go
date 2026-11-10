package controller

import (
	"context"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const nativeRecoveryRetryCase = "retry"
const nativeRecoveryDeleteCase = "delete"

type nativeRecoveryAdmission interface {
	nativeRecoveryQuiescentCurrent(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim, *corev1.Pod) (*v1beta1.Migration, *corev1.Pod, error)
}

func TestNativeRecoveryAdmission(t *testing.T) {
	for _, intent := range []string{nativeRecoveryRetryCase, probeSuspendCase, nativeRecoveryDeleteCase} {
		t.Run(intent, func(t *testing.T) {
			f := newNativeCloneFixture(t, true, nil)
			f.worker.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
			f.replace(f.worker)
			f.pod = nativeRecoveryTestPod(f.worker, 1)
			f.replace(f.pod)
			f.m.Status.Replication = nil
			if intent == probeSuspendCase {
				f.m.Spec.Suspend = true
			}
			f.replace(f.m)
			if intent == nativeRecoveryDeleteCase {
				f.m.Finalizers = []string{finalizerName}
				f.replace(f.m)
				if err := f.store.Delete(context.Background(), f.m); err != nil {
					t.Fatal(err)
				}
				if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), f.m); err != nil {
					t.Fatal(err)
				}
			}
			admit, ok := any(f.r).(nativeRecoveryAdmission)
			if !ok {
				t.Fatal("recovery-specific admission is missing")
			}
			if live, pod, err := admit.nativeRecoveryQuiescentCurrent(context.Background(), f.m, f.worker, f.pvc, f.pod); err != nil || live == nil || pod == nil {
				t.Fatal("confirmed no-E failed execution refused", err)
			}
			original := f.m.DeepCopy()
			f.m.Generation++
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if live, pod, err := admit.nativeRecoveryQuiescentCurrent(context.Background(), original, f.worker, f.pvc, f.pod); err == nil || live != nil || pod != nil {
				t.Fatal("changed intent generation admitted")
			}
		})
	}
}
