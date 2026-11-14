package native

import (
	"encoding/json"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// capturedResourcesDrift returns status.resources of the two reads in the OP-TRANSPORT capture shared with podexec.
func capturedResourcesDrift(t *testing.T) (before, after *corev1.ResourceRequirements) {
	t.Helper()
	raw, err := os.ReadFile("../podexec/testdata/pod-status-drift.json")
	if err != nil {
		t.Fatal(err)
	}
	var capture struct{ Before, After corev1.PodStatus }
	if err := json.Unmarshal(raw, &capture); err != nil || capture.Before.Resources == nil {
		t.Fatal("captured drift unreadable", err)
	}
	return capture.Before.Resources, capture.After.Resources
}

func TestNativeAdmissionWorkerPodStatusDrift(t *testing.T) {
	before, after := capturedResourcesDrift(t)
	for _, tc := range []struct {
		name     string
		edit     func(b *corev1.Pod)
		admitted bool
	}{
		{"captured kubelet drift", func(*corev1.Pod) {}, true},
		{"phase", func(b *corev1.Pod) { b.Status.Phase = corev1.PodUnknown }, false},
		{"finished at", func(b *corev1.Pod) { b.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.Unix(1, 0) }, false},
		{"message", func(b *corev1.Pod) { b.Status.ContainerStatuses[0].State.Terminated.Message = "late" }, false},
	} {
		t.Run("owned/"+tc.name, func(t *testing.T) {
			f := newOwnedFixture(t)
			base, err := validateOwnedObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false)
			if err != nil {
				t.Fatal(err)
			}
			a, b := cloneOwnedAdmission(base), cloneOwnedAdmission(base)
			a.workerPod.Status.Resources, b.workerPod.Status.Resources = before.DeepCopy(), after.DeepCopy()
			tc.edit(b.workerPod)
			if a.same(b) != tc.admitted {
				t.Fatalf("admitted=%v", !tc.admitted)
			}
		})
		t.Run("retry/"+tc.name, func(t *testing.T) {
			f := newRetryFixture(t)
			base, err := validateRetryObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false)
			if err != nil {
				t.Fatal(err)
			}
			a, b := *base, *base
			a.workerPod, b.workerPod = base.workerPod.DeepCopy(), base.workerPod.DeepCopy()
			a.workerPod.Status.Resources, b.workerPod.Status.Resources = before.DeepCopy(), after.DeepCopy()
			tc.edit(b.workerPod)
			if a.same(&b) != tc.admitted {
				t.Fatalf("admitted=%v", !tc.admitted)
			}
		})
	}
}
