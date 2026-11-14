package podexec

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const podStateUID = "captured-pod"

// capturedStatusDrift returns the two reads of one Succeeded helper pod from the OP-TRANSPORT live replay (2026-11-13),
// where the kubelet dropped status.resources.requests.cpu (489m) between them.
func capturedStatusDrift(t *testing.T) (before, after corev1.PodStatus) {
	t.Helper()
	raw, err := os.ReadFile("testdata/pod-status-drift.json")
	if err != nil {
		t.Fatal(err)
	}
	var capture struct{ Before, After corev1.PodStatus }
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatal(err)
	}
	return capture.Before, capture.After
}

// withCapturedDrift gives two reads of p the pod-level resources of the captured before and after reads.
func withCapturedDrift(t *testing.T, p *corev1.Pod) (before, after *corev1.Pod) {
	t.Helper()
	b, a := capturedStatusDrift(t)
	before, after = p.DeepCopy(), p.DeepCopy()
	before.Status.Resources, after.Status.Resources = b.Resources, a.Resources
	return before, after
}

func TestSamePodStateCapturedDrift(t *testing.T) {
	b, a := capturedStatusDrift(t)
	if reflect.DeepEqual(b, a) {
		t.Fatal("captured reads do not differ")
	}
	trimmed := a.DeepCopy()
	trimmed.Resources = b.Resources
	if !reflect.DeepEqual(b, *trimmed) {
		t.Fatal("captured drift is wider than status.resources")
	}
	before := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podStateUID}, Status: b}
	after := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podStateUID}, Status: a}
	if !SamePodState(before, after, containerName) {
		t.Fatal("captured kubelet drift refused")
	}
}

func TestSamePodStateRefusals(t *testing.T) {
	b, _ := capturedStatusDrift(t)
	base := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podStateUID}, Status: b}
	if base.Status.ContainerStatuses[0].State.Terminated == nil {
		t.Fatal("captured container is not terminated")
	}
	for _, tc := range []struct {
		name      string
		container string
		edit      func(*corev1.Pod)
	}{
		{"pod replaced", containerName, func(p *corev1.Pod) { p.UID = nativeFixtureOther }},
		{"pod without uid", containerName, func(p *corev1.Pod) { p.UID = "" }},
		{"phase", containerName, func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed }},
		{"exit code", containerName, func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1 }},
		{"signal", containerName, func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.Signal = 9 }},
		{"reason", containerName, func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.Reason = "OOMKilled" }},
		{"message", containerName, func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.Message = "late" }},
		{"container id", containerName, func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.ContainerID = "containerd://other" }},
		{"started at", containerName, func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.StartedAt = metav1.Unix(1, 0) }},
		{"finished at", containerName, func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.Unix(1, 0) }},
		{"running again", containerName, func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{"waiting added", containerName, func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State.Waiting = &corev1.ContainerStateWaiting{}
		}},
		{"container missing", containerName, func(p *corev1.Pod) { p.Status.ContainerStatuses = nil }},
		{"container duplicated", containerName, func(p *corev1.Pod) {
			p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, p.Status.ContainerStatuses[0])
		}},
		{"other container named", nativeFixtureContainerName, func(*corev1.Pod) {}},
		{"no container named", "", func(*corev1.Pod) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := base.DeepCopy()
			tc.edit(changed)
			if SamePodState(base, changed, tc.container) || SamePodState(changed, base, tc.container) {
				t.Fatal("changed read admitted")
			}
		})
	}
	if SamePodState(nil, base, containerName) || SamePodState(base, nil, containerName) {
		t.Fatal("missing read admitted")
	}
	unnamed := base.DeepCopy()
	unnamed.Status.ContainerStatuses[0].Name = ""
	if SamePodState(unnamed, unnamed.DeepCopy(), "") {
		t.Fatal("unnamed container admitted")
	}
	anonymous := base.DeepCopy()
	anonymous.UID = ""
	if SamePodState(anonymous, anonymous.DeepCopy(), containerName) {
		t.Fatal("reads without pod uid admitted")
	}
	if !SamePodState(base, base.DeepCopy(), containerName) {
		t.Fatal("identical reads refused")
	}
	zone := base.DeepCopy()
	finished := &zone.Status.ContainerStatuses[0].State.Terminated.FinishedAt
	finished.Time = finished.In(time.FixedZone("decoded", 3600))
	if !SamePodState(base, zone, containerName) {
		t.Fatal("same finishedAt instant in another location refused")
	}
}
