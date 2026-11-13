package podexec

import (
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

// SamePodState compares two reads of one pod by UID, phase and the named container's state.
// The kubelet keeps rewriting other status fields, such as status.resources, after the pod Succeeded.
func SamePodState(a, b *corev1.Pod, container string) bool {
	if container == "" || a == nil || b == nil || a.UID == "" || a.UID != b.UID || a.Status.Phase != b.Status.Phase {
		return false
	}
	x, okA := namedContainerState(a, container)
	y, okB := namedContainerState(b, container)
	return okA && okB && apiequality.Semantic.DeepEqual(x, y)
}

func namedContainerState(p *corev1.Pod, name string) (corev1.ContainerState, bool) {
	var state corev1.ContainerState
	found := 0
	for _, s := range p.Status.ContainerStatuses {
		if s.Name == name {
			state, found = s.State, found+1
		}
	}
	return state, found == 1
}
