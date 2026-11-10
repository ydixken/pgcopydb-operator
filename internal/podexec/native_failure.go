package podexec

import (
	"context"
	"io"
	"reflect"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// JobFailureNative captures complete original failed-worker logs without granting failure authority.
func (e *Exec) JobFailureNative(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	if jobUID == "" || want.ExecutionSHA256 == ([32]byte{}) {
		return NativeOutcome{}, errNativeTransport
	}
	before, err := e.nativeOutputIncarnation(ctx, namespace, jobUID, want)
	if err != nil || !before.failureOutputBinding(want) {
		return NativeOutcome{}, errNativeTransport
	}
	r, err := e.clientset.CoreV1().Pods(namespace).GetLogs(before.pod.Name, &corev1.PodLogOptions{Container: want.ContainerName}).Stream(ctx)
	if err != nil {
		return NativeOutcome{}, errNativeTransport
	}
	// One spare byte distinguishes underlying EOF from the limit reader's synthetic EOF.
	payload, readErr := io.ReadAll(io.LimitReader(r, nativePayloadLimit+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(payload) > nativePayloadLimit || ctx.Err() != nil {
		return NativeOutcome{}, errNativeTransport
	}
	after, err := e.nativeOutputIncarnation(ctx, namespace, jobUID, want)
	if err != nil || !before.same(after) || !after.failureOutputBinding(want) || !reflect.DeepEqual(before.pod.Status, after.pod.Status) || !reflect.DeepEqual(before.job.Status, after.job.Status) || ctx.Err() != nil {
		return NativeOutcome{}, errNativeTransport
	}
	return NativeOutcome{payload: payload, successful: true}, nil
}

func (n nativeIncarnation) failureOutputBinding(want NativeContainerBinding) bool {
	if NativeExecutionSHA256(n.job.Spec.Template) != want.ExecutionSHA256 || !reflect.DeepEqual(n.job.Spec.Template.Spec.Containers, n.pod.Spec.Containers) || !reflect.DeepEqual(n.job.Spec.Template.Spec.Volumes, n.pod.Spec.Volumes) {
		return false
	}
	s := n.pod.Status.ContainerStatuses[0]
	termination := s.State.Terminated
	if n.pod.Spec.RestartPolicy != corev1.RestartPolicyNever || n.job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever || n.pod.Status.Phase != corev1.PodFailed || s.RestartCount != 0 || s.LastTerminationState != (corev1.ContainerState{}) || termination == nil || termination.ExitCode <= 0 || termination.Signal != 0 || termination.Reason != "Error" || termination.ContainerID != s.ContainerID {
		return false
	}
	status := n.job.Status
	if status.Active != 0 || status.Succeeded != 0 || status.Failed != 1 || (status.Terminating != nil && *status.Terminating != 0) || (status.Ready != nil && *status.Ready != 0) || (status.UncountedTerminatedPods != nil && (len(status.UncountedTerminatedPods.Failed) != 0 || len(status.UncountedTerminatedPods.Succeeded) != 0)) {
		return false
	}
	failed := 0
	for _, condition := range status.Conditions {
		switch condition.Type {
		case batchv1.JobFailed:
			if condition.Status != corev1.ConditionTrue {
				return false
			}
			failed++
		case batchv1.JobComplete, batchv1.JobSuccessCriteriaMet:
			if condition.Status == corev1.ConditionTrue {
				return false
			}
		}
	}
	return failed == 1
}
