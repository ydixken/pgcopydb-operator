package podexec

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"reflect"
	"slices"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// NativeExecutionSHA256 correlates an admitted spec without literal env values or template metadata.
// Callers compare omitted fields directly in memory; the hash cannot admit an execution.
// Commands and arguments must be credential-free under the canonical builder contract.
func NativeExecutionSHA256(template corev1.PodTemplateSpec) [32]byte {
	spec := template.Spec.DeepCopy()
	projectEnv := func(env []corev1.EnvVar) {
		for i := range env {
			env[i].Value = ""
		}
	}
	for i := range spec.Containers {
		projectEnv(spec.Containers[i].Env)
	}
	for i := range spec.InitContainers {
		projectEnv(spec.InitContainers[i].Env)
	}
	for i := range spec.EphemeralContainers {
		projectEnv(spec.EphemeralContainers[i].Env)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}

// JobVersionNative captures original version bytes without granting completion authority.
func (e *Exec) JobVersionNative(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error) {
	return e.nativeCompletedOutput(ctx, namespace, jobUID, want, true)
}

// JobPreflightNative captures original preflight bytes for the caller's strict parser.
func (e *Exec) JobPreflightNative(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error) {
	return e.nativeCompletedOutput(ctx, namespace, jobUID, want, false)
}

func (e *Exec) nativeCompletedOutput(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding, version bool) (NativeOutcome, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	if jobUID == "" || want.ExecutionSHA256 == ([32]byte{}) {
		return NativeOutcome{}, errNativeTransport
	}
	before, err := e.nativeOutputIncarnation(ctx, namespace, jobUID, want)
	if err != nil || !before.outputBinding(want, version) {
		return NativeOutcome{}, errNativeTransport
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := before.completed()
		if err != nil {
			return NativeOutcome{}, errNativeTransport
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			return NativeOutcome{}, errNativeTransport
		case <-ticker.C:
		}
		next, err := e.nativeOutputIncarnation(ctx, namespace, jobUID, want)
		if err != nil || !before.same(next) || !next.outputBinding(want, version) {
			return NativeOutcome{}, errNativeTransport
		}
		before = next
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
	if err != nil || !before.same(after) || !after.outputBinding(want, version) || !reflect.DeepEqual(before.pod.Status, after.pod.Status) || !reflect.DeepEqual(before.job.Status, after.job.Status) || ctx.Err() != nil {
		return NativeOutcome{}, errNativeTransport
	}
	if ready, err := after.completed(); err != nil || !ready {
		return NativeOutcome{}, errNativeTransport
	}
	return NativeOutcome{payload: payload, successful: true}, nil
}

func (e *Exec) nativeOutputIncarnation(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (nativeIncarnation, error) {
	if namespace == "" || jobUID == "" || want.PodUID == "" {
		return nativeIncarnation{}, errNativeTransport
	}
	var original *corev1.Pod
	var resourceVersion string
	options := metav1.ListOptions{Limit: 100}
	seen := map[string]bool{}
	for {
		pods, err := e.clientset.CoreV1().Pods(namespace).List(ctx, options)
		if err != nil || ctx.Err() != nil || pods.ResourceVersion == "" || pods.ShardInfo != nil || (resourceVersion != "" && resourceVersion != pods.ResourceVersion) {
			return nativeIncarnation{}, errNativeTransport
		}
		resourceVersion = pods.ResourceVersion
		for i := range pods.Items {
			pod := &pods.Items[i]
			for _, owner := range pod.OwnerReferences {
				if owner.UID != jobUID {
					continue
				}
				if original != nil || pod.UID != want.PodUID || pod.Namespace != namespace || pod.Name == "" {
					return nativeIncarnation{}, errNativeTransport
				}
				original = pod
				break
			}
		}
		if pods.Continue == "" {
			break
		}
		if seen[pods.Continue] {
			return nativeIncarnation{}, errNativeTransport
		}
		seen[pods.Continue] = true
		options.Continue = pods.Continue
	}
	if original == nil {
		return nativeIncarnation{}, errNativeTransport
	}
	n, err := e.nativeIncarnation(ctx, namespace, original.Name, want)
	if err != nil || n.job.UID != jobUID || n.pod.Labels[batchv1.ControllerUidLabel] != string(jobUID) {
		return nativeIncarnation{}, errNativeTransport
	}
	return n, nil
}

func (n nativeIncarnation) outputBinding(want NativeContainerBinding, version bool) bool {
	if NativeExecutionSHA256(n.job.Spec.Template) != want.ExecutionSHA256 || !reflect.DeepEqual(n.job.Spec.Template.Spec.Containers, n.pod.Spec.Containers) {
		return false
	}
	for _, condition := range n.job.Status.Conditions {
		if condition.Type == batchv1.JobFailureTarget && condition.Status == corev1.ConditionTrue {
			return false
		}
	}
	c := n.pod.Spec.Containers[0]
	return !version || (slices.Equal(c.Command, []string{"/usr/local/bin/rpgmigrate"}) && slices.Equal(c.Args, []string{"version", "--json"}))
}
