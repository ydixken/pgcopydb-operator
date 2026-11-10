package native

import (
	"context"
	"reflect"
	"slices"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *Reader) admitCleaned(ctx context.Context, w ExecutionData, savedE string, completed bool) (*ownedAdmission, error) {
	if !validCleanedExecution(w) || ctx.Err() != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	m, pvc, job, worker := &v1beta1.Migration{}, &corev1.PersistentVolumeClaim{}, &batchv1.Job{}, &batchv1.Job{}
	for _, read := range []struct {
		name   string
		object client.Object
	}{{w.MigrationName, m}, {w.PVCName, pvc}, {w.JobName, job}} {
		if r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: read.name}, read.object) != nil {
			return nil, ErrNativeCleanedCompletionUnavailable
		}
	}
	if m.Status.JobName == "" || r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: m.Status.JobName}, worker) != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	pods := &corev1.PodList{}
	if r.api.List(ctx, pods, client.InNamespace(w.Namespace)) != nil || pods.Continue != "" || pods.ResourceVersion == "" || pods.ShardInfo != nil || pods.RemainingItemCount != nil && *pods.RemainingItemCount != 0 {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	var helperPod, workerPod *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		for _, owner := range p.OwnerReferences {
			if owner.Name == w.JobName && owner.UID != w.JobUID || owner.Name == worker.Name && owner.UID != w.WorkerJobUID {
				return nil, ErrNativeCleanedCompletionUnavailable
			}
			switch owner.UID {
			case w.JobUID:
				if helperPod != nil {
					return nil, ErrNativeCleanedCompletionUnavailable
				}
				helperPod = p
			case w.WorkerJobUID:
				if workerPod != nil {
					return nil, ErrNativeCleanedCompletionUnavailable
				}
				workerPod = p
			}
		}
	}
	if helperPod == nil || workerPod == nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	for _, selected := range []*corev1.Pod{helperPod, workerPod} {
		observed := &corev1.Pod{}
		if r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: selected.Name}, observed) != nil || observed.UID != selected.UID {
			return nil, ErrNativeCleanedCompletionUnavailable
		}
		*selected = *observed
	}
	return validateCleanedObjects(w, m, pvc, job, worker, helperPod, workerPod, savedE, completed)
}

func validateCleanedObjects(w ExecutionData, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, job, worker *batchv1.Job, pod, workerPod *corev1.Pod, savedE string, completed bool) (*ownedAdmission, error) {
	if !validCleanedExecution(w) || m == nil || pvc == nil || job == nil || worker == nil || pod == nil || workerPod == nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	if !cleanedMigrationMatches(w, m, worker, savedE) || !retryPVCMatches(w, m, pvc) || !retryHelperJobMatches(w, m, job) {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	if len(pvc.OwnerReferences) != 1 || len(job.OwnerReferences) != 1 || !successfulJob(worker) || !ownedTerminalWorker(w, m, worker, workerPod) || !ownedHelperPodIdentity(w, job, pod) || !ownedHelperState(w, job, pod, completed) {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	template := podexec.NativeTemplateSHA256(job.Spec.Template)
	references, err := CleanedCompletionConnectionReferenceSHA256(m, w)
	if err != nil || template != w.TemplateSHA256 || references != w.ConnectionReferenceSHA256 || !cleanedHelperSpecs(m, w, job, pod) {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	s := pod.Status.ContainerStatuses[0]
	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: s.Name, ContainerID: s.ContainerID, RestartCount: s.RestartCount, ImageID: s.ImageID, TemplateSHA256: template, ExecutionSHA256: podexec.NativeExecutionSHA256(job.Spec.Template)}
	if w.PodName != "" && w.PodName != pod.Name {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	if w.Container != (podexec.NativeContainerBinding{}) {
		expected := w.Container
		if expected.ExecutionSHA256 == ([32]byte{}) {
			expected.ExecutionSHA256 = binding.ExecutionSHA256
		}
		if expected != binding {
			return nil, ErrNativeCleanedCompletionUnavailable
		}
	}
	return &ownedAdmission{helper: &activeAdmission{migration: m.DeepCopy(), pvc: pvc.DeepCopy(), job: job.DeepCopy(), worker: worker.DeepCopy(), pod: pod.DeepCopy(), container: binding}, workerPod: workerPod.DeepCopy()}, nil
}

func cleanedMigrationMatches(w ExecutionData, m *v1beta1.Migration, worker *batchv1.Job, savedE string) bool {
	return NativeReceiptsMatch(m, w.WorkerJobUID, w.PVCUID) && ownedMigrationMatches(w, m, worker) && m.DeletionTimestamp.IsZero() && !m.Spec.Suspend && m.Status.Replication != nil && m.Status.Replication.Endpos == savedE
}

func cleanedHelperSpecs(m *v1beta1.Migration, w ExecutionData, job *batchv1.Job, pod *corev1.Pod) bool {
	command, args, env, volumes, mounts, err := CleanedCompletionMaterialization(m, w)
	if err != nil {
		return false
	}
	for _, spec := range []*corev1.PodSpec{&job.Spec.Template.Spec, &pod.Spec} {
		if !admittedPodSpec(spec, w.ImageRef) || !reflect.DeepEqual(spec.Volumes, volumes) {
			return false
		}
		c := spec.Containers[0]
		if !slices.Equal(c.Command, command) || !slices.Equal(c.Args, args) || !sameEnvironment(c.Env, env) || !reflect.DeepEqual(c.VolumeMounts, mounts) {
			return false
		}
	}
	return true
}
