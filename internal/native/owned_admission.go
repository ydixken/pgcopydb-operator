package native

import (
	"context"
	"reflect"
	"slices"
	"strconv"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const nativeOwnedEnginePath = "/usr/local/bin/rpgmigrate"

type ownedAdmission struct {
	helper    *activeAdmission
	workerPod *corev1.Pod
}

func (r *Reader) admitOwned(ctx context.Context, w ExecutionData, completed bool) (*ownedAdmission, error) {
	if !validOwnedExecution(w) || ctx.Err() != nil {
		return nil, ErrNativeOwnedStateUnavailable
	}
	m, pvc, job, worker := &v1beta1.Migration{}, &corev1.PersistentVolumeClaim{}, &batchv1.Job{}, &batchv1.Job{}
	for _, read := range []struct {
		name   string
		object client.Object
	}{{w.MigrationName, m}, {w.PVCName, pvc}, {w.JobName, job}} {
		if r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: read.name}, read.object) != nil {
			return nil, ErrNativeOwnedStateUnavailable
		}
	}
	if m.Status.JobName == "" || r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: m.Status.JobName}, worker) != nil {
		return nil, ErrNativeOwnedStateUnavailable
	}
	pods := &corev1.PodList{}
	if r.api.List(ctx, pods, client.InNamespace(w.Namespace)) != nil || pods.Continue != "" || pods.ResourceVersion == "" || pods.ShardInfo != nil || pods.RemainingItemCount != nil && *pods.RemainingItemCount != 0 {
		return nil, ErrNativeOwnedStateUnavailable
	}
	var helperPod, workerPod *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		for _, owner := range p.OwnerReferences {
			if owner.Name == w.JobName && owner.UID != w.JobUID || owner.Name == worker.Name && owner.UID != w.WorkerJobUID {
				return nil, ErrNativeOwnedStateUnavailable
			}
			switch owner.UID {
			case w.JobUID:
				if helperPod != nil {
					return nil, ErrNativeOwnedStateUnavailable
				}
				helperPod = p
			case w.WorkerJobUID:
				if workerPod != nil {
					return nil, ErrNativeOwnedStateUnavailable
				}
				workerPod = p
			}
		}
	}
	if helperPod == nil || workerPod == nil {
		return nil, ErrNativeOwnedStateUnavailable
	}
	for _, selected := range []*corev1.Pod{helperPod, workerPod} {
		observed := &corev1.Pod{}
		if r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: selected.Name}, observed) != nil || observed.UID != selected.UID {
			return nil, ErrNativeOwnedStateUnavailable
		}
		*selected = *observed
	}
	return validateOwnedObjects(w, m, pvc, job, worker, helperPod, workerPod, completed)
}

func validateOwnedObjects(w ExecutionData, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, job, worker *batchv1.Job, pod, workerPod *corev1.Pod, completed bool) (*ownedAdmission, error) {
	if !validOwnedExecution(w) || m == nil || pvc == nil || job == nil || worker == nil || pod == nil || workerPod == nil {
		return nil, ErrNativeOwnedStateUnavailable
	}
	if !ownedMigrationMatches(w, m, worker) || !retryPVCMatches(w, m, pvc) || !retryHelperJobMatches(w, m, job) {
		return nil, ErrNativeOwnedStateUnavailable
	}
	if len(pvc.OwnerReferences) != 1 || len(job.OwnerReferences) != 1 || !ownedTerminalWorker(w, m, worker, workerPod) || !ownedHelperPodIdentity(w, job, pod) {
		return nil, ErrNativeOwnedStateUnavailable
	}
	template := podexec.NativeTemplateSHA256(job.Spec.Template)
	references, err := OwnedInventoryConnectionReferenceSHA256(m, w)
	if err != nil || template != w.TemplateSHA256 || references != w.ConnectionReferenceSHA256 || !ownedHelperSpecs(m, w, job, pod) || !ownedHelperState(w, job, pod, completed) {
		return nil, ErrNativeOwnedStateUnavailable
	}
	s := pod.Status.ContainerStatuses[0]

	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: s.Name, ContainerID: s.ContainerID, RestartCount: s.RestartCount, ImageID: s.ImageID, TemplateSHA256: template, ExecutionSHA256: podexec.NativeExecutionSHA256(job.Spec.Template)}
	if w.PodName != "" && w.PodName != pod.Name {
		return nil, ErrNativeOwnedStateUnavailable
	}
	if w.Container != (podexec.NativeContainerBinding{}) {
		expected := w.Container
		if expected.ExecutionSHA256 == ([32]byte{}) {
			expected.ExecutionSHA256 = binding.ExecutionSHA256
		}
		if expected != binding {
			return nil, ErrNativeOwnedStateUnavailable
		}
	}
	return &ownedAdmission{helper: &activeAdmission{migration: m.DeepCopy(), pvc: pvc.DeepCopy(), job: job.DeepCopy(), worker: worker.DeepCopy(), pod: pod.DeepCopy(), container: binding}, workerPod: workerPod.DeepCopy()}, nil
}

func ownedMigrationMatches(w ExecutionData, m *v1beta1.Migration, worker *batchv1.Job) bool {
	return m.Namespace == w.Namespace && m.Name == w.MigrationName && m.UID == w.MigrationUID && m.Generation == w.MigrationGeneration && m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && m.Spec.Follow != nil && m.Spec.Follow.Enabled && !m.Spec.Clone.AllDatabases && m.Status.Attempts == w.Attempt && m.Status.JobName == worker.Name
}

func ownedHelperPodIdentity(w ExecutionData, job *batchv1.Job, pod *corev1.Pod) bool {
	return len(pod.OwnerReferences) == 1 && pod.Namespace == w.Namespace && pod.Name != "" && pod.UID != "" && pod.DeletionTimestamp.IsZero() && controlledBy(pod, "Job", job.Name, job.UID, "batch/v1") && pod.Spec.DNSPolicy == corev1.DNSClusterFirst
}

func ownedHelperState(w ExecutionData, job *batchv1.Job, pod *corev1.Pod, completed bool) bool {
	if !retryHelperState(w, job, pod, completed) || job.Status.Active < 0 {
		return false
	}
	if completed {
		if job.Status.Ready != nil && *job.Status.Ready != 0 || pod.Status.ContainerStatuses[0].Ready {
			return false
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Status == corev1.ConditionTrue && (condition.Type == corev1.PodReady || condition.Type == corev1.ContainersReady) {
				return false
			}
		}
	}
	return true
}

func ownedTerminalWorker(w ExecutionData, m *v1beta1.Migration, job *batchv1.Job, pod *corev1.Pod) bool {
	return ownedTerminalWorkerJob(w, m, job) && ownedTerminalWorkerPod(w, m, job, pod)
}

func ownedTerminalWorkerJob(w ExecutionData, m *v1beta1.Migration, job *batchv1.Job) bool {
	if len(job.OwnerReferences) != 1 || job.Namespace != w.Namespace || job.Name != m.Name+"-run-"+strconv.FormatInt(int64(w.Attempt), 10) || job.UID != w.WorkerJobUID || job.UID == w.JobUID || !job.DeletionTimestamp.IsZero() || !controlledBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) || !retrySingleJob(job) || job.Status.Active != 0 || job.Status.Ready != nil && *job.Status.Ready != 0 || !admittedPodSpec(&job.Spec.Template.Spec, w.ImageRef) {
		return false
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Name != nativeContainerName || len(c.Command) != 4 || c.Command[0] != nativeShellPath || c.Command[1] != "-c" || c.Command[3] != nativeOwnedEnginePath || len(c.Args) == 0 || c.Args[0] != nativeCloneCommand || !slices.Contains(c.Args, "--follow") || !identityEnvironment(c.Env, w.Work) {
		return false
	}
	return true
}

func ownedTerminalWorkerPod(w ExecutionData, m *v1beta1.Migration, job *batchv1.Job, pod *corev1.Pod) bool {
	if len(pod.OwnerReferences) != 1 || pod.Namespace != w.Namespace || pod.Name == "" || pod.UID == "" || !pod.DeletionTimestamp.IsZero() || !controlledBy(pod, "Job", job.Name, job.UID, "batch/v1") || !admittedPodSpec(&pod.Spec, w.ImageRef) || !samePodExecution(&job.Spec.Template.Spec, &pod.Spec) || len(pod.Status.ContainerStatuses) != 1 || len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && (condition.Type == corev1.PodReady || condition.Type == corev1.ContainersReady) {
			return false
		}
	}
	s := pod.Status.ContainerStatuses[0]
	if !ownedTerminalContainer(w, s) {
		return false
	}
	if successfulJob(job) {
		return pod.Status.Phase == corev1.PodSucceeded && s.State.Terminated.ExitCode == 0 && s.State.Terminated.Signal == 0
	}
	return retryFailedWorkerJob(w, m, job) && pod.Status.Phase == corev1.PodFailed && (s.State.Terminated.ExitCode != 0 || s.State.Terminated.Signal != 0)
}

func ownedTerminalContainer(w ExecutionData, s corev1.ContainerStatus) bool {
	if s.Name != nativeContainerName || s.ContainerID == "" || s.Image == "" || s.ImageID != w.PlatformImageID || s.RestartCount != 0 || s.Ready || s.LastTerminationState != (corev1.ContainerState{}) || s.State.Running != nil || s.State.Waiting != nil || s.State.Terminated == nil || s.State.Terminated.ContainerID != s.ContainerID {
		return false
	}
	return true
}

func ownedHelperSpecs(m *v1beta1.Migration, w ExecutionData, job *batchv1.Job, pod *corev1.Pod) bool {
	command, args, env, volumes, mounts, err := OwnedInventoryMaterialization(m, w)
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

func (a *ownedAdmission) same(b *ownedAdmission) bool {
	if a == nil || b == nil || !a.helper.sameExecution(b.helper) {
		return false
	}
	for _, pair := range [][2]metav1.Object{{a.helper.migration, b.helper.migration}, {a.helper.pvc, b.helper.pvc}, {a.helper.job, b.helper.job}, {a.helper.worker, b.helper.worker}, {a.helper.pod, b.helper.pod}, {a.workerPod, b.workerPod}} {
		x, y := pair[0], pair[1]
		if x.GetNamespace() != y.GetNamespace() || x.GetName() != y.GetName() || x.GetUID() != y.GetUID() || x.GetGeneration() != y.GetGeneration() || !reflect.DeepEqual(x.GetDeletionTimestamp(), y.GetDeletionTimestamp()) || !reflect.DeepEqual(x.GetDeletionGracePeriodSeconds(), y.GetDeletionGracePeriodSeconds()) || !slices.Equal(x.GetFinalizers(), y.GetFinalizers()) || !reflect.DeepEqual(x.GetOwnerReferences(), y.GetOwnerReferences()) || !reflect.DeepEqual(x.GetLabels(), y.GetLabels()) || !reflect.DeepEqual(x.GetAnnotations(), y.GetAnnotations()) {
			return false
		}
	}
	return reflect.DeepEqual(a.helper.migration.Status, b.helper.migration.Status) && reflect.DeepEqual(a.helper.pvc.Status, b.helper.pvc.Status) && reflect.DeepEqual(a.helper.worker.Status, b.helper.worker.Status) && reflect.DeepEqual(a.workerPod.Spec, b.workerPod.Spec) && reflect.DeepEqual(a.workerPod.Status, b.workerPod.Status)
}
