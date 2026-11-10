package controller

import (
	"context"
	"errors"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// NativeStopRequester supplies request data only, never terminal execution authority.
type NativeStopRequester interface {
	RequestNativeTerm(context.Context, string, string, podexec.NativeContainerBinding) error
}

var errNativeRetainedStop = errors.New("native retained worker stop is incomplete")

type nativeStopFacts struct {
	worker  *batchv1.Job
	pod     *corev1.Pod
	binding podexec.NativeContainerBinding
}

func (r *MigrationReconciler) nativeStopCurrent(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *nativeStopFacts) (nativeStopFacts, error) {
	if r.APIReader == nil || r.Scheme == nil || m == nil || worker == nil || pvc == nil || ctx.Err() != nil ||
		!native.NativeReceiptsMatch(m, worker.UID, pvc.UID) {
		return nativeStopFacts{}, errNativeRetainedStop
	}
	// Recovery admission preserves the caller's exact suspend/deletion intent and persisted E.
	live, claim, err := r.nativeRecoveryMigration(ctx, m, pvc)
	if err != nil || !native.NativeReceiptsMatch(live, worker.UID, claim.UID) || worker.Name != live.Status.JobName || worker.Namespace != live.Namespace {
		return nativeStopFacts{}, errNativeRetainedStop
	}
	expected, err := buildNativeWorkerJob(live, claim, r.RPGMigrateRunnerImage, live.Status.Attempts)
	if err != nil || controllerutil.SetControllerReference(live, expected, r.Scheme) != nil {
		return nativeStopFacts{}, errNativeRetainedStop
	}
	current := &batchv1.Job{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(worker), current) != nil || ctx.Err() != nil || current.UID != worker.UID || current.Generation != worker.Generation ||
		!native.NativeReceiptsMatch(live, current.UID, claim.UID) || !nativePreflightJobMatches(expected, current) || !nativePreflightJobMatches(worker, current) {
		return nativeStopFacts{}, errNativeRetainedStop
	}
	pod, err := r.nativePreflightPod(ctx, current)
	image := runnerImageFor(live, r.RPGMigrateRunnerImage)
	if err != nil || pod == nil || ctx.Err() != nil || !nativePreflightPodMatches(current, pod) ||
		!nativeControlPodRunning(pod, image) && !nativeRecoveryPodTerminal(pod, image) {
		return nativeStopFacts{}, errNativeRetainedStop
	}
	s := pod.Status.ContainerStatuses[0]
	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: workerContainer, ContainerID: s.ContainerID, ImageID: image,
		TemplateSHA256: podexec.NativeTemplateSHA256(current.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(current.Spec.Template)}
	if binding.TemplateSHA256 == ([32]byte{}) || binding.ExecutionSHA256 == ([32]byte{}) || !nativeStopOriginalMatches(original, pod, binding) {
		return nativeStopFacts{}, errNativeRetainedStop
	}
	return nativeStopFacts{worker: current, pod: pod, binding: binding}, nil
}

func nativeStopOriginalMatches(original *nativeStopFacts, pod *corev1.Pod, binding podexec.NativeContainerBinding) bool {
	return original == nil || binding == original.binding && pod.Name == original.pod.Name && pod.Generation == original.pod.Generation &&
		apiequality.Semantic.DeepEqual(pod.Spec, original.pod.Spec) && pod.Status.ContainerStatuses[0].Image == original.pod.Status.ContainerStatuses[0].Image
}

func nativeStopTerminal(facts nativeStopFacts) bool {
	job, pod := facts.worker, facts.pod
	if job.Status.Terminating != nil && *job.Status.Terminating != 0 || !nativeRecoveryPodTerminal(pod, facts.binding.ImageID) {
		return false
	}
	s := pod.Status.ContainerStatuses[0].State.Terminated
	return nativeEvidenceWorkerComplete(job) && pod.Status.Phase == corev1.PodSucceeded && s.ExitCode == 0 && s.Signal == 0 ||
		nativeRecoveryWorkerFailed(job) && pod.Status.Phase == corev1.PodFailed
}
