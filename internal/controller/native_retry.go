package controller

import (
	"context"
	"errors"
	"slices"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var (
	errNativeRecovery       = errors.New("native recovery identity or execution unavailable")
	errNativeRecoveryActive = errors.New("native original worker remains active")
)

func (r *MigrationReconciler) nativeRecoveryMigration(ctx context.Context, expected *v1beta1.Migration, expectedPVC *corev1.PersistentVolumeClaim) (*v1beta1.Migration, *corev1.PersistentVolumeClaim, error) {
	if r.APIReader == nil || expected == nil || expectedPVC == nil || ctx.Err() != nil {
		return nil, nil, errNativeRecovery
	}
	live, pvc := &v1beta1.Migration{}, &corev1.PersistentVolumeClaim{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), live) != nil ||
		r.APIReader.Get(ctx, client.ObjectKeyFromObject(expectedPVC), pvc) != nil || ctx.Err() != nil ||
		live.UID == "" || live.UID != expected.UID || live.Name != expected.Name || live.Namespace != expected.Namespace || live.ResourceVersion == "" ||
		live.Generation < 1 || live.Generation != expected.Generation || live.Spec.Engine != v1beta1.MigrationEngineRPGMigrate ||
		!apiequality.Semantic.DeepEqual(live.Spec, expected.Spec) || !apiequality.Semantic.DeepEqual(live.Status, expected.Status) ||
		!apiequality.Semantic.DeepEqual(live.Labels, expected.Labels) || !apiequality.Semantic.DeepEqual(live.Annotations, expected.Annotations) ||
		!apiequality.Semantic.DeepEqual(live.DeletionTimestamp, expected.DeletionTimestamp) ||
		live.Status.Attempts < 1 || live.Status.JobName != jobName(live, live.Status.Attempts) ||
		!nativeRecoveryPVCMatches(expectedPVC, pvc) ||
		!native.NativeReceiptsMatch(live, types.UID(expected.Status.NativeJobUID), pvc.UID) {
		return nil, nil, errNativeRecovery
	}
	if rep := live.Status.Replication; rep != nil && rep.Endpos != "" {
		if e, err := sentinel.ParseLSN(rep.Endpos); err != nil || e == 0 {
			return nil, nil, errNativeRecovery
		}
	}
	if _, err := nativeWorkIdentity(live, pvc); err != nil {
		return nil, nil, errNativeRecovery
	}
	return live, pvc, nil
}

func nativeRecoveryPVCMatches(expected, pvc *corev1.PersistentVolumeClaim) bool {
	return pvc.UID != "" && pvc.UID == expected.UID && pvc.Name == expected.Name && pvc.Namespace == expected.Namespace &&
		pvc.Status.Phase == corev1.ClaimBound && pvc.DeletionTimestamp.IsZero() &&
		apiequality.Semantic.DeepEqual(pvc.Spec, expected.Spec) && apiequality.Semantic.DeepEqual(pvc.OwnerReferences, expected.OwnerReferences) &&
		apiequality.Semantic.DeepEqual(pvc.Labels, expected.Labels) && apiequality.Semantic.DeepEqual(pvc.Annotations, expected.Annotations)
}

func (r *MigrationReconciler) nativeRecoveryCurrent(ctx context.Context, expected *v1beta1.Migration, expectedWorker *batchv1.Job, expectedPVC *corev1.PersistentVolumeClaim) (*v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim, error) {
	if expectedWorker == nil || expectedWorker.UID == "" || expected == nil || expected.Status.NativeJobUID != string(expectedWorker.UID) || r.Scheme == nil {
		return nil, nil, nil, errNativeRecovery
	}
	live, pvc, err := r.nativeRecoveryMigration(ctx, expected, expectedPVC)
	if err != nil || expectedWorker.Name != live.Status.JobName || expectedWorker.Namespace != live.Namespace {
		return nil, nil, nil, errNativeRecovery
	}
	canonical, err := buildNativeWorkerJob(live, pvc, r.RPGMigrateRunnerImage, live.Status.Attempts)
	if err != nil || controllerutil.SetControllerReference(live, canonical, r.Scheme) != nil {
		return nil, nil, nil, errNativeRecovery
	}
	worker := &batchv1.Job{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(expectedWorker), worker) != nil || ctx.Err() != nil || worker.UID != expectedWorker.UID ||
		!nativePreflightJobMatches(canonical, worker) || !nativePreflightJobMatches(expectedWorker, worker) ||
		!apiequality.Semantic.DeepEqual(worker.Status, expectedWorker.Status) {
		return nil, nil, nil, errNativeRecovery
	}
	return live, worker, pvc, nil
}

// Quiescence supplies no target retry census or checked commit-probe result.
func (r *MigrationReconciler) nativeWorkerQuiescent(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	_, err := r.nativeQuiescentWorker(ctx, m, worker, pvc)
	if errors.Is(err, errNativeRecoveryActive) {
		return false, nil
	}
	return err == nil, err
}

func (r *MigrationReconciler) nativeQuiescentWorker(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	live, current, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil {
		return nil, errNativeRecovery
	}
	if current.Status.Terminating != nil && *current.Status.Terminating != 0 {
		return nil, errNativeRecoveryActive
	}
	if !nativeEvidenceWorkerComplete(current) && !nativeRecoveryWorkerFailed(current) {
		return nil, errNativeRecoveryActive
	}
	pod, err := r.nativePreflightPod(ctx, current)
	if err != nil || pod == nil || !nativePreflightPodMatches(current, pod) ||
		!nativeRecoveryPodTerminal(pod, runnerImageFor(live, r.RPGMigrateRunnerImage)) ||
		nativeEvidenceWorkerComplete(current) && (pod.Status.Phase != corev1.PodSucceeded || pod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 || pod.Status.ContainerStatuses[0].State.Terminated.Signal != 0) ||
		nativeRecoveryWorkerFailed(current) && pod.Status.Phase != corev1.PodFailed {
		return nil, errNativeRecovery
	}
	if _, _, _, err := r.nativeRecoveryCurrent(ctx, live, current, claim); err != nil {
		return nil, errNativeRecovery
	}
	confirmed, err := r.nativePreflightPod(ctx, current)
	if err != nil || confirmed == nil || ctx.Err() != nil || !nativePreflightSamePod(pod, confirmed) ||
		!apiequality.Semantic.DeepEqual(pod.Spec, confirmed.Spec) || !apiequality.Semantic.DeepEqual(pod.Status, confirmed.Status) {
		return nil, errNativeRecovery
	}
	return confirmed, nil
}

func nativeRecoveryWorkerFailed(worker *batchv1.Job) bool {
	if worker.Status.Succeeded != 0 || worker.Status.Failed != 1 || worker.Status.Active != 0 || worker.Status.Terminating != nil && *worker.Status.Terminating != 0 {
		return false
	}
	failed := false
	for _, condition := range worker.Status.Conditions {
		if condition.Status == corev1.ConditionTrue {
			if condition.Type == batchv1.JobComplete {
				return false
			}
			failed = failed || condition.Type == batchv1.JobFailed
		}
	}
	return failed
}

func nativeRecoveryPodTerminal(pod *corev1.Pod, image string) bool {
	if len(pod.Status.ContainerStatuses) != 1 || len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 ||
		pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed || pod.Spec.NodeName == "" {
		return false
	}
	s := pod.Status.ContainerStatuses[0]
	return s.Name == workerContainer && s.ContainerID != "" && s.ImageID == image && s.RestartCount == 0 &&
		s.LastTerminationState == (corev1.ContainerState{}) && s.State.Running == nil && s.State.Waiting == nil && s.State.Terminated != nil && s.State.Terminated.ContainerID == s.ContainerID
}

// Original UIDs are retained only for this invocation; later missing names grant no permission.
func (r *MigrationReconciler) stopNativeWorker(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if r.Client == nil {
		return false, errNativeRecovery
	}
	live, original, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil || original.Status.Succeeded != 0 || slices.ContainsFunc(original.Status.Conditions, func(c batchv1.JobCondition) bool {
		return c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue
	}) {
		return false, errNativeRecovery
	}
	pod, err := r.nativePreflightPod(ctx, original)
	if err != nil || pod == nil || !nativePreflightPodMatches(original, pod) || len(pod.Status.ContainerStatuses) != 1 {
		return false, errNativeRecovery
	}
	s := pod.Status.ContainerStatuses[0]
	if s.Name != workerContainer || s.ContainerID == "" || s.ImageID != runnerImageFor(live, r.RPGMigrateRunnerImage) || s.RestartCount != 0 || s.LastTerminationState != (corev1.ContainerState{}) {
		return false, errNativeRecovery
	}
	if _, _, _, err := r.nativeRecoveryCurrent(ctx, live, original, claim); err != nil || ctx.Err() != nil {
		return false, errNativeRecovery
	}
	if r.nativePreflightCleanup(ctx, live, original, pod) != nil {
		return false, errNativeRecovery
	}
	if _, _, err := r.nativeRecoveryMigration(ctx, live, claim); err != nil || ctx.Err() != nil {
		return false, errNativeRecovery
	}
	return true, nil
}
