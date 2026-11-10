package controller

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var errNativeDisposition = errors.New("native disposition authority unavailable")

func (r *MigrationReconciler) reconcileNativeDisposition(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, ctrl.Result, error) {
	if m == nil || base == nil || m.Spec.Engine != v1beta1.MigrationEngineRPGMigrate {
		return false, ctrl.Result{}, nil
	}
	deleting := !m.DeletionTimestamp.IsZero()
	if !deleting && !m.Spec.Suspend && (worker == nil || !nativeRecoveryWorkerFailed(worker)) {
		return false, ctrl.Result{}, nil
	}
	r.stopSampling(client.ObjectKeyFromObject(m))
	if r.APIReader == nil || r.Client == nil || ctx.Err() != nil || !nativeDispositionSnapshotMatches(m, base) {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if deleting && m.Status.NativeFinalizedJobUID != "" && m.Status.NativeFinalizedJobUID != m.Status.NativeJobUID {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if deleting && nativeFinalizationRecorded(m) {
		result, err := r.reconcileNativeFinalizedDeletion(ctx, m)
		return true, result, err
	}
	if worker == nil {
		if err := r.nativeDispositionUnstarted(ctx, m); err != nil {
			return true, ctrl.Result{}, err
		}
		if deleting {
			return r.finishNativeDeletion(ctx, m)
		}
		return r.recordNativeSuspended(ctx, m, base)
	}
	if pvc == nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	live, current, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if deleting || live.Spec.Suspend {
		return r.reconcileNativeRetainedDisposition(ctx, live, current, claim)
	}
	if terminal, err := r.reconcileNativeFailure(ctx, live, current, claim); terminal || err != nil {
		return true, ctrl.Result{}, err
	}
	result, err := r.reconcileNativeRetry(ctx, live, live.DeepCopy(), current, claim)
	return true, result, err
}

func nativeDispositionSnapshotMatches(m, base *v1beta1.Migration) bool {
	return m.UID != "" && m.UID == base.UID && m.Generation == base.Generation && m.ResourceVersion == base.ResourceVersion &&
		apiequality.Semantic.DeepEqual(m.Spec, base.Spec) && apiequality.Semantic.DeepEqual(m.Status, base.Status)
}

type nativeFailureReader interface {
	JobFailureNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
}

func (r *MigrationReconciler) reconcileNativeFailure(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	live, original, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, nil)
	if err != nil || !nativeRecoveryWorkerFailed(worker) || live.Status.NativeFinalizedJobUID != "" {
		return false, errNativeDisposition
	}
	if condition := meta.FindStatusCondition(live.Status.Conditions, v1beta1.ConditionFailed); condition != nil && condition.Status == metav1.ConditionTrue && live.Status.Phase == v1beta1.PhaseFailed && live.Status.ObservedGeneration == live.Generation {
		switch condition.Reason {
		case nativeFailureBadArgs, nativeFailureBadConfig, nativeFailureBadState:
			return true, nil
		}
	}
	reader, available := r.NativeJobs.(nativeFailureReader)
	if !available {
		return false, nil
	}
	status := original.Status.ContainerStatuses[0]
	binding := podexec.NativeContainerBinding{PodUID: original.UID, ContainerName: workerContainer, ContainerID: status.ContainerID, ImageID: runnerImageFor(live, r.RPGMigrateRunnerImage), TemplateSHA256: podexec.NativeTemplateSHA256(worker.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(worker.Spec.Template)}
	output, captureErr := reader.JobFailureNative(ctx, live.Namespace, worker.UID, binding)
	confirmed, _, err := r.nativeRecoveryQuiescentCurrent(ctx, live, worker, pvc, original)
	if err != nil {
		return false, errNativeDisposition
	}
	if captureErr != nil || !output.Successful() {
		return false, nil
	}
	reason, terminal := nativeWorkerTerminalFailure(output.Payload())
	if !terminal {
		return false, nil
	}
	desired := confirmed.DeepCopy()
	desired.Status.Phase, desired.Status.ObservedGeneration = v1beta1.PhaseFailed, desired.Generation
	r.setCondition(desired, v1beta1.ConditionFailed, metav1.ConditionTrue, reason, "Native worker reported a terminal failure; retained state is preserved")
	r.setCondition(desired, v1beta1.ConditionComplete, metav1.ConditionFalse, reason, "Native worker did not complete")
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(confirmed, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	metrics.Record(desired)
	return true, nil
}

func (r *MigrationReconciler) reconcileNativeRetainedDisposition(ctx context.Context, live *v1beta1.Migration, worker *batchv1.Job, claim *corev1.PersistentVolumeClaim) (bool, ctrl.Result, error) {
	deleting := !live.DeletionTimestamp.IsZero()
	current, original, err := r.nativeRetainedDispositionTerminal(ctx, live, worker, claim)
	if err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if !deleting {
		if _, err := r.admitNativeCompareHelpers(ctx, live, current, claim, original); err != nil {
			return true, ctrl.Result{}, errNativeDisposition
		}
		return r.recordNativeSuspended(ctx, live, live.DeepCopy())
	}
	if _, err := r.admitNativeCompareHelpers(ctx, live, current, claim, original); err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if followEnabled(live) {
		if err := r.cleanupNativeDeletion(ctx, live, current, claim, original); err != nil {
			return true, ctrl.Result{}, err
		}
	}
	if _, err := r.admitNativeCompareHelpers(ctx, live, current, claim, original); err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, live, current, claim, original); err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	return r.finishNativeDeletion(ctx, live)

}

type nativeRetainedCompare struct {
	expected, job *batchv1.Job
	pod           *corev1.Pod
}

type nativeClosedCompareHelpers struct {
	migrationUID, workerUID, pvcUID types.UID
	generation                      int64
	pins                            [2]string
	removed                         []nativeRetainedCompare
}

// The old operation is used only to reconstruct a pinned immutable helper for removal.
func (r *MigrationReconciler) nativeRemovalCompareJob(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, job *batchv1.Job, check string) (*batchv1.Job, error) {
	if len(job.Spec.Template.Spec.Containers) != 1 || len(job.Spec.Template.Spec.Containers[0].Args) != 14 {
		return nil, errNativeDisposition
	}
	op := job.Spec.Template.Spec.Containers[0].Args[10]
	command, args, env, volumes, mounts, err := native.ComparisonMaterialization(m, op, check)
	if err != nil {
		return nil, errNativeDisposition
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		return nil, errNativeDisposition
	}
	expected, err := buildNativeVersionJob(m, r.RPGMigrateRunnerImage, "rpgmigrate-version-"+op)
	if err != nil {
		return nil, errNativeDisposition
	}
	expected.Name, expected.Spec.ActiveDeadlineSeconds = compareJobName(m, check), nil
	expected.Spec.Template.Spec.Volumes = append([]corev1.Volume{{Name: workdirVolume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}}, volumes...)
	c := &expected.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, append(env, nativeIdentityEnv(work)...), append([]corev1.VolumeMount{{Name: workdirVolume, MountPath: pgcopydb.WorkMount}}, mounts...)
	if controllerutil.SetControllerReference(m, expected, r.Scheme) != nil || native.ValidateEvidenceJobSize(expected) != nil {
		return nil, errNativeDisposition
	}
	return expected, nil
}

func (r *MigrationReconciler) admitNativeCompareHelpers(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) ([]nativeRetainedCompare, error) {
	var helpers []nativeRetainedCompare
	for _, check := range []string{compareSchema, compareData} {
		if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
			return nil, errNativeDisposition
		}
		job := &batchv1.Job{}
		err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: compareJobName(m, check)}, job)
		pin := nativeVerificationJobUID(m, check)
		if pin == "" {
			if !apierrors.IsNotFound(err) {
				return nil, errNativeDisposition
			}
			continue
		}
		if err != nil || job.UID != types.UID(pin) || job.ResourceVersion == "" || job.UID == worker.UID {
			return nil, errNativeDisposition
		}
		expected, err := r.nativeRemovalCompareJob(m, pvc, job, check)
		if err != nil || !nativePreflightJobMatches(expected, job) {
			return nil, errNativeDisposition
		}
		pod, err := r.nativePreflightPod(ctx, job)
		if err != nil || pod == nil || !nativePreflightPodMatches(job, pod) || !nativeStopTerminal(nativeStopFacts{worker: job, pod: pod, binding: podexec.NativeContainerBinding{ImageID: runnerImageFor(m, r.RPGMigrateRunnerImage)}}) {
			return nil, errNativeDisposition
		}
		helpers = append(helpers, nativeRetainedCompare{expected: expected, job: job, pod: pod})
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
		return nil, errNativeDisposition
	}
	return helpers, nil
}

func (r *MigrationReconciler) closeNativeCompareHelpers(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (*nativeClosedCompareHelpers, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	helpers, err := r.admitNativeCompareHelpers(ctx, m, worker, pvc, original)
	if err != nil {
		return nil, errNativeDisposition
	}
	for _, helper := range helpers {
		if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
			return nil, errNativeDisposition
		}
		job := &batchv1.Job{}
		if r.APIReader.Get(ctx, client.ObjectKeyFromObject(helper.job), job) != nil || job.UID != helper.job.UID || job.ResourceVersion != helper.job.ResourceVersion || !nativePreflightJobMatches(helper.expected, job) || !apiequality.Semantic.DeepEqual(job.Status, helper.job.Status) {
			return nil, errNativeDisposition
		}
		pod, err := r.nativePreflightPod(ctx, job)
		if err != nil || pod == nil || pod.Generation != helper.pod.Generation || !nativePreflightPodMatches(job, pod) || !nativePreflightSamePod(helper.pod, pod) || !apiequality.Semantic.DeepEqual(pod.Spec, helper.pod.Spec) || !apiequality.Semantic.DeepEqual(pod.Status, helper.pod.Status) {
			return nil, errNativeDisposition
		}
		if r.Delete(ctx, job, client.Preconditions{UID: &job.UID, ResourceVersion: &job.ResourceVersion}, client.PropagationPolicy(metav1.DeletePropagationForeground)) != nil {
			return nil, errNativeDisposition
		}
		if r.nativePreflightCleanup(ctx, m, helper.job, helper.pod) != nil {
			return nil, errNativeDisposition
		}
	}
	closed := &nativeClosedCompareHelpers{migrationUID: m.UID, generation: m.Generation, workerUID: worker.UID, pvcUID: pvc.UID, pins: [2]string{m.Status.NativeSchemaJobUID, m.Status.NativeDataJobUID}, removed: helpers}
	if r.nativeCompareClosureCurrent(ctx, m, closed) != nil {
		return nil, errNativeDisposition
	}
	return closed, nil
}

func (r *MigrationReconciler) nativeCompareClosureCurrent(ctx context.Context, m *v1beta1.Migration, closed *nativeClosedCompareHelpers) error {
	if closed == nil || m == nil || closed.migrationUID != m.UID || closed.generation != m.Generation || string(closed.workerUID) != m.Status.NativeJobUID || string(closed.pvcUID) != m.Status.NativePVCUID || closed.pins != [2]string{m.Status.NativeSchemaJobUID, m.Status.NativeDataJobUID} {
		return errNativeDisposition
	}
	remaining := len(closed.removed)
	for i, check := range []string{compareSchema, compareData} {
		if closed.pins[i] != "" {
			matched := 0
			for _, helper := range closed.removed {
				if helper.job != nil && helper.pod != nil && helper.expected != nil && string(helper.job.UID) == closed.pins[i] && helper.job.Name == compareJobName(m, check) && helper.pod.UID != "" && nativePreflightJobMatches(helper.expected, helper.job) {
					matched++
				}
			}
			if matched != 1 {
				return errNativeDisposition
			}
			remaining--
		}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: compareJobName(m, check)}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
			return errNativeDisposition
		}
	}
	if remaining != 0 {
		return errNativeDisposition
	}
	if len(closed.removed) > 0 {
		pods, err := r.nativePreflightPods(ctx, m.Namespace)
		if err != nil {
			return errNativeDisposition
		}
		for _, pod := range pods {
			for _, helper := range closed.removed {
				if pod.Name == helper.pod.Name || pod.UID == helper.pod.UID {
					return errNativeDisposition
				}
				for _, owner := range pod.OwnerReferences {
					if owner.UID == helper.job.UID {
						return errNativeDisposition
					}
				}
			}
		}
	}
	if ctx.Err() != nil {
		return errNativeDisposition
	}
	return nil
}

func (r *MigrationReconciler) readNativeDeletionInventory(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (native.AdmittedOwnedState, error) {
	var owned native.AdmittedOwnedState
	err := r.runNativeRecoveryRead(ctx, m, worker, pvc, original, native.JobRoleCleanup, func(ctx context.Context, w native.ExecutionData) error {
		var readErr error
		owned, readErr = r.NativeEvidence.ReadOwnedState(ctx, w)
		return readErr
	})
	if err != nil || owned.Work() == (native.WorkIdentity{}) || len(owned.Resources()) == 0 {
		return native.AdmittedOwnedState{}, errNativeDisposition
	}
	return owned, nil
}

func (r *MigrationReconciler) cleanupNativeDeletion(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) error {
	if m == nil || m.DeletionTimestamp.IsZero() || r.NativeJobs == nil || !followEnabled(m) {
		return errNativeDisposition
	}
	_, original, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original)
	if err != nil {
		return errNativeDisposition
	}
	owned, err := r.readNativeDeletionInventory(ctx, m, worker, pvc, original)
	if err != nil {
		return errNativeDisposition
	}
	if err := r.runNativeDeletionCleanup(ctx, m, worker, pvc, original, owned); err != nil {
		return err
	}
	after, err := r.readNativeDeletionInventory(ctx, m, worker, pvc, original)
	if err != nil || after.Work() != owned.Work() || !slices.Equal(after.Resources(), owned.Resources()) {
		return errNativeDisposition
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
		return errNativeDisposition
	}
	return nil
}

func (r *MigrationReconciler) runNativeDeletionCleanup(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, owned native.AdmittedOwnedState) (err error) {
	if r.ensureNativeRecoveryPreflight(ctx, m, worker, pvc, original) != nil {
		return errNativeDisposition
	}
	op, err := nativeRecoveryOperationID()
	if err != nil {
		return errNativeDisposition
	}
	expected, err := buildNativeCleanupJob(m, pvc, r.RPGMigrateRunnerImage, "rpgmigrate-cleanup-"+op, owned)
	if err != nil || controllerutil.SetControllerReference(m, expected, r.Scheme) != nil || native.ValidateEvidenceJobSize(expected) != nil {
		return errNativeDisposition
	}
	deadline := time.Now().Add(90 * time.Second)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	work, cancel := context.WithDeadline(ctx, deadline.Add(-nativePreflightCleanupReserve))
	defer cancel()
	if _, _, err := r.nativeRecoveryQuiescentCurrent(work, m, worker, pvc, original); err != nil {
		return errNativeDisposition
	}
	created := expected.DeepCopy()
	if r.Create(work, created) != nil || created.UID == "" {
		return errNativeDisposition
	}
	var pod *corev1.Pod
	defer func() {
		cleanup, end := context.WithDeadline(context.WithoutCancel(ctx), deadline)
		defer end()
		if r.nativePreflightCleanup(cleanup, m, created, pod) != nil {
			err = errNativeDisposition
		}
	}()
	if created.UID == worker.UID || !nativePreflightJobMatches(expected, created) {
		return errNativeDisposition
	}
	observed, terminal, err := r.nativeDeletionCleanupTerminal(work, m, worker, pvc, original, expected, created)
	if err != nil {
		return errNativeDisposition
	}
	pod = terminal
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: workerContainer, ContainerID: pod.Status.ContainerStatuses[0].ContainerID, ImageID: image, TemplateSHA256: podexec.NativeTemplateSHA256(created.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(created.Spec.Template)}
	output, err := r.NativeJobs.JobCleanupNative(work, m.Namespace, created.UID, binding)
	if err != nil || !output.Successful() {
		return errNativeDisposition
	}
	if !nativeDeletionCleanupReportMatches(output.Payload(), owned) {
		return errNativeDisposition
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(work, m, worker, pvc, original); err != nil {
		return errNativeDisposition
	}
	job, err := r.nativePreflightLiveJob(work, expected, created, binding)
	if err != nil || !apiequality.Semantic.DeepEqual(job.Status, observed.Status) {
		return errNativeDisposition
	}
	finalPod, err := r.nativePreflightPod(work, job)
	if err != nil || finalPod == nil || !nativePreflightPodMatches(job, finalPod) || !nativePreflightSamePod(pod, finalPod) || !apiequality.Semantic.DeepEqual(pod.Spec, finalPod.Spec) || !apiequality.Semantic.DeepEqual(pod.Status, finalPod.Status) || work.Err() != nil {
		return errNativeDisposition
	}
	return nil
}

func (r *MigrationReconciler) nativeDispositionUnstarted(ctx context.Context, expected *v1beta1.Migration) error {
	if expected.Status.Attempts != 0 || expected.Status.JobName != "" || expected.Status.NativeJobUID != "" || expected.Status.NativePVCUID != "" || expected.Status.NativeSchemaJobUID != "" || expected.Status.NativeDataJobUID != "" {
		return errNativeDisposition
	}
	live := &v1beta1.Migration{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), live) != nil || live.UID != expected.UID || live.Generation != expected.Generation || live.ResourceVersion != expected.ResourceVersion ||
		!apiequality.Semantic.DeepEqual(live.Spec, expected.Spec) || !apiequality.Semantic.DeepEqual(live.Status, expected.Status) || !apiequality.Semantic.DeepEqual(live.DeletionTimestamp, expected.DeletionTimestamp) {
		return errNativeDisposition
	}
	for _, name := range []string{jobName(live, 1), compareJobName(live, compareSchema), compareJobName(live, compareData)} {
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: live.Namespace, Name: name}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
			return errNativeDisposition
		}
	}
	if ctx.Err() != nil {
		return errNativeDisposition
	}
	return nil
}

func (r *MigrationReconciler) recordNativeSuspended(ctx context.Context, m, base *v1beta1.Migration) (bool, ctrl.Result, error) {
	desired := m.DeepCopy()
	desired.Status.Phase, desired.Status.ObservedGeneration = v1beta1.PhaseSuspended, desired.Generation
	r.endHold(desired, reasonHoldSuspended, "Native worker is quiescent; retained state is preserved")
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return true, ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	metrics.Record(desired)
	return true, ctrl.Result{}, nil
}

func (r *MigrationReconciler) finishNativeDeletion(ctx context.Context, m *v1beta1.Migration) (bool, ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(m, finalizerName) {
		desired := m.DeepCopy()
		controllerutil.RemoveFinalizer(desired, finalizerName)
		if err := r.Patch(ctx, desired, client.MergeFromWithOptions(m, client.MergeFromWithOptimisticLock{})); err != nil {
			return true, ctrl.Result{}, nativeWorkerError(err)
		}
		desired.DeepCopyInto(m)
	}
	metrics.Forget(m.Namespace, m.Name)
	return true, ctrl.Result{}, nil
}

func (r *MigrationReconciler) nativeDeletionCleanupTerminal(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, expected, created *batchv1.Job) (*batchv1.Job, *corev1.Pod, error) {
	var observed *batchv1.Job
	var pod *corev1.Pod
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	err := wait.PollUntilContextCancel(ctx, nativePreflightPoll, true, func(ctx context.Context) (bool, error) {
		if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
			return false, errNativeDisposition
		}
		job, err := r.nativeEvidenceLiveJob(ctx, created, m)
		if err != nil || job.Generation != created.Generation || !nativePreflightJobMatches(expected, job) || nativePreflightJobFailed(job) {
			return false, errNativeDisposition
		}
		current, err := r.nativePreflightPod(ctx, job)
		if err != nil || current != nil && (!nativePreflightPodMatches(job, current) || !nativePreflightSamePod(pod, current)) {
			return false, errNativeDisposition
		}
		if current == nil {
			return false, nil
		}
		pod = current.DeepCopy()
		if !nativeEvidenceWorkerComplete(job) {
			return false, nil
		}
		if !nativeRecoveryPodTerminal(pod, image) || pod.Status.Phase != corev1.PodSucceeded || pod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 || pod.Status.ContainerStatuses[0].State.Terminated.Signal != 0 {
			return false, errNativeDisposition
		}
		observed = job
		return true, nil
	})
	if err != nil {
		return nil, nil, errNativeDisposition
	}

	return observed, pod, nil

}

func nativeDeletionCleanupReportMatches(raw []byte, owned native.AdmittedOwnedState) bool {
	if len(raw) > 65536 || bytes.Count(raw, []byte{'\n'}) != 1 || !bytes.HasSuffix(raw, []byte{'\n'}) || !bytes.HasPrefix(raw, []byte("native-report: ")) {
		return false
	}
	report, err := native.ParseReportLine(raw, native.Binding{Command: "stream cleanup", Work: owned.Work(), Resources: owned.Resources()})
	if err != nil || report.Phase != "completed" || report.Cleanup == nil || report.Cleanup.Complete == nil || !*report.Cleanup.Complete {
		return false
	}

	return true

}

func (r *MigrationReconciler) nativeRetainedDispositionTerminal(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*batchv1.Job, *corev1.Pod, error) {
	current := worker.DeepCopy()
	var original *corev1.Pod
	if !nativeEvidenceWorkerComplete(current) && !nativeRecoveryWorkerFailed(current) {
		before, err := r.nativeStopCurrent(ctx, m, current, pvc, nil)
		if err != nil {
			return nil, nil, errNativeDisposition
		}
		done, err := r.stopNativeRetained(ctx, m, current, pvc)
		if err != nil || !done {
			return nil, nil, errNativeDisposition
		}
		after, err := r.nativeStopCurrent(ctx, m, current, pvc, &before)
		if err != nil || !nativeStopTerminal(after) {
			return nil, nil, errNativeDisposition
		}
		current, original = after.worker, after.pod
	}
	_, pod, err := r.nativeRecoveryQuiescentCurrent(ctx, m, current, pvc, original)
	if err != nil {
		return nil, nil, errNativeDisposition
	}
	return current, pod, nil
}
