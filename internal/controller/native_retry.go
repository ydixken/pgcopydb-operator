package controller

import (
	"context"
	"errors"
	"slices"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type nativeRetryAdmission struct {
	migration      *v1beta1.Migration
	worker         *batchv1.Job
	pvc            *corev1.PersistentVolumeClaim
	pod            *corev1.Pod
	readiness      *native.RetryReadiness
	closed         *nativeClosedCompareHelpers
	used, recorded bool
}

func nativeRetryPolicyValid(m *v1beta1.Migration) bool {
	return m != nil && m.Status.NativeFinalizedJobUID == "" && m.Status.Attempts > 0 && m.Status.Attempts < int32(1<<31-1) && m.Spec.BackoffLimit >= 0 && m.Spec.BackoffLimit < int32(1<<31-1) &&
		m.Status.ExemptAttempts >= 0 && m.Status.ExemptAttempts <= m.Status.Attempts
}

func nativeRetryObservationPassed(o native.RetryGateObservation, in native.RetryInputs, now time.Time) bool {
	observed := time.Unix(o.ObservedAtUnix, 0)
	return o.ObservedAtUnix > 0 && !observed.After(now) && !now.After(observed.Add(gatePassFreshness)) && o.Total == 0 && o.SyncRep == 0 && o.GateWaiting == 0 &&
		o.SynchronousCommitAdmitted && (!in.ExpectLocal || o.TargetSynchronousCommit == v1beta1.SynchronousCommitLocal) && (!in.CommitTest || o.ProbeAttempted && o.ProbeCommitted)
}

func nativeRetryInputs(m *v1beta1.Migration) native.RetryInputs {
	in := gateInputsFor(m)
	return native.RetryInputs{CommitTest: in.CommitTest, ExpectLocal: in.ExpectLocal}
}

func (r *MigrationReconciler) nativeRetryCurrent(ctx context.Context, expected *v1beta1.Migration, a *nativeRetryAdmission) (*v1beta1.Migration, *corev1.PersistentVolumeClaim, error) {
	if a == nil || a.migration == nil || a.readiness == nil || a.closed == nil || expected == nil || !nativeRetryPolicyValid(expected) || expected.Spec.Suspend || !expected.DeletionTimestamp.IsZero() ||
		!apiequality.Semantic.DeepEqual(expected.Status, a.migration.Status) || !apiequality.Semantic.DeepEqual(expected.Spec, a.migration.Spec) || expected.Generation != a.migration.Generation || expected.UID != a.migration.UID ||
		!onFailurePath(expected) || expected.Status.RetryGate.Exemption == "" || budgetSpent(expected) {
		return nil, nil, errNativeWorkerRetry
	}
	observation, known := a.readiness.Observation()
	if !known || !nativeRetryObservationPassed(observation, nativeRetryInputs(expected), r.currentTime()) {
		return nil, nil, errNativeWorkerRetry
	}
	live, _, err := r.nativeRecoveryQuiescentCurrent(ctx, expected, a.worker, a.pvc, a.pod)
	if err != nil || !nativeRecoveryWorkerFailed(a.worker) || r.nativeCompareClosureCurrent(ctx, live, a.closed) != nil {
		return nil, nil, errNativeWorkerRetry
	}
	return live, a.pvc, nil
}

func (r *MigrationReconciler) reconcileNativeRetry(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	if !nativeRetryPolicyValid(m) || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || !nativeRecoveryWorkerFailed(worker) {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	live, original, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, nil)
	if err != nil {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	if live.Status.RetryGate == nil {
		desired := live.DeepCopy()
		failedAt := jobFailedAt(worker)
		if failedAt.IsZero() {
			failedAt = r.currentTime()
		}
		desired.Status.RetryGate = &v1beta1.RetryGateStatus{FailedAttempt: live.Status.Attempts, FailedAt: metav1.NewTime(failedAt), Failure: "Native worker failed"}
		r.setCondition(desired, v1beta1.ConditionRetryHeld, metav1.ConditionTrue, reasonGateProbing, "Native retry waits for fresh target authority")
		return r.patchNativeRetry(ctx, m, desired, base)
	}
	rg := live.Status.RetryGate
	if !nativeRetryGateCurrent(rg, live.Status.Attempts, r.currentTime()) {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	if rg.Exemption == "" && verdictOverdue(rg, r.currentTime()) {
		desired := live.DeepCopy()
		desired.Status.RetryGate.Exemption = v1beta1.RetryGateCounted
		if budgetSpent(desired) {
			r.failNativeRetryBudget(desired)
		}
		return r.patchNativeRetry(ctx, m, desired, base)
	}
	if rg.Exemption != "" && budgetSpent(live) {
		if live.Status.Phase == v1beta1.PhaseFailed && meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionFailed) {
			return ctrl.Result{}, nil
		}
		desired := live.DeepCopy()
		r.failNativeRetryBudget(desired)
		return r.patchNativeRetry(ctx, m, desired, base)
	}
	var readiness *native.RetryReadiness
	err = r.runNativeRecoveryRead(ctx, live, worker, pvc, original, native.JobRoleRetryGate, func(ctx context.Context, w native.ExecutionData) error {
		var readErr error
		readiness, readErr = r.NativeEvidence.ReadRetryReadiness(ctx, w, nativeRetryInputs(live))
		return readErr
	})
	if err != nil || readiness == nil {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	observation, known := readiness.Observation()
	if !known {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, live, worker, pvc, original); err != nil {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	if rg.Exemption == "" {
		desired := live.DeepCopy()
		desired.Status.RetryGate.Exemption = exemptionVerdict(gateCensus{Total: observation.Total, SyncRep: observation.SyncRep, GateWaiting: observation.GateWaiting, SyncRepSince: observation.OldestSyncRepWaitUnix, Named: observation.ApplicationAttributionKnown}, rg.FailedAt.Time)
		if desired.Status.RetryGate.Exemption == v1beta1.RetryGateExempt {
			if desired.Status.ExemptAttempts == int32(1<<31-1) {
				return ctrl.Result{}, errNativeWorkerRetry
			}
			desired.Status.ExemptAttempts++
		}
		if budgetSpent(desired) {
			r.failNativeRetryBudget(desired)
		}
		return r.patchNativeRetry(ctx, m, desired, base)
	}
	if !nativeRetryObservationPassed(observation, nativeRetryInputs(live), r.currentTime()) {
		desired := live.DeepCopy()
		r.setCondition(desired, v1beta1.ConditionRetryHeld, metav1.ConditionTrue, reasonSessionsRemain, "Native target retry authority holds the next worker")
		return r.patchNativeRetry(ctx, m, desired, base)
	}
	closed, err := r.closeNativeCompareHelpers(ctx, live, worker, pvc, original)
	if err != nil {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	a := &nativeRetryAdmission{migration: live.DeepCopy(), worker: worker.DeepCopy(), pvc: pvc.DeepCopy(), pod: original.DeepCopy(), readiness: readiness, closed: closed}
	return r.startNativeRetry(ctx, live, live.DeepCopy(), a)
}

func (r *MigrationReconciler) failNativeRetryBudget(m *v1beta1.Migration) {
	m.Status.Phase = v1beta1.PhaseFailed
	r.setCondition(m, v1beta1.ConditionFailed, metav1.ConditionTrue, "BackoffLimitExceeded", "Native retry budget is exhausted")
}

func (r *MigrationReconciler) patchNativeRetry(ctx context.Context, m, desired, base *v1beta1.Migration) (ctrl.Result, error) {
	desired.Status.ObservedGeneration = desired.Generation
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	return ctrl.Result{RequeueAfter: retryGateInterval}, nil
}

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
		!apiequality.Semantic.DeepEqual(pod.Spec, confirmed.Spec) || !podexec.SamePodState(pod, confirmed, workerContainer) {
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

func nativeRetryGateCurrent(rg *v1beta1.RetryGateStatus, attempt int32, now time.Time) bool {
	return rg != nil && rg.FailedAttempt == attempt && !rg.FailedAt.IsZero() && !rg.FailedAt.After(now) && (rg.Exemption == "" || rg.Exemption == v1beta1.RetryGateCounted || rg.Exemption == v1beta1.RetryGateExempt)
}
