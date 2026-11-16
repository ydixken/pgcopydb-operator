package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// nativeRetryRouted reports whether a pass that is neither deleting nor suspended routes to the failed-worker retry.
func nativeRetryRouted(worker *batchv1.Job) bool {
	return nativeRetryWorker(worker)
}

// nativeRetryWorker is the W the retry gate may follow: a failed admitted Job, or a recorded Job that is gone.
func nativeRetryWorker(worker *batchv1.Job) bool {
	return worker != nil && (nativeWorkerMissing(worker) || nativeRecoveryWorkerFailed(worker))
}

// reconcileNativeFailedWorker ends the Migration on a terminal worker failure, or else runs the retry gate.
func (r *MigrationReconciler) reconcileNativeFailedWorker(ctx context.Context, live *v1beta1.Migration, current *batchv1.Job, claim *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	terminal, err := r.reconcileNativeFailure(ctx, live, current, claim)
	if busy, ok := errors.AsType[*nativeWorkerBusy](err); ok {
		return r.holdNativeRetryBusy(ctx, live, busy)
	}
	if terminal || err != nil {
		return ctrl.Result{}, err
	}
	return r.reconcileNativeRetry(ctx, live, live.DeepCopy(), current, claim)
}

// holdNativeRetryBusy is the retry wait of R1: RetryHeld=True WorkerPodRemains names the Job or pod.
func (r *MigrationReconciler) holdNativeRetryBusy(ctx context.Context, live *v1beta1.Migration, busy *nativeWorkerBusy) (ctrl.Result, error) {
	if r.Recorder == nil {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	desired := live.DeepCopy()
	r.setHold(desired, reasonWorkerPodRemains, busy.message(live.Status.Attempts+1))
	if !apiequality.Semantic.DeepEqual(desired.Status, live.Status) {
		if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(live, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, nativeWorkerError(err)
		}
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

type nativeFailureReader interface {
	JobFailureNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
}

func (r *MigrationReconciler) reconcileNativeFailure(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	live, original, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, nil)
	if errors.Is(err, errNativeRecoveryActive) {
		return false, err
	}
	if err != nil || !nativeRetryWorker(worker) || live.Status.NativeFinalizedJobUID != "" {
		return false, errNativeDisposition
	}
	if condition := meta.FindStatusCondition(live.Status.Conditions, v1beta1.ConditionFailed); condition != nil && condition.Status == metav1.ConditionTrue && live.Status.Phase == v1beta1.PhaseFailed && live.Status.ObservedGeneration == live.Generation {
		switch condition.Reason {
		case nativeFailureBadArgs, nativeFailureBadConfig, nativeFailureBadState:
			return true, nil
		}
	}
	reader, available := r.NativeJobs.(nativeFailureReader)
	if !available || nativePodless(original) {
		// Without a pod there is no log to classify; the attempt fails ordinarily and the gate decides.
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
	r.nativeCloneAttemptEnded(desired, reasonCloneFailed)
	r.setCondition(desired, v1beta1.ConditionFailed, metav1.ConditionTrue, reason, "Native worker reported a terminal failure; retained state is preserved")
	r.setCondition(desired, v1beta1.ConditionComplete, metav1.ConditionFalse, reason, "Native worker did not complete")
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(confirmed, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	metrics.Record(desired)
	return true, nil
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
	if err != nil || !nativeRetryWorker(a.worker) || r.nativeCompareClosureCurrent(ctx, live, a.closed) != nil {
		return nil, nil, errNativeWorkerRetry
	}
	return live, a.pvc, nil
}

func (r *MigrationReconciler) reconcileNativeRetry(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	if !nativeRetryPolicyValid(m) || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || !nativeRetryWorker(worker) {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	live, original, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, nil)
	if err != nil {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	if live.Status.Phase == v1beta1.PhaseFailed && meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionFailed) {
		return ctrl.Result{}, nil
	}
	if live.Status.RetryGate == nil {
		desired := live.DeepCopy()
		r.recordNativeRetryGate(desired, worker)
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
		desired := live.DeepCopy()
		r.failNativeRetryBudget(desired)
		return r.patchNativeRetry(ctx, m, desired, base)
	}
	now := r.currentTime()
	if last := rg.LastReadAt; last != nil && !last.After(now) {
		// The stamp keeps whole seconds, so one more second keeps two reads a full interval apart.
		if next := last.Add(retryGateInterval + time.Second); now.Before(next) {
			return ctrl.Result{RequeueAfter: next.Sub(now)}, nil
		}
	}
	// The stamp is durable before the helper Job exists, so neither a requeue nor a restart reads early.
	stamped := live.DeepCopy()
	stamped.Status.RetryGate.LastReadAt = &metav1.Time{Time: now}
	if _, err = r.patchNativeRetry(ctx, m, stamped, base); err != nil {
		return ctrl.Result{}, err
	}
	live, base = stamped, stamped.DeepCopy()
	var readiness *native.RetryReadiness
	err = r.runNativeRecoveryRead(ctx, live, worker, pvc, original, native.JobRoleRetryGate, func(ctx context.Context, w native.ExecutionData) error {
		var readErr error
		readiness, readErr = r.NativeEvidence.ReadRetryReadiness(ctx, w, nativeRetryInputs(live))
		return readErr
	})
	var observation native.RetryGateObservation
	known := false
	if err == nil && readiness != nil {
		observation, known = readiness.Observation()
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, live, worker, pvc, original); err != nil {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	if !known {
		return r.holdNativeRetryUnobserved(ctx, m, live, base)
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

// recordNativeRetryGate opens the gate for the failed attempt; a vanished worker counts at once,
// since no census binds it to a verdict.
func (r *MigrationReconciler) recordNativeRetryGate(desired *v1beta1.Migration, worker *batchv1.Job) {
	if nativeWorkerMissing(worker) {
		r.countNativeAttempt(desired, "Native worker Job is gone")
		return
	}
	failedAt := jobFailedAt(worker)
	if failedAt.IsZero() {
		failedAt = r.currentTime()
	}
	desired.Status.RetryGate = &v1beta1.RetryGateStatus{FailedAttempt: desired.Status.Attempts, FailedAt: metav1.NewTime(failedAt), Failure: "Native worker failed"}
	r.nativeCloneAttemptEnded(desired, reasonAttemptFailed)
	r.setCondition(desired, v1beta1.ConditionRetryHeld, metav1.ConditionTrue, reasonGateProbing, "Native retry waits for fresh target authority")
}

// holdNativeRetryUnobserved paces a gate read without an observation at the gate interval.
// Past the verdict deadline, a gate that still has none fails the Migration instead of looping.
func (r *MigrationReconciler) holdNativeRetryUnobserved(ctx context.Context, m, live, base *v1beta1.Migration) (ctrl.Result, error) {
	rg, desired := live.Status.RetryGate, live.DeepCopy()
	held := meta.FindStatusCondition(live.Status.Conditions, v1beta1.ConditionRetryHeld)
	// Only a hold whose last read had an observation (SessionsRemain) earns one more read.
	unobserved := held == nil || held.Reason == reasonGateProbing || held.Reason == reasonProbeFailed
	if unobserved && !r.currentTime().Before(rg.FailedAt.Add(verdictDeadline)) {
		desired.Status.Phase = v1beta1.PhaseFailed
		r.nativeCloneAttemptEnded(desired, reasonCloneFailed)
		r.setCondition(desired, v1beta1.ConditionFailed, metav1.ConditionTrue, reasonNativeRetryGateUnobserved, fmt.Sprintf(
			"The native retry-gate helper (Job rpgmigrate-retry-<operation>) gave no target observation within %s after attempt %d failed", verdictDeadline, rg.FailedAttempt))
		r.endHold(desired, reasonHoldMigrationFailed, fmt.Sprintf("Attempt %d does not start: the Migration failed with %s", rg.FailedAttempt+1, reasonNativeRetryGateUnobserved))
	} else {
		r.setCondition(desired, v1beta1.ConditionRetryHeld, metav1.ConditionTrue, reasonProbeFailed, fmt.Sprintf(
			"The native retry-gate helper (Job rpgmigrate-retry-<operation>) gave no target observation; the gate reads again in %s", retryGateInterval))
	}
	return r.patchNativeRetry(ctx, m, desired, base)
}

func (r *MigrationReconciler) failNativeRetryBudget(m *v1beta1.Migration) {
	m.Status.Phase = v1beta1.PhaseFailed
	r.nativeCloneAttemptEnded(m, reasonCloneFailed)
	r.setCondition(m, v1beta1.ConditionFailed, metav1.ConditionTrue, "BackoffLimitExceeded", "Native retry budget is exhausted")
	r.endHold(m, reasonHoldMigrationFailed, fmt.Sprintf("Attempt %d does not start: the Migration failed with BackoffLimitExceeded", m.Status.Attempts+1))
}

func (r *MigrationReconciler) patchNativeRetry(ctx context.Context, m, desired, base *v1beta1.Migration) (ctrl.Result, error) {
	desired.Status.ObservedGeneration = desired.Generation
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	return ctrl.Result{RequeueAfter: retryGateInterval}, nil
}

// reasonAttemptFailed replaces CloneRunning while a failed native attempt awaits the retry gate.
const reasonAttemptFailed = "AttemptFailed"

// nativeCloneAttemptEnded replaces the running claim of the failed worker; a finished copy stays True.
func (r *MigrationReconciler) nativeCloneAttemptEnded(m *v1beta1.Migration, reason string) {
	if c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCloneCompleted); c != nil && (c.Reason == reasonCloneRunning || c.Reason == reasonAttemptFailed) {
		r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, reason, fmt.Sprintf("attempt %d failed as Job %s", m.Status.Attempts, m.Status.JobName))
	}
}

// reasonNativeRetryGateUnobserved fails a native Migration whose retry gate never read the target.
const reasonNativeRetryGateUnobserved = "NativeRetryGateUnobserved"

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
	worker := &batchv1.Job{}
	err = r.APIReader.Get(ctx, client.ObjectKeyFromObject(expectedWorker), worker)
	if nativeWorkerMissing(expectedWorker) {
		// A recorded worker that is gone stays gone: any Job under its name is another object.
		if !apierrors.IsNotFound(err) || ctx.Err() != nil {
			return nil, nil, nil, errNativeRecovery
		}
		return live, expectedWorker, pvc, nil
	}
	// The admission digest, not a rebuild from the current spec, binds the live worker (lifecycle 4).
	if err != nil || ctx.Err() != nil || worker.UID != expectedWorker.UID || !nativeWorkerAdmitted(live, worker) ||
		!nativePreflightJobMatches(expectedWorker, worker) || !apiequality.Semantic.DeepEqual(worker.Status, expectedWorker.Status) {
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

// nativeQuiescentWorker is the engine-free check for readers that need the worker's log: it refuses the pod-less anchor.
func (r *MigrationReconciler) nativeQuiescentWorker(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	pod, err := r.nativeEngineFreeWorker(ctx, m, worker, pvc)
	if err == nil && nativePodless(pod) {
		return nil, errNativeRecovery
	}
	return pod, err
}

// nativeEngineFreeWorker is R1 for the recorded worker, observed twice: its terminal pod, or the pod-less anchor.
func (r *MigrationReconciler) nativeEngineFreeWorker(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	live, current, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil {
		return nil, errNativeRecovery
	}
	pod, err := r.nativeWorkerEngineFree(ctx, current)
	if err != nil {
		return nil, err
	}
	if !nativePodless(pod) && (!nativePreflightPodMatches(current, pod) ||
		!nativeRecoveryPodTerminal(pod, runnerImageFor(live, r.RPGMigrateRunnerImage)) ||
		nativeEvidenceWorkerComplete(current) && (pod.Status.Phase != corev1.PodSucceeded || pod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 || pod.Status.ContainerStatuses[0].State.Terminated.Signal != 0) ||
		nativeRecoveryWorkerFailed(current) && pod.Status.Phase != corev1.PodFailed) {
		return nil, errNativeRecovery
	}
	if _, _, _, err := r.nativeRecoveryCurrent(ctx, live, current, claim); err != nil {
		return nil, errNativeRecovery
	}
	confirmed, err := r.nativeWorkerEngineFree(ctx, current)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !nativeSameWorkerPod(pod, confirmed) {
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
