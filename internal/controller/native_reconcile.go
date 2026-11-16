package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	errNativeReconcile                   = errors.New("native reconciliation identity or execution unavailable")
	errNativeFinalizationReceiptConflict = errors.New("NativeFinalizationReceiptConflict")
	errNativeRunnerRecord                = errors.New("native runner record is missing or unqualified past admission")
)

const reasonNativePreflightFailed = "NativePreflightFailed"

func unsupportedEngineMessage(m *v1beta1.Migration, cause error) string {
	if cause != nil {
		return cause.Error()
	}
	msg := "rpgmigrate backend is not qualified for this runner image"
	// The runner's advertised capabilities are unknown here, so only configuration errors are named.
	if _, err := nativeConfigCapabilities(m); err != nil {
		msg += ": " + err.Error()
	}
	return msg
}

func nativeRunnerQualified(image string) bool {
	return nativeEvidenceDirectImage(image) && native.QualifiedRunner(image)
}

// engineUnsupported reports whether dispatch persists UnsupportedEngine; refused is a status integrity error that writes nothing.
func (r *MigrationReconciler) engineUnsupported(m *v1beta1.Migration, engine v1beta1.MigrationEngine, engineErr error) (bool, error) {
	switch {
	case engineErr != nil:
		return true, nil
	case engine != v1beta1.MigrationEngineRPGMigrate:
		return false, nil
	case !nativeRunnerRecordIntact(m):
		return false, errNativeRunnerRecord
	}
	return m.Status.NativeRunnerImage == "" && !nativeRunnerQualified(runnerImageFor(m, r.RPGMigrateRunnerImage)), nil
}

// nativeRunnerRecordIntact refuses a native Migration past admission without a qualified runner record.
// Failing it instead would end the Migration without native cleanup, and admitting it again could switch the runner.
func nativeRunnerRecordIntact(m *v1beta1.Migration) bool {
	if image := m.Status.NativeRunnerImage; image != "" {
		return nativeRunnerQualified(image)
	}
	// Only an empty status or an admission refusal precedes the record.
	return statusIsEmpty(m.Status) || meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed) && m.Status.Attempts == 0 && m.Status.JobName == ""
}

func (r *MigrationReconciler) reconcileNative(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	observing := false
	defer func() {
		if !observing && m != nil {
			r.stopSampling(client.ObjectKeyFromObject(m))
		}
	}()
	live, err := r.nativeReconcileCurrent(ctx, m, base)
	if err != nil {
		return ctrl.Result{}, err
	}
	m, base = live, live.DeepCopy()
	if handled, result, err := r.reconcileNativeHistory(ctx, m, base); handled {
		return result, err
	}
	if r.Scheme == nil {
		return ctrl.Result{}, errNativeReconcile
	}
	if nativeFailedAbsorbs(m) {
		metrics.Record(m)
		return ctrl.Result{}, nil
	}
	state, err := r.nativeReconcileResources(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if handled, result, err := r.reconcileNativeDeletionGuards(ctx, m, base, state); handled {
		return result, err
	}
	if handled, result, err := r.reconcileNativeWorkerState(ctx, m, base, state); handled {
		return result, err
	}
	worker, pvc := state.worker, state.pvc
	if handled, result, err := r.reconcileNativeDisposition(ctx, m, base, worker, pvc); handled || err != nil {
		return result, err
	}
	if !m.DeletionTimestamp.IsZero() || m.Spec.Suspend {
		return ctrl.Result{}, errNativeReconcile
	}
	if m.Status.Attempts == 0 {
		if worker != nil || nativeStartResidue(m) {
			return ctrl.Result{}, errNativeReconcile
		}
		if meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed) {
			metrics.Record(m)
			return ctrl.Result{}, nil
		}
		if meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete) {
			return ctrl.Result{}, errNativeReconcile
		}
		if statusIsEmpty(m.Status) {
			// Dispatch qualified this image in the same pass; recording it ends admission.
			m.Status.Phase, m.Status.NativeRunnerImage = v1beta1.PhasePending, runnerImageFor(m, r.RPGMigrateRunnerImage)
			if err := r.updateNativeStatus(ctx, m, base); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: time.Nanosecond}, nil
		}
		return r.reconcileNativeStart(ctx, m, base, pvc)
	}
	if worker == nil || pvc == nil || !native.NativeReceiptsMatch(m, worker.UID, pvc.UID) {
		return ctrl.Result{}, errNativeReconcile
	}
	if nativeEvidenceWorkerComplete(worker) {
		r.stopSampling(client.ObjectKeyFromObject(m))
		if followEnabled(m) {
			return r.finishNativeFollow(ctx, m, base, worker, pvc)
		}
		return r.finishNativeClone(ctx, m, base, worker, pvc)
	}
	if nativeRecoveryWorkerFailed(worker) {
		return ctrl.Result{}, errNativeReconcile
	}
	observing = true
	return r.reconcileNativeRunning(ctx, m, base, worker, pvc)
}

// nativeFailedAbsorbs is R6: Failed absorbs every action but deletion; it is written only once the worker is engine-free.
func nativeFailedAbsorbs(m *v1beta1.Migration) bool {
	return m.DeletionTimestamp.IsZero() && m.Status.Phase == v1beta1.PhaseFailed && meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed)
}

// nativeStartResidue reports attempt-0 status that only a recorded or intended attempt could have written.
func nativeStartResidue(m *v1beta1.Migration) bool {
	return m.Status.JobName != "" || m.Status.NativeJobUID != "" || m.Status.NativePVCUID != "" || m.Status.NativeSchemaJobUID != "" ||
		m.Status.NativeDataJobUID != "" || nativeControlEnd(m) != "" || m.Status.NativeWorkerIntent != ""
}

func (r *MigrationReconciler) reconcileNativeHistory(ctx context.Context, m, base *v1beta1.Migration) (bool, ctrl.Result, error) {
	if marker := m.Status.NativeFinalizedJobUID; marker != "" && marker != m.Status.NativeJobUID {
		return true, ctrl.Result{}, errNativeFinalizationReceiptConflict
	}
	if !nativeFinalizationRecorded(m) {
		return false, ctrl.Result{}, nil
	}
	r.stopSampling(client.ObjectKeyFromObject(m))
	metrics.Record(m)
	if !m.DeletionTimestamp.IsZero() {
		handled, result, err := r.reconcileNativeDisposition(ctx, m, base, nil, nil)
		if !handled && err == nil {
			return true, ctrl.Result{}, errNativeReconcile
		}
		return true, result, err
	}
	return true, ctrl.Result{}, r.disposeNativeFinalizedVerificationHelpers(ctx, m)
}

func (r *MigrationReconciler) nativeReconcileCurrent(ctx context.Context, m, base *v1beta1.Migration) (*v1beta1.Migration, error) {
	if r.Client == nil || r.APIReader == nil || m == nil || base == nil || ctx.Err() != nil ||
		!nativeControlStatusIdentity(m, base) || m.ResourceVersion == "" || m.ResourceVersion != base.ResourceVersion ||
		!apiequality.Semantic.DeepEqual(m.Status, base.Status) {
		return nil, errNativeReconcile
	}
	live := &v1beta1.Migration{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(m), live) != nil || ctx.Err() != nil ||
		!nativeControlStatusIdentity(live, m) || !apiequality.Semantic.DeepEqual(live.Labels, m.Labels) ||
		!apiequality.Semantic.DeepEqual(live.Annotations, m.Annotations) || !apiequality.Semantic.DeepEqual(live.Finalizers, m.Finalizers) ||
		!apiequality.Semantic.DeepEqual(live.DeletionTimestamp, m.DeletionTimestamp) {
		return nil, errNativeReconcile
	}
	if live.ResourceVersion != m.ResourceVersion || !apiequality.Semantic.DeepEqual(live.Status, m.Status) {
		return nil, nativeControlConflict()
	}
	return live, nil
}

// nativeReconcileResources resolves V, N and W from uncached reads; it refuses only a status that
// contradicts itself or an unavailable API, and leaves every object state to the tables.
func (r *MigrationReconciler) nativeReconcileResources(ctx context.Context, m *v1beta1.Migration) (nativeWorkerState, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var s nativeWorkerState
	n := m.Status.Attempts
	if n < 0 || n == int32(1<<31-1) || n > 0 && (m.Status.JobName != jobName(m, n) || !native.NativeReceiptsMatch(m, types.UID(m.Status.NativeJobUID), types.UID(m.Status.NativePVCUID)) || m.Status.NativeJobSHA256 == "") ||
		n == 0 && (m.Status.JobName != "" || m.Status.NativeJobUID != "" || m.Status.NativePVCUID != "" || m.Status.NativeJobSHA256 != "") {
		return s, errNativeReconcile
	}
	for _, resolve := range []func(context.Context, *v1beta1.Migration, *nativeWorkerState) error{r.nativeResolveVolume, r.nativeResolveNext, r.nativeResolveWorker} {
		if err := resolve(ctx, m, &s); err != nil {
			return s, errNativeReconcile
		}
	}
	if ctx.Err() != nil {
		return s, errNativeReconcile
	}
	return s, nil
}

func (r *MigrationReconciler) nativeResolveVolume(ctx context.Context, m *v1beta1.Migration, s *nativeWorkerState) error {
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: workPVCName(m)}, pvc)
	switch {
	case apierrors.IsNotFound(err):
		s.v = volumeAbsent
		if m.Status.Attempts > 0 {
			s.v = volumeLost
		}
	case err != nil:
		return err
	default:
		if _, idErr := nativeWorkIdentity(m, pvc); idErr != nil || m.Status.Attempts > 0 && string(pvc.UID) != m.Status.NativePVCUID {
			s.v = volumeLost
		} else {
			s.v, s.pvc = volumeBound, pvc
		}
	}
	return nil
}

// nativeResolveNext is R2's read: a missing receipt cannot hide a worker after an unknown Create response.
func (r *MigrationReconciler) nativeResolveNext(ctx context.Context, m *v1beta1.Migration, s *nativeWorkerState) error {
	next := &batchv1.Job{}
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: jobName(m, m.Status.Attempts+1)}, next)
	switch {
	case apierrors.IsNotFound(err):
		s.n = nextAbsent
		if m.Status.NativeWorkerIntent != "" {
			s.n = nextAbandoned
		}
	case err != nil:
		return err
	case !nativeEvidenceOwnedBy(next, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()):
		s.n, s.next = nextForeign, next
	case !next.DeletionTimestamp.IsZero():
		s.n, s.next = nextTerminating, next
	case nativeIntentAdmits(m, next):
		s.n, s.next = nextAdoptable, next
	default:
		s.n, s.next = nextUnadmitted, next
	}
	return nil
}

func (r *MigrationReconciler) nativeResolveWorker(ctx context.Context, m *v1beta1.Migration, s *nativeWorkerState) error {
	if m.Status.Attempts == 0 {
		return nil
	}
	worker := &batchv1.Job{}
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Status.JobName}, worker)
	switch {
	case apierrors.IsNotFound(err):
		s.w, s.worker = workerGone, nativeMissingWorker(m)
	case err != nil:
		return err
	case string(worker.UID) != m.Status.NativeJobUID:
		s.w, s.worker = workerForeign, worker
	case !worker.DeletionTimestamp.IsZero() || worker.Status.Terminating != nil && *worker.Status.Terminating != 0:
		s.w, s.worker = workerTerminating, worker
	case !nativeWorkerAdmitted(m, worker):
		s.w, s.worker = workerModified, worker
	default:
		s.w, s.worker = workerAdmitted, worker
	}
	return nil
}

func (r *MigrationReconciler) reconcileNativeStart(ctx context.Context, m, base *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	if _, err := nativeConfigCapabilities(m); err != nil {
		r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionFalse, "InvalidSpec", "Native configuration is not qualified")
		m.Status.Phase = v1beta1.PhaseFailed
		r.setCondition(m, v1beta1.ConditionFailed, metav1.ConditionTrue, "InvalidSpec", "Native configuration is not qualified")
		return ctrl.Result{}, r.updateNativeStatus(ctx, m, base)
	}
	if followEnabled(m) && !controllerutil.ContainsFinalizer(m, finalizerName) {
		if err := r.ensureFinalizer(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Nanosecond}, nil
	}
	_, passed, failure, err := r.ensureNativePreflight(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !passed {
		m.Status.Phase = v1beta1.PhaseValidating
		r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionUnknown, "NativePreflightPending", "Native preflight has not passed")
		if failure != "" {
			m.Status.Phase = v1beta1.PhaseFailed
			r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionFalse, reasonNativePreflightFailed, failure)
			r.setCondition(m, v1beta1.ConditionFailed, metav1.ConditionTrue, reasonNativePreflightFailed, failure)
			logf.FromContext(ctx).Info("native preflight failed", "reason", reasonNativePreflightFailed, "message", failure)
		}
		if err := r.updateNativeStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	if _, err := r.nativeReconcileCurrent(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
	if validated == nil || validated.Status != metav1.ConditionTrue || validated.ObservedGeneration != m.Generation {
		m.Status.Phase = v1beta1.PhaseValidating
		r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionTrue, "NativePreflightPassed", "Native configuration and active database preflight passed")
		if err := r.updateNativeStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Nanosecond}, nil
	}
	if pvc == nil {
		claim, err := buildNativeWorkClaim(m, r.Scheme)
		if err != nil || r.Create(ctx, claim) != nil || claim.UID == "" {
			return ctrl.Result{}, errNativeReconcile
		}
		if _, err := nativeWorkIdentity(m, claim); err != nil {
			return ctrl.Result{}, errNativeReconcile
		}
	}
	return r.startNativeAttempt(ctx, m, base)
}

func (r *MigrationReconciler) reconcileNativeRunning(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (result ctrl.Result, err error) {
	if m == nil || base == nil {
		return ctrl.Result{}, errNativeReconcile
	}
	key := client.ObjectKeyFromObject(m)
	defer func() {
		if err != nil {
			r.stopSampling(key)
		}
	}()
	if _, err := r.nativeReconcileCurrent(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	if m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || worker == nil || pvc == nil || !native.NativeReceiptsMatch(m, worker.UID, pvc.UID) {
		return ctrl.Result{}, errNativeReconcile
	}
	saved := followEnabled(m) && nativeControlEnd(m) != ""
	// Before the saved-E branch (lifecycle 11): admission needs a running worker pod, and the starting check reads no sample.
	if nativeWorkerStarting(worker) {
		if saved {
			r.stopSampling(key)
		}
		// The owned Job's status change wakes this pass.
		logf.FromContext(ctx).V(1).Info("native worker pod is not ready yet", "job", worker.Name)
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	// Saved E has its own fresh admission and must survive a stopped or unknown sampler.
	if saved {
		r.stopSampling(key)
		return r.reconcileNativeFollowCutover(ctx, m, worker, pvc, false)
	}
	got, _, sampleErr := r.observeNativeProgress(ctx, m, worker, pvc)
	if ctx.Err() != nil {
		return ctrl.Result{}, native.ErrNativeControlUnavailable
	}
	if sampleErr != nil {
		return ctrl.Result{}, sampleErr
	}
	if nativeSampleComplete(m, got) {
		r.applyNativeProgress(m, got)
	}
	// Recorded first, so the pass that cuts over also leaves CaughtUp=True behind.
	if err := r.updateNativeStatus(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	if followEnabled(m) && cutoverWanted(m, got.catchUpConfirmed) {
		r.stopSampling(key)
		return r.reconcileNativeFollowCutover(ctx, m, worker, pvc, got.catchUpConfirmed)
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

// reconcileNativeWorkerState applies the N table on every path, the W rows every path shares, and the
// retry and running columns of the W table; the suspend and deletion columns stay with disposition.
func (r *MigrationReconciler) reconcileNativeWorkerState(ctx context.Context, m, base *v1beta1.Migration, s nativeWorkerState) (bool, ctrl.Result, error) {
	if handled, result, err := r.reconcileNativeNext(ctx, m, base, s); handled {
		return handled, result, err
	}
	switch s.w {
	case workerForeign:
		return r.holdNative(ctx, m, base, v1beta1.ReasonForeignObject, fmt.Sprintf(
			"Job %s has UID %s, not the recorded native worker %s; the operator neither runs nor removes it", s.worker.Name, s.worker.UID, m.Status.NativeJobUID))
	case workerTerminating:
		return r.holdNative(ctx, m, base, v1beta1.ReasonWorkerPodRemains, fmt.Sprintf(
			"Native worker Job %s is being deleted or has terminating pods; the operator waits until they are gone", s.worker.Name))
	}
	if !m.DeletionTimestamp.IsZero() || m.Spec.Suspend {
		// The suspend and delete columns, a modified W and a lost V included, belong to disposition.
		return false, ctrl.Result{}, nil
	}
	if s.v == volumeLost {
		if m.Status.Attempts == 0 {
			return r.holdNative(ctx, m, base, v1beta1.ReasonForeignObject, fmt.Sprintf(
				"PVC %s is not a live work volume of this Migration (foreign, replaced or deleting); the operator neither uses nor removes it", workPVCName(m)))
		}
		return true, ctrl.Result{}, errNativeReconcile
	}
	return r.reconcileNativeLiveWorker(ctx, m, base, s)
}

// reconcileNativeNext is the N table, which every path applies before it looks at W (R2).
func (r *MigrationReconciler) reconcileNativeNext(ctx context.Context, m, base *v1beta1.Migration, s nativeWorkerState) (bool, ctrl.Result, error) {
	n := m.Status.Attempts
	switch s.n {
	case nextAbandoned:
		// The intent's Create never landed; admission runs again on a later pass, never in this one.
		desired := m.DeepCopy()
		desired.Status.NativeWorkerIntent = ""
		return r.patchNativeWorkerState(ctx, m, base, desired)
	case nextForeign:
		return r.holdNative(ctx, m, base, v1beta1.ReasonForeignObject, fmt.Sprintf(
			"Job %s is not controlled by this Migration; the operator neither runs nor removes it, and attempt %d waits until it is gone", s.next.Name, n+1))
	case nextTerminating:
		return r.holdNative(ctx, m, base, v1beta1.ReasonWorkerPodRemains, fmt.Sprintf(
			"Native worker Job %s is being deleted; the operator waits until it and its pods are gone", s.next.Name))
	case nextAdoptable, nextUnadmitted:
		// Only an engine-free W can hand its place to N; a receipt for N must never orphan a running W.
		if s.v != volumeBound || s.w != workerNone && s.w != workerGone && (s.w != workerAdmitted || !nativeWorkerEnded(s.worker)) {
			return r.holdNative(ctx, m, base, v1beta1.ReasonStatusIntegrity, fmt.Sprintf(
				"Native worker Job %s exists while attempt %d has not ended or its work PVC is not bound; the operator does not record it", s.next.Name, n))
		}
		if s.n == nextAdoptable {
			return r.adoptNativeAttempt(ctx, m, base, s.next, s.pvc)
		}
		return r.recordNativeUnadmittedAttempt(ctx, m, base, s.next, s.pvc)
	}
	return false, ctrl.Result{}, nil
}

// reconcileNativeLiveWorker is the W table's retry and running columns for a modified W, R7, and the
// Blocked clear of the first pass that proceeds.
func (r *MigrationReconciler) reconcileNativeLiveWorker(ctx context.Context, m, base *v1beta1.Migration, s nativeWorkerState) (bool, ctrl.Result, error) {
	if s.w == workerModified && !onFailurePath(m) {
		if !nativeWorkerEnded(s.worker) {
			return r.holdNative(ctx, m, base, v1beta1.ReasonWorkerModified, fmt.Sprintf(
				"Native worker Job %s no longer matches the Job admitted for attempt %d; restore it to let the attempt continue", s.worker.Name, m.Status.Attempts))
		}
		desired := m.DeepCopy()
		r.countNativeAttempt(desired, fmt.Sprintf("Native worker Job %s no longer matches its admission", s.worker.Name))
		desired.Status.ObservedGeneration = desired.Generation
		return r.patchNativeWorkerState(ctx, m, base, desired)
	}
	// R7: a counted attempt keeps no Job that is not its failed worker; the gate then follows a gone W.
	if onFailurePath(m) && (s.w == workerModified || s.w == workerAdmitted && !nativeRecoveryWorkerFailed(s.worker)) {
		done, err := r.dropNativeWorker(ctx, m, s.worker)
		if err != nil {
			return true, ctrl.Result{}, err
		}
		if !done {
			return true, ctrl.Result{RequeueAfter: pollInterval}, nil
		}
		return true, ctrl.Result{RequeueAfter: time.Nanosecond}, nil
	}
	// The finish path owns an EvidenceUnavailable hold of a Complete worker and clears it when it proceeds.
	if b := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionBlocked); b != nil && b.Status == metav1.ConditionTrue &&
		(b.Reason != v1beta1.ReasonEvidenceUnavailable || s.worker == nil || !nativeEvidenceWorkerComplete(s.worker)) {
		desired := m.DeepCopy()
		r.clearNativeBlocked(desired)
		return r.patchNativeWorkerState(ctx, m, base, desired)
	}
	return false, ctrl.Result{}, nil
}

func (r *MigrationReconciler) patchNativeWorkerState(ctx context.Context, m, base, desired *v1beta1.Migration) (bool, ctrl.Result, error) {
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return true, ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	return true, ctrl.Result{RequeueAfter: time.Nanosecond}, nil
}
