package controller

import (
	"context"
	"errors"
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var (
	errNativeReconcile                   = errors.New("native reconciliation identity or execution unavailable")
	errNativeFinalizationReceiptConflict = errors.New("NativeFinalizationReceiptConflict")
)

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
	worker, pvc, err := r.nativeReconcileResources(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if handled, result, err := r.reconcileNativeDisposition(ctx, m, base, worker, pvc); handled || err != nil {
		return result, err
	}
	if !m.DeletionTimestamp.IsZero() || m.Spec.Suspend {
		return ctrl.Result{}, errNativeReconcile
	}
	if m.Status.Attempts == 0 {
		if worker != nil || m.Status.JobName != "" || m.Status.NativeJobUID != "" || m.Status.NativePVCUID != "" || m.Status.NativeSchemaJobUID != "" || m.Status.NativeDataJobUID != "" || nativeControlEnd(m) != "" {
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
			m.Status.Phase = v1beta1.PhasePending
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

func (r *MigrationReconciler) nativeReconcileResources(ctx context.Context, m *v1beta1.Migration) (*batchv1.Job, *corev1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if m.Status.Attempts < 0 || m.Status.Attempts == int32(1<<31-1) && m.Status.JobName == "" {
		return nil, nil, errNativeReconcile
	}
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: workPVCName(m)}, pvc)
	if apierrors.IsNotFound(err) {
		pvc = nil
	} else if err != nil {
		return nil, nil, errNativeReconcile
	} else if _, err := nativeWorkIdentity(m, pvc); err != nil {
		return nil, nil, errNativeReconcile
	}
	name := m.Status.JobName
	if name == "" {
		// A missing receipt cannot hide the canonical worker after an unknown Create response.
		name = jobName(m, m.Status.Attempts+1)
	}
	worker := &batchv1.Job{}
	err = r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: name}, worker)
	if apierrors.IsNotFound(err) {
		worker = nil
	} else if err != nil {
		return nil, nil, errNativeReconcile
	}
	if ctx.Err() != nil || m.Status.Attempts > 0 && (worker == nil || pvc == nil || m.Status.JobName != jobName(m, m.Status.Attempts) || !native.NativeReceiptsMatch(m, worker.UID, pvc.UID)) {
		return nil, nil, errNativeReconcile
	}
	if m.Status.Attempts > 0 {
		expected, err := buildNativeWorkerJob(m, pvc, r.RPGMigrateRunnerImage, m.Status.Attempts)
		if err != nil || controllerutil.SetControllerReference(m, expected, r.Scheme) != nil || !nativePreflightJobMatches(expected, worker) {
			return nil, nil, errNativeReconcile
		}
	}
	return worker, pvc, nil
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
			r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionFalse, "NativePreflightFailed", failure)
			r.setCondition(m, v1beta1.ConditionFailed, metav1.ConditionTrue, "NativePreflightFailed", failure)
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
		claim := buildWorkPVC(m)
		if controllerutil.SetControllerReference(m, claim, r.Scheme) != nil || r.Create(ctx, claim) != nil || claim.UID == "" {
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
	// Saved E has its own fresh admission and must survive a stopped or unknown sampler.
	if followEnabled(m) && nativeControlEnd(m) != "" {
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
	if followEnabled(m) && cutoverWanted(m, got.catchUpConfirmed) {
		r.stopSampling(key)
		return r.reconcileNativeFollowCutover(ctx, m, worker, pvc, got.catchUpConfirmed)
	}
	r.applyNativeProgress(m, got)
	if err := r.updateNativeStatus(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}
