package controller

import (
	"context"
	"slices"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

func (r *MigrationReconciler) finishNativeFollow(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	if m == nil || base == nil || worker == nil || pvc == nil || !followEnabled(m) || !nativeControlStatusIdentity(m, base) {
		return ctrl.Result{}, errNativeFollowCompletion
	}
	entry := m.DeepCopy()
	_, original, err := r.nativeFollowTerminalCurrent(ctx, entry, worker, pvc, nil)
	if err != nil {
		return ctrl.Result{}, errNativeFollowCompletion
	}
	clone, err := r.readNativeCloneReport(ctx, entry, worker, pvc)
	if err != nil || !r.nativeVerificationOriginalUnchanged(ctx, entry, worker, pvc, original) {
		return ctrl.Result{}, errNativeFollowCompletion
	}
	observation, cleaned, err := r.readNativeFinalizationObservation(ctx, entry, worker, pvc, clone)
	if err != nil {
		return ctrl.Result{}, err
	}
	if observation == nil || !r.nativeVerificationOriginalUnchanged(ctx, entry, worker, pvc, original) {
		return ctrl.Result{}, errNativeFollowCompletion
	}
	done, err := r.ensureNativeVerification(ctx, m, worker, pvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if m.ResourceVersion != entry.ResourceVersion {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	if !r.nativeVerificationOriginalUnchanged(ctx, entry, worker, pvc, original) {
		return ctrl.Result{}, errNativeFollowCompletion
	}
	if !done {
		if err := r.updateNativeStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	if !cleaned {
		done, err := r.ensureNativeCleanup(ctx, entry, worker, pvc)
		if err != nil || !done {
			return ctrl.Result{}, errNativeCleanupFlow
		}
	}
	final, err := r.readNativeCleanedCompletedEvidence(ctx, entry, worker, pvc)
	if err != nil || final == nil || !nativeFollowCompletionUnchanged(observation, final) || !r.nativeVerificationOriginalUnchanged(ctx, entry, worker, pvc, original) {
		return ctrl.Result{}, errNativeFollowCompletion
	}
	now := metav1.NewTime(r.currentTime())
	m.Status.Phase, m.Status.CompletedAt = v1beta1.PhaseCompleted, &now
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "CloneSucceeded", "Native clone finished")
	r.setCondition(m, v1beta1.ConditionCutoverComplete, metav1.ConditionTrue, "NativeDrainConfirmed", "Native target durably crossed the saved cutover boundary")
	r.setCondition(m, v1beta1.ConditionFailed, metav1.ConditionFalse, "MigrationSucceeded", "Native follow finished")
	r.setCondition(m, v1beta1.ConditionComplete, metav1.ConditionTrue, "MigrationSucceeded", "Native follow finished")
	m.Status.NativeFinalizedJobUID = string(worker.UID)
	if err := r.updateNativeStatus(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.disposeNativeFinalizedVerificationHelpers(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Completed", "Complete", "Native follow finished")
	}
	return ctrl.Result{}, nil
}

func (r *MigrationReconciler) readNativeFinalizationObservation(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, clone *native.Report) (*native.CompletedObservation, bool, error) {
	observation, closed, err := r.captureNativeCleanedCompletedEvidence(ctx, m, worker, pvc)
	if err == nil {
		return observation, true, nil
	}
	if !closed {
		return nil, false, errNativeFollowCompletion
	}

	observation, closed, err = r.captureNativeCleanupRejoinCompletion(ctx, m, worker, pvc)
	if err == nil {
		return observation, false, nil
	}
	if !closed {
		return nil, false, errNativeFollowCompletion
	}
	observation, err = r.readNativeFollowTargetCompletion(ctx, m, worker, pvc, clone)
	return observation, false, err
}

func nativeFollowCompletionUnchanged(before, after *native.CompletedObservation) bool {
	return before != nil && after != nil && before.Checkpoint().Evidence() == after.Checkpoint().Evidence() && before.OwnedState().Work() == after.OwnedState().Work() && slices.Equal(before.OwnedState().Resources(), after.OwnedState().Resources())
}
