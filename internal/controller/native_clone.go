package controller

import (
	"context"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

func (r *MigrationReconciler) finishNativeClone(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	if m == nil || base == nil || followEnabled(m) || !nativeControlStatusIdentity(m, base) {
		return ctrl.Result{}, errNativeCloneReport
	}
	entry := m.DeepCopy()
	original, err := r.nativeQuiescentWorker(ctx, entry, worker, pvc)
	if err != nil {
		return ctrl.Result{}, errNativeCloneReport
	}
	if _, err := r.readNativeCloneReport(ctx, entry, worker, pvc); err != nil {
		return ctrl.Result{}, err
	}
	done, err := r.ensureNativeVerification(ctx, m, worker, pvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if m.ResourceVersion != entry.ResourceVersion {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	if !r.nativeVerificationOriginalUnchanged(ctx, entry, worker, pvc, original) {
		return ctrl.Result{}, errNativeCloneReport
	}
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "CloneSucceeded", "Native clone finished")
	if !done {
		if err := r.updateNativeStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	now := metav1.NewTime(r.currentTime())
	m.Status.Phase, m.Status.CompletedAt = v1beta1.PhaseCompleted, &now
	r.setCondition(m, v1beta1.ConditionComplete, metav1.ConditionTrue, "MigrationSucceeded", "Native clone finished")
	if err := r.updateNativeStatus(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Completed", "Complete", "Native clone finished")
	}
	return ctrl.Result{}, nil
}
