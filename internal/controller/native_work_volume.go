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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// buildNativeWorkClaim returns the native work PVC. A follow claim carries the Migration's UID label
// and no owner reference (R5): its finalizer path deletes it after cleanup, which reads the workdir.
func buildNativeWorkClaim(m *v1beta1.Migration, scheme *runtime.Scheme) (*corev1.PersistentVolumeClaim, error) {
	claim := buildWorkPVC(m)
	if followEnabled(m) {
		claim.Labels[native.WorkClaimMigrationUIDLabel] = string(m.UID)
		return claim, nil
	}
	if err := controllerutil.SetControllerReference(m, claim, scheme); err != nil {
		return nil, err
	}
	return claim, nil
}

// nativeWorkClaimBound refuses a work PVC that is not this Migration's live claim: a follow claim
// by its label before the receipt and also by nativePVCUID after it, a clone-only claim by its controller.
func nativeWorkClaimBound(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) error {
	if pvc == nil || pvc.Namespace != m.Namespace || pvc.Name != workPVCName(m) || !pvc.DeletionTimestamp.IsZero() {
		return errors.New("native work PVC is absent, foreign, or deleting")
	}
	if !native.WorkClaimBound(m, pvc) {
		return errors.New("native work PVC is not bound to this Migration")
	}
	if followEnabled(m) && m.Status.NativePVCUID != "" && string(pvc.UID) != m.Status.NativePVCUID {
		return errors.New("native work PVC is not the recorded claim")
	}
	return nil
}

// deleteNativeWorkClaim deletes this follow Migration's bound work PVC with UID and resourceVersion
// preconditions; the claim has no owner reference, so nothing else removes it. A gone claim needs nothing.
func (r *MigrationReconciler) deleteNativeWorkClaim(ctx context.Context, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) error {
	if pvc == nil || !followEnabled(m) || nativeWorkClaimBound(m, pvc) != nil || pvc.UID == "" {
		return errNativeDisposition
	}
	pre := client.Preconditions{UID: &pvc.UID}
	if pvc.ResourceVersion != "" {
		pre.ResourceVersion = &pvc.ResourceVersion
	}
	if err := r.Delete(ctx, pvc, pre); err != nil && !apierrors.IsNotFound(err) {
		return errNativeDisposition
	}
	return nil
}

// failNativeWorkVolumeLost is the V table's retry and finish cells past attempt 0 (decision di): W is
// stopped first, and only an engine-free W lets Failed=WorkVolumeLost be written (R1). A lost claim
// binds no TERM, so the stop is the drop, whose pod deletion sends SIGTERM.
func (r *MigrationReconciler) failNativeWorkVolumeLost(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job) (bool, ctrl.Result, error) {
	if r.Recorder == nil {
		return true, ctrl.Result{}, errNativeReconcile
	}
	if !nativeWorkerMissing(worker) && !nativeWorkerEnded(worker) {
		done, err := r.dropNativeWorker(ctx, m, worker)
		if err != nil {
			return true, ctrl.Result{}, err
		}
		if !done {
			return true, ctrl.Result{RequeueAfter: pollInterval}, nil
		}
		return true, ctrl.Result{RequeueAfter: time.Nanosecond}, nil
	}
	if _, err := r.nativeWorkerEngineFree(ctx, worker); err != nil {
		return r.holdNativeBusy(ctx, m, base, err)
	}
	msg := fmt.Sprintf("Work PVC %s is missing, deleting or replaced, so the native state of attempt %d is lost; delete the Migration, which releases it with CleanupFailed", workPVCName(m), m.Status.Attempts)
	desired := m.DeepCopy()
	desired.Status.ObservedGeneration = desired.Generation
	r.nativeCloneAttemptEnded(desired, reasonCloneFailed)
	r.fail(desired, v1beta1.ReasonWorkVolumeLost, "Fail", msg)
	r.setCondition(desired, v1beta1.ConditionComplete, metav1.ConditionFalse, v1beta1.ReasonWorkVolumeLost, "Native worker did not complete")
	r.clearNativeBlocked(desired)
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return true, ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	metrics.Record(desired)
	return true, ctrl.Result{}, nil
}

// disposeNativeWorkClaim deletes the recorded work PVC of a finalized follow Migration.
// A claim that this Migration still controls predates R5; owner-reference GC removes it.
func (r *MigrationReconciler) disposeNativeWorkClaim(ctx context.Context, m *v1beta1.Migration) error {
	claim := &corev1.PersistentVolumeClaim{}
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: workPVCName(m)}, claim)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil || claim.UID != types.UID(m.Status.NativePVCUID) || claim.Name != workPVCName(m) || claim.Namespace != m.Namespace || ctx.Err() != nil {
		return errNativeDisposition
	}
	if len(claim.OwnerReferences) == 1 && nativeEvidenceOwnedBy(claim, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
		return nil
	}
	if !claim.DeletionTimestamp.IsZero() && native.WorkClaimBound(m, claim) {
		return nil
	}
	return r.deleteNativeWorkClaim(ctx, m, claim)
}
