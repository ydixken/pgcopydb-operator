package controller

import (
	"context"
	"errors"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
