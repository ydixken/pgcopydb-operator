package controller

import (
	"context"
	"errors"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// buildNativeWorkClaim returns the native work PVC with the Migration as its controller.
func buildNativeWorkClaim(m *v1beta1.Migration, scheme *runtime.Scheme) (*corev1.PersistentVolumeClaim, error) {
	claim := buildWorkPVC(m)
	if err := controllerutil.SetControllerReference(m, claim, scheme); err != nil {
		return nil, err
	}
	return claim, nil
}

// nativeWorkClaimBound refuses a work PVC that is not this Migration's live, controlled claim.
func nativeWorkClaimBound(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) error {
	if pvc == nil || pvc.Namespace != m.Namespace || pvc.Name != workPVCName(m) || !pvc.DeletionTimestamp.IsZero() {
		return errors.New("native work PVC is absent, foreign, or deleting")
	}
	owner := metav1.GetControllerOf(pvc)
	if owner == nil || owner.UID != m.UID || owner.Name != m.Name || owner.Kind != "Migration" || owner.APIVersion != v1beta1.GroupVersion.String() {
		return errors.New("native work PVC is not controlled by this Migration")
	}
	return nil
}

// disposeNativeWorkClaim checks the recorded work PVC of a finalized Migration; it deletes nothing.
func (r *MigrationReconciler) disposeNativeWorkClaim(ctx context.Context, m *v1beta1.Migration) error {
	claim := &corev1.PersistentVolumeClaim{}
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: workPVCName(m)}, claim)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil || claim.UID != types.UID(m.Status.NativePVCUID) || claim.Name != workPVCName(m) || claim.Namespace != m.Namespace || len(claim.OwnerReferences) != 1 ||
		!nativeEvidenceOwnedBy(claim, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) || ctx.Err() != nil {
		return errNativeDisposition
	}
	// Migration owner-reference GC is the existing work-PVC deletion policy.
	return nil
}
