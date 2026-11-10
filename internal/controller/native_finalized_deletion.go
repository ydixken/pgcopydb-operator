package controller

import (
	"context"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *MigrationReconciler) nativeFinalizedDeletionCurrent(ctx context.Context, expected *v1beta1.Migration) (*v1beta1.Migration, error) {
	if r.Client == nil || r.APIReader == nil || expected == nil || expected.ResourceVersion == "" || expected.DeletionTimestamp.IsZero() || !nativeFinalizationRecorded(expected) || ctx.Err() != nil {
		return nil, errNativeDisposition
	}
	live := &v1beta1.Migration{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), live) != nil || live.UID != expected.UID || live.Name != expected.Name || live.Namespace != expected.Namespace ||
		live.ResourceVersion != expected.ResourceVersion || !nativeFinalizationRecorded(live) || !apiequality.Semantic.DeepEqual(live.Status, expected.Status) ||
		!apiequality.Semantic.DeepEqual(live.DeletionTimestamp, expected.DeletionTimestamp) || ctx.Err() != nil {
		return nil, errNativeDisposition
	}
	return live, nil
}

func (r *MigrationReconciler) reconcileNativeFinalizedDeletion(ctx context.Context, m *v1beta1.Migration) (ctrl.Result, error) {
	cleanup, cancel := context.WithTimeout(ctx, nativePreflightCleanupReserve)
	defer cancel()
	live, err := r.nativeFinalizedDeletionCurrent(cleanup, m)
	if err != nil || r.disposeNativeFinalizedVerificationHelpers(cleanup, live) != nil {
		return r.nativeFinalizedDeletionPending(m)
	}
	if r.disposeNativeFinalizedWorker(cleanup, live) != nil || r.nativeFinalizedDeletionPVC(cleanup, live) != nil {
		return r.nativeFinalizedDeletionPending(m)
	}
	live, err = r.nativeFinalizedDeletionCurrent(cleanup, m)
	if err != nil {
		return r.nativeFinalizedDeletionPending(m)
	}
	_, result, err := r.finishNativeDeletion(cleanup, live)
	if err != nil {
		return r.nativeFinalizedDeletionPending(m)
	}
	live.DeepCopyInto(m)
	return result, nil
}

func (r *MigrationReconciler) nativeFinalizedDeletionPending(m *v1beta1.Migration) (ctrl.Result, error) {
	if r.Recorder != nil {
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "NativeFinalizedCleanupPending", "Cleanup", "Recorded native resource cleanup is pending")
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

func (r *MigrationReconciler) disposeNativeFinalizedWorker(ctx context.Context, m *v1beta1.Migration) error {
	if _, err := r.nativeFinalizedDeletionCurrent(ctx, m); err != nil {
		return errNativeDisposition
	}
	known := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: m.Status.JobName, Namespace: m.Namespace, UID: types.UID(m.Status.NativeJobUID)}}
	current := &batchv1.Job{}
	err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(known), current)
	if err == nil {
		if current.UID != known.UID || current.Name != known.Name || current.Namespace != known.Namespace || current.ResourceVersion == "" || len(current.OwnerReferences) != 1 ||
			!nativeEvidenceOwnedBy(current, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) || !nativeEvidenceWorkerComplete(current) ||
			current.Status.Terminating != nil && *current.Status.Terminating != 0 {
			return errNativeDisposition
		}
		if _, err := r.nativeFinalizedDeletionCurrent(ctx, m); err != nil {
			return errNativeDisposition
		}
		if r.Delete(ctx, current, client.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion}, client.PropagationPolicy(metav1.DeletePropagationForeground)) != nil {
			return errNativeDisposition
		}
	} else if !apierrors.IsNotFound(err) {
		return errNativeDisposition
	}
	if r.nativePreflightCleanup(ctx, m, known, nil) != nil {
		return errNativeDisposition
	}
	_, err = r.nativeFinalizedDeletionCurrent(ctx, m)
	return err
}

func (r *MigrationReconciler) nativeFinalizedDeletionPVC(ctx context.Context, m *v1beta1.Migration) error {
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
