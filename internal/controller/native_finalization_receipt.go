package controller

import (
	"context"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// This records workflow history, never target replication or comparison authority.
func nativeFinalizationRecorded(m *v1beta1.Migration) bool {
	return m != nil && m.UID != "" && m.Generation > 0 && m.Status.Attempts > 0 &&
		m.Status.JobName == jobName(m, m.Status.Attempts) &&
		native.NativeReceiptsMatch(m, types.UID(m.Status.NativeJobUID), types.UID(m.Status.NativePVCUID)) &&
		m.Status.NativeFinalizedJobUID == m.Status.NativeJobUID && m.Status.Phase == v1beta1.PhaseCompleted &&
		meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete) &&
		meta.IsStatusConditionFalse(m.Status.Conditions, v1beta1.ConditionFailed)
}

func nativeFinalizationStatusTransition(desired, base *v1beta1.Migration) bool {
	if desired == nil || base == nil {
		return false
	}
	if desired.Status.NativeFinalizedJobUID == base.Status.NativeFinalizedJobUID {
		return true
	}
	return base.Status.NativeFinalizedJobUID == "" && nativeFinalizationRecorded(desired)
}

func (r *MigrationReconciler) nativeFinalizedCurrent(ctx context.Context, expected *v1beta1.Migration) (*v1beta1.Migration, error) {
	if r.Client == nil || r.APIReader == nil || expected == nil || expected.ResourceVersion == "" || ctx.Err() != nil || !nativeFinalizationRecorded(expected) {
		return nil, errNativeCleanupFlow
	}
	live := &v1beta1.Migration{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), live) != nil || live.Name != expected.Name || live.Namespace != expected.Namespace || live.UID != expected.UID || live.ResourceVersion != expected.ResourceVersion || !nativeFinalizationRecorded(live) || !apiequality.Semantic.DeepEqual(live.Status, expected.Status) {
		return nil, errNativeCleanupFlow
	}
	return live, nil
}

func (r *MigrationReconciler) disposeNativeFinalizedVerificationHelpers(ctx context.Context, m *v1beta1.Migration) error {
	cleanup, cancel := context.WithTimeout(ctx, nativePreflightCleanupReserve)
	defer cancel()
	live, err := r.nativeFinalizedCurrent(cleanup, m)
	if err != nil {
		return err
	}
	for _, check := range []string{compareSchema, compareData} {
		pin := nativeVerificationJobUID(live, check)
		if pin == "" {
			continue
		}
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: compareJobName(live, check), Namespace: live.Namespace, UID: types.UID(pin)}}
		current := &batchv1.Job{}
		if err := r.APIReader.Get(cleanup, client.ObjectKeyFromObject(job), current); err == nil {
			if current.Name != job.Name || current.Namespace != job.Namespace || current.UID != job.UID || current.ResourceVersion == "" || !nativeEvidenceOwnedBy(current, "Migration", live.Name, live.UID, v1beta1.GroupVersion.String()) || !nativeEvidenceWorkerComplete(current) {
				return errNativeCleanupFlow
			}
			job = current
		} else if !apierrors.IsNotFound(err) {
			return errNativeCleanupFlow
		}
		if _, err := r.nativeFinalizedCurrent(cleanup, m); err != nil {
			return err
		}
		if r.nativePreflightCleanup(cleanup, live, job, nil) != nil {
			return errNativeCleanupFlow
		}
		if _, err := r.nativeFinalizedCurrent(cleanup, m); err != nil {
			return err
		}
	}
	_, err = r.nativeFinalizedCurrent(cleanup, m)
	return err
}
