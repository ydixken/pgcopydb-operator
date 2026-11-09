package controller

import (
	"context"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The caller supplies fresh catch-up eligibility and user cutover policy; this method only freezes E.
func (r *MigrationReconciler) persistNativeEnd(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, control *native.Client) (ctrl.Result, error) {
	if r.Client == nil || control == nil || m == nil || !followEnabled(m) || ctx.Err() != nil {
		return ctrl.Result{}, errNativeControlAdmission
	}
	facts, err := r.admitNativeControlFacts(ctx, m, worker, pvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if facts.migration.ResourceVersion != m.ResourceVersion {
		return ctrl.Result{}, nativeControlConflict()
	}
	if e := nativeControlEnd(facts.migration); e != "" {
		if !nativeControlValidEnd(e) {
			return ctrl.Result{}, errNativeControlAdmission
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	end, err := control.CurrentEndLSN(ctx, facts.migration, facts.data)
	if err != nil || !nativeControlValidEnd(end) || ctx.Err() != nil {
		return ctrl.Result{}, native.ErrNativeControlUnavailable
	}
	after, err := r.nativeControlFacts(ctx, m, worker, pvc, facts.data.OperationID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !sameNativeControlFacts(facts, after) || after.migration.ResourceVersion != facts.migration.ResourceVersion {
		return ctrl.Result{}, nativeControlConflict()
	}
	desired, base := after.migration.DeepCopy(), after.migration.DeepCopy()
	if desired.Status.Replication == nil {
		desired.Status.Replication = &v1beta1.ReplicationStatus{}
	}
	desired.Status.Replication.Endpos = end
	if err := r.patchNativeControlStatus(ctx, desired, base); err != nil {
		return ctrl.Result{}, err
	}
	// Returning leaves the caller's pre-E snapshot untouched, including after an unknown patch reply.
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

func (r *MigrationReconciler) setNativePersistedEnd(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, control *native.Client) ([]byte, error) {
	if control == nil || m == nil || !followEnabled(m) {
		return nil, errNativeControlAdmission
	}
	before, end, err := r.nativePersistedEndFacts(ctx, m, worker, pvc)
	if err != nil {
		return nil, err
	}
	if before.migration.ResourceVersion != m.ResourceVersion {
		return nil, nativeControlConflict()
	}
	raw, callErr := control.SetEndpos(ctx, before.data, end)
	after, sameEnd, readErr := r.nativePersistedEndFacts(ctx, m, worker, pvc)
	after.data.OperationID = before.data.OperationID
	if readErr != nil || end != sameEnd || !sameNativeControlFacts(before, after) || after.migration.ResourceVersion != before.migration.ResourceVersion || ctx.Err() != nil {
		return nil, native.ErrNativeSetEndposUnknownEffect
	}
	if callErr != nil {
		return nil, callErr
	}
	// Checked raw bytes remain data; no report, ACK, condition or checkpoint is constructed here.
	return raw, nil
}

func (r *MigrationReconciler) updateNativeStatus(ctx context.Context, m, base *v1beta1.Migration) error {
	if r.Client == nil || r.APIReader == nil || m == nil || base == nil || ctx.Err() != nil || !nativeControlStatusIdentity(m, base) {
		return errNativeControlAdmission
	}
	live := &v1beta1.Migration{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(base), live) != nil || !nativeControlStatusIdentity(live, base) {
		return errNativeControlAdmission
	}
	if live.ResourceVersion != base.ResourceVersion || live.Status.Attempts != base.Status.Attempts || live.Status.JobName != base.Status.JobName || nativeControlEnd(live) != nativeControlEnd(base) {
		return nativeControlConflict()
	}
	end := nativeControlEnd(live)
	if end != "" && !nativeControlValidEnd(end) {
		return errNativeControlAdmission
	}
	desired := m.DeepCopy()
	if desired.Status.Replication != nil {
		desired.Status.Replication.Endpos = end
	} else if end != "" {
		desired.Status.Replication = &v1beta1.ReplicationStatus{Endpos: end}
	}
	return r.patchNativeControlStatus(ctx, desired, live)
}

func nativeControlStatusIdentity(a, b *v1beta1.Migration) bool {
	return a.UID != "" && a.UID == b.UID && a.Name == b.Name && a.Namespace == b.Namespace && a.Generation > 0 && a.Generation == b.Generation && a.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && apiequality.Semantic.DeepEqual(a.Spec, b.Spec)
}

func (r *MigrationReconciler) patchNativeControlStatus(ctx context.Context, m, base *v1beta1.Migration) error {
	if ctx.Err() != nil {
		return native.ErrNativeControlUnavailable
	}
	m.Status.ObservedGeneration = m.Generation
	if err := r.Status().Patch(ctx, m, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsConflict(err) {
			return nativeControlConflict()
		}
		return native.ErrNativeControlUnavailable
	}
	metrics.Record(m)
	return nil
}

func nativeControlConflict() error {
	return apierrors.NewConflict(schema.GroupResource{Group: v1beta1.GroupVersion.Group, Resource: "migrations"}, "", native.ErrNativeControlUnavailable)
}
