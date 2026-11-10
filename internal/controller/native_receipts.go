package controller

import (
	"context"
	"fmt"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Only a known successful Create may supply this Job; existing-name discovery grants no receipt.
func (r *MigrationReconciler) recordNativeAttempt(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job, pvc *corev1.PersistentVolumeClaim, announce bool, retry *nativeRetryAdmission) (ctrl.Result, error) {
	if r.Client == nil || r.APIReader == nil || r.Recorder == nil || pvc == nil || ctx.Err() != nil || !nativeAttemptRecordIdentity(m, base, job) {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	if _, err := nativeWorkIdentity(m, pvc); err != nil {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	if m.Status.Attempts == 0 {
		if m.Status.JobName != "" || m.Status.NativeJobUID != "" || m.Status.NativePVCUID != "" {
			return ctrl.Result{}, errNativeWorkerAdmission
		}
	} else if m.Status.JobName != jobName(m, m.Status.Attempts) || !native.NativeReceiptsMatch(m, types.UID(m.Status.NativeJobUID), pvc.UID) {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	if m.Status.Attempts > 0 {
		if retry == nil || !retry.used || retry.recorded {
			return ctrl.Result{}, errNativeWorkerRetry
		}
		if _, _, err := r.nativeRetryCurrent(ctx, m, retry); err != nil {
			return ctrl.Result{}, errNativeWorkerRetry
		}
		retry.recorded = true
	} else if retry != nil {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	desired := m.DeepCopy()
	if desired.Status.StartedAt == nil {
		now := metav1.Now()
		desired.Status.StartedAt = &now
	}
	desired.Status.Attempts++
	desired.Status.JobName = job.Name
	desired.Status.NativeJobUID, desired.Status.NativePVCUID = string(job.UID), string(pvc.UID)
	desired.Status.NativeFinalizedJobUID = ""
	if !native.NativeReceiptsMatch(desired, job.UID, pvc.UID) {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	desired.Status.Phase = attemptPhase(desired)
	if !meta.IsStatusConditionTrue(desired.Status.Conditions, v1beta1.ConditionCloneCompleted) {
		r.setCondition(desired, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, reasonCloneRunning,
			fmt.Sprintf("attempt %d running as Job %s", desired.Status.Attempts, job.Name))
	}
	if desired.Status.Attempts > 1 {
		desired.Status.NativeSchemaJobUID, desired.Status.NativeDataJobUID = "", ""
		desired.Status.Verification, desired.Status.VerificationProgress, desired.Status.CompletedAt = nil, nil, nil
		desired.Status.RetryGate = nil
		r.setCondition(desired, v1beta1.ConditionRetryHeld, metav1.ConditionFalse, reasonGatePassed, "Native retry admission passed")
		r.setCondition(desired, v1beta1.ConditionFailed, metav1.ConditionFalse, "NativeRetryStarted", "Native retry is running")
		r.setCondition(desired, v1beta1.ConditionComplete, metav1.ConditionFalse, "NativeRetryStarted", "Native retry awaits completion proof")
	}
	desired.Status.ObservedGeneration = desired.Generation
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	metrics.Record(desired)
	if announce {
		r.Recorder.Eventf(desired, nil, corev1.EventTypeNormal, "AttemptStarted", "StartAttempt", "attempt %d as Job %s", desired.Status.Attempts, job.Name)
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

func nativeAttemptRecordIdentity(m, base *v1beta1.Migration, job *batchv1.Job) bool {
	return m != nil && base != nil && job != nil && m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate &&
		m.UID != "" && m.UID == base.UID && m.Name == base.Name && m.Namespace == base.Namespace &&
		m.Generation > 0 && m.Generation == base.Generation && m.ResourceVersion != "" && m.ResourceVersion == base.ResourceVersion &&
		apiequality.Semantic.DeepEqual(m.Spec, base.Spec) && apiequality.Semantic.DeepEqual(m.Status, base.Status) &&
		m.Status.Attempts >= 0 && m.Status.Attempts < int32(1<<31-1) && m.DeletionTimestamp.IsZero() && !m.Spec.Suspend &&
		job.Name == jobName(m, m.Status.Attempts+1) && job.Namespace == m.Namespace && job.DeletionTimestamp.IsZero() &&
		nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String())
}
