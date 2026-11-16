package controller

import (
	"context"
	"fmt"
	"time"

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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Only a known successful Create, or adoption under the recorded intent, may supply this Job.
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
	// The receipt names the Job the intent admitted: its nonce annotation and its digest.
	if !nativeIntentAdmits(m, job) {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	_, _, digest, _ := nativeIntentParts(m.Status.NativeWorkerIntent)
	desired := m.DeepCopy()
	if !r.applyNativeAttemptReceipt(desired, job, pvc, digest) {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
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

// applyNativeAttemptReceipt records job as the next attempt with its admission digest and clears the intent.
func (r *MigrationReconciler) applyNativeAttemptReceipt(desired *v1beta1.Migration, job *batchv1.Job, pvc *corev1.PersistentVolumeClaim, digest string) bool {
	if desired.Status.StartedAt == nil {
		now := metav1.Now()
		desired.Status.StartedAt = &now
	}
	desired.Status.Attempts++
	desired.Status.JobName = job.Name
	desired.Status.NativeJobUID, desired.Status.NativePVCUID = string(job.UID), string(pvc.UID)
	desired.Status.NativeJobSHA256, desired.Status.NativeWorkerIntent = digest, ""
	desired.Status.NativeFinalizedJobUID, desired.Status.NativeReportJobUID = "", ""
	if !native.NativeReceiptsMatch(desired, job.UID, pvc.UID) || len(digest) != 64 {
		return false
	}
	if desired.Status.Attempts > 1 {
		// No incomplete sample clears the last attempt's measurements, so the new attempt does.
		// Streaming stays because copyDoneForGate reads it.
		desired.Status.Progress = nil
		if rs := desired.Status.Replication; rs != nil {
			rs.WriteLSN, rs.ReplayLSN, rs.LagBytes = "", "", nil
		}
		meta.RemoveStatusCondition(&desired.Status.Conditions, v1beta1.ConditionCaughtUp)
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
	return true
}

// nativeNextReceiptIdentity admits N for a receipt that no Create in this pass supplied.
func nativeNextReceiptIdentity(m *v1beta1.Migration, job *batchv1.Job, pvc *corev1.PersistentVolumeClaim) bool {
	if m == nil || job == nil || pvc == nil || job.UID == "" || m.Status.Attempts < 0 || m.Status.Attempts >= int32(1<<31-2) || job.Name != jobName(m, m.Status.Attempts+1) ||
		job.Namespace != m.Namespace || !job.DeletionTimestamp.IsZero() || !nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
		return false
	}
	if _, err := nativeWorkIdentity(m, pvc); err != nil {
		return false
	}
	return m.Status.Attempts > 0 || m.Status.JobName == "" && m.Status.NativeJobUID == "" && m.Status.NativePVCUID == ""
}

// adoptNativeAttempt records the worker whose Create landed but whose receipt was lost (q4).
func (r *MigrationReconciler) adoptNativeAttempt(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, ctrl.Result, error) {
	_, _, digest, ok := nativeIntentParts(m.Status.NativeWorkerIntent)
	if r.Recorder == nil || !ok || !nativeIntentAdmits(m, job) || !nativeNextReceiptIdentity(m, job, pvc) {
		return true, ctrl.Result{}, errNativeWorkerAdmission
	}
	desired := m.DeepCopy()
	if !r.applyNativeAttemptReceipt(desired, job, pvc, digest) {
		return true, ctrl.Result{}, errNativeWorkerAdmission
	}
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return true, ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	metrics.Record(desired)
	r.Recorder.Eventf(desired, nil, corev1.EventTypeNormal, "AttemptStarted", "StartAttempt", "attempt %d as Job %s, adopted under its recorded intent after the receipt was lost", desired.Status.Attempts, job.Name)
	return true, ctrl.Result{RequeueAfter: time.Nanosecond}, nil
}

// recordNativeUnadmittedAttempt records an owned next-name Job no intent admitted as a failed, counted
// attempt, so its UID stays under R1 while R7 drops it.
func (r *MigrationReconciler) recordNativeUnadmittedAttempt(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, ctrl.Result, error) {
	digest, err := nativeJobSHA256(job)
	if err != nil || !nativeNextReceiptIdentity(m, job, pvc) {
		return r.holdNative(ctx, m, base, v1beta1.ReasonForeignObject, fmt.Sprintf(
			"Native worker Job %s was not created under the recorded intent and cannot be recorded; the operator neither runs nor removes it", job.Name))
	}
	desired := m.DeepCopy()
	if !r.applyNativeAttemptReceipt(desired, job, pvc, digest) {
		return true, ctrl.Result{}, errNativeWorkerAdmission
	}
	r.countNativeAttempt(desired, fmt.Sprintf("Native worker Job %s was not created under the recorded intent", job.Name))
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return true, ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	metrics.Record(desired)
	logf.FromContext(ctx).Info("native worker Job without a matching intent recorded as a counted attempt", "job", job.Name, "attempt", desired.Status.Attempts)
	return true, ctrl.Result{RequeueAfter: time.Nanosecond}, nil
}
