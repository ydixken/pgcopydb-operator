package controller

import (
	"context"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type nativeVerificationCapture struct {
	expected, job *batchv1.Job
	pod           *corev1.Pod
	binding       podexec.NativeContainerBinding
	result        v1beta1.VerificationResult
	progress      native.ComparisonOutcome
}

// A changed ResourceVersion means a new helper receipt was persisted; the caller must return immediately.
func (r *MigrationReconciler) ensureNativeVerification(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	// Discovery needs its cleanup reservation before the bounded comparison capture.
	ctx, cancel := context.WithTimeout(ctx, nativePreflightTotal+30*time.Second)
	defer cancel()
	if m == nil || m.Spec.Verification != nil && m.Spec.Verification.Rehearsal {
		return false, native.ErrNativeComparisonUnavailable
	}
	if !verificationRequested(m) {
		return true, nil
	}
	if r.Client == nil || r.APIReader == nil || r.NativeJobs == nil || worker == nil || pvc == nil || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || !nativeEvidenceWorkerComplete(worker) || ctx.Err() != nil {
		return false, native.ErrNativeComparisonUnavailable
	}
	entry := m.DeepCopy()
	_, original, err := r.nativeCompletionCurrent(ctx, entry, worker, pvc, nil)
	if err != nil {
		return false, native.ErrNativeComparisonUnavailable
	}
	captures := make([]*nativeVerificationCapture, 0, len(enabledChecks(entry)))
	for _, check := range enabledChecks(entry) {
		capture, created, err := r.readNativeVerificationCheck(ctx, m, worker, pvc, original, check)
		if err != nil || created {
			return false, err
		}
		if capture == nil {
			m.Status.Phase = v1beta1.PhaseVerifying
			r.setCondition(m, v1beta1.ConditionVerified, metav1.ConditionUnknown, "VerificationRunning", "Native comparison is running")
			return false, nil
		}
		captures = append(captures, capture)
	}
	if !r.nativeVerificationOriginalUnchanged(ctx, entry, worker, pvc, original) {
		return false, native.ErrNativeComparisonUnavailable
	}
	results := make([]v1beta1.VerificationResult, 0, len(captures))
	mismatch := ""
	for _, capture := range captures {
		if !r.nativeVerificationCaptureUnchanged(ctx, entry, capture) {
			return false, native.ErrNativeComparisonUnavailable
		}
		results = append(results, capture.result)
		if !capture.result.Passed && mismatch == "" {
			mismatch = capture.result.Check
		}
	}
	if !r.nativeVerificationOriginalUnchanged(ctx, entry, worker, pvc, original) || ctx.Err() != nil {
		return false, native.ErrNativeComparisonUnavailable
	}
	m.Status.Phase, m.Status.Verification = v1beta1.PhaseVerifying, results
	for _, capture := range captures {
		r.recordNativeCompareProgress(m, capture)
	}
	if mismatch == "" {
		r.setCondition(m, v1beta1.ConditionVerified, metav1.ConditionTrue, "ComparePassed", "Native comparison found source and target matching")
	} else {
		reason := "DataMismatch"
		if mismatch == compareSchema {
			reason = "SchemaMismatch"
		}
		r.setCondition(m, v1beta1.ConditionVerified, metav1.ConditionFalse, reason, "Native comparison reported differences")
		if r.Recorder != nil {
			r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "VerificationMismatch", "Verify", "Native comparison reported differences")
		}
	}
	return true, nil
}

func nativeVerificationJobUID(m *v1beta1.Migration, check string) string {
	if check == compareSchema {
		return m.Status.NativeSchemaJobUID
	}
	return m.Status.NativeDataJobUID
}

func (r *MigrationReconciler) nativeVerificationOriginalUnchanged(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) bool {
	pod, err := r.nativeQuiescentWorker(ctx, m, worker, pvc)
	return err == nil && ctx.Err() == nil && nativePreflightSamePod(original, pod) && apiequality.Semantic.DeepEqual(original.Spec, pod.Spec) && apiequality.Semantic.DeepEqual(original.Status, pod.Status)
}

func (r *MigrationReconciler) readNativeVerificationCheck(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, check string) (*nativeVerificationCapture, bool, error) {
	expected, err := buildNativeCompareJob(m, pvc, worker, r.RPGMigrateRunnerImage, check)
	if err != nil || controllerutil.SetControllerReference(m, expected, r.Scheme) != nil || native.ValidateEvidenceJobSize(expected) != nil {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	job := &batchv1.Job{}
	err = r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), job)
	if apierrors.IsNotFound(err) && nativeVerificationJobUID(m, check) == "" {
		if err := r.createNativeVerificationCheck(ctx, m, worker, pvc, original, expected, check); err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	if err != nil || nativeVerificationJobUID(m, check) == "" || string(job.UID) != nativeVerificationJobUID(m, check) || job.UID == worker.UID || !nativePreflightJobMatches(expected, job) || nativePreflightJobFailed(job) {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	pod, err := r.nativePreflightPod(ctx, job)
	if err != nil || pod != nil && !nativePreflightPodMatches(job, pod) {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	if !nativeEvidenceWorkerComplete(job) {
		if !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
			return nil, false, native.ErrNativeComparisonUnavailable
		}
		return nil, false, nil
	}
	if pod == nil {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	ready, err := nativePreflightContainerReady(pod, runnerImageFor(m, r.RPGMigrateRunnerImage))
	if err != nil || !ready || pod.Status.Phase != corev1.PodSucceeded {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	if !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	status := pod.Status.ContainerStatuses[0]
	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: workerContainer, ContainerID: status.ContainerID, ImageID: status.ImageID, TemplateSHA256: podexec.NativeTemplateSHA256(job.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(job.Spec.Template)}
	output, err := r.NativeJobs.JobComparisonNative(ctx, m.Namespace, job.UID, binding)
	if err != nil {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	op, err := nativeComparisonOperationID(m, pvc, worker, work, check)
	if err != nil {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	outcome, err := native.ParseComparisonOutput(output, work, op, check, native.ComparisonProgramSHA256())
	matched, known := outcome.Result()
	if err != nil || !known {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	capture := &nativeVerificationCapture{expected: expected, job: job, pod: pod, binding: binding, progress: outcome, result: v1beta1.VerificationResult{Check: check, Passed: matched, StartedAt: job.Status.StartTime, CompletedAt: statusTime(jobFinishedAt(job))}}
	if !r.nativeVerificationCaptureUnchanged(ctx, m, capture) || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return nil, false, native.ErrNativeComparisonUnavailable
	}
	return capture, false, nil
}

func (r *MigrationReconciler) createNativeVerificationCheck(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, expected *batchv1.Job, check string) error {
	if err := r.nativeCleanupPreflight(ctx, m, worker, pvc, original); err != nil || native.ValidateEvidenceJobSize(expected) != nil {
		return native.ErrNativeComparisonUnavailable
	}
	if !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return native.ErrNativeComparisonUnavailable
	}
	created := expected.DeepCopy()
	if r.Create(ctx, created) != nil || created.UID == "" || created.UID == worker.UID || !nativePreflightJobMatches(expected, created) {
		return native.ErrNativeComparisonUnavailable
	}
	return r.recordNativeVerificationReceipt(ctx, m, worker, pvc, original, expected, created, check)
}

func (r *MigrationReconciler) nativeVerificationCaptureUnchanged(ctx context.Context, m *v1beta1.Migration, capture *nativeVerificationCapture) bool {
	job, err := r.nativePreflightLiveJob(ctx, capture.expected, capture.job, capture.binding)
	if err != nil || !nativeEvidenceWorkerComplete(job) || !apiequality.Semantic.DeepEqual(job.Status, capture.job.Status) || nativeVerificationJobUID(m, capture.result.Check) != string(job.UID) {
		return false
	}
	pod, err := r.nativePreflightPod(ctx, job)
	return err == nil && pod != nil && ctx.Err() == nil && nativePreflightPodMatches(job, pod) && nativePreflightSamePod(capture.pod, pod) && apiequality.Semantic.DeepEqual(capture.pod.Spec, pod.Spec) && apiequality.Semantic.DeepEqual(capture.pod.Status, pod.Status)
}

// Only a known successful Create can enter; a failed persistence leaves the unrecorded helper unusable.
func (r *MigrationReconciler) recordNativeVerificationReceipt(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, expected, created *batchv1.Job, check string) error {
	if nativeVerificationJobUID(m, check) != "" || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return native.ErrNativeComparisonUnavailable
	}
	job := &batchv1.Job{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(created), job) != nil || job.UID != created.UID || job.Generation != created.Generation || !nativePreflightJobMatches(expected, job) || !nativePreflightJobMatches(created, job) {
		return native.ErrNativeComparisonUnavailable
	}
	live, _, err := r.nativeRecoveryMigration(ctx, m, pvc)
	if err != nil || !nativeControlStatusIdentity(live, m) || !apiequality.Semantic.DeepEqual(live.Status, m.Status) {
		return native.ErrNativeComparisonUnavailable
	}
	if live.ResourceVersion != m.ResourceVersion {
		return nativeControlConflict()
	}
	desired := live.DeepCopy()
	if check == compareSchema {
		desired.Status.NativeSchemaJobUID = string(created.UID)
	} else {
		desired.Status.NativeDataJobUID = string(created.UID)
	}
	desired.Status.Phase = v1beta1.PhaseVerifying
	r.setCondition(desired, v1beta1.ConditionVerified, metav1.ConditionUnknown, "VerificationRunning", "Native comparison is running")
	if err := r.patchNativeControlStatus(ctx, desired, live); err != nil {
		return err
	}
	desired.DeepCopyInto(m)
	return nil
}

// recordNativeCompareProgress puts a checked data comparison's final sample in the data item.
// The helper logs only its sealed result, so a running native compare shows no sample.
func (r *MigrationReconciler) recordNativeCompareProgress(m *v1beta1.Migration, capture *nativeVerificationCapture) {
	p, ok := capture.progress.Progress()
	if !ok || capture.result.Check != compareData || compareProgressFinal(m, v1beta1.VerificationProgressData, capture.job) {
		return
	}
	r.setCompareProgress(m, v1beta1.VerificationProgressData, capture.job, nativeCompareSample(p))
}

// nativeCompareSample is the final sample p describes: every planned table and byte compared, none failed.
func nativeCompareSample(p native.ComparisonProgress) pgcopydb.CompareProgress {
	return pgcopydb.CompareProgress{
		PlannedTables: p.Tables, ComparedTables: p.Tables, PlannedBytes: p.Bytes, ComparedBytes: p.Bytes,
		ComparedRows: p.Rows, DifferingTables: p.DifferingTables, ElapsedMs: p.ElapsedMs, Final: true,
	}
}
