package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var errNativeCloneReport = errors.New("native clone completion report unavailable")

// Report DATA carries no Pod binding: follow callers must retain the original Pod across their outer proof join.
func (r *MigrationReconciler) readNativeCloneReport(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*native.Report, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if r.NativeJobs == nil || r.APIReader == nil || m == nil || worker == nil || pvc == nil || ctx.Err() != nil ||
		m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || !nativeEvidenceWorkerComplete(worker) {
		return nil, errNativeCloneReport
	}
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	_, original, err := r.nativeCompletionCurrent(ctx, m, worker, pvc, nil)
	if err != nil || nativePodless(original) {
		return nil, errNativeCloneReport
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		return nil, errNativeCloneReport
	}
	status := original.Status.ContainerStatuses[0]
	binding := podexec.NativeContainerBinding{PodUID: original.UID, ContainerName: workerContainer, ContainerID: status.ContainerID, ImageID: image, TemplateSHA256: podexec.NativeTemplateSHA256(worker.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(worker.Spec.Template)}
	output, err := r.NativeJobs.JobCloneNative(ctx, m.Namespace, worker.UID, binding)
	if err != nil || !output.Successful() {
		return nil, errNativeCloneReport
	}
	report, err := native.ParseCloneOutput(output.Payload(), work, followEnabled(m))
	if err != nil || native.ValidateCompletedClone(report, work) != nil {
		return nil, errNativeCloneReport
	}
	confirmed, err := r.nativeQuiescentWorker(ctx, m, worker, pvc)
	if err != nil || ctx.Err() != nil || !nativeSameWorkerPod(original, confirmed) {
		return nil, errNativeCloneReport
	}
	return report, nil
}

// nativeCloneReportReceipted reports that an earlier pass read and validated worker's completed-clone report.
func nativeCloneReportReceipted(m *v1beta1.Migration, worker *batchv1.Job) bool {
	return m != nil && worker != nil && worker.UID != "" && m.Status.NativeReportJobUID == string(worker.UID) && m.Status.NativeJobUID == string(worker.UID)
}

// reconcileNativeCloneReport is the finish column of the W table for a Complete worker: go with the
// report receipt, read the report from the worker's pod and record the receipt, or count an attempt
// whose pod went before its report was read. original is the engine-free anchor of this pass.
func (r *MigrationReconciler) reconcileNativeCloneReport(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (bool, ctrl.Result, error) {
	if nativeCloneReportReceipted(m, worker) {
		return false, ctrl.Result{}, nil
	}
	desired := m.DeepCopy()
	if nativePodless(original) {
		// Without a pod there is no log, so no pass can ever read this report; a resumed attempt proves completion again.
		r.countNativeAttempt(desired, fmt.Sprintf("Native worker Job %s completed, but its pod was gone before its report was read", worker.Name))
		desired.Status.ObservedGeneration = desired.Generation
	} else {
		if _, err := r.readNativeCloneReport(ctx, m, worker, pvc); err != nil {
			return true, ctrl.Result{}, err
		}
		desired.Status.NativeReportJobUID = string(worker.UID)
	}
	return r.patchNativeWorkerState(ctx, m, base, desired)
}

// nativeCloneReportAdmitted accepts the completed-clone report of this pass, or none once its receipt is recorded.
func nativeCloneReportAdmitted(m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, clone *native.Report) bool {
	if clone == nil {
		return nativeCloneReportReceipted(m, worker)
	}
	work, err := nativeWorkIdentity(m, pvc)
	return err == nil && native.ValidateCompletedClone(clone, work) == nil
}
