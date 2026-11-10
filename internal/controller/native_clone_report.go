package controller

import (
	"context"
	"errors"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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
	if err != nil {
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
	if err != nil || ctx.Err() != nil || !nativePreflightSamePod(original, confirmed) ||
		!apiequality.Semantic.DeepEqual(original.Spec, confirmed.Spec) || !apiequality.Semantic.DeepEqual(original.Status, confirmed.Status) {
		return nil, errNativeCloneReport
	}
	return report, nil
}
