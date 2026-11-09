package controller

import (
	"bytes"
	"context"
	"errors"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

var errNativeFollowCompletion = errors.New("native follow completion evidence unavailable")

func (r *MigrationReconciler) readNativeFollowCompletion(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, clone *native.Report, acknowledgment []byte) (*native.CompletedObservation, error) {
	if m == nil || worker == nil || pvc == nil || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || !followEnabled(m) ||
		m.Status.Replication == nil || m.Status.Replication.Endpos == "" ||
		len(acknowledgment) > 65536 || bytes.Count(acknowledgment, []byte{'\n'}) != 1 || !bytes.HasSuffix(acknowledgment, []byte{'\n'}) || !bytes.HasPrefix(acknowledgment, []byte("native-report: ")) {
		return nil, errNativeFollowCompletion
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil || !nativeEvidenceWorkerComplete(worker) || native.ValidateCompletedClone(clone, work) != nil {
		return nil, errNativeFollowCompletion
	}
	original, err := r.nativeQuiescentWorker(ctx, m, worker, pvc)
	if err != nil {
		return nil, errNativeFollowCompletion
	}
	observation, err := r.readNativeCompletedEvidence(ctx, m, worker, pvc)
	if err != nil || observation == nil {
		return nil, errNativeFollowCompletion
	}
	checkpoint := observation.Checkpoint().Evidence()
	report, err := native.ParseReportLine(acknowledgment, native.Binding{Command: "stream sentinel set endpos", Work: work, EndLSN: m.Status.Replication.Endpos, Checkpoint: &checkpoint})
	if err != nil || report.Replication == nil || report.Replication.Drained == nil || !*report.Replication.Drained || report.Replication.EndPersisted == nil || !*report.Replication.EndPersisted {
		return nil, errNativeFollowCompletion
	}
	confirmed, err := r.nativeQuiescentWorker(ctx, m, worker, pvc)
	if err != nil || ctx.Err() != nil || !nativePreflightSamePod(original, confirmed) || !apiequality.Semantic.DeepEqual(original.Spec, confirmed.Spec) || !apiequality.Semantic.DeepEqual(original.Status, confirmed.Status) {
		return nil, errNativeFollowCompletion
	}
	return observation, nil
}
