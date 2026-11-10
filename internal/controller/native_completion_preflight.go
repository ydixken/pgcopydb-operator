package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

func (r *MigrationReconciler) nativeCompletionCurrent(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (*v1beta1.Migration, *corev1.Pod, error) {
	if m == nil || worker == nil || pvc == nil || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || ctx.Err() != nil ||
		!nativeEvidenceDirectImage(runnerImageFor(m, r.RPGMigrateRunnerImage)) || !native.NativeReceiptsMatch(m, worker.UID, pvc.UID) ||
		followEnabled(m) && !nativeControlValidEnd(nativeControlEnd(m)) {
		return nil, nil, errNativePreflightUnavailable
	}
	live, current, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil || live.Spec.Suspend || !live.DeletionTimestamp.IsZero() || current.Generation != worker.Generation ||
		!nativeEvidenceWorkerComplete(current) || !native.NativeReceiptsMatch(live, current.UID, claim.UID) ||
		followEnabled(m) && nativeControlEnd(live) != nativeControlEnd(m) {
		return nil, nil, errNativePreflightUnavailable
	}
	pod, err := r.nativeQuiescentWorker(ctx, live, current, claim)
	if err != nil || ctx.Err() != nil || original != nil && (!nativePreflightSamePod(original, pod) || original.Generation != pod.Generation ||
		!apiequality.Semantic.DeepEqual(original.Spec, pod.Spec) || !apiequality.Semantic.DeepEqual(original.Status, pod.Status)) {
		return nil, nil, errNativePreflightUnavailable
	}
	return live, pod, nil
}

// Discovery returns capability data; only the completion Reader can establish target completion.
func (r *MigrationReconciler) ensureNativeCompletionPreflight(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (version *native.Version, passed bool, failure string, err error) {
	defer func() {
		if ctx.Err() != nil {
			version, passed, failure, err = nil, false, "", ctx.Err()
		}
	}()
	deadline := r.currentTime().Add(nativePreflightTotal)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	if ctx.Err() != nil {
		return nil, false, "", ctx.Err()
	}
	if m == nil || !r.nativePreflightDependencies() || time.Until(deadline) <= nativePreflightCleanupReserve {
		return nil, false, "", errNativePreflightUnavailable
	}
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	if !nativeEvidenceDirectImage(image) {
		return nil, false, nativeDiscoveryConfigRefused, nil
	}
	var original *corev1.Pod
	admit := func(ctx context.Context, expected *v1beta1.Migration, image string) (*v1beta1.Migration, error) {
		live, pod, err := r.nativeCompletionCurrent(ctx, expected, worker, pvc, original)
		if err != nil || runnerImageFor(live, r.RPGMigrateRunnerImage) != image {
			return nil, errNativePreflightUnavailable
		}
		if original == nil {
			original = pod.DeepCopy()
		}
		return live, nil
	}
	live, err := admit(ctx, m, image)
	if err != nil {
		return nil, false, "", err
	}
	if _, err := nativeConfigCapabilities(live); err != nil {
		return nil, false, nativePreflightConfigRefused, nil
	}
	work, cancel := context.WithDeadline(ctx, deadline.Add(-nativePreflightCleanupReserve))
	defer cancel()
	var operation [16]byte
	if _, err := rand.Read(operation[:]); err != nil {
		return nil, false, "", errNativePreflightUnavailable
	}
	job, err := buildNativeVersionJob(live, r.RPGMigrateRunnerImage, "rpgmigrate-version-"+hex.EncodeToString(operation[:]))
	if err != nil {
		return nil, false, nativeDiscoveryConfigRefused, nil
	}
	payload, err := r.nativePreflightPhase(ctx, work, deadline, live, image, job, true, admit)
	if err != nil || ctx.Err() != nil {
		return nativePreflightPhaseFailure(ctx, nil, err)
	}
	version, err = native.ParseVersionLine(payload)
	if err != nil {
		return nil, false, "native discovery result refused", nil
	}
	if validateNativeConfig(live, version.Capabilities) != nil {
		return nil, false, "native runner capabilities are not qualified", nil
	}
	if _, err := admit(work, live, image); err != nil {
		return nativePreflightPhaseFailure(ctx, nil, err)
	}
	return version, true, "", nil
}
