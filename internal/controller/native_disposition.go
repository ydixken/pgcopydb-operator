package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var errNativeDisposition = errors.New("native disposition authority unavailable")

func (r *MigrationReconciler) reconcileNativeDisposition(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, ctrl.Result, error) {
	if m == nil || base == nil || m.Spec.Engine != v1beta1.MigrationEngineRPGMigrate {
		return false, ctrl.Result{}, nil
	}
	deleting := !m.DeletionTimestamp.IsZero()
	if !deleting && !m.Spec.Suspend && !nativeRetryRouted(worker) {
		return false, ctrl.Result{}, nil
	}
	r.stopSampling(client.ObjectKeyFromObject(m))
	if r.APIReader == nil || r.Client == nil || ctx.Err() != nil || !nativeDispositionSnapshotMatches(m, base) {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if deleting && !controllerutil.ContainsFinalizer(m, finalizerName) {
		// Without the operator finalizer, owner-reference garbage collection removes everything.
		metrics.Forget(m.Namespace, m.Name)
		return true, ctrl.Result{}, nil
	}
	if deleting && m.Status.NativeFinalizedJobUID != "" && m.Status.NativeFinalizedJobUID != m.Status.NativeJobUID {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if deleting && nativeFinalizationRecorded(m) {
		result, err := r.reconcileNativeFinalizedDeletion(ctx, m)
		return true, result, err
	}
	if worker == nil {
		if err := r.nativeDispositionUnstarted(ctx, m); err != nil {
			return true, ctrl.Result{}, err
		}
		if deleting {
			if pvc != nil && followEnabled(m) && r.deleteNativeWorkClaim(ctx, m, pvc) != nil {
				return true, ctrl.Result{}, errNativeDisposition
			}
			return r.finishNativeDeletion(ctx, m)
		}
		return r.recordNativeSuspended(ctx, m, base, nil)
	}
	if deleting || m.Spec.Suspend {
		return r.reconcileNativeRetainedDisposition(ctx, m, base, worker, pvc)
	}
	if pvc == nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	live, current, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	result, err := r.reconcileNativeFailedWorker(ctx, live, current, claim)
	return true, result, err
}

func nativeDispositionSnapshotMatches(m, base *v1beta1.Migration) bool {
	return m.UID != "" && m.UID == base.UID && m.Generation == base.Generation && m.ResourceVersion == base.ResourceVersion &&
		apiequality.Semantic.DeepEqual(m.Spec, base.Spec) && apiequality.Semantic.DeepEqual(m.Status, base.Status)
}

// reconcileNativeDeletionGuards runs before the worker tables on a deletion pass. Without the operator
// finalizer, garbage collection owns every object; R9 releases a terminating namespace at once, and R8
// waits out foreground deletion before any helper Job, since the garbage collector would delete it too.
func (r *MigrationReconciler) reconcileNativeDeletionGuards(ctx context.Context, m, base *v1beta1.Migration, s nativeWorkerState) (bool, ctrl.Result, error) {
	if m.DeletionTimestamp.IsZero() || m.Spec.Engine != v1beta1.MigrationEngineRPGMigrate {
		return false, ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(m, finalizerName) {
		r.stopSampling(client.ObjectKeyFromObject(m))
		metrics.Forget(m.Namespace, m.Name)
		return true, ctrl.Result{}, nil
	}
	// Before a worker could have run there is nothing to clean up, and the release needs no helper.
	if m.Status.Attempts == 0 && m.Status.NativeWorkerIntent == "" && s.n == nextAbsent {
		return false, ctrl.Result{}, nil
	}
	if r.nativeNamespaceTerminating(ctx, m) {
		r.stopSampling(client.ObjectKeyFromObject(m))
		return r.releaseNativeCleanupFailed(ctx, m, v1beta1.ReasonNamespaceTerminating, s.pvc)
	}
	if slices.Contains(m.Finalizers, metav1.FinalizerDeleteDependents) {
		r.stopSampling(client.ObjectKeyFromObject(m))
		return r.holdNative(ctx, m, base, v1beta1.ReasonForegroundDeletion,
			"Foreground deletion is removing this Migration's Jobs; native cleanup starts once the garbage collector has deleted them and removed the foregroundDeletion finalizer")
	}
	return false, ctrl.Result{}, nil
}

// nativeNamespaceTerminating is R9: a dry-run create of the next helper Job runs admission and
// persists nothing. Only Forbidden with cause NamespaceTerminating counts; any other refusal, such
// as a quota or a webhook that refuses dry-run, leaves the pass on its normal path.
func (r *MigrationReconciler) nativeNamespaceTerminating(ctx context.Context, m *v1beta1.Migration) bool {
	op, err := nativeRecoveryOperationID()
	if err != nil || r.Scheme == nil {
		return false
	}
	probe, err := buildNativeVersionJob(m, r.RPGMigrateRunnerImage, "rpgmigrate-version-"+op)
	if err != nil || controllerutil.SetControllerReference(m, probe, r.Scheme) != nil {
		return false
	}
	err = r.Create(ctx, probe, client.DryRunAll)
	return apierrors.IsForbidden(err) && apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause)
}

var errNativeCleanupFailed = errors.New("native cleanup did not run; the finalizer is released")

// releaseNativeCleanupFailed is CF: CleanupFailed=True before the finalizer patch, an event attempt,
// and an error log line that names the slot and publication where the API server refuses the event (I4).
// The work PVC is deleted when it is still this Migration's bound claim.
func (r *MigrationReconciler) releaseNativeCleanupFailed(ctx context.Context, m *v1beta1.Migration, reason string, pvc *corev1.PersistentVolumeClaim) (bool, ctrl.Result, error) {
	msg, err := nativeCleanupFailedMessage(m, reason)
	run, runErr := nativeRunID(m)
	if err != nil || runErr != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	desired := m.DeepCopy()
	r.setCondition(desired, v1beta1.ConditionCleanupFailed, metav1.ConditionTrue, reason, msg)
	if !apiequality.Semantic.DeepEqual(desired.Status, m.Status) {
		if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(m, client.MergeFromWithOptimisticLock{})); err != nil {
			return true, ctrl.Result{}, nativeWorkerError(err)
		}
		desired.DeepCopyInto(m)
	}
	logf.FromContext(ctx).Error(errNativeCleanupFailed, msg, "reason", reason, "slots", "rpgm_s_"+run+"_*", "publications", "rpgm_p_"+run+"_*",
		"sourceHost", m.Spec.Source.Host, "sourceDatabase", m.Spec.Source.Database)
	if r.Recorder != nil {
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, v1beta1.ConditionCleanupFailed, "Cleanup", "%s", truncate(msg, maxDetailLen))
	}
	if pvc != nil && r.deleteNativeWorkClaim(ctx, m, pvc) != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	return r.finishNativeDeletion(ctx, m)
}

// reconcileNativeRetainedDisposition is the suspend and delete columns of the W, V and C tables for a
// recorded worker: stop or drop a worker that has not ended, then suspend, or clean up and release.
func (r *MigrationReconciler) reconcileNativeRetainedDisposition(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, ctrl.Result, error) {
	deleting := !m.DeletionTimestamp.IsZero()
	if deleting && pvc == nil && followEnabled(m) {
		// A missing, deleting or replaced work PVC cannot be mounted, so cleanup cannot read its receipts.
		return r.releaseNativeCleanupFailed(ctx, m, v1beta1.ReasonWorkVolumeLost, nil)
	}
	missing := nativeWorkerMissing(worker)
	admitted := !missing && nativeWorkerAdmitted(m, worker)
	if !missing && (!worker.DeletionTimestamp.IsZero() || worker.Status.Terminating != nil && *worker.Status.Terminating != 0) {
		return r.holdNative(ctx, m, base, v1beta1.ReasonWorkerPodRemains, fmt.Sprintf(
			"Native worker Job %s is being deleted or has terminating pods; the operator waits until they are gone", worker.Name))
	}
	if !missing && !nativeWorkerEnded(worker) {
		return r.stopNativeDispositionWorker(ctx, m, base, worker, pvc, admitted)
	}
	if !missing && !admitted && deleting {
		// Recovery reads bind only an admitted worker, so an ended modified one is dropped and cleanup follows a gone W.
		return r.dropNativeDispositionJob(ctx, m, base, worker)
	}
	// W has ended or is gone; R1 holds, with the same pod or none, before and after the compare disposal.
	original, err := r.nativeDispositionEngineFree(ctx, m, worker, pvc, nil)
	if err != nil {
		return r.holdNativeBusy(ctx, m, base, err)
	}
	if !deleting || !followEnabled(m) {
		unpin, held, err := r.disposeNativeCompareHelpers(ctx, m, pvc, !deleting)
		if held != nil || err != nil {
			return r.holdNativeCompare(ctx, m, base, held, err)
		}
		if _, err := r.nativeDispositionEngineFree(ctx, m, worker, pvc, original); err != nil {
			return r.holdNativeBusy(ctx, m, base, err)
		}
		if deleting {
			return r.finishNativeDeletion(ctx, m)
		}
		return r.recordNativeSuspended(ctx, m, base, unpin)
	}
	live, current, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if _, held, err := r.disposeNativeCompareHelpers(ctx, live, claim, false); held != nil || err != nil {
		return r.holdNativeCompare(ctx, live, live.DeepCopy(), held, err)
	}
	if err := r.cleanupNativeDeletion(ctx, live, current, claim, original); err != nil {
		return r.holdNativeCleanupUnavailable(ctx, live, err)
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, live, current, claim, original); err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if r.deleteNativeWorkClaim(ctx, live, claim) != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	return r.finishNativeDeletion(ctx, live)
}

// stopNativeDispositionWorker is the stop of lifecycle 7: a bound running container gets TERM, and a
// drop when it has not ended within the pass; a worker whose container never started is dropped. The
// worker table has already recorded a start it saw (dj), so the next attempt's --restart stays right.
func (r *MigrationReconciler) stopNativeDispositionWorker(ctx context.Context, m, base *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, admitted bool) (bool, ctrl.Result, error) {
	if !admitted {
		return r.dropNativeDispositionJob(ctx, m, base, worker)
	}
	// A container that never started is never bound, so this drops it rather than sending TERM.
	done, err := r.stopOrDropNativeWorker(ctx, m, worker, pvc)
	if err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if !done {
		return true, ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	// The stop ended W or dropped it; this pass continues from a fresh read of the recorded Job.
	current := &batchv1.Job{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(worker), current); apierrors.IsNotFound(err) {
		current = nativeMissingWorker(m)
	} else if err != nil || current.UID != worker.UID || !nativeWorkerEnded(current) {
		return true, ctrl.Result{RequeueAfter: time.Nanosecond}, nil
	}
	return r.reconcileNativeRetainedDisposition(ctx, m, base, current, pvc)
}

// nativeDispositionEngineFree is R1 for W observed twice, anchored to original: through the recovery
// reads when W is admitted or gone and its claim is bound, and on the Job itself otherwise.
func (r *MigrationReconciler) nativeDispositionEngineFree(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (*corev1.Pod, error) {
	if pvc != nil && (nativeWorkerMissing(worker) || nativeWorkerAdmitted(m, worker)) {
		_, pod, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original)
		return pod, err
	}
	pod, err := r.nativeWorkerEngineFree(ctx, worker)
	if err != nil {
		return nil, err
	}
	confirmed, err := r.nativeWorkerEngineFree(ctx, worker)
	if err != nil {
		return nil, err
	}
	if !nativeSameWorkerPod(pod, confirmed) || !nativeSameWorkerPod(original, confirmed) {
		return nil, errNativeRecovery
	}
	return confirmed, nil
}

func (r *MigrationReconciler) holdNativeBusy(ctx context.Context, m, base *v1beta1.Migration, err error) (bool, ctrl.Result, error) {
	if busy, ok := errors.AsType[*nativeWorkerBusy](err); ok {
		return r.holdNative(ctx, m, base, v1beta1.ReasonWorkerPodRemains, busy.message(m.Status.Attempts+1))
	}
	return true, ctrl.Result{}, errNativeDisposition
}

func (r *MigrationReconciler) dropNativeDispositionJob(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job) (bool, ctrl.Result, error) {
	done, err := r.dropNativeWorker(ctx, m, job)
	if err != nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	if !done {
		return r.holdNative(ctx, m, base, v1beta1.ReasonWorkerPodRemains, fmt.Sprintf(
			"Job %s was deleted and it or its pods are still terminating; the operator waits until they are gone", job.Name))
	}
	return true, ctrl.Result{RequeueAfter: time.Nanosecond}, nil
}

// nativeCompareHeld is a C-table cell that waits or holds: a compare Job that is not this
// Migration's, or one that is still terminating after its drop.
type nativeCompareHeld struct{ reason, message string }

// disposeNativeCompareHelpers is the C table's suspend and delete columns. It drops every owned
// compare Job at its canonical name by UID, with no spec match (R4), except the ended pinned compare
// that suspend keeps; it returns the checks whose pin suspend clears.
func (r *MigrationReconciler) disposeNativeCompareHelpers(ctx context.Context, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, suspend bool) ([]string, *nativeCompareHeld, error) {
	var unpin []string
	for _, check := range []string{compareSchema, compareData} {
		pin := nativeVerificationJobUID(m, check)
		job := &batchv1.Job{}
		err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: compareJobName(m, check)}, job)
		switch {
		case apierrors.IsNotFound(err):
			if pin == "" {
				continue
			}
			// Pinned and gone: the pin is released only once no pod of that Job can still run.
			if _, err := r.nativeWorkerEngineFree(ctx, nativeGoneJob(m.Namespace, compareJobName(m, check), types.UID(pin))); err != nil {
				if busy, ok := errors.AsType[*nativeWorkerBusy](err); ok {
					return nil, &nativeCompareHeld{v1beta1.ReasonWorkerPodRemains, fmt.Sprintf("Pod %s of compare Job %s is %s on node %s; the operator waits until it has ended", busy.pod, busy.job, busy.phase, busy.node)}, nil
				}
				return nil, nil, errNativeDisposition
			}
			unpin = append(unpin, check)
			continue
		case err != nil:
			return nil, nil, errNativeDisposition
		case !nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()):
			return nil, &nativeCompareHeld{v1beta1.ReasonForeignObject, fmt.Sprintf("Job %s is not controlled by this Migration; the operator neither reads nor removes it", job.Name)}, nil
		}
		if suspend && pin != "" && string(job.UID) == pin && r.nativeCompareKept(ctx, m, pvc, job, check) {
			continue
		}
		done, err := r.dropNativeWorker(ctx, m, job)
		if err != nil {
			return nil, nil, errNativeDisposition
		}
		if !done {
			return nil, &nativeCompareHeld{v1beta1.ReasonWorkerPodRemains, fmt.Sprintf("Compare Job %s was deleted and it or its pod is still terminating; the operator waits until they are gone", job.Name)}, nil
		}
		if pin != "" {
			unpin = append(unpin, check)
		}
	}
	return unpin, nil, nil
}

// nativeCompareKept is the suspend cell "pinned, ended, pod": the pinned Job still matches the Job
// this Migration builds, has ended, and keeps its terminal pod, so a resumed finish can read it.
func (r *MigrationReconciler) nativeCompareKept(ctx context.Context, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, job *batchv1.Job, check string) bool {
	if pvc == nil || !job.DeletionTimestamp.IsZero() {
		return false
	}
	expected, err := r.nativeRemovalCompareJob(m, pvc, job, check)
	if err != nil || !nativePreflightJobMatches(expected, job) {
		return false
	}
	pod, err := r.nativePreflightPod(ctx, job)
	return err == nil && pod != nil && nativePreflightPodMatches(job, pod) &&
		nativeStopTerminal(nativeStopFacts{worker: job, pod: pod, binding: podexec.NativeContainerBinding{ImageID: runnerImageFor(m, r.RPGMigrateRunnerImage)}})
}

func (r *MigrationReconciler) holdNativeCompare(ctx context.Context, m, base *v1beta1.Migration, held *nativeCompareHeld, err error) (bool, ctrl.Result, error) {
	if err != nil || held == nil {
		return true, ctrl.Result{}, errNativeDisposition
	}
	return r.holdNative(ctx, m, base, held.reason, held.message)
}

// holdNativeCleanupUnavailable keeps the finalizer when cleanup could run but did not finish, for
// example on an unreachable source: Blocked=True CleanupUnavailable, and the error keeps its backoff.
func (r *MigrationReconciler) holdNativeCleanupUnavailable(ctx context.Context, m *v1beta1.Migration, cause error) (bool, ctrl.Result, error) {
	desired := m.DeepCopy()
	r.setNativeBlocked(desired, v1beta1.ReasonCleanupUnavailable,
		"Native cleanup of the source slot and publication did not finish; the finalizer stays and the operator retries")
	if !apiequality.Semantic.DeepEqual(desired.Status, m.Status) {
		if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(m, client.MergeFromWithOptimisticLock{})); err != nil {
			return true, ctrl.Result{}, nativeWorkerError(err)
		}
	}
	return true, ctrl.Result{}, cause
}

// The old operation is used only to reconstruct a pinned immutable helper for removal.
func (r *MigrationReconciler) nativeRemovalCompareJob(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, job *batchv1.Job, check string) (*batchv1.Job, error) {
	if len(job.Spec.Template.Spec.Containers) != 1 || len(job.Spec.Template.Spec.Containers[0].Args) != 14 {
		return nil, errNativeDisposition
	}
	op := job.Spec.Template.Spec.Containers[0].Args[10]
	command, args, env, volumes, mounts, err := native.ComparisonMaterialization(m, op, check)
	if err != nil {
		return nil, errNativeDisposition
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		return nil, errNativeDisposition
	}
	expected, err := buildNativeVersionJob(m, r.RPGMigrateRunnerImage, "rpgmigrate-version-"+op)
	if err != nil {
		return nil, errNativeDisposition
	}
	expected.Name, expected.Spec.ActiveDeadlineSeconds = compareJobName(m, check), nil
	expected.Spec.Template.Spec.Volumes = append([]corev1.Volume{{Name: workdirVolume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}}, volumes...)
	c := &expected.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, append(env, nativeIdentityEnv(work)...), append([]corev1.VolumeMount{{Name: workdirVolume, MountPath: pgcopydb.WorkMount}}, mounts...)
	if controllerutil.SetControllerReference(m, expected, r.Scheme) != nil || native.ValidateEvidenceJobSize(expected) != nil {
		return nil, errNativeDisposition
	}
	return expected, nil
}

// nativeClosedCompareHelpers is the retry column of the C table, closed: the compare Jobs it dropped
// and the pins it saw, which the retry receipt then clears.
type nativeClosedCompareHelpers struct {
	migrationUID, workerUID, pvcUID types.UID
	generation                      int64
	pins                            [2]string
	removed                         []types.UID
}

// closeNativeCompareHelpers is the retry column of the C table: every owned compare Job is dropped
// by UID, pinned or not and with no spec match, a missing one needs nothing, and a foreign one holds.
func (r *MigrationReconciler) closeNativeCompareHelpers(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (*nativeClosedCompareHelpers, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*nativeWorkerDropWait+10*time.Second)
	defer cancel()
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
		return nil, errNativeDisposition
	}
	closed := &nativeClosedCompareHelpers{migrationUID: m.UID, generation: m.Generation, workerUID: worker.UID, pvcUID: pvc.UID, pins: [2]string{m.Status.NativeSchemaJobUID, m.Status.NativeDataJobUID}}
	for _, check := range []string{compareSchema, compareData} {
		job := &batchv1.Job{}
		err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: compareJobName(m, check)}, job)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil || job.UID == worker.UID {
			return nil, errNativeDisposition
		}
		if !nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
			desired := m.DeepCopy()
			r.setNativeBlocked(desired, v1beta1.ReasonForeignObject, fmt.Sprintf("Job %s is not controlled by this Migration; the operator neither reads nor removes it, and the retry waits until it is gone", job.Name))
			if !apiequality.Semantic.DeepEqual(desired.Status, m.Status) {
				_ = r.Status().Patch(ctx, desired, client.MergeFromWithOptions(m, client.MergeFromWithOptimisticLock{}))
			}
			return nil, errNativeDisposition
		}
		if done, err := r.dropNativeWorker(ctx, m, job); err != nil || !done {
			return nil, errNativeDisposition
		}
		closed.removed = append(closed.removed, job.UID)
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
		return nil, errNativeDisposition
	}
	if r.nativeCompareClosureCurrent(ctx, m, closed) != nil {
		return nil, errNativeDisposition
	}
	return closed, nil
}

// nativeCompareClosureCurrent proves the closure still holds: the same Migration, worker, claim and
// pins, no Job at either compare name, and no unfinished pod of a dropped or pinned compare.
func (r *MigrationReconciler) nativeCompareClosureCurrent(ctx context.Context, m *v1beta1.Migration, closed *nativeClosedCompareHelpers) error {
	if closed == nil || m == nil || closed.migrationUID != m.UID || closed.generation != m.Generation || string(closed.workerUID) != m.Status.NativeJobUID || string(closed.pvcUID) != m.Status.NativePVCUID || closed.pins != [2]string{m.Status.NativeSchemaJobUID, m.Status.NativeDataJobUID} {
		return errNativeDisposition
	}
	for _, check := range []string{compareSchema, compareData} {
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: compareJobName(m, check)}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
			return errNativeDisposition
		}
	}
	uids := slices.Clone(closed.removed)
	for _, pin := range closed.pins {
		if pin != "" {
			uids = append(uids, types.UID(pin))
		}
	}
	for _, uid := range uids {
		if _, err := r.nativeWorkerEngineFree(ctx, nativeGoneJob(m.Namespace, "compare", uid)); err != nil {
			return errNativeDisposition
		}
	}
	if ctx.Err() != nil {
		return errNativeDisposition
	}
	return nil
}

func (r *MigrationReconciler) readNativeDeletionInventory(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (native.AdmittedOwnedState, error) {
	var owned native.AdmittedOwnedState
	err := r.runNativeRecoveryRead(ctx, m, worker, pvc, original, native.JobRoleCleanup, func(ctx context.Context, w native.ExecutionData) error {
		var readErr error
		owned, readErr = r.NativeEvidence.ReadOwnedState(ctx, w)
		return readErr
	})
	if err != nil || owned.Work() == (native.WorkIdentity{}) || len(owned.Resources()) == 0 {
		return native.AdmittedOwnedState{}, errNativeDisposition
	}
	return owned, nil
}

func (r *MigrationReconciler) cleanupNativeDeletion(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) error {
	if m == nil || m.DeletionTimestamp.IsZero() || r.NativeJobs == nil || !followEnabled(m) {
		return errNativeDisposition
	}
	_, original, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original)
	if err != nil {
		return errNativeDisposition
	}
	owned, err := r.readNativeDeletionInventory(ctx, m, worker, pvc, original)
	if err != nil {
		return errNativeDisposition
	}
	// A complete inventory that found only the retained workdir leaves nothing to drop; the second read must agree.
	if !owned.Unprovisioned() {
		if err := r.runNativeDeletionCleanup(ctx, m, worker, pvc, original, owned); err != nil {
			return err
		}
	}
	after, err := r.readNativeDeletionInventory(ctx, m, worker, pvc, original)
	if err != nil || after.Work() != owned.Work() || !slices.Equal(after.Resources(), owned.Resources()) {
		return errNativeDisposition
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
		return errNativeDisposition
	}
	return nil
}

func (r *MigrationReconciler) runNativeDeletionCleanup(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, owned native.AdmittedOwnedState) (err error) {
	if r.ensureNativeRecoveryPreflight(ctx, m, worker, pvc, original) != nil {
		return errNativeDisposition
	}
	op, err := nativeRecoveryOperationID()
	if err != nil {
		return errNativeDisposition
	}
	expected, err := buildNativeCleanupJob(m, pvc, r.RPGMigrateRunnerImage, "rpgmigrate-cleanup-"+op, owned)
	if err != nil || controllerutil.SetControllerReference(m, expected, r.Scheme) != nil || native.ValidateEvidenceJobSize(expected) != nil {
		return errNativeDisposition
	}
	deadline := time.Now().Add(90 * time.Second)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	work, cancel := context.WithDeadline(ctx, deadline.Add(-nativePreflightCleanupReserve))
	defer cancel()
	if _, _, err := r.nativeRecoveryQuiescentCurrent(work, m, worker, pvc, original); err != nil {
		return errNativeDisposition
	}
	created := expected.DeepCopy()
	if r.Create(work, created) != nil || created.UID == "" {
		return errNativeDisposition
	}
	var pod *corev1.Pod
	defer func() {
		cleanup, end := context.WithDeadline(context.WithoutCancel(ctx), deadline)
		defer end()
		if r.nativePreflightCleanup(cleanup, m, created, pod) != nil {
			err = errNativeDisposition
		}
	}()
	if created.UID == worker.UID || !nativePreflightJobMatches(expected, created) {
		return errNativeDisposition
	}
	observed, terminal, err := r.nativeDeletionCleanupTerminal(work, m, worker, pvc, original, expected, created)
	if err != nil {
		return errNativeDisposition
	}
	pod = terminal
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: workerContainer, ContainerID: pod.Status.ContainerStatuses[0].ContainerID, ImageID: image, TemplateSHA256: podexec.NativeTemplateSHA256(created.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(created.Spec.Template)}
	output, err := r.NativeJobs.JobCleanupNative(work, m.Namespace, created.UID, binding)
	if err != nil || !output.Successful() {
		return errNativeDisposition
	}
	if !nativeDeletionCleanupReportMatches(output.Payload(), owned) {
		return errNativeDisposition
	}
	if _, _, err := r.nativeRecoveryQuiescentCurrent(work, m, worker, pvc, original); err != nil {
		return errNativeDisposition
	}
	job, err := r.nativePreflightLiveJob(work, expected, created, binding)
	if err != nil || !apiequality.Semantic.DeepEqual(job.Status, observed.Status) {
		return errNativeDisposition
	}
	finalPod, err := r.nativePreflightPod(work, job)
	if err != nil || finalPod == nil || !nativePreflightPodMatches(job, finalPod) || !nativePreflightSamePod(pod, finalPod) || !apiequality.Semantic.DeepEqual(pod.Spec, finalPod.Spec) || !podexec.SamePodState(pod, finalPod, workerContainer) || work.Err() != nil {
		return errNativeDisposition
	}
	return nil
}

func (r *MigrationReconciler) nativeDispositionUnstarted(ctx context.Context, expected *v1beta1.Migration) error {
	if expected.Status.Attempts != 0 || expected.Status.JobName != "" || expected.Status.NativeJobUID != "" || expected.Status.NativePVCUID != "" || expected.Status.NativeSchemaJobUID != "" || expected.Status.NativeDataJobUID != "" {
		return errNativeDisposition
	}
	live := &v1beta1.Migration{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), live) != nil || live.UID != expected.UID || live.Generation != expected.Generation || live.ResourceVersion != expected.ResourceVersion ||
		!apiequality.Semantic.DeepEqual(live.Spec, expected.Spec) || !apiequality.Semantic.DeepEqual(live.Status, expected.Status) || !apiequality.Semantic.DeepEqual(live.DeletionTimestamp, expected.DeletionTimestamp) {
		return errNativeDisposition
	}
	for _, name := range []string{jobName(live, 1), compareJobName(live, compareSchema), compareJobName(live, compareData)} {
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: live.Namespace, Name: name}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
			return errNativeDisposition
		}
	}
	if ctx.Err() != nil {
		return errNativeDisposition
	}
	return nil
}

// recordNativeSuspended records Suspended once W is engine-free. A compare suspend dropped loses its
// pin, its progress item and Verified, so a resumed finish creates a fresh one (lifecycle 9).
func (r *MigrationReconciler) recordNativeSuspended(ctx context.Context, m, base *v1beta1.Migration, unpin []string) (bool, ctrl.Result, error) {
	desired := m.DeepCopy()
	desired.Status.Phase, desired.Status.ObservedGeneration = v1beta1.PhaseSuspended, desired.Generation
	r.endHold(desired, reasonHoldSuspended, "Native worker is quiescent; retained state is preserved")
	for _, check := range unpin {
		if check == compareSchema {
			desired.Status.NativeSchemaJobUID = ""
		} else {
			desired.Status.NativeDataJobUID = ""
			desired.Status.VerificationProgress = slices.DeleteFunc(desired.Status.VerificationProgress, func(p v1beta1.VerificationProgress) bool {
				return p.Kind == v1beta1.VerificationProgressData && p.JobName == compareJobName(m, compareData)
			})
		}
		meta.RemoveStatusCondition(&desired.Status.Conditions, v1beta1.ConditionVerified)
	}
	r.clearNativeBlocked(desired)
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return true, ctrl.Result{}, nativeWorkerError(err)
	}
	desired.DeepCopyInto(m)
	metrics.Record(desired)
	return true, ctrl.Result{}, nil
}

func (r *MigrationReconciler) finishNativeDeletion(ctx context.Context, m *v1beta1.Migration) (bool, ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(m, finalizerName) {
		desired := m.DeepCopy()
		controllerutil.RemoveFinalizer(desired, finalizerName)
		if err := r.Patch(ctx, desired, client.MergeFromWithOptions(m, client.MergeFromWithOptimisticLock{})); err != nil {
			return true, ctrl.Result{}, nativeWorkerError(err)
		}
		desired.DeepCopyInto(m)
	}
	metrics.Forget(m.Namespace, m.Name)
	return true, ctrl.Result{}, nil
}

func (r *MigrationReconciler) nativeDeletionCleanupTerminal(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, expected, created *batchv1.Job) (*batchv1.Job, *corev1.Pod, error) {
	var observed *batchv1.Job
	var pod *corev1.Pod
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	err := wait.PollUntilContextCancel(ctx, nativePreflightPoll, true, func(ctx context.Context) (bool, error) {
		if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original); err != nil {
			return false, errNativeDisposition
		}
		job, err := r.nativeEvidenceLiveJob(ctx, created, m)
		if err != nil || job.Generation != created.Generation || !nativePreflightJobMatches(expected, job) || nativePreflightJobFailed(job) {
			return false, errNativeDisposition
		}
		current, err := r.nativePreflightPod(ctx, job)
		if err != nil || current != nil && (!nativePreflightPodMatches(job, current) || !nativePreflightSamePod(pod, current)) {
			return false, errNativeDisposition
		}
		if current == nil {
			return false, nil
		}
		pod = current.DeepCopy()
		if !nativeEvidenceWorkerComplete(job) {
			return false, nil
		}
		if !nativeRecoveryPodTerminal(pod, image) || pod.Status.Phase != corev1.PodSucceeded || pod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 || pod.Status.ContainerStatuses[0].State.Terminated.Signal != 0 {
			return false, errNativeDisposition
		}
		observed = job
		return true, nil
	})
	if err != nil {
		return nil, nil, errNativeDisposition
	}

	return observed, pod, nil

}

func nativeDeletionCleanupReportMatches(raw []byte, owned native.AdmittedOwnedState) bool {
	if len(raw) > 65536 || bytes.Count(raw, []byte{'\n'}) != 1 || !bytes.HasSuffix(raw, []byte{'\n'}) || !bytes.HasPrefix(raw, []byte("native-report: ")) {
		return false
	}
	report, err := native.ParseReportLine(raw, native.Binding{Command: "stream cleanup", Work: owned.Work(), Resources: owned.Resources()})
	if err != nil || report.Phase != "completed" || report.Cleanup == nil || report.Cleanup.Complete == nil || !*report.Cleanup.Complete {
		return false
	}

	return true

}
