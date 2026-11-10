package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// catchUpConfirmed must come from the current execution's two fresh sampler observations.
func (r *MigrationReconciler) reconcileNativeFollowCutover(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, catchUpConfirmed bool) (ctrl.Result, error) {
	if r.NativeControl == nil || m == nil || worker == nil || pvc == nil || !followEnabled(m) || !native.NativeReceiptsMatch(m, worker.UID, pvc.UID) {
		return ctrl.Result{}, errNativeControlAdmission
	}
	facts, err := r.admitNativeControlFacts(ctx, m, worker, pvc)
	if err != nil || !native.NativeReceiptsMatch(facts.migration, facts.worker.UID, facts.pvc.UID) {
		return ctrl.Result{}, errNativeControlAdmission
	}
	if nativeControlEnd(facts.migration) != nativeControlEnd(m) || facts.migration.ResourceVersion != m.ResourceVersion {
		return ctrl.Result{}, nativeControlConflict()
	}
	if nativeControlEnd(facts.migration) == "" {
		if !cutoverWanted(facts.migration, catchUpConfirmed) {
			return ctrl.Result{RequeueAfter: pollInterval}, nil
		}
		// The first persistence pass ends here even when the optimistic patch reply is lost.
		return r.persistNativeEnd(ctx, facts.migration, facts.worker, facts.pvc, r.NativeControl)
	}
	_, err = r.setNativePersistedEnd(ctx, facts.migration, facts.worker, facts.pvc, r.NativeControl)
	return ctrl.Result{RequeueAfter: pollInterval}, err
}

func (r *MigrationReconciler) nativeFollowTerminalCurrent(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (*v1beta1.Migration, *corev1.Pod, error) {
	if m == nil || worker == nil || pvc == nil || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || !followEnabled(m) ||
		!nativeControlValidEnd(nativeControlEnd(m)) || !native.NativeReceiptsMatch(m, worker.UID, pvc.UID) {
		return nil, nil, errNativeFollowCompletion
	}
	live, current, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil || !native.NativeReceiptsMatch(live, current.UID, claim.UID) || current.Generation != worker.Generation || !nativeEvidenceWorkerComplete(current) {
		return nil, nil, errNativeFollowCompletion
	}
	pod, err := r.nativeQuiescentWorker(ctx, live, current, claim)
	if err != nil || original != nil && (!nativePreflightSamePod(original, pod) || original.Generation != pod.Generation || !apiequality.Semantic.DeepEqual(original.Spec, pod.Spec) || !apiequality.Semantic.DeepEqual(original.Status, pod.Status)) {
		return nil, nil, errNativeFollowCompletion
	}
	return live, pod, nil
}

type nativeEndposCapture struct {
	raw                         []byte
	original                    *corev1.Pod
	expected, created, observed *batchv1.Job
	pod                         *corev1.Pod
	binding                     podexec.NativeContainerBinding
}

func (r *MigrationReconciler) readNativeEndposAck(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) ([]byte, error) {
	capture, err := r.captureNativeEndposAck(ctx, m, worker, pvc)
	if err != nil {
		return nil, err
	}
	return capture.raw, nil
}

func (r *MigrationReconciler) captureNativeEndposAck(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*nativeEndposCapture, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if r.NativeJobs == nil || r.Client == nil || r.APIReader == nil || r.Scheme == nil {
		return nil, errNativeFollowCompletion
	}
	live, original, err := r.nativeFollowTerminalCurrent(ctx, m, worker, pvc, nil)
	if err != nil {
		return nil, errNativeFollowCompletion
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, errNativeFollowCompletion
	}
	expected, err := buildNativeEndposJob(live, pvc, r.RPGMigrateRunnerImage, "rpgmigrate-endpos-"+hex.EncodeToString(nonce[:]), nativeControlEnd(live))
	if err != nil || controllerutil.SetControllerReference(live, expected, r.Scheme) != nil {
		return nil, errNativeFollowCompletion
	}
	if err := r.nativeCleanupPreflight(ctx, live, worker, pvc, original); err != nil {
		return nil, errNativeFollowCompletion
	}
	if _, _, err := r.nativeFollowTerminalCurrent(ctx, m, worker, pvc, original); err != nil || native.ValidateEvidenceJobSize(expected) != nil || ctx.Err() != nil {
		return nil, errNativeFollowCompletion
	}
	created := expected.DeepCopy()
	if r.Create(ctx, created) != nil || created.UID == "" || created.UID == worker.UID || !nativePreflightJobMatches(expected, created) {
		return nil, errNativeFollowCompletion
	}
	binding := podexec.NativeContainerBinding{ContainerName: workerContainer, ImageID: runnerImageFor(live, r.RPGMigrateRunnerImage),
		TemplateSHA256: podexec.NativeTemplateSHA256(created.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(created.Spec.Template)}
	job, pod, err := r.nativeEndposHelperTerminal(ctx, m, worker, pvc, original, expected, created, &binding)
	if err != nil {
		return nil, errNativeFollowCompletion
	}
	read, end := context.WithTimeout(ctx, 30*time.Second)
	out, err := r.NativeJobs.JobEndposNative(read, live.Namespace, job.UID, binding)
	end()
	if err != nil || !out.Successful() || ctx.Err() != nil {
		return nil, errNativeFollowCompletion
	}
	raw := out.Payload()
	if len(raw) > 65536 || bytes.Count(raw, []byte{'\n'}) != 1 || !bytes.HasSuffix(raw, []byte{'\n'}) || !bytes.HasPrefix(raw, []byte("native-report: ")) {
		return nil, errNativeFollowCompletion
	}
	capture := &nativeEndposCapture{raw: raw, original: original, expected: expected, created: created, observed: job, pod: pod, binding: binding}
	if !r.nativeEndposHelperUnchanged(ctx, capture) {
		return nil, errNativeFollowCompletion
	}
	if _, _, err := r.nativeFollowTerminalCurrent(ctx, m, worker, pvc, original); err != nil || ctx.Err() != nil {
		return nil, errNativeFollowCompletion
	}
	return capture, nil
}

func (r *MigrationReconciler) nativeEndposHelperUnchanged(ctx context.Context, capture *nativeEndposCapture) bool {
	after, err := r.nativePreflightLiveJob(ctx, capture.expected, capture.created, capture.binding)
	if err != nil || !apiequality.Semantic.DeepEqual(capture.observed.Status, after.Status) || !nativeEvidenceWorkerComplete(after) {
		return false
	}
	confirmed, err := r.nativePreflightPod(ctx, after)
	return err == nil && confirmed != nil && nativePreflightPodMatches(after, confirmed) && nativePreflightSamePod(capture.pod, confirmed) && capture.pod.Generation == confirmed.Generation &&
		apiequality.Semantic.DeepEqual(capture.pod.Spec, confirmed.Spec) && apiequality.Semantic.DeepEqual(capture.pod.Status, confirmed.Status)
}

func (r *MigrationReconciler) nativeEndposHelperTerminal(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, expected, created *batchv1.Job, binding *podexec.NativeContainerBinding) (*batchv1.Job, *corev1.Pod, error) {
	var job *batchv1.Job
	var pod *corev1.Pod
	var helperOriginal *corev1.Pod
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		if _, _, err := r.nativeFollowTerminalCurrent(ctx, m, worker, pvc, original); err != nil {
			return false, errNativeFollowCompletion
		}
		var err error
		job, err = r.nativePreflightLiveJob(ctx, expected, created, *binding)
		if err != nil || nativePreflightJobFailed(job) {
			return false, errNativeFollowCompletion
		}
		pod, err = r.nativePreflightPod(ctx, job)
		if err != nil {
			return false, errNativeFollowCompletion
		}
		if pod == nil {
			return false, nil
		}
		if !nativePreflightPodMatches(job, pod) || !nativePreflightSamePod(helperOriginal, pod) || helperOriginal != nil && helperOriginal.Generation != pod.Generation {
			return false, errNativeFollowCompletion
		}
		helperOriginal = pod.DeepCopy()
		ready, err := nativePreflightContainerReady(pod, binding.ImageID)
		if err != nil || !ready || !nativeEvidenceWorkerComplete(job) {
			return false, err
		}
		binding.PodUID, binding.ContainerID = pod.UID, pod.Status.ContainerStatuses[0].ContainerID
		return true, nil
	})
	return job, pod, err
}

// clone is DATA from a freshly admitted complete capture and ParseCloneOutput, not target authority.
func (r *MigrationReconciler) readNativeFollowTargetCompletion(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, clone *native.Report) (*native.CompletedObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if r.NativeEvidence == nil || m == nil || worker == nil || pvc == nil {
		return nil, errNativeFollowCompletion
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil || native.ValidateCompletedClone(clone, work) != nil {
		return nil, errNativeFollowCompletion
	}
	capture, err := r.captureNativeEndposAck(ctx, m, worker, pvc)
	if err != nil {
		return nil, errNativeFollowCompletion
	}
	observation, err := r.readNativeFollowCompletion(ctx, m, worker, pvc, clone, capture.raw)
	if err != nil || observation == nil {
		return nil, errNativeFollowCompletion
	}
	if _, _, err := r.nativeFollowTerminalCurrent(ctx, m, worker, pvc, capture.original); err != nil || ctx.Err() != nil || !r.nativeEndposHelperUnchanged(ctx, capture) {
		return nil, errNativeFollowCompletion
	}
	if r.nativePreflightCleanup(ctx, m, capture.created, capture.pod) != nil {
		return nil, errNativeFollowCompletion
	}
	return observation, nil
}
