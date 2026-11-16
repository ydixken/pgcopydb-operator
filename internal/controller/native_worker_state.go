package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// nativeWorkerIntentAnnotation carries <attempt>/<nonce> of status.nativeWorkerIntent; it is part of the digest.
	nativeWorkerIntentAnnotation = "pgcopydb-operator.io/native-worker-intent"
	// nativeWorkerMissingAnnotation marks nativeMissingWorker, which no API read returns because it has no resourceVersion.
	nativeWorkerMissingAnnotation = "pgcopydb-operator.io/native-worker-missing"
	nativeWorkerMissingValue      = "gone"
	nativeWorkerDropWait          = 60 * time.Second
)

// The recorded worker W of attempt n, as one pass finds it.
type nativeWorkerPhase int

const (
	workerNone        nativeWorkerPhase = iota // attempt 0, nothing recorded
	workerAdmitted                             // the live Job matches its receipt and admission digest
	workerGone                                 // the recorded Job is NotFound; worker is nativeMissingWorker
	workerTerminating                          // the recorded Job is being deleted or counts terminating pods
	workerModified                             // the recorded Job no longer matches its admission digest
	workerForeign                              // the recorded name is held by another UID
)

// The next-name Job N of attempt n+1, which has no receipt.
type nativeNextPhase int

const (
	nextAbsent      nativeNextPhase = iota
	nextAbandoned                   // absent, with an intent for it recorded
	nextAdoptable                   // owned, and its annotation and digest equal the intent
	nextUnadmitted                  // owned, without a matching intent
	nextTerminating                 // owned and being deleted
	nextForeign                     // not controlled by this Migration
)

// The work PVC V.
type nativeVolumePhase int

const (
	volumeAbsent nativeVolumePhase = iota
	volumeBound                    // this Migration's claim, and the recorded one once a receipt exists
	volumeLost                     // missing past attempt 0, deleting, foreign or another UID
)

// nativeWorkerState is the W, N and V a pass resolved; nativeReconcileResources returns it instead of refusing.
type nativeWorkerState struct {
	w      nativeWorkerPhase
	worker *batchv1.Job // live W when admitted, modified or terminating; nativeMissingWorker when gone
	n      nativeNextPhase
	next   *batchv1.Job
	v      nativeVolumePhase
	pvc    *corev1.PersistentVolumeClaim // only when bound
}

// nativeWorkerEnded reports a Job the Job controller has finished: no active or terminating pod and a terminal condition.
func nativeWorkerEnded(job *batchv1.Job) bool {
	return job != nil && (nativeEvidenceWorkerComplete(job) || nativeRecoveryWorkerFailed(job))
}

// nativeMissingWorker stands for the recorded worker Job after it is gone: only the receipt's name and UID.
func nativeMissingWorker(m *v1beta1.Migration) *batchv1.Job {
	return nativeGoneJob(m.Namespace, m.Status.JobName, types.UID(m.Status.NativeJobUID))
}

func nativeGoneJob(namespace, name string, uid types.UID) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: uid, Annotations: map[string]string{nativeWorkerMissingAnnotation: nativeWorkerMissingValue}}}
}

func nativeWorkerMissing(job *batchv1.Job) bool {
	return job != nil && job.ResourceVersion == "" && job.Annotations[nativeWorkerMissingAnnotation] == nativeWorkerMissingValue
}

// nativeJobSHA256 is the admission digest: SHA-256 over exactly what nativePreflightJobMatches compares,
// after its normalization. shortcut: quantities compare in serialized form, so a format-only edit of a
// never-started Job reads as modified; upgrade if a client ever rewrites quantity formats.
func nativeJobSHA256(job *batchv1.Job) (string, error) {
	if job == nil {
		return "", errNativeWorkerAdmission
	}
	j := job.DeepCopy()
	if !nativeWorkerControllerFields(j) || j.Spec.Template.Spec.RuntimeClassName != nil {
		return "", errNativeWorkerAdmission
	}
	nativeWorkerDefaults(j)
	if j.Spec.Template.Spec.HostUsers == nil {
		j.Spec.Template.Spec.HostUsers = ptr.To(true)
	}
	raw, err := json.Marshal(struct {
		Name, Namespace, GenerateName string
		Labels, Annotations           map[string]string
		OwnerReferences               []metav1.OwnerReference
		Finalizers                    []string
		Spec                          batchv1.JobSpec
	}{j.Name, j.Namespace, j.GenerateName, j.Labels, j.Annotations, j.OwnerReferences, j.Finalizers, j.Spec})
	if err != nil {
		return "", errNativeWorkerAdmission
	}
	var tree any
	if json.Unmarshal(raw, &tree) != nil {
		return "", errNativeWorkerAdmission
	}
	// Semantic equality treats nil and empty slices and maps alike, so the digest drops both.
	if raw, err = json.Marshal(nativeDigestPrune(tree)); err != nil {
		return "", errNativeWorkerAdmission
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func nativeDigestPrune(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, value := range t {
			if value = nativeDigestPrune(value); nativeDigestEmpty(value) {
				delete(t, k)
			} else {
				t[k] = value
			}
		}
	case []any:
		for i := range t {
			t[i] = nativeDigestPrune(t[i])
		}
	}
	return v
}

func nativeDigestEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	}
	return false
}

// nativeWorkerAdmitted binds a live worker Job to its receipt and to the bytes admitted at its Create,
// so a later spec edit applies from the next attempt instead of refusing this one.
func nativeWorkerAdmitted(m *v1beta1.Migration, job *batchv1.Job) bool {
	if m == nil || job == nil || job.UID == "" || string(job.UID) != m.Status.NativeJobUID || job.Name != m.Status.JobName || job.Namespace != m.Namespace ||
		!job.DeletionTimestamp.IsZero() || m.Status.NativeJobSHA256 == "" || !nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
		return false
	}
	sum, err := nativeJobSHA256(job)
	return err == nil && sum == m.Status.NativeJobSHA256
}

// nativeNewIntent returns the status intent and the Job annotation for attempt with a fresh nonce.
func nativeNewIntent(attempt int32) (annotation string, err error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", errNativeWorkerAdmission
	}
	return strconv.FormatInt(int64(attempt), 10) + "/" + hex.EncodeToString(nonce[:]), nil
}

func nativeIntentParts(intent string) (attempt int32, annotation, digest string, ok bool) {
	parts := strings.Split(intent, "/")
	if len(parts) != 3 || len(parts[1]) != 32 || len(parts[2]) != 64 || strings.Trim(parts[1]+parts[2], "0123456789abcdef") != "" {
		return 0, "", "", false
	}
	n, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil || n < 1 || strconv.FormatInt(n, 10) != parts[0] {
		return 0, "", "", false
	}
	return int32(n), parts[0] + "/" + parts[1], parts[2], true
}

// nativeIntentAdmits reports whether job is the worker the recorded intent names: the next attempt's
// canonical name, the intent's nonce annotation and the intent's digest.
func nativeIntentAdmits(m *v1beta1.Migration, job *batchv1.Job) bool {
	attempt, annotation, digest, ok := nativeIntentParts(m.Status.NativeWorkerIntent)
	if !ok || m.Status.Attempts < 0 || attempt != m.Status.Attempts+1 || job == nil || job.Name != jobName(m, attempt) || job.Namespace != m.Namespace ||
		job.Annotations[nativeWorkerIntentAnnotation] != annotation || !nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
		return false
	}
	sum, err := nativeJobSHA256(job)
	return err == nil && sum == digest
}

// nativeWorkerBusy is R1's refusal: an engine may still run in the named Job or pod.
type nativeWorkerBusy struct{ job, pod, node, phase string }

func (e *nativeWorkerBusy) Error() string { return errNativeRecoveryActive.Error() }

func (e *nativeWorkerBusy) Is(target error) bool { return target == errNativeRecoveryActive }

func (e *nativeWorkerBusy) message(attempt int32) string {
	if e.pod == "" {
		return fmt.Sprintf("Native worker Job %s has not ended; attempt %d waits, because its engine may still run", e.job, attempt)
	}
	node := e.node
	if node == "" {
		node = "<none>"
	}
	return fmt.Sprintf("Pod %s of native worker Job %s is %s on node %s; attempt %d waits until it has ended, because its engine may still run", e.pod, e.job, e.phase, node, attempt)
}

// nativePodlessAnchor records that a finished worker had no pod. It has no UID, so every pod
// matcher and podexec.SamePodState refuse it if it is ever read as a pod.
func nativePodlessAnchor() *corev1.Pod { return &corev1.Pod{} }

func nativePodless(pod *corev1.Pod) bool { return pod != nil && pod.UID == "" }

// nativeSameWorkerPod compares an anchored worker pod with a later observation: a nil original is
// the first observation, the pod-less anchor stays pod-less, and a present pod stays the same pod.
func nativeSameWorkerPod(original, pod *corev1.Pod) bool {
	if original == nil {
		return true
	}
	if pod == nil {
		return false
	}
	if nativePodless(original) || nativePodless(pod) {
		return nativePodless(original) && nativePodless(pod)
	}
	return nativePreflightSamePod(original, pod) && original.Generation == pod.Generation &&
		apiequality.Semantic.DeepEqual(original.Spec, pod.Spec) && podexec.SamePodState(original, pod, workerContainer)
}

// nativeWorkerEngineFree is R1: job is gone or ended, and no pod that carries its UID as owner or
// controller-uid label is unfinished, in one consistent uncached list. It returns the Job's one
// owned pod, or the pod-less anchor when there is none.
func (r *MigrationReconciler) nativeWorkerEngineFree(ctx context.Context, job *batchv1.Job) (*corev1.Pod, error) {
	if r.APIReader == nil || job == nil || job.UID == "" {
		return nil, errNativeRecovery
	}
	missing := nativeWorkerMissing(job)
	if !missing && (!job.DeletionTimestamp.IsZero() || job.Status.Terminating != nil && *job.Status.Terminating != 0 || !nativeWorkerEnded(job)) {
		return nil, &nativeWorkerBusy{job: job.Name}
	}
	pods, err := r.nativePreflightPods(ctx, job.Namespace)
	if err != nil {
		return nil, errNativeRecovery
	}
	var owned *corev1.Pod
	for i := range pods {
		p := &pods[i]
		byOwner := slices.ContainsFunc(p.OwnerReferences, func(o metav1.OwnerReference) bool { return o.UID == job.UID })
		byLabel := p.Labels[batchv1.ControllerUidLabel] == string(job.UID) || p.Labels[nativeWorkerLegacyControllerLabel] == string(job.UID)
		if !byOwner && !byLabel {
			continue
		}
		if p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed || !p.DeletionTimestamp.IsZero() {
			return nil, &nativeWorkerBusy{job: job.Name, pod: p.Name, node: p.Spec.NodeName, phase: string(p.Status.Phase)}
		}
		if byOwner {
			if owned != nil {
				return nil, errNativeRecovery
			}
			owned = p
		}
	}
	if owned == nil || missing {
		// A finished Job never gets another pod (Job docs), so no pod means no engine.
		return nativePodlessAnchor(), nil
	}
	pod := &corev1.Pod{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(owned), pod) != nil || pod.UID == "" || pod.UID != owned.UID || pod.Name != owned.Name || pod.Namespace != job.Namespace {
		return nil, errNativeRecovery
	}
	return pod, nil
}

// dropNativeWorker deletes a worker Job of this Migration with a UID precondition and foreground
// propagation, and reports whether the Job and every pod carrying its UID are gone.
func (r *MigrationReconciler) dropNativeWorker(ctx context.Context, m *v1beta1.Migration, job *batchv1.Job) (bool, error) {
	if r.Client == nil || r.APIReader == nil || m == nil || job == nil || job.UID == "" || job.Namespace != m.Namespace {
		return false, errNativeRecovery
	}
	ctx, cancel := context.WithTimeout(ctx, nativeWorkerDropWait)
	defer cancel()
	if !nativeWorkerMissing(job) {
		if !nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
			return false, errNativeRecovery
		}
		err := r.Delete(ctx, job, client.Preconditions{UID: &job.UID}, client.PropagationPolicy(metav1.DeletePropagationForeground))
		if err != nil && !apierrors.IsNotFound(err) {
			return false, errNativeRecovery
		}
	}
	err := wait.PollUntilContextCancel(ctx, nativePreflightPoll, true, func(ctx context.Context) (bool, error) {
		current := &batchv1.Job{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(job), current); err == nil {
			return false, nil
		} else if !apierrors.IsNotFound(err) {
			return false, errNativeRecovery
		}
		_, err := r.nativeWorkerEngineFree(ctx, nativeGoneJob(job.Namespace, job.Name, job.UID))
		if errors.Is(err, errNativeRecoveryActive) {
			return false, nil
		}
		return err == nil, err
	})
	if err != nil {
		if ctx.Err() != nil {
			// Not gone within the pass; the next pass sees it terminating or gone.
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// stopOrDropNativeWorker is the stop of lifecycle 7: TERM to a bound running container, and a drop
// when no container is bound or it has not ended within the pass. It reports when W is engine-free.
func (r *MigrationReconciler) stopOrDropNativeWorker(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	if nativeWorkerMissing(worker) {
		return r.dropNativeWorker(ctx, m, worker)
	}
	if _, err := r.nativeStopCurrent(ctx, m, worker, pvc, nil); err == nil {
		if done, err := r.stopNativeRetained(ctx, m, worker, pvc); err == nil && done {
			return true, nil
		}
	}
	return r.dropNativeWorker(ctx, m, worker)
}

// countNativeAttempt marks the recorded attempt failed and counted; the retry gate decides the next one.
func (r *MigrationReconciler) countNativeAttempt(m *v1beta1.Migration, failure string) {
	m.Status.RetryGate = &v1beta1.RetryGateStatus{FailedAttempt: m.Status.Attempts, FailedAt: metav1.NewTime(r.currentTime()), Failure: failure, Exemption: v1beta1.RetryGateCounted}
	r.nativeCloneAttemptEnded(m, reasonAttemptFailed)
	r.setCondition(m, v1beta1.ConditionRetryHeld, metav1.ConditionTrue, reasonGateProbing, "Native retry waits for fresh target authority")
}

// holdNative writes Blocked=True with reason and requeues; a repeated hold writes nothing.
func (r *MigrationReconciler) holdNative(ctx context.Context, m, base *v1beta1.Migration, reason, msg string) (bool, ctrl.Result, error) {
	desired := m.DeepCopy()
	r.setNativeBlocked(desired, reason, msg)
	if !apiequality.Semantic.DeepEqual(desired.Status, m.Status) {
		if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return true, ctrl.Result{}, nativeWorkerError(err)
		}
		desired.DeepCopyInto(m)
	}
	return true, ctrl.Result{RequeueAfter: pollInterval}, nil
}
