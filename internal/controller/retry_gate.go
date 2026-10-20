/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
)

// gatePassFreshness bounds the age of a completion that counts as a pass, so a pass from
// before a spec change or an operator outage never starts a worker.
const gatePassFreshness = 60 * time.Second

// uncached reads past the informer cache, where a stale read could start a second worker.
func (r *MigrationReconciler) uncached() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// onFailurePath reports a retry after a failed attempt, the one path the gate can exempt.
func onFailurePath(m *v1beta1.Migration) bool {
	return m.Status.RetryGate != nil && m.Status.RetryGate.FailedAttempt == m.Status.Attempts
}

// retryGate holds attempt n until the previous worker's pod has ended and a fresh probe of
// the target passed; passed means the worker may start. While held it writes the hold and
// requeues; the Job watch wakes it earlier when a probe finishes.
func (r *MigrationReconciler) retryGate(ctx context.Context, m, base *v1beta1.Migration, attempt int32) (res ctrl.Result, passed bool, err error) {
	// Before the pod check: time behind WorkerPodRemains counts toward the deadline too.
	if onFailurePath(m) && verdictOverdue(m.Status.RetryGate, r.currentTime()) {
		if failed, err := r.decide(ctx, m, base, v1beta1.RetryGateCounted); err != nil || failed {
			return ctrl.Result{}, false, err
		}
	}
	held := ctrl.Result{RequeueAfter: retryGateInterval}
	pod, err := r.previousWorkerPod(ctx, m, attempt)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if pod != nil {
		node := pod.Spec.NodeName
		if node == "" {
			node = "(none yet)"
		}
		r.setHold(m, reasonWorkerPodRemains, fmt.Sprintf(
			"Pod %s of attempt %d is %s on node %s; attempt %d waits until it has ended, because it may still hold sessions",
			pod.Name, attempt-1, podState(pod), node, attempt))
		return held, false, r.updateStatus(ctx, m, base)
	}

	name := gateJobName(m, attempt)
	probing := fmt.Sprintf("Attempt %d waits for the retry gate: probe Job %s checks the target", attempt, name)
	job := &batchv1.Job{}
	err = r.uncached().Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: name}, job)
	switch {
	case apierrors.IsNotFound(err):
		if err := r.createGateJob(ctx, m, attempt); err != nil {
			return ctrl.Result{}, false, err
		}
		r.probing(m, probing)
		return held, false, r.updateStatus(ctx, m, base)
	case err != nil:
		return ctrl.Result{}, false, err
	case !job.DeletionTimestamp.IsZero():
		r.probing(m, probing)
		return held, false, r.updateStatus(ctx, m, base)
	}
	if done, _ := jobFinished(job); !done {
		detail := r.podWaitDetail(ctx, m.Namespace, client.MatchingLabels{batchv1.ControllerUidLabel: string(job.UID)})
		if detail != "" {
			r.setHold(m, reasonProbeFailed, "The pod of probe Job "+name+" cannot start: "+truncate(detail, maxDetailLen))
		} else {
			r.probing(m, probing)
		}
		return held, false, r.updateStatus(ctx, m, base)
	}
	return r.judgeProbe(ctx, m, base, job, probing)
}

// judgeProbe reads a finished probe. Only a fresh completion built from the current inputs
// whose log shows every enabled check passing is a pass; any other result holds.
func (r *MigrationReconciler) judgeProbe(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job, probing string) (ctrl.Result, bool, error) {
	held := ctrl.Result{RequeueAfter: retryGateInterval}
	now := r.currentTime()
	_, ok := jobFinished(job)
	in := gateInputsFor(m)
	current := job.Annotations[annotationRetryGateInputs] == in.String()
	finishedAt := gateFinishedAt(job)
	if ok && (!current || now.After(finishedAt.Add(gatePassFreshness))) {
		if err := r.deleteGateProbe(ctx, job); err != nil {
			return ctrl.Result{}, false, err
		}
		r.probing(m, probing)
		return held, false, r.updateStatus(ctx, m, base)
	}

	raw, readErr := r.gateLog(ctx, job)
	result := parseGateLog(raw)
	if ok && readErr == nil && result.passed(in) {
		return ctrl.Result{}, true, nil
	}
	if rg := m.Status.RetryGate; onFailurePath(m) && rg.Exemption == "" && result.Census != nil {
		if failed, err := r.decide(ctx, m, base, exemptionVerdict(*result.Census, rg.FailedAt.Time)); err != nil || failed {
			return ctrl.Result{}, false, err
		}
	}
	reason, msg := result.hold(in, &m.Spec.Target)
	switch {
	case readErr != nil:
		reason, msg = reasonProbeFailed, "The log of probe Job "+job.Name+" was not readable: "+truncate(readErr.Error(), maxDetailLen)
	case reason == "":
		reason, msg = reasonProbeFailed, "The probe logged a pass"
	}
	if !ok && reason == reasonProbeFailed {
		msg += ". Job " + job.Name + " failed: " + failureReason(job)
	}
	r.setHold(m, reason, msg)

	// Keeping the failed probe paces the loop; a probe from other inputs goes at once.
	if keep := finishedAt.Add(retryGateInterval).Sub(now); current && keep > 0 {
		held.RequeueAfter = min(keep, retryGateInterval)
	} else if err := r.deleteGateProbe(ctx, job); err != nil {
		return ctrl.Result{}, false, err
	}
	return held, false, r.updateStatus(ctx, m, base)
}

// previousWorkerPod returns a pod of the previous attempt's worker that has not ended, or nil.
// An empty census does not prove it gone: a terminating pod may hold only source connections.
func (r *MigrationReconciler) previousWorkerPod(ctx context.Context, m *v1beta1.Migration, attempt int32) (*corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(m.Namespace),
		client.MatchingLabels{jobNameLabel: jobName(m, attempt-1)}); err != nil {
		return nil, err
	}
	for i := range pods.Items {
		if p := &pods.Items[i]; p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
			return p, nil
		}
	}
	return nil, nil
}

func podState(p *corev1.Pod) string {
	state := strings.ToLower(string(p.Status.Phase))
	if state == "" {
		state = "pending"
	}
	if !p.DeletionTimestamp.IsZero() {
		state += " and terminating"
	}
	return state
}

func (r *MigrationReconciler) createGateJob(ctx context.Context, m *v1beta1.Migration, attempt int32) error {
	job, err := buildGateJob(m, r.RunnerImage, attempt)
	if err != nil {
		return err
	}
	// The owner reference lets the Job watch wake the gate when the probe finishes, and
	// garbage collection remove the probes of a deleted Migration.
	if err := controllerutil.SetControllerReference(m, job, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// gateLog is the probe's log, read by Job UID: every probe of one attempt reuses the name.
func (r *MigrationReconciler) gateLog(ctx context.Context, job *batchv1.Job) (string, error) {
	if r.Logs == nil {
		return "", errors.New("no pod log reader is configured")
	}
	raw, err := r.Logs.JobLogsByUID(ctx, job.Namespace, job.UID, gateLogTail)
	return string(raw), err
}

// gateFinishedAt is when the probe failed or completed, zero when the Job records neither.
func gateFinishedAt(job *batchv1.Job) time.Time {
	if t := jobFailedAt(job); !t.IsZero() {
		return t
	}
	if job.Status.CompletionTime != nil {
		return job.Status.CompletionTime.Time
	}
	return time.Time{}
}

func (r *MigrationReconciler) deleteGateProbe(ctx context.Context, job *batchv1.Job) error {
	policy := metav1.DeletePropagationBackground
	if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// deleteGateJob removes attempt n's probe once the worker it cleared is recorded.
func (r *MigrationReconciler) deleteGateJob(ctx context.Context, m *v1beta1.Migration, attempt int32) error {
	return r.deleteGateProbe(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: gateJobName(m, attempt)}})
}

// setHold holds the next attempt. The Warning goes out when a hold gets its first result and
// when its reason changes; GateProbing, which every retry passes through, emits nothing.
func (r *MigrationReconciler) setHold(m *v1beta1.Migration, reason, msg string) {
	cur := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionRetryHeld)
	wasHeld := cur != nil && cur.Status == metav1.ConditionTrue
	if reason != reasonGateProbing && (!wasHeld || cur.Reason != reason) {
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, v1beta1.ConditionRetryHeld, "Retry", "%s", truncate(msg, maxDetailLen))
	}
	cond := metav1.Condition{
		Type: v1beta1.ConditionRetryHeld, Status: metav1.ConditionTrue, Reason: reason, Message: msg,
		ObservedGeneration: m.Generation,
	}
	if !wasHeld {
		// On the reconciler's clock, which the AttemptStarted note measures the hold with.
		cond.LastTransitionTime = metav1.NewTime(r.currentTime())
	}
	meta.SetStatusCondition(&m.Status.Conditions, cond)
	// Only startAttempt replaces Suspended, and the gate defers it: a resumed Migration that
	// holds would otherwise read Suspended and stay invisible to the alert.
	if m.Status.Phase == v1beta1.PhaseSuspended && !m.Spec.Suspend {
		m.Status.Phase = attemptPhase(m)
	}
}

// probing holds while a probe is pending. A reason an earlier probe gave stands until a
// result differs, so a new probe every 30 s does not flap it.
func (r *MigrationReconciler) probing(m *v1beta1.Migration, msg string) {
	if c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionRetryHeld); c != nil &&
		c.Status == metav1.ConditionTrue && c.Reason != reasonGateProbing && c.Reason != reasonWorkerPodRemains {
		r.setHold(m, c.Reason, c.Message)
		return
	}
	r.setHold(m, reasonGateProbing, msg)
}

// decide records the verdict and, when it is Counted with the budget spent, fails the
// Migration in the same patch. failed reports that failure.
func (r *MigrationReconciler) decide(ctx context.Context, m, base *v1beta1.Migration, verdict string) (failed bool, err error) {
	if verdict == v1beta1.RetryGateCounted && budgetSpent(m) {
		r.failBudget(m)
		failed = true
	}
	return failed, r.recordVerdict(ctx, m, base, verdict)
}

// recordVerdict sets the exemption verdict, and for Exempt refunds the attempt. The lock
// makes a pass working from a stale cache conflict instead of deciding twice.
func (r *MigrationReconciler) recordVerdict(ctx context.Context, m, base *v1beta1.Migration, verdict string) error {
	m.Status.RetryGate.Exemption = verdict
	if verdict == v1beta1.RetryGateExempt {
		m.Status.ExemptAttempts++
	}
	return r.patchStatusLocked(ctx, m, base)
}

// settleBudget is the patch that must land before a worker starts: Counted for an undecided
// verdict, and a failed Migration once counted attempts reach backoffLimit+1. The lock makes
// a stale base conflict here even when nothing changes, so no stale pass starts a worker.
func (r *MigrationReconciler) settleBudget(ctx context.Context, m, base *v1beta1.Migration) (failed bool, err error) {
	if onFailurePath(m) && m.Status.RetryGate.Exemption == "" {
		m.Status.RetryGate.Exemption = v1beta1.RetryGateCounted
	}
	if budgetSpent(m) {
		r.failBudget(m)
		failed = true
	}
	return failed, r.patchStatusLocked(ctx, m, base)
}

// failBudget fails a Migration whose retry budget is spent. A recorded failed attempt also
// fails the copy, as a spent budget did before the gate existed.
func (r *MigrationReconciler) failBudget(m *v1beta1.Migration) {
	if rg := m.Status.RetryGate; rg != nil {
		r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, "CloneFailed", rg.Failure)
	}
	r.fail(m, "BackoffLimitExceeded", "Fail", budgetMessage(m))
}

// patchStatusLocked is updateStatus with an optimistic lock. base follows the write, so a
// later patch in the same pass diffs against what the server now holds.
func (r *MigrationReconciler) patchStatusLocked(ctx context.Context, m, base *v1beta1.Migration) error {
	m.Status.ObservedGeneration = m.Generation
	if err := r.Status().Patch(ctx, m, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	metrics.Record(m)
	m.DeepCopyInto(base)
	return nil
}
