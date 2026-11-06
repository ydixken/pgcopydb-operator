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
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// The verification rehearsal (spec.verification.rehearsal) runs compare data once
// while a live migration streams, so the drain verify's duration can be sized
// before the cutover. It writes status.verificationRehearsal, never the phase,
// Verified, status.verification, or a compare or verify Job; only the drain
// verify's wait for its pod shows, in CutoverCompleted.

// reasonRehearsalPodRemains is CutoverCompleted Unknown while the drain verify waits for a rehearsal pod.
const reasonRehearsalPodRemains = "RehearsalPodRemains"

// rehearsalJobName is run n's Job. A name per run keeps ensureJob from adopting
// a finished run as the next one.
func rehearsalJobName(m *v1beta1.Migration, run int32) string {
	return fmt.Sprintf("%s-rehearsal-%d", m.Name, run)
}

func rehearsalRequested(m *v1beta1.Migration) bool {
	return m.Spec.Verification != nil && m.Spec.Verification.Rehearsal
}

// reconcileRehearsal starts and observes a rehearsal beside the running follow
// worker. A run starts once per change of rehearsal to true: armed stays true
// until rehearsal is false again and no pod of the run is left.
func (r *MigrationReconciler) reconcileRehearsal(ctx context.Context, m *v1beta1.Migration) error {
	st := m.Status.VerificationRehearsal
	armed := st != nil && st.Armed
	switch {
	case !rehearsalRequested(m):
		if !armed {
			return nil
		}
		if err := r.cancelRehearsal(ctx, m, "spec.verification.rehearsal was set to false"); err != nil {
			return err
		}
		// Re-arming before the pod has ended would run two full scans at once.
		pod, err := r.rehearsalPod(ctx, m)
		if err != nil || pod != nil {
			return err
		}
		st.Armed = false
		return nil
	case m.Status.Phase == v1beta1.PhaseCuttingOver:
		return r.cancelRehearsal(ctx, m, "the cutover started")
	case armed:
		return r.observeRehearsal(ctx, m)
	case m.Status.Phase != v1beta1.PhaseStreaming && m.Status.Phase != v1beta1.PhaseCutoverPending:
		return nil
	case m.Spec.Cutover.Mode == v1beta1.CutoverAutomatic:
		// CEL rejects Automatic, an older CRD may not: its cutover starts at catch-up.
		rehearsalNotStarted(m, "no run starts: a rehearsal needs spec.cutover.mode Manual, "+
			"because an Automatic cutover starts at catch-up and cancels it")
		return nil
	}
	if err := r.startRehearsal(ctx, m); err != nil {
		rehearsalNotStarted(m, "the next run did not start, the operator retries: "+truncate(err.Error(), maxDetailLen))
		return err
	}
	return nil
}

// rehearsalNotStarted says in status why no run started. armed stays false, so the
// next pass tries again.
func rehearsalNotStarted(m *v1beta1.Migration, why string) {
	if m.Status.VerificationRehearsal == nil {
		m.Status.VerificationRehearsal = &v1beta1.VerificationRehearsalStatus{}
	}
	m.Status.VerificationRehearsal.Message = why
}

// startRehearsal deletes the previous run's Job and creates the next run's. In that
// order, a failed delete starts no run that status cannot record.
func (r *MigrationReconciler) startRehearsal(ctx context.Context, m *v1beta1.Migration) error {
	prev := m.Status.VerificationRehearsal
	run := int32(1)
	if prev != nil {
		run = prev.Run + 1
	}
	name := rehearsalJobName(m, run)
	if prev != nil && prev.JobName != "" && prev.JobName != name {
		if err := r.deleteRehearsalJob(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Namespace: m.Namespace, Name: prev.JobName,
		}}); err != nil {
			return err
		}
	}
	if _, _, err := r.ensureJob(ctx, m, name, func() (*batchv1.Job, error) {
		return buildRehearsalJob(m, r.RunnerImage, run)
	}); err != nil {
		return err
	}
	m.Status.VerificationRehearsal = &v1beta1.VerificationRehearsalStatus{
		Armed: true, Run: run, JobName: name, Outcome: v1beta1.RehearsalRunning,
		Message: "pgcopydb compare data runs as Job " + name + " while the migration streams",
	}
	return nil
}

// observeRehearsal records a running rehearsal's times and, once its Job has
// finished, the outcome. A Job that vanished is never recreated: a second run
// would scan both databases again without being asked to.
func (r *MigrationReconciler) observeRehearsal(ctx context.Context, m *v1beta1.Migration) error {
	st := m.Status.VerificationRehearsal
	if st.Outcome != v1beta1.RehearsalRunning {
		return nil
	}
	job, err := r.rehearsalJob(ctx, m, st.JobName)
	if err != nil {
		return err
	}
	if job == nil {
		st.Outcome = v1beta1.RehearsalFailed
		st.Message = "rehearsal Job " + st.JobName + " was deleted before it finished"
		st.CompletedAt = statusTime(r.currentTime())
		return nil
	}
	// The outcome waits for a finished run's last sample while its read is retried.
	tail, err := r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, job)
	if err != nil {
		return err
	}
	recordRehearsal(m, job, tail)
	return nil
}

// cancelRehearsal stops a running rehearsal before the worker it runs beside goes:
// its sessions count in the retry gate's census and its scans compete with the drain
// verify. The foreground delete sends the pod TERM, and the script cancels its queries.
func (r *MigrationReconciler) cancelRehearsal(ctx context.Context, m *v1beta1.Migration, reason string) error {
	if err := r.cancelRecordedRehearsal(ctx, m, reason); err != nil {
		return err
	}
	// A run whose status write was lost is recorded nowhere; it must not outlive the worker either.
	jobs := &batchv1.JobList{}
	if err := r.uncached().List(ctx, jobs, client.InNamespace(m.Namespace),
		client.MatchingLabels{labelMigration: m.Name}); err != nil {
		return err
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if done, _ := jobFinished(job); done || !job.DeletionTimestamp.IsZero() ||
			!strings.HasPrefix(job.Name, m.Name+"-rehearsal-") {
			continue
		}
		if err := r.deleteRehearsalJob(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

// cancelRecordedRehearsal ends the run status records as Running: Canceled, or what its
// Job says when the Job finished first.
func (r *MigrationReconciler) cancelRecordedRehearsal(ctx context.Context, m *v1beta1.Migration, reason string) error {
	st := m.Status.VerificationRehearsal
	if st == nil || st.Outcome != v1beta1.RehearsalRunning {
		return nil
	}
	job, err := r.rehearsalJob(ctx, m, st.JobName)
	if err != nil {
		return err
	}
	if job != nil {
		if done, _ := jobFinished(job); done {
			// A cancel does not wait for the read, so its error costs only the last sample.
			tail, _ := r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, job)
			recordRehearsal(m, job, tail)
			return nil
		}
		if job.DeletionTimestamp.IsZero() {
			if err := r.deleteRehearsalJob(ctx, job); err != nil {
				return err
			}
		}
		st.StartedAt = job.Status.StartTime
	}
	st.Outcome = v1beta1.RehearsalCanceled
	st.Message = "canceled: " + reason
	st.CompletedAt = statusTime(r.currentTime())
	return nil
}

// recordRehearsal copies a rehearsal Job's times and, once it finished, its outcome,
// with the error from tail, the log readCompareProgress read. The script exits 0
// whenever compare data wrote a report, so a Job that failed produced none.
func recordRehearsal(m *v1beta1.Migration, job *batchv1.Job, tail string) {
	st := m.Status.VerificationRehearsal
	st.StartedAt = job.Status.StartTime
	done, ok := jobFinished(job)
	if !done {
		return
	}
	st.CompletedAt = statusTime(jobFinishedAt(job))
	if !ok {
		st.Outcome = v1beta1.RehearsalFailed
		st.Message = "rehearsal Job " + job.Name + " wrote no compare report: " + failureReason(job)
		// The worker's window: further up, an ERROR may be a table that differed, not the cause.
		if detail := truncate(pgcopydb.LastErrorLine([]byte(lastLines(tail, workerLogTail))), maxDetailLen); detail != "" {
			st.Message += "; last error: " + detail
		}
		return
	}
	st.Outcome = v1beta1.RehearsalCompleted
	took := ""
	if st.StartedAt != nil && st.CompletedAt != nil {
		took = " in " + st.CompletedAt.Sub(st.StartedAt.Time).Round(time.Second).String()
	}
	st.Message = "pgcopydb compare data finished" + took + " while the migration streamed;" +
		" tables that differ are expected then and are no verdict"
}

// lastLines is what a read of the last n lines of the log s would return.
func lastLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	return strings.Join(lines[max(len(lines)-n, 0):], "\n")
}

// rehearsalJob returns the named rehearsal Job, or nil once the API server, not
// only the cache, has none: a Job created a moment ago may not be cached yet.
func (r *MigrationReconciler) rehearsalJob(ctx context.Context, m *v1beta1.Migration, name string) (*batchv1.Job, error) {
	key := types.NamespacedName{Namespace: m.Namespace, Name: name}
	job := &batchv1.Job{}
	err := r.Get(ctx, key, job)
	if apierrors.IsNotFound(err) {
		err = r.uncached().Get(ctx, key, job)
	}
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return job, nil
}

// deleteRehearsalJob deletes in the foreground, so the Job outlives its pod.
func (r *MigrationReconciler) deleteRehearsalJob(ctx context.Context, job *batchv1.Job) error {
	policy := metav1.DeletePropagationForeground
	if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// drainWaitsForRehearsal holds the drain verify while a rehearsal pod has not ended and
// says so in CutoverCompleted. Once the drain is proven, a rehearsal pod holds nothing.
func (r *MigrationReconciler) drainWaitsForRehearsal(ctx context.Context, m *v1beta1.Migration) (bool, error) {
	cur := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCutoverComplete)
	if cur != nil && cur.Status == metav1.ConditionTrue {
		return false, nil
	}
	pod, err := r.rehearsalPod(ctx, m)
	if err != nil {
		return false, err
	}
	waited := cur != nil && cur.Reason == reasonRehearsalPodRemains
	if pod == nil {
		if waited {
			// The verify's result sets the condition; until then it would name a pod that is gone.
			meta.RemoveStatusCondition(&m.Status.Conditions, v1beta1.ConditionCutoverComplete)
		}
		return false, nil
	}
	msg := fmt.Sprintf("The drain verification waits for pod %s of the canceled verification rehearsal, "+
		"which is %s on node %s, because its scans would stretch the write-downtime window",
		pod.Name, podState(pod), podNode(pod))
	if !waited {
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, reasonRehearsalPodRemains, "Cutover", "%s", msg)
	}
	r.setCondition(m, v1beta1.ConditionCutoverComplete, metav1.ConditionUnknown, reasonRehearsalPodRemains, msg)
	return true, nil
}

// rehearsalPod returns a rehearsal pod of m that has not ended, or nil. Like the
// previous worker's pod, it may still hold sessions an empty census cannot show.
func (r *MigrationReconciler) rehearsalPod(ctx context.Context, m *v1beta1.Migration) (*corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(m.Namespace),
		client.MatchingLabels{labelMigration: m.Name}); err != nil {
		return nil, err
	}
	prefix := m.Name + "-rehearsal-"
	for i := range pods.Items {
		p := &pods.Items[i]
		if strings.HasPrefix(p.Labels[jobNameLabel], prefix) &&
			p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
			return p, nil
		}
	}
	return nil, nil
}
