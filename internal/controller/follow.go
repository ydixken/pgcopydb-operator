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
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// finalizerName guards replication-slot cleanup: a live migration that ever
// started holds a slot on the source, and a leaked slot retains WAL there
// without bound.
const finalizerName = "pgcopydb-operator.io/cleanup"

// defaultMaxCatchupLag applies when spec.follow.maxCatchupLag is unset.
const defaultMaxCatchupLag = int64(16 << 20)

// The CaughtUp reasons. ConfirmingCatchUp is the latch: one sample below the
// threshold parks here, and only a second consecutive one turns the condition
// true (see lagSeenBelow).
const (
	reasonLagBelowThreshold = "LagBelowThreshold"
	reasonConfirmingCatchUp = "ConfirmingCatchUp"
	reasonLagging           = "Lagging"
)

// lagSeenBelow reports whether an earlier pass already measured the lag below
// the threshold. Like copySeen, the latch lives in the condition reason, so it
// needs no API field and survives an operator restart; a sample above the
// threshold writes Lagging and clears it.
func lagSeenBelow(m *v1beta1.Migration) bool {
	c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCaughtUp)
	if c == nil {
		return false
	}
	return c.Status == metav1.ConditionTrue || c.Reason == reasonConfirmingCatchUp
}

// SentinelOps drives a running follow migration; nil disables follow handling
// (envtest injects a fake).
type SentinelOps interface {
	Read(ctx context.Context, namespace, jobName, slotName string) (*sentinel.State, error)
	SetEndposCurrent(ctx context.Context, namespace, jobName string) (string, error)
	NudgeEndpos(ctx context.Context, namespace, jobName string) error
}

func followEnabled(m *v1beta1.Migration) bool {
	return m.Spec.Follow != nil && m.Spec.Follow.Enabled
}

func effectiveSlotName(m *v1beta1.Migration) string {
	if m.Spec.Follow != nil && m.Spec.Follow.SlotName != "" {
		return m.Spec.Follow.SlotName
	}
	return pgcopydb.SlotName(m.Namespace, m.Name)
}

func maxCatchupLagBytes(m *v1beta1.Migration) int64 {
	if m.Spec.Follow != nil && m.Spec.Follow.MaxCatchupLag != nil {
		return m.Spec.Follow.MaxCatchupLag.Value()
	}
	return defaultMaxCatchupLag
}

// cutoverWanted decides whether the stream should be frozen now.
func cutoverWanted(m *v1beta1.Migration, caughtUp bool) bool {
	switch m.Spec.Cutover.Mode {
	case v1beta1.CutoverAutomatic:
		return caughtUp
	default:
		// Manual is the default mode: the user flips approved once writes to
		// the source are stopped.
		return m.Spec.Cutover.Approved
	}
}

// ensureFinalizer adds the cleanup finalizer to follow migrations before any
// worker runs, so a deletion at any later point routes through cleanup.
// Metadata-only patch, never Update: a full Update round-trips the spec
// through omitempty, stripping stored zero values (an explicit [] or ""),
// and the spec's immutability CEL rules reject that as a spec change.
func (r *MigrationReconciler) ensureFinalizer(ctx context.Context, m *v1beta1.Migration) error {
	if !followEnabled(m) || controllerutil.ContainsFinalizer(m, finalizerName) {
		return nil
	}
	base := m.DeepCopy()
	controllerutil.AddFinalizer(m, finalizerName)
	return r.Patch(ctx, m, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

// reconcileFollowRunning handles the streaming and cutover phases while the
// worker Job runs. The sample is best effort: no sample keeps the previous
// status, exactly like progress polling. cloneDone is the caller's reading of
// the base copy (the worker's own log markers); until it is true the stream
// is only reported, never acted on.
func (r *MigrationReconciler) reconcileFollowRunning(ctx context.Context, m *v1beta1.Migration, jobName string, cloneDone bool) {
	log := logf.FromContext(ctx)
	if r.Sentinel == nil {
		return
	}
	slot := effectiveSlotName(m)
	st, err := r.Sentinel.Read(ctx, m.Namespace, jobName, slot)
	if err != nil {
		log.V(1).Info("sentinel read failed", "error", err)
		return
	}
	if st == nil {
		return
	}
	rs := st.ToStatus(slot)
	if prev := m.Status.Replication; prev != nil {
		// What one pass did not learn, it must not erase. The sample is per
		// side: the source answering while the target does not (revoked
		// grants, a restarting target) yields a write_lsn with no replay
		// position, and publishing that alone would empty the lag out of the
		// CR and flip CaughtUp on a healthy stream. Endpos is carried for a
		// different reason: nothing reads it back from the worker any more,
		// so this status is the only place it lives.
		if rs.Endpos == "" {
			rs.Endpos = prev.Endpos
		}
		if rs.ReplayLSN == "" {
			rs.ReplayLSN = prev.ReplayLSN
		}
		if rs.LagBytes == nil {
			// The previous figure, not a fresh one computed from a stale
			// replay against a moved WAL head: that pair only ever reads as
			// falling behind.
			rs.LagBytes = prev.LagBytes
		}
	}
	m.Status.Replication = rs
	if !cloneDone {
		// Base copy still running; prefetch streams in the background.
		return
	}

	r.setCondition(m, v1beta1.ConditionStreaming, metav1.ConditionTrue, "Replaying",
		"logical replication is applying changes")
	m.Status.Phase = v1beta1.PhaseStreaming

	// Read the lag back off the status just written, so the condition and the
	// CR can never disagree: a pass that carried the previous figure forward
	// must carry the previous verdict with it.
	below := rs.LagBytes != nil && *rs.LagBytes <= maxCatchupLagBytes(m)

	// Two consecutive samples, because one can be measured inside a window
	// where the figure is not yet meaningful. pgcopydb's sentinel replay_lsn
	// starts at 0/0 and the override that ties the confirmed flush position to
	// the apply cursor is guarded on it being non-zero, so until the apply
	// loop's first sync the worker confirms its raw receive position instead.
	// Lag then reads near zero whatever the apply backlog is, and the fallback
	// in readScript cannot help: the walsender's replay column is NULL in that
	// same window, so it falls through to the very value that is polluted. The
	// window opens at every worker start and every worker pod restart and
	// closes within seconds, so a second sample clears it. All of it upstream
	// v0.18 code, zero guard included, so a rebase off the fork keeps it.
	// cloneDone already covers the fresh start, which leaves about one sample
	// at clone-end and after a restart. The cost is one poll interval of
	// cutover latency on a stream that really is caught up, against an endpos
	// frozen at a position the target has not actually applied to.
	caughtUp := below && lagSeenBelow(m)
	switch {
	case caughtUp:
		r.setCondition(m, v1beta1.ConditionCaughtUp, metav1.ConditionTrue, reasonLagBelowThreshold,
			"replication lag is below spec.follow.maxCatchupLag")
	case below:
		r.setCondition(m, v1beta1.ConditionCaughtUp, metav1.ConditionFalse, reasonConfirmingCatchUp,
			"one sample measured the lag below spec.follow.maxCatchupLag; CaughtUp turns true when the next one agrees")
	default:
		r.setCondition(m, v1beta1.ConditionCaughtUp, metav1.ConditionFalse, reasonLagging,
			"replication lag is above spec.follow.maxCatchupLag or unknown")
	}

	switch {
	case sentinel.EndposSet(rs.Endpos):
		// Cutover already triggered; the worker drains and exits 0.
		m.Status.Phase = v1beta1.PhaseCuttingOver
	case cutoverWanted(m, caughtUp):
		lsn, err := r.Sentinel.SetEndposCurrent(ctx, m.Namespace, jobName)
		if err != nil {
			// Transient (pod restarting): the next pass retries. Setting
			// endpos is idempotent.
			r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "CutoverRetry", "Cutover",
				"setting endpos failed, retrying: %s", err.Error())
			return
		}
		if sentinel.EndposSet(lsn) {
			m.Status.Replication.Endpos = lsn
		}
		m.Status.Phase = v1beta1.PhaseCuttingOver
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "CutoverStarted", "Cutover",
			"stream frozen at endpos %s, draining", lsn)
	case caughtUp:
		// Manual mode, waiting for approval.
		m.Status.Phase = v1beta1.PhaseCutoverPending
	}

	// Draining: pgcopydb 0.18 evaluates endpos only against WAL it receives,
	// so a fully idle source would never conclude the drain (see
	// docs/research/upstream-issues.md). One tiny logical message per pass
	// gives the receiver a record to evaluate; idempotent, harmless under
	// real traffic, best effort like the sentinel reads above.
	if m.Status.Phase == v1beta1.PhaseCuttingOver {
		if err := r.Sentinel.NudgeEndpos(ctx, m.Namespace, jobName); err != nil {
			log.V(1).Info("endpos nudge failed", "job", jobName, "error", err)
		}
	}
}

// finishFollow runs after the worker Job exited 0. Exit 0 alone is NOT
// trusted: after a crash inside the drain window, pgcopydb --resume exits 0
// without replaying pending WAL ("endpos previously reached" tracks the
// receive side). A verify Job proves the drain on the target: only origin
// progress exactly at the recorded endpos passes on the LSN, and every other
// reading, which is nearly every cutover, is decided by pgcopydb compare data
// (see buildVerifyJob); only proof gates CutoverCompleted and the cleanup.
// On refuted drain the Migration fails loudly with the slot intact, so the
// data stays recoverable (at the documented cost of WAL retention on the
// source).
func (r *MigrationReconciler) finishFollow(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "BaseCopyDone", "base copy finished")
	m.Status.Phase = v1beta1.PhaseCuttingOver

	verified, failedVerify, err := r.ensureVerify(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failedVerify {
		r.setCondition(m, v1beta1.ConditionCutoverComplete, metav1.ConditionFalse, "DrainIncomplete",
			"drain verification did not show the target holding every change below endpos; the replication slot is kept so the data is recoverable, and it retains WAL on the source until resolved")
		r.fail(m, "DrainIncomplete", "Verify",
			"cutover drain verification refuted completeness; do not switch applications to the target")
		return ctrl.Result{}, r.updateStatus(ctx, m, base)
	}
	if !verified {
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}

	r.setCondition(m, v1beta1.ConditionCutoverComplete, metav1.ConditionTrue, "DrainVerified",
		"drain verified on the target; changes applied up to endpos, sequences synced")

	done, err := r.ensureCleanup(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !done {
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}

	// Verification runs after the drain is proven and after cleanup: a data
	// compare against a target still applying WAL would mismatch by design,
	// so it must wait for the drain; and cleanup goes first because the slot
	// retains WAL on the source for as long as it exists, while the compare
	// needs no replication state at all (a mismatch never reopens the
	// stream). CutoverCompleted is already surfaced above: holding it back
	// for a long data compare would stretch the write-downtime window.
	vdone, err := r.ensureVerification(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !vdone {
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}

	now := metav1.Now()
	m.Status.CompletedAt = &now
	m.Status.Phase = v1beta1.PhaseCompleted
	r.setCondition(m, v1beta1.ConditionComplete, metav1.ConditionTrue, "MigrationSucceeded", "live migration finished")
	r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Completed", "Complete", "live migration finished")
	return ctrl.Result{}, r.updateStatus(ctx, m, base)
}

// ensureCleanup creates and observes the cleanup Job. It reports done=true
// once replication state is dropped, or when cleanup cannot run at all: after
// exhausted retries, and when the namespace is terminating (no Job can ever
// be created there again, so retrying would only deadlock namespace deletion
// against the finalizer). Both give-ups carry a loud CleanupFailed warning:
// the slot may leak on the source, and that needs an operator's attention,
// not an endlessly blocked Migration.
func (r *MigrationReconciler) ensureCleanup(ctx context.Context, m *v1beta1.Migration) (bool, error) {
	job, created, err := r.ensureJob(ctx, m, cleanupJobName(m), func() (*batchv1.Job, error) {
		return buildCleanupJob(m, r.RunnerImage)
	})
	if err != nil {
		if apierrors.IsForbidden(err) && apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
			// A source inside this namespace is deleted with it, nothing to
			// clean; a source elsewhere keeps its slot, so name it.
			r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "CleanupFailed", "Cleanup",
				"namespace %s is terminating, so the cleanup Job could not be created; a source inside this namespace is deleted with it, but a source outside it may retain replication slot %q and needs manual removal", m.Namespace, effectiveSlotName(m))
			return true, nil
		}
		return false, err
	}
	if created {
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "CleanupStarted", "Cleanup",
			"dropping replication slot, publication, and origin")
	}
	if job == nil {
		return false, nil
	}
	done, ok := jobFinished(job)
	if !done {
		return false, nil
	}
	if !ok {
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "CleanupFailed", "Cleanup",
			"stream cleanup failed after retries; the replication slot %q may be leaking WAL on the source and needs manual removal", effectiveSlotName(m))
	}
	return true, nil
}

// ensurePreflight creates and observes the preflight Job every Migration's
// first attempt gates on. Returns (passed, failureMessage, err); passed=false
// with an empty failureMessage means the check is still running. The failure
// message carries the pod's own check output (one line per failed
// prerequisite, with the exact GRANT or setting to fix it) so nobody has to
// chase pod logs of a finished Job.
func (r *MigrationReconciler) ensurePreflight(ctx context.Context, m *v1beta1.Migration) (bool, string, error) {
	job, created, err := r.ensureJob(ctx, m, preflightJobName(m), func() (*batchv1.Job, error) {
		return buildPreflightJob(m, r.RunnerImage)
	})
	if err != nil {
		return false, "", err
	}
	if created {
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "PreflightStarted", "Preflight",
			"running preflight checks as Job %s", preflightJobName(m))
	}
	if job == nil {
		return false, "", nil
	}
	done, ok := jobFinished(job)
	switch {
	case !done:
		return false, "", nil
	case ok:
		return true, "", nil
	}
	msg := "preflight failed"
	if tail := r.jobLogTail(ctx, m.Namespace, job.Name, preflightLogTail); tail != "" {
		msg += ":\n" + tail
	} else {
		msg += "; the check output was not readable, inspect the logs of Job " + job.Name
	}
	return false, msg, nil
}

// ensureVerify creates and observes the drain-verification Job. Returns
// (verified, refuted, err); (false, false, nil) means still running.
func (r *MigrationReconciler) ensureVerify(ctx context.Context, m *v1beta1.Migration) (bool, bool, error) {
	job, created, err := r.ensureJob(ctx, m, verifyJobName(m), func() (*batchv1.Job, error) {
		return buildVerifyJob(m, r.RunnerImage, r.progressGate())
	})
	if created {
		// Drop what sampleProgress estimated from the databases during the copy.
		// The worker Job is finished, so no further estimate can arrive, and this
		// Job's catalog line is the real count: recordCloneProgress waits on an
		// empty field to know it may still write one. This runs before the nil
		// check below because ensureJob reports no Job on the pass it creates
		// one, and it runs on that pass only, so a landed count is never cleared.
		//
		// The estimate's last word goes to the gauges first, squared off: the base
		// copy succeeded, and a tile left one table short for good is the confusing
		// end state. Record leaves those gauges alone while the field is empty, so
		// they hold that reading until the catalog line overwrites them.
		settleProgress(m)
		metrics.Record(m)
		m.Status.Progress = nil
	}
	if err != nil || job == nil {
		return false, false, err
	}
	done, ok := jobFinished(job)
	if !done {
		return false, false, nil
	}
	// A refuted drain carries the counters too: the Job ran, so the line is
	// there, and a failed migration is exactly when someone wants to know how
	// far the copy got.
	r.recordCloneProgress(ctx, m, job.Name)
	return ok, !ok, nil
}

// progressGate renders the verify Job's `list progress` block, or nothing
// when no poller is wired to say which pgcopydb versions may run it.
func (r *MigrationReconciler) progressGate() string {
	if r.Progress == nil {
		return ""
	}
	return r.Progress.GateScript()
}

// recordCloneProgress takes the copy counters out of a finished verify Job's
// log. This is where a follow migration's status.progress comes from: the
// worker owns its catalog until it exits, and then its pod is gone, so the
// verify Job (own pod, same work dir, worker dead) is the one place left that
// can count. Best effort in both directions: an absent or unparsable line
// leaves the field empty, and nothing here can move the drain verdict.
//
// An empty status.progress is the latch, so an unreadable log (the Job is
// finished, its pod not yet collected) is retried next pass and a landed
// count is never rewritten. finishFollow drops the copy-time estimate for
// exactly that reason: it would otherwise read as landed.
func (r *MigrationReconciler) recordCloneProgress(ctx context.Context, m *v1beta1.Migration, jobName string) {
	if m.Status.Progress != nil || r.Logs == nil {
		return
	}
	tail := r.jobLogTail(ctx, m.Namespace, jobName, verifyLogTail)
	for line := range strings.SplitSeq(tail, "\n") {
		raw, found := strings.CutPrefix(line, verifyProgressPrefix)
		if !found {
			continue
		}
		cp, err := progress.ParseListProgress([]byte(raw))
		if err != nil {
			logf.FromContext(ctx).V(1).Info("verify Job progress line did not parse", "job", jobName, "error", err)
			return
		}
		m.Status.Progress = cp
		return
	}
}

// reconcileDeletion routes deletion through cleanup for live migrations. The
// finalizer keeps the CR (and thus the owned PVC with the catalogs) alive
// until the slot is dropped; then garbage collection takes everything.
func (r *MigrationReconciler) reconcileDeletion(ctx context.Context, m *v1beta1.Migration) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(m, finalizerName) {
		metrics.Forget(m.Namespace, m.Name)
		return ctrl.Result{}, nil
	}
	// Nothing ever ran: no slot exists, nothing to clean.
	if m.Status.Attempts > 0 {
		// Stop the worker first so cleanup does not race a live stream.
		worker := &batchv1.Job{}
		if m.Status.JobName != "" {
			err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: m.Status.JobName}, worker)
			if err == nil && worker.DeletionTimestamp.IsZero() {
				policy := metav1.DeletePropagationForeground
				if err := r.Delete(ctx, worker, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: pollInterval}, nil
			}
		}
		done, err := r.ensureCleanup(ctx, m)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: pollInterval}, nil
		}
	}
	// Metadata-only patch for the same reason as ensureFinalizer: an Update
	// here would strip stored zero values from the spec and wedge deletion.
	base := m.DeepCopy()
	controllerutil.RemoveFinalizer(m, finalizerName)
	if err := r.Patch(ctx, m, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	metrics.Forget(m.Namespace, m.Name)
	return ctrl.Result{}, nil
}
