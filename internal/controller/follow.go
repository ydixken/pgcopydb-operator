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

// finalizerName guards replication-slot cleanup: a leaked slot retains WAL on
// the source without bound.
const finalizerName = "pgcopydb-operator.io/cleanup"

// defaultMaxCatchupLag applies when spec.follow.maxCatchupLag is unset.
const defaultMaxCatchupLag = int64(16 << 20)

// The CaughtUp reasons. ConfirmingCatchUp is the latch: one sample below the
// threshold parks here, and only a second consecutive one turns it true.
const (
	reasonLagBelowThreshold = "LagBelowThreshold"
	reasonConfirmingCatchUp = "ConfirmingCatchUp"
	reasonLagging           = "Lagging"
)

// lagSeenBelow reports whether an earlier pass already measured the lag below
// the threshold. Like copySeen, the latch lives in the condition reason, so it
// needs no API field and survives an operator restart.
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
		return m.Spec.Cutover.Approved && caughtUp
	}
}

// ensureFinalizer adds the cleanup finalizer to follow migrations before any
// worker runs, so a deletion at any later point routes through cleanup.
// Metadata-only patch, never Update: an Update strips stored zero values from
// the spec through omitempty, which the immutability CEL rules then reject.
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
// status. Until cloneDone (see confirmBaseCopy) the stream is only reported,
// never acted on.
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
		// The sample is per side, and a target that does not answer (revoked
		// grants, a restart) yields a write_lsn with no replay position:
		// publishing that alone would empty the lag out of the CR and flip
		// CaughtUp on a healthy stream. Endpos is carried because nothing
		// reads it back from the worker, so this status is its only copy.
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

	// Two consecutive samples, because a sentinel replay_lsn still at 0/0
	// carries the raw receive position, so one sample taken before the apply
	// loop's first sync reads near-zero lag whatever the backlog is (see
	// docs/design/follow-diagnostics.md). The window opens at every worker
	// start and pod restart and closes within seconds, so the second sample
	// costs one poll interval of cutover latency at most.
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

	// Some worker versions need new WAL to observe a freshly set endpos.
	// Emit a harmless logical message while draining; failures retry next pass.
	if m.Status.Phase == v1beta1.PhaseCuttingOver {
		if err := r.Sentinel.NudgeEndpos(ctx, m.Namespace, jobName); err != nil {
			log.V(1).Info("endpos nudge failed", "job", jobName, "error", err)
		}
	}
}

// finishFollow runs after the worker Job exited 0, which alone is NOT trusted:
// after a crash inside the drain window pgcopydb --resume exits 0 without
// replaying pending WAL, so a verify Job proves the drain on the target
// instead (see buildVerifyJob). A refuted drain or a failed handover fails the
// Migration with the slot intact, at the cost of WAL retention on the source.
func (r *MigrationReconciler) finishFollow(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	// A refused marker stays refused (see confirmBaseCopy): the worker exiting
	// 0 is the same word the marker was, and the verify Job decides by content.
	if !tablesEmptySeen(m) {
		r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "BaseCopyDone", "base copy finished")
	}
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

	// CutoverCompleted is the signal to point applications at the target, so
	// ownership must already be right when it turns True.
	if res, handled, err := r.reownGate(ctx, m, base, v1beta1.PhaseCuttingOver,
		"; the replication slot is kept, so the migration stays recoverable. "+
			"Deleting the Migration runs the cleanup Job and releases the slot"); handled || err != nil {
		return res, err
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

	// Verification runs last: a compare against a target still applying WAL
	// would mismatch by design, cleanup before it releases the slot retaining
	// WAL on the source, and CutoverCompleted is already set above so a long
	// compare does not stretch the write-downtime window.
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

// ensureCleanup creates and observes the cleanup Job. done=true once the
// replication state is dropped, and when it cannot run at all: retries
// exhausted, or a terminating namespace, where retrying would only deadlock
// namespace deletion against the finalizer. Both give-ups emit CleanupFailed.
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
// with an empty failureMessage means the check is still running. The message
// repeats the pod's check output, so the Job's logs are not the only copy.
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
	job, _, err := r.ensureJob(ctx, m, verifyJobName(m), func() (*batchv1.Job, error) {
		return buildVerifyJob(m, r.RunnerImage, r.progressGate())
	})
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
// log, reporting whether a line parsed; ensureCatalogCheck gates on that. The
// worker's pod is gone by then, so that Job is the one place left that can
// count, and nothing latches the field, so a lost status patch cannot leave
// the copy-time estimate standing as the final figure
// (see docs/research/measurements.md#a-stale-estimate-outlived-the-catalog-that-produced-it).
func (r *MigrationReconciler) recordCloneProgress(ctx context.Context, m *v1beta1.Migration, jobName string) bool {
	if r.Logs == nil {
		return false
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
			return false
		}
		m.Status.Progress = cp
		return true
	}
	return false
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
