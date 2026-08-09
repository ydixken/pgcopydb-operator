/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

package controller

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// finalizerName guards replication-slot cleanup: a live migration that ever
// started holds a slot on the source, and a leaked slot retains WAL there
// without bound.
const finalizerName = "pgcopydb-operator.io/cleanup"

// defaultMaxCatchupLag applies when spec.follow.maxCatchupLag is unset.
const defaultMaxCatchupLag = int64(16 << 20)

// SentinelOps drives a running follow migration; nil disables follow handling
// (envtest injects a fake).
type SentinelOps interface {
	Read(ctx context.Context, namespace, jobName string) (*sentinel.State, error)
	SetEndposCurrent(ctx context.Context, namespace, jobName string) (string, error)
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
func (r *MigrationReconciler) ensureFinalizer(ctx context.Context, m *v1beta1.Migration) error {
	if !followEnabled(m) || controllerutil.ContainsFinalizer(m, finalizerName) {
		return nil
	}
	controllerutil.AddFinalizer(m, finalizerName)
	return r.Update(ctx, m)
}

// reconcileFollowRunning handles the streaming and cutover phases while the
// worker Job runs. The sentinel sample is best effort: no sample keeps the
// previous status, exactly like progress polling.
func (r *MigrationReconciler) reconcileFollowRunning(ctx context.Context, m *v1beta1.Migration, jobName string) {
	log := logf.FromContext(ctx)
	if r.Sentinel == nil {
		return
	}
	st, err := r.Sentinel.Read(ctx, m.Namespace, jobName)
	if err != nil {
		log.V(1).Info("sentinel read failed", "error", err)
		return
	}
	if st == nil {
		return
	}
	m.Status.Replication = st.ToStatus(effectiveSlotName(m))
	if !st.ApplyEnabled {
		// Base copy still running; prefetch streams in the background.
		return
	}

	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "BaseCopyDone",
		"base copy finished, replaying changes")
	r.setCondition(m, v1beta1.ConditionStreaming, metav1.ConditionTrue, "Replaying",
		"logical replication is applying changes")
	m.Status.Phase = v1beta1.PhaseStreaming

	lag := st.Lag()
	caughtUp := lag >= 0 && lag <= maxCatchupLagBytes(m)
	if caughtUp {
		r.setCondition(m, v1beta1.ConditionCaughtUp, metav1.ConditionTrue, "LagBelowThreshold",
			"replication lag is below spec.follow.maxCatchupLag")
	} else {
		r.setCondition(m, v1beta1.ConditionCaughtUp, metav1.ConditionFalse, "Lagging",
			"replication lag is above spec.follow.maxCatchupLag or unknown")
	}

	switch {
	case sentinel.EndposSet(st.Endpos):
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
		m.Status.Phase = v1beta1.PhaseCuttingOver
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "CutoverStarted", "Cutover",
			"stream frozen at endpos %s, draining", lsn)
	case caughtUp:
		// Manual mode, waiting for approval.
		m.Status.Phase = v1beta1.PhaseCutoverPending
	}
}

// finishFollow runs after the worker Job exited 0. Exit 0 alone is NOT
// trusted: after a crash inside the drain window, pgcopydb --resume exits 0
// without replaying pending WAL ("endpos previously reached" tracks the
// receive side). A verify Job compares the target's replication origin
// progress, the durable apply truth, against the recorded endpos; only proof
// gates CutoverCompleted and the cleanup. On refuted drain the Migration
// fails loudly with the slot intact, so the data stays recoverable (at the
// documented cost of WAL retention on the source).
func (r *MigrationReconciler) finishFollow(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "BaseCopyDone", "base copy finished")
	m.Status.Phase = v1beta1.PhaseCuttingOver

	verified, failedVerify, err := r.ensureVerify(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failedVerify {
		r.setCondition(m, v1beta1.ConditionCutoverComplete, metav1.ConditionFalse, "DrainIncomplete",
			"the worker exited before applying all changes up to endpos; the replication slot is kept so the data is recoverable, and it retains WAL on the source until resolved")
		r.fail(m, "DrainIncomplete", "Verify",
			"cutover drain verification refuted completeness; do not switch applications to the target")
		return ctrl.Result{}, r.updateStatus(ctx, m, base)
	}
	if !verified {
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval / 3}, nil
	}

	r.setCondition(m, v1beta1.ConditionCutoverComplete, metav1.ConditionTrue, "DrainVerified",
		"target origin progress reached endpos; changes applied, sequences synced")

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
		return ctrl.Result{RequeueAfter: pollInterval / 3}, nil
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

// ensurePreflight creates and observes the follow preflight Job. Returns
// (passed, failureMessage, err); passed=false with an empty failureMessage
// means the check is still running. The failure message carries the pod's own
// check output (one line per failed prerequisite, with the exact GRANT or
// setting to fix it) so nobody has to chase pod logs of a finished Job.
func (r *MigrationReconciler) ensurePreflight(ctx context.Context, m *v1beta1.Migration) (bool, string, error) {
	job, created, err := r.ensureJob(ctx, m, preflightJobName(m), func() (*batchv1.Job, error) {
		return buildPreflightJob(m, r.RunnerImage)
	})
	if err != nil {
		return false, "", err
	}
	if created {
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "PreflightStarted", "Preflight",
			"checking follow-mode prerequisites as Job %s", preflightJobName(m))
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
	msg := "follow preflight failed"
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
		return buildVerifyJob(m, r.RunnerImage)
	})
	if err != nil || job == nil {
		return false, false, err
	}
	done, ok := jobFinished(job)
	if !done {
		return false, false, nil
	}
	return ok, !ok, nil
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
				return ctrl.Result{RequeueAfter: pollInterval / 3}, nil
			}
		}
		done, err := r.ensureCleanup(ctx, m)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: pollInterval / 3}, nil
		}
	}
	controllerutil.RemoveFinalizer(m, finalizerName)
	if err := r.Update(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	metrics.Forget(m.Namespace, m.Name)
	return ctrl.Result{}, nil
}
