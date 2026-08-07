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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
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

func followEnabled(m *v1alpha1.Migration) bool {
	return m.Spec.Follow != nil && m.Spec.Follow.Enabled
}

func effectiveSlotName(m *v1alpha1.Migration) string {
	if m.Spec.Follow != nil && m.Spec.Follow.SlotName != "" {
		return m.Spec.Follow.SlotName
	}
	return pgcopydb.SlotName(m.Namespace, m.Name)
}

func maxCatchupLagBytes(m *v1alpha1.Migration) int64 {
	if m.Spec.Follow != nil && m.Spec.Follow.MaxCatchupLag != nil {
		return m.Spec.Follow.MaxCatchupLag.Value()
	}
	return defaultMaxCatchupLag
}

// cutoverWanted decides whether the stream should be frozen now.
func cutoverWanted(m *v1alpha1.Migration, caughtUp bool) bool {
	switch m.Spec.Cutover.Mode {
	case v1alpha1.CutoverAutomatic:
		return caughtUp
	default:
		// Manual is the default mode: the user flips approved once writes to
		// the source are stopped.
		return m.Spec.Cutover.Approved
	}
}

// ensureFinalizer adds the cleanup finalizer to follow migrations before any
// worker runs, so a deletion at any later point routes through cleanup.
func (r *MigrationReconciler) ensureFinalizer(ctx context.Context, m *v1alpha1.Migration) error {
	if !followEnabled(m) || controllerutil.ContainsFinalizer(m, finalizerName) {
		return nil
	}
	controllerutil.AddFinalizer(m, finalizerName)
	return r.Update(ctx, m)
}

// reconcileFollowRunning handles the streaming and cutover phases while the
// worker Job runs. The sentinel sample is best effort: no sample keeps the
// previous status, exactly like progress polling.
func (r *MigrationReconciler) reconcileFollowRunning(ctx context.Context, m *v1alpha1.Migration, jobName string) {
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

	r.setCondition(m, v1alpha1.ConditionCloneCompleted, metav1.ConditionTrue, "BaseCopyDone",
		"base copy finished, replaying changes")
	r.setCondition(m, v1alpha1.ConditionStreaming, metav1.ConditionTrue, "Replaying",
		"logical replication is applying changes")
	m.Status.Phase = v1alpha1.PhaseStreaming

	lag := st.Lag()
	caughtUp := lag >= 0 && lag <= maxCatchupLagBytes(m)
	if caughtUp {
		r.setCondition(m, v1alpha1.ConditionCaughtUp, metav1.ConditionTrue, "LagBelowThreshold",
			"replication lag is below spec.follow.maxCatchupLag")
	} else {
		r.setCondition(m, v1alpha1.ConditionCaughtUp, metav1.ConditionFalse, "Lagging",
			"replication lag is above spec.follow.maxCatchupLag or unknown")
	}

	endposSet := st.Endpos != "" && st.Endpos != "0/0"
	switch {
	case endposSet:
		// Cutover already triggered; the worker drains and exits 0.
		m.Status.Phase = v1alpha1.PhaseCuttingOver
	case cutoverWanted(m, caughtUp):
		lsn, err := r.Sentinel.SetEndposCurrent(ctx, m.Namespace, jobName)
		if err != nil {
			// Transient (pod restarting): the next pass retries. Setting
			// endpos is idempotent.
			r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "CutoverRetry", "Cutover",
				"setting endpos failed, retrying: %s", err.Error())
			return
		}
		m.Status.Phase = v1alpha1.PhaseCuttingOver
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "CutoverStarted", "Cutover",
			"stream frozen at endpos %s, draining", lsn)
	case caughtUp:
		// Manual mode, waiting for approval.
		m.Status.Phase = v1alpha1.PhaseCutoverPending
	}
}

// finishFollow runs after the worker Job exited 0: for a live migration that
// means endpos was reached, everything up to it is applied, and sequences are
// already re-synced by pgcopydb. What remains is dropping the replication
// state through the cleanup Job; Complete waits for it.
func (r *MigrationReconciler) finishFollow(ctx context.Context, m, base *v1alpha1.Migration) (ctrl.Result, error) {
	r.setCondition(m, v1alpha1.ConditionCloneCompleted, metav1.ConditionTrue, "BaseCopyDone", "base copy finished")
	r.setCondition(m, v1alpha1.ConditionCutoverComplete, metav1.ConditionTrue, "Drained",
		"endpos reached, changes applied, sequences synced")
	m.Status.Phase = v1alpha1.PhaseCuttingOver

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

	now := metav1.Now()
	m.Status.CompletedAt = &now
	m.Status.Phase = v1alpha1.PhaseCompleted
	r.setCondition(m, v1alpha1.ConditionComplete, metav1.ConditionTrue, "MigrationSucceeded", "live migration finished")
	r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Completed", "Complete", "live migration finished")
	return ctrl.Result{}, r.updateStatus(ctx, m, base)
}

// ensureCleanup creates and observes the cleanup Job. It reports done=true
// once replication state is dropped, or when cleanup exhausted its retries
// (then with a loud warning: the slot may leak on the source, and that needs
// an operator's attention, not an endlessly blocked Migration).
func (r *MigrationReconciler) ensureCleanup(ctx context.Context, m *v1alpha1.Migration) (bool, error) {
	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: cleanupJobName(m)}, job)
	if apierrors.IsNotFound(err) {
		job, err = buildCleanupJob(m, r.RunnerImage)
		if err != nil {
			return false, err
		}
		if err := controllerutil.SetControllerReference(m, job, r.Scheme); err != nil {
			return false, err
		}
		if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, err
		}
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "CleanupStarted", "Cleanup",
			"dropping replication slot, publication, and origin")
		return false, nil
	}
	if err != nil {
		return false, err
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

// reconcileDeletion routes deletion through cleanup for live migrations. The
// finalizer keeps the CR (and thus the owned PVC with the catalogs) alive
// until the slot is dropped; then garbage collection takes everything.
func (r *MigrationReconciler) reconcileDeletion(ctx context.Context, m *v1alpha1.Migration) (ctrl.Result, error) {
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
