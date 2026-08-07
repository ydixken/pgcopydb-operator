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
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

// pollInterval is how often a running clone is re-checked (progress polling
// attaches here later).
const pollInterval = 30 * time.Second

// MigrationReconciler reconciles a Migration object.
//
// Everything is derived from observable state (the owned Job's status and the
// Migration's own status/conditions), never from reconciler memory: any
// replica can crash between any two steps and the next pass converges. There
// is no finalizer in M1: a clone leaves nothing behind on the databases that
// Kubernetes garbage collection (ownerReferences on Job/PVC/ConfigMap) does
// not clean up. Follow mode (M2) adds one for replication-slot cleanup.
type MigrationReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// RunnerImage is the default worker image; spec.runner.image overrides.
	RunnerImage string

	// Poller reads clone progress from running worker pods; nil disables
	// polling (envtest has no pods to ask).
	Poller *progress.Poller
}

// +kubebuilder:rbac:groups=pgcopydb-operator.io,resources=migrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgcopydb-operator.io,resources=migrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgcopydb-operator.io,resources=migrations/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims;configmaps,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create

// Reconcile drives one Migration toward completion.
func (r *MigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	m := &v1alpha1.Migration{}
	if err := r.Get(ctx, req.NamespacedName, m); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !m.DeletionTimestamp.IsZero() {
		// Owned objects go with the CR via garbage collection.
		metrics.Forget(m.Namespace, m.Name)
		return ctrl.Result{}, nil
	}

	// Terminal states are absorbing: a finished migration is history, not a
	// process to restart (source/target are immutable anyway).
	if meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete) ||
		meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionFailed) {
		return ctrl.Result{}, nil
	}

	if m.Spec.Suspend {
		return r.reconcileSuspended(ctx, m)
	}

	// Validation: materializing both connections exercises every spec error
	// the operator can catch without contacting the databases. Spec errors
	// are absorbing (source/target are immutable, retrying cannot help).
	if _, err := buildJob(m, r.RunnerImage, 1); err != nil {
		r.setCondition(m, v1alpha1.ConditionValidated, metav1.ConditionFalse, "InvalidSpec", err.Error())
		r.setCondition(m, v1alpha1.ConditionFailed, metav1.ConditionTrue, "InvalidSpec", err.Error())
		m.Status.Phase = v1alpha1.PhaseFailed
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "InvalidSpec", "Validate", "%s", err.Error())
		return ctrl.Result{}, r.updateStatus(ctx, m)
	}
	r.setCondition(m, v1alpha1.ConditionValidated, metav1.ConditionTrue, "SpecValid", "connection and clone options materialize cleanly")

	if err := r.ensureOwned(ctx, m); err != nil {
		return ctrl.Result{}, err
	}

	// Job orchestration: observe the current attempt or start the next one.
	if m.Status.JobName == "" {
		return r.startAttempt(ctx, m)
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: m.Status.JobName}, job)
	switch {
	case apierrors.IsNotFound(err):
		// The Job vanished (TTL, manual delete, or a suspend cycle). Start
		// the next attempt; pgcopydb resumes from the work-dir catalogs.
		log.Info("worker Job missing, starting next attempt", "job", m.Status.JobName)
		return r.startAttempt(ctx, m)
	case err != nil:
		return ctrl.Result{}, err
	}

	if done, ok := jobFinished(job); done {
		if ok {
			now := metav1.Now()
			m.Status.CompletedAt = &now
			m.Status.Phase = v1alpha1.PhaseCompleted
			r.setCondition(m, v1alpha1.ConditionCloneCompleted, metav1.ConditionTrue, "CloneSucceeded", "pgcopydb clone finished")
			r.setCondition(m, v1alpha1.ConditionComplete, metav1.ConditionTrue, "MigrationSucceeded", "migration finished")
			r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Completed", "Complete", "pgcopydb clone finished")
			return ctrl.Result{}, r.updateStatus(ctx, m)
		}
		return r.handleFailedJob(ctx, m, job)
	}

	m.Status.Phase = v1alpha1.PhaseCloning
	if r.Poller != nil {
		// Best effort: a missing sample (pod starting/terminating, catalogs
		// not ready) keeps the previous numbers instead of failing the pass.
		if p, err := r.Poller.CloneProgress(ctx, m.Namespace, job.Name); err != nil {
			log.V(1).Info("progress poll failed", "error", err)
		} else if p != nil {
			m.Status.Progress = p
		}
	}
	if err := r.updateStatus(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

// reconcileSuspended deletes the active worker (keeping the PVC, so a later
// resume continues from the catalogs) and parks the Migration.
func (r *MigrationReconciler) reconcileSuspended(ctx context.Context, m *v1alpha1.Migration) (ctrl.Result, error) {
	if m.Status.JobName != "" {
		job := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: m.Status.JobName}, job)
		if err == nil {
			// Foreground: pods are gone before the Job object is, so the
			// pgcopydb process is stopped (SIGTERM, clean shutdown) first.
			policy := metav1.DeletePropagationForeground
			if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Suspended", "Suspend", "worker Job deleted, work volume kept")
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		m.Status.JobName = ""
	}
	m.Status.Phase = v1alpha1.PhaseSuspended
	return ctrl.Result{}, r.updateStatus(ctx, m)
}

// startAttempt creates the next worker Job, unless the retry budget is spent.
// Budget: backoffLimit is the number of retries, so backoffLimit+1 attempts.
func (r *MigrationReconciler) startAttempt(ctx context.Context, m *v1alpha1.Migration) (ctrl.Result, error) {
	if m.Status.Attempts >= m.Spec.BackoffLimit+1 {
		msg := fmt.Sprintf("retry budget exhausted after %d attempts", m.Status.Attempts)
		m.Status.Phase = v1alpha1.PhaseFailed
		r.setCondition(m, v1alpha1.ConditionFailed, metav1.ConditionTrue, "BackoffLimitExceeded", msg)
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "BackoffLimitExceeded", "Fail", "%s", msg)
		return ctrl.Result{}, r.updateStatus(ctx, m)
	}

	attempt := m.Status.Attempts + 1
	job, err := buildJob(m, r.RunnerImage, attempt)
	if err != nil {
		// buildJob was already exercised during validation; an error here is
		// transient environment trouble, let the reconcile retry.
		return ctrl.Result{}, err
	}
	if err := controllerutil.SetControllerReference(m, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, err
	}

	if m.Status.StartedAt == nil {
		now := metav1.Now()
		m.Status.StartedAt = &now
	}
	m.Status.Attempts = attempt
	m.Status.JobName = job.Name
	m.Status.Phase = v1alpha1.PhaseCloning
	r.setCondition(m, v1alpha1.ConditionCloneCompleted, metav1.ConditionFalse, "CloneRunning",
		fmt.Sprintf("attempt %d running as Job %s", attempt, job.Name))
	r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "AttemptStarted", "StartAttempt", "attempt %d as Job %s", attempt, job.Name)
	if err := r.updateStatus(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

// handleFailedJob either schedules the next resume attempt or fails the
// Migration for good.
func (r *MigrationReconciler) handleFailedJob(ctx context.Context, m *v1alpha1.Migration, job *batchv1.Job) (ctrl.Result, error) {
	reason := failureReason(job)
	if m.Status.Attempts >= m.Spec.BackoffLimit+1 {
		m.Status.Phase = v1alpha1.PhaseFailed
		r.setCondition(m, v1alpha1.ConditionCloneCompleted, metav1.ConditionFalse, "CloneFailed", reason)
		r.setCondition(m, v1alpha1.ConditionFailed, metav1.ConditionTrue, "BackoffLimitExceeded",
			fmt.Sprintf("attempt %d failed: %s", m.Status.Attempts, reason))
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "Failed", "Fail", "attempt %d failed: %s", m.Status.Attempts, reason)
		return ctrl.Result{}, r.updateStatus(ctx, m)
	}

	// Clear jobName; the next pass creates attempt N+1 with --resume.
	r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "AttemptFailed", "Retry",
		"attempt %d failed (%s), retrying with --resume", m.Status.Attempts, reason)
	m.Status.JobName = ""
	if err := r.updateStatus(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// ensureOwned creates the PVC and the filters ConfigMap when missing. Both are
// immutable in practice (spec source/target and filters cannot change to
// different storage needs mid-run), so create-if-absent is enough.
func (r *MigrationReconciler) ensureOwned(ctx context.Context, m *v1alpha1.Migration) error {
	pvc := buildWorkPVC(m)
	if err := controllerutil.SetControllerReference(m, pvc, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	if cm := buildFiltersConfigMap(m); cm != nil {
		if err := controllerutil.SetControllerReference(m, cm, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

// jobFinished reports (finished, succeeded) from the Job's conditions.
func jobFinished(job *batchv1.Job) (bool, bool) {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return true, true
		case batchv1.JobFailed:
			return true, false
		}
	}
	return false, false
}

func failureReason(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			if c.Message != "" {
				return c.Message
			}
			return c.Reason
		}
	}
	return "worker Job failed"
}

func (r *MigrationReconciler) setCondition(m *v1alpha1.Migration, t string, s metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
		Type:               t,
		Status:             s,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: m.Generation,
	})
}

func (r *MigrationReconciler) updateStatus(ctx context.Context, m *v1alpha1.Migration) error {
	m.Status.ObservedGeneration = m.Generation
	metrics.Record(m)
	return r.Status().Update(ctx, m)
}

// SetupWithManager sets up the controller with the Manager.
func (r *MigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Migration{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.ConfigMap{}).
		Named("migration").
		Complete(r)
}
