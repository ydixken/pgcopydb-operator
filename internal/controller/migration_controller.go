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
	"fmt"
	"strings"
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

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// pollInterval is how often a running clone is re-checked (follow state and
// Job observation; no progress exec, see observeRunningJob).
const pollInterval = 30 * time.Second

const (
	// workerLogTail bounds the log window scanned for the terminal pgcopydb
	// error: the cause sits at the end, but supervisor shutdown chatter can
	// follow it.
	workerLogTail = 200
	// preflightLogTail bounds the preflight verdict carried into the
	// condition message: one line per failed check plus psql stderr.
	preflightLogTail = 20
	// zombieLogTail bounds the log window scanned for the supervisor-death
	// marker. Wider than workerLogTail because the surviving streaming child
	// keeps logging LSN reports after the marker and would push it out of a
	// short tail between two polls.
	zombieLogTail = 1000
	// maxDetailLen caps extracted log lines in condition/event messages
	// (events are server-limited to about 1KiB).
	maxDetailLen = 700
)

// LogReader fetches worker pod logs so terminal errors can be surfaced in
// status; nil degrades to the Job's own condition message (envtest has no
// pods to read). The Timestamps variant prefixes the container runtime's
// RFC3339Nano stamp to every line; the zombie check dates the
// supervisor-death marker with it.
type LogReader interface {
	JobLogs(ctx context.Context, namespace, jobName string, tailLines int64) ([]byte, error)
	JobLogsTimestamps(ctx context.Context, namespace, jobName string, tailLines int64) ([]byte, error)
}

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

	// Sentinel drives follow migrations; nil disables follow handling.
	Sentinel SentinelOps

	// Logs reads worker pod logs for failure surfacing; nil disables it.
	Logs LogReader
}

// +kubebuilder:rbac:groups=pgcopydb-operator.io,resources=migrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgcopydb-operator.io,resources=migrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgcopydb-operator.io,resources=migrations/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims;configmaps,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get

// Reconcile drives one Migration toward completion.
//
// A residual write conflict (the Owns(Job) watch triggers overlapping passes)
// requeues silently: the next pass reads the fresh object and converges, so
// the conflict carries no information worth logging or eventing.
func (r *MigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcile(ctx, req)
	if apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return res, err
}

func (r *MigrationReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	m := &v1beta1.Migration{}
	if err := r.Get(ctx, req.NamespacedName, m); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// base is the object as fetched; status writes patch against it so a
	// stale copy (an overlapping pass already moved the object on) does not
	// conflict the way a full Update would.
	base := m.DeepCopy()
	if !m.DeletionTimestamp.IsZero() {
		// Live migrations route through slot cleanup (finalizer); everything
		// else goes with the CR via garbage collection.
		return r.reconcileDeletion(ctx, m)
	}

	// Terminal states are absorbing: a finished migration is history, not a
	// process to restart (source/target are immutable anyway).
	if meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete) ||
		meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed) {
		return ctrl.Result{}, nil
	}

	if m.Spec.Suspend {
		return r.reconcileSuspended(ctx, m, base)
	}

	// Validation: materializing both connections exercises every spec error
	// the operator can catch without contacting the databases. Spec errors
	// are absorbing (source/target are immutable, retrying cannot help).
	if _, err := buildJob(m, r.RunnerImage, 1); err != nil {
		r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionFalse, "InvalidSpec", err.Error())
		r.fail(m, "InvalidSpec", "Validate", err.Error())
		return ctrl.Result{}, r.updateStatus(ctx, m, base)
	}
	r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionTrue, "SpecValid", "connection and clone options materialize cleanly")

	if err := r.ensureFinalizer(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureOwned(ctx, m); err != nil {
		return ctrl.Result{}, err
	}

	if res, handled, err := r.preflightGate(ctx, m, base); handled || err != nil {
		return res, err
	}

	// Job orchestration: observe the current attempt or start the next one.
	if m.Status.JobName == "" {
		return r.startAttempt(ctx, m, base)
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: m.Status.JobName}, job)
	switch {
	case apierrors.IsNotFound(err):
		// The Job vanished (TTL, manual delete, or a suspend cycle). Start
		// the next attempt; pgcopydb resumes from the work-dir catalogs.
		log.Info("worker Job missing, starting next attempt", "job", m.Status.JobName)
		return r.startAttempt(ctx, m, base)
	case err != nil:
		return ctrl.Result{}, err
	}

	if done, ok := jobFinished(job); done {
		if ok {
			if followEnabled(m) {
				// Worker exit 0 in follow mode means endpos reached and
				// sequences synced; Complete waits for slot cleanup.
				return r.finishFollow(ctx, m, base)
			}
			// Sets CloneCompleted, runs verification when requested, then
			// Complete; see verification.go.
			return r.finishClone(ctx, m, base)
		}
		return r.handleFailedJob(ctx, m, base, job)
	}

	return r.observeRunningJob(ctx, m, base, job)
}

// preflightGate probes the follow prerequisites before any worker runs: a
// one-shot Job checks wal_level, slot headroom, the REPLICATION attribute,
// origin-function EXECUTE, and the session_replication_role SET gate. Every
// one of these failed live before it failed loudly; two of them lose data
// silently. Failure is absorbing: these are configuration errors on the
// databases, retrying the migration cannot fix them. handled=true means this
// pass ends here; false lets the caller proceed to Job orchestration.
func (r *MigrationReconciler) preflightGate(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, bool, error) {
	if !followEnabled(m) || m.Status.Attempts != 0 {
		return ctrl.Result{}, false, nil
	}
	passed, failMsg, err := r.ensurePreflight(ctx, m)
	switch {
	case err != nil:
		return ctrl.Result{}, true, err
	case failMsg != "":
		r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionFalse, "PreflightFailed", failMsg)
		r.fail(m, "PreflightFailed", "Preflight", failMsg)
		return ctrl.Result{}, true, r.updateStatus(ctx, m, base)
	case !passed:
		m.Status.Phase = v1beta1.PhaseValidating
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{RequeueAfter: pollInterval / 3}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// observeRunningJob samples a live worker (follow state) and schedules the
// next look.
//
// Progress polling is disabled on pgcopydb 0.18: the `list progress` exec
// corrupts filtered catalogs and never returns data, see
// docs/research/upstream-issues.md. status.progress stays in the API,
// reserved for a fixed upstream.
func (r *MigrationReconciler) observeRunningJob(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job) (ctrl.Result, error) {
	m.Status.Phase = v1beta1.PhaseCloning
	if followEnabled(m) {
		// May advance the phase to Streaming/CutoverPending/CuttingOver and
		// trigger the cutover itself; see follow.go.
		r.reconcileFollowRunning(ctx, m, job.Name)
	}
	if err := r.updateStatus(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	if res, handled, err := r.reapZombieWorker(ctx, m, job); handled || err != nil {
		return res, err
	}
	// Cadence: until the base copy is done, poll at half speed. Every
	// sentinel read opens pgcopydb's SQLite catalogs, and on 0.18 concurrent
	// readers can starve its writers: a live clone's index worker died on
	// "[SQLite 5: database is locked]" (exit 12) under the 30s cadence, see
	// docs/research/upstream-issues.md. Post-clone the fast cadence returns,
	// keeping catchup and cutover reactive.
	interval := pollInterval
	if followEnabled(m) && !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted) {
		interval = 2 * pollInterval
	}
	return ctrl.Result{RequeueAfter: interval}, nil
}

// reapZombieWorker handles pgcopydb 0.18's zombie failure mode, proven live:
// a clone worker dies, pid 1 logs FATAL "Terminating all processes in our
// process group" and signals the group, but the streaming receive child
// survives and keeps pid 1 waiting, so the pod stays alive indefinitely. The
// Job never fails, the Migration reports Cloning forever, and the retry
// machinery never engages. Follow-only: plain clones have no child that
// outlives the group termination.
//
// Detection is stateless, from observable state only: the supervisor-death
// marker in the pod log (it cannot un-happen) plus the runtime's timestamp on
// the marker line. Acting only once the marker is a full pollInterval old is
// the stateless equivalent of requiring it on two consecutive polls, and
// keeps an ordinary failure shutdown in progress (marker just logged,
// container about to exit, Job about to fail on its own) from being misread
// as a zombie. Clock skew between the runtime's stamp and this process only
// shifts the grace by seconds either way and converges on the next poll.
// handled=true ends the pass here: confirm on the next poll, or pod deleted.
func (r *MigrationReconciler) reapZombieWorker(ctx context.Context, m *v1beta1.Migration, job *batchv1.Job) (ctrl.Result, bool, error) {
	if !followEnabled(m) || r.Logs == nil {
		return ctrl.Result{}, false, nil
	}
	raw, err := r.Logs.JobLogsTimestamps(ctx, m.Namespace, job.Name, zombieLogTail)
	if err != nil {
		// Unreadable logs (pod starting, already gone): nothing to detect.
		logf.FromContext(ctx).V(1).Info("zombie check skipped, pod log fetch failed", "job", job.Name, "error", err)
		return ctrl.Result{}, false, nil
	}
	died, found := pgcopydb.SupervisorDeath(raw)
	if !found {
		return ctrl.Result{}, false, nil
	}
	if time.Since(died) < pollInterval {
		// First sighting: give an ordinary shutdown one poll to fail the Job
		// on its own before treating the survivor as a zombie.
		return ctrl.Result{RequeueAfter: pollInterval}, true, nil
	}
	if err := r.deleteJobPods(ctx, m.Namespace, job.Name); err != nil {
		return ctrl.Result{}, true, err
	}
	r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "WorkerZombie", "Reap",
		"pgcopydb supervisor died but a child process kept the pod alive (upstream defect); worker pod removed so the normal retry path resumes the migration")
	return ctrl.Result{RequeueAfter: pollInterval}, true, nil
}

// deleteJobPods foreground-deletes the Job's live pods. With backoffLimit 0
// the Job controller then marks the Job failed and handleFailedJob runs its
// normal retry flow.
func (r *MigrationReconciler) deleteJobPods(ctx context.Context, namespace, jobName string) error {
	pods := &corev1.PodList{}
	// job-name is the Job controller's own pod label, the same selector the
	// log reader uses.
	if err := r.List(ctx, pods, client.InNamespace(namespace),
		client.MatchingLabels{"job-name": jobName}); err != nil {
		return err
	}
	policy := metav1.DeletePropagationForeground
	for i := range pods.Items {
		p := &pods.Items[i]
		if !p.DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.Delete(ctx, p, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// reconcileSuspended deletes the active worker (keeping the PVC, so a later
// resume continues from the catalogs) and parks the Migration.
func (r *MigrationReconciler) reconcileSuspended(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
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
			if followEnabled(m) {
				r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "SlotRetained", "Suspend",
					"the replication slot stays open while suspended and retains WAL on the source")
			}
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		m.Status.JobName = ""
	}
	m.Status.Phase = v1beta1.PhaseSuspended
	return ctrl.Result{}, r.updateStatus(ctx, m, base)
}

// startAttempt creates the next worker Job, unless the retry budget is spent.
// Budget: backoffLimit is the number of retries, so backoffLimit+1 attempts.
func (r *MigrationReconciler) startAttempt(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	// Reachable despite handleFailedJob's own budget check: the final
	// attempt's Job can vanish while running (TTL, manual delete) or be
	// cleared by a suspend cycle, and the next pass lands here over budget.
	if m.Status.Attempts >= m.Spec.BackoffLimit+1 {
		r.fail(m, "BackoffLimitExceeded", "Fail",
			fmt.Sprintf("retry budget exhausted after %d attempts", m.Status.Attempts))
		return ctrl.Result{}, r.updateStatus(ctx, m, base)
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
	createErr := r.Create(ctx, job)
	if createErr != nil && !apierrors.IsAlreadyExists(createErr) {
		return ctrl.Result{}, createErr
	}

	if m.Status.StartedAt == nil {
		now := metav1.Now()
		m.Status.StartedAt = &now
	}
	m.Status.Attempts = attempt
	m.Status.JobName = job.Name
	m.Status.Phase = v1beta1.PhaseCloning
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, "CloneRunning",
		fmt.Sprintf("attempt %d running as Job %s", attempt, job.Name))
	if createErr == nil {
		// AlreadyExists means an overlapping pass created this Job and
		// announced the attempt; a second event would just be noise.
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "AttemptStarted", "StartAttempt", "attempt %d as Job %s", attempt, job.Name)
	}
	if err := r.updateStatus(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

// handleFailedJob either schedules the next resume attempt or fails the
// Migration for good. The Job's own condition only says that the pod failed;
// the actual cause (a pgcopydb ERROR) lives in the pod log, so its last error
// line is appended when readable.
func (r *MigrationReconciler) handleFailedJob(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job) (ctrl.Result, error) {
	reason := failureReason(job)
	if detail := r.workerErrorDetail(ctx, m.Namespace, job.Name); detail != "" {
		reason += "; last error: " + detail
	}
	if m.Status.Attempts >= m.Spec.BackoffLimit+1 {
		r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, "CloneFailed", reason)
		r.fail(m, "BackoffLimitExceeded", "Fail",
			fmt.Sprintf("attempt %d failed: %s", m.Status.Attempts, reason))
		return ctrl.Result{}, r.updateStatus(ctx, m, base)
	}

	// Clear jobName; the next pass creates attempt N+1 with --resume.
	r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "AttemptFailed", "Retry",
		"attempt %d failed (%s), retrying with --resume", m.Status.Attempts, reason)
	m.Status.JobName = ""
	if err := r.updateStatus(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// ensureOwned creates the PVC and the filters ConfigMap when missing. Both are
// immutable in practice (spec source/target and filters cannot change to
// different storage needs mid-run), so create-if-absent is enough.
func (r *MigrationReconciler) ensureOwned(ctx context.Context, m *v1beta1.Migration) error {
	pvc := buildWorkPVC(m)
	if err := controllerutil.SetControllerReference(m, pvc, r.Scheme); err != nil {
		return err
	}
	if err := r.createStrictlyOwned(ctx, m, pvc); err != nil {
		return err
	}
	if cm := buildFiltersConfigMap(m); cm != nil {
		if err := controllerutil.SetControllerReference(m, cm, r.Scheme); err != nil {
			return err
		}
		if err := r.createStrictlyOwned(ctx, m, cm); err != nil {
			return err
		}
	}
	return nil
}

// createStrictlyOwned creates obj, and on AlreadyExists verifies the existing
// object is controlled by THIS Migration. A leftover from a deleted Migration
// (garbage collection is asynchronous) must never be adopted: it would be
// deleted under the running Job. The error requeues until GC clears it.
func (r *MigrationReconciler) createStrictlyOwned(ctx context.Context, m *v1beta1.Migration, obj client.Object) error {
	err := r.Create(ctx, obj)
	if err == nil || !apierrors.IsAlreadyExists(err) {
		return err
	}
	existing := obj.DeepCopyObject().(client.Object)
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), existing); err != nil {
		return err
	}
	if !metav1.IsControlledBy(existing, m) {
		return fmt.Errorf("%s %q exists but belongs to another owner (stale object awaiting garbage collection?)",
			existing.GetObjectKind().GroupVersionKind().Kind, obj.GetName())
	}
	return nil
}

// fail marks the Migration terminally failed: phase, the Failed condition,
// and a warning event. reason names the cause, action the reconcile step. The
// event note is capped (events are server-limited to about 1KiB); conditions
// keep the full message.
func (r *MigrationReconciler) fail(m *v1beta1.Migration, reason, action, msg string) {
	m.Status.Phase = v1beta1.PhaseFailed
	r.setCondition(m, v1beta1.ConditionFailed, metav1.ConditionTrue, reason, msg)
	r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, reason, action, "%s", truncate(msg, maxDetailLen))
}

// ensureJob fetches the named child Job, creating it from build when absent.
// A nil job with a nil error means the Job was created just now (or an
// overlapping pass won the create race) and has nothing to observe yet.
// created reports that THIS pass issued the create, so callers gate their
// announce events on it and stay silent on the race (the guard proven by the
// stale-object regression test).
func (r *MigrationReconciler) ensureJob(ctx context.Context, m *v1beta1.Migration, name string, build func() (*batchv1.Job, error)) (*batchv1.Job, bool, error) {
	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: name}, job)
	if err == nil {
		return job, false, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, false, err
	}
	job, err = build()
	if err != nil {
		return nil, false, err
	}
	if err := controllerutil.SetControllerReference(m, job, r.Scheme); err != nil {
		return nil, false, err
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return nil, true, nil
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

// jobLogTail returns the trimmed tail of the Job's newest pod's logs, or ""
// when logs are unreadable (no reader wired, pod already gone, RBAC): callers
// degrade to the information they already have.
func (r *MigrationReconciler) jobLogTail(ctx context.Context, namespace, jobName string, lines int64) string {
	if r.Logs == nil {
		return ""
	}
	raw, err := r.Logs.JobLogs(ctx, namespace, jobName, lines)
	if err != nil {
		logf.FromContext(ctx).V(1).Info("pod log fetch failed", "job", jobName, "error", err)
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// workerErrorDetail pulls the last pgcopydb ERROR/FATAL message out of a
// failed worker's structured logs (PGCOPYDB_LOG_JSON=on); "" when no parsable
// error line is available.
func (r *MigrationReconciler) workerErrorDetail(ctx context.Context, namespace, jobName string) string {
	tail := r.jobLogTail(ctx, namespace, jobName, workerLogTail)
	return truncate(pgcopydb.LastErrorLine([]byte(tail)), maxDetailLen)
}

// truncate caps s for contexts with server-side size limits (event notes).
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
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

func (r *MigrationReconciler) setCondition(m *v1beta1.Migration, t string, s metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
		Type:               t,
		Status:             s,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: m.Generation,
	})
}

// updateStatus writes m's status as a merge patch against base, the object as
// it was fetched at the top of the pass. Unlike Update, the patch carries no
// resourceVersion, so a pass working from a copy that another pass has since
// moved on still lands instead of erroring with a conflict.
func (r *MigrationReconciler) updateStatus(ctx context.Context, m, base *v1beta1.Migration) error {
	m.Status.ObservedGeneration = m.Generation
	metrics.Record(m)
	return r.Status().Patch(ctx, m, client.MergeFrom(base))
}

// SetupWithManager sets up the controller with the Manager.
func (r *MigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.Migration{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.ConfigMap{}).
		Named("migration").
		Complete(r)
}
