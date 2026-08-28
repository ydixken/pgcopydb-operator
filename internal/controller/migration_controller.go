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
	// condition message. 60, not 20: the failure footer re-prints the audit
	// list first, then the notes carrying exact GRANT statements, then the
	// hints, and the fix lines must survive the window even when a long
	// replica-identity audit precedes them.
	preflightLogTail = 60
	// preflightOkLogTail is effectively the whole preflight log: the
	// success-path parse must see every remediated: line even under a long
	// replica-identity audit, or applied grants lose their audit events.
	// A number only because the log API wants a TailLines value.
	preflightOkLogTail = 10000
	// zombieLogTail bounds the log window scanned for the supervisor-death
	// marker. Wider than workerLogTail because the surviving streaming child
	// keeps logging LSN reports after the marker and would push it out of a
	// short tail between two polls.
	zombieLogTail = 1000
	// maxDetailLen caps extracted log lines in condition/event messages
	// (events are server-limited to about 1KiB).
	maxDetailLen = 700
	// remediatedNoteLen caps the per-tier PreflightRemediated bundle just
	// under the events API's 1KiB note limit: the full follow battery is
	// ~620 bytes and must never truncate out of the audit trail.
	remediatedNoteLen = 950
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

// ProgressOps samples a running worker: clone progress for status and
// metrics, database sizes for metrics only. Nil disables sampling (envtest
// injects a fake). Both are best effort: a nil sample keeps the previous
// value and an error never fails the pass.
type ProgressOps interface {
	CloneProgress(ctx context.Context, namespace, jobName string) (*v1beta1.CloneProgress, error)
	DatabaseSizes(ctx context.Context, namespace, jobName string) (src, tgt *int64, err error)
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

	// Progress samples clone progress and database sizes; nil disables it.
	Progress ProgressOps
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
		if apierrors.IsNotFound(err) {
			// Clone-only Migrations skip reconcileDeletion (no finalizer), so
			// this is where their series must die.
			metrics.Forget(req.Namespace, req.Name)
		}
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
	// process to restart (source/target are immutable anyway). Still record:
	// a restarted operator's empty registry regains these series on its
	// startup pass.
	if meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete) ||
		meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed) {
		metrics.Record(m)
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
	if err := r.ensureFinalizer(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureOwned(ctx, m); err != nil {
		return ctrl.Result{}, err
	}

	if res, handled, err := r.preflightGate(ctx, m, base); handled || err != nil {
		return res, err
	}
	// Validated turns True only once the gate is clear: while the preflight
	// runs the condition is Unknown/PreflightRunning, so nobody reads a
	// stuck gate as a validated migration.
	r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionTrue, "SpecValid", "connection and clone options materialize cleanly and the preflight passed")

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

// preflightGate probes the databases before any worker runs: a one-shot Job
// validates connectivity first for every Migration, verifies configured
// superuser credentials, and for follow migrations checks wal_level, slot
// headroom, the REPLICATION attribute, origin-function EXECUTE, and the
// session_replication_role SET gate, remediating the grantable ones when a
// superuser connection is provided. Every follow check failed live before it
// failed loudly; two of them lose data silently. Failure is absorbing: these
// are configuration errors on the databases, retrying the migration cannot
// fix them. handled=true means this pass ends here; false lets the caller
// proceed to Job orchestration.
func (r *MigrationReconciler) preflightGate(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, bool, error) {
	if m.Status.Attempts != 0 {
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
		msg := "preflight Job " + preflightJobName(m) + " is running"
		if detail := r.preflightWaitDetail(ctx, m.Namespace, preflightJobName(m)); detail != "" {
			msg = "preflight Job " + preflightJobName(m) + " cannot start: " + truncate(detail, maxDetailLen)
		}
		r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionUnknown, "PreflightRunning", msg)
		m.Status.Phase = v1beta1.PhaseValidating
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{RequeueAfter: pollInterval / 3}, true, nil
	}
	// Emit once per gate outcome: after Validated=True is persisted this is
	// skipped. At-least-once on purpose: a pass losing every status write
	// re-emits, and duplicate events beat a silent audit trail of grants.
	if !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionValidated) {
		r.emitPreflightOutcome(ctx, m)
	}
	return ctrl.Result{}, false, nil
}

// emitPreflightOutcome turns the finished preflight's log into events: one
// PreflightRemediated per tier listing that tier's applied statements, then
// the PreflightPassed summary. Bundled, not one per statement: the events
// recorder correlates by reason and action and drops differing messages into
// a counter, which silently ate all but the first statement (found by the
// rc.1 gate); the distinct per-tier actions are what keep the two bundles
// apart. The "ok: ", "remediated: ", and "remediated-clone: " line prefixes
// are the script's log contract.
func (r *MigrationReconciler) emitPreflightOutcome(ctx context.Context, m *v1beta1.Migration) {
	checks := 0
	var clone, follow []string
	tail := r.jobLogTail(ctx, m.Namespace, preflightJobName(m), preflightOkLogTail)
	for line := range strings.SplitSeq(tail, "\n") {
		switch {
		case strings.HasPrefix(line, "ok: "):
			checks++
		case strings.HasPrefix(line, remPrefixClone):
			clone = append(clone, strings.TrimPrefix(line, remPrefixClone))
		case strings.HasPrefix(line, remPrefixFollow):
			follow = append(follow, strings.TrimPrefix(line, remPrefixFollow))
		}
	}
	if len(clone) > 0 {
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "PreflightRemediated", "RemediateClone",
			"%s", truncate(strings.Join(clone, "\n"), remediatedNoteLen))
	}
	if len(follow) > 0 {
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "PreflightRemediated", "RemediateFollow",
			"%s", truncate(strings.Join(follow, "\n"), remediatedNoteLen))
	}
	grants := len(clone) + len(follow)
	msg := "all preflight checks passed"
	if checks > 0 || grants > 0 {
		msg = fmt.Sprintf("%d checks passed, %d grants applied", checks, grants)
	}
	r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "PreflightPassed", "Preflight", "%s", msg)
}

// jobNameLabel is the Job controller's own pod label, the selector both the
// log reader and the pod inspectors use.
const jobNameLabel = "job-name"

// preflightWaitDetail reports why the preflight pod is not progressing, or ""
// while it starts normally. A pod that cannot start (missing Secret, image
// pull, unschedulable) would otherwise show a bare Validating phase with the
// cause buried in pod events; found live at a customer.
func (r *MigrationReconciler) preflightWaitDetail(ctx context.Context, namespace, jobName string) string {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(namespace),
		client.MatchingLabels{jobNameLabel: jobName}); err != nil {
		return ""
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		for _, cond := range p.Status.Conditions {
			if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse &&
				cond.Reason == corev1.PodReasonUnschedulable {
				return "pod unschedulable: " + cond.Message
			}
		}
		statuses := append(append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...),
			p.Status.ContainerStatuses...)
		for _, cs := range statuses {
			w := cs.State.Waiting
			if w == nil {
				continue
			}
			switch w.Reason {
			// The transient reasons every healthy start passes through.
			case "", "ContainerCreating", "PodInitializing":
			default:
				if w.Message != "" {
					return w.Reason + ": " + w.Message
				}
				return w.Reason
			}
		}
	}
	return ""
}

// observeRunningJob samples a live worker (progress, sizes, follow state)
// and schedules the next look.
//
// Progress polling is gated: the sampler runs `list progress` only on
// runner versions its allowlist names, because on stock pgcopydb 0.18 the
// exec corrupts filtered catalogs and never returns data, see
// docs/research/upstream-issues.md. Database sizes come from psql, which
// touches no catalog and is safe on any runner.
//
// Sentinel execs are quiesced until the base copy is done: every exec opens
// pgcopydb's SQLite catalogs inside the worker, and 0.18 crashes under
// concurrent catalog access (a live clone's index worker died on
// "[SQLite 5: database is locked]", exit 12; see
// docs/research/upstream-issues.md). So while CloneCompleted is False the
// operator touches only the pod LOG: the clone-done transition is derived
// from the markers pgcopydb prints when the copy phase ends
// (pgcopydb.CloneDone), and the single fetched tail also feeds the zombie
// check. Only once CloneCompleted is True do sentinel reads begin;
// Streaming/CaughtUp/cutover logic is unchanged from there.
func (r *MigrationReconciler) observeRunningJob(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job) (ctrl.Result, error) {
	m.Status.Phase = v1beta1.PhaseCloning
	follow := followEnabled(m)

	// One log fetch per pass serves both the clone-done and the zombie check.
	// Unreadable logs (pod starting, already gone) degrade to an empty tail:
	// no marker seen, nothing to reap, the next poll retries.
	var logTail []byte
	if follow && r.Logs != nil {
		raw, err := r.Logs.JobLogsTimestamps(ctx, m.Namespace, job.Name, zombieLogTail)
		if err != nil {
			logf.FromContext(ctx).V(1).Info("worker log fetch failed", "job", job.Name, "error", err)
		} else {
			logTail = raw
		}
	}

	cloneDone := meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)
	if follow && !cloneDone && pgcopydb.CloneDone(logTail) {
		cloneDone = true
		r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "BaseCopyDone",
			"base copy finished (worker logged clone completion), replaying changes")
	}
	if follow && (cloneDone || r.Logs == nil) {
		// May advance the phase to Streaming/CutoverPending/CuttingOver and
		// trigger the cutover itself; see follow.go. With no log reader wired
		// the clone-done transition cannot be observed, so the sentinel stays
		// the detector (the pre-quiescence behavior) rather than wedging the
		// migration in Cloning forever.
		r.reconcileFollowRunning(ctx, m, job.Name)
	}
	r.sampleProgress(ctx, m, job.Name)
	if err := r.updateStatus(ctx, m, base); err != nil {
		return ctrl.Result{}, err
	}
	if res, handled, err := r.reapZombieWorker(ctx, m, job, logTail); handled || err != nil {
		return res, err
	}
	// Cadence: until the base copy is done, poll at half speed; a pass costs
	// only a pod-log read, but there is nothing time-critical to observe
	// mid-copy. Post-clone the fast cadence returns, keeping catchup and
	// cutover reactive.
	interval := pollInterval
	if follow && !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted) {
		interval = 2 * pollInterval
	}
	return ctrl.Result{RequeueAfter: interval}, nil
}

// sampleProgress best-effort feeds the size gauges and status.progress from
// the running worker. Errors V(1)-log and never flip a condition or fail
// the pass; a nil progress sample keeps the previous one.
func (r *MigrationReconciler) sampleProgress(ctx context.Context, m *v1beta1.Migration, jobName string) {
	if r.Progress == nil {
		return
	}
	log := logf.FromContext(ctx)
	src, tgt, err := r.Progress.DatabaseSizes(ctx, m.Namespace, jobName)
	if err != nil {
		log.V(1).Info("database size sample failed", "job", jobName, "error", err)
	} else {
		// Metrics only, by design: sizes are observability, not state.
		metrics.RecordDatabaseSizes(m.Namespace, m.Name, src, tgt)
	}
	cp, err := r.Progress.CloneProgress(ctx, m.Namespace, jobName)
	switch {
	case err != nil:
		log.V(1).Info("progress sample failed", "job", jobName, "error", err)
	case cp != nil:
		m.Status.Progress = cp
	}
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
// the marker line. raw is the tail observeRunningJob already fetched for this
// pass (one fetch serves this check and the clone-done check); empty means
// the logs were unreadable and there is nothing to detect. Acting only once
// the marker is a full pollInterval old is the stateless equivalent of
// requiring it on two consecutive polls, and keeps an ordinary failure
// shutdown in progress (marker just logged, container about to exit, Job
// about to fail on its own) from being misread as a zombie. Clock skew
// between the runtime's stamp and this process only shifts the grace by
// seconds either way and converges on the next poll. handled=true ends the
// pass here: confirm on the next poll, or pod deleted.
func (r *MigrationReconciler) reapZombieWorker(ctx context.Context, m *v1beta1.Migration, job *batchv1.Job, raw []byte) (ctrl.Result, bool, error) {
	if !followEnabled(m) || len(raw) == 0 {
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
	if err := r.List(ctx, pods, client.InNamespace(namespace),
		client.MatchingLabels{jobNameLabel: jobName}); err != nil {
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
	// A gate-stage suspend must stop the preflight too: it may be applying
	// superuser remediation, and Suspended promises no further database
	// writes. The gate recreates the Job on resume.
	if m.Status.Attempts == 0 {
		pf := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: preflightJobName(m)}, pf)
		if err == nil {
			policy := metav1.DeletePropagationBackground
			if err := r.Delete(ctx, pf, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Suspended", "Suspend", "preflight Job deleted; the gate re-runs on resume")
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
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
	tail := r.jobLogTail(ctx, m.Namespace, job.Name, workerLogTail)
	if detail := truncate(pgcopydb.LastErrorLine([]byte(tail)), maxDetailLen); detail != "" {
		reason += "; last error: " + detail
	}

	// A permission error is database configuration: a retry replays the same
	// statements as the same role and refuses identically, so the budget only
	// delays the verdict. The preflight probes the known grants before attempt
	// 1; this catches what it cannot see (rights revoked mid-run, unprobed
	// classes like source-side SELECT).
	if line := pgcopydb.PermissionDeniedLine([]byte(tail)); line != "" {
		r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, "CloneFailed", reason)
		r.fail(m, "PermissionDenied", "Fail", fmt.Sprintf(
			"attempt %d failed on a permission error retries cannot fix: %s",
			m.Status.Attempts, truncate(line, maxDetailLen)))
		return ctrl.Result{}, r.updateStatus(ctx, m, base)
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
