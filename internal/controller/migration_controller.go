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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// pollInterval is 10s, not the old 30s: a pass is one psql query per view
// now, not the five catalog-opening pgcopydb commands that bought 30s.
const pollInterval = 10 * time.Second

// zombieGrace is not pollInterval: a worker inside Kubernetes' default 30s
// termination grace is still alive several polls in and is no zombie.
const zombieGrace = 30 * time.Second

const (
	// workerLogTail bounds the log window scanned for the terminal pgcopydb
	// error: the cause sits at the end, but shutdown chatter can follow it.
	workerLogTail = 200
	// preflightLogTail bounds the preflight verdict carried into the condition
	// message. 60, not 20: the fix lines sit behind a re-printed audit list.
	preflightLogTail = 60
	// preflightOkLogTail is the whole preflight log, a number only because the
	// API wants one: a missed remediated: line loses a grant's audit event.
	preflightOkLogTail = 10000
	// verifyLogTail bounds the log window scanned for the verify Job's copy
	// counters. Generous: a content-path compare pushes the line far up.
	verifyLogTail = 10000
	// zombieLogTail is wider than workerLogTail because the surviving
	// streaming child logs LSN reports that push the marker out of a short one.
	zombieLogTail = 1000
	// maxDetailLen caps extracted log lines in condition/event messages
	// (events are server-limited to about 1KiB).
	maxDetailLen = 700
	// remediatedNoteLen keeps the per-tier PreflightRemediated bundle under the
	// events API's 1KiB note limit; the full follow battery is ~620 bytes.
	remediatedNoteLen = 950
)

// The CloneCompleted=False reasons of a running attempt. Two of them double
// as latches: see copySeen and tablesEmptySeen.
const (
	reasonCloneRunning        = "CloneRunning"
	reasonCopyingData         = "CopyingData"
	reasonTablesEmptyOnTarget = "TablesEmptyOnTarget"
)

// reasonCloneIncomplete fails a plain clone whose worker exited 0 while
// pgcopydb's own catalog still counts tables not done (see finishClone).
const reasonCloneIncomplete = "CloneIncomplete"

// LogReader fetches worker pod logs so terminal errors reach status; nil
// degrades to the Job's own condition message (envtest has no pods). The
// zombie check dates the supervisor-death marker off the Timestamps variant.
type LogReader interface {
	JobLogs(ctx context.Context, namespace, jobName string, tailLines int64) ([]byte, error)
	JobLogsTimestamps(ctx context.Context, namespace, jobName string, tailLines int64) ([]byte, error)
}

// ProgressOps samples a running worker; nil disables sampling (envtest
// injects a fake). Best effort throughout: a nil sample keeps the previous
// value and an error never fails the pass.
type ProgressOps interface {
	// Sample reads both databases in one exec. Safe on a pass with a live
	// worker, which is what makes the progress fields move during a copy.
	Sample(ctx context.Context, namespace, jobName string, allDatabases bool) (*progress.Sample, error)
	CloneStage(ctx context.Context, namespace, jobName string) (copying, finalizing bool)
	// GateScript renders the version-gated `list progress` the verify Job
	// carries: the allowlist lives here, so both callers share one gate.
	GateScript() string
}

// MigrationReconciler reconciles a Migration object. Everything is derived
// from observable state, never from reconciler memory: any replica can crash
// between any two steps and the next pass converges. Only follow migrations
// carry a finalizer, for replication-slot cleanup.
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

	// now is overridden only by controlled-clock tests.
	now func() time.Time
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

// Reconcile drives one Migration toward completion. A residual write conflict
// (the Owns(Job) watch triggers overlapping passes) requeues silently: the
// next pass reads the fresh object and converges.
func (r *MigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcile(ctx, req)
	if apierrors.IsConflict(err) {
		//nolint:staticcheck // Requeue preserves the intentional silent rate-limited conflict retry.
		return ctrl.Result{Requeue: true}, nil
	}
	return res, err
}

func (r *MigrationReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reconcileStart := r.currentTime()

	m := &v1beta1.Migration{}
	if err := r.Get(ctx, req.NamespacedName, m); err != nil {
		if apierrors.IsNotFound(err) {
			// Clone-only Migrations skip reconcileDeletion (no finalizer), so
			// this is where their series must die.
			metrics.Forget(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// base is the object as fetched; every status write patches against it.
	base := m.DeepCopy()
	if !m.DeletionTimestamp.IsZero() {
		// Live migrations route through slot cleanup (finalizer); everything
		// else goes with the CR via garbage collection.
		return r.reconcileDeletion(ctx, m)
	}

	// Terminal states are absorbing: a finished migration is history, not a
	// process to restart. Still recorded, so a restarted operator's empty
	// registry regains these series on its startup pass.
	if meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete) ||
		meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed) {
		metrics.Record(m)
		return ctrl.Result{}, nil
	}
	if statusIsEmpty(m.Status) {
		m.Status.Phase = v1beta1.PhasePending
		m.Status.ObservedGeneration = m.Generation
		if err := r.Status().Patch(ctx, m,
			client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		metrics.Record(m)
		return ctrl.Result{RequeueAfter: time.Nanosecond}, nil
	}

	if m.Spec.Suspend {
		return r.reconcileSuspended(ctx, m, base)
	}

	// Validation catches deterministic connection and option errors, even with an older CRD.
	// InvalidSpec is terminal: retrying the same spec cannot help.
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
	// Only past the gate: while the preflight runs the condition stays Unknown,
	// so a stuck gate never reads as a validated migration.
	r.setCondition(m, v1beta1.ConditionValidated, metav1.ConditionTrue, "SpecValid", "connection and clone options materialize cleanly and the preflight passed")

	if m.Status.JobName == "" {
		return r.nextAttempt(ctx, m, base)
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: m.Status.JobName}, job)
	switch {
	case apierrors.IsNotFound(err):
		// The Job vanished (TTL, manual delete, or a suspend cycle).
		return r.nextAttempt(ctx, m, base)
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

	return r.observeRunningJob(ctx, m, base, job, reconcileStart)
}

func statusIsEmpty(status v1beta1.MigrationStatus) bool {
	return apiequality.Semantic.DeepEqual(status, v1beta1.MigrationStatus{})
}

func (r *MigrationReconciler) currentTime() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *MigrationReconciler) activePollDelay(reconcileStart time.Time) time.Duration {
	delay := pollInterval - r.currentTime().Sub(reconcileStart)
	if delay <= 0 {
		return time.Nanosecond
	}
	return delay
}

// preflightGate runs the one-shot probe Job before any worker starts.
// Failure is absorbing: these are configuration errors on the databases, so
// retrying the migration cannot fix them. handled=true ends the pass here.
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
		return ctrl.Result{RequeueAfter: pollInterval}, true, nil
	}
	// At-least-once: a pass that loses its status write re-emits, and a
	// duplicate event beats a grant with no audit trail.
	if !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionValidated) {
		r.emitPreflightOutcome(ctx, m)
	}
	return ctrl.Result{}, false, nil
}

// emitPreflightOutcome turns the finished preflight's log into events, one
// PreflightRemediated bundle per tier. Not one event per statement: the
// recorder collapses differing messages that share a reason and action into
// a counter, so all but the first statement would be lost.
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
// while it starts normally. A pod that cannot start would otherwise show a
// bare Validating phase with the cause buried in pod events (seen live).
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

// observeRunningJob samples a live worker (progress, sizes, follow state) and
// schedules the next look.
//
// The invariant: no pass exec-s a pgcopydb command into a live worker pod.
// There is no read-only one. Every invocation commits its own command line
// into the worker's SQLite catalog, invalidating a read snapshot the worker
// holds open and failing its next write with `SQLITE_BUSY_SNAPSHOT`, which no
// retry can clear (see
// docs/research/upstream-issues.md#why-the-operator-execs-no-pgcopydb-command-into-a-live-worker).
// A live worker gets psql and nothing else. The one exec that remains is
// `sentinel set endpos`, which is how a cutover is asked for and has no other
// route.
func (r *MigrationReconciler) observeRunningJob(
	ctx context.Context,
	m, base *v1beta1.Migration,
	job *batchv1.Job,
	reconcileStart time.Time,
) (res ctrl.Result, err error) {
	observationStart := r.currentTime()
	timings := []any{"scope", "active_worker_observation"}
	outcome := "normal"
	defer func() {
		timings = append(timings, "outcome", outcome, "total", r.currentTime().Sub(observationStart))
		logf.FromContext(ctx).V(1).Info("active worker observation", timings...)
	}()
	record := func(name string, started time.Time) {
		timings = append(timings, name, r.currentTime().Sub(started))
	}

	// The tail (index builds, a vacuum on the largest table) stops the target
	// growing, so a size estimate reads as finished; reporting it apart tells a
	// slow tail from a stall. Finalizing needs the copy seen first, or a sample
	// catching every worker between statements reports the tail mid-copy.
	started := r.currentTime()
	if r.Progress != nil {
		copying, finalizing := r.Progress.CloneStage(ctx, m.Namespace, job.Name)
		switch {
		case copying:
			m.Status.Phase = v1beta1.PhaseCloning
			// A refused marker outranks the probe: its latch is the only
			// memory of the marker once that scrolls out of the log tail.
			if meta.IsStatusConditionFalse(m.Status.Conditions, v1beta1.ConditionCloneCompleted) && !tablesEmptySeen(m) {
				r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, reasonCopyingData,
					"base copy running, copy workers connected to the target")
			}
		case finalizing && copySeen(m):
			m.Status.Phase = v1beta1.PhaseFinalizing
		}
		record("clone_stage", started)
	}
	follow := followEnabled(m)

	// One log fetch per pass serves both the clone-done and the zombie check.
	// Unreadable logs degrade to an empty tail; the next poll retries.
	var logTail []byte
	started = r.currentTime()
	if follow && r.Logs != nil {
		raw, err := r.Logs.JobLogsTimestamps(ctx, m.Namespace, job.Name, zombieLogTail)
		if err != nil {
			logf.FromContext(ctx).V(1).Info("worker log fetch failed", "job", job.Name, "error", err)
		} else {
			logTail = raw
		}
		record("follow_log_fetch", started)
	}

	// After the log fetch and before the marker is read: the sample is the
	// evidence the marker is checked against, so it must have seen every copy
	// transaction the marker follows commit.
	started = r.currentTime()
	counts := r.sampleProgress(ctx, m, job.Name)
	record("progress_sample", started)

	cloneDone := meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)
	if follow && !cloneDone && (pgcopydb.CloneDone(logTail) || tablesEmptySeen(m)) {
		cloneDone = r.confirmBaseCopy(m, counts)
	}
	if follow {
		// May advance the phase to Streaming/CutoverPending/CuttingOver and
		// trigger the cutover itself; see follow.go.
		started = r.currentTime()
		r.reconcileFollowRunning(ctx, m, job.Name, cloneDone)
		record("follow_control", started)
	}
	started = r.currentTime()
	if err := r.updateStatus(ctx, m, base); err != nil {
		record("status_patch", started)
		outcome = "status_patch_error"
		return ctrl.Result{}, err
	}
	record("status_patch", started)
	started = r.currentTime()
	if res, handled, err := r.reapZombieWorker(ctx, m, job, logTail); handled || err != nil {
		record("zombie_reap", started)
		switch {
		case err != nil:
			outcome = "zombie_reap_error"
		default:
			outcome = "zombie_reap_handled"
		}
		return res, err
	}
	record("zombie_reap", started)
	// One cadence throughout: the size sample is the only live view of a
	// running copy, and the throughput panel is its slope.
	delay := r.activePollDelay(reconcileStart)
	timings = append(timings, "next_delay", delay)
	return ctrl.Result{RequeueAfter: delay}, nil
}

// copySeen reports whether the clone-stage probe has caught this attempt's
// copy moving data. The latch lives in the condition reason, so it survives
// an operator restart; startAttempt resets it per attempt.
func copySeen(m *v1beta1.Migration) bool {
	c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCloneCompleted)
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == reasonCopyingData
}

// tablesEmptySeen reports whether a refused clone-done marker still stands.
// Like copySeen, the latch lives in the condition reason.
func tablesEmptySeen(m *v1beta1.Migration) bool {
	c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCloneCompleted)
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == reasonTablesEmptyOnTarget
}

// confirmBaseCopy decides whether the worker's clone-done marker turns
// CloneCompleted true. The marker is pgcopydb's own bookkeeping and has called
// a table done that no rows ever reached
// (docs/research/measurements.md#the-clone-done-marker-reported-a-table-no-rows-had-reached),
// so this pass's own sample has the last word. The sample tests presence, not
// a row count: the source runs ahead of the copy's snapshot until the stream
// catches up, so a count can hold this gate shut for good. The refusal latches
// in the reason because the marker scrolls out of the bounded log tail; only a
// sample that owes nothing clears it, and a pass without a sample neither
// causes nor lifts one.
func (r *MigrationReconciler) confirmBaseCopy(m *v1beta1.Migration, counts *progress.RelationCounts) bool {
	switch {
	case counts != nil && counts.TablesDone < counts.TablesTotal:
		msg := fmt.Sprintf("pgcopydb logged the base copy finished, but %d of %d in-scope tables hold rows on the source and none on the target",
			counts.TablesTotal-counts.TablesDone, counts.TablesTotal)
		if counts.EmptyOnTarget != "" {
			msg += ": " + truncate(counts.EmptyOnTarget, maxDetailLen)
		}
		msg += "; the copy is not reported complete until a sample finds a row in each of them"
		if !tablesEmptySeen(m) {
			r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, reasonTablesEmptyOnTarget, "Clone", "%s", msg)
		}
		r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, reasonTablesEmptyOnTarget, msg)
		return false
	case counts == nil && tablesEmptySeen(m):
		return false
	}
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "BaseCopyDone",
		"base copy finished (worker logged clone completion), replaying changes")
	return true
}

// sampleProgress feeds the size gauges and the progress counters while the
// worker runs, returning the counts this pass read or nil. It uses psql and
// nothing else, per the invariant on observeRunningJob, and is best effort:
// an error never flips a condition or fails the pass.
func (r *MigrationReconciler) sampleProgress(ctx context.Context, m *v1beta1.Migration, jobName string) *progress.RelationCounts {
	if r.Progress == nil {
		return nil
	}
	s, err := r.Progress.Sample(ctx, m.Namespace, jobName, m.Spec.Clone.AllDatabases)
	if err != nil {
		logf.FromContext(ctx).V(1).Info("database sample failed", "job", jobName, "error", err)
		return nil
	}
	if s == nil {
		return nil
	}
	// Metrics only: sizes are observability, not state.
	metrics.RecordDatabaseSizes(m.Namespace, m.Name, s.SourceSize, s.TargetSize)
	if s.Counts != nil {
		applyCounts(m, s.Counts)
	}
	return s.Counts
}

// applyCounts writes the newest estimate over whatever is there: every sample
// counts the databases afresh, and keeping an earlier reading froze the
// dashboard tiles at the first sample of the clone. No estimate outlives
// pgcopydb's own catalog, which overwrites the field (see recordCloneProgress).
func applyCounts(m *v1beta1.Migration, c *progress.RelationCounts) {
	if m.Status.Progress == nil {
		m.Status.Progress = &v1beta1.CloneProgress{}
	}
	p := m.Status.Progress
	p.TablesTotal, p.TablesDone = c.TablesTotal, c.TablesDone
	p.IndexesTotal, p.IndexesDone = c.IndexesTotal, c.IndexesDone
	p.BytesTotal = resource.NewQuantity(c.BytesTotal, resource.BinarySI)
	p.BytesDone = resource.NewQuantity(c.BytesDone, resource.BinarySI)
}

// reapZombieWorker deletes a worker pod pgcopydb 0.18 leaves alive: the
// supervisor dies, but the streaming receive child survives and keeps pid 1
// waiting, so the Job never fails and the retry path never engages. raw is
// the tail observeRunningJob already fetched. handled=true ends the pass.
func (r *MigrationReconciler) reapZombieWorker(ctx context.Context, m *v1beta1.Migration, job *batchv1.Job, raw []byte) (ctrl.Result, bool, error) {
	if !followEnabled(m) || len(raw) == 0 {
		return ctrl.Result{}, false, nil
	}
	died, found := pgcopydb.SupervisorDeath(raw)
	if !found {
		return ctrl.Result{}, false, nil
	}
	if time.Since(died) < zombieGrace {
		// Still inside the grace: let an ordinary shutdown use all of its
		// termination window to fail the Job before the survivor is a zombie.
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
	// Suspended promises no further database writes, and the handover writes
	// catalog rows. A finished Job stays: it is the audit trail, and deleting
	// it would make resume run a completed handover again.
	if reownRequested(m) {
		rj := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: reownJobName(m)}, rj)
		if err == nil {
			if done, _ := jobFinished(rj); !done {
				policy := metav1.DeletePropagationForeground
				if err := r.Delete(ctx, rj, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
				r.setCondition(m, v1beta1.ConditionOwnershipApplied, metav1.ConditionUnknown, "OwnershipSuspended",
					"ownership handover stopped by suspend; it re-runs on resume and picks up what is left")
				r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Suspended", "Suspend", "ownership handover Job deleted; it re-runs on resume")
			}
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	m.Status.Phase = v1beta1.PhaseSuspended
	return ctrl.Result{}, r.updateStatus(ctx, m, base)
}

// nextAttempt is where a pass lands with no worker Job to observe. It starts
// the next attempt, resuming from the work-dir catalogs, unless only the
// finish path remains: restarting pgcopydb there would put a second writer on
// the target beside a handover still altering ownership.
//
// One window survives: a follow worker collected between its exit 0 and the
// first finish pass has no handover Job yet, so it restarts, finds endpos
// reached, and exits 0 again. Closing the window would break the mid-stream
// restart a vanished follow worker usually needs.
func (r *MigrationReconciler) nextAttempt(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	exists, err := r.reownJobExists(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	switch {
	case !followEnabled(m) && (exists || meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)):
		log.Info("worker Job gone after the copy finished, resuming the finish path", "job", m.Status.JobName)
		return r.finishClone(ctx, m, base)
	case exists:
		log.Info("worker Job gone after the handover started, resuming the finish path", "job", m.Status.JobName)
		return r.finishFollow(ctx, m, base)
	}
	if m.Status.JobName != "" {
		log.Info("worker Job missing, starting next attempt", "job", m.Status.JobName)
	}
	return r.startAttempt(ctx, m, base)
}

// startAttempt creates the next worker Job, unless the retry budget is spent.
// Budget: backoffLimit is the number of retries, so backoffLimit+1 attempts.
func (r *MigrationReconciler) startAttempt(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	// Reachable despite handleFailedJob's own budget check: the final attempt's
	// Job can vanish while running, and the next pass lands here over budget.
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
	m.Status.Phase = attemptPhase(m)
	// A finished base copy does not un-happen when the worker is restarted;
	// only a copy still owed gets the fresh attempt's reason, which is also
	// what clears the Finalizing latch (see copySeen).
	if !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted) {
		r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, reasonCloneRunning,
			fmt.Sprintf("attempt %d running as Job %s", attempt, job.Name))
	}
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

// attemptPhase is where the next attempt picks up, read off the conditions
// rather than assumed: every attempt after the first resumes the migration,
// and one that has already cut over is not back at the base copy.
func attemptPhase(m *v1beta1.Migration) v1beta1.MigrationPhase {
	rep := m.Status.Replication
	switch {
	case rep != nil && sentinel.EndposSet(rep.Endpos),
		meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCutoverComplete):
		return v1beta1.PhaseCuttingOver
	case meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionStreaming):
		return v1beta1.PhaseStreaming
	}
	return v1beta1.PhaseCloning
}

// handleFailedJob either schedules the next resume attempt or fails the
// Migration for good. The Job's condition only says that the pod failed, so
// the pgcopydb ERROR behind it is appended from the pod log when readable.
func (r *MigrationReconciler) handleFailedJob(ctx context.Context, m, base *v1beta1.Migration, job *batchv1.Job) (ctrl.Result, error) {
	reason := failureReason(job)
	tail := r.jobLogTail(ctx, m.Namespace, job.Name, workerLogTail)
	if detail := truncate(pgcopydb.LastErrorLine([]byte(tail)), maxDetailLen); detail != "" {
		reason += "; last error: " + detail
	}

	// A retry replays the same statements as the same role and refuses
	// identically, so the budget would only delay the verdict. This catches
	// what the preflight cannot probe: rights revoked mid-run, source SELECT.
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
	//nolint:staticcheck // Requeue preserves the intentional rate-limited resume retry after the status patch.
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
// object is controlled by THIS Migration. Adopting a leftover from a deleted
// Migration would have it garbage collected under the running Job.
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

// fail marks the Migration terminally failed. The event note is capped by the
// server's ~1KiB limit; the condition keeps the full message.
func (r *MigrationReconciler) fail(m *v1beta1.Migration, reason, action, msg string) {
	m.Status.Phase = v1beta1.PhaseFailed
	r.setCondition(m, v1beta1.ConditionFailed, metav1.ConditionTrue, reason, msg)
	r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, reason, action, "%s", truncate(msg, maxDetailLen))
}

// ensureJob fetches the named child Job, creating it from build when absent.
// A nil job and a nil error mean the Job has just been created and has
// nothing to observe yet. created is false when an overlapping pass won the
// create race, so callers gating an announce event on it stay silent.
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

// updateStatus writes m's status as a merge patch against base. Unlike
// Update, the patch carries no resourceVersion, so a pass working from a copy
// another pass has moved on from still lands instead of conflicting.
func (r *MigrationReconciler) updateStatus(ctx context.Context, m, base *v1beta1.Migration) error {
	m.Status.ObservedGeneration = m.Generation
	metrics.Record(m)
	return r.Status().Patch(ctx, m, client.MergeFrom(base))
}

// migrationEvents drops the controller's own status writes from the Migration
// watch: the lag figure differs every pass, so each patch would wake the next
// pass at once and the loop would sustain itself (one to two passes per
// second through a cutover drain). Generation bumps still arrive.
var migrationEvents predicate.Predicate = predicate.GenerationChangedPredicate{}

// SetupWithManager wires the watches. The predicate is on the Migration watch
// alone; the owned Jobs must deliver every event.
func (r *MigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.Migration{}, builder.WithPredicates(migrationEvents)).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.ConfigMap{}).
		Named("migration").
		Complete(r)
}
