package controller

// The rejoin runs crash the engine's follow cleanup at the boundaries a crash can leave, then
// drive finishNativeFollow to Completed through the partial-cleanup rejoin route. A crash is a
// SIGKILL of the cleanup container while a source lock or a synchronous commit holds the engine.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
)

const (
	nativeCleanupJobPrefix = "rpgmigrate-cleanup-"
	// The engine removes the slot before the publication (src/cleanup.rs), so a crash never leaves
	// the publication tombstone without the slot one; the route's (false,true) admission has no live run.
	seamsCrashBeforeIntent      = "before-intent"
	seamsCrashAfterIntent       = "after-intent"
	seamsCrashBeforePublication = "before-publication"
	seamsCrashBeforeTombstone   = "before-tombstone"
	seamsAbsentStandby          = "seams_absent_standby"
)

// seamsCrashPoint is one crash and the durable state it must leave: the cleanup intent and its
// flags (slot_removed, publication_removed) in the engine manifest, and the source resources.
type seamsCrashPoint struct {
	kind              string
	intent            bool
	flags             [2]bool
	slot, publication bool
}

type seamsRejoinCase struct {
	prefix, database string
	crashes          []seamsCrashPoint
	// reappear recreates the removed slot before the finish: the rejoin route must refuse it.
	reappear bool
}

var seamsRejoinCases = []seamsRejoinCase{
	{prefix: "rejoin-00", database: "seams_rejoin_00", crashes: []seamsCrashPoint{
		{kind: seamsCrashBeforeIntent, slot: true, publication: true},
		{kind: seamsCrashAfterIntent, intent: true, slot: true, publication: true},
	}},
	{prefix: "rejoin-10", database: "seams_rejoin_10", reappear: true, crashes: []seamsCrashPoint{
		{kind: seamsCrashBeforePublication, intent: true, flags: [2]bool{true, false}, publication: true},
	}},
	{prefix: "rejoin-10-dropped", database: "seams_rejoin_10d", crashes: []seamsCrashPoint{
		{kind: seamsCrashBeforeTombstone, intent: true, flags: [2]bool{true, false}},
	}},
}

func seamsRejoinDatabases() []string {
	out := make([]string, 0, len(seamsRejoinCases))
	for _, c := range seamsRejoinCases {
		out = append(out, c.database)
	}
	return out
}

// seamsCrash is one armed crash; finished blocks once the kill was sent, so no reconcile pass
// runs while the injector still holds a source lock.
type seamsCrash struct {
	kind   string
	run    string
	mu     sync.Mutex
	killed bool
	done   chan struct{}
	detail string
	err    error
}

func (c *seamsCrash) finished() bool {
	select {
	case <-c.done:
		return true
	default:
	}
	c.mu.Lock()
	killed := c.killed
	c.mu.Unlock()
	if killed {
		<-c.done
	}
	return killed
}

func (c *seamsCrash) finish(detail string, err error) {
	c.detail, c.err = detail, err
	close(c.done)
}

func (h *seamsHarness) runRejoin() {
	for _, c := range seamsRejoinCases {
		h.runRejoinCase(c)
	}
}

func (h *seamsHarness) runRejoinCase(c seamsRejoinCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	name := "seams-" + c.prefix
	m := h.newMigration(name, c.database)
	run := runIDFor(m)
	if !h.streamAndDrain(ctx, c.prefix, seamsLiveMarker+c.prefix+"-", m, func() {}) {
		return
	}
	for _, point := range c.crashes {
		if !h.check(c.prefix+"/crash-"+point.kind, "reconcile: finishNativeFollow until SIGKILL interrupts its cleanup Job", func() (string, error) {
			return h.crashCleanup(ctx, name, run, point.kind)
		}) {
			return
		}
		h.check(c.prefix+"/state-"+point.kind, "engine manifest and source SQL after the crash", func() (string, error) { return h.expectCrashState(ctx, m, point) })
	}
	since := h.cluster.exitCount()
	if c.reappear {
		h.check(c.prefix+"/reappeared", "reconcile: the rejoin route refuses a slot recreated under its tombstone", func() (string, error) {
			return h.expectReappearedRefused(ctx, name, run)
		})
	}
	h.check(c.prefix+"/finish", "reconcile: rejoin route, resumed cleanup and cleaned completion to Completed", func() (string, error) {
		detail, err := h.reconcileCompleted(ctx, name)
		admitted, refused := h.cluster.exitsSince(since, nativeCleanupRejoinJobPrefix)
		cleaned, failed := h.cluster.exitsSince(since, nativeCleanupJobPrefix, nativeCleanupRejoinJobPrefix)
		detail = fmt.Sprintf("%s; rejoin helpers %d admitted, %d refused; cleanup Jobs %d succeeded, %d failed", detail, admitted, refused, cleaned, failed)
		if err == nil && (admitted == 0 || cleaned == 0) {
			err = errors.New("completion did not pass through an admitted rejoin and a resumed cleanup")
		}
		return detail, err
	})
	h.check(c.prefix+"/helpers", "API store: no state or cleanup helper Job outlives Completed", func() (string, error) { return h.expectNoStateHelpers(ctx, name) })
	h.check(c.prefix+"/verification", "compare verdicts that finishNativeFollow recorded", func() (string, error) { return h.expectVerification(ctx, name) })
	h.expectCleaned(ctx, run, c.database, c.prefix)
	h.deleteMigration(ctx, name, c.prefix)
}

// crashCleanup arms one crash for the next cleanup Job and reconciles until it fired. Every crash
// follows a state without cleanup intent, so each rejoin probe before it must refuse.
func (h *seamsHarness) crashCleanup(ctx context.Context, name, run, kind string) (string, error) {
	crash := &seamsCrash{kind: kind, run: run, done: make(chan struct{})}
	h.crashMu.Lock()
	h.crashes = append(h.crashes, crash)
	h.crashMu.Unlock()
	since := h.cluster.exitCount()
	detail, err := h.reconcileUntil(ctx, name, 6*time.Minute, func(o seamsObjects) bool { return crash.finished() || seamsCompleted(o) })
	h.crashMu.Lock()
	armed := len(h.crashes) > 0 && h.crashes[0] == crash
	h.crashes = nil
	h.crashMu.Unlock()
	if armed {
		return detail, errors.Join(err, errors.New("no cleanup Job started"))
	}
	if !crash.finished() {
		return detail, errors.Join(err, errors.New("the Migration completed before the crash"))
	}
	admitted, refused := h.cluster.exitsSince(since, nativeCleanupRejoinJobPrefix)
	detail = fmt.Sprintf("%s; %s; rejoin helpers %d admitted, %d refused", crash.detail, detail, admitted, refused)
	if crash.err != nil {
		return detail, crash.err
	}
	if admitted > 0 || refused == 0 {
		return detail, errors.New("the rejoin route did not refuse a state without cleanup intent")
	}
	return detail, nil
}

// cleanupStarted is the kubelet hook: it counts engine cleanup Jobs and arms the next pending crash.
func (h *seamsHarness) cleanupStarted(job *batchv1.Job) {
	if !strings.HasPrefix(job.Name, nativeCleanupJobPrefix) || strings.HasPrefix(job.Name, nativeCleanupRejoinJobPrefix) {
		return
	}
	h.crashMu.Lock()
	h.cleanupStarts++
	var crash *seamsCrash
	if len(h.crashes) > 0 {
		crash, h.crashes = h.crashes[0], h.crashes[1:]
	}
	h.crashMu.Unlock()
	if crash != nil {
		h.arm(crash, job.Name)
	}
}

// arm sets the crash's trap before the engine starts, then waits off the reconcile path for the
// engine to reach it, kills the container and releases the source.
func (h *seamsHarness) arm(crash *seamsCrash, job string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	hold, err := h.openSession(ctx, h.source, seamsAppDatabase)
	if err != nil {
		cancel()
		crash.finish("", err)
		return
	}
	var publication string
	switch crash.kind {
	case seamsCrashBeforeIntent, seamsCrashAfterIntent:
		// Inspection's last query reads pg_publication_tables, which opens every member table.
		_, err = hold.do("BEGIN; LOCK TABLE orders IN ACCESS EXCLUSIVE MODE;")
	case seamsCrashBeforePublication:
		if publication, err = hold.do("SELECT quote_ident(pubname) FROM pg_publication WHERE pubname LIKE 'rpgm_p_" + crash.run + "_%';"); err == nil {
			_, err = hold.do("BEGIN; COMMENT ON PUBLICATION " + publication + " IS NULL;")
		}
	case seamsCrashBeforeTombstone:
		if err = h.requireSyncWait(ctx, hold); err != nil {
			_, _ = hold.do(seamsSyncReset)
		}
	default:
		err = fmt.Errorf("unknown crash %q", crash.kind)
	}
	if err != nil {
		hold.close()
		cancel()
		crash.finish("", err)
		return
	}
	go func() {
		defer cancel()
		defer hold.close()
		detail, err := h.inject(ctx, crash, job, hold)
		crash.finish(detail, err)
	}()
}

const seamsSyncReset = "ALTER SYSTEM RESET synchronous_standby_names;\nSELECT pg_reload_conf();"

// requireSyncWait names a standby that never connects and proves that a commit now waits for it.
// statement_timeout cannot end that wait, so the probe commit's wait is cancelled from hold.
func (h *seamsHarness) requireSyncWait(ctx context.Context, hold *seamsSession) error {
	if _, err := hold.do("ALTER SYSTEM SET synchronous_standby_names = '" + seamsAbsentStandby + "';\nSELECT pg_reload_conf();"); err != nil {
		return err
	}
	probe, err := h.openSession(ctx, h.source, seamsAppDatabase)
	if err != nil {
		return err
	}
	defer probe.close()
	if _, err := probe.do("SET application_name TO 'seams-sync-probe';"); err != nil {
		return err
	}
	for range 50 {
		done := make(chan error, 1)
		go func() {
			// Only a commit that wrote WAL waits for a synchronous standby.
			_, err := probe.do("SELECT pg_logical_emit_message(true, 'seams', 'sync-probe');")
			done <- err
		}()
		waited := false
		for finished := false; !finished; {
			select {
			case err := <-done:
				if err != nil {
					return err
				}
				finished = true
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
				out, err := hold.do("SELECT count(pg_cancel_backend(a.pid)) FROM pg_catalog.pg_stat_get_activity(NULL) a WHERE a.application_name = 'seams-sync-probe' AND a.wait_event = 'SyncRep';")
				if err != nil {
					return err
				}
				waited = waited || out != "0"
			}
		}
		if waited {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("commits never waited for the absent synchronous standby")
}

func (h *seamsHarness) inject(ctx context.Context, crash *seamsCrash, job string, hold *seamsSession) (string, error) {
	kill := func() error {
		crash.mu.Lock()
		crash.killed = true
		crash.mu.Unlock()
		if err := h.cluster.kill(ctx, job, "KILL"); err != nil {
			return err
		}
		return h.cluster.waitFailedExit(ctx, job)
	}
	switch crash.kind {
	case seamsCrashBeforeIntent:
		if err := seamsEngineWaits(ctx, hold, "a.wait_event_type = 'Lock' AND a.query LIKE '%pg_publication_tables%'"); err != nil {
			return "", err
		}
		if err := kill(); err != nil {
			return "", err
		}
		_, err := hold.do("ROLLBACK;")
		return "SIGKILL while inspection waited on the member table lock", err
	case seamsCrashAfterIntent:
		if err := seamsEngineWaits(ctx, hold, "a.wait_event_type = 'Lock' AND a.query LIKE '%pg_publication_tables%'"); err != nil {
			return "", err
		}
		// Inspection already read pg_database; the slot removal's source check reads it next.
		catalog, err := h.openSession(ctx, h.source, seamsAppDatabase)
		if err != nil {
			return "", err
		}
		defer catalog.close()
		if _, err := catalog.do("BEGIN; LOCK TABLE pg_catalog.pg_database IN ACCESS EXCLUSIVE MODE;"); err != nil {
			return "", err
		}
		if _, err := hold.do("ROLLBACK;"); err != nil {
			return "", err
		}
		if err := seamsEngineWaits(ctx, catalog, "a.wait_event_type = 'Lock' AND a.query LIKE '%pg_control_system%'"); err != nil {
			return "", err
		}
		if err := kill(); err != nil {
			return "", err
		}
		_, err = catalog.do("ROLLBACK;")
		return "SIGKILL after inspection, while the slot removal's source check waited on pg_database", err
	case seamsCrashBeforePublication:
		if err := seamsEngineWaits(ctx, hold, "a.wait_event_type = 'Lock' AND a.query LIKE 'DROP PUBLICATION%'"); err != nil {
			return "", err
		}
		if err := kill(); err != nil {
			return "", err
		}
		_, err := hold.do("ROLLBACK;")
		return "SIGKILL while DROP PUBLICATION waited on the publication lock", err
	default:
		if err := seamsEngineWaits(ctx, hold, "a.wait_event = 'SyncRep'"); err != nil {
			_, _ = hold.do(seamsSyncReset)
			return "", err
		}
		killErr := kill()
		if _, err := hold.do(seamsSyncReset); err != nil || killErr != nil {
			return "", errors.Join(killErr, err)
		}
		// The released commit makes the DROP visible; the engine never recorded its flag.
		for {
			out, err := hold.do("SELECT count(*) FROM pg_publication WHERE pubname LIKE 'rpgm_p_" + crash.run + "_%';")
			if err != nil || out == "0" {
				return "SIGKILL while the committed DROP PUBLICATION waited for a synchronous standby", err
			}
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("the released DROP PUBLICATION never became visible: %w", ctx.Err())
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// seamsEngineWaits polls one held session until an engine backend matches the wait condition.
// pg_stat_get_activity reads no catalog, so it answers while pg_database is locked; inside the
// holder's transaction it repeats its first snapshot unless that is cleared.
func seamsEngineWaits(ctx context.Context, s *seamsSession, condition string) error {
	sql := "SELECT pg_catalog.pg_stat_clear_snapshot();\nSELECT count(*) FROM pg_catalog.pg_stat_get_activity(NULL) a WHERE a.application_name LIKE 'rpgmigrate[%' AND " + condition + ";"
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		out, err := s.do(sql)
		if err != nil {
			return err
		}
		// The cleared snapshot prints an empty line first.
		if strings.TrimSpace(out) != "0" {
			return nil
		}
		select {
		case <-ctx.Done():
			// The session list says where the engine was instead.
			seen, _ := s.do("SELECT pg_catalog.pg_stat_clear_snapshot();\nSELECT string_agg(concat_ws(' ', a.application_name, a.state, a.wait_event_type, a.wait_event, left(a.query, 80)), ' | ') FROM pg_catalog.pg_stat_get_activity(NULL) a WHERE a.backend_type = 'client backend';")
			return fmt.Errorf("the engine never waited where %s; sessions: %s", condition, seen)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// expectCrashState reads the cleanup progress the engine persisted and the source resources it left.
func (h *seamsHarness) expectCrashState(ctx context.Context, m *v1beta1.Migration, want seamsCrashPoint) (string, error) {
	h.cluster.mu.Lock()
	volume := h.cluster.claims[workPVCName(m)]
	h.cluster.mu.Unlock()
	if volume == "" {
		return "", errors.New("no work claim volume")
	}
	raw, stderr, err := seamsPodman(ctx, nil, seamsRunWord, "--rm", seamsLabelFlag, seamsOwnerKey+"="+h.owner, seamsNetworkFlag, "none", "--cpus", "1", "--memory", "256m",
		"--user", "65532:65532", "-v", volume+":/work:ro", "--entrypoint", "cat", h.images.source, "/work/pgcopydb/manifest.json")
	if err != nil {
		return "", fmt.Errorf("read the engine manifest: %v %s", err, stderr)
	}
	var manifest struct {
		Follow *struct {
			Cleanup *struct {
				Slot        bool `json:"slot_removed"`
				Publication bool `json:"publication_removed"`
			} `json:"cleanup"`
		} `json:"follow_state"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Follow == nil {
		return "", fmt.Errorf("the engine manifest has no follow state: %v", err)
	}
	run := runIDFor(m)
	slot := h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'rpgm_s_"+run+"_%';")
	publication := h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT count(*) FROM pg_publication WHERE pubname LIKE 'rpgm_p_"+run+"_%';")
	cleanup := manifest.Follow.Cleanup
	detail := fmt.Sprintf("cleanup intent absent; source slots %s, publications %s", slot, publication)
	if cleanup != nil {
		detail = fmt.Sprintf("cleanup slot_removed %v, publication_removed %v; source slots %s, publications %s", cleanup.Slot, cleanup.Publication, slot, publication)
	}
	present := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	if (cleanup != nil) != want.intent || (cleanup != nil && [2]bool{cleanup.Slot, cleanup.Publication} != want.flags) || slot != present(want.slot) || publication != present(want.publication) {
		return detail, fmt.Errorf("want intent %v, flags %v, slot %v, publication %v", want.intent, want.flags, want.slot, want.publication)
	}
	return detail, nil
}

// expectReappearedRefused recreates the removed slot by name and requires every finish pass to hold:
// the rejoin probe refuses, no cleanup Job starts and nothing completes. The slot is dropped again after.
func (h *seamsHarness) expectReappearedRefused(ctx context.Context, name, run string) (string, error) {
	slot := "rpgm_s_" + run + "_1"
	h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT slot_name FROM pg_create_logical_replication_slot('"+slot+"', 'pgoutput');")
	defer h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT pg_drop_replication_slot('"+slot+"');")
	since := h.cluster.exitCount()
	h.crashMu.Lock()
	starts := h.cleanupStarts
	h.crashMu.Unlock()
	detail, err := h.reconcileUntil(ctx, name, 4*time.Minute, func(o seamsObjects) bool {
		_, refused := h.cluster.exitsSince(since, nativeCleanupRejoinJobPrefix)
		return refused >= 2 || seamsCompleted(o)
	})
	admitted, refused := h.cluster.exitsSince(since, nativeCleanupRejoinJobPrefix)
	h.crashMu.Lock()
	started := h.cleanupStarts - starts
	h.crashMu.Unlock()
	o, oerr := h.objects(ctx, name)
	left := h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '"+slot+"';")
	detail = fmt.Sprintf("%s; rejoin helpers %d admitted, %d refused; %d cleanup Jobs started; recreated slot present %s", detail, admitted, refused, started, left)
	switch {
	case err != nil:
		return detail, err
	case oerr != nil:
		return detail, oerr
	case seamsCompleted(o) || admitted > 0 || started > 0 || left != "1":
		return detail, errors.New("a reappeared slot under its tombstone was admitted")
	}
	return detail, nil
}

// seamsSession is one long-lived superuser psql session, so a lock outlives the statement that took it.
type seamsSession struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    *bufio.Reader
	stderr bytes.Buffer
	n      int
}

func (h *seamsHarness) openSession(ctx context.Context, container, database string) (*seamsSession, error) {
	s := &seamsSession{}
	s.cmd = exec.CommandContext(ctx, "podman", seamsExecWord, "-i", container, "psql", "-h", "/tmp", "-U", "postgres", "-d", database, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1")
	s.cmd.Stderr = &s.stderr
	in, err := s.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := s.cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := s.cmd.Start(); err != nil {
		return nil, err
	}
	s.in, s.out = in, bufio.NewReader(out)
	if _, err := s.do("SELECT 1;"); err != nil {
		return nil, err
	}
	return s, nil
}

// do sends SQL and returns its output up to a marker line, so a statement that waits blocks here.
func (s *seamsSession) do(sql string) (string, error) {
	s.n++
	marker := fmt.Sprintf("seams-session-%d", s.n)
	if _, err := fmt.Fprintf(s.in, "%s\n\\echo %s\n", sql, marker); err != nil {
		return "", s.failed(err)
	}
	var lines []string
	for {
		line, err := s.out.ReadString('\n')
		if err != nil {
			return "", s.failed(err)
		}
		line = strings.TrimRight(line, "\n")
		if line == marker {
			return strings.Join(lines, "\n"), nil
		}
		lines = append(lines, line)
	}
}

func (s *seamsSession) failed(err error) error {
	_ = s.in.Close()
	_ = s.cmd.Wait()
	return fmt.Errorf("psql session: %v %s", err, strings.TrimSpace(s.stderr.String()))
}

// close ends the session; the server rolls back an open transaction and releases its locks.
func (s *seamsSession) close() {
	_ = s.in.Close()
	_ = s.cmd.Wait()
}
