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

// Package sentinel watches and drives a running follow migration from inside
// the worker pod. Watching is psql on the source and the target only, because
// there is no such thing as a read-only pgcopydb command: every CLI
// invocation logs its own command line into the worker's SQLite catalog, and
// that commit invalidates whatever read snapshot the worker holds open. Its
// next write on that cursor then fails SQLITE_BUSY_SNAPSHOT, which no retry
// can clear, and the attempt dies (see docs/research/upstream-issues.md).
// Driving the cutover still goes through the CLI, which owns the sentinel
// table: one write, once, and there is no other way to ask for it.
package sentinel

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

// ZeroLSN is PostgreSQL's null LSN; the sentinel reports it for an unset
// endpos.
const ZeroLSN = "0/0"

// EndposSet reports whether lsn names a real cutover endpos.
func EndposSet(lsn string) bool { return lsnSet(lsn) }

// lsnSet reports whether lsn names a real position: non-empty and not the
// null LSN.
func lsnSet(lsn string) bool { return lsn != "" && lsn != ZeroLSN }

// Client reads and drives the sentinel of one running migration pod.
type Client struct {
	exec *podexec.Exec
}

func New(exec *podexec.Exec) *Client { return &Client{exec: exec} }

// inPodSh runs one shell command in the worker pod with conn.URIRecover
// prefixed. Every exec in this package routes through here so no call site
// can forget the recovery secretRef connections depend on.
func (c *Client) inPodSh(ctx context.Context, namespace, pod, script string) ([]byte, error) {
	return c.exec.InPod(ctx, namespace, pod, []string{"sh", "-c", conn.URIRecover() + script})
}

// State is one replication sample. Endpos is never read from the worker: the
// operator sets it and keeps it in status.replication.
type State struct {
	WriteLSN  string
	ReplayLSN string
	Endpos    string
	// SourceHead is pg_current_wal_flush_lsn() on the source, the reference
	// for lag: replication is caught up when SourceHead - ReplayLSN is small.
	SourceHead string
}

// Lag returns SourceHead minus ReplayLSN in bytes, or -1 when unknown.
func (s State) Lag() int64 {
	head, err1 := ParseLSN(s.SourceHead)
	replay, err2 := ParseLSN(s.ReplayLSN)
	if err1 != nil || err2 != nil || head < replay {
		return -1
	}
	return int64(head - replay)
}

// readScript samples the stream in one exec, from the two databases rather
// than from the worker's catalogs.
//
// write_lsn prefers the walsender's own number but falls back to the slot's
// confirmed flush position, because PostgreSQL blanks every walsender detail
// column in pg_stat_replication for a role without pg_read_all_stats, its own
// row included (verified on 17). The slot row stays readable, and it is the
// same feedback, one confirmation behind. replay_lsn comes from the target's
// replication origin, the position the drain verification trusts too (see
// buildVerifyJob for why the sentinel's own replay_lsn does not mean what its
// name suggests); the source-side value is the fallback for the window before
// the origin has applied anything. Both sides are best effort and the script
// always exits 0: psql reports a SQL error in its exit status, and one side
// failing must not cost the other side's line.
func readScript(slot string) string {
	return `psql "$PGCOPYDB_SOURCE_PGURI" -XAtq -F ' ' ` +
		`-c "select 'source_head=' || pg_current_wal_flush_lsn()" ` +
		`-c "select 'write_lsn=' || coalesce(coalesce(r.write_lsn, s.confirmed_flush_lsn)::text, ''), ` +
		`'source_replay_lsn=' || coalesce(r.replay_lsn::text, '') from pg_replication_slots s ` +
		`left join pg_stat_replication r on r.pid = s.active_pid where s.slot_name = '` + slot + `'"; ` +
		`psql "$PGCOPYDB_TARGET_PGURI" -XAtq ` +
		`-c "select 'replay_lsn=' || coalesce(pg_replication_origin_progress(roname, true)::text, '') ` +
		`from pg_replication_origin where roname = '` + slot + `'"; exit 0`
}

// parseSample reads the labelled values out of the script's output. Anything
// that is not an LSN is ignored, so a missing row, a blank column, and a psql
// error line all land in the same place: absent.
func parseSample(out []byte) *State {
	vals := map[string]string{}
	for field := range strings.FieldsSeq(string(out)) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		if _, err := ParseLSN(v); err != nil {
			continue
		}
		vals[k] = v
	}
	if vals["write_lsn"] == "" {
		// No slot row, so nothing was learned this pass. Reporting the WAL
		// head alone would blank the LSNs already in status.
		return nil
	}
	st := &State{WriteLSN: vals["write_lsn"], ReplayLSN: vals["replay_lsn"], SourceHead: vals["source_head"]}
	if st.ReplayLSN == "" {
		st.ReplayLSN = vals["source_replay_lsn"]
	}
	return st
}

// Read samples the stream of one running follow migration. slotName names
// both the source slot and the target origin, which the operator keeps
// identical (see effectiveSlotName). A nil State with a nil error means "no
// sample, keep the previous one" (same contract as progress polling).
func (c *Client) Read(ctx context.Context, namespace, jobName, slotName string) (*State, error) {
	pod, err := c.exec.RunningPod(ctx, namespace, jobName)
	if err != nil {
		return nil, err
	}
	if pod == "" {
		return nil, nil
	}
	out, err := c.inPodSh(ctx, namespace, pod, readScript(slotName))
	if err != nil {
		// The pod can refuse an exec while it starts or terminates; the next
		// pass retries.
		return nil, nil
	}
	return parseSample(out), nil
}

// SetEndposCurrent freezes the stream at the source's current flush LSN; the
// follow process drains to it and exits 0. Idempotent per pgcopydb docs
// (endpos may move forward, never needs to move back).
func (c *Client) SetEndposCurrent(ctx context.Context, namespace, jobName string) (string, error) {
	pod, err := c.exec.RunningPod(ctx, namespace, jobName)
	if err != nil {
		return "", err
	}
	if pod == "" {
		return "", fmt.Errorf("no running worker pod for Job %s", jobName)
	}
	out, err := c.inPodSh(ctx, namespace, pod,
		`pgcopydb stream sentinel set endpos --current --source "$PGCOPYDB_SOURCE_PGURI" --dir `+pgcopydb.WorkDir)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// NudgeEndpos emits a tiny non-transactional logical message on the source.
// pgcopydb 0.18 evaluates endpos only against WAL it receives, so on a fully
// idle source a freshly set endpos is never reached and the drain hangs; one
// throwaway record gives the receiver something to evaluate (see
// docs/research/upstream-issues.md). pg_logical_emit_message carries the
// default PUBLIC execute grant, so the migration user can always call it.
// Idempotent and harmless under real traffic. Best effort like Read: no
// running pod is no error, and callers only debug-log failures.
func (c *Client) NudgeEndpos(ctx context.Context, namespace, jobName string) error {
	pod, err := c.exec.RunningPod(ctx, namespace, jobName)
	if err != nil || pod == "" {
		return err
	}
	_, err = c.inPodSh(ctx, namespace, pod,
		`psql "$PGCOPYDB_SOURCE_PGURI" -Xqtc "select pg_logical_emit_message(false, 'pgcopydb-operator', 'endpos-nudge')"`)
	return err
}

// ToStatus converts a sample into the CR's replication status block.
func (s *State) ToStatus(slotName string) *v1beta1.ReplicationStatus {
	if s == nil {
		return nil
	}
	// An unknown position is absent, never 0/0: the metric contract in
	// docs/operations/monitoring.md is that a missing value has no sample,
	// and a null LSN would plot as a real one at the bottom of the graph.
	rs := &v1beta1.ReplicationStatus{SlotName: slotName}
	if lsnSet(s.WriteLSN) {
		rs.WriteLSN = s.WriteLSN
	}
	if lsnSet(s.ReplayLSN) {
		rs.ReplayLSN = s.ReplayLSN
	}
	if lsnSet(s.Endpos) {
		rs.Endpos = s.Endpos
	}
	if lag := s.Lag(); lag >= 0 {
		rs.LagBytes = &lag
	}
	return rs
}

// ParseLSN converts a PostgreSQL LSN (X/Y, hex) to a byte position.
func ParseLSN(lsn string) (uint64, error) {
	hi, lo, ok := strings.Cut(strings.TrimSpace(lsn), "/")
	if !ok {
		return 0, fmt.Errorf("not an LSN: %q", lsn)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("not an LSN: %q", lsn)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("not an LSN: %q", lsn)
	}
	return h<<32 | l, nil
}
