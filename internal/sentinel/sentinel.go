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

// Package sentinel drives a running follow migration through pgcopydb's own
// CLI, exec-ed inside the worker pod (same filesystem as the catalogs, the
// sanctioned access path). Single-value selectors are used instead of
// `sentinel get --json` because of the known upstream bug where the JSON
// endpos field carries the startpos value (see docs/research).
package sentinel

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

// ZeroLSN is PostgreSQL's null LSN; the sentinel reports it for an unset
// endpos.
const ZeroLSN = "0/0"

// EndposSet reports whether lsn names a real cutover endpos: non-empty and
// not the null LSN.
func EndposSet(lsn string) bool { return lsn != "" && lsn != ZeroLSN }

// Client reads and drives the sentinel of one running migration pod.
type Client struct {
	exec *podexec.Exec
}

func New(exec *podexec.Exec) *Client { return &Client{exec: exec} }

// State is one sentinel sample plus the source WAL head.
type State struct {
	ApplyEnabled bool
	WriteLSN     string
	ReplayLSN    string
	Endpos       string
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

// Read samples the sentinel and the source WAL head from the worker pod.
// A nil State with nil error means "pod not ready to answer, keep the
// previous sample" (same contract as progress polling).
func (c *Client) Read(ctx context.Context, namespace, jobName string) (*State, error) {
	pod, err := c.exec.RunningPod(ctx, namespace, jobName)
	if err != nil {
		return nil, err
	}
	if pod == "" {
		return nil, nil
	}
	get := func(selector string) (string, error) {
		out, err := c.exec.InPod(ctx, namespace, pod,
			[]string{"pgcopydb", "stream", "sentinel", "get", selector, "--dir", pgcopydb.WorkDir})
		return strings.TrimSpace(string(out)), err
	}
	st := &State{}
	apply, err := get("--apply")
	if err != nil {
		// The sentinel exists only once follow setup ran; not an error.
		return nil, nil
	}
	st.ApplyEnabled = apply == "enabled"
	if st.WriteLSN, err = get("--write-lsn"); err != nil {
		return nil, nil
	}
	if st.ReplayLSN, err = get("--replay-lsn"); err != nil {
		return nil, nil
	}
	if st.Endpos, err = get("--endpos"); err != nil {
		return nil, nil
	}
	// The runner env carries the source URI; psql ships in the runner image.
	head, err := c.exec.InPod(ctx, namespace, pod,
		[]string{"sh", "-c", `psql "$PGCOPYDB_SOURCE_PGURI" -tAc "select pg_current_wal_flush_lsn()"`})
	if err == nil {
		st.SourceHead = strings.TrimSpace(string(head))
	}
	return st, nil
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
	out, err := c.exec.InPod(ctx, namespace, pod,
		[]string{"sh", "-c",
			`pgcopydb stream sentinel set endpos --current --source "$PGCOPYDB_SOURCE_PGURI" --dir ` + pgcopydb.WorkDir})
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// ToStatus converts a sample into the CR's replication status block.
func (s *State) ToStatus(slotName string) *v1alpha1.ReplicationStatus {
	if s == nil {
		return nil
	}
	rs := &v1alpha1.ReplicationStatus{
		SlotName:  slotName,
		WriteLSN:  s.WriteLSN,
		ReplayLSN: s.ReplayLSN,
	}
	if EndposSet(s.Endpos) {
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
