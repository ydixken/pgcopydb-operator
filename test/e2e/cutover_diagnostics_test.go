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

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Presence counts distinguish absent feedback from a real zero byte position.
const cutoverSourceProjection = "SELECT concat_ws(' ', pg_wal_lsn_diff(pg_current_wal_flush_lsn(), '0/0'), " +
	"coalesce(pg_wal_lsn_diff(r.write_lsn, '0/0'), 0), " +
	"coalesce(pg_wal_lsn_diff(r.replay_lsn, '0/0'), 0), " +
	"coalesce(pg_wal_lsn_diff(s.confirmed_flush_lsn, '0/0'), 0), " +
	"(r.write_lsn IS NOT NULL)::int, (r.replay_lsn IS NOT NULL)::int, " +
	"(s.confirmed_flush_lsn IS NOT NULL)::int, s.active::int, coalesce((r.state='streaming')::int, 0))"

const cutoverSourceUnavailable = "source_probe_unavailable"

func cutoverSourceSnapshot(raw string) string {
	var counts [9]uint64
	fields := strings.Fields(raw)
	if len(fields) != len(counts) {
		return cutoverSourceUnavailable
	}
	for i, field := range fields {
		n, err := strconv.ParseUint(field, 10, 64)
		if err != nil || (i >= 4 && n > 1) {
			return cutoverSourceUnavailable
		}
		counts[i] = n
	}
	for i := 1; i <= 3; i++ {
		if counts[i+3] == 0 && counts[i] != 0 {
			return cutoverSourceUnavailable
		}
	}
	return fmt.Sprintf("source_head/write/replay/confirmed_bytes=%d/%d/%d/%d "+
		"source_write/replay/confirmed_samples=%d/%d/%d source_active/streaming=%d/%d",
		counts[0], counts[1], counts[2], counts[3], counts[4], counts[5], counts[6], counts[7], counts[8])
}

func cutoverSnapshotDue(now time.Time, next *time.Time, deadline time.Time) bool {
	if now.Before(*next) || !now.Before(deadline) {
		return false
	}
	// Do not bunch missed samples after a slow probe or extend the spec's deadline.
	*next = now.Add(30 * time.Second)
	return true
}

func TestCutoverDiagnosticSchedule(t *testing.T) {
	start := time.Unix(0, 0)
	for _, budget := range []time.Duration{300 * time.Second, 10 * time.Minute} {
		next := start
		deadline := start.Add(budget)
		count := 0
		for elapsed := time.Duration(0); elapsed <= budget+time.Second; elapsed += time.Second {
			got := cutoverSnapshotDue(start.Add(elapsed), &next, deadline)
			want := elapsed < budget && elapsed%(30*time.Second) == 0
			if got != want {
				t.Fatalf("budget=%s elapsed=%s: due=%t, want %t", budget, elapsed, got, want)
			}
			if got {
				count++
			}
		}
		if count != int(budget/(30*time.Second)) {
			t.Fatalf("budget=%s: snapshots=%d", budget, count)
		}
	}
	next := start
	deadline := start.Add(time.Minute)
	if !cutoverSnapshotDue(start.Add(45*time.Second), &next, deadline) ||
		!next.Equal(start.Add(75*time.Second)) ||
		cutoverSnapshotDue(start.Add(46*time.Second), &next, deadline) ||
		cutoverSnapshotDue(start.Add(75*time.Second), &next, deadline) {
		t.Fatal("a delayed poll replayed missed samples or exceeded the deadline")
	}
}

func TestCutoverDiagnosticProjection(t *testing.T) {
	for _, tc := range []struct {
		raw, want string
	}{
		{"100 90 80 70 1 1 1 1 1\n", "source_head/write/replay/confirmed_bytes=100/90/80/70 " +
			"source_write/replay/confirmed_samples=1/1/1 source_active/streaming=1/1"},
		{"100 0 0 0 0 0 0 0 0", "source_head/write/replay/confirmed_bytes=100/0/0/0 " +
			"source_write/replay/confirmed_samples=0/0/0 source_active/streaming=0/0"},
		{"100 0 0 0 1 1 1 1 0", "source_head/write/replay/confirmed_bytes=100/0/0/0 " +
			"source_write/replay/confirmed_samples=1/1/1 source_active/streaming=1/0"},
		{"", ""},
		{"100 90 80 70", ""},
		{"100 90 80 70 1 1 1 1 1 PRIVATE_OUTPUT", ""},
		{"100 90 80 70 1 1 1 1 PRIVATE_OUTPUT", ""},
		{"100 90 80 70 1 1 1 1 2", ""},
		{"100 90 80 70 0 1 1 1 1", ""},
		{"100 90 -1 70 1 1 1 1 1", ""},
		{"18446744073709551616 90 80 70 1 1 1 1 1", ""},
		{"100 90 80 70 1 1 1 1 1\n100 90 80 70 1 1 1 1 1", ""},
	} {
		want := tc.want
		if want == "" {
			want = cutoverSourceUnavailable
		}
		if got := cutoverSourceSnapshot(tc.raw); got != want {
			t.Fatalf("projection=%q, want %q", got, want)
		}
	}
}

func TestCutoverDiagnosticSQL(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for the diagnostic projection")
	}
	for _, tc := range []struct {
		name, values, suffix string
	}{
		{"feedback", "'0/10'::pg_lsn, '0/8'::pg_lsn, 'streaming'", "16 8 4 1 1 1 1 1"},
		{"missing", "NULL::pg_lsn, NULL::pg_lsn, NULL::text", "0 0 4 0 0 1 1 0"},
		{"zero", "'0/0'::pg_lsn, '0/0'::pg_lsn, 'catchup'", "0 0 4 1 1 1 1 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := cutoverSourceProjection + " FROM (VALUES (" + tc.values +
				")) AS r(write_lsn, replay_lsn, state) CROSS JOIN " +
				"(VALUES ('0/4'::pg_lsn, true)) AS s(confirmed_flush_lsn, active)"
			commandCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := commandOutput(commandCtx, exec.CommandContext, "psql", uri,
				"-XAtq", "-v", "ON_ERROR_STOP=1", "-c", query)
			if err != nil {
				t.Fatal("diagnostic SQL query failed")
			}
			raw := strings.TrimSpace(string(out))
			if cutoverSourceSnapshot(raw) == cutoverSourceUnavailable || !strings.HasSuffix(raw, " "+tc.suffix) {
				t.Fatal("diagnostic SQL did not return the expected byte positions and presence counts")
			}
		})
	}
}
