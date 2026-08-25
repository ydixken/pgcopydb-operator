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

package pgcopydb

import (
	"testing"
	"time"
)

func TestLastErrorLine(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "last error wins over earlier ones",
			raw: `{"timestamp":"t","pid":1,"error_severity":"INFO","message":"STEP 1: starting"}
{"timestamp":"t","pid":1,"error_severity":"ERROR","message":"permission denied for function pg_replication_origin_drop"}
{"timestamp":"t","pid":1,"error_severity":"ERROR","message":"pgcopydb clone failed"}
{"timestamp":"t","pid":1,"error_severity":"INFO","message":"shutting down"}`,
			want: "pgcopydb clone failed",
		},
		{
			name: "fatal counts as error",
			raw:  `{"error_severity":"FATAL","message":"connection to source lost"}`,
			want: "connection to source lost",
		},
		{
			name: "non-JSON and partial lines are skipped",
			raw: `DROP PUBLICATION
{"error_severity":"ERROR","message":"boom"}
{"error_severity":"ERROR","mess`,
			want: "boom",
		},
		{name: "no error lines", raw: `{"error_severity":"INFO","message":"done"}`, want: ""},
		{name: "empty input", raw: "", want: ""},
		{name: "plain text only", raw: "panic: not json\nERROR: still not json\n", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := LastErrorLine([]byte(tc.raw)); got != tc.want {
				t.Fatalf("LastErrorLine() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCloneDone(t *testing.T) {
	// Line shapes as the operator fetches them: the runtime's RFC3339Nano
	// stamp, a space, then pgcopydb's JSON log line. The markers are the two
	// lines pgcopydb 0.18 logs when the clone phase of clone --follow ends
	// (cli_clone_follow.c, copydb_clone_database).
	const ts = "2026-08-09T10:00:00.000000000Z "
	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{
			name: "sentinel-apply marker means clone done",
			raw: ts + `{"error_severity":"INFO","message":"STEP 10: restore the post-data section to the target database"}` + "\n" +
				ts + `{"error_severity":"INFO","message":"Updating the pgcopydb.sentinel to enable applying changes"}`,
			want: true,
		},
		{
			name: "summary marker means clone done",
			raw:  ts + `{"error_severity":"INFO","message":"All step are now done, 12m34s elapsed"}`,
			want: true,
		},
		{
			name: "step banners alone are mid-copy",
			raw: ts + `{"error_severity":"INFO","message":"STEP 10: restore the post-data section to the target database"}` + "\n" +
				ts + `{"error_severity":"INFO","message":"reported write_lsn 0/5000"}`,
			want: false,
		},
		{
			name: "marker split across lines does not match",
			raw: ts + `{"error_severity":"INFO","message":"Updating the pgcopydb.sen` + "\n" +
				`tinel to enable applying changes"}`,
			want: false,
		},
		{name: "empty tail", raw: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CloneDone([]byte(tc.raw)); got != tc.want {
				t.Fatalf("CloneDone() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSupervisorDeath(t *testing.T) {
	// Line shapes as PodLogOptions Timestamps returns them: the runtime's
	// RFC3339Nano stamp, a space, then pgcopydb's JSON log line. The marker
	// messages are the ones proven live on 0.18.
	stamp := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	ts := stamp.Format(time.RFC3339Nano)
	cases := []struct {
		name  string
		raw   string
		want  time.Time
		found bool
	}{
		{
			name: "group termination FATAL is the marker",
			raw: ts + ` {"timestamp":"t","pid":1,"error_severity":"FATAL","message":"Terminating all processes in our process group"}` + "\n" +
				ts + ` {"timestamp":"t","pid":42,"error_severity":"INFO","message":"streamed up to write_lsn 0/5000"}`,
			want:  stamp,
			found: true,
		},
		{
			name:  "dead clone worker report is a marker too",
			raw:   ts + ` {"timestamp":"t","pid":1,"error_severity":"ERROR","message":"clone process 10 has terminated [6]"}`,
			want:  stamp,
			found: true,
		},
		{
			name: "first marker line dates the death",
			raw: ts + ` {"error_severity":"ERROR","message":"clone process 10 has terminated [6]"}` + "\n" +
				stamp.Add(time.Second).Format(time.RFC3339Nano) + ` {"error_severity":"FATAL","message":"Terminating all processes in our process group"}`,
			want:  stamp,
			found: true,
		},
		{
			name: "healthy stream has no marker",
			raw: ts + ` {"error_severity":"INFO","message":"reported write_lsn 0/5000 flush_lsn 0/5000"}` + "\n" +
				ts + ` {"error_severity":"INFO","message":"apply reached 0/5000"}`,
			found: false,
		},
		{
			name:  "marker without a parsable timestamp is skipped",
			raw:   `{"error_severity":"FATAL","message":"Terminating all processes in our process group"}`,
			found: false,
		},
		{name: "empty input", raw: "", found: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, found := SupervisorDeath([]byte(tc.raw))
			if found != tc.found {
				t.Fatalf("SupervisorDeath() found = %v, want %v", found, tc.found)
			}
			if found && !got.Equal(tc.want) {
				t.Fatalf("SupervisorDeath() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPermissionDeniedLine(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "pg_restore passthrough inside a structured line",
			raw: `{"error_severity":"INFO","message":"STEP 4: restore schema"}
{"error_severity":"ERROR","message":"pg_restore: error: could not execute query: ERROR:  permission denied for schema public"}
{"error_severity":"ERROR","message":"Failed to prepare schema on the target database, see above for details"}`,
			want: "pg_restore: error: could not execute query: ERROR:  permission denied for schema public",
		},
		{
			name: "pgcopydb wrapping a libpq error keeps no ERROR prefix",
			raw:  `{"error_severity":"ERROR","message":"permission denied for function pg_replication_origin_drop"}`,
			want: "permission denied for function pg_replication_origin_drop",
		},
		{
			name: "plain psql line without JSON wrapping",
			raw:  "ERROR:  permission denied for schema public\n",
			want: "ERROR:  permission denied for schema public",
		},
		{
			name: "sqlstate form matches regardless of severity text",
			raw:  `{"error_severity":"ERROR","message":"query failed: SQLSTATE 42501"}`,
			want: "query failed: SQLSTATE 42501",
		},
		{
			name: "info severity quoting the text is not a failure",
			raw:  `{"error_severity":"INFO","message":"will fail with permission denied unless granted"}`,
			want: "",
		},
		{
			name: "bare 42501 in data is not a permission error",
			raw:  `{"error_severity":"ERROR","message":"row 42501 rejected: value out of range"}`,
			want: "",
		},
		{
			name: "first match wins",
			raw: `{"error_severity":"ERROR","message":"ERROR:  permission denied for schema audit"}
{"error_severity":"ERROR","message":"ERROR:  permission denied for schema public"}`,
			want: "ERROR:  permission denied for schema audit",
		},
		{name: "unrelated error", raw: `{"error_severity":"ERROR","message":"deadlock detected"}`, want: ""},
		{
			name: "passthrough text marks severity even under a mild level",
			raw:  `{"error_severity":"WARNING","message":"subprocess said: ERROR:  permission denied for schema public"}`,
			want: "subprocess said: ERROR:  permission denied for schema public",
		},
		{name: "structured line without a message is skipped", raw: `{"error_severity":"ERROR"}` + "\npermission denied\n", want: ""},
		{name: "blank lines are skipped", raw: "\n\nERROR:  permission denied for schema public", want: "ERROR:  permission denied for schema public"},
		{name: "no input at all", raw: "", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PermissionDeniedLine([]byte(tc.raw)); got != tc.want {
				t.Fatalf("PermissionDeniedLine() = %q, want %q", got, tc.want)
			}
		})
	}
}
