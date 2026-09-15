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
	"strings"
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
			name: "sqlstate form matches on an error-severity line",
			raw:  `{"error_severity":"ERROR","message":"query failed: SQLSTATE 42501"}`,
			want: "query failed: SQLSTATE 42501",
		},
		{
			name: "sqlstate on a mild line does not classify",
			raw:  `{"error_severity":"INFO","message":"note: SQLSTATE 42501 seen earlier"}`,
			want: "",
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
			name: "relation ownership error does not classify",
			raw:  `{"error_severity":"ERROR","message":"must be owner of relation orders"}`,
			want: "",
		},
		{
			// pg_restore relays tolerated per-object errors this way while
			// continuing; classifying them would kill retryable attempts.
			name: "warning quoting ERROR text does not classify",
			raw:  `{"error_severity":"WARNING","message":"subprocess said: ERROR:  permission denied for schema public"}`,
			want: "",
		},
		{
			// libpq connect-time refusals arrive FATAL under a mild wrapper
			// severity and are deterministic.
			name: "warning quoting a FATAL libpq passthrough classifies",
			raw:  `{"error_severity":"WARNING","message":"connection attempt failed: FATAL:  permission denied for database app"}`,
			want: "connection attempt failed: FATAL:  permission denied for database app",
		},
		{
			name: "tolerated line outside the terminal window does not classify",
			raw: `{"error_severity":"ERROR","message":"ERROR:  permission denied for schema public"}` + "\n" +
				strings.Repeat(`{"error_severity":"INFO","message":"COPY progress"}`+"\n", permissionWindow) +
				`{"error_severity":"ERROR","message":"worker was killed: out of memory"}`,
			want: "",
		},
		{
			name: "terminal permission chain classifies end to end",
			raw: strings.Repeat(`{"error_severity":"INFO","message":"COPY progress"}`+"\n", permissionWindow) +
				`{"error_severity":"ERROR","message":"pg_restore: error: could not execute query: ERROR:  permission denied for schema public"}` + "\n" +
				`{"error_severity":"WARNING","message":"pg_restore: warning: errors ignored on restore: 6"}` + "\n" +
				`{"error_severity":"ERROR","message":"clone process 10 has terminated [6]"}`,
			want: "pg_restore: error: could not execute query: ERROR:  permission denied for schema public",
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

func TestPermissionDeniedLineExtensionOwnership(t *testing.T) {
	// Terminal tails from the named container-reproduction captures, starting at the ownership error.
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "aadc4bf-affected-jobs1.clone.log",
			raw: `{"timestamp":"2026-09-15 15:19:43.280","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"string_utils.c","file_line_num":500,"message":"pg_restore: error: could not execute query: ERROR:  must be owner of extension unaccent"}
{"timestamp":"2026-09-15 15:19:43.280","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"string_utils.c","file_line_num":500,"message":"Command was: DROP EXTENSION IF EXISTS unaccent;"}
{"timestamp":"2026-09-15 15:19:43.280","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"pgcmd.c","file_line_num":1044,"message":"Failed to run pg_restore: exit code 1"}
{"timestamp":"2026-09-15 15:19:43.280","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":1034,"message":"Failed to prepare schema on the target database, see above for details"}
{"timestamp":"2026-09-15 15:19:43.281","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":942,"message":"Failed to clone source database, see above for details"}
{"timestamp":"2026-09-15 15:19:43.344","pid":56,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":1202,"message":"clone process 63 has terminated [6]"}`,
			want: "pg_restore: error: could not execute query: ERROR:  must be owner of extension unaccent",
		},
		{
			name: "aadc4bf-no-drop-affected-jobs1.clone.log",
			raw: `{"timestamp":"2026-09-15 15:20:23.332","pid":809,"error_level":7,"error_severity":"ERROR","file_name":"string_utils.c","file_line_num":500,"message":"pg_restore: error: could not execute query: ERROR:  must be owner of extension citext"}
{"timestamp":"2026-09-15 15:20:23.333","pid":809,"error_level":7,"error_severity":"ERROR","file_name":"string_utils.c","file_line_num":500,"message":"Command was: COMMENT ON EXTENSION citext IS 'data type for case-insensitive character strings';"}
{"timestamp":"2026-09-15 15:20:23.334","pid":809,"error_level":7,"error_severity":"ERROR","file_name":"pgcmd.c","file_line_num":1044,"message":"Failed to run pg_restore: exit code 1"}
{"timestamp":"2026-09-15 15:20:23.334","pid":809,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":1034,"message":"Failed to prepare schema on the target database, see above for details"}
{"timestamp":"2026-09-15 15:20:23.334","pid":809,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":942,"message":"Failed to clone source database, see above for details"}
{"timestamp":"2026-09-15 15:20:23.380","pid":802,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":1202,"message":"clone process 809 has terminated [6]"}`,
			want: "pg_restore: error: could not execute query: ERROR:  must be owner of extension citext",
		},
		{
			name: "e37d2bd-affected-jobs1.clone.log",
			raw: `{"timestamp":"2026-09-15 15:18:21.868","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"string_utils.c","file_line_num":500,"message":"pg_restore: error: could not execute query: ERROR:  must be owner of extension unaccent"}
{"timestamp":"2026-09-15 15:18:21.868","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"string_utils.c","file_line_num":500,"message":"Command was: DROP EXTENSION IF EXISTS unaccent;"}
{"timestamp":"2026-09-15 15:18:21.868","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"pgcmd.c","file_line_num":1044,"message":"Failed to run pg_restore: exit code 1"}
{"timestamp":"2026-09-15 15:18:21.868","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":1034,"message":"Failed to prepare schema on the target database, see above for details"}
{"timestamp":"2026-09-15 15:18:21.868","pid":63,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":942,"message":"Failed to clone source database, see above for details"}
{"timestamp":"2026-09-15 15:18:21.929","pid":56,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":1202,"message":"clone process 63 has terminated [6]"}`,
			want: "pg_restore: error: could not execute query: ERROR:  must be owner of extension unaccent",
		},
		{
			name: "e37d2bd-no-drop-affected-jobs1.clone.log",
			raw: `{"timestamp":"2026-09-15 15:19:00.609","pid":810,"error_level":7,"error_severity":"ERROR","file_name":"string_utils.c","file_line_num":500,"message":"pg_restore: error: could not execute query: ERROR:  must be owner of extension citext"}
{"timestamp":"2026-09-15 15:19:00.609","pid":810,"error_level":7,"error_severity":"ERROR","file_name":"string_utils.c","file_line_num":500,"message":"Command was: COMMENT ON EXTENSION citext IS 'data type for case-insensitive character strings';"}
{"timestamp":"2026-09-15 15:19:00.609","pid":810,"error_level":7,"error_severity":"ERROR","file_name":"pgcmd.c","file_line_num":1044,"message":"Failed to run pg_restore: exit code 1"}
{"timestamp":"2026-09-15 15:19:00.609","pid":810,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":1034,"message":"Failed to prepare schema on the target database, see above for details"}
{"timestamp":"2026-09-15 15:19:00.609","pid":810,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":942,"message":"Failed to clone source database, see above for details"}
{"timestamp":"2026-09-15 15:19:00.713","pid":803,"error_level":7,"error_severity":"ERROR","file_name":"cli_clone_follow.c","file_line_num":1202,"message":"clone process 810 has terminated [6]"}`,
			want: "pg_restore: error: could not execute query: ERROR:  must be owner of extension citext",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PermissionDeniedLine([]byte(tc.raw)); got != tc.want {
				t.Errorf("PermissionDeniedLine() = %q, want %q", got, tc.want)
			}
			_, following, ok := strings.Cut(tc.raw, "\n")
			if !ok || following == "" {
				t.Fatal("fixture must include the command and follow-on errors")
			}
			if got := PermissionDeniedLine([]byte(following)); got != "" {
				t.Errorf("command and follow-on errors classified as permission denied: %q", got)
			}
		})
	}
}
