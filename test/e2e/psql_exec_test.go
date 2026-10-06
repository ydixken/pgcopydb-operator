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
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type execErrorCase struct {
	name   string
	stderr string
}

var safeExecErrorCases = []execErrorCase{
	{
		name:   "front-end TLS timeout",
		stderr: "Unable to connect to the server: net/http: TLS handshake timeout",
	},
	{
		name:   "front-end refusal",
		stderr: "The connection to the server 192.0.2.4:6443 was refused - did you specify the right host or port?",
	},
	{
		name:   "API-server backend dial",
		stderr: "error: Internal error occurred: error dialing backend: dial tcp 192.0.2.5:10250: i/o timeout",
	},
	{
		name: "SPDY TLS timeout",
		stderr: `error: error sending request: Post "https://192.0.2.4:6443/api/v1/namespaces/ns/pods/p/exec": ` +
			`net/http: TLS handshake timeout`,
	},
	{
		name: "SPDY refusal",
		stderr: `error: error sending request: Post "https://192.0.2.4:6443/api/v1/namespaces/ns/pods/p/exec": ` +
			`dial tcp 192.0.2.4:6443: connect: connection refused`,
	},
}

var terminalExecErrorCases = []execErrorCase{
	{name: "empty", stderr: ""},
	{name: "sending request EOF", stderr: "error: error sending request: EOF"},
	{name: "front-end EOF", stderr: "Unable to connect to the server: EOF"},
	{name: "bare TLS fragment", stderr: "TLS handshake timeout"},
	{name: "bare refusal fragment", stderr: "connection refused"},
	{name: "remote psql stderr", stderr: "psql: error: connection to server failed: connection refused"},
	{
		name:   "multiline after safe line",
		stderr: "Unable to connect to the server: net/http: TLS handshake timeout\nremote output",
	},
	{
		name:   "multiline before safe line",
		stderr: "remote output\nUnable to connect to the server: net/http: TLS handshake timeout",
	},
	{
		name:   "carriage return without LF",
		stderr: "Unable to connect to the server: net/http: TLS handshake timeout\r",
	},
	{
		name:   "empty refusal endpoint",
		stderr: "The connection to the server  was refused - did you specify the right host or port?",
	},
	{name: "empty backend detail", stderr: "error: Internal error occurred: error dialing backend: "},
	{name: "blank backend detail", stderr: "error: Internal error occurred: error dialing backend:   "},
	{
		name:   "empty SPDY URL",
		stderr: `error: error sending request: Post "": net/http: TLS handshake timeout`,
	},
	{
		name:   "empty SPDY address",
		stderr: `error: error sending request: Post "https://192.0.2.4/exec": dial tcp : connect: connection refused`,
	},
	{
		name:   "two final line feeds",
		stderr: "Unable to connect to the server: net/http: TLS handshake timeout\n\n",
	},
}

const (
	psqlExecHelperEnv       = "PSQL_EXEC_HELPER"
	psqlExecHelperStdoutEnv = "PSQL_EXEC_HELPER_STDOUT"
	psqlExecHelperStderrEnv = "PSQL_EXEC_HELPER_STDERR"
	psqlExecHelperBlockEnv  = "PSQL_EXEC_HELPER_BLOCK"
	psqlExecHelperExitEnv   = "PSQL_EXEC_HELPER_EXIT"
	psqlExecTestTimeout     = 1_000 * time.Millisecond
	psqlExecKubectl         = "kubectl"
	psqlExecTestPod         = "source-1"
	psqlExecTestDatabase    = "app"
)

type psqlExecResult struct {
	stdout   string
	stderr   string
	block    bool
	exitCode int
}

type psqlExecCall struct {
	name string
	args []string
}

type psqlExecState struct {
	t *testing.T

	mu       sync.Mutex
	results  []psqlExecResult
	calls    []psqlExecCall
	contexts []context.Context
	commands []*exec.Cmd
}

func newPSQLExecCommand(t *testing.T, results ...psqlExecResult) (commandFactory, *psqlExecState) {
	t.Helper()
	state := &psqlExecState{t: t, results: append([]psqlExecResult(nil), results...)}
	return state.command, state
}

func (s *psqlExecState) command(ctx context.Context, name string, args ...string) *exec.Cmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.results) == 0 {
		s.t.Errorf("unexpected command: %s %v", name, args)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
		s.commands = append(s.commands, cmd)
		return cmd
	}
	result := s.results[0]
	s.results = s.results[1:]
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPSQLExecHelper$")
	cmd.Env = append(os.Environ(),
		"GORACE=atexit_sleep_ms=0",
		psqlExecHelperEnv+"=1",
		psqlExecHelperStdoutEnv+"="+result.stdout,
		psqlExecHelperStderrEnv+"="+result.stderr,
		psqlExecHelperBlockEnv+"="+strconv.FormatBool(result.block),
		psqlExecHelperExitEnv+"="+strconv.Itoa(result.exitCode),
	)
	s.calls = append(s.calls, psqlExecCall{name: name, args: append([]string(nil), args...)})
	s.contexts = append(s.contexts, ctx)
	s.commands = append(s.commands, cmd)
	return cmd
}

func (s *psqlExecState) snapshot() ([]psqlExecCall, []context.Context, []*exec.Cmd) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]psqlExecCall(nil), s.calls...),
		append([]context.Context(nil), s.contexts...),
		append([]*exec.Cmd(nil), s.commands...)
}

func TestPSQLExecHelper(t *testing.T) {
	if os.Getenv(psqlExecHelperEnv) != "1" {
		return
	}
	if _, err := io.WriteString(os.Stdout, os.Getenv(psqlExecHelperStdoutEnv)); err != nil {
		t.Fatalf("write stdout: %v", err)
	}
	if _, err := io.WriteString(os.Stderr, os.Getenv(psqlExecHelperStderrEnv)); err != nil {
		t.Fatalf("write stderr: %v", err)
	}
	if os.Getenv(psqlExecHelperBlockEnv) == "true" {
		time.Sleep(time.Hour)
	}
	exitCode, err := strconv.Atoi(os.Getenv(psqlExecHelperExitEnv))
	if err != nil {
		t.Fatalf("parse exit code: %v", err)
	}
	os.Exit(exitCode)
}

func requirePSQLCommandsReaped(t *testing.T, commands []*exec.Cmd) {
	t.Helper()
	for i, cmd := range commands {
		if cmd.Process == nil || cmd.ProcessState == nil {
			t.Fatalf("command %d was not started and reaped: process=%v state=%v", i+1, cmd.Process, cmd.ProcessState)
		}
	}
}

func TestTransientExecError(t *testing.T) {
	tests := append([]execErrorCase(nil), safeExecErrorCases...)
	tests = append(tests,
		execErrorCase{
			name:   "one final LF",
			stderr: safeExecErrorCases[0].stderr + "\n",
		},
		execErrorCase{
			name:   "one final CRLF",
			stderr: safeExecErrorCases[0].stderr + "\r\n",
		},
	)
	for _, tt := range tests {
		t.Run("accepts "+tt.name, func(t *testing.T) {
			if !transientExecError(tt.stderr) {
				t.Fatalf("transientExecError(%q) = false, want true", tt.stderr)
			}
		})
	}

	for _, tt := range terminalExecErrorCases {
		t.Run("rejects "+tt.name, func(t *testing.T) {
			if transientExecError(tt.stderr) {
				t.Fatalf("transientExecError(%q) = true, want false", tt.stderr)
			}
		})
	}
}

func TestPSQLDBErrTimeoutIsTerminalAndReaped(t *testing.T) {
	const (
		cluster = "source-cluster"
		pod     = "source-7"
		sql     = "INSERT INTO orders VALUES (1)"
	)
	command, state := newPSQLExecCommand(t, psqlExecResult{
		stderr: safeExecErrorCases[0].stderr,
		block:  true,
	})
	primaryCalls := 0
	waits := 0

	_, err := psqlDBErrWith(
		cluster,
		psqlExecTestDatabase,
		sql,
		func(got string) string {
			primaryCalls++
			if got != cluster {
				t.Fatalf("primary cluster = %q, want %q", got, cluster)
			}
			return pod
		},
		func(time.Duration) { waits++ },
		command,
		psqlExecTestTimeout,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	for _, want := range []string{cluster, pod, sql, psqlExecTestTimeout.String()} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want %q", err, want)
		}
	}
	calls, _, commands := state.snapshot()
	if len(calls) != 1 || primaryCalls != 1 || waits != 0 {
		t.Fatalf("calls=%d primary=%d waits=%d, want 1, 1, 0", len(calls), primaryCalls, waits)
	}
	requirePSQLCommandsReaped(t, commands)
}

func TestPSQLDBErrRetriesEachSafeFailure(t *testing.T) {
	for _, tt := range safeExecErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			command, state := newPSQLExecCommand(t,
				psqlExecResult{stderr: tt.stderr, exitCode: 17},
				psqlExecResult{stdout: " 42 \n"},
			)
			var pods []string
			var waits []time.Duration
			out, err := psqlDBErrWith(
				"source-cluster",
				psqlExecTestDatabase,
				"SELECT 42",
				func(string) string {
					pod := fmt.Sprintf("source-%d", len(pods)+1)
					pods = append(pods, pod)
					return pod
				},
				func(delay time.Duration) { waits = append(waits, delay) },
				command,
				psqlExecTestTimeout,
			)
			if err != nil || out != "42" {
				t.Fatalf("out=%q err=%v, want trimmed 42", out, err)
			}
			if fmt.Sprint(pods) != "[source-1 source-2]" || fmt.Sprint(waits) != "[5s]" {
				t.Fatalf("pods=%v waits=%v", pods, waits)
			}
			calls, contexts, commands := state.snapshot()
			if len(calls) != 2 || len(contexts) != 2 || contexts[0] == contexts[1] {
				t.Fatalf("calls=%d contexts=%d fresh=%v", len(calls), len(contexts), contexts[0] != contexts[1])
			}
			for i, ctx := range contexts {
				if !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatalf("context %d error = %v, want canceled after attempt", i+1, ctx.Err())
				}
			}
			requirePSQLCommandsReaped(t, commands)
		})
	}
}

func TestPSQLDBErrDoesNotRetryAmbiguousFailures(t *testing.T) {
	for _, tt := range terminalExecErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			command, state := newPSQLExecCommand(t,
				psqlExecResult{stderr: tt.stderr, exitCode: 17},
				psqlExecResult{stdout: "must not run"},
			)
			primaryCalls := 0
			waits := 0
			_, err := psqlDBErrWith(
				"source-cluster",
				psqlExecTestDatabase,
				"UPDATE orders SET amount = 2",
				func(string) string {
					primaryCalls++
					return psqlExecTestPod
				},
				func(time.Duration) { waits++ },
				command,
				psqlExecTestTimeout,
			)
			if err == nil {
				t.Fatal("error = nil, want terminal command failure")
			}
			calls, _, commands := state.snapshot()
			if len(calls) != 1 || primaryCalls != 1 || waits != 0 {
				t.Fatalf("calls=%d primary=%d waits=%d, want 1, 1, 0", len(calls), primaryCalls, waits)
			}
			requirePSQLCommandsReaped(t, commands)
		})
	}
}

func TestPSQLDBErrStopsAtThreeSafeFailures(t *testing.T) {
	results := []psqlExecResult{
		{stderr: safeExecErrorCases[0].stderr, exitCode: 17},
		{stderr: safeExecErrorCases[0].stderr, exitCode: 17},
		{stderr: safeExecErrorCases[0].stderr, exitCode: 17},
		{stdout: "must not run"},
	}
	command, state := newPSQLExecCommand(t, results...)
	primaryCalls := 0
	var waits []time.Duration
	_, err := psqlDBErrWith(
		"source-cluster",
		psqlExecTestDatabase,
		"SELECT 1",
		func(string) string {
			primaryCalls++
			return fmt.Sprintf("source-%d", primaryCalls)
		},
		func(delay time.Duration) { waits = append(waits, delay) },
		command,
		psqlExecTestTimeout,
	)
	if err == nil {
		t.Fatal("error = nil, want exhausted retry failure")
	}
	calls, _, commands := state.snapshot()
	if len(calls) != 3 || primaryCalls != 3 || fmt.Sprint(waits) != "[5s 5s]" {
		t.Fatalf("calls=%d primary=%d waits=%v, want 3, 3, [5s 5s]", len(calls), primaryCalls, waits)
	}
	requirePSQLCommandsReaped(t, commands)
}

func TestPSQLDBErrReturnsTrimmedOutputAndExpectedArguments(t *testing.T) {
	command, state := newPSQLExecCommand(t, psqlExecResult{stdout: "  value \n"})
	out, err := psqlDBErrWith(
		"source-cluster",
		"inventory",
		"SELECT value FROM settings",
		func(string) string { return "source-2" },
		func(time.Duration) { t.Fatal("retry wait called after success") },
		command,
		psqlExecTestTimeout,
	)
	if err != nil || out != "value" {
		t.Fatalf("out=%q err=%v, want trimmed value", out, err)
	}
	calls, _, commands := state.snapshot()
	want := psqlExecCall{
		name: psqlExecKubectl,
		args: []string{
			psqlExecSubcommand, "-n", nsE2E, "source-2", "-c", postgresContainer, "--",
			psqlExecProgram, "-U", postgresContainer, "inventory", "-tAc", "SELECT value FROM settings",
		},
	}
	if len(calls) != 1 || !reflect.DeepEqual(calls[0], want) {
		t.Fatalf("calls=%#v, want %#v", calls, want)
	}
	requirePSQLCommandsReaped(t, commands)
}

// TestPSQLArgvKeepsCNPGCommands pins psqlArgv output for each probe site's flag
// shape to the argv that site spelled out before.
func TestPSQLArgvKeepsCNPGCommands(t *testing.T) {
	const (
		pod           = "target-1"
		db            = "app"
		superuser     = "postgres"
		sql           = "SELECT 1"
		oneShot       = "-tAc"
		stopOnErr     = "ON_ERROR_STOP=1"
		bounded       = "SET statement_timeout=3000"
		silence       = `exec 2>/dev/null; exec "$@"`
		wantExec      = "exec"
		wantPSQL      = "psql"
		wantContainer = "postgres"
	)
	for _, tc := range []struct {
		name      string
		got, want []string
	}{
		{
			name: "one statement",
			got:  psqlArgv(sourceCluster, psqlExecTestPod, db, false, oneShot, sql),
			want: []string{wantExec, "-n", nsE2E, psqlExecTestPod, "-c", wantContainer, "--",
				wantPSQL, "-U", superuser, db, oneShot, sql},
		},
		{
			name: "statement stream",
			got:  psqlArgv(sourceCluster, psqlExecTestPod, db, true, "-q", "-v", stopOnErr),
			want: []string{wantExec, "-i", "-n", nsE2E, psqlExecTestPod, "-c", wantContainer, "--",
				wantPSQL, "-U", superuser, db, "-q", "-v", stopOnErr},
		},
		{
			name: "bounded probe",
			got:  psqlArgv(targetCluster, pod, db, false, "-XqtA", "-v", stopOnErr, "-c", bounded, "-c", sql),
			want: []string{wantExec, "-n", nsE2E, pod, "-c", wantContainer, "--",
				wantPSQL, "-U", superuser, db, "-XqtA", "-v", stopOnErr, "-c", bounded, "-c", sql},
		},
		{
			name: "stderr-silenced probe",
			got: slices.Concat(kubectlExecArgs(pod, false), []string{"sh", "-c", silence, "sh"},
				psqlCommand(targetCluster, db, "-XAtq", "-c", sql)),
			want: []string{wantExec, "-n", nsE2E, pod, "-c", wantContainer, "--",
				"sh", "-c", silence, "sh", wantPSQL, "-U", superuser, db, "-XAtq", "-c", sql},
		},
	} {
		if !slices.Equal(tc.got, tc.want) {
			t.Errorf("%s argv = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

func TestBulkWriteLostSparesOnlyStatementErrors(t *testing.T) {
	tests := []struct {
		name    string
		results []psqlExecResult
		want    bool
	}{
		{name: "success", results: []psqlExecResult{{stdout: "UPDATE 1000"}}},
		{name: "hung exec", results: []psqlExecResult{{block: true}}, want: true},
		{
			name: "three TLS timeouts",
			results: []psqlExecResult{
				{stderr: safeExecErrorCases[0].stderr, exitCode: 1},
				{stderr: safeExecErrorCases[0].stderr, exitCode: 1},
				{stderr: safeExecErrorCases[0].stderr, exitCode: 1},
			},
			want: true,
		},
		{
			name:    "stream EOF",
			results: []psqlExecResult{{stderr: "error: error sending request: EOF\n", exitCode: 1}},
			want:    true,
		},
		{
			name:    "statement error",
			results: []psqlExecResult{{stderr: "ERROR:  relation \"documents\" does not exist\n", exitCode: 1}},
		},
		{
			name: "statement error after a notice",
			results: []psqlExecResult{{
				stderr: "NOTICE:  a notice\nERROR:  permission denied for table documents\n", exitCode: 1,
			}},
		},
		{
			name: "backend terminated",
			results: []psqlExecResult{{
				stderr: "FATAL:  terminating connection due to administrator command\n" +
					"server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally\n" +
					"command terminated with exit code 2\n",
				exitCode: 1,
			}},
		},
		{
			name:    "server panic",
			results: []psqlExecResult{{stderr: "PANIC:  could not write to file\n", exitCode: 1}},
		},
		{
			name: "server crashed",
			results: []psqlExecResult{{
				stderr: "server closed the connection unexpectedly\n" +
					"\tThis probably means the server terminated abnormally\n\tbefore or while processing the request.\n",
				exitCode: 1,
			}},
		},
		{
			name:    "connection lost",
			results: []psqlExecResult{{stderr: "connection to server was lost\n", exitCode: 1}},
		},
		{
			name: "connection refused by the server",
			results: []psqlExecResult{{
				stderr: "psql: error: connection to server on socket \"/run/postgresql/.s.PGSQL.5432\" failed: " +
					"FATAL:  database \"app\" does not exist\n",
				exitCode: 1,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, _ := newPSQLExecCommand(t, tt.results...)
			_, err := psqlDBErrWith(
				"source-cluster",
				psqlExecTestDatabase,
				"UPDATE documents SET body = body",
				func(string) string { return psqlExecTestPod },
				func(time.Duration) {},
				command,
				psqlExecTestTimeout,
			)
			if got := bulkWriteLost(err); got != tt.want {
				t.Fatalf("bulkWriteLost(%v) = %v, want %v", err, got, tt.want)
			}
		})
	}
}

func TestReadOnlySQL(t *testing.T) {
	reads := []string{
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgcopydb%'",
		"SELECT count(*) FROM pg_replication_origin WHERE (roname LIKE 'pgcopydb\\_e2e\\_%' OR roname LIKE 'x%')",
		"  select coalesce(sum(tuples_processed), 0) > 0 FROM pg_stat_progress_copy" +
			" WHERE relid = to_regclass('public.documents');",
		"SELECT count(*) FILTER (WHERE note LIKE 'a;b(') FROM orders",
		"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')",
		"SHOW wal_level",
	}
	writes := []string{
		"UPDATE documents SET body = 'x'",
		"INSERT INTO orders SELECT 1",
		"WITH d AS (DELETE FROM orders RETURNING 1) SELECT count(*) FROM d",
		"WITH s AS (SELECT 1) SELECT * FROM s",
		"SELECT 1; DELETE FROM orders",
		"SHOW wal_level; DROP TABLE orders",
		"SELECT id FROM orders FOR UPDATE",
		"SELECT * INTO orders_copy FROM orders",
		"SELECT nextval('orders_id_seq')",
		"SELECT pg_catalog.setval('orders_id_seq', 1)",
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity",
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots",
		"SELECT pg_logical_emit_message(true, 'p', 'x')",
		"SELECT count(*) FROM pg_logical_slot_get_changes('s', NULL, NULL)",
		`SELECT "nextval"('orders_id_seq')`,
		"SELECT E'\\'' || nextval('s')",
		"SELECT e'x'",
		"SELECT E'x'",
		"SELECT $$x$$",
		"SELECT 1 -- comment",
		"SELECT 'unterminated",
		"SELECT nextval\v('orders_id_seq')",
		"SELECT nextval\f('orders_id_seq')",
		"SELECT ne\u0301xtval('orders_id_seq')",
		"SELECT count(*) FROM pg_replication_slots\u00a0",
		"SELECT \u212aEY FROM t",
		"",
	}
	for _, sql := range reads {
		if !readOnlySQL(sql) {
			t.Errorf("readOnlySQL(%q) = false, want true", sql)
		}
	}
	for _, sql := range writes {
		if readOnlySQL(sql) {
			t.Errorf("readOnlySQL(%q) = true, want false", sql)
		}
	}
}

func TestPSQLDBErrRetriesOnlyReadsAfterATimeout(t *testing.T) {
	for _, tt := range []struct {
		sql       string
		wantCalls int
	}{
		{sql: "SELECT count(*) FROM pg_replication_slots", wantCalls: 2},
		{sql: "UPDATE documents SET body = body", wantCalls: 1},
	} {
		t.Run(tt.sql, func(t *testing.T) {
			command, state := newPSQLExecCommand(t, psqlExecResult{block: true}, psqlExecResult{stdout: "0\n"})
			var waits []time.Duration
			out, err := psqlDBErrWith(
				"source-cluster",
				psqlExecTestDatabase,
				tt.sql,
				func(string) string { return psqlExecTestPod },
				func(delay time.Duration) { waits = append(waits, delay) },
				command,
				psqlExecTestTimeout,
			)
			calls, _, commands := state.snapshot()
			if len(calls) != tt.wantCalls {
				t.Fatalf("calls=%d, want %d", len(calls), tt.wantCalls)
			}
			requirePSQLCommandsReaped(t, commands)
			if tt.wantCalls == 1 {
				if !errors.Is(err, context.DeadlineExceeded) || len(waits) != 0 {
					t.Fatalf("err=%v waits=%v, want a deadline and no wait", err, waits)
				}
				return
			}
			if err != nil || out != "0" || fmt.Sprint(waits) != "[5s]" {
				t.Fatalf("out=%q err=%v waits=%v, want 0 after one 5s wait", out, err, waits)
			}
		})
	}
}
