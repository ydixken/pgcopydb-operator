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
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

// The lines of a probe that found nothing to hold on.
const (
	gateCensusClean  = "gate: census total=0 syncrep=0 named=yes syncrep_since=0 gate_waiting=0"
	gateCommitOKLine = "gate: commit ok"
	gatePassLine     = "gate: pass"
	gateLocalLine    = "gate: synchronous_commit=local"
)

// The lines of a probe that holds, and the psql calls the stub records.
const (
	gateShowOnLine      = "gate: synchronous_commit=on"
	gateBlockedLine     = "gate: commit blocked"
	gateSkippedSyncRep  = "gate: commit skipped syncrep=1"
	gateSkippedWaiting  = "gate: commit skipped gate_waiting=1"
	gateSkippedNoCensus = "gate: commit skipped no census"
	callsCensus         = "census\n"
	callsCensusCommit   = "census\ncommit\n"
	callsCensusShow     = "census\nshow\n"
)

// requireGNUTimeout fails a script test without GNU timeout on PATH: the probe reads its
// exit codes 124 and 137, and its --kill-after.
func requireGNUTimeout(t *testing.T) {
	t.Helper()
	out, err := exec.Command("timeout", "--version").Output()
	if err != nil || !strings.Contains(string(out), "GNU coreutils") {
		t.Fatal("the retry gate script tests require GNU timeout on PATH")
	}
}

// runGate runs a gate script under the Job's shell, with env on top of the test's own.
func runGate(t *testing.T, script string, env ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(shellPath, "-c", script)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		return string(out), exitErr.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return string(out), 0
}

// gateStub is a psql that tells the probe's three calls apart by their arguments, records
// each in $STATE/calls and the census SQL it reads on stdin, then evals that call's body.
const gateStub = `#!/bin/sh
case "$*" in
*'CREATE TEMP TABLE'*) echo commit >>"$STATE/calls"; eval "$STUB_COMMIT" ;;
*'show synchronous_commit'*) echo show >>"$STATE/calls"; eval "$STUB_SHOW" ;;
*) echo census >>"$STATE/calls"; cat >"$STATE/census.sql"; eval "$STUB_CENSUS" ;;
esac
`

func TestRetryGateScript(t *testing.T) {
	requireGNUTimeout(t)
	const (
		blockedWarning = "WARNING:  canceling wait for synchronous replication due to user request"
		connectErr     = `psql: error: connection to server at "h" (10.0.0.1), port 5432 failed: FATAL:  password authentication failed for user "app"`
		uriErr         = `psql: error: invalid URI query parameter: "postgresql://u:secret@h/db?x"`
		tempErr        = `ERROR:  42501: permission denied to create temporary tables in database "app"`
		statErr        = "ERROR:  permission denied for view pg_stat_activity"
		session1       = "gate: session pid=11 state=idle wait=ClientRead age=40s started=2026-10-20T10:00:00Z app=pgcopydb[7] copy worker"
		session2       = "gate: session pid=12 state=active wait=- age=3s started=2026-10-20T10:00:05Z app=pgcopydb[7] clone"
	)
	echo := func(lines ...string) string {
		var b strings.Builder
		for _, line := range lines {
			b.WriteString("echo '" + line + "'\n")
		}
		return b.String()
	}
	stderr := func(line string, code int) string { return fmt.Sprintf("echo '%s' >&2; exit %d", line, code) }
	script := gateScript(false)
	// One stub serves every case: on macOS the first exec of each new file took long enough
	// to break the timing bounds.
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "psql"), []byte(gateStub), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                 string
		census, show, commit string // stub bodies; empty is a clean census, local, and a commit that succeeds
		flags                [2]string
		code                 int
		want, not            []string
		calls                string
		within               time.Duration
	}{
		{
			name: "pass with commit", code: 0, calls: callsCensusCommit,
			want: []string{gateCensusClean, gateCommitOKLine, gatePassLine},
		},
		{
			name:   "sessions remain",
			census: echo("gate: census total=2 syncrep=0 named=yes syncrep_since=0 gate_waiting=0", session1, session2),
			code:   1, calls: callsCensusCommit,
			want: []string{session1, session2, gateCommitOKLine},
		},
		{
			name:   "syncrep proof",
			census: echo("gate: census total=1 syncrep=1 named=yes syncrep_since=1760000000 gate_waiting=0"),
			code:   1, calls: callsCensus,
			want: []string{gateSkippedSyncRep}, not: []string{gateCommitOKLine},
		},
		{
			name:   "earlier probe waits",
			census: echo("gate: census total=0 syncrep=0 named=yes syncrep_since=0 gate_waiting=1"),
			code:   1, calls: callsCensus,
			want: []string{gateSkippedWaiting},
		},
		{
			name:   "named=no",
			census: echo("gate: census total=0 syncrep=0 named=no syncrep_since=0 gate_waiting=0"),
			code:   0, calls: callsCensusCommit,
			want: []string{"gate: census total=0 syncrep=0 named=no syncrep_since=0 gate_waiting=0", gatePassLine},
		},
		{
			name: "show local", flags: [2]string{"0", "1"}, code: 0, calls: callsCensusShow,
			want: []string{gateLocalLine, gatePassLine},
		},
		{
			name: "show on", flags: [2]string{"0", "1"}, show: "echo on", code: 1, calls: callsCensusShow,
			want: []string{gateShowOnLine},
		},
		{
			name:   "commit blocked",
			commit: `trap 'echo "` + blockedWarning + `" >&2; exit 0' INT; sleep 30`,
			code:   1, calls: callsCensusCommit, within: 7 * time.Second,
			want: []string{gateBlockedLine},
		},
		{
			name:   "cancelled without the replication warning",
			commit: "trap 'exit 0' INT; sleep 30",
			code:   1, calls: callsCensusCommit, within: 7 * time.Second,
			want: []string{"gate: commit failed (exit 124)"}, not: []string{gateBlockedLine},
		},
		{
			name:   "cancel ignored",
			commit: "trap '' INT; sleep 30",
			code:   1, calls: callsCensusCommit, within: 12 * time.Second,
			want: []string{"gate: commit failed"}, not: []string{gateBlockedLine},
		},
		{
			name: "connection error", census: stderr(connectErr, 2), code: 1, calls: callsCensus,
			want: []string{"gate: census failed psql: error: connection to server at", gateSkippedNoCensus},
		},
		{
			name: "withheld", flags: [2]string{"1", "1"}, census: stderr(uriErr, 1), show: stderr(uriErr, 1),
			code: 1, calls: callsCensusShow,
			want: []string{"gate: census failed client-side error withheld", "gate: synchronous_commit failed client-side error withheld"},
			not:  []string{"secret"},
		},
		{
			name: "census SQL error behind the script prefix", census: stderr("psql:<stdin>:1: "+statErr, 3), code: 1, calls: callsCensus,
			want: []string{"gate: census failed " + statErr + " (exit 3)"}, not: []string{"withheld"},
		},
		{
			name: "prefixed client-side error", census: stderr("psql:<stdin>:2: unterminated quoted string", 3), code: 1, calls: callsCensus,
			want: []string{"gate: census failed client-side error withheld (exit 3)"},
		},
		{
			name: "42501", commit: stderr(tempErr, 1), code: 1, calls: callsCensusCommit,
			want: []string{"gate: commit failed " + tempErr},
		},
		{
			name: "empty output", flags: [2]string{"1", "1"}, census: ":", show: ":", commit: ":",
			code: 1, calls: callsCensusShow,
			want: []string{"gate: synchronous_commit=\n", gateSkippedNoCensus},
		},
		{
			name: "malformed flag", flags: [2]string{"yes", "0"}, code: 1, calls: "",
			want: []string{"gate: invalid flags"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			census, show, commit := tc.census, tc.show, tc.commit
			if census == "" {
				census = echo(gateCensusClean)
			}
			if show == "" {
				show = "echo local"
			}
			if commit == "" {
				commit = "exit 0"
			}
			flags := tc.flags
			if flags == [2]string{} {
				flags = [2]string{"1", "0"}
			}
			within := tc.within
			if within == 0 {
				within = 3 * time.Second
			}
			state := t.TempDir()
			start := time.Now()
			out, code := runGate(t, script,
				"PATH="+bin+":"+os.Getenv("PATH"), "STATE="+state, "PGCOPYDB_TARGET_PGURI=tgt",
				"STUB_CENSUS="+census, "STUB_SHOW="+show, "STUB_COMMIT="+commit,
				"GATE_COMMIT_TEST="+flags[0], "GATE_EXPECT_LOCAL="+flags[1])
			if elapsed := time.Since(start); elapsed >= within {
				t.Errorf("the probe took %s, want under %s", elapsed.Round(time.Millisecond), within)
			}
			if code != tc.code {
				t.Errorf("exit code %d, want %d", code, tc.code)
			}
			if passed := strings.Contains(out, gatePassLine+"\n"); passed != (tc.code == 0) {
				t.Errorf("%q printed = %v, want it exactly when the probe exits 0", gatePassLine, passed)
			}
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Errorf("output lacks %q", want)
				}
			}
			for _, not := range tc.not {
				if strings.Contains(out, not) {
					t.Errorf("output carries %q", not)
				}
			}
			calls, _ := os.ReadFile(filepath.Join(state, "calls"))
			if string(calls) != tc.calls {
				t.Errorf("psql calls = %q, want %q", calls, tc.calls)
			}
			if strings.HasPrefix(tc.calls, "census") {
				if sql, _ := os.ReadFile(filepath.Join(state, "census.sql")); !strings.Contains(string(sql), censusSQL(false)) {
					t.Errorf("the census call read %q on stdin, want the census SQL", sql)
				}
			}
			if t.Failed() {
				t.Logf("output:\n%s", out)
			}
		})
	}

	// The probe's shell is PID 1, which ignores an untrapped TERM; here an untrapped shell
	// dies by the signal at once, so only the trap gives a normal exit 143.
	t.Run("TERM ends the probe once the running check returns", func(t *testing.T) {
		t.Parallel()
		state := t.TempDir()
		var out bytes.Buffer
		cmd := exec.Command(shellPath, "-c", script)
		cmd.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"), "STATE="+state, "PGCOPYDB_TARGET_PGURI=tgt",
			"STUB_CENSUS=sleep 2; "+echo(gateCensusClean), "STUB_SHOW=echo local", "STUB_COMMIT=exit 0",
			"GATE_COMMIT_TEST=1", "GATE_EXPECT_LOCAL=0")
		cmd.Stdout, cmd.Stderr = &out, &out
		start := time.Now()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			for _, f := range []string{"err", "out"} {
				_ = os.Remove(fmt.Sprintf("/tmp/pgm-gate-%d.%s", cmd.Process.Pid, f))
			}
		})
		waitUntil(t, "the census never ran", func() bool {
			calls, _ := os.ReadFile(filepath.Join(state, "calls"))
			return string(calls) == callsCensus
		})
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		_ = cmd.Wait()
		elapsed := time.Since(start)
		if ps := cmd.ProcessState; !ps.Exited() || ps.ExitCode() != 143 {
			t.Errorf("the probe ended with %s, want a normal exit 143", ps)
		}
		if elapsed >= 4*time.Second {
			t.Errorf("the probe took %s, want under 4s", elapsed.Round(time.Millisecond))
		}
		if calls, _ := os.ReadFile(filepath.Join(state, "calls")); string(calls) != callsCensus {
			t.Errorf("psql calls = %q, want %q", calls, callsCensus)
		}
		if strings.Contains(out.String(), "gate: commit") {
			t.Errorf("a check ran after TERM:\n%s", out.String())
		}
	})
}

func TestCensusSQL(t *testing.T) {
	like := "like '" + progress.PgcopydbAppNameLike + "'"
	single, all := censusSQL(false), censusSQL(true)
	for name, sql := range map[string]string{"single database": single, "all databases": all} {
		if n := strings.Count(sql, like); n != 2 {
			t.Errorf("%s: %q appears %d times, want once per statement", name, like, n)
		}
		for _, want := range []string{"limit 10", "gate: census total=%s", "gate: session pid=%s", "'pgm-retry-gate'"} {
			if !strings.Contains(sql, want) {
				t.Errorf("%s: census SQL lacks %q:\n%s", name, want, sql)
			}
		}
	}
	if n := strings.Count(single, "datname = current_database()"); n != 3 {
		t.Errorf("single-database census filters datname %d times, want 3 (census, subquery, sessions):\n%s", n, single)
	}
	if strings.Contains(all, "datname") {
		t.Errorf("all-databases census filters on datname:\n%s", all)
	}
}

func TestBuildGateJob(t *testing.T) {
	local := superMigration()
	local.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
	for _, tc := range []struct {
		name       string
		m          *v1beta1.Migration
		annotation string
	}{
		{"field unset", superMigration(), "commit=1,local=0"},
		{"field set, copy unfinished", local, "commit=0,local=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job, err := buildGateJob(tc.m, "runner:gate", 2)
			if err != nil {
				t.Fatal(err)
			}
			if job.Name != "m-gate-2" || job.Namespace != "ns" {
				t.Errorf("Job %s/%s, want ns/m-gate-2", job.Namespace, job.Name)
			}
			if !maps.Equal(job.Labels, labels(tc.m)) || !maps.Equal(job.Spec.Template.Labels, labels(tc.m)) {
				t.Errorf("labels %v, pod labels %v, want %v", job.Labels, job.Spec.Template.Labels, labels(tc.m))
			}
			spec := job.Spec
			if spec.BackoffLimit == nil || *spec.BackoffLimit != 0 ||
				spec.ActiveDeadlineSeconds == nil || *spec.ActiveDeadlineSeconds != 300 ||
				spec.TTLSecondsAfterFinished == nil || *spec.TTLSecondsAfterFinished != 600 {
				t.Errorf("backoffLimit %v, activeDeadlineSeconds %v, ttlSecondsAfterFinished %v, want 0, 300, 600",
					spec.BackoffLimit, spec.ActiveDeadlineSeconds, spec.TTLSecondsAfterFinished)
			}
			if got := job.Annotations["pgcopydb-operator.io/retry-gate-inputs"]; got != tc.annotation || got != gateInputsFor(tc.m).String() {
				t.Errorf("inputs annotation %q, want %q", got, tc.annotation)
			}

			workdir := false
			for _, v := range spec.Template.Spec.Volumes {
				if v.PersistentVolumeClaim != nil {
					t.Errorf("the probe mounts PVC %q in volume %s", v.PersistentVolumeClaim.ClaimName, v.Name)
				}
				if v.Name == workdirVolume {
					workdir = v.EmptyDir != nil
				}
			}
			if !workdir {
				t.Error("the workdir volume is not an emptyDir")
			}

			checkGateContainer(t, tc.m, spec.Template.Spec.Containers[0])
		})
	}
	t.Run("all databases", func(t *testing.T) {
		m := passwordMigration()
		m.Spec.Clone.AllDatabases = true
		job, err := buildGateJob(m, "img", 3)
		if err != nil {
			t.Fatal(err)
		}
		script := job.Spec.Template.Spec.Containers[0].Args[1]
		if job.Name != "m-gate-3" || !strings.Contains(script, "gate: census total=%s") {
			t.Fatalf("Job %s carries no census to inspect:\n%s", job.Name, script)
		}
		if strings.Contains(script, "datname") {
			t.Errorf("the all-databases census filters on datname:\n%s", script)
		}
	})
}

// checkGateContainer pins what the probe container carries: the runner image, the env the
// script reads, the target URI option, no superuser credentials, and the script itself.
func checkGateContainer(t *testing.T, m *v1beta1.Migration, c corev1.Container) {
	t.Helper()
	if c.Image != "runner:gate" {
		t.Errorf("image %q, want runner:gate", c.Image)
	}
	targetURI := envValue(c.Env, "PGCOPYDB_TARGET_PGURI")
	if targetURI == "" {
		t.Fatal("no target URI in the env to inspect")
	}
	for _, e := range c.Env {
		if strings.Contains(e.Name, "SUPER") {
			t.Errorf("the probe env carries superuser credentials: %s", e.Name)
		}
	}
	if strings.Contains(c.Command[2], "SUPER") {
		t.Errorf("the probe prelude carries superuser credentials:\n%s", c.Command[2])
	}
	in := gateInputsFor(m)
	for name, want := range map[string]string{
		"PGAPPNAME":         "pgm-retry-gate",
		"PGCONNECT_TIMEOUT": "10",
		"GATE_COMMIT_TEST":  gateFlag(in.CommitTest),
		"GATE_EXPECT_LOCAL": gateFlag(in.ExpectLocal),
	} {
		if got := envValue(c.Env, name); got != want {
			t.Errorf("env %s=%q, want %q", name, got, want)
		}
	}
	if got, want := strings.Contains(targetURI, conn.SyncCommitLocalParam), in.ExpectLocal; got != want {
		t.Errorf("target URI carries the synchronous_commit option = %v, want %v: %s", got, want, targetURI)
	}

	script := c.Args[1]
	if script != gateScript(false) || !strings.Contains(script, "datname = current_database()") {
		t.Errorf("the probe runs another script than the single-database gate script:\n%s", script)
	}
	for _, ends := range []string{"pg_terminate_backend", "pg_cancel_backend"} {
		if strings.Contains(script, ends) {
			t.Errorf("the probe script calls %s: the gate ends no session", ends)
		}
	}
}

func TestGateInputsFor(t *testing.T) {
	set := &v1beta1.Migration{}
	set.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
	done := set.DeepCopy()
	done.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionCloneCompleted, Status: metav1.ConditionTrue, Reason: "CloneDone"}}
	for _, tc := range []struct {
		name       string
		m          *v1beta1.Migration
		want       gateInputs
		annotation string
	}{
		{"unset", &v1beta1.Migration{}, gateInputs{CommitTest: true}, "commit=1,local=0"},
		{"set, copy unfinished", set, gateInputs{ExpectLocal: true}, "commit=0,local=1"},
		{"set, copy done", done, gateInputs{CommitTest: true, ExpectLocal: true}, "commit=1,local=1"},
	} {
		got := gateInputsFor(tc.m)
		if got != tc.want || got.String() != tc.annotation {
			t.Errorf("%s: gateInputsFor = %+v (%s), want %+v (%s)", tc.name, got, got, tc.want, tc.annotation)
		}
	}
}

func TestGateResultHold(t *testing.T) {
	const (
		idle    = "gate: session pid=101 state=idle wait=ClientRead age=40s started=2026-10-20T10:00:00Z app=pgcopydb[7] copy worker"
		waiting = "gate: session pid=102 state=active wait=SyncRep age=35s started=2026-10-20T10:00:01Z app=pgcopydb[7] clone"
		tempErr = `ERROR:  42501: permission denied to create temporary tables in database "d" (exit 1)`
	)
	census := func(total, syncrep int, named string, gateWaiting int) string {
		return fmt.Sprintf("gate: census total=%d syncrep=%d named=%s syncrep_since=0 gate_waiting=%d", total, syncrep, named, gateWaiting)
	}
	twelve := []string{census(12, 0, "yes", 0)}
	for pid := 201; pid <= 210; pid++ {
		twelve = append(twelve, fmt.Sprintf("gate: session pid=%d state=active wait=- age=5s started=2026-10-20T10:00:00Z app=pgcopydb[%d] copy worker", pid, pid))
	}
	commitOnly, localOnly, both := gateInputs{CommitTest: true}, gateInputs{ExpectLocal: true}, gateInputs{CommitTest: true, ExpectLocal: true}
	inline, bundled := passwordMigration().Spec.Target, secretRefTarget()
	for _, tc := range []struct {
		name      string
		lines     []string
		in        gateInputs
		target    v1beta1.PostgresConnection
		passed    bool
		reason    string
		want, not []string
	}{
		{name: "pass", lines: []string{gateCensusClean, gateCommitOKLine, gatePassLine}, in: commitOnly, passed: true},
		{name: "pass with every check", lines: []string{gateCensusClean, gateLocalLine, gateCommitOKLine, gatePassLine}, in: both, passed: true},
		{
			name:  "no census line",
			lines: []string{`gate: census failed FATAL:  password authentication failed for user "app" (exit 2)`, gateSkippedNoCensus},
			in:    commitOnly, reason: reasonProbeFailed, want: []string{`password authentication failed for user "app"`},
		},
		{
			name:  "sessions remain",
			lines: []string{census(2, 1, "yes", 0), idle, waiting, gateSkippedSyncRep},
			in:    commitOnly, reason: reasonSessionsRemain,
			want: []string{"2 in total", "pid=101", "pid=102", "app=pgcopydb[7] clone", "wait=SyncRep"},
			not:  []string{"connection pooler", "application_name"},
		},
		{
			name:  "twelve sessions",
			lines: append(slices.Clone(twelve), gateCommitOKLine), in: commitOnly, reason: reasonSessionsRemain,
			want: []string{"12 in total", "10 listed", "pid=201", "pid=205", "pid=210"},
		},
		{
			name:  "all idle",
			lines: []string{census(1, 0, "yes", 0), idle, gateCommitOKLine}, in: commitOnly, reason: reasonSessionsRemain,
			want: []string{"connection pooler", "holds no transaction"},
		},
		{
			name:  "named=no",
			lines: []string{census(1, 0, "no", 0), idle, gateCommitOKLine}, in: commitOnly, reason: reasonSessionsRemain,
			want: []string{"application_name"},
		},
		{
			name:  "pass line despite sessions",
			lines: []string{census(1, 0, "yes", 0), waiting, gateCommitOKLine, gatePassLine}, in: commitOnly, reason: reasonSessionsRemain,
		},
		{
			name:  "show not local",
			lines: []string{gateCensusClean, gateShowOnLine}, in: localOnly, reason: reasonSyncCommitNotLocal,
			want: []string{"synchronous_commit=on"},
		},
		{
			name:  "show failed",
			lines: []string{gateCensusClean, "gate: synchronous_commit failed client-side error withheld (exit 1)"}, in: localOnly, reason: reasonProbeFailed,
			want: []string{"client-side error withheld"},
		},
		{
			name: "show missing", lines: []string{gateCensusClean, gatePassLine}, in: localOnly, reason: reasonProbeFailed,
			want: []string{"synchronous_commit"},
		},
		{
			name: "commit blocked", lines: []string{gateCensusClean, gateBlockedLine}, in: commitOnly, reason: reasonCommitBlocked,
			want: []string{"synchronous replication"},
		},
		{
			name:  "commit skipped",
			lines: []string{census(0, 0, "yes", 1), gateSkippedWaiting}, in: commitOnly, reason: reasonCommitBlocked,
			want: []string{"earlier", "gate_waiting=1"},
		},
		{
			name: "commit failed", lines: []string{gateCensusClean, "gate: commit failed (exit 137)"}, in: commitOnly, reason: reasonProbeFailed,
			want: []string{"exit 137"}, not: []string{"GRANT"},
		},
		{
			name: "42501", lines: []string{gateCensusClean, "gate: commit failed " + tempErr}, in: commitOnly, target: inline, reason: reasonProbeFailed,
			want: []string{tempErr, `GRANT TEMPORARY ON DATABASE "d" TO "u"`},
		},
		{
			name: "42501 behind a secretRef", lines: []string{gateCensusClean, "gate: commit failed " + tempErr}, in: commitOnly, target: bundled,
			reason: reasonProbeFailed, want: []string{"GRANT TEMPORARY ON DATABASE <database> TO <role>"},
		},
		{
			name: "pass line without commit ok", lines: []string{gateCensusClean, gatePassLine}, in: commitOnly, reason: reasonProbeFailed,
			want: []string{"commit"},
		},
		{
			name: "no pass line", lines: []string{gateCensusClean, gateCommitOKLine}, in: commitOnly, reason: reasonProbeFailed,
			want: []string{"gate: pass"},
		},
		{
			name:  "every failing check",
			lines: []string{census(1, 1, "yes", 0), waiting, gateShowOnLine, gateSkippedSyncRep},
			in:    both, reason: reasonSessionsRemain,
			want: []string{"pid=102", "synchronous_commit=on", "test commit"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := parseGateLog(strings.Join(tc.lines, "\n") + "\n")
			if got := g.passed(tc.in); got != tc.passed {
				t.Errorf("passed = %v, want %v", got, tc.passed)
			}
			reason, msg := g.hold(tc.in, &tc.target)
			if reason != tc.reason {
				t.Errorf("reason %q, want %q (message %q)", reason, tc.reason, msg)
			}
			if tc.passed != (msg == "") {
				t.Errorf("message %q, want one exactly when the probe did not pass", msg)
			}
			for _, want := range tc.want {
				if !strings.Contains(msg, want) {
					t.Errorf("message lacks %q: %s", want, msg)
				}
			}
			for _, not := range tc.not {
				if strings.Contains(msg, not) {
					t.Errorf("message carries %q: %s", not, msg)
				}
			}
		})
	}
	t.Run("twelve sessions lists ten", func(t *testing.T) {
		g := parseGateLog(strings.Join(twelve, "\n"))
		if _, msg := g.hold(commitOnly, &inline); strings.Count(msg, "pid=") != 10 {
			t.Errorf("message lists %d sessions, want 10: %s", strings.Count(msg, "pid="), msg)
		}
	})
	t.Run("census fields", func(t *testing.T) {
		g := parseGateLog("prelude noise\ngate: census total=3 syncrep=2 named=yes syncrep_since=1760000000 gate_waiting=1\r\n" + idle + "\n")
		want := gateCensus{Total: 3, SyncRep: 2, GateWaiting: 1, SyncRepSince: 1760000000, Named: true}
		if g.Census == nil || *g.Census != want {
			t.Fatalf("census %+v, want %+v", g.Census, want)
		}
		if len(g.Sessions) != 1 || !g.Sessions[0].Idle || !strings.HasPrefix(g.Sessions[0].Line, "pid=101 ") {
			t.Errorf("sessions %+v, want the idle pid 101", g.Sessions)
		}
		inTx := parseGateLog("gate: session pid=9 state=idle in transaction wait=ClientRead age=1s started=2026-10-20T10:00:00Z app=pgcopydb[1] clone\n")
		if len(inTx.Sessions) != 1 || inTx.Sessions[0].Idle {
			t.Errorf("sessions %+v, want one that is not idle: it holds a transaction", inTx.Sessions)
		}
		if g := parseGateLog("gate: census total=x syncrep=0 named=yes syncrep_since=0 gate_waiting=0\n"); g.Census != nil {
			t.Errorf("a malformed census line parsed as %+v", g.Census)
		}
	})
}
