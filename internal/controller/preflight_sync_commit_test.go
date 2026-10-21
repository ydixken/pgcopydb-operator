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
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
)

const (
	showSyncCommit   = "show synchronous_commit"
	syncCommitOK     = "ok: target synchronous_commit local"
	temporaryProbe   = "select has_database_privilege(current_database(), 'TEMP')::int"
	connectedTarget  = `echo "ok: connectivity target"`
	targetConnectErr = "preflight: cannot connect to the target database"
)

// syncCommitNote is the spec's preflight note for a target that reports value.
func syncCommitNote(value string) string {
	return "preflight: spec.target.synchronousCommit is local but the target connection reports synchronous_commit=" +
		value + "; a connection pooler or proxy dropped the libpq startup option; connect the target directly or unset synchronousCommit"
}

func syncCommitMigration(allDatabases bool) *v1beta1.Migration {
	m := passwordMigration()
	m.Spec.Clone.AllDatabases = allDatabases
	m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
	return m
}

// TestPreflightSyncCommitBlock drives the SHOW block under the follow stub:
// only local passes, and an unreadable setting fails closed.
func TestPreflightSyncCommitBlock(t *testing.T) {
	run := followPreflightHarness(t)
	script := preflightScriptFor(syncCommitMigration(false))
	for _, tc := range []struct {
		name, answer, want string
		code               int
	}{
		{"local passes", "local", syncCommitOK, 0},
		{"another value fails by name", "on", syncCommitNote("on"), 1},
		{"an unreadable setting fails closed", "fail", syncCommitNote("(unreadable)"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, code, _ := run(t, script, "", "PSQL_SYNC_COMMIT="+tc.answer)
			if code != tc.code || !strings.Contains(out, tc.want) {
				t.Fatalf("code=%d, want %d and %q:\n%s", code, tc.code, tc.want, out)
			}
			if tc.code != 0 && strings.Contains(out, syncCommitOK) {
				t.Fatalf("a failed check printed its ok line:\n%s", out)
			}
		})
	}
	t.Run("only with the field, after target connectivity, on both paths", func(t *testing.T) {
		for _, all := range []bool{false, true} {
			plain := passwordMigration()
			plain.Spec.Clone.AllDatabases = all
			off := preflightScriptFor(plain)
			if !strings.Contains(off, connectedTarget) || strings.Contains(off, showSyncCommit) {
				t.Fatalf("allDatabases=%v: without the field the script must connect and never SHOW:\n%s", all, off)
			}
			mustPrecede(t, preflightScriptFor(syncCommitMigration(all)), connectedTarget, showSyncCommit)
		}
	})
	t.Run("prelude order", func(t *testing.T) {
		m := syncCommitMigration(false)
		m.Spec.Preflight = &v1beta1.PreflightOptions{RequireSameMajorVersion: true}
		s := preflightScriptFor(m)
		mustPrecede(t, s, connectedTarget, showSyncCommit)
		mustPrecede(t, s, showSyncCommit, temporaryProbe)
		mustPrecede(t, s, temporaryProbe, "server_version_num")
	})
}

// TestPreflightTemporaryWarning pins the warn-only TEMPORARY probe that every
// preflight runs: the retry gate's test commit creates a temporary table.
func TestPreflightTemporaryWarning(t *testing.T) {
	run := followPreflightHarness(t)
	script := preflightScriptFor(passwordMigration())
	for _, tc := range []struct{ name, answer, want string }{
		{"a missing grant warns with the remedy", "0", "GRANT TEMPORARY ON DATABASE"},
		{"an unreadable probe warns", "fail", "could not read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, code, _ := run(t, script, "", "PSQL_TEMP="+tc.answer)
			var lines []string
			for line := range strings.SplitSeq(out, "\n") {
				if strings.Contains(line, "TEMPORARY") {
					lines = append(lines, line)
				}
			}
			if code != 0 || !strings.Contains(out, preflightAllChecksPassed) {
				t.Fatalf("a TEMPORARY finding must never fail preflight: code=%d out:\n%s", code, out)
			}
			if len(lines) != 1 || !strings.HasPrefix(lines[0], warnPrefix) || !strings.Contains(lines[0], tc.want) {
				t.Fatalf("want one %q line containing %q, got %q:\n%s", warnPrefix, tc.want, lines, out)
			}
		})
	}
	t.Run("a held grant prints nothing", func(t *testing.T) {
		out, code, _ := run(t, script, "", "PSQL_TEMP=1")
		if code != 0 || !strings.Contains(out, "ok: connectivity target") || !strings.Contains(out, preflightAllChecksPassed) {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if strings.Contains(out, "TEMPORARY") {
			t.Fatalf("a held grant must print nothing:\n%s", out)
		}
	})
	t.Run("single and all-databases scripts probe it", func(t *testing.T) {
		for _, all := range []bool{false, true} {
			m := passwordMigration()
			m.Spec.Clone.AllDatabases = all
			if s := preflightScriptFor(m); !strings.Contains(s, temporaryProbe) {
				t.Fatalf("allDatabases=%v: TEMPORARY probe missing:\n%s", all, s)
			}
		}
	})
}

// TestPreflightTargetConnectHint: with the field set, a target that refuses
// every connect names the pooler setting that refuses the startup option.
func TestPreflightTargetConnectHint(t *testing.T) {
	run := followPreflightHarness(t)
	for _, tc := range []struct {
		name string
		m    *v1beta1.Migration
		hint bool
	}{
		{"field set", syncCommitMigration(false), true},
		{"field unset", passwordMigration(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, code, _ := run(t, preflightScriptFor(tc.m), "", "CONN_URI=tgt", "CONN_FAIL_N=99")
			lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
			last := lines[len(lines)-1]
			if code != 1 || strings.Count(out, "retry: target connectivity attempt") != 5 || !strings.Contains(last, targetConnectErr) {
				t.Fatalf("want the verdict after six target probes: code=%d out:\n%s", code, out)
			}
			if got := strings.Contains(last, "ignore_startup_parameters"); got != tc.hint {
				t.Fatalf("hint present = %v, want %v: %q", got, tc.hint, last)
			}
			if !tc.hint && last != targetConnectErr {
				t.Fatalf("without the field the verdict must stay %q, got %q", targetConnectErr, last)
			}
		})
	}
}

// TestPreflightSyncCommit_Server runs the prelude against a real server: the startup
// option reaches the session, a plain URI reports the server default by name, the
// pooler check reads the direct connection as direct, and the TEMPORARY probe stays silent.
func TestPreflightSyncCommit_Server(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("CI supplies PGCOPYDB_TEST_PGURI for preflight SQL regressions")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	script := preflightPrelude(syncCommitMigration(false)) + preflightScriptFooter
	for _, tc := range []struct {
		name, target, want string
		code               int
	}{
		{"the option reaches the session", uri + sep + conn.SyncCommitLocalParam, syncCommitOK, 0},
		{"a plain URI reports the server default", uri, syncCommitNote("on"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(shellPath, "-c", script)
			cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+uri, "PGCOPYDB_TARGET_PGURI="+tc.target)
			out, err := cmd.CombinedOutput()
			code := 0
			if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
				code = exitErr.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != tc.code || !strings.Contains(string(out), tc.want) {
				t.Fatalf("code=%d, want %d and %q:\n%s", code, tc.code, tc.want, out)
			}
			if !strings.Contains(string(out), "ok: connectivity target") || strings.Contains(string(out), "TEMPORARY") {
				t.Fatalf("the superuser holds TEMPORARY, so its probe must connect and print nothing:\n%s", out)
			}
			if !strings.Contains(string(out), poolerOK(conn.Source)) || !strings.Contains(string(out), poolerOK(conn.Target)) {
				t.Fatalf("a direct connection must pass the pooler check on both sides (needs psql 18):\n%s", out)
			}
		})
	}
}
