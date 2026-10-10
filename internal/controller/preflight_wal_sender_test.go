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
	"os"
	"os/exec"
	"strings"
	"testing"

	"k8s.io/client-go/tools/events"
)

const (
	walSenderTimeoutProbe = "wal_sender_timeout"
	wstWarnPrefix         = "warn: source wal_sender_timeout is "
	wstURIRemedy          = "options=-c wal_sender_timeout=60s"
	wstOverrideURI        = "src?options=-c%20wal_sender_timeout%3D60s"
	wstOK1min             = "ok: source wal_sender_timeout 1min"
)

// TestPreflightScript_WalSenderTimeout runs the follow preflight under the
// stub: a timeout below 60s warns with both remedies and never fails, and the
// probe reads over the source URI, so an options=-c override in it counts.
func TestPreflightScript_WalSenderTimeout(t *testing.T) {
	run := followPreflightHarness(t)
	cases := []struct {
		name, ok, warn string
		env            []string
	}{
		{name: "disabled", ok: "ok: source wal_sender_timeout disabled", env: []string{"PSQL_WST=0|0"}},
		{name: "5s warns", warn: wstWarnPrefix + "5s, below 60s", env: []string{"PSQL_WST=5000|5s"}},
		{name: "59s warns", warn: wstWarnPrefix + "59s, below 60s", env: []string{"PSQL_WST=59000|59s"}},
		{name: "60s passes", ok: wstOK1min, env: []string{"PSQL_WST=60000|1min"}},
		{name: "URI override wins over a 5s server", ok: wstOK1min,
			env: []string{"PSQL_WST=5000|5s", "PGCOPYDB_SOURCE_PGURI=" + wstOverrideURI}},
		{name: "unreadable warns", warn: "warn: could not read the source wal_sender_timeout", env: []string{"PSQL_WST=fail"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The stub's grants hold only once applied, so the super variant is the passing one.
			out, code, _ := run(t, preflightScriptFor(superMigration()), "", tc.env...)
			if code != 0 || !strings.Contains(out, preflightAllChecksPassed) {
				t.Fatalf("a wal_sender_timeout finding must never fail preflight: code=%d out:\n%s", code, out)
			}
			if tc.ok != "" && (!strings.Contains(out, tc.ok) || strings.Contains(out, "wal_sender_timeout is ")) {
				t.Fatalf("want %q and no warning:\n%s", tc.ok, out)
			}
			if tc.warn != "" && !strings.Contains(out, tc.warn) {
				t.Fatalf("missing %q:\n%s", tc.warn, out)
			}
			if strings.Contains(tc.warn, "below 60s") &&
				(!strings.Contains(out, wstURIRemedy) || !strings.Contains(out, "spec.postgresql.parameters")) {
				t.Fatalf("warning must name both remedies:\n%s", out)
			}
		})
	}
	t.Run("clone-only Migrations do not probe it", func(t *testing.T) {
		if strings.Contains(preflightScriptFor(passwordMigration()), walSenderTimeoutProbe) {
			t.Fatal("wal_sender_timeout matters only to follow; a plain clone must not warn about it")
		}
	})
}

// TestWalSenderTimeoutBlock_Server proves against a real server that the
// probe sees a per-URI options=-c override, which is what pgcopydb's
// replication connection, built from the same URI, gets.
func TestWalSenderTimeoutBlock_Server(t *testing.T) {
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
	for _, tc := range []struct{ value, want string }{
		{"5s", wstWarnPrefix + "5s, below 60s"},
		{"0", "ok: source wal_sender_timeout disabled"},
		{"60s", wstOK1min},
		{"2min", "ok: source wal_sender_timeout 2min"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			src := uri + sep + "options=-c%20wal_sender_timeout%3D" + tc.value
			cmd := exec.Command(shellPath, "-c", preflightHeader+walSenderTimeoutBlock)
			cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+src, "PGCOPYDB_TARGET_PGURI="+uri)
			out, err := cmd.CombinedOutput()
			if err != nil || !strings.Contains(string(out), tc.want) {
				t.Fatalf("err=%v, want %q:\n%s", err, tc.want, out)
			}
		})
	}
}

// TestEmitPreflightOutcome_Warnings: warn lines become one Warning event
// ahead of the summary, which still reports a pass.
func TestEmitPreflightOutcome_Warnings(t *testing.T) {
	wst := "source wal_sender_timeout is 5s, below 60s: raise it"
	log := "ok: connectivity source\nok: connectivity target\n" +
		"warn: source superuserSecretRef user lacks rolsuper; attempting remediation anyway\n" +
		warnPrefix + wst + "\n" +
		"preflight: all checks passed\n"
	r := &MigrationReconciler{Recorder: events.NewFakeRecorder(20), Logs: &fakeLogs{out: log}}
	r.emitPreflightOutcome(context.Background(), passwordMigration())
	got := drainEvents(r.Recorder.(*events.FakeRecorder))
	if len(got) != 2 {
		t.Fatalf("want 2 events (warning bundle, summary), got %v", got)
	}
	if !strings.HasPrefix(got[0], "Warning PreflightWarning source superuserSecretRef user lacks rolsuper") ||
		!strings.Contains(got[0], "\n"+wst) {
		t.Fatalf("warning bundle wrong: %q", got[0])
	}
	if !strings.Contains(got[1], "Normal PreflightPassed 2 checks passed, 0 grants applied") {
		t.Fatalf("summary event wrong: %q", got[1])
	}
}
