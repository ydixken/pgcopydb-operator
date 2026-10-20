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
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ydixken/pgcopydb-operator/internal/conn"
)

// TestGateProbe_Server runs the probe script as a plain role in a database of its own:
// revoking TEMPORARY on a shared database would break the packages that run in parallel.
func TestGateProbe_Server(t *testing.T) {
	admin := os.Getenv("PGCOPYDB_TEST_PGURI")
	if admin == "" {
		t.Skip("CI supplies PGCOPYDB_TEST_PGURI for retry gate SQL regressions")
	}
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	requireGNUTimeout(t)
	query := func(t *testing.T, uri, sql string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, psql, uri, "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", sql).CombinedOutput()
		if err != nil {
			t.Fatalf("fixture SQL %q failed: %v\n%s", sql, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	db := fmt.Sprintf("pgm_gate_%d", time.Now().UnixNano())
	role := db + "_role"
	query(t, admin, "CREATE ROLE "+role+" LOGIN PASSWORD '"+role+"'")
	t.Cleanup(func() { query(t, admin, "DROP ROLE "+role) })
	query(t, admin, "CREATE DATABASE "+db)
	t.Cleanup(func() { query(t, admin, "DROP DATABASE "+db+" WITH (FORCE)") })
	query(t, admin, "GRANT CONNECT ON DATABASE "+db+" TO "+role)

	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is not a URI")
	}
	u.User = url.UserPassword(role, role)
	u.Path = "/" + db
	// A URI application_name outranks PGAPPNAME, which names the probe and the leftovers below.
	q := u.Query()
	q.Del("application_name")
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	target := u.String()

	probe := func(t *testing.T, uri string, in gateInputs) (string, int, gateResult) {
		t.Helper()
		out, code := runGate(t, gateScript(false), "PGCOPYDB_TARGET_PGURI="+uri, "PGAPPNAME=pgm-retry-gate",
			"GATE_COMMIT_TEST="+gateFlag(in.CommitTest), "GATE_EXPECT_LOCAL="+gateFlag(in.ExpectLocal))
		t.Logf("probe output (exit %d):\n%s", code, out)
		return out, code, parseGateLog(out)
	}
	commitOnly := gateInputs{CommitTest: true}
	mustContain := func(t *testing.T, out string, want ...string) {
		t.Helper()
		for _, w := range want {
			if !strings.Contains(out, w) {
				t.Errorf("probe output lacks %q", w)
			}
		}
	}

	t.Run("a clean target passes", func(t *testing.T) {
		out, code, g := probe(t, target, commitOnly)
		mustContain(t, out, "gate: census total=0 syncrep=0 named=yes", gateCommitOKLine, gatePassLine)
		if code != 0 || !g.passed(commitOnly) {
			t.Errorf("exit %d, passed %v; want 0 and a pass", code, g.passed(commitOnly))
		}
	})

	t.Run("a pgcopydb session holds", func(t *testing.T) {
		const app = "pgcopydb[1] clone"
		sleeper := exec.Command(psql, target, "-XAtq", "-c", "select pg_sleep(20)")
		sleeper.Env = append(os.Environ(), "PGAPPNAME="+app)
		if err := sleeper.Start(); err != nil {
			t.Fatal(err)
		}
		sessions := func(extra string) string {
			return query(t, admin, "select count(*) from pg_stat_activity where datname = '"+db+"' and application_name = '"+app+"'"+extra)
		}
		defer func() {
			query(t, admin, "select count(pg_terminate_backend(pid)) from pg_stat_activity where datname = '"+db+"' and application_name = '"+app+"'")
			_ = sleeper.Wait()
			waitUntil(t, "the pg_sleep session outlived its termination", func() bool { return sessions("") == "0" })
		}()
		waitUntil(t, "the pg_sleep session never started waiting", func() bool { return sessions(" and wait_event = 'PgSleep'") == "1" })

		out, code, g := probe(t, target, commitOnly)
		mustContain(t, out, "gate: census total=1 syncrep=0 named=yes", gateCommitOKLine)
		line := regexp.MustCompile(`(?m)^gate: session pid=\d+ state=active wait=PgSleep age=\d+s started=\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ app=pgcopydb\[1\] clone$`)
		if !line.MatchString(out) {
			t.Errorf("no session line for the leftover matches %s", line)
		}
		if reason, _ := g.hold(commitOnly, &passwordMigration().Spec.Target); code != 1 || reason != reasonSessionsRemain {
			t.Errorf("exit %d, reason %q; want 1 and %s", code, reason, reasonSessionsRemain)
		}
	})

	t.Run("a read-only default still commits", func(t *testing.T) {
		query(t, admin, "ALTER ROLE "+role+" SET default_transaction_read_only = on")
		if got := query(t, target, "show default_transaction_read_only"); got != "on" {
			t.Fatalf("fixture: the role's sessions start with default_transaction_read_only=%s", got)
		}
		out, _, _ := probe(t, target, commitOnly)
		mustContain(t, out, gateCommitOKLine)
	})

	t.Run("a missing TEMPORARY grant fails with its SQLSTATE", func(t *testing.T) {
		query(t, admin, "REVOKE TEMPORARY ON DATABASE "+db+" FROM PUBLIC")
		if got := query(t, target, "select has_database_privilege(current_database(), 'TEMP')"); got != "f" {
			t.Fatalf("fixture: the role still holds TEMPORARY (%s)", got)
		}
		out, code, g := probe(t, target, commitOnly)
		mustContain(t, out, "gate: commit failed ERROR:  42501")
		reason, msg := g.hold(commitOnly, &passwordMigration().Spec.Target)
		if code != 1 || reason != reasonProbeFailed || !strings.Contains(msg, "GRANT TEMPORARY ON DATABASE") {
			t.Errorf("exit %d, reason %q, message %q; want 1, %s and the GRANT remedy", code, reason, msg, reasonProbeFailed)
		}
	})

	t.Run("SHOW reads the startup option", func(t *testing.T) {
		sep := "?"
		if strings.Contains(target, "?") {
			sep = "&"
		}
		localOnly := gateInputs{ExpectLocal: true}
		out, code, g := probe(t, target+sep+conn.SyncCommitLocalParam, localOnly)
		mustContain(t, out, gateLocalLine, gatePassLine)
		if code != 0 || !g.passed(localOnly) {
			t.Errorf("exit %d, passed %v; want 0 and a pass", code, g.passed(localOnly))
		}
	})
}

// waitUntil polls check every 100 ms and fails with description after 5 s.
func waitUntil(t *testing.T, description string, check func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !check(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal(description)
		}
	}
}
