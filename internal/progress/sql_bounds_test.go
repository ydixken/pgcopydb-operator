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

package progress

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func testPGURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for sampler SQL regressions")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatal("SQL regressions require psql")
	}
	requireTimeout(t)
	return uri
}

func sqlOutput(t *testing.T, uri, query string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "psql", uri, "-XqtA", "-v", "ON_ERROR_STOP=1", "-c", query).Output()
	if err != nil {
		t.Fatalf("fixture SQL failed: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func namedURI(t *testing.T, uri, db, app string) string {
	t.Helper()
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatal("test PostgreSQL URI is invalid")
	}
	if db != "" {
		u.Path = "/" + db
	}
	q := u.Query()
	q.Set("application_name", app)
	q.Set("options", "-c statement_timeout=0")
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	return u.String()
}

func TestProgressSQLBounds(t *testing.T) {
	uri := namedURI(t, testPGURI(t), "", "progress_sql_test")
	for _, tc := range []struct {
		name, query, wrapper, want string
		fail, timeout              bool
	}{
		{"URI cannot disable timeout", "show statement_timeout", progressSQL, "5s\n", false, false},
		{"commit restores timeout", "show statement_timeout; COMMIT; show statement_timeout", progressSQL, "5s\n0\n", false, false},
		{"rollback restores timeout", "show statement_timeout; ROLLBACK; show statement_timeout", progressSQL, "5s\n0\n", false, false},
		{"query error", "select 1/0", progressSQL, "", true, false},
		{"setup error stops query", "select 42", strings.Replace(progressSQL, "SET LOCAL statement_timeout = 5000", "SET LOCAL nonexistent_progress_setting = 5000", 1), "", true, false},
		{"SQL cancels before process deadline", "select pg_sleep(30)", progressSQL, "", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("sh", "-c", tc.wrapper+`progress_sql "$TEST_URI" "$TEST_QUERY"`)
			cmd.Env = append(os.Environ(), "TEST_URI="+uri, "TEST_QUERY="+tc.query)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			start := time.Now()
			out, err := cmd.Output()
			if (err != nil) != tc.fail || string(out) != tc.want {
				t.Fatalf("unexpected query outcome: failed=%t, output=%q", err != nil, out)
			}
			if tc.timeout && (!strings.Contains(stderr.String(), "statement timeout") || time.Since(start) > 6*time.Second) {
				t.Fatal("SQL was not canceled by statement_timeout before the process deadline")
			}
			if got := sqlOutput(t, uri, "select count(*) from pg_stat_activity where application_name='progress_sql_test' and pid <> pg_backend_pid()"); got != "0" {
				t.Fatal("sampler backend survived its command")
			}
		})
	}
}

func TestProgressSQLTransactionOutcome(t *testing.T) {
	uri := namedURI(t, testPGURI(t), "", "progress_transaction_test")
	table := fmt.Sprintf("progress_transaction_%d", time.Now().UnixNano())
	sqlOutput(t, uri, "CREATE TABLE "+table+" (id integer)")
	t.Cleanup(func() { sqlOutput(t, uri, "DROP TABLE "+table) })
	// A write between the helper's -c commands witnesses psql's automatic commit or rollback.
	wrapper := strings.Replace(progressSQL, `-c "$2"`, `-c 'INSERT INTO `+table+` VALUES (1)' -c "$2"`, 1)
	for _, tc := range []struct {
		name, query, rows string
		fail              bool
	}{
		{"success commits", "select 42", "1", false},
		{"SQL error rolls back", "select 1/0", "0", true},
		{"cancellation rolls back", "select pg_sleep(30)", "0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlOutput(t, uri, "TRUNCATE "+table)
			cmd := exec.Command("sh", "-c", wrapper+`progress_sql "$TEST_URI" "$TEST_QUERY"`)
			cmd.Env = append(os.Environ(), "TEST_URI="+uri, "TEST_QUERY="+tc.query)
			err := cmd.Run()
			if (err != nil) != tc.fail {
				t.Fatalf("unexpected transaction outcome: failed=%t", err != nil)
			}
			if tc.fail {
				// psql's exit code 3 (ON_ERROR_STOP) applies to -f/stdin scripts;
				// -c commands report the same failure as plain exit code 1.
				if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
					t.Fatalf("expected ON_ERROR_STOP SQL failure, not a connection or process timeout: %v", err)
				}
			}
			if got := sqlOutput(t, uri, "SELECT count(*) FROM "+table); got != tc.rows {
				t.Fatalf("transaction left %s rows, want %s", got, tc.rows)
			}
		})
	}
}

// The shape of issue #277 on a real pair: a table populated on the source,
// restored on the target (so its TOAST relation occupies a page) and holding
// no rows there. The sampler must count it owed, where a storage test counted
// every target table done.
func TestProgressSampleCountsRowsNotStorage(t *testing.T) {
	admin := testPGURI(t)
	uris := make([]string, 0, 2)
	for _, side := range []string{"source", "target"} {
		db := fmt.Sprintf("progress_rows_%s_%d", side, time.Now().UnixNano())
		sqlOutput(t, admin, "CREATE DATABASE "+db)
		t.Cleanup(func() { sqlOutput(t, admin, "DROP DATABASE "+db+" WITH (FORCE)") })
		uri := namedURI(t, admin, db, "progress_rows_"+side)
		sqlOutput(t, uri, `CREATE TABLE users (id integer PRIMARY KEY, name text);
CREATE TABLE documents (id integer PRIMARY KEY, body text);
CREATE TABLE audit (id integer PRIMARY KEY, note text);
INSERT INTO users SELECT i, 'u' || i FROM generate_series(1, 10) i`)
		uris = append(uris, uri)
	}
	sqlOutput(t, uris[0], "INSERT INTO documents SELECT i, repeat('x', 100) FROM generate_series(1, 50) i")
	if got := sqlOutput(t, uris[1], `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND n.nspname = 'public' AND pg_table_size(c.oid) > 0`); got != "3" {
		t.Fatalf("fixture: %s of the 3 target tables occupy storage, want all of them", got)
	}
	sample := func() *RelationCounts {
		t.Helper()
		argv := progressCommand(false)
		cmd := exec.Command(argv[0], argv[1:]...)
		cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+uris[0], "PGCOPYDB_TARGET_PGURI="+uris[1])
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("sample failed: %v", err)
		}
		s := parseSample(out)
		if s.Counts == nil {
			t.Fatalf("no counts in %q", out)
		}
		return s.Counts
	}
	// users landed, audit is empty on both sides, documents is owed.
	if c := sample(); c.TablesTotal != 3 || c.TablesDone != 2 {
		t.Fatalf("tables = %d of %d, want 2 of 3 with documents holding rows on the source and none on the target", c.TablesDone, c.TablesTotal)
	}
	sqlOutput(t, uris[1], "INSERT INTO documents SELECT i, repeat('x', 100) FROM generate_series(1, 50) i")
	if c := sample(); c.TablesTotal != 3 || c.TablesDone != 3 {
		t.Fatalf("tables = %d of %d after the rows landed, want 3 of 3", c.TablesDone, c.TablesTotal)
	}
}

// A held relation lock must cancel the sampled side without losing its peer.
func TestProgressRelationLocks(t *testing.T) {
	admin := testPGURI(t)
	const source, target = "source", "target"
	uris := make([]string, 0, 2)
	for _, side := range []string{source, target} {
		db := fmt.Sprintf("progress_%s_%d", side, time.Now().UnixNano())
		sqlOutput(t, admin, "CREATE DATABASE "+db)
		t.Cleanup(func() { sqlOutput(t, admin, "DROP DATABASE "+db+" WITH (FORCE)") })
		uri := namedURI(t, admin, db, "progress_"+side)
		sqlOutput(t, uri, "CREATE TABLE items (id integer); INSERT INTO items VALUES (1)")
		uris = append(uris, uri)
	}
	for i, uri := range uris {
		t.Run([]string{source, target}[i], func(t *testing.T) {
			blocker := exec.Command("psql", namedURI(t, uri, "", "progress_blocker"), "-XqtA", "-v", "ON_ERROR_STOP=1")
			stdin, err := blocker.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = blocker.Start(); err != nil {
				t.Fatal(err)
			}
			released := false
			release := func() {
				if !released {
					released = true
					_, _ = io.WriteString(stdin, "ROLLBACK;\n\\q\n")
					_ = stdin.Close()
					if err := blocker.Wait(); err != nil {
						t.Errorf("blocker failed: %v", err)
					}
				}
			}
			defer release()
			_, err = io.WriteString(stdin, "BEGIN; LOCK items IN ACCESS EXCLUSIVE MODE;\n")
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 2*time.Second, "blocker did not acquire relation lock", func() bool {
				return sqlOutput(t, uri, "select count(*) from pg_locks l join pg_stat_activity a using(pid) where a.application_name='progress_blocker' and l.relation='items'::regclass and l.mode='AccessExclusiveLock' and l.granted") == "1"
			})
			for range 3 {
				argv := progressCommand(false)
				cmd := exec.Command(argv[0], argv[1:]...)
				cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+uris[0], "PGCOPYDB_TARGET_PGURI="+uris[1])
				var out bytes.Buffer
				cmd.Stdout = &out
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				defer func() {
					if cmd.ProcessState == nil {
						_ = cmd.Process.Kill()
						<-done
					}
				}()
				waitFor(t, 3*time.Second, "sampler never waited on the held relation lock", func() bool {
					return sqlOutput(t, uri, "select count(*) from pg_stat_activity where application_name=current_setting('application_name') and pid<>pg_backend_pid() and wait_event_type='Lock' and query like 'with t as (%'") == "1"
				})
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("sample failed: %v", err)
					}
				case <-time.After(6 * time.Second):
					t.Fatal("blocked sampler exceeded SQL budget")
				}
				s := parseSample(out.Bytes())
				if s.Counts != nil || (i == 0 && (s.SourceSize != nil || s.TargetSize == nil)) || (i == 1 && (s.TargetSize != nil || s.SourceSize == nil)) {
					t.Fatal("blocked sample lost the healthy side or invented counts")
				}
				if sqlOutput(t, uri, "select count(*) from pg_stat_activity where application_name=current_setting('application_name') and pid<>pg_backend_pid()") != "0" {
					t.Fatal("sampler backend survived cancellation")
				}
				if sqlOutput(t, uri, "select count(*) from pg_stat_activity where application_name='progress_blocker' and state='idle in transaction'") != "1" {
					t.Fatal("sampler cancellation disturbed the blocker")
				}
			}
			release()
			argv := progressCommand(false)
			cmd := exec.Command(argv[0], argv[1:]...)
			cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+uris[0], "PGCOPYDB_TARGET_PGURI="+uris[1])
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("recovery sample failed: %v", err)
			}
			if s := parseSample(out); s.Counts == nil || s.Counts.TablesTotal != 1 || s.Counts.TablesDone != 1 {
				t.Fatal("sampling did not recover after unlocking")
			}
		})
	}
}
