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
		argv := progressCommand(false, false)
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
	// users landed, audit is empty on both sides, documents is owed and named.
	if c := sample(); c.TablesTotal != 3 || c.TablesDone != 2 || c.EmptyOnTarget != "public.documents" {
		t.Fatalf("tables = %d of %d owing %q, want 2 of 3 owing public.documents, which holds rows on the source and none on the target", c.TablesDone, c.TablesTotal, c.EmptyOnTarget)
	}
	sqlOutput(t, uris[1], "INSERT INTO documents SELECT i, repeat('x', 100) FROM generate_series(1, 50) i")
	if c := sample(); c.TablesTotal != 3 || c.TablesDone != 3 || c.EmptyOnTarget != "" {
		t.Fatalf("tables = %d of %d owing %q after the rows landed, want 3 of 3 owing nothing", c.TablesDone, c.TablesTotal, c.EmptyOnTarget)
	}
}

// A table holding rows on both sides reads done whatever the two counts are.
// That is the shape of a live source, which runs ahead of the copy's snapshot
// until the stream catches up: a sampler that compared the counts held the
// follow gate shut with the base copy long finished (see
// docs/research/measurements.md#an-exact-row-count-held-the-follow-gate-against-a-live-source).
// A table copied in parts reads the same way once no part of it is in flight,
// and belongs to the checks that read content: pgcopydb's catalog after a
// plain clone, the drain verification after a cutover.
func TestProgressSampleCountsTargetBehindSourceAsDone(t *testing.T) {
	admin := testPGURI(t)
	uris := make([]string, 0, 2)
	for _, side := range []string{"source", "target"} {
		db := fmt.Sprintf("progress_behind_%s_%d", side, time.Now().UnixNano())
		sqlOutput(t, admin, "CREATE DATABASE "+db)
		t.Cleanup(func() { sqlOutput(t, admin, "DROP DATABASE "+db+" WITH (FORCE)") })
		uri := namedURI(t, admin, db, "progress_behind_"+side)
		sqlOutput(t, uri, "CREATE TABLE orders (id integer PRIMARY KEY, note text)")
		uris = append(uris, uri)
	}
	sqlOutput(t, uris[0], "INSERT INTO orders SELECT i, repeat('x', 50) FROM generate_series(1, 35000) i")
	sqlOutput(t, uris[1], "INSERT INTO orders SELECT i, repeat('x', 50) FROM generate_series(1, 12000) i")
	argv := progressCommand(false, false)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+uris[0], "PGCOPYDB_TARGET_PGURI="+uris[1])
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("sample failed: %v", err)
	}
	c := parseSample(out).Counts
	if c == nil {
		t.Fatalf("no counts in %q", out)
	}
	if c.TablesTotal != 1 || c.TablesDone != 1 || c.EmptyOnTarget != "" {
		t.Fatalf("tables = %d of %d owing %q, want 1 of 1 owing nothing with orders holding 12000 of 35000 rows on the target", c.TablesDone, c.TablesTotal, c.EmptyOnTarget)
	}
}

// The all-databases sampler asks the instance catalog, and a side whose query
// fails prints empty rather than erroring, so only a real instance catches a
// malformed one: issue #277 left the source unparseable and every test stayed
// green.
func TestProgressSampleAllDatabases(t *testing.T) {
	uri := namedURI(t, testPGURI(t), "", "progress_all_databases")
	argv := progressCommand(false, true)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+uri, "PGCOPYDB_TARGET_PGURI="+uri)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("sample failed: %v", err)
	}
	s := parseSample(out)
	if s.SourceSize == nil || s.TargetSize == nil {
		t.Fatalf("sample = %+v from %q, want an instance size on both sides; psql said: %s", s, out, stderr.String())
	}
	// The instance is live and the sides are read a moment apart, so only the
	// sign is stable enough to assert.
	if *s.SourceSize <= 0 || *s.TargetSize <= 0 {
		t.Fatalf("sizes = %d and %d, want a positive instance size on both sides", *s.SourceSize, *s.TargetSize)
	}
	// The padding zeros must stay out of status rather than read as a clone
	// that finished with nothing to do.
	if s.Counts != nil {
		t.Fatalf("counts = %+v, want none: the instance catalog counts no relations", s.Counts)
	}
}

// CloneStage's query is the other script no live instance had parsed, and it
// answers "neither phase" when it fails, so a malformed one degrades to an
// unknown stage rather than erroring: the silent shape of issue #277.
func TestCloneStageQueryOnLiveInstance(t *testing.T) {
	// The query counts backends named pgcopydb%, so the sampler's own
	// connection must not be one or it counts itself as the tail.
	uri := namedURI(t, testPGURI(t), "", "progress_stage_test")
	stage := func() string {
		t.Helper()
		argv := progressCommand(true, false)
		cmd := exec.Command(argv[0], argv[1:]...)
		cmd.Env = append(os.Environ(), "PGCOPYDB_TARGET_PGURI="+uri)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("clone stage query failed: %v; psql said: %s", err, stderr.String())
		}
		return strings.TrimSpace(string(out))
	}
	worker := exec.Command("psql", namedURI(t, uri, "", "pgcopydb copy worker 3"), "-XqtA", "-v", "ON_ERROR_STOP=1")
	stdin, err := worker.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.Start(); err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_, _ = io.WriteString(stdin, "ROLLBACK;\n\\q\n")
		_ = stdin.Close()
		if err := worker.Wait(); err != nil {
			t.Errorf("copy worker failed: %v", err)
		}
	}
	// A failed assertion must not leave the backend behind for the next test.
	defer release()
	// An open transaction holds the backend without keeping it active: the
	// first counter has to match on the name alone.
	if _, err = io.WriteString(stdin, "BEGIN;\n"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "copy worker backend never appeared", func() bool {
		return sqlOutput(t, uri, "select count(*) from pg_stat_activity where application_name='pgcopydb copy worker 3'") == "1"
	})
	// Copy backends first, then the tail: CloneStage reads the pair with
	// Sscanf and calls anything else unknown.
	if got := stage(); got != "1 0" {
		t.Fatalf("clone stage = %q with one copy worker connected, want \"1 0\"", got)
	}
	release()
	if got := stage(); got != "0 0" {
		t.Fatalf("clone stage = %q with no pgcopydb backend left, want \"0 0\"", got)
	}
}

// A held relation lock must cancel the sampled side without losing its peer.
// The lock here belongs to no copy, so the target waits on it as before.
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
				argv := progressCommand(false, false)
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
					return sqlOutput(t, uri, "select count(*) from pg_stat_activity where application_name=current_setting('application_name') and pid<>pg_backend_pid() and wait_event_type='Lock' and query like 'with %'") == "1"
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
			argv := progressCommand(false, false)
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

// openCopy starts a session that runs setup and then a COPY FROM STDIN it
// never ends, the shape of a part in flight. It returns once the server has
// read every row it was sent.
func openCopy(t *testing.T, uri, app, setup, table string) {
	t.Helper()
	if sqlOutput(t, uri, "select current_setting('server_version_num')::int < 140000") == "t" {
		t.Skip("pg_stat_progress_copy arrived in PostgreSQL 14; older targets probe every table")
	}
	var b strings.Builder
	b.WriteString("BEGIN;\n" + setup + "COPY " + table + " FROM STDIN;\n")
	for i := range 20000 {
		fmt.Fprintf(&b, "%d\t%s\n", i, strings.Repeat("x", 40))
	}
	holdOpen(t, uri, app, b.String())
	last := ""
	waitFor(t, 10*time.Second, "the copy never settled", func() bool {
		got := copyBytes(t, uri, table)
		settled := got != "0" && got == last
		last = got
		time.Sleep(100 * time.Millisecond)
		return settled
	})
}

// holdOpen sends script to a psql session that stays connected, and so keeps
// its transaction open, until the test ends. It returns the session's input.
func holdOpen(t *testing.T, uri, app, script string) io.Writer {
	t.Helper()
	cmd := exec.Command("psql", namedURI(t, uri, "", app), "-Xq", "-v", "ON_ERROR_STOP=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Killing psql drops the connection, which aborts the open transaction.
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if _, err = io.WriteString(stdin, script); err != nil {
		t.Fatal(err)
	}
	return stdin
}

// pgcopydbWorker names a session the way pgcopydb names its copy workers.
const pgcopydbWorker = "pgcopydb[1] copy worker"

// copyBytes reads the bytes the open copy into table has processed.
func copyBytes(t *testing.T, uri, table string) string {
	t.Helper()
	return sqlOutput(t, uri, "select coalesce(sum(bytes_processed), 0) from pg_stat_progress_copy where relid = '"+table+"'::regclass")
}

// sampleDatabases creates a source and a target database, each running setup,
// and returns their URIs.
func sampleDatabases(t *testing.T, name, setup string) (source, target string) {
	t.Helper()
	admin := testPGURI(t)
	uris := make([]string, 0, 2)
	for _, side := range []string{sourceSide, targetSide} {
		db := fmt.Sprintf("progress_%s_%s_%d", name, side, time.Now().UnixNano())
		sqlOutput(t, admin, "CREATE DATABASE "+db)
		t.Cleanup(func() { sqlOutput(t, admin, "DROP DATABASE "+db+" WITH (FORCE)") })
		uri := namedURI(t, admin, db, "progress_"+name+"_"+side)
		sqlOutput(t, uri, setup)
		uris = append(uris, uri)
	}
	return uris[0], uris[1]
}

// runSample runs the single-database sampler against the pair and fails the
// test unless both sides answered within the SQL budget.
func runSample(t *testing.T, source, target string) *Sample {
	t.Helper()
	argv := progressCommand(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+source, "PGCOPYDB_TARGET_PGURI="+target)
	start := time.Now()
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("sample failed: %v", err)
	}
	s := parseSample(out)
	if s.SourceSize == nil || s.TargetSize == nil || s.Counts == nil {
		t.Fatalf("sample lost a side after %s: %q", time.Since(start).Round(time.Millisecond), out)
	}
	return s
}

// pgcopydb copies an unsplit table as BEGIN; TRUNCATE; COPY FREEZE under an
// AccessExclusiveLock, so the sample sizes it from the copy's byte count. A
// worker that reconnected after a failed copy is named after the table instead.
func TestProgressSampleReadsAnExclusiveCopy(t *testing.T) {
	for _, app := range []string{pgcopydbWorker, "pgcopydb[1] copy public.items"} {
		t.Run(app, func(t *testing.T) {
			source, target := sampleDatabases(t, "exclusive", "CREATE TABLE items (id integer, note text); INSERT INTO items VALUES (1, 'x')")
			openCopy(t, target, app, "TRUNCATE items;\n", "items")
			if got := sqlOutput(t, target, "select count(*) from pg_locks where relation = 'items'::regclass and mode = 'AccessExclusiveLock' and granted"); got != "1" {
				t.Fatalf("fixture: %s granted AccessExclusiveLock on items, want 1", got)
			}
			before := copyBytes(t, target, "items")
			c := runSample(t, source, target).Counts
			after := copyBytes(t, target, "items")
			if c.TablesTotal != 1 || c.TablesDone != 0 || c.EmptyOnTarget != "public.items" {
				t.Fatalf("tables = %d of %d owing %q, want 0 of 1 owing public.items while its copy is open", c.TablesDone, c.TablesTotal, c.EmptyOnTarget)
			}
			if got := fmt.Sprint(c.BytesDone); got != before || got != after {
				t.Fatalf("bytes done = %s, want the copy's bytes processed (%s before the sample, %s after)", got, before, after)
			}
		})
	}
}

// pgcopydb's index workers take AccessExclusiveLock too, on a table already
// copied, to attach a constraint to its index. That table stays done and
// sized on disk, so the sample waits out the short ALTER instead.
func TestProgressSampleWaitsOutAnIndexWorkersLock(t *testing.T) {
	source, target := sampleDatabases(t, "index_lock", `CREATE TABLE items (id integer, note text);
INSERT INTO items SELECT i, 'x' FROM generate_series(1, 20000) i;
CREATE UNIQUE INDEX items_idx ON items (id)`)
	alter := holdOpen(t, target, "pgcopydb[1] create index public.items_idx",
		"BEGIN;\nALTER TABLE items ADD CONSTRAINT items_pk PRIMARY KEY USING INDEX items_idx;\n")
	waitFor(t, 5*time.Second, "the index worker never held items exclusively", func() bool {
		return sqlOutput(t, target, "select count(*) from pg_locks where relation = 'items'::regclass and mode = 'AccessExclusiveLock' and granted") == "1"
	})
	// Commit once the sampler waits on the lock. This goroutine cannot fail
	// the test, so it commits at its deadline regardless.
	go func() {
		waiting := "select count(*) from pg_stat_activity where application_name = current_setting('application_name') and pid <> pg_backend_pid() and wait_event_type = 'Lock'"
		for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline); time.Sleep(25 * time.Millisecond) {
			if out, _ := exec.Command("psql", target, "-XqtAc", waiting).Output(); strings.TrimSpace(string(out)) == "1" {
				break
			}
		}
		_, _ = io.WriteString(alter, "COMMIT;\n")
	}()
	c := runSample(t, source, target).Counts
	if c.TablesTotal != 1 || c.TablesDone != 1 || c.EmptyOnTarget != "" {
		t.Fatalf("tables = %d of %d owing %q, want 1 of 1: an index worker's lock is no copy", c.TablesDone, c.TablesTotal, c.EmptyOnTarget)
	}
	if want := sqlOutput(t, target, "select pg_table_size('items')"); fmt.Sprint(c.BytesDone) != want {
		t.Fatalf("bytes done = %d, want %s, the table's size on disk", c.BytesDone, want)
	}
}

// An open part's rows stay invisible until it commits, so a probe reads the
// whole uncommitted heap. The sample must call the table owed without scanning
// it, and keep sizing it on disk (a part holds only RowExclusiveLock).
func TestProgressSampleNeverScansAnOpenCopy(t *testing.T) {
	source, target := sampleDatabases(t, "open_copy", `CREATE TABLE big (id integer, note text);
CREATE TABLE small (id integer, note text);
INSERT INTO small VALUES (1, 'x')`)
	sqlOutput(t, source, "INSERT INTO big VALUES (1, 'x')")
	openCopy(t, target, pgcopydbWorker, "", "big")
	// Another client's copy is not this migration's, so small stays done.
	openCopy(t, target, "progress_foreign_copy", "", "small")
	scans := func() (big, small string) {
		t.Helper()
		row := sqlOutput(t, target, "select string_agg(seq_scan::text, ' ' order by relname) from pg_stat_user_tables where relname in ('big', 'small')")
		big, small, _ = strings.Cut(row, " ")
		return big, small
	}
	bigBefore, smallBefore := scans()
	c := runSample(t, source, target).Counts
	if c.TablesTotal != 2 || c.TablesDone != 1 || c.EmptyOnTarget != "public.big" {
		t.Fatalf("tables = %d of %d owing %q, want 1 of 2 owing public.big while its copy is open", c.TablesDone, c.TablesTotal, c.EmptyOnTarget)
	}
	if want := sqlOutput(t, target, "select pg_table_size('big') + pg_table_size('small')"); fmt.Sprint(c.BytesDone) != want {
		t.Fatalf("bytes done = %d, want %s, the on-disk size of both tables, uncommitted pages included", c.BytesDone, want)
	}
	// The sampler's backend reports its scans when it exits, and on
	// PostgreSQL 14 they reach the view a moment later, so wait for the probe
	// of small before reading big's counter.
	var bigAfter string
	waitFor(t, 5*time.Second, "the sampler's probe of small never reached pg_stat_user_tables", func() bool {
		var smallAfter string
		bigAfter, smallAfter = scans()
		return smallAfter != smallBefore
	})
	if bigAfter != bigBefore {
		t.Fatalf("big seq_scan went from %s to %s across the sample: the probe scanned a table whose copy is open", bigBefore, bigAfter)
	}
}

// pgcopydb truncates before its copy opens, and truncating a partitioned
// parent locks every partition while the copy's progress names only the
// parent. A partition held that way must neither be sized nor probed.
func TestProgressSampleSkipsTablesTheWorkerHoldsExclusively(t *testing.T) {
	setup := "CREATE TABLE items (id integer, note text)"
	partitioned := "CREATE TABLE items (id integer, note text) PARTITION BY RANGE (id); CREATE TABLE items_p1 PARTITION OF items FOR VALUES FROM (0) TO (100000)"
	for _, tc := range []struct {
		name, setup, leaf string
		hold              func(t *testing.T, target string)
	}{
		{"truncated before its copy", setup, "items", func(t *testing.T, target string) {
			holdOpen(t, target, pgcopydbWorker, "BEGIN;\nTRUNCATE items;\n")
		}},
		{"partition under a copied parent", partitioned, "items_p1", func(t *testing.T, target string) {
			openCopy(t, target, pgcopydbWorker, "TRUNCATE items;\n", "items")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, target := sampleDatabases(t, "held", tc.setup+"; INSERT INTO items VALUES (1, 'x')")
			tc.hold(t, target)
			waitFor(t, 5*time.Second, "the worker never held the table exclusively", func() bool {
				return sqlOutput(t, target, "select count(*) from pg_locks where relation = '"+tc.leaf+"'::regclass and mode = 'AccessExclusiveLock' and granted") == "1"
			})
			c := runSample(t, source, target).Counts
			if c.TablesTotal != 1 || c.TablesDone != 0 || c.EmptyOnTarget != "public."+tc.leaf {
				t.Fatalf("tables = %d of %d owing %q, want 0 of 1 owing public.%s while it is held", c.TablesDone, c.TablesTotal, c.EmptyOnTarget, tc.leaf)
			}
			if c.BytesDone != 0 {
				t.Fatalf("bytes done = %d, want 0: no copy streams into the held table itself", c.BytesDone)
			}
		})
	}
}

// libpq echoes a connection URI it cannot parse, password included, and the
// lost-side log carries the reason, so only a server's own words may pass.
func TestProgressSampleWithholdsAnEchoedURI(t *testing.T) {
	source := testPGURI(t)
	for _, target := range []string{
		"postgresql://app:hunter%zz2@127.0.0.1:1/postgres",
		"postgresql://app:hunter2@[::1:5432/postgres",
	} {
		argv := progressCommand(false, false)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
		cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+source, "PGCOPYDB_TARGET_PGURI="+target)
		out, err := cmd.Output()
		cancel()
		if err != nil {
			t.Fatalf("sample failed: %v", err)
		}
		if !strings.Contains(string(out), "hunter") {
			t.Fatalf("psql no longer echoes the URI, so this test proves nothing: %q", out)
		}
		reason, lost := parseSample(out).Lost[targetSide]
		if !lost || reason != withheldReason {
			t.Errorf("target reason = %q (lost %v), want the withheld reason", reason, lost)
		}
	}
}
