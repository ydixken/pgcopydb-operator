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
	"path/filepath"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	reownTestOwner = "app_owner_role"
	reownFrom      = "reown_from_test"
	reownTo        = "reown_to_test"
	reownSuper     = "reown_super_test"
	reownHandedOK  = "ok: ownership handed over"
)

func reownMigration() *v1beta1.Migration {
	m := passwordMigration()
	m.Spec.Clone.NoOwner = true
	m.Spec.Clone.OwnerAfterRestore = reownTestOwner
	return m
}

func TestReownRequested(t *testing.T) {
	if reownRequested(passwordMigration()) {
		t.Fatal("an unset ownerAfterRestore must not request a handover")
	}
	if !reownRequested(reownMigration()) {
		t.Fatal("ownerAfterRestore must request a handover")
	}
}

func TestBuildReownJob(t *testing.T) {
	m := reownMigration()
	ttl := int32(60)
	m.Spec.TTLSecondsAfterFinished = &ttl
	job, err := buildReownJob(m, testRunnerImage)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "m-reown" {
		t.Fatalf("job name = %q", job.Name)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if got := envValue(c.Env, reownOwnerEnv); got != reownTestOwner {
		t.Fatalf("%s = %q", reownOwnerEnv, got)
	}
	for _, s := range slices.Concat(c.Command, c.Args) {
		if strings.Contains(s, reownTestOwner) {
			t.Fatalf("the role name must ride as env only, found in command text:\n%s", s)
		}
	}
	if c.Args[1] != reownScript() {
		t.Fatal("the container must run the shipped handover script")
	}
	for _, want := range []string{"statement_timeout=60s", "lock_timeout=60s"} {
		if !strings.Contains(envValue(c.Env, "PGOPTIONS"), want) {
			t.Fatalf("PGOPTIONS lacks %s: %q", want, envValue(c.Env, "PGOPTIONS"))
		}
	}
	if envValue(c.Env, "PGCONNECT_TIMEOUT") == "" {
		t.Fatal("connect timeout missing")
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 1800 {
		t.Fatalf("activeDeadlineSeconds = %v", job.Spec.ActiveDeadlineSeconds)
	}
	if job.Spec.TTLSecondsAfterFinished != nil {
		t.Fatal("the finished handover Job must outlive the spec TTL")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 2 {
		t.Fatalf("backoffLimit = %v", job.Spec.BackoffLimit)
	}
}

// TestReownScriptGolden pins the shipped script byte for byte: the Job's
// command text is a contract with the runner image and with the log lines
// the live test asserts on. UPDATE_GOLDEN=1 rewrites the snapshot.
func TestReownScriptGolden(t *testing.T) {
	path := filepath.Join("testdata", "reown.sh")
	got := reownScript()
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Fatalf("the handover script drifted from %s; review the change, then rerun with UPDATE_GOLDEN=1 to accept it", path)
	}
}

// TestReownScriptHeredocQuoting checks the one shell rule the script rests on:
// only the heredoc expanding $REOWN_CANDIDATES_CTE is unquoted, every other one
// is quoted so its SQL is never shell-evaluated, and that unquoted body carries
// no further $, no backtick, and a backslash only where the shell leaves it
// alone (psql's \gexec), never before $, a backtick, another backslash or a line end.
func TestReownScriptHeredocQuoting(t *testing.T) {
	lines := strings.Split(reownScript(), "\n")
	seen := 0
	for i := 0; i < len(lines); i++ {
		_, after, found := strings.Cut(lines[i], "<<")
		if !found {
			continue
		}
		marker := strings.Fields(after)[0]
		quoted := strings.HasPrefix(marker, "'")
		name := strings.Trim(marker, "'")
		var body []string
		for i++; i < len(lines) && lines[i] != name; i++ {
			body = append(body, lines[i])
		}
		if i == len(lines) {
			t.Fatalf("heredoc %s is unterminated", name)
		}
		expands := slices.Contains(body, "$REOWN_CANDIDATES_CTE")
		if expands == quoted {
			t.Errorf("heredoc %s: quoted=%t but expands the CTE=%t", marker, quoted, expands)
		}
		for _, line := range body {
			if quoted || line == "$REOWN_CANDIDATES_CTE" {
				continue
			}
			for j, r := range line {
				escaped := r == '\\' && (j+1 == len(line) || strings.ContainsRune("$`\\", rune(line[j+1])))
				if r == '$' || r == '`' || escaped {
					t.Errorf("unquoted heredoc %s would shell-expand %q", marker, line)
				}
			}
		}
		seen++
	}
	if seen < 8 {
		t.Fatalf("found %d heredocs, the script is not the one this test knows", seen)
	}
}

// reownFixture builds one of every object kind the candidate set tells
// apart, owned by the migration role. The ordered-set aggregate and the
// extension members are created by the superuser and handed over, because
// only a superuser may create an internal-state aggregate, and a trusted
// extension installed by a non-superuser leaves its members owned by the
// bootstrap superuser; a superuser migration role owns them directly.
const reownFixture = `CREATE ROLE reown_from_test;
CREATE ROLE reown_to_test;
GRANT reown_to_test TO reown_from_test;
SELECT format('GRANT CREATE ON DATABASE %I TO reown_from_test', current_database()) \gexec
SET ROLE reown_from_test;
CREATE SCHEMA reown_s;
CREATE TABLE reown_s.part (id bigint, k int) PARTITION BY LIST (k);
CREATE TABLE reown_s.part_1 PARTITION OF reown_s.part FOR VALUES IN (1);
CREATE TABLE reown_s.part_2 PARTITION OF reown_s.part FOR VALUES IN (2);
CREATE TABLE reown_s.ident (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY);
CREATE TABLE reown_s.bigs (id bigserial PRIMARY KEY, v text);
CREATE SEQUENCE reown_s.standalone_seq;
CREATE VIEW reown_s.v AS SELECT id FROM reown_s.bigs;
CREATE MATERIALIZED VIEW reown_s.mv AS SELECT id FROM reown_s.bigs;
CREATE TYPE reown_s.ctype AS (a int, b text);
CREATE TYPE reown_s.mood AS ENUM ('sad', 'ok');
CREATE DOMAIN reown_s.posint AS int CHECK (VALUE > 0);
CREATE TYPE reown_s.myrange AS RANGE (SUBTYPE = int);
CREATE FUNCTION reown_s.f(a int) RETURNS int LANGUAGE sql AS 'select a';
CREATE FUNCTION reown_s.f0() RETURNS int LANGUAGE sql AS 'select 1';
CREATE FUNCTION reown_s.fv(VARIADIC a int[]) RETURNS int LANGUAGE sql AS 'select 1';
CREATE PROCEDURE reown_s.proc(a int) LANGUAGE sql AS 'select 1';
CREATE AGGREGATE reown_s.sum2(int) (SFUNC = int4pl, STYPE = int4, INITCOND = '0');
CREATE AGGREGATE reown_s.cnt(*) (SFUNC = int8inc, STYPE = int8, INITCOND = '0');
CREATE TABLE reown_s.ext_table (id serial PRIMARY KEY);
RESET ROLE;
CREATE AGGREGATE reown_s.osa(float8 ORDER BY anyelement) (SFUNC = ordered_set_transition, STYPE = internal, FINALFUNC = percentile_disc_final, FINALFUNC_EXTRA);
ALTER AGGREGATE reown_s.osa(float8 ORDER BY anyelement) OWNER TO reown_from_test;
CREATE EXTENSION citext SCHEMA reown_s;
CREATE EXTENSION tablefunc SCHEMA reown_s;
ALTER EXTENSION citext ADD TABLE reown_s.ext_table;
ALTER TYPE reown_s.citext OWNER TO reown_from_test;
ALTER FUNCTION reown_s.citextin(cstring) OWNER TO reown_from_test;
ALTER TYPE reown_s.tablefunc_crosstab_2 OWNER TO reown_from_test;
`

// reownCensus lists every object in reown_s with its owner, kind|name|owner,
// straight from the owner columns and independent of the shipped CTE.
const reownCensus = `SELECT kind || '|' || name || '|' || owner FROM (
  SELECT 'schema' AS kind, n.nspname::text AS name, pg_get_userbyid(n.nspowner) AS owner FROM pg_namespace n WHERE n.nspname = 'reown_s'
  UNION ALL SELECT 'relation', c.relname, pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'reown_s'
  UNION ALL SELECT 'routine', p.proname, pg_get_userbyid(p.proowner) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'reown_s'
  UNION ALL SELECT 'type', t.typname, pg_get_userbyid(t.typowner) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = 'reown_s'
) o ORDER BY 1;
`

// reownCleanup removes a persisted fixture, including one a crashed run left
// behind. The schemas go first because reown_other is owned by the test's
// own connection role, which DROP OWNED would not touch.
const reownCleanup = `DROP SCHEMA IF EXISTS reown_s, reown_other, reown_super_s CASCADE;
DO $reown_cleanup$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['reown_from_test', 'reown_to_test', 'reown_super_test'] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = r) THEN
      EXECUTE format('DROP OWNED BY %I', r);
      EXECUTE format('DROP ROLE %I', r);
    END IF;
  END LOOP;
END
$reown_cleanup$;
`

// reownScriptFixture adds what the script-level checks need: a schema the
// migration role does not own but holds a table in (the schema pre-check),
// and a superuser migration role with its own objects (the skip path).
const reownScriptFixture = reownFixture + `CREATE SCHEMA reown_other;
GRANT USAGE, CREATE ON SCHEMA reown_other TO reown_from_test;
SET ROLE reown_from_test;
CREATE TABLE reown_other.t (id int);
RESET ROLE;
CREATE ROLE reown_super_test SUPERUSER;
SET ROLE reown_super_test;
CREATE SCHEMA reown_super_s;
CREATE TABLE reown_super_s.t (id int);
CREATE TABLE reown_other.t2 (id int);
RESET ROLE;
`

func shippedReownCTE(t *testing.T) string {
	t.Helper()
	_, after, found := strings.Cut(reownScript(), "<<'REOWN_CTE'\n")
	cte, _, terminated := strings.Cut(after, "\nREOWN_CTE\n")
	if !found || !terminated || !strings.Contains(cte, "candidates AS") {
		t.Fatal("shipped candidate CTE missing")
	}
	return cte
}

// reownPsql runs sql in one psql session with the owner variable bound the
// way the script binds it.
func reownPsql(t *testing.T, uri, sql string) string {
	t.Helper()
	cmd := exec.Command("psql", uri, "-XAtq", "-v", "ON_ERROR_STOP=1", "-v", "owner="+reownTo, "-f", "-")
	cmd.Stdin = strings.NewReader(sql)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("psql: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

// TestReownCandidateQueries runs the shipped candidate CTE and the shipped
// script against a live server. The SQL-level checks roll back; the
// script-level ones need a persisted fixture because the script opens one
// session per query, and clean it up afterwards.
func TestReownCandidateQueries(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Fatal("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for ownership handover SQL regressions")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	reownPsql(t, uri, reownCleanup)
	cte := shippedReownCTE(t)
	asFrom := func(sql string) string { return "SET ROLE reown_from_test;\n" + sql + "\nRESET ROLE;\n" }

	out := reownPsql(t, uri, "BEGIN;\n"+reownFixture+
		asFrom(cte+"\nSELECT sort || ' ' || stmt FROM candidates ORDER BY sort, stmt;")+"ROLLBACK;")
	t.Logf("candidates:\n%s", out)
	lines := strings.Split(out, "\n")
	want := func(t *testing.T, stmts ...string) {
		t.Helper()
		for _, s := range stmts {
			if !slices.Contains(lines, s) {
				t.Errorf("missing %q", s)
			}
		}
	}
	// absent fails on any candidate line of the given sort ("" for all) that
	// carries one of subs.
	absent := func(t *testing.T, sort string, subs ...string) {
		t.Helper()
		for _, line := range lines {
			for _, s := range subs {
				if strings.HasPrefix(line, sort) && strings.Contains(line, s) {
					t.Errorf("must not be a candidate: %q", line)
				}
			}
		}
	}
	t.Run("partitions and their parent each get a statement", func(t *testing.T) {
		want(t, "2 ALTER TABLE reown_s.part OWNER TO reown_to_test",
			"2 ALTER TABLE reown_s.part_1 OWNER TO reown_to_test",
			"2 ALTER TABLE reown_s.part_2 OWNER TO reown_to_test")
	})
	t.Run("schema statements sort first", func(t *testing.T) {
		if lines[0] != "1 ALTER SCHEMA reown_s OWNER TO reown_to_test" {
			t.Fatalf("first statement = %q", lines[0])
		}
		for _, line := range lines[1:] {
			if strings.HasPrefix(line, "1 ") {
				t.Fatalf("second schema statement %q", line)
			}
		}
	})
	t.Run("table-owned sequences follow their table", func(t *testing.T) {
		want(t, "2 ALTER SEQUENCE reown_s.standalone_seq OWNER TO reown_to_test")
		absent(t, "", "ident_id_seq", "bigs_id_seq", "ext_table_id_seq")
	})
	t.Run("extension members are excluded", func(t *testing.T) {
		absent(t, "", "citext", "ext_table", "tablefunc", "crosstab")
	})
	t.Run("types: composite, enum, domain and range only", func(t *testing.T) {
		want(t, "4 ALTER TYPE reown_s.ctype OWNER TO reown_to_test",
			"4 ALTER TYPE reown_s.mood OWNER TO reown_to_test",
			"4 ALTER DOMAIN reown_s.posint OWNER TO reown_to_test",
			"4 ALTER TYPE reown_s.myrange OWNER TO reown_to_test")
		absent(t, "4 ", "[]", "reown_s.part", "reown_s.bigs", "reown_s.ident",
			"reown_s.v ", "reown_s.mv", "reown_s.standalone_seq")
		if reownPsql(t, uri, "SHOW server_version_num;") >= "170000" {
			absent(t, "4 ", "mymultirange")
		} else {
			want(t, "4 ALTER TYPE reown_s.mymultirange OWNER TO reown_to_test")
		}
	})
	t.Run("routine shapes", func(t *testing.T) {
		want(t, "3 ALTER ROUTINE reown_s.f(integer) OWNER TO reown_to_test",
			"3 ALTER ROUTINE reown_s.f0() OWNER TO reown_to_test",
			"3 ALTER ROUTINE reown_s.fv(integer[]) OWNER TO reown_to_test",
			"3 ALTER ROUTINE reown_s.proc(integer) OWNER TO reown_to_test",
			"3 ALTER ROUTINE reown_s.sum2(integer) OWNER TO reown_to_test",
			"3 ALTER ROUTINE reown_s.cnt() OWNER TO reown_to_test",
			"3 ALTER ROUTINE reown_s.osa(double precision,anyelement) OWNER TO reown_to_test")
	})
	t.Run("handover executes, census agrees, second pass is empty", func(t *testing.T) {
		out := reownPsql(t, uri, "BEGIN;\n"+reownFixture+
			asFrom(cte+"\nSELECT stmt FROM candidates ORDER BY sort, stmt \\gexec")+
			"\\echo == census\n"+reownCensus+"\\echo == remaining\n"+
			asFrom(cte+"\nSELECT count(*) FROM candidates;")+"ROLLBACK;")
		_, rest, found := strings.Cut(out, "== census\n")
		census, remaining, split := strings.Cut(rest, "\n== remaining\n")
		if !found || !split {
			t.Fatalf("unexpected output:\n%s", out)
		}
		t.Logf("census:\n%s", census)
		if remaining != "0" {
			t.Fatalf("second enumeration = %q, want 0", remaining)
		}
		wantOwner := map[string]string{
			"schema|reown_s":     reownTo,
			"relation|ext_table": reownFrom, "relation|ext_table_id_seq": reownFrom,
			"routine|citextin": reownFrom, "type|citext": reownFrom, "type|ext_table": reownFrom,
			"type|tablefunc_crosstab_2": reownFrom,
		}
		for _, n := range []string{"part", "part_1", "part_2", "ident", "ident_id_seq", "bigs", "bigs_id_seq", "standalone_seq", "v", "mv"} {
			wantOwner["relation|"+n] = reownTo
		}
		for _, n := range []string{"f", "f0", "fv", "proc", "sum2", "cnt", "osa"} {
			wantOwner["routine|"+n] = reownTo
		}
		for _, n := range []string{"ctype", "_ctype", "mood", "_mood", "posint", "_posint", "myrange", "_myrange",
			"mymultirange", "_mymultirange", "part", "bigs", "ident", "v", "mv"} {
			wantOwner["type|"+n] = reownTo
		}
		seen := map[string]bool{}
		for row := range strings.SplitSeq(census, "\n") {
			parts := strings.Split(row, "|")
			if len(parts) != 3 {
				t.Fatalf("census row %q", row)
			}
			key := parts[0] + "|" + parts[1]
			if w, ok := wantOwner[key]; ok {
				seen[key] = true
				if parts[2] != w {
					t.Errorf("%s owned by %s, want %s", key, parts[2], w)
				}
			}
		}
		for key := range wantOwner {
			if !seen[key] {
				t.Errorf("census lacks %s", key)
			}
		}
	})
	t.Run("script", func(t *testing.T) { testReownScript(t, uri) })
}

// testReownScript runs the shipped script against a persisted fixture, one
// privilege state at a time, and reads the resulting owners back.
func testReownScript(t *testing.T, uri string) {
	reownPsql(t, uri, reownScriptFixture)
	t.Cleanup(func() { reownPsql(t, uri, reownCleanup) })
	run := func(t *testing.T, role, owner string) (string, int) {
		t.Helper()
		cmd := exec.Command(shellPath, "-c", reownScript())
		cmd.Env = append(os.Environ(), "PGCOPYDB_TARGET_PGURI="+uri, reownOwnerEnv+"="+owner,
			"PGOPTIONS=-c role="+role+" "+reownStatementBound)
		out, err := cmd.CombinedOutput()
		code := 0
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			code = exitErr.ExitCode()
		} else if err != nil {
			t.Fatalf("running the handover script: %v\n%s", err, out)
		}
		t.Logf("role=%s owner=%s EXIT=%d\n%s", role, owner, code, out)
		return string(out), code
	}
	schemaOwner := func(t *testing.T, name string) string {
		t.Helper()
		return reownPsql(t, uri, "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = '"+name+"';")
	}
	tableOwner := func(t *testing.T, name string) string {
		t.Helper()
		return reownPsql(t, uri, "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = '"+name+"'::regclass;")
	}
	dbGrant := reownPsql(t, uri, "SELECT format('GRANT CREATE ON DATABASE %I TO reown_from_test', current_database());")
	setDBCreate := func(t *testing.T, verb, prep string) {
		t.Helper()
		reownPsql(t, uri, "SELECT format('"+verb+" CREATE ON DATABASE %I "+prep+" reown_from_test', current_database()) \\gexec")
	}

	t.Run("same role short-circuits", func(t *testing.T) {
		out, code := run(t, reownFrom, reownFrom)
		if code != 0 || !strings.Contains(out, "ok: ownership already held by the migration role") {
			t.Fatal("same-role request must succeed without a handover")
		}
	})
	t.Run("missing role fails closed", func(t *testing.T) {
		out, code := run(t, reownFrom, "reown_missing_test")
		if code != 1 || !strings.Contains(out, `role "reown_missing_test" does not exist`) {
			t.Fatal("a missing role must name itself and fail")
		}
	})
	t.Run("database CREATE pre-check fires for the migration role", func(t *testing.T) {
		setDBCreate(t, "REVOKE", "FROM")
		out, code := run(t, reownFrom, reownTo)
		if code != 1 || !strings.Contains(out, dbGrant) || strings.Contains(out, "GRANT CREATE ON SCHEMA") {
			t.Fatalf("want exit 1 naming %q and no schema grant", dbGrant)
		}
		if got := schemaOwner(t, "reown_s"); got != reownFrom {
			t.Fatalf("a failed pre-check must not hand anything over, reown_s owned by %s", got)
		}
	})
	t.Run("schema CREATE pre-check fires for untransferred schemas only", func(t *testing.T) {
		setDBCreate(t, "GRANT", "TO")
		reownPsql(t, uri, "REVOKE CREATE ON SCHEMA reown_other FROM reown_to_test;")
		out, code := run(t, reownFrom, reownTo)
		if code != 1 || !strings.Contains(out, "GRANT CREATE ON SCHEMA reown_other TO reown_to_test") {
			t.Fatal("want exit 1 naming the reown_other grant")
		}
		if strings.Contains(out, "reown_s TO") || strings.Contains(out, dbGrant) {
			t.Fatal("a transferred schema and a granted database must stay silent")
		}
		if got := tableOwner(t, "reown_other.t"); got != reownFrom {
			t.Fatalf("a failed pre-check must not hand anything over, reown_other.t owned by %s", got)
		}
	})
	t.Run("granted pre-checks are silent and the handover lands", func(t *testing.T) {
		reownPsql(t, uri, "GRANT CREATE ON SCHEMA reown_other TO reown_to_test;")
		out, code := run(t, reownFrom, reownTo)
		if code != 0 || !strings.Contains(out, reownHandedOK) || strings.Contains(out, "GRANT CREATE") {
			t.Fatal("want a clean handover")
		}
		for name, got := range map[string]string{
			"reown_s": schemaOwner(t, "reown_s"), "reown_s.part_2": tableOwner(t, "reown_s.part_2"),
			"reown_other.t": tableOwner(t, "reown_other.t"),
		} {
			if got != reownTo {
				t.Errorf("%s owned by %s after the handover", name, got)
			}
		}
		if got := schemaOwner(t, "reown_other"); got == reownTo {
			t.Error("a schema the migration role does not own must keep its owner")
		}
	})
	t.Run("rerun is a no-op", func(t *testing.T) {
		out, code := run(t, reownFrom, reownTo)
		if code != 0 || !strings.Contains(out, reownHandedOK) || strings.Contains(out, "ALTER ") {
			t.Fatal("a second run must succeed with nothing to do")
		}
	})
	t.Run("superuser skips the pre-checks", func(t *testing.T) {
		reownPsql(t, uri, "REVOKE CREATE ON SCHEMA reown_other FROM reown_to_test;")
		out, code := run(t, reownSuper, reownTo)
		if code != 0 || !strings.Contains(out, reownHandedOK) || strings.Contains(out, "GRANT CREATE") {
			t.Fatal("a superuser migration role must hand over without grants")
		}
		for _, name := range []string{"reown_super_s.t", "reown_other.t2"} {
			if got := tableOwner(t, name); got != reownTo {
				t.Errorf("%s owned by %s after the superuser handover", name, got)
			}
		}
	})
}

// TestReownInheritPreCheck confirms the handover pre-checks the migration
// role's inherited privileges before any ALTER runs. SET ROLE alone lets a
// NOINHERIT membership pass preflight's probe, but ALTER SCHEMA OWNER needs
// the privileges of the new owner, not merely the ability to become it, so
// this must fire only when a schema is actually being transferred.
func TestReownInheritPreCheck(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Fatal("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for ownership handover SQL regressions")
	}
	const from, to = "reown_inh_from_test", "reown_inh_to_test"
	cleanup := `DROP SCHEMA IF EXISTS reown_inh_s, reown_inh_shared CASCADE;
DO $reown_inh_cleanup$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['` + from + `', '` + to + `'] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = r) THEN
      EXECUTE format('DROP OWNED BY %I', r);
      EXECUTE format('DROP ROLE %I', r);
    END IF;
  END LOOP;
END
$reown_inh_cleanup$;
`
	setup := func(t *testing.T, noInherit, transferSchema bool) {
		t.Helper()
		reownPsql(t, uri, cleanup)
		t.Cleanup(func() { reownPsql(t, uri, cleanup) })
		inherit := "INHERIT"
		if noInherit {
			inherit = "NOINHERIT"
		}
		sql := "CREATE ROLE " + from + " " + inherit + ";\n" +
			"CREATE ROLE " + to + ";\n" +
			"GRANT " + to + " TO " + from + ";\n" +
			"SELECT format('GRANT CREATE ON DATABASE %I TO " + from + "', current_database()) \\gexec\n" +
			"CREATE SCHEMA reown_inh_shared;\n" +
			"GRANT USAGE, CREATE ON SCHEMA reown_inh_shared TO " + from + ", " + to + ";\n" +
			"SET ROLE " + from + ";\n" +
			"CREATE TABLE reown_inh_shared.t (id int);\n" +
			"RESET ROLE;\n"
		if transferSchema {
			sql += "SET ROLE " + from + ";\n" +
				"CREATE SCHEMA reown_inh_s;\n" +
				"CREATE TABLE reown_inh_s.t (id int);\n" +
				"RESET ROLE;\n"
		}
		reownPsql(t, uri, sql)
	}
	run := func(t *testing.T) (string, int) {
		t.Helper()
		cmd := exec.Command(shellPath, "-c", reownScript())
		cmd.Env = append(os.Environ(), "PGCOPYDB_TARGET_PGURI="+uri, reownOwnerEnv+"="+to,
			"PGOPTIONS=-c role="+from+" "+reownStatementBound)
		out, err := cmd.CombinedOutput()
		code := 0
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			code = exitErr.ExitCode()
		} else if err != nil {
			t.Fatalf("running the handover script: %v\n%s", err, out)
		}
		t.Logf("EXIT=%d\n%s", code, out)
		return string(out), code
	}
	// The remediation the pre-check prints changed shape in PostgreSQL 16:
	// GRANT ... WITH INHERIT TRUE only parses from 16 on, below that
	// inheritance is a role attribute and ALTER ROLE ... INHERIT is what
	// actually fixes the handover (both confirmed live).
	wantRemediation := "GRANT " + to + " TO " + from + " WITH INHERIT TRUE"
	if reownPsql(t, uri, "SHOW server_version_num;") < "160000" {
		wantRemediation = "ALTER ROLE " + from + " INHERIT"
	}

	t.Run("NOINHERIT membership with a transferred schema fires the pre-check", func(t *testing.T) {
		setup(t, true, true)
		out, code := run(t)
		if code != 1 || !strings.Contains(out, wantRemediation) {
			t.Fatalf("want exit 1 naming %q, got exit %d:\n%s", wantRemediation, code, out)
		}
		if got := reownPsql(t, uri, "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'reown_inh_s';"); got != from {
			t.Fatalf("a failed pre-check must not hand anything over, reown_inh_s owned by %s", got)
		}
		reownPsql(t, uri, wantRemediation+";")
		out, code = run(t)
		if code != 0 || !strings.Contains(out, reownHandedOK) {
			t.Fatalf("applying the printed remediation must let the handover succeed, got exit %d:\n%s", code, out)
		}
	})
	t.Run("inheriting membership with a transferred schema stays silent", func(t *testing.T) {
		setup(t, false, true)
		out, code := run(t)
		if code != 0 || strings.Contains(out, "WITH INHERIT") {
			t.Fatalf("want a clean handover, got exit %d:\n%s", code, out)
		}
	})
	t.Run("NOINHERIT membership with no schema transfer stays silent", func(t *testing.T) {
		setup(t, true, false)
		out, code := run(t)
		if code != 0 || strings.Contains(out, "WITH INHERIT") {
			t.Fatalf("want a clean handover, got exit %d:\n%s", code, out)
		}
	})
	t.Run("inheriting membership with no schema transfer stays silent", func(t *testing.T) {
		setup(t, false, false)
		out, code := run(t)
		if code != 0 || strings.Contains(out, "WITH INHERIT") {
			t.Fatalf("want a clean handover, got exit %d:\n%s", code, out)
		}
	})
}
