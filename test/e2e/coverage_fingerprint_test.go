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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// coverageReaderRole receives the grants and policies of the coverage cases.
const coverageReaderRole = "cov_reader"

// ensureCoverageReaderSQL creates coverageReaderRole unless it exists.
const ensureCoverageReaderSQL = "DO $role$ BEGIN CREATE ROLE " + coverageReaderRole +
	" NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $role$"

// psqlScript feeds one script to a psql session on stdin, so every statement
// commits on its own, and returns the trimmed stdout.
type psqlScript func(ctx context.Context, script string) (string, error)

// localPSQL reaches uri with the local psql, for the cluster-free tests.
func localPSQL(psqlPath, uri string) psqlScript {
	return func(ctx context.Context, script string) (string, error) {
		return runPSQLScript(exec.CommandContext(ctx, psqlPath, uri, "-XqtA", "-v", "ON_ERROR_STOP=1"), script)
	}
}

func runPSQLScript(cmd *exec.Cmd, script string) (string, error) {
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.Output()
	if ee, ok := errors.AsType[*exec.ExitError](err); ok {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(string(ee.Stderr)))
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// fingerprintRow is one row of fingerprint.sql.
type fingerprintRow struct {
	Schema string `json:"schema"`
	Aspect string `json:"aspect"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

// readFingerprint runs fingerprint.sql over schemas.
func readFingerprint(ctx context.Context, run psqlScript, schemas []string) ([]fingerprintRow, error) {
	query, err := coverageFS.ReadFile(coverageFingerprint)
	if err != nil {
		return nil, err
	}
	out, err := run(ctx, strings.ReplaceAll(string(query), "${schemas}", sqlTextArray(schemas)))
	if err != nil {
		return nil, fmt.Errorf("fingerprint: %w", err)
	}
	var rows []fingerprintRow
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		return nil, fmt.Errorf("fingerprint output is not JSON: %w", err)
	}
	return rows, nil
}

func sqlTextArray(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = sqlLiteral(v)
	}
	return "ARRAY[" + strings.Join(quoted, ", ") + "]::text[]"
}

type fingerprintKey struct{ aspect, key string }

// caseFingerprint keeps the rows of the schemas in names and writes each name
// as its placeholder, so two schemas on one server compare like one on two.
// Keys lead with the placeholder: most fingerprint.sql keys omit the schema.
func caseFingerprint(rows []fingerprintRow, names map[string]string) map[fingerprintKey]string {
	// Longest first, so no name replaces part of a longer one.
	order := slices.SortedFunc(maps.Keys(names), func(a, b string) int { return cmp.Compare(len(b), len(a)) })
	pairs := make([]string, 0, 2*len(order))
	for _, name := range order {
		pairs = append(pairs, name, names[name])
	}
	normalize := strings.NewReplacer(pairs...)
	out := map[fingerprintKey]string{}
	for _, row := range rows {
		if placeholder, ok := names[row.Schema]; ok {
			out[fingerprintKey{row.Aspect, placeholder + " " + normalize.Replace(row.Key)}] = normalize.Replace(row.Value)
		}
	}
	return out
}

// fingerprintDiff is one aspect of one object that differs.
type fingerprintDiff struct{ aspect, key, source, target string }

// fingerprintAbsent stands in for the value of a side that lacks the object.
const fingerprintAbsent = "(absent)"

func (d fingerprintDiff) String() string {
	return fmt.Sprintf("%s %s:\n  source: %s\n  target: %s", d.aspect, d.key, d.source, d.target)
}

// diffFingerprint lists every differing key, sorted, ignoring skipped aspects.
func diffFingerprint(source, target map[fingerprintKey]string, skip map[string]bool) []fingerprintDiff {
	keys := map[fingerprintKey]bool{}
	for k := range source {
		keys[k] = true
	}
	for k := range target {
		keys[k] = true
	}
	var diffs []fingerprintDiff
	for k := range keys {
		s, inSource := source[k]
		t, inTarget := target[k]
		if skip[k.aspect] || (inSource && inTarget && s == t) {
			continue
		}
		if !inSource {
			s = fingerprintAbsent
		}
		if !inTarget {
			t = fingerprintAbsent
		}
		diffs = append(diffs, fingerprintDiff{aspect: k.aspect, key: k.key, source: s, target: t})
	}
	slices.SortFunc(diffs, func(a, b fingerprintDiff) int {
		return cmp.Or(cmp.Compare(a.aspect, b.aspect), cmp.Compare(a.key, b.key))
	})
	return diffs
}

// leakedObjectsSQL lists the relations, functions, types and schemas created
// or altered outside schemas since transaction since, so a case cannot touch
// what it does not own. Toast and temporary schemas follow their tables.
func leakedObjectsSQL(schemas []string, since string) string {
	return "SELECT coalesce(string_agg(o, ', ' ORDER BY o), '') FROM (" +
		" SELECT 'relation ' || c.oid::regclass AS o, c.xmin, n.nspname FROM pg_class c" +
		"  JOIN pg_namespace n ON n.oid = c.relnamespace" +
		" UNION ALL SELECT 'function ' || p.oid::regprocedure, p.xmin, n.nspname FROM pg_proc p" +
		"  JOIN pg_namespace n ON n.oid = p.pronamespace" +
		" UNION ALL SELECT 'type ' || t.oid::regtype, t.xmin, n.nspname FROM pg_type t" +
		"  JOIN pg_namespace n ON n.oid = t.typnamespace" +
		" UNION ALL SELECT 'schema ' || n.nspname, n.xmin, n.nspname FROM pg_namespace n" +
		") x WHERE age(x.xmin) < txid_current() - " + since +
		" AND x.nspname <> ALL (" + sqlTextArray(schemas) + ")" +
		` AND x.nspname <> 'pg_toast' AND x.nspname NOT LIKE 'pg\_temp\_%' AND x.nspname NOT LIKE 'pg\_toast\_temp\_%'`
}

// applyCoverageCase runs one case section through run, prefixed with prefix
// (a SET ROLE), and fails when it created or altered anything outside schemas.
func applyCoverageCase(
	ctx context.Context, run psqlScript, c coverageCase, section, prefix string, schemas []string,
) error {
	since, err := run(ctx, "SELECT txid_current()")
	if err != nil {
		return fmt.Errorf("%s: %w", c.path, err)
	}
	if _, err := strconv.ParseInt(since, 10, 64); err != nil {
		return fmt.Errorf("%s: transaction id %q: %w", c.path, since, err)
	}
	if _, err := run(ctx, prefix+c.render(section, schemas)); err != nil {
		return fmt.Errorf("%s: %w", c.path, err)
	}
	leaked, err := run(ctx, leakedObjectsSQL(schemas, since))
	if err != nil {
		return fmt.Errorf("%s: leak check: %w", c.path, err)
	}
	if leaked != "" {
		return fmt.Errorf("%s: touched objects outside its schemas: %s", c.path, leaked)
	}
	return nil
}

// unlinkCoverageLargeObjectsSQL unlinks the large objects a case schema lists;
// DROP SCHEMA leaves them behind.
func unlinkCoverageLargeObjectsSQL(schema string) string {
	return "DO $lo$ BEGIN IF to_regclass(" + sqlLiteral(sqlIdent(schema)+".cov_large_objects") + ") IS NOT NULL THEN" +
		" PERFORM lo_unlink(r.lo) FROM " + sqlIdent(schema) + ".cov_large_objects r" +
		" JOIN pg_largeobject_metadata m ON m.oid = r.lo; END IF; END $lo$"
}

// testPSQL is the cluster-free tests' server, or a skip without one.
func testPSQL(t *testing.T) psqlScript {
	t.Helper()
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("CI supplies PGCOPYDB_TEST_PGURI for the coverage fingerprint and case SQL")
	}
	psqlPath, err := exec.LookPath("psql")
	if err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	return localPSQL(psqlPath, uri)
}

// testQuery runs one script on the test server and fails the test on error.
func testQuery(t *testing.T, run psqlScript, script string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := run(ctx, script)
	if err != nil {
		t.Fatalf("%v\nscript:\n%s", err, script)
	}
	return out
}

// testCaseOwner stands in for the app role: a case runs as it, not as a superuser.
const testCaseOwner = "cov_case_owner"

// testCoverageSchemas creates schemas owned by testCaseOwner and drops them,
// with the large objects they list, when the test ends.
func testCoverageSchemas(t *testing.T, run psqlScript, schemas ...string) {
	t.Helper()
	ensureOwner := strings.ReplaceAll(ensureCoverageReaderSQL, coverageReaderRole, testCaseOwner)
	testQuery(t, run, ensureCoverageReaderSQL+";\n"+ensureOwner)
	for _, schema := range schemas {
		t.Cleanup(func() {
			testQuery(t, run, unlinkCoverageLargeObjectsSQL(schema)+";\nDROP SCHEMA IF EXISTS "+sqlIdent(schema)+" CASCADE")
		})
		testQuery(t, run, "CREATE SCHEMA "+sqlIdent(schema)+" AUTHORIZATION "+testCaseOwner)
	}
}

// fingerprintFixture has at least one object for every aspect.
const fingerprintFixture = `
CREATE TYPE ${schema}.mood AS ENUM ('sad', 'ok');
ALTER TYPE ${schema}.mood ADD VALUE 'happy';
CREATE DOMAIN ${schema}.posint AS integer CHECK (VALUE > 0);
CREATE TYPE ${schema}.pair AS (a integer, b text);
CREATE SEQUENCE ${schema}.counter CYCLE MAXVALUE 5;
SELECT nextval('${schema}.counter');
CREATE TABLE ${schema}.items (
  id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  name text COLLATE "C" NOT NULL CHECK (name <> ''),
  feeling ${schema}.mood DEFAULT 'happy',
  qty ${schema}.posint,
  twice integer GENERATED ALWAYS AS (qty * 2) STORED
);
INSERT INTO ${schema}.items (name, feeling, qty) VALUES ('a', 'happy', 1), ('b', 'sad', 2);
CREATE INDEX items_name ON ${schema}.items (name);
CREATE STATISTICS ${schema}.items_stats (dependencies) ON id, qty FROM ${schema}.items;
CREATE FUNCTION ${schema}.touch() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN RETURN NEW; END $f$;
CREATE TRIGGER items_touch BEFORE UPDATE ON ${schema}.items FOR EACH ROW EXECUTE FUNCTION ${schema}.touch();
CREATE VIEW ${schema}.item_names AS SELECT name FROM ${schema}.items;
ALTER TABLE ${schema}.items ENABLE ROW LEVEL SECURITY;
CREATE POLICY items_read ON ${schema}.items FOR SELECT TO cov_reader USING (qty > 0);
COMMENT ON TABLE ${schema}.items IS 'items';
COMMENT ON TRIGGER items_touch ON ${schema}.items IS 'trigger';
COMMENT ON POLICY items_read ON ${schema}.items IS 'policy';
COMMENT ON RULE "_RETURN" ON ${schema}.item_names IS 'rule';
GRANT SELECT ON ${schema}.items TO cov_reader;
CREATE TABLE ${schema}.parts (id integer NOT NULL, region integer NOT NULL) PARTITION BY LIST (region);
CREATE TABLE ${schema}.parts_one PARTITION OF ${schema}.parts FOR VALUES IN (1);
INSERT INTO ${schema}.parts VALUES (1, 1);
CREATE TABLE ${schema}.cov_large_objects (name text PRIMARY KEY, lo oid NOT NULL);
INSERT INTO ${schema}.cov_large_objects SELECT 'blob', lo_from_bytea(0, 'hello');
`

// fingerprintMutations change one aspect of fingerprintFixture each; their keys
// must be exactly the aspects fingerprint.sql reports.
var fingerprintMutations = map[string]string{
	"acl":        "REVOKE SELECT ON ${schema}.items FROM cov_reader",
	"column":     "ALTER TABLE ${schema}.items ALTER COLUMN feeling SET DEFAULT 'ok'",
	"comment":    "COMMENT ON TABLE ${schema}.items IS 'changed'",
	"constraint": "ALTER TABLE ${schema}.items DROP CONSTRAINT items_name_check",
	"data":       "UPDATE ${schema}.items SET name = 'c' WHERE name = 'b'",
	"function": "CREATE OR REPLACE FUNCTION ${schema}.touch() RETURNS trigger LANGUAGE plpgsql" +
		" AS $f$ BEGIN RETURN OLD; END $f$",
	"index":       "DROP INDEX ${schema}.items_name",
	"largeobject": "SELECT lo_put(lo, 0, 'J') FROM ${schema}.cov_large_objects",
	"owner":       "ALTER TABLE ${schema}.parts_one OWNER TO cov_reader",
	"policy":      "ALTER POLICY items_read ON ${schema}.items USING (qty > 1)",
	"relation":    "ALTER TABLE ${schema}.items FORCE ROW LEVEL SECURITY",
	"sequence":    "SELECT nextval('${schema}.counter')",
	"statistics":  "ALTER STATISTICS ${schema}.items_stats SET STATISTICS 7",
	"trigger":     "ALTER TABLE ${schema}.items DISABLE TRIGGER items_touch",
	"type":        "ALTER TYPE ${schema}.mood ADD VALUE 'meh'",
	"view":        "CREATE OR REPLACE VIEW ${schema}.item_names AS SELECT name FROM ${schema}.items WHERE id > 0",
}

// testFingerprint reads one schema's fingerprint with the schema written as
// ${schema}. One server cannot hold one large object OID twice, so the OID
// column of cov_large_objects is left out; the largeobject aspect covers it.
func testFingerprint(t *testing.T, run psqlScript, schema string) map[fingerprintKey]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := readFingerprint(ctx, run, []string{schema})
	if err != nil {
		t.Fatal(err)
	}
	fp := caseFingerprint(rows, map[string]string{schema: coverageSchema})
	delete(fp, fingerprintKey{"data", coverageSchema + " cov_large_objects"})
	return fp
}

func TestCoverageFingerprintDetectsEveryAspect(t *testing.T) {
	run := testPSQL(t)
	identity := strconv.FormatInt(time.Now().UnixNano(), 10)
	fixture := func(schema string) map[fingerprintKey]string {
		testCoverageSchemas(t, run, schema)
		testQuery(t, run, "SET ROLE "+testCaseOwner+";\n"+strings.ReplaceAll(fingerprintFixture, coverageSchema, schema))
		return testFingerprint(t, run, schema)
	}
	base := fixture("cov_fp_base_" + identity)
	reported := map[string]bool{}
	for k := range base {
		reported[k.aspect] = true
	}
	// Both directions: a new aspect needs a mutation, and a mutation needs rows to change.
	aspects, mutated := slices.Sorted(maps.Keys(reported)), slices.Sorted(maps.Keys(fingerprintMutations))
	if !slices.Equal(aspects, mutated) {
		t.Fatalf("fixture reports aspects %v, mutations cover %v", aspects, mutated)
	}
	// The owner's own entries are left out: PostgreSQL 17 added MAINTAIN to them.
	itemsACL := base[fingerprintKey{"acl", coverageSchema + " relation items"}]
	if got, want := itemsACL, testCaseOwner+">cov_reader:SELECT"; got != want {
		t.Fatalf("items acl = %q, want %q", got, want)
	}
	// pg_identify_object gives these no schema; fingerprint.sql takes their table's.
	for key, want := range map[string]string{
		"${schema} trigger items_touch on ${schema}.items": "trigger",
		"${schema} policy items_read on ${schema}.items":   "policy",
		`${schema} rule "_RETURN" on ${schema}.item_names`: "rule",
	} {
		if got := base[fingerprintKey{"comment", key}]; got != want {
			t.Fatalf("comment %s = %q, want %q", key, got, want)
		}
	}
	if diffs := diffFingerprint(base, fixture("cov_fp_same_"+identity), nil); len(diffs) > 0 {
		t.Fatalf("identical schemas differ: %v", diffs)
	}
	for aspect, mutation := range fingerprintMutations {
		t.Run(aspect, func(t *testing.T) {
			schema := "cov_fp_" + aspect + "_" + identity
			fixture(schema)
			// As the superuser: handing a table to cov_reader needs membership the owner lacks.
			testQuery(t, run, strings.ReplaceAll(mutation, coverageSchema, schema))
			mutated := testFingerprint(t, run, schema)
			isAspect := func(d fingerprintDiff) bool { return d.aspect == aspect }
			if diffs := diffFingerprint(base, mutated, nil); !slices.ContainsFunc(diffs, isAspect) {
				t.Fatalf("mutation not reported as %s: %v", aspect, diffs)
			}
			if diffs := diffFingerprint(base, mutated, map[string]bool{aspect: true}); slices.ContainsFunc(diffs, isAspect) {
				t.Fatalf("skipped aspect %s still reported: %v", aspect, diffs)
			}
		})
	}
}

// replicaIdentityFixture uses a replica identity index that is not the key,
// the shape pgcopydb used to reset to the default.
const replicaIdentityFixture = `
CREATE TABLE ${schema}.t (id integer PRIMARY KEY, code text NOT NULL);
CREATE UNIQUE INDEX t_code_key ON ${schema}.t (code);
ALTER TABLE ${schema}.t REPLICA IDENTITY USING INDEX t_code_key;
`

// TestCoverageFingerprintNamesReplicaIdentityIndex proves the relation aspect
// alone sees a lost or moved replica identity index: a cross-major pair skips
// the index aspect.
func TestCoverageFingerprintNamesReplicaIdentityIndex(t *testing.T) {
	run := testPSQL(t)
	identity := strconv.FormatInt(time.Now().UnixNano(), 10)
	fixture := func(schema string) map[fingerprintKey]string {
		testCoverageSchemas(t, run, schema)
		testQuery(t, run, "SET ROLE "+testCaseOwner+";\n"+strings.ReplaceAll(replicaIdentityFixture, coverageSchema, schema))
		return testFingerprint(t, run, schema)
	}
	relation := fingerprintKey{"relation", coverageSchema + " t"}
	base := fixture("cov_ri_base_" + identity)
	if got := base[relation]; !strings.Contains(got, " identity=i:t_code_key ") {
		t.Fatalf("relation t = %q, want the replica identity index named", got)
	}
	for name, mutation := range map[string]string{
		"default":     "ALTER TABLE ${schema}.t REPLICA IDENTITY DEFAULT",
		"other index": "ALTER TABLE ${schema}.t REPLICA IDENTITY USING INDEX t_pkey",
	} {
		t.Run(name, func(t *testing.T) {
			schema := "cov_ri_" + strings.ReplaceAll(name, " ", "_") + "_" + identity
			fixture(schema)
			testQuery(t, run, strings.ReplaceAll(mutation, coverageSchema, schema))
			diffs := diffFingerprint(base, testFingerprint(t, run, schema), deparsedAspects)
			if len(diffs) != 1 || diffs[0].aspect != relation.aspect || diffs[0].key != relation.key {
				t.Fatalf("diffs without the deparsed aspects = %v, want one on relation t", diffs)
			}
		})
	}
}

func TestDiffFingerprintReportsAbsentSides(t *testing.T) {
	source := map[fingerprintKey]string{{"a", "same"}: "1", {"b", "gone"}: "2"}
	target := map[fingerprintKey]string{{"a", "same"}: "1", {"c", "new"}: "3"}
	got := diffFingerprint(source, target, nil)
	want := []fingerprintDiff{
		{aspect: "b", key: "gone", source: "2", target: fingerprintAbsent},
		{aspect: "c", key: "new", source: fingerprintAbsent, target: "3"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("diff = %v, want %v", got, want)
	}
}

func TestCaseFingerprintNormalizesSchemaNames(t *testing.T) {
	const one, two = "cov_x_1", "cov2_x_1"
	rows := []fingerprintRow{
		{Schema: one, Aspect: "a", Key: "leaf", Value: "parents=cov_x_1.parent"},
		{Schema: two, Aspect: "a", Key: "cov2_x_1.leaf2", Value: "parents=cov_x_1.parent"},
		// Most keys omit the schema; neither schema's row may hide the other's.
		{Schema: two, Aspect: "a", Key: "t", Value: "2 rows"},
		{Schema: one, Aspect: "a", Key: "t", Value: "0 rows"},
		{Schema: "cov_y_1", Aspect: "a", Key: "other", Value: "x"},
	}
	got := caseFingerprint(rows, map[string]string{one: coverageSchema, two: coverageSchema2})
	want := map[fingerprintKey]string{
		{"a", "${schema} leaf"}:              "parents=${schema}.parent",
		{"a", "${schema2} ${schema2}.leaf2"}: "parents=${schema}.parent",
		{"a", "${schema2} t"}:                "2 rows",
		{"a", "${schema} t"}:                 "0 rows",
	}
	if !maps.Equal(got, want) {
		t.Fatalf("caseFingerprint = %v, want %v", got, want)
	}
}

// TestCoverageCasesApply runs every embedded case against a real server, so
// a syntax error or a stray schema reference fails CI instead of a release
// candidate, and the fingerprint is proven to read what each case creates.
func TestCoverageCasesApply(t *testing.T) {
	run := testPSQL(t)
	cases, err := loadCoverageCases(coverageFS, coverageOutcomes)
	if err != nil {
		t.Fatal(err)
	}
	major, err := strconv.Atoi(testQuery(t, run, "SELECT current_setting('server_version_num')::int / 10000"))
	if err != nil {
		t.Fatal(err)
	}
	identity := strconv.FormatInt(time.Now().UnixNano(), 10)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if major < c.minPG {
				t.Skipf("needs PostgreSQL %d, the server is %d", c.minPG, major)
			}
			schemas := c.schemaNames(identity)
			testCoverageSchemas(t, run, schemas...)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			prefix := "SET ROLE " + testCaseOwner + ";\n"
			if err := applyCoverageCase(ctx, run, c, c.setup, prefix, schemas); err != nil {
				t.Fatal(err)
			}
			if c.follow != "" {
				if err := applyCoverageCase(ctx, run, c, c.follow, prefix, schemas); err != nil {
					t.Fatal(err)
				}
			}
			rows, err := readFingerprint(ctx, run, schemas)
			if err != nil {
				t.Fatal(err)
			}
			// Every schema has owner and acl rows; anything else is what the case made.
			if !slices.ContainsFunc(rows, func(r fingerprintRow) bool { return r.Key != "schema" }) {
				t.Fatalf("fingerprint saw nothing the case created: %v", rows)
			}
		})
	}
}

func TestApplyCoverageCaseRejectsObjectsOutsideItsSchemas(t *testing.T) {
	run := testPSQL(t)
	identity := strconv.FormatInt(time.Now().UnixNano(), 10)
	c, err := parseCoverageCase("coverage/test/leak.sql",
		"-- coverage: group=clone expect=identical schemas=1 min_pg=14\n-- @setup\n"+
			"CREATE TABLE ${schema}.ok (id int);\nCREATE TABLE public.stray_"+identity+" (id int);\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	schemas := c.schemaNames(identity)
	testCoverageSchemas(t, run, schemas...)
	t.Cleanup(func() { testQuery(t, run, "DROP TABLE IF EXISTS public.stray_"+identity) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err = applyCoverageCase(ctx, run, c, c.setup, "", schemas)
	if err == nil || !strings.Contains(err.Error(), "relation stray_"+identity) {
		t.Fatalf("err = %v, want the leaked table named", err)
	}
	if strings.Contains(err.Error(), "relation "+schemas[0]) {
		t.Fatalf("err = %v names the case's own table", err)
	}
}

func TestUnlinkCoverageLargeObjects(t *testing.T) {
	run := testPSQL(t)
	schema := "cov_lo_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	testCoverageSchemas(t, run, schema)
	lo := testQuery(t, run, "CREATE TABLE "+schema+".cov_large_objects (name text, lo oid);\n"+
		"INSERT INTO "+schema+".cov_large_objects SELECT 'blob', lo_from_bytea(0, 'x') RETURNING lo")
	testQuery(t, run, unlinkCoverageLargeObjectsSQL(schema))
	if got := testQuery(t, run, "SELECT count(*) FROM pg_largeobject_metadata WHERE oid = "+lo); got != "0" {
		t.Fatalf("large object %s survived the unlink", lo)
	}
	// A schema without the table is a no-op, not an error.
	testQuery(t, run, unlinkCoverageLargeObjectsSQL("cov_lo_absent"))
}
