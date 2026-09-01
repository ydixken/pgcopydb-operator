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

// Package fixtures guards the wiring of the e2e seed fixtures. The seed only
// runs against a real cluster, so a stage that is misnamed, unreachable or in
// the wrong order costs a fixture bootstrap to discover. These are plain unit
// tests for the same reason test/buildconfig is: make test excludes test/e2e.
package fixtures

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const dir = "../e2e/fixtures"

// deferredIndexes are the non-unique secondary indexes that finish.sql builds
// after the load. Keeping them out of schema.sql is what stops the bulk
// inserts paying index maintenance per row (issue #146).
var deferredIndexes = []string{
	"orders_customer_idx",
	"app_users_tags_gin",
	"app_users_name_lower_idx",
	"events_customer_time_idx",
	"events_payload_gin",
	"readings_recent_partial_idx",
	"documents_title_lower_idx",
}

func read(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return string(b)
}

func sqlFiles(t *testing.T) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		t.Fatalf("globbing fixtures: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("no fixture SQL found; the suite would seed nothing")
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, filepath.Base(n))
	}
	return out
}

// stages returns the fixture names run.sh runs: bare stage calls, literal
// loops, and arrays that a loop expands.
func stages(t *testing.T, run string) []string {
	t.Helper()
	// A stage may carry an argument, which is how seed_extra receives its
	// shard index. The name is still literal in the file, which is all this
	// needs to prove the stage has a fixture and the fixture is reached.
	bare := regexp.MustCompile(`(?m)^\s*stage\s+([a-z_]+)(?:\s+"?\$?\w+"?)?\s*&?\s*$`).FindAllStringSubmatch(run, -1)
	names := make([]string, 0, len(bare))
	for _, m := range bare {
		names = append(names, m[1])
	}
	for _, m := range regexp.MustCompile(`for \w+ in ([a-z_ ]+);`).FindAllStringSubmatch(run, -1) {
		names = append(names, strings.Fields(m[1])...)
	}
	arrayLoops := regexp.MustCompile(`for\s+\w+\s+in\s+"\$\{(\w+)\[@\]\}";`)
	for _, loop := range arrayLoops.FindAllStringSubmatchIndex(run, -1) {
		array := run[loop[2]:loop[3]]
		assignments := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(array) +
			`\+?=\(([a-z_ ]*)\)\s*$`)
		for _, assignment := range assignments.FindAllStringSubmatch(run[:loop[0]], -1) {
			names = append(names, strings.Fields(assignment[1])...)
		}
	}
	if len(names) == 0 {
		t.Fatal("no stages parsed out of run.sh; the guards below would all pass vacuously")
	}
	return names
}

func TestStagesParsesAnIteratedArray(t *testing.T) {
	run := `stages=(seed_small seed_events)
stages+=(seed_documents)
for s in "${stages[@]}"; do
    stage "$s" &
done`
	want := []string{"seed_small", "seed_events", "seed_documents"}
	if got := stages(t, run); !slices.Equal(got, want) {
		t.Errorf("stages() = %v, want %v", got, want)
	}
}

func TestStagesIgnoresArrayAppendsAfterTheLoop(t *testing.T) {
	run := `stages=(seed_small)
for s in "${stages[@]}"; do
    stage "$s" &
done
stages+=(seed_late)`
	want := []string{"seed_small"}
	if got := stages(t, run); !slices.Equal(got, want) {
		t.Errorf("stages() = %v, want %v", got, want)
	}
}

func runFixtureScript(t *testing.T, tables, jobs string) string {
	t.Helper()
	tmp := t.TempDir()
	log := filepath.Join(tmp, "psql.log")
	psql := `#!/bin/sh
file=
shards=
shard=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -f)
            file=$2
            shift 2
            ;;
        -v)
            case "$2" in
                extra_shards=*) shards=${2#extra_shards=} ;;
                extra_shard=*) shard=${2#extra_shard=} ;;
            esac
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done
if [ -n "$file" ]; then
    printf '%s extra_shards=%s extra_shard=%s\n' "$file" "$shards" "$shard" >> "$PSQL_LOG"
    exit 0
fi
printf 'f\n'`
	if err := os.WriteFile(filepath.Join(tmp, "psql"), []byte(psql), 0o755); err != nil {
		t.Fatalf("writing fake psql: %v", err)
	}
	cmd := exec.Command("bash", filepath.Join(dir, "run.sh"))
	cmd.Env = append(os.Environ(),
		"PATH="+tmp+":"+os.Getenv("PATH"),
		"PSQL_LOG="+log,
		"SEED_SCALE=1",
		"SEED_PROFILE=test",
		"SEED_EXTRA_TABLES="+tables,
		"SEED_EXTRA_MB="+tables,
		"SEED_EXTRA_JOBS="+jobs,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run.sh failed: %v\n%s", err, out)
	}
	staged, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("reading psql log: %v", err)
	}
	return string(staged)
}

func TestRunShSkipsExtraConnectionsWithoutTables(t *testing.T) {
	staged := runFixtureScript(t, "0", "4")
	if strings.Contains(staged, "seed_extra.sql") {
		t.Errorf("run.sh opened extra seed connections without extra tables:\n%s", staged)
	}
}

func TestRunShStartsOneSessionPerExtraJob(t *testing.T) {
	staged := runFixtureScript(t, "1", "3")
	var got []string
	for line := range strings.SplitSeq(staged, "\n") {
		if strings.HasPrefix(line, "seed_extra.sql ") {
			got = append(got, line)
		}
	}
	slices.Sort(got)
	want := []string{
		"seed_extra.sql extra_shards=3 extra_shard=0",
		"seed_extra.sql extra_shards=3 extra_shard=1",
		"seed_extra.sql extra_shards=3 extra_shard=2",
	}
	if !slices.Equal(got, want) {
		t.Errorf("extra seed sessions = %v, want %v", got, want)
	}
}

// includes returns the fixtures a file pulls in with \ir.
func includes(body string) []string {
	found := regexp.MustCompile(`(?m)^\\ir\s+(\S+)`).FindAllStringSubmatch(body, -1)
	names := make([]string, 0, len(found))
	for _, m := range found {
		names = append(names, m[1])
	}
	return names
}

// TestEveryStageHasAFixture catches a stage name that run.sh spells wrong,
// which the Job would otherwise report only after a fixture bootstrap.
func TestEveryStageHasAFixture(t *testing.T) {
	for _, name := range stages(t, read(t, "run.sh")) {
		if _, err := os.Stat(filepath.Join(dir, name+".sql")); err != nil {
			t.Errorf("run.sh stages %q but %s.sql does not exist", name, name)
		}
	}
}

// TestEveryFixtureIsReachable fails when a fixture is orphaned: run.sh must
// stage it, or another fixture must include it.
func TestEveryFixtureIsReachable(t *testing.T) {
	staged := map[string]bool{}
	for _, n := range stages(t, read(t, "run.sh")) {
		staged[n+".sql"] = true
	}
	files := sqlFiles(t)
	included := map[string]bool{}
	for _, name := range files {
		for _, inc := range includes(read(t, name)) {
			included[inc] = true
		}
	}
	for _, name := range files {
		if !staged[name] && !included[name] {
			t.Errorf("fixture %s is never run: run.sh does not stage it and no fixture includes it", name)
		}
	}
}

// TestEveryIncludeExists fails when an \ir points at a fixture that is not
// there.
func TestEveryIncludeExists(t *testing.T) {
	for _, name := range sqlFiles(t) {
		for _, inc := range includes(read(t, name)) {
			if _, err := os.Stat(filepath.Join(dir, inc)); err != nil {
				t.Errorf("%s includes %s, which does not exist", name, inc)
			}
		}
	}
}

// TestSecondaryIndexesAreDeferred pins the split: schema.sql declares nothing
// but constraint-backed indexes, and every secondary index is built by
// finish.sql once the data is in.
func TestSecondaryIndexesAreDeferred(t *testing.T) {
	schema := read(t, "schema.sql")
	if strings.Contains(schema, "CREATE INDEX") {
		t.Error("schema.sql creates an index: secondary indexes belong in finish.sql, " +
			"or the bulk loads pay for maintaining them per row")
	}
	finish := read(t, "finish.sql")
	for _, idx := range deferredIndexes {
		if !strings.Contains(finish, idx) {
			t.Errorf("finish.sql never creates %s", idx)
		}
	}
}

// tableBlock returns the body of a table's CREATE TABLE in schema.sql, so a
// constraint check cannot be satisfied by some other table's column of the
// same name.
func tableBlock(t *testing.T, schema, table string) string {
	t.Helper()
	_, rest, ok := strings.Cut(schema, "CREATE TABLE IF NOT EXISTS "+table+" (")
	if !ok {
		t.Errorf("schema.sql declares no table %s", table)
		return ""
	}
	body, _, ok := strings.Cut(rest, "\n)")
	if !ok {
		t.Errorf("schema.sql leaves the %s block unterminated", table)
		return ""
	}
	return body
}

func dynamicConflictTargetExists(body, target string) bool {
	const tableExpression = `format('x_%s', lpad(i::text, 3, '0'))`
	body = regexp.MustCompile(`(?m)--.*$`).ReplaceAllString(body, "")
	expr := regexp.QuoteMeta(tableExpression)
	create := regexp.MustCompile(`(?s)EXECUTE\s+format\(\s*` +
		`'CREATE TABLE IF NOT EXISTS %I\s*\(([^']*)\)'\s*,\s*` + expr + `\s*\);`).FindStringSubmatch(body)
	insert := regexp.MustCompile(`(?s)EXECUTE\s+format\(\$f\$.*?INSERT INTO %I\b.*?` +
		`ON CONFLICT\s*\(([^)]*)\).*?\$f\$\s*,\s*` + expr + `(?:\s*,|\s*\))`).FindStringSubmatch(body)
	if create == nil || insert == nil || !sameConflictColumns(insert[1], target) {
		return false
	}
	cols := conflictColumns(target)
	if len(cols) == 1 {
		inline := regexp.MustCompile(`(?im)(?:^|,)\s*` + regexp.QuoteMeta(cols[0]) +
			`\s+[^,\n]*\b(?:PRIMARY KEY|UNIQUE)\b`)
		if inline.MatchString(create[1]) {
			return true
		}
	}
	keys := regexp.MustCompile(`(?i)\b(?:PRIMARY KEY|UNIQUE)\s*\(([^)]*)\)`)
	for _, key := range keys.FindAllStringSubmatch(create[1], -1) {
		if sameConflictColumns(key[1], target) {
			return true
		}
	}
	return false
}

func conflictColumns(target string) []string {
	cols := strings.Split(target, ",")
	for i := range cols {
		cols[i] = strings.TrimSpace(cols[i])
	}
	slices.Sort(cols)
	return cols
}

func sameConflictColumns(a, b string) bool {
	return slices.Equal(conflictColumns(a), conflictColumns(b))
}

func TestDynamicConflictTargetMustMatchDeclaredKey(t *testing.T) {
	body := `EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I (
    id bigint PRIMARY KEY,
    email text UNIQUE,
    tenant_id bigint,
    external_id text,
    UNIQUE (external_id, tenant_id)
)', format('x_%s', lpad(i::text, 3, '0')));
EXECUTE format($f$
    INSERT INTO %I (id, email, tenant_id, external_id)
    ON CONFLICT (id) DO NOTHING
$f$, format('x_%s', lpad(i::text, 3, '0')), 1);`
	for _, tc := range []struct {
		target string
		want   bool
	}{
		{target: "id", want: true},
		{target: "email", want: true},
		{target: "tenant_id, external_id", want: true},
		{target: "missing", want: false},
	} {
		t.Run(tc.target, func(t *testing.T) {
			paired := strings.Replace(body, "ON CONFLICT (id)", "ON CONFLICT ("+tc.target+")", 1)
			if got := dynamicConflictTargetExists(paired, tc.target); got != tc.want {
				t.Errorf("dynamicConflictTargetExists() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestDynamicConflictTargetUsesThePairedExecutableTemplate(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{
			name: "comment",
			body: `-- CREATE TABLE IF NOT EXISTS %I (id bigint PRIMARY KEY)'
EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I (id bigint)',
    format('x_%s', lpad(i::text, 3, '0')));
EXECUTE format($f$
    INSERT INTO %I (id) ON CONFLICT (id) DO NOTHING
$f$, format('x_%s', lpad(i::text, 3, '0')), 1);`,
		},
		{
			name: "different table expression",
			body: `EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I (id bigint PRIMARY KEY)',
    format('y_%s', lpad(i::text, 3, '0')));
EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I (id bigint)',
    format('x_%s', lpad(i::text, 3, '0')));
EXECUTE format($f$
    INSERT INTO %I (id) ON CONFLICT (id) DO NOTHING
$f$, format('x_%s', lpad(i::text, 3, '0')), 1);`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if dynamicConflictTargetExists(tc.body, "id") {
				t.Error("dynamic conflict target matched an unrelated CREATE template")
			}
		})
	}
}

// TestOnConflictTargetsExistBeforeTheLoad is the constraint that limits how
// much of schema.sql may be deferred: every bulk insert resolves ON CONFLICT
// against an index, so that index has to exist before the insert runs, on that
// table and not merely somewhere in the schema.
func TestOnConflictTargetsExistBeforeTheLoad(t *testing.T) {
	schema := read(t, "schema.sql")
	stmt := regexp.MustCompile(`INSERT INTO ([\w.%]+)[\s\S]*?ON CONFLICT \(([^)]*)\)`)
	var found int
	for _, name := range sqlFiles(t) {
		if !strings.HasPrefix(name, "seed_") {
			continue
		}
		for _, m := range stmt.FindAllStringSubmatch(read(t, name), -1) {
			found++
			table := strings.TrimPrefix(m[1], "public.")
			// A stage that builds its tables dynamically inserts into %I, so
			// there is no name to look up in schema.sql. The idempotency this
			// test exists to protect still has to hold, so require the same
			// file to declare the key its ON CONFLICT resolves against.
			if table == "%I" {
				body := read(t, name)
				if !dynamicConflictTargetExists(body, m[2]) {
					t.Errorf("%s inserts into a dynamic table conflicting on (%s) but"+
						" declares no matching key in its own CREATE TABLE", name, m[2])
				}
				continue
			}
			block := tableBlock(t, schema, table)
			cols := strings.Split(m[2], ",")
			for i := range cols {
				cols[i] = strings.TrimSpace(cols[i])
			}
			if len(cols) > 1 {
				want := "PRIMARY KEY (" + strings.Join(cols, ", ") + ")"
				if !strings.Contains(block, want) {
					t.Errorf("%s conflicts on (%s) but %s declares no %q",
						name, m[2], table, want)
				}
				continue
			}
			col := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(cols[0]) +
				`\s+[^\n]*\b(PRIMARY KEY|UNIQUE)\b`)
			if !col.MatchString(block) {
				t.Errorf("%s conflicts on (%s) but %s gives that column no primary "+
					"key or unique constraint", name, cols[0], table)
			}
		}
	}
	// Every batched insert must be one of them. A load without ON CONFLICT
	// duplicates rows when a crashed seed re-runs, which is the contract
	// prelude.sql states and the whole marker scheme rests on.
	var inserts int
	for _, name := range sqlFiles(t) {
		if strings.HasPrefix(name, "seed_") {
			inserts += strings.Count(read(t, name), "INSERT INTO ")
		}
	}
	if found != inserts || found == 0 {
		t.Fatalf("%d of %d seed inserts resolve ON CONFLICT; every one has to, or a "+
			"re-run duplicates instead of converging", found, inserts)
	}
}

func TestExtraFixtureRejectsImpossibleShapes(t *testing.T) {
	body := read(t, "seed_extra.sql")
	for _, want := range []string{
		"IF total_mb < n_tables THEN",
		"IF shards < 1 OR shard < 0 OR shard >= shards THEN",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("seed_extra.sql does not enforce %q", want)
		}
	}
}

func TestExtraFixtureAllocatesTheExactTotalBeforeSharding(t *testing.T) {
	body := read(t, "seed_extra.sql")
	steps := []string{
		"remaining_mb := total_mb - n_tables",
		"cumulative_w := cumulative_w + weights[i]",
		"next_allocated_mb := round(cumulative_w / total_w * remaining_mb)::bigint",
		"mb := 1 + next_allocated_mb - allocated_mb",
		"allocated_mb := next_allocated_mb",
		"CONTINUE WHEN (i % shards) <> shard",
	}
	previous := -1
	for _, step := range steps {
		at := strings.Index(body, step)
		if at < 0 {
			t.Errorf("seed_extra.sql does not contain allocation step %q", step)
			continue
		}
		if at < previous {
			t.Errorf("seed_extra.sql performs %q before the preceding allocation step", step)
		}
		previous = at
	}
	allocationLoop := strings.LastIndex(body, "FOR i IN 1..n_tables LOOP")
	if allocationLoop < 0 {
		t.Fatal("seed_extra.sql has no allocation loop")
	}
	loopEnd := strings.Index(body[allocationLoop:], "END LOOP;")
	if loopEnd < 0 {
		t.Fatal("seed_extra.sql leaves the allocation loop unterminated")
	}
	loopEnd += allocationLoop
	invariant := strings.Index(body, "IF allocated_mb + n_tables <> total_mb THEN")
	if invariant < loopEnd {
		t.Error("seed_extra.sql does not verify the exact total after the allocation loop")
	}
}

// TestIncludedFixturesDoNotQuit is a regression guard. psql's \quit ends the
// script it appears in, not the session, so a short-circuit inside an included
// file lets the including stage run on with none of the setup that file was
// supposed to establish. The seed marker is checked in run.sh for this reason.
func TestIncludedFixturesDoNotQuit(t *testing.T) {
	included := map[string]bool{}
	for _, name := range sqlFiles(t) {
		for _, inc := range includes(read(t, name)) {
			included[inc] = true
		}
	}
	if len(included) == 0 {
		t.Fatal("no fixture includes another; this guard would pass vacuously")
	}
	quit := regexp.MustCompile(`(?m)^\\(q|quit)\b`)
	for name := range included {
		if quit.MatchString(read(t, name)) {
			t.Errorf("%s is included by another fixture and calls \\quit, which ends "+
				"only this file: the including stage would carry on regardless", name)
		}
	}
}

// TestRunOrder guards the sequence the whole split depends on: the marker is
// read once the schema exists and before any load, and the finish stage that
// writes it follows every load.
func TestRunOrder(t *testing.T) {
	run := read(t, "run.sh")
	at := func(needle string) int { return strings.Index(run, needle) }
	schema, marker := at("stage schema"), at("FROM e2e_seed")
	loads, wait, finish := at("pids=()"), strings.LastIndex(run, "wait "), strings.LastIndex(run, "stage finish")
	for what, i := range map[string]int{
		"stage schema": schema, "the seed marker check": marker,
		"the concurrent loads": loads, "a wait on them": wait, "stage finish": finish,
	} {
		if i < 0 {
			t.Fatalf("run.sh has no %s", what)
		}
	}
	if marker < schema {
		t.Error("run.sh reads the seed marker before applying the schema that declares it")
	}
	if loads < marker {
		t.Error("run.sh loads before checking the marker: a seeded cluster would reseed")
	}
	if finish < wait {
		t.Error("run.sh runs finish before waiting on the loads: it would write the " +
			"seed marker for data that is not there yet")
	}
}
