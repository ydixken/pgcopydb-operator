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
	"path/filepath"
	"regexp"
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

// stages returns the fixture names run.sh runs, in order: the bare stage calls
// plus the loop that runs the bulk loads concurrently.
func stages(t *testing.T, run string) []string {
	t.Helper()
	bare := regexp.MustCompile(`(?m)^\s*stage\s+([a-z_]+)\s*$`).FindAllStringSubmatch(run, -1)
	names := make([]string, 0, len(bare))
	for _, m := range bare {
		names = append(names, m[1])
	}
	for _, m := range regexp.MustCompile(`for \w+ in ([a-z_ ]+);`).FindAllStringSubmatch(run, -1) {
		names = append(names, strings.Fields(m[1])...)
	}
	if len(names) == 0 {
		t.Fatal("no stages parsed out of run.sh; the guards below would all pass vacuously")
	}
	return names
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

// TestOnConflictTargetsExistBeforeTheLoad is the constraint that limits how
// much of schema.sql may be deferred: every bulk insert resolves ON CONFLICT
// against an index, so that index has to exist before the insert runs, on that
// table and not merely somewhere in the schema.
func TestOnConflictTargetsExistBeforeTheLoad(t *testing.T) {
	schema := read(t, "schema.sql")
	stmt := regexp.MustCompile(`INSERT INTO ([\w.]+)[\s\S]*?ON CONFLICT \(([^)]*)\)`)
	var found int
	for _, name := range sqlFiles(t) {
		if !strings.HasPrefix(name, "seed_") {
			continue
		}
		for _, m := range stmt.FindAllStringSubmatch(read(t, name), -1) {
			found++
			table := strings.TrimPrefix(m[1], "public.")
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
