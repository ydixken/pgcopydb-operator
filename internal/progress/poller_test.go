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
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ydixken/pgcopydb-operator/internal/conn"
)

// patchedVersion is the allowlisted fixture version across these tests.
const patchedVersion = "0.18.15.gea2dc96"

const keepalivePatchedVersion = "0.18.13.g4873c18"

const previousPatchedVersion = "0.18.10.gaadc4bf"

const oldestPatchedVersion = "0.18.5.ge37d2bd"

// fakeExec scripts the podexec surface: tests choose the pod lookup result
// and the exec output, and read back what was executed.
type fakeExec struct {
	pod          string
	podErr       error
	out          []byte
	execErr      error
	argv         []string
	calls        int
	goneOnExec   bool
	podErrOnExec error
}

func (f *fakeExec) RunningPod(context.Context, string, string) (string, error) {
	f.calls++
	return f.pod, f.podErr
}

func (f *fakeExec) InPod(_ context.Context, _, _ string, argv []string) ([]byte, error) {
	f.calls++
	f.argv = argv
	if f.goneOnExec {
		f.pod = ""
	}
	if f.podErrOnExec != nil {
		f.podErr = f.podErrOnExec
	}
	return f.out, f.execErr
}

func TestNewFromExec_DropsInvalidVersions(t *testing.T) {
	p := NewFromExec(&fakeExec{}, []string{
		patchedVersion,
		"1.0-beta_2",
		"0.18;rm -rf /",
		"$(reboot)",
		"a b",
		"*",
		"",
	})
	want := []string{patchedVersion, "1.0-beta_2"}
	if len(p.allowed) != len(want) {
		t.Fatalf("allowed = %v, want %v", p.allowed, want)
	}
	for i, v := range want {
		if p.allowed[i] != v {
			t.Fatalf("allowed = %v, want %v", p.allowed, want)
		}
	}
}

func TestGateScript(t *testing.T) {
	p := NewFromExec(&fakeExec{}, []string{patchedVersion, keepalivePatchedVersion, previousPatchedVersion, oldestPatchedVersion})
	s := p.GateScript()
	for _, want := range []string{
		// The pattern list opens with "(", and that is asserted on the text
		// rather than by parsing the script, because the shells that reject
		// the bare form are not the ones a CI runner has: bash 3.2 refuses
		// it, bash 4.0 and everything after parse it, and so does dash
		// (measured). A parse check would therefore pass on a tree that has
		// the bug, which is the one thing this test exists to prevent. The
		// verify Job embeds this script inside $( ), where the bare form is
		// ambiguous, so the leading "(" is the property, not the parse.
		"\n(0.18.15.gea2dc96|0.18.13.g4873c18|0.18.10.gaadc4bf|0.18.5.ge37d2bd) pgcopydb list progress",
		"pgcopydb list progress --json --dir /work/pgcopydb",
		"v=${v#pgcopydb version }",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("gate script misses %q:\n%s", want, s)
		}
	}
}

// stubPgcopydb drops a pgcopydb stand-in on PATH that reports the given
// version and answers `list progress` with canned JSON.
func stubPgcopydb(t *testing.T, version string) string {
	t.Helper()
	dir := t.TempDir()
	stub := `#!/bin/sh
case "$1" in
--version)
  echo "pgcopydb version ` + version + `"
  echo "compiled with PostgreSQL 18.0"
  ;;
list)
  echo '{"tables":{"total":2,"done":1}}'
  ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "pgcopydb"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestGateScript_Disabled: an empty allowlist renders no script. Rendering
// the case statement with no pattern would be a syntax error, and the verify
// Job embeds whatever comes back into a larger script, so "the poll is off"
// has to mean nothing is emitted rather than something that cannot parse.
func TestGateScript_Disabled(t *testing.T) {
	for name, versions := range map[string][]string{
		"nil":                 nil,
		"empty":               {},
		"all entries invalid": {"; rm -rf /", "$(id)"},
	} {
		if s := NewFromExec(&fakeExec{}, versions).GateScript(); s != "" {
			t.Errorf("%s allowlist must render nothing, got:\n%s", name, s)
		}
	}
}

// TestGateScript_UnderSh proves the gate on a real shell: an allowlisted
// version opens it, any other version produces no output and a zero exit,
// and every allowlist shape renders something a shell will accept.
func TestGateScript_UnderSh(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("no sh available: %v", err)
	}
	p := NewFromExec(&fakeExec{}, []string{patchedVersion, keepalivePatchedVersion, previousPatchedVersion, oldestPatchedVersion})
	for version, want := range map[string]string{
		patchedVersion:           `{"tables":{"total":2,"done":1}}` + "\n",
		keepalivePatchedVersion:  `{"tables":{"total":2,"done":1}}` + "\n",
		previousPatchedVersion:   `{"tables":{"total":2,"done":1}}` + "\n",
		oldestPatchedVersion:     `{"tables":{"total":2,"done":1}}` + "\n",
		"0.18":                   "",
		"0.18.15.gea2dc96-extra": "",
		"0.18.13.g4873c18-extra": "",
		"0.18.10.gaadc4bf-extra": "",
		"0.18.5.ge37d2bd-extra":  "",
	} {
		cmd := exec.Command(sh, "-c", p.GateScript())
		cmd.Env = append(os.Environ(), "PATH="+stubPgcopydb(t, version)+":"+os.Getenv("PATH"))
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("version %s: %v", version, err)
		}
		if string(out) != want {
			t.Errorf("version %s: output %q, want %q", version, out, want)
		}
	}
	// The disabled gate has to parse too: it is pasted into the verify Job's
	// script, where a syntax error would be the whole file's problem.
	for _, versions := range [][]string{nil, {patchedVersion}, {patchedVersion, keepalivePatchedVersion, previousPatchedVersion, oldestPatchedVersion}} {
		script := NewFromExec(&fakeExec{}, versions).GateScript()
		if err := exec.Command(sh, "-n", "-c", script).Run(); err != nil {
			t.Errorf("allowlist %v renders a script sh rejects: %v\n%s", versions, err, script)
		}
	}
}

func ptr(n int64) *int64 { return &n }

func eq(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func TestSample(t *testing.T) {
	ctx := context.Background()
	// Five fields a side: database size, tables, tables holding rows, indexes,
	// table bytes; the source adds a sixth, the tables it holds rows in that
	// the target holds none in.
	for name, tc := range map[string]struct {
		out        string
		src, tgt   *int64
		wantCounts *RelationCounts
	}{
		"mid copy": {
			out: "source=1073741824 60 60 85 48000000000 37\ntarget=536870912 60 23 0 12000000000\n",
			src: ptr(1073741824), tgt: ptr(536870912),
			wantCounts: &RelationCounts{
				TablesTotal: 60, TablesDone: 23,
				IndexesTotal: 85, IndexesDone: 0,
				BytesTotal: 48000000000, BytesDone: 12000000000,
			},
		},
		"index build under way": {
			out: "source=1073741824 60 60 85 48000000000 0\ntarget=1000000000 60 60 41 47000000000\n",
			src: ptr(1073741824), tgt: ptr(1000000000),
			wantCounts: &RelationCounts{
				TablesTotal: 60, TablesDone: 60,
				IndexesTotal: 85, IndexesDone: 41,
				BytesTotal: 48000000000, BytesDone: 47000000000,
			},
		},
		// The shape of issue #277: pgcopydb called every table done while
		// one, populated on the source, held no rows on the target. Storage
		// read 57 of 57 for it, since a TOAST relation occupies a page from
		// the schema restore on; the owed count reads 56.
		"table empty on the target": {
			out: "source=10134634496 57 57 81 10134634496 1\ntarget=9247000000 57 56 81 9247000000\n",
			src: ptr(10134634496), tgt: ptr(9247000000),
			wantCounts: &RelationCounts{
				TablesTotal: 57, TablesDone: 56,
				IndexesTotal: 81, IndexesDone: 81,
				BytesTotal: 10134634496, BytesDone: 9247000000,
			},
		},
		// A table empty on both sides owes nothing and counts done, so a
		// finished copy reads whole without anything rounding it up.
		"table empty on both sides": {
			out: "source=1000 3 2 4 500 0\ntarget=900 3 2 4 480\n",
			src: ptr(1000), tgt: ptr(900),
			wantCounts: &RelationCounts{
				TablesTotal: 3, TablesDone: 3,
				IndexesTotal: 4, IndexesDone: 4,
				BytesTotal: 500, BytesDone: 480,
			},
		},
		// The schema restore has not run yet, so there is a size but nothing
		// to count. 0 of 0 is an absent sample, not progress.
		"target has no schema": {
			out: "source=1073741824 60 60 85 48000000000 0\ntarget=8388608 0 0 0 0\n",
			src: ptr(1073741824), tgt: ptr(8388608),
		},
		// One side unreadable: its size goes too, and counts need both.
		"source failed": {
			out: "source=\ntarget=536870912 60 23 0 12000000000\n",
			tgt: ptr(536870912),
		},
		"target failed": {
			out: "source=1073741824 60 60 85 48000000000 0\ntarget=\n",
			src: ptr(1073741824),
		},
		// A source row that is short or not numeric kills the counts, which
		// need both sides, but the target answered and its size still stands.
		"short source row":    {out: "source=1 60 60 85 48\ntarget=1 60 23 0 12\n", tgt: ptr(1)},
		"source not a number": {out: "source=1 60 60 85 48 oom\ntarget=1 60 23 0 12\n", tgt: ptr(1)},
		"no output":           {out: ""},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeExec{pod: "p", out: []byte(tc.out)}
			got, err := NewFromExec(f, nil).Sample(ctx, "ns", "job", false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("got no sample at all")
			}
			if !eq(got.SourceSize, tc.src) || !eq(got.TargetSize, tc.tgt) {
				t.Errorf("sizes = %v/%v, want %v/%v", got.SourceSize, got.TargetSize, tc.src, tc.tgt)
			}
			switch {
			case tc.wantCounts == nil && got.Counts != nil:
				t.Errorf("counts = %+v, want none", got.Counts)
			case tc.wantCounts != nil && got.Counts == nil:
				t.Errorf("no counts, want %+v", tc.wantCounts)
			case tc.wantCounts != nil && *got.Counts != *tc.wantCounts:
				t.Errorf("counts = %+v, want %+v", got.Counts, tc.wantCounts)
			}
		})
	}
}

// The URI recovery prelude has to be in front of the script, or psql runs
// with whatever the pod's environment happens to hold.
func TestSample_RunsWithTheURIPrelude(t *testing.T) {
	f := &fakeExec{pod: "p", out: []byte("source=\ntarget=\n")}
	if _, err := NewFromExec(f, nil).Sample(context.Background(), "ns", "job", false); err != nil {
		t.Fatal(err)
	}
	if len(f.argv) != 3 || f.argv[0] != "sh" || f.argv[1] != "-c" {
		t.Fatalf("argv = %q, want sh -c <script>", f.argv)
	}
	if !strings.HasPrefix(f.argv[2], conn.URIRecover()) {
		t.Error("the script does not start with the URI recovery prelude")
	}
	if !strings.Contains(f.argv[2], sampleScript) {
		t.Error("the script is not sampleScript")
	}
}

func TestSample_NoPodOrError(t *testing.T) {
	ctx := context.Background()
	if got, err := NewFromExec(&fakeExec{pod: ""}, nil).Sample(ctx, "ns", "job", false); got != nil || err != nil {
		t.Fatalf("no pod: got %+v, %v; want nil, nil", got, err)
	}
	if _, err := NewFromExec(&fakeExec{podErr: errors.New("api down")}, nil).Sample(ctx, "ns", "job", false); err == nil {
		t.Fatal("a pod lookup failure must surface")
	}
	execErr := errors.New("refused")
	if _, err := NewFromExec(&fakeExec{pod: "p", execErr: execErr}, nil).Sample(ctx, "ns", "job", false); !errors.Is(err, execErr) {
		t.Fatalf("exec failure = %v, want original error", err)
	}
}

func TestSample_ContainerExitDuringExecIsNoSample(t *testing.T) {
	f := &fakeExec{pod: "p", execErr: errors.New("container gone"), goneOnExec: true}
	got, err := NewFromExec(f, nil).Sample(context.Background(), "ns", "job", false)
	if got != nil || err != nil {
		t.Fatalf("got %+v, %v; want nil, nil", got, err)
	}
}

func TestSample_PodRecheckFailurePreservesExecError(t *testing.T) {
	execErr := errors.New("exec refused")
	f := &fakeExec{pod: "p", execErr: execErr, podErrOnExec: errors.New("API unavailable")}
	_, err := NewFromExec(f, nil).Sample(context.Background(), "ns", "job", false)
	if !errors.Is(err, execErr) {
		t.Fatalf("got %v; want original exec error", err)
	}
	if f.calls != 3 {
		t.Fatalf("calls = %d, want pod lookup, exec, and pod recheck", f.calls)
	}
}

func TestSample_AllDatabases(t *testing.T) {
	f := &fakeExec{pod: "p", out: []byte("source=7000 0 0 0 0 0\ntarget=6000 0 0 0 0\n")}
	got, err := NewFromExec(f, nil).Sample(context.Background(), "ns", "job", true)
	if err != nil || got == nil || !eq(got.SourceSize, ptr(7000)) || !eq(got.TargetSize, ptr(6000)) || got.Counts != nil {
		t.Fatalf("sample = %+v, %v; want sizes without counts", got, err)
	}
	script := f.argv[2]
	// The source row keeps the single-database shape, owed count included.
	for _, want := range []string{conn.URIRecover(), progressSQL, "sum(pg_database_size(oid))", "not in ('template0', 'template1')", "PGCOPYDB_SOURCE_PGURI", "PGCOPYDB_TARGET_PGURI", `"$row || ' 0'"`} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q", want)
		}
	}
	if strings.Contains(script, "current_database()") || strings.Contains(script, "pg_class") {
		t.Fatal("all-databases sampler queries only the maintenance database")
	}
	if err := exec.Command("sh", "-n", "-c", script).Run(); err != nil {
		t.Fatal(err)
	}
}

// Counting a TOAST or system relation is wrong in a way that still looks
// plausible on a tile, so the query has to exclude them by name.
func TestRelationCountsScript_ExcludesSystemSchemas(t *testing.T) {
	for _, want := range []string{"pg_catalog", "information_schema", "pg_toast", "relkind"} {
		if !strings.Contains(sampleScript, want) {
			t.Errorf("sampleScript does not mention %q", want)
		}
	}
}

// pg_total_relation_size adds indexes and TOAST to the table's own bytes, so
// an empty table carrying a primary key reads as copied and the byte
// denominator counts indexes the target has not built. Verified against a real
// pair: a target holding one populated table of three reported two.
func TestRelationCountsScript_MeasuresTheTableAndItsToast(t *testing.T) {
	// Both alternatives were measured against a real server, and both are
	// wrong in a way that reached a dashboard.
	if strings.Contains(sampleScript, "pg_total_relation_size") {
		t.Error("sampleScript uses pg_total_relation_size; it adds the indexes, so an " +
			"empty table carrying a primary key counts as copied. Use pg_table_size.")
	}
	if strings.Contains(sampleScript, "pg_relation_size") {
		t.Error("sampleScript uses pg_relation_size; it counts only the main fork, so a " +
			"table of documents measured 256kB where pg_table_size measured 66MB. " +
			"Use pg_table_size.")
	}
	if !strings.Contains(sampleScript, "pg_table_size") {
		t.Error("sampleScript measures no table size at all")
	}
}

// A table is done once its exact row count on the target is at least the
// source's, and only a read of the table can say so: its TOAST relation
// occupies a page from the schema restore on, so a storage test called an
// 848MB table with no rows on the target copied (issue #277), and a
// has-any-row test called a table interrupted mid-copy done the instant it
// held one row of many. The count travels in the target's scope list, which
// lets the source count exactly the tables whose target count falls short of
// its own; a table empty on both sides owes nothing.
func TestRelationCountsScript_CountsRowsNotStorage(t *testing.T) {
	for _, want := range []string{
		`rowcount="(xpath('/row/count/text()', query_to_xml(format('select count(*) from %I.%I', t.nspname, t.relname), false, true, '')))[1]::text::bigint"`,
		"(select count(*) from t where $rowcount > 0)",
		"|| ',' || ($rowcount)::text || ')'",                   // the target's row carries the count per table
		"join ($landed) as landed(name, rowcount)",             // and the source joins it
		"where t.target_rowcount < $rowcount)",                 // owed: target short of source
		`*\|?*) landed="values ${t#*|}"`,                       // the list rides the row as a second column
		`*) landed="select null::text, 0::bigint where false"`, // and a target that did not answer joins nothing
	} {
		if !strings.Contains(sampleScript, want) {
			t.Errorf("sampleScript is missing %q", want)
		}
	}
	if strings.Contains(sampleScript, "pg_table_size(t.oid) > 0") {
		t.Error("sampleScript calls a table done off its storage, which a TOAST relation occupies before any row lands")
	}
}

// The source must be asked about the target's tables, not its own. pgcopydb
// restores only the in-scope schema, so an unscoped source count reports
// indexes and bytes for tables this migration was told to leave behind, and a
// filtered migration then shows a denominator it can never reach.
func TestRelationCountsScript_ScopesTheSourceToTheTarget(t *testing.T) {
	// query returns the SQL one side is sent: from its progress_sql call to
	// the `") || ` that ends every such call in the script.
	query := func(side string) string {
		t.Helper()
		start := strings.Index(sampleScript, `progress_sql "$PGCOPYDB_`+side+`_PGURI"`)
		if start < 0 {
			t.Fatalf("sampleScript never asks the %s", side)
		}
		rest := sampleScript[start:]
		return rest[:strings.Index(rest, `") || `)]
	}
	// The list is built on the target, quoted, and the source joins it in
	// place of reading its own catalog unscoped.
	if target := query("TARGET"); !strings.Contains(target, "string_agg('(' || quote_literal(") {
		t.Errorf("the target's row does not carry the table list:\n%s", target)
	}
	source := query("SOURCE")
	if !strings.Contains(source, "join ($landed) as landed(name, rowcount) on landed.name = n.nspname || '.' || c.relname") {
		t.Errorf("the source is not scoped to the target's tables:\n%s", source)
	}
	if strings.Contains(source, "string_agg") {
		t.Errorf("the source builds a table list of its own:\n%s", source)
	}
}

// CloneStage decides a user-visible phase, so every way the probe can fail has
// to land on "unknown" rather than on a confident wrong answer. Unknown is
// both flags false, which leaves the caller reporting Cloning.

func TestCloneStage(t *testing.T) {
	for _, tc := range []struct {
		name                string
		exec                *fakeExec
		copying, finalizing bool
	}{
		{
			name:    "copy workers busy",
			exec:    &fakeExec{pod: "w", out: []byte("4 0\n")},
			copying: true,
		},
		{
			name:       "only the tail left",
			exec:       &fakeExec{pod: "w", out: []byte("0 1\n")},
			finalizing: true,
		},
		{
			// Both counts zero is a worker that holds no backend the query
			// counts: not connected yet, or already gone. Neither state.
			name: "no counted backends",
			exec: &fakeExec{pod: "w", out: []byte("0 0\n")},
		},
		{
			// The copy is winding down while the tail has started. Still
			// copying, because data is still moving.
			name:    "both kinds active",
			exec:    &fakeExec{pod: "w", out: []byte("2 3\n")},
			copying: true,
		},
		{name: "no running pod", exec: &fakeExec{}},
		{name: "pod lookup failed", exec: &fakeExec{podErr: errors.New("boom")}},
		{name: "exec failed", exec: &fakeExec{pod: "w", execErr: errors.New("boom")}},
		{name: "unparseable output", exec: &fakeExec{pod: "w", out: []byte("ERROR: nope\n")}},
		{name: "empty output", exec: &fakeExec{pod: "w", out: nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewFromExec(tc.exec, nil)
			copying, finalizing := p.CloneStage(context.Background(), "ns", "job")
			if copying != tc.copying || finalizing != tc.finalizing {
				t.Errorf("copying=%v finalizing=%v, want copying=%v finalizing=%v",
					copying, finalizing, tc.copying, tc.finalizing)
			}
		})
	}
}

// The probe must never open pgcopydb's SQLite catalog: doing that during a
// copy is what killed workers and made the poll conditional in the first
// place. It must also only count this worker's own backends, or another
// migration's compare workers read as this clone's tail.
func TestCloneStageQueryIsCatalogFreeAndScoped(t *testing.T) {
	f := &fakeExec{pod: "w", out: []byte("0 1\n")}
	NewFromExec(f, nil).CloneStage(context.Background(), "ns", "job")

	joined := strings.Join(f.argv, " ")
	for _, forbidden := range []string{"pgcopydb", "--dir", "list progress"} {
		if strings.Contains(joined, forbidden) && forbidden != "pgcopydb" {
			t.Errorf("probe runs %q; it must not touch the pgcopydb catalog: %s", forbidden, joined)
		}
	}
	if !strings.Contains(joined, "pg_stat_activity") {
		t.Errorf("probe does not read pg_stat_activity: %s", joined)
	}
	if !strings.Contains(joined, "client_addr = inet_client_addr()") {
		t.Errorf("probe is not scoped to this worker's own backends: %s", joined)
	}
}

// The two counts are asked differently on purpose, so the predicate is pinned
// by shape rather than by text: on a live worker all four copy workers stayed
// connected for the whole base copy, and counting only the active ones read as
// the tail while data was still moving.
//
// Both narrowings are pinned by absence, because a narrowing can be spelled
// any number of ways and only its absence is checkable. The copy count may
// name state exactly once, in the arm that falls back to the statement text,
// and the row filter may not name it at all. The edit this guards against is
// a likely one: anyone acting on a copy worker that lingers connected after
// its queue drains reaches for exactly such a conjunction.
func TestCloneStageCountsCopyWorkersByConnection(t *testing.T) {
	f := &fakeExec{pod: "w", out: []byte("4 1\n")}
	NewFromExec(f, nil).CloneStage(context.Background(), "ns", "job")
	// Case folded: SQL is case insensitive, so a narrowing spelled STATE has
	// to fail these checks the same way a lowercase one does.
	flat := strings.ToLower(strings.Join(strings.Fields(strings.Join(f.argv, " ")), " "))
	_, flat, foundSQL := strings.Cut(flat, `"select `)
	if !foundSQL {
		t.Fatal("probe SQL is missing")
	}

	copyCount, rest, ok := strings.Cut(flat, "|| ' ' ||")
	if !ok {
		t.Fatalf("probe no longer asks for two counts: %s", flat)
	}
	tailCount, afterFrom, ok := strings.Cut(rest, "from pg_stat_activity")
	if !ok {
		t.Fatalf("probe no longer reads pg_stat_activity: %s", flat)
	}
	// The SQL ends at the psql argument's closing quote; the URI prelude's
	// own quotes are all behind us by here.
	where, _, ok := strings.Cut(afterFrom, `"`)
	if !ok {
		t.Fatalf("cannot find the end of the probe query: %s", flat)
	}

	// A copy worker counts while it is connected, so the first arm of the
	// copy count tests the name and nothing else.
	primary, _, ok := strings.Cut(copyCount, " or ")
	if !ok {
		t.Fatalf("the copy count lost its statement-text fallback: %s", copyCount)
	}
	if !strings.Contains(primary, "application_name ilike '%copy worker%'") || strings.Contains(primary, "state") {
		t.Errorf("copy workers are not counted by connection: %s", primary)
	}
	// One mention of state in the whole copy count, the fallback arm's own.
	// A second one narrows the count however it is parenthesized, including
	// a conjunction wrapped around every arm at once.
	if n := strings.Count(copyCount, "state"); n != 1 {
		t.Errorf("copy count names state %d times, want 1 (the fallback arm alone): %s", n, copyCount)
	}
	if !strings.Contains(tailCount, "state = 'active'") {
		t.Errorf("the tail count dropped its active test: %s", tailCount)
	}
	if strings.Contains(where, "state") {
		t.Errorf("the row filter narrows by state, which is the bug this fixed: %s", where)
	}
}
