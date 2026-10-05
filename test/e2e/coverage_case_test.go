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
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

// coverageFS carries the feature coverage case files and the fingerprint query.
//
//go:embed coverage
var coverageFS embed.FS

const (
	coverageGroupClone  = "clone"
	coverageGroupFollow = "follow"
	coverageGroupOwn    = "own"
	coverageIdentical   = "identical"
	coverageFingerprint = "coverage/fingerprint.sql"
	coverageSetup       = "setup"
	coverageFollow      = "follow"
	coverageSchema      = "${schema}"
	coverageSchema2     = "${schema2}"
	// cov2_<name>_<19-digit identity> must stay within PostgreSQL's 63-byte names.
	coverageMaxName = 37
)

// coverageOutcomes names the outcome assertions an own case may expect.
var coverageOutcomes = map[string]bool{}

// coverageCase is one parsed case file. Its sections keep their placeholders
// until render.
type coverageCase struct {
	name, path     string
	group, expect  string
	schemas, minPG int
	setup, follow  string
}

var (
	coverageCaseName    = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	coveragePlaceholder = regexp.MustCompile(`\$\{[^}]*\}`)
	// GRANT ... ON SCHEMA stays allowed; it changes neither owner nor comment.
	coverageSchemaChange = regexp.MustCompile(`(?i)\b(alter|comment\s+on)\s+schema\b`)
)

// parseCoverageCase parses one case file; file is its path in coverageFS.
func parseCoverageCase(file, src string, outcomes map[string]bool) (coverageCase, error) {
	c := coverageCase{path: file, name: strings.TrimSuffix(path.Base(file), ".sql")}
	lines := strings.Split(strings.TrimRight(src, "\n"), "\n")
	err := func() error {
		if !coverageCaseName.MatchString(c.name) || len(c.name) > coverageMaxName {
			return fmt.Errorf("case name must match %s and be at most %d bytes", coverageCaseName, coverageMaxName)
		}
		if err := c.parseHeader(lines[0], outcomes); err != nil {
			return err
		}
		if err := c.parseSections(lines[1:]); err != nil {
			return err
		}
		return c.checkBody()
	}()
	if err != nil {
		return coverageCase{}, fmt.Errorf("%s: %w", file, err)
	}
	return c, nil
}

func (c *coverageCase) parseHeader(line string, outcomes map[string]bool) error {
	header, ok := strings.CutPrefix(strings.TrimSpace(line), "-- coverage:")
	if !ok {
		return errors.New("first line must be the -- coverage: header")
	}
	seen := map[string]bool{}
	for field := range strings.FieldsSeq(header) {
		key, value, _ := strings.Cut(field, "=")
		if seen[key] {
			return fmt.Errorf("header repeats %q", key)
		}
		seen[key] = true
		var err error
		switch key {
		case "group":
			c.group = value
		case "expect":
			c.expect = value
		case "schemas":
			c.schemas, err = strconv.Atoi(value)
		case "min_pg":
			c.minPG, err = strconv.Atoi(value)
		default:
			return fmt.Errorf("unknown header %q", key)
		}
		if err != nil {
			return fmt.Errorf("header %s=%q is not a number", key, value)
		}
	}
	for _, key := range []string{"group", "expect", "schemas", "min_pg"} {
		if !seen[key] {
			return fmt.Errorf("header lacks %s", key)
		}
	}
	switch {
	case !slices.Contains([]string{coverageGroupClone, coverageGroupFollow, coverageGroupOwn}, c.group):
		return fmt.Errorf("unknown group %q", c.group)
	case c.group == coverageGroupOwn && !outcomes[c.expect]:
		return fmt.Errorf("unknown outcome %q for an own case", c.expect)
	case c.group != coverageGroupOwn && c.expect != coverageIdentical:
		return fmt.Errorf("a %s case must expect identical, got %q", c.group, c.expect)
	case c.schemas != 1 && c.schemas != 2:
		return fmt.Errorf("schemas must be 1 or 2, got %d", c.schemas)
	case c.minPG < 14:
		return fmt.Errorf("min_pg must be at least 14, got %d", c.minPG)
	}
	return nil
}

// parseSections reads the lines after the header. Plain -- comments may
// precede @setup; any other text outside a section is an error.
func (c *coverageCase) parseSections(lines []string) error {
	sections := map[string]*strings.Builder{}
	var current *strings.Builder
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		marker, isMarker := strings.CutPrefix(trimmed, "-- @")
		switch {
		case isMarker && marker != coverageSetup && marker != coverageFollow:
			return fmt.Errorf("line %d: unknown section @%s", i+2, marker)
		case isMarker && sections[marker] != nil:
			return fmt.Errorf("line %d: repeated section @%s", i+2, marker)
		case isMarker && marker == coverageFollow && sections[coverageSetup] == nil:
			return fmt.Errorf("line %d: @follow before @setup", i+2)
		case isMarker:
			current = &strings.Builder{}
			sections[marker] = current
		case current != nil:
			current.WriteString(line + "\n")
		case trimmed != "" && !strings.HasPrefix(trimmed, "--"):
			return fmt.Errorf("line %d: SQL outside a section", i+2)
		}
	}
	if sections[coverageSetup] == nil || strings.TrimSpace(sections[coverageSetup].String()) == "" {
		return errors.New("missing or empty @setup")
	}
	c.setup = sections[coverageSetup].String()
	if follow := sections[coverageFollow]; follow != nil {
		c.follow = follow.String()
	}
	switch {
	case c.group == coverageGroupClone && c.follow != "":
		return errors.New("a clone case has no @follow")
	case c.group != coverageGroupClone && strings.TrimSpace(c.follow) == "":
		return fmt.Errorf("a %s case needs a non-empty @follow", c.group)
	}
	return nil
}

// checkBody rejects unknown placeholders and statements on the case schemas
// themselves: their owner and comment are the stamp that guards cleanup.
func (c *coverageCase) checkBody() error {
	if coverageSchemaChange.MatchString(c.setup + c.follow) {
		return errors.New("a case must not alter or comment on its schemas")
	}
	usesSecond := false
	for _, m := range coveragePlaceholder.FindAllString(c.setup+c.follow, -1) {
		switch {
		case m == coverageSchema:
		case m == coverageSchema2 && c.schemas == 2:
			usesSecond = true
		default:
			return fmt.Errorf("unknown placeholder %s for schemas=%d", m, c.schemas)
		}
	}
	if c.schemas == 2 && !usesSecond {
		return errors.New("schemas=2 but ${schema2} is never used")
	}
	return nil
}

// loadCoverageCases parses every coverage/<area>/<case>.sql in fsys, sorted by
// path, and reports every broken file at once.
func loadCoverageCases(fsys fs.FS, outcomes map[string]bool) ([]coverageCase, error) {
	var cases []coverageCase
	var errs []error
	byName := map[string]string{}
	walkErr := fs.WalkDir(fsys, "coverage", func(file string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || file == coverageFingerprint {
			return err
		}
		if path.Ext(file) != ".sql" || strings.Count(file, "/") != 2 {
			errs = append(errs, fmt.Errorf("%s: expected coverage/<area>/<case>.sql", file))
			return nil
		}
		src, err := fs.ReadFile(fsys, file)
		if err != nil {
			return err
		}
		c, err := parseCoverageCase(file, string(src), outcomes)
		if err != nil {
			errs = append(errs, err)
			return nil
		}
		if other, dup := byName[c.name]; dup {
			errs = append(errs, fmt.Errorf("%s: case name %q already used by %s", file, c.name, other))
			return nil
		}
		byName[c.name] = file
		cases = append(cases, c)
		return nil
	})
	if err := errors.Join(append(errs, walkErr)...); err != nil {
		return nil, err
	}
	return cases, nil
}

// schemaNames are the case's schemas for one run, in placeholder order.
func (c coverageCase) schemaNames(identity string) []string {
	names := []string{"cov_" + c.name + "_" + identity}
	if c.schemas == 2 {
		names = append(names, "cov2_"+c.name+"_"+identity)
	}
	return names
}

// render replaces the placeholders in sql. The names are plain identifiers,
// so they need no quoting and also work inside string literals.
func (c coverageCase) render(sql string, schemas []string) string {
	replace := []string{coverageSchema, schemas[0]}
	if c.schemas == 2 {
		replace = append(replace, coverageSchema2, schemas[1])
	}
	return strings.NewReplacer(replace...).Replace(sql)
}

func TestParseCoverageCase(t *testing.T) {
	outcomes := map[string]bool{"truncate_not_replicated": true}
	const setup = "-- @setup\nCREATE TABLE ${schema}.t (id int);\n"
	const follow = "-- @follow\nINSERT INTO ${schema}.t VALUES (1);\n"
	for _, tc := range []struct {
		name, file, src, wantErr string
	}{
		{name: "clone", src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n" + setup},
		{name: "follow", src: "-- coverage: group=follow expect=identical schemas=1 min_pg=15\n" + setup + follow},
		{name: "own", src: "-- coverage: group=own expect=truncate_not_replicated schemas=1 min_pg=14\n" + setup + follow},
		{name: "two schemas", src: "-- coverage: group=clone  expect=identical  schemas=2  min_pg=14\n" +
			"-- @setup\nCREATE TABLE ${schema}.p (id int) PARTITION BY RANGE (id);\n" +
			"-- A leaf in the second schema.\nCREATE TABLE ${schema2}.l PARTITION OF ${schema}.p DEFAULT;\n"},
		{name: "comment and blank before setup",
			src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n\n-- why this case exists\n" + setup},
		{name: "no header", src: setup, wantErr: "first line must be the -- coverage: header"},
		{name: "unknown header", src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14 mode=x\n" + setup,
			wantErr: `unknown header "mode"`},
		{name: "repeated header", src: "-- coverage: group=clone group=follow expect=identical schemas=1 min_pg=14\n" + setup,
			wantErr: `header repeats "group"`},
		{name: "missing min_pg", src: "-- coverage: group=clone expect=identical schemas=1\n" + setup,
			wantErr: "header lacks min_pg"},
		{name: "non-numeric schemas", src: "-- coverage: group=clone expect=identical schemas=one min_pg=14\n" + setup,
			wantErr: `header schemas="one" is not a number`},
		{name: "unknown group", src: "-- coverage: group=clones expect=identical schemas=1 min_pg=14\n" + setup,
			wantErr: `unknown group "clones"`},
		{name: "unknown outcome", src: "-- coverage: group=own expect=truncate schemas=1 min_pg=14\n" + setup + follow,
			wantErr: `unknown outcome "truncate"`},
		{name: "group case with outcome",
			src:     "-- coverage: group=follow expect=truncate_not_replicated schemas=1 min_pg=14\n" + setup + follow,
			wantErr: "a follow case must expect identical"},
		{name: "three schemas", src: "-- coverage: group=clone expect=identical schemas=3 min_pg=14\n" + setup,
			wantErr: "schemas must be 1 or 2, got 3"},
		{name: "old major", src: "-- coverage: group=clone expect=identical schemas=1 min_pg=13\n" + setup,
			wantErr: "min_pg must be at least 14, got 13"},
		{name: "follow in clone", src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n" + setup + follow,
			wantErr: "a clone case has no @follow"},
		{name: "follow case without follow", src: "-- coverage: group=follow expect=identical schemas=1 min_pg=14\n" + setup,
			wantErr: "a follow case needs a non-empty @follow"},
		{name: "unknown section", src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n" + setup +
			"-- @teardown\nSELECT 1;\n", wantErr: "line 4: unknown section @teardown"},
		{name: "repeated setup", src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n" + setup + setup,
			wantErr: "line 4: repeated section @setup"},
		{name: "follow first", src: "-- coverage: group=follow expect=identical schemas=1 min_pg=14\n" + follow + setup,
			wantErr: "line 2: @follow before @setup"},
		{name: "sql before setup", src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\nSELECT 1;\n" + setup,
			wantErr: "line 2: SQL outside a section"},
		{name: "empty setup", src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n-- @setup\n\n",
			wantErr: "missing or empty @setup"},
		{name: "unknown placeholder",
			src:     "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n-- @setup\nCREATE TABLE ${other}.t ();\n",
			wantErr: "unknown placeholder ${other} for schemas=1"},
		{name: "second schema undeclared",
			src:     "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n-- @setup\nCREATE TABLE ${schema2}.t ();\n",
			wantErr: "unknown placeholder ${schema2} for schemas=1"},
		{name: "schema comment",
			src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n" +
				"-- @setup\nCOMMENT ON SCHEMA ${schema} IS 'x';\n",
			wantErr: "must not alter or comment on its schemas"},
		{name: "schema owner",
			src: "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n" +
				"-- @setup\nALTER SCHEMA ${schema} OWNER TO x;\n",
			wantErr: "must not alter or comment on its schemas"},
		{name: "second schema unused", src: "-- coverage: group=clone expect=identical schemas=2 min_pg=14\n" + setup,
			wantErr: "schemas=2 but ${schema2} is never used"},
		{name: "bad file name", file: "coverage/types/Money.sql",
			src:     "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n" + setup,
			wantErr: "case name must match"},
		{name: "long file name", file: "coverage/types/" + strings.Repeat("a", 38) + ".sql",
			src:     "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n" + setup,
			wantErr: "at most 37 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := tc.file
			if file == "" {
				file = "coverage/area/sample.sql"
			}
			c, err := parseCoverageCase(file, tc.src, outcomes)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				if !strings.HasPrefix(err.Error(), file+": ") {
					t.Fatalf("err = %v, want it to name %s", err, file)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.name != "sample" || !strings.Contains(c.setup, "CREATE TABLE ${schema}") {
				t.Fatalf("parsed %+v", c)
			}
		})
	}
}

func TestCoverageCaseRender(t *testing.T) {
	c, err := parseCoverageCase("coverage/partitioning/cross_leaf.sql",
		"-- coverage: group=clone expect=identical schemas=2 min_pg=14\n-- @setup\n"+
			"CREATE TABLE ${schema}.p (id int) PARTITION BY RANGE (id);\n"+
			"CREATE TABLE ${schema2}.l PARTITION OF ${schema}.p DEFAULT;\n"+
			"SELECT nextval('${schema}.s');\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	names := c.schemaNames("1759680000000000000")
	want := []string{"cov_cross_leaf_1759680000000000000", "cov2_cross_leaf_1759680000000000000"}
	if !slices.Equal(names, want) {
		t.Fatalf("schemaNames = %q, want %q", names, want)
	}
	got := c.render(c.setup, names)
	wantSQL := "CREATE TABLE cov_cross_leaf_1759680000000000000.p (id int) PARTITION BY RANGE (id);\n" +
		"CREATE TABLE cov2_cross_leaf_1759680000000000000.l PARTITION OF cov_cross_leaf_1759680000000000000.p DEFAULT;\n" +
		"SELECT nextval('cov_cross_leaf_1759680000000000000.s');\n"
	if got != wantSQL {
		t.Fatalf("render =\n%s\nwant\n%s", got, wantSQL)
	}
	longest := coverageCase{name: strings.Repeat("a", coverageMaxName), schemas: 2}
	for _, name := range longest.schemaNames("9223372036854775807") {
		if len(name) > 63 {
			t.Fatalf("schema %s is %d bytes, PostgreSQL truncates past 63", name, len(name))
		}
	}
}

func TestLoadCoverageCases(t *testing.T) {
	const ok = "-- coverage: group=clone expect=identical schemas=1 min_pg=14\n-- @setup\nSELECT 1;\n"
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }
	cases, err := loadCoverageCases(fstest.MapFS{
		"coverage/fingerprint.sql":      file("SELECT 1"),
		"coverage/types/b_uuid.sql":     file(ok),
		"coverage/objects/a_views.sql":  file(ok),
		"coverage/partitioning/c_x.sql": file(ok),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(cases))
	for _, c := range cases {
		names = append(names, c.name)
	}
	if want := []string{"a_views", "c_x", "b_uuid"}; !slices.Equal(names, want) {
		t.Fatalf("names = %q, want path order %q", names, want)
	}

	_, err = loadCoverageCases(fstest.MapFS{
		"coverage/types/dup.sql":   file(ok),
		"coverage/objects/dup.sql": file(ok),
		"coverage/types/bad.sql":   file("CREATE TABLE t ();\n"),
		"coverage/stray.sql":       file(ok),
		"coverage/types/notes.txt": file("x"),
	}, nil)
	for _, want := range []string{
		`coverage/types/dup.sql: case name "dup" already used by coverage/objects/dup.sql`,
		"coverage/types/bad.sql: first line must be the -- coverage: header",
		"coverage/stray.sql: expected coverage/<area>/<case>.sql",
		"coverage/types/notes.txt: expected coverage/<area>/<case>.sql",
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to contain %q", err, want)
		}
	}
}

// TestEmbeddedCoverageCasesParse keeps a broken case file from reaching a
// release candidate, where it would only surface as a failed spec.
func TestEmbeddedCoverageCasesParse(t *testing.T) {
	cases, err := loadCoverageCases(coverageFS, coverageOutcomes)
	if err != nil {
		t.Fatal(err)
	}
	groups := map[string]int{}
	for _, c := range cases {
		groups[c.group]++
	}
	for _, group := range []string{coverageGroupClone, coverageGroupFollow} {
		if groups[group] == 0 {
			t.Fatalf("no %s cases embedded", group)
		}
	}
}

// coverageMatrixRow is one row of the matrix in docs/reference/coverage.md.
var coverageMatrixRow = regexp.MustCompile("^\\| `([a-z_]+/[a-z0-9_]+)` \\| ([a-z]+) \\| ([a-z_]+) \\| ([0-9]+) \\|$")

// TestCoverageMatrixListsEveryCase keeps the documented matrix equal to the
// embedded cases, so a case cannot ship or change verdict undocumented.
func TestCoverageMatrixListsEveryCase(t *testing.T) {
	doc, err := os.ReadFile("../../docs/reference/coverage.md")
	if err != nil {
		t.Fatal(err)
	}
	var documented []string
	for line := range strings.Lines(string(doc)) {
		if m := coverageMatrixRow.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			documented = append(documented, strings.Join(m[1:], " "))
		}
	}
	cases, err := loadCoverageCases(coverageFS, coverageOutcomes)
	if err != nil {
		t.Fatal(err)
	}
	embedded := make([]string, 0, len(cases))
	for _, c := range cases {
		row := strings.TrimSuffix(strings.TrimPrefix(c.path, "coverage/"), ".sql")
		embedded = append(embedded, fmt.Sprintf("%s %s %s %d", row, c.group, c.expect, c.minPG))
	}
	if !slices.Equal(documented, embedded) {
		t.Fatalf("docs/reference/coverage.md lists\n%s\nthe embedded cases are\n%s",
			strings.Join(documented, "\n"), strings.Join(embedded, "\n"))
	}
}
