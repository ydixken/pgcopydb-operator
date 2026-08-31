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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Reports as pgcopydb compare data --json renders them, trimmed to the keys
// the wrapper reads. The rowcount pair is the live finding that exposed the
// blind gate: 338 rows on the source, 333 on the target, exit code 0.
const (
	reportMatching = `[{"schema":"public","name":"orders",` +
		`"source":{"rowcount":338,"checksum":"a1"},"target":{"rowcount":338,"checksum":"a1"}}]`
	reportRowcount = `[{"schema":"public","name":"orders",` +
		`"source":{"rowcount":338,"checksum":"a1"},"target":{"rowcount":333,"checksum":"a1"}}]`
	reportChecksum = `[{"schema":"public","name":"orders",` +
		`"source":{"rowcount":338,"checksum":"a1"},"target":{"rowcount":338,"checksum":"b2"}}]`
)

// The stand-in for the runner image's pgcopydb: it replays a canned report,
// or fails the way a broken catalog would.
const stubPgcopydb = `#!/bin/sh
if [ -n "${STUB_PGCOPYDB_FAIL:-}" ]; then
  echo "pgcopydb: could not open catalogs" >&2
  exit 12
fi
cat "$STUB_REPORT"
`

// The stand-in for the server. It checks that the wrapper wrote the report
// where the query reads it and handed over the query itself (a wrapper that
// did neither would otherwise look like a clean pass), then replays the
// verdict the case asks for. TestCompareDataQuery covers the query itself
// against a real Postgres. The sed pulls the report path back out of the
// \set line through which psql reads it.
const stubPsql = `#!/bin/sh
sql=$(cat)
case "$sql" in *json_array_elements*) ;; *) echo "psql stub: no query" >&2; exit 9 ;; esac
path=$(printf '%s' "$sql" | sed -n 's/^.set r .cat \(.*\).$/\1/p')
if [ -z "$path" ] || [ "$(cat "$path")" != "$(cat "$STUB_REPORT")" ]; then
  echo "psql stub: wrapper did not hand over the report" >&2
  exit 9
fi
if [ -n "${STUB_PSQL_FAIL:-}" ]; then
  echo "ERROR:  invalid input syntax for type json" >&2
  exit 3
fi
printf '%s' "${STUB_DIFFERING:-}"
`

// runCompareScript executes the shipped compare data script under a real
// shell with both stubs on PATH, and reports its combined output and exit
// code. compareReportPath is rewritten into the test's own directory: it is
// an absolute path, and the Job's emptyDir has no counterpart here.
func runCompareScript(t *testing.T, report string, env ...string) (string, int) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{"pgcopydb": stubPgcopydb, "psql": stubPsql} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	reportPath := filepath.Join(dir, "report.json")
	if err := os.WriteFile(reportPath, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}

	script := strings.ReplaceAll(compareDataScript, compareReportPath, filepath.Join(dir, "compare-data.json"))
	cmd := exec.Command(shellPath, "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+dir+":"+os.Getenv("PATH"),
		"PGCOPYDB_TARGET_PGURI=tgt",
		"STUB_REPORT="+reportPath,
	)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return string(out), 0
	case errors.As(err, &exitErr):
		return string(out), exitErr.ExitCode()
	default:
		t.Fatalf("running the compare script: %v\n%s", err, out)
		return "", 0
	}
}

// TestCompareDataScript_Verdict is the regression test for a gate that could
// not fail: pgcopydb compare data logs a row-count or checksum difference and
// exits 0 regardless, so every caller that read the exit code (the Verified
// condition and the drain verification alike) reported a match over a target
// missing rows. The wrapper must now decide on the report, and must refuse
// whenever it has no report it could trust.
func TestCompareDataScript_Verdict(t *testing.T) {
	for _, tc := range []struct {
		name    string
		report  string
		env     []string
		code    int
		wantLog string
	}{
		{
			name:    "every table matches",
			report:  reportMatching,
			code:    0,
			wantLog: `"rowcount":338`,
		},
		{
			name:    "a row count differs",
			report:  reportRowcount,
			env:     []string{"STUB_DIFFERING=public.orders: source 338 rows (checksum a1), target 333 rows (checksum a1)"},
			code:    1,
			wantLog: "public.orders: source 338 rows",
		},
		{
			name:    "a checksum differs",
			report:  reportChecksum,
			env:     []string{"STUB_DIFFERING=public.orders: source 338 rows (checksum a1), target 338 rows (checksum b2)"},
			code:    1,
			wantLog: "checksum b2",
		},
		{
			name:    "pgcopydb itself fails",
			report:  reportMatching,
			env:     []string{"STUB_PGCOPYDB_FAIL=1"},
			code:    1,
			wantLog: "could not run",
		},
		{
			name:    "the report cannot be evaluated",
			report:  reportMatching,
			env:     []string{"STUB_PSQL_FAIL=1"},
			code:    1,
			wantLog: "could not be evaluated",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, code := runCompareScript(t, tc.report, tc.env...)
			if code != tc.code {
				t.Fatalf("exit = %d, want %d\n%s", code, tc.code, out)
			}
			if !strings.Contains(out, tc.wantLog) {
				t.Fatalf("log missing %q:\n%s", tc.wantLog, out)
			}
		})
	}
}

// TestCompareDataScript_ReportIsLogged: the wrapper is the only thing that
// reads the report, so the per-table detail a human needs to act on a
// mismatch has to reach the Job log even when the verdict is a pass.
func TestCompareDataScript_ReportIsLogged(t *testing.T) {
	out, code := runCompareScript(t, reportMatching)
	if code != 0 || !strings.Contains(out, reportMatching) {
		t.Fatalf("the passing run must still print the whole report (exit %d):\n%s", code, out)
	}
}

// TestCompareDataQuery runs the shipped SQL against a real server, which is
// the only place the JSON key paths and the comparisons can be proven: the
// stub above stands in for the server, not for this query, so without this
// test the one piece of logic that decides the verdict ships unexercised. CI
// sets PGCOPYDB_TEST_PGURI from a Postgres service container (see ci.yml);
// point it at any reachable server to run it locally.
func TestCompareDataQuery(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("set PGCOPYDB_TEST_PGURI to a reachable Postgres to exercise the compare query")
	}
	// Fatal, not a skip: an image that lost psql would otherwise take this
	// test back out of CI without anyone noticing.
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatalf("PGCOPYDB_TEST_PGURI is set but psql is missing: %v", err)
	}

	const line = "public.orders: source 338 rows (checksum a1), target %s rows (checksum %s)"
	for _, tc := range []struct {
		name   string
		report string
		want   string
	}{
		{"every table matches", reportMatching, ""},
		{"a row count differs", reportRowcount, fmt.Sprintf(line, "333", "a1")},
		{"a checksum differs", reportChecksum, fmt.Sprintf(line, "338", "b2")},
		// The three shapes a report can take that carry no difference to find.
		// Each one used to read as a full pass, which is the original defect
		// with the report as its source instead of the exit code.
		{"the report lists no table", `[]`, "the report lists no table, so nothing was compared"},
		{
			name:   "the target side is absent",
			report: `[{"schema":"public","name":"orders","source":{"rowcount":338,"checksum":"a1"}}]`,
			want:   fmt.Sprintf(line, "", ""),
		},
		{
			name:   "the keys are not the ones the query reads",
			report: `[{"schema":"public","name":"orders","source":{"rows":338},"target":{"rows":333}}]`,
			want:   "public.orders: source  rows (checksum ), target  rows (checksum )",
		},
		// The report reaches psql off argv, which caps one string at 128 KiB
		// on Linux; through -v this many tables failed to exec at all.
		{"a report past the argv cap", bigMatchingReport(1000), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "compare-data.json")
			if err := os.WriteFile(path, []byte(tc.report), 0o644); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("psql", uri, "-tAX", "-v", "ON_ERROR_STOP=1", "-f", "-")
			cmd.Stdin = strings.NewReader(strings.ReplaceAll(compareReportQuery, compareReportPath, path))
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("psql: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != tc.want {
				t.Fatalf("query returned %q, want %q", got, tc.want)
			}
		})
	}
}

// bigMatchingReport renders n matching tables the way pgcopydb pretty-prints
// them, checksum length included, so the result is representative in size.
func bigMatchingReport(n int) string {
	var b strings.Builder
	b.WriteString("[")
	for i := range n {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "\n  {\n    \"schema\": \"public\",\n    \"name\": \"t%d\",\n"+
			"    \"source\": {\"rowcount\": 1, \"checksum\": \"%[2]s\"},\n"+
			"    \"target\": {\"rowcount\": 1, \"checksum\": \"%[2]s\"}\n  }", i, strings.Repeat("0123456789abcdef", 4))
	}
	b.WriteString("\n]\n")
	return b.String()
}

// TestBuildCompareJob_Shapes: the data check ships as a script Job so the
// wrapper's status becomes the Job's, while compare schema keeps running as
// bare argv (it counts its own diffs and exits on them, so it was never
// blind). Both keep the bounded libpq connect timeout.
func TestBuildCompareJob_Shapes(t *testing.T) {
	m := passwordMigration()

	data, err := buildCompareJob(m, testRunnerImage, compareData)
	if err != nil {
		t.Fatal(err)
	}
	c := data.Spec.Template.Spec.Containers[0]
	if got := c.Command[len(c.Command)-1]; got != shellPath {
		t.Fatalf("data compare $0 = %q, want %s", got, shellPath)
	}
	if len(c.Args) != 2 || c.Args[0] != "-c" || c.Args[1] != compareDataScript {
		t.Fatalf("data compare must run the wrapper script, got %v", c.Args)
	}
	if envValue(c.Env, "PGCONNECT_TIMEOUT") != "10" {
		t.Fatal("data compare must keep the bounded connect timeout")
	}

	schema, err := buildCompareJob(m, testRunnerImage, compareSchema)
	if err != nil {
		t.Fatal(err)
	}
	sc := schema.Spec.Template.Spec.Containers[0]
	if got := strings.Join(sc.Args, " "); got != "compare schema --dir /work/pgcopydb" {
		t.Fatalf("schema compare argv = %q", got)
	}
}

// TestVerifyScript_ComparesThroughTheWrapper: the drain gate and the Verified
// condition must share one implementation, or fixing one leaves the other
// blind. The drain script may reach its pass only through the wrapper.
func TestVerifyScript_ComparesThroughTheWrapper(t *testing.T) {
	job, err := buildVerifyJob(passwordMigration(), testRunnerImage, "")
	if err != nil {
		t.Fatal(err)
	}
	script := job.Spec.Template.Spec.Containers[0].Args[1]
	if !strings.Contains(script, compareDataStrict) {
		t.Fatalf("verify script must carry the shared wrapper:\n%s", script)
	}
	if !strings.Contains(script, "if compare_data_strict; then") {
		t.Fatalf("the content path must run through the wrapper:\n%s", script)
	}
	// A bare `pgcopydb compare data` outside the wrapper would be a second,
	// blind gate: the only occurrence is the one the wrapper runs.
	if n := strings.Count(script, "compare data --dir"); n != 1 {
		t.Fatalf("compare data must be invoked once, through the wrapper, got %d:\n%s", n, script)
	}
	// The wrapper returns rather than exits, so the drain script keeps its own
	// refusal line and stays the thing that reports DrainIncomplete.
	if strings.Contains(compareDataStrict, "exit ") {
		t.Fatalf("the wrapper must return, not exit, or the drain verdict line is lost:\n%s", compareDataStrict)
	}
}
