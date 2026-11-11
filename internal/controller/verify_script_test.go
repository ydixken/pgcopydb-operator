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
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// The stand-in for the runner image's pgcopydb, dispatching on argv: the two
// sentinel reads and the compare are the only calls the drain script makes.
// The marker on stderr is how a case proves the content path was or was not
// reached. The report ends in a newline, as pgcopydb's does.
const stubVerifyPgcopydb = `#!/bin/sh
case "$*" in
  "list tables "*) ;;
  *--endpos*) echo "$STUB_ENDPOS" ;;
  *--replay-lsn*) echo "$STUB_REPLAY" ;;
  *"compare data"*)
    echo "` + verifyCompareMarker + `" >&2
    printf '%s' "${STUB_COMPARE_LOG:-}" >&2
    if [ -n "${STUB_PGCOPYDB_FAIL:-}" ]; then
      echo "pgcopydb: could not open catalogs" >&2
      exit 12
    fi
    cat "$STUB_REPORT"; echo ;;
  *) echo "pgcopydb stub: unexpected call: $*" >&2; exit 9 ;;
esac
`

// The stand-in for the server: the two origin queries answer from the case,
// and anything read off stdin is the report query, whose verdict the compare
// stubs in compare_data_test.go already cover in detail.
const stubVerifyPsql = `#!/bin/sh
case "$*" in
  *pg_replication_origin_progress*) echo "$STUB_PROGRESS" ;;
  *pg_wal_lsn_diff*) echo "$STUB_GAP" ;;
  *pg_largeobject_metadata*)
    case "$1" in
      src) echo "${STUB_SOURCE_EMPTY:-f}" ;;
      tgt) echo "${STUB_TARGET_EMPTY:-f}" ;;
      *) exit 9 ;;
    esac ;;
  *)
    sql=$(cat)
    case "$sql" in
      *json_array_elements*) printf '%s' "${STUB_DIFFERING:-}" ;;
      *) echo "psql stub: unexpected query: $*" >&2; exit 9 ;;
    esac ;;
esac
`

const verifyCompareMarker = "stub: compare data ran"

// The refusal a human greps for, and the line whose exit status the controller
// turns into DrainIncomplete.
const wantRefused = "drain refuted"

// The pass a content check that found every table matching ends with.
const wantContentPass = "drain verified: pgcopydb compare data found all migrated tables matching"

// runVerifyScript executes the shipped drain script under a real shell with
// both stubs on PATH, with the wrapper's files in the test's own directory.
func runVerifyScript(t *testing.T, report string, env ...string) (string, int) {
	t.Helper()
	return runGatedVerifyScript(t, "", report, env...)
}

// runGatedVerifyScript is runVerifyScript for a verify Job built with a progress gate.
func runGatedVerifyScript(t *testing.T, gate, report string, env ...string) (string, int) {
	t.Helper()
	return runMigrationVerifyScript(t, passwordMigration(), gate, report, env...)
}

func runMigrationVerifyScript(t *testing.T, migration *v1beta1.Migration, gate, report string, env ...string) (string, int) {
	t.Helper()
	dir := t.TempDir()
	// The worker container is named after the binary, so the constant doubles
	// as the stub's file name.
	for name, body := range map[string]string{workerContainer: stubVerifyPgcopydb, "psql": stubVerifyPsql} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	reportPath := filepath.Join(dir, "report.json")
	if err := os.WriteFile(reportPath, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}

	job, err := buildVerifyJob(migration, testRunnerImage, gate)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(shellPath, "-c", inDir(job.Spec.Template.Spec.Containers[0].Args[1], dir))
	cmd.Env = append(os.Environ(),
		"PATH="+dir+":"+os.Getenv("PATH"),
		"PGCOPYDB_SOURCE_PGURI=src",
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
		t.Fatalf("running the verify script: %v\n%s", err, out)
		return "", 0
	}
}

// TestVerifyScript_Verdict is the regression test for a gate that blessed a
// cutover which had lost its last three commits. The origin sat 1040 bytes
// below endpos, under the 8192-byte tolerance the fast path allowed, so the
// content check never ran and the cleanup Job dropped the slot over three
// committed transactions. Only an exactly closed gap may pass on the origin;
// every other reading is the content check's to decide.
func TestVerifyScript_Verdict(t *testing.T) {
	const endpos = "0/92E17EB0"
	for _, tc := range []struct {
		name string
		// endpos defaults to the incident's, so only the unset-sentinel case
		// carries one.
		endpos      string
		report      string
		env         []string
		code        int
		wantLog     string
		wantCompare bool
	}{
		{
			name:    "the origin closed the gap",
			report:  reportMatching,
			env:     []string{"STUB_PROGRESS=" + endpos, "STUB_GAP=0"},
			code:    0,
			wantLog: "drain verified: origin progress " + endpos + " equals endpos " + endpos,
		},
		{
			name:        "the lost tail the tolerance blessed",
			report:      reportRowcount,
			env:         []string{"STUB_PROGRESS=0/92E17AA0", "STUB_GAP=1040", "STUB_DIFFERING=public.orders: source 20195 rows (checksum a1), target 20192 rows (checksum a1)"},
			code:        1,
			wantLog:     wantRefused,
			wantCompare: true,
		},
		{
			name:        "a gap inside the old tolerance, on an active source",
			report:      reportRowcount,
			env:         []string{"STUB_PROGRESS=0/92E17E78", "STUB_GAP=56", stubRowcountDiffers},
			code:        1,
			wantLog:     wantRefused,
			wantCompare: true,
		},
		{
			name:        "an idle source parked below endpos",
			report:      reportMatching,
			env:         []string{"STUB_PROGRESS=0/92D00000", "STUB_GAP=1186480"},
			code:        0,
			wantLog:     wantContentPass,
			wantCompare: true,
		},
		{
			name:        "empty databases with publication-filtered WAL",
			report:      "[]",
			env:         []string{"STUB_PROGRESS=0/92000000", "STUB_GAP=1477808", "STUB_DIFFERING=" + emptyCompareReportMessage, "STUB_SOURCE_EMPTY=t", "STUB_TARGET_EMPTY=t"},
			code:        0,
			wantLog:     "empty databases verified",
			wantCompare: true,
		},
		{
			name:        "an empty cached selection in a nonempty source",
			report:      "[]",
			env:         []string{"STUB_PROGRESS=0/92000000", "STUB_GAP=1477808", "STUB_DIFFERING=" + emptyCompareReportMessage, "STUB_SOURCE_EMPTY=f", "STUB_TARGET_EMPTY=t"},
			code:        1,
			wantLog:     wantRefused,
			wantCompare: true,
		},
		{
			// An origin reading past endpos is no more proof than one below
			// it, and the fast path must not read the sign as an argument.
			name:        "the origin reads past endpos",
			report:      reportMatching,
			env:         []string{"STUB_PROGRESS=0/92E17EC0", "STUB_GAP=-16"},
			code:        0,
			wantLog:     wantContentPass,
			wantCompare: true,
		},
		{
			// A work dir without a sentinel endpos reads 0/0, and a target
			// that applied nothing coalesces to 0/0 too: a zero gap between
			// two null LSNs must not read as a closed one.
			name:        "no endpos to prove anything against",
			endpos:      sentinel.ZeroLSN,
			report:      reportRowcount,
			env:         []string{"STUB_PROGRESS=" + sentinel.ZeroLSN, "STUB_GAP=0", "STUB_DIFFERING=public.orders: source 20195 rows (checksum a1), target 20192 rows (checksum a1)"},
			code:        1,
			wantLog:     wantRefused,
			wantCompare: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := tc.endpos
			if ep == "" {
				ep = endpos
			}
			out, code := runVerifyScript(t, tc.report,
				append(tc.env, "STUB_ENDPOS="+ep, "STUB_REPLAY="+ep)...)
			if code != tc.code {
				t.Fatalf("exit = %d, want %d\n%s", code, tc.code, out)
			}
			if !strings.Contains(out, tc.wantLog) {
				t.Fatalf("log missing %q:\n%s", tc.wantLog, out)
			}
			if got := strings.Contains(out, verifyCompareMarker); got != tc.wantCompare {
				t.Fatalf("content check ran = %v, want %v:\n%s", got, tc.wantCompare, out)
			}
			// The measurements a human reads off a refusal, on every path.
			if !strings.Contains(out, "endpos="+ep+" replay_lsn="+ep) {
				t.Fatalf("the drain line must carry endpos and replay_lsn:\n%s", out)
			}
		})
	}
}

// TestVerifyScript_ComparePathIsFailurePath: a content check that cannot
// produce a verdict must end the Job non-zero, because the Job's status is
// what the controller turns into DrainIncomplete. Here pgcopydb itself fails,
// which the wrapper refuses to read as a match.
func TestVerifyScript_ComparePathIsFailurePath(t *testing.T) {
	out, code := runVerifyScript(t, reportMatching,
		"STUB_ENDPOS=0/92E17EB0", "STUB_REPLAY=0/92E17EB0",
		"STUB_PROGRESS=0/92E17AA0", "STUB_GAP=1040", "STUB_PGCOPYDB_FAIL=1")
	if code == 0 {
		t.Fatalf("a compare that could not run must fail the Job:\n%s", out)
	}
	if !strings.Contains(out, wantRefused) {
		t.Fatalf("the refusal must name the drain:\n%s", out)
	}
}

// The drain read takes the copy counters and the compare progress from one
// window, so on the content path both lines come again right before the verdict.
func TestVerifyScript_ProgressStaysAtTheEnd(t *testing.T) {
	const counters = `{"tables":{"total":2,"done":2}}`
	gate := "echo '" + counters + "'\n"
	for name, tc := range map[string]struct {
		report  string
		env     []string
		code    int
		verdict string
	}{
		"the compare passes": {reportMatching, nil, 0, wantContentPass},
		"the compare refutes": {reportRowcount, []string{
			stubRowcountDiffers,
		}, 1, wantRefused},
	} {
		t.Run(name, func(t *testing.T) {
			out, code := runGatedVerifyScript(t, gate, tc.report, append(tc.env,
				"STUB_ENDPOS=0/92E17EB0", "STUB_REPLAY=0/92E17EB0", "STUB_PROGRESS=0/92E17AA0", "STUB_GAP=1040",
				"STUB_COMPARE_LOG="+runningProgressLine+"\n"+finalProgressLine+"\n")...)
			if code != tc.code {
				t.Fatalf("exit %d, want %d:\n%s", code, tc.code, out)
			}
			lines := strings.Split(strings.TrimSpace(out), "\n")
			if len(lines) < 3 {
				t.Fatalf("too few lines:\n%s", out)
			}
			got := lines[len(lines)-3:]
			want := []string{finalProgressLine, verifyProgressPrefix + counters}
			if got[0] != want[0] || got[1] != want[1] || !strings.HasPrefix(got[2], tc.verdict) {
				t.Fatalf("the log ends %q, want the final progress line, the counters and %q:\n%s", got, tc.verdict, out)
			}
			if n := strings.Count(out, verifyProgressPrefix); n != 2 {
				t.Fatalf("the counters line appears %d times, want 2: before the verdict logic, and again last:\n%s", n, out)
			}
		})
	}
}
