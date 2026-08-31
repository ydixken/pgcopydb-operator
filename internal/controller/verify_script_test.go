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
)

// The stand-in for the runner image's pgcopydb, dispatching on argv: the two
// sentinel reads and the compare are the only calls the drain script makes.
// The marker on stderr is how a case proves the content path was or was not
// reached.
const stubVerifyPgcopydb = `#!/bin/sh
case "$*" in
  *--endpos*) echo "$STUB_ENDPOS" ;;
  *--replay-lsn*) echo "$STUB_REPLAY" ;;
  *"compare data"*)
    echo "` + verifyCompareMarker + `" >&2
    if [ -n "${STUB_PGCOPYDB_FAIL:-}" ]; then
      echo "pgcopydb: could not open catalogs" >&2
      exit 12
    fi
    cat "$STUB_REPORT" ;;
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
  *)
    sql=$(cat)
    case "$sql" in
      *json_array_elements*) printf '%s' "${STUB_DIFFERING:-}" ;;
      *) echo "psql stub: unexpected query: $*" >&2; exit 9 ;;
    esac ;;
esac
`

const verifyCompareMarker = "stub: compare data ran"

// runVerifyScript executes the shipped drain script under a real shell with
// both stubs on PATH. compareReportPath is rewritten into the test's own
// directory, because the Job's emptyDir has no counterpart here.
func runVerifyScript(t *testing.T, report string, env ...string) (string, int) {
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

	job, err := buildVerifyJob(passwordMigration(), testRunnerImage, "")
	if err != nil {
		t.Fatal(err)
	}
	script := strings.ReplaceAll(job.Spec.Template.Spec.Containers[0].Args[1],
		compareReportPath, filepath.Join(dir, "compare-data.json"))
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
		name        string
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
			wantLog:     "drain refuted",
			wantCompare: true,
		},
		{
			name:        "a gap inside the old tolerance, on an active source",
			report:      reportRowcount,
			env:         []string{"STUB_PROGRESS=0/92E17E78", "STUB_GAP=56", "STUB_DIFFERING=public.orders: source 338 rows (checksum a1), target 333 rows (checksum a1)"},
			code:        1,
			wantLog:     "drain refuted",
			wantCompare: true,
		},
		{
			name:        "an idle source parked below endpos",
			report:      reportMatching,
			env:         []string{"STUB_PROGRESS=0/92D00000", "STUB_GAP=1186480"},
			code:        0,
			wantLog:     "drain verified: pgcopydb compare data found all migrated tables matching",
			wantCompare: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, code := runVerifyScript(t, tc.report,
				append(tc.env, "STUB_ENDPOS="+endpos, "STUB_REPLAY="+endpos)...)
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
			if !strings.Contains(out, "endpos="+endpos+" replay_lsn="+endpos) {
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
	if !strings.Contains(out, "drain refuted") {
		t.Fatalf("the refusal must name the drain:\n%s", out)
	}
}
