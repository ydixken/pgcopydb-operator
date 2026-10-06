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
	"fmt"
	"os"
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The preflight refusals, copied from the audits in internal/controller/resources.go.
const (
	rlsRefusal = "preflight: row-level security hides rows of these tables from the source migration role," +
		" so the copy would fail on them with the bundled runner," +
		" or copy and verify only the rows the role sees with a runner before 0.18.34.g7fddd6f"
	unloggedRefusal = "preflight: follow cannot replicate unlogged tables, whose changes never reach the WAL"
)

const (
	reasonPreflightFailed      = "PreflightFailed"
	reasonBackoffLimitExceeded = "BackoffLimitExceeded"
	largeObjectAspect          = "largeobject"
	// fingerprintMissingObject is fingerprint.sql's value for a listed large object the server lacks.
	fingerprintMissingObject = "missing"
)

// checkPreflightRefusal reports why m is not refused by the preflight with
// refusal naming exactly tables, as the audit lists them.
func checkPreflightRefusal(m *v1beta1.Migration, refusal, tables string) error {
	failed := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
	switch {
	case failed == nil || failed.Reason != reasonPreflightFailed:
		return fmt.Errorf("want Failed reason %s, got %+v", reasonPreflightFailed, failed)
	case m.Status.Attempts != 0:
		return fmt.Errorf("the preflight must refuse before any attempt, got %d attempts", m.Status.Attempts)
	case !strings.Contains(failed.Message, refusal+": "+tables+"; "):
		return fmt.Errorf("message does not refuse exactly %s with %q:\n%s", tables, refusal, failed.Message)
	}
	return nil
}

// checkMissingColumnFailure reports why m did not spend its whole retry
// budget on an apply that hits a column the target lacks (SQLSTATE 42703).
func checkMissingColumnFailure(m *v1beta1.Migration, workerLog string) error {
	failed := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
	switch {
	case failed == nil || failed.Reason != reasonBackoffLimitExceeded:
		return fmt.Errorf("want Failed reason %s, got %+v", reasonBackoffLimitExceeded, failed)
	case m.Status.Attempts != m.Spec.BackoffLimit+1:
		return fmt.Errorf("want %d attempts, got %d", m.Spec.BackoffLimit+1, m.Status.Attempts)
	case !strings.Contains(workerLog, "[42703]") || !strings.Contains(workerLog, "does not exist"):
		return fmt.Errorf("the last attempt's log shows no missing column (42703):\n%s", workerLog)
	}
	return nil
}

// checkLargeObjectsNotReplicated reports why diffs are not exactly the
// large-object changes made during follow, each missing from the target.
func checkLargeObjectsNotReplicated(diffs []fingerprintDiff) error {
	// Per object: whether the source, then the target, lacks it.
	want := map[string][2]bool{
		coverageSchema + " blob1": {false, false}, // patched on the source only
		coverageSchema + " blob2": {true, false},  // unlinked on the source only
		coverageSchema + " live":  {false, true},  // created during follow
	}
	if len(diffs) != len(want) {
		return fmt.Errorf("want %d large object differences, got %d: %v", len(want), len(diffs), diffs)
	}
	for _, d := range diffs {
		missing, ok := want[d.key]
		if d.aspect != largeObjectAspect || !ok ||
			(d.source == fingerprintMissingObject) != missing[0] || (d.target == fingerprintMissingObject) != missing[1] {
			return fmt.Errorf("unexpected difference %s", d)
		}
	}
	return nil
}

// failedOwnMigration is a Migration with backoffLimit 1, as the own cases run it.
func failedOwnMigration(reason, message string, attempts int32) *v1beta1.Migration {
	return &v1beta1.Migration{
		Spec: v1beta1.MigrationSpec{BackoffLimit: 1},
		Status: v1beta1.MigrationStatus{Phase: v1beta1.PhaseFailed, Attempts: attempts,
			Conditions: []metav1.Condition{{Type: v1beta1.ConditionFailed, Status: metav1.ConditionTrue,
				Reason: reason, Message: message}}},
	}
}

func TestCoveragePreflightRefusal(t *testing.T) {
	const table = "cov_rls_force_1.docs"
	refused := "preflight failed:\n" + rlsRefusal + ": " + table + "; grant the role BYPASSRLS\n"
	for _, tc := range []struct {
		name, refusal string
		m             *v1beta1.Migration
		tables        string
		wantErr       string
	}{
		{name: "refused", m: failedOwnMigration(reasonPreflightFailed, refused, 0), tables: table},
		{name: "other reason", m: failedOwnMigration(reasonBackoffLimitExceeded, refused, 2),
			tables: table, wantErr: "want Failed reason PreflightFailed"},
		{name: "not failed", m: &v1beta1.Migration{}, tables: table, wantErr: "want Failed reason PreflightFailed"},
		{name: "after an attempt", m: failedOwnMigration(reasonPreflightFailed, refused, 1),
			tables: table, wantErr: "before any attempt"},
		{name: "another table", m: failedOwnMigration(reasonPreflightFailed, refused, 0),
			tables: "cov_rls_force_1.other", wantErr: "does not refuse exactly cov_rls_force_1.other"},
		{name: "one table more", m: failedOwnMigration(reasonPreflightFailed,
			strings.Replace(refused, table+";", table+", public.extra;", 1), 0),
			tables: table, wantErr: "does not refuse exactly"},
		{name: "another refusal", refusal: unloggedRefusal, m: failedOwnMigration(reasonPreflightFailed, refused, 0),
			tables: table, wantErr: "does not refuse exactly"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkPreflightRefusal(tc.m, cmp.Or(tc.refusal, rlsRefusal), tc.tables)
			if tc.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestCoverageMissingColumnFailure(t *testing.T) {
	// The probe's worker log line, as PGCOPYDB_LOG_JSON=on writes it.
	const failedApply = `{"error_severity":"ERROR",` +
		`"message":"[TARGET 67] [42703] ERROR:  column \"extra\" of relation \"t\" does not exist"}`
	const exhausted = "attempt 2 failed: Job has reached the specified backoff limit;" +
		" last error: follow process 4 has terminated [12]"
	for _, tc := range []struct {
		name    string
		m       *v1beta1.Migration
		log     string
		wantErr string
	}{
		{name: "exhausted", m: failedOwnMigration(reasonBackoffLimitExceeded, exhausted, 2), log: failedApply},
		{name: "refused at preflight", m: failedOwnMigration(reasonPreflightFailed, "preflight failed", 0),
			log: failedApply, wantErr: "want Failed reason BackoffLimitExceeded"},
		{name: "budget left", m: failedOwnMigration(reasonBackoffLimitExceeded, exhausted, 1), log: failedApply,
			wantErr: "want 2 attempts, got 1"},
		{name: "other error", m: failedOwnMigration(reasonBackoffLimitExceeded, exhausted, 2),
			log:     `{"error_severity":"ERROR","message":"[TARGET 67] [57014] ERROR:  canceling statement"}`,
			wantErr: "no missing column (42703)"},
		{name: "no log", m: failedOwnMigration(reasonBackoffLimitExceeded, exhausted, 2),
			wantErr: "no missing column (42703)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkMissingColumnFailure(tc.m, tc.log)
			if tc.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestCoverageLargeObjectsNotReplicated(t *testing.T) {
	lo := func(key, source, target string) fingerprintDiff {
		return fingerprintDiff{aspect: largeObjectAspect, key: coverageSchema + " " + key, source: source, target: target}
	}
	pinned := []fingerprintDiff{
		lo("blob1", "9b19 owner=app acl=", "aed5 owner=app acl="),
		lo("blob2", fingerprintMissingObject, "bc4b owner=app acl="),
		lo("live", "0c3a owner=app acl=", fingerprintMissingObject),
	}
	if err := checkLargeObjectsNotReplicated(pinned); err != nil {
		t.Fatal(err)
	}
	for name, diffs := range map[string][]fingerprintDiff{
		"replicated":        nil,
		"patch replicated":  pinned[1:],
		"unlink replicated": {pinned[0], pinned[2]},
		"row lost":          {pinned[0], pinned[1], lo("live", "0c3a owner=app acl=", fingerprintAbsent)},
		"created on target": {pinned[0], pinned[1], lo("live", "0c3a owner=app acl=", "0c3a owner=app acl=x")},
		"untouched changed": {pinned[0], pinned[1], pinned[2], lo("blob3", "f1d3 owner=app acl=", "0000 owner=app acl=")},
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkLargeObjectsNotReplicated(diffs); err == nil {
				t.Fatalf("accepted %v", diffs)
			}
		})
	}
}

// TestCoverageRefusalsMatchThePreflight keeps the copied refusals equal to
// the audits' messages, so a reworded audit fails here and not on a cluster.
func TestCoverageRefusalsMatchThePreflight(t *testing.T) {
	src, err := os.ReadFile("../../internal/controller/resources.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, refusal := range []string{rlsRefusal, unloggedRefusal} {
		if !strings.Contains(string(src), "missing: `"+refusal+": $agg; ") {
			t.Errorf("internal/controller/resources.go has no audit message %q", refusal)
		}
	}
}
