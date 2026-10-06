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
	"crypto/md5"
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
	reasonSchemaDrift          = "SchemaDrift"
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

// checkMissingColumnFailure reports why m did not fail as SchemaDrift on its
// first attempt, on the column the target lacks (42703) that attemptLog shows.
func checkMissingColumnFailure(m *v1beta1.Migration, attemptLog string) error {
	failed := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
	switch {
	case failed == nil || failed.Reason != reasonSchemaDrift:
		return fmt.Errorf("want Failed reason %s, got %+v", reasonSchemaDrift, failed)
	case m.Status.Attempts != 1:
		return fmt.Errorf("want the first attempt to fail for good, got %d attempts", m.Status.Attempts)
	case !strings.Contains(failed.Message, "[42703]"):
		return fmt.Errorf("message does not quote the missing column (42703):\n%s", failed.Message)
	case !strings.Contains(attemptLog, "[42703]") || !strings.Contains(attemptLog, "does not exist"):
		return fmt.Errorf("attempt 1's log shows no missing column (42703):\n%s", attemptLog)
	}
	return nil
}

// checkLargeObjectsNotReplicated reports why diffs are not exactly the
// large-object changes limitations/large_object_change.sql makes during follow,
// with each object's base-copy content on the target.
func checkLargeObjectsNotReplicated(diffs []fingerprintDiff) error {
	contentHash := func(content string) string { return fmt.Sprintf("%x", md5.Sum([]byte(content))) }
	// Per object: the content hash on the source, then on the target.
	want := map[string][2]string{
		coverageSchema + " blob1": {contentHash("PATCHED" + strings.Repeat("x", 93)), contentHash(strings.Repeat("x", 100))},
		coverageSchema + " blob2": {fingerprintMissingObject, contentHash(strings.Repeat("x", 200))},
		coverageSchema + " live":  {contentHash("created during follow"), fingerprintMissingObject},
	}
	if len(diffs) != len(want) {
		return fmt.Errorf("want %d large object differences, got %d: %v", len(want), len(diffs), diffs)
	}
	for _, d := range diffs {
		hashes, ok := want[d.key]
		source, _, _ := strings.Cut(d.source, " ")
		target, _, _ := strings.Cut(d.target, " ")
		if d.aspect != largeObjectAspect || !ok || source != hashes[0] || target != hashes[1] {
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
	// A worker log line, as PGCOPYDB_LOG_JSON=on writes it.
	logLine := func(msg string) string { return `{"error_severity":"ERROR","message":"[TARGET 67] ` + msg + `"}` }
	failedApply := logLine(`[42703] ERROR:  column \"extra\" of relation \"t\" does not exist`)
	const drift = "attempt 1 failed on a column the target lacks retries cannot fix:" +
		` [TARGET 67] [42703] ERROR:  column "extra" of relation "t" does not exist; DDL ran on the source`
	const exhausted = "attempt 2 failed: Job has reached the specified backoff limit;" +
		" last error: follow process 4 has terminated [12]"
	const notMissingColumn = "attempt 1's log shows no missing column (42703)"
	for _, tc := range []struct {
		name    string
		m       *v1beta1.Migration
		log     string
		wantErr string
	}{
		{name: "schema drift", m: failedOwnMigration(reasonSchemaDrift, drift, 1), log: failedApply},
		{name: "budget spent", m: failedOwnMigration(reasonBackoffLimitExceeded, exhausted, 2), log: failedApply,
			wantErr: "want Failed reason SchemaDrift"},
		{name: "not failed", m: &v1beta1.Migration{}, log: failedApply, wantErr: "want Failed reason SchemaDrift"},
		{name: "after a retry", m: failedOwnMigration(reasonSchemaDrift, drift, 2), log: failedApply,
			wantErr: "got 2 attempts"},
		{name: "message without the line", m: failedOwnMigration(reasonSchemaDrift, "attempt 1 failed", 1),
			log: failedApply, wantErr: "message does not quote the missing column"},
		{name: "other error", m: failedOwnMigration(reasonSchemaDrift, drift, 1),
			log: logLine(`[57014] ERROR:  canceling statement`), wantErr: notMissingColumn},
		{name: "other relation missing", m: failedOwnMigration(reasonSchemaDrift, drift, 1),
			log: logLine(`[42P01] ERROR:  relation \"t\" does not exist`), wantErr: notMissingColumn},
		{name: "42703 not a missing column", m: failedOwnMigration(reasonSchemaDrift, drift, 1),
			log: logLine(`[42703] ERROR:  column \"extra\" is unknown`), wantErr: notMissingColumn},
		{name: "no log", m: failedOwnMigration(reasonSchemaDrift, drift, 1), wantErr: notMissingColumn},
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
	// fingerprint.sql's value: the content's md5, then owner and grants.
	value := func(content string) string { return fmt.Sprintf("%x owner=app acl=", md5.Sum([]byte(content))) }
	setup1, setup2 := value(strings.Repeat("x", 100)), value(strings.Repeat("x", 200))
	patched, live := value("PATCHED"+strings.Repeat("x", 93)), value("created during follow")
	const missing = fingerprintMissingObject
	pinned := []fingerprintDiff{
		lo("blob1", patched, setup1),
		lo("blob2", missing, setup2),
		lo("live", live, missing),
	}
	if err := checkLargeObjectsNotReplicated(pinned); err != nil {
		t.Fatal(err)
	}
	for name, diffs := range map[string][]fingerprintDiff{
		"replicated":        nil,
		"patch replicated":  pinned[1:],
		"unlink replicated": {pinned[0], pinned[2]},
		"row lost":          {pinned[0], pinned[1], lo("live", live, fingerprintAbsent)},
		"created on target": {pinned[0], pinned[1], lo("live", live, live+"x")},
		"untouched changed": {pinned[0], pinned[1], pinned[2], lo("blob3", value("y"), value("z"))},
		"target content":    {lo("blob1", patched, setup2), pinned[1], pinned[2]},
		"patch not applied": {lo("blob1", setup2, setup1), pinned[1], pinned[2]},
		"not unlinked":      {pinned[0], lo("blob2", setup1, setup2), pinned[2]},
		"another aspect":    {pinned[0], pinned[1], {aspect: "rows", key: pinned[2].key, source: live, target: missing}},
		// fingerprint.sql writes a NULL value as '', which a key's zero hashes would match.
		"another object": {pinned[0], pinned[1], lo("blob3", "", "")},
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
