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
	"os"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// TestTargetFollowGrantSQL pins the per-engine target grants to docs/reference/prerequisites.md:
// pgcopydb gets EXECUTE on the origin functions, native gets it revoked, both get SET.
func TestTargetFollowGrantSQL(t *testing.T) {
	raw, err := os.ReadFile("../../docs/reference/prerequisites.md")
	if err != nil {
		t.Fatal(err)
	}
	docs := string(raw)
	const origin = "p.proname LIKE 'pg_replication_origin%'"
	srr := "GRANT SET ON PARAMETER session_replication_role TO " + sqlIdent("app")
	for engine, want := range map[v1beta1.MigrationEngine]struct{ suite, doc string }{
		v1beta1.MigrationEnginePGCopyDB: {
			"'GRANT EXECUTE ON FUNCTION %s TO %I'", "format('GRANT EXECUTE ON FUNCTION %s TO app'"},
		v1beta1.MigrationEngineRPGMigrate: {
			"'REVOKE EXECUTE ON FUNCTION %s FROM %I'", "format('REVOKE EXECUTE ON FUNCTION %s FROM app'"},
	} {
		got := targetFollowGrantSQL(engine, "app")
		if len(got) != 2 || !strings.Contains(got[0], origin) || !strings.Contains(got[0], want.suite) || got[1] != srr {
			t.Errorf("%s target grants = %q, want the origin loop with %s and %q", engine, got, want.suite, srr)
		}
		if !strings.Contains(docs, want.doc) || !strings.Contains(docs, origin) {
			t.Errorf("prerequisites.md does not document the %s origin statement %s", engine, want.doc)
		}
	}
	native := strings.Join(targetFollowGrantSQL(v1beta1.MigrationEngineRPGMigrate, "app"), "\n")
	if strings.Contains(native, "GRANT EXECUTE") {
		t.Error("native must not be granted EXECUTE on the origin functions")
	}
}
