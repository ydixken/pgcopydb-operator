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
	"os"
	"os/exec"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

const partialEventsTable = "partial_policy.events"

func partialTableMigration() *v1beta1.Migration {
	m := passwordMigration()
	m.UID = types.UID("partial-policy-test")
	m.Spec.Follow = &v1beta1.FollowOptions{
		Enabled: true,
		UnloggedTables: []v1beta1.UnloggedTable{
			{Table: "partial_policy.empty", Mode: v1beta1.UnloggedSchemaOnly},
			{Table: "partial_policy.cache", Mode: v1beta1.UnloggedCopyOnce},
		},
		InsertOnlyTables: []string{partialEventsTable},
	}
	m.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlySchemas: []string{"partial_policy"}}
	return m
}

func TestPartialTableValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.Migration)
	}{
		{"disabled follow", func(m *v1beta1.Migration) { m.Spec.Follow.Enabled = false }},
		{v1beta1.PluginWal2json, func(m *v1beta1.Migration) { m.Spec.Follow.Plugin = v1beta1.PluginWal2json }},
		{"foreign publication", func(m *v1beta1.Migration) { m.Spec.Follow.Publication = "foreign_publication" }},
		{"custom slot", func(m *v1beta1.Migration) { m.Spec.Follow.SlotName = "foreign_slot" }},
		{"invalid mode", func(m *v1beta1.Migration) { m.Spec.Follow.UnloggedTables[0].Mode = "Ignore" }},
		{"pattern", func(m *v1beta1.Migration) { m.Spec.Follow.UnloggedTables[0].Table = "~/x/.y" }},
		{"overlap", func(m *v1beta1.Migration) { m.Spec.Follow.InsertOnlyTables[0] = m.Spec.Follow.UnloggedTables[0].Table }},
		{"ambiguous filters", func(m *v1beta1.Migration) { m.Spec.Clone.Filters.IncludeOnlySchemas = []string{"~/x/"} }},
		{"no fully followed table", func(m *v1beta1.Migration) {
			m.Spec.Clone.Filters.IncludeOnlyTables = []string{partialEventsTable}
		}},
		{"skipped insert-only data", func(m *v1beta1.Migration) {
			m.Spec.Follow.UnloggedTables = nil
			m.Spec.Clone.Filters.ExcludeTableData = []string{partialEventsTable}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := partialTableMigration()
			tc.change(m)
			if _, err := buildJob(m, "runner", 1); err == nil {
				t.Fatal("invalid partial policy reached the worker")
			}
		})
	}
	if err := validatePartialTables(partialTableMigration()); err != nil {
		t.Fatal(err)
	}
}

func TestPartialTableJobs(t *testing.T) {
	m := partialTableMigration()
	worker, err := buildJob(m, "runner", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(worker.Spec.Template.Spec.Containers[0].Command, "\n"), "name collision") {
		t.Fatal("worker lost its publication ownership guard")
	}
	cm := buildFiltersConfigMap(m)
	if !strings.Contains(cm.Data["filters.ini"], "[exclude-table-data]\npartial_policy.empty") ||
		!strings.Contains(cm.Data["data-filters.ini"], "partial_policy.cache") {
		t.Fatal(cm.Data)
	}
	verify, err := buildVerifyJob(m, "runner", "")
	if err != nil {
		t.Fatal(err)
	}
	script := verify.Spec.Template.Spec.Containers[0].Args[1]
	guard := strings.Index(script, "drain unproven: INSERT-only")
	fallback := strings.LastIndex(script, "if compare_data_strict")
	if guard < 0 || fallback <= guard {
		t.Fatal("partial checksums must never prove INSERT-only drain")
	}
	if !strings.Contains(replicaIdentityAuditFor(m), "INSERT-only table") ||
		replicaIdentityAuditFor(passwordMigration()) != riAuditBlock {
		t.Fatal("INSERT-only handling changed the acknowledgement-only default")
	}
	cleanup, err := buildCleanupJob(m, "runner")
	if err != nil {
		t.Fatal(err)
	}
	script = cleanup.Spec.Template.Spec.Containers[0].Args[1]
	ownership := strings.Index(script, "publication is not owned")
	clean := strings.Index(script, "pgcopydb stream cleanup")
	drop := strings.Index(script, "DROP PUBLICATION")
	if ownership < 0 || clean <= ownership || drop <= clean {
		t.Fatal("cleanup must guard ownership before dropping slots, then remove owned publications")
	}
	for _, name := range pgcopydb.ManagedPublications(&m.Spec, m.Namespace, m.Name) {
		if !strings.Contains(script, name) {
			t.Fatalf("cleanup omits %s", name)
		}
	}
}

func TestInsertOnlyDrainRequiresExactOrigin(t *testing.T) {
	for _, tc := range []struct {
		name, endpos, progress, gap, want string
		code                              int
	}{
		{"exact nonzero origin", caughtUpLSN, caughtUpLSN, "0", "drain verified: origin progress", 0},
		{"partial checksums cannot prove missing inserts", caughtUpLSN, "0/80", "128", "drain unproven: INSERT-only", 1},
		{"zero positions prove nothing", sentinel.ZeroLSN, sentinel.ZeroLSN, "0", "drain unproven: INSERT-only", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, code := runMigrationVerifyScript(t, partialTableMigration(), "", reportMatching,
				"STUB_ENDPOS="+tc.endpos, "STUB_REPLAY="+tc.endpos,
				"STUB_PROGRESS="+tc.progress, "STUB_GAP="+tc.gap)
			if code != tc.code || !strings.Contains(out, tc.want) || strings.Contains(out, verifyCompareMarker) {
				t.Fatalf("drain exit=%d, want=%d with %q and no partial checksum fallback:\n%s", code, tc.code, tc.want, out)
			}
		})
	}
}

func TestPartialTablePolicyQueries(t *testing.T) {
	const expectedOrdinary = "expected an included ordinary"
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Fatal("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance")
	}
	const fixture = `CREATE SCHEMA partial_policy;
CREATE TABLE partial_policy.items (id int PRIMARY KEY);
CREATE TABLE partial_policy.events (id int, value text);
CREATE UNLOGGED TABLE partial_policy.empty (id int);
CREATE UNLOGGED TABLE partial_policy.cache (id int);
`
	for _, tc := range []struct {
		name, extra, want string
		change            func(*v1beta1.Migration)
	}{
		{"both modes and insert-only", "", "", func(*v1beta1.Migration) {}},
		{"missing table", "", expectedOrdinary, func(m *v1beta1.Migration) {
			m.Spec.Follow.UnloggedTables[0].Table = "partial_policy.missing"
		}},
		{"logged table mislabeled", "", expectedOrdinary, func(m *v1beta1.Migration) {
			m.Spec.Follow.UnloggedTables[0].Table = "partial_policy.items"
		}},
		{"other publication rejects source updates", "CREATE PUBLICATION partial_other FOR TABLE partial_policy.events;",
			"still publishes UPDATE or DELETE", func(*v1beta1.Migration) {}},
		{"insert-only table is unlogged", "", expectedOrdinary, func(m *v1beta1.Migration) {
			m.Spec.Follow.InsertOnlyTables[0] = "partial_policy.empty"
		}},
		{"no comparable fully followed table", "", "at least one LOGGED table", func(m *v1beta1.Migration) {
			m.Spec.Clone.Filters.ExcludeTableData = []string{"partial_policy.items"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := partialTableMigration()
			tc.change(m)
			got := auditQuery(t, uri, fixture+tc.extra, unloggedPolicyQuery, migrationTableScope(m))
			if (tc.want == "" && got != "") || (tc.want != "" && !strings.Contains(got, tc.want)) {
				t.Fatalf("policy verdict %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPartialPublicationLifecycleSQL(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Fatal("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance")
	}
	m := partialTableMigration()
	run := func(script string, success bool) string {
		t.Helper()
		cmd := exec.Command("sh", "-c", "set -eu\n"+script)
		cmd.Env = append(os.Environ(), "PGCOPYDB_SOURCE_PGURI="+uri)
		out, err := cmd.CombinedOutput()
		if (err == nil) != success {
			t.Fatalf("publication lifecycle success=%t, error=%v:\n%s", success, err, out)
		}
		return string(out)
	}
	query := func(sql string) string {
		t.Helper()
		return run(`psql "$PGCOPYDB_SOURCE_PGURI" -XAtq -v ON_ERROR_STOP=1 <<'SQL'
`+sql+"\nSQL\n", true)
	}
	query(`CREATE SCHEMA partial_policy;
CREATE TABLE partial_policy.items (id int PRIMARY KEY);
CREATE TABLE partial_policy.events (id int, value text);
CREATE UNLOGGED TABLE partial_policy.empty (id int);
CREATE UNLOGGED TABLE partial_policy.cache (id int);
`)
	t.Cleanup(func() {
		query(`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ` + sqlLiteral(effectiveSlotName(m)))
		for _, name := range pgcopydb.ManagedPublications(&m.Spec, m.Namespace, m.Name) {
			query(`DROP PUBLICATION IF EXISTS "` + name + `"`)
		}
		query("DROP SCHEMA partial_policy CASCADE")
	})
	run(managedPublicationSetup(m, true), true)
	names := pgcopydb.ManagedPublications(&m.Spec, m.Namespace, m.Name)
	if got := strings.TrimSpace(query("SELECT tablename FROM pg_publication_tables WHERE pubname=" + sqlLiteral(names[0]))); got != "items" {
		t.Fatal("full publication includes a partial table:", got)
	}
	if got := strings.TrimSpace(query("SELECT pubinsert AND NOT pubupdate AND NOT pubdelete AND NOT pubtruncate FROM pg_publication WHERE pubname=" + sqlLiteral(names[1]))); got != "t" {
		t.Fatal("INSERT-only publication has unsafe actions:", got)
	}
	query("INSERT INTO partial_policy.events VALUES (1, 'initial'); UPDATE partial_policy.events SET value='changed'; DELETE FROM partial_policy.events;")
	run(managedPublicationSetup(m, false), true)
	// Physical slots exercise the same ownership guard on CI's wal_level=replica server.
	query(`SELECT pg_create_physical_replication_slot(` + sqlLiteral(effectiveSlotName(m)) + `)`)
	run(managedPublicationSetup(m, false), true)
	run(managedPublicationSetup(m, true), false)
	query(`ALTER PUBLICATION "` + names[0] + `" DROP TABLE partial_policy.items`)
	if got := run(managedPublicationSetup(m, false), false); !strings.Contains(got, "membership or publication options changed") {
		t.Fatal("resume did not reject changed membership:", got)
	}
	query(`ALTER PUBLICATION "` + names[0] + `" ADD TABLE partial_policy.items`)
	query(`ALTER PUBLICATION "` + names[0] + `" SET (publish = 'insert, update, delete')`)
	if got := run(managedPublicationSetup(m, false), false); !strings.Contains(got, "membership or publication options changed") {
		t.Fatal("resume did not reject changed actions:", got)
	}
	query(`SELECT pg_drop_replication_slot(` + sqlLiteral(effectiveSlotName(m)) + `)`)
	run(managedPublicationSetup(m, false), true)
	query(`COMMENT ON PUBLICATION "` + names[0] + `" IS 'someone else'`)
	run(managedPublicationSetup(m, false), false)
	run(managedPublicationCleanup(m, false), false)
	query(`COMMENT ON PUBLICATION "` + names[0] + `" IS ` + sqlLiteral(publicationMarker(m)))
	run(managedPublicationCleanup(m, true), true)
	run(managedPublicationCleanup(m, true), true)
}
