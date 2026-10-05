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

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	okRLSAudit    = "ok: row-level security audit"
	rlsProbe      = "relrowsecurity"
	emptyScope    = `{"include":[],"exclude":[],"skipData":[]}`
	pfRLS         = "pf_rls"
	pfReader      = "pf_reader"
	pfOtherForced = "pf_other.forced"
)

func TestTableScope(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    *v1beta1.Filters
		want string
	}{
		{"no filters", nil, emptyScope},
		{"include schemas and tables", &v1beta1.Filters{
			IncludeOnlySchemas: []string{testSchemaInc}, IncludeOnlyTables: []string{"app.orders"},
		}, `{"include":[["sales",null],["app","orders"]],"exclude":[],"skipData":[]}`},
		{"a pattern include widens to everything", &v1beta1.Filters{
			IncludeOnlySchemas: []string{testSchemaInc}, IncludeOnlyTables: []string{"app.orders", "~/^x/.t"},
		}, emptyScope},
		{"a quoted include widens", &v1beta1.Filters{IncludeOnlyTables: []string{`"App".orders`}},
			emptyScope},
		{"an unqualified include widens", &v1beta1.Filters{IncludeOnlyTables: []string{"orders"}},
			emptyScope},
		{"unresolvable excludes are ignored", &v1beta1.Filters{
			ExcludeSchemas: []string{"scratch", "~/tmp_/"},
			ExcludeTables:  []string{"app.audit", `"App".x`, "~/a/.b", "bare"},
		}, `{"include":[],"exclude":[["scratch",null],["app","audit"]],"skipData":[]}`},
		{"table data exclusions", &v1beta1.Filters{ExcludeTableData: []string{"app.blobs", "~/x/.y"}},
			`{"include":[],"exclude":[],"skipData":[["app","blobs"]]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tableScope(tc.f); got != tc.want {
				t.Fatalf("tableScope = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestPreflightScriptFor_TableAudits pins which Migrations get the audit:
// every single-database Migration, never all-databases (both sides are superusers there).
func TestPreflightScriptFor_TableAudits(t *testing.T) {
	for _, m := range []*v1beta1.Migration{passwordMigration(), superMigration()} {
		if s := preflightScriptFor(m); !strings.Contains(s, rlsProbe) {
			t.Fatalf("script must audit RLS:\n%s", s)
		}
	}
	all := passwordMigration()
	all.Spec.Clone.AllDatabases = true
	if s := preflightScriptFor(all); strings.Contains(s, rlsProbe) {
		t.Fatalf("all-databases script must not audit RLS:\n%s", s)
	}
	// The queries ride inside a double-quoted shell word.
	if strings.ContainsAny(rlsAuditQuery, "\"$`\\") {
		t.Fatalf("query carries a shell-active character: %s", rlsAuditQuery)
	}
	m := passwordMigration()
	m.Spec.Clone.Filters = &v1beta1.Filters{ExcludeTables: []string{"app.audit"}}
	job, err := buildPreflightJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	if got := envValue(job.Spec.Template.Spec.Containers[0].Env, tableScopeEnv); got != tableScope(m.Spec.Clone.Filters) {
		t.Fatalf("%s = %q", tableScopeEnv, got)
	}
}

func TestPreflightScript_RowLevelSecurityAudit(t *testing.T) {
	run := clonePreflightHarness(t)
	m := passwordMigration()
	t.Run("passes when nothing hides rows", func(t *testing.T) {
		out, code, _, _ := run(t, m)
		if code != 0 || !strings.Contains(out, okRLSAudit) {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
	t.Run("fails naming the tables and the remedies", func(t *testing.T) {
		out, code, _, _ := run(t, m, "PSQL_RLS_TABLES=app.docs, app.notes")
		if code != 1 || strings.Contains(out, okRLSAudit) {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		for _, want := range []string{"app.docs, app.notes", "BYPASSRLS", "disable row-level security", "clone.filters"} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing %q:\n%s", want, out)
			}
		}
	})
	t.Run("a failed probe fails closed", func(t *testing.T) {
		out, code, _, _ := run(t, m, "PSQL_FAIL_SUBSTR="+rlsProbe)
		if code != 1 || !strings.Contains(out, "probing row-level security on the source tables failed") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
}

// TestPreflightTableAuditQueries runs the shipped queries against a live server
// with the scope JSON tableScope renders, as each role the audit distinguishes.
func TestPreflightTableAuditQueries(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Fatal("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for preflight table audit SQL regressions")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	const fixture = `CREATE ROLE pf_owner;
CREATE ROLE pf_member IN ROLE pf_owner;
CREATE ROLE pf_reader;
CREATE ROLE pf_bypass BYPASSRLS;
CREATE SCHEMA pf_rls; CREATE SCHEMA pf_leaf; CREATE SCHEMA pf_other;
CREATE TABLE pf_rls.forced (id int);
ALTER TABLE pf_rls.forced ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY;
CREATE POLICY p ON pf_rls.forced FOR SELECT USING (id < 10);
CREATE TABLE pf_rls.plain (id int);
ALTER TABLE pf_rls.plain ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON pf_rls.plain FOR SELECT USING (id < 10);
CREATE TABLE pf_rls.nopolicy (id int);
ALTER TABLE pf_rls.nopolicy ENABLE ROW LEVEL SECURITY;
CREATE TABLE pf_rls.open (id int);
CREATE TABLE pf_rls.parted (k int) PARTITION BY LIST (k);
CREATE TABLE pf_leaf.part1 PARTITION OF pf_rls.parted FOR VALUES IN (1);
ALTER TABLE pf_leaf.part1 ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY;
CREATE TABLE pf_other.forced (id int);
ALTER TABLE pf_other.forced ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY;
DO $do$ DECLARE r regclass; BEGIN
  FOR r IN SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('pf_rls', 'pf_leaf', 'pf_other') AND c.relkind IN ('r', 'p') LOOP
    EXECUTE format('ALTER TABLE %s OWNER TO pf_owner', r);
  END LOOP;
END $do$;
`
	inRLS := &v1beta1.Filters{IncludeOnlySchemas: []string{pfRLS}}
	for _, tc := range []struct {
		name, role, query string
		f                 *v1beta1.Filters
		want              string
	}{
		{"owner: FORCE only, a leaf through its root", "pf_owner", rlsAuditQuery, inRLS, "pf_leaf.part1, pf_rls.forced"},
		{"inherited owner membership counts as owner", "pf_member", rlsAuditQuery, inRLS, "pf_leaf.part1, pf_rls.forced"},
		{"non-owner: every RLS table, policies or not", pfReader, rlsAuditQuery, inRLS,
			"pf_leaf.part1, pf_rls.forced, pf_rls.nopolicy, pf_rls.plain"},
		{"BYPASSRLS is exempt", "pf_bypass", rlsAuditQuery, inRLS, ""},
		{"superuser is exempt", "", rlsAuditQuery, inRLS, ""},
		{"include scopes to its schema", pfReader, rlsAuditQuery,
			&v1beta1.Filters{IncludeOnlySchemas: []string{"pf_other"}}, pfOtherForced},
		{"include tables scope to those tables", pfReader, rlsAuditQuery,
			&v1beta1.Filters{IncludeOnlyTables: []string{"pf_rls.plain", pfOtherForced}}, "pf_other.forced, pf_rls.plain"},
		{"excluded tables and skipped data leave the scope", pfReader, rlsAuditQuery, &v1beta1.Filters{
			IncludeOnlySchemas: []string{pfRLS}, ExcludeTables: []string{"pf_rls.forced", "pf_leaf.part1"},
			ExcludeTableData: []string{"pf_rls.plain"},
		}, "pf_rls.nopolicy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRole := ""
			if tc.role != "" {
				setRole = "SET ROLE " + tc.role + ";\n"
			}
			if got := auditQuery(t, uri, fixture+setRole, tc.query, tableScope(tc.f)); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	t.Run("no filters audit every user table", func(t *testing.T) {
		got := auditQuery(t, uri, fixture+"SET ROLE pf_reader;\n", rlsAuditQuery, tableScope(nil))
		for _, want := range []string{pfOtherForced, "pf_rls.forced", "pf_leaf.part1"} {
			if !strings.Contains(got, want) {
				t.Fatalf("unfiltered audit missed %s: %q", want, got)
			}
		}
	})
}

func auditQuery(t *testing.T, uri, setup, query, scope string) string {
	t.Helper()
	cmd := exec.Command("psql", uri, "-XAtq", "-v", "ON_ERROR_STOP=1", "-v", "list="+scope, "-f", "-")
	cmd.Stdin = strings.NewReader("BEGIN;\n" + setup + query + ";\nRESET ROLE; ROLLBACK;\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("audit query: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}
