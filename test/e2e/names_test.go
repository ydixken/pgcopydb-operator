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
	"regexp"
	"strings"
	"testing"
)

// CNPG bootstraps both fixture clusters with this database and owning role,
// and psql inside their pods connects as the superuser.
const (
	cnpgAppDatabase = "app"
	cnpgAppRole     = "app"
	cnpgAdminRole   = "postgres"
)

// appRole is the role a Migration connects as on cluster's side.
func appRole(cluster string) string {
	if external == nil {
		return cnpgAppRole
	}
	return external.side(cluster).AppRole
}

// appDatabase is the database a Migration copies from or into on cluster's side.
func appDatabase(cluster string) string {
	if external == nil {
		return cnpgAppDatabase
	}
	return external.side(cluster).Database
}

// adminRole is the role the suite seeds, resets, and grants as on cluster's side.
func adminRole(cluster string) string {
	if external == nil {
		return cnpgAdminRole
	}
	return external.side(cluster).AdminRole
}

// sqlIdent quotes a role or database name for SQL text. Supplied names need not
// be plain identifiers, so it always quotes, as the operator's own hints do.
func sqlIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// sqlLiteral quotes a name for comparison with a catalog column.
func sqlLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// plainIdent matches the names PostgreSQL's quote_ident leaves bare.
var plainIdent = regexp.MustCompile(`^[a-z_][a-z0-9_$]*$`)

// quotedKeywords are the keywords quote_ident quotes (every category but
// unreserved), from PostgreSQL's kwlist.h.
var quotedKeywords = func() map[string]bool {
	m := map[string]bool{}
	for k := range strings.FieldsSeq(`all analyse analyze and any array as asc asymmetric both case cast
		check collate column constraint create current_catalog current_date current_role current_time
		current_timestamp current_user default deferrable desc distinct do else end except false fetch
		for foreign from grant group having in initially intersect into lateral leading limit localtime
		localtimestamp not null offset on only or order placing primary references returning select
		session_user some symmetric system_user table then to trailing true union unique user using
		variadic when where window with authorization binary collation concurrently cross current_schema
		freeze full ilike inner is isnull join left like natural notnull outer overlaps right similar
		tablesample verbose between bigint bit boolean char character coalesce dec decimal exists
		extract float greatest grouping inout int integer interval json json_array json_arrayagg
		json_exists json_object json_objectagg json_query json_scalar json_serialize json_table
		json_value least merge_action national nchar none normalize nullif numeric out overlay
		position precision real row setof smallint substring time timestamp treat trim values varchar
		xmlattributes xmlconcat xmlelement xmlexists xmlforest xmlnamespaces xmlparse xmlpi xmlroot
		xmlserialize xmltable`) {
		m[k] = true
	}
	return m
}()

// quoteIdentIfNeeded mirrors format('%I', name), the form the operator's
// messages carry, so an asserted message stays exact for any role name.
func quoteIdentIfNeeded(name string) string {
	if plainIdent.MatchString(name) && !quotedKeywords[name] {
		return name
	}
	return sqlIdent(name)
}

// asSourceAppRole prefixes source statements whose objects the migration role must own.
func asSourceAppRole() string {
	return "SET ROLE " + sqlIdent(appRole(sourceCluster)) + "; "
}

// withExternal runs one test in external mode and restores the previous mode.
func withExternal(t *testing.T, c *externalConfig) {
	t.Helper()
	previous := external
	external = c
	t.Cleanup(func() { external = previous })
}

func TestE2ENamesDefaultToCNPG(t *testing.T) {
	const app, superuser = "app", "postgres"
	for _, cluster := range []string{sourceCluster, targetCluster} {
		got := [3]string{appRole(cluster), appDatabase(cluster), adminRole(cluster)}
		if want := [3]string{app, app, superuser}; got != want {
			t.Errorf("%s names = %q, want %q", cluster, got, want)
		}
	}
	if got := asSourceAppRole(); got != `SET ROLE "app"; ` {
		t.Errorf("asSourceAppRole() = %q", got)
	}
}

func TestE2ENamesFollowTheExternalSide(t *testing.T) {
	withExternal(t, &externalConfig{
		Source: externalSide{Database: "orders", AppRole: "orders_rw", AdminRole: "orders_admin"},
		Target: externalSide{Database: "orders_v2", AppRole: `Orders "Writer"`, AdminRole: "platform_admin"},
	})
	for cluster, want := range map[string][3]string{
		sourceCluster: {"orders_rw", "orders", "orders_admin"},
		targetCluster: {`Orders "Writer"`, "orders_v2", "platform_admin"},
	} {
		if got := [3]string{appRole(cluster), appDatabase(cluster), adminRole(cluster)}; got != want {
			t.Errorf("%s names = %q, want %q", cluster, got, want)
		}
	}
	if got := asSourceAppRole(); got != `SET ROLE "orders_rw"; ` {
		t.Errorf("asSourceAppRole() = %q", got)
	}
}

func TestE2ENamesRejectAClusterWithoutAnExternalSide(t *testing.T) {
	withExternal(t, &externalConfig{})
	defer func() {
		if recover() == nil {
			t.Error("appRole resolved a cluster no external database stands in for")
		}
	}()
	appRole("e2e-progress-pool-source")
}

func TestE2ENamesQuoteForSQL(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{sqlIdent("app"), `"app"`},
		{sqlIdent(`Orders "Writer"`), `"Orders ""Writer"""`},
		{sqlLiteral("app"), `'app'`},
		{sqlLiteral("o'brien"), `'o''brien'`},
	} {
		if tc.got != tc.want {
			t.Errorf("quoted = %s, want %s", tc.got, tc.want)
		}
	}
}

func TestE2ENamesQuoteIdentIfNeeded(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"app", "app"},
		{"_app$1", "_app$1"},
		{"snake_case_1", "snake_case_1"},
		{"user", `"user"`},
		{"authorization", `"authorization"`},
		{"Orders", `"Orders"`},
		{"1app", `"1app"`},
		{"my-role", `"my-role"`},
		{`say "hi"`, `"say ""hi"""`},
		{"", `""`},
	} {
		if got := quoteIdentIfNeeded(tc.name); got != tc.want {
			t.Errorf("quoteIdentIfNeeded(%q) = %s, want %s", tc.name, got, tc.want)
		}
	}
}
