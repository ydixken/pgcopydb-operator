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
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

const (
	dataFiltersPath   = "/etc/pgcopydb/conf/data-filters.ini"
	dataCompareDir    = "/tmp/pgcopydb-data"
	unloggedCondition = "UnsynchronizedTables"
)

var exactSchema = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
var exactTable = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}\.[a-z_][a-z0-9_]{0,62}$`)

func hasPartialTables(m *v1beta1.Migration) bool {
	return len(pgcopydb.ManagedPublications(&m.Spec, m.Namespace, m.Name)) > 0
}

func validatePartialTables(m *v1beta1.Migration) error {
	if !hasPartialTables(m) {
		return nil
	}
	f := m.Spec.Follow
	if !f.Enabled || (f.Plugin != "" && f.Plugin != "pgoutput") || f.Publication != "" || f.SlotName != "" ||
		m.Spec.Clone.AllDatabases || (m.Spec.Engine != "" && m.Spec.Engine != v1beta1.MigrationEnginePGCopyDB) {
		return fmt.Errorf("partial table policies require single-database pgcopydb pgoutput follow with an operator-managed slot and publication")
	}
	if err := validatePartialSelection(f); err != nil {
		return err
	}
	return validatePartialFilters(m)
}

func validatePartialSelection(f *v1beta1.FollowOptions) error {
	if len(f.UnloggedTables) > 64 {
		return fmt.Errorf("unloggedTables supports at most 64 tables")
	}
	seen := map[string]bool{}
	for _, table := range f.UnloggedTables {
		if !exactTable.MatchString(table.Table) || seen[table.Table] ||
			(table.Mode != "SchemaOnly" && table.Mode != "CopyOnce") {
			return fmt.Errorf("unloggedTables needs unique exact lowercase schema.table names and modes SchemaOnly or CopyOnce")
		}
		seen[table.Table] = true
	}
	if len(f.InsertOnlyTables) > 64 {
		return fmt.Errorf("insertOnlyTables supports at most 64 tables")
	}
	for _, table := range f.InsertOnlyTables {
		if !exactTable.MatchString(table) || seen[table] {
			return fmt.Errorf("insertOnlyTables needs unique exact lowercase schema.table names disjoint from unloggedTables")
		}
		seen[table] = true
	}
	return nil
}

func validatePartialFilters(m *v1beta1.Migration) error {
	f := m.Spec.Follow
	if filters := m.Spec.Clone.Filters; filters != nil {
		for _, group := range [][]string{filters.IncludeOnlySchemas, filters.ExcludeSchemas} {
			for _, entry := range group {
				if !exactSchema.MatchString(entry) {
					return fmt.Errorf("partial table policies require exact lowercase schema filters")
				}
			}
		}
		for _, group := range [][]string{filters.IncludeOnlyTables, filters.ExcludeTables, filters.ExcludeTableData} {
			for _, entry := range group {
				if !exactTable.MatchString(entry) {
					return fmt.Errorf("partial table policies require exact lowercase schema-qualified table filters")
				}
			}
		}
		for _, table := range f.UnloggedTables {
			if table.Mode == "CopyOnce" && slices.Contains(filters.ExcludeTableData, table.Table) {
				return fmt.Errorf("CopyOnce conflicts with excludeTableData for %s", table.Table)
			}
		}
		for _, table := range f.InsertOnlyTables {
			if slices.Contains(filters.ExcludeTableData, table) {
				return fmt.Errorf("insertOnlyTables conflicts with excludeTableData for %s", table)
			}
		}
		if len(filters.IncludeOnlyTables) > 0 && len(pgcopydb.DataFilters(&m.Spec).IncludeOnlyTables) == 0 {
			return fmt.Errorf("partial table policies require at least one included table to follow and verify")
		}
	}
	return nil
}

func migrationTableScope(m *v1beta1.Migration) string {
	scope := tableScope(pgcopydb.CloneFilters(&m.Spec))
	if !hasPartialTables(m) {
		return scope
	}
	// This type contains only strings, so JSON encoding cannot fail.
	tables, _ := json.Marshal(append([]v1beta1.UnloggedTable{}, m.Spec.Follow.UnloggedTables...))
	inserts, _ := json.Marshal(append([]string{}, m.Spec.Follow.InsertOnlyTables...))
	return strings.TrimSuffix(scope, "}") + `,"unlogged":` + string(tables) + `,"insertOnly":` + string(inserts) + "}"
}

func selectedTablesQuery() string {
	return `select c.oid, format('%I.%I', n.nspname, c.relname) as name,
c.relkind, c.relpersistence, c.relispartition ` + tableScopeFromQuery(copyScope) + `c.relkind = 'r'`
}

// The same read-only checks run in preflight and again before publication setup.
var unloggedPolicyQuery = `with selected as (` + selectedTablesQuery() + `),
requested as (select value->>'table' as name from jsonb_array_elements(:'list'::jsonb->'unlogged')),
insert_only as (select jsonb_array_elements_text(:'list'::jsonb->'insertOnly') as name),
problems as (
select 'partial table policies require PostgreSQL 15 or later on the source' as problem
where current_setting('server_version_num')::int < 150000
union all
select r.name || ': expected an included ordinary non-partitioned UNLOGGED table outside extensions'
from requested r left join selected c on c.name = r.name
where c.oid is null or c.relkind <> 'r' or c.relpersistence <> 'u' or c.relispartition
or exists (select 1 from pg_depend d where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e')
union all
select r.name || ': expected an included ordinary non-partitioned LOGGED table outside extensions'
from insert_only r left join selected c on c.name = r.name
where c.oid is null or c.relkind <> 'r' or c.relpersistence <> 'p' or c.relispartition
or exists (select 1 from pg_depend d where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e')
union all
select r.name || ': publication ' || p.pubname || ' still publishes UPDATE or DELETE'
from insert_only r join pg_publication_tables pt on pt.schemaname || '.' || pt.tablename = r.name
join pg_publication p on p.pubname = pt.pubname where p.pubupdate or p.pubdelete
union all
select 'at least one LOGGED table must remain in the follow selection'
where not exists (select 1 from selected c where relpersistence = 'p'
and not exists (select 1 from insert_only i where i.name = c.name)
and not exists (select 1 from jsonb_array_elements(:'list'::jsonb->'skipData') e(v)
  where v->>0 || '.' || (v->>1) = c.name)
and not exists (select 1 from pg_depend d where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e'))
)
select string_agg(problem, ', ' order by problem) as problem from problems`

func unloggedPreflightBlock(m *v1beta1.Migration) string {
	if !hasPartialTables(m) {
		return unloggedAuditBlock
	}
	remaining := tableScopeQuery(copyScope) + `c.relkind in ('r', 'p') and c.relpersistence = 'u'
and not exists (select 1 from jsonb_array_elements(s.f->'unlogged') u where u->>'table' = n.nspname || '.' || c.relname)`
	return remAggBlock(remAggregate{
		query:   fmt.Sprintf(tableAuditCmd, unloggedPolicyQuery),
		ok:      "explicit partial table policy",
		missing: "preflight: invalid partial table policy: $agg",
		onProbe: "preflight: probing the partial table policy failed",
	}) + "\n" + remAggBlock(remAggregate{
		query:   fmt.Sprintf(tableAuditCmd, remaining),
		ok:      "unlogged table audit",
		missing: "preflight: follow cannot replicate these unacknowledged UNLOGGED tables: $agg; set follow.unloggedTables explicitly or leave them out with clone.filters",
		onProbe: "preflight: probing the source for unlogged tables failed",
	})
}

func publicationMarker(m *v1beta1.Migration) string {
	return "pgcopydb-operator migration " + string(m.UID)
}

func sqlLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// pgcopydb treats --publication as externally managed. The operator owns this
// publication instead, identified by the immutable Kubernetes UID in its comment.
func managedPublicationSetup(m *v1beta1.Migration, fresh bool) string {
	if !hasPartialTables(m) {
		return ""
	}
	slot := effectiveSlotName(m)
	freshGuard := ""
	if fresh {
		freshGuard = `IF slot_exists THEN RAISE EXCEPTION 'UNLOGGED publication: a fresh copy cannot reuse an existing slot'; END IF;`
	}
	return `psql "$PGCOPYDB_SOURCE_PGURI" -Xq -v ON_ERROR_STOP=1 -v list=` + sqlLiteral(migrationTableScope(m)) + ` <<'UNLOGGED_PUBLICATION'
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
CREATE TEMP TABLE selected_tables ON COMMIT DROP AS ` + selectedTablesQuery() + `;
CREATE TEMP TABLE policy_problems ON COMMIT DROP AS ` + unloggedPolicyQuery + `;
CREATE TEMP TABLE unlogged_tables ON COMMIT DROP AS
  SELECT value->>'table' AS name FROM jsonb_array_elements(:'list'::jsonb->'unlogged');
CREATE TEMP TABLE insert_only_tables ON COMMIT DROP AS
  SELECT jsonb_array_elements_text(:'list'::jsonb->'insertOnly') AS name;
DO $unlogged_publication$
BEGIN
  IF EXISTS (SELECT 1 FROM policy_problems WHERE problem IS NOT NULL) THEN
    RAISE EXCEPTION 'partial-table publication: invalid table policy';
  END IF;
  IF EXISTS (SELECT 1 FROM selected_tables c WHERE c.relpersistence = 'u'
    AND NOT EXISTS (SELECT 1 FROM unlogged_tables u WHERE u.name = c.name)) THEN
    RAISE EXCEPTION 'UNLOGGED publication: an UNLOGGED table lacks an explicit policy';
  END IF;
END;
$unlogged_publication$;
` + managedPublicationSQL(m, slot, slot, false, freshGuard) + managedInsertPublicationSQL(m, slot, freshGuard) + `
COMMIT;
UNLOGGED_PUBLICATION
`
}

func managedInsertPublicationSQL(m *v1beta1.Migration, slot, freshGuard string) string {
	names := pgcopydb.ManagedPublications(&m.Spec, m.Namespace, m.Name)
	if len(names) < 2 {
		return ""
	}
	return managedPublicationSQL(m, names[1], slot, true, freshGuard)
}

func managedPublicationSQL(m *v1beta1.Migration, publication, slot string, insertOnly bool, freshGuard string) string {
	membership := "not exists"
	options := ""
	if insertOnly {
		membership = "exists"
		options = " WITH (publish = 'insert')"
	}
	return `DO $managed_publication$
DECLARE
  publication_oid oid;
  slot_exists boolean;
  relations text;
  expected oid[];
BEGIN
  SELECT string_agg(name, ', ' ORDER BY oid), array_agg(oid ORDER BY oid)
    INTO relations, expected FROM selected_tables c WHERE c.relpersistence = 'p'
    AND ` + membership + ` (SELECT 1 FROM insert_only_tables i WHERE i.name = c.name);
  SELECT oid INTO publication_oid FROM pg_publication WHERE pubname = '` + publication + `';
  SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '` + slot + `') INTO slot_exists;
  ` + freshGuard + `
  IF publication_oid IS NOT NULL AND obj_description(publication_oid, 'pg_publication') IS DISTINCT FROM ` + sqlLiteral(publicationMarker(m)) + ` THEN
    RAISE EXCEPTION 'UNLOGGED publication: name collision, refusing a publication not owned by this Migration UID';
  END IF;
  IF slot_exists THEN
    IF publication_oid IS NULL THEN
      RAISE EXCEPTION 'UNLOGGED publication: existing slot has no owned publication';
    END IF;
    IF (SELECT array_agg(prrelid ORDER BY prrelid) FROM pg_publication_rel WHERE prpubid = publication_oid) IS DISTINCT FROM expected
      OR EXISTS (SELECT 1 FROM pg_publication WHERE oid = publication_oid
        AND (puballtables OR pubviaroot OR NOT pubinsert
          OR pubupdate IS DISTINCT FROM ` + fmt.Sprint(!insertOnly) + `
          OR pubdelete IS DISTINCT FROM ` + fmt.Sprint(!insertOnly) + `
          OR pubtruncate IS DISTINCT FROM ` + fmt.Sprint(!insertOnly) + `))
      OR EXISTS (SELECT 1 FROM pg_publication_rel WHERE prpubid = publication_oid AND (prqual IS NOT NULL OR prattrs IS NOT NULL))
      OR EXISTS (SELECT 1 FROM pg_publication_namespace WHERE pnpubid = publication_oid) THEN
      RAISE EXCEPTION 'UNLOGGED publication: membership or publication options changed during follow';
    END IF;
  ELSE
    IF publication_oid IS NOT NULL THEN
      EXECUTE 'DROP PUBLICATION "` + publication + `"';
    END IF;
    EXECUTE 'CREATE PUBLICATION "` + publication + `" FOR TABLE ' || relations || ` + sqlLiteral(options) + `;
    EXECUTE 'COMMENT ON PUBLICATION "` + publication + `" IS ' || quote_literal(` + sqlLiteral(publicationMarker(m)) + `);
  END IF;
END;
$managed_publication$;
`
}

func managedPublicationCleanup(m *v1beta1.Migration, drop bool) string {
	var b strings.Builder
	for _, name := range pgcopydb.ManagedPublications(&m.Spec, m.Namespace, m.Name) {
		b.WriteString(managedPublicationCleanupOne(m, name, drop))
	}
	return b.String()
}

func managedPublicationCleanupOne(m *v1beta1.Migration, name string, drop bool) string {
	action := ""
	if drop {
		action = `EXECUTE 'DROP PUBLICATION "` + name + `"';`
	}
	return `psql "$PGCOPYDB_SOURCE_PGURI" -Xq -v ON_ERROR_STOP=1 <<'UNLOGGED_CLEANUP'
DO $unlogged_cleanup$
DECLARE p oid;
BEGIN
  SELECT oid INTO p FROM pg_publication WHERE pubname = '` + name + `';
  IF p IS NOT NULL THEN
    IF obj_description(p, 'pg_publication') IS DISTINCT FROM ` + sqlLiteral(publicationMarker(m)) + ` THEN
      RAISE EXCEPTION 'UNLOGGED cleanup: publication is not owned by this Migration UID';
    END IF;
    ` + action + `
  ELSIF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '` + effectiveSlotName(m) + `') THEN
    RAISE EXCEPTION 'UNLOGGED cleanup: slot exists without its owned publication';
  END IF;
END;
$unlogged_cleanup$;
UNLOGGED_CLEANUP
`
}

func dataComparisonPrelude(m *v1beta1.Migration) string {
	if !hasPartialTables(m) {
		return ""
	}
	return `echo "Partial-table policy: content verification excludes the explicitly acknowledged tables"
pgcopydb list tables --dir ` + dataCompareDir + ` --filters ` + dataFiltersPath + ` >/dev/null
`
}

func dataComparisonStrict(m *v1beta1.Migration) string {
	if !hasPartialTables(m) {
		return compareDataStrict
	}
	return strings.ReplaceAll(compareDataStrict, pgcopydb.WorkDir, dataCompareDir)
}

func unloggedPolicyMessage(m *v1beta1.Migration) string {
	var entries []string
	for _, table := range m.Spec.Follow.UnloggedTables {
		entries = append(entries, table.Table+" ("+table.Mode+")")
	}
	for _, table := range m.Spec.Follow.InsertOnlyTables {
		entries = append(entries, table+" (INSERT-only, no UPDATE/DELETE/TRUNCATE)")
	}
	return "Partial table policies, excluded from full content verification: " + strings.Join(entries, ", ")
}

func replicaIdentityAuditFor(m *v1beta1.Migration) string {
	if m.Spec.Follow == nil || len(m.Spec.Follow.InsertOnlyTables) == 0 {
		return riAuditBlock
	}

	skip := `
    if printf '%s\n' ` + sqlLiteral(strings.Join(m.Spec.Follow.InsertOnlyTables, "\n")) + ` | grep -Fxq -- "$tbl"; then
      echo "preflight: warning: INSERT-only table $tbl does not replicate UPDATE, DELETE or TRUNCATE"
      continue
    fi`
	return strings.Replace(riAuditBlock, `while IFS= read -r tbl; do`, `while IFS= read -r tbl; do`+skip, 1)
}

func partialDrainGuard(m *v1beta1.Migration) string {
	if m.Spec.Follow == nil || len(m.Spec.Follow.InsertOnlyTables) == 0 {
		return ""
	}
	return `echo "drain unproven: INSERT-only tables require exact origin progress at a nonzero endpos; matching fully replicated tables cannot prove that all INSERTs were applied. The replication slot is kept."
exit 1
`
}
