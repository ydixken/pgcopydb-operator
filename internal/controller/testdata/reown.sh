set -u
reown() { psql "$PGCOPYDB_TARGET_PGURI" -XAtq -v ON_ERROR_STOP=1 -v owner="$REOWN_OWNER" -f -; }
die() { printf '%s\n' "$1"; exit 1; }
REOWN_CANDIDATES_CTE=$(cat <<'REOWN_CTE'
WITH me AS (
  SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = current_user
),
-- classid matters: an OID is unique within a catalog, not across catalogs.
ext_member AS (
  SELECT d.classid, d.objid FROM pg_catalog.pg_depend d WHERE d.deptype = 'e'
),
-- A serial (a) or identity (i) sequence cannot change owner on its own:
-- ATExecChangeOwner raises "cannot change owner of sequence". It follows its
-- table instead, which also covers the sequence of an extension-member
-- table, which carries no deptype e row of its own.
-- refobjsubid > 0 keeps this to a dependency on a column. A partition
-- depends on its parent with refobjsubid = 0 and must stay a candidate.
owned_sequence AS (
  SELECT d.objid
    FROM pg_catalog.pg_depend d
    JOIN pg_catalog.pg_class s ON s.oid = d.objid AND s.relkind = 'S'
   WHERE d.classid = 'pg_catalog.pg_class'::regclass
     AND d.refclassid = 'pg_catalog.pg_class'::regclass
     AND d.refobjsubid > 0
     AND d.deptype IN ('a', 'i')
),
-- !~ '^pg_' also excludes pg_temp_N and pg_toast_temp_N, and unlike a LIKE
-- pattern it needs no backslash escape.
user_schema AS (
  SELECT n.oid, n.nspname, n.nspowner
    FROM pg_catalog.pg_namespace n
   WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
),
transferred_schema AS (
  SELECT n.oid, n.nspname
    FROM user_schema n, me
   WHERE n.nspowner = me.oid
     AND n.oid >= 16384
     AND NOT EXISTS (SELECT 1 FROM ext_member e
                      WHERE e.classid = 'pg_catalog.pg_namespace'::regclass
                        AND e.objid = n.oid)
),
candidates AS (
  SELECT 1 AS sort, 'schema' AS kind, s.oid AS nspoid,
         format('ALTER SCHEMA %I OWNER TO %I', s.nspname, :'owner') AS stmt
    FROM transferred_schema s
  UNION ALL
  -- ALTER TABLE OWNER never recurses, so every partition is its own row.
  SELECT 2, 'relation', n.oid,
         format('ALTER %s %s OWNER TO %I',
                CASE c.relkind WHEN 'S' THEN 'SEQUENCE'
                               WHEN 'v' THEN 'VIEW'
                               WHEN 'm' THEN 'MATERIALIZED VIEW'
                               WHEN 'f' THEN 'FOREIGN TABLE'
                               ELSE 'TABLE' END,
                c.oid::regclass, :'owner')
    FROM pg_catalog.pg_class c
    JOIN user_schema n ON n.oid = c.relnamespace, me
   WHERE c.relowner = me.oid AND c.oid >= 16384
     AND c.relkind IN ('r', 'p', 'S', 'v', 'm', 'f')
     AND NOT EXISTS (SELECT 1 FROM ext_member e
                      WHERE e.classid = 'pg_catalog.pg_class'::regclass AND e.objid = c.oid)
     AND NOT EXISTS (SELECT 1 FROM owned_sequence os WHERE os.objid = c.oid)
  UNION ALL
  -- ALTER ROUTINE resolves functions, procedures and aggregates alike, the
  -- (*) and ordered-set aggregate forms included: regprocedure lists every
  -- argument type, which is the lookup key for all of them.
  SELECT 3, 'routine', n.oid,
         format('ALTER ROUTINE %s OWNER TO %I', p.oid::regprocedure, :'owner')
    FROM pg_catalog.pg_proc p
    JOIN user_schema n ON n.oid = p.pronamespace, me
   WHERE p.proowner = me.oid AND p.oid >= 16384
     AND NOT EXISTS (SELECT 1 FROM ext_member e
                      WHERE e.classid = 'pg_catalog.pg_proc'::regclass AND e.objid = p.oid)
  UNION ALL
  SELECT 4, 'type', n.oid,
         format('ALTER %s %s OWNER TO %I',
                CASE WHEN t.typtype = 'd' THEN 'DOMAIN' ELSE 'TYPE' END,
                t.oid::regtype, :'owner')
    FROM pg_catalog.pg_type t
    JOIN user_schema n ON n.oid = t.typnamespace, me
   WHERE t.typowner = me.oid AND t.oid >= 16384
     -- typrelid = 0 is a non-composite type. Relkind c is a standalone
     -- composite (CREATE TYPE AS); any other relkind is a row type, which
     -- changes owner with its relation and must not get an ALTER of its own.
     AND (t.typrelid = 0
          OR (SELECT c.relkind FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid) = 'c')
     -- An array type follows its element type.
     AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type e WHERE e.typarray = t.oid)
     -- A multirange follows its range type from PostgreSQL 17 on, which
     -- refuses to alter it directly; 14 to 16 leave it behind and need the
     -- explicit statement (both verified live, TestReownCandidateQueries).
     AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_range r
                      WHERE r.rngmultitypid = t.oid
                        AND current_setting('server_version_num')::int >= 170000)
     AND NOT EXISTS (SELECT 1 FROM ext_member e
                      WHERE e.classid = 'pg_catalog.pg_type'::regclass AND e.objid = t.oid)
)
REOWN_CTE
)
same=$(reown <<'SQL'
SELECT (current_user::text = :'owner')::int;
SQL
) || die "reown: resolving the migration role on the target failed"
if [ "$same" = 1 ]; then
  echo "ok: ownership already held by the migration role, nothing to hand over"
  exit 0
fi
exists=$(reown <<'SQL'
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'owner')::int;
SQL
) || die "reown: probing the target for the role failed"
[ "$exists" = 1 ] || die "reown: role \"$REOWN_OWNER\" does not exist on the target: create it there, or set clone.ownerAfterRestore to an existing role (see docs/troubleshooting.md)"
super=$(reown <<'SQL'
SELECT rolsuper::int FROM pg_catalog.pg_roles WHERE rolname = current_user;
SQL
) || die "reown: probing the superuser attribute of the migration role failed"
if [ "$super" != 1 ]; then
  grant=$(reown <<SQL
$REOWN_CANDIDATES_CTE
SELECT format('GRANT CREATE ON DATABASE %I TO %I', current_database(), current_user)
 WHERE EXISTS (SELECT 1 FROM candidates c WHERE c.sort = 1)
   AND NOT pg_catalog.has_database_privilege(current_user, current_database(), 'CREATE');
SQL
) || die "reown: probing CREATE on the target database failed"
  [ -z "$grant" ] || die "reown: the migration role lacks CREATE on the target database, which ALTER SCHEMA OWNER needs; run on the target: $grant"
  grants=$(reown <<SQL
$REOWN_CANDIDATES_CTE
SELECT string_agg(format('GRANT CREATE ON SCHEMA %I TO %I', n.nspname, :'owner'), '; ' ORDER BY n.nspname)
  FROM user_schema n
 WHERE n.oid IN (SELECT c.nspoid FROM candidates c WHERE c.sort > 1)
   AND n.oid NOT IN (SELECT t.oid FROM transferred_schema t)
   AND NOT pg_catalog.has_schema_privilege(:'owner'::name, n.oid, 'CREATE');
SQL
) || die "reown: probing CREATE on the schemas holding objects to hand over failed"
  [ -z "$grants" ] || die "reown: role \"$REOWN_OWNER\" lacks CREATE on schemas it would own objects in, which ALTER OWNER needs; run on the target: $grants"
fi
reown <<SQL || die "reown: counting the objects to hand over failed"
$REOWN_CANDIDATES_CTE
SELECT format('reown: %s %s statement(s)', count(*), kind) FROM candidates GROUP BY sort, kind ORDER BY sort;
SQL
echo "reown: statements (first 200):"
reown <<SQL || die "reown: listing the objects to hand over failed"
$REOWN_CANDIDATES_CTE
SELECT stmt FROM candidates ORDER BY sort, stmt LIMIT 200;
SQL
reown <<SQL || die "reown: a statement failed (psql stops at the first error, see above); statements already applied stay applied, and a rerun picks up the rest"
$REOWN_CANDIDATES_CTE
SELECT stmt FROM candidates ORDER BY sort, stmt \gexec
SQL
left=$(reown <<SQL
$REOWN_CANDIDATES_CTE
SELECT stmt FROM candidates ORDER BY sort, stmt LIMIT 20;
SQL
) || die "reown: re-checking ownership after the handover failed"
if [ -n "$left" ]; then
  printf '%s\n' "$left"
  die "reown: the objects above are still owned by the migration role after the handover"
fi
echo "ok: ownership handed over to \"$REOWN_OWNER\""
