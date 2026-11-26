set -u
fail=0
fails=''
fails_audit=''
hints=''
check() { psql "$1" -XAtq -v ON_ERROR_STOP=1 -c "$2"; }
checkv() { printf '%s' "$2" | psql "$1" -XAtq -v ON_ERROR_STOP=1 -v list="$3" -f -; }
note() { printf '%s\n' "$1"; fails="$fails$1
"; fail=1; }
note_audit() { printf '%s\n' "$1"; fails_audit="$fails_audit$1
"; fail=1; }
hint() { hints="$hints$1
"; }
connect_retry() {
  n=1
  perm=0
  while :; do
    err=$(check "$1" 'select 1' 2>&1 >/dev/null); rc=$?
    [ -n "$err" ] && printf '%s\n' "$err" >&2
    [ "$rc" -eq 0 ] && break
    case "$err" in
    *'password authentication failed'*|*'role "'*'" does not exist'*|*'database "'*'" does not exist'*)
      perm=$((perm+1)) ;;
    *) perm=0 ;;
    esac
    if [ "$perm" -ge 2 ] || [ "$n" -ge 6 ]; then echo "$3${err:+: $err}"; exit 1; fi
    echo "retry: $2 connectivity attempt $n failed"
    n=$((n+1))
    sleep "${PREFLIGHT_RETRY_SLEEP:-10}"
  done
}
connect_retry "$PGCOPYDB_SOURCE_PGURI" source "preflight: cannot connect to the source database"
echo "ok: connectivity source"
connect_retry "$PGCOPYDB_TARGET_PGURI" target "preflight: cannot connect to the target database"
echo "ok: connectivity target"
pooler_check() {
  if ! pooler_out=$(LC_ALL=C psql "$2" -XAtq -v ON_ERROR_STOP=1 -c '\conninfo' -c 'select pg_backend_pid()'); then
    note "preflight: cannot tell whether the $1 connection goes through a connection pooler such as PgBouncer: the probe connection failed"
    return 0
  fi
  pooler_key=$(printf '%s\n' "$pooler_out" | sed -n 's/^Backend PID|//p')
  pooler_pid=$(printf '%s\n' "$pooler_out" | sed -n '$p')
  case "${pooler_key#-}" in
  ''|*[!0-9]*)
    note "preflight: cannot tell whether the $1 connection goes through a connection pooler such as PgBouncer: psql printed no Backend PID in \\conninfo; the runner image needs psql 18 or later"
    return 0 ;;
  esac
  case "$pooler_pid" in
  ''|*[!0-9]*)
    note "preflight: cannot tell whether the $1 connection goes through a connection pooler such as PgBouncer: select pg_backend_pid() returned no PID"
    return 0 ;;
  esac
  if [ "$pooler_key" = "$pooler_pid" ]; then
    echo "ok: no PgBouncer in the $1 connection"
  else
    note "preflight: the $1 connection goes through a connection pooler such as PgBouncer (the server's backend PID is $pooler_pid, the connection reported $pooler_key); connect directly to PostgreSQL"
  fi
}
pooler_check source "$PGCOPYDB_SOURCE_PGURI"
pooler_check target "$PGCOPYDB_TARGET_PGURI"
temp_priv=$(check "$PGCOPYDB_TARGET_PGURI" "select has_database_privilege(current_database(), 'TEMP')::int") || temp_priv=''
case "$temp_priv" in
1) ;;
0) echo "warn: the migration role lacks TEMPORARY on the target database, so the retry gate's test commit fails and holds any retry that runs it; run GRANT TEMPORARY ON DATABASE <database> TO <role> on the target" ;;
*) echo "warn: could not read whether the migration role holds TEMPORARY on the target database, which the retry gate's test commit needs" ;;
esac
restore_bin=''
restore_src=$(check "$PGCOPYDB_SOURCE_PGURI" "select current_setting('server_version_num')::int / 10000") || restore_src=''
restore_tgt=$(check "$PGCOPYDB_TARGET_PGURI" "select current_setting('server_version_num')::int / 10000") || restore_tgt=''
case "$restore_src" in ''|*[!0-9]*) restore_src='' ;; esac
case "$restore_tgt" in ''|*[!0-9]*) restore_tgt='' ;; esac
restore_gate=''
if [ -z "$restore_src" ]; then
  restore_gate="preflight: cannot read the source major version for the pg_dump check"
elif [ -z "$restore_tgt" ]; then
  restore_gate="preflight: cannot read the target major version for the pg_restore check"
else
  if [ "$restore_tgt" -le 16 ]; then PGCOPYDB_CLIENT_MAJOR=16; else PGCOPYDB_CLIENT_MAJOR=18; fi
  export PGCOPYDB_CLIENT_MAJOR
fi
if [ -n "${PG_CONFIG-}" ] && [ -f "$PG_CONFIG" ] && restore_dir=$("$PG_CONFIG" --bindir) && [ -f "$restore_dir/psql" ]; then
  restore_bin=$restore_dir/pg_restore
elif restore_psql=$(command -v psql); then
  restore_bin=${restore_psql%/*}/pg_restore
fi
restore_ver=''
[ -n "$restore_gate" ] || { [ -n "$restore_bin" ] && restore_ver=$("$restore_bin" --version) || restore_ver=''; }
restore_major=''
case "$restore_ver" in
*'
'*) ;;
'pg_restore (PostgreSQL) '*)
  restore_rest=${restore_ver#'pg_restore (PostgreSQL) '}
  case "$restore_rest" in
  [1-9].[0-9]*|[1-9]devel*|[1-9]beta[0-9]*|[1-9]rc[0-9]*|[1-9][0-9].[0-9]*|[1-9][0-9]devel*|[1-9][0-9]beta[0-9]*|[1-9][0-9]rc[0-9]*)
    restore_major=${restore_rest%%[!0-9]*} ;;
  esac ;;
esac
if [ -z "$restore_major" ]; then
  [ -n "$restore_gate" ] || restore_gate="preflight: cannot read the major version of the runner's pg_restore: ${restore_bin:-pg_restore} --version did not print one line of the form pg_restore (PostgreSQL) <major>.<minor>"
elif [ -n "$restore_gate" ]; then
  :
elif [ "$restore_major" -lt "$restore_src" ]; then
  restore_gate="preflight: the runner's pg_dump $restore_major is older than source PostgreSQL $restore_src and cannot dump it"
elif [ "$restore_major" -lt "$restore_tgt" ]; then
  restore_gate="preflight: the runner's pg_restore $restore_major is older than target PostgreSQL $restore_tgt"
elif [ "$restore_major" -ge 17 ] && [ "$restore_tgt" -lt 17 ]; then
  restore_gate="preflight: the runner's pg_restore $restore_major cannot restore into the target's PostgreSQL $restore_tgt: pg_restore 17 and later send SET transaction_timeout = 0, which PostgreSQL 16 and older reject, so every attempt fails at the pre-data restore; use a PostgreSQL 17 or later target, or a custom spec.runner.image whose pg_restore major is not newer than 16"
else
  echo "ok: runner pg_dump and pg_restore $restore_major support source major $restore_src and target major $restore_tgt"
fi
if [ -n "$restore_gate" ]; then
  note "$restore_gate"
if [ "$fail" -eq 0 ]; then
  echo "preflight: all checks passed"
else
  printf 'preflight failed:\n%s%s%s' "$fails_audit" "$fails" "$hints"
fi
exit "$fail"
fi
extension_names_valid() {
  [ -n "$1" ] || return 1
  extension_shape=$(checkv "$PGCOPYDB_TARGET_PGURI" "$(cat <<'PF_EXTENSION_VALIDATE'
SELECT CASE WHEN jsonb_typeof(:'list'::jsonb) = 'array' THEN
CASE WHEN NOT EXISTS (SELECT 1 FROM jsonb_array_elements(:'list'::jsonb) AS item WHERE jsonb_typeof(item) <> 'string')
THEN jsonb_array_length(:'list'::jsonb) ELSE -1 END ELSE -1 END;
PF_EXTENSION_VALIDATE
)" "$1" 2>/dev/null) || return 1
  case "$extension_shape" in ''|*[!0-9]*) return 1 ;; esac
}

if selected_extensions=$(checkv "$PGCOPYDB_SOURCE_PGURI" "$(cat <<'PF_EXTENSION_SOURCE'
SELECT COALESCE(
jsonb_agg(source_extension.extname ORDER BY source_extension.extname), '[]'::jsonb)
FROM pg_catalog.pg_extension AS source_extension, (SELECT :'list'::jsonb AS filters) AS input
WHERE (jsonb_array_length(filters->'include') = 0 OR (filters->'include') ? source_extension.extname::text)
AND NOT ((filters->'exclude') ? source_extension.extname::text);
PF_EXTENSION_SOURCE
)" "${PREFLIGHT_EXTENSION_FILTERS:-}" 2>/dev/null) && extension_names_valid "$selected_extensions"; then
  if extension_issues=$(checkv "$PGCOPYDB_TARGET_PGURI" "$(cat <<'PF_EXTENSION_TARGET'
SELECT COALESCE(jsonb_agg(selected.name ORDER BY selected.name), '[]'::jsonb)
FROM jsonb_array_elements_text(:'list'::jsonb) AS selected(name)
LEFT JOIN pg_catalog.pg_extension AS installed ON installed.extname::text COLLATE "C" = selected.name
LEFT JOIN pg_catalog.pg_available_extensions AS available ON available.name::text COLLATE "C" = selected.name
WHERE NOT (installed.extname IS NOT NULL OR available.default_version IS NOT NULL);
PF_EXTENSION_TARGET
)" "$selected_extensions" 2>/dev/null) && extension_names_valid "$extension_issues"; then
    if [ "$extension_shape" = 0 ]; then
      echo "ok: selected extensions available"
    else
      note "preflight: selected extensions unavailable on target: $extension_issues; install the required target extension package or choose a target that provides it"
    fi
  else
    note "preflight: target extension availability probe failed"
  fi
else
  note "preflight: source extension selection probe failed"
fi
[ "$fail" -eq 0 ] || exit 1
extension_names_valid() {
  [ -n "$1" ] || return 1
  extension_shape=$(checkv "$PGCOPYDB_TARGET_PGURI" "$(cat <<'PF_EXTENSION_VALIDATE'
SELECT CASE WHEN jsonb_typeof(:'list'::jsonb) = 'array' THEN
CASE WHEN NOT EXISTS (SELECT 1 FROM jsonb_array_elements(:'list'::jsonb) AS item WHERE jsonb_typeof(item) <> 'string')
THEN jsonb_array_length(:'list'::jsonb) ELSE -1 END ELSE -1 END;
PF_EXTENSION_VALIDATE
)" "$1" 2>/dev/null) || return 1
  case "$extension_shape" in ''|*[!0-9]*) return 1 ;; esac
}

if selected_extensions=$(checkv "$PGCOPYDB_SOURCE_PGURI" "$(cat <<'PF_EXTENSION_SOURCE'
SELECT COALESCE(
jsonb_agg(source_extension.extname ORDER BY source_extension.extname), '[]'::jsonb)
FROM pg_catalog.pg_extension AS source_extension, (SELECT :'list'::jsonb AS filters) AS input
WHERE (jsonb_array_length(filters->'include') = 0 OR (filters->'include') ? source_extension.extname::text)
AND NOT ((filters->'exclude') ? source_extension.extname::text)
AND source_extension.oid >= 16384;
PF_EXTENSION_SOURCE
)" "${PREFLIGHT_EXTENSION_FILTERS:-}" 2>/dev/null) && extension_names_valid "$selected_extensions"; then
  if extension_issues=$(checkv "$PGCOPYDB_TARGET_PGURI" "$(cat <<'PF_EXTENSION_TARGET'
SELECT COALESCE(jsonb_agg(
format('extension %I (owner %I, migration role %I)', installed.extname,
pg_get_userbyid(installed.extowner), current_user) ORDER BY selected.name), '[]'::jsonb)
FROM jsonb_array_elements_text(:'list'::jsonb) AS selected(name)
JOIN pg_catalog.pg_extension AS installed ON installed.extname::text COLLATE "C" = selected.name
WHERE NOT (pg_has_role(current_user, installed.extowner, 'USAGE')
OR (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user));
PF_EXTENSION_TARGET
)" "$selected_extensions" 2>/dev/null) && extension_names_valid "$extension_issues"; then
    if [ "$extension_shape" = 0 ]; then
      echo "ok: selected extension ownership"
    else
      note "preflight: target extension ownership required for COMMENT ON EXTENSION (dropIfExists: false): $extension_issues; use a migration role with the owner's privileges, or set clone.skip: [extensionComments]"
    fi
  else
    note "preflight: target extension ownership probe failed"
  fi
else
  note "preflight: source extension ownership selection probe failed"
fi

if [ "$fail" -ne 0 ]; then
if [ "$fail" -eq 0 ]; then
  echo "preflight: all checks passed"
else
  printf 'preflight failed:\n%s%s%s' "$fails_audit" "$fails" "$hints"
fi
exit "$fail"
fi

tgt_user=$(check "$PGCOPYDB_TARGET_PGURI" 'select current_user') || note "preflight: could not resolve the target role"
if v=$(check "$PGCOPYDB_TARGET_PGURI" "select has_database_privilege(current_user, current_database(), 'CREATE')::int"); then
  if [ "$v" != 1 ]; then
    if stmt=$(check "$PGCOPYDB_TARGET_PGURI" "select format('GRANT CREATE ON DATABASE %I TO %I', current_database(), current_user)"); then
      note "preflight: target role \"$tgt_user\" lacks CREATE on the target database: $stmt"
      hint "hint: spec.target.superuserSecretRef lets the operator apply this itself"
    else
      note "preflight: composing the database GRANT failed"
    fi
  else
    echo "ok: clone rights database"
  fi
else
  note "preflight: probing CREATE on the target database failed"
fi
if clone_schemas=$(check "$PGCOPYDB_SOURCE_PGURI" "select n.nspname from pg_namespace n where n.nspname !~ '^pg_' and n.nspname <> 'information_schema' order by 1"); then
sc_inc="${PREFLIGHT_SCHEMA_INCLUDE:-}"
sc_exc="${PREFLIGHT_SCHEMA_EXCLUDE:-}"
sc_list=''
while IFS= read -r s; do
  [ -n "$s" ] || continue
  if [ -n "$sc_inc" ] && ! printf '%s\n' "$sc_inc" | grep -Fxq -- "$s"; then continue; fi
  if printf '%s\n' "$sc_exc" | grep -Fxq -- "$s"; then continue; fi
  sc_list="$sc_list$s
"
done <<PF_SCHEMAS
$clone_schemas
PF_SCHEMAS
if [ -n "$sc_list" ]; then
if agg=$(checkv "$PGCOPYDB_TARGET_PGURI" "select string_agg(format('GRANT CREATE ON SCHEMA %I TO %I', n.nspname, current_user), '; ') from pg_namespace n where n.nspname = any(string_to_array(:'list', chr(10))) and not has_schema_privilege(current_user, n.oid, 'CREATE')" "$sc_list"); then
  if [ -n "$agg" ]; then
    note "preflight: target role \"$tgt_user\" lacks CREATE on schemas the restore targets, run on the target: $agg"
    hint "hint: spec.target.superuserSecretRef lets the operator apply this itself"
  else
    echo "ok: clone rights schemas"
  fi
else
  note "preflight: probing CREATE on the restore's target schemas failed"
fi
else
  echo "ok: clone rights schemas"
fi
else
note "preflight: listing the source schemas for the clone-rights probe failed"
fi
if v=$(check "$PGCOPYDB_TARGET_PGURI" "select (pg_has_role(current_user, (select datdba from pg_database where datname = current_database()), 'USAGE') or (select rolsuper from pg_roles where rolname = current_user))::int"); then
  if [ "$v" != 1 ]; then
    note "preflight: target role \"$tgt_user\" cannot run ALTER DATABASE ... SET (the db-properties step needs ownership): make the role a member of the owning role, or set clone.skip: [dbProperties]"
  else
    echo "ok: clone rights db-properties"
  fi
else
  note "preflight: probing database ownership for the db-properties step failed"
fi
if agg=$(checkv "$PGCOPYDB_SOURCE_PGURI" "select string_agg(format('%I.%I', n.nspname, c.relname), ', ' order by n.nspname, c.relname)
from pg_class c join pg_namespace n on n.oid = c.relnamespace,
(select :'list'::jsonb as f) s
where n.nspname !~ '^pg_' and n.nspname not in ('information_schema', 'pgcopydb')
and (jsonb_array_length(s.f->'schemas') = 0 or exists (select 1 from jsonb_array_elements(s.f->'schemas') e(v) where v->>0 = n.nspname))
and (jsonb_array_length(s.f->'tables') = 0 or exists (select 1 from jsonb_array_elements(s.f->'tables') e(v) where v->>0 = n.nspname and v->>1 = c.relname))
and not exists (select 1 from jsonb_array_elements(s.f->'exclude') e(v) where v->>0 = n.nspname and coalesce(v->>1, c.relname) = c.relname)
and c.relkind = 'r' and c.relrowsecurity
and not exists (select 1 from pg_depend d where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e')
and (c.relforcerowsecurity or not pg_has_role(current_user, c.relowner, 'USAGE'))
and not (select rolsuper or rolbypassrls from pg_roles where rolname = current_user)
and not exists (select 1 from jsonb_array_elements(s.f->'skipData') e(v) where v->>0 = n.nspname and v->>1 = c.relname)" "${PREFLIGHT_TABLE_SCOPE:-}"); then
  if [ -n "$agg" ]; then
    note "preflight: row-level security hides rows of these tables from the source migration role, so the copy would fail on them with the bundled runner, or copy and verify only the rows the role sees with a runner before 0.18.34.g7fddd6f: $agg; grant the role BYPASSRLS, disable row-level security on them for the migration, or leave them out with clone.filters"
  else
    echo "ok: row-level security audit"
  fi
else
  note "preflight: probing row-level security on the source tables failed"
fi
wal_level=$(check "$PGCOPYDB_SOURCE_PGURI" 'show wal_level')
if [ "$wal_level" != logical ]; then
  note "preflight: source wal_level is '$wal_level', follow needs 'logical': set wal_level = logical on the source and restart it"
else
  echo "ok: source wal_level logical"
fi
free_slots=$(check "$PGCOPYDB_SOURCE_PGURI" "select current_setting('max_replication_slots')::int - count(*) from pg_replication_slots")
if [ "${free_slots:-0}" -lt 1 ]; then
  note "preflight: no free replication slot on the source: raise max_replication_slots or drop an unused slot from pg_replication_slots"
else
  echo "ok: replication slot headroom"
fi
wst=$(check "$PGCOPYDB_SOURCE_PGURI" "select setting || '|' || current_setting('wal_sender_timeout') || '|' || current_setting('server_version_num')::int / 10000 from pg_settings where name = 'wal_sender_timeout'")
wst_ms=${wst%%|*}
wst_rest=${wst#*|}
wst_value=${wst_rest%%|*}
wst_uri_fix=''
if [ "${wst_rest#*|}" -ge 12 ] 2>/dev/null; then
  wst_uri_fix=', or append options=-c%20wal_sender_timeout%3D60s to the source URI query (uriSecretRef, or a secretRef DB key holding a URI)'
fi
case "$wst_ms" in
''|*[!0-9]*)
  echo "warn: could not read the source wal_sender_timeout; make sure it is 60s or more, a lower value can stall the follow stream" ;;
0)
  echo "ok: source wal_sender_timeout disabled" ;;
*)
  if [ "$wst_ms" -lt 60000 ]; then
    echo "warn: source wal_sender_timeout is $wst_value, below 60s: a receive stall that long ends the follow stream; raise it on the source server (on CloudNativePG, spec.postgresql.parameters)$wst_uri_fix"
  else
    echo "ok: source wal_sender_timeout $wst_value"
  fi ;;
esac
src_user=$(check "$PGCOPYDB_SOURCE_PGURI" 'select current_user') || note "preflight: could not resolve the source role"
if v=$(check "$PGCOPYDB_SOURCE_PGURI" 'select (rolreplication or rolsuper)::int from pg_roles where rolname = current_user'); then
  if [ "$v" != 1 ]; then
    if stmt=$(check "$PGCOPYDB_SOURCE_PGURI" "select format('ALTER ROLE \"%s\" REPLICATION', replace(current_user::text, '\"', '\"\"'))"); then
      note "preflight: source role \"$src_user\" lacks the REPLICATION attribute: $stmt"
      hint "hint: spec.source.superuserSecretRef lets the operator apply this itself"
    else
      note "preflight: composing the ALTER ROLE failed"
    fi
  else
    echo "ok: source replication attribute"
  fi
else
  note "preflight: probing the source REPLICATION attribute failed"
fi
if agg=$(check "$PGCOPYDB_TARGET_PGURI" "select string_agg(format('GRANT EXECUTE ON FUNCTION %s TO %I;', p.oid::regprocedure, current_user), ' ') from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'pg_catalog' and p.proname in ('pg_replication_origin_oid', 'pg_replication_origin_create', 'pg_replication_origin_drop', 'pg_replication_origin_session_setup', 'pg_replication_origin_xact_setup', 'pg_replication_origin_advance', 'pg_replication_origin_progress') and not has_function_privilege(current_user, p.oid, 'execute')"); then
  if [ -n "$agg" ]; then
    note "preflight: target role lacks EXECUTE on replication origin functions, run on the target: $agg"
    hint "hint: spec.target.superuserSecretRef lets the operator apply this itself"
  else
    echo "ok: target origin function grants"
  fi
else
  note "preflight: probing the origin function grants failed"
fi
tgt_user=$(check "$PGCOPYDB_TARGET_PGURI" 'select current_user') || note "preflight: could not resolve the target role"
if check "$PGCOPYDB_TARGET_PGURI" "begin; set session_replication_role = 'replica'; rollback;" >/dev/null; then
  echo "ok: target session_replication_role"
else
  if stmt=$(check "$PGCOPYDB_TARGET_PGURI" "select format('GRANT SET ON PARAMETER session_replication_role TO \"%s\"', replace(current_user::text, '\"', '\"\"'))"); then
    note "preflight: target role \"$tgt_user\" cannot SET session_replication_role, so pgcopydb would apply NOTHING while reporting success: $stmt (PostgreSQL 15+; older targets need a superuser role)"
    hint "hint: spec.target.superuserSecretRef lets the operator apply this itself"
  else
    note "preflight: composing the GRANT SET failed"
  fi
fi
if agg=$(checkv "$PGCOPYDB_SOURCE_PGURI" "select string_agg(format('%I.%I', n.nspname, c.relname), ', ' order by n.nspname, c.relname)
from pg_class c join pg_namespace n on n.oid = c.relnamespace,
(select :'list'::jsonb as f) s
where n.nspname !~ '^pg_' and n.nspname not in ('information_schema', 'pgcopydb')
and (jsonb_array_length(s.f->'publish') = 0 or exists (select 1
  from (select c.oid as relid union select relid from pg_partition_ancestors(c.oid)) a
  join pg_class ac on ac.oid = a.relid join pg_namespace an on an.oid = ac.relnamespace,
  jsonb_array_elements(s.f->'publish') e(v) where v->>0 = an.nspname and coalesce(v->>1, ac.relname) = ac.relname))
and not exists (select 1 from jsonb_array_elements(s.f->'exclude') e(v) where v->>0 = n.nspname and coalesce(v->>1, c.relname) = c.relname)
and c.relkind in ('r', 'p') and c.relpersistence = 'u'" "${PREFLIGHT_TABLE_SCOPE:-}"); then
  if [ -n "$agg" ]; then
    note "preflight: follow cannot replicate unlogged tables, whose changes never reach the WAL: $agg; run ALTER TABLE ... SET LOGGED on the source, or leave them out with clone.filters excludeTables or excludeSchemas"
  else
    echo "ok: unlogged table audit"
  fi
else
  note "preflight: probing the source for unlogged tables failed"
fi
ri_offenders=$(check "$PGCOPYDB_SOURCE_PGURI" "select n.nspname || '.' || c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind = 'r' and c.relpersistence = 'p' and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' and (c.relreplident = 'n' or (c.relreplident = 'd' and not exists (select 1 from pg_index i where i.indrelid = c.oid and i.indisprimary))) order by 1")
if [ -n "$ri_offenders" ]; then
  printf '%s\n' "$ri_offenders" > /tmp/ri_offenders
  ri_allow="${PREFLIGHT_ALLOW_MISSING_RI:-}"
  ack_all=0
  printf '%s\n' "$ri_allow" | grep -Fxq '*' && ack_all=1
  while IFS= read -r tbl; do
    if [ "$ack_all" = 1 ] || printf '%s\n' "$ri_allow" | grep -Fxq -- "$tbl"; then
      echo "preflight: warning: acknowledged table $tbl has no usable replica identity; UPDATE and DELETE on it will fail on the source during the migration window"
    else
      note_audit "preflight: table $tbl has no replica identity usable for UPDATE/DELETE (the audit covers all user tables, including ones excluded by clone.filters): ALTER TABLE $tbl REPLICA IDENTITY USING INDEX <unique index>, or ALTER TABLE $tbl REPLICA IDENTITY FULL, or acknowledge it in spec.follow.allowMissingReplicaIdentity"
    fi
  done < /tmp/ri_offenders
else
  echo "ok: replica identity audit"
fi
if [ "$fail" -eq 0 ]; then
  echo "preflight: all checks passed"
else
  printf 'preflight failed:\n%s%s%s' "$fails_audit" "$fails" "$hints"
fi
exit "$fail"