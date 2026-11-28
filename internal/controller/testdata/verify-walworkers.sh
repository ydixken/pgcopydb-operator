set -eu
compare_data_strict() {
  { rc=0; pgcopydb compare data --dir /work/pgcopydb --json 2>&1 >/tmp/compare-data.json || rc=$?; echo "$rc" >/tmp/compare.status; } | tee /tmp/compare.log >&2
  rc=$(cat /tmp/compare.status) || rc=
  cat /tmp/compare-data.json || :
  verdict=1
  if [ "$rc" != 0 ]; then
    echo "compare data exited ${rc:-without a status}, so tables differ or it could not compare them; refusing to read that as a match"
  elif ! unmatched=$(psql "$PGCOPYDB_TARGET_PGURI" -tAX -v ON_ERROR_STOP=1 -f - <<'SQL'
\set r `cat /tmp/compare-data.json`
select 'the report lists no table, so nothing was compared'
 where json_array_length(:'r'::json) = 0;
select format('%s.%s: source %s rows (checksum %s), target %s rows (checksum %s)%s',
              t->>'schema', t->>'name',
              t->'source'->>'rowcount', t->'source'->>'checksum',
              t->'target'->>'rowcount', t->'target'->>'checksum',
              ', status ' || (t->>'status'))
  from json_array_elements(:'r'::json) as report(t)
 where t->'source'->>'rowcount' is distinct from t->'target'->>'rowcount'
    or t->'source'->>'checksum' is distinct from t->'target'->>'checksum'
    or t->'source'->>'rowcount' is null
    or t->'source'->>'checksum' is null
    or (t->'status' is not null and t->>'status' is distinct from 'match');
SQL
  ); then
    echo "compare data report could not be evaluated; refusing to read that as a match"
  elif [ "$unmatched" = "the report lists no table, so nothing was compared" ]; then
    if source_empty=$(psql "$PGCOPYDB_SOURCE_PGURI" -tAX -v ON_ERROR_STOP=1 -c "select not exists (
  select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
   where n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
     and c.relkind in ('r', 'p', 'f', 'm', 'S')
) and not exists (select 1 from pg_catalog.pg_largeobject_metadata);") &&
       target_empty=$(psql "$PGCOPYDB_TARGET_PGURI" -tAX -v ON_ERROR_STOP=1 -c "select not exists (
  select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
   where n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
     and c.relkind in ('r', 'p', 'f', 'm', 'S')
) and not exists (select 1 from pg_catalog.pg_largeobject_metadata);"); then
      if [ "$source_empty" = t ] && [ "$target_empty" = t ]; then
        echo "empty databases verified: neither source nor target has data-bearing objects"
        verdict=0
      else
        echo "empty compare report cannot prove a match: source or target has data-bearing objects"
      fi
    else
      echo "empty compare report cannot be verified: live database inventory failed"
    fi
  elif [ -n "$unmatched" ]; then
    echo "compare data did not find every table matching between source and target:"
    echo "$unmatched"
  else
    verdict=0
  fi
  grep -F 'compare progress: ' /tmp/compare.log | tail -n 1
  if [ -n "${clone_progress:-}" ]; then
    printf 'clone-progress: %s\n' "$clone_progress"
  fi
  return "$verdict"
}
endpos=$(pgcopydb stream sentinel get --endpos --dir /work/pgcopydb)
replay=$(pgcopydb stream sentinel get --replay-lsn --dir /work/pgcopydb)
behind=0
for origin in pgcopydb_ns_m_a22ad55b pgcopydb_ns_m_a22ad55b_w1 pgcopydb_ns_m_a22ad55b_w2; do
  progress=$(psql "$PGCOPYDB_TARGET_PGURI" -tAc "select coalesce(pg_replication_origin_progress('$origin', true)::text, '0/0')")
  gap=$(psql "$PGCOPYDB_TARGET_PGURI" -tAc "select pg_wal_lsn_diff('$endpos'::pg_lsn, '$progress'::pg_lsn)")
  echo "endpos=$endpos replay_lsn=$replay origin_progress=$progress origin_gap_bytes=$gap origin=$origin"
  [ "$gap" -eq 0 ] || behind=$((behind + 1))
done
clone_progress=$( { set +e
echo gate
} 2>/dev/null | tr -d '\n' ) || true
if [ -n "$clone_progress" ]; then
  printf 'clone-progress: %s\n' "$clone_progress"
fi
if [ "$endpos" = "0/0" ]; then
  echo "the work dir reports no cutover endpos, so the origins have nothing to be measured against. Deciding by content."
elif [ "$behind" -eq 0 ]; then
  echo "drain verified: origin progress equals endpos $endpos on all 3 WAL worker origins, nothing left to apply"
  exit 0
else
  echo "$behind of 3 WAL worker origins are not at endpos, and no distance decides the drain in either direction: unapplied commits and publication-filtered WAL measure alike. Deciding by content."
fi
if compare_data_strict; then
  echo "drain verified: pgcopydb compare data found all migrated tables matching"
  exit 0
fi
echo "drain refuted: pgcopydb compare data did not show the target matching the source (the compare lines above say whether it found a difference or could not produce a verdict); do not switch applications to the target (the replication slot is kept)"
exit 1