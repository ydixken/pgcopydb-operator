#!/bin/sh
set -eu

tool=${0##*/}
tools_root=${PGCOPYDB_TOOLS_ROOT:-/opt/pgtools}
cache_file=${PGCOPYDB_CLIENT_CACHE:-/tmp/pgcopydb-client-major}
real_pgcopydb=${PGCOPYDB_REAL_BINARY:-/usr/local/lib/pgcopydb/pgcopydb}

fail() {
  printf 'runner client selector: %s\n' "$1" >&2
  exit 1
}

select_client() {
  if [ -n "${PGCOPYDB_CLIENT_MAJOR:-}" ]; then
    major=$PGCOPYDB_CLIENT_MAJOR
  elif [ -f "$cache_file" ]; then
    major=$(cat "$cache_file")
  elif [ -n "${PGCOPYDB_TARGET_PGURI:-}" ]; then
    if ! target_major=$(psql "$PGCOPYDB_TARGET_PGURI" -XAtq -v ON_ERROR_STOP=1 \
      -c "select current_setting('server_version_num')::int / 10000" 2>/dev/null); then
      fail "cannot read the target PostgreSQL major version"
    fi
    case "$target_major" in
      ''|*[!0-9]*) fail "target PostgreSQL major is not numeric" ;;
      9|10|11|12|13|14|15|16) major=16 ;;
      *) major=18 ;;
    esac
    umask 077
    cache_tmp="${cache_file}.$$"
    printf '%s\n' "$major" > "$cache_tmp" || fail "cannot write client selection cache"
    mv "$cache_tmp" "$cache_file" || fail "cannot save client selection cache"
  else
    major=18
  fi

  case "$major" in
    16|18) printf '%s\n' "$major" ;;
    *) fail "unsupported client major $major" ;;
  esac
}

major=$(select_client)
case "$tool" in
  pgcopydb)
    PATH="$tools_root/$major:$PATH"
    export PATH
    exec "$real_pgcopydb" "$@"
    ;;
  pg_dump|pg_restore)
    exec "$tools_root/$major/$tool" "$@"
    ;;
  *)
    fail "unsupported command name $tool"
    ;;
esac
