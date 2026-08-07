# pgcopydb CLI Reference (for the Kubernetes operator)

Ground truth: upstream repo `dimitri/pgcopydb` at commit `d8c1ec51f7104a0c80feaa02bfcd56fa3e35ff5e` (2026-07-03), which is **v0.18** (released 2026-06-27, latest release; `PGCOPYDB_VERSION "0.18"` in `src/bin/pgcopydb/defaults.h`). Sources: `docs/` RST (source of readthedocs), `docs/include/*.rst` (generated verbatim from `--help` output), and `src/bin/pgcopydb/*.c/h`. Local clone: `/private/tmp/claude-501/-Users-ydixken-development-pgcopydb-operator/6205d4b3-3179-43ee-adfb-fc9bc4b48926/scratchpad/pgcopydb-src`.

Key v0.18 changes that invalidate older blog posts/docs: default logical decoding plugin is now **pgoutput** (was test_decoding); CDC intermediate storage is now **SQLite** (`*-output.db`, `*-replay.db`) instead of JSON/SQL files; there is **no `stream transform` command** anymore (transform is inline in apply); new `--all-databases`, `--publication`, `--replay-no-op-updates`, `--host/--port` sentinel TCP transport, regex filter patterns, `[include-only-extension]`/`[exclude-extension]` filter sections, arm64 images, `stream prune` command; the sentinel is now a row in the **local SQLite catalog**, not a `pgcopydb.sentinel` table on the source Postgres (log messages still say "pgcopydb.sentinel").

---

## 1. Command tree

```
pgcopydb
  clone     Clone an entire database from source to target (the main command)
  fork      Exact alias of clone
  follow    Replay changes from the source database to the target database (CDC-only client)
  snapshot  Create and export a snapshot on the source database (holds it while running)
  compare
    schema  Compare source and target schema (tables/columns/indexes/constraints/sequences as pgcopydb models them)
    data    Compare source and target data (rowcount + per-table checksum)
  copy
    db           Copy an entire database (clone minus the CDC machinery; historical `copy-db` spelling exists as alias)
    roles        Copy roles from source instance to target (pg_dumpall --roles-only; superuser)
    extensions   Copy extensions from source to target (superuser)
    schema       Copy the database schema (pre+post data) from source to target
    data         Copy the data section (tables, blobs, sequences, indexes, constraints)
    table-data   Copy the data from all tables only
    blobs        Copy large-object data only
    sequences    Copy current sequence values
    indexes      Create all source indexes on target
    constraints  Create all source constraints on target
  dump
    schema  pg_dump the source schema (pre-data + post-data custom-format file) into the work dir
    roles   pg_dumpall --roles-only into the work dir
  restore
    schema      pg_restore the whole schema dump file to the target
    pre-data    Restore pre-data section only
    post-data   Restore post-data section only
    roles       Restore roles SQL file to target
    parse-list  Parse/print the pg_restore --list archive catalog (filter debugging)
  list
    databases    List databases on the source instance
    extensions   List source extensions to copy (--available-versions, --requirements --json)
    collations   List source collations to copy
    tables       List source tables to copy (--without-pkey, filters)
    table-parts  Show the same-table-concurrency COPY partition plan for one table
    sequences    List source sequences
    indexes      List indexes to recreate
    depends      List objects filtered-out by dependency (filter debugging)
    schema       Dump pgcopydb's internal schema model as JSON
    progress     Read local SQLite catalogs and report copy progress (--json, --summary)
  stream                    (mostly unit-testing granularity; noted exceptions are operational)
    setup     [operational] Create replication origin on target at slot LSN; sentinel bootstrap
    cleanup   [operational] Drop replication slot on source, origin on target, auto-created publication
    prune     Remove already-applied CDC SQLite files from disk to reclaim space
    prefetch  Stream changes from source into the SQLite CDC store
    catchup   Transform+apply prefetched changes to target
    replay    Live replay changes source→target (has --max-replaydb-size, default 1GB rotation)
    receive   Low-level: stream changes into CDC output.db
    apply     Low-level: transform/apply from replay.db to target (or stdout)
    sentinel
      setup  Bootstrap the sentinel row (always SQLite-direct)
      get    [operational] Read sentinel values (--json)
      set
        startpos  Set start position LSN
        endpos    [operational] Set cutover LSN (`--current` = pg_current_wal_flush_lsn())
        apply     Enable/disable the apply mode
        prefetch  Enable/disable prefetch mode
  ping      Attempt to connect to both source and target, concurrently, with retry (Decorrelated Jitter, ~60 s budget)
  help      Print help
  version   Print version (--json supported)
```

---

## 2. Full option sets

### 2.1 `pgcopydb clone` (= `fork`)

From `docs/include/clone.rst` (generated help) plus the getopt table in `cli_common.c:709` (aliases noted). Defaults from `defaults.h`.

| Option | Arg | Default | Effect |
|---|---|---|---|
| `--source` | conninfo | `$PGCOPYDB_SOURCE_PGURI` | Source Postgres URI/conninfo (both `postgres://…` and `"host=… dbname=…"` forms). |
| `--target` | conninfo | `$PGCOPYDB_TARGET_PGURI` | Target Postgres URI/conninfo. |
| `--dir` | path | `${TMPDIR}/pgcopydb` else `/tmp/pgcopydb` | Work directory (state, catalogs, dumps, cdc/). |
| `--table-jobs` (alias `--jobs`, `-J`) | int | 4 | Concurrent table COPY processes; also sizes the VACUUM worker pool. |
| `--index-jobs` | int | 4 | Concurrent CREATE INDEX processes (global across tables). |
| `--restore-jobs` | int | 0 → falls back to `--index-jobs` | `pg_restore --jobs` for pre-data/post-data sections. |
| `--large-objects-jobs` | int | 4 | Concurrent large-object data workers. |
| `--split-tables-larger-than` (alias `--split-at`) | byte size (`B,kB,MB,GB,TB,PB,EB` or plain bytes) | 0 (off) | Enables same-table concurrency for tables ≥ threshold; part count = table size / threshold. |
| `--split-max-parts` | int | unset | Cap on the number of parts for same-table concurrency. |
| `--estimate-table-sizes` | flag | off | Use page-count estimates (runs `vacuumdb --analyze-only --jobs=<table-jobs>` on source) instead of exact size queries for split decisions. |
| `--drop-if-exists` | flag | off | Adds `pg_restore --clean --if-exists` (issues DROPs on target). |
| `--roles` | flag | off | Pre-step: copy roles (`pg_dumpall --roles-only`; needs superuser for passwords). |
| `--no-role-passwords` | flag | off | Dump roles from `pg_roles` (no passwords) instead of `pg_authid`; avoids superuser for role dump. |
| `--no-owner` | flag | off | pg_restore `-O`: don't emit ALTER OWNER/SET SESSION AUTHORIZATION. |
| `--no-acl` | flag | off | pg_restore `-x`: skip GRANT/REVOKE. |
| `--no-comments` | flag | off | Skip COMMENT commands. |
| `--no-tablespaces` | flag | off | Skip tablespace selection commands. (Env: `PGCOPYDB_SKIP_TABLESPACES`.) |
| `--skip-large-objects` (alias `--skip-blobs`) | flag | off | Don't copy large objects. |
| `--skip-extensions` | flag | off | Don't create extensions (nor their dependent schemas) on target. |
| `--skip-ext-comments` (alias `--skip-ext-comment`) | flag | off (implied by `--skip-extensions`) | Skip COMMENT ON EXTENSION. |
| `--skip-collations` | flag | off | Skip collations (OS collation mismatch scenarios). |
| `--skip-vacuum` | flag | off | Skip post-COPY VACUUM ANALYZE jobs. |
| `--skip-analyze` | flag | off | Skip `vacuumdb --analyze-only` (only meaningful with `--estimate-table-sizes`). |
| `--skip-db-properties` | flag | off | Don't copy `ALTER DATABASE … SET` GUC properties. |
| `--skip-split-by-ctid` | flag | off | Disable CTID-based fallback splitting for same-table concurrency. |
| `--requirements` | JSON filename | unset | Pin extension versions to install on target (array of `{"name","version"}`; produce with `pgcopydb list extensions --requirements --json`). |
| `--filters` (alias `--filter`) | INI filename | unset | Object filtering; see §7. |
| `--fail-fast` | flag | off | On first non-zero child exit, SIGTERM the whole process group. |
| `--restart` | flag | off | Permit erasing an existing work directory from a previous run. |
| `--resume` | flag | off | Resume an interrupted run reusing local catalogs; completed tables/indexes skipped. Requires `--not-consistent` unless the original snapshot still exists. |
| `--not-consistent` | flag | off | Allow taking a fresh snapshot instead of reusing the recorded one (breaks single-snapshot consistency). |
| `--snapshot` | snapshot id | unset (auto-export) | Reuse an externally exported snapshot (`pg_export_snapshot()` value); the exporting transaction must remain open. |
| `--follow` | flag | off | Run CDC (the whole `follow` machinery) concurrently with the base copy; slot created with the same snapshot. |
| `--plugin` | `pgoutput` \| `test_decoding` \| `wal2json` | `pgoutput` | Logical decoding output plugin. |
| `--publication` | name | derived from slot name, auto-managed | pgoutput publication name; if omitted pgcopydb CREATEs (and later DROPs) a publication itself, filter-aware (`FOR TABLE …` when filters are active). |
| `--wal2json-numeric-as-string` | flag | off | Pass `numeric-data-types-as-string` to wal2json. |
| `--replay-no-op-updates` | flag | off | Replay UPDATEs even when no column changed (needed when target triggers must fire; relevant to REPLICA IDENTITY FULL). |
| `--slot-name` | name | `pgcopydb` | Replication slot name. |
| `--create-slot` | flag | off (clone --follow creates it as part of setup anyway) | Explicitly create the replication slot. |
| `--origin` | name | `pgcopydb` | Replication origin node name on the target (per-source uniqueness matters for fan-in). |
| `--endpos` | LSN | unset | Stop replaying at this LSN, exit 0. Not transaction-boundary aware. Settable at runtime via `stream sentinel set endpos`. |
| `--use-copy-binary` | flag | off | `COPY WITH (FORMAT BINARY)` for table data. |
| `--all-databases` | flag | off | Clone every non-template database of the instance (3-phase orchestration; source/target URIs are instance-level). |
| `--host` / `--port` | host / int | unset / 5442 | Start the sentinel TCP coordinator inside the follow supervisor so `stream sentinel` CLIs can control the run without shared catalog files. (Env: `PGCOPYDB_HOST`/`PGCOPYDB_PORT`.) In help output only via docs; present in getopts. |
| `--verbose`/`--notice`, `--debug`, `--trace`, `--quiet` | flags | INFO | Log verbosity (levels: FATAL, ERROR, WARN, INFO, NOTICE, SQL, DEBUG, TRACE). |

### 2.2 `pgcopydb follow`

Shares the clone getopts function (`cli_copy_db_getopts`); advertised options:

`--source`, `--target`, `--dir`, `--filters <filename>`, `--restart`, `--resume`, `--not-consistent`, `--snapshot`, `--plugin`, `--publication`, `--wal2json-numeric-as-string`, `--replay-no-op-updates`, `--slot-name`, `--create-slot`, `--origin`, `--endpos`, `--host`/`--port`, plus verbosity flags. Semantics identical to the clone rows above. `follow` performs `stream setup` implicitly if not already done. Runs two workers: **receive** (slot → `*-output.db`) and **apply** (inline transform to `*-replay.db` → target, origin tracking); apply only writes to the target once the sentinel `apply` flag is enabled (automatic in `clone --follow` after base copy; manual via `stream sentinel set apply`).

### 2.3 `pgcopydb snapshot`

| Option | Notes |
|---|---|
| `--source` | Source URI (no `--target`). |
| `--dir` | Work dir; the snapshot ID is written to `<dir>/snapshot` and printed on stdout. |
| `--follow` | Create the replication slot via replication-protocol `CREATE_REPLICATION_SLOT`, exporting *its* snapshot (required for gapless CDC consistency). |
| `--plugin` | Plugin for the slot when `--follow` (default pgoutput). |
| `--wal2json-numeric-as-string` | As above. |
| `--slot-name` | Slot to create when `--follow`. |
| verbosity flags | as usual |

Long-running: holds the exporting transaction open until killed (SIGTERM ⇒ clean exit). Other commands auto-discover the snapshot file in the work dir.

### 2.4 `pgcopydb compare schema|data`

| Option | schema | data | Notes |
|---|---|---|---|
| `--source` | ✓ | ✓ | Instance-level URI when `--all-databases`. |
| `--target` | ✓ | ✓ | |
| `--dir` | ✓ | ✓ | Writes `compare/source-schema.json`, `target-schema.json`, `source-data.json`, `target-data.json` under work dir. |
| `--all-databases` | ✓ | ✓ | Compare every non-template DB; up to `--table-jobs` DBs in parallel (sliding window); consolidated summary; three-part `db.schema.table` names. |
| `--json` | – | ✓ | JSON output (compare data only). |

Exit code 0 = match; non-zero on mismatch. See §11.

---

## 3. Connection handling

- `--source`/`--target` accept any libpq conninfo (URI or key=value string). Everything libpq supports (`sslmode`, `sslrootcert`, `application_name`, `passfile`, etc.) passes through untouched; pgcopydb has **no TLS-specific options of its own**. Standard `PGPASSWORD`/`~/.pgpass`/`PGSSLMODE` etc. apply (libpq environment).
- pgcopydb **appends keepalive defaults** to both URIs unless user-set (`copydb.c`): `keepalives=1, keepalives_idle=10, keepalives_interval=10, keepalives_count=60`.
- Child `pg_dump`/`pg_restore`/`pg_dumpall` invocations get `PGCONNECT_TIMEOUT=10` exported (`pgcmd.c`).
- The URIs (sans credentials) are recorded in the SQLite `setup` catalog table for resume-context validation; **credentials are not stored** in catalogs.

### Environment variables read by the code (grep `PGCOPYDB_` over `src/bin/pgcopydb`)

| Variable | Maps to / effect |
|---|---|
| `PGCOPYDB_SOURCE_PGURI` | `--source` |
| `PGCOPYDB_TARGET_PGURI` | `--target` |
| `PGCOPYDB_TABLE_JOBS` | `--table-jobs` |
| `PGCOPYDB_INDEX_JOBS` | `--index-jobs` |
| `PGCOPYDB_RESTORE_JOBS` | `--restore-jobs` |
| `PGCOPYDB_LARGE_OBJECTS_JOBS` | `--large-objects-jobs` |
| `PGCOPYDB_SPLIT_TABLES_LARGER_THAN` | `--split-tables-larger-than` |
| `PGCOPYDB_SPLIT_MAX_PARTS` | `--split-max-parts` |
| `PGCOPYDB_ESTIMATE_TABLE_SIZES` | `--estimate-table-sizes` (bool) |
| `PGCOPYDB_DROP_IF_EXISTS` | `--drop-if-exists` (bool) |
| `PGCOPYDB_SNAPSHOT` | `--snapshot` |
| `PGCOPYDB_OUTPUT_PLUGIN` | `--plugin` |
| `PGCOPYDB_WAL2JSON_NUMERIC_AS_STRING` | `--wal2json-numeric-as-string` (bool) |
| `PGCOPYDB_FAIL_FAST` | `--fail-fast` (bool) |
| `PGCOPYDB_SKIP_VACUUM` | `--skip-vacuum` (bool) |
| `PGCOPYDB_SKIP_ANALYZE` | `--skip-analyze` (bool) |
| `PGCOPYDB_SKIP_TABLESPACES` | `--no-tablespaces` (bool) |
| `PGCOPYDB_SKIP_DB_PROPERTIES` | `--skip-db-properties` (bool) |
| `PGCOPYDB_SKIP_CTID_SPLIT` | `--skip-split-by-ctid` (bool) |
| `PGCOPYDB_USE_COPY_BINARY` | `--use-copy-binary` (bool) |
| `PGCOPYDB_REPLAY_NO_OP_UPDATES` | `--replay-no-op-updates` (bool) |
| `PGCOPYDB_HOST` / `PGCOPYDB_PORT` | sentinel TCP coordinator listen/connect endpoint (default port 5442) |
| `PGCOPYDB_LOG_TIME_FORMAT` | strftime format for log timestamps (`%H:%M:%S` on tty, `%Y-%m-%d %H:%M:%S` otherwise) |
| `PGCOPYDB_LOG_JSON` | JSON-format logs on stderr (bool) |
| `PGCOPYDB_LOG_FILENAME` | Additionally write logs to this file (overwritten each run; dir must exist) |
| `PGCOPYDB_LOG_JSON_FILE` | JSON format for the log file (bool) |
| `PGCOPYDB_DEBUG` | Enables debug facilities (internal) |
| `PGCOPYDB_LOG_SEMAPHORE` | Internal: id of the logging semaphore shared across forks |
| `PGCOPYDB_SERVICE`, `PGCOPYDB_DEBUG_BIN_PATH`, `PGCOPYDB_CLONE_GETOPTS_HELP` | Internal/testing |
| Non-PGCOPYDB: `TMPDIR` | Work dir base (`${TMPDIR}/pgcopydb`) |
| `XDG_DATA_HOME` | CDC file location when `--dir` not given (order: `--dir`/cdc → `$XDG_DATA_HOME` → `~/.local/share`) |
| `PG_CONFIG` | Selects the Postgres installation whose `bindir` provides `pg_dump`/`pg_restore` |

Boolean env vars accept Postgres boolean spellings (`true/yes/on/1`).

---

## 4. Work directory and catalogs

Layout (`copydb_paths.h`), rooted at `--dir`, else `${TMPDIR}/pgcopydb`, else `/tmp/pgcopydb`:

```
<workdir>/
  pgcopydb.pid, pgcopydb.service.pid     # pidfiles (also pgcopydb.aux.pid for snapshot)
  snapshot                               # exported snapshot id
  schema.json                            # internal schema model
  summary.json                           # end-of-run summary
  schema/
    source.db                            # SQLite catalog: source schema, progress, sentinel, setup table
    filter.db                            # SQLite catalog: filtering results
    target.db                            # SQLite catalog: target schema (compare, index dedup)
    schema.dump                          # pg_dump -Fc pre+post data
    pre-filtered.list / post-filtered.list  # pg_restore --use-list files
    roles.sql, extension namespaces dump …
  compare/
    source-schema.json, target-schema.json, source-data.json, target-data.json
  cdc/                                   # CDC state (or XDG_DATA_HOME when no --dir)
    origin, wal_segment_size, tli, lsn.json
    *-output.db, *-replay.db             # SQLite CDC stores (rotated; --max-replaydb-size, default 1GB)
```

- The SQLite `setup` table in `schema/source.db` records `source_pg_uri`, `target_pg_uri`, `snapshot`, `split_tables_larger_than`, `filters`, `plugin`, `slot_name`; checked at startup to refuse resuming in a different context (mismatch ⇒ suggest `--resume --not-consistent`).
- The **sentinel** (startpos/endpos/apply/write_lsn/flush_lsn/replay_lsn) is a single row in `schema/source.db` (since the SQLite rewrite), not on the source Postgres.
- **`--restart`**: deletes the existing work dir contents (a safety valve; without it, a non-empty work dir from a previous *different* run makes the command refuse to start).
- **`--resume`**: reuses catalogs; fully-copied tables (recorded done in catalogs), completed CREATE INDEX/ALTER TABLE are skipped; interrupted COPYs restart from scratch (transactional). Consistency requires the original snapshot to still be importable (held by a still-running `pgcopydb snapshot`), else combine with `--not-consistent`.
- **Operator implication**: sentinel CLI ↔ running follow process coordination via SQLite requires *same filesystem* (SysV `IPC_PRIVATE` semaphore only shared across fork). Across pods/containers use the TCP transport: run clone/follow with `PGCOPYDB_HOST=0.0.0.0` (coordinator listens on 5442) and drive it with `pgcopydb stream sentinel … --host <svc> [--port 5442]`. `stream sentinel setup` is always SQLite-direct.

---

## 5. Required PostgreSQL privileges

Base clone (no `--follow`):
- **Source**: pg_dump-level read access: SELECT on all copied tables/sequences, USAGE on schemas, ability to run `pg_export_snapshot()` (any role) and keep a REPEATABLE READ transaction open. No replication privilege needed.
- **Target**: CREATE on the database (schema restore), plus ownership semantics: without `--no-owner`, `pg_restore` issues `ALTER OWNER`, which requires superuser (or connecting as the owning role). `ALTER DATABASE … SET` (db properties step) requires database ownership or superuser (skip with `--skip-db-properties`).
- **Superuser required** for: `--roles` (reads `pg_authid` passwords; avoid with `--no-role-passwords`), `copy extensions`/creating most C extensions on target, and (a pg_dump limitation) cloning a database that has an extension *with configuration tables* installed by superuser (even filters can't exclude extension members). Documented split-privilege pattern: run `pgcopydb copy roles` + `pgcopydb copy extensions` as superuser, then `pgcopydb clone --skip-extensions` as app role, all sharing one `pgcopydb snapshot`.

`--follow` (CDC) additionally:
- **Source**: `wal_level = logical` (all follow/cdc test compose files set it), free replication slot (`max_replication_slots`), role with `REPLICATION` attribute (or superuser) to use the replication protocol and `CREATE_REPLICATION_SLOT`; with the default **pgoutput** plugin pgcopydb auto-creates (and drops) a **publication** unless `--publication` names a pre-created one; creating a publication requires CREATE on the database, and `FOR TABLE` publications require table ownership (superuser for `FOR ALL TABLES`). wal2json requires the extension installed on the source; test_decoding ships with core.
- **Target**: `pg_replication_origin_create()` / `_advance()` / `_xact_setup()` are called; these require superuser by default (grantable to non-superusers on modern Postgres, but the docs don't discuss it).
- Tables without a primary key need `REPLICA IDENTITY USING INDEX` or `REPLICA IDENTITY FULL` on the source for UPDATE/DELETE replay.
- Logical-decoding restrictions apply: no DDL, no sequence changes (pgcopydb re-syncs sequences at endpos), no large-object replication.

---

## 6. Supported PostgreSQL versions

- v0.18 CHANGELOG: "PostgreSQL 17 and 18 are now fully supported and tested". The `pgcopydb version` banner reports "compatible with Postgres 14, 15, 16, 17, and 18" (reflects tested matrix; not a hard floor).
- CDC client (`follow`): "compatible with old versions of Postgres, starting with version **9.4**" (`docs/features.rst`); there is a `tests/follow-9.6` suite. pgoutput requires source ≥ 10 (use wal2json/test_decoding below that).
- Practical constraints come from the bundled `pg_dump`/`pg_restore`: their version must match (≥) the **target** server version (README "Dependencies"); modern pg_dump reads servers back to 9.2. `PG_CONFIG` selects the toolchain. The official Docker image builds against PGVERSION=16 client tools (Debian bookworm + PGDG), so a target on PG 17/18 needs an image with matching client tools.
- Same-table split requires an integer (int2/4/8) unique/PK column, else CTID ranges (disable with `--skip-split-by-ctid`).
- Tablespaces are not migrated (feature matrix: ✗).

---

## 7. Filtering INI format (`--filters <file>`)

INI sections; entries are lines (keys only, values unused). Schema-qualified names use Postgres quoting rules; any name component may be a POSIX ERE via `~/pattern/` (unmatched patterns are silently ignored).

| Section | Semantics | Constraints |
|---|---|---|
| `[include-only-table]` | Exclusive list of tables to copy (`schema.table`). Materialized views count as tables. | Disallows `[exclude-schema]` and `[exclude-table]`. |
| `[include-only-schema]` | Only listed schemas are processed (implemented as exclusion of the rest). Bare schema names. | Disallows `[exclude-schema]`. |
| `[exclude-schema]` | Skip all tables of the listed schemas. | Not with `include-only-*`. |
| `[exclude-table]` | Skip listed tables entirely. | Not with `[include-only-table]`. |
| `[exclude-index]` | Skip individual index definitions (table still copied). | |
| `[exclude-table-data]` | Copy schema/indexes/constraints but not the rows. | |
| `[exclude-extension]` | Don't create the listed extensions nor copy their config tables. Bare names. | Not with `[include-only-extension]`. |
| `[include-only-extension]` | Only listed extensions handled. Bare names. | Not with `[exclude-extension]` nor `--skip-extensions`. |

Filters apply to CDC too in v0.18 (filter-aware publication creation; wal2json `filter-tables`). Debug with `pgcopydb list depends`, `pgcopydb list tables --filter … --list-skipped`, `pgcopydb restore parse-list`.

---

## 8. Process model & signals

- Pure `fork()` worker tree. `pgcopydb clone --follow --table-jobs 4 --index-jobs 4 --large-objects-jobs 4` ⇒ 26 processes: clone worker → copy supervisor (+1 queue worker + 4 COPY workers), blob metadata worker (+4 blob data workers), index supervisor (+4 index/constraint workers), vacuum supervisor (+4 vacuum workers, pool sized by `--table-jobs`), sequences reset worker; follow worker → `stream receive` + `stream apply` (transform inline in apply).
- Ordering: table COPY queue ordered largest-first (reltuples/size); indexes for a table enqueue when its COPY finishes; PKs created as `CREATE UNIQUE INDEX` then `ALTER TABLE … ADD CONSTRAINT … USING INDEX` (parallelizable); VACUUM ANALYZE queued per finished table; post-data `pg_restore --use-list` last.
- `--all-databases`: Phase I snapshot-holder subprocess (one REPEATABLE READ snapshot per DB, held until Phase III) + `--table-jobs` pre-data workers per-DB in parallel; Phase II single **global** COPY/index/vacuum pools across all DBs (largest-first across DB boundaries, per-DB connection cache); Phase III sequential per-DB post-data + sequence reset; consolidated summary.
- Signals (`signals.c`): `SIGTERM` = graceful stop (`asked_to_stop`), `SIGINT` = fast stop, `SIGQUIT` = immediate quit, `SIGHUP` = reload flag. Handlers propagate to the process group; a second signal escalates. Interrupted runs are designed to be re-run with `--resume` (plus `--not-consistent` if the snapshot is gone). `--fail-fast` (or `PGCOPYDB_FAIL_FAST`) SIGTERMs the group on the first failed child. `pgcopydb snapshot` exits cleanly on SIGTERM ("Asked to terminate, aborting").
- Sentinel coordinator (optional) runs inside the follow supervisor when `--host`/`PGCOPYDB_HOST` set; TCP wire protocol `[version:1][type:1][len:2][payload]` with PING/SET_STARTPOS/SET_ENDPOS/SET_APPLY/QUERY_SENTINEL, default port **5442**.

---

## 9. Container images & releases

- Latest release: **v0.18** (tag `v0.18`, 2026-06-27). Version scheme `0.x`, `DEF_VER=0.18` in GIT-VERSION-GEN.
- **Docker Hub `dimitri/pgcopydb`**: tagged releases `v0.2`…`v0.18` + `latest`. `latest` and `v0.18`/`v0.17` are multi-arch (amd64 + arm64); v0.16 and older amd64-only. `docker run --rm -it dimitri/pgcopydb:v0.18 pgcopydb --version`.
- **GHCR `ghcr.io/dimitri/pgcopydb`**: CI-built from every commit to `main` (`:latest`).
- Image base: Debian **bookworm** slim-style with PGDG apt repo, `postgresql-client-16` (PGVERSION=16 build arg), libpq5, libgc1, sqlite3 binary included; runs as non-root user `docker` (group postgres, passwordless sudo); `ENTRYPOINT []`; pre-creates `/var/run/pgcopydb` owned `docker:postgres` (intended volume mount point for the work dir).
- No official Helm chart or Kubernetes manifests upstream.

---

## 10. Exit codes & machine-readable output

Exit codes (`defaults.h`): `0` OK/asked-to-quit; `1` bad args; `2` bad config; `3` bad state; `4` PGSQL error; `5` pg_ctl error; `6` source error; `7` target error; `9` reload; `12` internal error; `122` fatal (no retry).

Machine-readable surfaces:
- `--json` output flag on: `pgcopydb version`, all `pgcopydb list *` subcommands (notably `list progress --json`, `list progress --summary --json`, `list extensions --requirements --json`, `list schema`), `pgcopydb compare data`, `pgcopydb stream sentinel get`.
- **Progress**: `pgcopydb list progress --json` (reads local SQLite catalogs; needs access to the work dir, i.e. same pod/volume) returns `{table-jobs, index-jobs, tables: {total, done, in-progress:[{oid, schema, name, reltuples, bytes, process:{pid, start-time-epoch, command}…}]}, indexes: {…}}`. `--summary --json` gives per-step/per-table timing after or during the run.
- **Summary file**: `<workdir>/summary.json` written at completion.
- **CDC lag**: `stream sentinel get --json` (`startpos`, `endpos`, `apply`, `write_lsn`, `flush_lsn`, `replay_lsn`); also visible source-side in `pg_stat_replication` for the slot.
- **Logs**: `PGCOPYDB_LOG_JSON=on` for structured stderr logs (`timestamp, pid, error_level, error_severity, file_name, file_line_num, message`); `PGCOPYDB_LOG_FILENAME` (+`PGCOPYDB_LOG_JSON_FILE`) for a log file.
- `pgcopydb ping` as a readiness probe (built-in retry ≈60 s, decorrelated jitter).

---

## 11. `pgcopydb compare` for post-migration verification

- `compare schema`: fetches pgcopydb's own catalog model from both sides (tables, columns, indexes, constraints, sequences incl. last_value) and matches them; explicitly **limited** to what pgcopydb models; it is not a full DDL diff (no functions, triggers, ACLs, defaults…). Exit 0 on match.
- `compare data`: per table computes `count(1)` and a checksum `md5(format('%s-%s', sum(hashtext(row::text)::bigint), count(1)))::uuid` over `FROM ONLY <table>`; compares source vs target, prints a table with `!` markers, ERROR lines per mismatch, non-zero exit on any diff. `--json` supported. Full-table scans are expensive on large tables; no row-level diff, no sampling option.
- Both support `--all-databases` (parallelism = `--table-jobs`, sliding window; unified `db.schema.table` output).
- Note: `compare data` on a live CDC target before cutover will naturally mismatch; sequence a quiesce → endpos → compare for verification.

---

## Operator-relevant gotchas (summary)

1. Work dir is the unit of resumability: it must live on a PV and be reused with `--resume` across pod restarts; `--restart` wipes it.
2. Snapshot lifetime: consistent multi-step flows require a `pgcopydb snapshot` process kept alive for the whole base copy (sidecar/long-running container), or accept `--not-consistent`.
3. Cross-pod control of a running follow: use the TCP sentinel coordinator (`PGCOPYDB_HOST=0.0.0.0`, port 5442) instead of sharing the SQLite catalog volume.
4. `pgcopydb stream cleanup` must run after CDC to drop the slot/origin/publication, or the source accumulates WAL.
5. Default slot/origin/publication names are all `pgcopydb`; they must be overridden for multi-migration-per-server setups.
6. Image pg_dump version must be ≥ target server major (default image ships PG16 client tools).
7. `--endpos` cutover: sequences are re-synced automatically at endpos in `clone --follow` (or manually via `copy sequences`).


## Open questions from this research pass

- Whether target-side pg_replication_origin_* calls strictly require superuser on PG 14+ or work with GRANT EXECUTE / pg_write_all_data-style grants; upstream docs do not address it; needs a live test against the target PG version.
- Exact minimum supported source server version for the base clone path (pg_dump from PG16 reads back to 9.2; pgcopydb docs only state CDC compatibility 'starting with 9.4'); no explicit clone-side floor is documented.
- Whether the version banner 'compatible with Postgres 14, 15, 16, 17, and 18' is a hard compatibility claim or just the tested matrix; docs/example outputs are inconsistent across versions.
- Whether the v0.18 Docker Hub image's PG16 client tools officially support PG 17/18 targets (pg_dump version-matching rule suggests a custom image is needed for PG17/18 targets).
- --host/--port on `clone --follow`/`follow` appear in the getopt table and sentinel-protocol docs but not in the generated --help includes; confirm they are accepted on clone (not only on stream replay) in a live run.
- Precise behavior of `pgcopydb stream prune` (retention criteria for output.db/replay.db files); only the one-line help and CHANGELOG entry document it.
- Exact JSON schema of summary.json (docs show `list progress --summary --json` output but not the file itself); worth capturing from a test run before parsing it in the operator.
