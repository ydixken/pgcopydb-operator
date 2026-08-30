# Upstream issue drafts

Drafts for issues against [pgcopydb](https://github.com/dimitri/pgcopydb) found during operator development. Filing is reserved for the maintainer (outward communication needs sign-off). Further candidates without drafts yet: resume-after-endpos exits 0 without replaying, swallowed apply-preamble errors, non-idempotent CREATE PUBLICATION on `--resume`.

## `pgcopydb list progress` (no `--filters`) permanently corrupts the stored filtering of a filtered catalog, killing concurrent or resumed `clone --filters` runs

Status: filed as [dimitri/pgcopydb#1038](https://github.com/dimitri/pgcopydb/issues/1038) on 2026-08-09; fix proposed in [dimitri/pgcopydb#1042](https://github.com/dimitri/pgcopydb/pull/1042) on 2026-08-22.
The fork branch [ydixken/pgcopydb `v0.18-fixes`](https://github.com/ydixken/pgcopydb/tree/v0.18-fixes) carries the fix, and the [pgcopydb builder image](../../images/pgcopydb-builder/README.md) builds pgcopydb from it until an upstream release ships it.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- PostgreSQL 17 on both source and target
- rootless podman

### Reproduction (self-contained)

```sh
podman network create pgnet
podman run -d --name src --network pgnet -e POSTGRES_PASSWORD=pw postgres:17
podman run -d --name tgt --network pgnet -e POSTGRES_PASSWORD=pw postgres:17
sleep 5
podman exec src psql -U postgres -c "
  CREATE SCHEMA audit;
  CREATE TABLE audit.events (id int);
  CREATE TABLE public.t (id int);
  INSERT INTO public.t SELECT generate_series(1, 100000);"

cat > filters.ini <<'EOF'
[exclude-schema]
audit
EOF

SRC=postgres://postgres:pw@src/postgres
TGT=postgres://postgres:pw@tgt/postgres
IMG=ghcr.io/dimitri/pgcopydb:v0.18

# 1. A filtered clone completes fine.
podman run --rm --network pgnet -v ./filters.ini:/filters.ini:ro -v work:/work \
  $IMG pgcopydb clone --source $SRC --target $TGT --dir /work/m \
  --filters /filters.ini

# 2. One bare list progress against the same work dir. The command fails on
#    its own bug (exit 12, "[SQLite] no such column: bytes") but has already
#    rewritten the stored filtering by then.
podman run --rm --network pgnet -v work:/work \
  $IMG pgcopydb list progress --json --dir /work/m

# 3. Resume the same filtered clone: exit 12, "different filtering".
podman run --rm --network pgnet -v ./filters.ini:/filters.ini:ro -v work:/work \
  $IMG pgcopydb clone --source $SRC --target $TGT --dir /work/m \
  --filters /filters.ini --resume --not-consistent
```

Live variant: run step 2 in a loop while the filtered clone from step 1 is still running. Every clone worker exits 12 with "different filtering" and the clone dies.

### Evidence

`SELECT filters FROM setup` in `<dir>/schema/source.db`:

- before the `list progress` call: `{"type":"SOURCE_FILTER_TYPE_EXCL","exclude-schema":["audit"]}`
- after: `{"type":"SOURCE_FILTER_TYPE_EXCL"}`

The work dir is poisoned for good: every later `clone --filters` run against it fails the filter check with exit 12.

### Root cause

`list progress` takes no `--filters` option, so it opens the catalogs with the bare filtering. The catalog setup check handles "Case 3: the catalogs carry a different filtering" by adopting the caller's filtering and writing it back into the catalog (`catalog_update_filters` write-back, catalog.c:1336). `cli_list_progress` does not set `skipFilterCheck`, so a read-only listing command runs that write path and overwrites the stored record with its own empty filtering. Commands that set `skipFilterCheck` (the sentinel commands) are unaffected.

Secondary bug: the progress query reads `sum(bytes)` from `s_table`, which has no `bytes` column (catalog.c:10911), so `list progress` always fails outright on 0.18 (with and without `--json`, see [#1036](https://github.com/dimitri/pgcopydb/issues/1036), fix proposed in [#1041](https://github.com/dimitri/pgcopydb/pull/1041) on 2026-08-22; the `v0.18-fixes` fork branch carries this fix as well). The corruption happens regardless of that failure.

### Suggested fix

Set `skipFilterCheck` for `list progress` (it only reads), or keep the adopted filtering in memory instead of writing it back to the catalog in Case 3.

## Writing through an open read cursor makes any concurrent commit fatal: `copydb_prepare_sequence_specs` and `copydb_create_constraints` die on `SQLITE_BUSY_SNAPSHOT`, which no busy timeout can clear

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- `clone --follow` under Kubernetes; a supervising operator ran `pgcopydb stream sentinel get --json` and `pgcopydb list progress` in the worker pod repeatedly, for as long as a worker was running
- Line numbers point at upstream [v0.18](https://github.com/dimitri/pgcopydb/tree/v0.18). `sequences.c` and `indexes.c` are byte-identical between v0.18 and the fork our runner builds ([ydixken/pgcopydb](https://github.com/ydixken/pgcopydb) at `ea87951`), so both fault sites below are stock upstream code and every line number for those two files lands on v0.18 as written. `catalog.c` does differ, so its references are given as "fork N / upstream M".

### What happened

Two failures with the same error, at two points in a run.

The first was during the base copy of a live migration (2026-08-09): an index-creation worker died on `[SQLite 5: database is locked]` while the only other catalog users were the operator's periodic sentinel commands, and the clone supervisor tore the run down with it. Log excerpt shape (JSON logging, fields abbreviated):

```
{"pid":123,"error_severity":"ERROR","message":"[SQLite 5: database is locked]"}
...
{"pid":1,"error_severity":"ERROR","message":"clone process 10 has terminated [6]"}
{"pid":1,"error_severity":"FATAL","message":"Terminating all processes in our process group"}
```

Treat that one as a signature match rather than a traced failure. The index worker does carry a site with this fault, described below, but that log does not name the in-flight statement and nobody has matched the two up.

The second is the sequence reset, and it is traced. After `follow` reaches its endpos, pgcopydb resets the target's sequences. On 2026-08-30 both follow migrations of the same e2e suite lost attempts there:

```
16:08:09.610  follow.c    Follow mode is now done, reached endpos 1/76081A70
16:08:09.866  copydb_schema.c  A previous run has run through completion
16:08:09.867  sequences.c  Reset sequences values on the target database
16:08:09.867  sequences.c  Fetching information for 5 sequences
16:08:15.248  catalog.c   ERROR  Failed to execute statement: update s_seq set last_value = $1, isCalled = $2 where nspname = $3 and relname = $4
16:08:15.248  catalog.c   ERROR  [SQLite 5: database is locked]: database is locked
16:08:15.248  sequences.c  ERROR  Failed to prepare our internal sequence catalogs, see above for details
```

`follow.c` had logged "Subprocesses for receive and apply have now all exited" immediately before the sequence step, so pgcopydb's own children were not committing into the catalog. The operator was.
The retry died the same way half a minute later, on a different sequence.
A second run of the same suite cost `e2e-metrics` an attempt at the same step, on `public.invoice_number_seq`, again about five seconds after the step started, and cost `e2e-follow-manual` the same two attempts it had lost in the first run.

The failure needs two things at once: an attempt that reaches the post-drain sequence reset, and another connection committing while that attempt's cursor is open. Every attempt with both died there.

| migration | suite run | follow | attempts | attempts that logged `database is locked` |
|---|---|---|---|---|
| e2e-follow-manual | first | yes | 3 | 1 and 2 |
| e2e-follow-auto | first | yes | 2 | 1 |
| e2e-follow-del | first | yes | 1 | none |
| e2e-metrics | second | yes | 2 | 1 |
| e2e-follow-manual | second | yes | 3 | 1 and 2 |

`e2e-follow-del` is the control for the first condition. Its spec deletes the Migration mid-stream, before any cutover, so it never drains and never reaches the sequence reset: same mode as the others, polled the same way, no failure.
The clone-only migrations are the control for the second. The operator ran no catalog commands against those, so nothing was committing alongside their workers, and none of them logged the error in either run.
`e2e-follow-manual` is listed twice on purpose: the same migration lost the same two attempts at the same step in two separate runs, which says more than the total does.

### Root cause

The catalogs are WAL databases: `catalog_set_wal_mode` runs `PRAGMA journal_mode = WAL` (catalog.c fork 1999 / upstream 1951), called from `catalog_init` when the file is created, and WAL is persistent in the file header.

`copydb_prepare_sequence_specs` iterates `s_seq` with an open cursor (sequences.c:86) and calls `catalog_update_sequence_values` from inside the iteration callback (sequences.c:175). Both use the same `sqlite3 *`: the iterator binds to `iter->catalog->db` (catalog.c fork 7139 / upstream 7091), and the callback gets that same `DatabaseCatalog` through its context (sequences.c:81-84), which the writer opens with `sqlite3 *db = catalog->db;`. So the `UPDATE` runs while the `SELECT` cursor is mid-step: it has returned `SQLITE_ROW` and has not yet returned `SQLITE_DONE`.

That is a read-to-write upgrade inside an open read transaction, and SQLite documents this exact case as [`SQLITE_BUSY_SNAPSHOT`](https://www.sqlite.org/rescode.html#busy_snapshot):

> The SQLITE_BUSY_SNAPSHOT error code is an extended error code for SQLITE_BUSY that occurs on WAL mode databases when a database connection tries to promote a read transaction into a write transaction but finds that another database connection has already written to the database and thus invalidated prior reads.

The scenario that page then walks through (A reads and holds the transaction open, B writes, A tries to write and gets the error) describes the callback line for line.

Retrying cannot win. SQLite downgrades `SQLITE_BUSY_SNAPSHOT` to a plain, retryable `SQLITE_BUSY` only when no transaction was open when the call started (vendored `sqlite3.c:73763`):

```c
}else if( rc==SQLITE_BUSY_SNAPSHOT && pBt->inTransaction==TRANS_NONE ){
  /* if there was no transaction opened when this function was
  ** called and SQLITE_BUSY_SNAPSHOT is returned, change the error
  ** code to SQLITE_BUSY. */
  rc = SQLITE_BUSY;
}
```

With the cursor open, `inTransaction` is not `TRANS_NONE`: the extended code survives, and the enclosing busy-handler loop is skipped by the same condition. The snapshot is stale and stays stale until the cursor closes.

pgcopydb retries regardless. `catalog_sql_step` (catalog.c fork 11211 / upstream 11162) loops for five seconds:

```c
int maxT = 5;            /* 5s */
int maxSleepTime = 350;  /* 350ms */
int baseSleepTime = 10;  /* 10ms */
...
while ((rc == SQLITE_LOCKED || rc == SQLITE_BUSY) &&
       !pgsql_retry_policy_expired(&retryPolicy))
{
    int sleepTimeMs = pgsql_compute_connection_retry_sleep_time(&retryPolicy);

    log_notice("[SQLite %d]: %s, try again in %dms",
               rc, sqlite3_errmsg(query->db), sleepTimeMs);

    (void) pg_usleep(sleepTimeMs * 1000);
    rc = sqlite3_step(query->ppStmt);
}
```

`pgsql_retry_policy_expired` enforces a fixed 5000 ms budget, which is the whole distance between the sequence step starting and the error: three failures in our worker logs landed at 5.381, 5.224 and 5.237 seconds. Expiry is checked before sleeping, so the last iteration adds one more sleep of 10 to 350 ms plus a final step, and the 0.157 s spread across those three sits inside that window. That last point is read off the code rather than instrumented, so take it as consistent with the numbers, not as a measurement.

The same budget bounds how small the vulnerable window is. `pgsql_retry_policy_expired` starts its clock on its first call, which happens after the first `sqlite3_step` has already returned `SQLITE_BUSY`, so the first busy result landed 5.0 to 5.35 seconds before the error. For the 16:08 failure that puts the first `UPDATE` attempt between 0.03 and 0.38 seconds after the "Fetching information for 5 sequences" line: the cursor had been open a few hundred milliseconds at most when a commit invalidated it. That is arithmetic over the fixed budget and the log timestamps rather than a measurement. Running the worker once with `--verbose` would print the retry lines and pin the first busy result exactly, which is a test-run change and not a code change.

The loop logs at `log_notice`. pgcopydb's default level is INFO and our runner passes no `--verbose`, so at default verbosity those five seconds leave no trace: the log shows a step starting, then an error five seconds later, with nothing in between. Two more copies of the same loop live at catalog.c fork 2019 and 10930 / upstream 1971 and 10881.

### The same fault at a second site, where pgcopydb is its own concurrent writer

`copydb_create_constraints` iterates `s_index` for a table (indexes.c:1247, binding `iter->catalog->db` at catalog.c fork 6623 / upstream 6575) and writes three times from inside the callback on that same connection: `summary_add_constraint` (indexes.c:1304), `summary_finish_constraint` (:1354) and `summary_increment_timing` (:1360).

Here the concurrent commits are pgcopydb's own. Constraint creation runs in the one index worker that finished last, while the other `--index-jobs` workers are still in `copydb_create_index` writing to the same `source.db` from their own processes (indexes.c:1008, :1028, :1034). No external client is involved, so this site stands without accepting anything about how we drive pgcopydb.

The code reasons about concurrency at exactly this spot and reaches the opposite conclusion (indexes.c:1338):

```c
/*
 * Constraints are built by the CREATE INDEX worker process that is
 * the last one to finish an index for a given table. We do not
 * have to care about concurrency here: no semaphore locking.
 */
```

This site is traced in code and has not been tied to a specific incident of ours.
It is also why a caller-side fix is not a fix: quiescing our polling closes the sequence site and does nothing here, where pgcopydb supplies the concurrent commits itself.

### Why the documented way to watch a migration is also a writer

Under WAL a reader cannot block a writer, and it does not need to: `SQLITE_BUSY_SNAPSHOT` needs a commit from another connection, not a lock. The commands we used to watch the migration commit.

Every top-level `pgcopydb` invocation calls `catalog_log_command`, which runs `insert into command_log(cmdline) values($1)` (catalog.c fork 10327 / upstream 10279). It is called unconditionally from `catalog_register_setup_from_specs` (fork 1456 / upstream 1408), which `catalog_init_from_specs` calls unconditionally in turn, so any command reaching `catalog_init_from_specs` writes a row. A guard (fork 10320 / upstream 10272) skips it for fork-only children by testing `getpid() != getpgid(0)`, since `main()` calls `setpgrp()` (main.c:61) and only the top-level process has `PID == PGID`. A process started by `kubectl exec` is its own process-group leader, so it writes exactly one row.

Both commands we poll with reach it, and neither has a read-only path: `stream sentinel get` through `cli_sentinel_init_specs` (cli_sentinel.c:936), `list progress` directly (cli_list.c:2050).

Our own polling supplied the commits at the sequence site: each reconcile pass runs `stream sentinel get` four times, once each for `--apply`, `--write-lsn`, `--replay-lsn` and `--endpos`, plus one `list progress`, and every one of them commits a `command_log` row.

How often a caller does that is not part of this report: upstream does not have our operator, and the mechanism needs no rate. The precondition is one commit from any connection between the cursor's first step and the write.

Six attempts across two runs died at this step: reproduced six times, same step, same signature.

### Reproduction

A short harness compiled against the SQLite the fork vendors (3.45.1, `sqlite3.h:149`) opens a WAL database, iterates `s_seq` with an open cursor, and issues the same parameterised `update s_seq ...` from inside the row loop, re-stepping four times the way `catalog_sql_step` does:

```
=== A: no external process (control) ===
  update public.a -> rc=101 ... (all five succeed)

=== B: external PURE READER mid-loop ===
  update public.a -> rc=101 ... (all five succeed)

=== C: external WRITER mid-loop (one INSERT, like catalog_log_command) ===
  update public.a -> rc=101 (no more rows available)
  update public.b -> rc=5 (database is locked) extended=517 (database is locked)
    retry 1 -> rc=5 extended=517
    retry 2 -> rc=5 extended=517
    retry 3 -> rc=5 extended=517
    retry 4 -> rc=5 extended=517
```

`101` is `SQLITE_DONE`. Case B is the one that matters: the child opened the same file read-write and only read, and the writer was untouched. One external `INSERT` breaks it, and retrying never recovers. The harness exercises SQLite at the call shape v0.18 has byte-identically; it is not a run of stock 0.18 against a live migration, and nobody has done one.

The harness is [`lockrepro.c`](lockrepro.c), 107 lines, kept here so it outlives the investigation; it creates and removes `/tmp/lockrepro`. Build it against the amalgamation pgcopydb vendors, and inline it in the issue body if this is ever filed:

```sh
cc -O0 -I src/bin/lib/sqlite -o lockrepro lockrepro.c src/bin/lib/sqlite/sqlite3.c -lpthread
```

### Why a busy timeout does not fix it, and neither does a semaphore

A search over `src/bin/pgcopydb` for `busy_timeout`, `busy_handler`, `sqlite3_busy` and `SQLITE_BUSY` finds no `sqlite3_busy_timeout`, no `sqlite3_busy_handler` and no `PRAGMA busy_timeout`; every match is in the vendored SQLite amalgamation under `src/bin/lib/sqlite`. So SQLite's default applies and `sqlite3_step` returns immediately rather than waiting.

Installing one would not help. A busy handler answers `SQLITE_BUSY`, which SQLite raises when a lock is held and may later be released. `SQLITE_BUSY_SNAPSHOT` is the other case: nothing is holding anything, the transaction's snapshot is simply older than the database, and waiting does not make an old snapshot current. Raising `maxT` in `catalog_sql_step` fails later for the same reason.

A semaphore does not fix it either, which is worth saying because `catalog_update_sequence_values` is missing the `catalog->sema` locking that most other catalog writers take (catalog.c fork 2201, 2274, 2334, 2556, 2708) and that looks like the bug. The constraint site is the counter-example: `summary_add_constraint` and `summary_finish_constraint` do take the semaphore (summary.c:1541, :1624) and fail anyway. Serializing writers does nothing about a read snapshot that went stale before the writer acquired the lock.

### The log does not distinguish the two errors

`sqlite3_step` returns the primary code unless extended result codes are enabled, and nothing in `src/bin/pgcopydb` calls `sqlite3_extended_result_codes`. The catalog error path prints that primary code straight (catalog.c fork 11098 / upstream 11049), so `SQLITE_BUSY_SNAPSHOT` (517, `SQLITE_BUSY | (2<<8)`, `sqlite3.h:535`) reaches the log as `[SQLite 5: database is locked]`. The message string does not help either: `sqlite3_errstr(517)` returns the same "database is locked" text, which the reproduction above shows directly.

pgcopydb does call `sqlite3_extended_errcode`, but only in the replay-store layer (ld_store.c:2637, :2879, :3143) and never in catalog.c, and even there it only feeds `sqlite3_errstr`. So the number that would identify this bug is printed nowhere. A maintainer reading a user's log sees plain `SQLITE_BUSY`, concludes lock contention, and reaches for a timeout, which is the one fix that cannot work.

### Suggested fix

Collect the rows, close the cursor, then apply the writes. At the sequence site that means letting `copydb_prepare_sequence_specs` finish its iteration before calling `catalog_update_sequence_values`; at the constraint site, the same for the three `summary_*` writes in `copydb_create_constraints_hook`. Any shape works as long as no write is issued on a connection whose read cursor is still open.

Two smaller things worth doing on the same paths:

- Log the retry loop at a level users see. Five seconds of invisible retrying reads as a hang, and it is the only evidence that anything was retried at all.
- Print the extended result code in catalog.c's error path. `[SQLite 5: database is locked]` sends a reader hunting for lock contention, which is the wrong bug class.

## `catalog_sql_execute` finalizes a cached statement without evicting it

Status: draft, not filed. Found by reading the source, not from an incident.

### Environment

- Source read from upstream [v0.18](https://github.com/dimitri/pgcopydb/tree/v0.18) and the fork at `ea87951`; `catalog.c` references are given as "fork N / upstream M"

### What happens

`catalog_sql_execute`'s error path calls `sqlite3_finalize(query->ppStmt)` even when `query->fromCache` is true (catalog.c fork 11104 / upstream 11055), while the `CachedStmt` entry in `catalog->stmtCache` keeps the pointer that call just freed. A later hit in `catalog_sql_prepare_cached` then calls `sqlite3_reset(entry->stmt)` on freed memory (catalog.c fork 10986).

We have not seen it bite. On the catalog-lock path above the process exits before the cache is reused, and whether another error path survives to a later cache hit is not established.

### Suggested fix

Evict the cache entry when finalizing a cached statement, or leave the statement alone on the error path and let the cache own its lifetime.

## Process-group termination on clone failure leaves the streaming receive child running, keeping the container alive indefinitely

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- `clone --follow` under Kubernetes; pgcopydb is pid 1 of the container

### What happened

A clone worker died (see the catalog-lock draft above), the supervisor reported it, and pid 1 terminated the process group:

```
{"pid":1,"error_severity":"ERROR","message":"clone process 10 has terminated [6]"}
{"pid":1,"error_severity":"FATAL","message":"Terminating all processes in our process group"}
```

The streaming receive child survived that termination. For more than 40 minutes afterwards it kept reporting write_lsn/flush_lsn progress on the replication stream while no clone or apply work existed anymore. pid 1 never exited, so the container stayed alive, and a supervising Kubernetes Job never saw a failure: from the outside the pod looked healthy while the migration was dead. The run only ended when the pod was deleted externally.

### Root cause (suspected)

The group signal does not stop the receive subprocess (it likely ignores or misses SIGTERM while blocked on the replication connection), and the supervisor then waits for its children without a deadline.

### Suggested fix

After signaling the process group, wait with a bounded timeout and escalate to SIGKILL for children that survive; or have the receive child handle SIGTERM by closing the replication connection and exiting, so a failed clone reliably terminates the whole process tree.

## `clone --resume` re-vacuums already-copied tables and dies on `vacuum_summary`'s `unique(tableoid)` (exit 12 loop)

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- observed live during `clone --follow` under Kubernetes (2026-08-09); the vacuum path is shared by all clone runs

### Reproduction (sketch)

Containers, `$SRC`, `$TGT`, `$IMG` as in the `list progress` draft above; seed enough data that the copy phase takes a minute.

```sh
# 1. Start a clone and kill the container once the log shows VACUUM ANALYZE
#    running for some tables, but before the run completes.
podman run --name run1 --network pgnet -v work:/work \
  $IMG pgcopydb clone --source $SRC --target $TGT --dir /work/m &
# wait for a "VACUUM ANALYZE" log line, then:
podman kill run1

# 2. Resume. Tables whose vacuum already ran are vacuumed again, and the
#    vacuum summary insert collides with the row the killed run left behind.
podman run --rm --network pgnet -v work:/work \
  $IMG pgcopydb clone --source $SRC --target $TGT --dir /work/m \
  --resume --not-consistent
# exit 12; every further --resume fails the same way
```

### Evidence

The resumed run's vacuum worker dies on the insert (JSON logging, fields abbreviated):

```
{"error_severity":"ERROR","message":"Failed to execute statement: insert into vacuum_summary(pid, tableoid, start_time_epoch)values($1, $2, $3)"}
{"error_severity":"ERROR","message":"[SQLite 19: constraint failed]: UNIQUE constraint failed: vacuum_summary.tableoid"}
```

followed by the usual supervisor teardown (`clone process ... has terminated`, FATAL group termination) and exit 12. The offending row survives the crash by design (it lives in the work-dir catalog), so every subsequent `--resume` repeats the collision: the run can never get past vacuum again.

### Root cause

`vacuum_analyze_table_by_oid` (vacuum.c:438) registers the vacuum via `summary_add_vacuum` unconditionally before running `VACUUM ANALYZE`. Unlike the COPY and CREATE INDEX paths, which call `summary_lookup_table` / `summary_lookup_index` first and skip work a previous run finished, there is no vacuum lookup on the resume path. `summary_add_vacuum` (summary.c:944) INSERTs into `vacuum_summary`, declared `unique(tableoid)` (catalog.c:198). On `--resume` the previous run's row is still there, the INSERT fails, and the failed statement is fatal for the worker.

### Suggested fix

Look up `vacuum_summary` before vacuuming and skip tables already done, mirroring the COPY and index paths; or make the registration idempotent (`insert or replace`, or delete the stale row first, as the table-summary path does with `summary_delete_table`).

## Repeated resumes accumulate duplicate index rows in `summary`; single-row lookups then fail with `[SQLite 100: another row available]`, wedging every later resume

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- observed live during `clone --follow` under Kubernetes (2026-08-09), after several crash-and-resume cycles (see the `vacuum_summary` draft above for one reliable crash source)

### Reproduction (sketch)

Containers as above. Crash a clone mid-copy (kill the container while indexes are being built), then resume repeatedly:

```sh
for i in 1 2 3; do
  podman run --rm --network pgnet -v work:/work \
    $IMG pgcopydb clone --source $SRC --target $TGT --dir /work/m \
    --resume --not-consistent
done
```

Each resume that redoes a `CREATE INDEX` inserts another `summary` row for the same index. Once any index has two rows, the next resume fails during its summary lookup, before doing any work, and keeps failing forever.

### Evidence

JSON logging, fields abbreviated:

```
{"error_severity":"ERROR","message":"Failed to execute statement:   select pid, start_time_epoch, done_time_epoch, duration, command     from summary    where indexoid = $1"}
{"error_severity":"ERROR","message":"[SQLite 100: another row available]"}
```

### Root cause

The `summary` table's only uniqueness constraint is `unique(tableoid, partnum)` (catalog.c:185). Index runs insert with `insert into summary(pid, indexoid, start_time_epoch, command)` (summary.c:1369), leaving `tableoid` and `partnum` NULL, and SQLite treats NULLs as distinct in unique indexes, so nothing stops a second row for the same `indexoid`. `summary_lookup_index` (summary.c:1084) then selects `from summary where indexoid = $1` without `LIMIT 1` through `catalog_sql_execute`, which demands exactly one row when a fetch callback is set ("when we have a fetchFunction we expect only one row, and exactly one", catalog.c:11061) and fails on the second SQLITE_ROW. The codebase already knows this trap: `catalog_lookup_filter_by_oid` carries a `LIMIT 1` specifically "to avoid throwing an SQLite error condition about \"another row available\"" (catalog.c:7850).

### Suggested fix

Constrain index summary rows (`unique(indexoid)`) with an idempotent insert, or delete the stale row before re-registering an index run; failing that, add `LIMIT 1` (with an ordering that prefers the latest attempt) to `summary_lookup_index` and its constraint-summary sibling so resumes read a deterministic row instead of erroring.

## `follow` never reaches a freshly set endpos on an idle source: endpos is only evaluated against received WAL, and keepalives do not conclude the drain

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- observed live under Kubernetes (2026-08-09/10, three consistent occurrences): follow migrations wedged in their drain forever after `sentinel set endpos --current` against a fully idle source

### What happened

A follow migration fully caught up (write LSN = replay LSN = the source's flushed LSN), then `pgcopydb stream sentinel set endpos --current`. The worker stays healthy and the receive loop keeps running, but the stream never concludes and the worker never exits. Runs where the source happened to produce WAL shortly after the endpos was set (recent seed activity, autovacuum) drained fine; deep-idle sources hang indefinitely.

### Reproduction (sketch)

```sh
# follow migration caught up against a source with zero write activity
pgcopydb stream sentinel set endpos --current --dir /work/m
# the receive loop waits; nothing arrives, endpos is never evaluated, no exit
# any WAL record releases it, for example:
psql $SRC -Xqtc "select pg_logical_emit_message(false, 'nudge', '')"
# the worker now drains and exits 0 promptly
```

### Root cause (suspected)

The receive loop compares its position against endpos only while processing arriving WAL (copy-data messages). Walsender keepalives do report the server's current WAL position, and on an idle source that position already equals the endpos, but the keepalive path does not perform the endpos check. With no traffic there is nothing to evaluate, so an endpos set at (or past) the idle source's head is unreachable until unrelated WAL happens to arrive.

### Suggested fix

Evaluate endpos on walsender keepalives as well: when the keepalive's reported WAL position has reached endpos, conclude the stream exactly as if a data message had crossed it.

### Related: no LSN distance works as a post-drain criterion under filtered traffic

The same idle windows break the obvious external drain check, comparing the target's `pg_replication_origin_progress` against endpos after the worker exits. The origin only advances on commits the apply executes, and the WAL between the last applied commit and an idle-set endpos is publication-filtered traffic (autovacuum on unpublished tables, catalog churn) that never reaches the apply. The distance therefore grows with idle time without any loss behind it: observed live, 56 bytes right after write activity, beyond one WAL page (8192 bytes, two refusals on record) after deep idle. The sentinel's `replay_lsn` cannot serve either, in the opposite direction: the apply advances it past records it never executes (keepalives, filtered transactions; `ld_apply.c` publishes endpos as `replay_lsn` once the stream is consumed), and it advanced normally in the `session_replication_role` incident where nothing was applied at all. After an idle-set endpos, only a content comparison (`pgcopydb compare data`) distinguishes a drained stream from a lost one.
