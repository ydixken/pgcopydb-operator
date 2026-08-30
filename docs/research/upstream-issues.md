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

## `copydb_prepare_sequence_specs` writes through an open read cursor: any concurrent commit fails the write for good, and no busy timeout can clear it

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- `clone --follow` under Kubernetes; a supervising operator ran `pgcopydb stream sentinel get --json` and `pgcopydb list progress` in the worker pod on every reconcile pass (at least one pass per second, measured below)

### What happened

Two failures with the same error, at two points in a run.

The first was during the base copy of a live migration (2026-08-09): an index-creation worker died on `[SQLite 5: database is locked]` while the only other catalog users were the operator's periodic sentinel commands, and the clone supervisor tore the run down with it. Log excerpt shape (JSON logging, fields abbreviated):

```
{"pid":123,"error_severity":"ERROR","message":"[SQLite 5: database is locked]"}
...
{"pid":1,"error_severity":"ERROR","message":"clone process 10 has terminated [6]"}
{"pid":1,"error_severity":"FATAL","message":"Terminating all processes in our process group"}
```

That one is a signature match, not a traced failure: same error, same conditions, and we have not traced a cursor-write on the index path. The rest of this entry is the second site, which is traced.

After `follow` reaches its endpos, pgcopydb resets the target's sequences. On 2026-08-30 both follow migrations of the same e2e suite lost attempts there:

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
A second run of the same suite cost `e2e-metrics` an attempt at the same step, on `public.invoice_number_seq`, again about five seconds after the step started.
Only follow migrations are still polled that late in a run, and across both runs only follow migrations lost attempts:

| migration | follow | attempts | `database is locked` |
|---|---|---|---|
| nine clone-only migrations | no | 1 each | none |
| e2e-follow-manual | yes | 3 | run-1, run-2 |
| e2e-follow-auto | yes | 2 | run-1 |
| e2e-metrics | yes | 2 | run-1 |

### Root cause

`copydb_prepare_sequence_specs` iterates `s_seq` with an open cursor and calls `catalog_update_sequence_values` from inside the iteration callback, on the same connection. The failing statement in the log above is that write.

Writing through an open read cursor is a read-to-write upgrade, and SQLite fails the upgrade with `SQLITE_BUSY_SNAPSHOT` (extended code 517) as soon as another connection has committed since the cursor opened. SQLite deliberately does not downgrade that to a retryable `SQLITE_BUSY` while a transaction is already open. Waiting cannot help: the cursor's snapshot is older than the database and stays that way until the cursor closes.

pgcopydb retries regardless. `catalog_sql_step` loops for `maxT = 5` seconds with a 10 to 350ms backoff, so the statement burns five seconds and then fails. That is the entire distance between the sequence step starting and the error, and it is why three separate failures landed at 5.381, 5.224 and 5.237 seconds: the 0.157s spread is the random final sleep, not contention.

The retry loop logs at `log_notice`, so at pgcopydb's default level those five seconds leave no trace. The log shows a step that starts and an error five seconds later with nothing in between, which is a hang as far as anyone reading it can tell.

### Why any client can trigger it, and why pgcopydb's own CLI does

The catalogs are opened in WAL mode. Under WAL a reader cannot block a writer, and a reader does not have to: `SQLITE_BUSY_SNAPSHOT` needs a commit from another connection, not a lock. A reproduction compiled against the vendored SQLite confirms both halves. An external process running `SELECT` in a loop against a writing loop had no effect at all, and a single external `INSERT` failed the writer permanently.

The commits do not have to come from a third-party tool either. Every top-level `pgcopydb` invocation calls `catalog_log_command`, which inserts the command line into `command_log`. So `stream sentinel get` and `list progress`, the documented way to watch a running migration, each commit into that migration's catalog before doing any of their own work.

Our own polling is what supplied the commits here. Each pass runs five of those commands: `stream sentinel get` four times, once each for `--apply`, `--write-lsn`, `--replay-lsn` and `--endpos`, plus one `list progress`.
Sampling one migration's status once per second through its cutover returned five distinct resourceVersions across the five seconds from 16:50:28 to 16:50:33, the last of them in the second the worker logged the lock error: about one reconcile pass per second, so at least five commits per second into the catalog the worker was iterating.
That is a floor and not a rate, since sampling once a second cannot resolve two passes inside one second.
It is also one migration over five seconds, but the cadence is structural rather than a burst: the operator's watch carries no predicate, so each status write feeds back as an event that starts the next pass, and the replication lag it writes changes on every pass because it is derived from the source's WAL head.
None of that is what makes a caller dangerous, since one commit from any client is enough. It is what makes the collision certain: the cursor only has to outlive a single commit, and at five or more commits a second its snapshot is stale within a couple of hundred milliseconds of opening.

### Why a busy timeout does not fix it

There is no `sqlite3_busy_timeout` call in pgcopydb, and adding one would not help. A busy handler answers `SQLITE_BUSY`, which SQLite raises when a lock is held and may later be released. `SQLITE_BUSY_SNAPSHOT` is the other case: nothing is holding anything, the transaction's snapshot is simply older than the database, and no amount of waiting makes an old snapshot current. Raising `maxT` in `catalog_sql_step` has the same problem and only fails later.

### Suggested fix

Collect the sequence rows, close the cursor, then apply the updates. Any shape works as long as the write is not issued while the read cursor over the same connection is open.

Two smaller things worth doing on the same path:

- Log the retry loop at a level users see. Five seconds of invisible retrying reads as a hang, and it is the only evidence that anything was retried at all.
- Surface the extended result code. `[SQLite 5: database is locked]` sends a reader hunting for lock contention, which is the wrong bug class; `SQLITE_BUSY_SNAPSHOT` names the actual condition.

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
