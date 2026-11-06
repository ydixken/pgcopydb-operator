# Performance tuning

The phases of a migration have different bottlenecks and respond to different knobs.
The source read, the network, and the target write limit the base copy.
The CPU and memory of the target limit the index builds, and these builds happen after the data is in.
In follow mode, each of these can limit progress: source decoding, receive-spool writes, inline transformation, and target apply.
Measure the pipeline.
Do not assume that the target is the bottleneck.

## Follow receive and apply

The bundled pgcopydb carries the fork PRs that the [runner image README](https://github.com/ydixken/pgcopydb-operator/blob/main/images/runner/README.md) lists.
Fork PRs #27 to #30 raise its follow throughput:

1. Apply waits on the receive pipe only after a pass that found nothing to do.
   Apply also returns from the pipeline sync as soon as the sync result is in ([#27](https://github.com/ydixken/pgcopydb/pull/27)).
1. The replay database (`*-replay.db`) runs with SQLite `synchronous=NORMAL` and gets indexes for the next-event and transaction lookups.
   Apply writes the sentinel `replay_lsn` at most once a second ([#28](https://github.com/ydixken/pgcopydb/pull/28)).
   A power loss can drop the last transactions of `*-replay.db`.
   After such a loss, apply resumes from the target replication origin and transforms them again from `*-output.db`, which stays at `synchronous=FULL`.
1. Receive commits `*-output.db` in groups ([#29](https://github.com/ydixken/pgcopydb/pull/29)).
   A batch commits when receive drains the replication socket, or when the batch is 50 ms old.
   A batch also commits at every flush and close, and before a file rotation.
1. Apply commits several source transactions in one target transaction, an apply group ([#30](https://github.com/ydixken/pgcopydb/pull/30)).

Apply still sends each DELETE on a `REPLICA IDENTITY FULL` table without a key as its own statement.
Because of this, a burst of these DELETEs on such a table applies more slowly than on a keyed table.
We did not measure that cost.

### Apply groups

A group closes after 100 source transactions, 10,000 rows or 200 ms.
It also closes at endpos, before a keepalive, and when the transform has no further complete transaction to deliver.
The group COMMIT moves the replication origin to the COMMIT LSN of the last member of the group.
Thus, if the target refuses a group, none of its rows stay on the target, and the origin stays before the group.
A member after the first that holds a TRUNCATE starts a new group.
This is because PostgreSQL refuses a TRUNCATE while deferred trigger events are pending in the same transaction.
A member that holds a ROLLBACK row starts a new group too.
Deferred triggers and constraints on the target run at the group COMMIT, so they see the rows of every member.
On PostgreSQL 17 and later, apply sets `transaction_timeout` to 0 in its own session.
The reason is that a group can stay open while apply transforms the next source transaction.

`spec.follow.applyGroupTransactions` sets the transaction limit (`PGCOPYDB_APPLY_GROUP_TXNS`).
When the field is unset, pgcopydb keeps its default of 100.
Set it to 1 to commit every source transaction in its own target transaction, as runners before `0.18.94.gbaa94ee` do.
The row and time limits keep pgcopydb's defaults.

> [!note]
> The worker Job reads the field when the operator creates the Job.
> Thus a change reaches a running Migration on its next attempt.
> Runners older than `0.18.94.gbaa94ee` ignore the variable.

### What we measured

We measured each change on one cluster, with source `wal_sender_timeout` at 60s.
The worker had 500m CPU and 64Mi memory, and its work volume was on Longhorn.
The workload is pgbench `tpcb-like` with one client on the source.
The client wrote 3,000 transactions while apply was not running.
Then `pgcopydb follow` drained them to endpos.
Every run compared equal to the source afterwards.

| Change | Build before | With the change |
| --- | ---: | ---: |
| #27 apply wakeup | 32.7 txn/s | 65.8 txn/s |
| #28 replay database | 65.8 txn/s | 174.4 txn/s |
| #29 receive group commit | 185.1 txn/s | 189.4 txn/s |
| #30 apply groups | 164.4 txn/s | 517.9 and 511.8 txn/s |

The #29 and #30 rows compare against the build before them in the same session.
The #27 row compares against `0.18.70.gbb8dbfc`, and the #28 row compares against the #27 build.
Each of these two rows comes from an earlier session of the same setup.
#29 does not move this workload, because apply, not receive, bounds it.
#29 shortens receive instead.
With a 200,000-transaction backlog, receive finished after 57 s instead of 1,101 s.

With all four changes, a 200,000-transaction backlog drained at 513.7 txn/s in 389.3 s, in 2,004 target transactions.
With `PGCOPYDB_APPLY_GROUP_TXNS=1` the same 3,000-transaction run gave 168.5 txn/s against 164.4 for the build without apply groups.
A workload where 1 in 10 transactions truncates a table ran at 267.2 txn/s against 198.8 without apply groups.

Before apply groups, one apply connection committed at most about 1 / (target commit latency) source transactions per second.
On the [measured target](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/measurements.md#one-durable-commit-per-transaction-caps-a-single-connection-near-800-per-second), that was roughly 800 per second.
A group now waits for one durable target COMMIT, so commit latency is spread over up to 100 source transactions.
These numbers come from one setup and one workload shape.
Rehearse catch-up under the intended workload.
A backlog above the catch-up threshold cannot drain while source changes keep arriving faster than the whole pipeline can process them.

### Receive batching in `0.18.10.gaadc4bf`

`0.18.10.gaadc4bf` replaced per-row and per-column SQLite commits in receive with SQLite transactions.
[Fork PR #7](https://github.com/ydixken/pgcopydb/pull/7) records the patch and these measurements, for one source transaction that contains 20,000 four-column rows.
These measurements predate the [certified keepalive feedback](../design/follow-diagnostics.md) in `0.18.13.g4873c18` and the bootstrap recovery in `0.18.15.gea2dc96`.
They measure neither version.
The before/after runs used the same configuration, but not identical volume or cache state.
The before values cover the row-only receive window.
The after values include the SQLite batch commit:

| Storage | `fsync` calls, before to after | Time inside syncs | Receive window |
| --- | ---: | ---: | ---: |
| Longhorn | 100,831 to 3 | 314.08 s to 0.18 s | 324.04 s to 0.98 s |
| Local NVMe | 100,774 to 3 | 125.90 s to 0.04 s | 133.67 s to 0.82 s |

> [!important]
> No retained baseline trace includes the commit.
> The conservative mixed-window ratios are about 330x on Longhorn and 164x on NVMe.
> These ratios are speedups of the receive window, not speedups of the end-to-end migration.
> These individual bursts do not establish sustained throughput, and they do not measure target WAL durability waits.

## What the operator decides

`spec.clone` fields are optional, and a zero value means the operator decides.
For most fields that means pgcopydb's own default.
These five are the exceptions, because a default that suits an ad-hoc copy does not suit a migration.

| Field | Unset behaviour | Why |
|-------|-----------------|-----|
| `runner.resources` | 4 CPUs, 4Gi, requests only | pgcopydb runs four table jobs by default, and a worker that requests less CPU than its own concurrency starves it. |
| `clone.tableJobs` | Follows the worker's CPU request, minimum 4 | One knob instead of two: raise the worker and the copy concurrency follows. |
| `clone.splitTablesLargerThan` | `512Mi` | pgcopydb ships this disabled, which leaves one large table to one worker however many table jobs are running. |
| `clone.splitMaxParts` | `8` | Without a cap, a very large table becomes hundreds of parts and catalog rows. |
| `clone.useCopyBinary` | `true` | Text COPY doubles `bytea` on the wire, and the worker relays every row, so the cost lands on both legs. |

Everything else is pgcopydb's default: four index jobs, restore jobs that follow index jobs, and four large-object jobs.

> [!important]
> When you raise `spec.runner.resources.requests.cpu`, it sizes the worker and the copy concurrency together.
> You do not need to change a second field to match.

## Same-table concurrency

Same-table concurrency matters when a few tables hold most of the bytes.
Without it, `--table-jobs` parallelises *across* tables only.
Thus the biggest table gets one worker, and the clone cannot finish before that one worker does.
With it, pgcopydb splits a table past the threshold into parts.
pgcopydb gives these parts to separate workers from the same table-jobs pool.

The part count is the table size divided by the threshold, capped by `splitMaxParts`.
Lowering the threshold makes more tables eligible and splits large ones further.

A table needs a single-column integer primary or unique key to be split on key ranges.
Without one, pgcopydb splits on `ctid` as a fallback.
`ctid` follows the physical layout, not the key order.
Thus each part is a separate scan over its own page range.
`skip: [ctidSplit]` disables that fallback if the source cannot afford the extra scans.

> [!warning]
> pgcopydb disables same-table concurrency when the source connection lands on a standby.
> It does this without a message.
> A migration that points at a read replica gets no splitting and no warning.

On a `NOT NULL` key, one of the parts is the NULL range, and it copies zero rows.
Thus a cap of 8 gives 7 useful parts.
Splitting also loses the `COPY FREEZE` path for that table.
For a table that is large enough to be worth splitting, this is usually a good trade.

## Index jobs, and the 1GB you cannot see

`clone.indexJobs` has no operator default, because its cost lands on a machine that the operator cannot see.

Each index worker opens one target connection and immediately sets `maintenance_work_mem` to 1GB.
pgcopydb hardcodes that value and applies it per worker.
**It overrides whatever value the configuration of the target itself sets.**
Thus the real cost of this number is roughly `indexJobs` GB of target memory.
pgcopydb's default of four asks for 4GB.

Size it against the target.
Start from the core count of the target, minus the cores that the COPY workers already use there.
Do not go above what the target memory can carry at 1GB each.
With `clone.deferIndexes`, the index builds start only after every table copy finishes.
Then only the vacuum pool shares the target with them, so do not subtract cores for COPY workers.
On a small target, four is already too many, and lowering it to 2 makes the index phase faster.

`clone.restoreJobs` follows `indexJobs` unless you set it.
Set it separately when they differ.
`pg_restore` is a separate process and never receives that GUC.
Thus the same memory ceiling does not bind it.

## Table jobs and the connections they cost

Each table job holds one source connection and one target connection.
pgcopydb also sizes its `VACUUM ANALYZE` pool from the same number, and it runs the pool alongside the copy.
Thus N table jobs can mean up to 2N concurrent backends on the target.
This is not the case when `clone.deferIndexes` moves the pool after the copy.

Check the target's `max_connections` before you raise any value.
Without `clone.deferIndexes`, the worst case is roughly `tableJobs * 2 + indexJobs + largeObjectsJobs + 1`.
Table jobs count two times because of the vacuum pool.
Then there is one per index worker, one per large-object worker, and one for the large-object metadata pass.
With it, the vacuum pool and the index workers never run beside a copy or large-object worker.
Thus the worst case drops to roughly the larger of `tableJobs + largeObjectsJobs + 1` and `tableJobs + indexJobs`.
The source sees the same table and large-object connections, without the index and vacuum ones.
`skip: [vacuum]` drops the vacuum half if you analyse the target separately after cutover.

`clone.largeObjectsJobs` costs four connection pairs by default, whether or not there is anything to move through them: see [`largeObjectsJobs`](#largeobjectsjobs).

## Deferring index builds

`spec.clone.deferIndexes: true` passes `--defer-indexes`: pgcopydb copies every table first, then builds the indexes and constraints and runs `VACUUM ANALYZE`.
Without it, pgcopydb builds the indexes of a table while other tables are still copying.
[Deferred index builds](../configuration.md#deferred-index-builds) covers immutability and the preflight check.
Weigh these before you set it:

1. The clone takes longer.
   Without the field, index builds overlap the copy.
   Thus the clone takes about as long as the longer of the two.
   With the field, the clone takes about as long as both added together.
   In exchange, the copy runs without index builds competing for the target's CPU and I/O.
1. All index workers start together when the copy ends, and they take the largest tables first.
   Each worker has `maintenance_work_mem` at 1GB.
   Thus the memory, temporary-file and WAL use of the target arrive in one burst.
1. With `spec.follow.enabled`, the receive backlog and the WAL that the replication slot holds on the source grow with the longer clone.
1. It does not help against a proxy that cuts silent connections: a long `CREATE INDEX` still sends nothing until it ends.
1. An attempt in which a table's copy fails builds no index, not even on the tables it copied.
   The next attempt builds them after its own copy.
1. A runner that is lost during the index builds can leave up to `indexJobs` sessions running `CREATE INDEX` on the target.
   With the server default `client_connection_check_interval`, each session stays until its statement ends.
   The [retry gate](lifecycle.md#the-retry-gate) holds the next attempt until the last of them ends (see [Retries and resume](lifecycle.md#retries-and-resume)).
1. Without `clone.allDatabases`, the index builds start only after pgcopydb copies the large objects, the extension configuration rows and the sequences.
   Thus the large-object copy no longer overlaps them.
   With `clone.allDatabases`, pgcopydb copies large objects after every index build, with or without the field.
   Thus the field changes nothing there.

## Binary COPY

`clone.useCopyBinary` sends `COPY WITH (FORMAT BINARY)`, and it is on by default.
Text-format COPY encodes `bytea` as hex, with two wire bytes per data byte.
The worker relays every row between source and target, so both legs pay the cost.
On a database whose bytes are mostly `bytea`, this is a large saving.
On a database that is mostly narrow rows, the saving is close to nothing.

It is safe to leave on, because pgcopydb makes the choice per table, not once for the migration.
pgcopydb checks every column of a table against the source catalog.
When the binary encoding of any column is not safe, pgcopydb uses text COPY for that table instead, and logs a notice.
Set `useCopyBinary: false` to force text everywhere.

## Knobs left at pgcopydb's default, and why

### `largeObjectsJobs`

It stays at pgcopydb's four.
The reason is that no value is right for both kinds of database, and the operator cannot tell which kind it migrates.

Large objects live in `pg_largeobject`.
A pool of their own copies them, separate from the table COPY pipeline.
The pool has one metadata worker plus N data workers, and each worker holds a source connection and a target connection.
Thus the default costs four connection pairs on both servers, whether or not there is anything to move through them.
On a database with no large objects, that is eight connections that do nothing.
If you drop it to 1, a genuinely blob-heavy database loses most of its parallelism in the one phase that needed it.

Ask the source which case you are in:

```sql
SELECT count(*), pg_size_pretty(sum(pg_column_size(data))) FROM pg_largeobject;
```

- **No rows:** use `skip: [largeObjects]`.
  Then pgcopydb does not create the pool at all.
  This is better than a job count of 1, because it also skips the metadata pass.
- **A handful, or a few MB:** set `largeObjectsJobs: 1`.
  The copy is short either way, and the connections are more useful elsewhere.
- **Many, or a large total:** leave the default.
  Raise it if the large-object phase is visibly the tail of your migration.

> [!note]
> Large objects are not the same thing as `bytea`.
> A column of type `bytea` is ordinary table data, however big it is, and the table workers copy it.
> Only `lo_*` and `oid`-referenced objects go through this pool.
> Thus most databases want `skip: [largeObjects]`, not a job count.

### `restoreJobs`

It follows `indexJobs` unless you set it.
This is pgcopydb's behaviour, not the operator's.
When you lower `indexJobs` to protect the target's memory, this silently lowers the restore parallelism too.
This happens even though `pg_restore` runs as a separate process and never receives the 1GB `maintenance_work_mem` that constrains index jobs.
Whether that is a problem depends on whether the target is short of memory or short of cores.
Again, the operator cannot see this.
Set both explicitly when they should differ.

## Tuning the target

These settings are worth changing on a target while the migration loads it.
They are also safe to keep on a target that becomes production.

`max_wal_size` is the one that matters most.
A bulk load reaches it long before `checkpoint_timeout`, and every checkpoint restarts full-page-image writing for every page touched afterwards.
Size it against the volume, not as a flat number.
WAL lives inside `PGDATA`.
Thus a value that is comfortable on a large volume fills a small volume.
Then the server stops with a low-disk condition.

`maintenance_work_mem` on the server is mostly irrelevant during the index phase, because pgcopydb overrides it per index worker, as the section above describes.
It still applies to anything you build yourself afterwards.

`shared_buffers` and `effective_cache_size` matter on both sides.
`checkpoint_timeout` only binds when the load is slower than `max_wal_size` divided by it.
Thus it has no effect on a fast load.

Two settings are not worth changing:

`synchronous_commit = off` gives close to no gain during a base copy, because pgcopydb copies a whole table, or one split part, in a single transaction.
There is one commit per table, not per row.
Follow-mode transactions may be small and frequent.
But the bundled runner explicitly sets `synchronous_commit=on` for SQLite apply.
This overrides a server default of `off`, to make sure that the target WAL is durable before the runner publishes progress.
[`spec.target.synchronousCommit: local`](../configuration.md#synchronous-commit-on-the-target) is for availability under strict synchronous replication, not for speed.

`full_page_writes = off` is not safe on a target that becomes production.
CloudNativePG defaults `wal_log_hints` to `on`.
This setting WAL-logs hint-bit full-page images, even without data checksums.
When you enable data checksums, they require that logging independently.

## The VACUUM tail

pgcopydb runs `VACUUM ANALYZE` per table, with a worker pool that it sizes from `tableJobs`.
The pool runs alongside the copy, unless `clone.deferIndexes` moves the pool after the copy.
A table's vacuum cannot start until that table's own copy finishes, and the largest table finishes last.
Without `clone.deferIndexes`, the end of a clone therefore routinely narrows to a single `VACUUM ANALYZE` on the biggest table.
This vacuum runs alone while every other worker sits idle.

On the e2e fixture, one table holds 73% of the bytes.
There, that tail measured roughly a third of the clone's wall clock: 19404ms with it against 13253ms without.
The two runs were a clean pair, with a fresh target database per arm.
The operator reports this tail as the `Finalizing` phase.
In this phase, the target stops growing, unless `clone.deferIndexes` moved every index build there.
Thus estimates that come from the size can read as finished while real work continues.
[Conditions and reasons](../reference/conditions.md#phases) has the full phase table and what runs inside each phase.
With `clone.deferIndexes`, no vacuum starts before every table copy finishes.
The vacuum of each table follows the index builds and constraints of that table, and `Finalizing` holds every index build as well.

Skipping the vacuum recovers that time:

```yaml
spec:
  clone:
    skip: [vacuum]
```

> [!important]
> The target then has no fresh statistics.
> The planner will use whatever it had.
> On a freshly restored database, that is nothing.
> Thus the first real queries after cutover can choose badly.
> Run `ANALYZE` on the target yourself before you point traffic at it.
> One bulk `ANALYZE` is better than per-table vacuums that compete with the copy or the index builds.

The operator does not skip it by default, because a target that silently lacks statistics is invisible until a query plan goes wrong in production.

## What the defaults are worth, measured

We measured two source shapes on the same cluster.
The clones of both shapes used binary COPY and skipped the vacuum.
Each rate is the source database size divided by the wall clock.

The first shape is a **production-shaped** database: 15 tables that total 1010MB.
The largest table has 31% of the bytes, and no table is past the split threshold.
Thus every stream comes from `tableJobs`:

| `tableJobs` | wall clock | rate |
|---|---|---|
| 2 | 7548 ms | 139.7 MiB/s |
| 4 | 4996 ms | 211.1 MiB/s |
| 8 | 5299 ms | 199.0 MiB/s |
| 16 | 5181 ms | 203.5 MiB/s |

Throughput scales to four jobs and then flattens, which is where the default sits.

The second shape is the **one-dominant-table** shape: 2909MB, with a single TOASTed table that holds 73% of it.
This shape is the harder case, and it runs at roughly half the rate.
There, `tableJobs` cannot help, because a table is one COPY stream unless pgcopydb splits it.
Further splitting makes it slower:

| split threshold | max parts | rate |
|---|---|---|
| 512Mi (default) | 8 (default) | 102.4 MiB/s |
| 256MB | 16 | 80.7 MiB/s |
| 128MB | 32 | 57.2 MiB/s |

More parts mean more writers that contend on one relation, and that costs more than the added parallelism returns.
Leave both at their defaults.

The ceiling sits where it does because of round-trip latency.
A copy worker in the middle of a COPY uses 7-12% CPU.
It moves roughly 14kB per socket round trip, one round trip at a time.
That latency, not CPU, disk or network, bounds each stream.
Thus throughput is the number of streams multiplied by what one stream sustains.
Adding streams therefore helps until the servers saturate, and adding jobs with no streams to fill does nothing.

## Measuring

Two clones of identical data on the same cluster minutes apart measured 666s and 399s.
That is a 67% spread from thin provisioning alone.
The first run writes into freshly allocated blocks, and the second run overwrites blocks that already exist.
Thus a single before-and-after pair proves nothing.
Give every arm a freshly provisioned target volume.
Make at least three runs.
Report the median and the range.

Watch the divisor too.
When you divide the source database size by wall clock, you overstate throughput by roughly 16%.
This is because the size of a relation counts its indexes, page and tuple headers, alignment padding and free space.
A COPY stream carries none of them.
One run moved 5078 MB against a 5886 MiB source.
pgcopydb prints its own summary at the end of every clone, and this summary is the accurate source.
Its `COPY (cumulative)` row gives both the bytes and the time.

The operator exports `pgcopydb_migration_start_time_seconds` and `pgcopydb_migration_completion_time_seconds`.
The difference between them is the duration, and that is the number to compare.
`pgcopydb_migration_clone_copied_bytes` and `pgcopydb_migration_source_database_size_bytes` give you a rate to divide it into.
For `pgcopydb compare data`, `pgcopydb_migration_verification_duration_seconds` and `pgcopydb_migration_verification_compared_bytes` are the same pair, per kind of run (see [Progress](verification.md#progress)).
[Monitoring](monitoring.md) has the full metric reference.

For per-table detail, run `pgcopydb list progress --summary --json` against a finished Migration's work PVC (`<name>-work`) from a short-lived pod that mounts it.
Never run it with `kubectl exec` into a running worker.
Every pgcopydb invocation writes to the catalog.
An invocation that lands while a worker is mid-cursor kills that worker.
`list progress` reports per-step and per-table timings, which is how you find which table was slow.
On stock 0.18, skip it when the Migration used filters.
`list progress` overwrites the stored filtering and poisons later resume (see the [upstream drafts](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/upstream-issues.md)).
The bundled runner fixes that filter corruption, but the restriction against concurrent catalog access still applies.
