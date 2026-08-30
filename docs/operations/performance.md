# Performance tuning

A migration's wall clock is governed by three separate bottlenecks, and they respond to different knobs.
The base copy is bound by the source read, the network, and the target write.
Index builds are bound by the target's CPU and memory, and they happen after the data is in.
In follow mode, the steady state is bound by how fast the target applies changes, not by how fast the source decodes them.

This page covers what the operator decides for you, what is left to you, and how to tell whether a change helped.

## What the operator decides

`spec.clone` fields are optional, and a zero value means the operator decides.
For most fields that means pgcopydb's own default.
Three it overrides, because pgcopydb's defaults are wrong for a migration rather than for a copy.

| Field | Unset behaviour | Why |
|-------|-----------------|-----|
| `runner.resources` | 4 CPUs, 4Gi, requests only | pgcopydb runs four table jobs by default, and a worker that requests less CPU than its own concurrency starves it. |
| `clone.tableJobs` | Follows the worker's CPU request, minimum 4 | One knob instead of two: raise the worker and the copy concurrency follows. |
| `clone.splitTablesLargerThan` | `512Mi` | pgcopydb ships this disabled, which leaves one large table to one worker however many table jobs are running. |
| `clone.splitMaxParts` | `8` | Without a cap, a very large table fans out into hundreds of parts and catalog rows. |
| `clone.useCopyBinary` | `true` | Text COPY doubles `bytea` on the wire, and the worker relays every row, so the cost lands on both legs. |

Everything else is pgcopydb's default: four index jobs, restore jobs following index jobs, four large-object jobs.

> [!important]
> Raising `spec.runner.resources.requests.cpu` is the single knob most migrations need.
> It sizes the worker and the copy concurrency together, so there is no second field to keep in step.

## Same-table concurrency

This is the largest available win for the ordinary shape of a database, which is a handful of tables holding most of the bytes.
Without it, `--table-jobs` parallelises *across* tables only: the biggest table gets one worker, and the clone cannot finish before that one worker does.
With it, a table past the threshold is split into parts that are handed to separate workers out of the same table-jobs pool.

The part count is the table size divided by the threshold, capped by `splitMaxParts`.
Lowering the threshold makes more tables eligible and splits large ones further.

Three things decide whether it engages at all, and two of them fail quietly.

A table needs a single-column integer primary or unique key to be split on key ranges.
Without one, pgcopydb falls back to splitting on `ctid`, which follows physical layout rather than key order, so each part is a separate scan over its own page range.
`skip: [ctidSplit]` disables that fallback if the source cannot afford the extra scans.

> [!warning]
> pgcopydb disables same-table concurrency when the source connection lands on a standby, and says nothing about it.
> A migration pointed at a read replica gets no splitting and no warning.

On a `NOT NULL` key, one of the parts is the NULL range and copies zero rows, so a cap of 8 yields 7 useful parts.
Splitting also gives up the `COPY FREEZE` path for that table, which is usually a good trade for a table large enough to be worth splitting.

## Index jobs, and the 1GB you cannot see

`clone.indexJobs` has no operator default, because its cost lands on a machine the operator cannot see.

Each index worker opens one target connection and immediately sets `maintenance_work_mem` to 1GB.
pgcopydb hardcodes that and applies it per worker, **overriding whatever the target itself is configured with**.
So the real cost of this number is roughly `indexJobs` GB of target memory, and pgcopydb's default of four asks for 4GB.

Size it against the target: its core count, minus what the COPY workers are already using there, and no more than its memory can carry at 1GB each.
On a small target, four is already too many, and lowering it to 2 is a speed-up rather than a sacrifice.

`clone.restoreJobs` follows `indexJobs` unless you set it.
Set it separately when they differ: `pg_restore` is a separate process and never receives that GUC, so it is not bound by the same memory ceiling.

## Table jobs and the connections they cost

Each table job holds one source connection and one target connection.
pgcopydb also sizes its `VACUUM ANALYZE` pool from the same number and runs it alongside the copy, so N table jobs can mean up to 2N concurrent backends on the target.

Check the target's `max_connections` before raising anything.
The worst case is roughly `tableJobs * 2 + indexJobs + largeObjectsJobs + 1`: table jobs twice over because of the vacuum pool, then one per index worker, one per large-object worker, and one for its metadata pass.
The source sees the same table and large-object connections, without the index and vacuum ones.
`skip: [vacuum]` removes the vacuum half if the target will be analysed separately after cutover.

`clone.largeObjectsJobs` is worth setting to 1, or skipping with `skip: [largeObjects]`, unless the database is genuinely blob-heavy.
The default of four opens four connection pairs whether or not there is anything to move through them.

## Binary COPY

`clone.useCopyBinary` sends `COPY WITH (FORMAT BINARY)`, and it is on by default.
Text-format COPY encodes `bytea` as hex, two wire bytes per data byte, and the worker relays every row between source and target, so the cost is paid on both legs.
On a database whose bytes are mostly `bytea`, this is a large saving; on one that is mostly narrow rows, it is close to nothing.

It is safe to leave on because the choice is made per table, not once for the migration.
pgcopydb checks every column of a table against the source catalog and falls back to text COPY for that table when any column's binary encoding is not safe, logging a notice when it does.
Set `useCopyBinary: false` to force text everywhere.

## Knobs left at pgcopydb's default, and why

### `largeObjectsJobs`

Stays at pgcopydb's four, because there is no value that is right for both kinds of database and the operator cannot tell which one it is pointed at.

Large objects live in `pg_largeobject` and are copied by a pool of their own, separate from the table COPY pipeline: one metadata worker plus N data workers, each holding a source connection and a target connection.
So the default costs four connection pairs on both servers whether or not there is anything to move through them, and on a database with no large objects that is eight connections doing nothing.
Drop it to 1 and a genuinely blob-heavy database loses most of its parallelism in the one phase that needed it.

Ask the source which case you are in:

```sql
SELECT count(*), pg_size_pretty(sum(pg_column_size(data))) FROM pg_largeobject;
```

- **No rows.** Use `skip: [largeObjects]`. The pool is then not created at all, which is better than setting the job count to 1, because it also skips the metadata pass.
- **A handful, or a few MB.** Set `largeObjectsJobs: 1`. The copy is short either way and the connections are better spent elsewhere.
- **Many, or a large total.** Leave the default, and raise it if the large-object phase is visibly the tail of your migration.

> [!note]
> Large objects are not the same thing as `bytea`.
> A column of type `bytea`, however big, is ordinary table data and is copied by the table workers.
> Only `lo_*` and `oid`-referenced objects go through this pool, so most databases want `skip: [largeObjects]` rather than a job count.

### `restoreJobs`

Follows `indexJobs` unless you set it, which is pgcopydb's behaviour and not the operator's.
That coupling is worth knowing about: lowering `indexJobs` to protect the target's memory silently lowers the restore parallelism too, even though `pg_restore` runs as a separate process and never receives the 1GB `maintenance_work_mem` that constrains index jobs.
Whether that is a problem depends on whether the target is short of memory or short of cores, which again is not something the operator can see.
Set both explicitly when they should differ.

## Tuning the target

These are worth changing on a target that is being loaded, and safe to leave in place on one that becomes production.

`max_wal_size` is the one that matters most.
A bulk load reaches it long before `checkpoint_timeout`, and every checkpoint restarts full-page-image writing for every page touched afterwards.
Size it against the volume, not as a flat number: WAL lives inside `PGDATA`, so a value comfortable on a large volume fills a small one and stops the server with a low-disk condition.

`maintenance_work_mem` on the server is largely moot during the index phase, because pgcopydb overrides it per index worker as described above.
It still applies to anything you build yourself afterwards.

`shared_buffers` and `effective_cache_size` matter on both sides.
`checkpoint_timeout` only binds when the load is slower than `max_wal_size` divided by it, so on a fast load it is inert.

Two that circulate as advice and are not worth taking:

`synchronous_commit = off` buys close to nothing during a base copy, because pgcopydb copies a whole table, or one split part, in a single transaction.
There is one commit per table, not per row.
It does matter during follow-mode apply, where transactions are small and frequent.

`full_page_writes = off` is not safe on a target that becomes production, and is not safe at all on a CloudNativePG cluster, which enables `wal_log_hints` and data checksums.

## The VACUUM tail

pgcopydb runs `VACUUM ANALYZE` per table alongside the copy, with a worker pool sized from `tableJobs`.
The catch is ordering: a table's vacuum cannot start until that table's own copy finishes, and the largest table finishes last.
So the end of a clone routinely narrows to a single `VACUUM ANALYZE` on the biggest table, running alone while every other worker sits idle.

On the e2e fixture, where one table holds 73% of the bytes, that tail measured roughly a fifth of the clone's wall clock.
The operator reports it as the `Finalizing` phase precisely because it looks like a stall and is not: the target has stopped growing, so every size-derived estimate reads as finished while real work continues.
[Conditions and reasons](../reference/conditions.md#phases) has the full phase table and what runs inside each one.

You can have that time back:

```yaml
spec:
  clone:
    skip: [vacuum]
```

> [!important]
> The target is then left without fresh statistics.
> The planner will use whatever it had, which on a freshly restored database is nothing, so the first real queries after cutover can choose badly.
> Run `ANALYZE` on the target yourself before pointing traffic at it, and the trade is a good one: one bulk `ANALYZE` beats per-table vacuums competing with the copy.

The operator does not skip it by default, because a migration target that silently lacks statistics is a worse failure than a slow one: it is invisible until a query plan goes wrong in production.

## Measuring

Tune nothing you cannot measure, and be careful what you measure with.

Two clones of identical data on the same cluster minutes apart measured 666s and 399s.
That is a 67% spread from thin provisioning alone: the first run writes into freshly allocated blocks, the second overwrites blocks that already exist.
So a single before-and-after pair proves nothing.
Give every arm a freshly provisioned target volume, run at least three, and report the median and the range.

Watch the divisor too.
Dividing the source database size by wall clock overstates throughput by roughly 16%, because a relation's size counts its indexes, page and tuple headers, alignment padding and free space, and a COPY stream carries none of them.
One run moved 5078 MB against a 5886 MiB source.
pgcopydb's own summary, printed at the end of every clone, is the honest source: its `COPY (cumulative)` row gives both the bytes and the time.

The operator exports `pgcopydb_migration_start_time_seconds` and `pgcopydb_migration_completion_time_seconds`.
Subtracting them gives the duration, which is the number to compare.
`pgcopydb_migration_clone_copied_bytes` and `pgcopydb_migration_source_database_size_bytes` give you a rate to divide it into.
[Monitoring](monitoring.md) has the full metric reference.

For per-table detail, `pgcopydb list progress --summary --json` inside the worker's work directory reports per-step and per-table timings, which is the only way to answer which table was slow.
Run it only against a finished migration's work volume, and not at all when the Migration used filters: every pgcopydb invocation writes to the catalog, so one landing while a worker is mid-cursor kills that worker, and `list progress` additionally overwrites a filtered work dir's stored filtering (both in the [upstream drafts](../research/upstream-issues.md)).
