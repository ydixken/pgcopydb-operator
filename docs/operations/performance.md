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

Everything else is pgcopydb's default: four index jobs, restore jobs following index jobs, four large-object jobs, text-format COPY.

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

Check the target's `max_connections` against `tableJobs * 2 + indexJobs` before raising anything.
`skip: [vacuum]` removes the vacuum half if the target will be analysed separately after cutover.

`clone.largeObjectsJobs` is worth setting to 1, or skipping with `skip: [largeObjects]`, unless the database is genuinely blob-heavy.
The default of four opens four connection pairs whether or not there is anything to move through them.

## Binary COPY

`clone.useCopyBinary` sends `COPY WITH (FORMAT BINARY)`.
Text-format COPY encodes `bytea` as hex, two wire bytes per data byte, and the worker relays every row between source and target, so the cost is paid on both legs.
On a database whose bytes are mostly `bytea`, this is a large saving; on one that is mostly narrow rows, it is close to nothing.
pgcopydb guards it per table and falls back to text where a type is not safe in binary.

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

## Measuring

Tune nothing you cannot measure.
Two runs on a shared cluster vary on their own, so treat any change smaller than the spread between two identical runs as noise.

The operator exports `pgcopydb_migration_start_time_seconds` and `pgcopydb_migration_completion_time_seconds`.
Subtracting them gives the duration, which is the number to compare.
`pgcopydb_migration_clone_copied_bytes` and `pgcopydb_migration_source_database_size_bytes` give you a rate to divide it into.
[Monitoring](monitoring.md) has the full metric reference.

For per-table detail, `pgcopydb list progress --summary --json` inside the worker's work directory reports per-step and per-table timings, which is the only way to answer which table was slow.
