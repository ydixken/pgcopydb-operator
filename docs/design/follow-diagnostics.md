# Follow diagnostics

The early-cutover E2E spec samples source replication feedback and the target marker-row count roughly every 30 seconds after resuming its paused sender.
Sampling stops when cutover starts or the 12-minute backlog drain deadline expires.
A snapshot shares a two-second probe budget, capped by the remaining deadline, and each SQL statement has a one-second timeout.
Delayed polls skip missed intervals rather than issuing catch-up probes.

## What the snapshot measures

`source_head/write/replay/confirmed_bytes` contains WAL byte positions measured from `0/0`.
Subtract positions across snapshots and divide by elapsed time to estimate progress in WAL bytes per second, not payload bytes per second.
`source_write/replay/confirmed_samples` contains presence counts, each zero or one.
A position with a zero presence count is unavailable, not measured zero.
`source_active/streaming` contains zero-or-one counts for slot activity and streaming state.
`target_marker_rows` counts committed rows from the test's burst.
An absent slot, failed probe, or invalid projection is reported as unavailable.

Only byte positions and counts leave the database pods on stdout; remote stderr is discarded.
The caller validates the numeric projection before logging it and does not print raw probe errors.
The snapshot does not run pgcopydb, open the worker's catalogs, inspect SQL or row contents, or add worker filesystem paths.

## What the snapshot can distinguish

The [pinned feedback implementation](https://github.com/ydixken/pgcopydb/blob/e37d2bd4dd10b7ed7b415555ce3318202d9633cf/src/bin/pgcopydb/ld_stream.c#L1495-L1551) reports receive progress as `write_lsn` and target replay progress as `replay_lsn`.
It also uses replay progress for the slot's confirmed-flush feedback, so confirmed flush is not an independent transformation boundary.

A slowly advancing write position below the quiet source's head demonstrates slow receive-side progress.
A write position that has caught up while replay and target rows remain behind localizes the remaining work downstream of receive.
Neither observation identifies the cause of slow receive, nor separates inline transformation from target apply.
Source WAL includes activity outside the published tables, so a nonzero head-to-replay gap alone does not prove missing rows.
The burst commits as one transaction: target rows can remain zero during healthy work and appear together at commit.

> [!warning]
> These snapshots are not a receive/transform/apply classifier.
> Do not label downstream delay as apply-bound, or a flat file size as a stalled process.

## Why SQLite file sizes are not the replacement

The pinned pipeline writes received changes to output SQLite databases and transformed statements to replay SQLite databases.
The [apply loop invokes transformation inline](https://github.com/ydixken/pgcopydb/blob/e37d2bd4dd10b7ed7b415555ce3318202d9633cf/src/bin/pgcopydb/ld_apply.c#L314-L353), and both stages share one process and an in-memory progress record.
That record is [persisted every 64 driver iterations](https://github.com/ydixken/pgcopydb/blob/e37d2bd4dd10b7ed7b415555ce3318202d9633cf/src/bin/pgcopydb/ld_apply.c#L234-L240), not at a fixed wall-clock interval.
Per-process CPU and I/O counts therefore do not separate the two stages, and the persisted record need not describe a long-running operation.

[SQLite WAL mode](https://www.sqlite.org/wal.html#avoiding_excessively_large_wal_files) normally recycles a checkpointed WAL file without truncating it.
Database and WAL lengths are storage footprints, not cumulative bytes written or counts of unprocessed changes.
Rotation, cleanup, page reuse, and checkpoint timing also affect their changes.
Small synchronous writes make storage latency a plausible throughput constraint, but file lengths do not measure sync count, latency, or time spent waiting for storage.
We do not add those sizes as stage diagnostics because they cannot answer the attribution question reliably.

Full three-stage attribution requires an independently observable transformation boundary, such as bounded read-only counts of received and transformed complete transactions, or stage-specific work and wait counters emitted by the worker.
Neither is implemented here; any worker probe must first have its paths and semantics verified on a live worker without invoking pgcopydb.

## Backlog drain budget

We use `backlogDrainTimeout = 12 * time.Minute` for early-cutover catch-up and progress-bounds recovery after unlocking.
Each wait and its probes share one deadline.
The five-minute `lagConvergeTimeout` still distinguishes idle catch-up from a stall; its existing stress-tier override is unchanged.
The 20,000-row burst, 16Mi allowance, EXTERNAL payload, and recovery batch shape are unchanged.

The A/B/A replay on 2026-09-13 (roughly 20:18 to 20:55 UTC) used the rc.5 paused-walsender scenario, digest-pinned images, fixed placement, and unchanged storage configuration.
It measured:

| CPU policy | Receive progress | Resume to cutover | 300-second gate |
| --- | --- | --- | --- |
| A1 powersave | 84.3 KB/s | 347.2 s | Missed |
| B performance | 123.5 KB/s | 236.9 s | Met |
| A2 powersave restored | 85.5 KB/s | 337.3 s | Missed |

Rates are decimal KB/s of source WAL position progress, not network throughput.
A2 returned within 1.5% of A1, but performance mode did not restore the historical 170 KB/s.
Both powersave phases exceeded 300 seconds, and CI remains on powersave.
The measurements establish a need for roughly 350 seconds plus operating headroom, not a measured 12-minute requirement.
We allow 12 minutes because the throughput floor remains unexplained; the trade-off is slower reporting of a genuine failure.

> [!important]
> This budget is environmental accommodation, not a resolution of the throughput investigation in [#260](https://github.com/ydixken/pgcopydb-operator/issues/260).
> The A/B/A experiment did not exercise the progress-bounds spec, so its fresh-seed behavior remains unproven.
