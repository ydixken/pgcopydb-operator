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

The [pinned feedback path](https://github.com/ydixken/pgcopydb/blob/ea2dc96a47c2f7676d71a4967d044a1e469e4110/src/bin/pgcopydb/ld_stream.c#L1490-L1546) reports receive progress as `write_lsn` and source-visible replay feedback as `replay_lsn`.
The bundled runner, pgcopydb `0.18.15.gea2dc96`, certifies genuine primary keepalive positions from the current connection for replay and flush feedback when initialized durable apply covers all stored, non-skipped COMMITs, including retained spool, no receive transaction is open, and endpos is unset.
Synthetic keepalives and WAL data headers cannot establish that boundary.
Confirmed flush is therefore not an independent transformation boundary, and `replay_lsn` is not strictly the last applied data transaction's position.
The [apply path confirms target COMMIT results](https://github.com/ydixken/pgcopydb/blob/ea2dc96a47c2f7676d71a4967d044a1e469e4110/src/bin/pgcopydb/ld_apply.c#L1013-L1034) with [`synchronous_commit=on`](https://github.com/ydixken/pgcopydb/blob/ea2dc96a47c2f7676d71a4967d044a1e469e4110/src/bin/pgcopydb/ld_apply.c#L37-L42) before advancing its data cursor.
Keepalive certification changes network feedback without advancing that cursor, the target replication origin, or the sentinel's data replay position.
The other supported runner versions do not all provide this certification; see [client tool versions](../reference/prerequisites.md#client-tool-versions).

A slowly advancing write position below the quiet source's head demonstrates slow receive-side progress.
A write position that has caught up while replay and target rows remain behind localizes the remaining work downstream of receive.
Neither observation identifies the cause of slow receive, nor separates inline transformation from target apply.
Source WAL includes activity outside the published tables, so a nonzero head-to-replay gap alone does not prove missing rows.
The burst commits as one transaction: target rows can remain zero during healthy work and appear together at commit.

> [!warning]
> These snapshots are not a receive/transform/apply classifier.
> Do not label downstream delay as apply-bound, or a flat file size as a stalled process.

### The zero-guard window

The sentinel seeds `replay_lsn` at `0/0`, and the [override that reports the apply cursor as the feedback flush position](https://github.com/ydixken/pgcopydb/blob/ea2dc96a47c2f7676d71a4967d044a1e469e4110/src/bin/pgcopydb/ld_stream.c#L1526-L1546) is guarded on that value being non-zero.
Until the apply loop's first sentinel sync the worker keeps confirming its raw receive position instead, which is why the lag reads near zero whatever the apply backlog is.
The worker also reports its apply position as `0/0` throughout that window, and PostgreSQL renders an invalid apply position as NULL, so `pg_stat_replication.replay_lsn` is NULL for exactly as long.
`readScript` prefers that column and falls back to the slot's `confirmed_flush_lsn`, so the fallback lands on the polluted value.
A walsender fallback is the obvious repair, and it is already in place and does not help.

We answer the window with the `CaughtUp` latch rather than a better probe.
A single below-threshold reading taken inside it cannot flip the condition: a wrong verdict needs the next sample, a poll interval later, to find the window still open (see [`ConfirmingCatchUp`](../reference/conditions.md#reasons)).
The stream is reported but not acted on until the base copy completes, which covers a fresh start and leaves roughly one exposed sample at clone end and one after each worker restart.
The latch costs a poll interval of cutover latency and prevents an endpos frozen at a source position the target has not applied to.

The `0/0` seed, the zero guard, and the apply field of the feedback message are [upstream v0.18 code](https://github.com/dimitri/pgcopydb/blob/95ebd553790fa45de67c92b934917d777131bdd3/src/bin/pgcopydb/ld_stream.c#L1548-L1551), not additions of the bundled runner.
The keepalive certification above is such an addition; the zero guard is not, so moving off that runner to stock v0.18 would keep this window.

## Idle-feedback regression

The keepalive-feedback E2E cases hold a published table frozen while unpublished WAL keeps advancing on the source.
They require source replay, flush, and slot confirmed-flush feedback to cross a captured WAL boundary more than the default catch-up allowance beyond the durable target origin, while that origin and the exact published rows stay unchanged.
The cases withhold approval during this observation, so neither automatic cleanup nor the post-cutover logical-message nudge can mask a failure.

A second filtered-WAL burst runs with only the migration's walsender paused.
The operator must report `Lagging`, then `Streaming` with no endpos while the sender is still paused.
Resuming the sender must lead to `Completed`, `CutoverCompleted`, and successful cleanup without published writes or a raised `maxCatchupLag`.
In Automatic mode the confirmed catch-up verdict alone triggers cutover; the case keeps `approved: false`.

## Why SQLite file sizes are not the replacement

The pinned pipeline writes received changes to output SQLite databases and transformed statements to replay SQLite databases.
The [apply loop invokes transformation inline](https://github.com/ydixken/pgcopydb/blob/ea2dc96a47c2f7676d71a4967d044a1e469e4110/src/bin/pgcopydb/ld_apply.c#L314-L353), and both stages share one process and an in-memory progress record.
That record is [checkpointed after every 64 inline-transform passes that make progress](https://github.com/ydixken/pgcopydb/blob/ea2dc96a47c2f7676d71a4967d044a1e469e4110/src/bin/pgcopydb/ld_apply.c#L400-L408), not at a fixed wall-clock interval.
Per-process CPU and I/O counts therefore do not separate the two stages, and the persisted record need not describe a long-running operation.

[SQLite WAL mode](https://www.sqlite.org/wal.html#avoiding_excessively_large_wal_files) normally recycles a checkpointed WAL file without truncating it.
Database and WAL lengths are storage footprints, not cumulative bytes written or counts of unprocessed changes.
Rotation, cleanup, page reuse, and checkpoint timing also change those lengths.
Small synchronous writes make storage latency a plausible throughput constraint, but file lengths do not measure sync count, latency, or time spent waiting for storage.
We do not add those sizes as stage diagnostics because they cannot answer the attribution question reliably.

Full three-stage attribution requires an independently observable transformation boundary, such as bounded read-only counts of received and transformed complete transactions, or stage-specific work and wait counters emitted by the worker.
Neither is implemented here; any worker probe must first have its paths and semantics verified on a live worker without invoking pgcopydb.

## Backlog drain budget

We use `backlogDrainTimeout = 12 * time.Minute` for early-cutover catch-up and progress-bounds recovery after unlocking.
Each wait and its probes share one deadline.
The five-minute `lagConvergeTimeout` distinguishes idle catch-up from a stall, and keeps its stress-tier override.
The scenario keeps its 20,000-row burst, 16Mi allowance, EXTERNAL payload, and recovery batch shape.

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
Those rc.5 measurements established a need for roughly 350 seconds plus operating headroom, not a measured 12-minute requirement.
The [receive batching fix](../operations/performance.md#follow-receive-and-apply) addresses the per-insert sync cost behind that ceiling.
We retain the 12-minute budget until measurements with the bundled runner justify a smaller one; the trade-off is slower reporting of a genuine failure.

> [!important]
> This budget is environmental accommodation, not a resolution of the throughput investigation in [#260](https://github.com/ydixken/pgcopydb-operator/issues/260).
> The A/B/A experiment did not exercise the progress-bounds spec, so its fresh-seed behavior remains unproven.
