# Follow diagnostics

The early-cutover E2E spec samples source replication feedback and the target marker-row count roughly every 30 seconds after it resumes its paused sender.
Sampling ends when cutover starts or when the 12-minute backlog drain deadline expires.
The probes of one snapshot share a two-second budget, and the remaining deadline caps that budget.
Each SQL statement has a one-second timeout.
Delayed polls skip missed intervals instead of issuing catch-up probes.

## What the snapshot measures

`source_head/write/replay/confirmed_bytes` contains WAL byte positions measured from `0/0`.
To estimate progress, subtract positions across snapshots.
Then divide the difference by the elapsed time.
The result is progress in WAL bytes per second, not payload bytes per second.
`source_write/replay/confirmed_samples` contains presence counts, each zero or one.
A position with a zero presence count is unavailable, not a measured zero.
`source_active/streaming` contains zero-or-one counts for slot activity and streaming state.
`target_marker_rows` counts committed rows from the test's burst.
The snapshot reports an absent slot, a failed probe, or an invalid projection as unavailable.

Only byte positions and counts leave the database pods, on stdout.
Remote stderr is discarded.
Before the caller logs the numeric projection, it makes sure that the projection is valid.
The caller does not print raw probe errors.
The snapshot does not run pgcopydb, open the worker's catalogs, inspect SQL or row contents, or add worker filesystem paths.

## What the snapshot can distinguish

The [pinned feedback path](https://github.com/ydixken/pgcopydb/blob/86bf908adca02d87063849dae775a02c1659c829/src/bin/pgcopydb/ld_stream.c#L1597-L1653) reports receive progress as `write_lsn` and source-visible replay feedback as `replay_lsn`.
The bundled runner is pgcopydb `0.18.144.g86bf908`.
For replay and flush feedback, it certifies genuine primary keepalive positions from the current connection when all of these conditions are true:

- Initialized durable apply covers every non-skipped COMMIT that receive has read.
  This includes retained spool, and COMMITs whose output batch has not committed yet.
- No receive transaction is open.
- The endpos is unset.

Synthetic keepalives and WAL data headers cannot establish that boundary.
So confirmed flush is not an independent transformation boundary, and `replay_lsn` is not strictly the position of the last applied data transaction.
The [apply path confirms target COMMIT results](https://github.com/ydixken/pgcopydb/blob/86bf908adca02d87063849dae775a02c1659c829/src/bin/pgcopydb/ld_apply.c#L1056-L1071) with [`synchronous_commit=on`](https://github.com/ydixken/pgcopydb/blob/86bf908adca02d87063849dae775a02c1659c829/src/bin/pgcopydb/ld_apply.c#L38-L44) before it advances its data cursor.
It commits several source transactions in one target transaction, an [apply group](../operations/performance.md#apply-groups).
It sends the origin setup and the group `COMMIT` as one query.
When the target refuses that `COMMIT` and the connection stays up, it moves the origin back ([fork PR #23](https://github.com/ydixken/pgcopydb/pull/23)).
If a target backend is terminated after the origin setup and before the `COMMIT` record is written, the origin can still stay advanced.
This is because apply cannot move the origin back over a dead connection.
A `COMMIT` that fails and breaks the connection can also leave the origin advanced.
An example is a target restart during the `COMMIT`.
Keepalive certification changes network feedback without advancing that cursor, the target replication origin, or the sentinel's data replay position.
Not all of the other supported runner versions provide this certification.
See [client tool versions](../reference/prerequisites.md#client-tool-versions).

A slowly advancing write position below the quiet source's head demonstrates slow receive-side progress.
A write position that has caught up while replay and target rows remain behind localizes the remaining work downstream of receive.
Neither observation identifies the cause of slow receive, nor separates inline transformation from target apply.
Source WAL includes activity outside the published tables, so a nonzero head-to-replay gap alone does not prove missing rows.
The burst commits as one transaction: target rows can remain zero during healthy work and appear together at commit.

> [!warning]
> These snapshots are not a receive/transform/apply classifier.
> Do not label downstream delay as apply-bound.
> Do not label a flat file size as a stalled process.

### The zero-guard window

The sentinel seeds `replay_lsn` at `0/0`.
The [override that reports the apply cursor as the feedback flush position](https://github.com/ydixken/pgcopydb/blob/86bf908adca02d87063849dae775a02c1659c829/src/bin/pgcopydb/ld_stream.c#L1633-L1653) has a guard: it applies only when that value is non-zero.
Until the first sentinel sync of the apply loop, the worker continues to confirm its raw receive position instead.
That is why the lag reads near zero, whatever the apply backlog is.
The worker also reports its apply position as `0/0` throughout that window.
PostgreSQL renders an invalid apply position as NULL, so `pg_stat_replication.replay_lsn` is NULL for exactly as long.
`readScript` prefers that column and uses the slot's `confirmed_flush_lsn` as the fallback, so the fallback returns the polluted value.
A walsender fallback is the obvious fix.
It is already in place, and it does not help.

We answer the window with the `CaughtUp` latch instead of a better probe.
A single below-threshold reading taken inside the window cannot flip the condition.
A wrong verdict needs the next sample, a poll interval later, to find the window still open (see [`ConfirmingCatchUp`](../reference/conditions.md#reasons)).
Until the base copy completes, the operator reports the stream but does not act on it.
This rule covers a fresh start.
It leaves roughly one exposed sample at clone end, and one after each worker restart.
The latch costs a poll interval of cutover latency.
It prevents an endpos frozen at a source position that the target has not applied to.

The `0/0` seed, the zero guard, and the apply field of the feedback message are [upstream v0.18 code](https://github.com/dimitri/pgcopydb/blob/95ebd553790fa45de67c92b934917d777131bdd3/src/bin/pgcopydb/ld_stream.c#L1548-L1551), not additions of the bundled runner.
The keepalive certification above is such an addition.
The zero guard is not, so a change from that runner to stock v0.18 would keep this window.

## Idle-feedback regression

The keepalive-feedback E2E cases hold a published table frozen while unpublished WAL continues to advance on the source.
They require source replay, flush, and slot confirmed-flush feedback to cross a captured WAL boundary.
That boundary is more than the default catch-up allowance beyond the durable target origin.
At the same time, that origin and the exact published rows must stay unchanged.
The cases withhold approval during this observation, so neither automatic cleanup nor the post-cutover logical-message nudge can mask a failure.

A second filtered-WAL burst runs with only the migration's walsender paused.
The operator must report `Lagging`, then `Streaming` with no endpos while the sender is still paused.
Resuming the sender must lead to `Completed`, `CutoverCompleted`, and successful cleanup without published writes or a raised `maxCatchupLag`.
In Automatic mode, the confirmed catch-up verdict alone triggers cutover.
The case keeps `approved: false`.

## Why SQLite file sizes are not the replacement

The pinned pipeline writes received changes to output SQLite databases and transformed statements to replay SQLite databases.
The [apply loop invokes transformation inline](https://github.com/ydixken/pgcopydb/blob/86bf908adca02d87063849dae775a02c1659c829/src/bin/pgcopydb/ld_apply.c#L404-L443), and both stages share one process and an in-memory progress record.
That record is [checkpointed after every 64 inline-transform passes that make progress](https://github.com/ydixken/pgcopydb/blob/86bf908adca02d87063849dae775a02c1659c829/src/bin/pgcopydb/ld_apply.c#L511-L519), not at a fixed wall-clock interval.
Per-process CPU and I/O counts therefore do not separate the two stages, and the persisted record need not describe a long-running operation.

[SQLite WAL mode](https://www.sqlite.org/wal.html#avoiding_excessively_large_wal_files) normally recycles a checkpointed WAL file without truncating it.
Database and WAL lengths are storage footprints, not cumulative bytes written or counts of unprocessed changes.
Rotation, cleanup, page reuse, and checkpoint timing also change those lengths.
Small synchronous writes make storage latency a plausible throughput constraint.
But file lengths do not measure sync count, latency, or time spent waiting for storage.
We do not add those sizes as stage diagnostics because they cannot answer the attribution question reliably.

Full three-stage attribution requires an independently observable transformation boundary.
Bounded read-only counts of received and transformed complete transactions are one example.
Stage-specific work and wait counters emitted by the worker are another example.
We implement neither of them here.
We must first prove the paths and semantics of any worker probe on a live worker, without invoking pgcopydb.

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
We keep the 12-minute budget until measurements with the bundled runner justify a smaller one.
The trade-off is slower reporting of a genuine failure.

> [!important]
> This budget is an accommodation for the environment.
> The receive batching fix closed [#260](https://github.com/ydixken/pgcopydb-operator/issues/260).
> But no bundled-runner measurement has yet justified a smaller budget.
> The A/B/A experiment did not exercise the progress-bounds spec, so its fresh-seed behavior remains unproven.
