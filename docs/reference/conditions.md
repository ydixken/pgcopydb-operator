# Conditions and reasons

This page lists the condition types and reason strings that the controller writes to `status.conditions`.
They are API contract: stable identifiers that `kubectl wait`, GitOps health assessments, and alerts can match on without reading messages.
Every condition is a `metav1.Condition` and carries `observedGeneration`, so a stale condition is detectable after a spec change.

`status.phase` summarizes conditions and worker progress for the printer column.
Its `Pending` value records the first observation of the controller, before conditions exist.
The API server does not initialize status when it creates the object.
Conditions stay authoritative for every outcome after that.

```sh
kubectl wait --for=condition=Complete migration/shop --timeout=1h
```

## Phases

`status.phase` is the printer column: one word for what the migration is doing now.
After the first `Pending` observation, it reflects conditions and what the worker is doing.
Automation should wait on conditions, not on phase strings.

| Phase | The operator is | Next |
|---|---|---|
| `Pending` | Persisted its first observation, before validation or provisioning | `Validating`, `Failed`, or `Suspended` |
| `Validating` | Materializing the spec and running the preflight Job | `Cloning`, `Completed` (a passed dry run), or `Failed` |
| `Cloning` | Running the worker: schema, then table data | `Finalizing`, `Streaming`, `Completed`, or `Failed` |
| `Finalizing` | Past the data copy, finishing indexes, constraints and vacuum. On a clone with `clone.ownerAfterRestore`, also running the handover of the restored objects after the worker exits | `Streaming`, `Verifying`, `Completed`, or `Failed` |
| `Streaming` | Applying changes from the replication slot (live migrations) | `CutoverPending`, or `Failed` |
| `CutoverPending` | Caught up and waiting for approval (`cutover.mode: Manual`) | `CuttingOver` |
| `CuttingOver` | Setting the end position, draining, proving the drain | `Verifying`, `Completed`, or `Failed` |
| `Verifying` | Running each requested `pgcopydb compare` | `Completed` |
| `Completed` | Finished, or a `spec.dryRun` preflight passed (terminal) | |
| `Failed` | Finished badly (terminal) | |
| `Suspended` | Holding, because `spec.suspend` is true | whatever it was doing |

No phase stands for the [retry gate](../operations/lifecycle.md#the-retry-gate).
A held retry keeps the phase that the failed attempt left, and `RetryHeld` reports the hold.
The one exception is a resumed Migration.
While the gate holds its next attempt, the phase moves from `Suspended` to the phase where that attempt resumes.
Thus the phase no longer reads as suspended.

### Inside `Cloning` and `Finalizing`

pgcopydb does not run its steps in sequence.
When the schema is in place, it starts the table copy, index builds, constraint creation, large-object copy and vacuum **all at once**.
The exception is `clone.deferIndexes`, which holds the index builds, constraints and vacuum until every table copy finishes.
pgcopydb then prints nothing until they finish, so the operator reports the copy and its tail as separate phases.

| Sub-state | Phase | What is running | How it looks |
|---|---|---|---|
| Catalog and schema | `Cloning` | Source catalog queries, `pg_dump`, pre-data `pg_restore` | Seconds. The target barely grows |
| Table data | `Cloning` | `tableJobs` COPY workers, plus, without `clone.deferIndexes`, index, constraint and vacuum workers on whatever has already finished | The bulk of the run. The target grows steadily |
| The tail | `Finalizing` | Index builds, constraints, and vacuum on the tables that finished last, or on every table with `clone.deferIndexes` | **The target stops growing** while work continues, unless `clone.deferIndexes` moved every index build here |
| Post-data | `Finalizing` | Post-data `pg_restore`: foreign keys and the rest | Seconds |

A table's vacuum cannot start until that table's own copy finishes, and the largest table finishes last.
Without `clone.deferIndexes`, a clone therefore routinely ends with one `VACUUM ANALYZE` still running while every other worker is idle.
With it, no vacuum starts before every table copy finishes.
The vacuum of each table follows the index builds and constraints of that table.
[The VACUUM tail](../operations/performance.md#the-vacuum-tail) has the measurement.

> [!important]
> During `Finalizing`, the target database stops growing, unless `clone.deferIndexes` moved every index build there.
> Thus anything that comes from its size can read as finished while real work continues.
> This is why `PgcopydbMigrationCloneStalled` matches `Cloning` alone: a long tail is not a stall.
> [Performance tuning](../operations/performance.md) explains how to trade the vacuum for that time.

The phase comes from a `pg_stat_activity` query on the target that touches no pgcopydb catalog.
If a client reads that catalog while the copy writes it, the workers die.
The query counts copy workers, other active pgcopydb backends, and index or vacuum workers separately.
Zero on both counts is the unknown answer, not the tail, so the phase stays where the last answered sample left it.

## Condition types

We named each type for what `True` means.
Eight are normal-true: True is the desired state.
Three are abnormal-true: True reports a problem.
`Failed` means the migration ended in failure, while `CommitsBlocked` and `RetryHeld` report a wait that clears once its cause is gone.

| Type | Polarity | True means |
|---|---|---|
| `Validated` | normal-true | The spec materializes cleanly and the preflight passed. |
| `CloneCompleted` | normal-true | The base copy finished. |
| `Streaming` | normal-true | Logical replication is applying changes (live migrations only). |
| `CaughtUp` | normal-true | Two consecutive samples measured the replication lag at or below `spec.follow.maxCatchupLag`. |
| `CutoverCompleted` | normal-true | The drain is proven: a clean data compare, or origin progress exactly at the cutover LSN on the rare cutover where the two coincide. With `clone.ownerAfterRestore` set, the handover finished first. |
| `OwnershipApplied` | normal-true | The restored objects belong to `clone.ownerAfterRestore`. The condition is present only when the spec sets that field. |
| `Verified` | normal-true | Every requested `pgcopydb compare` found source and target matching. |
| `Complete` | normal-true | The migration finished. Terminal and absorbing. |
| `Failed` | abnormal-true | The migration failed for good. Terminal and absorbing. |
| `CommitsBlocked` | abnormal-true | Two progress samples in a row found a target session of the migration role whose commit waits for a synchronous standby. That wait is 30 seconds or more. The condition is present from the first sample on. |
| `RetryHeld` | abnormal-true | The [retry gate](../operations/lifecycle.md#the-retry-gate) holds the next attempt. The condition is present from the first retry or resume on. |

## Reasons

This table lists every reason that the controller sets, spelled exactly as it appears on the wire.

| Condition | Status | Reason | Appears when |
|---|---|---|---|
| `Validated` | `True` | `SpecValid` | The connections and clone options materialize cleanly and the preflight passed. The controller refreshes it on every reconcile of an active Migration. |
| `Validated` | `Unknown` | `PreflightRunning` | The preflight Job is running. When its pod cannot start, the message carries the kubelet reason verbatim (misnamed Secret, unschedulable). |
| `Validated` | `False` | `InvalidSpec` | The controller cannot render the spec into a worker Job. The Migration fails terminally with the same reason. |
| `Validated` | `False` | `PreflightFailed` | A preflight test failed. The failed test can be connectivity, a PgBouncer in the source or target connection, the `synchronousCommit` test, or selected extension availability or ownership. It can also be a target clone privilege, a source table that row-level security would filter, or an all-databases superuser or database-listing probe. Or it can be a follow prerequisite, including unlogged tables in scope. The message names the failed test and the recovery action. A `superuserSecretRef` hint applies only to grant remediation. When the preflight log stays unreadable for two minutes after the Job fails, the message carries the read error and the Job name instead. Terminal. |
| `CloneCompleted` | `False` | `CloneRunning` | A worker attempt is running the base copy. |
| `CloneCompleted` | `False` | `CopyingData` | The probe saw copy, index or vacuum workers of this attempt connected to the target. This reason replaces `CloneRunning` for the rest of the attempt. The phase cannot reach `Finalizing` before the controller sets this reason. |
| `CloneCompleted` | `False` | `CloneFailed` | The final attempt failed. The message carries the Job failure and the last pgcopydb error line. |
| `CloneCompleted` | `False` | `SnapshotLost` | Live migration with `spec.clone.dropIfExists`: pgcopydb refused to resume the copy without the replication slot's snapshot. Thus the next attempt starts from the beginning. The message carries the refusal. The next attempt's `CloneRunning` replaces it. |
| `CloneCompleted` | `False` | `TablesEmptyOnTarget` | Live migration: the worker logged that the base copy finished. But the pass's own sample found tables that hold rows on the source and none on the target. A `--resume` after killed attempts can produce this. The message names them. The controller does not trust the marker, and it reports the stream but does not drive it. The reason stands until a sample finds a row in each of these tables. |
| `CloneCompleted` | `False` | `CloneIncomplete` | Clone-only migration: the worker exited 0, but pgcopydb's own catalog counts tables that are not done. A Job that mounts the same work dir reads that catalog after the worker is gone. The Migration fails with the same reason. |
| `CloneCompleted` | `True` | `CloneSucceeded` | Clone-only migration: the worker Job finished, and pgcopydb's catalog, where it could be read, counted every table done. |
| `CloneCompleted` | `True` | `BaseCopyDone` | Live migration: the worker logged that the base copy finished. The pass's own sample found no table that holds rows on the source and none on the target. Change replay also took control. |
| `Streaming` | `True` | `Replaying` | The worker's apply process is replaying changes to the target. |
| `CaughtUp` | `True` | `LagBelowThreshold` | Two consecutive samples measured the replication lag at or below `spec.follow.maxCatchupLag`. |
| `CaughtUp` | `False` | `Lagging` | Lag is above the threshold, or no replication sample is available yet. |
| `CaughtUp` | `False` | `ConfirmingCatchUp` | One sample measured the lag at or below `spec.follow.maxCatchupLag`, and the operator needs a second consecutive one before it sets `CaughtUp` True. A single sample can land while the worker still acknowledges its raw receive position. At that point, the lag reads near zero, whatever the apply backlog is. A sample above the threshold returns the condition to `Lagging`, and the count restarts. |
| `CutoverCompleted` | `Unknown` | `RehearsalPodRemains` | The worker drained, and the drain verify waits until the pod of a canceled verification rehearsal ends. The message names the pod and its node. The controller deletes the condition after the pod ends, and the result of the verify then sets it. |
| `CutoverCompleted` | `True` | `DrainVerified` | The verify Job proved the drain: the target's origin progress sits exactly on the cutover LSN, or `pgcopydb compare data` found every migrated table matching. For any other reading, the verify decides by content. Nearly every cutover takes this path, because publication-filtered WAL and unapplied commits measure alike. Changes are applied and sequences are synced. |
| `CutoverCompleted` | `False` | `DrainIncomplete` | Drain verification did not show the target holding every change: `pgcopydb compare data` found a difference or gave no verdict. The Migration fails with the same reason. |
| `OwnershipApplied` | `Unknown` | `OwnershipRunning` | The `<name>-reown` Job is running: after the worker exited on a clone, after the drain is proven on a live migration. No worker restarts while it exists. |
| `OwnershipApplied` | `Unknown` | `OwnershipSuspended` | `spec.suspend` deleted a running handover Job. The Job runs again on resume and runs the statements that remain. Each `ALTER` commits on its own. |
| `OwnershipApplied` | `True` | `OwnershipApplied` | Every schema, relation, routine and type the migration role owned in the target database now belongs to `clone.ownerAfterRestore`. |
| `OwnershipApplied` | `False` | `OwnershipFailed` | The handover Job failed after its retries, and the data is on the target. The Migration fails with the same reason. |
| `Verified` | `Unknown` | `VerificationRunning` | A `pgcopydb compare` Job is running. |
| `Verified` | `True` | `ComparePassed` | Every requested compare found source and target matching. |
| `Verified` | `False` | `SchemaMismatch` | `pgcopydb compare schema` reported differences. |
| `Verified` | `False` | `DataMismatch` | `pgcopydb compare data` reported differences while the schema matched (or was not compared). |
| `Complete` | `True` | `MigrationSucceeded` | The migration finished. On live migrations, the controller sets it after cleanup and verification. |
| `Complete` | `True` | `DryRunSucceeded` | A `spec.dryRun` Migration's preflight passed. The controller created no worker Job, and no data moved. |
| `CommitsBlocked` | `True` | `SyncRepWait` | Two samples in a row found a target session of the migration role that waits for a synchronous standby to acknowledge its commit. That wait is 30 seconds or more. The message gives the waiting sessions, how many of them are pgcopydb's, and the oldest wait. It stands until a sample shows no such wait or the worker succeeds. A sample that cannot read the count leaves it `True`. |
| `CommitsBlocked` | `False` | `NoSyncRepWait` | A sample read the count and found no session whose wait is 30 seconds or more. One such sample clears `True`. |
| `CommitsBlocked` | `False` | `WorkerSucceeded` | The worker Job completed, so every commit that it sent returned. No sampler runs on the finish path, so this reason prevents a stale `True` on that path. |
| `CommitsBlocked` | `Unknown` | `SyncRepUnobserved` | A sample could not read the count: the exec or the count query failed, or no worker pod was running. It replaces `False`, never `True`. |
| `RetryHeld` | `True` | `GateProbing` | The first probe of a hold is pending or running, or it does not exist yet. Every retry passes through it. |
| `RetryHeld` | `True` | `WorkerPodRemains` | A pod of the previous attempt's worker, or of a verification rehearsal, has not ended. The message names the pod and its node. No probe runs meanwhile. |
| `RetryHeld` | `True` | `SessionsRemain` | The probe's census found sessions of the migration role left on the target. The message lists up to ten with pid, state, wait event, age, backend start, and name. The operator never ends them. |
| `RetryHeld` | `True` | `SyncCommitNotLocal` | With `spec.target.synchronousCommit: local`, the probe's target connection reports another `synchronous_commit`: a pooler or proxy dropped the startup option. |
| `RetryHeld` | `True` | `CommitBlocked` | The test commit waited for a synchronous standby until the probe cancelled it at 5 seconds. Or the test commit did not run, because a session, or an earlier probe, still waits for one. |
| `RetryHeld` | `True` | `ProbeFailed` | One of the probe tests gave no result. The cause can be a failed connection, an SQL error, a deadline, or a probe pod that cannot start. It can also be an unreadable log, or a log without the expected lines. The message carries the server's error. For a missing `TEMPORARY` grant, it also carries the `GRANT` that fixes it. |
| `RetryHeld` | `False` | `GatePassed` | The probe passed, and the next attempt's worker Job exists. |
| `RetryHeld` | `False` | `Suspended` | `spec.suspend` stopped the hold. The resume runs the gate again. |
| `RetryHeld` | `False` | `MigrationFailed` | The Migration failed instead of retrying. |
| `Failed` | `True` | `InvalidSpec` | Spec validation failed. A retry cannot help (source and target are immutable, except `target.synchronousCommit`). |
| `Failed` | `True` | `PreflightFailed` | The preflight failed before any data moved. |
| `Failed` | `True` | `BackoffLimitExceeded` | The Migration has no retry budget left: `backoffLimit` + 1 counted attempts. Attempts that the retry gate exempted (`status.exemptAttempts`) do not count. The message reads `retry budget exhausted after N attempts (M exempt)`, followed by the last failed attempt's error when the gate recorded one. |
| `Failed` | `True` | `PermissionDenied` | An attempt hit a permission error that retries cannot fix. The classification of the log tail is best-effort, and a miss keeps normal retries. The message carries the matched log line, and the remaining retry budget stays unspent. When the worker log is unreadable, the controller tries to read it again for two minutes after the Job fails. After that, the attempt retries unclassified, with the read error in its message. |
| `Failed` | `True` | `DiskFull` | An attempt filled the work volume. The controller classifies it like `PermissionDenied`, and the message carries the matched log line. A full target retries. |
| `Failed` | `True` | `SchemaDrift` | The apply hit a change that names a column that the target lacks (SQLSTATE 42703). This is usually because DDL ran on the source. The controller classifies it like `PermissionDenied`. The message carries the matched log line and the remedy. Other schema errors retry. |
| `Failed` | `True` | `SnapshotLost` | pgcopydb refused to resume a live migration's copy without the replication slot's snapshot, and `spec.clone.dropIfExists` is false. Thus the attempt cannot start from the beginning on a target that holds the objects of the earlier attempts. The message carries the refusal and the remedy. |
| `Failed` | `True` | `CloneIncomplete` | A clone-only worker exited 0 while pgcopydb's catalog counted tables that were not done. Do not use the target as a complete copy. |
| `Failed` | `True` | `DrainIncomplete` | Cutover drain verification refuted completeness. |
| `Failed` | `True` | `OwnershipFailed` | The ownership handover failed. The data is on the target. |

Two of those terminal reasons need a recovery step rather than a new Migration.
On `DrainIncomplete`, do not switch applications to the target.
[Phase Failed with reason DrainIncomplete](../troubleshooting.md#phase-failed-with-reason-drainincomplete) covers what the verify Job logs say and how to recover.
The operator keeps the replication slot, so the data stays recoverable.
On `OwnershipFailed`, finish the handover by hand with the statements in the Job log.
Follow [Ownership handover failures](../troubleshooting.md#ownership-handover-failures).
On a live migration, the operator keeps the slot until you delete the Migration.
The condition message carries the Job's last lines: the missing role, or the exact `GRANT`.

A mismatch on `Verified` does not fail the Migration, because the transfer itself finished.
The controller sets `Complete` either way.
See [Verification](../operations/verification.md).

The `RetryHeld` reason names the first test that did not pass, in this order: census, `synchronous_commit`, test commit.
The message lists every test that did not pass.
A new probe every 30 seconds keeps the reason until its own result differs.

## Event reasons

Events record each transition.
Reasons are stable.
Messages are not.
Terminal failures also emit a Warning event whose reason equals the `Failed` condition reason above.

| Reason | Type | Appears when |
|---|---|---|
| `AttemptStarted` | Normal | The controller created a worker attempt's Job. After a hold, the message says how long the retry gate held the attempt. For an exempt attempt, the message also says that the failed attempt did not count against `backoffLimit`. |
| `AttemptFailed` | Warning | An attempt failed. A retry waits for the retry gate, then resumes from the work-dir catalogs. If no retry budget remains, a retry follows only when the gate exempts the failed attempt. |
| `SnapshotLost` | Warning | pgcopydb refused to resume a live migration's copy. With `spec.clone.dropIfExists` (action `Retry`), a retry waits for the retry gate. Then it drops the slot and starts from the beginning with `--restart --drop-if-exists`. If no retry budget remains, a retry follows only when the gate exempts the failed attempt. Without `dropIfExists`, this is the terminal failure event (action `Fail`), and no attempt follows. |
| `TablesEmptyOnTarget` | Warning | The worker logged that the base copy finished, while a sample showed tables that hold rows on the source and none on the target. The message names them. The event occurs once per refusal. See the condition reason of the same name. |
| `CommitsBlocked` | Warning | `CommitsBlocked` turned `True` (action `Sample`): once on each entry into `True`, with the condition's message. |
| `RetryHeld` | Warning | The retry gate holds the next attempt (action `Retry`): when a hold gets its first result, and whenever its reason changes. `GateProbing` emits none, because every retry passes through it. |
| `WorkerZombie` | Warning | The pgcopydb supervisor died, but a child process kept the worker pod alive (upstream pgcopydb 0.18 defect). The operator deleted the pod so that the normal retry could run. |
| `PreflightStarted` | Normal | The controller created the preflight Job. |
| `PreflightPassed` | Normal | Every preflight test passed. The message counts the tests and the applied grants. In a dry run, it counts the grants that would be applied, in place of the applied grants. |
| `PreflightRemediated` | Normal | The preflight applied missing grants through `superuserSecretRef`. There is one event per tier (clone, follow), and each message lists the exact statements of that tier. |
| `PreflightWarning` | Warning | A passing preflight logged findings that do not block. A finding can be a `superuserSecretRef` role without `rolsuper`, or a source `wal_sender_timeout` below 60s (or unreadable). A finding can also be a migration role without `TEMPORARY` on the target database (or unreadable). The test commit of the retry gate needs that grant. One event lists them all. |
| `PreflightWouldRemediate` | Normal | A dry run found grants that `superuserSecretRef` would apply, and applied none. There is one event per tier, and each message lists the statements of that tier. |
| `DryRunSucceeded` | Normal | A dry run's preflight passed. The Migration completed without a worker Job. |
| `CutoverStarted` | Normal | The controller set the cutover LSN. The stream is frozen and draining. |
| `CutoverRetry` | Warning | Setting the cutover LSN failed transiently. The controller tries again on the next pass. |
| `RehearsalPodRemains` | Warning | The drain verify waits for the pod of a canceled verification rehearsal (action `Cutover`): once when the wait starts, with the condition's message. |
| `CleanupStarted` | Normal | The controller created the cleanup Job (slot, publication, origin). |
| `CleanupFailed` | Warning | Cleanup exhausted its retries. The named slot may still hold WAL on the source, and it needs manual removal. |
| `OwnershipStarted` | Normal | The controller created the `<name>-reown` handover Job. |
| `OwnershipApplied` | Normal | The handover finished. The restored objects belong to `clone.ownerAfterRestore`. |
| `Suspended` | Normal | `spec.suspend` deleted the worker, and a running handover Job with it. The work volume stays. |
| `SlotRetained` | Warning | A suspended live migration keeps its replication slot, which holds WAL on the source. |
| `VerificationStarted` | Normal | The controller created a compare Job. |
| `Verified` | Normal | Compare found source and target matching. |
| `VerificationMismatch` | Warning | Compare reported differences. The details are in the compare Job logs. |
| `Completed` | Normal | The migration finished. |
