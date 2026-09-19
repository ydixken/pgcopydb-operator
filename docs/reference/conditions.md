# Conditions and reasons

The condition types and reason strings the controller writes to `status.conditions`.
They are API contract: stable identifiers that `kubectl wait`, GitOps health checks and alerts can match on without reading messages.
Every condition is a `metav1.Condition` and carries `observedGeneration`, so a stale condition is detectable after a spec change.

`status.phase` summarizes conditions and worker progress for the printer column.
Its `Pending` value records the controller's first observation, before conditions exist; API-server creation does not initialize status.
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
| `Validating` | Materializing the spec and running the preflight Job | `Cloning`, or `Failed` |
| `Cloning` | Running the worker: schema, then table data | `Finalizing`, `Streaming`, `Completed`, or `Failed` |
| `Finalizing` | Past the data copy, finishing indexes, constraints and vacuum; on a clone with `clone.ownerAfterRestore`, also handing the restored objects over once the worker has exited | `Streaming`, `Verifying`, `Completed`, or `Failed` |
| `Streaming` | Applying changes from the replication slot (live migrations) | `CutoverPending`, or `Failed` |
| `CutoverPending` | Caught up and waiting for approval (`cutover.mode: Manual`) | `CuttingOver` |
| `CuttingOver` | Setting the end position, draining, proving the drain | `Verifying`, `Completed`, or `Failed` |
| `Verifying` | Running the requested `pgcopydb compare` checks | `Completed`, or `Failed` |
| `Completed` | Finished; terminal | |
| `Failed` | Finished badly; terminal | |
| `Suspended` | Holding, because `spec.suspend` is true | whatever it was doing |

### Inside `Cloning` and `Finalizing`

pgcopydb does not run its steps in sequence.
Once the schema is in place it starts the table copy, index builds, constraint creation, large-object copy and vacuum **all at once**.
It then prints nothing until they finish, so the operator reports the copy and its tail as separate phases.

| Sub-state | Phase | What is running | How it looks |
|---|---|---|---|
| Catalog and schema | `Cloning` | Source catalog queries, `pg_dump`, pre-data `pg_restore` | Seconds. The target barely grows |
| Table data | `Cloning` | `tableJobs` COPY workers, plus index, constraint and vacuum workers on whatever has already finished | The bulk of the run. The target grows steadily |
| The tail | `Finalizing` | Index builds, constraints, and vacuum on the tables that finished last | **The target stops growing** while work continues |
| Post-data | `Finalizing` | Post-data `pg_restore`: foreign keys and the rest | Seconds |

A table's vacuum cannot start until that table's own copy finishes, and the largest table finishes last.
So a clone routinely ends with one `VACUUM ANALYZE` still running while every other worker is idle.
[The VACUUM tail](../operations/performance.md#the-vacuum-tail) has the measurement.

> [!important]
> During `Finalizing` the target database stops growing, so anything derived from its size reads as finished while real work continues.
> This is why `PgcopydbMigrationCloneStalled` matches `Cloning` alone: a long tail is not a stall.
> [Performance tuning](../operations/performance.md) explains how to trade the vacuum away for the time.

The phase comes from a `pg_stat_activity` query on the target that touches no pgcopydb catalog.
If a client reads that catalog while the copy writes it, the workers die.
The query counts copy workers and other workers separately.
Zero on both counts is the unknown answer, not the tail, so the phase stays where the last answered sample left it.

## Condition types

Each type is named for what `True` means.
Eight are normal-true: True is the desired state.
`Failed` is abnormal-true: True means the migration ended in failure.

| Type | Polarity | True means |
|---|---|---|
| `Validated` | normal-true | The spec materializes cleanly and the preflight passed. |
| `CloneCompleted` | normal-true | The base copy finished. |
| `Streaming` | normal-true | Logical replication is applying changes (live migrations only). |
| `CaughtUp` | normal-true | Replication lag has been at or below `spec.follow.maxCatchupLag` on two consecutive samples. |
| `CutoverCompleted` | normal-true | The drain is proven: a clean data compare, or origin progress exactly at the cutover LSN on the rare cutover where the two coincide. With `clone.ownerAfterRestore` set, the handover has finished first. |
| `OwnershipApplied` | normal-true | The restored objects belong to `clone.ownerAfterRestore`. Only present when that field is set. |
| `Verified` | normal-true | The requested `pgcopydb compare` checks found source and target matching. |
| `Complete` | normal-true | The migration finished. Terminal and absorbing. |
| `Failed` | abnormal-true | The migration failed for good. Terminal and absorbing. |

## Reasons

Every reason the controller sets, spelled exactly as it appears on the wire.

| Condition | Status | Reason | Appears when |
|---|---|---|---|
| `Validated` | `True` | `SpecValid` | The connections and clone options materialize cleanly and the preflight passed; refreshed on every reconcile of an active Migration. |
| `Validated` | `Unknown` | `PreflightRunning` | The preflight Job is running; when its pod cannot start, the message carries the kubelet reason verbatim (misnamed Secret, unbound PVC, unschedulable). |
| `Validated` | `False` | `InvalidSpec` | The spec cannot be rendered into a worker Job. The Migration fails terminally with the same reason. |
| `Validated` | `False` | `PreflightFailed` | Connectivity, selected extension availability or ownership, a target clone privilege, an all-databases superuser or database-listing probe, or a follow prerequisite failed. The message names the failed check and recovery action; a `superuserSecretRef` hint applies only to grant remediation. Terminal. |
| `CloneCompleted` | `False` | `CloneRunning` | A worker attempt is running the base copy. |
| `CloneCompleted` | `False` | `CopyingData` | The probe has seen this attempt's copy workers connected to the target; it replaces `CloneRunning` for the rest of the attempt, and the phase cannot reach `Finalizing` before it is set. |
| `CloneCompleted` | `False` | `CloneFailed` | The final attempt failed; the message carries the Job failure and the last pgcopydb error line. |
| `CloneCompleted` | `False` | `TablesEmptyOnTarget` | Live migration: the worker logged the base copy finished, but the pass's own sample found tables holding rows on the source and none on the target, which a `--resume` after killed attempts can produce. The marker is not trusted, the stream is reported but not driven, and the reason stands until a sample finds every such table populated. |
| `CloneCompleted` | `False` | `CloneIncomplete` | Clone-only migration: the worker exited 0, but pgcopydb's own catalog, read by a Job that mounts the same work dir once the worker is gone, counts tables not done. The Migration fails with the same reason. |
| `CloneCompleted` | `True` | `CloneSucceeded` | Clone-only migration: the worker Job finished, and pgcopydb's catalog, where it could be read, counted every table done. |
| `CloneCompleted` | `True` | `BaseCopyDone` | Live migration: the worker logged the base copy finished, the pass's own sample found no table holding rows on the source and none on the target, and change replay took over. |
| `Streaming` | `True` | `Replaying` | The worker's apply process is replaying changes to the target. |
| `CaughtUp` | `True` | `LagBelowThreshold` | Two consecutive samples measured the replication lag at or below `spec.follow.maxCatchupLag`. |
| `CaughtUp` | `False` | `Lagging` | Lag is above the threshold, or no replication sample is available yet. |
| `CaughtUp` | `False` | `ConfirmingCatchUp` | One sample measured the lag at or below `spec.follow.maxCatchupLag`, and the operator needs a second consecutive one before it sets `CaughtUp` True. A single sample can land while the worker still confirms its raw receive position, where the lag reads near zero whatever the apply backlog is. A sample above the threshold returns the condition to `Lagging` and the count starts over. |
| `CutoverCompleted` | `True` | `DrainVerified` | The verify Job proved the drain: the target's origin progress sits exactly on the cutover LSN, or `pgcopydb compare data` found every migrated table matching. Any other reading is decided by content, which is the path nearly every cutover takes, because publication-filtered WAL and unapplied commits measure alike. Changes are applied and sequences are synced. |
| `CutoverCompleted` | `False` | `DrainIncomplete` | Drain verification did not show the target holding every change: `pgcopydb compare data` found a difference or gave no verdict. The Migration fails with the same reason. |
| `OwnershipApplied` | `Unknown` | `OwnershipRunning` | The `<name>-reown` Job is running: after the worker exited on a clone, after the drain is proven on a live migration. No worker restarts while it exists. |
| `OwnershipApplied` | `Unknown` | `OwnershipSuspended` | `spec.suspend` deleted a running handover Job. It re-runs on resume and picks up the statements that are left; each `ALTER` commits on its own. |
| `OwnershipApplied` | `True` | `OwnershipApplied` | Every schema, relation, routine and type the migration role owned in the target database now belongs to `clone.ownerAfterRestore`. |
| `OwnershipApplied` | `False` | `OwnershipFailed` | The handover Job failed after its retries, and the data is on the target. The Migration fails with the same reason. |
| `Verified` | `Unknown` | `VerificationRunning` | A `pgcopydb compare` Job is running. |
| `Verified` | `True` | `ComparePassed` | Every requested compare found source and target matching. |
| `Verified` | `False` | `SchemaMismatch` | `pgcopydb compare schema` reported differences. |
| `Verified` | `False` | `DataMismatch` | `pgcopydb compare data` reported differences while the schema matched (or was not checked). |
| `Complete` | `True` | `MigrationSucceeded` | The migration finished; on live migrations, set after cleanup and verification. |
| `Failed` | `True` | `InvalidSpec` | Spec validation failed; retrying cannot help (source and target are immutable). |
| `Failed` | `True` | `PreflightFailed` | The preflight failed before any data moved. |
| `Failed` | `True` | `BackoffLimitExceeded` | The retry budget is exhausted (`backoffLimit` + 1 attempts). |
| `Failed` | `True` | `PermissionDenied` | An attempt hit a permission error retries cannot fix (best-effort log-tail classification; a miss keeps normal retries); the message carries the matched log line, and the remaining retry budget stays unspent. |
| `Failed` | `True` | `CloneIncomplete` | A clone-only worker exited 0 while pgcopydb's catalog counted tables not done. Do not use the target as a complete copy. |
| `Failed` | `True` | `DrainIncomplete` | Cutover drain verification refuted completeness. |
| `Failed` | `True` | `OwnershipFailed` | The ownership handover failed; the data is on the target. |

Two of those terminal reasons need a recovery step rather than a new Migration.
On `DrainIncomplete`, do not switch applications to the target; [Phase Failed with reason DrainIncomplete](../troubleshooting.md#phase-failed-with-reason-drainincomplete) covers what the verify Job logs say and how to recover.
The replication slot is kept, so the data stays recoverable.
On `OwnershipFailed`, finish the handover by hand with the statements in the Job log, following [Ownership handover failures](../troubleshooting.md#ownership-handover-failures); on a live migration the slot is kept until the Migration is deleted.
The condition message carries the Job's last lines: the missing role, or the exact `GRANT`.

A mismatch on `Verified` does not fail the Migration, because the transfer itself finished.
`Complete` is set either way; see [Verification](../operations/verification.md).

## Event reasons

Events record each transition.
Reasons are stable; messages are not.
Terminal failures also emit a Warning event whose reason equals the `Failed` condition reason above.

| Reason | Type | Appears when |
|---|---|---|
| `AttemptStarted` | Normal | A worker attempt's Job was created. |
| `AttemptFailed` | Warning | An attempt failed; the next one resumes from the work-dir catalogs. |
| `TablesEmptyOnTarget` | Warning | The worker logged the base copy finished while a sample showed tables holding rows on the source and none on the target; once per refusal, see the condition reason of the same name. |
| `WorkerZombie` | Warning | The pgcopydb supervisor died but a child process kept the worker pod alive (upstream pgcopydb 0.18 defect); the operator removed the pod so the normal retry could resume. |
| `PreflightStarted` | Normal | The preflight Job was created. |
| `PreflightPassed` | Normal | Every preflight check passed; the message counts checks and applied grants. |
| `PreflightRemediated` | Normal | The preflight applied missing grants through `superuserSecretRef`; one event per tier (clone, follow), each message listing that tier's exact statements. |
| `CutoverStarted` | Normal | The cutover LSN is set; the stream is frozen and draining. |
| `CutoverRetry` | Warning | Setting the cutover LSN failed transiently; retried on the next pass. |
| `CleanupStarted` | Normal | The cleanup Job (slot, publication, origin) was created. |
| `CleanupFailed` | Warning | Cleanup exhausted its retries; the named slot may still hold WAL on the source and needs manual removal. |
| `OwnershipStarted` | Normal | The `<name>-reown` handover Job was created. |
| `OwnershipApplied` | Normal | The handover finished; the restored objects belong to `clone.ownerAfterRestore`. |
| `Suspended` | Normal | `spec.suspend` deleted the worker, and a running handover Job with it; the work volume is kept. |
| `SlotRetained` | Warning | A suspended live migration keeps its replication slot, which holds WAL on the source. |
| `VerificationStarted` | Normal | A compare Job was created. |
| `Verified` | Normal | Compare found source and target matching. |
| `VerificationMismatch` | Warning | Compare reported differences; details are in the compare Job logs. |
| `Completed` | Normal | The migration finished. |
