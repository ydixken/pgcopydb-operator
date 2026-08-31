# Conditions and reasons

The condition types and reason strings the controller writes to `status.conditions`. They are API contract: stable identifiers you can consume from automation (`kubectl wait`, GitOps health checks, alerting) without parsing messages. Every condition is a `metav1.Condition` and carries `observedGeneration`, so a stale condition is detectable after a spec change.

`status.phase` is a one-word summary derived from these conditions for the printer column; the conditions are authoritative.

```sh
kubectl wait --for=condition=Complete migration/shop --timeout=1h
```

## Phases

`status.phase` is the printer column: one word for what the migration is doing now.
It is derived from the conditions and from what the worker is observably doing, and the conditions remain authoritative.
Automation should wait on conditions, not on phase strings.

| Phase | The operator is | Next |
|---|---|---|
| `Pending` | Accepted the object, not yet acted | `Validating` |
| `Validating` | Materializing the spec and running the preflight Job | `Cloning`, or `Failed` |
| `Cloning` | Running the worker: schema, then table data | `Finalizing`, `Streaming`, `Completed`, or `Failed` |
| `Finalizing` | Past the data copy, finishing indexes, constraints and vacuum | `Streaming`, `Completed`, or `Failed` |
| `Streaming` | Applying changes from the replication slot (live migrations) | `CutoverPending`, or `Failed` |
| `CutoverPending` | Caught up and waiting for approval (`cutover.mode: Manual`) | `CuttingOver` |
| `CuttingOver` | Setting the end position, draining, proving the drain | `Verifying`, `Completed`, or `Failed` |
| `Verifying` | Running the requested `pgcopydb compare` checks | `Completed`, or `Failed` |
| `Completed` | Finished. Terminal | |
| `Failed` | Finished badly. Terminal | |
| `Suspended` | Holding, because `spec.suspend` is true | whatever it was doing |

### Inside `Cloning` and `Finalizing`

pgcopydb does not run its steps in sequence.
After the schema is in place it starts the table copy, index builds, constraint creation, large-object copy and vacuum **all at once**, and prints nothing between starting them and finishing them.
So a single phase would cover the whole run and tell you nothing, which is why the copy and its tail are reported apart.

| Sub-state | Phase | What is running | How it looks |
|---|---|---|---|
| Catalog and schema | `Cloning` | Source catalog queries, `pg_dump`, pre-data `pg_restore` | Seconds. The target barely grows |
| Table data | `Cloning` | `tableJobs` COPY workers, plus index, constraint and vacuum workers on whatever has already finished | The bulk of the run. The target grows steadily |
| The tail | `Finalizing` | Index builds, constraints, and vacuum on the tables that finished last | **The target stops growing** while work continues |
| Post-data | `Finalizing` | Post-data `pg_restore`: foreign keys and the rest | Seconds |

The tail is the one that surprises people.
A table's vacuum cannot start until that table's own copy finishes, and the largest table finishes last, so a clone routinely ends as a single `VACUUM ANALYZE` running alone while every other worker sits idle.
On a fixture where one table held 73% of the bytes, that tail was roughly a fifth of the wall clock.

> [!important]
> During `Finalizing` the target database stops growing, so anything derived from its size reads as finished while real work continues.
> This is why `PgcopydbMigrationCloneStalled` matches `Cloning` alone: a long tail is not a stall.
> [Performance tuning](../operations/performance.md) explains how to trade the vacuum away for the time.

The phase is derived by asking the target what the worker is doing there, a plain `pg_stat_activity` query that touches no pgcopydb catalog: copy workers count while they are connected, everything else only while it is running a statement.
pgcopydb opens its copy workers up front and they idle between statements, so a copy worker counted only mid-`COPY` reads as finished while data is still moving; an index worker parked idle on a `SET` is not the tail either, which is why the two counts differ.
The query is scoped to the worker's own pod, so another migration's backends on a shared target cannot read as this clone's tail.
Reading pgcopydb's catalog while the copy is writing it kills workers, so the operator does not do it during a clone.
`Finalizing` needs the probe to have seen this attempt's copy workers at least once, which the `CopyingData` reason below records.
Until that first sighting, a sample with no copy workers and other backends busy leaves the phase at `Cloning`: the copy has not been seen running, so it cannot have been seen stopping.
Zero on both counts is the unknown answer rather than the tail, and the phase stays where the last answered sample left it: not knowing is not evidence of finishing.

## Condition types

Each type is named for what `True` means. Seven are normal-true (True is the desired state); `Failed` is abnormal-true (True means the migration ended in failure).

| Type | Polarity | True means |
|---|---|---|
| `Validated` | normal-true | The spec materializes cleanly and the preflight passed. |
| `CloneCompleted` | normal-true | The base copy finished. |
| `Streaming` | normal-true | Logical replication is applying changes (live migrations only). |
| `CaughtUp` | normal-true | Replication lag has been at or below `spec.follow.maxCatchupLag` on two consecutive samples. |
| `CutoverCompleted` | normal-true | The drain is proven: a clean data compare, or origin progress exactly at the cutover LSN on the rare cutover where the two coincide. |
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
| `Validated` | `False` | `PreflightFailed` | The preflight found a failed check: connectivity or a target clone privilege on any migration, or a missing follow prerequisite; the message carries the check output with the exact `GRANT` or setting to fix, plus a `superuserSecretRef` hint when that field could apply it. Terminal. |
| `CloneCompleted` | `False` | `CloneRunning` | A worker attempt is running the base copy. |
| `CloneCompleted` | `False` | `CopyingData` | The probe has seen this attempt's copy workers connected to the target; it replaces `CloneRunning` for the rest of the attempt, and the phase cannot reach `Finalizing` before it is set. |
| `CloneCompleted` | `False` | `CloneFailed` | The final attempt failed; the message carries the Job failure and the last pgcopydb error line. |
| `CloneCompleted` | `True` | `CloneSucceeded` | Clone-only migration: the worker Job finished. |
| `CloneCompleted` | `True` | `BaseCopyDone` | Live migration: the base copy finished (detected from the worker's clone-completion log line) and change replay took over. |
| `Streaming` | `True` | `Replaying` | The worker's apply process is replaying changes to the target. |
| `CaughtUp` | `True` | `LagBelowThreshold` | Two consecutive samples measured the replication lag at or below `spec.follow.maxCatchupLag`. |
| `CaughtUp` | `False` | `Lagging` | Lag is above the threshold, or no replication sample is available yet. |
| `CaughtUp` | `False` | `ConfirmingCatchUp` | One sample measured the lag at or below `spec.follow.maxCatchupLag`, and the operator wants a second consecutive one before turning `CaughtUp` True: a single sample can land while the worker is still confirming its raw receive position, where the lag reads near zero whatever the apply backlog is. A sample above the threshold returns the condition to `Lagging` and the count starts over. |
| `CutoverCompleted` | `True` | `DrainVerified` | The verify Job proved the drain: the target's origin progress sits exactly on the cutover LSN, or `pgcopydb compare data` found every migrated table matching. Any other reading, in either direction, is decided by content, which is the path nearly every cutover takes, because publication-filtered WAL and unapplied commits measure alike. Changes applied, sequences synced. |
| `CutoverCompleted` | `False` | `DrainIncomplete` | Drain verification did not show the target holding every change (`pgcopydb compare data` either found a difference or produced no verdict); the replication slot is kept so the data stays recoverable. The Migration fails with the same reason. |
| `Verified` | `Unknown` | `VerificationRunning` | A `pgcopydb compare` Job is running. |
| `Verified` | `True` | `ComparePassed` | Every requested compare found source and target matching. |
| `Verified` | `False` | `SchemaMismatch` | `pgcopydb compare schema` reported differences. |
| `Verified` | `False` | `DataMismatch` | `pgcopydb compare data` reported differences while the schema matched (or was not checked). |
| `Complete` | `True` | `MigrationSucceeded` | The migration finished; on live migrations, set after cleanup and verification. |
| `Failed` | `True` | `InvalidSpec` | Spec validation failed; retrying cannot help (source and target are immutable). |
| `Failed` | `True` | `PreflightFailed` | The preflight failed before any data moved. |
| `Failed` | `True` | `BackoffLimitExceeded` | The retry budget is exhausted (`backoffLimit` + 1 attempts). |
| `Failed` | `True` | `PermissionDenied` | An attempt hit a permission error retries cannot fix (best-effort log-tail classification; a miss keeps normal retries); the message carries the matched log line, and the remaining retry budget stays unspent. |
| `Failed` | `True` | `DrainIncomplete` | Cutover drain verification refuted completeness. Do not switch applications to the target; see the [troubleshooting table](../troubleshooting.md). |

A mismatch on `Verified` does not fail the Migration: the transfer itself finished, and what to do about a content difference is your call. `Complete` is set either way; see [Verification](../operations/verification.md).

## Event reasons

Events carry the play-by-play; reasons are stable, messages are not. Terminal failures additionally emit a Warning event whose reason equals the `Failed` condition reason above.

| Reason | Type | Appears when |
|---|---|---|
| `AttemptStarted` | Normal | A worker attempt's Job was created. |
| `AttemptFailed` | Warning | An attempt failed; the next one resumes from the work-dir catalogs. |
| `WorkerZombie` | Warning | The pgcopydb supervisor died but a child process kept the worker pod alive (upstream pgcopydb 0.18 defect); the operator removed the pod so the normal retry could resume. |
| `PreflightStarted` | Normal | The preflight Job was created. |
| `PreflightPassed` | Normal | Every preflight check passed; the message counts checks and applied grants. |
| `PreflightRemediated` | Normal | The preflight applied missing grants through `superuserSecretRef`; one event per tier (clone, follow), each message listing that tier's exact statements. |
| `CutoverStarted` | Normal | The cutover LSN is set; the stream is frozen and draining. |
| `CutoverRetry` | Warning | Setting the cutover LSN failed transiently; retried on the next pass. |
| `CleanupStarted` | Normal | The cleanup Job (slot, publication, origin) was created. |
| `CleanupFailed` | Warning | Cleanup exhausted its retries; the named slot may keep retaining WAL on the source and needs manual removal. |
| `Suspended` | Normal | `spec.suspend` deleted the worker; the work volume is kept. |
| `SlotRetained` | Warning | A suspended live migration keeps its replication slot, which retains WAL on the source. |
| `VerificationStarted` | Normal | A compare Job was created. |
| `Verified` | Normal | Compare found source and target matching. |
| `VerificationMismatch` | Warning | Compare reported differences; details are in the compare Job logs. |
| `Completed` | Normal | The migration finished. |
