# Conditions and reasons

The condition types and reason strings the controller writes to `status.conditions`. They are API contract: stable identifiers you can consume from automation (`kubectl wait`, GitOps health checks, alerting) without parsing messages. Every condition is a `metav1.Condition` and carries `observedGeneration`, so a stale condition is detectable after a spec change.

`status.phase` is a one-word summary derived from these conditions for the printer column; the conditions are authoritative.

```sh
kubectl wait --for=condition=Complete migration/shop --timeout=1h
```

## Condition types

Each type is named for what `True` means. Seven are normal-true (True is the desired state); `Failed` is abnormal-true (True means the migration ended in failure).

| Type | Polarity | True means |
|---|---|---|
| `Validated` | normal-true | The spec materializes cleanly and the preflight passed. |
| `CloneCompleted` | normal-true | The base copy finished. |
| `Streaming` | normal-true | Logical replication is applying changes (live migrations only). |
| `CaughtUp` | normal-true | Replication lag is at or below `spec.follow.maxCatchupLag`. |
| `CutoverCompleted` | normal-true | The drain is proven: origin progress at the cutover LSN, or a clean data compare when the origin alone cannot decide. |
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
| `Validated` | `False` | `PreflightFailed` | The preflight found a failed check: connectivity on any migration, or a missing follow prerequisite; the message carries the check output with the exact `GRANT` or setting to fix, plus a `superuserSecretRef` hint when that field could apply it. Terminal. |
| `CloneCompleted` | `False` | `CloneRunning` | A worker attempt is running the base copy. |
| `CloneCompleted` | `False` | `CloneFailed` | The final attempt failed; the message carries the Job failure and the last pgcopydb error line. |
| `CloneCompleted` | `True` | `CloneSucceeded` | Clone-only migration: the worker Job finished. |
| `CloneCompleted` | `True` | `BaseCopyDone` | Live migration: the base copy finished (detected from the worker's clone-completion log line) and change replay took over. |
| `Streaming` | `True` | `Replaying` | The worker's apply process is replaying changes to the target. |
| `CaughtUp` | `True` | `LagBelowThreshold` | Replication lag is at or below `spec.follow.maxCatchupLag`. |
| `CaughtUp` | `False` | `Lagging` | Lag is above the threshold, or no sentinel sample is available yet. |
| `CutoverCompleted` | `True` | `DrainVerified` | The verify Job proved the drain: the target's origin progress reached the cutover LSN within one WAL page, or `pgcopydb compare data` found every migrated table matching (an idle source leaves the origin behind by publication-filtered WAL, which is not loss). Changes applied, sequences synced. |
| `CutoverCompleted` | `False` | `DrainIncomplete` | Drain verification found changes missing on the target (`pgcopydb compare data` backed the refusal); the replication slot is kept so the data stays recoverable. The Migration fails with the same reason. |
| `Verified` | `Unknown` | `VerificationRunning` | A `pgcopydb compare` Job is running. |
| `Verified` | `True` | `ComparePassed` | Every requested compare found source and target matching. |
| `Verified` | `False` | `SchemaMismatch` | `pgcopydb compare schema` reported differences. |
| `Verified` | `False` | `DataMismatch` | `pgcopydb compare data` reported differences while the schema matched (or was not checked). |
| `Complete` | `True` | `MigrationSucceeded` | The migration finished; on live migrations, set after cleanup and verification. |
| `Failed` | `True` | `InvalidSpec` | Spec validation failed; retrying cannot help (source and target are immutable). |
| `Failed` | `True` | `PreflightFailed` | The preflight failed before any data moved. |
| `Failed` | `True` | `BackoffLimitExceeded` | The retry budget is exhausted (`backoffLimit` + 1 attempts). |
| `Failed` | `True` | `DrainIncomplete` | Cutover drain verification refuted completeness. Do not switch applications to the target; see the [troubleshooting table](../troubleshooting.md). |

A mismatch on `Verified` does not fail the Migration: the transfer itself finished, and what to do about a content difference is your call. `Complete` is set either way; see [Verification](../operations/verification.md).

## Event reasons

Events carry the play-by-play; reasons are stable, messages are not. Terminal failures additionally emit a Warning event whose reason equals the `Failed` condition reason above.

| Reason | Type | Appears when |
|---|---|---|
| `AttemptStarted` | Normal | A worker attempt's Job was created. |
| `AttemptFailed` | Warning | An attempt failed; the next one resumes from the work-dir catalogs. |
| `WorkerZombie` | Warning | The pgcopydb supervisor died but a child process kept the worker pod alive (upstream 0.18 defect); the operator removed the pod so the normal retry could resume. |
| `PreflightStarted` | Normal | The preflight Job was created. |
| `PreflightPassed` | Normal | Every preflight check passed; the message counts checks and applied grants. |
| `PreflightRemediated` | Normal | The preflight applied one missing grant through `superuserSecretRef`; the message is the exact statement. |
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
