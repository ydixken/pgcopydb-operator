# Dry run, suspend, retries, deletion

The lifecycle of a Migration: how to check it with a dry run, how to suspend it, how the operator retries failed attempts, and what deletion cleans up.

## Initial status

`Pending` is the first phase of a Migration.
The controller persists it, so it is not an API-server default at creation.
`Pending` has no validation condition and no durable condition-transition timestamp.
An API watch can see it, but a Prometheus scrape may miss the short-lived phase.

## Dry run

`spec.dryRun: true` runs the [preflight](live-migration.md#preflight) and stops there.
No worker Job starts, so pgcopydb never runs: nothing is copied, and no replication slot, publication, or origin is created.
A passed dry run ends in phase `Completed` with `Complete=True` and reason `DryRunSucceeded`, and `status.attempts` stays 0.
A failed one ends like any failed preflight, in `Failed` with reason `PreflightFailed`.

```yaml
apiVersion: pgcopydb-operator.io/v1beta1
kind: Migration
metadata:
  name: shop-dry-run
spec:
  dryRun: true                    # preflight only, no worker Job
  source:
    secretRef: {name: shop-source}
  target:
    secretRef: {name: shop-target}
    superuserSecretRef: {name: shop-target-admin}  # verified; missing grants are reported, not applied
```

With a `superuserSecretRef`, the preflight still connects as that user and verifies it, but it applies no grant.
It prints each statement it would apply as `would-remediate:` (follow rights) or `would-remediate-clone:` (clone rights), and emits one `PreflightWouldRemediate` event per tier that lists them.
Without a `superuserSecretRef`, a missing grant fails the dry run exactly as it fails a real preflight.

A dry run applies none of the statements it reports, so it cannot show that they succeed.
On a PostgreSQL 14 target it reports `GRANT SET ON PARAMETER session_replication_role` and passes, although that grant exists only on PostgreSQL 15 and later.
A `superuserSecretRef` user that lacks the right to grant passes too.
Both fail the real preflight.

> [!warning]
> Upgrade the operator before you create a Migration with `dryRun: true`.
> An operator release without the field ignores it and runs a real migration.

`dryRun` is immutable.
For the real run, create a separate Migration without it.

> [!important]
> `spec.dryRun` has nothing to do with `kubectl apply --dry-run`: the Migration is created, and its preflight Job connects to both databases.
> It still allocates the work PVC, because the preflight pod mounts it.
> A passed dry run reports as `Completed`: Argo CD shows it `Healthy`, the fleet dashboard counts it as completed, and `kubectl wait --for=condition=Complete` returns.
> A dry run is final whether it passes or fails, and it never turns into the real run.

## Suspend

`spec.suspend: true` deletes the worker Job and keeps the work volume.
The delete is in the foreground, so pgcopydb receives SIGTERM and shuts down cleanly.
The phase becomes `Suspended`.
Set `spec.suspend` back to false to start the next attempt, which resumes from the work-dir catalogs.
Each resume consumes one attempt from the `backoffLimit` budget.

> [!warning]
> Suspending a live migration does NOT drop the replication slot.
> The slot keeps retaining WAL on the source for as long as the Migration is suspended, without bound, and the operator emits a `SlotRetained` warning event to that effect.
> Watch source disk, and do not park live migrations for days.
> Deleting the Migration drops the slot.

## Retries and resume

`spec.backoffLimit` is the number of retries, so a Migration makes `backoffLimit + 1` attempts.
The default budget is 4 attempts.
Each attempt is a fresh Job with Kubernetes-level retries disabled.
The operator owns the retry policy, so attempt counts and failure reasons live on the Migration.

- Attempt 1 runs with `--restart`, which wipes any state on the work volume.
  A fresh Migration has no prior state of its own, so that state is foreign.
- Retries run with `--resume --not-consistent`.
  The retry skips tables and indexes already recorded done in pgcopydb's catalogs, and restarts interrupted copies.
  `--not-consistent` is needed because the failed attempt's snapshot died with its process.
  The retry copies the remaining tables under a new snapshot.
- A worker Job that disappears triggers the next attempt the same way.
  The `ttlSecondsAfterFinished` TTL and manual deletion both remove the Job.
- Failures the retry cannot fix, such as a source that dies mid-copy or a broken restore, still consume the budget one attempt at a time.
- The [preflight](live-migration.md#preflight) stops some failures before the first attempt.
  It checks connectivity, credentials, and target clone privileges on every migration, and the follow prerequisites on live ones.
- Permission errors end the Migration as `PermissionDenied`, on the attempt whose log tail shows one as the terminal cause.
  Permission matching is best-effort, so a missed match keeps normal retries.
- A full work volume ends the Migration as `DiskFull` the same way.
  A retry cannot free space on it, and its resume may fail on an unrelated error that hides the cause.
  A target out of disk space or shared memory keeps normal retries, because space or load can change between attempts.

When worker logs are readable, the `AttemptFailed` event names the pgcopydb error after each failed attempt.
The terminal `Failed` condition keeps that error when the budget drains.
The log tail can contain a pg_restore database error before generic shutdown messages.
In that case, retry events and the terminal `Failed` condition keep it, with the affected relation.

A `Completed` or `Failed` Migration is terminal and never restarts.
The source and target are immutable.
Fix the cause and create a new Migration.

## Deletion

- **Clone-only Migration**: deleting the CR garbage-collects everything it owns (Jobs, work PVC, filters ConfigMap).
  Nothing on either database needs cleaning; there is no finalizer in the way.
- **Live Migration that ever started**: a finalizer routes deletion through cleanup, because a leaked replication slot retains WAL on the source forever.
  The operator first stops the worker, then runs the cleanup Job (drops slot, auto-created publication, target origin), then releases the CR to garbage collection.
  Deletion therefore takes a few reconcile cycles, not milliseconds.
- **Cleanup that cannot finish**: if cleanup exhausts its retries, because the source is gone for instance, the operator emits a `CleanupFailed` warning naming the slot and releases the Migration anyway.
  An unreachable source should not block deletion forever.
  If the source still exists, check `pg_replication_slots` there and `SELECT pg_drop_replication_slot('<slot>')` manually.
- **Deleting the whole namespace** is supported: a terminating namespace refuses new Jobs, so cleanup cannot run there.
  The operator emits the same `CleanupFailed` warning (naming the slot) and releases the Migration, so namespace deletion never deadlocks against the finalizer.
  A source inside the namespace is deleted along with it, so nothing leaks; a source outside it (another namespace, or outside the cluster) keeps its slot and retains WAL.
  For such sources, delete the Migration first and let its cleanup Job finish before removing the namespace.
