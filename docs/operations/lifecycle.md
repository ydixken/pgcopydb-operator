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
On a PostgreSQL 14 target, with a custom runner whose `pg_restore` is version 16 or older, it reports `GRANT SET ON PARAMETER session_replication_role` and passes, although that grant exists only on PostgreSQL 15 and later.
The default runner fails that target earlier, at the [`pg_restore` check](../reference/prerequisites.md#pg_restore-and-the-target-major).
A `superuserSecretRef` user that lacks the right to grant passes too.
Both fail the real preflight.

> [!warning]
> Upgrade the operator before you create a Migration with `dryRun: true`.
> An operator release without the field ignores it and runs a real migration.

`dryRun` is immutable.
For the real run, create a separate Migration without it.

> [!important]
> `spec.dryRun` has nothing to do with `kubectl apply --dry-run`: the Migration is created, and its preflight Job connects to both databases.
> It still creates the work PVC, although the preflight pod does not mount it.
> A passed dry run reports as `Completed`: Argo CD shows it `Healthy`, the fleet dashboard counts it as completed, and `kubectl wait --for=condition=Complete` returns.
> A dry run is final whether it passes or fails, and it never turns into the real run.

## Suspend

`spec.suspend: true` deletes the worker Job and keeps the work volume.
The delete is in the foreground, so pgcopydb receives SIGTERM and shuts down cleanly.
While the [retry gate](#the-retry-gate) holds a retry, there is no worker, and the suspend deletes the gate's probe Job instead, because its test commit writes to the target.
The phase becomes `Suspended`.
Set `spec.suspend` back to false to start the next attempt.
It waits for the retry gate, then resumes from the work-dir catalogs.
Each resume consumes one attempt from the `backoffLimit` budget.
A live migration suspended after its replication slot exists and before all its table data is copied cannot resume that copy, and neither can one whose worker dies in that window; see [Retries and resume](#retries-and-resume).
With `spec.clone.dropIfExists: true`, the resume fails and the start-over takes a second attempt, so budget two attempts for each such suspend.
Without `dropIfExists`, the refused resume ends the Migration as `SnapshotLost`.

> [!warning]
> Suspending a live migration does NOT drop the replication slot.
> The slot keeps retaining WAL on the source for as long as the Migration is suspended, without bound, and the operator emits a `SlotRetained` warning event to that effect.
> Watch source disk, and do not park live migrations for days.
> Deleting the Migration drops the slot.

## Retries and resume

`spec.backoffLimit` is the number of retries, so a Migration makes `backoffLimit + 1` counted attempts.
An attempt that the retry gate finds cut during a synchronous standby outage does not count; see [Exempt attempts](#exempt-attempts).
The default budget is 4 attempts.
Each attempt is a fresh Job with Kubernetes-level retries disabled.
The operator owns the retry policy, so attempt counts and failure reasons live on the Migration.

- Attempt 1 runs with `--restart`, which wipes any state on the work volume.
  A fresh Migration has no prior state of its own, so that state is foreign.
- Retries run with `--resume --not-consistent`.
  The retry skips tables and indexes already recorded done in pgcopydb's catalogs, and restarts interrupted copies.
  When only some parts of a split table were copied, the retry copies every part again, and when all were, it keeps the table.
  `--not-consistent` is needed because the failed attempt's snapshot died with its process.
  The retry copies the remaining tables under a new snapshot.
  Runners older than `0.18.112.g4c61e97` can lose rows of a split table on a resume without `clone.allDatabases`; see [client tool versions](../reference/prerequisites.md#client-tool-versions).
  They also fail a retry with exit code 12 on a table without indexes whose `VACUUM` an earlier attempt had started.
  If the failed retry built every index, the retry after it skips the data phase and exits 0.
  A table whose `VACUUM` had not finished then has no planner statistics until you run `ANALYZE` on it.
  Runners older than `0.18.129.gd7547b5` fail every retry with exit code 12 after an attempt died while it built a table's constraints, with or without `clone.allDatabases`.
  The error names one of that table's constraints, as in `relation "cx_pkey" already exists`.
  `0.18.129.gd7547b5` still fails every retry that way when the attempt died while it added a constraint, or after it added one and before it recorded it.
  When it died while it added one, its `ALTER TABLE` keeps running on the target and commits, and the retry gate holds the retry until that session has ended, so either way the target holds the constraint without a record.
  From `0.18.132.gd4a2db1` on, the retry keeps such a constraint when its `pg_get_constraintdef()` text equals the source's.
  When the two texts differ, every retry stops with exit code 12 on an error that prints both.
  The texts of one constraint can differ when it names an object whose schema is on the `search_path` of one database and not the other, such as the function in an `EXCLUDE` constraint, or when the two servers' major versions print it differently.
  With `clone.allDatabases`, runners older than `0.18.129.gd7547b5` also fail every retry with exit code 12 after an attempt died while it built a split table's indexes, on an error that names one of those indexes (as in `relation "big_gate" already exists`), and after one died during a split table's copy they can copy a part twice or keep rows that the source changed since.
  A Migration whose retry failed after a constraint or index build on a runner older than `0.18.129.gd7547b5` keeps failing after an upgrade.
  One whose retries fail on a constraint on `0.18.129.gd7547b5` converges after an upgrade to `0.18.132.gd4a2db1`, unless it has already ended `Failed`.
  Replace a `Failed` Migration with a new one against a target that does not hold the failed copy, or, without `clone.allDatabases`, with a new one with `clone.dropIfExists: true`.
  Without `clone.allDatabases`, the retry of a runner older than `0.18.129.gd7547b5` skips an unfinished large-object or extension configuration step when the tables, indexes and sequences were already done, and then exits 0 without what that step had not copied.
- A live migration cannot resume its table copy that way once its replication slot exists.
  The slot replays every change since its own snapshot, so tables copied under a newer one would get some changes twice.
  pgcopydb refuses that resume with `Refusing to resume the copy: ...`, and the operator starts the next attempt over.
  The start-over runs `pgcopydb stream cleanup`, which drops the slot, the automatic publication, and the target origin, then `--restart`.
  It needs `spec.clone.dropIfExists: true`, so the restore replaces the objects the earlier attempts left on the target.
  Without `dropIfExists`, the Migration ends as `SnapshotLost`.
  The refused resume and the start-over each consume one attempt, so a failure between the slot's creation and the end of the table copy costs two.
  Size `spec.backoffLimit` for that: the default of 3 survives one such failure, not two.
  Runners older than `0.18.104.g362a559` lack the refusal; see [client tool versions](../reference/prerequisites.md#client-tool-versions).
  A runner without it resumes the copy anyway, and the apply can then fail on a duplicate key or leave the target diverged.
- A worker Job that disappears triggers the next attempt the same way.
  The `ttlSecondsAfterFinished` TTL and manual deletion both remove the Job.
- A runner lost without closing its connections, such as a deleted pod whose network went with it, can leave target sessions open with their table locks.
  The [retry gate](#the-retry-gate) holds the next attempt until they are gone, so the attempt does not start behind them.
  From runner `0.18.109.gbec4ebb` on, the target ends a session that waits on the lost runner, such as an open `COPY`, within about 70 seconds.
  With older runners such a session can stay for about two hours.
  A session busy in a long statement such as `CREATE INDEX` stays until that statement ends.
  The `pg_restore` sessions, the session in which `pgcopydb compare` reads the target's catalogs, and any session that reaches the target through a connection pooler do not get that bound, so they can stay for longer.
- Failures the retry cannot fix, such as a source that dies mid-copy or a broken restore, still consume the budget one attempt at a time.
- The [preflight](live-migration.md#preflight) stops some failures before the first attempt.
  It checks connectivity, credentials, that no PgBouncer sits in either connection, and target clone privileges on every migration, and the follow prerequisites on live ones.
- Permission errors end the Migration as `PermissionDenied`, on the attempt whose log tail shows one as the terminal cause.
  Permission matching is best-effort, so a missed match keeps normal retries.
- A full work volume ends the Migration as `DiskFull` the same way.
  A retry cannot free space on it, and its resume may fail on an unrelated error that hides the cause.
  A target out of disk space or shared memory keeps normal retries, because space or load can change between attempts.
- A change the apply cannot replay because the target lacks its column ends the Migration as `SchemaDrift` the same way.
  Usually DDL ran on the source, and every retry would stop at the same change.

When worker logs are readable, the `AttemptFailed` event names the pgcopydb error after each failed attempt.
A start-over emits a `SnapshotLost` event in its place.
The terminal `Failed` condition keeps that error when the budget drains.
When the log read fails, the operator retries it for two minutes after the Job fails, so a short API server outage does not cost the `PermissionDenied`, `DiskFull`, `SchemaDrift`, or `SnapshotLost` check.
Past that, the attempt retries without the check, and the event and the condition carry the read error instead.
The log tail can contain a pg_restore database error before generic shutdown messages.
In that case, retry events and the terminal `Failed` condition keep it, with the affected relation.

A `Completed` or `Failed` Migration is terminal and never restarts.
The source and target are immutable, except `spec.target.synchronousCommit`, which applies from the next attempt.
Fix the cause and create a new Migration.

### The retry gate

Every attempt after the first waits for the retry gate: a retry after a failure, the attempt after a vanished worker Job, and the attempt a resume starts.
A failed attempt can leave target sessions behind.
For example, a proxy that cuts a worker while its commit waits for a synchronous standby leaves the backend waiting on the target, holding its locks and, for apply, the replication origin.
A new attempt would fail on those sessions or wait behind them.

The gate checks two things in order:

1. The previous attempt's worker pod has ended.
   While it runs or terminates, `RetryHeld` is True with reason `WorkerPodRemains`, and no probe runs.
   We wait for the pod, because a terminating pod can still hold source connections that the target cannot show.
2. A probe Job, `<name>-gate-<n>` for attempt n, passes.
   It connects as the migration role over the worker's own target connection.
   Its pod takes at most 100m CPU and 128Mi memory from `spec.runner.resources`.
   A namespace LimitRange with a `min` above these values rejects the probe pod, and once the probe Job reaches its deadline the gate holds with `ProbeFailed`.

The probe runs up to three checks:

- A census of the migration role's pgcopydb-named sessions in the target database, or on the whole instance with `clone.allDatabases`.
  It passes when it finds none.
- With `spec.target.synchronousCommit: local`, `show synchronous_commit`, which has to read `local`.
- A test commit with `synchronous_commit=on`, which passes when it returns within 5 seconds.
  Under synchronous replication, it returns only once a synchronous standby confirms it.
  It runs whenever the next worker could commit with `on`: always without `synchronousCommit`, and with it once the copy is done, because apply then sets `on` itself.

When every check passes, the next attempt's worker starts.
Otherwise `RetryHeld` stays True with the reason of the first check that failed, and the gate probes again 30 seconds later.
A hold spends no attempt, and the gate ends no session: the sessions end on their own, or you end them.
[`RetryHeld` is True](../troubleshooting.md#retryheld-is-true) covers each reason.
The test commit creates a temporary table, so the migration role needs `TEMPORARY` on the target database, which `PUBLIC` holds by default.
A suspend stops a hold, and the resume runs the gate again.

### Exempt attempts

An attempt that failed while its commits waited for a missing synchronous standby does not count against `backoffLimit`.
The first census after the failure decides.
The attempt is exempt when that census finds a pgcopydb session waiting for synchronous replication whose statement started 30 seconds or more before the attempt failed.
Such a wait started while the worker still ran and outlived it, which is what a proxy cut during a standby outage leaves behind.
A catch-up wait that began just before an unrelated failure does not qualify.
Neither does any wait when the target URI sets `application_name`, because the census then cannot tell pgcopydb's sessions from others.
Only a failed attempt can be exempt: an attempt whose Job vanished, or that a suspend stopped, always counts.

`status.exemptAttempts` counts the exempt attempts, and `kubectl get pgm -o wide` shows it in the `EXEMPT` column, so `ATTEMPTS` can exceed `backoffLimit + 1`.
The next attempt's `AttemptStarted` event says that the failed one did not count.
Exempt attempts are not capped: a standby that keeps failing keeps the Migration retrying, and the `PgcopydbMigrationRetrying` and `PgcopydbMigrationCommitsBlocked` alerts report it.

When no census has decided 15 minutes after the failure, for example because the probe cannot reach the target, the attempt counts.
With the budget spent, the Migration then fails as `BackoffLimitExceeded`; otherwise the gate keeps holding until a probe passes.
Time behind `WorkerPodRemains` and time suspended count toward the 15 minutes.

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
