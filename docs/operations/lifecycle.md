# Dry run, suspend, retries, deletion

This page covers the lifecycle of a Migration.
It shows how to check a Migration with a dry run and how to suspend it.
It also shows how the operator retries failed attempts, and what deletion cleans up.

Native worker and evidence-bearing helper Job builders omit `ttlSecondsAfterFinished` so the TTL controller cannot delete execution evidence before validation.
Callers MUST retain each helper until checked result capture and the complete proof join consume its evidence.
Retain schema and data verification helpers until the caller durably records fresh final verification.
After that point, callers MUST either delete the exact recorded Job UID or restore the configured TTL.
Clear a helper UID pin only after its caller deletes the known terminal Job and an uncached read returns `NotFound`.
Cleanup remains a caller obligation, and legacy Job TTL policies stay unchanged.

## Initial status

`Pending` is the first phase of a Migration.
The controller persists it, so it is not an API-server default at creation.
`Pending` has no validation condition and no durable timestamp for a condition transition.
An API watch can see it, but a Prometheus scrape may miss the short-lived phase.

## Dry run

`spec.dryRun: true` runs the [preflight](live-migration.md#preflight) and stops there.
No worker Job starts, so pgcopydb never runs.
The dry run copies nothing, and it creates no replication slot, publication, or origin.
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

With a `superuserSecretRef`, the preflight still connects as that user and checks it, but it applies no grant.
It prints each statement that it would apply as `would-remediate:` (follow rights) or `would-remediate-clone:` (clone rights).
For each tier, it also emits one `PreflightWouldRemediate` event that lists those statements.
Without a `superuserSecretRef`, a missing grant fails the dry run exactly as it fails a real preflight.

A dry run applies none of the statements it reports, so it cannot show that they succeed.
On a PostgreSQL 14 target, with a custom runner whose `pg_restore` is version 16 or older, it reports `GRANT SET ON PARAMETER session_replication_role`.
The dry run then passes, although that grant exists only on PostgreSQL 15 and later.
The default runner fails that target earlier, at the [`pg_restore` check](../reference/prerequisites.md#pg_restore-and-the-target-major).
A `superuserSecretRef` user that does not have the right to grant passes too.
Both fail the real preflight.

> [!warning]
> Upgrade the operator before you create a Migration with `dryRun: true`.
> An operator release without the field ignores it and runs a real migration.

`dryRun` is immutable.
For the real run, create a separate Migration without it.

> [!important]
> `spec.dryRun` is not related to `kubectl apply --dry-run`.
> With `spec.dryRun`, the Migration is created, and its preflight Job connects to both databases.
> It still creates the work PVC, although the preflight pod does not mount it.
> A passed dry run reports as `Completed`.
> Argo CD shows it as `Healthy`, the fleet dashboard counts it as completed, and `kubectl wait --for=condition=Complete` returns.
> A dry run is final whether it passes or fails, and it never becomes the real run.

## Suspend

`spec.suspend: true` deletes the worker Job and keeps the work volume.
The delete is in the foreground, so pgcopydb receives SIGTERM and stops cleanly.
The suspend also cancels a running [verification rehearsal](verification.md#rehearsal), and a resume does not start it again.
While the [retry gate](#the-retry-gate) holds a retry, there is no worker.
In that case, the suspend deletes the probe Job of the gate instead, because its test commit writes to the target.
The phase becomes `Suspended`.
Set `spec.suspend` back to false to start the next attempt.
That attempt waits for the retry gate, then resumes from the work-dir catalogs.
Each resume consumes one attempt from the `backoffLimit` budget.
A live migration that you suspend after its replication slot exists and before its table copy ends cannot resume that copy.
A live migration whose worker dies in that window cannot resume it either.
See [Retries and resume](#retries-and-resume).
With `spec.clone.dropIfExists: true`, the resume fails, and the start-over takes a second attempt.
So budget two attempts for each such suspend.
Without `dropIfExists`, the refused resume ends the Migration as `SnapshotLost`.

> [!warning]
> A suspend does NOT drop the replication slot of a live migration.
> The slot continues to retain WAL on the source for the full time of the suspend, without a limit.
> The operator emits a `SlotRetained` warning event about this.
> Monitor the source disk.
> Do not keep live migrations suspended for days.
> Deletion of the Migration drops the slot.

## Retries and resume

`spec.backoffLimit` is the number of retries, so a Migration makes `backoffLimit + 1` counted attempts.
An attempt does not count when the retry gate finds that it was cut during a synchronous standby outage.
See [Exempt attempts](#exempt-attempts).
The default budget is 4 attempts.
Each attempt is a new Job with Kubernetes-level retries disabled.
The operator owns the retry policy, so the Migration holds the attempt counts and the failure reasons.

- Attempt 1 runs with `--restart`, which deletes any state on the work volume.
  A new Migration has no earlier state of its own, so any such state is foreign.
- Retries run with `--resume --not-consistent`.
  The retry skips the tables and indexes that the pgcopydb catalogs record as done.
  It restarts interrupted copies.
  When earlier attempts copied only some parts of a split table, the retry copies every part again.
  When they copied all parts, the retry keeps the table.
  The retry needs `--not-consistent` because the snapshot of the failed attempt died with its process.
  The retry copies the remaining tables under a new snapshot.
  Runners older than `0.18.112.g4c61e97` can lose rows of a split table on a resume without `clone.allDatabases`.
  See [client tool versions](../reference/prerequisites.md#client-tool-versions).
  They also fail a retry with exit code 12 on a table without indexes whose `VACUUM` an earlier attempt started.
  If the failed retry built every index, the retry after it skips the data phase and exits 0.
  A table whose `VACUUM` did not finish then has no planner statistics until you run `ANALYZE` on it.
  Runners older than `0.18.129.gd7547b5` fail every retry with exit code 12 after an attempt died while it built the constraints of a table.
  This occurs with or without `clone.allDatabases`.
  The error names one of the constraints of that table, as in `relation "cx_pkey" already exists`.
  `0.18.129.gd7547b5` still fails every retry that way when the attempt died while it added a constraint, or after it added one and before it recorded it.
  When the attempt died while it added a constraint, its `ALTER TABLE` keeps running on the target.
  With the server default `client_connection_check_interval`, that statement then commits.
  The retry gate holds the retry until that session ends.
  So in both cases, the target holds the constraint without a record.
  From `0.18.132.gd4a2db1` on, the retry keeps such a constraint when its `pg_get_constraintdef()` text equals the text on the source.
  When the two texts differ, every retry stops with exit code 12 on an error that prints both.
  The texts of one constraint can differ when the constraint names an object whose schema is on the `search_path` of one database and not the other.
  An example is the function in an `EXCLUDE` constraint.
  The texts can also differ when the major versions of the two servers print the constraint differently.
  With `clone.allDatabases`, runners older than `0.18.129.gd7547b5` also fail every retry with exit code 12 after an attempt died while it built a split table's indexes.
  The error names one of those indexes, as in `relation "big_gate" already exists`.
  In the same mode, after an attempt died during a split table's copy, these runners can copy a part twice.
  They can also keep rows that the source changed since then.
  A Migration whose retry failed after a constraint or index build on a runner older than `0.18.129.gd7547b5` can keep failing after an upgrade.
  A Migration whose retries fail on a constraint on `0.18.129.gd7547b5` converges after an upgrade to `0.18.132.gd4a2db1`, unless it is already `Failed`.
  Replace a `Failed` Migration with a new one.
  Give the new Migration a target that does not hold the failed copy.
  Without `clone.allDatabases`, you can instead set `clone.dropIfExists: true` on the new Migration.
  Without `clone.allDatabases`, the retry of a runner older than `0.18.129.gd7547b5` skips an unfinished large-object or extension configuration step.
  It does this when the tables, indexes, and sequences were already done.
  It then exits 0 without what that step did not copy.
- A live migration cannot resume its table copy that way after its replication slot exists.
  The slot replays every change since its own snapshot, so tables copied under a newer one would get some changes twice.
  pgcopydb refuses that resume with `Refusing to resume the copy: ...`, and the operator starts the next attempt over.
  The start-over runs `pgcopydb stream cleanup`, which drops the slot, the automatic publication, and the target origin.
  Then it runs with `--restart`.
  It needs `spec.clone.dropIfExists: true`, so that the restore replaces the objects that the earlier attempts left on the target.
  Without `dropIfExists`, the Migration ends as `SnapshotLost`.
  The refused resume and the start-over each consume one attempt.
  So a failure between the creation of the slot and the end of the table copy costs two attempts.
  Size `spec.backoffLimit` for that.
  The default of 3 survives one such failure, but not two.
  Runners older than `0.18.104.g362a559` do not have the refusal.
  See [client tool versions](../reference/prerequisites.md#client-tool-versions).
  A runner without it resumes the copy anyway, and the apply can then fail on a duplicate key or leave the target diverged.
- A worker Job that disappears triggers the next attempt the same way.
  The `ttlSecondsAfterFinished` TTL and manual deletion both delete the Job.
- A deleted pod whose network went with it is an example of a runner lost without closing its connections.
  Such a runner can leave target sessions open with their table locks.
  The [retry gate](#the-retry-gate) holds the next attempt until those sessions are gone, so the attempt does not start behind them.
  From runner `0.18.109.gbec4ebb` on, the target ends a session that waits on the lost runner, such as an open `COPY`, within about 70 seconds.
  With older runners, such a session can stay for about two hours.
  With the server default `client_connection_check_interval`, a session busy in a long statement such as `CREATE INDEX` stays until that statement ends.
  The `pg_restore` sessions do not get that bound, so they can stay for longer.
  The same is true for the session in which `pgcopydb compare` reads the catalogs of the target.
  It is also true for any session that reaches the target through a connection pooler.
- A retry cannot fix some failures, such as a source that dies during the copy or a broken restore.
  These failures still consume the budget one attempt at a time.
- The [preflight](live-migration.md#preflight) stops some failures before the first attempt.
  On every migration, it checks connectivity, credentials, target clone privileges, and that no PgBouncer sits in either connection.
  On live migrations, it also checks the follow prerequisites.
- Permission errors end the Migration as `PermissionDenied`, on the attempt whose log tail shows one as the terminal cause.
  Permission matching is best-effort, so a missed match keeps normal retries.
- A full work volume ends the Migration as `DiskFull` the same way.
  A retry cannot free space on it, and its resume may fail on an unrelated error that hides the cause.
  A target out of disk space or shared memory keeps normal retries, because space or load can change between attempts.
- A change that the apply cannot replay because the target does not have its column ends the Migration as `SchemaDrift` the same way.
  Usually, DDL ran on the source, and every retry would stop at the same change.

When worker logs are readable, the `AttemptFailed` event names the pgcopydb error after each failed attempt.
A start-over emits a `SnapshotLost` event in its place.
The terminal `Failed` condition keeps that error when the budget is empty.
When the log read fails, the operator retries it for two minutes after the Job fails.
So a short API server outage does not cost the `PermissionDenied`, `DiskFull`, `SchemaDrift`, or `SnapshotLost` check.
After that time, the attempt retries without the check, and the event and the condition contain the read error instead.
The log tail can contain a pg_restore database error before generic shutdown messages.
In that case, retry events and the terminal `Failed` condition keep it, with the affected relation.

A `Completed` or `Failed` Migration is terminal and never restarts.
The source and target are immutable, except `spec.target.synchronousCommit`, which applies from the next attempt.
Fix the cause.
Then create a new Migration.

### The retry gate

Every attempt after the first waits for the retry gate.
This includes a retry after a failure, the attempt after a worker Job that disappeared, and the attempt that a resume starts.
A failed attempt can leave target sessions behind.
For example, when a proxy cuts a worker while its commit waits for a synchronous standby, the backend continues to wait on the target.
That backend holds its locks and, for apply, the replication origin.
A new attempt would fail on those sessions or wait behind them.

The gate checks two things in order:

1. The worker pod of the previous attempt has ended, and so has the pod of a canceled [verification rehearsal](verification.md#rehearsal).
   While one of them is running or terminating, `RetryHeld` is True with reason `WorkerPodRemains`, and no probe runs.
   We wait for the pod, because a terminating pod can still hold source connections that the target cannot show.
2. A probe Job, `<name>-gate-<n>` for attempt n, passes.
   It connects as the migration role over the worker's own target connection.
   Its pod takes at most 100m CPU and 128Mi memory from `spec.runner.resources`.
   A namespace LimitRange with a `min` above these values rejects the probe pod.
   In that case, the gate holds with `ProbeFailed` after the probe Job reaches its deadline.

The probe runs up to three checks:

- A census of the migration role's pgcopydb-named sessions in the target database, or on the whole instance with `clone.allDatabases`.
  It passes when it finds none.
- With `spec.target.synchronousCommit: local`, `show synchronous_commit`, which has to read `local`.
- A test commit with `synchronous_commit=on`, which passes when it returns within 5 seconds.
  Under synchronous replication, it returns only after a synchronous standby acknowledges it.
  It runs whenever the next worker could commit with `on`.
  Without `synchronousCommit`, it always runs.
  With `synchronousCommit`, it runs after the copy finishes, because apply then sets `on` itself.

When every check passes, the worker of the next attempt starts.
Otherwise, `RetryHeld` stays True with the reason of the first check that failed, and the gate probes again 30 seconds later.
A hold spends no attempt.
The gate ends no session: the sessions end by themselves, or you end them.
[`RetryHeld` is True](../troubleshooting.md#retryheld-is-true) covers each reason.
The test commit creates a temporary table, so the migration role needs `TEMPORARY` on the target database.
`PUBLIC` holds that privilege by default.
A suspend stops a hold, and the resume runs the gate again.

### Exempt attempts

An attempt that failed while its commits waited for a missing synchronous standby does not count against `backoffLimit`.
The first census after the failure decides.
The attempt is exempt when that census finds a pgcopydb session that waits for synchronous replication.
The statement of that session must have started 30 seconds or more before the attempt failed.
Such a wait started while the worker still ran, and it lasted longer than the worker.
That is what a proxy cut during a standby outage leaves behind.
A catch-up wait that started just before an unrelated failure does not qualify.
Neither does any wait when the target URI sets `application_name`, because the census then cannot tell the pgcopydb sessions from other sessions.
Only a failed attempt can be exempt.
An attempt whose Job disappeared, or that a suspend stopped, always counts.

`status.exemptAttempts` counts the exempt attempts.
`kubectl get pgm -o wide` shows it in the `EXEMPT` column.
So `ATTEMPTS` can exceed `backoffLimit + 1`.
The `AttemptStarted` event of the next attempt says that the failed attempt did not count.
No limit applies to exempt attempts.
A standby that keeps failing keeps the Migration retrying, and the `PgcopydbMigrationRetrying` and `PgcopydbMigrationCommitsBlocked` alerts report it.

When no census decides in the 15 minutes after the failure, the attempt counts.
One example cause is a probe that cannot reach the target.
If the budget is then empty, the Migration fails as `BackoffLimitExceeded`.
Otherwise, the gate keeps holding until a probe passes.
Time behind `WorkerPodRemains` and time suspended count toward the 15 minutes.

## Deletion

- **Clone-only Migration**: when you delete the CR, garbage collection deletes everything that the CR owns (Jobs, work PVC, filters ConfigMap).
  Nothing on either database needs cleaning.
  No finalizer blocks the deletion.
- **Live Migration that ever started**: a finalizer routes deletion through cleanup, because a leaked replication slot retains WAL on the source forever.
  The operator first stops the worker and a running [verification rehearsal](verification.md#rehearsal).
  It then runs the cleanup Job, which drops the slot, the auto-created publication, and the target origin.
  Last, it releases the CR to garbage collection.
  Deletion therefore takes a few reconcile cycles, not milliseconds.
- **Native live Migration that ever started** (`spec.engine: rpgmigrate`): the finalizer routes deletion through an inventory helper.
  The helper reads the retained work volume and connects to the source and the target.
  When the work volume records a slot or a publication, the operator runs the cleanup Job, then reads the inventory again.
  Sometimes the engine stopped before it recorded a creation.
  Then the helper must also find no slot, publication or target schema with the names of the run.
  Two such complete inventories release the Migration without a cleanup Job, so the operator changes neither database.
  An inventory that does not run, does not complete in time, or cannot connect keeps the finalizer.
  An inventory that finds such a resource also keeps it, and the operator tries again.
- **Cleanup that cannot finish**: if cleanup uses all its retries, the operator emits a `CleanupFailed` warning that names the slot.
  It then releases the Migration anyway.
  This occurs, for instance, when the source is gone.
  An unreachable source should not block deletion forever.
  If the source still exists, check `pg_replication_slots` on the source.
  Then run `SELECT pg_drop_replication_slot('<slot>')` manually.
- **Deleting the whole namespace**: we support it.
  A terminating namespace refuses new Jobs, so cleanup cannot run there.
  The operator emits the same `CleanupFailed` warning, which names the slot, and releases the Migration.
  So namespace deletion never deadlocks against the finalizer.
  The namespace deletion also deletes a source inside the namespace, so nothing leaks.
  A source outside the namespace (another namespace, or outside the cluster) keeps its slot and retains WAL.
  For such sources, delete the Migration first.
  Let its cleanup Job finish before you delete the namespace.
