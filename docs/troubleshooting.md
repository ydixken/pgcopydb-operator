# Troubleshooting

Run `kubectl describe migration <name>` first, and read the conditions and the events.
Then read the worker pod logs with `kubectl logs job/<name>-run-<N>`, which are structured JSON.
The `Failed` condition carries the last pgcopydb ERROR from the failed pod, after Kubernetes' own "Job has reached the specified backoff limit".
It falls back to the bare Job message when the pod is already gone.

| Symptom | Section |
|---|---|
| [`describe` shows no phase, no conditions, no events](#migration-has-no-status-and-the-log-repeats-is-immutable) | Spec and admission |
| [An update fails with `<field> is immutable`, for `dryRun`, `allDatabases`, `ownerAfterRestore`, `source`, `target` or `follow`](#an-update-fails-with-field-is-immutable) | Spec and admission |
| [Phase `Failed` at `Validating`, reason `PreflightFailed`](#phase-failed-at-validating) | Preflight failures |
| [`PreflightFailed` names the all-databases superuser requirement](#all-databases-superuser-requirement) | Preflight failures |
| [`PreflightFailed` names unavailable selected extensions](#unavailable-selected-extensions) | Preflight failures |
| [`Validated` is `Unknown` with reason `PreflightRunning`](#preflight-pod-cannot-start) | Preflight failures |
| [`PreflightFailed` and the Job shows `DeadlineExceeded`](#preflight-deadline-exceeded) | Preflight failures |
| [`PreflightFailed` with `the check output was not readable`](#preflight-output-not-readable) | Preflight failures |
| [`applying ... via superuserSecretRef failed`](#superusersecretref-cannot-apply-the-grants) | Preflight failures |
| [`PreflightFailed` with `still lacks ... after remediation`](#a-grant-is-still-missing-after-remediation) | Preflight failures |
| [`no replica identity usable for UPDATE/DELETE`](#no-replica-identity-usable-for-updatedelete) | Preflight failures |
| [`row-level security hides rows of these tables`](#row-level-security-hides-rows) | Preflight failures |
| [`follow cannot replicate unlogged tables`](#follow-cannot-replicate-unlogged-tables) | Preflight failures |
| [`... connection goes through a connection pooler such as PgBouncer`](#a-connection-goes-through-pgbouncer) | Preflight failures |
| [`spec.target.synchronousCommit is local but the target connection reports ...`](#target-synchronous_commit-is-not-local) | Preflight failures |
| [`the runner's pg_restore ... cannot restore into the target's PostgreSQL ...`](#the-runners-pg_restore-cannot-restore-into-the-target) | Preflight failures |
| [`must be owner of extension ...`, or `target extension ownership required`](#extension-ownership-failures) | Extension ownership failures |
| [Slot creation fails with `could not access file "wal2json"`](#wal2json-is-not-installed-on-the-source) | Replication and follow |
| [Slot creation fails with `may not be used as an output plugin`](#wal2json-is-not-trusted-by-the-source) | Replication and follow |
| [`permission denied for function pg_replication_origin_drop`](#permission-denied-for-function-pg_replication_origin_drop) | Replication and follow |
| [The migration completes, but changes from the window are missing](#changes-from-the-window-are-missing-on-the-target) | Replication and follow |
| [`permission denied to start WAL sender`](#permission-denied-to-start-wal-sender) | Replication and follow |
| [The stream keeps dropping and attempts burn](#the-stream-keeps-dropping) | Replication and follow |
| [`CaughtUp` stays False](#caughtup-stays-false) | Replication and follow |
| [`CommitsBlocked` is True, or `PgcopydbMigrationCommitsBlocked` fires](#commits-blocked-on-synchronous-replication) | Synchronous replication and retries |
| [`RetryHeld` is True and no new attempt starts](#retryheld-is-true) | Synchronous replication and retries |
| [`publication ... already exists` or `publication retry refused`](#publication-retry-failures) | Publication retry failures |
| [The worker pod runs and logs, but the phase never moves](#the-worker-pod-runs-but-the-phase-never-moves) | Clone progress |
| [`Finalizing` runs long with the target size flat](#finalizing-takes-a-long-time) | Clone progress |
| [`Streaming` and `CaughtUp` stay unset during the clone](#streaming-and-caughtup-stay-unset-during-the-clone) | Clone progress |
| [`status.progress` stands still while a large table copies](#progress-stops-moving-while-a-large-table-copies) | Clone progress |
| [`CloneCompleted` is `False` with reason `TablesEmptyOnTarget`](#clonecompleted-is-false-with-reason-tablesemptyontarget) | Clone progress |
| [Phase `Failed`, reason `CloneIncomplete`](#phase-failed-with-reason-cloneincomplete) | Clone progress |
| [The `<name>-verify` pod runs for a long time](#the-verify-pod-runs-for-a-long-time) | Cutover and verification |
| [Phase `Failed`, reason `DrainIncomplete`](#phase-failed-with-reason-drainincomplete) | Cutover and verification |
| [Phase `CutoverPending` and nothing happens](#phase-cutoverpending-and-nothing-happens) | Cutover and verification |
| [Phase `Failed`, reason `OwnershipFailed`](#ownership-handover-failures) | Ownership handover failures |
| [Event `... exists but belongs to another owner`](#exists-but-belongs-to-another-owner) | Workers and cluster objects |
| [`pg_dump: error: server version mismatch`](#pg_dump-reports-a-server-version-mismatch) | Workers and cluster objects |
| [No worker pod, PVC `Pending`](#no-worker-pod-and-the-pvc-stays-pending) | Workers and cluster objects |
| [Phase `Failed`, "retry budget exhausted"](#phase-failed-with-retry-budget-exhausted) | Workers and cluster objects |
| [Phase `Failed`, reason `PermissionDenied`, after one attempt](#phase-failed-with-reason-permissiondenied) | Workers and cluster objects |
| [Phase `Failed`, reason `DiskFull`, after one attempt](#phase-failed-with-reason-diskfull) | Workers and cluster objects |
| [Phase `Failed`, reason `SchemaDrift`, after one attempt](#phase-failed-with-reason-schemadrift) | Workers and cluster objects |
| [Phase `Failed`, reason `SnapshotLost`](#phase-failed-with-reason-snapshotlost) | Workers and cluster objects |
| [Status and metrics lag behind the worker](#status-updates-lag-behind-the-worker) | Workers and cluster objects |
| [Scrapes return 401](#scrapes-return-401) | Metrics and dashboards |
| [Scrapes return 403](#scrapes-return-403) | Metrics and dashboards |
| [Scrapes return 500 and the manager logs `Authentication failed`](#scrapes-return-500) | Metrics and dashboards |
| [`status.progress` and the clone metrics stop at the estimate](#progress-metrics-stop-at-the-estimate) | Metrics and dashboards |
| [Grafana never shows the installed dashboards](#dashboards-never-appear-in-grafana) | Metrics and dashboards |
| [The installed PrometheusRule alerts never appear](#prometheusrule-alerts-never-appear-in-prometheus) | Metrics and dashboards |

## Spec and admission

### Migration has no status and the log repeats `is immutable`

`kubectl describe` on a follow migration shows nothing at all: no phase, no conditions, no events.
The operator log repeats `follow is immutable`, or `source is immutable`, or `target is immutable`.

The spec gives an optional field of an immutable block an explicit zero value.
The immutable blocks are `follow`, `source`, and `target`, apart from `target.synchronousCommit`.
The typical case is `allowMissingReplicaIdentity: []` from a template that renders an empty list.
Operators up to v0.8.1 rejected that spelling before any status could be set.

Upgrade past v0.8.1, where both spellings work.
On an older version, omit the field or set it to `null` instead of `[]`, `""`, or `false`.

### An update fails with `<field> is immutable`

Admission rejects the changes below to an existing Migration, each with its message:

- `dryRun is immutable: create a separate Migration for the real run`: a dry run stops before the first worker attempt, so turning it on mid-run would halt the migration between attempts, and turning it off after completion would change nothing.
- `allDatabases is immutable: create a separate Migration`: retries and compares reuse the work directory that mode laid out.
  Dropping the field or the whole `clone` block from a Migration created with `allDatabases: true` is such a change.
- `ownerAfterRestore is immutable: the preflight probes the target role before the first attempt, and the handover Job is built once`.
- `source is immutable`, `target is immutable except synchronousCommit` or `follow is immutable`: admission rejects any change to an endpoint apart from `target.synchronousCommit`, and any change inside a `follow` block that the Migration already has.
  If the Migration also shows no status at all, see [Migration has no status and the log repeats `is immutable`](#migration-has-no-status-and-the-log-repeats-is-immutable).

Create a new Migration with the spec you want.

## Preflight failures

### Phase `Failed` at `Validating`

Phase `Failed` at `Validating` with reason `PreflightFailed` means one preflight check failed.
The possible causes are connectivity, PgBouncer in a connection, the target's `synchronous_commit`, the major version gate, the runner's `pg_restore` against the target major, selected extension availability or ownership, a clone privilege, a source table audit, an all-databases probe, or a follow prerequisite.

Follow the condition's recovery action, then create a new Migration.
No worker attempt has started.

### All-databases superuser requirement

`PreflightFailed` names the all-databases superuser requirement when a migration connection lacks `rolsuper`.

Use superuser migration credentials on both sides and recreate the Migration.
`superuserSecretRef` and managed admin roles without `rolsuper` do not satisfy [this contract](reference/prerequisites.md#all-databases).

### Unavailable selected extensions

`PreflightFailed` names unavailable selected extensions when the target lacks both an installed extension and its default package.
It also names them when `dropIfExists` or `allDatabases` needs a missing default version.

Install the missing extension package on the target, or choose a target that provides it.
Then create a new Migration.
Preflight does not install extensions, and source and target versions need not match.

### Preflight pod cannot start

The Migration sits in `Validating`, and `Validated` is `Unknown` with reason `PreflightRunning`.
The preflight pod cannot start, and the condition message carries the kubelet reason verbatim: `CreateContainerConfigError`, `Unschedulable`, or a pull error.
A misnamed credentials Secret is the typical cause.

Fix the object the message names.
The gate then resumes on its own.
The preflight Job is bounded at 30 minutes, and then the Migration fails.

### Preflight deadline exceeded

`PreflightFailed` with `DeadlineExceeded` on the Job means a check hung past the 30-minute preflight deadline, or the pod never started in time.
Each connect times out after 10 seconds, and preflight retries it six times.
The condition carries the server's line for a permanent error, such as a wrong password, an unknown role, or an unknown database.

Read the condition message and the preflight Job logs for the last check that ran.
Fix the endpoint or the object it names.
Then create a new Migration.

### Preflight output not readable

`PreflightFailed` with `the check output was not readable (...)` means the operator could not read the preflight pod's log.
The text in parentheses is the read error, such as an API server timeout or a pod that no longer exists.
The operator retries the read for two minutes after the Job fails, and the Migration stays in `Validating` until then.
`Job <name>-preflight printed no check output` is different: the read worked and the log was empty, so check the pod's events.

Read the verdict with `kubectl logs job/<name>-preflight`.
Fix what it names, then create a new Migration.

### `superuserSecretRef` cannot apply the grants

The preflight log shows `warn: ... lacks rolsuper` and later `applying ... via superuserSecretRef failed`.
The `superuserSecretRef` role is no superuser and lacks the rights to apply the grants.
A missing `rolsuper` alone is only a `PreflightWarning` event, because managed admin roles work without it.

Point the Secret at a role that can run the printed statements: a superuser, or the provider's admin role.
If no such role is available, drop `superuserSecretRef` and apply the grants by hand.

### A grant is still missing after remediation

`PreflightFailed` with `still lacks ... after remediation` means preflight applied a grant, and the grant did not stick.
On a target older than PostgreSQL 15, the `session_replication_role` GRANT does not exist.

Check the target's major version.
If it is older than PostgreSQL 15, connect the migration as a superuser-capable role.

### No replica identity usable for UPDATE/DELETE

`PreflightFailed` with `no replica identity usable for UPDATE/DELETE` and a table list names the tables at risk.
Once such a table is published, an UPDATE or a DELETE on it fails on the source.
The audit covers all user tables, and `clone.filters` does not exclude a table from it.

Give each table a primary key, `REPLICA IDENTITY USING INDEX`, or `REPLICA IDENTITY FULL`.
If a table is read-only or insert-only, acknowledge it in `spec.follow.allowMissingReplicaIdentity` in a new Migration.
The value `["*"]` acknowledges all of them.

### Row-level security hides rows

`PreflightFailed` with `row-level security hides rows of these tables` names tables whose policies apply to the source migration role.
The bundled runner would fail the copy on them with SQLSTATE 42501.
A runner before `0.18.34.g7fddd6f` would copy only the rows that role can see, and its comparison would agree, so the loss would go unnoticed.

Give the role `BYPASSRLS`, disable row-level security on those tables for the migration, or leave them out with `clone.filters`.
Then create a new Migration.
See [Row-level security](reference/prerequisites.md#row-level-security) for when policies apply and how the audit scopes tables.

### Follow cannot replicate unlogged tables

`PreflightFailed` with `follow cannot replicate unlogged tables` names unlogged tables in a follow Migration's scope.
Their changes never reach the WAL, so pgcopydb cannot decode them, and its publication refuses them.

Run `ALTER TABLE ... SET LOGGED` on the source, or leave the tables out with `excludeTables` or `excludeSchemas`.
The list can name tables outside `includeOnlySchemas`: without `includeOnlyTables`, pgcopydb still publishes them, so leave them out with `excludeTables`.
Then create a new Migration.

### A connection goes through PgBouncer

`PreflightFailed` with `the <side> connection goes through a connection pooler such as PgBouncer (the server's backend PID is <n>, the connection reported <m>)` means a connection pooler, such as PgBouncer, sits between the runner and PostgreSQL on that side.
The check compares, on one session, the backend PID in psql 18's `\conninfo` with `pg_backend_pid()`, and PgBouncer issues a cancel key of its own, so the two differ in every pool mode.
A direct connection or a TCP proxy forwards the server's key; pgpool-II does too, so the check does not detect it.

`cannot tell whether the <side> connection goes through a connection pooler such as PgBouncer` fails as well, because we never read a missing answer as a direct connection.
The text after it names the reason: the probe connection failed, psql printed no `Backend PID` in `\conninfo`, or `select pg_backend_pid()` returned no PID.
A custom runner image with psql older than 18 prints no `Backend PID`.

Point that side of the Migration at PostgreSQL directly, for example at the database's primary Service instead of the pooler's.
With a custom `spec.runner.image`, use one that ships psql 18 or later.
Then create a new Migration.

### Target `synchronous_commit` is not local

`PreflightFailed` with `spec.target.synchronousCommit is local but the target connection reports synchronous_commit=<value>` means the libpq startup option did not reach the server.
A connection pooler or proxy on the way dropped it, as a CloudNativePG Pooler or a PgBouncer with `options` in `ignore_startup_parameters` does.
Listing `synchronous_commit` in PgBouncer's `track_extra_parameters` does not change that: in session mode, PgBouncer 1.25.2 accepts the option at connect and never sends it to the server.
Behind PgBouncer, the [PgBouncer check](#a-connection-goes-through-pgbouncer) fails as well.
A PgBouncer that refuses the option fails the connectivity check instead, and that message names `spec.target.synchronousCommit`.

Connect the target directly, or leave `synchronousCommit` unset.
Then create a new Migration.
See [Synchronous commit on the target](configuration.md#synchronous-commit-on-the-target).

### The runner's `pg_restore` cannot restore into the target

`PreflightFailed` with `the runner's pg_restore <n> cannot restore into the target's PostgreSQL <m>` means the runner's `pg_restore` is version 17 or later and the target runs PostgreSQL 16 or older.
`pg_restore` 17 and later send `SET transaction_timeout = 0`, which those servers reject with `unrecognized configuration parameter "transaction_timeout"`, so every worker attempt would fail at the pre-data restore.
The default runner ships `pg_restore` 18.

Use a PostgreSQL 17 or later target, or set `spec.runner.image` to an image whose `pg_restore` is version 16 or older, as [Client tool versions](reference/prerequisites.md#client-tool-versions) describes.
Then create a new Migration.

`cannot read the major version of the runner's pg_restore` fails as well, because we never read an unknown version as a compatible one.
It names the binary preflight ran, and a custom image whose `pg_restore` is missing there or prints an unexpected `--version` line produces it.
`cannot read the target major version for the pg_restore check` means the target's `server_version_num` query failed after the connectivity check had passed.

## Extension ownership failures

`must be owner of extension ...` in the worker log and `target extension ownership required` from preflight have the same meaning.
The target migration role lacks the privileges of a selected extension's owner.
The worker-side error has two causes with identical text.
If this error is the terminal cause in attempt 1, the Migration ends at once as `Failed` with reason `PermissionDenied`.

Read the `Command was:` line that follows the error in the worker log:

- `Command was: COMMENT ON EXTENSION ...` identifies comment restoration.
  The minimal spec `spec.clone: {}` can hit it, because `CREATE EXTENSION IF NOT EXISTS` keeps the target's existing owner.
  Without `dropIfExists`, set `clone.skip: [extensionComments]` to keep other comments, or `clone.noComments: true` to suppress all comments.
- `Command was: DROP EXTENSION IF EXISTS ...` identifies the clean phase that `clone.dropIfExists: true` enables.
  `IF EXISTS` suppresses a missing-object error, not an ownership check.
  A suppressed comment cannot fix this route, so use `clone.skip: [extensions]` or an authorised migration role.

The `selected extension ownership` preflight check reports the extension, its target owner, the migration role, and the remedy.
The migration role can own the extension, inherit the owner role's privileges, or be a superuser.
Database ownership and `clone.noOwner` do not satisfy an extension ownership check, and `superuserSecretRef` does not change extension ownership.
Correct a terminal preflight failure, then create a new Migration.
A skipped extension also bypasses the availability gate, so provide the target extensions yourself.

## Replication and follow

### `wal2json` is not installed on the source

The first follow attempt fails at slot creation with `could not access file "wal2json"`.
The spec sets `spec.follow.plugin: wal2json`, but the source does not have the plugin installed.
Preflight cannot detect this, because an output plugin has no catalog entry to query.

Install the wal2json package on the source, or stay on the built-in `pgoutput`.

### `wal2json` is not trusted by the source

The first follow attempt fails at slot creation with `library "wal2json" may not be used as an output plugin`.
The spec sets `spec.follow.plugin: wal2json`, but the source does not trust it.
Since the 2026-08-13 minor releases, PostgreSQL loads only the output plugins that `output_plugin_libraries` names.
That parameter defaults to `pgoutput, test_decoding`.

Add `wal2json` to `output_plugin_libraries` on the source, then reload the config without a restart.
Alternatively, stay on the built-in `pgoutput`.
See the [prerequisites](reference/prerequisites.md).

### permission denied for function pg_replication_origin_drop

The first follow attempt dies before any data moves, and the logs show `permission denied for function pg_replication_origin_drop`.
The target role misses EXECUTE on the `pg_replication_origin_*` functions.
The error surfaces in pgcopydb's setup cleanup, which runs first, and the attempt fails terminally as `PermissionDenied`.

Apply the grant block from the [prerequisites](reference/prerequisites.md), then create a new Migration.
Preflight catches this error, so someone revoked the grants after the check.

### Changes from the window are missing on the target

The live migration streams and reports completion, and `replayLSN` advanced normally, but changes from the window are missing on the target.
The target role cannot `SET session_replication_role`, and pgcopydb 0.18 swallows the failure.
It applies nothing, and it still reports progress.

Run `GRANT SET ON PARAMETER session_replication_role TO <role>` on PostgreSQL 15 and above.
On an older target, connect as a superuser instead.
Alternatively, set `superuserSecretRef` and let preflight apply the grant.
Check row counts before you trust any re-run.
Preflight probes this before the first attempt.

### permission denied to start WAL sender

`permission denied to start WAL sender` in the logs means the source role lacks the REPLICATION attribute.
Only a role with that attribute can start a write-ahead log (WAL) sender.

Run `ALTER ROLE <role> REPLICATION`, or set `superuserSecretRef`.
Preflight checks this attribute too.

### The stream keeps dropping

The stream keeps dropping, the source logs show terminated walsenders, and attempts burn during catchup or drain.
Or the follow stops advancing while the worker keeps running.
A low `wal_sender_timeout` on the source kills the logical walsender.
CloudNativePG (CNPG) defaults to 5s.
A `PreflightWarning` event names the value preflight read.

Raise `wal_sender_timeout` to 60s or more for the migration window.
On CNPG, set it in `spec.postgresql.parameters`.
On PostgreSQL 12 and later, you can instead append `options=-c%20wal_sender_timeout%3D60s` to the source URI query (after `?` or `&`), which raises it for the migration's connections only.
That works in a `uriSecretRef` URI and in a `secretRef` DB key holding a URI; keep the percent-encoding, because libpq rejects the plain form.
Runners older than `0.18.72.g2aa91e7` can also abort receive with `free(): double free detected in tcache 2` when the source closes the stream near endpos, for example with `wal_sender_timeout=5s`; the retry recovers, but each abort uses one attempt of `spec.backoffLimit` ([#362](https://github.com/ydixken/pgcopydb-operator/issues/362)).
Runners older than `0.18.74.gc682dce` abort receive with `free(): invalid pointer` or `double free or corruption` after `Failed to IDENTIFY_SYSTEM` when the source ends the walsender while receive reconnects; `0.18.74.gc682dce` exits receive with the source error instead, and that attempt still uses one retry ([#364](https://github.com/ydixken/pgcopydb-operator/issues/364)).
Runners older than `0.18.76.gf9b328b` send receive status only every 10 seconds, so below a 10s timeout the source can end every session before receive reports a flush position: the slot's `confirmed_flush_lsn` stops moving while the worker keeps running ([#369](https://github.com/ydixken/pgcopydb-operator/issues/369)).
`0.18.76.gf9b328b` sends status within half of `wal_sender_timeout`.

Attempts that fail in apply setup with `replication origin with ID 1 is already active for PID ...` have a target-side cause.
The previous attempt's apply backend is still on the target and holds the replication origin, typically because a proxy cut it while its commit waited for a synchronous standby.
The [retry gate](#retryheld-is-true) holds the next attempt while that backend remains, with reason `SessionsRemain`, so the attempt starts once the origin is free.

### `CaughtUp` stays False

`CaughtUp` stays False in three cases: the lag is above `follow.maxCatchupLag`, only one below-threshold sample has arrived, or no replication sample exists yet.
With one below-threshold sample the reason is `ConfirmingCatchUp`, and it clears on the next sample.

Check `status.replication.lagBytes`.
If heavy write traffic keeps the lag high, throttle the traffic or raise the threshold.

## Synchronous replication and retries

### Commits blocked on synchronous replication

`CommitsBlocked` is True with reason `SyncRepWait`, and a `CommitsBlocked` Warning event names the waiting sessions.
After 15 minutes the `PgcopydbMigrationCommitsBlocked` alert fires.
Two progress samples in a row found a target session of the migration role whose commit has waited 30 seconds or more in wait event `SyncRep`.
Under strict synchronous replication, such as Patroni `synchronous_mode_strict` or CloudNativePG `dataDurability: required`, a commit waits until a synchronous standby confirms it, and without a standby it keeps waiting.
While it waits, the session sends nothing.

Healthy standbys can raise the condition too.
After a bulk load, the first commits that wait for a standby can wait while it catches up on the load's WAL ([measured](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/measurements.md#catch-up-waits-followed-a-bulk-load-with-both-standbys-attached)).
Such a wait sets `CommitsBlocked` and emits its event once it lasts roughly 40 seconds while samples come every 5 seconds, or roughly 50 seconds once they come every 10 seconds after the table copy.
The next sample that finds no wait of 30 seconds or more clears it.

Check the target's standbys in `pg_stat_replication` on the primary, and its `synchronous_standby_names`.
The waiting commits finish on their own once a synchronous standby confirms them.
A proxy or load balancer with an idle timeout cuts a connection that sends nothing for that long, and the backend behind it keeps waiting on the target.
The attempt then fails, and the [retry gate](#retryheld-is-true) holds the next one until those backends are gone and a test commit returns.
With a 900-second proxy timeout, the proxy cuts the worker at about the time the alert fires, so the alert can fire just as the retry gate takes over.
It keeps firing while `RetryHeld` is True, because the rule reads both conditions on one 15-minute clock.

To keep the copy's commits from waiting for a standby, set [`spec.target.synchronousCommit: local`](configuration.md#synchronous-commit-on-the-target).
Apply still waits for one, because a lost apply commit would lose source changes for good.

When a sample cannot read the count, the operator logs `progress sample lost the SyncRep count; until it answers again, CommitsBlocked stays True or turns Unknown`.
A True condition stands until a sample shows no wait of 30 seconds or more, or the worker succeeds.
No sampler runs between attempts, while suspended, or after the Migration ends, so the condition keeps its last value there; the alert skips `Failed`, `Completed`, and `Suspended` for that reason.

### `RetryHeld` is True

`RetryHeld` is True, and no new attempt starts.
The [retry gate](operations/lifecycle.md#the-retry-gate) holds the next attempt, and the reason names the first check that did not pass.
The gate probes again every 30 seconds and starts the attempt once a probe passes.
A hold spends no attempt, and the operator never ends, cancels, or terminates a session.
A `RetryHeld` Warning event records the first result of a hold and each change of reason.
`kubectl logs job/<name>-gate-<n>` shows the probe's `gate:` result lines; the operator keeps a failed probe Job for 30 seconds.
To stop a hold, set `spec.suspend: true`, which deletes the probe Job; the resume runs the gate again.

`GateProbing` means the hold has no probe result yet: the probe Job is pending, running, or not created yet.
Every retry passes through it, within seconds when the failed worker's log was readable.
After a worker pod that vanished it can last several minutes, because the operator first waits up to 2 minutes for the log and then requeues with backoff before it creates `<name>-gate-<n>`.
A probe pod that cannot start turns it into `ProbeFailed` with the kubelet reason.

`WorkerPodRemains` means a pod of the previous attempt's worker has not ended; the message names the pod and its node.
A pod on a lost node holds the retry until the pod object is gone.

> [!warning]
> Force-delete the pod only once you have confirmed that its node is down.
> On a node that is only unreachable, the old worker can still write to the target.

To remove the pod of a node that is down:

```sh
kubectl delete pod <pod> --grace-period=0 --force
```

`SessionsRemain` means the probe's census found sessions of the migration role on the target under pgcopydb's name.
The message lists up to ten, with pid, state, wait event, age, backend start (`started=`), and name.
On a direct connection, a session that started before the failed attempt is not its leftover: it belongs to another workload under the same role, which the [prerequisites](reference/prerequisites.md#retries-and-snapshot-consistency) rule out.
List the sessions yourself on the target, connected as the migration role, a superuser, or a member of `pg_read_all_stats`:

```sql
SELECT pid, state, wait_event, backend_start,
       clock_timestamp() - coalesce(query_start, backend_start) AS age,
       application_name
  FROM pg_stat_activity
 WHERE usename = '<migration role>'
   AND datname = '<target database>'
   AND application_name LIKE 'pgcopydb[%'
 ORDER BY coalesce(query_start, backend_start);
```

The query only reads `pg_stat_activity`.
Drop the `datname` line for an all-databases Migration.
Drop the `application_name` line when the target URI sets `application_name`, because the gate then counts every session of the role, and its message says so.
A session waiting in `SyncRep` ends once a synchronous standby confirms its commit; see [Commits blocked on synchronous replication](#commits-blocked-on-synchronous-replication).
A session busy in a long statement stays until the statement ends.
A session in state `idle` holds no transaction, so ending it by hand with `SELECT pg_terminate_backend(<pid>);` is safe.
We count idle sessions too, because an idle backend can still hold apply's replication origin, and neither `pg_locks` nor `pg_stat_activity` shows that.

Preflight fails a target behind PgBouncer, so a pooler holds a retry here only when preflight did not see it, for example one that joined the target path later.
Through a transaction-mode pooler, idle server connections keep the name of the last client they served, so connections that last served pgcopydb hold the retry until the pooler closes them.
PgBouncer does that after `server_idle_timeout`, 600 seconds by default, or after `server_lifetime`, 3600 seconds by default, when the idle timeout is off.
When every listed session is `idle`, the message adds that a pooler can keep them and that ending them is safe.

`SyncCommitNotLocal` means that with `spec.target.synchronousCommit: local`, the probe's target connection reports another `synchronous_commit`.
A pooler or proxy on the way dropped the startup option.
The preflight checks the same, so either the path changed since, or `synchronousCommit` was set after the preflight ran.
Unset `synchronousCommit`, which the next probe picks up, or connect the target directly as the [preflight entry](#target-synchronous_commit-is-not-local) describes.
The target's endpoint fields are immutable, so connecting directly takes a new Migration.

`CommitBlocked` means the test commit, run with `synchronous_commit=on`, waited for a synchronous standby until the probe cancelled it after 5 seconds.
The next worker's commits would wait the same way.
It also appears when the probe skipped the test commit because a session, or an earlier probe's commit, still waits in `SyncRep`.
We skip it then, so that the gate never adds waiting commits of its own.
Restore a synchronous standby; the gate passes once a probe after that also finds no sessions left.
A cancelled test commit is committed locally, and it only created a temporary table that its own commit dropped.

`ProbeFailed` means a check gave no result: a failed connection, an SQL error, a deadline, a probe pod that cannot start, an unreadable log, or a log without the expected lines.
The message carries the server's error: a failed connection shows the server's `FATAL:` line, such as a failed password or an unknown database.
Any other client-side error, such as a host name that does not resolve, reads `client-side error withheld`, because libpq can echo a connection URI with its password; check the target connection settings.
When the test commit fails for lack of `TEMPORARY` on the target database, the message includes the `GRANT TEMPORARY ON DATABASE` statement that fixes it.

## Publication retry failures

A retry can fail with `publication ... already exists` or `publication retry refused`.
Check the publication and the resume state on the source.

For `pgoutput` with an automatic publication, a retry preserves the publication whenever its source slot exists.
The operator drops only an orphan that has no source slot, and pgcopydb then retries the incomplete setup.
An explicit `spec.follow.publication` value bypasses this guard, and so does any plugin other than `pgoutput`, such as `wal2json` and `test_decoding`.
A retry that [starts over](#phase-failed-with-reason-snapshotlost) runs `pgcopydb stream cleanup` before the guard, so it finds neither the slot nor the automatic publication.

The bundled runner also repairs an interrupted setup when the retry creates a fresh slot and the work catalog has no sentinel row.
This bootstrap recovery is not in every runner version; see [client tool versions](reference/prerequisites.md#client-tool-versions).
If a retained slot has missing or unreadable sentinel state, the setup fails closed and does not rebuild established progress.

`publication retry refused: source slot "..." exists but auto publication "..." is missing` means the source state is inconsistent.
The attempt fails before pgcopydb starts, and a catalog-query or publication-drop error stops it too.
Read the worker log, then check `pg_replication_slots` and `pg_publication` on the source.
A `publication ... already exists` error that remains can show an interrupted setup beyond the guard's recovery window, and not a user-owned publication.

> [!warning]
> Do not drop an established publication to force a retry or assume that recreating a missing one recovers the stream.
> Keep applications on the source, resolve the cause, and use a fresh Migration when resume state cannot be trusted.
> The retained slot can continue accumulating WAL until cleanup.

## Clone progress

### The worker pod runs but the phase never moves

The Migration sits in `Cloning` or `CuttingOver` with `attempts: 1` and never moves.
The worker pod runs and still logs log sequence number (LSN) positions.
Earlier in its log, pid 1 said FATAL `Terminating all processes in our process group`.
This is a pgcopydb 0.18 zombie: a child process survives and keeps the pod alive, so the Job never fails.

The operator detects this and recovers on its own.
It fires a `WorkerZombie` warning event, deletes the pod, and the normal retry starts the next attempt.
On an operator version without that recovery, delete the worker pod by hand.

### `Finalizing` takes a long time

The Migration sits in `Finalizing` for a long time, the target database size is flat, and the worker pod is still running.
The end of a clone narrows to a single `VACUUM ANALYZE` on the largest table, and the target stops growing during it.
The pass is healthy while the worker pod log advances; a stopped log is a stall.

Check `CommitsBlocked` first.
When it is True, the tail waits for a synchronous standby to confirm its commits; see [Commits blocked on synchronous replication](#commits-blocked-on-synchronous-replication).
Otherwise watch the worker pod log instead of the target size.
If the tail is too long, set `skip: [vacuum]`, which leaves the target without statistics.
Then run `ANALYZE` yourself before you send traffic.
See [The VACUUM tail](operations/performance.md#the-vacuum-tail) and the [phase table](reference/conditions.md#phases).

### `Streaming` and `CaughtUp` stay unset during the clone

The live migration stays in `Cloning` with `Streaming` and `CaughtUp` unset.
Meanwhile the worker log shows healthy `STEP` progress, and `status.replication` fills in.
The operator samples the stream on every pass, but it does not act on that sample until the copy is done.
`Streaming`, `CaughtUp`, and cutover all wait for `CloneCompleted`.
The copy-phase status comes from the worker log and a psql size sample, because a read of pgcopydb's own catalogs kills workers.
The pass is healthy while the `STEP` lines advance.

Wait for the worker to print `All step are now done`.
`CloneCompleted` then goes True on the next pass, and the stream conditions engage.

### Progress stops moving while a large table copies

`status.progress` and the target size gauge stand still for minutes while the worker log shows a large table copying.
The copy itself is unaffected.

Operator v0.16.0 and earlier read the table being copied.
The sample probed it for a row and read every uncommitted page of the open copy, or it waited on the exclusive lock of a table copied whole.
Either ran the target query past its 5-second timeout, and status kept its last figures, without a log line, until a copy into the table committed.
Later versions skip the tables a copy worker holds locked on any target, and skip every open copy on a PostgreSQL 14 or later target.
They log `progress sample lost a side` with the server's error when a sample side fails.
`status.progress.observedAt` shows how old the figures are: it stops while samples are lost and moves again with the next one that answers.
On an older target the probe still reads an open part, and that log line shows the statement timeout.

Upgrade the operator, and on a target before PostgreSQL 14 also shrink the parts.
Raise `spec.clone.splitMaxParts` or lower `spec.clone.splitTablesLargerThan`, so each copy is a smaller part that commits sooner.
See [Same-table concurrency](operations/performance.md#same-table-concurrency).

### `CloneCompleted` is False with reason `TablesEmptyOnTarget`

The live migration stays in `Cloning`, and `CloneCompleted` is `False` with reason `TablesEmptyOnTarget`.
Meanwhile the worker log shows the base copy finished and the stream is applying.
The operator's sample found tables that hold rows on the source and none on the target, and the condition message names them.
It therefore does not trust the worker's clone-completion line; see [#277](https://github.com/ydixken/pgcopydb-operator/issues/277).
The sample tests whether each table holds a row, not how many, because the source runs ahead of the copy's snapshot until the stream catches up.
A table that got its first source rows after that snapshot reads this way until the stream delivers one of them.

Check when the named tables got their rows on the source.
Rows written after the base copy's snapshot arrive through the stream, and the condition clears on the first sample that finds one landed.
Rows that predate the snapshot are missing from the copy: delete the Migration and run a fresh one.
pgcopydb does not re-copy a table that its catalog calls done.

### Phase `Failed` with reason `CloneIncomplete`

Phase `Failed` with reason `CloneIncomplete` means a clone-only worker exited 0, but pgcopydb's own catalog counted tables not done.

Do not use the target as a complete copy.
Read `status.progress` and the worker log, then run a fresh Migration.

## Cutover and verification

### The verify pod runs for a long time

The Migration sits in `CuttingOver` with the `<name>-verify` pod running for a long time.
The target's origin lands exactly on the cutover LSN only when nothing wrote on the source between the last applied commit and the approval.
Nearly every cutover therefore proves the drain by content instead.
`pgcopydb compare data` checksums every migrated table, and the database size, not the remaining lag, sets how long that scan takes.
The Job logs `endpos`, `replay_lsn`, and `origin_progress` before the compare starts, so its log names the path it took.

Wait for the scan, and plan the write-downtime window around it.
A verify Job that fails runs once more before the Migration fails, so a refusal pays for the scan twice.

### Phase `Failed` with reason `DrainIncomplete`

Phase `Failed` with reason `DrainIncomplete` means the verify Job found the target missing changes below the cutover LSN.
`pgcopydb compare data` reported differing tables; the typical cause is a crash inside the drain window, after which pgcopydb `--resume` exits 0 without replaying.
With a runner older than `0.18.98.g944abcc`, apply that fell behind receive across an `output.db` rotation while streaming can also have skipped committed changes ([#376](https://github.com/ydixken/pgcopydb-operator/issues/376)).

Do NOT switch applications to the target.
No data is lost at the source: the slot is kept and retains WAL.
The verify Job logs name the differing tables and print `endpos`, `replay_lsn` (below `endpos` means the stream was never consumed to the cutover LSN), and `origin_progress`.
The simplest recovery is to keep running on the source, delete the Migration so cleanup drops the slot, and run a fresh live migration.

### Phase `CutoverPending` and nothing happens

Phase `CutoverPending` means manual mode waits for `spec.cutover.approved: true`.

Stop the writes on the source, then set `spec.cutover.approved: true`.

## Ownership handover failures

Phase `Failed` with reason `OwnershipFailed` means the `<name>-reown` Job could not hand the restored objects to `clone.ownerAfterRestore`.
The role does not exist on the target, or a `CREATE` grant or an inherited-membership grant is missing.
A lock wait can also time out on all three pod attempts.
The log prints the exact `GRANT` that is missing.

The data is on the target, and only some or all of the `ALTER ... OWNER TO` statements are missing.
Finish the handover by hand: `Failed` is absorbing, so a new Migration would re-run the whole clone.

1. Read the handover Job's log: `kubectl logs job/<migration>-reown`.
   It names the pre-check that refused, if any.
   Otherwise it lists the first 200 statements it generated and stops at the first one that failed.
2. Apply what the log names.
   Create the missing role on the target, or run the `GRANT` it printed verbatim.
   The pre-checks cover `CREATE` on the database, `CREATE` on the schemas that stay behind, and inherited membership in the new owner.
   They all run before any `ALTER`, so `permission denied for schema <schema>` mid-handover means someone revoked a grant that passed preflight.
   All three cases are in [Ownership after restore](reference/prerequisites.md#ownership-after-restore-cloneownerafterrestore).
3. Replay the `ALTER ... OWNER TO` statements from the log against the target, connected as the migration role.
   Each one commits on its own, so a replay is safe: `ALTER ... OWNER TO` does nothing when the object already has that owner.
4. Check the target with the query below, connected as the migration role.
   It MUST come back empty.
5. On a live migration, delete the Migration once the handover is done.
   The finalizer runs the cleanup Job, which drops the replication slot and ends the WAL retention on the source.
   A terminal failure does not release the slot on its own.

The query is the set the Job itself re-checks before it reports success:

```sql
WITH me AS (
  SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user
),
ext_member AS (
  SELECT classid, objid FROM pg_catalog.pg_depend WHERE deptype = 'e'
),
user_schema AS (
  SELECT oid, nspname, nspowner FROM pg_catalog.pg_namespace
   WHERE nspname !~ '^pg_' AND nspname <> 'information_schema'
)
SELECT 'schema' AS kind, n.nspname AS name
  FROM user_schema n, me
 WHERE n.nspowner = me.oid AND n.oid >= 16384
   AND NOT EXISTS (SELECT 1 FROM ext_member e
                    WHERE e.classid = 'pg_catalog.pg_namespace'::regclass AND e.objid = n.oid)
UNION ALL
SELECT 'relation', c.oid::regclass::text
  FROM pg_catalog.pg_class c JOIN user_schema n ON n.oid = c.relnamespace, me
 WHERE c.relowner = me.oid AND c.oid >= 16384
   AND c.relkind IN ('r', 'p', 'S', 'v', 'm', 'f')
   AND NOT EXISTS (SELECT 1 FROM ext_member e
                    WHERE e.classid = 'pg_catalog.pg_class'::regclass AND e.objid = c.oid)
UNION ALL
SELECT 'routine', p.oid::regprocedure::text
  FROM pg_catalog.pg_proc p JOIN user_schema n ON n.oid = p.pronamespace, me
 WHERE p.proowner = me.oid AND p.oid >= 16384
   AND NOT EXISTS (SELECT 1 FROM ext_member e
                    WHERE e.classid = 'pg_catalog.pg_proc'::regclass AND e.objid = p.oid)
UNION ALL
SELECT 'type', t.oid::regtype::text
  FROM pg_catalog.pg_type t JOIN user_schema n ON n.oid = t.typnamespace, me
 WHERE t.typowner = me.oid AND t.oid >= 16384
   AND NOT EXISTS (SELECT 1 FROM ext_member e
                    WHERE e.classid = 'pg_catalog.pg_type'::regclass AND e.objid = t.oid)
ORDER BY 1, 2;
```

Every row it returns is still owned by the migration role.
Run it before step 3 too, to see what is outstanding.
An array type and a row type show up beside the type or relation they belong to, and they need no statement of their own.

A `canceling statement due to lock timeout` in the log is not a privilege problem.
The Job caps each `ALTER` and its lock wait at 60 seconds, and a session on the target holds a conflicting lock.
It sets both caps with `SET LOCAL` inside that statement's own transaction.
Clear that session, then replay.

## Workers and cluster objects

### exists but belongs to another owner

The Migration sits at attempt 1 with the event `... exists but belongs to another owner`.
The work PVC or ConfigMap of a just-deleted Migration with the same name still waits for garbage collection.

Wait for garbage collection to finish, or delete the leftover objects.

### `pg_dump` reports a server version mismatch

`pg_dump: error: server version mismatch` in the logs means the client tools in the runner image are older than a server major.
`pg_dump` must be at least the newest major on either side.

If you pinned `spec.runner.image`, point it at an image with matching client tools.
The default runner ships PostgreSQL 18 client tools.

### No worker pod and the PVC stays Pending

No worker pod appears and the PVC stays `Pending` when no StorageClass can provision the work volume.

Set `spec.workVolume.storageClassName`, or fix the cluster default.

### Phase `Failed` with retry budget exhausted

Phase `Failed` with "retry budget exhausted" means the counted attempts reached `backoffLimit + 1`, each failing on a cause the operator cannot classify as deterministic.
The message reads `retry budget exhausted after N attempts (M exempt)`, followed by `: attempt K failed:` and that attempt's error when a failed attempt spent the last of the budget.
Exempt attempts are those the retry gate found cut during a synchronous standby outage, and they do not count, so `N` can exceed `backoffLimit + 1`; see [Exempt attempts](operations/lifecycle.md#exempt-attempts).
The gate decides at its first census after the failure.
When no probe gets a census within 15 minutes of the failure, for example because it cannot connect, the attempt counts, and with the budget spent the Migration fails then.
The first attempt's logs almost always name the cause.
A permission error mostly stops early as `PermissionDenied`, a full work volume as `DiskFull`, and a change naming a column the target lacks as `SchemaDrift`.
One that sits outside the last 40 log lines the classifier reads lands here.
A work volume that filled up then shows `database or disk is full` or `No space left on device` in the first attempt's logs.
A live migration that kept starting over shows a `SnapshotLost` event for each start-over.
`the pod log was not readable (...)` in the message means the operator could not read the worker log for two minutes after the Job failed, so it could not classify the cause.
The text in parentheses is the read error.
Read the cause with `kubectl logs job/<name>-run-<attempt>` while the Job still exists.

Fix the cause, then create a new Migration.
A terminal state is absorbing, so this Migration cannot resume.

### Phase `Failed` with reason `PermissionDenied`

Phase `Failed` with reason `PermissionDenied` after a single attempt means the worker hit a permission error that no retry can fix.
The log holds `permission denied`, `must be owner of extension`, or SQLSTATE 42501.
The condition message carries the matched line.

Grant what the message names, then create a new Migration.
A missing source-side SELECT or USAGE is the usual cause, because preflight covers the target CREATE rights.
For the grantable target rights, set `superuserSecretRef`.

### Phase `Failed` with reason `DiskFull`

Phase `Failed` with reason `DiskFull` after a single attempt means the work volume is full.
The condition message carries the matched line.
`database or disk is full` comes from pgcopydb's SQLite catalogs, and `No space left on device` from a file pgcopydb or pg_dump writes under the work directory.

Enlarge `spec.workVolume.size`, then create a new Migration.
A terminal state is absorbing (see [Retries and resume](operations/lifecycle.md#retries-and-resume)), so this Migration cannot resume.
On a live migration the change spool grows with source writes until cutover, so size it for the writes the migration has to carry.

A target that runs out of disk (`could not extend file`) or shared memory (`could not resize shared memory segment`) is not `DiskFull`.
Those attempts keep normal retries, because space or load on the target can change between attempts.

### Phase `Failed` with reason `SchemaDrift`

Phase `Failed` with reason `SchemaDrift` after a single attempt means the apply hit a change that names a column the target lacks.
Usually DDL ran on the source during the migration window, and DDL is not replicated.
A `DROP COLUMN` or `RENAME COLUMN` on the target gives the same error.
The condition message carries the matched line, `[TARGET <pid>] [42703] ERROR:  column ... does not exist`.

Create a new Migration: it copies the schema from the source again, so the target needs no manual DDL.
A terminal state is absorbing (see [Retries and resume](operations/lifecycle.md#retries-and-resume)), so this Migration cannot resume.
Run no DDL on either side until the cutover, as the [live migration prerequisites](reference/prerequisites.md#live-migration-specfollowenabled-true) require.

The operator classifies only SQLSTATE 42703 on the apply's target connection.
A missing table (42P01) or any other schema error keeps normal retries and ends as `BackoffLimitExceeded`.

### Phase `Failed` with reason `SnapshotLost`

Phase `Failed` with reason `SnapshotLost` means a live migration's retry could not resume its table copy, and `spec.clone.dropIfExists` is false.
The replication slot outlived the attempt that created it, and its snapshot did not.
Tables copied without that snapshot would hold rows the slot then replays again, so pgcopydb refused the resume.
The condition message carries its `Refusing to resume the copy: ...` line, which names the slot, its snapshot and LSN, and how many tables are still to copy.

Starting over needs `--drop-if-exists`, because the target holds the objects and rows of the earlier attempts.
Delete the failed Migration first: a terminal failure keeps the slot, which retains WAL on the source, and deletion drops it.
Then create a new Migration with `spec.clone.dropIfExists: true`, or one against an empty target.
Keep that order when the new Migration reuses a fixed `spec.follow.slotName`: the failed Migration's cleanup drops the slot and origin of that name, which would then be the new Migration's.

With `dropIfExists: true` the operator starts over on its own and emits a `SnapshotLost` warning event.
That attempt runs `pgcopydb stream cleanup` to drop the slot, the automatic publication, and the target origin, then `pgcopydb clone --follow --restart --drop-if-exists`.

### Status updates lag behind the worker

The worker makes progress, but `status` updates later than the 10-second poll interval, or the size gauges later than their 5-second sample interval during a copy.

A reconcile pass that runs longer than the poll interval logs `active worker observation` at the info level, a sanitized duration summary of the operations it reached.
Set `--zap-log-level=debug` on the manager to see the same summary for every pass.
The summary records timing only: no credentials, no SQL, and no raw command output.
It shows where a slow pass spent its time.
It does not establish the cause of a past delay, and it does not guarantee a wall-clock poll interval.
The size gauges come from a sampler that does not wait for the pass; a sample that outruns its own interval logs `progress sample took longer than its interval`.

## Metrics and dashboards

### Scrapes return 401

The Prometheus target for the operator is down, and scrapes return 401 because the scrape carried no credential.
The manager refuses an anonymous caller before it checks any access rights.

Send a token.
The chart's ServiceMonitor sets `bearerTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token`, and a hand-written ServiceMonitor needs the same line.

### Scrapes return 403

The Prometheus target for the operator is down, and scrapes return 403.
The manager identified the scraper, but the scraper has no `get` on the `/metrics` nonResourceURL.
That permission belongs to the scraper's own role-based access control (RBAC), so the chart cannot grant it.

Bind a ClusterRole with `nonResourceURLs: ["/metrics"], verbs: ["get"]` to the ServiceAccount that Prometheus runs as.
kube-prometheus-stack already binds one to its own Prometheus.

### Scrapes return 500

Scrapes return 500, and the manager logs `Authentication failed`.
The manager cannot create TokenReviews, so it cannot check the caller's token at all.

Grant its ServiceAccount `create` on `tokenreviews` (authentication.k8s.io) and `subjectaccessreviews` (authorization.k8s.io).
The chart renders both whenever `rbac.create` and `metrics.enabled` are on.
This failure therefore means `rbac.create=false` and RBAC that you supply yourself.

### Progress metrics stop at the estimate

`status.progress` and the `_tables_*`/`_indexes_*`/`_clone_*_bytes` metrics stop at the estimate.
They move during the copy, but pgcopydb's exact count never replaces them, at neither the clone's completion nor the drain verification.
An exact runner-version allowlist gates the progress poll, and this runner's pgcopydb is not on it.
The flag is `--progress-poll-versions`, and the chart value is `runner.progressPollVersions`.
For stock pgcopydb 0.18 the gate stays closed on purpose: its `list progress` returns no data (`no such column: bytes`).
Against a filtered work dir, it also corrupts the stored filters and kills the clone.

Use the bundled runner, whose patched pgcopydb is on the allowlist by default.
Extend the allowlist only for a runner image whose pgcopydb carries the upstream fixes.
The database-size metrics, the phase metrics, and the counters themselves flow on any runner.

### Dashboards never appear in Grafana

Dashboards installed with `grafana.dashboards.enabled=true` never show up in Grafana.
The sidecar watches ConfigMaps only in Grafana's own namespace and in the namespaces its `searchNamespace` names.
The chart rendered the dashboards into the release namespace instead.

Set `grafana.dashboards.namespace` to Grafana's namespace, or widen the sidecar's `searchNamespace`.
See [Monitoring](operations/monitoring.md).

### PrometheusRule alerts never appear in Prometheus

A PrometheusRule installed with `metrics.prometheusRule.enabled=true` never shows its alerts in Prometheus.
Prometheus selects rules by label, and kube-prometheus-stack matches its release label by default.
The rule object carries none of the selected labels.

Add the matching label with `metrics.prometheusRule.additionalLabels`.
For kube-prometheus-stack that label is `release: <its release name>`.
Alternatively, widen the Prometheus `ruleSelector`.
