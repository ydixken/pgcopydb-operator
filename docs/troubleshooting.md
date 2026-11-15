# Troubleshooting

First, run `kubectl describe migration <name>`.
Read the conditions and the events.
Then read the worker pod logs with `kubectl logs job/<name>-run-<N>`.
The worker pod writes these logs as structured JSON.
The `Failed` condition carries the last pgcopydb ERROR from the failed pod, after Kubernetes' own "Job has reached the specified backoff limit".
When the pod is already gone, the condition carries only the Job message.

| Symptom | Section |
|---|---|
| [`describe` shows no phase, no conditions, no events](#migration-has-no-status-and-the-log-repeats-is-immutable) | Spec and admission |
| [An update fails with `<field> is immutable`, for `dryRun`, `allDatabases`, `deferIndexes`, `ownerAfterRestore`, `source`, `target` or `follow`](#an-update-fails-with-field-is-immutable) | Spec and admission |
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
| [`Finalizing` runs long and the worker pod is still running](#finalizing-takes-a-long-time) | Clone progress |
| [`Streaming` and `CaughtUp` stay unset during the clone](#streaming-and-caughtup-stay-unset-during-the-clone) | Clone progress |
| [`status.progress` stands still while a large table copies](#progress-stops-moving-while-a-large-table-copies) | Clone progress |
| [`CloneCompleted` is `False` with reason `TablesEmptyOnTarget`](#clonecompleted-is-false-with-reason-tablesemptyontarget) | Clone progress |
| [Phase `Failed`, reason `CloneIncomplete`](#phase-failed-with-reason-cloneincomplete) | Clone progress |
| [`CutoverCompleted` is Unknown with reason `RehearsalPodRemains`](#cutovercompleted-is-unknown-with-reason-rehearsalpodremains) | Cutover and verification |
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

The spec gives an explicit zero value to an optional field of an immutable block.
The immutable blocks are `follow`, `source`, and `target`, apart from `target.synchronousCommit`.
The typical case is `allowMissingReplicaIdentity: []` from a template that renders an empty list.
Operators up to v0.8.1 rejected that spelling before they could set any status.

Upgrade the operator to a version after v0.8.1.
Both spellings work in those versions.
On an older version, omit the field or set it to `null` instead of `[]`, `""`, or `false`.

### An update fails with `<field> is immutable`

Admission rejects the changes below to an existing Migration, each with its message:

- `dryRun is immutable: create a separate Migration for the real run`: a dry run stops before the first worker attempt.
  Turning it on mid-run would therefore stop the migration between attempts.
  Turning it off after completion would change nothing.
- `allDatabases is immutable: create a separate Migration`: retries and compares reuse the work directory, which has the layout of that mode.
  Dropping the field or the whole `clone` block from a Migration created with `allDatabases: true` is such a change.
- `clone.deferIndexes is immutable: create a separate Migration`: preflight tests the runner for `--defer-indexes` only before the first attempt.
  Every attempt therefore keeps the mode that preflight saw.
  Dropping the field or the whole `clone` block from a Migration created with `deferIndexes: true` is such a change.
- `ownerAfterRestore is immutable: the preflight probes the target role before the first attempt, and the handover Job is built once`.
- `source is immutable`, `target is immutable except synchronousCommit` or `follow is immutable`: admission rejects any change to an endpoint, apart from `target.synchronousCommit`.
  It also rejects any change inside a `follow` block that the Migration already has.
  If the Migration also shows no status at all, see [Migration has no status and the log repeats `is immutable`](#migration-has-no-status-and-the-log-repeats-is-immutable).

Create a new Migration with the spec you want.

## Preflight failures

### Phase `Failed` at `Validating`

Phase `Failed` at `Validating` with reason `PreflightFailed` means that one preflight test failed.
The possible causes are:

- connectivity
- PgBouncer in a connection
- the target's `synchronous_commit`
- the major version gate
- the runner's `pg_restore` against the target major
- selected extension availability or ownership
- a clone privilege
- a source table audit
- an all-databases probe
- a follow prerequisite

Do the recovery action that the condition gives.
Then create a new Migration.
No worker attempt has started.

### All-databases superuser requirement

`PreflightFailed` names the all-databases superuser requirement when a migration connection lacks `rolsuper`.

Use superuser migration credentials on both sides.
Then recreate the Migration.
`superuserSecretRef` and managed admin roles without `rolsuper` do not satisfy [this contract](reference/prerequisites.md#all-databases).

### Unavailable selected extensions

`PreflightFailed` names unavailable selected extensions when the target lacks both an installed extension and its default package.
It also names them when `dropIfExists` or `allDatabases` needs a missing default version.

Install the missing extension package on the target, or choose a target that provides it.
Then create a new Migration.
Preflight does not install extensions.
The source and target versions do not have to match.

### Preflight pod cannot start

The Migration stays in `Validating`, and `Validated` is `Unknown` with reason `PreflightRunning`.
The preflight pod cannot start.
The condition message carries the kubelet reason verbatim: `CreateContainerConfigError`, `Unschedulable`, or a pull error.
A misnamed credentials Secret is the typical cause.

Fix the object that the message names.
The gate then resumes on its own.
The preflight Job has a limit of 30 minutes.
After that time, the Migration fails.

### Preflight deadline exceeded

`PreflightFailed` with `DeadlineExceeded` on the Job means that a test hung past the 30-minute preflight deadline, or that the pod never started in time.
Each connection attempt times out after 10 seconds, and preflight retries it six times.
The condition carries the server's line for a permanent error, such as a wrong password, an unknown role, or an unknown database.

Read the condition message and the preflight Job logs to find the last test that ran.
Fix the endpoint or the object that it names.
Then create a new Migration.

### Preflight output not readable

`PreflightFailed` with `the check output was not readable (...)` means that the operator could not read the log of the preflight pod.
The text in parentheses is the read error, such as an API server timeout or a pod that no longer exists.
The operator retries the read for two minutes after the Job fails.
Until then, the Migration stays in `Validating`.
`Job <name>-preflight printed no check output` is different: the read worked and the log was empty.
In that case, examine the events of the pod.

Read the verdict with `kubectl logs job/<name>-preflight`.
Fix what it names.
Then create a new Migration.

### `superuserSecretRef` cannot apply the grants

The preflight log shows `warn: ... lacks rolsuper` and later `applying ... via superuserSecretRef failed`.
The `superuserSecretRef` role is not a superuser, and it does not have the rights to apply the grants.
A missing `rolsuper` alone is only a `PreflightWarning` event, because managed admin roles work without it.

Point the Secret at a role that can run the printed statements: a superuser, or the provider's admin role.
If no such role is available, drop `superuserSecretRef`.
Then apply the grants by hand.

### A grant is still missing after remediation

`PreflightFailed` with `still lacks ... after remediation` means that preflight applied a grant, but the grant had no effect.
On a target older than PostgreSQL 15, the `session_replication_role` GRANT does not exist.

Find the major version of the target.
If it is older than PostgreSQL 15, connect the migration as a superuser-capable role.

### No replica identity usable for UPDATE/DELETE

`PreflightFailed` with `no replica identity usable for UPDATE/DELETE` and a table list names the tables at risk.
After a publication includes such a table, an UPDATE or a DELETE on it fails on the source.
The audit covers all user tables.
`clone.filters` does not exclude a table from the audit.

Give each table a primary key, `REPLICA IDENTITY USING INDEX`, or `REPLICA IDENTITY FULL`.
If a table is read-only or insert-only, acknowledge it in `spec.follow.allowMissingReplicaIdentity` in a new Migration.
The value `["*"]` acknowledges all of them.
Acknowledgement does not suppress UPDATE or DELETE.
For deliberate INSERT-only replication, use [`spec.follow.insertOnlyTables`](configuration.md#partial-table-policies).
The target then retains rows and values that later DELETE and UPDATE operations change on the source.

### Row-level security hides rows

`PreflightFailed` with `row-level security hides rows of these tables` names tables whose policies apply to the source migration role.
The bundled runner would fail the copy on them with SQLSTATE 42501.
A runner before `0.18.34.g7fddd6f` would copy only the rows that the role can see.
Its comparison would agree, so the loss would go unnoticed.

Give the role `BYPASSRLS`, disable row-level security on those tables for the migration, or exclude the tables with `clone.filters`.
Then create a new Migration.
See [Row-level security](reference/prerequisites.md#row-level-security) for when policies apply and how the audit scopes tables.

### Follow cannot replicate unlogged tables

`PreflightFailed` with `follow cannot replicate unlogged tables` names unlogged tables in a follow Migration's scope.
Changes to these tables never reach the WAL.
So pgcopydb cannot decode them, and its publication refuses them.

Run `ALTER TABLE ... SET LOGGED` on the source, or exclude the tables with `excludeTables` or `excludeSchemas`.
The list can name tables outside `includeOnlySchemas`.
Without `includeOnlyTables`, pgcopydb still publishes those tables, so exclude them with `excludeTables`.
Then create a new Migration.

For an empty table or an initial copy without follow, use [`spec.follow.unloggedTables`](configuration.md#partial-table-policies).
Each table needs an explicit `SchemaOnly` or `CopyOnce` mode.
The operator reports these partial policies separately from full content verification.

### A connection goes through PgBouncer

`PreflightFailed` with `the <side> connection goes through a connection pooler such as PgBouncer (the server's backend PID is <n>, the connection reported <m>)` means that a connection pooler, such as PgBouncer, sits between the runner and PostgreSQL on that side.
On one session, the test compares the backend PID in the `\conninfo` output of psql 18 with `pg_backend_pid()`.
PgBouncer issues a cancel key of its own, so the two values differ in every pool mode.
A direct connection or a TCP proxy forwards the server's key.
pgpool-II also forwards it, so the test does not detect pgpool-II.

`cannot tell whether the <side> connection goes through a connection pooler such as PgBouncer` also fails, because we never read a missing answer as a direct connection.
The text after it names the reason: the probe connection failed, psql printed no `Backend PID` in `\conninfo`, or `select pg_backend_pid()` returned no PID.
A custom runner image with psql older than 18 prints no `Backend PID`.

Point that side of the Migration at PostgreSQL directly, for example at the database's primary Service instead of the pooler's.
With a custom `spec.runner.image`, use one that ships psql 18 or later.
Then create a new Migration.

### Target `synchronous_commit` is not local

`PreflightFailed` with `spec.target.synchronousCommit is local but the target connection reports synchronous_commit=<value>` means that the libpq startup option did not reach the server.
A connection pooler or a proxy on the way dropped it.
A CloudNativePG Pooler does this, and so does a PgBouncer with `options` in `ignore_startup_parameters`.
Listing `synchronous_commit` in PgBouncer's `track_extra_parameters` does not change that.
In session mode, PgBouncer 1.25.2 accepts the option at connect, but it never sends the option to the server.
Behind PgBouncer, the [PgBouncer test](#a-connection-goes-through-pgbouncer) also fails.
A PgBouncer that refuses the option fails the connectivity test instead.
That failure message names `spec.target.synchronousCommit`.

Connect the target directly, or leave `synchronousCommit` unset.
Then create a new Migration.
See [Synchronous commit on the target](configuration.md#synchronous-commit-on-the-target).

### The runner's `pg_restore` cannot restore into the target

`PreflightFailed` with `the runner's pg_restore <n> cannot restore into the target's PostgreSQL <m>` means that the runner's `pg_restore` is version 17 or later, and that the target runs PostgreSQL 16 or older.
`pg_restore` 17 and later send `SET transaction_timeout = 0`.
Those servers reject it with `unrecognized configuration parameter "transaction_timeout"`, so every worker attempt would fail at the pre-data restore.
The default runner ships `pg_restore` 18.

Use a target that runs PostgreSQL 17 or later.
Alternatively, set `spec.runner.image` to an image whose `pg_restore` is version 16 or older, as [Client tool versions](reference/prerequisites.md#client-tool-versions) describes.
Then create a new Migration.

`cannot read the major version of the runner's pg_restore` also fails, because we never read an unknown version as a compatible one.
The message names the binary that preflight ran.
A custom image produces this message when its `pg_restore` is missing at that path or prints an unexpected `--version` line.
`cannot read the target major version for the pg_restore check` means that the target's `server_version_num` query failed after the connectivity test passed.

## Extension ownership failures

`must be owner of extension ...` in the worker log and `target extension ownership required` from preflight have the same meaning.
The target migration role lacks the privileges of a selected extension's owner.
The worker-side error has two causes with identical text.
If this error is the terminal cause in attempt 1, the Migration ends at once as `Failed` with reason `PermissionDenied`.

Read the `Command was:` line that follows the error in the worker log:

- `Command was: COMMENT ON EXTENSION ...` identifies comment restoration.
  This error can occur with the minimal spec `spec.clone: {}`, because `CREATE EXTENSION IF NOT EXISTS` keeps the target's existing owner.
  Without `dropIfExists`, set `clone.skip: [extensionComments]` to keep other comments, or `clone.noComments: true` to suppress all comments.
- `Command was: DROP EXTENSION IF EXISTS ...` identifies the clean phase that `clone.dropIfExists: true` enables.
  `IF EXISTS` suppresses a missing-object error, but not an ownership error.
  A suppressed comment cannot fix this route.
  Use `clone.skip: [extensions]` or an authorised migration role.

The `selected extension ownership` preflight test reports the extension, its target owner, the migration role, and the remedy.
The migration role can own the extension, inherit the owner role's privileges, or be a superuser.
Database ownership and `clone.noOwner` do not satisfy an extension ownership test.
`superuserSecretRef` does not change extension ownership.
Fix a terminal preflight failure.
Then create a new Migration.
A skipped extension also bypasses the availability gate.
In that case, provide the target extensions yourself.

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

Add `wal2json` to `output_plugin_libraries` on the source.
Then reload the configuration without a restart.
Alternatively, stay on the built-in `pgoutput`.
See the [prerequisites](reference/prerequisites.md).

### permission denied for function pg_replication_origin_drop

The first follow attempt fails before any data moves.
The logs show `permission denied for function pg_replication_origin_drop`.
The target role does not have EXECUTE on the `pg_replication_origin_*` functions.
The error appears in pgcopydb's setup cleanup, which runs first.
The attempt fails terminally as `PermissionDenied`.

Apply the grant block from the [prerequisites](reference/prerequisites.md).
Then create a new Migration.
Preflight detects this error, so someone revoked the grants after preflight ran.

### Changes from the window are missing on the target

The live migration streams and reports completion, and `replayLSN` advanced normally.
But changes from the window are missing on the target.
The target role cannot `SET session_replication_role`, and pgcopydb 0.18 hides the failure.
It applies nothing, and it still reports progress.

Run `GRANT SET ON PARAMETER session_replication_role TO <role>` on PostgreSQL 15 and above.
On an older target, connect as a superuser instead.
Alternatively, set `superuserSecretRef` and let preflight apply the grant.
Examine the row counts before you trust any re-run.
Preflight probes this before the first attempt.

### permission denied to start WAL sender

`permission denied to start WAL sender` in the logs means the source role lacks the REPLICATION attribute.
Only a role with that attribute can start a write-ahead log (WAL) sender.

Run `ALTER ROLE <role> REPLICATION`, or set `superuserSecretRef`.
Preflight also tests this attribute.

### The stream keeps dropping

The stream keeps dropping, and the source logs show walsender terminations.
Failures during catchup or drain consume attempts.
Or the follow stops advancing while the worker keeps running.
A low `wal_sender_timeout` on the source kills the logical walsender.
The CloudNativePG (CNPG) default is 5s.
A `PreflightWarning` event names the value that preflight read.

Raise `wal_sender_timeout` to 60s or more for the migration window.
On CNPG, set it in `spec.postgresql.parameters`.
On PostgreSQL 12 and later, you can instead append `options=-c%20wal_sender_timeout%3D60s` to the source URI query (after `?` or `&`).
This raises the timeout only for the connections of the migration.
The option works in a `uriSecretRef` URI, and in a `secretRef` DB key that holds a URI.
Keep the percent-encoding, because libpq rejects the plain form.

Runners older than `0.18.72.g2aa91e7` can also abort receive with `free(): double free detected in tcache 2` when the source closes the stream near endpos.
This can occur, for example, with `wal_sender_timeout=5s`.
The retry recovers, but each abort uses one attempt of `spec.backoffLimit` ([#362](https://github.com/ydixken/pgcopydb-operator/issues/362)).

Runners older than `0.18.74.gc682dce` abort receive with `free(): invalid pointer` or `double free or corruption` after `Failed to IDENTIFY_SYSTEM`.
This occurs when the source ends the walsender while receive reconnects.
Version `0.18.74.gc682dce` exits receive with the source error instead.
That attempt still uses one retry ([#364](https://github.com/ydixken/pgcopydb-operator/issues/364)).

Runners older than `0.18.76.gf9b328b` send receive status only every 10 seconds.
So with a timeout below 10s, the source can end every session before receive reports a flush position.
Then the slot's `confirmed_flush_lsn` stops moving while the worker keeps running ([#369](https://github.com/ydixken/pgcopydb-operator/issues/369)).
Version `0.18.76.gf9b328b` sends status within half of `wal_sender_timeout`.

Attempts that fail in apply setup with `replication origin with ID 1 is already active for PID ...` have a target-side cause.
The apply backend of the previous attempt is still on the target, and it holds the replication origin.
Typically, the backend remains because a proxy cut it while its commit waited for a synchronous standby.
The [retry gate](#retryheld-is-true) holds the next attempt while that backend remains, with reason `SessionsRemain`.
The attempt starts when the origin is free.

### `CaughtUp` stays False

`CaughtUp` stays False in three cases:

- The lag is above `follow.maxCatchupLag`.
- Only one below-threshold sample has arrived.
- No replication sample exists yet.

With one below-threshold sample, the reason is `ConfirmingCatchUp`.
That reason clears on the next sample.

Read `status.replication.lagBytes`.
If heavy write traffic keeps the lag high, throttle the traffic or raise the threshold.

## Synchronous replication and retries

### Commits blocked on synchronous replication

`CommitsBlocked` is True with reason `SyncRepWait`, and a `CommitsBlocked` Warning event names the waiting sessions.
After 15 minutes, the `PgcopydbMigrationCommitsBlocked` alert fires.
Two progress samples in a row found a target session of the migration role with a waiting commit.
That commit waited 30 seconds or more in wait event `SyncRep`.

Under strict synchronous replication, a commit waits until a synchronous standby acknowledges it.
Examples are Patroni `synchronous_mode_strict` and CloudNativePG `dataDurability: required`.
Without a standby, the commit keeps waiting.
While it waits, the session sends nothing.

Healthy standbys can also cause this condition.
After a bulk load, the first commits that wait for a standby can wait while it catches up on the load's WAL ([measured](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/measurements.md#catch-up-waits-followed-a-bulk-load-with-both-standbys-attached)).
While samples come every 5 seconds, such a wait sets `CommitsBlocked` and emits its event after it lasts roughly 40 seconds.
After the table copy, samples come every 10 seconds, and then the same occurs after roughly 50 seconds.
The next sample that finds no wait of 30 seconds or more clears the condition.

Examine the standbys of the target in `pg_stat_replication` on the primary.
Also examine the `synchronous_standby_names` of the target.
The waiting commits finish on their own when a synchronous standby acknowledges them.

A proxy or a load balancer with an idle timeout cuts a connection that sends nothing for that long.
The backend behind that connection keeps waiting on the target.
The attempt then fails, and the [retry gate](#retryheld-is-true) holds the next one until those backends are gone and a test commit returns.
With a 900-second proxy timeout, the proxy cuts the worker at about the time when the alert fires.
The alert can therefore fire just when the retry gate becomes active.
The alert keeps firing while `RetryHeld` is True, because the rule reads both conditions on one 15-minute clock.

So that the commits of the copy do not wait for a standby, set [`spec.target.synchronousCommit: local`](configuration.md#synchronous-commit-on-the-target).
Apply still waits for a standby, because a lost apply commit would lose source changes permanently.

When a sample cannot read the count, the operator logs `progress sample lost the SyncRep count; until it answers again, CommitsBlocked stays True or turns Unknown`.
A True condition stays True until a sample shows no wait of 30 seconds or more, or until the worker succeeds.
No sampler runs between attempts, during a suspension, or after the Migration ends.
In these states, the condition keeps its last value.
For that reason, the alert skips `Failed`, `Completed`, and `Suspended`.

### `RetryHeld` is True

`RetryHeld` is True, and no new attempt starts.
The [retry gate](operations/lifecycle.md#the-retry-gate) holds the next attempt.
The reason names the first test that did not pass.
The gate probes again every 30 seconds.
When a probe passes, the gate starts the attempt.

A hold spends no attempt.
The operator never ends, cancels, or kills a session.
A `RetryHeld` Warning event records the first result of a hold and each change of reason.
`kubectl logs job/<name>-gate-<n>` shows the probe's `gate:` result lines.
The operator keeps a failed probe Job for 30 seconds.

To stop a hold, set `spec.suspend: true`.
This deletes the probe Job.
The resume runs the gate again.

`GateProbing` means that the hold has no probe result yet.
The probe Job is pending or running, or the operator did not create it yet.
Every retry passes through this reason.
When the log of the failed worker was readable, the retry passes through it within seconds.

After a worker pod vanished, `GateProbing` can last several minutes.
The reason is that the operator first waits up to 2 minutes for the log.
Then it requeues with backoff before it creates `<name>-gate-<n>`.
If a probe pod cannot start, the reason changes to `ProbeFailed` with the kubelet reason.

`WorkerPodRemains` means that a pod of the previous attempt's worker, or of a canceled verification rehearsal, has not ended.
The message names the pod and its node.
A pod on a lost node holds the retry until the pod object is gone.
The drain verify waits for a rehearsal pod the same way, with [`RehearsalPodRemains`](#cutovercompleted-is-unknown-with-reason-rehearsalpodremains).

> [!warning]
> Force-delete the pod only after you make sure that its node is down.
> On a node that is only unreachable, the old worker can still write to the target.

To delete the pod of a node that is down:

```sh
kubectl delete pod <pod> --grace-period=0 --force
```

`SessionsRemain` means that the probe's census found sessions of the migration role on the target under pgcopydb's name.
The message lists up to ten of these sessions.
For each session, it shows the pid, state, wait event, age, backend start (`started=`), and name.
On a direct connection, a session that started before the failed attempt is not a leftover of that attempt.
It belongs to another workload under the same role, which the [prerequisites](reference/prerequisites.md#retries-and-snapshot-consistency) rule out.
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
For an all-databases Migration, delete the `datname` line from the query.
When the target URI sets `application_name`, delete the `application_name` line.
The reason is that the gate then counts every session of the role, and its message says so.

A session that waits in `SyncRep` ends when a synchronous standby acknowledges its commit.
See [Commits blocked on synchronous replication](#commits-blocked-on-synchronous-replication).
A session busy in a long statement stays until the statement ends.
A session in state `idle` holds no transaction, so it is safe to end it by hand with `SELECT pg_terminate_backend(<pid>);`.
We count idle sessions too, because an idle backend can still hold the replication origin of apply.
Neither `pg_locks` nor `pg_stat_activity` shows that.

Preflight fails a target behind PgBouncer.
So a pooler holds a retry here only when preflight did not see it.
An example is a pooler that joined the target path later.

Through a transaction-mode pooler, idle server connections keep the name of the last client that they served.
So connections that last served pgcopydb hold the retry until the pooler closes them.
PgBouncer closes them after `server_idle_timeout` (600 seconds by default).
When the idle timeout is off, PgBouncer closes them after `server_lifetime` (3600 seconds by default).
When every listed session is `idle`, the message adds that a pooler can keep them and that ending them is safe.

`SyncCommitNotLocal` means that with `spec.target.synchronousCommit: local`, the probe's target connection reports another `synchronous_commit`.
A pooler or proxy on the way dropped the startup option.
The preflight tests the same thing.
So either the path changed after the preflight, or someone set `synchronousCommit` after the preflight ran.

Unset `synchronousCommit`, and the next probe reads the change.
Alternatively, connect the target directly, as the [preflight entry](#target-synchronous_commit-is-not-local) describes.
The endpoint fields of the target are immutable, so a direct connection needs a new Migration.

`CommitBlocked` means that the test commit waited for a synchronous standby until the probe cancelled it after 5 seconds.
The probe runs the test commit with `synchronous_commit=on`.
The next worker's commits would wait the same way.
It also appears when the probe skipped the test commit because a session, or an earlier probe's commit, still waits in `SyncRep`.
We skip it then, so that the gate never adds waiting commits of its own.

Restore a synchronous standby.
After that, the gate passes when a probe also finds no sessions left.
A cancelled test commit still commits locally, and it only created a temporary table that its own commit dropped.

`ProbeFailed` means that a test gave no result, because of one of these:

- a failed connection
- an SQL error
- a deadline
- a probe pod that cannot start
- an unreadable log
- a log without the expected lines

The message carries the server's error.
For a failed connection, it shows the server's `FATAL:` line, such as a failed password or an unknown database.
Any other client-side error, such as a host name that does not resolve, reads `client-side error withheld`.
The reason is that libpq can echo a connection URI with its password.
Examine the target connection settings.
When the test commit fails for lack of `TEMPORARY` on the target database, the message includes the `GRANT TEMPORARY ON DATABASE` statement that fixes it.

## Publication retry failures

A retry can fail with `publication ... already exists` or `publication retry refused`.
Examine the publication and the resume state on the source.

For `pgoutput` with an automatic publication, a retry preserves the publication whenever its source slot exists.
The operator drops only an orphan that has no source slot, and pgcopydb then retries the incomplete setup.
An explicit `spec.follow.publication` value bypasses this guard, and so does any plugin other than `pgoutput`, such as `wal2json` and `test_decoding`.
A retry that [starts over](#phase-failed-with-reason-snapshotlost) runs `pgcopydb stream cleanup` before the guard, so it finds neither the slot nor the automatic publication.

The bundled runner also fixes an interrupted setup when the retry creates a fresh slot and the work catalog has no sentinel row.
Not every runner version has this bootstrap recovery.
See [client tool versions](reference/prerequisites.md#client-tool-versions).
If a retained slot has missing or unreadable sentinel state, the setup fails closed and does not rebuild established progress.

`publication retry refused: source slot "..." exists but auto publication "..." is missing` means that the source state is inconsistent.
The attempt fails before pgcopydb starts.
A catalog-query error or a publication-drop error also stops it.
Read the worker log.
Then examine `pg_replication_slots` and `pg_publication` on the source.
A `publication ... already exists` error that remains can show an interrupted setup beyond the guard's recovery window, and not a user-owned publication.

> [!warning]
> Do not drop an established publication to force a retry.
> Do not assume that the stream recovers when you create a missing publication again.
> Keep the applications on the source.
> Resolve the cause.
> When you cannot trust the resume state, use a fresh Migration.
> Until cleanup, the retained slot can continue to accumulate WAL.

## Clone progress

### The worker pod runs but the phase never moves

The Migration stays in `Cloning` or `CuttingOver` with `attempts: 1`, and the phase never moves.
The worker pod runs and still logs log sequence number (LSN) positions.
Earlier in its log, pid 1 logged FATAL `Terminating all processes in our process group`.
This is a pgcopydb 0.18 zombie.
A child process survives and keeps the pod alive, so the Job never fails.

The operator detects this and recovers on its own.
It emits a `WorkerZombie` warning event and deletes the pod.
Then the normal retry starts the next attempt.
On an operator version without that recovery, delete the worker pod by hand.

### `Finalizing` takes a long time

The Migration stays in `Finalizing` for a long time, and the worker pod is still running.
Without `clone.deferIndexes`, the target database size is flat.
At the end of a clone, only a single `VACUUM ANALYZE` on the largest table runs, and the target stops growing during it.
With `clone.deferIndexes`, `Finalizing` holds every index build, and the target grows while the builds run.
The pass is healthy while the worker pod log advances.
A log that stops advancing shows a stall.

Look at `CommitsBlocked` first.
When it is True, the tail waits for a synchronous standby to acknowledge its commits.
See [Commits blocked on synchronous replication](#commits-blocked-on-synchronous-replication).
Otherwise, watch the worker pod log, not the target size.

If the tail is too long, set `skip: [vacuum]`.
This setting leaves the target without statistics.
Then run `ANALYZE` yourself before you send traffic.
See [The VACUUM tail](operations/performance.md#the-vacuum-tail), [Deferring index builds](operations/performance.md#deferring-index-builds) and the [phase table](reference/conditions.md#phases).

### `Streaming` and `CaughtUp` stay unset during the clone

The live migration stays in `Cloning` with `Streaming` and `CaughtUp` unset.
At the same time, the worker log shows healthy `STEP` progress, and `status.replication` gets values.
The operator samples the stream on every pass, but it does not act on that sample until the copy completes.
`Streaming`, `CaughtUp`, and cutover all wait for `CloneCompleted`.
The copy-phase status comes from the worker log and a psql size sample, because a read of pgcopydb's own catalogs kills workers.
The pass is healthy while the `STEP` lines advance.

Wait for the worker to print `All step are now done`.
Then `CloneCompleted` becomes True on the next pass, and the stream conditions become active.

### Progress stops moving while a large table copies

`status.progress` and the target size gauge stand still for minutes while the worker log shows a large table copying.
This does not affect the copy itself.

Operator v0.16.0 and earlier read the table during its copy.
The sample probed the table for a row and read every uncommitted page of the open copy.
Or it waited on the exclusive lock of a table copied whole.
Either case ran the target query past its 5-second timeout.
Then status kept its last figures, without a log line, until a copy into the table committed.

Later versions skip the tables that a copy worker holds locked, on any target.
On a PostgreSQL 14 or later target, they also skip every open copy.
They log `progress sample lost a side` with the server's error when a sample side fails.
`status.progress.observedAt` shows how old the figures are.
It stops while samples are lost, and it moves again with the next sample that answers.
On an older target, the probe still reads an open part, and that log line shows the statement timeout.

Upgrade the operator.
Also make the parts smaller on a target before PostgreSQL 14.
Raise `spec.clone.splitMaxParts` or lower `spec.clone.splitTablesLargerThan`, so each copy is a smaller part that commits sooner.
See [Same-table concurrency](operations/performance.md#same-table-concurrency).

### `CloneCompleted` is False with reason `TablesEmptyOnTarget`

The live migration stays in `Cloning`, and `CloneCompleted` is `False` with reason `TablesEmptyOnTarget`.
At the same time, the worker log shows that the base copy finished and that the stream is applying.
The operator's sample found tables that hold rows on the source and no rows on the target.
The condition message names these tables.
The operator therefore does not trust the worker's clone-completion line.
See [#277](https://github.com/ydixken/pgcopydb-operator/issues/277).

The sample tests whether each table holds a row, not how many rows it holds.
The reason is that the source runs ahead of the copy's snapshot until the stream catches up.
A table that got its first source rows after that snapshot reads this way until the stream delivers one of them.

Determine when the named tables got their rows on the source.
Rows written after the base copy's snapshot arrive through the stream.
The condition clears on the first sample that finds one of these rows on the target.
Rows that predate the snapshot are missing from the copy.
In that case, delete the Migration and run a fresh one.
pgcopydb does not re-copy a table that its catalog calls done.

### Phase `Failed` with reason `CloneIncomplete`

Phase `Failed` with reason `CloneIncomplete` means that a clone-only worker exited 0, but pgcopydb's own catalog counted tables that are not done.

Do not use the target as a complete copy.
Read `status.progress` and the worker log.
Then run a fresh Migration.

## Cutover and verification

### `CutoverCompleted` is Unknown with reason `RehearsalPodRemains`

The worker has drained, and no `<name>-verify` Job exists yet.
The cutover canceled a [verification rehearsal](operations/verification.md#rehearsal).
The drain verify waits until the rehearsal pod ends, because the scans of that pod would make the write-downtime window longer.
The condition message names the pod and its node, and a `RehearsalPodRemains` Warning event marks the start of the wait.
On a healthy node, the pod ends seconds after the cancel.
A pod on a lost node holds the drain verify until the pod object is gone, and nothing ends that wait.

> [!warning]
> Force-delete the pod only after you make sure that its node is down.
> On a node that is only unreachable, the pod's checksum queries can still run beside the drain verify.

To delete the pod of a node that is down:

```sh
kubectl delete pod <pod> --grace-period=0 --force
```

### The verify pod runs for a long time

The Migration stays in `CuttingOver`, and the `<name>-verify` pod runs for a long time.
The target's origin lands exactly on the cutover LSN only when nothing wrote on the source between the last applied commit and the approval.
Nearly every cutover therefore proves the drain by content instead.
`pgcopydb compare data` checksums every migrated table.
The database size sets how long that scan takes, not the remaining lag.
The Job logs `endpos`, `replay_lsn`, and `origin_progress` before the compare starts, so its log names the path that it took.

Wait for the scan.
Plan the write-downtime window around it.
A [verification rehearsal](operations/verification.md#rehearsal) times the same scan before a cutover.
A verify Job that fails runs once more before the Migration fails, so a refusal costs the time of two scans.

### Phase `Failed` with reason `DrainIncomplete`

Phase `Failed` with reason `DrainIncomplete` means that the verify Job found changes below the cutover LSN that the target does not have.
`pgcopydb compare data` reported differing tables.
The typical cause is a crash inside the drain window.
After such a crash, pgcopydb `--resume` exits 0 without replaying.
With a runner older than `0.18.98.g944abcc`, apply that fell behind receive across an `output.db` rotation while streaming can also have skipped committed changes ([#376](https://github.com/ydixken/pgcopydb-operator/issues/376)).

Do NOT switch applications to the target.
No data is lost at the source: the slot stays and retains WAL.
The verify Job logs name the differing tables.
They also print `endpos`, `replay_lsn`, and `origin_progress`.
A `replay_lsn` below `endpos` means that the stream was never consumed to the cutover LSN.
The simplest recovery is:

1. Keep running on the source.
2. Delete the Migration, so that cleanup drops the slot.
3. Run a fresh live migration.

### Phase `CutoverPending` and nothing happens

Phase `CutoverPending` means that manual mode waits for `spec.cutover.approved: true`.

Stop the writes on the source.
Then set `spec.cutover.approved: true`.

## Ownership handover failures

Phase `Failed` with reason `OwnershipFailed` means that the `<name>-reown` Job could not hand the restored objects to `clone.ownerAfterRestore`.
The role does not exist on the target, or a `CREATE` grant or an inherited-membership grant is missing.
A lock wait can also time out on all three pod attempts.
The log prints the exact `GRANT` that is missing.

The data is on the target, and only some or all of the `ALTER ... OWNER TO` statements are missing.
Finish the handover by hand.
`Failed` is absorbing, so a new Migration would run the whole clone again.

1. Read the handover Job's log: `kubectl logs job/<migration>-reown`.
   It names the preliminary test that refused, if any.
   Otherwise, it lists the first 200 statements that it generated and stops at the first one that failed.
2. Apply what the log names.
   Create the missing role on the target, or run the `GRANT` that it printed verbatim.
   The preliminary tests cover `CREATE` on the database, `CREATE` on the schemas that stay behind, and inherited membership in the new owner.
   They all run before any `ALTER`.
   So `permission denied for schema <schema>` mid-handover means that someone revoked a grant that passed preflight.
   All three cases are in [Ownership after restore](reference/prerequisites.md#ownership-after-restore-cloneownerafterrestore).
3. Replay the `ALTER ... OWNER TO` statements from the log against the target, connected as the migration role.
   Each statement commits on its own, so a replay is safe.
   `ALTER ... OWNER TO` does nothing when the object already has that owner.
4. Run the query below against the target, connected as the migration role.
   The query MUST return no rows.
5. On a live migration, delete the Migration after you finish the handover.
   The finalizer runs the cleanup Job, which drops the replication slot and ends the WAL retention on the source.
   A terminal failure does not release the slot on its own.

The query is the set that the Job itself examines again before it reports success:

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

The migration role still owns every row that the query returns.
Also run the query before step 3 to see what is outstanding.
An array type and a row type appear beside the type or relation that they belong to.
They need no statement of their own.

A `canceling statement due to lock timeout` in the log is not a privilege problem.
The Job caps each `ALTER` and its lock wait at 60 seconds, and a session on the target holds a conflicting lock.
The Job sets both caps with `SET LOCAL` inside that statement's own transaction.
Clear that session.
Then replay the statements.

## Workers and cluster objects

### exists but belongs to another owner

The Migration stays at attempt 1 with the event `... exists but belongs to another owner`.
The work PVC or ConfigMap of a just-deleted Migration with the same name still waits for garbage collection.

Wait for garbage collection to finish, or delete the leftover objects.

### `pg_dump` reports a server version mismatch

`pg_dump: error: server version mismatch` in the logs means that the client tools in the runner image are older than a server major.
`pg_dump` must be at least the newest major on either side.

If you pinned `spec.runner.image`, point it at an image with matching client tools.
The default runner ships PostgreSQL 18 client tools.

### No worker pod and the PVC stays Pending

When no StorageClass can provision the work volume, no worker pod appears and the PVC stays `Pending`.

Set `spec.workVolume.storageClassName`, or fix the cluster default.

### Phase `Failed` with retry budget exhausted

Phase `Failed` with "retry budget exhausted" means that the counted attempts reached `backoffLimit + 1`.
Each of them failed on a cause that the operator cannot classify as deterministic.
The message reads `retry budget exhausted after N attempts (M exempt)`.
When a failed attempt spent the last of the budget, `: attempt K failed:` and that attempt's error follow.
Exempt attempts are the attempts that the retry gate found cut during a synchronous standby outage.
They do not count, so `N` can be more than `backoffLimit + 1`.
See [Exempt attempts](operations/lifecycle.md#exempt-attempts).

The gate decides at its first census after the failure.
When no probe gets a census within 15 minutes of the failure, the attempt counts.
An example is a probe that cannot connect.
If that spends the budget, the Migration fails then.

The first attempt's logs almost always name the cause.
A permission error mostly stops early as `PermissionDenied`.
A full work volume mostly stops early as `DiskFull`.
A change that names a column the target lacks mostly stops early as `SchemaDrift`.
Such an error lands here when it is outside the last 40 log lines that the classifier reads.

A work volume that became full then shows `database or disk is full` or `No space left on device` in the first attempt's logs.
A live migration that kept starting over shows a `SnapshotLost` event for each start-over.

`the pod log was not readable (...)` in the message means that the operator could not read the worker log for two minutes after the Job failed.
As a result, the operator could not classify the cause.
The text in parentheses is the read error.
Read the cause with `kubectl logs job/<name>-run-<attempt>` while the Job still exists.

Fix the cause.
Then create a new Migration.
A terminal state is absorbing, so this Migration cannot resume.

### Phase `Failed` with reason `PermissionDenied`

Phase `Failed` with reason `PermissionDenied` after a single attempt means that the worker got a permission error that no retry can fix.
The log contains `permission denied`, `must be owner of extension`, or SQLSTATE 42501.
The condition message carries the matched line.

Grant what the message names.
Then create a new Migration.
A missing source-side SELECT or USAGE is the usual cause, because preflight covers the target CREATE rights.
For the grantable target rights, set `superuserSecretRef`.

### Phase `Failed` with reason `DiskFull`

Phase `Failed` with reason `DiskFull` after a single attempt means that the work volume is full.
The condition message carries the matched line.
`database or disk is full` comes from pgcopydb's SQLite catalogs.
`No space left on device` comes from a file that pgcopydb or pg_dump writes under the work directory.

Increase `spec.workVolume.size`.
Then create a new Migration.
A terminal state is absorbing (see [Retries and resume](operations/lifecycle.md#retries-and-resume)), so this Migration cannot resume.
On a live migration, the change spool grows with source writes until cutover.
So size the work volume for the writes that the migration has to carry.

When the target has no more disk space (`could not extend file`) or shared memory (`could not resize shared memory segment`), the reason is not `DiskFull`.
Those attempts keep normal retries, because space or load on the target can change between attempts.

### Phase `Failed` with reason `SchemaDrift`

Phase `Failed` with reason `SchemaDrift` after a single attempt means that apply got a change that names a column the target lacks.
Usually, DDL ran on the source during the migration window, and replication does not carry DDL.
A `DROP COLUMN` or `RENAME COLUMN` on the target gives the same error.
The condition message carries the matched line, `[TARGET <pid>] [42703] ERROR:  column ... does not exist`.

Create a new Migration.
It copies the schema from the source again, so the target needs no manual DDL.
A terminal state is absorbing (see [Retries and resume](operations/lifecycle.md#retries-and-resume)), so this Migration cannot resume.
Do not run DDL on either side until the cutover, as the [live migration prerequisites](reference/prerequisites.md#live-migration-specfollowenabled-true) require.

The operator classifies only SQLSTATE 42703 on the target connection of apply.
A missing table (42P01) or any other schema error keeps normal retries and ends as `BackoffLimitExceeded`.

### Phase `Failed` with reason `SnapshotLost`

Phase `Failed` with reason `SnapshotLost` means that a live migration's retry could not resume its table copy, and that `spec.clone.dropIfExists` is false.
The replication slot outlived the attempt that created it, and its snapshot did not.
Tables copied without that snapshot would hold rows that the slot then replays again, so pgcopydb refused the resume.
The condition message carries its `Refusing to resume the copy: ...` line.
That line names the slot, its snapshot and LSN, and how many tables are still to copy.

Starting over needs `--drop-if-exists`, because the target holds the objects and rows of the earlier attempts.
First, delete the failed Migration.
A terminal failure keeps the slot, which retains WAL on the source, and the deletion drops the slot.
Then create a new Migration with `spec.clone.dropIfExists: true`, or one against an empty target.

Keep that order when the new Migration reuses a fixed `spec.follow.slotName`.
The failed Migration's cleanup drops the slot and the origin of that name.
In the other order, that slot and origin would then belong to the new Migration.

With `dropIfExists: true`, the operator starts over on its own and emits a `SnapshotLost` warning event.
That attempt runs `pgcopydb stream cleanup` to drop the slot, the automatic publication, and the target origin.
Then it runs `pgcopydb clone --follow --restart --drop-if-exists`.

### Status updates lag behind the worker

The worker makes progress, but `status` updates later than the 10-second poll interval.
Or, during a copy, the size gauges update later than their 5-second sample interval.

A reconcile pass that runs longer than the poll interval logs `active worker observation` at the info level.
This log entry is a sanitized duration summary of the operations that the pass reached.
Set `--zap-log-level=debug` on the manager to see the same summary for every pass.
The summary records only timing: no credentials, no SQL, and no raw command output.
It shows where a slow pass spent its time.
It does not establish the cause of a past delay, and it does not guarantee a wall-clock poll interval.

The size gauges come from a sampler that does not wait for the pass.
A sample that takes longer than its own interval logs `progress sample took longer than its interval`.

## Metrics and dashboards

### Scrapes return 401

The Prometheus target for the operator is down, and scrapes return 401 because the scrape carried no credential.
The manager refuses an anonymous caller before it examines any access rights.

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
The manager cannot create TokenReviews, so it cannot review the caller's token at all.

Grant its ServiceAccount `create` on `tokenreviews` (authentication.k8s.io) and `subjectaccessreviews` (authorization.k8s.io).
The chart renders both whenever `rbac.create` and `metrics.enabled` are on.
This failure therefore means `rbac.create=false` and RBAC that you supply yourself.

### Progress metrics stop at the estimate

`status.progress` and the `_tables_*`/`_indexes_*`/`_clone_*_bytes` metrics stop at the estimate.
They move during the copy.
But pgcopydb's exact count never replaces them, not at the clone's completion and not at the drain verification.
An exact runner-version allowlist gates the progress poll, and this runner's pgcopydb is not on it.
The flag is `--progress-poll-versions`, and the chart value is `runner.progressPollVersions`.

For stock pgcopydb 0.18 the gate stays closed on purpose.
Its `list progress` returns no data (`no such column: bytes`).
Against a filtered work dir, it also corrupts the stored filters and kills the clone.

Use the bundled runner, whose patched pgcopydb is on the allowlist by default.
Extend the allowlist only for a runner image whose pgcopydb carries the upstream fixes.
The database-size metrics, the phase metrics, and the counters themselves flow on any runner.

### Dashboards never appear in Grafana

Dashboards installed with `grafana.dashboards.enabled=true` never appear in Grafana.
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
