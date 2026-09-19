# Troubleshooting

Run `kubectl describe migration <name>` first, and read the conditions and the events.
Then read the worker pod logs with `kubectl logs job/<name>-run-<N>`, which are structured JSON.
The `Failed` condition carries the last pgcopydb ERROR from the failed pod, after Kubernetes' own "Job has reached the specified backoff limit".
It falls back to the bare Job message when the pod is already gone.

| Symptom | Section |
|---|---|
| [`describe` shows no phase, no conditions, no events](#migration-has-no-status-and-the-log-repeats-is-immutable) | Spec and admission |
| [Phase `Failed` at `Validating`, reason `PreflightFailed`](#phase-failed-at-validating) | Preflight failures |
| [`PreflightFailed` names the all-databases superuser requirement](#all-databases-superuser-requirement) | Preflight failures |
| [`PreflightFailed` names unavailable selected extensions](#unavailable-selected-extensions) | Preflight failures |
| [`Validated` is `Unknown` with reason `PreflightRunning`](#preflight-pod-cannot-start) | Preflight failures |
| [`PreflightFailed` and the Job shows `DeadlineExceeded`](#preflight-deadline-exceeded) | Preflight failures |
| [`applying ... via superuserSecretRef failed`](#superusersecretref-cannot-apply-the-grants) | Preflight failures |
| [`PreflightFailed` with `still lacks ... after remediation`](#a-grant-is-still-missing-after-remediation) | Preflight failures |
| [`no replica identity usable for UPDATE/DELETE`](#no-replica-identity-usable-for-updatedelete) | Preflight failures |
| [`must be owner of extension ...`, or `target extension ownership required`](#extension-ownership-failures) | Extension ownership failures |
| [Slot creation fails with `could not access file "wal2json"`](#wal2json-is-not-installed-on-the-source) | Replication and follow |
| [Slot creation fails with `may not be used as an output plugin`](#wal2json-is-not-trusted-by-the-source) | Replication and follow |
| [`permission denied for function pg_replication_origin_drop`](#permission-denied-for-function-pg_replication_origin_drop) | Replication and follow |
| [The migration completes, but changes from the window are missing](#changes-from-the-window-are-missing-on-the-target) | Replication and follow |
| [`permission denied to start WAL sender`](#permission-denied-to-start-wal-sender) | Replication and follow |
| [The stream keeps dropping and attempts burn](#the-stream-keeps-dropping) | Replication and follow |
| [`CaughtUp` stays False](#caughtup-stays-false) | Replication and follow |
| [`publication ... already exists` or `publication retry refused`](#publication-retry-failures) | Publication retry failures |
| [The worker pod runs and logs, but the phase never moves](#the-worker-pod-runs-but-the-phase-never-moves) | Clone progress |
| [`Finalizing` runs long with the target size flat](#finalizing-takes-a-long-time) | Clone progress |
| [`Streaming` and `CaughtUp` stay unset during the clone](#streaming-and-caughtup-stay-unset-during-the-clone) | Clone progress |
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
The immutable blocks are `follow`, `source`, and `target`.
The typical case is `allowMissingReplicaIdentity: []` from a template that renders an empty list.
Operators up to v0.8.1 rejected that spelling before any status could be set.

Upgrade past v0.8.1, where both spellings work.
On an older version, omit the field or set it to `null` instead of `[]`, `""`, or `false`.

## Preflight failures

### Phase `Failed` at `Validating`

Phase `Failed` at `Validating` with reason `PreflightFailed` means one preflight check failed.
The possible causes are connectivity, selected extension availability or ownership, a clone privilege, an all-databases probe, or a follow prerequisite.

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
A misnamed credentials Secret or an unbound work PersistentVolumeClaim (PVC) are the typical causes.

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

### `superuserSecretRef` cannot apply the grants

The preflight log shows `warn: ... lacks rolsuper` and later `applying ... via superuserSecretRef failed`.
The `superuserSecretRef` role is no superuser and lacks the rights to apply the grants.
A missing `rolsuper` alone is only a warning, because managed admin roles work without it.

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
A low `wal_sender_timeout` on the source kills the logical walsender.
CloudNativePG (CNPG) defaults to 5s.

Raise `wal_sender_timeout` to 60s or more for the migration window.
On CNPG, set it in `spec.postgresql.parameters`.

### `CaughtUp` stays False

`CaughtUp` stays False in three cases: the lag is above `follow.maxCatchupLag`, only one below-threshold sample has arrived, or no replication sample exists yet.
With one below-threshold sample the reason is `ConfirmingCatchUp`, and it clears on the next sample.

Check `status.replication.lagBytes`.
If heavy write traffic keeps the lag high, throttle the traffic or raise the threshold.

## Publication retry failures

A retry can fail with `publication ... already exists` or `publication retry refused`.
Check the publication and the resume state on the source.

For `pgoutput` with an automatic publication, a retry preserves the publication whenever its source slot exists.
The operator drops only an orphan that has no source slot, and pgcopydb then retries the incomplete setup.
An explicit `spec.follow.publication` value bypasses this guard, and so does any plugin other than `pgoutput`, such as `wal2json` and `test_decoding`.

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
It fires a `WorkerZombie` warning event, deletes the pod, and the normal retry resumes the migration.
On an operator version without that recovery, delete the worker pod by hand.

### `Finalizing` takes a long time

The Migration sits in `Finalizing` for a long time, the target database size is flat, and the worker pod is still running.
The end of a clone narrows to a single `VACUUM ANALYZE` on the largest table, and the target stops growing during it.
The pass is healthy while the worker pod log advances; a stopped log is a stall.

Watch the worker pod log instead of the target size.
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

### `CloneCompleted` is False with reason `TablesEmptyOnTarget`

The live migration stays in `Cloning`, and `CloneCompleted` is `False` with reason `TablesEmptyOnTarget`.
Meanwhile the worker log shows the base copy finished and the stream is applying.
The operator's sample found tables that hold rows on the source and none on the target.
It therefore does not trust the worker's clone-completion line; see [#277](https://github.com/ydixken/pgcopydb-operator/issues/277).
A table that got its first source rows after the clone's snapshot reads the same way until the stream delivers them.

List the target's tables without rows, then check those tables on the source.
If the rows are genuinely missing, delete the Migration and run a fresh one.
pgcopydb does not re-copy a table that its catalog calls done.
If the stream is delivering them, the condition clears on the next sample.

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
The Job caps each statement and its lock wait at 60 seconds, and a session on the target holds a conflicting lock.
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

Phase `Failed` with "retry budget exhausted" means every attempt failed on a cause the operator cannot classify as deterministic.
The first attempt's logs almost always name it.
A permission error mostly stops early as `PermissionDenied`, but one too deep in the log tail for the classifier lands here.

Fix the cause, then create a new Migration.
A terminal state is absorbing, so this Migration cannot resume.

### Phase `Failed` with reason `PermissionDenied`

Phase `Failed` with reason `PermissionDenied` after a single attempt means the worker hit a permission error that no retry can fix.
The log holds `permission denied`, `must be owner of extension`, or SQLSTATE 42501.
The condition message carries the matched line.

Grant what the message names, then create a new Migration.
A missing source-side SELECT or USAGE is the usual cause, because preflight covers the target CREATE rights.
For the grantable target rights, set `superuserSecretRef`.

### Status updates lag behind the worker

The worker makes progress, but `status` and the metrics update later than the 10-second poll interval.

Set `--zap-log-level=debug` on the manager to expose the controller's timing messages.
The controller emits a sanitized duration summary at verbosity `V(1)` for the operations it reaches during a reconcile pass.
The summary records timing only: no credentials, no SQL, and no raw command output.
It shows where a slow pass spent its time.
It does not establish the cause of a past delay, and it does not guarantee a wall-clock poll interval.

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
