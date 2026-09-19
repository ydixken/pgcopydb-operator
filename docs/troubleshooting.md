# Troubleshooting

Look at `kubectl describe migration <name>` first (conditions and events), then at the worker pod logs: `kubectl logs job/<name>-run-<N>` (structured JSON). The `Failed` condition carries the last pgcopydb ERROR from the failed pod after Kubernetes' own "Job has reached the specified backoff limit", so the cause is usually there without opening the logs. It falls back to the bare Job message when the pod is already gone.

| Symptom | Cause | Fix |
|---|---|---|
| `describe` on a follow migration shows nothing at all: no phase, no conditions, no events; the operator log repeats `follow is immutable` (or `source is immutable`, `target is immutable`) | The spec spells an optional field of an immutable block (`follow`, `source`, `target`) as an explicit zero value (typical: `allowMissingReplicaIdentity: []` from a template that renders an empty list). Operators up to v0.8.1 rewrote the whole object when adding the finalizer; Go's `omitempty` dropped the stored zero value from that write, and the immutability rule rejected it before any status could be set | Upgrade past v0.8.1: the operator patches metadata only and both spellings work. On older versions, omit the field (or set it to `null`) instead of rendering `[]`, `""`, or `false` |
| Phase `Failed` at `Validating`, reason `PreflightFailed` | Connectivity, selected extension availability or ownership, a clone privilege, an all-databases probe, or a follow prerequisite failed | Follow the condition's recovery action, then create a new Migration. Grant hints apply only to remediable privilege failures. No worker attempt has started. |
| `PreflightFailed` names the all-databases superuser requirement | A migration connection lacks `rolsuper` | Use superuser migration credentials on both sides and recreate the Migration; `superuserSecretRef` and managed admin roles without `rolsuper` do not satisfy [this contract](reference/prerequisites.md#all-databases). |
| `PreflightFailed` names unavailable selected extensions | The target lacks both an installed extension and its default package, or `dropIfExists` or `allDatabases` requires a missing default version | Install the required target extension package or choose a target that provides it, then create a new Migration. Preflight does not install extensions or require source and target versions to match. |
| Logs: `must be owner of extension ...`, or preflight reports `target extension ownership required` | The target migration role lacks a selected extension owner's privileges | Follow [Extension ownership failures](#extension-ownership-failures); the remedy depends on whether the failing command is `DROP` or `COMMENT`. |
| Migration sits in `Validating`; `Validated` is `Unknown` with reason `PreflightRunning` and the message names a pod reason (`CreateContainerConfigError`, `Unschedulable`, a pull error) | The preflight pod cannot start; the condition message carries the kubelet reason verbatim (typical: a misnamed credentials Secret, an unbound work PVC) | Fix the named object; the gate resumes on its own. The preflight Job is bounded at 30 minutes, after which the Migration fails instead of waiting |
| `PreflightFailed` and the Job shows `DeadlineExceeded` | A check hung past the 30-minute preflight deadline, or the pod never started in time; each connect already times out after 10 seconds and is retried six times before failing, except for permanent errors (wrong password, unknown role or database), which end the retries once a second probe repeats them, with the server's line in the condition | Read the condition message and the preflight Job logs for the last check that ran, fix the endpoint or object it names, create a new Migration |
| Preflight log shows `warn: ... lacks rolsuper` and later `applying ... via superuserSecretRef failed` | The `superuserSecretRef` role is no superuser and lacks the rights to apply the grants (`rolsuper` alone is only a warning, since managed admin roles work without it) | Point the Secret at a role that can run the printed statements (superuser, or the provider's admin role), or drop `superuserSecretRef` and apply the grants by hand |
| `PreflightFailed` with `still lacks ... after remediation` | A grant was applied but did not stick; on targets older than PostgreSQL 15 the `session_replication_role` GRANT does not exist | Check the target's major version and the preflight logs; on old targets the regular role cannot be granted this, connect the migration as a superuser-capable role instead |
| `PreflightFailed` with `no replica identity usable for UPDATE/DELETE` and a table list | UPDATE or DELETE on those tables would fail on the source once published; the audit covers all user tables, including ones excluded by `clone.filters` | Give each table a primary key or `REPLICA IDENTITY USING INDEX`/`FULL`, or acknowledge read-only and insert-only ones in `spec.follow.allowMissingReplicaIdentity` (`["*"]` acknowledges all) in a new Migration |
| First follow attempt fails at slot creation: `could not access file "wal2json"` | `spec.follow.plugin: wal2json`, but the plugin is not installed on the source. Preflight cannot detect this (a decoding plugin has no catalog entry to query) and says so in its log | Install the wal2json package on the source, or stay on the built-in `pgoutput` |
| First follow attempt fails at slot creation: `library "wal2json" may not be used as an output plugin` | `spec.follow.plugin: wal2json`, but the source does not trust it. Since the 2026-08-13 minor releases PostgreSQL loads only the output plugins named in `output_plugin_libraries`, which defaults to `pgoutput, test_decoding` | Add `wal2json` to `output_plugin_libraries` on the source and reload the config (no restart), or stay on the built-in `pgoutput`; see the [prerequisites](reference/prerequisites.md) |
| First follow attempt dies before any data moves; logs show `permission denied for function pg_replication_origin_drop` | Target role misses EXECUTE on the `pg_replication_origin_*` functions; the error surfaces in pgcopydb's setup cleanup, which runs first, and the attempt fails terminally as `PermissionDenied` without retries | Apply the grant block from the [prerequisites](reference/prerequisites.md), then create a new Migration. Preflight catches this now, so reaching it means the grants were revoked after the check |
| Live migration streams and "completes", but changes from the window are missing on the target; `replayLSN` advanced normally | Target role cannot `SET session_replication_role`; pgcopydb 0.18 swallows the failure and applies nothing while reporting progress | `GRANT SET ON PARAMETER session_replication_role TO <role>` (PostgreSQL 15+, superuser otherwise), or set `superuserSecretRef` and let preflight apply it; verify with row counts before trusting any re-run. Preflight probes this before the first attempt |
| Logs: `permission denied to start WAL sender` | Source role lacks the REPLICATION attribute | `ALTER ROLE <role> REPLICATION`, or set `superuserSecretRef`; preflight checks this too |
| Stream keeps dropping; source logs show terminated walsenders; attempts burn during catchup or drain | Aggressive `wal_sender_timeout` on the source kills the logical walsender (CloudNativePG defaults to 5s) | Raise it to 60s or more for the migration window, on CNPG via `spec.postgresql.parameters` |
| Migration sits in `Cloning` or `CuttingOver` with `attempts: 1` and never moves; the worker pod runs and keeps logging LSN positions, but earlier in its log pid 1 said FATAL `Terminating all processes in our process group` | pgcopydb 0.18 zombie: a clone worker died and the supervisor terminated its process group, but the streaming receive child survived and keeps the pod alive, so the Job never fails | The operator detects this and recovers on its own since this change: a `WorkerZombie` warning event fires, the pod is deleted, and the normal retry resumes the migration. On older operator versions, delete the worker pod by hand |
| Migration sits in `Finalizing` for a long time, the target database size is flat, and the worker pod is still running | Not a stall. pgcopydb runs `VACUUM ANALYZE` per table alongside the copy, but a table's vacuum cannot start until its own copy finishes, and the largest table finishes last, so the end of a clone narrows to one vacuum running alone. On a fixture where a single table held 73% of the bytes this tail was roughly a fifth of the wall clock | Nothing to fix. `skip: [vacuum]` gives the time back at the cost of leaving the target without statistics, so run `ANALYZE` yourself before sending traffic; see [Performance tuning](operations/performance.md) and the [phase table](reference/conditions.md#phases) |
| Live migration stays in `Cloning` with `Streaming` and `CaughtUp` unset, while `status.replication` fills in and the worker log shows healthy `STEP` progress | By design. The operator samples the stream from the source and the target on every pass, so `status.replication` is populated during the base copy, but it does not act on that reading until the copy is done: `Streaming`, `CaughtUp` and cutover all wait for `CloneCompleted`. Copy-phase status comes from the worker log plus a catalog-free psql size sample, and nothing in either path opens pgcopydb's own catalogs, because reading those while the copy writes them kills workers | Nothing to fix while the log advances: when the worker prints `All step are now done`, `CloneCompleted` goes True on the next pass and the stream conditions engage |
| Migration sits in `CuttingOver` with the `<name>-verify` pod running for a long time | Not a stall. The target's origin lands exactly on the cutover LSN only when nothing wrote on the source between the last applied commit and the approval, so nearly every cutover proves the drain by content instead: `pgcopydb compare data` checksums every migrated table, and that scan is sized by the database, not by the remaining lag | Wait for it, and plan the write-downtime window around it. The Job logs `endpos`, `replay_lsn` and `origin_progress` before the compare starts, so the pod's log says which path it took and why. A verify Job that fails runs once more (`backoffLimit: 1`, which absorbs a pod eviction) before the Migration fails, so a refusal pays for the scan twice |
| Migration `Failed`, condition reason `DrainIncomplete` | The verify Job found the target missing changes below the cutover LSN: `pgcopydb compare data` reported differing tables (typical cause: a crash inside the drain window, after which pgcopydb `--resume` exits 0 without replaying) | Do NOT switch applications to the target. No data is lost at the source: the slot is kept and retains WAL. The verify Job logs name the differing tables and print `endpos`, `replay_lsn` (below `endpos` means the stream was never consumed to the cutover LSN), and `origin_progress`. Simplest recovery: keep running on the source, delete the Migration (cleanup drops the slot), and run a fresh live migration |
| Live migration stays in `Cloning`; `CloneCompleted` is `False` with reason `TablesEmptyOnTarget`, while the worker log shows the base copy finished and the stream is applying | The operator's sample found tables that hold rows on the source and none on the target, so it does not trust the worker's clone-completion line: a `--resume` after killed attempts has called such a table done ([#277](https://github.com/ydixken/pgcopydb-operator/issues/277)). A table that received its first rows on the source after the clone's snapshot reads the same way until the stream delivers them | Find the tables by listing the target's tables without rows and checking them on the source. If the rows are genuinely missing, delete the Migration and run a fresh one: pgcopydb does not re-copy a table its catalog calls done. If the stream is delivering them, the condition clears on the next sample |
| Phase `Failed`, reason `CloneIncomplete` | A clone-only worker exited 0, but pgcopydb's own catalog counted tables not done | Do not use the target as a complete copy. Read `status.progress` and the worker log, then run a fresh Migration |
| Migration `Failed`, condition reason `OwnershipFailed` | The `<name>-reown` Job could not hand the restored objects to `clone.ownerAfterRestore`: the role does not exist on the target, a `CREATE` or inherited-membership grant is missing (the log prints the exact `GRANT`, and the pre-checks catch this before any `ALTER` runs), or a lock wait timed out on all three pod attempts | The data is on the target and the handover is re-runnable. See [Ownership handover failures](#ownership-handover-failures) |
| Retry fails with `publication ... already exists` or `publication retry refused` | Publication and resume state need inspection | See [Publication retry failures](#publication-retry-failures). |
| Migration sits at attempt 1 with event `... exists but belongs to another owner` | The work PVC or ConfigMap of a just-deleted Migration with the same name is still awaiting garbage collection | Wait for GC to finish, or delete the leftover objects |
| Logs: `pg_dump: error: server version mismatch` | Client tools in the runner image are older than a server major (`pg_dump` must be at least the newest major on either side) | The default runner ships PostgreSQL 18 client tools; if you pinned `spec.runner.image`, point it at an image with matching tools |
| Phase `CutoverPending` and nothing happens | Manual mode waiting for `spec.cutover.approved: true` | That is the contract: stop writes, then approve |
| `CaughtUp` stays False | Lag above `follow.maxCatchupLag`, one below-threshold sample so far (reason `ConfirmingCatchUp`, which clears on the next sample), or no replication sample yet | Check `status.replication.lagBytes`; heavy write traffic keeps lag high, throttle it or raise the threshold |
| No worker pod; PVC `Pending` | No StorageClass can provision the work volume | Set `spec.workVolume.storageClassName`, or fix the cluster default |
| Phase `Failed`, "retry budget exhausted" | Every attempt failed on a cause the operator cannot classify as deterministic; the first attempt's logs almost always name it (permission errors mostly stop early as `PermissionDenied`; one buried too deep in the log tail for the classifier lands here) | Fix the cause and create a new Migration; terminal states are absorbing by design |
| Phase `Failed`, reason `PermissionDenied`, after a single attempt | The worker hit a permission error retries cannot fix (`permission denied`, `must be owner of extension`, or SQLSTATE 42501 in its log); the operator stops instead of burning the remaining budget, and the condition message carries the matched line | Grant what the message names (source-side SELECT/USAGE are the usual suspects, since the target CREATE rights are preflighted), or set `superuserSecretRef` for the grantable target rights, then create a new Migration |
| Prometheus target for the operator is down, scrapes return 401 | The scrape carried no credential. The manager refuses anonymous callers outright, before it checks anyone's access | Send a token: the chart's ServiceMonitor sets `bearerTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token`, and a hand-written one needs the same line |
| Prometheus target for the operator is down, scrapes return 403 | The scraper reached the manager and was identified, but has no `get` on the `/metrics` nonResourceURL. That half is the scraper's own RBAC, so the chart cannot grant it | Bind a ClusterRole with `nonResourceURLs: ["/metrics"], verbs: ["get"]` to the ServiceAccount Prometheus runs as. kube-prometheus-stack already binds one to its own Prometheus |
| Scrapes return 500 and the manager logs `Authentication failed` | The manager may not create TokenReviews, so it cannot check the caller's token at all | Grant its ServiceAccount `create` on `tokenreviews` (authentication.k8s.io) and `subjectaccessreviews` (authorization.k8s.io). The chart renders both whenever `rbac.create` and `metrics.enabled` are on, so this means `rbac.create=false` and RBAC you supply yourself |
| `status.progress` and the `_tables_*`/`_indexes_*`/`_clone_*_bytes` metrics stop at the estimate: they move during the copy but pgcopydb's exact count never replaces them, at a plain clone's completion or at a live migration's drain verification | The progress poll is gated on an exact runner-version allowlist (`--progress-poll-versions`, chart value `runner.progressPollVersions`) and this runner's pgcopydb is not on it. For stock pgcopydb 0.18 that is deliberate: its `list progress` returns no data (`no such column: bytes`) and, run against a filtered work dir, corrupts the stored filtering, killing the clone | Use the bundled runner (its patched pgcopydb is allowlisted by default), or extend the allowlist only for runner images whose pgcopydb carries the upstream fixes. The database-size and phase metrics flow on any runner, as do the counters themselves: what an unallowlisted runner gives up is only the exact count that would replace the estimate at the end |
| Dashboards installed with `grafana.dashboards.enabled=true` but Grafana never shows them | The sidecar only watches ConfigMaps in Grafana's own namespace (or the namespaces its `searchNamespace` names), and the chart rendered them into the release namespace | Set `grafana.dashboards.namespace` to Grafana's namespace, or widen the sidecar's `searchNamespace`; see [Monitoring](operations/monitoring.md) |
| PrometheusRule installed with `metrics.prometheusRule.enabled=true` but the alerts never appear in Prometheus | Prometheus selects rules by label (kube-prometheus-stack matches its release label by default) and the rule object carries none of the selected labels | Add the matching label via `metrics.prometheusRule.additionalLabels` (for kube-prometheus-stack: `release: <its release name>`), or widen the Prometheus `ruleSelector` |

## Publication retry failures

For `pgoutput` with an automatic publication, retries preserve the publication whenever its source slot exists.
Only an orphan with no source slot is dropped before pgcopydb retries incomplete setup.
Explicit `spec.follow.publication` values and non-`pgoutput` plugins, including `wal2json` and `test_decoding`, bypass this guard.

The bundled pgcopydb `0.18.15.gea2dc96` also repairs interrupted initial setup when the retry creates a fresh slot but the work catalog has no sentinel row ([fork PR #11](https://github.com/ydixken/pgcopydb/pull/11)).
Together with the operator's slotless-orphan guard, this lets automatic retries recreate the publication and initialize the sentinel at the new slot's start LSN.
Setup preserves existing sentinel fields; a retained slot with missing or unreadable sentinel state fails closed rather than rebuilding established progress.
Version `0.18.13.g4873c18` has certified keepalive feedback but lacks this bootstrap recovery.

The operator fixes in [#274](https://github.com/ydixken/pgcopydb-operator/issues/274) remain separate: progress SQL restores pooled settings on commit or rollback, and the publication guard uses a named dollar-quote tag because kubelet reduces `$$` to `$` in container commands and arguments.
The fork fix addresses the missing-sentinel failure after that guard can run, not the `DO $` syntax error.
The default plugin remains `pgoutput`, and `wal2jsonNumericAsString` remains an opt-in for `wal2json`; bootstrap recovery does not change numeric decoding.

`publication retry refused: source slot "..." exists but auto publication "..." is missing` means the source state is inconsistent.
The attempt fails before pgcopydb starts; a catalog-query or publication-drop error also stops the attempt.
Read the worker log and inspect `pg_replication_slots` and `pg_publication` on the source.
A remaining `publication ... already exists` error can indicate interrupted setup beyond the guard's recovery window, not necessarily a user-owned publication.

> [!warning]
> Do not drop an established publication to force a retry or assume that recreating a missing one recovers the stream.
> Keep applications on the source, resolve the cause, and use a fresh Migration when resume state cannot be trusted.
> The retained slot can continue accumulating WAL until cleanup.

## Extension ownership failures

`must be owner of extension ...` has two causes with identical error wording.
If this worker-side error is the terminal cause in attempt 1's log tail, the operator ends the Migration in phase `Failed` with reason `PermissionDenied` on attempt 1, without spending the remaining retry budget.
Read the following `Command was:` line in the worker log:

- `Command was: COMMENT ON EXTENSION ...` identifies comment restoration.
  This can fail with the minimal spec `spec.clone: {}`: `CREATE EXTENSION IF NOT EXISTS` leaves the target's existing owner unchanged, then the comment requires that owner's privileges.
  Without `dropIfExists`, set `clone.skip: [extensionComments]` to retain other comments, or `clone.noComments: true` to suppress all comments.
- `Command was: DROP EXTENSION IF EXISTS ...` identifies the clean phase enabled by `clone.dropIfExists: true`.
  `IF EXISTS` suppresses missing-object errors, not ownership checks.
  Suppressing comments cannot fix this route; use `clone.skip: [extensions]` or an authorised migration role.

The `selected extension ownership` preflight check reports the extension, its target owner, the migration role, and the remedy for the configured route.
The migration role can own the extension, inherit the owning role's privileges, or be a superuser.
Database ownership and `clone.noOwner` do not satisfy extension ownership checks, and `superuserSecretRef` does not change extension ownership.
After correcting a terminal preflight failure, create a new Migration.
Skipping extensions also bypasses the availability gate, so provide the required target extensions yourself.

## Ownership handover failures

Phase `Failed` with reason `OwnershipFailed` means the `<name>-reown` Job could not hand the restored objects to `clone.ownerAfterRestore`.
The clone itself is finished: the data is on the target, and what is missing is some or all of the `ALTER ... OWNER TO` statements.
This Migration cannot be resumed, because `Failed` is absorbing in this operator's state machine, and creating a new one would re-run the entire clone from scratch.
Finish the handover by hand instead.

1. Read the handover Job's log: `kubectl logs job/<migration>-reown`.
   It names the pre-check that refused, if any, and otherwise lists the statements it generated (the first 200) and stops at the first one that failed.
2. Apply what the log names: create the missing role on the target, or run the `GRANT` it printed verbatim.
   The pre-checks cover `CREATE` on the database, `CREATE` on schemas that stay behind, and inherited membership in the new owner, all before any `ALTER` runs, so `permission denied for schema <schema>` mid-handover means a grant that passed preflight was revoked afterwards, not a gap the pre-checks miss.
   All three cases are in [Ownership after restore](reference/prerequisites.md#ownership-after-restore-cloneownerafterrestore).
3. Replay the `ALTER ... OWNER TO` statements from the log against the target, connected as the migration role.
   Each one commits on its own, the ones that already ran stay applied, and replaying those is a no-op: `ALTER ... OWNER TO` does nothing when the object already has that owner.
4. Verify with the query below, connected as the migration role. It MUST come back empty.
5. On a live migration, delete the Migration once the handover is done.
   The finalizer runs the cleanup Job, which drops the replication slot and ends the WAL retention it causes on the source.
   A terminal failure does not release the slot on its own, and deleting the Migration is the supported way to release it.

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
Array types and row types show up beside the type or relation they belong to; they follow it and need no statement of their own.

A `canceling statement due to lock timeout` in the log is not a privilege problem: the Job caps each statement and its lock wait at 60 seconds, and a session on the target is holding a conflicting lock.
`ALTER ... OWNER TO` is a catalog update that takes milliseconds, so the cap is there to keep a blocked handover from sitting on the cutover window.
Clear the blocking session and replay.
