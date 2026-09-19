# Live migration

`spec.follow.enabled: true` turns the clone into a live migration.
The base copy runs under a replication slot.
Logical replication then streams and applies every change until you cut over.
Start from [05-live-migration.yaml](../examples/05-live-migration.yaml).
Two of the [follow-specific prerequisites](../reference/prerequisites.md#live-migration-specfollowenabled-true) fail silently when missed.
The operator checks the ones it can reach before any data moves; see [preflight](#preflight) below.
Before you enable follow mode, use the [Planning checklist](../planning.md) to agree the operating plan, cutover ownership, recovery criteria, and rehearsal.

The phases of a live migration:

| Phase            | Meaning                                                                                        |
|------------------|------------------------------------------------------------------------------------------------|
| `Validating`     | Preflight Job probing connectivity, clone privileges, and follow prerequisites; no worker yet. |
| `Cloning`        | Base copy running; changes are already being received into the work volume.                    |
| `Streaming`      | Base copy done; changes are replayed onto the target continuously.                             |
| `CutoverPending` | Caught up (`CaughtUp` condition True) and waiting for approval (Manual mode).                  |
| `CuttingOver`    | Cutover LSN frozen; draining remaining changes, verifying the drain, cleaning up replication.  |
| `Completed`      | Drain proven complete, sequences synced, slot dropped; safe to switch applications.            |

## Preflight

A `<name>-preflight` Job gates every Migration's first attempt.
A follow migration adds the follow prerequisite checks.
The checks run over plain psql in a fixed order.
Each success prints an `ok:` line in the Job log.

1. Connectivity to both endpoints.
   Each connect is retried up to six times, ten seconds apart, with one logged `retry:` line per miss.
   A failover blip does not fail the Migration.
   A permanent connection error ends the retries once the next probe repeats it: a wrong password, an unknown role, an unknown database.
   The server's error line lands in the condition message.
   It takes two in a row because a pooler with `auth_query` reports an authentication failure of its own when its backend blips.
2. The superuser connection on each side that sets a [`superuserSecretRef`](../reference/prerequisites.md#superuser-remediation-superusersecretref).
   The Job probes it the same way.
   If the role does not have `rolsuper`, the Job logs a warning and remediation proceeds.
   Managed-Postgres admin roles hold the grant rights without the attribute.
3. The selected source extensions, which must be installed or default-available on the target.
   With `spec.clone.dropIfExists`, their default versions must be installable.
   To bypass this gate, set `spec.clone.skip` to include `extensions`.
4. The clone privileges on the target: CREATE on the database, CREATE on the schemas the restore targets, and the db-properties ownership probe.
   See [prerequisites](../reference/prerequisites.md#base-clone-every-migration) for the details.
5. The follow prerequisites: `wal_level`, free replication-slot headroom, the source role's `REPLICATION` attribute, `EXECUTE` on the target's `pg_replication_origin_*` functions, the `session_replication_role` SET privilege, and the replica-identity audit of every user table.
   Two of these lose data and raise no error.

The `Validated` and `Failed` condition messages name the exact `GRANT` or setting that fixes a failed rights check.
When `superuserSecretRef` could have applied it, the message adds a hint that names the field:

```sh
kubectl get pgm billing -o jsonpath='{.status.conditions[?(@.type=="Validated")].message}'
```

With `superuserSecretRef` set, the preflight applies the grantable rights itself and re-checks them.
It emits one `PreflightRemediated` event per tier, clone rights and follow rights, with the applied statements.
On success a `PreflightPassed` event counts the checks and the applied grants.
The operator keeps the finished preflight Job as an audit trail: `spec.ttlSecondsAfterFinished` does not apply to it, and it is removed with the Migration.

If you set `spec.suspend` while the gate runs, the operator deletes the preflight Job and stops remediation with it.
The gate re-runs the preflight on resume.
While the gate runs, the `Validated` condition is `Unknown` with reason `PreflightRunning`.
If the preflight pod cannot start, the kubelet's reason lands verbatim in the condition message.
A misnamed Secret, an unbound work PVC, and an unschedulable node all reach that path.

Connections time out after 10 seconds, and the whole preflight after 30 minutes.
A black-holed endpoint fails rather than hangs.
`PGCONNECT_TIMEOUT` sets that connect timeout on every operator control Job, never on the pgcopydb worker, whose data path must not race a connect cap.

Preflight failure is terminal: these are configuration errors on the databases, so retrying the Migration cannot fix them.
Fix the endpoint.
Then create a new Migration.
The preflight does not check the workload contract: no DDL during the migration, no large-object changes, wal2json presence.
That contract stays your responsibility.

## Watching the stream

The operator samples `status.replication` from the source rather than from the worker: one row that joins the replication slot to `pg_stat_replication`.
It fills in as soon as the slot answers, which is during the base copy.
The operator acts on it for catch-up and cutover only once `CloneCompleted` is True.
Nothing in that path opens pgcopydb's own catalogs, because a read of those while the copy writes them kills the worker.
See the [upstream drafts](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/upstream-issues.md) for the detail.

```sh
kubectl get pgm billing -o jsonpath='{.status.replication}' | jq
```

```json
{
  "slotName": "pgcopydb_billing_billing_1a2b3c4d",
  "writeLSN": "0/5B2C6E70",
  "replayLSN": "0/5B2C6E70",
  "lagBytes": 0
}
```

`writeLSN` reports receive progress from the walsender, or the slot's `confirmed_flush_lsn` as a fallback.
`replayLSN` is the walsender's replay position, or the slot's `confirmed_flush_lsn` where the migration role may not read the walsender.
The bundled runner, pgcopydb `0.18.15.gea2dc96`, confirms target COMMITs with `synchronous_commit=on` before it reports their replay progress.

When published tables are idle, genuine primary keepalives from the current connection can [advance certified network replay and flush feedback](https://github.com/ydixken/pgcopydb/blob/ea2dc96a47c2f7676d71a4967d044a1e469e4110/src/bin/pgcopydb/ld_stream.c#L1521-L1546) across WAL outside the publication.
That feedback does not move the target replication origin or the sentinel's data replay cursor.
`replayLSN` is therefore not necessarily the LSN of the last applied data transaction.
See [Follow diagnostics](../design/follow-diagnostics.md) for the conditions.
Other supported runner versions differ; see [client tool versions](../reference/prerequisites.md#client-tool-versions).
The drain verification after cutover still proves that the target applied everything through the frozen endpos.

`lagBytes` is the distance from the source's current WAL head.
The `CaughtUp` condition goes True once two consecutive samples put the lag at or below `follow.maxCatchupLag`, 16Mi by default.
With ongoing writes it may flap.
With the bundled runner, an idle publication needs neither heartbeat INSERTs nor a higher `maxCatchupLag` to cross filtered WAL.
Catch-up does not replace endpos drain verification or prove that source writers have stopped.

`pg_read_all_stats` on the migration's source role is optional, and it sharpens both LSN readings.
Without the grant, PostgreSQL blanks the walsender columns in `pg_stat_replication` for that role, its own row included.
`writeLSN` and `replayLSN` then both fall back to the slot's confirmed flush position: one confirmation behind, and identical to each other.
The apply backlog derived from them reads zero.
Lag and `CaughtUp` follow `replayLSN`, so they inherit whichever reading is available.

## Manual cutover runbook

Manual is the default mode.
Cutover freezes the stream at the source's current LSN: anything written after that instant never reaches the target.

Manual approval arms cutover, which starts only when the controller's two-sample `CaughtUp` verdict is true.
If you approve early, the Migration stays in `Streaming` while that verdict is false, and endpos stays unset.
The first confirming sample is part of that window.
The second qualifying sample starts cutover without another approval edit.
Approval does not stop source writes or freeze the stream while catch-up is pending.

> [!warning]
> Operators MUST stop source writes before cutover freezes the stream, in both Manual and Automatic modes.
> The operator does not fence source writes or terminate source sessions.

1. Wait for `CaughtUp` to be True.

    ```sh
    kubectl wait pgm/billing --for=condition=CaughtUp
    ```

    It returns up to one poll interval, about 10 seconds, after the lag drops, because the condition waits for a second confirming sample.

2. Stop writes to the source.
    Stop the application or revoke its access.

3. Approve the cutover.

    ```sh
    kubectl patch pgm billing --type=merge -p '{"spec":{"cutover":{"approved":true}}}'
    ```

4. Once approval and confirmed catch-up both hold, the operator sets the cutover LSN with pgcopydb `sentinel set endpos --current`.
    The worker then drains the remaining changes, syncs sequences, and exits.
    The phase is `CuttingOver`.

5. A verify Job (`<name>-verify`) proves the drain on the target rather than the worker's exit code.
    Only that proof sets `CutoverCompleted`.
    A refuted drain fails the Migration instead, as `DrainIncomplete` in [troubleshooting](../troubleshooting.md) describes.

    The fast path passes only when the target's replication origin sits exactly on the cutover LSN.
    Any remaining distance is decided by content, never by its size.
    Nearly every cutover takes the compare path.
    Size the write-downtime window for a `compare data` over the whole database.
    Treat the exact-LSN pass as the exception.

    The Job runs `pgcopydb compare data` and validates the report `--json` prints, instead of the exit status alone.
    The compare passes only when the report accounts for every migrated table, and each one matches on row count and checksum.
    A compare that could not run, and a report the Job cannot read, refuse rather than pass.

6. A cleanup Job (`<name>-cleanup`) drops the replication slot, the auto-created publication, and the target origin.
    Then `Complete` goes True and the phase is `Completed`.

7. Point the application at the target.

During `CuttingOver`, the operator emits a logical message on the source with `pg_logical_emit_message` on every pass.
Some worker versions need new WAL to see a freshly set endpos.
The message carries no table data, needs no special privilege, and changes nothing user-visible.
This endpos nudge runs only after cutover starts, so it cannot unblock a Migration that waits for `CaughtUp`.
Certified keepalive feedback handles idle catch-up before endpos is set.

The end-to-end suite verifies under load that an application may run throughout the copy and the stream and lose no committed transaction.
The guarantee stops at the freeze, because the source is silent from step 2 onward.
[Automatic mode](#automatic-mode) below carries the same warning for the mode that skips approval.

## Automatic mode

`cutover.mode: Automatic` skips approval and starts cutover when the same two-sample `CaughtUp` verdict first goes True.
Use it only when the source is already quiesced (a decommissioned system, a maintenance window that started before the Migration).
Against a source still taking writes, "caught up" is a moving target crossed at an arbitrary moment, and every write after the freeze is lost to the target.
When in doubt, use Manual.
