# Live migration

`spec.follow.enabled: true` changes the clone into a live migration.
The base copy runs under a replication slot.
Logical replication then streams and applies every change until you cut over.
Start from [05-live-migration.yaml](../examples/05-live-migration.yaml).
Two of the [follow-specific prerequisites](../reference/prerequisites.md#live-migration-specfollowenabled-true) fail silently when they are missing.
Before any data moves, the operator checks the prerequisites that it can reach.
See [preflight](#preflight) below.
Before you enable follow mode, use the [Planning checklist](../planning.md).
With it, agree the operating plan, cutover ownership, recovery criteria, and rehearsal.

The phases of a live migration:

| Phase            | Meaning                                                                                             |
|------------------|-----------------------------------------------------------------------------------------------------|
| `Validating`     | The preflight Job probes connectivity, clone privileges, and follow prerequisites. No worker yet.   |
| `Cloning`        | The base copy runs. Changes already arrive in the work volume.                                      |
| `Streaming`      | The base copy is complete. The worker replays changes onto the target continuously.                 |
| `CutoverPending` | The Migration is caught up (`CaughtUp` condition True) and waits for approval (Manual mode).        |
| `CuttingOver`    | The cutover LSN is frozen. Remaining changes drain. Drain verification and replication cleanup run. |
| `Completed`      | Drain proven complete, sequences synced, slot dropped. It is safe to switch applications.           |

If a worker dies after the replication slot exists and before its table copy ends, the slot's snapshot is lost with the worker.
The next attempt then cannot resume that copy.
Without `spec.clone.dropIfExists`, the Migration fails as [`SnapshotLost`](../troubleshooting.md#phase-failed-with-reason-snapshotlost).
With `spec.clone.dropIfExists`, the operator starts the clone again from the beginning, and the failure costs two attempts ([Retries and resume](lifecycle.md#retries-and-resume)).

## Native candidate observation

The native development helpers accept only native format 8 with metadata 6, or candidate native format 9 with metadata 7.
They refuse mixed pairs, unknown versions, and conversion of existing state.
The candidate reader checks the saved COPY policy and part profile against the table identities.
It checks the exact owned identities and catalog definitions for the checkpoint, part markers, and large-object ledger.
The completed-evidence reader checks the 29-column checkpoint, current generation, snapshot binding, preparation state, and large-object aggregate receipts.
Its marker guard covers both candidate receipt catalogs.

Source telemetry uses the source binding only.
Inventory reports owned resources and does not prove part rows, large-object contents, or migration completion.
Completion still requires checkpoint authority and successful engine comparison.
Candidate helper decoding does not activate native production admission or capabilities.
The M8 runtime qualification gate remains separate from these source checks.

## Preflight

A `<name>-preflight` Job gates the first attempt of every Migration.
A follow migration adds the follow prerequisite checks.
The checks run over plain psql in a fixed order.
Each success prints an `ok:` line in the Job log.

1. Connectivity to both endpoints.
   The Job retries each connect up to six times, ten seconds apart.
   It logs one `retry:` line for each miss.
   A short failover interruption does not fail the Migration.
   A permanent connection error (a wrong password, an unknown role, an unknown database) ends the retries when the next probe repeats it.
   The condition message contains the server's error line.
   The Job needs two of these errors in a row.
   The reason is that a managed proxy can report its own authentication failure when its backend has a short interruption.
2. No PgBouncer in the source or target connection.
   On one session for each side, the Job compares the backend PID in the `\conninfo` of psql 18 with `pg_backend_pid()`.
   The PID in `\conninfo` comes from the cancel key that the connection received.
   PgBouncer issues its own cancel key, so behind PgBouncer the two values differ in every pool mode.
   A direct connection or a TCP proxy forwards the server's key, so the check passes.
   pgpool-II also forwards the key, so this check does not detect pgpool-II.
   Connect the Migration directly to PostgreSQL.
   [Troubleshooting](../troubleshooting.md#a-connection-goes-through-pgbouncer) explains the messages.
   The check also fails when it cannot tell, for example on a custom runner image with psql older than 18.
3. With [`spec.target.synchronousCommit: local`](../configuration.md#synchronous-commit-on-the-target), `show synchronous_commit` must read `local` over the same target URI that the worker uses.
   A connection pooler or proxy that dropped the libpq startup option fails this check.
4. With `spec.preflight.requireSameMajorVersion: true`, the source and target PostgreSQL major versions must match.
   See [prerequisites](../reference/prerequisites.md#major-version-gate).
5. The superuser connection on each side that sets a [`superuserSecretRef`](../reference/prerequisites.md#superuser-remediation-superusersecretref).
   The Job probes it the same way.
   If the role does not have `rolsuper`, the Job logs a `warn:` line, and the remediation continues.
   Managed-Postgres admin roles hold the grant rights without the attribute.
6. The selected source extensions, which must be installed or default-available on the target.
   With `spec.clone.dropIfExists`, their default versions must be installable.
   To bypass this gate, set `spec.clone.skip` to include `extensions`.
7. The clone privileges on the target: CREATE on the database, CREATE on the schemas that the restore targets, and the db-properties ownership probe.
   See [prerequisites](../reference/prerequisites.md#base-clone-every-migration) for the details.
8. The follow prerequisites.
   These are `wal_level`, free replication-slot headroom, `wal_sender_timeout` (a warning only), and the `REPLICATION` attribute of the source role.
   They also include `EXECUTE` on the `pg_replication_origin_*` functions of the target and the `session_replication_role` SET privilege.
   The replica-identity audit of every user table is also a follow prerequisite.
   Two of these lose data and raise no error.

The `Validated` and `Failed` condition messages name the exact `GRANT` or setting that fixes a failed rights check.
When `superuserSecretRef` could have applied it, the message adds a hint that names the field:

```sh
kubectl get pgm billing -o jsonpath='{.status.conditions[?(@.type=="Validated")].message}'
```

With `superuserSecretRef` set, the preflight applies the grantable rights itself and checks them again.
It emits one `PreflightRemediated` event for each tier (clone rights and follow rights), with the applied statements.
A [dry run](lifecycle.md#dry-run) applies none of them and reports them in `PreflightWouldRemediate` events instead.
On success, a `PreflightPassed` event counts the checks and the applied grants.
When the operator can read the Job log, the `warn:` lines of a passing preflight become one `PreflightWarning` event.
The `warn:` lines report these conditions:

- A `superuserSecretRef` role without `rolsuper`.
- A source `wal_sender_timeout` below 60s.
- A migration role without `TEMPORARY` on the target database.
  The test commit of the retry gate needs this privilege.

These warnings never fail the Migration.
The operator keeps the finished preflight Job as an audit trail.
`spec.ttlSecondsAfterFinished` does not apply to this Job, and the Job is deleted together with the Migration.

If you set `spec.suspend` while the gate runs, the operator deletes the preflight Job, and this also stops the remediation.
On resume, the gate runs the preflight again.
While the gate runs, the `Validated` condition is `Unknown` with reason `PreflightRunning`.
If the preflight pod cannot start, the condition message shows the kubelet's reason verbatim.
This path applies to a misnamed Secret and to an unschedulable node.

Each connection times out after 10 seconds, and the full preflight times out after 30 minutes.
A black-holed endpoint fails and does not hang.
`PGCONNECT_TIMEOUT` sets that connect timeout on every operator control Job.
It never sets the timeout on the pgcopydb worker, because the data path of the worker must not race a connect cap.

A preflight failure is terminal.
These failures are configuration errors on the databases, so a retry of the Migration cannot fix them.
The failure message contains the end of the preflight log.
When the operator cannot read that log, it retries the read for two minutes after the Job fails.
Thus a short API server outage does not cost the verdict.
Fix the endpoint.
Then create a new Migration.
The preflight does not check the workload contract: no DDL during the migration, no large-object changes, the presence of wal2json.
That contract stays your responsibility.

## Watching the stream

The operator samples `status.replication` from the source, not from the worker.
The sample is one row that joins the replication slot to `pg_stat_replication`.
The operator fills the field as soon as the slot answers, and the slot answers during the base copy.
The operator uses the field for catch-up and cutover only after `CloneCompleted` is True.
Nothing in that path opens pgcopydb's own catalogs, because a read of those catalogs while the copy writes them kills the worker.
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

`writeLSN` reports the receive progress from the walsender, or the slot's `confirmed_flush_lsn` as a fallback.
`replayLSN` is the replay position of the walsender, or the slot's `confirmed_flush_lsn` where the migration role may not read the walsender.
The bundled runner, pgcopydb `0.18.144.g86bf908`, runs target COMMITs with `synchronous_commit=on` and checks the result of each COMMIT before it reports their replay progress.
On a target with synchronous standbys, each of those COMMITs waits until a standby acknowledges it, with or without [`spec.target.synchronousCommit`](../configuration.md#synchronous-commit-on-the-target).
`CommitsBlocked` becomes True when two samples in a row show a wait of 30 seconds or more.
See [Commits blocked on synchronous replication](../troubleshooting.md#commits-blocked-on-synchronous-replication).
If a proxy cuts the apply connection during such a wait, its backend still waits on the target and holds the replication origin.
The next attempt resumes only when the [retry gate](lifecycle.md#retries-and-resume) finds that this session is gone and that a standby acknowledged a test commit.

When published tables are idle, genuine primary keepalives from the current connection can [advance certified network replay and flush feedback](https://github.com/ydixken/pgcopydb/blob/86bf908adca02d87063849dae775a02c1659c829/src/bin/pgcopydb/ld_stream.c#L1628-L1653) across WAL outside the publication.
That feedback does not move the target replication origin or the sentinel's data replay cursor.
`replayLSN` is therefore not necessarily the LSN of the last applied data transaction.
See [Follow diagnostics](../design/follow-diagnostics.md) for the conditions.
Other supported runner versions differ.
See [client tool versions](../reference/prerequisites.md#client-tool-versions).
After cutover, when the target replication origin is exactly on the frozen endpos, the drain verification proves this.
Where the origin is not on endpos, the drain verification compares content.
In the first case, the proof depends only on the origin bookkeeping of the runner.

> [!warning]
> Runners older than `0.18.70.gbb8dbfc` can lose committed source changes during follow without an error.
> They skip a transaction that began before another one committed ([#356](https://github.com/ydixken/pgcopydb-operator/issues/356)).
> They keep one row for each heap page of a source `COPY` ([#357](https://github.com/ydixken/pgcopydb-operator/issues/357)).
> Under `pgoutput`, they truncate only the first table of a multi-table `TRUNCATE` ([#358](https://github.com/ydixken/pgcopydb-operator/issues/358)).
> After an apply crash or a failed `COMMIT` on a PostgreSQL 16 or newer target, the next run can skip a transaction.
> The skipped transaction is the one that did not commit ([#359](https://github.com/ydixken/pgcopydb-operator/issues/359), fixed by [fork PR #23](https://github.com/ydixken/pgcopydb/pull/23)).
> Runners older than `0.18.98.g944abcc` can also skip the committed transactions that stay in an `output.db` after receive closes it.
> This can occur when apply falls behind receive across a file rotation during streaming.
> An example is a backlog after a long clone ([#376](https://github.com/ydixken/pgcopydb-operator/issues/376), fixed by [fork PR #31](https://github.com/ydixken/pgcopydb/pull/31)).
> In each case, the origin moves past the lost change.
> Runners older than `0.18.104.g362a559` can also copy the remaining tables of a retried live migration without the snapshot of the replication slot.
> Then apply can stop on a duplicate key, or the target can diverge without an error ([#378](https://github.com/ydixken/pgcopydb-operator/issues/378), fixed by [fork PR #32](https://github.com/ydixken/pgcopydb/pull/32)).
> At cutover, the drain verification passes without a content compare only when the origin lands exactly on endpos.
> Otherwise, the compare refuses the cutover as [`DrainIncomplete`](../troubleshooting.md#phase-failed-with-reason-drainincomplete).
> With such a runner, set [`spec.verification.data`](verification.md).
> Then a content compare runs after cutover, even when the origin lands on endpos.
> The bundled runner still has [narrow windows](../design/follow-diagnostics.md#what-the-snapshot-can-distinguish) in which a transaction can be lost while the origin lands on endpos.
> In one window, a target backend is killed after the origin setup and before the `COMMIT` record.
> In the other window, a `COMMIT` breaks the connection.
> `spec.verification.data` covers these windows too.

`lagBytes` is the distance from the current WAL head of the source.
The `CaughtUp` condition becomes True when two consecutive samples put the lag at or below `follow.maxCatchupLag`, 16Mi by default.
With ongoing writes, the condition may flap.
With the bundled runner, an idle publication needs neither heartbeat INSERTs nor a higher `maxCatchupLag` to cross filtered WAL.
Catch-up does not replace endpos drain verification, and it does not prove that source writers have stopped.

`pg_read_all_stats` on the source role of the migration is optional, and it makes both LSN readings more precise.
Without the grant, PostgreSQL blanks the walsender columns in `pg_stat_replication` for that role, including its own row.
`writeLSN` and `replayLSN` then both fall back to the slot's `confirmed_flush_lsn`.
This position is one confirmation behind, and the two readings are identical.
The apply backlog derived from them reads zero.
Lag and `CaughtUp` follow `replayLSN`, so they inherit the reading that is available.

## Manual cutover runbook

Manual is the default mode.
Cutover freezes the stream at the current LSN of the source.
A write after that instant never reaches the target.

Manual approval arms the cutover.
The cutover starts only when the two-sample `CaughtUp` verdict of the controller is true.
If you approve early, the Migration stays in `Streaming` while that verdict is false, and endpos stays unset.
The first qualifying sample is part of that window.
The second qualifying sample starts cutover without another approval edit.
Approval does not stop source writes or freeze the stream while catch-up is pending.

> [!warning]
> Operators MUST stop source writes before cutover freezes the stream, in both Manual and Automatic modes.
> The operator does not fence source writes, and it does not end source sessions.

1. Wait for `CaughtUp` to be True.

    ```sh
    kubectl wait pgm/billing --for=condition=CaughtUp
    ```

    The command returns up to one poll interval (about 10 seconds) after the lag drops.
    The reason is that the condition waits for a second qualifying sample.

2. Stop writes to the source.
    Stop the application or revoke its access.

3. Approve the cutover.

    ```sh
    kubectl patch pgm billing --type=merge -p '{"spec":{"cutover":{"approved":true}}}'
    ```

4. When the approval and the two-sample `CaughtUp` verdict both hold, the operator sets the cutover LSN with pgcopydb `sentinel set endpos --current`.
    The worker then drains the remaining changes, syncs sequences, and exits.
    The phase is `CuttingOver`.

5. A drain verification Job (`<name>-verify`) proves the drain on the target.
    The exit code of the worker is not the proof.
    Only that proof sets `CutoverCompleted`.
    When the Job refutes the drain, the Migration fails instead as `DrainIncomplete`.
    [Troubleshooting](../troubleshooting.md) describes this failure.

    The fast path passes only when the target's replication origin is exactly on the cutover LSN.
    When any distance remains, the content decides, never the size of the distance.
    Nearly every cutover takes the compare path.
    Size the write-downtime window for `compare data` over the selected storage tables.
    A [rehearsal](verification.md#rehearsal) measures the duration of that compare while the migration still streams.
    Treat the exact-LSN pass as the exception.

    The Job runs `pgcopydb compare data` and checks the report that `--json` prints.
    It does not rely on the exit status alone.
    The compare passes only when the report includes every migrated table and each table matches on row count and checksum.
    With the bundled runner, each row's `status` must also be `match`.
    The result is a refusal, not a pass, when the compare could not run or when the Job cannot read the report.

6. A cleanup Job (`<name>-cleanup`) drops the replication slot, the auto-created publication, and the target origin.
    After any requested [post-transfer verification](verification.md), `Complete` becomes True and the phase is `Completed`.

7. Connect the application to the target.

During `CuttingOver`, the operator emits a logical message on the source with `pg_logical_emit_message` on every pass.
Some runner versions need new WAL to see a newly set endpos.
The message carries no table data, needs no special privilege, and changes nothing that users can see.
This endpos nudge runs only after cutover starts, so it cannot unblock a Migration that waits for `CaughtUp`.
Certified keepalive feedback handles idle catch-up before the operator sets endpos.

Under load, the end-to-end suite checks that an application may run throughout the copy and the stream and lose no committed transaction.
Further specs require the target to match the content of the source after these events:

- Overlapping source transactions.
- A walsender restart that makes receive get a large transaction again.
- A `COMMIT` that the target refused once.

The guarantee stops at the freeze, because the source is silent from step 2 onward.
[Automatic mode](#automatic-mode) below carries the same warning for the mode that skips approval.

## Automatic mode

`cutover.mode: Automatic` skips approval and starts cutover when the same two-sample `CaughtUp` verdict first becomes True.
Use it only when the source is already quiesced (a decommissioned system, a maintenance window that started before the Migration).
When the source still takes writes, "caught up" is a moving target crossed at an arbitrary moment.
Every write after the freeze is then lost to the target.
When in doubt, use Manual.
