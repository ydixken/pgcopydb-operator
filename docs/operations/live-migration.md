# Live migration

`spec.follow.enabled: true` turns the clone into a live migration: the base copy runs under a replication slot, then logical replication streams and applies every change until you cut over. Start from [migration-follow.yaml](../examples/migration-follow.yaml). The [follow-specific prerequisites](../reference/prerequisites.md#live-migration-specfollowenabled-true) (wal_level, REPLICATION attribute, target grants, replica identities) are strict, and two of them fail silently when missed, so the operator checks the ones it can reach before any data moves (see [preflight](#preflight) below).

The phases of a live migration:

| Phase            | Meaning                                                                                        |
|------------------|------------------------------------------------------------------------------------------------|
| `Validating`     | Preflight Job probing connectivity and the replication prerequisites; no worker has started yet. |
| `Cloning`        | Base copy running; changes are already being received into the work volume.                     |
| `Streaming`      | Base copy done; changes are replayed onto the target continuously.                              |
| `CutoverPending` | Caught up (`CaughtUp` condition True) and waiting for approval (Manual mode).                   |
| `CuttingOver`    | Cutover LSN frozen; draining remaining changes, verifying the drain, cleaning up replication.   |
| `Completed`      | Drain proven complete, sequences synced, slot dropped. Safe to switch applications.             |

## Preflight

Every Migration's first attempt is gated by a `<name>-preflight` Job; for a follow migration it carries the full battery.
The checks run over plain psql, in a fixed order, and each success prints an `ok:` line in the Job log, so the log reads as an audit trail.
Connectivity to both endpoints always comes first, and each connect is retried up to six times ten seconds apart, one logged `retry:` line per miss, so a failover blip does not fail an otherwise sound Migration.
Then, per side with a [`superuserSecretRef`](../reference/prerequisites.md#superuser-remediation-superusersecretref), the superuser connection is probed the same way; a role without `rolsuper` only logs a warning and remediation proceeds, since managed-Postgres admin roles hold the grant rights without the attribute.
Then the follow prerequisites: `wal_level`, free replication-slot headroom, the source role's `REPLICATION` attribute, `EXECUTE` on the target's `pg_replication_origin_*` functions, the `session_replication_role` SET privilege, and the replica-identity audit of every user table.
Every one of these has failed a live run, and two lose data quietly rather than loudly.

A failed rights check names the exact `GRANT` or setting that fixes it, in the `Validated` and `Failed` condition messages, and adds a hint naming `superuserSecretRef` when that field could have applied it:

```sh
kubectl get pgm billing -o jsonpath='{.status.conditions[?(@.type=="Validated")].message}'
```

With `superuserSecretRef` set, the preflight applies the grantable rights itself, re-checks them, and emits one `PreflightRemediated` event per applied statement; on success a `PreflightPassed` event counts checks and applied grants.
The finished preflight Job is kept as that audit trail: `spec.ttlSecondsAfterFinished` does not apply to it, and it is removed with the Migration.
Setting `spec.suspend` while the gate runs deletes the preflight Job, stopping remediation with it; the gate re-runs the preflight on resume.

While the gate runs, the `Validated` condition is `Unknown` with reason `PreflightRunning`; if the preflight pod cannot start (a misnamed Secret, an unbound work PVC, an unschedulable node), the kubelet's reason lands verbatim in the condition message.
The Job is bounded: connections time out after 10 seconds (`PGCONNECT_TIMEOUT`, set on every operator control Job but never on the pgcopydb worker, whose data path must not race a connect cap) and the whole preflight after 30 minutes, so a black-holed endpoint fails instead of waiting forever.

Preflight failure is terminal: these are configuration errors on the databases, so retrying the Migration cannot fix them. Fix the endpoint, then create a new Migration. The workload contract (no DDL during the migration, no large-object changes, wal2json presence) is not preflighted and stays your responsibility.

## Watching the stream

`status.replication` mirrors the pgcopydb sentinel from the moment streaming starts. During the base copy it stays empty on purpose: on pgcopydb 0.18 a sentinel read opens the SQLite catalogs the copy is writing to, and concurrent access can crash workers, so the operator derives the copy phase from the worker log and only starts sentinel reads once `CloneCompleted` is True.

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

`writeLSN` is the last position received from the source, `replayLSN` the last transaction durably applied to the target, `lagBytes` the distance from the source's current WAL head. The `CaughtUp` condition goes True once the lag is at or below `follow.maxCatchupLag` (16Mi by default); with ongoing writes it may flap, which is fine.

## Manual cutover runbook

Manual is the default mode. Cutover freezes the stream at the source's current LSN: anything written after that instant never reaches the target. So the order matters.

1. Wait for `CaughtUp` to be True (`kubectl wait pgm/billing --for=condition=CaughtUp`).
2. Stop writes to the source (stop the application, revoke access, whatever your setup calls quiescing).
3. Approve: `kubectl patch pgm billing --type=merge -p '{"spec":{"cutover":{"approved":true}}}'`.
4. The operator sets the cutover LSN (pgcopydb `sentinel set endpos --current`); the worker drains the remaining changes, syncs sequences, and exits. Phase: `CuttingOver`.
5. The operator does not trust the worker's exit code: a verify Job (`<name>-verify`) proves the drain on the target. The fast path passes when the replication origin reached the cutover LSN within one WAL page (the origin parks at the last applied commit record). A larger distance is not treated as loss, because on an idle source the remaining WAL is publication-filtered traffic (autovacuum, catalog churn) that pgcopydb never applies; the Job then runs `pgcopydb compare data` and passes only when every migrated table matches, which can add time to the window after an idle wait. Only that proof sets `CutoverCompleted`. A refuted drain fails the Migration instead (see `DrainIncomplete` in [troubleshooting](../troubleshooting.md)).
6. A cleanup Job (`<name>-cleanup`) drops the replication slot, the auto-created publication, and the target origin. Then `Complete` goes True, phase `Completed`.
7. Point the application at the target.

An idle source needs no extra care from you. pgcopydb 0.18 only checks the cutover LSN against WAL it receives, and a source with zero writes sends none, which would leave the drain waiting forever. So while the phase is `CuttingOver` the operator emits a tiny logical message on the source (`pg_logical_emit_message`) on every pass: one WAL record for the stream to deliver, letting the worker reach the LSN promptly. The message carries no data, needs no special privilege, and changes nothing user-visible.

## Automatic mode

`cutover.mode: Automatic` skips the approval: the operator cuts over the moment `CaughtUp` first goes True. Use it only when the source is already quiesced (a decommissioned system, a maintenance window that started before the Migration). Against a source still taking writes, "caught up" is a moving target crossed at an arbitrary moment, and every write after the freeze is lost to the target. When in doubt, use Manual.
