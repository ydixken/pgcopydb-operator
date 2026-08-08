# Live migration

`spec.follow.enabled: true` turns the clone into a live migration: the base copy runs under a replication slot, then logical replication streams and applies every change until you cut over. Start from [migration-follow.yaml](../examples/migration-follow.yaml). The [follow-specific prerequisites](../reference/prerequisites.md#live-migration-specfollowenabled-true) (wal_level, REPLICATION attribute, target grants, replica identities) are strict, and two of them fail silently when missed, so the operator checks the ones it can reach before any data moves (see [preflight](#preflight) below).

The phases of a live migration:

| Phase            | Meaning                                                                                        |
|------------------|------------------------------------------------------------------------------------------------|
| `Validating`     | Preflight Job probing the replication prerequisites; no worker has started yet.                 |
| `Cloning`        | Base copy running; changes are already being received into the work volume.                     |
| `Streaming`      | Base copy done; changes are replayed onto the target continuously.                              |
| `CutoverPending` | Caught up (`CaughtUp` condition True) and waiting for approval (Manual mode).                   |
| `CuttingOver`    | Cutover LSN frozen; draining remaining changes, verifying the drain, cleaning up replication.   |
| `Completed`      | Drain proven complete, sequences synced, slot dropped. Safe to switch applications.             |

## Preflight

The first attempt of a follow migration is gated by a `<name>-preflight` Job. It probes, over plain psql, what the databases must provide: `wal_level`, free replication-slot headroom, the source role's `REPLICATION` attribute, `EXECUTE` on the target's `pg_replication_origin_*` functions, and the `session_replication_role` SET privilege. Every one of these has failed a live run, and the last two lose data quietly rather than loudly.

A failed check names the exact `GRANT` or setting that fixes it, in the `Validated` and `Failed` condition messages:

```sh
kubectl get pgm billing -o jsonpath='{.status.conditions[?(@.type=="Validated")].message}'
```

Preflight failure is terminal: these are configuration errors on the databases, so retrying the Migration cannot fix them. Fix the endpoint, then create a new Migration. The schema and workload contract (replica identity, no DDL during the migration) is NOT preflighted and stays your responsibility.

## Watching the stream

`status.replication` mirrors the pgcopydb sentinel while the worker runs:

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
5. The operator does not trust the worker's exit code: a verify Job (`<name>-verify`) proves on the target that the replication origin reached the cutover LSN, within one WAL page (the origin parks at the last applied commit record, which trails the LSN by a few non-data bytes even on a quiesced source). Only that proof sets `CutoverCompleted`. A refuted drain fails the Migration instead (see `DrainIncomplete` in [troubleshooting](../troubleshooting.md)).
6. A cleanup Job (`<name>-cleanup`) drops the replication slot, the auto-created publication, and the target origin. Then `Complete` goes True, phase `Completed`.
7. Point the application at the target.

## Automatic mode

`cutover.mode: Automatic` skips the approval: the operator cuts over the moment `CaughtUp` first goes True. Use it only when the source is already quiesced (a decommissioned system, a maintenance window that started before the Migration). Against a source still taking writes, "caught up" is a moving target crossed at an arbitrary moment, and every write after the freeze is lost to the target. When in doubt, use Manual.
