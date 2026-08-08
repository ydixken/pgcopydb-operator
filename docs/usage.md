# Usage

How to run migrations with the operator: install, a first clone, a live migration end to end, and what to do when something breaks. What your PostgreSQL endpoints must provide beforehand (privileges, WAL settings, replica identities) lives in [PREREQUISITES.md](../PREREQUISITES.md); read it first, most of the failures in the troubleshooting table trace back to it.

## Install

```sh
helm install pgcopydb-operator oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
  --namespace pgcopydb-system --create-namespace
```

The chart installs the `Migration` CRD, the controller manager, and its RBAC. Values reference: [chart README](../charts/pgcopydb-operator/README.md). Check it is up:

```sh
kubectl get crd migrations.pgcopydb-operator.io
kubectl -n pgcopydb-system get deploy
```

## A first clone

A `Migration` without `follow` is a one-shot bulk copy: schema, data, indexes, constraints, sequences. Put the passwords in Secrets in the Migration's namespace, then apply the minimal example ([docs/examples/migration-minimal.yaml](examples/migration-minimal.yaml)):

```sh
kubectl apply -f docs/examples/migration-minimal.yaml
kubectl get pgm -w
```

```text
NAME   PHASE      TABLES   ATTEMPTS   AGE
shop   Cloning    12       1          40s
shop   Completed  31       1          3m
```

What happens behind the phases:

- The operator creates a work PVC (`<name>-work`, 10Gi by default) and one worker Job per attempt (`<name>-run-<attempt>`), running `pgcopydb clone` from the runner image.
- `status.progress` mirrors pgcopydb's own progress (tables, indexes, bytes), polled from the running pod. `status.conditions` are authoritative; `phase` is a summary for the printer column.
- `Completed` means pgcopydb finished: data copied under one consistent snapshot, sequences synced. The source is left untouched; a plain clone needs no replication privilege and leaves nothing behind on either database.

Tuning (parallelism, same-table splitting, filters, skips) is the `clone` block; see [docs/examples/migration-tuned.yaml](examples/migration-tuned.yaml). DBaaS endpoints with a provider-issued DSN: [docs/examples/migration-dsn-secret.yaml](examples/migration-dsn-secret.yaml).

The manager also exports Prometheus metrics per Migration (`pgcopydb_migration_phase`, `_attempts`, `_tables_done`, `_tables_total`, `_indexes_done`, `_indexes_total`, `_replication_lag_bytes`, `_verified`) on HTTPS :8443; `metrics.serviceMonitor.enabled=true` in the chart wires them into the Prometheus Operator.

## Live migration

`spec.follow.enabled: true` turns the clone into a live migration: the base copy runs under a replication slot, then logical replication streams and applies every change until you cut over. Start from [docs/examples/migration-follow.yaml](examples/migration-follow.yaml). The follow-specific prerequisites (wal_level, REPLICATION attribute, target grants, replica identities) are strict, and two of them fail silently when missed, so the operator checks the ones it can reach before any data moves (see [preflight](#preflight) below).

The phases of a live migration:

| Phase            | Meaning                                                                                        |
|------------------|------------------------------------------------------------------------------------------------|
| `Validating`     | Preflight Job probing the replication prerequisites; no worker has started yet.                 |
| `Cloning`        | Base copy running; changes are already being received into the work volume.                     |
| `Streaming`      | Base copy done; changes are replayed onto the target continuously.                              |
| `CutoverPending` | Caught up (`CaughtUp` condition True) and waiting for approval (Manual mode).                   |
| `CuttingOver`    | Cutover LSN frozen; draining remaining changes, verifying the drain, cleaning up replication.   |
| `Completed`      | Drain proven complete, sequences synced, slot dropped. Safe to switch applications.             |

### Preflight

The first attempt of a follow migration is gated by a `<name>-preflight` Job. It probes, over plain psql, what the databases must provide: `wal_level`, free replication-slot headroom, the source role's `REPLICATION` attribute, `EXECUTE` on the target's `pg_replication_origin_*` functions, and the `session_replication_role` SET privilege. Every one of these has failed a live run, and the last two lose data quietly rather than loudly.

A failed check names the exact `GRANT` or setting that fixes it, in the `Validated` and `Failed` condition messages:

```sh
kubectl get pgm billing -o jsonpath='{.status.conditions[?(@.type=="Validated")].message}'
```

Preflight failure is terminal: these are configuration errors on the databases, so retrying the Migration cannot fix them. Fix the endpoint, then create a new Migration. The schema and workload contract (replica identity, no DDL during the migration) is NOT preflighted and stays your responsibility.

### Watching the stream

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

### Manual cutover runbook

Manual is the default mode. Cutover freezes the stream at the source's current LSN: anything written after that instant never reaches the target. So the order matters.

1. Wait for `CaughtUp` to be True (`kubectl wait pgm/billing --for=condition=CaughtUp`).
2. Stop writes to the source (stop the application, revoke access, whatever your setup calls quiescing).
3. Approve: `kubectl patch pgm billing --type=merge -p '{"spec":{"cutover":{"approved":true}}}'`.
4. The operator sets the cutover LSN (pgcopydb `sentinel set endpos --current`); the worker drains the remaining changes, syncs sequences, and exits. Phase: `CuttingOver`.
5. The operator does not trust the worker's exit code: a verify Job (`<name>-verify`) proves on the target that the replication origin reached the cutover LSN, within one WAL page (the origin parks at the last applied commit record, which trails the LSN by a few non-data bytes even on a quiesced source). Only that proof sets `CutoverCompleted`. A refuted drain fails the Migration instead (see `DrainIncomplete` in troubleshooting).
6. A cleanup Job (`<name>-cleanup`) drops the replication slot, the auto-created publication, and the target origin. Then `Complete` goes True, phase `Completed`.
7. Point the application at the target.

### Automatic mode

`cutover.mode: Automatic` skips the approval: the operator cuts over the moment `CaughtUp` first goes True. Use it only when the source is already quiesced (a decommissioned system, a maintenance window that started before the Migration). Against a source still taking writes, "caught up" is a moving target crossed at an arbitrary moment, and every write after the freeze is lost to the target. When in doubt, use Manual.

## Verification

`spec.verification` runs `pgcopydb compare` after the migration completes, one Job per enabled check ([docs/examples/migration-verified.yaml](examples/migration-verified.yaml)):

```yaml
spec:
  verification:
    schema: true   # compares both catalogs
    data: true     # reads every row on both sides
```

Both are opt-in because neither is free: schema refetches both catalogs, data reads the whole database twice. Results land in the `Verified` condition (`SchemaMismatch` or `DataMismatch`) and the `pgcopydb_migration_verified` metric, with phase `Verifying` while the checks run.

A mismatch reports, it does not fail the Migration. The data is already on the target by then, so failing would misstate what happened; and after a live cutover, writes that reached the target are indistinguishable from genuine differences. Read the condition and the compare Job logs, then decide.

For follow migrations the checks run last, after the drain is verified and the slot is dropped: comparing a still-streaming target mismatches by design. Quiesce the target before trusting a data compare.

## Suspend

`spec.suspend: true` deletes the worker Job (foreground, so pgcopydb receives SIGTERM and shuts down cleanly) and keeps the work volume; phase becomes `Suspended`. Setting it back to false starts the next attempt, which resumes from the work-dir catalogs.

Two things to know:

- **WAL retention warning**: suspending a live migration does NOT drop the replication slot. The slot keeps retaining WAL on the source for as long as the Migration is suspended, without bound; the operator emits a `SlotRetained` warning event to that effect. Watch source disk, and do not park live migrations for days. Deleting the Migration drops the slot.
- Each resume consumes one attempt from the `backoffLimit` budget.

## Retries and resume

`spec.backoffLimit` is the number of retries, so `backoffLimit + 1` attempts total (default 4). Each attempt is a fresh Job with Kubernetes-level retries disabled: the operator owns the retry policy, so attempt counts and failure reasons live on the Migration, not buried in Job internals.

- Attempt 1 runs with `--restart`: any state on the work volume is foreign (a fresh Migration never has prior state of its own) and is wiped.
- Retries run with `--resume --not-consistent`: tables and indexes already recorded done in pgcopydb's catalogs are skipped, interrupted copies restart. `--not-consistent` is required because the failed attempt's snapshot died with its process; the retry copies remaining tables under a new snapshot.
- A worker Job that disappears (TTL via `ttlSecondsAfterFinished`, manual deletion) triggers the next attempt the same way.
- Failures the retry cannot fix (wrong privileges, unreachable host) still burn the budget one attempt at a time, except for the follow prerequisites that [preflight](#preflight) catches before the first attempt. The `Failed` condition names the pgcopydb error, so check it after the first failed attempt instead of waiting for the budget to drain; classifying the remaining deterministic failures to stop early is an open task.

Terminal states are absorbing: a `Completed` or `Failed` Migration never restarts (source and target are immutable anyway). Fix the cause and create a new Migration.

## Deletion

- **Clone-only Migration**: deleting the CR garbage-collects everything it owns (Jobs, work PVC, filters ConfigMap). Nothing on either database needs cleaning; there is no finalizer in the way.
- **Live Migration that ever started**: a finalizer routes deletion through cleanup, because a leaked replication slot retains WAL on the source forever. The operator first stops the worker, then runs the cleanup Job (drops slot, auto-created publication, target origin), then releases the CR to garbage collection. Deletion therefore takes a few reconcile cycles, not milliseconds.
- If cleanup exhausts its retries (for example, the source is gone), the operator emits a `CleanupFailed` warning naming the slot and releases the Migration anyway: an unreachable source should not block deletion forever. If the source still exists, check `pg_replication_slots` there and `SELECT pg_drop_replication_slot('<slot>')` manually.

## Troubleshooting

Look at `kubectl describe migration <name>` first (conditions and events), then at the worker pod logs: `kubectl logs job/<name>-run-<N>` (structured JSON). The `Failed` condition carries the last pgcopydb ERROR from the failed pod after Kubernetes' own "Job has reached the specified backoff limit", so the cause is usually there without opening the logs. It falls back to the bare Job message when the pod is already gone.

| Symptom | Cause | Fix |
|---|---|---|
| Phase `Failed` at `Validating`, reason `PreflightFailed` | A follow prerequisite is missing; the condition message names which one | Apply the `GRANT` or setting the message quotes, then create a new Migration. This is preflight doing its job: nothing moved, nothing to clean up |
| `PreflightFailed` with `no replica identity usable for UPDATE/DELETE` and a table list | UPDATE or DELETE on those tables would fail on the source once published; the audit covers all user tables, including ones excluded by `clone.filters` | Give each table a primary key or `REPLICA IDENTITY USING INDEX`/`FULL`, or acknowledge read-only and insert-only ones in `spec.follow.allowMissingReplicaIdentity` (`["*"]` acknowledges all) in a new Migration |
| First follow attempt fails at slot creation: `could not access file "wal2json"` | `spec.follow.plugin: wal2json`, but the plugin is not installed on the source. Preflight cannot detect this (a decoding plugin has no catalog entry to query) and says so in its log | Install the wal2json package on the source, or stay on the built-in `pgoutput` |
| First follow attempt dies before any data moves; logs show `permission denied for function pg_replication_origin_drop` | Target role misses EXECUTE on the `pg_replication_origin_*` functions; the error surfaces in pgcopydb's setup cleanup, which runs first | Apply the grant block from [PREREQUISITES.md](../PREREQUISITES.md), then create a new Migration. Preflight catches this now, so reaching it means the grants were revoked after the check |
| Live migration streams and "completes", but changes from the window are missing on the target; `replayLSN` advanced normally | Target role cannot `SET session_replication_role`; pgcopydb 0.18 swallows the failure and applies nothing while reporting progress | `GRANT SET ON PARAMETER session_replication_role TO <role>` (PostgreSQL 15+, superuser otherwise); verify with row counts before trusting any re-run. Preflight probes this before the first attempt |
| Logs: `permission denied to start WAL sender` | Source role lacks the REPLICATION attribute | `ALTER ROLE <role> REPLICATION`; preflight checks this too |
| Stream keeps dropping; source logs show terminated walsenders; attempts burn during catchup or drain | Aggressive `wal_sender_timeout` on the source kills the logical walsender (CloudNativePG defaults to 5s) | Raise it to 60s or more for the migration window, on CNPG via `spec.postgresql.parameters` |
| Migration `Failed`, condition reason `DrainIncomplete` | The worker exited before applying everything up to the cutover LSN (crash inside the drain window; pgcopydb `--resume` then exits 0 without replaying) | Do NOT switch applications to the target. No data is lost at the source: the slot is kept and retains WAL. Compare the verify Job logs (`endpos` vs `origin_progress`) to size the gap. Simplest recovery: keep running on the source, delete the Migration (cleanup drops the slot), and run a fresh live migration |
| Retry fails with `publication ... already exists` | The previous attempt crashed mid-setup and `--resume` re-runs CREATE PUBLICATION non-idempotently | Retries drop the auto-managed (slot-named) publication first, so this means the publication is your own: either drop it on the source, or leave `spec.follow.publication` pointing at it and pre-create it as PREREQUISITES.md describes |
| Migration sits at attempt 1 with event `... exists but belongs to another owner` | The work PVC or ConfigMap of a just-deleted Migration with the same name is still awaiting garbage collection | Wait for GC to finish, or delete the leftover objects |
| Logs: `pg_dump: error: server version mismatch` | Client tools in the runner image are older than a server major (`pg_dump` must be at least the newest major on either side) | The default runner ships PostgreSQL 18 client tools; if you pinned `spec.runner.image`, point it at an image with matching tools |
| Phase `CutoverPending` and nothing happens | Manual mode waiting for `spec.cutover.approved: true` | That is the contract: stop writes, then approve |
| `CaughtUp` stays False | Lag above `follow.maxCatchupLag`, or no sentinel sample yet | Check `status.replication.lagBytes`; heavy write traffic keeps lag high, throttle it or raise the threshold |
| No worker pod; PVC `Pending` | No StorageClass can provision the work volume | Set `spec.workVolume.storageClassName`, or fix the cluster default |
| Phase `Failed`, "retry budget exhausted" | Every attempt failed; the first attempt's logs almost always name the real cause | Fix the cause and create a new Migration; terminal states are absorbing by design |
