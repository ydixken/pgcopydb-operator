# Quickstart

A first clone, end to end. What your PostgreSQL endpoints must provide beforehand (privileges, WAL settings, replica identities) lives in the [prerequisites](reference/prerequisites.md); read it first, most of the failures in the [troubleshooting table](troubleshooting.md) trace back to it. [Install the operator](installation.md), then continue here.

A `Migration` without `follow` is a one-shot bulk copy: schema, data, indexes, constraints, sequences. Put the passwords in Secrets in the Migration's namespace, then apply the minimal example ([01-clone-minimal.yaml](examples/01-clone-minimal.yaml)):

```sh
kubectl apply -f docs/examples/01-clone-minimal.yaml
kubectl get pgm -w
```

```text
NAME   PHASE      COMPLETE   ATTEMPTS   AGE
shop   Cloning               1          40s
shop   Completed  True       1          3m
```

`COMPLETE` mirrors the `Complete` condition and stays empty until the migration finishes. Live migrations get a `LAG` column (replication lag in bytes) with `kubectl get pgm -o wide`.

What happens behind the phases:

- The operator creates a work PVC (`<name>-work`, 10Gi by default) and one worker Job per attempt (`<name>-run-<attempt>`), running `pgcopydb clone` from the runner image.
- `status.progress` (tables, indexes, bytes) fills while the clone runs, fed by a `pgcopydb list progress` poll that only runs on runner versions the operator's allowlist names; the bundled runner qualifies. On any other runner, such as a custom stock 0.18 image, the gate stays closed and the fields stay empty, because that pgcopydb returns no data and corrupts the stored filtering of a filtered work dir (see [Troubleshooting](troubleshooting.md)). `status.conditions` are authoritative; `phase` is a summary for the printer column.
- `Completed` means pgcopydb finished: data copied under one consistent snapshot, sequences synced. The source is left untouched; a plain clone needs no replication privilege and leaves nothing behind on either database.

Tuning (parallelism, same-table splitting, filters, skips) and DBaaS connection forms are covered in [Configuration](configuration.md).

The manager also exports Prometheus metrics per Migration on HTTPS :8443 (all `pgcopydb_migration_*`): phase, attempts, table/index/byte progress, database sizes on both ends, LSN positions and replication lag, start and completion timestamps, verification outcome, and a mode-labeled info series, plus `pgcopydb_operator_build_info` for the operator itself. `metrics.serviceMonitor.enabled=true` in the chart wires them into the Prometheus Operator; the [monitoring guide](operations/monitoring.md) has the full metric reference, the bundled Grafana dashboards, and the alert rules.

Next: [Live migration](operations/live-migration.md) for the follow-and-cutover walkthrough.
