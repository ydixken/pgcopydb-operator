# Quickstart

A first clone, end to end. What your PostgreSQL endpoints must provide beforehand (privileges, WAL settings, replica identities) lives in the [prerequisites](reference/prerequisites.md); read it first, most of the failures in the [troubleshooting table](troubleshooting.md) trace back to it. [Install the operator](installation.md), then continue here.

A `Migration` without `follow` is a one-shot bulk copy: schema, data, indexes, constraints, sequences. Put the passwords in Secrets in the Migration's namespace, then apply the minimal example ([migration-minimal.yaml](examples/migration-minimal.yaml)):

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

Tuning (parallelism, same-table splitting, filters, skips) and DBaaS connection forms are covered in [Configuration](configuration.md).

The manager also exports Prometheus metrics per Migration (`pgcopydb_migration_phase`, `_attempts`, `_tables_done`, `_tables_total`, `_indexes_done`, `_indexes_total`, `_replication_lag_bytes`, `_verified`) on HTTPS :8443; `metrics.serviceMonitor.enabled=true` in the chart wires them into the Prometheus Operator.

Next: [Live migration](operations/live-migration.md) for the follow-and-cutover walkthrough.
