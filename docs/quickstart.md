# Quickstart

A first clone, end to end.
Read the [prerequisites](reference/prerequisites.md) first: the privileges, WAL settings, and replica identities your PostgreSQL endpoints must provide.
Most failures in the [troubleshooting table](troubleshooting.md) trace back to them.
[Install the operator](installation.md), then continue here.

A `Migration` without `follow` is a one-shot bulk copy: schema, data, indexes, constraints, sequences.
Put the passwords in Secrets in the Migration's namespace.
Then apply the minimal example, [01-clone-minimal.yaml](examples/01-clone-minimal.yaml):

```sh
kubectl apply -f docs/examples/01-clone-minimal.yaml
kubectl get pgm -w
```

```text
NAME   PHASE      COMPLETE   ATTEMPTS   AGE
shop   Cloning               1          40s
shop   Completed  True       1          3m
```

`COMPLETE` mirrors the `Complete` condition and stays empty until the migration finishes.
Live migrations get a `LAG` column (replication lag in bytes) with `kubectl get pgm -o wide`.

What the operator does:

- It creates a work PVC (`<name>-work`, 10Gi by default).
  Each attempt gets one worker Job (`<name>-run-<attempt>`) that runs `pgcopydb clone` from the runner image.
- `status.progress` (tables, indexes, bytes) fills while the copy runs, counted with psql against both databases.
  pgcopydb's own count replaces that estimate at the end, on the bundled runner and other allowlisted runners; see the [monitoring guide](operations/monitoring.md).
- `status.conditions` are authoritative; `phase` is a summary for the printer column.
- `Completed` means pgcopydb finished: data copied under one consistent snapshot, sequences synced.
  The source stays untouched.
  A plain clone needs no replication privilege, and leaves nothing behind on either database.

[Configuration](configuration.md) covers parallelism, same-table splitting, filters, skips, and connection forms for managed databases.

The manager also exports Prometheus metrics per Migration on HTTPS :8443, all named `pgcopydb_migration_*`.
They cover phase, progress, database sizes on both ends, LSN positions and replication lag, and the verification outcome.
`metrics.serviceMonitor.enabled=true` in the chart wires them into the Prometheus Operator.
The [monitoring guide](operations/monitoring.md) has the full metric reference, the bundled Grafana dashboards, and the alert rules.

Next: [Live migration](operations/live-migration.md) for the follow-and-cutover walkthrough.
