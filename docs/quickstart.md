# Quickstart

This page shows a first clone, end to end.
Read the [prerequisites](reference/prerequisites.md) first: the privileges, WAL settings, and replica identities that your PostgreSQL endpoints must provide.
Most failures in the [troubleshooting table](troubleshooting.md) have their cause in these prerequisites.
[Install the operator](installation.md).
Then continue on this page.

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

`COMPLETE` shows the `Complete` condition.
It stays empty until the migration finishes.
Live migrations get a `LAG` column (replication lag in bytes) with `kubectl get pgm -o wide`.

What the operator does:

- It creates a work PVC (`<name>-work`, 10Gi by default).
  Each attempt gets one worker Job (`<name>-run-<attempt>`) that runs `pgcopydb clone` from the runner image.
- `status.progress` (tables, indexes, bytes) fills while the copy runs.
  The operator counts these values with psql on both databases.
  At the end, pgcopydb's own count replaces that estimate, on the bundled runner and on other allowlisted runners.
  See the [monitoring guide](operations/monitoring.md).
- `status.conditions` are the source of truth.
  `phase` is a summary for the printer column.
- `Completed` means that pgcopydb finished.
  It copied the data under one consistent snapshot, and it synced the sequences.
  The source stays unchanged.
  A plain clone needs no replication privilege, and it leaves nothing on either database.

[Configuration](configuration.md) describes parallelism, same-table splitting, filters, skips, and connection forms for managed databases.

The manager also exports Prometheus metrics for each Migration on HTTPS :8443.
Each metric name matches `pgcopydb_migration_*`.
The metrics cover the phase, the progress, the database sizes on both ends, the LSN positions and the replication lag, and the verification outcome.
`metrics.serviceMonitor.enabled=true` in the chart connects them to the Prometheus Operator.
The [monitoring guide](operations/monitoring.md) has the full metric reference, the bundled Grafana dashboards, and the alert rules.

Next: [Live migration](operations/live-migration.md) for the follow-and-cutover walkthrough.
