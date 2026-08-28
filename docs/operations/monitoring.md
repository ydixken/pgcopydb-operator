# Monitoring

The operator exports per-Migration Prometheus metrics, and the Helm chart ships the pieces that turn them into something you look at: a ServiceMonitor for the scrape, alert rules as a PrometheusRule, and three Grafana dashboards.
All three are opt-in values; this page is the reference for the metrics and for wiring the chart into a kube-prometheus-stack style setup.

## Scrape setup

The manager serves metrics over HTTPS on :8443 and authenticates every scrape; `metrics.enabled` (default `true`) controls the endpoint and its Service.
With the Prometheus Operator, `metrics.serviceMonitor.enabled=true` is the whole scrape setup; the ServiceMonitor sends Prometheus' own ServiceAccount token.
The scraping ServiceAccount additionally needs `get` on the `/metrics` nonResourceURL; kube-prometheus-stack already grants that to its Prometheus, and the 401/403/500 rows in [Troubleshooting](../troubleshooting.md) map the failure modes.

The ServiceMonitor sets `honorLabels: true`, so the `namespace` and `name` labels on migration metrics stay the Migration's own instead of being renamed to `exported_namespace` by the scrape.
Optional values tune the rest: `metrics.serviceMonitor.additionalLabels` (for a Prometheus that selects monitors by label), `interval`, `scrapeTimeout`, `relabelings`, and `metricRelabelings`.

## Metric reference

Every `pgcopydb_migration_*` metric is a gauge labeled `namespace` and `name` for the Migration it describes.
A value the operator does not know is absent, never zero: dashboards and alerts MUST treat a missing series as "no data", not as 0.

| Metric | Extra labels | Meaning | Exists |
|---|---|---|---|
| `pgcopydb_migration_phase` | `phase` | 1 for the current phase | always |
| `pgcopydb_migration_attempts` | | Worker Jobs created so far | always |
| `pgcopydb_migration_info` | `mode` | Always 1; mode is `clone` or `follow` | always |
| `pgcopydb_migration_start_time_seconds` | | Unix time the first attempt started | once started |
| `pgcopydb_migration_completion_time_seconds` | | Unix time the migration completed | once completed |
| `pgcopydb_migration_verified` | | 1 on compare pass, 0 on mismatch | after a verification result |
| `pgcopydb_migration_source_database_size_bytes` | | Source database size | worker running |
| `pgcopydb_migration_target_database_size_bytes` | | Target database size; `rate()` of it is the copy throughput | worker running |
| `pgcopydb_migration_tables_done` / `_tables_total` | | Tables copied / planned | patched runner |
| `pgcopydb_migration_indexes_done` / `_indexes_total` | | Indexes built / planned | patched runner |
| `pgcopydb_migration_clone_copied_bytes` / `_clone_planned_bytes` | | Base-copy bytes moved / planned | patched runner |
| `pgcopydb_migration_replication_lag_bytes` | | Total replication lag | follow, streaming |
| `pgcopydb_migration_source_lsn_bytes` | | Source WAL head as an absolute byte position | follow, streaming |
| `pgcopydb_migration_write_lsn_bytes` | | Last LSN written by the receiver | follow, streaming |
| `pgcopydb_migration_replay_lsn_bytes` | | Last LSN replayed on the target | follow, streaming |
| `pgcopydb_migration_endpos_lsn_bytes` | | Cutover endpos as an absolute byte position | after cutover set it |
| `pgcopydb_operator_build_info` | `version` | Always 1; operator-wide, no migration labels | always |

The "Exists" column is the contract for when a series is present:

- **always**: from the first reconcile of the Migration until its deletion removes every series.
- **worker running**: the sizes are live samples from the worker pod, so they appear during attempts and fade out with the pod.
- **patched runner**: the in-pod progress poll runs only on allowlisted runner versions; the bundled runner qualifies, a custom stock 0.18 image keeps these series dark (see the [troubleshooting row](../troubleshooting.md)).
- **follow, streaming**: plain clones never produce these; in follow mode they start once streaming does.

Derived quantities stay in PromQL rather than becoming metrics: receive lag is `source - write`, apply backlog is `write - replay`, WAL generation is `rate(source_lsn_bytes)`, and percent done divides the done gauges by their totals.

## Dashboards

The chart ships three dashboards, linked to each other through their shared `pgcopydb` tag:

- **Migration detail** (uid `pgcopydb-migration`): one migration end to end; phase and its timeline, database sizes and copy throughput, percent done, a naive ETA, LSN positions with the lag split, the cutover drain, and the verification outcome.
- **Fleet overview** (uid `pgcopydb-fleet`): counts by phase, an all-migrations table whose name column links into the detail dashboard, and lag, throughput, and attempt churn per migration.
- **Operator health** (uid `pgcopydb-operator`): build and leader status, reconcile rate and duration percentiles, workqueue depth and latencies, and process CPU, memory, goroutines, and file descriptors.

`grafana.dashboards.enabled=true` renders them as ConfigMaps labeled `grafana_dashboard: "1"` for Grafana's dashboard sidecar.
The sidecar usually watches only Grafana's own namespace, so set `grafana.dashboards.namespace` to it (the chart's release namespace is only the default).
`grafana.dashboards.folder` writes a `grafana_folder` annotation; the sidecar honors it only when its `folderAnnotation` setting names that annotation, and files the dashboards in General otherwise.

Without a sidecar, import by hand: take the JSON files from `charts/pgcopydb-operator/dashboards/` and import them in the Grafana UI.
Every panel reads its data source from the dashboard's `datasource` variable, so pick your Prometheus there after importing; nothing is hardcoded.

## Alerts

`metrics.prometheusRule.enabled=true` installs the alert rules from [`charts/pgcopydb-operator/rules/migrations.yaml`](https://github.com/ydixken/pgcopydb-operator/blob/main/charts/pgcopydb-operator/rules/migrations.yaml) as a PrometheusRule.
If your Prometheus selects rules by label (kube-prometheus-stack does), add the matching label with `metrics.prometheusRule.additionalLabels`.
Thresholds and windows are starting points; the promtool unit tests under `test/alerts/` pin each one, so retune there first.

| Alert | Severity | Fires when |
|---|---|---|
| `PgcopydbMigrationFailed` | critical | The phase is `Failed` for 5m |
| `PgcopydbMigrationVerificationFailed` | critical | A compare mismatch stands for 5m |
| `PgcopydbMigrationRetrying` | warning | Three or more new attempts in 30m while active |
| `PgcopydbMigrationCloneStalled` | warning | Cloning while the target size is flat for 1h |
| `PgcopydbMigrationReplicationLagHigh` | warning | Lag above 64Mi for 10m |
| `PgcopydbMigrationCutoverStalled` | critical | An endpos is set and not reached for 15m |

Slot retention is deliberately not covered.
While a follow migration is suspended, failed, or streaming, its replication slot retains WAL on the **source**, and the operator's metrics cannot see the source's disk.
Monitor `pg_replication_slots` on the source itself (`active` and `safe_wal_size`; postgres_exporter exposes both) and alert on inactive slots or a shrinking `safe_wal_size`.

## How this is tested

Every change runs the static gates: the dashboards must parse with unique uids, every PromQL token in a panel or alert must name a registered metric (and every metric must be consumed somewhere), and promtool checks the rules plus a unit test per alert.
Each release candidate then runs a live gate: the e2e suite drives a real follow migration, checks the exported series against a running Prometheus mid-stream and after cutover, replays every dashboard panel query and fails on an empty answer unless the panel is legitimately empty for a completed migration, and finally verifies deletion removes the series.

## Caveats

- The gauges are process state in the manager.
  They disappear on an operator restart and come back as each Migration is reconciled again, which happens at startup; a scrape gap around restarts is normal.
- The database sizes are live samples from the worker pod.
  For a finished migration there is no pod to sample, so those two series do not return after a restart even though the migration's other series do.
- `rate()` and `delta()` over the size gauges misread a shrinking database as a counter reset; the throughput panels note it and the stalled-clone alert uses `delta()` for that reason.
- On a custom stock 0.18 runner the tables, indexes, and clone byte series stay absent; the percent-done panel shows `n/a` for them and the size-based panels keep working.
