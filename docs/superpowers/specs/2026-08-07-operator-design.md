# pgcopydb-operator design

Date: 2026-08-07. Status: approved. The keywords MUST, SHOULD, MAY follow RFC 2119.

Ground truth for every pgcopydb claim: [docs/research/pgcopydb-cli.md](../../research/pgcopydb-cli.md) and [docs/research/pgcopydb-follow.md](../../research/pgcopydb-follow.md) (pgcopydb v0.18). Cluster facts: [docs/research/dev-cluster.md](../../research/dev-cluster.md). Pattern sources: [docs/research/prior-art.md](../../research/prior-art.md).

## Purpose

A Kubernetes operator that turns pgcopydb into Migration-as-a-service for PostgreSQL: declare a `Migration` CR with a source and a target, get a supervised bulk clone, optionally logical-replication follow with a controlled cutover, verification, and cleanup. Agnostic by design: source and target are plain libpq endpoints (any cloud, any operator, any bare instance); the only Kubernetes requirements are Secrets for credentials and a StorageClass for the work directory.

Non-goals (v1): DDL replication (logical decoding cannot), multi-database `--all-databases` orchestration, scheduling/recurring migrations, in-place major upgrades, managing the Postgres servers themselves.

## API

Group `pgcopydb-operator.io`, version `v1alpha1`, namespaced kind `Migration`, shortName `pgm`. Single kind by decision; the connection struct is a named type and `source`/`target` are one-of shaped (inline XOR a future `connectionRef`) so a reusable `PostgresConnection` kind can be added later without breaking changes.

```yaml
apiVersion: pgcopydb-operator.io/v1alpha1
kind: Migration
metadata:
  name: shop-to-cnpg
spec:
  source:                      # immutable (CEL); one-of: inline | uriSecretRef
    host: db.old-dc.example.com
    port: 5432                 # default 5432
    database: shop
    username: migrator
    passwordSecretRef: {name: shop-src, key: password}
    sslMode: verify-full       # disable|allow|prefer|require|verify-ca|verify-full
    sslSecretRefs:             # optional client cert/key/root CA, mounted 0600
      rootCA: {name: shop-src-tls, key: ca.crt}
      cert: {name: shop-src-tls, key: tls.crt}
      key: {name: shop-src-tls, key: tls.key}
    # OR: uriSecretRef: {name: shop-src, key: uri}   # full libpq URI/DSN
  target:                      # same struct, immutable
    host: shop-pg-rw.shop.svc
    database: shop
    username: app
    passwordSecretRef: {name: shop-pg-app, key: password}
  clone:                       # maps pgcopydb clone; all optional
    tableJobs: 4
    indexJobs: 4
    restoreJobs: 0             # 0 = follow indexJobs (pgcopydb default)
    largeObjectsJobs: 4
    splitTablesLargerThan: 1Gi # resource.Quantity, rendered to bytes
    splitMaxParts: 0
    dropIfExists: false
    roles: false               # needs superuser unless noRolePasswords
    noRolePasswords: false
    noOwner: false
    noACL: false
    noComments: false
    noTablespaces: false
    useCopyBinary: false
    failFast: false
    skip: [largeObjects, extensions, collations, vacuum, analyze, dbProperties, ctidSplit]  # enum list
    filters:                   # rendered to the pgcopydb --filters INI
      includeOnlyTables: ["public.orders", "public.~/^audit_/"]
      excludeTables: []
      excludeSchemas: []
      includeOnlySchemas: []
      excludeIndexes: []
      excludeTableData: []
      excludeExtensions: []
      includeOnlyExtensions: []
  follow:                      # M2; presence enables clone --follow
    enabled: true
    plugin: pgoutput           # pgoutput|wal2json|test_decoding
    slotName: ""               # default: generated, unique per migration
    publication: ""            # empty = auto-managed by pgcopydb
    replayNoOpUpdates: false
    maxCatchupLag: 16Mi        # lag threshold for the CaughtUp condition
  cutover:                     # M2
    mode: Manual               # Manual|Automatic
    approved: false            # mutable; Manual mode waits for true
  verification:                # M3; runs pgcopydb compare after completion
    schema: true
    data: false                # full-table checksums, expensive
  workVolume:
    storageClassName: ""       # cluster default when empty
    size: 10Gi
  runner:                      # worker pod knobs
    image: ""                  # default: operator-configured runner image
    resources: {}
    nodeSelector: {}
    tolerations: []
    affinity: {}
  suspend: false
  backoffLimit: 3              # operator-level retry budget (Jobs run backoffLimit 0)
  ttlSecondsAfterFinished: null
status:
  observedGeneration: 3
  phase: Streaming             # printer column, derived from conditions
  conditions:                  # metav1.Condition, positive polarity
  # Validated, SnapshotReady (M2), CloneCompleted, Streaming (M2), CaughtUp (M2),
  # CutoverRequested (M2), CutoverCompleted (M2), Verified (M3), Complete, Failed
  attempts: 1
  progress: {tablesTotal: 120, tablesDone: 87, indexesTotal: 300, indexesDone: 12, bytesTotal: "…", bytesDone: "…"}
  replication: {writeLSN: "0/5000000", replayLSN: "0/4FFF000", endpos: "", lagBytes: 4096, slotName: "…"}
  jobName: shop-to-cnpg-clone-1
  startedAt: …
  completedAt: …
```

Validation is CEL-only (`x-kubernetes-validations`), no webhooks: source/target immutability (`self == oldSelf` transition rules), one-of enforcement, mode-dependent field rules (`cutover.approved` only mutable field besides `suspend`, `verification`, `runner.resources`, `backoffLimit`, `ttlSecondsAfterFinished`). Defaulting via `+kubebuilder:default`.

## Controller architecture

One controller, one owned workload chain per Migration, everything derived from observable state (owned object status + pgcopydb catalogs), nothing from controller memory. Owned objects, all with ownerReferences and controller labels:

1. **PVC** `<name>-work`: the pgcopydb work dir (`--dir /workdir`). The unit of resumability; retained until CR deletion.
2. **Migration Job** `<name>-run-<attempt>`: `backoffLimit: 0`, one container from the runner image, command `pgcopydb clone [--follow]` with flags rendered from spec, `--resume` on attempt > 1 (`--not-consistent` when the snapshot is gone). Retries are operator-driven: on Job failure and `attempts < backoffLimit`, a new Job mounts the same PVC.
3. **Cleanup Job** `<name>-cleanup` (follow mode): runs `pgcopydb stream cleanup` on the same PVC. Triggered by the finalizer on deletion and by the abort path. Mandatory: it drops the replication slot, auto-created publication, and target origin; a leaked slot retains WAL on the source without bound.

Secrets and connections: URIs are composed by the operator without passwords; passwords go through libpq's `PGPASSFILE` (a projected file mounted 0600 from the referenced Secrets), or the full URI is injected as `PGCOPYDB_SOURCE_PGURI`/`PGCOPYDB_TARGET_PGURI` env from `secretKeyRef` when `uriSecretRef` is used. TLS materials are mounted 0600 and referenced via libpq keywords (`sslrootcert`, `sslcert`, `sslkey`). The operator never writes credentials into CR status, logs, or events; pgcopydb itself stores URIs sans credentials in its catalogs.

Control plane for a running follow: pgcopydb v0.18's TCP sentinel coordinator. The runner starts with `PGCOPYDB_HOST=0.0.0.0` (port 5442); the operator drives cutover with `pgcopydb stream sentinel set endpos --current --host <pod-ip>` and reads `sentinel get` single-value selectors (the `--json` output has a known endpos bug, see research). The protocol is unauthenticated, so the chart ships a NetworkPolicy restricting 5442 to the operator; if the M1 spike finds the coordinator unusable on `clone --follow`, fallback is `pods/exec` against the runner pod (RBAC already scoped). Progress polling: `pgcopydb list progress --json` needs work-dir access, so it runs via exec in the runner pod on a requeue interval.

### State machine (conditions, derived phase)

Pending → Validating → (SnapshotReady) → Cloning → [no follow: Verifying? → Completed] / [follow: Streaming → CaughtUp ⇄ Streaming → CutoverPending (Manual waits for approved, Automatic proceeds at CaughtUp) → CuttingOver (endpos set) → Draining (wait Job exit 0, authoritative done signal; sequences re-synced by pgcopydb) → Verifying? → Completing (cleanup Job) → Completed]. Failed is absorbing after retry budget exhaustion; Suspended overlays any running phase (Job deleted, PVC kept, slot lag surfaced as a warning since WAL retention continues).

Pre-flight validation (Validating, M1 subset + M2 follow checks): connectivity both sides (`pgcopydb ping`), `wal_level=logical`, free replication slots, REPLICATION privilege, replica-identity audit for PK-less tables (fail unless acknowledged via an allowlist field), plugin availability, slot-name uniqueness. Follow-mode findings land in conditions before any data movement.

## Observability

- Status as above; Events for every transition and retry.
- Metrics (controller-runtime registry, ServiceMonitor in chart): `pgcopydb_migration_phase` (gauge per phase), `pgcopydb_migration_tables_done/total`, `pgcopydb_migration_bytes_done`, `pgcopydb_migration_replication_lag_bytes`, `pgcopydb_migration_attempts_total`, `pgcopydb_migration_failures_total`. Slot-retention alert rule shipped as an example PrometheusRule.
- Runner logs: `PGCOPYDB_LOG_JSON=on` for structured stderr.

## Images

- Operator image: distroless, scaffold Dockerfile, digest-pinned (existing harness rules).
- Runner image (`images/runner/`): Debian bookworm + PGDG, `pgcopydb` 0.18 pinned + `postgresql-client-17` (the upstream image ships PG16 client tools; pg_dump must be ≥ target major and dev targets are PG17). Non-root, work dir `/workdir`. Both published to ghcr.io with the Helm chart (OCI) via a GitHub release workflow.

## Helm chart (`charts/pgcopydb-operator`)

Templated CRDs gated by `crds.install` with `helm.sh/resource-policy: keep`; values: `image.*`, `runner.image.*`, `imagePullSecrets`, `rbac.create`, `serviceAccount.*`, `watchNamespaces` (empty = cluster-wide), `resources`, `metrics.serviceMonitor.enabled`, `networkPolicy.enabled`, `nodeSelector/tolerations/affinity`. No webhooks, no cert-manager dependency.

Dev cluster registration (app-of-apps): `platform/templates/pgcopydb-operator/application-pgcopydb-operator.yaml` at sync-wave `-3` with ServerSideApply, git-repo-with-path source (`charts/pgcopydb-operator` on GitHub main; the proven pattern), values block in `platform/values.yaml`, GitHub repo URL added to the `platform` AppProject `sourceRepos` whitelist. OCI source can replace it once proven.

## Testing

- Unit/envtest: every package; controller reconcile paths against envtest with a fake runner (Job status manipulation); CEL validation tests via envtest CRD apply; flag/INI rendering golden tests. Mandatory by default (AGENTS.md).
- E2e (`task e2e`, local against dev cluster context): Ginkgo suite in `test/e2e` adapted off kind. Fixtures: two CNPG `Cluster` CRs (pagila demo data via initdb import job), namespace `pgcopydb-e2e`. Scenarios: same-cluster clone, cross-namespace with Secrets + service DNS (stands in for cross-cluster; the wire protocol is identical), filters, resume after runner-pod kill, follow + live pgbench writes + Manual cutover + row equality, Automatic cutover, abort path drops the slot, compare verification. Cross-cluster against a second real cluster stays a documented manual scenario until a second cluster exists.
- The ten research gaps become M1 spike tasks with recorded outcomes in MILESTONES.md.

## Security posture

Least privilege RBAC (Jobs/PVCs/Secrets get/list/watch/create in watched namespaces, pods/exec only for the runner label selector); runner and manager run non-root with readOnlyRootFilesystem where possible; NetworkPolicy for the sentinel port; no credential material in status/logs/events; images and chart digest-pinned and Renovate-tracked.

## Extension path (recorded, not built)

`PostgresConnection` kind via `connectionRef` one-of; decomposed phase commands with a dedicated snapshot-holder pod for consistent base-copy resume; `--all-databases` instance migrations; auto-generated PrometheusRule per Migration; multi-version CRD once the API stabilizes (requires conversion webhook, revisit no-webhook stance then).
