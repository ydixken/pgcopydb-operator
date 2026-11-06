# pgcopydb-operator

pgcopydb-operator is a Kubernetes operator that runs PostgreSQL migrations with [pgcopydb](https://github.com/dimitri/pgcopydb).
Declare a `Migration` resource to get a supervised bulk clone, an optional logical-replication follow with controlled cutover, verification, and cleanup.
The source and the target are plain libpq endpoints, so the operator works with any PostgreSQL: managed, operator-run, or bare.

**In development.** The one-shot clone, the live migration with follow and controlled cutover, and the verification are functional, and e2e tests cover them.
The API is v1beta1 and may still change.
The CRD still serves v1alpha1, but we deprecated it.

Where to go:

- [Quickstart](quickstart.md): a first clone, end to end.
- [Installation](installation.md): the Helm chart and the CRD lifecycle.
- [Configuration](configuration.md): clone tuning, filters, work volume, runner image, credentials.
- [Live migration](operations/live-migration.md): follow mode, preflight, and the cutover runbook.
- [Verification](operations/verification.md): `pgcopydb compare` after completion, and a timed rehearsal while a live migration streams.
- [Dry run, suspend, retries, deletion](operations/lifecycle.md): how to check a Migration before it runs, and its day-2 lifecycle.
- [Monitoring](operations/monitoring.md): per-Migration metrics, the bundled Grafana dashboards, and alert rules.
- [Migrating into CloudNativePG](operations/cloudnativepg.md): CNPG targets and sources.
- [Running the E2E suite against your databases](operations/e2e-external.md): the product's own tests, run against a disposable pair of your databases.
- [Argo CD health checks](operations/argocd.md): GitOps health for `Migration` resources.
- [Troubleshooting](troubleshooting.md): symptoms, with their causes and fixes.
- [Prerequisites](reference/prerequisites.md): what your endpoints must provide.
  Read this page before a live migration.
- [Planning checklist](planning.md): decisions to settle before you create a `Migration`.
- [CRD reference](reference/api.md): every `Migration` field with defaults and validation.
- [Conditions and reasons](reference/conditions.md): the condition types and reason strings as API contract.
