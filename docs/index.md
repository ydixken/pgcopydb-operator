# pgcopydb-operator

A Kubernetes operator that runs PostgreSQL migrations with [pgcopydb](https://github.com/dimitri/pgcopydb).
Declare a `Migration` resource to get a supervised bulk clone, optional logical-replication follow with controlled cutover, verification, and cleanup.
Source and target are plain libpq endpoints, so it works with any PostgreSQL: managed, operator-run, or bare.

**In development.** One-shot clone, live migration with follow and controlled cutover, and verification are functional and e2e-tested.
The API is v1beta1 and may still change.
v1alpha1 stays served, and is deprecated.

Where to go:

- [Quickstart](quickstart.md): a first clone, end to end.
- [Installation](installation.md): the Helm chart and the CRD lifecycle.
- [Configuration](configuration.md): clone tuning, filters, work volume, runner image, credentials.
- [Live migration](operations/live-migration.md): follow mode, preflight, and the cutover runbook.
- [Verification](operations/verification.md): `pgcopydb compare` after completion.
- [Suspend, retries, deletion](operations/lifecycle.md): day-2 lifecycle of a Migration.
- [Monitoring](operations/monitoring.md): per-Migration metrics, the bundled Grafana dashboards, and alert rules.
- [Migrating into CloudNativePG](operations/cloudnativepg.md): CNPG targets and sources.
- [Argo CD health checks](operations/argocd.md): GitOps health for `Migration` resources.
- [Troubleshooting](troubleshooting.md): symptoms mapped to causes and fixes.
- [Prerequisites](reference/prerequisites.md): what your endpoints must provide; read this before a live migration.
- [Planning checklist](planning.md): decisions to settle before you create a `Migration`.
- [CRD reference](reference/api.md): every `Migration` field with defaults and validation.
- [Conditions and reasons](reference/conditions.md): the condition types and reason strings as API contract.
