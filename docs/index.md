# pgcopydb-operator

A Kubernetes operator that turns [pgcopydb](https://github.com/dimitri/pgcopydb) into Migration-as-a-service for PostgreSQL: declare a `Migration` resource, get a supervised bulk clone, optional logical-replication follow with controlled cutover, verification, and cleanup. Source and target are plain libpq endpoints, so it works with any PostgreSQL: managed, operator-run, or bare.

**In development.** One-shot clone and live migration with follow and controlled cutover are functional and e2e-tested; verification shipped with envtest coverage. The API is v1beta1 (v1alpha1 stays served but is deprecated) and may still change.

Where to go:

- [Quickstart](quickstart.md): a first clone, end to end.
- [Installation](installation.md): the Helm chart and the CRD lifecycle.
- [Configuration](configuration.md): clone tuning, filters, work volume, runner image, credentials.
- [Live migration](operations/live-migration.md): follow mode, preflight, and the cutover runbook.
- [Verification](operations/verification.md): `pgcopydb compare` after completion.
- [Suspend, retries, deletion](operations/lifecycle.md): day-2 lifecycle of a Migration.
- [Migrating into CloudNativePG](operations/cloudnativepg.md): the recipe for CNPG targets and sources.
- [Argo CD health checks](operations/argocd.md): GitOps health for `Migration` resources.
- [Troubleshooting](troubleshooting.md): symptoms mapped to causes and fixes.
- [Prerequisites](reference/prerequisites.md): what your endpoints must provide; read this before a live migration.
- [CRD reference](reference/api.md): every `Migration` field with defaults and validation.
- [Conditions and reasons](reference/conditions.md): the condition types and reason strings as API contract.
