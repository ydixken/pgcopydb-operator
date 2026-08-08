# pgcopydb-operator Helm chart

Installs the pgcopydb-operator: the `Migration` CRD, the controller manager
Deployment, its ServiceAccount and RBAC, and an HTTPS metrics Service. The
manager creates one worker Job per Migration from the runner image.

## Install

```sh
helm install pgcopydb-operator oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator
```

CRDs install as regular templates, annotated `helm.sh/resource-policy: keep`,
so `helm upgrade` updates them and `helm uninstall` leaves them (and your
Migration CRs) in place.

## Values

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `ghcr.io/ydixken/pgcopydb-operator` | Manager image. |
| `image.tag` | `""` | Manager tag; empty uses the chart appVersion. |
| `image.pullPolicy` | `IfNotPresent` | Manager pull policy. |
| `runner.image.repository` | `ghcr.io/ydixken/pgcopydb-operator/runner` | Worker Job image, passed as `--runner-image`. |
| `runner.image.tag` | `""` | Runner tag; empty uses the chart appVersion. |
| `imagePullSecrets` | `[]` | Pull secrets for the manager pod. |
| `nameOverride` | `""` | Overrides the chart name in resource names. |
| `fullnameOverride` | `""` | Overrides the full resource name. |
| `serviceAccount.create` | `true` | Create the manager ServiceAccount. |
| `serviceAccount.name` | `""` | ServiceAccount name; empty derives from the fullname. |
| `rbac.create` | `true` | Create ClusterRole, bindings, and the leader-election Role. |
| `watchNamespaces` | `[]` | Namespaces the manager watches and reconciles (passed as `--watch-namespaces`); empty is cluster-wide. RBAC stays cluster-scoped either way, only the informer cache narrows. |
| `replicaCount` | `1` | Manager replicas; extras are leader-election standbys. |
| `leaderElection.enabled` | `true` | Pass `--leader-elect` and create the election Role. |
| `crds.install` | `true` | Render the Migration CRD. |
| `crds.keep` | `true` | Annotate the CRD so uninstall keeps it. |
| `resources` | requests 100m/128Mi, limit 256Mi | Manager resources; no cpu limit by design. |
| `metrics.enabled` | `true` | Serve HTTPS metrics on :8443 and create the Service. |
| `metrics.serviceMonitor.enabled` | `false` | Create a ServiceMonitor; needs the Prometheus Operator CRDs. |
| `networkPolicy.enabled` | `false` | Placeholder; renders nothing yet. |
| `nodeSelector` / `tolerations` / `affinity` | `{}` / `[]` / `{}` | Manager pod scheduling. |

## First Migration

```yaml
apiVersion: pgcopydb-operator.io/v1alpha1
kind: Migration
metadata:
  name: app-to-new-cluster
spec:
  source:
    host: old-postgres.example.com
    username: postgres
    database: app
    passwordSecretRef:
      name: old-postgres-credentials
      key: password
  target:
    host: new-postgres.db.svc
    username: postgres
    database: app
    passwordSecretRef:
      name: new-postgres-credentials
      key: password
```

The operator spawns a worker Job that runs `pgcopydb clone` from source to
target, resumable across retries through a work PVC (10Gi by default).
Passwords come from the referenced Secrets and are written to a libpq
passfile, never to the command line. Progress appears in
`kubectl get migrations` (phase, tables copied, attempts).

Endpoint requirements (privileges, WAL settings) are in the
[prerequisites](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/reference/prerequisites.md);
the walkthrough starts at the
[quickstart](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/quickstart.md),
live migrations with cutover at the
[live-migration runbook](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/operations/live-migration.md).
