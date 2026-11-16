# Installation

Install the operator with Helm:

```sh
helm install pgcopydb-operator oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
  --namespace pgcopydb-system --create-namespace
```

The chart installs the `Migration` CRD, the controller manager, and its RBAC.
The [chart README](https://github.com/ydixken/pgcopydb-operator/blob/main/charts/pgcopydb-operator/README.md) is the full values reference.
The [Artifact Hub page](https://artifacthub.io/packages/helm/pgcopydb-operator/pgcopydb-operator) of the chart shows this README together with the CRD and the example resources.
Check the installation:

```sh
kubectl get crd migrations.pgcopydb-operator.io
kubectl -n pgcopydb-system get deploy
```

The chart can also install a ServiceMonitor for the scrape, alert rules, and Grafana dashboards.
Each of these parts has its own value.
See [Monitoring](operations/monitoring.md).

## Manager RBAC

The manager needs `get` and `create` on `pods/exec`.
The native engine opens each exec with a WebSocket `GET` and has no fallback, so without `get` every native exec is refused.
The pgcopydb engine falls back to a SPDY `POST`, which needs only `create`.
With `rbac.create=true` (the default) the chart's ClusterRole grants both verbs.
With `rbac.create=false` the cluster admin supplies the manager's role, and that role MUST grant both verbs:

```yaml
- apiGroups: [""]
  resources: ["pods/exec"]
  verbs: ["create", "get"]
```

## Migration edit rights

Read this before you grant users rights on `migrations`, for example through the `migration-editor-role` ClusterRole in `config/rbac/`.

> [!warning]
> A user who can create a Migration can send any Secret key in its namespace to a host that the user selects.
> In the inline connection form, the Migration sets `host` and `passwordSecretRef` names the Secret key.
> The runner puts the password of that key into the passfile line for that host, and libpq sends it to that host.
> When the server asks for cleartext password authentication, libpq sends the password in cleartext.
> The same user also sets `sslMode`, so the TLS settings do not stop this.
> This is true for both engines.
> On the pgcopydb engine, a user who can create or update a Migration can also set `spec.runner.image`.
> The Jobs then run that image with the Secrets that the Migration references, which gives the same access.
> Grant `create`, `update` or `patch` on `migrations` only to users that you trust with all Secrets in that namespace.

## CRD lifecycle

The CRD renders as a regular chart template, so `helm upgrade` updates it in place.
Two values control it:

- `crds.install` (default `true`): render the CRD.
  When something else owns the CRD, for example a second operator install in another namespace, set it to `false`.

> [!warning]
> `crds.keep: false` lets `helm uninstall` delete the CRD and every `Migration` resource with it.
> Set it to `false` only when that data loss is acceptable.

- `crds.keep` (default `true`): annotate the CRD with `helm.sh/resource-policy: keep`.
  `helm uninstall` then leaves the CRD in place.

### Upgrades

Each operator release compiles in the runner digests it qualifies, and every native reconcile checks `status.nativeRunnerImage` against that list.

> [!warning]
> Before an upgrade, let every native Migration finish or delete it while its admitted runner is still qualified.
> A release that drops that runner refuses the Migration on every reconcile, deletion included, and its source slot and publication stay in place.
> Rolling back to the admitting release resumes it.

With `rbac.create=false`, add `get` on `pods/exec` to the manager's role before the upgrade, as [Manager RBAC](#manager-rbac) describes.
`helm upgrade` adds it for you when the chart creates the RBAC.

### Downgrades

An operator release without the [retry gate](operations/lifecycle.md#the-retry-gate) ignores `status.exemptAttempts` and counts every attempt against `spec.backoffLimit`.
`kubectl get pgm -A -o wide` shows the exempt attempts of every Migration in the `EXEMPT` column.

> [!warning]
> Before a downgrade, increase `spec.backoffLimit` to at least `status.attempts` on every Migration with exempt attempts.
> If you do not, the older operator can fail such a Migration permanently at its next retry.
> An older release also ignores `spec.target.synchronousCommit`, so the commits of the copy wait for a synchronous standby again.
> It also runs no retry gate, so retries start without a wait for the sessions of a cut worker.
> After you upgrade again, check that `spec.target.synchronousCommit` is still set, because the older CRD can drop the field.

### API versions

The CRD serves two versions: `v1beta1` and the deprecated `v1alpha1`.
`v1beta1` is the storage version.
The schemas are identical, so existing `v1alpha1` manifests continue to work.
The API server answers them with a deprecation warning.
Change your manifests to `apiVersion: pgcopydb-operator.io/v1beta1`.

Next: [Quickstart](quickstart.md).
