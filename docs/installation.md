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
