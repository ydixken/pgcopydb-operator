# Installation

Install the operator with Helm:

```sh
helm install pgcopydb-operator oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
  --namespace pgcopydb-system --create-namespace
```

The chart installs the `Migration` CRD, the controller manager, and its RBAC. The full values reference is the [chart README](https://github.com/ydixken/pgcopydb-operator/blob/main/charts/pgcopydb-operator/README.md). Check it is up:

```sh
kubectl get crd migrations.pgcopydb-operator.io
kubectl -n pgcopydb-system get deploy
```

## CRD lifecycle

The CRD renders as a regular chart template, so `helm upgrade` updates it in place. Two values control it:

- `crds.install` (default `true`): render the CRD at all. Set it to `false` when something else owns the CRD, for example a second operator install in another namespace.
- `crds.keep` (default `true`): annotate the CRD with `helm.sh/resource-policy: keep`, so `helm uninstall` leaves the CRD, and with it every `Migration` resource, in place. Only set it to `false` if losing all Migrations on uninstall is acceptable.

### API versions

The CRD serves two versions: `v1beta1` (the storage version, use this) and the deprecated `v1alpha1`. The schemas are identical, so existing `v1alpha1` manifests keep working; the API server answers them with a deprecation warning. Switch manifests to `apiVersion: pgcopydb-operator.io/v1beta1` at your convenience.

Note for future operator maintainers, not a user action: `v1alpha1` MUST NOT be dropped from the CRD while the CRD's `status.storedVersions` still lists it. Before removing it, rewrite every stored object at `v1beta1` (a no-op update of each `Migration` is enough; [kube-storage-version-migrator](https://github.com/kubernetes-sigs/kube-storage-version-migrator) automates this), then patch `v1alpha1` out of `status.storedVersions`. Only then can a release stop serving it.

Next: [Quickstart](quickstart.md).
