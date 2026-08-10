# pgcopydb-operator

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/pgcopydb-operator)](https://artifacthub.io/packages/search?repo=pgcopydb-operator)

A Kubernetes operator that turns [pgcopydb](https://github.com/dimitri/pgcopydb) into Migration-as-a-service for PostgreSQL: declare a `Migration` resource, get a supervised bulk clone, optional logical-replication follow with controlled cutover, verification, and cleanup. Source and target are plain libpq endpoints, so it works with any PostgreSQL: managed, operator-run, or bare.

**Documentation:** [ydixken.github.io/pgcopydb-operator](https://ydixken.github.io/pgcopydb-operator/)

> [!important]
> In development, pre-1.0. One-shot clone, live migration with follow and controlled cutover, and verification are functional and e2e-tested against real clusters.

## Install

```sh
helm install pgcopydb-operator oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
  --namespace pgcopydb-system --create-namespace
```

The chart is also listed on [Artifact Hub](https://artifacthub.io/packages/helm/pgcopydb-operator/pgcopydb-operator), with the values reference, the `Migration` CRD and example resources.

Then create a `Migration` (full examples with explanations in [docs/examples/](docs/examples/)):

```sh
kubectl apply -f docs/examples/migration-minimal.yaml
kubectl get pgm -w
```

The [quickstart](docs/quickstart.md) walks through the first clone; [live migration](docs/operations/live-migration.md) covers follow, cutover, and the runbook.

## Structure

```text
api/                       # Migration CRD types, v1beta1 storage + deprecated v1alpha1 (CEL validation, no webhooks)
charts/pgcopydb-operator/  # Helm chart (published as OCI to ghcr.io)
cmd/, internal/            # manager and controller (kubebuilder go/v4)
config/                    # kubebuilder-generated kustomize tree
docs/                      # user docs, rendered to a site by mkdocs (mkdocs.yml)
docs/examples/             # Migration resources with short explanations
images/runner/             # worker image: pgcopydb + PostgreSQL 17 client tools
test/e2e/                  # e2e suite (local only, current kubectl context)
.claude/skills/            # vendored skills (ponytail, humanizer, brainstorming), mandatory per AGENTS.md
.github/workflows/         # GitHub->GitLab mirror, ghcr release, docs deploy, Artifact Hub metadata
.gitlab-ci.yml             # branch pipelines: yamllint, golangci-lint, envtest, image build
Taskfile.yml               # task help | lint | test | e2e
```

## Developing

```sh
task help   # list tasks
task lint   # the pre-commit gate (yamllint + make lint)
task test   # unit and envtest suites
task e2e    # e2e against your CURRENT kubectl context (local only; prompts first)
            # E2E_SCALE sizes the seeded fixtures (default 1 = ~12GB); task e2e:stress runs the ~120GB tier
            # task e2e:matrix runs the PG version combos 14->18, 18->18, 15->17 at scale 0.1

```

## Documentation

- [Quickstart](docs/quickstart.md), [installation](docs/installation.md), [configuration](docs/configuration.md): getting a Migration running and tuned.
- [Live migration](docs/operations/live-migration.md), [verification](docs/operations/verification.md), [lifecycle](docs/operations/lifecycle.md): the operations runbooks.
- [Troubleshooting](docs/troubleshooting.md): symptoms mapped to causes and fixes.
- [Prerequisites](docs/reference/prerequisites.md): what your PostgreSQL endpoints and cluster must provide before a Migration can run.
- [docs/examples/](docs/examples/): Migration resources for common scenarios.
- [CRD reference](docs/reference/api.md): every `Migration` field with defaults and validation, generated from the Go types (`task docs`).
- [Option coverage](docs/reference/coverage.md): every pgcopydb clone/follow option mapped to the Migration spec.
- [Chart README](charts/pgcopydb-operator/README.md): values reference.
- [CONTRIBUTING.md](CONTRIBUTING.md): how to work here.
- [AGENTS.md](AGENTS.md): rules for AI agents, including the mandatory skills.
- [LICENSE](LICENSE): GPL-2.0-only.
