# pgcopydb-operator

A Kubernetes operator that turns [pgcopydb](https://github.com/dimitri/pgcopydb) into Migration-as-a-service for PostgreSQL: declare a `Migration` resource, get a supervised bulk clone, optional logical-replication follow with controlled cutover, verification, and cleanup. Source and target are plain libpq endpoints, so it works with any PostgreSQL: managed, operator-run, or bare.

> [!important]
> In development. One-shot clone (M1) and live migration with follow and controlled cutover (M2) are functional and e2e-tested; verification (M3) shipped with envtest coverage, its e2e scenario queued; release polish (M4) is underway. See [MILESTONES.md](MILESTONES.md) for the task ledger and the [design](docs/superpowers/specs/2026-08-07-operator-design.md) for the architecture.

## Install

```sh
helm install pgcopydb-operator oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
  --namespace pgcopydb-system --create-namespace
```

Then create a `Migration` (full examples with explanations in [docs/examples/](docs/examples/)):

```sh
kubectl apply -f docs/examples/migration-minimal.yaml
kubectl get pgm -w
```

The [usage guide](docs/usage.md) walks through everything from here: clones, live migrations with cutover, suspend, retries, and troubleshooting.

## Structure

```text
api/v1alpha1/              # Migration CRD types (CEL validation, no webhooks)
charts/pgcopydb-operator/  # Helm chart (published as OCI to ghcr.io)
cmd/, internal/            # manager and controller (kubebuilder go/v4)
config/                    # kubebuilder-generated kustomize tree
docs/examples/             # Migration resources with short explanations
docs/research/             # pgcopydb CLI + CDC references, prior art
docs/superpowers/specs/    # approved design documents
images/runner/             # worker image: pgcopydb + PostgreSQL 17 client tools
test/e2e/                  # e2e suite (local only, current kubectl context)
.claude/skills/            # vendored skills (ponytail, humanizer, brainstorming), mandatory per AGENTS.md
.github/workflows/         # GitHub->GitLab mirror + ghcr release workflow
.gitlab-ci.yml             # branch pipelines: yamllint, golangci-lint, envtest, image build
Taskfile.yml               # task help | lint | test | e2e
```

## Developing

```sh
task help   # list tasks
task lint   # the pre-commit gate (yamllint + make lint)
task test   # unit and envtest suites
task e2e    # e2e against your CURRENT kubectl context (local only; prompts first)
```

## Documentation

- [Usage guide](docs/usage.md): install, first clone, live migration with cutover, troubleshooting.
- [PREREQUISITES.md](PREREQUISITES.md): what your PostgreSQL endpoints and cluster must provide before a Migration can run.
- [docs/examples/](docs/examples/): Migration resources for common scenarios.
- [CRD reference](docs/api.md): every `Migration` field with defaults and validation, generated from the Go types (`task docs`).
- [docs/coverage.md](docs/coverage.md): every pgcopydb clone/follow option mapped to the Migration spec.
- [Chart README](charts/pgcopydb-operator/README.md): values reference.
- [CONTRIBUTING.md](CONTRIBUTING.md): how to work here.
- [AGENTS.md](AGENTS.md): rules for AI agents, including the mandatory skills.
- [MILESTONES.md](MILESTONES.md): where the project stands and why decisions were made.
- [Design](docs/superpowers/specs/2026-08-07-operator-design.md): the approved operator architecture.
- [LICENSE](LICENSE): Apache 2.0.
