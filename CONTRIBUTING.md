# CONTRIBUTING.md

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119.

[TOC]

## Requirements

- [go](https://go.dev): the operator's language (nothing to build yet).
- [task](https://taskfile.dev): task runner, the entrypoint for everything.
- [yamllint](https://yamllint.readthedocs.io): lints all YAML; the only linter with work to do pre-scaffold.
- [golangci-lint](https://golangci-lint.run) v2: Go linting, activates once `go.mod` exists.
- [kubectl](https://kubernetes.io/docs/reference/kubectl/): only needed for `task e2e`.
- [gh](https://cli.github.com): PRs happen on GitHub.
- Docker MAY be installed for local image builds; CI builds the published image.

## Day-to-day loop

1. Branch from `main`.
1. Make one logical change.
1. If the change touches `api/v1alpha1`, run `task docs` and commit the regenerated `docs/reference/api.md` with it.
1. Run `task lint` (and `task test` once Go code exists). Both MUST be clean before every commit.
1. Commit (see below), push the branch to GitHub, open a PR.

The GitLab project (`gitlab.com/ydixken/pgcopydb-operator`) is a push mirror that runs the pipeline. Watch it, but never commit or open MRs there.

## E2e tests

`task e2e` runs `test/e2e/` against the CURRENT kubectl context, a real cluster; it prints the context and prompts before touching anything (see the Caution section in [AGENTS.md](AGENTS.md)). The suite installs a throwaway operator, creates two CNPG clusters, and seeds the source through a Kubernetes Job running psql with the SQL under `test/e2e/fixtures/`. Seeding is idempotent: an `e2e_seed` marker table records profile and scale, a matching marker skips the seed, and a kept cluster with a mismatching marker is recreated.

Two tiers, controlled by environment variables:

| Variable           | Default | Effect                                                                                                              |
|--------------------|---------|----------------------------------------------------------------------------------------------------------------------|
| `E2E_SCALE`        | `1`     | Fixture size multiplier. 1 seeds roughly 12GB; row counts scale linearly, so 0.1 gives a ~1.2GB quick run.           |
| `E2E_STRESS`       | unset   | `true` selects the stress tier: scale 10 (~120GB), 200/150/50Gi volumes, longer budgets. Use `task e2e:stress`.      |
| `E2E_KEEP_FIXTURES`| unset   | `true` keeps the fixture namespaces and clusters for iteration; the next run reuses them and skips a matching seed.  |
| `E2E_FORCE`        | unset   | `true` takes over the helm release a crashed run left behind.                                                        |

The stress tier (`task e2e:stress`) requires Longhorn. The suite creates a `longhorn-e2e-ephemeral` StorageClass (numberOfReplicas 1, reclaimPolicy Delete) if absent, and refuses to start (Skip) unless the `nodes.longhorn.io` CRs report enough available storage for the requested volumes times 1.2 headroom. The capacity check reads live cluster state; nothing about the cluster is hardcoded.

## Commits and pull requests

- Conventional commits: `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `ci:`.
- One logical change per commit. Every commit MUST be lint-clean on its own.
- `main` is protected: changes land via GitHub PRs with a green pipeline. Nobody pushes to `main` directly.
