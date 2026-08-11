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
1. If the change touches `api/v1beta1`, run `task docs` and commit the regenerated `docs/reference/api.md` with it.
1. Run `task lint` (and `task test` once Go code exists). Both MUST be clean before every commit.
1. Commit (see below), push the branch to GitHub, open a PR.

`.github/workflows/ci.yml` runs lint, tests and the docs build on every push and pull request, and those three jobs are the required checks on `main`. The GitLab project (`gitlab.com/ydixken/pgcopydb-operator`) is a push mirror and nothing else: it keeps the branches and tags off GitHub, runs no pipeline, and never takes a commit or an MR.

Coverage goes to Codecov, gated on the `CODECOV_TOKEN` repository secret. Codecov rejects tokenless uploads even from public repositories, so without the secret the upload step skips visibly rather than passing quietly; with it set, a failed upload fails the job. The coverage total is printed in the job summary either way.

## Self-hosted runners

Two runner scale sets serve this repository, both backed by Actions Runner Controller on the dev cluster. Everything else stays on GitHub-hosted runners. The scale sets, their GitHub App credentials and their Helm values are declared outside this repository (see private ops notes); nothing here configures them beyond the `runs-on:` label.

`github-runner-pgcopydb-operator` builds the release workflow's two images. Its jobs get no Kubernetes API access, so they cannot reach the cluster they run on. `github-runner-pgcopydb-e2e` is the deliberate exception: the e2e job installs a chart and drives real workloads, so that scale set does reach the API. Its ServiceAccount is scoped to the e2e namespaces, which GitOps owns; it can work inside them but cannot create or delete one, which is why the suite runs there with `E2E_MANAGE_NAMESPACES=false`.

Two rules hold because this repository is public and both scale sets are real machines on a private cluster:

- No workflow that can be triggered by a fork MAY target them. Today only tag pushes (`release.yml`) and manual dispatch (`runner-smoke.yml`) do.
- `pull_request_target` MUST NOT be used in any workflow. It runs the base branch's copy of the workflow with the base branch's secrets, which is fork approval skipped by design.

The e2e job backs the first rule with something GitHub enforces rather than something we remember. Its `environment: e2e-cluster` carries a deployment branch and tag policy that permits `main` and `v*` and nothing else, evaluated before the job is dispatched. A fork pull request runs at `refs/pull/N/merge`, matches neither, and never reaches a machine that can talk to the cluster.

`runner-smoke.yml` is a `workflow_dispatch` build that exercises the build runner and its Docker daemon without publishing anything. Run it after any change to that scale set.

## E2e tests

`task e2e` runs `test/e2e/` against the CURRENT kubectl context, a real cluster; it prints the context and prompts before touching anything (see the Caution section in [AGENTS.md](AGENTS.md)). The suite installs a throwaway operator, creates two CNPG clusters, and seeds the source through a Kubernetes Job running psql with the SQL under `test/e2e/fixtures/`. Seeding is idempotent: an `e2e_seed` marker table records profile and scale, a matching marker skips the seed, and a kept cluster with a mismatching marker is recreated.

Two tiers, and the environment variables a run reads:

| Variable                | Default | Effect                                                                                                             |
|-------------------------|---------|--------------------------------------------------------------------------------------------------------------------|
| `E2E_SCALE`             | `1`     | Fixture size multiplier. 1 seeds roughly 12GB; row counts scale linearly, so 0.1 gives a ~1.2GB quick run.          |
| `E2E_STRESS`            | unset   | `true` selects the stress tier: scale 10 (~120GB), 200/150/50Gi volumes, longer budgets. Use `task e2e:stress`.     |
| `E2E_KEEP_FIXTURES`     | unset   | `true` keeps the fixture namespaces and clusters for iteration; the next run reuses them and skips a matching seed. |
| `E2E_FORCE`             | unset   | `true` takes over the helm release a crashed run left behind.                                                       |
| `E2E_PG_SOURCE`         | `17`    | PostgreSQL major (14 to 18) for the source cluster's CNPG operand image.                                            |
| `E2E_PG_TARGET`         | `17`    | PostgreSQL major for the target. MUST NOT be older than the source, and MUST be at least 15 (see below).            |
| `E2E_OPERATOR_TAG`      | unset   | Manager image tag to install instead of the pinned release; the runner follows it.                                  |
| `E2E_RUNNER_TAG`        | unset   | Worker image tag on its own, for an unreleased `images/runner` build.                                               |
| `E2E_STORAGE_CLASS`     | unset   | Pins the fixture volumes to one StorageClass, and wins over the stress tier's own.                                  |
| `E2E_MANAGE_NAMESPACES` | `true`  | `false` works inside namespaces someone else owns: creates and deletes none, installs with `rbac.create=false`.     |

Outside the stress tier the fixture volumes follow the scale, down from 40/40/10Gi at scale 1, with a floor at an eighth of that: a 0.1 run gets 5/5/2Gi. The floor is there because WAL, indexes and the change spool need headroom that the row counts alone do not size.

A kept cluster whose server runs a different major than `E2E_PG_SOURCE`/`E2E_PG_TARGET` request is deleted and recreated before the suite proceeds: CNPG cannot change majors in place.

`task e2e:matrix` runs the full suite (chaos specs excluded) three times at `E2E_SCALE=0.1`, one version combo per run: PG 14 to 18, 18 to 18, and 15 to 17. One confirmation prompt up front covers all three; each combo is echoed before it starts. The fixture namespaces stay up between combos (only a cluster on the wrong major gets recreated) and the last combo tears them down. A failing combo does not stop the rest: the task prints a pass/fail summary at the end and exits nonzero if any combo failed. The matrix is upgrade-direction only because pgcopydb needs `pg_dump` at least at the target's major and a newer major's dump does not restore into an older server. PG14 appears as a source only because the follow-mode target contract includes `GRANT SET ON PARAMETER session_replication_role`, which PostgreSQL grew in 15 ([docs/reference/prerequisites.md](docs/reference/prerequisites.md)).

The stress tier (`task e2e:stress`) requires Longhorn. The suite creates a `longhorn-e2e-ephemeral` StorageClass (numberOfReplicas 1, reclaimPolicy Delete) if absent, and refuses to start (Skip) unless the `nodes.longhorn.io` CRs report enough available storage for the requested volumes times 1.2 headroom. The capacity check reads live cluster state; nothing about the cluster is hardcoded.

Chaos scenarios live in `test/e2e/chaos_test.go` behind the Ginkgo label `chaos`: they kill fixture pods (CNPG primaries, the runner mid-drain), overflow a follow migration's change spool on a deliberately tiny work volume, and fan two concurrent follow migrations out of one source. `task e2e` and `task e2e:stress` exclude them (`-ginkgo.label-filter='!chaos'`); `task e2e:chaos` runs exactly them, with the same context echo and confirmation prompt. Each chaos spec creates its own Migration and restores what it disturbed, so the set runs standalone against kept fixtures. The source-kill spec times its kill off `pg_stat_progress_copy` on the target and Skips below `E2E_SCALE` 0.05, where the documents COPY gets too short to hit reliably.

`release.yml` runs this suite too, against a release candidate rather than a branch: `E2E_SCALE=0.1`, chaos excluded, `E2E_OPERATOR_TAG` set to the candidate so it installs the images that run was built from, and `E2E_MANAGE_NAMESPACES=false` because there the namespaces belong to GitOps and the CI identity may not create one. It calls `go test` directly, not `task e2e`: that target's confirmation prompt exists for a developer who could be pointed at any cluster, and answering it with `task --yes` is forbidden.

## Releasing

Releases cut themselves. Every Monday at 08:00 UTC `auto-release.yml` reads what landed since the last stable tag and pushes a release candidate: `vX.Y.Z-rc.1`, a patch bump unless a `feat:` commit is in the range, in which case a minor one. A week with nothing merged ends with no tag and a green run, which is not a failure. When a candidate for the same version already exists the number counts up, rather than reusing a tag whose images are published.

That tag starts `release.yml`, which publishes the manager and runner images (multi-arch) and the Helm chart as OCI, creates the GitHub release whose notes GitHub generates from the merged PRs, and runs the e2e suite against exactly those artifacts on the cluster. Pass, and the workflow pushes the stable tag `vX.Y.Z`, which starts the same workflow once more on the same commit: the same images from the same context, and this time `latest` moves and the release is not marked a prerelease. Fail, and nothing is promoted. The candidate's artifacts stay where they are, `latest` still points at the last stable release, and the workflow opens an issue naming the run. Fix forward on `main` and the next candidate carries the fix.

> [!important]
> A release candidate publishes under its own tag and never moves `latest`.
> A `helm install` without an explicit version therefore never lands on a build e2e has not signed off.

The chart job waits on both image jobs, so a published chart never points at an image that failed to build. A tag containing a hyphen is a SemVer prerelease and is marked as one on GitHub and Artifact Hub, so the candidate round stays out of the way of anyone browsing the releases page for a version to install.

Tagging by hand is the out-of-band case, for a fix that cannot wait until Monday:

```sh
git tag -a v0.4.1 -m "v0.4.1: short subject"
git push origin v0.4.1
```

A stable tag pushed that way is published as it stands: it skips the candidate round and with it the e2e gate.

Versions are plain SemVer. The `-alpha.N` prereleases ran up to `v0.2.0-alpha.8` and stop there; `v0.3.0` is the first ordinary release. Pre-1.0 still means the API can change, which is what the major version zero says.

Chart `version` and `appVersion` come from the tag, which is why the values committed in `Chart.yaml` are placeholders. `hack/stamp-chart.sh` runs just before packaging and fills in the three Artifact Hub annotations that only make sense per release: the image tags, the prerelease flag, and a changelog built from the `feat:`, `fix:`, `perf:` and `refactor:` commit subjects since the previous tag. It edits the checkout and commits nothing.

Renovate keeps the e2e install pinned to the current release. A custom manager in `.renovaterc.json` watches `operatorTag` in `test/e2e/e2e_suite_test.go` and bumps it with the weekly dependency PR, so nobody writes that `chore:` commit by hand any more.

### Artifact Hub

The chart is listed at [artifacthub.io/packages/helm/pgcopydb-operator/pgcopydb-operator](https://artifacthub.io/packages/helm/pgcopydb-operator/pgcopydb-operator). Artifact Hub reads the OCI repository directly, so a release needs no extra step to show up there.

Ownership is proved by `charts/pgcopydb-operator/artifacthub-repo.yml`, pushed to the chart's OCI repository under the fixed `artifacthub.io` tag. That tag is not SemVer, so neither Helm nor Artifact Hub mistakes it for a chart version. `.github/workflows/artifacthub-metadata.yml` pushes it on any change to the file and on manual dispatch. `.helmignore` keeps it out of the packaged chart: it is a sibling artifact, not chart content.

The rest of the listing comes from `Chart.yaml` annotations. Two are hand-maintained and worth knowing about:

- `artifacthub.io/crdsExamples` duplicates `docs/examples/migration-minimal.yaml` with the comments stripped. Nothing enforces the copy, so update it when that example changes. List one entry per CRD kind and no more: Artifact Hub matches an example to a CRD by kind and renders the first match, so extra `Migration` entries are dead weight.
- `artifacthub.io/images` lists the runner image explicitly. The runner reaches the cluster as a `--runner-image` flag rather than as a container in a manifest, so Artifact Hub cannot discover it, and without the annotation it is never scanned for vulnerabilities.

## Commits and pull requests

- Conventional commits: `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `ci:`.
- One logical change per commit. Every commit MUST be lint-clean on its own.
- `main` is protected: changes land via GitHub PRs, and `lint`, `test` and `docs` MUST be green. Nobody pushes to `main` directly.
