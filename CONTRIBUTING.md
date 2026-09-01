# CONTRIBUTING.md

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119.

[TOC]

## Requirements

- [go](https://go.dev): the operator's language and test toolchain.
- [task](https://taskfile.dev): task runner, the entrypoint for everything.
- [yamllint](https://yamllint.readthedocs.io): lints all YAML; the only linter with work to do pre-scaffold.
- [golangci-lint](https://golangci-lint.run) v2: Go linting, activates once `go.mod` exists.
- [kubectl](https://kubernetes.io/docs/reference/kubectl/): only needed for `task e2e`.
- [gh](https://cli.github.com): PRs happen on GitHub.
- Docker MAY be installed for local image builds; CI builds the published image. Building `images/runner` locally pulls the pinned `pgcopydb-builder` tag from ghcr instead of compiling pgcopydb; see [images/pgcopydb-builder](images/pgcopydb-builder/README.md).

## Day-to-day loop

1. Branch from `main`.
1. Make one logical change.
1. If the change touches `api/v1beta1`, run `task docs` and commit the regenerated `docs/reference/api.md` with it.
1. If the change touches a `+kubebuilder:rbac` marker, run `make manifests` and then `hack/sync-chart-rbac.sh`, and commit the regenerated `config/rbac/role.yaml` and chart templates with it. The chart's rules are generated from `config/rbac`, and `task lint` fails when the two disagree.
1. Run `task lint` (and `task test` once Go code exists). Both MUST be clean before every commit.
1. Commit (see below), push the branch to GitHub, open a PR.

`.github/workflows/ci.yml` runs lint, tests and the docs build on every push and pull request, and those three jobs are the required checks on `main`. The GitLab project (`gitlab.com/ydixken/pgcopydb-operator`) is a push mirror and nothing else: it keeps the branches and tags off GitHub, runs no pipeline, and never takes a commit or an MR.
The pull request `lint` job runs GitHub Dependency Review and rejects new dependencies with moderate or higher known vulnerabilities, disallowed licenses, or violations in runtime, development, or unknown scopes.
GitHub cannot fail Dependency Review for every unresolved license, so contributors MUST review those warnings and resolve each license from a public source before merging.

The `test` job runs beside a throwaway Postgres service container and points `PGCOPYDB_TEST_PGURI` at it.
That variable is what runs `TestCompareDataQuery`, the only test that exercises the SQL deciding the `pgcopydb compare data` verdict; without it the test skips.
Set it when you touch that query: `PGCOPYDB_TEST_PGURI=postgres://user:pw@host/db make test` against any reachable server, since the query reads a JSON literal and touches no table.

Coverage goes to Codecov, gated on the `CODECOV_TOKEN` repository secret. Codecov rejects tokenless uploads even from public repositories, so without the secret the upload step skips visibly rather than passing quietly; with it set, a failed upload fails the job. The coverage total is printed in the job summary either way. `codecov.yml` excludes the `zz_generated*.go` files controller-gen writes, so the Codecov number reflects hand-written code.

## Self-hosted runners

Two runner scale sets serve this repository, both backed by Actions Runner Controller on the dev cluster, and between them they run every job. The scale sets, their GitHub App credentials and their Helm values are declared outside this repository (see private ops notes); nothing here configures them beyond the `runs-on:` label.

`github-runner-pgcopydb-operator` runs everything that is not e2e: both release images, the chart push, the release notes, the promotion tag, the docs deploy, the GitLab mirror, the Artifact Hub push, the weekly candidate, and the three CI jobs. Its jobs get no Kubernetes API access, so they cannot reach the cluster they run on. `github-runner-pgcopydb-e2e` is the deliberate exception: the e2e job installs a chart and drives real workloads, so that scale set does reach the API. Its ServiceAccount is scoped to the e2e namespaces, which GitOps owns; it can work inside them but cannot create or delete one, which is why the suite runs there with `E2E_MANAGE_NAMESPACES=false`.

Two rules hold because this repository is public and both scale sets are real machines on a private cluster:

- No workflow that can be triggered by a fork MAY target them, on any code path a fork can reach.
- `pull_request_target` MUST NOT be used in any workflow. It runs the base branch's copy of the workflow, with the base branch's secrets, against a fork's code, so the approval that gates a fork's first run never gets asked for.

`ci.yml` runs on `pull_request`, so it is the one workflow a fork can trigger, and it picks its runner per event instead of pinning one:

```yaml
runs-on: ${{ github.event.pull_request.head.repo.fork && 'ubuntu-latest' || 'github-runner-pgcopydb-operator' }}
```

A push carries no `pull_request` payload and falls through to the cluster runner, as does a pull request from a branch in this repository; only a fork's pull request lands on a hosted one. GitHub's own gate is at its strictest setting (`approval_policy` is `all_external_contributors`) and a public-repo fork run gets a read-only token and no secrets, but that gate is deliberately not what we lean on: the runner namespaces carry no NetworkPolicy, so an approved fork run would have unrestricted east-west access to the cluster, and approving a pull request should not double as a cluster-security decision. Confining them is [issue #182](https://github.com/ydixken/pgcopydb-operator/issues/182); until it lands, that expression is the control.

The e2e job backs the first rule with something GitHub enforces rather than something we remember. Its `environment: e2e-cluster` carries a deployment branch and tag policy that permits `main` and `v*` and nothing else, evaluated before the job is dispatched. A fork pull request runs at `refs/pull/N/merge`, matches neither, and never reaches a machine that can talk to the cluster.

`pgcopydb-builder.yml` is the one workflow that deliberately uses a hosted runner. It compiles pgcopydb from C source for both architectures, and building arm64 on this cluster means QEMU, which was the entire cost of that job: 933 of 964 seconds. The vendored `sqlite3.c` is a single 9MB translation unit, so more cores do not help, and a layer cache cannot help either, because the things that rebuild it at all (a new pgcopydb commit, a Renovate bump of the debian digest) invalidate that layer by definition. Each architecture is built on a machine of that architecture instead, pushed by digest, and joined into one tag by `docker buildx imagetools create`. The arm64 half runs on `ubuntu-24.04-arm`, free for a public repository, and the workflow is not fork-triggerable.

[release.yml](.github/workflows/release.yml) calls that workflow rather than repeating it. Its `builder-check` job asks whether the pinned tag is already published, which it almost always is, and `builder-build` runs the split only when it is not. `runner-image` then guards on `!cancelled()` rather than a plain `needs`, because a skipped dependency would otherwise skip the release itself.

`runner-smoke.yml` is a `workflow_dispatch` build that exercises the build runner and its Docker daemon without publishing anything. Run it after any change to that scale set.

The runners boot `ghcr.io/ydixken/pgcopydb-operator/github-runner`, built from [`images/github-runner/`](images/github-runner/) by [github-runner-image.yml](.github/workflows/github-runner-image.yml). The stock `actions-runner` it starts from carries git, curl, jq, python3 and a Docker client, where a hosted runner ships hundreds of tools, so every job used to install the difference at runtime. The image bakes it instead: Go, make, gh, psql, Helm, kubectl, promtool, oras, yamllint, mkdocs-material, and the Makefile's own tools at a baked `LOCALBIN` with the envtest binaries and a warm module cache. `verify.sh` runs inside the image before it is pushed, so one that is missing something never becomes the tag the scale sets boot.

Versions are pinned so the image is reproducible and Renovate can see them, except Go, promtool and crd-ref-docs, which are read out of `go.mod`, `hack/ensure-promtool.sh` and `Taskfile.yml` at build time so the image cannot drift from what the Makefile and the docs gate use. Merging a Renovate bump rebuilds the image; the weekly schedule is for the apt packages, which move on their own.

No job runs `actions/setup-go`. The image carries Go, and `GOTOOLCHAIN` is left at its default so a `go.mod` that has moved ahead of the image fetches the toolchain it asks for rather than failing. Pinning it to `local` deadlocked instead: the image rebuilds only once a bump is on `main`, so the pull request making the bump would be red with no way through, and a push to main races its own rebuild.

`setup-helm` and `setup-python` survive in [ci.yml](.github/workflows/ci.yml) alone, gated on `github.event.pull_request.head.repo.fork`, because that is the one workflow whose jobs can land on a hosted runner, and `ubuntu-latest` has neither Helm nor a guaranteed Python.

`task lint` runs [actionlint](https://github.com/rhysd/actionlint), which yamllint cannot replace: a step that lost its `uses:` is still valid YAML. One shipped that way, and because `release.yml` only runs on a tag, nothing would have noticed until a release failed. [`.github/actionlint.yaml`](.github/actionlint.yaml) declares the two self-hosted labels so the real findings are not buried under unknown-label warnings.

The Go build cache, the module cache, golangci-lint's analysis cache and the buildx layer cache live on the node under `/cache`, mounted by the scale set. Nothing goes through GitHub's cache service: the jobs used to spend 109 and 173 seconds shipping 444MB of Go cache to the internet and pulling it back, and buildx would have done the same with the image layers.

## E2e tests

`task e2e` runs `test/e2e/` against the CURRENT kubectl context, a real cluster; it prints the context and prompts before touching anything (see the Caution section in [AGENTS.md](AGENTS.md)).
The suite installs a throwaway operator, creates two single-instance CNPG clusters by default, and seeds the source through a Kubernetes Job running `test/e2e/fixtures/run.sh`.
That script applies `schema.sql`, runs the three base seed stages concurrently, and starts `E2E_EXTRA_JOBS` extra-table workers before applying `finish.sql`.
The two bulk tables are 92% of the base seed and are bound by different resources, `events` per row and `documents` per byte, so they overlap instead of queueing.
The non-unique secondary indexes are built once by `finish.sql` rather than maintained per insert.
Primary keys and unique constraints stay in `schema.sql` because the loads resolve `ON CONFLICT` against them.
Seeding is idempotent: an `e2e_seed` marker table records profile and scale, a matching marker skips the seed, and a kept cluster with a mismatching marker is recreated.

Two tiers, and the environment variables a run reads:

| Variable                      | Default | Effect                                                                                                                                                     |
|-------------------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `E2E_SCALE`                   | `1`     | Fixture size multiplier, sizing both the seeded data and the volumes. 1 seeds roughly 12GB on 50Gi volumes; 0.25 seeds 3GB on 13Gi, which is what CI runs. |
| `E2E_CNPG_INSTANCES`          | `1`     | Instances per fixture CNPG cluster. One keeps setup and teardown short, and the migration still crosses the network because source, target and worker are separate pods. Raise it to 3 for the chaos scenarios, one of which kills a primary. |
| `E2E_EXTRA_TABLES`            | unset   | Adds this many extra tables on top of the base fixture, with sizes drawn from a normal distribution and normalised to `E2E_EXTRA_SIZE_GB`. The base fixture is deliberately lopsided (one table holds 73% of the bytes); this gives it a production shape. Must be set with `E2E_EXTRA_SIZE_GB`. |
| `E2E_EXTRA_SIZE_GB`           | unset   | Total size of the extra tables. Both fixture volumes grow by twice this, because the bytes are written once by the seed and again by WAL. Changing either value changes the seed marker, so a kept fixture is rebuilt rather than reused at the old shape. |
| `E2E_EXTRA_JOBS`              | `4`     | Concurrent psql sessions that seed the extra tables. Each session derives the same deterministic layout and builds its assigned tables. |
| `E2E_STRESS`                  | unset   | `true` selects the stress tier: scale 10 (~120GB), 200/150/50Gi volumes per instance, longer budgets. Use `task e2e:stress`.                               |
| `E2E_KEEP_FIXTURES`           | unset   | `true` keeps the fixture namespaces and clusters for iteration; the next run reuses them and skips a matching seed.                                        |
| `E2E_FORCE`                   | unset   | `true` takes over the helm release a crashed run left behind.                                                                                              |
| `E2E_PG_SOURCE`               | `17`    | PostgreSQL major (14 to 18) for the source cluster's CNPG operand image.                                                                                   |
| `E2E_PG_TARGET`               | `17`    | PostgreSQL major for the target. MUST NOT be older than the source, and MUST be at least 15 (see below).                                                   |
| `E2E_OPERATOR_TAG`            | unset   | Manager image tag to install instead of the pinned release; the runner follows it.                                                                         |
| `E2E_RUNNER_TAG`              | unset   | Worker image tag on its own, for an unreleased `images/runner` build; building one locally pulls the pinned `pgcopydb-builder` tag from ghcr.              |
| `E2E_STORAGE_CLASS`           | unset   | Pins the fixture volumes to one StorageClass, and wins over the suite-owned one. Setting it also skips the capacity check.                                 |
| `E2E_MANAGE_NAMESPACES`       | `true`  | `false` works inside namespaces someone else owns: creates and deletes none, installs with `rbac.create=false`.                                            |
| `E2E_PROMETHEUS_URL`          | unset   | Base URL of a Prometheus that scrapes the suite's operator install; enables the metrics specs.                                                             |
| `E2E_PROMETHEUS_PORT_FORWARD` | unset   | `namespace/service:port` of a Prometheus Service; the suite spawns and owns the kubectl port-forward to it.                                                |

Outside the stress tier the fixture volumes follow the scale, down from 50/50/12Gi at scale 1, with a floor at an eighth of that: a 0.1 run gets 7/7/2Gi and the 0.25 CI tier gets 13/13/3Gi. `max_wal_size` follows the volume at a fifth of it, because CNPG keeps `pg_wal` inside PGDATA and a flat value sized for a big fixture fills a small one outright. The floor is there because WAL, indexes and the change spool need headroom that the row counts alone do not size. Source and target sizes are per instance, so a three-instance cluster provisions three of them; the work volume is one per migration and does not multiply.

The metrics specs (`test/e2e/metrics_test.go`, Ginkgo label `metrics`) replay the whole monitoring path against a real Prometheus: scrape health, the live series of a streaming migration, the terminal series after cutover, every dashboard panel query, and series removal on deletion. They need a Prometheus that scrapes the suite's operator install; the chart's ServiceMonitor (always enabled by the suite, inert without the Prometheus Operator CRDs) provides the target. Set `E2E_PROMETHEUS_URL` when the suite can reach Prometheus directly, or `E2E_PROMETHEUS_PORT_FORWARD` (for example `monitoring/kube-prometheus-stack-prometheus:9090`) to have the suite tunnel through kubectl. With neither knob the specs Skip; with a knob that points nowhere they fail, because a misconfigured gate must be red. They assert metrics of the installed operator, so point `E2E_OPERATOR_TAG` at a build that exports them when the pinned default predates the metrics work.

A kept cluster the run cannot adopt in place is deleted and recreated before the suite proceeds. Three things force that: a server on a different major than `E2E_PG_SOURCE`/`E2E_PG_TARGET` request, because CNPG cannot change majors in place; a different instance count; and a different StorageClass, which is immutable once a PVC is bound.

`task e2e:matrix` runs the full suite (chaos specs excluded) three times at `E2E_SCALE=0.1`, one version combo per run: PG 14 to 18, 18 to 18, and 15 to 17. One confirmation prompt up front covers all three; each combo is echoed before it starts. The fixture namespaces stay up between combos (only a cluster on the wrong major gets recreated) and the last combo tears them down. A failing combo does not stop the rest: the task prints a pass/fail summary at the end and exits nonzero if any combo failed. The matrix is upgrade-direction only because pgcopydb needs `pg_dump` at least at the target's major and a newer major's dump does not restore into an older server. PG14 appears as a source only because the follow-mode target contract includes `GRANT SET ON PARAMETER session_replication_role`, which PostgreSQL grew in 15 ([docs/reference/prerequisites.md](docs/reference/prerequisites.md)).

Every tier puts the fixture volumes on a `longhorn-e2e-ephemeral` StorageClass (numberOfReplicas 1, reclaimPolicy Delete) that the suite creates if absent, and refuses to start (Skip) unless the `nodes.longhorn.io` CRs report enough available storage for the requested volumes times 1.2 headroom.
One Longhorn replica is deliberate: CNPG already manages its own instances, so a three-replica StorageClass would store three copies beneath every instance without adding coverage the suite can observe.
The capacity check reads live cluster state; nothing about the cluster is hardcoded.
On a cluster without Longhorn the fixtures fall back to the default StorageClass and no capacity check runs.

The fixtures are placed, not left to the scheduler.
Each CNPG cluster has one instance by default and uses preferred pod anti-affinity over `kubernetes.io/hostname` when `E2E_CNPG_INSTANCES` adds replicas.
The runner Jobs carry anti-affinity against the two primaries so a migration's SQL legs cross the network instead of looping back inside one node.
The target additionally repels the source's first instance, because CNPG's own anti-affinity only separates instances of the same cluster and the two primaries would otherwise share whichever node scores highest.
Every suite-created pod also declares CPU and memory requests.
A pod that requests nothing scores identically on every node, so the least-allocated node wins every scheduling decision and never gets any less attractive, and an entire run piles onto one node.
All placement rules are preferred, so a smaller cluster can co-locate the pods and still pass.

Fixture servers get 2 CPUs and 4Gi, and the seed Job and runner Jobs the same. Requests only, so nothing is throttled. The caches are set by hand alongside them (`shared_buffers`, `effective_cache_size`, `maintenance_work_mem`, `wal_buffers`, `max_wal_size`, `checkpoint_timeout`), because CNPG does not derive `shared_buffers` from the memory request: raising the request on its own would leave PostgreSQL on its 128MB default and the clone would spend its time reading pages back off the volume, measuring the storage instead of the operator.

Two specs cover this. One reads what was rendered onto the pods, an anti-affinity term and non-zero requests, which is namespaced and so runs anywhere; the other counts the nodes the instances actually occupy, which needs to read nodes and skips where that is not permitted. The first is the one that binds: CloudNativePG defaults to a preferred hostname anti-affinity on its own, so the fixtures would still spread, and the node count alone would still pass, with the suite's own configuration deleted.

Chaos scenarios live in `test/e2e/chaos_test.go` behind the Ginkgo label `chaos`: they kill fixture pods (CNPG primaries, the runner mid-drain), overflow a follow migration's change spool on a deliberately tiny work volume, and fan two concurrent follow migrations out of one source. `task e2e` and `task e2e:stress` exclude them (`-ginkgo.label-filter='!chaos'`); `task e2e:chaos` runs exactly them, with the same context echo and confirmation prompt. Each chaos spec creates its own Migration and restores what it disturbed, so the set runs standalone against kept fixtures. The source-kill spec times its kill off `pg_stat_progress_copy` on the target and Skips below `E2E_SCALE` 0.05, where the documents COPY gets too short to hit reliably.

`release.yml` runs this suite too, against a release candidate rather than a branch: `E2E_SCALE=0.25`, chaos excluded, `E2E_OPERATOR_TAG` set to the candidate so it installs the images that run was built from, and `E2E_MANAGE_NAMESPACES=false` because there the namespaces belong to GitOps and the CI identity may not create one. It calls `go test` directly, not `task e2e`: that target's confirmation prompt exists for a developer who could be pointed at any cluster, and answering it with `task --yes` is forbidden. `E2E_PROMETHEUS_URL` comes from a repository variable, and a guard step fails the job when the variable is unset, so the metrics gate can never shrink to a silent Skip; `e2e.yml` guards the same way.

## Releasing

Releases cut themselves. Every Monday at 08:00 UTC `auto-release.yml` reads what landed since the last stable tag and pushes a release candidate: `vX.Y.Z-rc.1`, a patch bump unless a `feat:` commit is in the range, in which case a minor one. A week with nothing merged ends with no tag and a green run, which is not a failure. When a candidate for the same version already exists the number counts up, rather than reusing a tag whose images are published.

That tag starts `release.yml`, which publishes the manager and runner images (multi-arch) and the Helm chart as OCI, creates the GitHub release whose notes GitHub generates from the merged PRs, and runs the e2e suite against exactly those artifacts on the cluster.

Every job below `builder-build` carries `!cancelled()` in its condition, and that is load-bearing rather than defensive. `builder-build` skips on the normal path, because the pinned builder tag is almost always published already, and GitHub propagates that skip to every descendant: a job in between that survives it with its own status function still passes the skip along to its own dependants. `v0.12.1-rc.2` published two images and then skipped the chart, the release notes and e2e, reporting success.

Fail, and the candidate's artifacts stay where they are, `latest` still points at the last stable release, and the workflow opens an issue naming the run. Fix forward on `main`, and the next candidate carries the fix.

Pass, and nothing happens on its own. Promotion is [promote.yml](.github/workflows/promote.yml), dispatched by hand with the candidate tag. That is what lets several candidates stand between two releases: a candidate that is never promoted just stays a candidate, and rc.2 can supersede rc.1 without rc.1 having already become the release.

Promoting pushes the stable tag `vX.Y.Z`, which starts `release.yml` once more on the same commit: the same images from the same context, and this time `latest` moves and the release is not marked a prerelease.

The gate used to be `needs: [e2e, release-notes]`, which GitHub enforced. A manual promotion has to earn that back, so `promote.yml` reads the candidate's own release run and refuses unless its `e2e` job concluded `success`. Skipped, cancelled and never-ran are all refusals, not passes.

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

- `artifacthub.io/crdsExamples` duplicates `docs/examples/01-clone-minimal.yaml` and `docs/examples/03-clone-platform-secret.yaml` with the comments stripped. Nothing enforces the copies, so update them when those examples change. Artifact Hub matches an example to a CRD by kind and renders only the first match, so keep the minimal clone first; further entries serve readers of the annotation itself.
- `artifacthub.io/images` lists the runner image explicitly. The runner reaches the cluster as a `--runner-image` flag rather than as a container in a manifest, so Artifact Hub cannot discover it, and without the annotation it is never scanned for vulnerabilities.

## Commits and pull requests

- Conventional commits: `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `ci:`.
- One logical change per commit. Every commit MUST be lint-clean on its own.
- `main` is protected: changes land via GitHub PRs, and `lint`, `test` and `docs` MUST be green. Nobody pushes to `main` directly.
