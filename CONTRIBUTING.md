# CONTRIBUTING.md

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119.

[TOC]

## Requirements

- [go](https://go.dev): the operator's language and test toolchain.
- [task](https://taskfile.dev): task runner, the entrypoint for everything.
- [yamllint](https://yamllint.readthedocs.io): lints all YAML.
- [golangci-lint](https://golangci-lint.run) v2: Go linting.
- [kubectl](https://kubernetes.io/docs/reference/kubectl/): only needed for `task e2e`.
- [gh](https://cli.github.com): PRs happen on GitHub.
- Docker MAY be installed for local image builds; CI builds the published image.
  Building `images/runner` locally pulls the pinned `pgcopydb-builder` tag from ghcr instead of compiling pgcopydb; see [images/pgcopydb-builder](images/pgcopydb-builder/README.md).

## Day-to-day loop

1. Branch from `main`.
1. Make one logical change.
1. If the change touches `api/v1beta1`, run `make manifests`, `hack/sync-chart-crd.sh`, and `task docs`, then commit the regenerated CRD, chart template, and `docs/reference/api.md` with it.
1. If the change touches a `+kubebuilder:rbac` marker, run `make manifests` and then `hack/sync-chart-rbac.sh`, and commit the regenerated `config/rbac/role.yaml` and chart templates with it.
   The chart's rules are generated from `config/rbac`, and `task lint` fails when the two disagree.
1. Format touched files before every commit and run `task lint`.
   For CRD or RBAC changes, inspect and commit the generated files first, then rerun `task lint` before pushing: staged but uncommitted generated files fail the check.
1. Commit (see below), push the branch to GitHub, open a PR.
1. Merge when the CI `lint`, `test`, and `docs` checks are green on the current head.
   No pull request runs the E2E suite; the next release candidate does (see [Releasing](#releasing)).

`.github/workflows/ci.yml` runs lint, tests and the docs build on every push and pull request, and those three jobs are the required checks on `main`.
The GitLab project (`gitlab.com/ydixken/pgcopydb-operator`) is a push mirror and nothing else: it keeps the branches and tags off GitHub, runs no pipeline, and never takes a commit or an MR.
The pull request `lint` job runs GitHub Dependency Review and rejects new dependencies with moderate or higher known vulnerabilities, disallowed licenses, or violations in runtime, development, or unknown scopes.
GitHub cannot fail Dependency Review for every unresolved license, so contributors MUST review those warnings and resolve each license from a public source before merging.

The CI `lint` job and local `task lint` run `make manifests` and fail if any file in `config/crd/bases` or `config/rbac` differs from the index or is untracked.
The check prints the offending file paths.
Staged changes in those directories also fail the check.
Regeneration writes to the working tree; inspect the generated changes and include them in the API or RBAC marker commit, then rerun lint to verify the committed tree.

The CI `test` job runs beside throwaway PostgreSQL 16 and 18 service containers and points `PGCOPYDB_TEST_PGURI` at the 16 one.
A second step runs the coverage tests against 18, because release candidates never stream a case that needs it.
`TestPreflightExtensionOwnershipQueries`, `TestPreflightOwnerAfterRestoreQueries`, `TestReownCandidateQueries` and `TestPreflightTableAuditQueries` require that variable and a working `psql`; `task test` fails without them rather than leaving the ownership and table audit SQL untested.
The two `ownerAfterRestore` tests run the handover's candidate query and the preflight's `SET ROLE` probe against a live server, because both have version-dependent answers (multirange types, `WITH SET FALSE` membership) that no golden test can pin down.
That variable enables `TestCompareDataQuery` and the progress sampler SQL cancellation regressions; without it those tests skip.
The sampler tests own temporary databases and relation locks, verify cancellation and recovery, and remove their fixtures afterward.

Coverage goes to Codecov, gated on the `CODECOV_TOKEN` repository secret.
Codecov rejects tokenless uploads even from public repositories, so without the secret the upload step skips visibly rather than passing quietly; with it set, a failed upload fails the job.
The coverage total is printed in the job summary either way.
`codecov.yml` excludes the `zz_generated*.go` files controller-gen writes, so the Codecov number reflects hand-written code.

## Self-hosted runners

Two runner scale sets serve this repository, both backed by Actions Runner Controller on the dev cluster, and between them they run every job.
The scale sets, their GitHub App credentials and their Helm values are declared outside this repository (see private ops notes); nothing here configures them beyond the `runs-on:` label.

`github-runner-pgcopydb-operator` runs builds, publication, and base CI.
Its jobs get no Kubernetes API credentials for the cluster they run on.
`github-runner-pgcopydb-e2e` runs release candidate and published-release E2E and can reach that Kubernetes API.
Its ServiceAccount is scoped to the e2e namespaces, which GitOps owns; it can work inside them but cannot create or delete one, which is why CI runs the suite with `E2E_MANAGE_NAMESPACES=false`.

> [!warning]
> Two rules hold because this repository is public and both scale sets are real machines on a private cluster:
>
> - No workflow that can be triggered by a fork MAY target them, on any code path a fork can reach.
> - `pull_request_target` MUST NOT be used in any workflow.
>   It runs the base branch's copy of the workflow, with the base branch's secrets, against a fork's code, so the approval that gates a fork's first run never gets asked for.

`ci.yml` runs on `pull_request`, so it is the one workflow a fork can trigger, and it picks its runner per event instead of pinning one:

```yaml
runs-on: ${{ github.event.pull_request.head.repo.fork && 'ubuntu-latest' || 'github-runner-pgcopydb-operator' }}
```

A push carries no `pull_request` payload and falls through to the cluster runner, as does a pull request from a branch in this repository; only a fork's pull request lands on a hosted one.
GitHub's own gate is at its strictest setting (`approval_policy` is `all_external_contributors`), and a public-repo fork run gets a read-only token and no secrets.
That gate is not what we lean on: approving a fork's pull request would also decide whether its code reaches a cluster runner, and a review approval should not double as a cluster-security decision.
[Issue #182](https://github.com/ydixken/pgcopydb-operator/issues/182) tracks enforcing that in the cluster; until it lands, the expression above is the control.

The e2e job backs the first rule with a control GitHub enforces.
Its `environment: e2e-cluster` carries a deployment branch and tag policy that permits `main` and `v*` and nothing else, evaluated before the job is dispatched.
A fork pull request runs at `refs/pull/N/merge`, matches neither, and never reaches a machine that can talk to the cluster.

`pgcopydb-builder.yml` is the one workflow that targets a hosted runner unconditionally.
It compiles pgcopydb from C source for both architectures, and building arm64 on the cluster runner means QEMU, which dominated that job ([measured](docs/research/measurements.md#qemu-emulation-was-the-whole-cost-of-the-builder-image)).
A layer cache does not help either, because the things that rebuild it at all (a new pgcopydb commit, a Renovate bump of the debian digest) invalidate that layer by definition.
Each architecture is built on a machine of that architecture instead, pushed by digest, and joined into one tag by `docker buildx imagetools create`.
The arm64 half runs on `ubuntu-24.04-arm`, free for a public repository, and the workflow is not fork-triggerable.

[release.yml](.github/workflows/release.yml) calls that workflow rather than repeating it.
Its `builder-check` job tests whether the pinned tag is already published, which it almost always is, and `builder-build` runs the split only when it is not.
`runner-image` then guards on `!cancelled()` rather than a plain `needs`, because a skipped dependency would otherwise skip the release itself.

`runner-smoke.yml` is a `workflow_dispatch` build that exercises the build runner and its Docker daemon without publishing anything.
Run it after any change to that scale set.

The runners boot `ghcr.io/ydixken/pgcopydb-operator/github-runner`, built from [`images/github-runner/`](images/github-runner/) by [github-runner-image.yml](.github/workflows/github-runner-image.yml).
The stock `actions-runner` it starts from carries git, curl, jq, python3 and a Docker client, where a hosted runner ships hundreds of tools, so every job used to install the difference at runtime.
The image bakes it instead: Go, make, gh, psql, Helm, kubectl, promtool, oras, yamllint, mkdocs-material, and the Makefile's own tools at a baked `LOCALBIN` with the envtest binaries and a warm module cache.
`verify.sh` runs inside the image before it is pushed, so one that is missing something never becomes the tag the scale sets boot.

Versions are pinned so the image is reproducible and Renovate can see them, except Go, promtool and crd-ref-docs, which are read out of `go.mod`, `hack/ensure-promtool.sh` and `Taskfile.yml` at build time so the image cannot drift from what the Makefile and the docs gate use.
Merging a Renovate bump rebuilds the image; the weekly schedule is for the apt packages, which move on their own.

No job runs `actions/setup-go`.
The image carries Go, and `GOTOOLCHAIN` is left at its default so a `go.mod` that has moved ahead of the image fetches the toolchain it asks for rather than failing.
Pinning it to `local` deadlocks: the image rebuilds only once a bump is on `main`, so the pull request making the bump would be red with no way through, and a push to main races its own rebuild.

`setup-helm` and `setup-python` survive in [ci.yml](.github/workflows/ci.yml) alone, gated on `github.event.pull_request.head.repo.fork`, because that is the one workflow whose jobs can land on a hosted runner, and `ubuntu-latest` has neither Helm nor a guaranteed Python.

The CI `lint` job runs [actionlint](https://github.com/rhysd/actionlint) on all workflows; yamllint alone cannot detect a step that lost its `uses:`.
If the runner lacks actionlint, CI installs the version pinned in `images/github-runner/Dockerfile` and fails if installation or linting fails.
Local `task lint` runs actionlint when installed and prints a skip message otherwise.
[`.github/actionlint.yaml`](.github/actionlint.yaml) declares the two self-hosted runner labels.

The Go build cache, the module cache, golangci-lint's analysis cache and the buildx layer cache live on the node under `/cache`, mounted by the scale set.
Nothing goes through GitHub's cache service: a round trip to it cost more than it returned ([measured](docs/research/measurements.md#a-github-cache-round-trip-for-the-runner-image)), and buildx would have done the same with the image layers.

## E2e tests

`task e2e` runs `test/e2e/` against the CURRENT kubectl context, a real cluster; it prints the context and prompts before touching anything (see the Caution section in [AGENTS.md](AGENTS.md)).
Every e2e Task target then passes the context it confirmed to the suite as `E2E_CONTEXT`, assigned in the command itself because Task lets an exported variable override a Taskfile `env:` value.
The cluster entry point `TestE2E` skips when `E2E_CONTEXT` is unset, and fails before any cluster call when it names a context other than `kubectl config current-context`.
We make the context an explicit input because the suite's teardown deletes the fixture namespaces, so a plain `go test ./test/e2e` or `go test ./...` must never reach a cluster that nobody named.
The suite installs an OpenTelemetry Collector in the operator namespace and a throwaway operator that exports its metrics to it over OTLP.
It creates a source/target CNPG pair for each Ginkgo process, with one instance per cluster by default.
If the suite can list nodes, each one-instance cluster prefers one node, so CNPG's initdb pod and the instance pod use the same volume attachment.
Pair N's source prefers usable node N in name order, and its target prefers the next node.
It seeds each source through a Kubernetes Job running `test/e2e/fixtures/run.sh`.
With the `E2E_SOURCE_*` and `E2E_TARGET_*` variables set it runs in external mode instead: it uses an existing database pair, creates no CNPG clusters, and seeds the source through the same Job (see [Running the E2E suite against your databases](docs/operations/e2e-external.md)).
That script applies `schema.sql`, runs the three base seed stages concurrently, and starts `E2E_EXTRA_JOBS` extra-table workers before applying `finish.sql`.
The two bulk tables are 92% of the base seed and are bound by different resources, `events` per row and `documents` per byte, so they overlap instead of queueing.
The non-unique secondary indexes are built once by `finish.sql` rather than maintained per insert.
Primary keys and unique constraints stay in `schema.sql` because the loads resolve `ON CONFLICT` against them.
After refreshing the materialized view, `finish.sql` runs `VACUUM (ANALYZE)` across the source database before writing the success marker, setting hint bits before a migration opens its replication slot.
Seeding is idempotent: an `e2e_seed` marker table records profile and scale, a matching marker skips the seed, and a kept cluster with a mismatching marker is recreated.
The fixture manifest leaves `bootstrap.initdb.dataChecksums` unset; the [CNPG option defaults to `false`](https://cloudnative-pg.io/docs/1.27/bootstrap/#passing-options-to-initdb).

The pooling spec creates its own ephemeral CNPG pair because [managed Pooler authentication](https://cloudnative-pg.io/docs/1.27/connection_pooling/#authentication) installs objects in `postgres`, even when clients use `app`.
Those objects can persist after Pooler deletion, so a separate application database does not isolate the shared maintenance catalogs used by all-databases clones.
The dedicated pair follows `E2E_PG_SOURCE`/`E2E_PG_TARGET` and the fixture StorageClass, but always uses one instance and a 1Gi PVC per cluster, independent of scale, stress, and `E2E_CNPG_INSTANCES`.
It requests 100m CPU and 256Mi memory per instance, uses PostgreSQL's default caches, and sizes `max_wal_size` at a fifth of its volume.
Cleanup registers after each successful create and removes the runner and Poolers before the clusters, checks deletion UIDs, and waits for cluster-labelled pods and UID-owned PVCs to disappear, even with `E2E_KEEP_FIXTURES=true`.
The spec requires dedicated-cluster Pooler ownership and unchanged schema/function signatures and owners in both shared `postgres` databases while pooling and after cleanup; it reads no authentication function bodies or secret values.

> [!important]
> Recreate kept shared fixtures whose maintenance databases contain Pooler authentication objects before running all-databases tests.
> `E2E_KEEP_FIXTURES=false` controls teardown after a run; it does not clean those databases before the run.

The pooling fixture requires the E2E runner to have `create`, `get`, and `delete` on `poolers.postgresql.cnpg.io`, in addition to its existing CNPG Cluster, pod, and PVC permissions, including `pods/exec`.
Dedicated-cluster readiness and cleanup require `get`, `list`, and `delete` on PVCs in `pgcopydb-e2e`.
Grant those three Pooler verbs through a dedicated Role and RoleBinding in `pgcopydb-e2e` only.
The runner's ServiceAccount and GitOps RBAC live outside this repository (see private ops notes).
The [chart's RBAC and ServiceAccount values](charts/pgcopydb-operator/README.md#rbac-and-serviceaccounts) configure the manager, which does not manage CNPG fixtures; changing them cannot repair a runner authorization failure.

The suite has two tiers, default and stress, and a run reads these environment variables, each shown with the default it takes when unset:

- `E2E_CONTEXT` (unset) MUST name the current kubectl context, or be `in-cluster` when no kubeconfig sets one, for `TestE2E` to run.
  The Task targets set it; set it yourself only when you call `go test` or the ginkgo CLI directly.
  Point the suite at another kubeconfig with `KUBECONFIG`: `TestE2E` refuses the `-kubeconfig` flag, because the suite's `kubectl` and `helm` calls would not follow it.
- `E2E_SCALE` (`1`) multiplies the fixture and volume sizes.
  Scale 1 seeds roughly 12GB on 50Gi volumes; release candidate CI uses 0.1, roughly 1.2GB on 7Gi.
- `E2E_CNPG_INSTANCES` (`1`) sets the instances per shared source/target CNPG cluster.
  Raise it to 3 for chaos failover coverage.
  The pooling pair stays at one instance each.
- `E2E_EXTRA_TABLES` (unset) adds this many extra tables on top of the base fixture, with sizes drawn from a normal distribution and normalised to `E2E_EXTRA_SIZE_GB`.
  The base fixture is lopsided, one table holding 73% of the bytes, and the extra tables give it a production shape.
  Must be set with `E2E_EXTRA_SIZE_GB`.
- `E2E_EXTRA_SIZE_GB` (unset) is the total size of the extra tables.
  Both fixture volumes grow by twice this, because the bytes are written once by the seed and again by WAL.
  Changing either value changes the seed marker, so a kept fixture is rebuilt rather than reused at the old shape.
- `E2E_EXTRA_SKEW` (unset) draws the extra-table sizes from a lognormal distribution instead, so a few large tables carry most of the bytes.
  It takes a number above 0 and at most 10, and requires `E2E_EXTRA_TABLES`; larger values concentrate more of the total in fewer tables.
  It is part of the seed marker, so changing it rebuilds a kept fixture.
- `E2E_EXTRA_JOBS` (`4`) sets the concurrent psql sessions that seed the extra tables.
  Each session derives the same deterministic layout and builds its assigned tables.
  Tables go largest first to the least-loaded session, so a few large tables do not queue behind each other on one session.
  Each table loads in transactions of about 128MB, and a retried seed Job resumes a table after its highest committed id.
- `E2E_STRESS` (unset) selects the stress tier when `true`: scale 10 (~120GB), 200/150/50Gi volumes per instance, longer budgets.
  Use `task e2e:stress`.
- `E2E_SEED_TIMEOUT` (`30m`, `3h` under stress) bounds the seed Job, as a Go duration such as `4h`.
  Set it for a fixture larger than the tier was sized for, for example a large `E2E_EXTRA_SIZE_GB`.
- `E2E_MIGRATION_TIMEOUT` (`30m`, `2h` under stress) bounds each wait for a Migration to reach a phase, as a Go duration.
  The run's own `go test` timeout still has to cover the seed and the migrations; `task e2e:focus` and `task e2e:focus:unattended` take it as `TIMEOUT=` (default `1h`), and the two chaos tasks take it the same way (default `3h`).
- `E2E_KEEP_FIXTURES` (unset) keeps the fixture namespaces and shared clusters for iteration when `true`, and the next run reuses them and skips a matching seed.
  The pooling pair is always removed.
- `E2E_FORCE` (unset) takes over the helm release a crashed run left behind when `true`.
- `E2E_PG_SOURCE` (`17`) is the PostgreSQL major (14 to 18) for the source cluster's CNPG operand image.
- `E2E_PG_TARGET` (`17`) is the PostgreSQL major for the target.
  It MUST NOT be older than the source, and MUST be at least 15 (see below).
- `E2E_OPERATOR_TAG` (unset) is a manager image tag to install instead of the pinned release; the runner follows it.
- `E2E_RUNNER_TAG` (unset) is the worker image tag on its own, for an unreleased `images/runner` build; building one locally pulls the pinned `pgcopydb-builder` tag from ghcr.
- `E2E_STORAGE_CLASS` (unset) pins the fixture volumes to one StorageClass, and wins over the suite-owned one.
  Setting it also skips the capacity check.
- `E2E_MANAGE_NAMESPACES` (`true`) set to `false` works inside namespaces someone else owns: it creates and deletes none, and installs with `rbac.create=false`.
- `E2E_PROMETHEUS_URL` (unset) is the base URL of a Prometheus that scrapes the suite's operator install, and enables the metrics specs.
- `E2E_PROMETHEUS_PORT_FORWARD` (unset) is the `namespace/service:port` of a Prometheus Service; the suite spawns and owns the kubectl port-forward to it.
- `E2E_SOURCE_URI` (unset) switches to external mode, which runs against an existing database pair instead of CNPG fixtures; use `task e2e:external`.
  Setting it or any of its siblings makes all eight required: `E2E_SOURCE_URI`, `E2E_SOURCE_PASSWORD`, `E2E_SOURCE_ADMIN_URI`, `E2E_SOURCE_ADMIN_PASSWORD`, and the four `E2E_TARGET_*` counterparts.
  The URIs carry no password and no query parameter except `sslmode`.
  External mode reads the server majors from the servers, so it rejects `E2E_PG_SOURCE` and `E2E_PG_TARGET`, and `E2E_CNPG_INSTANCES` has no effect.

Outside the stress tier the fixture volumes follow the scale, down from 50/50/12Gi at scale 1, with a floor at an eighth of that: the 0.1 release candidate tier gets 7/7/2Gi.
`max_wal_size` follows the volume at a fifth of it, because CNPG keeps `pg_wal` inside PGDATA and a flat value sized for a big fixture fills a small one outright.
The floor is there because WAL, indexes and the change spool need headroom that the row counts alone do not size.
Source and target sizes are per instance, so raising `E2E_CNPG_INSTANCES` multiplies those volumes; the work volume is one per migration and does not multiply.
Fixed WAL-noise fixtures remain unscaled because they must exceed the default `16Mi` lag allowance.

The metrics specs (`test/e2e/metrics_test.go`, Ginkgo label `metrics`) replay the whole monitoring path against a real Prometheus: scrape health, the live series of a streaming migration, the terminal series after cutover, every dashboard panel query, and series removal on deletion.
They need a Prometheus that scrapes the suite's operator install; the chart's ServiceMonitor (always enabled by the suite, inert without the Prometheus Operator CRDs) provides the target.
Set `E2E_PROMETHEUS_URL` when the suite can reach Prometheus directly, or `E2E_PROMETHEUS_PORT_FORWARD` (for example `monitoring/kube-prometheus-stack-prometheus:9090`) to have the suite tunnel through kubectl.
With neither knob the specs Skip; with a knob that points nowhere they fail, because a misconfigured gate must be red.
They assert metrics of the installed operator, so point `E2E_OPERATOR_TAG` at a build that exports them when the pinned default predates the metrics work.

A kept cluster the run cannot adopt in place is deleted and recreated before the suite proceeds.
Three things force that: a server on a different major than `E2E_PG_SOURCE`/`E2E_PG_TARGET` request, because CNPG cannot change majors in place; a different instance count; and a different StorageClass, which is immutable once a PVC is bound.

`task e2e:matrix` runs the full suite (chaos specs excluded) three times at `E2E_SCALE=0.1`, one version combo per run: PG 14 to 18, 18 to 18, and 15 to 17.
One confirmation prompt up front covers all three; each combo is echoed before it starts.
The fixture namespaces stay up between combos (only a cluster on the wrong major gets recreated) and the last combo tears them down.
A failing combo does not stop the rest: the task prints a pass/fail summary at the end and exits nonzero if any combo failed.
The matrix is upgrade-direction only because pgcopydb needs `pg_dump` at least at the target's major and a newer major's dump does not restore into an older server.
PG14 appears as a source only because the follow-mode target contract includes `GRANT SET ON PARAMETER session_replication_role`, which PostgreSQL grew in 15 ([docs/reference/prerequisites.md](docs/reference/prerequisites.md)).

When `E2E_STORAGE_CLASS` is unset and the suite-owned path is selected, the suite creates and capacity-checks its ephemeral StorageClass; release callers that supply an existing class through the override use that class and skip suite-owned setup and capacity checking.
The suite-owned class uses one Longhorn replica: CNPG already manages its own instances, so a three-replica StorageClass would store three copies beneath every instance without adding coverage the suite can observe.
The class also sets `dataLocality: best-effort`, so the single replica follows its pod to the pod's node and the pairs' I/O spreads over the nodes' disks instead of landing on the disk with the most free space.
The capacity check reads live cluster state; nothing about the cluster is hardcoded.
Its requested-storage budget includes the shared pair, work volume, and two additional 1Gi pooling volumes before applying 20% headroom.
On a cluster without Longhorn the fixtures fall back to the default StorageClass and no capacity check runs.

The shared fixtures are placed, not left to the scheduler.
Each shared CNPG cluster has one instance by default and uses preferred pod anti-affinity over `kubernetes.io/hostname` when `E2E_CNPG_INSTANCES` adds replicas.
The runner Jobs carry anti-affinity against the two primaries so a migration's SQL legs cross the network instead of looping back inside one node.
The target also repels the source's first instance, because CNPG's own anti-affinity only separates instances of the same cluster and the two primaries would otherwise share whichever node scores highest.
Every suite-created pod also declares CPU and memory requests.
A pod that requests nothing counts the same on every node, so the least-allocated node wins every scheduling decision, its allocation never rises, and an entire run piles onto one node.
All placement rules are preferred, so a smaller cluster can co-locate the pods and still pass.

Shared fixture servers get 2 CPUs and 4Gi and the seed Job the same, while migration runner Jobs keep the operator's default request.
A parallel run (more than one Ginkgo process) lowers these requests, so that one pair per process fits the cluster.
Each fixture server then requests 1 CPU and 1Gi, each seed Job 100m and 128Mi, and each runner 250m and 512Mi.
Requests only, so nothing is throttled.
A parallel run also halves the caches (512MB `shared_buffers`), so a fixture stays near its 1Gi request.
The caches are set by hand alongside the requests (`shared_buffers`, `effective_cache_size`, `maintenance_work_mem`, `wal_buffers`, `max_wal_size`, `checkpoint_timeout`), because CNPG does not derive `shared_buffers` from the memory request: raising the request on its own would leave PostgreSQL on its 128MB default and the clone would spend its time reading pages back off the volume, measuring the storage instead of the operator.

Two specs cover this.
One reads what was rendered onto the pods, an anti-affinity term and non-zero requests, which is namespaced and so runs anywhere; the other counts the nodes the instances actually occupy, which needs to read nodes and skips where that is not permitted.
The first is the one that binds: CloudNativePG defaults to a preferred hostname anti-affinity on its own, so the fixtures would still spread, and the node count alone would still pass, with the suite's own configuration deleted.

Chaos scenarios live in `test/e2e/chaos_test.go` behind the Ginkgo label `chaos`: they kill fixture pods (CNPG primaries, the runner mid-drain), overflow a follow migration's change spool on a deliberately tiny work volume, and fan two concurrent follow migrations out of one source.
`task e2e` and `task e2e:stress` exclude them (`-ginkgo.label-filter='!chaos'`); `task e2e:chaos` runs exactly them, with the same context echo and confirmation prompt.
It takes the `task e2e:focus` defaults (scale 0.1, kept fixtures, namespaces left alone) and the same `SCALE=`, `KEEP=`, and `MANAGE_NS=` overrides.
`task e2e:chaos:unattended` drops the prompt and requires `EXPECT_CONTEXT` to name the current context, as `task e2e:focus:unattended` does.
Set `E2E_CNPG_INSTANCES=3` so the primary-kill specs have a replica to fail over to:

```sh
E2E_CNPG_INSTANCES=3 task e2e:chaos:unattended EXPECT_CONTEXT=my-cluster
```

Each chaos spec creates its own Migration and restores what it disturbed, so the set runs standalone against kept fixtures.
The source-kill spec times its kill off `pg_stat_progress_copy` on the target and Skips below `E2E_SCALE` 0.05, where the documents COPY gets too short to hit reliably.

A spec that needs the database pods (exec into an instance, signal or delete one, or create its own CNPG clusters), or that acts beyond the two fixture databases (creates, drops, or connects to another database), MUST call `requireCNPGFixtures()` before it does.
In external mode that skips the spec with a reason.
We check at runtime rather than by label, so a run that forgets the label filter still cannot send such a spec at someone else's server.
A spec that only runs SQL needs no gate: `psql` and the helpers built on `psqlArgv` reach either kind of server.

`release.yml` runs this suite against a release candidate at `E2E_SCALE=0.1`, with the label filter `!chaos && !flaky`.
`E2E_OPERATOR_TAG` selects the candidate's published images, and `E2E_MANAGE_NAMESPACES=false` keeps the GitOps-owned namespaces intact.
`E2E_CONTEXT` comes from the `E2E_EXPECT_CONTEXT` secret of the `e2e-cluster` environment, and the run step fails when it is empty, because a skipped `TestE2E` would otherwise pass the gate without running a spec.
It calls the ginkgo CLI directly, not `task e2e`.
That target's confirmation prompt exists for a developer who could be pointed at any cluster, and answering it with `task --yes` is forbidden.
`E2E_PROMETHEUS_URL` comes from a repository variable, and a guard step fails the job when the variable is unset, so the metrics gate can never shrink to a silent Skip; `e2e.yml` guards the same way.
The published-release workflow `e2e.yml` defines its scale independently, defaulting to `0.25`.

### How does a parallel run work?

`release.yml` and `e2e.yml` run the suite with `--procs` set from their `E2E_PROCS` value, which is 6.
`go test` cannot run Ginkgo in parallel, so they use the ginkgo CLI at the version that `go.mod` pins.
Local tasks such as `task e2e` run one process.

Each Ginkgo process gets its own pair in `pgcopydb-e2e`.
Process 1 uses `e2e-source`, `e2e-target` and the seed Job `e2e-seed`, and process N adds `-N` to each name.
Process 1 alone checks the CRD, prepares the storage, installs the operator, purges old Migrations and applies the seed ConfigMap.
Then every process creates and seeds its own pair, at the same time as the others.
After the specs, process 1 waits for the other processes to stop.
Then it deletes every CNPG cluster and seed Job in `pgcopydb-e2e`, also those that an earlier run with more processes left behind.
The Longhorn capacity check counts one pair and one work volume for each process.
External mode and protected feature runs have one pair only, so they refuse more than one process before any setup or teardown runs.

> [!important]
> A spec runs on any free process, in any order, against the pair of that process.
> Only the specs in an `Ordered` container keep their order and their process.
> A new spec MUST NOT depend on what another spec left behind: it resets what it needs before it starts.

Two decorators control the schedule.
A spec that blocks a resource which all processes share MUST be `Serial`, so that Ginkgo runs it on process 1 after the parallel specs.
The operator's single reconcile worker is such a resource.
The progress-sampler bounds spec is not `Serial`: its locks stall only the background sampler, which runs outside the reconcile pass.
A container or spec that runs for two minutes or more SHOULD carry a `SpecPriority`, so that it starts early and not last.
Ginkgo starts the higher value first, so the tiers order the longest specs first: `SpecPriority(3)` for about 230 seconds and up, `SpecPriority(2)` for 180 to 229 seconds, and `SpecPriority(1)` for 120 to 179 seconds.
An Ordered container carries its priority on the container.
Ginkgo orders top-level containers by the highest priority of any spec inside them, and keeps file order within a container.
A long spec in a large container therefore goes in its own top-level container, because the container's highest priority moves every spec in it, short ones included.
Inside a container that is not Ordered, place the longer specs first, since file order decides the order among them.

### Cluster coverage

The progress-sampler bounds spec waits for follow to start, then gives replication lag five minutes to converge and waits up to another five minutes for `CutoverPending`.
A missing replication sample or lag above the allowance fails with the convergence helper's diagnostic instead of consuming the clone's 30-minute budget.
The EXTERNAL payload and recovery batch sizes exercise uncompressed storage traffic during recovery.
Recovery after unlocking has a separate 12-minute backlog drain budget shared by the wait and its target probes.

The early-cutover spec emits a snapshot roughly every 30 seconds from sender resume until cutover starts or the same 12-minute backlog drain budget expires.
See [Follow diagnostics](docs/design/follow-diagnostics.md) for the byte positions, missing-sample counts, and limits on stage attribution.
CI runs every test in `test/e2e` except the cluster entry point `TestE2E`, with `-race -v` (`go test ./test/e2e -skip '^TestE2E$'`).
A new test is picked up without touching the workflow, and `TestCIRunsEveryClusterFreeE2ETest` in `test/buildconfig/buildconfig_test.go` rejects a `-skip` regex that matches anything else.
Verbose output makes actual test selection and the expected parent-only subprocess-helper skip visible; it does not waive any meaningful regression.
The `TestLiveWriterHelper` subprocess entry point skips in the parent process; the lifecycle tests invoke it as a child.
`TestPSQLExecHelper` is the psql-channel tests' child entry point and returns at once in the parent process.

The live-load spec emits a writer report only on failure, after stopping the writer.
It records failure-time timestamps, submitted marker and byte counts, the child exit or signal, and final-query execution, parsing, and marker availability separately from the first returned error.
Submitted bytes and markers are not committed-row counts; a valid zero marker differs from an unavailable query result.
The writer still uses one persistent child and the primary captured at startup, followed by one fresh bounded final query after stdin closes and the child is reaped.
There is no active-writing deadline; the command timeout bounds shutdown and the final query.
`Cmd.WaitDelay` bounds inherited output descriptors to at most one additional second per command after exit or cancellation.
The report keeps an 8KiB stderr prefix while draining excess output, discards a truncated partial line, and applies the publication-retry redactors after joining chunks.
Known DNS and TCP/UDP error phrases suppress the whole line, including bare hostnames without a URI or IP address.
Final-query stderr comes from `exec.ExitError` when available, with the same prefix limit and redaction.
The report uses a source-role label rather than a pod name, emits no raw command error or query output, and makes no additional Kubernetes requests.
These diagnostics do not establish whether a broken pipe originated in SQL, the session, or the exec transport.

The suite installs the chart with `crds.install=false` and never creates, upgrades, or deletes the Migration CRD; the CI identity may only `get` it by name.
Before installing the operator, it compares the cluster's served schema with this checkout's generated CRD and fails naming every missing field, because admission would otherwise prune those fields silently.
On the shared cluster the CRD follows `main` through GitOps (see private ops notes), so a field added on `main` is testable at the next candidate.
The check polls for up to five minutes to cover a candidate tagged straight after a merge.

A behavior pull request MUST ship its E2E specs in the same change, so the candidate exercises them.
A contributor with a cluster SHOULD run the new specs locally with `task e2e:focus` before merging.

### How do you add a feature coverage case?

The feature coverage specs in `test/e2e/coverage_test.go` (clone group) and `test/e2e/coverage_follow_test.go` (follow group) run the SQL cases under `test/e2e/coverage/`, and the [coverage reference](docs/reference/coverage.md#postgresql-feature-coverage) lists every one of them.
A case that comes out identical needs one file and no Go change.

1. Write `test/e2e/coverage/<area>/<case>.sql`.
   The first line is the header, `-- @setup` starts the SQL that builds the case on the source, and `${schema}` names the case's own schema:

   ```sql
   -- coverage: group=clone  expect=identical  schemas=1  min_pg=14
   -- @setup
   CREATE TABLE ${schema}.t (id int PRIMARY KEY, v text);
   INSERT INTO ${schema}.t SELECT g, md5(g::text) FROM generate_series(1, 20) g;
   ```

2. Start a throwaway PostgreSQL, the image CI uses, and wait until it accepts connections:

   ```sh
   podman run -d --name coverage-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
     --health-cmd 'pg_isready -U postgres -h 127.0.0.1' --health-interval 1s docker.io/library/postgres:16-alpine
   podman wait --condition=healthy coverage-pg
   ```

3. Run the cluster-free coverage tests.
   They apply every case as a role without superuser rights, fail a case that creates or alters anything outside its own schemas, read its fingerprint, and fail a `@follow` that leaves the fingerprint unchanged:

   ```sh
   PGCOPYDB_TEST_PGURI=postgres://postgres:postgres@127.0.0.1:5432/postgres go test ./test/e2e -run Coverage -count=1
   ```

   A case whose `min_pg` is above the server's major skips; run those against a `postgres:18-alpine` server, as CI does.

4. Remove the throwaway server:

   ```sh
   podman rm -f coverage-pg
   ```

5. Add the case's row to the matrix in `docs/reference/coverage.md`.
   `TestCoverageMatrixListsEveryCase` fails until the row matches the header.

6. Run the group on a cluster, with the confirmation prompt described in [AGENTS.md](AGENTS.md):

   ```sh
   task e2e:focus FOCUS='Feature coverage'
   ```

The parser rejects a file that breaks these rules, so a typo cannot drop a case silently:

- The header sets `group`, `expect`, `schemas` (1 or 2) and `min_pg` (14 or later), and nothing else.
- `group=clone` and `group=follow` cases expect `identical`; a `group=own` case names an outcome registered in Go.
- `-- @follow` holds what a follow case runs while replication streams; a clone case has none.
- `${schema2}` exists only with `schemas=2`, and such a case must use it; any other `${...}` is an error.
- The file name is the case name: lowercase letters, digits and underscores, at most 37 bytes, unique across areas.

Three conventions keep a case within what the spec cleans up:

- Each statement commits on its own and runs as the source app role, so a case may use an enum value it added earlier, and pgcopydb owns what it copies.
- Grants and policies go to `cov_reader`, which the spec creates on both servers.
- A case that creates large objects lists them in `${schema}.cov_large_objects (name text, lo oid)`: the fingerprint compares their contents, and cleanup unlinks them, because dropping a schema leaves large objects behind.

## Releasing

Every Monday at 08:00 UTC, `auto-release.yml` reads what landed since the last stable tag and pushes a release candidate: `vX.Y.Z-rc.1`, a patch bump unless a `feat:` commit is in the range, in which case a minor one.
A week with nothing merged ends with no tag and a green run, which is not a failure.
When a candidate for the same version already exists the number counts up, rather than reusing a tag whose images are published.

That tag starts `release.yml`, which publishes the manager and runner images (multi-arch) and the Helm chart as OCI, creates the GitHub release whose notes GitHub generates from the merged PRs, and runs the e2e suite against exactly those artifacts on the cluster.

Every job below `builder-build` carries `!cancelled()` in its condition.
`builder-build` skips on the normal path, because the pinned builder tag is almost always published already, and GitHub propagates that skip to every descendant: a job in between that survives it with its own status function still passes the skip along to its own dependants.
Without the condition, a run can publish the images and then skip the chart, the release notes and e2e while reporting success.

When the suite fails, the candidate's artifacts stay where they are, `latest` still points at the last stable release, and the workflow opens an issue naming the run.
Fix forward on `main`, and the next candidate carries the fix.

A candidate that passes is not promoted on its own.
Promotion is [promote.yml](.github/workflows/promote.yml), dispatched by hand with the candidate tag.
That is what lets several candidates stand between two releases: a candidate that is never promoted just stays a candidate, and rc.2 can supersede rc.1 without rc.1 having already become the release.

Promoting pushes the stable tag `vX.Y.Z`, which starts `release.yml` once more on the same commit: the same images from the same context, and this time `latest` moves and the release is not marked a prerelease.
After `helm push`, the chart job uses the runner's existing ORAS tool and Helm login config to tag the published manifest as `latest`, preserving its digest.
Chart version tags omit the leading `v`; image tags and chart `appVersion` keep it.
`test/buildconfig` exercises the chart alias and image tag scripts for stable and prerelease tags, including chart retagging failures, without contacting a registry.

The gate used to be `needs: [e2e, release-notes]`, which GitHub enforced.
A manual promotion carries no such dependency, so `promote.yml` reads the candidate's own release run and refuses unless its `e2e` job concluded `success`.
Skipped, cancelled and never-ran are all refusals, not passes.

> [!important]
> A release candidate publishes under its own tag and never moves `latest`.
> Helm without an explicit version selects the highest stable SemVer chart tag, independently of the OCI `latest` alias.

The chart job waits on both image jobs, so a published chart never points at an image that failed to build.
A tag containing a hyphen is a SemVer prerelease and is marked as one on GitHub and Artifact Hub, so the candidate round stays out of the way of anyone browsing the releases page for a version to install.

Tagging by hand is the out-of-band case, for a fix that cannot wait until Monday:

```sh
git tag -a v0.4.1 -m "v0.4.1: short subject"
git push origin v0.4.1
```

> [!warning]
> A stable tag pushed by hand is published as it stands.
> It skips the candidate round, and with it the e2e gate.

Versions are plain SemVer.
The `-alpha.N` prereleases ran up to `v0.2.0-alpha.8` and stop there; `v0.3.0` is the first ordinary release.
Pre-1.0 means the API can change.

`v1alpha1` MUST NOT be dropped from the CRD while the CRD's `status.storedVersions` still lists it.
Before removing it, rewrite every stored object at `v1beta1` (a no-op update of each `Migration` is enough; [kube-storage-version-migrator](https://github.com/kubernetes-sigs/kube-storage-version-migrator) automates this), then patch `v1alpha1` out of `status.storedVersions`.
Only then can a release stop serving it.

Chart `version` and `appVersion` come from the tag, which is why the values committed in `Chart.yaml` are placeholders.
`hack/stamp-chart.sh` runs just before packaging and fills in the three Artifact Hub annotations that only make sense per release: the image tags, the prerelease flag, and a changelog built from the `feat:`, `fix:`, `perf:` and `refactor:` commit subjects since the previous tag.
It edits the checkout and commits nothing.

Renovate keeps the e2e install pinned to the current release.
A custom manager in `.renovaterc.json` watches `operatorTag` in `test/e2e/e2e_suite_test.go` and bumps it with the weekly dependency PR.

### Artifact Hub

The chart is listed at [artifacthub.io/packages/helm/pgcopydb-operator/pgcopydb-operator](https://artifacthub.io/packages/helm/pgcopydb-operator/pgcopydb-operator).
Artifact Hub reads the OCI repository directly, so a release needs no extra step to show up there.

Ownership is proved by `charts/pgcopydb-operator/artifacthub-repo.yml`, pushed to the chart's OCI repository under the fixed `artifacthub.io` tag.
That tag is not SemVer, so neither Helm nor Artifact Hub mistakes it for a chart version.
`.github/workflows/artifacthub-metadata.yml` pushes it on any change to the file and on manual dispatch.
`.helmignore` keeps it out of the packaged chart: it is a sibling artifact, not chart content.

The rest of the listing comes from `Chart.yaml` annotations, and two of them are hand-maintained:

- `artifacthub.io/crdsExamples` duplicates `docs/examples/01-clone-minimal.yaml` and `docs/examples/03-clone-platform-secret.yaml` with the comments stripped.
  Nothing enforces the copies, so update them when those examples change.
  Artifact Hub matches an example to a CRD by kind and renders only the first match, so keep the minimal clone first; further entries serve readers of the annotation itself.
- `artifacthub.io/images` lists the runner image explicitly.
  The runner reaches the cluster as a `--runner-image` flag rather than as a container in a manifest, so Artifact Hub cannot discover it, and without the annotation it is never scanned for vulnerabilities.

## Commits and pull requests

- Conventional commits: `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `ci:`.
- One logical change per commit.
  Every commit MUST be lint-clean on its own.
- `main` is protected: changes land via GitHub PRs, and `lint`, `test` and `docs` MUST be green.
  Nobody pushes to `main` directly.
