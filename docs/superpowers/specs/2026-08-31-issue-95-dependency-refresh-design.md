# Issue 95 dependency refresh design

## Context

[Issue 95](https://github.com/ydixken/pgcopydb-operator/issues/95) is Renovate's Dependency Dashboard, not a defect report or a complete list of versions at implementation time.
Its 2026-08-12 snapshot is stale because `main` already contains some updates that the issue still reports as pending.
We will use the repository at `fix/issue-95-maintenance` as the baseline and verify each target against its public upstream source before changing it.
We will leave the dashboard under Renovate's control.

The user authorized the complete public release path after reviewing the proposed two-PR design.
That authorization covers pull requests, merges, a manual auto-release dispatch, published release candidates and stable artifacts, and the repository's protected real-cluster e2e jobs.
It does not authorize a production deployment, destructive cluster work, secret access, or rewriting published tags.

## Goals

The change has four goals.

1. Bring every GitHub Action reference in the repository to its approved major version.
2. Move the Go toolchain to 1.27.0 and refresh the existing Go dependency graph within the controller-runtime compatibility boundary.
3. Refresh public container base-image digests and the local e2e default release.
4. Exercise all nine workflows through the branch, merge, builder, metadata, release, and e2e paths.

## First pull request

The first pull request uses branch `fix/issue-95-maintenance`.
It updates all 51 `uses:` references across the nine files in `.github/workflows/`.
Repeated references receive the same target major in every workflow.

The action mapping is:

- `actions/checkout`: v7.
- `actions/setup-go`: v7.
- `actions/setup-python`: v7.
- `actions/cache`: v6.
- `actions/configure-pages`: v6.
- `actions/upload-pages-artifact`: v5.
- `actions/deploy-pages`: v5.
- `azure/setup-helm`: v5.
- `azure/setup-kubectl`: v5.
- `codecov/codecov-action`: v7.
- `docker/setup-qemu-action`: v4.
- `docker/setup-buildx-action`: v4.
- `docker/login-action`: v4.
- `docker/build-push-action`: v7.
- `oras-project/setup-oras`: v2.

The Docker action v4 releases use Node 24 and require Actions Runner v2.327.1 or newer, as recorded in the [setup-buildx v4.0.0 release](https://github.com/docker/setup-buildx-action/releases/tag/v4.0.0).
The repository already uses Node 24 actions with that same floor, including the version documented by [checkout v5.0.0](https://github.com/actions/checkout/releases/tag/v5.0.0).
The branch smoke run will prove that the protected self-hosted build runner satisfies the floor without exposing its configuration.

The Go and tool targets are:

- Set the `go` directive and manager builder image to Go 1.27.0.
- Update the builder image to the current digest for the exact `golang:1.27.0` tag.
- Update both golangci-lint pins to v2.13.2 so the custom binary and the Makefile installer stay in sync.
- Keep Ginkgo at v2.32.1.
- Update Gomega from v1.42.1 to v1.43.0.
- Keep `prometheus/client_golang` at v1.24.1.
- Update `prometheus/client_model` from v0.6.2 to v0.6.3.
- Update `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go`, and `k8s.io/streaming` from v0.36.3 to v0.36.4.
- Keep `sigs.k8s.io/controller-runtime` at v0.24.1 and `sigs.k8s.io/yaml` at v1.6.0.

Go 1.27.0 must be an official release listed in the [Go release history](https://go.dev/doc/devel/release) before the image and module directive move.
Go tooling may refresh indirect modules that the exact direct targets require.
We will accept those indirect changes only when `go mod tidy` produces them, the module graph remains within the limits below, and lint and tests pass.
We will not add dependencies or run an unconstrained upgrade across major versions.

The container refresh covers public base images used by the manager, runner, and pgcopydb builder Dockerfiles.
Each public image keeps its intended tag and receives the current multi-platform manifest digest for that tag.
The Go image tag is the one intentional exception because it moves to 1.27.0.
The refresh includes the Go builder, Distroless static nonroot, and Debian trixie-slim images.
The internal `pgcopydb-builder` digest in `images/runner/Dockerfile` stays unchanged in this pull request for the publication reason described below.

The local e2e suite changes its default operator tag from v0.5.0 to the current stable release, v0.11.3.
The environment override remains the supported way to test another published version.
The related test and comment must change with the default so the file remains true.

## Compatibility limits

[controller-runtime's compatibility matrix](https://github.com/kubernetes-sigs/controller-runtime#compatibility) maps controller-runtime v0.24 to Kubernetes v0.36.
All four direct Kubernetes modules therefore move together to v0.36.4 and stop there.
Kubernetes v0.37 and controller-runtime v0.25 are outside this change.
Indirect Kubernetes modules must remain on the v0.36 line unless the existing controller-runtime graph selects an older compatible v0.36 patch.

The Go baseline is exactly 1.27.0.
The change does not adopt a later Go 1.27 patch, Go 1.28, or a development toolchain without another review.

Each Action moves only to the major listed above.
Inputs, outputs, permissions, event filters, concurrency controls, and environment guards remain unchanged unless the new major removed a field and its public release notes prescribe a direct replacement.
Any required semantic workflow rewrite stops the update for review instead of guessing.
The self-hosted runners must be Actions Runner v2.327.1 or newer because the new action majors use Node 24.

## Verification before the first pull request

Run local checks from the isolated worktree in this order.

1. Confirm the intended branch and review the changed-file boundary.

   ```sh
   git status --short --branch
   ```

2. Confirm that the workflow inventory still contains exactly 51 action references in nine files.

   ```sh
   test "$(rg '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 51
   ```

3. Check the patch for whitespace errors.

   ```sh
   git diff --check
   ```

4. Run the repository lint gate.

   ```sh
   task lint
   ```

5. Run the unit and envtest gate.

   ```sh
   task test
   ```

Review `go.mod`, `go.sum`, both golangci-lint pins, every workflow reference, Dockerfile tags and digests, and the e2e default after the commands pass.
No missing match may count as success in the inventory checks.

Local `task e2e` is not part of this unattended branch gate because it targets the developer's current Kubernetes context and requires a human to answer its confirmation prompt.
No agent may bypass that prompt with `task --yes`.
The authorized real-cluster gates run later through the protected release workflows.

## GitHub workflow verification

The first pull request must pass the `lint`, `test`, and `docs` jobs in `ci.yml` before merge.
Dispatch `runner-smoke.yml` on `fix/issue-95-maintenance` and require a successful multi-platform build so the Docker action upgrades and runner compatibility are tested before release.
Review the run URLs, job conclusions, and relevant step logs without copying secret values or infrastructure details into the repository.

After the first merge, require successful runs for:

- `ci.yml` on `main`.
- `docs.yml`, which the new design document and workflow edit trigger.
- `mirror-to-gitlab.yml` for the merged branch state.
- `pgcopydb-builder.yml`, which publishes the refreshed builder manifest after its Dockerfile changes land on `main`.

Dispatch `artifacthub-metadata.yml` on `main` after its ORAS v2 update is merged.
That run verifies the ORAS action path without waiting for repository metadata to change.
The dispatch must use the existing permissions and public package target.

## Why the builder digest needs a second pull request

`images/runner/Dockerfile` copies pgcopydb from an internal multi-platform builder image.
It pins both the pgcopydb commit tag and the manifest digest.
The first pull request changes the public base image used to build that internal image.
GitHub can publish the resulting manifest only after the change reaches `main`, so its final digest does not exist while the first pull request is open.

After `pgcopydb-builder.yml` succeeds on `main`, read the published multi-platform manifest digest from the public package registry and verify both expected platforms.
Create a second branch from that exact `main` commit and change only the internal builder digest plus any narrowly required test expectation.
Do not replace the digest with a floating tag.

The second pull request must pass local lint and tests, the three required CI jobs, and a branch runner smoke run.
Merge it only after those checks pass.
Then confirm `ci.yml` and `mirror-to-gitlab.yml` on the resulting `main` commit.

## Release and e2e chain

The last stable tag is v0.11.3, and the range after it contains a `feat:` commit.
The approved auto-release dispatch on `main` is therefore expected to create v0.12.0-rc.1.
If that tag already exists or the computed version differs, stop before publishing and reconcile the tag history with `auto-release.yml`.

The complete chain is:

1. Dispatch `auto-release.yml` on the final `main` commit and require a successful candidate job.
2. Confirm that v0.12.0-rc.1 starts `release.yml` and publishes candidate manager images, runner images, the Helm chart, and a GitHub prerelease.
3. Require the candidate `release.yml` e2e job to pass against the published candidate at `E2E_SCALE=0.1` with the repository's existing label filter.
4. Confirm that the promote job creates v0.12.0 only after candidate e2e succeeds.
5. Confirm the stable v0.12.0 `release.yml` run publishes stable artifacts and moves the intended `latest` tags.
6. Require the stable-tag `e2e.yml` run to pass against the published v0.12.0 images.
7. Confirm the mirror runs for both tags and verify that the candidate and stable e2e cleanup steps ran.

The three public runs from the prior complete chain provide comparison points for [auto-release](https://github.com/ydixken/pgcopydb-operator/actions/runs/33361872716), [candidate release and e2e](https://github.com/ydixken/pgcopydb-operator/actions/runs/33361881756), and [stable release and e2e](https://github.com/ydixken/pgcopydb-operator/actions/runs/33365428821).
Use them to compare job shape and expected conditional skips, not as evidence that the new versions pass.

## Failure and stop rules

- Do not push a branch whose local lint or test gate fails.
- Do not merge a pull request with a failed, cancelled, or missing required check.
- If an action major requires a workflow redesign, stop and request review before expanding scope.
- If the builder publication fails, leave the old digest pinned and fix forward before opening the digest pull request.
- If candidate build, chart, release, or e2e fails, do not promote the candidate.
- Preserve the failed candidate tag and prerelease, fix forward on `main`, and let auto-release choose the next candidate number.
- If stable publication or stable e2e fails, stop further release work, report the public run, and fix forward without deleting or moving v0.12.0.
- Do not force-push `main`, reuse a published tag, delete a release, bypass a workflow gate, or treat an absent result as success.

## Security constraints

This public repository must contain no private endpoints, host names, addresses, cluster inventory, runner configuration, or GitOps details.
The work may name existing secrets and variables but must not read, print, set, or copy their values.
Workflow permissions must stay least privilege.
The e2e environment policy, repository checks, tag filters, branch filters, and concurrency controls must not be weakened.
Public base images remain pinned by digest, and the internal builder remains pinned by tag and digest.
Release and e2e logs must be reviewed in place, with only public run URLs and non-sensitive conclusions recorded.

## Acceptance criteria

- The first pull request updates all 51 action references in all nine workflows to the exact mapping in this document.
- Go 1.27.0, golangci-lint v2.13.2, Gomega v1.43.0, `prometheus/client_model` v0.6.3, and the four Kubernetes v0.36.4 modules are present at their exact targets.
- controller-runtime remains v0.24.1, no Kubernetes v0.37 module enters the graph, and no new direct dependency is added.
- Any new indirect module is required by the resolved graph and passes compatibility, license, security, lint, and test review.
- Public Dockerfile base images use verified current digests, and the local e2e default is v0.11.3 with a matching test.
- Local lint, unit, envtest, inventory, and diff checks pass before each pull request.
- The required pull request checks and branch runner smoke run pass before each merge.
- All nine workflows are exercised through their relevant branch, `main`, metadata, builder, release, mirror, and stable-tag triggers.
- The second pull request pins the exact builder manifest published from the first merged change and leaves no floating internal image reference.
- v0.12.0-rc.1 passes candidate e2e before promotion, v0.12.0 publishes successfully, and the stable-tag e2e run passes.
- The final review records public run URLs and conclusions without secrets or private infrastructure facts.

## Out of scope

- Kubernetes v0.37, controller-runtime v0.25, and Go versions other than 1.27.0.
- New dependencies, frameworks, architectural patterns, API changes, controller behavior changes, or schema migrations.
- Changes to Renovate configuration or manual edits to the Dependency Dashboard.
- Production deployment or any use of a production Kubernetes context.
- Force-pushes, release deletion, tag replacement, and rollback by mutating published artifacts.
- Disclosure or modification of private infrastructure, credentials, protected environments, or external GitOps configuration.
