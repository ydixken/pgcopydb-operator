# Issue 95 dependency refresh design

## Context

[Issue 95](https://github.com/ydixken/pgcopydb-operator/issues/95) is Renovate's Dependency Dashboard, not a defect report or a complete list of versions at implementation time.
Its 2026-08-12 snapshot is stale because `main` already contains some updates that the issue still reports as pending.
We will use `fix/issue-95-maintenance` as the baseline and verify each target against its public upstream source before changing it.
We will leave the dashboard under Renovate's control.

The user approved the two-pull-request design, the release promotion fix, and the permanent dependency review gate.
The user also authorized enabling GitHub's Dependency Graph through the repository Settings UI.
The authorization covers pull requests, merges, a manual auto-release dispatch, published release candidates and stable artifacts, and the repository's protected real-cluster e2e jobs.
It does not authorize a production deployment, destructive cluster work, secret access, changes that weaken the e2e environment policy, or rewriting published tags.

## Goals

The work has five goals.

1. Upgrade all 51 existing GitHub Action references to their approved majors and add one dependency review reference.
2. Move the Go toolchain to 1.27.0 and refresh the existing Go dependency graph within the controller-runtime compatibility boundary.
3. Fix the release promotion dependency so a stable tag cannot precede the candidate GitHub release.
4. Refresh public container base-image digests and the local e2e default release.
5. Exercise all nine workflows through their branch, merge, builder, metadata, release, and e2e paths.

## First pull request

The first pull request uses branch `fix/issue-95-maintenance`.
It upgrades all 51 existing `uses:` references across the nine files in `.github/workflows/` and adds `actions/dependency-review-action@v4` once.
The completed first pull request therefore contains 52 action references across the same nine workflow files.
Repeated existing references receive the same target major in every workflow.

The existing action mapping is:

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
The repository already uses Node 24 actions with that floor, including the version documented by [checkout v5.0.0](https://github.com/actions/checkout/releases/tag/v5.0.0).
The branch smoke run will prove that the protected build runner satisfies the floor without exposing its configuration.

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
We will accept those indirect changes only when `go mod tidy` produces them, the module graph remains within the limits below, and all dependency, lint, and test gates pass.
We will not add a direct Go dependency or run an unconstrained upgrade across major versions.

The container refresh covers public base images used by the manager, runner, and pgcopydb builder Dockerfiles.
Each public image keeps its intended tag and receives the current multi-platform manifest digest for that tag.
The Go image tag is the one intentional exception because it moves to 1.27.0.
The refresh includes the Go builder, Distroless static nonroot, and Debian trixie-slim images.
The internal `pgcopydb-builder` digest in `images/runner/Dockerfile` stays unchanged in this pull request for the publication reason described below.

The local e2e suite changes its default operator tag from v0.5.0 to the current stable release, v0.11.3.
The environment override remains the supported way to test another published version.
The related test and comment must change with the default so the file remains true.

## Dependency Graph and dependency gates

Before opening the first pull request, enable Dependency Graph through the repository Settings UI.
Do not use an undocumented or unsupported REST mutation to change this setting.
Confirm that GitHub generated the graph with this read-only command:

```sh
gh api repos/ydixken/pgcopydb-operator/dependency-graph/sbom --jq '.sbom.creationInfo.created'
```

The command must exit 0 and print a non-empty creation timestamp.
An absent timestamp, 404 response, or permission error is a failed prerequisite.

Add this pull-request-only step to the existing required `lint` job in `.github/workflows/ci.yml`, immediately after its checkout:

```yaml
- name: Review dependency changes
  if: github.event_name == 'pull_request'
  uses: actions/dependency-review-action@v4
  with:
    fail-on-severity: moderate
    fail-on-scopes: runtime, development, unknown
    allow-licenses: Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, MIT
```

The step reuses the lint job's checkout and the workflow's existing `contents: read` permission.
It must not run on the `main` push event because dependency review requires a pull request comparison.
Do not add retries or broader permissions.
The action must fail the required lint job for a new dependency at moderate or higher severity, a disallowed resolved license, or a violation in any configured scope.

Dependency Review v4 warns when GitHub cannot resolve a dependency's license, but it cannot fail on that condition.
Review every unresolved license manually and treat an unknown or unacceptable license as a merge blocker.
Record the public source used to resolve it without copying dependency source text into the repository.

Run the pinned one-time Go vulnerability check outside `go.mod`:

```sh
go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
```

Use the default non-JSON output.
The command must exit 0 and report no reachable known vulnerability.
Do not add `golang.org/x/vuln` to `go.mod` or install an unpinned latest tool.

## Release promotion fix

The candidate `e2e` and `release-notes` jobs both depend on `chart`, so they run in parallel after candidate artifacts are available.
The current `promote` job depends only on `e2e`.
That allows it to push the stable tag while `release-notes` is still running, or even when candidate release creation fails.

Change the promotion dependency in `.github/workflows/release.yml` to:

```yaml
needs: [e2e, release-notes]
```

Keep `e2e` and `release-notes` parallel, with each depending on `chart` rather than on the other.
Keep `promote` free of a job-level `if` expression.
GitHub's default dependency handling will then run promotion only when both required jobs succeed and will skip it on stable-tag runs where candidate e2e is skipped.
An `always()`, `success()`, or result-based job condition would override that default and is not allowed.

Extend the existing parsed workflow model in `test/buildconfig/buildconfig_test.go` with `Needs` and `If` fields.
Add one focused regression test that fails if `promote` is absent, if its prerequisites are not exactly `e2e` and `release-notes`, or if its parsed job-level `If` field is non-empty.
The test must parse `.github/workflows/release.yml` through the existing `sigs.k8s.io/yaml` path rather than compare source substrings.

## Compatibility limits

[controller-runtime's compatibility matrix](https://github.com/kubernetes-sigs/controller-runtime#compatibility) maps controller-runtime v0.24 to Kubernetes v0.36.
All four direct Kubernetes modules therefore move together to v0.36.4 and stop there.
Kubernetes v0.37 and controller-runtime v0.25 are outside this change.
Indirect Kubernetes modules must remain on the v0.36 line unless the existing controller-runtime graph selects an older compatible v0.36 patch.

The Go baseline is exactly 1.27.0.
The change does not adopt a later Go 1.27 patch, Go 1.28, or a development toolchain without another review.

Each existing Action moves only to the major listed above.
Inputs, outputs, permissions, event filters, concurrency controls, and environment guards remain unchanged except for the approved Dependency Review step and promotion dependency.
If a new major removed a field, stop that Action's upgrade and review its public release notes before changing workflow behavior.
The self-hosted runners must be Actions Runner v2.327.1 or newer because the new action majors use Node 24.

## Verification before the first pull request

Run local checks from the isolated worktree in this order.

1. Confirm the intended branch and review the changed-file boundary.

   ```sh
   git status --short --branch
   ```

2. Confirm the completed workflow inventory contains 52 action references in nine files and exactly one new dependency review reference.

   ```sh
   test "$(rg '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 52
   test "$(rg -l '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 9
   test "$(rg -c 'uses: actions/dependency-review-action@v4' .github/workflows/ci.yml)" = 1
   ```

3. Confirm Dependency Graph is available.

   ```sh
   test -n "$(gh api repos/ydixken/pgcopydb-operator/dependency-graph/sbom --jq '.sbom.creationInfo.created')"
   ```

4. Check the patch for whitespace errors.

   ```sh
   git diff --check
   ```

5. Run the pinned one-time vulnerability scan in default non-JSON mode.

   ```sh
   go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
   ```

6. Review the licenses for all changed direct and indirect modules, including every dependency whose license GitHub cannot resolve.

7. Run the repository lint gate.

   ```sh
   task lint
   ```

8. Run the unit and envtest gate.

   ```sh
   task test
   ```

Review `go.mod`, `go.sum`, both golangci-lint pins, every workflow reference, Dockerfile tags and digests, release promotion dependencies, and the e2e default after the commands pass.
No missing match, empty API result, warning about an unresolved license, or skipped test may count as success.

Local `task e2e` is not part of the unattended branch gate because it targets the developer's current Kubernetes context and requires a human to answer its confirmation prompt.
No agent may bypass that prompt with `task --yes`.
The authorized real-cluster checks run through the protected workflows.

## GitHub workflow verification

The first pull request must pass the `lint`, `test`, and `docs` jobs in `ci.yml` before merge.
The conditional Dependency Review step must run in `lint`, and its result and license warnings require manual review.
Do not merge when the step is absent, skipped on a pull request, or inconclusive.

Dispatch `runner-smoke.yml` on `fix/issue-95-maintenance` and require a successful multi-platform build.
This run tests the Docker action upgrades and build runner compatibility before release.

Dispatch `e2e.yml` on `fix/issue-95-maintenance` with tag v0.11.3 and scale 0.1.
The existing `e2e-cluster` environment policy must reject the feature branch before assigning a protected runner.
Treat runner assignment or execution from the feature branch as a security failure, and do not weaken the environment policy to make the run pass.

After the first merge, require successful runs for:

- `ci.yml` on `main`.
- `docs.yml`, which the design and workflow edits trigger.
- `mirror-to-gitlab.yml` for the merged branch state.
- `pgcopydb-builder.yml`, which publishes the refreshed builder manifest after its Dockerfile changes reach `main`.

Dispatch the merged `e2e.yml` from `main` with tag v0.11.3 and scale 0.1.
Require the real-cluster suite and cleanup step to pass before continuing to the digest pull request.
Review run URLs, conclusions, and relevant logs in place without copying secret values or infrastructure details into the repository.

## Why the builder digest needs a second pull request

`images/runner/Dockerfile` copies pgcopydb from an internal multi-platform builder image.
It pins both the pgcopydb commit tag and the manifest digest.
The first pull request changes the public base image used to build that internal image.
GitHub can publish the resulting manifest only after the change reaches `main`, so its final digest does not exist while the first pull request is open.

After `pgcopydb-builder.yml` succeeds on `main`, read the public manifest from the package registry.
Verify that its digest is the registry's multi-platform index and that the index contains linux/amd64 and linux/arm64 manifests.
Create a second branch from that exact `main` commit and change only the internal builder digest plus a narrowly required existing test expectation if one stores the digest.
Do not replace the digest with a floating tag or include unrelated edits.

The second pull request must pass local lint and tests, the three required CI jobs, Dependency Review, and a branch runner smoke run.
Merge it only after those checks pass.
Then confirm `ci.yml` and `mirror-to-gitlab.yml` on the resulting `main` commit.
Re-read the published manifest and confirm that `images/runner/Dockerfile` pins its exact index digest.

## Metadata, release, and e2e chain

Dispatch `artifacthub-metadata.yml` on the final `main` commit after the second pull request merges.
Require a successful ORAS setup and metadata push using the workflow's existing permissions and public package target.

The last stable tag is v0.11.3, and the range after it contains a `feat:` commit.
The approved auto-release dispatch on `main` is therefore expected to create v0.12.0-rc.1.
If that tag already exists or the computed version differs, stop before publishing and reconcile the public tag history with `auto-release.yml`.

The complete release chain is:

1. Dispatch `auto-release.yml` on the final `main` commit and require a successful candidate job.
2. Confirm that v0.12.0-rc.1 starts `release.yml` and publishes candidate manager images, runner images, the Helm chart, and a GitHub prerelease.
3. Confirm that candidate `e2e` and `release-notes` run in parallel after `chart`.
4. Require the candidate `e2e` job to pass against the published candidate at `E2E_SCALE=0.1` with the existing label filter, and confirm its cleanup step ran.
5. Confirm that `promote` starts only after both candidate e2e and candidate release creation succeed, then creates v0.12.0.
6. Confirm the stable v0.12.0 `release.yml` run publishes stable artifacts and moves the intended `latest` tags.
7. Require the stable-tag `e2e.yml` run to pass against the published v0.12.0 images, and confirm its cleanup step ran.
8. Confirm mirror runs for both tags.
9. Inspect the candidate, stable, and `latest` image indexes, require linux/amd64 and linux/arm64 manifests, and confirm the expected tags resolve to the published indexes.

The three public runs from the prior complete chain provide comparison points for [auto-release](https://github.com/ydixken/pgcopydb-operator/actions/runs/33361872716), [candidate release and e2e](https://github.com/ydixken/pgcopydb-operator/actions/runs/33361881756), and [stable release and e2e](https://github.com/ydixken/pgcopydb-operator/actions/runs/33365428821).
Use them to compare job shape and expected conditional skips, not as evidence that the new versions pass.

## Failure and stop rules

- Do not open the first pull request until Dependency Graph is enabled and the SBOM command returns a non-empty timestamp.
- Do not push a branch whose local lint, test, vulnerability, license, inventory, or diff gate fails.
- Do not merge a pull request with a failed, cancelled, skipped, inconclusive, or missing required check.
- Do not merge when Dependency Review reports a vulnerability at moderate or higher severity, a disallowed license, or an unresolved license that manual review has not cleared.
- Do not add retries to conceal a Dependency Review failure.
- If an action major requires a workflow redesign, stop and request review before expanding scope.
- If the feature-branch e2e dispatch reaches a protected runner, stop and correct the repository environment policy before continuing.
- If the merged `main` e2e dispatch fails, do not open or merge the builder digest pull request.
- If the builder publication fails or its manifest lacks an expected platform, leave the old digest pinned and fix forward before opening the digest pull request.
- If candidate build, chart, GitHub release, or e2e fails, do not promote the candidate.
- Preserve a failed candidate tag and prerelease, fix forward on `main`, and let auto-release choose the next candidate number.
- If promotion starts before either candidate prerequisite succeeds, stop the run and fix the release gate before a stable tag is pushed.
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

- The first pull request upgrades all 51 existing action references to the exact mapping in this document and adds one `actions/dependency-review-action@v4` reference, for 52 references across nine workflows.
- Dependency Graph is enabled through the repository Settings UI, and the read-only SBOM command returns a non-empty creation timestamp.
- The required PR lint job runs Dependency Review once with the approved severity, scopes, and license allowlist, no retry, and no broader permission.
- Manual review clears every unresolved dependency license before merge.
- Pinned `govulncheck@v1.7.0` exits 0 in default non-JSON mode and reports no reachable known vulnerability without changing `go.mod`.
- Go 1.27.0, golangci-lint v2.13.2, Gomega v1.43.0, `prometheus/client_model` v0.6.3, and the four Kubernetes v0.36.4 modules are present at their exact targets.
- controller-runtime remains v0.24.1, no Kubernetes v0.37 module enters the graph, and no new direct Go dependency is added.
- Every changed indirect module is required by the resolved graph and passes compatibility, license, security, lint, and test review.
- Public Dockerfile base images use verified current digests, and the local e2e default is v0.11.3 with a matching test.
- `promote` has exactly `e2e` and `release-notes` as prerequisites, has no job-level `if`, and has parsed regression coverage in `test/buildconfig/buildconfig_test.go`.
- Candidate e2e and release-notes remain parallel after `chart`.
- Local lint, unit, envtest, inventory, vulnerability, license, and diff checks pass before each pull request.
- The required pull request checks and branch runner smoke run pass before each merge.
- The feature-branch e2e dispatch is rejected before runner assignment, and the merged `main` dispatch against v0.11.3 at scale 0.1 passes with cleanup.
- All nine workflows are exercised through their relevant pull request, branch, `main`, metadata, builder, release, mirror, and stable-tag triggers.
- The second pull request changes only the builder digest and any required existing expectation, pins the exact multi-platform manifest published from the first merge, and leaves no floating internal image reference.
- v0.12.0-rc.1 publishes the candidate GitHub release and passes candidate e2e before promotion.
- v0.12.0 publishes successfully, stable e2e passes, both cleanup steps run, and the candidate, stable, and `latest` image manifests contain both expected platforms.
- The final review records public run URLs and conclusions without secrets or private infrastructure facts.

## Out of scope

- Kubernetes v0.37, controller-runtime v0.25, and Go versions other than 1.27.0.
- New direct Go dependencies, frameworks, architectural patterns, API changes, controller behavior changes, or schema migrations.
- Changes to Renovate configuration or manual edits to the Dependency Dashboard.
- Automatic retries for the Dependency Review step.
- Production deployment or any use of a production Kubernetes context.
- Weakening the protected e2e environment so feature branches can reach its runner.
- Force-pushes, release deletion, tag replacement, and rollback by mutating published artifacts.
- Disclosure or modification of private infrastructure, credentials, protected environments, or external GitOps configuration.
