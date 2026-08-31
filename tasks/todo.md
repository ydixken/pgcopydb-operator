# Issue 95 maintenance plan

## Design gate

- [x] Create an isolated worktree at `fix/issue-95-maintenance` and record the clean baseline.
- [x] Read issue 95, repository documentation, affected code, and all nine workflows.
- [x] Present the two-pull-request design with trade-offs and get user approval.
- [x] Get user approval for the release promotion fix and permanent dependency gate.
- [x] Revise `docs/superpowers/specs/2026-08-31-issue-95-dependency-refresh-design.md` with the approved gates and stop rules.
- [x] Self-review the revised spec for placeholders, contradictions, exact mappings, semantic line breaks, prohibited dashes, private information, and runnable commands.
- [x] Complete an independent review of the revised written spec before implementation planning.

## First pull request

- [x] Write the implementation plan from the approved and reviewed design.
- [x] Complete an independent review of the implementation plan before implementation.
- [x] Get user approval for the implementation plan before implementation.
- [x] Enable GitHub Dependency Graph through the repository Settings UI.
- [x] Confirm Dependency Graph with `gh api repos/ydixken/pgcopydb-operator/dependency-graph/sbom --jq '.sbom.creationInfo.created'`.
- [x] Add the conditional pull-request-only Dependency Review step to the required CI lint job with the approved severity, scopes, and license allowlist.
- [x] Add parsed regression coverage for the promotion job's exact `needs` list and empty job-level `if`.
- [x] Make `promote` depend on `e2e` and `release-notes` while keeping those candidate jobs parallel.
- [x] Upgrade all 51 existing Action references and add the one approved Dependency Review reference.
- [x] Upgrade Go, golangci-lint, and the approved direct and indirect Go modules within the compatibility limits.
- [x] Refresh the public base-image digests and keep the unpublished internal builder digest unchanged.
- [x] Update the local e2e default tag and its test to v0.11.3.
- [x] Review every changed dependency license and block unresolved or unacceptable licenses.
- [x] Run pinned `govulncheck@v1.7.0` in default non-JSON mode and require exit 0 with no reachable known vulnerability.
- [x] Run the action inventory, `git diff --check`, `task lint`, and `task test` gates.
- [x] Delegate independent code and workflow review, then resolve every finding.
- [ ] Push `fix/issue-95-maintenance`, open the first pull request, and record its public URL.
- [ ] Confirm the PR `lint`, `test`, and `docs` jobs, including a non-skipped Dependency Review step.
- [ ] Dispatch `runner-smoke.yml` on the branch and require the multi-platform build to pass.
- [ ] Dispatch `e2e.yml` on the branch with tag v0.11.3 and scale 0.1, then confirm the environment rejects it before runner assignment.
- [ ] Merge the first pull request only after all first-pull-request gates pass.
- [ ] Confirm post-merge `ci.yml`, `docs.yml`, and `mirror-to-gitlab.yml` on `main`.
- [ ] Confirm `pgcopydb-builder.yml` publishes a linux/amd64 and linux/arm64 manifest from the first merge.
- [ ] Dispatch the merged `e2e.yml` from `main` with tag v0.11.3 and scale 0.1, then require suite and cleanup success.

## Builder digest pull request

- [ ] Read the published builder index digest and verify its linux/amd64 and linux/arm64 manifests.
- [ ] Create a second branch from the exact first-merge `main` commit.
- [ ] Change only the internal builder digest and any narrowly required existing test expectation.
- [ ] Run `git diff --check`, `task lint`, and `task test` on the digest-only change.
- [ ] Push the second branch, open the digest pull request, and record its public URL.
- [ ] Confirm the second PR `lint`, `test`, and `docs` jobs, including Dependency Review.
- [ ] Dispatch `runner-smoke.yml` on the second branch and require success.
- [ ] Merge the digest pull request only after all second-pull-request gates pass.
- [ ] Confirm post-merge `ci.yml` and `mirror-to-gitlab.yml` on the final `main` commit.
- [ ] Re-read the builder manifest and confirm `images/runner/Dockerfile` pins its exact index digest.

## Metadata and full release chain

- [ ] Dispatch `artifacthub-metadata.yml` on final `main` and require the ORAS metadata push to pass.
- [ ] Reconcile the next expected version with public tag history, and stop if it is not v0.12.0-rc.1.
- [ ] Dispatch `auto-release.yml` on final `main` and require the candidate job to publish v0.12.0-rc.1.
- [ ] Confirm candidate manager images, runner images, Helm chart, and GitHub prerelease publication.
- [ ] Confirm candidate `e2e` and `release-notes` start in parallel after `chart`.
- [ ] Require candidate e2e at scale 0.1 and its cleanup step to pass.
- [ ] Confirm `promote` starts only after candidate e2e and release creation pass, then publishes v0.12.0.
- [ ] Confirm the stable release publishes the manager images, runner images, Helm chart, GitHub release, and intended `latest` tags.
- [ ] Require stable-tag `e2e.yml` and its cleanup step to pass.
- [ ] Confirm mirror runs for the candidate and stable tags.
- [ ] Verify candidate, stable, and `latest` image indexes and both expected platforms.
- [ ] Confirm all nine workflows have passed or produced their designed security-gate result on the intended trigger.
- [ ] Delegate final review of the diff, command evidence, public workflow runs, release artifacts, and e2e results.
- [ ] Record final command, CI, dependency, manifest, release, and e2e evidence in the review below.

## Review

- Worktree setup is complete at commit `fcbc56dc813f9e026650bffd984e86c7d604241d`.
- `go mod download` exited 0 with Go 1.27.0 on Darwin arm64.
- Baseline `task lint` exited 0 after YAML, chart, documentation link, Prometheus rule, and Go lint checks.
- Baseline `task test` exited 0 after all 13 non-e2e Go packages passed.
- Revised-design `task lint` exited 0 after the same repository lint gates.
- Revised-design `task test` exited 0 after all 13 non-e2e Go packages passed.
- Independent review approved the implementation plan at `b47dd9d` with no blockers.
- The Dependency Graph SBOM proof exited 0 with a non-empty creation timestamp after the setting was enabled.
- The release promotion regression test failed against the old dependency list, then passed after `promote` required exactly `e2e` and `release-notes` with no job-level `if`.
- The workflow regression tests passed, and the source inventory found 52 Action references across nine workflow files with one Dependency Review step.
- All 16 approved Action references resolved from their public upstream repositories.
- Go 1.27.0, golangci-lint v2.13.2, the ten direct modules, module verification, and `go mod tidy -diff` passed their exact assertions.
- The public image digest and platform checks passed, the internal builder reference stayed byte-identical to the baseline, and the e2e workflow and suite use v0.11.3.
- The audit classified all 44 changed module versions under allowed licenses, with no unknown or unacceptable license.
- Pinned `govulncheck@v1.7.0` exited 0 with no reachable known vulnerability.
- The complete 24-file branch diff passed whitespace and sensitive-information review from baseline `fcbc56d`.
- `GOTOOLCHAIN=go1.27.0 task lint` exited 0 with every repository lint gate active.
- `GOTOOLCHAIN=go1.27.0 task test` exited 0 after generation, formatting, vetting, envtest, and all 13 non-e2e Go packages.
- The final generated-file comparison exited 0 with no drift in `config`, `api`, `internal`, `test`, `go.mod`, or `go.sum`.
- E2e was not run during setup because it targets a real Kubernetes cluster.
- Task 7 rebased all 18 commits onto `bfc6e2de46367b55765711cae9d6ca0c6307584b`; the only conflict kept the upstream e2e timeout handling and the Go 1.27 `errors.AsType` update.
- The protected `CONTRIBUTING.md` and `tasks/todo.md` diff restored at its exact pre-rebase SHA-256 hash before this final task-state update.
- Post-integration build-configuration, dashboard, metrics, and timeout-focused e2e unit tests exited 0.
- Post-integration `GOTOOLCHAIN=go1.27.0 task lint`, `GOTOOLCHAIN=go1.27.0 task test`, and `mkdocs build --strict` exited 0 without generated-file drift.
- Independent review approved the complete 25-path diff from the integration target with no findings, and a later fetch found `origin/main` unchanged.
- Local real-cluster e2e was not run during Task 7, as required.
