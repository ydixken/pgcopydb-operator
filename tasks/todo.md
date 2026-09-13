# Issue 95 maintenance plan

## M2: follow diagnostics

- [x] Branch from `origin/main` and rebase after PR #261 without editing its two E2E files.
- [x] Check the pinned receive/apply implementation and SQLite WAL semantics.
- [x] Replace the progress-bounds setup wait with the existing convergence helper.
- [x] Sample source byte positions and target row counts every 30 seconds within the existing budget.
- [x] Add regression coverage and document the diagnostic's limits.
- [x] Run `task lint` for each implementation commit.
- [ ] Push a draft PR and inspect CI tests and documentation checks.
- [ ] Confirm any proposed worker paths on a live worker before adding a worker probe.
- [ ] Resolve transform/apply discrimination before claiming three-stage diagnosis.
- [ ] Wait for the A/B/A result before a separate, final budget-value commit.

File sizes do not supply a reliable transform/apply boundary: SQLite recycles WAL files, and apply runs transformation inline.
The diagnostic must not classify a stage from those sizes alone.
The existing 300-second convergence budget, 20,000-row backlog, 16Mi allowance, and EXTERNAL progress payload remain unchanged.
Local `task lint` passed for the wait change and the diagnostic change with `0 issues.`.
Workflow lint skipped locally because `actionlint` is unavailable; functional tests and the documentation build run in CI.

## Issue 220: all databases

- [x] Add the API field, admission rules, generated artifacts, and validation tests.
- [x] Render clone and schema-compare arguments with golden coverage.
- [x] Add instance-wide preflight probes and shell tests.
- [x] Sample instance sizes without per-database progress counters.
- [x] Wire schema verification and reject data verification in the builder.
- [x] Update configuration, prerequisites, reference, troubleshooting, and examples.
- [x] Add the positive E2E spec with fixture cleanup.
- [x] Resolve the negative E2E attempts assertion and add the zero-attempt/no-worker-Job spec.
- [x] Fix the four repeated preflight test literals reported by `goconst`.
- [x] Reproduce the chart-ownership failures on clean baseline `25822ae` with the changes stashed, then restore the changes.
- [x] Format, run `task lint` on each implementation commit, and compare the reconstructed tree with the reviewed snapshot.
- [ ] Require CI and the full feature E2E result on the exact PR head before any merge.

### Issue 220 review fixes

- [x] Round 2: stop after failed superuser probes and prove downstream probes do not run (S1).
- [x] Round 2: fix the prerequisites summary and document sequential extension-probe cost (O3 and O4).
- [x] Round 2: clarify the compare guard's direct-call coverage and the inadmissible deepcopy fixtures (O1 and O2).
- [x] Round 2: rerun lint, affected packages, E2E compilation, and tool availability checks.

The S1 harness regression failed without the early exit because database enumeration continued after both superuser failures; it passed with the guard immediately after those probes.
The compare-builder guard remains as a defensive backstop for direct callers; its builder test covers it, while the post-clone reconcile test exercises `buildJob` validation.
Both deepcopy fixtures retain populated incompatible options and explicitly prohibit reuse for admission tests.
Round 2 verification passed all five affected packages with envtest, E2E compilation, and `task lint` (`0 issues.`).
GNU `timeout`, `actionlint`, and `mkdocs` are still unavailable on PATH: `timeout` reports uutils 0.8.0, and workflow lint still skips.
The full test and docs-build gates remain outstanding without workarounds.

- [x] Reject all three incompatible all-databases combinations through terminal `InvalidSpec` validation before creating Jobs (findings 1 and 5).
- [x] Populate the field in both full API fixtures and correct the metric and connection documentation (findings 2 through 4).
- [x] Omit unused remediation credentials from all-databases preflight and remove the one-item list (findings 6 and 10).
- [ ] Run regression tests, lint, E2E compilation, and the full gates when their tools are available.

The admission-bypass and credential-omission regressions failed before the review fixes and passed afterward.
The controller regression checks persisted conditions, a warning event, zero attempts, no Jobs, and an absorbing terminal reconciliation for each invalid combination.
A post-clone regression checks terminal failure without losing recorded clone success or creating a data-compare Job.
Existing transient-error propagation tests remain unchanged and pass.

With an absolute envtest assets path, `go test ./api/... ./internal/conn ./internal/controller ./internal/pgcopydb -count=1` passed all five packages after the fixes.
The first package run could not start envtest because the invocation supplied a relative assets path; the corrected invocation required no code or test changes.
E2E compilation exited 0, and the final `task lint` exited 0 with `0 issues.`; workflow lint still skipped because `actionlint` was unavailable.

Finding 7 is deferred: the suspected kept-cluster conflict is unverified, and resetting the shared maintenance database or cluster-wide roles needs ownership-scoped fixtures rather than broad deletion.
Finding 8 is deferred: log-tail truncation can undercount the displayed checks but does not change the Job's preflight verdict.
For finding 9, commit `578b3e3` isolates the unrelated defaults, binary-COPY comment, and runner-version documentation corrections.
The database-listing failure path remains a diagnosis limitation: if the target disappears after the superuser probes, subsequent extension failures can displace the target error with source-labelled notes before the footer runs.
The failure exit code and terminal state remain correct.

The approved plan requires local tests for this task.
Publication of the feature branch and GitHub PR, followed by CI and the exact-head full feature E2E gate, is authorized.
Merge and release creation require a separate confirmation after the gate results and RC procedure are reported.

### Issue 220 handoff

The user corrected the planned negative E2E assertion to zero worker attempts after `PreflightFailed`; lifecycle semantics remain unchanged.
The negative spec uses the fixture's app credentials with maintenance-database connections, requires superuser failure messages for both sides, and asserts zero worker attempts and no worker Job.
Configuration, prerequisites, option coverage, conditions, troubleshooting, planning, and examples document the all-databases contract.
The positive E2E spec seeds an extra source database, clones through superuser maintenance connections, checks the target app and extra-database row counts, and cleans up its Migration, database, password, Secret, and target objects.
After both E2E specs were added, `go test -c ./test/e2e -o /tmp/opencode/issue220-e2e.test` exited 0; no local cluster E2E run was made.

`make manifests generate`, `./hack/sync-chart-crd.sh`, and `task docs` exited successfully.
`task test` passed the API, controller, argument, and seven other packages, but failed in `internal/progress` because the installed `timeout` is uutils rather than GNU, and in two `TestFeatureE2EChartOwnershipLifecycle` cases with `run-owned Migration UID-preconditioned deletion failed`.
`go test ./test/buildconfig -run '^TestFeatureE2EChartOwnershipLifecycle$' -count=1` reproduced both chart-ownership failures on clean baseline `25822ae` with the changes stashed.
Both failures reported `run-owned Migration UID-preconditioned deletion failed`; they predate this change and are out of scope.
The stash restored without conflicts.
After the negative E2E spec was added, `task lint` exited 0 with `0 issues.` from Go lint and successful YAML, generated-chart drift, chart, documentation-link, and Prometheus checks.
Workflow lint still reported `lint: no actionlint, skipping workflow lint`.
The tool check still resolved `timeout` to uutils and found no `actionlint` on PATH.
Per the user's instruction, full tests and the docs build await tool provisioning; workflow lint also needs rerunning once `actionlint` is available.
`git diff --check` passed.
The first checks used Go 1.27.0, Task, and yamllint installed under `/tmp/opencode`; subsequent lint and E2E compilation used the provisioned tools on PATH.
No repository dependency versions or host configuration were changed by this work.

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
- [ ] Push `fix/issue-95-maintenance`, open the first pull request, and record its public URL (Task 8 remote validation in progress).
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
- [ ] Run `git diff --check`, format touched files, and run `task lint` on the digest-only change.
  Require CI tests and documentation validation before merge.
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

## Runtime safety implementation plan

- [x] Merge #242 progress-sampler cleanup through PR 250 with exact-head shared E2E evidence.
- [x] Retire the temporary Kind delivery design and preserve its source refs.
  The route did not prove its prerequisite on the actual runner path; no failure cause is claimed.
- [x] Replace PR 252 locally with #211, #223, #210, and only the approved #200 scheduling compensation.
- [x] Defer all #215, #243, #209, and #221 behavior.
- [x] Keep #88 out of scope.
- [x] Complete the shared-cluster delivery gates below.

## Runtime safety evidence

- #242 merged through [PR #250](https://github.com/ydixken/pgcopydb-operator/pull/250).
  [Full feature E2E](https://github.com/ydixken/pgcopydb-operator/actions/runs/34134512551) verified head `21d10c3864973c8e877a8cafa99f5b24f3318fa6`, including the sampler scenario and cleanup.
  Post-merge [CI](https://github.com/ydixken/pgcopydb-operator/actions/runs/34139188021), [docs](https://github.com/ydixken/pgcopydb-operator/actions/runs/34139188028), and [mirror](https://github.com/ydixken/pgcopydb-operator/actions/runs/34139188030) passed on `9419372db414c96731abda6ebad13ac99ce7e1b8`.
- #211 uses coherent source `972781357628526a220db172f157398e2090599c`.
- The complete deferred #215 and #243 source remains at `a62ef17724108f6d20b796672fa656188deb82f8`.
  Earlier #215 branch and stash refs remain extra backups.
- The deferred #209 and #221 source remains at `43f51270455370224cea493cd51d7b3908fea56d`.
- PR 252 used local formatting, `git diff --check`, and `task lint`.
  CI supplied functional, build, and documentation evidence.

## Runtime safety exclusions

- [x] Defer #88, including pgcopydb fork, SQLite contention, builder publication, and runner digest work.
- [x] Exclude source write fencing from the #211 catch-up guard.

## Shared-cluster scope reduction

- [x] Approve one replacement PR 252 that removes the temporary Kind environment and observer while keeping the existing GitHub/ARC runners and shared-cluster E2E route.
- [x] Preserve the original issue branches, integration branches, complete deferred PR source, and protected stash listed in `tasks/shared-e2e-removal-design.md`.
- [x] Record the approved implementation and delivery gates in `tasks/shared-e2e-removal-plan.md`.
- [x] Create `integrate/shared-e2e-on-main-1933ce5` from fetched actual `main` at `1933ce5bb7a2322fddf563ffdbf09b012885cbf1`.
- [x] Remove the temporary disposable workflow, observer, documentation, and tests without changing the established shared-cluster route.
- [x] Integrate #211, #223, #210, and only the approved #200 scheduling compensation with focused CI-run regression coverage and current documentation.
- [x] Exclude #215, #243, #209, #221, and #88.
  Keep #200 open for the historical 35-second symptom.
- [x] Complete an independent static review of the retained runtime diff.
- [x] Complete an independent static review of the workflow, helper, and build-configuration removal diff.
- [x] Format touched files, run `git diff --check`, and run `task lint` locally.
  CI remains responsible for functional tests, builds, and documentation validation.
- [x] Fetch and rebase onto actual `origin/main`, then repeat the static review and local formatting, whitespace, and lint gates if the head changes.
- [x] Publish over the existing PR branch only with an explicit lease against `a62ef17724108f6d20b796672fa656188deb82f8`.
- The `feature-e2e` gate is removed by this change; PR 252 predates the removal and no run URL is recorded here.
- [x] Merge normally, verify the merged commit on actual `main`, and close #211, #223, and #210.
  Leave #200, #215, #243, #209, and #221 open.
