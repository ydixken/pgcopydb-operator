# Issue 95 maintenance plan

## M2: follow diagnostics

- [x] Branch from `origin/main` and rebase after PR #261 without editing its two E2E files.
- [x] Check the pinned receive/apply implementation and SQLite WAL semantics.
- [x] Replace the progress-bounds setup wait with the existing convergence helper.
- [x] Sample source byte positions and target row counts every 30 seconds within the existing budget.
- [x] Add regression coverage and document the diagnostic's limits.
- [x] Run `task lint` for each implementation commit.
- [x] Push a draft PR and inspect CI tests and documentation checks.
- [ ] Verify workflow syntax with `actionlint`; neither the local toolchain nor the CI lint steps ran it.
- [ ] Confirm any proposed worker paths on a live worker before adding a worker probe.
- [ ] Resolve transform/apply discrimination before claiming three-stage diagnosis.
- [x] Receive the A/B/A result before the separate, final budget-value commit.

### M2 finalization

- [x] Fetch and rebase onto `origin/main`: already contains `9b1668f`, with no conflicts or rewritten commits.
- [x] Add the approved 12-minute backlog drain budget without changing idle convergence or fixture shape.
- [x] Cover the longer diagnostic schedule and record the A/B/A provenance and attribution limits.
- [x] Run `task lint` before the unsigned final budget commit: exit 0, `0 issues.`; local `actionlint` unavailable.

Workflow lint belongs to a separate PR; no workflow edits, merge, or release dispatch belong to this finalization.
Record publication, draft removal, and exact-head `lint`, `test`, and `docs` evidence in PR #263.

File sizes do not supply a reliable transform/apply boundary: SQLite recycles WAL files, and apply runs transformation inline.
The diagnostic must not classify a stage from those sizes alone.
The diagnostic commits preserved the 300-second convergence budget, 20,000-row backlog, 16Mi allowance, and EXTERNAL progress payload.
Local `task lint` passed for the wait change and the diagnostic change with `0 issues.`.
Workflow lint skipped locally because `actionlint` is unavailable; functional tests and the documentation build run in CI.

### M2 verification

[PR #263](https://github.com/ydixken/pgcopydb-operator/pull/263) contains the wait and recurring-snapshot changes.
[CI run 34779816683](https://github.com/ydixken/pgcopydb-operator/actions/runs/34779816683) passed `lint`, `test`, and `docs` for `e61e0ab24655fcf9fc5d168a4f6c76ee09e0f708`.
The test log records successful unit/envtest suites and `go test ./test/e2e -run '^TestCutoverDiagnostic' -count=1` against disposable PostgreSQL.
The documentation log records a successful `mkdocs build --strict`.
The CI lint job does not invoke `actionlint`, so its success does not close the workflow-syntax gate.
Three-stage attribution remains unresolved, and no worker filesystem probe is included.
The 2026-09-13 A/B/A replay measured 84.3/123.5/85.5 KB/s receive progress and 347.2/236.9/337.3 seconds from sender resume to cutover under powersave/performance/powersave.
The approved 12-minute budget is environmental headroom over roughly 350 seconds on powersave, not a measured requirement or a resolution of [#260](https://github.com/ydixken/pgcopydb-operator/issues/260).
The experiment did not exercise progress-bounds, whose fresh-seed behavior remains unproven.

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

## Issues 274 and 275: RC1 verification and rebase checkpoint

The user authorized the next delivery sequence: "Afterwards rebase pr 276, merge it and cut another rc. then e2e test."
This supersedes the earlier no-merge-276 restriction in the historical phase records below.
The current task permits local rebase, diff review, formatting checks, and `task lint`; publication and merge await the review handoff.
Keep the fork's `fix/keepalive-feedback` branch for upstream submission.

- [x] Verify `v0.13.3-rc.1` [release run 35117259154](https://github.com/ydixken/pgcopydb-operator/actions/runs/35117259154) succeeded on `2045910b60406a250cdeb2f9060595a91e79e060`, including [E2E job 104866789797](https://github.com/ydixken/pgcopydb-operator/actions/runs/35117259154/job/104866789797) and its cleanup step.
  The RC1 handoff records 40 passing specs out of 47, with seven excluded, including both Manual and Automatic keepalive cases.
- [x] Verify a clean tracked tree, local and remote PR #276 head `7a86003a6e937b48671bdd6d0a1010a11db284e1`, and fetched `origin/main` at the RC1 SHA; record both user-owned release-assessment file hashes without reading their contents.
- [ ] Replay all three PR #276 commits with per-invocation `--no-gpg-sign`, preserving their messages, both issue plans, and both fixes.
- [ ] Review `git range-diff`, verify formatting and whitespace, run `task lint`, and record final HEAD and file-hash proof in the delivery handoff before publishing.
- [ ] After review, publish with a strict lease against `7a86003a6e937b48671bdd6d0a1010a11db284e1`, verify exact-head CI, and merge PR #276.
- [ ] Cut the next release candidate and verify its E2E suite and cleanup on the exact merged SHA.
- [ ] Update the existing [issue 274 resolution comment](https://github.com/ydixken/pgcopydb-operator/issues/274#issuecomment-5701335993) and [issue 275 resolution comment](https://github.com/ydixken/pgcopydb-operator/issues/275#issuecomment-5701345951) with the final RC2 proof after it exists.

This checkpoint precedes the commit replay; the sections below preserve each earlier phase's evidence and authorization boundaries.
RC1 verifies the keepalive fix on main, not PR #276 or RC2.

## Issue 275: approved keepalive feedback

The receiver owns genuine primary keepalive position `K`, required stored COMMIT position `C`, and the certified network replay floor.
`C` is the maximum non-skipped COMMIT stored for apply, including preexisting unapplied spool across retained and rotated files at startup.
`P` is the durable apply sentinel position.
Promote network replay and flush feedback only when `P` is initialized, `P >= C`, no receive transaction is open, endpos is unset, and a genuine `K` has arrived on this connection.
The certified replay floor MUST be monotonic; synthetic keepalives and WAL data headers do not establish `K`.
Keep `previousLSN`, target replication origin, and sentinel replay as data cursors.
Do not relax operator lag thresholds, add a heartbeat, or introduce persistent catalog schema without escalation.

### Delivery plan

- [x] Prepare isolated branches from freshly fetched operator `origin/main` and fork `origin/v0.18-fixes`, and read repository guidance and delivery workflows.
- [x] Record the approved design and verification sequence without changing production code.
- [ ] Add fork regressions with baseline-fail evidence for idle feedback and safety assertions for `P = 0` prefetch/resume, paused apply, retained/rotated startup spool, a large open transaction, an empty source, reconnect, and explicit endpos.
  Assert source feedback, durable data cursors, and target contents; missing observations MUST fail.
- [x] Implement the receiver certification using existing stream and SQLite interfaces, with no new persistent catalog schema, and update the affected fork documentation.
- [ ] Record before/after CI run URLs and exact SHAs, plus isolated fork regression results proving baseline failure and fixed success.
  Dispatch `run-tests.yml` explicitly for the feature branch because its automatic triggers only target `main`; include rotation coverage through `nightly.yml` or an explicit regression run.
- [x] Complete independent review and resolve findings; the publication handoff reports approval with no blockers.
- [x] Merge the fork PR into `v0.18-fixes` only after the delivery leader reviews final exact-head CI results.
  Use `git commit --no-gpg-sign` per invocation; do not change Git configuration.
- [x] Retain the fork's `fix/keepalive-feedback` branch after merge for an upstream PR; do not delete it as part of merge cleanup.
- [x] Create a concise upstream-submission tracking issue linking the retained branch, fork PR, and exact regression evidence: [fork issue #8](https://github.com/ydixken/pgcopydb/issues/8).
  The delivery owner performs these remote actions; the operator E2E/docs task does not commit, push, merge, dispatch workflows, or access the cluster.
- [x] Derive the version from the merged fork commit, stage that SHA/version in the operator builder branch, and dispatch `pgcopydb-builder.yml` on that branch.
  Verify both architectures and record the published manifest digest and build run URL before pinning the runner.
- [x] Update the operator builder/runner pins, version assertions, progress allowlists, affected tests and docs, and issue-specific E2E coverage together.
  Formatting is complete; final `task lint` awaits review and commit of intentional generated CRD changes.
  Use CI for operator functional and documentation checks.
- [ ] Verify `runner-smoke.yml` on the pinned operator branch, review the final diff, and merge the operator PR only after exact-head `lint`, `test`, and `docs` succeed.
- [ ] Cut a release candidate through `auto-release.yml`, verify the candidate's published image/chart artifacts and successful `release.yml` E2E job, and record exact SHAs, digests, and run URLs.
  Promote through `promote.yml` with its `candidate` input when completing stable delivery, then verify the stable artifacts before closing issue 275.

### Setup review

Operator base: `515cca8e39a665358b9f58b58a83deaa556a0639`, branch `fix/issue-275-keepalive-feedback`.
Fork base: `aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad`, branch `fix/keepalive-feedback`.
The fork's public upstream tags were fetched locally; the base describes as `v0.18-10-gaadc4bf`.
GitHub Actions is enabled on the fork, whose test workflows use `ubuntu-latest`; no workflow was dispatched during setup.
The original issue 274 workspace, PR 276, and its untracked release assessments remain untouched.
Runner permissions, environments, credentials, branch protection, and other security configuration changes require escalation rather than alteration during this work.

### Operator E2E and documentation

- [x] Verify `cutover.mode` is mutable in the served CRD and trace the two-sample catch-up gate and endpos-only nudge.
- [x] Add active Manual and Automatic idle-feedback cases with exact publication inventory, frozen rows, durable-origin checks, default lag allowance, and test-owned cleanup.
- [x] Distinguish certified network feedback from durable data cursors in the live-migration, monitoring, and follow-diagnostics guides.
- [x] Complete scoped cleanup, formatting, and local lint review.
- [ ] Require CI verification with the integrated runner pin and release-candidate E2E evidence; the old runner must fail the pre-cutover feedback boundary assertion.

`gofmt -w test/e2e/keepalive_feedback_test.go` and `git diff --check` passed.
`task lint` passed with `0 issues.` after correcting two Ginkgo error-assertion lint findings without exposing raw remote errors.
Workflow lint skipped because `actionlint` is unavailable; generated CRD/RBAC and chart drift checks passed.
No functional tests, documentation build, or cluster E2E ran locally, and no commit, push, merge, workflow dispatch, or tracking-issue creation occurred in this task.
Operator baseline failure and fixed-runner success remain CI gates, not observed results from this task.

### Operator review follow-up

- [x] Remove the terminal cached-replay comparison; retain direct pre-cutover feedback, exact origin, frozen data, final endpos, drain, and cleanup assertions.
- [x] Share the ownership-checked sender signal helper, explain the unscaled WAL payload, and poll the five-query feedback check every five seconds.
- [x] Restore published baseline source permalinks without attributing candidate certification to the baseline implementation.
- [x] Run formatting and local lint after the review fixes.
- [x] Integrate the candidate builder/runner pin and replace feedback provenance with the published candidate commit before delivery.
  The documentation uses merged source `4873c18`; the reviewed exact-origin assertion remains unchanged.

Both touched Go files were formatted, and `task lint` passed with `0 issues.` after the follow-up changes.
Workflow lint still skipped because `actionlint` is unavailable; no functional tests, builds, or cluster operations ran in this follow-up.

### Fork publication gates

- [x] Inspect all 20 intended files, including the seven untracked suite files, recent history, remote tracking, and the full base diff.
  Fetched `origin/v0.18-fixes` remains at `aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad`, with no preexisting feature commits.
- [x] Match the C-source diff and final harness hashes to the recorded fixed runtime inputs.
- [x] Run final C-style, banned-API (`make tests/ci`), shell-syntax, and diff-whitespace checks successfully.
- [x] Inspect GitHub's branch-deletion setting read-only: `delete_branch_on_merge=false`.
- [x] Commit only the intended fork paths with per-invocation `--no-gpg-sign`, then push without force.
- [x] Publish the fork tracking issue and PR with the executed baseline/fixed reproduction and its limits.
- [x] Dispatch `run-tests.yml` and `nightly.yml` on `fix/keepalive-feedback`, then record their exact-head URLs.
- [x] Require final CI review before merge and retain the feature branch for upstream submission.
- [ ] Complete the builder/image-pin gates above after the fork merge.

The paired idle reproduction uses PostgreSQL 18.6 source and target and the same test harness against baseline and fixed binaries.
Baseline `0.18.10.gaadc4bf` fails the feedback predicate at filtered head `0/4000000`, while flush and replay remain at durable COMMIT `0/179B088`.
The fixed idle run reports `0/4000000` for wire write, flush, replay, and confirmed flush, with durable origin and sentinel replay still at `0/179B088`.
The final full keepalive suite passes idle, backlog, restart, empty, and synthetic in-flight cases.
The runtime-tested fixed binary identifies as `0.18.10.gaadc4bf.dirty`, SHA-256 `c6cc4ddbe644896f5421cc151f6e78f95d12f481924374936a082d41a92241d3`.
Its recorded C-source diff SHA-256 is `4d7d88e7a7e027d510a26170ae95f4c282fb01763849062a3f5c5c969fa9ed7c`; the publication diff matches it.
CI must build the committed source before any exact-commit runtime claim.
Existing `cdc-pgoutput`, `cdc-pgoutput-resume`, `cdc-file-rotation`, and `cdc-endpos-mid-txn` logs record exit 0 with PostgreSQL 18.6 source and target.
The handoff records the corrected multi-WAL suite passing with PostgreSQL 16.15 source and 18.6 target after the narrow wal2json allowlist fix; this is not an 18/18 result.
Receiver SQL/I/O failure injection and a genuine primary keepalive inside an open receive transaction remain untested at runtime; the in-flight case uses a synthetic flush keepalive.
The fix permits a transient feedback gap until the next keepalive and status exchange.

### Fork publication evidence

[Commit d255e43](https://github.com/ydixken/pgcopydb/commit/d255e4316def7002ded1374c79691bb97fa60c26) contains the 20 intended fork files and is unsigned, as authorized.
At initial publication, the remote feature ref matched `d255e4316def7002ded1374c79691bb97fa60c26`; Git described it as `v0.18-11-gd255e43`, and regenerated version metadata reported `0.18.11.gd255e43`.
[Fork issue #8](https://github.com/ydixken/pgcopydb/issues/8) contains the runnable paired reproduction and actual failing/passing output, with links to the baseline, fixed source, retained branch, and [PR #9](https://github.com/ydixken/pgcopydb/pull/9).
PR #9 targets `v0.18-fixes` from `fix/keepalive-feedback` and has no auto-merge request.

Both manual workflow runs identify the exact feature head above:

- [Run Tests, 35096822986](https://github.com/ydixken/pgcopydb/actions/runs/35096822986): completed with failure; unit failed on PG16/18 and cdc-wal2json failed on PG18.
- [Nightly Tests, 35096822809](https://github.com/ydixken/pgcopydb/actions/runs/35096822809): completed with failure; unit and cdc-wal2json failed on PG16/17/18, and cdc-pgoutput failed on PG16/17.

The delivery leader MUST review both final CI results before the fork merge.
Keep the feature branch after merge for the upstream PR; GitHub's read-only setting check returned `delete_branch_on_merge=false`.
This publication step changes no repository settings and publishes no images.
The operator bookkeeping remains uncommitted; builder publication, image pins, operator CI, and release E2E still require the gates above.

### Fork PR 9 CI remediation publication

The user approved publication of the independently reviewed test-only batch at fork head `d255e4316def7002ded1374c79691bb97fa60c26`.
The production `src` tree MUST remain `0f1d5b02332ac1f47dedc79507302a32ad1f7ca9`.
The initial workflows contain 40 and 86 jobs, respectively; all 126 gates require fresh runs on the remediation commit.
The initial cdc-keepalive jobs passed on PG16/17/18, covering idle, backlog, restart, empty, and synthetic in-flight cases.
Those results are initial-head evidence, not a pass for the next commit.

- [x] Inspect actual status, the complete tracked diff, all six new files, staged changes, recent history, remote refs, and source-tree identity.
- [x] Confirm `delete_branch_on_merge=false`, fork PR #9 open with auto-merge disabled, and operator PR #276 open and unmerged.
- [x] Verify final C-style, banned-API, shell-syntax, and diff-whitespace checks without changing the approved test sources.
- [x] Stage only the 22 intended test paths, commit with per-command `--no-gpg-sign`, and push `fix/keepalive-feedback` without force.
- [x] Update existing PR #9's five-section body with the CI remediation and actual runtime evidence; retain issue #8 as the keepalive reproduction.
- [x] Dispatch new `run-tests.yml` and `nightly.yml` runs on the exact new feature head and record their URLs and initial status.
- [x] Require the delivery leader's review of all 126 final gates before merge; retain the feature branch and leave operator PR #276 unmerged.

The batch corrects the two-line psql padding golden, explicitly allows wal2json, and normalizes XIDs by first appearance while preserving transaction identity and all other message content.
The pgoutput harness uses exact decoded COMMIT positions for captured XIDs and proves uncommitted target DML with a row-lock barrier before SIGKILL, with GDB inferior calls disabled.
The follow harness propagates failures, rejects missing or empty CDC evidence, checks a persistent marker and full nonempty table digests, and lets the main test own completion after the injector receives its endpos acknowledgement.
The follow fixture opts into the existing numeric-as-string flag, which requires wal2json 2.6; production defaults remain unchanged and compatible with wal2json 2.5.
Earlier follow jobs reported success despite plugin failure and missing CDC databases; their green status did not prove replication.

The approved runtime handoff records full PG18 unit success for nine goldens and six scripts, full cdc-pgoutput success in 42 seconds (commit, between, SIGTERM, SIGKILL before COMMIT, and resume), and full cdc-wal2json success for 114 messages, SQL templates, idempotency, and float precision.
The final follow run records matching actor, rental, and payment digests with 200, 16044, and 16049 rows, respectively, the persistent marker, and exit 0 for both the main test and injector.
These are pre-publication runtime results; fresh exact-head CI remains required.
Runtime fixture credentials and raw logs MUST remain outside commits and public PR text.

### CI remediation publication evidence

[Commit b205225](https://github.com/ydixken/pgcopydb/commit/b20522547688021b65d176b97677415d2ae89076), `test: repair CDC CI fixtures and durability assertions`, contains 22 test paths with 502 insertions and 98 deletions, including all six new harness/documentation files.
The commit is unsigned as authorized, the non-force push succeeded, and the remote branch and PR #9 head match `b20522547688021b65d176b97677415d2ae89076`.
Git describes it as `v0.18-12-gb205225`, corresponding to version `0.18.12.gb205225`; this is a source-version derivation, not a claim that the local runtime binary was rebuilt.
Both the staged tree and committed tree retain production `src` hash `0f1d5b02332ac1f47dedc79507302a32ad1f7ca9`, with no production diff.
The fork worktree is clean, with no unstaged, staged, or untracked files after publication.

The final CI-image `citus_indent --check` exited 0, as did `make tests/ci`, direct `sh ./ci/banned.h.sh`, all ten changed shell files' `bash -n` checks, and staged `git diff --check`.
The host formatter reported differences on unchanged C files; the fork's documented CI style image passed on those same files.
No production formatting edits were made.
The first push failed because the stored credential helper referenced a missing Homebrew executable.
The retry used the installed `gh auth git-credential` helper through command-scoped `git -c` options; stored Git configuration and hooks were unchanged.

Both fresh runs use event `workflow_dispatch`, branch `fix/keepalive-feedback`, and exact head `b20522547688021b65d176b97677415d2ae89076`:

- [Run Tests, 35108902754](https://github.com/ydixken/pgcopydb/actions/runs/35108902754): initial observation `in_progress`, no conclusion; style and banned-API checks passed while docs and PG16/18 image builds ran.
- [Nightly Tests, 35108903235](https://github.com/ydixken/pgcopydb/actions/runs/35108903235): initial observation `in_progress`, no conclusion; banned-API passed while PG16/17/18 image builds ran.

The last publication-session observation found all 40 and 86 jobs present, with 16 and four successful jobs, respectively, and no failed jobs.
GitHub reported both workflows as `queued` with active test jobs and no conclusion; this is an intermediate snapshot, not a final gate result.

The five-section PR #9 body records the original failure history, safe reproduction excerpts, full local suite evidence, remediation design, unchanged numeric defaults, and these new CI URLs.
Issue #8 remains the keepalive reproduction and was not edited.
At publication, full exact-head CI and delivery-leader review were pending; neither fork PR #9 nor operator PR #276 was merged or given an auto-merge request.
This session updates only this uncommitted task file in the operator worktree; all preexisting operator changes remain preserved.

### Conditional fork merge verification

- [x] Verify Run Tests `35108902754` and Nightly Tests `35108903235` at exact head `b20522547688021b65d176b97677415d2ae89076`, with all 40 and 86 individual jobs completed successfully.
  Any non-success conclusion blocks merge; do not rerun or waive a gate.
- [x] Recheck the two reviewed commits, 42 reviewed paths, original base `aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad`, and production `src` tree `0f1d5b02332ac1f47dedc79507302a32ad1f7ca9`.
- [x] Update PR #9 with final exact-head CI evidence and coverage, reconfirm `delete_branch_on_merge=false`, and merge through `gh` with the exact-head guard.
- [x] Fetch actual `origin/v0.18-fixes`, verify the merged source tree, derive the merged version from the fetched `v0.18` tag, and prove the retained remote feature branch still points to the reviewed head.
- [x] Record PR/issue states and merge evidence here; preserve operator PR #276 unmerged and leave operator changes uncommitted.

At `2026-09-16T14:40:47Z`, both exact-head workflows were completed with conclusion `success` on attempt 1.
The paginated jobs API returned exactly 40 and 86 distinct jobs, all completed with conclusion `success`; no skipped, cancelled, timed-out, neutral, or missing job counted as a pass.
A subsequent step-level check confirmed that all 117 matrix jobs actually completed their `Run a test` step successfully: 35 in Run Tests and 82 in Nightly.
Nightly passed keepalive, file rotation, pgoutput, wal2json, follow-wal2json, unit, and both endpos suites on PG16/17/18; Run Tests also passed the PG18 receive-resume suite, C style, and documentation checks.
The keepalive suite covers idle feedback, paused-apply backlog, retained/rotated startup spool with uninitialized apply, empty source, and synthetic in-flight feedback.
The post-CI GitHub comparison was exactly two commits ahead and zero behind the original base: `d255e4316def7002ded1374c79691bb97fa60c26` and `b20522547688021b65d176b97677415d2ae89076`.
Their 20 source/suite paths and 22 fixture paths match the 42-path PR; both commits have the approved production `src` tree, and the fork worktree is clean.

### Conditional fork merge evidence

The final gates are [Run Tests `35108902754`](https://github.com/ydixken/pgcopydb/actions/runs/35108902754), 40/40 successful jobs, and [Nightly Tests `35108903235`](https://github.com/ydixken/pgcopydb/actions/runs/35108903235), 86/86 successful jobs.
Both runs verified exact head `b20522547688021b65d176b97677415d2ae89076`; the immediate pre-merge check reconfirmed all 126 successful conclusions without reruns or exceptions.
PR #9's description records these final URLs, the exact head, executed test-step counts, coverage, reviewed ancestry, source-tree identity, and runtime limits.

Immediately before merge, GitHub returned `allow_merge_commit=true` and `delete_branch_on_merge=false`; the base remained `aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad`.
The merge used `gh pr merge 9 --repo ydixken/pgcopydb --merge --match-head-commit b20522547688021b65d176b97677415d2ae89076` without branch deletion, auto-merge, or an administrative override.
[Fork PR #9](https://github.com/ydixken/pgcopydb/pull/9) is `MERGED` at `2026-09-16T14:43:54Z`, with merge commit [`4873c1810b73086473903110d9057a1bde37195a`](https://github.com/ydixken/pgcopydb/commit/4873c1810b73086473903110d9057a1bde37195a).
Its parents are the original base and the reviewed head, in that order.

Fetched actual `origin/v0.18-fixes` in `/tmp/opencode/pgcopydb-275`; it resolves to `4873c1810b73086473903110d9057a1bde37195a`.
Fetched upstream `dimitri/pgcopydb` tag `v0.18`, whose peeled commit is `95ebd553790fa45de67c92b934917d777131bdd3`.
`git describe --match 'v[0-9]*' 4873c1810b73086473903110d9057a1bde37195a` returns `v0.18-13-g4873c18`; removing the leading `v` and replacing hyphens with dots gives `0.18.13.g4873c18`.
The merged production `src` tree is `0f1d5b02332ac1f47dedc79507302a32ad1f7ca9`, and the entire merged tree is identical to the reviewed head.
This derives source version metadata; it does not claim a rebuilt binary or published image.

Post-merge GitHub ref lookup and `git ls-remote --exit-code origin refs/heads/fix/keepalive-feedback refs/heads/v0.18-fixes` confirm both remote refs exist:

```text
b20522547688021b65d176b97677415d2ae89076 refs/heads/fix/keepalive-feedback
4873c1810b73086473903110d9057a1bde37195a refs/heads/v0.18-fixes
```

The local feature branch and HEAD also remain at `b20522547688021b65d176b97677415d2ae89076`, with a clean fork worktree.
GitHub's post-merge repository setting remains `delete_branch_on_merge=false`.
[Fork issue #8](https://github.com/ydixken/pgcopydb/issues/8) remains `OPEN` for upstream tracking.
[Operator issue #275](https://github.com/ydixken/pgcopydb-operator/issues/275) remains `OPEN` for the builder, image-pin, and release delivery gates.
[Operator PR #276](https://github.com/ydixken/pgcopydb-operator/pull/276) remains `OPEN` and unmerged at `7a86003a6e937b48671bdd6d0a1010a11db284e1`, with no auto-merge request.
This task changes only this task file in the operator worktree and creates no operator commit.
No builder workflow, release, or image publication was dispatched.

### Builder publication phase

- [x] Inspect worktree status, staged and unstaged diffs, recent history, remote tracking, fork provenance, and the existing builder workflow.
  Read-only fetch found operator `origin/main` unchanged at `515cca8e39a665358b9f58b58a83deaa556a0639`.
- [x] Set the builder's authoritative source SHA and version to merged fork `4873c1810b73086473903110d9057a1bde37195a` and `0.18.13.g4873c18`, with concise keepalive and retained durability provenance.
- [x] Run `git diff --check` and `task lint`, then create an unsigned conventional commit containing only `images/pgcopydb-builder/Dockerfile`.
- [x] Push `fix/issue-275-keepalive-feedback` with upstream tracking and without force, then dispatch `pgcopydb-builder.yml` on that branch without inputs.
- [x] Require successful `pin`, `amd64`, `arm64`, and `merge` jobs on the exact builder commit through bounded GitHub API polling.
- [x] Inspect the published tag, hash its raw index, verify exactly `linux/amd64` and `linux/arm64` image children apart from attestation entries, and record immutable digests and the run URL here.

This phase owns only the builder Dockerfile and this uncommitted bookkeeping.
The runner pin and build-configuration assertions intentionally await the published digest and subsequent integration.
Preserve all pending operator E2E, shared-helper, documentation, and lessons changes unstaged; keep PR #276 unmerged and the fork feature branch at `b20522547688021b65d176b97677415d2ae89076`.
No feature PR, merge, release, cluster action, or security-configuration change belongs to this phase.

The Dockerfile-only source change passed `git diff --check` and local `task lint` with exit 0 and `0 issues.`.
YAML, generated CRD/RBAC drift, chart, documentation-link, Prometheus rule, and Go lint checks passed; workflow lint reported `lint: no actionlint, skipping workflow lint`.
No formatter applies to this Dockerfile edit; operator functional tests remain CI-owned.

[Builder preparation commit `67c95d2`](https://github.com/ydixken/pgcopydb-operator/commit/67c95d2d892fed6d62de7bb117c46280101ab529), `chore: prepare certified keepalive pgcopydb builder`, contains only the builder Dockerfile, with six insertions and three deletions.
The commit used `git commit --no-gpg-sign`; Git reports signature status `N`.
The non-force push created the remote feature branch at exactly `67c95d2d892fed6d62de7bb117c46280101ab529` and set its upstream tracking.
The existing workflow was dispatched with `--ref fix/issue-275-keepalive-feedback` and no inputs: [builder run `35111549279`](https://github.com/ydixken/pgcopydb-operator/actions/runs/35111549279).
The pending docs, lessons, shared-helper, new keepalive E2E file, and runner Dockerfile match their pre-edit Git blob hashes; this bookkeeping remains unstaged.

### Builder publication evidence

[Run `35111549279`](https://github.com/ydixken/pgcopydb-operator/actions/runs/35111549279) completed successfully on attempt 1 at `2026-09-16T14:57:31Z`, with event `workflow_dispatch`, branch `fix/issue-275-keepalive-feedback`, and exact head `67c95d2d892fed6d62de7bb117c46280101ab529`.
Bounded GitHub API polling and the final jobs query confirmed exactly four successful jobs: `pin` (`104846260745`), `amd64` (`104846392257`), `arm64` (`104846392156`), and `merge` (`104847792089`).
Both build-and-push steps, index creation, and the workflow's architecture assertion completed successfully.
The build uses fork source `4873c1810b73086473903110d9057a1bde37195a` and version `0.18.13.g4873c18`; each platform's Dockerfile build includes the existing version canary.

At `2026-09-16T14:58:40Z`, `docker buildx imagetools inspect` resolved the published tag to this immutable image reference:

```text
ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:4873c1810b73086473903110d9057a1bde37195a@sha256:73cd1dbdaa6493f7b0a59a8ebab5742fedccdc0e2ce9f4605c533ed1148978e3
```

The SHA-256 of the exact 1607 raw index bytes equals the reported index digest, and inspection by that digest returns identical bytes.
The index contains exactly these image platforms and child digests:

- `linux/amd64`: `sha256:2bfb53df1329458eee064af8d6c6d34c717f9a72d3111c91fcadae98f144d9b4`.
- `linux/arm64`: `sha256:af8064fe474416a654e49ba1ad939ea2f82551f5e38cef3adbbb486e92a8d62a`.

Each child's raw manifest hash and byte size match its index descriptor.
The two excluded `unknown/unknown` entries identify themselves as attestation manifests and reference those image children.
A second tag read returned the same raw index bytes.

Final checks found operator remote `main` still at `515cca8e39a665358b9f58b58a83deaa556a0639`, and both local and remote feature heads at `67c95d2d892fed6d62de7bb117c46280101ab529`.
Git's staging index is empty, the builder commit contains only its Dockerfile, and all preexisting pending operator paths retain their recorded blob hashes.
The runner still pins the previous source and digest, so the intermediate builder/runner mismatch remains an intentional integration gate.
The fork feature ref remains `b20522547688021b65d176b97677415d2ae89076`; operator PR #276 is open and unmerged at `7a86003a6e937b48671bdd6d0a1010a11db284e1`, with no auto-merge request.
The feature-branch PR query returned no pull requests.
This phase published only the builder image; its evidence remains in this uncommitted task file.

### Final operator integration

Task base: builder-only commit `67c95d2d892fed6d62de7bb117c46280101ab529`.
The edit phase used generation, formatting, and local lint only; functional tests and documentation validation remain CI-owned.
Preserve the reviewed E2E cases, shared sender helper, lessons, unrelated worktrees, fork branch, and operator PR #276.

- [x] Pin the runner to merged fork `4873c1810b73086473903110d9057a1bde37195a` and verified builder index `sha256:73cd1dbdaa6493f7b0a59a8ebab5742fedccdc0e2ce9f4605c533ed1148978e3`, and update both version assertions.
- [x] Put `0.18.13.g4873c18` first in the CLI/chart progress defaults and fixtures, retaining `0.18.10.gaadc4bf` and `0.18.5.ge37d2bd` in that order with explicit compatibility coverage.
- [x] Update runner provenance and public guides to the merged source, preserving historical measurements and distinguishing network feedback from durable data cursors.
- [x] Correct only affected replication-status comments in both API versions, then run `make manifests`, `hack/sync-chart-crd.sh`, and `task docs` without hand-editing generated output.
- [x] Review touched hunks, format touched Go files, and run `git diff --check`.
- [ ] After independent review and a separate commit of intentional generated changes, run `task lint`; its generated-drift check deliberately rejects uncommitted CRD output.
- [ ] Require exact-head operator CI, runner smoke, and release-candidate E2E before claiming integration is verified.

### Final integration edit evidence

The runner FROM uses the full source-SHA tag and the verified multi-platform index above, not either architecture's child digest.
The runner canary and the sole workflow edit, the release version smoke assertion, expect `0.18.13.g4873c18`.
The builder Dockerfile is unchanged from `67c95d2`.
CLI/chart defaults and gate fixtures contain all three versions in the required order; build-configuration coverage explicitly retains both older versions, and shell-gate cases accept all three and reject suffix variants.
Those regression tests are updated, not locally executed.

Source permalink anchors were checked against the fork's clean source tree, which matches merged commit `4873c1810b73086473903110d9057a1bde37195a` at tree `0f1d5b02332ac1f47dedc79507302a32ad1f7ca9`.
The local merged object describes as `v0.18-13-g4873c18`.
The guides attribute certified idle feedback only to the new version; older allowlisted workers retain progress polling, not the keepalive fix.
Historical receive and A/B/A measurements remain attributed to their original runs.

Generation completed with exit 0:

- `make manifests` used the existing controller-gen v0.21.0 and changed only the two served versions' replication-status descriptions in `config/crd/bases/pgcopydb-operator.io_migrations.yaml`.
- `hack/sync-chart-crd.sh` regenerated `charts/pgcopydb-operator/templates/crd-migrations.yaml` from that output.
- `task docs` installed the repository-pinned `crd-ref-docs@v0.3.0` and regenerated `docs/reference/api.md`.
  It warned that two Kubernetes selector types reached its recursion-depth limit; the resulting diff contains only the intended replication-status descriptions.

No API fields, schema validation, RBAC, repository dependency versions, manager version, or chart release version changed.
Scoped cleanup found no additional code to remove.
`gofmt -w` completed for all nine new or touched Go files, and `git diff --check` exited 0.
The reviewed E2E files, shared sender helper, lessons file, and builder Dockerfile retained their pre-task Git blob hashes after formatting.

`git status --porcelain --untracked-files=all -- config/crd/bases config/rbac` reports the intentional CRD modification and no RBAC changes.
The edit phase did not invoke `task lint`: its generated-drift gate requires these changes committed first, and commits were outside that phase.
The verifier MUST review and commit the generated artifacts, then run the unchanged `task lint` before publication.
No local Go tests, E2E, image build, documentation build, remote mutation, or cluster operation ran.
Operator CI, runner smoke, and release-candidate E2E remain pending; the earlier builder and fork results do not verify this operator diff.

### Final operator publication checkpoint

- [x] Accept the full independent review: approved with no blockers.
  Preserve the two optional diagnostic/lint nits unless the required lint gate fails.
- [x] Inspect status, tracked and untracked diffs, recent history, remote tracking, every feature commit, and the comparison with current GitHub `main` at `515cca8e39a665358b9f58b58a83deaa556a0639`.
  The integration contains exactly 24 intended paths, including the new `test/e2e/keepalive_feedback_test.go`; the only earlier feature commit is builder preparation `67c95d2`.
- [x] Confirm the merged fork tree matches tested head `b205225`, both fork workflows passed all 126 jobs, and the retained fork branch still points to that head.
- [x] Verify the published builder index and both image platforms against the runner pin, and inspect the generated descriptions from both API versions, chart CRD, and API reference.
- [x] Format all nine touched Go files and pass tracked and new-file whitespace checks.
- [ ] Commit the 24 explicit paths with `git commit --no-gpg-sign -m "fix: restore catch-up for idle publications"`, then run unchanged `task lint`.
  This intentional commit-before-lint exception follows `CONTRIBUTING.md`: staged generated CRD changes fail the drift gate until committed.
  Stop before pushing if lint fails; any later correction requires a new commit.
- [ ] Push without force, open the normal integration PR to `main`, and dispatch `runner-smoke.yml` on the final feature head for both platforms with `push: false`.
- [ ] Record the exact head, local lint output, PR URL, smoke URL, and observed `lint`, `test`, `docs`, and Codecov results in the PR and delivery report.
  Keep this committed checkpoint's gates pending until their evidence exists; avoid an evidence-only follow-up commit.
- [ ] Hand off to the next gate verifier before merge.
  Release-candidate E2E remains required before closing issue 275.

Operator PR #276 is a separate, open change and contributes no commits to this branch.
The publication scope excludes a merge, release, cluster operation, fork-branch deletion, and changes to other worktrees or pull requests.

## Issue 274: operator-only remediation

### Agreed design

We keep the fix in the operator: conservatively guard publication retries, run all shared progress SQL with `psql --single-transaction` and `SET LOCAL`, and preserve useful database failure lines.
Release E2Es MUST cover retries with an established publication, early orphan recovery, and pooled SQL isolation with COPY and index operations lasting longer than five seconds.
The approved scope excludes pgcopydb fork, catalog, and image-pin changes, and eviction recovery implementation.

### Plan

- [x] Approve the operator-only scope and complete research, as recorded in the approved handoff.
- [x] Author the publication retry guard with regression coverage and active release E2Es for established-publication retry and early orphan recovery.
- [x] Author transaction-local shared progress SQL settings with regression coverage and active release pooling E2Es, including COPY and index operations lasting longer than five seconds.
- [x] Author database failure-line preservation and focused tests.
- [x] Update affected behavior and troubleshooting documentation.
- [x] Complete independent review and resolve findings.
- [x] Inspect formatting of all ten touched Go files and check diff whitespace without modifying code.
- [x] Resolve the local lint findings below and rerun `task lint` successfully.
- [ ] Verify exact-head CI `lint`, `test`, and `docs` on the corrected PR head.
- [ ] Verify the active E2Es on the release candidate carrying the fix.

### Local verification evidence

The checked authoring steps record code, test, and documentation presence, not functional success or review approval.
The three new test files are `internal/controller/publication_retry_test.go`, `test/e2e/publication_retry_test.go`, and `test/e2e/progress_pooling_test.go`.
Both release E2E files register active specs without build tags or excluded labels.
The lifecycle, monitoring, prerequisites, and troubleshooting pages describe the authored behavior.

`git diff --check` passed with no output.
`gofmt -l` passed with no output for all seven modified Go files and the three new test files.
The initial `task lint` run failed with 18 Go lint findings: four `ginkgolinter`, thirteen `goconst`, and one `prealloc`.
Its final output was `make: *** [Makefile:102: lint] Error 1` followed by `task: Failed to run task "lint": exit status 2`.
YAML, generated-manifest drift, chart synchronization, Helm/chart checks, documentation-link checks, and Prometheus rule checks passed before Go lint failed.
The task also ran its built-in `promtool test rules` step, which reported `SUCCESS`.
Workflow lint reported `lint: no actionlint, skipping workflow lint`.

The initial findings, resolved by the successful rerun below, were:

- `test/e2e/progress_pooling_test.go:81:4`, `:125:4`, `:145:3`, and `:186:5`: `ginkgolinter` rejects boolean error assertions and recommends `Expect(error).ToNot(HaveOccurred(), ...)`.
- `internal/pgcopydb/logs_test.go:46:10`: `goconst` reports three occurrences of `pg_restore: error: could not execute query: ERROR:  relation "public.events_2026_01_idx" does not exist`.
- `test/e2e/e2e_suite_test.go:1351:3`, `:1352:3`, `:1353:32`, and `:1354:3`: `goconst` reports `apiVersion`, `kind`, `name`, and `spec`.
- `test/e2e/metrics_test.go:431:49` and `test/e2e/progress_bounds_test.go:70:26`: `goconst` reports `name` and `job-name`, respectively.
- `test/e2e/progress_pooling_test.go:277:4`, `:278:31`, `:279:4`, `:280:31`, and `:322:51`: `goconst` reports `apiVersion`, `namespace`, `spec`, `name`, and `job-name`.
- `test/e2e/publication_retry_test.go:62:27`: `goconst` reports `job-name`.
- `test/e2e/progress_pooling_test.go:268:6`: `prealloc` recommends capacity two for `passfiles`.

`task lint` invokes `make lint`, which runs `golangci-lint run` across `./...`; `run --help` confirms test analysis defaults to enabled.
The configuration does not exclude `test/e2e`, and the initial diagnostics included both new E2E files.
Go lint therefore covers E2E syntax and type loading.
No Go unit, envtest, integration, or E2E tests, test-binary builds, documentation builds, cluster commands, or workflow dispatches ran during this verification.

### Verification rerun

After the review and lint fixes, `gofmt -l` on all ten touched Go files and `git diff --check` passed with no output.
The approved `task lint` rerun exited 0 and reported `0 issues.` from Go lint, resolving all 18 initial findings.
YAML, generated-manifest drift, chart synchronization, Helm/chart checks, documentation-link checks, and Prometheus checks passed.
The task's built-in `promtool test rules` step reported `SUCCESS`.
The only skip was `lint: no actionlint, skipping workflow lint`.
The static analysis includes both new E2E files; it does not execute their specs.
The first exact-head CI result is recorded below; the corrected head still needs passing `lint`, `test`, and `docs` checks.
The release candidate E2E gate remains pending and unexecuted.

### Independent review

The independent reviewer approved targeted closure with no blockers.
At pinned pgcopydb commit `aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad`, [snapshot.c:401-489](https://github.com/dimitri/pgcopydb/blob/aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad/src/bin/pgcopydb/snapshot.c#L401-L489) returns early with a saved LSN and snapshot; otherwise it creates the publication at line 450, creates the slot at line 463, and persists state at line 489.
Comparison respects the fetched, filtered source catalog through [cli_compare.c:366-400](https://github.com/dimitri/pgcopydb/blob/aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad/src/bin/pgcopydb/cli_compare.c#L366-L400), [catalog.c:1363-1400](https://github.com/dimitri/pgcopydb/blob/aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad/src/bin/pgcopydb/catalog.c#L1363-L1400), [copydb_schema.c:318-329](https://github.com/dimitri/pgcopydb/blob/aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad/src/bin/pgcopydb/copydb_schema.c#L318-L329) and [48-51](https://github.com/dimitri/pgcopydb/blob/aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad/src/bin/pgcopydb/copydb_schema.c#L48-L51), and [compare.c:141-149](https://github.com/dimitri/pgcopydb/blob/aadc4bf7a60f3030c569a10c5da2eeb4e531e6ad/src/bin/pgcopydb/compare.c#L141-L149).

### Publication authorization and plan

The user authorized publication with "create a pr, but dont merge yet".
Merge and auto-merge MUST NOT be enabled without further authorization.
The PR MUST use "Related to #274" because fork-level index and eviction recovery remain outside this operator-only patch.

- [x] Inspect the full tracked diff and all three new test files against the intended 16-path scope.
- [x] Fetch `origin/main` and confirm the base is `515cca8e39a665358b9f58b58a83deaa556a0639`, with no extra local commits.
- [x] Prepare `fix/issue-274-operator-retries` and rerun `task lint`.
- [x] Commit only the intended paths as `c4e46db10b90dc001b945fd413d6e726c72bcab7`.
- [x] Push the feature branch, open [PR #276](https://github.com/ydixken/pgcopydb-operator/pull/276), and record its first exact-head CI result below.

The publication-preparation `task lint` run exited 0 with `0 issues.` from Go lint and `SUCCESS` from the built-in Prometheus rule tests.
YAML, manifest drift, chart synchronization, chart rendering, and documentation-link checks passed.
Workflow lint reported `lint: no actionlint, skipping workflow lint`.
`gofmt -l` on all ten touched Go files and `git diff --check` produced no output.
Functional tests and documentation builds remain CI-only; the new cluster specs await the release candidate.

The first commit attempt failed with `gpg: signing failed: No secret key` and `fatal: failed to write commit object`.
The user explicitly authorized one unsigned commit with `git commit --no-gpg-sign`, followed by push and PR creation while leaving the PR unmerged.
This one-off authorization resolves the signing blocker for this commit; Git configuration and signing defaults MUST remain unchanged, and hooks MUST NOT be bypassed.

The untracked `docs/release-assessment.de.md` and `docs/release-assessment.en.md` belong to the user and MUST remain outside the commit.
The publication handoff pushed the implementation commit and opened PR #276.

### PR 276 CI correction

[CI run 35080095962](https://github.com/ydixken/pgcopydb-operator/actions/runs/35080095962) completed on `c4e46db10b90dc001b945fd413d6e726c72bcab7` with `lint` and `docs` successful and `test` failed.
The two failing cases were `TestProgressSQLTransactionOutcome/SQL_error_rolls_back` and `TestProgressSQLTransactionOutcome/cancellation_rolls_back`.
The expectation was wrong: psql's `ACT_SINGLE_QUERY` (`-c`) path returns `EXIT_FAILURE` (1) for a failed query; the test expected the script-mode `ON_ERROR_STOP` exit code 3.
The independent reviewer verified this distinction against psql source and approved the correction.
The assertion now requires exit code 1, still rejects connection failure 2 and process timeouts 124/137, and includes the actual error in its failure message.
The persisted-row commit and rollback assertions remain intact.

- [x] Record the failed exact-head CI run and correct the exit-code expectation.
- [x] Format the touched test and independently run `task lint` on the final correction.
- [ ] Verify the next CI run's `lint`, `test`, and `docs` on the corrected head; no new functional pass is claimed.

`gofmt -w internal/progress/sql_bounds_test.go` and `git diff --check` completed without output.
The final correction's independent `task lint` run exited 0 with `0 issues.` from Go lint.
YAML, manifest drift, chart synchronization, chart rendering, documentation-link checks, and Prometheus checks passed; the built-in rule tests reported `SUCCESS`.
Workflow lint reported `lint: no actionlint, skipping workflow lint`.

The user authorized a new `fix(test)` commit with per-invocation `--no-gpg-sign` and publication on `fix/issue-274-operator-retries` only.
PR #276 MUST remain open and unmerged, with auto-merge disabled.
Functional tests, SQL reproductions, builds, documentation builds, and cluster runs remain outside local verification for this correction.

## RC2 results and RC3 follow-up

PR #276 merged as `9413a3ca6820ca47b6f22091d23d4656dc78904f` under the user's later authorization, superseding the historical no-merge restrictions above.
The RC3 worktree starts from that freshly fetched `origin/main` commit on `fix/rc3-e2e-regressions`.
Keep the fork's `fix/keepalive-feedback` branch, existing worktrees, and user-owned release assessments untouched.
RC2 used pgcopydb `0.18.13.g4873c18`, with builder index `sha256:73cd1dbdaa6493f7b0a59a8ebab5742fedccdc0e2ce9f4605c533ed1148978e3`.
The RC3 builder and pin integration below supersedes that pin.

[RC2 release run 35128223015](https://github.com/ydixken/pgcopydb-operator/actions/runs/35128223015) failed on that SHA.
The handoff records [E2E job 104903548865](https://github.com/ydixken/pgcopydb-operator/actions/runs/35128223015/job/104903548865) with 40 passing, three failing, and seven skipped specs.
Both keepalive cases passed; both publication-retry cases failed, and the pooling case failed with HTTP 403 on Pooler creation.
GitHub's job metadata confirms the E2E failure and successful `Empty the e2e namespaces` cleanup step.

- [x] Create the isolated public worktree from fresh `origin/main` without altering existing worktrees.
- [x] Set the candidate suite in `release.yml` to `E2E_SCALE=0.1`, update its existing contract test, and correct release-coverage documentation.
  Preserve `!chaos && !flaky`, timeouts, context checks, and confirmation prompts; leave `e2e.yml` and unscaled WAL-noise fixtures unchanged.
- [x] Document that chart RBAC and ServiceAccount values configure the manager, not the external E2E runner; keep generated RBAC and chart values unchanged.
  Extend the existing chart RBAC lint check to reject the CNPG fixture API group in the manager ClusterRole.
- [x] Have the private GitOps worker grant the E2E runner only `create`, `get`, and `delete` on `poolers.postgresql.cnpg.io` through a dedicated Role and RoleBinding in `pgcopydb-e2e`, then verify allowed and denied operations.
  Keep private configuration and identity details in private ops notes; do not expand manager privileges.
  The integration handoff records the external E2E RBAC verification with all 15 checks passing; see private ops notes.
- [x] Reproduce the publication-retry defects and supply the reviewed named-dollar-quote and fork bootstrap fixes with regression coverage.
  Full candidate verification remains a separate gate below.
- [x] Review the public config/docs changes, format touched Go files, and run `task lint` locally.
  Functional tests and documentation builds remain CI-owned.
- [ ] After integration and review, publish the follow-up PR, require exact-head `lint`, `test`, and `docs`, and merge.
- [ ] Cut RC3 and require the full candidate suite at `E2E_SCALE=0.1` plus cleanup to pass on the exact merged SHA.

The config/docs phase created no commit, push, PR, merge, release, or cluster change.

### Config/docs verification

Scoped review used base `9413a3ca6820ca47b6f22091d23d4656dc78904f` and the eight uncommitted task paths only.
`gofmt -w test/buildconfig/buildconfig_test.go` and `git diff --check` exited 0.
`task lint` exited 0 on its first run, with `0 issues.` from Go lint.
YAML, generated-manifest drift, chart synchronization, chart RBAC/monitoring, and documentation-link checks passed; the built-in Prometheus rule tests reported `SUCCESS`.
Workflow lint reported `lint: no actionlint, skipping workflow lint`.
The scale contract test is updated but not executed locally; Go tests, the documentation build, and RC3 E2E remain CI gates.
The later integration handoff confirms the external Pooler grant and supplies the named-dollar-quote and fork bootstrap fixes described below.

## Missing-sentinel fork delivery

The approved fork change starts at `4873c1810b73086473903110d9057a1bde37195a` on `fix/resume-sentinel-bootstrap`.
It initializes a missing sentinel only when this invocation's `exportedCreateSlotSnapshot` proves fresh slot creation, preserves existing sentinel fields, and fails on missing retained-slot state or SQL errors.
The RC2 cluster first encountered the operator's `DO $` guard defect; the missing-sentinel failure was reproduced locally through the same clone entrypoint after guard repair.
Track that distinction under [operator issue #274](https://github.com/ydixken/pgcopydb-operator/issues/274) and [RC2 failure #279](https://github.com/ydixken/pgcopydb-operator/issues/279).

- [x] Inspect status, complete intended source/test/docs diff, recent history, remote refs, workflow matrices, and the approved local review evidence.
  The final paired fixture logs show a baseline zero-row sentinel failure and five fixed bootstrap PASS lines across all three plugins.
  The earlier full six-case keepalive, unit, and pagila runs passed; final style and explicit Sphinx `-W` logs passed.
- [x] Confirm preflight fork base `4873c1810b73086473903110d9057a1bde37195a`, retained branch `b20522547688021b65d176b97677415d2ae89076`, and `delete_branch_on_merge=false`.
- [x] Commit only the 14 approved source/test/docs paths with `--no-gpg-sign`, push the new branch without force, and publish the five-section fork issue and PR against `v0.18-fixes`.
  Keep untracked fork `tasks/lessons.md` and ignored environment/log artifacts out of the commit.
- [x] Dispatch both `run-tests.yml` and `nightly.yml` on the exact published feature head and require all 40 and 86 jobs to complete successfully.
  Any failure blocks delivery without reruns or gate exceptions.
- [x] Reconfirm the reviewed tree, ancestry, exact PR head, and disabled automatic branch deletion, then merge with `gh pr merge --match-head-commit --merge` and retain every feature branch.
- [x] Verify the actual merge tree equals the tested feature tree, derive its version with `git describe` from `v0.18`, and record both retained feature refs and public run URLs.
- [x] Complete builder publication and consistent operator-pin integration in a separate task, then obtain independent review before the final integration commit.

That fork delivery did not publish a builder; the RC3 integration below records its publication separately.
Index failures, eviction recovery, established-stream CDC-file loss, and general corrupt-metadata repair are outside this fix.

### Missing-sentinel publication evidence

[Fork commit `5d10b14`](https://github.com/ydixken/pgcopydb/commit/5d10b14de662540a9b2a27030fe264a4e8e7116d) is unsigned (`N`) and has the sole parent `4873c1810b73086473903110d9057a1bde37195a`.
Its 14 paths match the inspected staged tree `e8dfb7af0afdad2844e01a96541bfc425c5cbd0e`; production `src` is `ec71e1fb0a08eef74b92ff705b50ade66a11c56b`.
`git describe --match v0.18` returns `v0.18-14-g5d10b14`, deriving source version `0.18.14.g5d10b14` before merge.
The default push helper referenced an unavailable executable; the successful non-force push used the installed `gh auth git-credential` through command-scoped helper options without changing stored Git configuration.
Remote refs confirm `fix/resume-sentinel-bootstrap` at `5d10b14de662540a9b2a27030fe264a4e8e7116d` and `fix/keepalive-feedback` unchanged at `b20522547688021b65d176b97677415d2ae89076`.

[Fork issue #10](https://github.com/ydixken/pgcopydb/issues/10) and [PR #11](https://github.com/ydixken/pgcopydb/pull/11) publish the executed test entrypoint, actual paired output excerpts, cause, fix, and limits.
PR #11 targeted `v0.18-fixes` with only the reviewed commit and no auto-merge request.
Both manual runs report that exact head, `workflow_dispatch`, and attempt 1:

- [Run Tests `35150666775`](https://github.com/ydixken/pgcopydb/actions/runs/35150666775), created `2026-09-16T21:08:18Z`: all 40 jobs completed successfully.
- [Nightly Tests `35150664335`](https://github.com/ydixken/pgcopydb/actions/runs/35150664335), created `2026-09-16T21:08:16Z`: all 86 jobs completed successfully.

At `2026-09-16T21:21:10Z`, both workflows had completed successfully on attempt 1 at the exact published head.
The paginated jobs API returned exactly 40 and 86 distinct jobs, all successful, and successful execution of all 35 and 82 matrix `Run a test` steps.
No skipped, cancelled, neutral, missing, or failed job counted as a pass.
The final PR body records these results and the reviewed source and complete-tree identities.

### Missing-sentinel merge evidence

The immediate pre-merge check at `2026-09-16T21:22:40Z` reconfirmed all 126 successful jobs and all 117 successful matrix test steps on attempt 1.
GitHub's comparison showed exactly one reviewed commit ahead of base `4873c1810b73086473903110d9057a1bde37195a`, zero behind, and the same 14 paths.
The PR head, local head, full tree, and production source tree matched the published review identities above.
GitHub returned `mergeable=true`, `mergeable_state=clean`, `allow_merge_commit=true`, and `delete_branch_on_merge=false`.

The authorized merge used `gh pr merge 11 --repo ydixken/pgcopydb --match-head-commit 5d10b14de662540a9b2a27030fe264a4e8e7116d --merge`, without branch deletion or an administrative override.
[PR #11](https://github.com/ydixken/pgcopydb/pull/11) merged at `2026-09-16T21:22:51Z` as [`ea2dc96a47c2f7676d71a4967d044a1e469e4110`](https://github.com/ydixken/pgcopydb/commit/ea2dc96a47c2f7676d71a4967d044a1e469e4110).
Its parents are the verified base and tested feature head, in that order.
Fetched `origin/v0.18-fixes` resolves to that merge; `git diff --exit-code HEAD origin/v0.18-fixes` passed.
The entire merged tree equals CI-tested tree `e8dfb7af0afdad2844e01a96541bfc425c5cbd0e`, including production `src` tree `ec71e1fb0a08eef74b92ff705b50ade66a11c56b`.

Tag `v0.18` peels to `95ebd553790fa45de67c92b934917d777131bdd3`.
`git describe --match v0.18 ea2dc96a47c2f7676d71a4967d044a1e469e4110` returns `v0.18-15-gea2dc96`, deriving merged source version `0.18.15.gea2dc96`.
This is source provenance, not a rebuilt or published builder image.

Post-merge remote-ref checks prove both feature branches remain:

```text
b20522547688021b65d176b97677415d2ae89076 refs/heads/fix/keepalive-feedback
5d10b14de662540a9b2a27030fe264a4e8e7116d refs/heads/fix/resume-sentinel-bootstrap
```

All nine preexisting non-base remote branches retain their preflight SHAs, including every prior feature branch; `delete_branch_on_merge` remains `false`.
Fork issue #10 remains open for upstream tracking, and the fork worktree has no tracked changes.
Untracked fork `tasks/lessons.md` and ignored environment/log artifacts remain outside the commit.
The fork-delivery phase changed only this task file in the public operator worktree and left it uncommitted.
The following integration uses the actual merged SHA and version above.

## RC3 builder and pin integration

Base: `9413a3ca6820ca47b6f22091d23d4656dc78904f`; branch: `fix/rc3-e2e-regressions`.
The user authorized one unsigned builder-preparation commit and feature-branch push, followed by builder publication and an uncommitted final integration for independent review.
No PR, operator merge, or candidate release is authorized in this step.
Keep the next candidate's `E2E_SCALE=0.1`, the existing RC3 changes, both fork feature branches, and user-owned release assessments intact.

- [x] Refresh `origin/main` and confirm the base, existing diff, recent history, fork PR #11 merge tree, and retained fork refs.
- [x] Change only the builder Dockerfile's source SHA, version, and provenance; run scoped cleanup and `task lint` before the unsigned commit and non-force push.
- [x] Dispatch `pgcopydb-builder.yml --ref fix/rc3-e2e-regressions` and require both architecture builds and the index merge to pass on the exact commit.
- [x] Verify anonymous public registry access, the raw index hash, exactly two runnable platforms, and each child manifest and architecture config.
- [x] Pin the runner to the published index, update the runner and release assertions, and retain all four versions in CLI/chart defaults and regression fixtures.
- [x] Update bundled-source links and bootstrap recovery documentation without relabeling historical measurements or changing API fields, dependencies, or generated artifacts.
- [x] Format touched Go files, review only the integration hunks, and run final `task lint`.
- [x] Obtain independent review before the final integration commit and PR, including acceptance of the corrected CI diagnostic selector contract.
- [ ] Require exact-head CI `lint`, `test`, and `docs` before any later merge.
- [ ] Require the full next-candidate E2E suite at `E2E_SCALE=0.1` and cleanup on its exact merged SHA before claiming RC3 success.

### Builder evidence

Unsigned commit [`7ad579fcb7dde2da37d648e0f6e8ea4f02167e74`](https://github.com/ydixken/pgcopydb-operator/commit/7ad579fcb7dde2da37d648e0f6e8ea4f02167e74), `chore: prepare bootstrap recovery pgcopydb builder`, changes only `images/pgcopydb-builder/Dockerfile`.
Source is fork merge `ea2dc96a47c2f7676d71a4967d044a1e469e4110`, version `0.18.15.gea2dc96` (`v0.18-15-gea2dc96`).
[Builder run `35152785053`](https://github.com/ydixken/pgcopydb-operator/actions/runs/35152785053) completed successfully on that exact operator commit, with `workflow_dispatch`, attempt 1.
The `pin`, `amd64`, `arm64`, and `merge` jobs all passed, including the existing build-time version checks and merged-platform assertion.

Verified public builder reference:

```text
ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:ea2dc96a47c2f7676d71a4967d044a1e469e4110@sha256:1145d382fc74bb35c1b8a19a42ed9469639b66d405b560ad9777a7111c9d3b33
```

Anonymous registry reads matched SHA-256 of the raw index bytes to the registry digest header and the pin above.
The only runnable platforms are `linux/amd64` and `linux/arm64`; each child manifest's raw hash, declared size, and config platform matched its descriptor:

- `linux/amd64`: `sha256:82d1cd44335dea8ec0c27daba962627d31be4e2ed1fb4c61da0a49808fd83eed`.
- `linux/arm64`: `sha256:c9dbf94158e56c417aeb621a5905a06442db86dd085ee5c908fc5063c92a45e6`.

The two remaining entries are verified `unknown/unknown` attestation manifests linked to those children, not additional runnable platforms:

- amd64 attestation: `sha256:caae4a4377ed138eaacf3a90e147c51e72c699ad5fc966378cd89fceccd33b36`.
- arm64 attestation: `sha256:5356b8916ebbbe5c5132918ded375d1dee71bdaf4d24e9c94803723397c6d933`.

### Scope and verification boundaries

The operator-side [#274](https://github.com/ydixken/pgcopydb-operator/issues/274) fixes preserve publications, restore pooled SQL settings on rollback, and use named dollar quoting to survive kubelet expansion.
Fork [PR #11](https://github.com/ydixken/pgcopydb/pull/11) separately repairs missing-sentinel bootstrap after fresh slot creation, preserves existing sentinel fields, and refuses missing retained-slot recovery state.
Neither fix claims recovery from interrupted index builds, eviction damage, or established-stream CDC-file loss.
The default plugin and opt-in `wal2jsonNumericAsString` behavior are unchanged.

The integration handoff records a focused pooling PASS at scale `0.1`: one of 50 specs selected, with deferred background Pooler cleanup completing.
This is focused diagnostic evidence, not a full RC3 pass and not a cluster test of the new runner.
External E2E RBAC was verified separately; see private ops notes.
No private repository URL, cluster identity, context, or topology belongs in the public evidence.

The pre-commit `task lint` passed with `0 issues.` and no generated CRD/RBAC or chart drift.
It reported `lint: no actionlint, skipping workflow lint`.
Go functional tests, runner builds, documentation builds, and the E2E diagnostic helper regressions remain CI-owned; none ran locally in this integration.

Final integration lint passed after wrapping the longer CLI default to satisfy the 120-character limit; no lint configuration or dependency changed.
YAML, generated-manifest drift, chart synchronization, chart RBAC/monitoring, documentation-link checks, and Prometheus rule checks/tests passed; Go lint reported `0 issues.`.
Actionlint was unavailable and skipped by the existing local target.
Formatting and `git diff --check` passed; API types, generated CRD/RBAC/chart artifacts, and dependency manifests are unchanged.
The integration phase handed off the runner pin and remaining RC3 files for independent review after pushing only the builder-preparation commit.

## RC3 publication gates

The user approved the reviewed batch for an unsigned commit, feature-branch push, and PR to `main`.
This authorization supersedes the builder-only publication restriction above.
Merge and RC3 creation wait for the next verifier's full gate review; stable promotion is outside this handoff.
Record remote publication and exact-head results in a maintained PR comment rather than follow-up bookkeeping commits.

- [x] Inspect status, the complete tracked and untracked diff, the last ten commits, remote tracking, and all branch commits against freshly fetched `origin/main`.
  The base remains `9413a3ca6820ca47b6f22091d23d4656dc78904f`; the only existing branch commit is builder preparation `7ad579fcb7dde2da37d648e0f6e8ea4f02167e74`.
- [x] Confirm the approved CI selector includes all six `TestPublicationRetry*` helpers and all three `TestCutoverDiagnostic*` helpers, with per-declaration contract coverage and rejection of other test families.
- [x] Review the public diff for private infrastructure details and keep the external runner's Pooler permissions separate from manager RBAC; see private ops notes.
- [x] Complete final formatting, whitespace, and local `task lint` checks.
- [ ] Commit only the 27 intended pending public paths with `--no-gpg-sign`, then push `fix/rc3-e2e-regressions` without force and create the five-section PR.
- [ ] Dispatch `runner-smoke.yml` on the final feature head for `linux/amd64,linux/arm64` with the workflow's `push: false` setting.
- [ ] Hand the exact head, PR, CI `lint`/`test`/`docs` and Codecov observations, and smoke URL to the next verifier.
- [ ] Require the verifier's full gate approval before merging or cutting RC3.
- [ ] Require the full RC3 suite at `E2E_SCALE=0.1` and successful cleanup before claiming candidate verification or closing the original issues.

The RC2 job log records:

```text
Ran 43 of 50 Specs in 4597.834 seconds
FAIL! -- 40 Passed | 3 Failed | 0 Pending | 7 Skipped
```

The recorded focused Pooler run at scale `0.1` used candidate RC2 runtime images with the revised test code and completed deferred cleanup:

```text
Ran 1 of 50 Specs in 246.168 seconds
SUCCESS! -- 1 Passed | 0 Failed | 0 Pending | 49 Skipped
PASS
```

This proves the focused pooling fixture, including same-backend restoration and long SQL assertions, rather than the new runner or the full RC3 suite.
The fork's paired bootstrap failure and success are published in [fork issue #10](https://github.com/ydixken/pgcopydb/issues/10).

### Publication preflight review

Final `gofmt -l` returned no paths, `git diff --check` exited 0, and `task lint` exited 0 with `0 issues.`.
YAML, generated-manifest drift, chart synchronization, RBAC/monitoring, documentation-link, and Prometheus checks passed.
The target reported `lint: no actionlint, skipping workflow lint`; CI owns that check.
No local Go tests, image builds, documentation builds, or cluster operations ran in this publication phase.
Functional tests, documentation validation, the final-head runner smoke build, and full RC3 E2E remain remote gates.

## Stable v0.13.3: allDatabases E2E follow-up

- [x] Create isolated branch `fix/all-databases-e2e-failure` from freshly fetched `origin/main`.
  Local `main`, `origin/main`, and stable tag `v0.13.3` resolve to `d6a32c4e665dbdd5408caf585204ba97495642c8`.
- [x] Record the user-verified release handoff: stable `v0.13.3` is published and its artifacts are verified.
  Publication and artifact checks were not repeated during workspace preparation.
- [x] Record the user-reported stable `allDatabases` E2E failure in [run 35165037653](https://github.com/ydixken/pgcopydb-operator/actions/runs/35165037653), [job 105024114133](https://github.com/ydixken/pgcopydb-operator/actions/runs/35165037653/job/105024114133): timestamp `00:35:06Z`, attempt `4`, with `lastError` reporting a cleanup queue `Invalid argument` error.

> [!important]
> The focused stable-image diagnostic confirms that Pooler cleanup leaves `public.user_search(text)` in both shared `postgres` databases, causing AllDatabases to fail during pre-data restore.
> The cleanup queue error follows the duplicate-function failure.
> A separate baseline diagnostic confirms both keepalive failures after a full clone leaves target dependencies excluded from the next filtered restore.
> Manual passed in the first ordered diagnostic, which did not exercise that preceding full-clone setup.

- [x] Collect the actual AllDatabases worker errors preceding cleanup, including earlier attempts, and identify the first causal failure.
  Keep private infrastructure details in private ops notes; include only sanitized evidence in public records.
- [x] Reproduce the failure on the published baseline, confirm the root cause, then fix it with paired baseline-fail and focused fixed-pass evidence.
- [ ] Add regression coverage and affected documentation, obtain independent review, run local `task lint`, and require exact-head CI `lint`, `test`, and `docs` success.
- [ ] Verify the affected scenario and the full suite through the appropriate new release-candidate or authorized harness proof route, recording exact source and artifact identities, run evidence, and cleanup results.
  Focused harness success MUST NOT substitute for the full release-candidate suite; published tag `v0.13.3` MUST NOT be rewritten.

### Ordered stable-image diagnostic

- [x] Verify the stable source and original suite seed without changing cluster resources.
  Run `35165037653` used seed `1789603652` and ran Keepalive Manual before Pooling, then AllDatabases.
  Native dry-runs preserve that relative order when the suite is filtered.
  With the same seed, `randomize-all` puts Pooling before Manual before AllDatabases.
  Sequential seed-only dry-runs found `1789603663` gives Pooling, positive AllDatabases, then Keepalive Manual.
- [x] Validate the approved context privately and record fixture inventories plus namespace and preexisting Secret metadata UIDs.
  Test resources were absent before the diagnostic, and all three namespace UIDs plus twelve preexisting Secret UIDs were recorded privately.
- [x] Run one guarded focus with stable manager and runner `v0.13.3`, scale `0.25`, `KEEP=false`, and `MANAGE_NS=false`.
  Temporary diagnostic hooks may read function identity and owner in `postgres` and `app`, and bounded sanitized worker logs before cleanup.
  They MUST NOT read function bodies, credentials, or live SQLite files, or remove the managed auth function.
- [x] Verify cleanup and preservation, save the diagnostic-only patch privately, and restore only the temporary hunks.
- [x] Record the first worker errors and limits of the evidence without claiming a production fix or a release gate result.

#### Diagnostic result

The run selected three of fifty specs and finished with two passed, one failed, and forty-seven skipped.
Pooling passed, AllDatabases failed, and Keepalive Manual passed in that order.
The runner identified itself as `0.18.15.gea2dc96`.
The manager image ID was `sha256:06096c6ec3572a83e0bbfcc108a3af6acd7fecedc966031c22b14f56df66ef0a`.
The runner image ID was `sha256:51a94c4cdd6454ceeba3b4b050e28d623a565fdf5efe6bbc9de76e10ce31b4f4`.

Before Pooling, `public.user_search(text)` was absent from `postgres` and `app` on both clusters.
While Poolers existed and after both were deleted, the function existed in both `postgres` databases with owner `postgres`.
It remained absent from both `app` databases throughout the diagnostic.

Every AllDatabases attempt first reported `pg_restore: error: could not execute query: ERROR: function "user_search" already exists with same argument types`.
Each then reported failure to restore pre-data for database `postgres`, worker exit `12`, and Job failure with `BackoffLimitExceeded`.
The message-queue `Invalid argument` error followed those failures.
All four complete worker streams were below the prefix limit and were captured before fixture cleanup, with a separate bounded tail.

Keepalive Manual reached `Completed` in one attempt and its worker exited `0`.
Its complete captured worker stream contained no ERROR lines.
The original stable seed ran Manual and Automatic before Pooling, so this Pooling case cannot explain those earlier failures within that run.

Cleanup left zero Migrations, CNPG Clusters, Poolers, Jobs, pods, PVCs, Deployments, ReplicaSets, or Services in the three fixture namespaces.
The Helm release was absent, and all three namespace UIDs, twelve preexisting Secret metadata UIDs, and three baseline ConfigMap metadata UIDs were preserved exactly.
Private evidence and the restored diagnostic patch are in private ops notes outside git.
The diagnostic handoff changed only this task note.
Local `task lint` exited successfully with zero Go issues; its actionlint check reported that the tool was unavailable and skipped workflow lint.

### Pooling fixture isolation

- [x] Trace the pooling spec, CNPG constructor, readiness and PVC helpers, and existing capacity budget from stable source `d6a32c4e665dbdd5408caf585204ba97495642c8`.
- [x] Move both Poolers and every pooling probe onto a dedicated ephemeral CNPG pair, preserving the PostgreSQL version matrix and functional assertions.
- [x] Register each cluster's cleanup immediately after creation, delete only UID-owned resources, and wait for its pods and PVCs to disappear after dependent cleanup.
- [x] Assert that Poolers reference the new cluster UIDs and that shared maintenance schema/function metadata stays unchanged during pooling and after cleanup.
- [x] Account for two fixed 1Gi auxiliary PVCs in the existing capacity budget and update affected test documentation.
- [x] Review the scoped diff, format touched Go files, and run local `task lint`.
- [x] Obtain focused fixed-pass proof, including isolation and cleanup, after the separate keepalive baseline run.
- [ ] Complete final independent review, exact-head CI, and full E2E verification after review corrections.

Keep production behavior, chart sources, runner pins, and published release tags unchanged.
Do not remove CNPG-managed authentication objects from shared databases.
The initial scope withheld keepalive/publication changes pending diagnosis; the separate baseline evidence below supports their one-time target resets.
The earlier stable failures remain the baseline proof; focused fixed-pass evidence does not close the full-suite gate.
Regression assertions live in the existing `Progress sampler transaction pooling` Ginkgo spec; no standalone Go test family or CI selector is added.
The existing non-cluster CI selector remains limited to `TestCutoverDiagnostic*` and `TestPublicationRetry*`.
The focused verifier passed the new assertions, small-volume startup, and cleanup paths; full E2E and release-candidate coverage remain separate gates.

#### Static review

`gofmt -l` returned no paths and `git diff --check` exited 0.
`task lint` exited 0 with `0 issues.` from Go lint; YAML, generated manifests, chart synchronization and rendering, documentation links, and Prometheus checks passed.
It reported `lint: no actionlint, skipping workflow lint`.
The diff against stable source is empty for `api`, `cmd`, `internal`, `charts`, `config`, `images`, `.github`, `go.mod`, `go.sum`, `Dockerfile`, and `Makefile`.
This implementation phase ran no local Go tests, builds, or cluster operations and made no commits or remote changes.
The seven fixture-fix files are the pooling, keepalive, and publication-retry specs, the suite helper file, `CONTRIBUTING.md`, `docs/operations/monitoring.md`, and this task record.
The review lesson is recorded separately in `tasks/lessons.md`.

### Separate keepalive baseline

The separate diagnostic used stable source `d6a32c4e665dbdd5408caf585204ba97495642c8`, published manager and runner `v0.13.3`, seed `1789603652`, and scale `0.25`, with pooling excluded.
Progress sampler bounds passed first; Keepalive Manual and Automatic then failed during their initial clones, before reaching feedback assertions.
Each keepalive case exhausted four worker attempts.
All eight workers first reported that `public.e2e_touch_updated_at()` could not be dropped because trigger `app_users_touch_updated` on `public.app_users` depended on it; PostgreSQL recorded SQLSTATE `2BP01`.
The prior full clone left that table on the target, but the keepalive filter excluded it from restore cleanup.
Other excluded-table dependencies blocked type and extension cleanup; duplicate-definition errors followed, and the stable log's `END $$;` suffix did not identify the first cause.

```text
Ran 3 of 50 Specs in 612.141 seconds
FAIL! -- 1 Passed | 2 Failed | 0 Pending | 47 Skipped
```

Each keepalive/publication fixture now calls `resetTargetObjects()` once after the zero-slot and zero-origin checks, before its filtered clone starts.
There is no reset between publication attempts or suspend/resume phases.
The publication reset was preventive because its setup has the same filtered-clone precondition; those cases were not part of this baseline diagnostic.
Both publication cases subsequently passed the fixed functional verification below.

### Fixed focused verification

Both groups ran sequentially at scale `0.25` with the same published manager and runner `v0.13.3` and runtime digests recorded above.
Group A used seed `1789603663`: Pooling, AllDatabases, then Keepalive Manual all passed.

```text
Ran 3 of 50 Specs in 678.141 seconds
SUCCESS! -- 3 Passed | 0 Failed | 0 Pending | 47 Skipped
```

Group B used seed `1789603652`: Progress sampler bounds, Keepalive Manual, Keepalive Automatic, established-publication suspend/resume, then orphan-publication repair all passed.

```text
Ran 5 of 50 Specs in 1016.847 seconds
SUCCESS! -- 5 Passed | 0 Failed | 0 Pending | 45 Skipped
```

The eight successful spec executions include Manual in both groups.
Both auxiliary CNPG clusters became ready with one instance and one Bound 1Gi PVC each; pods and PVCs had the expected cluster controller UIDs.
Pooler ownership chains, same-backend restoration, cancellation, six-second COPY/index operations, unchanged shared maintenance catalogs, and deferred cleanup all passed.
AllDatabases, bounds, and keepalive passed their single-worker-attempt assertions.
Both publication cases passed their attempt-2 and resume-argument assertions, including retained publication/work-PVC identity and marker rows for suspend/resume, plus the deliberate duplicate-publication failure and repair for the orphan case.
Independent inventories found all main and auxiliary test resources absent after each run, with namespace and preexisting Secret/ConfigMap metadata UIDs preserved.
Detailed evidence is in private ops notes outside git.

Review closure narrows only the pooling readiness pod selector to CNPG instances and gives its cleanup context one minute beyond the unchanged ten-minute wait budget.
Cleanup still checks all cluster-labelled pods and retains its UID-checked PVC deletion fallback.
After these review edits, `gofmt -l` returned no paths, `git diff --check` passed, and `task lint` exited 0 with `0 issues.`; actionlint was unavailable and skipped.
The focused runs preceded these review edits; exact-head CI and full manual E2E remain pending, with no additional focused run in this closure pass.
The focused results do not establish a full stable-suite pass or authorize changing published tags.

### Test-only publication and published-image verification

The user approved independent review closure and authorized unsigned publication, gated merge, and one full `e2e.yml` dispatch from merged `main` with `tag=v0.13.3` and `scale=0.25`.
This uses the reviewed test harness with the existing release artifacts; no new release, retag, or image build is authorized.
Record remote gate and dispatch results in the PR rather than follow-up bookkeeping commits.

- [x] Inspect the eight intended paths, all branch commits, recent history, remote tracking, and fresh `origin/main`; the base is `d6a32c4e665dbdd5408caf585204ba97495642c8`.
- [x] Confirm review closure, paired baseline/fixed proof, unchanged production sources, and preserved fork feature refs.
- [x] Complete final local formatting, whitespace, and `task lint` checks with `0 issues.`; the existing target reports unavailable actionlint and leaves workflow validation to CI.
- [x] Verify the published tag object `4df87d225b1f03f454025f6aef9b821b4b07f415`, both recorded image digests, and chart OCI digest `sha256:d1b17eb243b45887a47e7db6eaa1ae92b1cd6d58f87d34fc4ba7e5d0fea9d893` before publication.
- [ ] Commit only the eight intended files unsigned, push the feature branch normally, and open the test-only PR.
- [ ] Require exact-head `lint`, `test`, `docs`, and any Codecov checks; recheck the reviewed diff and base before merging with the matched head.
- [ ] Verify merge-tree equality, post-merge CI, documentation deployment, mirror success, and unchanged release source and artifact identities.
- [ ] Dispatch `e2e.yml` once from merged `main` against published `v0.13.3` at scale `0.25`, then verify checkout, image availability, prerequisites, and suite startup.
- [ ] Follow the full E2E result and cleanup separately; startup alone does not close the stable-suite gate.

## Stable v0.13.3: continuous-writer failure follow-up

### Baseline and latest full-run evidence

[PR #281](https://github.com/ydixken/pgcopydb-operator/pull/281) merged the fixture fixes as `b12c254fc5b03bc0c4de3e2c9dd4d3831bb2e64c`.
The isolated follow-up branch is `fix/e2e-live-writer-failure`, based on that freshly fetched `origin/main` commit.
[Full E2E run 35180589925](https://github.com/ydixken/pgcopydb-operator/actions/runs/35180589925) completed with conclusion `failure` using the `main` harness at `b12c254fc5b03bc0c4de3e2c9dd4d3831bb2e64c` against published `v0.13.3` artifacts at scale `0.25`.
The release source remains `d6a32c4e665dbdd5408caf585204ba97495642c8`; its annotated tag object is `4df87d225b1f03f454025f6aef9b821b4b07f415`.
Keep the harness revision separate from the immutable release source and artifact identities.

The run handoff records `FAILED`: 32 passed, 1 failed, and 17 skipped, including 10 ordered-dependent skips.
All previously fixed fixture scenarios passed.
The log records writer start at `04:57:34` and stop/failure at `04:59:12`, with `EPIPE` at attempted marker `199` and last reported marker `0`.
The attempted marker describes submitted SQL, not committed rows; the zero result is ambiguous because final-query validity was not recorded.
Source forensics recorded zero pod restarts and zero sampled OOM/cgroup OOM counters in the inspected window, with no source Kubernetes events.
Server logs covering the failure were not retained, and the writer discarded child stderr, so the evidence cannot distinguish SQL/session failure from exec transport failure.
The traced writer uses `WithCancel` without an active-writing deadline; its 30-second timeout bounds shutdown and the final query only.
The cause remains **UNCONFIRMED**.
These results supersede the pending full-run outcome in the preceding publication checklist and do not establish a full-suite pass.

### Follow-up checklist

- [x] Verify the destination parent and absent target path/branch, then create the isolated worktree from freshly fetched `origin/main` at the expected merge commit.
- [x] Read `AGENTS.md`, `tasks/lessons.md`, and ponytail at full intensity; record the latest completed full run and separate harness/release provenance.
- [x] Review the failed run's existing evidence and source forensics, and trace the writer lifecycle and helper callers.
  The evidence is insufficient to identify the cause; `EPIPE` remains a symptom.
- [x] Implement bounded, sanitized failure-only diagnostics and regression coverage while preserving the first error, one child, captured primary, SQL, pace, and shutdown/query order.
  Record failure-time timestamps, submitted counters, child exit/signal, stderr, and independent final-query validity without private identities or raw command errors.
- [x] Configure all three helper families in the no-cluster CI step with `-race -v` and extend the per-source-test selector contract.
- [x] Update the diagnostic documentation, format touched files, and run initial local `task lint`: exit 0, `0 issues.`.
  Workflow lint skipped because actionlint was unavailable; CI retains that gate.
- [x] Apply review follow-ups for atomic final-query snapshots, exact CI flags, and known DNS/transport redaction with synthetic regressions.
- [x] Format the four touched Go files and run `task lint` after the review follow-ups: exit 0, `0 issues.`.
  YAML, generated-manifest/chart drift, Helm, documentation-link, Prometheus, and Go lint checks passed; workflow lint again skipped because actionlint was unavailable.
- [ ] Verify exact-head functional/race tests, selector/flag contracts, and documentation checks in PR CI before merge or use of public runtime diagnostics.
  Publish a draft or normal PR only in the separately authorized delivery phase; record run URLs, actual test selection, and results.
  The runner image's GCC/cgo prerequisites were verified, and helper children already use `GORACE=atexit_sleep_ms=0`.
  Remaining race-instrumented runtime and timing cost is a CI gate, not a reason to add a local functional gate.
- [ ] Run the capture-removal mutation in disposable CI, require a behavioral failure rather than a compile failure, restore capture, and require the same regressions to pass.
  See [Live-writer diagnostic verification](live-writer-diagnostics.md) for the procedure.
- [ ] Identify the writer's failure cause from retained evidence before choosing a root fix.
- [ ] Establish a meaningful reproduction that fails on the affected baseline, then implement the smallest root-cause fix with regression coverage that passes on the fixed revision.
  Preserve writer-error propagation and all data-loss, marker, cutover, and cleanup assertions; MUST NOT hide the failure with retries, skips, or weakened assertions.
- [ ] After separate authorization and exact-head CI, use focused E2E if full-run evidence still cannot resolve the assertion.
  Preserve context confirmation, bounded execution, and owned-resource cleanup; see private ops notes for environment details.
  A focused pass is diagnostic evidence and MUST NOT replace the full suite.
- [ ] After the follow-up is delivered under separate authorization, run the full native `e2e.yml` workflow from `main` with `tag=v0.13.3` and `scale=0.25`.
  Keep the native suite selection and assertions, record the exact harness SHA, and verify the existing release tag and image/chart digests remain unchanged.
  Do not rebuild, retag, or replace the published artifacts.
- [ ] Record the full suite result, ordered-dependent outcomes, cleanup result, run URL, and immutable artifact identities before closing the follow-up.

### Implementation and review scope

The implementation phase covered the scoped test-harness diagnostics, regression sources, CI contract, and documentation updates.
The publication phase below supersedes its commit and push restriction.
Local verification is limited to formatting and `task lint`; functional and race evidence comes from exact-head PR CI.
Preserve the root worktree's user task note and untracked release assessments unread, and retain all existing worktrees and fork branches.
Secret values MUST NOT be read, printed, or changed; private contexts and GitOps paths MUST NOT enter public notes.

### Diagnostics-only publication

The user authorized one unsigned commit, a normal branch push, a PR against `main`, and exact-head CI verification.
Merge, cluster access, focused or full E2E, release creation, and stable-tag mutation require a separate delivery-leader check.
Record the PR URL, commit SHA, CI URLs, test selection, and timings in the PR rather than a follow-up bookkeeping commit.

- [x] Inspect status, all ten intended files including the new verification note, recent history, upstream tracking, and the complete base diff.
  Fetched `origin/main` and the branch both resolve to `b12c254fc5b03bc0c4de3e2c9dd4d3831bb2e64c`, with no earlier branch commits to publish.
- [x] Verify all five touched Go files are gofmt-stable, `git diff --check` passes, and final local `task lint` exits 0 with `0 issues.` in 10.454 seconds.
  Local actionlint is unavailable; CI owns workflow syntax verification.
- [ ] Stage only the ten reviewed paths, commit with `--no-gpg-sign`, push without force, and open the five-section diagnostics-only PR.
- [ ] Verify `lint`, `test`, and `docs` on the exact published head, plus Codecov when present.
  Record helper-step build/startup time, every meaningful `TestLiveWriter` parent result, stderr flood, atomic-query and concurrency coverage, and the parent-only subprocess-helper skip.
  Stop on any failure with its exact head and logs; report a bounded checkpoint if CI exceeds 20 minutes.
