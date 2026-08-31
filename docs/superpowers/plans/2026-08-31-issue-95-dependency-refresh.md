# Issue 95 dependency refresh implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the repository automation, Go toolchain, dependencies, and pinned container bases, fix the release promotion gate, and verify the complete two-pull-request release path through v0.12.0-rc.1 and v0.12.0.

**Architecture:** The first pull request carries every static dependency, workflow, test, and public image update while retaining the internal builder digest that exists before publication.
After that pull request merges and publishes a verified multi-platform builder image, a digest-only second pull request pins the resulting index.
The final `main` commit then runs the metadata, auto-release, candidate, promotion, stable release, mirror, and protected e2e paths.

**Tech Stack:** Go 1.27.0, controller-runtime v0.24.1, Kubernetes v0.36.4 modules, Ginkgo and Gomega, GitHub Actions, GitHub Dependency Review, Docker Buildx, OCI images, Task, Helm, ORAS, kubectl, yq, crane v0.20.6, and GitHub CLI.

**Spec:** `docs/superpowers/specs/2026-08-31-issue-95-dependency-refresh-design.md`

## Global Constraints

### Pull request and workflow scope

- The first pull request uses branch `fix/issue-95-maintenance`.
- It upgrades all 51 existing `uses:` references across the nine files in `.github/workflows/` and adds `actions/dependency-review-action@v4` once.
- The completed first pull request therefore contains 52 action references across the same nine workflow files.
- Repeated existing references receive the same target major in every workflow.
- Inputs, outputs, permissions, event filters, concurrency controls, and environment guards remain unchanged except for the approved Dependency Review step and promotion dependency.
- If a new major removed a field, stop that Action's upgrade and review its public release notes before changing workflow behavior.
- The self-hosted runners must be Actions Runner v2.327.1 or newer because the new action majors use Node 24.

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

### Go, tools, and module graph

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

- Go 1.27.0 must be an official release listed in the [Go release history](https://go.dev/doc/devel/release) before the image and module directive move.
- Go tooling may refresh indirect modules that the exact direct targets and the approved patch-only test graph require.
- We will accept resolver-selected indirect patch or pseudo-version changes from `go get -u=patch -t ./...` and `go mod tidy` only when the module graph remains within the limits below and all dependency, lint, and test gates pass.
- We will not add a direct Go dependency or run an unconstrained minor or major upgrade.
- All four direct Kubernetes modules therefore move together to v0.36.4 and stop there.
- Kubernetes v0.37 and controller-runtime v0.25 are outside this change.
- Indirect Kubernetes modules must remain on the v0.36 line unless the existing controller-runtime graph selects an older compatible v0.36 patch.
- The Go baseline is exactly 1.27.0.
- The change does not adopt a later Go 1.27 patch, Go 1.28, or a development toolchain without another review.

### Exact container targets

The public multi-platform manifest targets resolved for this plan are:

- `golang:1.27.0@sha256:4013ae0f9e7994f8535c58c811f8f863fbed38b72e0d51e6592156f758d66146`.
- `gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7`.
- `debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132`.

- The container refresh covers public base images used by the manager, runner, and pgcopydb builder Dockerfiles.
- Each public image keeps its intended tag and receives the current multi-platform manifest digest for that tag.
- The Go image tag is the one intentional exception because it moves to 1.27.0.
- The refresh includes the Go builder, Distroless static nonroot, and Debian trixie-slim images.
- The internal `pgcopydb-builder` digest in `images/runner/Dockerfile` stays unchanged in this pull request for the publication reason described below.
- The first pull request must retain `ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:e37d2bd4dd10b7ed7b415555ce3318202d9633cf@sha256:0b3a2afef9d6cc156aa59713bbb8ab7bd97597b978b7938c0e804ba26c044a59`.
- If any public tag no longer resolves to the exact index listed above before editing starts, stop and reconcile the target with the approved spec instead of silently selecting another digest.

### Dependency gates

- Before opening the first pull request, enable Dependency Graph through the repository Settings UI.
- Do not use an undocumented or unsupported REST mutation to change this setting.
- The command must exit 0 and print a non-empty creation timestamp.
- An absent timestamp, 404 response, or permission error is a failed prerequisite.
- The step reuses the lint job's checkout and the workflow's existing `contents: read` permission.
- It must not run on the `main` push event because dependency review requires a pull request comparison.
- Do not add retries or broader permissions.
- The action must fail the required lint job for a new dependency at moderate or higher severity, a disallowed resolved license, or a violation in any configured scope.
- Review every unresolved license manually and treat an unknown or unacceptable license as a merge blocker.
- Record the public source used to resolve it without copying dependency source text into the repository.
- Use the default non-JSON output.
- The command must exit 0 and report no reachable known vulnerability.
- Do not add `golang.org/x/vuln` to `go.mod` or install an unpinned latest tool.

Use this exact Dependency Review step immediately after checkout in the `lint` job:

```yaml
- name: Review dependency changes
  if: github.event_name == 'pull_request'
  uses: actions/dependency-review-action@v4
  with:
    fail-on-severity: moderate
    fail-on-scopes: runtime, development, unknown
    allow-licenses: Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, MIT
```

Run this exact vulnerability command:

```sh
go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
```

### Release promotion

- Change the promotion dependency in `.github/workflows/release.yml` to:

```yaml
needs: [e2e, release-notes]
```

- Keep `e2e` and `release-notes` parallel, with each depending on `chart` rather than on the other.
- Keep `promote` free of a job-level `if` expression.
- An `always()`, `success()`, or result-based job condition would override that default and is not allowed.
- Extend the existing parsed workflow model in `test/buildconfig/buildconfig_test.go` with `Needs` and `If` fields.
- Add one focused regression test that fails if `promote` is absent, if its prerequisites are not exactly `e2e` and `release-notes`, or if its parsed job-level `If` field is non-empty.
- The test must parse `.github/workflows/release.yml` through the existing `sigs.k8s.io/yaml` path rather than compare source substrings.

### E2e and release targets

- The local e2e suite changes its default operator tag from v0.5.0 to the current stable release, v0.11.3.
- The environment override remains the supported way to test another published version.
- The related test and comment must change with the default so the file remains true.
- Local `task e2e` is not part of the unattended branch gate because it targets the developer's current Kubernetes context and requires a human to answer its confirmation prompt.
- No agent may bypass that prompt with `task --yes`.
- The authorized real-cluster checks run through the protected workflows.
- The last stable tag is v0.11.3, and the range after it contains a `feat:` commit.
- The approved auto-release dispatch on `main` is therefore expected to create v0.12.0-rc.1.
- If that tag already exists or the computed version differs, stop before publishing and reconcile the public tag history with `auto-release.yml`.

### Two-pull-request sequence

- The first pull request must pass its local gates, required PR jobs, branch runner smoke run, and feature-branch e2e rejection before merge.
- After the first merge, require successful `main` runs for `ci.yml`, `docs.yml`, `mirror-to-gitlab.yml`, and `pgcopydb-builder.yml`.
- Dispatch `e2e.yml` from `main` with tag v0.11.3 and scale 0.1, and require the suite and cleanup step to pass before starting the second pull request.
- After `pgcopydb-builder.yml` succeeds on `main`, read the public manifest from the package registry.
- Verify that its digest is the registry's multi-platform index and that the index contains linux/amd64 and linux/arm64 manifests.
- Create branch `fix/issue-95-builder-digest` from the exact first-merge `main` commit.
- Change only the internal builder digest plus a narrowly required existing test expectation if one stores the digest.
- Do not replace the digest with a floating tag or include unrelated edits.
- The second pull request must pass local lint and tests, the three required CI jobs, Dependency Review, and a branch runner smoke run.
- Merge it only after those checks pass.
- Then confirm `ci.yml` and `mirror-to-gitlab.yml` on the resulting `main` commit.
- Re-read the published manifest and confirm that `images/runner/Dockerfile` pins its exact index digest.
- The second pull request's digest cannot be written into this plan because the first merge creates it.
- The only permitted value is the literal multi-platform index digest published by the successful first-merge `pgcopydb-builder.yml` run.

### Stop and security rules

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
- This public repository must contain no private endpoints, host names, addresses, cluster inventory, runner configuration, or GitOps details.
- The work may name existing secrets and variables but must not read, print, set, or copy their values.
- Workflow permissions must stay least privilege.
- The e2e environment policy, repository checks, tag filters, branch filters, and concurrency controls must not be weakened.
- Public base images remain pinned by digest, and the internal builder remains pinned by tag and digest.
- Release and e2e logs must be reviewed in place, with only public run URLs and non-sensitive conclusions recorded.

---

## File ownership map

- Planning and evidence: `docs/superpowers/plans/2026-08-31-issue-95-dependency-refresh.md` contains this implementation plan, and `tasks/todo.md` carries delegated progress and final evidence.
- Go graph and lint tooling: `go.mod`, `go.sum`, `.custom-gcl.yml`, `Makefile`, and `.golangci.yml` own the exact toolchain, direct module targets, tidy-generated indirect graph, matching golangci-lint pins, and the `embedlit` policy exception.
- Go 1.27 compatibility: `test/e2e/e2e_suite_test.go`, `internal/controller/error_paths_test.go`, and `internal/controller/migration_controller.go` own the required `errors.AsType` updates and narrow rate-limited `Requeue` staticcheck suppressions.
- Public manager image pins: `Dockerfile` owns Go 1.27.0, the Go builder digest, and the Distroless digest.
- Public runner image pins: `images/pgcopydb-builder/Dockerfile` and `images/runner/Dockerfile` own the Debian digest.
- Internal builder pin: only `images/runner/Dockerfile` changes in the second pull request unless an existing exact-digest expectation also requires an update.
- Workflow automation: `.github/workflows/artifacthub-metadata.yml`, `.github/workflows/auto-release.yml`, `.github/workflows/ci.yml`, `.github/workflows/docs.yml`, `.github/workflows/e2e.yml`, `.github/workflows/mirror-to-gitlab.yml`, `.github/workflows/pgcopydb-builder.yml`, `.github/workflows/release.yml`, and `.github/workflows/runner-smoke.yml` own all 52 final action references.
- Dependency policy: `.github/workflows/ci.yml` owns the pull-request-only Dependency Review step.
- Release ordering: `.github/workflows/release.yml` owns the exact `promote.needs` dependency and the absence of a job-level `if`.
- Release regression coverage: `test/buildconfig/buildconfig_test.go` owns parsed checks for the promotion prerequisites and job-level condition.
- Local e2e default: `test/e2e/e2e_suite_test.go` owns the v0.11.3 default and its accurate comment, while `.github/workflows/e2e.yml` owns the matching public dispatch example.
- No README, API, chart, Renovate configuration, schema, or generated scaffold file changes belong in this work.

Workers that share a file must run sequentially.
A worker must not absorb a neighboring concern or edit a file outside this map without escalating to the Team Leader.

## Global execution and team rules

- The Team Leader coordinates, delegates, reviews, and synthesizes.
- The Team Leader must not edit files or execute repository commands directly.
- Delegate each focused plan task to a fresh worker with its exact files, inputs, expected output, and acceptance checks.
- Use a separate verification worker after each implementation worker.
- Run independent research and review in parallel, but serialize edits that touch the same file.
- Every worker must read `AGENTS.md`, the approved spec, the assigned task, and the relevant existing files before acting.
- Every coding worker must invoke ponytail at level `full`.
- Apply humanizer to documentation, comments, commit messages, pull request prose, and evidence notes.
- The approved design satisfies the brainstorming gate.
- Reopen design only if execution discovers a required scope or architecture change.
- Write the promotion regression test first, run it to observe the intended failure, make the smallest workflow and model change, then run it again.
- Mechanical version and digest changes must use exact inventory checks instead of speculative new test frameworks.
- Keep one focused responsibility per commit and use conventional commit prefixes.
- Each commit must be lint-clean on its own.
- A worker must report changed paths, commands run, exit results, non-sensitive findings, risks, and the commit SHA.
- A worker must escalate any cross-task decision, new dependency, workflow redesign, security setting change, protected-environment anomaly, or mismatch with the approved targets.
- Update `tasks/todo.md` through a delegated worker as milestones finish.
- If review rejects a worker's output, re-delegate the correction.
- After two failed attempts on one subtask, stop and re-plan that subtask.
- Before any completion claim, invoke `superpowers:verification-before-completion` and obtain fresh command or workflow evidence.

## Verification evidence format

Record evidence under `## Review` in `tasks/todo.md` with one sentence per line.

For local commands, record the exact command, exit code, and the non-sensitive result.
The required command records are:

- `git status --short --branch`: correct worktree branch and changed-file boundary.
- The three `rg` inventory assertions from the spec: 52 references, nine workflow files, and one Dependency Review reference.
- `gh api repos/ydixken/pgcopydb-operator/dependency-graph/sbom --jq '.sbom.creationInfo.created'`: exit 0 and a non-empty public creation timestamp.
- `git diff --check`: exit 0 and no output.
- `go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...`: exit 0 and no reachable known vulnerability.
- `task lint`: exit 0 with every configured lint gate executed.
- `task test`: exit 0 with every non-e2e Go package, unit test, and envtest gate passing.
- The focused `go test` command for the new promotion regression: failure before implementation and success afterward.
- The module license review: every changed module, its resolved license, and the public source used for any license GitHub did not resolve.
- The module boundary review: no new direct dependency, controller-runtime v0.24.1 retained, and no Kubernetes v0.37 module.

For pull requests, record the public URL, head SHA, base branch, required job conclusions, Dependency Review conclusion, runner smoke URL and conclusion, review result, and merge commit SHA.

For GitHub Actions, record the workflow name, event, ref or tag, commit SHA, public run URL, overall conclusion, relevant job conclusions, and any designed skip or rejection.
The feature-branch e2e record must prove rejection before protected runner assignment.
Each real-cluster record must state that the suite and cleanup step passed without copying cluster details.

For OCI images, record the full public tag, index digest, linux/amd64 presence, linux/arm64 presence, and the workflow run that published it.
Record the builder index before the second pull request and re-read the same public index after the second merge.

For releases, record the auto-release calculation, v0.12.0-rc.1 candidate URL, candidate artifact conclusions, candidate e2e result, release-notes result, promotion start ordering, v0.12.0 stable URL, stable artifact conclusions, stable e2e result, mirror results, and the candidate, stable, and `latest` index digests.

Do not paste secret values, private infrastructure facts, complete logs, or transient runner configuration into the repository.
An absent URL, empty API field, skipped required job, missing cleanup result, or unreviewed warning is a failed gate.

## Plan self-review checklist

- [x] The title, required agentic-worker notice, goal, architecture, tech stack, and spec path match the writing-plans format.
- [x] Every requirement in the approved spec maps to at least one implementation or verification task.
- [x] The plan covers all 51 existing action upgrades, the one new Dependency Review reference, all nine workflow files, and the exact action mapping.
- [x] The plan carries every exact Go, module, tool, release, e2e, and public image target from this frame.
- [x] The first pull request retains the exact existing internal builder pin.
- [x] The second pull request begins only after the first merge publishes a two-platform builder index and merged `main` e2e passes.
- [x] The second pull request changes only the literal published builder index digest and a narrowly required existing expectation.
- [x] The promotion test parses YAML through `sigs.k8s.io/yaml`, requires exactly `e2e` and `release-notes`, and rejects a job-level `if`.
- [x] Candidate `e2e` and `release-notes` remain parallel after `chart`.
- [x] Every behavior task contains a failing test, its observed failure, the minimal change, and the passing rerun.
- [x] Every mechanical task contains exact commands and exact expected matches or counts.
- [x] Every task names exact files and prevents concurrent ownership conflicts.
- [x] Every repository-editing task ends with focused verification, independent review, and a conventional commit.
- [x] Every GitHub gate states its trigger, ref, expected job result, and stop condition.
- [x] The release tasks preserve failed published artifacts and use fix-forward handling.
- [x] The final chain exercises all nine workflows on their intended paths and records public evidence.
- [x] The plan contains no placeholders, unresolved types, unspecified error handling, or references such as "similar to another task."
- [x] All commands run from the isolated worktree and none bypass the local e2e confirmation prompt.
- [x] No step reads or prints secrets, weakens protected environment policy, exposes private infrastructure, or mutates published tags.
- [x] Markdown uses semantic line breaks, contains no em dash or en dash, and follows the repository voice and callout rules.

### Task 1: Enable Dependency Graph and prove SBOM generation

**Files:**

- Modify: none.
- Read: GitHub repository settings.
- Verify: GitHub Dependency Graph SBOM endpoint.

**Interfaces:**

- Consumes: Maintainer access to `ydixken/pgcopydb-operator`.
- Produces: An enabled Dependency Graph and a non-empty SBOM creation timestamp.
- Blocks: Tasks 2 through 6 may run locally, but the first pull request MUST NOT open until this task passes.

**Worker:** Settings worker with repository administration access.

**Independent review gate:** A separate verification worker runs the read-only SBOM query and confirms that it prints one non-empty timestamp.

- [ ] **Step 1: Enable Dependency Graph through GitHub's supported UI**

Open:

```text
https://github.com/ydixken/pgcopydb-operator/settings/security_analysis
```

Under "Dependency graph", click "Enable" and accept GitHub's confirmation if it appears.

Do not use a REST mutation, change Dependabot settings, or broaden repository permissions.

Expected: GitHub shows Dependency Graph as enabled.

- [ ] **Step 2: Prove that GitHub generated an SBOM**

Run:

```sh
set -euo pipefail
created="$(gh api repos/ydixken/pgcopydb-operator/dependency-graph/sbom --jq '.sbom.creationInfo.created')"
test -n "$created"
test "$created" != "null"
printf '%s\n' "$created"
```

Expected: exit 0 and one non-empty creation timestamp.

A 404, permission error, empty value, or `null` is a failed prerequisite.

- [ ] **Step 3: Run the independent review**

Have the verification worker run:

```sh
set -euo pipefail
created="$(gh api repos/ydixken/pgcopydb-operator/dependency-graph/sbom --jq '.sbom.creationInfo.created')"
test -n "$created"
test "$created" != "null"
```

Expected: exit 0 with no output.

**Commit boundary:** None.
This task changes repository settings only.

### Task 2: Guard release promotion with a parsed workflow test

**Files:**

- Modify: `test/buildconfig/buildconfig_test.go`
- Modify: `.github/workflows/release.yml`
- Test: `test/buildconfig/buildconfig_test.go`

**Interfaces:**

- Consumes: The existing `sigs.k8s.io/yaml` workflow parser and the `promote`, `e2e`, and `release-notes` jobs in `.github/workflows/release.yml`.
- Produces: `workflowJob.Needs` as `json.RawMessage`, `workflowJob.If` as `string`, and `TestReleasePromotionWaitsForCandidateChecks`.
- Guarantees: `promote` depends on exactly `e2e` and `release-notes`, and it has no job-level `if`.

**Worker:** Release workflow implementation worker.

**Independent review gate:** A separate reviewer checks the parsed test, confirms that `e2e` and `release-notes` still depend directly on `chart`, and runs the focused test.

- [ ] **Step 1: Extend the parsed workflow model**

Add `encoding/json` to the import block in `test/buildconfig/buildconfig_test.go`.

Replace the anonymous job structure with:

```go
type workflowJob struct {
	Needs json.RawMessage `json:"needs"`
	If    string          `json:"if"`
	Steps []workflowStep  `json:"steps"`
}

type workflow struct {
	Jobs map[string]workflowJob `json:"jobs"`
}
```

Keep the existing `workflowStep` type unchanged in this task.

- [ ] **Step 2: Add the regression test**

Add:

```go
func TestReleasePromotionWaitsForCandidateChecks(t *testing.T) {
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, releaseWorkflow)), &wf); err != nil {
		t.Fatalf("parse release.yml: %v", err)
	}

	promote, ok := wf.Jobs["promote"]
	if !ok {
		t.Fatal("release.yml has no promote job")
	}
	if len(promote.Needs) == 0 {
		t.Fatal("release.yml promote job declares no prerequisites")
	}

	var needs []string
	if err := json.Unmarshal(promote.Needs, &needs); err != nil {
		t.Fatalf("parse release.yml promote needs: %v", err)
	}
	want := []string{"e2e", "release-notes"}
	if !slices.Equal(needs, want) {
		t.Errorf("release.yml promote needs %v, want exactly %v", needs, want)
	}
	if promote.If != "" {
		t.Errorf("release.yml promote has job-level if %q; default dependency handling must gate promotion", promote.If)
	}
}
```

Format the file:

```sh
gofmt -w test/buildconfig/buildconfig_test.go
```

Expected: no output.

- [ ] **Step 3: Run the test and observe the failure**

Run:

```sh
go test ./test/buildconfig -run '^TestReleasePromotionWaitsForCandidateChecks$' -count=1
```

Expected: FAIL with:

```text
release.yml promote needs [e2e], want exactly [e2e release-notes]
```

The test must fail for the missing dependency, not for a parse or compile error.

- [ ] **Step 4: Apply the minimal workflow fix**

In `.github/workflows/release.yml`, change:

```yaml
needs: [e2e]
```

to:

```yaml
needs: [e2e, release-notes]
```

Do not add a job-level `if` to `promote`.
Do not serialize `e2e` and `release-notes`.
Both jobs must continue to depend directly on `chart`.

- [ ] **Step 5: Run the focused test again**

Run:

```sh
go test ./test/buildconfig -run '^TestReleasePromotionWaitsForCandidateChecks$' -count=1
```

Expected:

```text
ok  	github.com/ydixken/pgcopydb-operator/test/buildconfig
```

- [ ] **Step 6: Check the job graph directly**

Run:

```sh
rg -n '^\s+needs:|^  (chart|e2e|release-notes|promote):' .github/workflows/release.yml
```

Expected: `e2e` and `release-notes` each depend on `chart`, while `promote` has `needs: [e2e, release-notes]`.

- [ ] **Step 7: Run the task gates**

Run:

```sh
set -euo pipefail
git diff --check
task lint
task test
```

Expected: all three commands exit 0.
`task lint` and `task test` must not report skipped repository gates.

- [ ] **Step 8: Run the independent review**

The reviewer runs:

```sh
set -euo pipefail
go test ./test/buildconfig -run '^TestReleasePromotionWaitsForCandidateChecks$' -count=1
git diff --check
```

Expected: PASS and no whitespace errors.

The reviewer must reject a job-level `if`, including `always()`, `success()`, or a result expression.

- [ ] **Step 9: Commit the release gate**

Run:

```sh
set -euo pipefail
git add test/buildconfig/buildconfig_test.go .github/workflows/release.yml
git commit -m "fix(release): wait for candidate release before promotion"
```

Expected: one commit containing only the parser extension, regression test, and promotion dependency fix.

### Task 3: Upgrade every GitHub Action and add Dependency Review

**Files:**

- Modify: `test/buildconfig/buildconfig_test.go`
- Modify: `.github/workflows/artifacthub-metadata.yml`
- Modify: `.github/workflows/auto-release.yml`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/docs.yml`
- Modify: `.github/workflows/e2e.yml`
- Modify: `.github/workflows/mirror-to-gitlab.yml`
- Modify: `.github/workflows/pgcopydb-builder.yml`
- Modify: `.github/workflows/release.yml`
- Modify: `.github/workflows/runner-smoke.yml`
- Test: `test/buildconfig/buildconfig_test.go`

**Interfaces:**

- Consumes: The `workflow`, `workflowJob`, and `workflowStep` parser types from Task 2.
- Produces: Exactly 52 `uses:` references across nine workflow files, one Dependency Review step, and permanent tests for the approved action majors and dependency gate inputs.
- Preserves: Workflow events, permissions, inputs, outputs, concurrency, environments, and runner selection.

**Worker:** GitHub Actions implementation worker.

**Independent review gate:** A separate workflow reviewer runs the inventory tests, inspects the workflow diff, and confirms that no action input or security boundary changed outside the approved Dependency Review step.

- [ ] **Step 1: Extend the workflow test model**

Add `path/filepath` to the import block.

Add these constants to the existing constant block:

```go
workflowDir = "../../.github/workflows"
ciWorkflow  = "../../.github/workflows/ci.yml"
```

Replace `workflowStep` and `workflowJob` with:

```go
type workflowStep struct {
	Name string `json:"name"`
	Uses string `json:"uses"`
	If   string `json:"if"`
	With struct {
		AllowLicenses  string `json:"allow-licenses"`
		Context        string `json:"context"`
		FailOnScopes   string `json:"fail-on-scopes"`
		FailOnSeverity string `json:"fail-on-severity"`
		Platforms      string `json:"platforms"`
	} `json:"with"`
}

type workflowJob struct {
	Needs json.RawMessage `json:"needs"`
	If    string          `json:"if"`
	Uses  string          `json:"uses"`
	Steps []workflowStep  `json:"steps"`
}
```

- [ ] **Step 2: Add the approved-major inventory test**

Add:

```go
func TestWorkflowActionInventory(t *testing.T) {
	approved := map[string]string{
		"actions/cache":                     "v6",
		"actions/checkout":                  "v7",
		"actions/configure-pages":           "v6",
		"actions/dependency-review-action":  "v4",
		"actions/deploy-pages":              "v5",
		"actions/setup-go":                  "v7",
		"actions/setup-python":              "v7",
		"actions/upload-pages-artifact":     "v5",
		"azure/setup-helm":                  "v5",
		"azure/setup-kubectl":               "v5",
		"codecov/codecov-action":            "v7",
		"docker/build-push-action":          "v7",
		"docker/login-action":               "v4",
		"docker/setup-buildx-action":        "v4",
		"docker/setup-qemu-action":          "v4",
		"oras-project/setup-oras":           "v2",
	}

	entries, err := os.ReadDir(workflowDir)
	if err != nil {
		t.Fatalf("read workflow directory: %v", err)
	}

	files := 0
	references := 0
	dependencyReviews := 0
	check := func(location, uses string) {
		if uses == "" {
			return
		}
		references++
		name, version, ok := strings.Cut(uses, "@")
		if !ok || name == "" || version == "" {
			t.Errorf("%s has malformed action reference %q", location, uses)
			return
		}
		want, ok := approved[name]
		if !ok {
			t.Errorf("%s uses unapproved action %q", location, name)
			return
		}
		if version != want {
			t.Errorf("%s uses %s@%s, want %s@%s", location, name, version, name, want)
		}
		if name == "actions/dependency-review-action" {
			dependencyReviews++
		}
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yml" {
			continue
		}
		files++
		path := filepath.Join(workflowDir, entry.Name())
		var wf workflow
		if err := yaml.Unmarshal([]byte(read(t, path)), &wf); err != nil {
			t.Errorf("parse %s: %v", path, err)
			continue
		}
		for jobName, job := range wf.Jobs {
			check(entry.Name()+"/jobs/"+jobName, job.Uses)
			for i, step := range job.Steps {
				check(fmt.Sprintf("%s/jobs/%s/steps/%d", entry.Name(), jobName, i), step.Uses)
			}
		}
	}

	if files != 9 {
		t.Errorf("workflow inventory contains %d YAML files, want 9", files)
	}
	if references != 52 {
		t.Errorf("workflow inventory contains %d action references, want 52", references)
	}
	if dependencyReviews != 1 {
		t.Errorf("workflow inventory contains %d dependency review references, want 1", dependencyReviews)
	}
}
```

Add `fmt` to the import block if it is not already present.

- [ ] **Step 3: Add the Dependency Review configuration test**

Add:

```go
func TestDependencyReviewGatesPullRequests(t *testing.T) {
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, ciWorkflow)), &wf); err != nil {
		t.Fatalf("parse ci.yml: %v", err)
	}

	lint, ok := wf.Jobs["lint"]
	if !ok {
		t.Fatal("ci.yml has no lint job")
	}

	reviews := 0
	for i, step := range lint.Steps {
		if step.Uses != "actions/dependency-review-action@v4" {
			continue
		}
		reviews++
		if i == 0 || lint.Steps[i-1].Uses != "actions/checkout@v7" {
			t.Error("dependency review must appear immediately after lint checkout")
		}
		if step.Name != "Review dependency changes" {
			t.Errorf("dependency review name is %q", step.Name)
		}
		if step.If != "github.event_name == 'pull_request'" {
			t.Errorf("dependency review if is %q", step.If)
		}
		if step.With.FailOnSeverity != "moderate" {
			t.Errorf("dependency review fail-on-severity is %q", step.With.FailOnSeverity)
		}
		if step.With.FailOnScopes != "runtime, development, unknown" {
			t.Errorf("dependency review fail-on-scopes is %q", step.With.FailOnScopes)
		}
		if step.With.AllowLicenses != "Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, MIT" {
			t.Errorf("dependency review allow-licenses is %q", step.With.AllowLicenses)
		}
	}
	if reviews != 1 {
		t.Errorf("lint job contains %d dependency review steps, want 1", reviews)
	}
}
```

Format the file:

```sh
gofmt -w test/buildconfig/buildconfig_test.go
```

Expected: no output.

- [ ] **Step 4: Run the new tests and observe the failures**

Run:

```sh
go test ./test/buildconfig -run '^(TestWorkflowActionInventory|TestDependencyReviewGatesPullRequests)$' -count=1
```

Expected: FAIL because the workflows still use the old majors, contain 51 references, and have no Dependency Review step.

- [ ] **Step 5: Confirm every approved major ref exists upstream**

Run:

```sh
set -euo pipefail
while read -r repository ref; do
  git ls-remote --exit-code "https://github.com/${repository}.git" "$ref" >/dev/null ||
    { printf 'missing %s %s\n' "$repository" "$ref" >&2; exit 1; }
done <<'EOF'
actions/cache refs/tags/v6
actions/checkout refs/tags/v7
actions/configure-pages refs/tags/v6
actions/dependency-review-action refs/heads/v4
actions/deploy-pages refs/tags/v5
actions/setup-go refs/tags/v7
actions/setup-python refs/tags/v7
actions/upload-pages-artifact refs/tags/v5
azure/setup-helm refs/tags/v5
azure/setup-kubectl refs/tags/v5
codecov/codecov-action refs/tags/v7
docker/build-push-action refs/tags/v7
docker/login-action refs/tags/v4
docker/setup-buildx-action refs/tags/v4
docker/setup-qemu-action refs/tags/v4
oras-project/setup-oras refs/tags/v2
EOF
```

Expected: exit 0 with no output.

If an exact tag or the Dependency Review v4 branch is absent, stop and report it.
Do not substitute another version.

- [ ] **Step 6: Apply the mechanical major upgrades**

Use the `apply_patch` tool to make these exact substitutions across the nine `.github/workflows/*.yml` files:

```text
actions/checkout@v5 -> actions/checkout@v7
actions/setup-go@v5 -> actions/setup-go@v7
actions/setup-go@v6 -> actions/setup-go@v7
actions/setup-python@v5 -> actions/setup-python@v7
actions/setup-python@v6 -> actions/setup-python@v7
actions/cache@v4 -> actions/cache@v6
actions/configure-pages@v5 -> actions/configure-pages@v6
actions/upload-pages-artifact@v3 -> actions/upload-pages-artifact@v5
actions/deploy-pages@v4 -> actions/deploy-pages@v5
azure/setup-helm@v4 -> azure/setup-helm@v5
azure/setup-kubectl@v4 -> azure/setup-kubectl@v5
codecov/codecov-action@v5 -> codecov/codecov-action@v7
docker/setup-qemu-action@v3 -> docker/setup-qemu-action@v4
docker/setup-buildx-action@v3 -> docker/setup-buildx-action@v4
docker/login-action@v3 -> docker/login-action@v4
docker/build-push-action@v6 -> docker/build-push-action@v7
oras-project/setup-oras@v1 -> oras-project/setup-oras@v2
```

Expected: each existing reference matches the approved major inventory after the patch.

Do not change action inputs to make a new major pass.
If an existing input is unsupported, stop and return the action to the Team Leader for release-note review.

- [ ] **Step 7: Add the Dependency Review step**

In `.github/workflows/ci.yml`, insert this immediately after the `lint` job's checkout:

```yaml
      - name: Review dependency changes
        if: github.event_name == 'pull_request'
        uses: actions/dependency-review-action@v4
        with:
          fail-on-severity: moderate
          fail-on-scopes: runtime, development, unknown
          allow-licenses: Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, MIT
```

Keep the workflow-level permission exactly:

```yaml
permissions:
  contents: read
```

Do not add retries or write permissions.

- [ ] **Step 8: Run the focused tests**

Run:

```sh
go test ./test/buildconfig -run '^(TestWorkflowActionInventory|TestDependencyReviewGatesPullRequests|TestReleasePromotionWaitsForCandidateChecks)$' -count=1
```

Expected:

```text
ok  	github.com/ydixken/pgcopydb-operator/test/buildconfig
```

- [ ] **Step 9: Confirm the source inventory independently**

Run:

```sh
set -euo pipefail
test "$(rg '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 52
test "$(rg -l '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 9
test "$(rg -c 'uses: actions/dependency-review-action@v4' .github/workflows/ci.yml)" = 1
```

Expected: all commands exit 0 with no output.

- [ ] **Step 10: Review the workflow diff**

Run:

```sh
set -euo pipefail
git diff -- .github/workflows
git diff --check
```

Expected: the workflow diff contains only the approved action-major replacements, the Dependency Review step, and Task 2's `promote.needs` change.

Reject changes to events, permissions, concurrency, environments, runner selection, inputs, outputs, and existing action inputs.

- [ ] **Step 11: Run the task gates**

Run:

```sh
set -euo pipefail
task lint
task test
```

Expected: both commands exit 0 without skipped repository gates.

- [ ] **Step 12: Run the independent review**

The reviewer runs:

```sh
set -euo pipefail
go test ./test/buildconfig -run '^(TestWorkflowActionInventory|TestDependencyReviewGatesPullRequests|TestReleasePromotionWaitsForCandidateChecks)$' -count=1
test "$(rg '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 52
test "$(rg -l '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 9
git diff --check
```

Expected: all commands pass.

The reviewer also inspects `.github/workflows/ci.yml` and confirms that Dependency Review is conditional on pull requests and inherits only `contents: read`.

- [ ] **Step 13: Commit the workflow refresh**

Run:

```sh
set -euo pipefail
git add test/buildconfig/buildconfig_test.go .github/workflows
git commit -m "ci: refresh actions and review dependencies"
```

Expected: one lint-clean commit containing the workflow upgrades and their permanent tests.

### Task 4: Upgrade Go, direct modules, and golangci-lint

**Files:**

- Modify: `.custom-gcl.yml`
- Modify: `.golangci.yml`
- Modify: `Makefile`
- Modify: `go.mod`
- Modify: `go.sum`
- Modify: `test/e2e/e2e_suite_test.go`
- Modify: `internal/controller/error_paths_test.go`
- Modify: `internal/controller/migration_controller.go`
- Verify: resolved module graph

**Interfaces:**

- Consumes: Official Go 1.27.0 toolchain, controller-runtime v0.24.1 compatibility with Kubernetes v0.36, and the existing custom golangci-lint builder.
- Produces: Go directive 1.27.0, golangci-lint v2.13.2 in both pins, an `embedlit` policy exception, six approved direct module upgrades, resolver-selected patch or pseudo-version indirect changes, two `errors.AsType` compatibility updates, and three rate-limited `Requeue` staticcheck suppressions.
- Preserves: Ginkgo v2.32.1, `prometheus/client_golang` v1.24.1, controller-runtime v0.24.1, and `sigs.k8s.io/yaml` v1.6.0.

**Worker:** Go dependency implementation worker.

**Independent review gate:** A separate dependency reviewer inspects all eight files, the resolved module graph, both golangci-lint pins, the `embedlit` policy exception, and the compatibility edits before accepting the commit.

- [ ] **Step 1: Confirm Go 1.27.0 is an official release**

Run:

```sh
set -euo pipefail
curl -fsSL https://go.dev/doc/devel/release | rg -F 'go1.27.0'
```

Expected: exit 0 and output containing `go1.27.0`.

If the official release history does not list it, stop without changing the toolchain.

- [ ] **Step 2: Update the golangci-lint pins and policy**

Change `Makefile` to:

```make
GOLANGCI_LINT_VERSION ?= v2.13.2
```

Change `.custom-gcl.yml` to:

```yaml
version: v2.13.2
```

In `.golangci.yml`, add `embedlit` beside the existing `modernize.disable` entries.
This defers optional Go 1.27 literal-style churn while preserving every other analyzer.
Do not change the plugin list or replace its existing version policy.

In `test/e2e/e2e_suite_test.go`, retain the two `errors.AsType` updates for `*psqlFailure` and `*exec.ExitError`.
In `internal/controller/migration_controller.go`, retain the narrow adjacent `//nolint:staticcheck` directives for the silent rate-limited conflict retry and the rate-limited resume retry after the status patch.
In `internal/controller/error_paths_test.go`, retain the corresponding directive for the intentional silent rate-limited `Requeue` assertion.
Each directive must explain the rate-limited behavior it preserves.
Do not replace either `ctrl.Result{Requeue: true}` with `RequeueAfter`.

- [ ] **Step 3: Set the exact Go directive**

Run:

```sh
GOTOOLCHAIN=go1.27.0 go mod edit -go=1.27.0
```

Expected: exit 0.

- [ ] **Step 4: Pin the approved direct modules and refresh indirect patches**

Run:

```sh
set -euo pipefail
GOTOOLCHAIN=go1.27.0 go get \
  github.com/onsi/gomega@v1.43.0 \
  github.com/prometheus/client_model@v0.6.3 \
  k8s.io/api@v0.36.4 \
  k8s.io/apimachinery@v0.36.4 \
  k8s.io/client-go@v0.36.4 \
  k8s.io/streaming@v0.36.4
GOTOOLCHAIN=go1.27.0 go get -u=patch -t ./...
GOTOOLCHAIN=go1.27.0 go mod tidy
```

Expected: all three commands exit 0.
The patch-only upgrade may select newer indirect patch releases or pseudo-versions required by the package and test graph.
`go mod tidy` then removes graph entries that the resolved package and test graph does not need.

Do not omit `-u=patch`, add a direct dependency, or run an unconstrained minor or major module upgrade.

- [ ] **Step 5: Verify every direct dependency**

Run:

```sh
set -euo pipefail
direct_modules=$(GOTOOLCHAIN=go1.27.0 go mod edit -json |
  jq -r '.Go, (.Require[] | select(.Indirect | not) | "\(.Path) \(.Version)")'
)
expected_direct_modules='1.27.0
github.com/onsi/ginkgo/v2 v2.32.1
github.com/onsi/gomega v1.43.0
github.com/prometheus/client_golang v1.24.1
github.com/prometheus/client_model v0.6.3
k8s.io/api v0.36.4
k8s.io/apimachinery v0.36.4
k8s.io/client-go v0.36.4
k8s.io/streaming v0.36.4
sigs.k8s.io/controller-runtime v0.24.1
sigs.k8s.io/yaml v1.6.0'
test "$direct_modules" = "$expected_direct_modules"
printf '%s\n' "$direct_modules"
```

Expected output:

```text
1.27.0
github.com/onsi/ginkgo/v2 v2.32.1
github.com/onsi/gomega v1.43.0
github.com/prometheus/client_golang v1.24.1
github.com/prometheus/client_model v0.6.3
k8s.io/api v0.36.4
k8s.io/apimachinery v0.36.4
k8s.io/client-go v0.36.4
k8s.io/streaming v0.36.4
sigs.k8s.io/controller-runtime v0.24.1
sigs.k8s.io/yaml v1.6.0
```

Any added direct dependency or different version fails the task.

- [ ] **Step 6: Enforce the Kubernetes compatibility boundary**

Run:

```sh
set -euo pipefail
module_graph=$(GOTOOLCHAIN=go1.27.0 go list -m all)
printf '%s\n' "$module_graph" |
  awk '$1 ~ /^k8s\.io\// && $2 ~ /^v0\.37\./ { found = 1 } END { exit found }'
kubernetes_modules=$(GOTOOLCHAIN=go1.27.0 go list -m \
  k8s.io/api \
  k8s.io/apimachinery \
  k8s.io/client-go \
  k8s.io/streaming \
  sigs.k8s.io/controller-runtime)
expected_kubernetes_modules='k8s.io/api v0.36.4
k8s.io/apimachinery v0.36.4
k8s.io/client-go v0.36.4
k8s.io/streaming v0.36.4
sigs.k8s.io/controller-runtime v0.24.1'
test "$kubernetes_modules" = "$expected_kubernetes_modules"
printf '%s\n' "$kubernetes_modules"
```

Expected first command: exit 0 with no output.

Expected second command:

```text
k8s.io/api v0.36.4
k8s.io/apimachinery v0.36.4
k8s.io/client-go v0.36.4
k8s.io/streaming v0.36.4
sigs.k8s.io/controller-runtime v0.24.1
```

Indirect Kubernetes modules may remain on older v0.36 patches selected by controller-runtime.
No Kubernetes v0.37 module may enter the graph.

- [ ] **Step 7: Prove that the module files are tidy**

Run:

```sh
GOTOOLCHAIN=go1.27.0 go mod tidy -diff
```

Expected: exit 0 with no output.

- [ ] **Step 8: Rebuild the exact custom linter**

Remove only the generated linter entry so the Makefile cannot reuse the old binary:

```sh
set -euo pipefail
rm -f bin/golangci-lint
GOTOOLCHAIN=go1.27.0 make golangci-lint
bin/golangci-lint version | rg '2\.13\.2'
```

Expected: the installer builds the custom linter and the version output contains `2.13.2`.

Do not remove the `bin` directory or any source file.

- [ ] **Step 9: Inspect the dependency and compatibility diff**

Run:

```sh
set -euo pipefail
git diff -- \
  .custom-gcl.yml \
  .golangci.yml \
  Makefile \
  go.mod \
  go.sum \
  test/e2e/e2e_suite_test.go \
  internal/controller/error_paths_test.go \
  internal/controller/migration_controller.go
git diff --check
```

Expected: the direct `go.mod` block contains only the approved version changes, and the diff contains only the eight listed files.
Every indirect change must be selected by the approved patch-only upgrade or `go mod tidy`.

- [ ] **Step 10: Run the task gates with the exact toolchain**

Run:

```sh
set -euo pipefail
GOTOOLCHAIN=go1.27.0 task lint
GOTOOLCHAIN=go1.27.0 task test
```

Expected: both commands exit 0 without skipped repository gates.

- [ ] **Step 11: Run the independent review**

The reviewer reruns the exact direct and Kubernetes assertions from Steps 5 and 6, then runs:

```sh
set -euo pipefail
test "$(GOTOOLCHAIN=go1.27.0 go env GOVERSION)" = "go1.27.0"
test "$(rg -c '^GOLANGCI_LINT_VERSION \\?= v2\\.13\\.2$' Makefile)" = 1
test "$(rg -c '^version: v2\\.13\\.2$' .custom-gcl.yml)" = 1
test "$(rg -c '^\s*- embedlit$' .golangci.yml)" = 1
GOTOOLCHAIN=go1.27.0 go mod tidy -diff
module_graph=$(GOTOOLCHAIN=go1.27.0 go list -m all)
printf '%s\n' "$module_graph" |
  awk '$1 ~ /^k8s\.io\// && $2 ~ /^v0\.37\./ { found = 1 } END { exit found }'
```

Expected: all commands exit 0, and the tidy and forbidden-version checks print nothing.
The reviewer also confirms the two `errors.AsType` updates and three narrow rate-limited `Requeue` staticcheck suppressions preserve existing behavior.

- [ ] **Step 12: Commit the Go dependency refresh**

Run:

```sh
set -euo pipefail
git add \
  .custom-gcl.yml \
  .golangci.yml \
  Makefile \
  go.mod \
  go.sum \
  test/e2e/e2e_suite_test.go \
  internal/controller/error_paths_test.go \
  internal/controller/migration_controller.go
test "$(git diff --cached --name-only | wc -l | tr -d ' ')" = 8
git commit -m "chore(deps): upgrade Go and modules"
```

Expected: one lint-clean commit containing exactly the eight listed files, the Go directive, approved module changes, tidy output, synchronized linter pins, the `embedlit` policy exception, and the required compatibility edits.

### Task 5: Refresh public image digests and the local e2e baseline

**Files:**

- Modify: `Dockerfile`
- Modify: `images/pgcopydb-builder/Dockerfile`
- Modify: `images/runner/Dockerfile`
- Modify: `.github/workflows/e2e.yml`
- Modify: `test/e2e/e2e_suite_test.go`
- Modify: `test/buildconfig/buildconfig_test.go`
- Test: `test/buildconfig/buildconfig_test.go`

**Interfaces:**

- Consumes: Go 1.27.0 from Task 4 and the public multi-platform image indexes for `golang:1.27.0`, `gcr.io/distroless/static:nonroot`, and `debian:trixie-slim`.
- Produces: Current public image index pins and an e2e default of v0.11.3 with a permanent assertion.
- Preserves: The internal `pgcopydb-builder` tag and digest in `images/runner/Dockerfile`.

**Worker:** Container pin and e2e baseline implementation worker.

**Independent review gate:** A separate reviewer resolves each public tag again, verifies amd64 and arm64 entries, confirms the exact Dockerfile pins, and proves that the internal builder reference did not change.

- [ ] **Step 1: Add the e2e default regression test**

Add this constant to `test/buildconfig/buildconfig_test.go`:

```go
e2eSuite = "../../test/e2e/e2e_suite_test.go"
```

Add:

```go
func TestE2EDefaultRelease(t *testing.T) {
	re := regexp.MustCompile(`(?m)^var operatorTag = "([^"]+)"$`)
	match := re.FindStringSubmatch(read(t, e2eSuite))
	if match == nil {
		t.Fatal("e2e suite declares no operatorTag default")
	}
	if got, want := match[1], "v0.11.3"; got != want {
		t.Errorf("e2e operatorTag default is %s, want %s", got, want)
	}
}
```

Format the file:

```sh
gofmt -w test/buildconfig/buildconfig_test.go
```

Expected: no output.

- [ ] **Step 2: Run the new test and observe the failure**

Run:

```sh
go test ./test/buildconfig -run '^TestE2EDefaultRelease$' -count=1
```

Expected: FAIL with:

```text
e2e operatorTag default is v0.5.0, want v0.11.3
```

- [ ] **Step 3: Verify the approved public image digests**

Run:

```sh
set -euo pipefail
test "$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 digest golang:1.27.0)" = \
  "sha256:4013ae0f9e7994f8535c58c811f8f863fbed38b72e0d51e6592156f758d66146"
test "$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 digest gcr.io/distroless/static:nonroot)" = \
  "sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7"
test "$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 digest debian:trixie-slim)" = \
  "sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132"
```

Expected: all commands exit 0 with no output.

If any tag resolves to another index, stop and return the new public digest to the Team Leader for review before editing.

- [ ] **Step 4: Verify amd64 and arm64 manifests**

Run:

```sh
set -euo pipefail
for image in \
  "golang:1.27.0@sha256:4013ae0f9e7994f8535c58c811f8f863fbed38b72e0d51e6592156f758d66146" \
  "gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7" \
  "debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132"
do
  go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 manifest "$image" |
    jq -e '
      [.manifests[].platform
       | select(.os == "linux" and (.architecture == "amd64" or .architecture == "arm64"))
       | .architecture]
      | unique == ["amd64", "arm64"]
    '
done
```

Expected: three `true` values and exit 0.

- [ ] **Step 5: Apply the public image pins**

Set the manager builder line in `Dockerfile` to:

```dockerfile
FROM --platform=$BUILDPLATFORM golang:1.27.0@sha256:4013ae0f9e7994f8535c58c811f8f863fbed38b72e0d51e6592156f758d66146 AS builder
```

Set the manager runtime line in `Dockerfile` to:

```dockerfile
FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7
```

Set the Debian builder line in `images/pgcopydb-builder/Dockerfile` to:

```dockerfile
FROM debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132 AS build
```

Set the Debian runtime line in `images/runner/Dockerfile` to:

```dockerfile
FROM debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132
```

Do not change this internal builder reference in the first pull request:

```dockerfile
FROM ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:e37d2bd4dd10b7ed7b415555ce3318202d9633cf@sha256:0b3a2afef9d6cc156aa59713bbb8ab7bd97597b978b7938c0e804ba26c044a59 AS pgcopydb
```

- [ ] **Step 6: Update the e2e baseline and its explanation**

In `test/e2e/e2e_suite_test.go`, replace the old comment and default with:

```go
// operatorTag pins the manager and runner images for the throwaway install and
// can be overridden with E2E_OPERATOR_TAG. v0.11.3 is the stable baseline.
var operatorTag = "v0.11.3"
```

In `.github/workflows/e2e.yml`, change the input description to:

```yaml
description: Published release tag to install, for example v0.11.3
```

Keep `E2E_OPERATOR_TAG` as the supported override.

- [ ] **Step 7: Run the focused test**

Run:

```sh
go test ./test/buildconfig -run '^TestE2EDefaultRelease$' -count=1
```

Expected:

```text
ok  	github.com/ydixken/pgcopydb-operator/test/buildconfig
```

- [ ] **Step 8: Confirm the resulting image references**

Run:

```sh
set -euo pipefail
rg -n '^FROM ' Dockerfile images/pgcopydb-builder/Dockerfile images/runner/Dockerfile
test "$(
  git show HEAD:images/runner/Dockerfile |
    rg '^FROM ghcr\.io/ydixken/pgcopydb-operator/pgcopydb-builder:'
)" = "$(
  rg '^FROM ghcr\.io/ydixken/pgcopydb-operator/pgcopydb-builder:' images/runner/Dockerfile
)"
```

Expected: every public image has the exact tag and digest above, and the internal builder comparison exits 0.

- [ ] **Step 9: Run the task gates**

Run:

```sh
set -euo pipefail
git diff --check
GOTOOLCHAIN=go1.27.0 task lint
GOTOOLCHAIN=go1.27.0 task test
```

Expected: all commands exit 0 without skipped repository gates.

Do not run local `task e2e`.
It targets the current real-cluster context and requires a human confirmation prompt.

- [ ] **Step 10: Run the independent review**

The reviewer runs the digest and platform commands from Steps 3 and 4, followed by:

```sh
set -euo pipefail
go test ./test/buildconfig -run '^TestE2EDefaultRelease$' -count=1
test "$(
  git show HEAD:images/runner/Dockerfile |
    rg '^FROM ghcr\.io/ydixken/pgcopydb-operator/pgcopydb-builder:'
)" = "$(
  rg '^FROM ghcr\.io/ydixken/pgcopydb-operator/pgcopydb-builder:' images/runner/Dockerfile
)"
```

Expected: all commands pass.

- [ ] **Step 11: Commit the public pins and e2e baseline**

Run:

```sh
set -euo pipefail
git add \
  Dockerfile \
  images/pgcopydb-builder/Dockerfile \
  images/runner/Dockerfile \
  .github/workflows/e2e.yml \
  test/e2e/e2e_suite_test.go \
  test/buildconfig/buildconfig_test.go
git commit -m "chore(deps): refresh image and e2e pins"
```

Expected: one lint-clean commit.
The internal builder digest remains unchanged for the second pull request.

### Task 6: Audit the supply chain and run the complete local gate

**Files:**

- Modify: none.
- Inspect: all first-pull-request changes.
- Produce outside git: a license audit and a verification report to the Team Leader.

**Interfaces:**

- Consumes: The completed Tasks 1 through 5 and the baseline commit `fcbc56d`.
- Produces: Clean dependency, license, vulnerability, lint, test, inventory, and diff results suitable for opening the first pull request.
- Blocks: No branch push or pull request may proceed if any result is absent, skipped, unresolved, or non-zero.

**Worker:** Supply-chain audit worker.

**Independent review gate:** A separate verification worker reruns the deterministic checks from a clean worktree and reviews the complete diff.

- [ ] **Step 1: Confirm branch and worktree state**

Run:

```sh
git status --short --branch
```

Expected first line:

```text
## fix/issue-95-maintenance
```

Expected: no uncommitted paths after the branch line.

- [ ] **Step 2: Confirm Dependency Graph remains available**

Run:

```sh
set -euo pipefail
created="$(gh api repos/ydixken/pgcopydb-operator/dependency-graph/sbom --jq '.sbom.creationInfo.created')"
test -n "$created"
test "$created" != "null"
```

Expected: exit 0 with no output.

- [ ] **Step 3: Recheck the workflow inventory**

Run:

```sh
set -euo pipefail
test "$(rg '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 52
test "$(rg -l '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 9
test "$(rg -c 'uses: actions/dependency-review-action@v4' .github/workflows/ci.yml)" = 1
go test ./test/buildconfig -count=1
```

Expected: all shell assertions pass and the Go package reports `ok`.

- [ ] **Step 4: Verify the exact module boundaries**

Run:

```sh
set -euo pipefail
test "$(GOTOOLCHAIN=go1.27.0 go env GOVERSION)" = "go1.27.0"
GOTOOLCHAIN=go1.27.0 go mod tidy -diff
module_graph=$(GOTOOLCHAIN=go1.27.0 go list -m all)
printf '%s\n' "$module_graph" |
  awk '$1 ~ /^k8s\.io\// && $2 ~ /^v0\.37\./ { found = 1 } END { exit found }'
GOTOOLCHAIN=go1.27.0 go mod edit -json |
  jq -e '
    .Go == "1.27.0" and
    ([.Require[] | select(.Indirect | not) | "\(.Path) \(.Version)"] == [
      "github.com/onsi/ginkgo/v2 v2.32.1",
      "github.com/onsi/gomega v1.43.0",
      "github.com/prometheus/client_golang v1.24.1",
      "github.com/prometheus/client_model v0.6.3",
      "k8s.io/api v0.36.4",
      "k8s.io/apimachinery v0.36.4",
      "k8s.io/client-go v0.36.4",
      "k8s.io/streaming v0.36.4",
      "sigs.k8s.io/controller-runtime v0.24.1",
      "sigs.k8s.io/yaml v1.6.0"
    ])
  '
```

Expected: the Go version, tidy, and module boundary assertions print nothing.
The `jq` command prints `true`.
All commands exit 0.

- [ ] **Step 5: List every changed module version for review**

Run:

```sh
set -euo pipefail
changed_modules="$({
  git diff --unified=0 fcbc56d..HEAD -- go.mod |
    sed -n -E 's/^\+[[:space:]]+([^[:space:]]+)[[:space:]]+(v[^[:space:]]+).*/\1 \2/p'
} | sort -u)"
test -n "$changed_modules"
printf '%s\n' "$changed_modules"
```

Expected: a non-empty, sorted list of the direct and indirect module versions added to the resolved graph.

Review every module version in this list.
An indirect change is acceptable only when the resolved graph requires it.

- [ ] **Step 6: Audit each changed module license manually**

Run:

```sh
set -euo pipefail
changed_modules="$({
  git diff --unified=0 fcbc56d..HEAD -- go.mod |
    sed -n -E 's/^\+[[:space:]]+([^[:space:]]+)[[:space:]]+(v[^[:space:]]+).*/\1 \2/p'
} | sort -u)"
test -n "$changed_modules"
while read -r module version; do
  info="$(GOTOOLCHAIN=go1.27.0 go mod download -json "$module@$version")"
  dir="$(printf '%s\n' "$info" | jq -er '.Dir')"
  origin_url="$(printf '%s\n' "$info" | jq -er '.Origin.URL')"
  origin_hash="$(printf '%s\n' "$info" | jq -er '.Origin.Hash')"
  license_files="$(rg --files "$dir" | rg -i '/(LICENSE|COPYING|NOTICE)(\..*)?$')"
  test -n "$license_files"
  printf '%s %s\norigin: %s %s\nlicense files:\n%s\n' \
    "$module" "$version" "$origin_url" "$origin_hash" "$license_files"
done <<EOF
$changed_modules
EOF
```

Expected: every module resolves to a public origin commit and at least one license, copying, or notice file.

Read each listed file and classify it as Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, or MIT.
Record the module, version, SPDX identifier, and an exact public upstream license URL in the worker report.
Do not add `go-licenses` or another audit dependency.

For each unresolved license, inspect the dependency's public upstream license file and report the module path, SPDX identifier, and public source URL to the Team Leader.
An unresolved or unacceptable license blocks the pull request.
Do not copy dependency source text into the repository.

- [ ] **Step 7: Run the pinned vulnerability scan**

Run:

```sh
go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
```

Expected: exit 0 and:

```text
No vulnerabilities found.
```

The command must use default non-JSON output.
It must not add `golang.org/x/vuln` to `go.mod`.

- [ ] **Step 8: Prove that audit tools did not alter the module files**

Run:

```sh
git status --short
```

Expected: no output.

- [ ] **Step 9: Inspect the full first-pull-request diff**

Run:

```sh
set -euo pipefail
git diff --check fcbc56d..HEAD
git diff --stat fcbc56d..HEAD
git diff fcbc56d..HEAD
```

Expected: no whitespace errors.

Confirm all of the following:

- The 51 existing action references use only the approved majors.
- Dependency Review appears once, directly after lint checkout.
- `promote` depends exactly on `e2e` and `release-notes`.
- Go and direct dependency versions match Task 4.
- Both golangci-lint pins are v2.13.2.
- Public image references match Task 5.
- The internal builder digest is unchanged.
- The e2e default and example are v0.11.3.
- No private infrastructure facts, credentials, or secret values appear.

- [ ] **Step 10: Run the repository lint and test gates**

Run:

```sh
set -euo pipefail
GOTOOLCHAIN=go1.27.0 task lint
GOTOOLCHAIN=go1.27.0 task test
```

Expected: both commands exit 0 without skipped repository gates.

- [ ] **Step 11: Run the independent verification gate**

From the same branch with a clean worktree, the verification worker runs:

```sh
set -euo pipefail
git diff --check fcbc56d..HEAD
test "$(rg '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 52
test "$(rg -l '^\s*-?\s*uses:' .github/workflows | wc -l | tr -d ' ')" = 9
created="$(gh api repos/ydixken/pgcopydb-operator/dependency-graph/sbom --jq '.sbom.creationInfo.created')"
test -n "$created"
test "$created" != "null"
GOTOOLCHAIN=go1.27.0 go mod tidy -diff
go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
GOTOOLCHAIN=go1.27.0 task lint
GOTOOLCHAIN=go1.27.0 task test
git status --short
```

Expected:

- Every command exits 0.
- `govulncheck` reports no reachable known vulnerability.
- `task lint` and `task test` pass.
- The final `git status --short` prints nothing.

The reviewer returns command conclusions and the license audit result to the Team Leader.
Do not record private runner, cluster, endpoint, or credential details.

**Commit boundary:** None.
If an audit tool changes tracked files, stop and return the branch to the implementation worker instead of committing audit fallout.

### Task 7: Document the dependency gate and finalize the first pull request

**Files:**

- Modify: `CONTRIBUTING.md`
- Modify: `tasks/todo.md`
- Review: every path changed from `origin/main`

**Interfaces:**

- Consumes: the focused implementation commits and verified evidence from Tasks 1 through 6.
- Produces: contributor guidance for the required Dependency Review gate and an accurate local task record.
- Produces: a clean, independently reviewed `fix/issue-95-maintenance` branch ready to push.
- Preserves: the `.github/workflows/e2e.yml` example already updated in Task 5.

**Worker:** Documentation and integration worker.

**Independent review gate:** A separate reviewer compares the complete branch and the two pending documentation files against `origin/main`, then approves or returns concrete findings before the documentation commit.

- [ ] **Step 1: Document the pull request dependency gate**

Use the `apply_patch` tool to add these sentences immediately after the paragraph in `CONTRIBUTING.md` that defines the three required CI jobs:

```markdown
The pull request `lint` job runs GitHub Dependency Review and rejects new dependencies with moderate or higher known vulnerabilities, disallowed licenses, or violations in runtime, development, or unknown scopes.
GitHub cannot fail Dependency Review for every unresolved license, so contributors MUST review those warnings and resolve each license from a public source before merging.
```

Expected: each sentence occupies one Markdown line.
The text contains no private repository or runner information.

- [ ] **Step 2: Update the local task record**

Use the `apply_patch` tool to mark the completed first-pull-request implementation and local verification items in `tasks/todo.md`.
Add the exact command conclusions from Tasks 1 through 6 to its `## Review` section.
Do not mark a push, pull request, workflow, merge, publication, or real-cluster item complete before its result exists.

Expected: `tasks/todo.md` separates completed local work from pending remote work and contains no predicted result.

- [ ] **Step 3: Check the documentation edits**

Apply the humanizer skill to both edited files, then run:

```sh
set -euo pipefail
git diff --check
rg -n 'Dependency Review|Implementation plan|Plan review' CONTRIBUTING.md tasks/todo.md
```

Expected: the diff check prints nothing.
The matches describe only completed evidence and pending gates.
Neither file contains an em dash, en dash, private infrastructure fact, or secret value.

- [ ] **Step 4: Run the repository gates after the documentation edit**

Run:

```sh
set -euo pipefail
GOTOOLCHAIN=go1.27.0 task lint
GOTOOLCHAIN=go1.27.0 task test
```

Expected: both commands exit 0 without skipped repository gates.
Do not run `task e2e` or any command containing `task --yes`.

- [ ] **Step 5: Obtain independent first-pull-request approval**

Give the reviewer the approved spec, Task 6 evidence, module license evidence, and these diffs:

```sh
set -euo pipefail
git diff --stat origin/main
git diff origin/main
```

Expected: the combined committed and uncommitted diff contains only the approved design, plan, task record, workflow, dependency, Dockerfile, test, and documentation changes.

The reviewer confirms all of the following:

- The internal `pgcopydb-builder` digest in `images/runner/Dockerfile` still matches `origin/main`.
- The inventory contains 52 action references across nine workflow files.
- `promote` depends exactly on `e2e` and `release-notes` and has no job-level `if`.
- Go, module, linter, public image, and e2e versions match the approved spec.
- The dependency gates and tests fail closed on absence.
- The branch contains no unrelated edit, private infrastructure fact, credential, or secret value.

A finding blocks the commit until a worker fixes it, reruns the affected gate, and obtains approval of the revised diff.

- [ ] **Step 6: Commit only the documentation and task record**

Run:

```sh
set -euo pipefail
git add CONTRIBUTING.md tasks/todo.md
test "$(git diff --cached --name-only | wc -l | tr -d ' ')" = 2
test -z "$(git diff --cached --name-only | rg -v '^(CONTRIBUTING.md|tasks/todo.md)$')"
git diff --cached --check
git commit -m "docs: document dependency review gate"
git status --short --branch
```

Expected: the cached diff contains only `CONTRIBUTING.md` and `tasks/todo.md`.
The commit succeeds, and the final status shows `fix/issue-95-maintenance` with no uncommitted paths.

**Stop rule:** Do not push while any local, security, license, documentation, or reviewer gate remains unresolved.

### Task 8: Open, validate, and merge the first pull request

**Files:**

- Remote branch: `fix/issue-95-maintenance`
- Pull request base: `main`
- Read: `.github/workflows/ci.yml`
- Read: `.github/workflows/e2e.yml`
- Read: `.github/workflows/mirror-to-gitlab.yml`
- Read: `.github/workflows/runner-smoke.yml`

**Interfaces:**

- Consumes: the clean, reviewed branch from Task 7.
- Produces: a merged first pull request and public evidence for required CI, Dependency Review, mirror, runner smoke, and the protected feature-branch e2e rejection.
- Preserves: branch protection, the `e2e-cluster` environment policy, protected runner selection, and public logs.

**Worker:** GitHub workflow operator.

**Independent review gate:** A reviewer who did not operate the workflows approves the pull request diff and every branch result before merge.

- [ ] **Step 1: Push the reviewed branch**

Run:

```sh
set -euo pipefail
test "$(git branch --show-current)" = "fix/issue-95-maintenance"
test -z "$(git status --porcelain)"
git push --set-upstream origin fix/issue-95-maintenance
```

Expected: the push succeeds without a force option, and the remote branch points at the reviewed local commit.

- [ ] **Step 2: Open the first pull request**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr create \
  --base main \
  --head fix/issue-95-maintenance \
  --title "chore: refresh dependencies and release gates" \
  --body-file - <<'EOF'
## Summary

- refresh GitHub Actions, Go 1.27.0, approved Go modules, golangci-lint, and public base-image digests
- add the pull request dependency gate and fix release promotion ordering
- update the local e2e default to v0.11.3

Tracks #95.

## Local verification

- `git diff --check`
- `go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...`
- changed-module license review
- `task lint`
- `task test`

## Remote verification

- required pull request CI and Dependency Review
- feature-branch e2e environment rejection
- branch runner smoke
- post-merge builder publication and main e2e
EOF
)
test -n "$first_pr_url"
printf '%s\n' "$first_pr_url"
```

Expected: `gh pr create` prints one public pull request URL.
The body tracks issue 95 without closing or editing Renovate's dashboard.

- [ ] **Step 3: Prove the protected environment rejects the feature branch**

Record the head and dispatch time, dispatch the workflow, then locate the new run instead of relying on `gh workflow run` to return a URL:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_head_sha=$(gh pr view "$first_pr_url" --json headRefOid --jq '.headRefOid')
before_branch_e2e_runs=$(
  gh run list \
    --workflow e2e.yml \
    --branch fix/issue-95-maintenance \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId |
    jq '[.[].databaseId]'
)
gh workflow run e2e.yml \
  --ref fix/issue-95-maintenance \
  -f tag=v0.11.3 \
  -f scale=0.1

branch_e2e_run=
for attempt in $(seq 1 60); do
  branch_e2e_run=$(gh run list \
    --workflow e2e.yml \
    --branch fix/issue-95-maintenance \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId,headBranch,headSha |
    jq -r \
      --arg sha "$first_head_sha" \
      --arg ref fix/issue-95-maintenance \
      --argjson before "$before_branch_e2e_runs" '
        [.[]
         | .databaseId as $id
         | select(.headSha == $sha and .headBranch == $ref
                  and (($before | index($id)) == null))
         | $id]
        | first // empty
      ')
  test -n "$branch_e2e_run" && break
  test "$attempt" -lt 60 || {
    echo "feature-branch e2e run did not appear" >&2
    exit 1
  }
  sleep 5
done

if gh run watch "$branch_e2e_run" --exit-status; then
  echo "feature-branch e2e unexpectedly passed" >&2
  exit 1
fi

test "$(gh run view "$branch_e2e_run" --json conclusion --jq '.conclusion')" = "failure"

branch_e2e_jobs=$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$branch_e2e_run/jobs?per_page=100")
printf '%s\n' "$branch_e2e_jobs" | jq -e '
  (.total_count == 0)
  or all(.jobs[];
    (.runner_name // "") == ""
    and all(.steps[]?;
      .status != "completed" or (.conclusion // "") == "skipped"))
'

branch_e2e_url=$(gh run view "$branch_e2e_run" --json url --jq '.url')
printf '%s\n' "$branch_e2e_url"
```

Expected: the public run shows the `e2e-cluster` deployment branch policy rejection.
The jobs response either contains no job or contains only jobs with no `runner_name` and no executed step.
This is the observable proof that the protected runner was not assigned.

If any job has a runner name or any non-skipped step completes, stop before further workflow or merge work.
Do not weaken the environment policy.

- [ ] **Step 4: Run the branch runner smoke workflow**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_head_sha=$(gh pr view "$first_pr_url" --json headRefOid --jq '.headRefOid')
before_runner_smoke_runs=$(
  gh run list \
    --workflow runner-smoke.yml \
    --branch fix/issue-95-maintenance \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId |
    jq '[.[].databaseId]'
)
gh workflow run runner-smoke.yml --ref fix/issue-95-maintenance

runner_smoke_run=
for attempt in $(seq 1 60); do
  runner_smoke_run=$(gh run list \
    --workflow runner-smoke.yml \
    --branch fix/issue-95-maintenance \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId,headBranch,headSha |
    jq -r \
      --arg sha "$first_head_sha" \
      --arg ref fix/issue-95-maintenance \
      --argjson before "$before_runner_smoke_runs" '
        [.[]
         | .databaseId as $id
         | select(.headSha == $sha and .headBranch == $ref
                  and (($before | index($id)) == null))
         | $id]
        | first // empty
      ')
  test -n "$runner_smoke_run" && break
  test "$attempt" -lt 60 || {
    echo "runner smoke run did not appear" >&2
    exit 1
  }
  sleep 5
done

gh run watch "$runner_smoke_run" --exit-status
test "$(gh run view "$runner_smoke_run" --json conclusion --jq '.conclusion')" = "success"
test "$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$runner_smoke_run/jobs?per_page=100" \
  --jq '[.jobs[].steps[] | select(.name == "Build both platforms without pushing") | .conclusion] | unique')" = '["success"]'
gh run view "$runner_smoke_run" --json url,conclusion
```

Expected: the `smoke` job and its `Build both platforms without pushing` step conclude `success`.
The smoke build covers linux/amd64 and linux/arm64 without publishing an image.

- [ ] **Step 5: Require every pull request check**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
gh pr checks "$first_pr_url" --watch --fail-fast
test "$(gh pr checks "$first_pr_url" --required --json bucket --jq 'all(.[]; .bucket == "pass")')" = "true"
test "$(gh pr checks "$first_pr_url" --json name,bucket \
  --jq '[.[] | select(.name == "lint" or .name == "test" or .name == "docs") | .bucket] | sort')" = '["pass","pass","pass"]'
```

Expected: `lint`, `test`, and `docs` pass.
No required check is skipped, cancelled, missing, or inconclusive.

- [ ] **Step 6: Confirm Dependency Review ran and inspect its warnings**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_head_sha=$(gh pr view "$first_pr_url" --json headRefOid --jq '.headRefOid')
ci_run_id=$(gh run list \
  --workflow ci.yml \
  --branch fix/issue-95-maintenance \
  --event pull_request \
  --limit 20 \
  --json databaseId \
  --jq '.[0].databaseId')
test -n "$ci_run_id"
test "$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$ci_run_id" \
  --jq '.pull_requests[0].head.sha')" = "$first_head_sha"
test "$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$ci_run_id/jobs?per_page=100" \
  --jq '[.jobs[] | select(.name == "lint").steps[] | select(.name == "Review dependency changes") | .conclusion] | unique')" = '["success"]'
gh run view "$ci_run_id" --web
```

Expected: the `Review dependency changes` step exists once in the `lint` job and concludes `success` for the exact pull request head.
Inspect its annotations and log in the public run.
Resolve every unknown license from a public source.
A moderate or higher vulnerability, disallowed license, unresolved license, or missing step blocks the merge.

- [ ] **Step 7: Confirm the branch mirror**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_head_sha=$(gh pr view "$first_pr_url" --json headRefOid --jq '.headRefOid')
mirror_run_id=$(gh run list \
  --workflow mirror-to-gitlab.yml \
  --commit "$first_head_sha" \
  --event push \
  --limit 20 \
  --json databaseId \
  --jq '.[0].databaseId')
test -n "$mirror_run_id"
gh run watch "$mirror_run_id" --exit-status
test "$(gh run view "$mirror_run_id" --json conclusion --jq '.conclusion')" = "success"
gh run view "$mirror_run_id" --json url,conclusion
```

Expected: the mirror run for the exact pull request head concludes `success`.

- [ ] **Step 8: Obtain independent merge approval**

Give the reviewer the pull request URL, exact head SHA, required checks, Dependency Review annotations and license resolutions, feature-branch e2e URL and jobs response, runner-smoke URL, and mirror URL.

Expected: the reviewer confirms that the protected e2e runner was not assigned and that every positive gate passed.
Any failed, skipped, cancelled, missing, or inconclusive required result blocks the merge.

- [ ] **Step 9: Squash-merge the exact reviewed head**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_head_sha=$(gh pr view "$first_pr_url" --json headRefOid --jq '.headRefOid')
gh pr merge "$first_pr_url" --squash --match-head-commit "$first_head_sha"

for attempt in $(seq 1 60); do
  first_pr_state=$(gh pr view "$first_pr_url" --json state --jq '.state')
  test "$first_pr_state" = "MERGED" && break
  test "$attempt" -lt 60 || {
    echo "first pull request did not merge" >&2
    exit 1
  }
  sleep 5
done

gh pr view "$first_pr_url" --json mergeCommit,url
```

Expected: GitHub merges the exact reviewed head through normal branch protection.
Do not use `--admin`, force-push, or delete the branch.

**Stop and patch-forward rules:**

- Fix a pre-merge defect on `fix/issue-95-maintenance` with a normal commit, rerun every invalidated check, and obtain fresh review.
- Do not merge while any required or independent-review gate is unresolved.
- Do not add retries to hide Dependency Review failures or bypass branch protection.

### Task 9: Verify the first merge, builder publication, and main e2e

**Files:**

- Read: `.github/workflows/ci.yml`
- Read: `.github/workflows/docs.yml`
- Read: `.github/workflows/e2e.yml`
- Read: `.github/workflows/mirror-to-gitlab.yml`
- Read: `.github/workflows/pgcopydb-builder.yml`
- Read: `images/pgcopydb-builder/Dockerfile`
- Remote artifact: `ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder`

**Interfaces:**

- Consumes: the first pull request merge commit from Task 8.
- Produces: successful `main` CI, docs, mirror, and builder runs for that exact commit.
- Produces: the literal two-platform builder index digest required by Task 10.
- Produces: a successful protected `main` e2e run against v0.11.3 at scale 0.1, including cleanup.

**Worker:** Post-merge verification operator.

**Independent review gate:** A separate reviewer confirms every public run, the builder index, and the main e2e cleanup result before the digest branch begins.

- [ ] **Step 1: Resolve the exact first merge commit**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_merge_sha=$(gh pr view "$first_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
test -n "$first_merge_sha"
test "$(gh api repos/ydixken/pgcopydb-operator/commits/main --jq '.sha')" = "$first_merge_sha"
git fetch origin main
test "$(git rev-parse origin/main)" = "$first_merge_sha"
```

Expected: the pull request merge commit, GitHub's `main`, and `origin/main` are the same commit.
If `main` advanced, stop and reconcile the new public history before continuing.

- [ ] **Step 2: Require all first-merge workflows**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_merge_sha=$(gh pr view "$first_pr_url" --json mergeCommit --jq '.mergeCommit.oid')

for workflow in ci.yml docs.yml mirror-to-gitlab.yml pgcopydb-builder.yml; do
  run_id=
  for attempt in $(seq 1 60); do
    run_id=$(gh run list \
      --workflow "$workflow" \
      --commit "$first_merge_sha" \
      --event push \
      --limit 20 \
      --json databaseId \
      --jq '.[0].databaseId')
    test -n "$run_id" && break
    test "$attempt" -lt 60 || {
      echo "$workflow did not start for $first_merge_sha" >&2
      exit 1
    }
    sleep 5
  done
  gh run watch "$run_id" --exit-status
  test "$(gh run view "$run_id" --json conclusion --jq '.conclusion')" = "success"
  gh run view "$run_id" --json url,workflowName,event,headSha,conclusion,jobs
done
```

Expected: `CI`, `Docs`, `Mirror to GitLab`, and `pgcopydb builder image` each conclude `success` for the exact first merge commit.
The CI record shows successful `lint`, `test`, and `docs` jobs.
The other records show successful `deploy`, `mirror`, and `build` jobs, respectively.

- [ ] **Step 3: Read and verify the published builder index**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_merge_sha=$(gh pr view "$first_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
builder_tag=$(git show "$first_merge_sha":images/pgcopydb-builder/Dockerfile |
  sed -n 's/^ARG PGCOPYDB_SHA=//p')
test -n "$builder_tag"

builder_ref="ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:$builder_tag"
builder_digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  digest "$builder_ref")
printf '%s\n' "$builder_digest" | rg '^sha256:[0-9a-f]{64}$'

go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  manifest "$builder_ref@$builder_digest" |
  jq -e '
    (.mediaType == "application/vnd.oci.image.index.v1+json"
     or .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json")
    and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/amd64") != null)
    and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/arm64") != null)
  '

printf '%s@%s\n' "$builder_ref" "$builder_digest"
```

Expected: the registry returns one multi-platform index digest with linux/amd64 and linux/arm64 manifests.
Record that literal digest and the successful `pgcopydb builder image` run URL for Task 10.

- [ ] **Step 4: Dispatch and require main e2e**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_merge_sha=$(gh pr view "$first_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
before_main_e2e_runs=$(
  gh run list \
    --workflow e2e.yml \
    --branch main \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId |
    jq '[.[].databaseId]'
)
gh workflow run e2e.yml --ref main -f tag=v0.11.3 -f scale=0.1

main_e2e_run=
for attempt in $(seq 1 60); do
  main_e2e_run=$(gh run list \
    --workflow e2e.yml \
    --branch main \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId,headBranch,headSha |
    jq -r \
      --arg sha "$first_merge_sha" \
      --arg ref main \
      --argjson before "$before_main_e2e_runs" '
        [.[]
         | .databaseId as $id
         | select(.headSha == $sha and .headBranch == $ref
                  and (($before | index($id)) == null))
         | $id]
        | first // empty
      ')
  test -n "$main_e2e_run" && break
  test "$attempt" -lt 60 || {
    echo "main e2e run did not appear" >&2
    exit 1
  }
  sleep 5
done

gh run watch "$main_e2e_run" --exit-status
test "$(gh run view "$main_e2e_run" --json conclusion --jq '.conclusion')" = "success"
main_e2e_jobs=$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$main_e2e_run/jobs?per_page=100")
printf '%s\n' "$main_e2e_jobs" | jq -e '
  [.jobs[] | select(.name == "e2e").steps[]
   | select(.name | startswith("Run the suite against "))
   | .conclusion] == ["success"]
  and
  [.jobs[] | select(.name == "e2e").steps[]
   | select(.name == "Empty the e2e namespaces")
   | .conclusion] == ["success"]
'
gh run view "$main_e2e_run" --json url,workflowName,event,headSha,conclusion
```

Expected: the E2E workflow runs from the exact first merge commit against v0.11.3 at scale 0.1.
The suite and `Empty the e2e namespaces` step both conclude `success`.
Inspect logs in place and record no cluster or runner details.

- [ ] **Step 5: Obtain approval to start the digest pull request**

Give the reviewer the first pull request URL and merge SHA, four first-merge workflow URLs and conclusions, builder tag and index digest, two-platform manifest result, and main e2e URL with suite and cleanup conclusions.

Expected: the reviewer approves the milestone and confirms that the digest came from the successful first-merge builder workflow.

**Stop and patch-forward rules:**

- If a first-merge workflow fails or is missing, fix forward through a reviewed pull request before continuing.
- If the builder publication fails or its index lacks either platform, keep the old internal digest and do not create the second branch.
- If main e2e or cleanup fails, do not create the second branch.
- Do not republish the builder tag by hand.

### Task 10: Pin the published builder digest through the second pull request

**Files:**

- Modify: `images/runner/Dockerfile`
- Pull request branch: `fix/issue-95-builder-digest`
- Pull request base: `main`
- Remote artifact: `ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder`

**Interfaces:**

- Consumes: the approved builder tag and literal multi-platform index digest from Task 9.
- Produces: a runner Dockerfile pinned to that exact tag and index digest.
- Produces: a merged digest-only pull request with local, CI, Dependency Review, mirror, and branch runner-smoke evidence.
- Produces: the final `main` commit for Task 11.

**Worker:** Builder digest implementation and GitHub workflow operator.

**Independent review gate:** A reviewer confirms the one-file diff, registry index, local gates, pull request checks, mirror, and runner smoke before merge.

- [ ] **Step 1: Re-read the builder index before creating the branch**

Run:

```sh
set -euo pipefail
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_merge_sha=$(gh pr view "$first_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
test "$(gh api repos/ydixken/pgcopydb-operator/commits/main --jq '.sha')" = "$first_merge_sha"
git fetch origin main
test "$(git rev-parse origin/main)" = "$first_merge_sha"

builder_tag=$(git show origin/main:images/pgcopydb-builder/Dockerfile |
  sed -n 's/^ARG PGCOPYDB_SHA=//p')
builder_ref="ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:$builder_tag"
builder_digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  digest "$builder_ref")
printf '%s\n' "$builder_digest" | rg '^sha256:[0-9a-f]{64}$'
go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  manifest "$builder_ref@$builder_digest" |
  jq -e '
    (.mediaType == "application/vnd.oci.image.index.v1+json"
     or .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json")
    and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/amd64") != null)
    and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/arm64") != null)
  '
printf '%s@%s\n' "$builder_ref" "$builder_digest"
```

Expected: this independent read returns the same tag and index digest approved in Task 9, with both expected platforms.
If it differs, stop before creating the branch.

- [ ] **Step 2: Create the exact second branch from the first merge**

Run:

```sh
set -euo pipefail
test -z "$(git status --porcelain)"
first_pr_url=$(gh pr view fix/issue-95-maintenance --json url --jq '.url')
first_merge_sha=$(gh pr view "$first_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
test "$(git rev-parse origin/main)" = "$first_merge_sha"
git switch --create fix/issue-95-builder-digest --no-track origin/main
test "$(git branch --show-current)" = "fix/issue-95-builder-digest"
test "$(git rev-parse HEAD)" = "$first_merge_sha"
```

Expected: the new branch starts at the exact first merge commit.
If the branch name already exists, stop and inspect it rather than overwriting it.

- [ ] **Step 3: Pin the literal published digest**

Use the `apply_patch` tool to replace only the digest in the `pgcopydb-builder` `FROM` line in `images/runner/Dockerfile`.
Keep the tag `e37d2bd4dd10b7ed7b415555ce3318202d9633cf`, `AS pgcopydb`, and every other byte in the file unchanged.
Write the literal `sha256:` value printed and approved in Step 1 into the Dockerfile.
Do not write a shell variable, placeholder, or floating tag into the file.

Then run:

```sh
set -euo pipefail
builder_tag=$(git show origin/main:images/pgcopydb-builder/Dockerfile |
  sed -n 's/^ARG PGCOPYDB_SHA=//p')
builder_ref="ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:$builder_tag"
builder_digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  digest "$builder_ref")
runner_pin=$(sed -n \
  's|^FROM ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:\([^ ]*\) AS pgcopydb$|\1|p' \
  images/runner/Dockerfile)
test "$runner_pin" = "$builder_tag@$builder_digest"
test "$(git diff --name-only | wc -l | tr -d ' ')" = 1
test "$(git diff --name-only)" = "images/runner/Dockerfile"
git diff -- images/runner/Dockerfile
```

Expected: the runner pin equals the registry tag and exact index digest, and no test expectation stores a digest that requires an edit.
Only `images/runner/Dockerfile` changes.

- [ ] **Step 4: Run local verification**

Run:

```sh
set -euo pipefail
git diff --check
GOTOOLCHAIN=go1.27.0 task lint
GOTOOLCHAIN=go1.27.0 task test
```

Expected: every command exits 0 without a skipped repository gate.

- [ ] **Step 5: Review and commit the one-file change**

Give the reviewer the Task 9 builder run, both independent registry reads, the one-file diff, and local command results.

After approval, run:

```sh
set -euo pipefail
git add images/runner/Dockerfile
test "$(git diff --cached --name-only)" = "images/runner/Dockerfile"
git diff --cached --check
git commit -m "chore(deps): pin refreshed pgcopydb builder"
test -z "$(git status --porcelain)"
```

Expected: one lint-clean commit contains only the published builder index pin.

- [ ] **Step 6: Push and open the digest pull request**

Run:

```sh
set -euo pipefail
git push --set-upstream origin fix/issue-95-builder-digest
second_pr_url=$(gh pr create \
  --base main \
  --head fix/issue-95-builder-digest \
  --title "chore(deps): pin refreshed pgcopydb builder" \
  --body-file - <<'EOF'
## Summary

- pin the pgcopydb builder index published from the issue 95 dependency refresh
- keep the builder tag and both target platforms unchanged

## Verification

- registry index contains linux/amd64 and linux/arm64
- `git diff --check`
- `task lint`
- `task test`
- required pull request CI and Dependency Review
- branch runner smoke
EOF
)
test -n "$second_pr_url"
printf '%s\n' "$second_pr_url"
```

Expected: the push and pull request creation succeed without force options.

- [ ] **Step 7: Require pull request CI and Dependency Review**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
second_head_sha=$(gh pr view "$second_pr_url" --json headRefOid --jq '.headRefOid')
gh pr checks "$second_pr_url" --watch --fail-fast
test "$(gh pr checks "$second_pr_url" --required --json bucket --jq 'all(.[]; .bucket == "pass")')" = "true"
test "$(gh pr checks "$second_pr_url" --json name,bucket \
  --jq '[.[] | select(.name == "lint" or .name == "test" or .name == "docs") | .bucket] | sort')" = '["pass","pass","pass"]'

ci_run_id=$(gh run list \
  --workflow ci.yml \
  --branch fix/issue-95-builder-digest \
  --event pull_request \
  --limit 20 \
  --json databaseId \
  --jq '.[0].databaseId')
test -n "$ci_run_id"
test "$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$ci_run_id" \
  --jq '.pull_requests[0].head.sha')" = "$second_head_sha"
test "$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$ci_run_id/jobs?per_page=100" \
  --jq '[.jobs[] | select(.name == "lint").steps[] | select(.name == "Review dependency changes") | .conclusion] | unique')" = '["success"]'
```

Expected: `lint`, `test`, and `docs` pass for the exact second pull request head.
Dependency Review runs once and concludes `success`.

- [ ] **Step 8: Dispatch and require the second branch runner smoke**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
second_head_sha=$(gh pr view "$second_pr_url" --json headRefOid --jq '.headRefOid')
before_second_smoke_runs=$(
  gh run list \
    --workflow runner-smoke.yml \
    --branch fix/issue-95-builder-digest \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId |
    jq '[.[].databaseId]'
)
gh workflow run runner-smoke.yml --ref fix/issue-95-builder-digest

second_smoke_run=
for attempt in $(seq 1 60); do
  second_smoke_run=$(gh run list \
    --workflow runner-smoke.yml \
    --branch fix/issue-95-builder-digest \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId,headBranch,headSha |
    jq -r \
      --arg sha "$second_head_sha" \
      --arg ref fix/issue-95-builder-digest \
      --argjson before "$before_second_smoke_runs" '
        [.[]
         | .databaseId as $id
         | select(.headSha == $sha and .headBranch == $ref
                  and (($before | index($id)) == null))
         | $id]
        | first // empty
      ')
  test -n "$second_smoke_run" && break
  test "$attempt" -lt 60 || {
    echo "second branch runner smoke did not appear" >&2
    exit 1
  }
  sleep 5
done

gh run watch "$second_smoke_run" --exit-status
test "$(gh run view "$second_smoke_run" --json conclusion --jq '.conclusion')" = "success"
test "$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$second_smoke_run/jobs?per_page=100" \
  --jq '[.jobs[].steps[] | select(.name == "Build both platforms without pushing") | .conclusion] | unique')" = '["success"]'
gh run view "$second_smoke_run" --json url,conclusion
```

Expected: the smoke run builds both configured platforms from the exact second pull request head without publishing.

- [ ] **Step 9: Confirm the mirror and obtain merge approval**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
second_head_sha=$(gh pr view "$second_pr_url" --json headRefOid --jq '.headRefOid')
mirror_run_id=$(gh run list \
  --workflow mirror-to-gitlab.yml \
  --commit "$second_head_sha" \
  --event push \
  --limit 20 \
  --json databaseId \
  --jq '.[0].databaseId')
test -n "$mirror_run_id"
gh run watch "$mirror_run_id" --exit-status
test "$(gh run view "$mirror_run_id" --json conclusion --jq '.conclusion')" = "success"
gh run view "$mirror_run_id" --json url,conclusion
```

Give the reviewer the one-file diff, exact registry index, local results, pull request checks, Dependency Review result, mirror URL, and runner-smoke URL.

Expected: the mirror succeeds and the reviewer approves the exact head for merge.

- [ ] **Step 10: Merge the exact second pull request head**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
second_head_sha=$(gh pr view "$second_pr_url" --json headRefOid --jq '.headRefOid')
gh pr merge "$second_pr_url" --squash --match-head-commit "$second_head_sha"

for attempt in $(seq 1 60); do
  second_pr_state=$(gh pr view "$second_pr_url" --json state --jq '.state')
  test "$second_pr_state" = "MERGED" && break
  test "$attempt" -lt 60 || {
    echo "second pull request did not merge" >&2
    exit 1
  }
  sleep 5
done

gh pr view "$second_pr_url" --json mergeCommit,url
```

Expected: GitHub merges the exact reviewed head through normal branch protection.
Do not use `--admin`, force-push, or delete the branch.

- [ ] **Step 11: Confirm final main CI, mirror, and exact pin**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
final_main_sha=$(gh pr view "$second_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
test "$(gh api repos/ydixken/pgcopydb-operator/commits/main --jq '.sha')" = "$final_main_sha"

for workflow in ci.yml mirror-to-gitlab.yml; do
  run_id=
  for attempt in $(seq 1 60); do
    run_id=$(gh run list \
      --workflow "$workflow" \
      --commit "$final_main_sha" \
      --event push \
      --limit 20 \
      --json databaseId \
      --jq '.[0].databaseId')
    test -n "$run_id" && break
    test "$attempt" -lt 60 || {
      echo "$workflow did not start for $final_main_sha" >&2
      exit 1
    }
    sleep 5
  done
  gh run watch "$run_id" --exit-status
  test "$(gh run view "$run_id" --json conclusion --jq '.conclusion')" = "success"
  gh run view "$run_id" --json url,workflowName,event,headSha,conclusion,jobs
done

git fetch origin main
test "$(git rev-parse origin/main)" = "$final_main_sha"
builder_tag=$(git show origin/main:images/pgcopydb-builder/Dockerfile |
  sed -n 's/^ARG PGCOPYDB_SHA=//p')
builder_ref="ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:$builder_tag"
builder_digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  digest "$builder_ref")
go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  manifest "$builder_ref@$builder_digest" |
  jq -e '
    (.mediaType == "application/vnd.oci.image.index.v1+json"
     or .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json")
    and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/amd64") != null)
    and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/arm64") != null)
  '
runner_pin=$(git show origin/main:images/runner/Dockerfile |
  sed -n 's|^FROM ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:\([^ ]*\) AS pgcopydb$|\1|p')
test "$runner_pin" = "$builder_tag@$builder_digest"
printf '%s@%s\n' "$builder_ref" "$builder_digest"
```

Expected: final `main` CI and mirror succeed.
The committed runner pin matches the registry's exact multi-platform index digest.

**Stop and patch-forward rules:**

- Keep the old digest if publication evidence changes or cannot be reproduced.
- Do not merge a second pull request with an unrelated file, failed check, skipped required check, or failed smoke build.
- Fix pre-merge failures on `fix/issue-95-builder-digest` with normal commits and rerun invalidated gates.
- Fix any post-merge failure through another reviewed pull request.
- Do not move or republish the builder tag by hand.

### Task 11: Publish metadata and validate the complete release chain

**Files:**

- Read: `.github/workflows/artifacthub-metadata.yml`
- Read: `.github/workflows/auto-release.yml`
- Read: `.github/workflows/e2e.yml`
- Read: `.github/workflows/release.yml`
- Read: `charts/pgcopydb-operator/Chart.yaml`
- Modify locally after all remote gates: `tasks/todo.md`

**Interfaces:**

- Consumes: the exact final `main` commit and builder pin from Task 10.
- Produces: a successful Artifact Hub metadata push.
- Produces: v0.12.0-rc.1 candidate artifacts, candidate e2e, candidate release notes, and gated promotion.
- Produces: v0.12.0 stable artifacts, stable e2e, mirror runs, and verified candidate, stable, and `latest` indexes.
- Produces: local public evidence for all nine workflows without an issue comment or a third pull request.

**Worker:** Release operator.

**Independent review gates:** A release reviewer approves the exact version calculation before auto-release dispatch.
A final reviewer approves all workflow, release, cleanup, mirror, and manifest evidence before completion.

**External effects:** This task pushes Artifact Hub metadata, creates public v0.12.0-rc.1 and v0.12.0 tags, publishes manager and runner images, publishes Helm charts, creates GitHub releases, moves stable `latest` tags, mirrors both release tags, and runs candidate and stable real-cluster e2e.
These are the release effects the user approved.

- [ ] **Step 1: Dispatch and require Artifact Hub metadata**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
final_main_sha=$(gh pr view "$second_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
test "$(gh api repos/ydixken/pgcopydb-operator/commits/main --jq '.sha')" = "$final_main_sha"

before_metadata_runs=$(
  gh run list \
    --workflow artifacthub-metadata.yml \
    --branch main \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId |
    jq '[.[].databaseId]'
)
gh workflow run artifacthub-metadata.yml --ref main

metadata_run=
for attempt in $(seq 1 60); do
  metadata_run=$(gh run list \
    --workflow artifacthub-metadata.yml \
    --branch main \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId,headBranch,headSha |
    jq -r \
      --arg sha "$final_main_sha" \
      --arg ref main \
      --argjson before "$before_metadata_runs" '
        [.[]
         | .databaseId as $id
         | select(.headSha == $sha and .headBranch == $ref
                  and (($before | index($id)) == null))
         | $id]
        | first // empty
      ')
  test -n "$metadata_run" && break
  test "$attempt" -lt 60 || {
    echo "Artifact Hub metadata run did not appear" >&2
    exit 1
  }
  sleep 5
done

gh run watch "$metadata_run" --exit-status
test "$(gh run view "$metadata_run" --json conclusion --jq '.conclusion')" = "success"
test "$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$metadata_run/jobs?per_page=100" \
  --jq '[.jobs[] | select(.name == "push").steps[] | select(.name == "Push repository metadata") | .conclusion]')" = '["success"]'
gh run view "$metadata_run" --json url,workflowName,event,headSha,conclusion
```

Expected: the `Artifact Hub metadata` workflow runs from the exact final `main` commit.
Its `push` job and `Push repository metadata` step succeed.
Because that named step invokes `oras`, its success also proves the preceding unnamed ORAS setup step worked.

- [ ] **Step 2: Calculate the candidate from the workflow's own script**

The repository has no separate release-version helper.
Extract the actual `Tag the next release candidate` step and execute it only through the `tag=` assignment, before its `git config`, `git tag`, and `git push` side effects.

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
final_main_sha=$(gh pr view "$second_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
git fetch origin main --tags
test "$(git rev-parse origin/main)" = "$final_main_sha"

original_branch=$(git branch --show-current)
test -n "$original_branch"
test -z "$(git status --porcelain)"
trap 'git switch "$original_branch" >/dev/null' EXIT
git switch --detach "$final_main_sha"

release_step=$(yq -r \
  '.jobs.candidate.steps[] | select(.name == "Tag the next release candidate") | .run' \
  .github/workflows/auto-release.yml)
test -n "$release_step"
safe_release_step=$(printf '%s\n' "$release_step" |
  sed '/^[[:space:]]*git config user.name /,$d')
printf '%s\n' "$safe_release_step" | rg '^tag="v\$major\.\$minor\.\$patch-rc\.\$n"$'
printf '%s\n' "$safe_release_step" |
  awk '/^[[:space:]]*git (config|tag|push) / { found = 1 } END { exit found }'

candidate=$(
  {
    printf '%s\n' "$safe_release_step"
    printf '%s\n' 'printf "%s\n" "$tag"'
  } | GITHUB_STEP_SUMMARY=/dev/null bash
)
latest=$(git describe --tags --abbrev=0 --match 'v*' --exclude '*-*')

git switch "$original_branch"
trap - EXIT

test "$latest" = "v0.11.3"
test "$candidate" = "v0.12.0-rc.1"
for absent_ref in "refs/tags/$candidate" refs/tags/v0.12.0; do
  if remote_ref=$(git ls-remote --exit-code --tags origin "$absent_ref"); then
    printf 'tag already exists: %s\n' "$absent_ref" >&2
    exit 1
  else
    ls_remote_exit=$?
    test "$ls_remote_exit" -eq 2
    test -z "$remote_ref"
  fi
done
release_tags=$(gh release list --limit 1000 --json tagName | jq -r '.[].tagName')
printf '%s\n' "$release_tags" |
  awk -v candidate="$candidate" \
    '$0 == candidate || $0 == "v0.12.0" { found = 1 } END { exit found }'
printf '%s -> %s\n' "$latest" "$candidate"
```

Expected: the exact workflow logic reads v0.11.3 as the last stable tag and selects v0.12.0-rc.1.
Neither the candidate nor stable tag or release exists.
The worktree returns to `fix/issue-95-builder-digest` cleanly.

If the extracted step shape changes, the calculated version differs, or either tag exists, stop before publishing and reconcile the public history with `auto-release.yml`.

- [ ] **Step 3: Obtain pre-release approval**

Give the release reviewer the final `main` SHA, Artifact Hub URL, extracted safe workflow script, last stable tag, selected candidate, public tag absence, and release absence.

Expected: the reviewer confirms v0.12.0-rc.1 and approves dispatch against the same final `main` commit.

- [ ] **Step 4: Dispatch auto-release and confirm the candidate tag**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
final_main_sha=$(gh pr view "$second_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
before_auto_release_runs=$(
  gh run list \
    --workflow auto-release.yml \
    --branch main \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId |
    jq '[.[].databaseId]'
)
gh workflow run auto-release.yml --ref main

auto_release_run=
for attempt in $(seq 1 60); do
  auto_release_run=$(gh run list \
    --workflow auto-release.yml \
    --branch main \
    --event workflow_dispatch \
    --limit 100 \
    --json databaseId,headBranch,headSha |
    jq -r \
      --arg sha "$final_main_sha" \
      --arg ref main \
      --argjson before "$before_auto_release_runs" '
        [.[]
         | .databaseId as $id
         | select(.headSha == $sha and .headBranch == $ref
                  and (($before | index($id)) == null))
         | $id]
        | first // empty
      ')
  test -n "$auto_release_run" && break
  test "$attempt" -lt 60 || {
    echo "auto-release run did not appear" >&2
    exit 1
  }
  sleep 5
done

gh run watch "$auto_release_run" --exit-status
test "$(gh run view "$auto_release_run" --json conclusion --jq '.conclusion')" = "success"
test "$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$auto_release_run/jobs?per_page=100" \
  --jq '[.jobs[] | select(.name == "candidate").steps[] | select(.name == "Tag the next release candidate") | .conclusion]')" = '["success"]'

candidate_target=$(git ls-remote --exit-code origin 'refs/tags/v0.12.0-rc.1^{}' |
  awk 'NR == 1 { print $1 }')
test "$candidate_target" = "$final_main_sha"
gh run view "$auto_release_run" --json url,workflowName,event,headSha,conclusion
```

Expected: the `Auto release` candidate job succeeds and its annotated v0.12.0-rc.1 tag peels to the exact final `main` commit.

- [ ] **Step 5: Require the candidate release workflow and promotion ordering**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
final_main_sha=$(gh pr view "$second_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
candidate_release_run=
for attempt in $(seq 1 120); do
  candidate_release_run=$(gh run list \
    --workflow release.yml \
    --branch v0.12.0-rc.1 \
    --event push \
    --limit 20 \
    --json databaseId,headBranch,headSha |
    jq -r --arg sha "$final_main_sha" --arg ref v0.12.0-rc.1 '
      [.[] | select(.headSha == $sha and .headBranch == $ref) | .databaseId]
      | first // empty
    ')
  test -n "$candidate_release_run" && break
  test "$attempt" -lt 120 || {
    echo "candidate release run did not appear" >&2
    exit 1
  }
  sleep 5
done

gh run watch "$candidate_release_run" --exit-status
test "$(gh run view "$candidate_release_run" --json conclusion --jq '.conclusion')" = "success"
candidate_jobs=$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$candidate_release_run/jobs?per_page=100")

printf '%s\n' "$candidate_jobs" | jq -e '
  ([.jobs[] | select(.conclusion == "success") | .name] | sort)
    == ["chart", "e2e", "manager-image", "promote", "release-notes", "runner-image"]
  and
  ([.jobs[] | select(.name == "e2e-failed") | .conclusion] == ["skipped"])
'

printf '%s\n' "$candidate_jobs" | jq -e '
  ([.jobs[] | select(.name == "chart") | .completed_at][0]) as $chart_done
  | ([.jobs[] | select(.name == "e2e") | .started_at][0]) as $e2e_start
  | ([.jobs[] | select(.name == "release-notes") | .started_at][0]) as $notes_start
  | ([.jobs[] | select(.name == "e2e") | .completed_at][0]) as $e2e_done
  | ([.jobs[] | select(.name == "release-notes") | .completed_at][0]) as $notes_done
  | ([.jobs[] | select(.name == "promote") | .started_at][0]) as $promote_start
  | $e2e_start >= $chart_done
    and $notes_start >= $chart_done
    and $promote_start >= $e2e_done
    and $promote_start >= $notes_done
'

printf '%s\n' "$candidate_jobs" | jq -e '
  [.jobs[] | select(.name == "e2e").steps[]
   | select(.name == "Run the suite against the published candidate")
   | .conclusion] == ["success"]
  and
  [.jobs[] | select(.name == "e2e").steps[]
   | select(.name == "Empty the e2e namespaces")
   | .conclusion] == ["success"]
  and
  [.jobs[] | select(.name == "release-notes").steps[]
   | select(.name == "Create the GitHub release")
   | .conclusion] == ["success"]
  and
  [.jobs[] | select(.name == "promote").steps[]
   | select(.name == "Push the stable tag")
   | .conclusion] == ["success"]
'

gh run view "$candidate_release_run" --json url,workflowName,event,headSha,conclusion
```

Expected: candidate images, chart, release notes, e2e, cleanup, and promotion succeed.
The `e2e` and `release-notes` jobs both start after `chart`, with no dependency between them.
`promote` starts only after both complete successfully.

- [ ] **Step 6: Verify candidate artifacts and indexes**

Run:

```sh
set -euo pipefail
test "$(gh release view v0.12.0-rc.1 --json isPrerelease --jq '.isPrerelease')" = "true"
gh release view v0.12.0-rc.1 --json tagName,url,isPrerelease

candidate_chart=$(helm show chart \
  oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
  --version 0.12.0-rc.1)
printf '%s\n' "$candidate_chart" | rg '^version: 0\.12\.0-rc\.1$'
printf '%s\n' "$candidate_chart" | rg '^appVersion: v0\.12\.0-rc\.1$'

for image in \
  ghcr.io/ydixken/pgcopydb-operator:v0.12.0-rc.1 \
  ghcr.io/ydixken/pgcopydb-operator/runner:v0.12.0-rc.1; do
  digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 digest "$image")
  go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
    manifest "$image@$digest" |
    jq -e '
      (.mediaType == "application/vnd.oci.image.index.v1+json"
       or .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json")
      and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/amd64") != null)
      and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/arm64") != null)
    '
  printf '%s@%s\n' "$image" "$digest"
done
```

Expected: GitHub marks v0.12.0-rc.1 as a prerelease.
The chart carries candidate `version` and `appVersion` values.
Both candidate image indexes contain linux/amd64 and linux/arm64.

- [ ] **Step 7: Locate and require both stable workflows**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
final_main_sha=$(gh pr view "$second_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
stable_target=$(git ls-remote --exit-code origin 'refs/tags/v0.12.0^{}' |
  awk 'NR == 1 { print $1 }')
test "$stable_target" = "$final_main_sha"

stable_release_run=
stable_e2e_run=
for attempt in $(seq 1 120); do
  stable_release_run=$(gh run list \
    --workflow release.yml \
    --branch v0.12.0 \
    --event push \
    --limit 20 \
    --json databaseId,headBranch,headSha |
    jq -r --arg sha "$final_main_sha" --arg ref v0.12.0 '
      [.[] | select(.headSha == $sha and .headBranch == $ref) | .databaseId]
      | first // empty
    ')
  stable_e2e_run=$(gh run list \
    --workflow e2e.yml \
    --branch v0.12.0 \
    --event push \
    --limit 20 \
    --json databaseId,headBranch,headSha |
    jq -r --arg sha "$final_main_sha" --arg ref v0.12.0 '
      [.[] | select(.headSha == $sha and .headBranch == $ref) | .databaseId]
      | first // empty
    ')
  test -n "$stable_release_run" && test -n "$stable_e2e_run" && break
  test "$attempt" -lt 120 || {
    echo "stable release or stable e2e run did not appear" >&2
    exit 1
  }
  sleep 5
done

gh run watch "$stable_release_run" --exit-status
gh run watch "$stable_e2e_run" --exit-status
test "$(gh run view "$stable_release_run" --json conclusion --jq '.conclusion')" = "success"
test "$(gh run view "$stable_e2e_run" --json conclusion --jq '.conclusion')" = "success"
```

Expected: promotion creates v0.12.0 at the exact final `main` commit.
The stable `Release` and stable-tag `E2E` workflows both conclude `success`.

- [ ] **Step 8: Verify stable jobs, release, chart, and cleanup**

Run:

```sh
set -euo pipefail
stable_release_jobs=$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$stable_release_run/jobs?per_page=100")
printf '%s\n' "$stable_release_jobs" | jq -e '
  ([.jobs[] | select(.conclusion == "success") | .name] | sort)
    == ["chart", "manager-image", "release-notes", "runner-image"]
  and
  ([.jobs[] | select(.conclusion == "skipped") | .name] | sort)
    == ["e2e", "e2e-failed", "promote"]
'

stable_e2e_jobs=$(gh api \
  "repos/ydixken/pgcopydb-operator/actions/runs/$stable_e2e_run/jobs?per_page=100")
printf '%s\n' "$stable_e2e_jobs" | jq -e '
  [.jobs[] | select(.name == "e2e").steps[]
   | select(.name | startswith("Run the suite against "))
   | .conclusion] == ["success"]
  and
  [.jobs[] | select(.name == "e2e").steps[]
   | select(.name == "Empty the e2e namespaces")
   | .conclusion] == ["success"]
'

test "$(gh release view v0.12.0 --json isPrerelease --jq '.isPrerelease')" = "false"
gh release view v0.12.0 --json tagName,url,isPrerelease

stable_chart=$(helm show chart \
  oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
  --version 0.12.0)
printf '%s\n' "$stable_chart" | rg '^version: 0\.12\.0$'
printf '%s\n' "$stable_chart" | rg '^appVersion: v0\.12\.0$'

gh run view "$stable_release_run" --json url,workflowName,event,headSha,conclusion
gh run view "$stable_e2e_run" --json url,workflowName,event,headSha,conclusion
```

Expected: the stable release publishes its four intended jobs while candidate-only jobs skip.
The separate stable e2e suite and cleanup pass.
GitHub and the chart report stable v0.12.0.

- [ ] **Step 9: Verify stable and latest image indexes**

Run:

```sh
set -euo pipefail
for image in \
  ghcr.io/ydixken/pgcopydb-operator:v0.12.0 \
  ghcr.io/ydixken/pgcopydb-operator:latest \
  ghcr.io/ydixken/pgcopydb-operator/runner:v0.12.0 \
  ghcr.io/ydixken/pgcopydb-operator/runner:latest; do
  digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 digest "$image")
  go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
    manifest "$image@$digest" |
    jq -e '
      (.mediaType == "application/vnd.oci.image.index.v1+json"
       or .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json")
      and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/amd64") != null)
      and ([.manifests[].platform | "\(.os)/\(.architecture)"] | index("linux/arm64") != null)
    '
  printf '%s@%s\n' "$image" "$digest"
done

manager_stable_digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  digest ghcr.io/ydixken/pgcopydb-operator:v0.12.0)
manager_latest_digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  digest ghcr.io/ydixken/pgcopydb-operator:latest)
runner_stable_digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  digest ghcr.io/ydixken/pgcopydb-operator/runner:v0.12.0)
runner_latest_digest=$(go run github.com/google/go-containerregistry/cmd/crane@v0.20.6 \
  digest ghcr.io/ydixken/pgcopydb-operator/runner:latest)
test "$manager_latest_digest" = "$manager_stable_digest"
test "$runner_latest_digest" = "$runner_stable_digest"
```

Expected: every stable and `latest` reference is a two-platform index.
Stable and `latest` resolve to the same manager digest and the same runner digest.

- [ ] **Step 10: Confirm candidate and stable mirrors**

Run:

```sh
set -euo pipefail
second_pr_url=$(gh pr view fix/issue-95-builder-digest --json url --jq '.url')
final_main_sha=$(gh pr view "$second_pr_url" --json mergeCommit --jq '.mergeCommit.oid')
for tag in v0.12.0-rc.1 v0.12.0; do
  mirror_run=
  for attempt in $(seq 1 60); do
    mirror_run=$(gh run list \
      --workflow mirror-to-gitlab.yml \
      --branch "$tag" \
      --event push \
      --limit 20 \
      --json databaseId,headBranch,headSha |
      jq -r --arg sha "$final_main_sha" --arg ref "$tag" '
        [.[] | select(.headSha == $sha and .headBranch == $ref) | .databaseId]
        | first // empty
      ')
    test -n "$mirror_run" && break
    test "$attempt" -lt 60 || {
      echo "mirror run missing for $tag" >&2
      exit 1
    }
    sleep 5
  done
  gh run watch "$mirror_run" --exit-status
  test "$(gh run view "$mirror_run" --json conclusion --jq '.conclusion')" = "success"
  gh run view "$mirror_run" --json url,workflowName,event,headSha,conclusion
done
```

Expected: mirror runs for v0.12.0-rc.1 and v0.12.0 both conclude `success` at the exact release commit.

- [ ] **Step 11: Obtain final independent review for all nine workflows**

Collect the public URLs and evidence for:

```text
artifacthub-metadata.yml
auto-release.yml
ci.yml
docs.yml
e2e.yml
mirror-to-gitlab.yml
pgcopydb-builder.yml
release.yml
runner-smoke.yml
```

Give the final reviewer:

- both pull request URLs, reviewed heads, and merge SHAs;
- local inventory, vulnerability, license, lint, and test conclusions;
- Dependency Review results for both pull requests;
- feature-branch e2e rejection evidence and both runner-smoke URLs;
- first and final `main` CI, docs, mirror, builder, metadata, and main e2e URLs;
- auto-release, candidate release, stable release, and stable e2e URLs;
- candidate and stable cleanup conclusions;
- candidate, stable, and `latest` platform and digest evidence;
- candidate and stable GitHub release URLs and tag targets;
- candidate and stable mirror URLs.

Expected: each workflow has a successful intended run or, only for the feature-branch e2e dispatch, the designed environment rejection before runner assignment.
An absent run does not count as success.
The reviewer confirms the exact version history, promotion ordering, cleanup, manifest platforms, and absence of sensitive information.

- [ ] **Step 12: Finish the local review record**

Use the `apply_patch` tool to mark only verified items complete in `tasks/todo.md` and add the public URLs and non-sensitive conclusions to `## Review`.
Do not post an issue comment, edit Renovate's dashboard, or create a third pull request only for task status.

Run:

```sh
git status --short --branch
```

Expected: only `tasks/todo.md` may differ after final evidence is recorded.
Leave the requested worktree and both local and remote branches in place unless the user separately asks for cleanup.

**Stop and patch-forward rules:**

- If Artifact Hub metadata fails, fix it before auto-release.
- If v0.12.0-rc.1 is not the next candidate selected by the workflow's own script, stop before dispatch.
- If a candidate image, chart, GitHub prerelease, release note, e2e suite, or cleanup step fails, do not promote.
- Preserve failed candidate tags, prereleases, and artifacts.
- Fix forward on `main` through a reviewed pull request, then let auto-release select the next unused candidate number.
- If `promote` starts before candidate e2e and release notes both succeed, cancel the run and fix the dependency gate before a stable tag is pushed.
- If stable publication, stable e2e, cleanup, mirror, or a multi-platform manifest check fails, stop further release work and report the public run.
- Fix forward without deleting, moving, or reusing v0.12.0.
- Never force-push `main`, delete a published release, replace a published tag, bypass branch protection, weaken the e2e environment, expose secrets, or treat an absent result as success.
