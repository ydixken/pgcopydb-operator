# Issue 95 maintenance plan

- [x] Create an isolated worktree and record the clean baseline.
- [x] Read issue 95, the repository docs, and the affected code and workflows.
- [x] Present a minimal design with trade-offs and get approval before implementation.
- [x] Write the approved design to `docs/superpowers/specs/2026-08-31-issue-95-dependency-refresh-design.md`.
- [x] Self-review the written spec for placeholders, contradictions, scope, ambiguity, and prohibited dashes.
- [ ] Get the user's review of the written spec before planning implementation.
- [ ] Add a deterministic check for each planned behavior or dependency change.
- [ ] Fix the root causes, add tests, and update affected documentation.
- [ ] Bump Go and existing dependencies after compatibility, license, and security checks.
- [ ] Verify the GitHub Actions workflows, including CI, release, auto-release, and branch-safe trigger behavior.
- [ ] Confirm the authorized candidate and stable real-cluster e2e gates, then verify their cleanup steps.
- [ ] Delegate independent code review and lint, unit, workflow, release, and e2e verification.
- [ ] Push the test branch and confirm every required GitHub Actions check before integration.
- [ ] Record final command, CI, release, and e2e evidence in the review below.

## Review

- Worktree setup is complete at commit `fcbc56dc813f9e026650bffd984e86c7d604241d`.
- `go mod download` exited 0 with Go 1.27.0 on Darwin arm64.
- `task lint` exited 0 after YAML, chart, documentation link, Prometheus rule, and Go lint checks.
- `task test` exited 0 after all 13 non-e2e Go packages passed.
- E2e was not run during setup because it targets the current real Kubernetes cluster.
