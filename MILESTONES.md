# PROGRESS.md

Status: **H0 done**. Harness in place, no Go code.

[TOC]

## Milestones

### H0: Repository harness (done)

- [x] Agent guides (AGENTS.md, CLAUDE.md pointer), vendored ponytail and humanizer skills
- [x] Taskfile (`lint`, `test`, `e2e` with context guard), yamllint
- [x] GitLab CI: branch pipelines, self-activating jobs, digest-pinned images
- [x] CONTRIBUTING, README, this file

### H1: Kubebuilder scaffold

- [ ] `mv README.md README.md.harness && mv .gitignore .gitignore.harness`, run `kubebuilder init` (go/v4), then: keep the harness README (discard kubebuilder's), diff-union the .gitignore (expected: no-op, kubebuilder's entries are pre-merged).
- [ ] Delete `.devcontainer/` and the generated `.github/workflows/{lint,test,test-e2e}.yml`. CI lives on GitLab.
- [ ] Patch the generated Dockerfile: pin both `FROM` images by tag AND sha256 digest (builder `golang`, final `gcr.io/distroless/static:nonroot`, whose floating tag makes the digest the real pin). Nothing else changes.
- [ ] Adapt the generated `test/e2e/` from kind to current-context targeting (`task e2e` contract).
- [ ] Verify: `lint:go`, `test:unit`, and (on main) `docker-build` turn green in CI without any CI edit.

### H2: First CRD and controller

### H3: Real e2e suite against the dev cluster

## Decision log

| Date       | Decision                                                                        | Rationale                                                                                                                                            |
|------------|---------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2026-08-07 | GitHub is primary; GitLab is a push mirror running CI, branch pipelines only    | The mirror pushes branches and opens no MRs, so merge_request_event rules would never fire.                                                          |
| 2026-08-07 | Taskfile, not Makefile                                                          | Kubebuilder owns the Makefile it generates; a pristine Makefile keeps upstream diffs clean. Taskfile never collides and stays the entrypoint.         |
| 2026-08-07 | `go.mod`, `Makefile`, `Dockerfile`, `PROJECT`, `.golangci.yml` left to kubebuilder | They arrive correct and together with the code they serve; hand-made versions would make `kubebuilder init` error.                                   |
| 2026-08-07 | No Dockerfile in the harness                                                    | It cannot build without code, and the CI job gates on its existence, activating the commit the scaffold lands. Digest-pin patch tracked in H1.     |
| 2026-08-07 | `.gitignore` and `README.md` are harness-owned                                  | Needed now for secret hygiene and orientation; H1 documents the move-aside so `kubebuilder init` succeeds.                                           |
| 2026-08-07 | CI jobs self-activate via `rules:exists`                                        | The pipeline is green on the bare harness and needs no edit when Go code lands.                                                                      |
| 2026-08-07 | E2e tests are local only, against the developer's current kubectl context       | No CI cluster credentials yet; `task e2e` shows the context and prompts before touching a real cluster.                                              |
| 2026-08-07 | yamllint yes, markdownlint no, `.editorconfig` no                               | YAML is the harness's executable content; prose linting and editor config fix nothing that breaks (solution ladder, rung 1).                         |
| 2026-08-07 | Ponytail vendored into `.claude/skills/ponytail/` and made mandatory            | Always-on for every contributor with no per-user install, pinned to a known version: github.com/DietrichGebert/ponytail @ `16f29800fd26` (MIT).      |
| 2026-08-07 | Humanizer vendored into `.claude/skills/humanizer/` and made mandatory for prose | No AI slop in writing, same vendoring rationale as ponytail: github.com/blader/humanizer v2.8.2 @ `1b48564` (MIT).                                    |
| 2026-08-07 | Em dashes are forbidden everywhere (docs, comments, commit messages)            | Hard requirement from the maintainer; also a known AI-writing tell. Enforced via AGENTS.md writing conventions.                                       |
| 2026-08-07 | Brainstorming vendored into `.claude/skills/brainstorming/` and made mandatory for creative work | Design before implementation, same vendoring rationale: superpowers 6.2.0 by obra (MIT), full skill directory copied verbatim.                        |
| 2026-08-07 | Renovate config named `.renovaterc.json`                                        | `renovate.json` (lowercase, non-dotfile) would make kubebuilder's directory check reject the repo at H1.                                             |
| 2026-08-07 | `.golangci.yml` accepted verbatim from kubebuilder at H1                        | Its v2 config is the standard linter set. Any later suppression MUST carry an inline comment explaining why.                                          |
