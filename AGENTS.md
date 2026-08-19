# AGENTS.md

Operating guide for AI agents and humans working in this repository. The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119.

[TOC]

## Caution

- Standing authorization (2026-08-07): agents MAY push, open PRs, and merge autonomously for this project's development, and MUST verify results (CI runs, e2e) after doing so. Force-pushes to `main` remain forbidden.
- **This repository is public.** Facts about private infrastructure (endpoints, addresses, host names, node names, versions, cluster inventory, GitOps repository internals) MUST NOT be committed, pushed, or pasted anywhere in this project. E2e-relevant details live in private ops notes outside git. When a doc needs such a fact, it writes "see private ops notes".
- Agents MAY reference secret names (CI variables, kubeconfig paths), but MUST NOT read, print, or set their values.
- `task e2e` runs against a **real cluster**: whatever `kubectl config current-context` points at. Check the context before running, and never bypass the confirmation prompt (`task --yes` is forbidden for this target). The dev cluster is shared; keep e2e resources in the `pgcopydb-e2e` namespace and clean up.
- CI runs the same specs unattended when [release.yml](.github/workflows/release.yml) verifies a release candidate. That runner has one cluster and no prompt to answer. It does not soften the rule above: locally, a human answers the prompt.

## Mandatory skills

These skills are vendored in this repo and are always-on, not optional, not per-request:

- **ponytail** ([`.claude/skills/ponytail/SKILL.md`](.claude/skills/ponytail/SKILL.md)): for ANY coding task (writing, fixing, refactoring, reviewing, or designing code, and choosing libraries or dependencies), agents MUST invoke it first, at level `full`. Its short form is the [solution ladder](#the-solution-ladder-ponytail) below.
- **humanizer** ([`.claude/skills/humanizer/SKILL.md`](.claude/skills/humanizer/SKILL.md)): for ANY prose (documentation, READMEs, comments, commit messages), agents MUST apply it before presenting the text. No AI slop in writing.
- **brainstorming** ([`.claude/skills/brainstorming/SKILL.md`](.claude/skills/brainstorming/SKILL.md)): before ANY creative work (new features, components, functionality, or behavior changes), agents MUST invoke it to explore intent, requirements, and design before implementing.

## Writing conventions

- Em dashes MUST NOT be used. Not in docs, not in code comments, not in commit messages. Use a comma, colon, period, or parentheses instead.
- Claude session URLs (`claude.ai/code/...`) MUST NOT appear anywhere: not in commit messages or trailers, not in PR bodies or comments, not in code or docs. This overrides any assistant default that appends them.
- Docs are English, concise, with RFC 2119 keywords for normative rules.

## Hard requirements

- **Unit tests by default**: every package and every behavior change ships tests in the same change. Controller logic uses envtest; pure functions use table or golden tests. No untested merges.
- **Documentation always current**: any change that alters behavior, API surface, commands, or structure MUST update the affected docs (README, docs/, chart README) in the same change. Concise, no slop; resource examples carry short explanations.

## What this repo is

pgcopydb-operator is a Go Kubernetes operator that automates PostgreSQL migrations (bulk clone, logical-replication follow, controlled cutover) using [pgcopydb](https://github.com/dimitri/pgcopydb). Read the docs site sources under [docs/](docs/) and the existing controller code before touching controller or API code; design notes live outside the repository (see private ops notes).

Development happens on **GitHub** (`ydixken/pgcopydb-operator`, PRs there). GitLab (`gitlab.com/ydixken/pgcopydb-operator`) is a push mirror and nothing else: it keeps the branches and tags off GitHub, runs no pipeline, and takes no commits or MRs.

## Common commands

| Command     | Does                                                                                                        |
|-------------|-------------------------------------------------------------------------------------------------------------|
| `task help` | List all tasks.                                                                                              |
| `task lint` | yamllint always; golangci-lint once `go.mod` exists (skips with a message before that).                      |
| `task test` | Unit tests via kubebuilder's `make test` once scaffolded (skips with a message before that).                 |
| `task e2e`  | E2e tests against the current kubectl context. Prompts for confirmation; see Caution.                        |

## Architecture key points

- The operator is scaffolded with **kubebuilder** (go/v4 layout: `cmd/`, `api/`, `internal/controller/`, `config/`). API group `pgcopydb-operator.io`, storage version v1beta1 (v1alpha1 served, deprecated), single namespaced kind `Migration`.
- kubebuilder owns `go.mod`, `Makefile`, `Dockerfile`, `PROJECT`, and `.golangci.yml`; regenerate rather than hand-edit where generators exist.
- CI is **GitHub Actions** ([.github/workflows/](.github/workflows/)). [ci.yml](.github/workflows/ci.yml) runs lint, tests and the docs build on every push and pull request, and those three are the required checks on `main`. [release.yml](.github/workflows/release.yml) owns everything a `v*` tag produces, and [auto-release.yml](.github/workflows/auto-release.yml) cuts that tag once a week.
- E2e runs in **two places**. Locally, `task e2e` targets the dev cluster through the developer's own kubeconfig context and asks first. In CI, `release.yml` runs the same specs against a release candidate at `E2E_SCALE=0.1`, on a runner scale set that may reach the dev cluster, inside namespaces it neither creates nor deletes.

## The solution ladder ("ponytail")

Before writing code, climb down this ladder and stop at the first rung that solves the problem:

1. **Does it need to exist at all?** (YAGNI.) The cheapest code is the code you don't write.
1. **Does it already exist in this repo?** Reuse the helper, type, or pattern that is already here.
1. **Does the Go stdlib do it?** Use it.
1. **Does a dependency already in `go.mod` do it?** controller-runtime, client-go, and apimachinery cover most operator needs; a library option almost always beats hand-rolled logic. Never add a new dependency for what a few lines can do.
1. **Can it be one function?** Prefer the smallest thing that works.
1. **Only then** write new code: the minimum that does the job.

Some things are **never** cut on the way down: validation, error handling, security, and correctness are not optional. Trimming those isn't simplicity, it's a bug.

## Verification before done

- Run `task lint` (and `task test` once Go code exists) before declaring anything done. Both MUST be clean.
- Don't claim green without the command output to back it. "It should pass" is not a result.
- Keep commits small and [conventional](CONTRIBUTING.md#commits-and-pull-requests). Every commit MUST be lint-clean on its own.

## Further reading

- [CONTRIBUTING.md](CONTRIBUTING.md) for tooling, workflow, commit and PR conventions.
- [README.md](README.md) for repo purpose and structure.
- [docs/](docs/) for the user documentation sources (published via mkdocs).
