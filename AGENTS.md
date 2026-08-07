# AGENTS.md

Operating guide for AI agents and humans working in this repository. The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119.

[TOC]

## Caution

- Agents MUST NOT push commits, or open or merge pull requests, unless the user asks.
- Agents MAY read and reference secret names (CI variables, kubeconfig paths), but MUST NOT read, print, or set their values.
- `task e2e` runs against a **real cluster**: whatever `kubectl config current-context` points at. Check the context before running, and never bypass the confirmation prompt (`task --yes` is forbidden for this target).

## Mandatory skills

Both skills are vendored in this repo and are always-on, not optional, not per-request:

- **ponytail** ([`.claude/skills/ponytail/SKILL.md`](.claude/skills/ponytail/SKILL.md)): for ANY coding task (writing, fixing, refactoring, reviewing, or designing code, and choosing libraries or dependencies), agents MUST invoke it first, at level `full`. Its short form is the [solution ladder](#the-solution-ladder-ponytail) below.
- **humanizer** ([`.claude/skills/humanizer/SKILL.md`](.claude/skills/humanizer/SKILL.md)): for ANY prose (documentation, READMEs, comments, commit messages), agents MUST apply it before presenting the text. No AI slop in writing.

## Writing conventions

- Em dashes MUST NOT be used. Not in docs, not in code comments, not in commit messages. Use a comma, colon, period, or parentheses instead.
- Docs are English, concise, with RFC 2119 keywords for normative rules.

## What this repo is

pgcopydb-operator will be a Go Kubernetes operator that automates PostgreSQL clone/migration workflows using [pgcopydb](https://github.com/dimitri/pgcopydb). Right now it is a **harness only**: guidelines, task runner, and CI. No Go code yet. See [PROGRESS.md](PROGRESS.md) for milestones and the decision log.

Development happens on **GitHub** (`ydixken/pgcopydb-operator`, PRs there). GitLab (`gitlab.com/ydixken/pgcopydb-operator`) is a push mirror that runs CI; nobody commits or opens MRs on GitLab.

## Common commands

| Command     | Does                                                                                                        |
|-------------|-------------------------------------------------------------------------------------------------------------|
| `task help` | List all tasks.                                                                                              |
| `task lint` | yamllint always; golangci-lint once `go.mod` exists (skips with a message before that).                      |
| `task test` | Unit tests via kubebuilder's `make test` once scaffolded (skips with a message before that).                 |
| `task e2e`  | E2e tests against the current kubectl context. Local only, never CI. Prompts for confirmation; see Caution.  |

## Architecture key points

- The operator will be scaffolded with **kubebuilder** (go/v4 layout: `cmd/`, `api/`, `internal/controller/`, `config/`). Do not pre-create any of it.
- File ownership until the scaffold lands: kubebuilder owns `go.mod`, `Makefile`, `Dockerfile`, `PROJECT`, and `.golangci.yml`. These MUST NOT be created by hand. The harness owns `.gitignore` and `README.md`; the H1 scaffold procedure in [PROGRESS.md](PROGRESS.md) explains how they survive `kubebuilder init`.
- CI ([.gitlab-ci.yml](.gitlab-ci.yml)) runs **branch pipelines only**, because merge-request rules never fire on a push mirror. Jobs gate on the files they need (`rules:exists`), so the pipeline is green today and starts linting, testing, and building images automatically in the commit the scaffold lands.
- E2e tests are **local only** by decision: they target the dev cluster through the developer's own kubeconfig context. There is deliberately no e2e CI job yet.

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
- [PROGRESS.md](PROGRESS.md) for milestones and the decision log.
- [README.md](README.md) for repo purpose and structure.
