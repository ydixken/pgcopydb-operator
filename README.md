# pgcopydb-operator

A Kubernetes operator that automates PostgreSQL clone and migration workflows using [pgcopydb](https://github.com/dimitri/pgcopydb).

> [!important]
> This repository is a **harness only**: guidelines, task runner, and CI. There is no operator code yet. See [PROGRESS.md](PROGRESS.md) for milestones and the decision log.

## Structure

```text
.claude/skills/            # vendored skills (ponytail, humanizer, brainstorming), mandatory per AGENTS.md
.github/workflows/         # push mirror GitHub -> GitLab (CI runs on the mirror)
.gitlab-ci.yml             # branch pipelines: yamllint now; Go lint/test/image build self-activate with the scaffold
.renovaterc.json           # keeps the tag+digest image pins current, once Renovate is enabled
.yamllint.yml              # yamllint policy, every deviation commented
AGENTS.md                  # single source of truth for agents (CLAUDE.md points here)
CONTRIBUTING.md            # tooling, workflow, commit and PR conventions
PROGRESS.md                # milestones and decision log
Taskfile.yml               # task help | lint | test | e2e
```

## Quickstart

```sh
task help   # list tasks
task lint   # the pre-commit gate
task e2e    # e2e tests against your CURRENT kubectl context (local only; prompts first)
```

## Documentation

- [CONTRIBUTING.md](CONTRIBUTING.md): how to work here.
- [AGENTS.md](AGENTS.md): rules for AI agents, including the mandatory ponytail and humanizer skills.
- [PROGRESS.md](PROGRESS.md): where the project stands and why decisions were made.
- [LICENSE](LICENSE): Apache 2.0.
