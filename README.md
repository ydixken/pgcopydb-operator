# pgcopydb-operator

A Kubernetes operator that turns [pgcopydb](https://github.com/dimitri/pgcopydb) into Migration-as-a-service for PostgreSQL: declare a `Migration` resource, get a supervised bulk clone, optional logical-replication follow with controlled cutover, verification, and cleanup. Source and target are plain libpq endpoints, so it works with any PostgreSQL: managed, operator-run, or bare.

> [!important]
> In development, milestone M1 (one-shot clone). See [MILESTONES.md](MILESTONES.md) for the task ledger and the [design](docs/superpowers/specs/2026-08-07-operator-design.md) for the architecture.

## Structure

```text
.claude/skills/            # vendored skills (ponytail, humanizer, brainstorming), mandatory per AGENTS.md
.github/workflows/         # push mirror GitHub -> GitLab (CI runs on the mirror)
.gitlab-ci.yml             # branch pipelines: yamllint now; Go lint/test/image build self-activate with the scaffold
.renovaterc.json           # keeps the tag+digest image pins current, once Renovate is enabled
.yamllint.yml              # yamllint policy, every deviation commented
AGENTS.md                  # single source of truth for agents (CLAUDE.md points here)
CONTRIBUTING.md            # tooling, workflow, commit and PR conventions
MILESTONES.md              # task ledger and decision log
Taskfile.yml               # task help | lint | test | e2e
docs/research/             # pgcopydb CLI + CDC references, dev-cluster recon, prior art
docs/superpowers/specs/    # approved design documents
```

## Quickstart

```sh
task help   # list tasks
task lint   # the pre-commit gate
task e2e    # e2e tests against your CURRENT kubectl context (local only; prompts first)
```

## Documentation

- [CONTRIBUTING.md](CONTRIBUTING.md): how to work here.
- [AGENTS.md](AGENTS.md): rules for AI agents, including the mandatory skills.
- [MILESTONES.md](MILESTONES.md): where the project stands and why decisions were made.
- [Design](docs/superpowers/specs/2026-08-07-operator-design.md): the approved operator architecture.
- [LICENSE](LICENSE): Apache 2.0.
