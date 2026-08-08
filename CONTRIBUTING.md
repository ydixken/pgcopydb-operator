# CONTRIBUTING.md

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119.

[TOC]

## Requirements

- [go](https://go.dev): the operator's language (nothing to build yet).
- [task](https://taskfile.dev): task runner, the entrypoint for everything.
- [yamllint](https://yamllint.readthedocs.io): lints all YAML; the only linter with work to do pre-scaffold.
- [golangci-lint](https://golangci-lint.run) v2: Go linting, activates once `go.mod` exists.
- [kubectl](https://kubernetes.io/docs/reference/kubectl/): only needed for `task e2e`.
- [gh](https://cli.github.com): PRs happen on GitHub.
- Docker MAY be installed for local image builds; CI builds the published image.

## Day-to-day loop

1. Branch from `main`.
1. Make one logical change.
1. If the change touches `api/v1alpha1`, run `task docs` and commit the regenerated `docs/api.md` with it.
1. Run `task lint` (and `task test` once Go code exists). Both MUST be clean before every commit.
1. Commit (see below), push the branch to GitHub, open a PR.

The GitLab project (`gitlab.com/ydixken/pgcopydb-operator`) is a push mirror that runs the pipeline. Watch it, but never commit or open MRs there.

## Commits and pull requests

- Conventional commits: `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `ci:`.
- One logical change per commit. Every commit MUST be lint-clean on its own.
- `main` is protected: changes land via GitHub PRs with a green pipeline. Nobody pushes to `main` directly.
