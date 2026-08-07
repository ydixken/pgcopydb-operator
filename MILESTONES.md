# MILESTONES.md

Status: **H0 done, design approved (2026-08-07). M1 in progress.**

Design: [docs/superpowers/specs/2026-08-07-operator-design.md](docs/superpowers/specs/2026-08-07-operator-design.md). Research ground truth: [docs/research/](docs/research/). This file is the compaction-proof task ledger: every task carries enough context to execute without the original conversation.

[TOC]

## H0: Repository harness (done)

Agent guides, vendored skills (ponytail, humanizer, brainstorming), Taskfile, yamllint, branch-only GitLab CI with self-activating jobs, mirror workflow. See decision log.

## M1: v0.1, one-shot clone + Helm + dev cluster

Target: a `Migration` CR performs `pgcopydb clone` source to target with status, retries, metrics; installable chart; running on the dev cluster; first e2e green.

### Spikes (verify research gaps, record outcome here)

- [ ] S1: confirm `--host/--port` (sentinel TCP) is accepted by `pgcopydb clone --follow` and `follow` in a live v0.18 container (getopt table says yes, help text omits it). Outcome feeds the control-plane choice (TCP vs exec).
- [ ] S2: probe the sentinel TCP protocol for auth (none expected per `ld_ipc.h`); decide NetworkPolicy shape for port 5442.
- [ ] S3: test `pg_replication_origin_create/_advance/_xact_setup` as a non-superuser on PG17 (GRANT EXECUTE path) against a CNPG cluster; determines target-credential requirements for follow.
- [x] S4 (done 2026-08-07, live check): CNPG defaults verified on the dev cluster: `wal_level=logical`, `max_replication_slots=32`, `max_wal_senders=10`, PostgreSQL 17.9. Follow mode needs no per-cluster tuning.
- [ ] S5: how to declare a REPLICATION-attribute role via CNPG `managed.roles` for the e2e source cluster.
- [x] S6 (image built and smoke-tested on arm64 via podman: pgcopydb 0.18, pg_dump 17.10, non-root; amd64 + registry push still pending in CI, see B11): build the runner image (pgcopydb 0.18 + postgresql-client-17) and run a clone against a PG17 target; upstream image ships PG16 client tools and pg_dump must be >= target major.
- [ ] S7: capture exact JSON from `pgcopydb list progress --json`, `list progress --summary --json`, `summary.json`, and `stream sentinel get` single-value selectors from a live run; check `sentinel get --json` endpos bug still present; commit samples under `docs/research/samples/`.
- [ ] S8: SIGTERM a mid-clone run; record exit code and verify `--resume` picks up correctly (drives Job-failure interpretation).
- [x] S9 (done 2026-08-07, live check): k3s v1.35.1, 3 amd64 nodes (20 CPU / 32Gi each). Runner image targets amd64 first; arm64 in M4.
- [ ] S10: (deferred) OCI-sourced ArgoCD Application viability; M1 uses the proven git-repo-with-path chart source.

### Build

- [x] B1 (done, PR #5): kubebuilder scaffold. Procedure: `mv README.md README.md.h && mv .gitignore .gitignore.h`, `kubebuilder init` (go/v4, domain `pgcopydb-operator.io`, project name pgcopydb-operator), `kubebuilder create api --group '' --version v1alpha1 --kind Migration` (group empty means core group of the domain), restore README (keep ours) and .gitignore (diff-union, expected no-op), delete `.devcontainer/` and generated `.github/workflows/{lint,test,test-e2e}.yml`, digest-pin the generated Dockerfile FROMs, verify `lint:go` and `test:unit` turn green in GitLab CI with zero CI edits.
- [x] B2 (done): `Migration` v1alpha1 types, v0.1 surface only: `source`/`target` connection structs (one-of inline XOR `uriSecretRef`; named type for future `connectionRef`), `clone` options block, `workVolume`, `runner`, `suspend`, `backoffLimit`, `ttlSecondsAfterFinished`; status with conditions, derived `phase` printer column, `progress`, `attempts`, `jobName`. `follow`/`cutover`/`verification` fields arrive with their milestones.
- [ ] B3: CEL validations (source/target immutability, one-of enforcement) + defaults + envtest coverage of accept/reject cases.
- [x] B4 (done, internal/conn): connection materialization: URI composition without passwords, `PGPASSFILE` projection, TLS secret mounts (0600), `PGCOPYDB_*_PGURI` env path for `uriSecretRef`. Unit tests incl. no-credential-leak assertions.
- [x] B5 (done, internal/pgcopydb, 100% coverage): clone flag + filters INI renderer with golden tests (every spec field to exact pgcopydb argv/INI).
- [ ] B6: reconciler: PVC + attempt-numbered Job (`backoffLimit: 0`), `--resume`/`--not-consistent` on retries, operator-counted retry budget, conditions (Validated, CloneCompleted, Complete, Failed), events, suspend, TTL; envtest suite driving Job status transitions.
- [ ] B7: progress polling via `pods/exec` `pgcopydb list progress --json` into `status.progress` (schema from S7).
- [ ] B8: Prometheus metrics (phase, tables/bytes done, attempts, failures) + chart ServiceMonitor toggle.
- [x] B9 (Dockerfile done, images/runner/; CI build job + ghcr publish pending, B11): `images/runner/` Dockerfile (Debian + PGDG, pgcopydb 0.18, postgresql-client-17, non-root, digest-pinned) + GitLab CI build job + ghcr publish in release workflow.
- [ ] B10: Helm chart `charts/pgcopydb-operator` (templated CRDs, `crds.install`, resource-policy keep, values per spec, NetworkPolicy toggle) + chart lint in CI.
- [ ] B11: GitHub release workflow: goreleaser-or-docker-build of both images (amd64 first; arm64 per S9) + `helm push` OCI chart to ghcr.io.
- [ ] B12: register in cloudcats/app-of-apps: `platform/templates/pgcopydb-operator/application-pgcopydb-operator.yaml` (sync-wave -3, ServerSideApply, git source `charts/pgcopydb-operator` on GitHub main), values block in `platform/values.yaml`, GitHub URL in `platform-appproject.yaml` `sourceRepos` (whitelist is mandatory). MR to that repo.
- [ ] B13: e2e harness (`test/e2e`, Ginkgo, current-context targeting per `task e2e` contract): fixtures = 2 CNPG clusters in ns `pgcopydb-e2e` + pagila demo data; scenarios: same-cluster clone, cross-namespace clone with secrets (cross-cluster stand-in), filters, resume after runner-pod kill.
- [ ] B14: docs: README quickstart, `docs/examples/*.yaml` (each example a real resource with a short explanation), values documentation in chart README.

## M2: live migration (follow + cutover)

- [ ] `spec.follow` (plugin, slotName, publication, replayNoOpUpdates, maxCatchupLag) + generated unique slot/origin/publication names.
- [ ] Follow-mode preflight: `wal_level`, slot capacity, REPLICATION privilege, replica-identity audit (fail on PK-less tables unless allowlisted), plugin availability.
- [ ] Control plane: runner starts with `PGCOPYDB_HOST=0.0.0.0`; operator drives sentinel via TCP (S1/S2 outcome; exec fallback). NetworkPolicy in chart.
- [ ] Conditions Streaming/CaughtUp (lag from sentinel selectors vs `pg_current_wal_lsn`), `status.replication` block, lag metric.
- [ ] Cutover `mode: Manual|Automatic` + `approved` gate: quiesce guidance, `sentinel set endpos --current`, drain = Job exit 0 (authoritative; sequences re-synced by `clone --follow` itself), CutoverCompleted.
- [ ] Cleanup Job + finalizer: `pgcopydb stream cleanup` on abort/deletion (drops slot, publication, origin); slot-retention warning while suspended.
- [ ] Snapshot-holder hardening (separate `pgcopydb snapshot --follow` container for consistent base-copy resume) or documented `--not-consistent` trade-off; decide from S8 evidence.
- [ ] E2e: follow + live pgbench writes + Manual cutover + row equality; Automatic cutover; abort drops the slot; suspend/resume.
- [ ] Example PrometheusRule (slot retention, apply crash-loop) under docs/examples.

## M3: verification + service polish

- [ ] `spec.verification` (schema/data) running `pgcopydb compare` post-completion in a Job on the work PVC; results in conditions + status.
- [ ] Evaluate `PostgresConnection` kind + `connectionRef` (as-a-service reusable endpoints) based on e2e experience.
- [ ] Data-compare guidance for follow mode (quiesce before compare) in docs.

## M4: OSS release polish

- [ ] Option-coverage audit: spec surface vs `docs/research/pgcopydb-cli.md`, close gaps or record exclusions.
- [ ] Multi-arch images (amd64+arm64), Artifact Hub listing, CRD reference docs generation, versioned docs.
- [ ] Public issue templates/community files as adoption warrants (deliberately deferred, see H0 slop-trap decision).

## Decision log

| Date       | Decision                                                                        | Rationale                                                                                                                                            |
|------------|---------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2026-08-07 | GitHub is primary; GitLab is a push mirror running CI, branch pipelines only    | The mirror pushes branches and opens no MRs, so merge_request_event rules would never fire.                                                          |
| 2026-08-07 | Taskfile, not Makefile                                                          | Kubebuilder owns the Makefile it generates; a pristine Makefile keeps upstream diffs clean. Taskfile never collides and stays the entrypoint.         |
| 2026-08-07 | `go.mod`, `Makefile`, `Dockerfile`, `PROJECT`, `.golangci.yml` left to kubebuilder | They arrive correct and together with the code they serve; hand-made versions would make `kubebuilder init` error.                                   |
| 2026-08-07 | No Dockerfile in the harness                                                    | It cannot build without code, and the CI job gates on its existence, activating the commit the scaffold lands. Digest-pin patch tracked in M1/B1.     |
| 2026-08-07 | `.gitignore` and `README.md` are harness-owned                                  | Needed now for secret hygiene and orientation; M1/B1 documents the move-aside so `kubebuilder init` succeeds.                                        |
| 2026-08-07 | CI jobs self-activate via `rules:exists`                                        | The pipeline is green on the bare harness and needs no edit when Go code lands.                                                                      |
| 2026-08-07 | E2e tests are local only, against the developer's current kubectl context       | No CI cluster credentials yet; `task e2e` shows the context and prompts before touching a real cluster.                                              |
| 2026-08-07 | yamllint yes, markdownlint no, `.editorconfig` no                               | YAML is the harness's executable content; prose linting and editor config fix nothing that breaks (solution ladder, rung 1).                         |
| 2026-08-07 | Ponytail vendored into `.claude/skills/ponytail/` and made mandatory            | Always-on for every contributor with no per-user install, pinned to a known version: github.com/DietrichGebert/ponytail @ `16f29800fd26` (MIT).      |
| 2026-08-07 | Humanizer vendored into `.claude/skills/humanizer/` and made mandatory for prose | No AI slop in writing, same vendoring rationale as ponytail: github.com/blader/humanizer v2.8.2 @ `1b48564` (MIT).                                    |
| 2026-08-07 | Em dashes are forbidden everywhere (docs, comments, commit messages)            | Hard requirement from the maintainer; also a known AI-writing tell. Enforced via AGENTS.md writing conventions.                                       |
| 2026-08-07 | Brainstorming vendored into `.claude/skills/brainstorming/` and made mandatory for creative work | Design before implementation, same vendoring rationale: superpowers 6.2.0 by obra (MIT), full skill directory copied verbatim.                        |
| 2026-08-07 | Renovate config named `.renovaterc.json`                                        | `renovate.json` (lowercase, non-dotfile) would make kubebuilder's directory check reject the repo at scaffold time.                                  |
| 2026-08-07 | `.golangci.yml` accepted verbatim from kubebuilder at scaffold                  | Its v2 config is the standard linter set. Any later suppression MUST carry an inline comment explaining why.                                          |
| 2026-08-07 | "Gitflow-style commits" = conventional commits, confirmed; no develop branch    | User clarification; the existing CONTRIBUTING convention stands.                                                                                     |
| 2026-08-07 | API group `pgcopydb-operator.io`, v1alpha1, single namespaced kind `Migration`  | One kind covers the whole lifecycle; connection struct is one-of shaped so a `PostgresConnection` kind can be added additively (evaluated in M3).     |
| 2026-08-07 | Two images: slim operator + runner (pgcopydb 0.18 + postgresql-client-17)       | Independent versioning, small attack surface; upstream image ships PG16 client tools while dev targets run PG17 (pg_dump must be >= target major).    |
| 2026-08-07 | Public artifacts on ghcr.io (images + OCI Helm chart) via GitHub release workflow | GitHub is primary and public; GitLab CI remains lint/test/build validation.                                                                          |
| 2026-08-07 | Cutover modes Manual and Automatic both ship with the follow milestone          | User requirement; Manual gates on `spec.cutover.approved`, Automatic proceeds at CaughtUp.                                                            |
| 2026-08-07 | Status + events + Prometheus metrics from v0.1                                  | User requirement ("both").                                                                                                                            |
| 2026-08-07 | Job with `backoffLimit: 0`; retries are operator-driven with `--resume`         | Retry policy and attempt counts belong in CR status; pgcopydb catalogs make resume correct (prior-art + CLI research).                               |
| 2026-08-07 | CEL validation + defaults, no admission webhooks in v1                          | Removes cert-manager dependency; immutability via transition rules; conversion webhook reconsidered only when multi-version arrives.                  |
| 2026-08-07 | TCP sentinel coordinator as follow control plane, exec as fallback              | v0.18 built it exactly for cross-container control (no shared volume, no SQLITE_BUSY); it is unauthenticated so the chart ships a NetworkPolicy. S1/S2 verify. |
| 2026-08-07 | Chart source for the dev cluster = git-repo-with-path, not OCI                  | No existing Application in app-of-apps uses OCI; git+path is the proven route (S10 may revisit).                                                     |
| 2026-08-07 | Unit tests by default; documentation always current; both hard requirements     | User requirement; codified in AGENTS.md.                                                                                                             |
| 2026-08-07 | MILESTONES.md replaces PROGRESS.md as the single task ledger                    | User requirement; decision log carried over.                                                                                                          |
