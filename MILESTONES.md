# MILESTONES.md

Status: **M1 functional (2026-08-07): first live migration completed end to end on the e2e cluster** (CNPG-to-CNPG, 251k rows, indexes and sequences verified identical; three defects found and fixed through live iteration). Remaining M1: e2e suite (B13), GitOps registration (B12).

Design: [docs/superpowers/specs/2026-08-07-operator-design.md](docs/superpowers/specs/2026-08-07-operator-design.md). Research ground truth: [docs/research/](docs/research/). Facts about the private e2e infrastructure are never committed here. This file is the compaction-proof task ledger: every task carries enough context to execute without the original conversation.

[TOC]

## H0: Repository harness (done)

Agent guides, vendored skills (ponytail, humanizer, brainstorming), Taskfile, yamllint, branch-only GitLab CI with self-activating jobs, mirror workflow. See decision log.

## M1: v0.1, one-shot clone + Helm + dev cluster

Target: a `Migration` CR performs `pgcopydb clone` source to target with status, retries, metrics; installable chart; running on the dev cluster; first e2e green.

### Spikes (verify research gaps, record outcome here)

- [x] S1 (done 2026-08-07, live container run): `clone --follow --host --port` parses and runs (no getopt rejection). TCP sentinel coordinator is the M2 control plane.
- [x] S2 (source-verified): the sentinel wire protocol has no authentication; the chart MUST ship a NetworkPolicy restricting 5442 to the operator (M2 task below).
- [x] S3 (done 2026-08-07, live on PG18; CORRECTED same day): GRANT EXECUTE on the six pg_replication_origin_* functions is necessary but NOT sufficient. pgcopydb's apply session also runs SET session_replication_role TO 'replica' (superuser-gated); the full non-superuser target contract is those grants PLUS (PG15+) GRANT SET ON PARAMETER session_replication_role. Without it pgcopydb 0.18 silently applies nothing while advancing replay_lsn (second silent-loss mode, found live).
- [x] S4 (done 2026-08-07, live check): follow-mode prerequisites (logical wal_level, slot and sender headroom) are met on the e2e cluster out of the box; specifics live in private ops notes.
- [ ] S5: how to declare a REPLICATION-attribute role via CNPG `managed.roles` for the e2e source fixture.
- [x] S6 (image built and smoke-tested on arm64 via podman: pgcopydb 0.18, pg_dump 17.10, non-root; amd64 + registry push still pending in CI, see B11): build the runner image (pgcopydb 0.18 + postgresql-client-17) and run a clone against a PG17 target; upstream image ships PG16 client tools and pg_dump must be >= target major.
- [ ] S7: capture exact JSON from `pgcopydb list progress --json`, `list progress --summary --json`, `summary.json`, and `stream sentinel get` single-value selectors from a live run; check `sentinel get --json` endpos bug still present; commit samples under `docs/research/samples/`.
- [ ] S8: SIGTERM a mid-clone run; record exit code and verify `--resume` picks up correctly (drives Job-failure interpretation).
- [x] S9 (done 2026-08-07, live check): e2e-cluster facts recorded in private ops notes (this repository is public). Project consequence: the runner image targets amd64 first; arm64 in M4.
- [ ] S10: (deferred) OCI-sourced ArgoCD Application viability; M1 uses the proven git-repo-with-path chart source.

### Build

- [x] B1 (done, PR #5): kubebuilder scaffold. Procedure: `mv README.md README.md.h && mv .gitignore .gitignore.h`, `kubebuilder init` (go/v4, domain `pgcopydb-operator.io`, project name pgcopydb-operator), `kubebuilder create api --group '' --version v1alpha1 --kind Migration` (group empty means core group of the domain), restore README (keep ours) and .gitignore (diff-union, expected no-op), delete `.devcontainer/` and generated `.github/workflows/{lint,test,test-e2e}.yml`, digest-pin the generated Dockerfile FROMs, verify `lint:go` and `test:unit` turn green in GitLab CI with zero CI edits.
- [x] B2 (done): `Migration` v1alpha1 types, v0.1 surface only: `source`/`target` connection structs (one-of inline XOR `uriSecretRef`; named type for future `connectionRef`), `clone` options block, `workVolume`, `runner`, `suspend`, `backoffLimit`, `ttlSecondsAfterFinished`; status with conditions, derived `phase` printer column, `progress`, `attempts`, `jobName`. `follow`/`cutover`/`verification` fields arrive with their milestones.
- [x] B3 (done: CEL one-of + immutability markers, exercised via envtest admission): CEL validations (source/target immutability, one-of enforcement) + defaults + envtest coverage of accept/reject cases.
- [x] B4 (done, internal/conn): connection materialization: URI composition without passwords, `PGPASSFILE` projection, TLS secret mounts (0600), `PGCOPYDB_*_PGURI` env path for `uriSecretRef`. Unit tests incl. no-credential-leak assertions.
- [x] B5 (done, internal/pgcopydb, 100% coverage): clone flag + filters INI renderer with golden tests (every spec field to exact pgcopydb argv/INI).
- [x] B6 (done): reconciler: PVC + attempt-numbered Job (`backoffLimit: 0`), `--resume`/`--not-consistent` on retries, operator-counted retry budget, conditions (Validated, CloneCompleted, Complete, Failed), events, suspend, TTL; envtest suite driving Job status transitions.
- [x] B7 (done, internal/progress): progress polling via `pods/exec` `pgcopydb list progress --json` into `status.progress` (schema from S7).
- [x] B8 (done, internal/metrics; ServiceMonitor ships with the chart): Prometheus metrics (phase, tables/bytes done, attempts, failures) + chart ServiceMonitor toggle.
- [x] B9 (Dockerfile done, images/runner/; CI build job + ghcr publish pending, B11): `images/runner/` Dockerfile (Debian + PGDG, pgcopydb 0.18, postgresql-client-17, non-root, digest-pinned) + GitLab CI build job + ghcr publish in release workflow.
- [x] B10 (done, charts/pgcopydb-operator; helm lint + template verified): Helm chart `charts/pgcopydb-operator` (templated CRDs, `crds.install`, resource-policy keep, values per spec, NetworkPolicy toggle) + chart lint in CI.
- [x] B11 (workflow done; verified with the first tag): GitHub release workflow: both images (amd64 first per S9) + OCI chart to ghcr.io on v* tags.
- [ ] B12: register the operator in the private GitOps repository (recipe lives there; it describes private infrastructure and MUST NOT be documented here).
- [x] B13 (done 2026-08-07: 6/6 scenarios green live in 148s: fresh clone, dropIfExists re-clone, filters, resume-after-pod-kill, cross-namespace, CEL rejection; suite self-installs a namespace-scoped operator into pgcopydb-e2e-system and always removes it): e2e harness (`test/e2e`, Ginkgo, current-context targeting per `task e2e` contract): fixtures = 2 CNPG clusters in ns `pgcopydb-e2e` + pagila demo data; scenarios: same-cluster clone, cross-namespace clone with secrets (cross-cluster stand-in), filters, resume after runner-pod kill.
- [x] B14 (done): docs: README install/quickstart, `docs/examples/*.yaml` (minimal, tuned+filters, DBaaS DSN), chart README values reference.

## M2: live migration (follow + cutover)

- [x] `spec.follow` (plugin, slotName, publication, replayNoOpUpdates, maxCatchupLag) + generated unique slot/origin names (done, PR #16).
- [x] Follow-mode preflight (done: a `<name>-preflight` Job gates the first attempt of every follow migration; envtest-covered): `wal_level`, free-slot headroom, REPLICATION on the source role, EXECUTE on the target's pg_replication_origin_* functions, and the `session_replication_role` SET probe (the S3 silent-loss gate). Each failed check prints one line with the exact GRANT/setting to fix; the operator absorbs the failure (Validated=False, Failed=True, reason PreflightFailed) with the check output in the condition message, read from the pod log (pods/log RBAC). Live evidence (2026-08-07): missing grants burned 4 attempts in 110s with the real cause visible only in pod logs.
- [ ] Preflight extensions: replica-identity audit (fail on PK-less tables unless allowlisted; needs an API field) and plugin availability (wal2json presence when selected).
- [x] Surface the worker's terminal error in status (done): on Job failure the operator reads the failed pod's log tail and appends the last ERROR/FATAL `message` from the structured JSON logs to the AttemptFailed event and the Failed condition; unreadable logs degrade to the Job's own condition message.
- [x] Retry-after-setup-crash guard (done): retry attempts of follow migrations drop pgcopydb's own leftover publication (`DROP PUBLICATION IF EXISTS "<slot>"` on the source, in the worker prelude before `--resume`), only when `spec.follow.publication` is empty; user-provided publications are never touched. The upstream issue on the non-idempotent CREATE PUBLICATION in `--resume` is still to be filed (needs maintainer sign-off for outward communication).
- [x] Control plane decision (2026-08-07): sentinel driven via pods/exec in the runner pod (same filesystem, sanctioned by pgcopydb docs; exec plumbing already exists for progress). No PGCOPYDB_HOST set, so no open port and no NetworkPolicy needed yet; the TCP coordinator (S1-verified) stays the documented alternative if exec proves limiting.
- [x] Conditions Streaming/CaughtUp with lag from sentinel selectors vs the source WAL head, `status.replication` block (done; lag metric still open).
- [x] Cutover `mode: Manual|Automatic` + `approved` gate via `sentinel set endpos --current`; drain = worker exit 0; CutoverCompleted (done, envtest-covered).
- [x] Cleanup Job + finalizer: `pgcopydb stream cleanup` gates Complete and routes deletion; slot-retention warning on suspend (done, envtest-covered).
- [ ] Snapshot-holder hardening (separate `pgcopydb snapshot --follow` container for consistent base-copy resume) or documented `--not-consistent` trade-off; decide from S8 evidence.
- [x] E2e (2026-08-07, 9/9 green in 464s live): follow with live-write burst + Manual cutover + row/sequence equality, Automatic cutover, delete-mid-stream drops the slot. Still open: a suspend/resume-under-streaming scenario and sustained pgbench-style write load.
- [ ] Example PrometheusRule (slot retention, apply crash-loop) under docs/examples.

## M3: verification + service polish

- [x] `spec.verification` (schema/data, both opt-in: even compare schema costs catalog fetches, compare data reads every row twice) running `pgcopydb compare` per enabled check in a Job on the work PVC; Verified condition (SchemaMismatch/DataMismatch), phase Verifying, `pgcopydb_migration_verified` metric. A mismatch reports, it does not fail the Migration: the transfer already happened, and on follow mode post-cutover writes to the target are indistinguishable from real diffs. Follow ordering: after the drain-verify gate (compare on a live target mismatches by design) and after cleanup (the slot retains WAL while it exists; compare needs no replication state). Envtest-covered (done 2026-08-07).
- [ ] Evaluate `PostgresConnection` kind + `connectionRef` (as-a-service reusable endpoints) based on e2e experience.
- [x] Data-compare guidance for follow mode (quiesce before compare) in docs: see docs/examples/migration-verified.yaml (done 2026-08-07).

## M4: OSS release polish

- [x] Option-coverage audit (done: [docs/coverage.md](docs/coverage.md)): every clone/follow option from `docs/research/pgcopydb-cli.md` sections 2.1/2.2 mapped to a spec field, operator-managed behavior, or a recorded exclusion. Three unintentional gaps found, queued below.
- [ ] Close audit gaps (small additive API fields): `clone.estimateTableSizes` (`--estimate-table-sizes`; `skip: analyze` is already exposed but only matters with it), a `clone.skip` entry `extensionComments` (`--skip-ext-comments`), `follow.wal2jsonNumericAsString` (`--wal2json-numeric-as-string`; wal2json is selectable but its only tuning knob is not).
- [x] Usage guide (done: [docs/usage.md](docs/usage.md): install, first clone, live-migration runbook with Manual/Automatic cutover, suspend, retries, deletion, troubleshooting from the live findings) plus a commented follow example ([docs/examples/migration-follow.yaml](docs/examples/migration-follow.yaml)).
- [x] Multi-arch images (workflow done: release builds manager and runner for linux/amd64 + linux/arm64 via QEMU/buildx; verified with the next tag, like B11).
- [x] Artifact Hub metadata (done: `charts/pgcopydb-operator/artifacthub-repo.yml`, Chart.yaml annotations). One manual step remains, listing the repo: on artifacthub.io add a repository of kind "Helm charts (OCI)" with url `oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator`, then push the metadata next to the chart: `oras push ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator:artifacthub.io --config /dev/null:application/vnd.cncf.artifacthub.config.v1+yaml artifacthub-repo.yml:application/vnd.cncf.artifacthub.repository-metadata.layer.v1.yaml`; set the assigned repositoryID in the file and re-push for verified-publisher status.
- [ ] CRD reference docs generation, versioned docs.
- [ ] Public issue templates/community files as adoption warrants (deliberately deferred, see H0 slop-trap decision).

### Live-iteration findings (2026-08-07, all shipped)

1. **Verify-gate defects (found by the alpha.8 confirmation run, fixed same day)**: (a) buildVerifyJob replaced the container command with bare /bin/sh, discarding the passfile prelude, so verification always failed auth and falsely refuted every password-based cutover; the script now runs behind the shared passfile prelude (the `scriptJob` helper, which keeps the worker pod shape and swaps only the exec'd program). (b) The origin-vs-endpos predicate was exact, but the origin parks at the last applied commit record and can trail endpos by a few non-data WAL bytes on a quiesced source; the predicate now tolerates up to one WAL page (8192 bytes), which still catches both real loss modes, with spec.verification.data as the airtight complement.
1. Observation to investigate: pgcopydb_* replication origins linger on the target even after successful cleanup Jobs; `stream cleanup` may not drop the target origin as documented. Harmless for reruns, needs a look.
1. **Second silent-loss mode (critical, crash-free)**: with a non-superuser target user missing the session_replication_role SET privilege, pgcopydb 0.18 swallows the apply-preamble failure, applies nothing, advances replay_lsn to endpos, and exits 0. Preflight (open task above) MUST check this grant. Open question against the drain-verify gate: whether pgcopydb also advances the target origin in this mode (if yes, origin-vs-endpos verification passes falsely and needs a row-motion sanity signal; check on the next occurrence). Two upstream pgcopydb issues to file pending maintainer sign-off: resume-after-endpos skips replay, and swallowed apply-preamble errors.
1. **Data-loss gate (critical)**: a crash between endpos-set and drain-complete makes pgcopydb `--resume` exit 0 without replaying pending WAL ("endpos previously reached" tracks the receive side). The operator no longer trusts exit 0: a verify Job compares the target's replication origin progress against the recorded endpos; only proof gates CutoverCompleted and cleanup, refutation fails loudly with the slot kept (data recoverable, WAL retention warned). Upstream issue against pgcopydb's resume-after-endpos path still to be filed (needs maintainer sign-off for outward communication). Also worth pursuing: session-level `wal_sender_timeout` on the source connection (aggressive server defaults, for example 5s, kill walsenders mid-drain; version-aware design needed since startup-packet GUCs fail on old servers; documented in PREREQUISITES.md meanwhile).

1. PGPASSFILE now lives in the container spec env, not only the prelude shell: exec'd sentinel commands (WAL-head query, endpos) failed password auth otherwise, pinning follow migrations at Streaming. Found by the live follow suite; envtest cannot see it (fake sentinel), which is exactly why both test layers exist.

1. First attempts pass `--restart`: a fresh Migration once adopted the still-terminating work PVC of a deleted one and choked on foreign catalogs. Owned-object creation also refuses foreign owners now.
1. Events were silently dropped: the new events API needs `events.k8s.io` RBAC; the chart ClusterRole now mirrors controller-gen output verbatim (which also restored pods/exec for progress polling).
1. Runner ships PostgreSQL client major 18: pg_dump must be >= the newest server major on either side (new unpinned clusters already run 18), and the work dir moved below the volume mount so `--restart` can remove it.
1. Open hardening items: classify deterministic pgcopydb failures (permission errors) to stop useless retries; status updates move to patch semantics (in review).

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
| 2026-08-07 | GitOps chart source = git-repo-with-path, not OCI                               | Matches the proven pattern in the private GitOps repo (S10 may revisit).                                                                             |
| 2026-08-07 | Unit tests by default; documentation always current; both hard requirements     | User requirement; codified in AGENTS.md.                                                                                                             |
| 2026-08-07 | MILESTONES.md replaces PROGRESS.md as the single task ledger                    | User requirement; decision log carried over.                                                                                                          |
