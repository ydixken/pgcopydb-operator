# Runtime safety implementation plan

## Scope

This plan implements the approved runtime safety work for issues #242, #211, #215, #243, #223, #221, #209, #210, and #200.
Issue #88 remains deferred.
This plan excludes the pgcopydb fork, SQLite contention work, builder publication, runner digest changes, and source-write fencing changes.

## Global constraints

- Use normal feature branches in the primary checkout and implement tasks sequentially because controller, API, and workflow changes share interfaces.
- Preserve public API group, identities, existing RBAC, candidate-SHA validation, generated-artifact checks, and current validation rules except for the approved optional API fields and the guarded split-table CEL rule.
- Do not log SQL, connection strings, credentials, or private cluster information.
- Every behavior change needs focused regression coverage, current documentation, `task lint`, `task test`, strict documentation validation, and a full `feature-e2e` result for the exact final head before merge.
- Do not run local real-cluster E2E without the repository's required context confirmation flow.
- The isolated kind workflow may receive only its disposable cluster kubeconfig.
- #200 is investigation-first and remains open until a measured slow-pass cause has a discriminating regression and a fix.

## Shared prerequisite: disposable-cluster E2E

Add opt-in `schema_validation=additive-disposable` to the trusted feature workflow.
Create a disposable kind cluster on the Docker-capable runner, install the candidate CRD, and bootstrap pinned CNPG and Prometheus dependencies.
Reuse the existing fixtures with `E2E_SCALE=0.1`, one CNPG instance, and the `standard` StorageClass.
Require at least 8 CPUs, 16 GiB memory, and 32 GiB free disk before bootstrap.
Keep candidate code scoped to the disposable kubeconfig.
Preserve immutable candidate-SHA checking and existing CI trust restrictions.
Require full-suite success, metrics assertions, and successful teardown before publishing `feature-e2e` for the candidate SHA.
Test workflow configuration and failure paths without a live cluster.
Prove effective Docker-daemon capacity and storage provenance before enabling this route.
Keep the shared profile's alert rules disabled.
Enable the owned isolated operator rule only after proving that Prometheus has no alert-delivery destinations.

## Task 1: Fix #242 progress sampler resource leaks

Add one progress-only wrapper for all three sample queries and `CloneStage`.
Use a five-second SQL timeout, a six-second process deadline, and a one-second kill grace.
Set the SQL timeout explicitly so connection-string options cannot override it.
Preserve partial readings and the last recorded values when sampling fails.
Keep these limits out of cutover execution transport.
Test blocked source and target queries, hanging connections, TERM-resistant processes, repeated polls, cleanup within the budget, and recovery after unlock.

## Task 2: Fix #211 manual cutover catch-up guard

Require both manual approval and the existing two-sample caught-up verdict before starting cutover.
Allow an early approval to remain armed while streaming so the second qualifying sample starts cutover without another spec update.
Do not add source write fencing.
Test high lag, unavailable readings, confirmation, renewed lag, and exactly-once end-position setting.
Add a controlled-backlog E2E case that proves early approval does not start cutover, then completes after the writer stops and catch-up is observed.

## Task 3: Fix #215 replication cleanup outcome reporting

Add `CleanupCompleted=True` with reason `CleanupSucceeded`, or `CleanupCompleted=False` with reason `CleanupFailed`, to both served APIs.
Keep workflow completion independent from cleanup success.
Publish cleanup status before completion or finalizer release when status is writable.
Before reporting success, verify that the replication slot, target origin, and operator-managed publication are absent.
Do not remove caller-owned publications.
Cover cleanup failure and deletion paths, extend condition metrics, and update alerts, dashboards, runbooks, and E2E teardown.
Publish this task only after the disposable prerequisite is merged and live-validated.
The full isolated gate must prove that cleanup failure status, metrics, and a firing alert survive cleanup Job TTL expiry.

## Task 4: Fix #243 orphaned worker connections

Apply worker-session keepalive settings to new workers: `tcp_keepalives_idle=60`, `tcp_keepalives_interval=10`, `tcp_keepalives_count=6`, and `tcp_user_timeout=120000`.
Export settings through the existing connection prelude and verify effective client-origin settings at preflight and each worker start.
Preserve connection strings and reject conflicting user settings with actionable diagnostics.
Document that existing orphans require manual recovery and TCP-terminating poolers fall outside this guarantee.
After Task 1, strengthen the resume regression to prove COPY is active before the worker is killed, require orphan cleanup within 180 seconds, and require successful data-matching retry.

## Task 5: Fix #223 extension availability preflight

Compare selected installed source extensions with target-installed extensions or target default installation availability.
Honor extension include and exclude filters and `skip: [extensions]`.
Require availability when `dropIfExists=true` because restore can recreate extensions.
Allow version differences.
Use `PreflightFailed` for unavailable names, query errors, or missing verdicts without exposing connection details.
Add focused coverage and an E2E case using the existing `citext` fixture.

## Task 6: Fix #221 optional PostgreSQL major-version gate

Add immutable `spec.requireSameMajorVersion`, defaulting to `false`, to both served APIs.
When enabled, compare normalized `server_version_num` values after connectivity and before privilege remediation.
Ignore minor-version differences and use `PreflightFailed` for mismatches or malformed and missing results.
Preserve cross-major migration behavior when the field is omitted.
Regenerate API, chart CRD, and reference artifacts, then cover version cases, defaults, immutability, and no-worker rejection.

## Task 7: Fix #209 split-table disabling

Add pointer boolean `spec.clone.splitTables`, defaulting to `true`.
Nil and true preserve existing split-table defaults.
False omits both split-table arguments.
Add a CEL rule rejecting false combined with either split-table tuning field while allowing explicit zero where valid.
Preserve tuning defaults in argument construction.
Round-trip false through both API versions, regenerate artifacts, and run a real clone that proves arguments and data correctness.

## Task 8: Fix #210 initial Pending status

Set untouched status to `Pending` with the current observed generation through an optimistic-lock status patch.
Return with an immediate explicit requeue because the generation-filtered watch does not react to a status update.
On the next pass, validate or enter `Suspended`.
Do not reset recorded progress.
Cover normal and failed validation, initial suspension, patch conflicts, existing state, controlled envtest resource absence, and ordered E2E status observations.

## Task 9: Investigate #200 slow reconcile passes

Add scoped structured operation timings without SQL or connection details.
Reproduce slow clone and follow passes, identify the responsible operation, and write a discriminating regression plus a concrete fix design.
Separately correct the confirmed scheduling contribution by scheduling successful active-worker polling from reconcile start, subtracting elapsed time, and keeping a positive requeue when overdue.
Preserve other scheduling paths.
Do not close #200 after instrumentation or scheduling compensation alone.

## Completion gates

For each task, start with a regression that fails against existing behavior.
Do not treat absent fixtures, skipped scenarios, or missing observations as success.
Run excluded resume coverage explicitly where required and verify cleanup of test-owned locks, processes, privileges, and replication state.
Run task-level implementation and review loops before the next task.
After the final task, complete an independent whole-branch review and collect exact command and CI evidence before merge.
