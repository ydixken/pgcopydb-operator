# Lessons

- When unsigned commits are explicitly authorized, use `--no-gpg-sign` per invocation.
  Do not persist that exception in global or repository Git configuration.

- Trace early exits when composing preflight blocks; failure summaries are part of the condition-message contract.
  Stop failed prerequisites before per-database probes can push the root cause out of the log tail.

- A deterministic validation error must reach a persisted terminal condition, not the reconcile retry channel.
  Test the full reconcile path with admission bypassed, including status, events, and the absence of worker Jobs.

- `status.attempts` counts worker Jobs, not preflight Jobs.
  Preflight-failure coverage must assert zero worker attempts and no first worker Job.

- Keep environment failures separate from feature defects when the user is provisioning missing tools.
  Preserve the tests and lint gates unchanged, and rerun them after provisioning.
  When authorized, reproduce unrelated failures on the clean baseline before expanding the fix.

- The E2E `psql` helper connects as `postgres`, while migration fixtures use `app`.
  Create follow-test tables under `SET ROLE app` and verify ownership before lifecycle assertions.
  Exercise publication creation under the migration role in the fixture regression; a successful superuser setup does not prove that the migration can publish the table.
- Batch approved related issues after local formatting and lint to reduce trusted runner consumption.
  Keep per-issue tests and reviews, whole-batch integration review, and full E2E on the release candidate that carries the batch.
- After design approval, dispatch one focused implementer instead of repeating planning and audit passes.
  Use one independent final-diff review for each coherent batch unless new risk changes the scope.
- Write regression coverage with each behavior change, but run functional tests only in CI.
  CI also runs build and documentation verification.
  Locally, format touched files and run `task lint` only.
- Do not add local test, diagnostic, documentation-build, smoke, or skill-evaluation gates.
- Use CI wait time for approved unpublished local batch work.
  Keep exact-head CI, review, merge, privilege, and publication gates intact.
- Treat physical storage counters as nonmonotonic across recovery.
  Require valid changed positive values and preserve the CI-only verification policy.
- Use a focused local run when a failed suite lacks the evidence for one assertion.
  A focused success is diagnostic evidence and never replaces the full suite on the release candidate.
- Preserve mount propagation when an observer must inspect nested read-only storage mounts.
  A readonly root bind alone does not prove that descendants remain visible and read-only.
- When an E2E prerequisite cannot be proven on the actual runner path, remove it rather than extend it with more infrastructure or diagnostics.
- Verify CRD ownership and reconciliation on the actual cluster before documenting an upgrade prerequisite.
- Mark a remote gate complete only with a run URL and the exact verified SHA.
- Mark a removed historical gate as superseded when its outcome has no recorded evidence, rather than changing its checkbox to assert failure.
- Generated-file drift checks must reject untracked output as well as tracked changes.
  Assert required check presence and ordering without coupling unrelated step adjacency.
- Print the offending paths when a generated-file check fails, without swallowing generation or Git failures.
  Document commit-then-lint sequencing when staged generated files deliberately fail the check.
- Distinguish independent sufficient causes from jointly required premises.
  Either `wal_log_hints` or data checksums can require hint-bit WAL; use the measured before/after behaviour to test the proposed fix.
- Retain a fork feature branch after downstream merge when the fix will be proposed upstream.
  Track upstream submission separately rather than deleting the branch during merge cleanup.
- A completed Migration can retain a replay sample from before endpos, and catch-up allows nonzero lag.
  Prove feedback directly before cutover; use final endpos, drain verification, and data checks for completion.
- When the user asks for operator-only remediation, keep fork-level resume and eviction recovery separate.
  Do not expand the scope without approval.
- When the user replaces a merge restriction, record the new authorization and preserve the review gate for the current task.
  Later release success requires its own exact-SHA evidence.
- The user requires the next RC run to use `E2E_SCALE=0.1`.
  Set that value in the tag-triggered `release.yml` suite and its contract test; it has no scale input.
  Do not infer a new default for local scripts or the independently configured stable-release workflow.
  Keep fixed WAL-noise fixtures unscaled so they still exceed the default `16Mi` lag allowance.
- Distinguish objects left by a prior full clone from objects selected by the next filtered restore.
  Keep baseline failures, preventive fixture changes, focused passes, and pending full-suite gates separate in evidence summaries.
- Reviewer recommendations do not override the CI-only functional-test rule; unknown race-instrumented timing belongs to exact-head CI, not a new local gate.
- Publish a diagnostic result and its validity together; an interim failure label can mislead concurrent readers even when every field is locked.
- Redact known DNS/transport error phrases as whole lines: private hostnames can appear without a URI or IP address.
