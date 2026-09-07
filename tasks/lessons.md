# Lessons

- The E2E `psql` helper connects as `postgres`, while migration fixtures use `app`.
  Create follow-test tables under `SET ROLE app` and verify ownership before lifecycle assertions.
  Exercise publication creation under the migration role in the fixture regression; a successful superuser setup does not prove that the migration can publish the table.
- Batch approved related issues after local formatting and lint to reduce trusted runner consumption.
  Keep per-issue tests and reviews, whole-batch integration review, and full E2E on each batch's exact final head.
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
