# Lessons

- The E2E `psql` helper connects as `postgres`, while migration fixtures use `app`.
  Create follow-test tables under `SET ROLE app` and verify ownership before lifecycle assertions.
  Exercise publication creation under the migration role in the fixture regression; a successful superuser setup does not prove that the migration can publish the table.
- Batch approved related issues after local validation to reduce trusted runner consumption.
  Keep per-issue tests and reviews, whole-batch integration review, and full E2E on each batch's exact final head.
- After design approval, dispatch one focused implementer instead of repeating planning and audit passes.
  Use one independent final-diff review for each coherent batch unless new risk changes the scope.
- Run focused tests while coding, then run the full local gate once when the batch is ready.
  Do not repeat whole-suite runs without a changed risk or final-head change.
- Use CI wait time for approved unpublished local batch work.
  Keep exact-head CI, review, merge, privilege, and publication gates intact.
