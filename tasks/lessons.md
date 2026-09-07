# Lessons

- The E2E `psql` helper connects as `postgres`, while migration fixtures use `app`.
  Create follow-test tables under `SET ROLE app` and verify ownership before lifecycle assertions.
  Exercise publication creation under the migration role in the fixture regression; a successful superuser setup does not prove that the migration can publish the table.
- Batch approved related issues after local validation to reduce trusted runner consumption.
  Keep per-issue tests and reviews, whole-batch integration review, and full E2E on each batch's exact final head.
