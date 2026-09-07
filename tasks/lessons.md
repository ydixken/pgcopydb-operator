# Lessons

- The E2E `psql` helper connects as `postgres`, while migration fixtures use `app`.
  Create follow-test tables under `SET ROLE app` and verify ownership before lifecycle assertions.
  Exercise publication creation under the migration role in the fixture regression; a successful superuser setup does not prove that the migration can publish the table.
