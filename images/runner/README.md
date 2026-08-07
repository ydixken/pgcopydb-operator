# Runner image

Image for the migration Jobs the operator spawns. It contains pgcopydb 0.18 and the PostgreSQL 18 client tools (pg_dump, pg_restore, psql), both from the PGDG apt repo on `debian:bookworm-slim`. It does not reuse the upstream `dimitri/pgcopydb:v0.18` image because that one bundles postgresql-client-16, and pg_dump/pg_restore MUST be at least the target's major version (our targets run PostgreSQL 17).

The image runs as the non-root user `runner` (uid 65532) with `/work` as the working directory, where Jobs mount the migration work volume. No credentials are baked in: pgcopydb reads `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, and `PGPASSFILE` from the Job's environment. The entrypoint is empty, so the Job supplies the full command (`pgcopydb clone`, `pgcopydb follow`, and so on).

The base image is pinned by tag and digest; Renovate bumps both together. apt package versions float within their majors, and a build-time canary fails the build if pgcopydb drifts off 0.18 or the client tools off 17.
