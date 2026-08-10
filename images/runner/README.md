# Runner image

Image for the migration Jobs the operator spawns. It contains pgcopydb 0.18 and the PostgreSQL 18 client tools (pg_dump, pg_restore, psql). The client tools come from Alpine's `postgresql18-client`; pgcopydb is compiled from its `v0.18` tag in a builder stage, because no Alpine package exists. It does not reuse the upstream `dimitri/pgcopydb:v0.18` image because that one bundles postgresql-client-16, and pg_dump/pg_restore MUST be at least the target's major version.

The image runs as the non-root user `runner` (uid 65532) with `/work` as the working directory, where Jobs mount the migration work volume. No credentials are baked in: pgcopydb reads `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, and `PGPASSFILE` from the Job's environment. The entrypoint is empty, so the Job supplies the full command (`pgcopydb clone`, `pgcopydb follow`, and so on).

## Why Alpine

The image used to be `debian:bookworm-slim` with the PGDG apt repo. It reported 241 vulnerabilities, 16 of them critical, and **none had a fixed version available in Debian**, so no amount of patching moved it. Most of them were perl, which `postgresql-client-common` depends on and which the runner never executes. Rebasing onto trixie changed the total by nothing and cleared a single critical.

The same image on Alpine reports zero, at about a fifth of the size. That zero is a real one: the apk database is intact, so a scanner still enumerates every package in the image. Building from binaries copied onto `scratch` would also have reported near zero, but only because no package metadata would survive to be scanned, which is a different thing entirely.

Two costs come with it. We build pgcopydb ourselves rather than installing a package, so its version is a pinned git tag we bump deliberately. And Alpine is musl rather than glibc, which unit tests cannot speak to; `task e2e` is the gate, since it runs clone, follow, cutover and verification against real servers.

GNU `sed` is installed on purpose. The Job's prelude escapes passwords into `.pgpass` with sed, and Alpine's default is busybox, whose behaviour on that escaping is close but not guaranteed identical.

The base image is pinned by tag and digest; Renovate bumps both together, and tracks the pgcopydb tag through the `PGCOPYDB_VERSION` build argument. apk package versions float within the Alpine release, and a build-time canary fails the build if pgcopydb drifts off 0.18, the client tools off 18, or `sed` turns out not to be GNU.
