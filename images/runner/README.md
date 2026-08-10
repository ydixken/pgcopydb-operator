# Runner image

Image for the migration Jobs the operator spawns. It contains pgcopydb 0.18 and the PostgreSQL 18 client tools (pg_dump, pg_restore, psql). The client tools come from Wolfi's `postgresql-18-client`; pgcopydb is compiled from its `v0.18` tag in a builder stage, because no Wolfi package exists. It does not reuse the upstream `dimitri/pgcopydb:v0.18` image because that one bundles postgresql-client-16, and pg_dump/pg_restore MUST be at least the target's major version.

The image runs as uid 65532 (`nonroot`, which Wolfi already provides) with `/work` as the working directory, where Jobs mount the migration work volume. No credentials are baked in: pgcopydb reads `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, and `PGPASSFILE` from the Job's environment. The entrypoint is empty, so the Job supplies the full command (`pgcopydb clone`, `pgcopydb follow`, and so on).

## Why Wolfi

The image used to be `debian:bookworm-slim` with the PGDG apt repo. It reported 241 vulnerabilities, 16 of them critical, and **none had a fixed version available in Debian**, so no amount of patching moved it. Most of them were perl, which `postgresql-client-common` depends on and which the runner never executes. Rebasing onto trixie changed the total by nothing and cleared a single critical.

The same image on Wolfi reports zero. That zero is a real one: the apk database is intact, so a scanner still enumerates every package. Building from binaries copied onto `scratch` would also have reported near zero, but only because no package metadata would survive to be scanned, which is a different thing entirely.

## Why not Alpine

Alpine was tried first and is smaller still, at 37 MB against 111 MB. It does not work.

pgcopydb's CLI depends on GNU `getopt_long` permuting argv, which is what lets `pgcopydb clone --dir /work --not-consistent` parse options written after the subcommand. musl's getopt does not permute; it stops at the first non-option argument. Every worker Job died immediately with `pgcopydb: unrecognized option: dir`, and the migration failed its backoff limit before touching a database. Upstream fixed the Alpine *build* in [dimitri/pgcopydb#193](https://github.com/dimitri/pgcopydb/pull/193), which says nothing about runtime argument handling.

Wolfi is glibc, so argument handling matches the Debian image it replaces, while still being apk-based with a near-zero CVE baseline. The build-time canary now asserts this directly: it runs a `clone` with options after the subcommand and requires the "Options --source and --target are mandatory" error, so a libc that stops permuting fails the build instead of every migration on the cluster.

## Other notes

GNU `sed` is installed on purpose. The Job's prelude escapes passwords into `.pgpass` with sed, and the busybox applet's behaviour on that escaping is close but not guaranteed identical.

Build parallelism is capped at `-j4` rather than `-j$(nproc)`. The release builds arm64 under QEMU, where each `cc1` costs far more memory than it does natively, and one job per core is enough to get the compiler OOM-killed.

The base image is pinned by digest, because Chainguard rebuilds the floating tag constantly; Renovate bumps it and tracks the pgcopydb tag through the `PGCOPYDB_VERSION` build argument. apk package versions float, and the canary fails the build if pgcopydb drifts off 0.18, the client tools off 18, `sed` turns out not to be GNU, or option permutation stops working.
