# Runner image

Image for the migration Jobs the operator spawns. It contains pgcopydb 0.18, patched and copied in from a separate image (see below), and the PostgreSQL 18 client tools (pg_dump, pg_restore, psql) from the PGDG apt repo on `debian:trixie-slim`. It does not reuse the upstream `dimitri/pgcopydb:v0.18` image because that one bundles postgresql-client-16, and pg_dump/pg_restore MUST be at least the target's major version.

## Why pgcopydb comes from a fork

Stock pgcopydb 0.18 cannot report progress: `pgcopydb list progress` always fails on a broken SQL query ([dimitri/pgcopydb#1036](https://github.com/dimitri/pgcopydb/issues/1036)) and corrupts the stored filtering of a filtered catalog along the way ([#1038](https://github.com/dimitri/pgcopydb/issues/1038)), which kills concurrent or resumed `clone --filters` runs.
The operator needs that command, so this image `COPY --from`s the binary out of [images/pgcopydb-builder](../pgcopydb-builder/README.md), which compiles it from [ydixken/pgcopydb](https://github.com/ydixken/pgcopydb) branch `v0.18-fixes`, pinned to commit `ea87951753f06361550c0a1357da7b42c3c55034`: upstream v0.18 plus the two fixes, sent upstream as [#1041](https://github.com/dimitri/pgcopydb/pull/1041) and [#1042](https://github.com/dimitri/pgcopydb/pull/1042).
The version string is `0.18.2.gea87951`, what `git describe` prints for that commit with dashes as dots, and the build canary and the release smoke test both assert it exactly.
Once an upstream release ships both fixes, revert here: swap `libgc1` for the `pgcopydb` package in the install line, drop the `COPY --from=pgcopydb` line, and point the version greps in this Dockerfile and the release workflow back at the release.
`images/pgcopydb-builder` can then go away entirely.

The image runs as the non-root user `runner` (uid 65532) with `/work` as the working directory, where Jobs mount the migration work volume. No credentials are baked in: pgcopydb reads `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, and `PGPASSFILE` from the Job's environment. The entrypoint is empty, so the Job supplies the full command (`pgcopydb clone`, `pgcopydb follow`, and so on).

## Why it removes packages

Debian ships no fixed version for most of what a scanner reports here, so patching cannot help. Removal can, for the parts a migration never executes:

| | findings | critical | high |
|---|---|---|---|
| stock trixie install | 241 | 16 | 30 |
| perl removed | 177 | 0 | 14 |
| plus util-linux, login, gzip | 92 | 0 | 3 |

perl accounted for every critical. It arrives as a dependency of `postgresql-client-common`, whose only contribution is the pg_wrapper perl scripts in `/usr/bin`; `PATH` already prefers the real binaries in `/usr/lib/postgresql/18/bin`, so purging it removes a scripting language the image never runs. util-linux, `login` and gzip are likewise untouched by any migration.

The three remaining are genuinely needed: `libacl1` because GNU sed links it, `libtinfo6` and `ncurses-base` because psql links readline. An earlier attempt purged libacl1 as well and broke sed outright, which the build canary caught.

The removals happen inside the same `RUN` as the install. Files deleted in a later layer still occupy the earlier one, so splitting them would leave the image the same size; done in one layer it drops from 176 MB to 122 MB.

The cost is honest to state: `dpkg --purge --force-depends --force-remove-essential --force-remove-protected` leaves a package database that no longer satisfies its own dependencies. In an image that never runs apt again this is inert, but anything later added that expects perl or util-linux will not work.

## Why not Alpine or Wolfi

Both were built, measured and rejected.

**Alpine** reported zero findings at 37 MB and does not work. pgcopydb's CLI relies on GNU `getopt_long` permuting argv, which is what lets `pgcopydb clone --dir /work --not-consistent` parse options written after the subcommand. musl's getopt does not permute; every worker Job died with `pgcopydb: unrecognized option: dir` and the migration burned its backoff limit without touching a database. Upstream fixed the Alpine *build* in [dimitri/pgcopydb#193](https://github.com/dimitri/pgcopydb/pull/193), which says nothing about runtime argument handling.

**Wolfi** is glibc and worked correctly, at zero findings and 111 MB. It was rejected because `cgr.dev` images sit behind Chainguard's catalog tiers, where the public tier serves only `:latest`, so the base would be a dependency on a vendor's pricing terms. Wolfi's packages are Apache-2.0, but the images are a product.

Staying on Debian keeps glibc, a free base, and an intact package database, so a scanner reports the truth rather than the absence of anything to enumerate. A `scratch` image assembled from copied binaries would also report near zero, while still containing openssl, krb5 and readline.

## Canary

Every assertion in the build-time canary stands for a way this image has actually broken: version drift off the pinned pgcopydb build or client 18; a purge that takes GNU sed with it; a libc whose getopt stops permuting; and perl surviving the purge. The base image is pinned by tag and digest, and Renovate bumps both together.
