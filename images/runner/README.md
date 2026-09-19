# Runner image

Image for the migration Jobs the operator spawns.
It contains pgcopydb 0.18, patched and copied in from a separate image (see below), and the PostgreSQL 18 client tools (pg_dump, pg_restore, psql) from the PGDG apt repo on `debian:trixie-slim`.
It does not reuse the upstream `dimitri/pgcopydb:v0.18` image, which bundles postgresql-client-16 and a passwordless-sudo user: pg_dump/pg_restore MUST be at least the target's major version, and PGDG trixie ships postgresql-client-18 for both amd64 and arm64.

## Why pgcopydb comes from a fork

Stock pgcopydb 0.18 cannot report progress: `pgcopydb list progress` always fails on a broken SQL query ([dimitri/pgcopydb#1036](https://github.com/dimitri/pgcopydb/issues/1036)) and corrupts the stored filtering of a filtered catalog along the way ([#1038](https://github.com/dimitri/pgcopydb/issues/1038)), which kills concurrent or resumed `clone --filters` runs.
The operator needs that command, so this image `COPY --from`s the binary out of [images/pgcopydb-builder](../pgcopydb-builder/README.md), which compiles it from [ydixken/pgcopydb](https://github.com/ydixken/pgcopydb) branch `v0.18-fixes`, pinned to commit [`ea2dc96a47c2f7676d71a4967d044a1e469e4110`](https://github.com/ydixken/pgcopydb/commit/ea2dc96a47c2f7676d71a4967d044a1e469e4110).
The version string is `0.18.15.gea2dc96`, derived from `git describe` (`v0.18-15-gea2dc96`) by removing the leading `v` and replacing dashes with dots; the build canary and release smoke test both assert it.
The Git distance from upstream v0.18 is fifteen commits, including merge commits, not an upstream release named v0.18.15.
The runner pins the builder's multi-platform index `sha256:1145d382fc74bb35c1b8a19a42ed9469639b66d405b560ad9777a7111c9d3b33`, published by [builder run `35152785053`](https://github.com/ydixken/pgcopydb-operator/actions/runs/35152785053).

The five patches inherited from `e37d2bd` are:

1. [`82aa566`](https://github.com/ydixken/pgcopydb/commit/82aa566): count table bytes from `s_table_size` in `list progress` (upstream [#1041](https://github.com/dimitri/pgcopydb/pull/1041)).
2. [`ea87951`](https://github.com/ydixken/pgcopydb/commit/ea87951): keep adopted filters in memory without overwriting the stored filtering (upstream [#1042](https://github.com/dimitri/pgcopydb/pull/1042)).
3. [`ed62eec`](https://github.com/ydixken/pgcopydb/commit/ed62eec): make single-database `compare data` exit nonzero when data differs.
4. [`1393b60`](https://github.com/ydixken/pgcopydb/commit/1393b60): stop overwriting `replay_lsn` with cutover endpos.
5. [`e37d2bd`](https://github.com/ydixken/pgcopydb/commit/e37d2bd): keep keepalives off the apply cursor and drain committed spool work before completion or rotation.

[Fork PR #7](https://github.com/ydixken/pgcopydb/pull/7) adds five commits, closing fork issue #5 and addressing fork issue #6:

1. [`01bfe16`](https://github.com/ydixken/pgcopydb/commit/01bfe16): batch receive writes in SQLite transactions, committed at source COMMIT, flush, and close/rotation, with `synchronous=FULL` unchanged.
2. [`92e9209`](https://github.com/ydixken/pgcopydb/commit/92e9209): pin the Pagila test fixture to its pre-v4 commit.
3. [`58abca7`](https://github.com/ydixken/pgcopydb/commit/58abca7): confirm target COMMIT results before publishing apply progress.
4. [`6277199`](https://github.com/ydixken/pgcopydb/commit/6277199): add receive SIGKILL/resume and flush-order regressions.
5. [`aadc4bf`](https://github.com/ydixken/pgcopydb/commit/aadc4bf): stop follow cleanly after confirmed apply, use `synchronous_commit=on` for SQLite-apply transactions, and cover shutdown and pre-COMMIT apply termination/resume.

[Fork PR #9](https://github.com/ydixken/pgcopydb/pull/9), merged as `4873c18`, adds certified keepalive feedback and CDC regression fixes.
The receiver can advance network replay and flush feedback to a genuine primary keepalive only after initialized durable apply covers every stored, non-skipped COMMIT, including retained spool, with no receive transaction open and endpos unset.
The certified feedback floor is monotonic and leaves the data apply cursor, target replication origin, and sentinel replay position unchanged.
This keeps the confirmed-COMMIT and `synchronous_commit=on` guarantees from `aadc4bf`; it does not replace post-cutover drain verification.

[Fork PR #11](https://github.com/ydixken/pgcopydb/pull/11), merged as `ea2dc96`, lets a retry initialize a missing sentinel only when that invocation created a fresh replication slot.
Setup preserves every existing sentinel field, including startpos, endpos, apply mode, and receive/apply positions.
A retained slot without valid sentinel state, or a catalog SQL error, fails closed rather than resetting established progress.
The merged tree matches feature `5d10b14`, tested by [Run Tests `35150666775`](https://github.com/ydixken/pgcopydb/actions/runs/35150666775) and [Nightly Tests `35150664335`](https://github.com/ydixken/pgcopydb/actions/runs/35150664335).
This bootstrap recovery does not repair interrupted index builds, eviction damage, lost established-stream CDC files, or arbitrary corrupt metadata.

> [!warning]
> Each source transaction waits for target WAL durability before apply progress advances.
> This may raise latency for workloads with many small transactions; that cost is unmeasured.
> A shutdown request does not guarantee that all received work was applied; interrupted work may need resume from the target replication origin.

The manager and chart allow `0.18.15.gea2dc96`, `0.18.13.g4873c18`, `0.18.10.gaadc4bf`, and `0.18.5.ge37d2bd` to run the catalog progress poll.
We keep all three older versions so upgrading the operator does not suppress counters for existing workers.
Version `0.18.13.g4873c18` provides certified idle keepalive feedback but not the missing-sentinel bootstrap recovery.
Neither `0.18.10.gaadc4bf` nor `0.18.5.ge37d2bd` provides certified idle keepalive feedback.
This allowlist does not select or upgrade worker images.

Once an upstream release includes the required runtime fixes above, return to PGDG: swap `libgc1` for the `pgcopydb` package in the install line, drop the `COPY --from=pgcopydb` line, and update the version assertions and progress allowlists together.
`images/pgcopydb-builder` can then go away entirely.

The image runs as the non-root user `runner` (uid 65532) with `/work` as the working directory, where Jobs mount the migration work volume.
No credentials are baked in: pgcopydb reads `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, and `PGPASSFILE` from the Job's environment.
The entrypoint is empty, so the Job supplies the full command (`pgcopydb clone`, `pgcopydb follow`, and so on).

## Why it removes packages

Debian ships no fixed version for most of what a scanner reports here, so patching cannot help.
Removal can, for the parts a migration never executes:

| | findings | critical | high |
|---|---|---|---|
| stock trixie install | 241 | 16 | 30 |
| perl removed | 177 | 0 | 14 |
| plus util-linux, login, gzip | 92 | 0 | 3 |

perl accounted for every critical.
It arrives as a dependency of `postgresql-client-common`, whose only contribution is the pg_wrapper perl scripts in `/usr/bin`; `PATH` already prefers the real binaries in `/usr/lib/postgresql/18/bin`, so purging it removes a scripting language the image never runs.
util-linux, `login` and gzip are likewise untouched by any migration.

The three that stay are needed: `libacl1` because GNU sed links it, `libtinfo6` and `ncurses-base` because psql links readline.
An earlier attempt purged libacl1 as well and broke sed outright, which the build canary caught.

The removals happen inside the same `RUN` as the install.
Files deleted in a later layer still occupy the earlier one, so splitting them would leave the image the same size; done in one layer it drops from 176 MB to 122 MB.

`dpkg --purge --force-depends --force-remove-essential --force-remove-protected` leaves a package database that no longer satisfies its own dependencies.
In an image that never runs apt again this is inert, but anything added later that expects perl or util-linux will not work.

## Why not Alpine or Wolfi

Both were built, measured and rejected.

**Alpine** reported zero findings at 37 MB and does not work.
pgcopydb's CLI relies on GNU `getopt_long` permuting argv, which is what lets `pgcopydb clone --dir /work --not-consistent` parse options written after the subcommand.
musl's getopt does not permute; every worker Job died with `pgcopydb: unrecognized option: dir` and the migration burned its backoff limit without touching a database.
Upstream fixed the Alpine *build* in [dimitri/pgcopydb#193](https://github.com/dimitri/pgcopydb/pull/193), which says nothing about runtime argument handling.

**Wolfi** is glibc and worked correctly, at zero findings and 111 MB.
It was rejected because `cgr.dev` images sit behind Chainguard's catalog tiers, where the public tier serves only `:latest`, so the base would be a dependency on a vendor's pricing terms.
Wolfi's packages are Apache-2.0, but the images are a product.

Staying on Debian keeps glibc, a free base, and metadata for the retained packages, so a scanner can enumerate them despite the purged dependencies.
A `scratch` image assembled from copied binaries would also report near zero, while still containing openssl, krb5 and readline.

## Canary

The build-time canary checks the pinned pgcopydb build, client 18, GNU sed, GNU `timeout`, getopt argument permutation, and the absence of perl after package purging.
Progress sampling requires GNU `timeout` with `--signal=TERM --kill-after=1s 6s`; custom runners MUST provide this command as well as psql.
The canary executes that invocation after purging, because a missing timeout command would suppress all database progress readings.
The base image is pinned by tag and digest, and Renovate bumps both together.
