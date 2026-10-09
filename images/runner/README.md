# Runner image

Image for the migration Jobs the operator spawns.
It contains pgcopydb 0.18, patched and copied in from a separate image (see below), and the PostgreSQL 18 client tools (pg_dump, pg_restore, psql) from the PGDG apt repo on `debian:trixie-slim`.
It does not reuse the upstream `dimitri/pgcopydb:v0.18` image, which bundles postgresql-client-16 and a passwordless-sudo user: pg_dump/pg_restore MUST be at least the target's major version, and PGDG trixie ships postgresql-client-18 for both amd64 and arm64.

## Why pgcopydb comes from a fork

Stock pgcopydb 0.18 cannot report progress: `pgcopydb list progress` always fails on a broken SQL query ([dimitri/pgcopydb#1036](https://github.com/dimitri/pgcopydb/issues/1036)) and corrupts the stored filtering of a filtered catalog along the way ([#1038](https://github.com/dimitri/pgcopydb/issues/1038)), which kills concurrent or resumed `clone --filters` runs.
The operator needs that command, so this image `COPY --from`s the binary out of [images/pgcopydb-builder](../pgcopydb-builder/README.md), which compiles it from [ydixken/pgcopydb](https://github.com/ydixken/pgcopydb) branch `v0.18-fixes`, pinned to commit [`c682dce859a770adeffd10a38001aa7cd1bbb87c`](https://github.com/ydixken/pgcopydb/commit/c682dce859a770adeffd10a38001aa7cd1bbb87c).
The version string is `0.18.74.gc682dce`, derived from `git describe` (`v0.18-74-gc682dce`) by removing the leading `v` and replacing dashes with dots; the build canary and release smoke test both assert it.
The Git distance from upstream v0.18 is seventy-four commits, including merge commits.
The runner pins the builder's multi-platform index `sha256:a4b4c052d2b5a00667b4d775c577742f7cb3ec96b1f2b073f6f8e987d9cef1e4`, published by [builder run `37895605293`](https://github.com/ydixken/pgcopydb-operator/actions/runs/37895605293).

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

[Fork PR #12](https://github.com/ydixken/pgcopydb/pull/12), merged as `22e29c3`, adds partition topology checks to schema and data comparison.
Data comparison scans each selected storage leaf once; see [Partitioned tables](../../docs/operations/verification.md#partitioned-tables) for supported topologies and filter scope.
The merged commit passed [Run Tests `37177374540`](https://github.com/ydixken/pgcopydb/actions/runs/37177374540) and [Nightly Tests `37177376486`](https://github.com/ydixken/pgcopydb/actions/runs/37177376486).

Three more fork PRs, merged up to `7fddd6f`, fix cases where the target silently diverged from the source:

1. [Fork PR #15](https://github.com/ydixken/pgcopydb/pull/15) sets `row_security` to `off` on source, target, apply and `compare data` sessions, as pg_dump does.
   A table whose policies apply to the migration role now fails the copy with SQLSTATE 42501 instead of copying only the visible rows.
2. [Fork PR #16](https://github.com/ydixken/pgcopydb/pull/16) restores `REPLICA IDENTITY USING INDEX` on the target after post-data; before, those tables arrived with `REPLICA IDENTITY DEFAULT`.
   It also adds the new catalog column when it opens a work directory written by an older pgcopydb, so a retry on an upgraded runner resumes instead of failing with `no such column: i.isreplident`.
   Such a resumed run still leaves those tables at `REPLICA IDENTITY DEFAULT`.
3. [Fork PR #14](https://github.com/ydixken/pgcopydb/pull/14) applies each UPDATE or DELETE on a `REPLICA IDENTITY FULL` table without a key to one row, through `(tableoid, ctid)`.
   Before, a change to one of several identical rows changed all of them on the target.

The merged commit passed [Run Tests `37383701991`](https://github.com/ydixken/pgcopydb/actions/runs/37383701991) and [Nightly Tests `37383705906`](https://github.com/ydixken/pgcopydb/actions/runs/37383705906).

[Fork PR #17](https://github.com/ydixken/pgcopydb/pull/17), merged as `972e221`, fixes a misleading error on `--resume`.
When pgcopydb cannot read the previous run's catalog, for example `[SQLite] disk I/O error` on a full work volume, it used to exit with `Option --resume requires option --not-consistent`, although the retry passes that flag.
It now exits with `Failed to check options against the previous run, see above for details`, after the line that names the read error.
The merged commit passed [Run Tests `37409560033`](https://github.com/ydixken/pgcopydb/actions/runs/37409560033) and [Nightly Tests `37409562393`](https://github.com/ydixken/pgcopydb/actions/runs/37409562393).

[Fork PR #18](https://github.com/ydixken/pgcopydb/pull/18), merged as `93eda1d`, fixes two cases on a `REPLICA IDENTITY FULL` table without a key.
With `test_decoding`, the old row of an UPDATE or DELETE leaves out its NULL columns, so the one-row match from #14 compared fewer columns than the row has and could change a row that differs only in a NULL column.
pgcopydb now adds each missing column back as NULL and matches it with `IS NULL`; generated columns stay out of the match.
Under every plugin, a change whose old row is all NULL became a statement without parameters, which apply skipped, so the target kept the row; apply now runs it.
The merged commit passed [Run Tests `37476971179`](https://github.com/ydixken/pgcopydb/actions/runs/37476971179) and [Nightly Tests `37476976824`](https://github.com/ydixken/pgcopydb/actions/runs/37476976824).

Four fork PRs on top of `93eda1d` fix follow cases where the target lost committed source changes without an error, while the target replication origin still reached endpos:

1. [Fork PR #19](https://github.com/ydixken/pgcopydb/pull/19) transforms transactions in commit order.
   Before, a transaction that began on the source before another one committed was skipped, or apply stopped with exit 12 when it was the last commit before endpos ([#356](https://github.com/ydixken/pgcopydb-operator/issues/356)).
   After a restart, apply now takes the unapplied transactions in replay.db in commit order, where it skipped one of them before, and it drops the part of a transaction that a killed apply left there, where the resumed run failed on a duplicate key.
2. [Fork PR #20](https://github.com/ydixken/pgcopydb/pull/20) replays every table of a multi-table `TRUNCATE` in one statement, with `RESTART IDENTITY` when the source used it.
   Before, `pgoutput` truncated only the first table ([#358](https://github.com/ydixken/pgcopydb-operator/issues/358)), and a `TRUNCATE` of tables linked by a foreign key stopped apply with exit 12.
   With `wal2json`, pgcopydb still truncates only the last table of the statement and never restarts identity.
3. [Fork PR #21](https://github.com/ydixken/pgcopydb/pull/21) keeps every row of a multi-insert record in output.db, under every plugin.
   A source `COPY` writes one such record per heap page, and all its rows share one LSN, so receive kept only the last row of each page ([#357](https://github.com/ydixken/pgcopydb-operator/issues/357)).
4. [Fork PR #23](https://github.com/ydixken/pgcopydb/pull/23) sends the origin setup and the `COMMIT` of each data transaction as one query, and moves the origin back when the target refuses that `COMMIT` and the connection stays up.
   On PostgreSQL 16 and later, a transaction that aborts after the setup still advances the origin, so the next run skipped a transaction the target never committed.
   A target backend terminated after the origin setup and before the `COMMIT` record is written can still leave the origin advanced, as can a `COMMIT` that breaks the connection.

[Fork PR #22](https://github.com/ydixken/pgcopydb/pull/22) changes a fork test only: it waits for the follow snapshot instead of sleeping one second.
The merge of #23, `bb8dbfc`, passed [Run Tests `37852957788`](https://github.com/ydixken/pgcopydb/actions/runs/37852957788) and [Nightly Tests `37852960601`](https://github.com/ydixken/pgcopydb/actions/runs/37852960601).

[Fork PR #24](https://github.com/ydixken/pgcopydb/pull/24), merged as `2aa91e7`, stops receive from freeing the `COPY_OUT` result twice ([#362](https://github.com/ydixken/pgcopydb-operator/issues/362)).
When the walsender closed the connection after receive reached endpos and before it sent CopyDone, for example through `wal_sender_timeout`, receive aborted with `free(): double free detected in tcache 2` and follow exited 12.
Receive now reconnects and stops at endpos.
The merged commit passed [Run Tests `37886654042`](https://github.com/ydixken/pgcopydb/actions/runs/37886654042) and [Nightly Tests `37886656126`](https://github.com/ydixken/pgcopydb/actions/runs/37886656126).

[Fork PR #25](https://github.com/ydixken/pgcopydb/pull/25), merged as `c682dce`, stops a failed `IDENTIFY_SYSTEM` from finishing the source connection twice ([#364](https://github.com/ydixken/pgcopydb-operator/issues/364)).
When the source ended the walsender while receive reconnected, receive aborted with `free(): invalid pointer` and follow ended the attempt.
Receive now exits with the source error (code 6), and the next attempt resumes.
The PR also clears two leaked results on the replication path.
The merged commit passed [Run Tests `37893940302`](https://github.com/ydixken/pgcopydb/actions/runs/37893940302) and [Nightly Tests `37893942845`](https://github.com/ydixken/pgcopydb/actions/runs/37893942845).

> [!warning]
> Each source transaction waits for target WAL durability before apply progress advances.
> This raises latency for workloads with many small transactions; [Performance tuning](../../docs/operations/performance.md#follow-receive-and-apply) has the measured cost.
> A shutdown request does not guarantee that all received work was applied; interrupted work may need resume from the target replication origin.

The manager and chart allow `0.18.74.gc682dce`, `0.18.72.g2aa91e7`, `0.18.70.gbb8dbfc`, `0.18.39.g93eda1d`, `0.18.36.g972e221`, `0.18.34.g7fddd6f`, `0.18.22.g22e29c3`, `0.18.15.gea2dc96`, `0.18.13.g4873c18`, `0.18.10.gaadc4bf`, and `0.18.5.ge37d2bd` to run the catalog progress poll.
We keep all ten older versions so upgrading the operator does not suppress counters for existing workers.
Version `0.18.72.g2aa91e7` has the #24 receive fix but not the #25 reconnect fix.
Version `0.18.70.gbb8dbfc` has the four follow fixes but not the #24 receive fix.
Version `0.18.39.g93eda1d` has the #18 fixes but not the four fixes from #19, #20, #21 and #23.
Version `0.18.36.g972e221` has the `--resume` error fix but not the #18 fixes.
Version `0.18.34.g7fddd6f` has the three fixes above but not the `--resume` error fix.
Version `0.18.22.g22e29c3` has partition comparison but none of the three fixes, and `0.18.15.gea2dc96` has neither.
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
