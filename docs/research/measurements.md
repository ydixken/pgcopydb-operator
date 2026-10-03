# Measured findings

Numbers measured against real databases, moved out of the comments that would otherwise carry them.
Each section is referenced from the code by anchor.

## Progress sampling

Where `internal/progress/progress.go` decides whether a table still owes the copy.
The counts are read exactly, with a one-row select per table, because no size function answers the question.

`sampleScript` asks each database for one row of sizes and counts.
The source row ends in a sixth figure, the tables that hold rows on the source and none on the target, and their names follow on an `owed=` line.
The source is asked about the target's tables rather than its own, because the target holds the in-scope schema, and an unscoped source would count toward a total the copy can never reach.
The sample tests presence rather than a row count, because storage cannot tell an empty table from a copied one, and a live source runs ahead of the copy's snapshot, so a count compared against it never settles in follow mode (see [An exact row count held the follow gate against a live source](#an-exact-row-count-held-the-follow-gate-against-a-live-source)).
A table this worker is copying into owes the copy and is never probed, because the probe would read its whole uncommitted heap (see [A presence probe read a whole uncommitted copy](#a-presence-probe-read-a-whole-uncommitted-copy)).
Targets before PostgreSQL 14 have no `pg_stat_progress_copy`, so they probe every table they do not see locked.
Bytes come from `pg_table_size`, because its neighbours add the indexes or drop the TOAST (see the last two sections below).
`pg_table_size` would wait on a table a copy worker holds under AccessExclusiveLock: a whole-table copy, or every partition under a truncated parent.
Such a table counts the bytes its own copy has streamed, which a target before PostgreSQL 14 cannot report, so it counts none there.
This worker's backends are those named `pgcopydb...` from the sampler's own client address, so another migration's copy into the same database is not mistaken for this one's.
Among them, a copy worker is one named a copy worker or whose last statement was a COPY, so the sample waits out an index worker's short ALTER on a copied table.
Unlike `stageScript`, it does not require the backend to be active, because a backend idle in its transaction after a COPY still holds the copy's lock.
A failed side prints an empty row and parses to no sample, never to zero.

### Storage cannot tell an empty table from a copied one

A table's TOAST relation occupies a page from the moment the schema is restored.
A `pg_table_size` test therefore counted an 848MB table with no rows on the target as copied (issue #277).

### A presence probe read a whole uncommitted copy

Measured in podman on PostgreSQL 14 and 18, with one table holding an open copy that had not committed.
`select 1 from <table> limit 1` scanned every uncommitted page and returned no row, while `pg_table_size` on the same table took about a millisecond.
From about 7.8GB of uncommitted heap the target's sample ran past its 5-second statement timeout.
A table pgcopydb copies whole is truncated in the copy's own transaction, so the probe and `pg_table_size` both waited on its AccessExclusiveLock instead.

### pg_total_relation_size counts the indexes

An empty table carrying a primary key counted as copied.
A target holding one populated table of three reported two.

### pg_relation_size counts only the main fork

A table of documents reported 256kB where `pg_table_size` reported 66MB.
On an e2e clone that read as 512MiB to copy while the target grew past 3GB.

## Cutover verification

Where `buildVerifyJob` in `internal/controller/resources.go` decides whether the target applied everything up to endpos.
Only exact equality of the origin progress and endpos proves the drain, so the gate falls back to comparing content.

### A byte tolerance blessed a cutover that had lost commits

Measured live: 1040 bytes below endpos against the 8192 the tolerance allowed, with the last three commits gone.
A single-row commit measures about 347 bytes.

### endpos and the origin coincide only when nothing wrote in between

Measured live: 56 bytes apart right after write activity.
endpos is the source's WAL head at the approval instant, while the origin holds the last commit the apply committed.

## Clone completion (issue #277)

Two places where pgcopydb's own bookkeeping reported a copy complete that was not, and one where the operator's own check for it could not clear.

### A stale estimate outlived the catalog that produced it

In `recordCloneProgress`, `internal/controller/follow.go`.
A pass that lost its status patch, or a worker restarted after the verify Job existed, left the copy-time estimate standing as the final figure: 81 of 81 indexes over a catalog that counted 75.

### The clone-done marker reported a table no rows had reached

In `confirmBaseCopy`, `internal/controller/migration_controller.go`.
`--resume` reported 57 of 57 tables done with 848MB on the source and the target empty.

### An exact row count held the follow gate against a live source

In `sampleScript`, `internal/progress/progress.go`.
The sampler once compared each table's row count on the target against the live source, and a follow migration's source runs ahead of the copy's snapshot until the stream catches up.
Measured live on a release candidate, with the migration's walsender paused and 20000 rows committed on the source after the snapshot: 16 of 17 tables done for ten minutes with no copy backend left, the lag byte-identical across every poll, and `CloneCompleted` latched on `TablesEmptyOnTarget`.
The sampler tests presence instead, which the same table passes once one row lands.

## Worker sizing

Where `internal/pgcopydb/defaults.go` picks the CPU a worker requests and derives the table jobs from it.
COPY spends its time on the network and on the servers, so neither number is bound by the worker's own compute.

### A copy worker mid-COPY is not CPU bound

A copy worker traced mid-COPY measured 7-12% CPU, roughly 14kB in flight per trip.
Measured on one wide TOASTed table.
The four-core default is headroom rather than a requirement, and a smaller request would very likely serve.
A database of narrow rows puts more work per byte on the worker and has not been measured.

### Table jobs past four buy nothing on one dominant table

Measured 4 jobs at 103-131 MiB/s against 16 jobs at 102 MiB/s on the same fixture.
A table is one COPY stream unless pgcopydb splits it, so splitting is what adds streams.

## Clone-stage probe

Where `stageScript` in `internal/progress/progress.go`, the first query of every sample, counts pgcopydb's own backends on the target to tell a running copy from its vacuum tail.
Copy workers count by connection, the tail only while active.
Index and vacuum workers count by connection too, as proof that the copy started: pgcopydb restores the schema before it starts any of them (`STEP 3` in its `cli_clone_follow.c`).

### A copy worker's connection outlives the statement it is running

Sampled across a whole base copy on a live worker 2026-08-30.
Four copy workers connected in every sample, zero the instant it ended, while the active count dipped to zero mid-copy and read as the tail.

## Sampling cadence

Where `copyPollInterval` and `recordSizes` in `internal/controller/sampler.go` decide how often a copy is sampled and when its target size counts.

### One sample costs under a second

Measured during the release-candidate e2e behind issue #200: one psql query took 70 to 90 ms, and the exec round trip around the script took about 480 ms.
A sample is one exec running three psql calls, so about 0.7 s.
At a 5-second interval that fills a seventh of each gap; at 2 seconds it would fill a third, and its own jitter would show in the spacing.

### The first size sample of a copy read the target before pgcopydb cleaned it

Measured on the v0.17.0 post-release e2e, with the target size read off the target primary every 2.3 seconds as ground truth.
A follow migration cloned into a database an earlier spec had filled.
Its first point, scraped four seconds after the target had dropped to 151 MB and started refilling, still read 3.03 GB.
A point that old can only come from a sample taken before pgcopydb dropped and restored the schema.

## Shell portability of the progress gate

Where `GateScript` in `internal/progress/progress.go` renders a `case` statement that the verify Job embeds inside `$( )`.
The pattern list opens with "(", the unambiguous POSIX form, because a shell may read the bare pattern's own ")" as the end of the substitution.

### Shells disagree about a bare case pattern inside a command substitution

Measured across bash 3.2, 4.0, 4.4, 5.1, 5.3 and the shipped runner image.
bash 3.2 refuses the bare form, bash 4.0 and later accept it, and dash accepts it.
The runner image links `/bin/sh` to dash, measured on the shipped image, so nothing we ship runs a shell that refuses it: this is portability rather than a live bug.

## Build and CI timings

Where the workflows under `.github/workflows/` and the build-config test decide what to emulate, what to cross-compile, and what to cache.
Each figure is a wall-clock or transfer cost from a real run.

### QEMU emulation was the whole cost of the builder image

In `.github/workflows/pgcopydb-builder.yml`.
Building both architectures at once on the cluster runner, with arm64 under QEMU, cost 933 of 964 seconds.
pgcopydb is a C project whose vendored sqlite3.c is a single 9MB translation unit, so more cores do not help and emulation cannot be cached around.

### Emulated against native go build for the manager image

In `test/buildconfig/buildconfig_test.go`.
Measured on release run 33242100882: 544.7s emulated against 55.6s native for the same `go build`.
Without `--platform=$BUILDPLATFORM` a FROM resolves to the stage's target platform, so buildx runs the whole Go toolchain under QEMU for the arm64 half.

### The release job's builder build spent most of its time under QEMU

In `.github/workflows/release.yml`.
Roughly 15 of that job's 20 minutes went to emulating the arm64 half, before the per-architecture split moved it off QEMU.

### A layer cache for the manager image cost more than it returned

In `.github/workflows/release.yml`.
Exporting it cost 92 of that job's 199 seconds and 1.1GB on the node, to make one layer reusable: go mod download.
The layer that actually costs time, go build, is invalidated by the commit being released.

### A GitHub cache round trip for the runner image

In `.github/workflows/github-runner-image.yml`.
Sending a cache to the internet and pulling it back cost 444MB and about two minutes a run.
The layer cache goes on the node beside the Go caches instead.

## E2E fixture sizing

Where `test/e2e/e2e_suite_test.go` sizes the fixture servers and bounds the waits that depend on how fast they run.

### Four CPUs per fixture server did not seed faster than two

Seeding ran at 16.6 MiB/s at four against 15.9 at two, which is noise.
Issue #146 later found why: the seeding backend waits on WAL write and fsync, never on a core.
Four is not free either.
Longhorn's instance manager holds a CPU guarantee on every node, so six four-CPU instances leave no room for a worker and the run dies on FailedScheduling instead of running slowly.

### CPU governor against follow throughput

The A/B/A replay on 2026-09-13 (issue #260) is published in full under "Backlog drain budget" in the [follow diagnostics design note](../design/follow-diagnostics.md#backlog-drain-budget), with the receive rates, the resume-to-cutover times and the 300-second gate per phase.
It is not repeated here.
