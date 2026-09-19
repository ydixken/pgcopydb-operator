# Measured findings

Numbers measured against real databases, moved out of the comments that would otherwise carry them.
Each section is referenced from the code by anchor.

## Progress sampling

Where `internal/progress/progress.go` decides whether a table still owes the copy.
The counts are read exactly, with a one-row select per table, because no size function answers the question.

### Storage cannot tell an empty table from a copied one

A table's TOAST relation occupies a page from the moment the schema is restored.
A `pg_table_size` test therefore counted an 848MB table with no rows on the target as copied (issue #277).

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

Two places where pgcopydb's own bookkeeping reported a copy complete that was not.

### A stale estimate outlived the catalog that produced it

In `recordCloneProgress`, `internal/controller/follow.go`.
A pass that lost its status patch, or a worker restarted after the verify Job existed, left the copy-time estimate standing as the final figure: 81 of 81 indexes over a catalog that counted 75.

### The clone-done marker reported a table no rows had reached

In `confirmBaseCopy`, `internal/controller/migration_controller.go`.
`--resume` reported 57 of 57 tables done with 848MB on the source and the target empty.

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

Where `finalizingScript` in `internal/progress/progress.go` counts pgcopydb's own backends on the target to tell a running copy from its vacuum tail.
Copy workers count by connection, the tail only while active.

### A copy worker's connection outlives the statement it is running

Sampled across a whole base copy on a live worker 2026-08-30.
Four copy workers connected in every sample, zero the instant it ended, while the active count dipped to zero mid-copy and read as the tail.

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
Longhorn's instance manager holds a guaranteed 6 CPUs per node here, so six four-CPU instances leave no room for a worker and the run dies on FailedScheduling instead of running slowly.

### CPU governor against follow throughput

The A/B/A replay on 2026-09-13 (issue #260) is published in full under "Backlog drain budget" in the [follow diagnostics design note](../design/follow-diagnostics.md#backlog-drain-budget), with the receive rates, the resume-to-cutover times and the 300-second gate per phase.
It is not repeated here.
