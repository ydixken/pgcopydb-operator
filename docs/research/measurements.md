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
