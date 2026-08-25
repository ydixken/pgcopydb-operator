# Suspend, retries, deletion

The day-2 lifecycle of a Migration: pausing it, how the operator retries failed attempts, and what deletion cleans up.

## Suspend

`spec.suspend: true` deletes the worker Job (foreground, so pgcopydb receives SIGTERM and shuts down cleanly) and keeps the work volume; phase becomes `Suspended`. Setting it back to false starts the next attempt, which resumes from the work-dir catalogs.

Two things to know:

- **WAL retention warning**: suspending a live migration does NOT drop the replication slot. The slot keeps retaining WAL on the source for as long as the Migration is suspended, without bound; the operator emits a `SlotRetained` warning event to that effect. Watch source disk, and do not park live migrations for days. Deleting the Migration drops the slot.
- Each resume consumes one attempt from the `backoffLimit` budget.

## Retries and resume

`spec.backoffLimit` is the number of retries, so `backoffLimit + 1` attempts total (default 4). Each attempt is a fresh Job with Kubernetes-level retries disabled: the operator owns the retry policy, so attempt counts and failure reasons live on the Migration, not buried in Job internals.

- Attempt 1 runs with `--restart`: any state on the work volume is foreign (a fresh Migration never has prior state of its own) and is wiped.
- Retries run with `--resume --not-consistent`: tables and indexes already recorded done in pgcopydb's catalogs are skipped, interrupted copies restart. `--not-consistent` is required because the failed attempt's snapshot died with its process; the retry copies remaining tables under a new snapshot.
- A worker Job that disappears (TTL via `ttlSecondsAfterFinished`, manual deletion) triggers the next attempt the same way.
- Failures the retry cannot fix (a source that dies mid-copy, a broken restore) still burn the budget one attempt at a time, with two exceptions: what the [preflight](live-migration.md#preflight) catches before the first attempt (connectivity, credentials, and target clone privileges on every migration, the follow prerequisites on live ones), and permission errors, which end the Migration as `PermissionDenied` on the attempt whose log tail shows one as the terminal cause (best-effort: a missed match keeps normal retries). The `Failed` condition names the pgcopydb error, so check it after the first failed attempt instead of waiting for the budget to drain; classifying further deterministic failure classes to stop early stays an open task.

Terminal states are absorbing: a `Completed` or `Failed` Migration never restarts (source and target are immutable anyway). Fix the cause and create a new Migration.

## Deletion

- **Clone-only Migration**: deleting the CR garbage-collects everything it owns (Jobs, work PVC, filters ConfigMap). Nothing on either database needs cleaning; there is no finalizer in the way.
- **Live Migration that ever started**: a finalizer routes deletion through cleanup, because a leaked replication slot retains WAL on the source forever. The operator first stops the worker, then runs the cleanup Job (drops slot, auto-created publication, target origin), then releases the CR to garbage collection. Deletion therefore takes a few reconcile cycles, not milliseconds.
- If cleanup exhausts its retries (for example, the source is gone), the operator emits a `CleanupFailed` warning naming the slot and releases the Migration anyway: an unreachable source should not block deletion forever. If the source still exists, check `pg_replication_slots` there and `SELECT pg_drop_replication_slot('<slot>')` manually.
- **Deleting the whole namespace** is supported: a terminating namespace refuses new Jobs, so cleanup cannot run there. Rather than deadlock namespace deletion against the finalizer, the operator emits the same `CleanupFailed` warning (naming the slot) and releases the Migration. A source inside the namespace is deleted along with it, so nothing leaks; a source outside it (another namespace, or outside the cluster) keeps its slot and retains WAL. For such sources, delete the Migration first and let its cleanup Job finish before removing the namespace.
