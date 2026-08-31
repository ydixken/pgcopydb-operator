# Persistent e2e live writer throughput

## Scope

Issue #172 changes the e2e `liveWriter` in `test/e2e/e2e_suite_test.go` and the adjacent source-target count assertion comment in `test/e2e/e2e_test.go`.
The writer will keep one `kubectl exec -i` session open to the source primary's `psql` process.
It will continue to write ordered rows to the existing `orders` fixture table through the base copy and streaming phases.
No production API, controller, chart, or user-facing documentation behavior changes.

## Current cause

The current writer starts a separate `kubectl exec` for every tick and resolves the primary for each call.
Process setup and repeated primary discovery cap the writer's throughput and add failure paths unrelated to the ordered inserts.

## Architecture and data flow

`startLiveWriter` resolves the source primary once and starts `kubectl exec -i` in that pod's PostgreSQL container.
The child command is `psql -U postgres app -q -v ON_ERROR_STOP=1`.
The writer sends one ordered `INSERT` to the child's standard input for each ticker event.
With psql's default autocommit, each statement is one committed transaction.
The stream remains specialized to `liveWriter`; no reusable streaming helper is introduced.

## Concurrency and lifecycle

The writer goroutine continues to defer `GinkgoRecover()`.
`sync.Once` remains the guard that lets cleanup call `stop` after the spec already stopped the writer.
`stop` closes the psql standard input, waits for the child process, and then runs a separate one-shot source query for the authoritative highest committed marker.
The query reads `orders.note`, filters rows with the writer's exact `marker-` prefix, casts the suffix after that prefix to an integer, and returns its maximum.
For the scenario marker `live-load`, it uses `SELECT COALESCE(MAX((substring(note FROM char_length('live-load') + 2))::int), 0) FROM orders WHERE note LIKE 'live-load-%'`.
Failure to run that query or parse its scalar result fails the spec.
That source value, rather than an in-memory counter, defines the rows the test must find on the target.
The writer remains stopped before cutover approval, so the source is quiescent from the freeze onward.

## Error handling

Failure to resolve the initial primary, start the command, write to standard input, close the stream, wait for psql, query the final marker, or parse the marker result fails the spec.
The writer records the first error for `stop` to report to the spec goroutine.
It does not replace the stream or re-resolve the primary after a persistent-session failure.
The final marker query is a separate required post-shutdown operation, not a reconnect or continuation of the failed stream.
Retrying the persistent stream can obscure whether an ordered insert committed, so a failed stream is a test failure.

## Tests and verification

The existing scenario must still require a highest committed marker greater than 100.
It must compare the source and target marker counts and retain the target gap query.
It must retain the full data verification and the replication cleanup assertions already in the scenario.
Implementation must run `task lint` and `task test`.
`task e2e` remains a guarded decision because it targets the current real Kubernetes context and requires human confirmation.

## Documentation impact

Refresh only code comments made stale by the persistent psql lifecycle.
No public documentation, API, controller, chart, or configuration change is needed.

## Rejected alternatives

A generic streaming helper is unnecessary because `liveWriter` is its only caller.
Batching inserts is rejected because the scenario requires one transaction per row.

## Acceptance criteria

- `liveWriter` starts one long-lived interactive psql process against the source primary resolved at startup.
- Each ticker event writes one ordered autocommit `INSERT` to that process.
- Start, stream, write, close, and wait failures fail the test without replacing the stream or re-resolving the primary.
- `stop` closes standard input, waits for psql, and uses a separate source query to return the maximum numeric suffix for rows matching the writer's exact marker.
- A final marker query or parse failure fails the test.
- The scenario still enforces `last > 100`, source-target count equality, gap-free target rows, data verification, and cleanup.
- The implementation changes no production behavior.
