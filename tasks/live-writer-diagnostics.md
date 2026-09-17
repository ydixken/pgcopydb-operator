# Live-writer diagnostic verification

Base: `b12c254fc5b03bc0c4de3e2c9dd4d3831bb2e64c`.
Scope: test-harness observability only; the live EPIPE cause remains unconfirmed.
Keep production code, published artifacts, tags, E2E workflow settings, permissions, and fork references unchanged.

Implementation status, source-forensics evidence, observed lint results, and pending delivery gates are maintained in the continuous-writer follow-up section of [tasks/todo.md](todo.md).
This document defines the verification procedure rather than a second status checklist.

## CI gates

1. Run the existing `ci.yml` test job on the proposed head.
   Its helper step selects every `TestCutoverDiagnostic*`, `TestPublicationRetry*`, and `TestLiveWriter*` test with `-count=1 -race -v`.
   Require the selector contract to enforce both `-race` and `-v` as exact flags.
   Only the parent invocation of the subprocess entry point may skip in the live-writer family; every meaningful regression must execute.
   Verbose output makes that distinction visible.
   Require concurrent final-query snapshots to publish classification and marker validity together, and synthetic DNS/transport cases to suppress bare hostnames across chunks and at the capture boundary.
2. In a disposable CI checkout, remove only `cmd.Stderr = &stderr` from the persistent writer and rerun the live-writer regressions.
   Require a behavioral failure for missing early stderr or flood capture, not a compilation failure from removing the diagnostic API.
   Restore capture and require the same regressions to pass before recording paired evidence.
3. Require exact-head lint, functional tests, selector-contract tests, and documentation checks before delivery.
   A draft or normal PR runs the exact race-enabled step before merge or use of public runtime diagnostics.
   Unknown runtime and timing cost under race instrumentation remains a CI gate, not a new local-test requirement.
4. In a separately authorized phase, run focused E2E and then the full native suite, retaining the existing load, gap, exact-data, single-attempt, and verification assertions.
   A focused pass does not replace the full-suite gate.

Formatting and `task lint` are the only local verification gates; Go tests and builds run in CI.
Publication authority and the separate merge, cluster, and release restrictions are recorded in [tasks/todo.md](todo.md).
The historical failure discarded child stderr, so it cannot supply before/after proof for this new diagnostic API.
No baseline, mutation, fixed-test, or root-cause claim is valid until its corresponding result has been observed.
