# Issue #172: persistent e2e live writer

## Plan

- [x] Research the existing e2e writer, psql helper, and repository constraints.
- [x] Discuss the design options and select the specialized persistent psql writer.
- [x] Record approval for the selected design.
- [x] Write the design specification.
- [x] Self-review the design artifacts for scope, ambiguity, contradictions, and placeholders.
- [x] Draft the implementation plan.
- [x] Review the initial implementation plan and record required corrections.
- [x] Research the local live-writer test seam and confirm the named standard-test command.
- [x] Revise the implementation plan for lifecycle cleanup and local subprocess tests.
- [x] Review the broad revised plan and record why it still failed review.
- [x] Decompose the correction into focused test-protocol and lifecycle studies.
- [x] Integrate both studies into a detailed implementation-plan draft.
- [x] Review commit `dc96010` and record its remaining lifecycle-proof and stale-comment gaps.
- [x] Correct the command-state proof, lifecycle assertions, and helper-comment coverage.
- [x] Obtain an independent re-review of the corrected plan before implementation starts.
- [x] Implement the persistent writer, update the adjacent source-target count assertion comment, and verify the marker-query and error paths.
- [x] Obtain an independent review of the implementation.
- [x] Run `task lint` and `task test` for the implementation.
- [x] Run guarded `task e2e` against the current real cluster context after human confirmation.
- [x] Push the implementation, open a pull request, and verify its required checks.

## Review and results

- Independent design review: corrected the e2e scenario scope, marker query, persistent-session failure boundary, and implementation coverage.
- Initial implementation plan review: required Start-failure cleanup, an error-aware final-query seam, local subprocess coverage, full scenario-range guidance, and fact-ordered tracker delivery.
- Testability research: confirmed `go test ./test/e2e -run '^TestLiveWriter' -count=1` skips `TestE2E` and `BeforeSuite`.
- Broad revised plan review: failed because it re-resolved the primary for the final query, expected two primary calls, omitted complete helper and lifecycle test bodies, and stopped after the tracker push without checking that SHA.
- Focused planning studies: specified the deterministic subprocess test protocol and the single-resolution close, Wait, and final-query lifecycle.
- Commit `dc96010` review: required proof that the persistent child was not started on `StdinPipe` failure, proof that Wait completed before every final query, and three omitted stale-comment replacements.
- Review corrections: complete with locked command snapshots, process-state assertions, and exact bounded comment edits.
- Independent implementation-plan re-review after commit `22f446b`: APPROVED.
- Implementation: commit `ac8cad0` implemented the persistent writer.
- Focused test, e2e vet, `task lint`, and `task test`: passed.
- Independent implementation review: APPROVED.
- Task 3: commit `70b835b` completed the persistent writer, local tests, source-max scenario assertion, and comment updates.
- Task 3 focused test, e2e vet, `task lint`, and `task test`: passed.
- Independent Task 3 review: APPROVED.
- Task 4 focused test, e2e vet, `task lint`, and `task test`: passed at `21da875f5d1d1206957622295b71e9d0a0532497`.
- Task 4 guarded e2e: human-confirmed `task e2e` failed in `BeforeSuite` because the suite operator Deployment did not become ready, so 0 scenarios ran.
- Task 4 guarded e2e cleanup: `AfterSuite` completed after the `BeforeSuite` failure.
- Implementation commit: `21da875f5d1d1206957622295b71e9d0a0532497`.
- Pull request: https://github.com/ydixken/pgcopydb-operator/pull/174.
- First required-check watch for `21da875f5d1d1206957622295b71e9d0a0532497`: docs, lint, and test passed.
