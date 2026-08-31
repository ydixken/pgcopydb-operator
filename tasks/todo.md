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
- [x] Integrate both studies into a copy-ready corrected implementation plan.
- [ ] Obtain an independent re-review of the corrected plan before implementation starts.
- [ ] Implement the persistent writer, update the adjacent source-target count assertion comment, and verify the marker-query and error paths.
- [ ] Obtain an independent review of the implementation.
- [ ] Run `task lint` and `task test` for the implementation.
- [ ] Decide whether to run guarded `task e2e` against the current real cluster context.
- [ ] Commit, push, open a pull request, and verify CI.

## Review and results

- Independent design review: corrected the e2e scenario scope, marker query, persistent-session failure boundary, and implementation coverage.
- Initial implementation plan review: required Start-failure cleanup, an error-aware final-query seam, local subprocess coverage, full scenario-range guidance, and fact-ordered tracker delivery.
- Testability research: confirmed `go test ./test/e2e -run '^TestLiveWriter' -count=1` skips `TestE2E` and `BeforeSuite`.
- Broad revised plan review: failed because it re-resolved the primary for the final query, expected two primary calls, omitted complete helper and lifecycle test bodies, and stopped after the tracker push without checking that SHA.
- Focused planning studies: specified the deterministic subprocess test protocol and the single-resolution close, Wait, and final-query lifecycle.
- Corrected plan integration: complete with copy-ready tests and implementation, one captured pod, and two required-check watches.
  Independent re-review is pending.
- Implementation: pending until the corrected plan passes independent re-review.
- Implementation review: pending.
- Verification results: pending implementation.
- Guarded e2e decision: pending implementation and human confirmation.
- Commit, pull request, and CI: pending implementation and verification.
