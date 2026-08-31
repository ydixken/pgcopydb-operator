# Issue #172: persistent e2e live writer

## Plan

- [x] Research the existing e2e writer, psql helper, and repository constraints.
- [x] Discuss the design options and select the specialized persistent psql writer.
- [x] Record approval for the selected design.
- [x] Write the design specification.
- [x] Self-review the design artifacts for scope, ambiguity, contradictions, and placeholders.
- [ ] Draft the implementation plan.
- [ ] Implement the persistent writer, update the adjacent source-target count assertion comment, and verify the marker-query and error paths.
- [ ] Obtain an independent review of the implementation.
- [ ] Run `task lint` and `task test` for the implementation.
- [ ] Decide whether to run guarded `task e2e` against the current real cluster context.
- [ ] Commit, push, open a pull request, and verify CI.

## Review and results

- Independent design review: corrected the e2e scenario scope, marker query, persistent-session failure boundary, and implementation coverage.
- Implementation review:
- Verification results:
- Guarded e2e decision:
- Commit, pull request, and CI:
