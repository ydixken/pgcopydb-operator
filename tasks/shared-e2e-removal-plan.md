# Shared E2E removal plan

## Implementation

- [x] Create `integrate/shared-e2e-on-main-1933ce5` from fetched `origin/main` at `1933ce5bb7a2322fddf563ffdbf09b012885cbf1`.
- [ ] Cherry-pick #211 from `972781357628526a220db172f157398e2090599c` before the other implementation work.
- [ ] Remove the disposable workflow path, observer, documentation, and their tests.
  Retain the native shared-cluster workflow path and add CI-run regression assertions for that boundary.
- [ ] Isolate #223 from `48d091453d93222f0d3555157b90513ed5976ab7` without #243 worker-session behavior.
- [ ] Apply only the #210 Pending-status and approved #200 scheduling hunks from the `a62ef17..43f5127` controller diff, with focused tests and documentation.
- [ ] Reconcile the shared controller and E2E registration files after the focused changes land.
- [ ] Confirm that the combined diff excludes #215, #243, #209, #221, #88, and any new coverage-summary behavior.
  Keep #200 open for the historical 35-second symptom.

## Verification and delivery

- [ ] Complete one independent static review of the combined diff.
- [ ] Format touched files, run `git diff --check`, and run `task lint` locally.
  Do not run local functional tests, builds, documentation builds, runtime checks, or cluster diagnostics.
- [ ] Fetch and rebase onto actual `origin/main` before publication.
  Repeat the static and local gates if the exact head changes.
- [ ] Update the existing PR branch with an explicit old-head lease against `a62ef17724108f6d20b796672fa656188deb82f8`.
- [ ] Require naturally triggered base CI, coverage, one full shared-cluster `feature-e2e` result, and successful cleanup on the exact final head before merge.
  Do not allocate a separate disposable or focused substitute.
- [ ] Merge normally, verify the merged commit on actual `main`, and close #211, #223, and #210.
  Leave #200, #215, #243, #209, and #221 open.
