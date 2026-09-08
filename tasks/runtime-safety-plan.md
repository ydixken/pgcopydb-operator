# Runtime safety delivery status

## Current batch

The active implementation plan is [Shared E2E removal](shared-e2e-removal-plan.md).
One replacement PR 252 removes the temporary Kind environment and observer while retaining the established shared-cluster route.
The PR includes #211, #223, #210, and only the approved #200 scheduling compensation.
Keep #200 open because the historical 35-second delay still has no measured cause.

## Deferred work

- Defer all #215 cleanup-outcome behavior and #243 worker-session behavior until their isolation-dependent acceptance can run.
- Defer #209 split-table configuration and #221 major-version configuration because the shared route requires an identical installed CRD.
- Keep #88 out of scope, including the pgcopydb fork, SQLite contention work, builder publication, and runner digest changes.

The retired disposable route did not prove its prerequisite on the actual runner path.
This plan does not claim a diagnosed cause for that failure.

## Preserved sources

- The complete deferred #215 and #243 source remains at `a62ef17724108f6d20b796672fa656188deb82f8`.
- The earlier #215 branch remains at `72c4e7f1577abb25cfb83d3b94346e643899809f`, with protected stash `c459eaaa45434f117cdb204f160a1c145b40dbc6` untouched.
- The #209 and #221 integration source remains at `43f51270455370224cea493cd51d7b3908fea56d`.
- The coherent #211 source is `972781357628526a220db172f157398e2090599c`.
- The mixed #223 source is `48d091453d93222f0d3555157b90513ed5976ab7`; only its extension-preflight behavior belongs in the replacement PR.

## Delivery gates

- Write focused regressions for every retained behavior and run them in CI.
- The retained runtime diff has an independent static review.
- Complete an independent static review of the workflow, helper, and build-configuration removal diff.
- Format locally, run `git diff --check`, and run `task lint` locally.
- Fetch and rebase onto actual `origin/main` before publication.
- Update the existing PR branch only with the approved explicit old-head lease.
- Require exact-head base CI, coverage, one full shared-cluster E2E result, and successful cleanup before a normal merge.
- Verify the merged commit on actual `main`, close #211, #223, and #210, and leave #200, #215, #243, #209, and #221 open.
