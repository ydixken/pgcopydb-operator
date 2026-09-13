# Runtime safety delivery status

## Current state

PR 252 merged the shared E2E removal with #211, #223, #210, and the approved #200 scheduling compensation.
The feature-branch E2E gate is removed; the merge gate on `main` is `lint`, `test`, and `docs`.
Release candidate E2E in `release.yml` is the only cluster coverage.
Keep #200 open because the historical 35-second delay still has no measured cause.

## Unblocked work

- #220 (PR #257), #209, and #221 no longer wait on an identical installed CRD, because no pre-merge gate compares one.
  Their first cluster run is the release candidate that carries them.

## Deferred work

- Defer all #215 cleanup-outcome behavior and #243 worker-session behavior until their isolation-dependent acceptance can run.
- Keep #88 out of scope, including the pgcopydb fork, SQLite contention work, builder publication, and runner digest changes.

## Preserved sources

- The complete deferred #215 and #243 source remains at `a62ef17724108f6d20b796672fa656188deb82f8`.
- The earlier #215 branch remains at `72c4e7f1577abb25cfb83d3b94346e643899809f`, with protected stash `c459eaaa45434f117cdb204f160a1c145b40dbc6` untouched.
- The #209 and #221 integration source remains at `43f51270455370224cea493cd51d7b3908fea56d`.
- The coherent #211 source is `972781357628526a220db172f157398e2090599c`.
- The mixed #223 source is `48d091453d93222f0d3555157b90513ed5976ab7`; only its extension-preflight behavior belongs in the replacement PR.

## Delivery gates

- Write focused regressions for every retained behavior and run them in CI.
- Format locally, run `git diff --check`, and run `task lint` locally.
- Require exact-head `lint`, `test`, and `docs` before a normal merge.
- Verify the merged commit on actual `main`.
