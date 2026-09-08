# Shared E2E removal design

## Approved scope

Remove the temporary Kind E2E environment and its observer.
Keep the existing GitHub/ARC runners and the established shared-cluster E2E route unchanged.
Replace PR 252 with one branch from actual `main` that contains the removal plus #211, #223, #210, and only the approved #200 scheduling compensation.
Keep #200 open because this change does not explain or fix the historical 35-second symptom.
Exclude #88.

## Deferred work and preserved refs

- Preserve the complete deferred #215 and #243 source at PR 252 head `a62ef17724108f6d20b796672fa656188deb82f8`.
- Keep `fix/215-persist-cleanup-outcome` (`72c4e7f1577abb25cfb83d3b94346e643899809f`) and protected stash `c459eaaa45434f117cdb204f160a1c145b40dbc6` as additional #215 backups.
- Defer #243 from the preserved PR 252 source, including `48d091453d93222f0d3555157b90513ed5976ab7` and `f03c4e8ff5a0190b4c753e4d7634b7795cdf5f8e`.
  Extract only #223 from the shared source commit.
- Defer #209 and #221 from `integrate/config-lifecycle-on-pr2-a62ef17` (`43f51270455370224cea493cd51d7b3908fea56d`), including their shared source commit `faabf28a040f7365f5d7731c8fe876236523ab25`.
  Apply only the reviewed #210 and #200 controller hunks from the integration diff.
- Do not alter, delete, or rewrite the preserved source refs.
  Update the published PR 252 branch only through the approved explicit old-head lease.

## Delivery boundary

The replacement branch starts from fetched actual `main` at `1933ce5bb7a2322fddf563ffdbf09b012885cbf1`.
CI runs the functional tests, builds, and documentation validation.
Local work is limited to formatting, `git diff --check`, and `task lint`.
Exact-head base CI, coverage, one full shared-cluster E2E result, and successful cleanup gate a normal merge.
Verify the merged commit on actual `main`, close #211, #223, and #210, and leave #200, #215, #243, #209, and #221 open.
