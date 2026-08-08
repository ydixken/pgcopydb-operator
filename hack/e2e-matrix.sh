#!/bin/sh
# Sequential PG version-matrix e2e runs; invoked by `task e2e:matrix`, which
# holds the single confirmation prompt. Runs against the CURRENT kubectl
# context, so never call this directly against a cluster you did not intend.
#
# The combos are upgrade-direction only: pgcopydb needs pg_dump at least at
# the target's major, and a newer major's dump does not restore into an older
# server. PG14 appears as a source only: the follow-mode target contract
# includes GRANT SET ON PARAMETER session_replication_role, which PostgreSQL
# grew in 15 (docs/reference/prerequisites.md).
#
# Every combo runs even when an earlier one fails; the summary prints at the
# end and any failure exits nonzero.
set -u

combos="14:18 18:18 15:17"
last="15:17"
summary=""
failed=0
force=false

for combo in $combos; do
  src=${combo%%:*}
  tgt=${combo##*:}
  # Keep the fixture namespaces between combos (a kept cluster on the wrong
  # major is recreated by the suite); the last combo cleans up after itself.
  keep=true
  [ "$combo" = "$last" ] && keep=false
  echo ""
  echo "=== e2e matrix: PG ${src} -> PG ${tgt} (E2E_SCALE=0.1, chaos excluded) ==="
  echo ""
  if E2E_PG_SOURCE="$src" E2E_PG_TARGET="$tgt" E2E_SCALE=0.1 \
    E2E_KEEP_FIXTURES="$keep" E2E_FORCE="$force" \
    go test ./test/e2e/... -v -timeout 40m -ginkgo.v -ginkgo.label-filter='!chaos'; then
    summary="${summary}PG ${src} -> PG ${tgt}  PASS\n"
  else
    summary="${summary}PG ${src} -> PG ${tgt}  FAIL\n"
    failed=1
  fi
  # From the second combo on, take over the helm release: a combo killed by
  # the go test timeout skips AfterSuite and leaves the release behind, and
  # this loop is its sole owner once the first combo passed the tenancy check.
  force=true
done

echo ""
echo "=== e2e matrix summary ==="
printf '%b' "$summary"
exit "$failed"
