#!/usr/bin/env bash
set -euo pipefail
suite_report=$FEATURE_E2E_HELPERS/suite-report.json
rm -f "$FEATURE_E2E_HELPERS/manager-attested" "$suite_report"
export PATH="$FEATURE_E2E_HELPERS/bin:$PATH"
export E2E_RUN_LABEL_VALUE
E2E_RUN_LABEL_VALUE=$(<"$FEATURE_E2E_OWNER_FILE")
[[ "$E2E_RUN_LABEL_VALUE" =~ ^[0-9a-f]{32}$ ]]
test_args=(
  ./test/e2e/...
  -run
  '^TestE2E$'
  -v
  -timeout 4h
  -ginkgo.v
  -ginkgo.timeout=4h
  -ginkgo.poll-progress-after=15m
  "-ginkgo.label-filter=!chaos && !flaky"
  -ginkgo.fail-on-empty
  "-ginkgo.json-report=$suite_report"
)
if [ "$RUN_MODE" = focus ]; then
  test_args+=("-ginkgo.focus=$E2E_FOCUS")
fi
set +e
go test "${test_args[@]}"
suite_result=$?
set -e
printf 'suite_ran=true\n' >> "$GITHUB_OUTPUT"
[ -f "$FEATURE_E2E_HELPERS/manager-attested" ] || {
  echo "::error::manager attestation did not complete"
  exit 1
}
printf 'manager_attested=true\n' >> "$GITHUB_OUTPUT"
jq -e --argjson result "$suite_result" '
  (type == "array" and length == 1) and
  (.[0] as $report |
    ($report.SuiteDescription == "pgcopydb-operator e2e suite") and
    ($report.PreRunStats.TotalSpecs |
      type == "number" and . > 0 and floor == .) and
    ($report.PreRunStats.SpecsThatWillRun |
      type == "number" and . > 0 and floor == .) and
    ($report.PreRunStats.SpecsThatWillRun <= $report.PreRunStats.TotalSpecs) and
    ($report | has("SpecialSuiteFailureReasons")) and
    ($report.SpecialSuiteFailureReasons == null or
      ($report.SpecialSuiteFailureReasons | type == "array" and length == 0)) and
    ($report.SpecReports | type == "array") and
    (([$report.SpecReports[] | select(.LeafNodeType == "It")] | length) ==
      $report.PreRunStats.TotalSpecs) and
    all($report.SpecReports[];
      (.State == "passed" or .State == "pending" or
        .State == "skipped" or .State == "failed") and
      (if has("AdditionalFailures") then
        (.AdditionalFailures |
          if type == "array" then length == 0 else false end)
      else true end) and
      (if has("Failure") then
        (.Failure |
          if type == "object" then
            (has("AdditionalFailure") | not)
          else false end)
      else true end)) and
    if $result == 0 then
      $report.SuiteSucceeded == true and
      all($report.SpecReports[]; .State != "failed")
    elif $result == 1 then
      $report.SuiteSucceeded == false and
      ([$report.SpecReports[] | select(.State == "failed")] | length) > 0 and
      all($report.SpecReports[];
        if .State == "failed" then
          .LeafNodeType == "It" and
          (.Failure |
            type == "object" and
            (.Message | type == "string" and length > 0) and
            (.Location |
              type == "object" and
              (.FileName | type == "string" and length > 0) and
              (.LineNumber |
                type == "number" and . > 0 and floor == .)) and
            .FailureNodeContext == "leaf-node" and
            .FailureNodeType == "It")
        else true end)
    else false end)
' "$suite_report" >/dev/null || {
  echo "::error::suite completion report is missing, malformed, or unsafe"
  exit 1
}
printf 'suite_completed=true\n' >> "$GITHUB_OUTPUT"
exit "$suite_result"
