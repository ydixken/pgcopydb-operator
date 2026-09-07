#!/usr/bin/env bash
set -euo pipefail
schema_profile=${FEATURE_E2E_SCHEMA_VALIDATION:-identical}
suite_profile=${SUITE_PROFILE:-baseline}
case "$schema_profile" in
  identical) ;;
  additive-disposable) unset E2E_EXTRA_TABLES E2E_EXTRA_SIZE_GB E2E_EXTRA_JOBS ;;
  *) echo "::error::schema validation profile is invalid"; exit 1 ;;
esac
case "$suite_profile" in
  baseline) label_filter='!chaos && !flaky && !isolated-runtime-safety' ;;
  runtime-safety)
    [ "$schema_profile" = additive-disposable ] && [ "$RUN_MODE" = full ] &&
      [ "${E2E_SCALE:-}" = 0.1 ] && [ -z "${E2E_FOCUS:-}" ] || {
        echo "::error::runtime-safety suite profile is invalid"
        exit 1
      }
    label_filter='(!chaos && !flaky && !isolated-runtime-safety) || (isolated-runtime-safety && !flaky)' ;;
  *) echo "::error::suite profile is invalid"; exit 1 ;;
esac
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
  "-ginkgo.label-filter=$label_filter"
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
if [ "$suite_result" -eq 0 ] && [ "$schema_profile" = additive-disposable ] && [ "$RUN_MODE" = full ]; then
  jq -e '
    [.[0].SpecReports[] | select(.LeafNodeType == "It") |
      . as $spec |
      ((.ContainerHierarchyTexts // []) | index("Migration metrics") != null) as $container |
      (((.ContainerHierarchyLabels // [] | flatten) + (.LeafNodeLabels // [])) |
        index("metrics") != null) as $label |
      select($container or $label) |
      {valid: ($container and $label and $spec.State == "passed")}] |
    length > 0 and all(.[]; .valid)
  ' "$suite_report" >/dev/null 2>&1 || {
    echo "::error::full disposable metrics evidence is missing or incomplete"
    exit 1
  }
  printf 'metrics_completed=true\n' >> "$GITHUB_OUTPUT"
fi
if [ "$suite_result" -eq 0 ] && [ "$suite_profile" = runtime-safety ]; then
  jq -e '
    def proof($name; $leaf):
      [.[0].SpecReports[] as $spec | ($spec.ReportEntries // [])[] |
        select(.Name == $name) | {spec: $spec, entry: .}] |
      if length == 1 and .[0].spec.LeafNodeType == "It" and
         .[0].spec.LeafNodeText == $leaf and .[0].spec.State == "passed"
      then .[0] else error("invalid proof entry") end;
    def positive_integer: type == "number" and . > 0 and floor == .;
    def payload: .entry.Value.AsJSON | fromjson | select(type == "object");
    . as $report |
    ($report | proof("dead-worker-session-expiry";
      "expires abandoned source and target sessions after packet loss and resumes")) as $expiry |
    ($report | proof("cleanup-alert-after-job-ttl";
      "records exhausted cleanup after proven drain and restores retained replication state") | payload) as $cleanup |
    ($expiry | payload) as $worker |
    ($expiry.spec.ContainerHierarchyTexts | index("Worker session bounds") != null) and
    ($worker.migrationUID | type == "string" and length > 0) and
    ($cleanup.migrationUID | type == "string" and length > 0) and
    ($worker.sourceCohortSize | positive_integer) and
    ($worker.targetCohortSize | positive_integer) and
    ($worker.sourceExpiredCount | positive_integer) and
    ($worker.targetExpiredCount | positive_integer) and
    $worker.sourceExpiredCount == $worker.sourceCohortSize and
    $worker.targetExpiredCount == $worker.targetCohortSize and
    ($worker.expirySeconds | type == "number" and . > 0 and . <= 180) and
    all(["targetCopyObserved", "rollbackRegisteredBeforeFault", "faultRuleInstalled", "faultRuleRemoved",
      "controlSessionHealthy", "longCopyOutlivedProbe", "resumeObserved", "dataMatched"][]; $worker[.] == true) and
    $worker.backendTerminationUsed == false and
    all(["jobTTLObserved", "failureMetricObserved", "firingAlertObserved"][]; $cleanup[.] == true)
  ' "$suite_report" >/dev/null 2>&1 || {
    echo "::error::runtime-safety proof is missing or invalid"
    exit 1
  }
  printf 'runtime_safety_completed=true\n' >> "$GITHUB_OUTPUT"
fi
printf 'suite_completed=true\n' >> "$GITHUB_OUTPUT"
exit "$suite_result"
