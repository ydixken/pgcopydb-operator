#!/bin/sh
# Renders the chart in the value combinations that decide what the monitoring
# surface ships: the PrometheusRule and ServiceMonitor gating and their knobs.
# helm lint only ever renders the defaults, so a template gated on the wrong
# value would otherwise ship green and nobody would get alerts or scrapes.
# Also asserts every shipped alert has a promtool unit-test case.
set -eu

chart=charts/pgcopydb-operator
rules=$chart/rules/migrations.yaml
tests=test/alerts/migrations_test.yaml
tpl=templates/prometheusrule.yaml
fail=0
out=""

# Sets $out to the rendered template. Helm exits nonzero with "could not find
# template" when the template renders empty, which is exactly the gated-off
# case; any other failure is a real error and stops the script.
render() {
  if out=$(helm template rel "$chart" --namespace ns --show-only "$tpl" "$@" 2>&1); then
    return 0
  fi
  case $out in
  *"could not find template"*)
    out=""
    return 0
    ;;
  *)
    echo "helm template failed for '${*:-defaults}':" >&2
    printf '%s\n' "$out" >&2
    exit 1
    ;;
  esac
}

expect_absent() {
  render "$@"
  if [ -n "$out" ]; then
    echo "FAIL: $tpl rendered with '$*' but must not" >&2
    fail=1
    return 0
  fi
  echo "ok: nothing from $tpl with '$*'"
}

expect_match() {
  needle=$1
  shift
  render "$@"
  if printf '%s\n' "$out" | grep -q -e "$needle"; then
    echo "ok: $tpl has '$needle' with '${*:-defaults}'"
    return 0
  fi
  echo "FAIL: $tpl lacks '$needle' with '${*:-defaults}':" >&2
  printf '%s\n' "$out" >&2
  fail=1
}

expect_no_match() {
  needle=$1
  shift
  render "$@"
  if printf '%s\n' "$out" | grep -q -e "$needle"; then
    echo "FAIL: $tpl has '$needle' with '${*:-defaults}' but must not" >&2
    printf '%s\n' "$out" >&2
    fail=1
    return 0
  fi
  echo "ok: $tpl lacks '$needle' with '${*:-defaults}'"
}

alerts=$(sed -n 's/^ *- alert: //p' "$rules")
if [ -z "$alerts" ]; then
  echo "FAIL: no alerts found in $rules" >&2
  exit 1
fi

# The PrometheusRule is opt-in and needs the Prometheus Operator CRDs
# (--api-versions stands in for them) and a metrics endpoint to alert on.
expect_absent --api-versions monitoring.coreos.com/v1
expect_absent --set metrics.prometheusRule.enabled=true
expect_absent --set metrics.prometheusRule.enabled=true \
  --set metrics.enabled=false --api-versions monitoring.coreos.com/v1

# Enabled: the rule renders with the selection label and every alert.
set -- --set metrics.prometheusRule.enabled=true \
  --set metrics.prometheusRule.additionalLabels.release=kps \
  --api-versions monitoring.coreos.com/v1
expect_match '^kind: PrometheusRule$' "$@"
expect_match '^    release: kps$' "$@"
for a in $alerts; do
  expect_match "- alert: $a\$" "$@"
done

# The ServiceMonitor stays minimal by default and renders each knob when set.
tpl=templates/servicemonitor.yaml
set -- --set metrics.serviceMonitor.enabled=true --api-versions monitoring.coreos.com/v1
expect_match '^kind: ServiceMonitor$' "$@"
# Without honor_labels the Migration's namespace label would be renamed
# exported_namespace and the alerts and dashboards would group wrongly.
expect_match '^      honorLabels: true$' "$@"
# The scrape ships matched to the operator's poll, not left to the Prometheus
# default: a slower scrape drops gauge samples that already exist. Timeout
# stays unset, since Prometheus clamps an inherited global down to the interval.
expect_match '^      interval: 10s$' "$@"
expect_no_match '^      scrapeTimeout:' "$@"
expect_no_match '^      relabelings:' "$@"
expect_no_match '^      metricRelabelings:' "$@"
set -- "$@" \
  --set metrics.serviceMonitor.additionalLabels.release=kps \
  --set metrics.serviceMonitor.interval=15s \
  --set metrics.serviceMonitor.scrapeTimeout=10s \
  --set 'metrics.serviceMonitor.relabelings[0].action=labeldrop' \
  --set 'metrics.serviceMonitor.relabelings[0].regex=instance' \
  --set 'metrics.serviceMonitor.metricRelabelings[0].action=labeldrop' \
  --set 'metrics.serviceMonitor.metricRelabelings[0].regex=pod'
expect_match '^    release: kps$' "$@"
expect_match '^      interval: 15s$' "$@"
expect_match '^      scrapeTimeout: 10s$' "$@"
expect_match '^      relabelings:$' "$@"
expect_match '^      metricRelabelings:$' "$@"
expect_match '^        - action: labeldrop$' "$@"

# The dashboards are opt-in ConfigMaps for Grafana's sidecar: off by default,
# each labeled for pickup, namespace and folder overridable, and one sentinel
# panel per file proves the right JSON landed in the right ConfigMap.
tpl=templates/grafana-dashboards.yaml
expect_absent
set -- --set grafana.dashboards.enabled=true
expect_match '^  name: rel-pgcopydb-operator-migration-detail$' "$@"
expect_match '^  name: rel-pgcopydb-operator-fleet-overview$' "$@"
expect_match '^  name: rel-pgcopydb-operator-operator-health$' "$@"
expect_match '^  namespace: ns$' "$@"
expect_match '^    grafana_dashboard: "1"$' "$@"
expect_match '^    grafana_folder: "pgcopydb"$' "$@"
expect_match '"title": "Phase Timeline"' "$@"
expect_match '"title": "Migrations By Phase"' "$@"
expect_match '"title": "Reconcile Rate"' "$@"
render "$@"
cms=$(printf '%s\n' "$out" | grep -c '^kind: ConfigMap$' || true)
labels=$(printf '%s\n' "$out" | grep -c '^    grafana_dashboard: "1"$' || true)
if [ "$cms" -eq 3 ] && [ "$labels" -eq 3 ]; then
  echo "ok: $tpl renders 3 labeled ConfigMaps"
else
  echo "FAIL: $tpl renders $cms ConfigMaps with $labels sidecar labels, want 3/3" >&2
  fail=1
fi
expect_match '^  namespace: monitoring$' "$@" --set grafana.dashboards.namespace=monitoring
expect_no_match 'grafana_folder' "$@" --set grafana.dashboards.folder=

# Every shipped alert keeps its promtool unit test.
for a in $alerts; do
  if grep -q "alertname: $a\$" "$tests"; then
    echo "ok: $tests covers $a"
  else
    echo "FAIL: $tests has no case for $a" >&2
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "chart monitoring check failed" >&2
  exit 1
fi
echo "chart monitoring renders as expected"
