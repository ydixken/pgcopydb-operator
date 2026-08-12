#!/bin/sh
# Renders the chart in the value combinations that decide whether the
# metrics-auth RBAC exists and whom it binds, then asserts the result.
# helm lint only ever renders the defaults, so a template gated on the wrong
# value would otherwise ship green: the operator loses the permission it needs
# to authenticate a metrics scraper, and every scrape is rejected.
set -eu

chart=charts/pgcopydb-operator
tpl=templates/metrics-auth-rbac.yaml
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
    echo "FAIL: metrics-auth RBAC rendered with '$*' but must not" >&2
    fail=1
    return 0
  fi
  echo "ok: no metrics-auth RBAC with '$*'"
}

expect_match() {
  needle=$1
  shift
  render "$@"
  if printf '%s\n' "$out" | grep -q "$needle"; then
    echo "ok: '$needle' with '${*:-defaults}'"
    return 0
  fi
  echo "FAIL: '$needle' missing from the metrics-auth RBAC with '${*:-defaults}':" >&2
  printf '%s\n' "$out" >&2
  fail=1
}

# Defaults (rbac.create=true, metrics.enabled=true): both objects exist, and
# the binding names the ServiceAccount the Deployment actually runs as.
expect_match '^kind: ClusterRole$'
expect_match '^kind: ClusterRoleBinding$'
expect_match '^  name: rel-pgcopydb-operator-metrics-auth$'
expect_match '^    name: rel-pgcopydb-operator$'
expect_match '^    namespace: ns$'

# No metrics endpoint means no authn/authz filter, so the permission is dead.
expect_absent --set metrics.enabled=false

# rbac.create=false means the user brings their own RBAC; the e2e install does.
expect_absent --set rbac.create=false

# A ServiceAccount the chart does not create must still be the bound subject.
expect_match '^    name: custom$' --set serviceAccount.create=false --set serviceAccount.name=custom

if [ "$fail" -ne 0 ]; then
  echo "chart RBAC check failed" >&2
  exit 1
fi
echo "chart RBAC renders as expected"
