#!/bin/sh
# Renders the manager Deployment in the value combinations that decide what
# OTel settings it carries. helm lint renders only the defaults, and a default
# that grew env or volumes would change every existing install on upgrade.
set -eu

chart=charts/pgcopydb-operator
fail=0

render() { helm template rel "$chart" --namespace ns --show-only templates/deployment.yaml "$@"; }
has() { printf '%s\n' "$out" | grep -q -- "$1" || { echo "missing '$1' for: $label" >&2; fail=1; }; }
lacks() { if printf '%s\n' "$out" | grep -q -- "$1"; then echo "unexpected '$1' for: $label" >&2; fail=1; fi; }
# Value on the line after the name: traces and logs also carry "none".
after() { printf '%s\n' "$out" | grep -A1 -- "name: $1\$" | grep -q -- "value: \"$2\"" || { echo "'$1' is not '$2' for: $label" >&2; fail=1; }; }

label=defaults; out=$(render)
lacks 'env:'; lacks 'OTEL_'; lacks 'otel-ca'; lacks 'otel-client'

label='enabled without endpoint'
if render --set otel.enabled=true >/dev/null 2>&1; then echo "rendered without otel.endpoint" >&2; fail=1; fi

label='enabled'; out=$(render --set otel.enabled=true --set otel.endpoint=http://c:4318 --set otel.protocol=http/protobuf)
has 'name: OTEL_EXPORTER_OTLP_ENDPOINT'; has 'value: "http://c:4318"'; has 'value: "http/protobuf"'
after OTEL_METRICS_EXPORTER otlp; has 'name: OTEL_TRACES_EXPORTER'; has 'name: OTEL_LOGS_EXPORTER'
has 'name: OTEL_METRIC_EXPORT_INTERVAL'; has 'value: "30000"'; has 'fieldPath: metadata.name'; lacks 'OTEL_EXPORTER_OTLP_HEADERS'

label='metrics off'; out=$(render --set otel.enabled=true --set otel.endpoint=c:4317 --set otel.signals.metrics=false)
after OTEL_METRICS_EXPORTER none

label='headers'; out=$(render --set otel.enabled=true --set otel.endpoint=c:4317 --set otel.headersSecret.name=otlp-auth)
has 'name: OTEL_EXPORTER_OTLP_HEADERS'; has 'name: otlp-auth'; has 'key: headers'

label='tls'; out=$(render --set otel.enabled=true --set otel.endpoint=https://c:4317 \
  --set otel.tls.caSecret.name=otlp-ca --set otel.tls.clientCertSecret.name=otlp-client)
has 'OTEL_EXPORTER_OTLP_CERTIFICATE'; has '/etc/otel/ca/ca.crt'; has 'OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE'
has '/etc/otel/client/tls.crt'; has '/etc/otel/client/tls.key'; has 'secretName: otlp-ca'; has 'secretName: otlp-client'

label='insecure'; out=$(render --set otel.enabled=true --set otel.endpoint=c:4317 --set otel.tls.insecure=true)
has 'name: OTEL_EXPORTER_OTLP_INSECURE'; has 'value: "true"'

label='extraEnv only'; out=$(render --set 'extraEnv[0].name=FOO' --set 'extraEnv[0].value=bar')
has 'name: FOO'; lacks 'OTEL_'

# helm upgrade --reuse-values from a pre-otel release renders with that release's
# values, which have no otel or extraEnv key at all.
label='values without otel or extraEnv'
old=$(mktemp -d); trap 'rm -rf "$old"' EXIT
cp -R "$chart" "$old/"
awk '/^[^ #]/ { skip = ($0 ~ /^(otel|extraEnv):/) } !skip' "$chart/values.yaml" >"$old/pgcopydb-operator/values.yaml"
if grep -Eq '^(otel|extraEnv):' "$old/pgcopydb-operator/values.yaml"; then echo "strip left otel or extraEnv for: $label" >&2; fail=1; fi
if out=$(chart="$old/pgcopydb-operator" render); then
  lacks 'env:'; lacks 'OTEL_'
  [ "$out" = "$(render)" ] || { echo "differs from the default render for: $label" >&2; fail=1; }
else
  echo "render failed for: $label" >&2; fail=1
fi

exit "$fail"
