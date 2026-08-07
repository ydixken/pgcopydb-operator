#!/bin/sh
# Regenerates the chart's CRD template from controller-gen output
# (config/crd/bases), injecting the Helm gates the chart needs. With --check
# it only verifies the template is current and exits 1 on drift; CI runs that
# so the chart can never ship a stale CRD again.
set -eu

src=config/crd/bases/pgcopydb-operator.io_migrations.yaml
dst=charts/pgcopydb-operator/templates/crd-migrations.yaml
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

{
  printf '{{- if .Values.crds.install }}\n'
  # controller-gen output starts with "---"; keep it, then inject the keep
  # annotation gate right after the generator annotation line.
  awk '
    { print }
    /controller-gen.kubebuilder.io\/version:/ {
      print "    {{- if .Values.crds.keep }}"
      print "    {{- /* keep survives helm uninstall so Migration CRs are never mass-deleted */}}"
      print "    helm.sh/resource-policy: keep"
      print "    {{- end }}"
    }
  ' "$src"
  printf '{{- end }}\n'
} > "$tmp"

if [ "${1:-}" = "--check" ]; then
  if ! diff -q "$tmp" "$dst" >/dev/null 2>&1; then
    echo "chart CRD template is stale: run hack/sync-chart-crd.sh and commit" >&2
    diff -u "$dst" "$tmp" | head -40 >&2 || true
    exit 1
  fi
  echo "chart CRD template is current"
else
  cp "$tmp" "$dst"
  echo "chart CRD template regenerated from $src"
fi
