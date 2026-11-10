#!/bin/sh
# helm lint renders only defaults, so digest pins need explicit render checks.
set -eu

chart=charts/pgcopydb-operator
fail=0
manager_digest=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
runner_digest=sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789

render() { helm template rel "$chart" --namespace ns --show-only templates/deployment.yaml "$@"; }
images() {
  printf '%s\n' "$out" | grep -Fqx "          image: \"$1\"" || {
    echo "wrong manager image for: $label" >&2; fail=1;
  }
  printf '%s\n' "$out" | grep -Fqx "            - --runner-image=$2" || {
    echo "wrong runner image for: $label" >&2; fail=1;
  }
}

app_version=$(sed -n 's/^appVersion: "\(.*\)"$/\1/p' "$chart/Chart.yaml")
[ -n "$app_version" ] || { echo "missing chart appVersion" >&2; exit 1; }
manager=ghcr.io/ydixken/pgcopydb-operator
runner=ghcr.io/ydixken/pgcopydb-operator/runner

label=defaults; out=$(render)
images "$manager:$app_version" "$runner:$app_version"
default_out=$out

label='custom repositories and tags'
out=$(render --set-string image.repository=registry.example/manager --set-string image.tag=manager-custom \
  --set-string runner.image.repository=registry.example/runner --set-string runner.image.tag=runner-custom)
images registry.example/manager:manager-custom registry.example/runner:runner-custom

label='digest overrides tag'
out=$(render --set-string image.repository=registry.example/manager --set-string image.tag=ignored \
  --set-string runner.image.repository=registry.example/runner --set-string runner.image.tag=ignored \
  --set-string image.digest="$manager_digest" --set-string runner.image.digest="$runner_digest")
images "registry.example/manager@$manager_digest" "registry.example/runner@$runner_digest"

label='manager digest only'; out=$(render --set-string image.digest="$manager_digest")
images "$manager@$manager_digest" "$runner:$app_version"

label='runner digest only'; out=$(render --set-string runner.image.digest="$runner_digest")
images "$manager:$app_version" "$runner@$runner_digest"

for key in image.digest runner.image.digest; do
  for invalid in sha256:short "sha256:$(printf '%064d' 0)extra" "sha256:$(printf '%064d' 0 | tr 0 A)" \
    "sha512:${manager_digest#sha256:}" "@$manager_digest" " $manager_digest"; do
    label="$key rejects invalid digest"
    if out=$(render --set-string "$key=$invalid" 2>&1); then
      echo "rendered invalid $key" >&2; fail=1
    elif ! printf '%s\n' "$out" | grep -Fq "$key must be empty or sha256:<64 lowercase hex characters>"; then
      echo "wrong validation error for: $label" >&2; fail=1
    fi
  done
  if render --set "$key=false" >/dev/null 2>&1; then
    echo "rendered non-string $key" >&2; fail=1
  fi
done

# Reused values from older releases omit both digest keys.
old=$(mktemp -d); trap 'rm -rf "$old"' EXIT
cp -R "$chart" "$old/"
sed '/^[[:space:]]*digest:/d' "$chart/values.yaml" >"$old/pgcopydb-operator/values.yaml"
if grep -Eq '^[[:space:]]*digest:' "$old/pgcopydb-operator/values.yaml"; then
  echo "old values still contain a digest key" >&2; exit 1
fi
label='values without digest keys'; out=$(chart="$old/pgcopydb-operator" render)
[ "$out" = "$default_out" ] || { echo "default render changed for: $label" >&2; fail=1; }

[ "$fail" -eq 0 ] || exit "$fail"
echo "chart images render as expected"
