#!/usr/bin/env bash
# Runs inside a freshly built runner image, before it is pushed.
#
# A published image missing a tool turns every workflow red at once, and the
# scale sets boot whatever tag they are pointed at without asking. Everything
# here is something a job in this repository actually invokes, so a failure is
# a job that would have failed later and further from the cause.
set -euo pipefail

fail=0
note() {
    echo "  $1"
    fail=1
}

# On PATH, because a workflow step calls these by name.
for tool in go make gcc gh psql helm kubectl promtool oras yamllint mkdocs \
    python3 pip jq git curl docker; do
    command -v "$tool" >/dev/null || note "missing on PATH: $tool"
done

# The Makefile's own tools. These are the point of the image: without them
# every lint job relinks a custom golangci-lint and every test job re-downloads
# controller-gen and the envtest binaries.
: "${LOCALBIN:?LOCALBIN is not set in the image}"
for tool in controller-gen setup-envtest golangci-lint kustomize crd-ref-docs; do
    [ -x "$LOCALBIN/$tool" ] || note "missing in LOCALBIN: $tool"
done
[ -d "$LOCALBIN/k8s" ] && [ -n "$(ls -A "$LOCALBIN/k8s")" ] ||
    note "missing: envtest control-plane binaries under $LOCALBIN/k8s"

# The module cache is baked so a cold node does not download the world. An
# empty one means `go mod download` silently did nothing at build time.
mod=$(go env GOMODCACHE)
[ -d "$mod" ] && [ -n "$(ls -A "$mod" 2>/dev/null)" ] ||
    note "missing: a warm module cache at $mod"

# Every path Go writes to at job time has to belong to the runner. The image
# builds as root, so a directory left behind by a build step is root-owned and
# the first `go env` of the first job dies on it: that is exactly how
# /home/runner/.cache/go-build shipped broken once.
for dir in "$mod" "$HOME/.cache" "$HOME/go"; do
    [ -e "$dir" ] || continue
    [ -O "$dir" ] || note "not owned by the runner: $dir"
    [ -w "$dir" ] || note "not writable by the runner: $dir"
done
go env GOCACHE >/dev/null 2>&1 || note "go env fails, usually an unwritable GOCACHE"

if [ "$fail" -ne 0 ]; then
    echo "image verification failed" >&2
    exit 1
fi

# The rest is the job summary, so a run says what it shipped.
printf '| tool | version |\n|---|---|\n'
printf '| go | %s |\n' "$(go version | cut -d' ' -f3)"
printf '| helm | %s |\n' "$(helm version --short 2>/dev/null)"
printf '| kubectl | %s |\n' "$(kubectl version --client -o json | jq -r .clientVersion.gitVersion)"
printf '| promtool | %s |\n' "$(promtool --version 2>&1 | head -1 | cut -d' ' -f3)"
printf '| gh | %s |\n' "$(gh --version | head -1 | cut -d' ' -f3)"
printf '| psql | %s |\n' "$(psql --version | cut -d' ' -f3)"
printf '| yamllint | %s |\n' "$(yamllint --version | cut -d' ' -f2)"
printf '| mkdocs-material | %s |\n' "$(pip show mkdocs-material | sed -n 's/^Version: //p')"
printf '| module cache | %s |\n' "$(du -sh "$mod" 2>/dev/null | cut -f1)"
