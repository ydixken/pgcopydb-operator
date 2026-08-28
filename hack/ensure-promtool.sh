#!/bin/sh
# Installs the pinned promtool into bin/. Pinned to the Prometheus version the
# rules will actually run under, so a check here means the same parse there.
# Renovate does not manage this pin; bump it alongside a Prometheus upgrade.
set -eu

version=3.12.0
# Prometheus 3.y.z tags its Go module v0.3yy.z; the two pins move together.
modtag=v0.312.0

bindir="$(cd "$(dirname "$0")/.." && pwd)/bin"
promtool="$bindir/promtool"

if [ -x "$promtool" ] && "$promtool" --version 2>/dev/null | grep -qF "version $version"; then
  exit 0
fi

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case $arch in
x86_64) arch=amd64 ;;
aarch64 | arm64) arch=arm64 ;;
esac

# Checksums from the v3.12.0 release's sha256sums.txt.
case "$os-$arch" in
linux-amd64) sha=20da47f8e5303f74aecb78edd7f7e39041dac08ac4939dba75efd7a900ae8867 ;;
darwin-arm64) sha=d758070049a4de5abbeb925b1d4540c5c38a2f40d1356f59aa17a71ac8b48be3 ;;
*)
  echo "ensure-promtool: no pinned tarball for $os-$arch, go-installing $modtag"
  mkdir -p "$bindir"
  GOBIN="$bindir" go install "github.com/prometheus/prometheus/cmd/promtool@$modtag"
  exit 0
  ;;
esac

tarball="prometheus-$version.$os-$arch.tar.gz"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL -o "$tmp/$tarball" \
  "https://github.com/prometheus/prometheus/releases/download/v$version/$tarball"
if command -v sha256sum >/dev/null; then
  echo "$sha  $tmp/$tarball" | sha256sum -c - >/dev/null
else
  echo "$sha  $tmp/$tarball" | shasum -a 256 -c - >/dev/null
fi
tar -xzf "$tmp/$tarball" -C "$tmp" "prometheus-$version.$os-$arch/promtool"
mkdir -p "$bindir"
mv "$tmp/prometheus-$version.$os-$arch/promtool" "$promtool"
echo "ensure-promtool: installed promtool $version into bin/"
