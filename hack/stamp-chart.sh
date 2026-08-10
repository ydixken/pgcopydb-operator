#!/bin/sh
# Stamps the release-specific Artifact Hub annotations into the chart's
# Chart.yaml in the working tree, right before `helm package`. All three depend
# on the tag being released, so they are derived here instead of committed,
# where they would be wrong for every version but one.
#
#   images     the __TAG__ placeholder becomes the tag
#   prerelease set when the tag is a SemVer prerelease, so it stops being
#              claimed by itself at the first stable release
#   changes    the conventional-commit subjects since the previous tag
#
# The release workflow calls this; it edits a checkout and never commits.
set -eu

tag=${1:?usage: hack/stamp-chart.sh <git-tag>}
chart=charts/pgcopydb-operator/Chart.yaml
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

# Guard against a silent no-op: a renamed placeholder would otherwise ship a
# chart advertising images that do not exist.
if ! grep -q __TAG__ "$chart"; then
  echo "$chart: no __TAG__ placeholder to stamp, refusing to package" >&2
  exit 1
fi
sed -i.bak "s/__TAG__/$tag/g" "$chart"
rm -f "$chart.bak"

case $tag in
  *-*) printf '  artifacthub.io/prerelease: "true"\n' >> "$chart" ;;
esac

# Written to a file rather than captured in $(...): a case statement inside a
# command substitution is a parse error under bash's POSIX mode, which is what
# /bin/sh is on macOS.
prev=$(git describe --tags --abbrev=0 "$tag^" 2>/dev/null || true)
git log --no-merges --pretty=%s "${prev:+$prev..}$tag" | while IFS= read -r subject; do
  # Conventional-commit type: the leading run of lowercase letters.
  type=$(printf '%s' "$subject" | sed 's/[^a-z].*//')
  case $type in
    feat) kind=added ;;
    fix) kind=fixed ;;
    perf | refactor) kind=changed ;;
    # chore, ci, docs and test say nothing to someone installing the chart.
    *) continue ;;
  esac
  description=$(printf '%s' "${subject#*: }" | sed 's/\\/\\\\/g; s/"/\\"/g')
  printf '    - kind: %s\n      description: "%s"\n' "$kind" "$description"
done > "$tmp"

if [ -s "$tmp" ]; then
  printf '  artifacthub.io/changes: |\n' >> "$chart"
  cat "$tmp" >> "$chart"
fi

echo "stamped $chart for $tag"
