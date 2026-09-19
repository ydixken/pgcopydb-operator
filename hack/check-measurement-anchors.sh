#!/bin/sh
# Every docs/research/measurements.md#<anchor> pointer in the tree must name a
# heading that exists in that file. The Go comments carry these as plain text,
# which no markdown or mkdocs check can see.
set -eu

cd "$(dirname "$0")/.."

doc=docs/research/measurements.md
[ -f "$doc" ] || { echo "anchors: $doc missing" >&2; exit 1; }

# Slugged the way GitHub does it: lowercase, drop everything that is not a
# word character, a space, or a hyphen, then spaces to hyphens. Underscores
# survive, so a heading naming pg_table_size anchors with them intact.
have=$(grep -E '^#{2,} ' "$doc" | sed -e 's/^#* //' -e 's/[^A-Za-z0-9 _-]//g' \
        | tr 'A-Z ' 'a-z-' | sort -u)
[ -n "$have" ] || { echo "anchors: $doc has no headings, so this check proves nothing" >&2; exit 1; }

want=$(grep -rhoE 'docs/research/measurements\.md#[A-Za-z0-9_-]+' \
        --include='*.go' --include='*.md' --include='*.yaml' --include='*.yml' . \
        | sed 's/.*#//' | sort -u)
[ -n "$want" ] || { echo "anchors: no pointers found, so this check proves nothing" >&2; exit 1; }

rc=0
for a in $want; do
  printf '%s\n' "$have" | grep -qx "$a" || { echo "anchors: no heading for #$a in $doc" >&2; rc=1; }
done
[ "$rc" -eq 0 ] && echo "anchors: $(printf '%s\n' "$want" | wc -l) pointer(s) all resolve"
exit "$rc"
