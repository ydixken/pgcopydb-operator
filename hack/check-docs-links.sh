#!/bin/sh
# Fails when a published page links into a directory mkdocs excludes from the
# site. mkdocs logs those at INFO and caps the level there
# (structure/pages.py: min(logging.INFO, ...)), so `mkdocs build --strict`
# passes on them, while it does abort on a target that is merely missing.
# Reference an excluded page by its absolute GitHub URL instead.
set -eu

cd "$(dirname "$0")/.."

# The prefixes come from mkdocs.yml, so this cannot drift when that list moves.
prefixes=$(awk '
  /^exclude_docs:/ { block = 1; next }
  block && /^[[:space:]]+[^[:space:]]/ {
    gsub(/^[[:space:]]+|[[:space:]]+$|\/$/, "")
    print
    next
  }
  block { exit }
' mkdocs.yml)

if [ -z "$prefixes" ]; then
  echo "check-docs-links: no exclude_docs entries in mkdocs.yml, so this check proves nothing" >&2
  exit 1
fi

alt=$(printf '%s' "$prefixes" | tr '\n' '|' | sed 's/|$//')

# "](", an optional leading / or ./ ../ hops, then an excluded directory. An
# absolute https:// URL to the same file does not match, which is the way to
# link one of these pages on purpose.
pattern="\]\(/?(\.{1,2}/)*($alt)/"

# Pages inside an excluded directory are not published either, so their links
# are nobody's problem; mkdocs logs those at DEBUG for the same reason.
set --
for p in $prefixes; do
  set -- "$@" --exclude-dir="$p"
done

if hits=$(grep -rnE "$pattern" docs --include='*.md' "$@"); then
  echo "check-docs-links: published page links into a directory mkdocs excludes:" >&2
  printf '%s\n' "$hits" >&2
  echo "" >&2
  echo "These render as dead links on the site and mkdocs --strict cannot catch them." >&2
  echo "Link the absolute URL instead:" >&2
  echo "  https://github.com/ydixken/pgcopydb-operator/blob/main/docs/<path>" >&2
  exit 1
fi

echo "docs links: no published page links into $(printf '%s' "$prefixes" | tr '\n' ' ')"
