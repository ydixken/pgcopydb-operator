# Upstream issue drafts

Drafts for issues against [pgcopydb](https://github.com/dimitri/pgcopydb) found during operator development. Filing is reserved for the maintainer (outward communication needs sign-off). Further candidates are ledgered in [MILESTONES.md](../../MILESTONES.md) without drafts yet: resume-after-endpos exits 0 without replaying, swallowed apply-preamble errors, non-idempotent CREATE PUBLICATION on `--resume`.

## `pgcopydb list progress` (no `--filters`) permanently corrupts the stored filtering of a filtered catalog, killing concurrent or resumed `clone --filters` runs

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- PostgreSQL 17 on both source and target
- rootless podman

### Reproduction (self-contained)

```sh
podman network create pgnet
podman run -d --name src --network pgnet -e POSTGRES_PASSWORD=pw postgres:17
podman run -d --name tgt --network pgnet -e POSTGRES_PASSWORD=pw postgres:17
sleep 5
podman exec src psql -U postgres -c "
  CREATE SCHEMA audit;
  CREATE TABLE audit.events (id int);
  CREATE TABLE public.t (id int);
  INSERT INTO public.t SELECT generate_series(1, 100000);"

cat > filters.ini <<'EOF'
[exclude-schema]
audit
EOF

SRC=postgres://postgres:pw@src/postgres
TGT=postgres://postgres:pw@tgt/postgres
IMG=ghcr.io/dimitri/pgcopydb:v0.18

# 1. A filtered clone completes fine.
podman run --rm --network pgnet -v ./filters.ini:/filters.ini:ro -v work:/work \
  $IMG pgcopydb clone --source $SRC --target $TGT --dir /work/m \
  --filters /filters.ini

# 2. One bare list progress against the same work dir. The command fails on
#    its own bug (exit 12, "[SQLite] no such column: bytes") but has already
#    rewritten the stored filtering by then.
podman run --rm --network pgnet -v work:/work \
  $IMG pgcopydb list progress --json --dir /work/m

# 3. Resume the same filtered clone: exit 12, "different filtering".
podman run --rm --network pgnet -v ./filters.ini:/filters.ini:ro -v work:/work \
  $IMG pgcopydb clone --source $SRC --target $TGT --dir /work/m \
  --filters /filters.ini --resume --not-consistent
```

Live variant: run step 2 in a loop while the filtered clone from step 1 is still running. Every clone worker exits 12 with "different filtering" and the clone dies.

### Evidence

`SELECT filters FROM setup` in `<dir>/schema/source.db`:

- before the `list progress` call: `{"type":"SOURCE_FILTER_TYPE_EXCL","exclude-schema":["audit"]}`
- after: `{"type":"SOURCE_FILTER_TYPE_EXCL"}`

The work dir is poisoned for good: every later `clone --filters` run against it fails the filter check with exit 12.

### Root cause

`list progress` takes no `--filters` option, so it opens the catalogs with the bare filtering. The catalog setup check handles "Case 3: the catalogs carry a different filtering" by adopting the caller's filtering and writing it back into the catalog (`catalog_update_filters` write-back, catalog.c:1336). `cli_list_progress` does not set `skipFilterCheck`, so a read-only listing command runs that write path and overwrites the stored record with its own empty filtering. Commands that set `skipFilterCheck` (the sentinel commands) are unaffected.

Secondary bug: the progress query reads `sum(bytes)` from `s_table`, which has no `bytes` column (catalog.c:10911), so `list progress` always fails outright on 0.18 (with and without `--json`, see [#1036](https://github.com/dimitri/pgcopydb/issues/1036)). The corruption happens regardless of that failure.

### Suggested fix

Set `skipFilterCheck` for `list progress` (it only reads), or keep the adopted filtering in memory instead of writing it back to the catalog in Case 3.
