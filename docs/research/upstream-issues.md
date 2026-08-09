# Upstream issue drafts

Drafts for issues against [pgcopydb](https://github.com/dimitri/pgcopydb) found during operator development. Filing is reserved for the maintainer (outward communication needs sign-off). Further candidates without drafts yet: resume-after-endpos exits 0 without replaying, swallowed apply-preamble errors, non-idempotent CREATE PUBLICATION on `--resume`.

## `pgcopydb list progress` (no `--filters`) permanently corrupts the stored filtering of a filtered catalog, killing concurrent or resumed `clone --filters` runs

Status: filed as [dimitri/pgcopydb#1038](https://github.com/dimitri/pgcopydb/issues/1038) on 2026-08-09.

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

## Concurrent catalog readers (`stream sentinel get`) can starve catalog writers: clone worker dies with `[SQLite 5: database is locked]` (exit 12)

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- `clone --follow` under Kubernetes; a supervising operator ran `pgcopydb stream sentinel get --json` in the worker pod every 30 seconds

### What happened

During the base copy of a live migration (observed 2026-08-09), an index-creation worker died on SQLITE_BUSY while the only other catalog users were the periodic sentinel reads. The clone supervisor then tore the run down. Log excerpt shape (JSON logging, fields abbreviated):

```
{"pid":123,"error_severity":"ERROR","message":"[SQLite 5: database is locked]"}
...
{"pid":1,"error_severity":"ERROR","message":"clone process 10 has terminated [6]"}
{"pid":1,"error_severity":"FATAL","message":"Terminating all processes in our process group"}
```

### Root cause (suspected)

`stream sentinel get` opens the same SQLite catalog databases the clone workers write to. With SQLite's default rollback journal and no busy timeout, a writer that hits a reader's lock gets SQLITE_BUSY immediately; pgcopydb treats the failed statement as fatal for that worker, and the clone dies with it. Any external reader on the sanctioned CLI surface (sentinel reads are the documented way to watch a migration) can therefore kill a running clone, with a probability that grows with polling frequency.

### Suggested fix

Set `PRAGMA busy_timeout` on catalog connections so writers wait out short reader locks instead of failing, or open the catalogs in WAL journal mode (`PRAGMA journal_mode=WAL`) so readers stop blocking writers entirely. Read-only commands could also open the catalog files read-only.

## Process-group termination on clone failure leaves the streaming receive child running, keeping the container alive indefinitely

Status: draft, not filed.

### Environment

- pgcopydb 0.18-1.pgdg12+1 (upstream container image)
- `clone --follow` under Kubernetes; pgcopydb is pid 1 of the container

### What happened

A clone worker died (see the SQLITE_BUSY draft above), the supervisor reported it, and pid 1 terminated the process group:

```
{"pid":1,"error_severity":"ERROR","message":"clone process 10 has terminated [6]"}
{"pid":1,"error_severity":"FATAL","message":"Terminating all processes in our process group"}
```

The streaming receive child survived that termination. For more than 40 minutes afterwards it kept reporting write_lsn/flush_lsn progress on the replication stream while no clone or apply work existed anymore. pid 1 never exited, so the container stayed alive, and a supervising Kubernetes Job never saw a failure: from the outside the pod looked healthy while the migration was dead. The run only ended when the pod was deleted externally.

### Root cause (suspected)

The group signal does not stop the receive subprocess (it likely ignores or misses SIGTERM while blocked on the replication connection), and the supervisor then waits for its children without a deadline.

### Suggested fix

After signaling the process group, wait with a bounded timeout and escalate to SIGKILL for children that survive; or have the receive child handle SIGTERM by closing the replication connection and exiting, so a failed clone reliably terminates the whole process tree.
