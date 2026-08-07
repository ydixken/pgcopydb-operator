# pgcopydb follow / CDC internals: reference for the operator design

Sources: pgcopydb git main `d8c1ec51` (2026-07-03, version 0.18; tag `v0.18` is one commit behind main and content-identical for everything below), local clone at `/private/tmp/claude-501/-Users-ydixken-development-pgcopydb-operator/6205d4b3-3179-43ee-adfb-fc9bc4b48926/scratchpad/pgcopydb-follow-src`; in-tree docs (`docs/logical-decoding.rst`, `docs/sentinel-protocol.rst`, `docs/resume.rst`, `docs/ref/pgcopydb_follow.rst`, `docs/ref/pgcopydb_stream.rst`, `docs/ref/pgcopydb_clone.rst`, `docs/tutorial.rst`); readthedocs `/en/latest/` (tracks main) and `/en/v0.17/`.

**Version watershed (matters for image pinning).** v0.18 (current release) replaced the old CDC pipeline wholesale:

| | v0.17 and earlier | v0.18 / main |
|---|---|---|
| Default `--plugin` | `test_decoding` | `pgoutput` (auto-managed publication) |
| CDC storage | `.json` + `.sql` files in `XDG_DATA_HOME` / `--dir/cdc` | SQLite file pairs `<timeline>-<startlsn>-output.db` / `replay.db` |
| Follow workers | 3: prefetch, transform (SysV mqueue), catchup; alternating "prefetch+catchup" ↔ "live replay" (Unix pipes) modes | 2: `receive` + `apply` (transform runs inline inside apply); single catchup mode, no mode switching |
| `pgcopydb stream transform` command | exists | **removed** |
| Sentinel remote control | SQLite catalog only (shared `--dir` required) | SQLite catalog **or** TCP coordinator (`--host`/`--port`, default port 5442) |
| `pgcopydb stream prune` | absent | present (auto-run every 300 s by follow) |
| `--replay-no-op-updates`, `--max-replaydb-size` | absent | present |

Note: parts of `docs/ref/pgcopydb_follow.rst` (the "three concurrent subprocesses ... JSON files ... Unix pipe" description) are stale text describing v0.17; the code and `docs/logical-decoding.rst`/`stream.rst` are authoritative for v0.18. Everything below describes v0.18 unless flagged.

---

## 1. End-to-end lifecycle of `pgcopydb clone --follow`

`clone_and_follow()` in `src/bin/pgcopydb/cli_clone_follow.c`:

1. **Optional cleanup**: with `--restart`, runs `stream_cleanup_databases()` first: drops the replication slot, the auto-created publication, `DROP SCHEMA IF EXISTS pgcopydb CASCADE` on source, the replication origin on target, and deletes the snapshot file. (`--restart` therefore destroys the CDC stream; never use it to "retry".)
2. **Snapshot + slot creation** (`follow_export_snapshot` → `copydb_create_logical_replication_slot`, `snapshot.c`): for `pgoutput`, first `CREATE PUBLICATION "<name>"` (default name = slot name, default `pgcopydb`; `FOR ALL TABLES`, or an explicit `FOR TABLE <list>` when `--filters` is used; flagged `publicationAutoManaged` unless `--publication` names a pre-existing one). Then, over a replication-protocol connection, `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`. The slot's consistent-point LSN and the exported snapshot name are recorded (snapshot also written to a file to support `--resume --snapshot`). This is the consistency guarantee: the initial COPY sees exactly the state at the slot's consistent point; the slot delivers exactly everything after it: no gap, no overlap. If a snapshot was exported by a plain `pgcopydb snapshot` (non-replication protocol), it is **rejected** for follow; you must use `pgcopydb snapshot --follow --plugin ... --slot-name ...` (which creates the slot and holds the snapshot session open).
3. **Setup** (`follow_setup_databases` → `stream_setup_databases`, `ld_stream.c:2182`): creates the sentinel row (startpos = slot LSN, endpos = 0/0, apply = false) in the SQLite catalog, and on the **target** creates replication origin (`pg_replication_origin_create('pgcopydb')`) advanced to the slot LSN.
4. Fetches source schema into the SQLite catalog, closes SQLite handles (not fork-safe), then **forks two children**: `clone` and `follow`.
5. **clone child** (`copydb_clone_database`): STEP 1–10: schema fetch, `pg_dump` pre/post-data, restore pre-data, parallel table COPY under the shared snapshot (`SET TRANSACTION SNAPSHOT`), index/constraint builds, vacuum, sequences (STEP 9 sets sequence values as of the snapshot), restore post-data. On success it executes `sentinel_update_apply(sourceDB, true)`: "Updating the pgcopydb.sentinel to enable applying changes".
6. **follow child** (`follow_main_loop` → `followDB`, `follow.c`): forks the `receive` and `apply` workers (see §8). `receive` starts streaming from the slot **immediately, concurrently with the COPY** (prefetch), writing decoded messages into `output.db`. `apply` starts too, but blocks in `stream_apply_wait_for_sentinel()` until `sentinel.apply = true`. So: prefetch runs during copy (bounding WAL retention on the source); catchup/replay begins the moment the base copy finishes.
7. Parent waits for clone → closes the snapshot transaction → waits for follow (which exits when endpos is reached, §2) → runs `follow_reset_sequences()`, the equivalent of `pgcopydb copy sequences --resume --not-consistent`, fetching the **now-current** sequence values (not snapshot values) and applying them to target, then waits for stragglers and exits 0.

The follow child loops: when workers exit successfully but endpos is unset (caught up, nothing to do), it restarts them in the same mode ("Restarting logical decoding follower ... (endpos unset, waiting for more data)"). It returns success only when `follow_reached_endpos()` says done; if endpos is set but workers exited without reaching it → error exit.

## 2. The sentinel

**Where it lives.** A single SQLite row: table `sentinel(id=1, startpos, endpos, apply, write_lsn, flush_lsn, replay_lsn)` in `<workdir>/schema/source.db` (workdir = `--dir`, else `$TMPDIR/pgcopydb`, else `/tmp/pgcopydb`). It is **not** stored in Postgres in current versions (very old releases used a `pgcopydb.sentinel` table on the source; `stream cleanup` still drops the leftover `pgcopydb` schema there). Consequence: a plain `pgcopydb stream sentinel ...` command must run with the **same `--dir` / same filesystem** as the follow process (i.e., in the same pod/container or on a shared volume), *or* use the TCP transport.

**TCP coordinator (v0.18)**: start the server side with `--host`/`--port` (or `PGCOPYDB_HOST`/`PGCOPYDB_PORT`) on `clone --follow` / `follow` / `stream replay`; the coordinator runs *inside the follow supervisor* (100 ms accept folded into its monitoring loop) and applies sentinel reads/writes using its own SQLite handle and the fork-inherited `IPC_PRIVATE` SysV write semaphore. Clients: `pgcopydb stream sentinel get|set ... --host <h> [--port 5442]`: no catalog access, no shared volume needed; fails hard (no fallback) if unreachable. Rationale: an independently-launched CLI cannot share the `semget(IPC_PRIVATE)` semaphore and SQLite-direct access across containers is prone to `SQLITE_BUSY` (`docs/sentinel-protocol.rst`). Wire protocol in `ld_ipc.h`: `PING/PONG`, `SET_STARTPOS`, `SET_ENDPOS`, `SET_APPLY`, `QUERY_SENTINEL`→`SENTINEL_REPLY`, `ACK_CONFIRMED`/`ERROR`. **For the operator, the TCP coordinator is the natural control plane: no shared PVC mount between the operator and the migration pod.**

**Commands** (`cli_sentinel.c`):
- `pgcopydb stream sentinel setup <startpos> <endpos>`: bootstrap (unit-test/advanced; normal runs do this automatically).
- `pgcopydb stream sentinel get`: prints `startpos endpos apply write_lsn flush_lsn replay_lsn`; selectors `--startpos --endpos --apply --write-lsn --flush-lsn --replay-lsn` print one raw value (`--apply` prints `enabled`/`disabled`); `--json` prints a JSON object.
- `pgcopydb stream sentinel set startpos <lsn>`: advanced/testing only.
- `pgcopydb stream sentinel set endpos <lsn>` or `set endpos --current`: `--current` requires `--source <pguri>` and executes `pg_current_wal_flush_lsn()` on the source, then stores it; prints the LSN on stdout.
- `pgcopydb stream sentinel set apply` / `set prefetch`: set the apply boolean true/false (prefetch = receive only, don't apply). `clone --follow` flips apply on automatically after the base copy; the operator only needs these for manual gating.
- Pruning is **not** a sentinel subcommand: `pgcopydb stream prune [--dry-run]` deletes fully-applied `output.db`/`replay.db` pairs (safe when file `endpos < sentinel.replay_lsn` and the receive process closed it); the follow process auto-prunes every 300 s.

**Semantics of the fields**: `startpos` = slot consistent point at setup (not advanced later); `endpos` = stop position (0/0 = run forever); `apply` = gate for the apply stage; `write_lsn` = last LSN received by `receive`; `flush_lsn` = last LSN made durable client-side; `replay_lsn` = last transaction COMMIT LSN durably applied to the target (updated only after target COMMIT succeeds; also fed back to the source so `pg_stat_replication.replay_lsn` mirrors it).

**Driving cutover from the operator**:
- Set the cutover point: `pgcopydb stream sentinel set endpos --current --source $SRC` (or compute an LSN and `set endpos <lsn>`). Endpos can be set before or while the command runs, and can be moved *forward*; setting an endpos ≤ current `replay_lsn` makes a (re)started follow exit immediately ("Current endpos ... was previously reached").
- **Endpos need not be a transaction boundary** (`docs/logical-decoding.rst`): pgcopydb handles endpos at a COMMIT boundary (apply that txn, `replay_lsn = endpos`), between transactions, or mid-transaction (the straddling transaction is **not applied**; slot will re-deliver it on any later run). Done detection is therefore two checks (`follow_reached_endpos`, `follow.c:443`): (a) `replay_lsn >= endpos`, else (b) the `apply` worker recorded `run_state='done'` in the internal `pipeline_state` table (clean intentional exit only; signal deaths record `'error'`).
- **Detecting "done" externally**: there is **no `done` field** in `sentinel get` output. Options, in order of reliability: (1) the `pgcopydb clone --follow` / `pgcopydb follow` **process exit**: exit code 0 after logging "Follow mode is now done, reached endpos %X/%X"; for `clone --follow` this also means sequences were already re-synced; (2) poll `stream sentinel get` and test `replay_lsn >= endpos`, sufficient in the common case but can miss the boundary/mid-txn cases where done fires via check (b) with `replay_lsn < endpos`; treat process exit as authoritative. In Kubernetes terms: run the migration as a Job/Pod and let cutover-complete = container exited 0 after endpos was set.
- **Known bug (present in v0.18 and main)**: `stream sentinel get --json` sets the `endpos` JSON field from the **startpos** value (`cli_sentinel.c:859`: `json_object_set_string(jsobj, "endpos", startpos)`). The JSON keys emitted are `startpos, endpos, apply, write_lsn, flush_lsn, replay_lsn`, but `endpos` is wrong. Use the plain-text output or the single-value selectors (`get --endpos`, `get --replay-lsn`) for automation. (Also, `get --transform-lsn` parses but has no output branch and falls through to the full listing.)

## 3. Replication internals

**Plugins**: `pgoutput` (default ≥0.18; core; requires a publication, auto-created and auto-dropped by pgcopydb unless `--publication` names an existing one), `test_decoding` (core; needs pgcopydb's catalog knowledge of primary keys to build DML), `wal2json` (external extension; `--wal2json-numeric-as-string` maps to wal2json's `numeric-data-types-as-string`). Parsers in `ld_pgoutput.c` / `ld_test_decoding.c` / `ld_wal2json.c`. Actions handled: BEGIN/COMMIT/INSERT/UPDATE/DELETE/TRUNCATE/MESSAGE/SWITCH/KEEPALIVE/ENDPOS/ROLLBACK (`ld_stream.h`).

**Slot**: default name `pgcopydb` (`--slot-name`); created via replication protocol with snapshot export at `clone --follow`/`follow` start (or earlier by `pgcopydb snapshot --follow`, or `--create-slot`). Dropped only by `pgcopydb stream cleanup` (or `--restart`).

**Origin tracking on target**: origin node name default `pgcopydb` (`--origin`; must be unique per source when fanning multiple sources into one target). Every applied transaction runs on the apply connection as: `BEGIN;` … statements … `SELECT pg_replication_origin_xact_setup(<commitLSN>, <commitTimestamp>); COMMIT;` (`ld_apply.c:913`), so origin progress advances atomically with the data: exactly-once apply. `synchronous_commit` is `off` on apply connections for throughput, switched to `on` for the final transaction that reaches endpos (`ld_apply.c:1609` region), so the recorded completion is durable. On (re)start, `setupReplicationOrigin()` reads `pg_replication_origin_progress()` and that value **overrides** everything else as the apply resume point.

**Restart/resume of streaming**: the source slot's `confirmed_flush_lsn` is advanced by the client's flush feedback and Postgres restarts delivery from `max(requested_startpos, confirmed_flush_lsn)`. In v0.18 the feedback loop (`stream_sync_sentinel`, `ld_stream.c:1550`) reports **`sentinel.replay_lsn` as the flush position once apply has committed anything**, so `confirmed_flush` only moves past transactions durably applied to the target, and any interrupted transaction is fully re-delivered. Before apply has ever committed (prefetch-only phase, `replay_lsn = 0/0`), the reported flush is the receive-side fsync position (`streamFlush`, `ld_stream.c:1338`): WAL that exists **only in the local `output.db`** is acknowledged to the source. **Operator consequence: the CDC/work directory must be on durable storage (PVC). If the pod loses the workdir after prefetch acked WAL, that data is unrecoverable from the slot.** On receive restart, startpos is clamped up to the slot's current LSN (`ld_stream.c:885`), and re-delivered rows are upserted idempotently (`INSERT OR REPLACE`) into `output.db`.

**WAL retention risk**: the slot pins WAL from its creation until `confirmed_flush` advances. Prefetch-during-copy exists precisely to let the source recycle WAL during the (long) initial COPY. But whenever the operator pauses streaming, the follow pod is down/CrashLooping, or apply is stuck, `pg_wal` grows on the **source** without bound. Monitor `pg_replication_slots` (`restart_lsn`, `confirmed_flush_lsn`, `active`, and on PG13+ `wal_status`/`safe_wal_size`). If the source has `max_slot_wal_keep_size` set, the slot can be invalidated (`wal_status='lost'`), which kills the migration and forces a full restart. The operator should surface slot lag and alert; and on migration abort/failure, it must run `pgcopydb stream cleanup` (or drop the slot itself) promptly.

**Teardown**: `pgcopydb stream cleanup` (`stream_cleanup_databases`, `ld_stream.c:2207`): drops the replication slot; drops the publication if auto-managed; `DROP SCHEMA IF EXISTS pgcopydb CASCADE` on source; removes the snapshot file; drops the replication origin on target. It needs the same workdir catalog (for the publication metadata); run it in the migration pod context. It is the mandatory final step after cutover *and* the mandatory rollback step for abandoned migrations.

## 4. Restrictions and caveats of follow mode

- **DDL is not replicated.** Any schema change on the source during the migration window is invisible to CDC (and can break apply). Docs specifically recommend pre-creating partitions (pg_partman) ahead of the window. The operator should document/enforce a "no DDL during migration" contract; optionally diff with `pgcopydb compare schema` before cutover.
- **Sequences are not replicated** by logical decoding. `pgcopydb clone --follow` re-syncs them **automatically after endpos is reached** (built-in `follow_reset_sequences`, equivalent to `pgcopydb copy sequences --resume --not-consistent`, fetching post-cutover current values). The **standalone `pgcopydb follow` command does *not* do this**: if the operator decomposes phases, it must run `pgcopydb copy sequences` after the follow process terminates (docs Example 2 shows exactly this).
- **Large objects are not replicated** in CDC (Postgres logical-decoding restriction). LOs are only handled by the base copy (`--skip-large-objects` to skip even that). If the app mutates LOs during the window, those changes are silently lost; flag as unsupported.
- **Tables without a primary key / replica identity**: UPDATE/DELETE require a replica identity. Fixes: `ALTER TABLE ... REPLICA IDENTITY USING INDEX <unique-not-partial-not-deferrable-notnull index>` or `REPLICA IDENTITY FULL`. With pgoutput, publications refuse UPDATE/DELETE on tables with `REPLICA IDENTITY NOTHING`/no identity (errors on the **source** at DML time, visible app breakage); a pre-flight check for PK-less tables is a must-have operator validation.
- **TRUNCATE is replicated** by all three plugins (emitted as `TRUNCATE ONLY <table>`, one relation per statement; `ld_transform.c:2545`).
- **No error-skip knobs for CDC.** The `--skip-*` family (`--skip-large-objects`, `--skip-extensions`, `--skip-collations`, `--skip-vacuum`, `--skip-analyze`, `--skip-db-properties`, `--skip-split-by-ctid`, ...) affects the clone/base-copy only. There is no "skip bad transaction"/"skip conflicting row" option for apply: an apply SQL error rolls back and the follow process exits nonzero; blind restarts will replay the same transaction and fail again (crash-loop). The only CDC-behavior flags are `--replay-no-op-updates` (replay UPDATEs that change no columns, needed when target UPDATE triggers must fire; default skips them) and `--filters` (table filtering applies to CDC too: filtered tables are excluded from the auto-created publication for pgoutput and filtered out in transform for the other plugins).
- **Empty transactions** are skipped (synthetic KEEPALIVE messages preserve LSN progress). `--endpos` is not transaction-boundary-aware (see §2). Only one migration per workdir; one slot name per source database (default `pgcopydb` collides if migrating several databases of the same instance; set distinct `--slot-name`/`--origin` per database; `clone --all-databases` cannot be combined with `--follow`).

## 5. Recommended cutover runbook (docs) and what the operator automates

From `docs/tutorial.rst` + `docs/ref/pgcopydb_clone.rst` (Examples 1 & 2):

```
$ pgcopydb snapshot --follow --plugin pgoutput --slot-name pgcopydb   # optional but recommended: hold snapshot in its own process
$ pgcopydb stream setup                                               # optional; clone --follow does it otherwise
$ pgcopydb clone --follow &                                           # base copy + CDC; keeps applying until endpos
# ... monitor lag (pg_stat_replication / sentinel get) until small ...
# 1. stop/disconnect applications writing to the source  (quiesce)
$ pgcopydb stream sentinel set endpos --current                       # 2. freeze the stream at the source's current flush LSN
# 3. wait for the clone --follow process to terminate (exit 0)        #    => all changes ≤ endpos applied
$ pgcopydb copy sequences                                             # 4. only needed when using standalone `pgcopydb follow`;
                                                                      #    clone --follow resets sequences itself before exiting
# 5. re-point applications at the target database                      (cutover)
$ pgcopydb stream cleanup                                             # 6. drop slot/publication/origin/pgcopydb schema
# stop the pgcopydb snapshot process (kill %1)
```

Ordering matters: writes must stop **before** `set endpos --current`, otherwise writes after the captured LSN are never replicated (and are silently lost if you cut over anyway). Signals of readiness for the operator state machine:
- *Ready-for-cutover* (window can open): base copy done (`sentinel get --apply` = `enabled`) **and** lag small: `pg_current_wal_lsn() - replay_lsn` below threshold, sustained.
- *Cutover in progress*: endpos set (record the LSN the `set endpos` command prints).
- *Drained*: process exit 0 (authoritative; encompasses sequences for `clone --follow`), or `replay_lsn >= endpos` as a progress observation.
- *Finalized*: `stream cleanup` completed; then app switch/DNS/service flip is external to pgcopydb.

The operator must automate: quiesce hook (or verify writes stopped, e.g. `pg_current_wal_lsn()` stable), `set endpos --current`, waiting on Job completion, sequence sync when using decomposed commands, `stream cleanup`, and abort path (cleanup on failure to release the slot).

## 6. Failure and retry semantics

- **Idempotent / safe to retry**: re-running the same command with `--resume` (plus `--not-consistent` once the original snapshot transaction is gone, a required pairing). Base copy: fully-copied tables are skipped, interrupted COPYs restart from scratch (COPY is transactional), CREATE INDEX/ALTER TABLE skipped if known-complete. CDC receive: slot re-delivers from `confirmed_flush`; `output.db` upserts are idempotent. CDC apply: origin progress + Guard 1 (`commitLSN <= previousLSN` → skip) makes replay exactly-once. `stream cleanup`, `stream prune`, and all `sentinel set` commands are idempotent. `copy sequences` is idempotent.
- **Consistency caveat on resume**: to resume the *base copy* consistently, the exported snapshot transaction must still be open. `pgcopydb clone --follow` holds it in-process (crash ⇒ snapshot lost ⇒ resume of an unfinished base copy is only possible with `--resume --not-consistent`, which risks inconsistency vs. the slot). The mitigation the docs recommend (and the operator should implement) is a **separate long-lived `pgcopydb snapshot --follow` holder** (sidecar/dedicated process) so the clone Job can crash and resume with `--resume --snapshot <id>` consistently. Once the base copy is finished, the snapshot no longer matters; follow-only restarts are always safe because state lives in the slot + origin.
- **Must NOT be retried blindly**: `--restart` (drops slot ⇒ the CDC stream and the source-consistency anchor are gone forever; only valid to start a brand-new migration); dropping/recreating the slot; `sentinel set startpos`; re-setting endpos backwards; and crash-looping apply on a deterministic SQL error (constraint conflict from target-side writes, missing replica identity, DDL drift); that needs human/operator intervention, not retries.
- **Apply dies mid-transaction**: the target transaction rolls back; origin progress and `replay_lsn` are unchanged; `confirmed_flush` was never advanced past the last committed boundary, so on the next run the slot re-delivers the whole transaction and apply replays it. No partial transactions can be observed on the target (apply's cursor only returns transactions whose COMMIT is already stored; `ld_apply.c:181`).
- **Worker failure coupling**: an abnormal worker exit ⇒ supervisor SIGTERMs the sibling ⇒ `followDB` returns false ⇒ the follow process exits nonzero ⇒ (under `clone --follow`) the whole command exits nonzero. Clean worker exits with endpos unset ⇒ supervisor restarts both workers in-loop. Exit code 0 strictly means: endpos reached and everything applied (or command-line `--endpos` already reached previously).

## 7. Observing progress during follow

- `pgcopydb stream sentinel get [--json|--write-lsn|--flush-lsn|--replay-lsn|--endpos|--apply]` (SQLite or `--host` TCP): receive lag = source `pg_current_wal_lsn()` − `write_lsn`; apply lag = source `pg_current_wal_lsn()` − `replay_lsn`; `apply` tells whether the base copy has finished (for `clone --follow`). Remember the `--json` endpos bug (§2).
- **Source-side, no pod access needed**: `pg_stat_replication`, where pgcopydb feeds back write/flush/apply positions, so `write_lsn`/`flush_lsn`/`replay_lsn` and the `*_lag` columns there track the pipeline (`replay_lsn` there = target durable commit position; `flush_lsn` = confirmed target-side durability, see §3). Connections carry an `application_name` built from the pgcopydb process title + pid (`pgsql.c:509`). `pg_replication_slots` for retention (`restart_lsn`, `confirmed_flush_lsn`, `active`, `wal_status`, `safe_wal_size`).
- Internal (same filesystem only): `sqlite3 <dir>/schema/source.db`: tables `sentinel`, `pipeline_state` (per-worker `run_state` running/done/error, last xid/LSNs), `cdc_files`, `command_log`, `setup`. Useful for deep diagnostics, not a stable API.
- Logs: the follow supervisor logs "Current sentinel replay_lsn is %X/%X, endpos is %X/%X" each poll, and the terminal "Follow mode is now done, reached endpos ...".

## 8. Multi-process architecture in follow mode (v0.18)

Process tree of `pgcopydb clone --follow`:

```
pgcopydb clone --follow            (top: snapshot holder unless external, setup, forks)
├── pgcopydb: clone                (base copy; its own worker pool for COPY/index/vacuum)
└── pgcopydb: follow               (supervisor: follow_main_loop; optional TCP coordinator;
    │                               polls sentinel, waits children, auto-prunes CDC files)
    ├── pgcopydb: follow receive   (startLogicalStreaming: slot → decode → output.db;
    │                               sends standby feedback; updates write/flush_lsn)
    └── pgcopydb: follow apply     (waits for sentinel.apply; driver loop alternating
                                    transform stage: output.db → replay.db (parameterised SQL)
                                    and replay stage: replay.db → target with origin tracking;
                                    updates replay_lsn; named "replay" in stream replay mode)
```

- The old third process (transform) is gone; transform is a stage inside apply's driver loop, which only evaluates terminal conditions on iterations that make no progress (guarantees produced transactions are consumed before declaring done).
- IPC: (1) SQLite stores (`output.db`, `replay.db`, `source.db`) with a fork-inherited `IPC_PRIVATE` SysV semaphore serialising writers (`lock_utils.c`); (2) a one-shot **receive→apply lifecycle pipe** carrying a single "done at LSN X" message, doubling as a Postgres-style death-watch (EOF without data = upstream died); latency optimisation only, durable `pipeline_state` is the fallback when stages run as separate commands; (3) the sentinel row as the external control channel; (4) optional TCP coordinator inside the supervisor.
- Failure coupling: error exit of either worker ⇒ SIGTERM to the other ⇒ supervisor exits nonzero (under endpos) or restarts the pair (no endpos). The clone child failing ⇒ parent runs `copydb_fatal_exit()` so the follow child doesn't wait forever for an apply-enable that will never come. SIGTERM/SIGINT to the supervisor are handled signals → clean shutdown, resumable with `--resume`; SIGQUIT → immediate error path.
- CDC disk usage is bounded by rotation (`--max-replaydb-size`, default 1 GB per `output.db`, rotated at commit boundaries; single oversized transactions may exceed it) plus auto-prune of fully-applied file pairs every 300 s.

### Operator design cheat-sheet (derived)

- Run `clone --follow` as a Job with a PVC-backed `--dir`; set `PGCOPYDB_HOST=0.0.0.0` so the operator can drive/inspect the sentinel via TCP (`--host <pod-ip> --port 5442`) without exec or shared volumes; consider a separate `pgcopydb snapshot --follow` holder pod for consistent resume of the base copy.
- Pre-flight validations: replica-identity audit, plugin availability (pgoutput needs nothing; wal2json needs the extension), `wal_level=logical`, `max_replication_slots`/`max_wal_senders`, slot-name uniqueness, no `--all-databases`.
- Cutover: quiesce → `stream sentinel set endpos --current` → wait Job success → (sequences if decomposed) → switch app → `stream cleanup`. Abort: `stream cleanup` to free the slot.
- Watch: slot retention on source, sentinel LSN deltas, pod exit codes; alert on `wal_status != 'reserved'` and on apply crash-loops (deterministic apply errors need intervention, not retries).

## Open questions from this research pass

- Whether the pgcopydb v0.18 `stream sentinel get --json` endpos bug (JSON endpos field carries the startpos value, cli_sentinel.c:859) has been reported/fixed upstream after commit d8c1ec51 (2026-07-03); not verifiable from the shallow clone; operator should use plain-text/selector output regardless.
- Exact feedback behavior in released v0.17 (JSON/SQL file model): this report's flush/confirmed_flush analysis (flush = replay_lsn once apply active, receive-fsync position during prefetch-only) was verified against v0.18 source only; v0.17 internals differ and were not audited since v0.18 is the version to pin.
- Behavior of pgoutput auto-created publications when new tables are created on the source mid-migration (FOR ALL TABLES publication would include them but the target lacks the table; DDL-not-replicated caveat presumably makes apply fail); not explicitly tested or documented in-tree.
- Precise minimum Postgres version bounds for source/target supported by pgcopydb 0.18 (docs reference PG10+ for pgoutput; a full support matrix was not located in the tree).
- Whether pg_stat_replication.flush_lsn on the source can transiently exceed the last target-committed LSN during the prefetch-only phase on ephemeral storage recovery scenarios; inferred yes from streamFlush/stream_sync_sentinel code paths, but not validated against a live cluster.
