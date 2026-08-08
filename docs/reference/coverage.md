# pgcopydb option coverage

Every `pgcopydb clone` and `pgcopydb follow` option (per [pgcopydb-cli.md](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/pgcopydb-cli.md), sections 2.1 and 2.2, pgcopydb 0.18) mapped to its `Migration` spec field, to operator-managed behavior, or to an explicit exclusion. The three gaps the audit found are closed; the ledger lives in [MILESTONES.md](https://github.com/ydixken/pgcopydb-operator/blob/main/MILESTONES.md).

## `pgcopydb clone`

| pgcopydb option | Migration spec | Notes |
|---|---|---|
| `--source` | `spec.source` | Rendered to `PGCOPYDB_SOURCE_PGURI`; credentials via passfile, never argv. |
| `--target` | `spec.target` | Rendered to `PGCOPYDB_TARGET_PGURI`. |
| `--dir` | operator-managed | Fixed to `/work/pgcopydb` on the work PVC; `spec.workVolume` sizes it. |
| `--table-jobs` | `spec.clone.tableJobs` | |
| `--index-jobs` | `spec.clone.indexJobs` | |
| `--restore-jobs` | `spec.clone.restoreJobs` | 0 follows indexJobs, as upstream. |
| `--large-objects-jobs` | `spec.clone.largeObjectsJobs` | |
| `--split-tables-larger-than` | `spec.clone.splitTablesLargerThan` | Quantity, rendered to plain bytes. |
| `--split-max-parts` | `spec.clone.splitMaxParts` | |
| `--estimate-table-sizes` | `spec.clone.estimateTableSizes` | Runs `vacuumdb --analyze-only` on the source to refresh the estimates, unless `skip: analyze` is also set. |
| `--drop-if-exists` | `spec.clone.dropIfExists` | |
| `--roles` | `spec.clone.roles` | |
| `--no-role-passwords` | `spec.clone.noRolePasswords` | |
| `--no-owner` | `spec.clone.noOwner` | |
| `--no-acl` | `spec.clone.noACL` | |
| `--no-comments` | `spec.clone.noComments` | |
| `--no-tablespaces` | `spec.clone.noTablespaces` | |
| `--skip-large-objects` | `spec.clone.skip: largeObjects` | |
| `--skip-extensions` | `spec.clone.skip: extensions` | |
| `--skip-ext-comments` | `spec.clone.skip: extensionComments` | Already implied by `skip: extensions`; listing it alone installs extensions without their COMMENTs. |
| `--skip-collations` | `spec.clone.skip: collations` | |
| `--skip-vacuum` | `spec.clone.skip: vacuum` | |
| `--skip-analyze` | `spec.clone.skip: analyze` | |
| `--skip-db-properties` | `spec.clone.skip: dbProperties` | |
| `--skip-split-by-ctid` | `spec.clone.skip: ctidSplit` | |
| `--requirements` | not exposed (deliberate) | Needs a file produced by `pgcopydb list extensions --requirements --json`; no declarative story yet, revisit on demand. |
| `--filters` | `spec.clone.filters` | Rendered to the INI, mounted from an operator-owned ConfigMap. All eight filter sections are covered. |
| `--fail-fast` | `spec.clone.failFast` | |
| `--restart` | operator-managed | First attempt only: any pre-existing work-dir state is foreign and gets wiped. |
| `--resume` | operator-managed | Retry attempts resume from the work-dir catalogs. |
| `--not-consistent` | operator-managed | Paired with `--resume`: the failed attempt's snapshot died with its process. |
| `--snapshot` | not exposed (deliberate, for now) | Needs a snapshot-holder sidecar; open M2 task, decided from spike S8 evidence. |
| `--follow` | `spec.follow.enabled` | |
| `--plugin` | `spec.follow.plugin` | |
| `--publication` | `spec.follow.publication` | Empty lets pgcopydb create and drop its own. |
| `--wal2json-numeric-as-string` | `spec.follow.wal2jsonNumericAsString` | CEL rejects it unless `follow.plugin` is `wal2json`; other plugins would silently ignore it. |
| `--replay-no-op-updates` | `spec.follow.replayNoOpUpdates` | |
| `--slot-name` | `spec.follow.slotName` | Empty generates a unique per-Migration name; a set name is pattern-restricted to PostgreSQL's slot charset. |
| `--create-slot` | not exposed (deliberate) | `clone --follow` creates the slot during setup; nothing to configure. |
| `--origin` | operator-managed | Always the same generated per-Migration name as the slot; unique, so fan-in stays safe. |
| `--endpos` | operator-managed | Cutover sets it at runtime via `stream sentinel set endpos --current`. |
| `--use-copy-binary` | `spec.clone.useCopyBinary` | |
| `--all-databases` | not exposed (deliberate) | A Migration migrates one database; create one Migration per database. |
| `--host` / `--port` | not exposed (deliberate) | The operator drives the sentinel via `pods/exec`; the TCP coordinator stays the documented alternative if exec proves limiting. |
| `--verbose` / `--debug` / `--trace` / `--quiet` | not exposed (deliberate) | Runner logs are structured JSON (`PGCOPYDB_LOG_JSON=on`) at the default level; a verbosity knob can come with demand. |

## `pgcopydb follow`

The standalone `follow` command advertises a subset of the clone options (research section 2.2) with identical semantics; every one of them is covered by the rows above. The operator never runs standalone `follow`: it always runs `clone --follow`, so the base copy and the replication slot share one snapshot, which is the whole consistency point.

`spec.follow.maxCatchupLag`, `spec.cutover`, `spec.suspend`, `spec.backoffLimit`, and `spec.ttlSecondsAfterFinished` are operator-level controls with no pgcopydb flag behind them.

## `pgcopydb compare`

`spec.verification.schema` and `spec.verification.data` run `pgcopydb compare schema` and `pgcopydb compare data` after completion, each in its own Job on the work PVC. Both take source, target, and `--dir` from the same operator-managed values as the rows above. `compare data` adds `--json`, which makes the per-table result parseable from the Job logs.
