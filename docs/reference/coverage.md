# pgcopydb option coverage

Every `pgcopydb clone` and `pgcopydb follow` option (pgcopydb 0.18, per the [upstream reference](https://pgcopydb.readthedocs.io/)) mapped to its `Migration` spec field, to operator-managed behavior, or to an explicit exclusion.

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
| `--no-owner` | `spec.clone.noOwner` | `spec.clone.ownerAfterRestore` builds on it, an operator-level handover run after the worker exits; see [Ownership after restore](prerequisites.md#ownership-after-restore-cloneownerafterrestore). |
| `--no-acl` | `spec.clone.noACL` | |
| `--no-comments` | `spec.clone.noComments` | |
| `--no-tablespaces` | `spec.clone.noTablespaces` | |
| `--skip-large-objects` | `spec.clone.skip: largeObjects` | |
| `--skip-extensions` | `spec.clone.skip: extensions` | |
| `--skip-ext-comments` | `spec.clone.skip: extensionComments` | Already implied by `skip: extensions`; on its own it installs extensions without their COMMENTs. |
| `--skip-collations` | `spec.clone.skip: collations` | |
| `--skip-vacuum` | `spec.clone.skip: vacuum` | |
| `--skip-analyze` | `spec.clone.skip: analyze` | |
| `--skip-db-properties` | `spec.clone.skip: dbProperties` | |
| `--skip-split-by-ctid` | `spec.clone.skip: ctidSplit` | |
| `--requirements` | not exposed | Needs a file produced by `pgcopydb list extensions --requirements --json`, which has no declarative form in the spec. |
| `--filters` | `spec.clone.filters` | Rendered to the INI, mounted from an operator-owned ConfigMap. All eight filter sections are covered. |
| `--fail-fast` | `spec.clone.failFast` | |
| `--restart` | operator-managed | First attempt only: any pre-existing work-dir state is foreign and gets wiped. |
| `--resume` | operator-managed | Retry attempts resume from the work-dir catalogs. |
| `--not-consistent` | operator-managed | Paired with `--resume`: the failed attempt's snapshot died with its process. |
| `--snapshot` | not exposed | Needs a snapshot-holder sidecar to keep the exported snapshot alive for the whole clone. |
| `--follow` | `spec.follow.enabled` | |
| `--plugin` | `spec.follow.plugin` | |
| `--publication` | `spec.follow.publication` | Empty lets pgcopydb create and drop its own. |
| `--wal2json-numeric-as-string` | `spec.follow.wal2jsonNumericAsString` | CEL rejects it unless `follow.plugin` is `wal2json`; other plugins would silently ignore it. |
| `--replay-no-op-updates` | `spec.follow.replayNoOpUpdates` | |
| `--slot-name` | `spec.follow.slotName` | Empty generates a unique per-Migration name; a set name is pattern-restricted to PostgreSQL's slot charset. |
| `--create-slot` | not exposed | `clone --follow` creates the slot during setup; nothing to configure. |
| `--origin` | operator-managed | Always the same generated per-Migration name as the slot; unique, so fan-in stays safe. |
| `--endpos` | operator-managed | Cutover sets it at runtime via `stream sentinel set endpos --current`. |
| `--use-copy-binary` | `spec.clone.useCopyBinary` | On by default. pgcopydb falls back to text per table when a column's binary encoding is unsafe. |
| `--all-databases` | `spec.clone.allDatabases` | Needs superuser on both sides; see [All databases](../configuration.md#all-databases). |
| `--host` / `--port` | not exposed | The operator drives the sentinel via `pods/exec`, not the TCP coordinator. |
| `--verbose` / `--debug` / `--trace` / `--quiet` | not exposed | Runner logs are structured JSON (`PGCOPYDB_LOG_JSON=on`) at the default level. |

## `pgcopydb follow`

The standalone `follow` command exposes a subset of the clone options with identical semantics; the rows above cover all of them.
The operator never runs standalone `follow`: it always runs `clone --follow`.
The base copy and the replication slot then share one snapshot, which keeps the result consistent.

`spec.follow.maxCatchupLag`, `spec.cutover`, `spec.suspend`, `spec.dryRun`, `spec.preflight`, `spec.backoffLimit`, `spec.ttlSecondsAfterFinished`, and the per-side `superuserSecretRef` are operator-level controls with no pgcopydb flag behind them.

## `pgcopydb compare`

With `spec.clone.allDatabases: true`, the schema compare also receives `--all-databases`.
Admission rejects `verification.data` in this mode because pgcopydb produces no JSON report for the strict wrapper to evaluate.

`spec.verification.schema` and `spec.verification.data` run `pgcopydb compare schema` and `pgcopydb compare data` after completion, each in its own Job on the work PVC.
Both take source, target, and `--dir` from the same operator-managed values as the rows above.
`compare data` adds `--json` and runs inside a wrapper that takes its verdict from the report, not from the exit code.
Stock pgcopydb 0.18 logs a differing table and still exits 0; the bundled runner's patched pgcopydb exits nonzero instead.
The wrapper reads the report back through `psql`, the only JSON parser in the runner image.
It fails the Job when a table differs on row count or checksum.
A compare that could not run, or a report that could not be read, fails the Job too.
The wrapper prints the report it evaluates, so the Job log keeps the per-table detail.

## PostgreSQL feature coverage

Every release candidate migrates a catalog of PostgreSQL features and holds the target to the source.
Each case is one SQL file under [`test/e2e/coverage/`](https://github.com/ydixken/pgcopydb-operator/tree/main/test/e2e/coverage), named `<area>/<case>` below.

The clone group creates each clone case in its own schema on the source and clones all of them in one Migration filtered to those schemas, with `spec.verification.schema` and `spec.verification.data` set.
A case passes when `Verified` is `True` and a fingerprint read on both sides matches.
The fingerprint covers row data per table and per partition leaf, relation and partition layout, replica identity with its index, columns, constraints, indexes, triggers, functions, views, row-level security policies, statistics objects, types, sequence values, comments, owners, grants, and large object contents.
Large objects belong to no schema, so a clone filtered by `includeOnlySchemas` still copies every large object in the database, with its OID and owner.

The follow group sets up each follow case the same way and starts one follow Migration over those schemas with a Manual cutover.
Once the Migration waits at `CutoverPending` with its lag converged, the spec runs every case's `@follow` statements on the source, then writes a marker row and waits until the target has it.
It approves the cutover and, after `Completed`, requires `Verified` `True` and a matching fingerprint, as the clone group does.
The fingerprint is read after the cutover, because the target's sequences keep their base-copy values until pgcopydb re-syncs them at the cutover.

An own case pins a documented limitation instead of identity.
It runs alone in a follow Migration of its own, set up like the follow group's, with `spec.backoffLimit: 1`, and its verdict names the outcome the spec requires:

- `preflight_refuses_rls`, `preflight_refuses_unlogged`: the Migration fails with `PreflightFailed` before any attempt, and the refusal names exactly the case's table.
- `apply_fails_on_ddl`: after `@follow` adds a column on the source, both attempts fail on the first change that uses it (SQLSTATE 42703), and the Migration fails with `BackoffLimitExceeded`.
- `large_objects_not_replicated`: the Migration completes with `Verified` `True`, and the fingerprint differs only in the large objects `@follow` patched, unlinked or created, each still in its base-copy state on the target.

[Prerequisites](prerequisites.md) states each of these limitations where it applies.

> [!note]
> When the source and target majors differ, the fingerprint skips constraints, indexes, triggers, functions, views, policies, and statistics objects.
> Each server prints those definitions itself, and identical objects print differently across majors.
> pgcopydb's schema compare still checks indexes and the constraints an index backs (primary key, unique, exclusion).
> Check and foreign key constraints, triggers, functions, views, policies, and statistics objects go unchecked on such a pair.

The last column is the oldest source major the case runs on.
A release candidate uses a PostgreSQL 17 source, so it skips the cases that need 18.
CI applies their SQL against PostgreSQL 18 on every pull request, and a cluster run with `E2E_PG_SOURCE=18 E2E_PG_TARGET=18` streams them.

| Case | Group | Verdict | Source |
|---|---|---|---|
| `limitations/ddl_add_column` | own | apply_fails_on_ddl | 14 |
| `limitations/large_object_change` | own | large_objects_not_replicated | 14 |
| `limitations/unlogged_writes` | own | preflight_refuses_unlogged | 14 |
| `objects/check_constraint` | clone | identical | 14 |
| `objects/comments` | clone | identical | 14 |
| `objects/cycle_sequence` | clone | identical | 14 |
| `objects/deferrable_fk` | clone | identical | 14 |
| `objects/exclusion_constraint` | clone | identical | 14 |
| `objects/functions` | clone | identical | 14 |
| `objects/grants` | clone | identical | 14 |
| `objects/identity_by_default` | clone | identical | 14 |
| `objects/large_objects` | clone | identical | 14 |
| `objects/no_primary_key` | clone | identical | 14 |
| `objects/plain_view` | clone | identical | 14 |
| `objects/rls_force` | own | preflight_refuses_rls | 14 |
| `objects/security_barrier_view` | clone | identical | 14 |
| `objects/statistics` | clone | identical | 14 |
| `objects/trigger` | clone | identical | 14 |
| `objects/unlogged_sequence` | clone | identical | 15 |
| `objects/unlogged_table` | clone | identical | 14 |
| `operations/array_composite_updates` | follow | identical | 14 |
| `operations/crud_keyed` | follow | identical | 14 |
| `operations/generated_stored` | follow | identical | 14 |
| `operations/generated_virtual` | follow | identical | 18 |
| `operations/nextval` | follow | identical | 14 |
| `operations/replica_identity_full` | follow | identical | 14 |
| `operations/replica_identity_index` | follow | identical | 14 |
| `operations/sequence_cutover` | follow | identical | 14 |
| `operations/toast_unchanged` | follow | identical | 14 |
| `operations/truncate` | follow | identical | 14 |
| `partitioning/cross_schema_leaf` | clone | identical | 14 |
| `partitioning/follow_routing` | follow | identical | 14 |
| `partitioning/hash_mod4` | clone | identical | 14 |
| `partitioning/list_default_null` | clone | identical | 14 |
| `partitioning/range_expr` | clone | identical | 14 |
| `partitioning/range_multicol` | clone | identical | 14 |
| `partitioning/two_level` | clone | identical | 14 |
| `types/bit_varbit` | clone | identical | 14 |
| `types/collation_c_posix` | clone | identical | 14 |
| `types/composite_array` | clone | identical | 14 |
| `types/domain_array` | clone | identical | 14 |
| `types/enum_added_value` | clone | identical | 14 |
| `types/interval` | clone | identical | 14 |
| `types/money` | clone | identical | 14 |
| `types/network` | clone | identical | 14 |
| `types/numeric_special` | clone | identical | 14 |
| `types/range_multirange` | clone | identical | 14 |
| `types/timestamp_infinity` | clone | identical | 14 |
| `types/toasted_jsonb` | clone | identical | 14 |
| `types/tsvector` | clone | identical | 14 |
| `types/uuid` | clone | identical | 14 |
| `types/xml` | clone | identical | 14 |
