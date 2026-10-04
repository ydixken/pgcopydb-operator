# Verification

`spec.verification` runs `pgcopydb compare` after the data transfer finishes, one Job per enabled check ([07-verified.yaml](../examples/07-verified.yaml)):

```yaml
spec:
  verification:
    schema: true   # compares both catalogs
    data: true     # reads selected rows on both sides
```

Both are opt-in because both are expensive: schema refetches both catalogs, and data reads every selected row on both sides.
The phase is `Verifying` while the checks run.
Each check reports separately in `status.verification` and in the `pgcopydb_migration_verification_check` metric.
The `Verified` condition and the `pgcopydb_migration_verified` metric collapse the checks into one verdict, which names only the first mismatch.
The condition reason is `SchemaMismatch` or `DataMismatch`.

The per-check metric carries the opt-out, so `-1` and an absent series mean different things:

- `1`: the check passed.
- `0`: the check found a mismatch.
- `-1`: the spec does not request the check.
- No series: a requested check has no result yet.

A result outranks the spec.
If you switch a check off after it reported a mismatch, the `0` stays, because `status.verification` keeps the result.

> [!warning]
> A mismatch is recorded, and the Migration still completes.
> The data is already on the target by then.
> After a live cutover, writes that reached the target are indistinguishable from genuine differences.

Read the `Verified` condition and the compare Job logs before you act.

> [!warning]
> Quiesce the target before trusting a data compare.
> A compare against a still-streaming target can mismatch while replication is catching up.

For follow migrations, the checks run last, after the operator verifies the drain and drops the slot.

## Partitioned tables

Both checks compare the current source and target partition catalogs before comparing schema definitions or row contents.
They compare qualified parent and leaf identities, parent-child attachment, partition strategies and keys, bounds, and selected membership in both directions.
Missing parents or leaves, detached or reparented leaves, changed keys or bounds, and extra eligible target partitions fail verification even when the original leaves still match.
Unrelated target partition families remain outside the comparison scope.

RANGE, LIST, HASH, nested partitions, DEFAULT partitions, and empty storage leaves are supported.
The data check scans each selected storage leaf once; partitioned parents supply metadata and do not add another scan of their descendants.
Selected foreign partitions are unsupported and fail verification.

Comparison reuses the migration's persisted include/exclude filters.
An exact or regex table include selects matching names and the ancestor metadata needed to check their attachment; it does not select nonmatching siblings.
Schema inclusion selects eligible family members within the included schemas, so extra target members in that scope fail.
Explicit table exclusions remain excluded.
Selecting only a parent checks its metadata without adding leaves to the copy or checksum selection.

> [!important]
> Schema verification can check a parent with no leaves.
> Data verification MUST produce a nonempty report of matching storage tables.
> A parent-only selection that produces `[]` fails the operator's data check because no row contents were compared.

The bundled data comparator returns a nonzero exit code on a mismatch.
The operator also validates the JSON report and rejects missing reports, malformed reports, empty arrays, and absent row-count or checksum fields.
This report check also covers runner overrides whose comparator does not return a nonzero code on content differences.
