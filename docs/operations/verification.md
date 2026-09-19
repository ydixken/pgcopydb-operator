# Verification

`spec.verification` runs `pgcopydb compare` after the migration completes, one Job per enabled check ([07-verified.yaml](../examples/07-verified.yaml)):

```yaml
spec:
  verification:
    schema: true   # compares both catalogs
    data: true     # reads every row on both sides
```

Both are opt-in because both are expensive: schema refetches both catalogs, and data reads the whole database twice.
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
> A compare against a still-streaming target always mismatches.

For follow migrations, the checks run last, after the operator verifies the drain and drops the slot.
