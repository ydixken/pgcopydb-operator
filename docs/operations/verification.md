# Verification

`spec.verification` runs `pgcopydb compare` after the migration completes, one Job per enabled check ([07-verified.yaml](../examples/07-verified.yaml)):

```yaml
spec:
  verification:
    schema: true   # compares both catalogs
    data: true     # reads every row on both sides
```

Both are opt-in because neither is free: schema refetches both catalogs, data reads the whole database twice. Results land in the `Verified` condition (`SchemaMismatch` or `DataMismatch`) and the `pgcopydb_migration_verified` metric, with phase `Verifying` while the checks run.

A mismatch reports, it does not fail the Migration. The data is already on the target by then, so failing would misstate what happened; and after a live cutover, writes that reached the target are indistinguishable from genuine differences. Read the condition and the compare Job logs, then decide.

For follow migrations the checks run last, after the drain is verified and the slot is dropped: comparing a still-streaming target mismatches by design. Quiesce the target before trusting a data compare.
