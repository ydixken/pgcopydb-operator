# Verification

`spec.verification` runs `pgcopydb compare` after the migration completes, one Job per enabled check ([07-verified.yaml](../examples/07-verified.yaml)):

```yaml
spec:
  verification:
    schema: true   # compares both catalogs
    data: true     # reads every row on both sides
```

Both are opt-in because neither is free: schema refetches both catalogs, data reads the whole database twice. The phase is `Verifying` while the checks run. Each check reports separately in `status.verification` and in the `pgcopydb_migration_verification_check` metric; the `Verified` condition (`SchemaMismatch` or `DataMismatch`) and the `pgcopydb_migration_verified` metric collapse them into one verdict, naming only the first mismatch.

The per-check metric carries the opt-out too, so a dashboard can tell a check nobody asked for from one still to come: `1` passed, `0` mismatched, `-1` not requested, and no series at all while a requested check has yet to produce a result. A result outranks the spec. Switching a check off after it reported a mismatch leaves the `0` standing, because `status.verification` keeps the result and hiding it behind the flag would be worse than stale.

A mismatch reports, it does not fail the Migration. The data is already on the target by then, so failing would misstate what happened; and after a live cutover, writes that reached the target are indistinguishable from genuine differences. Read the condition and the compare Job logs, then decide.

For follow migrations the checks run last, after the drain is verified and the slot is dropped: comparing a still-streaming target mismatches by design. Quiesce the target before trusting a data compare.
