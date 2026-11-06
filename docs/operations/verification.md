# Verification

`spec.verification.schema` and `spec.verification.data` run `pgcopydb compare` after the data transfer finishes, one Job per enabled check ([07-verified.yaml](../examples/07-verified.yaml)):

```yaml
spec:
  verification:
    schema: true   # compares both catalogs
    data: true     # reads selected rows on both sides
```

Both are opt-in because both are expensive: schema refetches both catalogs, and data reads every selected row on both sides.
A live migration can also run a data compare while it streams, to time the compare its cutover waits for; see [Rehearsal](#rehearsal).
The phase is `Verifying` while the checks run.
While the data check runs, `status.verificationProgress` shows how far it got; see [Progress](#progress).
Each check reports separately in `status.verification`, with the `startedAt` and `completedAt` of its Job, and in the `pgcopydb_migration_verification_check` metric.
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

For follow migrations, the schema and data checks run last, after the operator verifies the drain and drops the slot.
A rehearsal is the one compare that runs earlier, and for that reason it reports no verdict.

## Rehearsal

On a live migration the drain verify runs `pgcopydb compare data` inside the write-downtime window ([runbook](live-migration.md#manual-cutover-runbook)), so the duration of that compare sets the size of the window.
`spec.verification.rehearsal` measures it before the cutover: it runs the same compare once while the migration streams.
[10-live-rehearsal.yaml](../examples/10-live-rehearsal.yaml) is a complete resource.

```yaml
spec:
  follow:
    enabled: true
  cutover:
    mode: Manual      # required
  verification:
    rehearsal: true   # compares once while the migration streams
```

The rehearsal requires `follow.enabled` and `cutover.mode: Manual`, and the API server rejects a Migration that sets it without them.
An Automatic cutover starts as soon as the stream catches up, and the cutover cancels a rehearsal.

Once the phase is `Streaming` or `CutoverPending`, the operator starts Job `<name>-rehearsal-<n>`, where `n` counts the runs.
The Job runs beside the worker and does not mount the work volume, which the worker holds.
In a scratch directory, `pgcopydb list tables` reads the current source catalog with the Migration's `spec.clone.filters`, and `pgcopydb compare data` compares the tables it lists.
The compare scans every selected table on both databases, as the drain verify does.

`status.verificationRehearsal` records the latest run:

```sh
kubectl get pgm billing -o jsonpath='{.status.verificationRehearsal}' | jq
```

```json
{
  "armed": true,
  "run": 1,
  "jobName": "billing-rehearsal-1",
  "startedAt": "2026-11-06T09:12:04Z",
  "completedAt": "2026-11-06T09:31:47Z",
  "outcome": "Completed",
  "message": "pgcopydb compare data finished in 19m43s while the migration streamed; tables that differ are expected then and are no verdict"
}
```

`completedAt` minus `startedAt` is how long the run took under the load at that time, pod start and catalog read included.
While the run goes on, its item in `status.verificationProgress` counts the tables and bytes compared so far; see [Progress](#progress).
The outcome is one of these:

- `Running`: the Job runs.
- `Completed`: `compare data` wrote its report, whatever the report says.
- `Canceled`: the run was stopped; the message names why.
- `Failed`: `compare data` wrote no report, or someone deleted the Job before it finished; the message carries the last error from the Job's log.

A run that cannot start, for example because a ResourceQuota or an admission webhook refuses its Job, leaves `armed` false and puts the reason in `message`.
The operator tries again on each pass.

> [!important]
> A rehearsal reports a duration, not a verdict.
> While the migration streams, the source takes writes and the target is still applying them, so tables that differ are expected.
> The rehearsal never sets `Verified`, the phase, or `status.verification`, and the drain verify alone decides the cutover.

The drain verify runs with source writes stopped, and the rehearsal competes with live writes and the apply.
Its duration is therefore an estimate of the compare in the cutover window, not a measurement of it.
Each table's checksum query holds a snapshot on both databases for its whole scan, so until a large table's scan ends, vacuum on either side cannot remove rows that died after it started.
Run it when both databases can take a full read of the selected tables next to the live traffic.

The rehearsal reads the source catalog as it is when the run starts.
A table created on the source after the clone is missing on the target, because DDL is not replicated.
Its checksum query fails, and the bundled runner records that table as an `error` row, compares the rest, and writes its report, so the run ends `Completed`.
With `0.18.136.gb42f560` or older, the run writes no report and ends `Failed`.

### Run it again

The operator starts one run for each change of `rehearsal` from `false` to `true`.
`status.verificationRehearsal.armed` is true from the start of a run until `rehearsal` is `false` again and the run's pod has ended.
A `true` that arrives while `armed` is still true starts nothing, so a quick toggle can leave you with no new run.

1. Set `rehearsal` to `false`.

    ```sh
    kubectl patch pgm billing --type=merge -p '{"spec":{"verification":{"rehearsal":false}}}'
    ```

2. Wait until `armed` is false.

    ```sh
    kubectl wait pgm/billing --for=jsonpath='{.status.verificationRehearsal.armed}'=false --timeout=10m
    ```

3. Set `rehearsal` to `true`.

    ```sh
    kubectl patch pgm billing --type=merge -p '{"spec":{"verification":{"rehearsal":true}}}'
    ```

The new run's Job replaces the previous one, and the operator deletes the previous Job.

### What cancels a rehearsal

A running rehearsal ends `Canceled` when `rehearsal` goes back to `false`, when the cutover starts, on a suspend, when a worker attempt fails, when the Migration ends `Completed` or `Failed`, and when the Migration is deleted.
The operator deletes every unfinished rehearsal Job of the Migration in the foreground, so a run whose start it failed to record stops too.
pgcopydb checks for a stop only between tables, so the Job's TERM handler stops pgcopydb and cancels the compare's queries on both databases with `pg_cancel_backend`.
It cancels the sessions of the migration role that come from the rehearsal pod's client address and are named `pgcopydb[<pid>] compare worker`, the processes that run the checksum queries.
The Job also sets `client_connection_check_interval` to 10 seconds, so a backend notices within seconds when the pod is gone and ends its query.
The [retry gate](lifecycle.md#the-retry-gate) and the drain verify both wait until the rehearsal pod has ended: its compare sessions count as leftovers on the target, and its scans would stretch the write-downtime window.
While the drain verify waits, `CutoverCompleted` is `Unknown` with reason [`RehearsalPodRemains`](../troubleshooting.md#cutovercompleted-is-unknown-with-reason-rehearsalpodremains), and the message names the pod and its node.

> [!note]
> `client_connection_check_interval` exists from PostgreSQL 14 on, and the Job sets it as a startup option on both connections.
> An older server refuses the connection, as does a connection pooler that rejects startup options, and the rehearsal then ends `Failed`.

A refused connection shows in the rehearsal's message, which quotes the last error from the Job's log.

> [!warning]
> The cancel finds sessions by client address, role, and name, so it cannot tell compare Jobs apart that share an address.
> Behind a proxy or NAT that gives several pods one address, it also cancels the compare workers of other compare Jobs of the same role, such as another Migration's drain verify.
> A connection string that sets `application_name` replaces pgcopydb's session names, and the cancel then matches nothing: only `client_connection_check_interval` ends the queries, once the pod is gone.

Behind a shared address, start a rehearsal only while no other compare of the same role runs, and keep `application_name` out of the connection strings where you can.

> [!warning]
> Each checksum query holds an `ACCESS SHARE` lock on its table until it ends.
> A `TRUNCATE` of that table on the source then waits for the rehearsal's scan, and so does the apply when it replays a `TRUNCATE` on the target, which stalls the stream.
> We inferred this from PostgreSQL's lock rules and have not reproduced it.
> Avoid `TRUNCATE` on selected tables while a rehearsal runs.

## Progress

The bundled runner's `pgcopydb compare data` logs a progress line at most 10 seconds apart, and one last line after every table was compared.
The operator reads these lines from the Job's log into `status.verificationProgress`, which holds one item for each kind of run:

- `rehearsal`: the [rehearsal](#rehearsal) Job.
- `drain`: the drain verify Job `<name>-verify`, only when it compares; the exact-LSN fast path leaves no item.
- `data`: the `<name>-compare-data` Job of `spec.verification.data`.

`compare schema` logs no progress, and a runner older than `0.18.144.g86bf908` logs none for any compare, so these leave no item.

```sh
kubectl get pgm billing -o jsonpath='{.status.verificationProgress}' | jq
```

```json
[
  {
    "kind": "rehearsal",
    "jobName": "billing-rehearsal-1",
    "observedAt": "2026-11-06T09:20:14Z",
    "final": false,
    "plannedTables": 412,
    "comparedTables": 168,
    "plannedBytes": "84Gi",
    "comparedBytes": "21Gi",
    "comparedRows": 96118342,
    "differingTables": 3,
    "elapsed": "8m10.448s",
    "inFlight": [
      { "schema": "public", "name": "orders", "bytes": "18Gi", "elapsed": "4m51s" }
    ]
  }
]
```

The fields mean this:

- `plannedTables` and `plannedBytes`: the tables the run compares, and their size on the source.
- `comparedTables` and `comparedBytes`: the tables that are finished, whether they match, differ, or failed, and their size on the source.
- `comparedRows`: the source rows of the finished tables that match or differ.
- `differingTables`: tables whose row count or checksum differs.
- `failedTables`: tables whose checksum query failed.
- `elapsed`: the time since `compare data` started, its read of the source catalog included.
- `inFlight`: the tables being compared, oldest first, at most 64, each with its source size and the time since it started, in whole seconds.
- `observedAt`: when the operator last read the sample.

While a Job runs, the operator reads the last 1000 lines of its log on each pass and keeps the newest sample.
A window without a sample keeps the previous one.
After the Job ends, the operator reads its log once more, and `final` turns true when the last line is there.
When that read fails, the operator retries it for up to 2 minutes, and the rehearsal's outcome and the completion after `spec.verification.data` wait for it.
A run that ends without the last line, because it was canceled or its pod died, keeps its last sample with `final` false.
When the drain verify or the data compare retries a failed pod, or its Job is recreated under the same name, the operator reads the new pod's samples too.
The drain verify and the data compare print the newest progress line again after their report, so a long report cannot push it out of what the operator reads.
An item stays after its run ends.
The operator removes it when it creates the next Job of the same kind, and that Job's first sample adds it back, so `jobName` names the run it describes.

The operator exports each item's counts, sizes, and duration as gauges labeled with its `kind`, such as `pgcopydb_migration_verification_compared_tables`, and the time spent on each table in flight; see the [metric reference](monitoring.md#metric-reference).
The Verification row of the Migration Detail dashboard charts them, with the average bytes and rows per second since the run started; see [Monitoring](monitoring.md#reading-migration-detail).

> [!note]
> In a rehearsal, `differingTables` above 0 is expected: the source takes writes while the compare runs, and the target applies them later.
> `failedTables` counts tables pgcopydb could not compare at all, such as a table created on the source after the clone.

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
The bundled runner gives each report row a `status`, and the operator rejects a row whose `status` is anything but `match`; rows from an older runner carry none and are checked as before.
This report check also covers runner overrides whose comparator does not return a nonzero code on content differences.
