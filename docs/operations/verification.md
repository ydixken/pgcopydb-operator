# Verification

`spec.verification.schema` and `spec.verification.data` run `pgcopydb compare` after the data transfer finishes, with one Job for each enabled compare ([07-verified.yaml](../examples/07-verified.yaml)):

```yaml
spec:
  verification:
    schema: true   # compares both catalogs
    data: true     # reads selected rows on both sides
```

Both are opt-in, because both are expensive.
The schema compare reads both catalogs again, and the data compare reads every selected row on both sides.
A live migration can also run a data compare while it streams.
This measures the duration of the compare that its cutover waits for.
See [Rehearsal](#rehearsal).
The phase is `Verifying` while the schema compare and the data compare run.
While the data compare runs, `status.verificationProgress` shows how far it got.
With `spec.engine: rpgmigrate`, it shows only the final count, after the compare ends.
See [Progress](#progress).
The schema compare and the data compare each report separately in `status.verification`, with the `startedAt` and `completedAt` of their Jobs.
Both also report in the `pgcopydb_migration_verification_check` metric.
The `Verified` condition and the `pgcopydb_migration_verified` metric combine the two compares into one verdict.
This verdict names only the first mismatch.
The condition reason is `SchemaMismatch` or `DataMismatch`.

The metric for each of the two compares carries the opt-out, so `-1` and an absent series mean different things:

- `1`: the compare passed.
- `0`: the compare found a mismatch.
- `-1`: the spec does not request the compare.
- No series: a requested compare has no result yet.

A result has priority over the spec.
If you disable the schema compare or the data compare after it reported a mismatch, the `0` stays, because `status.verification` keeps the result.

> [!warning]
> The operator records a mismatch, and the Migration still completes.
> The data is already on the target by then.
> After a live cutover, a write that reached the target looks the same as a genuine difference.

Read the `Verified` condition and the compare Job logs before you act.

> [!warning]
> Quiesce the target before you trust a data compare.
> A compare against a target that still receives the stream can mismatch while replication catches up.

For follow migrations, the schema and data compares run last, after the operator verifies the drain and drops the slot.
A rehearsal is the one compare that runs earlier, and for that reason it reports no verdict.

## Rehearsal

On a live migration, the drain verify runs `pgcopydb compare data` inside the write-downtime window ([runbook](live-migration.md#manual-cutover-runbook)).
The duration of that compare therefore sets the size of the window.
`spec.verification.rehearsal` measures this duration before the cutover.
It runs the same compare one time while the migration streams.
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

The rehearsal requires `follow.enabled` and `cutover.mode: Manual`.
The API server rejects a Migration that sets the rehearsal without them.
An Automatic cutover starts as soon as the stream catches up, and the cutover cancels a rehearsal.

When the phase is `Streaming` or `CutoverPending`, the operator starts the Job `<name>-rehearsal-<n>`, where `n` counts the runs.
The Job runs beside the worker and does not mount the work volume, which the worker holds.
In a scratch directory, `pgcopydb list tables` reads the current source catalog with the `spec.clone.filters` of the Migration.
Then, in the same scratch directory, `pgcopydb compare data` compares the tables that `pgcopydb list tables` lists.
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

`completedAt` minus `startedAt` is the duration of the run under the load at that time.
This duration includes the pod start and the catalog read.
While the run continues, its item in `status.verificationProgress` counts the tables and bytes compared so far.
See [Progress](#progress).
The outcome is one of these:

- `Running`: the Job runs, or it has ended and the operator retries a failed read of its log (see [Progress](#progress)).
- `Completed`: `compare data` wrote its report, whatever the report says.
- `Canceled`: the run was stopped.
  The message names the reason.
- `Failed`: `compare data` wrote no report, or someone deleted the Job before it finished.
  The message contains the last error from the Job's log.

When a run cannot start, for example because a ResourceQuota or an admission webhook refuses its Job, `armed` stays false.
The operator puts the reason in `message`.
The operator tries again on each pass.

> [!important]
> A rehearsal reports a duration, not a verdict.
> While the migration streams, the source takes writes and the target still applies them, so tables that differ are expected.
> The rehearsal never sets `Verified`, the phase, or `status.verification`.
> Only the drain verify decides the cutover.

The drain verify runs with source writes stopped.
The rehearsal competes with live writes and the apply.
The duration of the rehearsal is therefore an estimate of the compare in the cutover window, not a measurement of it.
The checksum query of each table holds a snapshot on both databases for its full scan.
As a result, until the scan of a large table ends, vacuum on either side cannot reclaim rows that died after the scan started.
Run the rehearsal when both databases can take a full read of the selected tables next to the live traffic.

The rehearsal reads the source catalog as it is when the run starts.
A table created on the source after the clone is missing on the target, because DDL is not replicated.
The checksum query of that table fails.
The bundled runner records that table as an `error` row, compares the rest, and writes its report, so the run ends `Completed`.
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

The Job of the new run replaces the previous one, and the operator deletes the previous Job.

### What cancels a rehearsal

A running rehearsal ends `Canceled` in these cases:

- `rehearsal` changes back to `false`.
- The cutover starts.
- Someone suspends the Migration.
- A worker attempt fails.
- The Migration ends `Completed` or `Failed`.
- Someone deletes the Migration.

The operator deletes every unfinished rehearsal Job of the Migration in the foreground.
This also stops a run whose start the operator failed to record.
pgcopydb polls for a stop only between tables.
For this reason, the TERM handler of the Job stops pgcopydb and cancels the queries of the compare on both databases with `pg_cancel_backend`.
It cancels the sessions of the migration role that come from the client address of the rehearsal pod and have the name `pgcopydb[<pid>] compare worker`.
These sessions are the processes that run the checksum queries.
The Job also sets `client_connection_check_interval` to 10 seconds, so a backend notices within seconds when the pod is gone and ends its query.
The [retry gate](lifecycle.md#the-retry-gate) and the drain verify both wait until the rehearsal pod ends.
The compare sessions of that pod count as leftovers on the target, and its scans would stretch the write-downtime window.
While the drain verify waits, `CutoverCompleted` is `Unknown` with reason [`RehearsalPodRemains`](../troubleshooting.md#cutovercompleted-is-unknown-with-reason-rehearsalpodremains), and the message names the pod and its node.

> [!note]
> `client_connection_check_interval` exists in PostgreSQL 14 and later.
> The Job sets it as a startup option on both connections.
> An older server refuses the connection.
> A connection pooler that rejects startup options also refuses it.
> The rehearsal then ends `Failed`.

A refused connection shows in the rehearsal's message, which quotes the last error from the Job's log.

> [!warning]
> The cancel finds sessions by client address, role, and name, so it cannot distinguish compare Jobs that share an address.
> When a proxy or NAT gives several pods one address, it also cancels the compare workers of other compare Jobs of the same role.
> An example is the drain verify of another Migration.
> A connection string that sets `application_name` replaces the session names of pgcopydb.
> The cancel then matches nothing.
> Only `client_connection_check_interval` ends the queries, after the pod is gone.

Behind a shared address, start a rehearsal only while no other compare of the same role runs.
Where you can, do not put `application_name` in the connection strings.

> [!warning]
> Each checksum query holds an `ACCESS SHARE` lock on its table until it ends.
> A `TRUNCATE` of that table on the source then waits for the scan of the rehearsal.
> When the apply replays a `TRUNCATE` on the target, the apply also waits, and this stalls the stream.
> We inferred this from PostgreSQL's lock rules and have not reproduced it.
> Avoid `TRUNCATE` on selected tables while a rehearsal runs.

## Progress

In the bundled runner, `pgcopydb compare data` logs a progress line at most 10 seconds apart.
It logs one last line after all compare workers exit.
When a worker stops, the tables that it never reached stay uncompared.
In that last line, `comparedTables` can therefore be below `plannedTables`.
The drain verify and the data compare decide from the report, not from these lines.
The operator reads these lines from the Job's log into `status.verificationProgress`, which holds one item for each kind of run:

- `rehearsal`: the [rehearsal](#rehearsal) Job.
- `drain`: the drain verify Job `<name>-verify`, only when it compares.
  The exact-LSN fast path leaves no item.
- `data`: the `<name>-compare-data` Job of `spec.verification.data`.

`compare schema` logs no progress, and a runner older than `0.18.144.g86bf908` logs none for any compare, so these runs leave no item.

### Native engine

With `spec.engine: rpgmigrate`, only the `data` item exists.
The native engine has no rehearsal, and its drain is proved without a compare, so it leaves no `drain` item.
The native compare helper checks every progress line of `rpgmigrate compare data`, but its Job logs only its sealed result.
The operator therefore reads no sample while the compare runs.
After the helper succeeds, the operator records the final sample from that result: `final` is true, every planned table and byte is compared, and `inFlight` is empty.
A compare that the operator refuses leaves no item.

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
- `comparedTables` and `comparedBytes`: the finished tables, whether they match, differ, or failed, and their size on the source.
- `comparedRows`: the source rows of the finished tables that match or differ.
- `differingTables`: tables whose row count or checksum differs.
- `failedTables`: tables whose checksum query failed.
- `elapsed`: the time since `compare data` started, including its read of the source catalog.
- `inFlight`: the tables that the run compares now, oldest first, at most 64.
  Each table has its source size and the time since it started, in whole seconds.
- `observedAt`: when the operator last read the sample.

While a Job runs, the operator reads the last 1000 lines of its log on each pass and keeps the newest sample.
A window without a sample keeps the previous one.
After the Job ends, the operator reads its log one more time.
`final` becomes true when the last line is there.
When that read fails, the operator retries it for up to 2 minutes.
Until a read succeeds, the rehearsal's outcome stays `Running`, and a Migration with `spec.verification.data` stays in `Verifying`.
When a run ends without the last line, because of a cancel or because its pod died, it keeps its last sample with `final` false.
When the drain verify or the data compare retries a failed pod, the operator also reads the samples of the new pod.
The operator also does this when the Job is recreated under the same name.
The drain verify and the data compare print the newest progress line again after their report.
As a result, a long report cannot move that line out of the log lines that the operator reads.
An item stays after its run ends.
When the operator creates the next Job of the same kind, it deletes the item.
The first sample of that Job adds the item again, so `jobName` names the run that the item describes.

The operator exports the counts, sizes, and duration of each item as gauges labeled with its `kind`, such as `pgcopydb_migration_verification_compared_tables`.
It also exports the time spent on each table in flight.
See the [metric reference](monitoring.md#metric-reference).
The Verification row of the Migration Detail dashboard charts these gauges, with the average bytes and rows per second since the run started.
See [Monitoring](monitoring.md#reading-migration-detail).

> [!note]
> In a rehearsal, `differingTables` above 0 is expected.
> The source takes writes while the compare runs, and the target applies them later.
> `failedTables` counts the tables that pgcopydb could not compare at all.
> An example is a table created on the source after the clone.

## Partitioned tables

The schema compare and the data compare both compare the current source and target partition catalogs first.
Then they compare schema definitions or row contents.
They compare qualified parent and leaf identities, parent-child attachment, partition strategies and keys, bounds, and selected membership in both directions.
These differences fail verification, even when the original leaves still match:

- Missing parents or leaves.
- Detached or reparented leaves.
- Changed keys or bounds.
- Extra eligible target partitions.

Unrelated target partition families remain outside the comparison scope.

Verification supports RANGE, LIST, HASH, nested partitions, DEFAULT partitions, and empty storage leaves.
The data compare scans each selected storage leaf one time.
Partitioned parents supply metadata and do not add another scan of their descendants.
Verification does not support selected foreign partitions, and they fail verification.

The compares reuse the persisted include/exclude filters of the migration.
An exact or regex table include selects matching names and the ancestor metadata needed to compare their attachment.
It does not select nonmatching siblings.
A schema include selects eligible family members within the included schemas, so extra target members in that scope fail.
Explicit table exclusions remain excluded.
A selection of only a parent compares its metadata and does not add leaves to the copy or checksum selection.

> [!important]
> Schema verification can compare a parent with no leaves.
> Data verification MUST produce a nonempty report of matching storage tables.
> A parent-only selection that produces `[]` fails the operator's data verification, because pgcopydb compared no row contents.

The bundled data comparator returns a nonzero exit code on a mismatch.
The operator also inspects the JSON report.
It rejects missing reports, malformed reports, empty arrays, and absent row-count or checksum fields.
The bundled runner gives each report row a `status`.
The operator rejects a row whose `status` is anything but `match`.
Rows from an older runner carry no `status`, and the operator inspects them as before.
This inspection of the report also covers runner overrides whose comparator does not return a nonzero code on content differences.
