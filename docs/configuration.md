# Configuration

The [CRD reference](reference/api.md) lists every field, with its default and its validation.
Complete resources with comments are in the [examples index](examples.md).

## Migration engine

Omit `spec.engine` or set it to `pgcopydb` to use the pgcopydb backend.
The effective engine is immutable for each Migration.
The controller rejects `rpgmigrate` until a native backend is implemented and qualified.
It sets `Validated=False` and `Failed=True` with reason `UnsupportedEngine` before it creates resources or executes commands.
It also rejects unknown engine values if admission does not reject them first.
The rejection does not delete, adopt, or clean up existing Jobs, PVCs, or replication resources.

## Clone tuning

The `clone` block holds parallelism, same-table splitting, and skips.
See [04-clone-tuned.yaml](examples/04-clone-tuned.yaml) for the full version with comments:

```yaml
spec:
  clone:
    tableJobs: 8                 # concurrent table COPY workers
    indexJobs: 8                 # concurrent CREATE INDEX workers
    splitTablesLargerThan: 2Gi   # tables above this copy in parallel parts
    skip: [largeObjects]         # steps to omit entirely
```

By default, the operator sets the table jobs to the CPU request of the worker.
It also turns on same-table splitting and binary COPY.
The other options that you do not set use the pgcopydb defaults.
Size the job counts against both endpoints.
See [Performance tuning](operations/performance.md).
Every `pgcopydb clone` flag maps to a spec field or to a recorded exclusion.
The [option coverage table](reference/coverage.md) lists the flags.

### Deferred index builds

Set `spec.clone.deferIndexes: true` to make pgcopydb copy every table before it builds any index or constraint or runs any `VACUUM ANALYZE`:

```yaml
spec:
  clone:
    deferIndexes: true   # build indexes after the whole copy
```

The copy then runs without index builds on the target, and the clone takes longer.
[Deferring index builds](operations/performance.md#deferring-index-builds) lists the trade-offs.
Admission rejects a change to `deferIndexes` on an existing Migration with `clone.deferIndexes is immutable: create a separate Migration`.
Preflight probes the runner image before the first attempt.
When the `pgcopydb clone --help` of the image does not list the `--defer-indexes` flag of the fork, preflight fails and names the image.
A custom runner image needs pgcopydb `0.18.136.gb42f560` or later.
See [Client tool versions](reference/prerequisites.md#client-tool-versions).

### Extension comments and skips

`clone.skip: [extensionComments]` suppresses only extension comments.
The restore keeps table and other object comments.
`clone.noComments: true` suppresses all restored comments.
Both options avoid the extension-comment ownership check that an otherwise empty `spec.clone: {}` can hit.
Neither avoids the check that `dropIfExists: true` adds for `DROP EXTENSION`.
`clone.skip: [extensions]` skips the restore of extensions, including extension comments.
It also bypasses both the availability and the ownership preflight checks.
If you skip extensions, provide the extensions that the application needs on the target yourself.
See [Prerequisites](reference/prerequisites.md#base-clone-every-migration) for the ownership rules and remedies.

### Ownership after restore

`clone.ownerAfterRestore` names the role that owns the restored objects after the migration finishes:

```yaml
spec:
  clone:
    dropIfExists: true
    noOwner: true
    ownerAfterRestore: app_role   # the application's owning role on the target
```

Use it when the migration cannot connect as the role that must own the objects.
This is the usual case on managed PostgreSQL, where the provider's admin role is not the application's owning role.
You cannot create a role that owns both sides.
Give the role name without quotes, as PostgreSQL stores it.
The operator quotes the name, so hyphenated names such as `db-admin` work.
`ownerAfterRestore` needs `noOwner: true`, and the CRD rejects the field without it.
Otherwise, `pg_restore` assigns the source owners, and the handover covers only the part of the schema that landed on the migration role.
The CRD also rejects `clone.allDatabases` with this field, because the handover runs only in the database of the target connection.

The handover runs as a `<name>-reown` Job after the worker exits.
On a plain clone it runs in `Finalizing`, before the verification compares, so nothing reads the target while ownership changes.
On a live migration it runs in `CuttingOver`, after the drain is proven and before `CutoverCompleted` turns True.
That condition is the signal to point applications at the target, so the handover time counts against the cutover window.

You can re-run the Job safely.
Each `ALTER` commits separately, so many partitions cannot push one transaction past `max_locks_per_transaction`.
The field is immutable after the Migration exists.
This is because preflight probes the role before the first attempt, and the operator builds the Job one time.

See [Ownership after restore](reference/prerequisites.md#ownership-after-restore-cloneownerafterrestore) for the object classes and privileges.
See [Ownership handover failures](troubleshooting.md#ownership-handover-failures) to recover a failed handover.

## All databases

Set `spec.clone.allDatabases: true` to clone the whole source instance with superuser credentials on both sides.

Both connections MUST name an existing maintenance database, such as `postgres`.
pgcopydb itself substitutes each database name into the connection URIs, and it creates the missing target databases.
[The all-databases contract](reference/prerequisites.md#all-databases) gives the superuser rules for each side and the preflight checks.
See [09-all-databases.yaml](examples/09-all-databases.yaml) for a complete resource.

> [!warning]
> Target databases MUST NOT already hold the schema that the migration restores.
> `pg_restore` reports errors on existing objects.
> This includes objects in the target's `postgres` database.
> Admission rejects `clone.dropIfExists`, `follow.enabled`, and `verification.data` with `allDatabases`.

Admission rejects those three options for these reasons:

- pgcopydb ignores follow in this mode.
- `dropIfExists` would drop the connected maintenance database.
- `verification.data` gets no JSON verdict from the data compare.

If admission does not enforce these rules, the controller rejects the same combinations with terminal reason `InvalidSpec` before it creates any Jobs.

Admission also rejects a change to `allDatabases` on an existing Migration with `allDatabases is immutable: create a separate Migration`.
Retries and compares reuse the work directory that this mode laid out.
A single-database data compare on an all-databases work directory checks only the maintenance database.

pgcopydb copies roles unconditionally, so you can set `clone.roles: true`, but it is redundant.
`clone.noRolePasswords` omits the role passwords, and the superuser contract still applies.
Filters, skip options, and other clone options apply to every database.
Job counts are global across databases, not multiplied per database.
Restart and resume use per-database work directories under the existing work directory.

The operator supports `verification.schema` across all databases.
In this mode, `status.progress` reports summed database sizes on each side, without per-database relation counts or pgcopydb counters.
`uriSecretRef` accepts both PostgreSQL URIs and libpq keyword/value connection strings (DSNs) for this mode.
pgcopydb replaces only the database name and keeps the other options, including the password, TLS settings, and `passfile`.
For inline and `secretRef` connections, the worker writes passfile entries as `host:*:*:user:password`, so one set of credentials authenticates every database.

## Filters

`clone.filters` selects what to copy.
The operator renders it to pgcopydb's filters INI file in an operator-owned ConfigMap:

```yaml
spec:
  clone:
    filters:
      excludeSchemas: ["audit", "scratch"]
      excludeTableData: ["public.event_log"]  # schema yes, rows no
```

Table names follow PostgreSQL quoting, and `~/regex/` patterns work.
`clone.filters` exposes all eight pgcopydb filter sections.
The [CRD reference](reference/api.md) lists the sections.

## Partial table policies

These policies require `engine: pgcopydb`, `follow.enabled: true`, and the `pgoutput` plugin.
The source MUST run PostgreSQL 15 or later.
The operator manages the slot and publications.
You MUST NOT combine these policies with a custom slot or publication.

```yaml
spec:
  follow:
    enabled: true
    unloggedTables:
      - table: app.transient_locks
        mode: SchemaOnly
      - table: app.cached_results
        mode: CopyOnce
    insertOnlyTables:
      - app.event_history
```

`SchemaOnly` restores the table without its rows.
`CopyOnce` copies the initial rows.
Neither mode replicates later changes.
The source tables stay UNLOGGED.
The selected UNLOGGED tables MUST be ordinary tables, not partitions or extension members.

`insertOnlyTables` requires ordinary LOGGED tables, not partitions or extension members.
The operator copies their initial rows and publishes later INSERTs through a separate publication.
It does not publish their UPDATE, DELETE, or TRUNCATE operations.
The target can retain stale rows and rows that the source deleted.
Preflight rejects another publication that publishes UPDATE or DELETE for these tables.
`allowMissingReplicaIdentity` remains an acknowledgement only.
It does not suppress any source operation or replication action.

Names MUST be exact, lowercase `schema.table` identifiers.
Table and schema filters MUST also use exact names.
Each list accepts at most 64 tables.
The lists MUST NOT overlap.
At least one ordinary LOGGED table MUST remain fully replicated.
The policies and `clone.filters` are immutable.
Create a new Migration to change them.

The operator checks every selected table before the copy.
It rejects UNLOGGED tables without an explicit policy.
It checks publication ownership against the Migration UID before reuse or removal.
Resume preserves established publication membership.

Content verification excludes all partial tables.
Schema verification still includes their structure.
`UnsynchronizedTables=True` names each partial policy.
`Verified=True` applies only to the configured verification scope, not full database equality.

INSERT-only migrations require exact replication-origin progress at a nonzero cutover end position.
Partial content comparisons cannot prove that the target applied every INSERT.
If the origin position differs, the drain check fails and retains the slot.
The operator does not infer a successful drain from partial checksums.

## Follow mode

`spec.follow.mode` selects how pgcopydb receives and applies the changes of a live migration.

```yaml
spec:
  follow:
    enabled: true
    mode: performance      # sqlite (default) | performance
```

The two values have these effects:

- `sqlite` keeps the changes in SQLite files on the work volume.
  When `walWorkers` is more than 1, each WAL worker uses its own replication slot.
- `performance` writes the pgoutput stream to a spool on the work volume as it arrives.
  It uses one replication slot and applies the changes from the spool.

When `mode` is not set, the migration uses `sqlite`.
A Migration that you created before the operator had this field uses `sqlite`.

With `mode: performance`, these rules apply:

- `follow.enabled` MUST be `true`.
- The plugin MUST be `pgoutput`.
- `spec.follow.publication` MUST be empty.
- `unloggedTables` and `highTrafficTables` MUST be empty.
- `walWorkers` MUST be unset or 1.
- `spec.engine` MUST be `pgcopydb`.
- `clone.allDatabases` MUST be `false`, because `allDatabases` with `follow.enabled` is not permitted.

You can use `insertOnlyTables` with `mode: performance`.
Admission rejects a spec that breaks these rules.

The mode is part of `spec.follow`, so it is immutable.
To change the mode, create a new Migration.

The operator sets `PGCOPYDB_FOLLOW_MODE=performance` in the worker Job only for `mode: performance`.
For `sqlite` or no mode, the operator does not set the variable, and the worker Job does not change.

> [!warning]
> `mode: performance` needs a runner image with a pgcopydb that shows `--follow-mode` in `pgcopydb clone --help`.
> An older pgcopydb ignores the variable and uses `sqlite`, so preflight fails the Migration on such a runner ([Prerequisites](reference/prerequisites.md#follow-mode)).

The same source changes can give a different target in the two modes.
These are the cases:

- A transaction starts before the cutover end position and commits after it.
  `performance` stops at a cut: the larger of the end position and the highest commit position that the workers applied before the end position arrived.
  It does not apply a transaction that commits above the cut.
  Thus a commit above the end position that a worker applied before the end position arrived stays applied.
- The definition of a table changes during the live migration.
  `performance` decodes the changes with the new definition.
  `sqlite` decodes them with the first definition that it received.
- An UPDATE or DELETE has a key column that has no value in the change.
  `performance` stops the run with an error.
  In `sqlite`, the statement can change more target rows than the source changed.
- The text of a key value changes with a session setting, for example `DateStyle`.
  `performance` sets these settings to fixed values before it receives changes.
  `sqlite` does not set them.
- A source transaction has a replication origin.
  In `sqlite`, such a transaction can stop the live migration, or the target does not get it.

## WAL workers

By default, one stream receives the source changes and one connection applies them to the target.
Set `spec.follow.walWorkers` to split the change stream into parallel WAL workers.
Each worker has its own replication slot on the source and its own replication origin on the target.
Each worker applies only the changes of its tables.

```yaml
spec:
  follow:
    enabled: true
    walWorkers: 4          # parallel change streams, 1 to 16
    highTrafficTables:     # each listed table gets a worker of its own
      - app.orders
```

When `walWorkers` is unset or 1, the migration uses one stream.
pgcopydb gives each published table to one worker.
A partitioned table and all its partitions go to the same worker.
Each table in `highTrafficTables` gets a different worker.
When the list has fewer entries than `walWorkers`, no other table shares a worker with a listed table.
pgcopydb puts the other tables on the remaining workers by their write count on the source (`n_tup_ins + n_tup_upd + n_tup_del` in `pg_stat_user_tables`).
[Performance tuning](operations/performance.md#wal-workers) tells when to use the two fields.

With `walWorkers` above 1, these rules apply:

- `follow.enabled` MUST be `true`.
- The plugin MUST be `pgoutput`.
- `spec.follow.mode` MUST be unset or `sqlite`.
- `spec.follow.publication` MUST be empty.
  pgcopydb creates one publication for each table, with the name `<slot>_t<table oid>`.
- `unloggedTables` and `insertOnlyTables` MUST be empty.
- `clone.allDatabases` MUST be `false`.
- A set `slotName` MUST have 51 characters or fewer.
  pgcopydb adds `_w<k>` to the slot name and to the origin name of worker k.
  The operator makes a generated slot name short enough.
- A set `slotName` MUST NOT end in `_w<digits>` or `_t<digits>`.

`highTrafficTables` needs `walWorkers` above 1 and at most `walWorkers` entries.
Each entry MUST be an exact, lowercase `schema.table` name, and each entry MUST be unique.
Preflight makes sure that each listed table exists on the source.

Admission rejects a spec that breaks these rules.
The controller does the same checks when it builds the worker Job, and it fails the Migration with `InvalidSpec`.

Admission also rejects a `slotName` that ends in `_w<digits>` or `_t<digits>` when `walWorkers` is unset or 1.
Thus the cleanup of the WAL workers of one Migration cannot drop the slot of a different Migration that admission accepted with this rule.
A Migration that admission accepted before this rule can have a `slotName` that ends in `_w<digits>` or `_t<digits>`.
If such a Migration uses the same source, the cleanup can drop its slot.
The controller does this check only when `walWorkers` is above 1.
Thus a Migration that admission accepted before this rule continues to run with one stream.
On Kubernetes 1.30 and later, you can also change the other fields of that Migration, for example `cutover.approved`.

Admission and the controller do not do the checks that follow.
pgcopydb does them at the start, and the first attempt fails when one of them is not true:

- The source MUST run PostgreSQL 13 or later, because PostgreSQL 12 cannot publish partitioned tables.
- Each `highTrafficTables` entry MUST match a published table.
- Two entries MUST NOT be in the same partition family (a partitioned table and its partitions).
- Each worker MUST get at least one table.
  Thus `walWorkers` MUST NOT be more than the number of published tables (a partition family counts as one).

Both fields are part of `spec.follow`, so they are immutable.
To change them, create a new Migration.

pgcopydb sets the table-to-worker assignment one time, at the start of the first attempt, and keeps it in its work directory.
A retry that resumes uses the same assignment.
Thus, a table that gets more writes later stays on its worker.

With `walWorkers` above 1, the target has no foreign keys while the workers stream.
pgcopydb creates the foreign keys after every worker reaches the cutover LSN.
This step also checks the rows against each foreign key.
[Cutover with WAL workers](operations/live-migration.md#cutover-with-wal-workers) describes the effect on the cutover window.
[Prerequisites](reference/prerequisites.md#wal-workers) lists the source and target capacity that the workers need.

## Work volume

`spec.workVolume` sizes the PersistentVolumeClaim that holds pgcopydb's dumps and catalogs.
Retries resume from it, except when a live migration's retry [starts over](operations/lifecycle.md#retries-and-resume).

```yaml
spec:
  workVolume:
    size: 50Gi                    # default 10Gi; size it near the database size
    storageClassName: fast-ssd    # empty uses the cluster default
```

For live migrations, the volume also buffers the change stream.
Budget the needs of the clone, plus the write rate times the expected migration window.

### Which node the Jobs prefer

A Migration runs its Jobs one after another, except a [verification rehearsal](operations/verification.md#rehearsal), which runs at the same time as the worker.
Every Job mounts the work volume except the checks that run before an attempt and the rehearsal.
A ReadWriteOnce volume attaches to one node at a time.
A Job pod on a different node must wait until the volume detaches from the previous node.
To prevent this wait, the operator writes the node of the newest Job pod that mounts the work volume to the Migration annotation `pgcopydb-operator.io/work-volume-node`.
Each Job that it creates after that has a preferred node affinity to that node.
The operator adds this preference to `spec.runner.affinity` and keeps `nodeSelector` and `tolerations` as you set them.
The preference is not a requirement: if that node is full or not available, the scheduler uses a different node.

## Runner image

The worker Jobs run the operator-wide runner image from the chart value `runner.image`: pgcopydb 0.18 with PostgreSQL 16 and 18 client tools.
`spec.runner` overrides it per Migration, together with pod placement and resources:

```yaml
spec:
  runner:
    image: ghcr.io/ydixken/pgcopydb-operator/runner:v0.1.0
    resources:
      requests: {cpu: "2", memory: 4Gi}
```

The default image selects PostgreSQL 16 client tools for targets through version 16 and version 18 tools for targets 17 and 18.
The selected `pg_dump`/`pg_restore` must be at least the major version of both source and target.
Preflight checks this before a migration starts.
Targets newer than PostgreSQL 18, or sources newer than the selected client, need a custom `spec.runner.image` with compatible tools.
See [client tool versions](reference/prerequisites.md#client-tool-versions).

If you do not set `resources`, the default is 4 CPUs and 4Gi, as requests only.
Table jobs follow that CPU request unless `clone.tableJobs` sets them.
See [Performance tuning](operations/performance.md).

The runner version also controls the in-pod progress poll that fills `status.progress` and the byte-progress metrics.
The chart value `runner.progressPollVersions` lists the exact pgcopydb versions allowed to run it.
The manager flag for the same list is `--progress-poll-versions`.
The allowlist fails closed: an unlisted version, such as a custom image with stock 0.18, never runs the catalog poll.
Single-database clones keep their psql-based estimates, and all-databases clones report summed database sizes whatever the allowlist holds.
The [monitoring guide](operations/monitoring.md) lists the metrics this affects.

## Credentials

Passwords never appear in the Migration resource, the Job spec, or the logs.
They come from Secrets in the Migration's namespace, and reach pgcopydb through a libpq passfile.
Each endpoint uses exactly one of three forms:

```yaml
spec:
  source:                         # inline form: host + username + password Secret
    host: db.example.com
    database: app
    username: migrator
    passwordSecretRef: {name: app-source, key: password}
    sslMode: require
  target:                         # DSN form: the Secret holds a full libpq URI
    uriSecretRef: {name: rds-target, key: uri}
```

The DSN form fits managed database endpoints such as RDS or Neon that give you a complete connection URI.
[02-clone-dsn-secret.yaml](examples/02-clone-dsn-secret.yaml) shows it with TLS verification against a provider CA bundle.

The third form, `secretRef`, points at one Secret whose keys hold the connection parts, as platform provisioners issue them:

```yaml
spec:
  source:                         # details form: one Secret, one key per part
    secretRef:
      name: clouddb-app           # expects keys DB, PW, URL, URL_EXTERNAL, USER
  target:
    secretRef:
      name: platform-db
      endpoint: external          # take the host from the external URL key
      keys: {database: db, password: pw, urlExternal: host, username: role}
```

`DB` holds either a bare database name or a full libpq URI.
The URI MUST be password-free.
The password comes from the `PW` key, which MUST exist in every layout.
A URI that carries credentials is rejected.
A URI is authoritative for user, host, port, and database name.
It also keeps its own `sslmode` over the one in the spec.
When a URI names no user, the user comes from the `USER` key.

The worker uses the key values literally.
It rejects a value that holds URI syntax, such as `@`, `:`, `/`, or `%`, and names the key in the error.
Put a complete DSN in `uriSecretRef` instead.

With a bare database name, the host comes from `URL` as `host` or `host:port`, and the user comes from `USER`.
The default port is 5432, and `endpoint: external` takes the host from `URL_EXTERNAL`.

`keys` remaps any of the five key names.
If the URI sets no `sslmode`, the spec's `sslMode` applies, and the `tls` file paths always apply.
The password stays a projected file that feeds the passfile, like the other forms.
[03-clone-platform-secret.yaml](examples/03-clone-platform-secret.yaml) is the complete example.

Each side MAY also set `superuserSecretRef`, a Secret in the same convention that names a superuser on the same endpoint.
For single-database migrations, preflight checks that Secret.
For these migrations, preflight also applies the grants that the regular role does not have, for both the base clone and follow.
Each `PreflightRemediated` event lists the statements that one tier applied.
A [dry run](operations/lifecycle.md#dry-run) applies none of them and reports them in `PreflightWouldRemediate` events instead.
See [prerequisites](reference/prerequisites.md#superuser-remediation-superusersecretref) for the contract and [06-live-superuser.yaml](examples/06-live-superuser.yaml) for the example.

### Connection keepalives

The operator adds TCP keepalive settings to every connection URI it composes, which covers the inline and `secretRef` forms:
`keepalives=1`, `keepalives_idle=10`, `keepalives_interval=10`, and `keepalives_count=6`.
Without them, libpq uses the two-hour default of the kernel.
A replaced pod and a torn-down network path are examples of a server that disappears without closing its connections.
Without keepalives, such a server leaves the pgcopydb copy workers waiting on a dead socket for hours.
With them, the read fails after about 70 seconds and the attempt ends with a connection error.
A source outage longer than about 60 to 70 seconds therefore fails the attempt, even when it is only a temporary network partition.
The operator then starts the next attempt, as [Retries and resume](operations/lifecycle.md#retries-and-resume) describes.

Keepalives only probe an idle socket.
When the source disappears while a request to it is still unacknowledged, the kernel keeps retransmitting instead.
It retransmits up to its default limit (`tcp_retries2=15`, about 924 seconds).
An example of such a request is the `COMMIT` that closes the pgcopydb snapshot.
Source URIs therefore also get `tcp_user_timeout=60000`, which fails that connection after 60 seconds of unacknowledged data.
Target URIs deliberately do not get it.
The timeout also counts the time that queued data waits on a zero receive window.
So a live target that stops reading COPY data for a minute would fail the attempt, for example during a storage stall.
`tcp_user_timeout` needs libpq 12 or newer.
The runner image has libpq 18.

Inline connections and `secretRef` connections whose `DB` key is a bare name always get these defaults.
Only a `DB` URI or a `uriSecretRef` value lets you change them.
A `DB` URI keeps each of these keys that it already sets, and the operator adds only the missing keys.

> [!note]
> The operator does not rewrite a `uriSecretRef` value.
> Add the keepalive parameters to that connection string yourself.
> For the source, also add `tcp_user_timeout`.

### Synchronous commit on the target

`spec.target.synchronousCommit: local` makes the copy's target sessions commit with `synchronous_commit=local`, so their commits do not wait for a synchronous standby.
It is for a target that has strict synchronous replication and that you reach through a TCP proxy or load balancer with an idle timeout.
Patroni `synchronous_mode_strict` and CloudNativePG `dataDurability: required` are examples of strict synchronous replication.
A commit that waits for a standby sends nothing, so such a proxy cuts the connection while the backend behind it keeps waiting.

```yaml
spec:
  target:
    host: payroll-pg-rw.payroll.svc
    database: payroll
    username: app
    passwordSecretRef: {name: payroll-pg-app, key: password}
    synchronousCommit: local      # the only value; unset keeps the server's setting
```

The field is unset by default, which leaves `synchronous_commit` to the server.
When you set it, the operator adds the libpq startup option `options=-c%20synchronous_commit%3Dlocal` to the target URI of three Jobs.
These Jobs are the worker, the preflight, and the probe of the retry gate.
pgcopydb passes the option to every target connection that it opens, including pg_restore ([measured](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/measurements.md#a-startup-option-reaches-every-pgcopydb-target-connection)).
The source and every other Job, such as cleanup, verification, and the ownership handover, connect without it.

CDC apply still waits for a standby.
It sets `synchronous_commit=on` after every `BEGIN`, and a session `SET` has priority over the startup option.
We keep apply that way because apply reports replay progress to the source after each commit.
The source never resends a change after apply reports it.
So an apply commit lost in a target failover would lose those changes for good.

`synchronousCommit` is the only target field you can change after the Migration exists.
A change applies from the next attempt, because a running worker keeps the URI it started with.

Admission rejects the field on the source.
It also rejects the field together with `uriSecretRef`, because the operator never reads that Secret and cannot add the option to it.
To use the field, a `uriSecretRef` target has to move to `secretRef` or to the inline fields.
Because the rest of the target is immutable, that move takes a new Migration.
With `secretRef`, a `DB` URI that sets its own `options` is refused, because the operator's option would replace it.
The Jobs that carry the option then exit with `target connection secret: the DB URI sets options, which spec.target.synchronousCommit would replace; remove options from the DB URI or unset synchronousCommit`, so the preflight fails before any data moves.

A connection pooler can refuse the startup option or drop it silently, and the preflight fails either way.
An unconfigured PgBouncer refuses the connection, and the preflight's connectivity failure says so.
A pooler that ignores the parameter, as a CloudNativePG Pooler does, connects without it.
The preflight then reads `show synchronous_commit` over the worker's own target URI and fails on any value but `local`.
The retry gate repeats that check before every retry and holds the retry with reason `SyncCommitNotLocal` while it fails.
We support the field on targets that you reach directly or through a TCP proxy, not through a connection pooler.
The preflight fails a target behind PgBouncer in any pool mode, with or without the field.
See [preflight](operations/live-migration.md#preflight).

> [!warning]
> A target failover during the copy can lose recently copied data on the new primary.
> The next attempt resumes and trusts what pgcopydb recorded as done.
> So, instead of a resume, start the copy again from the beginning.
> To do this, create a new Migration with `spec.clone.dropIfExists: true`.
> The bulk COPY and the index builds already commit with `synchronous_commit=off` today.
> This field makes pg_restore, VACUUM, index builds after a reconnect, and the other copy statements commit with `local`.

Apply still waits for a standby.
When the standby acknowledges the first apply commit, that also acknowledges every earlier local commit.
We infer that from how synchronous replication works.
A standby acknowledges a WAL position, and every earlier commit lies below that position.
The sequence reset at cutover commits locally too.
A clone without follow has no waiting commit after the copy, unless you set `clone.ownerAfterRestore`.
In that case, the ownership handover commits with the setting of the server.
Either way, check `pg_stat_replication` on the target primary before you switch applications.
Its standbys must be streaming and caught up.
