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
For inline and `secretRef` connections, the worker writes passfile entries as `host:port:*:user:password`, so one set of credentials authenticates every database on that endpoint.

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

The worker Jobs run the operator-wide runner image from the chart value `runner.image`: pgcopydb 0.18 with PostgreSQL 18 client tools.
`spec.runner` overrides it per Migration, together with pod placement and resources:

```yaml
spec:
  runner:
    image: ghcr.io/ydixken/pgcopydb-operator/runner:v0.1.0
    resources:
      requests: {cpu: "2", memory: 4Gi}
```

The `pg_restore` 18 of the default image cannot restore into PostgreSQL 16 or older.
Such a target needs a `spec.runner.image` whose `pg_restore` is version 16 or older.
The version of `pg_dump` in the image must be at least the major version of the target.
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

### Native helper image

The chart value `runner.rpgmigrateImage` sets `--rpgmigrate-runner-image` only when it is nonempty.
The flag defaults to `RPGMIGRATE_RUNNER_IMAGE`, which you can set through `extraEnv` in the chart.
An explicit flag takes priority over the environment, including an explicit empty value.
The default is empty.
Without a per-Migration image override, the empty default leaves native helpers unavailable.
The native image never defaults to `runner.image` or `--runner-image`.
`spec.runner.image` still overrides the image per Migration.
We preserve image values exactly, without trimming or normalization.
Native worker Jobs set `LC_ALL=C` and `LANG=C`.

> [!important]
> This setting does not enable the native backend.
> `spec.engine: rpgmigrate` still fails with `UnsupportedEngine` before the controller creates resources or executes commands.

A native helper image MUST use an immutable, canonical single-platform manifest reference with `@sha256:` and 64 lowercase hexadecimal characters.
A tag, image index digest, or image configuration digest does not satisfy this requirement.
The configured reference MUST match the original Pod's raw `imageID` exactly under the qualified runtime profile.
Image syntax alone does not prove the manifest type or runtime identity.
The profile requires accepted archive/import identity and packaged Python/libpq validation.
It also requires file/TLS/passfile, resolver, checked transport, process limits, and owned PVC validation.
Native activation also requires the worker report, identical-end-position acknowledgment, exact owned cleanup, and full lifecycle validation.
These requirements remain separate from configuration and Reader wiring.

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

Each passfile line names the exact host and port the connection uses, so a source and a target on one host and user but different ports keep their own passwords.
A URI without a port resolves to `PGPORT`, then 5432, the way libpq does.
The worker rejects, by name, an endpoint it cannot bind to one line: IPv6 literals, several hosts, a missing host, percent-encoding in the host or a query key, and `host`, `port`, or `service` in the URI query.
The inline `host` follows the same rule and accepts only a DNS name or an IPv4 address.
`uriSecretRef` carries its own credentials and has none of these limits, unless the side also sets `superuserSecretRef`.

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
