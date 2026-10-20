# Configuration

Every field, with defaults and validation, is in the [CRD reference](reference/api.md); complete commented resources live in the [examples index](examples.md).

## Clone tuning

The `clone` block holds parallelism, same-table splitting, and skips.
See [04-clone-tuned.yaml](examples/04-clone-tuned.yaml) for the full commented version:

```yaml
spec:
  clone:
    tableJobs: 8                 # concurrent table COPY workers
    indexJobs: 8                 # concurrent CREATE INDEX workers
    splitTablesLargerThan: 2Gi   # tables above this copy in parallel parts
    skip: [largeObjects]         # steps to omit entirely
```

The operator defaults table jobs to the worker's CPU request and turns on same-table splitting and binary COPY; other unset options use pgcopydb's defaults.
Size job counts against both endpoints; see [Performance tuning](operations/performance.md).
Every `pgcopydb clone` flag maps to a spec field or a recorded exclusion; the [option coverage table](reference/coverage.md) lists them.

### Extension comments and skips

`clone.skip: [extensionComments]` suppresses only extension comments.
The restore keeps table and other object comments.
`clone.noComments: true` suppresses all restored comments.
Both options avoid the extension-comment ownership check that an otherwise empty `spec.clone: {}` can hit.
Neither avoids the check that `dropIfExists: true` adds for `DROP EXTENSION`.
`clone.skip: [extensions]` skips extension restoration, including extension comments, and bypasses both the availability and ownership preflight checks.
If you skip extensions, provide the extensions that the application needs on the target yourself.
See [Prerequisites](reference/prerequisites.md#base-clone-every-migration) for the ownership rules and remedies.

### Ownership after restore

`clone.ownerAfterRestore` names the role that owns the restored objects once the migration finishes:

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
Give the role name unquoted, as PostgreSQL stores it: the operator quotes it, so hyphenated names such as `db-admin` work.
`ownerAfterRestore` needs `noOwner: true`; the CRD rejects the field on its own.
Otherwise `pg_restore` assigns the source owners, and the handover covers only the part of the schema that landed on the migration role.
The CRD rejects `clone.allDatabases` too, because the handover runs only in the target connection's database.

The handover runs as a `<name>-reown` Job after the worker exits.
On a plain clone it runs in `Finalizing`, before the verification compares, so nothing reads the target while ownership changes.
On a live migration it runs in `CuttingOver`, after the drain is proven and before `CutoverCompleted` turns True.
That condition is the signal to point applications at the target, so the handover time counts against the cutover window.

You can re-run the Job safely.
Each `ALTER` commits on its own, so many partitions cannot push one transaction past `max_locks_per_transaction`.
The field is immutable after the Migration exists: preflight probes the role before the first attempt, and the operator builds the Job once.

See [Ownership after restore](reference/prerequisites.md#ownership-after-restore-cloneownerafterrestore) for the object classes and privileges.
See [Ownership handover failures](troubleshooting.md#ownership-handover-failures) to recover a failed handover.

## All databases

Set `spec.clone.allDatabases: true` to clone the whole source instance with superuser credentials on both sides.

Both connections MUST name an existing maintenance database, such as `postgres`.
pgcopydb substitutes each database name into the connection URIs itself and creates missing target databases.
[The all-databases contract](reference/prerequisites.md#all-databases) gives the superuser rules for each side and the preflight checks.
See [09-all-databases.yaml](examples/09-all-databases.yaml) for a complete resource.

> [!warning]
> Target databases MUST NOT already hold the schema being restored; `pg_restore` errors on existing objects.
> This includes objects in the target's `postgres` database.
> Admission rejects `clone.dropIfExists`, `follow.enabled`, and `verification.data` with `allDatabases`.

Admission rejects those three options for these reasons:

- pgcopydb ignores follow in this mode.
- `dropIfExists` would drop the connected maintenance database.
- `verification.data` gets no JSON verdict from the data compare.

If admission does not enforce these rules, the controller rejects the same combinations with terminal reason `InvalidSpec` before it creates any Jobs.

pgcopydb copies roles unconditionally, so `clone.roles: true` is allowed but redundant.
`clone.noRolePasswords` omits the role passwords, and the superuser contract still applies.
Filters, skip options, and other clone options apply to every database.
Job counts are global across databases, not multiplied per database.
Restart and resume use per-database work directories beneath the existing work directory.

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

Table names follow PostgreSQL quoting and `~/regex/` patterns work.
`clone.filters` exposes all eight pgcopydb filter sections; the [CRD reference](reference/api.md) lists them.

## Work volume

`spec.workVolume` sizes the PersistentVolumeClaim that holds pgcopydb's dumps and catalogs.
Retries resume from it, except when a live migration's retry [starts over](operations/lifecycle.md#retries-and-resume).

```yaml
spec:
  workVolume:
    size: 50Gi                    # default 10Gi; size it near the database size
    storageClassName: fast-ssd    # empty uses the cluster default
```

For live migrations the volume also buffers the change stream.
Budget the clone's needs plus write rate times the expected migration window.

### Which node the Jobs prefer

A Migration runs its Jobs one after another, and every Job except the checks that run before an attempt mounts the work volume.
A ReadWriteOnce volume attaches to one node at a time.
A Job pod on a different node must wait until the volume detaches from the previous node.
To prevent this wait, the operator writes the node of the newest Job pod that mounts the work volume to the Migration annotation `pgcopydb-operator.io/work-volume-node`.
Each Job that it creates after that has a preferred node affinity to that node.
The operator adds this preference to `spec.runner.affinity` and keeps `nodeSelector` and `tolerations` as you set them.
The preference is not a requirement: if that node is full or not available, the scheduler uses a different node.

## Runner image

The worker Jobs run the operator-wide runner image from the chart value `runner.image`: pgcopydb 0.18 with PostgreSQL 18 client tools.
`spec.runner` overrides it per Migration, along with pod placement and resources:

```yaml
spec:
  runner:
    image: ghcr.io/ydixken/pgcopydb-operator/runner:v0.1.0
    resources:
      requests: {cpu: "2", memory: 4Gi}
```

`pg_dump` in the image must be at least the target's major version; see [client tool versions](reference/prerequisites.md#client-tool-versions).

If you leave `resources` unset, it defaults to 4 CPUs and 4Gi, requests only.
Table jobs follow that CPU request unless `clone.tableJobs` sets them; see [Performance tuning](operations/performance.md).

The runner version also controls the in-pod progress poll that fills `status.progress` and the byte-progress metrics.
The chart value `runner.progressPollVersions` lists the exact pgcopydb versions allowed to run it, and the manager flag is `--progress-poll-versions`.
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
The URI MUST be password-free: the password comes from the `PW` key, which MUST exist in every layout, and a URI carrying credentials is rejected.
A URI is authoritative for user, host, port, and database name, and it keeps its own `sslmode` over the spec's.
A URI that names no user falls back to the `USER` key.

The worker uses the key values literally.
It rejects a value that holds URI syntax, such as `@`, `:`, `/`, or `%`, and names the key in the error.
Put a complete DSN in `uriSecretRef` instead.

With a bare database name, the host comes from `URL` as `host` or `host:port`, and the user from `USER`.
The default port is 5432, and `endpoint: external` takes the host from `URL_EXTERNAL`.

`keys` remaps any of the five key names.
If the URI sets no `sslmode`, the spec's `sslMode` applies, and the `tls` file paths always apply.
The password stays a projected file that feeds the passfile, like the other forms.
[03-clone-platform-secret.yaml](examples/03-clone-platform-secret.yaml) is the complete example.

Each side MAY additionally set `superuserSecretRef`, a Secret in the same convention that names a superuser on the same endpoint.
For single-database migrations, preflight checks that Secret and applies the grants the regular role is missing, for the base clone and for follow alike.
Each `PreflightRemediated` event lists the statements that one tier applied.
A [dry run](operations/lifecycle.md#dry-run) applies none of them and reports them in `PreflightWouldRemediate` events instead.
See [prerequisites](reference/prerequisites.md#superuser-remediation-superusersecretref) for the contract and [06-live-superuser.yaml](examples/06-live-superuser.yaml) for the example.

### Connection keepalives

The operator adds TCP keepalive settings to every connection URI it composes, which covers the inline and `secretRef` forms:
`keepalives=1`, `keepalives_idle=10`, `keepalives_interval=10`, and `keepalives_count=6`.
Without them libpq falls back to the kernel's two-hour default.
A server that disappears without closing its connections, such as a replaced pod or a torn-down network path, then leaves the pgcopydb copy workers waiting on a dead socket for hours.
With them, the read fails after about 70 seconds and the attempt ends with a connection error.
A source outage longer than about 60 to 70 seconds, even a temporary network partition, therefore fails the attempt, and the operator starts the next one as [Retries and resume](operations/lifecycle.md#retries-and-resume) describes.

Keepalives only probe an idle socket.
When the source vanishes while a request to it is still unacknowledged, such as the `COMMIT` that closes pgcopydb's snapshot, the kernel keeps retransmitting up to its default limit instead (`tcp_retries2=15`, about 924 seconds).
Source URIs therefore also get `tcp_user_timeout=60000`, which fails that connection after 60 seconds of unacknowledged data.
Target URIs deliberately do not get it.
The timeout also counts the time queued data waits on a zero receive window, so a live target that stops reading COPY data for a minute, during a storage stall for example, would fail the attempt.
`tcp_user_timeout` needs libpq 12 or newer; the runner image ships libpq 18.

Inline connections and `secretRef` connections whose `DB` key is a bare name always get these defaults.
Only a `DB` URI or a `uriSecretRef` value lets you change them: a `DB` URI keeps any of these keys it already sets, and the operator fills in only the missing ones.

> [!note]
> The operator does not rewrite a `uriSecretRef` value.
> Add the keepalive parameters, and `tcp_user_timeout` on the source, to that connection string yourself.

### Synchronous commit on the target

`spec.target.synchronousCommit: local` makes the copy's target sessions commit with `synchronous_commit=local`, so their commits do not wait for a synchronous standby.
It is meant for a target with strict synchronous replication, such as Patroni `synchronous_mode_strict` or CloudNativePG `dataDurability: required`, reached through a TCP proxy or load balancer with an idle timeout.
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
When it is set, the operator adds the libpq startup option `options=-c%20synchronous_commit%3Dlocal` to the target URI of three Jobs: the worker, the preflight, and the retry gate's probe.
pgcopydb passes the option on to every target connection it opens, pg_restore included ([measured](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/measurements.md#a-startup-option-reaches-every-pgcopydb-target-connection)).
The source and every other Job, such as cleanup, verification, and the ownership handover, connect without it.

CDC apply still waits for a standby.
It sets `synchronous_commit=on` after every `BEGIN`, and a session `SET` outranks the startup option.
We keep apply that way because it reports replay progress to the source after each commit, and the source never resends what it has confirmed: an apply commit lost in a target failover would lose those changes for good.

`synchronousCommit` is the only target field you can change after the Migration exists.
A change applies from the next attempt, because a running worker keeps the URI it started with.

Admission rejects the field on the source.
It also rejects the field together with `uriSecretRef`, because the operator never reads that Secret and cannot add the option to it.
To use the field, a `uriSecretRef` target has to move to `secretRef` or the inline fields, and because the rest of the target is immutable, that takes a new Migration.
With `secretRef`, a `DB` URI that sets its own `options` is refused, because the operator's option would replace it.
The Jobs that carry the option then exit with `target connection secret: the DB URI sets options, which spec.target.synchronousCommit would replace; remove options from the DB URI or unset synchronousCommit`, so the preflight fails before any data moves.

A connection pooler can refuse the startup option or drop it silently, and the preflight fails either way.
An unconfigured PgBouncer refuses the connection, and the preflight's connectivity failure says so.
A pooler that ignores the parameter, as a CloudNativePG Pooler does, connects without it.
The preflight then reads `show synchronous_commit` over the worker's own target URI and fails on any value but `local`.
The retry gate repeats that check before every retry and holds the retry with reason `SyncCommitNotLocal` while it fails.
The field is supported on direct connections and session-mode poolers only.
Through a transaction-mode pooler, one check samples one server connection and cannot vouch for the ones the worker gets later.

> [!warning]
> A target failover during the copy can lose recently copied data on the new primary.
> The next attempt resumes and trusts what pgcopydb recorded as done, so start the copy over instead: create a new Migration with `spec.clone.dropIfExists: true`.
> The bulk COPY and the index builds already commit with `synchronous_commit=off` today.
> This field makes pg_restore, VACUUM, index builds after a reconnect, and the other copy statements commit with `local`.

Apply still waits for a standby, and its first confirmed commit also confirms every earlier local commit.
We infer that from how synchronous replication works: a standby confirms a WAL position, and every earlier commit lies below it.
The sequence reset at cutover commits locally too.
A clone without follow has no waiting commit after the copy, unless `clone.ownerAfterRestore` is set: the ownership handover commits with the server's setting.
Either way, confirm in `pg_stat_replication` on the target primary that its standbys are streaming and caught up before you switch applications.
