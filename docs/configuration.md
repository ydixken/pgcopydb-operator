# Configuration

The knobs you reach for after the first clone works. Every field, with defaults and validation, is in the [CRD reference](reference/api.md); complete commented resources live in the [examples index](examples.md).

## Clone tuning

Tuning (parallelism, same-table splitting, skips) is the `clone` block; see [04-clone-tuned.yaml](examples/04-clone-tuned.yaml) for the full commented version:

```yaml
spec:
  clone:
    tableJobs: 8                 # concurrent table COPY workers
    indexJobs: 8                 # concurrent CREATE INDEX workers
    splitTablesLargerThan: 2Gi   # tables above this copy in parallel parts
    skip: [largeObjects]         # steps to omit entirely
```

The operator defaults table jobs to the worker's CPU request and enables same-table splitting; other unset options use pgcopydb's defaults.
Size job counts against both endpoints; see [Performance tuning](operations/performance.md).
Every `pgcopydb clone` flag maps to a spec field or a recorded exclusion; the [option coverage table](reference/coverage.md) is the map.

### Extension comments and skips

`clone.skip: [extensionComments]` suppresses only extension comments; table and other object comments are still restored.
`clone.noComments: true` suppresses all restored comments.
Both avoid the extension-comment ownership requirement when `dropIfExists` is false, including for an otherwise empty `spec.clone: {}`.
Neither avoids the ownership requirement for `DROP EXTENSION` with `dropIfExists: true`.
`clone.skip: [extensions]` skips extension restoration, including extension comments, and bypasses both the availability and ownership preflight gates.
When skipping extensions, provide the extensions required by the application on the target yourself.
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

Reach for it when the migration cannot connect as the role the objects have to end up under.
That is the usual shape on managed PostgreSQL, where the admin role the provider hands out is not the application's owning role and creating one that owns both sides is not an option.
`noOwner: true` is required alongside it, and the CRD rejects the pair without it: `pg_restore` would otherwise assign the source owners, and the handover would cover only the part of the schema that happened to land on the migration role.
`clone.allDatabases` is rejected too, because the handover runs in the target connection's database only.

The handover runs as a `<name>-reown` Job after the worker has exited.
On a plain clone it runs in `Finalizing`, before the verification compares, so nothing reads the target while ownership is still moving.
On a live migration it is in `CuttingOver`, after the drain is proven and before `CutoverCompleted` turns True.
That condition is the signal to point applications at the target, so the handover sits inside the cutover window and its duration is part of that window.

Re-running is safe.
The Job derives its statement list from current ownership each time, so a rerun selects only what is left, and each `ALTER` commits on its own rather than in one transaction that many partitions could push past `max_locks_per_transaction`.
The field is immutable once the Migration exists, because preflight probes the role before the first attempt and the Job is built once.

The handover transfers schemas, relations (tables, partitions, sequences, views, materialized views, foreign tables), routines (functions, procedures, aggregates), and types including domains.
Extension members keep their owner, and the classes outside those four, large objects and publications among them, are left alone.
A sequence attached to a column by `serial` or `IDENTITY` changes owner with its table, so it needs no statement of its own.

See [Ownership after restore](reference/prerequisites.md#ownership-after-restore-cloneownerafterrestore) for the privileges it needs and [Ownership handover failures](troubleshooting.md#ownership-handover-failures) for recovering one that failed.

## All databases

Set `spec.clone.allDatabases: true` to clone the whole source instance using superuser credentials on both sides.

Both connections MUST name an existing maintenance database, such as `postgres`.
pgcopydb substitutes each database name into the connection URIs itself and creates missing target databases.
It excludes only `template0` and `template1`: the source's `postgres` database is cloned into the target's `postgres` database too.
See [09-all-databases.yaml](examples/09-all-databases.yaml) for a complete resource.

> [!warning]
> Target databases MUST NOT already hold the schema being restored; `pg_restore` errors on existing objects.
> This includes objects in the target's `postgres` database.
> Admission rejects `clone.dropIfExists`, `follow.enabled`, and `verification.data` with `allDatabases`.

If admission does not enforce these rules, the controller rejects the same combinations with terminal reason `InvalidSpec` before creating any Jobs.

The source superuser covers the role dump's access to `pg_authid` and dumps across every database.
The target superuser covers database creation, role restore, and ownership changes.
Managed admin roles without `rolsuper`, such as `rds_superuser`, fail preflight; `superuserSecretRef` does not replace the migration connections' own superuser requirement.
All-databases preflight ignores `superuserSecretRef` and does not mount its credentials, because this mode cannot remediate missing privileges.

Roles are copied unconditionally, so `clone.roles: true` is allowed but redundant.
Roles already present on the target are skipped.
`clone.noRolePasswords` omits role passwords without relaxing the superuser contract.
Filters, skip options, and other clone options apply to every database, and job counts are global across databases rather than multiplied per database.
Restart and resume use per-database work directories beneath the existing work directory.

`verification.schema` is supported across all databases.
Follow is rejected because pgcopydb ignores it in this mode; `dropIfExists` would try to drop the connected maintenance database, and the data compare produces no JSON verdict.
Progress sampling reports summed database sizes on each side, without per-database relation counts or pgcopydb counters.
`uriSecretRef` accepts both PostgreSQL URIs and libpq keyword/value DSNs for this mode.
pgcopydb parses either form with `PQconninfoParse`, replaces the database name, and renders a per-database URI while preserving connection options, including passwords, TLS settings, and `passfile`.
For inline and `secretRef` connections, the runner writes passfile entries as `host:*:*:user:password`; the wildcard database field lets the same credentials authenticate every per-database connection.

## Filters

`clone.filters` selects what to copy; the operator renders it to pgcopydb's filters INI in an operator-owned ConfigMap:

```yaml
spec:
  clone:
    filters:
      excludeSchemas: ["audit", "scratch"]
      excludeTableData: ["public.event_log"]  # schema yes, rows no
```

Table names follow PostgreSQL quoting and `~/regex/` patterns work. All eight pgcopydb filter sections are exposed; the [CRD reference](reference/api.md) lists them.

## Work volume

`spec.workVolume` sizes the PVC that holds pgcopydb's dumps and catalogs. It is the unit of resumability: retries resume from it.

```yaml
spec:
  workVolume:
    size: 50Gi                    # default 10Gi; size it near the database size
    storageClassName: fast-ssd    # empty uses the cluster default
```

For live migrations the volume also buffers the change stream, so budget the clone's needs plus write rate times the expected migration window.

## Runner image

The worker Jobs run the operator-wide runner image (chart value `runner.image`, pgcopydb 0.18 with PostgreSQL 18 client tools). `spec.runner` overrides it per Migration, along with pod placement and resources:

```yaml
spec:
  runner:
    image: ghcr.io/ydixken/pgcopydb-operator/runner:v0.1.0
    resources:
      requests: {cpu: "2", memory: 4Gi}
```

`pg_dump` in the image must be at least the target's major version; see [client tool versions](reference/prerequisites.md#client-tool-versions).

Left unset, `resources` defaults to 4 CPUs and 4Gi, requests only.
The copy concurrency follows that request, so raising it is usually the only tuning a migration needs; see [Performance tuning](operations/performance.md).

The runner version also gates the in-pod progress poll that fills `status.progress` and the byte-progress metrics.
The chart value `runner.progressPollVersions` (manager flag `--progress-poll-versions`) lists the exact pgcopydb versions allowed to run it, and it fails closed: an unlisted version, such as a custom stock 0.18 image, never runs the catalog poll.
Single-database clones retain their psql-based estimates; all-databases clones report only summed database sizes regardless of the allowlist.
The [monitoring guide](operations/monitoring.md) lists which metrics that gate affects.

## Credentials

Passwords never appear in the CR, Job spec, or logs; they come from Secrets in the Migration's namespace and reach pgcopydb through a libpq passfile. Three forms, mutually exclusive per endpoint:

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

The DSN form fits DBaaS endpoints (RDS, Neon, ...) that hand you a complete connection URI; [02-clone-dsn-secret.yaml](examples/02-clone-dsn-secret.yaml) shows it together with TLS verification against a provider CA bundle.

The third form, `secretRef`, points at one Secret whose keys hold the connection parts, the way platform provisioners hand them out:

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
A URI is authoritative for user, host, port, and database name, and keeps its own `sslmode` over the spec's; a URI that names no user falls back to the `USER` key.
Values are used literally: anything containing URI syntax (`@`, `:`, `/`, `%`, and the like) is rejected by name, and a complete DSN belongs in `uriSecretRef` instead.
With a bare name, the host comes from `URL` (or `URL_EXTERNAL` under `endpoint: external`) as `host` or `host:port` with 5432 as the default port, and the user from `USER`.
`keys` remaps any of the five key names; `sslMode` fills the gap when the URI sets none, and the `tls` file paths always apply.
The password stays a projected file feeding the passfile, with the same guarantee as the other forms.
[03-clone-platform-secret.yaml](examples/03-clone-platform-secret.yaml) is the complete example.

Each side MAY additionally set `superuserSecretRef`, a Secret in the same convention naming a superuser on the same endpoint.
For single-database migrations, the preflight checks it and applies the grants the regular role is missing, for the base clone (`GRANT CREATE` on the target database and schemas) and for follow alike.
Each `PreflightRemediated` event lists the statements applied by one tier; [prerequisites](reference/prerequisites.md#superuser-remediation-superusersecretref) has the contract and [06-live-superuser.yaml](examples/06-live-superuser.yaml) the example.
