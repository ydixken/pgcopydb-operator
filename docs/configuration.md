# Configuration

The knobs you reach for after the first clone works. Every field, with defaults and validation, is in the [CRD reference](reference/api.md); complete commented resources live in [examples/](https://github.com/ydixken/pgcopydb-operator/tree/main/docs/examples).

## Clone tuning

Tuning (parallelism, same-table splitting, skips) is the `clone` block; see [migration-tuned.yaml](examples/migration-tuned.yaml) for the full commented version:

```yaml
spec:
  clone:
    tableJobs: 8                 # concurrent table COPY workers
    indexJobs: 8                 # concurrent CREATE INDEX workers
    splitTablesLargerThan: 2Gi   # tables above this copy in parallel parts
    skip: [largeObjects]         # steps to omit entirely
```

The defaults are pgcopydb's own; raise the job counts to the parallelism your source can serve. Every `pgcopydb clone` flag maps to a spec field or a recorded exclusion; the [option coverage table](reference/coverage.md) is the map.

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

The DSN form fits DBaaS endpoints (RDS, Neon, ...) that hand you a complete connection URI; [migration-dsn-secret.yaml](examples/migration-dsn-secret.yaml) shows it together with TLS verification against a provider CA bundle.

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
[migration-details-secret.yaml](examples/migration-details-secret.yaml) is the complete example.
