# Running the E2E suite against your databases

The E2E suite normally creates its own CloudNativePG source and target.
In external mode, the suite runs from your own Kubernetes cluster against a database pair that you already have.
It seeds its fixtures into your source, migrates them with an operator that it installs, and checks the result on your target.
It also deploys an OpenTelemetry Collector in the operator namespace, and the operator exports its metrics to it.
The collector image comes from `ghcr.io`.
This run shows that the operator works with your servers, network, and roles.
It does not tell you anything about your own data.

> [!caution]
> External mode is destructive on both databases.
> It seeds and writes the source.
> On the target, it drops and recreates the `public` and `audit` schemas.
> It revokes and restores the replication rights of the app role.
> It creates and drops roles: `e2e_noselect` on the source, `e2e_limited` and `app-owner` on the target.
> Use it only with a dedicated, disposable pair that nothing else uses.
> Never use it with a server that another pgcopydb migration reads from.

## Prerequisites

- A kubectl context for the cluster, and `task`, `go`, and `helm` on your machine.
- The `Migration` CRD installed at the schema of the checkout that you run from.
  The suite installs the chart with `crds.install=false`.
  When the served CRD lacks a field, the suite fails with an error that names every missing field.
- Rights to create the namespaces `pgcopydb-e2e`, `pgcopydb-e2e-x`, and `pgcopydb-e2e-system`.
  As an alternative: `E2E_MANAGE_NAMESPACES=false`, with those namespaces and the manager ServiceAccount that their owner provides (see the [contributor guide](https://github.com/ydixken/pgcopydb-operator/blob/main/CONTRIBUTING.md#e2e-tests)).
- Network access from pods in those namespaces to both servers.
- Image pulls from `ghcr.io` in the cluster.
  The operator and runner images are under `ghcr.io/ydixken/pgcopydb-operator`.
  The client pod and the seed Job use `ghcr.io/cloudnative-pg/postgresql:18`.
  The collector uses `ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector:0.162.0`.
- One database on each server, with no user objects in it on the first run.
- On each side, an app role that can log in, owns its database, and is not a superuser.
  The migrations run as this role.
  The seed creates schemas and the `citext` extension as this role.
  On the target, the role also needs `TEMPORARY` on its database for the test commit of the [retry gate](lifecycle.md#the-retry-gate).
  The owner of the database has this right, unless someone revoked it.
- On each side, a separate admin role that is a true superuser (`rolsuper`).
  The app role is not a member of this admin role.
  As this role, the suite seeds and resets, runs `SET SESSION AUTHORIZATION`, and creates and drops roles.
  It also grants and revokes `REPLICATION`, `EXECUTE` on `pg_catalog` functions, and `SET` on `session_replication_role`.
  In addition, it comments on a database that the app role owns, and it drops replication slots and origins.
  A managed-service admin role that is not a superuser, such as a member of `rds_superuser`, `cloudsqlsuperuser`, or `azure_pg_admin`, lacks some of these rights.
  With such a role, the run fails partway.
- On the source, `wal_level=logical` and room for one more replication slot and WAL sender.
  The suite runs one follow migration at a time.
  External mode runs as one Ginkgo process and refuses `ginkgo --procs` above 1, because it has one database pair.
- A target on PostgreSQL 17 or later, not older than the source.
  The source may be PostgreSQL 14 or later.
  The runner's `pg_restore` 18 cannot restore into PostgreSQL 16 or older ([Client tool versions](../reference/prerequisites.md#client-tool-versions)).
- The `citext` and `hstore` extensions available on both servers (PostgreSQL contrib).
- An `sslmode` of `disable`, `allow`, `prefer`, or `require`.
  `verify-ca` and `verify-full` are not supported, because the suite cannot pass a CA certificate.
- A host name or an IPv4 address in each URI.
  IPv6 literal hosts are not supported, because the operator writes the inline connection's `host:port` without brackets.

## Environment variables

When you set any of these variables, the suite switches to external mode.
Then the suite needs all eight.
When one is missing, the suite fails at start and names each missing variable.
Unset them before you run a CloudNativePG task such as `task e2e`.
Otherwise, that task also runs in external mode.

- `E2E_SOURCE_URI` is the app connection, `postgresql://<role>@<host>:<port>/<database>`.
  The role and the database names come from it.
- `E2E_SOURCE_PASSWORD` is that role's password.
- `E2E_SOURCE_ADMIN_URI` is the admin connection.
  It names the same database, or no database.
- `E2E_SOURCE_ADMIN_PASSWORD` is the admin role's password.
- `E2E_TARGET_URI`, `E2E_TARGET_PASSWORD`, `E2E_TARGET_ADMIN_URI`, and `E2E_TARGET_ADMIN_PASSWORD` are the same four for the target.

The suite checks the values before it touches anything.
Each error names the variable, and it never quotes a URI, a password, or a query parameter.
The suite stops with an error when:

- A URI carries a password, or any query parameter other than `sslmode`.
  The inline connection that the suite gives each `Migration` can carry only `sslmode`.
- An admin URI names another host, port, or `sslmode` than its app URI, another database, or the app role itself.
- The source and target URIs name the same database.
- The variables give one role on one server two different passwords, or a password holds a line break.
- A URI names an IPv6 literal host.
- `E2E_RUN_LABEL_VALUE` is set, because a run with that label skips the teardown that drops the slots and the password Secrets.

The suite stores the passwords in the Secret `e2e-external-credentials` in `pgcopydb-e2e`.
It gives them to psql through a pgpass file in its client pod `e2e-psql`, never through argv.

`task e2e:external` sets `E2E_SCALE`, which sizes the fixtures, to 0.1 unless you pass `SCALE=` (see the examples below).
`E2E_STORAGE_CLASS` picks the StorageClass of the work volume, and `E2E_OPERATOR_TAG` picks the operator build, as in a normal run.
The suite reads the major versions from the servers, so it rejects `E2E_PG_SOURCE` and `E2E_PG_TARGET` in external mode.
In external mode, `E2E_CNPG_INSTANCES` has no effect.

## Running it

1. Check the cluster that you are about to use.

    ```sh
    kubectl config current-context
    ```

2. Export the connections.
   Then read the passwords without echoing them.
   These commands are for bash or zsh.

    ```sh
    export E2E_SOURCE_URI='postgresql://shop_app@source.example.com:5432/shop?sslmode=require'
    export E2E_SOURCE_ADMIN_URI='postgresql://shop_admin@source.example.com:5432/shop?sslmode=require'
    export E2E_TARGET_URI='postgresql://shop_app@target.example.com:5432/shop?sslmode=require'
    export E2E_TARGET_ADMIN_URI='postgresql://shop_admin@target.example.com:5432/shop?sslmode=require'
    for v in E2E_SOURCE_PASSWORD E2E_SOURCE_ADMIN_PASSWORD E2E_TARGET_PASSWORD E2E_TARGET_ADMIN_PASSWORD; do
      printf '%s: ' "$v"; read -rs value; echo; export "$v=$value"
    done; unset value
    ```

3. Run the suite.
   The prompt shows the context and both `host:port/database` pairs, never a password.

    ```sh
    task e2e:external
    ```

The `e2e:external` tasks run only the Ginkgo suite, not the unit tests that pin the CloudNativePG fixtures.
The tasks pass the context that you accepted to the suite as `E2E_CONTEXT`.
A `go test` run without this variable skips the suite instead of reaching a cluster.

Check out these **examples**:

- Run the specs whose names match a regex.

    ```sh
    task e2e:external FOCUS='completes a fresh clone'
    ```

- Seed larger fixtures.

    ```sh
    task e2e:external SCALE=0.25
    ```

- Run from a script that cannot answer the prompt.
  `EXPECT_CONTEXT` must name the current context, so the run cannot reach another cluster.

    ```sh
    task e2e:external:unattended EXPECT_CONTEXT=my-cluster
    ```

> [!warning]
> Never answer the prompt with `task --yes`.
> That flag would answer every prompt, including a prompt for the wrong cluster.

## First and later runs

On the first run, both databases must hold no user objects:

- no schema other than `public`
- no table, view, sequence, function, or type outside the system schemas
- no extension other than `plpgsql`
- no large object

The suite checks both databases first.
Then it stamps each with the database comment `pgcopydb-e2e: disposable, the e2e suite may wipe this database` before it seeds or resets anything.
A later run treats a database the same way.
It stamps a database that is still empty.
It refuses a database that holds objects but lacks the stamp, and the error lists up to ten of the objects that it found.
When the seed on the source has another scale or profile than the run asks for, the suite seeds the source again.
Before it seeds again, it drops the fixture objects on the source.

The roles that the suite creates (`e2e_noselect` on the source, `e2e_limited` and `app-owner` on the target) carry the same text as a role comment.
The specs drop these roles with everything that they own.
So the suite drops only a stamped role.
It also refuses to start when a role with one of these names exists on its server without the stamp.

> [!important]
> A URI that points at the wrong, populated database stops the run instead of wiping it.
> Do not create the stamp by hand to bypass that check.

## What is skipped

Every spec runs, except the specs below.
Each of them skips with its reason in the report:

- Fixture placement (both specs): they read the CloudNativePG instance pods.
- Keepalive feedback (Manual and Automatic) and early Manual cutover: they pause the source WAL sender with a signal sent inside the database pod.
- Progress sampler transaction pooling: it creates its own CloudNativePG clusters and a Pooler.
- `synchronousCommit` on a direct target: it holds its table lock through a psql session inside the source's database pod.
- `synchronousCommit` through a transaction-mode Pooler: it creates its own CloudNativePG cluster and a Pooler.
- Synchronous replication on the target (both specs): they create their own two-instance CloudNativePG cluster and fence its standby.
- All databases (both specs): the first spec creates and drops databases, sets the `postgres` password, and creates an extension in `postgres` on both servers.
  Both specs connect to the `postgres` database.
- All databases resume (both specs): they set the `postgres` password on both servers, create a schema in the source's `postgres` database, and connect to the `postgres` database.
  The clone copies that schema to the target.
  In the first spec, the clone also copies a role that the spec creates on the source.
- Large object resume: it locks `pg_largeobject` on the target, which needs a superuser.
- Chaos source-kill and target-kill: they delete the primary pod.
- Chaos fan-out: it creates and drops a second database on the target server.
- Feature coverage: it creates the `cov_reader` role, which is server-wide, on both servers.
- Follow correctness (all three specs): they compare content through `cov_reader`, and they signal the source WAL sender inside the database pod.
- Connection details from a single Secret, when an app role or database name holds anything but letters, digits, `-`, `.`, `_`, or `~`.
  The operator's secretRef form rejects the percent-encoded user that such a name needs.
  This spec also carries the schema and data compare after a plain clone, so that compare skips with it.
  The other specs, including extension ownership, run with names that need quoting.

These skips are runtime checks rather than labels, so a run started without the task's label filter skips them too.
The task also excludes every spec labeled `chaos` or `flaky`, as the release-candidate run does.
The metrics specs skip unless `E2E_PROMETHEUS_URL` or `E2E_PROMETHEUS_PORT_FORWARD` is set.

Release candidates run the suite against CloudNativePG fixtures only.
CI covers the code of external mode itself with unit tests.

## Cleanup

At the end, the suite drops the replication slots and origins that its Migrations left.
It also drops the publications of pgcopydb in the source database.
It does this even with `E2E_KEEP_FIXTURES=true`, so that nothing holds WAL on your source.
It recognizes its own slots and origins by the `pgcopydb_pgcopydb_e2e_` prefix that its namespaces give them.
On the source, it looks only at the slots of the test database, so other slots on your servers stay.
Every teardown step runs, even when an earlier step fails.
For example, a step fails when a leftover Migration does not disappear, or when a slot is still there afterwards.
The run fails after the last step, so the operator and the Secrets below are gone either way.
The suite leaves the fixtures and the stamp in both databases, so the next run reuses the seed.

The Secrets hold your passwords, so the suite deletes them on every run, even with `E2E_KEEP_FIXTURES=true`.
Each spec that stores your admin password in a Secret deletes that Secret when it ends.
The other objects that the suite deletes from the cluster depend on two variables:

- By default it deletes `pgcopydb-e2e` and `pgcopydb-e2e-x`, and every Secret that holds your passwords goes with them.
- With `E2E_MANAGE_NAMESPACES=false`, it deletes the client pod, the seed Job, and every Secret whose name starts with `e2e-` in both namespaces.
  This set includes every Secret that the suite wrote.
- With `E2E_KEEP_FIXTURES=true` it keeps everything else in those namespaces and still deletes every `e2e-` Secret.

> [!warning]
> A run that is killed before its teardown can leave a pgcopydb replication slot on the source.
> That slot holds WAL until someone drops it.
> The run can also leave the login roles `e2e_noselect` on the source and `e2e_limited` on the target.
> The passwords of these two roles are in the source code of this repository.
> The run can also leave the role `app-owner` on the target.
> In the cluster, it can leave the `e2e-` Secrets that hold your passwords in `pgcopydb-e2e` and `pgcopydb-e2e-x`.
> These Secrets include `e2e-external-credentials`.
> After any interrupted run, check both servers as the admin role.
> Then check both namespaces.

On the source, in the test database:

```sql
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
WHERE database = current_database() AND slot_name LIKE 'pgcopydb\_pgcopydb\_e2e\_%' AND NOT active;
DO $$ BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'e2e_noselect'
             AND shobj_description(oid, 'pg_authid') = 'pgcopydb-e2e: disposable, the e2e suite may wipe this database') THEN
    EXECUTE 'DROP ROLE e2e_noselect';
  END IF;
END $$;
```

On the target, in the test database, with `shop_app` replaced by your target app role:

```sql
SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin
WHERE roname LIKE 'pgcopydb\_pgcopydb\_e2e\_%';
DO $$ BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'e2e_limited'
             AND shobj_description(oid, 'pg_authid') = 'pgcopydb-e2e: disposable, the e2e suite may wipe this database') THEN
    EXECUTE 'DROP OWNED BY e2e_limited CASCADE';
    EXECUTE 'DROP ROLE e2e_limited';
  END IF;
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'app-owner'
             AND shobj_description(oid, 'pg_authid') = 'pgcopydb-e2e: disposable, the e2e suite may wipe this database') THEN
    EXECUTE 'REASSIGN OWNED BY "app-owner" TO shop_app';
    EXECUTE 'DROP OWNED BY "app-owner"';
    EXECUTE 'DROP ROLE "app-owner"';
  END IF;
END $$;
```

When you no longer need the pair, drop both databases.
The suite leaves its fixtures and the stamp in them.
Two grants live outside the databases, and they stay after the drop.
The source app role keeps the `REPLICATION` attribute, and the target app role keeps `SET` on `session_replication_role`.
A `TEMPORARY` grant on the target database goes with the database.
To revoke the two grants, run this on the source:

```sql
ALTER ROLE shop_app NOREPLICATION;
```

Then run this on the target:

```sql
REVOKE SET ON PARAMETER session_replication_role FROM shop_app;
```
