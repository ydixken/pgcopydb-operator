# Running the E2E suite against your databases

The E2E suite normally creates its own CloudNativePG source and target.
In external mode it runs against a database pair you already have, from your own Kubernetes cluster.
It seeds its fixtures into your source, migrates them with an operator it installs, and checks the result on your target.
That shows the operator works with your servers, network, and roles; it says nothing about your own data.

> [!caution]
> External mode is destructive on both databases.
> It seeds and writes the source, drops and recreates the `public` and `audit` schemas on the target, and revokes and restores the app role's replication rights.
> It creates and drops roles: `e2e_noselect` on the source, `e2e_limited` and `app-owner` on the target.
> Point it only at a dedicated, disposable pair that nothing else uses, and never at a server another pgcopydb migration reads from.

## Prerequisites

- A kubectl context for the cluster, and `task`, `go`, and `helm` on your machine.
- The `Migration` CRD installed at the schema of the checkout you run from.
  The suite installs the chart with `crds.install=false` and fails naming every field the served CRD lacks.
- Rights to create the namespaces `pgcopydb-e2e`, `pgcopydb-e2e-x`, and `pgcopydb-e2e-system`, or `E2E_MANAGE_NAMESPACES=false` with those namespaces and the manager ServiceAccount provided by their owner (see the [contributor guide](https://github.com/ydixken/pgcopydb-operator/blob/main/CONTRIBUTING.md#e2e-tests)).
- Network access from pods in those namespaces to both servers.
- One database on each server, holding no user objects on the first run.
- On each side, an app role that can log in, owns its database, and is not a superuser.
  The migrations run as this role, and the seed creates schemas and the `citext` extension as it.
- On each side, a separate admin role that is a true superuser (`rolsuper`), of which the app role is not a member.
  As this role the suite seeds and resets, runs `SET SESSION AUTHORIZATION`, creates and drops roles, and grants and revokes `REPLICATION`, `EXECUTE` on `pg_catalog` functions, and `SET` on `session_replication_role`.
  It also comments on a database the app role owns, and drops replication slots and origins.
  Managed-service admin roles that are not superusers, such as members of `rds_superuser`, `cloudsqlsuperuser`, or `azure_pg_admin`, lack some of these rights, and the run fails partway.
- On the source, `wal_level=logical` and room for one more replication slot and WAL sender.
  The suite runs one follow migration at a time.
- A target on PostgreSQL 15 or later, not older than the source; the source may be 14 or later.
- The `citext` and `hstore` extensions available on both servers (PostgreSQL contrib).
- An `sslmode` of `disable`, `allow`, `prefer`, or `require`.
  `verify-ca` and `verify-full` are not supported, because the suite cannot pass a CA certificate.
- A host name or an IPv4 address in each URI.
  IPv6 literal hosts are not supported, because the operator writes the inline connection's `host:port` without brackets.

## Environment variables

Setting any of these switches the suite to external mode, and then all eight are required; the suite fails at start naming any that are missing.
Unset them before you run a CloudNativePG task such as `task e2e`, which would otherwise run in external mode too.

- `E2E_SOURCE_URI` is the app connection, `postgresql://<role>@<host>:<port>/<database>`.
  The role and the database names come from it.
- `E2E_SOURCE_PASSWORD` is that role's password.
- `E2E_SOURCE_ADMIN_URI` is the admin connection, naming the same database or none.
- `E2E_SOURCE_ADMIN_PASSWORD` is the admin role's password.
- `E2E_TARGET_URI`, `E2E_TARGET_PASSWORD`, `E2E_TARGET_ADMIN_URI`, and `E2E_TARGET_ADMIN_PASSWORD` are the same four for the target.

The suite checks the values before it touches anything and stops with an error naming the variable, never its value, when:

- a URI carries a password, or any query parameter other than `sslmode`, which is all the inline connection the suite gives each `Migration` can carry;
- an admin URI names another host, port, or `sslmode` than its app URI, another database, or the app role itself;
- the source and target URIs name the same database;
- one role on one server is given two different passwords, or a password holds a line break;
- a URI names an IPv6 literal host.

The suite stores the passwords in the Secret `e2e-external-credentials` in `pgcopydb-e2e` and hands them to psql through a pgpass file in its client pod `e2e-psql`, never through argv.

`E2E_SCALE` sizes the fixtures (the task defaults it to 0.1), `E2E_STORAGE_CLASS` picks the work volume's StorageClass, and `E2E_OPERATOR_TAG` picks the operator build, as in a normal run.
The suite reads the server majors from the servers, so it rejects `E2E_PG_SOURCE` and `E2E_PG_TARGET` in external mode, and `E2E_CNPG_INSTANCES` has no effect.

## Running it

1. Confirm the cluster you are about to use.

    ```sh
    kubectl config current-context
    ```

2. Export the connections, then read the passwords without echoing them (bash or zsh).

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

Check out these **examples**:

- Run the specs whose names match a regex.

    ```sh
    task e2e:external FOCUS='completes a fresh clone'
    ```

- Seed larger fixtures.

    ```sh
    task e2e:external SCALE=0.25
    ```

- Run from a script that cannot answer the prompt; `EXPECT_CONTEXT` must name the current context, so the run cannot reach another cluster.

    ```sh
    task e2e:external:unattended EXPECT_CONTEXT=my-cluster
    ```

> [!warning]
> Never answer the prompt with `task --yes`: it would answer every prompt, including one for the wrong cluster.

## First and later runs

On the first run both databases must hold no user objects: no schema besides `public`, no table, view, sequence, function, or type outside the system schemas, no extension besides `plpgsql`, and no large object.
The suite checks both databases first, then stamps each with the database comment `pgcopydb-e2e: disposable, the e2e suite may wipe this database` before it seeds or resets anything.
Every later run refuses to touch a database that lacks the stamp, and the error lists up to ten of the objects it found.
When the source's seed has another scale or profile than the run asks for, the suite drops the fixture objects on the source and seeds it again.

> [!important]
> A URI that points at the wrong, populated database stops the run instead of wiping it.
> Do not create the stamp by hand to get past that check.

## What is skipped

Every spec runs except these, each of which skips with its reason in the report:

- Fixture placement (both specs): they read the CloudNativePG instance pods.
- Keepalive feedback (Manual and Automatic) and early Manual cutover: they pause the source WAL sender with a signal sent inside the database pod.
- Progress sampler transaction pooling: it creates its own CloudNativePG clusters and a Pooler.
- All databases (both specs): they create and drop databases server-wide and connect to the `postgres` database.
- Chaos source-kill and target-kill: they delete the primary pod.
- Chaos fan-out: it creates and drops a second database on the target server.
- Connection details from a single Secret, when an app role or database name holds anything but letters, digits, `-`, `.`, `_`, or `~`: the operator's secretRef form rejects the percent-encoded user such a name needs.
  The other specs, including extension ownership, run with names that need quoting.

These skips are runtime checks rather than labels, so a run started without the task's label filter skips them too.
The task also excludes every spec labeled `chaos` or `flaky`, as the release-candidate run does.
The metrics specs skip unless `E2E_PROMETHEUS_URL` or `E2E_PROMETHEUS_PORT_FORWARD` is set.

Release candidates run the suite against CloudNativePG fixtures only; CI covers external mode's own code with unit tests.

## Cleanup

At the end the suite drops the replication slots and origins its Migrations left and pgcopydb's publications in the source database, even with `E2E_KEEP_FIXTURES=true`, so nothing holds WAL on your source.
It recognizes its own slots and origins by the `pgcopydb_pgcopydb_e2e_` prefix its namespaces give them, and on the source it looks only at the test database's slots, so other slots on your servers stay.
The run fails if a slot is still there afterwards, once it has removed the operator and the Secrets below.
It leaves the fixtures and the stamp in both databases, so the next run reuses the seed.

The Secrets hold your passwords, so the suite deletes them on every run, even with `E2E_KEEP_FIXTURES=true`.
Each spec that stores your admin password in a Secret deletes that Secret when it ends.
What else the suite removes from the cluster depends on two variables:

- By default it deletes `pgcopydb-e2e` and `pgcopydb-e2e-x`, and every Secret holding your passwords goes with them.
- With `E2E_MANAGE_NAMESPACES=false` it deletes the client pod, the seed Job, and every Secret whose name starts with `e2e-` in both namespaces, which covers every Secret it wrote.
- With `E2E_KEEP_FIXTURES=true` it keeps everything else in those namespaces and still deletes every `e2e-` Secret.

> [!warning]
> A run that is killed before its teardown can leave a pgcopydb replication slot on the source, and that slot holds WAL until someone drops it.
> It can also leave the login roles `e2e_noselect` on the source and `e2e_limited` on the target, whose passwords are in this repository's source, and the role `app-owner` on the target.
> Check both servers as the admin role after any interrupted run.

On the source, in the test database:

```sql
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
WHERE database = current_database() AND slot_name LIKE 'pgcopydb\_pgcopydb\_e2e\_%' AND NOT active;
DROP ROLE IF EXISTS e2e_noselect;
```

On the target, in the test database, with `shop_app` replaced by your target app role:

```sql
SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin
WHERE roname LIKE 'pgcopydb\_pgcopydb\_e2e\_%';
DO $$ BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'e2e_limited') THEN
    EXECUTE 'DROP OWNED BY e2e_limited CASCADE';
    EXECUTE 'DROP ROLE e2e_limited';
  END IF;
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'app-owner') THEN
    EXECUTE 'REASSIGN OWNED BY "app-owner" TO shop_app';
    EXECUTE 'DROP OWNED BY "app-owner"';
    EXECUTE 'DROP ROLE "app-owner"';
  END IF;
END $$;
```

When you are done with the pair, drop both databases: the suite leaves its fixtures and the stamp in them.
Two grants live outside the databases and survive that: the source app role keeps the `REPLICATION` attribute, and the target app role keeps `SET` on `session_replication_role`.
To remove them, run this on the source:

```sql
ALTER ROLE shop_app NOREPLICATION;
```

and this on the target:

```sql
REVOKE SET ON PARAMETER session_replication_role FROM shop_app;
```
