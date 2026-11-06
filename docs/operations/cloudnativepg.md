# Migrating into a CloudNativePG cluster

[CloudNativePG](https://cloudnative-pg.io/) (CNPG) provisions and runs the target.
The `Migration` moves the data into the target.
The e2e suite migrates between live CNPG clusters in the shape that this page shows.

The recipe assumes a CNPG `Cluster` with the name `shop-pg` in the namespace `shop`.
The `app` role owns the default `app` database of this cluster.
Replace these names with your names.

## 1. Target the `-rw` Service

CNPG maintains a `<cluster>-rw` Service.
This Service always routes to the current primary and follows failovers.
Use this Service as the target host, never a pod name:

```yaml
target:
  host: shop-pg-rw.shop.svc  # the CNPG read-write Service: always the primary
  database: app
  username: app
```

## 2. Reuse the app Secret CNPG generated

The `initdb` bootstrap creates a Secret with the name `<cluster>-app`.
This Secret holds the credentials of the owner role.
Reference this Secret directly, not a second copy of the password:

```yaml
target:
  # ...
  passwordSecretRef:
    name: shop-pg-app  # created by CNPG at bootstrap
    key: password
```

The Migration MUST be in the same namespace as that Secret.

## 3. Check ownership and grant the target prerequisites

CNPG's [`postInitApplicationSQL`](https://cloudnative-pg.io/docs/1.30/bootstrap#executing-queries-after-initialization) runs as `postgres` in the application database.
The extensions that it creates there belong to `postgres`.
They do not belong to the `app` role, which owns the database.
Even a plain clone with `spec.clone: {}` can fail when it restores the comments on those extensions.

> [!warning]
> Without `dropIfExists`, set `clone.skip: [extensionComments]`.
> This setting keeps the target extensions that an administrator owns, and it keeps the application comments.
> `clone.noComments: true` also prevents this failure, but it suppresses all comments.
> With `dropIfExists: true`, comment suppression does not prevent ownership failures on `DROP EXTENSION`.
> In that case, use `clone.skip: [extensions]` or a migration role with the privileges of the owner.
> `superuserSecretRef` does not remediate extension ownership.
> See [Prerequisites](../reference/prerequisites.md#base-clone-every-migration).

A live migration (`spec.follow.enabled: true`) needs two more grants on the target that only a superuser can give.
In the instance pod, peer auth lets you connect without a superuser password.
Run the grants one time through that pod:

```sh
kubectl exec -it -n shop shop-pg-1 -c postgres -- psql -U postgres app
```

```sql
-- Let app manage replication origins (pgcopydb tracks apply progress with them).
DO $$
DECLARE f oid;
BEGIN
  FOR f IN
    SELECT p.oid FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'pg_catalog' AND p.proname LIKE 'pg_replication_origin%'
  LOOP
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO app', f::regprocedure);
  END LOOP;
END $$;

-- Let the apply session mute triggers and FKs during replay (PostgreSQL 15+).
GRANT SET ON PARAMETER session_replication_role TO app;
```

The second grant is the dangerous one to skip.
Without it, pgcopydb 0.18 reports success but applies nothing.
The [preflight](live-migration.md#preflight) probes both grants before any data moves.

To skip the manual step, set `spec.target.superuserSecretRef` to a Secret with superuser credentials.
CNPG creates `<cluster>-superuser` when `enableSuperuserAccess` is on.
The preflight then applies exactly these grants itself and records a `PreflightRemediated` event.
A [dry run](lifecycle.md#dry-run) applies none of them and reports them in `PreflightWouldRemediate` events instead.
See the [prerequisites](../reference/prerequisites.md#superuser-remediation-superusersecretref).

## 4. CNPG as the source

When the source is also a CNPG cluster, two additions to the source `Cluster` resource are important:

```yaml
spec:
  managed:
    roles:
      - name: app
        login: true
        replication: true  # reconciles to ALTER ROLE app REPLICATION
  postgresql:
    parameters:
      wal_sender_timeout: 60s  # CNPG defaults to 5s, too short for pgcopydb's receive
```

- `managed.roles`: by default, CNPG does not manage its bootstrap owner role, so the role needs this entry.
  Without this entry, run `ALTER ROLE app REPLICATION` one time by hand.
- `wal_sender_timeout`: for the migration window, raise the CNPG default of 5s to the PostgreSQL default of 60s or more.
  The [source instance prerequisites](../reference/prerequisites.md#live-migration-specfollowenabled-true) explain why 5s is too short.
  They also explain how a source URI can raise the value for the connections of the migration only.

> [!warning]
> When a CNPG source stays at 5s, the follow stream of a live migration can stall.
> The preflight does not block on this value.
> It passes and emits a `PreflightWarning` event that names the value.

## 5. The Migration

```yaml
apiVersion: pgcopydb-operator.io/v1beta1
kind: Migration
metadata:
  name: shop
  namespace: shop  # same namespace as the CNPG cluster and its app Secret
spec:
  source:
    host: shop-db.old-datacenter.example.com  # any libpq target: DBaaS, VM, another operator
    database: shop
    username: migrator  # needs REPLICATION for follow; see prerequisites
    passwordSecretRef: {name: shop-source, key: password}
    sslMode: require
  target:
    host: shop-pg-rw.shop.svc
    database: app
    username: app
    passwordSecretRef: {name: shop-pg-app, key: password}
  follow:
    enabled: true  # drop this block for a one-shot clone
  cutover:
    mode: Manual  # stop writes to the source, then set approved: true
```

From this point, the [live-migration runbook](live-migration.md) applies without change.
After `CutoverCompleted` is True, connect the application to `shop-pg-rw.shop.svc`.

CNPG has its own `initdb.import`, which also moves data into a new cluster.
For a small, offline copy from CNPG to CNPG, `initdb.import` is simpler.
Use this operator when the source is elsewhere, when the database needs a parallel copy, or when the application cannot stop.
