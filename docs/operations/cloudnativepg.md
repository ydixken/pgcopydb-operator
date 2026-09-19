# Migrating into a CloudNativePG cluster

[CloudNativePG](https://cloudnative-pg.io/) (CNPG) provisions and runs the target; the `Migration` moves the data in.
This is the operator's best-tested path: the e2e suite migrates between live CNPG clusters in exactly the shape on this page.

The recipe assumes a CNPG `Cluster` named `shop-pg` in namespace `shop`, bootstrapped with the default `app` database owned by the `app` role.
Substitute your names.

## 1. Target the `-rw` Service

CNPG maintains a `<cluster>-rw` Service that always routes to the current primary and follows failovers.
Use it as the target host, never a pod name:

```yaml
target:
  host: shop-pg-rw.shop.svc  # the CNPG read-write Service: always the primary
  database: app
  username: app
```

## 2. Reuse the app Secret CNPG generated

The `initdb` bootstrap creates a Secret named `<cluster>-app` with the owner role's credentials.
Reference it directly instead of maintaining a second copy of the password:

```yaml
target:
  # ...
  passwordSecretRef:
    name: shop-pg-app  # created by CNPG at bootstrap
    key: password
```

The Migration MUST live in the same namespace as that Secret.

## 3. Check ownership and grant the target prerequisites

CNPG's [`postInitApplicationSQL`](https://cloudnative-pg.io/docs/1.30/bootstrap#executing-queries-after-initialization) runs as `postgres` against the application database, so extensions created there belong to `postgres`, not to the `app` role that owns the database.
Even a plain clone with `spec.clone: {}` can fail when restoring comments on those extensions.

> [!warning]
> Without `dropIfExists`, set `clone.skip: [extensionComments]` to retain administrator-owned target extensions and keep application comments.
> `clone.noComments: true` also avoids this failure but suppresses all comments.
> With `dropIfExists: true`, comment suppression does not prevent ownership failures on `DROP EXTENSION`: use `clone.skip: [extensions]` or a migration role with the owner's privileges.
> `superuserSecretRef` does not remediate extension ownership; see [Prerequisites](../reference/prerequisites.md#base-clone-every-migration).

A live migration (`spec.follow.enabled: true`) needs two more grants on the target that only a superuser can give.
Run them once through the instance pod (peer auth, no superuser password needed):

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

The second grant is the dangerous one to skip: pgcopydb 0.18 reports success while applying nothing.
The [preflight](live-migration.md#preflight) probes both before any data moves.

The manual step has an alternative: set `spec.target.superuserSecretRef` to a Secret carrying superuser credentials (CNPG creates `<cluster>-superuser` when `enableSuperuserAccess` is on), and the preflight applies exactly these grants itself, logging them in a `PreflightRemediated` event; see the [prerequisites](../reference/prerequisites.md#superuser-remediation-superusersecretref).

## 4. CNPG as the source

When the source is also a CNPG cluster, two additions to the source `Cluster` resource matter:

```yaml
spec:
  managed:
    roles:
      - name: app
        login: true
        replication: true  # reconciles to ALTER ROLE app REPLICATION
  postgresql:
    parameters:
      wal_sender_timeout: 60s  # CNPG defaults to 5s, which kills logical walsenders
```

- `managed.roles` with `replication: true` gives the role the `REPLICATION` attribute declaratively.
  CNPG does not manage its bootstrap owner role by default, so listing it here starts managing it; alternatively run `ALTER ROLE app REPLICATION` once by hand.
- `wal_sender_timeout` at CNPG's 5s default kills pgcopydb's logical-decoding walsender.
  Raise it to the PostgreSQL default (60s) or more for the migration window; the [source instance prerequisites](../reference/prerequisites.md#live-migration-specfollowenabled-true) explain why 5s is too short.

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

From here the [live-migration runbook](live-migration.md) applies unchanged: watch `CaughtUp`, stop writes, approve the cutover, and point the application at `shop-pg-rw.shop.svc` once `CutoverCompleted` is True.

CNPG's own `initdb.import` also moves data into a new cluster and is the simpler tool for a small, offline CNPG-to-CNPG copy.
This operator is the better choice when the source is elsewhere, the database is large (parallel copy), or the application cannot stop for the duration.

The e2e suite runs this exact shape on every release: CNPG source and target clusters, the `app` role and Secret, the grants above, and `wal_sender_timeout: 60s` on the source.
