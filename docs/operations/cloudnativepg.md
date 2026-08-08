# Migrating into a CloudNativePG cluster

[CloudNativePG](https://cloudnative-pg.io/) (CNPG) provisions and runs the target; the `Migration` moves the data in. This is the operator's best-tested path: the e2e suite migrates between live CNPG clusters in exactly the shape on this page.

The recipe assumes a CNPG `Cluster` named `shop-pg` in namespace `shop`, bootstrapped with the default `app` database owned by the `app` role. Substitute your names.

## 1. Target the `-rw` Service

CNPG maintains a `<cluster>-rw` Service that always routes to the current primary and follows failovers. Use it as the target host, never a pod name:

```yaml
target:
  host: shop-pg-rw.shop.svc  # the CNPG read-write Service: always the primary
  database: app
  username: app
```

## 2. Reuse the app Secret CNPG generated

The `initdb` bootstrap creates a Secret named `<cluster>-app` with the owner role's credentials. Reference it directly instead of maintaining a second copy of the password:

```yaml
target:
  # ...
  passwordSecretRef:
    name: shop-pg-app  # created by CNPG at bootstrap
    key: password
```

The Migration MUST live in the same namespace as that Secret.

## 3. Grant the target prerequisites (live migrations only)

A plain clone connecting as `app` needs nothing more: the role owns the `app` database and everything restored into it, which covers the ownership rules in the [prerequisites](../reference/prerequisites.md). A live migration (`spec.follow.enabled: true`) needs two more grants on the target that only a superuser can give. Run them once through the instance pod (peer auth, no superuser password needed):

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

Skipping the second grant is the dangerous one: pgcopydb 0.18 reports success while applying nothing. The [preflight](live-migration.md#preflight) probes both before any data moves.

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

- `managed.roles` with `replication: true` gives the role the `REPLICATION` attribute declaratively. CNPG does not manage its bootstrap owner role by default, so listing it here starts managing it; alternatively run `ALTER ROLE app REPLICATION` once by hand.
- `wal_sender_timeout` at CNPG's 5s default terminates pgcopydb's logical-decoding walsender whenever a status update arrives a few seconds late, burning attempts during catchup and drain. Raise it to the PostgreSQL default (60s) or more for the migration window; see [troubleshooting](../troubleshooting.md).

## 5. The Migration

```yaml
apiVersion: pgcopydb-operator.io/v1alpha1
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

CNPG's own `initdb.import` also moves data into a new cluster and is the simpler tool for a small, offline CNPG-to-CNPG copy. This operator earns its keep when the source is elsewhere, the database is large (parallel copy), or the application cannot stop for the duration: see the [comparison](../design/comparison.md).

The e2e suite runs this exact shape on every release: CNPG source and target clusters, the `app` role and Secret, the grants above, and `wal_sender_timeout: 60s` on the source.
