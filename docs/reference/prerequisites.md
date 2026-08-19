# Prerequisites

What a `Migration` needs from your PostgreSQL endpoints and your Kubernetes cluster before the operator can run it. The keywords MUST, SHOULD, and MAY are to be interpreted as described in RFC 2119.

Scope: the v0.1 surface, base clone (`pgcopydb clone`), live migration (`clone --follow`), cutover, and cleanup. Ground truth for the pgcopydb behavior behind each rule: [pgcopydb-cli.md](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/pgcopydb-cli.md) and [pgcopydb-follow.md](https://github.com/ydixken/pgcopydb-operator/blob/main/docs/research/pgcopydb-follow.md). The e2e fixtures ([test/e2e](https://github.com/ydixken/pgcopydb-operator/tree/main/test/e2e)) apply exactly the grants below.

## Summary

| Requirement                                             | Where  | Needed for       |
|---------------------------------------------------------|--------|------------------|
| SELECT on copied tables and sequences, USAGE on schemas | source | every migration  |
| CREATE on the database, ownership alignment             | target | every migration  |
| `wal_level = logical`                                   | source | follow           |
| Free replication slot and WAL sender                    | source | follow           |
| `REPLICATION` role attribute                            | source | follow           |
| CREATE on the database plus table ownership (publication) | source | follow         |
| EXECUTE on `pg_replication_origin_*` functions          | target | follow           |
| Primary key or replica identity on replicated tables    | source | follow           |

## Kubernetes

- The `migrations.pgcopydb-operator.io` CRD MUST be installed (chart `crds.install=true`, or `config/crd`).
- Credentials MUST live in Secrets in the Migration's namespace: a password Secret for the inline connection form, a full libpq URI Secret for `uriSecretRef`, or one details Secret for `secretRef`. TLS client material, if used, likewise.
- A StorageClass MUST be able to provision the work-volume PVC (`spec.workVolume`); it is the unit of resumability.
- Runner pods MUST be able to reach both endpoints on their PostgreSQL port. Both endpoints are plain libpq targets; they do not have to run on Kubernetes.

## Client tool versions

The runner image bundles pgcopydb and the PostgreSQL client tools. `pg_dump`/`pg_restore` MUST be at least the target server's major version. The default runner image ships pgcopydb 0.18 with PostgreSQL 18 client tools; for a newer target major, set `spec.runner.image` to an image with matching tools.

## Base clone (every Migration)

Source role:

- SELECT on every table and sequence being copied and USAGE on their schemas. Owning the objects covers all of it.
- Snapshot export needs no special privilege; plain clones need no replication privilege at all.

Target role:

- CREATE on the target database.
- Ownership alignment: without `clone.noOwner`, `pg_restore` emits `ALTER OWNER`, which only works as superuser or as the owning role. The simplest non-superuser setup is the pattern the e2e fixtures use: the migration connects as the role that owns every migrated object on both sides. Otherwise set `clone.noOwner: true`.
- `ALTER DATABASE ... SET` (the db-properties step) requires database ownership or superuser. If the target role has neither, add `dbProperties` to `clone.skip`.

Superuser is required only for:

- `clone.roles: true` without `clone.noRolePasswords: true` (reads passwords from `pg_authid`).
- Extensions: creating most C extensions on the target, and cloning a database whose superuser-installed extensions have configuration tables (a pg_dump limitation that filters cannot exclude).

## Live migration (`spec.follow.enabled: true`)

The operator preflights the requirements of this section before the first attempt: a `<name>-preflight` Job checks `wal_level`, free-slot headroom, the source role's REPLICATION attribute, EXECUTE on the origin functions, the `session_replication_role` SET privilege, and audits every user table for a usable replica identity. A failed check fails the Migration with the exact missing GRANT, setting, or table list in the `Validated` condition message, before any data moves. The rest of the workload contract (no DDL, large objects, wal2json presence) is not preflighted; checking it stays your job.

Source instance:

- `wal_level` MUST be `logical` (changing it requires a server restart).
- One free replication slot per running live Migration (`max_replication_slots`) and one free WAL sender (`max_wal_senders`).
- A replication slot retains WAL until it is dropped. Budget disk for the migration window, and mind `max_slot_wal_keep_size`: it caps retention, but a slot invalidated by the cap kills the migration. The operator drops the slot at cutover, on abort, and on Migration deletion.
- `wal_sender_timeout` SHOULD be at its PostgreSQL default (60s) or higher. Aggressive values terminate pgcopydb's logical-decoding walsender whenever a status update is a few seconds late; CloudNativePG sets 5s by default for its own HA streaming, so CNPG sources SHOULD override it in `spec.postgresql.parameters` for the migration window.

Source role:

- MUST have the `REPLICATION` attribute (or be superuser): `ALTER ROLE app REPLICATION`. Without it, slot creation fails with "permission denied to start WAL sender". On CloudNativePG sources this is declarative: a role listed in the Cluster's `managed.roles` with `replication: true` reconciles to the attribute (verified live). CNPG does not manage its bootstrap owner role by default, so either declare that role under `managed.roles` or run the `ALTER ROLE` once.
- Publication: pgcopydb auto-creates a publication for the migrated tables (named after the slot) and drops it during cleanup. The role MUST have CREATE on the source database and own every published table. Alternatively, pre-create a publication (superuser is needed for `FOR ALL TABLES`) and point `spec.follow.publication` at it; pgcopydb then leaves it alone. On a retry after a crashed attempt, the operator drops a leftover auto-managed publication before resuming (pgcopydb would otherwise fail on its own leftover); a publication named in `spec.follow.publication` is never touched.
- Plugin: `pgoutput` (default) and `test_decoding` ship with PostgreSQL; `wal2json` MUST be installed on the source before selecting it. The preflight cannot verify that: a logical decoding plugin is a bare shared library with no catalog entry to query, and the only positive probe (creating a slot with it) is too invasive for a check. A missing plugin fails the first attempt at slot creation with `could not access file "wal2json"`.

Target role:

- MUST be able to `SET session_replication_role` (the apply session runs with it set to `replica` to keep triggers and foreign keys quiet during replay). Superuser can always; on PostgreSQL 15+ grant it explicitly: `GRANT SET ON PARAMETER session_replication_role TO app;`. WARNING: with pgcopydb 0.18, a role without this privilege does not fail the migration; apply silently replays nothing while reporting success, and only a row count comparison exposes the loss. Grant it before every live migration.
- MUST have EXECUTE on the `pg_replication_origin_*` catalog functions (superuser has it implicitly). Non-superuser grant, as used by the e2e fixtures:

```sql
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
```

  Missing grants surface in a confusing place: the very first runner attempt dies in pgcopydb's setup cleanup with "permission denied for function pg_replication_origin_drop", before any data moves.

Schema and workload contract:

- Every table that receives UPDATE or DELETE during the migration window MUST have a primary key or a replica identity (`REPLICA IDENTITY USING INDEX ...` or `REPLICA IDENTITY FULL`). With `pgoutput`, DML on a published table without one fails on the source at write time, breaking the application, not just the migration. The preflight audits all user tables for this and fails on offenders; it deliberately ignores `clone.filters`, since a filtered table can still take writes. Tables that are read-only or insert-only during the window MAY be acknowledged in `spec.follow.allowMissingReplicaIdentity` (schema-qualified names exactly as the preflight prints them; `["*"]` acknowledges every offender), which downgrades them to a warning.
- DDL is not replicated and MUST NOT run during the migration window; pre-create upcoming partitions before starting.
- Large-object changes during the window are not replicated (base copy only). Sequences need no handling: pgcopydb re-syncs them automatically after cutover.

## Retries and snapshot consistency

The operator retries a failed attempt with `pgcopydb clone --resume --not-consistent` on the same work volume: finished tables are skipped, interrupted tables are re-copied from scratch (each table's COPY is a single transaction, so a killed attempt leaves no partial rows). `--not-consistent` is not optional here: the first attempt's exported snapshot dies with its session, and a plain `--resume` fails before touching any data ("snapshot ... does not exist").

The trade-off: re-copied tables read a fresh snapshot. A retried clone of a source that keeps taking writes is therefore not one single point in time across tables. If that matters, stop writes across the retry window, or delete and recreate the Migration for a fresh consistent copy. Follow migrations replay every change since the slot's consistent point on top of the base copy, and the cutover drain-verify gate plus `spec.verification.data` are the checks that catch divergence.

## Cutover

Cutover freezes the stream at the source's current LSN and drains it; anything written after that point does not reach the target. Writes to the source MUST be stopped before `spec.cutover.approved: true` in Manual mode, and before the lag drops under `follow.maxCatchupLag` in Automatic mode. Automatic is for sources that are already quiesced.
