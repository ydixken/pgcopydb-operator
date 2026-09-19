# Prerequisites

What a `Migration` needs from your PostgreSQL endpoints and your Kubernetes cluster before the operator can run it.
The keywords MUST, SHOULD, and MAY are to be interpreted as described in RFC 2119.

Scope: base clone (`pgcopydb clone`), whole-instance clone (`clone --all-databases`), live migration (`clone --follow`), cutover, and cleanup.
Ground truth for the pgcopydb behavior behind each rule is the [upstream pgcopydb documentation](https://pgcopydb.readthedocs.io/) and the [pinned fork](https://github.com/ydixken/pgcopydb/tree/ea2dc96a47c2f7676d71a4967d044a1e469e4110) for all-databases behavior.
The e2e fixtures ([test/e2e](https://github.com/ydixken/pgcopydb-operator/tree/main/test/e2e)) apply the grants below.
Use the [Planning checklist](../planning.md) to record scope, operating, cutover, recovery, and rehearsal decisions that preflight cannot verify.

## Summary

Whole-instance clones (`spec.clone.allDatabases: true`) MUST connect as superuser on both sides; see [All databases](#all-databases).

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
- Credentials MUST live in Secrets in the Migration's namespace: a password Secret for the inline connection form, a full libpq URI Secret for `uriSecretRef`, or one details Secret for `secretRef`.
  TLS client material, if used, likewise.
- A StorageClass MUST be able to provision the work-volume PVC (`spec.workVolume`); it is the unit of resumability.
- Runner pods MUST be able to reach both endpoints on their PostgreSQL port.
  Both endpoints are plain libpq targets; they do not have to run on Kubernetes.

## Client tool versions

The runner image bundles pgcopydb and the PostgreSQL client tools.
`pg_dump`/`pg_restore` MUST be at least the target server's major version.
The default runner image ships pgcopydb 0.18 with PostgreSQL 18 client tools; for a newer target major, set `spec.runner.image` to an image with matching tools.

pgcopydb 0.18 is also a floor for the replication lag reading, not only for the client tools.
At pgcopydb 0.18 and above, `status.replication.lagBytes` reflects what the target has applied; below it the same figure reflects what the target has received, so it reads optimistically and a migration can look caught up while the apply is still behind.
The bundled runner pins its pgcopydb fork version in [the builder Dockerfile](https://github.com/ydixken/pgcopydb-operator/blob/main/images/pgcopydb-builder/Dockerfile).

## Base clone (every Migration)

Every Migration is gated by a `<name>-preflight` Job before the first attempt.
Its first check, always, is connectivity: `select 1` against both endpoints, each result logged in the Job output.
Wrong credentials or an unreachable host fail the Migration in `Validating`, before any worker attempt burns; a permanent error such as a failed password authentication ends the retry ladder as soon as a second probe repeats it, so a wrong password is a verdict in seconds while a pooler's momentary auth failure still gets the full ladder.
Selected installed source extensions must be installed on the target or have a non-null default version in `pg_available_extensions`.
With `clone.dropIfExists: true` or `clone.allDatabases: true`, a target default version is required even for an installed extension, because restore may recreate it or create it in a new database.
Extension include and exclude filters match exact names, with exclusion applied last; table and schema filters do not affect extension selection.
`clone.skip: [extensions]` bypasses this check, while `extensionComments` does not.
Missing, malformed, or failed probes fail preflight; an explicitly empty selection passes.
The check does not compare extension versions, install packages, grant installation privileges, or establish extension compatibility.
The separate `selected extension ownership` check covers target-installed extensions selected from the source when `dropIfExists` is true or extension comments are restored (the default).
It honours extension include/exclude filters and omits source extensions with initdb-reserved OIDs, such as built-in `plpgsql`, whose DDL and comments `pg_dump` does not emit.
The migration role MUST be a superuser or have the owning role's privileges, directly or through inherited membership (`pg_has_role(..., 'USAGE')`); owning the database alone does not suffice.
Not covered: extension ownership preflight on PostgreSQL 11 and older sources, which are outside the documented [E2E version matrix](https://github.com/ydixken/pgcopydb-operator/blob/main/CONTRIBUTING.md#e2e-tests).

> [!warning]
> The minimal failing spec is `spec.clone: {}` when the target already holds selected administrator-owned extensions.
> Without `dropIfExists`, `CREATE EXTENSION IF NOT EXISTS` is a no-op but the subsequent `COMMENT ON EXTENSION` fails ownership checks.
> Use `clone.skip: [extensionComments]` to keep other comments, or `clone.noComments: true` to suppress all comments.
> With `dropIfExists: true`, neither comment option prevents `DROP EXTENSION` failures: use an authorised migration role or `clone.skip: [extensions]` and provide the required extensions on the target yourself.

For single-database migrations, three target-side grant probes follow, all read-only: CREATE on the target database, CREATE on each source schema that already exists on the target (honouring the schema filters in `clone.filters`, and with `includeOnlyTables` narrowing the probes to those tables' schemas; schemas the restore must create fall under the database-level probe), and, unless `dbProperties` is in `clone.skip`, whether `ALTER DATABASE ... SET` can run (database ownership via `pg_has_role`, or superuser).
All-databases clones replace these maintenance-database probes with the instance-wide checks below.
A failed grant probe puts the exact `GRANT CREATE ...` statement in the condition message, with the `superuserSecretRef` hint when [that field](#superuser-remediation-superusersecretref) could apply it; the db-properties probe instead names its two outs, membership in the owning role or `clone.skip: [dbProperties]`.
Ownership alignment (`clone.noOwner`) and the source-side SELECT/USAGE privileges are not probed; a permission error they cause fails fast on the first attempt with reason `PermissionDenied` instead of burning the retry budget, when it is the attempt's terminal cause in the log tail (a best-effort scan, so a miss falls back to normal retries).

With `clone.ownerAfterRestore` set, two probes run ahead of those three: the named role MUST exist on the target, and the migration role MUST be able to `SET ROLE` to it, which is what `ALTER ... OWNER TO` requires.
A missing role stops the script there, so its diagnosis stays in the condition's log tail; a refused `SET ROLE` names the exact `GRANT <owner> TO <migration role>`, which `superuserSecretRef` applies for you.
The new owner's `CREATE` on the database and on the schemas it does not receive is not probed here, because the restore has not created those schemas yet: the `<name>-reown` Job checks it before it alters anything.

Source role:

- SELECT on every table and sequence being copied and USAGE on their schemas.
  Owning the objects covers all of it.
- Snapshot export needs no special privilege; plain clones need no replication privilege at all.

Target role:

- CREATE on the target database.
- Ownership alignment: without `clone.noOwner`, `pg_restore` emits `ALTER OWNER`, which only works as superuser or as the owning role.
  The simplest non-superuser setup is the pattern the e2e fixtures use: the migration connects as the role that owns every migrated object on both sides.
  Otherwise set `clone.noOwner: true`, and add [`clone.ownerAfterRestore`](#ownership-after-restore-cloneownerafterrestore) when the objects have to end up under a role the migration cannot connect as.
- `ALTER DATABASE ... SET` (the db-properties step) requires database ownership or superuser.
  If the target role has neither, add `dbProperties` to `clone.skip`.

Superuser is required only for:

- `clone.allDatabases: true` on both sides, even when role passwords are omitted.
- `clone.roles: true` without `clone.noRolePasswords: true` (reads passwords from `pg_authid`).
- Extensions: creating most C extensions on the target, and cloning a database whose superuser-installed extensions have configuration tables (a pg_dump limitation that filters cannot exclude).

### Ownership after restore (`clone.ownerAfterRestore`)

`clone.noOwner: true` leaves every restored object owned by the migration role.
`clone.ownerAfterRestore` names the role that MUST own them instead: the restore still runs as the migration role, and a `<name>-reown` Job hands the objects over once the worker has exited.
The field requires `noOwner: true`, cannot be combined with `clone.allDatabases`, and is immutable, all three enforced by the CRD.
See [Ownership after restore](../configuration.md#ownership-after-restore) for when to reach for it.

Preflight covers two of the requirements, ahead of the clone grant probes above: the role MUST exist on the target, and the migration role MUST be able to `SET ROLE` to it.
`SET ROLE` is what `ALTER ... OWNER TO` needs, and membership that only inherits the role's privileges does not supply it: PostgreSQL 16 added `GRANT ... WITH SET FALSE`, which inherits the privileges without the `SET ROLE`.
The probe is therefore a real `SET ROLE` inside a rolled-back transaction rather than a `pg_has_role(..., 'USAGE')` test.
`superuserSecretRef` remediates a refused `SET ROLE` with the `GRANT <owner> TO <migration role>` the preflight composes on the server.

Two more requirements wait for the handover Job, because the restore has not created the schemas yet when preflight runs.
The Job checks both before it alters anything, prints the exact `GRANT` when one is missing, and skips both when the migration role is a superuser (a third check, on inherited privileges, follows the same rule; see the callout below):

- The **migration role** needs `CREATE` on the target database, and only when the handover transfers at least one schema.
  This is the migration role rather than the new owner because `ALTER SCHEMA ... OWNER TO` checks the current user's right to create schemas, the same check `CREATE SCHEMA` makes.
- The **new owner** needs `CREATE` on every schema that holds objects it receives and that it does not receive itself.
  A schema in the transfer set supplies the privilege through its own `ALTER`, which runs first, so only the schemas that stay behind need a standing grant.
  `public` is the usual one: from PostgreSQL 15 it belongs to `pg_database_owner` and no longer grants `CREATE` to `PUBLIC`.

> [!important]
> The migration role also needs `USAGE` on every schema it hands over: after the `ALTER SCHEMA`, it still has to name the objects left inside that schema.
> Membership in the new owner supplies this when the membership carries inheritance, which is the default for `GRANT <owner> TO <migration role>` and therefore for the preflight's remediation too.
> It does not when the migration role has the `NOINHERIT` attribute, or when the membership was granted `WITH INHERIT FALSE` (PostgreSQL 16 and later).
> Both pass the `SET ROLE` probe, so the Job checks inherited privileges as a third pre-check, only when the handover transfers at least one schema, and prints the exact remediation for the target's version before altering anything: `GRANT <owner> TO <migration role> WITH INHERIT TRUE` from PostgreSQL 16, where inheritance is a property of the membership, or `ALTER ROLE <migration role> INHERIT` before that, where it is a role attribute instead.
> `GRANT USAGE ON SCHEMA <schema> TO <migration role>` is not a working alternative: granting `USAGE` to a role that owns the schema collapses into that role's own ACL entry, and the `ALTER SCHEMA ... OWNER TO` the handover runs next rewrites that entry to the new owner, wiping the grant out.
> Fix the inheritance instead.

The handover covers four object classes in the target database: schemas, relations (tables, partitions, sequences, views, materialized views, foreign tables), routines (functions, procedures, aggregates), and types including domains.
It leaves extension members alone, so the platform's own extensions keep their owner, and it does not touch large objects, publications, subscriptions, event triggers, foreign-data wrappers, operators, collations, text search objects, or statistics objects.
A sequence attached to a column by `serial` or `IDENTITY` gets no statement of its own because PostgreSQL refuses to change its owner directly; it follows its table, as array types, row types and multirange types follow the type they belong to.

> [!warning]
> The handover covers every schema, relation, routine and type in the target database that the migration role owns, not only the ones this restore created.
> Nothing in the catalog records which objects a restore created.
> Use a migration role dedicated to the migration when the target database also holds objects that role owns.

## All databases

Both migration connections MUST name an existing maintenance database such as `postgres` and use a role with `rolsuper`.
The source role dumps every database and copies roles, whose password dump reads `pg_authid`.
The target role creates missing databases, restores roles, and runs ownership changes across every database.
Managed admin roles without `rolsuper` are refused, and configuring `superuserSecretRef` does not satisfy this requirement for a non-superuser migration role.
All-databases preflight ignores those remediation references and does not project their credentials into the pod.

pgcopydb excludes only `template0` and `template1`, so source `postgres` is cloned into target `postgres`.
Roles are implied; `clone.roles: true` is redundant, and existing target roles are skipped.
`clone.noRolePasswords` is honoured but does not relax the required privileges.

> [!warning]
> Target databases MUST NOT already hold the schema being restored, including in `postgres`.
> Preflight lists existing target databases but does not check them for conflicting objects.
> `clone.dropIfExists`, `follow.enabled`, and `verification.data` are rejected in this mode; `verification.schema` is supported.

Preflight checks the migration roles' superuser attributes, lists the source databases and which already exist on the target, then checks selected extensions across every source database unless extensions are skipped.
A failed superuser check stops the script before database enumeration and extension probes.
A failed source or target database listing stops it before extension probes and prints a closing `preflight failed:` summary, keeping the listing failure in the condition's log tail.
Extension probing opens a separate psql session for each source database and runs sequentially, so its work grows linearly with database count within the preflight Job's 30-minute deadline.
Per-database extension failures distinguish source selection from validation on the target and include the database name.
The target extension check requires package availability, not merely installation in the maintenance database, because target databases may not exist yet.
Extension comments are restored per database, but the required target migration superuser already bypasses extension ownership checks.
pgcopydb changes only the database name in each connection URI and retains that role, so this mode needs no separate per-database ownership probe.
A failed database query fails preflight rather than reporting success on an empty result.

Filters and skips apply to every database, and job counts are global across databases.
The operator reports summed database sizes without per-database relation or catalog counters.
See [All databases configuration](../configuration.md#all-databases) and [the example](../examples/09-all-databases.yaml).

## Live migration (`spec.follow.enabled: true`)

The preflight Job additionally checks the requirements of this section before the first attempt, after the connectivity probes: `wal_level`, free-slot headroom, the source role's REPLICATION attribute, EXECUTE on the origin functions, the `session_replication_role` SET privilege, and an audit of every user table for a usable replica identity.
A failed check fails the Migration with the exact missing GRANT, setting, or table list in the `Validated` condition message, before any data moves; when the failing side has no `superuserSecretRef`, the message adds a hint naming that field.
With `superuserSecretRef` set, the three grantable rights are [applied by the preflight itself](#superuser-remediation-superusersecretref) instead of failing.
The rest of the workload contract (no DDL, large objects, wal2json presence) is not preflighted; checking it stays your job.

Source instance:

- `wal_level` MUST be `logical` (changing it requires a server restart).
- One free replication slot per running live Migration (`max_replication_slots`) and one free WAL sender (`max_wal_senders`).
- A replication slot retains WAL until it is dropped.
  Budget disk for the migration window, and mind `max_slot_wal_keep_size`: it caps retention, but a slot invalidated by the cap kills the migration.
  The operator drops the slot at cutover, on abort, and on Migration deletion.
- `wal_sender_timeout` SHOULD be at its PostgreSQL default (60s) or higher.
  Aggressive values terminate pgcopydb's logical-decoding walsender whenever a status update is a few seconds late; CloudNativePG sets 5s by default for its own HA streaming, so CNPG sources SHOULD override it in `spec.postgresql.parameters` for the migration window.

Source role:

- MUST have the `REPLICATION` attribute (or be superuser): `ALTER ROLE app REPLICATION`.
  Without it, slot creation fails with "permission denied to start WAL sender".
  On CloudNativePG sources this is declarative: a role listed in the Cluster's `managed.roles` with `replication: true` reconciles to the attribute (verified live).
  CNPG does not manage its bootstrap owner role by default, so either declare that role under `managed.roles` or run the `ALTER ROLE` once.
- Publication: pgcopydb auto-creates a publication for the migrated tables (named after the slot) and drops it during cleanup.
  The role MUST have CREATE on the source database and own every published table.
  Alternatively, pre-create a publication (superuser is needed for `FOR ALL TABLES`) and point `spec.follow.publication` at it; pgcopydb then leaves it alone.
  On automatic-publication `pgoutput` retries, the operator preserves the publication whenever the source slot exists.
  It drops an orphan publication only when the slot is absent, allowing pgcopydb to retry incomplete setup.
  A slot without its publication, or a failed catalog query, fails the attempt before pgcopydb starts; this guard does not recover every interrupted setup state.
  The guard never touches a publication named in `spec.follow.publication` and does not run for non-`pgoutput` plugins, including `wal2json` and `test_decoding`.
- Plugin: `pgoutput` (default) and `test_decoding` ship with PostgreSQL; `wal2json` MUST be installed on the source and named in the source's `output_plugin_libraries` before selecting it.
  That parameter defaults to `pgoutput, test_decoding`, and since the 2026-08-13 minor releases (14.24, 15.19, 16.15, 17.11, 18.6, which close [CVE-2026-6471](https://www.postgresql.org/support/security/CVE-2026-6471/)) the server refuses every output plugin the list omits.
  Adding an entry takes a config reload, not a restart.
  The preflight cannot verify the installation: a logical decoding plugin is a bare shared library with no catalog entry to query, and the only positive probe (creating a slot with it) is too invasive for a check.
  A plugin the source will not load fails the first attempt at slot creation, with `library "wal2json" may not be used as an output plugin` when the list omits it and `could not access file "wal2json"` when the library itself is absent.

Target role:

- MUST be able to `SET session_replication_role` (the apply session runs with it set to `replica` to keep triggers and foreign keys quiet during replay).
  Superuser can always; on PostgreSQL 15+ grant it explicitly: `GRANT SET ON PARAMETER session_replication_role TO app;`.
  WARNING: with pgcopydb 0.18, a role without this privilege does not fail the migration; apply silently replays nothing while reporting success, and only a row count comparison exposes the loss.
  Grant it before every live migration.
- MUST have EXECUTE on the `pg_replication_origin_*` catalog functions (superuser has it implicitly).
  Non-superuser grant, as used by the e2e fixtures:

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

- Every table that receives UPDATE or DELETE during the migration window MUST have a primary key or a replica identity (`REPLICA IDENTITY USING INDEX ...` or `REPLICA IDENTITY FULL`).
  With `pgoutput`, DML on a published table without one fails on the source at write time, breaking the application, not just the migration.
  The preflight audits all user tables for this and fails on offenders; it ignores `clone.filters`, since a filtered table can still take writes.
  Tables that are read-only or insert-only during the window MAY be acknowledged in `spec.follow.allowMissingReplicaIdentity` (schema-qualified names exactly as the preflight prints them; `["*"]` acknowledges every offender), which downgrades them to a warning.
- DDL is not replicated and MUST NOT run during the migration window; pre-create upcoming partitions before starting.
- Large-object changes during the window are not replicated (base copy only).
  Sequences need no handling: pgcopydb re-syncs them automatically after cutover.

## Superuser remediation (`superuserSecretRef`)

Each side of the Migration MAY carry `superuserSecretRef`, a Secret in the same convention as [`secretRef`](../configuration.md#credentials): the `USER` and `PW` keys name a superuser on that same endpoint.
The `URL`/`URL_EXTERNAL` keys, when present, MUST match the connection's endpoint; a mismatch fails the preflight by name, and a value carrying `:port` is compared port and all.
With a `secretRef` primary, the internal/external choice follows the primary's `endpoint` field, so both connections always name the same server.
The preflight probes the superuser connection (with the same retries as the primaries) and checks `rolsuper`; a role without it only logs a warning, because managed-Postgres admin roles (`rds_superuser` and friends) can hold the grant rights without the attribute.
It then applies the rights the regular role is missing, exactly these statements:

- `GRANT CREATE ON DATABASE <db> TO <role>` on the target, when the clone probe finds it missing (single-database migrations).
- `GRANT CREATE ON SCHEMA <schema> TO <role>` on the target, one grant per restore-target schema the role cannot create in (single-database migrations).
- `GRANT <owner> TO <role>` on the target, when the migration role cannot `SET ROLE` to `clone.ownerAfterRestore` (single-database migrations).
  The membership carries `SET` on every supported version, which is the part the handover needs.
- `ALTER ROLE <role> REPLICATION` on the source (follow only).
- `GRANT EXECUTE ON FUNCTION pg_replication_origin_* ...` on the target, one grant per missing function (follow only).
- `GRANT SET ON PARAMETER session_replication_role TO <role>` on the target (PostgreSQL 15+; on older targets the grant fails loudly; follow only).

Every applied statement is re-checked and logged in the preflight output, and one `PreflightRemediated` event per tier (clone rights, follow rights) on the Migration lists that tier's statements.
One event rather than one per statement, because the events API folds same-reason events into a counter that keeps only the first message.
Applied grants are kept, never reverted: they are the same grants you would run by hand.
Remediation never alters schema objects or data; it only grants rights.
It never touches replica identity, `wal_level`, plugin installation, database ownership, or extension ownership (the ownership probes only report remedies).
The remediation credentials are confined to preflight; pgcopydb uses the primary migration connections, which MUST themselves be superusers for all-databases clones.
One restriction: the superuser connection reuses the primary connection's URI, so a `uriSecretRef` primary holding a conninfo-style `key=value` DSN cannot host it and is rejected by name; use the URI form.
The reuse extends to TLS transport settings, including any client certificate; when the server maps certificate identities to roles, the certificate cannot present the superuser, so use password auth for it.

## Retries and snapshot consistency

The operator retries a failed attempt with `pgcopydb clone --resume --not-consistent` on the same work volume: finished tables are skipped, interrupted tables are re-copied from scratch (each table's COPY is a single transaction, so a killed attempt leaves no partial rows).
`--not-consistent` is required here: the first attempt's exported snapshot dies with its session, and a plain `--resume` fails before touching any data ("snapshot ... does not exist").

The trade-off: re-copied tables read a fresh snapshot.
A retried clone of a source that keeps taking writes is therefore not one single point in time across tables.
If that matters, stop writes across the retry window, or delete and recreate the Migration for a fresh consistent copy.
Follow migrations replay every change since the slot's consistent point on top of the base copy, and the cutover drain-verify gate plus `spec.verification.data` are the checks that catch divergence.

## Cutover

Cutover freezes the stream at the source's current LSN and drains it; anything written after that point does not reach the target.
Writes to the source MUST be stopped before `spec.cutover.approved: true` in Manual mode, and before the lag drops under `follow.maxCatchupLag` in Automatic mode.
Automatic is for sources that are already quiesced.
