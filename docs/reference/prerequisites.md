# Prerequisites

What a `Migration` needs from your PostgreSQL endpoints and your Kubernetes cluster before the operator can run it.
The keywords MUST, SHOULD, and MAY are to be interpreted as described in RFC 2119.

Scope: base clone (`pgcopydb clone`), whole-instance clone (`clone --all-databases`), live migration (`clone --follow`), cutover, and cleanup.
Ground truth for the pgcopydb behavior behind each rule is the [upstream pgcopydb documentation](https://pgcopydb.readthedocs.io/) and the [pinned fork](https://github.com/ydixken/pgcopydb/tree/93eda1dd9b3864e46e6b5913a65e1e7e8a400783) for all-databases behavior, partition comparison, and row-level security.
The e2e fixtures ([test/e2e](https://github.com/ydixken/pgcopydb-operator/tree/main/test/e2e)) apply the grants below.
Use the [Planning checklist](../planning.md) to record scope, operational, cutover, recovery, and rehearsal decisions that preflight cannot verify.

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
| No row-level security that filters the migration role  | source | single-database migrations |
| No unlogged tables in scope                             | source | follow           |

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
The default runner image ships pgcopydb 0.18 with PostgreSQL 18 client tools.
For a newer target major, set `spec.runner.image` to an image with client tools for that major version.

See [Follow diagnostics](../design/follow-diagnostics.md#what-the-snapshot-can-distinguish) for the feedback guarantees behind `status.replication.lagBytes`.
The bundled runner pins its pgcopydb fork version in [the builder Dockerfile](https://github.com/ydixken/pgcopydb-operator/blob/main/images/pgcopydb-builder/Dockerfile).

The progress poll supports eight pgcopydb versions, with different guarantees.
The bundled runner, `0.18.39.g93eda1d`, matches the NULL columns that `test_decoding` leaves out of a keyless `REPLICA IDENTITY FULL` old row, and applies a keyless change whose old row is all NULL; see the [live migration workload contract](#live-migration-specfollowenabled-true).
`0.18.36.g972e221` added the retry error fix: a retry that cannot read the previous run's catalog reports that read failure, where older runners asked for `--not-consistent`.
Both retain three fixes from `0.18.34.g7fddd6f`, listed in the [runner image README](https://github.com/ydixken/pgcopydb-operator/blob/main/images/runner/README.md): reads with [`row_security` off](#row-level-security), restores `REPLICA IDENTITY USING INDEX` on the target, and applies changes on a keyless `REPLICA IDENTITY FULL` table to one row.
They also retain [partition comparison](../operations/verification.md#partitioned-tables) from `0.18.22.g22e29c3`, and [certified idle feedback](../design/follow-diagnostics.md#what-the-snapshot-can-distinguish) and [missing-sentinel bootstrap recovery](../troubleshooting.md#publication-retry-failures) from `0.18.15.gea2dc96`.
`0.18.36.g972e221` lacks the NULL column fixes, `0.18.34.g7fddd6f` also lacks the retry error fix, `0.18.22.g22e29c3` also lacks the three fixes, and `0.18.15.gea2dc96` also lacks partition comparison.
`0.18.13.g4873c18` has certified idle feedback but lacks bootstrap recovery.
`0.18.10.gaadc4bf` and `0.18.5.ge37d2bd` have neither.
An older or custom runner may report weaker durability guarantees.

## Base clone (every Migration)

A `<name>-preflight` Job gates every Migration before the first attempt.
Its first check is connectivity: `select 1` against both endpoints, with each result logged in the Job output.
The probe retries up to six times, 10 seconds apart.
Two consecutive permanent errors, such as a failed password authentication, an unknown role, or an unknown database, end the retries early.
Wrong credentials or an unreachable host fail the Migration in `Validating`, before any worker attempt runs.
With `spec.preflight.requireSameMajorVersion: true`, the [major version gate](#major-version-gate) runs right after connectivity.
Selected installed source extensions must be installed on the target or have a non-null default version in `pg_available_extensions`.
With `clone.dropIfExists: true` or `clone.allDatabases: true`, an installed extension also needs a target default version.
Restore may recreate the extension, or create it in a new database.
Extension include and exclude filters match exact names, and exclusion applies last.
Table and schema filters do not affect extension selection.
`clone.skip: [extensions]` bypasses this check, while `extensionComments` does not.
Missing, malformed, or failed probes fail preflight; an explicitly empty selection passes.
The check does not compare extension versions, install packages, grant installation privileges, or establish extension compatibility.
The `selected extension ownership` check covers target-installed extensions selected from the source when `dropIfExists` is true or extension comments are restored (the default).
It honours the extension include and exclude filters.
It omits source extensions with initdb-reserved object identifiers, such as built-in `plpgsql`, whose data definition language (DDL) and comments `pg_dump` does not emit.
The migration role MUST be a superuser or have the owning role's privileges, directly or through inherited membership (`pg_has_role(..., 'USAGE')`); owning the database alone does not suffice.
Not covered: extension ownership preflight on PostgreSQL 11 and older sources, outside the documented [E2E version matrix](https://github.com/ydixken/pgcopydb-operator/blob/main/CONTRIBUTING.md#e2e-tests).

> [!warning]
> The minimal failing spec is `spec.clone: {}` when the target already holds selected administrator-owned extensions.
> Without `dropIfExists`, `CREATE EXTENSION IF NOT EXISTS` is a no-op but the subsequent `COMMENT ON EXTENSION` fails ownership checks.
> Use `clone.skip: [extensionComments]` to keep other comments, or `clone.noComments: true` to suppress all comments.
> With `dropIfExists: true`, neither comment option prevents `DROP EXTENSION` failures: use an authorised migration role or `clone.skip: [extensions]` and provide the required extensions on the target yourself.

For single-database migrations, three read-only grant probes then run on the target side:

- `CREATE` on the target database.
- `CREATE` on each source schema that already exists on the target.
  The probe honours the schema filters in `clone.filters`, and `includeOnlyTables` narrows it to those tables' schemas.
  Schemas the restore has to create fall under the database-level probe.
- Unless `dbProperties` is in `clone.skip`, whether `ALTER DATABASE ... SET` can run (database ownership through `pg_has_role`, or superuser).

All-databases clones replace these maintenance-database probes with the instance-wide checks below.
With `clone.ownerAfterRestore` set, two probes run ahead of those three: the named role MUST exist on the target, and the migration role MUST be able to `SET ROLE` to it, which is what `ALTER ... OWNER TO` requires.
A failed grant probe puts the exact `GRANT CREATE ...` statement in the condition message.
It adds a `superuserSecretRef` hint when [that field](#superuser-remediation-superusersecretref) could apply the grant.
The db-properties probe instead names its two remedies: membership in the owning role, or `clone.skip: [dbProperties]`.

Preflight does not probe ownership alignment (`clone.noOwner`) or the source-side SELECT and USAGE privileges.
A permission error from either fails the first attempt with reason `PermissionDenied` and does not spend the retry budget.
This holds only when the error is the attempt's terminal cause in the log tail.
The log scan is best-effort, so a miss falls back to normal retries.

Source role:

- SELECT on every table and sequence being copied and USAGE on their schemas.
  Owning the objects covers all of it.
- Snapshot export needs no special privilege; plain clones need no replication privilege at all.

Target role:

- CREATE on the target database.
- Ownership alignment: without `clone.noOwner`, `pg_restore` emits `ALTER OWNER`, which only works as superuser or as the owning role.
  The simplest non-superuser setup is the pattern the e2e fixtures use: the migration connects as the role that owns every migrated object on both sides.
  Otherwise set `clone.noOwner: true`.
  When the objects have to end up under a role the migration cannot connect as, add [`clone.ownerAfterRestore`](#ownership-after-restore-cloneownerafterrestore).
- `ALTER DATABASE ... SET` (the db-properties step) needs database ownership or superuser.
  If the target role has neither, add `dbProperties` to `clone.skip`.

Superuser is needed only for:

- `clone.allDatabases: true` on both sides, even when role passwords are omitted.
- `clone.roles: true` without `clone.noRolePasswords: true` (reads passwords from `pg_authid`).
- Extensions: most C extensions on the target, and any database whose superuser-installed extensions have configuration tables (a `pg_dump` limitation that filters cannot exclude).

### Row-level security

Row-level security that applies to the source migration role MUST NOT cover a table in scope.
The bundled runner reads with `row_security` off, as pg_dump does, so such a table fails the copy with SQLSTATE 42501.
Runners before `0.18.34.g7fddd6f` read with it on: the policies filter their COPY and their `compare data` alike, the target gets only the rows the role can see, and verification still passes.
The bundled runner also applies follow changes on the target with `row_security` off, so a target policy that would hide a row from an UPDATE or DELETE fails the apply instead of skipping the row.
Policies apply to the role when the table has row-level security enabled and the role does not own it (directly or through an inherited membership), or when the table also has `FORCE ROW LEVEL SECURITY`.
Row-level security without any policy hides every row from such a role.
Superusers and roles with `BYPASSRLS` are exempt.
pgcopydb reads each partition directly, and a query on a partition applies only that partition's own policies, so the audit checks plain tables and partitions and ignores policies on a partitioned parent.
It also skips tables that belong to an extension, such as `cron.job` from `pg_cron`, because pgcopydb leaves them out of its table copy.

Every single-database Migration's preflight fails when such a table is in scope, and names the tables.
Fix it in one of three ways:

- Give the source migration role `BYPASSRLS` (`ALTER ROLE <role> BYPASSRLS`, which takes a superuser).
- Disable row-level security on those tables for the migration.
- Leave them out with `clone.filters`: `excludeTables`, `excludeSchemas`, or `excludeTableData`.

All-databases clones skip the check, because their migration role is a superuser.
The [`objects/rls_force`](coverage.md#postgresql-feature-coverage) coverage case pins the refusal for a table with `FORCE ROW LEVEL SECURITY`.

This audit and the follow audit for [unlogged tables](#live-migration-specfollowenabled-true) cover the tables `clone.filters` keeps, matched by exact `schema.table` name.
Each audit applies the include filters the way pgcopydb applies them to its own step.
The copy keeps a table only when it passes both lists: its own schema is in `includeOnlySchemas`, if set, and its own name is in `includeOnlyTables`, if set.
So naming a partitioned table in `includeOnlyTables` copies none of its partitions, and a partition in another schema stays out of an `includeOnlySchemas` scope.
The follow publication takes a table when either list names it or any partition ancestor, because `FOR TABLE` on a partitioned table covers its partitions.
An entry the audits cannot match exactly (a `~` pattern, a quoted name, a table name without a schema, or an uppercase schema name) widens them instead: it voids its include list and the publication's, and drops out of an exclude list.

### Ownership after restore (`clone.ownerAfterRestore`)

`clone.noOwner: true` leaves every restored object owned by the migration role.
`clone.ownerAfterRestore` names the role that MUST own them instead: the restore still runs as the migration role, and a `<name>-reown` Job hands the objects over once the worker has exited.
The field needs `noOwner: true`, does not combine with `clone.allDatabases`, and is immutable; the CRD enforces all three.
Give the role name unquoted, exactly as PostgreSQL stores it.
The operator quotes it as an identifier wherever it reaches SQL, so names such as `kkp-db-medium-dev` work as they are.
The CRD accepts any name except one that contains a double quote or a control character, or is longer than 63 bytes.
We reject the double quote because a self-quoted name such as `"db-admin"` would be quoted a second time and name a role that does not exist.
The limit counts bytes, because PostgreSQL truncates a longer name to 63 bytes, and a name with non-ASCII characters reaches that before 63 characters.
See [Ownership after restore](../configuration.md#ownership-after-restore) for when to use it.

Preflight covers two of the requirements, ahead of the clone grant probes above: the role MUST exist on the target, and the migration role MUST be able to `SET ROLE` to it.
`SET ROLE` is what `ALTER ... OWNER TO` needs, and membership that only inherits the role's privileges does not supply it.
PostgreSQL 16 added `GRANT ... WITH SET FALSE`, which inherits the privileges without the `SET ROLE`.
The probe is therefore a real `SET ROLE` inside a rolled-back transaction rather than a `pg_has_role(..., 'USAGE')` test.
`superuserSecretRef` remediates a refused `SET ROLE` with the `GRANT <owner> TO <migration role>` the preflight composes on the server.

Two more requirements belong to the `<name>-reown` Job, because the restore has not created the schemas when preflight runs.
The Job checks both before it alters anything, and prints the exact `GRANT` when one is missing.
It skips both when the migration role is a superuser, and the inherited-privilege check below follows the same rule.

- The **migration role** needs `CREATE` on the target database, and only when the handover transfers at least one schema.
  This is the migration role rather than the new owner, because `ALTER SCHEMA ... OWNER TO` checks the current user's right to create schemas.
  That is the same check `CREATE SCHEMA` makes.
- The **new owner** needs `CREATE` on every schema that holds objects it receives and that it does not receive itself.
  A schema in the transfer set supplies the privilege through its own `ALTER`, which runs first.
  Only the schemas that stay behind need an explicit grant.
  `public` is the usual one: from PostgreSQL 15 it belongs to `pg_database_owner` and no longer grants `CREATE` to `PUBLIC`.

> [!important]
> The migration role also needs `USAGE` on every schema it hands over: after the `ALTER SCHEMA`, it still has to name the objects left inside that schema.
> Membership in the new owner supplies this when the membership carries inheritance, which is the default for `GRANT <owner> TO <migration role>` and therefore for the preflight's remediation too.
> It does not when the migration role has the `NOINHERIT` attribute, or when the membership was granted `WITH INHERIT FALSE` (PostgreSQL 16 and later).
> Both pass the `SET ROLE` probe, so the Job checks inherited privileges as a third pre-check, only when the handover transfers at least one schema, and prints the exact remediation for the target's version before altering anything: `GRANT <owner> TO <migration role> WITH INHERIT TRUE` from PostgreSQL 16, where inheritance is a property of the membership, or `ALTER ROLE <migration role> INHERIT` before that, where it is a role attribute instead.
> `GRANT USAGE ON SCHEMA <schema> TO <migration role>` is not a working alternative: granting `USAGE` to a role that owns the schema collapses into that role's own ACL entry, and the `ALTER SCHEMA ... OWNER TO` the handover runs next rewrites that entry to the new owner, wiping the grant out.
> Fix the inheritance instead.

The handover covers four object classes in the target database:

- Schemas.
- Relations: tables, partitions, sequences, views, materialized views, foreign tables.
- Routines: functions, procedures, aggregates.
- Types, including domains.

It leaves extension members alone, so the platform's own extensions keep their owner.
It does not touch large objects, publications, subscriptions, event triggers, foreign-data wrappers, operators, collations, text search objects, or statistics objects.
A sequence attached to a column by `serial` or `IDENTITY` gets no statement of its own, because PostgreSQL refuses to change its owner directly.
It follows its table, as array types, row types and multirange types follow the type they belong to.

> [!warning]
> The handover covers every schema, relation, routine and type in the target database that the migration role owns, not only the ones this restore created.
> Nothing in the catalog records which objects a restore created.
> Use a migration role dedicated to the migration when the target database also holds objects that role owns.

### Major version gate

`spec.preflight.requireSameMajorVersion: true` fails preflight when the source and target PostgreSQL major versions differ.
Minor versions are ignored.
It is off by default, because a cross-major upgrade is a main use of pgcopydb.
The gate runs on the plain and the all-databases path, and under `spec.dryRun` it reports like any other failed check.

Preflight asks each live server over the Migration's own source and target connections: `current_setting('server_version_num')`, divided by 10000.
It does not trust the spec, the image tag, or the `server_version` text, because managed services decorate that text (Debian or Aurora suffixes).
Through PgBouncer the query runs on the real backend, so it reports the server's version, not the one PgBouncer advertises at connect.
A side that cannot be read fails the gate rather than passing it.
A failed gate stops preflight, so no later check runs and the version line is the one reported.

```yaml
spec:
  preflight:
    requireSameMajorVersion: true
```

## All databases

Both migration connections MUST name an existing maintenance database such as `postgres` and use a role with `rolsuper`.
The source role dumps every database and copies roles, whose password dump reads `pg_authid`.
The target role creates missing databases, restores roles, and runs ownership changes across every database.
Managed admin roles without `rolsuper` are refused, and `superuserSecretRef` does not satisfy this requirement for a non-superuser migration role.
All-databases preflight ignores those remediation references and does not project their credentials into the pod.

pgcopydb excludes only `template0` and `template1`, so source `postgres` is cloned into target `postgres`.
Roles are implied; `clone.roles: true` is redundant, and existing target roles are skipped.
`clone.noRolePasswords` is honoured but does not relax the required privileges.

> [!warning]
> Target databases MUST NOT already hold the schema being restored, including in `postgres`.
> Preflight lists existing target databases but does not check them for conflicting objects.
> `clone.dropIfExists`, `follow.enabled`, and `verification.data` are rejected in this mode; `verification.schema` is supported.

Preflight checks the migration roles' superuser attributes, and lists the source databases and which already exist on the target.
Unless extensions are skipped, it also checks selected extensions across every source database.
The extension probe runs sequentially, so its work grows linearly with the database count, inside the preflight Job's 30-minute deadline.
A failed database query fails preflight; it never reports success on an empty result.

The target extension check needs package availability, not installation in the maintenance database, because target databases may not exist yet.

Filters and skips apply to every database, and job counts are global across databases.
The operator reports summed database sizes without per-database relation or catalog counters.
See [All databases configuration](../configuration.md#all-databases) and [the example](../examples/09-all-databases.yaml).

## Live migration (`spec.follow.enabled: true`)

Before the first attempt, and after the connectivity probes, the preflight Job also checks the requirements of this section:

- `wal_level`.
- Free-slot headroom.
- The source role's REPLICATION attribute.
- EXECUTE on the origin functions.
- The `session_replication_role` SET privilege.
- An audit of every user table for a usable replica identity.
- An audit of the tables in scope for unlogged tables.

A failed check fails the Migration before any data moves, with the exact missing GRANT, setting, or table list in the `Validated` condition message.
When the failing side has no `superuserSecretRef`, the message adds a hint that names that field.
With `superuserSecretRef` set, [the preflight applies](#superuser-remediation-superusersecretref) the three grantable rights and does not fail.
Preflight does not cover the rest of the workload contract (no DDL, large objects, wal2json presence); that stays your job.

Source instance:

- `wal_level` MUST be `logical` (changing it requires a server restart).
- One free replication slot per concurrent live Migration (`max_replication_slots`) and one free WAL sender (`max_wal_senders`).
- A replication slot keeps WAL until it is dropped.
  Budget disk for the migration window.
  `max_slot_wal_keep_size` caps that retention, but a slot invalidated by the cap kills the migration.
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
  Alternatively, pre-create a publication (superuser is needed for `FOR ALL TABLES`) and point `spec.follow.publication` at it.
  pgcopydb then leaves it alone.
  On retries the operator guards an automatic `pgoutput` publication, and never touches one named in `spec.follow.publication`.
  See [Publication retry failures](../troubleshooting.md#publication-retry-failures) for that guard and its limits.
- Plugin: `pgoutput` (default) and `test_decoding` ship with PostgreSQL; `wal2json` MUST be installed on the source and named in the source's `output_plugin_libraries` before selecting it.
  That parameter defaults to `pgoutput, test_decoding`.
  Since the 2026-08-13 minor releases (14.24, 15.19, 16.15, 17.11, 18.6, which close [CVE-2026-6471](https://www.postgresql.org/support/security/CVE-2026-6471/)), the server refuses every output plugin the list omits.
  A new entry takes a config reload, not a restart.
  The preflight cannot verify the installation: a logical decoding plugin is a bare shared library with no catalog entry to query.
  A plugin the source will not load fails the first attempt at slot creation.
  When the list omits the plugin, the error is `library "wal2json" may not be used as an output plugin`.
  When the library itself is absent, it is `could not access file "wal2json"`.

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

  Missing grants surface on the first runner attempt, which dies in pgcopydb's setup cleanup with "permission denied for function pg_replication_origin_drop", before any data moves.

Schema and workload contract:

- Every table that receives UPDATE or DELETE during the migration window MUST have a primary key or a replica identity (`REPLICA IDENTITY USING INDEX ...` or `REPLICA IDENTITY FULL`).
  With `pgoutput`, a data manipulation language (DML) statement on a published table without one fails on the source at write time.
  That breaks the application, not just the migration.
  The preflight audits all user tables for this and fails on offenders.
  It ignores `clone.filters`, because a filtered table can still take writes.
  Tables that are read-only or insert-only during the window MAY be acknowledged in `spec.follow.allowMissingReplicaIdentity` (schema-qualified names exactly as the preflight prints them; `["*"]` acknowledges every offender), which downgrades them to a warning.
  The bundled runner keeps `REPLICA IDENTITY USING INDEX` and `REPLICA IDENTITY FULL` on the target as the source has them.
  Stock pgcopydb falls back to the default identity where the source uses `USING INDEX`.
- `TRUNCATE` is replicated: the target is truncated at the same point in the change stream.
- On a table with `REPLICA IDENTITY FULL` and no key, the bundled runner changes one matching row per UPDATE or DELETE and matches each NULL column of the old row with `IS NULL`, under every plugin.
  That covers the NULL columns `test_decoding` leaves out of the old row, and an old row whose columns are all NULL.
  A runner older than `0.18.39.g93eda1d` SHOULD NOT be used with `test_decoding` on such a table if it holds rows that are equal except where one of them is NULL: it matches only the columns `test_decoding` sends, so the change can land on the wrong row.
  Those runners also skip an UPDATE or DELETE whose old row is all NULL, under every plugin, and the target keeps the row.
  On a Citus-distributed target, the one-row form fails the apply with an error.
- DDL is not replicated and MUST NOT run during the migration window; pre-create upcoming partitions before starting.
  A change that uses a column the target lacks stops the apply: the worker log shows `[42703] ERROR:  column ... does not exist` (two spaces after `ERROR:`).
  Every retry would stop at the same change, so the Migration fails with `SchemaDrift` on that attempt.
  The [`limitations/ddl_add_column`](coverage.md#postgresql-feature-coverage) coverage case pins this.
- Unlogged tables MUST NOT be in scope.
  They write no WAL, so logical decoding never sees their changes.
  With the automatic publication, pgcopydb's `CREATE PUBLICATION ... FOR TABLE` refuses them and the first attempt fails before any copy.
  With `spec.follow.publication` or `wal2json`, their changes during the window are lost, and only `spec.verification.data` notices.
  The preflight fails on unlogged tables in scope, scoped as described under [Row-level security](#row-level-security).
  One difference: pgcopydb's publication honours `includeOnlySchemas` only next to `includeOnlyTables`, so `includeOnlySchemas` alone still publishes every table outside the exclusions, and the audit checks all of them.
  Run `ALTER TABLE ... SET LOGGED` on the source, or leave them out with `excludeTables` or `excludeSchemas`.
  Next to `includeOnlySchemas`, use `excludeTables`, because `excludeSchemas` cannot be combined with it.
  `excludeTableData` does not help, because pgcopydb still publishes those tables.
  The [`limitations/unlogged_writes`](coverage.md#postgresql-feature-coverage) coverage case pins the refusal.
- Large-object changes during the window are not replicated (base copy only).
  A large object written or unlinked during the window keeps its base-copy content on the target, and one created during the window does not exist there, even when a replicated row references it.
  Verification cannot see this: `compare data` covers tables, not `pg_largeobject`, so `Verified` stays `True`.
  The [`limitations/large_object_change`](coverage.md#postgresql-feature-coverage) coverage case pins this.
- Sequences need no action: pgcopydb re-syncs them automatically after cutover.

## Superuser remediation (`superuserSecretRef`)

Each side of the Migration MAY carry `superuserSecretRef`, a Secret in the same convention as [`secretRef`](../configuration.md#credentials): the `USER` and `PW` keys name a superuser on that same endpoint.
The `URL`/`URL_EXTERNAL` keys, when present, MUST match the connection's endpoint; a mismatch fails the preflight by name, and a value carrying `:port` is compared port and all.
With a `secretRef` primary, the primary's `endpoint` field decides between the internal and external URL, so both connections always name the same server.
The preflight probes the superuser connection with the same retries as the primaries, and checks `rolsuper`.
A role without `rolsuper` only logs a warning, because managed-Postgres admin roles such as `rds_superuser` can hold the grant rights without the attribute.
It then applies the rights the regular role is missing, exactly these statements:

- `GRANT CREATE ON DATABASE <db> TO <role>` on the target, when the clone probe finds it missing (single-database migrations).
- `GRANT CREATE ON SCHEMA <schema> TO <role>` on the target, one grant per restore-target schema the role cannot create in (single-database migrations).
- `GRANT <owner> TO <role>` on the target, when the migration role cannot `SET ROLE` to `clone.ownerAfterRestore` (single-database migrations).
  The membership carries `SET` on every supported version, which is the part the handover needs.
- `ALTER ROLE <role> REPLICATION` on the source (follow only).
- `GRANT EXECUTE ON FUNCTION pg_replication_origin_* ...` on the target, one grant per missing function (follow only).
- `GRANT SET ON PARAMETER session_replication_role TO <role>` on the target (PostgreSQL 15+; on older targets the grant fails loudly; follow only).

The preflight re-checks and logs every applied statement in its output.
One `PreflightRemediated` event per tier (clone rights, follow rights) on the Migration lists that tier's statements.
Under [`spec.dryRun`](../operations/lifecycle.md#dry-run) the preflight verifies the superuser connection and applies nothing.
It prints each statement it would apply as `would-remediate:` or `would-remediate-clone:`, and one `PreflightWouldRemediate` event per tier lists them.
It runs none of them, so a statement that would fail, such as the `session_replication_role` grant on a PostgreSQL 14 target, passes the dry run and fails the real preflight.
Applied grants are kept, never reverted: they are the same grants you would run by hand.
Remediation never alters schema objects or data; it only grants rights.
It never touches replica identity, `wal_level`, plugin installation, database ownership, or extension ownership (the ownership probes only report remedies).
The remediation credentials are confined to preflight; pgcopydb uses the primary migration connections, which MUST themselves be superusers for all-databases clones.
One restriction: the superuser connection reuses the primary connection's URI.
A `uriSecretRef` primary that holds a conninfo-style `key=value` connection string cannot host it, and preflight rejects it by name.
Use the URI form.
The reuse extends to TLS transport settings, including any client certificate.
When the server maps certificate identities to roles, the certificate cannot present the superuser, so use password auth for it.

## Retries and snapshot consistency

The operator retries a failed attempt with `pgcopydb clone --resume --not-consistent` on the same work volume.
Finished tables are skipped, and interrupted tables are re-copied from scratch.
Each table's COPY is a single transaction, so a killed attempt leaves no partial rows.
`--not-consistent` is needed here: the first attempt's exported snapshot dies with its session.
A plain `--resume` fails before it touches any data ("snapshot ... does not exist").
A permission error, a full work volume, or a change naming a column the target lacks stops the `--resume` retry: the Migration ends as `PermissionDenied`, `DiskFull`, or `SchemaDrift` (see [Retries and resume](../operations/lifecycle.md#retries-and-resume)).

The trade-off: re-copied tables read a fresh snapshot.
A retried clone of a source that still takes writes is therefore not one single point in time across tables.
If that matters, stop writes across the retry window, or delete and recreate the Migration for a fresh consistent copy.
Follow migrations replay every change since the slot's consistent point on top of the base copy.
The cutover drain-verify gate and `spec.verification.data` are the checks that catch divergence.

## Cutover

Cutover freezes the stream at the source's current LSN and drains it; anything written after that point does not reach the target.
Writes to the source MUST be stopped before `spec.cutover.approved: true` in Manual mode, and before the lag drops under `follow.maxCatchupLag` in Automatic mode.
Automatic is for sources that are already quiesced.
