# Prerequisites

This page describes what a `Migration` needs from your PostgreSQL endpoints and your Kubernetes cluster before the operator can run it.
Read the keywords MUST, SHOULD, and MAY as RFC 2119 describes them.

This page covers the base clone (`pgcopydb clone`), the whole-instance clone (`clone --all-databases`), live migration (`clone --follow`), cutover, and cleanup.
The ground truth for the pgcopydb behavior behind each rule is the [upstream pgcopydb documentation](https://pgcopydb.readthedocs.io/).
For all-databases behavior, partition comparison, and row-level security, it is also the [pinned fork](https://github.com/ydixken/pgcopydb/tree/86bf908adca02d87063849dae775a02c1659c829).
The e2e fixtures ([test/e2e](https://github.com/ydixken/pgcopydb-operator/tree/main/test/e2e)) apply the grants below.
Use the [Planning checklist](../planning.md) to record the scope, operational, cutover, recovery, and rehearsal decisions that preflight cannot check.

## Summary

Whole-instance clones (`spec.clone.allDatabases: true`) MUST connect as superuser on both sides.
See [All databases](#all-databases).

| Requirement                                             | Where  | Needed for       |
|---------------------------------------------------------|--------|------------------|
| No PgBouncer between the runner and either endpoint     | both   | every migration  |
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
| `TEMPORARY` on the database (`PUBLIC` holds it by default) | target | retries        |
| A migration role no other Migration shares on the target database | target | every migration |

## Kubernetes

- The `migrations.pgcopydb-operator.io` CRD MUST be installed (chart `crds.install=true`, or `config/crd`).
- Credentials MUST live in Secrets in the Migration's namespace.
  The inline connection form uses a password Secret, `uriSecretRef` uses a Secret with a full libpq URI, and `secretRef` uses one details Secret.
  TLS client material, if you use it, MUST also live in Secrets in that namespace.
- A StorageClass MUST be able to provision the work-volume PVC (`spec.workVolume`).
  That PVC is the unit of resumability.
- Runner pods MUST be able to reach both endpoints on their PostgreSQL port.
  Both endpoints are plain libpq targets.
  They do not have to run on Kubernetes.

## Client tool versions

The runner image bundles pgcopydb and the PostgreSQL client tools.
`pg_dump`/`pg_restore` MUST be at least the target server's major version.
`pg_restore` 17 or later MUST NOT restore into a target older than PostgreSQL 17.
The default runner image ships pgcopydb 0.18 with PostgreSQL 18 client tools, so its target MUST run PostgreSQL 17 or later.

> [!warning]
> The default runner cannot clone into PostgreSQL 16 or older.
> Its `pg_restore` sends `SET transaction_timeout = 0` on every target connection.
> Those servers reject that setting, so every attempt fails at the pre-data restore.
> Preflight fails such a Migration with the [`pg_restore` check](#pg_restore-and-the-target-major).

For a newer target major, set `spec.runner.image` to an image with client tools for that major version.
For a PostgreSQL 16 or older target, set it to an image whose `pg_restore` is version 16 or older.
pgcopydb takes `pg_dump` and `pg_restore` from one directory.
When `PG_CONFIG` names a file, it is the directory that holds the `psql` in `$PG_CONFIG --bindir`.
Otherwise, it is the directory that holds the first `psql` on `PATH`.
Thus that `pg_dump` is as old as the `pg_restore`, and it refuses a source that is newer than its own major version.
A custom runner image MUST ship psql 18 or later.
The reason is that the [PgBouncer check](#no-pgbouncer-in-the-connection) reads the backend PID that psql 18 added to `\conninfo`.
Without that PID, every preflight fails.
A custom runner image that you use with `spec.clone.deferIndexes` MUST ship pgcopydb `0.18.136.gb42f560` or later.
With that field, preflight fails unless the `pgcopydb clone --help` of the image lists `--defer-indexes` as `Build indexes once all table data is copied`.
The check matches that description, not only the flag name, because another pgcopydb fork ships a `--defer-indexes` that behaves differently.

See [Follow diagnostics](../design/follow-diagnostics.md#what-the-snapshot-can-distinguish) for the feedback guarantees behind `status.replication.lagBytes`.
The bundled runner pins its pgcopydb fork version in [the builder Dockerfile](https://github.com/ydixken/pgcopydb-operator/blob/main/images/pgcopydb-builder/Dockerfile).

The progress poll supports twenty-one pgcopydb versions, with different guarantees.

The bundled runner, `0.18.144.g86bf908`, added the compare progress lines.
`pgcopydb compare data` logs a `compare progress:` line of JSON counters when its workers start, then every 10 seconds.
It logs one last line with `"final":true`.
The operator reads these lines into [`status.verificationProgress`](../operations/verification.md#progress).

The bundled runner gives each `--json` report row a `status` of `match`, `differ`, `error` or `unchecked`.
When a checksum query fails, it continues with the next table.
In that case, `0.18.136.gb42f560` stops that worker and prints no report.
The bundled runner still exits with code 12 when a table differs, failed, or was not compared.

`0.18.136.gb42f560` added the deferred index builds.
From `0.18.136.gb42f560` on, `pgcopydb clone` accepts `--defer-indexes`.
With that option, it copies every table before it builds any index or constraint or runs any `VACUUM ANALYZE`.

`0.18.132.gd4a2db1` added the constraint fix.
This fix applies to a constraint that a killed attempt added on the target without recording it.
A retry keeps that constraint when its `pg_get_constraintdef()` text equals the source's text.
When the two texts differ, the retry stops with an error that prints both texts.
`0.18.129.gd7547b5` fails every retry on such a constraint with exit code 12, on `relation "..." already exists` or `index "..." is already associated with a constraint`.

`0.18.129.gd7547b5` added the all-databases resume fixes.
One fix is for a retry with `clone.allDatabases` after an attempt died during the copy or index build of a split table.
That retry copies or keeps the table as a single-database retry does.
`0.18.112.g4c61e97` can fail every such retry with exit code 12, copy a part twice, or keep rows that the source changed since.
In both modes, `0.18.129.gd7547b5` also builds the constraints of a table after an attempt died while it built them.
In that case, `0.18.112.g4c61e97` fails every retry with exit code 12 on `relation "..." already exists`.
`0.18.129.gd7547b5` still fails every retry with exit code 12 when the attempt died while it added a constraint.
The same occurs when the attempt died after it added a constraint but before it recorded that constraint.

With `clone.allDatabases`, `0.18.129.gd7547b5` copies large objects and the user rows of extension configuration tables.
With `clone.allDatabases`, `0.18.112.g4c61e97` copies neither and still exits 0.
Without `clone.allDatabases`, a retry finishes a large-object or extension configuration step that an earlier attempt started.
`0.18.112.g4c61e97` skips that step when the tables, indexes and sequences were already done.
It then exits 0 without the objects or rows that the earlier attempt did not copy.

`0.18.129.gd7547b5` logs a message longer than 2 KB in full.
`0.18.112.g4c61e97` aborts on such a message with `stack smashing detected`.
With `clone.allDatabases`, `0.18.129.gd7547b5` clones an instance of five or more databases.
With `clone.allDatabases`, four table jobs, and five or more databases (counting `postgres`), `0.18.112.g4c61e97` fails every attempt in its first phase.

`0.18.129.gd7547b5` finds an existing role whose name needs quoting.
`0.18.112.g4c61e97` fails on `role "..." already exists` whenever the target already has such a role.
That includes every retry of a Migration with `clone.allDatabases` or `clone.roles: true`.
`0.18.129.gd7547b5` also copies a role whose quoted name is 64 bytes or longer.
`0.18.112.g4c61e97` cuts that name to 63 bytes and sends a broken `CREATE ROLE`, even into a target without the role.

`0.18.112.g4c61e97` added the split-table resume fix.
Without `clone.allDatabases`, a retry keeps a split table whose parts were all copied.
It copies a partly copied split table again in full.
`0.18.109.gbec4ebb` truncates the table and skips the finished parts, so the Migration can complete without their rows.
`0.18.112.g4c61e97` also vacuums each table at most once across retries.
`0.18.109.gbec4ebb` fails a retry with exit code 12 on a table without indexes when an earlier attempt started the `VACUUM` of that table.
The next retry can then exit 0 without ever analyzing that table.
See [Retries and resume](../operations/lifecycle.md#retries-and-resume).

`0.18.109.gbec4ebb` added the target keepalives.
It sets server-side TCP keepalives on the target sessions that it opens itself.
When a runner dies and such a session waits on it, the target ends the session within about 70 seconds.
With `0.18.104.g362a559`, the session keeps its locks for about two hours at the kernel defaults ([#381](https://github.com/ydixken/pgcopydb-operator/issues/381)).
[Retries and resume](../operations/lifecycle.md#retries-and-resume) lists the sessions that this bound does not cover.

`0.18.104.g362a559` added the snapshot fix for live migration retries.
A retry that creates the replication slot copies under the snapshot of the slot.
A retry that finds the slot of an earlier attempt and still has tables to copy stops with `Refusing to resume the copy: ...` before it copies any of them.
`0.18.98.g944abcc` copies the tables of either retry under a newer snapshot, and apply can then stop on a duplicate key ([#378](https://github.com/ydixken/pgcopydb-operator/issues/378)).

`0.18.98.g944abcc` added the closed `output.db` fix.
During streaming, apply stays on an `output.db` that receive closed, until apply applies every committed transaction in that file.
`0.18.94.gbaa94ee` can move to the next file and skip those transactions ([#376](https://github.com/ydixken/pgcopydb-operator/issues/376)).

`0.18.94.gbaa94ee` added apply groups.
It commits up to 100 source transactions in one target transaction.
Older runners commit each source transaction on its own.
`spec.follow.applyGroupTransactions` sets the limit ([Apply groups](../operations/performance.md#apply-groups)).

`0.18.76.gf9b328b` added the status interval fix.
It sends receive status within half of the source `wal_sender_timeout`.
`0.18.74.gc682dce` sends it every 10 seconds.
So with `0.18.74.gc682dce`, a timeout below 10 seconds can end every receive session before a receive that lagged reports a flush position ([#369](https://github.com/ydixken/pgcopydb-operator/issues/369)).

`0.18.74.gc682dce` added the reconnect fix.
When the source ends the walsender while receive reconnects, this runner exits receive with a source error.
In that case, `0.18.72.g2aa91e7` aborts receive with `free(): invalid pointer` ([#364](https://github.com/ydixken/pgcopydb-operator/issues/364)).

`0.18.72.g2aa91e7` added the receive fix.
It reconnects when the source closes the stream near endpos.
In that case, `0.18.70.gbb8dbfc` aborts receive with a double free ([#362](https://github.com/ydixken/pgcopydb-operator/issues/362)).

`0.18.70.gbb8dbfc` added four follow fixes:

- It applies source transactions that overlapped, in commit order.
- It keeps every row of a multi-insert write, such as a source `COPY`.
- Under `pgoutput` and `test_decoding`, it replays every table of a multi-table `TRUNCATE`.
- It sends the origin setup and the `COMMIT` of a data transaction as one query.
  When the target refuses that `COMMIT` and the connection stays up, it moves the target replication origin back.

See the [live migration hazard](../operations/live-migration.md#watching-the-stream) for what older runners lose, and the [live migration workload contract](#live-migration-specfollowenabled-true).

`0.18.39.g93eda1d` added the NULL column fixes.
It matches the NULL columns that `test_decoding` omits from a keyless `REPLICA IDENTITY FULL` old row.
It also applies a keyless change whose old row is all NULL.

`0.18.36.g972e221` added the retry error fix.
A retry that cannot read the catalog of the previous run reports that read failure.
Older runners asked for `--not-consistent` instead.

All fifteen of these versions retain three fixes from `0.18.34.g7fddd6f`.
The [runner image README](https://github.com/ydixken/pgcopydb-operator/blob/main/images/runner/README.md) lists them:

- The runner reads with [`row_security` off](#row-level-security).
- It restores `REPLICA IDENTITY USING INDEX` on the target.
- It applies a change on a keyless `REPLICA IDENTITY FULL` table to one row.

They also retain [partition comparison](../operations/verification.md#partitioned-tables) from `0.18.22.g22e29c3`.
From `0.18.15.gea2dc96`, they retain [certified idle feedback](../design/follow-diagnostics.md#what-the-snapshot-can-distinguish) and [missing-sentinel bootstrap recovery](../troubleshooting.md#publication-retry-failures).

The older runners lack these items:

- `0.18.136.gb42f560` lacks the compare progress lines.
- `0.18.132.gd4a2db1` also lacks the deferred index builds.
- `0.18.129.gd7547b5` also lacks the constraint fix.
- `0.18.112.g4c61e97` also lacks the all-databases resume, large-object, long log message, five-database and quoted role name fixes.
- `0.18.109.gbec4ebb` also lacks the split-table resume fix.
- `0.18.104.g362a559` also lacks the target keepalives.
- `0.18.98.g944abcc` also lacks the snapshot fix.
- `0.18.94.gbaa94ee` and `0.18.76.gf9b328b` also lack the closed `output.db` fix.
- `0.18.74.gc682dce` also lacks the status interval fix.
- `0.18.72.g2aa91e7` also lacks the reconnect fix.
- `0.18.70.gbb8dbfc` also lacks the receive fix.
- `0.18.39.g93eda1d` also lacks the four follow fixes.
- `0.18.36.g972e221` also lacks the NULL column fixes.
- `0.18.34.g7fddd6f` also lacks the retry error fix.
- `0.18.22.g22e29c3` also lacks the three fixes.
- `0.18.15.gea2dc96` also lacks partition comparison.
- `0.18.13.g4873c18` has certified idle feedback but lacks bootstrap recovery.
- `0.18.10.gaadc4bf` and `0.18.5.ge37d2bd` have neither.

An older or custom runner may report weaker durability guarantees.

## Base clone (every Migration)

A `<name>-preflight` Job gates every Migration before the first attempt.
Its first check is connectivity.
It runs `select 1` against both endpoints and logs each result in the Job output.
The probe retries up to six times, 10 seconds apart.
Two consecutive permanent errors, such as a failed password authentication, an unknown role, or an unknown database, end the retries early.
Wrong credentials or an unreachable host fail the Migration in `Validating`, before any worker attempt runs.
The [PgBouncer check](#no-pgbouncer-in-the-connection) runs immediately after connectivity, on both sides.
With `spec.preflight.requireSameMajorVersion: true`, the [major version gate](#major-version-gate) runs after it.
The [`pg_restore` check](#pg_restore-and-the-target-major) runs next, on every Migration.

Each selected extension in the source database must be installed on the target, or have a non-null default version in `pg_available_extensions`.
With `clone.dropIfExists: true` or `clone.allDatabases: true`, an installed extension also needs a default version on the target.
Restore may recreate the extension, or create it in a new database.
The extension include and exclude filters match exact names, and the exclusion applies last.
Table and schema filters do not affect extension selection.
`clone.skip: [extensions]` bypasses this check, while `extensionComments` does not.
A missing, malformed, or failed probe fails preflight.
An explicitly empty selection passes.
The check does not compare extension versions, install packages, grant installation privileges, or establish extension compatibility.

The `selected extension ownership` check covers target-installed extensions selected from the source when `dropIfExists` is true, or when the restore includes extension comments (the default).
It honours the extension include and exclude filters.
It omits source extensions with initdb-reserved object identifiers, such as the built-in `plpgsql`.
`pg_dump` does not emit the data definition language (DDL) or the comments of these extensions.
The migration role MUST be a superuser or have the privileges of the owning role, directly or through inherited membership (`pg_has_role(..., 'USAGE')`).
Ownership of the database alone is not sufficient.
Not covered: the extension ownership preflight on PostgreSQL 11 and older sources, which are outside the documented [E2E version matrix](https://github.com/ydixken/pgcopydb-operator/blob/main/CONTRIBUTING.md#e2e-tests).

> [!warning]
> The minimal failing spec is `spec.clone: {}` when the target already holds selected administrator-owned extensions.
> Without `dropIfExists`, `CREATE EXTENSION IF NOT EXISTS` does nothing, but the next `COMMENT ON EXTENSION` fails the ownership checks.
> To keep the other comments, use `clone.skip: [extensionComments]`.
> To suppress all comments, use `clone.noComments: true`.
> With `dropIfExists: true`, neither comment option prevents `DROP EXTENSION` failures.
> In that case, use an authorised migration role.
> As an alternative, use `clone.skip: [extensions]` and provide the required extensions on the target yourself.

For single-database migrations, three read-only grant probes then run on the target side:

- `CREATE` on the target database.
- `CREATE` on each source schema that already exists on the target.
  The probe honours the schema filters in `clone.filters`.
  `includeOnlyTables` narrows it to the schemas of those tables.
  The database-level probe covers the schemas that the restore has to create.
- Unless `dbProperties` is in `clone.skip`, whether `ALTER DATABASE ... SET` can run (database ownership through `pg_has_role`, or superuser).

All-databases clones replace these maintenance-database probes with the instance-wide checks below.
With `clone.ownerAfterRestore` set, two probes run before those three.
The named role MUST exist on the target.
The migration role MUST be able to `SET ROLE` to it, which `ALTER ... OWNER TO` requires.
A failed grant probe puts the exact `GRANT CREATE ...` statement in the condition message.
It adds a `superuserSecretRef` hint when [that field](#superuser-remediation-superusersecretref) could apply the grant.
The db-properties probe instead names its two remedies: membership in the owning role, or `clone.skip: [dbProperties]`.

Preflight does not probe ownership alignment (`clone.noOwner`) or the source-side SELECT and USAGE privileges.
A permission error from either fails the first attempt with reason `PermissionDenied` and does not spend the retry budget.
This is true only when the error is the terminal cause of the attempt in the log tail.
The log scan is best-effort.
When the scan misses the error, normal retries apply.

Source role:

- SELECT on every table and sequence that the migration copies, and USAGE on their schemas.
  Ownership of the objects covers all of it.
- Snapshot export needs no special privilege.
  Plain clones need no replication privilege at all.

Target role:

- CREATE on the target database.
- Ownership alignment: without `clone.noOwner`, `pg_restore` emits `ALTER OWNER`.
  That statement works only as superuser or as the owning role.
  The simplest non-superuser setup is the pattern that the e2e fixtures use.
  In that pattern, the migration connects as the role that owns every migrated object on both sides.
  Otherwise, set `clone.noOwner: true`.
  When the objects must finally belong to a role that the migration cannot connect as, add [`clone.ownerAfterRestore`](#ownership-after-restore-cloneownerafterrestore).
- `ALTER DATABASE ... SET` (the db-properties step) needs database ownership or superuser.
  If the target role has neither, add `dbProperties` to `clone.skip`.
- `TEMPORARY` on the target database, for the test commit of the [retry gate](#retries-and-snapshot-consistency).
  `PUBLIC` and the database owner hold it by default, so only a revoked default needs `GRANT TEMPORARY ON DATABASE <db> TO <role>`.
  Without it, the preflight only warns.
  A retry that runs the test commit then holds with reason `ProbeFailed`, and its message contains the `GRANT`.

Only these cases need a superuser:

- `clone.allDatabases: true` on both sides, even when the clone omits role passwords.
- `clone.roles: true` without `clone.noRolePasswords: true` (reads passwords from `pg_authid`).
- Extensions: most C extensions on the target, and any database whose superuser-installed extensions have configuration tables (a `pg_dump` limitation that filters cannot exclude).

### Row-level security

Row-level security that applies to the source migration role MUST NOT cover a table in scope.
The bundled runner reads with `row_security` off, as pg_dump does, so such a table fails the copy with SQLSTATE 42501.
Runners before `0.18.34.g7fddd6f` read with it on.
The policies then filter their COPY and their `compare data` alike.
The target gets only the rows that the role can see, and verification still passes.
The bundled runner also applies follow changes on the target with `row_security` off.
Thus, when a target policy would hide a row from an UPDATE or DELETE, the apply fails and does not skip the row.
Policies apply to the role when the table has row-level security enabled and the role does not own it (directly or through an inherited membership).
They also apply when the table has both row-level security enabled and `FORCE ROW LEVEL SECURITY`.
Row-level security without any policy hides every row from such a role.
Superusers and roles with `BYPASSRLS` are exempt.
pgcopydb reads each partition directly.
A query on a partition applies only the policies of that partition.
So the audit checks plain tables and partitions, and ignores policies on a partitioned parent.
It also skips tables that belong to an extension, such as `cron.job` from `pg_cron`, because pgcopydb excludes them from its table copy.

For every single-database Migration, preflight fails when such a table is in scope, and names the tables.
Fix it in one of three ways:

- Give the source migration role `BYPASSRLS` (`ALTER ROLE <role> BYPASSRLS`, which needs a superuser).
- Disable row-level security on those tables for the migration.
- Exclude them with `clone.filters`: `excludeTables`, `excludeSchemas`, or `excludeTableData`.

All-databases clones skip the check, because their migration role is a superuser.
The [`objects/rls_force`](coverage.md#postgresql-feature-coverage) coverage case pins the refusal for a table with `FORCE ROW LEVEL SECURITY`.

This audit and the follow audit for [unlogged tables](#live-migration-specfollowenabled-true) cover the tables that `clone.filters` keeps.
They match tables by exact `schema.table` name.
Each audit applies the include filters the way pgcopydb applies them to its own step.
The copy keeps a table only when it passes both lists:

- When you set `includeOnlySchemas`, the table's own schema is in it.
- When you set `includeOnlyTables`, the table's own name is in it.

So when `includeOnlyTables` names a partitioned table, the copy includes none of its partitions.
A partition in another schema also stays outside an `includeOnlySchemas` scope.
The follow publication includes a table when either list names the table or any of its partition ancestors.
This is because `FOR TABLE` on a partitioned table covers its partitions.
The audits cannot match these kinds of entry exactly: a `~` pattern, a quoted name, a table name without a schema, or an uppercase schema name.
Such an entry widens the audits instead.
It voids its include list and the include list of the publication, and the audits drop it from an exclude list.

### Large objects

pgcopydb copies every large object with its OID and content, whatever `clone.filters` selects.
`clone.skip: [largeObjects]` excludes all of them.

> [!caution]
> On the target, each large object belongs to the migration role, with no grants and no comment.
> `pg_dump` 17 and later keep the owner, grants and comment of a large object in the data section.
> The bundled runner ships such a `pg_dump`.
> pgcopydb restores only the pre-data and post-data sections.
> Unless `lo_compat_privileges` is on, a role that is neither a superuser nor a member of the migration role then gets `permission denied for large object`.
> Neither the clone nor `spec.verification` reports this error.

To restore this metadata after the clone:

1. Save this query as `lo-metadata.sql`:

    ```sql
    SELECT 'BEGIN; ' || string_agg(stmt, ' ' ORDER BY o, k) || ' COMMIT;'
      FROM (SELECT oid, 1, format('ALTER LARGE OBJECT %s OWNER TO %I;', oid, pg_get_userbyid(lomowner))
              FROM pg_largeobject_metadata
            UNION ALL
            SELECT m.oid, 2, format('GRANT %s ON LARGE OBJECT %s TO %s%s;', a.privilege_type, m.oid,
                                    CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END,
                                    CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END)
              FROM pg_largeobject_metadata m, aclexplode(m.lomacl) a
             WHERE a.grantee <> m.lomowner
            UNION ALL
            SELECT objoid, 3, format('COMMENT ON LARGE OBJECT %s IS %L;', objoid, description)
              FROM pg_description
             WHERE classoid = 'pg_catalog.pg_largeobject'::regclass) AS t(o, k, stmt)
     GROUP BY o::bigint / 1000
     ORDER BY o::bigint / 1000;
    ```

    Run it against the source database:

    ```sh
    psql "<source URI>" -XAt -v ON_ERROR_STOP=1 -f lo-metadata.sql > lo-metadata-apply.sql
    ```

2. Apply the statements on the target database as a superuser:

    ```sh
    psql "<target URI>" -X -v ON_ERROR_STOP=1 -f lo-metadata-apply.sql
    ```

Each `BEGIN; ... COMMIT;` group in `lo-metadata-apply.sql` is one transaction over at most 1000 large objects.
We use these groups because each statement locks its object until commit.
One transaction over every object can exhaust the lock table.
The statements are idempotent.
After a stop, run the file again.
Step 2 stops at a large object that the target lacks, such as one created on the source after the clone started.
That error repeats on every run.
Every owner and grantee MUST exist on the target first.
With `clone.allDatabases`, run both steps once per database.

### Ownership after restore (`clone.ownerAfterRestore`)

`clone.noOwner: true` leaves every restored object owned by the migration role.
`clone.ownerAfterRestore` names the role that MUST own them instead.
The restore still runs as the migration role.
After the worker exits, a `<name>-reown` Job transfers the objects to that role.
The field needs `noOwner: true`, does not combine with `clone.allDatabases`, and is immutable.
The CRD enforces all three rules.
Give the role name unquoted, exactly as PostgreSQL stores it.
The operator quotes it as an identifier wherever it reaches SQL, so names such as `kkp-db-medium-dev` work as they are.
The CRD accepts any name except one that contains a double quote or a control character, or is longer than 63 bytes.
We reject the double quote, because the operator would quote a self-quoted name such as `"db-admin"` a second time.
The result would name a role that does not exist.
The limit counts bytes, because PostgreSQL truncates a longer name to 63 bytes.
A name with non-ASCII characters reaches that limit before 63 characters.
See [Ownership after restore](../configuration.md#ownership-after-restore) for when to use it.

Preflight covers two of the requirements, before the clone grant probes above.
The role MUST exist on the target.
The migration role MUST be able to `SET ROLE` to it.
`ALTER ... OWNER TO` needs `SET ROLE`, and a membership that only inherits the privileges of the role does not supply it.
PostgreSQL 16 added `GRANT ... WITH SET FALSE`, which inherits the privileges without the `SET ROLE`.
Thus the probe is a real `SET ROLE` inside a rolled-back transaction, not a `pg_has_role(..., 'USAGE')` test.
`superuserSecretRef` remediates a refused `SET ROLE` with the `GRANT <owner> TO <migration role>` that the preflight composes on the server.

Two more requirements belong to the `<name>-reown` Job, because the restore has not created the schemas when preflight runs.
The Job checks both before it changes anything, and prints the exact `GRANT` when one is missing.
It skips both when the migration role is a superuser, and the inherited-privilege check below follows the same rule.

- The **migration role** needs `CREATE` on the target database, and only when the handover transfers at least one schema.
  The migration role needs this right, not the new owner, because `ALTER SCHEMA ... OWNER TO` checks the right of the current user to create schemas.
  `CREATE SCHEMA` makes the same check.
- The **new owner** needs `CREATE` on every schema that holds objects it receives and that it does not receive itself.
  A schema in the transfer set supplies the privilege through its own `ALTER`, which runs first.
  Only the schemas that the handover does not transfer need an explicit grant.
  `public` is the usual one.
  From PostgreSQL 15, it belongs to `pg_database_owner` and no longer grants `CREATE` to `PUBLIC`.

> [!important]
> The migration role also needs `USAGE` on every schema that the handover transfers.
> After the `ALTER SCHEMA`, the migration role still has to name the objects that remain inside that schema.
> Membership in the new owner supplies this right when the membership carries inheritance.
> Inheritance is the default for `GRANT <owner> TO <migration role>`, and thus also for the remediation of the preflight.
> The membership does not supply it when the migration role has the `NOINHERIT` attribute, or when its grant used `WITH INHERIT FALSE` (PostgreSQL 16 and later).
> Both cases pass the `SET ROLE` probe.
> So the Job checks inherited privileges as a third pre-check, but only when the handover transfers at least one schema.
> Before it changes anything, the Job prints the exact remediation for the version of the target.
> From PostgreSQL 16, inheritance is a property of the membership, and the remediation is `GRANT <owner> TO <migration role> WITH INHERIT TRUE`.
> Before PostgreSQL 16, inheritance is a role attribute, and the remediation is `ALTER ROLE <migration role> INHERIT`.
> `GRANT USAGE ON SCHEMA <schema> TO <migration role>` is not a working alternative.
> When a role owns the schema, a `USAGE` grant to that role collapses into its own ACL entry.
> The `ALTER SCHEMA ... OWNER TO` that the handover runs next rewrites that entry to the new owner, and this deletes the grant.
> Fix the inheritance instead.

The handover covers four object classes in the target database:

- Schemas.
- Relations: tables, partitions, sequences, views, materialized views, foreign tables.
- Routines: functions, procedures, aggregates.
- Types, including domains.

It does not change extension members, so the platform's own extensions keep their owner.
It does not touch large objects, publications, subscriptions, event triggers, foreign-data wrappers, operators, collations, text search objects, or statistics objects.
A sequence attached to a column by `serial` or `IDENTITY` gets no statement of its own, because PostgreSQL refuses to change its owner directly.
It follows its table, as array types, row types and multirange types follow the type they belong to.

> [!warning]
> The handover covers every schema, relation, routine and type in the target database that the migration role owns.
> It also covers the objects that this restore did not create.
> Nothing in the catalog records which objects a restore created.
> When the target database also holds objects that the role owns, use a migration role dedicated to the migration.

### No PgBouncer in the connection

The source and the target connection MUST reach PostgreSQL without PgBouncer in between, in any pool mode.
Preflight checks both sides, on one session each.
It compares the backend PID in the `\conninfo` of psql 18 with `pg_backend_pid()`.
The PID in `\conninfo` comes from the cancel key that the connection received.
PgBouncer issues a cancel key of its own, so the two PIDs differ behind it.
A direct connection or a TCP proxy such as HAProxy forwards the server's key, so the check passes.
pgpool-II forwards the server's key as well, so preflight does not detect it.
The failure names the side and both PIDs.
Point that side of the Migration directly at PostgreSQL.
Then create a new Migration.

A check that cannot tell fails instead of passing.
This occurs when the probe connection failed, when psql printed no backend PID (as psql older than 18 does), or when `pg_backend_pid()` returned none.
The check runs on the plain and the all-databases path.

### Major version gate

`spec.preflight.requireSameMajorVersion: true` fails preflight when the source and target PostgreSQL major versions differ.
The gate ignores minor versions.
It is off by default, because a cross-major upgrade is a main use of pgcopydb.
The gate runs on the plain and the all-databases path, and under `spec.dryRun` it reports like any other failed check.

Preflight asks each live server over the Migration's own source and target connections.
It reads `current_setting('server_version_num')` and divides it by 10000.
It does not trust the spec, the image tag, or the `server_version` text, because managed services add suffixes to that text (Debian or Aurora suffixes).
When preflight cannot read a side, the gate fails instead of passing.
A failed gate stops preflight.
No later check runs, and the version line is the line that preflight reports.

```yaml
spec:
  preflight:
    requireSameMajorVersion: true
```

### `pg_restore` and the target major

Preflight fails when the runner's `pg_restore` is version 17 or later and the target runs PostgreSQL 16 or older.
`pg_restore` 17 and later send `SET transaction_timeout = 0` on every target connection, without a check of the server version.
A server older than 17 rejects the setting.
pgcopydb then fails the pre-data restore on every attempt, before it copies any data, so retries never converge.
The check runs on every Migration, on the plain and the all-databases path.
Under `spec.dryRun`, it reports like any other failed check.

Preflight runs `pg_restore --version` on the binary that pgcopydb runs.
[Client tool versions](#client-tool-versions) tells how pgcopydb finds that binary.
We read that binary rather than the first `pg_restore` on `PATH`, so a custom image that points `PG_CONFIG` at older client tools passes.
The target's major comes from `server_version_num`, as in the [major version gate](#major-version-gate).
When preflight cannot parse the version output, or cannot read the target major, the check fails instead of passing.
A failed check stops preflight, so its line is the line that preflight reports.

Use a PostgreSQL 17 or later target, or a custom `spec.runner.image` whose `pg_restore` is version 16 or older.

## All databases

Both migration connections MUST name an existing maintenance database such as `postgres` and use a role with `rolsuper`.
The source role dumps every database and copies the roles.
The dump of the role passwords reads `pg_authid`.
The target role creates missing databases, restores roles, and runs ownership changes across every database.
Preflight refuses managed admin roles without `rolsuper`.
`superuserSecretRef` does not satisfy this requirement for a non-superuser migration role.
All-databases preflight ignores those remediation references and does not project their credentials into the pod.

pgcopydb excludes only `template0` and `template1`, so it clones the source `postgres` into the target `postgres`.
This mode implies roles, so `clone.roles: true` is redundant.
pgcopydb skips the roles that already exist on the target.
This mode honours `clone.noRolePasswords`, but that option does not relax the required privileges.

> [!warning]
> Target databases, `postgres` included, MUST NOT already hold the schema that the clone restores.
> Preflight lists existing target databases but does not check them for conflicting objects.
> This mode rejects `clone.dropIfExists`, `follow.enabled`, and `verification.data`.
> It supports `verification.schema`.

Preflight checks the superuser attributes of the migration roles.
It lists the source databases, and which of them already exist on the target.
Unless you skip extensions, it also checks the selected extensions across every source database.
The extension probe runs sequentially, so its work grows linearly with the database count.
It runs inside the 30-minute deadline of the preflight Job.
A failed database query fails preflight.
Preflight never reports success on an empty result.

The target extension check needs package availability, not installation in the maintenance database, because target databases may not exist yet.

The clone copies large objects and the user rows of extension configuration tables in every database, `postgres` included.
A retry copies every large object again.
Runners older than `0.18.129.gd7547b5` copy neither of them in this mode, and still exit 0.
See [client tool versions](#client-tool-versions).

> [!caution]
> In `postgres`, the configuration rows of `pg_cron` are its jobs in `cron.job` and their history in `cron.job_run_details`.
> So the target receives the schedule of the source.
> A target that runs `pg_cron` can run those jobs in addition to the source, from the moment they arrive.
> To exclude the extension and its rows, set `clone.filters.excludeExtensions: [pg_cron]` before the clone.
> Otherwise, unschedule the copied jobs on the target as soon as the clone finishes.

Filters and skips apply to every database, and job counts are global across databases.
The operator reports summed database sizes without per-database relation or catalog counters.
See [All databases configuration](../configuration.md#all-databases) and [the example](../examples/09-all-databases.yaml).

## Live migration (`spec.follow.enabled: true`)

Before the first attempt, and after the connectivity probes, the preflight Job also checks the requirements of this section:

- `wal_level`.
- Free-slot headroom.
- `wal_sender_timeout`, which only warns.
- The source role's REPLICATION attribute.
- EXECUTE on the origin functions.
- The `session_replication_role` SET privilege.
- An audit of every user table for a usable replica identity.
- An audit of the tables in scope for unlogged tables.

A failed check fails the Migration before any data moves.
The `Validated` condition message then gives the exact missing GRANT, setting, or table list.
A `wal_sender_timeout` below 60s never fails it.
Preflight passes and emits a `PreflightWarning` event.
When the failing side has no `superuserSecretRef`, the message adds a hint that names that field.
With `superuserSecretRef` set, [the preflight applies](#superuser-remediation-superusersecretref) the three grantable rights and does not fail.
Preflight does not cover the rest of the workload contract (no DDL, large objects, wal2json presence).
That part stays your job.

Source instance:

- `wal_level` MUST be `logical` (a change to it requires a server restart).
- One free replication slot per concurrent live Migration (`max_replication_slots`) and one free WAL sender (`max_wal_senders`).
- A replication slot keeps WAL until it is dropped.
  Budget disk for the migration window.
  `max_slot_wal_keep_size` caps that retention, but a slot that the cap invalidates kills the migration.
  The operator drops the slot at cutover, on abort, and on Migration deletion.
- `wal_sender_timeout` SHOULD be at its PostgreSQL default (60s) or higher, or `0` (disabled).
  A lower value ends pgcopydb's logical-decoding walsender whenever receive stalls that long, for example on an fsync or under CPU throttling.
  In our test runs on pgcopydb 0.18.74 and 0.18.39 runners, pgcopydb's receive livelocked during `clone --follow` at 5s.
  At 60s, the same runs completed.
  Runners from `0.18.76.gf9b328b` on, the bundled one included, send receive status within half of the timeout.
  In those runs, a build of the `0.18.76.gf9b328b` source kept one walsender session at 5s, and its clone finished ([#369](https://github.com/ydixken/pgcopydb-operator/issues/369)).
  A single receive stall longer than half the timeout still ends the session, so we still recommend 60s.
  CloudNativePG sets 5s by default for its own HA streaming.
  Raise it on the server.
  On CNPG, use `spec.postgresql.parameters` for that.
  Alternatively, raise it only for the connections of the migration.
  From PostgreSQL 12 on, the parameter is user-settable.
  The replication connection of pgcopydb uses the source URI, so `options=-c wal_sender_timeout=60s` in a source URI applies to it.
  That URI is a `uriSecretRef`, or a `secretRef` DB key that holds a URI, which keeps its query.
  Append the parameter percent-encoded to the URI query, after `?` or `&`: `options=-c%20wal_sender_timeout%3D60s`.
  libpq rejects the unencoded form with `extra key/value separator "=" in URI query parameter: "options"`.
  Before PostgreSQL 12, the server rejects that override with `parameter "wal_sender_timeout" cannot be changed now`.
  Every connection then fails, and preflight fails too.
  Preflight reads the value over that same source URI, so an override in the URI counts.
  Preflight warns below 60s.
  The warning names the URI remedy only for a PostgreSQL 12 or later source.

Source role:

- MUST have the `REPLICATION` attribute (or be superuser): `ALTER ROLE app REPLICATION`.
  Without it, slot creation fails with "permission denied to start WAL sender".
  On CloudNativePG sources, this is declarative.
  A role in the Cluster's `managed.roles` with `replication: true` reconciles to the attribute (verified live).
  CNPG does not manage its bootstrap owner role by default.
  So declare that role under `managed.roles`, or run the `ALTER ROLE` once.
- Publication: pgcopydb auto-creates a publication for the migrated tables (named after the slot) and drops it during cleanup.
  The role MUST have CREATE on the source database and own every published table.
  Alternatively, pre-create a publication (`FOR ALL TABLES` needs a superuser).
  Then point `spec.follow.publication` at it.
  pgcopydb then does not touch it.
  On retries, the operator guards an automatic `pgoutput` publication, and never touches one named in `spec.follow.publication`.
  See [Publication retry failures](../troubleshooting.md#publication-retry-failures) for that guard and its limits.
- Plugin: `pgoutput` (default) and `test_decoding` ship with PostgreSQL.
  Before you select `wal2json`, it MUST be installed on the source and named in the source's `output_plugin_libraries`.
  That parameter defaults to `pgoutput, test_decoding`.
  Since the 2026-08-13 minor releases (14.24, 15.19, 16.15, 17.11, 18.6, which close [CVE-2026-6471](https://www.postgresql.org/support/security/CVE-2026-6471/)), the server refuses every output plugin that the list omits.
  A new entry needs a config reload, not a restart.
  The preflight cannot check the installation.
  A logical decoding plugin is a bare shared library with no catalog entry to query.
  A plugin that the source will not load fails the first attempt at slot creation.
  When the list omits the plugin, the error is `library "wal2json" may not be used as an output plugin`.
  When the library itself is absent, it is `could not access file "wal2json"`.

Target role:

- MUST be able to `SET session_replication_role` (the apply session runs with it set to `replica` to keep triggers and foreign keys quiet during replay).
  A superuser can always do this.
  On PostgreSQL 15+, grant it explicitly: `GRANT SET ON PARAMETER session_replication_role TO app;`.
  WARNING: with pgcopydb 0.18, a role without this privilege does not fail the migration.
  Apply silently replays nothing and reports success.
  Only a row count comparison exposes the loss.
  Grant it before every live migration.
- MUST have EXECUTE on the `pg_replication_origin_*` catalog functions (superuser has it implicitly).
  The e2e fixtures use this non-superuser grant:

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

  The first runner attempt exposes a missing grant.
  That attempt dies in pgcopydb's setup cleanup with "permission denied for function pg_replication_origin_drop", before any data moves.

Schema and workload contract:

- Every table that receives UPDATE or DELETE during the migration window MUST have a primary key or a replica identity (`REPLICA IDENTITY USING INDEX ...` or `REPLICA IDENTITY FULL`).
  With `pgoutput`, a data manipulation language (DML) statement on a published table without one fails on the source at write time.
  That breaks the application, not just the migration.
  The preflight audits all user tables for this and fails on offenders.
  It ignores `clone.filters`, because a filtered table can still take writes.
  You MAY acknowledge a table that stays read-only or insert-only during the window in `spec.follow.allowMissingReplicaIdentity`.
  Use schema-qualified names, exactly as the preflight prints them.
  `["*"]` acknowledges every offender.
  The acknowledgement downgrades these tables to a warning.
  To replicate only INSERTs, use [`spec.follow.insertOnlyTables`](../configuration.md#partial-table-policies) instead.
  That policy deliberately omits UPDATE, DELETE, and TRUNCATE from replication.
  It does not change the acknowledgement option.
  The bundled runner keeps `REPLICA IDENTITY USING INDEX` and `REPLICA IDENTITY FULL` on the target as the source has them.
  Where the source uses `USING INDEX`, stock pgcopydb uses the default identity instead.
- pgcopydb replicates `TRUNCATE`.
  It truncates the target at the same point in the change stream.
  With `pgoutput` and `test_decoding`, the bundled runner truncates, in one statement, every table that the source statement reached, with `RESTART IDENTITY` when the source used it.
  It does not replay `CASCADE`, but the change already lists the tables that a cascade reached on the source.
  A target table that references a truncated table, and was not truncated on the source, fails the replay.
  With `wal2json`, a multi-table `TRUNCATE` SHOULD NOT run during the window.
  pgcopydb then truncates only its last table and never restarts identity.
  Runners older than `0.18.70.gbb8dbfc` truncate only the first table under `pgoutput`.
  With these runners, run one `TRUNCATE` per table.
  Tables linked by a foreign key cannot be truncated one at a time, so on those runners they SHOULD NOT be truncated during the window.
- The bundled runner replicates `COPY FROM` and other multi-insert writes in full.
  You SHOULD NOT use a runner older than `0.18.70.gbb8dbfc` when the source runs `COPY FROM` during the window.
  Such a runner keeps one row per heap page of each such write, under every plugin.
- On a table with `REPLICA IDENTITY FULL` and no key, the bundled runner changes one matching row per UPDATE or DELETE.
  It matches each NULL column of the old row with `IS NULL`.
  The bundled runner does both under every plugin.
  That covers the NULL columns that `test_decoding` omits from the old row, and an old row whose columns are all NULL.
  You SHOULD NOT use a runner older than `0.18.39.g93eda1d` with `test_decoding` if such a table holds rows that are equal except where one is NULL.
  Such a runner matches only the columns that `test_decoding` sends, so it can change the wrong row.
  Those runners also skip an UPDATE or DELETE whose old row is all NULL, under every plugin, and the target keeps the row.
  On a Citus-distributed target, the one-row form fails the apply with an error.
- pgcopydb does not replicate DDL, and DDL MUST NOT run during the migration window.
  Pre-create the upcoming partitions before you start.
  A change that uses a column that the target lacks stops the apply.
  The worker log then shows `[42703] ERROR:  column ... does not exist` (two spaces after `ERROR:`).
  Every retry would stop at the same change, so the Migration fails with `SchemaDrift` on that attempt.
  The [`limitations/ddl_add_column`](coverage.md#postgresql-feature-coverage) coverage case pins this.
- Unlogged tables MUST NOT be in scope without an explicit partial policy.
  They write no WAL, so logical decoding never sees their changes.
  With the automatic publication, pgcopydb's `CREATE PUBLICATION ... FOR TABLE` refuses them and the first attempt fails before any copy.
  With `spec.follow.publication` or `wal2json`, their changes during the window are lost, and only `spec.verification.data` notices.
  The preflight fails on unlogged tables in scope.
  [Row-level security](#row-level-security) describes that scope.
  There is one difference.
  pgcopydb's publication honours `includeOnlySchemas` only together with `includeOnlyTables`.
  So `includeOnlySchemas` alone still publishes every table outside the exclusions, and the audit checks all of them.
  Run `ALTER TABLE ... SET LOGGED` on the source, or exclude the tables with `excludeTables` or `excludeSchemas`.
  Together with `includeOnlySchemas`, use `excludeTables`, because `excludeSchemas` cannot be combined with it.
  `excludeTableData` does not help, because pgcopydb still publishes those tables.
  Alternatively, select each table in [`spec.follow.unloggedTables`](../configuration.md#partial-table-policies).
  `SchemaOnly` restores an empty table.
  `CopyOnce` copies its initial rows.
  Neither policy replicates later changes or changes the source table to LOGGED.
  These policies use operator-managed publications and retain the default refusal for other UNLOGGED tables.
  The [`limitations/unlogged_writes`](coverage.md#postgresql-feature-coverage) coverage case pins the refusal.
- Large-object changes during the window are not replicated (base copy only).
  A large object written or unlinked during the window keeps its base-copy content on the target.
  A large object created during the window does not exist there, even when a replicated row references it.
  Verification cannot see this.
  `compare data` covers tables, not `pg_largeobject`, so `Verified` stays `True`.
  The [`limitations/large_object_change`](coverage.md#postgresql-feature-coverage) coverage case pins this.
- Sequences need no action: pgcopydb re-syncs them automatically after cutover.

## Superuser remediation (`superuserSecretRef`)

Each side of the Migration MAY carry `superuserSecretRef`.
It is a Secret in the same convention as [`secretRef`](../configuration.md#credentials).
Its `USER` and `PW` keys name a superuser on that same endpoint.
When present, the `URL`/`URL_EXTERNAL` keys MUST match the endpoint of the connection.
A mismatch fails the preflight by name.
For a value with `:port`, the comparison includes the port.
With a `secretRef` primary, the primary's `endpoint` field decides between the internal and external URL, so both connections always name the same server.
The preflight probes the superuser connection with the same retries as the primaries, and checks `rolsuper`.
A role without `rolsuper` only warns (a `PreflightWarning` event), because managed-Postgres admin roles such as `rds_superuser` can hold the grant rights without the attribute.
The preflight then applies the rights that the regular role is missing, with exactly these statements:

- `GRANT CREATE ON DATABASE <db> TO <role>` on the target, when the clone probe finds it missing (single-database migrations).
- `GRANT CREATE ON SCHEMA <schema> TO <role>` on the target, one grant per restore-target schema that the role cannot create in (single-database migrations).
- `GRANT <owner> TO <role>` on the target, when the migration role cannot `SET ROLE` to `clone.ownerAfterRestore` (single-database migrations).
  The membership carries `SET` on every supported version, and the handover needs that part.
- `ALTER ROLE <role> REPLICATION` on the source (follow only).
- `GRANT EXECUTE ON FUNCTION pg_replication_origin_* ...` on the target, one grant per missing function (follow only).
- `GRANT SET ON PARAMETER session_replication_role TO <role>` on the target (PostgreSQL 15+, follow only).
  On older targets, the grant fails loudly.

The preflight re-checks and logs every applied statement in its output.
One `PreflightRemediated` event per tier (clone rights, follow rights) on the Migration lists the statements of that tier.
Under [`spec.dryRun`](../operations/lifecycle.md#dry-run), the preflight checks the superuser connection and applies nothing.
It prints each statement it would apply as `would-remediate:` or `would-remediate-clone:`, and one `PreflightWouldRemediate` event per tier lists them.
It runs none of them.
So a statement that would fail passes the dry run and fails the real preflight.
An example is a grant that the `superuserSecretRef` user has no right to give.
Applied grants stay in place and are never reverted.
They are the same grants that you would run by hand.
Remediation never changes schema objects or data.
It only grants rights.
It never touches replica identity, `wal_level`, plugin installation, database ownership, or extension ownership (the ownership probes only report remedies).
Only preflight uses the remediation credentials.
pgcopydb uses the primary migration connections, which MUST themselves be superusers for all-databases clones.
There is one restriction: the superuser connection reuses the URI of the primary connection.
A `uriSecretRef` primary that holds a conninfo-style `key=value` connection string cannot host it.
Preflight rejects such a primary by name.
Use the URI form.
The reuse extends to TLS transport settings, including any client certificate.
When the server maps certificate identities to roles, the certificate cannot present the superuser.
In that case, use password auth for the superuser connection.

## Retries and snapshot consistency

The operator retries a failed attempt with `pgcopydb clone --resume --not-consistent` on the same work volume.
The retry skips finished tables, and copies interrupted tables again from scratch.
Each table's COPY is a single transaction, so a killed attempt leaves no partial rows.
Runners older than `0.18.112.g4c61e97` truncate a split table on a retry without `clone.allDatabases` and skip its finished parts.
Their rows are then lost ([client tool versions](#client-tool-versions)).
Runners older than `0.18.129.gd7547b5` can also fail every retry after an attempt died while it built the constraints of a table.
Without `clone.allDatabases`, they can also skip an unfinished large-object or extension configuration step ([client tool versions](#client-tool-versions)).
`0.18.129.gd7547b5` still fails every retry after an attempt died while it added a constraint.
The same occurs when the attempt died after it added a constraint but before it recorded that constraint ([client tool versions](#client-tool-versions)).
The retry needs `--not-consistent`, because the exported snapshot of the first attempt dies with its session.
A plain `--resume` fails before it touches any data ("snapshot ... does not exist").
A permission error, a full work volume, or a change that names a column that the target lacks stops the `--resume` retry.
The Migration then ends as `PermissionDenied`, `DiskFull`, or `SchemaDrift` (see [Retries and resume](../operations/lifecycle.md#retries-and-resume)).

The trade-off is that re-copied tables read a fresh snapshot.
Thus a retried clone of a source that still takes writes is not one single point in time across tables.
If that matters, stop writes across the retry window, or delete and recreate the Migration for a fresh consistent copy.
Follow migrations replay every change since the slot's consistent point on top of the base copy, so their tables need the slot's own snapshot.
On runner `0.18.104.g362a559` and later, a retry that creates the slot copies under the snapshot of the slot.
A retry that finds the slot from an earlier attempt and still has tables to copy cannot resume.
That runner refuses it.
When `spec.clone.dropIfExists` is true, the operator then restarts the attempt from the beginning.
Otherwise, it fails the Migration as `SnapshotLost` (see [Retries and resume](../operations/lifecycle.md#retries-and-resume)).
Older runners copy the remaining tables of either retry under a newer snapshot.
Apply can then stop on a duplicate key or leave the target diverged ([client tool versions](#client-tool-versions)).
The drain verification gate at cutover and `spec.verification.data` are the checks that catch divergence.

Before every retry, and before the attempt that a resume starts, the [retry gate](../operations/lifecycle.md#the-retry-gate) checks the target.
It holds the attempt until all of these conditions are true:

- The pod of the previous worker has ended.
- The target database has no session of the migration role under a pgcopydb name.
- Where the gate runs a test commit, that commit returns within 5 seconds.
  Under synchronous replication, the test commit needs the confirmation of a synchronous standby.

The census distinguishes the sessions of this Migration from other sessions by role, database, and name.
So these rules apply:

- Two Migrations that write one target database MUST use different migration roles.
- A Migration with `clone.allDatabases` MUST NOT share its migration role with another Migration on that instance, because its census covers every database.
- The target URI SHOULD NOT set `application_name`.
  pgcopydb names every connection `pgcopydb[<pid>] <process>`.
  A URI parameter overrides that name, and the gate then counts every session of the role.

With a shared role, the sessions of another Migration can hold the retries of this Migration for as long as they run.
A shared role can also exempt an attempt that should count.
Preflight fails a target behind PgBouncer.
So a pooler reaches the retry gate only when preflight did not see it, for example a pooler that joined the target path later.
Through a transaction-mode pooler, a retry can then wait until the pooler closes its idle server connections.
A server connection keeps the name of the last client it served, so the census counts it as a leftover.
PgBouncer closes it after `server_idle_timeout`, or after `server_lifetime` when the idle timeout is off.

## Cutover

Cutover freezes the stream at the current LSN of the source and drains it.
Anything written after that point does not reach the target.
Writes to the source MUST be stopped before `spec.cutover.approved: true` in Manual mode, and before the lag drops under `follow.maxCatchupLag` in Automatic mode.
Automatic mode is for sources that are already quiesced.
