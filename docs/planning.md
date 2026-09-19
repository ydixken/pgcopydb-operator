# Migration planning checklist

Answer these questions before you create a `Migration` resource.
The operator's preflight covers only the items in the coverage notes below.
A `Partial` or `Not checked` item remains an operator decision.

Start with the [operator prerequisites](reference/prerequisites.md).
When follow mode is enabled, read the [live migration runbook](operations/live-migration.md).
When a preflight check fails, use the [troubleshooting guide](troubleshooting.md).

pgcopydb is a migration tool, not a backup system.

## Migration checklist

1. Which databases, schemas, tables, roles, ACLs, extensions, tablespaces, settings, and large objects must move, and which must stay behind?
2. What are the source and target PostgreSQL versions, and is the target compatible with the extensions, types, collations, and locales you need?
3. What downtime, RPO, RTO, maintenance window, load impact, and acceptance criteria did you agree?
4. Where does pgcopydb run, can it keep TLS-verified connections to both endpoints, and which roles and credential process does it use?
5. What are the database size, largest relations, write and WAL rates, largest transaction, and growth?
   What connection, compute, I/O, network, and work-disk capacity is available?
6. Does the agreed downtime justify follow mode, and did a plain clone rehearsal measure copy time and resource use?
7. Is logical decoding ready: logical WAL, slot and WAL sender capacity, plugin availability, HBA access, retention limits, and slot behavior after failover?
8. Who stops source writes, confirms quiescence, sets the end position, waits for replay, switches clients, and authorizes cutover?
9. Which validation checks must pass, when is rollback still safe, who decides, and when do you clean up the replication resources?
10. What backup system protects the source, and when was the last successful test restore or point-in-time recovery?
11. Can you create an isolated rehearsal environment from representative production data?
    Does it block every write path to production and apply approved controls for masked data, access, retention, and teardown?

## Operator preflight coverage

The controller exposes one aggregate gate through `Validated=Unknown` with `PreflightRunning`, `Validated=False` and `Failed=True` with `PreflightFailed`, or `Validated=True` with `SpecValid`.
Individual checks appear as `ok:` log lines.

1. Partial.
   The API validates the filters, but preflight does not check the declared scope against your intent.
2. Partial.
   Preflight checks the selected extensions and their target ownership.
   It does not check extension versions, package installation, installation privileges, collations, encodings, server-version equality, or wider source-to-target compatibility.
3. Partial outside preflight.
   `follow.maxCatchupLag` and `cutover.mode` configure behavior, but preflight does not check downtime, RPO, RTO, or acceptance criteria.
4. Partial.
   Preflight probes connectivity, clone rights, superuser attributes, and the follow grants.
   See the [operator prerequisites](reference/prerequisites.md) for each check.
   Preflight does not audit TLS policy, every ownership and publication right, or whether the target schemas are empty.
   A `superuserSecretRef` role without `rolsuper` only logs a warning, but an all-databases migration connection without `rolsuper` fails preflight.
5. Partial.
   A pending pod or an unbound PVC can keep the status at `PreflightRunning`.
   Preflight does not measure data size, free space, throughput, connection headroom, compute, I/O, network, WAL growth, or work-disk capacity.
6. Partial.
   `spec.follow.enabled` adds the follow checks.
   The operator does not choose between clone and follow, and it does not verify that a rehearsal ran.
7. Partial.
   Preflight covers logical WAL, replication slot headroom, and the source replication attribute.
   It does not check WAL sender headroom, plugin installation, WAL retention budget, or slot behavior after failover.
8. Partial outside preflight.
   The API has cutover mode and approval fields.
   Preflight does not name an owner, verify that source writes stopped, or check the maintenance window.
9. Not checked by preflight.
   The runtime can drain replay, run comparisons, and clean the replication resources.
   Preflight does not check acceptance tests, rollback criteria, or cleanup timing.
10. Not checked.
    Clone-right checks are not backup or restore-readiness checks.
    The operator does not check backup recency, PITR, or restore drills.
11. Not checked.
    Product E2E tests do not prove that your rehearsal used representative data or enough isolation controls.

## Official references

Read the pgcopydb [clone](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_clone.html) and [follow](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_follow.html) command references.
Review pgcopydb [concurrency](https://pgcopydb.readthedocs.io/en/latest/concurrency.html) and [filter configuration](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_config.html).
Read PostgreSQL's [logical replication configuration](https://www.postgresql.org/docs/current/logical-replication-config.html), [logical replication restrictions](https://www.postgresql.org/docs/current/logical-replication-restrictions.html), [backup and restore](https://www.postgresql.org/docs/current/backup.html), and [`pg_verifybackup`](https://www.postgresql.org/docs/current/app-pgverifybackup.html) references.
