# Migration planning checklist

A PostgreSQL migration is a change project, not only a `Migration` resource. Use this checklist to agree the scope, compatibility, operating plan, capacity, replication contract, cutover, recovery, and rehearsal before creating the resource.

The operator's preflight checks only the items listed in the coverage notes below. A `Partial` or `Not checked` item remains an operator decision. Start with the [operator prerequisites](reference/prerequisites.md), read the [live migration runbook](operations/live-migration.md) when follow mode is enabled, and use the [troubleshooting guide](troubleshooting.md) when a preflight check fails.

pgcopydb is a migration tool, not a backup system.

## Migration checklist

1. Which databases, schemas, tables, roles, ACLs, extensions, tablespaces, settings, and large objects must move or be excluded?
2. What are the source, target, pgcopydb, pg_dump, and pg_restore versions, and is the target compatible with required extensions, types, collations, and locales?
3. What downtime, RPO, RTO, maintenance window, load impact, and acceptance criteria are agreed?
4. Where will pgcopydb run, can it maintain TLS-verified connections to both endpoints, and which least-privilege roles and credential process will it use?
5. What are the database size, largest relations, write and WAL rates, largest transaction, growth, and available connection, compute, I/O, network, and work-disk capacity?
6. Does the downtime requirement justify follow mode, and has a plain clone rehearsal established copy time and resource use?
7. Is logical decoding ready, including logical WAL, slot and WAL sender capacity, plugin availability, HBA access, retention limits, and slot behavior after failover?
8. Do tables receiving updates or deletes have suitable replica identity, and are RLS, triggers, and target-side writers accounted for?
9. Who stops source writes, confirms quiescence, sets the end position, waits for replay, switches clients, and authorizes cutover?
10. Which validation checks must pass, when is rollback still safe, who decides, and when are replication resources cleaned up?
11. What backup system protects the source, and when was the last successful test restore or point-in-time recovery?
12. Can you create an isolated rehearsal environment from representative production data with no production write path and approved masking, access, retention, and teardown controls?

## Operator preflight coverage

The controller exposes one aggregate gate through `Validated=Unknown` with `PreflightRunning`, `Validated=False` and `Failed=True` with `PreflightFailed`, or `Validated=True` with `SpecValid`.
Individual checks appear as `ok:` log lines.

1. Partial.
   API filter validation and `clone rights schemas` operate on declared scope, but preflight does not confirm operator intent.
2. Not checked.
   Preflight does not query server or client versions, extensions, collations, encodings, or source-to-target compatibility.
3. Partial outside preflight.
   `follow.maxCatchupLag` and `cutover.mode` configure behavior, but preflight does not validate downtime, RPO, RTO, or acceptance criteria.
4. Partial.
   Exact checks include `connectivity source`, `connectivity target`, `clone rights database`, `clone rights schemas`, `clone rights db-properties` when enabled, `superuser source connected` when applicable, `superuser source verified` when applicable, `superuser target connected` when applicable, `superuser target verified` when applicable, and follow-specific `source replication attribute`, `target origin function grants`, and `target session_replication_role`.
   A connected role without `rolsuper` emits the corresponding warning instead of a verified line.
   Preflight does not audit TLS policy or all required ownership and publication rights.
5. Partial.
   Scheduling and PVC binding problems may keep status at `PreflightRunning`, but preflight does not measure data size, free space, throughput, connection headroom, compute, I/O, network, WAL growth, or work-disk capacity.
6. Partial.
   `spec.follow.enabled` chooses the follow check set, but the operator does not recommend clone versus follow or verify that a rehearsal occurred.
7. Partial.
   Exact checks are `source wal_level logical`, `replication slot headroom`, and `source replication attribute`.
   Preflight does not check WAL sender headroom, plugin installation, WAL retention budget, or slot behavior after failover.
8. Partial.
   Exact checks are `replica identity audit` and `target session_replication_role`.
   Preflight does not audit RLS, trigger behavior, or target-side writers.
9. Partial outside preflight.
    The API has cutover mode and approval fields, but preflight does not identify an owner, verify source writes stopped, or validate a maintenance window.
10. Not checked by preflight.
    Runtime may drain replay, run optional comparisons, and clean replication resources, but preflight does not validate acceptance tests, rollback criteria, or cleanup timing.
11. Not checked.
    Clone-right checks are not backup or restore-readiness checks, and the operator does not inspect backup recency, PITR, or restore drills.
12. Not checked.
    Product E2E tests do not prove that an operator rehearsal used representative data or adequate isolation controls.

## Official references

Read the pgcopydb [clone](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_clone.html) and [follow](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_follow.html) command references.
Review pgcopydb [concurrency](https://pgcopydb.readthedocs.io/en/latest/concurrency.html) and [filter configuration](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_config.html).
Read PostgreSQL's [logical replication configuration](https://www.postgresql.org/docs/current/logical-replication-config.html), [logical replication restrictions](https://www.postgresql.org/docs/current/logical-replication-restrictions.html), [backup and restore](https://www.postgresql.org/docs/current/backup.html), and [`pg_verifybackup`](https://www.postgresql.org/docs/current/app-pgverifybackup.html) references.
