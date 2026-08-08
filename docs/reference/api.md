# API Reference

## Packages
- [pgcopydb-operator.io/v1beta1](#pgcopydb-operatoriov1beta1)


## pgcopydb-operator.io/v1beta1

Package v1beta1 contains API Schema definitions for the  v1beta1 API group.

### Resource Types
- [Migration](#migration)



#### CloneOptions



CloneOptions maps the pgcopydb clone surface. All fields are optional; zero
values mean "use the pgcopydb default".



_Appears in:_
- [MigrationSpec](#migrationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `tableJobs` _integer_ | tableJobs is the number of concurrent table COPY workers (pgcopydb --table-jobs). |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `indexJobs` _integer_ | indexJobs is the number of concurrent CREATE INDEX workers (--index-jobs). |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `restoreJobs` _integer_ | restoreJobs is pg_restore --jobs (--restore-jobs); 0 follows indexJobs. |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `largeObjectsJobs` _integer_ | largeObjectsJobs is the number of concurrent large-object workers. |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `splitTablesLargerThan` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#quantity-resource-api)_ | splitTablesLargerThan enables same-table concurrency for tables at or<br />above this size (--split-tables-larger-than), rendered to bytes. |  | Optional: \{\} <br /> |
| `splitMaxParts` _integer_ | splitMaxParts caps the number of parts per table (--split-max-parts). |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `estimateTableSizes` _boolean_ | estimateTableSizes bases split decisions on pg_class page-count<br />estimates instead of exact size queries (--estimate-table-sizes). To<br />refresh those estimates pgcopydb first runs vacuumdb --analyze-only<br />(with tableJobs workers) on the SOURCE; add "analyze" to skip to leave<br />the source untouched and trust its existing statistics. |  | Optional: \{\} <br /> |
| `dropIfExists` _boolean_ | dropIfExists issues pg_restore --clean --if-exists on the target. |  | Optional: \{\} <br /> |
| `roles` _boolean_ | roles copies roles before the clone (--roles). Needs superuser on the<br />source unless noRolePasswords is also set. |  | Optional: \{\} <br /> |
| `noRolePasswords` _boolean_ | noRolePasswords dumps roles without passwords (--no-role-passwords),<br />avoiding the superuser requirement of roles. |  | Optional: \{\} <br /> |
| `noOwner` _boolean_ | noOwner skips ALTER OWNER on restore (--no-owner). |  | Optional: \{\} <br /> |
| `noACL` _boolean_ | noACL skips GRANT/REVOKE on restore (--no-acl). |  | Optional: \{\} <br /> |
| `noComments` _boolean_ | noComments skips COMMENT statements (--no-comments). |  | Optional: \{\} <br /> |
| `noTablespaces` _boolean_ | noTablespaces skips tablespace selection (--no-tablespaces). |  | Optional: \{\} <br /> |
| `useCopyBinary` _boolean_ | useCopyBinary uses COPY WITH (FORMAT BINARY) (--use-copy-binary). |  | Optional: \{\} <br /> |
| `failFast` _boolean_ | failFast stops the whole run on the first failed child (--fail-fast). |  | Optional: \{\} <br /> |
| `skip` _[SkipOption](#skipoption) array_ | skip lists base-copy sections to skip. |  | Enum: [largeObjects extensions extensionComments collations vacuum analyze dbProperties ctidSplit] <br />Optional: \{\} <br /> |
| `filters` _[Filters](#filters)_ | filters is rendered to the pgcopydb --filters INI file. |  | Optional: \{\} <br /> |


#### CloneProgress



CloneProgress mirrors pgcopydb list progress --json into status.



_Appears in:_
- [MigrationStatus](#migrationstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `tablesTotal` _integer_ |  |  | Optional: \{\} <br /> |
| `tablesDone` _integer_ |  |  | Optional: \{\} <br /> |
| `indexesTotal` _integer_ |  |  | Optional: \{\} <br /> |
| `indexesDone` _integer_ |  |  | Optional: \{\} <br /> |
| `bytesTotal` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#quantity-resource-api)_ | bytesTotal is the total bytes to copy, as reported by pgcopydb. |  | Optional: \{\} <br /> |
| `bytesDone` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#quantity-resource-api)_ | bytesDone is the bytes copied so far. |  | Optional: \{\} <br /> |


#### CutoverMode

_Underlying type:_ _string_

CutoverMode picks who pulls the trigger.

_Validation:_
- Enum: [Manual Automatic]

_Appears in:_
- [CutoverSpec](#cutoverspec)

| Field | Description |
| --- | --- |
| `Manual` | CutoverManual waits for spec.cutover.approved.<br /> |
| `Automatic` | CutoverAutomatic cuts over as soon as the migration is caught up.<br /> |


#### CutoverSpec



CutoverSpec controls when replication stops and the migration finalizes.
Cutover means: writes to the source MUST already be stopped, the stream is
frozen at the source's current LSN, drained, and sequences re-synced.



_Appears in:_
- [MigrationSpec](#migrationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[CutoverMode](#cutovermode)_ | mode selects Manual (default) or Automatic cutover. | Manual | Enum: [Manual Automatic] <br />Optional: \{\} <br /> |
| `approved` _boolean_ | approved triggers the cutover in Manual mode. Mutable. Setting it back<br />to false after the cutover started has no effect. |  | Optional: \{\} <br /> |


#### Filters



Filters maps the pgcopydb --filters INI sections. pgcopydb rejects some
combinations (for example include-only-table with exclude-table); those are
enforced by CEL below and surfaced as validation errors.



_Appears in:_
- [CloneOptions](#cloneoptions)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `includeOnlyTables` _string array_ |  |  | Optional: \{\} <br /> |
| `excludeTables` _string array_ |  |  | Optional: \{\} <br /> |
| `includeOnlySchemas` _string array_ |  |  | Optional: \{\} <br /> |
| `excludeSchemas` _string array_ |  |  | Optional: \{\} <br /> |
| `excludeIndexes` _string array_ |  |  | Optional: \{\} <br /> |
| `excludeTableData` _string array_ |  |  | Optional: \{\} <br /> |
| `includeOnlyExtensions` _string array_ |  |  | Optional: \{\} <br /> |
| `excludeExtensions` _string array_ |  |  | Optional: \{\} <br /> |


#### FollowOptions



FollowOptions enables live migration: clone under a replication slot, then
stream and apply changes until cutover (pgcopydb clone --follow).



_Appears in:_
- [MigrationSpec](#migrationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ | enabled turns the migration into a live one. |  | Optional: \{\} <br /> |
| `plugin` _string_ | plugin is the logical decoding plugin (--plugin). | pgoutput | Enum: [pgoutput wal2json test_decoding] <br />Optional: \{\} <br /> |
| `slotName` _string_ | slotName overrides the replication slot name (--slot-name). Empty means<br />a generated name unique to this Migration; set it only when fanning<br />several migrations out of one source instance deliberately. The pattern<br />is PostgreSQL's own slot-name charset; the operator relies on it when<br />interpolating the name into SQL (origin verification, retry cleanup). |  | MaxLength: 63 <br />Pattern: `^[a-z0-9_]+$` <br />Optional: \{\} <br /> |
| `publication` _string_ | publication names a pre-created publication (--publication); empty lets<br />pgcopydb create and drop its own. |  | Optional: \{\} <br /> |
| `wal2jsonNumericAsString` _boolean_ | wal2jsonNumericAsString makes wal2json emit numeric values as JSON<br />strings (--wal2json-numeric-as-string), preserving precision a JSON<br />number would lose. Only meaningful with plugin wal2json; admission<br />rejects it under any other plugin. |  | Optional: \{\} <br /> |
| `replayNoOpUpdates` _boolean_ | replayNoOpUpdates replays UPDATEs that change no columns<br />(--replay-no-op-updates), needed when target triggers must fire. |  | Optional: \{\} <br /> |
| `allowMissingReplicaIdentity` _string array_ | allowMissingReplicaIdentity acknowledges tables that the preflight<br />replica-identity audit would otherwise fail on. Entries are<br />schema-qualified table names exactly as the preflight prints them<br />(schema.table, unquoted); the single entry "*" acknowledges every<br />offender. Acknowledged tables are reported as a warning instead of<br />failing the Migration. The risk stays: UPDATE or DELETE on such a<br />table during the migration window fails on the source at write time,<br />so acknowledge only tables that are read-only or insert-only while<br />the migration runs. Immutable with the rest of follow. |  | Optional: \{\} <br /> |
| `maxCatchupLag` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#quantity-resource-api)_ | maxCatchupLag is the replication lag under which the migration counts<br />as caught up (the CaughtUp condition and Automatic cutover trigger). | 16Mi | Optional: \{\} <br /> |


#### Migration



Migration is the Schema for the migrations API.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `pgcopydb-operator.io/v1beta1` | | |
| `kind` _string_ | `Migration` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  | Optional: \{\} <br /> |
| `spec` _[MigrationSpec](#migrationspec)_ | spec defines the desired state of Migration |  | Required: \{\} <br /> |
| `status` _[MigrationStatus](#migrationstatus)_ | status defines the observed state of Migration |  | Optional: \{\} <br /> |


#### MigrationPhase

_Underlying type:_ _string_

MigrationPhase is a human-facing summary derived from conditions. It exists
for the printer column only; conditions are authoritative.

_Validation:_
- Enum: [Pending Validating Cloning Streaming CutoverPending CuttingOver Verifying Completed Failed Suspended]

_Appears in:_
- [MigrationStatus](#migrationstatus)

| Field | Description |
| --- | --- |
| `Pending` |  |
| `Validating` |  |
| `Cloning` |  |
| `Streaming` |  |
| `CutoverPending` |  |
| `CuttingOver` |  |
| `Verifying` |  |
| `Completed` |  |
| `Failed` |  |
| `Suspended` |  |


#### MigrationSpec



MigrationSpec is the desired state of a Migration. source and target are
immutable after creation (a migration is a one-shot job, like batch/v1 Job).



_Appears in:_
- [Migration](#migration)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `source` _[PostgresConnection](#postgresconnection)_ | source is the PostgreSQL endpoint to migrate from. Immutable. |  | Required: \{\} <br /> |
| `target` _[PostgresConnection](#postgresconnection)_ | target is the PostgreSQL endpoint to migrate to. Immutable. |  | Required: \{\} <br /> |
| `clone` _[CloneOptions](#cloneoptions)_ | clone configures the base copy. |  | Optional: \{\} <br /> |
| `follow` _[FollowOptions](#followoptions)_ | follow enables live migration (CDC after the base copy). Immutable:<br />a one-shot clone cannot become a live migration after the fact. |  | Optional: \{\} <br /> |
| `cutover` _[CutoverSpec](#cutoverspec)_ | cutover controls how a live migration ends; ignored without follow. |  | Optional: \{\} <br /> |
| `verification` _[VerificationOptions](#verificationoptions)_ | verification runs pgcopydb compare checks once the migration reaches<br />its success path (clone: after the copy; follow: after cutover drained<br />and replication state is cleaned up). |  | Optional: \{\} <br /> |
| `workVolume` _[WorkVolume](#workvolume)_ | workVolume configures the work-directory PVC. |  | Optional: \{\} <br /> |
| `runner` _[RunnerSpec](#runnerspec)_ | runner configures the worker pod. |  | Optional: \{\} <br /> |
| `suspend` _boolean_ | suspend stops the worker while preserving the work volume. |  | Optional: \{\} <br /> |
| `backoffLimit` _integer_ | backoffLimit is the operator-level retry budget. Each attempt is a fresh<br />Job (backoffLimit 0) that resumes via the pgcopydb work directory. | 3 | Minimum: 0 <br />Optional: \{\} <br /> |
| `ttlSecondsAfterFinished` _integer_ | ttlSecondsAfterFinished deletes owned Jobs this long after completion. |  | Minimum: 0 <br />Optional: \{\} <br /> |


#### MigrationStatus



MigrationStatus is the observed state of a Migration.



_Appears in:_
- [Migration](#migration)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `observedGeneration` _integer_ | observedGeneration is the .metadata.generation last reconciled. |  | Optional: \{\} <br /> |
| `phase` _[MigrationPhase](#migrationphase)_ | phase is a human-facing summary derived from conditions. |  | Enum: [Pending Validating Cloning Streaming CutoverPending CuttingOver Verifying Completed Failed Suspended] <br />Optional: \{\} <br /> |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#condition-v1-meta) array_ | conditions represent the current state of the Migration. |  | Optional: \{\} <br /> |
| `attempts` _integer_ | attempts is the number of worker Jobs created so far. |  | Optional: \{\} <br /> |
| `progress` _[CloneProgress](#cloneprogress)_ | progress reports base-copy progress. |  | Optional: \{\} <br /> |
| `replication` _[ReplicationStatus](#replicationstatus)_ | replication reports streaming state while following. |  | Optional: \{\} <br /> |
| `jobName` _string_ | jobName is the current worker Job. |  | Optional: \{\} <br /> |
| `startedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#time-v1-meta)_ |  |  | Optional: \{\} <br /> |
| `completedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#time-v1-meta)_ |  |  | Optional: \{\} <br /> |


#### PostgresConnection



PostgresConnection describes how to reach one PostgreSQL endpoint. It is a
self-contained type so a reusable connection kind can reference it later.
Provide either the inline fields (host/database/username plus a password
secret) or uriSecretRef (a full libpq URI/DSN), never both.



_Appears in:_
- [MigrationSpec](#migrationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `host` _string_ | host is the server hostname or IP. Required unless uriSecretRef is set. |  | Optional: \{\} <br /> |
| `port` _integer_ | port is the server port. | 5432 | Maximum: 65535 <br />Minimum: 1 <br />Optional: \{\} <br /> |
| `database` _string_ | database is the database name to connect to. Required unless uriSecretRef is set. |  | Optional: \{\} <br /> |
| `username` _string_ | username is the role to connect as. Required unless uriSecretRef is set. |  | Optional: \{\} <br /> |
| `passwordSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#secretkeyselector-v1-core)_ | passwordSecretRef selects the password. Rendered into a libpq passfile,<br />never into argv or CR status. |  | Optional: \{\} <br /> |
| `sslMode` _string_ | sslMode is the libpq sslmode. | prefer | Enum: [disable allow prefer require verify-ca verify-full] <br />Optional: \{\} <br /> |
| `tls` _[TLSSecretRefs](#tlssecretrefs)_ | tls references client certificate material, mounted 0600 for libpq. |  | Optional: \{\} <br /> |
| `uriSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#secretkeyselector-v1-core)_ | uriSecretRef selects a full libpq connection URI/DSN (with credentials).<br />Mutually exclusive with the inline fields; useful for DBaaS sources. |  | Optional: \{\} <br /> |


#### ReplicationStatus



ReplicationStatus mirrors the pgcopydb sentinel while streaming.



_Appears in:_
- [MigrationStatus](#migrationstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `slotName` _string_ |  |  | Optional: \{\} <br /> |
| `writeLSN` _string_ | writeLSN is the last LSN received from the source. |  | Optional: \{\} <br /> |
| `replayLSN` _string_ | replayLSN is the last transaction durably applied to the target. |  | Optional: \{\} <br /> |
| `endpos` _string_ | endpos is the cutover LSN once set. |  | Optional: \{\} <br /> |
| `lagBytes` _integer_ | lagBytes is the byte distance between the source WAL head and replayLSN. |  | Optional: \{\} <br /> |


#### RunnerSpec



RunnerSpec configures the migration worker pod.



_Appears in:_
- [MigrationSpec](#migrationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _string_ | image overrides the runner image (default: operator-configured). |  | Optional: \{\} <br /> |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#resourcerequirements-v1-core)_ | resources sets the runner container resource requirements. |  | Optional: \{\} <br /> |
| `nodeSelector` _object (keys:string, values:string)_ |  |  | Optional: \{\} <br /> |
| `tolerations` _[Toleration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#toleration-v1-core) array_ |  |  | Optional: \{\} <br /> |
| `affinity` _[Affinity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#affinity-v1-core)_ |  |  | Optional: \{\} <br /> |


#### SkipOption

_Underlying type:_ _string_

SkipOption names a section of the base copy to skip. Maps to pgcopydb
--skip-* flags; CDC (follow) is unaffected by these. extensionComments
(--skip-ext-comments) is already implied by extensions; list it alone to
install extensions but drop their COMMENT statements.

_Validation:_
- Enum: [largeObjects extensions extensionComments collations vacuum analyze dbProperties ctidSplit]

_Appears in:_
- [CloneOptions](#cloneoptions)



#### TLSSecretRefs



TLSSecretRefs points at client certificate material for a connection.



_Appears in:_
- [PostgresConnection](#postgresconnection)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `rootCA` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#secretkeyselector-v1-core)_ | rootCA is the server CA bundle (libpq sslrootcert). |  | Optional: \{\} <br /> |
| `cert` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#secretkeyselector-v1-core)_ | cert is the client certificate (libpq sslcert). |  | Optional: \{\} <br /> |
| `key` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#secretkeyselector-v1-core)_ | key is the client private key (libpq sslkey). |  | Optional: \{\} <br /> |


#### VerificationOptions



VerificationOptions selects post-migration pgcopydb compare checks. Both
default to off: even the schema compare costs a catalog fetch on both sides,
and the data compare reads every table row twice. Results are information,
not a gate: a mismatch sets the Verified condition to False and emits a
warning event, but the Migration still completes (the data has arrived; what
to do about a difference is the operator's call). Mutable until completion.



_Appears in:_
- [MigrationSpec](#migrationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `schema` _boolean_ | schema runs pgcopydb compare schema: tables, columns, indexes,<br />constraints, and sequence values as pgcopydb models them. Not a full<br />DDL diff (no functions, triggers, ACLs, defaults). |  | Optional: \{\} <br /> |
| `data` _boolean_ | data runs pgcopydb compare data: per-table row counts and full-table<br />checksums. Expensive: budget a sequential scan of every table on both<br />sides. |  | Optional: \{\} <br /> |


#### WorkVolume



WorkVolume configures the PVC that holds the pgcopydb work directory. This
is the unit of resumability and MUST survive pod restarts.



_Appears in:_
- [MigrationSpec](#migrationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storageClassName` _string_ | storageClassName selects the StorageClass; empty uses the cluster default. |  | Optional: \{\} <br /> |
| `size` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/#quantity-resource-api)_ | size is the requested volume size. | 10Gi | Optional: \{\} <br /> |


