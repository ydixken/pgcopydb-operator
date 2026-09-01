/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PostgresConnection describes how to reach one PostgreSQL endpoint. It is a
// self-contained type so a reusable connection kind can reference it later.
// Provide exactly one form: the inline fields (host/database/username plus a
// password secret), uriSecretRef (a full libpq URI/DSN), or secretRef (one
// Secret holding the parts as individual keys).
// +kubebuilder:validation:XValidation:rule="(has(self.secretRef) ? 1 : 0) + (has(self.uriSecretRef) ? 1 : 0) + ((has(self.host) || has(self.username) || has(self.database) || has(self.passwordSecretRef)) ? 1 : 0) == 1",message="set exactly one of secretRef, uriSecretRef, or the inline connection fields"
// +kubebuilder:validation:XValidation:rule="has(self.secretRef) || has(self.uriSecretRef) || (has(self.host) && has(self.username))",message="inline form needs both host and username"
type PostgresConnection struct {
	// host is the server hostname or IP, for the inline form.
	// +optional
	Host string `json:"host,omitempty"`

	// port is the server port.
	// +kubebuilder:default=5432
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// database is the database name to connect to, for the inline form.
	// +optional
	Database string `json:"database,omitempty"`

	// username is the role to connect as, for the inline form.
	// +optional
	Username string `json:"username,omitempty"`

	// passwordSecretRef selects the password. Rendered into a libpq passfile,
	// never into argv or CR status.
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`

	// sslMode is the libpq sslmode.
	// +kubebuilder:validation:Enum=disable;allow;prefer;require;verify-ca;verify-full
	// +kubebuilder:default=prefer
	// +optional
	SSLMode string `json:"sslMode,omitempty"`

	// tls references client certificate material, mounted 0600 for libpq.
	// +optional
	TLS *TLSSecretRefs `json:"tls,omitempty"`

	// uriSecretRef selects a full libpq connection URI/DSN (with credentials).
	// Mutually exclusive with the inline fields; useful for DBaaS sources.
	// +optional
	URISecretRef *corev1.SecretKeySelector `json:"uriSecretRef,omitempty"`

	// secretRef references one Secret carrying the connection details as
	// individual keys, the way platform provisioners hand them out.
	// Mutually exclusive with the inline fields and uriSecretRef.
	// +optional
	SecretRef *ConnectionSecret `json:"secretRef,omitempty"`

	// superuserSecretRef names a superuser on this same endpoint, in the same
	// Secret convention (USER/PW; URL keys, when present, must match this
	// connection). The preflight uses it to verify and apply missing grants;
	// applied statements are logged and kept, never reverted.
	// +optional
	SuperuserSecretRef *ConnectionSecret `json:"superuserSecretRef,omitempty"`
}

// ConnectionSecret points at a Secret whose keys hold the parts of a
// connection. Key names are remappable; defaults match the common platform
// convention (DB, PW, URL, URL_EXTERNAL, USER).
type ConnectionSecret struct {
	// name is the Secret in the Migration's namespace.
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`

	// endpoint picks which URL key supplies the host when the database key
	// holds a bare name: internal (url key) or external (urlExternal key).
	// +kubebuilder:validation:Enum=internal;external
	// +kubebuilder:default=internal
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// keys remaps the Secret key names.
	// +optional
	Keys *ConnectionSecretKeys `json:"keys,omitempty"`
}

// ConnectionSecretKeys names the Secret keys the connection parts come from.
type ConnectionSecretKeys struct {
	// database is a bare database name or a libpq URI that MUST be
	// password-free (the password key carries it); a URI is authoritative for
	// user, host, port, and database name. Values are used literally: special
	// characters need uriSecretRef.
	// +kubebuilder:default=DB
	// +optional
	Database string `json:"database,omitempty"`

	// password holds the password; projected as a file, never env or argv.
	// The key MUST exist even when the database key holds a URI.
	// +kubebuilder:default=PW
	// +optional
	Password string `json:"password,omitempty"`

	// url holds the internal hostname, optionally host:port.
	// +kubebuilder:default=URL
	// +optional
	URL string `json:"url,omitempty"`

	// urlExternal holds the externally reachable hostname, optionally host:port.
	// +kubebuilder:default=URL_EXTERNAL
	// +optional
	URLExternal string `json:"urlExternal,omitempty"`

	// username holds the role to connect as.
	// +kubebuilder:default=USER
	// +optional
	Username string `json:"username,omitempty"`
}

// TLSSecretRefs points at client certificate material for a connection.
type TLSSecretRefs struct {
	// rootCA is the server CA bundle (libpq sslrootcert).
	// +optional
	RootCA *corev1.SecretKeySelector `json:"rootCA,omitempty"`
	// cert is the client certificate (libpq sslcert).
	// +optional
	Cert *corev1.SecretKeySelector `json:"cert,omitempty"`
	// key is the client private key (libpq sslkey).
	// +optional
	Key *corev1.SecretKeySelector `json:"key,omitempty"`
}

// SkipOption names a section of the base copy to skip. Maps to pgcopydb
// --skip-* flags; CDC (follow) is unaffected by these. extensionComments
// (--skip-ext-comments) is already implied by extensions; list it alone to
// install extensions but drop their COMMENT statements.
// +kubebuilder:validation:Enum=largeObjects;extensions;extensionComments;collations;vacuum;analyze;dbProperties;ctidSplit
type SkipOption string

// CloneOptions maps the pgcopydb clone surface. All fields are optional. A
// zero value means the operator decides, which for most fields is pgcopydb's
// own default. Three it overrides, because pgcopydb's defaults are wrong for a
// migration: tableJobs follows the worker's CPU request, and
// splitTablesLargerThan and splitMaxParts turn on same-table concurrency,
// which pgcopydb ships disabled. See docs/configuration.md.
type CloneOptions struct {
	// tableJobs is the number of concurrent table COPY workers (pgcopydb
	// --table-jobs). Unset follows the worker's CPU request, minimum four. Each
	// job also gets a concurrent VACUUM ANALYZE backend on the target, so N
	// here means up to 2N target connections.
	// +kubebuilder:validation:Minimum=1
	// +optional
	TableJobs int32 `json:"tableJobs,omitempty"`

	// indexJobs is the number of concurrent CREATE INDEX workers (--index-jobs).
	// Unset leaves pgcopydb's default of four. Size it against the TARGET, not
	// the worker: pgcopydb sets maintenance_work_mem to 1GB per index worker,
	// overriding the server's own setting, so four jobs authorise 4GB there.
	// +kubebuilder:validation:Minimum=1
	// +optional
	IndexJobs int32 `json:"indexJobs,omitempty"`

	// restoreJobs is pg_restore --jobs (--restore-jobs); 0 follows indexJobs.
	// +kubebuilder:validation:Minimum=0
	// +optional
	RestoreJobs int32 `json:"restoreJobs,omitempty"`

	// largeObjectsJobs is the number of concurrent large-object workers.
	// +kubebuilder:validation:Minimum=1
	// +optional
	LargeObjectsJobs int32 `json:"largeObjectsJobs,omitempty"`

	// splitTablesLargerThan enables same-table concurrency for tables at or
	// above this size (--split-tables-larger-than), rendered to bytes. Unset
	// defaults to 512Mi; pgcopydb itself ships this disabled, which leaves one
	// large table to a single worker however many table jobs are running.
	// Splitting needs a single-column integer key, or it falls back to ctid
	// ranges, and pgcopydb disables it silently when the source is a standby.
	// +optional
	SplitTablesLargerThan *resource.Quantity `json:"splitTablesLargerThan,omitempty"`

	// splitMaxParts caps the number of parts per table (--split-max-parts).
	// Unset defaults to 8, so a very large table cannot fan out into hundreds
	// of parts and catalog rows.
	// +kubebuilder:validation:Minimum=0
	// +optional
	SplitMaxParts int32 `json:"splitMaxParts,omitempty"`

	// estimateTableSizes bases split decisions on pg_class page-count
	// estimates instead of exact size queries (--estimate-table-sizes). To
	// refresh those estimates pgcopydb first runs vacuumdb --analyze-only
	// (with tableJobs workers) on the SOURCE; add "analyze" to skip to leave
	// the source untouched and trust its existing statistics.
	// +optional
	EstimateTableSizes bool `json:"estimateTableSizes,omitempty"`

	// dropIfExists issues pg_restore --clean --if-exists on the target.
	// +optional
	DropIfExists bool `json:"dropIfExists,omitempty"`

	// roles copies roles before the clone (--roles). Needs superuser on the
	// source unless noRolePasswords is also set.
	// +optional
	Roles bool `json:"roles,omitempty"`

	// noRolePasswords dumps roles without passwords (--no-role-passwords),
	// avoiding the superuser requirement of roles.
	// +optional
	NoRolePasswords bool `json:"noRolePasswords,omitempty"`

	// noOwner skips ALTER OWNER on restore (--no-owner).
	// +optional
	NoOwner bool `json:"noOwner,omitempty"`

	// noACL skips GRANT/REVOKE on restore (--no-acl).
	// +optional
	NoACL bool `json:"noACL,omitempty"`

	// noComments skips COMMENT statements (--no-comments).
	// +optional
	NoComments bool `json:"noComments,omitempty"`

	// noTablespaces skips tablespace selection (--no-tablespaces).
	// +optional
	NoTablespaces bool `json:"noTablespaces,omitempty"`

	// useCopyBinary uses COPY WITH (FORMAT BINARY) (--use-copy-binary), which
	// is on by default. Text COPY encodes bytea as hex, two wire bytes per
	// data byte, and the worker relays every row between source and target, so
	// the cost lands on both legs. pgcopydb checks each table against the
	// source catalog and falls back to text for any table with a column whose
	// binary encoding is not safe, so this is per table rather than all or
	// nothing. Set it false to force text everywhere.
	//
	// A pointer, and defaulted by the API server rather than by the operator,
	// because this is the only shape where all three states are expressible.
	// A plain bool cannot carry them: false is its zero value, so a Go client
	// that never touches the field still marshals "useCopyBinary": false, the
	// API server sees a value present and skips its default, and the setting
	// silently stays off for everyone not writing YAML by hand. nil with
	// omitempty leaves the key absent, which is what the default needs.
	// +kubebuilder:default=true
	// +optional
	UseCopyBinary *bool `json:"useCopyBinary,omitempty"`

	// failFast stops the whole run on the first failed child (--fail-fast).
	// +optional
	FailFast bool `json:"failFast,omitempty"`

	// skip lists base-copy sections to skip.
	// +listType=set
	// +optional
	Skip []SkipOption `json:"skip,omitempty"`

	// filters is rendered to the pgcopydb --filters INI file.
	// +optional
	Filters *Filters `json:"filters,omitempty"`
}

// Filters maps the pgcopydb --filters INI sections. pgcopydb rejects some
// combinations (for example include-only-table with exclude-table); those are
// enforced by CEL below and surfaced as validation errors.
// +kubebuilder:validation:XValidation:rule="!(has(self.includeOnlyTables) && (has(self.excludeTables) || has(self.excludeSchemas)))",message="includeOnlyTables cannot be combined with excludeTables or excludeSchemas"
// +kubebuilder:validation:XValidation:rule="!(has(self.includeOnlySchemas) && has(self.excludeSchemas))",message="includeOnlySchemas cannot be combined with excludeSchemas"
// +kubebuilder:validation:XValidation:rule="!(has(self.includeOnlyExtensions) && has(self.excludeExtensions))",message="includeOnlyExtensions cannot be combined with excludeExtensions"
type Filters struct {
	// +optional
	IncludeOnlyTables []string `json:"includeOnlyTables,omitempty"`
	// +optional
	ExcludeTables []string `json:"excludeTables,omitempty"`
	// +optional
	IncludeOnlySchemas []string `json:"includeOnlySchemas,omitempty"`
	// +optional
	ExcludeSchemas []string `json:"excludeSchemas,omitempty"`
	// +optional
	ExcludeIndexes []string `json:"excludeIndexes,omitempty"`
	// +optional
	ExcludeTableData []string `json:"excludeTableData,omitempty"`
	// +optional
	IncludeOnlyExtensions []string `json:"includeOnlyExtensions,omitempty"`
	// +optional
	ExcludeExtensions []string `json:"excludeExtensions,omitempty"`
}

// WorkVolume configures the PVC that holds the pgcopydb work directory. This
// is the unit of resumability and MUST survive pod restarts.
type WorkVolume struct {
	// storageClassName selects the StorageClass; empty uses the cluster default.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// size is the requested volume size.
	// +kubebuilder:default="10Gi"
	// +optional
	Size resource.Quantity `json:"size,omitempty"`
}

// RunnerSpec configures the migration worker pod.
type RunnerSpec struct {
	// image overrides the runner image (default: operator-configured).
	// +optional
	Image string `json:"image,omitempty"`
	// resources sets the runner container resource requirements.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
}

// PluginWal2json is the one decoding plugin the operator special-cases: it
// ships outside PostgreSQL, so it carries both the preflight's cannot-verify
// note and the numeric-as-string knob.
const PluginWal2json = "wal2json"

// FollowOptions enables live migration: clone under a replication slot, then
// stream and apply changes until cutover (pgcopydb clone --follow).
// +kubebuilder:validation:XValidation:rule="!(has(self.wal2jsonNumericAsString) && self.wal2jsonNumericAsString) || (has(self.plugin) && self.plugin == 'wal2json')",message="wal2jsonNumericAsString configures the wal2json plugin and does nothing under any other; set plugin: wal2json or drop the field"
type FollowOptions struct {
	// enabled turns the migration into a live one.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// plugin is the logical decoding plugin (--plugin).
	// +kubebuilder:validation:Enum=pgoutput;wal2json;test_decoding
	// +kubebuilder:default=pgoutput
	// +optional
	Plugin string `json:"plugin,omitempty"`

	// slotName overrides the replication slot name (--slot-name). Empty means
	// a generated name unique to this Migration; set it only when fanning
	// several migrations out of one source instance deliberately. The pattern
	// is PostgreSQL's own slot-name charset; the operator relies on it when
	// interpolating the name into SQL (origin verification, retry cleanup).
	// +kubebuilder:validation:Pattern=`^[a-z0-9_]+$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	SlotName string `json:"slotName,omitempty"`

	// publication names a pre-created publication (--publication); empty lets
	// pgcopydb create and drop its own.
	// +optional
	Publication string `json:"publication,omitempty"`

	// wal2jsonNumericAsString makes wal2json emit numeric values as JSON
	// strings (--wal2json-numeric-as-string), preserving precision a JSON
	// number would lose. Only meaningful with plugin wal2json; admission
	// rejects it under any other plugin.
	// +optional
	Wal2jsonNumericAsString bool `json:"wal2jsonNumericAsString,omitempty"`

	// replayNoOpUpdates replays UPDATEs that change no columns
	// (--replay-no-op-updates), needed when target triggers must fire.
	// +optional
	ReplayNoOpUpdates bool `json:"replayNoOpUpdates,omitempty"`

	// allowMissingReplicaIdentity acknowledges tables that the preflight
	// replica-identity audit would otherwise fail on. Entries are
	// schema-qualified table names exactly as the preflight prints them
	// (schema.table, unquoted); the single entry "*" acknowledges every
	// offender. Acknowledged tables are reported as a warning instead of
	// failing the Migration. The risk stays: UPDATE or DELETE on such a
	// table during the migration window fails on the source at write time,
	// so acknowledge only tables that are read-only or insert-only while
	// the migration runs. Immutable with the rest of follow.
	// +kubebuilder:validation:XValidation:rule="self.all(x, x != '')",message="entries must be non-empty table names (or \"*\")"
	// +listType=set
	// +optional
	AllowMissingReplicaIdentity []string `json:"allowMissingReplicaIdentity,omitempty"`

	// maxCatchupLag is the replication lag under which the migration counts
	// as caught up (the CaughtUp condition and Automatic cutover trigger).
	// +kubebuilder:default="16Mi"
	// +optional
	MaxCatchupLag *resource.Quantity `json:"maxCatchupLag,omitempty"`
}

// VerificationOptions selects post-migration pgcopydb compare checks. Both
// default to off: even the schema compare costs a catalog fetch on both sides,
// and the data compare reads every table row twice. Results are information,
// not a gate: a mismatch sets the Verified condition to False and emits a
// warning event, but the Migration still completes (the data has arrived; what
// to do about a difference is the operator's call). Mutable until completion.
type VerificationOptions struct {
	// schema runs pgcopydb compare schema: tables, columns, indexes,
	// constraints, and sequence values as pgcopydb models them. Not a full
	// DDL diff (no functions, triggers, ACLs, defaults).
	// +optional
	Schema bool `json:"schema,omitempty"`

	// data runs pgcopydb compare data: per-table row counts and full-table
	// checksums. Expensive: budget a sequential scan of every table on both
	// sides.
	// +optional
	Data bool `json:"data,omitempty"`
}

// VerificationResult is one compare check's outcome.
type VerificationResult struct {
	// check is the pgcopydb compare subcommand: schema or data.
	// +kubebuilder:validation:Enum=schema;data
	Check string `json:"check"`

	// passed is false when that compare reported differences.
	Passed bool `json:"passed"`
}

// CutoverMode picks who pulls the trigger.
// +kubebuilder:validation:Enum=Manual;Automatic
type CutoverMode string

const (
	// CutoverManual waits for spec.cutover.approved.
	CutoverManual CutoverMode = "Manual"
	// CutoverAutomatic cuts over as soon as the migration is caught up.
	CutoverAutomatic CutoverMode = "Automatic"
)

// CutoverSpec controls when replication stops and the migration finalizes.
// Cutover means: writes to the source MUST already be stopped, the stream is
// frozen at the source's current LSN, drained, and sequences re-synced.
type CutoverSpec struct {
	// mode selects Manual (default) or Automatic cutover.
	// +kubebuilder:default=Manual
	// +optional
	Mode CutoverMode `json:"mode,omitempty"`

	// approved triggers the cutover in Manual mode. Mutable. Setting it back
	// to false after the cutover started has no effect.
	// +optional
	Approved bool `json:"approved,omitempty"`
}

// ReplicationStatus mirrors the pgcopydb sentinel while streaming.
type ReplicationStatus struct {
	// +optional
	SlotName string `json:"slotName,omitempty"`
	// writeLSN is the last LSN received from the source.
	// +optional
	WriteLSN string `json:"writeLSN,omitempty"`
	// replayLSN is how far the target has consumed the stream, as the source
	// reports it: the walsender's replay position, or the slot's confirmed
	// flush position where the migration role may not read the walsender.
	// It measures consumption, not application. Whether the target really
	// applied every change is settled after cutover by the drain
	// verification, which reads the target's own replication origin.
	// +optional
	ReplayLSN string `json:"replayLSN,omitempty"`
	// endpos is the cutover LSN once set.
	// +optional
	Endpos string `json:"endpos,omitempty"`
	// lagBytes is the byte distance between the source WAL head and replayLSN.
	// +optional
	LagBytes *int64 `json:"lagBytes,omitempty"`
}

// MigrationSpec is the desired state of a Migration. source and target are
// immutable after creation (a migration is a one-shot job, like batch/v1 Job).
type MigrationSpec struct {
	// source is the PostgreSQL endpoint to migrate from. Immutable.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="source is immutable"
	// +required
	Source PostgresConnection `json:"source"`

	// target is the PostgreSQL endpoint to migrate to. Immutable.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="target is immutable"
	// +required
	Target PostgresConnection `json:"target"`

	// clone configures the base copy.
	// +optional
	Clone CloneOptions `json:"clone,omitempty"`

	// follow enables live migration (CDC after the base copy). Immutable:
	// a one-shot clone cannot become a live migration after the fact.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="follow is immutable"
	// +optional
	Follow *FollowOptions `json:"follow,omitempty"`

	// cutover controls how a live migration ends; ignored without follow.
	// +optional
	Cutover CutoverSpec `json:"cutover,omitempty"`

	// verification runs pgcopydb compare checks once the migration reaches
	// its success path (clone: after the copy; follow: after cutover drained
	// and replication state is cleaned up).
	// +optional
	Verification *VerificationOptions `json:"verification,omitempty"`

	// workVolume configures the work-directory PVC.
	// +optional
	WorkVolume WorkVolume `json:"workVolume,omitempty"`

	// runner configures the worker pod.
	// +optional
	Runner RunnerSpec `json:"runner,omitempty"`

	// suspend stops the worker while preserving the work volume.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// backoffLimit is the operator-level retry budget. Each attempt is a fresh
	// Job (backoffLimit 0) that resumes via the pgcopydb work directory.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +optional
	BackoffLimit int32 `json:"backoffLimit,omitempty"`

	// ttlSecondsAfterFinished deletes owned Jobs this long after completion.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// MigrationPhase is a human-facing summary derived from conditions. It exists
// for the printer column only; conditions are authoritative.
// +kubebuilder:validation:Enum=Pending;Validating;Cloning;Finalizing;Streaming;CutoverPending;CuttingOver;Verifying;Completed;Failed;Suspended
type MigrationPhase string

const (
	PhasePending    MigrationPhase = "Pending"
	PhaseValidating MigrationPhase = "Validating"
	PhaseCloning    MigrationPhase = "Cloning"
	// PhaseFinalizing is the tail of a base copy: the data is across and the
	// worker is building indexes, applying constraints and vacuuming. It is
	// distinct from Cloning because it behaves nothing like it. The copy runs
	// with every worker busy; the tail routinely narrows to a single VACUUM on
	// the largest table, because a table's vacuum cannot start until its own
	// copy finishes and the largest one finishes last. That tail measured
	// roughly a fifth of a clone's wall clock, during which the target stops
	// growing and every size-based estimate reads as finished.
	PhaseFinalizing     MigrationPhase = "Finalizing"
	PhaseStreaming      MigrationPhase = "Streaming"
	PhaseCutoverPending MigrationPhase = "CutoverPending"
	PhaseCuttingOver    MigrationPhase = "CuttingOver"
	PhaseVerifying      MigrationPhase = "Verifying"
	PhaseCompleted      MigrationPhase = "Completed"
	PhaseFailed         MigrationPhase = "Failed"
	PhaseSuspended      MigrationPhase = "Suspended"
)

// Condition type constants (positive polarity, per Kubernetes API conventions).
const (
	ConditionValidated       = "Validated"
	ConditionCloneCompleted  = "CloneCompleted"
	ConditionStreaming       = "Streaming"
	ConditionCaughtUp        = "CaughtUp"
	ConditionCutoverComplete = "CutoverCompleted"
	ConditionVerified        = "Verified"
	ConditionComplete        = "Complete"
	ConditionFailed          = "Failed"
)

// CloneProgress carries how far the base copy has got. While the copy runs
// the operator counts relations on both databases with psql; pgcopydb's own
// `list progress` replaces that wherever it can be read.
type CloneProgress struct {
	// +optional
	TablesTotal int64 `json:"tablesTotal,omitempty"`
	// +optional
	TablesDone int64 `json:"tablesDone,omitempty"`
	// +optional
	IndexesTotal int64 `json:"indexesTotal,omitempty"`
	// +optional
	IndexesDone int64 `json:"indexesDone,omitempty"`
	// bytesTotal is the total bytes to copy: the in-scope tables' size on the
	// source, or pgcopydb's own figure once it has answered.
	// +optional
	BytesTotal *resource.Quantity `json:"bytesTotal,omitempty"`
	// bytesDone is the bytes copied so far.
	// +optional
	BytesDone *resource.Quantity `json:"bytesDone,omitempty"`
}

// MigrationStatus is the observed state of a Migration.
type MigrationStatus struct {
	// observedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase is a human-facing summary derived from conditions.
	// +optional
	Phase MigrationPhase `json:"phase,omitempty"`

	// conditions represent the current state of the Migration.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// attempts is the number of worker Jobs created so far.
	// +optional
	Attempts int32 `json:"attempts,omitempty"`

	// progress reports base-copy progress.
	// +optional
	Progress *CloneProgress `json:"progress,omitempty"`

	// replication reports streaming state while following.
	// +optional
	Replication *ReplicationStatus `json:"replication,omitempty"`

	// verification reports each requested compare check separately, because
	// the Verified condition collapses them and its reason names only the
	// first mismatch. Re-derived from the compare Jobs every reconcile.
	// +listType=map
	// +listMapKey=check
	// +optional
	Verification []VerificationResult `json:"verification,omitempty"`

	// jobName is the current worker Job.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:shortName=pgm
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Complete",type=string,JSONPath=`.status.conditions[?(@.type=="Complete")].status`
// +kubebuilder:printcolumn:name="Lag",type=integer,priority=1,JSONPath=`.status.replication.lagBytes`
// +kubebuilder:printcolumn:name="Attempts",type=integer,JSONPath=`.status.attempts`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Migration is the Schema for the migrations API.
type Migration struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Migration
	// +required
	Spec MigrationSpec `json:"spec"`

	// status defines the observed state of Migration
	// +optional
	Status MigrationStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// MigrationList contains a list of Migration.
type MigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Migration `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Migration{}, &MigrationList{})
		return nil
	})
}
