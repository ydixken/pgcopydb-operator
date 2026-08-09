/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
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
// Provide either the inline fields (host/database/username plus a password
// secret) or uriSecretRef (a full libpq URI/DSN), never both.
// +kubebuilder:validation:XValidation:rule="has(self.uriSecretRef) ? !has(self.host) && !has(self.username) : has(self.host) && has(self.username)",message="set either uriSecretRef or the inline host/username fields, not both"
type PostgresConnection struct {
	// host is the server hostname or IP. Required unless uriSecretRef is set.
	// +optional
	Host string `json:"host,omitempty"`

	// port is the server port.
	// +kubebuilder:default=5432
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// database is the database name to connect to. Required unless uriSecretRef is set.
	// +optional
	Database string `json:"database,omitempty"`

	// username is the role to connect as. Required unless uriSecretRef is set.
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

// CloneOptions maps the pgcopydb clone surface. All fields are optional; zero
// values mean "use the pgcopydb default".
type CloneOptions struct {
	// tableJobs is the number of concurrent table COPY workers (pgcopydb --table-jobs).
	// +kubebuilder:validation:Minimum=1
	// +optional
	TableJobs int32 `json:"tableJobs,omitempty"`

	// indexJobs is the number of concurrent CREATE INDEX workers (--index-jobs).
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
	// above this size (--split-tables-larger-than), rendered to bytes.
	// +optional
	SplitTablesLargerThan *resource.Quantity `json:"splitTablesLargerThan,omitempty"`

	// splitMaxParts caps the number of parts per table (--split-max-parts).
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

	// useCopyBinary uses COPY WITH (FORMAT BINARY) (--use-copy-binary).
	// +optional
	UseCopyBinary bool `json:"useCopyBinary,omitempty"`

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
	// replayLSN is the last transaction durably applied to the target.
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
// +kubebuilder:validation:Enum=Pending;Validating;Cloning;Streaming;CutoverPending;CuttingOver;Verifying;Completed;Failed;Suspended
type MigrationPhase string

const (
	PhasePending        MigrationPhase = "Pending"
	PhaseValidating     MigrationPhase = "Validating"
	PhaseCloning        MigrationPhase = "Cloning"
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

// CloneProgress mirrors pgcopydb list progress --json into status.
type CloneProgress struct {
	// +optional
	TablesTotal int64 `json:"tablesTotal,omitempty"`
	// +optional
	TablesDone int64 `json:"tablesDone,omitempty"`
	// +optional
	IndexesTotal int64 `json:"indexesTotal,omitempty"`
	// +optional
	IndexesDone int64 `json:"indexesDone,omitempty"`
	// bytesTotal is the total bytes to copy, as reported by pgcopydb.
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
