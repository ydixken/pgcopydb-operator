package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	nativeWireLimit       = 65536
	completedEvidenceKind = "completed_evidence"
	nativeDrainKeepalive  = "keepalive"
	nativeDrainLaterBegin = "later_begin"
	nativeResourceOrigin  = "replication_origin"
)

var completedGuards = []string{"target_settings", "target_identity", "configure_durability", "target_fence", "idle_guard", "begin", "lock_checkpoint", "lock_marker", "catalog_checkpoint", "catalog_marker", "row_read", "same_value_update", "deferred_effects", "row_comparison", "precommit_guard", "commit", "idle_guard", "unlock", "closure_guard"}

type NativeTransport interface {
	InPodNative(context.Context, string, string, podexec.NativeContainerBinding, []string, []byte) (podexec.NativeOutcome, error)
	JobCompletedNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
}

type Reader struct {
	api       client.Reader
	transport NativeTransport
}

// Use an uncached API reader so replacement checks observe live object identities.
func NewReader(api client.Reader, transport NativeTransport) *Reader {
	return &Reader{api: api, transport: transport}
}

type ConfirmedCheckpoint struct {
	evidence  CheckpointEvidence
	confirmed bool
}

func (p ConfirmedCheckpoint) Evidence() CheckpointEvidence {
	if !p.confirmed {
		return CheckpointEvidence{}
	}
	return p.evidence
}

type AdmittedOwnedState struct {
	work      WorkIdentity
	resources []ResourceObligation
	admitted  bool
}

func (s AdmittedOwnedState) Work() WorkIdentity {
	if !s.admitted {
		return WorkIdentity{}
	}
	return s.work
}
func (s AdmittedOwnedState) Resources() []ResourceObligation {
	if !s.admitted {
		return nil
	}
	return slices.Clone(s.resources)
}

type CompletedObservation struct {
	checkpoint ConfirmedCheckpoint
	owned      AdmittedOwnedState
}

func (o CompletedObservation) Checkpoint() ConfirmedCheckpoint { return o.checkpoint }
func (o CompletedObservation) OwnedState() AdmittedOwnedState {
	return AdmittedOwnedState{work: o.owned.work, resources: o.owned.Resources(), admitted: o.owned.admitted}
}

func (r *Reader) ReadCompleted(ctx context.Context, want ExecutionRequest, savedE string) (*CompletedObservation, error) {
	if r == nil || r.api == nil || r.transport == nil || want.Role != JobRoleEvidence {
		return nil, errors.New("native evidence admission unavailable")
	}
	admitted, err := r.admitCompleted(ctx, want, false)
	if err != nil {
		return nil, err
	}
	if admitted.migration.Status.Replication == nil || !sameLSN(admitted.migration.Status.Replication.Endpos, savedE) {
		return nil, errors.New("native evidence persisted end LSN changed")
	}
	body, err := completedRequest(want, admitted.pod.Name, admitted.container, savedE)
	if err != nil {
		return nil, err
	}
	relay := EvidenceRequestWriterProgram()
	if len(relay) == 0 || len(relay) > 16384 {
		return nil, errors.New("native evidence relay unavailable")
	}
	argv := []string{"/usr/bin/env", "-u", "LD_PRELOAD", "-u", "LD_LIBRARY_PATH", "/usr/bin/python3", "-I", "-B", "-c", string(relay), want.OperationID}
	relayCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	outcome, err := r.transport.InPodNative(relayCtx, want.Namespace, admitted.pod.Name, admitted.container, argv, body)
	cancel()
	if err != nil || !outcome.Successful() {
		return nil, errors.New("native evidence relay unavailable")
	}
	result, err := parseCompletedResult(outcome.Payload(), want, body, savedE)
	if err != nil {
		return nil, err
	}
	completionCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	completed, err := r.transport.JobCompletedNative(completionCtx, want.Namespace, want.JobUID, admitted.container)
	cancel()
	if err != nil || !completed.Successful() || completed.Payload() != nil {
		return nil, errors.New("native evidence main completion unavailable")
	}
	after, err := r.admitCompleted(ctx, want, true)
	if err != nil || !admitted.sameExecution(after) || after.migration.Status.Replication == nil || !sameLSN(after.migration.Status.Replication.Endpos, savedE) || ctx.Err() != nil {
		return nil, errors.New("native evidence execution changed")
	}
	return &CompletedObservation{checkpoint: ConfirmedCheckpoint{evidence: result.checkpoint(), confirmed: true}, owned: AdmittedOwnedState{work: result.Work, resources: result.obligations(), admitted: true}}, nil
}

type requestWire struct {
	Version       int64         `json:"version"`
	Kind          string        `json:"kind"`
	OperationID   string        `json:"operation_id"`
	ProgramSHA256 string        `json:"program_sha256"`
	Execution     executionWire `json:"execution"`
	PodName       string        `json:"pod_name"`
	Container     containerWire `json:"container"`
	SavedE        string        `json:"saved_e"`
}

type executionWire struct {
	Namespace                 string       `json:"namespace"`
	MigrationName             string       `json:"migration_name"`
	PVCName                   string       `json:"pvc_name"`
	JobName                   string       `json:"job_name"`
	MigrationUID              string       `json:"migration_uid"`
	PVCUID                    string       `json:"pvc_uid"`
	JobUID                    string       `json:"job_uid"`
	WorkerJobUID              string       `json:"worker_job_uid"`
	MigrationGeneration       int64        `json:"migration_generation"`
	Attempt                   int32        `json:"attempt"`
	Role                      JobRole      `json:"role"`
	Work                      WorkIdentity `json:"work"`
	ImageRef                  string       `json:"image_ref"`
	PlatformImageID           string       `json:"platform_image_id"`
	TemplateSHA256            string       `json:"template_sha256"`
	ConnectionReferenceSHA256 string       `json:"connection_reference_sha256"`
}

type containerWire struct {
	PodUID         string `json:"pod_uid"`
	ContainerName  string `json:"container_name"`
	ContainerID    string `json:"container_id"`
	RestartCount   int32  `json:"restart_count"`
	ImageID        string `json:"image_id"`
	TemplateSHA256 string `json:"template_sha256"`
}

func completedRequest(w ExecutionRequest, pod string, c podexec.NativeContainerBinding, savedE string) ([]byte, error) {
	v := requestWire{Version: 1, Kind: completedEvidenceKind, OperationID: w.OperationID, ProgramSHA256: hex.EncodeToString(w.ProgramSHA256[:]), Execution: executionWire{Namespace: w.Namespace, MigrationName: w.MigrationName, PVCName: w.PVCName, JobName: w.JobName, MigrationUID: string(w.MigrationUID), PVCUID: string(w.PVCUID), JobUID: string(w.JobUID), WorkerJobUID: string(w.WorkerJobUID), MigrationGeneration: w.MigrationGeneration, Attempt: w.Attempt, Role: w.Role, Work: w.Work, ImageRef: w.ImageRef, PlatformImageID: w.PlatformImageID, TemplateSHA256: hex.EncodeToString(w.TemplateSHA256[:]), ConnectionReferenceSHA256: hex.EncodeToString(w.ConnectionReferenceSHA256[:])}, PodName: pod, Container: containerWire{PodUID: string(c.PodUID), ContainerName: c.ContainerName, ContainerID: c.ContainerID, RestartCount: c.RestartCount, ImageID: c.ImageID, TemplateSHA256: hex.EncodeToString(c.TemplateSHA256[:])}, SavedE: savedE}
	body, err := json.Marshal(v)
	if err != nil {
		return nil, errors.New("native evidence request invalid")
	}
	if _, err := parseCompletedRequest(body); err != nil {
		return nil, err
	}
	return body, nil
}

func parseCompletedRequest(raw []byte) (*requestWire, error) {
	var v requestWire
	if !decodeNativeWire(raw, &v, "request") || v.Version != 1 || v.Kind != completedEvidenceKind || !lowerHex(v.OperationID, 32) || !lowerHex(v.ProgramSHA256, 64) || !validIdentity(v.Execution.Work) || v.Execution.Role != JobRoleEvidence || v.Execution.MigrationGeneration <= 0 || v.Execution.Attempt < 0 || v.Container.RestartCount < 0 {
		return nil, errors.New("native evidence request invalid")
	}
	x := v.Execution
	c := v.Container
	for _, s := range []string{x.Namespace, x.MigrationName, x.PVCName, x.JobName, x.MigrationUID, x.PVCUID, x.JobUID, x.WorkerJobUID, x.ImageRef, x.PlatformImageID, v.PodName, c.PodUID, c.ContainerName, c.ContainerID, c.ImageID} {
		if !validLabel(s) {
			return nil, errors.New("native evidence request identity invalid")
		}
	}
	if x.Work.MigrationUID != x.MigrationUID || x.Work.WorkdirID != x.PVCUID || !lowerHex(x.TemplateSHA256, 64) || !lowerHex(x.ConnectionReferenceSHA256, 64) || c.TemplateSHA256 != x.TemplateSHA256 || c.ImageID != x.PlatformImageID {
		return nil, errors.New("native evidence request binding invalid")
	}
	if e, err := sentinel.ParseLSN(v.SavedE); err != nil || e == 0 {
		return nil, errors.New("native evidence end LSN invalid")
	}
	return &v, nil
}

func requestDigests(raw []byte) ([32]byte, [32]byte) {
	request := sha256.Sum256(raw)
	preimage := append([]byte("rpgmigrate.native.execution.v1\x00"), request[:]...)
	return request, sha256.Sum256(preimage)
}

type checkpointWire struct {
	CheckpointID     string `json:"checkpoint_id"`
	CertifiedLSN     string `json:"certified_lsn"`
	DurableReplayLSN string `json:"durable_replay_lsn"`
	DrainEndLSN      string `json:"drain_end_lsn"`
	DrainReplayLSN   string `json:"drain_replay_lsn"`
	DrainKind        string `json:"drain_kind"`
	BarrierConfirmed bool   `json:"barrier_confirmed"`
}
type resourceWire struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Owned  bool   `json:"owned"`
	Retain bool   `json:"retain"`
}
type resultWire struct {
	Version          int64          `json:"version"`
	Kind             string         `json:"kind"`
	OperationID      string         `json:"operation_id"`
	RequestSHA256    string         `json:"request_sha256"`
	ProgramSHA256    string         `json:"program_sha256"`
	ExecutionSHA256  string         `json:"execution_sha256"`
	Work             WorkIdentity   `json:"work"`
	Checkpoint       checkpointWire `json:"checkpoint"`
	Resources        []resourceWire `json:"resources"`
	Guards           []string       `json:"guards"`
	SourceClosed     bool           `json:"source_closed"`
	TargetClosed     bool           `json:"target_closed"`
	PrivateClosed    bool           `json:"private_closed"`
	DiagnosticsClean bool           `json:"diagnostics_clean"`
}

func (r resultWire) checkpoint() CheckpointEvidence {
	return CheckpointEvidence(r.Checkpoint)
}
func (r resultWire) obligations() []ResourceObligation {
	out := make([]ResourceObligation, len(r.Resources))
	for i, v := range r.Resources {
		out[i] = ResourceObligation(v)
	}
	return out
}

func parseCompletedResult(raw []byte, w ExecutionRequest, body []byte, savedE string) (*resultWire, error) {
	if len(raw) < 2 || len(raw) > nativeWireLimit || raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw[:len(raw)-1], '\n') {
		return nil, errors.New("native evidence result framing invalid")
	}
	var v resultWire
	if !decodeNativeWire(raw[:len(raw)-1], &v, "result") {
		return nil, errors.New("native evidence result fields invalid")
	}
	if !validCompletedResultBinding(v, w, body) || !validCompletedCheckpoint(v.checkpoint(), w, savedE) {
		return nil, errors.New("native evidence result binding invalid")
	}
	if !validEvidenceInventory(v.Resources) {
		return nil, errors.New("native evidence resource inventory invalid")
	}
	return &v, nil
}

func validCompletedResultBinding(v resultWire, w ExecutionRequest, body []byte) bool {
	request, execution := requestDigests(body)
	return v.Version == 1 && v.Kind == completedEvidenceKind && v.OperationID == w.OperationID && v.RequestSHA256 == hex.EncodeToString(request[:]) && v.ExecutionSHA256 == hex.EncodeToString(execution[:]) && v.ProgramSHA256 == hex.EncodeToString(w.ProgramSHA256[:]) && v.Work == w.Work && validIdentity(v.Work) && v.SourceClosed && v.TargetClosed && v.PrivateClosed && v.DiagnosticsClean && slices.Equal(v.Guards, completedGuards)
}

func validCompletedCheckpoint(c CheckpointEvidence, w ExecutionRequest, savedE string) bool {
	if !validCheckpointID(c.CheckpointID, w.Work.CheckpointGeneration) || !c.BarrierConfirmed {
		return false
	}
	e, err := sentinel.ParseLSN(savedE)
	if err != nil || e == 0 {
		return false
	}
	certified, err1 := sentinel.ParseLSN(c.CertifiedLSN)
	durable, err2 := sentinel.ParseLSN(c.DurableReplayLSN)
	end, err3 := sentinel.ParseLSN(c.DrainEndLSN)
	replay, err4 := sentinel.ParseLSN(c.DrainReplayLSN)
	return err1 == nil && err2 == nil && err3 == nil && err4 == nil && durable >= certified && end == e && (c.DrainKind == nativeDrainKeepalive || c.DrainKind == nativeDrainLaterBegin) && (c.DrainKind != nativeDrainKeepalive || replay >= e && replay <= certified) && (c.DrainKind != nativeDrainLaterBegin || replay > e)
}

func validEvidenceInventory(resources []resourceWire) bool {
	if len(resources) != 3 && len(resources) != 4 {
		return false
	}
	seen := map[string]bool{}
	for _, r := range resources {
		if seen[r.Kind] || !validLabel(r.Name) {
			return false
		}
		seen[r.Kind] = true
		switch r.Kind {
		case "replication_slot", "publication":
			if !r.Owned || r.Retain {
				return false
			}
		case resourceKindWorkdir:
			if r.Name != "/work/pgcopydb" || !r.Owned || !r.Retain {
				return false
			}
		case nativeResourceOrigin:
			if r.Owned || r.Retain {
				return false
			}
		default:
			return false
		}
	}
	return seen["replication_slot"] && seen["publication"] && seen[resourceKindWorkdir]
}

func lowerHex(s string, n int) bool {
	return len(s) == n && !strings.ContainsFunc(s, func(c rune) bool { return (c < '0' || c > '9') && (c < 'a' || c > 'f') })
}

const (
	nativeExecutionWireKeys = "namespace migration_name pvc_name job_name migration_uid pvc_uid job_uid worker_job_uid migration_generation attempt role work image_ref platform_image_id template_sha256 connection_reference_sha256"
	nativeContainerWireKeys = "pod_uid container_name container_id restart_count image_id template_sha256"
)

var nativeWireKeys = map[string]string{
	"cleanup_rejoin_request":                        cleanupRejoinRequestWireKeys,
	"cleanup_rejoin_request.execution":              nativeExecutionWireKeys,
	"cleanup_rejoin_request.execution.work":         sourceWorkWireKeys,
	"cleanup_rejoin_request.container":              nativeContainerWireKeys,
	"cleanup_rejoin_completed_evidence":             "version kind operation_id program_sha256 request_sha256 execution_sha256 work checkpoint resources guards source_closed target_closed materialization_closed private_closed diagnostics_clean cleanup_pending source_resources_checked",
	"cleanup_rejoin_completed_evidence.work":        sourceWorkWireKeys,
	"cleanup_rejoin_completed_evidence.checkpoint":  cleanupRejoinCheckpointWireKeys,
	"cleanup_rejoin_completed_evidence.resources[]": cleanedResourceWireKeys,

	"cleaned_request":                        "version kind operation_id program_sha256 execution pod_name container saved_e",
	"cleaned_request.execution":              nativeExecutionWireKeys,
	"cleaned_request.execution.work":         sourceWorkWireKeys,
	"cleaned_request.container":              nativeContainerWireKeys,
	"cleaned_completed_evidence":             "version kind operation_id program_sha256 request_sha256 execution_sha256 work checkpoint resources guards source_closed target_closed materialization_closed private_closed diagnostics_clean cleanup_complete source_resources_absent",
	"cleaned_completed_evidence.work":        sourceWorkWireKeys,
	"cleaned_completed_evidence.checkpoint":  "checkpoint_id certified_lsn durable_replay_lsn drain_end_lsn drain_replay_lsn drain_kind barrier_confirmed",
	"cleaned_completed_evidence.resources[]": cleanedResourceWireKeys,

	"owned_request":                 "version kind operation_id program_sha256 execution pod_name container",
	"owned_request.execution":       nativeExecutionWireKeys,
	"owned_request.execution.work":  sourceWorkWireKeys,
	"owned_request.container":       nativeContainerWireKeys,
	"owned_inventory":               "version kind operation_id program_sha256 request_sha256 execution_sha256 work resources source_closed target_closed materialization_closed private_closed diagnostics_clean",
	"owned_inventory.work":          sourceWorkWireKeys,
	"owned_inventory.resources[]":   "kind name owned retain",
	"retry_request":                 "version kind operation_id program_sha256 execution pod_name container inputs",
	"retry_request.execution":       nativeExecutionWireKeys,
	"retry_request.execution.work":  sourceWorkWireKeys,
	"retry_request.container":       nativeContainerWireKeys,
	"retry_request.inputs":          "commit_test expect_local",
	"target_retry":                  "version kind operation_id program_sha256 request_sha256 execution_sha256 work observed_at_unix census_known total syncrep gate_waiting oldest_syncrep_wait_unix application_attribution_known target_synchronous_commit synchronous_commit_admitted probe_attempted probe_committed target_closed materialization_closed diagnostics_clean",
	"target_retry.work":             sourceWorkWireKeys,
	"source_request":                "version kind operation_id program_sha256 execution pod_name container",
	"source_request.execution":      nativeExecutionWireKeys,
	"source_request.execution.work": sourceWorkWireKeys,
	"source_request.container":      nativeContainerWireKeys,
	"source_head":                   "version kind operation_id program_sha256 request_sha256 execution_sha256 work source_binding_sha256 head_lsn source_closed private_closed diagnostics_clean",
	"source_head.work":              sourceWorkWireKeys,
	"active_lag":                    "version kind operation_id program_sha256 request_sha256 execution_sha256 work source_binding_sha256 lag_bytes source_closed private_closed diagnostics_clean",
	"active_lag.work":               sourceWorkWireKeys,
	"native_preflight":              "version kind operation_id program_sha256 source_major target_major pg_dump_major pg_restore_major checks source_closed target_closed materialization_closed diagnostics_clean",
	"request":                       "version kind operation_id program_sha256 execution pod_name container saved_e",
	"request.execution":             nativeExecutionWireKeys,
	"request.execution.work":        "migration_uid run_id workdir_id checkpoint_generation",
	"request.container":             nativeContainerWireKeys,
	"result":                        "version kind operation_id request_sha256 program_sha256 execution_sha256 work checkpoint resources guards source_closed target_closed private_closed diagnostics_clean",
	"result.work":                   "migration_uid run_id workdir_id checkpoint_generation",
	"result.checkpoint":             "checkpoint_id certified_lsn durable_replay_lsn drain_end_lsn drain_replay_lsn drain_kind barrier_confirmed",
	"result.resources[]":            "kind name owned retain",
}

func decodeNativeWire(raw []byte, out any, root string) bool {
	if len(raw) == 0 || len(raw) > nativeWireLimit || !utf8.Valid(raw) || bytes.HasPrefix(raw, []byte{0xef, 0xbb, 0xbf}) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if !nativeWireValue(d, root, 0) {
		return false
	}
	if _, err := d.Token(); err != io.EOF {
		return false
	}
	typed := json.NewDecoder(bytes.NewReader(raw))
	typed.DisallowUnknownFields()
	return typed.Decode(out) == nil
}

func nativeWireValue(d *json.Decoder, path string, depth int) bool {
	if depth > 32 {
		return false
	}
	token, err := d.Token()
	if err != nil || token == nil {
		return false
	}
	delim, container := token.(json.Delim)
	if !container {
		if number, ok := token.(json.Number); ok {
			n, err := strconv.ParseInt(string(number), 10, 64)
			return err == nil && strconv.FormatInt(n, 10) == string(number)
		}
		return true
	}
	if delim == '{' {
		keys, exists := nativeWireKeys[path]
		if !exists {
			return false
		}
		want := strings.Fields(keys)
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			name, ok := key.(string)
			if err != nil || !ok || seen[name] || !slices.Contains(want, name) {
				return false
			}
			seen[name] = true
			if !nativeWireValue(d, path+"."+name, depth+1) {
				return false
			}
		}
		end, err := d.Token()
		return err == nil && end == json.Delim('}') && len(seen) == len(want)
	}
	if delim == '[' {
		for d.More() {
			if !nativeWireValue(d, path+"[]", depth+1) {
				return false
			}
		}
		end, err := d.Token()
		return err == nil && end == json.Delim(']')
	}
	return false
}
