package native

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const cleanupRejoinCompletionKind = "cleanup_rejoin_completed_evidence"
const cleanupRejoinRequestWireKeys = "version kind operation_id program_sha256 execution pod_name container saved_e"
const cleanupRejoinCheckpointWireKeys = "checkpoint_id certified_lsn durable_replay_lsn drain_end_lsn drain_replay_lsn drain_kind barrier_confirmed"

var ErrNativeCleanupRejoinCompletionUnavailable = errors.New("native cleanup rejoin completion unavailable")

func (r *Reader) ReadCleanupRejoinCompleted(ctx context.Context, w ExecutionData, savedE string) (*CompletedObservation, error) {
	if r == nil || r.api == nil || r.transport == nil || !validCleanupRejoinExecution(w) || ctx.Err() != nil {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	before, err := r.admitCleanupRejoin(ctx, w, savedE, false)
	if err != nil {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	w.PodName, w.Container = before.helper.pod.Name, before.helper.container
	body, err := cleanupRejoinRequest(w, savedE)
	if err != nil {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	relay := CleanupRejoinCompletionRequestWriterProgram()
	if len(relay) == 0 || len(relay) > 16384 {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	argv := []string{nativeEnvExecutable, "-u", nativePreloadEnv, "-u", nativeLibraryPathEnv, nativePythonExecutable, "-I", "-B", "-c", string(relay), w.OperationID}
	relayCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	outcome, err := r.transport.InPodNative(relayCtx, w.Namespace, w.PodName, w.Container, argv, body)
	stop()
	if err != nil || !outcome.Successful() || ctx.Err() != nil {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	completeCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	complete, err := r.transport.JobCompletedNative(completeCtx, w.Namespace, w.JobUID, w.Container)
	stop()
	if err != nil || !complete.Successful() || complete.Payload() != nil {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	after, err := r.admitCleanupRejoin(ctx, w, savedE, true)
	if err != nil || !before.same(after) || ctx.Err() != nil {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	result, err := parseCleanupRejoinResult(outcome.Payload(), w, body, savedE)
	if err != nil || ctx.Err() != nil {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	return &CompletedObservation{checkpoint: ConfirmedCheckpoint{evidence: result.checkpoint(), confirmed: true}, owned: AdmittedOwnedState{work: result.Work, resources: result.obligations(), admitted: true}}, nil
}

func cleanupRejoinRequestValue(w ExecutionData, savedE string) requestWire {
	v := sourceRequestValue(cleanupRejoinCompletionKind, w)
	return requestWire{Version: v.Version, Kind: v.Kind, OperationID: v.OperationID, ProgramSHA256: v.ProgramSHA256, Execution: v.Execution, PodName: v.PodName, Container: v.Container, SavedE: savedE}
}

func cleanupRejoinRequest(w ExecutionData, savedE string) ([]byte, error) {
	if !validCleanupRejoinRequestExecution(w) {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	e, err := sentinel.ParseLSN(savedE)
	if err != nil || e == 0 {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	body, err := json.Marshal(cleanupRejoinRequestValue(w, savedE))
	var v requestWire
	if err != nil || !decodeNativeWire(body, &v, "cleanup_rejoin_request") {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	return body, nil
}

func validCleanupRejoinExecution(w ExecutionData) bool {
	if w.Role != JobRoleEvidence || w.ProgramSHA256 != CleanupRejoinCompletionProgramSHA256() || w.JobUID == w.WorkerJobUID || w.MigrationGeneration <= 0 || w.Attempt < 1 || !validIdentity(w.Work) || w.Work.MigrationUID != string(w.MigrationUID) || w.Work.WorkdirID != string(w.PVCUID) || !lowerHex(w.OperationID, 32) || w.TemplateSHA256 == ([32]byte{}) || w.ConnectionReferenceSHA256 == ([32]byte{}) {
		return false
	}
	for _, name := range []string{w.MigrationName, w.PVCName, w.JobName} {
		if len(validation.IsDNS1123Subdomain(name)) != 0 {
			return false
		}
	}
	if len(validation.IsDNS1123Label(w.Namespace)) != 0 {
		return false
	}
	for _, uid := range []types.UID{w.MigrationUID, w.PVCUID, w.JobUID, w.WorkerJobUID} {
		if !retryUID(uid) {
			return false
		}
	}
	return pinnedImage(w.ImageRef) && platformImage(w.PlatformImageID)
}

func validCleanupRejoinRequestExecution(w ExecutionData) bool {
	c := w.Container
	return validCleanupRejoinExecution(w) && len(validation.IsDNS1123Subdomain(w.PodName)) == 0 && retryUID(c.PodUID) && c.ContainerName == nativeContainerName && validLabel(c.ContainerID) && c.RestartCount == 0 && c.ImageID == w.PlatformImageID && c.TemplateSHA256 == w.TemplateSHA256
}

type cleanupRejoinResultWire struct {
	resultWire
	MaterializationClosed  bool `json:"materialization_closed"`
	CleanupPending         bool `json:"cleanup_pending"`
	SourceResourcesChecked bool `json:"source_resources_checked"`
}

func parseCleanupRejoinResult(raw []byte, w ExecutionData, body []byte, savedE string) (*cleanupRejoinResultWire, error) {
	if !validCleanupRejoinRequestExecution(w) || len(raw) < 2 || len(raw) > nativeWireLimit || raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw[:len(raw)-1], '\n') {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	var request requestWire
	if !decodeNativeWire(body, &request, "cleanup_rejoin_request") || request != cleanupRejoinRequestValue(w, savedE) {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	var v cleanupRejoinResultWire
	if !decodeNativeWire(raw[:len(raw)-1], &v, cleanupRejoinCompletionKind) {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	digest, execution := requestDigests(body)
	if v.Version != 1 || v.Kind != cleanupRejoinCompletionKind || v.OperationID != w.OperationID || v.ProgramSHA256 != hex.EncodeToString(w.ProgramSHA256[:]) || v.RequestSHA256 != hex.EncodeToString(digest[:]) || v.ExecutionSHA256 != hex.EncodeToString(execution[:]) || v.Work != w.Work || !v.SourceClosed || !v.TargetClosed || !v.MaterializationClosed || !v.PrivateClosed || !v.DiagnosticsClean || !v.CleanupPending || !v.SourceResourcesChecked || !slices.Equal(v.Guards, completedGuards) || !validCompletedCheckpoint(v.checkpoint(), cleanedExpectation(w), savedE) || !validEvidenceInventory(v.Resources) {
		return nil, ErrNativeCleanupRejoinCompletionUnavailable
	}
	for _, resource := range v.Resources {
		if resource.Kind != resourceKindWorkdir && len(resource.Name) > 63 {
			return nil, ErrNativeCleanupRejoinCompletionUnavailable
		}
	}
	return &v, nil
}
