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

const cleanedCompletionKind = "cleaned_completed_evidence"
const cleanedResourceWireKeys = "kind name owned retain"

var ErrNativeCleanedCompletionUnavailable = errors.New("native cleaned completion unavailable")

func (r *Reader) ReadCleanedCompleted(ctx context.Context, w ExecutionData, savedE string) (*CompletedObservation, error) {
	if r == nil || r.api == nil || r.transport == nil || !validCleanedExecution(w) || ctx.Err() != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	before, err := r.admitCleaned(ctx, w, savedE, false)
	if err != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	w.PodName, w.Container = before.helper.pod.Name, before.helper.container
	body, err := cleanedRequest(w, savedE)
	if err != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	relay := CleanedCompletionRequestWriterProgram()
	if len(relay) == 0 || len(relay) > 16384 {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	argv := []string{nativeEnvExecutable, "-u", nativePreloadEnv, "-u", nativeLibraryPathEnv, nativePythonExecutable, "-I", "-B", "-c", string(relay), w.OperationID}
	relayCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	outcome, err := r.transport.InPodNative(relayCtx, w.Namespace, w.PodName, w.Container, argv, body)
	stop()
	if err != nil || !outcome.Successful() || ctx.Err() != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	completeCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	complete, err := r.transport.JobCompletedNative(completeCtx, w.Namespace, w.JobUID, w.Container)
	stop()
	if err != nil || !complete.Successful() || complete.Payload() != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	after, err := r.admitCleaned(ctx, w, savedE, true)
	if err != nil || !before.same(after) || ctx.Err() != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	result, err := parseCleanedResult(outcome.Payload(), w, body, savedE)
	if err != nil || ctx.Err() != nil {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	return &CompletedObservation{checkpoint: ConfirmedCheckpoint{evidence: result.checkpoint(), confirmed: true}, owned: AdmittedOwnedState{work: result.Work, resources: result.obligations(), admitted: true}}, nil
}

func cleanedExpectation(w ExecutionData) ExecutionRequest {
	return ExecutionRequest{Namespace: w.Namespace, MigrationName: w.MigrationName, PVCName: w.PVCName, JobName: w.JobName, MigrationUID: w.MigrationUID, PVCUID: w.PVCUID, JobUID: w.JobUID, WorkerJobUID: w.WorkerJobUID, MigrationGeneration: w.MigrationGeneration, Attempt: w.Attempt, Role: w.Role, Work: w.Work, ImageRef: w.ImageRef, PlatformImageID: w.PlatformImageID, TemplateSHA256: w.TemplateSHA256, ConnectionReferenceSHA256: w.ConnectionReferenceSHA256, OperationID: w.OperationID, ProgramSHA256: w.ProgramSHA256}
}

func cleanedRequestValue(w ExecutionData, savedE string) requestWire {
	v := sourceRequestValue(cleanedCompletionKind, w)
	return requestWire{Version: v.Version, Kind: v.Kind, OperationID: v.OperationID, ProgramSHA256: v.ProgramSHA256, Execution: v.Execution, PodName: v.PodName, Container: v.Container, SavedE: savedE}
}

func cleanedRequest(w ExecutionData, savedE string) ([]byte, error) {
	if !validCleanedRequestExecution(w) {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	e, err := sentinel.ParseLSN(savedE)
	if err != nil || e == 0 {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	body, err := json.Marshal(cleanedRequestValue(w, savedE))
	var v requestWire
	if err != nil || !decodeNativeWire(body, &v, "cleaned_request") {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	return body, nil
}

func validCleanedExecution(w ExecutionData) bool {
	if w.Role != JobRoleEvidence || w.ProgramSHA256 != CleanedCompletionProgramSHA256() || w.JobUID == w.WorkerJobUID || w.MigrationGeneration <= 0 || w.Attempt < 1 || !validIdentity(w.Work) || w.Work.MigrationUID != string(w.MigrationUID) || w.Work.WorkdirID != string(w.PVCUID) || !lowerHex(w.OperationID, 32) || w.TemplateSHA256 == ([32]byte{}) || w.ConnectionReferenceSHA256 == ([32]byte{}) {
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

func validCleanedRequestExecution(w ExecutionData) bool {
	c := w.Container
	return validCleanedExecution(w) && len(validation.IsDNS1123Subdomain(w.PodName)) == 0 && retryUID(c.PodUID) && c.ContainerName == nativeContainerName && validLabel(c.ContainerID) && c.RestartCount == 0 && c.ImageID == w.PlatformImageID && c.TemplateSHA256 == w.TemplateSHA256
}

type cleanedResultWire struct {
	resultWire
	MaterializationClosed bool `json:"materialization_closed"`
	CleanupComplete       bool `json:"cleanup_complete"`
	SourceResourcesAbsent bool `json:"source_resources_absent"`
}

func parseCleanedResult(raw []byte, w ExecutionData, body []byte, savedE string) (*cleanedResultWire, error) {
	if !validCleanedRequestExecution(w) || len(raw) < 2 || len(raw) > nativeWireLimit || raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw[:len(raw)-1], '\n') {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	var request requestWire
	if !decodeNativeWire(body, &request, "cleaned_request") || request != cleanedRequestValue(w, savedE) {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	var v cleanedResultWire
	if !decodeNativeWire(raw[:len(raw)-1], &v, cleanedCompletionKind) {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	digest, execution := requestDigests(body)
	if v.Version != 1 || v.Kind != cleanedCompletionKind || v.OperationID != w.OperationID || v.ProgramSHA256 != hex.EncodeToString(w.ProgramSHA256[:]) || v.RequestSHA256 != hex.EncodeToString(digest[:]) || v.ExecutionSHA256 != hex.EncodeToString(execution[:]) || v.Work != w.Work || !v.SourceClosed || !v.TargetClosed || !v.MaterializationClosed || !v.PrivateClosed || !v.DiagnosticsClean || !v.CleanupComplete || !v.SourceResourcesAbsent || !slices.Equal(v.Guards, completedGuards) || !validCompletedCheckpoint(v.checkpoint(), cleanedExpectation(w), savedE) || !validEvidenceInventory(v.Resources) {
		return nil, ErrNativeCleanedCompletionUnavailable
	}
	for _, resource := range v.Resources {
		if resource.Kind != resourceKindWorkdir && len(resource.Name) > 63 {
			return nil, ErrNativeCleanedCompletionUnavailable
		}
	}
	return &v, nil
}
