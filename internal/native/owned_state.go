package native

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const ownedInventoryKind = "owned_inventory"

var ErrNativeOwnedStateUnavailable = errors.New("native owned inventory unavailable")

// The caller must independently admit the canonical helper and retained original terminal worker before and after this read.
func (r *Reader) ReadOwnedState(ctx context.Context, w ExecutionData) (AdmittedOwnedState, error) {
	if r == nil || r.api == nil || r.transport == nil || ctx.Err() != nil || !validOwnedRequestExecution(w) {
		return AdmittedOwnedState{}, ErrNativeOwnedStateUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	before, err := r.admitOwned(ctx, w, false)
	if err != nil {
		return AdmittedOwnedState{}, ErrNativeOwnedStateUnavailable
	}
	w.PodName, w.Container = before.helper.pod.Name, before.helper.container
	body, err := ownedRequest(w)
	if err != nil {
		return AdmittedOwnedState{}, ErrNativeOwnedStateUnavailable
	}
	relay := OwnedInventoryRequestWriterProgram()
	if len(relay) == 0 || len(relay) > 16384 {
		return AdmittedOwnedState{}, ErrNativeOwnedStateUnavailable
	}
	argv := []string{nativeEnvExecutable, "-u", nativePreloadEnv, "-u", nativeLibraryPathEnv, nativePythonExecutable, "-I", "-B", "-c", string(relay), w.OperationID}
	relayCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	outcome, err := r.transport.InPodNative(relayCtx, w.Namespace, w.PodName, w.Container, argv, body)
	stop()
	if err != nil || !outcome.Successful() {
		return AdmittedOwnedState{}, ErrNativeOwnedStateUnavailable
	}
	completeCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	complete, err := r.transport.JobCompletedNative(completeCtx, w.Namespace, w.JobUID, w.Container)
	stop()
	if err != nil || !complete.Successful() || complete.Payload() != nil {
		return AdmittedOwnedState{}, ErrNativeOwnedStateUnavailable
	}
	after, err := r.admitOwned(ctx, w, true)
	if err != nil || !before.same(after) || ctx.Err() != nil {
		return AdmittedOwnedState{}, ErrNativeOwnedStateUnavailable
	}
	result, err := parseOwnedResult(outcome.Payload(), w, body)
	if err != nil || ctx.Err() != nil {
		return AdmittedOwnedState{}, ErrNativeOwnedStateUnavailable
	}
	resources := make([]ResourceObligation, len(result.Resources))
	for i, v := range result.Resources {
		resources[i] = ResourceObligation(v)
	}
	return AdmittedOwnedState{work: result.Work, resources: resources, admitted: true}, nil
}

func ownedRequest(w ExecutionData) ([]byte, error) {
	if !validOwnedRequestExecution(w) {
		return nil, ErrNativeOwnedStateUnavailable
	}
	raw, err := json.Marshal(sourceRequestValue(ownedInventoryKind, w))
	if err != nil {
		return nil, ErrNativeOwnedStateUnavailable
	}
	if _, err := parseOwnedRequest(raw); err != nil {
		return nil, ErrNativeOwnedStateUnavailable
	}
	return raw, nil
}

func parseOwnedRequest(raw []byte) (*sourceRequestWire, error) {
	var v sourceRequestWire
	if !decodeNativeWire(raw, &v, "owned_request") || v.Version != 1 || v.Kind != ownedInventoryKind {
		return nil, ErrNativeOwnedStateUnavailable
	}
	x, c := v.Execution, v.Container
	w := ExecutionData{Namespace: x.Namespace, MigrationName: x.MigrationName, PVCName: x.PVCName, JobName: x.JobName, PodName: v.PodName, MigrationUID: types.UID(x.MigrationUID), PVCUID: types.UID(x.PVCUID), JobUID: types.UID(x.JobUID), WorkerJobUID: types.UID(x.WorkerJobUID), MigrationGeneration: x.MigrationGeneration, Attempt: x.Attempt, Role: x.Role, Work: x.Work, ImageRef: x.ImageRef, PlatformImageID: x.PlatformImageID, OperationID: v.OperationID}
	for _, field := range []struct {
		raw string
		out *[32]byte
	}{{v.ProgramSHA256, &w.ProgramSHA256}, {x.TemplateSHA256, &w.TemplateSHA256}, {x.ConnectionReferenceSHA256, &w.ConnectionReferenceSHA256}} {
		if !lowerHex(field.raw, 64) {
			return nil, ErrNativeOwnedStateUnavailable
		}
		decoded, err := hex.DecodeString(field.raw)
		if err != nil {
			return nil, ErrNativeOwnedStateUnavailable
		}
		copy(field.out[:], decoded)
	}
	if c.TemplateSHA256 != x.TemplateSHA256 {
		return nil, ErrNativeOwnedStateUnavailable
	}
	w.Container = podexec.NativeContainerBinding{PodUID: types.UID(c.PodUID), ContainerName: c.ContainerName, ContainerID: c.ContainerID, RestartCount: c.RestartCount, ImageID: c.ImageID, TemplateSHA256: w.TemplateSHA256}
	if !validOwnedRequestExecution(w) {
		return nil, ErrNativeOwnedStateUnavailable
	}
	return &v, nil
}

func validOwnedExecution(w ExecutionData) bool {
	if w.Role != JobRoleCleanup || w.JobUID == w.WorkerJobUID || w.MigrationGeneration <= 0 || w.Attempt < 1 || !validIdentity(w.Work) || w.Work.MigrationUID != string(w.MigrationUID) || w.Work.WorkdirID != string(w.PVCUID) || !lowerHex(w.OperationID, 32) || w.ProgramSHA256 != OwnedInventoryProgramSHA256() || w.ProgramSHA256 == ([32]byte{}) || w.TemplateSHA256 == ([32]byte{}) || w.ConnectionReferenceSHA256 == ([32]byte{}) {
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

func validOwnedRequestExecution(w ExecutionData) bool {
	c := w.Container
	return validOwnedExecution(w) && len(validation.IsDNS1123Subdomain(w.PodName)) == 0 && retryUID(c.PodUID) && c.ContainerName == nativeContainerName && validLabel(c.ContainerID) && c.RestartCount == 0 && c.ImageID == w.PlatformImageID && c.TemplateSHA256 == w.TemplateSHA256
}

type ownedResultWire struct {
	Version               int64          `json:"version"`
	Kind                  string         `json:"kind"`
	OperationID           string         `json:"operation_id"`
	ProgramSHA256         string         `json:"program_sha256"`
	RequestSHA256         string         `json:"request_sha256"`
	ExecutionSHA256       string         `json:"execution_sha256"`
	Work                  WorkIdentity   `json:"work"`
	Resources             []resourceWire `json:"resources"`
	SourceClosed          bool           `json:"source_closed"`
	TargetClosed          bool           `json:"target_closed"`
	MaterializationClosed bool           `json:"materialization_closed"`
	PrivateClosed         bool           `json:"private_closed"`
	DiagnosticsClean      bool           `json:"diagnostics_clean"`
}

func parseOwnedResult(raw []byte, w ExecutionData, body []byte) (*ownedResultWire, error) {
	if !validOwnedRequestExecution(w) || len(raw) < 2 || len(raw) > nativeWireLimit || raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw[:len(raw)-1], '\n') {
		return nil, ErrNativeOwnedStateUnavailable
	}
	request, err := parseOwnedRequest(body)
	if err != nil || *request != sourceRequestValue(ownedInventoryKind, w) {
		return nil, ErrNativeOwnedStateUnavailable
	}
	var v ownedResultWire
	if !decodeNativeWire(raw[:len(raw)-1], &v, ownedInventoryKind) {
		return nil, ErrNativeOwnedStateUnavailable
	}
	digest, execution := requestDigests(body)
	if v.Version != 1 || v.Kind != ownedInventoryKind || v.OperationID != w.OperationID || v.ProgramSHA256 != hex.EncodeToString(w.ProgramSHA256[:]) || v.RequestSHA256 != hex.EncodeToString(digest[:]) || v.ExecutionSHA256 != hex.EncodeToString(execution[:]) || v.Work != w.Work || !v.SourceClosed || !v.TargetClosed || !v.MaterializationClosed || !v.PrivateClosed || !v.DiagnosticsClean || !validEvidenceInventory(v.Resources) {
		return nil, ErrNativeOwnedStateUnavailable
	}
	// Native FollowState also bounds the recorded unowned origin name to 63 bytes.
	for _, resource := range v.Resources {
		if resource.Kind != resourceKindWorkdir && len(resource.Name) > 63 {
			return nil, ErrNativeOwnedStateUnavailable
		}
	}
	return &v, nil
}
