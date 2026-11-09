package native

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const targetRetryKind = "target_retry"

var ErrNativeRetryUnavailable = errors.New("native retry observation unavailable")

type RetryInputs struct {
	CommitTest  bool `json:"commit_test"`
	ExpectLocal bool `json:"expect_local"`
}

// RetryGateObservation is census and probe data, with no completion authority.
type RetryGateObservation struct {
	ObservedAtUnix, Total, SyncRep, GateWaiting, OldestSyncRepWaitUnix int64
	ApplicationAttributionKnown                                        bool
	TargetSynchronousCommit                                            string
	SynchronousCommitAdmitted, ProbeAttempted, ProbeCommitted          bool
}

type RetryReadiness struct {
	observation RetryGateObservation
	admitted    bool
}

func (r RetryReadiness) Observation() (RetryGateObservation, bool) {
	if !r.admitted {
		return RetryGateObservation{}, false
	}
	return r.observation, true
}

// The caller must independently admit the canonical original failed worker before and after this call.
// Target clock skew outside this read's interval conservatively refuses the observation.
func (r *Reader) ReadRetryReadiness(ctx context.Context, w ExecutionData, in RetryInputs) (*RetryReadiness, error) {
	if r == nil || r.api == nil || r.transport == nil || ctx.Err() != nil {
		return nil, ErrNativeRetryUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	started := time.Now().Unix()
	before, err := r.admitRetry(ctx, w, false)
	if err != nil {
		return nil, ErrNativeRetryUnavailable
	}
	w.PodName, w.Container = before.helper.pod.Name, before.helper.container
	body, err := retryRequest(w, in)
	if err != nil {
		return nil, ErrNativeRetryUnavailable
	}
	relay := EvidenceRequestWriterProgram()
	if len(relay) == 0 || len(relay) > 16384 {
		return nil, ErrNativeRetryUnavailable
	}
	argv := []string{"/usr/bin/env", "-u", "LD_PRELOAD", "-u", "LD_LIBRARY_PATH", "/usr/bin/python3", "-I", "-B", "-c", string(relay), w.OperationID}
	relayCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	outcome, err := r.transport.InPodNative(relayCtx, w.Namespace, w.PodName, w.Container, argv, body)
	stop()
	if err != nil || !outcome.Successful() {
		return nil, ErrNativeRetryUnavailable
	}
	completeCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	complete, err := r.transport.JobCompletedNative(completeCtx, w.Namespace, w.JobUID, w.Container)
	stop()
	if err != nil || !complete.Successful() || complete.Payload() != nil {
		return nil, ErrNativeRetryUnavailable
	}
	after, err := r.admitRetry(ctx, w, true)
	if err != nil || !before.same(after) || ctx.Err() != nil {
		return nil, ErrNativeRetryUnavailable
	}
	result, err := parseRetryResult(outcome.Payload(), w, body, in, started, time.Now().Unix())
	if err != nil || ctx.Err() != nil {
		return nil, ErrNativeRetryUnavailable
	}
	return &RetryReadiness{observation: result.observation(), admitted: true}, nil
}

type retryRequestWire struct {
	Version       int64         `json:"version"`
	Kind          string        `json:"kind"`
	OperationID   string        `json:"operation_id"`
	ProgramSHA256 string        `json:"program_sha256"`
	Execution     executionWire `json:"execution"`
	PodName       string        `json:"pod_name"`
	Container     containerWire `json:"container"`
	Inputs        RetryInputs   `json:"inputs"`
}

func retryRequestValue(w ExecutionData, in RetryInputs) retryRequestWire {
	v := sourceRequestValue(targetRetryKind, w)
	return retryRequestWire{v.Version, v.Kind, v.OperationID, v.ProgramSHA256, v.Execution, v.PodName, v.Container, in}
}

func retryRequest(w ExecutionData, in RetryInputs) ([]byte, error) {
	if !validRetryRequestExecution(w) {
		return nil, ErrNativeRetryUnavailable
	}
	raw, err := json.Marshal(retryRequestValue(w, in))
	if err != nil {
		return nil, ErrNativeRetryUnavailable
	}
	if _, err = parseRetryRequest(raw); err != nil {
		return nil, ErrNativeRetryUnavailable
	}
	return raw, nil
}

func parseRetryRequest(raw []byte) (*retryRequestWire, error) {
	var v retryRequestWire
	if !decodeNativeWire(raw, &v, "retry_request") || v.Version != 1 || v.Kind != targetRetryKind || !lowerHex(v.ProgramSHA256, 64) {
		return nil, ErrNativeRetryUnavailable
	}
	x, c := v.Execution, v.Container
	w := ExecutionData{Namespace: x.Namespace, MigrationName: x.MigrationName, PVCName: x.PVCName, JobName: x.JobName, PodName: v.PodName, MigrationUID: types.UID(x.MigrationUID), PVCUID: types.UID(x.PVCUID), JobUID: types.UID(x.JobUID), WorkerJobUID: types.UID(x.WorkerJobUID), MigrationGeneration: x.MigrationGeneration, Attempt: x.Attempt, Role: x.Role, Work: x.Work, ImageRef: x.ImageRef, PlatformImageID: x.PlatformImageID, OperationID: v.OperationID}
	for _, field := range []struct {
		raw string
		out *[32]byte
	}{{v.ProgramSHA256, &w.ProgramSHA256}, {x.TemplateSHA256, &w.TemplateSHA256}, {x.ConnectionReferenceSHA256, &w.ConnectionReferenceSHA256}} {
		if !lowerHex(field.raw, 64) {
			return nil, ErrNativeRetryUnavailable
		}
		decoded, err := hex.DecodeString(field.raw)
		if err != nil {
			return nil, ErrNativeRetryUnavailable
		}
		copy(field.out[:], decoded)
	}
	if c.TemplateSHA256 != x.TemplateSHA256 {
		return nil, ErrNativeRetryUnavailable
	}
	w.Container = podexec.NativeContainerBinding{PodUID: types.UID(c.PodUID), ContainerName: c.ContainerName, ContainerID: c.ContainerID, RestartCount: c.RestartCount, ImageID: c.ImageID, TemplateSHA256: w.TemplateSHA256}
	if !validRetryRequestExecution(w) {
		return nil, ErrNativeRetryUnavailable
	}
	return &v, nil
}

func validRetryExecution(w ExecutionData) bool {
	if w.Role != JobRoleRetryGate || w.JobUID == w.WorkerJobUID || w.MigrationGeneration <= 0 || w.Attempt < 1 || !validIdentity(w.Work) || w.Work.MigrationUID != string(w.MigrationUID) || w.Work.WorkdirID != string(w.PVCUID) || !lowerHex(w.OperationID, 32) || w.ProgramSHA256 != EvidenceProgramSHA256() || w.ProgramSHA256 == ([32]byte{}) || w.TemplateSHA256 == ([32]byte{}) || w.ConnectionReferenceSHA256 == ([32]byte{}) {
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

func retryUID(uid types.UID) bool {
	s := string(uid)
	return validLabel(s) && len(s) <= 256 && strings.Trim(s, "0-") != "" && !strings.ContainsAny(s, `/\`) && !strings.ContainsFunc(s, func(c rune) bool { return c < 33 || c > 126 })
}

func validRetryRequestExecution(w ExecutionData) bool {
	c := w.Container
	return validRetryExecution(w) && len(validation.IsDNS1123Subdomain(w.PodName)) == 0 && retryUID(c.PodUID) && c.ContainerName == nativeContainerName && validLabel(c.ContainerID) && c.RestartCount == 0 && c.ImageID == w.PlatformImageID && c.TemplateSHA256 == w.TemplateSHA256
}

type retryResultWire struct {
	Version                     int64        `json:"version"`
	Kind                        string       `json:"kind"`
	OperationID                 string       `json:"operation_id"`
	ProgramSHA256               string       `json:"program_sha256"`
	RequestSHA256               string       `json:"request_sha256"`
	ExecutionSHA256             string       `json:"execution_sha256"`
	Work                        WorkIdentity `json:"work"`
	ObservedAtUnix              int64        `json:"observed_at_unix"`
	CensusKnown                 bool         `json:"census_known"`
	Total                       int64        `json:"total"`
	SyncRep                     int64        `json:"syncrep"`
	GateWaiting                 int64        `json:"gate_waiting"`
	OldestSyncRepWaitUnix       int64        `json:"oldest_syncrep_wait_unix"`
	ApplicationAttributionKnown bool         `json:"application_attribution_known"`
	TargetSynchronousCommit     string       `json:"target_synchronous_commit"`
	SynchronousCommitAdmitted   bool         `json:"synchronous_commit_admitted"`
	ProbeAttempted              bool         `json:"probe_attempted"`
	ProbeCommitted              bool         `json:"probe_committed"`
	TargetClosed                bool         `json:"target_closed"`
	MaterializationClosed       bool         `json:"materialization_closed"`
	DiagnosticsClean            bool         `json:"diagnostics_clean"`
}

func parseRetryResult(raw []byte, w ExecutionData, body []byte, in RetryInputs, started, finished int64) (*retryResultWire, error) {
	if !validRetryRequestExecution(w) || len(raw) < 2 || len(raw) > nativeWireLimit || raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw[:len(raw)-1], '\n') {
		return nil, ErrNativeRetryUnavailable
	}
	request, err := parseRetryRequest(body)
	if err != nil || *request != retryRequestValue(w, in) {
		return nil, ErrNativeRetryUnavailable
	}
	var v retryResultWire
	if !decodeNativeWire(raw[:len(raw)-1], &v, targetRetryKind) {
		return nil, ErrNativeRetryUnavailable
	}
	if !validRetryResultBinding(v, w, body) || !validRetryResultCensus(v, started, finished) || !validRetryResultProbe(v, in) {
		return nil, ErrNativeRetryUnavailable
	}
	return &v, nil
}

func validRetryResultBinding(v retryResultWire, w ExecutionData, body []byte) bool {
	requestSHA, executionSHA := requestDigests(body)
	return v.Version == 1 && v.Kind == targetRetryKind && v.OperationID == w.OperationID && v.ProgramSHA256 == hex.EncodeToString(w.ProgramSHA256[:]) && v.RequestSHA256 == hex.EncodeToString(requestSHA[:]) && v.ExecutionSHA256 == hex.EncodeToString(executionSHA[:]) && v.Work == w.Work && v.CensusKnown && v.TargetClosed && v.MaterializationClosed && v.DiagnosticsClean
}

func validRetryResultCensus(v retryResultWire, started, finished int64) bool {
	return started > 0 && finished >= started && v.ObservedAtUnix >= started && v.ObservedAtUnix <= finished && v.Total >= 0 && v.SyncRep >= 0 && v.SyncRep <= v.Total && v.GateWaiting >= 0 && (v.SyncRep != 0 || v.OldestSyncRepWaitUnix == 0) && (v.SyncRep <= 0 || v.OldestSyncRepWaitUnix > 0 && v.OldestSyncRepWaitUnix <= v.ObservedAtUnix)
}

func validRetryResultProbe(v retryResultWire, in RetryInputs) bool {
	return slices.Contains([]string{"on", "off", v1beta1.SynchronousCommitLocal, "remote_write", "remote_apply"}, v.TargetSynchronousCommit) && v.SynchronousCommitAdmitted == (!in.ExpectLocal || v.TargetSynchronousCommit == v1beta1.SynchronousCommitLocal) && v.ProbeAttempted == (in.CommitTest && v.SyncRep == 0 && v.GateWaiting == 0) && v.ProbeCommitted == v.ProbeAttempted
}

func (v retryResultWire) observation() RetryGateObservation {
	return RetryGateObservation{v.ObservedAtUnix, v.Total, v.SyncRep, v.GateWaiting, v.OldestSyncRepWaitUnix, v.ApplicationAttributionKnown, v.TargetSynchronousCommit, v.SynchronousCommitAdmitted, v.ProbeAttempted, v.ProbeCommitted}
}
