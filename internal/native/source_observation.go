package native

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	"k8s.io/apimachinery/pkg/types"
)

type sourceRequestWire struct {
	Version       int64         `json:"version"`
	Kind          string        `json:"kind"`
	OperationID   string        `json:"operation_id"`
	ProgramSHA256 string        `json:"program_sha256"`
	Execution     executionWire `json:"execution"`
	PodName       string        `json:"pod_name"`
	Container     containerWire `json:"container"`
}

const sourceWorkWireKeys = "migration_uid run_id workdir_id checkpoint_generation"

func sourceRequestValue(kind string, w ExecutionData) sourceRequestWire {
	c := w.Container
	return sourceRequestWire{Version: 1, Kind: kind, OperationID: w.OperationID, ProgramSHA256: hex.EncodeToString(w.ProgramSHA256[:]), Execution: executionWire{Namespace: w.Namespace, MigrationName: w.MigrationName, PVCName: w.PVCName, JobName: w.JobName, MigrationUID: string(w.MigrationUID), PVCUID: string(w.PVCUID), JobUID: string(w.JobUID), WorkerJobUID: string(w.WorkerJobUID), MigrationGeneration: w.MigrationGeneration, Attempt: w.Attempt, Role: w.Role, Work: w.Work, ImageRef: w.ImageRef, PlatformImageID: w.PlatformImageID, TemplateSHA256: hex.EncodeToString(w.TemplateSHA256[:]), ConnectionReferenceSHA256: hex.EncodeToString(w.ConnectionReferenceSHA256[:])}, PodName: w.PodName, Container: containerWire{PodUID: string(c.PodUID), ContainerName: c.ContainerName, ContainerID: c.ContainerID, RestartCount: c.RestartCount, ImageID: c.ImageID, TemplateSHA256: hex.EncodeToString(c.TemplateSHA256[:])}}
}

func sourceRequest(kind string, w ExecutionData) ([]byte, error) {
	if !activeRoleWorker(w) {
		return nil, errors.New("native source request unavailable")
	}
	raw, err := json.Marshal(sourceRequestValue(kind, w))
	if err != nil {
		return nil, errors.New("native source request unavailable")
	}
	if _, err := parseSourceRequest(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func parseSourceRequest(raw []byte) (*sourceRequestWire, error) {
	var v sourceRequestWire
	if !decodeNativeWire(raw, &v, "source_request") || v.Version != 1 || (v.Kind != sourceHeadKind && v.Kind != activeLagKind) || !lowerHex(v.ProgramSHA256, 64) {
		return nil, errors.New("native source request invalid")
	}
	x, c := v.Execution, v.Container
	w := ExecutionData{Namespace: x.Namespace, MigrationName: x.MigrationName, PVCName: x.PVCName, JobName: x.JobName, PodName: v.PodName, MigrationUID: types.UID(x.MigrationUID), PVCUID: types.UID(x.PVCUID), JobUID: types.UID(x.JobUID), WorkerJobUID: types.UID(x.WorkerJobUID), MigrationGeneration: x.MigrationGeneration, Attempt: x.Attempt, Role: x.Role, Work: x.Work, ImageRef: x.ImageRef, PlatformImageID: x.PlatformImageID, OperationID: v.OperationID}
	program, err1 := hex.DecodeString(v.ProgramSHA256)
	template, err2 := hex.DecodeString(x.TemplateSHA256)
	references, err3 := hex.DecodeString(x.ConnectionReferenceSHA256)
	if err1 != nil || err2 != nil || err3 != nil || !lowerHex(x.TemplateSHA256, 64) || !lowerHex(x.ConnectionReferenceSHA256, 64) || c.TemplateSHA256 != x.TemplateSHA256 {
		return nil, errors.New("native source request invalid")
	}
	copy(w.ProgramSHA256[:], program)
	copy(w.TemplateSHA256[:], template)
	copy(w.ConnectionReferenceSHA256[:], references)
	w.Container.PodUID, w.Container.ContainerName, w.Container.ContainerID = types.UID(c.PodUID), c.ContainerName, c.ContainerID
	w.Container.RestartCount, w.Container.ImageID, w.Container.TemplateSHA256 = c.RestartCount, c.ImageID, w.TemplateSHA256
	if !activeRoleWorker(w) {
		return nil, errors.New("native source request invalid")
	}
	return &v, nil
}

type sourceResultWire struct {
	Version             int64        `json:"version"`
	Kind                string       `json:"kind"`
	OperationID         string       `json:"operation_id"`
	ProgramSHA256       string       `json:"program_sha256"`
	RequestSHA256       string       `json:"request_sha256"`
	ExecutionSHA256     string       `json:"execution_sha256"`
	Work                WorkIdentity `json:"work"`
	SourceBindingSHA256 string       `json:"source_binding_sha256"`
	HeadLSN             string       `json:"head_lsn,omitempty"`
	LagBytes            int64        `json:"lag_bytes,omitempty"`
	SourceClosed        bool         `json:"source_closed"`
	PrivateClosed       bool         `json:"private_closed"`
	DiagnosticsClean    bool         `json:"diagnostics_clean"`
}

func parseSourceResult(raw, body []byte, kind string, w ExecutionData) (*sourceResultWire, error) {
	if !activeRoleWorker(w) || (kind != sourceHeadKind && kind != activeLagKind) || len(raw) < 2 || len(raw) > nativeWireLimit || raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw[:len(raw)-1], '\n') {
		return nil, errors.New("native source observation unavailable")
	}
	request, err := parseSourceRequest(body)
	if err != nil || *request != sourceRequestValue(kind, w) {
		return nil, errors.New("native source observation unavailable")
	}
	var v sourceResultWire
	if !decodeNativeWire(raw[:len(raw)-1], &v, kind) {
		return nil, errors.New("native source observation unavailable")
	}
	r, e := requestDigests(body)
	if v.Version != 1 || v.Kind != kind || v.OperationID != w.OperationID || v.ProgramSHA256 != hex.EncodeToString(w.ProgramSHA256[:]) || v.RequestSHA256 != hex.EncodeToString(r[:]) || v.ExecutionSHA256 != hex.EncodeToString(e[:]) || v.Work != w.Work || !lowerHex(v.SourceBindingSHA256, 64) || v.SourceBindingSHA256 == string(bytes.Repeat([]byte{'0'}, 64)) || !v.SourceClosed || !v.PrivateClosed || !v.DiagnosticsClean {
		return nil, errors.New("native source observation unavailable")
	}
	if kind == sourceHeadKind {
		if head, err := sentinel.ParseLSN(v.HeadLSN); err != nil || head == 0 {
			return nil, errors.New("native source observation unavailable")
		}
	} else if v.LagBytes < 0 {
		return nil, errors.New("native source observation unavailable")
	}
	return &v, nil
}

// InformationalLag carries no end boundary or completion evidence.
type InformationalLag struct {
	valid                bool
	lagBytes             int64
	admissionFingerprint [32]byte
}

func (l InformationalLag) LagBytes() *int64 {
	if !l.valid {
		return nil
	}
	return &l.lagBytes
}

func (l InformationalLag) AdmissionFingerprint() [32]byte {
	if !l.valid {
		return [32]byte{}
	}
	return l.admissionFingerprint
}

func sourceAdmissionFingerprint(w ExecutionData, binding string) [32]byte {
	v := sourceRequestValue(activeLagKind, w)
	stable := struct {
		ProgramSHA256 string        `json:"program_sha256"`
		Execution     executionWire `json:"execution"`
		PodName       string        `json:"pod_name"`
		Container     containerWire `json:"container"`
		SourceBinding string        `json:"source_binding_sha256"`
	}{v.ProgramSHA256, v.Execution, v.PodName, v.Container, binding}
	raw, err := json.Marshal(stable)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}
