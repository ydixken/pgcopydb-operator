package native

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	nativePreflightKind        = "native_preflight"
	nativePreflightTmp         = "tmp"
	nativePreflightLang        = "LANG"
	nativePreflightLocale      = "LC_ALL"
	nativePreflightPassfileEnv = "PGPASSFILE"
	nativePreflightTimeoutEnv  = "PGCONNECT_TIMEOUT"
	nativePreflightFollowEnv   = "RPGMIGRATE_NATIVE_PREFLIGHT_FOLLOW"
)

// PreflightResult carries no completion, resource ownership, or cutover authority.
type PreflightResult struct{ passed bool }

func (p PreflightResult) Passed() bool { return p.passed }

type preflightWire struct {
	Version               int64    `json:"version"`
	Kind                  string   `json:"kind"`
	OperationID           string   `json:"operation_id"`
	ProgramSHA256         string   `json:"program_sha256"`
	SourceMajor           int64    `json:"source_major"`
	TargetMajor           int64    `json:"target_major"`
	PGDumpMajor           int64    `json:"pg_dump_major"`
	PGRestoreMajor        int64    `json:"pg_restore_major"`
	Checks                []string `json:"checks"`
	SourceClosed          bool     `json:"source_closed"`
	TargetClosed          bool     `json:"target_closed"`
	MaterializationClosed bool     `json:"materialization_closed"`
	DiagnosticsClean      bool     `json:"diagnostics_clean"`
}

func ParsePreflightResult(raw []byte, operationID string, programSHA256 [32]byte, follow bool) (*PreflightResult, error) {
	if programSHA256 != EvidenceProgramSHA256() {
		return nil, errors.New("native preflight program binding invalid")
	}
	if !lowerHex(operationID, 32) || len(raw) < 2 || len(raw) > nativeWireLimit || raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw[:len(raw)-1], '\n') {
		return nil, errors.New("native preflight result framing invalid")
	}
	var result preflightWire
	if !decodeNativeWire(raw[:len(raw)-1], &result, nativePreflightKind) {
		return nil, errors.New("native preflight result fields invalid")
	}
	checks := []string{"connections", "identity", "source_primary", "target_writable", "versions", "tools", "temporary_privilege", "extensions", "clone_rights", "rls"}
	if follow {
		checks = append(checks, "wal", "publication", "slots", "replication", "replica_role", "unlogged", "replica_identity")
	}
	if result.Version != 1 || result.Kind != nativePreflightKind || result.OperationID != operationID || result.ProgramSHA256 != hex.EncodeToString(programSHA256[:]) || !slices.Equal(result.Checks, checks) || !result.SourceClosed || !result.TargetClosed || !result.MaterializationClosed || !result.DiagnosticsClean {
		return nil, errors.New("native preflight result binding invalid")
	}
	pairSupported := result.SourceMajor == 16 && (result.TargetMajor == 16 || result.TargetMajor == 18) || result.SourceMajor == 18 && result.TargetMajor == 18
	if !pairSupported || result.PGDumpMajor != result.TargetMajor || result.PGRestoreMajor != result.TargetMajor {
		return nil, errors.New("native preflight version pair invalid")
	}
	return &PreflightResult{passed: true}, nil
}

// preflightFailureClasses mirrors ERROR_CLASSES in evidence_program.py.
var preflightFailureClasses = []string{"admission", "capability", "input", "timeout", "connection", "diagnostic", "result", "transaction_state", "unknown_commit", "closure", "internal"}

// PreflightFailureClass names a refusal only from the program's exact closed-set line, never from free text.
func PreflightFailureClass(message string) (string, bool) {
	for _, class := range preflightFailureClasses {
		line := `{"version":1,"kind":"native_preflight_failure","error_class":"` + class + `"}`
		if message == line || message == line+"\n" {
			return class, true
		}
	}
	return "", false
}

// The caller must freshly admit configuration, primary profiles, and actual image capabilities before projection.
func PreflightMaterialization(m *v1beta1.Migration, operationID string) ([]string, []string, []corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, error) {
	if m == nil || m.UID == "" || !lowerHex(operationID, 32) {
		return nil, nil, nil, nil, nil, errors.New("native preflight materialization refused")
	}
	src, err := conn.Materialize(conn.Source, &m.Spec.Source)
	if err != nil {
		return nil, nil, nil, nil, nil, errors.New("native connection materialization invalid")
	}
	tgt, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		return nil, nil, nil, nil, nil, errors.New("native connection materialization invalid")
	}
	env := []corev1.EnvVar{}
	volumes := []corev1.Volume{{Name: nativePreflightTmp, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: resource.NewQuantity(64*1024*1024, resource.BinarySI)}}}}
	mounts := []corev1.VolumeMount{{Name: nativePreflightTmp, MountPath: "/tmp"}}
	var preludes []string
	var passfiles []conn.Passfile
	for _, material := range []*conn.Materialized{src, tgt} {
		env = append(env, material.Env...)
		volumes = append(volumes, material.Volumes...)
		mounts = append(mounts, material.Mounts...)
		if material.Prelude != "" {
			preludes = append(preludes, material.Prelude)
		}
		if material.Passfile != nil {
			passfiles = append(passfiles, *material.Passfile)
		}
	}
	if len(preludes) > 0 || len(passfiles) > 0 {
		env = append(env, corev1.EnvVar{Name: nativePreflightPassfileEnv, Value: conn.PgpassPath})
	}
	follow := "0"
	if m.Spec.Follow != nil && m.Spec.Follow.Enabled {
		follow = "1"
	}
	env = append(env, corev1.EnvVar{Name: nativePreflightTimeoutEnv, Value: "10"}, corev1.EnvVar{Name: nativePreflightLocale, Value: "C"}, corev1.EnvVar{Name: nativePreflightLang, Value: "C"}, corev1.EnvVar{Name: nativePreflightFollowEnv, Value: follow})
	command, args, err := renderPreflightScripts(EvidenceProgram(), operationID, EvidenceProgramSHA256(), conn.PreludeScript(preludes, passfiles, ""))
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	return command, args, env, volumes, mounts, nil
}

func renderPreflightScripts(program []byte, operationID string, programSHA256 [32]byte, prelude string) ([]string, []string, error) {
	const delimiter = "RPGMIGRATE_NATIVE_EVIDENCE_PY"
	if !lowerHex(operationID, 32) || len(program) == 0 || len(program) > EvidenceProgramSourceLimit || program[len(program)-1] != '\n' || bytes.IndexByte(program, 0) >= 0 || sha256.Sum256(program) != programSHA256 || bytes.Contains(append([]byte{'\n'}, program...), []byte("\n"+delimiter+"\n")) {
		return nil, nil, errors.New("native preflight program invalid")
	}
	main := "exec /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B - native_preflight " + operationID + " " + hex.EncodeToString(programSHA256[:]) + " <<'" + delimiter + "'\n" + string(program) + delimiter + "\n"
	if len(main)+1 > EvidenceMainArgumentLimit || len(prelude)+1 > EvidencePreludeArgumentLimit || strings.IndexByte(main, 0) >= 0 || strings.IndexByte(prelude, 0) >= 0 {
		return nil, nil, errors.New("native preflight script invalid")
	}
	return []string{nativeShellPath, "-c", prelude, nativeShellPath}, []string{"-c", main}, nil
}
