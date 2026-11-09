package native

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/apimachinery/pkg/types"
)

const (
	sourceZeroLSN          = "0/0"
	sourceContainerKey     = "container"
	sourceCopyingPhase     = "copying"
	sourceHoldCase         = "hold"
	sourceImageCase        = "image"
	sourceJobKind          = "Job"
	sourceCaseVersionJSON  = `"Version":1`
	sourceAttemptJSON      = `"attempt":1`
	sourceRestartCase      = "restart"
	sourcePrivateClosedKey = "private_closed"
	sourceRequestSHAKey    = "request_sha256"
	sourceHeadLSNKey       = "head_lsn"
	sourceLagBytesKey      = "lag_bytes"
	sourceStderrCase       = "stderr"
	sourceStdoutOnlyCase   = "stdout_only"
	sourceTruncatedCase    = "truncated"
	sourceTrailingCase     = "trailing"
)

func sourceWant() ExecutionData {
	w := ExecutionData{Namespace: "test", MigrationName: "migration", PVCName: "migration-work", JobName: "migration-run-1", PodName: "worker-pod", MigrationUID: types.UID(fixtureMigrationID), PVCUID: "source-pvc-uid", JobUID: "source-worker-uid", WorkerJobUID: "source-worker-uid", MigrationGeneration: 1, Attempt: 1, Role: JobRoleWorker, Work: WorkIdentity{MigrationUID: fixtureMigrationID, RunID: fixtureRunID, WorkdirID: "source-pvc-uid", CheckpointGeneration: 1}, ImageRef: "runner@sha256:" + strings.Repeat("1", 64), PlatformImageID: "docker-pullable://runner@sha256:" + strings.Repeat("1", 64), TemplateSHA256: sha256.Sum256([]byte("template")), ConnectionReferenceSHA256: sha256.Sum256([]byte("references")), OperationID: strings.Repeat("a", 32), ProgramSHA256: EvidenceProgramSHA256()}
	w.Container = podexec.NativeContainerBinding{PodUID: "source-pod-uid", ContainerName: nativeContainerName, ContainerID: "containerd://source-fixture", ImageID: w.PlatformImageID, TemplateSHA256: w.TemplateSHA256, ExecutionSHA256: sha256.Sum256([]byte("execution"))}
	return w
}

func TestNativeSourceOnlyAdmission(t *testing.T) {
	w := sourceWant()
	if !activeRoleWorker(w) {
		t.Fatal("valid current-worker data refused")
	}
	for name, mutate := range map[string]func(*ExecutionData){
		"role":                 func(w *ExecutionData) { w.Role = JobRoleEvidence },
		"missing_job":          func(w *ExecutionData) { w.JobUID = "" },
		"zero_job":             func(w *ExecutionData) { w.JobUID = "000-000"; w.WorkerJobUID = w.JobUID },
		"zero_pod":             func(w *ExecutionData) { w.Container.PodUID = "000-000" },
		"distinct_worker":      func(w *ExecutionData) { w.WorkerJobUID = "old-worker" },
		"zero_attempt":         func(w *ExecutionData) { w.Attempt = 0 },
		"generation":           func(w *ExecutionData) { w.MigrationGeneration = 0 },
		"work_generation":      func(w *ExecutionData) { w.Work.CheckpointGeneration = 0 },
		"foreign_work":         func(w *ExecutionData) { w.Work.WorkdirID = evidenceOther },
		sourceRestartCase:      func(w *ExecutionData) { w.Container.RestartCount = 1 },
		sourceImageCase:        func(w *ExecutionData) { w.Container.ImageID = evidenceOther },
		"template":             func(w *ExecutionData) { w.Container.TemplateSHA256 = [32]byte{} },
		"references":           func(w *ExecutionData) { w.ConnectionReferenceSHA256 = [32]byte{} },
		"program":              func(w *ExecutionData) { w.ProgramSHA256 = [32]byte{} },
		admissionCaseOperation: func(w *ExecutionData) { w.OperationID = strings.Repeat("A", 32) },
		"pod":                  func(w *ExecutionData) { w.PodName = "" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := w
			mutate(&bad)
			if activeRoleWorker(bad) {
				t.Fatal("invalid active execution accepted")
			}
		})
	}
}
