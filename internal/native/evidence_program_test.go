package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeEvidenceProgramEmbedding(t *testing.T) {
	for _, test := range []struct {
		name    string
		program func() []byte
		digest  func() [32]byte
		limit   int
	}{
		{"main", EvidenceProgram, EvidenceProgramSHA256, EvidenceProgramSourceLimit},
		{"writer", EvidenceRequestWriterProgram, EvidenceRequestWriterSHA256, 16384},
	} {
		t.Run(test.name, func(t *testing.T) {
			program := test.program()
			if len(program) == 0 || len(program) > test.limit || program[len(program)-1] != '\n' || bytes.Contains(program, []byte("\x00")) {
				t.Fatal("embedded program violates its source bounds")
			}
			if test.digest() != sha256.Sum256(program) {
				t.Fatal("source and program hash differ")
			}
			program[0] ^= 1
			if bytes.Equal(program, test.program()) || test.digest() != sha256.Sum256(test.program()) {
				t.Fatal("caller mutated the embedded source")
			}
		})
	}
	if bytes.Contains(EvidenceProgram(), []byte("\nRPGMIGRATE_NATIVE_EVIDENCE_PY\n")) {
		t.Fatal("embedded source collides with the fixed heredoc delimiter")
	}
	const prefix = "exec /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B - completed_evidence "
	const opening = " <<'RPGMIGRATE_NATIVE_EVIDENCE_PY'\n"
	const closing = "RPGMIGRATE_NATIVE_EVIDENCE_PY\n"
	argumentBytes := len(prefix) + 32 + 1 + 64 + len(opening) + len(EvidenceProgram()) + len(closing) + 1
	if argumentBytes > EvidenceMainArgumentLimit {
		t.Fatal("rendered main argument exceeds its NUL-inclusive bound")
	}
}

func pythonMechanismCase(t *testing.T, method string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	path, err := filepath.Abs("evidence_program_test.py")
	if err != nil {
		t.Fatal("cannot locate the private Python tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path, "NativePythonTests."+method)
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("Ran 1 test")) || !bytes.Contains(output, []byte("\nOK\n")) {
		t.Fatalf("focused Python mechanism case %s failed: %s", method, output)
	}
}

func TestNativePythonABI(t *testing.T) { pythonMechanismCase(t, "test_ABI") }
func TestNativePythonCredentialsPrivate(t *testing.T) {
	pythonMechanismCase(t, "test_CredentialsPrivate")
}
func TestNativePythonResultDiscipline(t *testing.T) { pythonMechanismCase(t, "test_ResultDiscipline") }
func TestNativePythonOrderedGuards(t *testing.T)    { pythonMechanismCase(t, "test_OrderedGuards") }
func TestNativePythonPrivateInput(t *testing.T)     { pythonMechanismCase(t, "test_PrivateInput") }
func TestNativePythonEnvironmentDNS(t *testing.T)   { pythonMechanismCase(t, "test_EnvironmentDNS") }
func TestNativePythonRequestWire(t *testing.T)      { pythonMechanismCase(t, "test_RequestWire") }
func TestNativePythonRequestDigests(t *testing.T)   { pythonMechanismCase(t, "test_RequestDigests") }

func TestNativePythonUnknownCommit(t *testing.T)    { pythonMechanismCase(t, "test_UnknownCommit") }
func TestNativePythonPrivateAdmission(t *testing.T) { pythonMechanismCase(t, "test_PrivateAdmission") }
func TestNativePythonCatalogFixtures(t *testing.T)  { pythonMechanismCase(t, "test_CatalogFixtures") }
func TestNativePythonRuntimeMetadata(t *testing.T)  { pythonMechanismCase(t, "test_RuntimeMetadata") }
func TestNativePythonWriter(t *testing.T)           { pythonMechanismCase(t, "test_Writer") }
func TestNativePythonObservationOrder(t *testing.T) { pythonMechanismCase(t, "test_ObservationOrder") }
func TestNativePythonNoSQLBeforeAdmission(t *testing.T) {
	pythonMechanismCase(t, "test_NoSQLBeforeAdmission")
}
func TestNativePythonDescriptorMutation(t *testing.T) {
	pythonMechanismCase(t, "test_DescriptorMutation")
}
func TestNativePythonConnectPolling(t *testing.T) { pythonMechanismCase(t, "test_ConnectPolling") }

func TestNativePythonRelay(t *testing.T) {
	for _, method := range []string{"test_RelayProtocol", "test_RelayFaults", "test_RelayLifetime", "test_RelayDuplicate", "test_RelayDeadline", "test_RelayMainQuiet", "test_RelayClosureOrder", "test_RelayCloseFault", "test_RelayReplacement", "test_RelayOverflow"} {
		t.Run(method, func(t *testing.T) { pythonMechanismCase(t, method) })
	}
}

func TestNativeEvidenceEmbeddedLimits(t *testing.T) {
	if EvidenceProgramSourceLimit != 122880 || EvidenceMainArgumentLimit != 126976 || EvidencePreludeArgumentLimit != 65536 || EvidenceJobJSONLimit != 1048576 {
		t.Fatal("native evidence policy limits differ from the approved main-only amendment")
	}
	if len(EvidenceProgram()) <= 65536 || EvidenceMainArgumentLimit-EvidenceProgramSourceLimit != 4096 || 131072-EvidenceMainArgumentLimit != 4096 {
		t.Fatal("expanded source or minimum-kernel headroom is not represented")
	}
	if len(EvidenceRequestWriterProgram()) > 16384 {
		t.Fatal("main policy changed the relay source limit")
	}
}

func TestNativePythonFileOptions(t *testing.T) { pythonMechanismCase(t, "test_FileOptions") }
func TestNativePythonFileOptionPrecedence(t *testing.T) {
	pythonMechanismCase(t, "test_FileOptionPrecedence")
}
func TestNativePythonFileSources(t *testing.T)   { pythonMechanismCase(t, "test_FileSources") }
func TestNativePythonFileSnapshots(t *testing.T) { pythonMechanismCase(t, "test_FileSnapshots") }
func TestNativePythonFileAbsence(t *testing.T)   { pythonMechanismCase(t, "test_FileAbsence") }
func TestNativePythonFileConnectionStart(t *testing.T) {
	pythonMechanismCase(t, "test_FileConnectionStart")
}
func TestNativePythonFileCleanupOrder(t *testing.T) { pythonMechanismCase(t, "test_FileCleanupOrder") }

func TestNativePythonFileKernelOpen(t *testing.T) { pythonMechanismCase(t, "test_FileKernelOpen") }
func TestNativePythonFileMaterialization(t *testing.T) {
	pythonMechanismCase(t, "test_FileMaterialization")
}
