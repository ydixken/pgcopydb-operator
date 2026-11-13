package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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
func TestNativePythonPrivateInputSetgidParent(t *testing.T) {
	pythonMechanismCase(t, "test_PrivateInputSetgidParent")
}
func TestNativePythonEnvironmentDNS(t *testing.T) { pythonMechanismCase(t, "test_EnvironmentDNS") }
func TestNativePythonNativePreflightFailureLine(t *testing.T) {
	pythonMechanismCase(t, "test_NativePreflightFailureLine")
}
func TestNativePythonQualifiedResolverPolicy(t *testing.T) {
	pythonMechanismCase(t, "test_QualifiedResolverPolicy")
}
func TestNativePythonQualifiedResolverTuple(t *testing.T) {
	pythonMechanismCase(t, "test_QualifiedResolverTuple")
}
func TestNativePythonConnectionHomePolicy(t *testing.T) {
	pythonMechanismCase(t, "test_ConnectionHomePolicy")
}
func TestNativePythonRequestWire(t *testing.T)    { pythonMechanismCase(t, "test_RequestWire") }
func TestNativePythonRequestDigests(t *testing.T) { pythonMechanismCase(t, "test_RequestDigests") }

func TestNativePythonUnknownCommit(t *testing.T) { pythonMechanismCase(t, "test_UnknownCommit") }
func TestNativePythonSessionTransactionState(t *testing.T) {
	pythonMechanismCase(t, "test_SessionTransactionState")
}
func TestNativePythonPrivateAdmission(t *testing.T) { pythonMechanismCase(t, "test_PrivateAdmission") }
func TestNativePythonWorkRootAdmission(t *testing.T) {
	pythonMechanismCase(t, "test_WorkRootAdmission")
}
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
	for _, method := range []string{"test_RelayProtocol", "test_RelayFifoArrival", "test_RelayTeardownArrival", "test_RelayEarlyTeardown", "test_RelayFaults", "test_RelayLifetime", "test_RelayDuplicate", "test_RelayDeadline", "test_RelayModeSettle", "test_RelayMainQuiet", "test_RelayClosureOrder", "test_RelayCloseFault", "test_RelayReplacement", "test_RelayOverflow"} {
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
func TestNativePythonFileKeyLogRefusal(t *testing.T) {
	pythonMechanismCase(t, "test_FileKeyLogRefusal")
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

func TestNativePythonNativePreflightDispatch(t *testing.T) {
	pythonMechanismCase(t, "test_NativePreflightDispatch")
}
func TestNativePythonNativePreflightClosedResult(t *testing.T) {
	pythonMechanismCase(t, "test_NativePreflightClosedResult")
}
func TestNativePythonNativePreflightRefusals(t *testing.T) {
	pythonMechanismCase(t, "test_NativePreflightRefusals")
}
func TestNativePythonNativePreflightResolverClosed(t *testing.T) {
	pythonMechanismCase(t, "test_NativePreflightResolverClosed")
}
func TestNativePythonNativePreflightTools(t *testing.T) {
	pythonMechanismCase(t, "test_NativePreflightTools")
}
func TestNativePythonNativePreflightBounds(t *testing.T) {
	pythonMechanismCase(t, "test_NativePreflightBounds")
}

func TestNativePythonSourceObservation(t *testing.T) {
	for _, method := range []string{"test_SourceObservationWire", "test_SourceObservationState", "test_SourceObservationMaterialization", "test_SourceObservationClosedResult", "test_SourceObservationRefusals", "test_SourceObservationIO"} {
		t.Run(method, func(t *testing.T) { pythonMechanismCase(t, method) })
	}
}

func TestNativePythonSourceObservationRequired(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	source, err := os.ReadFile("evidence_program_test.py")
	if err != nil {
		t.Fatal("required Python source tests are absent")
	}
	path := filepath.Join(t.TempDir(), "evidence_program_test.py")
	if os.WriteFile(path, source, 0o600) != nil {
		t.Fatal("cannot prepare missing-module control")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path, "NativePythonTests.test_SourceObservationWire")
	output, err := command.CombinedOutput()
	if err == nil || bytes.Contains(output, []byte("\nOK\n")) || bytes.Contains(output, []byte("skipped")) {
		t.Fatal("missing source module passed the required gate")
	}
}

func TestNativePythonTargetRetry(t *testing.T) {
	for _, method := range []string{"test_TargetRetryWire", "test_TargetRetryData", "test_TargetRetryRefusals", "test_TargetRetryMaterialization"} {
		t.Run(method, func(t *testing.T) { pythonMechanismCase(t, method) })
	}
}

func TestNativePythonTargetRetryRequired(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	source, err := os.ReadFile("evidence_program_test.py")
	if err != nil {
		t.Fatal("required Python retry tests are absent")
	}
	path := filepath.Join(t.TempDir(), "evidence_program_test.py")
	if os.WriteFile(path, source, 0o600) != nil {
		t.Fatal("cannot prepare missing-module control")
	}
	for _, method := range []string{"test_TargetRetryWire", "test_TargetRetryData", "test_TargetRetryRefusals", "test_TargetRetryMaterialization"} {
		t.Run(method, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path, "NativePythonTests."+method)
			output, err := command.CombinedOutput()
			if err == nil || bytes.Contains(output, []byte("\nOK\n")) || bytes.Contains(output, []byte("skipped")) {
				t.Fatal("missing canonical module passed the required retry gate")
			}
		})
	}
}

func TestNativePythonMechanismInventory(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "evidence_program_test.go", nil, 0)
	if err != nil {
		t.Fatal("cannot read the registered Go mechanism tests")
	}
	var registered []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !strings.HasPrefix(function.Name.Name, "TestNativePython") {
			continue
		}
		callsMechanism := false
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "pythonMechanismCase" {
					callsMechanism = true
				}
			}
			return true
		})
		if !callsMechanism {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
				value, err := strconv.Unquote(literal.Value)
				if err == nil && strings.HasPrefix(value, "test_") {
					registered = append(registered, value)
				}
			}
			return true
		})
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	path, err := filepath.Abs("evidence_program_test.py")
	if err != nil {
		t.Fatal("cannot locate the private Python tests")
	}
	const inventory = `import importlib.util,json,sys,unittest
s=importlib.util.spec_from_file_location('native_mechanism_inventory',sys.argv[1])
m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
if m.P is None: raise RuntimeError('canonical module absent')
print(json.dumps(unittest.defaultTestLoader.getTestCaseNames(m.NativePythonTests)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", "-c", inventory, path).Output()
	var actual []string
	if err != nil || json.Unmarshal(output, &actual) != nil || len(actual) != 66 || len(registered) != 66 {
		t.Fatalf("required Python mechanism inventory is unavailable or incomplete: Go=%d Python=%d", len(registered), len(actual))
	}
	slices.Sort(registered)
	slices.Sort(actual)
	if !slices.Equal(registered, actual) {
		t.Fatal("registered Go mechanisms differ from the actual Python TestCase methods")
	}
}

func TestNativePythonPrivateInputTimeout(t *testing.T) {
	pythonMechanismCase(t, "test_PrivateInputTimeout")
}

func TestNativePythonPrivateInputTrailing(t *testing.T) {
	pythonMechanismCase(t, "test_PrivateInputTrailing")
}
