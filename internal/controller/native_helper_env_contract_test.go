package controller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
)

// The driver runs each program's own entry up to the first step after its environment
// checks, so a variable renamed or dropped on either side refuses here, not in a cluster.
const nativeHelperEnvDriver = `import json,os,sys,types
path,entry,style,work,*args=sys.argv[1:]
m=types.ModuleType('_rpgmigrate_env_contract');sys.modules[m.__name__]=m
with open(path,'rb') as f: source=f.read()
exec(compile(source,path,'exec'),m.__dict__)
reached=[]
class Admitted(BaseException): pass
def admitted(*_a,**_k):
 reached.append(True);raise Admitted()
m.InputChannel=m.Diagnostics=admitted
os.geteuid=lambda:65532
m.__dict__[entry](['-',*args]) if style=='argv' else m.__dict__[entry](*args)
if not reached: print('refused');sys.exit(0)
if work: m.work_environment({'execution':{'work':json.loads(work)}})
print('admitted')
`

type nativeHelperEnvCase struct {
	entry   string
	argv    bool // the entry takes the bootstrap's argv list instead of positional strings
	program []byte
	args    []string
	work    *native.WorkIdentity
	build   func(t *testing.T) (*batchv1.Job, error)
}

func nativeHelperEnvCases(t *testing.T) []nativeHelperEnvCase {
	t.Helper()
	evidence, want := nativeEvidenceJobInputs(t)
	data := func(role native.JobRole, prefix string, program [32]byte) (*batchv1.Job, error) {
		m, w := cleanedJobInputs(t)
		w.Role, w.JobName, w.ProgramSHA256 = role, prefix+w.OperationID, program
		switch role {
		case native.JobRoleRetryGate, native.JobRoleCleanup:
			return buildNativeRecoveryReadJob(m, nativeEvidenceTestImage, w)
		}
		if prefix == nativeCleanupRejoinJobPrefix {
			return buildNativeCleanupRejoinCompletionJob(m, nativeEvidenceTestImage, w)
		}
		return buildNativeCleanedCompletionJob(m, nativeEvidenceTestImage, w)
	}
	compare := func(check string) func(*testing.T) (*batchv1.Job, error) {
		return func(t *testing.T) (*batchv1.Job, error) {
			m, pvc, worker, _ := nativeCompareInputs(t)
			return buildNativeCompareJob(m, pvc, worker, nativeEvidenceTestImage, check)
		}
	}
	const comparisonMode = "native_comparison"
	op := want.OperationID
	hash := func(sum [32]byte) string { return hex.EncodeToString(sum[:]) }
	work := &want.Work
	compareOp := func(t *testing.T, check string) string {
		m, pvc, worker, identity := nativeCompareInputs(t)
		id, err := nativeComparisonOperationID(m, pvc, worker, identity, check)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	return []nativeHelperEnvCase{
		{"run_native_preflight", false, native.EvidenceProgram(), []string{probeTestOperation, hash(native.EvidenceProgramSHA256())}, nil,
			func(*testing.T) (*batchv1.Job, error) {
				m, _ := nativeEndposJobInputs()
				return buildNativePreflightJob(m, nativeEvidenceTestImage, probeTestPreflightName, probeTestOperation)
			}},
		{"run_completed_evidence", false, native.EvidenceProgram(), []string{op, hash(native.EvidenceProgramSHA256())}, work,
			func(*testing.T) (*batchv1.Job, error) {
				return buildNativeEvidenceJob(evidence, nativeEvidenceTestImage, want)
			}},
		{"run_target_retry", false, native.EvidenceProgram(), []string{op, hash(native.EvidenceProgramSHA256())}, work,
			func(*testing.T) (*batchv1.Job, error) {
				return data(native.JobRoleRetryGate, "rpgmigrate-retry-", native.EvidenceProgramSHA256())
			}},
		{"owned_inventory_main", true, native.OwnedInventoryProgram(), []string{"owned_inventory", op, hash(native.OwnedInventoryProgramSHA256())}, work,
			func(*testing.T) (*batchv1.Job, error) {
				return data(native.JobRoleCleanup, nativeInventoryJobPrefix, native.OwnedInventoryProgramSHA256())
			}},
		{"cleaned_completion_main", true, native.CleanedCompletionProgram(), []string{"cleaned_completed_evidence", op, hash(native.CleanedCompletionProgramSHA256())}, work,
			func(*testing.T) (*batchv1.Job, error) {
				return data(native.JobRoleEvidence, "rpgmigrate-cleaned-", native.CleanedCompletionProgramSHA256())
			}},
		{"cleanup_rejoin_completion_main", true, native.CleanupRejoinCompletionProgram(), []string{"cleanup_rejoin_completed_evidence", op, hash(native.CleanupRejoinCompletionProgramSHA256())}, work,
			func(*testing.T) (*batchv1.Job, error) {
				return data(native.JobRoleEvidence, nativeCleanupRejoinJobPrefix, native.CleanupRejoinCompletionProgramSHA256())
			}},
		{"comparison_main", true, native.ComparisonProgram(), []string{comparisonMode, compareSchema, compareOp(t, compareSchema), hash(native.ComparisonProgramSHA256())}, nil, compare(compareSchema)},
		{"comparison_main", true, native.ComparisonProgram(), []string{comparisonMode, compareData, compareOp(t, compareData), hash(native.ComparisonProgramSHA256())}, nil, compare(compareData)},
	}
}

// nativeHelperEnvVerdict runs the program's own environment checks under the rendered env alone.
func nativeHelperEnvVerdict(t *testing.T, tc nativeHelperEnvCase, env []string) string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	path := filepath.Join(t.TempDir(), "program.py")
	if err := os.WriteFile(path, tc.program, 0o600); err != nil {
		t.Fatal(err)
	}
	work := ""
	if tc.work != nil {
		raw, err := json.Marshal(tc.work)
		if err != nil {
			t.Fatal(err)
		}
		work = string(raw)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	style := "args"
	if tc.argv {
		style = "argv"
	}
	command := exec.CommandContext(ctx, python, append([]string{"-I", "-B", "-Werror", "-c", nativeHelperEnvDriver, path, tc.entry, style, work}, tc.args...)...)
	command.Env = env
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("environment driver failed: %v: %s", err, output)
	}
	// A refusing program may write its own failure line first; the driver's verdict is last.
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	return lines[len(lines)-1]
}

func renderedNativeHelperEnv(t *testing.T, job *batchv1.Job) []string {
	t.Helper()
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatal("native helper renders one container")
	}
	env := make([]string, 0, len(job.Spec.Template.Spec.Containers[0].Env))
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		value := e.Value
		if e.ValueFrom != nil {
			value = "projected-" + strings.ToLower(e.Name)
		}
		env = append(env, e.Name+"="+value)
	}
	return env
}

func TestNativeHelperEnvironmentMatchesProgramChecks(t *testing.T) {
	for i, tc := range nativeHelperEnvCases(t) {
		t.Run(fmt.Sprint(i, " ", tc.entry), func(t *testing.T) {
			job, err := tc.build(t)
			if err != nil || job == nil {
				t.Fatalf("helper Job unavailable: %v", err)
			}
			env := renderedNativeHelperEnv(t, job)
			if got := nativeHelperEnvVerdict(t, tc, env); got != "admitted" {
				t.Fatalf("program refused the controller's rendered environment (%s): %v", got, env)
			}
			// The driver must discriminate: a missing locale is refused by every program.
			var stripped []string
			for _, e := range env {
				if !strings.HasPrefix(e, "LC_ALL=") {
					stripped = append(stripped, e)
				}
			}
			if got := nativeHelperEnvVerdict(t, tc, stripped); got != "refused" {
				t.Fatalf("driver admitted an environment the program refuses: %s", got)
			}
		})
	}
}
