package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// The driver runs each program's own entry up to the first step after its environment
// checks, so a variable renamed or dropped on either side refuses here, not in a cluster.
// An empty entry runs a -c script whose first step after its checks is execve.
const nativeHelperEnvDriver = `import json,os,sys,types
path,entry,style,work,*args=sys.argv[1:]
with open(path,'rb') as f: source=f.read()
reached=[]
class Admitted(BaseException): pass
def admitted(*_a,**_k):
 reached.append(True);raise Admitted()
os.geteuid=lambda:65532
if not entry:
 def leave(code): raise SystemExit(code)
 sys.argv=['-c',*args];os.execve=admitted;os._exit=leave
 try: exec(compile(source,path,'exec'),{'__name__':'__main__'})
 except SystemExit: pass
 print('admitted' if reached else 'refused');sys.exit(0)
m=types.ModuleType('_rpgmigrate_env_contract');sys.modules[m.__name__]=m
exec(compile(source,path,'exec'),m.__dict__)
for name in ('InputChannel','Diagnostics','remaining'):
 if name in m.__dict__: setattr(m,name,admitted)
try: m.__dict__[entry](['-',*args]) if style=='argv' else m.__dict__[entry](*args)
except Exception: pass
except Admitted: pass
if not reached: print('refused');sys.exit(0)
if work: m.work_environment({'execution':{'work':json.loads(work)}})
print('admitted')
`

// nativeComparisonEntry is the comparison program's entry, which two helper Jobs run.
const nativeComparisonEntry = "comparison_main"

// nativeKubernetesService is the default namespace's Service that kubelet always links.
const nativeKubernetesService = "kubernetes"

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
		{nativeComparisonEntry, true, native.ComparisonProgram(), []string{comparisonMode, compareSchema, compareOp(t, compareSchema), hash(native.ComparisonProgramSHA256())}, nil, compare(compareSchema)},
		{nativeComparisonEntry, true, native.ComparisonProgram(), []string{comparisonMode, compareData, compareOp(t, compareData), hash(native.ComparisonProgramSHA256())}, nil, compare(compareData)},
	}
}

// nativeHelperEnvVerdict runs the program's own environment checks under the rendered env alone,
// behind the exec's own env prefix (the argv words before the interpreter) when one is given.
func nativeHelperEnvVerdict(t *testing.T, tc nativeHelperEnvCase, prefix, env []string) string {
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
	argv := append([]string{python, "-I", "-B", "-Werror", "-c", nativeHelperEnvDriver, path, tc.entry, style, work}, tc.args...)
	if len(prefix) != 0 {
		argv = append(slices.Clone(prefix), argv...)
	}
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.Env = env
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("environment driver failed: %v: %s", err, output)
	}
	// A refusing program may write its own failure line first; the driver's verdict is last.
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	return lines[len(lines)-1]
}

// nativeKubeletServices is a namespace kubelet links into a pod unless service links are off:
// one Service named ld-* and enough others to pass the programs' 64 KiB environment cap.
func nativeKubeletServices() []corev1.Service {
	services := make([]corev1.Service, 0, 301)
	services = append(services, corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "ld-cache"}})
	for i := range 300 {
		services = append(services, corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("svc-%03d", i)}})
	}
	for i := range services {
		services[i].Spec = corev1.ServiceSpec{ClusterIP: fmt.Sprintf("10.96.%d.%d", 1+i/250, 1+i%250), Ports: []corev1.ServicePort{{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP}}}
	}
	return services
}

// nativeKubeletServiceEnv is kubelet's envvars.FromServices: the default namespace's kubernetes
// Service always, the pod namespace's Services only while enableServiceLinks is unset or true.
func nativeKubeletServiceEnv(spec *corev1.PodSpec, services []corev1.Service) []string {
	master := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: nativeKubernetesService}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", Ports: []corev1.ServicePort{{Name: "https", Port: 443, Protocol: corev1.ProtocolTCP}}}}
	linked := []corev1.Service{master}
	if spec.EnableServiceLinks == nil || *spec.EnableServiceLinks {
		linked = append(linked, services...)
	}
	var env []string
	for _, s := range linked {
		name, ip := strings.ToUpper(strings.ReplaceAll(s.Name, "-", "_")), s.Spec.ClusterIP
		env = append(env, fmt.Sprintf("%s_SERVICE_HOST=%s", name, ip), fmt.Sprintf("%s_SERVICE_PORT=%d", name, s.Spec.Ports[0].Port))
		for i, port := range s.Spec.Ports {
			proto, url := strings.ToLower(string(port.Protocol)), fmt.Sprintf("%s://%s:%d", strings.ToLower(string(port.Protocol)), ip, port.Port)
			link := fmt.Sprintf("%s_PORT_%d_%s", name, port.Port, port.Protocol)
			env = append(env, fmt.Sprintf("%s_SERVICE_PORT_%s=%d", name, strings.ToUpper(port.Name), port.Port), link+"="+url, link+"_PROTO="+proto, fmt.Sprintf("%s_PORT=%d", link, port.Port), link+"_ADDR="+ip)
			if i == 0 {
				env = append(env, name+"_PORT="+url)
			}
		}
	}
	return env
}

// renderedNativeHelperEnv is the container's environment as kubelet starts it, Service variables included.
func renderedNativeHelperEnv(t *testing.T, job *batchv1.Job) []string {
	t.Helper()
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatal("native helper renders one container")
	}
	env := nativeKubeletServiceEnv(&job.Spec.Template.Spec, nativeKubeletServices())
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
			if got := nativeHelperEnvVerdict(t, tc, nil, env); got != "admitted" {
				t.Fatalf("program refused the controller's rendered environment (%s): %v", got, env)
			}
			// The driver must discriminate: a missing locale is refused by every program.
			var stripped []string
			for _, e := range env {
				if !strings.HasPrefix(e, "LC_ALL=") {
					stripped = append(stripped, e)
				}
			}
			if got := nativeHelperEnvVerdict(t, tc, nil, stripped); got != nativeRuntimeRefused {
				t.Fatalf("driver admitted an environment the program refuses: %s", got)
			}
		})
	}
}

// nativeExecRolePackage declares JobRole; podexec issues execs of its own.
const nativeExecRolePackage = "native"

// nativeExecSite is one InPodNative call in production code, keyed by its enclosing function.
type nativeExecSite struct {
	argv []string // the argv literal, resolved; nil when a builder renders it at run time
}

type nativeExecEnvCase struct {
	site  string
	role  native.JobRole
	argv  func(t *testing.T) []string // the production builder; nil takes the site's literal
	env   func(t *testing.T) []string // the target container's rendered env
	check nativeHelperEnvCase         // a nil program takes the exec's own -c script
	needs string                      // a variable the program refuses without: proves the run discriminates
	quiet bool                        // the program reads no environment at all
}

// nativeExecSource parses the non-test Go files of the packages that issue native execs.
func nativeExecSource(t *testing.T) (sites map[string]nativeExecSite, roles []native.JobRole) {
	t.Helper()
	sites, consts := map[string]nativeExecSite{}, map[string]map[string]string{}
	for _, pkg := range []string{nativeExecRolePackage, "podexec"} {
		files, err := filepath.Glob(filepath.Join("..", pkg, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("%s sources unavailable: %v", pkg, err)
		}
		fset, parsed := token.NewFileSet(), []*ast.File{}
		consts[pkg] = map[string]string{}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			parsed = append(parsed, file)
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST {
					continue
				}
				for _, spec := range gen.Specs {
					value := spec.(*ast.ValueSpec)
					for i, name := range value.Names {
						if typ, ok := value.Type.(*ast.Ident); ok && pkg == nativeExecRolePackage && typ.Name == "JobRole" {
							roles = append(roles, native.JobRole(nativeExecString(t, value.Values[i], nil)))
						}
						if i < len(value.Values) {
							if lit, ok := value.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
								consts[pkg][name.Name] = nativeExecString(t, lit, nil)
							}
						}
					}
				}
			}
		}
		for _, file := range parsed {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if sel, isSel := callFun(call); !ok || !isSel || sel != "InPodNative" {
						return true
					}
					if _, dup := sites[fn.Name.Name]; dup || len(call.Args) != 6 {
						t.Fatalf("%s: exec site not keyed by one call in its function", fn.Name.Name)
					}
					sites[fn.Name.Name] = nativeExecSite{argv: nativeExecLiteral(t, fn, call.Args[4], consts[pkg])}
					return true
				})
			}
		}
	}
	return sites, roles
}

func callFun(call *ast.CallExpr) (string, bool) {
	if call == nil {
		return "", false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	return sel.Sel.Name, true
}

// nativeExecLiteral resolves an argv composite literal, inline or assigned once in fn; a builder call yields nil.
func nativeExecLiteral(t *testing.T, fn *ast.FuncDecl, arg ast.Expr, consts map[string]string) []string {
	t.Helper()
	if id, ok := arg.(*ast.Ident); ok {
		arg = nil
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok || assign.Tok != token.DEFINE {
				return arg == nil
			}
			if lhs, named := assign.Lhs[0].(*ast.Ident); named && lhs.Name == id.Name {
				arg = assign.Rhs[0]
			}
			return arg == nil
		})
	}
	switch value := arg.(type) {
	case *ast.CompositeLit:
		argv := make([]string, len(value.Elts))
		for i, elt := range value.Elts {
			argv[i] = nativeExecString(t, elt, consts)
		}
		return argv
	case *ast.CallExpr:
		return nil
	}
	t.Fatalf("%s: exec argv is neither a literal nor a builder call", fn.Name.Name)
	return nil
}

// nativeExecString resolves a string literal or package constant; anything else is a run-time value.
func nativeExecString(t *testing.T, expr ast.Expr, consts map[string]string) string {
	t.Helper()
	switch value := expr.(type) {
	case *ast.BasicLit:
		text, err := strconv.Unquote(value.Value)
		if err != nil {
			t.Fatal(err)
		}
		return text
	case *ast.Ident:
		if text, ok := consts[value.Name]; ok {
			return text
		}
	}
	return "<run-time>"
}

// nativeExecPrefix splits a rendered exec argv at the interpreter, which must take the fixed flags and a -c script.
func nativeExecPrefix(t *testing.T, argv []string) (prefix []string, script string, args []string) {
	t.Helper()
	at := slices.Index(argv, nativeRuntimePython)
	if at < 0 || len(argv) < at+5 || !slices.Equal(argv[at+1:at+4], []string{"-I", "-B", "-c"}) {
		t.Fatalf("exec argv does not run the fixed interpreter: %q", argv)
	}
	if at > 0 && argv[0] != nativeRuntimeEnvBinary {
		t.Fatalf("exec env prefix is not /usr/bin/env: %q", argv[:at])
	}
	return argv[:at], argv[at+4], argv[at+5:]
}

type nativeExecCapture struct{ argv []string }

func (c *nativeExecCapture) InPodNative(_ context.Context, _, _ string, _ podexec.NativeContainerBinding, argv []string, _ []byte) (podexec.NativeOutcome, error) {
	c.argv = slices.Clone(argv)
	return podexec.NativeOutcome{}, errBoom
}

func (*nativeExecCapture) JobCompletedNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	return podexec.NativeOutcome{}, errBoom
}

// nativeWorkerExec renders the worker Job and the execution data the controller's worker execs carry.
func nativeWorkerExec(t *testing.T) (*v1beta1.Migration, native.ExecutionData, []string) {
	t.Helper()
	m, pvc := nativeEndposJobInputs()
	job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, req := nativeEvidenceJobInputs(t)
	image := "runner@sha256:" + strings.Repeat("1", 64)
	w := native.ExecutionData{Namespace: m.Namespace, MigrationName: m.Name, PVCName: pvc.Name, JobName: m.Status.JobName, PodName: "worker-pod",
		MigrationUID: m.UID, PVCUID: pvc.UID, JobUID: "worker-job-uid", WorkerJobUID: "worker-job-uid", MigrationGeneration: m.Generation, Attempt: 1,
		Role: native.JobRoleWorker, Work: req.Work, ImageRef: image, PlatformImageID: "docker-pullable://" + image,
		TemplateSHA256: sha256.Sum256([]byte("template")), ConnectionReferenceSHA256: sha256.Sum256([]byte("references")),
		OperationID: strings.Repeat("a", 32), ProgramSHA256: native.EvidenceProgramSHA256()}
	w.Container = podexec.NativeContainerBinding{PodUID: "worker-pod-uid", ContainerName: "pgcopydb", ContainerID: "containerd://worker", ImageID: w.PlatformImageID, TemplateSHA256: w.TemplateSHA256}
	m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
	m.Status.NativeJobUID, m.Status.NativePVCUID = string(w.WorkerJobUID), string(w.PVCUID)
	return m, w, renderedNativeHelperEnv(t, job)
}

func nativeExecEnvCases(t *testing.T) []nativeExecEnvCase {
	t.Helper()
	captured := func(call func(*native.Client, *v1beta1.Migration, native.ExecutionData)) func(*testing.T) []string {
		return func(t *testing.T) []string {
			m, w, _ := nativeWorkerExec(t)
			transport := &nativeExecCapture{}
			call(native.NewClient(transport), m, w)
			if len(transport.argv) == 0 {
				t.Fatal("production client issued no exec")
			}
			return transport.argv
		}
	}
	worker := func(t *testing.T) []string { _, _, env := nativeWorkerExec(t); return env }
	helper := func(entry string) func(*testing.T) []string {
		return func(t *testing.T) []string {
			for _, tc := range nativeHelperEnvCases(t) {
				if tc.entry == entry {
					job, err := tc.build(t)
					if err != nil {
						t.Fatal(err)
					}
					return renderedNativeHelperEnv(t, job)
				}
			}
			t.Fatalf("no helper Job renders %s", entry)
			return nil
		}
	}
	_, w, _ := nativeWorkerExec(t)
	hash := hex.EncodeToString(w.ProgramSHA256[:])
	source := func(kind string) nativeHelperEnvCase {
		return nativeHelperEnvCase{entry: "run_source_observation", program: native.EvidenceProgram(), args: []string{kind, w.OperationID, hash}, work: &w.Work}
	}
	// Every relay writer embeds the evidence writer, whose deliver holds the environment checks.
	writer := nativeHelperEnvCase{entry: "deliver", program: native.EvidenceRequestWriterProgram(), args: []string{w.OperationID}}
	const target = "PGCOPYDB_TARGET_PGURI"
	return []nativeExecEnvCase{
		{site: "sourceObservation", role: native.JobRoleWorker, env: worker, check: source("source_head"), needs: nativeLCAllEnv,
			argv: captured(func(c *native.Client, m *v1beta1.Migration, w native.ExecutionData) {
				_, _ = c.CurrentEndLSN(context.Background(), m, w)
			})},
		{site: "sourceObservation", role: native.JobRoleWorker, env: worker, check: source("active_lag"), needs: nativeLCAllEnv,
			argv: captured(func(c *native.Client, m *v1beta1.Migration, w native.ExecutionData) {
				_, _ = c.SampleLag(context.Background(), m, w)
			})},
		{site: "Progress", role: native.JobRoleWorker, env: worker, needs: target,
			argv: captured(func(c *native.Client, _ *v1beta1.Migration, w native.ExecutionData) {
				_, _ = c.Progress(context.Background(), w)
			})},
		{site: "SetEndpos", role: native.JobRoleWorker, env: worker, needs: target,
			argv: captured(func(c *native.Client, _ *v1beta1.Migration, w native.ExecutionData) {
				_, _ = c.SetEndpos(context.Background(), w, "0/16B6C50")
			})},
		{site: "RequestNativeTerm", role: native.JobRoleWorker, env: worker, quiet: true},
		{site: "ReadCompleted", role: native.JobRoleEvidence, env: helper("run_completed_evidence"), check: writer, needs: nativeLCAllEnv},
		{site: "ReadCleanedCompleted", role: native.JobRoleEvidence, env: helper("cleaned_completion_main"), check: writer, needs: nativeLCAllEnv},
		{site: "ReadCleanupRejoinCompleted", role: native.JobRoleEvidence, env: helper("cleanup_rejoin_completion_main"), check: writer, needs: nativeLCAllEnv},
		{site: "ReadOwnedState", role: native.JobRoleCleanup, env: helper("owned_inventory_main"), check: writer, needs: nativeLCAllEnv},
		{site: "ReadRetryReadiness", role: native.JobRoleRetryGate, env: helper("run_target_retry"), check: writer, needs: nativeLCAllEnv},
	}
}

// Roles whose container runs the engine binary itself and is never exec'd into: nothing Python checks their env.
var nativeEngineOnlyRoles = map[native.JobRole]func(*testing.T) (*batchv1.Job, error){
	native.JobRoleEndpos: func(*testing.T) (*batchv1.Job, error) {
		m, pvc := nativeEndposJobInputs()
		return buildNativeEndposJob(m, pvc, nativeEvidenceTestImage, nativeEndposTestName, nativeEndposTestE)
	},
}

func TestNativeExecEnvironmentMatchesProgramChecks(t *testing.T) {
	sites, roles := nativeExecSource(t)
	cases := nativeExecEnvCases(t)
	covered, served := map[string]bool{}, map[native.JobRole]bool{}
	for _, tc := range cases {
		covered[tc.site], served[tc.role] = true, true
	}
	for name := range sites {
		if !covered[name] {
			t.Errorf("exec site %s has no environment contract row", name)
		}
	}
	if len(roles) == 0 {
		t.Fatal("no JobRole constants found")
	}
	for _, role := range roles {
		if build, engine := nativeEngineOnlyRoles[role]; engine {
			job, err := build(t)
			if err != nil || len(job.Spec.Template.Spec.Containers) != 1 || slices.Contains(job.Spec.Template.Spec.Containers[0].Command, nativeRuntimePython) ||
				job.Spec.Template.Spec.Containers[0].Command[len(job.Spec.Template.Spec.Containers[0].Command)-1] != "/usr/local/bin/rpgmigrate" {
				t.Errorf("role %s is listed as engine-only but its Job does not run the engine directly: %v", role, err)
			}
			if served[role] {
				t.Errorf("role %s is both engine-only and exec'd into", role)
			}
		} else if !served[role] {
			t.Errorf("role %s has no environment contract row", role)
		}
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i, " ", tc.site), func(t *testing.T) {
			site, ok := sites[tc.site]
			if !ok {
				t.Fatal("row names no production exec site")
			}
			argv := site.argv
			if (argv == nil) != (tc.argv != nil) {
				t.Fatal("a builder-rendered argv needs the production builder, a literal one the literal")
			}
			if tc.argv != nil {
				argv = tc.argv(t)
			}
			prefix, script, args := nativeExecPrefix(t, argv)
			if tc.quiet {
				if script == "<run-time>" || strings.Contains(script, "environ") || strings.Contains(script, "getenv") {
					t.Fatalf("exec script is unresolved or reads the environment: %q", script)
				}
				return
			}
			check := tc.check
			if check.program == nil {
				check.program, check.args = []byte(script), args
			}
			env := tc.env(t)
			if got := nativeHelperEnvVerdict(t, check, prefix, env); got != "admitted" {
				t.Fatalf("program refused the exec argv %q over the rendered env (%s): %v", prefix, got, env)
			}
			stripped := slices.DeleteFunc(slices.Clone(env), func(e string) bool { return strings.HasPrefix(e, tc.needs+"=") })
			if len(stripped) == len(env) {
				t.Fatalf("rendered env carries no %s to strip", tc.needs)
			}
			if got := nativeHelperEnvVerdict(t, check, prefix, stripped); got != nativeRuntimeRefused {
				t.Fatalf("driver admitted an environment without %s: %s", tc.needs, got)
			}
		})
	}
}

// Service links must stay off: the linked namespace gives every native program an LD_ name and
// more than 64 KiB of environment, which each one refuses, so a linked pod is a stuck pod.
func TestNativeHelperEnvironmentRefusesServiceLinks(t *testing.T) {
	linked := nativeKubeletServiceEnv(&corev1.PodSpec{}, nativeKubeletServices())
	size := 0
	for _, e := range linked {
		size += len(e) + 1
	}
	if !slices.Contains(linked, "LD_CACHE_SERVICE_HOST=10.96.1.1") || size <= 65536 {
		t.Fatalf("kubelet stand-in links no ld-* Service or stays under the cap: %d bytes", size)
	}
	off := nativeKubeletServiceEnv(&corev1.PodSpec{EnableServiceLinks: ptr.To(false)}, nativeKubeletServices())
	if len(off) != 8 || slices.ContainsFunc(off, func(e string) bool { return !strings.HasPrefix(e, "KUBERNETES_") }) {
		t.Fatalf("kubelet stand-in links namespace Services with links off: %v", off)
	}
	for i, tc := range nativeHelperEnvCases(t) {
		t.Run(fmt.Sprint(i, " ", tc.entry), func(t *testing.T) {
			job, err := tc.build(t)
			if err != nil || job == nil {
				t.Fatalf("helper Job unavailable: %v", err)
			}
			if links := job.Spec.Template.Spec.EnableServiceLinks; links == nil || *links {
				t.Fatal("native helper Job leaves service links on")
			}
			for _, links := range []*bool{nil, ptr.To(true)} {
				job.Spec.Template.Spec.EnableServiceLinks = links
				if got := nativeHelperEnvVerdict(t, tc, nil, renderedNativeHelperEnv(t, job)); got != nativeRuntimeRefused {
					t.Fatalf("program admitted a linked namespace's environment: %s", got)
				}
			}
		})
	}
}

// Every native builder is listed, checked by parsing the package, so a new one cannot skip the setting.
func TestNativeBuildersDisableServiceLinks(t *testing.T) {
	builders := map[string]func(*testing.T) (*batchv1.Job, error){
		"buildNativeVersionJob": func(*testing.T) (*batchv1.Job, error) {
			m, _ := nativeEndposJobInputs()
			return buildNativeVersionJob(m, nativeEvidenceTestImage, "rpgmigrate-version-"+probeTestOperation)
		},
		"buildNativeWorkerJob": func(*testing.T) (*batchv1.Job, error) {
			m, pvc := nativeEndposJobInputs()
			return buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
		},
		"buildNativeEndposJob": nativeEngineOnlyRoles[native.JobRoleEndpos],
		"buildNativeOwnedInventoryJob": func(t *testing.T) (*batchv1.Job, error) {
			m, w := cleanedJobInputs(t)
			w.Role, w.JobName, w.ProgramSHA256, w.PlatformImageID = native.JobRoleCleanup, nativeInventoryJobPrefix+w.OperationID, native.OwnedInventoryProgramSHA256(), w.ImageRef
			return buildNativeOwnedInventoryJob(m, nativeEvidenceTestImage, w)
		},
		"buildNativeCleanupJob": func(t *testing.T) (*batchv1.Job, error) {
			f := newCleanupInventoryFixture(t)
			owned, err := f.read(t)
			if err != nil {
				t.Fatal(err)
			}
			return buildNativeCleanupJob(f.migration, f.pvc, f.want.ImageRef, nativeCleanupTestName, owned)
		},
	}
	helpers := map[string]string{"run_native_preflight": "buildNativePreflightJob", "run_completed_evidence": "buildNativeEvidenceJob", "run_target_retry": "buildNativeRecoveryReadJob",
		"cleaned_completion_main": "buildNativeCleanedCompletionJob", "cleanup_rejoin_completion_main": "buildNativeCleanupRejoinCompletionJob", nativeComparisonEntry: "buildNativeCompareJob"}
	for _, tc := range nativeHelperEnvCases(t) {
		if name, ok := helpers[tc.entry]; ok {
			builders[name] = tc.build
		}
	}
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("controller sources unavailable: %v", err)
	}
	declared := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "buildNative") {
				declared++
				if builders[fn.Name.Name] == nil {
					t.Errorf("native builder %s has no service links row", fn.Name.Name)
				}
			}
		}
	}
	if declared != len(builders) {
		t.Fatalf("%d native builders declared, %d rows", declared, len(builders))
	}
	for name, build := range builders {
		t.Run(name, func(t *testing.T) {
			job, err := build(t)
			if err != nil || job == nil {
				t.Fatalf("builder refused its fixture: %v", err)
			}
			if links := job.Spec.Template.Spec.EnableServiceLinks; links == nil || *links {
				t.Fatal("native pod leaves service links on")
			}
		})
	}
	// Legacy pods keep the API default.
	m := passwordMigration()
	for name, build := range map[string]func() (*batchv1.Job, error){
		"buildJob":          func() (*batchv1.Job, error) { return buildJob(m, testRunnerImage, 1) },
		"buildCleanupJob":   func() (*batchv1.Job, error) { return buildCleanupJob(m, testRunnerImage) },
		"buildPreflightJob": func() (*batchv1.Job, error) { return buildPreflightJob(m, testRunnerImage) },
	} {
		if job, err := build(); err != nil || job.Spec.Template.Spec.EnableServiceLinks != nil {
			t.Errorf("legacy %s changed service links: %v", name, err)
		}
	}
}
