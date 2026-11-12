package native

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNativeOwnedInventoryProgramCanonicalSource(t *testing.T) {
	program := OwnedInventoryProgram()
	if !bytes.Equal(program, append(append(EvidenceProgram(), []byte(ownedInventorySeparator)...), []byte(ownedInventoryProgramSource)...)) || sha256.Sum256(program) != OwnedInventoryProgramSHA256() {
		t.Fatal("composite changed canonical bytes or identity")
	}
	coreHash := EvidenceProgramSHA256()
	writerHash := EvidenceRequestWriterSHA256()
	if hex.EncodeToString(coreHash[:]) != "0536e85dc1a586a09e929f5b628e81dc335503321853b767b8f904cac38c5b5f" || hex.EncodeToString(writerHash[:]) != "4412adaaa55dbd69aff1ad754c7252562da32440e0f2d2a07e60205d4fb6b882" {
		t.Fatal("canonical source changed")
	}
}

func TestNativeOwnedInventoryProgram(t *testing.T) {
	program := OwnedInventoryProgram()
	f := newOwnedFixture(t)
	command, args, env, volumes, mounts, err := OwnedInventoryMaterialization(f.base.m, f.want)
	if err != nil || len(command) != 4 || len(args) != 13 || len(ownedInventoryBootstrap) > 2048 {
		t.Fatal("inventory delivery unavailable")
	}
	sourceRoles := map[string]bool{}
	for _, e := range env {
		if strings.Contains(e.Name, "SOURCE") {
			sourceRoles["source"] = true
		}
		if strings.Contains(e.Name, "TARGET") {
			sourceRoles["target"] = true
		}
	}
	if !sourceRoles["source"] || !sourceRoles["target"] || !volumes[0].PersistentVolumeClaim.ReadOnly || !mounts[0].ReadOnly || volumes[1].EmptyDir.Medium != corev1.StorageMediumMemory || volumes[1].EmptyDir.SizeLimit.Value() != 64*1024*1024 {
		t.Fatal("both connections and isolated read-only storage were not preserved")
	}
	total, maxArg := 0, 0
	for _, arg := range append(append([]string{}, command...), args...) {
		total += len(arg) + 1
		maxArg = max(maxArg, len(arg)+1)
		if len(arg)+1 > EvidenceMainArgumentLimit {
			t.Fatal("argument cap exceeded")
		}
	}
	for i, source := range [][]byte{EvidenceProgram(), []byte(ownedInventoryProgramSource)} {
		raw, err := base64.StdEncoding.Strict().DecodeString(args[11+i])
		if err != nil {
			t.Fatal(err)
		}
		reader, err := zlib.NewReader(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil || closeErr != nil || !bytes.Equal(decoded, source) {
			t.Fatal("actual framing changed source bytes")
		}
	}
	relay := OwnedInventoryRequestWriterProgram()
	if len(ownedInventoryWriterAdapterSource) > 3072 || len(relay) == 0 || len(relay) > 16384 || sha256.Sum256(relay) != OwnedInventoryRequestWriterSHA256() {
		t.Fatal("fixed relay exceeded its cap or identity")
	}
	job := f.base.job.DeepCopy()
	job.TypeMeta = metav1.TypeMeta{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: sourceJobKind}
	jobJSON, err := json.Marshal(job)
	if err != nil || ValidateEvidenceJobSize(job) != nil {
		t.Fatal("materialized inventory Job exceeds JSON cap")
	}
	t.Logf("canonical=%d helper=%d composite=%d bootstrap=%d relay=%d adapter=%d argv_with_NULs=%d max_arg_with_NUL=%d job_json=%d", len(EvidenceProgram()), len(ownedInventoryProgramSource), len(program), len(ownedInventoryBootstrap), len(relay), len(ownedInventoryWriterAdapterSource), total, maxArg, len(jobJSON))
	program[0] ^= 1
	relay[0] ^= 1
	if bytes.Equal(program, OwnedInventoryProgram()) || bytes.Equal(relay, OwnedInventoryRequestWriterProgram()) {
		t.Fatal("program export aliases source")
	}
}

func TestNativeOwnedInventoryMaterializationRefusals(t *testing.T) {
	f := newOwnedFixture(t)
	w := f.want
	w.JobUID = ""
	w.PodName = ""
	w.Container = readerBinding(f.base.want)
	w.Container.PodUID = ""
	if _, _, _, _, _, err := OwnedInventoryMaterialization(f.base.m, w); err != nil {
		t.Fatal("pre-create materialization requires fabricated execution identities")
	}
	for _, mutate := range []func(*ExecutionData){func(w *ExecutionData) { w.Role = JobRoleEvidence }, func(w *ExecutionData) { w.ProgramSHA256[0] ^= 1 }, func(w *ExecutionData) { w.Work.WorkdirID = "foreign" }} {
		w := f.want
		mutate(&w)
		if _, _, _, _, _, err := OwnedInventoryMaterialization(f.base.m, w); err == nil {
			t.Fatal("invalid materialization admitted")
		}
	}
}

func TestNativeOwnedInventoryComposite(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter absent")
	}
	w := ownedWant()
	_, args, err := ownedInventoryScripts(EvidenceProgram(), []byte(ownedInventoryProgramSource), w, "exec \"$0\" \"$@\"")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func([]string)
	}{
		{"runtime_refuses", func([]string) {}},
		{"wrong_composite_hash", func(a []string) { a[2] = strings.Repeat("0", 64) }},
		{"bad_base64", func(a []string) { a[3] += "?" }},
		{"core_mutation", func(a []string) {
			raw, _ := base64.StdEncoding.DecodeString(a[3])
			raw[len(raw)/2] ^= 1
			a[3] = base64.StdEncoding.EncodeToString(raw)
		}},
		{"trailing_zlib", func(a []string) {
			raw, _ := base64.StdEncoding.DecodeString(a[3])
			a[3] = base64.StdEncoding.EncodeToString(append(raw, 'x'))
		}},
		{"truncated_zlib", func(a []string) { a[4] = a[4][:len(a[4])-4] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := append([]string{}, args[8:]...)
			tc.edit(a)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, append([]string{"-I", "-B", "-c"}, a...)...).CombinedOutput()
			if err == nil || len(output) != 0 || ctx.Err() != nil {
				t.Fatal("refused loader executed or exposed data")
			}
		})
	}
	helperPrefix := []byte("def owned_inventory_main(argv): return 0\n#")
	helperAtLimit := append(append(bytes.Clone(helperPrefix), bytes.Repeat([]byte{'x'}, OwnedInventoryHelperSourceLimit-len(helperPrefix)-1)...), '\n')
	coreAtLimit := append(append(EvidenceProgram(), []byte("\n#")...), bytes.Repeat([]byte{'x'}, EvidenceProgramSourceLimit-len(EvidenceProgram())-3)...)
	coreAtLimit = append(coreAtLimit, '\n')
	for _, tc := range []struct {
		name         string
		core, helper []byte
		ok           bool
	}{
		{"fixed_entrypoint", EvidenceProgram(), []byte("def owned_inventory_main(argv): return 0\n"), true},
		{"helper_exact_cap", EvidenceProgram(), helperAtLimit, true},
		{"core_exact_cap", coreAtLimit, []byte("def owned_inventory_main(argv): return 0\n"), true},
		{"overflow_helper", EvidenceProgram(), append(bytes.Repeat([]byte{'#'}, OwnedInventoryHelperSourceLimit), '\n'), false},
		{"overflow_core", append(bytes.Repeat([]byte{'#'}, EvidenceProgramSourceLimit), '\n'), []byte("def owned_inventory_main(argv): return 0\n"), false},
		{"ambiguous_boundary", EvidenceProgram(), []byte("def owned_inventory_main(argv): return 0\n" + ownedInventorySeparator), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payloads := []string{}
			for _, source := range [][]byte{tc.core, tc.helper} {
				var b bytes.Buffer
				z := zlib.NewWriter(&b)
				_, e := z.Write(source)
				c := z.Close()
				if e != nil || c != nil {
					t.Fatal("compression failed")
				}
				payloads = append(payloads, base64.StdEncoding.EncodeToString(b.Bytes()))
			}
			hash := sha256.Sum256(append(append(bytes.Clone(tc.core), []byte(ownedInventorySeparator)...), tc.helper...))
			a := []string{"-I", "-B", "-c", ownedInventoryBootstrap, w.OperationID, hex.EncodeToString(hash[:]), payloads[0], payloads[1]}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, a...).CombinedOutput()
			if (err == nil) != tc.ok || len(output) != 0 || ctx.Err() != nil {
				t.Fatal("loader source cap or fixed entrypoint violated")
			}
		})
	}
}

func TestNativeOwnedInventoryPython(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter absent")
	}
	path, err := filepath.Abs("owned_inventory_program_test.py")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path).CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("Ran 8 tests")) || !bytes.Contains(output, []byte("\nOK\n")) || bytes.Contains(output, []byte("Ran 0 tests")) || bytes.Contains(output, []byte("skipped=")) {
		t.Fatalf("required nonempty inventory Python suite failed: %s", output)
	}
	for _, name := range []string{"ConfirmedPhases", "PendingRefusals", "NoEndposAuthority", "SourceReplacement", "ClosedSessions", "ResolverPolicy", "CompositeDelivery", "RelayPolicy"} {
		if !bytes.Contains(output, []byte("test_Inventory"+name+" (")) {
			t.Fatalf("required inventory Python case %s did not run: %s", name, output)
		}
	}
}

func TestNativeOwnedInventoryRelay(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter absent")
	}
	w := ownedWant()
	body, err := ownedRequest(w)
	if err != nil {
		t.Fatal(err)
	}
	raw := ownedResult(t, w, body)
	script := `import sys,base64
sys.argv=['-']
try:
 exec(base64.b64decode(PROGRAM),{'__name__':'_inventory_loader'})
except SystemExit as e:
 assert e.code==1
m=sys.modules['_rpgmigrate_native_owned_inventory_writer']
b=base64.b64decode(BODY);r=base64.b64decode(RESULT);o=OPERATION
assert m.strict_body(b,o)['kind']=='owned_inventory'
assert m.result_envelope(r,b,o) is True
for value in (b.replace(b'"cleanup"',b'"evidence"'),b.replace(b'"version":1',b'"version":true'),b.replace(b'"version":1',b'"version":1,"version":1'),b.replace(b'"version":1',b'"version":1,"saved_e":"0/20"')):
 try:m.strict_body(value,o)
 except BaseException:pass
 else:raise AssertionError('request policy admitted invalid bytes')
for value in (r.replace(b'"source_closed":true',b'"source_closed":false'),r.replace(b'"owned":true',b'"owned":false',1),r.replace(b'"version":1',b'"version":1,"checkpoint":{}'),r+r):
 try:m.result_envelope(value,b,o)
 except BaseException:pass
 else:raise AssertionError('result policy admitted invalid bytes')
`
	for key, value := range map[string]string{"PROGRAM": base64.StdEncoding.EncodeToString(OwnedInventoryRequestWriterProgram()), "BODY": base64.StdEncoding.EncodeToString(body), "RESULT": base64.StdEncoding.EncodeToString(raw), "OPERATION": w.OperationID} {
		script = strings.ReplaceAll(script, key, strconv.Quote(value))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, python, "-I", "-B", "-c", script).CombinedOutput()
	if err != nil || len(output) != 0 || ctx.Err() != nil {
		t.Fatalf("Go-generated inventory relay policy failed: %s", output)
	}
}

func TestNativeOwnedInventoryReferencePrivacy(t *testing.T) {
	f := newOwnedFixture(t)
	f.base.m.Spec.Source.URISecretRef = nil
	f.base.m.Spec.Source.Host = "inventory.invalid"
	f.base.m.Spec.Source.Database = "database"
	f.base.m.Spec.Source.Username = "private-one"
	first, err := OwnedInventoryConnectionReferenceSHA256(f.base.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	f.base.m.Spec.Source.Username = "private-two"
	second, err := OwnedInventoryConnectionReferenceSHA256(f.base.m, f.want)
	if err != nil || first != second {
		t.Fatal("literal credential affected structural hash")
	}
	f.base.m.Spec.Target.URISecretRef.Key = "rotated-reference"
	third, err := OwnedInventoryConnectionReferenceSHA256(f.base.m, f.want)
	if err != nil || third == second {
		t.Fatal("changed structural target reference was not bound")
	}
}
