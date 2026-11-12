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
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const rejoinCanonicalSHA = "a2890d3fb11b5aa334b9d5073f9f54e0d72b665840f7ea54eaf1d4efb81e6658"

func TestNativeCleanupRejoinCompletionProgram(t *testing.T) {
	f := newCleanupRejoinFixture(t)
	command, args, _, volumes, mounts, err := CleanupRejoinCompletionMaterialization(f.base.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	program, relay := CleanupRejoinCompletionProgram(), CleanupRejoinCompletionRequestWriterProgram()
	if len(cleanupRejoinCompletionProgramSource) > CleanupRejoinCompletionHelperSourceLimit || len(program) == 0 || sha256.Sum256(program) != CleanupRejoinCompletionProgramSHA256() || len(relay) == 0 || len(relay) > 16384 || len(cleanupRejoinCompletionBootstrap) > 2048 {
		t.Fatal("bounded fixed program unavailable")
	}
	canonical := EvidenceProgramSHA256()
	if hex.EncodeToString(canonical[:]) != rejoinCanonicalSHA || len(EvidenceProgram()) != 120465 {
		t.Fatal("ordinary canonical source changed")
	}
	if !bytes.Equal(program, append(append(OwnedInventoryProgram(), []byte(cleanupRejoinCompletionSeparator)...), []byte(cleanupRejoinCompletionProgramSource)...)) {
		t.Fatal("composite source changed")
	}
	if !volumes[0].PersistentVolumeClaim.ReadOnly || !mounts[0].ReadOnly {
		t.Fatal("cleaned proof writable work state")
	}
	total, maximum := 0, 0
	for _, arg := range append(slices.Clone(command), args...) {
		total += len(arg) + 1
		maximum = max(maximum, len(arg)+1)
		if len(arg)+1 > EvidenceMainArgumentLimit {
			t.Fatal("argument limit exceeded")
		}
	}
	if ValidateEvidenceJobSize(f.base.job) != nil {
		t.Fatal("Job size cap exceeded")
	}
	jobJSON, err := json.Marshal(f.base.job)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("core=%d inventory=%d helper=%d composite=%d bootstrap=%d relay=%d argv_NUL=%d max_arg_NUL=%d job_json=%d", len(EvidenceProgram()), len(ownedInventoryProgramSource), len(cleanupRejoinCompletionProgramSource), len(program), len(cleanupRejoinCompletionBootstrap), len(relay), total, maximum, len(jobJSON))
	program[0] ^= 1
	relay[0] ^= 1
	if bytes.Equal(program, CleanupRejoinCompletionProgram()) || bytes.Equal(relay, CleanupRejoinCompletionRequestWriterProgram()) {
		t.Fatal("source export aliases owned bytes")
	}
}

func TestNativeCleanupRejoinCompletionProgramMechanisms(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	path, err := filepath.Abs("cleanup_rejoin_completion_program_test.py")
	if err != nil {
		t.Fatal(err)
	}
	const inventory = `import ast,json,sys
t=ast.parse(open(sys.argv[1]).read())
print(json.dumps(sorted(f.name for c in t.body if isinstance(c,ast.ClassDef) and c.name=='CleanupRejoinTests' for f in c.body if isinstance(f,ast.FunctionDef) and f.name.startswith('test_'))))
`
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, python, "-I", "-B", "-c", inventory, path).Output()
	var actual []string
	want := []string{"test_ControlLock", "test_MainClosure", "test_PartialState", "test_Request", "test_RoutesStayNarrow", "test_SourceObservations", "test_TargetAuthority", "test_WriterPolicy"}
	if err != nil || json.Unmarshal(output, &actual) != nil || !slices.Equal(want, actual) {
		t.Fatalf("nonempty cleaned mechanism inventory changed: %v %v", actual, err)
	}
	for _, name := range actual {
		t.Run(name, func(t *testing.T) {
			out, err := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path, "CleanupRejoinTests."+name).CombinedOutput()
			if err != nil || !bytes.Contains(out, []byte("Ran 1 test")) || !bytes.Contains(out, []byte("\nOK\n")) || bytes.Contains(out, []byte("skipped=")) {
				t.Fatalf("required mechanism failed: %v\n%s", err, out)
			}
		})
	}
}

func TestNativeCleanupRejoinCompletionProgramClosedComposite(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	f := newCleanupRejoinFixture(t)
	_, args, err := cleanupRejoinCompletionScripts(f.want, "exec \"$0\" \"$@\"")
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range [][]byte{EvidenceProgram(), []byte(ownedInventoryProgramSource), []byte(cleanupRejoinCompletionProgramSource)} {
		compressed, err := base64.StdEncoding.Strict().DecodeString(args[11+i])
		if err != nil {
			t.Fatal(err)
		}
		reader, err := zlib.NewReader(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		raw, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(raw, want) {
			t.Fatal("fixed composite changed source bytes")
		}
	}
	for _, edit := range []func([]string){
		func(a []string) { a[2] = strings.Repeat("0", 64) },
		func(a []string) { a[3] += "?" },
		func(a []string) { a[4] = a[5] },
		func(a []string) { a[5] = a[4] },
		func(a []string) {
			a[3] = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, EvidenceProgramSourceLimit+1))
		},
	} {
		argv := slices.Clone(args[8:])
		edit(argv)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := exec.CommandContext(ctx, python, append([]string{"-I", "-B", "-c"}, argv...)...).CombinedOutput()
		cancel()
		if err == nil || len(out) != 0 || ctx.Err() == context.DeadlineExceeded {
			t.Fatal("corrupt composite was not quietly refused before input or SQL")
		}
	}
}

func TestNativeCleanupRejoinCompletionProgramRelay(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	f := newCleanupRejoinFixture(t)
	body, err := cleanupRejoinRequest(f.want, fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	raw := cleanupRejoinResult(t, f.want, body)
	script := `import sys,base64
sys.argv=['-']
try:exec(base64.b64decode(REJOIN_PROGRAM_B64),{'__name__':'_cleaned_loader'})
except SystemExit as e:assert e.code==1
m=sys.modules['_rpgmigrate_native_cleanup_rejoin_writer']
b=base64.b64decode(REJOIN_REQUEST_B64);r=base64.b64decode(REJOIN_RESULT_B64);o=REJOIN_OPERATION_ID
assert m.strict_body(b,o)['kind']=='cleanup_rejoin_completed_evidence'
assert m.result_envelope(r,b,o)is True
for value in (r+r,r.replace(b'"cleanup_pending":true',b'"cleanup_pending":false'),r.replace(b'"source_resources_checked":true',b'"source_resources_checked":false')):
 try:m.result_envelope(value,b,o)
 except BaseException:pass
 else:raise AssertionError('invalid cleaned relay result accepted')
`
	for key, value := range map[string]string{"REJOIN_PROGRAM_B64": base64.StdEncoding.EncodeToString(CleanupRejoinCompletionRequestWriterProgram()), "REJOIN_REQUEST_B64": base64.StdEncoding.EncodeToString(body), "REJOIN_RESULT_B64": base64.StdEncoding.EncodeToString(raw), "REJOIN_OPERATION_ID": f.want.OperationID} {
		script = strings.ReplaceAll(script, key, strconv.Quote(value))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, python, "-I", "-B", "-c", script).CombinedOutput()
	if err != nil || len(out) != 0 || ctx.Err() != nil {
		t.Fatalf("generated cleaned relay policy failed: %v\n%s", err, out)
	}
}
