package native

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeComparisonLargeReport(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	path, err := filepath.Abs("comparison_program_test.py")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path).CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("Ran 9 tests")) || !bytes.Contains(output, []byte("\nOK\n")) {
		t.Fatalf("comparison mechanism failed: %s", output)
	}
}

func TestNativeCompositeResolverCalls(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	const check = `import ast,sys
tree=ast.parse(sys.stdin.buffer.read())
calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='qualified_resolver_policy']
assert len(calls)==int(sys.argv[1])
assert all(len(n.args)==1 and not n.keywords and isinstance(n.args[0],ast.Name) and n.args[0].id=='resolver' for n in calls)
`
	for _, test := range []struct {
		name, count string
		program     []byte
	}{
		{"canonical", "8", EvidenceProgram()},
		{"comparison", "10", ComparisonProgram()},
		{"inventory", "10", OwnedInventoryProgram()},
		{"cleaned", "12", CleanedCompletionProgram()},
		{"cleanup rejoin", "12", CleanupRejoinCompletionProgram()},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-I", "-B", "-c", check, test.count)
			cmd.Stdin = bytes.NewReader(test.program)
			out, err := cmd.CombinedOutput()
			if err != nil || len(out) != 0 || ctx.Err() != nil {
				t.Fatal("joined production source lost retained resolver policy arguments")
			}
		})
	}
}

func TestNativeComparisonProgramDelivery(t *testing.T) {
	program := ComparisonProgram()
	if !bytes.Equal(program, append(append(EvidenceProgram(), []byte(comparisonSourceSeparator)...), []byte(comparisonProgramSource)...)) || len(program) > EvidenceProgramSourceLimit+len(comparisonSourceSeparator)+ComparisonHelperSourceLimit || sha256.Sum256(program) != ComparisonProgramSHA256() {
		t.Fatal("composite changed canonical source or bound")
	}
	command, args, err := comparisonScripts(comparisonCheckData, strings.Repeat("a", 32), "set -eu\nexec \"$0\" \"$@\"\n")
	if err != nil || len(command) != 4 || command[3] != nativeEnvExecutable || len(args) != 14 || args[8] != comparisonBootstrap || len(comparisonBootstrap) > 2048 {
		t.Fatal("comparison delivery exceeds actual argument cap")
	}
	total, maxArg := 0, 0
	for _, arg := range append(append([]string{}, command...), args...) {
		total += len(arg) + 1
		maxArg = max(maxArg, len(arg)+1)
	}
	t.Logf("canonical=%d helper=%d composite=%d bootstrap=%d all argv including NULs=%d largest argument including NUL=%d", len(EvidenceProgram()), len(comparisonProgramSource), len(program), len(comparisonBootstrap), total, maxArg)
	for i, source := range [][]byte{EvidenceProgram(), []byte(comparisonProgramSource)} {
		raw, err := base64.StdEncoding.Strict().DecodeString(args[12+i])
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
			t.Fatal("delivery changed exact source")
		}
	}
	program[0] ^= 1
	if bytes.Equal(program, ComparisonProgram()) {
		t.Fatal("program source is mutable")
	}
	for _, check := range []string{"", "DATA", "data;bad"} {
		if _, _, err := comparisonScripts(check, strings.Repeat("a", 32), ""); err == nil {
			t.Fatal("invalid check accepted")
		}
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	for _, test := range []struct {
		name string
		edit func([]string)
	}{
		{"fixed unavailable guard", func([]string) {}},
		{"wrong hash", func(a []string) { a[3] = strings.Repeat("0", 64) }},
		{"bad base64", func(a []string) { a[4] += "?" }},
		{"compressed trailing bytes", func(a []string) {
			raw, _ := base64.StdEncoding.DecodeString(a[4])
			a[4] = base64.StdEncoding.EncodeToString(append(raw, 'x'))
		}},
		{"truncated helper", func(a []string) { a[5] = a[5][:len(a[5])-4] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := append([]string{}, args[8:]...)
			test.edit(a)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, append([]string{"-I", "-B", "-c"}, a...)...).CombinedOutput()
			if err == nil || len(output) != 0 || ctx.Err() != nil {
				t.Fatal("bootstrap executed or exposed refused input")
			}
		})
	}
	t.Run("source budgets", func(t *testing.T) { testComparisonSourceBudgets(t, python) })
}

func testComparisonSourceBudgets(t *testing.T, python string) {
	t.Helper()
	helperPrefix := []byte("def comparison_main(argv): return 0\n#")
	helperAtLimit := append(append(bytes.Clone(helperPrefix), bytes.Repeat([]byte{'x'}, ComparisonHelperSourceLimit-len(helperPrefix)-1)...), '\n')
	for _, test := range []struct {
		name              string
		canonical, helper []byte
		ok                bool
	}{
		{"private entrypoint", EvidenceProgram(), []byte("def comparison_main(argv): return 0\n"), true},
		{"helper exact cap", EvidenceProgram(), helperAtLimit, true},
		{"helper overflow", EvidenceProgram(), append(bytes.Clone(helperAtLimit), '\n'), false},
		{"ambiguous source boundary", EvidenceProgram(), []byte("def comparison_main(argv): return 0\n" + comparisonSourceSeparator), false},
		{"canonical overflow", append(append(EvidenceProgram(), []byte("\n#")...), append(bytes.Repeat([]byte{'x'}, EvidenceProgramSourceLimit-len(EvidenceProgram())), '\n')...), []byte("def comparison_main(argv): return 0\n"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			payloads := []string{}
			for _, s := range [][]byte{test.canonical, test.helper} {
				var b bytes.Buffer
				z := zlib.NewWriter(&b)
				_, err := z.Write(s)
				closeErr := z.Close()
				if err != nil || closeErr != nil {
					t.Fatal("fixture compression failed")
				}
				payloads = append(payloads, base64.StdEncoding.EncodeToString(b.Bytes()))
			}
			hash := sha256.Sum256(append(append(bytes.Clone(test.canonical), []byte(comparisonSourceSeparator)...), test.helper...))
			a := []string{"-I", "-B", "-c", comparisonBootstrap, comparisonCheckData, strings.Repeat("a", 32), hex.EncodeToString(hash[:]), payloads[0], payloads[1]}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, a...).CombinedOutput()
			if (err == nil) != test.ok || len(output) != 0 || ctx.Err() != nil {
				t.Fatal("comparison source budget or private entrypoint violated")
			}
		})
	}
}
