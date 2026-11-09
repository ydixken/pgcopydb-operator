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
	"strings"
	"testing"
	"time"
)

func TestNativeSourceProgramDelivery(t *testing.T) {
	w := sourceWant()
	for _, kind := range []string{sourceHeadKind, activeLagKind} {
		argv, err := sourceExecArgv(kind, w)
		if err != nil || len(argv) != 14 || argv[0] != nativeEnvExecutable || argv[5] != nativePythonExecutable || argv[9] != sourceBootstrap || argv[10] != kind || argv[11] != w.OperationID {
			t.Fatal("fixed source argv not rendered")
		}
		compressed, err := base64.StdEncoding.Strict().DecodeString(argv[13])
		if err != nil {
			t.Fatal(err)
		}
		reader, err := zlib.NewReader(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		raw, readErr := io.ReadAll(reader)
		if reader.Close() != nil || readErr != nil || !bytes.Equal(raw, EvidenceProgram()) || sha256.Sum256(raw) != w.ProgramSHA256 {
			t.Fatal("Go-compressed bytes differ from canonical source")
		}
		if len(sourceBootstrap) > 2048 || !sourceExecFits(w, argv) {
			t.Fatal("complete encoded command exceeds bound")
		}
		t.Logf("%s canonical=%d compressed=%d padded_base64=%d bootstrap=%d", kind, len(raw), len(compressed), len(argv[13]), len(sourceBootstrap))
	}
	for _, kind := range []string{"", "observe", "completed_evidence", "source_head;bad"} {
		if _, err := sourceExecArgv(kind, w); err == nil {
			t.Fatal("open role selector accepted")
		}
	}
	argv, err := sourceExecArgv(sourceHeadKind, w)
	if err != nil {
		t.Fatal(err)
	}
	argv[13] = strings.Repeat("+", nativeWireLimit)
	if sourceExecFits(w, argv) {
		t.Fatal("percent-encoded framing overflow accepted")
	}
	argv[13] = "\x00"
	if sourceExecFits(w, argv) {
		t.Fatal("NUL argument accepted")
	}
	bad := w
	bad.ProgramSHA256 = [32]byte{}
	if _, err := sourceExecArgv(sourceHeadKind, bad); err == nil {
		t.Fatal("changed source SHA accepted")
	}
	t.Run("loader", testSourceLoader)
	t.Run("canonical_private_module_refusal", testCanonicalSourceLoader)
}

func testCanonicalSourceLoader(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter absent")
	}
	argv, err := sourceExecArgv(sourceHeadKind, sourceWant())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, python, argv[6:]...)
	command.Env = []string{"LANG=C", "LC_ALL=C", "PGCONNECT_TIMEOUT=10"}
	command.Stdin = strings.NewReader("{}")
	var output, diagnostic bytes.Buffer
	command.Stdout, command.Stderr = &output, &diagnostic
	if err := command.Run(); err == nil || output.Len() != 0 || diagnostic.Len() != 0 {
		t.Fatal("canonical source loader did not quietly propagate refusal")
	}
}

func testSourceLoader(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter absent")
	}
	program := []byte("import os\ndef run_source_observation(kind, operation, digest):\n os.write(1,b'fixed-source-loader\\n')\n return 0\n")
	var compressed bytes.Buffer
	writer, err := zlib.NewWriterLevel(&compressed, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write(program); err != nil {
		t.Fatal(err)
	}
	if writer.Close() != nil {
		t.Fatal("compression close failed")
	}
	digest := sha256.Sum256(program)
	encoded := base64.StdEncoding.EncodeToString(compressed.Bytes())
	run := func(t *testing.T, role, hash, data string, accepted bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, python, "-I", "-B", "-c", sourceBootstrap, role, strings.Repeat("a", 32), hash, data)
		var out, diagnostic bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diagnostic
		err := cmd.Run()
		if accepted {
			if err != nil || out.String() != "fixed-source-loader\n" || diagnostic.Len() != 0 {
				t.Fatal("valid bounded source refused")
			}
			return
		}
		if err == nil || out.Len() != 0 || diagnostic.Len() != 0 {
			t.Fatal("loader failure emitted output, diagnostics or success")
		}
	}
	hash := hex.EncodeToString(digest[:])
	run(t, sourceHeadKind, hash, encoded, true)
	for name, payload := range map[string]string{
		sourceTruncatedCase: base64.StdEncoding.EncodeToString(compressed.Bytes()[:compressed.Len()-1]),
		sourceTrailingCase:  base64.StdEncoding.EncodeToString(append(bytes.Clone(compressed.Bytes()), 'x')),
		"concatenated":      base64.StdEncoding.EncodeToString(append(bytes.Clone(compressed.Bytes()), compressed.Bytes()...)),
		"base64":            "!!!!", "noncanonical": encoded + "=",
	} {
		t.Run(name, func(t *testing.T) { run(t, sourceHeadKind, hash, payload, false) })
	}
	run(t, "observe", hash, encoded, false)
	run(t, sourceHeadKind, strings.Repeat("0", 64), encoded, false)
	var bomb bytes.Buffer
	zw := zlib.NewWriter(&bomb)
	if _, err = zw.Write(bytes.Repeat([]byte{'x'}, EvidenceProgramSourceLimit+1)); err != nil {
		t.Fatal(err)
	}
	if zw.Close() != nil {
		t.Fatal("bomb compression close failed")
	}
	run(t, sourceHeadKind, hash, base64.StdEncoding.EncodeToString(bomb.Bytes()), false)
}
