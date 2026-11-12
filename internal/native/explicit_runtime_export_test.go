package native

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// explicitArgvExportEnv names the file hack/native-explicit-file-qualify.py reads the rendered argv from.
const explicitArgvExportEnv = "RPGMIGRATE_EXPLICIT_ARGV_EXPORT"

// The approved caps are literals so a changed constant cannot move the runtime boundary with it.
const (
	explicitSourceCap  = 122880
	explicitMainCap    = 126976
	explicitPreludeCap = 65536
)

type explicitArgvCase struct {
	Name         string   `json:"name"`
	Command      []string `json:"command"`
	Args         []string `json:"args"`
	SourceBytes  int      `json:"source_bytes"`
	MainBytes    int      `json:"main_bytes_with_nul"`
	PreludeBytes int      `json:"prelude_bytes_with_nul"`
	Marker       string   `json:"marker"`
}

// explicitSyntheticProgram is exactly size bytes; its only output line is last, so a truncated heredoc prints nothing.
func explicitSyntheticProgram(t *testing.T, size int, marker string) []byte {
	t.Helper()
	head := "import json, os, sys\n"
	tail := "print(json.dumps({'marker': '" + marker + "', 'argv': [len(a.encode()) for a in sys.argv], 'env_bytes': sum(len(k) + len(v) + 2 for k, v in os.environb.items()), 'env_count': len(os.environb), 'pgpass': os.path.isfile('/tmp/pgpass')}))\n"
	pad := size - len(head) - len(tail)
	if pad < 2 {
		t.Fatal("synthetic program size too small")
	}
	var body strings.Builder
	for pad > 0 {
		line := min(pad, 4096)
		body.WriteString("#" + strings.Repeat("p", line-2) + "\n")
		pad -= line
		if pad == 1 {
			t.Fatal("synthetic padding left one byte")
		}
	}
	program := []byte(head + body.String() + tail)
	if len(program) != size {
		t.Fatal("synthetic program size mismatch")
	}
	return program
}

func explicitInlineMigration(t *testing.T) (*v1beta1.Migration, ExecutionRequest) {
	t.Helper()
	f := newAdmissionFixture(t)
	m := f.m.DeepCopy()
	for role, port := range map[string]int32{evidenceSource: 5418, evidenceTarget: 5416} {
		c := v1beta1.PostgresConnection{Host: "localhost", Port: port, Username: "pwuser", Database: "app", SSLMode: "verify-full", PasswordSecretRef: policySecret(role+"-password", "password"), TLS: &v1beta1.TLSSecretRefs{RootCA: policySecret("tls", "ca.crt"), Cert: policySecret("tls", "tls.crt"), Key: policySecret("tls", "tls.key")}}
		if role == evidenceSource {
			m.Spec.Source = c
		} else {
			m.Spec.Target = c
		}
	}
	w := f.want
	w.ProgramSHA256 = EvidenceProgramSHA256()
	return m, w
}

// TestNativeEvidenceArgvRuntimeExport renders the boundary argv the Linux qualification executes.
// The assertions always run; the file is written only when the qualification harness asks for it.
func TestNativeEvidenceArgvRuntimeExport(t *testing.T) {
	m, w := explicitInlineMigration(t)
	command, _, env, volumes, mounts, err := EvidenceMaterialization(m, w)
	if err != nil {
		t.Fatal(err)
	}
	prelude := command[2]
	paddedPrelude := "#" + strings.Repeat("p", explicitPreludeCap-1-len(prelude)-2) + "\n" + prelude
	if len(paddedPrelude)+1 != explicitPreludeCap {
		t.Fatal("prelude padding missed the cap")
	}

	real := EvidenceProgram()
	refusals := map[string]bool{}
	for _, size := range []int{explicitSourceCap, explicitSourceCap + 1} {
		padded := append(append([]byte{}, real...), []byte("#"+strings.Repeat("p", size-len(real)-2)+"\n")...)
		want := w
		want.ProgramSHA256 = sha256.Sum256(padded)
		_, args, err := renderEvidenceScripts(padded, want, prelude)
		if size == explicitSourceCap && (err != nil || !strings.Contains(args[1], string(padded))) {
			t.Fatal("binding source padded to the cap was refused")
		}
		if size > explicitSourceCap {
			refusals["binding_source_122881"] = err != nil
		}
	}

	var cases []explicitArgvCase
	render := func(name string, source []byte, marker, prelude string, mainBytes int) {
		want := w
		want.ProgramSHA256 = sha256.Sum256(source)
		command, args, err := renderEvidenceScripts(source, want, prelude)
		if err == nil && mainBytes > 0 {
			want.OperationID += strings.Repeat("a", mainBytes-len(args[1])-1)
			command, args, err = renderEvidenceScripts(source, want, prelude)
		}
		if err != nil {
			refusals[name] = true
			return
		}
		cases = append(cases, explicitArgvCase{Name: name, Command: command, Args: args, SourceBytes: len(source), MainBytes: len(args[1]) + 1, PreludeBytes: len(command[2]) + 1, Marker: marker})
	}
	for _, size := range []int{explicitSourceCap - 1, explicitSourceCap} {
		render("source_"+strconv.Itoa(size), explicitSyntheticProgram(t, size, "source-"+strconv.Itoa(size)), "source-"+strconv.Itoa(size), prelude, 0)
	}
	render("source_122881", explicitSyntheticProgram(t, explicitSourceCap+1, "oversize"), "oversize", prelude, 0)
	atCap := explicitSyntheticProgram(t, explicitSourceCap, "both-caps")
	render("main_126976_prelude_65536", atCap, "both-caps", paddedPrelude, explicitMainCap)
	render("main_126977", atCap, "oversize", prelude, explicitMainCap+1)
	render("prelude_65537", atCap, "oversize", ":"+paddedPrelude, 0)

	if len(cases) != 3 || !refusals["source_122881"] || !refusals["main_126977"] || !refusals["prelude_65537"] || !refusals["binding_source_122881"] {
		t.Fatalf("boundary inventory wrong: %d accepted, refusals %v", len(cases), refusals)
	}
	if cases[2].MainBytes != explicitMainCap || cases[2].PreludeBytes != explicitPreludeCap || cases[1].SourceBytes != explicitSourceCap {
		t.Fatal("accepted boundary case missed its cap")
	}

	out := os.Getenv(explicitArgvExportEnv)
	if out == "" {
		return
	}
	sum := EvidenceProgramSHA256()
	raw, err := json.MarshalIndent(map[string]any{
		preflightProgramKey: hex.EncodeToString(sum[:]), "program_bytes": len(real),
		"limits":      map[string]int{"source_bytes": EvidenceProgramSourceLimit, "main_bytes_with_nul": EvidenceMainArgumentLimit, "prelude_bytes_with_nul": EvidencePreludeArgumentLimit, "job_json_bytes": EvidenceJobJSONLimit},
		"cases":       cases,
		"refusals":    refusals,
		"integration": map[string]any{"command": command, "env": env, "volumes": volumes, "mounts": mounts},
	}, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
