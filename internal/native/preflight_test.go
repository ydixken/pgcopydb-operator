package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	preflightOperation       = "0123456789abcdef0123456789abcdef"
	preflightSourceMajorKey  = "source_major"
	preflightSourceClosedKey = "source_closed"
	preflightDiagnosticsKey  = "diagnostics_clean"
	preflightChecksKey       = "checks"
	preflightRemediation     = "forbidden-remediation"
	preflightTargetMajorKey  = "target_major"
	preflightProgramKey      = "program_sha256"
	preflightDumpMajorKey    = "pg_dump_major"
	preflightTargetClosedKey = "target_closed"
	preflightRestoreMajorKey = "pg_restore_major"
)

var preflightCloneChecks = []string{"connections", "identity", "source_primary", "target_writable", "versions", "tools", "temporary_privilege", "extensions", "clone_rights", "rls"}
var preflightFollowChecks = []string{"wal", "publication", "slots", "replication", "replica_role", "unlogged", "replica_identity"}

func preflightPayload(t *testing.T, follow bool) map[string]any {
	t.Helper()
	hash := EvidenceProgramSHA256()
	checks := slices.Clone(preflightCloneChecks)
	if follow {
		checks = append(checks, preflightFollowChecks...)
	}
	return map[string]any{fixtureVersionKind: 1, readerKindKey: "native_preflight", readerOperationIDKey: preflightOperation, preflightProgramKey: hex.EncodeToString(hash[:]), preflightSourceMajorKey: 16, preflightTargetMajorKey: 18, preflightDumpMajorKey: 18, preflightRestoreMajorKey: 18, preflightChecksKey: checks, preflightSourceClosedKey: true, preflightTargetClosedKey: true, "materialization_closed": true, preflightDiagnosticsKey: true}
}

func preflightJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func TestParsePreflightResult(t *testing.T) {
	if (PreflightResult{}).Passed() {
		t.Fatal("zero result passed")
	}
	for _, follow := range []bool{false, true} {
		for _, pair := range [][2]int{{16, 16}, {18, 18}, {16, 18}} {
			payload := preflightPayload(t, follow)
			payload[preflightSourceMajorKey], payload[preflightTargetMajorKey] = pair[0], pair[1]
			payload[preflightDumpMajorKey], payload[preflightRestoreMajorKey] = pair[1], pair[1]
			result, err := ParsePreflightResult(preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), follow)
			if err != nil || result == nil || !result.Passed() {
				t.Fatal("valid closed fixed preflight refused")
			}
		}
	}
}

func TestParsePreflightResultRefusals(t *testing.T) {
	good := preflightPayload(t, false)
	for key := range good {
		t.Run("missing_"+key, func(t *testing.T) {
			payload := preflightPayload(t, false)
			delete(payload, key)
			refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), false)
		})
		for _, value := range []any{nil, map[string]any{}, []int{1}} {
			payload := preflightPayload(t, false)
			payload[key] = value
			refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), false)
		}
	}
	for _, tc := range []struct {
		key   string
		value any
	}{
		{fixtureVersionKind, 2}, {fixtureVersionKind, true}, {fixtureVersionKind, "1"}, {readerKindKey, "completed_evidence"}, {readerOperationIDKey, strings.Repeat("a", 32)},
		{preflightProgramKey, strings.Repeat("b", 64)}, {preflightProgramKey, strings.Repeat("A", 64)},
		{preflightSourceMajorKey, 17}, {preflightSourceMajorKey, true}, {preflightTargetMajorKey, 17}, {preflightTargetMajorKey, "18"},
		{preflightDumpMajorKey, 16}, {preflightRestoreMajorKey, 16}, {preflightDumpMajorKey, true},
		{preflightSourceClosedKey, false}, {preflightTargetClosedKey, false}, {"materialization_closed", false}, {preflightDiagnosticsKey, false}, {preflightDiagnosticsKey, 1},
		{preflightChecksKey, []string{}}, {preflightChecksKey, append(slices.Clone(preflightCloneChecks), "extra")}, {preflightChecksKey, preflightCloneChecks[:9]},
		{preflightChecksKey, append(slices.Clone(preflightCloneChecks), preflightFollowChecks...)}, {readerCaseUnknown, true},
	} {
		payload := preflightPayload(t, false)
		payload[tc.key] = tc.value
		refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), false)
	}
	for i := range preflightCloneChecks {
		payload := preflightPayload(t, false)
		checks := slices.Clone(preflightCloneChecks)
		checks[i] = evidenceOther
		payload[preflightChecksKey] = checks
		refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), false)
	}
	for i := range preflightFollowChecks {
		payload := preflightPayload(t, true)
		checks := payload[preflightChecksKey].([]string)
		checks[len(preflightCloneChecks)+i] = evidenceOther
		refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), true)
	}
	payload := preflightPayload(t, false)
	reversed := slices.Clone(preflightCloneChecks)
	slices.Reverse(reversed)
	payload[preflightChecksKey] = reversed
	refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), false)
	payload = preflightPayload(t, false)
	payload[preflightSourceMajorKey], payload[preflightTargetMajorKey], payload[preflightDumpMajorKey], payload[preflightRestoreMajorKey] = 18, 16, 16, 16
	refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), false)
	payload = preflightPayload(t, false)
	refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), true)
	refusePreflight(t, preflightJSON(t, payload), strings.ToUpper(preflightOperation), EvidenceProgramSHA256(), false)
	hash := EvidenceProgramSHA256()
	hash[0] ^= 1
	refusePreflight(t, preflightJSON(t, payload), preflightOperation, hash, false)
}

func refusePreflight(t *testing.T, raw []byte, operation string, hash [32]byte, follow bool) {
	t.Helper()
	result, err := ParsePreflightResult(raw, operation, hash, follow)
	if err == nil || result != nil {
		t.Fatal("unproved preflight accepted")
	}
	if strings.Contains(err.Error(), "credential-canary") {
		t.Fatal("raw output escaped in refusal")
	}
}

func TestParsePreflightResultFraming(t *testing.T) {
	raw := preflightJSON(t, preflightPayload(t, false))
	for _, invalid := range [][]byte{
		nil, {}, raw[:len(raw)-1], append(slices.Clone(raw), '\n'), append(slices.Clone(raw), raw...), append([]byte("log\n"), raw...),
		append([]byte{0xef, 0xbb, 0xbf}, raw...), append([]byte{0xff}, raw...), []byte("credential-canary\n"),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"\u0076ersion":1`), 1),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1.0`), 1),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1e0`), 1),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":-0`), 1),
		bytes.Replace(raw, []byte(`"checks":[`), []byte("\"checks\":\n["), 1),
		append(bytes.Repeat([]byte(" "), nativeWireLimit), raw...),
	} {
		refusePreflight(t, invalid, preflightOperation, EvidenceProgramSHA256(), false)
	}
	atLimit := append(bytes.Repeat([]byte(" "), nativeWireLimit-len(raw)), raw...)
	if result, err := ParsePreflightResult(atLimit, preflightOperation, EvidenceProgramSHA256(), false); err != nil || !result.Passed() {
		t.Fatal("wire cap is not inclusive")
	}
	refusePreflight(t, append([]byte(" "), atLimit...), preflightOperation, EvidenceProgramSHA256(), false)
}

func preflightMigration() *v1beta1.Migration {
	return &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{UID: "migration-uid"}, Spec: v1beta1.MigrationSpec{Source: v1beta1.PostgresConnection{URISecretRef: policySecret("source-uri", "uri")}, Target: v1beta1.PostgresConnection{URISecretRef: policySecret("target-uri", "uri")}}}
}

func TestNativePreflightMaterialization(t *testing.T) {
	forms := []struct {
		name string
		make func(string) v1beta1.PostgresConnection
	}{
		{"inline", func(role string) v1beta1.PostgresConnection {
			return v1beta1.PostgresConnection{Host: "preflight.invalid", Username: role, Database: "db", PasswordSecretRef: policySecret(role+"-password", "pw"), TLS: &v1beta1.TLSSecretRefs{RootCA: policySecret("tls", "ca"), Cert: policySecret("tls", "cert"), Key: policySecret("tls", "key")}}
		}},
		{"secret", func(role string) v1beta1.PostgresConnection {
			return v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: role + "-connection"}}
		}},
		{"opaque", func(role string) v1beta1.PostgresConnection {
			return v1beta1.PostgresConnection{URISecretRef: policySecret(role+"-uri", "uri")}
		}},
	}
	for _, source := range forms {
		for _, target := range forms {
			for _, follow := range []bool{false, true} {
				t.Run(source.name+"_"+target.name+"_"+strconv.FormatBool(follow), func(t *testing.T) {
					m := preflightMigration()
					m.Spec.Source = source.make(evidenceSource)
					m.Spec.Target = target.make(evidenceTarget)
					m.Spec.Follow = &v1beta1.FollowOptions{Enabled: follow}
					m.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: preflightRemediation}
					m.Spec.Target.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: preflightRemediation}
					command, args, env, volumes, mounts, err := PreflightMaterialization(m, preflightOperation)
					if err != nil {
						t.Fatal(err)
					}
					src, _ := conn.Materialize(conn.Source, &m.Spec.Source)
					tgt, _ := conn.Materialize(conn.Target, &m.Spec.Target)
					wantEnv := append(slices.Clone(src.Env), tgt.Env...)
					if !reflect.DeepEqual(env[:len(wantEnv)], wantEnv) {
						t.Fatal("primary connection environment changed")
					}
					wantVolumes := append([]corev1.Volume{{Name: nativePreflightTmp, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: resource.NewQuantity(64*1024*1024, resource.BinarySI)}}}}, src.Volumes...)
					wantVolumes = append(wantVolumes, tgt.Volumes...)
					wantMounts := append([]corev1.VolumeMount{{Name: nativePreflightTmp, MountPath: "/tmp"}}, src.Mounts...)
					wantMounts = append(wantMounts, tgt.Mounts...)
					if !reflect.DeepEqual(volumes, wantVolumes) || !reflect.DeepEqual(mounts, wantMounts) {
						t.Fatal("unexpected work or remediation projection")
					}
					fixed := map[string]string{nativePreflightLocale: "C", nativePreflightLang: "C", nativePreflightTimeoutEnv: "10", nativePreflightFollowEnv: "0"}
					if follow {
						fixed[nativePreflightFollowEnv] = "1"
					}
					for _, e := range env[len(wantEnv):] {
						if e.Name == nativePreflightPassfileEnv {
							if e.Value != conn.PgpassPath {
								t.Fatal("wrong private passfile")
							}
							continue
						}
						if value, ok := fixed[e.Name]; !ok || value != e.Value || e.ValueFrom != nil {
							t.Fatal("unexpected role environment")
						}
						delete(fixed, e.Name)
					}
					if len(fixed) != 0 {
						t.Fatal("missing fixed environment")
					}
					if len(command) != 4 || command[0] != "/bin/sh" || command[1] != "-c" || command[3] != "/bin/sh" || len(args) != 2 || args[0] != "-c" {
						t.Fatal("private interpreter launcher changed")
					}
					hash := EvidenceProgramSHA256()
					if !strings.Contains(args[1], "/usr/bin/python3 -I -B - native_preflight "+preflightOperation+" "+hex.EncodeToString(hash[:])) || !strings.Contains(args[1], string(EvidenceProgram())) {
						t.Fatal("role or canonical source binding missing")
					}
					for _, forbidden := range []string{preflightRemediation, "postgresql://source:", "postgresql://target:"} {
						if strings.Contains(strings.Join(append(slices.Clone(command), args...), " "), forbidden) {
							t.Fatal("connection or work identity appeared in launcher")
						}
					}
				})
			}
		}
	}
}

func TestNativePreflightMaterializationRefusals(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(**v1beta1.Migration)
		operation string
	}{
		{"nil", func(m **v1beta1.Migration) { *m = nil }, preflightOperation},
		{"uid", func(m **v1beta1.Migration) { (*m).UID = "" }, preflightOperation},
		{admissionCaseOperation, func(_ **v1beta1.Migration) {}, "a';credential-canary"},
		{evidenceSource, func(m **v1beta1.Migration) { (*m).Spec.Source = v1beta1.PostgresConnection{} }, preflightOperation},
		{evidenceTarget, func(m **v1beta1.Migration) { (*m).Spec.Target = v1beta1.PostgresConnection{} }, preflightOperation},
		{"source_sync", func(m **v1beta1.Migration) { (*m).Spec.Source.SynchronousCommit = "local" }, preflightOperation},
		{"target_sync_opaque", func(m **v1beta1.Migration) { (*m).Spec.Target.SynchronousCommit = "local" }, preflightOperation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := preflightMigration()
			tc.mutate(&m)
			command, args, env, volumes, mounts, err := PreflightMaterialization(m, tc.operation)
			if err == nil || command != nil || args != nil || env != nil || volumes != nil || mounts != nil {
				t.Fatal("invalid input materialized")
			}
			if strings.Contains(err.Error(), "credential-canary") {
				t.Fatal("unsafe input echoed")
			}
		})
	}
}

func TestNativePreflightScriptBounds(t *testing.T) {
	source := []byte("pass\n")
	for _, invalid := range [][]byte{nil, []byte("pass"), []byte("pass\x00\n"), []byte("RPGMIGRATE_NATIVE_EVIDENCE_PY\n"), bytes.Repeat([]byte("\n"), EvidenceProgramSourceLimit+1)} {
		if _, _, err := renderPreflightScripts(invalid, preflightOperation, sha256.Sum256(invalid), ""); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
	if _, _, err := renderPreflightScripts(source, preflightOperation, [32]byte{}, ""); err == nil {
		t.Fatal("changed source accepted")
	}
	for _, prelude := range []string{strings.Repeat("x", EvidencePreludeArgumentLimit), "\x00"} {
		if _, _, err := renderPreflightScripts(source, preflightOperation, sha256.Sum256(source), prelude); err == nil {
			t.Fatal("invalid prelude accepted")
		}
	}
	if _, _, err := renderPreflightScripts(source, preflightOperation, sha256.Sum256(source), strings.Repeat("x", EvidencePreludeArgumentLimit-1)); err != nil {
		t.Fatal("inclusive prelude limit refused")
	}
	source = bytes.Repeat([]byte("\n"), EvidenceProgramSourceLimit)
	if _, args, err := renderPreflightScripts(source, preflightOperation, sha256.Sum256(source), ""); err != nil || len(args[1])+1 > EvidenceMainArgumentLimit {
		t.Fatal("approved source cap refused or complete argv cap exceeded")
	}
}

func TestNativePreflightProgramRequired(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	tests, err := os.ReadFile("evidence_program_test.py")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "evidence_program_test.py")
	if err := os.WriteFile(path, tests, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path, "NativePythonTests.test_NativePreflightDispatch").CombinedOutput()
	if err == nil || !bytes.Contains(output, []byte("reviewed evidence program is absent")) || !bytes.Contains(output, []byte("Ran 1 test")) || bytes.Contains(output, []byte("\nOK\n")) {
		t.Fatal("missing private program did not fail its required nonempty gate")
	}
}

func TestNativePreflightMaterializationIgnoresWork(t *testing.T) {
	m := preflightMigration()
	command, args, env, volumes, mounts, err := PreflightMaterialization(m, preflightOperation)
	if err != nil {
		t.Fatal(err)
	}
	m.Name = "foreign-work"
	m.Generation = 99
	storageClass := "unconsumed-storage"
	m.Spec.WorkVolume.StorageClassName = &storageClass
	m.Status.JobName = "unconsumed-worker"
	m.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "unconsumed-remediation"}
	m.Spec.Target.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "unconsumed-remediation"}
	gotCommand, gotArgs, gotEnv, gotVolumes, gotMounts, err := PreflightMaterialization(m, preflightOperation)
	if err != nil || !reflect.DeepEqual(command, gotCommand) || !reflect.DeepEqual(args, gotArgs) || !reflect.DeepEqual(env, gotEnv) || !reflect.DeepEqual(volumes, gotVolumes) || !reflect.DeepEqual(mounts, gotMounts) {
		t.Fatal("work or remediation inputs affected fixed preflight materialization")
	}
}

func TestParsePreflightProgramBinding(t *testing.T) {
	stale := EvidenceProgramSHA256()
	stale[0] ^= 1
	for _, hash := range [][32]byte{{}, stale} {
		payload := preflightPayload(t, false)
		payload[preflightProgramKey] = hex.EncodeToString(hash[:])
		refusePreflight(t, preflightJSON(t, payload), preflightOperation, hash, false)
	}
}

func TestNativePreflightTemporaryStorage(t *testing.T) {
	_, _, _, volumes, mounts, err := PreflightMaterialization(preflightMigration(), preflightOperation)
	if err != nil {
		t.Fatal(err)
	}
	if len(volumes) != 1 || volumes[0].EmptyDir == nil {
		t.Fatal("temporary storage is not an isolated emptyDir")
	}
	tmp := volumes[0].EmptyDir
	if tmp.Medium != corev1.StorageMediumMemory || tmp.SizeLimit == nil || tmp.SizeLimit.Value() != 67108864 {
		t.Fatal("credential temporary storage is not bounded 64 MiB tmpfs")
	}
	if len(mounts) != 1 || mounts[0].Name != volumes[0].Name || mounts[0].MountPath != "/tmp" {
		t.Fatal("temporary storage mount changed")
	}
}

func TestPreflightQualifiedMajorPairs(t *testing.T) {
	for _, pair := range []struct {
		source, target int
		admitted       bool
	}{{16, 16, true}, {18, 18, true}, {16, 18, true}, {18, 16, false}, {17, 17, false}, {16, 17, false}, {17, 18, false}, {15, 16, false}, {19, 19, false}} {
		t.Run(fmt.Sprintf("%d:%d", pair.source, pair.target), func(t *testing.T) {
			payload := preflightPayload(t, false)
			payload[preflightSourceMajorKey], payload[preflightTargetMajorKey] = pair.source, pair.target
			payload[preflightDumpMajorKey], payload[preflightRestoreMajorKey] = pair.target, pair.target
			if !pair.admitted {
				refusePreflight(t, preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), false)
				return
			}
			if result, err := ParsePreflightResult(preflightJSON(t, payload), preflightOperation, EvidenceProgramSHA256(), false); err != nil || !result.Passed() {
				t.Fatalf("qualified pair refused: %v", err)
			}
		})
	}
}

func TestPreflightFailureClass(t *testing.T) {
	line := func(class string) string {
		return `{"version":1,"kind":"native_preflight_failure","error_class":"` + class + `"}`
	}
	if strings.Join(preflightFailureClasses, " ") != "admission capability input timeout connection diagnostic result transaction_state unknown_commit closure internal" {
		t.Fatal("closed refusal set differs from the program's ERROR_CLASSES")
	}
	for _, class := range preflightFailureClasses {
		for _, message := range []string{line(class), line(class) + "\n"} {
			if got, ok := PreflightFailureClass(message); !ok || got != class {
				t.Fatalf("closed-set refusal %q was not named", message)
			}
		}
	}
	for _, message := range []string{"", "\n", line("secret"), line("transaction_state") + "\n\n", " " + line("result"), line("result") + " ", "partial\n" + line("result"), line("result") + "\n" + line("result"),
		`{"version":1,"kind":"native_preflight_failure","error_class":"result","sqlstate":"42501"}`, `{"kind":"native_preflight_failure","version":1,"error_class":"result"}`, "transaction_state"} {
		if got, ok := PreflightFailureClass(message); ok || got != "" {
			t.Fatalf("free-form termination message %q named a class", message)
		}
	}
}
