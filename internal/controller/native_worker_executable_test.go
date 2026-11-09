//go:build native_worker_parity

package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	parityConnectionMessage = "native:Source: PostgreSQL connection failed; connection details redacted"
	parityIdentityMessage   = "native:BadConfig: invalid operator identity environment; supply all four canonical identity values"
	parityProductionDir     = "/work/pgcopydb"
	parityMigrationEnv      = "RPGMIGRATE_MIGRATION_UID"
	parityRunEnv            = "RPGMIGRATE_RUN_ID"
	parityWorkdirEnv        = "RPGMIGRATE_WORKDIR_ID"
	parityGenerationEnv     = "RPGMIGRATE_CHECKPOINT_GENERATION"
	parityCloneCommand      = "clone"
	parityBinaryCase        = "binary"
	parityJSONNull          = "null"
	paritySourceEnv         = "PGCOPYDB_SOURCE_PGURI"
	parityTargetEnv         = "PGCOPYDB_TARGET_PGURI"
	parityPVCUID            = "pvc-owned"
)

func TestNativeWorkerExecutableParity(t *testing.T) {
	binary := os.Getenv("RPGMIGRATE_TEST_BINARY")
	info, err := os.Stat(binary)
	if binary == "" || err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		t.Fatal("RPGMIGRATE_TEST_BINARY must name the actual executable")
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		t.Fatal("cannot resolve the actual executable")
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal("cannot read the actual executable")
	}
	t.Logf("engine binary SHA-256 %x", sha256.Sum256(data))
	for _, follow := range []bool{false, true} {
		name := "plain"
		if follow {
			name = "follow"
		}
		t.Run(name, func(t *testing.T) {
			m, pvc, private := parityWorkerInputs(t)
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: follow}
			state := filepath.Join(t.TempDir(), "native-state")
			first := parityWorkerContainer(t, m, pvc, 1)
			env := parityWorkerEnvironment(t, first)
			for attempt := int32(1); attempt <= 2; attempt++ {
				stage := "first"
				if attempt == 2 {
					stage = "retry construction"
				}
				t.Run(stage, func(t *testing.T) {
					container := parityWorkerContainer(t, m, pvc, attempt)
					if !reflect.DeepEqual(container.Env, first.Env) {
						t.Fatal("retry changed the identity or connection environment")
					}
					want := []string{parityCloneCommand, "--dir", parityProductionDir, "--table-jobs", "4", "--skip-large-objects"}
					if attempt == 1 {
						want = append(want, "--restart")
					} else {
						want = append(want, "--resume", "--not-consistent")
					}
					if follow {
						want = append(want, "--follow", "--plugin", "pgoutput")
					}
					if !slices.Equal(container.Args, want) {
						t.Fatal("narrow worker changed the complete approved argument vector")
					}
					parityInvoke(t, binary, parityRebindDir(t, container.Args, state), env, "Source", 6, parityConnectionMessage, private)
					parityAssertInit(t, state, follow)
				})
			}
		})
	}
	for _, name := range []string{"old zero and max", "nonzero split", parityBinaryCase, "operator defaults", "standalone max"} {
		t.Run(name, func(t *testing.T) {
			m, pvc, private := parityWorkerInputs(t)
			switch name {
			case "nonzero split":
				threshold := resource.MustParse("1Mi")
				m.Spec.Clone.SplitTablesLargerThan = &threshold
			case parityBinaryCase:
				m.Spec.Clone.UseCopyBinary = new(true)
			case "operator defaults":
				m.Spec.Clone = v1beta1.CloneOptions{}
			}
			container := parityWorkerContainer(t, m, pvc, 1)
			if name == "old zero and max" {
				container.Args = slices.Insert(container.Args, 5, "--split-tables-larger-than", "0", "--split-max-parts", "8")
			}
			if name == "standalone max" {
				container.Args = append(container.Args, "--split-max-parts", "8")
			}
			option := "split-max-parts"
			if name == parityBinaryCase {
				option = "use-copy-binary"
			}
			state := filepath.Join(t.TempDir(), "refused-state")
			message := "native:BadConfig: unimplemented option --" + option + "; no database or state action was started"
			parityInvoke(t, binary, parityRebindDir(t, container.Args, state), parityWorkerEnvironment(t, container), "BadConfig", 2, message, private)
			parityAssertAbsent(t, state)
		})
	}
	for _, name := range []string{"partial identity", "all zero identity"} {
		t.Run(name, func(t *testing.T) {
			m, pvc, private := parityWorkerInputs(t)
			container := parityWorkerContainer(t, m, pvc, 1)
			env := parityWorkerEnvironment(t, container)
			category, code, message := "BadConfig", 2, parityIdentityMessage
			if name == "partial identity" {
				env = slices.DeleteFunc(env, func(value string) bool { return strings.HasPrefix(value, "RPGMIGRATE_WORKDIR_ID=") })
			} else {
				for i, value := range env {
					if strings.HasPrefix(value, "RPGMIGRATE_MIGRATION_UID=") {
						env[i] = "RPGMIGRATE_MIGRATION_UID=00000000-0000-0000-0000-000000000000"
					}
					if strings.HasPrefix(value, "RPGMIGRATE_RUN_ID=") {
						env[i] = "RPGMIGRATE_RUN_ID=00000000000000000000000000000000"
					}
				}
				category, code, message = "BadState", 3, "native:BadState: state-operator: canonical operator identity is required"
			}
			state := filepath.Join(t.TempDir(), "identity-refused-state")
			parityInvoke(t, binary, parityRebindDir(t, container.Args, state), env, category, code, message, private)
			parityAssertAbsent(t, state)
		})
	}
}

func parityWorkerInputs(t *testing.T) (*v1beta1.Migration, *corev1.PersistentVolumeClaim, []string) {
	t.Helper()
	m, pvc := nativeIdentityObjects()
	no, zero := false, resource.MustParse("0")
	m.Spec.Clone.UseCopyBinary, m.Spec.Clone.SplitTablesLargerThan = &no, &zero
	m.Spec.Clone.Skip = []v1beta1.SkipOption{testSkipLargeObjects}
	missing := filepath.Join(t.TempDir(), "absent-socket")
	parityAssertAbsent(t, missing)
	connection := v1beta1.PostgresConnection{Host: missing, Database: "parity-db-canary", Username: "parity-user-canary", SSLMode: "disable"}
	m.Spec.Source, m.Spec.Target = connection, connection
	for _, side := range []struct {
		name  conn.Side
		value *v1beta1.PostgresConnection
	}{{conn.Source, &m.Spec.Source}, {conn.Target, &m.Spec.Target}} {
		uri, err := conn.ComposeURI(side.name, side.value)
		prefix := "postgresql://" + connection.Username + "@" + url.PathEscape(missing) + ":5432/" + connection.Database + "?"
		if err != nil || !strings.HasPrefix(uri, prefix) {
			t.Fatal("connection fixture is not an owned missing Unix socket")
		}
	}
	return m, pvc, []string{missing, connection.Database, connection.Username}
}

func parityWorkerContainer(t *testing.T, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, attempt int32) corev1.Container {
	t.Helper()
	job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatal("native worker must have one container")
	}
	container := job.Spec.Template.Spec.Containers[0]
	if len(container.Command) != 4 || container.Command[3] != nativeWorkerTestExecutable {
		t.Fatal("worker does not select the actual native executable")
	}
	return container
}

func parityWorkerEnvironment(t *testing.T, container corev1.Container) []string {
	t.Helper()
	env := make([]string, 0, 4+len(container.Env))
	env = append(env, "PATH=/usr/bin:/bin", "LC_ALL=C", "HOME="+t.TempDir(), "TMPDIR="+t.TempDir())
	seen := make(map[string]bool)
	for _, variable := range container.Env {
		if variable.ValueFrom != nil || seen[variable.Name] {
			t.Fatal("fixture requires unique directly materialized environment values")
		}
		seen[variable.Name] = true
		env = append(env, variable.Name+"="+variable.Value)
	}
	for key, want := range map[string]string{
		parityMigrationEnv: nativeWorkerTestMigrationUID,
		parityRunEnv:       nativeTestRunID,
		parityWorkdirEnv:   parityPVCUID, parityGenerationEnv: "1",
		"PGCOPYDB_LOG_JSON": "on",
	} {
		if !slices.Contains(env, key+"="+want) {
			t.Fatalf("worker environment lost exact %s", key)
		}
	}
	for _, key := range []string{paritySourceEnv, parityTargetEnv} {
		if !seen[key] {
			t.Fatal("worker lacks a connection environment value")
		}
	}
	return env
}

func parityRebindDir(t *testing.T, production []string, state string) []string {
	t.Helper()
	args := slices.Clone(production)
	count := 0
	for i, value := range production {
		if value == "--dir" {
			if i+1 >= len(production) || production[i+1] != parityProductionDir {
				t.Fatal("worker changed the production directory")
			}
			args[i+1] = state
			count++
		}
	}
	if count != 1 || args[0] != parityCloneCommand {
		t.Fatal("worker lacks the exact clone/workdir vector")
	}
	return args
}

func parityInvoke(t *testing.T, binary string, args, env []string, category string, code int, message string, private []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = env
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	var exited *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exited) || exited.ExitCode() != code {
		t.Fatalf("actual executable did not reach the expected %s/%d boundary", category, code)
	}
	if stdout.Len() != 0 {
		t.Fatal("failed worker emitted stdout or a completion report")
	}
	for _, value := range private {
		if strings.Contains(stderr.String(), value) {
			t.Fatal("worker disclosed private connection input")
		}
	}
	var diagnostic struct {
		Category string `json:"category"`
		ExitCode int    `json:"exit_code"`
		Message  string `json:"message"`
	}
	if json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &diagnostic) != nil || diagnostic.Category != category || diagnostic.ExitCode != code || diagnostic.Message != message {
		t.Fatalf("actual executable did not return the exact sanitized %s diagnostic", category)
	}
}

func parityAssertInit(t *testing.T, state string, follow bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(state, "manifest.json"))
	if err != nil {
		t.Fatal("initial process did not create reusable native INIT state")
	}
	var manifest struct {
		CloneState       json.RawMessage            `json:"clone_state"`
		SourceSlot       json.RawMessage            `json:"source_slot"`
		TargetProvenance []json.RawMessage          `json:"target_provenance"`
		FollowState      json.RawMessage            `json:"follow_state"`
		Operator         map[string]json.RawMessage `json:"operator"`
	}
	if json.Unmarshal(data, &manifest) != nil || string(manifest.CloneState) != parityJSONNull || string(manifest.SourceSlot) != parityJSONNull || len(manifest.TargetProvenance) != 0 || (string(manifest.FollowState) != parityJSONNull) != follow || len(manifest.Operator) == 0 {
		t.Fatal("missing-socket process advanced beyond native INIT or lost caller identity")
	}
	var uid, workdir string
	var generation int
	var run []byte
	if json.Unmarshal(manifest.Operator["migration_uid"], &uid) != nil || uid != nativeWorkerTestMigrationUID ||
		json.Unmarshal(manifest.Operator["workdir_id"], &workdir) != nil || workdir != parityPVCUID ||
		json.Unmarshal(manifest.Operator["checkpoint_generation"], &generation) != nil || generation != 1 ||
		json.Unmarshal(manifest.Operator["run"], &run) != nil || !bytes.Equal(run, []byte{1, 35, 69, 103, 137, 171, 205, 239, 1, 35, 69, 103, 137, 171, 205, 239}) {
		t.Fatal("native INIT state changed the exact worker identity")
	}
	entries, err := os.ReadDir(state)
	if err != nil || len(entries) != 1 || entries[0].Name() != "manifest.json" {
		t.Fatal("missing-socket process created unexpected state artifacts")
	}
}

func parityAssertAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused invocation created state or socket fixture already exists")
	}
}
