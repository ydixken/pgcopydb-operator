/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestKindSmallEffectiveProfile(t *testing.T) {
	if os.Getenv("E2E_TEST_CHILD") == "kind-small" {
		if !smallProfile || scale != 0.01 || cnpgInstances != 1 {
			t.Fatalf("small profile = %t/%g/%d", smallProfile, scale, cnpgInstances)
		}
		fixture, seed, runner := suiteRequests(1)
		if fixture != (requests{kindSmallCPU, parallelFixtureMemory}) ||
			seed != (requests{parallelSeedCPU, parallelSeedMemory}) || runner != (requests{kindSmallCPU, parallelRunnerMemory}) {
			t.Fatalf("requests = %v/%v/%v", fixture, seed, runner)
		}
		wantCaches := pgCaches{kindSmallSharedBuffers, kindSmallEffectiveCache, kindSmallMaintenance, kindSmallWalBuffers}
		if got := suiteCaches(1); got != wantCaches {
			t.Fatalf("caches = %v", got)
		}
		c := cnpgCluster(sourceCluster, srcStorageSize, 17)
		got, _, _ := unstructured.NestedStringMap(c.Object, "spec", "resources", "requests")
		if got["cpu"] != "500m" || got["memory"] != parallelFixtureMemory {
			t.Fatalf("rendered requests = %v", got)
		}
		params, _, _ := unstructured.NestedStringMap(c.Object, "spec", "postgresql", "parameters")
		for key, want := range map[string]string{"shared_buffers": "128MB", "effective_cache_size": "384MB",
			"maintenance_work_mem": kindSmallMaintenance, "wal_buffers": "16MB"} {
			if params[key] != want {
				t.Fatalf("rendered cache %s=%s, want %s", key, params[key], want)
			}
		}
		if srcStorageSize != "7Gi" || tgtStorageSize != "7Gi" || workVolumeSize != "2Gi" {
			t.Fatalf("volume floors changed: %s/%s/%s", srcStorageSize, tgtStorageSize, workVolumeSize)
		}
		s := buildSeedJob().Spec.Template.Spec.Containers[0]
		if s.Resources.Requests.Cpu().String() != parallelSeedCPU ||
			s.Resources.Requests.Memory().String() != parallelSeedMemory {
			t.Fatalf("seed requests = %v", s.Resources.Requests)
		}
		gotRunner := newMigration("size", nsE2E, v1beta1.CloneOptions{}).Spec.Runner.Resources.Requests
		if gotRunner.Cpu().MilliValue() != 500 || gotRunner.Memory().String() != parallelRunnerMemory {
			t.Fatalf("runner requests = %v", gotRunner)
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestKindSmallEffectiveProfile$")
	cmd.Env = []string{"E2E_TEST_CHILD=kind-small", "E2E_SMALL=true"}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("small child: %v\n%s", err, out)
	}
}

func TestKindSmallConflictingOverrides(t *testing.T) {
	if os.Getenv("E2E_TEST_CHILD") == "kind-conflict" {
		return
	}
	for _, env := range []string{
		"E2E_SCALE=0.1", "E2E_STRESS=true", "E2E_CNPG_INSTANCES=2",
		"E2E_EXTRA_TABLES=2", "E2E_KEEP_FIXTURES=true",
	} {
		t.Run(env, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestKindSmallConflictingOverrides$")
			cmd.Env = []string{"E2E_TEST_CHILD=kind-conflict", "E2E_SMALL=true", env}
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "E2E_SMALL conflicts") {
				t.Fatalf("conflict accepted or wrong refusal: %v\n%s", err, out)
			}
		})
	}
}

func TestKindImageSelection(t *testing.T) {
	const runner = "example.invalid/runner:test@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	t.Setenv("E2E_RUNNER_IMAGE", runner)
	oldSeed := seedImage
	seedImage = runner
	t.Cleanup(func() { seedImage = oldSeed })
	t.Setenv("E2E_TOOL_IMAGE", runner)
	collector := "example.invalid/collector@sha256:" + strings.Repeat("c", 64)
	postgres := "example.invalid/postgres:17@sha256:" + strings.Repeat("d", 64)
	t.Setenv("E2E_COLLECTOR_IMAGE", collector)
	t.Setenv("E2E_POSTGRES_17_IMAGE", postgres)
	if got := buildSeedJob().Spec.Template.Spec.Containers[0].Image; got != runner {
		t.Fatalf("seed image = %s", got)
	}
	if got := buildExternalClientPod().Spec.Containers[0].Image; got != runner {
		t.Fatalf("client image = %s", got)
	}
	cluster := cnpgCluster(sourceCluster, "7Gi", 17)
	if got, _, _ := unstructured.NestedString(cluster.Object, "spec", "imageName"); got != postgres {
		t.Fatalf("postgres image = %s", got)
	}
	_, dep, _ := otelCollectorObjects()
	if dep.Spec.Template.Spec.Containers[0].Image != collector {
		t.Fatal("collector override ignored")
	}
	if selectedRunnerImage() != runner {
		t.Fatal("runner override ignored")
	}
	scheme := k8sruntime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := clientfake.NewClientBuilder().WithScheme(scheme).Build()
	cl := featureLabelingClient{Client: base}
	t.Setenv("E2E_ENGINE", string(v1beta1.MigrationEnginePGCopyDB))
	m := newMigration("kind-image", nsE2E, v1beta1.CloneOptions{})
	if err := cl.Create(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	gotMigration := &v1beta1.Migration{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(m), gotMigration); err != nil {
		t.Fatal(err)
	}
	if gotMigration.Spec.Runner.Image != runner || gotMigration.Spec.Engine != v1beta1.MigrationEnginePGCopyDB {
		t.Fatal("effective Migration did not retain the full runner image and explicit legacy engine")
	}

}

func validateSmallProfile(getenv func(string) string) error {
	for name, want := range map[string]string{
		"E2E_SCALE": "0.01", "E2E_CNPG_INSTANCES": "1", "E2E_STRESS": strconv.FormatBool(false),
		"E2E_KEEP_FIXTURES": strconv.FormatBool(false), "E2E_EXTRA_TABLES": "0", "E2E_EXTRA_SIZE_GB": "0",
	} {
		if value := getenv(name); value != "" && value != want {
			return fmt.Errorf("E2E_SMALL conflicts with %s", name)
		}
	}
	return nil
}

func selectedRunnerImage() string {
	return envDefault("E2E_RUNNER_IMAGE", "ghcr.io/ydixken/pgcopydb-operator/runner:"+runnerTag)
}
func selectedToolImage() string { return envDefault("E2E_TOOL_IMAGE", selectedRunnerImage()) }
func selectedPostgresImage(major int) string {
	return envDefault(fmt.Sprintf("E2E_POSTGRES_%d_IMAGE", major),
		fmt.Sprintf("ghcr.io/cloudnative-pg/postgresql:%d", major))
}

// nativePrerequisite names the gate whose landing lifts the native refusal.
const nativePrerequisite = "M6-ACTIVATE (runner-profile qualification and the lifted UnsupportedEngine)"

func suiteEngine() (v1beta1.MigrationEngine, error) {
	switch value := envDefault("E2E_ENGINE", string(v1beta1.MigrationEnginePGCopyDB)); v1beta1.MigrationEngine(value) {
	case v1beta1.MigrationEnginePGCopyDB:
		return v1beta1.MigrationEnginePGCopyDB, nil
	case v1beta1.MigrationEngineRPGMigrate:
		return "", errors.New("native kind qualification is refused: prerequisite " + nativePrerequisite +
			" has not landed, so no qualified native runner is available")
	default:
		return "", errors.New("E2E_ENGINE must be pgcopydb or rpgmigrate")
	}
}

func kindFixtureLockImages(images map[string]kindImage, source map[string]string) map[string]any {
	resolvedImages := map[string]any{}
	for _, role := range kindLockRoles {
		image, ok := images[role]
		if !ok {
			image = images[kindManagerContainer]
		}
		resolved := map[string]any{kindBuildRoleType: role, kindRefField: image.Ref, kindDigestField: image.Digest,
			kindConfigDigestField: image.ImageID,
			kindArchitectureField: image.Architecture, "os": kindLinux, kindVariantField: nil,
			"index_digest": image.RegistryIndexDigest,
			"provenance": map[string]string{"origin_ref": image.Ref, "registry_receipt_sha256": strings.Repeat("a", 64),
				"public_receipt_sha256": strings.Repeat("b", 64)}}
		if role == kindManagerContainer || role == kindNativeRunner {
			resolved["source"] = source
		}
		resolvedImages[role] = resolved
	}
	return resolvedImages
}

func mutateKindLockShape(which string, lockFields, lockImages map[string]any) {
	switch which {
	case "unknown lock field":
		lockFields["unknown"] = true
	case "missing lock field":
		delete(lockFields, "engine_source")
	case "unknown lock role":
		lockImages["unowned-role"] = lockImages["node"]
	case "missing lock role":
		delete(lockImages, "node")
	case "unknown selected field":
		lockImages[kindManagerContainer].(map[string]any)["unknown"] = true
	}
}

func mutateKindLockFixture(t *testing.T, which, dir string, input *kindSuiteInput, env map[string]string) {
	t.Helper()
	switch which {
	case "wrong lock hash":
		input.ImageLockSHA256 = strings.Repeat("f", 64)
	case "wrong lock source":
		input.SourceTreeSHA256 = strings.Repeat("f", 64)
	case "wrong lock image":
		image := input.Images[kindManagerContainer]
		image.Ref = strings.Replace(image.Ref, "example.invalid/", "other.invalid/", 1)
		for node, binding := range image.NodeBindings {
			binding.RequestedRef = image.Ref
			for i, id := range binding.AcceptedImageIDs {
				binding.AcceptedImageIDs[i] = strings.Replace(id, "example.invalid/", "other.invalid/", 1)
			}
			slices.Sort(binding.AcceptedImageIDs)
			image.NodeBindings[node] = binding
		}
		input.Images[kindManagerContainer] = image
		env["E2E_MANAGER_IMAGE"] = image.Ref
	case "missing lock":
		if err := os.Remove(filepath.Join(dir, "image-lock.json")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKindPrivateConfigRefusals(t *testing.T) {
	for _, which := range []string{
		"accepted private config", "host kubeconfig", "wrong context", "extra context",
		"symlink", "public mode", "wrong image", "missing role", "unprefixed cluster",
		"wrong lock hash", "wrong lock source", "wrong lock image", "missing lock", "oversized input",
		"unknown lock field", "missing lock field", "unknown lock role", "missing lock role", "unknown selected field",
	} {
		t.Run(which, func(t *testing.T) {
			dir := t.TempDir()
			dir, _ = filepath.EvalSymlinks(dir)
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			runID := strings.Repeat("a", 32)
			context := "kind-rpgmigrate-m7-" + runID
			configPath := filepath.Join(dir, "kubeconfig")
			cfg := `apiVersion: v1
kind: Config
current-context: ` + context + `
contexts:
- name: ` + context + `
  context: {cluster: ` + context + `, user: ` + context + `}
clusters:
- name: ` + context + `
  cluster: {server: https://127.0.0.1:12345, insecure-skip-tls-verify: true}
users:
- name: ` + context + `
  user: {token: fixture-token}
`
			if which == "unprefixed cluster" {
				cfg = strings.ReplaceAll(cfg, "cluster: "+context, "cluster: rpgmigrate-m7-"+runID)
				cfg = strings.Replace(cfg, "clusters:\n- name: "+context, "clusters:\n- name: rpgmigrate-m7-"+runID, 1)
			}
			if which == "extra context" {
				cfg = strings.Replace(cfg, "clusters:\n",
					"- name: foreign\n  context: {cluster: foreign, user: foreign}\nclusters:\n", 1)
			}
			if err := os.WriteFile(configPath, []byte(cfg), 0600); err != nil {
				t.Fatal(err)
			}
			images := map[string]kindImage{}
			env := map[string]string{
				"E2E_KIND_RUN_DIR": dir, "E2E_KIND_CONTEXT": context, "E2E_CONTEXT": context, "KUBECONFIG": configPath,
				"E2E_ENGINE": string(v1beta1.MigrationEnginePGCopyDB), "E2E_SMALL": strconv.FormatBool(true),
			}
			for role, key := range kindImageEnvironment {
				ref := "example.invalid/" + role + ":test@sha256:" + strings.Repeat("b", 64)
				images[role] = kindImage{
					Ref: ref, Digest: "sha256:" + strings.Repeat("b", 64),
					Architecture: runtime.GOARCH, ImageID: "sha256:" + strings.Repeat("c", 64),
					LoadedDigest: "sha256:" + strings.Repeat("b", 64),
				}
				env[key] = ref
			}
			for role, image := range images {
				images[role], _ = kindBuildRegistryImage(t, image, "rpgmigrate-m7-"+runID+"-worker")
			}
			env["E2E_TOOL_IMAGE"] = images["seed-tool-runner"].Ref
			input := kindSuiteInput{
				SchemaVersion: 2, RunID: runID, Context: context, SourceTreeSHA256: strings.Repeat("d", 64),
				Images: images, Engine: string(v1beta1.MigrationEnginePGCopyDB), Pair: kindLegacyPair,
				LeaseExpiresAt: float64(time.Now().Add(time.Hour).Unix()),
			}
			source := map[string]string{"repository": "https://github.com/example/operator", "commit": strings.Repeat("a", 40),
				"git_tree": strings.Repeat("b", 40), "payload_inventory_sha256": input.SourceTreeSHA256}
			lockImages := kindFixtureLockImages(input.Images, source)
			lockFields := map[string]any{"schema_version": 2, "transport": "public-registry", "operator_source": source,
				"engine_source": source, "images": lockImages, "publication_receipts_sha256": map[string]string{
					kindManagerContainer: strings.Repeat("a", 64), kindNativeRunner: strings.Repeat("b", 64)}}
			mutateKindLockShape(which, lockFields, lockImages)
			lock, err := json.Marshal(lockFields)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "image-lock.json"), lock, 0600); err != nil {
				t.Fatal(err)
			}
			input.ImageLockSHA256 = fmt.Sprintf("%x", sha256.Sum256(lock))
			mutateKindLockFixture(t, which, dir, &input, env)
			if which == "host kubeconfig" {
				env["KUBECONFIG"] = configPath + ":/tmp/host"
			}
			if which == "wrong context" {
				env["E2E_CONTEXT"] = "foreign"
			}
			if which == "public mode" {
				if err := os.Chmod(configPath, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if which == "symlink" {
				real := configPath + "-real"
				if err := os.Rename(configPath, real); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(real, configPath); err != nil {
					t.Fatal(err)
				}
			}
			if which == "wrong image" {
				env["E2E_RUNNER_IMAGE"] = "example.invalid/wrong:test"
			}
			if which == "missing role" {
				delete(input.Images, "legacy-runner")
			}
			data, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if which == "oversized input" {
				data = append(data, bytes.Repeat([]byte(" "), 1024*1024)...)
			}
			if err := os.WriteFile(filepath.Join(dir, "suite-input.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			run, err := loadKindSuite(func(key string) string { return env[key] })
			if which == "accepted private config" {
				if err != nil || run == nil {
					t.Fatalf("valid private config refused: %v", err)
				}
				got := run.commandArgs("kubectl", []string{"get", "pods"})
				want := []string{kindKubeconfigFlag, configPath, "--context", context, "get", "pods"}
				if !slices.Equal(got, want) {
					t.Fatalf("argv %v", got)
				}
			} else if err == nil {
				t.Fatalf("%s accepted", which)
			}
		})
	}
}

func TestKindEngineRefusal(t *testing.T) {
	for _, engine := range []string{"", string(v1beta1.MigrationEnginePGCopyDB), "rpgmigrate", "unknown"} {
		t.Setenv("E2E_ENGINE", engine)
		got, err := suiteEngine()
		if engine == "" || engine == string(v1beta1.MigrationEnginePGCopyDB) {
			if err != nil || got != v1beta1.MigrationEnginePGCopyDB {
				t.Fatalf("legacy selection %q = %s,%v", engine, got, err)
			}
		} else if err == nil {
			t.Fatalf("unqualified engine %s admitted", engine)
		}
	}
}

const kindLegacyPair = "17:17"
const kindLinux = "linux"
const kindArchitectureField = "architecture"
const kindConfigDigestField = "config_digest"
const kindDigestField = "digest"
const kindRefField = "ref"
const kindVariantField = "variant"
const kindNativeRunner = "native-runner"

var kindNodeSuffixes = []string{"control-plane", "worker", "worker2"}
var kindLockRoles = []string{"node", "cnpg-controller", "postgresql-16", "postgresql-17", "postgresql-18",
	kindManagerContainer, "legacy-runner", kindNativeRunner, "seed-tool-runner", "otel-collector",
	"storage-helper", "storage-provisioner"}

type kindNodeBinding struct {
	NodeID           string   `json:"node_id"`
	RequestedRef     string   `json:"requested_ref"`
	ManifestDigest   string   `json:"manifest_digest"`
	ConfigDigest     string   `json:"config_digest"`
	OS               string   `json:"os"`
	Architecture     string   `json:"architecture"`
	Variant          *string  `json:"variant"`
	AcceptedImageIDs []string `json:"accepted_image_ids"`
}
type kindImage struct {
	Ref                 string                     `json:"ref"`
	Digest              string                     `json:"digest"`
	Architecture        string                     `json:"architecture"`
	ImageID             string                     `json:"image_id"`
	LoadedDigest        string                     `json:"loaded_digest"`
	RegistryIndexDigest *string                    `json:"registry_index_digest"`
	NodeBindings        map[string]kindNodeBinding `json:"node_bindings"`
}
type kindSuiteInput struct {
	SchemaVersion    int                  `json:"schema_version"`
	RunID            string               `json:"run_id"`
	Context          string               `json:"context"`
	SourceTreeSHA256 string               `json:"source_tree_sha256"`
	ImageLockSHA256  string               `json:"image_lock_sha256"`
	Images           map[string]kindImage `json:"images"`
	Engine           string               `json:"engine"`
	Pair             string               `json:"pair"`
	LeaseExpiresAt   float64              `json:"lease_expires_at"`
}

func uniqueKindJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	keys := map[string]bool{}
	for decoder.More() {
		if delim == '{' {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || keys[name] {
				return errors.New("duplicate kind input field")
			}
			keys[name] = true
		}
		if err := uniqueKindJSON(decoder); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

func decodeKindFields(data []byte, destination any, fields ...string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := uniqueKindJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("invalid kind input suffix")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	if len(object) != len(fields) {
		return errors.New("kind input fields differ")
	}
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return errors.New("kind input field missing")
		}
	}
	return json.Unmarshal(data, destination)
}

func (image *kindImage) UnmarshalJSON(data []byte) error {
	type plain kindImage
	return decodeKindFields(data, (*plain)(image), kindRefField, kindDigestField, kindArchitectureField, "image_id",
		"loaded_digest", "registry_index_digest", "node_bindings")
}
func (binding *kindNodeBinding) UnmarshalJSON(data []byte) error {
	type plain kindNodeBinding
	return decodeKindFields(data, (*plain)(binding), "node_id", "requested_ref", "manifest_digest", kindConfigDigestField,
		"os", kindArchitectureField, kindVariantField, "accepted_image_ids")
}
func (input *kindSuiteInput) UnmarshalJSON(data []byte) error {
	type plain kindSuiteInput
	return decodeKindFields(data, (*plain)(input), "schema_version", "run_id", "context", "source_tree_sha256",
		"image_lock_sha256", "images", "engine", "pair", "lease_expires_at")
}

func readKindInput(path string) ([]byte, error) {
	if err := privateKindPath(path, false); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, 1024*1024+1))
	if err != nil || len(data) > 1024*1024 {
		return nil, errors.New("kind input exceeds bounded read")
	}
	return data, nil
}

func (run *kindSuite) validateImageLock() error {
	data, err := readKindInput(filepath.Join(run.dir, "image-lock.json"))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(data)) != run.input.ImageLockSHA256 {
		return errors.New("kind image lock hash differs")
	}
	var lock struct {
		SchemaVersion  int    `json:"schema_version"`
		Transport      string `json:"transport"`
		OperatorSource struct {
			PayloadInventorySHA256 string `json:"payload_inventory_sha256"`
		} `json:"operator_source"`
		Images map[string]struct {
			Role         string  `json:"role"`
			Ref          string  `json:"ref"`
			Digest       string  `json:"digest"`
			ConfigDigest string  `json:"config_digest"`
			Architecture string  `json:"architecture"`
			OS           string  `json:"os"`
			Variant      *string `json:"variant"`
			IndexDigest  *string `json:"index_digest"`
		} `json:"images"`
	}
	var raw struct {
		OperatorSource json.RawMessage            `json:"operator_source"`
		Images         map[string]json.RawMessage `json:"images"`
	}
	if decodeKindFields(data, &raw, "schema_version", "transport", "operator_source", "engine_source", "images",
		"publication_receipts_sha256") != nil || len(raw.Images) != len(kindLockRoles) {
		return errors.New("kind image lock fields differ")
	}
	for _, role := range kindLockRoles {
		if _, ok := raw.Images[role]; !ok {
			return errors.New("kind image lock roles differ")
		}
	}
	if decodeKindFields(raw.OperatorSource, &lock.OperatorSource, "repository", "commit", "git_tree",
		"payload_inventory_sha256") != nil || json.Unmarshal(data, &lock) != nil || lock.SchemaVersion != 2 ||
		lock.Transport != "public-registry" || lock.OperatorSource.PayloadInventorySHA256 != run.input.SourceTreeSHA256 {
		return errors.New("kind image lock source differs")
	}
	for role, image := range run.input.Images {
		expected, ok := lock.Images[role]
		fields := []string{kindBuildRoleType, kindRefField, "os", kindArchitectureField, kindVariantField, kindDigestField,
			"index_digest", kindConfigDigestField, "provenance"}
		if role == kindManagerContainer {
			fields = append(fields, "source")
		}
		if decodeKindFields(raw.Images[role], &expected, fields...) != nil {
			return errors.New("kind image lock image fields differ")
		}

		if !ok || expected.Role != role || expected.Ref != image.Ref || expected.Digest != image.Digest ||
			expected.ConfigDigest != image.ImageID ||
			expected.Architecture != image.Architecture || expected.OS != kindLinux ||
			!equalKindOptional(expected.IndexDigest, image.RegistryIndexDigest) {
			return errors.New("kind image lock binding differs")
		}
		for _, binding := range image.NodeBindings {
			if !equalKindOptional(binding.Variant, expected.Variant) {
				return errors.New("kind image lock platform differs")
			}
		}
	}
	return nil
}
func equalKindOptional(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

type kindSuite struct {
	input                        kindSuiteInput
	dir, kubeconfig, inputSHA256 string
	raw                          clientcmdapi.Config
	operatorNamespaceUID         types.UID
	operatorNamespaceAbsent      bool
}

var activeKindSuite *kindSuite
var kindFixturesCleaned bool
var kindPublicImagePattern = regexp.MustCompile(
	`^[a-z0-9][a-z0-9.-]*(?::[0-9]+)?/[A-Za-z0-9._/-]+(?::[A-Za-z0-9._-]+)?@sha256:[0-9a-f]{64}$`)

var kindImageEnvironment = map[string]string{
	kindManagerContainer: "E2E_MANAGER_IMAGE", "legacy-runner": "E2E_RUNNER_IMAGE",
	"seed-tool-runner": "E2E_SEED_IMAGE", "otel-collector": "E2E_COLLECTOR_IMAGE",
	"postgresql-17": "E2E_POSTGRES_17_IMAGE",
}

func privateKindPath(path string, directory bool) error {
	real, err := filepath.EvalSymlinks(path)
	if err != nil || !filepath.IsAbs(path) || real != filepath.Clean(path) {
		return errors.New("kind path must be absolute and contain no symlinks")
	}
	st, err := os.Stat(path)
	if err != nil {
		return errors.New("kind private path is unavailable")
	}
	mode := os.FileMode(0600)
	if directory {
		mode = 0700
	}
	if st.IsDir() != directory || st.Mode().Perm() != mode || (!directory && !st.Mode().IsRegular()) {
		return errors.New("kind private path has an unsafe type or mode")
	}
	return nil
}

func loadKindSuite(getenv func(string) string) (*kindSuite, error) {
	dir := getenv("E2E_KIND_RUN_DIR")
	if dir == "" {
		if getenv("E2E_KIND_CONTEXT") != "" {
			return nil, errors.New("kind context requires private run directory")
		}
		return nil, nil
	}
	if err := privateKindPath(dir, true); err != nil {
		return nil, err
	}
	run := &kindSuite{dir: dir, kubeconfig: filepath.Join(dir, "kubeconfig")}
	if getenv("KUBECONFIG") != run.kubeconfig {
		return nil, errors.New("kind KUBECONFIG must be the single private file")
	}
	for _, path := range []string{run.kubeconfig, filepath.Join(dir, "suite-input.json")} {
		if err := privateKindPath(path, false); err != nil {
			return nil, err
		}
	}
	data, err := readKindInput(filepath.Join(dir, "suite-input.json"))
	if err != nil {
		return nil, errors.New("kind suite input unavailable")
	}
	if err := json.Unmarshal(data, &run.input); err != nil {
		return nil, errors.New("invalid kind suite input")
	}
	if err := run.validateInput(getenv); err != nil {
		return nil, err
	}
	if err := run.validateImageLock(); err != nil {
		return nil, err
	}
	if err := run.loadKubeconfig(); err != nil {
		return nil, err
	}
	run.inputSHA256 = fmt.Sprintf("%x", sha256.Sum256(data))
	return run, nil
}

func (run *kindSuite) validateInput(getenv func(string) string) error {
	input := run.input
	if math.IsNaN(input.LeaseExpiresAt) || math.IsInf(input.LeaseExpiresAt, 0) || input.LeaseExpiresAt <= 0 ||
		input.SchemaVersion != 2 || !featureE2ERunPattern.MatchString(input.RunID) ||
		!kindHashPattern.MatchString(input.ImageLockSHA256) || !kindHashPattern.MatchString(input.SourceTreeSHA256) ||
		input.Context != "kind-rpgmigrate-m7-"+input.RunID ||
		input.Engine != string(v1beta1.MigrationEnginePGCopyDB) || input.Pair != kindLegacyPair {
		return errors.New("kind suite input binding is invalid")
	}
	if getenv("E2E_CONTEXT") != input.Context || getenv("E2E_KIND_CONTEXT") != input.Context ||
		getenv("E2E_ENGINE") != input.Engine || getenv("E2E_SMALL") != strconv.FormatBool(true) {
		return errors.New("kind suite context, engine or profile conflicts with input")
	}
	for _, name := range []string{envSourceURI, envTargetURI, "E2E_PROMETHEUS_URL", "E2E_PROMETHEUS_PORT_FORWARD"} {
		if getenv(name) != "" {
			return errors.New("kind suite refuses external endpoints")
		}
	}
	for name, want := range map[string]string{"E2E_RUN_LABEL_VALUE": input.RunID,
		"E2E_OPERATOR_NAMESPACE": defaultOperatorNamespace, "E2E_MANAGE_NAMESPACES": strconv.FormatBool(true),
		"E2E_PG_SOURCE": "17", "E2E_PG_TARGET": "17"} {
		if value := getenv(name); value != "" && value != want {
			return fmt.Errorf("kind suite conflicts with %s", name)
		}
	}
	if len(input.Images) != len(kindImageEnvironment) {
		return errors.New("kind image role set differs")
	}
	for role, key := range kindImageEnvironment {
		image := input.Images[role]
		if getenv(key) != image.Ref ||
			!validKindManagerImageID(image.ImageID, "rpgmigrate-m7-"+input.RunID+"-control-plane", image) {
			return fmt.Errorf("kind image binding invalid for role %s", role)
		}
	}
	if getenv("E2E_TOOL_IMAGE") != input.Images["seed-tool-runner"].Ref {
		return errors.New("kind tool image binding invalid")
	}
	return nil
}

func (run *kindSuite) loadKubeconfig() error {
	raw, err := clientcmd.LoadFromFile(run.kubeconfig)
	if err != nil {
		return errors.New("kind kubeconfig cannot be loaded")
	}
	if raw.CurrentContext != run.input.Context || len(raw.Contexts) != 1 || len(raw.Clusters) != 1 ||
		len(raw.AuthInfos) != 1 {
		return errors.New("kind kubeconfig must contain only the owned context")
	}
	c := raw.Contexts[run.input.Context]
	if c == nil || c.Cluster != run.input.Context ||
		raw.Clusters[c.Cluster] == nil || raw.AuthInfos[c.AuthInfo] == nil {
		return errors.New("kind kubeconfig does not bind the owned cluster")
	}
	auth := raw.AuthInfos[c.AuthInfo]
	cluster := raw.Clusters[c.Cluster]
	endpoint, endpointErr := url.Parse(cluster.Server)
	if endpointErr != nil {
		return errors.New("kind API endpoint is invalid")
	}
	address := net.ParseIP(endpoint.Hostname())
	if endpoint.Scheme != "https" || address == nil || !address.IsLoopback() || endpoint.Port() == "" ||
		endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") {
		return errors.New("kind API endpoint must be owned loopback HTTPS")
	}
	if auth.Exec != nil || auth.AuthProvider != nil || auth.ClientKey != "" || auth.ClientCertificate != "" ||
		auth.TokenFile != "" || cluster.CertificateAuthority != "" || cluster.ProxyURL != "" {
		return errors.New("kind kubeconfig must not load external credentials or proxies")
	}
	run.raw = *raw
	return nil
}

var (
	kindImagePattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*(?:@sha256:[0-9a-f]{64})?$`)
	kindHashPattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	kindDigestPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	kindNodeNamePattern = regexp.MustCompile(`^(rpgmigrate-m7-[0-9a-f]{32})-(?:control-plane|worker|worker2)$`)
)

func (run *kindSuite) commandArgs(tool string, args []string) []string {
	if run == nil {
		return args
	}
	contextFlag := "--context"
	if tool == "helm" {
		contextFlag = "--kube-context"
	}
	return append([]string{kindKubeconfigFlag, run.kubeconfig, contextFlag, run.input.Context}, args...)
}
func suiteCommandArgs(tool string, args []string) []string {
	return activeKindSuite.commandArgs(tool, args)
}

func helmImageValues() ([]string, error) {
	values := []string{}
	for _, selection := range []struct{ env, prefix string }{
		{"E2E_MANAGER_IMAGE", "image"}, {"E2E_RUNNER_IMAGE", "runner.image"},
	} {
		image := os.Getenv(selection.env)
		if image == "" {
			continue
		}
		if !kindImagePattern.MatchString(image) {
			return nil, fmt.Errorf("%s is not a valid image reference", selection.env)
		}
		name, digest, pinned := strings.Cut(image, "@")
		colon := strings.LastIndex(name, ":")
		if colon <= strings.LastIndex(name, "/") {
			if !pinned || !kindDigestPattern.MatchString(digest) {
				return nil, fmt.Errorf("%s must include an explicit image tag or digest", selection.env)
			}
			values = append(values, selection.prefix+".repository="+name, selection.prefix+".digest="+digest)
			continue
		}
		if colon == len(name)-1 {
			return nil, fmt.Errorf("%s has an empty image tag", selection.env)
		}
		values = append(values, selection.prefix+".repository="+image[:colon], selection.prefix+".tag="+image[colon+1:])
	}
	if activeKindSuite != nil {
		values = append(values, "image.pullPolicy=IfNotPresent")
	}
	return values, nil
}

const kindRunLabel = "pgcopydb-operator.io/m7-run"
const kindKubeconfigFlag = "--kubeconfig"
const kindSmallMaintenance = "64MB"

func requireKindNamespace(name string) error {
	return requireKindNamespaceContext(ctx, name)
}

func requireKindNamespaceContext(callCtx context.Context, name string) error {
	ns := &corev1.Namespace{}
	if err := k8sClient.Get(callCtx, client.ObjectKey{Name: name}, ns); err != nil {
		return err
	}
	if ns.Labels[kindRunLabel] != activeKindSuite.input.RunID ||
		name == defaultOperatorNamespace && (activeKindSuite.operatorNamespaceUID == "" ||
			ns.UID != activeKindSuite.operatorNamespaceUID) {
		return errors.New("kind namespace is not owned by this run")
	}
	return nil
}

func TestKindTaskContextAndSelection(t *testing.T) {
	taskPath, err := exec.LookPath("task")
	if err != nil {
		t.Skip("Task is required for the command guard test")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, override := range []string{"", "E2E_CONTEXT=foreign", "EXPECT_CONTEXT=foreign", "E2E_ENGINE=rpgmigrate"} {
		t.Run(override, func(t *testing.T) {
			dir := t.TempDir()
			capture := filepath.Join(dir, "capture")
			fakeGo := `#!/bin/sh
printf '%s\n' "$E2E_CONTEXT" "$E2E_ENGINE" "$@" > "$M7_CAPTURE"
`
			if err := os.WriteFile(filepath.Join(dir, "go"), []byte(fakeGo), 0700); err != nil {
				t.Fatal(err)
			}
			args := []string{"--dir", root, "e2e:kind:suite", "RUN_DIR=" + dir, "EXPECT_CONTEXT=owned", "E2E_ENGINE=pgcopydb"}
			if override != "" {
				args = append(args, override)
			}
			cmd := exec.Command(taskPath, args...)
			cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin", "E2E_KIND_RUN_DIR=" + dir, "E2E_KIND_CONTEXT=owned",
				"E2E_ENGINE=pgcopydb", "M7_CAPTURE=" + capture}
			out, err := cmd.CombinedOutput()
			data, readErr := os.ReadFile(capture)
			if override != "" {
				if err == nil || !os.IsNotExist(readErr) {
					t.Fatalf("conflicting Task variable reached suite: %v\n%s", err, out)
				}
				return
			}
			if err != nil || readErr != nil {
				t.Fatalf("Task entry failed: %v/%v\n%s", err, readErr, out)
			}
			got := strings.Split(strings.TrimSpace(string(data)), "\n")
			for _, want := range []string{"owned", string(v1beta1.MigrationEnginePGCopyDB), "^TestE2E$", "-count=1",
				"-ginkgo.label-filter=kind-bootstrap", "-ginkgo.fail-on-empty"} {
				if !slices.Contains(got, want) {
					t.Fatalf("Task did not select required argument %s", want)
				}
			}
		})
	}
}

func TestKindHelmImageValues(t *testing.T) {
	t.Setenv("E2E_MANAGER_IMAGE", "registry.example:5000/manager:m7")
	t.Setenv("E2E_RUNNER_IMAGE", "example.invalid/runner:pinned@sha256:"+strings.Repeat("a", 64))
	values, err := helmImageValues()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"image.repository=registry.example:5000/manager", "image.tag=m7",
		"runner.image.repository=example.invalid/runner", "runner.image.tag=pinned@sha256:" + strings.Repeat("a", 64)} {
		if !slices.Contains(values, want) {
			t.Fatalf("Helm lost the complete image identity: %s", want)
		}
	}
	t.Setenv("E2E_MANAGER_IMAGE", "registry.example/manager@sha256:"+strings.Repeat("a", 64))
	values, err = helmImageValues()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"image.repository=registry.example/manager",
		"image.digest=sha256:" + strings.Repeat("a", 64)} {
		if !slices.Contains(values, want) {
			t.Fatalf("digest-only manager lost identity: %s", want)
		}
	}
}

func TestKindNamespaceOwnershipBlocksObjectDeletion(t *testing.T) {
	oldRun, oldClient, oldCtx := activeKindSuite, k8sClient, ctx
	t.Cleanup(func() { activeKindSuite, k8sClient, ctx = oldRun, oldClient, oldCtx })
	ctx = context.Background()
	activeKindSuite = &kindSuite{input: kindSuiteInput{RunID: featureRunOwnerFixture},
		operatorNamespaceUID: "namespace-owned", operatorNamespaceAbsent: true}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: nsOperator, UID: activeKindSuite.operatorNamespaceUID, Labels: map[string]string{kindRunLabel: "foreign"},
	}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "owned-name", Namespace: nsOperator}}
	base := clientfake.NewClientBuilder().WithObjects(ns, job).Build()
	k8sClient = featureLabelingClient{Client: base}
	if err := k8sClient.Delete(ctx, job); err == nil {
		t.Fatal("foreign namespace permitted an object deletion")
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(job), &batchv1.Job{}); err != nil {
		t.Fatal("refused object was deleted")
	}
	ns.Labels[kindRunLabel] = featureRunOwnerFixture
	if err := base.Update(ctx, ns); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Delete(ctx, job); err != nil {
		t.Fatal(err)
	}
}

func TestKindManagerImageInput(t *testing.T) {
	id := strings.Repeat("a", 32)
	digest, config := "sha256:"+strings.Repeat("c", 64), "sha256:"+strings.Repeat("b", 64)
	manager := func(m map[string]any) map[string]any {
		return m["images"].(map[string]any)[kindManagerContainer].(map[string]any)
	}
	binding := func(m map[string]any) map[string]any {
		return manager(m)["node_bindings"].(map[string]any)["rpgmigrate-m7-"+id+"-worker"].(map[string]any)
	}
	for _, tc := range []struct {
		name     string
		change   func(map[string]any)
		accepted bool
	}{
		{"registry", func(map[string]any) {}, true},
		{"duplicate_suite", func(map[string]any) {}, false},
		{"duplicate_image", func(map[string]any) {}, false},
		{"duplicate_binding", func(map[string]any) {}, false},
		{"missing_index", func(m map[string]any) { delete(manager(m), "registry_index_digest") }, false},
		{"empty_index", func(m map[string]any) { manager(m)["registry_index_digest"] = "" }, false},
		{"config_as_index", func(m map[string]any) { manager(m)["registry_index_digest"] = config }, false},
		{"manifest_as_index", func(m map[string]any) { manager(m)["registry_index_digest"] = digest }, false},
		{"missing_config", func(m map[string]any) { delete(manager(m), "image_id") }, false},
		{"missing_registry_manifest_input", func(m map[string]any) { delete(manager(m), "loaded_digest") }, false},
		{"missing_variant", func(m map[string]any) { delete(binding(m), kindVariantField) }, false},
		{"unknown_binding", func(m map[string]any) { binding(m)["unexpected_registry_field"] = true }, false},
		{"unknown_image", func(m map[string]any) { manager(m)["unexpected_registry_field"] = true }, false},
		{"archive_field", func(m map[string]any) { manager(m)["archive_index_digest"] = nil }, false},
		{"unknown_role", func(m map[string]any) { m["images"].(map[string]any)["foreign"] = manager(m) }, false},
		{"missing_role", func(m map[string]any) { delete(m["images"].(map[string]any), kindManagerContainer) }, false},
		{"missing_bindings", func(m map[string]any) { delete(manager(m), "node_bindings") }, false},
		{"null_bindings", func(m map[string]any) { manager(m)["node_bindings"] = nil }, false},
		{"missing_node", func(m map[string]any) {
			delete(manager(m)["node_bindings"].(map[string]any), "rpgmigrate-m7-"+id+"-worker")
		}, false},
		{"wrong_node_id", func(m map[string]any) { binding(m)["node_id"] = "invalid-node-id" }, false},
		{"wrong_platform", func(m map[string]any) { binding(m)[kindArchitectureField] = "unsupported-platform" }, false},
		{"wrong_os", func(m map[string]any) { binding(m)["os"] = "windows" }, false},
		{"wrong_requested_ref",
			func(m map[string]any) { binding(m)["requested_ref"] = "ghcr.io/foreign/manager@" + digest }, false},
		{"non_digest_manager", func(m map[string]any) { manager(m)["ref"] = "ghcr.io/example/manager:mutable" }, false},
		{"wrong_source_hash", func(m map[string]any) { m["source_tree_sha256"] = "invalid-source-hash" }, false},
		{"wrong_lock_hash", func(m map[string]any) { m["image_lock_sha256"] = "invalid-lock-hash" }, false},
		{"schema1", func(m map[string]any) { m["schema_version"] = 1 }, false},
		{"unknown_suite", func(m map[string]any) { m["unexpected_suite_field"] = true }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			images := map[string]any{}
			env := map[string]string{"E2E_CONTEXT": "kind-rpgmigrate-m7-" + id,
				"E2E_KIND_CONTEXT": "kind-rpgmigrate-m7-" + id, "E2E_ENGINE": string(v1beta1.MigrationEnginePGCopyDB),
				"E2E_SMALL": strconv.FormatBool(true)}
			for role, key := range kindImageEnvironment {
				ref := "ghcr.io/example/" + role + "@" + digest
				bindings := map[string]any{}
				for i, suffix := range kindNodeSuffixes {
					bindings["rpgmigrate-m7-"+id+"-"+suffix] = map[string]any{"node_id": strings.Repeat(strconv.Itoa(i+1), 64),
						"requested_ref": ref, "manifest_digest": digest, kindConfigDigestField: config, "os": kindLinux,
						kindArchitectureField: runtime.GOARCH, kindVariantField: nil, "accepted_image_ids": []string{ref, config,
							digest}}
				}
				images[role] = map[string]any{kindRefField: ref, kindDigestField: digest, kindArchitectureField: runtime.GOARCH,
					"image_id": config, "loaded_digest": digest, "registry_index_digest": nil, "node_bindings": bindings}
				env[key] = ref
			}
			env["E2E_TOOL_IMAGE"] = env["E2E_SEED_IMAGE"]
			fields := map[string]any{"schema_version": 2, "run_id": id, "context": env["E2E_CONTEXT"],
				"source_tree_sha256": strings.Repeat("a", 64), "images": images,
				"engine": string(v1beta1.MigrationEnginePGCopyDB), "pair": kindLegacyPair,
				"lease_expires_at": float64(time.Now().Add(time.Hour).Unix()), "image_lock_sha256": strings.Repeat("d", 64)}
			tc.change(fields)
			data, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.name {
			case "duplicate_suite":
				data = bytes.Replace(data, []byte(`"schema_version":2`), []byte(`"schema_version":1,"schema_version":2`), 1)
			case "duplicate_image":
				data = bytes.Replace(data, []byte(`"ref":`), []byte(`"ref":"foreign","ref":`), 1)
			case "duplicate_binding":
				data = bytes.Replace(data, []byte(`"node_id":`), []byte(`"node_id":"foreign","node_id":`), 1)
			}
			var run kindSuite
			if err = json.Unmarshal(data, &run.input); err == nil {
				err = run.validateInput(func(key string) string { return env[key] })
			}
			if (err == nil) != tc.accepted {
				t.Fatalf("input accepted=%t: %v", tc.accepted, err)
			}
		})
	}
}
