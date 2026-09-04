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

package buildconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	gingkotypes "github.com/onsi/ginkgo/v2/types"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

const (
	featureWorkflow              = "../../.github/workflows/feature-e2e.yml"
	publishedE2EWorkflow         = "../../.github/workflows/e2e.yml"
	protectedClusterGroup        = "e2e-cluster"
	resolveJob                   = "resolve"
	pendingStatusJob             = "pending-status"
	managerImageJob              = "manager-image"
	runnerImageJob               = "runner-image"
	permissionContents           = "contents"
	permissionPackages           = "packages"
	permissionRead               = "read"
	permissionWrite              = "write"
	falseValue                   = "false"
	trueValue                    = "true"
	successValue                 = "success"
	failureValue                 = "failure"
	skippedValue                 = "skipped"
	errorValue                   = "error"
	unreadableValue              = "unreadable"
	fullModeValue                = "full"
	focusModeValue               = "focus"
	featureDefaultScale          = "0.1"
	featureFullScale             = "1.0"
	recoveryValue                = "recovery"
	roleKind                     = "Role"
	roleBindingKind              = "RoleBinding"
	clusterRoleKind              = "ClusterRole"
	clusterRoleBindingKind       = "ClusterRoleBinding"
	customResourceDefinitionKind = "CustomResourceDefinition"
	listKind                     = "List"
	schemaDescriptionKey         = "description"
	schemaTypeKey                = "type"
	schemaKeepValue              = "keep"
	schemaRemoveValue            = "remove"
	schemaStringType             = "string"
	itemsKey                     = "items"
	nameKey                      = "name"
	imageKey                     = "image"
	metadataKey                  = "metadata"
	specKey                      = "spec"
	containersKey                = "containers"
	statusKey                    = "status"
	stateKey                     = "state"
	apiVersionKey                = "apiVersion"
	sidecarName                  = "sidecar"
	imageIDKey                   = "imageID"
	kubectlCommand               = "kubectl"
	replacementUID               = "44444444-4444-4444-8444-444444444444"
	conditionTrueValue           = "True"
	replacementName              = "replacement"
	rollbackFailureNoController  = "rollback-failure-no-controller"
	deploymentKind               = "Deployment"
	leaderElectArg               = "--leader-elect"
	readyConditionType           = "Ready"
	featureControllerName        = "pgcopydb-e2e"
	featureControllerNS          = "pgcopydb-e2e"
	featureFixtureChart          = "../../charts/pgcopydb-operator"
	helmInstallCommand           = "install"
	helmTemplateCommand          = "template"
	helmSetFlag                  = "--set"
	helmSetStringFlag            = "--set-string"
	helmSetJSONFlag              = "--set-json"
	helmPostRendererFlag         = "--post-renderer"
	featurePostRendererPlugin    = "feature-e2e-postrenderer"
	appsV1                       = "apps/v1"
	namespaceKey                 = "namespace"
	labelsKey                    = "labels"
	replicasKey                  = "replicas"
	replicaSetKind               = "ReplicaSet"
	featureReplicaSetName        = "pgcopydb-e2e-7d4c"
	blockOwnerDeletionKey        = "blockOwnerDeletion"
	podKind                      = "Pod"
	podListKind                  = "PodList"
	kindKey                      = "kind"
	uidKey                       = "uid"
	resourceVersionKey           = "resourceVersion"
	controllerKey                = "controller"
	wrongManagerImage            = "ghcr.io/fixture/wrong:latest"
)

type protectedInput struct {
	Required bool     `json:"required"`
	Type     string   `json:"type"`
	Default  string   `json:"default"`
	Options  []string `json:"options"`
}

type protectedConcurrency struct {
	Group            string `json:"group"`
	CancelInProgress bool   `json:"cancel-in-progress"`
}

type protectedStep struct {
	Name            string            `json:"name"`
	ID              string            `json:"id"`
	Uses            string            `json:"uses"`
	If              string            `json:"if"`
	Run             string            `json:"run"`
	Env             map[string]string `json:"env"`
	With            map[string]any    `json:"with"`
	ContinueOnError bool              `json:"continue-on-error"`
}

type protectedJob struct {
	If          string               `json:"if"`
	Needs       any                  `json:"needs"`
	RunsOn      string               `json:"runs-on"`
	Environment string               `json:"environment"`
	Permissions map[string]string    `json:"permissions"`
	Concurrency protectedConcurrency `json:"concurrency"`
	Outputs     map[string]string    `json:"outputs"`
	Env         map[string]string    `json:"env"`
	Steps       []protectedStep      `json:"steps"`
}

type protectedWorkflow struct {
	On map[string]struct {
		Inputs map[string]protectedInput `json:"inputs"`
	} `json:"on"`
	Permissions map[string]string       `json:"permissions"`
	Concurrency protectedConcurrency    `json:"concurrency"`
	Jobs        map[string]protectedJob `json:"jobs"`
}

func parseProtectedWorkflow(t *testing.T, path string) protectedWorkflow {
	t.Helper()
	var wf protectedWorkflow
	if err := yaml.Unmarshal([]byte(read(t, path)), &wf); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return wf
}

func protectedStepNamed(t *testing.T, job protectedJob, name string) protectedStep {
	t.Helper()
	for _, step := range job.Steps {
		if step.Name == name {
			return step
		}
	}
	t.Fatalf("job has no %q step", name)
	return protectedStep{}
}

func protectedStepIndex(t *testing.T, job protectedJob, name string) int {
	t.Helper()
	for i, step := range job.Steps {
		if step.Name == name {
			return i
		}
	}
	t.Fatalf("job has no %q step", name)
	return -1
}

func protectedRuns(job protectedJob) string {
	runs := make([]string, 0, len(job.Steps))
	for _, step := range job.Steps {
		runs = append(runs, step.Run)
	}
	return strings.Join(runs, "\n")
}

func protectedStepsUsing(job protectedJob, uses string) []protectedStep {
	var steps []protectedStep
	for _, step := range job.Steps {
		if step.Uses == uses {
			steps = append(steps, step)
		}
	}
	return steps
}

func protectedNeeds(t *testing.T, job protectedJob) []string {
	t.Helper()
	switch needs := job.Needs.(type) {
	case nil:
		return nil
	case string:
		return []string{needs}
	case []any:
		result := make([]string, 0, len(needs))
		for _, need := range needs {
			name, ok := need.(string)
			if !ok {
				t.Fatalf("non-string job dependency %v", need)
			}
			result = append(result, name)
		}
		return result
	default:
		t.Fatalf("unexpected job dependency type %T", needs)
		return nil
	}
}

func protectedWithString(t *testing.T, step protectedStep, key string) string {
	t.Helper()
	v, ok := step.With[key]
	if !ok {
		t.Fatalf("step %q has no with.%s", step.Name, key)
	}
	return fmt.Sprint(v)
}

type compatibilitySnapshot struct {
	CRDs               map[string]string
	RBAC               map[string]string
	RenderedPrivileges []string
	Builder            map[string]string
}

func writeCompatibilityFixture(t *testing.T, root, name, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func removeDescriptions(value any) {
	document, ok := value.(map[string]any)
	if !ok {
		return
	}
	spec, ok := document[specKey].(map[string]any)
	if !ok {
		return
	}
	if validation, ok := spec["validation"].(map[string]any); ok {
		removeSchemaDescriptions(validation["openAPIV3Schema"])
	}
	versions, _ := spec["versions"].([]any)
	for _, value := range versions {
		version, ok := value.(map[string]any)
		if !ok {
			continue
		}
		schema, ok := version["schema"].(map[string]any)
		if ok {
			removeSchemaDescriptions(schema["openAPIV3Schema"])
		}
	}
}

func removeSchemaDescriptions(value any) {
	schema, ok := value.(map[string]any)
	if !ok {
		return
	}
	delete(schema, schemaDescriptionKey)
	for _, key := range []string{
		"additionalItems", "additionalProperties", itemsKey, "not",
		"allOf", "anyOf", "oneOf",
	} {
		child := schema[key]
		if children, ok := child.([]any); ok {
			for _, child := range children {
				removeSchemaDescriptions(child)
			}
			continue
		}
		removeSchemaDescriptions(child)
	}
	for _, key := range []string{"definitions", "dependencies", "patternProperties", "properties"} {
		children, _ := schema[key].(map[string]any)
		for _, child := range children {
			removeSchemaDescriptions(child)
		}
	}
}

func TestRemoveDescriptionsOnlyTouchesCRDSchemaNodes(t *testing.T) {
	document := map[string]any{
		metadataKey: map[string]any{
			"annotations": map[string]any{schemaDescriptionKey: schemaKeepValue},
		},
		specKey: map[string]any{
			"versions": []any{map[string]any{
				"schema": map[string]any{
					"openAPIV3Schema": map[string]any{
						schemaDescriptionKey: schemaRemoveValue,
						"default":            map[string]any{schemaDescriptionKey: schemaKeepValue},
						"properties": map[string]any{
							schemaDescriptionKey: map[string]any{
								schemaDescriptionKey: schemaRemoveValue,
								schemaTypeKey:        schemaStringType,
							},
							"list": map[string]any{
								itemsKey: map[string]any{
									schemaDescriptionKey: schemaRemoveValue,
									schemaTypeKey:        schemaStringType,
								},
								schemaTypeKey: "array",
							},
						},
					},
				},
			}},
		},
	}

	removeDescriptions(document)
	metadata := document[metadataKey].(map[string]any)
	annotations := metadata["annotations"].(map[string]any)
	if got := annotations[schemaDescriptionKey]; got != schemaKeepValue {
		t.Errorf("non-schema description = %v, want keep", got)
	}
	root := document[specKey].(map[string]any)["versions"].([]any)[0].(map[string]any)
	rootSchema := root["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
	if _, exists := rootSchema[schemaDescriptionKey]; exists {
		t.Error("root schema description was preserved")
	}
	if got := rootSchema["default"].(map[string]any)[schemaDescriptionKey]; got != schemaKeepValue {
		t.Errorf("schema default description = %v, want keep", got)
	}
	properties := rootSchema["properties"].(map[string]any)
	descriptionProperty, exists := properties[schemaDescriptionKey].(map[string]any)
	if !exists || descriptionProperty[schemaTypeKey] != schemaStringType {
		t.Fatalf("properties.description = %v, want a string schema", properties[schemaDescriptionKey])
	}
	if _, exists := descriptionProperty[schemaDescriptionKey]; exists {
		t.Error("properties.description annotation was preserved")
	}
	items := properties["list"].(map[string]any)[itemsKey].(map[string]any)
	if _, exists := items[schemaDescriptionKey]; exists {
		t.Error("nested items schema description was preserved")
	}
}

func compatibilityDocuments(
	t *testing.T,
	root, relative string,
	accept func(string) bool,
	ignoreDescriptions bool,
) map[string]string {
	t.Helper()
	documents := make(map[string]string)
	dir := filepath.Join(root, filepath.FromSlash(relative))
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(body), 4096)
		for index := 0; ; index++ {
			var document map[string]any
			if err := decoder.Decode(&document); err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("decode %s document %d: %w", path, index, err)
			}
			if len(document) == 0 {
				continue
			}
			kind, _ := document[kindKey].(string)
			if !accept(kind) {
				continue
			}
			apiVersion, _ := document[apiVersionKey].(string)
			metadata, _ := document[metadataKey].(map[string]any)
			name, _ := metadata["name"].(string)
			if apiVersion == "" || name == "" {
				return fmt.Errorf("%s document %d has no identity", path, index)
			}
			if ignoreDescriptions {
				removeDescriptions(document)
			}
			canonical, err := json.Marshal(document)
			if err != nil {
				return err
			}
			key := apiVersion + "/" + kind + "/" + name
			if _, exists := documents[key]; exists {
				return fmt.Errorf("duplicate document %s", key)
			}
			documents[key] = string(canonical)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("load compatibility documents under %s: %v", dir, err)
	}
	return documents
}

func compatibilityTree(t *testing.T, root, relative string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	dir := filepath.Join(root, filepath.FromSlash(relative))
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", path)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		name, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(name)] = info.Mode().Perm().String() + "\x00" + string(body)
		return nil
	})
	if err != nil {
		t.Fatalf("load compatibility tree under %s: %v", dir, err)
	}
	return files
}

func compatibilityRenderedPrivileges(t *testing.T, root string) []string {
	t.Helper()
	helm := os.Getenv("FEATURE_E2E_HELM")
	if !filepath.IsAbs(helm) {
		t.Fatal("FEATURE_E2E_HELM must name the trusted Helm executable by absolute path")
	}
	chart := filepath.Join(root, "charts", "pgcopydb-operator")
	args := []string{
		helmTemplateCommand, featureControllerName, chart,
		"--namespace", featureControllerNS,
		"--include-crds", "--skip-tests",
		helmSetFlag, "crds.install=true",
		helmSetFlag, "image.tag=feature-fixture@sha256:" + strings.Repeat("a", 64),
		helmSetFlag, "runner.image.tag=feature-fixture@sha256:" + strings.Repeat("b", 64),
		helmSetFlag, "watchNamespaces={pgcopydb-e2e,pgcopydb-e2e-x}",
		helmSetFlag, "leaderElection.enabled=true",
		helmSetFlag, "metrics.enabled=true",
		helmSetFlag, "metrics.serviceMonitor.enabled=true",
		helmSetFlag, "rbac.create=true",
		helmSetFlag, "serviceAccount.create=true",
		helmSetFlag, "serviceAccount.name=pgcopydb-e2e-manager",
		helmSetStringFlag, "fullnameOverride=" + featureControllerName,
	}
	output, err := exec.Command(helm, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("render fixed feature chart %s: %v\n%s", chart, err, output)
	}

	documents := make(map[string]string)
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
	for index := 0; ; index++ {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode fixed feature chart %s document %d: %v", chart, index, err)
		}
		if len(document) == 0 {
			continue
		}
		objects, err := compatibilityRenderedObjects(document)
		if err != nil {
			t.Fatalf("flatten fixed feature chart %s document %d: %v", chart, index, err)
		}
		for _, object := range objects {
			kind, _ := object[kindKey].(string)
			isCRD := kind == customResourceDefinitionKind
			isRBAC := kind == roleKind || kind == roleBindingKind ||
				kind == clusterRoleKind || kind == clusterRoleBindingKind
			if !isCRD && !isRBAC {
				continue
			}
			identity, err := normalizeCompatibilityRenderedIdentity(object)
			if err != nil {
				t.Fatalf("identify fixed feature chart %s document %d: %v", chart, index, err)
			}
			if isCRD {
				removeDescriptions(object)
			}
			canonical, err := json.Marshal(object)
			if err != nil {
				t.Fatalf("canonicalize fixed feature chart %s document %d: %v", chart, index, err)
			}
			if _, exists := documents[identity]; exists {
				t.Fatalf("duplicate rendered privilege identity %s", identity)
			}
			documents[identity] = string(canonical)
		}
	}

	normalized := make([]string, 0, len(documents))
	for identity, document := range documents {
		normalized = append(normalized, identity+"\x00"+document)
	}
	slices.Sort(normalized)
	return normalized
}

func compatibilityRenderedObjects(document map[string]any) ([]map[string]any, error) {
	kind, ok := document[kindKey].(string)
	if !ok || kind == "" {
		return nil, fmt.Errorf("rendered object has no kind")
	}
	if kind != listKind {
		if strings.HasSuffix(kind, listKind) {
			return nil, fmt.Errorf("unsupported rendered privilege List kind %s", kind)
		}
		if _, exists := document[itemsKey]; exists {
			return nil, fmt.Errorf("unsupported rendered privilege List envelope %s", kind)
		}
		return []map[string]any{document}, nil
	}
	if document[apiVersionKey] != "v1" {
		return nil, fmt.Errorf("unsupported rendered privilege List API version")
	}
	items, ok := document[itemsKey].([]any)
	if !ok || len(items) == 0 {
		return nil, fmt.Errorf("rendered privilege List has no items")
	}
	objects := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("rendered privilege List has an invalid item")
		}
		flattened, err := compatibilityRenderedObjects(object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, flattened...)
	}
	return objects, nil
}

func normalizeCompatibilityRenderedIdentity(document map[string]any) (string, error) {
	apiVersion, _ := document[apiVersionKey].(string)
	kind, _ := document[kindKey].(string)
	metadata, ok := document[metadataKey].(map[string]any)
	if apiVersion == "" || kind == "" || !ok {
		return "", fmt.Errorf("target resource has no API version, kind, or metadata")
	}
	name, _ := metadata[nameKey].(string)
	if name == "" {
		return "", fmt.Errorf("%s target resource has no name", kind)
	}
	namespace := ""
	if kind == roleKind || kind == roleBindingKind {
		value, exists := metadata[namespaceKey]
		if exists {
			var ok bool
			namespace, ok = value.(string)
			if !ok {
				return "", fmt.Errorf("%s/%s has a non-string namespace", kind, name)
			}
		}
		if namespace == "" {
			namespace = featureControllerNS
			metadata[namespaceKey] = namespace
		}
	}
	return apiVersion + "/" + kind + "/" + namespace + "/" + name, nil
}

func loadCompatibilitySnapshot(t *testing.T, root string) compatibilitySnapshot {
	t.Helper()
	snapshot := compatibilitySnapshot{
		CRDs: compatibilityDocuments(t, root, "config/crd/bases",
			func(kind string) bool { return kind == customResourceDefinitionKind }, true),
		RBAC: compatibilityDocuments(t, root, "config/rbac",
			func(kind string) bool {
				return kind == roleKind || kind == roleBindingKind ||
					kind == clusterRoleKind || kind == clusterRoleBindingKind
			}, false),
		RenderedPrivileges: compatibilityRenderedPrivileges(t, root),
		Builder:            compatibilityTree(t, root, "images/pgcopydb-builder"),
	}
	if len(snapshot.CRDs) == 0 || len(snapshot.Builder) == 0 {
		t.Fatal("compatibility input is missing CRDs or pgcopydb-builder files")
	}
	for _, kind := range []string{roleKind, roleBindingKind, clusterRoleKind, clusterRoleBindingKind} {
		found := false
		for identity := range snapshot.RBAC {
			if strings.Contains(identity, "/"+kind+"/") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("compatibility input is missing %s documents", kind)
		}
	}
	for _, kind := range []string{
		customResourceDefinitionKind, roleKind, roleBindingKind, clusterRoleKind, clusterRoleBindingKind,
	} {
		found := false
		for _, document := range snapshot.RenderedPrivileges {
			identity, _, ok := strings.Cut(document, "\x00")
			if ok && strings.Contains(identity, "/"+kind+"/") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("rendered privilege input is missing %s documents", kind)
		}
	}
	return snapshot
}

func TestFeatureE2ECandidateCompatibility(t *testing.T) {
	base, head := os.Getenv("FEATURE_E2E_BASE"), os.Getenv("FEATURE_E2E_HEAD")
	if base == "" && head == "" {
		t.Skip("the trusted workflow supplies compatibility roots")
	}
	if base == "" || head == "" {
		t.Fatal("FEATURE_E2E_BASE and FEATURE_E2E_HEAD must be set together")
	}
	baseSnapshot := loadCompatibilitySnapshot(t, base)
	headSnapshot := loadCompatibilitySnapshot(t, head)
	if !maps.Equal(baseSnapshot.CRDs, headSnapshot.CRDs) {
		t.Error("candidate changes the CRD beyond description fields")
	}
	if !maps.Equal(baseSnapshot.RBAC, headSnapshot.RBAC) {
		t.Error("candidate changes Role, RoleBinding, ClusterRole, or ClusterRoleBinding documents")
	}
	if !slices.Equal(baseSnapshot.RenderedPrivileges, headSnapshot.RenderedPrivileges) {
		t.Error("candidate changes rendered CRD, Role, RoleBinding, ClusterRole, or ClusterRoleBinding documents")
	}
	if !maps.Equal(baseSnapshot.Builder, headSnapshot.Builder) {
		t.Error("candidate changes images/pgcopydb-builder")
	}
}

func TestProtectedE2EWorkflowsQueueWithoutChangingReleaseScale(t *testing.T) {
	published := parseProtectedWorkflow(t, publishedE2EWorkflow)
	if published.Concurrency.Group != protectedClusterGroup || published.Concurrency.CancelInProgress {
		t.Errorf("published e2e concurrency = %+v, want queued e2e-cluster", published.Concurrency)
	}

	release := parseProtectedWorkflow(t, releaseWorkflow)
	candidate, ok := release.Jobs["e2e"]
	if !ok {
		t.Fatal("release.yml has no e2e job")
	}
	if candidate.Concurrency.Group != protectedClusterGroup || candidate.Concurrency.CancelInProgress {
		t.Errorf("candidate e2e concurrency = %+v, want queued e2e-cluster", candidate.Concurrency)
	}
	run := protectedStepNamed(t, candidate, "Run the suite against the published candidate")
	if got, want := run.Env["E2E_SCALE"], "0.25"; got != want {
		t.Errorf("release candidate E2E_SCALE = %q, want %q", got, want)
	}
}

func TestE2ESuiteDoesNotPrintTheRawKubeContext(t *testing.T) {
	src := read(t, e2eSuite)
	for _, banned := range []string{
		"clientcmd.NewDefaultClientConfigLoadingRules",
		"raw.CurrentContext",
		"e2e running against kubectl context",
	} {
		if strings.Contains(src, banned) {
			t.Errorf("e2e suite still exposes the kube context through %q", banned)
		}
	}
}

func TestFeatureE2ETriggerInputsAndResolver(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	if got := slices.Sorted(maps.Keys(wf.On)); !slices.Equal(got, []string{"workflow_dispatch"}) {
		t.Fatalf("feature triggers = %v, want workflow_dispatch only", got)
	}
	inputs := wf.On["workflow_dispatch"].Inputs
	if got := slices.Sorted(maps.Keys(inputs)); !slices.Equal(got, []string{"focus", "mode", "pr", "scale"}) {
		t.Errorf("feature inputs = %v, want focus, mode, pr, scale", got)
	}
	if in := inputs["pr"]; !in.Required || in.Type != "string" {
		t.Errorf("pr input = %+v, want required string", in)
	}
	if in := inputs["mode"]; !in.Required || in.Type != "choice" || in.Default != "full" ||
		!slices.Equal(in.Options, []string{"full", "focus"}) {
		t.Errorf("mode input = %+v", in)
	}
	if in := inputs["focus"]; in.Required || in.Type != "string" || in.Default != "" {
		t.Errorf("focus input = %+v", in)
	}
	if in := inputs["scale"]; !in.Required || in.Type != "choice" || in.Default != featureDefaultScale ||
		!slices.Equal(in.Options, []string{featureDefaultScale, featureFullScale}) {
		t.Errorf("scale input = %+v", in)
	}
	resolve := wf.Jobs[resolveJob]
	if resolve.If != "github.repository == 'ydixken/pgcopydb-operator' && github.ref == 'refs/heads/main'" {
		t.Errorf("resolve trust guard = %q", resolve.If)
	}
	if got := protectedNeeds(t, wf.Jobs[pendingStatusJob]); !slices.Equal(got, []string{resolveJob}) {
		t.Errorf("pending status needs = %v, want resolve", got)
	}
	if got := protectedNeeds(t, wf.Jobs["preflight"]); !slices.Equal(got, []string{resolveJob, pendingStatusJob}) {
		t.Errorf("preflight needs = %v, want resolve and pending-status", got)
	}
	if got := resolve.Outputs["scale"]; got != "${{ steps.resolve.outputs.scale }}" {
		t.Errorf("resolved scale output = %q", got)
	}
	resolver := protectedStepNamed(t, resolve, "Resolve pull request")
	if got := resolver.Env["INPUT_SCALE"]; got != "${{ inputs.scale }}" {
		t.Errorf("resolver scale input = %q", got)
	}
	run := resolver.Run
	for _, want := range []string{
		`[[ "$INPUT_PR" =~ ^[1-9][0-9]*$ ]]`, `case "$INPUT_MODE" in`,
		`case "$INPUT_SCALE" in`, `tr -d '\000-\037\177'`, `.state == "open"`, `.base.ref == "main"`,
		`.base.repo.full_name == $repo`, `.head.repo.full_name == $repo`,
		`[[ "$sha" =~ ^[0-9a-f]{40}$ ]]`, `GITHUB_OUTPUT`,
	} {
		if !strings.Contains(run, want) {
			t.Errorf("resolver is missing %q", want)
		}
	}
	for jobName, job := range wf.Jobs {
		for _, step := range job.Steps {
			if strings.Contains(step.Run, "${{ inputs.") {
				t.Errorf("%s/%s interpolates input into shell source", jobName, step.Name)
			}
		}
	}
}

func TestFeatureE2EScaleResolver(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	resolver := protectedStepNamed(t, wf.Jobs[resolveJob], "Resolve pull request")
	const sha = "0123456789abcdef0123456789abcdef01234567"
	for _, tt := range []struct {
		scale     string
		wantError bool
	}{
		{scale: featureDefaultScale},
		{scale: featureFullScale},
		{scale: "0.2", wantError: true},
	} {
		t.Run(tt.scale, func(t *testing.T) {
			dir := t.TempDir()
			ghCalled := filepath.Join(dir, "gh-called")
			outputPath := filepath.Join(dir, "output")
			gh := `#!/usr/bin/env bash
set -euo pipefail
: > "$GH_CALLED"
printf '%s\n' '{"state":"open","base":{"ref":"main","repo":'` +
				`'{"full_name":"ydixken/pgcopydb-operator"}},"head":{"repo":'` +
				`'{"full_name":"ydixken/pgcopydb-operator"},"sha":"` + sha + `"}}'
`
			if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(gh), 0o700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", "-c", resolver.Run)
			cmd.Env = append(os.Environ(),
				"PATH="+dir+":"+os.Getenv("PATH"),
				"GH_CALLED="+ghCalled,
				"GH_TOKEN=test-token",
				"GITHUB_OUTPUT="+outputPath,
				"GITHUB_REPOSITORY=ydixken/pgcopydb-operator",
				"INPUT_PR=1",
				"INPUT_MODE=full",
				"INPUT_FOCUS=",
				"INPUT_SCALE="+tt.scale,
			)
			output, err := cmd.CombinedOutput()
			if tt.wantError {
				if err == nil {
					t.Error("resolver accepted an unlisted scale")
				}
				if _, statErr := os.Stat(ghCalled); !os.IsNotExist(statErr) {
					t.Error("resolver contacted GitHub before rejecting the scale")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolver rejected scale %s: %v\n%s", tt.scale, err, output)
			}
			resolved, readErr := os.ReadFile(outputPath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !strings.Contains(string(resolved), "scale="+tt.scale+"\n") {
				t.Errorf("resolver output = %q, want scale=%s", resolved, tt.scale)
			}
		})
	}
}

func TestFeatureE2EPermissionsAndStatuses(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	if wf.Permissions == nil || len(wf.Permissions) != 0 {
		t.Fatalf("top permissions = %v, want explicit empty map", wf.Permissions)
	}
	want := map[string]map[string]string{
		resolveJob:       {permissionContents: permissionRead, "pull-requests": permissionRead},
		pendingStatusJob: {"statuses": permissionWrite},
		"preflight":      {permissionContents: permissionRead, permissionPackages: permissionRead},
		managerImageJob:  {permissionContents: permissionRead, permissionPackages: permissionWrite},
		runnerImageJob:   {permissionContents: permissionRead, permissionPackages: permissionWrite},
		"cluster":        {permissionContents: permissionRead, permissionPackages: permissionRead},
		"final-status":   {"statuses": permissionWrite},
	}
	if got := slices.Sorted(maps.Keys(wf.Jobs)); !slices.Equal(got, slices.Sorted(maps.Keys(want))) {
		t.Fatalf("feature jobs = %v, want %v", got, slices.Sorted(maps.Keys(want)))
	}
	for name, permissions := range want {
		if !maps.Equal(wf.Jobs[name].Permissions, permissions) {
			t.Errorf("%s permissions = %v, want %v", name, wf.Jobs[name].Permissions, permissions)
		}
	}
	for _, name := range []string{pendingStatusJob, "final-status"} {
		run := protectedRuns(wf.Jobs[name])
		for _, needle := range []string{"statuses/$SHA", "feature-e2e", "feature-e2e/focus", "RUN_URL"} {
			if !strings.Contains(run, needle) {
				t.Errorf("%s is missing %q", name, needle)
			}
		}
	}
	if !strings.Contains(wf.Jobs["final-status"].If, "always()") {
		t.Error("final status does not run after failed dependencies")
	}
}

func TestFeatureE2ECompatibilityAndImmutableImages(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	preflightCheckouts := protectedStepsUsing(wf.Jobs["preflight"], "actions/checkout@v7")
	if len(preflightCheckouts) != 2 {
		t.Fatalf("preflight checkout count = %d, want 2", len(preflightCheckouts))
	}
	wantPreflightRefs := map[string]string{
		"trusted":   "${{ github.sha }}",
		"candidate": "${{ needs.resolve.outputs.sha }}",
	}
	for _, checkout := range preflightCheckouts {
		path := protectedWithString(t, checkout, "path")
		if protectedWithString(t, checkout, "ref") != wantPreflightRefs[path] ||
			protectedWithString(t, checkout, "persist-credentials") != falseValue {
			t.Errorf("preflight checkout for %q is not pinned safely", path)
		}
	}
	for _, jobName := range []string{managerImageJob, runnerImageJob, "cluster"} {
		checkouts := protectedStepsUsing(wf.Jobs[jobName], "actions/checkout@v7")
		if len(checkouts) != 1 ||
			protectedWithString(t, checkouts[0], "ref") != "${{ needs.resolve.outputs.sha }}" ||
			protectedWithString(t, checkouts[0], "persist-credentials") != falseValue {
			t.Errorf("%s does not check out only the resolved SHA", jobName)
		}
	}
	preflight := protectedRuns(wf.Jobs["preflight"])
	for _, want := range []string{
		"make -C candidate -f ../trusted/Makefile manifests",
		"git status --porcelain -- config/crd/bases config/rbac",
		"../trusted/hack/sync-chart-crd.sh --check", "../trusted/hack/sync-chart-rbac.sh --check",
		"FEATURE_E2E_BASE", "FEATURE_E2E_HEAD", "FEATURE_E2E_HELM",
		"TestFeatureE2ECandidateCompatibility",
		"pgcopydb-builder:", "candidate_ref", "trusted_ref",
		"docker buildx imagetools inspect",
	} {
		if !strings.Contains(preflight, want) {
			t.Errorf("preflight is missing %q", want)
		}
	}
	manager := wf.Jobs[managerImageJob]
	runner := wf.Jobs[runnerImageJob]
	for _, jobName := range []string{managerImageJob, runnerImageJob} {
		job := wf.Jobs[jobName]
		if got := protectedNeeds(t, job); !slices.Equal(got, []string{resolveJob, "preflight"}) || job.If != "" {
			t.Errorf("%s can run without a successful compatibility preflight: needs %v, if %q",
				jobName, got, job.If)
		}
	}
	if manager.Outputs["digest"] != "${{ steps.build.outputs.digest }}" ||
		runner.Outputs["digest"] != "${{ steps.build.outputs.digest }}" {
		t.Error("image jobs do not export build digests")
	}
	for _, build := range []struct {
		step    protectedStep
		context string
	}{
		{protectedStepNamed(t, manager, "Build and push manager"), "."},
		{protectedStepNamed(t, runner, "Build and push runner"), "images/runner"},
	} {
		if protectedWithString(t, build.step, "context") != build.context ||
			protectedWithString(t, build.step, "platforms") != "linux/amd64,linux/arm64" ||
			protectedWithString(t, build.step, "push") != trueValue ||
			!strings.Contains(protectedWithString(t, build.step, "tags"), "feature-${{ needs.resolve.outputs.sha }}") {
			t.Errorf("build step %q is not exact-SHA multi-arch output", build.step.Name)
		}
	}
	body := read(t, featureWorkflow)
	if strings.Contains(body, "context: images/pgcopydb-builder") || strings.Contains(body, "pgcopydb-builder.yml") {
		t.Error("feature workflow builds or delegates pgcopydb-builder")
	}
	if !strings.Contains(read(t, e2eSuite), `"crds.install=false"`) ||
		strings.Contains(body, "crds.install=true") {
		t.Error("feature path does not preserve the cluster CRD")
	}
	cluster := wf.Jobs["cluster"]
	if !strings.Contains(cluster.Env["MANAGER_REF"], "@${{ needs.manager-image.outputs.digest }}") ||
		!strings.Contains(cluster.Env["RUNNER_REF"], "@${{ needs.runner-image.outputs.digest }}") {
		t.Error("cluster image references are not digest-qualified")
	}
}

func TestFeatureE2EImageAttestation(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	helpers := protectedStepNamed(t, wf.Jobs["cluster"], "Write cluster helpers").Run
	attest := extractImageAttestationHeredoc(t, helpers)
	const (
		topDigest              = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
		childDigest            = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
		secondChild            = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
		unrelatedDigest        = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
		provenanceDigest       = "sha256:5555555555555555555555555555555555555555555555555555555555555555"
		secondProvenanceDigest = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
		uppercaseDigest        = "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		expectedImage          = "ghcr.io/ydixken/pgcopydb-operator"
		wrongImage             = "ghcr.io/fixture/unrelated"
	)
	expectedRef := expectedImage + ":feature-0123456789abcdef0123456789abcdef01234567@" + topDigest
	runnerRef := expectedImage + "/runner:feature-0123456789abcdef0123456789abcdef01234567@" + topDigest
	wrongRef := wrongImage + ":feature-0123456789abcdef0123456789abcdef01234567@" + topDigest
	schemeRef := "docker://" + expectedRef
	validRegistry := fmt.Sprintf(`{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"amd64","os":"linux"}},
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"arm64","os":"linux"}}
  ]
}`, childDigest, secondChild)
	dockerRegistry := fmt.Sprintf(`{
  "schemaVersion": 2,
  "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
  "manifests": [
    {"mediaType":"application/vnd.docker.distribution.manifest.v2+json","digest":%q,"size":1,
      "platform":{"architecture":"amd64","os":"linux"}},
    {"mediaType":"application/vnd.docker.distribution.manifest.v2+json","digest":%q,"size":1,
      "platform":{"architecture":"arm64","os":"linux"}}
  ]
}`, childDigest, secondChild)
	validOptionalMetadata := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"urls":["https://example.invalid/manifest"],
      "annotations":{"com.example.fixture":"value"},
      "data":"e30=","artifactType":"application/vnd.example.fixture",
      "platform":{"architecture":"amd64","os":"linux","variant":"v3",
        "os.version":"6.8","os.features":["fixture"]}`,
		1)
	validDockerPlatformMetadata := strings.Replace(dockerRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64","os":"linux","variant":"v3",
        "os.version":"6.8","os.features":["fixture"],"features":["sse4"]}`,
		1)
	provenanceRegistry := fmt.Sprintf(`{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"amd64","os":"linux"}},
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"arm64","os":"linux"}},
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"unknown","os":"unknown"},
      "annotations":{"vnd.docker.reference.digest":%q,
        "vnd.docker.reference.type":"attestation-manifest"}},
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"unknown","os":"unknown"},
      "annotations":{"vnd.docker.reference.digest":%q,
        "vnd.docker.reference.type":"attestation-manifest"}}
  ]
}`, childDigest, secondChild, provenanceDigest, childDigest, secondProvenanceDigest, secondChild)
	attestationOnRuntimePlatform := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64","os":"linux"},
      "annotations":{"vnd.docker.reference.digest":"`+childDigest+`",
        "vnd.docker.reference.type":"attestation-manifest"}`, 1)
	missingAttestationReference := strings.Replace(provenanceRegistry,
		`"vnd.docker.reference.digest":"`+childDigest+`",`+"\n        ", "", 1)
	malformedAttestationReference := strings.Replace(provenanceRegistry,
		`"vnd.docker.reference.digest":"`+childDigest+`"`,
		`"vnd.docker.reference.digest":"sha256:not-a-digest"`, 1)
	unrelatedAttestationReference := strings.Replace(provenanceRegistry,
		`"vnd.docker.reference.digest":"`+childDigest+`"`,
		`"vnd.docker.reference.digest":"`+unrelatedDigest+`"`, 1)
	ambiguousDescriptorDigest := strings.Replace(provenanceRegistry,
		provenanceDigest, childDigest, 1)
	unknownNonAttestation := strings.Replace(provenanceRegistry,
		`,
      "annotations":{"vnd.docker.reference.digest":"`+childDigest+`",
        "vnd.docker.reference.type":"attestation-manifest"}`,
		"", 1)
	singleManifestWithManifests := strings.Replace(validRegistry,
		"application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json", 1)
	missingIndexMediaType := strings.Replace(validRegistry,
		`  "mediaType": "application/vnd.oci.image.index.v1+json",`+"\n", "", 1)
	wrongSchemaVersion := strings.Replace(validRegistry, `"schemaVersion": 2`, `"schemaVersion": 1`, 1)
	missingDescriptorMediaType := strings.Replace(validRegistry,
		`"mediaType":"application/vnd.oci.image.manifest.v1+json",`, "", 1)
	invalidDescriptorMediaType := strings.Replace(validRegistry,
		"application/vnd.oci.image.manifest.v1+json", "application/octet-stream", 1)
	missingDescriptorSize := strings.Replace(validRegistry, `,"size":1`, "", 1)
	zeroDescriptorSize := strings.Replace(validRegistry, `"size":1`, `"size":0`, 1)
	fractionalDescriptorSize := strings.Replace(validRegistry, `"size":1`, `"size":1.5`, 1)
	missingDescriptorPlatform := strings.Replace(validRegistry,
		`,
      "platform":{"architecture":"amd64","os":"linux"}`, "", 1)
	missingPlatformOS := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64"}`, 1)
	missingPlatformArchitecture := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"os":"linux"}`, 1)
	annotationsArray := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"annotations":[],"platform":{"architecture":"amd64","os":"linux"}`, 1)
	numericAnnotationValue := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"annotations":{"com.example.fixture":42},
      "platform":{"architecture":"amd64","os":"linux"}`, 1)
	nonStringAttestationAnnotation := strings.Replace(provenanceRegistry,
		`"vnd.docker.reference.type":"attestation-manifest"`,
		`"com.example.fixture":["not-a-string"],
        "vnd.docker.reference.type":"attestation-manifest"`, 1)
	numericVariant := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64","os":"linux","variant":3}`, 1)
	numericOSVersion := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64","os":"linux","os.version":6.8}`, 1)
	nonArrayOSFeatures := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64","os":"linux","os.features":"fixture"}`, 1)
	nonStringOSFeature := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64","os":"linux","os.features":[42]}`, 1)
	nonArrayFeatures := strings.Replace(dockerRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64","os":"linux","features":"sse4"}`, 1)
	nonStringFeature := strings.Replace(dockerRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"platform":{"architecture":"amd64","os":"linux","features":[42]}`, 1)
	nonArrayURLs := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"urls":"https://example.invalid/manifest",
      "platform":{"architecture":"amd64","os":"linux"}`, 1)
	nonStringURL := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"urls":[42],"platform":{"architecture":"amd64","os":"linux"}`, 1)
	numericData := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"data":42,"platform":{"architecture":"amd64","os":"linux"}`, 1)
	numericArtifactType := strings.Replace(validRegistry,
		`"platform":{"architecture":"amd64","os":"linux"}`,
		`"artifactType":42,"platform":{"architecture":"amd64","os":"linux"}`, 1)
	uppercaseDescriptorDigest := strings.Replace(validRegistry, childDigest, uppercaseDigest, 1)
	missingAMD64 := fmt.Sprintf(`{
  "schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"arm64","os":"linux"}}
  ]
}`, secondChild)
	missingARM64 := fmt.Sprintf(`{
  "schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"amd64","os":"linux"}}
  ]
}`, childDigest)
	duplicateAMD64 := strings.Replace(validRegistry,
		`"architecture":"arm64"`, `"architecture":"amd64"`, 1)
	nonRuntimePlatform := strings.Replace(provenanceRegistry,
		`"architecture":"unknown","os":"unknown"`,
		`"architecture":"s390x","os":"linux"`, 1)
	emptyRegistry := `{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": []
}`
	nonmatchingRegistry := strings.ReplaceAll(validRegistry, childDigest, unrelatedDigest)
	tests := []struct {
		name           string
		pods           string
		registry       string
		kubectlFailure bool
		dockerFailure  bool
		wantSuccess    bool
	}{
		{"top-level index digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), validRegistry, false, false, true},
		{"child manifest digest", imageAttestationPods(t, expectedRef,
			"docker-pullable://"+expectedImage+"@"+childDigest), validRegistry, false, false, true},
		{"Docker manifest-list child digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+secondChild), dockerRegistry, false, false, true},
		{"OCI descriptor with valid optional metadata", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), validOptionalMetadata, false, false, true},
		{"Docker platform with valid optional metadata", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), validDockerPlatformMetadata, false, false, true},
		{"OCI index with Buildx provenance", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), provenanceRegistry, false, false, true},
		{"attestation digest as Pod imageID", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+provenanceDigest), provenanceRegistry, false, false, false},
		{"attestation marker on runtime platform", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), attestationOnRuntimePlatform, false, false, false},
		{"attestation without reference digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), missingAttestationReference, false, false, false},
		{"attestation with malformed reference digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), malformedAttestationReference, false, false, false},
		{"attestation with unrelated reference digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), unrelatedAttestationReference, false, false, false},
		{"attestation with a runtime descriptor digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), ambiguousDescriptorDigest, false, false, false},
		{"unknown non-attestation descriptor", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), unknownNonAttestation, false, false, false},
		{"non-runtime platform digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+provenanceDigest), nonRuntimePlatform, false, false, false},
		{"unrelated digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+unrelatedDigest), validRegistry, false, false, false},
		{"digest substring", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest+"0"), validRegistry, false, false, false},
		{"malformed image ID", imageAttestationPods(t, expectedRef,
			"docker-pullable://"+expectedImage+"@sha256:not-a-digest"), validRegistry, false, false, false},
		{"missing image ID", imageAttestationPods(t, expectedRef, nil), validRegistry, false, false, false},
		{"wrong runtime repository", imageAttestationPods(t, expectedRef,
			wrongImage+"@"+childDigest), validRegistry, false, false, false},
		{"wrong declared image", imageAttestationPods(t, wrongRef,
			wrongImage+"@"+childDigest), validRegistry, false, false, false},
		{"scheme-prefixed declared image", imageAttestationPods(t, schemeRef,
			"docker://"+expectedImage+"@"+childDigest), validRegistry, false, false, false},
		{"zero selected Pods", imageAttestationPods(t, expectedRef), validRegistry, false, false, false},
		{"two selected Pods with one eligible", imageAttestationPodLists(t,
			imageAttestationPods(t, expectedRef, expectedImage+"@"+childDigest),
			imageAttestationPodsForContainer(t, imageAttestationRunnerComponent, expectedRef,
				expectedImage+"@"+childDigest)), validRegistry, false, false, false},
		{"two selected Pods with one wrong image", imageAttestationPodLists(t,
			imageAttestationPods(t, expectedRef, expectedImage+"@"+childDigest),
			imageAttestationPods(t, wrongRef, wrongImage+"@"+childDigest)),
			validRegistry, false, false, false},
		{"multiple eligible Pods", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest, expectedImage+"@"+childDigest), validRegistry, false, false, false},
		{"one Pod with duplicate target containers", imageAttestationPodsWithDuplicateTarget(t,
			expectedRef, expectedImage+"@"+childDigest), validRegistry, false, false, false},
		{"one Pod with object-shaped target status", imageAttestationPodsWithObjectStatus(t,
			expectedRef, expectedImage+"@"+childDigest), validRegistry, false, false, false},
		{"one Pod with an extra sidecar", imageAttestationPodsWithSidecar(t, expectedRef,
			expectedImage+"@"+childDigest), validRegistry, false, false, true},
		{"malformed registry JSON", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), "{", false, false, false},
		{"single manifest with injected manifests", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), singleManifestWithManifests, false, false, false},
		{"index without media type", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), missingIndexMediaType, false, false, false},
		{"index with wrong schema version", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), wrongSchemaVersion, false, false, false},
		{"descriptor without media type", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), missingDescriptorMediaType, false, false, false},
		{"descriptor with unsupported media type", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), invalidDescriptorMediaType, false, false, false},
		{"descriptor without size", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), missingDescriptorSize, false, false, false},
		{"descriptor with zero size", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), zeroDescriptorSize, false, false, false},
		{"descriptor with fractional size", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), fractionalDescriptorSize, false, false, false},
		{"descriptor without platform", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), missingDescriptorPlatform, false, false, false},
		{"platform without OS", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), missingPlatformOS, false, false, false},
		{"platform without architecture", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), missingPlatformArchitecture, false, false, false},
		{"descriptor annotations array", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), annotationsArray, false, false, false},
		{"runtime descriptor annotation with numeric value", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), numericAnnotationValue, false, false, false},
		{"attestation descriptor annotation with non-string value", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), nonStringAttestationAnnotation, false, false, false},
		{"platform with numeric variant", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), numericVariant, false, false, false},
		{"platform with numeric OS version", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), numericOSVersion, false, false, false},
		{"platform with non-array OS features", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), nonArrayOSFeatures, false, false, false},
		{"platform with non-string OS feature", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), nonStringOSFeature, false, false, false},
		{"Docker platform with non-array features", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), nonArrayFeatures, false, false, false},
		{"Docker platform with non-string feature", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), nonStringFeature, false, false, false},
		{"descriptor with non-array URLs", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), nonArrayURLs, false, false, false},
		{"descriptor with non-string URL", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), nonStringURL, false, false, false},
		{"descriptor with numeric data", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), numericData, false, false, false},
		{"descriptor with numeric artifact type", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), numericArtifactType, false, false, false},
		{"descriptor with uppercase digest", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), uppercaseDescriptorDigest, false, false, false},
		{"index without amd64", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+secondChild), missingAMD64, false, false, false},
		{"index without arm64", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), missingARM64, false, false, false},
		{"duplicate amd64 platform", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), duplicateAMD64, false, false, false},
		{"nonmatching registry JSON", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), nonmatchingRegistry, false, false, false},
		{"registry index without manifests", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+topDigest), emptyRegistry, false, false, false},
		{"kubectl failure", "", validRegistry, true, false, false},
		{"registry inspection failure", imageAttestationPods(t, expectedRef,
			expectedImage+"@"+childDigest), "", false, true, false},
	}
	protected := []string{
		expectedRef, runnerRef, wrongRef, schemeRef, topDigest, childDigest, secondChild,
		unrelatedDigest, provenanceDigest, secondProvenanceDigest,
		imageAttestationOwnerKey, imageAttestationOwnerValue,
		imageAttestationOwnerKey + "=" + imageAttestationOwnerValue,
		"registry-credential-fixture", "private-context-fixture",
		"private-secret-fixture", "private-node-fixture",
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runImageAttestationFixture(t, attest, imageAttestationFixture{
				component:       imageAttestationManagerComponent,
				managerRef:      expectedRef,
				runnerRef:       runnerRef,
				pods:            tt.pods,
				registry:        tt.registry,
				kubectlFailure:  tt.kubectlFailure,
				dockerFailure:   tt.dockerFailure,
				protectedOutput: strings.Join(protected, " "),
			})
			if got := result.err == nil; got != tt.wantSuccess {
				t.Errorf("success = %t, want %t: %v\n%s", got, tt.wantSuccess, result.err, result.output)
			}
			for _, value := range protected {
				if strings.Contains(result.output, value) {
					t.Errorf("attestation output disclosed protected fixture input")
				}
			}
		})
	}
	t.Run("runner component", func(t *testing.T) {
		pods := imageAttestationPodsForContainer(t, imageAttestationRunnerComponent, runnerRef,
			expectedImage+"/runner@"+childDigest)
		result := runImageAttestationFixture(t, attest, imageAttestationFixture{
			component: imageAttestationRunnerComponent, managerRef: expectedRef, runnerRef: runnerRef,
			pods: pods, registry: validRegistry, protectedOutput: strings.Join(protected, " "),
		})
		if result.err != nil {
			t.Errorf("runner attestation failed: %v\n%s", result.err, result.output)
		}
		for _, value := range protected {
			if strings.Contains(result.output, value) {
				t.Error("runner attestation output disclosed protected fixture input")
			}
		}
	})
	for _, tt := range []struct {
		name, component string
		extraArgs       []string
	}{
		{"unknown component", "unrelated", nil},
		{"extra declared reference", imageAttestationManagerComponent, []string{wrongRef}},
		{"extra scheme-prefixed reference", imageAttestationManagerComponent, []string{schemeRef}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := runImageAttestationFixture(t, attest, imageAttestationFixture{
				component: tt.component, extraArgs: tt.extraArgs,
				managerRef: expectedRef, runnerRef: runnerRef,
				pods:     imageAttestationPods(t, expectedRef, expectedImage+"@"+childDigest),
				registry: validRegistry, protectedOutput: strings.Join(protected, " "),
			})
			if result.err == nil {
				t.Error("attestation accepted an untrusted component or declared reference")
			}
			for _, value := range protected {
				if strings.Contains(result.output, value) {
					t.Error("rejected attestation output disclosed protected fixture input")
				}
			}
		})
	}
	for _, tt := range []struct {
		name, reference, runtime string
	}{
		{"redirected manager reference", wrongRef, wrongImage + "@" + childDigest},
		{"scheme-prefixed manager reference", schemeRef, "docker://" + expectedImage + "@" + childDigest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := runImageAttestationFixture(t, attest, imageAttestationFixture{
				component: imageAttestationManagerComponent, managerRef: tt.reference, runnerRef: runnerRef,
				pods: imageAttestationPods(t, tt.reference, tt.runtime), registry: validRegistry,
				protectedOutput: strings.Join(protected, " "),
			})
			if result.err == nil {
				t.Error("attestation accepted a redirected manager reference")
			}
		})
	}
}

func TestFeatureE2EImageAttestationSuppressesXtrace(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	helpers := protectedStepNamed(t, wf.Jobs["cluster"], "Write cluster helpers").Run
	attest := extractImageAttestationHeredoc(t, helpers)
	const (
		topDigest     = "sha256:6666666666666666666666666666666666666666666666666666666666666666"
		amd64Digest   = "sha256:7777777777777777777777777777777777777777777777777777777777777777"
		arm64Digest   = "sha256:8888888888888888888888888888888888888888888888888888888888888888"
		expectedImage = "ghcr.io/ydixken/pgcopydb-operator"
	)
	managerRef := expectedImage + ":feature-0123456789abcdef0123456789abcdef01234567@" + topDigest
	runnerRef := expectedImage + "/runner:feature-0123456789abcdef0123456789abcdef01234567@" + topDigest
	registry := fmt.Sprintf(`{
  "schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"amd64","os":"linux"}},
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"arm64","os":"linux"}}
  ]
}`, amd64Digest, arm64Digest)
	ownerSelector := imageAttestationOwnerKey + "=" + imageAttestationOwnerValue
	protected := []string{
		managerRef, runnerRef, topDigest, amd64Digest, arm64Digest,
		imageAttestationOwnerKey, imageAttestationOwnerValue, ownerSelector,
		"registry-credential-fixture", "private-context-fixture",
		"private-secret-fixture", "private-node-fixture",
	}
	result := runImageAttestationFixture(t, attest, imageAttestationFixture{
		component: imageAttestationManagerComponent, managerRef: managerRef, runnerRef: runnerRef,
		pods:     imageAttestationPods(t, managerRef, expectedImage+"@"+amd64Digest),
		registry: registry, xtrace: true, protectedOutput: strings.Join(protected, " "),
	})
	if result.err != nil {
		t.Errorf("xtrace attestation failed: %v\n%s", result.err, result.output)
	}
	for _, value := range protected {
		if strings.Contains(result.output, value) {
			t.Errorf("xtrace output disclosed protected fixture input %q", value)
		}
	}
}

const (
	imageAttestationManagerComponent = "manager"
	imageAttestationRunnerComponent  = "runner"
	imageAttestationOwnerKey         = "pgcopydb-operator.io/feature-e2e-run"
	imageAttestationOwnerValue       = "0123456789abcdef0123456789abcdef"
)

type imageAttestationFixture struct {
	component       string
	extraArgs       []string
	managerRef      string
	runnerRef       string
	pods            string
	registry        string
	kubectlFailure  bool
	dockerFailure   bool
	xtrace          bool
	protectedOutput string
}

type imageAttestationFixtureResult struct {
	output string
	err    error
}

func extractImageAttestationHeredoc(t *testing.T, helpers string) string {
	t.Helper()
	const startMarker = `cat > "$FEATURE_E2E_HELPERS/attest-image" <<'EOF_ATTEST'` + "\n"
	start := strings.Index(helpers, startMarker)
	if start < 0 {
		t.Fatal("cluster helpers have no image attestation heredoc")
	}
	start += len(startMarker)
	end := strings.Index(helpers[start:], "\nEOF_ATTEST\n")
	if end < 0 {
		t.Fatal("cluster image attestation heredoc is not terminated")
	}
	return helpers[start : start+end]
}

func imageAttestationPods(t *testing.T, image string, imageIDs ...any) string {
	t.Helper()
	return imageAttestationPodsForContainer(t, imageAttestationManagerComponent, image, imageIDs...)
}

func imageAttestationPodsForContainer(
	t *testing.T, containerName, image string, imageIDs ...any,
) string {
	t.Helper()
	items := make([]any, 0, len(imageIDs))
	for i, imageID := range imageIDs {
		status := map[string]any{
			nameKey: containerName, imageKey: image, "ready": true, "restartCount": 0,
			"started": true, stateKey: map[string]any{"running": map[string]any{}},
		}
		if imageID != nil {
			status[imageIDKey] = imageID
		}
		items = append(items, map[string]any{
			metadataKey: map[string]any{nameKey: fmt.Sprintf("fixture-%d", i)},
			specKey: map[string]any{containersKey: []any{map[string]any{
				nameKey: containerName, imageKey: image,
			}}},
			statusKey: map[string]any{"containerStatuses": []any{status}},
		})
	}
	body, err := json.Marshal(map[string]any{
		apiVersionKey: "v1", kindKey: podListKind, itemsKey: items,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func imageAttestationPodLists(t *testing.T, lists ...string) string {
	t.Helper()
	items := make([]any, 0, len(lists))
	for _, body := range lists {
		var pods map[string]any
		if err := json.Unmarshal([]byte(body), &pods); err != nil {
			t.Fatal(err)
		}
		podItems, ok := pods[itemsKey].([]any)
		if !ok {
			t.Fatal("Pod fixture items is not an array")
		}
		items = append(items, podItems...)
	}
	body, err := json.Marshal(map[string]any{
		apiVersionKey: "v1", kindKey: podListKind, itemsKey: items,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func imageAttestationPodsWithSidecar(t *testing.T, image, imageID string) string {
	t.Helper()
	var pods map[string]any
	if err := json.Unmarshal([]byte(imageAttestationPods(t, image, imageID)), &pods); err != nil {
		t.Fatal(err)
	}
	pod := pods[itemsKey].([]any)[0].(map[string]any)
	spec := pod[specKey].(map[string]any)
	spec[containersKey] = append(spec[containersKey].([]any), map[string]any{
		nameKey: sidecarName, imageKey: "example.invalid/sidecar:fixture",
	})
	status := pod[statusKey].(map[string]any)
	status["containerStatuses"] = append(status["containerStatuses"].([]any), map[string]any{
		nameKey: sidecarName, imageKey: "example.invalid/sidecar:fixture",
		imageIDKey: "example.invalid/sidecar@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	body, err := json.Marshal(pods)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func imageAttestationPodsWithDuplicateTarget(t *testing.T, image, imageID string) string {
	t.Helper()
	var pods map[string]any
	if err := json.Unmarshal([]byte(imageAttestationPods(t, image, imageID)), &pods); err != nil {
		t.Fatal(err)
	}
	pod := pods[itemsKey].([]any)[0].(map[string]any)
	spec := pod[specKey].(map[string]any)
	spec[containersKey] = append(spec[containersKey].([]any), map[string]any{
		nameKey: imageAttestationManagerComponent, imageKey: image,
	})
	body, err := json.Marshal(pods)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func imageAttestationPodsWithObjectStatus(t *testing.T, image, imageID string) string {
	t.Helper()
	var pods map[string]any
	if err := json.Unmarshal([]byte(imageAttestationPods(t, image, imageID)), &pods); err != nil {
		t.Fatal(err)
	}
	pod := pods[itemsKey].([]any)[0].(map[string]any)
	status := pod[statusKey].(map[string]any)
	containerStatus := status["containerStatuses"].([]any)[0]
	status["containerStatuses"] = map[string]any{"fixture": containerStatus}
	body, err := json.Marshal(pods)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func runImageAttestationFixture(
	t *testing.T,
	attest string,
	fixture imageAttestationFixture,
) imageAttestationFixtureResult {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	attestPath := filepath.Join(dir, "attest-image")
	if err := os.WriteFile(attestPath, []byte(attest), 0o700); err != nil {
		t.Fatal(err)
	}
	ownerFile := filepath.Join(dir, "owner")
	if err := os.WriteFile(ownerFile, []byte(imageAttestationOwnerValue), 0o600); err != nil {
		t.Fatal(err)
	}
	ownerSelector := imageAttestationOwnerKey + "=" + imageAttestationOwnerValue
	expectedRef := fixture.managerRef
	kubectlResponse := fixture.pods
	expectedKubectlArgs := "get deployments,replicasets,pods -n " + featureControllerNS + " -l " +
		ownerSelector + ",app.kubernetes.io/instance=" + featureControllerName +
		",app.kubernetes.io/name=pgcopydb-operator -o json"
	if fixture.component == imageAttestationManagerComponent {
		kubectlResponse = managerCorrelationResourcesForPods(
			t, fixture.managerRef, fixture.runnerRef, fixture.pods,
		)
	}
	if fixture.component == imageAttestationRunnerComponent {
		expectedRef = fixture.runnerRef
		expectedKubectlArgs = "get pods -n pgcopydb-e2e -l " +
			"job-name=feature-e2e-runner-attest," + ownerSelector + " -o json"
	}
	kubectl := `#!/usr/bin/env bash
set -euo pipefail
[ "$*" = "$FAKE_EXPECTED_KUBECTL_ARGS" ] || exit 98
if [ "$FAKE_KUBECTL_FAILURE" = true ]; then
  printf '%s\n' "$FAKE_PROTECTED_OUTPUT" >&2
  exit 1
fi
printf '%s' "$FAKE_PODS"
`
	docker := `#!/usr/bin/env bash
set -euo pipefail
[ "$*" = "buildx imagetools inspect $FAKE_EXPECTED_REF --raw" ] || exit 98
if [ "$FAKE_DOCKER_FAILURE" = true ]; then
  printf '%s\n' "$FAKE_PROTECTED_OUTPUT" >&2
  exit 1
fi
printf '%s' "$FAKE_REGISTRY"
`
	for name, body := range map[string]string{kubectlCommand: kubectl, "docker": docker} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	args := append([]string{fixture.component}, fixture.extraArgs...)
	var cmd *exec.Cmd
	if fixture.xtrace {
		cmd = exec.Command("bash", "-c", `set -euo pipefail
set -x
export SHELLOPTS
"$ATTEST_PATH" "$FIXTURE_COMPONENT"`)
	} else {
		cmd = exec.Command(attestPath, args...)
	}
	cmd.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"E2E_OPERATOR_NAMESPACE="+featureControllerNS,
		"ATTEST_PATH="+attestPath,
		"FIXTURE_COMPONENT="+fixture.component,
		"FEATURE_E2E_OWNER_KEY="+imageAttestationOwnerKey,
		"FEATURE_E2E_OWNER_FILE="+ownerFile,
		"MANAGER_REF="+fixture.managerRef,
		"RUNNER_REF="+fixture.runnerRef,
		"FAKE_EXPECTED_REF="+expectedRef,
		"FAKE_EXPECTED_KUBECTL_ARGS="+expectedKubectlArgs,
		"FAKE_PODS="+kubectlResponse,
		"FAKE_REGISTRY="+fixture.registry,
		"FAKE_KUBECTL_FAILURE="+fmt.Sprint(fixture.kubectlFailure),
		"FAKE_DOCKER_FAILURE="+fmt.Sprint(fixture.dockerFailure),
		"FAKE_PROTECTED_OUTPUT="+fixture.protectedOutput,
		"REGISTRY_CREDENTIAL_SENTINEL=registry-credential-fixture",
		"PRIVATE_CONTEXT_SENTINEL=private-context-fixture",
		"PRIVATE_SECRET_SENTINEL=private-secret-fixture",
		"PRIVATE_NODE_SENTINEL=private-node-fixture",
	)
	output, err := cmd.CombinedOutput()
	return imageAttestationFixtureResult{output: string(output), err: err}
}

func TestFeatureE2EManagerCorrelation(t *testing.T) { //nolint:gocyclo // One table owns the trust boundary.
	wf := parseProtectedWorkflow(t, featureWorkflow)
	helpers := protectedStepNamed(t, wf.Jobs["cluster"], "Write cluster helpers").Run
	attest := extractImageAttestationHeredoc(t, helpers)
	helm := extractFeatureHelmHeredoc(t, helpers)
	ownership := extractFeatureGeneratedFile(t, helpers, "ownership", "OWNERSHIP")
	const (
		topDigest     = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
		amd64Digest   = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
		arm64Digest   = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
		expectedImage = "ghcr.io/ydixken/pgcopydb-operator"
	)
	managerRef := expectedImage + ":feature-0123456789abcdef0123456789abcdef01234567@" + topDigest
	runnerRef := expectedImage + "/runner:feature-0123456789abcdef0123456789abcdef01234567@" + topDigest
	registry := fmt.Sprintf(`{
  "schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"amd64","os":"linux"}},
    {"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":1,
      "platform":{"architecture":"arm64","os":"linux"}}
  ]
}`, amd64Digest, arm64Digest)

	tests := []struct {
		name        string
		mutate      func(*testing.T, map[string]any)
		wantSuccess bool
	}{
		{"one correlated Ready chain with unrelated resources", nil, true},
		{"missing List apiVersion", func(t *testing.T, document map[string]any) {
			delete(document, apiVersionKey)
		}, false},
		{"wrong List apiVersion", func(t *testing.T, document map[string]any) {
			document[apiVersionKey] = appsV1
		}, false},
		{"missing List kind", func(t *testing.T, document map[string]any) {
			delete(document, kindKey)
		}, false},
		{"wrong List kind", func(t *testing.T, document map[string]any) {
			document[kindKey] = podListKind
		}, false},
		{"missing List items", func(t *testing.T, document map[string]any) {
			delete(document, itemsKey)
		}, false},
		{"non-array List items", func(t *testing.T, document map[string]any) {
			document[itemsKey] = map[string]any{"resources": document[itemsKey]}
		}, false},
		{"expected image unready and wrong image Ready", func(t *testing.T, document map[string]any) {
			pod := managerCorrelationItem(t, document, podKind, 0)
			managerCorrelationReadyCondition(t, pod)[statusKey] = "False"
			replacement := cloneManagerCorrelationItem(t, pod)
			metadata := managerCorrelationMetadata(t, replacement)
			metadata[nameKey] = "pgcopydb-e2e-wrong-image"
			metadata[uidKey] = replacementUID
			managerCorrelationContainer(t, replacement)[imageKey] = wrongManagerImage
			status := managerCorrelationContainerStatus(t, replacement)
			status[imageKey] = wrongManagerImage
			status[imageIDKey] = "ghcr.io/fixture/wrong@" + amd64Digest
			managerCorrelationReadyCondition(t, replacement)[statusKey] = conditionTrueValue
			managerCorrelationAppend(t, document, replacement)
		}, false},
		{"zero Deployments", func(t *testing.T, document map[string]any) {
			managerCorrelationRemoveKind(t, document, deploymentKind)
		}, false},
		{"multiple Deployments", func(t *testing.T, document map[string]any) {
			deployment := cloneManagerCorrelationItem(t, managerCorrelationItem(t, document, deploymentKind, 0))
			managerCorrelationAppend(t, document, deployment)
			deployment = managerCorrelationItem(t, document, deploymentKind, 1)
			metadata := managerCorrelationMetadata(t, deployment)
			metadata[nameKey] = "pgcopydb-e2e-shadow"
			metadata[uidKey] = replacementUID
		}, false},
		{"zero ReplicaSets", func(t *testing.T, document map[string]any) {
			managerCorrelationRemoveKind(t, document, replicaSetKind)
		}, false},
		{"multiple ReplicaSets", func(t *testing.T, document map[string]any) {
			replicaSet := cloneManagerCorrelationItem(t, managerCorrelationItem(t, document, replicaSetKind, 0))
			metadata := managerCorrelationMetadata(t, replicaSet)
			metadata[nameKey] = "pgcopydb-e2e-shadow-7d4c"
			metadata[uidKey] = replacementUID
			managerCorrelationAppend(t, document, replicaSet)
		}, false},
		{"zero Pods", func(t *testing.T, document map[string]any) {
			managerCorrelationRemoveKind(t, document, podKind)
		}, false},
		{"multiple Pods", func(t *testing.T, document map[string]any) {
			pod := cloneManagerCorrelationItem(t, managerCorrelationItem(t, document, podKind, 0))
			metadata := managerCorrelationMetadata(t, pod)
			metadata[nameKey] = "pgcopydb-e2e-7d4c-second"
			metadata[uidKey] = replacementUID
			managerCorrelationAppend(t, document, pod)
		}, false},
		{"missing Deployment UID", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationMetadata(t, managerCorrelationItem(t, document, deploymentKind, 0)), uidKey)
		}, false},
		{"malformed Deployment UID", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, deploymentKind, 0))[uidKey] = 42
		}, false},
		{"missing ReplicaSet UID", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationMetadata(t, managerCorrelationItem(t, document, replicaSetKind, 0)), uidKey)
		}, false},
		{"malformed ReplicaSet UID", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, replicaSetKind, 0))[uidKey] = replacementName
		}, false},
		{"missing Pod UID", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationMetadata(t, managerCorrelationItem(t, document, podKind, 0)), uidKey)
		}, false},
		{"malformed Pod UID", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, podKind, 0))[uidKey] = []any{}
		}, false},
		{"all resource UIDs collide", func(t *testing.T, document map[string]any) {
			deploymentUID := managerCorrelationMetadata(t,
				managerCorrelationItem(t, document, deploymentKind, 0))[uidKey]
			managerCorrelationMetadata(t,
				managerCorrelationItem(t, document, replicaSetKind, 0))[uidKey] = deploymentUID
			pod := managerCorrelationItem(t, document, podKind, 0)
			managerCorrelationMetadata(t, pod)[uidKey] = deploymentUID
			managerCorrelationOwner(t, pod)[uidKey] = deploymentUID
		}, false},
		{"Deployment and ReplicaSet UIDs collide", func(t *testing.T, document map[string]any) {
			deploymentUID := managerCorrelationMetadata(t,
				managerCorrelationItem(t, document, deploymentKind, 0))[uidKey]
			managerCorrelationMetadata(t,
				managerCorrelationItem(t, document, replicaSetKind, 0))[uidKey] = deploymentUID
			managerCorrelationOwner(t,
				managerCorrelationItem(t, document, podKind, 0))[uidKey] = deploymentUID
		}, false},
		{"Deployment and Pod UIDs collide", func(t *testing.T, document map[string]any) {
			deploymentUID := managerCorrelationMetadata(t,
				managerCorrelationItem(t, document, deploymentKind, 0))[uidKey]
			managerCorrelationMetadata(t,
				managerCorrelationItem(t, document, podKind, 0))[uidKey] = deploymentUID
		}, false},
		{"ReplicaSet and Pod UIDs collide", func(t *testing.T, document map[string]any) {
			replicaSetUID := managerCorrelationMetadata(t,
				managerCorrelationItem(t, document, replicaSetKind, 0))[uidKey]
			managerCorrelationMetadata(t,
				managerCorrelationItem(t, document, podKind, 0))[uidKey] = replicaSetUID
		}, false},
		{"ReplicaSet owner missing UID", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationOwner(t, managerCorrelationItem(t, document, replicaSetKind, 0)), uidKey)
		}, false},
		{"ReplicaSet owner wrong UID", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, replicaSetKind, 0))[uidKey] =
				replacementUID
		}, false},
		{"ReplicaSet owner missing apiVersion", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationOwner(t, managerCorrelationItem(t, document, replicaSetKind, 0)), apiVersionKey)
		}, false},
		{"ReplicaSet owner wrong apiVersion", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, replicaSetKind, 0))[apiVersionKey] = "v1"
		}, false},
		{"ReplicaSet owner wrong kind", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, replicaSetKind, 0))[kindKey] = "StatefulSet"
		}, false},
		{"ReplicaSet owner wrong name", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, replicaSetKind, 0))[nameKey] = replacementName
		}, false},
		{"ReplicaSet owner missing controller flag", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationOwner(t, managerCorrelationItem(t, document, replicaSetKind, 0)), controllerKey)
		}, false},
		{"ReplicaSet owner false controller flag", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, replicaSetKind, 0))[controllerKey] = false
		}, false},
		{"ReplicaSet ambiguous controlling owners", func(t *testing.T, document map[string]any) {
			replicaSet := managerCorrelationItem(t, document, replicaSetKind, 0)
			managerCorrelationAppendOwner(t, replicaSet, cloneManagerCorrelationItem(t,
				managerCorrelationOwner(t, replicaSet)))
		}, false},
		{"Pod owner missing UID", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationOwner(t, managerCorrelationItem(t, document, podKind, 0)), uidKey)
		}, false},
		{"Pod owner wrong UID", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, podKind, 0))[uidKey] =
				replacementUID
		}, false},
		{"Pod owner missing apiVersion", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationOwner(t, managerCorrelationItem(t, document, podKind, 0)), apiVersionKey)
		}, false},
		{"Pod owner wrong apiVersion", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, podKind, 0))[apiVersionKey] = "v1"
		}, false},
		{"Pod owner wrong kind", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, podKind, 0))[kindKey] = deploymentKind
		}, false},
		{"Pod owner wrong name", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, podKind, 0))[nameKey] = replacementName
		}, false},
		{"Pod owner missing controller flag", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationOwner(t, managerCorrelationItem(t, document, podKind, 0)), controllerKey)
		}, false},
		{"Pod owner string controller flag", func(t *testing.T, document map[string]any) {
			managerCorrelationOwner(t, managerCorrelationItem(t, document, podKind, 0))[controllerKey] = "true"
		}, false},
		{"Pod ambiguous controlling owners", func(t *testing.T, document map[string]any) {
			pod := managerCorrelationItem(t, document, podKind, 0)
			managerCorrelationAppendOwner(t, pod, cloneManagerCorrelationItem(t, managerCorrelationOwner(t, pod)))
		}, false},
		{"replacement Deployment UID", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, deploymentKind, 0))[uidKey] =
				replacementUID
		}, false},
		{"wrong runner argument", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, deploymentKind, 0))["args"] =
				[]any{leaderElectArg, "--runner-image=ghcr.io/fixture/wrong@" + topDigest}
		}, false},
		{"missing runner argument", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, deploymentKind, 0))["args"] =
				[]any{leaderElectArg}
		}, false},
		{"duplicate runner argument", func(t *testing.T, document map[string]any) {
			container := managerCorrelationContainer(t, managerCorrelationItem(t, document, deploymentKind, 0))
			args := container["args"].([]any)
			container["args"] = append(args, "--runner-image="+runnerRef)
		}, false},
		{"correct and wrong runner arguments", func(t *testing.T, document map[string]any) {
			container := managerCorrelationContainer(t, managerCorrelationItem(t, document, deploymentKind, 0))
			args := container["args"].([]any)
			container["args"] = append(args, "--runner-image=ghcr.io/fixture/wrong@"+topDigest)
		}, false},
		{"missing ReplicaSet runner argument", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, replicaSetKind, 0))["args"] =
				[]any{leaderElectArg}
		}, false},
		{"wrong ReplicaSet runner argument", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, replicaSetKind, 0))["args"] =
				[]any{leaderElectArg, "--runner-image=ghcr.io/fixture/wrong@" + topDigest}
		}, false},
		{"duplicate ReplicaSet runner argument", func(t *testing.T, document map[string]any) {
			container := managerCorrelationContainer(t, managerCorrelationItem(t, document, replicaSetKind, 0))
			container["args"] = append(container["args"].([]any), "--runner-image="+runnerRef)
		}, false},
		{"missing Pod runner argument", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, podKind, 0))["args"] =
				[]any{leaderElectArg}
		}, false},
		{"wrong Pod runner argument", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, podKind, 0))["args"] =
				[]any{leaderElectArg, "--runner-image=ghcr.io/fixture/wrong@" + topDigest}
		}, false},
		{"duplicate Pod runner argument", func(t *testing.T, document map[string]any) {
			container := managerCorrelationContainer(t, managerCorrelationItem(t, document, podKind, 0))
			container["args"] = append(container["args"].([]any), "--runner-image="+runnerRef)
		}, false},
		{"Deployment manager image divergence", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, deploymentKind, 0))[imageKey] =
				wrongManagerImage
		}, false},
		{"ReplicaSet manager image divergence", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, replicaSetKind, 0))[imageKey] =
				wrongManagerImage
		}, false},
		{"Pod manager image divergence", func(t *testing.T, document map[string]any) {
			managerCorrelationContainer(t, managerCorrelationItem(t, document, podKind, 0))[imageKey] =
				wrongManagerImage
		}, false},
		{"missing manager container", func(t *testing.T, document map[string]any) {
			managerCorrelationPodSpec(t, managerCorrelationItem(t, document, deploymentKind, 0))[containersKey] =
				[]any{map[string]any{nameKey: sidecarName, imageKey: "example.invalid/sidecar:latest"}}
		}, false},
		{"duplicate manager container", func(t *testing.T, document map[string]any) {
			deployment := managerCorrelationItem(t, document, deploymentKind, 0)
			spec := managerCorrelationPodSpec(t, deployment)
			container := cloneManagerCorrelationItem(t, managerCorrelationContainer(t, deployment))
			spec[containersKey] = append(spec[containersKey].([]any), container)
		}, false},
		{"manager is not Ready", func(t *testing.T, document map[string]any) {
			managerCorrelationReadyCondition(t, managerCorrelationItem(t, document, podKind, 0))[statusKey] = "False"
		}, false},
		{"ambiguous Ready conditions", func(t *testing.T, document map[string]any) {
			pod := managerCorrelationItem(t, document, podKind, 0)
			conditions := managerCorrelationConditions(t, pod)
			pod[statusKey].(map[string]any)["conditions"] = append(conditions,
				map[string]any{schemaTypeKey: readyConditionType, statusKey: conditionTrueValue})
		}, false},
		{"wrong Deployment namespace", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, deploymentKind, 0))[namespaceKey] =
				replacementName
		}, false},
		{"wrong Deployment name", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, deploymentKind, 0))[nameKey] =
				replacementName
		}, false},
		{"wrong controller instance", func(t *testing.T, document map[string]any) {
			labels := managerCorrelationLabels(t, managerCorrelationItem(t, document, deploymentKind, 0))
			labels["app.kubernetes.io/instance"] = replacementName
		}, false},
		{"wrong controller name", func(t *testing.T, document map[string]any) {
			labels := managerCorrelationLabels(t, managerCorrelationItem(t, document, deploymentKind, 0))
			labels["app.kubernetes.io/name"] = replacementName
		}, false},
		{"wrong ReplicaSet namespace", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, replicaSetKind, 0))[namespaceKey] =
				replacementName
		}, false},
		{"missing ReplicaSet instance label", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationLabels(t, managerCorrelationItem(t, document, replicaSetKind, 0)),
				"app.kubernetes.io/instance")
		}, false},
		{"wrong ReplicaSet instance label", func(t *testing.T, document map[string]any) {
			labels := managerCorrelationLabels(t, managerCorrelationItem(t, document, replicaSetKind, 0))
			labels["app.kubernetes.io/instance"] = replacementName
		}, false},
		{"missing ReplicaSet application label", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationLabels(t, managerCorrelationItem(t, document, replicaSetKind, 0)),
				"app.kubernetes.io/name")
		}, false},
		{"wrong ReplicaSet application label", func(t *testing.T, document map[string]any) {
			labels := managerCorrelationLabels(t, managerCorrelationItem(t, document, replicaSetKind, 0))
			labels["app.kubernetes.io/name"] = replacementName
		}, false},
		{"arbitrary ReplicaSet name", func(t *testing.T, document map[string]any) {
			replicaSetName := "unrelated-replica-set"
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, replicaSetKind, 0))[nameKey] = replicaSetName
			pod := managerCorrelationItem(t, document, podKind, 0)
			managerCorrelationOwner(t, pod)[nameKey] = replicaSetName
			managerCorrelationMetadata(t, pod)[nameKey] = replicaSetName + "-first"
		}, false},
		{"ReplicaSet name missing generated suffix", func(t *testing.T, document map[string]any) {
			metadata := managerCorrelationMetadata(t, managerCorrelationItem(t, document, replicaSetKind, 0))
			metadata[nameKey] = featureControllerName
			pod := managerCorrelationItem(t, document, podKind, 0)
			managerCorrelationOwner(t, pod)[nameKey] = featureControllerName
			managerCorrelationMetadata(t, pod)[nameKey] = featureControllerName + "-first"
		}, false},
		{"wrong Pod namespace", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, podKind, 0))[namespaceKey] =
				replacementName
		}, false},
		{"missing Pod instance label", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationLabels(t, managerCorrelationItem(t, document, podKind, 0)),
				"app.kubernetes.io/instance")
		}, false},
		{"wrong Pod instance label", func(t *testing.T, document map[string]any) {
			labels := managerCorrelationLabels(t, managerCorrelationItem(t, document, podKind, 0))
			labels["app.kubernetes.io/instance"] = replacementName
		}, false},
		{"missing Pod application label", func(t *testing.T, document map[string]any) {
			delete(managerCorrelationLabels(t, managerCorrelationItem(t, document, podKind, 0)),
				"app.kubernetes.io/name")
		}, false},
		{"wrong Pod application label", func(t *testing.T, document map[string]any) {
			labels := managerCorrelationLabels(t, managerCorrelationItem(t, document, podKind, 0))
			labels["app.kubernetes.io/name"] = replacementName
		}, false},
		{"inconsistent Pod name", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, podKind, 0))[nameKey] =
				"pgcopydb-e2e-other-first"
		}, false},
		{"Pod name missing generated suffix", func(t *testing.T, document map[string]any) {
			managerCorrelationMetadata(t, managerCorrelationItem(t, document, podKind, 0))[nameKey] =
				featureReplicaSetName
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			document := managerCorrelationDocument(managerRef, runnerRef, expectedImage+"@"+amd64Digest)
			if tt.mutate != nil {
				tt.mutate(t, document)
			}
			resources := marshalManagerCorrelationDocument(t, document)
			result := runManagerCorrelationFixture(
				t, helm, ownership, attest, managerRef, runnerRef, registry, resources,
			)
			requireFeatureHelmArgs(t, result.helmArgs, nil)
			if (result.err == nil) != tt.wantSuccess {
				t.Errorf("manager correlation error = %v, output:\n%s", result.err, result.output)
			}
			if result.attested != tt.wantSuccess {
				t.Errorf("manager attested = %t, want %t", result.attested, tt.wantSuccess)
			}
			if result.resources != resources {
				t.Error("manager attestation changed fake API resources")
			}
		})
	}

	resources := marshalManagerCorrelationDocument(t,
		managerCorrelationDocument(managerRef, runnerRef, expectedImage+"@"+amd64Digest))
	rejectedHelmArgs := []struct {
		name string
		args []string
	}{
		{"reserved set key", []string{helmSetFlag, "fullnameOverride=candidate-controller"}},
		{"reserved set equals key", []string{"--set=fullnameOverride=candidate-controller"}},
		{"escaped reserved set key", []string{helmSetFlag, `full\nameOverride=candidate-controller`}},
		{"escaped reserved set equals key", []string{`--set=full\nameOverride=candidate-controller`}},
		{"reserved set-string key", []string{helmSetStringFlag, "fullnameOverride=candidate-controller"}},
		{"reserved set-string equals key", []string{"--set-string=fullnameOverride=candidate-controller"}},
		{"escaped reserved set-string key", []string{helmSetStringFlag, `full\nameOverride=candidate-controller`}},
		{"escaped reserved set-string equals key", []string{`--set-string=full\nameOverride=candidate-controller`}},
		{"reserved set-json key", []string{helmSetJSONFlag, `fullnameOverride="candidate-controller"`}},
		{"reserved set-json equals key", []string{`--set-json=fullnameOverride="candidate-controller"`}},
		{"escaped reserved set-json key", []string{helmSetJSONFlag, `full\nameOverride="candidate-controller"`}},
		{"escaped reserved set-json equals key", []string{`--set-json=full\nameOverride="candidate-controller"`}},
		{"set-file separate flag", []string{"--set-file", `full\nameOverride=/dev/stdin`}},
		{"set-file equals flag", []string{`--set-file=full\nameOverride=/dev/stdin`}},
		{"option terminator", []string{"--"}},
		{"post-renderer separate flag", []string{helmPostRendererFlag, "candidate-renderer"}},
		{"post-renderer equals flag", []string{"--post-renderer=candidate-renderer"}},
		{"post-renderer-args separate flag", []string{"--post-renderer-args", "candidate-argument"}},
		{"post-renderer-args equals flag", []string{"--post-renderer-args=candidate-argument"}},
		{"kube-context separate flag", []string{"--kube-context", "candidate-context"}},
		{"kube-context equals flag", []string{"--kube-context=candidate-context"}},
		{"short values missing value", []string{"-f"}},
		{"short values empty equals value", []string{"-f="}},
		{"values missing value", []string{"--values"}},
		{"values empty equals value", []string{"--values="}},
		{"set missing value", []string{helmSetFlag}},
		{"set empty equals value", []string{"--set="}},
		{"set malformed value", []string{helmSetFlag, "leaderElection.enabled"}},
		{"set-string missing value", []string{helmSetStringFlag}},
		{"set-string empty equals value", []string{"--set-string="}},
		{"set-string malformed value", []string{helmSetStringFlag, "runner.image.tag"}},
		{"set-json missing value", []string{helmSetJSONFlag}},
		{"set-json empty equals value", []string{"--set-json="}},
		{"set-json malformed value", []string{helmSetJSONFlag, "watchNamespaces"}},
		{"safe flag followed by option", []string{helmSetFlag, "--values=fixture-values.yaml"}},
		{"short values followed by option", []string{"-f", "--values=fixture-values.yaml"}},
	}
	for _, tt := range rejectedHelmArgs {
		t.Run(tt.name+" is rejected before Helm", func(t *testing.T) {
			result := runManagerCorrelationFixture(
				t, helm, ownership, attest, managerRef, runnerRef, registry, resources, tt.args...,
			)
			if result.err == nil {
				t.Error("candidate Helm argument unexpectedly succeeded")
			}
			if len(result.helmArgs) != 0 {
				t.Errorf("rejected candidate reached Helm with arguments %q", result.helmArgs)
			}
			if result.attested {
				t.Error("rejected candidate reached manager attestation")
			}
			if result.resources != resources {
				t.Error("rejected candidate changed fake API resources")
			}
		})
	}

	valuesPath := filepath.Join(t.TempDir(), "candidate-values.yaml")
	if err := os.WriteFile(valuesPath, []byte("fullnameOverride: candidate-controller\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	safeHelmArgs := []struct {
		name          string
		args          []string
		requireRender bool
	}{
		{"short values separate flag", []string{"-f", valuesPath}, true},
		{"short values equals flag", []string{"-f=" + valuesPath}, true},
		{"values separate flag", []string{"--values", valuesPath}, false},
		{"values equals flag", []string{"--values=" + valuesPath}, false},
		{"set separate flag", []string{helmSetFlag, "leaderElection.enabled=false"}, false},
		{"set equals flag", []string{"--set=leaderElection.enabled=false"}, false},
		{"set-string separate flag", []string{helmSetStringFlag, "runner.image.tag=fixture"}, false},
		{"set-string equals flag", []string{"--set-string=runner.image.tag=fixture"}, false},
		{"set-json separate flag", []string{helmSetJSONFlag, `watchNamespaces=["pgcopydb-e2e"]`}, false},
		{"set-json equals flag", []string{`--set-json=watchNamespaces=["pgcopydb-e2e"]`}, false},
	}
	for _, tt := range safeHelmArgs {
		t.Run(tt.name+" preserves the trusted name on retries", func(t *testing.T) {
			for range 2 {
				result := runManagerCorrelationFixture(
					t, helm, ownership, attest, managerRef, runnerRef, registry, resources, tt.args...,
				)
				if result.err != nil {
					t.Errorf("safe candidate Helm argument failed: %v\n%s", result.err, result.output)
				}
				requireFeatureHelmArgs(t, result.helmArgs, tt.args)
				if !result.attested {
					t.Error("safe candidate did not reach manager attestation")
				}
				if result.resources != resources {
					t.Error("safe candidate changed fake API resources")
				}
				if tt.requireRender {
					requireTrustedFeatureHelmRender(t, result.helmArgs)
				}
			}
		})
	}
}

func requireTrustedFeatureHelmRender(t *testing.T, helmArgs []string) {
	t.Helper()
	helmPath, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is unavailable")
	}
	trustedSuffix := 4
	templateArgs := append([]string{helmTemplateCommand}, helmArgs[1:5]...)
	templateArgs = append(templateArgs, helmArgs[6:len(helmArgs)-trustedSuffix]...)
	output, err := exec.Command(helmPath, templateArgs...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if document[kindKey] != deploymentKind {
			continue
		}
		metadata, _ := document[metadataKey].(map[string]any)
		if metadata[nameKey] != featureControllerName {
			t.Errorf("rendered Deployment name = %v, want %s", metadata[nameKey], featureControllerName)
		}
		return
	}
	t.Fatal("helm template rendered no Deployment")
}

func TestFeatureE2EPostRendererRunsWithHelm4(t *testing.T) {
	helmPath, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is unavailable")
	}
	version, err := exec.Command(helmPath, "version", "--template", "{{.Version}}").CombinedOutput()
	if err != nil {
		t.Fatalf("read Helm version: %v\n%s", err, version)
	}
	if !strings.HasPrefix(strings.TrimSpace(string(version)), "v4.") {
		t.Skipf("Helm 4 is required for this integration test, found %s", strings.TrimSpace(string(version)))
	}
	if _, err := exec.LookPath(kubectlCommand); err != nil {
		t.Skip("kubectl is unavailable")
	}

	wf := parseProtectedWorkflow(t, featureWorkflow)
	helpers := protectedStepNamed(t, wf.Jobs["cluster"], "Write cluster helpers").Run
	plugin := extractFeatureGeneratedFile(t, helpers,
		"plugins/"+featurePostRendererPlugin+"/plugin.yaml", "HELM_PLUGIN")
	postRenderer := extractFeatureGeneratedFile(t, helpers, "post-renderer", "POST_RENDERER")
	renderSafety := buildFeatureRenderSafety(t,
		extractFeatureGeneratedFile(t, helpers, "render-safety.go", "RENDER_SAFETY"))
	helmWrapper := extractFeatureHelmHeredoc(t, helpers)
	if !strings.Contains(helmWrapper,
		`export HELM_PLUGINS="$FEATURE_E2E_HELPERS/plugins"`) {
		t.Fatal("feature Helm wrapper does not scope HELM_PLUGINS to the run-owned plugin directory")
	}
	if !strings.Contains(helpers,
		`ln -s ../../post-renderer "$FEATURE_E2E_HELPERS/plugins/feature-e2e-postrenderer/post-renderer"`) {
		t.Fatal("cluster helpers do not register the trusted post-renderer executable")
	}
	if strings.Contains(helpers, `printf 'HELM_PLUGINS=`) {
		t.Fatal("cluster helpers export HELM_PLUGINS beyond the Helm wrapper operation")
	}

	dir := t.TempDir()
	pluginRoot := filepath.Join(dir, "plugins")
	pluginDir := filepath.Join(pluginRoot, featurePostRendererPlugin)
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for path, file := range map[string]struct {
		body string
		mode os.FileMode
	}{
		filepath.Join(dir, "owner"):             {imageAttestationOwnerValue, 0o600},
		filepath.Join(dir, "post-renderer"):     {postRenderer, 0o700},
		filepath.Join(pluginDir, "plugin.yaml"): {plugin, 0o600},
	} {
		if err := os.WriteFile(path, []byte(file.body), file.mode); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(renderSafety, filepath.Join(dir, "bin", "render-safety")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../post-renderer", filepath.Join(pluginDir, "post-renderer")); err != nil {
		t.Fatal(err)
	}

	args := []string{
		helmTemplateCommand, featureControllerName, featureFixtureChart,
		"--namespace", featureControllerNS,
		helmSetFlag, "crds.install=false",
		helmSetFlag, "rbac.create=false",
		helmSetFlag, "serviceAccount.create=false",
		helmSetFlag, "serviceAccount.name=pgcopydb-e2e-manager",
		helmSetFlag, "leaderElection.enabled=false",
		helmSetStringFlag, "fullnameOverride=" + featureControllerName,
		helmPostRendererFlag, featurePostRendererPlugin,
	}
	cmd := exec.Command(helmPath, args...)
	cmd.Env = append(os.Environ(),
		"HELM_PLUGINS="+pluginRoot,
		"FEATURE_E2E_HELPERS="+dir,
		"FEATURE_E2E_OWNER_KEY="+imageAttestationOwnerKey,
		"FEATURE_E2E_OWNER_FILE="+filepath.Join(dir, "owner"),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Helm 4 post-renderer plugin failed: %v\n%s", err, output)
	}

	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
	objects := 0
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if len(document) == 0 {
			continue
		}
		metadata, ok := document[metadataKey].(map[string]any)
		if !ok {
			t.Fatalf("rendered %v has no metadata", document[kindKey])
		}
		labels, ok := metadata[labelsKey].(map[string]any)
		if !ok || labels[imageAttestationOwnerKey] != imageAttestationOwnerValue {
			t.Fatalf("rendered %v is not owned by the feature run", document[kindKey])
		}
		objects++
	}
	if objects == 0 {
		t.Fatal("Helm 4 post-renderer plugin rendered no objects")
	}
}

func requireFeatureHelmArgs(t *testing.T, got, candidate []string) {
	t.Helper()
	want := make([]string, 0, 6+len(candidate)+5)
	want = append(want,
		helmInstallCommand, featureControllerName, featureFixtureChart,
		"-n", featureControllerNS, "--wait",
	)
	want = append(want, candidate...)
	want = append(want,
		helmSetStringFlag, "fullnameOverride="+featureControllerName,
		"--rollback-on-failure", "--timeout=5m", helmPostRendererFlag, featurePostRendererPlugin,
	)
	if !slices.Equal(got, want) {
		t.Errorf("effective Helm arguments = %q, want trusted suffix after %q", got, want)
	}
}

func extractFeatureHelmHeredoc(t *testing.T, helpers string) string {
	t.Helper()
	const startMarker = `cat > "$FEATURE_E2E_HELPERS/bin/helm" <<'EOF_HELM'` + "\n"
	start := strings.Index(helpers, startMarker)
	if start < 0 {
		t.Fatal("cluster helpers have no Helm wrapper heredoc")
	}
	start += len(startMarker)
	end := strings.Index(helpers[start:], "\nEOF_HELM\n")
	if end < 0 {
		t.Fatal("cluster Helm wrapper heredoc is not terminated")
	}
	return helpers[start : start+end]
}

func managerCorrelationDocument(managerRef, runnerRef, imageID string) map[string]any {
	const (
		deploymentUID = "11111111-1111-4111-8111-111111111111"
		replicaSetUID = "22222222-2222-4222-8222-222222222222"
		ownerKey      = "pgcopydb-operator.io/feature-e2e-run"
		ownerValue    = "0123456789abcdef0123456789abcdef"
	)
	labels := func(owner string) map[string]any {
		return map[string]any{
			"app.kubernetes.io/instance": featureControllerName,
			"app.kubernetes.io/name":     "pgcopydb-operator",
			ownerKey:                     owner,
		}
	}
	managerContainer := func() map[string]any {
		return map[string]any{
			nameKey:  imageAttestationManagerComponent,
			imageKey: managerRef,
			"args":   []any{"--health-probe-bind-address=:8081", leaderElectArg, "--runner-image=" + runnerRef},
		}
	}
	deployment := map[string]any{
		apiVersionKey: appsV1, kindKey: deploymentKind,
		metadataKey: map[string]any{
			nameKey: featureControllerName, namespaceKey: featureControllerNS, uidKey: deploymentUID,
			labelsKey: labels(ownerValue), resourceVersionKey: "101",
			"annotations": map[string]any{
				"meta.helm.sh/release-name":      featureControllerName,
				"meta.helm.sh/release-namespace": featureControllerNS,
			},
		},
		specKey: map[string]any{
			replicasKey: 1,
			"selector":  map[string]any{"matchLabels": labels(ownerValue)},
			helmTemplateCommand: map[string]any{
				metadataKey: map[string]any{labelsKey: labels(ownerValue)},
				specKey:     map[string]any{containersKey: []any{managerContainer()}},
			},
		},
		statusKey: map[string]any{"availableReplicas": 1, "readyReplicas": 1, replicasKey: 1},
	}
	replicaSet := map[string]any{
		apiVersionKey: appsV1, kindKey: replicaSetKind,
		metadataKey: map[string]any{
			nameKey: featureReplicaSetName, namespaceKey: featureControllerNS, uidKey: replicaSetUID,
			labelsKey: labels(ownerValue), resourceVersionKey: "102",
			"ownerReferences": []any{map[string]any{
				apiVersionKey: appsV1, kindKey: deploymentKind, nameKey: featureControllerName,
				uidKey: deploymentUID, controllerKey: true, blockOwnerDeletionKey: true,
			}},
		},
		specKey: map[string]any{
			replicasKey: 1,
			"selector":  map[string]any{"matchLabels": labels(ownerValue)},
			helmTemplateCommand: map[string]any{
				metadataKey: map[string]any{labelsKey: labels(ownerValue)},
				specKey:     map[string]any{containersKey: []any{managerContainer()}},
			},
		},
		statusKey: map[string]any{"availableReplicas": 1, "readyReplicas": 1, replicasKey: 1},
	}
	pod := map[string]any{
		apiVersionKey: "v1", kindKey: podKind,
		metadataKey: map[string]any{
			nameKey: "pgcopydb-e2e-7d4c-first", namespaceKey: featureControllerNS,
			uidKey: "33333333-3333-4333-8333-333333333333", labelsKey: labels(ownerValue),
			resourceVersionKey: "103",
			"ownerReferences": []any{map[string]any{
				apiVersionKey: appsV1, kindKey: replicaSetKind, nameKey: featureReplicaSetName,
				uidKey: replicaSetUID, controllerKey: true, blockOwnerDeletionKey: true,
			}},
		},
		specKey: map[string]any{containersKey: []any{managerContainer()}},
		statusKey: map[string]any{
			"phase": "Running",
			"conditions": []any{
				map[string]any{schemaTypeKey: "PodScheduled", statusKey: conditionTrueValue},
				map[string]any{schemaTypeKey: readyConditionType, statusKey: conditionTrueValue},
			},
			"containerStatuses": []any{map[string]any{
				nameKey: imageAttestationManagerComponent, imageKey: managerRef, imageIDKey: imageID,
				"ready": true, "restartCount": 0, "started": true,
				stateKey: map[string]any{"running": map[string]any{}},
			}},
		},
	}
	unrelatedLabels := labels("someone-else")
	unrelated := []any{
		map[string]any{
			apiVersionKey: appsV1, kindKey: deploymentKind,
			metadataKey: map[string]any{nameKey: "customer", namespaceKey: featureControllerNS,
				uidKey: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", labelsKey: unrelatedLabels},
		},
		map[string]any{
			apiVersionKey: appsV1, kindKey: replicaSetKind,
			metadataKey: map[string]any{nameKey: "customer-7d4c", namespaceKey: featureControllerNS,
				uidKey: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", labelsKey: unrelatedLabels},
		},
		map[string]any{
			apiVersionKey: "v1", kindKey: podKind,
			metadataKey: map[string]any{nameKey: "customer-first", namespaceKey: featureControllerNS,
				uidKey: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", labelsKey: unrelatedLabels},
		},
	}
	return map[string]any{
		apiVersionKey: "v1", kindKey: listKind,
		itemsKey:    append([]any{deployment, replicaSet, pod}, unrelated...),
		metadataKey: map[string]any{resourceVersionKey: "103"},
	}
}

func managerCorrelationResourcesForPods(
	t *testing.T, managerRef, runnerRef, pods string,
) string {
	t.Helper()
	if pods == "" {
		pods = imageAttestationPods(t, managerRef)
	}
	var podList map[string]any
	if err := json.Unmarshal([]byte(pods), &podList); err != nil {
		t.Fatal(err)
	}
	podItems, ok := podList[itemsKey].([]any)
	if !ok {
		t.Fatal("image attestation Pod fixture items is not an array")
	}
	document := managerCorrelationDocument(managerRef, runnerRef,
		"ghcr.io/ydixken/pgcopydb-operator@sha256:2222222222222222222222222222222222222222222222222222222222222222")
	managerCorrelationRemoveKind(t, document, podKind)
	for i, value := range podItems {
		pod, ok := value.(map[string]any)
		if !ok {
			t.Fatal("image attestation Pod fixture item is not an object")
		}
		metadata, ok := pod[metadataKey].(map[string]any)
		if !ok {
			metadata = map[string]any{}
			pod[metadataKey] = metadata
		}
		metadata[nameKey] = fmt.Sprintf("pgcopydb-e2e-7d4c-%d", i)
		metadata[namespaceKey] = featureControllerNS
		metadata[uidKey] = fmt.Sprintf("%08x-3333-4333-8333-333333333333", i+1)
		metadata[labelsKey] = map[string]any{
			"app.kubernetes.io/instance": featureControllerName,
			"app.kubernetes.io/name":     "pgcopydb-operator",
			imageAttestationOwnerKey:     imageAttestationOwnerValue,
		}
		metadata["ownerReferences"] = []any{map[string]any{
			apiVersionKey: appsV1, kindKey: replicaSetKind, nameKey: featureReplicaSetName,
			uidKey: "22222222-2222-4222-8222-222222222222", controllerKey: true,
			blockOwnerDeletionKey: true,
		}}
		pod[apiVersionKey] = "v1"
		pod[kindKey] = podKind
		for _, value := range managerCorrelationPodSpec(t, pod)[containersKey].([]any) {
			container := value.(map[string]any)
			if container[nameKey] == imageAttestationManagerComponent {
				container["args"] = []any{leaderElectArg, "--runner-image=" + runnerRef}
			}
		}
		status, ok := pod[statusKey].(map[string]any)
		if !ok {
			status = map[string]any{}
			pod[statusKey] = status
		}
		status["phase"] = "Running"
		status["conditions"] = []any{
			map[string]any{schemaTypeKey: "PodScheduled", statusKey: conditionTrueValue},
			map[string]any{schemaTypeKey: readyConditionType, statusKey: conditionTrueValue},
		}
		managerCorrelationAppend(t, document, pod)
	}
	selected := make([]any, 0, len(document[itemsKey].([]any)))
	for _, value := range document[itemsKey].([]any) {
		item := value.(map[string]any)
		if managerCorrelationLabels(t, item)[imageAttestationOwnerKey] == imageAttestationOwnerValue {
			selected = append(selected, item)
		}
	}
	document[itemsKey] = selected
	return marshalManagerCorrelationDocument(t, document)
}

func managerCorrelationItem(t *testing.T, document map[string]any, kind string, ordinal int) map[string]any {
	t.Helper()
	for _, value := range document[itemsKey].([]any) {
		item := value.(map[string]any)
		if item[kindKey] == kind {
			if ordinal == 0 {
				return item
			}
			ordinal--
		}
	}
	t.Fatalf("manager correlation fixture has no %s at ordinal %d", kind, ordinal)
	return nil
}

func managerCorrelationMetadata(t *testing.T, item map[string]any) map[string]any {
	t.Helper()
	metadata, ok := item[metadataKey].(map[string]any)
	if !ok {
		t.Fatal("manager correlation fixture metadata is not an object")
	}
	return metadata
}

func managerCorrelationLabels(t *testing.T, item map[string]any) map[string]any {
	t.Helper()
	labels, ok := managerCorrelationMetadata(t, item)[labelsKey].(map[string]any)
	if !ok {
		t.Fatal("manager correlation fixture labels is not an object")
	}
	return labels
}

func managerCorrelationOwner(t *testing.T, item map[string]any) map[string]any {
	t.Helper()
	owners, ok := managerCorrelationMetadata(t, item)["ownerReferences"].([]any)
	if !ok || len(owners) == 0 {
		t.Fatal("manager correlation fixture ownerReferences is not a populated array")
	}
	owner, ok := owners[0].(map[string]any)
	if !ok {
		t.Fatal("manager correlation fixture ownerReference is not an object")
	}
	return owner
}

func managerCorrelationAppendOwner(t *testing.T, item, owner map[string]any) {
	t.Helper()
	metadata := managerCorrelationMetadata(t, item)
	metadata["ownerReferences"] = append(metadata["ownerReferences"].([]any), owner)
}

func managerCorrelationPodSpec(t *testing.T, item map[string]any) map[string]any {
	t.Helper()
	spec := item[specKey].(map[string]any)
	if item[kindKey] == deploymentKind || item[kindKey] == replicaSetKind {
		template := spec[helmTemplateCommand].(map[string]any)
		spec = template[specKey].(map[string]any)
	}
	return spec
}

func managerCorrelationContainer(t *testing.T, item map[string]any) map[string]any {
	t.Helper()
	containers := managerCorrelationPodSpec(t, item)[containersKey].([]any)
	for _, value := range containers {
		container := value.(map[string]any)
		if container[nameKey] == imageAttestationManagerComponent {
			return container
		}
	}
	t.Fatal("manager correlation fixture has no manager container")
	return nil
}

func managerCorrelationContainerStatus(t *testing.T, item map[string]any) map[string]any {
	t.Helper()
	statuses := item[statusKey].(map[string]any)["containerStatuses"].([]any)
	for _, value := range statuses {
		status := value.(map[string]any)
		if status[nameKey] == imageAttestationManagerComponent {
			return status
		}
	}
	t.Fatal("manager correlation fixture has no manager container status")
	return nil
}

func managerCorrelationConditions(t *testing.T, item map[string]any) []any {
	t.Helper()
	conditions, ok := item[statusKey].(map[string]any)["conditions"].([]any)
	if !ok {
		t.Fatal("manager correlation fixture conditions is not an array")
	}
	return conditions
}

func managerCorrelationReadyCondition(t *testing.T, item map[string]any) map[string]any {
	t.Helper()
	for _, value := range managerCorrelationConditions(t, item) {
		condition := value.(map[string]any)
		if condition[schemaTypeKey] == readyConditionType {
			return condition
		}
	}
	t.Fatal("manager correlation fixture has no Ready condition")
	return nil
}

func managerCorrelationAppend(t *testing.T, document map[string]any, item map[string]any) {
	t.Helper()
	document[itemsKey] = append(document[itemsKey].([]any), item)
}

func managerCorrelationRemoveKind(t *testing.T, document map[string]any, kind string) {
	t.Helper()
	items := document[itemsKey].([]any)
	kept := make([]any, 0, len(items))
	for _, value := range items {
		item := value.(map[string]any)
		owned := managerCorrelationLabels(t, item)[imageAttestationOwnerKey] == imageAttestationOwnerValue
		if item[kindKey] != kind || !owned {
			kept = append(kept, item)
		}
	}
	document[itemsKey] = kept
}

func cloneManagerCorrelationItem(t *testing.T, item map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	var clone map[string]any
	if err := json.Unmarshal(body, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func marshalManagerCorrelationDocument(t *testing.T, document map[string]any) string {
	t.Helper()
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

type managerCorrelationResult struct {
	output    string
	resources string
	helmArgs  []string
	attested  bool
	err       error
}

func runManagerCorrelationFixture(
	t *testing.T,
	helm, ownership, attest, managerRef, runnerRef, registry, resources string,
	installArgs ...string,
) managerCorrelationResult {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	ownerPath := filepath.Join(dir, "owner")
	resourcesPath := filepath.Join(dir, "resources.json")
	attestPath := filepath.Join(dir, "attest-image")
	helmPath := filepath.Join(dir, "helm")
	helmArgsPath := filepath.Join(dir, "helm-args")
	markerPath := filepath.Join(dir, "manager-attested")
	for path, fixture := range map[string]struct {
		body string
		mode os.FileMode
	}{
		ownerPath:                           {imageAttestationOwnerValue, 0o600},
		resourcesPath:                       {resources, 0o600},
		attestPath:                          {attest, 0o700},
		helmPath:                            {helm, 0o700},
		filepath.Join(dir, "ownership"):     {ownership, 0o600},
		filepath.Join(dir, "post-renderer"): {"#!/usr/bin/env bash\ncat\n", 0o700},
	} {
		if err := os.WriteFile(path, []byte(fixture.body), fixture.mode); err != nil {
			t.Fatal(err)
		}
	}

	kubectl := `#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -eq 6 ] && [ "$1" = label ] && [ "$2" = -f ] && [ "$3" = - ] &&
   [ "$4" = "$FEATURE_E2E_OWNER_KEY=$FAKE_OWNER_VALUE" ] && [ "$5" = --overwrite ]; then
  exit 98
fi
if [ "${1:-}" = label ]; then
  [ "$*" = "label -f - $FEATURE_E2E_OWNER_KEY=$FAKE_OWNER_VALUE --overwrite" ] || exit 98
  cat >/dev/null
  exit 0
fi
if [ "${1:-}" = patch ]; then
  [ "$2" = deployment ] && [ "$3" = pgcopydb-e2e ] && [ "$4" = -n ] &&
    [ "$5" = "$E2E_OPERATOR_NAMESPACE" ] && [ "$6" = --type=merge ] && [ "$7" = -p ] && [ -n "$8" ] || exit 98
  exit 0
fi
if [ "${1:-}" = rollout ]; then
  [ "$*" = "rollout status deployment/pgcopydb-e2e -n $E2E_OPERATOR_NAMESPACE --timeout=5m" ] || exit 98
  exit 0
fi
if [ "${1:-}" = get ]; then
  if [ "$*" = "get deployments -n $E2E_OPERATOR_NAMESPACE -l $FEATURE_E2E_OWNER_KEY=$FAKE_OWNER_VALUE -o json" ]; then
    jq -c --arg key "$FEATURE_E2E_OWNER_KEY" --arg value "$FAKE_OWNER_VALUE" '
      .items = [.items[] |
        select(.kind == "Deployment" and .metadata.labels[$key] == $value)]
    ' "$FAKE_RESOURCES_PATH"
    exit 0
  fi
  expected="get deployments,replicasets,pods -n $E2E_OPERATOR_NAMESPACE"
  expected="$expected -l $FEATURE_E2E_OWNER_KEY=$FAKE_OWNER_VALUE"
  expected="$expected,app.kubernetes.io/instance=pgcopydb-e2e"
  expected="$expected,app.kubernetes.io/name=pgcopydb-operator -o json"
  [ "$*" = "$expected" ] || exit 98
  jq -c --arg key "$FEATURE_E2E_OWNER_KEY" --arg value "$FAKE_OWNER_VALUE" '
    if (.items | type) == "array" then
      .items = [.items[] | select(.metadata.labels[$key] == $value)]
    else . end
  ' "$FAKE_RESOURCES_PATH"
  exit 0
fi
exit 98
`
	realHelm := `#!/usr/bin/env bash
set -euo pipefail
if [ "${1:-}" = install ] && [ "${2:-}" = pgcopydb-e2e ]; then
  printf '%s\n' "$@" > "$FAKE_HELM_ARGS_PATH"
  exit 0
fi
if [ "$*" = "get manifest pgcopydb-e2e -n $E2E_OPERATOR_NAMESPACE" ]; then
  printf '%s\n' 'apiVersion: apps/v1' 'kind: Deployment' 'metadata:' '  name: pgcopydb-e2e'
  exit 0
fi
exit 98
`
	docker := `#!/usr/bin/env bash
set -euo pipefail
[ "$*" = "buildx imagetools inspect $MANAGER_REF --raw" ] || exit 98
printf '%s' "$FAKE_REGISTRY"
`
	for name, body := range map[string]string{kubectlCommand: kubectl, "real-helm": realHelm, "docker": docker} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	cmdArgs := make([]string, 0, 6+len(installArgs))
	cmdArgs = append(cmdArgs,
		helmInstallCommand, featureControllerName, featureFixtureChart,
		"-n", featureControllerNS, "--wait",
	)
	cmdArgs = append(cmdArgs, installArgs...)
	cmd := exec.Command(helmPath, cmdArgs...)
	cmd.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"E2E_OPERATOR_NAMESPACE="+featureControllerNS,
		"REAL_HELM="+filepath.Join(bin, "real-helm"),
		"FEATURE_E2E_HELPERS="+dir,
		"FEATURE_E2E_OWNER_KEY="+imageAttestationOwnerKey,
		"FEATURE_E2E_OWNER_FILE="+ownerPath,
		"MANAGER_REF="+managerRef,
		"RUNNER_REF="+runnerRef,
		"FAKE_OWNER_VALUE="+imageAttestationOwnerValue,
		"FAKE_RESOURCES_PATH="+resourcesPath,
		"FAKE_REGISTRY="+registry,
		"FAKE_HELM_ARGS_PATH="+helmArgsPath,
	)
	output, runErr := cmd.CombinedOutput()
	_, statErr := os.Stat(markerPath)
	if statErr != nil && !os.IsNotExist(statErr) {
		t.Fatal(statErr)
	}
	finalResources, readErr := os.ReadFile(resourcesPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	helmArgs := []string{}
	if body, err := os.ReadFile(helmArgsPath); err == nil {
		helmArgs = strings.Split(strings.TrimSpace(string(body)), "\n")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return managerCorrelationResult{
		output: string(output), resources: string(finalResources),
		helmArgs: helmArgs, attested: statErr == nil, err: runErr,
	}
}

func TestFeatureE2EClusterUsesDaemonlessRegistryInspection(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	cluster := wf.Jobs["cluster"]
	if steps := protectedStepsUsing(cluster, "docker/setup-buildx-action@v4"); len(steps) != 0 {
		t.Fatalf("cluster setup-buildx steps = %d, want 0", len(steps))
	}
	if steps := protectedStepsUsing(cluster, "docker/login-action@v4"); len(steps) != 1 {
		t.Errorf("cluster login steps = %d, want 1", len(steps))
	}
	inputs := protectedStepNamed(t, cluster, "Validate immutable inputs").Run
	for _, ref := range []string{"$MANAGER_REF", "$RUNNER_REF"} {
		if !strings.Contains(inputs, `docker buildx imagetools inspect "`+ref+`"`) {
			t.Errorf("immutable input validation does not inspect %s", ref)
		}
	}
}

func TestFeatureE2EClusterSafetyAndCleanup(t *testing.T) { //nolint:gocyclo // One graph owns the safety ordering.
	wf := parseProtectedWorkflow(t, featureWorkflow)
	body := read(t, featureWorkflow)
	if strings.Count(body, "${{ vars.E2E_EXCLUSIVE_CONTROLLER }}") != 1 {
		t.Error("exclusive-controller attestation must be mapped exactly once")
	}
	if strings.Count(body, "${{ secrets.E2E_EXPECT_CONTEXT }}") != 1 {
		t.Error("expected context secret must be mapped exactly once")
	}
	cluster := wf.Jobs["cluster"]
	if cluster.Environment != protectedClusterGroup || cluster.Concurrency.Group != protectedClusterGroup ||
		cluster.Concurrency.CancelInProgress {
		t.Errorf("cluster protection = environment %q concurrency %+v", cluster.Environment, cluster.Concurrency)
	}
	exclusive := protectedStepNamed(t, cluster, "Require exclusive controller policy")
	if exclusive.Env["E2E_EXCLUSIVE_CONTROLLER"] != "${{ vars.E2E_EXCLUSIVE_CONTROLLER }}" ||
		!strings.Contains(exclusive.Run, `[ "$E2E_EXCLUSIVE_CONTROLLER" = "true" ]`) {
		t.Error("exclusive controller policy is not literal-true")
	}
	exclusiveIndex := protectedStepIndex(t, cluster, "Require exclusive controller policy")
	for _, earlier := range cluster.Steps[:exclusiveIndex] {
		if earlier.Run != "" {
			t.Errorf("shell step %q runs before exclusive controller policy", earlier.Name)
		}
	}
	contextStep := protectedStepNamed(t, cluster, "Mask and confirm Kubernetes context")
	if contextStep.Env["E2E_EXPECT_CONTEXT"] != "${{ secrets.E2E_EXPECT_CONTEXT }}" ||
		strings.Count(contextStep.Run, "::add-mask::") != 2 ||
		!strings.Contains(contextStep.Run, `actual=$(kubectl config current-context)`) ||
		!strings.Contains(contextStep.Run, `[ "$actual" = "$E2E_EXPECT_CONTEXT" ]`) {
		t.Error("protected context is not masked and compared")
	}
	if protectedStepIndex(t, cluster, "Require exclusive controller policy") >=
		protectedStepIndex(t, cluster, "Mask and confirm Kubernetes context") {
		t.Error("controller policy follows Kubernetes access")
	}
	for _, path := range []string{releaseWorkflow, publishedE2EWorkflow} {
		if strings.Contains(read(t, path), "secrets.E2E_EXPECT_CONTEXT") {
			t.Errorf("%s maps the feature-only context secret", path)
		}
	}
	run := protectedStepNamed(t, cluster, "Run non-chaos suite")
	if run.Env["E2E_SCALE"] != "${{ needs.resolve.outputs.scale }}" ||
		run.Env["E2E_MANAGE_NAMESPACES"] != falseValue ||
		run.Env["E2E_KEEP_FIXTURES"] != trueValue {
		t.Errorf("feature suite environment = %v", run.Env)
	}
	for _, want := range []string{
		"-ginkgo.label-filter=!chaos && !flaky",
		"-ginkgo.fail-on-empty",
		`test_args+=("-ginkgo.focus=$E2E_FOCUS")`,
	} {
		if !strings.Contains(run.Run, want) {
			t.Errorf("suite command is missing %q", want)
		}
	}
	helpers := protectedStepNamed(t, cluster, "Write cluster helpers").Run
	for _, want := range []string{
		imageIDKey, "imagetools inspect", "--runner-image=", "FEATURE_E2E_OWNER_KEY",
		"FEATURE_E2E_OWNER_FILE", "capture_owned", "cleanup_owned", `-l "$owner_selector"`,
		`kubectl delete --raw "$delete_path"`, "preconditions:{uid:$uid}",
		"propagation=Foreground", "require_empty",
	} {
		if !strings.Contains(helpers, want) {
			t.Errorf("cluster helpers are missing %q", want)
		}
	}
	if strings.Contains(helpers, "|| true") {
		t.Error("feature cleanup can suppress a command failure")
	}
	for _, banned := range []string{
		"delete migrations.pgcopydb-operator.io --all",
		"app.kubernetes.io/managed-by=pgcopydb-operator",
		"kubectl delete namespace", "clusters.postgresql.cnpg.io",
		"kubectl delete pvc", "kubectl delete configmap", "finalizers-",
	} {
		if strings.Contains(helpers, banned) {
			t.Errorf("cleanup contains forbidden target %q", banned)
		}
	}
	cleanup := extractCleanupHeredoc(t, helpers)
	migrations := strings.Index(cleanup, "migrations.pgcopydb-operator.io")
	jobs := strings.Index(cleanup, "batch/v1 Job jobs")
	controller := strings.Index(cleanup, "feature_uninstall_controller")
	if migrations < 0 || jobs <= migrations || controller <= jobs {
		t.Error("cleanup does not delete run-owned Migrations, Jobs, and controller in order")
	}
	if protectedStepNamed(t, cluster, "Cleanup feature resources").If != "always()" {
		t.Error("cleanup does not run under always()")
	}
	finish := protectedStepNamed(t, cluster, "Require suite and cleanup success")
	for _, want := range []string{`[ "$SUITE_OUTCOME" = success ]`, `[ "$CLEANUP_OUTCOME" = success ]`} {
		if !strings.Contains(finish.Run, want) {
			t.Errorf("cluster finalizer is missing %q", want)
		}
	}
	final := protectedStepNamed(t, wf.Jobs["final-status"], "Publish final status")
	for _, want := range []string{
		`CLUSTER_RESULT`, `SUITE_OUTCOME`, `CLEANUP_OUTCOME`,
		`state=success`, `state=failure`, `state=error`,
		`context=feature-e2e/focus`, `statuses/$SHA`,
	} {
		if !strings.Contains(final.Run, want) && final.Env[want] == "" {
			t.Errorf("final status is missing %q", want)
		}
	}
}

func TestFeatureE2ECleanupIsSafeBeforeHelperSetup(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	cluster := wf.Jobs["cluster"]
	helpers := protectedStepNamed(t, cluster, "Write cluster helpers")
	cleanup := protectedStepNamed(t, cluster, "Cleanup feature resources")
	if helpers.ID != "helpers" {
		t.Errorf("cluster helper step ID = %q, want helpers", helpers.ID)
	}
	if cleanup.Env["HELPERS_OUTCOME"] != "${{ steps.helpers.outcome }}" {
		t.Errorf("cleanup helper outcome = %q", cleanup.Env["HELPERS_OUTCOME"])
	}
	if cleanup.Env["SUITE_OUTCOME"] != "${{ steps.suite.outcome }}" {
		t.Errorf("cleanup suite outcome = %q", cleanup.Env["SUITE_OUTCOME"])
	}
	if protectedStepIndex(t, cluster, "Write cluster helpers") >=
		protectedStepIndex(t, cluster, "Attest runner image") {
		t.Error("cluster mutation can start before helper setup succeeds")
	}

	tests := []struct {
		name           string
		helpersOutcome string
		suiteOutcome   string
		wantArgs       string
		wantCall       bool
		wantError      bool
	}{
		{"suite success is strict", successValue, successValue, "", true, false},
		{"suite failure recovers", successValue, failureValue, recoveryValue, true, false},
		{"suite skipped recovers", successValue, skippedValue, recoveryValue, true, false},
		{"suite cancellation recovers", successValue, "cancelled", recoveryValue, true, false},
		{"unknown suite outcome fails", successValue, "unknown", "", false, true},
		{"skipped helpers are safe", skippedValue, skippedValue, "", false, false},
		{"failed helpers are safe", failureValue, failureValue, "", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			calls := filepath.Join(dir, "calls")
			if err := os.WriteFile(filepath.Join(dir, "cleanup"),
				[]byte("#!/usr/bin/env bash\nprintf '%s' \"$*\" > \"$CALLS\"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", "-c", cleanup.Run)
			cmd.Env = []string{
				"PATH=" + os.Getenv("PATH"),
				"FEATURE_E2E_HELPERS=" + dir,
				"CALLS=" + calls,
				"HELPERS_OUTCOME=" + tt.helpersOutcome,
				"SUITE_OUTCOME=" + tt.suiteOutcome,
			}
			output, err := cmd.CombinedOutput()
			if tt.wantError {
				if err == nil {
					t.Fatalf("cleanup accepted suite outcome %q", tt.suiteOutcome)
				}
				return
			}
			if err != nil {
				t.Fatalf("cleanup routing failed: %v\n%s", err, output)
			}
			got, readErr := os.ReadFile(calls)
			if !tt.wantCall {
				if !os.IsNotExist(readErr) {
					t.Fatalf("cleanup ran without helpers: %q, err %v", got, readErr)
				}
				return
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
			if string(got) != tt.wantArgs {
				t.Fatalf("cleanup args = %q, want %q", got, tt.wantArgs)
			}
		})
	}

	t.Run("missing_cleanup_after_helpers", func(t *testing.T) {
		cmd := exec.Command("bash", "-c", cleanup.Run)
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"),
			"HELPERS_OUTCOME=success",
			"SUITE_OUTCOME=success",
			"FEATURE_E2E_HELPERS=" + t.TempDir(),
		}
		if err := cmd.Run(); err == nil {
			t.Fatal("cleanup succeeded without its helper after helper setup")
		}
	})
}

func TestFeatureE2EProtectsPrometheusURLBeforeRunnerUse(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	cluster := wf.Jobs["cluster"]
	validateIndex := protectedStepIndex(t, cluster, "Validate immutable inputs")
	validate := cluster.Steps[validateIndex]
	const secretSource = "${{ secrets.E2E_PROMETHEUS_URL }}"
	if strings.Contains(read(t, featureWorkflow), "vars.E2E_PROMETHEUS_URL") {
		t.Fatal("Prometheus URL enters a non-secret runner-rendered expression")
	}
	if validate.Env["E2E_PROMETHEUS_URL"] != secretSource {
		t.Fatal("Prometheus URL is not mapped from the GitHub Actions secret")
	}
	for _, step := range cluster.Steps[:validateIndex] {
		if step.Env["E2E_PROMETHEUS_URL"] != "" || strings.Contains(step.Run, "E2E_PROMETHEUS_URL") {
			t.Fatal("Prometheus URL is available before the masking step")
		}
	}

	maskCommand := `printf '::add-mask::%s\n' "$E2E_PROMETHEUS_URL"`
	xtraceOffIndex := strings.Index(validate.Run, "set +x")
	maskIndex := strings.Index(validate.Run, maskCommand)
	firstReference := strings.Index(validate.Run, "$E2E_PROMETHEUS_URL")
	nonemptyIndex := strings.Index(validate.Run, `[ -n "$E2E_PROMETHEUS_URL" ]`)
	if xtraceOffIndex < 0 || maskIndex <= xtraceOffIndex ||
		firstReference != maskIndex+strings.Index(maskCommand, "$E2E_PROMETHEUS_URL") ||
		nonemptyIndex <= maskIndex {
		t.Fatal("Prometheus URL is not masked in full with xtrace disabled before validation")
	}
	suiteIndex := protectedStepIndex(t, cluster, "Run non-chaos suite")
	if suiteIndex <= validateIndex {
		t.Fatal("feature suite can use the Prometheus URL before masking")
	}
	if cluster.Steps[suiteIndex].Env["E2E_PROMETHEUS_URL"] != secretSource {
		t.Fatal("Prometheus URL is not mapped from the GitHub Actions secret into the suite")
	}

	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/usr/bin/env bash\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := "https://prometheus.invalid:9090/private/api?token=test-value"
	sha := strings.Repeat("1", 40)
	digest := strings.Repeat("2", 64)
	runValidation := func(url string) ([]byte, error) {
		cmd := exec.Command("bash", "-x", "-c", validate.Run)
		cmd.Env = []string{
			"PATH=" + bin + ":" + os.Getenv("PATH"),
			"E2E_PROMETHEUS_URL=" + url,
			"E2E_OPERATOR_NAMESPACE=pgcopydb-e2e",
			"MANAGER_REF=ghcr.io/ydixken/pgcopydb-operator:feature-" + sha + "@sha256:" + digest,
			"RUNNER_REF=ghcr.io/ydixken/pgcopydb-operator/runner:feature-" + sha + "@sha256:" + digest,
		}
		return cmd.CombinedOutput()
	}
	output, err := runValidation(sentinel)
	if err != nil {
		t.Fatal("Prometheus validation step failed")
	}
	masked := false
	for line := range strings.SplitSeq(strings.TrimSuffix(string(output), "\n"), "\n") {
		if line == "::add-mask::"+sentinel {
			masked = true
			continue
		}
		if strings.Contains(line, sentinel) {
			t.Fatal("Prometheus URL was emitted outside the masking command")
		}
	}
	if !masked {
		t.Fatal("complete Prometheus URL was not registered for masking")
	}

	output, err = runValidation("")
	if err == nil {
		t.Fatal("missing Prometheus URL did not fail validation")
	}
	if !bytes.Contains(output, []byte("::error::Prometheus URL is not configured")) {
		t.Fatalf("missing Prometheus URL failed without the safe diagnostic: %s", output)
	}
}

func TestFeatureE2EHasNoReleasePath(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	body := read(t, featureWorkflow)
	for _, banned := range []string{
		"pull_request_target", "auto-release.yml", "promote.yml", "gh release", "git tag",
		"helm push", "imagetools create", ":latest", "-rc.",
	} {
		if strings.Contains(body, banned) {
			t.Errorf("feature workflow contains release path %q", banned)
		}
	}
	for name, job := range wf.Jobs {
		if job.Permissions[permissionContents] == permissionWrite || job.Permissions["deployments"] != "" ||
			job.Permissions["actions"] != "" || job.Permissions["id-token"] != "" {
			t.Errorf("%s has release-capable permissions: %v", name, job.Permissions)
		}
	}
}

func TestFeatureE2EFinalStatusClassification(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	cluster := wf.Jobs["cluster"]
	final := protectedStepNamed(t, wf.Jobs["final-status"], "Publish final status")
	tests := []struct {
		name, cluster, suite, cleanup, suiteRan, suiteCompleted, manager, runner, want string
	}{
		{"safe success", successValue, successValue, successValue,
			trueValue, trueValue, trueValue, trueValue, successValue},
		{"safe assertion failure", failureValue, failureValue, successValue,
			trueValue, trueValue, trueValue, trueValue, failureValue},
		{"suite timeout", failureValue, failureValue, successValue,
			trueValue, "", trueValue, trueValue, errorValue},
		{"suite panic or abort", failureValue, failureValue, successValue,
			trueValue, falseValue, trueValue, trueValue, errorValue},
		{"completion evidence missing", failureValue, failureValue, successValue,
			trueValue, "", trueValue, trueValue, errorValue},
		{"completion evidence malformed", failureValue, failureValue, successValue,
			trueValue, unreadableValue, trueValue, trueValue, errorValue},
		{"manager digest mismatch", failureValue, failureValue, successValue,
			trueValue, trueValue, falseValue, trueValue, errorValue},
		{"manager attestation unreadable", failureValue, failureValue, successValue,
			trueValue, trueValue, unreadableValue, trueValue, errorValue},
		{"manager attestation missing", failureValue, failureValue, successValue,
			trueValue, trueValue, "", trueValue, errorValue},
		{"runner digest mismatch", failureValue, failureValue, successValue,
			trueValue, trueValue, trueValue, falseValue, errorValue},
		{"runner attestation unreadable", failureValue, failureValue, successValue,
			trueValue, trueValue, trueValue, unreadableValue, errorValue},
		{"runner attestation missing", failureValue, "", successValue,
			"", "", trueValue, "", errorValue},
		{"suite did not run", failureValue, failureValue, successValue,
			falseValue, falseValue, trueValue, trueValue, errorValue},
		{"cluster cancelled", "cancelled", failureValue, successValue,
			trueValue, trueValue, trueValue, trueValue, errorValue},
		{"cleanup failure", failureValue, successValue, failureValue,
			trueValue, trueValue, trueValue, trueValue, errorValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runFinalStatusFixture(
				t, final.Run, tt.cluster, tt.suite, tt.cleanup, tt.suiteRan,
				tt.suiteCompleted, tt.manager, tt.runner,
			)
			if got != tt.want {
				t.Errorf("emitted state = %q, want %q", got, tt.want)
			}
		})
	}
	wantOutputs := map[string]string{
		"suite_ran":        "${{ steps.suite.outputs.suite_ran }}",
		"suite_completed":  "${{ steps.suite.outputs.suite_completed }}",
		"manager_attested": "${{ steps.suite.outputs.manager_attested }}",
		"runner_attested":  "${{ steps.runner-attestation.outputs.runner_attested }}",
	}
	for name, want := range wantOutputs {
		if got := cluster.Outputs[name]; got != want {
			t.Errorf("cluster output %s = %q, want %q", name, got, want)
		}
	}
	wantFinalEnv := map[string]string{
		"SUITE_RAN":        "${{ needs.cluster.outputs.suite_ran }}",
		"SUITE_COMPLETED":  "${{ needs.cluster.outputs.suite_completed }}",
		"MANAGER_ATTESTED": "${{ needs.cluster.outputs.manager_attested }}",
		"RUNNER_ATTESTED":  "${{ needs.cluster.outputs.runner_attested }}",
	}
	for name, want := range wantFinalEnv {
		if got := final.Env[name]; got != want {
			t.Errorf("final status %s = %q, want %q", name, got, want)
		}
	}

	runnerAttestation := protectedStepNamed(t, cluster, "Attest runner image")
	if runnerAttestation.ID != "runner-attestation" || runnerAttestation.ContinueOnError {
		t.Error("runner attestation is not a fail-closed output step")
	}
	runnerCheck := strings.LastIndex(runnerAttestation.Run, `"$FEATURE_E2E_HELPERS/attest-image"`)
	runnerOutput := strings.Index(runnerAttestation.Run, `printf 'runner_attested=true\n'`)
	if runnerCheck < 0 || runnerOutput <= runnerCheck {
		t.Error("runner success is recorded before runtime attestation")
	}
	if protectedStepIndex(t, cluster, "Attest runner image") >=
		protectedStepIndex(t, cluster, "Run non-chaos suite") {
		t.Error("the suite can run before runner attestation")
	}

	suite := protectedStepNamed(t, cluster, "Run non-chaos suite")
	reportArg := strings.Index(suite.Run, `-ginkgo.json-report=$suite_report`)
	goTest := strings.Index(suite.Run, `go test "${test_args[@]}"`)
	suiteOutput := strings.Index(suite.Run, `printf 'suite_ran=true\n'`)
	managerCheck := strings.Index(suite.Run, `[ -f "$FEATURE_E2E_HELPERS/manager-attested" ]`)
	managerOutput := strings.Index(suite.Run, `printf 'manager_attested=true\n'`)
	completionCheck := strings.Index(suite.Run, `jq -e --argjson result "$suite_result"`)
	completionOutput := strings.Index(suite.Run, `printf 'suite_completed=true\n'`)
	if reportArg < 0 || goTest <= reportArg || suiteOutput <= goTest || managerCheck <= suiteOutput ||
		managerOutput <= managerCheck || completionCheck <= managerOutput || completionOutput <= completionCheck {
		t.Error("suite or manager success is recorded before its safety evidence")
	}
}

func TestFeatureE2EUsesApprovedOperatorNamespace(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	cluster := wf.Jobs["cluster"]
	if got := cluster.Env["E2E_OPERATOR_NAMESPACE"]; got != "pgcopydb-e2e" {
		t.Errorf("feature operator namespace = %q, want pgcopydb-e2e", got)
	}
	if strings.Contains(protectedRuns(cluster), "pgcopydb-e2e-system") {
		t.Error("protected feature lifecycle still references pgcopydb-e2e-system")
	}
	if got := protectedStepNamed(t, cluster, "Run non-chaos suite").Env["E2E_MANAGE_NAMESPACES"]; got != falseValue {
		t.Errorf("feature E2E_MANAGE_NAMESPACES = %q, want false", got)
	}
	for _, path := range []string{publishedE2EWorkflow, releaseWorkflow} {
		if strings.Contains(read(t, path), "E2E_OPERATOR_NAMESPACE") {
			t.Errorf("%s overrides the normal operator namespace default", path)
		}
	}
}

func runFinalStatusFixture(
	t *testing.T, script, cluster, suite, cleanup, suiteRan, suiteCompleted, manager, runner string,
) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, stateKey)
	gh := `#!/usr/bin/env bash
set -euo pipefail
for arg in "$@"; do
  case "$arg" in
    state=*) printf '%s\n' "${arg#state=}" > "$STATUS_STATE" ;;
  esac
done
test -s "$STATUS_STATE"
`
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(gh), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"GH_TOKEN=test-token",
		"GITHUB_REPOSITORY=example/repository",
		"SHA=0123456789abcdef0123456789abcdef01234567",
		"RUN_MODE=full",
		"CLUSTER_RESULT="+cluster,
		"SUITE_OUTCOME="+suite,
		"CLEANUP_OUTCOME="+cleanup,
		"SUITE_RAN="+suiteRan,
		"SUITE_COMPLETED="+suiteCompleted,
		"MANAGER_ATTESTED="+manager,
		"RUNNER_ATTESTED="+runner,
		"RUN_URL=https://example.invalid/run",
		"STATUS_STATE="+statePath,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run final status: %v\n%s", err, output)
	}
	state, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(state))
}

func TestFeatureE2ESuiteCompletionEvidence(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	suite := protectedStepNamed(t, wf.Jobs["cluster"], "Run non-chaos suite")
	passed := ginkgoSpecFixture(gingkotypes.SpecStatePassed, "selected spec")
	skipped := ginkgoSpecFixture(gingkotypes.SpecStateSkipped, "filter-excluded spec")
	failed := ginkgoSpecFixture(gingkotypes.SpecStateFailed, "selected assertion")
	failed.Failure = gingkotypes.Failure{
		Message: "Expected migration to complete",
		Location: gingkotypes.CodeLocation{
			FileName:       "/workspace/test/e2e/migration_test.go",
			LineNumber:     42,
			FullStackTrace: "example.com/e2e.TestMigration\n\t/workspace/test/e2e/migration_test.go:42",
		},
		FailureNodeContext:  gingkotypes.FailureNodeIsLeafNode,
		FailureNodeType:     gingkotypes.NodeTypeIt,
		FailureNodeLocation: gingkotypes.CodeLocation{FileName: "/workspace/test/e2e/migration_test.go", LineNumber: 40},
	}
	safeReport := ginkgoReportFixture(t, true, 1, 1, gingkotypes.SpecReports{passed})
	safeFailureReport := ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{failed})
	mutateFailure := func(mutate func(map[string]any)) string {
		return mutateGinkgoReport(t, safeFailureReport, func(report map[string]any) {
			specs := report["SpecReports"].([]any)
			failure := specs[0].(map[string]any)["Failure"].(map[string]any)
			mutate(failure)
		})
	}
	mutateLocation := func(mutate func(map[string]any)) string {
		return mutateFailure(func(failure map[string]any) {
			mutate(failure["Location"].(map[string]any))
		})
	}
	mutateCount := func(name string, value any) string {
		return mutateGinkgoReport(t, safeReport, func(report map[string]any) {
			report["PreRunStats"].(map[string]any)[name] = value
		})
	}
	deleteCount := func(name string) string {
		return mutateGinkgoReport(t, safeReport, func(report map[string]any) {
			delete(report["PreRunStats"].(map[string]any), name)
		})
	}
	failedWithCleanup := failed
	failedWithCleanup.AdditionalFailures = []gingkotypes.AdditionalFailure{{
		State: gingkotypes.SpecStateFailed,
		Failure: gingkotypes.Failure{
			Message:            "AfterEach cleanup failed",
			FailureNodeContext: gingkotypes.FailureNodeInContainer,
			FailureNodeType:    gingkotypes.NodeTypeAfterEach,
		},
	}}
	failedWithTimedOutCleanup := failed
	failedWithTimedOutCleanup.AdditionalFailures = []gingkotypes.AdditionalFailure{{
		State: gingkotypes.SpecStateTimedout,
		Failure: gingkotypes.Failure{
			Message:            "DeferCleanup did not return",
			FailureNodeContext: gingkotypes.FailureNodeInContainer,
			FailureNodeType:    gingkotypes.NodeTypeCleanupAfterEach,
		},
	}}
	failedWithNestedFailure := failed
	failedWithNestedFailure.Failure.AdditionalFailure = &gingkotypes.AdditionalFailure{
		State: gingkotypes.SpecStateFailed,
		Failure: gingkotypes.Failure{
			Message:            "failure after the primary node timed out",
			FailureNodeContext: gingkotypes.FailureNodeIsLeafNode,
		},
	}
	malformedAdditionalFailures := setGinkgoSpecReportField(
		t, safeFailureReport, 0, "AdditionalFailures", map[string]any{"State": "failed"},
	)
	incompleteFailure := ginkgoSpecFixture(gingkotypes.SpecStateFailed, "missing failure evidence")
	inconsistentFailureReport := ginkgoReportFixture(
		t, false, 1, 1, gingkotypes.SpecReports{passed},
	)
	panicked := ginkgoSpecFixture(gingkotypes.SpecStatePanicked, "panicked spec")
	panicked.Failure = gingkotypes.Failure{Message: "panic", ForwardedPanic: "boom"}
	timedOut := ginkgoSpecFixture(gingkotypes.SpecStateTimedout, "timed out spec")
	timedOut.Failure = gingkotypes.Failure{Message: "spec timed out"}
	aborted := ginkgoSpecFixture(gingkotypes.SpecStateAborted, "aborted spec")
	aborted.Failure = gingkotypes.Failure{Message: "suite aborted"}
	interrupted := ginkgoSpecFixture(gingkotypes.SpecStateInterrupted, "interrupted spec")
	interrupted.Failure = gingkotypes.Failure{Message: "suite interrupted"}
	tests := []struct {
		name        string
		fixture     suiteFixture
		completed   bool
		stepSuccess bool
	}{
		{"realistic full report with filtered spec", suiteFixture{
			report:  ginkgoReportFixture(t, true, 3, 2, gingkotypes.SpecReports{passed, skipped, passed}),
			runMode: fullModeValue,
		}, true, true},
		{"realistic focused report with filtered specs", suiteFixture{
			report:  ginkgoReportFixture(t, true, 3, 1, gingkotypes.SpecReports{skipped, passed, skipped}),
			runMode: focusModeValue, focus: "cutover",
		}, true, true},
		{"It report count differs from total specs", suiteFixture{
			report: mutateCount("TotalSpecs", 2),
		}, false, false},
		{"ordinary It assertion failure with filtered spec", suiteFixture{
			report:   ginkgoReportFixture(t, false, 2, 1, gingkotypes.SpecReports{failed, skipped}),
			goResult: 1,
		}, true, false},
		{"missing failure node context", suiteFixture{
			report: mutateFailure(func(failure map[string]any) {
				delete(failure, "FailureNodeContext")
			}), goResult: 1,
		}, false, false},
		{"wrong failure node context", suiteFixture{
			report: mutateFailure(func(failure map[string]any) {
				failure["FailureNodeContext"] = gingkotypes.FailureNodeInContainer
			}), goResult: 1,
		}, false, false},
		{"missing failure node type", suiteFixture{
			report: mutateFailure(func(failure map[string]any) {
				delete(failure, "FailureNodeType")
			}), goResult: 1,
		}, false, false},
		{"wrong failure node type", suiteFixture{
			report: mutateFailure(func(failure map[string]any) {
				failure["FailureNodeType"] = "BeforeEach"
			}), goResult: 1,
		}, false, false},
		{"missing failure message", suiteFixture{
			report: mutateFailure(func(failure map[string]any) {
				delete(failure, "Message")
			}), goResult: 1,
		}, false, false},
		{"empty failure message", suiteFixture{
			report: mutateFailure(func(failure map[string]any) {
				failure["Message"] = ""
			}), goResult: 1,
		}, false, false},
		{"missing failure location", suiteFixture{
			report: mutateFailure(func(failure map[string]any) {
				delete(failure, "Location")
			}), goResult: 1,
		}, false, false},
		{"non-object failure location", suiteFixture{
			report: mutateFailure(func(failure map[string]any) {
				failure["Location"] = "migration_test.go:42"
			}), goResult: 1,
		}, false, false},
		{"missing failure filename", suiteFixture{
			report: mutateLocation(func(location map[string]any) {
				delete(location, "FileName")
			}), goResult: 1,
		}, false, false},
		{"empty failure filename", suiteFixture{
			report: mutateLocation(func(location map[string]any) {
				location["FileName"] = ""
			}), goResult: 1,
		}, false, false},
		{"non-string failure filename", suiteFixture{
			report: mutateLocation(func(location map[string]any) {
				location["FileName"] = 42
			}), goResult: 1,
		}, false, false},
		{"missing failure line number", suiteFixture{
			report: mutateLocation(func(location map[string]any) {
				delete(location, "LineNumber")
			}), goResult: 1,
		}, false, false},
		{"zero failure line number", suiteFixture{
			report: mutateLocation(func(location map[string]any) {
				location["LineNumber"] = 0
			}), goResult: 1,
		}, false, false},
		{"fractional failure line number", suiteFixture{
			report: mutateLocation(func(location map[string]any) {
				location["LineNumber"] = 42.5
			}), goResult: 1,
		}, false, false},
		{"zero selected specs", suiteFixture{
			report: ginkgoReportFixture(t, true, 2, 0, gingkotypes.SpecReports{skipped, skipped}),
		}, false, false},
		{"fractional selected count", suiteFixture{
			report: mutateCount("SpecsThatWillRun", 0.5),
		}, false, false},
		{"selected count exceeds total", suiteFixture{
			report: mutateCount("SpecsThatWillRun", 2),
		}, false, false},
		{"negative selected count", suiteFixture{
			report: mutateCount("SpecsThatWillRun", -1),
		}, false, false},
		{"non-number selected count", suiteFixture{
			report: mutateCount("SpecsThatWillRun", "1"),
		}, false, false},
		{"missing selected count", suiteFixture{
			report: deleteCount("SpecsThatWillRun"),
		}, false, false},
		{"zero total count", suiteFixture{
			report: mutateCount("TotalSpecs", 0),
		}, false, false},
		{"negative total count", suiteFixture{
			report: mutateCount("TotalSpecs", -1),
		}, false, false},
		{"fractional total count", suiteFixture{
			report: mutateCount("TotalSpecs", 1.5),
		}, false, false},
		{"non-number total count", suiteFixture{
			report: mutateCount("TotalSpecs", "1"),
		}, false, false},
		{"missing total count", suiteFixture{
			report: deleteCount("TotalSpecs"),
		}, false, false},
		{"failed AfterEach in AdditionalFailures", suiteFixture{
			report:   ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{failedWithCleanup}),
			goResult: 1,
		}, false, false},
		{"timed out DeferCleanup in AdditionalFailures", suiteFixture{
			report:   ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{failedWithTimedOutCleanup}),
			goResult: 1,
		}, false, false},
		{"malformed AdditionalFailures collection", suiteFixture{
			report: malformedAdditionalFailures, goResult: 1,
		}, false, false},
		{"nested Failure AdditionalFailure", suiteFixture{
			report:   ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{failedWithNestedFailure}),
			goResult: 1,
		}, false, false},
		{"incomplete ordinary failure", suiteFixture{
			report:   ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{incompleteFailure}),
			goResult: 1,
		}, false, false},
		{"panic", suiteFixture{
			report: ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{panicked}), goResult: 1,
		}, false, false},
		{"timeout", suiteFixture{
			report: ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{timedOut}), goResult: 1,
		}, false, false},
		{"abort", suiteFixture{
			report: ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{aborted}), goResult: 1,
		}, false, false},
		{"interruption", suiteFixture{
			report: ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{interrupted}), goResult: 130,
		}, false, false},
		{"exit 1 with successful report", suiteFixture{report: safeReport, goResult: 1}, false, false},
		{"exit 1 with inconsistent report", suiteFixture{
			report: inconsistentFailureReport, goResult: 1,
		}, false, false},
		{"missing report", suiteFixture{goResult: 1, staleReport: safeReport}, false, false},
		{"malformed report", suiteFixture{report: "{", goResult: 1}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runSuiteFixture(t, suite.Run, tt.fixture)
			if (result.err == nil) != tt.stepSuccess {
				t.Errorf("step error = %v, output:\n%s", result.err, result.output)
			}
			if got := result.outputs["suite_ran"]; got != trueValue {
				t.Errorf("suite_ran = %q, want true", got)
			}
			if got := result.outputs["manager_attested"]; got != trueValue {
				t.Errorf("manager_attested = %q, want true", got)
			}
			got, exists := result.outputs["suite_completed"]
			if tt.completed {
				if !exists || got != trueValue {
					t.Errorf("suite_completed = %q, exists %t, want literal true", got, exists)
				}
			} else if exists {
				t.Errorf("unsafe suite emitted suite_completed=%q", got)
			}
		})
	}
}

func TestFeatureE2ESuiteCompletionAcceptsPrunedStackAssertion(t *testing.T) {
	t.Setenv("GINKGO_PRUNE_STACK", "")
	prunedStack := gingkotypes.PruneStack(`ginkgo.internal
/workspace/ginkgo/internal.go:1
runtime.goexit
/usr/local/go/src/pkg/runtime/asm_amd64.s:1
`, 0)
	if prunedStack != "" {
		t.Fatalf("Ginkgo pruned stack = %q, want empty", prunedStack)
	}

	failed := ginkgoSpecFixture(gingkotypes.SpecStateFailed, "selected assertion")
	failed.Failure = gingkotypes.Failure{
		Message: "Expected migration to complete",
		Location: gingkotypes.CodeLocation{
			FileName:       "/workspace/test/e2e/pruned_stack_test.go",
			LineNumber:     42,
			FullStackTrace: prunedStack,
		},
		FailureNodeContext: gingkotypes.FailureNodeIsLeafNode,
		FailureNodeType:    gingkotypes.NodeTypeIt,
	}
	report := ginkgoReportFixture(t, false, 1, 1, gingkotypes.SpecReports{failed})
	var serialized []struct {
		SpecReports []struct {
			Failure struct {
				Location map[string]json.RawMessage
			}
		}
	}
	if err := json.Unmarshal([]byte(report), &serialized); err != nil {
		t.Fatal(err)
	}
	if _, exists := serialized[0].SpecReports[0].Failure.Location["FullStackTrace"]; exists {
		t.Fatal("Ginkgo serialized an empty FullStackTrace despite omitempty")
	}

	wf := parseProtectedWorkflow(t, featureWorkflow)
	suite := protectedStepNamed(t, wf.Jobs["cluster"], "Run non-chaos suite")
	result := runSuiteFixture(t, suite.Run, suiteFixture{report: report, goResult: 1})
	if result.err == nil {
		t.Error("assertion failure unexpectedly returned success")
	}
	if got := result.outputs["suite_completed"]; got != trueValue {
		t.Errorf("suite_completed = %q, want true", got)
	}
}

func TestFeatureE2ESuiteProcessBoundary(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	suite := protectedStepNamed(t, wf.Jobs["cluster"], "Run non-chaos suite")
	passed := ginkgoSpecFixture(gingkotypes.SpecStatePassed, "selected spec")
	result := runSuiteFixture(t, suite.Run, suiteFixture{
		report:            ginkgoReportFixture(t, true, 1, 1, gingkotypes.SpecReports{passed}),
		simulateLaterTest: true,
	})
	if result.err != nil {
		t.Errorf("isolated TestE2E process failed: %v\n%s", result.err, result.output)
	}
	if result.laterTestRan {
		t.Error("a later native Test function shared the classified Ginkgo process")
	}
	wantArgs := []string{
		"test", "./test/e2e/...", "-run", "^TestE2E$", "-v", "-timeout", "4h",
		"-ginkgo.v", "-ginkgo.timeout=4h", "-ginkgo.poll-progress-after=15m",
		"-ginkgo.label-filter=!chaos && !flaky", "-ginkgo.fail-on-empty",
		"-ginkgo.json-report=" + result.reportPath,
	}
	if !slices.Equal(result.args, wantArgs) {
		t.Errorf("go test arguments = %q, want %q", result.args, wantArgs)
	}
	verifyGoTestProcessBoundary(t, result.args)
	if got := result.outputs["suite_completed"]; got != trueValue {
		t.Errorf("suite_completed = %q, want true", got)
	}
}

func ginkgoSpecFixture(state gingkotypes.SpecState, text string) gingkotypes.SpecReport {
	return gingkotypes.SpecReport{
		LeafNodeType: gingkotypes.NodeTypeIt,
		LeafNodeText: text,
		State:        state,
	}
}

func ginkgoReportFixture(
	t *testing.T, succeeded bool, total, selected int, specs gingkotypes.SpecReports,
) string {
	t.Helper()
	body, err := json.Marshal([]gingkotypes.Report{{
		SuitePath:                  "/workspace/test/e2e",
		SuiteDescription:           "pgcopydb-operator e2e suite",
		SuiteSucceeded:             succeeded,
		SpecialSuiteFailureReasons: []string{},
		PreRunStats: gingkotypes.PreRunStats{
			TotalSpecs:       total,
			SpecsThatWillRun: selected,
		},
		SpecReports: specs,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func setGinkgoSpecReportField(
	t *testing.T, report string, index int, name string, value any,
) string {
	t.Helper()
	return mutateGinkgoReport(t, report, func(document map[string]any) {
		specs, ok := document["SpecReports"].([]any)
		if !ok {
			t.Fatal("fixture SpecReports is not an array")
		}
		spec, ok := specs[index].(map[string]any)
		if !ok {
			t.Fatal("fixture SpecReport is not an object")
		}
		spec[name] = value
	})
}

func mutateGinkgoReport(t *testing.T, report string, mutate func(map[string]any)) string {
	t.Helper()
	var document []map[string]any
	if err := json.Unmarshal([]byte(report), &document); err != nil {
		t.Fatal(err)
	}
	mutate(document[0])
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

type suiteFixture struct {
	report            string
	goResult          int
	staleReport       string
	runMode           string
	focus             string
	simulateLaterTest bool
}

type suiteFixtureResult struct {
	outputs      map[string]string
	args         []string
	reportPath   string
	laterTestRan bool
	output       string
	err          error
}

func runSuiteFixture(
	t *testing.T, script string, fixture suiteFixture,
) suiteFixtureResult {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	ownerPath := filepath.Join(dir, "owner")
	outputPath := filepath.Join(dir, "github-output")
	reportSource := filepath.Join(dir, "fixture-report.json")
	suiteReport := filepath.Join(dir, "suite-report.json")
	argsPath := filepath.Join(dir, "go-args")
	laterTestPath := filepath.Join(dir, "later-test-ran")
	if err := os.WriteFile(ownerPath, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	if fixture.report != "" {
		if err := os.WriteFile(reportSource, []byte(fixture.report), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if fixture.staleReport != "" {
		if err := os.WriteFile(suiteReport, []byte(fixture.staleReport), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fakeGo := `#!/usr/bin/env bash
set -euo pipefail
suite_report=
exact_selector=false
previous=
printf '%s\n' "$@" > "$FAKE_ARGS_PATH"
for arg in "$@"; do
  case "$arg" in
    -ginkgo.json-report=*) suite_report=${arg#*=} ;;
  esac
  if [ "$previous" = -run ] && [ "$arg" = '^TestE2E$' ]; then
    exact_selector=true
  fi
  previous=$arg
done
[ -n "$suite_report" ] || exit 97
if [ -n "${FAKE_REPORT_SOURCE:-}" ]; then
  cp "$FAKE_REPORT_SOURCE" "$suite_report"
fi
: > "$FEATURE_E2E_HELPERS/manager-attested"
result=$FAKE_GO_RESULT
if [ "$FAKE_SIMULATE_LATER_TEST" = true ] && [ "$exact_selector" = false ]; then
  : > "$FAKE_LATER_TEST_PATH"
  result=1
fi
exit "$result"
`
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fakeGo), 0o700); err != nil {
		t.Fatal(err)
	}

	runMode := fixture.runMode
	if runMode == "" {
		runMode = fullModeValue
	}
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"FEATURE_E2E_HELPERS="+dir,
		"FEATURE_E2E_OWNER_FILE="+ownerPath,
		"GITHUB_OUTPUT="+outputPath,
		"RUN_MODE="+runMode,
		"E2E_FOCUS="+fixture.focus,
		"FAKE_GO_RESULT="+fmt.Sprint(fixture.goResult),
		"FAKE_ARGS_PATH="+argsPath,
		"FAKE_LATER_TEST_PATH="+laterTestPath,
		"FAKE_SIMULATE_LATER_TEST="+fmt.Sprint(fixture.simulateLaterTest),
	)
	if fixture.report != "" {
		cmd.Env = append(cmd.Env, "FAKE_REPORT_SOURCE="+reportSource)
	}
	output, runErr := cmd.CombinedOutput()
	outputs := map[string]string{}
	if body, err := os.ReadFile(outputPath); err == nil {
		for line := range strings.SplitSeq(strings.TrimSpace(string(body)), "\n") {
			name, value, ok := strings.Cut(line, "=")
			if ok {
				outputs[name] = value
			}
		}
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	args := []string{}
	if body, err := os.ReadFile(argsPath); err == nil {
		args = strings.Split(strings.TrimSpace(string(body)), "\n")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	_, statErr := os.Stat(laterTestPath)
	if statErr != nil && !os.IsNotExist(statErr) {
		t.Fatal(statErr)
	}
	return suiteFixtureResult{
		outputs: outputs, args: args, reportPath: suiteReport,
		laterTestRan: statErr == nil, output: string(output), err: runErr,
	}
}

func verifyGoTestProcessBoundary(t *testing.T, args []string) {
	t.Helper()
	selector := ""
	for i := range len(args) - 1 {
		if args[i] == "-run" {
			selector = args[i+1]
			break
		}
	}
	if selector == "" {
		t.Fatal("go test arguments have no selector")
	}

	dir := t.TempDir()
	files := map[string]string{
		"go.mod": "module example.invalid/feature-e2e-selector\n\ngo 1.25.0\n",
		"selector_test.go": `package selector

import "testing"

func TestE2E(t *testing.T) {}

func TestLaterNative(t *testing.T) {
	panic("later native Test ran")
}
`,
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(testArgs ...string) ([]byte, error) {
		cmd := exec.Command("go", append([]string{"test", ".", "-count=1"}, testArgs...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOWORK=off")
		return cmd.CombinedOutput()
	}
	if output, err := run(); err == nil || !strings.Contains(string(output), "TestLaterNative") {
		t.Fatalf("unselected Go fixture did not run the later test: %v\n%s", err, output)
	}
	if output, err := run("-run", selector); err != nil {
		t.Fatalf("exact Go test selector did not isolate TestE2E: %v\n%s", err, output)
	}
}

func TestFeatureE2ECleanupOwnership(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	helpers := protectedStepNamed(t, wf.Jobs["cluster"], "Write cluster helpers").Run
	cleanup := extractCleanupHeredoc(t, helpers)
	for _, banned := range []string{
		"--all", "app.kubernetes.io/managed-by=pgcopydb-operator", "kubectl delete namespace", "finalizers-",
	} {
		if strings.Contains(cleanup, banned) {
			t.Fatalf("cleanup contains forbidden authority %q", banned)
		}
	}
	for _, required := range []string{
		`-l "$owner_selector"`, "feature_uninstall_controller", "require_empty",
	} {
		if !strings.Contains(cleanup, required) {
			t.Errorf("cleanup is missing %q", required)
		}
	}
}

func extractCleanupHeredoc(t *testing.T, helpers string) string {
	t.Helper()
	const startMarker = `cat > "$FEATURE_E2E_HELPERS/cleanup" <<'EOF_CLEANUP'` + "\n"
	start := strings.Index(helpers, startMarker)
	if start < 0 {
		t.Fatal("cluster helpers have no cleanup heredoc")
	}
	start += len(startMarker)
	end := strings.Index(helpers[start:], "\nEOF_CLEANUP\n")
	if end < 0 {
		t.Fatal("cluster cleanup heredoc is not terminated")
	}
	return helpers[start : start+end]
}

func TestFeatureE2EChartOwnershipLifecycle(t *testing.T) { //nolint:gocyclo // One fake API owns the lifecycle contract.
	wf := parseProtectedWorkflow(t, featureWorkflow)
	helpers := protectedStepNamed(t, wf.Jobs["cluster"], "Write cluster helpers").Run
	scripts := featureOwnershipScripts{
		postRenderer: extractFeatureGeneratedFile(t, helpers, "post-renderer", "POST_RENDERER"),
		plugin: extractFeatureGeneratedFile(t, helpers,
			"plugins/"+featurePostRendererPlugin+"/plugin.yaml", "HELM_PLUGIN"),
		renderSafety: buildFeatureRenderSafety(t,
			extractFeatureGeneratedFile(t, helpers, "render-safety.go", "RENDER_SAFETY")),
		ownership: extractFeatureGeneratedFile(t, helpers, "ownership", "OWNERSHIP"),
		helm:      extractFeatureHelmHeredoc(t, helpers),
		cleanup:   extractCleanupHeredoc(t, helpers),
	}

	t.Run("partial install is labelled before native rollback", func(t *testing.T) {
		for _, scenario := range []string{
			"rollback-success", "rollback-failure", rollbackFailureNoController,
		} {
			t.Run(scenario, func(t *testing.T) {
				initialState := ""
				if scenario == rollbackFailureNoController {
					initialState = "services|customer-system|service/customer|" +
						"88888888-8888-4888-8888-888888888888|someone-else|customer|customer-system|false\n"
				}
				fixture := newFeatureOwnershipFixture(t, scripts, scenario, initialState)
				result := fixture.install()
				if result.err == nil {
					t.Fatal("failed install unexpectedly succeeded")
				}
				if strings.Contains(result.output+fixture.readLog(), fixture.owner) {
					t.Fatal("install or recovery printed the ownership value")
				}
				if !strings.Contains(fixture.readLog(), "create:labelled") {
					t.Fatal("partial install was not labelled before creation")
				}
				if scenario == "rollback-success" && fixture.hasOwnedState() {
					t.Fatal("successful native rollback left run-owned chart objects")
				}
				if strings.HasPrefix(scenario, "rollback-failure") &&
					!strings.Contains(fixture.readLog(), "rollback:failed") {
					t.Fatal("failed native rollback was not observed")
				}
				if scenario == rollbackFailureNoController {
					if fixture.hasOwnedState() {
						t.Fatal("bounded recovery left a partial run-owned chart object")
					}
					if !strings.Contains(fixture.readLog(), "uninstall\n") {
						t.Fatal("bounded recovery did not release the fixed Helm slot")
					}
					if release := strings.TrimSpace(fixture.read("release")); release != "" {
						t.Fatalf("bounded recovery left release %q", release)
					}
					fixture.write("scenario", "success")
					if retry := fixture.install(); retry.err != nil {
						t.Fatalf("clean reinstall failed: %v\n%s", retry.err, retry.output)
					}
					if !strings.Contains(fixture.read(stateKey),
						"deployments|"+featureControllerNS+"|deployment.apps/pgcopydb-e2e|") ||
						strings.TrimSpace(fixture.read("controller.uid")) != featureControllerUID ||
						!strings.Contains(fixture.readLog(), "rollout:ready\n") {
						t.Fatal("clean reinstall did not reach the expected Ready controller state")
					}
					if !strings.Contains(fixture.read(stateKey), "service/customer") {
						t.Fatal("bounded recovery deleted an unrelated resource")
					}
					if cleanup := fixture.cleanup(); cleanup.err != nil {
						t.Fatalf("final cleanup failed: %v\n%s", cleanup.err, cleanup.output)
					}
					if fixture.hasOwnedState() || strings.TrimSpace(fixture.read("release")) != "" {
						t.Fatal("final cleanup left run-owned state or the fixed Helm slot occupied")
					}
					if !strings.Contains(fixture.read(stateKey), "service/customer") {
						t.Fatal("final cleanup deleted an unrelated resource")
					}
				}
			})
		}
	})

	t.Run("failed no-controller recovery cannot claim reinstall success", func(t *testing.T) {
		fixture := newFeatureOwnershipFixture(t, scripts,
			"rollback-failure-no-controller-uninstall-failure", "")
		if result := fixture.install(); result.err == nil {
			t.Fatal("install succeeded despite failed recovery uninstall")
		}
		if release := strings.TrimSpace(fixture.read("release")); release != featureControllerName {
			t.Fatalf("failed recovery release = %q, want occupied fixed slot", release)
		}
		fixture.write("scenario", "success")
		if retry := fixture.install(); retry.err == nil ||
			!strings.Contains(fixture.readLog(), "install:occupied\n") {
			t.Fatalf("occupied fixed slot was reported as a successful reinstall: %v\n%s",
				retry.err, retry.output)
		}
	})

	t.Run("pre-controller recovery requires a proven empty slot", func(t *testing.T) {
		fixture := newFeatureOwnershipFixture(t, scripts, "success", "")
		if result := fixture.run(fixture.path("cleanup"), "recovery"); result.err != nil {
			t.Fatalf("proven empty recovery failed: %v\n%s", result.err, result.output)
		}
		if strings.Contains(fixture.readLog(), "uninstall\n") {
			t.Fatal("empty recovery invoked Helm uninstall")
		}

		fixture = newFeatureOwnershipFixture(t, scripts, "success", "")
		fixture.write("rendered.yaml", "")
		if result := fixture.run(fixture.path("cleanup"), "recovery"); result.err == nil {
			t.Fatal("empty rendered-manifest evidence was accepted")
		}

		fixture = newFeatureOwnershipFixture(t, scripts, "success", "")
		fixture.write("release", featureControllerName)
		if result := fixture.run(fixture.path("cleanup"), "recovery"); result.err == nil {
			t.Fatal("occupied fixed Helm slot was accepted")
		}
	})

	t.Run("Namespace render is rejected before creation", func(t *testing.T) {
		tests := []struct {
			name     string
			manifest string
		}{
			{"top level", "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: forbidden\n"},
			{"List member", `apiVersion: v1
kind: List
items:
  - apiVersion: v1
    kind: Namespace
    metadata:
      name: forbidden
`},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				fixture := newFeatureOwnershipFixture(t, scripts, "success", "")
				fixture.write("manifest.yaml", tt.manifest)
				result := fixture.install()
				if result.err == nil {
					t.Fatal("Namespace render unexpectedly succeeded")
				}
				if strings.Contains(fixture.readLog(), "create:labelled") {
					t.Fatal("Namespace render reached fake Kubernetes creation")
				}
				if strings.Contains(result.output+fixture.readLog(), fixture.owner) {
					t.Fatal("Namespace rejection printed the ownership value")
				}
			})
		}
	})

	t.Run("successful install labels the complete render and captures exact ownership", func(t *testing.T) {
		fixture := newFeatureOwnershipFixture(t, scripts, "success", "")
		result := fixture.install()
		if result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		rendered := fixture.read("rendered.yaml")
		if got := strings.Count(rendered, imageAttestationOwnerKey+": "+fixture.owner); got != 3 {
			t.Fatalf("rendered ownership labels = %d, want 3\n%s", got, rendered)
		}
		if got := strings.TrimSpace(fixture.read("controller.uid")); got != featureControllerUID {
			t.Fatalf("captured controller UID = %q", got)
		}
		args := strings.Fields(fixture.read("helm-args"))
		for _, want := range []string{
			"--rollback-on-failure", "--timeout=5m", helmPostRendererFlag, featurePostRendererPlugin,
		} {
			if !slices.Contains(args, want) {
				t.Errorf("trusted Helm arguments are missing %q: %v", want, args)
			}
		}
		if strings.Contains(result.output+fixture.readLog(), fixture.owner) {
			t.Fatal("successful install printed the ownership value")
		}
	})

	t.Run("post-install attestation failure frees the fixed slot before retry", func(t *testing.T) {
		fixture := newFeatureOwnershipFixture(t, scripts, "success", "")
		fixture.writeMode("attest-image", "#!/usr/bin/env bash\nexit 42\n", 0o700)
		if result := fixture.install(); result.err == nil {
			t.Fatal("failed post-install attestation unexpectedly succeeded")
		}
		if fixture.hasOwnedState() || strings.TrimSpace(fixture.read("release")) != "" {
			t.Fatal("post-install attestation failure left owned state or the fixed Helm slot")
		}
		if !strings.Contains(fixture.readLog(), "uninstall\n") {
			t.Fatal("post-install attestation failure did not run bounded recovery")
		}
		fixture.writeMode("attest-image", "#!/usr/bin/env bash\nexit 0\n", 0o700)
		if retry := fixture.install(); retry.err != nil {
			t.Fatalf("clean retry after post-install failure failed: %v\n%s", retry.err, retry.output)
		}
		if cleanup := fixture.cleanup(); cleanup.err != nil {
			t.Fatalf("cleanup after clean retry failed: %v\n%s", cleanup.err, cleanup.output)
		}
	})

	t.Run("foreground uninstall waits for the controller Pod", func(t *testing.T) {
		fixture := newFeatureOwnershipFixture(t, scripts, "pod-termination", "")
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		if cleanup := fixture.cleanup(); cleanup.err != nil {
			t.Fatalf("foreground cleanup failed: %v\n%s", cleanup.err, cleanup.output)
		}
		if fixture.hasOwnedState() {
			t.Fatal("foreground cleanup returned before the controller Pod disappeared")
		}
	})

	t.Run("cleanup rejects ownership changes before Helm", func(t *testing.T) {
		tests := []struct {
			name     string
			mutation string
		}{
			{"missing label", "missing-label"},
			{"wrong label", "wrong-label"},
			{"wrong release", "wrong-release"},
			{"wrong release namespace", "wrong-release-namespace"},
			{"wrong namespace", "wrong-namespace"},
			{"wrong name", "wrong-name"},
			{"different UID", "different-uid"},
			{"unowned replacement", "unowned-replacement"},
			{"customer release redirect", "customer-release"},
			{"ambiguous owned controller", "ambiguous"},
			{"unreadable controller", "unreadable"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				fixture := newFeatureOwnershipFixture(t, scripts, "success", "")
				if result := fixture.install(); result.err != nil {
					t.Fatalf("install failed: %v\n%s", result.err, result.output)
				}
				fixture.write("mutation", tt.mutation)
				before := strings.Count(fixture.readLog(), "uninstall\n")
				result := fixture.cleanup()
				if result.err == nil {
					t.Fatal("unsafe controller was accepted")
				}
				after := strings.Count(fixture.readLog(), "uninstall\n")
				if after != before {
					t.Fatal("cleanup invoked Helm after controller ownership changed")
				}
			})
		}
	})

	t.Run("captured deletion race is idempotent but never observed absence is not", func(t *testing.T) {
		fixture := newFeatureOwnershipFixture(t, scripts, "success", "")
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		fixture.write("mutation", "not-found")
		if result := fixture.cleanup(); result.err != nil {
			t.Fatalf("captured NotFound was not idempotent: %v\n%s", result.err, result.output)
		}
		if !strings.Contains(fixture.readLog(), "uninstall\n") {
			t.Fatal("captured NotFound did not recover the exact release")
		}

		fixture = newFeatureOwnershipFixture(t, scripts, "success", "")
		fixture.write("release", "pgcopydb-e2e")
		result := fixture.cleanup()
		if result.err == nil || strings.Contains(fixture.readLog(), "uninstall\n") {
			t.Fatalf("never-observed absence was accepted: %v\n%s", result.err, result.output)
		}
	})

	t.Run("captured resource races are safe and replacements are rejected", func(t *testing.T) {
		state := "migrations.pgcopydb-operator.io|pgcopydb-e2e|" +
			"migration.pgcopydb-operator.io/owned|" + migrationUID + "|" +
			imageAttestationOwnerValue + "|||false\n"
		fixture := newFeatureOwnershipFixture(t, scripts, "success", state)
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		fixture.write("mutation", "drop-migration")
		if result := fixture.cleanup(); result.err != nil {
			t.Fatalf("captured Migration NotFound was not idempotent: %v\n%s", result.err, result.output)
		}

		fixture = newFeatureOwnershipFixture(t, scripts, "success", state)
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		fixture.write("mutation", "replace-migration")
		result := fixture.cleanup()
		if result.err == nil || strings.Contains(fixture.readLog(), "delete:migration\n") {
			t.Fatalf("replacement Migration was deleted: %v\n%s", result.err, result.output)
		}

		fixture = newFeatureOwnershipFixture(t, scripts, "success", state)
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		fixture.write("mutation", "replace-on-delete")
		result = fixture.cleanup()
		if result.err == nil || strings.Contains(fixture.readLog(), "delete:migration\n") {
			t.Fatalf("UID precondition deleted a replacement Migration: %v\n%s", result.err, result.output)
		}

		fixture = newFeatureOwnershipFixture(t, scripts, "success", state)
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		fixture.write("mutation", "replace-after-delete")
		result = fixture.cleanup()
		if result.err == nil || !strings.Contains(fixture.read(stateKey),
			"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa") {
			t.Fatalf("replacement during deletion did not survive: %v\n%s", result.err, result.output)
		}
	})

	t.Run("cleanup order is label-only and rejects owned remainder", func(t *testing.T) {
		initial := strings.Join([]string{
			"migrations.pgcopydb-operator.io|pgcopydb-e2e|migration.pgcopydb-operator.io/owned|" +
				migrationUID + "|" + imageAttestationOwnerValue + "|||false",
			"jobs|pgcopydb-e2e|job.batch/owned|" + jobUID + "|" + imageAttestationOwnerValue + "|||false",
			"migrations.pgcopydb-operator.io|pgcopydb-e2e|migration.pgcopydb-operator.io/customer|" +
				customerMigrationUID + "|||||false",
			"jobs|pgcopydb-e2e|job.batch/customer|" + customerJobUID + "|someone-else|||false",
			"deployments|customer-system|deployment.apps/customer|" + customerControllerUID +
				"|someone-else|customer|customer-system|false",
			"pods|customer-system|pod/customer|" + customerPodUID + "|someone-else|customer|customer-system|false",
		}, "\n") + "\n"
		fixture := newFeatureOwnershipFixture(t, scripts, "success", initial)
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		if result := fixture.cleanup(); result.err != nil {
			t.Fatalf("cleanup failed: %v\n%s", result.err, result.output)
		}
		log := fixture.readLog()
		migration := strings.Index(log, "delete:migration\n")
		job := strings.Index(log, "delete:job\n")
		controller := strings.Index(log, "uninstall\n")
		if migration < 0 || job <= migration || controller <= job {
			t.Fatalf("cleanup order is unsafe:\n%s", log)
		}
		state := fixture.read(stateKey)
		for _, survivor := range []string{
			"migration.pgcopydb-operator.io/customer", "job.batch/customer",
			"deployment.apps/customer", "pod/customer",
		} {
			if !strings.Contains(state, survivor) {
				t.Errorf("cleanup deleted %s", survivor)
			}
		}

		fixture = newFeatureOwnershipFixture(t, scripts, "remainder", initial)
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		if result := fixture.cleanup(); result.err == nil {
			t.Fatal("owned remainder was accepted")
		}
	})

	t.Run("rendered object read errors fail cleanup", func(t *testing.T) {
		fixture := newFeatureOwnershipFixture(t, scripts, "success", "")
		if result := fixture.install(); result.err != nil {
			t.Fatalf("install failed: %v\n%s", result.err, result.output)
		}
		fixture.write("mutation", "rendered-unreadable")
		result := fixture.cleanup()
		if result.err == nil || !strings.Contains(result.output,
			"unable to inspect rendered feature objects") {
			t.Fatalf("rendered object read error was accepted: %v\n%s", result.err, result.output)
		}
	})

	t.Run("malformed render fails closed before creation", func(t *testing.T) {
		fixture := newFeatureOwnershipFixture(t, scripts, "malformed-render", "")
		result := fixture.install()
		if result.err == nil || strings.Contains(fixture.readLog(), "create:labelled") {
			t.Fatalf("malformed render reached creation: %v\n%s", result.err, result.output)
		}
		if strings.Contains(result.output+fixture.readLog(), fixture.owner) {
			t.Fatal("render failure printed the ownership value")
		}
	})
}

const (
	featureControllerUID  = "11111111-1111-4111-8111-111111111111"
	migrationUID          = "22222222-2222-4222-8222-222222222222"
	jobUID                = "33333333-3333-4333-8333-333333333333"
	customerMigrationUID  = "44444444-4444-4444-8444-444444444444"
	customerJobUID        = "55555555-5555-4555-8555-555555555555"
	customerControllerUID = "66666666-6666-4666-8666-666666666666"
	customerPodUID        = "77777777-7777-4777-8777-777777777777"
)

type featureOwnershipScripts struct {
	postRenderer string
	plugin       string
	renderSafety string
	ownership    string
	helm         string
	cleanup      string
}

type featureOwnershipFixture struct {
	t     *testing.T
	dir   string
	owner string
}

type featureOwnershipResult struct {
	output string
	err    error
}

func extractFeatureGeneratedFile(t *testing.T, helpers, name, marker string) string {
	t.Helper()
	startMarker := fmt.Sprintf("cat > \"$FEATURE_E2E_HELPERS/%s\" <<'EOF_%s'\n", name, marker)
	start := strings.Index(helpers, startMarker)
	if start < 0 {
		t.Fatalf("cluster helpers have no %s heredoc", name)
	}
	start += len(startMarker)
	endMarker := "\nEOF_" + marker + "\n"
	end := strings.Index(helpers[start:], endMarker)
	if end < 0 {
		t.Fatalf("cluster %s heredoc is not terminated", name)
	}
	return helpers[start : start+end]
}

func newFeatureOwnershipFixture(
	t *testing.T, scripts featureOwnershipScripts, scenario, initialState string,
) *featureOwnershipFixture {
	t.Helper()
	dir := t.TempDir()
	fixture := &featureOwnershipFixture{t: t, dir: dir, owner: imageAttestationOwnerValue}
	pluginDir := filepath.Join(dir, "plugins", featurePostRendererPlugin)
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]struct {
		body string
		mode os.FileMode
	}{
		"post-renderer": {scripts.postRenderer, 0o700},
		"plugins/" + featurePostRendererPlugin + "/plugin.yaml": {scripts.plugin, 0o600},
		"ownership":        {scripts.ownership, 0o600},
		"bin/feature-helm": {scripts.helm, 0o700},
		"cleanup":          {scripts.cleanup, 0o700},
		"owner":            {fixture.owner, 0o600},
		stateKey:           {initialState, 0o600},
		"release":          {"", 0o600},
		"log":              {"", 0o600},
		"helm-args":        {"", 0o600},
		"mutation":         {"", 0o600},
		"manifest.yaml": {`apiVersion: apps/v1
kind: Deployment
metadata:
  name: pgcopydb-e2e
  namespace: pgcopydb-e2e
spec:
  template:
    metadata: {}
---
apiVersion: v1
kind: Service
metadata:
  name: pgcopydb-e2e-metrics
  namespace: pgcopydb-e2e
`, 0o600},
		"attest-image": {"#!/usr/bin/env bash\nexit 0\n", 0o700},
		"bin/helm":     {featureOwnershipHelmFixture, 0o700},
		"bin/kubectl":  {featureOwnershipKubectlFixture, 0o700},
	}
	for name, file := range files {
		fixture.writeMode(name, file.body, file.mode)
	}
	if err := os.Symlink(scripts.renderSafety, fixture.path("bin/render-safety")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../post-renderer", filepath.Join(pluginDir, "post-renderer")); err != nil {
		t.Fatal(err)
	}
	fixture.write("scenario", scenario)
	return fixture
}

func buildFeatureRenderSafety(t *testing.T, source string) string {
	t.Helper()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "render-safety.go")
	binaryPath := filepath.Join(dir, "render-safety")
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "build", "-o", binaryPath, sourcePath)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build render safety helper: %v\n%s", err, output)
	}
	return binaryPath
}

func (f *featureOwnershipFixture) path(name string) string {
	f.t.Helper()
	return filepath.Join(f.dir, name)
}

func (f *featureOwnershipFixture) write(name, body string) {
	f.t.Helper()
	f.writeMode(name, body, 0o600)
}

func (f *featureOwnershipFixture) writeMode(name, body string, mode os.FileMode) {
	f.t.Helper()
	if err := os.WriteFile(f.path(name), []byte(body), mode); err != nil {
		f.t.Fatal(err)
	}
}

func (f *featureOwnershipFixture) read(name string) string {
	f.t.Helper()
	body, err := os.ReadFile(f.path(name))
	if err != nil {
		f.t.Fatal(err)
	}
	return string(body)
}

func (f *featureOwnershipFixture) readLog() string {
	f.t.Helper()
	return f.read("log")
}

func (f *featureOwnershipFixture) hasOwnedState() bool {
	f.t.Helper()
	return strings.Contains(f.read(stateKey), "|"+f.owner+"|")
}

func (f *featureOwnershipFixture) install() featureOwnershipResult {
	f.t.Helper()
	return f.run(f.path("bin/feature-helm"), helmInstallCommand, featureControllerName,
		featureFixtureChart, "-n", featureControllerNS, "--wait")
}

func (f *featureOwnershipFixture) cleanup() featureOwnershipResult {
	f.t.Helper()
	return f.run(f.path("cleanup"))
}

func (f *featureOwnershipFixture) run(command string, args ...string) featureOwnershipResult {
	f.t.Helper()
	cmd := exec.Command(command, args...)
	cmd.Env = append(os.Environ(),
		"PATH="+f.path("bin")+":"+os.Getenv("PATH"),
		"E2E_OPERATOR_NAMESPACE="+featureControllerNS,
		"REAL_HELM="+f.path("bin/helm"),
		"FEATURE_E2E_HELPERS="+f.dir,
		"FEATURE_E2E_OWNER_KEY="+imageAttestationOwnerKey,
		"FEATURE_E2E_OWNER_FILE="+f.path("owner"),
		"FAKE_STATE="+f.path(stateKey),
		"FAKE_RELEASE="+f.path("release"),
		"FAKE_LOG="+f.path("log"),
		"FAKE_ARGS="+f.path("helm-args"),
		"FAKE_MUTATION="+f.path("mutation"),
		"FAKE_SCENARIO="+f.path("scenario"),
		"FAKE_MANIFEST="+f.path("manifest.yaml"),
		"FAKE_OWNER="+f.owner,
		"FAKE_TEST_BINARY="+os.Args[0],
		"FEATURE_E2E_KUSTOMIZE_HELPER=1",
	)
	output, err := cmd.CombinedOutput()
	return featureOwnershipResult{output: string(output), err: err}
}

const featureOwnershipHelmFixture = `#!/usr/bin/env bash
set -euo pipefail
scenario=$(<"$FAKE_SCENARIO")
case "${1:-}" in
  install)
    [ ! -s "$FAKE_RELEASE" ] || {
      printf 'install:occupied\n' >> "$FAKE_LOG"
      exit 96
    }
    printf '%s ' "$@" > "$FAKE_ARGS"
    post_renderer=
    rollback_on_failure=false
    args=("$@")
    for ((i=0; i<${#args[@]}; i++)); do
      case "${args[$i]}" in
        --rollback-on-failure) rollback_on_failure=true ;;
        --post-renderer) post_renderer=${args[$((i+1))]:-} ;;
      esac
    done
    [ "$rollback_on_failure" = true ] && [ "$post_renderer" = feature-e2e-postrenderer ] || exit 90
    post_renderer="$HELM_PLUGINS/$post_renderer/post-renderer"
    [ -x "$post_renderer" ] || exit 90
    manifest=$(<"$FAKE_MANIFEST")
    [ "$scenario" != malformed-render ] || manifest='kind: []'
    if ! printf '%s\n' "$manifest" | "$post_renderer" > "$FEATURE_E2E_HELPERS/rendered.yaml"; then
      exit 91
    fi
    [ "$(grep -c "$FEATURE_E2E_OWNER_KEY: $FAKE_OWNER" "$FEATURE_E2E_HELPERS/rendered.yaml")" -eq 3 ] || exit 92
    printf 'create:labelled\n' >> "$FAKE_LOG"
    controller="deployments|$E2E_OPERATOR_NAMESPACE|deployment.apps/pgcopydb-e2e|"
    controller+="11111111-1111-4111-8111-111111111111|$FAKE_OWNER|pgcopydb-e2e|$E2E_OPERATOR_NAMESPACE|true"
    service="services|$E2E_OPERATOR_NAMESPACE|service/pgcopydb-e2e-metrics|"
    service+="88888888-8888-4888-8888-888888888888|$FAKE_OWNER|pgcopydb-e2e|$E2E_OPERATOR_NAMESPACE|true"
    printf '%s\n' "$controller" "$service" >> "$FAKE_STATE"
    if [ "$scenario" = pod-termination ]; then
      pod="pods|$E2E_OPERATOR_NAMESPACE|pod/pgcopydb-e2e-fixture|"
      pod+="77777777-7777-4777-8777-777777777777|$FAKE_OWNER|pgcopydb-e2e|$E2E_OPERATOR_NAMESPACE|true"
      printf '%s\n' "$pod" >> "$FAKE_STATE"
    fi
    printf 'pgcopydb-e2e' > "$FAKE_RELEASE"
    case "$scenario" in
      rollback-success)
        awk -F'|' '$8 != "true"' "$FAKE_STATE" > "$FAKE_STATE.tmp"
        mv "$FAKE_STATE.tmp" "$FAKE_STATE"
        : > "$FAKE_RELEASE"
        printf 'rollback:succeeded\n' >> "$FAKE_LOG"
        exit 1
        ;;
      rollback-failure)
        printf 'rollback:failed\n' >> "$FAKE_LOG"
        exit 1
        ;;
      rollback-failure-no-controller|rollback-failure-no-controller-uninstall-failure)
        awk -F'|' '$1 != "deployments"' "$FAKE_STATE" > "$FAKE_STATE.tmp"
        mv "$FAKE_STATE.tmp" "$FAKE_STATE"
        printf 'rollback:failed\n' >> "$FAKE_LOG"
        exit 1
        ;;
    esac
    ;;
  uninstall)
    case "$*" in
      "uninstall pgcopydb-e2e -n $E2E_OPERATOR_NAMESPACE --cascade foreground --wait --timeout=5m"|\
      "uninstall pgcopydb-e2e -n $E2E_OPERATOR_NAMESPACE --cascade foreground --wait "\
"--timeout=5m --ignore-not-found") ;;
      *) exit 93 ;;
    esac
    printf 'uninstall\n' >> "$FAKE_LOG"
    if [ "$scenario" = uninstall-failure ] ||
       [ "$scenario" = rollback-failure-no-controller-uninstall-failure ]; then
      exit 1
    fi
    if [ "$scenario" = pod-termination ]; then
      awk -F'|' '$8 != "true" || $1 == "pods"' "$FAKE_STATE" > "$FAKE_STATE.tmp"
    else
      awk -F'|' '$8 != "true"' "$FAKE_STATE" > "$FAKE_STATE.tmp"
    fi
    if [ "$scenario" = remainder ]; then
      remainder="services|$E2E_OPERATOR_NAMESPACE|service/remains|"
      remainder+="99999999-9999-4999-8999-999999999999|$FAKE_OWNER|pgcopydb-e2e|$E2E_OPERATOR_NAMESPACE|true"
      printf '%s\n' "$remainder" >> "$FAKE_STATE.tmp"
    fi
    mv "$FAKE_STATE.tmp" "$FAKE_STATE"
    : > "$FAKE_RELEASE"
    ;;
  list)
    [ "$*" = "list -n $E2E_OPERATOR_NAMESPACE -q --filter ^pgcopydb-e2e$" ] || exit 94
    [ ! -s "$FAKE_RELEASE" ] || printf 'pgcopydb-e2e\n'
    ;;
  *) exit 95 ;;
esac
`

const featureOwnershipKubectlFixture = `#!/usr/bin/env bash
set -euo pipefail
mutation=$(<"$FAKE_MUTATION")
scenario=$(<"$FAKE_SCENARIO")
if [ "${1:-}" = kustomize ]; then
  directory=$2
  [ -s "$directory/manifest.yaml" ] || exit 80
  grep -Fq "includeTemplates: true" "$directory/kustomization.yaml" || exit 82
  grep -Fq "$FEATURE_E2E_OWNER_KEY: $FAKE_OWNER" "$directory/kustomization.yaml" || exit 83
  printf 'kustomize:input\n' >> "$FAKE_LOG"
  exec "$FAKE_TEST_BINARY" -test.run '^TestFeatureE2EKustomizeHelper$' -- \
    "$directory/manifest.yaml"
fi
command=${1:-}
shift || true
resource=${1:-}
shift || true
namespace=
selector=
output=
name=
from_file=false
ignore_not_found=false
raw_path=
if [ "$resource" = --raw ]; then
  raw_path=${1:-}
  shift || true
  resource=raw
fi
if [ "$resource" = -f ]; then
  resource_file=${1:-}
  shift || true
  from_file=true
fi
if [[ "$resource" == */* ]]; then
  name=${resource#*/}
  resource=${resource%%/*}
  [ "$resource" != migrations.pgcopydb-operator.io ] || resource=migrations.pgcopydb-operator.io
fi
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n) namespace=$2; shift 2 ;;
    -l) selector=$2; shift 2 ;;
    -o) output=$2; shift 2 ;;
    --ignore-not-found) ignore_not_found=true; shift ;;
    --wait=true|--cascade=foreground) shift ;;
    --timeout=*) shift ;;
    -f) resource_file=$2; from_file=true; shift 2 ;;
    *) shift ;;
  esac
done
if [ "$command" = rollout ]; then
  printf 'rollout:ready\n' >> "$FAKE_LOG"
  exit 0
fi
if [ "$command" = get ] && [ "$mutation" = unreadable ] && [ "$resource" = deployments ]; then
  exit 79
fi
if [ "$command" = get ] && [ "$mutation" = rendered-unreadable ] && [ "$from_file" = true ]; then
  exit 79
fi
owner=${selector#*=}
row_json() {
  local row=$1 resource_name=$2 ns=$3 object_name=$4 uid=$5 object_owner=$6 release=$7 release_ns=$8
  case "$mutation" in
    missing-label) object_owner= ;;
    wrong-label|unowned-replacement) object_owner=someone-else ;;
    wrong-release|customer-release) release=customer ;;
    wrong-release-namespace) release_ns=customer-system ;;
    wrong-namespace) ns=customer-system ;;
    wrong-name) object_name=deployment.apps/customer ;;
    different-uid|unowned-replacement) uid=aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa ;;
  esac
  jq -cn --arg resource "$resource_name" --arg namespace "$ns" --arg name "${object_name#*/}" \
    --arg uid "$uid" --arg owner "$object_owner" --arg release "$release" --arg release_ns "$release_ns" '
    {apiVersion:(if $resource == "deployments" then "apps/v1"
       elif $resource == "jobs" then "batch/v1"
       elif $resource == "migrations.pgcopydb-operator.io" then "pgcopydb-operator.io/v1beta1"
       else "v1" end),
     kind:(if $resource == "deployments" then "Deployment" elif $resource == "jobs" then "Job"
       elif $resource == "migrations.pgcopydb-operator.io" then "Migration" else "Service" end),
     metadata:{name:$name,namespace:$namespace,uid:$uid,labels:{},annotations:{}}} |
    .metadata.labels[env.FEATURE_E2E_OWNER_KEY]=$owner |
    .metadata.annotations["meta.helm.sh/release-name"]=$release |
    .metadata.annotations["meta.helm.sh/release-namespace"]=$release_ns'
}
if [ "$command" = get ]; then
  if [ "$mutation" = not-found ] && [ "$resource" = deployments ]; then
    [ "$output" != json ] || printf '{"apiVersion":"v1","kind":"List","items":[]}'
    exit 0
  fi
  rows=()
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    IFS='|' read -r row_resource row_ns row_name row_uid row_owner row_release row_release_ns row_chart <<< "$row"
    if [ "$from_file" = true ]; then
      [ "$row_chart" = true ] || continue
    else
      [ "$row_resource" = "$resource" ] || continue
    fi
    [ -z "$namespace" ] || [ "$row_ns" = "$namespace" ] || continue
    [ -z "$name" ] || [ "${row_name#*/}" = "$name" ] || continue
    [ -z "$selector" ] || [ "$row_owner" = "$owner" ] || continue
    rows+=("$(
      row_json "$row" "$row_resource" "$row_ns" "$row_name" \
        "$row_uid" "$row_owner" "$row_release" "$row_release_ns"
    )")
  done < "$FAKE_STATE"
  if [ "$scenario" = pod-termination ] && [ "$resource" = pods ] &&
     [ ! -s "$FAKE_RELEASE" ] && [ "${#rows[@]}" -gt 0 ] &&
     [ ! -e "$FAKE_STATE.pod-terminated" ]; then
    awk -F'|' '$1 != "pods"' "$FAKE_STATE" > "$FAKE_STATE.tmp"
    mv "$FAKE_STATE.tmp" "$FAKE_STATE"
    : > "$FAKE_STATE.pod-terminated"
  fi
  if [ "$mutation" = drop-migration ] && [ "$resource" = migrations.pgcopydb-operator.io ] &&
     [ -n "$selector" ] && [ ! -e "$FAKE_STATE.drop-migration" ]; then
    awk -F'|' -v owner="$owner" \
      '!( $1 == "migrations.pgcopydb-operator.io" && $5 == owner )' "$FAKE_STATE" > "$FAKE_STATE.tmp"
    mv "$FAKE_STATE.tmp" "$FAKE_STATE"
    : > "$FAKE_STATE.drop-migration"
  fi
  if [ "$mutation" = replace-migration ] && [ "$resource" = migrations.pgcopydb-operator.io ] &&
     [ -n "$selector" ] && [ ! -e "$FAKE_STATE.replace-migration" ]; then
    awk -F'|' 'BEGIN { OFS="|" }
      $1 == "migrations.pgcopydb-operator.io" { $4="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" }
      { print }' "$FAKE_STATE" > "$FAKE_STATE.tmp"
    mv "$FAKE_STATE.tmp" "$FAKE_STATE"
    : > "$FAKE_STATE.replace-migration"
  fi
  if [ "$mutation" = ambiguous ] && [ "$resource" = deployments ] && [ -n "$selector" ] && [ "${#rows[@]}" -eq 1 ]; then
    rows+=("${rows[0]}")
  fi
  if [ "$from_file" = true ] && [ "${#rows[@]}" -eq 0 ] && [ "$ignore_not_found" = false ]; then
    exit 1
  fi
  if [ "$output" = json ]; then
    if [ -n "$name" ]; then
      [ "${#rows[@]}" -eq 0 ] || printf '%s' "${rows[0]}"
    else
      printf '%s\n' "${rows[@]}" | jq -sc '{apiVersion:"v1",kind:"List",items:.}'
    fi
  elif [ "$output" = name ]; then
    printf '%s\n' "${rows[@]}" | jq -r '.kind + "/" + .metadata.name'
  fi
  exit 0
fi
if [ "$command" = delete ]; then
  if [ "$resource" = raw ]; then
    IFS=/ read -r _ api_root group version namespace_segment namespace resource name <<< "$raw_path"
    [ "$api_root" = apis ] && [ "$namespace_segment" = namespaces ] || exit 76
    case "$group/$version/$resource" in
      pgcopydb-operator.io/v1beta1/migrations) resource=migrations.pgcopydb-operator.io ;;
      batch/v1/jobs) resource=jobs ;;
      *) exit 76 ;;
    esac
    delete_options=$(cat)
    uid=$(jq -er '.preconditions.uid' <<< "$delete_options")
    propagation=$(jq -er '.propagationPolicy' <<< "$delete_options")
    if [ "$resource" = jobs ]; then
      [ "$propagation" = Foreground ] || exit 75
    else
      [ "$propagation" = Background ] || exit 75
    fi
    if [ "$mutation" = replace-on-delete ] && [ "$resource" = migrations.pgcopydb-operator.io ]; then
      awk -F'|' 'BEGIN { OFS="|" }
        $1 == "migrations.pgcopydb-operator.io" { $4="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" }
        { print }' "$FAKE_STATE" > "$FAKE_STATE.tmp"
      mv "$FAKE_STATE.tmp" "$FAKE_STATE"
    fi
    matched=$(awk -F'|' -v resource="$resource" -v namespace="$namespace" \
      -v name="$name" -v uid="$uid" \
      '$1 == resource && $2 == namespace && substr($3, index($3, "/") + 1) == name && $4 == uid { count++ }
       END { print count + 0 }' "$FAKE_STATE")
    [ "$matched" -eq 1 ] || exit 74
    case "$resource" in
      migrations.pgcopydb-operator.io) printf 'delete:migration\n' >> "$FAKE_LOG" ;;
      jobs) printf 'delete:job\n' >> "$FAKE_LOG" ;;
    esac
    awk -F'|' -v resource="$resource" -v namespace="$namespace" \
      -v name="$name" -v uid="$uid" \
      '!( $1 == resource && $2 == namespace &&
        substr($3, index($3, "/") + 1) == name && $4 == uid )' \
      "$FAKE_STATE" > "$FAKE_STATE.tmp"
    mv "$FAKE_STATE.tmp" "$FAKE_STATE"
    if [ "$mutation" = replace-after-delete ] && [ "$resource" = migrations.pgcopydb-operator.io ]; then
      printf '%s\n' \
        "migrations.pgcopydb-operator.io|$namespace|migration.pgcopydb-operator.io/$name|"\
"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa|someone-else|||false" >> "$FAKE_STATE"
    fi
    exit 0
  fi
  if [ "$from_file" = true ]; then
    printf 'delete:rendered\n' >> "$FAKE_LOG"
    awk -F'|' -v owner="$owner" '!( $5 == owner && $8 == "true" )' \
      "$FAKE_STATE" > "$FAKE_STATE.tmp"
    mv "$FAKE_STATE.tmp" "$FAKE_STATE"
    exit 0
  fi
  case "$resource" in
    migrations.pgcopydb-operator.io) printf 'delete:migration\n' >> "$FAKE_LOG" ;;
    jobs) printf 'delete:job\n' >> "$FAKE_LOG" ;;
    *) exit 78 ;;
  esac
  awk -F'|' -v resource="$resource" -v namespace="$namespace" -v owner="$owner" \
    -v name="$name" -v fake_owner="$FAKE_OWNER" \
    '!( $1 == resource && $2 == namespace && $5 == fake_owner &&
      (name == "" || substr($3, index($3, "/") + 1) == name) )' \
    "$FAKE_STATE" > "$FAKE_STATE.tmp"
  mv "$FAKE_STATE.tmp" "$FAKE_STATE"
  exit 0
fi
exit 77
`

func TestFeatureE2EKustomizeHelper(t *testing.T) {
	t.Helper()
	if os.Getenv("FEATURE_E2E_KUSTOMIZE_HELPER") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 || separator+2 != len(os.Args) {
		fmt.Fprintln(os.Stderr, "invalid render fixture arguments")
		os.Exit(2)
	}
	input, err := os.Open(os.Args[separator+1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "unable to read render fixture")
		os.Exit(2)
	}
	defer func() { _ = input.Close() }()

	decoder := k8syaml.NewYAMLOrJSONDecoder(input, 4096)
	objects := []map[string]any{}
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			break
		} else if err != nil || len(document) == 0 {
			fmt.Fprintln(os.Stderr, "invalid render fixture")
			os.Exit(2)
		}
		flattened, err := featureKustomizeFixtureObjects(document)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid render fixture")
			os.Exit(2)
		}
		objects = append(objects, flattened...)
	}
	if len(objects) == 0 {
		fmt.Fprintln(os.Stderr, "empty render fixture")
		os.Exit(2)
	}
	for index, object := range objects {
		featureKustomizeFixtureLabel(object, imageAttestationOwnerKey, imageAttestationOwnerValue)
		body, err := yaml.Marshal(object)
		if err != nil {
			fmt.Fprintln(os.Stderr, "unable to encode render fixture")
			os.Exit(2)
		}
		if index > 0 {
			fmt.Print("---\n")
		}
		fmt.Print(string(body))
	}
	os.Exit(0)
}

func featureKustomizeFixtureObjects(document map[string]any) ([]map[string]any, error) {
	kind, ok := document[kindKey].(string)
	if !ok || kind == "" {
		return nil, fmt.Errorf("invalid kind")
	}
	if kind != listKind {
		return []map[string]any{document}, nil
	}
	items, ok := document[itemsKey].([]any)
	if !ok || len(items) == 0 {
		return nil, fmt.Errorf("invalid List")
	}
	objects := []map[string]any{}
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid List member")
		}
		flattened, err := featureKustomizeFixtureObjects(object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, flattened...)
	}
	return objects, nil
}

func featureKustomizeFixtureLabel(object map[string]any, key, value string) {
	featureKustomizeFixtureMetadataLabel(object, key, value)
	apiVersion, _ := object[apiVersionKey].(string)
	kind, _ := object[kindKey].(string)
	paths := [][]string{}
	switch {
	case apiVersion == appsV1 && slices.Contains(
		[]string{deploymentKind, replicaSetKind, "DaemonSet", "StatefulSet"}, kind,
	):
		paths = append(paths, []string{specKey, helmTemplateCommand})
	case apiVersion == "batch/v1" && kind == "Job":
		paths = append(paths, []string{specKey, helmTemplateCommand})
	case apiVersion == "batch/v1" && kind == "CronJob":
		paths = append(paths,
			[]string{specKey, "jobTemplate"},
			[]string{specKey, "jobTemplate", specKey, helmTemplateCommand},
		)
	case apiVersion == "v1" && kind == "ReplicationController":
		paths = append(paths, []string{specKey, helmTemplateCommand})
	}
	for _, path := range paths {
		current := object
		for _, name := range path {
			next, ok := current[name].(map[string]any)
			if !ok {
				next = map[string]any{}
				current[name] = next
			}
			current = next
		}
		featureKustomizeFixtureMetadataLabel(current, key, value)
	}
}

func featureKustomizeFixtureMetadataLabel(object map[string]any, key, value string) {
	metadata, ok := object[metadataKey].(map[string]any)
	if !ok {
		metadata = map[string]any{}
		object[metadataKey] = metadata
	}
	labels, ok := metadata[labelsKey].(map[string]any)
	if !ok {
		labels = map[string]any{}
		metadata[labelsKey] = labels
	}
	labels[key] = value
}

func TestCompatibilitySnapshotIgnoresOnlyCRDDescriptions(t *testing.T) {
	t.Setenv("FEATURE_E2E_HELM", newCompatibilityHelmFixture(t))
	base, head := t.TempDir(), t.TempDir()
	crd := func(
		metadataDescription, topDescription, fieldDescription, fieldType,
		descriptionPropertyAnnotation, descriptionPropertyType string,
		includeDescriptionProperty bool,
	) string {
		descriptionProperty := ""
		if includeDescriptionProperty {
			descriptionProperty = fmt.Sprintf(`            description:
              description: %s
              type: %s
`, descriptionPropertyAnnotation, descriptionPropertyType)
		}
		return fmt.Sprintf(`apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: migrations.pgcopydb-operator.io
  annotations:
    description: %s
spec:
  versions:
    - name: v1beta1
      schema:
        openAPIV3Schema:
          description: %s
          properties:
%s
            spec:
              properties:
                follow:
                  description: %s
                  type: %s
`, metadataDescription, topDescription, descriptionProperty, fieldDescription, fieldType)
	}
	rbac := func(kind, name, marker string) string {
		return fmt.Sprintf(`apiVersion: rbac.authorization.k8s.io/v1
kind: %s
metadata:
  name: %s
  labels:
    guard.example/value: %s
`, kind, name, marker)
	}
	writeCompatibilityFixture(t, base, "config/crd/bases/migration.yaml",
		crd("keep", "base", "old", "object", "base property", schemaStringType, true))
	writeCompatibilityFixture(t, head, "config/crd/bases/migration.yaml",
		crd("keep", "head", "new", "object", "head property", schemaStringType, true))
	rbacKinds := []string{roleKind, roleBindingKind, clusterRoleKind, clusterRoleBindingKind}
	for _, kind := range rbacKinds {
		path := "config/rbac/" + strings.ToLower(kind) + ".yaml"
		writeCompatibilityFixture(t, base, path, rbac(kind, strings.ToLower(kind), "base"))
		writeCompatibilityFixture(t, head, path, rbac(kind, strings.ToLower(kind), "base"))
	}
	writeCompatibilityFixture(t, base, "images/pgcopydb-builder/Dockerfile", "FROM scratch\n")
	writeCompatibilityFixture(t, head, "images/pgcopydb-builder/Dockerfile", "FROM scratch\n")
	writeCompatibilityFixture(t, base, "images/pgcopydb-builder/README.md", "builder\n")
	writeCompatibilityFixture(t, head, "images/pgcopydb-builder/README.md", "builder\n")
	writeCompatibilityChart(t, base)
	writeCompatibilityChart(t, head)

	baseSnapshot := loadCompatibilitySnapshot(t, base)
	headSnapshot := loadCompatibilitySnapshot(t, head)
	if !maps.Equal(baseSnapshot.CRDs, headSnapshot.CRDs) {
		t.Fatal("description-only CRD change was rejected")
	}

	writeCompatibilityFixture(t, head, "config/crd/bases/migration.yaml",
		crd("changed", "head", "new", "object", "head property", schemaStringType, true))
	headSnapshot = loadCompatibilitySnapshot(t, head)
	if maps.Equal(baseSnapshot.CRDs, headSnapshot.CRDs) {
		t.Fatal("non-schema metadata description change was accepted")
	}

	writeCompatibilityFixture(t, head, "config/crd/bases/migration.yaml",
		crd("keep", "head", "new", schemaStringType, "head property", schemaStringType, true))
	headSnapshot = loadCompatibilitySnapshot(t, head)
	if maps.Equal(baseSnapshot.CRDs, headSnapshot.CRDs) {
		t.Fatal("structural CRD change was accepted")
	}

	writeCompatibilityFixture(t, head, "config/crd/bases/migration.yaml",
		crd("keep", "head", "new", "object", "head property", "integer", true))
	headSnapshot = loadCompatibilitySnapshot(t, head)
	if maps.Equal(baseSnapshot.CRDs, headSnapshot.CRDs) {
		t.Fatal("structural properties.description change was accepted")
	}

	writeCompatibilityFixture(t, head, "config/crd/bases/migration.yaml",
		crd("keep", "head", "new", "object", "", "", false))
	headSnapshot = loadCompatibilitySnapshot(t, head)
	if maps.Equal(baseSnapshot.CRDs, headSnapshot.CRDs) {
		t.Fatal("properties.description removal was accepted")
	}

	writeCompatibilityFixture(t, head, "config/crd/bases/migration.yaml",
		crd("keep", "head", "new", "object", "head property", schemaStringType, true))
	for _, kind := range rbacKinds {
		t.Run(kind, func(t *testing.T) {
			path := "config/rbac/" + strings.ToLower(kind) + ".yaml"
			writeCompatibilityFixture(t, head, path, rbac(kind, strings.ToLower(kind), "head"))
			headSnapshot = loadCompatibilitySnapshot(t, head)
			if maps.Equal(baseSnapshot.RBAC, headSnapshot.RBAC) {
				t.Fatalf("structural %s change was accepted", kind)
			}
			writeCompatibilityFixture(t, head, path, rbac(kind, strings.ToLower(kind), "base"))
		})
	}

	writeCompatibilityFixture(t, head, "images/pgcopydb-builder/Dockerfile", "FROM busybox\n")
	headSnapshot = loadCompatibilitySnapshot(t, head)
	if maps.Equal(baseSnapshot.Builder, headSnapshot.Builder) {
		t.Fatal("pgcopydb-builder change was accepted")
	}

	writeCompatibilityFixture(t, head, "images/pgcopydb-builder/Dockerfile", "FROM scratch\n")
	writeCompatibilityFixture(t, head, "images/pgcopydb-builder/nested/extra", "extra\n")
	headSnapshot = loadCompatibilitySnapshot(t, head)
	if maps.Equal(baseSnapshot.Builder, headSnapshot.Builder) {
		t.Fatal("nested pgcopydb-builder addition was accepted")
	}
	if err := os.RemoveAll(filepath.Join(head, "images/pgcopydb-builder/nested")); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(head, "images/pgcopydb-builder/Dockerfile")); err != nil {
		t.Fatal(err)
	}
	headSnapshot = loadCompatibilitySnapshot(t, head)
	if maps.Equal(baseSnapshot.Builder, headSnapshot.Builder) {
		t.Fatal("pgcopydb-builder removal was accepted")
	}

	writeCompatibilityFixture(t, head, "images/pgcopydb-builder/Dockerfile", "FROM scratch\n")
	if err := os.Chmod(filepath.Join(head, "images/pgcopydb-builder/Dockerfile"), 0o755); err != nil {
		t.Fatal(err)
	}
	headSnapshot = loadCompatibilitySnapshot(t, head)
	if maps.Equal(baseSnapshot.Builder, headSnapshot.Builder) {
		t.Fatal("pgcopydb-builder executable-bit change was accepted")
	}
}

func TestFeatureE2ECandidateCompatibilityRejectsCompleteRenderedPrivilegeDrift(t *testing.T) {
	t.Setenv("FEATURE_E2E_HELM", newCompatibilityHelmFixture(t))
	t.Run("comparison profile includes existing gated privileges", func(t *testing.T) {
		base, head := newCompatibilityChartRoots(t)
		baseSnapshot := loadCompatibilitySnapshot(t, base)
		headSnapshot := loadCompatibilitySnapshot(t, head)
		if !slices.Equal(baseSnapshot.RenderedPrivileges, headSnapshot.RenderedPrivileges) {
			t.Fatal("identical fixed privilege renders differ")
		}
		for _, kind := range []string{
			customResourceDefinitionKind, roleKind, roleBindingKind, clusterRoleKind, clusterRoleBindingKind,
		} {
			found := false
			for _, document := range baseSnapshot.RenderedPrivileges {
				if strings.Contains(document, "/"+kind+"/") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("comparison profile omitted existing gated %s", kind)
			}
		}
	})

	tests := []struct {
		name string
		path string
		kind string
	}{
		{"added chart CRD", "crds/extra-crd.yaml", customResourceDefinitionKind},
		{"added chart Role", "templates/extra-role.yaml", roleKind},
		{"added independent RoleBinding template", "templates/manual-rolebinding.yaml", roleBindingKind},
		{"added chart ClusterRole", "templates/extra-clusterrole.yaml", clusterRoleKind},
		{"added independent ClusterRoleBinding template", "templates/manual-clusterrolebinding.yaml",
			clusterRoleBindingKind},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, head := newCompatibilityChartRoots(t)
			writeCompatibilityFixture(t, head, "charts/pgcopydb-operator/"+tt.path,
				compatibilityPrivilegeManifest(tt.kind, "extra"))
			requireFeatureCompatibilityRejected(t, base, head)
		})
	}

	t.Run("removed rendered target resource", func(t *testing.T) {
		base, head := newCompatibilityChartRoots(t)
		writeCompatibilityFixture(t, base,
			"charts/pgcopydb-operator/templates/manual-rolebinding.yaml",
			compatibilityPrivilegeManifest(roleBindingKind, "removed"))
		requireFeatureCompatibilityRejected(t, base, head)
	})

	t.Run("matching incomplete rendered inventory", func(t *testing.T) {
		base, head := newCompatibilityChartRoots(t)
		for _, root := range []string{base, head} {
			if err := os.Remove(filepath.Join(root, "charts", "pgcopydb-operator",
				"templates", "rolebinding.yaml")); err != nil {
				t.Fatal(err)
			}
		}
		requireFeatureCompatibilityRejected(t, base, head)
	})

	t.Run("duplicate rendered target identity", func(t *testing.T) {
		base, head := newCompatibilityChartRoots(t)
		manifest := compatibilityPrivilegeManifest(roleKind, "duplicate")
		writeCompatibilityFixture(t, base,
			"charts/pgcopydb-operator/templates/role.yaml", manifest)
		writeCompatibilityFixture(t, head,
			"charts/pgcopydb-operator/templates/role.yaml", manifest)
		writeCompatibilityFixture(t, head,
			"charts/pgcopydb-operator/templates/duplicate-role.yaml",
			strings.Replace(manifest, "metadata:\n", "metadata:\n  namespace: pgcopydb-e2e\n", 1))
		requireFeatureCompatibilityRejected(t, base, head)
	})

	t.Run("typed target List is rejected", func(t *testing.T) {
		base, head := newCompatibilityChartRoots(t)
		writeCompatibilityFixture(t, head,
			"charts/pgcopydb-operator/templates/role-list.yaml", `apiVersion: rbac.authorization.k8s.io/v1
kind: RoleList
items: []
`)
		requireFeatureCompatibilityRejected(t, base, head)
	})

	t.Run("arbitrary typed List is rejected", func(t *testing.T) {
		base, head := newCompatibilityChartRoots(t)
		writeCompatibilityFixture(t, head,
			"charts/pgcopydb-operator/templates/other-list.yaml", `apiVersion: fixture.example/v1
kind: FooList
items: []
`)
		requireFeatureCompatibilityRejected(t, base, head)
	})

	t.Run("non-List items envelope is rejected", func(t *testing.T) {
		base, head := newCompatibilityChartRoots(t)
		writeCompatibilityFixture(t, head,
			"charts/pgcopydb-operator/templates/items-envelope.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: envelope
items:
  - apiVersion: rbac.authorization.k8s.io/v1
    kind: Role
    metadata:
      name: enveloped
`)
		requireFeatureCompatibilityRejected(t, base, head)
	})

	t.Run("exact v1 List duplicate identity is rejected", func(t *testing.T) {
		base, head := newCompatibilityChartRoots(t)
		writeCompatibilityFixture(t, head,
			"charts/pgcopydb-operator/templates/role-list.yaml", `apiVersion: v1
kind: List
items:
  - apiVersion: rbac.authorization.k8s.io/v1
    kind: Role
    metadata:
      name: existing-role
      namespace: pgcopydb-e2e
`)
		requireFeatureCompatibilityRejected(t, base, head)
	})
}

func newCompatibilityChartRoots(t *testing.T) (string, string) {
	t.Helper()
	base, head := t.TempDir(), t.TempDir()
	for _, root := range []string{base, head} {
		writeCompatibilityFixture(t, root, "config/crd/bases/migration.yaml", `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: migrations.pgcopydb-operator.io
spec:
  versions: []
`)
		for _, kind := range []string{roleKind, roleBindingKind, clusterRoleKind, clusterRoleBindingKind} {
			writeCompatibilityFixture(t, root, "config/rbac/"+strings.ToLower(kind)+".yaml",
				compatibilityPrivilegeManifest(kind, strings.ToLower(kind)))
		}
		writeCompatibilityFixture(t, root, "images/pgcopydb-builder/Dockerfile", "FROM scratch\n")
		writeCompatibilityChart(t, root)
	}
	return base, head
}

func writeCompatibilityChart(t *testing.T, root string) {
	t.Helper()
	writeCompatibilityFixture(t, root, "charts/pgcopydb-operator/Chart.yaml", `apiVersion: v2
name: pgcopydb-operator
version: 0.1.0
`)
	writeCompatibilityFixture(t, root, "charts/pgcopydb-operator/values.yaml", "{}\n")
	writeCompatibilityFixture(t, root, "charts/pgcopydb-operator/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture
`)
	writeCompatibilityFixture(t, root, "charts/pgcopydb-operator/templates/crd.yaml",
		"{{- if .Values.crds.install }}\n"+
			compatibilityPrivilegeManifest(customResourceDefinitionKind, "existing")+
			"{{- end }}\n")
	for path, kind := range map[string]string{
		"clusterrole.yaml":        clusterRoleKind,
		"clusterrolebinding.yaml": clusterRoleBindingKind,
		"role.yaml":               roleKind,
		"rolebinding.yaml":        roleBindingKind,
	} {
		condition := ".Values.rbac.create"
		if kind == roleKind || kind == roleBindingKind {
			condition = "and .Values.rbac.create .Values.leaderElection.enabled"
		}
		writeCompatibilityFixture(t, root, "charts/pgcopydb-operator/templates/"+path,
			"{{- if "+condition+" }}\n"+
				compatibilityPrivilegeManifest(kind, "existing-"+strings.ToLower(kind))+
				"{{- end }}\n")
	}
}

func compatibilityPrivilegeManifest(kind, name string) string {
	apiVersion := "rbac.authorization.k8s.io/v1"
	if kind == customResourceDefinitionKind {
		apiVersion = "apiextensions.k8s.io/v1"
	}
	return fmt.Sprintf("apiVersion: %s\nkind: %s\nmetadata:\n  name: %s\n", apiVersion, kind, name)
}

func requireFeatureCompatibilityRejected(t *testing.T, base, head string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFeatureE2ECandidateCompatibility$", "-test.count=1")
	cmd.Env = compatibilityFixtureEnv(base, head)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("candidate compatibility unexpectedly succeeded:\n%s", output)
	}
	if !bytes.Contains(output, []byte("candidate changes rendered")) &&
		!bytes.Contains(output, []byte("duplicate rendered privilege identity")) &&
		!bytes.Contains(output, []byte("rendered privilege input is missing")) &&
		!bytes.Contains(output, []byte("unsupported rendered privilege List")) {
		t.Fatalf("candidate compatibility failed outside the rendered privilege guard: %v\n%s", err, output)
	}
}

func compatibilityFixtureEnv(base, head string) []string {
	env := make([]string, 0, len(os.Environ())+2)
	for _, value := range os.Environ() {
		if strings.HasPrefix(value, "FEATURE_E2E_BASE=") ||
			strings.HasPrefix(value, "FEATURE_E2E_HEAD=") {
			continue
		}
		env = append(env, value)
	}
	return append(env, "FEATURE_E2E_BASE="+base, "FEATURE_E2E_HEAD="+head)
}

func newCompatibilityHelmFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "helm")
	body := `#!/bin/sh
set -eu
[ "$#" -ge 3 ] && [ "$1" = template ] && [ "$2" = pgcopydb-e2e ] || exit 70
chart=$3
shift 3
manager_digest=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
runner_digest=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
expected='--namespace pgcopydb-e2e --include-crds --skip-tests --set crds.install=true'
expected="$expected --set image.tag=feature-fixture@sha256:$manager_digest"
expected="$expected --set runner.image.tag=feature-fixture@sha256:$runner_digest"
expected="$expected --set watchNamespaces={pgcopydb-e2e,pgcopydb-e2e-x}"
expected="$expected --set leaderElection.enabled=true --set metrics.enabled=true"
expected="$expected --set metrics.serviceMonitor.enabled=true --set rbac.create=true"
expected="$expected --set serviceAccount.create=true"
expected="$expected --set serviceAccount.name=pgcopydb-e2e-manager"
expected="$expected --set-string fullnameOverride=pgcopydb-e2e"
[ "$*" = "$expected" ] || exit 71
for directory in "$chart/crds" "$chart/templates"; do
  [ -d "$directory" ] || continue
  find "$directory" -type f \( -name '*.yaml' -o -name '*.yml' \) -print
done | LC_ALL=C sort | while IFS= read -r file; do
  printf '%s\n' '---'
  sed '/^{{/d' "$file"
done
`
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
