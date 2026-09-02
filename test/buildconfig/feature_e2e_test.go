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

	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

const (
	featureWorkflow        = "../../.github/workflows/feature-e2e.yml"
	publishedE2EWorkflow   = "../../.github/workflows/e2e.yml"
	protectedClusterGroup  = "e2e-cluster"
	resolveJob             = "resolve"
	pendingStatusJob       = "pending-status"
	managerImageJob        = "manager-image"
	runnerImageJob         = "runner-image"
	permissionContents     = "contents"
	permissionPackages     = "packages"
	permissionRead         = "read"
	permissionWrite        = "write"
	falseValue             = "false"
	trueValue              = "true"
	successValue           = "success"
	failureValue           = "failure"
	errorValue             = "error"
	roleKind               = "Role"
	roleBindingKind        = "RoleBinding"
	clusterRoleKind        = "ClusterRole"
	clusterRoleBindingKind = "ClusterRoleBinding"
	schemaDescriptionKey   = "description"
	schemaTypeKey          = "type"
	schemaKeepValue        = "keep"
	schemaRemoveValue      = "remove"
	schemaStringType       = "string"
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
	CRDs    map[string]string
	RBAC    map[string]string
	Builder map[string]string
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
	spec, ok := document["spec"].(map[string]any)
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
		"additionalItems", "additionalProperties", "items", "not",
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
		"metadata": map[string]any{
			"annotations": map[string]any{schemaDescriptionKey: schemaKeepValue},
		},
		"spec": map[string]any{
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
								"items": map[string]any{
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
	metadata := document["metadata"].(map[string]any)
	annotations := metadata["annotations"].(map[string]any)
	if got := annotations[schemaDescriptionKey]; got != schemaKeepValue {
		t.Errorf("non-schema description = %v, want keep", got)
	}
	root := document["spec"].(map[string]any)["versions"].([]any)[0].(map[string]any)
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
	items := properties["list"].(map[string]any)["items"].(map[string]any)
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
			kind, _ := document["kind"].(string)
			if !accept(kind) {
				continue
			}
			apiVersion, _ := document["apiVersion"].(string)
			metadata, _ := document["metadata"].(map[string]any)
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

func loadCompatibilitySnapshot(t *testing.T, root string) compatibilitySnapshot {
	t.Helper()
	snapshot := compatibilitySnapshot{
		CRDs: compatibilityDocuments(t, root, "config/crd/bases",
			func(kind string) bool { return kind == "CustomResourceDefinition" }, true),
		RBAC: compatibilityDocuments(t, root, "config/rbac",
			func(kind string) bool {
				return kind == roleKind || kind == roleBindingKind ||
					kind == clusterRoleKind || kind == clusterRoleBindingKind
			}, false),
		Builder: compatibilityTree(t, root, "images/pgcopydb-builder"),
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
	if got := slices.Sorted(maps.Keys(inputs)); !slices.Equal(got, []string{"focus", "mode", "pr"}) {
		t.Fatalf("feature inputs = %v, want focus, mode, pr", got)
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
	run := protectedStepNamed(t, resolve, "Resolve pull request").Run
	for _, want := range []string{
		`[[ "$INPUT_PR" =~ ^[1-9][0-9]*$ ]]`, `case "$INPUT_MODE" in`,
		`tr -d '\000-\037\177'`, `.state == "open"`, `.base.ref == "main"`,
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
		"make -C trusted controller-gen", "../trusted/bin/controller-gen",
		"git status --porcelain -- config/crd/bases config/rbac",
		"../trusted/hack/sync-chart-crd.sh --check", "../trusted/hack/sync-chart-rbac.sh --check",
		"FEATURE_E2E_BASE", "FEATURE_E2E_HEAD", "TestFeatureE2ECandidateCompatibility",
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
	if run.Env["E2E_SCALE"] != "0.1" || run.Env["E2E_MANAGE_NAMESPACES"] != falseValue ||
		run.Env["E2E_KEEP_FIXTURES"] != trueValue {
		t.Errorf("feature suite environment = %v", run.Env)
	}
	for _, want := range []string{
		"-ginkgo.label-filter=!chaos",
		"-ginkgo.fail-on-empty",
		`test_args+=("-ginkgo.focus=$E2E_FOCUS")`,
	} {
		if !strings.Contains(run.Run, want) {
			t.Errorf("suite command is missing %q", want)
		}
	}
	helpers := protectedStepNamed(t, cluster, "Write cluster helpers").Run
	for _, want := range []string{
		"imageID", "imagetools inspect", "--runner-image=", "FEATURE_E2E_OWNER_KEY",
		"FEATURE_E2E_OWNER_FILE", "require_expected", `-l "$owner_selector"`, "require_empty",
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
	migrations := strings.Index(helpers, "kubectl delete migrations.pgcopydb-operator.io")
	jobs := strings.Index(helpers, "kubectl delete jobs")
	controller := strings.Index(helpers, `"$REAL_HELM" uninstall "$release"`)
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
	final := protectedStepNamed(t, wf.Jobs["final-status"], "Publish final status")
	tests := []struct {
		name, cluster, suite, cleanup, manager, runner, want string
	}{
		{"safe success", successValue, successValue, successValue, trueValue, trueValue, successValue},
		{"safe assertion failure", failureValue, failureValue, successValue, trueValue, trueValue, failureValue},
		{"manager attestation failure", failureValue, failureValue, successValue, falseValue, trueValue, errorValue},
		{"runner attestation failure", failureValue, failureValue, successValue, trueValue, falseValue, errorValue},
		{"cleanup failure", failureValue, successValue, failureValue, trueValue, trueValue, errorValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runFinalStatusFixture(t, final.Run, tt.cluster, tt.suite, tt.cleanup, tt.manager, tt.runner)
			if got != tt.want {
				t.Errorf("emitted state = %q, want %q", got, tt.want)
			}
		})
	}
	for _, name := range []string{"MANAGER_ATTESTED", "RUNNER_ATTESTED"} {
		if final.Env[name] == "" {
			t.Errorf("final status does not receive %s", name)
		}
	}
}

func runFinalStatusFixture(
	t *testing.T,
	script, cluster, suite, cleanup, manager, runner string,
) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, "state")
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

	const owner = "0123456789abcdef0123456789abcdef"
	baseState := strings.Join([]string{
		"migrations.pgcopydb-operator.io|pgcopydb-e2e|migration.pgcopydb-operator.io/run-owned|" + owner,
		"jobs|pgcopydb-e2e|job.batch/run-worker|" + owner,
		"jobs|pgcopydb-e2e|job.batch/run-cleanup|" + owner,
		"jobs|pgcopydb-e2e|job.batch/feature-e2e-runner-attest|" + owner,
		"deployments|pgcopydb-e2e-system|deployment.apps/pgcopydb-e2e|" + owner,
		"pods|pgcopydb-e2e-system|pod/pgcopydb-e2e-manager|" + owner,
		"migrations.pgcopydb-operator.io|pgcopydb-e2e|migration.pgcopydb-operator.io/customer|",
		"jobs|pgcopydb-e2e|job.batch/unrelated|someone-else",
	}, "\n") + "\n"

	t.Run("owned resources only", func(t *testing.T) {
		result := runCleanupFixture(t, cleanup, owner, baseState, "")
		if result.err != nil {
			t.Fatalf("cleanup failed: %v\n%s", result.err, result.output)
		}
		if strings.Contains(result.output, owner) {
			t.Fatal("cleanup printed the ownership value")
		}
		if got, want := strings.Fields(result.log), []string{"migration", "job", "controller"}; !slices.Equal(got, want) {
			t.Fatalf("deletion order = %v, want %v", got, want)
		}
		if strings.Contains(result.state, "|"+owner+"\n") {
			t.Fatalf("run-owned resources remain:\n%s", result.state)
		}
		for _, survivor := range []string{"migration.pgcopydb-operator.io/customer", "job.batch/unrelated"} {
			if !strings.Contains(result.state, survivor) {
				t.Errorf("cleanup deleted %s", survivor)
			}
		}
	})

	t.Run("missing captured resource", func(t *testing.T) {
		result := runCleanupFixture(t, cleanup, owner, baseState, "migrations.pgcopydb-operator.io")
		if result.err == nil || !strings.Contains(result.output, "expected run-owned Migrations is absent") {
			t.Fatalf("missing captured Migration did not fail closed: %v\n%s", result.err, result.output)
		}
	})

	t.Run("fixed feature name without ownership", func(t *testing.T) {
		state := "deployments|pgcopydb-e2e-system|deployment.apps/pgcopydb-e2e|\n" +
			"pods|pgcopydb-e2e-system|pod/pgcopydb-e2e-manager|\n"
		result := runCleanupFixture(t, cleanup, owner, state, "")
		if result.err == nil {
			t.Fatal("unowned fixed-name controller was accepted")
		}
		if !strings.Contains(result.state, "deployment.apps/pgcopydb-e2e") {
			t.Fatal("unowned fixed-name controller was deleted")
		}
	})

	for _, badOwner := range []string{"", "not-a-valid-owner"} {
		name := "empty owner"
		if badOwner != "" {
			name = "malformed owner"
		}
		t.Run(name, func(t *testing.T) {
			result := runCleanupFixture(t, cleanup, badOwner, baseState, "")
			if result.err == nil || !strings.Contains(result.output, "feature ownership value is invalid") {
				t.Fatalf("owner %q did not fail closed: %v\n%s", badOwner, result.err, result.output)
			}
			if result.state != baseState {
				t.Fatal("invalid ownership changed fake API state")
			}
		})
	}
}

type cleanupFixtureResult struct {
	output string
	state  string
	log    string
	err    error
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

func runCleanupFixture(
	t *testing.T,
	cleanup, owner, state, dropAfterCapture string,
) cleanupFixtureResult {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	cleanupPath := filepath.Join(dir, "cleanup")
	ownerPath := filepath.Join(dir, "owner")
	statePath := filepath.Join(dir, "state")
	logPath := filepath.Join(dir, "log")
	for path, body := range map[string]string{
		cleanupPath: cleanup,
		ownerPath:   owner,
		statePath:   state,
		logPath:     "",
	} {
		mode := os.FileMode(0o600)
		if path == cleanupPath {
			mode = 0o700
		}
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}

	kubectl := `#!/usr/bin/env bash
set -euo pipefail
command=$1
shift
resource=$1
shift
namespace=
selector=
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n) namespace=$2; shift 2 ;;
    -l) selector=$2; shift 2 ;;
    -o) output=$2; shift 2 ;;
    *) shift ;;
  esac
done
owner=${selector#*=}
if [ "$command" = get ] && [[ "$resource" == */* ]]; then
  awk -F'|' -v name="$resource" -v ns="$namespace" \
    '$2 == ns && $3 == name { found=1 } END { exit !found }' "$FAKE_STATE"
  printf 'pgcopydb-e2e'
  exit 0
fi
if [ "$command" = get ]; then
  awk -F'|' -v resource="$resource" -v ns="$namespace" -v owner="$owner" \
    '$1 == resource && $2 == ns && $4 == owner { print $3 }' "$FAKE_STATE"
  if [ "${FAKE_DROP_AFTER_CAPTURE:-}" = "$resource" ] && [ ! -e "$FAKE_STATE.drop" ]; then
    tmp=$FAKE_STATE.tmp
    awk -F'|' -v resource="$resource" -v ns="$namespace" -v owner="$owner" \
      '!( $1 == resource && $2 == ns && $4 == owner )' "$FAKE_STATE" > "$tmp"
    mv "$tmp" "$FAKE_STATE"
    : > "$FAKE_STATE.drop"
  fi
  exit 0
fi
if [ "$command" = delete ]; then
  case "$resource" in
    migrations.pgcopydb-operator.io) printf 'migration\n' >> "$FAKE_LOG" ;;
    jobs) printf 'job\n' >> "$FAKE_LOG" ;;
    *) exit 1 ;;
  esac
  tmp=$FAKE_STATE.tmp
  awk -F'|' -v resource="$resource" -v ns="$namespace" -v owner="$owner" \
    '!( $1 == resource && $2 == ns && $4 == owner )' "$FAKE_STATE" > "$tmp"
  mv "$tmp" "$FAKE_STATE"
  exit 0
fi
exit 1
`
	helm := `#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  uninstall)
    printf 'controller\n' >> "$FAKE_LOG"
    tmp=$FAKE_STATE.tmp
    awk -F'|' -v owner="$FAKE_OWNER" \
      '!( ($1 == "deployments" || $1 == "pods") && $2 == "pgcopydb-e2e-system" && $4 == owner )' \
      "$FAKE_STATE" > "$tmp"
    mv "$tmp" "$FAKE_STATE"
    ;;
  list) ;;
  *) exit 1 ;;
esac
`
	for name, body := range map[string]string{"kubectl": kubectl, "helm": helm} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	cmd := exec.Command("bash", cleanupPath)
	cmd.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"FEATURE_E2E_OWNER_KEY=pgcopydb-operator.io/feature-e2e-run",
		"FEATURE_E2E_OWNER_FILE="+ownerPath,
		"FEATURE_E2E_HELPERS="+dir,
		"REAL_HELM="+filepath.Join(bin, "helm"),
		"FAKE_STATE="+statePath,
		"FAKE_LOG="+logPath,
		"FAKE_OWNER="+owner,
		"FAKE_DROP_AFTER_CAPTURE="+dropAfterCapture,
	)
	output, err := cmd.CombinedOutput()
	finalState, readErr := os.ReadFile(statePath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	log, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	return cleanupFixtureResult{output: string(output), state: string(finalState), log: string(log), err: err}
}

func TestCompatibilitySnapshotIgnoresOnlyCRDDescriptions(t *testing.T) {
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
