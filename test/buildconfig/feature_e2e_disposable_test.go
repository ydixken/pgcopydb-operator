package buildconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	gingkotypes "github.com/onsi/ginkgo/v2/types"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	featureNoneConversion   = "None"
	featureInvalidInput     = "invalid"
	featureOccupiedCluster  = "occupied"
	featureNodeIdentity     = "node-identity"
	featureOwnedMerged      = "owned merged"
	featureCRDSchemaKey     = "schema"
	featureCRDConversionKey = "conversion"
	featureDockerCommand    = "docker"
	featureReadyStage       = "ready"
	featureStrategyKey      = "strategy"
	featureStorageKey       = "storage"
	featureMatchLabelsKey   = "matchLabels"
	featureInstanceLabel    = "app.kubernetes.io/instance"
	featureDisposableJob    = "disposable"
	featureBaselineProfile  = "baseline"
	featureRuntimeProfile   = "runtime-safety"
	featureMetricsText      = "Migration metrics"
	featureExpiryEntry      = "dead-worker-session-expiry"
	featureCleanupEntry     = "cleanup-alert-after-job-ttl"
	featureExpiryLeaf       = "expires abandoned source and target sessions after packet loss and resumes"
	featureCleanupLeaf      = "records exhausted cleanup after proven drain and restores retained replication state"
	featurePreflightJob     = "preflight"
	featureHelpersStep      = "Write cluster helpers"
	featureSuiteStep        = "Run non-chaos suite"
	featureFocusText        = "one scenario"
	featureMissingValue     = "missing"
	featureCleanupOutput    = "cleanup"
	featureManagerOutput    = "manager_attested"
	featureRunnerOutput     = "runner_attested"
	featureSuiteRanOutput   = "suite_ran"
	featureCompleteOutput   = "suite_completed"
	featureCancelledValue   = "cancelled"
	featureUnknownProfile   = "unknown profile"
	featureChoiceType       = "choice"
	featureResolvedSHA      = "${{ needs.resolve.outputs.sha }}"
	featurePassingShell     = "#!/usr/bin/env bash\nexit 0\n"
	featureSharedTarget     = "shared"
	featureSuiteOutput      = "suite"
	featureRuleEnabled      = "metrics.prometheusRule.enabled=true"
	featureOtherValue       = "other"
	featureOwnerFile        = "owner"
	featureSchemaFile       = "schema-profile"
	featureValuesFlag       = "--values"
)

func TestFeatureE2EProfileInputs(t *testing.T) {
	inputs := parseProtectedWorkflow(t, featureWorkflow).On["workflow_dispatch"].Inputs
	for name, options := range map[string][]string{
		"schema_validation": {identicalSchemaProfile, disposableSchemaProfile},
		"suite_profile":     {featureBaselineProfile, featureRuntimeProfile},
	} {
		if in := inputs[name]; !in.Required || in.Type != featureChoiceType ||
			in.Default != options[0] || !slices.Equal(in.Options, options) {
			t.Errorf("%s input = %+v", name, in)
		}
	}
}

func TestFeatureE2EProfileResolver(t *testing.T) {
	resolver := protectedStepNamed(t, parseProtectedWorkflow(t, featureWorkflow).Jobs[resolveJob], "Resolve pull request")
	const sha = "0123456789abcdef0123456789abcdef01234567"
	for _, tt := range []struct {
		name, schema, suite, mode, scale, focus string
		wantError                               bool
	}{
		{name: "defaults", mode: fullModeValue, scale: featureDefaultScale},
		{
			name:   featureSharedTarget,
			schema: identicalSchemaProfile,
			suite:  featureBaselineProfile,
			mode:   fullModeValue,
			scale:  featureFullScale,
		},
		{
			name:   "isolated baseline",
			schema: disposableSchemaProfile,
			suite:  featureBaselineProfile,
			mode:   fullModeValue,
			scale:  featureDefaultScale,
		},
		{
			name:   "isolated focus",
			schema: disposableSchemaProfile,
			suite:  featureBaselineProfile,
			mode:   focusModeValue,
			scale:  featureDefaultScale,
			focus:  featureFocusText,
		},
		{
			name:   "runtime",
			schema: disposableSchemaProfile,
			suite:  featureRuntimeProfile,
			mode:   fullModeValue,
			scale:  featureDefaultScale,
		},
		{
			name:      "unknown schema",
			schema:    featureInvalidInput,
			suite:     featureBaselineProfile,
			mode:      fullModeValue,
			scale:     featureDefaultScale,
			wantError: true,
		},
		{
			name:      "unknown suite",
			schema:    identicalSchemaProfile,
			suite:     featureInvalidInput,
			mode:      fullModeValue,
			scale:     featureDefaultScale,
			wantError: true,
		},
		{
			name:      "isolated large",
			schema:    disposableSchemaProfile,
			suite:     featureBaselineProfile,
			mode:      fullModeValue,
			scale:     featureFullScale,
			wantError: true,
		},
		{
			name:      "shared runtime",
			schema:    identicalSchemaProfile,
			suite:     featureRuntimeProfile,
			mode:      fullModeValue,
			scale:     featureDefaultScale,
			wantError: true,
		},
		{
			name:      "focused runtime",
			schema:    disposableSchemaProfile,
			suite:     featureRuntimeProfile,
			mode:      focusModeValue,
			scale:     featureDefaultScale,
			focus:     featureFocusText,
			wantError: true,
		},
		{
			name:      "runtime focus input",
			schema:    disposableSchemaProfile,
			suite:     featureRuntimeProfile,
			mode:      fullModeValue,
			scale:     featureDefaultScale,
			focus:     featureFocusText,
			wantError: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			gh := `#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == "api repos/ydixken/pgcopydb-operator/pulls/1" ]]
printf 'lookup\n' >> "$GH_CALLED"
printf '%s\n' '{"state":"open","base":{"ref":"main","repo":{"full_name":"ydixken/pgcopydb-operator"}},
"head":{"repo":{"full_name":"ydixken/pgcopydb-operator"},"sha":"` + sha + `"}}'
`
			if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(gh), 0o700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", "-c", resolver.Run)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "GH_CALLED="+filepath.Join(dir, "calls"),
				"GITHUB_OUTPUT="+filepath.Join(dir, "output"), "GITHUB_REPOSITORY=ydixken/pgcopydb-operator", "INPUT_PR=1",
				"INPUT_MODE="+tt.mode, "INPUT_SCALE="+tt.scale, "INPUT_FOCUS="+tt.focus,
				"INPUT_SCHEMA_VALIDATION="+tt.schema, "INPUT_SUITE_PROFILE="+tt.suite)
			output, err := cmd.CombinedOutput()
			if (err != nil) != tt.wantError {
				t.Fatalf("resolver error = %v, want error %v: %s", err, tt.wantError, output)
			}
			calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
			if tt.wantError {
				if len(calls) != 0 {
					t.Fatal("invalid profile contacted GitHub")
				}
				return
			}
			if string(calls) != "lookup\n" {
				t.Fatalf("candidate lookup was not exactly once: %q", calls)
			}
			schema, suite := tt.schema, tt.suite
			if schema == "" {
				schema = identicalSchemaProfile
			}
			if suite == "" {
				suite = featureBaselineProfile
			}
			resolved := read(t, filepath.Join(dir, "output"))
			for _, want := range []string{
				"sha=" + sha + "\n",
				"schema_validation=" + schema + "\n",
				"suite_profile=" + suite + "\n",
			} {
				if !strings.Contains(resolved, want) {
					t.Errorf("resolved outputs lack %q", want)
				}
			}
		})
	}
}

func TestFeatureE2EPreflightRoute(t *testing.T) {
	job := parseProtectedWorkflow(t, featureWorkflow).Jobs[featurePreflightJob]
	check := protectedStepNamed(t, job, "Check candidate compatibility")
	for _, tt := range []struct {
		profile, target, failure string
	}{
		{profile: identicalSchemaProfile, target: featureSharedTarget},
		{profile: disposableSchemaProfile, target: featureDisposableJob},
		{profile: disposableSchemaProfile, failure: "make"},
		{profile: disposableSchemaProfile, failure: "go"},
		{profile: disposableSchemaProfile, failure: featureDockerCommand},
	} {
		t.Run(tt.profile+tt.failure, func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			crd := "candidate CRD bytes\n"
			for _, root := range []string{"trusted", "candidate"} {
				writeCompatibilityFixture(t, dir, root+"/images/runner/Dockerfile",
					"FROM ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:test@sha256:"+strings.Repeat("1", 64)+" AS pgcopydb\n")
			}
			writeCompatibilityFixture(t, dir, "candidate/config/crd/bases/pgcopydb-operator.io_migrations.yaml", crd)
			for _, name := range []string{
				"make",
				"go",
				featureDockerCommand,
				"git",
				"helm",
				"../trusted/hack/sync-chart-crd.sh",
				"../trusted/hack/sync-chart-rbac.sh",
			} {
				path := filepath.Join(dir, "bin", name)
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				body := `#!/usr/bin/env bash
set -euo pipefail
name=${0##*/}
[[ "$name" != "$FAIL_COMMAND" ]]
if [[ "$name" == go ]]; then
  [[ "$FEATURE_E2E_SCHEMA_VALIDATION" == "$EXPECTED_PROFILE" ]]
  [[ "$PWD" == "$GITHUB_WORKSPACE/trusted" ]]
  [[ "$*" == "test ./test/buildconfig -run ^TestFeatureE2ECandidateCompatibility$ -count=1" ]]
fi
`
				if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("bash", "-c", check.Run)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "PATH="+filepath.Join(dir, "bin")+":"+os.Getenv("PATH"),
				"GITHUB_WORKSPACE="+dir, "GITHUB_OUTPUT="+filepath.Join(dir, "output"),
				"FEATURE_E2E_SCHEMA_VALIDATION="+tt.profile, "EXPECTED_PROFILE="+tt.profile, "FAIL_COMMAND="+tt.failure)
			output, err := cmd.CombinedOutput()
			if tt.failure != "" {
				if err == nil {
					t.Fatal("preflight failure was accepted")
				}
				if data, _ := os.ReadFile(filepath.Join(dir, "output")); len(data) != 0 {
					t.Fatal("failed preflight emitted route evidence")
				}
				return
			}
			if err != nil {
				t.Fatalf("preflight: %v: %s", err, output)
			}
			want := fmt.Sprintf("validation_target=%s\ncandidate_crd_sha256=%x\n", tt.target, sha256.Sum256([]byte(crd)))
			if got := read(t, filepath.Join(dir, "output")); got != want {
				t.Errorf("preflight evidence = %q, want %q", got, want)
			}
		})
	}
}

func TestFeatureE2EDisposableRouting(t *testing.T) {
	wf := parseProtectedWorkflow(t, featureWorkflow)
	for jobName, target := range map[string]string{
		featureClusterJob:    featureSharedTarget,
		featureDisposableJob: featureDisposableJob,
	} {
		job, ok := wf.Jobs[jobName]
		if !ok {
			t.Fatalf("missing selected route %s", jobName)
		}
		if job.If != "needs.preflight.outputs.validation_target == '"+target+"'" ||
			!slices.Equal(protectedNeeds(t, job), []string{resolveJob, featurePreflightJob, managerImageJob, runnerImageJob}) {
			t.Errorf("%s can run without its successful selected preflight", jobName)
		}
		for _, stepName := range []string{featureHelpersStep, featureSuiteStep} {
			_ = protectedStepNamed(t, job, stepName)
		}
	}
	job := wf.Jobs[featureDisposableJob]
	if job.RunsOn != "github-runner-pgcopydb-operator" || job.Environment != "" || job.Concurrency.Group != "" {
		t.Fatal("disposable route inherits shared runner protections or credentials")
	}
	if !maps.Equal(job.Permissions, map[string]string{
		permissionContents: permissionRead, permissionPackages: permissionRead,
	}) {
		t.Fatal("disposable route has excessive permissions")
	}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"secrets.E2E", "vars.E2E", "config current-context", "e2e-cluster", "~/.kube"} {
		if strings.Contains(string(encoded), banned) {
			t.Errorf("disposable route contains shared fallback %q", banned)
		}
	}
	steps := []string{
		"Prepare isolated state",
		"Create disposable cluster",
		featureHelpersStep,
		"Require an empty feature slot",
		"Attest runner image",
		featureSuiteStep,
		"Cleanup feature resources",
		"Destroy disposable cluster",
		"Require suite cleanup and teardown success",
	}
	previous := -1
	for _, name := range steps {
		index := protectedStepIndex(t, job, name)
		if index <= previous {
			t.Fatalf("disposable lifecycle order breaks at %s", name)
		}
		previous = index
	}
	for _, name := range []string{
		"Cleanup feature resources",
		"Destroy disposable cluster",
		"Require suite cleanup and teardown success",
	} {
		if protectedStepNamed(t, job, name).If != "always()" {
			t.Errorf("%s does not run after failure", name)
		}
	}
	suite := protectedStepNamed(t, job, featureSuiteStep)
	for key, want := range map[string]string{
		"FEATURE_E2E_SCHEMA_VALIDATION": disposableSchemaProfile, "E2E_SCALE": featureDefaultScale,
		"E2E_CNPG_INSTANCES": "1", "E2E_STORAGE_CLASS": "standard", "E2E_MANAGE_NAMESPACES": trueValue,
		"E2E_KEEP_FIXTURES": falseValue, "E2E_STRESS": falseValue, "E2E_FORCE": falseValue,
		"E2E_PG_SOURCE": "17", "E2E_PG_TARGET": "17", "E2E_PROMETHEUS_URL": "",
		"E2E_PROMETHEUS_PORT_FORWARD": "monitoring/feature-monitoring-prometheus:9090",
		"SUITE_PROFILE":               "${{ needs.resolve.outputs.suite_profile }}",
	} {
		if got, exists := suite.Env[key]; !exists || got != want {
			t.Errorf("disposable suite %s = %q (present %v), want %q", key, got, exists, want)
		}
	}
	if got := job.Outputs["schema_verified"]; got != "${{ steps.bootstrap.outputs.schema_verified }}" {
		t.Errorf("schema verification output = %q", got)
	}
	if got := job.Outputs["teardown"]; got != "${{ steps.teardown.outcome }}" {
		t.Errorf("teardown output = %q", got)
	}
}

func TestFeatureE2EWorkflowKindLifecycle(t *testing.T) {
	job := parseProtectedWorkflow(t, featureWorkflow).Jobs[featureDisposableJob]
	prepare := protectedStepNamed(t, job, "Prepare isolated state")
	create := protectedStepNamed(t, job, "Create disposable cluster")
	destroy := protectedStepNamed(t, job, "Destroy disposable cluster")
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			trusted := filepath.Join(dir, "feature-e2e-trusted/hack")
			if err := os.MkdirAll(trusted, 0o700); err != nil {
				t.Fatal(err)
			}
			fake := `#!/usr/bin/env bash
set -euo pipefail
[[ "$2" == "$FEATURE_E2E_KIND_STATE" && "$KUBECONFIG" == "$2/kubeconfig" ]]
[[ "$2" == "$RUNNER_TEMP"/feature-kind.* ]]
printf '%s\n' "$1" >> "$CALLS"
case "$1" in
  create)
    [[ "$#" == 4 && "$3" == "$GITHUB_WORKSPACE/config/crd/bases/pgcopydb-operator.io_migrations.yaml" ]]
    [[ "$4" == "$CANDIDATE_CRD_SHA256" ]]
    [[ "$CREATE_FAILS" == false ]] ;;
  destroy) [[ "$#" == 2 ]] ;;
  *) exit 1 ;;
esac
`
			if err := os.WriteFile(filepath.Join(trusted, "feature-e2e-kind.sh"), []byte(fake), 0o700); err != nil {
				t.Fatal(err)
			}
			env := append(os.Environ(), "GITHUB_WORKSPACE="+dir, "RUNNER_TEMP="+dir,
				"GITHUB_ENV="+filepath.Join(dir, "env"), "GITHUB_OUTPUT="+filepath.Join(dir, "output"),
				"KUBECONFIG=/not-the-run-kubeconfig", "CREATE_FAILS="+fmt.Sprint(failed),
				"CANDIDATE_CRD_SHA256="+strings.Repeat("a", 64), "CALLS="+filepath.Join(dir, "calls"))
			run := func(script string) error {
				t.Helper()
				cmd := exec.Command("bash", "-c", script)
				cmd.Env = env
				output, runErr := cmd.CombinedOutput()
				if len(output) > 0 {
					t.Errorf("workflow lifecycle printed unexpected output")
				}
				return runErr
			}
			if err := run(prepare.Run); err != nil {
				t.Fatal(err)
			}
			exported := strings.Split(strings.TrimSpace(read(t, filepath.Join(dir, "env"))), "\n")
			if len(exported) != 2 || !strings.HasPrefix(exported[0], "FEATURE_E2E_KIND_STATE="+dir+"/feature-kind.") ||
				exported[1] != "KUBECONFIG="+strings.TrimPrefix(exported[0], "FEATURE_E2E_KIND_STATE=")+"/kubeconfig" {
				t.Fatal("workflow did not export one exact private state/kubeconfig binding")
			}
			env = append(env, exported...)
			if err := run(create.Run); (err != nil) != failed {
				t.Fatalf("create error = %v, want failure %v", err, failed)
			}
			output, _ := os.ReadFile(filepath.Join(dir, "output"))
			if (string(output) == "schema_verified=true\n") == failed {
				t.Fatal("schema output did not follow successful create-owned verification")
			}
			if err := run(destroy.Run); err != nil {
				t.Fatal(err)
			}
			if got := read(t, filepath.Join(dir, "calls")); got != "create\ndestroy\n" {
				t.Errorf("lifecycle calls = %q", got)
			}
		})
	}
}

func featureProofSpecs() gingkotypes.SpecReports {
	metric := ginkgoSpecFixture(gingkotypes.SpecStatePassed, "healthy operator scrape")
	metric.ContainerHierarchyTexts = []string{featureMetricsText}
	metric.ContainerHierarchyLabels = [][]string{{"metrics"}}
	expiry := ginkgoSpecFixture(gingkotypes.SpecStatePassed, featureExpiryLeaf)
	expiry.ContainerHierarchyTexts = []string{"Worker session bounds"}
	expiry.ReportEntries = gingkotypes.ReportEntries{{Name: featureExpiryEntry,
		Value: gingkotypes.WrapEntryValue(map[string]any{
			"migrationUID": "test-migration", "sourceCohortSize": 2, "targetCohortSize": 3,
			"sourceExpiredCount": 2, "targetExpiredCount": 3, "expirySeconds": 120,
			"targetCopyObserved": true, "rollbackRegisteredBeforeFault": true, "faultRuleInstalled": true,
			"faultRuleRemoved": true, "controlSessionHealthy": true, "longCopyOutlivedProbe": true,
			"resumeObserved": true, "dataMatched": true, "backendTerminationUsed": false,
		})}}
	cleanup := ginkgoSpecFixture(gingkotypes.SpecStatePassed, featureCleanupLeaf)
	cleanup.ReportEntries = gingkotypes.ReportEntries{{Name: featureCleanupEntry,
		Value: gingkotypes.WrapEntryValue(map[string]any{
			"migrationUID": "test-cleanup", "jobTTLObserved": true, "failureMetricObserved": true, "firingAlertObserved": true,
		})}}
	return gingkotypes.SpecReports{metric, expiry, cleanup}
}

func TestFeatureE2ESuiteProfiles(t *testing.T) {
	suite := protectedStepNamed(t, parseProtectedWorkflow(t, featureWorkflow).Jobs[featureClusterJob], featureSuiteStep)
	for _, tt := range []struct {
		name, schema, profile, mode, focus, filter string
		wantError                                  bool
	}{
		{name: "default baseline", filter: "!chaos && !flaky && !isolated-runtime-safety"},
		{
			name:    "isolated baseline",
			schema:  disposableSchemaProfile,
			profile: featureBaselineProfile,
			filter:  "!chaos && !flaky && !isolated-runtime-safety",
		},
		{
			name:    "runtime",
			schema:  disposableSchemaProfile,
			profile: featureRuntimeProfile,
			filter:  "(!chaos && !flaky && !isolated-runtime-safety) || (isolated-runtime-safety && !flaky)",
		},
		{name: "unknown schema", schema: featureInvalidInput, wantError: true},
		{name: "unknown suite", profile: featureInvalidInput, wantError: true},
		{name: "shared runtime", profile: featureRuntimeProfile, wantError: true},
		{
			name:      "focused runtime",
			schema:    disposableSchemaProfile,
			profile:   featureRuntimeProfile,
			mode:      focusModeValue,
			focus:     featureFocusText,
			wantError: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := runSuiteFixture(t, suite.Run, suiteFixture{schemaProfile: tt.schema, suiteProfile: tt.profile,
				runMode: tt.mode, focus: tt.focus, report: ginkgoReportFixture(t, true, 3, 3, featureProofSpecs())})
			if tt.wantError {
				if result.err == nil || len(result.args) != 0 || len(result.outputs) != 0 {
					t.Fatalf("invalid suite profile reached candidate execution: %+v", result)
				}
				return
			}
			if result.err != nil || !slices.Contains(result.args, "-ginkgo.label-filter="+tt.filter) {
				t.Fatalf("suite profile: %v; args %q; %s", result.err, result.args, result.output)
			}
			if got := result.outputs["runtime_safety_completed"]; (got == trueValue) != (tt.profile == featureRuntimeProfile) {
				t.Errorf("runtime proof output = %q for %q", got, tt.profile)
			}
		})
	}
}

func TestFeatureE2EDisposableMetricsEvidence(t *testing.T) {
	suite := protectedStepNamed(t, parseProtectedWorkflow(t, featureWorkflow).Jobs[featureClusterJob], featureSuiteStep)
	for _, tt := range []struct {
		name, mode string
		mutate     func(*gingkotypes.SpecReport)
		wantError  bool
	}{
		{name: "passed inherited label"},
		{
			name:      "no metrics",
			mutate:    func(s *gingkotypes.SpecReport) { s.ContainerHierarchyTexts = nil; s.ContainerHierarchyLabels = nil },
			wantError: true,
		},
		{
			name:      "missing inherited label",
			mutate:    func(s *gingkotypes.SpecReport) { s.ContainerHierarchyLabels = nil },
			wantError: true,
		},
		{
			name:      "label without metrics container",
			mutate:    func(s *gingkotypes.SpecReport) { s.ContainerHierarchyTexts = nil },
			wantError: true,
		},
		{
			name:      "skipped metrics",
			mutate:    func(s *gingkotypes.SpecReport) { s.State = gingkotypes.SpecStateSkipped },
			wantError: true,
		},
		{
			name:   "focused nonmetrics",
			mode:   focusModeValue,
			mutate: func(s *gingkotypes.SpecReport) { s.ContainerHierarchyTexts = nil; s.ContainerHierarchyLabels = nil },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			specs := featureProofSpecs()
			if tt.mutate != nil {
				tt.mutate(&specs[0])
			}
			result := runSuiteFixture(t, suite.Run, suiteFixture{schemaProfile: disposableSchemaProfile, runMode: tt.mode,
				focus: "healthy operator scrape", report: ginkgoReportFixture(t, true, 3, 3, specs)})
			if (result.err != nil) != tt.wantError {
				t.Fatalf("metrics gate error = %v, want %v: %s", result.err, tt.wantError, result.output)
			}
			if tt.wantError && (result.outputs["metrics_completed"] != "" || result.outputs[featureCompleteOutput] != "") {
				t.Fatal("invalid metrics emitted completion evidence")
			}
			if !tt.wantError && tt.mode != focusModeValue && result.outputs["metrics_completed"] != trueValue {
				t.Fatal("passed metrics did not emit completion evidence")
			}
		})
	}
}

func TestFeatureE2ERuntimeProofPayloads(t *testing.T) {
	suite := protectedStepNamed(t, parseProtectedWorkflow(t, featureWorkflow).Jobs[featureClusterJob], featureSuiteStep)
	for _, index := range []int{1, 2} {
		payload := featureProofSpecs()[index].ReportEntries[0].Value.GetRawValue().(map[string]any)
		mutations := map[string]func(map[string]any){
			"missing uid": func(p map[string]any) { delete(p, "migrationUID") },
			"empty uid":   func(p map[string]any) { p["migrationUID"] = "" },
			"typed uid":   func(p map[string]any) { p["migrationUID"] = 1 },
		}
		for key, value := range payload {
			if _, ok := value.(bool); ok {
				mutations[key+" reversed"] = func(p map[string]any) { p[key] = !value.(bool) }
				mutations[key+" string"] = func(p map[string]any) { p[key] = "true" }
				mutations[key+" missing"] = func(p map[string]any) { delete(p, key) }
			}
		}
		if index == 1 {
			for _, key := range []string{"sourceCohortSize", "targetCohortSize", "sourceExpiredCount", "targetExpiredCount"} {
				for _, value := range []any{0, -1, 1.5, "2", 7, nil} {
					mutations[fmt.Sprintf("%s %v", key, value)] = func(p map[string]any) { p[key] = value }
				}
			}
			for _, value := range []any{0, -1, 180.1, "120", nil} {
				mutations[fmt.Sprintf("expiry %v", value)] = func(p map[string]any) { p["expirySeconds"] = value }
			}
		}
		for name, mutate := range mutations {
			t.Run(fmt.Sprintf("%d/%s", index, name), func(t *testing.T) {
				specs := featureProofSpecs()
				changed := maps.Clone(payload)
				mutate(changed)
				specs[index].ReportEntries[0].Value = gingkotypes.WrapEntryValue(changed)
				result := runSuiteFixture(t, suite.Run, suiteFixture{
					schemaProfile: disposableSchemaProfile, suiteProfile: featureRuntimeProfile,
					report: ginkgoReportFixture(t, true, 3, 3, specs)})
				if result.err == nil || result.outputs["runtime_safety_completed"] != "" ||
					result.outputs[featureCompleteOutput] != "" {
					t.Fatalf("invalid payload earned completion: %v, %v", result.err, result.outputs)
				}
			})
		}
	}
}

func TestFeatureE2ERuntimeProofEntries(t *testing.T) {
	suite := protectedStepNamed(t, parseProtectedWorkflow(t, featureWorkflow).Jobs[featureClusterJob], featureSuiteStep)
	for _, index := range []int{1, 2} {
		for name, mutate := range map[string]func(*gingkotypes.SpecReport){
			featureMissingValue: func(s *gingkotypes.SpecReport) { s.ReportEntries = nil },
			"duplicate": func(s *gingkotypes.SpecReport) {
				s.ReportEntries = append(s.ReportEntries, s.ReportEntries[0])
			},
			"wrong leaf":   func(s *gingkotypes.SpecReport) { s.LeafNodeText = "unrelated passing scenario" },
			"wrong node":   func(s *gingkotypes.SpecReport) { s.LeafNodeType = gingkotypes.NodeTypeBeforeAll },
			"skipped":      func(s *gingkotypes.SpecReport) { s.State = gingkotypes.SpecStateSkipped },
			"failed":       func(s *gingkotypes.SpecReport) { s.State = gingkotypes.SpecStateFailed },
			"null payload": func(s *gingkotypes.SpecReport) { s.ReportEntries[0].Value = gingkotypes.WrapEntryValue(nil) },
			"string payload": func(s *gingkotypes.SpecReport) {
				s.ReportEntries[0].Value = gingkotypes.WrapEntryValue(`{"migrationUID":"misleading-string"}`)
			},
		} {
			t.Run(fmt.Sprintf("%d/%s", index, name), func(t *testing.T) {
				specs := featureProofSpecs()
				mutate(&specs[index])
				result := runSuiteFixture(t, suite.Run, suiteFixture{
					schemaProfile: disposableSchemaProfile, suiteProfile: featureRuntimeProfile,
					report: ginkgoReportFixture(t, true, 3, 3, specs)})
				if result.err == nil || result.outputs["runtime_safety_completed"] != "" ||
					result.outputs[featureCompleteOutput] != "" {
					t.Fatalf("invalid report entry earned completion: %v, %v", result.err, result.outputs)
				}
			})
		}
	}
	for _, name := range []string{"wrong container", "malformed JSON", "duplicate on other leaf"} {
		t.Run(name, func(t *testing.T) {
			specs := featureProofSpecs()
			switch name {
			case "wrong container":
				specs[1].ContainerHierarchyTexts = []string{"unrelated container"}
			case "duplicate on other leaf":
				specs[0].ReportEntries = specs[1].ReportEntries
			}
			report := ginkgoReportFixture(t, true, 3, 3, specs)
			if name == "malformed JSON" {
				report = mutateGinkgoReport(t, report, func(r map[string]any) {
					s := r["SpecReports"].([]any)[1].(map[string]any)
					s["ReportEntries"].([]any)[0].(map[string]any)["Value"].(map[string]any)["AsJSON"] = "{"
				})
			}
			result := runSuiteFixture(t, suite.Run, suiteFixture{
				schemaProfile: disposableSchemaProfile,
				suiteProfile:  featureRuntimeProfile,
				report:        report,
			})
			if result.err == nil || result.outputs["runtime_safety_completed"] != "" {
				t.Fatalf("invalid proof passed: %v, %v", result.err, result.outputs)
			}
		})
	}
}

func TestFeatureE2EFinalRouteSelection(t *testing.T) {
	final := protectedStepNamed(t, parseProtectedWorkflow(t, featureWorkflow).Jobs["final-status"], "Publish final status")
	passed := map[string]string{
		featureSuiteOutput: successValue, featureCleanupOutput: successValue, "teardown": successValue,
		featureSuiteRanOutput: trueValue, featureCompleteOutput: trueValue,
		featureManagerOutput: trueValue, featureRunnerOutput: trueValue,
		"schema_verified": trueValue, "metrics_completed": trueValue, "runtime_safety_completed": trueValue}
	for _, tt := range []struct {
		name, target, shared, disposable, mode, profile, want string
		mutate                                                func(map[string]string)
		malformed                                             string
	}{
		{
			name:       "shared success",
			target:     featureSharedTarget,
			shared:     successValue,
			disposable: skippedValue,
			want:       successValue,
		},
		{
			name:       "disposable success",
			target:     featureDisposableJob,
			shared:     skippedValue,
			disposable: successValue,
			want:       successValue,
		},
		{
			name:       "runtime success",
			target:     featureDisposableJob,
			shared:     skippedValue,
			disposable: successValue,
			profile:    featureRuntimeProfile,
			want:       successValue,
		},
		{
			name:       "focused nonmetrics",
			target:     featureDisposableJob,
			shared:     skippedValue,
			disposable: successValue,
			mode:       focusModeValue,
			mutate:     func(p map[string]string) { delete(p, "metrics_completed") },
			want:       successValue,
		},
		{name: "absent target", shared: successValue, disposable: skippedValue, want: errorValue},
		{
			name:       "unknown target",
			target:     featureInvalidInput,
			shared:     successValue,
			disposable: skippedValue,
			want:       errorValue,
		},
		{
			name:       "both run",
			target:     featureSharedTarget,
			shared:     successValue,
			disposable: successValue,
			want:       errorValue,
		},
		{
			name:       "neither runs",
			target:     featureDisposableJob,
			shared:     skippedValue,
			disposable: skippedValue,
			want:       errorValue,
		},
		{
			name:       "unselected failure",
			target:     featureDisposableJob,
			shared:     failureValue,
			disposable: successValue,
			want:       errorValue,
		},
		{
			name:       featureCancelledValue,
			target:     featureDisposableJob,
			shared:     skippedValue,
			disposable: featureCancelledValue,
			want:       errorValue,
		},
		{
			name:       "borrowed shared evidence",
			target:     featureDisposableJob,
			shared:     skippedValue,
			disposable: successValue,
			malformed:  "{}",
			want:       errorValue,
		},
		{
			name:       "malformed evidence",
			target:     featureDisposableJob,
			shared:     skippedValue,
			disposable: successValue,
			malformed:  "{",
			want:       errorValue,
		},
		{
			name:       "wrong evidence type",
			target:     featureDisposableJob,
			shared:     skippedValue,
			disposable: successValue,
			malformed:  "[]",
			want:       errorValue,
		},
		{name: "safe assertion failure", target: featureDisposableJob, shared: skippedValue, disposable: failureValue,
			mutate: func(p map[string]string) {
				p[featureSuiteOutput] = failureValue
				delete(p, "metrics_completed")
				delete(p, "runtime_safety_completed")
			}, want: failureValue},
		{name: "missing runtime proof", target: featureDisposableJob,
			shared: skippedValue, disposable: successValue, profile: featureRuntimeProfile,
			mutate: func(p map[string]string) { delete(p, "runtime_safety_completed") }, want: errorValue},
		{
			name:       "runtime on shared",
			target:     featureSharedTarget,
			shared:     successValue,
			disposable: skippedValue,
			profile:    featureRuntimeProfile,
			want:       errorValue,
		},
		{
			name:       featureUnknownProfile,
			target:     featureSharedTarget,
			shared:     successValue,
			disposable: skippedValue,
			profile:    featureInvalidInput,
			want:       errorValue,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			outputs := maps.Clone(passed)
			if tt.mutate != nil {
				tt.mutate(outputs)
			}
			body, err := json.Marshal(outputs)
			if err != nil {
				t.Fatal(err)
			}
			if tt.malformed != "" {
				body = []byte(tt.malformed)
			}
			profile, mode := tt.profile, tt.mode
			if profile == "" {
				profile = featureBaselineProfile
			}
			if mode == "" {
				mode = fullModeValue
			}
			got := runFinalStatusFixture(t, final.Run, tt.shared, successValue, successValue,
				trueValue, trueValue, trueValue, trueValue,
				"VALIDATION_TARGET="+tt.target, "DISPOSABLE_RESULT="+tt.disposable,
				"DISPOSABLE_OUTPUTS="+string(body), "SUITE_PROFILE="+profile, "RUN_MODE="+mode)
			if got != tt.want {
				t.Errorf("selected status = %q, want %q", got, tt.want)
			}
		})
	}
	for _, key := range []string{
		featureSuiteOutput,
		featureCleanupOutput,
		"teardown",
		featureSuiteRanOutput,
		featureCompleteOutput,
		featureManagerOutput,
		featureRunnerOutput,
		"schema_verified",
		"metrics_completed",
		"runtime_safety_completed",
	} {
		for _, value := range []string{"", falseValue, featureInvalidInput, failureValue, skippedValue} {
			t.Run(key+"="+value, func(t *testing.T) {
				outputs := maps.Clone(passed)
				outputs[key] = value
				body, err := json.Marshal(outputs)
				if err != nil {
					t.Fatal(err)
				}
				got := runFinalStatusFixture(t, final.Run, skippedValue, successValue, successValue,
					trueValue, trueValue, trueValue, trueValue,
					"VALIDATION_TARGET=disposable", "DISPOSABLE_RESULT=success",
					"DISPOSABLE_OUTPUTS="+string(body), "SUITE_PROFILE=runtime-safety")
				if got != errorValue {
					t.Errorf("missing selected evidence status = %q, want error", got)
				}
			})
		}
	}
}

func TestFeatureE2EHelmProfiles(t *testing.T) {
	job := parseProtectedWorkflow(t, featureWorkflow).Jobs[featureClusterJob]
	helpers := protectedStepNamed(t, job, featureHelpersStep).Run
	for _, tt := range []struct {
		name, stored, env string
		args              []string
		verifierFails     bool
		wantError         bool
	}{
		{name: featureSharedTarget, stored: identicalSchemaProfile},
		{
			name:   "shared candidate enable",
			stored: identicalSchemaProfile,
			args:   []string{"--set-string", featureRuleEnabled},
		},
		{
			name:      "shared namespace creation",
			stored:    identicalSchemaProfile,
			args:      []string{"--create-namespace"},
			wantError: true,
		},
		{
			name:   "isolated",
			stored: disposableSchemaProfile,
			env:    disposableSchemaProfile,
			args:   []string{"--create-namespace"},
		},
		{
			name:      "isolated invalid namespace flag",
			stored:    disposableSchemaProfile,
			env:       disposableSchemaProfile,
			args:      []string{"--create-namespace=true"},
			wantError: true,
		},
		{
			name:          "no delivery unproved",
			stored:        disposableSchemaProfile,
			env:           disposableSchemaProfile,
			verifierFails: true,
			wantError:     true,
		},
		{
			name:      "environment cannot upgrade shared",
			stored:    identicalSchemaProfile,
			env:       disposableSchemaProfile,
			wantError: true,
		},
		{
			name:      "environment cannot downgrade isolated",
			stored:    disposableSchemaProfile,
			env:       identicalSchemaProfile,
			wantError: true,
		},
		{name: "missing profile", wantError: true},
		{
			name:      featureUnknownProfile,
			stored:    featureInvalidInput,
			env:       featureInvalidInput,
			wantError: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, sub := range []string{"bin", "feature-e2e-trusted/hack", "kind-state"} {
				if err := os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			files := map[string]string{
				"bin/feature-helm":   extractFeatureHelmHeredoc(t, helpers),
				"bin/helm":           "#!/usr/bin/env bash\nprintf 'helm\\n' >> \"$CALLS\"\nprintf '%s\\n' \"$@\" > \"$ARGS\"\n",
				"bin/kubectl":        featurePassingShell,
				"attest-image":       featurePassingShell,
				"ownership":          "feature_capture_controller() { return 0; }\n",
				featureCleanupOutput: featurePassingShell,
				"feature-e2e-trusted/hack/feature-e2e-kind.sh": `#!/usr/bin/env bash
set -euo pipefail
[[ "$#" == 2 && "$1" == verify-ready && "$2" == "$FEATURE_E2E_KIND_STATE" ]]
[[ "$KUBECONFIG" == "$FEATURE_E2E_KIND_STATE/kubeconfig" ]]
printf 'verify\n' >> "$CALLS"
[[ "$VERIFIER_FAILS" == false ]]
`,
			}
			if tt.stored != "" {
				files[featureSchemaFile] = tt.stored
			}
			for name, body := range files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			args := append([]string{
				helmInstallCommand,
				featureControllerName,
				featureFixtureChart,
				"-n",
				featureControllerNS,
				"--wait",
			}, tt.args...)
			cmd := exec.Command(filepath.Join(dir, "bin/feature-helm"), args...)
			cmd.Env = append(os.Environ(), "PATH="+filepath.Join(dir, "bin")+":"+os.Getenv("PATH"),
				"REAL_HELM="+filepath.Join(dir, "bin/helm"), "FEATURE_E2E_HELPERS="+dir, "GITHUB_WORKSPACE="+dir,
				"E2E_OPERATOR_NAMESPACE="+featureControllerNS, "FEATURE_E2E_SCHEMA_VALIDATION="+tt.env,
				"FEATURE_E2E_KIND_STATE="+filepath.Join(dir, "kind-state"),
				"KUBECONFIG="+filepath.Join(dir, "kind-state/kubeconfig"),
				"CALLS="+filepath.Join(dir, "calls"), "ARGS="+filepath.Join(dir, "args"),
				"VERIFIER_FAILS="+fmt.Sprint(tt.verifierFails))
			output, err := cmd.CombinedOutput()
			calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
			if (err != nil) != tt.wantError {
				t.Fatalf("Helm profile error = %v, want %v: %s", err, tt.wantError, output)
			}
			if tt.wantError {
				if strings.Contains(string(calls), "helm") {
					t.Fatal("unproved profile reached Helm")
				}
				return
			}
			wantCalls, enabled := "helm\n", falseValue
			if tt.stored == disposableSchemaProfile {
				wantCalls, enabled = "verify\nhelm\n", trueValue
			}
			if string(calls) != wantCalls {
				t.Errorf("readiness/install order = %q, want %q", calls, wantCalls)
			}
			got := strings.Split(strings.TrimSpace(read(t, filepath.Join(dir, "args"))), "\n")
			index := slices.Index(got, "metrics.prometheusRule.enabled="+enabled)
			if index < len(args) || got[index-1] != helmSetFlag {
				t.Errorf("trusted rule override does not follow candidate arguments: %q", got)
			}
		})
	}
}

func TestFeatureE2ERenderedRuleProfiles(t *testing.T) {
	if _, err := exec.LookPath(kubectlCommand); err != nil {
		t.Skip("kubectl is unavailable")
	}
	job := parseProtectedWorkflow(t, featureWorkflow).Jobs[featureClusterJob]
	helpers := protectedStepNamed(t, job, featureHelpersStep).Run
	binary := buildFeatureRenderSafety(t, extractFeatureGeneratedFile(t, helpers, "render-safety.go", "RENDER_SAFETY"))
	for _, tt := range []struct {
		name, profile, namespace string
		list, duplicate, absent  bool
		wantError                bool
	}{
		{name: "shared no rule", profile: identicalSchemaProfile, absent: true},
		{name: "shared rule", profile: identicalSchemaProfile, wantError: true},
		{name: "shared List rule", profile: identicalSchemaProfile, list: true, wantError: true},
		{name: "isolated omitted namespace", profile: disposableSchemaProfile},
		{
			name:      "isolated explicit namespace",
			profile:   disposableSchemaProfile,
			namespace: featureControllerNS,
		},
		{name: "isolated List omitted namespace", profile: disposableSchemaProfile, list: true},
		{name: "isolated List explicit namespace", profile: disposableSchemaProfile,
			namespace: featureControllerNS, list: true},
		{
			name:      "isolated wrong namespace",
			profile:   disposableSchemaProfile,
			namespace: featureOtherValue,
			wantError: true,
		},
		{
			name:      "isolated List wrong namespace",
			profile:   disposableSchemaProfile,
			namespace: featureOtherValue,
			list:      true,
			wantError: true,
		},
		{name: "isolated duplicate", profile: disposableSchemaProfile, duplicate: true, wantError: true},
		{name: "isolated absent rule", profile: disposableSchemaProfile, absent: true, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "bin"), 0o700); err != nil {
				t.Fatal(err)
			}
			for name, body := range map[string]string{
				featureOwnerFile: imageAttestationOwnerValue, featureSchemaFile: tt.profile,
				"post-renderer": extractFeatureGeneratedFile(t, helpers, "post-renderer", "POST_RENDERER"),
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(binary, filepath.Join(dir, "bin/render-safety")); err != nil {
				t.Fatal(err)
			}
			metadata := map[string]any{
				nameKey:   featureControllerName,
				labelsKey: map[string]any{featureInstanceLabel: featureControllerName},
			}
			if tt.namespace != "" {
				metadata[namespaceKey] = tt.namespace
			}
			rule := map[string]any{apiVersionKey: "monitoring.coreos.com/v1", kindKey: "PrometheusRule", metadataKey: metadata,
				specKey: map[string]any{"groups": []any{}}}
			var object any = rule
			if tt.list {
				object = map[string]any{apiVersionKey: "v1", kindKey: listKind, itemsKey: []any{rule}}
			}
			if tt.absent {
				object = map[string]any{
					apiVersionKey: "v1",
					kindKey:       "ConfigMap",
					metadataKey:   map[string]any{nameKey: "fixture"},
				}
			}
			body, err := json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			if tt.duplicate {
				body = append(append(body, []byte("\n---\n")...), body...)
			}
			cmd := exec.Command("bash", filepath.Join(dir, "post-renderer"))
			cmd.Stdin = bytes.NewReader(body)
			cmd.Env = append(os.Environ(), "FEATURE_E2E_HELPERS="+dir,
				"FEATURE_E2E_OWNER_FILE="+filepath.Join(dir, featureOwnerFile),
				"FEATURE_E2E_OWNER_KEY="+imageAttestationOwnerKey, "FEATURE_E2E_SCHEMA_VALIDATION="+tt.profile)
			output, err := cmd.CombinedOutput()
			if (err != nil) != tt.wantError {
				t.Fatalf("render rule error = %v, want %v", err, tt.wantError)
			}
			if !tt.wantError && !tt.absent && !strings.Contains(string(output), "namespace: pgcopydb-e2e") {
				t.Fatal("isolated rule lacks its explicit namespace")
			}
		})
	}
}

func TestFeatureE2ERuleOutputIdentity(t *testing.T) {
	job := parseProtectedWorkflow(t, featureWorkflow).Jobs[featureClusterJob]
	helpers := protectedStepNamed(t, job, featureHelpersStep).Run
	binary := buildFeatureRenderSafety(t, extractFeatureGeneratedFile(t, helpers, "render-safety.go", "RENDER_SAFETY"))
	for name, mutate := range map[string]func(map[string]any){
		"expected rule":     nil,
		"wrong api":         func(rule map[string]any) { rule[apiVersionKey] = "other/v1" },
		"wrong name":        func(rule map[string]any) { rule[metadataKey].(map[string]any)[nameKey] = featureOtherValue },
		"missing namespace": func(rule map[string]any) { delete(rule[metadataKey].(map[string]any), namespaceKey) },
		"wrong namespace": func(rule map[string]any) {
			rule[metadataKey].(map[string]any)[namespaceKey] = featureOtherValue
		},
		"wrong owner": func(rule map[string]any) {
			rule[metadataKey].(map[string]any)[labelsKey].(map[string]any)[imageAttestationOwnerKey] = featureOtherValue
		},
		"wrong instance": func(rule map[string]any) {
			rule[metadataKey].(map[string]any)[labelsKey].(map[string]any)[featureInstanceLabel] = featureOtherValue
		},
		"duplicate List": func(rule map[string]any) {
			copy := maps.Clone(rule)
			clear(rule)
			rule[apiVersionKey], rule[kindKey], rule[itemsKey] = "v1", listKind, []any{copy, copy}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			rule := map[string]any{apiVersionKey: "monitoring.coreos.com/v1", kindKey: "PrometheusRule",
				metadataKey: map[string]any{nameKey: featureControllerName, namespaceKey: featureControllerNS,
					labelsKey: map[string]any{
						featureInstanceLabel:     featureControllerName,
						imageAttestationOwnerKey: imageAttestationOwnerValue,
					}}}
			if mutate != nil {
				mutate(rule)
			}
			body, err := json.Marshal(rule)
			if err != nil {
				t.Fatal(err)
			}
			for file, value := range map[string]string{
				featureOwnerFile: imageAttestationOwnerValue, featureSchemaFile: disposableSchemaProfile,
				"rendered.json": string(body),
			} {
				if err := os.WriteFile(filepath.Join(dir, file), []byte(value), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(binary, filepath.Join(dir, "rendered.json"))
			cmd.Env = append(os.Environ(), "FEATURE_E2E_HELPERS="+dir,
				"FEATURE_E2E_OWNER_FILE="+filepath.Join(dir, featureOwnerFile), "FEATURE_E2E_OWNER_KEY="+imageAttestationOwnerKey,
				"FEATURE_E2E_SCHEMA_VALIDATION="+disposableSchemaProfile)
			output, err := cmd.CombinedOutput()
			if (err != nil) != (mutate != nil) || strings.Contains(string(output), imageAttestationOwnerValue) {
				t.Fatalf("rendered identity gate error = %v, want error %v", err, mutate != nil)
			}
		})
	}
}

func TestFeatureE2EInstalledCRDCompatibility(t *testing.T) {
	candidatePath, candidateSet := os.LookupEnv("FEATURE_E2E_CANDIDATE_CRD")
	installedPath, installedSet := os.LookupEnv("FEATURE_E2E_INSTALLED_CRD")
	if !candidateSet && !installedSet {
		t.Skip("installed CRD paths not supplied")
	}
	read := func(path string) compatibilityDocument {
		t.Helper()
		body, err := os.ReadFile(path)
		if err != nil || len(bytes.TrimSpace(body)) == 0 {
			t.Fatal("installed-crd-input-invalid")
		}
		decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(body), 4096)
		var document, extra compatibilityDocument
		if decoder.Decode(&document) != nil || len(document) == 0 || decoder.Decode(&extra) != io.EOF {
			t.Fatal("installed-crd-input-invalid")
		}
		return document
	}
	if err := validateInstalledFeatureCRD(read(candidatePath), read(installedPath)); err != nil {
		t.Fatal(err)
	}
}

func validateInstalledFeatureCRD(candidate, installed map[string]any) error {
	for _, document := range []map[string]any{candidate, installed} {
		metadata, ok := document[metadataKey].(map[string]any)
		if !ok || metadata["name"] != "migrations.pgcopydb-operator.io" ||
			document[apiVersionKey] != "apiextensions.k8s.io/v1" || document[kindKey] != "CustomResourceDefinition" {
			return fmt.Errorf("installed-crd-identity-invalid")
		}
		for _, key := range []string{"uid", "resourceVersion", "generation", "creationTimestamp", "managedFields"} {
			delete(metadata, key)
		}
		delete(document, "status")
		spec, ok := document[specKey].(map[string]any)
		if !ok {
			return fmt.Errorf("installed-crd-specification-invalid")
		}
		versions, ok := spec["versions"].([]any)
		if !ok || len(versions) == 0 {
			return fmt.Errorf("installed-crd-versions-invalid")
		}
		for _, value := range versions {
			version, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("installed-crd-version-invalid")
			}
			schema, ok := version[featureCRDSchemaKey].(map[string]any)
			if !ok {
				return fmt.Errorf("installed-crd-schema-invalid")
			}
			if root, ok := schema["openAPIV3Schema"].(map[string]any); !ok || len(root) == 0 {
				return fmt.Errorf("installed-crd-schema-invalid")
			}
		}
		if _, exists := spec[featureCRDConversionKey]; !exists {
			spec[featureCRDConversionKey] = map[string]any{featureStrategyKey: featureNoneConversion}
		}
		removeDescriptions(document)
	}
	if !reflect.DeepEqual(candidate, installed) {
		return fmt.Errorf("installed-crd-differs")
	}
	return nil
}

func TestFeatureE2EInstalledCRDEntrypoint(t *testing.T) {
	for _, scenario := range []string{
		"defaulted", "large integer", "missing candidate", "missing installed", "empty", featureInvalidInput, "multiple",
		featureCRDSchemaKey, featureStorageKey, "identity", featureCRDConversionKey, "metadata",
	} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			candidate := featureSchemaFixture(t)
			installed := featureSchemaFixture(t)
			installed[metadataKey].(map[string]any)["resourceVersion"] = "42"
			installed[metadataKey].(map[string]any)["uid"] = "server-owned"
			installed["status"] = map[string]any{"storedVersions": []any{"v1beta1"}}
			installed[specKey].(map[string]any)[featureCRDConversionKey] = map[string]any{
				featureStrategyKey: featureNoneConversion,
			}
			switch scenario {
			case featureCRDSchemaKey:
				mutateFeatureSchema(installed, "default")
			case featureStorageKey:
				mutateFeatureSchema(installed, featureStorageKey)
			case "identity":
				installed[metadataKey].(map[string]any)["name"] = "other.example"
			case featureCRDConversionKey:
				installed[specKey].(map[string]any)[featureCRDConversionKey] = map[string]any{featureStrategyKey: "Webhook"}
			case "metadata":
				installed[metadataKey].(map[string]any)["labels"] = map[string]any{"unexpected": "label"}
			case "large integer":
				before := candidate[specKey].(map[string]any)["versions"].([]any)[0].(map[string]any)
				after := installed[specKey].(map[string]any)["versions"].([]any)[0].(map[string]any)
				before["large"], after["large"] = json.Number("9007199254740992"), json.Number("9007199254740993")
			}
			candidateBytes, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			installedBytes, err := json.Marshal(installed)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "empty":
				installedBytes = nil
			case featureInvalidInput:
				installedBytes = []byte("{broken")
			case "multiple":
				installedBytes = append(installedBytes, []byte("\n---\n{}\n")...)
			}
			writeCompatibilityFixture(t, dir, "candidate.yaml", string(candidateBytes))
			writeCompatibilityFixture(t, dir, "installed.json", string(installedBytes))
			candidatePath, installedPath := filepath.Join(dir, "candidate.yaml"), filepath.Join(dir, "installed.json")
			if scenario == "missing candidate" {
				candidatePath = ""
			}
			if scenario == "missing installed" {
				installedPath = ""
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestFeatureE2EInstalledCRDCompatibility$", "-test.v")
			cmd.Env = append(os.Environ(), "FEATURE_E2E_CANDIDATE_CRD="+candidatePath,
				"FEATURE_E2E_INSTALLED_CRD="+installedPath)
			output, err := cmd.CombinedOutput()
			if scenario == "defaulted" {
				if err != nil || !strings.Contains(string(output), "--- PASS: TestFeatureE2EInstalledCRDCompatibility") {
					t.Fatalf("entrypoint did not pass: %s", output)
				}
			} else if err == nil {
				t.Fatalf("entrypoint accepted %s: %s", scenario, output)
			}
		})
	}
}

func TestFeatureE2EKindMonitoringSelectors(t *testing.T) {
	documents := readFeatureMonitoringEvidence(t, "../../hack/feature-e2e-kind-monitoring.yaml")
	prometheus := documents[0]["prometheus"].(map[string]any)["prometheusSpec"].(map[string]any)
	for key, want := range map[string]any{
		"ruleSelectorNilUsesHelmValues": false,
		"ruleSelector": map[string]any{
			featureMatchLabelsKey: map[string]any{featureInstanceLabel: featureControllerName},
		},
		"ruleNamespaceSelector": map[string]any{
			featureMatchLabelsKey: map[string]any{"kubernetes.io/metadata.name": featureControllerName},
		},
	} {
		if !reflect.DeepEqual(prometheus[key], want) {
			t.Fatalf("%s does not select isolated rules", key)
		}
	}
}

func TestFeatureE2EKindArguments(t *testing.T) {
	script, err := filepath.Abs("../../hack/feature-e2e-kind.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{nil, {"unknown"}, {"destroy", "/"}, {"verify-ready", "relative"}, {"create"}} {
		cmd := exec.Command("bash", append([]string{script}, args...)...)
		output, err := cmd.CombinedOutput()
		if err == nil || string(output) != "kind-input-invalid\n" {
			t.Fatalf("args %v: %v %s", args, err, output)
		}
	}
}

//nolint:gocyclo // Each branch checks a distinct bootstrap or recovery boundary.
func TestFeatureE2EKindBootstrap(t *testing.T) {
	const containerdValid = "containerd-valid"
	for _, scenario := range []string{
		successValue, "architecture", featureDockerCommand, "kind-checksum", "cnpg-checksum", "monitoring-checksum",
		"cpu", "memory", featureStorageKey, "cgroup", "submount", "observer", "observer-cleanup",
		featureOccupiedCluster, "partial",
		featureNodeIdentity, "cnpg-ready", "prometheus-ready", "storage-class", "pvc", "crd-readback", "alert-delivery",
		"ready-node", "ready-binding", "ready-delivery", "teardown-timeout", "teardown-remains", "absence-failed",
		"observer-partial", "upper-outside", "observer-ownership", "partial-stopped", "node-pid-zero", "pvc-pending",
		"crd-absence-failed",
		"ready-initial-missing", "ready-initial-wrong", "ready-initial-nodes",
		"ready-installed-missing", "ready-installed-empty", "ready-kubeconfig-missing", "ready-kubeconfig-empty",
		containerdValid, "containerd-metadata-timeout", "containerd-metadata-drift", "containerd-pid-drift",
		"containerd-unsupported-driver", "containerd-partial-graphdriver", "containerd-cleanup",
	} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			state := filepath.Join(dir, "state")
			if err := os.Mkdir(state, 0o700); err != nil {
				t.Fatal(err)
			}
			dir, err := filepath.EvalSymlinks(dir)
			if err != nil {
				t.Fatal(err)
			}
			state = filepath.Join(dir, "state")
			writeCompatibilityFixture(t, dir, "candidate.yaml", `{ "candidate": true }`)
			writeCompatibilityFixture(t, dir, "unrelated-cluster", "preserve")
			for _, command := range []string{
				"uname", "stat", featureDockerCommand, "curl", "sha256sum", "kind", "kubectl", "helm", "go", "timeout",
			} {
				path := filepath.Join(dir, "bin", command)
				writeCompatibilityFixture(t, dir, "bin/"+command, featureKindCommandFixture)
				if err := os.Chmod(path, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			script, err := filepath.Abs("../../hack/feature-e2e-kind.sh")
			if err != nil {
				t.Fatal(err)
			}
			activeScenario := scenario
			realTimeout, err := exec.LookPath("timeout")
			if err != nil {
				t.Fatal(err)
			}
			postCreate := strings.HasPrefix(scenario, "ready-") || strings.HasPrefix(scenario, "teardown-")
			if postCreate {
				activeScenario = successValue
			}
			run := func(action string) ([]byte, error) {
				args := []string{script, action, state}
				if action == "create" {
					args = append(args, filepath.Join(dir, "candidate.yaml"), strings.Repeat("a", 64))
				}
				cmd := exec.Command("bash", args...)
				cmd.Env = append(os.Environ(), "PATH="+filepath.Join(dir, "bin")+":"+os.Getenv("PATH"), "RUNNER_TEMP="+dir,
					"FEATURE_E2E_KIND_STATE="+state, "KUBECONFIG="+filepath.Join(state, "kubeconfig"),
					"GITHUB_RUN_ID=123", "GITHUB_RUN_ATTEMPT=2", "FAKE_KIND_ROOT="+dir,
					"FAKE_KIND_SCENARIO="+activeScenario, "FAKE_REAL_TIMEOUT="+realTimeout)
				return cmd.CombinedOutput()
			}
			output, err := run("create")
			defer func() {
				if _, err := os.Stat(filepath.Join(dir, "unrelated-cluster")); err != nil {
					t.Fatal("unrelated cluster removed")
				}
			}()
			if scenario != successValue && scenario != containerdValid && !postCreate {
				if err == nil {
					t.Fatalf("accepted %s: %s", scenario, output)
				}
				if strings.Contains(string(output), "PRIVATE_SENTINEL") {
					t.Fatalf("leaked command output: %s", output)
				}
				if strings.HasPrefix(scenario, "containerd-") {
					calls, readErr := os.ReadFile(filepath.Join(dir, "calls"))
					if readErr != nil {
						t.Fatal(readErr)
					}
					if scenario != "containerd-unsupported-driver" &&
						!strings.Contains(string(calls), "\ndocker rm -f "+strings.Repeat("0", 63)+"2\n") {
						t.Fatal("storage failure omitted exact owned observer cleanup")
					}
					if strings.Contains(scenario, "metadata-") && !strings.Contains(string(calls), "\ndocker exec -i ") {
						t.Fatal("metadata failure did not reach the reader")
					}
				}
				if scenario == "observer-ownership" {
					calls, err := os.ReadFile(filepath.Join(dir, "calls"))
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(calls), "docker start") {
						t.Fatal("unowned observer was executed")
					}
				}
				if scenario != featureOccupiedCluster && scenario != featureNodeIdentity && scenario != "partial-stopped" {
					activeScenario = successValue
				}
				output, err = run("destroy")
				if scenario == featureOccupiedCluster || scenario == featureNodeIdentity {
					if err == nil {
						t.Fatalf("destroy accepted uncertain ownership: %s", output)
					}
				} else if err != nil {
					t.Fatalf("recovery after %s: %v %s", scenario, err, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("create: %v %s", err, output)
			}
			if scenario == containerdValid {
				calls, readErr := os.ReadFile(filepath.Join(dir, "calls"))
				if readErr != nil || strings.Count(string(calls), "\ndocker exec -i ") != 2 ||
					strings.Count(string(calls), "timeout --kill-after=10s 30 docker exec -i ") != 2 {
					t.Fatalf("node metadata must bracket backing checks: %v %s", readErr, calls)
				}
			}
			body, err := os.ReadFile(filepath.Join(state, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			var result map[string]any
			if json.Unmarshal(body, &result) != nil || result["stage"] != featureReadyStage {
				t.Fatalf("not ready: %s", body)
			}
			if postCreate {
				action := "verify-ready"
				mutation := ""
				switch scenario {
				case "ready-node":
					activeScenario = featureNodeIdentity
				case "ready-delivery":
					activeScenario = "alert-delivery"
				case "ready-binding":
					mutation = ".runAttempt = 3"
				case "ready-initial-missing":
					mutation = "del(.evidence.initialAbsence)"
				case "ready-initial-wrong":
					mutation = `.evidence.initialAbsence.candidateSHA256 = "wrong-candidate"`
				case "ready-initial-nodes":
					mutation = ".evidence.initialAbsence.nodes = .nodes"
				case "ready-installed-missing", "ready-installed-empty", "ready-kubeconfig-missing", "ready-kubeconfig-empty":
					file := filepath.Join(state, "installed-crd.json")
					if strings.HasPrefix(scenario, "ready-kubeconfig-") {
						file = filepath.Join(state, "kubeconfig")
					}
					if strings.HasSuffix(scenario, "-missing") {
						err = os.Remove(file)
					} else {
						err = os.WriteFile(file, nil, 0o600)
					}
					if err != nil {
						t.Fatal(err)
					}
				default:
					action = "destroy"
					activeScenario = scenario
				}
				if mutation != "" {
					body, err = exec.Command("jq", mutation, filepath.Join(state, "state.json")).Output()
					if err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(filepath.Join(state, "state.json"), body, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if output, err = run(action); err == nil {
					t.Fatalf("accepted %s: %s", scenario, output)
				}
				if strings.HasPrefix(scenario, "ready-installed-") || strings.HasPrefix(scenario, "ready-kubeconfig-") {
					if output, err = run("destroy"); err != nil {
						t.Fatalf("partial-file teardown after %s: %v %s", scenario, err, output)
					}
				}
				return
			}
			if output, err = run("verify-ready"); err != nil {
				t.Fatalf("verify-ready: %v %s", err, output)
			}
			if output, err = run("destroy"); err != nil {
				t.Fatalf("destroy: %v %s", err, output)
			}
			if _, err := os.Stat(filepath.Join(dir, "cluster")); !os.IsNotExist(err) {
				t.Fatal("owned cluster survived destruction")
			}
		})
	}
}

//nolint:lll // Preserve complete command and JSON wire shapes in the executable fixture.
const featureKindCommandFixture = `#!/usr/bin/env bash
set -euo pipefail
tool=$(basename "$0")
scenario=$FAKE_KIND_SCENARIO
root=$FAKE_KIND_ROOT
printf '%s %s\n' "$tool" "$*" >> "$root/calls"
case "$tool" in
timeout) exec "$FAKE_REAL_TIMEOUT" "$@" ;;
stat) if command -v gstat >/dev/null; then exec gstat "$@"; else exec /usr/bin/stat "$@"; fi ;;
uname) [[ "$scenario" != architecture ]] || { echo unknown; exit; }; [[ "$1" == -s ]] && echo Linux || echo x86_64 ;;
sha256sum)
  case "$1" in
    */candidate.yaml) digest=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ;;
    */kind) digest=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d; [[ "$scenario" != kind-checksum ]] || digest=bad ;;
    */cnpg.tgz) digest=668e065ff53508d58238788fd35b355a925060843629a951df0e6a9362e6d32f; [[ "$scenario" != cnpg-checksum ]] || digest=bad ;;
    */monitoring.tgz) digest=04b90a3f4aab4b40572585095331259bc8f00328e40f9ec588e54cdb9aa6a55f; [[ "$scenario" != monitoring-checksum ]] || digest=bad ;;
    *) exit 80 ;;
  esac
  printf '%s  %s\n' "$digest" "$1" ;;
curl)
  destination=
  while [[ $# -gt 0 ]]; do [[ "$1" != -o ]] || destination=$2; shift; done
  [[ -n "$destination" ]] || exit 81
  if [[ "$destination" == */kind ]]; then cp "$root/bin/kind" "$destination"; else echo archive > "$destination"; fi ;;
kind)
  [[ "$KUBECONFIG" == "$FEATURE_E2E_KIND_STATE/kubeconfig" ]] || exit 82
  case "$1 $2" in
    'get clusters') [[ "$*" == "get clusters" ]] || exit 91
      [[ ! -f "$root/cluster" && "$scenario" != occupied ]] || echo pgcopydb-feature-123-2 ;;
    'create cluster')
      [[ "$*" == 'create cluster --name pgcopydb-feature-123-2 --image kindest/node:v1.36.4@sha256:'*' --wait 5m --kubeconfig '* ]] || exit 92
      touch "$root/cluster"; echo isolated > "$KUBECONFIG"
      [[ "$scenario" != partial && "$scenario" != partial-stopped ]] ;;
    'delete cluster') [[ "$*" == "delete cluster --name pgcopydb-feature-123-2 --kubeconfig $KUBECONFIG" ]] || exit 93
      [[ "$scenario" != teardown-timeout ]] || exit 124; rm "$root/cluster" ;;
    *) exit 83 ;;
  esac ;;
docker)
  case "$1" in
    info) [[ "$scenario" != docker ]] || { echo PRIVATE_SENTINEL >&2; exit 1; }
      echo '{"OSType":"linux","CgroupVersion":"2","Driver":"overlay2","DockerRootDir":"/var/lib/docker","NCPU":8,"MemTotal":17179869184,"DriverStatus":[]}' |
        jq --arg s "$scenario" 'if $s|startswith("containerd-") then .Driver="overlayfs" | .DriverStatus=[["driver-type","io.containerd.snapshotter.v1"]] |
          if $s=="containerd-unsupported-driver" then .Driver="unknown" else . end else . end' ;;
    ps)
      if [[ "$*" == *'label=io.x-k8s.kind.cluster=pgcopydb-feature-123-2'* ]]; then
        [[ "$scenario" != absence-failed ]] || exit 1
        [[ ! -f "$root/cluster" ]] || printf '%064d\n' 1
      elif [[ "$*" == *'name=^/pgcopydb-feature-123-2-capacity$'* ]]; then
        [[ ! -f "$root/observer" ]] || printf '%064d\n' 2
      elif [[ "$*" == *"id=$(printf '%064d' 1)"* && "$scenario" == teardown-remains ]]; then
        printf '%064d\n' 1
      fi ;;
    create)
      [[ "$*" == *'--network none --read-only --cap-drop ALL --security-opt no-new-privileges --pid host --cgroupns host'* ]] || exit 94
      [[ "$*" != *'--cap-add'* && "$*" != *'--privileged'* ]] || exit 95
      touch "$root/observer"; [[ "$scenario" != observer-partial ]] || exit 1
      printf '%s\n' "${@: -2:1}" > "$root/observer-pid"; printf '%064d\n' 2 ;;
    inspect)
      if [[ "$*" == *"$(printf '%064d' 2)"* || "$*" == *pgcopydb-feature-123-2-capacity* ]]; then
        echo '[{"Id":"0000000000000000000000000000000000000000000000000000000000000002",
          "Name":"/pgcopydb-feature-123-2-capacity","Config":{"Labels":{"pgcopydb-operator.io/feature-run":"123-2"},
          "Image":"kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed"},
          "GraphDriver":{"Name":"overlay2","Data":{"UpperDir":"/var/lib/docker/overlay2/observer/diff",
          "MergedDir":"/var/lib/docker/overlay2/observer/merged"}},"State":{"Running":false,"ExitCode":0}}]' |
          jq --arg scenario "$scenario" 'if $scenario=="upper-outside" then .[0].GraphDriver.Data.UpperDir="/other/diff"
            elif $scenario=="observer-ownership" then .[0].Config.Labels["pgcopydb-operator.io/feature-run"]="foreign"
            else . end | if $scenario|startswith("containerd-") then .[0].GraphDriver={"Name":"overlayfs"} |
              if $scenario=="containerd-partial-graphdriver" then .[0].GraphDriver.Data={"UpperDir":"/unproved"}
              elif $scenario=="containerd-valid" then del(.[0].GraphDriver) else . end else . end'
      else
        id=0000000000000000000000000000000000000000000000000000000000000001
        [[ "$scenario" != node-identity ]] || id=0000000000000000000000000000000000000000000000000000000000000003
        printf '[{"Id":"%s","Name":"/pgcopydb-feature-123-2-control-plane","Created":"2026-09-07T00:00:00Z",
          "Config":{"Image":"kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed",
          "Labels":{"io.x-k8s.kind.cluster":"pgcopydb-feature-123-2","io.x-k8s.kind.role":"control-plane"}},
          "GraphDriver":{"Name":"overlay2","Data":{"UpperDir":"/var/lib/docker/overlay2/node/diff",
          "MergedDir":"/var/lib/docker/overlay2/node/merged"}},
          "Mounts":[{"Destination":"/var","Type":"volume","Driver":"local","Source":"/var/lib/docker/volumes/node/_data"}],
          "State":{"Running":true,"Pid":77,"StartedAt":"2026-09-07T00:00:00Z"}}]\n' "$id" |
          jq --arg scenario "$scenario" 'if $scenario=="partial-stopped" then .[0].State.Running=false | .[0].State.Pid=0
            elif $scenario=="node-pid-zero" then .[0].State.Pid=0 else . end |
            if $scenario=="containerd-valid" then del(.[0].GraphDriver)
            elif $scenario|startswith("containerd-") then .[0].GraphDriver={"Name":"overlayfs"} else . end' |
          jq --arg scenario "$scenario" --arg drift "$(test -e "$root/reader-before" && echo yes || echo no)" '
            if $scenario=="containerd-pid-drift" and $drift=="yes" then .[0].State.Pid=78 else . end'
      fi ;;
    exec)
      [[ "$*" == "exec -i $(printf '%064d' 1) /bin/sh -c exec timeout --kill-after=5s 20s /bin/sh -s" ]] || exit 96
      [[ "$scenario" != containerd-metadata-timeout ]] || { echo PRIVATE_SENTINEL; exit 124; }
      if [[ -f "$root/reader-before" ]]; then
        cmp -s "$root/reader-before" - || exit 97
        [[ -f "$root/node-backing-checked" ]] || exit 98
        [[ "$scenario" != containerd-metadata-drift ]] || { echo changed; exit; }
      else cat > "$root/reader-before"; fi
      printf '900 4026532000\n1:100:overlayfs\n1:200:ext2/ext3\nroot mount\nvolume mount\n' ;;
    start)
      cat > "$root/observer-script"
      if [[ "$scenario" == containerd-* && "$(cat "$root/observer-pid")" == 77 ]]; then
        [[ -f "$root/reader-before" ]] || exit 99
        touch "$root/node-backing-checked"
      fi
      case "$scenario" in cpu|memory|storage|cgroup|submount|observer) echo PRIVATE_SENTINEL >&2; exit 1 ;; esac
      printf '{"nodePID":%s,"nodeStartTime":"900","observerStartTime":"800"}\n' "$(cat "$root/observer-pid")" ;;
    rm) [[ "$*" == "rm -f $(printf '%064d' 2)" ]] || exit 100
      [[ "$scenario" != observer-cleanup && "$scenario" != containerd-cleanup ]] || exit 1; rm -f "$root/observer" ;;
    *) exit 84 ;;
  esac ;;
kubectl)
  [[ "$*" == "--kubeconfig $KUBECONFIG --context kind-pgcopydb-feature-123-2 "* ]] || exit 85
  shift 4
  case "$*" in
    'config view -o json') echo '{"current-context":"kind-pgcopydb-feature-123-2","contexts":[{"name":"kind-pgcopydb-feature-123-2","context":{"cluster":"kind-pgcopydb-feature-123-2"}}],"clusters":[{"name":"kind-pgcopydb-feature-123-2"}]}' ;;
    'get nodes -o json') echo '{"items":[{"metadata":{"name":"pgcopydb-feature-123-2-control-plane","uid":"owned-api-node"},"spec":{},"status":{"allocatable":{"cpu":"8","memory":"16777216Ki"},"conditions":[{"type":"Ready","status":"True"},{"type":"MemoryPressure","status":"False"},{"type":"DiskPressure","status":"False"},{"type":"PIDPressure","status":"False"}]}}]}' ;;
    'get storageclass standard -o json') [[ "$scenario" != storage-class ]] || exit 1; echo '{"provisioner":"rancher.io/local-path","volumeBindingMode":"WaitForFirstConsumer"}' ;;
    *'wait '*feature-storage-proof*) [[ "$scenario" != pvc ]] ;;
    *'get pvc feature-storage-proof'*)
      [[ "$scenario" == pvc-pending ]] && echo '{"status":{"phase":"Pending"}}' || echo '{"status":{"phase":"Bound"}}' ;;
    *'rollout status deployment/feature-cnpg'*) [[ "$scenario" != cnpg-ready ]] ;;
    *'rollout status statefulset/prometheus-feature-monitoring-prometheus'*) [[ "$scenario" != prometheus-ready ]] ;;
    'get crd migrations.pgcopydb-operator.io --ignore-not-found -o name') [[ "$scenario" != crd-absence-failed ]] ;;
    'get crd migrations.pgcopydb-operator.io -o json') [[ "$scenario" != crd-readback ]] || exit 1; echo '{"installed":true}' ;;
    'get prometheus feature-monitoring-prometheus -n monitoring -o json') echo '{"kind":"Prometheus","spec":{}}' ;;
    *'/api/v1/status/config') echo '{"status":"success","data":{"yaml":"global: {}"}}' ;;
    *'/api/v1/alertmanagers') echo '{"status":"success","data":{"activeAlertmanagers":[],"droppedAlertmanagers":[]}}' ;;
    'create -f -') cat > /dev/null ;;
    wait*|rollout*|create*|delete*|get\ --raw*) ;;
    *) exit 86 ;;
  esac ;;
helm)
  case "$1" in template) echo 'kind: Prometheus';; upgrade) ;; *) exit 87 ;; esac ;;
go) [[ "$scenario" != alert-delivery ]] || exit 1 ;;
*) exit 88 ;;
esac
`

func TestFeatureE2EMonitoringNoDeliveryEntrypoint(t *testing.T) {
	for _, scenario := range []string{
		"empty destinations", "destination", "additional config", "loaded destination",
		"active", "dropped", featureMissingValue, featureInvalidInput, "error status",
	} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			spec := `{"kind":"Prometheus","spec":{}}`
			loaded := `{"status":"success","data":{"yaml":"global: {}\nalerting:\n  alertmanagers: []\n"}}`
			discovery := `{"status":"success","data":{"activeAlertmanagers":[],"droppedAlertmanagers":[]}}`
			switch scenario {
			case "destination":
				spec = `{"kind":"Prometheus","spec":{"alerting":{"alertmanagers":[{"name":"delivery"}]}}}`
			case "additional config":
				spec = `{"kind":"Prometheus","spec":{"additionalAlertManagerConfigs":{"name":"secret"}}}`
			case "loaded destination":
				loaded = `{"status":"success","data":{"yaml":"alerting:\n  alertmanagers:\n  - static_configs: []\n"}}`
			case "active":
				discovery = `{"status":"success","data":{"activeAlertmanagers":[{}],"droppedAlertmanagers":[]}}`
			case "dropped":
				discovery = `{"status":"success","data":{"activeAlertmanagers":[],"droppedAlertmanagers":[{}]}}`
			case featureMissingValue:
				discovery = `{"status":"success","data":{}}`
			case featureInvalidInput:
				loaded = `{"status":"success","data":{"yaml":"PRIVATE_SENTINEL: ["}}`
			case "error status":
				loaded = `{"status":"error","data":{"yaml":"global: {}"}}`
			}
			writeCompatibilityFixture(t, dir, "spec.json", spec)
			writeCompatibilityFixture(t, dir, "loaded.json", loaded)
			writeCompatibilityFixture(t, dir, "discovery.json", discovery)
			cmd := exec.Command(os.Args[0], "-test.run=^TestFeatureE2EMonitoringNoDelivery$", "-test.v")
			cmd.Env = append(os.Environ(), "FEATURE_E2E_MONITORING_SPEC="+filepath.Join(dir, "spec.json"),
				"FEATURE_E2E_MONITORING_CONFIG="+filepath.Join(dir, "loaded.json"),
				"FEATURE_E2E_MONITORING_DISCOVERY="+filepath.Join(dir, "discovery.json"))
			output, err := cmd.CombinedOutput()
			if scenario == "empty destinations" {
				if err != nil || !strings.Contains(string(output), "--- PASS: TestFeatureE2EMonitoringNoDelivery") {
					t.Fatalf("missing success: %v %s", err, output)
				}
			} else if err == nil {
				t.Fatalf("accepted %s: %s", scenario, output)
			}
			if strings.Contains(string(output), "PRIVATE_SENTINEL") {
				t.Fatal("leaked configuration")
			}
		})
	}
}

func TestFeatureE2EMonitoringNoDelivery(t *testing.T) {
	rendered, renderedSet := os.LookupEnv("FEATURE_E2E_MONITORING_RENDERED")
	spec, specSet := os.LookupEnv("FEATURE_E2E_MONITORING_SPEC")
	config, configSet := os.LookupEnv("FEATURE_E2E_MONITORING_CONFIG")
	discovery, discoverySet := os.LookupEnv("FEATURE_E2E_MONITORING_DISCOVERY")
	if !renderedSet && !specSet && !configSet && !discoverySet {
		t.Skip("monitoring evidence not supplied")
	}
	if renderedSet {
		if specSet || configSet || discoverySet {
			t.Fatal("monitoring-input-invalid")
		}
		checkFeatureMonitoringSpec(t, readFeatureMonitoringEvidence(t, rendered))
		return
	}
	if !specSet || !configSet || !discoverySet {
		t.Fatal("monitoring-input-invalid")
	}
	checkFeatureMonitoringSpec(t, readFeatureMonitoringEvidence(t, spec))
	response := func(path string) map[string]any {
		t.Helper()
		documents := readFeatureMonitoringEvidence(t, path)
		if len(documents) != 1 || documents[0]["status"] != successValue {
			t.Fatal("monitoring-response-invalid")
		}
		data, ok := documents[0]["data"].(map[string]any)
		if !ok {
			t.Fatal("monitoring-response-invalid")
		}
		return data
	}
	loaded, ok := response(config)["yaml"].(string)
	if !ok || strings.TrimSpace(loaded) == "" {
		t.Fatal("monitoring-loaded-config-invalid")
	}
	decoder := k8syaml.NewYAMLOrJSONDecoder(strings.NewReader(loaded), 4096)
	var configuration, extra compatibilityDocument
	if decoder.Decode(&configuration) != nil || len(configuration) == 0 || decoder.Decode(&extra) != io.EOF {
		t.Fatal("monitoring-loaded-config-invalid")
	}
	if alerting, exists := configuration["alerting"]; exists {
		value, ok := alerting.(map[string]any)
		if !ok {
			t.Fatal("monitoring-loaded-config-invalid")
		}
		if destinations, exists := value["alertmanagers"]; exists {
			list, ok := destinations.([]any)
			if !ok || len(list) != 0 {
				t.Fatal("monitoring-delivery-enabled")
			}
		}
	}
	data := response(discovery)
	for _, key := range []string{"activeAlertmanagers", "droppedAlertmanagers"} {
		list, ok := data[key].([]any)
		if !ok || len(list) != 0 {
			t.Fatal("monitoring-discovery-invalid")
		}
	}
}

func readFeatureMonitoringEvidence(t *testing.T, path string) []compatibilityDocument {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil || len(bytes.TrimSpace(body)) == 0 {
		t.Fatal("monitoring-input-invalid")
	}
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(body), 4096)
	var result []compatibilityDocument
	for {
		var document compatibilityDocument
		if err := decoder.Decode(&document); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal("monitoring-input-invalid")
		}
		if len(document) > 0 {
			result = append(result, document)
		}
	}
	if len(result) == 0 {
		t.Fatal("monitoring-input-invalid")
	}
	return result
}

func checkFeatureMonitoringSpec(t *testing.T, documents []compatibilityDocument) {
	t.Helper()
	found := 0
	for _, document := range documents {
		if document[kindKey] == "Alertmanager" || document[kindKey] == "AlertmanagerConfig" {
			t.Fatal("monitoring-delivery-enabled")
		}
		if document[kindKey] != "Prometheus" {
			continue
		}
		found++
		specification, ok := document[specKey].(map[string]any)
		if !ok {
			t.Fatal("monitoring-spec-invalid")
		}
		for _, key := range []string{
			"additionalAlertManagerConfigs", "additionalAlertRelabelConfigs",
			"alertmanagerConfigSelector", "alertmanagerConfigNamespaceSelector",
		} {
			if _, exists := specification[key]; exists {
				t.Fatal("monitoring-delivery-config-present")
			}
		}
		if alerting, exists := specification["alerting"]; exists {
			value, ok := alerting.(map[string]any)
			if !ok {
				t.Fatal("monitoring-alerting-invalid")
			}
			for key, destinations := range value {
				list, ok := destinations.([]any)
				if key != "alertmanagers" || !ok || len(list) != 0 {
					t.Fatal("monitoring-delivery-enabled")
				}
			}
		}
	}
	if found != 1 {
		t.Fatal("monitoring-prometheus-inventory-invalid")
	}
}

func TestFeatureE2EKindNodeMetadata(t *testing.T) {
	body, err := os.ReadFile("../../hack/feature-e2e-kind.sh")
	if err != nil {
		t.Fatal(err)
	}
	_, reader, found := strings.Cut(string(body), "<<'NODE_METADATA'\n")
	if !found {
		t.Fatal("shipped node metadata reader missing")
	}
	reader, _, found = strings.Cut(reader, "\nNODE_METADATA\n")
	if !found {
		t.Fatal("shipped node metadata terminator missing")
	}
	for _, scenario := range []string{
		successValue, "namespace mismatch", "namespace changed", "root changed", "volume changed",
		"start changed", "missing mount", "stat failed", "malformed start",
		"empty reader start", "reader start changed",
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			start := "1 (node) S " + strings.Repeat("0 ", 18) + "900 0\n"
			if scenario == "malformed start" {
				start = "invalid\n"
			}
			writeCompatibilityFixture(t, dir, "proc/1/stat", start)
			mounts := "31 0 0:3 / / rw - overlay overlay rw,lowerdir=/lower,upperdir=/upper,workdir=/work\n"
			if scenario != "missing mount" {
				mounts += "32 31 0:2 /docker/volumes/node/_data /var rw - ext4 /dev/test rw\n"
			}
			writeCompatibilityFixture(t, dir, "proc/self/mountinfo", mounts)
			writeCompatibilityFixture(t, dir, "reader.sh", strings.ReplaceAll(reader, "/proc/", dir+"/proc/"))
			writeCompatibilityFixture(t, dir, "bin/stat", featureKindMetadataStatFixture)
			if err := os.Chmod(filepath.Join(dir, "bin/stat"), 0o755); err != nil {
				t.Fatal(err)
			}
			args := []string{"-c", `mkdir -p "$1/proc/$$"
sed "s/900 0/$2 0/" "$1/proc/1/stat" > "$1/proc/$$/stat"
printf '%s\n' "$$" > "$1/reader-pid"
if [ "$FAKE_METADATA_SCENARIO" = 'empty reader start' ]; then : > "$1/proc/$$/stat"; fi
exec /bin/sh "$1/reader.sh"`, "reader", dir, "800"}
			cmd := exec.Command("sh", args...)
			cmd.Env = append(os.Environ(), "PATH="+filepath.Join(dir, "bin")+":"+os.Getenv("PATH"),
				"FAKE_METADATA_SCENARIO="+scenario, "FAKE_METADATA_ROOT="+dir)
			output, err := cmd.CombinedOutput()
			if scenario == successValue {
				if err != nil || !strings.HasPrefix(string(output), "900 4026532000\n1:100:overlayfs\n1:200:ext2/ext3\n") {
					t.Fatalf("metadata: %v %s", err, output)
				}
				args[len(args)-1] = "801"
				second := exec.Command("sh", args...)
				second.Env = cmd.Env
				secondOutput, secondErr := second.CombinedOutput()
				if secondErr != nil || string(secondOutput) != string(output) {
					t.Fatalf("transient reader identity changed stable node evidence: %v %s", secondErr, secondOutput)
				}
			} else if err == nil {
				t.Fatalf("accepted %s: %s", scenario, output)
			} else if (scenario == "empty reader start" || scenario == "reader start changed") && len(output) != 0 {
				t.Fatalf("invalid reader lifetime emitted metadata: %s", output)
			}
		})
	}
}

const featureKindMetadataStatFixture = `#!/usr/bin/env bash
set -eu
scenario=$FAKE_METADATA_SCENARIO
root=$FAKE_METADATA_ROOT
case "$*" in
  *'/self/ns/mnt')
    if [[ "$scenario" == 'reader start changed' ]]; then
      sed -i.bak 's/800 0/801 0/' "$root/proc/$(cat "$root/reader-pid")/stat"
    fi
    [[ ! -f "$root/namespace-read" ]] && touch "$root/namespace-read" || touch "$root/second-read"
    if [[ "$scenario" == 'namespace mismatch' ||
          ( "$scenario" == 'namespace changed' && -f "$root/second-read" ) ]]; then
      echo 4026532001
    else echo 4026532000; fi ;;
  *'/1/ns/mnt') echo 4026532000 ;;
  *'%d:%i /')
    [[ "$scenario" != 'stat failed' ]] || exit 1
    [[ "$scenario" != 'start changed' ]] || sed -i.bak 's/900 0/901 0/' "$root/proc/1/stat"
    [[ "$scenario" == 'root changed' && -f "$root/second-read" ]] && echo 1:101 || echo 1:100 ;;
  *'%d:%i /var') [[ "$scenario" == 'volume changed' && -f "$root/second-read" ]] && echo 1:201 || echo 1:200 ;;
  *'%T /') echo overlayfs ;;
  *'%T /var') echo ext2/ext3 ;;
  *) exit 1 ;;
esac
`

//nolint:gocyclo // Mutations exercise individual kernel evidence boundaries in the shipped body.
func TestFeatureE2EKindCapacityBody(t *testing.T) {
	body, err := os.ReadFile("../../hack/feature-e2e-kind.sh")
	if err != nil {
		t.Fatal(err)
	}
	_, bodyText, found := strings.Cut(string(body), "<<'CAPACITY'\n")
	if !found {
		t.Fatal("shipped capacity body missing")
	}
	bodyText, _, found = strings.Cut(bodyText, "\nCAPACITY\n")
	if !found {
		t.Fatal("shipped capacity body terminator missing")
	}
	for _, scenario := range []string{
		successValue, featureOwnedMerged, "ancestor cpu", "ancestor memory", "ancestor cpuset", "unknown namespace",
		"masked cgroup", "storage submount", "storage floor", "cpu floor", "memory floor", "missing limit",
		"malformed limit", "pid reused", "different backing", "unowned merged",
		"containerd valid", "containerd rootfs guess", "containerd upper outside", "containerd work outside",
		"containerd lower outside", "containerd first lower outside", "containerd lower missing",
		"containerd snapshot mismatch",
		"containerd separate backing", "containerd hidden mount", "containerd malformed options", "containerd unknown option",
		"containerd node start", "containerd node namespace", "containerd node root", "containerd node volume",
		"containerd observer root", "containerd duplicate option",
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			write := func(name, body string) { writeCompatibilityFixture(t, dir, name, body) }
			for _, level := range []string{"", "/parent", "/parent/observer", "/parent/node"} {
				write("capacity/cgroup"+level+"/cpuset.cpus.effective", "0-7\n")
				if level != "" {
					write("capacity/cgroup"+level+"/cpu.max", "max 100000\n")
					write("capacity/cgroup"+level+"/memory.max", "max\n")
				}
			}
			write("capacity/storage/overlay2/layer/diff/marker", "")
			write("capacity/storage/volumes/marker", "")
			write("proc/observer-stat", "1 (observer) S "+strings.Repeat("0 ", 18)+"800 0\n")
			write("proc/77/stat", "77 (kind node) S "+strings.Repeat("0 ", 18)+"900 0\n")
			write("proc/observer-cgroup", "0::/parent/observer\n")
			write("proc/77/cgroup", "0::/parent/node\n")
			write("proc/meminfo", "MemTotal: 16777216 kB\n")
			profile, metadata := "overlay2", ""
			merged := dir + "/capacity/storage/overlay2/layer/merged"
			upper := dir + "/capacity/storage/overlay2/layer/diff"
			volume := ""
			mounts := "1 0 0:1 / /capacity/cgroup ro - cgroup2 cgroup ro\n" +
				"2 0 0:2 /docker /capacity/storage ro - ext4 /dev/test ro\n"
			if strings.HasPrefix(scenario, "containerd ") {
				profile = "containerd"
				mounts = strings.ReplaceAll(mounts, "- ext4 /dev/test ro", "- ext4 /dev/test rw")
				merged, upper = "", ""
				volume = dir + "/capacity/storage/volumes/node/_data"
				write("capacity/storage/volumes/node/_data/marker", "")
				for _, layer := range []string{"10/fs", "10/work", "9/fs", "8/fs"} {
					write("capacity/storage/containerd/snapshots/"+layer+"/marker", "")
				}
				options := "rw,lowerdir=/docker/containerd/snapshots/9/fs:/docker/containerd/snapshots/8/fs," +
					"upperdir=/docker/containerd/snapshots/10/fs,workdir=/docker/containerd/snapshots/10/work"
				switch scenario {
				case "containerd upper outside":
					options = strings.ReplaceAll(options, "upperdir=/docker/", "upperdir=/elsewhere/")
				case "containerd work outside":
					options = strings.ReplaceAll(options, "workdir=/docker/", "workdir=/elsewhere/")
				case "containerd lower outside":
					options = strings.ReplaceAll(options, ":/docker/", ":/elsewhere/")
				case "containerd first lower outside":
					options = strings.ReplaceAll(options, "lowerdir=/docker/", "lowerdir=/elsewhere/")
				case "containerd lower missing":
					options = strings.ReplaceAll(options, "/8/fs", "/7/fs")
				case "containerd snapshot mismatch":
					options = strings.ReplaceAll(options, "/10/work", "/11/work")
				case "containerd malformed options":
					options = strings.ReplaceAll(options, "lowerdir=", "lowerdir=:")
				case "containerd unknown option":
					options += ",unproved=on"
				case "containerd duplicate option":
					options += ",workdir=/docker/containerd/snapshots/10/work"
				}
				observerID, nodeID := strings.Repeat("0", 63)+"2", strings.Repeat("0", 63)+"1"
				observerMount := "/capacity/storage/runtime/" + observerID + "/rootfs"
				nodeMount := "/capacity/storage/runtime/" + nodeID + "/rootfs"
				write(strings.TrimPrefix(observerMount, "/")+"/marker", "")
				write(strings.TrimPrefix(nodeMount, "/")+"/marker", "")
				mounts += "3 0 0:3 / / rw - overlay overlay " + options + "\n"
				mounts += "4 2 0:3 / " + observerMount + " ro - overlay overlay " + options + "\n"
				mounts += "5 2 0:3 / " + nodeMount + " ro - overlay overlay " + options + "\n"
				metadata = "900 4026532000\n1:100:overlayfs\n1:200:ext2/ext3\n" +
					"31 0 0:3 / / rw - overlay overlay " + options + "\n" +
					"32 31 0:2 /docker/volumes/node/_data /var rw - ext4 /dev/test rw"
				write("proc/77/ns/mnt", "")
				switch scenario {
				case "containerd rootfs guess":
					mounts = strings.ReplaceAll(mounts, nodeID+"/rootfs", "unowned/rootfs")
				case "containerd hidden mount":
					mounts += "6 2 0:2 /masked /capacity/storage/containerd/snapshots ro - ext4 /dev/test ro\n"
				case "containerd node start":
					metadata = strings.Replace(metadata, "900 ", "901 ", 1)
				case "containerd node namespace":
					metadata = strings.Replace(metadata, "4026532000", "unknown", 1)
				case "containerd node root":
					metadata = strings.Replace(metadata, "1:100:", "1:101:", 1)
				case "containerd node volume":
					metadata = strings.Replace(metadata, "1:200:", "1:201:", 1)
				}
			}
			switch scenario {
			case featureOwnedMerged:
				mounts += "3 2 0:3 / /capacity/storage/overlay2/layer/merged ro - overlay overlay ro\n"
			case "ancestor cpu":
				write("capacity/cgroup/parent/cpu.max", "799999 100000\n")
			case "ancestor memory":
				write("capacity/cgroup/parent/memory.max", "17179869183\n")
			case "ancestor cpuset":
				write("capacity/cgroup/parent/cpuset.cpus.effective", "0-6\n")
			case "masked cgroup":
				mounts = strings.ReplaceAll(mounts, "0:1 / /capacity/cgroup", "0:1 /masked /capacity/cgroup")
			case "storage submount":
				mounts += "3 2 0:3 / /capacity/storage/volumes ro - ext4 /dev/other ro\n"
			case "unowned merged":
				mounts += "3 2 0:3 / /capacity/storage/overlay2/unowned/merged ro - overlay overlay ro\n"
			case "memory floor":
				write("proc/meminfo", "MemTotal: 16777215 kB\n")
			case "missing limit":
				if err := os.Remove(filepath.Join(dir, "capacity/cgroup/parent/cpu.max")); err != nil {
					t.Fatal(err)
				}
			case "malformed limit":
				write("capacity/cgroup/parent/cpu.max", "invalid\n")
			}
			relocate := strings.NewReplacer("/capacity/", dir+"/capacity/", "/proc/", dir+"/proc/")
			write("proc/self/mountinfo", relocate.Replace(mounts))
			write("probe.sh", relocate.Replace(bodyText))
			for _, command := range []string{"stat", "df", "getconf", "readlink"} {
				write("bin/"+command, featureKindCapacityFixture)
				if err := os.Chmod(filepath.Join(dir, "bin", command), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			start := "900"
			if scenario == "pid reused" {
				start = "901"
			}
			cmd := exec.Command("bash", "-c", `mkdir -p "$1/proc/$$"
cp "$1/proc/observer-stat" "$1/proc/$$/stat"
cp "$1/proc/observer-cgroup" "$1/proc/$$/cgroup"
exec bash "$1/probe.sh" 77 "$2" "$3" "$4" "$5" "$6" /docker "$7" "$8" "$9"`, "capacity", dir, start,
				merged, upper, volume, profile, strings.Repeat("0", 63)+"2", strings.Repeat("0", 63)+"1", metadata)
			cmd.Env = append(os.Environ(), "PATH="+filepath.Join(dir, "bin")+":"+os.Getenv("PATH"),
				"FAKE_CAPACITY_SCENARIO="+scenario)
			output, err := cmd.CombinedOutput()
			if scenario == successValue || scenario == featureOwnedMerged || scenario == "containerd valid" {
				if err != nil || !strings.Contains(string(output), `"nodeStartTime":"900"`) {
					t.Fatalf("capacity: %v %s", err, output)
				}
			} else if err == nil {
				t.Fatalf("accepted %s: %s", scenario, output)
			} else if strings.HasPrefix(scenario, "containerd ") && string(output) != "kind-storage-unproved\n" {
				t.Fatalf("did not reach containerd storage rejection: %v %s", err, output)
			}
		})
	}
}

const featureKindCapacityFixture = `#!/usr/bin/env bash
set -eu
case "$(basename "$0")" in
stat)
  case "$*" in
    *'%t '*'/proc/self/ns/cgroup') echo 6e736673 ;;
    *'%i '*'/proc/self/ns/cgroup')
      [[ "$FAKE_CAPACITY_SCENARIO" == 'unknown namespace' ]] && echo 4000 || echo 4026531835 ;;
    *'%t '*'/capacity/cgroup') echo 63677270 ;;
    *'%i '*'/ns/mnt') echo 4026532000 ;;
    *'%d:%i '*'/volumes/node/_data') echo 1:200 ;;
    *'%d:%i '*'/0000000000000000000000000000000000000000000000000000000000000002/rootfs')
      [[ "$FAKE_CAPACITY_SCENARIO" == 'containerd observer root' ]] && echo 1:101 || echo 1:100 ;;
    *'%d:%i '*) echo 1:100 ;;
    *'%T '*'/volumes/node/_data') echo ext2/ext3 ;;
    *'%T '*) echo overlayfs ;;
    *'%d '*'/snapshots/8/fs') [[ "$FAKE_CAPACITY_SCENARIO" == 'containerd separate backing' ]] && echo 2 || echo 1 ;;
    *'%d '*'/overlay2/layer/diff') [[ "$FAKE_CAPACITY_SCENARIO" == 'different backing' ]] && echo 2 || echo 1 ;;
    *'%d '*) echo 1 ;;
    *) exit 1 ;;
  esac ;;
df)
  available=33554432
  [[ "$FAKE_CAPACITY_SCENARIO" != 'storage floor' ]] || available=33554431
  printf 'Filesystem 1024-blocks Used Available Capacity Mounted\n/dev/test 40000000 0 %s 0%% /data\n' "$available" ;;
getconf) [[ "$FAKE_CAPACITY_SCENARIO" == 'cpu floor' ]] && echo 7 || echo 8 ;;
readlink) if command -v greadlink >/dev/null; then exec greadlink "$@"; else exec /usr/bin/readlink "$@"; fi ;;
esac
`

func compareFeatureCRDs(base, head map[string]string, profile string) error {
	if profile != "" && profile != identicalSchemaProfile && profile != disposableSchemaProfile {
		return fmt.Errorf("unknown schema validation profile")
	}
	if maps.Equal(base, head) {
		return nil
	}
	if profile != disposableSchemaProfile || len(base) != len(head) {
		return fmt.Errorf("CRD inventory or schema differs")
	}
	for identity, original := range base {
		candidate, found := head[identity]
		if !found {
			return fmt.Errorf("CRD identity differs")
		}
		var before, after compatibilityDocument
		if json.Unmarshal([]byte(original), &before) != nil || json.Unmarshal([]byte(candidate), &after) != nil {
			return fmt.Errorf("CRD is not canonical JSON")
		}
		if err := compareFeatureCRDDocument(before, after); err != nil {
			return err
		}
	}
	return nil
}

func compareFeatureCRDDocument(base, head map[string]any) error {
	baseSpec, baseOK := base[specKey].(map[string]any)
	headSpec, headOK := head[specKey].(map[string]any)
	if !baseOK || !headOK {
		return fmt.Errorf("CRD specification is missing")
	}
	baseVersions, baseOK := baseSpec["versions"].([]any)
	headVersions, headOK := headSpec["versions"].([]any)
	if !baseOK || !headOK || len(baseVersions) != len(headVersions) || len(baseVersions) == 0 {
		return fmt.Errorf("CRD versions differ or are missing")
	}
	var servedAdditions map[string]string
	for i, value := range baseVersions {
		before, beforeOK := value.(map[string]any)
		after, afterOK := headVersions[i].(map[string]any)
		if !beforeOK || !afterOK {
			return fmt.Errorf("CRD version is malformed")
		}
		beforeSchema, beforeOK := before[featureCRDSchemaKey].(map[string]any)
		afterSchema, afterOK := after[featureCRDSchemaKey].(map[string]any)
		if !beforeOK || !afterOK {
			return fmt.Errorf("CRD schema is missing")
		}
		beforeRoot, beforeOK := beforeSchema["openAPIV3Schema"].(map[string]any)
		afterRoot, afterOK := afterSchema["openAPIV3Schema"].(map[string]any)
		if !beforeOK || !afterOK {
			return fmt.Errorf("CRD root schema is missing")
		}
		additions := map[string]string{}
		if err := validateFeatureSchemaAdditions(beforeRoot, afterRoot, nil, additions); err != nil {
			return err
		}
		if before["served"] == true {
			if servedAdditions != nil && !maps.Equal(servedAdditions, additions) {
				return fmt.Errorf("served versions have different schema additions")
			}
			servedAdditions = additions
		}
		delete(beforeSchema, "openAPIV3Schema")
		delete(afterSchema, "openAPIV3Schema")
	}
	if !reflect.DeepEqual(base, head) {
		return fmt.Errorf("existing CRD identity or schema metadata differs")
	}
	return nil
}

func validateFeatureSchemaAdditions(base, head map[string]any, path []string, additions map[string]string) error {
	if reflect.DeepEqual(base, head) {
		return nil
	}
	before, beforeOK := base[schemaPropertiesKey].(map[string]any)
	after, afterOK := head[schemaPropertiesKey].(map[string]any)
	if _, exists := base[schemaPropertiesKey]; exists && !beforeOK {
		return fmt.Errorf("invalid original schema properties")
	}
	if _, exists := head[schemaPropertiesKey]; exists && !afterOK {
		return fmt.Errorf("invalid candidate schema properties")
	}
	for key, child := range before {
		candidate, exists := after[key]
		if !exists {
			return fmt.Errorf("existing property removed")
		}
		baseChild, baseOK := child.(map[string]any)
		headChild, headOK := candidate.(map[string]any)
		if !baseOK || !headOK {
			return fmt.Errorf("schema property is not an object")
		}
		childPath := append(slices.Clone(path), key)
		if err := validateFeatureSchemaAdditions(baseChild, headChild, childPath, additions); err != nil {
			return err
		}
	}
	for key, child := range after {
		if _, exists := before[key]; exists {
			continue
		}
		if len(path) == 0 || (path[0] != specKey && path[0] != statusKey) {
			return fmt.Errorf("new property outside spec or status")
		}
		if _, valid := child.(map[string]any); !valid {
			return fmt.Errorf("new property is not a schema")
		}
		encoded, err := json.Marshal(child)
		if err != nil {
			return err
		}
		encodedPath, err := json.Marshal(append(slices.Clone(path), key))
		if err != nil {
			return err
		}
		additions["property:"+string(encodedPath)] = string(encoded)
	}
	baseKeywords, headKeywords := maps.Clone(base), maps.Clone(head)
	delete(baseKeywords, schemaPropertiesKey)
	delete(headKeywords, schemaPropertiesKey)
	if !reflect.DeepEqual(baseKeywords["x-kubernetes-validations"], headKeywords["x-kubernetes-validations"]) &&
		slices.Equal(path, []string{specKey, "clone"}) && approvedSplitRule(base, head) {
		encoded, err := json.Marshal(headKeywords["x-kubernetes-validations"])
		if err != nil {
			return err
		}
		additions["validation:spec/clone"] = string(encoded)
		delete(baseKeywords, "x-kubernetes-validations")
		delete(headKeywords, "x-kubernetes-validations")
	}
	if !reflect.DeepEqual(baseKeywords, headKeywords) {
		return fmt.Errorf("existing schema keyword differs")
	}
	return nil
}

func approvedSplitRule(base, head map[string]any) bool {
	baseProps, _ := base[schemaPropertiesKey].(map[string]any)
	headProps, _ := head[schemaPropertiesKey].(map[string]any)
	if _, exists := baseProps["splitTables"]; exists {
		return false
	}
	field, ok := headProps["splitTables"].(map[string]any)
	if !ok || field[schemaTypeKey] != schemaBooleanType || field[schemaDefaultKey] != true {
		return false
	}
	before, _ := base["x-kubernetes-validations"].([]any)
	after, ok := head["x-kubernetes-validations"].([]any)
	if !ok || len(after) != len(before)+1 || !slices.EqualFunc(before, after[:len(before)], reflect.DeepEqual) {
		return false
	}
	rule, ok := after[len(before)].(map[string]any)
	if !ok || len(rule) != 2 || rule["rule"] != splitTablesRule {
		return false
	}
	message, ok := rule["message"].(string)
	return ok && strings.TrimSpace(message) != ""
}

func compareFeatureRenderedPrivileges(base, head []string, profile string) error {
	baseCRDs, headCRDs := map[string]string{}, map[string]string{}
	other := make([][]string, 2)
	for i, documents := range [][]string{base, head} {
		crds := baseCRDs
		if i == 1 {
			crds = headCRDs
		}
		seen := map[string]bool{}
		for _, document := range documents {
			identity, body, ok := strings.Cut(document, "\x00")
			if !ok || seen[identity] {
				return fmt.Errorf("rendered identity is missing or duplicated")
			}
			seen[identity] = true
			if strings.Contains(identity, "/CustomResourceDefinition/") {
				crds[identity] = body
			} else {
				other[i] = append(other[i], document)
			}
		}
	}
	if !slices.Equal(other[0], other[1]) {
		return fmt.Errorf("rendered RBAC differs")
	}
	return compareFeatureCRDs(baseCRDs, headCRDs, profile)
}

const (
	schemaPropertiesKey       = "properties"
	schemaDefaultKey          = "default"
	schemaBooleanType         = "boolean"
	schemaRequiredKey         = "required"
	unknownValue              = "unknown"
	optionalSchemaMutation    = "optional"
	schemaListFixture         = "list"
	asymmetricSchemaMutation  = "asymmetric"
	unguardedSplitMutation    = "unguarded"
	oldRuleSplitMutation      = "old-rule"
	missingSplitMutation      = "no-split"
	falseSplitMutation        = "false-split"
	extraSplitKeyMutation     = "extra-key"
	emptySplitMessageMutation = "empty-message"
	featureClusterJob         = "cluster"
	identicalSchemaProfile    = "identical"
	disposableSchemaProfile   = "additive-disposable"
	splitTablesRule           = "!has(self.splitTables) || self.splitTables || " +
		"(!has(self.splitTablesLargerThan) && !has(self.splitMaxParts))"
)

func TestFeatureE2ESchemaProfileAdmission(t *testing.T) {
	t.Setenv("FEATURE_E2E_HELM", newCompatibilityHelmFixture(t))
	for _, tt := range []struct {
		name      string
		profile   string
		mutation  string
		wantError bool
	}{
		{"unset identical", "", "", false},
		{"explicit identical", identicalSchemaProfile, "", false},
		{"disposable smoke", disposableSchemaProfile, "", false},
		{featureUnknownProfile, unknownValue, "", true},
		{"optional boolean strict", identicalSchemaProfile, optionalSchemaMutation, true},
		{"optional boolean both versions", disposableSchemaProfile, optionalSchemaMutation, false},
		{"optional status child requirements", disposableSchemaProfile, "status", false},
		{"changed existing default", disposableSchemaProfile, schemaDefaultKey, true},
		{"changed existing type", disposableSchemaProfile, schemaTypeKey, true},
		{"changed existing bounds", disposableSchemaProfile, "bounds", true},
		{"changed existing pruning", disposableSchemaProfile, "pruning", true},
		{"changed existing enum", disposableSchemaProfile, "enum", true},
		{"changed existing nullable", disposableSchemaProfile, "nullable", true},
		{"changed existing list semantics", disposableSchemaProfile, schemaListFixture, true},
		{"changed existing combinator", disposableSchemaProfile, "combinator", true},
		{"removed existing property", disposableSchemaProfile, "remove", true},
		{"new required property", disposableSchemaProfile, schemaRequiredKey, true},
		{"property outside spec and status", disposableSchemaProfile, "outside", true},
		{"asymmetric served versions", disposableSchemaProfile, asymmetricSchemaMutation, true},
		{"changed API group", disposableSchemaProfile, "group", true},
		{"changed conversion", disposableSchemaProfile, featureCRDConversionKey, true},
		{"changed storage version", disposableSchemaProfile, featureStorageKey, true},
		{"new guarded split rule", disposableSchemaProfile, "split", false},
		{"unguarded parent rule", disposableSchemaProfile, unguardedSplitMutation, true},
		{"changed old parent rule", disposableSchemaProfile, oldRuleSplitMutation, true},
		{"missing new split field", disposableSchemaProfile, missingSplitMutation, true},
		{"false split default", disposableSchemaProfile, falseSplitMutation, true},
		{"extra split rule key", disposableSchemaProfile, extraSplitKeyMutation, true},
		{"empty split message", disposableSchemaProfile, emptySplitMessageMutation, true},
		{"rendered-only drift", disposableSchemaProfile, "rendered", true},
		{"builder mode changed", disposableSchemaProfile, "builder-mode", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("FEATURE_E2E_SCHEMA_VALIDATION", tt.profile)
			base, head := newCompatibilityChartRoots(t)
			for _, root := range []string{base, head} {
				doc := featureSchemaFixture(t)
				if root == head {
					mutateFeatureSchema(doc, tt.mutation)
				}
				body, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				writeCompatibilityFixture(t, root, "config/crd/bases/migration.yaml", string(body)+"\n")
				if tt.mutation == "rendered" && root == head {
					doc[specKey].(map[string]any)["group"] = "changed.example"
					body, err = json.Marshal(doc)
					if err != nil {
						t.Fatal(err)
					}
				}
				writeCompatibilityFixture(t, root, "charts/pgcopydb-operator/templates/crd.yaml", string(body)+"\n")
			}
			if tt.mutation == "builder-mode" {
				if err := os.Chmod(head+"/images/pgcopydb-builder/Dockerfile", 0o755); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestFeatureE2ECandidateCompatibility$", "-test.count=1")
			cmd.Env = compatibilityFixtureEnv(base, head)
			output, err := cmd.CombinedOutput()
			if (err != nil) != tt.wantError {
				t.Fatalf("compatibility error = %v, want error %v:\n%s", err, tt.wantError, output)
			}
			if err != nil && !strings.Contains(string(output), "candidate changes") {
				t.Fatalf("fixture failed before candidate comparison: %v\n%s", err, output)
			}
		})
	}
}

func TestFeatureE2ESchemaLargeIntegerDefaults(t *testing.T) {
	t.Setenv("FEATURE_E2E_HELM", newCompatibilityHelmFixture(t))
	for _, tt := range []struct {
		name, profile, sourceDefault, renderedDefault string
		existing, sourceError, renderedError          bool
	}{
		{"equal additions", disposableSchemaProfile, "9007199254740993", "9007199254740993", false, false, false},
		{"asymmetric source", disposableSchemaProfile, "9007199254740992", "9007199254740993", false, true, false},
		{"asymmetric render", disposableSchemaProfile, "9007199254740993", "9007199254740992", false, false, true},
		{"asymmetric both", disposableSchemaProfile, "9007199254740992", "9007199254740992", false, true, true},
		{"strict existing change", identicalSchemaProfile, "9007199254740993", "9007199254740993", true, true, true},
		{"additive existing change", disposableSchemaProfile, "9007199254740993", "9007199254740993", true, true, true},
	} {
		for _, prefix := range []string{"", "---\n"} {
			t.Run(fmt.Sprintf("%s/yaml=%t", tt.name, prefix != ""), func(t *testing.T) {
				t.Setenv("FEATURE_E2E_SCHEMA_VALIDATION", tt.profile)
				base, head := newCompatibilityChartRoots(t)
				for _, root := range []string{base, head} {
					for path, firstDefault := range map[string]string{
						"config/crd/bases/migration.yaml":             tt.sourceDefault,
						"charts/pgcopydb-operator/templates/crd.yaml": tt.renderedDefault,
					} {
						doc := featureSchemaFixture(t)
						if root == head || tt.existing {
							versions := doc[specKey].(map[string]any)["versions"].([]any)
							for i, item := range versions {
								value := "9007199254740993"
								if root == base {
									value = "9007199254740992"
								} else if i == 0 {
									value = firstDefault
								}
								version := item.(map[string]any)
								schema := version[featureCRDSchemaKey].(map[string]any)["openAPIV3Schema"].(map[string]any)
								spec := schema[schemaPropertiesKey].(map[string]any)[specKey].(map[string]any)
								spec[schemaPropertiesKey].(map[string]any)["largeDefault"] = map[string]any{
									schemaTypeKey: "integer", schemaDefaultKey: json.Number(value),
								}
							}
						}
						body, err := json.Marshal(doc)
						if err != nil {
							t.Fatal(err)
						}
						writeCompatibilityFixture(t, root, path, prefix+string(body)+"\n")
					}
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestFeatureE2ECandidateCompatibility$", "-test.count=1")
				cmd.Env = compatibilityFixtureEnv(base, head)
				output, err := cmd.CombinedOutput()
				if (err != nil) != (tt.sourceError || tt.renderedError) ||
					strings.Contains(string(output), "candidate changes the CRD") != tt.sourceError ||
					strings.Contains(string(output), "candidate changes rendered") != tt.renderedError {
					t.Fatalf("numeric admission error = %v, want source=%t rendered=%t:\n%s",
						err, tt.sourceError, tt.renderedError, output)
				}
			})
		}
	}
}

func featureSchemaFixture(t *testing.T) map[string]any {
	t.Helper()
	const schema = `{"type":"object","properties":{
  "spec":{"type":"object","properties":{
    "clone":{"type":"object","x-kubernetes-validations":[{"rule":"true","message":"existing"}],
      "properties":{"splitTablesLargerThan":{"type":"integer","minimum":0},
        "splitMaxParts":{"type":"integer","minimum":0}}},
    "follow":{"type":"boolean","default":false}}},
  "status":{"type":"object","properties":{"phase":{"type":"string"}}}}}`
	var doc map[string]any
	body := `{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition",
  "metadata":{"name":"migrations.pgcopydb-operator.io"},
  "spec":{"group":"pgcopydb-operator.io","scope":"Namespaced",
    "names":{"kind":"Migration","plural":"migrations"},"versions":[
      {"name":"v1alpha1","served":true,"storage":false,"schema":{"openAPIV3Schema":` + schema + `}},
      {"name":"v1beta1","served":true,"storage":true,"schema":{"openAPIV3Schema":` + schema + `}}]}}`
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

//nolint:gocyclo // Each mutation targets one admission invariant.
func mutateFeatureSchema(doc map[string]any, mutation string) {
	spec := doc[specKey].(map[string]any)
	if mutation == "group" {
		spec["group"] = "changed.example"
	}
	if mutation == featureCRDConversionKey {
		spec[featureCRDConversionKey] = map[string]any{featureStrategyKey: featureNoneConversion}
	}
	for i, item := range spec["versions"].([]any) {
		version := item.(map[string]any)
		if mutation == featureStorageKey {
			version[featureStorageKey] = i == 0
		}
		root := version[featureCRDSchemaKey].(map[string]any)["openAPIV3Schema"].(map[string]any)
		props := root[schemaPropertiesKey].(map[string]any)
		specSchema := props[specKey].(map[string]any)
		fields := specSchema[schemaPropertiesKey].(map[string]any)
		follow := fields["follow"].(map[string]any)
		clone := fields["clone"].(map[string]any)
		switch mutation {
		case optionalSchemaMutation, schemaRequiredKey, asymmetricSchemaMutation:
			if mutation != asymmetricSchemaMutation || i == 0 {
				fields["requireSameMajorVersion"] = map[string]any{schemaTypeKey: schemaBooleanType, schemaDefaultKey: false,
					"x-kubernetes-validations": []any{map[string]any{"rule": "self == oldSelf", "message": "immutable"}}}
			}
			if mutation == schemaRequiredKey {
				specSchema[schemaRequiredKey] = []any{"requireSameMajorVersion"}
			}
		case "status":
			props[statusKey].(map[string]any)[schemaPropertiesKey].(map[string]any)["sample"] = map[string]any{
				schemaTypeKey: "object", schemaRequiredKey: []any{"count"},
				schemaPropertiesKey: map[string]any{"count": map[string]any{schemaTypeKey: "integer"}},
			}
		case schemaDefaultKey:
			follow[schemaDefaultKey] = true
		case schemaTypeKey:
			follow[schemaTypeKey] = schemaStringType
		case "bounds":
			clone[schemaPropertiesKey].(map[string]any)["splitMaxParts"].(map[string]any)["minimum"] = 1
		case "pruning":
			specSchema["x-kubernetes-preserve-unknown-fields"] = true
		case "enum":
			follow["enum"] = []any{true}
		case "nullable":
			follow["nullable"] = true
		case schemaListFixture:
			clone["x-kubernetes-map-type"] = "atomic"
		case "combinator":
			specSchema["allOf"] = []any{map[string]any{schemaRequiredKey: []any{"follow"}}}
		case "remove":
			delete(fields, "follow")
		case "outside":
			props["extra"] = map[string]any{schemaTypeKey: schemaStringType}
		case "split", unguardedSplitMutation, oldRuleSplitMutation, missingSplitMutation,
			falseSplitMutation, extraSplitKeyMutation, emptySplitMessageMutation:
			newRule := map[string]any{
				"rule": "!has(self.splitTables) || self.splitTables || " +
					"(!has(self.splitTablesLargerThan) && !has(self.splitMaxParts))",
				"message": "split tuning requires splitting",
			}
			if mutation == unguardedSplitMutation {
				newRule["rule"] = "self.splitTables"
			}
			if mutation == extraSplitKeyMutation {
				newRule["reason"] = "FieldValueInvalid"
			}
			if mutation == emptySplitMessageMutation {
				newRule["message"] = ""
			}
			if mutation != missingSplitMutation {
				clone[schemaPropertiesKey].(map[string]any)["splitTables"] = map[string]any{
					schemaTypeKey: schemaBooleanType, schemaDefaultKey: mutation != falseSplitMutation,
				}
			}
			clone["x-kubernetes-validations"] = append(clone["x-kubernetes-validations"].([]any), newRule)
			if mutation == oldRuleSplitMutation {
				clone["x-kubernetes-validations"].([]any)[0].(map[string]any)["rule"] = "false"
			}
		}
	}
}
