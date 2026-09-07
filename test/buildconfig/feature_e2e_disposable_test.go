package buildconfig

import (
	"bytes"
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
)

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
				"uname", "stat", featureDockerCommand, "curl", "sha256sum", "kind", "kubectl", "helm", "go",
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
					"FAKE_KIND_SCENARIO="+activeScenario)
				return cmd.CombinedOutput()
			}
			output, err := run("create")
			defer func() {
				if _, err := os.Stat(filepath.Join(dir, "unrelated-cluster")); err != nil {
					t.Fatal("unrelated cluster removed")
				}
			}()
			if scenario != successValue && !postCreate {
				if err == nil {
					t.Fatalf("accepted %s: %s", scenario, output)
				}
				if strings.Contains(string(output), "PRIVATE_SENTINEL") {
					t.Fatalf("leaked command output: %s", output)
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
    info) [[ "$scenario" != docker ]] || { echo PRIVATE_SENTINEL >&2; exit 1; }; echo '{"OSType":"linux","CgroupVersion":"2","Driver":"overlay2","DockerRootDir":"/var/lib/docker","NCPU":8,"MemTotal":17179869184,"DriverStatus":[]}' ;;
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
            else . end'
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
            elif $scenario=="node-pid-zero" then .[0].State.Pid=0 else . end'
      fi ;;
    start)
      cat > "$root/observer-script"
      case "$scenario" in cpu|memory|storage|cgroup|submount|observer) echo PRIVATE_SENTINEL >&2; exit 1 ;; esac
      printf '{"nodePID":%s,"nodeStartTime":"900","observerStartTime":"800"}\n' "$(cat "$root/observer-pid")" ;;
    rm) [[ "$scenario" != observer-cleanup ]] || exit 1; rm -f "$root/observer" ;;
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
		"active", "dropped", "missing", featureInvalidInput, "error status",
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
			case "missing":
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
			mounts := "1 0 0:1 / /capacity/cgroup ro - cgroup2 cgroup ro\n" +
				"2 0 0:2 /docker /capacity/storage ro - ext4 /dev/test ro\n"
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
exec bash "$1/probe.sh" 77 "$2" "$3" "$4" ""`, "capacity", dir, start,
				dir+"/capacity/storage/overlay2/layer/merged", dir+"/capacity/storage/overlay2/layer/diff")
			cmd.Env = append(os.Environ(), "PATH="+filepath.Join(dir, "bin")+":"+os.Getenv("PATH"),
				"FAKE_CAPACITY_SCENARIO="+scenario)
			output, err := cmd.CombinedOutput()
			if scenario == successValue || scenario == featureOwnedMerged {
				if err != nil || !strings.Contains(string(output), `"nodeStartTime":"900"`) {
					t.Fatalf("capacity: %v %s", err, output)
				}
			} else if err == nil {
				t.Fatalf("accepted %s: %s", scenario, output)
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
		{"unknown profile", unknownValue, "", true},
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
