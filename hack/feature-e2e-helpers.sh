#!/usr/bin/env bash
set -euo pipefail
schema_profile=${FEATURE_E2E_SCHEMA_VALIDATION:-identical}
case "$schema_profile" in
  identical | additive-disposable) ;;
  *) echo "::error::schema validation profile is invalid"; exit 1 ;;
esac
FEATURE_E2E_HELPERS=$RUNNER_TEMP/feature-e2e
REAL_HELM=$(command -v helm)
FEATURE_E2E_OWNER_KEY=pgcopydb-operator.io/feature-e2e-run
FEATURE_E2E_OWNER_FILE=$FEATURE_E2E_HELPERS/owner
install -d -m 0755 "$FEATURE_E2E_HELPERS/bin" \
  "$FEATURE_E2E_HELPERS/plugins/feature-e2e-postrenderer"
umask 077
printf '%s' "$schema_profile" > "$FEATURE_E2E_HELPERS/schema-profile"
od -An -N16 -tx1 /dev/urandom | tr -d ' \n' > "$FEATURE_E2E_OWNER_FILE"
[[ "$(<"$FEATURE_E2E_OWNER_FILE")" =~ ^[0-9a-f]{32}$ ]]
[ "$(stat -c '%a' "$FEATURE_E2E_OWNER_FILE")" = 600 ]

cat > "$FEATURE_E2E_HELPERS/attest-image" <<'EOF_ATTEST'
#!/usr/bin/env bash
set +x
set -euo pipefail
[ "$#" -eq 1 ] || {
  echo "::error::attestation component is invalid"
  exit 1
}
component=$1
owner_file=${FEATURE_E2E_OWNER_FILE:-}
[ -n "$owner_file" ] || {
  echo "::error::attestation ownership is invalid"
  exit 1
}
owner_value=$(<"$owner_file") 2>/dev/null || {
  echo "::error::attestation ownership is invalid"
  exit 1
}
[ "${FEATURE_E2E_OWNER_KEY:-}" = pgcopydb-operator.io/feature-e2e-run ] &&
  [[ "$owner_value" =~ ^[0-9a-f]{32}$ ]] || {
    echo "::error::attestation ownership is invalid"
    exit 1
  }
owner_selector="pgcopydb-operator.io/feature-e2e-run=$owner_value"
case "$component" in
  manager)
    namespace=$E2E_OPERATOR_NAMESPACE
    container=manager
    expected_ref=${MANAGER_REF:-}
    [[ "$expected_ref" =~ ^ghcr\.io/ydixken/pgcopydb-operator:feature-[0-9a-f]{40}@sha256:[0-9a-f]{64}$ ]] || {
      echo "::error::attestation reference is invalid"
      exit 1
    }
    expected_runner_ref=${RUNNER_REF:-}
    [[ "$expected_runner_ref" =~ ^ghcr\.io/ydixken/pgcopydb-operator/runner:feature-[0-9a-f]{40}@sha256:[0-9a-f]{64}$ ]] || {
      echo "::error::attestation reference is invalid"
      exit 1
    }
    manager_selector="$owner_selector,app.kubernetes.io/instance=pgcopydb-e2e,app.kubernetes.io/name=pgcopydb-operator"
    resources=$(kubectl get deployments,replicasets,pods -n "$namespace" \
      -l "$manager_selector" -o json 2>/dev/null) || {
        echo "::error::unable to inspect the manager ownership chain"
        exit 1
      }
    pods=$(jq -cer \
      --arg namespace "$namespace" \
      --arg owner_key "$FEATURE_E2E_OWNER_KEY" \
      --arg owner_value "$owner_value" \
      --arg manager_ref "$expected_ref" \
      --arg runner_arg "--runner-image=$expected_runner_ref" '
      def valid_uid:
        type == "string" and
        test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
      def generated_from($name):
        if type != "string" then false
        else startswith($name + "-") and length > (($name | length) + 1)
        end;
      def valid_runner_args($runner_arg):
        if type != "array" then false
        else
          all(.[]; type == "string") and
          ([.[] | select(type == "string") |
            select(. == "--runner-image" or startswith("--runner-image="))] |
            length == 1) and
          ([.[] | select(. == $runner_arg)] | length == 1)
        end;
      def valid_manager($manager_ref; $runner_arg):
        if type != "array" then false
        else
          [.[] | select(type == "object" and .name == "manager")] as $managers |
          ($managers | length) == 1 and
          $managers[0].image == $manager_ref and
          ($managers[0].args | valid_runner_args($runner_arg))
        end;
      def controlled_by($api_version; $kind; $name; $uid):
        (.metadata.ownerReferences | type == "array") and
        ([.metadata.ownerReferences[] |
          select(.controller == true)] | length == 1) and
        ([.metadata.ownerReferences[] |
          select(.controller == true and .apiVersion == $api_version and
            .kind == $kind and .name == $name and .uid == $uid)] | length == 1);
      if type == "object" and .apiVersion == "v1" and
         .kind == "List" and (.items | type) == "array"
      then .items else error("invalid resource list") end |
      if all(.[];
        type == "object" and
        (.kind == "Deployment" or .kind == "ReplicaSet" or .kind == "Pod") and
        (.metadata | type == "object") and
        .metadata.namespace == $namespace and
        (.metadata.labels | type == "object") and
        .metadata.labels[$owner_key] == $owner_value)
      then . else error("invalid run-owned resource") end |
      [ .[] | select(.kind == "Deployment") ] as $deployments |
      [ .[] | select(.kind == "ReplicaSet") ] as $replica_sets |
      [ .[] | select(.kind == "Pod") ] as $pods |
      if ($deployments | length) == 1 and
         ($replica_sets | length) == 1 and
         ($pods | length) == 1
      then
        $deployments[0] as $deployment |
        $replica_sets[0] as $replica_set |
        $pods[0] as $pod |
        if $deployment.apiVersion == "apps/v1" and
           $deployment.metadata.name == "pgcopydb-e2e" and
           ($deployment.metadata.uid | valid_uid) and
           $deployment.metadata.labels["app.kubernetes.io/instance"] == "pgcopydb-e2e" and
           $deployment.metadata.labels["app.kubernetes.io/name"] == "pgcopydb-operator" and
           $replica_set.apiVersion == "apps/v1" and
           ($replica_set.metadata.name |
             generated_from($deployment.metadata.name)) and
           ($replica_set.metadata.uid | valid_uid) and
           $deployment.metadata.uid != $replica_set.metadata.uid and
           $replica_set.metadata.labels["app.kubernetes.io/instance"] == "pgcopydb-e2e" and
           $replica_set.metadata.labels["app.kubernetes.io/name"] == "pgcopydb-operator" and
           ($replica_set | controlled_by("apps/v1"; "Deployment";
             $deployment.metadata.name; $deployment.metadata.uid)) and
           $pod.apiVersion == "v1" and
           ($pod.metadata.name | generated_from($replica_set.metadata.name)) and
           ($pod.metadata.uid | valid_uid) and
           $deployment.metadata.uid != $pod.metadata.uid and
           $replica_set.metadata.uid != $pod.metadata.uid and
           $pod.metadata.labels["app.kubernetes.io/instance"] == "pgcopydb-e2e" and
           $pod.metadata.labels["app.kubernetes.io/name"] == "pgcopydb-operator" and
           ($pod | controlled_by("apps/v1"; "ReplicaSet";
             $replica_set.metadata.name; $replica_set.metadata.uid)) and
           ($pod.status.conditions | type == "array") and
           ([$pod.status.conditions[] |
             select(.type == "Ready")] | length == 1) and
           ([$pod.status.conditions[] |
             select(.type == "Ready" and .status == "True")] | length == 1) and
           ($deployment.spec.template.spec.containers |
             valid_manager($manager_ref; $runner_arg)) and
           ($replica_set.spec.template.spec.containers |
             valid_manager($manager_ref; $runner_arg)) and
           ($pod.spec.containers |
             valid_manager($manager_ref; $runner_arg))
        then {apiVersion:"v1", kind:"PodList", items:[$pod]}
        else error("invalid manager ownership chain") end
      else error("manager ownership chain is not unique") end
    ' <<<"$resources" 2>/dev/null) || {
      echo "::error::manager ownership chain is invalid"
      exit 1
    }
    ;;
  runner)
    namespace=pgcopydb-e2e
    selector="job-name=feature-e2e-runner-attest,$owner_selector"
    container=runner
    expected_ref=${RUNNER_REF:-}
    [[ "$expected_ref" =~ ^ghcr\.io/ydixken/pgcopydb-operator/runner:feature-[0-9a-f]{40}@sha256:[0-9a-f]{64}$ ]] || {
      echo "::error::attestation reference is invalid"
      exit 1
    }
    pods=$(kubectl get pods -n "$namespace" -l "$selector" -o json 2>/dev/null) || {
      echo "::error::unable to inspect runtime Pods"
      exit 1
    }
    ;;
  *)
    echo "::error::attestation component is invalid"
    exit 1
    ;;
esac
expected_image=${expected_ref%@*}
expected_image=${expected_image%:*}
top_digest=${expected_ref##*@}
count=$(jq -er '
  .items | if type == "array" then length else error("invalid Pod list") end
' <<<"$pods" 2>/dev/null) || {
  echo "::error::runtime Pod data is invalid"
  exit 1
}
[ "$count" -eq 1 ] || {
  echo "::error::expected exactly one controller or canary Pod"
  exit 1
}
jq -e --arg container "$container" --arg image "$expected_ref" '
  (.items[0].spec.containers | type == "array") and
  ([.items[0].spec.containers[] |
    select(.name == $container)] | length == 1) and
  ([.items[0].spec.containers[] |
    select(.name == $container and .image == $image)] | length == 1)
' <<<"$pods" >/dev/null 2>&1 || {
  echo "::error::runtime Pod data is invalid"
  exit 1
}
image_id=$(jq -er --arg container "$container" '
  .items[0].status.containerStatuses |
  if type == "array" then . else error("invalid container statuses") end |
  [.[] |
    select(.name == $container)] |
  if length == 1 and ((.[0].imageID | type) == "string") then
    .[0].imageID
  else
    error("invalid container status")
  end
' <<<"$pods" 2>/dev/null) || {
  echo "::error::runtime imageID is missing or malformed"
  exit 1
}
runtime_ref=${image_id#docker-pullable://}
runtime_image=${runtime_ref%@*}
actual_digest=${runtime_ref##*@}
[ "$runtime_ref" = "$runtime_image@$actual_digest" ] &&
  [ "$runtime_image" = "$expected_image" ] &&
  [[ "$actual_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "::error::runtime imageID is missing or malformed"
    exit 1
  }
raw=$(docker buildx imagetools inspect "$expected_ref" --raw 2>/dev/null) || {
  echo "::error::unable to inspect the requested manifest"
  exit 1
}
jq -e --arg actual "$actual_digest" --arg top "$top_digest" '
  def allowed_manifest_media_type:
    . == "application/vnd.oci.image.manifest.v1+json" or
    . == "application/vnd.docker.distribution.manifest.v2+json";
  def optional_string($key):
    (has($key) | not) or ((.[$key] | type) == "string");
  def optional_string_array($key):
    (has($key) | not) or
    (((.[$key] | type) == "array") and
     all(.[$key][]; type == "string"));
  def valid_annotations:
    (has("annotations") | not) or
    (((.annotations | type) == "object") and
     all(.annotations | to_entries[];
       ((.key | type) == "string") and
       ((.value | type) == "string")));
  def valid_descriptor:
    type == "object" and
    (.mediaType | allowed_manifest_media_type) and
    ((.digest | type) == "string") and
    (.digest | test("^sha256:[0-9a-f]{64}$")) and
    ((.size | type) == "number") and .size > 0 and
    ((.size | floor) == .size) and
    valid_annotations and
    optional_string_array("urls") and
    optional_string("data") and
    optional_string("artifactType") and
    ((.platform | type) == "object") and
    ((.platform.os | type) == "string") and
    (.platform.os | length) > 0 and
    ((.platform.architecture | type) == "string") and
    (.platform.architecture | length) > 0 and
    (.platform | optional_string("variant")) and
    (.platform | optional_string("os.version")) and
    (.platform | optional_string_array("os.features")) and
    (.platform | optional_string_array("features"));
  def attestation_marker:
    ((.annotations? | type) == "object") and
    .annotations["vnd.docker.reference.type"] == "attestation-manifest";
  def runtime_platform:
    .platform.os == "linux" and
    (.platform.architecture == "amd64" or
     .platform.architecture == "arm64");
  def valid_runtime_descriptor:
    valid_descriptor and runtime_platform and (attestation_marker | not);
  def valid_attestation_descriptor($runtime_digests):
    valid_descriptor and
    .platform.os == "unknown" and
    .platform.architecture == "unknown" and
    attestation_marker and
    (.annotations["vnd.docker.reference.digest"] as $reference |
      (($reference | type) == "string") and
      ($reference | test("^sha256:[0-9a-f]{64}$")) and
      (($runtime_digests | index($reference)) != null));
  ([.manifests[]? | select(valid_runtime_descriptor) |
    .digest]) as $runtime_digests |
  (type == "object" and .schemaVersion == 2) and
  (.mediaType == "application/vnd.oci.image.index.v1+json" or
   .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json") and
  (.manifests | type == "array" and length > 0) and
  all(.manifests[];
    valid_runtime_descriptor or valid_attestation_descriptor($runtime_digests)) and
  ([.manifests[] | select(valid_runtime_descriptor and
    .platform.architecture == "amd64")]
    | length == 1) and
  ([.manifests[] | select(valid_runtime_descriptor and
    .platform.architecture == "arm64")]
    | length == 1) and
  ([.manifests[].digest] | length == (unique | length)) and
  ($actual == $top or ($runtime_digests | index($actual)) != null)
' <<<"$raw" >/dev/null 2>&1 || {
  echo "::error::runtime imageID does not belong to the requested manifest"
  exit 1
}
EOF_ATTEST

cat > "$FEATURE_E2E_HELPERS/render-safety.go" <<'EOF_RENDER_SAFETY'
package main

import (
  "fmt"
  "io"
  "os"
  "strings"

  k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

const ownerKey = "pgcopydb-operator.io/feature-e2e-run"

func main() {
  namespaceInput := len(os.Args) == 3 && os.Args[2] == "namespace-input"
  if (len(os.Args) != 2 && !namespaceInput) || os.Getenv("FEATURE_E2E_OWNER_KEY") != ownerKey {
    fail()
  }
  profile, err := os.ReadFile(os.Getenv("FEATURE_E2E_HELPERS") + "/schema-profile")
  expected := os.Getenv("FEATURE_E2E_SCHEMA_VALIDATION")
  if expected == "" {
    expected = "identical"
  }
  isolated := string(profile) == "additive-disposable"
  if err != nil || string(profile) != expected || (!isolated && string(profile) != "identical") || (namespaceInput && !isolated) {
    fail()
  }
  owner, err := os.ReadFile(os.Getenv("FEATURE_E2E_OWNER_FILE"))
  if err != nil || !validOwner(string(owner)) {
    fail()
  }
  input, err := os.Open(os.Args[1])
  if err != nil {
    fail()
  }
  defer input.Close()
  decoder := k8syaml.NewYAMLOrJSONDecoder(input, 4096)
  resources := 0
  rules := 0
  for {
    var document map[string]any
    err := decoder.Decode(&document)
    if err == io.EOF {
      break
    }
    if err != nil || len(document) == 0 {
      fail()
    }
    count, ok := validateObject(document, string(owner), isolated, namespaceInput, &rules)
    if !ok {
      fail()
    }
    resources += count
  }
  if resources == 0 || (isolated && rules != 1) || (!isolated && rules != 0) {
    fail()
  }
}

func fail() {
  fmt.Fprintln(os.Stderr, "::error::feature chart render is unsafe")
  os.Exit(1)
}

func validOwner(value string) bool {
  if len(value) != 32 {
    return false
  }
  for _, character := range value {
    if !strings.ContainsRune("0123456789abcdef", character) {
      return false
    }
  }
  return true
}

func validateObject(object map[string]any, owner string, isolated, namespaceInput bool, rules *int) (int, bool) {
  apiVersion, apiOK := object["apiVersion"].(string)
  kind, kindOK := object["kind"].(string)
  if !apiOK || apiVersion == "" || !kindOK || kind == "" || kind == "Namespace" {
    return 0, false
  }
  if kind == "List" {
    if apiVersion != "v1" {
      return 0, false
    }
    items, ok := object["items"].([]any)
    if !ok || len(items) == 0 {
      return 0, false
    }
    count := 0
    for _, item := range items {
      member, ok := item.(map[string]any)
      if !ok {
        return 0, false
      }
      nested, ok := validateObject(member, owner, isolated, namespaceInput, rules)
      if !ok {
        return 0, false
      }
      count += nested
    }
    return count, true
  }
  if strings.HasSuffix(kind, "List") || kind == "PodTemplate" {
    return 0, false
  }
  metadata, ok := object["metadata"].(map[string]any)
  if !ok || (!namespaceInput && !hasLabel(metadata, owner)) {
    return 0, false
  }
  if kind == "PrometheusRule" {
    namespace, present := metadata["namespace"]
    labels, labelled := metadata["labels"].(map[string]any)
    if !isolated || apiVersion != "monitoring.coreos.com/v1" || metadata["name"] != "pgcopydb-e2e" ||
      (present && namespace != "pgcopydb-e2e") || (!present && !namespaceInput) ||
      !labelled || labels["app.kubernetes.io/instance"] != "pgcopydb-e2e" {
      return 0, false
    }
    *rules++
  }
  if namespaceInput {
    return 1, true
  }
  paths, ok := templatePaths(apiVersion, kind, object)
  if !ok {
    return 0, false
  }
  for _, path := range paths {
    template, ok := nestedMap(object, path...)
    if !ok {
      return 0, false
    }
    metadata, ok := template["metadata"].(map[string]any)
    if !ok || !hasLabel(metadata, owner) {
      return 0, false
    }
  }
  return 1, true
}

func templatePaths(apiVersion, kind string, object map[string]any) ([][]string, bool) {
  switch {
  case apiVersion == "apps/v1" &&
    (kind == "Deployment" || kind == "ReplicaSet" ||
      kind == "DaemonSet" || kind == "StatefulSet"):
    return [][]string{{"spec", "template"}}, true
  case apiVersion == "batch/v1" && kind == "Job":
    return [][]string{{"spec", "template"}}, true
  case apiVersion == "batch/v1" && kind == "CronJob":
    return [][]string{{"spec", "jobTemplate"},
      {"spec", "jobTemplate", "spec", "template"}}, true
  case apiVersion == "v1" && kind == "ReplicationController":
    return [][]string{{"spec", "template"}}, true
  }
  if _, ok := nestedMap(object, "spec", "template"); ok {
    return nil, false
  }
  if _, ok := nestedMap(object, "spec", "jobTemplate"); ok {
    return nil, false
  }
  return nil, true
}

func nestedMap(object map[string]any, path ...string) (map[string]any, bool) {
  current := object
  for _, name := range path {
    next, ok := current[name].(map[string]any)
    if !ok {
      return nil, false
    }
    current = next
  }
  return current, true
}

func hasLabel(metadata map[string]any, owner string) bool {
  labels, ok := metadata["labels"].(map[string]any)
  if !ok {
    return false
  }
  value, ok := labels[ownerKey].(string)
  return ok && value == owner
}
EOF_RENDER_SAFETY

go build -o "$FEATURE_E2E_HELPERS/bin/render-safety" \
  "$FEATURE_E2E_HELPERS/render-safety.go"

cat > "$FEATURE_E2E_HELPERS/post-renderer" <<'EOF_POST_RENDERER'
#!/usr/bin/env bash
set +x
set -euo pipefail
owner_value=$(<"$FEATURE_E2E_OWNER_FILE")
[ "${FEATURE_E2E_OWNER_KEY:-}" = pgcopydb-operator.io/feature-e2e-run ] &&
  [[ "$owner_value" =~ ^[0-9a-f]{32}$ ]] || {
    echo "::error::feature ownership is invalid"
    exit 1
  }
render_dir=$FEATURE_E2E_HELPERS/post-render
input=$render_dir/manifest.yaml
output=$FEATURE_E2E_HELPERS/rendered.yaml
install -d -m 0700 "$render_dir"
: > "$output"
cat > "$input"
[ -s "$input" ] || {
  echo "::error::feature chart rendered no objects"
  exit 1
}
schema_profile=$(<"$FEATURE_E2E_HELPERS/schema-profile")
if [ "$schema_profile" = additive-disposable ]; then
  "$FEATURE_E2E_HELPERS/bin/render-safety" "$input" namespace-input || exit 1
fi
{
  printf '%s\n' 'apiVersion: kustomize.config.k8s.io/v1beta1' 'kind: Kustomization'
  if [ "$schema_profile" = additive-disposable ]; then
    printf '%s\n' 'namespace: pgcopydb-e2e'
  fi
  printf '%s\n' 'resources:' '- manifest.yaml' 'labels:' '- pairs:'
  printf '    %s: %s\n' "$FEATURE_E2E_OWNER_KEY" "$owner_value"
  printf '%s\n' '  includeSelectors: false' '  includeTemplates: true'
} > "$render_dir/kustomization.yaml"
kubectl kustomize "$render_dir" > "$output.tmp" 2>/dev/null || {
  echo "::error::feature chart render cannot be labelled"
  exit 1
}
[ -s "$output.tmp" ] || {
  echo "::error::feature chart rendered no labelled objects"
  exit 1
}
"$FEATURE_E2E_HELPERS/bin/render-safety" "$output.tmp" || exit 1
mv "$output.tmp" "$output"
cat "$output"
EOF_POST_RENDERER

cat > "$FEATURE_E2E_HELPERS/plugins/feature-e2e-postrenderer/plugin.yaml" <<'EOF_HELM_PLUGIN'
name: feature-e2e-postrenderer
version: "1.0.0"
type: postrenderer/v1
apiVersion: v1
runtime: subprocess
runtimeConfig:
  platformCommand:
    - command: "${HELM_PLUGIN_DIR}/post-renderer"
EOF_HELM_PLUGIN
ln -s ../../post-renderer "$FEATURE_E2E_HELPERS/plugins/feature-e2e-postrenderer/post-renderer"

cat > "$FEATURE_E2E_HELPERS/ownership" <<'EOF_OWNERSHIP'
feature_owner_value() {
  FEATURE_OWNER_VALUE=$(<"$FEATURE_E2E_OWNER_FILE")
  [ "${FEATURE_E2E_OWNER_KEY:-}" = pgcopydb-operator.io/feature-e2e-run ] &&
    [[ "$FEATURE_OWNER_VALUE" =~ ^[0-9a-f]{32}$ ]] || {
      echo "::error::feature ownership is invalid"
      return 1
    }
  FEATURE_OWNER_SELECTOR="$FEATURE_E2E_OWNER_KEY=$FEATURE_OWNER_VALUE"
}

feature_controller_list() {
  kubectl get deployments -n "$E2E_OPERATOR_NAMESPACE" \
    -l "$FEATURE_OWNER_SELECTOR" -o json 2>/dev/null || {
      echo "::error::unable to inspect the feature controller"
      return 1
    }
}

feature_valid_controller() {
  local document=$1
  local expected_uid=${2:-}
  jq -er \
    --arg owner_key "$FEATURE_E2E_OWNER_KEY" \
    --arg owner_value "$FEATURE_OWNER_VALUE" \
    --arg expected_uid "$expected_uid" \
    --arg namespace "$E2E_OPERATOR_NAMESPACE" '
    select((type == "object" and .apiVersion == "v1" and .kind == "List") and
    (.items | type == "array" and length == 1) and
    (.items[0] | type == "object" and .apiVersion == "apps/v1" and
      .kind == "Deployment") and
    (.items[0].metadata | type == "object") and
    .items[0].metadata.name == "pgcopydb-e2e" and
    .items[0].metadata.namespace == $namespace and
    (.items[0].metadata.uid | type == "string" and
      test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")) and
    ($expected_uid == "" or .items[0].metadata.uid == $expected_uid) and
    (.items[0].metadata.labels | type == "object") and
    .items[0].metadata.labels[$owner_key] == $owner_value and
    (.items[0].metadata.annotations | type == "object") and
    .items[0].metadata.annotations["meta.helm.sh/release-name"] == "pgcopydb-e2e" and
    .items[0].metadata.annotations["meta.helm.sh/release-namespace"] ==
      $namespace) |
    .items[0].metadata.uid
  ' <<<"$document" 2>/dev/null
}

feature_capture_controller() {
  local controllers uid
  feature_owner_value
  controllers=$(feature_controller_list)
  uid=$(feature_valid_controller "$controllers") || {
    echo "::error::feature controller ownership is invalid"
    return 1
  }
  umask 077
  printf '%s' "$uid" > "$FEATURE_E2E_HELPERS/controller.uid"
}

feature_revalidate_controller() {
  local captured controllers named
  feature_owner_value
  [ -s "$FEATURE_E2E_HELPERS/controller.uid" ] || {
    echo "::error::feature controller was never captured"
    return 1
  }
  captured=$(<"$FEATURE_E2E_HELPERS/controller.uid")
  [[ "$captured" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] || {
    echo "::error::captured feature controller UID is invalid"
    return 1
  }
  controllers=$(feature_controller_list)
  if jq -e '.items | type == "array" and length == 0' \
    <<<"$controllers" >/dev/null 2>&1; then
    named=$(kubectl get deployment.apps/pgcopydb-e2e -n "$E2E_OPERATOR_NAMESPACE" \
      --ignore-not-found -o json 2>/dev/null) || {
        echo "::error::unable to inspect the feature controller"
        return 1
      }
    [ -z "$named" ] && return 3
    echo "::error::feature controller ownership changed"
    return 1
  fi
  feature_valid_controller "$controllers" "$captured" >/dev/null || {
    echo "::error::feature controller ownership changed"
    return 1
  }
}

feature_uninstall_controller() {
  local validation=0
  feature_revalidate_controller || validation=$?
  [ "$validation" -eq 0 ] || [ "$validation" -eq 3 ] || return 1
  "$REAL_HELM" uninstall pgcopydb-e2e -n "$E2E_OPERATOR_NAMESPACE" \
    --cascade foreground --wait --timeout=5m >/dev/null
}
EOF_OWNERSHIP

cat > "$FEATURE_E2E_HELPERS/cleanup" <<'EOF_CLEANUP'
#!/usr/bin/env bash
set +x
set -euo pipefail
mode=${1:-normal}
[ "$mode" = normal ] || [ "$mode" = recovery ] || {
  echo "::error::feature cleanup mode is invalid"
  exit 1
}
. "$FEATURE_E2E_HELPERS/ownership"
feature_owner_value
owner_selector=$FEATURE_OWNER_SELECTOR
expected_dir=$FEATURE_E2E_HELPERS/expected
install -d -m 0700 "$expected_dir"

capture_owned() {
  local file=$1
  local api_version=$2
  local kind=$3
  local resource=$4
  local namespace=$5
  local document
  document=$(kubectl get "$resource" -n "$namespace" \
    -l "$owner_selector" -o json 2>/dev/null) || {
      echo "::error::unable to capture run-owned $kind resources"
      return 1
    }
  jq -e \
    --arg api_version "$api_version" --arg kind "$kind" \
    --arg namespace "$namespace" --arg owner_key "$FEATURE_E2E_OWNER_KEY" \
    --arg owner_value "$FEATURE_OWNER_VALUE" '
    type == "object" and .apiVersion == "v1" and .kind == "List" and
    (.items | type == "array") and
    all(.items[];
      type == "object" and .apiVersion == $api_version and .kind == $kind and
      (.metadata | type == "object") and
      (.metadata.name | type == "string" and
        test("^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")) and
      .metadata.namespace == $namespace and
      (.metadata.uid | type == "string" and
        test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")) and
      (.metadata.labels | type == "object") and
      .metadata.labels[$owner_key] == $owner_value)
  ' <<<"$document" >/dev/null 2>&1 || {
    echo "::error::run-owned $kind capture is invalid"
    return 1
  }
  printf '%s' "$document" > "$file"
}
cleanup_owned() {
  local file=$1
  local api_version=$2
  local kind=$3
  local resource=$4
  local namespace=$5
  local item name uid current after after_uid delete_path delete_options propagation deadline
  propagation=Background
  [ "$kind" != Job ] || propagation=Foreground
  while IFS= read -r item; do
    name=$(jq -er '.metadata.name' <<<"$item")
    uid=$(jq -er '.metadata.uid' <<<"$item")
    current=$(kubectl get "$resource/$name" -n "$namespace" \
      --ignore-not-found -o json 2>/dev/null) || {
        echo "::error::unable to revalidate run-owned $kind"
        return 1
      }
    [ -n "$current" ] || continue
    jq -e \
      --arg api_version "$api_version" --arg kind "$kind" --arg name "$name" \
      --arg namespace "$namespace" --arg uid "$uid" \
      --arg owner_key "$FEATURE_E2E_OWNER_KEY" \
      --arg owner_value "$FEATURE_OWNER_VALUE" '
      type == "object" and .apiVersion == $api_version and .kind == $kind and
      (.metadata | type == "object") and .metadata.name == $name and
      .metadata.namespace == $namespace and .metadata.uid == $uid and
      (.metadata.labels | type == "object") and
      .metadata.labels[$owner_key] == $owner_value
    ' <<<"$current" >/dev/null 2>&1 || {
      echo "::error::run-owned $kind ownership changed"
      return 1
    }
    case "$resource" in
      migrations.pgcopydb-operator.io)
        delete_path="/apis/pgcopydb-operator.io/v1beta1/namespaces/$namespace/migrations/$name"
        ;;
      jobs)
        delete_path="/apis/batch/v1/namespaces/$namespace/jobs/$name"
        ;;
      *)
        echo "::error::unsupported run-owned cleanup resource"
        return 1
        ;;
    esac
    delete_options=$(jq -cn --arg uid "$uid" --arg propagation "$propagation" '
      {apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid},
        propagationPolicy:$propagation}
    ')
    if ! kubectl delete --raw "$delete_path" -f - \
      <<<"$delete_options" >/dev/null 2>&1; then
      after=$(kubectl get "$resource/$name" -n "$namespace" \
        --ignore-not-found -o json 2>/dev/null) || {
          echo "::error::unable to confirm failed run-owned $kind deletion"
          return 1
        }
      [ -z "$after" ] && continue
      echo "::error::run-owned $kind UID-preconditioned deletion failed"
      return 1
    fi
    deadline=$((SECONDS + 300))
    while :; do
      after=$(kubectl get "$resource/$name" -n "$namespace" \
        --ignore-not-found -o json 2>/dev/null) || {
          echo "::error::unable to confirm run-owned $kind deletion"
          return 1
        }
      [ -n "$after" ] || break
      after_uid=$(jq -er '.metadata.uid' <<<"$after") || {
        echo "::error::run-owned $kind deletion state is invalid"
        return 1
      }
      [ "$after_uid" = "$uid" ] || {
        echo "::error::run-owned $kind was replaced during deletion"
        return 1
      }
      [ "$SECONDS" -lt "$deadline" ] || {
        echo "::error::run-owned $kind deletion timed out"
        return 1
      }
      sleep 1
    done
  done < <(jq -c '.items[]' "$file")
}
require_empty() {
  local description=$1
  shift
  local deadline result
  deadline=$((SECONDS + 300))
  while :; do
    result=$("$@" -l "$owner_selector" -o name)
    [ -n "$result" ] || return 0
    [ "$SECONDS" -lt "$deadline" ] || {
      echo "::error::run-owned $description remain after cleanup"
      return 1
    }
    sleep 1
  done
}

for namespace in pgcopydb-e2e pgcopydb-e2e-x; do
  expected="$expected_dir/migrations-$namespace"
  capture_owned "$expected" pgcopydb-operator.io/v1beta1 Migration \
    migrations.pgcopydb-operator.io "$namespace"
  cleanup_owned "$expected" pgcopydb-operator.io/v1beta1 Migration \
    migrations.pgcopydb-operator.io "$namespace"
done
for namespace in pgcopydb-e2e pgcopydb-e2e-x; do
  expected="$expected_dir/jobs-$namespace"
  capture_owned "$expected" batch/v1 Job jobs "$namespace"
  cleanup_owned "$expected" batch/v1 Job jobs "$namespace"
done

if [ "$mode" = recovery ] && [ ! -s "$FEATURE_E2E_HELPERS/controller.uid" ]; then
  controllers=$(feature_controller_list)
  if ! jq -e '.items | type == "array" and length == 0' \
    <<<"$controllers" >/dev/null 2>&1; then
    feature_capture_controller
  else
    named=$(kubectl get deployment.apps/pgcopydb-e2e -n "$E2E_OPERATOR_NAMESPACE" \
      --ignore-not-found -o json 2>/dev/null) || {
        echo "::error::unable to inspect the feature controller"
        exit 1
      }
    [ -z "$named" ] || {
      echo "::error::feature controller ownership is invalid"
      exit 1
    }
  fi
fi
if [ -s "$FEATURE_E2E_HELPERS/controller.uid" ]; then
  feature_uninstall_controller
elif [ "$mode" = normal ]; then
  echo "::error::feature controller was never captured"
  exit 1
elif [ -s "$FEATURE_E2E_HELPERS/rendered.yaml" ]; then
  kubectl delete -f "$FEATURE_E2E_HELPERS/rendered.yaml" \
    -l "$owner_selector" --ignore-not-found --cascade=foreground \
    --wait=true --timeout=5m >/dev/null
  "$REAL_HELM" uninstall pgcopydb-e2e -n "$E2E_OPERATOR_NAMESPACE" \
    --cascade foreground --wait --timeout=5m --ignore-not-found >/dev/null
else
  [ ! -e "$FEATURE_E2E_HELPERS/rendered.yaml" ] || {
    echo "::error::labelled feature render is invalid for recovery"
    exit 1
  }
fi

for namespace in pgcopydb-e2e pgcopydb-e2e-x; do
  require_empty "Migrations in $namespace" \
    kubectl get migrations.pgcopydb-operator.io -n "$namespace"
  require_empty "worker or cleanup Jobs in $namespace" \
    kubectl get jobs -n "$namespace"
done
require_empty "feature controller Deployments" \
  kubectl get deployments -n "$E2E_OPERATOR_NAMESPACE"
require_empty "feature controller Pods" \
  kubectl get pods -n "$E2E_OPERATOR_NAMESPACE"
if [ -s "$FEATURE_E2E_HELPERS/rendered.yaml" ]; then
  remaining=$(kubectl get -f "$FEATURE_E2E_HELPERS/rendered.yaml" \
    -l "$owner_selector" --ignore-not-found -o name 2>/dev/null) || {
      echo "::error::unable to inspect rendered feature objects"
      exit 1
    }
  [ -z "$remaining" ] || {
    echo "::error::run-owned rendered objects remain after cleanup"
    exit 1
  }
fi
remaining=$("$REAL_HELM" list -n "$E2E_OPERATOR_NAMESPACE" -q \
  --filter '^pgcopydb-e2e$')
[ -z "$remaining" ] || {
  echo "::error::feature Helm release remains after cleanup"
  exit 1
}
EOF_CLEANUP

cat > "$FEATURE_E2E_HELPERS/bin/helm" <<'EOF_HELM'
#!/usr/bin/env bash
set +x
set -euo pipefail
helm_args=("$@")
if [ "${1:-}" = install ] && [ "${2:-}" = pgcopydb-e2e ]; then
  [ "$#" -ge 6 ] &&
    [ "$3" = ../../charts/pgcopydb-operator ] &&
    [ "$4" = -n ] && [ "$5" = "$E2E_OPERATOR_NAMESPACE" ] &&
    [ "$6" = --wait ] || {
      echo "::error::feature controller Helm argument is unsupported"
      exit 1
    }
  [ -s "$FEATURE_E2E_HELPERS/schema-profile" ] || {
    echo "::error::feature schema profile is missing"
    exit 1
  }
  schema_profile=$(<"$FEATURE_E2E_HELPERS/schema-profile")
  [ "$schema_profile" = "${FEATURE_E2E_SCHEMA_VALIDATION:-identical}" ] || {
    echo "::error::feature schema profile changed"
    exit 1
  }
  case "$schema_profile" in
    identical | additive-disposable) ;;
    *) echo "::error::feature schema profile is invalid"; exit 1 ;;
  esac
  candidate_args=("${@:7}")
  while [ "${#candidate_args[@]}" -gt 0 ]; do
    arg=${candidate_args[0]}
    case "$arg" in
      --create-namespace)
        [ "$schema_profile" = additive-disposable ] || {
          echo "::error::feature namespace creation is not permitted"
          exit 1
        }
        candidate_args=("${candidate_args[@]:1}")
        ;;
      -f|--values|--set|--set-string|--set-json)
        [ "${#candidate_args[@]}" -ge 2 ] || {
          echo "::error::feature controller Helm argument is unsupported"
          exit 1
        }
        value=${candidate_args[1]}
        [ -n "$value" ] && [[ "$value" != -* ]] || {
          echo "::error::feature controller Helm argument is unsupported"
          exit 1
        }
        candidate_args=("${candidate_args[@]:2}")
        ;;
      -f=*|--values=*|--set=*|--set-string=*|--set-json=*)
        value=${arg#*=}
        [ -n "$value" ] || {
          echo "::error::feature controller Helm argument is unsupported"
          exit 1
        }
        candidate_args=("${candidate_args[@]:1}")
        ;;
      *)
        echo "::error::feature controller Helm argument is unsupported"
        exit 1
        ;;
    esac
    case "$arg" in
      --set|--set-string|--set-json|--set=*|--set-string=*|--set-json=*)
        [[ "$value" == ?*=* ]] || {
          echo "::error::feature controller Helm argument is unsupported"
          exit 1
        }
        normalized=${value//\\/}
        case "$normalized" in
          *fullnameOverride*)
            echo "::error::feature controller name override is reserved"
            exit 1
            ;;
        esac
        ;;
    esac
  done
  rules_enabled=false
  if [ "$schema_profile" = additive-disposable ]; then
    bash "$GITHUB_WORKSPACE/feature-e2e-trusted/hack/feature-e2e-kind.sh" verify-ready "$FEATURE_E2E_KIND_STATE"
    rules_enabled=true
  fi
  helm_args+=(
    --set-string fullnameOverride=pgcopydb-e2e
    --set "metrics.prometheusRule.enabled=$rules_enabled"
    --rollback-on-failure
    --timeout=5m
    --post-renderer feature-e2e-postrenderer
  )
  export HELM_PLUGINS="$FEATURE_E2E_HELPERS/plugins"
fi
install_result=0
"$REAL_HELM" "${helm_args[@]}" || install_result=$?
if [ "${1:-}" = install ] && [ "${2:-}" = pgcopydb-e2e ]; then
  if [ "$install_result" -eq 0 ]; then
    . "$FEATURE_E2E_HELPERS/ownership" || install_result=$?
  fi
  if [ "$install_result" -eq 0 ]; then
    feature_capture_controller || install_result=$?
  fi
  if [ "$install_result" -eq 0 ]; then
    kubectl rollout status deployment/pgcopydb-e2e -n "$E2E_OPERATOR_NAMESPACE" \
      --timeout=5m >/dev/null || install_result=$?
  fi
  if [ "$install_result" -eq 0 ]; then
    "$FEATURE_E2E_HELPERS/attest-image" manager || install_result=$?
  fi
  if [ "$install_result" -eq 0 ]; then
    : > "$FEATURE_E2E_HELPERS/manager-attested" || install_result=$?
  fi
  if [ "$install_result" -ne 0 ]; then
    recovery_result=0
    "$FEATURE_E2E_HELPERS/cleanup" recovery || recovery_result=$?
    [ "$recovery_result" -eq 0 ] || exit "$recovery_result"
    exit "$install_result"
  fi
fi
exit "$install_result"
EOF_HELM

chmod 0755 "$FEATURE_E2E_HELPERS/attest-image" \
  "$FEATURE_E2E_HELPERS/post-renderer" "$FEATURE_E2E_HELPERS/cleanup" \
  "$FEATURE_E2E_HELPERS/bin/helm"
{
  printf 'REAL_HELM=%s\n' "$REAL_HELM"
  printf 'FEATURE_E2E_HELPERS=%s\n' "$FEATURE_E2E_HELPERS"
  printf 'FEATURE_E2E_OWNER_KEY=%s\n' "$FEATURE_E2E_OWNER_KEY"
  printf 'FEATURE_E2E_OWNER_FILE=%s\n' "$FEATURE_E2E_OWNER_FILE"
} >> "$GITHUB_ENV"
