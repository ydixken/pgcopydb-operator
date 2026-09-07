#!/usr/bin/env bash
# This script runs only from the trusted checkout, with a private run directory.
set -Eeuo pipefail
umask 077

fail() { printf '%s\n' "$1" >&2; exit 1; }
[[ $# -ge 2 ]] || fail kind-input-invalid
command=$1
state=$2
case "$command:$#" in create:4|destroy:2|verify-ready:2) ;; *) fail kind-input-invalid ;; esac
[[ "$state" == /* && "$state" != / && -d "$state" && ! -L "$state" &&
   "${GITHUB_RUN_ID:-}" =~ ^[1-9][0-9]{0,14}$ && "${GITHUB_RUN_ATTEMPT:-}" =~ ^[1-9][0-9]{0,5}$ &&
   "${RUNNER_TEMP:-}" == /* && "${FEATURE_E2E_KIND_STATE:-}" == "$state" &&
   "${KUBECONFIG:-}" == "$state/kubeconfig" ]] || fail kind-input-invalid

exec 3>&2
exec 2>/dev/null
fail() { printf '%s\n' "$1" >&3; exit 1; }
trap 'fail kind-operation-failed' ERR
trusted=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
[[ "$(cd -- "$state" && pwd -P)" == "$state" && "$state" == "$(cd -- "$RUNNER_TEMP" && pwd -P)"/* &&
   -O "$state" && "$(stat -c %a "$state")" == 700 ]] || fail kind-state-invalid
for binary in jq docker curl sha256sum timeout kubectl helm go; do command -v "$binary" >/dev/null || fail kind-tool-missing; done
export KIND_EXPERIMENTAL_PROVIDER=docker
cluster="pgcopydb-feature-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
context="kind-$cluster"
owner="$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
node_image=kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed
work=$(mktemp -d "$RUNNER_TEMP/feature-kind-work.XXXXXXXX")
observer=
finish() {
  local result=$?
  trap - EXIT ERR
  if [[ -n "$observer" ]]; then
    cleanup_observer || { printf 'kind-observer-cleanup-failed\n' >&3; result=1; }
  fi
  # The private directory is created above and contains only this invocation's downloads and evidence.
  rm -rf -- "$work" || result=1
  exit "$result"
}
trap finish EXIT
[[ "$(stat -c %d "$work")" == "$(stat -c %d "$state")" ]] || fail kind-state-invalid

save() {
  local update=$1
  shift
  jq "$@" "$update" "$state/state.json" > "$work/state.json" || return 1
  mv -- "$work/state.json" "$state/state.json"
}
binding() {
  jq -c '{runID,runAttempt,cluster,context,kubeconfig,candidateSHA256,nodes}' "$state/state.json"
}
record() {
  save '.evidence[$predicate] = $binding' --arg predicate "$1" --argjson binding "$(binding)"
}
load_state() {
  [[ -f "$state/state.json" && ! -L "$state/state.json" && -O "$state/state.json" &&
     "$(stat -c %a "$state/state.json")" == 600 ]] || fail kind-state-invalid
  local file
  for file in "$KUBECONFIG" "$state/installed-crd.json"; do
    if [[ -e "$file" || -L "$file" ]]; then
      [[ -f "$file" && ! -L "$file" && -O "$file" && "$(stat -c %a "$file")" == 600 ]] || fail kind-state-invalid
    fi
  done
  jq -e --arg cluster "$cluster" --arg context "$context" --arg kubeconfig "$KUBECONFIG" \
    --argjson run "$GITHUB_RUN_ID" --argjson attempt "$GITHUB_RUN_ATTEMPT" '
    .runID == $run and .runAttempt == $attempt and .cluster == $cluster and .context == $context and
    .kubeconfig == $kubeconfig and (.candidateSHA256 | test("^[a-f0-9]{64}$")) and
    (.owned | type == "boolean") and (.nodes | type == "array") and
    (.stage | IN("preflight", "intent", "created", "ready", "destroyed")) and
    (.teardown | IN("pending", "deleting", "complete")) and (.evidence | type == "object")
  ' "$state/state.json" >/dev/null || fail kind-state-invalid
}
kubectl_owned() { timeout --kill-after=10s 330 kubectl --kubeconfig "$KUBECONFIG" --context "$context" "$@"; }
kind_owned() { timeout --kill-after=10s 360 "$work/kind" "$@" --kubeconfig "$KUBECONFIG"; }
download() {
  timeout --kill-after=10s 180 curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
    --connect-timeout 20 --max-time 150 -o "$work/$1" "$2"
  [[ "$(sha256sum "$work/$1" | cut -d ' ' -f 1)" == "$3" ]] || fail kind-checksum-invalid
}
get_kind() {
  [[ "$(uname -s)" == Linux ]] || fail kind-platform-unsupported
  local arch checksum
  case "$(uname -m)" in
    x86_64) arch=amd64; checksum=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d ;;
    aarch64|arm64) arch=arm64; checksum=20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2 ;;
    *) fail kind-platform-unsupported ;;
  esac
  download kind "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-$arch" "$checksum"
  chmod 700 "$work/kind"
}
cluster_nodes() {
  timeout --kill-after=10s 30 docker ps -aq --no-trunc --filter "label=io.x-k8s.kind.cluster=$cluster"
}
absent() {
  local clusters ids listed
  ids=$(cluster_nodes) || return 1
  [[ -z "$ids" ]] || return 1
  # This provider-only command has no kubeconfig flag in kind v0.33.0.
  clusters=$(KUBECONFIG="$KUBECONFIG" timeout --kill-after=10s 30 "$work/kind" get clusters) || return 1
  while IFS= read -r listed; do
    [[ "$listed" != "$cluster" ]] || return 1
  done <<< "$clusters"
}
capture_node() {
  local ids mode=${1:-live}
  ids=$(cluster_nodes)
  [[ "$ids" =~ ^[a-f0-9]{64}$ ]] || fail kind-node-identity-invalid
  timeout --kill-after=10s 30 docker inspect "$ids" > "$work/node.json"
  jq -e --arg id "$ids" --arg cluster "$cluster" --arg image "$node_image" --arg mode "$mode" '
    length == 1 and .[0].Id == $id and .[0].Name == ("/"+$cluster+"-control-plane") and
    .[0].Config.Labels["io.x-k8s.kind.cluster"] == $cluster and
    .[0].Config.Labels["io.x-k8s.kind.role"] == "control-plane" and .[0].Config.Image == $image and
    ($mode=="teardown" or (.[0].State.Running == true and .[0].State.Pid>0)) and
    (.[0].State.Pid | type == "number" and . >= 0) and
    (.[0].State.StartedAt | type == "string" and length > 0)
  ' "$work/node.json" >/dev/null || fail kind-node-identity-invalid
  jq -c '[.[0] | {id:.Id,name:.Name,pid:.State.Pid,startedAt:.State.StartedAt,image:.Config.Image}]' "$work/node.json"
}
verify_node() {
  local current recorded
  current=$(capture_node)
  recorded=$(jq -c '[.nodes[] | del(.startTime,.apiUID)]' "$state/state.json")
  [[ "$current" == "$recorded" ]] || fail kind-node-identity-invalid
}
cleanup_observer() {
  local details id
  details=$(timeout --kill-after=10s 30 docker inspect "$observer") || return 1
  jq -e --arg id "$observer" --arg name "/$cluster-capacity" --arg owner "$owner" --arg image "$node_image" '
    length == 1 and (.[0].Id == $id or ("/"+$id)==$name) and .[0].Name == $name and
    (.[0].Id | test("^[a-f0-9]{64}$")) and
    .[0].Config.Labels["pgcopydb-operator.io/feature-run"] == $owner and .[0].Config.Image==$image
  ' <<< "$details" >/dev/null || return 1
  id=$(jq -r '.[0].Id' <<< "$details") || return 1
  timeout --kill-after=10s 30 docker rm -f "$id" >/dev/null || return 1
  local remaining
  remaining=$(timeout --kill-after=10s 30 docker ps -aq --no-trunc --filter "id=$id") || return 1
  [[ -z "$remaining" ]] || return 1
  observer=
  [[ ! -f "$state/state.json" ]] || save '.observer = null'
}
node_metadata() {
  timeout --kill-after=10s 30 docker exec -i "$1" /bin/sh -c 'exec timeout --kill-after=5s 20s /bin/sh -s' <<'NODE_METADATA'
set -eu
for tool in stat awk; do command -v "$tool" >/dev/null || exit 1; done
start_time() { awk '{sub(/^.*\) /, ""); if ($20 !~ /^[0-9]+$/) exit 1; print $20}' "/proc/$1/stat"; }
identity() {
  value=$(stat -Lc %d:%i "$1") || exit 1
  type=$(stat -f -c %T "$1") || exit 1
  printf '%s:%s\n' "$value" "$type"
}
collect() {
  own=$(stat -Lc %i /proc/self/ns/mnt)
  node=$(stat -Lc %i /proc/1/ns/mnt)
  test "$own" = "$node" || exit 1
  printf '%s %s\n' "$(start_time 1)" "$own"
  identity /
  identity /var
  awk '$5=="/" {root=$0; r++} $5=="/var" {volume=$0; v++}
    END {if(r!=1 || v!=1) exit 1; print root; print volume}' /proc/self/mountinfo
}
self=$(start_time "$$")
before=$(collect)
after=$(collect)
test -n "$self" && test "$self" = "$(start_time "$$")" && test "$before" = "$after"
printf '%s\n' "$before"
NODE_METADATA
}
capacity() {
  local pid=${1:-0} start=${2:-} root merged= upper= volume details existing profile node_id= metadata=
  timeout --kill-after=10s 30 docker info --format '{{json .}}' > "$work/docker.json"
  jq -e '.OSType == "linux" and .CgroupVersion == "2" and
    (.NCPU | type == "number" and . >= 8) and (.MemTotal | type == "number" and . >= 17179869184) and
    (.DockerRootDir | type == "string" and test("^/[A-Za-z0-9_./-]+$") and (contains("..") | not)) and
    (.DriverStatus | type == "array") and
    ((.Driver=="overlay2" and ([.DriverStatus[][] | tostring | ascii_downcase | contains("snapshotter")] | any | not)) or
     (.Driver=="overlayfs" and [.DriverStatus[] | select(.[0]=="driver-type")]==[["driver-type","io.containerd.snapshotter.v1"]]))
  ' "$work/docker.json" >/dev/null || fail kind-capacity-unproved
  profile=$(jq -r 'if .Driver=="overlay2" then "overlay2" else "containerd" end' "$work/docker.json")
  root=$(jq -r '.DockerRootDir' "$work/docker.json")
  existing=$(timeout --kill-after=10s 30 docker ps -aq --no-trunc --filter "name=^/$cluster-capacity$") || fail kind-observer-unproved
  [[ -z "$existing" ]] || fail kind-observer-occupied
  save '.observer = {id:null,name:$name,owner:$owner}' --arg name "$cluster-capacity" --arg owner "$owner"
  observer=$(timeout --kill-after=10s 180 docker create --name "$cluster-capacity" --label "pgcopydb-operator.io/feature-run=$owner" \
    --network none --read-only --cap-drop ALL --security-opt no-new-privileges --pid host --cgroupns host \
    --mount "type=bind,src=/sys/fs/cgroup,dst=/capacity/cgroup,readonly,bind-recursive=readonly,bind-propagation=rprivate" \
    --mount "type=bind,src=$root,dst=/capacity/storage,readonly,bind-recursive=readonly,bind-propagation=rprivate" \
    --entrypoint /bin/bash -i "$node_image" -s -- "$pid" "$start")
  [[ "$observer" =~ ^[a-f0-9]{64}$ ]] || fail kind-observer-invalid
  [[ ! -f "$state/state.json" ]] || save '.observer = {id:$id,name:$name,owner:$owner}' \
    --arg id "$observer" --arg name "$cluster-capacity" --arg owner "$owner"
  details=$(timeout --kill-after=10s 30 docker inspect "$observer")
  jq -e --arg image "$node_image" --arg id "$observer" --arg name "/$cluster-capacity" --arg owner "$owner" '
    length==1 and .[0].Id==$id and .[0].Config.Image==$image and .[0].Name==$name and
    .[0].Config.Labels["pgcopydb-operator.io/feature-run"]==$owner' \
    <<< "$details" >/dev/null || fail kind-observer-invalid
  if [[ "$pid" != 0 ]]; then
    verify_node
    jq -e '[.[0].Mounts[]? | select(.Destination=="/var")]|length==1' "$work/node.json" >/dev/null || fail kind-storage-unproved
    node_id=$(jq -r '.[0].Id' "$work/node.json")
    if [[ "$profile" == containerd ]]; then
      cp "$work/node.json" "$work/node-before.json"
      node_metadata "$node_id" > "$work/node-metadata-before" || fail kind-storage-unproved
      metadata=$(<"$work/node-metadata-before")
    fi
    details=$(jq -sc '.[0]+.[1]' <(printf '%s' "$details") "$work/node.json")
  fi
  jq -e --arg root "$root" --arg profile "$profile" 'all(.[];
    if $profile=="containerd" then (.GraphDriver==null or .GraphDriver.Name=="overlayfs") and .GraphDriver.Data==null else
    .GraphDriver.Name=="overlay2" and
    (.GraphDriver.Data.UpperDir | startswith($root+"/overlay2/") and endswith("/diff")) and
    .GraphDriver.Data.MergedDir==(.GraphDriver.Data.UpperDir|rtrimstr("/diff")+"/merged") end) and
    all(.[].Mounts[]? | select(.Destination=="/var");
      .Type=="volume" and .Driver=="local" and (.Source|startswith($root+"/volumes/") and endswith("/_data")))
  ' <<< "$details" >/dev/null || fail kind-storage-unproved
  if [[ "$profile" == overlay2 ]]; then
    merged=$(jq -r --arg root "$root" '[.[].GraphDriver.Data.MergedDir | sub("^"+$root;"/capacity/storage")]|join(":")' <<< "$details")
    upper=$(jq -r --arg root "$root" '[.[].GraphDriver.Data.UpperDir | sub("^"+$root;"/capacity/storage")]|join(":")' <<< "$details")
  fi
  volume=$(jq -r --arg root "$root" '[.[].Mounts[]? | select(.Destination=="/var") | .Source |
    sub("^"+$root;"/capacity/storage")]|join(":")' <<< "$details")
  [[ "$merged$upper$volume" =~ ^[A-Za-z0-9_./:-]*$ && "$merged$upper$volume" != *..* ]] || fail kind-storage-unproved
  # Arguments arrive through stdin so inspection can bind the observer's own layer before it runs.
  { printf 'set -- %q %q %q %q %q %q %q %q %q %q\n' "$pid" "$start" "$merged" "$upper" "$volume" "$profile" "$root" "$observer" "$node_id" "$metadata"; cat <<'CAPACITY'
set -euo pipefail
exec 2>/dev/null
die() { exit 1; }
for tool in stat awk readlink df getconf; do command -v "$tool" >/dev/null || die; done
# Linux's initial cgroup namespace inode is an implementation invariant, not a portable ABI.
[[ $(stat -f -c %t /proc/self/ns/cgroup) == 6e736673 &&
   $(stat -Lc %i /proc/self/ns/cgroup) == 4026531835 ]] || die
[[ $(stat -f -c %t /capacity/cgroup) == 63677270 ]] || die
awk '$5=="/capacity/cgroup" { if ($4!="/" || $6 !~ /(^|,)ro(,|$)/ || $0 !~ / - cgroup2 /) exit 1; n++ }
     index($5,"/capacity/cgroup/")==1 { exit 1 } END { if(n!=1) exit 1 }' /proc/self/mountinfo || die
start_time() {
  local value
  value=$(<"/proc/$1/stat") || die
  value=${value##*) }
  awk '{if ($20 !~ /^[0-9]+$/) exit 1; print $20}' <<< "$value"
}
if [[ "$6" == overlay2 ]]; then
awk -v allowed="$3" 'BEGIN {split(allowed,a,":"); for(i in a) owned[a[i]]=1}
     $5=="/capacity/storage" { n++; if ($6 !~ /(^|,)ro(,|$)/ || $0 ~ / - overlay /) exit 1 }
     index($5,"/capacity/storage/")==1 {
       if(!owned[$5] || $4!="/" || $6 !~ /(^|,)ro(,|$)/ || $0 !~ / - overlay /) exit 1 }
     END { if(n!=1) exit 1 }' /proc/self/mountinfo || die
[[ -d /capacity/storage/overlay2 && -d /capacity/storage/volumes &&
   ! -e /capacity/storage/containerd/daemon/io.containerd.snapshotter.v1.overlayfs &&
   $(readlink -f /capacity/storage/overlay2) == /capacity/storage/overlay2 &&
   $(readlink -f /capacity/storage/volumes) == /capacity/storage/volumes &&
   $(stat -c %d /capacity/storage) == "$(stat -c %d /capacity/storage/overlay2)" &&
   $(stat -c %d /capacity/storage) == "$(stat -c %d /capacity/storage/volumes)" ]] || die
IFS=: read -r -a backing <<< "$4:$5"
for path in "${backing[@]}"; do
  [[ -n "$path" ]] || continue
  [[ "$path" == /capacity/storage/* && "$path" != *..* && -d "$path" &&
     $(readlink -f "$path") == "$path" && $(stat -c %d "$path") == "$(stat -c %d /capacity/storage)" ]] || die
done
else
  die() { printf 'kind-storage-unproved\n'; exit 1; }
  storage_host_root=$7
  mountinfo=$(</proc/self/mountinfo)
  mount_record() {
    awk -v path="$1" '$5==path {n++; for(i=7;i<=NF;i++) if($i=="-") {
      if(NF!=i+3) exit 1; print $3, $4, $(i+1), $(i+2), $(i+3)}} END {if(n!=1)exit 1}' <<< "$mountinfo"
  }
  storage_record=$(mount_record /capacity/storage) || die
  read -r storage_device storage_root storage_type storage_source storage_options <<< "$storage_record"
  [[ "$storage_type" != overlay && "$storage_root" =~ ^/[A-Za-z0-9_./-]*$ && "$storage_root" != *..* ]] || die
  backing_device=$(stat -c %d /capacity/storage)
  owned_mount() {
    [[ "$1" =~ ^[a-f0-9]{64}$ ]] || die
    awk -v suffix="/$1/rootfs" 'index($5,"/capacity/storage/")==1 &&
      substr($5,length($5)-length(suffix)+1)==suffix {n++; print $5} END{if(n!=1)exit 1}' <<< "$mountinfo"
  }
  file_identity() {
    local identity type
    identity=$(stat -Lc %d:%i "$1") || die
    type=$(stat -f -c %T "$1") || die
    [[ "$identity" =~ ^[0-9]+:[0-9]+$ && -n "$type" ]] || die
    printf '%s:%s' "$identity" "$type"
  }
  canonical_backing() {
    local path=$1
    [[ "$path" == "$storage_host_root"/* ]] || die
    path="/capacity/storage${path#"$storage_host_root"}"
    [[ "$path" =~ ^/[A-Za-z0-9_./-]+$ && "$path" != *..* && -d "$path" &&
       $(readlink -f "$path") == "$path" && $(stat -c %d "$path") == "$backing_device" ]] || die
  }
  check_overlay() {
    local record=$1 device mountroot type source options option upper= work= lower= extra= path
    read -r device mountroot type source options extra <<< "$record"
    [[ "$type" == overlay && "$mountroot" == / && "$source" == overlay && -z "$extra" ]] || die
    awk -F, '{for(i=1;i<=NF;i++) {split($i,a,"="); if($i=="" || seen[a[1]]++)exit 1}}' <<< "$options" || die
    IFS=, read -r -a option_list <<< "$options"
    for option in "${option_list[@]}"; do
      case "$option" in
        rw|ro) [[ -z "$extra" ]] || die; extra=$option ;;
        upperdir=*) [[ -z "$upper" ]] || die; upper=${option#*=} ;;
        workdir=*) [[ -z "$work" ]] || die; work=${option#*=} ;;
        lowerdir=*) [[ -z "$lower" ]] || die; lower=${option#*=} ;;
        index=off|metacopy=off|redirect_dir=off|xino=off|userxattr) ;;
        *) die ;;
      esac
    done
    [[ -n "$extra" && "$upper" == */snapshots/*/fs && "$work" == "${upper%/fs}/work" &&
       "${upper%/fs}" =~ /snapshots/[0-9]+$ && -n "$lower" && "$lower" != :* && "$lower" != *: && "$lower" != *::* ]] || die
    canonical_backing "$upper"
    canonical_backing "$work"
    IFS=: read -r -a lowers <<< "$lower"
    for path in "${lowers[@]}"; do
      [[ "$path" =~ /snapshots/[0-9]+/fs$ ]] || die
      canonical_backing "$path"
    done
  }
  observer_mount=$(owned_mount "$8") || die
  observer_record=$(mount_record "$observer_mount") || die
  own_record=$(mount_record /) || die
  [[ "$own_record" == "$observer_record" && $(readlink -f "$observer_mount") == "$observer_mount" ]] || die
  own_stat=$(file_identity /) || die
  self_start=$(start_time "$$") || die
  [[ "$own_stat" == "$(file_identity "$observer_mount")" ]] || die
  check_overlay "$own_record"
  node_mount=
  if [[ "$1" != 0 ]]; then
    node_mount=$(owned_mount "$9") || die
    node_record=$(mount_record "$node_mount") || die
    check_overlay "$node_record"
    node_metadata=()
    while IFS= read -r line; do node_metadata+=("$line"); done <<< "${10}"
    [[ ${#node_metadata[@]} == 5 && "${node_metadata[0]}" =~ ^[0-9]+\ [1-9][0-9]*$ &&
       "${node_metadata[0]%% *}" == "$(start_time "$1")" &&
       "${node_metadata[1]}" == "$(file_identity "$node_mount")" &&
       "${node_metadata[2]}" == "$(file_identity "$5")" ]] || die
    reader_mountinfo="$mountinfo"
    mountinfo="${node_metadata[3]}"$'\n'"${node_metadata[4]}"
    [[ "$(mount_record /)" == "$node_record" ]] || die
    read -r volume_device volume_root volume_type volume_source volume_options <<< "$(mount_record /var)"
    mountinfo="$reader_mountinfo"
    [[ "$volume_device" == "$storage_device" && "$volume_type" == "$storage_type" &&
       "$volume_root" == "${storage_root%/}${5#/capacity/storage}" &&
       "$volume_source" == "$storage_source" && "$volume_options" == "$storage_options" ]] || die
    canonical_backing "$7${5#/capacity/storage}"
  fi
  awk -v observer="$observer_mount" -v node="$node_mount" '
    $5=="/capacity/storage" {n++; if($6 !~ /(^|,)ro(,|$)/)exit 1}
    index($5,"/capacity/storage/")==1 {
      if(($5!=observer && $5!=node) || $6 !~ /(^|,)ro(,|$)/)exit 1}
    END{if(n!=1)exit 1}' <<< "$mountinfo" || die
  [[ "$mountinfo" == "$(</proc/self/mountinfo)" && "$own_stat" == "$(file_identity /)" &&
     "$own_stat" == "$(file_identity "$observer_mount")" && "$self_start" == "$(start_time "$$")" ]] || die
fi
df -Pk /capacity/storage | awk 'NR==2 {if ($4 !~ /^[0-9]+$/ || $4<33554432) exit 1; n++} END {if(n!=1) exit 1}' || die
[[ $(getconf _NPROCESSORS_ONLN) =~ ^[0-9]+$ && $(getconf _NPROCESSORS_ONLN) -ge 8 ]] || die
awk '$1=="MemTotal:" {if($2 !~ /^[0-9]+$/ || $2<16777216 || $3!="kB") exit 1; n++} END {if(n!=1) exit 1}' /proc/meminfo || die
walk() {
  local process=$1 expected=$2 before membership path quota period memory
  before=$(start_time "$process") || die
  [[ -n "$before" && ( -z "$expected" || "$before" == "$expected" ) ]] || die
  membership=$(<"/proc/$process/cgroup") || die
  [[ "$membership" == 0::/* && "$membership" != *$'\n'* && "$membership" != *..* && "$membership" != *' (deleted)' ]] || die
  path=${membership#0::}
  [[ "$(readlink -f "/capacity/cgroup$path")" == "/capacity/cgroup${path%/}" ]] || die
  while :; do
    local directory="/capacity/cgroup${path%/}"
    awk -F, '{last=-1; count=0; for(i=1;i<=NF;i++) {if ($i !~ /^[0-9]+(-[0-9]+)?$/) exit 1;
      n=split($i,a,"-"); end=n==1?a[1]:a[2]; if(a[1]<=last || end<a[1]) exit 1;
      count+=end-a[1]+1; last=end} if(count<8) exit 1; seen++} END{if(seen!=1)exit 1}' "$directory/cpuset.cpus.effective" || die
    if [[ "$path" != / ]]; then
      read -r quota period < "$directory/cpu.max" || die
      [[ "$period" =~ ^[1-9][0-9]{0,8}$ ]] || die
      [[ "$quota" == max || ( "$quota" =~ ^[1-9][0-9]{0,14}$ && "$quota" -ge $((8*period)) ) ]] || die
      memory=$(<"$directory/memory.max") || die
      [[ "$memory" == max || ( "$memory" =~ ^[1-9][0-9]{0,17}$ && "$memory" -ge 17179869184 ) ]] || die
    fi
    [[ "$path" != / ]] || break
    path=${path%/*}; path=${path:-/}
  done
  [[ "$(start_time "$process")" == "$before" && "$(<"/proc/$process/cgroup")" == "$membership" ]] || die
  printf '%s' "$before"
}
self=$(walk "$$" '') || die
node=0
[[ "$1" == 0 ]] || node=$(walk "$1" "$2") || die
printf '{"nodePID":%s,"nodeStartTime":"%s","observerStartTime":"%s"}\n' "$1" "$node" "$self"
CAPACITY
  } | timeout --kill-after=10s 90 docker start -ai "$observer" > "$work/capacity.json"
  timeout --kill-after=10s 30 docker inspect "$observer" | jq -e 'length==1 and .[0].State.Running==false and .[0].State.ExitCode==0' \
    >/dev/null || fail kind-capacity-unproved
  jq -e --argjson pid "$pid" '.nodePID == $pid and (.nodeStartTime | test("^[0-9]+$")) and
    (.observerStartTime | test("^[1-9][0-9]*$"))' "$work/capacity.json" >/dev/null || fail kind-capacity-unproved
  if [[ "$profile" == containerd && "$pid" != 0 ]]; then
    verify_node
    jq -e -s '.[0][0] as $before | .[1][0] as $after |
      $before.GraphDriver==$after.GraphDriver and $before.Mounts==$after.Mounts' \
      "$work/node-before.json" "$work/node.json" >/dev/null || fail kind-storage-unproved
    node_metadata "$node_id" > "$work/node-metadata-after" || fail kind-storage-unproved
    cmp -s "$work/node-metadata-before" "$work/node-metadata-after" || fail kind-storage-unproved
  fi
  cleanup_observer || fail kind-observer-cleanup-failed
}

no_delivery() {
  kubectl_owned get prometheus feature-monitoring-prometheus -n monitoring -o json > "$work/prometheus.json"
  kubectl_owned get --raw /api/v1/namespaces/monitoring/services/http:feature-monitoring-prometheus:9090/proxy/api/v1/status/config > "$work/loaded.json"
  kubectl_owned get --raw /api/v1/namespaces/monitoring/services/http:feature-monitoring-prometheus:9090/proxy/api/v1/alertmanagers > "$work/discovery.json"
  (cd "$trusted" && FEATURE_E2E_MONITORING_SPEC="$work/prometheus.json" FEATURE_E2E_MONITORING_CONFIG="$work/loaded.json" \
    FEATURE_E2E_MONITORING_DISCOVERY="$work/discovery.json" timeout --kill-after=10s 180 go test ./test/buildconfig -run '^TestFeatureE2EMonitoringNoDelivery$' -count=1)
}
bootstrap() {
  local existing
  kubectl_owned config view -o json | jq -e --arg context "$context" '
    .["current-context"] == $context and (.contexts|length)==1 and (.clusters|length)==1 and
    .contexts[0].name == $context and .contexts[0].context.cluster == $context and .clusters[0].name == $context
  ' >/dev/null || fail kind-kubeconfig-invalid
  record kubeconfig
  kubectl_owned get --raw /readyz >/dev/null
  kubectl_owned wait nodes --all --for=condition=Ready --timeout=300s
  kubectl_owned get nodes -o json | jq -e --arg name "$cluster-control-plane" --arg uid "$(jq -r '.nodes[0].apiUID' "$state/state.json")" '
    def cpu: if endswith("m") then rtrimstr("m")|tonumber/1000 else tonumber end;
    def memory: if endswith("Ki") then rtrimstr("Ki")|tonumber*1024 elif endswith("Mi") then rtrimstr("Mi")|tonumber*1048576
      elif endswith("Gi") then rtrimstr("Gi")|tonumber*1073741824 else tonumber end;
    (.items|length)==1 and .items[0].metadata.name == $name and .items[0].metadata.uid==$uid and
    (.items[0].spec.unschedulable // false)==false and
    (.items[0].status.allocatable.cpu|cpu)>=8 and (.items[0].status.allocatable.memory|memory)>=17179869184 and
    (.items[0].status.conditions | any(.type=="Ready" and .status=="True")) and
    (.items[0].status.conditions | [.[]|select(.type=="DiskPressure" or .type=="MemoryPressure" or .type=="PIDPressure") ] |
      length==3 and all(.status=="False"))
  ' >/dev/null || fail kind-node-not-ready
  record node
  kubectl_owned rollout status deployment/coredns -n kube-system --timeout=300s
  kubectl_owned rollout status daemonset/kindnet -n kube-system --timeout=300s
  record network
  kubectl_owned get storageclass standard -o json | jq -e '
    .provisioner=="rancher.io/local-path" and .volumeBindingMode=="WaitForFirstConsumer"
  ' >/dev/null || fail kind-storage-class-invalid
  kubectl_owned create namespace pgcopydb-e2e
  kubectl_owned create -f - <<STORAGE
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: feature-storage-proof
  namespace: pgcopydb-e2e
spec:
  storageClassName: standard
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: feature-storage-proof
  namespace: pgcopydb-e2e
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  containers:
    - name: proof
      image: $node_image
      command: [/bin/sh, -ec]
      args: ['getent hosts kubernetes.default.svc.cluster.local >/dev/null; echo proof > /proof/check; test "\$(cat /proof/check)" = proof']
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: [ALL]
      volumeMounts:
        - name: proof
          mountPath: /proof
  volumes:
    - name: proof
      persistentVolumeClaim:
        claimName: feature-storage-proof
STORAGE
  kubectl_owned wait pod/feature-storage-proof -n pgcopydb-e2e --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
  kubectl_owned get pvc feature-storage-proof -n pgcopydb-e2e -o json | jq -e '.status.phase=="Bound"' >/dev/null
  kubectl_owned delete pod/feature-storage-proof pvc/feature-storage-proof -n pgcopydb-e2e --wait=true --timeout=120s
  record storage
  download cnpg.tgz https://github.com/cloudnative-pg/charts/releases/download/cloudnative-pg-v0.29.0/cloudnative-pg-0.29.0.tgz \
    668e065ff53508d58238788fd35b355a925060843629a951df0e6a9362e6d32f
  timeout --kill-after=10s 360 helm upgrade --install feature-cnpg "$work/cnpg.tgz" --namespace cnpg-system --create-namespace \
    --kubeconfig "$KUBECONFIG" --kube-context "$context" --wait --timeout 5m --set fullnameOverride=feature-cnpg \
    --set image.tag=1.30.0@sha256:a2701eb97cdd2a34b1fdb2cb51987f544b706e40bec72ae7146cd8580efefebb
  for crd in backups clusterimagecatalogs clusters databaseroles databases failoverquorums imagecatalogs poolers publications scheduledbackups subscriptions; do
    kubectl_owned wait "crd/$crd.postgresql.cnpg.io" --for=condition=Established --timeout=120s
  done
  kubectl_owned rollout status deployment/feature-cnpg -n cnpg-system --timeout=300s
  record cnpg
  download monitoring.tgz https://github.com/prometheus-community/helm-charts/releases/download/kube-prometheus-stack-90.0.0/kube-prometheus-stack-90.0.0.tgz \
    04b90a3f4aab4b40572585095331259bc8f00328e40f9ec588e54cdb9aa6a55f
  timeout --kill-after=10s 90 helm template feature-monitoring "$work/monitoring.tgz" --namespace monitoring --include-crds \
    --values "$trusted/hack/feature-e2e-kind-monitoring.yaml" > "$work/rendered.yaml"
  (cd "$trusted" && FEATURE_E2E_MONITORING_RENDERED="$work/rendered.yaml" timeout --kill-after=10s 180 \
    go test ./test/buildconfig -run '^TestFeatureE2EMonitoringNoDelivery$' -count=1)
  timeout --kill-after=10s 360 helm upgrade --install feature-monitoring "$work/monitoring.tgz" --namespace monitoring --create-namespace \
    --kubeconfig "$KUBECONFIG" --kube-context "$context" --wait --timeout 5m --values "$trusted/hack/feature-e2e-kind-monitoring.yaml"
  for crd in alertmanagerconfigs alertmanagers podmonitors probes prometheusagents prometheuses prometheusrules scrapeconfigs servicemonitors thanosrulers; do
    kubectl_owned wait "crd/$crd.monitoring.coreos.com" --for=condition=Established --timeout=120s
  done
  kubectl_owned rollout status deployment/feature-monitoring-operator -n monitoring --timeout=300s
  kubectl_owned rollout status statefulset/prometheus-feature-monitoring-prometheus -n monitoring --timeout=300s
  record monitoring
  no_delivery
  record noDelivery
  existing=$(kubectl_owned get crd migrations.pgcopydb-operator.io --ignore-not-found -o name) || fail kind-candidate-absence-unproved
  [[ -z "$existing" ]] || fail kind-candidate-crd-exists
  [[ "$(sha256sum "$1" | cut -d ' ' -f 1)" == "$(jq -r '.candidateSHA256' "$state/state.json")" ]] || fail kind-candidate-checksum-invalid
  kubectl_owned create -f "$1"
  kubectl_owned wait crd/migrations.pgcopydb-operator.io --for=condition=Established --timeout=120s
  kubectl_owned get crd migrations.pgcopydb-operator.io -o json > "$work/installed-crd.json"
  [[ -s "$work/installed-crd.json" ]] || fail kind-candidate-readback-invalid
  mv -- "$work/installed-crd.json" "$state/installed-crd.json"
  (cd "$trusted" && FEATURE_E2E_CANDIDATE_CRD="$1" FEATURE_E2E_INSTALLED_CRD="$state/installed-crd.json" \
    timeout --kill-after=10s 180 go test ./test/buildconfig -run '^TestFeatureE2EInstalledCRDCompatibility$' -count=1)
  record candidateCRD
}
destroy() {
  if [[ ! -e "$state/state.json" ]]; then
    [[ ! -e "$KUBECONFIG" && ! -e "$state/installed-crd.json" ]] && absent || fail kind-absence-unproved
    return
  fi
  load_state
  observer=$(jq -r '.observer.id // .observer.name // empty' "$state/state.json")
  [[ -z "$observer" ]] || cleanup_observer || fail kind-observer-cleanup-failed
  if [[ "$(jq -r '.stage' "$state/state.json")" == destroyed ]]; then
    absent || fail kind-absence-unproved
    return
  fi
  if [[ "$(jq -r '.stage' "$state/state.json")" == preflight ]]; then
    absent || fail kind-absence-unproved
    save '.stage="destroyed" | .teardown="complete"'
    return
  fi
  jq -e '.owned==true and .evidence.initialAbsence==
    {runID,runAttempt,cluster,context,kubeconfig,candidateSHA256,nodes:[]}
  ' "$state/state.json" >/dev/null || fail kind-ownership-invalid
  if [[ "$(jq '.nodes|length' "$state/state.json")" == 0 ]]; then
    if absent; then
      save '.stage="destroyed" | .teardown="complete"'
      return
    fi
    save '.nodes=$nodes' --argjson nodes "$(capture_node teardown)"
  else
    verify_node
  fi
  save '.teardown="deleting"'
  kind_owned delete cluster --name "$cluster"
  absent || fail kind-teardown-incomplete
  local id remaining
  while read -r id; do
    [[ "$id" =~ ^[a-f0-9]{64}$ ]] || fail kind-state-invalid
    remaining=$(timeout --kill-after=10s 30 docker ps -aq --no-trunc --filter "id=$id")
    [[ -z "$remaining" ]] || fail kind-teardown-incomplete
  done < <(jq -r '.nodes[].id' "$state/state.json")
  save '.stage="destroyed" | .teardown="complete"'
}

main() {
  get_kind
  if [[ "$command" == create ]]; then
    [[ ! -e "$state/state.json" && ! -e "$KUBECONFIG" && ! -e "$state/installed-crd.json" &&
       "$3" == /* && -f "$3" && ! -L "$3" && "$4" =~ ^[a-f0-9]{64}$ ]] || fail kind-input-invalid
    [[ -z "$(ls -A -- "$state")" ]] || fail kind-state-invalid
    [[ "$(sha256sum "$3" | cut -d ' ' -f 1)" == "$4" ]] || fail kind-candidate-checksum-invalid
    jq -n --argjson run "$GITHUB_RUN_ID" --argjson attempt "$GITHUB_RUN_ATTEMPT" --arg cluster "$cluster" \
      --arg context "$context" --arg kubeconfig "$KUBECONFIG" --arg sha "$4" '
      {runID:$run,runAttempt:$attempt,cluster:$cluster,context:$context,kubeconfig:$kubeconfig,candidateSHA256:$sha,
       owned:false,stage:"preflight",teardown:"pending",nodes:[],observer:null,evidence:{}}
    ' > "$work/state.json"
    mv -- "$work/state.json" "$state/state.json"
    capacity
    absent || fail kind-cluster-occupied
    save '.owned=true | .stage="intent"'
    record initialAbsence
    kind_owned create cluster --name "$cluster" --image "$node_image" --wait 5m
    save '.nodes=$nodes | .stage="created"' --argjson nodes "$(capture_node)"
    kubectl_owned get nodes -o json > "$work/api-node.json"
    jq -e --arg name "$cluster-control-plane" '(.items|length)==1 and .items[0].metadata.name==$name and
      (.items[0].metadata.uid|type=="string" and length>0)' "$work/api-node.json" >/dev/null || fail kind-node-identity-invalid
    save '.nodes[0].apiUID=$uid' --arg uid "$(jq -r '.items[0].metadata.uid' "$work/api-node.json")"
    capacity "$(jq -r '.nodes[0].pid' "$state/state.json")"
    save '.nodes[0].startTime=$start' --arg start "$(jq -r '.nodeStartTime' "$work/capacity.json")"
    verify_node
    record capacity
    bootstrap "$3"
    save '.stage="ready"'
  elif [[ "$command" == verify-ready ]]; then
    load_state
    [[ -s "$KUBECONFIG" && -s "$state/installed-crd.json" ]] || fail kind-readiness-invalid
    jq -e '. as $s | {runID,runAttempt,cluster,context,kubeconfig,candidateSHA256,nodes} as $b |
      .owned == true and .stage == "ready" and .teardown == "pending" and (.nodes|length)==1 and
      .evidence.initialAbsence == ($b | .nodes=[]) and
      (["capacity","kubeconfig","node","network","storage","cnpg","monitoring","candidateCRD","noDelivery"] |
        all(. as $key | $s.evidence[$key] == $b))' "$state/state.json" >/dev/null || fail kind-readiness-invalid
    verify_node
    capacity "$(jq -r '.nodes[0].pid' "$state/state.json")" "$(jq -r '.nodes[0].startTime' "$state/state.json")"
    verify_node
    kubectl_owned get nodes -o json | jq -e --arg name "$cluster-control-plane" --arg uid "$(jq -r '.nodes[0].apiUID' "$state/state.json")" '
      (.items|length)==1 and .items[0].metadata.name==$name and .items[0].metadata.uid==$uid
    ' >/dev/null || fail kind-node-identity-invalid
    no_delivery
  else
    destroy
  fi
}

main "$@" >/dev/null
printf 'kind-%s-success\n' "$command" >&3
