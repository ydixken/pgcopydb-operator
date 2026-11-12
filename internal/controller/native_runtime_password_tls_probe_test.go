package controller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

const nativePasswordSecretKind = "Secret"
const nativePasswordServiceKind = "Service"
const nativePasswordContainerKey = "container_id"
const nativePasswordJobKey = "job_uid"
const nativePasswordQualificationLabel = "rpgmigrate.io/qualification"
const nativePasswordOwnedNode = "owned-node"
const nativePasswordOriginalContainer = "containerd://original"
const nativePasswordReplacementContainer = "containerd://replacement"
const nativePasswordUnexpectedKey = "unexpected"
const nativePasswordForeignOwner = "foreign-owner"

const nativePasswordPodKey = "pod_uid"
const nativePasswordSandboxKey = "sandbox_id"
const nativePasswordServerVolume = "server-input"
const nativePasswordServerMount = "/run/server-input"
const nativePasswordClientVolume = "client-input"

func nativePasswordActual(ctx context.Context, clientset kubernetes.Interface, node nativeRuntimeNode, pod *corev1.Pod, expected nativeRuntimeProfileRequest, previous, self map[string]any, terminal bool) (map[string]any, error) {
	if pod == nil || len(pod.Status.ContainerStatuses) != 1 {
		return nil, errors.New("runtime container absent")
	}
	mode := lifecycleWaitingMain
	if terminal {
		mode = probeTerminalCase
	}
	projection, err := nativeRuntimeProfile(ctx, clientset, node, expected, pod.Status.ContainerStatuses[0].ContainerID, mode, previous, self)
	if err != nil {
		return nil, err
	}
	return projection, nil
}

func nativePasswordNode(ctx context.Context, clientset kubernetes.Interface, input nativeRuntimeInput, node nativeRuntimeNode) error {
	live, err := clientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	if err != nil || live.UID != node.UID || live.Labels[nativeRuntimeRunLabel] != input.RunID || live.Spec.Unschedulable || live.Status.NodeInfo.KubeletVersion != "v1.36.4" || live.Status.NodeInfo.ContainerRuntimeVersion != "containerd://2.3.4" || live.Status.NodeInfo.Architecture != nativeRuntimeARM64 || live.Status.NodeInfo.OperatingSystem != nativeRuntimeLinux {
		return errors.New("owned node changed")
	}
	facts, err := nativeRuntimeNodeFacts(ctx, node)
	if err != nil {
		return err
	}
	if facts["active_runtime_verified"] != true || facts["stock_runtime_options"] != true {
		return errors.New("active runtime profile refused")
	}
	return nil
}

func nativePasswordImages(path string, input nativeRuntimeInput) (map[int]nativeRuntimeImage, error) {
	bound, err := nativeRuntimeAssetsByRole(path, input)
	if err != nil {
		return nil, err
	}
	return map[int]nativeRuntimeImage{16: bound[nativeRuntimePG16Role], 18: bound[nativeRuntimePG18Role]}, nil
}

func nativePasswordValidateInput(input nativePasswordInput, path, program string, now time.Time) error {
	r := input.Runtime
	current := float64(now.UnixNano()) / 1e9
	evidence := native.EvidenceProgramSHA256()
	if input.SchemaVersion != 1 || r.SchemaVersion != 1 || r.Phase != nativePasswordPhase || r.Pair != nil || r.NamespaceUID != nil || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(r.QualificationID) || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(r.RunID) || r.Namespace != "native-password-tls-"+r.QualificationID[:16] || r.ReceiptPath != filepath.Join(filepath.Dir(path), "password-tls-receipt.json") || r.Kubeconfig != filepath.Join(r.RunDir, "kubeconfig") || r.Context != "kind-rpgmigrate-m7-"+r.RunID || !slices.Equal(r.RequiredCases, nativePasswordCases()) || len(r.Nodes) != 3 {
		return errors.New("fixed primitive probe input refused")
	}
	for _, digest := range []string{r.LegacySourceSHA, r.LegacyGuardSHA, r.LegacyOwnershipSHA, r.QualificationSourceSHA, r.EvidenceProgramSHA, r.AssetsSHA, r.MetadataProgramSHA, input.ProgramSHA} {
		if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(digest) {
			return errors.New("probe source binding absent")
		}
	}
	if r.EvidenceProgramSHA != hex.EncodeToString(evidence[:]) || input.ProgramSHA != nativeRuntimeSHA([]byte(program)) || r.MetadataProgramSHA != nativeRuntimeSHA([]byte(nativeRuntimeMetadataProgram+"\x00"+nativeRuntimeNodeProgram)) || !(current < r.StageDeadline && r.StageDeadline <= current+600 && r.StageDeadline+600 <= min(r.LeaseExpiry, input.ParentDeadline)) {
		return errors.New("probe original source or deadline changed")
	}
	seen := map[string]bool{}
	previous := ""
	for _, node := range r.Nodes {
		if node.Name <= previous || node.UID == "" || seen[string(node.UID)] || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(node.ContainerID) {
			return errors.New("probe node identity refused")
		}
		previous = node.Name
		seen[string(node.UID)] = true
	}
	return nil
}

func nativePasswordPodStatus(pod *corev1.Pod, image nativeRuntimeImage, known string) (string, error) {
	id, _, err := nativeRuntimeStatus(pod, image, known)
	if err != nil {
		return id, err
	}
	if id != "" {
		status := pod.Status.ContainerStatuses[0]
		if !reflect.DeepEqual(status.LastTerminationState, corev1.ContainerState{}) || status.State.Terminated != nil && status.State.Terminated.ContainerID != id {
			return id, errors.New("terminal container incarnation changed")
		}
	}
	return id, nil
}

func nativePasswordPodMatches(expected, actual *corev1.Pod) bool {
	return expected != nil && actual != nil && (expected.UID == "" || expected.UID == actual.UID) && expected.Name == actual.Name && expected.Namespace == actual.Namespace && actual.DeletionTimestamp.IsZero() && reflect.DeepEqual(expected.Spec, actual.Spec) && reflect.DeepEqual(expected.Labels, actual.Labels) && reflect.DeepEqual(expected.Annotations, actual.Annotations) && reflect.DeepEqual(expected.OwnerReferences, actual.OwnerReferences) && reflect.DeepEqual(expected.Finalizers, actual.Finalizers)
}

func nativePasswordServerReady(ctx context.Context, clientset kubernetes.Interface, created *corev1.Pod, image nativeRuntimeImage) (*corev1.Pod, error) {
	var result *corev1.Pod
	known := ""
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(call context.Context) (bool, error) {
		live, err := clientset.CoreV1().Pods(created.Namespace).Get(call, created.Name, metav1.GetOptions{})
		if err != nil || !nativePasswordPodMatches(created, live) {
			return false, errors.New("server Pod replaced")
		}
		id, err := nativePasswordPodStatus(live, image, known)
		if err != nil {
			return false, err
		}
		known = id
		if id == "" || live.Status.ContainerStatuses[0].State.Running == nil || !live.Status.ContainerStatuses[0].Ready {
			return false, nil
		}
		result = live
		return true, nil
	})
	return result, err
}

func nativePasswordClientReady(ctx context.Context, clientset kubernetes.Interface, job *batchv1.Job, image nativeRuntimeImage) (*corev1.Pod, error) {
	var result *corev1.Pod
	known := ""
	var original types.UID
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(call context.Context) (bool, error) {
		live, err := clientset.BatchV1().Jobs(job.Namespace).Get(call, job.Name, metav1.GetOptions{})
		if err != nil || live.UID != job.UID || !reflect.DeepEqual(live.Spec, job.Spec) {
			return false, errors.New("client Job replaced")
		}
		pods, err := clientset.CoreV1().Pods(job.Namespace).List(call, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + job.Name})
		if err != nil || len(pods.Items) > 1 {
			return false, errors.New("client Pod inventory changed")
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		pod := pods.Items[0].DeepCopy()
		if !nativePreflightPodMetadataMatches(job, pod) || original != "" && original != pod.UID {
			return false, errors.New("original client Pod changed")
		}
		if original == "" {
			original = pod.UID
		}
		result = pod
		if !nativePreflightPodMatches(job, pod) {
			return false, errors.New("client Pod admission refused")
		}
		id, err := nativePasswordPodStatus(pod, image, known)
		if err != nil {
			return false, err
		}
		known = id
		if id == "" || pod.Status.ContainerStatuses[0].State.Running == nil {
			return false, nil
		}
		result = pod
		return true, nil
	})
	return result, err
}

func nativePasswordContinuity(ctx context.Context, clientset kubernetes.Interface, job *batchv1.Job, pod *corev1.Pod, image nativeRuntimeImage, terminal bool) error {
	live, err := clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || !nativePasswordPodMatches(pod, live) {
		return errors.New("probe Pod continuity lost")
	}
	known := pod.Status.ContainerStatuses[0].ContainerID
	if _, err := nativePasswordPodStatus(live, image, known); err != nil {
		return err
	}
	if job != nil {
		current, err := clientset.BatchV1().Jobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
		if err != nil || current.UID != job.UID || !reflect.DeepEqual(current.Spec, job.Spec) || !nativePreflightPodMatches(job, live) || terminal && !nativeEvidenceWorkerComplete(current) {
			return errors.New("probe Job continuity lost")
		}
	}
	if terminal {
		state := live.Status.ContainerStatuses[0].State.Terminated
		if state == nil || state.ExitCode != 0 || state.Signal != 0 {
			return errors.New("probe terminal state refused")
		}
	} else if live.Status.ContainerStatuses[0].State.Running == nil {
		return errors.New("probe container left running incarnation")
	}
	return nil
}

func nativePasswordClientData(input nativeRuntimeInput, job *batchv1.Job, pod *corev1.Pod, node nativeRuntimeNode, cri map[string]any, resolver map[string]any, image nativeRuntimeImage, major int, clusterIP string, keys nativePasswordKeys, programSHA string, deadline time.Time) (map[string][]byte, error) {
	start, startOK := cri["start_ticks"].(float64)
	pid, pidOK := cri["pid"].(float64)
	namespace, namespaceOK := cri["mount_namespace"].(string)
	if !startOK || !pidOK || !namespaceOK || start <= 0 || start != float64(int64(start)) || pid <= 1 || pid != float64(int64(pid)) || !regexp.MustCompile(`^mnt:\[[0-9]+\]$`).MatchString(namespace) || cri["live_process_verified"] != true || cri["state"] != lifecycleWaitingMain || cri["actual_mounts_verified"] != true || cri["actual_environment_verified"] != true || cri["resource_bounds_verified"] != true {
		return nil, errors.New("private process binding refused")
	}
	host := fmt.Sprintf("pg%d.%s.svc.cluster.local", major, input.Namespace)
	absent := "/usr/share/rpgmigrate/.native-file-absent"
	base := " port=5432 user=native_probe dbname=postgres sslmode=verify-full sslrootcert=/etc/pgcopydb/tls/source/ca.crt sslcert=" + absent + " sslkey=" + absent + " sslcrl=" + absent + " passfile=" + absent + " gssencmode=disable require_auth=scram-sha-256 connect_timeout=5"
	binding := map[string]any{probeTestVersionCommand: 1, lifecycleKindKey: nativePasswordPhase, nativeRuntimeQualificationKey: input.QualificationID, nativeRuntimeRunKey: input.RunID, nativePasswordJobKey: job.UID, nativePasswordPodKey: pod.UID, nativePasswordContainerKey: pod.Status.ContainerStatuses[0].ContainerID, "image_ref": image.Ref, nativeRuntimeImageIDKey: image.Ref, "node_uid": node.UID, "node_container_id": node.ContainerID, nativePasswordSandboxKey: cri[nativePasswordSandboxKey], "resolver_sha256": resolver["sha256"], nativeControlProgramKey: programSHA, "original_deadline": deadline.Unix(), "process_start_ticks": int64(start), "process_pid": int64(pid), "process_mount_namespace": namespace}
	raw, err := json.Marshal(binding)
	if err != nil || len(raw) > 4096 {
		return nil, errors.New("private binding cap")
	}
	return map[string][]byte{"binding.json": raw, "source.uri": []byte("host=" + host + " password=" + keys.password + base), "wrong.uri": []byte("host=" + host + " password=" + keys.wrong + base), "hostname.uri": []byte("host=wrong.invalid hostaddr=" + clusterIP + " password=" + keys.password + base), "ca.crt": keys.ca}, nil
}

func nativePasswordRemove(ctx context.Context, clientset kubernetes.Interface, owned nativeRuntimeOwned) nativePasswordCleanup {
	o := owned.object
	result := nativePasswordCleanup{APIVersion: "v1", Kind: owned.kind, Namespace: o.GetNamespace(), Name: o.GetName(), UID: o.GetUID(), ResourceVersion: o.GetResourceVersion(), Action: nativeRuntimeRetained}
	if owned.kind == lifecycleJobKind {
		result.APIVersion = probeBatchAPI
	}
	if owned.kind == nativePasswordSecretKind {
		err := nativePasswordDeleteSecret(ctx, clientset, o.(*corev1.Secret))
		if err == nil {
			result.Action = nativeRuntimeDeleted
			result.VerifiedAbsent = true
		}
		return result
	}
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(call context.Context) (bool, error) {
		var live metav1.Object
		var err error
		switch owned.kind {
		case nativeRuntimePodKind:
			live, err = clientset.CoreV1().Pods(o.GetNamespace()).Get(call, o.GetName(), metav1.GetOptions{})
		case lifecycleJobKind:
			live, err = clientset.BatchV1().Jobs(o.GetNamespace()).Get(call, o.GetName(), metav1.GetOptions{})
		case nativePasswordServiceKind:
			live, err = clientset.CoreV1().Services(o.GetNamespace()).Get(call, o.GetName(), metav1.GetOptions{})
		case nativeRuntimeNamespaceKind:
			live, err = clientset.CoreV1().Namespaces().Get(call, o.GetName(), metav1.GetOptions{})
		default:
			return false, errors.New("cleanup kind refused")
		}
		if apierrors.IsNotFound(err) {
			result.VerifiedAbsent = true
			return true, nil
		}
		if err != nil || live.GetUID() != o.GetUID() || live.GetResourceVersion() == "" {
			return false, errors.New("cleanup identity changed")
		}
		uid, version := live.GetUID(), live.GetResourceVersion()
		result.ResourceVersion = version
		foreground := metav1.DeletePropagationForeground
		options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}, PropagationPolicy: &foreground}
		switch owned.kind {
		case nativeRuntimePodKind:
			err = clientset.CoreV1().Pods(o.GetNamespace()).Delete(call, o.GetName(), options)
		case lifecycleJobKind:
			err = clientset.BatchV1().Jobs(o.GetNamespace()).Delete(call, o.GetName(), options)
		case nativePasswordServiceKind:
			err = clientset.CoreV1().Services(o.GetNamespace()).Delete(call, o.GetName(), options)
		case nativeRuntimeNamespaceKind:
			err = clientset.CoreV1().Namespaces().Delete(call, o.GetName(), options)
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return false, errors.New("cleanup delete refused")
		}
		return false, nil
	})
	if err == nil && result.VerifiedAbsent {
		result.Action = nativeRuntimeDeleted
	}
	return result
}

func nativePasswordFinish(ctx context.Context, clientset kubernetes.Interface, owned []nativeRuntimeOwned, unknown bool, receipt *nativePasswordReceipt) {
	blocked := unknown
	for _, kind := range []string{nativeRuntimePodKind, lifecycleJobKind, nativePasswordServiceKind, nativePasswordSecretKind, nativeRuntimeNamespaceKind} {
		for _, object := range owned {
			if object.kind != kind {
				continue
			}
			if kind == lifecycleJobKind && !nativeRuntimeChildrenAbsent(ctx, clientset, object) {
				blocked = true
			}
			if blocked && (kind == lifecycleJobKind || kind == nativeRuntimeNamespaceKind) {
				receipt.Cleanup.Objects = append(receipt.Cleanup.Objects, nativePasswordCleanup{APIVersion: map[bool]string{true: probeBatchAPI, false: "v1"}[kind == lifecycleJobKind], Kind: kind, Namespace: object.object.GetNamespace(), Name: object.object.GetName(), UID: object.object.GetUID(), ResourceVersion: object.object.GetResourceVersion(), Action: nativeRuntimeRetained})
				continue
			}
			result := nativePasswordRemove(ctx, clientset, object)
			receipt.Cleanup.Objects = append(receipt.Cleanup.Objects, result)
			if !result.VerifiedAbsent {
				blocked = true
			}
		}
	}
	receipt.Cleanup.Complete = !blocked && len(owned) == 13 && len(receipt.Cleanup.Objects) == 13
	for index := range receipt.Cases {
		receipt.Cases[index].Cleanup = receipt.Cleanup.Complete
	}
	receipt.FinishedAt = float64(time.Now().UnixNano()) / 1e9
}

func nativePasswordScanPod(ctx context.Context, clientset kubernetes.Interface, pod *corev1.Pod, scan *nativePasswordCanaries) error {
	current, err := clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || current.UID != pod.UID {
		return errors.New("output Pod identity changed")
	}
	raw, err := json.Marshal(current)
	if err != nil || scan.check(raw) != nil {
		return errors.New("private Pod status output refused")
	}
	stream, err := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: workerContainer}).Stream(ctx)
	if err != nil {
		return errors.New("output log unavailable")
	}
	raw, readErr := io.ReadAll(io.LimitReader(stream, nativeRuntimeWireLimit+1))
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || len(raw) > nativeRuntimeWireLimit || scan.check(raw) != nil {
		return errors.New("private log closure refused")
	}
	return nil
}

type nativePasswordServerFixture struct {
	pod     *corev1.Pod
	service *corev1.Service
	keys    nativePasswordKeys
	before  map[string]any
	image   nativeRuntimeImage
	profile nativeRuntimeProfileRequest
}

func nativePasswordSecretMatches(expected, actual *corev1.Secret) bool {
	return expected != nil && actual != nil && actual.Immutable != nil && *actual.Immutable && reflect.DeepEqual(expected.Data, actual.Data)
}

type nativePasswordExecution struct {
	job        *batchv1.Job
	pod        *corev1.Pod
	stream     io.ReadCloser
	reader     *bufio.Reader
	before     map[string]any
	readyBytes int
	readyLine  []byte
	profile    nativeRuntimeProfileRequest
	process    map[string]any
}

func nativePasswordPrepareServer(ctx context.Context, clientset kubernetes.Interface, input nativeRuntimeInput, node nativeRuntimeNode, pgImage nativeRuntimeImage, major int, owned *[]nativeRuntimeOwned, unknown *bool, scan *nativePasswordCanaries) (*nativePasswordServerFixture, error) {
	if err := nativePasswordNode(ctx, clientset, input, node); err != nil {
		return nil, err
	}
	host := fmt.Sprintf("pg%d.%s.svc.cluster.local", major, input.Namespace)
	keys, err := nativePasswordCredentials(host)
	if err != nil {
		return nil, errors.New("private credential generation failed")
	}
	scan.values = append(scan.values, []byte(keys.password), []byte(keys.wrong), keys.key, keys.caPrivate)
	for line := range bytes.SplitSeq(append(append([]byte{}, keys.key...), keys.caPrivate...), []byte("\n")) {
		if len(line) >= 32 && !bytes.HasPrefix(line, []byte("-")) {
			scan.values = append(scan.values, line)
		}
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("server-%d", major), Namespace: input.Namespace}, Immutable: ptr.To(true), Data: map[string][]byte{"password": []byte(keys.password), "server.crt": keys.certificate, "server.key": keys.key}}
	createdSecret, err := clientset.CoreV1().Secrets(input.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil || createdSecret.UID == "" || createdSecret.ResourceVersion == "" {
		*unknown = true
		return nil, errors.New("server Secret Create unknown")
	}
	*owned = append(*owned, nativeRuntimeOwned{object: createdSecret, kind: nativePasswordSecretKind})
	if !nativePasswordSecretMatches(secret, createdSecret) {
		return nil, errors.New("private immutable Secret acknowledgment differs")
	}
	expectedServer, err := nativePasswordServer(input, node, pgImage, major)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(expectedServer)
	if scan.check(raw) != nil {
		return nil, errors.New("private server command refused")
	}
	server, err := clientset.CoreV1().Pods(input.Namespace).Create(ctx, expectedServer, metav1.CreateOptions{})
	if err != nil || server.UID == "" {
		*unknown = true
		return nil, errors.New("server Pod Create unknown")
	}
	*owned = append(*owned, nativeRuntimeOwned{object: server, kind: nativeRuntimePodKind})
	if !nativePasswordPodMatches(expectedServer, server) {
		return nil, errors.New("server admission changed fixed specification")
	}
	server, err = nativePasswordServerReady(ctx, clientset, server, pgImage)
	if err != nil {
		return nil, err
	}
	profile, err := nativeRuntimeExpected(ctx, clientset, server, fmt.Sprintf("pg%d", major), nativeRuntimeOwnedServices(*owned))
	if err != nil {
		return nil, err
	}
	beforeServer, err := nativePasswordActual(ctx, clientset, node, server, profile, nil, nil, false)
	if err != nil {
		return nil, err
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pg%d", major), Namespace: input.Namespace}, Spec: corev1.ServiceSpec{Selector: map[string]string{"native-probe-server": fmt.Sprint(major)}, Ports: []corev1.ServicePort{{Port: 5432, TargetPort: intstr.FromInt32(5432), Protocol: corev1.ProtocolTCP}}}}
	service, err = clientset.CoreV1().Services(input.Namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil || service.UID == "" {
		*unknown = true
		return nil, errors.New("Service Create unknown")
	}
	*owned = append(*owned, nativeRuntimeOwned{object: service, kind: nativePasswordServiceKind})
	if net.ParseIP(service.Spec.ClusterIP) == nil {
		return nil, errors.New("owned Service IP absent")
	}
	return &nativePasswordServerFixture{pod: server, service: service, keys: keys, before: beforeServer, image: pgImage, profile: profile}, nil
}

type nativePasswordReady struct {
	Kind     string         `json:"kind"`
	Resolver map[string]any `json:"resolver"`
	Process  map[string]any `json:"process"`
}

func nativePasswordDecodeReady(line []byte) (nativePasswordReady, error) {
	var ready nativePasswordReady
	if nativeRuntimeDecode(line, &ready) != nil || ready.Kind != "ready" || len(ready.Resolver) != 9 || len(ready.Process) != 3 {
		return ready, errors.New("client readiness shape refused")
	}
	return ready, nil
}

func nativePasswordDecodeResult(payload []byte, process map[string]any) (map[string]bool, map[string]any, error) {
	var result map[string]json.RawMessage
	if nativeRuntimeDecode(payload, &result) != nil || len(result) != 7 {
		return nil, nil, errors.New("client primitive result shape refused")
	}
	var kind string
	if json.Unmarshal(result[lifecycleKindKey], &kind) != nil || kind != "result" {
		return nil, nil, errors.New("client result absent")
	}
	var finalProcess map[string]any
	if nativeRuntimeDecode(result["process"], &finalProcess) != nil || !reflect.DeepEqual(finalProcess, process) {
		return nil, nil, errors.New("client final local process proof refused")
	}
	passed := map[string]bool{}
	for _, name := range []string{"good_scram_verify_full", "wrong_password_refused", "hostname_mismatch_refused", "sealed_ca_identity", "private_closure"} {
		var actual bool
		if json.Unmarshal(result[name], &actual) != nil || !actual {
			return nil, nil, errors.New("client primitive did not pass")
		}
		passed[name] = true
	}
	return passed, finalProcess, nil
}

func nativePasswordAuthorizeClient(ctx context.Context, clientset kubernetes.Interface, input nativeRuntimeInput, node nativeRuntimeNode, nativeImage nativeRuntimeImage, fixture *nativePasswordServerFixture, major int, program string, owned *[]nativeRuntimeOwned, unknown *bool, scan *nativePasswordCanaries) (*nativePasswordExecution, error) {
	server, service, keys, pgImage := fixture.pod, fixture.service, fixture.keys, fixture.image
	expected, err := nativePasswordClient(input, node, nativeImage, major, program)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(expected)
	if scan.check(raw) != nil {
		return nil, errors.New("private client command refused")
	}
	job, err := clientset.BatchV1().Jobs(input.Namespace).Create(ctx, expected, metav1.CreateOptions{})
	if err != nil || job.UID == "" {
		*unknown = true
		return nil, errors.New("client Job Create unknown")
	}
	*owned = append(*owned, nativeRuntimeOwned{object: job, kind: lifecycleJobKind})
	if !nativePreflightJobMatches(expected, job) {
		return nil, errors.New("client Job admission changed")
	}
	pod, err := nativePasswordClientReady(ctx, clientset, job, nativeImage)
	if pod != nil {
		*owned = append(*owned, nativeRuntimeOwned{object: pod, kind: nativeRuntimePodKind})
	}
	if err != nil {
		return nil, err
	}
	stream, err := clientset.CoreV1().Pods(input.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: workerContainer, Follow: true}).Stream(ctx)
	if err != nil {
		return nil, errors.New("client readiness stream unavailable")
	}
	transferred := false
	defer func() {
		if !transferred {
			_ = stream.Close()
		}
	}()
	reader := bufio.NewReaderSize(io.LimitReader(stream, nativeRuntimeWireLimit+1), nativeRuntimeWireLimit+1)
	line, err := reader.ReadSlice('\n')
	if err != nil || len(line) > nativeRuntimeWireLimit || scan.check(line) != nil {
		return nil, errors.New("client readiness output refused")
	}
	ready, err := nativePasswordDecodeReady(line)
	if err != nil {
		return nil, err
	}
	profile, err := nativeRuntimeExpected(ctx, clientset, pod, fmt.Sprintf("client%d", major), nativeRuntimeOwnedServices(*owned))
	if err != nil {
		return nil, err
	}
	before, err := nativePasswordActual(ctx, clientset, node, pod, profile, nil, ready.Process, false)
	if err != nil {
		return nil, err
	}
	cri, err := nativeRuntimeCRIFacts(ctx, node, pod, ready.Resolver, before)
	if err != nil {
		return nil, err
	}
	if err = nativePasswordContinuity(ctx, clientset, job, pod, nativeImage, false); err != nil {
		return nil, err
	}
	if err = nativePasswordContinuity(ctx, clientset, nil, server, pgImage, false); err != nil {
		return nil, err
	}
	if err = nativePasswordNode(ctx, clientset, input, node); err != nil {
		return nil, err
	}
	serverNow, err := nativePasswordActual(ctx, clientset, node, server, fixture.profile, fixture.before, nil, false)
	if err != nil || !reflect.DeepEqual(serverNow, fixture.before) {
		return nil, errors.New("server active process continuity refused")
	}
	maps.Copy(cri, before)
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("original probe deadline absent")
	}
	data, err := nativePasswordClientData(input, job, pod, node, cri, ready.Resolver, nativeImage, major, service.Spec.ClusterIP, keys, nativeRuntimeSHA([]byte(program)), deadline)
	if err != nil {
		return nil, err
	}
	inputSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("client-%d", major), Namespace: input.Namespace}, Immutable: ptr.To(true), Data: data}
	inject, end := context.WithTimeout(ctx, 5*time.Second)
	delivered, err := clientset.CoreV1().Secrets(input.Namespace).Create(inject, inputSecret, metav1.CreateOptions{})
	end()
	if err != nil || delivered.UID == "" || delivered.ResourceVersion == "" {
		*unknown = true
		return nil, errors.New("client input Secret Create unknown")
	}
	*owned = append(*owned, nativeRuntimeOwned{object: delivered, kind: nativePasswordSecretKind})
	if !nativePasswordSecretMatches(inputSecret, delivered) {
		return nil, errors.New("private immutable Secret acknowledgment differs")
	}
	transferred = true
	return &nativePasswordExecution{job: job, pod: pod, stream: stream, reader: reader, before: before, readyBytes: len(line), readyLine: append([]byte{}, line...), profile: profile, process: ready.Process}, nil
}

func nativePasswordCollect(ctx context.Context, clientset kubernetes.Interface, input nativeRuntimeInput, node nativeRuntimeNode, nativeImage nativeRuntimeImage, fixture *nativePasswordServerFixture, execution *nativePasswordExecution, scan *nativePasswordCanaries) (map[string]bool, error) {
	job, pod, stream, reader, before := execution.job, execution.pod, execution.stream, execution.reader, execution.before
	server, pgImage, beforeServer := fixture.pod, fixture.image, fixture.before
	var err error
	payload, readErr := io.ReadAll(io.LimitReader(reader, nativeRuntimeWireLimit-int64(execution.readyBytes)+1))
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || len(payload)+execution.readyBytes > nativeRuntimeWireLimit || scan.check(payload) != nil {
		return nil, errors.New("client terminal output refused")
	}
	passed, finalProcess, err := nativePasswordDecodeResult(payload, execution.process)
	if err != nil {
		return nil, err
	}
	terminal, stop := context.WithTimeout(ctx, 5*time.Second)
	closed, replay, collectErr := nativeRuntimePod(terminal, clientset, input, job, nativeImage)
	stop()
	if collectErr != nil || closed == nil || closed.UID != pod.UID || scan.check(replay) != nil || !bytes.Equal(replay, append(append([]byte{}, execution.readyLine...), payload...)) {
		return nil, errors.New("original terminal observation or log closure failed")
	}
	if err = nativePasswordContinuity(ctx, clientset, job, pod, nativeImage, true); err != nil {
		return nil, err
	}
	if err = nativePasswordContinuity(ctx, clientset, nil, server, pgImage, false); err != nil {
		return nil, err
	}
	after, err := nativePasswordActual(ctx, clientset, node, pod, execution.profile, before, finalProcess, true)
	if err != nil || after["terminal_pid_absent"] != true || after["live_process_verified"] != false {
		return nil, errors.New("client CRI incarnation changed")
	}
	afterServer, err := nativePasswordActual(ctx, clientset, node, server, fixture.profile, beforeServer, nil, false)
	if err != nil || !reflect.DeepEqual(beforeServer, afterServer) {
		return nil, errors.New("server CRI incarnation changed")
	}
	if err = nativePasswordScanPod(ctx, clientset, pod, scan); err != nil {
		return nil, err
	}
	if err = nativePasswordScanPod(ctx, clientset, server, scan); err != nil {
		return nil, err
	}
	events, err := clientset.CoreV1().Events(input.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, errors.New("event output unavailable")
	}
	raw, err := json.Marshal(events)
	if err != nil || len(raw) > nativeRuntimeFileLimit || scan.check(raw) != nil {
		return nil, errors.New("private event output refused")
	}
	passed["no_sensitive_persistence"] = true
	return passed, nil
}

func nativePasswordRunPair(ctx context.Context, clientset kubernetes.Interface, input nativeRuntimeInput, node nativeRuntimeNode, nativeImage, pgImage nativeRuntimeImage, major int, program string, owned *[]nativeRuntimeOwned, unknown *bool, scan *nativePasswordCanaries) (map[string]bool, error) {
	fixture, err := nativePasswordPrepareServer(ctx, clientset, input, node, pgImage, major, owned, unknown, scan)
	if err != nil {
		return nil, err
	}
	execution, err := nativePasswordAuthorizeClient(ctx, clientset, input, node, nativeImage, fixture, major, program, owned, unknown, scan)
	if err != nil {
		return nil, err
	}
	return nativePasswordCollect(ctx, clientset, input, node, nativeImage, fixture, execution, scan)
}

func TestNativePackagedPasswordTLSProbe(t *testing.T) {
	path, selected := os.LookupEnv(nativePasswordSelector)
	if !selected {
		t.Skip("fixed password/TLS probe is not selected")
	}
	program, err := nativePasswordProgram()
	if err != nil {
		t.Fatal("fixed probe source unavailable")
	}
	raw, err := nativeRuntimePrivate(path)
	if err != nil {
		t.Fatal("private probe input unavailable")
	}
	var input nativePasswordInput
	var fields map[string]json.RawMessage
	if nativeRuntimeDecode(raw, &input) != nil || json.Unmarshal(raw, &fields) != nil || nativeRuntimeDecode(fields["runtime"], &input.Runtime) != nil || nativePasswordValidateInput(input, path, program, time.Now()) != nil {
		t.Fatal("fixed probe input refused")
	}
	r := input.Runtime
	clientset, err := nativeRuntimeClient(r)
	if err != nil {
		t.Fatal("owned client unavailable")
	}
	nativeImage, err := nativeRuntimeAssets(path, r)
	if err != nil {
		t.Fatal("native image source unavailable")
	}
	images, err := nativePasswordImages(path, r)
	if err != nil {
		t.Fatal("pinned server images unavailable")
	}
	workDeadline := time.Unix(0, int64(r.StageDeadline*1e9))
	cleanupDeadline := time.Unix(0, int64(min(r.StageDeadline+600, r.LeaseExpiry, input.ParentDeadline)*1e9))
	if parent, ok := t.Deadline(); ok && parent.Before(cleanupDeadline.Add(time.Second)) {
		cleanupDeadline = parent.Add(-time.Second)
		workDeadline = minTime(workDeadline, cleanupDeadline.Add(-600*time.Second))
	}
	if !time.Now().Before(workDeadline) {
		t.Fatal("original cleanup horizon unavailable")
	}
	work, cancel := context.WithDeadline(context.Background(), workDeadline)
	defer cancel()
	owned := make([]nativeRuntimeOwned, 0, 13)
	unknown := false
	scan := &nativePasswordCanaries{}
	receipt := nativePasswordReceipt{SchemaVersion: 1, Kind: nativePasswordPhase, QualificationID: r.QualificationID, RunID: r.RunID, InputSHA: nativeRuntimeSHA(raw), SourceSHA: r.QualificationSourceSHA, EvidenceSHA: r.EvidenceProgramSHA, AssetsSHA: r.AssetsSHA, ProgramSHA: input.ProgramSHA, StartedAt: float64(time.Now().UnixNano()) / 1e9, Cases: []nativeRuntimeCase{}}
	defer func() {
		cleanup, end := context.WithDeadline(context.Background(), cleanupDeadline)
		defer end()
		nativePasswordFinish(cleanup, clientset, owned, unknown, &receipt)
		receipt.ScannedBuffers = scan.buffers + 1
		receipt.CanaryHits = scan.hits
		encoded, err := json.Marshal(receipt)
		if err != nil || scan.check(encoded) != nil {
			t.Error("private receipt output refused")
			return
		}
		if nativeRuntimeWrite(r.ReceiptPath, receipt) != nil {
			t.Error("probe receipt persistence failed")
		}
		if !receipt.Cleanup.Complete {
			t.Error("exact thirteen-object cleanup incomplete")
		}
	}()
	if _, err := clientset.CoreV1().Namespaces().Get(work, r.Namespace, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("fresh namespace absence required")
	}
	namespace, err := clientset.CoreV1().Namespaces().Create(work, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.Namespace, Labels: map[string]string{nativeRuntimeRunLabel: r.RunID, nativePasswordQualificationLabel: r.QualificationID}}}, metav1.CreateOptions{})
	if err != nil || namespace.UID == "" {
		unknown = true
		t.Fatal("namespace Create unknown")
	}
	owned = append(owned, nativeRuntimeOwned{object: namespace, kind: nativeRuntimeNamespaceKind})
	receipt.NamespaceUID = namespace.UID
	for index, major := range []int{16, 18} {
		passed, err := nativePasswordRunPair(work, clientset, r, r.Nodes[index], nativeImage, images[major], major, program, &owned, &unknown, scan)
		if err != nil {
			t.Fatal("actual password/TLS primitive probe refused")
		}
		for _, caseName := range nativePasswordCases()[index*6 : (index+1)*6] {
			name := strings.Split(caseName, "/")[1]
			ok := t.Run(caseName, func(t *testing.T) {
				if !passed[name] {
					t.Fatal("actual primitive case absent")
				}
			})
			status, exit := nativeRuntimeRefused, 1
			if ok {
				status, exit = nativeRuntimePassed, 0
			}
			receipt.Cases = append(receipt.Cases, nativeRuntimeCase{Name: caseName, Status: status, ExitCode: exit})
		}
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

type nativePasswordInput struct {
	SchemaVersion  int                `json:"schema_version"`
	Runtime        nativeRuntimeInput `json:"runtime"`
	ProgramSHA     string             `json:"probe_program_sha256"`
	ParentDeadline float64            `json:"original_parent_deadline"`
}

type nativePasswordCleanup struct {
	APIVersion      string    `json:"api_version"`
	Kind            string    `json:"kind"`
	Namespace       string    `json:"namespace"`
	Name            string    `json:"name"`
	UID             types.UID `json:"uid"`
	ResourceVersion string    `json:"resource_version"`
	Action          string    `json:"action"`
	VerifiedAbsent  bool      `json:"verified_absent"`
}

type nativePasswordReceipt struct {
	SchemaVersion   int                 `json:"schema_version"`
	Kind            string              `json:"kind"`
	QualificationID string              `json:"qualification_id"`
	RunID           string              `json:"run_id"`
	InputSHA        string              `json:"input_sha256"`
	SourceSHA       string              `json:"qualification_source_sha256"`
	EvidenceSHA     string              `json:"evidence_program_sha256"`
	AssetsSHA       string              `json:"native_assets_sha256"`
	ProgramSHA      string              `json:"probe_program_sha256"`
	NamespaceUID    types.UID           `json:"namespace_uid"`
	StartedAt       float64             `json:"started_at"`
	FinishedAt      float64             `json:"finished_at"`
	Cases           []nativeRuntimeCase `json:"cases"`
	Cleanup         struct {
		Complete bool                    `json:"complete"`
		Objects  []nativePasswordCleanup `json:"objects"`
	} `json:"cleanup"`
	ScannedBuffers int `json:"scanned_buffers"`
	CanaryHits     int `json:"canary_hits"`
}

type nativePasswordCanaries struct {
	values  [][]byte
	buffers int
	hits    int
}

func (scan *nativePasswordCanaries) check(raw []byte) error {
	scan.buffers++
	for _, value := range scan.values {
		if len(value) > 0 && bytes.Contains(raw, value) {
			scan.hits++
		}
	}
	if scan.hits != 0 {
		return errors.New("private canary reached an output buffer")
	}
	return nil
}

type nativePasswordKeys struct {
	password    string
	wrong       string
	ca          []byte
	certificate []byte
	key         []byte
	caPrivate   []byte
}

func nativePasswordCredentials(host string) (nativePasswordKeys, error) {
	var result nativePasswordKeys
	password, wrong := make([]byte, 24), make([]byte, 24)
	if _, err := rand.Read(password); err != nil {
		return result, err
	}
	if _, err := rand.Read(wrong); err != nil {
		return result, err
	}
	result.password = "probe_" + hex.EncodeToString(password)
	result.wrong = "wrong_" + hex.EncodeToString(wrong)
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return result, err
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return result, err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "native-probe-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	encoded, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return result, err
	}
	result.ca = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: encoded})
	caPrivate, err := x509.MarshalPKCS8PrivateKey(caKey)
	if err != nil {
		return result, err
	}
	result.caPrivate = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: caPrivate})
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: host}, DNSNames: []string{host}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	encoded, err = x509.CreateCertificate(rand.Reader, leaf, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		return result, err
	}
	result.certificate = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: encoded})
	encoded, err = x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return result, err
	}
	result.key = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
	return result, nil
}

func nativePasswordServerCommand(major int) []string {
	binary := fmt.Sprintf("/usr/lib/postgresql/%d/bin/", major)
	return []string{shellPath, "-ec", binary + "initdb -D /var/lib/postgresql/probe-data --username=native_probe --pwfile=/run/server-input/password --auth-local=reject --auth-host=scram-sha-256 --encoding=UTF8 --no-locale; exec " + binary + "postgres -D /var/lib/postgresql/probe-data -c listen_addresses='*' -c unix_socket_directories=/tmp -c ssl=on -c ssl_cert_file=/run/server-input/server.crt -c ssl_key_file=/run/server-input/server.key -c password_encryption=scram-sha-256 -c log_min_error_statement=panic"}
}

func nativePasswordServer(input nativeRuntimeInput, node nativeRuntimeNode, image nativeRuntimeImage, major int) (*corev1.Pod, error) {
	job, err := nativeRuntimeJob(input, node, image, major)
	if err != nil {
		return nil, err
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pg%d", major), Namespace: input.Namespace, Labels: map[string]string{nativeRuntimeRunLabel: input.RunID, nativePasswordQualificationLabel: input.QualificationID, "native-probe-server": fmt.Sprint(major)}}, Spec: *job.Spec.Template.Spec.DeepCopy()}
	uid, yes, no, grace, priority, mode := int64(999), true, false, int64(30), int32(0), int32(0o440)
	pod.Spec.HostUsers = &yes
	pod.Spec.SecurityContext.RunAsUser = &uid
	pod.Spec.SecurityContext.RunAsGroup = &uid
	pod.Spec.SecurityContext.FSGroup = &uid
	pod.Spec.ServiceAccountName = nativePreflightDefaultSA
	pod.Spec.DeprecatedServiceAccount = nativePreflightDefaultSA
	pod.Spec.EnableServiceLinks = &yes
	pod.Spec.SchedulerName = corev1.DefaultSchedulerName
	pod.Spec.TerminationGracePeriodSeconds = &grace
	pod.Spec.Priority = &priority
	preemption := corev1.PreemptLowerPriority
	pod.Spec.PreemptionPolicy = &preemption
	pod.Spec.Tolerations = []corev1.Toleration{}
	seconds := int64(300)
	for _, key := range []string{"node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"} {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds})
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: nativePasswordServerVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: fmt.Sprintf("server-%d", major), DefaultMode: &mode, Optional: &no}}}, corev1.Volume{Name: "pgdata", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(512*1024*1024, resource.BinarySI)}}})
	c := &pod.Spec.Containers[0]
	c.Command = nativePasswordServerCommand(major)
	c.Args = nil
	c.Env = nil
	c.ImagePullPolicy = corev1.PullIfNotPresent
	c.TerminationMessagePath = corev1.TerminationMessagePathDefault
	c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: nativePasswordServerVolume, MountPath: nativePasswordServerMount, ReadOnly: true}, corev1.VolumeMount{Name: "pgdata", MountPath: "/var/lib/postgresql"})
	c.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(5432)}}, PeriodSeconds: 1, TimeoutSeconds: 1, SuccessThreshold: 1, FailureThreshold: 3}
	return pod, nil
}

func nativePasswordClient(input nativeRuntimeInput, node nativeRuntimeNode, image nativeRuntimeImage, major int, program string) (*batchv1.Job, error) {
	job, err := nativeRuntimeJob(input, node, image, major)
	if err != nil {
		return nil, err
	}
	job.Name = fmt.Sprintf("client-%d", major)
	container := &job.Spec.Template.Spec.Containers[0]
	container.Command = nativePasswordCommand(program)
	mode, yes := int32(0o440), true
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{Name: nativePasswordClientVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: fmt.Sprintf("client-%d", major), DefaultMode: &mode, Optional: &yes}}})
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: nativePasswordClientVolume, MountPath: "/etc/pgcopydb/tls/source", ReadOnly: true})
	container.Env = []corev1.EnvVar{{Name: "NATIVE_PROBE_ID", Value: input.QualificationID}, {Name: "NATIVE_PROBE_RUN", Value: input.RunID}, {Name: "NATIVE_PROBE_IMAGE", Value: image.Ref}, {Name: "NATIVE_PROBE_PROGRAM", Value: nativeRuntimeSHA([]byte(program))}, {Name: nativePasswordUIDEnvironment, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.uid"}}}}
	container.Env = append([]corev1.EnvVar{{Name: nativeLCAllEnv, Value: "C"}, {Name: nativeLangEnv, Value: "C"}}, container.Env...)
	raw, err := json.Marshal(job)
	if err != nil || len(raw) > nativeRuntimeFileLimit {
		return nil, errors.New("client Job cap")
	}
	return job, nil
}

const nativePasswordPhase = "password_tls_probe"
const nativePasswordSelector = "RPGMIGRATE_NATIVE_PASSWORD_TLS_INPUT"

const nativePasswordTLSProgram = `import hashlib,ipaddress,json,os,resource,stat,sys,time
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
resource.setrlimit(resource.RLIMIT_CPU,(8,8))
resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
P=canonical
def need(value):
 if not value:raise RuntimeError('probe_refused')
def mounts():
 raw=open('/proc/self/mountinfo','rb').read(65537);need(len(raw)<=65536)
 rows=[]
 for line in raw.decode('ascii').splitlines():
  f=line.split();rows.append((f[4],f[5].split(',')))
 return raw,rows
def image(path,rows):
 need(os.path.realpath(path)==path and any(p=='/' and 'ro' in flags for p,flags in rows))
 need(not any(p!='/' and (path==p or path.startswith(p.rstrip('/')+'/')) for p,_ in rows))
 f=P.StableFile(path,16777216);i=f.before
 need(i.st_uid==0 and i.st_gid==0 and not i.st_mode&0o022)
 return f
def resolver_shape(raw):
 need(raw and len(raw)<=65536 and all(c==10 or 32<=c<127 for c in raw))
 search=[];servers=[];options=[]
 for line in raw.decode('ascii').splitlines():
  words=line.split();need(words)
  if words[0]=='search':search.append(words[1:])
  elif words[0]=='nameserver':need(len(words)==2);ipaddress.ip_address(words[1]);servers.append(words[1])
  elif words[0]=='options':options.append(words[1:])
  else:raise RuntimeError('probe_refused')
 need(len(search)==1 and search[0] and 1<=len(servers)<=3 and options==[['ndots:5']] and sum(len(x.encode())+1 for x in search[0][:6])<=256)
def private_info(info):
 need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_gid==65532 and stat.S_IMODE(info.st_mode)==0o440 and info.st_nlink==1)
def private(path,cap):
 return P.StableFile(path,cap,opener=lambda original:P.open_connection_file(os.fsencode(original),cwd),validate=private_info)
def self_process():
 need(os.geteuid()==65532 and os.getegid()==65532 and sorted(set(os.getgroups()+[os.getegid()]))==[65532])
 state={}
 for line in open('/proc/self/status').read(65537).splitlines():
  key,value=line.split(':',1);state[key]=value.strip()
 need(state.get('NoNewPrivs')=='1' and state.get('Seccomp')=='2')
 for key in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb'):need(int(state[key],16)==0)
 fields=open('/proc/self/stat','rb').read(16385).rsplit(b') ',1)[1].split()
 return dict(pid=os.getpid(),start_ticks=int(fields[19]),mount_namespace=os.readlink('/proc/self/ns/mnt'))
def local_closure():
 need(mounts()[0]==original_mounts)
 for file in files+inputs:file.check()
 return self_process()
start=time.monotonic();limit=start+30;files=[];inputs=[];cwd=None;diag=None
try:
 need(os.geteuid()==65532 and os.getegid()==65532 and os.uname().machine=='aarch64');P.validate_environment(os.environ)
 original_mounts,rows=mounts()
 expected={'/etc/passwd':'21352194cc533bc5878721507450d867d28ccb1c2f5cd773c792251fa1e63185','/etc/nsswitch.conf':'eec30745bade42a3f3f792e4d4192e57d2bcfe8e472433b1de426fe39a39cddb','/usr/share/rpgmigrate/python-libpq-runtime.json':'122ec42c730898d3f9680b394b0a521ef6f808e29c6db013c0c495caa34dab44','/usr/lib/aarch64-linux-gnu/libpq.so.5.18':'094cbe08c8c6134b91b1946e42012318728003cf2dd2fcc0ab1ee9821ba38f58'}
 for path,digest in expected.items():
  file=image(path,rows);files.append(file);need(hashlib.sha256(file.raw).hexdigest()==digest)
 for path in ('/etc/ssl/openssl.cnf','/usr/lib/aarch64-linux-gnu/libc.so.6','/usr/bin/python3.13'):files.append(image(path,rows))
 resolver=P.StableFile('/etc/resolv.conf',65536);files.append(resolver);resolver_shape(resolver.raw)
 info=resolver.before;need(info.st_uid==0 and info.st_gid==0 and stat.S_IMODE(info.st_mode)==0o644)
 need(sum(path=='/etc/resolv.conf' and 'ro' in flags for path,flags in rows)==1)
 need(any(path=='/etc/pgcopydb/tls/source' and 'ro' in flags for path,flags in rows))
 ready=dict(device=info.st_dev,inode=info.st_ino,uid=info.st_uid,gid=info.st_gid,mode=stat.S_IMODE(info.st_mode),links=info.st_nlink,size=len(resolver.raw),sha256=hashlib.sha256(resolver.raw).hexdigest(),bytes=resolver.raw.decode('ascii'))
 process=local_closure()
 print(json.dumps(dict(kind='ready',resolver=ready,process=process),separators=(',',':')),flush=True)
 cwd=P.fdopen('.',True)
 while True:
  need(time.monotonic()<limit)
  try:binding=private('/etc/pgcopydb/tls/source/binding.json',4096);break
  except FileNotFoundError:time.sleep(min(.02,max(0,limit-time.monotonic())))
 inputs.append(binding);value=P.strict_json(binding.raw,4096)
 P.keys(value,'version kind qualification_id run_id job_uid pod_uid container_id image_ref image_id node_uid node_container_id sandbox_id resolver_sha256 program_sha256 original_deadline process_start_ticks process_pid process_mount_namespace')
 need(value['version']==1 and value['kind']=='password_tls_probe' and value['qualification_id']==os.environ['NATIVE_PROBE_ID'] and value['run_id']==os.environ['NATIVE_PROBE_RUN'] and value['pod_uid']==os.environ['NATIVE_PROBE_POD_UID'])
 need(value['image_ref']==value['image_id']==os.environ['NATIVE_PROBE_IMAGE'] and value['resolver_sha256']==ready['sha256'] and value['program_sha256']==os.environ['NATIVE_PROBE_PROGRAM'])
 need(type(value['original_deadline']) is int and time.time()<value['original_deadline'])
 need(all(isinstance(value[k],str) and value[k] for k in ('job_uid','container_id','node_uid','node_container_id','sandbox_id')))
 need(type(value['process_start_ticks']) is int and type(value['process_pid']) is int and value['process_pid']>1 and value['process_start_ticks']==process['start_ticks'] and value['process_mount_namespace']==process['mount_namespace'] and local_closure()==process)
 payload=[]
 for name in ('source.uri','wrong.uri','hostname.uri'):
  file=private('/etc/pgcopydb/tls/source/'+name,65536);inputs.append(file);need(file.raw and b'\0' not in file.raw);payload.append(file.raw)
 need(len(set(payload))==3)
 library=P.load_runtime();diag=P.Diagnostics();success=[];sealed=False
 deadline=min(time.monotonic()+25,time.monotonic()+value['original_deadline']-time.time())
 for number,raw in enumerate(payload):
  need(mounts()[0]==original_mounts)
  for file in files+inputs:file.check()
  os.environ['PGCOPYDB_SOURCE_PGURI']=raw.decode('utf-8')
  material=P.Materialization(os.environ,source_only=True);session=None
  try:
   material.prepare(library,deadline);connection=material.connection('source')
   need(len(connection.snapshots)==1)
   for snapshot in connection.snapshots:snapshot.check()
   sealed=True
   need(local_closure()==process)
   try:session=P.Session(library,connection,deadline,diag)
   except P.Refusal as error:
    need(number>0 and error.error_class=='connection');success.append(True)
   else:
    need(number==0 and session.props['ssl']==1)
    result=session.query('SELECT current_user,current_database(),ssl FROM pg_catalog.pg_stat_ssl WHERE pid=pg_catalog.pg_backend_pid()',width=3)
    need(result==[[b'native_probe',b'postgres',b't']]);success.append(True)
    session.close();session=None
  finally:
   if session is not None:session.close(False)
   material.close()
 need(success==[True,True,True] and sealed)
 for file in files+inputs:file.check()
 need(mounts()[0]==original_mounts)
 diag.close();diag=None
 for file in reversed(files+inputs):file.close()
 files=[];inputs=[];P.checked_close(cwd);cwd=None
 need(local_closure()==process)
 print(json.dumps(dict(kind='result',process=process,good_scram_verify_full=True,wrong_password_refused=True,hostname_mismatch_refused=True,sealed_ca_identity=True,private_closure=True),separators=(',',':')),flush=True)
except BaseException:
 for file in reversed(files+inputs):
  try:file.close()
  except BaseException:pass
 if cwd is not None:
  try:P.checked_close(cwd)
  except BaseException:pass
 if diag is not None:
  try:diag.close()
  except BaseException:pass
 raise SystemExit(1)
`

// The canonical helper travels as its own argument, so each argument keeps its own cap.
func nativePasswordProgram() (string, error) {
	evidence := native.EvidenceProgramSHA256()
	program := "import hashlib,sys,types\nSOURCE=sys.argv[1]\nif hashlib.sha256(SOURCE.encode()).hexdigest()!='" + hex.EncodeToString(evidence[:]) + "':raise SystemExit(2)\ncanonical=types.ModuleType('native_runtime_probe_canonical')\nexec(compile(SOURCE,'<canonical-native-evidence>','exec'),canonical.__dict__)\n" + nativePasswordTLSProgram
	if len(program) > native.EvidenceProgramSourceLimit || len(program)+1 > native.EvidenceMainArgumentLimit || len(native.EvidenceProgram())+1 > native.EvidenceMainArgumentLimit {
		return "", errors.New("password TLS fixed source cap")
	}
	return program, nil
}

func nativePasswordCommand(program string) []string {
	return []string{nativeRuntimeEnvBinary, "-u", nativeRuntimePreload, "-u", nativeRuntimeLibraryPath, nativeRuntimePython, "-I", "-B", "-c", program, string(native.EvidenceProgram())}
}

func nativePasswordCases() []string {
	cases := make([]string, 0, 12)
	for _, major := range []int{16, 18} {
		for _, name := range []string{"good_scram_verify_full", "wrong_password_refused", "hostname_mismatch_refused", "sealed_ca_identity", "private_closure", "no_sensitive_persistence"} {
			cases = append(cases, fmt.Sprintf("pg%d/%s", major, name))
		}
	}
	return cases
}

func nativePasswordDeleteSecret(ctx context.Context, clientset kubernetes.Interface, secret *corev1.Secret) error {
	if secret.UID == "" || secret.ResourceVersion == "" {
		return errors.New("Secret Create identity missing")
	}
	uid, version := secret.UID, secret.ResourceVersion
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}
	for {
		err := clientset.CoreV1().Secrets(secret.Namespace).Delete(ctx, secret.Name, options)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return errors.New("fixed Secret deletion refused")
		}
		select {
		case <-ctx.Done():
			return errors.New("Secret absence deadline")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestNativePasswordTLSProgramBounds(t *testing.T) {
	program, err := nativePasswordProgram()
	command := nativePasswordCommand(program)
	if err != nil || len(program) > native.EvidenceProgramSourceLimit || len(program)+1 > native.EvidenceMainArgumentLimit || bytes.Contains([]byte(program), native.EvidenceProgram()) || command[len(command)-1] != string(native.EvidenceProgram()) {
		t.Fatal("full canonical source or fixed probe argument bound refused")
	}
	if len(nativePasswordCases()) != 12 {
		t.Fatal("fixed primitive case inventory disappeared")
	}
}

func TestNativePasswordTLSSecretDeleteUsesOnlyFixedCreateIdentity(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "input", Namespace: "delete-fixture", UID: "created", ResourceVersion: "7"}}
	clientset := kubefake.NewClientset(secret)
	if err := nativePasswordDeleteSecret(context.Background(), clientset, secret); err != nil {
		t.Fatal(err)
	}
	if len(clientset.Actions()) != 2 {
		t.Fatal("Secret absence was not proved by DELETE then NotFound")
	}
	for _, action := range clientset.Actions() {
		deleted, ok := action.(ktesting.DeleteAction)
		if !ok || action.GetVerb() != "delete" || action.GetResource().Resource != "secrets" {
			t.Fatal("Secret read or unexpected operation occurred")
		}
		options := deleted.GetDeleteOptions()
		if options.Preconditions == nil || *options.Preconditions.UID != secret.UID || *options.Preconditions.ResourceVersion != "7" {
			t.Fatal("Secret Create identity was refreshed or lost")
		}
	}
	clientset = kubefake.NewClientset(secret)
	clientset.PrependReactor("delete", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, "input", nil)
	})
	if nativePasswordDeleteSecret(context.Background(), clientset, secret) == nil || len(clientset.Actions()) != 1 {
		t.Fatal("conflicted Secret deletion was retried or accepted")
	}
}

func TestNativePasswordTLSFixedFixtureAndTerminalIncarnation(t *testing.T) {
	input := nativeRuntimeInput{QualificationID: strings.Repeat("1", 32), RunID: strings.Repeat("2", 32), Namespace: "owned"}
	image := nativeRuntimeImage{Ref: "localhost/rpgmigrate@sha256:" + strings.Repeat("3", 64), Digest: "sha256:" + strings.Repeat("3", 64), ConfigDigest: "sha256:" + strings.Repeat("4", 64)}
	server, err := nativePasswordServer(input, nativeRuntimeNode{Name: nativePasswordOwnedNode}, image, 16)
	if err != nil {
		t.Fatal(err)
	}
	if *server.Spec.SecurityContext.RunAsUser != 999 || *server.Spec.SecurityContext.RunAsGroup != 999 || *server.Spec.SecurityContext.FSGroup != 999 || *server.Spec.AutomountServiceAccountToken || !*server.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem || server.Spec.RuntimeClassName != nil || server.Spec.Volumes[1].Secret == nil || *server.Spec.Volumes[1].Secret.DefaultMode != 0o440 || server.Spec.Volumes[2].EmptyDir == nil || len(server.Spec.Containers[0].Env) != 0 {
		t.Fatal("fixed server ownership or private-file fixture changed")
	}
	program, err := nativePasswordProgram()
	if err != nil {
		t.Fatal(err)
	}
	job, err := nativePasswordClient(input, nativeRuntimeNode{Name: nativePasswordOwnedNode}, image, 16, program)
	if err != nil {
		t.Fatal(err)
	}
	if len(job.Spec.Template.Spec.Volumes) != 2 || job.Spec.Template.Spec.Volumes[1].Secret == nil || !*job.Spec.Template.Spec.Volumes[1].Secret.Optional || *job.Spec.Template.Spec.Volumes[1].Secret.DefaultMode != 0o440 {
		t.Fatal("late private projection changed")
	}
	image.NodeBindings = map[string]nativeRuntimeImageBinding{nativePasswordOwnedNode: {RequestedRef: image.Ref, ManifestDigest: image.Digest, ConfigDigest: image.ConfigDigest, AcceptedImageIDs: []string{image.Ref}}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{NodeName: nativePasswordOwnedNode}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer, ContainerID: nativePasswordOriginalContainer, ImageID: image.Ref, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: nativePasswordReplacementContainer, ExitCode: 0}}}}}}
	valid := pod.DeepCopy()
	valid.Status.ContainerStatuses[0].State.Terminated.ContainerID = nativePasswordOriginalContainer
	if _, err := nativePasswordPodStatus(valid, image, nativePasswordOriginalContainer); err != nil {
		t.Fatal("healthy bound terminal incarnation refused", err)
	}
	if _, err := nativePasswordPodStatus(pod, image, nativePasswordOriginalContainer); err == nil {
		t.Fatal("terminal container identity was not bound")
	}
}

func TestNativePasswordTLSCanariesNeverReachOutput(t *testing.T) {
	scan := &nativePasswordCanaries{values: [][]byte{[]byte("generated-private-canary")}}
	if scan.check([]byte("fixed result")) != nil || scan.buffers != 1 || scan.hits != 0 {
		t.Fatal("safe output scan failed")
	}
	if scan.check([]byte("prefix generated-private-canary suffix")) == nil || scan.hits != 1 {
		t.Fatal("sensitive output passed")
	}
}

func TestNativePasswordTLSClosedInputRetainsOriginalParentHorizon(t *testing.T) {
	program, err := nativePasswordProgram()
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("1", 64)
	evidence := native.EvidenceProgramSHA256()
	r := nativeRuntimeInput{SchemaVersion: 1, QualificationID: strings.Repeat("1", 32), Phase: nativePasswordPhase, RunID: strings.Repeat("2", 32), RunDir: "/private/owned-run", LegacySourceRoot: "/private/legacy-source", LegacySourceSHA: digest, LegacyGuardSHA: digest, LegacyOwnershipSHA: digest, QualificationSourceSHA: digest, EvidenceProgramSHA: hex.EncodeToString(evidence[:]), AssetsSHA: digest, Kubeconfig: "/private/owned-run/kubeconfig", Context: "kind-rpgmigrate-m7-" + strings.Repeat("2", 32), Namespace: "native-password-tls-" + strings.Repeat("1", 16), StageDeadline: 1600, LeaseExpiry: 2200, RequiredCases: nativePasswordCases(), ReceiptPath: "/private/qual/password-tls-receipt.json", MetadataProgramSHA: nativeRuntimeSHA([]byte(nativeRuntimeMetadataProgram + "\x00" + nativeRuntimeNodeProgram))}
	for index := range 3 {
		r.Nodes = append(r.Nodes, nativeRuntimeNode{Name: fmt.Sprintf("node-%d", index), ContainerID: strings.Repeat(fmt.Sprint(index+1), 64), UID: types.UID(fmt.Sprintf("uid-%d", index))})
	}
	input := nativePasswordInput{SchemaVersion: 1, Runtime: r, ProgramSHA: nativeRuntimeSHA([]byte(program)), ParentDeadline: 2200}
	path := "/private/qual/password-tls-input.json"
	if nativePasswordValidateInput(input, path, program, time.Unix(1000, 0)) != nil {
		t.Fatal("fixed original probe input refused")
	}
	for _, change := range []string{"parent", "program_binding", "phase_selection", "cases", nativeRuntimeNodeMode, nativeTestNamespace} {
		candidate := input
		switch change {
		case "parent":
			candidate.ParentDeadline = 2199
		case "program_binding":
			candidate.ProgramSHA = digest
		case "phase_selection":
			candidate.Runtime.Phase = "connections"
		case "cases":
			candidate.Runtime.RequiredCases = nil
		case nativeRuntimeNodeMode:
			candidate.Runtime.Nodes = nil
		case nativeTestNamespace:
			uid := types.UID("existing")
			candidate.Runtime.NamespaceUID = &uid
		}
		if nativePasswordValidateInput(candidate, path, program, time.Unix(1000, 0)) == nil {
			t.Fatal("changed authority or original horizon accepted")
		}
	}
}

func TestNativePasswordTLSUnknownCreateRetainsNamespace(t *testing.T) {
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "owned", UID: "original", ResourceVersion: "7"}}
	clientset := kubefake.NewClientset(namespace)
	receipt := &nativePasswordReceipt{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	nativePasswordFinish(ctx, clientset, []nativeRuntimeOwned{{object: namespace, kind: nativeRuntimeNamespaceKind}}, true, receipt)
	if receipt.Cleanup.Complete || len(receipt.Cleanup.Objects) != 1 || receipt.Cleanup.Objects[0].Action != nativeRuntimeRetained || receipt.Cleanup.Objects[0].UID != namespace.UID || len(clientset.Actions()) != 0 {
		t.Fatal("unknown Create permitted parent deletion or completion")
	}
}

func TestNativePasswordTLSDirectPodMetadataCannotDrift(t *testing.T) {
	image := nativeRuntimeImage{Ref: "localhost/rpgmigrate@sha256:" + strings.Repeat("1", 64)}
	id := "containerd://" + strings.Repeat("2", 64)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: nativeRuntimePG16, Namespace: "primitive-fixture", UID: "primitive-original-uid"}, Spec: corev1.PodSpec{NodeName: nativePasswordOwnedNode}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer, ContainerID: id, ImageID: image.Ref, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	image.NodeBindings = map[string]nativeRuntimeImageBinding{nativePasswordOwnedNode: {RequestedRef: image.Ref, ManifestDigest: image.Digest, ConfigDigest: image.ConfigDigest, AcceptedImageIDs: []string{image.Ref}}}
	if err := nativePasswordContinuity(context.Background(), kubefake.NewClientset(pod), nil, pod, image, false); err != nil {
		t.Fatal("healthy bound direct Pod refused", err)
	}
	for index, mutate := range []func(*corev1.Pod){func(p *corev1.Pod) { p.Labels = map[string]string{nativeRuntimeRunLabel: "foreign-label"} }, func(p *corev1.Pod) {
		p.Annotations = map[string]string{nativePasswordUnexpectedKey: "foreign-annotation"}
	}, func(p *corev1.Pod) { p.Finalizers = []string{"foreign-finalizer"} }, func(p *corev1.Pod) { p.OwnerReferences = []metav1.OwnerReference{{UID: nativePasswordForeignOwner}} }} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			live := pod.DeepCopy()
			mutate(live)
			clientset := kubefake.NewClientset(live)
			if nativePasswordContinuity(context.Background(), clientset, nil, pod, image, false) == nil {
				t.Fatal("direct Pod metadata drift was admitted")
			}
		})
	}
}

func TestNativePasswordTLSPrivateSecretAcknowledgmentIsExact(t *testing.T) {
	desired := &corev1.Secret{Immutable: ptr.To(true), Data: map[string][]byte{"key": []byte("fixture-bytes")}}
	created := desired.DeepCopy()
	if !nativePasswordSecretMatches(desired, created) {
		t.Fatal("unchanged immutable input was refused")
	}
	created.Data["key"] = []byte("changed-fixture")
	if nativePasswordSecretMatches(desired, created) {
		t.Fatal("changed private payload was accepted")
	}
	created = desired.DeepCopy()
	created.Immutable = ptr.To(false)
	if nativePasswordSecretMatches(desired, created) {
		t.Fatal("mutable private input was accepted")
	}
}
