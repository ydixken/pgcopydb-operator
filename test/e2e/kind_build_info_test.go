package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	clientspdy "k8s.io/client-go/transport/spdy"
	"k8s.io/streaming/pkg/httpstream"
	streamspdy "k8s.io/streaming/pkg/httpstream/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const kindManagerContainer = "manager"
const kindManagerMetricsPath = "/metrics"
const kindManagerMetricsVerb = "get"
const kindManagerHTTPS = "https"

const kindManagerBuildMetric = "pgcopydb_operator_build_info"
const kindManagerMetricsLimit = 2 * 1024 * 1024

var kindBuildVersionPattern = regexp.MustCompile(`^m7-[0-9a-f]{12}$`)
var errKindManagerBuildInfo = errors.New("kind manager build info unavailable")
var errKindManagerMetricsPeer = errors.New("kind manager metrics peer changed")

func kindRuntimeText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsFunc(value,
		func(c rune) bool { return unicode.IsControl(c) || unicode.IsSpace(c) })
}

func parseKindManagerBuildInfo(body []byte, expectedVersion string) (string, error) {
	if len(body) == 0 || len(body) > kindManagerMetricsLimit || body[len(body)-1] != '\n' ||
		!kindBuildVersionPattern.MatchString(expectedVersion) {
		return "", errKindManagerBuildInfo
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return "", errKindManagerBuildInfo
	}
	family := families[kindManagerBuildMetric]
	if family == nil || family.GetType() != dto.MetricType_GAUGE || len(family.Metric) != 1 {
		return "", errKindManagerBuildInfo
	}
	metric := family.Metric[0]
	if metric.Gauge == nil || len(metric.Label) != 1 || metric.Label[0].GetName() != "version" ||
		metric.Label[0].GetValue() != expectedVersion {
		return "", errKindManagerBuildInfo
	}
	value := metric.Gauge.GetValue()
	if math.IsNaN(value) || math.IsInf(value, 0) || value != 1 {
		return "", errKindManagerBuildInfo
	}
	return metric.Label[0].GetValue(), nil
}

func readKindManagerBuildInfo(ctx context.Context, baseURL, bearer, expectedVersion string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil || base.Scheme != kindManagerHTTPS || base.Hostname() != "127.0.0.1" || base.Port() == "" ||
		base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.Path != "" || !kindRuntimeText(bearer,
		65536) || !kindBuildVersionPattern.MatchString(expectedVersion) {
		return "", errKindManagerBuildInfo
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	transport := kindManagerMetricsTransport()
	defer transport.CloseIdleConnections()
	metricsClient := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request,
		[]*http.Request) error {
		return errKindManagerBuildInfo
	}}
	base.Path = kindManagerMetricsPath
	attempts := 0
	observed := ""
	err = wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		attempts++
		version, transient, scrapeErr := scrapeKindManagerBuildInfo(ctx, metricsClient, base.String(), bearer,
			expectedVersion)
		if scrapeErr == nil {
			observed = version
			return true, nil
		}
		if !transient || attempts >= 3 {
			return false, errKindManagerBuildInfo
		}
		return false, nil
	})
	if err != nil || ctx.Err() != nil {
		return "", errKindManagerBuildInfo
	}
	return observed, nil
}

func kindManagerMetricsTransport() *http.Transport {
	var lock sync.Mutex
	var pin [32]byte
	seen := false
	verify := func(state tls.ConnectionState) error {
		lock.Lock()
		defer lock.Unlock()
		if len(state.PeerCertificates) == 0 {
			return errKindManagerMetricsPeer
		}
		current := sha256.Sum256(state.PeerCertificates[0].Raw)
		if seen && current != pin {
			return errKindManagerMetricsPeer
		}
		seen, pin = true, current
		return nil
	}
	return &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second,
		KeepAlive: 5 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second, IdleConnTimeout: 5 * time.Second,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12,
			// Only the exact loopback Pod tunnel accepts its first self-signed metrics certificate.
			InsecureSkipVerify: true, VerifyConnection: verify,
		}}
}

func scrapeKindManagerBuildInfo(ctx context.Context, metricsClient *http.Client, endpoint, bearer,
	expectedVersion string) (string, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", false, errKindManagerBuildInfo
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	response, err := metricsClient.Do(request)
	if err != nil {
		return "", !errors.Is(err, errKindManagerMetricsPeer) && !errors.Is(err, errKindManagerBuildInfo) &&
			ctx.Err() == nil, errKindManagerBuildInfo
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		transient := response.StatusCode == http.StatusTooManyRequests ||
			response.StatusCode == http.StatusBadGateway || response.StatusCode == http.StatusServiceUnavailable ||
			response.StatusCode == http.StatusGatewayTimeout
		return "", transient, errKindManagerBuildInfo
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, kindManagerMetricsLimit+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(body) > kindManagerMetricsLimit {
		return "", false, errKindManagerBuildInfo
	}
	observed, err := parseKindManagerBuildInfo(body, expectedVersion)
	return observed, false, err
}

type kindManagerRuntimeEvidence struct {
	ExpectedVersion     string  `json:"expected_version"`
	ObservedVersion     string  `json:"observed_version"`
	Authenticated       bool    `json:"authenticated"`
	NamespaceUID        string  `json:"namespace_uid"`
	DeploymentUID       string  `json:"deployment_uid"`
	ReplicaSetUID       string  `json:"replicaset_uid"`
	PodUID              string  `json:"pod_uid"`
	NodeName            string  `json:"node_name"`
	ImageRef            string  `json:"image_ref"`
	ImageConfigID       string  `json:"image_config_id"`
	ImageManifestDigest string  `json:"image_manifest_digest"`
	ContainerImageID    string  `json:"container_image_id"`
	ContainerID         string  `json:"container_id"`
	RestartCount        int32   `json:"restart_count"`
	ObservedAt          float64 `json:"observed_at"`
	Cleanup             bool    `json:"cleanup"`
}

type kindManagerIdentity struct {
	namespaceUID, deploymentUID, replicaSetUID, podUID, nodeUID types.UID
	podName, nodeName, containerID, imageID                     string
	restarts                                                    int32
}

type kindMetricsAuthObject struct {
	want                 client.Object
	uid                  types.UID
	version              string
	attempted, uncertain bool
}

type kindManagerCleanupHandoff struct{}

func (kindManagerCleanupHandoff) Error() string {
	return "kind manager authorization cleanup requires handoff"
}

var kindManagerRuntime kindManagerRuntimeEvidence
var kindManagerMetricsForward = forwardKindManagerMetrics

func captureKindManagerBuildInfo(parent context.Context, run *kindSuite) (evidence kindManagerRuntimeEvidence,
	finalErr error) {
	start := time.Now()
	deadline, err := kindManagerCaptureDeadline(parent, run, start)
	if err != nil || k8sClient == nil {
		return evidence, errKindManagerBuildInfo
	}
	evidence.ExpectedVersion = "m7-" + run.input.SourceTreeSHA256[:12]
	image := run.input.Images[kindManagerContainer]
	evidence.ImageRef, evidence.ImageConfigID, evidence.ImageManifestDigest = image.Ref, image.ImageID,
		image.LoadedDigest
	captureCtx, cancel := context.WithDeadline(parent, deadline.Add(-30*time.Second))
	defer cancel()
	before, err := readKindManagerIdentity(captureCtx, run, &evidence)
	if err != nil {
		return evidence, errKindManagerBuildInfo
	}
	cfg, err := clientcmd.NewNonInteractiveClientConfig(run.raw, run.input.Context, &clientcmd.ConfigOverrides{},
		nil).ClientConfig()
	if err != nil {
		return evidence, errKindManagerBuildInfo
	}
	if cfg.Insecure || len(cfg.CAData) == 0 {
		return evidence, errKindManagerBuildInfo
	}
	cfg = rest.CopyConfig(cfg)
	cfg.Timeout = 5 * time.Second
	cfg.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	api, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return evidence, errKindManagerBuildInfo
	}
	auth, err := prepareKindMetricsAuthorization(captureCtx, run)
	if err != nil {
		return evidence, errKindManagerBuildInfo
	}
	var stop func() error
	defer func() {
		cleanupCtx, cleanupCancel := context.WithDeadline(context.WithoutCancel(parent), deadline)
		defer cleanupCancel()
		var stopErr error
		if stop != nil && stop() != nil {
			stopErr = kindManagerCleanupHandoff{}
		}
		cleanupErr := cleanupKindMetricsAuthorization(cleanupCtx, auth)
		evidence.Cleanup = stopErr == nil && cleanupErr == nil && kindMetricsAuthorizationCreated(auth)
		if !evidence.Cleanup {
			finalErr = errors.Join(finalErr, stopErr, cleanupErr)
		}
	}()
	if err := createKindMetricsAuthorization(captureCtx, run, auth); err != nil {
		return evidence, err
	}
	if !kindManagerIdentityUnchanged(captureCtx, run, before) {
		return evidence, errKindManagerBuildInfo
	}
	var version string
	{
		bearer, err := issueKindMetricsToken(captureCtx, run, api, auth)
		if err != nil {
			return evidence, errKindManagerBuildInfo
		}
		if !kindManagerIdentityUnchanged(captureCtx, run, before) || validateKindMetricsAuthorization(captureCtx,
			auth) != nil {
			return evidence,
				errKindManagerBuildInfo
		}
		base, closeTunnel, err := kindManagerMetricsForward(captureCtx, cfg, defaultOperatorNamespace, before.podName)
		stop = closeTunnel
		if err == nil && stop == nil {
			stop = func() error { return kindManagerCleanupHandoff{} }
			err = errKindManagerBuildInfo
		}
		if err != nil {
			return evidence, errKindManagerBuildInfo
		}
		if !kindManagerIdentityUnchanged(captureCtx, run, before) {
			return evidence, errKindManagerBuildInfo
		}
		version, err = readKindManagerBuildInfo(captureCtx, base, bearer, evidence.ExpectedVersion)
		if err != nil {
			return evidence, errKindManagerBuildInfo
		}
	}
	evidence.ObservedVersion = version
	if !kindManagerIdentityUnchanged(captureCtx, run, before) || captureCtx.Err() != nil {
		return evidence, errKindManagerBuildInfo
	}
	evidence.ObservedAt = float64(time.Now().UnixNano()) / float64(time.Second)
	if evidence.ObservedAt <= 0 || evidence.ObservedAt > run.input.LeaseExpiresAt {
		return evidence, errKindManagerBuildInfo
	}
	evidence.Authenticated = true
	return evidence, nil
}

func kindManagerCaptureDeadline(ctx context.Context, run *kindSuite, now time.Time) (time.Time, error) {
	if run == nil || ctx.Err() != nil || !featureE2ERunPattern.MatchString(run.input.RunID) ||
		!kindHashPattern.MatchString(run.input.SourceTreeSHA256) || !run.operatorNamespaceAbsent ||
		!kindRuntimeText(string(run.operatorNamespaceUID), 256) || nsOperator != defaultOperatorNamespace {
		return time.Time{}, errKindManagerBuildInfo
	}
	expiry := run.input.LeaseExpiresAt
	if math.IsNaN(expiry) || math.IsInf(expiry, 0) || expiry <= 0 || expiry >= float64(math.MaxInt64) ||
		expiry-float64(now.UnixNano())/float64(time.Second) < 600 {
		return time.Time{}, errKindManagerBuildInfo
	}
	image, ok := run.input.Images[kindManagerContainer]
	if !ok || !kindImagePattern.MatchString(image.Ref) || !kindDigestPattern.MatchString(image.ImageID) ||
		!kindDigestPattern.MatchString(image.LoadedDigest) ||
		!validKindManagerImageID(image.ImageID, "rpgmigrate-m7-"+run.input.RunID+"-control-plane", image) {
		return time.Time{}, errKindManagerBuildInfo
	}
	seconds, fraction := math.Modf(expiry)
	deadline := time.Unix(int64(seconds), int64(fraction*float64(time.Second)))
	if cap := now.Add(90 * time.Second); deadline.After(cap) {
		deadline = cap
	}
	if outer, ok := ctx.Deadline(); ok && deadline.After(outer) {
		deadline = outer
	}
	if !deadline.After(now.Add(30 * time.Second)) {
		return time.Time{}, errKindManagerBuildInfo
	}
	return deadline, nil
}

func validKindManagerImageID(raw, nodeName string, image kindImage) bool {
	node := kindNodeNamePattern.FindStringSubmatch(nodeName)
	if len(node) != 2 || !kindPublicImagePattern.MatchString(image.Ref) || image.Architecture != runtime.GOARCH ||
		!kindDigestPattern.MatchString(image.ImageID) || !kindDigestPattern.MatchString(image.LoadedDigest) ||
		image.Digest != image.LoadedDigest || image.ImageID == image.LoadedDigest ||
		!strings.HasSuffix(image.Ref, "@"+image.LoadedDigest) || len(image.NodeBindings) != len(kindNodeSuffixes) {
		return false
	}
	repository, _, _ := strings.Cut(image.Ref, "@")
	host, _, _ := strings.Cut(repository, "/")
	host, _, _ = strings.Cut(host, ":")
	if host == "localhost" || net.ParseIP(host) != nil {
		return false
	}
	untagged := repository
	if colon := strings.LastIndex(untagged, ":"); colon > strings.LastIndex(untagged, "/") {
		untagged = untagged[:colon]
	}
	if image.RegistryIndexDigest != nil && (!kindDigestPattern.MatchString(*image.RegistryIndexDigest) ||
		*image.RegistryIndexDigest == image.ImageID || *image.RegistryIndexDigest == image.LoadedDigest) {
		return false
	}
	nodeIDs := map[string]bool{}
	for _, suffix := range kindNodeSuffixes {
		binding, ok := image.NodeBindings[node[1]+"-"+suffix]
		if !ok || nodeIDs[binding.NodeID] || !validKindNodeBinding(binding, image, repository, untagged) {
			return false
		}
		nodeIDs[binding.NodeID] = true
	}
	return raw != "" && slices.Contains(image.NodeBindings[nodeName].AcceptedImageIDs, raw)
}

func validKindNodeBinding(binding kindNodeBinding, image kindImage, repository, untagged string) bool {
	if !kindHashPattern.MatchString(binding.NodeID) || binding.RequestedRef != image.Ref ||
		binding.ManifestDigest != image.LoadedDigest || binding.ConfigDigest != image.ImageID ||
		binding.OS != kindLinux || binding.Architecture != image.Architecture ||
		binding.Variant != nil && (binding.Architecture != "arm64" || *binding.Variant != "v8") ||
		len(binding.AcceptedImageIDs) == 0 || !slices.IsSorted(binding.AcceptedImageIDs) ||
		!slices.Contains(binding.AcceptedImageIDs, image.ImageID) ||
		!slices.Contains(binding.AcceptedImageIDs, image.LoadedDigest) {
		return false
	}
	for i, id := range binding.AcceptedImageIDs {
		if i > 0 && id == binding.AcceptedImageIDs[i-1] {
			return false
		}
		if id == image.ImageID || id == image.LoadedDigest {
			continue
		}
		repo, digest, ok := strings.Cut(id, "@")
		if !ok || (repo != repository && repo != untagged) ||
			(digest != image.LoadedDigest && (image.RegistryIndexDigest == nil || digest != *image.RegistryIndexDigest)) {
			return false
		}
	}
	return true
}

func readKindManagerIdentity(ctx context.Context, run *kindSuite,
	evidence *kindManagerRuntimeEvidence) (kindManagerIdentity, error) {
	var identity kindManagerIdentity
	namespace := &corev1.Namespace{}
	if k8sClient.Get(ctx, client.ObjectKey{Name: defaultOperatorNamespace}, namespace) != nil ||
		namespace.UID != run.operatorNamespaceUID || namespace.Labels[kindRunLabel] != run.input.RunID ||
		!namespace.DeletionTimestamp.IsZero() {
		return identity, errKindManagerBuildInfo
	}
	deployment := &appsv1.Deployment{}
	name := helmRelease + "-pgcopydb-operator"
	if k8sClient.Get(ctx, client.ObjectKey{Namespace: defaultOperatorNamespace, Name: name},
		deployment) != nil || !validKindManagerDeployment(deployment) {
		return identity, errKindManagerBuildInfo
	}
	pods := &corev1.PodList{}
	if k8sClient.List(ctx, pods, client.InNamespace(defaultOperatorNamespace),
		client.MatchingLabels(deployment.Spec.Selector.MatchLabels)) != nil || len(pods.Items) != 1 {
		return identity, errKindManagerBuildInfo
	}
	pod := &corev1.Pod{}
	selected := pods.Items[0]
	if k8sClient.Get(ctx, client.ObjectKeyFromObject(&selected), pod) != nil || pod.UID != selected.UID ||
		!kindRuntimeText(string(pod.UID), 256) || !pod.DeletionTimestamp.IsZero() {
		return identity, errKindManagerBuildInfo
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" ||
		!kindRuntimeText(string(owner.UID), 256) {
		return identity, errKindManagerBuildInfo
	}
	replicaSet := &appsv1.ReplicaSet{}
	if k8sClient.Get(ctx, client.ObjectKey{Namespace: defaultOperatorNamespace, Name: owner.Name},
		replicaSet) != nil || replicaSet.UID != owner.UID || !kindManagerControlledBy(pod, "ReplicaSet",
		replicaSet.Name, replicaSet.UID) || !kindManagerControlledBy(replicaSet, "Deployment", deployment.Name,
		deployment.UID) || !replicaSet.DeletionTimestamp.IsZero() {
		return identity, errKindManagerBuildInfo
	}
	image := run.input.Images[kindManagerContainer]
	if !validKindManagerPodSpec(deployment.Spec.Template.Spec, image.Ref) ||
		!validKindManagerPodSpec(replicaSet.Spec.Template.Spec, image.Ref) || !validKindManagerPodSpec(pod.Spec,
		image.Ref) || !validKindManagerStatus(pod) {
		return identity, errKindManagerBuildInfo
	}
	status := pod.Status.ContainerStatuses[0]
	identity = kindManagerIdentity{namespaceUID: namespace.UID, deploymentUID: deployment.UID,
		replicaSetUID: replicaSet.UID, podUID: pod.UID, podName: pod.Name, nodeName: pod.Spec.NodeName,
		containerID: status.ContainerID, imageID: status.ImageID, restarts: status.RestartCount}
	if evidence != nil {
		evidence.NamespaceUID, evidence.DeploymentUID, evidence.ReplicaSetUID,
			evidence.PodUID = string(namespace.UID), string(deployment.UID), string(replicaSet.UID), string(pod.UID)
		evidence.NodeName, evidence.ContainerID, evidence.ContainerImageID,
			evidence.RestartCount = identity.nodeName, identity.containerID, identity.imageID, identity.restarts
	}
	if !validKindManagerImageID(status.ImageID, pod.Spec.NodeName, image) {
		return identity, errKindManagerBuildInfo
	}
	node := &corev1.Node{}
	if k8sClient.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node) != nil || !validKindManagerNode(node,
		run.input.RunID) {
		return identity, errKindManagerBuildInfo
	}
	identity.nodeUID = node.UID
	return identity, nil
}

func validKindManagerDeployment(deployment *appsv1.Deployment) bool {
	want := map[string]string{labelAppName: uidOperator, "app.kubernetes.io/instance": helmRelease}
	return kindRuntimeText(string(deployment.UID), 256) && deployment.DeletionTimestamp.IsZero() &&
		deployment.Namespace == defaultOperatorNamespace && deployment.Name == helmRelease+"-pgcopydb-operator" &&
		deployment.Annotations["meta.helm.sh/release-name"] == helmRelease &&
		deployment.Annotations["meta.helm.sh/release-namespace"] == defaultOperatorNamespace &&
		deployment.Labels["app.kubernetes.io/managed-by"] == "Helm" && deployment.Spec.Replicas != nil &&
		*deployment.Spec.Replicas == 1 && deployment.Spec.Selector != nil &&
		len(deployment.Spec.Selector.MatchExpressions) == 0 &&
		reflect.DeepEqual(deployment.Spec.Selector.MatchLabels, want)
}

func kindManagerControlledBy(object metav1.Object, kind, name string, uid types.UID) bool {
	count := 0
	for _, owner := range object.GetOwnerReferences() {
		if owner.Controller != nil && *owner.Controller {
			count++
		}
	}
	owner := metav1.GetControllerOf(object)
	return count == 1 && owner != nil && owner.APIVersion == "apps/v1" && owner.Kind == kind &&
		owner.Name == name && owner.UID == uid
}

func validKindManagerPodSpec(spec corev1.PodSpec, image string) bool {
	if spec.HostNetwork || len(spec.Containers) != 1 || len(spec.InitContainers) != 0 ||
		len(spec.EphemeralContainers) != 0 {
		return false
	}
	c := spec.Containers[0]
	return c.Name == kindManagerContainer && c.Image == image && c.ImagePullPolicy == corev1.PullIfNotPresent
}

func validKindManagerStatus(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) != 1 ||
		len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 {
		return false
	}
	ready := 0
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			if condition.Status != corev1.ConditionTrue {
				return false
			}
			ready++
		}
	}
	status := pod.Status.ContainerStatuses[0]
	return ready == 1 && status.Name == kindManagerContainer && status.Ready && status.RestartCount >= 0 &&
		kindRuntimeText(status.ContainerID, 512) && status.State.Running != nil && status.State.Waiting == nil &&
		status.State.Terminated == nil
}

func validKindManagerNode(node *corev1.Node, runID string) bool {
	prefix := "rpgmigrate-m7-" + runID
	if !kindRuntimeText(string(node.UID), 256) || node.Labels[kindRunLabel] != runID ||
		!node.DeletionTimestamp.IsZero() || !slices.Contains([]string{prefix + "-control-plane", prefix + "-worker",
		prefix + "-worker2"}, node.Name) {
		return false
	}
	ready := 0
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			if condition.Status != corev1.ConditionTrue {
				return false
			}
			ready++
		}
	}
	return ready == 1
}

func kindMetricsAuthorizationObjects(runID string) []*kindMetricsAuthObject {
	labels := map[string]string{kindRunLabel: runID, "pgcopydb-operator.io/m7-purpose": "runtime-version-observer"}
	name := "m7-" + runID + "-metrics"
	saName := "m7-metrics-observer-" + runID
	automount := false
	return []*kindMetricsAuthObject{
		{want: &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: saName,
			Namespace: defaultOperatorNamespace, Labels: labels}, AutomountServiceAccountToken: &automount}},
		{want: &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Rules: []rbacv1.PolicyRule{{NonResourceURLs: []string{kindManagerMetricsPath},
				Verbs: []string{kindManagerMetricsVerb}}}}},
		{want: &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: saName, Namespace: defaultOperatorNamespace}}}},
	}
}

func emptyKindMetricsAuthObject(want client.Object) client.Object {
	switch want.(type) {
	case *corev1.ServiceAccount:
		return &corev1.ServiceAccount{}
	case *rbacv1.ClusterRole:
		return &rbacv1.ClusterRole{}
	default:
		return &rbacv1.ClusterRoleBinding{}
	}
}

func prepareKindMetricsAuthorization(ctx context.Context, run *kindSuite) ([]*kindMetricsAuthObject, error) {
	objects := kindMetricsAuthorizationObjects(run.input.RunID)
	for _, object := range objects {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object.want),
			emptyKindMetricsAuthObject(object.want)); !apierrors.IsNotFound(err) {
			return nil, errKindManagerBuildInfo
		}
	}
	return objects, nil
}

func createKindMetricsAuthorization(ctx context.Context, run *kindSuite, objects []*kindMetricsAuthObject) error {
	for _, record := range objects {
		namespace := &corev1.Namespace{}
		if k8sClient.Get(ctx, client.ObjectKey{Name: defaultOperatorNamespace}, namespace) != nil ||
			namespace.UID != run.operatorNamespaceUID || namespace.Labels[kindRunLabel] != run.input.RunID ||
			!namespace.DeletionTimestamp.IsZero() {
			return errKindManagerBuildInfo
		}
		record.attempted = true
		made := record.want.DeepCopyObject().(client.Object)
		if k8sClient.Create(ctx, made) != nil {
			record.uncertain = true
			return errors.Join(errKindManagerBuildInfo, kindManagerCleanupHandoff{})
		}
		if made.GetName() != record.want.GetName() || made.GetNamespace() != record.want.GetNamespace() ||
			!kindRuntimeText(string(made.GetUID()), 256) || !kindRuntimeText(made.GetResourceVersion(), 256) {
			record.uncertain = true
			return errors.Join(errKindManagerBuildInfo, kindManagerCleanupHandoff{})
		}
		record.uid, record.version = made.GetUID(), made.GetResourceVersion()
		if !sameKindMetricsAuthObject(made, record.want) {
			return errKindManagerBuildInfo
		}
	}
	return nil
}

func sameKindMetricsAuthObject(live, want client.Object) bool {
	if live.GetName() != want.GetName() || live.GetNamespace() != want.GetNamespace() ||
		!live.GetDeletionTimestamp().IsZero() || !reflect.DeepEqual(live.GetLabels(), want.GetLabels()) ||
		len(live.GetAnnotations()) != 0 || len(live.GetOwnerReferences()) != 0 || len(live.GetFinalizers()) != 0 {
		return false
	}
	switch actual := live.(type) {
	case *corev1.ServiceAccount:
		expected, ok := want.(*corev1.ServiceAccount)
		return ok && reflect.DeepEqual(actual.AutomountServiceAccountToken,
			expected.AutomountServiceAccountToken) && len(actual.Secrets) == 0 && len(actual.ImagePullSecrets) == 0
	case *rbacv1.ClusterRole:
		expected, ok := want.(*rbacv1.ClusterRole)
		return ok && actual.AggregationRule == nil && reflect.DeepEqual(actual.Rules, expected.Rules)
	case *rbacv1.ClusterRoleBinding:
		expected, ok := want.(*rbacv1.ClusterRoleBinding)
		return ok && reflect.DeepEqual(actual.RoleRef, expected.RoleRef) && reflect.DeepEqual(actual.Subjects,
			expected.Subjects)
	}
	return false
}

func validateKindMetricsAuthorization(ctx context.Context, objects []*kindMetricsAuthObject) error {
	if len(objects) != 3 {
		return errKindManagerBuildInfo
	}
	for _, record := range objects {
		live := emptyKindMetricsAuthObject(record.want)
		if record.uncertain || record.uid == "" || record.version == "" || k8sClient.Get(ctx,
			client.ObjectKeyFromObject(record.want), live) != nil || live.GetUID() != record.uid ||
			live.GetResourceVersion() != record.version || !sameKindMetricsAuthObject(live, record.want) {
			return errKindManagerBuildInfo
		}
	}
	return nil
}

func issueKindMetricsToken(ctx context.Context, run *kindSuite, api kubernetes.Interface,
	objects []*kindMetricsAuthObject) (string, error) {
	if validateKindMetricsAuthorization(ctx, objects) != nil {
		return "", errKindManagerBuildInfo
	}
	sa := "m7-metrics-observer-" + run.input.RunID
	review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
		User: "system:serviceaccount:" +

			defaultOperatorNamespace + ":" + sa,
		NonResourceAttributes: &authorizationv1.NonResourceAttributes{Verb: kindManagerMetricsVerb,
			Path: kindManagerMetricsPath}}}
	allowed := &authorizationv1.SubjectAccessReview{}
	err := api.AuthorizationV1().RESTClient().Post().
		Resource("subjectaccessreviews").MaxRetries(0).
		Body(review).Do(ctx).Into(allowed)
	if err != nil || !allowed.Status.Allowed || allowed.Status.Denied || allowed.Status.EvaluationError != "" {
		return "", errKindManagerBuildInfo
	}
	if validateKindMetricsAuthorization(ctx, objects) != nil ||
		run.input.LeaseExpiresAt-float64(time.Now().UnixNano())/float64(time.Second) < 600 {
		return "", errKindManagerBuildInfo
	}
	seconds := int64(600)
	issued := &authenticationv1.TokenRequest{}
	request := &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &seconds}}
	err = api.CoreV1().RESTClient().Post().Namespace(defaultOperatorNamespace).
		Resource("serviceaccounts").Name(sa).SubResource("token").
		MaxRetries(0).Body(request).Do(ctx).Into(issued)
	if err != nil {
		return "", errKindManagerBuildInfo
	}
	expires := float64(issued.Status.ExpirationTimestamp.UnixNano()) / float64(time.Second)
	token := issued.Status.Token
	issued.Status.Token = ""
	if !kindRuntimeText(token, 65536) || expires <= float64(time.Now().UnixNano())/float64(time.Second) ||
		expires > run.input.LeaseExpiresAt {
		return "", errKindManagerBuildInfo
	}
	return token, nil
}

func cleanupKindMetricsAuthorization(ctx context.Context, objects []*kindMetricsAuthObject) error {
	if len(objects) != 3 {
		return kindManagerCleanupHandoff{}
	}
	var failures []error
	for _, index := range []int{2, 0, 1} {
		record := objects[index]
		perObject, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := cleanupKindMetricsAuthObject(perObject, record)
		cancel()
		if err != nil {
			failures = append(failures, kindManagerCleanupHandoff{})
		}
	}
	return errors.Join(failures...)
}

func cleanupKindMetricsAuthObject(ctx context.Context, record *kindMetricsAuthObject) error {
	if record.uncertain {
		return kindManagerCleanupHandoff{}
	}
	live := emptyKindMetricsAuthObject(record.want)
	err := k8sClient.Get(ctx, client.ObjectKeyFromObject(record.want), live)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil || !record.attempted || record.uid == "" || record.version == "" ||
		live.GetUID() != record.uid || live.GetResourceVersion() != record.version ||
		!sameKindMetricsAuthObject(live, record.want) {
		return kindManagerCleanupHandoff{}
	}
	options := &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &record.uid,
		ResourceVersion: &record.version}}
	if k8sClient.Delete(ctx, live, options) != nil {
		return kindManagerCleanupHandoff{}
	}
	err = wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(record.want), emptyKindMetricsAuthObject(record.want))
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, kindManagerCleanupHandoff{}
		}
		return false, nil
	})
	if err != nil {
		return kindManagerCleanupHandoff{}
	}
	return nil
}

var errKindManagerMetricsTunnel = errors.New("kind manager metrics tunnel unavailable")

type kindMetricsRoundTripper func(*http.Request) (*http.Response, error)

func (call kindMetricsRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return call(request)
}

type kindMetricsContextConn struct {
	net.Conn
	cancelClose func() bool
}

func (c *kindMetricsContextConn) Close() error {
	c.cancelClose()
	return c.Conn.Close()
}

type kindMetricsSafeDialer struct{ httpstream.Dialer }

func (d kindMetricsSafeDialer) Dial(protocols ...string) (httpstream.Connection, string, error) {
	connection, protocol, err := d.Dialer.Dial(protocols...)
	if err != nil {
		return nil, "", errKindManagerMetricsTunnel
	}
	return &kindMetricsSafeConnection{Connection: connection}, protocol, nil
}

type kindMetricsSafeConnection struct{ httpstream.Connection }

func (c *kindMetricsSafeConnection) CreateStream(headers http.Header) (httpstream.Stream, error) {
	stream, err := c.Connection.CreateStream(headers)
	if err != nil {
		return nil, errKindManagerMetricsTunnel
	}
	return &kindMetricsSafeStream{Stream: stream,
		errorStream: headers.Get(corev1.StreamType) == corev1.StreamTypeError}, nil
}
func (c *kindMetricsSafeConnection) RemoveStreams(streams ...httpstream.Stream) {
	raw := make([]httpstream.Stream, len(streams))
	for i, stream := range streams {
		if wrapped, ok := stream.(*kindMetricsSafeStream); ok {
			raw[i] = wrapped.Stream
		} else {
			raw[i] = stream
		}
	}
	c.Connection.RemoveStreams(raw...)
}

type kindMetricsSafeStream struct {
	httpstream.Stream
	errorStream bool
}

func (s *kindMetricsSafeStream) Read(buffer []byte) (int, error) {
	if s.errorStream {
		var one [1]byte
		n, err := s.Stream.Read(one[:])
		if n != 0 || err != nil && !errors.Is(err, io.EOF) {
			return 0, errKindManagerMetricsTunnel
		}
		return 0, err
	}
	n, err := s.Stream.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, errKindManagerMetricsTunnel
	}
	return n, err
}
func (s *kindMetricsSafeStream) Write(buffer []byte) (int, error) {
	n, err := s.Stream.Write(buffer)
	if err != nil {
		return n, errKindManagerMetricsTunnel
	}
	return n, nil
}
func (s *kindMetricsSafeStream) Close() error {
	if s.Stream.Close() != nil {
		return errKindManagerMetricsTunnel
	}
	return nil
}
func (s *kindMetricsSafeStream) Reset() error {
	if s.Stream.Reset() != nil {
		return errKindManagerMetricsTunnel
	}
	return nil
}

func forwardKindManagerMetrics(ctx context.Context, config *rest.Config, namespace, podName string) (string,
	func() error, error) {
	if ctx.Err() != nil {
		return "", nil, errKindManagerMetricsTunnel
	}
	endpoint, err := kindManagerForwardEndpoint(config, namespace, podName)
	if err != nil {
		return "", nil, errKindManagerMetricsTunnel
	}
	forwardCtx, cancel := context.WithCancel(ctx)
	cfg := rest.CopyConfig(config)
	cfg.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	tlsConfig, err := rest.TLSConfigFor(cfg)
	if err != nil || tlsConfig == nil || tlsConfig.InsecureSkipVerify {
		cancel()
		return "", nil, errKindManagerMetricsTunnel
	}
	// The SDK upgrade reader does not observe cancellation after TCP dial.
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dialContext := cfg.Dial
		if dialContext == nil {
			dialContext = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
		}
		conn, err := dialContext(ctx, network, address)
		if err != nil {
			return nil, errKindManagerMetricsTunnel
		}
		stop := context.AfterFunc(forwardCtx, func() { _ = conn.Close() })
		return &kindMetricsContextConn{Conn: conn, cancelClose: stop}, nil
	}
	upgradeTransport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, DialContext: dial}
	upgrader,
		err := streamspdy.NewRoundTripperWithConfig(streamspdy.RoundTripperConfig{UpgradeTransport: upgradeTransport})
	if err != nil {
		cancel()
		return "", nil, errKindManagerMetricsTunnel
	}
	wrapped, err := rest.HTTPWrappersForConfig(cfg, upgrader)
	if err != nil {
		cancel()
		return "", nil, errKindManagerMetricsTunnel
	}
	transport := kindMetricsRoundTripper(func(request *http.Request) (*http.Response, error) {
		response, err := wrapped.RoundTrip(request.Clone(forwardCtx))
		if err != nil {
			return nil, errKindManagerMetricsTunnel
		}
		if response.StatusCode != http.StatusSwitchingProtocols {
			if response.Body != nil {
				_ = response.Body.Close()
			}
			return nil, errKindManagerMetricsTunnel
		}
		return response, nil
	})
	httpClient := &http.Client{Transport: transport, CheckRedirect: func(*http.Request,
		[]*http.Request) error {
		return errKindManagerMetricsTunnel
	}}
	dialer := kindMetricsSafeDialer{Dialer: clientspdy.NewDialerForStreaming(clientspdy.NewUpgraderForStreaming(upgrader),

		httpClient, http.MethodPost, endpoint)}
	stopChannel, ready := make(chan struct{}), make(chan struct{})
	forwarder, err := portforward.NewOnAddressesForStreaming(dialer, []string{"127.0.0.1"}, []string{"0:8443"},
		stopChannel,
		ready, io.Discard, io.Discard)
	if err != nil {
		cancel()
		return "", nil, errKindManagerMetricsTunnel
	}
	done := make(chan struct{})
	var forwardErr error
	go func() { defer close(done); forwardErr = forwarder.ForwardPorts() }()
	var once sync.Once
	var stopErr error
	stop := func() error {
		once.Do(func() {
			close(stopChannel)
			cancel()
			upgradeTransport.CloseIdleConnections()
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-done:
				if forwardErr != nil {
					stopErr = errKindManagerMetricsTunnel
				}
			case <-timer.C:
				stopErr = errKindManagerMetricsTunnel
			}
		})
		return stopErr
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-ready:
		ports, err := forwarder.GetPorts()
		if err != nil || len(ports) != 1 || ports[0].Remote != 8443 || ports[0].Local == 0 {
			_ = stop()
			return "", stop, errKindManagerMetricsTunnel
		}
		select {
		case <-done:
			_ = stop()
			return "", stop, errKindManagerMetricsTunnel
		default:
		}
		return "https://127.0.0.1:" + strconv.Itoa(int(ports[0].Local)), stop, nil
	case <-done:
		_ = stop()
		return "", stop, errKindManagerMetricsTunnel
	case <-ctx.Done():
		_ = stop()
		return "", stop, errKindManagerMetricsTunnel
	case <-timer.C:
		_ = stop()
		return "", stop, errKindManagerMetricsTunnel
	}
}

func createKindOperatorNamespace(ctx context.Context, run *kindSuite) error {
	if run == nil || !featureE2ERunPattern.MatchString(run.input.RunID) || run.operatorNamespaceUID != "" ||
		run.operatorNamespaceAbsent || k8sClient == nil {
		return errKindManagerBuildInfo
	}
	live := &corev1.Namespace{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: defaultOperatorNamespace}, live); !apierrors.IsNotFound(err) {
		return errKindManagerBuildInfo
	}
	run.operatorNamespaceAbsent = true
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: defaultOperatorNamespace,
		Labels: map[string]string{kindRunLabel: run.input.RunID}}}
	if k8sClient.Create(ctx, namespace) != nil || namespace.Name != defaultOperatorNamespace ||
		!kindRuntimeText(string(namespace.UID), 256) || namespace.Labels[kindRunLabel] != run.input.RunID {
		return errKindManagerBuildInfo
	}
	run.operatorNamespaceUID = namespace.UID
	if k8sClient.Get(ctx, client.ObjectKey{Name: defaultOperatorNamespace}, live) != nil ||
		live.UID != run.operatorNamespaceUID || live.Labels[kindRunLabel] != run.input.RunID ||
		!live.DeletionTimestamp.IsZero() {
		return errKindManagerBuildInfo
	}
	return nil
}

func kindManagerRuntimeWitness(run *kindSuite, evidence kindManagerRuntimeEvidence) error {
	if run == nil || !kindHashPattern.MatchString(run.input.SourceTreeSHA256) ||
		!featureE2ERunPattern.MatchString(run.input.RunID) {
		return errKindManagerBuildInfo
	}
	image, ok := run.input.Images[kindManagerContainer]
	expected := "m7-" + run.input.SourceTreeSHA256[:12]
	if !ok || !evidence.Authenticated || !evidence.Cleanup || evidence.ExpectedVersion != expected ||
		evidence.ObservedVersion != expected || evidence.ImageRef != image.Ref ||
		evidence.ImageConfigID != image.ImageID || evidence.ImageManifestDigest != image.LoadedDigest ||
		!kindDigestPattern.MatchString(image.ImageID) || !kindDigestPattern.MatchString(image.LoadedDigest) {
		return errKindManagerBuildInfo
	}
	if !validKindManagerImageID(evidence.ContainerImageID, evidence.NodeName, image) ||
		!kindRuntimeText(evidence.ContainerID, 512) || evidence.RestartCount < 0 {
		return errKindManagerBuildInfo
	}
	for _, uid := range []string{evidence.NamespaceUID, evidence.DeploymentUID, evidence.ReplicaSetUID,
		evidence.PodUID} {
		if !kindRuntimeText(uid, 256) {
			return errKindManagerBuildInfo
		}
	}
	prefix := "rpgmigrate-m7-" + run.input.RunID
	if !slices.Contains([]string{prefix + "-control-plane", prefix + "-worker", prefix + "-worker2"},
		evidence.NodeName) || math.IsNaN(evidence.ObservedAt) || math.IsInf(evidence.ObservedAt, 0) ||
		evidence.ObservedAt <= 0 || math.IsNaN(run.input.LeaseExpiresAt) || math.IsInf(run.input.LeaseExpiresAt,
		0) || evidence.ObservedAt > run.input.LeaseExpiresAt {
		return errKindManagerBuildInfo
	}
	return nil
}

func kindManagerForwardEndpoint(config *rest.Config, namespace, podName string) (*url.URL, error) {
	if config == nil || config.Insecure || len(config.CAData) == 0 ||
		len(validation.IsDNS1123Label(namespace)) != 0 || len(validation.IsDNS1123Subdomain(podName)) != 0 {
		return nil, errKindManagerMetricsTunnel
	}
	endpoint, err := url.Parse(config.Host)
	if err != nil || endpoint.Scheme != kindManagerHTTPS || endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" || endpoint.Path != "" {
		return nil, errKindManagerMetricsTunnel
	}
	address := net.ParseIP(endpoint.Hostname())
	if address == nil || !address.IsLoopback() || endpoint.Port() == "" {
		return nil, errKindManagerMetricsTunnel
	}
	endpoint.Path = "/api/v1/namespaces/" + namespace + "/pods/" + podName + "/portforward"
	return endpoint, nil
}

func kindManagerIdentityUnchanged(ctx context.Context, run *kindSuite, before kindManagerIdentity) bool {
	after, err := readKindManagerIdentity(ctx, run, nil)
	return err == nil && after == before
}

func kindMetricsAuthorizationCreated(objects []*kindMetricsAuthObject) bool {
	if len(objects) != 3 {
		return false
	}
	for _, record := range objects {
		if !record.attempted || record.uncertain || record.uid == "" || record.version == "" {
			return false
		}
	}
	return true
}
