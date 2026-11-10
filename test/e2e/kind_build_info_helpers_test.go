package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	goruntime "runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"
	streamspdy "k8s.io/streaming/pkg/httpstream/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const kindBuildForeignNode = "foreign-node"
const kindBuildAccepted = "accepted"
const kindBuildReviewFault = "review"

const kindBuildExpectedMetricsPath = "/metrics"
const kindBuildSuiteAdmin = "fixture-suite-admin"
const kindBuildMissingUIDCase = "missing_uid"
const kindBuildReplacement = "runtime-replacement"

const kindBuildPostAuthReplacement = "post_auth_replacement"
const kindBuildStopFault = "stop"
const kindBuildRoleType = "role"
const kindBuildBindingType = "binding"
const kindBuildPolicyVerb = "get"
const kindBuildPreexisting = "preexisting"
const kindBuildWrongVersion = "wrong_version"

const kindBuildFixtureVersion = "m7-aaaaaaaaaaaa"
const kindBuildSecretSentinel = "private-build-info-bearer-sentinel"
const kindBuildCertificatePEM = "CERTIFICATE"

func kindBuildFixtureMetric(version string) string {
	return "# HELP pgcopydb_operator_build_info Manager build version.\n" +
		"# TYPE pgcopydb_operator_build_info gauge\n" +
		"pgcopydb_operator_build_info{version=\"" +

		version + "\"} 1\n"
}

func TestKindManagerBuildInfoParser(t *testing.T) {
	valid := kindBuildFixtureMetric(kindBuildFixtureVersion)
	for _, tc := range []struct{ name, body string }{
		{"valid", valid},
		{"other_metrics_allowed", valid + "other_metric 3\n"},
		{"missing_family", "other_metric 1\n"},
		{"duplicate_sample", valid + "pgcopydb_operator_build_info{version=\"" + kindBuildFixtureVersion + "\"} 1\n"},
		{"duplicate_family", valid + valid},
		{"extra_label", strings.Replace(valid, "version=", "extra=\"value\",version=", 1)},
		{"duplicate_label", strings.Replace(valid, "version=", "version=\"duplicate\",version=", 1)},
		{"wrong_label", strings.Replace(valid, "version=", "build=", 1)},
		{kindBuildWrongVersion, kindBuildFixtureMetric("m7-bbbbbbbbbbbb")},
		{"counter", strings.Replace(valid, "gauge", "counter", 1)},
		{"untyped", strings.Replace(valid, "# TYPE pgcopydb_operator_build_info gauge\n", "", 1)},
		{"nan", strings.Replace(valid, "} 1\n", "} NaN\n", 1)},
		{"infinite", strings.Replace(valid, "} 1\n", "} +Inf\n", 1)},
		{"wrong_value", strings.Replace(valid, "} 1\n", "} 2\n", 1)},
		{"truncated", strings.TrimSuffix(valid, "\n")},
		{"malformed", valid + kindBuildSecretSentinel + "{\n"},
		{"oversized", valid + strings.Repeat("# padding\n", 300000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseKindManagerBuildInfo([]byte(tc.body), kindBuildFixtureVersion)
			accepted := tc.name == "valid" || tc.name == "other_metrics_allowed"
			if (err == nil) != accepted || accepted && got != kindBuildFixtureVersion {
				t.Fatalf("parser accepted=%v observed=%q error=%v", accepted, got, err)
			}
			if err != nil && strings.Contains(err.Error(), kindBuildSecretSentinel) {
				t.Fatal("parser exposed private input")
			}
		})
	}
}

func TestKindManagerBuildInfoHTTP(t *testing.T) {
	for _, tc := range []struct {
		name      string
		statuses  []int
		body      string
		wantCalls int
		accepted  bool
	}{
		{"authenticated", []int{http.StatusOK}, kindBuildFixtureMetric(kindBuildFixtureVersion), 1, true},
		{"transient", []int{http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusOK},
			kindBuildFixtureMetric(kindBuildFixtureVersion), 3, true},
		{"bounded_transient", []int{http.StatusServiceUnavailable}, kindBuildFixtureMetric(kindBuildFixtureVersion),
			3, false},
		{"unauthorized", []int{http.StatusUnauthorized}, kindBuildSecretSentinel, 1, false},
		{"forbidden", []int{http.StatusForbidden}, kindBuildSecretSentinel, 1, false},
		{kindBuildWrongVersion, []int{http.StatusOK}, kindBuildFixtureMetric("m7-bbbbbbbbbbbb"), 1, false},
		{"too_large", []int{http.StatusOK}, strings.Repeat("x", 2*1024*1024+1), 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := int(calls.Add(1)) - 1
				if r.URL.Path != kindBuildExpectedMetricsPath || r.Header.Get("Authorization") != "Bearer "+
					kindBuildSecretSentinel {
					t.Error("scrape did not use the exact authenticated metrics route")
				}
				if n >= len(tc.statuses) {
					n = len(tc.statuses) - 1
				}
				w.WriteHeader(tc.statuses[n])
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(server.Close)
			got, err := readKindManagerBuildInfo(context.Background(), server.URL, kindBuildSecretSentinel,
				kindBuildFixtureVersion)
			if (err == nil) != tc.accepted || tc.accepted && got != kindBuildFixtureVersion ||
				calls.Load() != int32(tc.wantCalls) {
				t.Fatalf("HTTP capture calls=%d observed=%q error=%v", calls.Load(), got, err)
			}
			if err != nil && strings.Contains(err.Error(), kindBuildSecretSentinel) {
				t.Fatal("HTTP capture exposed bearer/body")
			}
		})
	}
}

func TestKindManagerBuildInfoHTTPRefusesRedirectAndCancellation(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		_, _ = io.WriteString(w, kindBuildFixtureMetric(kindBuildFixtureVersion))
	}))
	t.Cleanup(target.Close)
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	t.Cleanup(origin.Close)
	if _, err := readKindManagerBuildInfo(context.Background(), origin.URL, kindBuildSecretSentinel,
		kindBuildFixtureVersion); err == nil || redirected.Load() != 0 {
		t.Fatal("metrics redirect followed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readKindManagerBuildInfo(ctx, origin.URL, kindBuildSecretSentinel,
		kindBuildFixtureVersion); err == nil {
		t.Fatal("canceled scrape accepted")
	}
	for _, base := range []string{"http://127.0.0.1:1", "https://localhost:1", "https://127.0.0.1:1/path",
		"https://user@127.0.0.1:1", "https://127.0.0.1:1?query", "https://example.invalid:1"} {
		if _, err := readKindManagerBuildInfo(context.Background(), base, kindBuildSecretSentinel,
			kindBuildFixtureVersion); err == nil {
			t.Fatalf("unsafe tunnel URL accepted: %s", base)
		}
	}
}

func TestKindManagerBuildInfoHTTPPinsPeerWithinTunnel(t *testing.T) {
	first, second := kindBuildAlternateCertificate(t), kindBuildAlternateCertificate(t)
	var handshakes atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{first},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			chosen := first
			if handshakes.Add(1) > 1 {
				chosen = second
			}
			return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{chosen}}, nil
		}}
	server.StartTLS()
	defer server.Close()
	if _, err := readKindManagerBuildInfo(context.Background(), server.URL, kindBuildSecretSentinel,
		kindBuildFixtureVersion); err == nil ||
		handshakes.Load() != 2 {
		t.Fatalf("changed tunnel peer accepted or retried: handshakes=%d error=%v",
			handshakes.Load(), err)
	}
}

func kindBuildAlternateCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1),
		Subject: pkix.Name{CommonName: "fixture-localhost"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

type kindBuildCaptureFixture struct {
	mu                                          sync.Mutex
	run                                         *kindSuite
	api                                         *httptest.Server
	metrics                                     *httptest.Server
	client                                      client.Client
	namespace                                   *corev1.Namespace
	deployment                                  *appsv1.Deployment
	replicaSet                                  *appsv1.ReplicaSet
	pod                                         *corev1.Pod
	node                                        *corev1.Node
	created, deleted                            []string
	tokenCalls, reviewCalls, metricCalls, stops int
	fault                                       string
	mutate                                      func(*kindBuildCaptureFixture)
}

func kindBuildOwner(kind, name string, uid types.UID, version string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{APIVersion: version, Kind: kind, Name: name, UID: uid, Controller: &controller}
}

func newKindBuildCaptureFixture(t *testing.T) *kindBuildCaptureFixture {
	t.Helper()
	id := strings.Repeat("a", 32)
	image, _ := kindBuildRegistryImage(t, kindImage{Ref: "ghcr.io/example/manager",
		ImageID: "sha256:" + strings.Repeat("b", 64), LoadedDigest: "sha256:" + strings.Repeat("c", 64)},
		"rpgmigrate-m7-"+id+"-worker")
	run := &kindSuite{input: kindSuiteInput{SchemaVersion: 2, RunID: id, Context: "kind-rpgmigrate-m7-" + id,
		SourceTreeSHA256: strings.Repeat("a", 64), Images: map[string]kindImage{"manager": image},
		LeaseExpiresAt: float64(time.Now().Add(time.Hour).Unix())}, operatorNamespaceUID: "namespace-owned",
		operatorNamespaceAbsent: true}
	labels := map[string]string{labelAppName: "pgcopydb-operator", "app.kubernetes.io/instance": helmRelease}
	replicas := int32(1)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: defaultOperatorNamespace,
		UID: run.operatorNamespaceUID, Labels: map[string]string{kindRunLabel: id}, CreationTimestamp: metav1.Now()}}
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: helmRelease + "-pgcopydb-operator",
		Namespace: defaultOperatorNamespace, UID: "deployment-owned",
		Labels: map[string]string{"app.kubernetes.io/managed-by": "Helm"},
		Annotations: map[string]string{"meta.helm.sh/release-name": helmRelease,
			"meta.helm.sh/release-namespace": defaultOperatorNamespace}},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: kindManagerContainer, Image: image.Ref,
					ImagePullPolicy: corev1.PullIfNotPresent}}}}}}
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: deployment.Name + "-hash",
		Namespace: defaultOperatorNamespace, UID: "replicaset-owned",
		OwnerReferences: []metav1.OwnerReference{kindBuildOwner("Deployment", deployment.Name, deployment.UID,
			"apps/v1")}}, Spec: appsv1.ReplicaSetSpec{Replicas: &replicas,
		Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: *deployment.Spec.Template.DeepCopy()}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "rpgmigrate-m7-" + id + "-worker",
		UID: "node-owned", Labels: map[string]string{kindRunLabel: id}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady,
			Status: corev1.ConditionTrue}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: replicaSet.Name + "-pod",
		Namespace: defaultOperatorNamespace, UID: "pod-owned", Labels: labels,
		OwnerReferences: []metav1.OwnerReference{kindBuildOwner("ReplicaSet", replicaSet.Name, replicaSet.UID,
			"apps/v1")}}, Spec: *replicaSet.Spec.Template.Spec.DeepCopy(),
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady,
			Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: kindManagerContainer,
			Ready:   true,
			ImageID: image.ImageID, ContainerID: "containerd://exact-manager",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	pod.Spec.NodeName = node.Name
	f := &kindBuildCaptureFixture{run: run, namespace: namespace, deployment: deployment, replicaSet: replicaSet,
		pod: pod, node: node}
	f.api = httptest.NewTLSServer(http.HandlerFunc(f.serveAPI))
	t.Cleanup(f.api.Close)
	configName := run.input.Context
	run.raw = clientcmdapi.Config{CurrentContext: configName,
		Contexts: map[string]*clientcmdapi.Context{configName: {Cluster: configName, AuthInfo: configName}},
		Clusters: map[string]*clientcmdapi.Cluster{configName: {Server: f.api.URL,
			CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: kindBuildCertificatePEM,
				Bytes: f.api.Certificate().Raw})}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{configName: {Token: kindBuildSuiteAdmin}}}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	f.client = clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(namespace, deployment, replicaSet,
		pod, node).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, base client.WithWatch, key client.ObjectKey, object client.Object,
			opts ...client.GetOption) error {
			if _,
				secret := object.(*corev1.Secret); secret {
				t.Error("capture accessed Secret API")
				return errors.New(kindBuildSecretSentinel)
			}
			return base.Get(ctx, key, object, opts...)
		},
		Create: func(ctx context.Context, base client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			kind := kindBuildAuthKind(object)
			if kind == "" {
				t.Error("capture created a non-policy object")
				return errors.New(kindBuildSecretSentinel)
			}
			f.created = append(f.created, kind)
			if f.fault == "create_"+kind {
				return errors.New(kindBuildSecretSentinel)
			}
			object.SetUID(types.UID("created-" + kind))
			if err := base.Create(ctx, object, opts...); err != nil {
				return err
			}
			if f.fault == kindBuildPostAuthReplacement && kind == kindBuildBindingType {
				f.pod.UID = kindBuildReplacement
				if err := base.Update(ctx, f.pod); err != nil {
					return err
				}
			}
			if f.fault == "lost_"+kind {
				return errors.New(kindBuildSecretSentinel)
			}
			return nil
		},
		Delete: func(ctx context.Context, base client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
			kind := kindBuildAuthKind(object)
			if kind == "" {
				t.Error("capture deleted a non-policy object")
				return errors.New(kindBuildSecretSentinel)
			}
			options := &client.DeleteOptions{}
			options.ApplyOptions(opts)
			if options.Preconditions == nil || options.Preconditions.UID == nil ||
				*options.Preconditions.UID != object.GetUID() || options.Preconditions.ResourceVersion == nil ||
				*options.Preconditions.ResourceVersion != object.GetResourceVersion() {
				t.Error("cleanup omitted exact UID/resourceVersion")
			}
			f.deleted = append(f.deleted, kind)
			if f.fault == "delete_"+kind {
				return errors.New(kindBuildSecretSentinel)
			}
			return base.Delete(ctx, object, opts...)
		},
	}).Build()
	f.metrics = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.metricCalls++
		if r.URL.Path != kindBuildExpectedMetricsPath || r.Header.Get("Authorization") != "Bearer "+
			kindBuildSecretSentinel {
			t.Error("capture used wrong authenticated endpoint")
		}
		body := kindBuildFixtureMetric(kindBuildFixtureVersion)
		if f.fault == "parse" {
			body = kindBuildSecretSentinel + "{\n"
		}
		_, _ = io.WriteString(w, body)
		if f.mutate != nil {
			f.mutate(f)
		}
	}))
	t.Cleanup(f.metrics.Close)
	oldClient, oldForward, oldRun := k8sClient, kindManagerMetricsForward, activeKindSuite
	k8sClient, activeKindSuite = f.client, run
	kindManagerMetricsForward = func(_ context.Context, cfg *rest.Config, namespace, podName string) (string,
		func() error, error) {
		if cfg.Host != f.api.URL || cfg.Insecure || len(cfg.CAData) == 0 || namespace != defaultOperatorNamespace ||
			podName != pod.Name {
			t.Error("tunnel escaped exact Pod/private API TLS config")
		}
		if f.fault == "ready_replacement" {
			f.pod.UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.pod); err != nil {
				return "", nil, err
			}
		}
		if f.fault == "tunnel" {
			return "", nil, errors.New(kindBuildSecretSentinel)
		}
		return f.metrics.URL, func() error {
			f.stops++
			if f.fault == kindBuildStopFault {
				return errors.New(kindBuildSecretSentinel)
			}
			return nil
		}, nil
	}
	t.Cleanup(func() { k8sClient, kindManagerMetricsForward, activeKindSuite = oldClient, oldForward, oldRun })
	return f
}

func kindBuildAuthKind(object client.Object) string {
	switch object.(type) {
	case *corev1.ServiceAccount:
		return "sa"
	case *rbacv1.ClusterRole:
		return kindBuildRoleType
	case *rbacv1.ClusterRoleBinding:
		return kindBuildBindingType
	default:
		return ""
	}
}

func (f *kindBuildCaptureFixture) serveAPI(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+kindBuildSuiteAdmin {
		http.Error(w,
			kindBuildSecretSentinel, http.StatusForbidden)
		return
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, kindBuildSecretSentinel, http.StatusBadRequest)
		return
	}
	switch r.URL.Path {
	case "/apis/authorization.k8s.io/v1/subjectaccessreviews":
		f.reviewCalls++
		if f.fault == "review_retry" {
			w.Header().Set("Retry-After", "1")
			http.Error(w, kindBuildSecretSentinel, http.StatusTooManyRequests)
			return
		}
		review := &authorizationv1.SubjectAccessReview{}
		if _, _, err := clientgoscheme.Codecs.UniversalDeserializer().Decode(raw, nil, review); err != nil {
			http.Error(w, kindBuildSecretSentinel, http.StatusBadRequest)
			return
		}
		want := &authorizationv1.NonResourceAttributes{Verb: kindBuildPolicyVerb, Path: kindBuildExpectedMetricsPath}
		if review.Spec.User != "system:serviceaccount:"+defaultOperatorNamespace+":m7-metrics-observer-"+
			f.run.input.RunID || !reflect.DeepEqual(review.Spec.NonResourceAttributes, want) ||
			review.Spec.ResourceAttributes != nil {
			http.Error(w, kindBuildSecretSentinel, http.StatusForbidden)
			return
		}
		review.TypeMeta = metav1.TypeMeta{APIVersion: "authorization.k8s.io/v1", Kind: "SubjectAccessReview"}
		review.Status.Allowed = f.fault != kindBuildReviewFault
		_ = json.NewEncoder(w).Encode(review)
	case "/api/v1/namespaces/" + defaultOperatorNamespace + "/serviceaccounts/m7-metrics-observer-" +
		f.run.input.RunID + "/token":
		f.tokenCalls++
		if f.fault == "token_retry" {
			w.Header().Set("Retry-After", "1")
			http.Error(w, kindBuildSecretSentinel, http.StatusTooManyRequests)
			return
		}
		request := &authenticationv1.TokenRequest{}
		if _, _, err := clientgoscheme.Codecs.UniversalDeserializer().Decode(raw, nil, request); err != nil {
			http.Error(w, kindBuildSecretSentinel, http.StatusBadRequest)
			return
		}
		if request.Spec.ExpirationSeconds == nil || *request.Spec.ExpirationSeconds != 600 ||
			len(request.Spec.Audiences) != 0 || request.Spec.BoundObjectRef != nil {
			http.Error(w, kindBuildSecretSentinel, http.StatusBadRequest)
			return
		}
		if f.fault == "token" {
			http.Error(w, kindBuildSecretSentinel, http.StatusInternalServerError)
			return
		}
		request.TypeMeta = metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenRequest"}
		expires := time.Now().Add(600 * time.Second)
		if f.fault == "token_expired" {
			expires = time.Now().Add(-time.Minute)
		}
		if f.fault == "token_beyond_lease" {
			expires = time.Unix(int64(f.run.input.LeaseExpiresAt), 0).Add(time.Second)
		}
		request.Status = authenticationv1.TokenRequestStatus{Token: kindBuildSecretSentinel,
			ExpirationTimestamp: metav1.NewTime(expires)}
		_ = json.NewEncoder(w).Encode(request)
	default:
		http.Error(w, kindBuildSecretSentinel, http.StatusNotFound)
	}
}

func TestKindManagerBuildInfoCapture(t *testing.T) {
	f := newKindBuildCaptureFixture(t)
	evidence, err := f.capture(context.Background(), nil)
	if err != nil || !evidence.Authenticated || !evidence.Cleanup ||
		evidence.ExpectedVersion != kindBuildFixtureVersion || evidence.ObservedVersion != kindBuildFixtureVersion ||
		evidence.NamespaceUID != string(f.namespace.UID) || evidence.PodUID != string(f.pod.UID) ||
		evidence.ContainerImageID != f.run.input.Images[kindManagerContainer].ImageID || evidence.ObservedAt <= 0 {
		t.Fatalf("authenticated capture failed: evidence=%+v error=%v", evidence, err)
	}
	if !slices.Equal(f.created, []string{"sa", kindBuildRoleType, kindBuildBindingType}) ||
		!slices.Equal(f.deleted, []string{kindBuildBindingType, "sa", kindBuildRoleType}) || f.reviewCalls != 1 ||
		f.tokenCalls != 1 || f.metricCalls != 1 || f.stops != 1 {
		t.Fatalf("capture effects differ: create=%v delete=%v review=%d token=%d metrics=%d stop=%d", f.created,
			f.deleted, f.reviewCalls, f.tokenCalls, f.metricCalls, f.stops)
	}
	raw, marshalErr := json.Marshal(evidence)
	if marshalErr != nil || strings.Contains(string(raw), kindBuildSecretSentinel) {
		t.Fatal("runtime evidence contains private material")
	}
}

func TestKindManagerBuildInfoCaptureFailureCleanup(t *testing.T) {
	for _, fault := range []string{"create_sa", "create_role", "create_binding", "lost_sa", "lost_role",
		"lost_binding", kindBuildReviewFault, "token", "token_expired", "token_beyond_lease", "token_retry", "review_retry",
		"tunnel", "parse", kindBuildStopFault, "delete_binding", "delete_sa", "delete_role"} {
		t.Run(fault, func(t *testing.T) {
			f := newKindBuildCaptureFixture(t)
			evidence, err := f.capture(context.Background(), func() { f.fault = fault })
			if err == nil || evidence.Authenticated && evidence.Cleanup || strings.Contains(err.Error(),
				kindBuildSecretSentinel) {
				t.Fatalf("failed capture leaked or qualified: evidence=%+v error=%v", evidence, err)
			}
			if f.tokenCalls > 1 {
				t.Fatal("capture renewed/replayed token")
			}
			if strings.HasPrefix(fault, "lost_") || strings.HasPrefix(fault, "delete_") || fault == kindBuildStopFault {
				if evidence.Cleanup {
					t.Fatal("uncertain ownership/stop claimed cleanup")
				}
			}
			if !strings.HasPrefix(fault, "create_") && !strings.HasPrefix(fault, "lost_") && !strings.HasPrefix(fault,
				"delete_") && fault != kindBuildStopFault {
				if !evidence.Cleanup || !slices.Equal(f.deleted, []string{kindBuildBindingType, "sa", kindBuildRoleType}) {
					t.Fatal("failure skipped exact auth cleanup")
				}
			}
		})
	}
}

func (f *kindBuildCaptureFixture) capture(ctx context.Context, configure func()) (kindManagerRuntimeEvidence, error) {
	f.mu.Lock()
	if configure != nil {
		configure()
	}
	f.mu.Unlock()
	evidence, err := captureKindManagerBuildInfo(ctx, f.run)
	f.mu.Lock()
	if f.tokenCalls > 1 || f.reviewCalls > 1 {
		f.mu.Unlock()
		return evidence, errors.New("fixture observed request replay")
	}
	f.mu.Unlock()
	return evidence, err
}

func TestKindManagerNamespaceRequiresFreshServerUID(t *testing.T) {
	for _, scenario := range []string{"fresh", kindBuildPreexisting, "lost_create", kindBuildMissingUIDCase,
		"readback_replacement"} {
		t.Run(scenario, func(t *testing.T) {
			old := k8sClient
			t.Cleanup(func() { k8sClient = old })
			run := &kindSuite{input: kindSuiteInput{RunID: strings.Repeat("a", 32)}}
			objects := []client.Object{}
			if scenario == kindBuildPreexisting {
				objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: defaultOperatorNamespace,
					UID: kindBuildPreexisting, Labels: map[string]string{kindRunLabel: run.input.RunID}}})
			}
			creates := 0
			k8sClient = clientfake.NewClientBuilder().WithObjects(objects...).
				WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context,

					base client.WithWatch, object client.Object, opts ...client.CreateOption) error {
					creates++
					if _, ok := object.(*corev1.Namespace); !ok || object.GetName() != defaultOperatorNamespace ||
						object.GetLabels()[kindRunLabel] != run.input.RunID {
						t.Error("namespace producer escaped owned scope")
					}
					object.SetUID("fresh-server-namespace")
					object.SetCreationTimestamp(metav1.Now())
					if scenario == kindBuildMissingUIDCase {
						object.SetUID("")
					}
					if err := base.Create(ctx, object, opts...); err != nil {
						return err
					}
					if scenario == "lost_create" {
						return errors.New(kindBuildSecretSentinel)
					}
					if scenario == "readback_replacement" {
						stored := object.DeepCopyObject().(client.Object)
						stored.SetUID(kindBuildReplacement)
						if err := base.Update(ctx, stored); err != nil {
							return err
						}
					}
					return nil
				}}).Build()
			err := createKindOperatorNamespace(context.Background(), run)
			if scenario == "fresh" {
				if err != nil || !run.operatorNamespaceAbsent || run.operatorNamespaceUID != "fresh-server-namespace" ||
					creates != 1 {
					t.Fatalf("fresh namespace ownership unavailable: %v", err)
				}
			} else if err == nil {
				t.Fatal("ambiguous/preexisting namespace adopted")
			}
			if scenario == kindBuildPreexisting && creates != 0 {
				t.Fatal("preexisting namespace reached Create")
			}
		})
	}
}

func TestKindManagerNamespaceReplacementBlocksTeardown(t *testing.T) {
	oldClient, oldRun, oldCtx := k8sClient, activeKindSuite, ctx
	t.Cleanup(func() { k8sClient, activeKindSuite, ctx = oldClient, oldRun, oldCtx })
	ctx = context.Background()
	activeKindSuite = &kindSuite{input: kindSuiteInput{RunID: strings.Repeat("a", 32)},
		operatorNamespaceAbsent: true, operatorNamespaceUID: "original-namespace"}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: defaultOperatorNamespace,
		UID: "replacement-namespace", Labels: map[string]string{kindRunLabel: activeKindSuite.input.RunID}}}
	k8sClient = clientfake.NewClientBuilder().WithObjects(namespace).Build()
	if err := requireKindNamespace(defaultOperatorNamespace); err == nil {
		t.Fatal("namespace replacement inherited teardown ownership")
	}
}

func TestKindManagerBuildInfoIdentityRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*kindBuildCaptureFixture)
	}{
		{"no_namespace_provenance", func(f *kindBuildCaptureFixture) { f.run.operatorNamespaceAbsent = false }},
		{"no_namespace_uid", func(f *kindBuildCaptureFixture) { f.run.operatorNamespaceUID = "" }},
		{"invalid_source", func(f *kindBuildCaptureFixture) { f.run.input.SourceTreeSHA256 = "invalid" }},
		{"missing_manifest", func(f *kindBuildCaptureFixture) {
			image := f.run.input.Images[kindManagerContainer]
			image.LoadedDigest = ""
			f.run.input.Images[kindManagerContainer] = image
		}},
		{"insufficient_lease", func(f *kindBuildCaptureFixture) {
			f.run.input.LeaseExpiresAt = float64(time.Now().Add(599 * time.Second).Unix())
		}},
		{"expired_lease", func(f *kindBuildCaptureFixture) { f.run.input.LeaseExpiresAt = 1 }},
		{"nonfinite_lease", func(f *kindBuildCaptureFixture) { f.run.input.LeaseExpiresAt = math.Inf(1) }},
		{"namespace_replaced", func(f *kindBuildCaptureFixture) {
			f.namespace.UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.namespace); err != nil {
				panic(err)
			}
		}},
		{"helm_release", func(f *kindBuildCaptureFixture) {
			f.deployment.Annotations["meta.helm.sh/release-name"] = "unrelated-fixture"
			if err := f.client.Update(context.Background(), f.deployment); err != nil {
				panic(err)
			}
		}},
		{"selector", func(f *kindBuildCaptureFixture) {
			f.deployment.Spec.Selector.MatchLabels = map[string]string{"unrelated-fixture": "selector"}
			if err := f.client.Update(context.Background(), f.deployment); err != nil {
				panic(err)
			}
		}},
		{"rs_owner", func(f *kindBuildCaptureFixture) {
			f.replicaSet.OwnerReferences[0].UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.replicaSet); err != nil {
				panic(err)
			}
		}},
		{"pod_owner", func(f *kindBuildCaptureFixture) {
			f.pod.OwnerReferences[0].UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"two_pods", func(f *kindBuildCaptureFixture) {
			second := f.pod.DeepCopy()
			second.ResourceVersion = ""
			second.Name += "-other"
			second.UID = "other-pod"
			base := clientfake.NewClientBuilder().WithObjects(f.namespace, f.deployment, f.replicaSet, f.pod, second,
				f.node).Build()
			k8sClient = base
		}},
		{"pod_not_ready", func(f *kindBuildCaptureFixture) {
			f.pod.Status.Conditions = nil
			if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"host_network", func(f *kindBuildCaptureFixture) {
			f.pod.Spec.HostNetwork = true
			if err := f.client.Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"image_ref", func(f *kindBuildCaptureFixture) {
			f.pod.Spec.Containers[0].Image = "foreign:tag"
			if err := f.client.Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"image_id_prefix", func(f *kindBuildCaptureFixture) {
			f.pod.Status.ContainerStatuses[0].ImageID = "containerd://" + f.run.input.Images[kindManagerContainer].ImageID
			if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"container_id_empty", func(f *kindBuildCaptureFixture) {
			f.pod.Status.ContainerStatuses[0].ContainerID = ""
			if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"wrong_node", func(f *kindBuildCaptureFixture) {
			f.pod.Spec.NodeName = "foreign-node"
			if err := f.client.Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"node_not_ready", func(f *kindBuildCaptureFixture) {
			f.node.Status.Conditions = nil
			if err := f.client.Status().Update(context.Background(), f.node); err != nil {
				panic(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newKindBuildCaptureFixture(t)
			evidence, err := f.capture(context.Background(), func() { tc.change(f) })
			if err == nil || evidence.Authenticated || len(f.created) != 0 || f.tokenCalls != 0 || f.reviewCalls != 0 {
				t.Fatalf("invalid identity reached effects: %v", err)
			}
		})
	}
}

func TestKindManagerBuildInfoReadmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*kindBuildCaptureFixture)
	}{
		{"namespace_changed", func(f *kindBuildCaptureFixture) {
			f.namespace.UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.namespace); err != nil {
				panic(err)
			}
		}},
		{"deployment", func(f *kindBuildCaptureFixture) {
			f.deployment.UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.deployment); err != nil {
				panic(err)
			}
		}},
		{"replicaset", func(f *kindBuildCaptureFixture) {
			f.replicaSet.UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.replicaSet); err != nil {
				panic(err)
			}
		}},
		{"pod", func(f *kindBuildCaptureFixture) {
			f.pod.UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"container", func(f *kindBuildCaptureFixture) {
			f.pod.Status.ContainerStatuses[0].ContainerID = kindBuildReplacement
			if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"restart", func(f *kindBuildCaptureFixture) {
			f.pod.Status.ContainerStatuses[0].RestartCount++
			if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"image_representation", func(f *kindBuildCaptureFixture) {
			f.pod.Status.ContainerStatuses[0].ImageID = f.run.input.Images[kindManagerContainer].LoadedDigest
			if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
				panic(err)
			}
		}},
		{"node", func(f *kindBuildCaptureFixture) {
			f.node.UID = kindBuildReplacement
			if err := f.client.Update(context.Background(), f.node); err != nil {
				panic(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newKindBuildCaptureFixture(t)
			evidence, err := f.capture(context.Background(), func() { f.mutate = tc.change })
			if err == nil || evidence.Authenticated || !evidence.Cleanup || f.tokenCalls != 1 || f.stops != 1 {
				t.Fatalf("replacement inherited proof or skipped cleanup: %+v %v", evidence, err)
			}
		})
	}
}

func kindBuildSPDYFixture(t *testing.T, target, fault string) (*rest.Config, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	var lock sync.Mutex
	var connections []httpstream.Connection
	var copies sync.WaitGroup
	errorWritten := make(chan struct{})
	api := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/v1/namespaces/pgcopydb-e2e-system/pods/manager-exact/portforward" ||
			r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer fixture-suite-admin" {
			t.Error("forwarder changed Pod target or API credentials")
			http.Error(w, "wrong route", http.StatusForbidden)
			return
		}
		if fault == "early_exit" {
			http.Error(w, kindBuildSecretSentinel, http.StatusForbidden)
			return
		}
		if fault == "stalled_upgrade" {
			<-r.Context().Done()
			return
		}
		if _, err := httpstream.Handshake(r, w, []string{portforward.PortForwardProtocolV1Name}); err != nil {
			t.Error("SPDY handshake failed")
			return
		}
		upgrader := streamspdy.NewResponseUpgrader()
		connection := upgrader.UpgradeResponse(w, r, func(stream httpstream.Stream, replied <-chan struct{}) error {
			copies.Go(func() {
				<-replied
				if stream.Headers().Get(corev1.PortHeader) != "8443" {
					t.Error("forwarder changed remote port")
					_ = stream.Close()
					return
				}
				if stream.Headers().Get(corev1.StreamType) == corev1.StreamTypeError {
					if fault == "remote_error" {
						_, _ = io.WriteString(stream, kindBuildSecretSentinel)
						close(errorWritten)
					}
					_ = stream.Close()
					return
				}
				if fault == "remote_error" {
					<-errorWritten
					_ = stream.Close()
					return
				}
				remote, err := net.DialTimeout("tcp", target, time.Second)
				if err != nil {
					t.Error("fixture target unavailable")
					_ = stream.Close()
					return
				}
				copied := make(chan struct{})
				go func() { defer close(copied); _, _ = io.Copy(stream, remote); _ = stream.Close() }()
				_, _ = io.Copy(remote, stream)
				_ = remote.Close()
				<-copied
			})
			return nil
		})
		if connection == nil {
			return
		}
		lock.Lock()
		connections = append(connections, connection)
		lock.Unlock()
		<-connection.CloseChan()
	}))
	api.Config.ErrorLog = log.New(io.Discard, "", 0)
	api.StartTLS()
	t.Cleanup(func() {
		lock.Lock()
		for _, connection := range connections {
			_ = connection.Close()
		}
		lock.Unlock()
		api.CloseClientConnections()
		api.Close()
		done := make(chan struct{})
		go func() { copies.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("fixture tunnel stream did not join")
		}
	})
	cfg := &rest.Config{Host: api.URL, BearerToken: kindBuildSuiteAdmin,
		TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: kindBuildCertificatePEM,
			Bytes: api.Certificate().Raw})}}
	return cfg, &calls
}

func TestKindManagerTunnelExactPodAndJoin(t *testing.T) {
	metrics := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+kindBuildSecretSentinel {
			t.Error("tunnel scrape lost memory-only bearer")
		}
		_, _ = io.WriteString(w, kindBuildFixtureMetric(kindBuildFixtureVersion))
	}))
	defer metrics.Close()
	cfg, calls := kindBuildSPDYFixture(t, metrics.Listener.Addr().String(), "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base, stop, err := forwardKindManagerMetrics(ctx, cfg, defaultOperatorNamespace, "manager-exact")
	if err != nil || stop == nil || !strings.HasPrefix(base, "https://127.0.0.1:") {
		t.Fatalf("exact Pod tunnel unavailable: %v", err)
	}
	observed, scrapeErr := readKindManagerBuildInfo(ctx, base, kindBuildSecretSentinel, kindBuildFixtureVersion)
	if scrapeErr != nil || observed != kindBuildFixtureVersion || calls.Load() != 1 {
		t.Fatalf("actual forwarded TLS scrape failed: %v", scrapeErr)
	}
	if err := stop(); err != nil {
		t.Fatalf("tunnel did not close/join: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatal("idempotent tunnel stop changed result")
	}
	endpoint, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialTimeout("tcp", endpoint.Host, 100*time.Millisecond)
	if err == nil {
		_ = connection.Close()
		t.Fatal("owned tunnel listener survived stop")
	}
	if cfg.Insecure || len(cfg.CAData) == 0 || cfg.BearerToken != kindBuildSuiteAdmin {
		t.Fatal("tunnel mutated API trust/auth config")
	}
}

func TestKindManagerTunnelCancellationAndEarlyExit(t *testing.T) {
	for _, fault := range []string{"stalled_upgrade", "early_exit"} {
		t.Run(fault, func(t *testing.T) {
			cfg, _ := kindBuildSPDYFixture(t, "", fault)
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, stop, err := forwardKindManagerMetrics(ctx, cfg, defaultOperatorNamespace, "manager-exact")
				if stop != nil {
					err = errors.Join(err, stop())
				}
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil || strings.Contains(err.Error(), kindBuildSecretSentinel) {
					t.Fatal("failed tunnel qualified or leaked API error")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("tunnel cancellation failed to close/join blocked upgrade")
			}
		})
	}
}

func TestKindManagerTunnelReadinessBound(t *testing.T) {
	cfg, _ := kindBuildSPDYFixture(t, "", "stalled_upgrade")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, stop, err := forwardKindManagerMetrics(ctx, cfg, defaultOperatorNamespace, "manager-exact")
		if stop != nil {
			err = errors.Join(err, stop())
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled upgrade qualified")
		}
	case <-time.After(18 * time.Second):
		cancel()
		<-done
		t.Fatal("readiness exceeded15-second ceiling plus bounded join")
	}
}

func TestKindManagerTunnelSuppressesRemoteErrorBytes(t *testing.T) {
	childCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestKindManagerTunnelLogChild$")
	command.Env = []string{"E2E_TEST_CHILD=kind-manager-tunnel-log"}
	command.WaitDelay = time.Second
	output, err := command.CombinedOutput()
	if bytes.Contains(output, []byte(kindBuildSecretSentinel)) {
		t.Fatal("client-go tunnel logging exposed remote error bytes")
	}
	if err != nil {
		t.Fatalf("isolated tunnel failure control failed: %v", err)
	}
	if !bytes.Contains(output, []byte(errKindManagerMetricsTunnel.Error())) {
		t.Fatal("actual SDK error logging path was not exercised")
	}
}

func TestKindManagerTunnelLogChild(t *testing.T) {
	if os.Getenv("E2E_TEST_CHILD") != "kind-manager-tunnel-log" {
		return
	}
	defer klog.Flush()
	cfg, _ := kindBuildSPDYFixture(t, "", "remote_error")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	base, stop, err := forwardKindManagerMetrics(ctx, cfg, defaultOperatorNamespace, "manager-exact")
	if err != nil || stop == nil {
		t.Fatal("fixture could not start actual SDK tunnel")
	}
	if _, err := readKindManagerBuildInfo(ctx, base, kindBuildSecretSentinel, kindBuildFixtureVersion); err == nil {
		t.Fatal("nonempty tunnel error stream became success")
	}
	_ = stop()
}

func TestKindManagerRuntimeWitness(t *testing.T) {
	f := newKindBuildCaptureFixture(t)
	valid, err := f.capture(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*kindManagerRuntimeEvidence)
	}{
		{kindBuildAccepted, func(*kindManagerRuntimeEvidence) {}},
		{"historical", func(e *kindManagerRuntimeEvidence) { *e = kindManagerRuntimeEvidence{} }},
		{kindBuildWrongVersion, func(e *kindManagerRuntimeEvidence) { e.ObservedVersion = "m7-bbbbbbbbbbbb" }},
		{"unauthenticated", func(e *kindManagerRuntimeEvidence) { e.Authenticated = false }},
		{"unclean", func(e *kindManagerRuntimeEvidence) { e.Cleanup = false }},
		{kindBuildMissingUIDCase, func(e *kindManagerRuntimeEvidence) { e.PodUID = "" }},
		{"bad_uid", func(e *kindManagerRuntimeEvidence) { e.NamespaceUID = "bad uid" }},
		{"wrong_config", func(e *kindManagerRuntimeEvidence) { e.ImageConfigID = e.ImageManifestDigest }},
		{"wrong_manifest", func(e *kindManagerRuntimeEvidence) { e.ImageManifestDigest = e.ImageConfigID }},
		{"image_alias", func(e *kindManagerRuntimeEvidence) { e.ContainerImageID = "containerd://" + e.ContainerImageID }},
		{"bad_container", func(e *kindManagerRuntimeEvidence) { e.ContainerID = "bad container" }},
		{"negative_restart", func(e *kindManagerRuntimeEvidence) { e.RestartCount = -1 }},
		{"nonfinite_time", func(e *kindManagerRuntimeEvidence) { e.ObservedAt = math.NaN() }},
		{"time_after_lease", func(e *kindManagerRuntimeEvidence) { e.ObservedAt = f.run.input.LeaseExpiresAt + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := valid
			tc.change(&evidence)
			err := kindManagerRuntimeWitness(f.run, evidence)
			if (err == nil) != (tc.name == kindBuildAccepted) {
				t.Fatalf("runtime witness accepted=%v: %v", tc.name, err)
			}
		})
	}
}

func TestKindManagerNamespaceGuardUsesCallerContext(t *testing.T) {
	oldClient, oldRun, oldCtx := k8sClient, activeKindSuite, ctx
	t.Cleanup(func() { k8sClient, activeKindSuite, ctx = oldClient, oldRun, oldCtx })
	ctx = context.Background()
	activeKindSuite = &kindSuite{input: kindSuiteInput{RunID: strings.Repeat("a", 32)},
		operatorNamespaceUID: "namespace-guard-owned", operatorNamespaceAbsent: true}
	base := clientfake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{Get: func(callCtx context.Context,
		_ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
		if callCtx.Err() == nil {
			return errors.New("namespace guard ignored caller cancellation")
		}
		return callCtx.Err()
	}}).Build()
	scoped := featureLabelingClient{Client: base}
	k8sClient = scoped
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err := scoped.Delete(canceled, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "owned",
		Namespace: defaultOperatorNamespace}})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("namespace guard bypassed the cleanup request deadline")
	}
}

func TestKindManagerMetricsPolicyMatchesApprovedObjects(t *testing.T) {
	id := strings.Repeat("a", 32)
	objects := kindMetricsAuthorizationObjects(id)
	if len(objects) != 3 {
		t.Fatal("temporary policy changed its object inventory")
	}
	sa, ok := objects[0].want.(*corev1.ServiceAccount)
	if !ok || sa.Name != "m7-metrics-observer-"+id || sa.Namespace != defaultOperatorNamespace ||
		sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken || len(sa.Secrets) != 0 ||
		len(sa.ImagePullSecrets) != 0 {
		t.Fatal("temporary ServiceAccount escaped reviewed scope")
	}
	role, ok := objects[1].want.(*rbacv1.ClusterRole)
	wantedRules := []rbacv1.PolicyRule{{NonResourceURLs: []string{kindBuildExpectedMetricsPath},
		Verbs: []string{kindBuildPolicyVerb}}}
	if !ok || role.Name != "m7-"+id+"-metrics" || role.Namespace != "" || role.AggregationRule != nil ||
		!reflect.DeepEqual(role.Rules, wantedRules) {
		t.Fatal("temporary grant is broader than sole GET /metrics")
	}
	binding, ok := objects[2].want.(*rbacv1.ClusterRoleBinding)
	expectedRef := rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role.Name}
	expectedSubjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: sa.Name, Namespace: defaultOperatorNamespace}}
	if !ok || binding.Name != role.Name || binding.Namespace != "" || !reflect.DeepEqual(binding.RoleRef,
		expectedRef) || !reflect.DeepEqual(binding.Subjects, expectedSubjects) {
		t.Fatal("temporary binding changed the one-SA subject")
	}
	for _, record := range objects {
		if len(record.want.GetLabels()) != 2 || record.want.GetLabels()[kindRunLabel] != id ||
			record.want.GetLabels()["pgcopydb-operator.io/m7-purpose"] != "runtime-version-observer" ||
			len(record.want.GetOwnerReferences()) != 0 {
			t.Fatal("temporary policy metadata differs from approved template")
		}
	}
}

func TestKindManagerBuildInfoCancellationStillCleans(t *testing.T) {
	f := newKindBuildCaptureFixture(t)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	evidence, err := f.capture(parent, func() { f.mutate = func(*kindBuildCaptureFixture) { cancel() } })
	if err == nil || evidence.Authenticated || !evidence.Cleanup || f.tokenCalls != 1 || f.stops != 1 ||
		!slices.Equal(f.deleted, []string{kindBuildBindingType, "sa", kindBuildRoleType}) {
		t.Fatal("cancellation escaped reserved exact cleanup")
	}
}

func TestKindManagerBuildInfoCleanupRefusesReplacement(t *testing.T) {
	f := newKindBuildCaptureFixture(t)
	evidence, err := f.capture(context.Background(), func() {
		f.mutate = func(f *kindBuildCaptureFixture) {
			role := &rbacv1.ClusterRole{}
			if err := f.client.Get(context.Background(), client.ObjectKey{Name: "m7-" + f.run.input.RunID +
				"-metrics"}, role); err != nil {
				panic(err)
			}
			role.Rules = []rbacv1.PolicyRule{{Verbs: []string{"*"}, Resources: []string{"*"}}}
			if err := f.client.Update(context.Background(), role); err != nil {
				panic(err)
			}
		}
	})
	var handoff kindManagerCleanupHandoff
	if err == nil || evidence.Cleanup || !errors.As(err, &handoff) || slices.Contains(f.deleted, kindBuildRoleType) {
		t.Fatal("mutated auth object was deleted or qualified as cleaned")
	}
}

func TestKindManagerCleanupRequiresAbsenceReadback(t *testing.T) {
	f := newKindBuildCaptureFixture(t)
	records, err := prepareKindMetricsAuthorization(context.Background(), f.run)
	if err != nil {
		t.Fatal(err)
	}
	if err := createKindMetricsAuthorization(context.Background(), f.run, records); err != nil {
		t.Fatal(err)
	}
	base := f.client
	k8sClient = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Delete: func(context.Context,
		client.WithWatch, client.Object, ...client.DeleteOption) error {
		return nil
	}})
	bound, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if err := cleanupKindMetricsAuthObject(bound, records[2]); err == nil {
		t.Fatal("accepted Delete without absence became successful cleanup")
	}
}

func TestKindManagerBuildInfoRejectsReplacementBeforeScrape(t *testing.T) {
	for _, fault := range []string{kindBuildPostAuthReplacement, "ready_replacement"} {
		t.Run(fault, func(t *testing.T) {
			f := newKindBuildCaptureFixture(t)
			evidence, err := f.capture(context.Background(), func() { f.fault = fault })
			if err == nil || evidence.Authenticated || f.metricCalls != 0 || !evidence.Cleanup {
				t.Fatal("known replacement received bearer scrape or retained proof")
			}
			if fault == kindBuildPostAuthReplacement && f.tokenCalls != 0 {
				t.Fatal("known replacement reached token issuance")
			}
		})
	}
}

func TestKindManagerBuildInfoHTTPRequestBound(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter,
		r *http.Request) {
		calls.Add(1)
		<-r.Context().Done()
	}))
	defer server.Close()
	parent, cancel := context.WithTimeout(context.Background(), 6500*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := readKindManagerBuildInfo(parent, server.URL, kindBuildSecretSentinel, kindBuildFixtureVersion)
	if err == nil || calls.Load() != 2 || time.Since(started) > 8*time.Second {
		t.Fatal("per-request timeout or enclosing scrape bound was not enforced")
	}
}

func TestKindManagerTunnelPreservesAPITLS(t *testing.T) {
	cfg, calls := kindBuildSPDYFixture(t, "", "early_exit")
	wrong := kindBuildAlternateCertificate(t)
	cfg.CAData = pem.EncodeToMemory(&pem.Block{Type: kindBuildCertificatePEM, Bytes: wrong.Certificate[0]})
	bound, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, stop, err := forwardKindManagerMetrics(bound, cfg, defaultOperatorNamespace, "manager-exact")
	if stop != nil {
		_ = stop()
	}
	if err == nil || calls.Load() != 0 || cfg.Insecure {
		t.Fatal("API TLS verification was weakened to reach the tunnel")
	}
}

func kindBuildRegistryImage(t *testing.T, image kindImage, nodeName string) (kindImage, string) {
	t.Helper()
	node := kindNodeNamePattern.FindStringSubmatch(nodeName)
	if len(node) != 2 {
		t.Fatal("invalid fixture node")
	}
	index := "sha256:" + strings.Repeat("d", 64)
	image.RegistryIndexDigest = &index
	image.Digest, image.Architecture = image.LoadedDigest, goruntime.GOARCH
	if !strings.Contains(image.Ref, "@") {
		image.Ref += "@" + image.LoadedDigest
	}
	raw := strings.SplitN(image.Ref, "@", 2)[0] + "@" + index
	image.NodeBindings = map[string]kindNodeBinding{}
	for i, suffix := range kindNodeSuffixes {
		ids := []string{image.ImageID, image.LoadedDigest, image.Ref, raw}
		slices.Sort(ids)
		image.NodeBindings[node[1]+"-"+suffix] = kindNodeBinding{NodeID: strings.Repeat(strconv.Itoa(i+1), 64),
			RequestedRef: image.Ref, ManifestDigest: image.LoadedDigest, ConfigDigest: image.ImageID, OS: kindLinux,
			Architecture: image.Architecture, AcceptedImageIDs: ids}
	}
	data, err := json.Marshal(image)
	if err != nil {
		t.Fatal(err)
	}
	var decoded kindImage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(image, decoded) {
		t.Fatal("private image binding did not round-trip")
	}
	return decoded, raw
}

func TestKindManagerBuildInfoRegistryImageID(t *testing.T) {
	const imageChanged = "raw_image_changed"
	for _, fault := range []string{kindBuildAccepted, kindBuildReviewFault, kindBuildStopFault, imageChanged} {
		t.Run(fault, func(t *testing.T) {
			f := newKindBuildCaptureFixture(t)
			image, alias := kindBuildRegistryImage(t, f.run.input.Images[kindManagerContainer], f.node.Name)
			f.run.input.Images[kindManagerContainer] = image
			f.pod.Status.ContainerStatuses[0].ImageID = alias
			if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
				t.Fatal(err)
			}
			evidence, err := f.capture(context.Background(), func() {
				if fault == imageChanged {
					f.mutate = func(f *kindBuildCaptureFixture) {
						f.pod.Status.ContainerStatuses[0].ImageID = image.LoadedDigest
						if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
							panic(err)
						}
					}
				} else if fault != kindBuildAccepted {
					f.fault = fault
				}
			})
			if fault != kindBuildAccepted {
				if err == nil || evidence.Authenticated && evidence.Cleanup || kindManagerRuntimeWitness(f.run, evidence) == nil {
					t.Fatal("registry imageID bypassed authentication, cleanup, or raw imageID continuity")
				}
				if fault == imageChanged && (!evidence.Cleanup || f.metricCalls != 1) {
					t.Fatal("imageID change skipped authenticated capture or exact cleanup")
				}
				return
			}
			if err != nil || !evidence.Authenticated || !evidence.Cleanup || evidence.ContainerImageID != alias ||
				evidence.ImageConfigID != "sha256:"+strings.Repeat("b", 64) ||
				evidence.ImageManifestDigest != "sha256:"+strings.Repeat("c", 64) || f.metricCalls != 1 {
				t.Fatalf("source-bound registry imageID failed raw authenticated capture: %+v %v", evidence, err)
			}
			data, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 16 || fields["container_image_id"] != alias ||
				fields["image_manifest_digest"] != image.LoadedDigest {
				t.Fatal("runtime envelope or raw imageID changed")
			}
		})
	}
}

func TestKindManagerRuntimeWitnessRegistryImageID(t *testing.T) {
	f := newKindBuildCaptureFixture(t)
	evidence, err := f.capture(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	image, alias := kindBuildRegistryImage(t, f.run.input.Images[kindManagerContainer], f.node.Name)
	f.run.input.Images[kindManagerContainer] = image
	evidence.ContainerImageID = alias
	observed := time.Date(2026, time.November, 10, 0, 0, 0, 0, time.UTC)
	evidence.ObservedAt = float64(observed.Unix())
	f.run.input.LeaseExpiresAt = float64(observed.Add(time.Hour).Unix())
	if err := kindManagerRuntimeWitness(f.run, evidence); err != nil {
		t.Fatalf("source-bound registry imageID witness refused: %v", err)
	}
	if evidence.ContainerImageID != alias {
		t.Fatal("raw imageID changed")
	}
}

func TestKindManagerImageIDBindings(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*kindImage, *string, *string)
		accepted bool
	}{
		{"single_platform", func(image *kindImage, raw, _ *string) {
			index := *image.RegistryIndexDigest
			image.RegistryIndexDigest = nil
			*raw = image.Ref
			for node, b := range image.NodeBindings {
				b.AcceptedImageIDs = slices.DeleteFunc(b.AcceptedImageIDs,
					func(id string) bool { return strings.HasSuffix(id, "@"+index) })
				image.NodeBindings[node] = b
			}
		}, true},
		{"other_node_only", func(image *kindImage, raw, node *string) {
			b := image.NodeBindings[*node]
			b.AcceptedImageIDs = slices.DeleteFunc(b.AcceptedImageIDs, func(id string) bool { return id == *raw })
			image.NodeBindings[*node] = b
		}, false},
		{"bound_index", func(*kindImage, *string, *string) {}, true},
		{"bare_config", func(image *kindImage, raw, _ *string) { *raw = image.ImageID }, true},
		{"bare_manifest", func(image *kindImage, raw, _ *string) { *raw = image.LoadedDigest }, true},
		{"requested_manifest", func(image *kindImage, raw, _ *string) { *raw = image.Ref }, true},
		{"missing_index", func(image *kindImage, _, _ *string) { image.RegistryIndexDigest = nil }, false},
		{"config_as_index", func(image *kindImage, _, _ *string) { image.RegistryIndexDigest = &image.ImageID }, false},
		{"manifest_as_index", func(image *kindImage, _, _ *string) { image.RegistryIndexDigest = &image.LoadedDigest },
			false},
		{"missing_config", func(image *kindImage, _, _ *string) { image.ImageID = "" }, false},
		{"missing_registry_manifest", func(image *kindImage, _, _ *string) { image.LoadedDigest = "" }, false},
		{"missing_binding", func(image *kindImage, _, node *string) { delete(image.NodeBindings, *node) }, false},
		{"wrong_node", func(_ *kindImage, _, node *string) { *node = kindBuildForeignNode }, false},
		{"foreign_binding", func(image *kindImage, _, node *string) {
			image.NodeBindings[kindBuildForeignNode] = image.NodeBindings[*node]
		}, false},
		{"wrong_repository", func(_ *kindImage, raw, _ *string) {
			*raw = strings.Replace(*raw, "ghcr.io/example/", "ghcr.io/foreign/", 1)
		}, false},
		{"mutable_tag", func(_ *kindImage, raw, _ *string) { *raw = "ghcr.io/example/manager:test" }, false},
		{"bare_index", func(image *kindImage, raw, _ *string) { *raw = *image.RegistryIndexDigest }, false},
		{"containerd_prefix", func(_ *kindImage, raw, _ *string) { *raw = "containerd://" + *raw }, false},
		{"docker_pullable_prefix", func(_ *kindImage, raw, _ *string) { *raw = "docker-pullable://" + *raw }, false},
		{"unknown_prefix", func(_ *kindImage, raw, _ *string) { *raw = "unknown://" + *raw }, false},
		{"archive_alias", func(_ *kindImage, raw, _ *string) {
			*raw = "docker.io/library/import-2026-11-09@sha256:" + strings.Repeat("d", 64)
		}, false},
		{"wrong_platform", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			b.Architecture = "unsupported-platform"
			image.NodeBindings[*node] = b
		}, false},
		{"wrong_os", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			b.OS = "windows"
			image.NodeBindings[*node] = b
		}, false},
		{"wrong_variant", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			v := "v7"
			b.Variant = &v
			image.NodeBindings[*node] = b
		}, false},
		{"wrong_config", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			b.ConfigDigest = image.LoadedDigest
			image.NodeBindings[*node] = b
		}, false},
		{"wrong_manifest", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			b.ManifestDigest = image.ImageID
			image.NodeBindings[*node] = b
		}, false},
		{"wrong_requested_ref", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			b.RequestedRef = "ghcr.io/foreign/manager@" + image.LoadedDigest
			image.NodeBindings[*node] = b
		}, false},
		{"empty_ids", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			b.AcceptedImageIDs = nil
			image.NodeBindings[*node] = b
		}, false},
		{"duplicate_ids", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			b.AcceptedImageIDs = append(b.AcceptedImageIDs, b.AcceptedImageIDs[len(b.AcceptedImageIDs)-1])
			image.NodeBindings[*node] = b
		}, false},
		{"unsorted_ids", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			slices.Reverse(b.AcceptedImageIDs)
			image.NodeBindings[*node] = b
		}, false},
		{"forged_accepted_repository", func(image *kindImage, raw, node *string) {
			b := image.NodeBindings[*node]
			*raw = "ghcr.io/foreign/manager@" + image.LoadedDigest
			b.AcceptedImageIDs = append(b.AcceptedImageIDs, *raw)
			slices.Sort(b.AcceptedImageIDs)
			image.NodeBindings[*node] = b
		}, false},
		{"reused_node_id", func(image *kindImage, _, node *string) {
			b := image.NodeBindings[*node]
			b.NodeID = strings.Repeat("1", 64)
			image.NodeBindings[*node] = b
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newKindBuildCaptureFixture(t)
			image, raw := kindBuildRegistryImage(t, f.run.input.Images[kindManagerContainer], f.node.Name)
			node := f.node.Name
			tc.change(&image, &raw, &node)
			f.run.input.Images[kindManagerContainer] = image
			f.pod.Spec.NodeName = node
			f.deployment.Spec.Template.Spec.Containers[0].Image = image.Ref
			f.replicaSet.Spec.Template.Spec.Containers[0].Image = image.Ref
			f.pod.Spec.Containers[0].Image = image.Ref
			if err := f.client.Update(context.Background(), f.deployment); err != nil {
				t.Fatal(err)
			}
			if err := f.client.Update(context.Background(), f.replicaSet); err != nil {
				t.Fatal(err)
			}
			if err := f.client.Update(context.Background(), f.pod); err != nil {
				t.Fatal(err)
			}
			f.pod.Status.ContainerStatuses[0].ImageID = raw
			if err := f.client.Status().Update(context.Background(), f.pod); err != nil {
				t.Fatal(err)
			}
			var evidence kindManagerRuntimeEvidence
			identity, err := readKindManagerIdentity(context.Background(), f.run, &evidence)
			if (err == nil) != tc.accepted {
				t.Fatalf("live imageID accepted=%t: %v", tc.accepted, err)
			}
			if tc.accepted && (identity.imageID != raw || evidence.ContainerImageID != raw) {
				t.Fatal("live imageID was normalized")
			}
			evidence = kindManagerRuntimeEvidence{ExpectedVersion: kindBuildFixtureVersion,
				ObservedVersion: kindBuildFixtureVersion, Authenticated: true, Cleanup: true,
				NamespaceUID: string(f.namespace.UID), DeploymentUID: "deployment-owned", ReplicaSetUID: "replicaset-owned",
				PodUID: "pod-owned", NodeName: node, ImageRef: image.Ref, ImageConfigID: image.ImageID,
				ImageManifestDigest: image.LoadedDigest, ContainerImageID: raw, ContainerID: "containerd://exact-manager",
				ObservedAt: float64(time.Now().Unix())}
			if err := kindManagerRuntimeWitness(f.run, evidence); (err == nil) != tc.accepted {
				t.Fatalf("retained imageID accepted=%t: %v", tc.accepted, err)
			}
		})
	}
}
