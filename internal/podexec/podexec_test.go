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

package podexec

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// execChannels is the remotecommand channel layout the client multiplexes
// over one websocket: stdin, stdout, stderr, error, resize.
var execChannels = []wsstream.ChannelType{
	wsstream.ReadChannel, wsstream.WriteChannel, wsstream.WriteChannel,
	wsstream.WriteChannel, wsstream.ReadChannel,
}

// serveExecSuccess answers one exec request over the v5 websocket protocol:
// out goes to the stdout channel, then the connection closes normally with the
// error channel empty, which the client reads as a successful exec.
func serveExecSuccess(w http.ResponseWriter, r *http.Request, out []byte) {
	conn := wsstream.NewConn(map[string]wsstream.ChannelProtocolConfig{
		"v5.channel.k8s.io": {Binary: true, Channels: execChannels},
	})
	conn.SetIdleTimeout(time.Minute)
	_, streams, err := conn.Open(w, r)
	if err != nil {
		return
	}
	defer conn.Close() //nolint:errcheck
	_, _ = streams[1].Write(out)
}

// apiServer fakes the parts of the Kubernetes API this package talks to: pod
// lists, pod logs, and the exec subresource (which never upgrades to SPDY, so
// streaming fails there by design). It records request URLs because the
// contract under test is what the client asks the server for: pod filtering
// happens server-side via selectors, log routing via the pod name in the path.
type apiServer struct {
	*httptest.Server
	pods     []corev1.Pod
	fail     bool
	requests []*url.URL
	// execOut, when set, makes the exec subresource succeed over the v5
	// websocket protocol with this stdout instead of refusing the upgrade.
	execOut []byte
}

// runnerContainer pins the container name requests must target; deliberately
// a test-local copy so a rename in the source fails here.
const runnerContainer = "pgcopydb"

// paramTrue is the query-parameter value boolean options serialize to.
const paramTrue = "true"

func newAPIServer(t *testing.T) *apiServer {
	t.Helper()
	s := &apiServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests = append(s.requests, r.URL)
		switch {
		case s.fail:
			http.Error(w, "boom", http.StatusInternalServerError)
		case strings.HasSuffix(r.URL.Path, "/pods"):
			w.Header().Set("Content-Type", "application/json")
			list := corev1.PodList{
				TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"},
				Items:    s.pods,
			}
			_ = json.NewEncoder(w).Encode(list)
		case strings.HasSuffix(r.URL.Path, "/log"):
			_, _ = w.Write([]byte("clone step 9/9 done\n"))
		case strings.HasSuffix(r.URL.Path, "/exec") && s.execOut != nil:
			serveExecSuccess(w, r, s.execOut)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *apiServer) lastRequest(t *testing.T) *url.URL {
	t.Helper()
	if len(s.requests) == 0 {
		t.Fatal("no request reached the API server")
	}
	return s.requests[len(s.requests)-1]
}

func newExec(t *testing.T, host string) *Exec {
	t.Helper()
	e, err := New(&rest.Config{Host: host})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func workerPod(name string, created time.Time) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "ns",
			Labels:            map[string]string{"job-name": "mig-run-1"},
			CreationTimestamp: metav1.NewTime(created),
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestNew_RejectsBrokenConfig(t *testing.T) {
	_, err := New(&rest.Config{
		Host:            "https://example.invalid",
		TLSClientConfig: rest.TLSClientConfig{CAData: []byte("not a certificate")},
	})
	if err == nil {
		t.Fatal("want error for unusable rest config")
	}
}

func TestRunningPod(t *testing.T) {
	srv := newAPIServer(t)
	srv.pods = []corev1.Pod{workerPod("mig-run-1-abc", time.Now())}
	e := newExec(t, srv.URL)

	name, err := e.RunningPod(context.Background(), "ns", "mig-run-1")
	if err != nil {
		t.Fatal(err)
	}
	if name != "mig-run-1-abc" {
		t.Fatalf("pod name = %q", name)
	}
	// Pending/Succeeded pods are skipped by the field selector: the apiserver
	// does the filtering, so the request must carry it, along with the Job
	// controller's job-name label selector.
	q := srv.lastRequest(t).Query()
	if got := q.Get("fieldSelector"); got != "status.phase=Running" {
		t.Fatalf("fieldSelector = %q, want status.phase=Running", got)
	}
	if got := q.Get("labelSelector"); got != "job-name=mig-run-1" {
		t.Fatalf("labelSelector = %q", got)
	}
}

func TestRunningPod_NoneIsNotAnError(t *testing.T) {
	srv := newAPIServer(t)
	e := newExec(t, srv.URL)
	name, err := e.RunningPod(context.Background(), "ns", "mig-run-1")
	if err != nil {
		t.Fatal(err)
	}
	if name != "" {
		t.Fatalf("want empty name for no running pod, got %q", name)
	}
}

func TestRunningPod_ListError(t *testing.T) {
	srv := newAPIServer(t)
	srv.fail = true
	e := newExec(t, srv.URL)
	if _, err := e.RunningPod(context.Background(), "ns", "mig-run-1"); err == nil {
		t.Fatal("want error when the pod list fails")
	}
}

func TestJobLogs_PicksNewestPod(t *testing.T) {
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	srv := newAPIServer(t)
	// Deliberately unordered: the newest pod (a retry under the Job's own
	// backoffLimit) sits in the middle.
	srv.pods = []corev1.Pod{
		workerPod("attempt-old", base),
		workerPod("attempt-newest", base.Add(2*time.Minute)),
		workerPod("attempt-middle", base.Add(time.Minute)),
	}
	e := newExec(t, srv.URL)

	out, err := e.JobLogs(context.Background(), "ns", "mig-run-1", 44)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "clone step 9/9 done\n" {
		t.Fatalf("logs = %q", out)
	}
	req := srv.lastRequest(t)
	if !strings.HasSuffix(req.Path, "/pods/attempt-newest/log") {
		t.Fatalf("logs fetched from %q, want the newest pod", req.Path)
	}
	q := req.Query()
	if q.Get("container") != runnerContainer || q.Get("tailLines") != "44" {
		t.Fatalf("log query = %v, want container=%s tailLines=44", q, runnerContainer)
	}
}

// TestJobLogsTimestamps pins the one thing the variant adds: the request asks
// the runtime to stamp every line, which is what the zombie check dates the
// supervisor-death marker with.
func TestJobLogsTimestamps(t *testing.T) {
	srv := newAPIServer(t)
	srv.pods = []corev1.Pod{workerPod("mig-run-1-abc", time.Now())}
	e := newExec(t, srv.URL)

	out, err := e.JobLogsTimestamps(context.Background(), "ns", "mig-run-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "clone step 9/9 done\n" {
		t.Fatalf("logs = %q", out)
	}
	q := srv.lastRequest(t).Query()
	if q.Get("timestamps") != paramTrue || q.Get("tailLines") != "7" {
		t.Fatalf("log query = %v, want timestamps=true tailLines=7", q)
	}
}

func TestJobLogs_NoPods(t *testing.T) {
	srv := newAPIServer(t)
	e := newExec(t, srv.URL)
	if _, err := e.JobLogs(context.Background(), "ns", "mig-run-1", 10); err == nil ||
		!strings.Contains(err.Error(), "no pods found") {
		t.Fatalf("want 'no pods found' error, got %v", err)
	}
}

func TestJobLogs_ListError(t *testing.T) {
	srv := newAPIServer(t)
	srv.fail = true
	e := newExec(t, srv.URL)
	if _, err := e.JobLogs(context.Background(), "ns", "mig-run-1", 10); err == nil {
		t.Fatal("want error when the pod list fails")
	}
}

// TestInPod_RequestShape drives InPod against a server that cannot upgrade to
// SPDY: the stream fails (that error path is the one under test), but not
// before the exec request reaches the server, where its shape is asserted.
// The success path needs a real streaming API server and stays to e2e.
func TestInPod_RequestShape(t *testing.T) {
	srv := newAPIServer(t)
	e := newExec(t, srv.URL)

	_, err := e.InPod(context.Background(), "ns", "worker-0",
		[]string{runnerContainer, "list", "progress", "--json"})
	if err == nil {
		t.Fatal("want stream error from a non-SPDY server")
	}
	if !strings.Contains(err.Error(), "exec [pgcopydb list progress --json]") {
		t.Fatalf("error must name the argv, got: %v", err)
	}
	req := srv.lastRequest(t)
	if !strings.HasSuffix(req.Path, "/pods/worker-0/exec") {
		t.Fatalf("exec posted to %q", req.Path)
	}
	q := req.Query()
	if got := q["command"]; len(got) != 4 || got[0] != runnerContainer || got[3] != "--json" {
		t.Fatalf("command params = %v", got)
	}
	if q.Get("container") != runnerContainer || q.Get("stdout") != paramTrue || q.Get("stderr") != paramTrue {
		t.Fatalf("exec query = %v", q)
	}
}

// TestInPod_Success drives the whole websocket exec path: the fake API
// upgrades the connection, streams stdout, and closes cleanly, so the bytes
// the "pod" wrote come back with a nil error.
func TestInPod_Success(t *testing.T) {
	srv := newAPIServer(t)
	srv.execOut = []byte("0/5000000\n")
	e := newExec(t, srv.URL)

	out, err := e.InPod(context.Background(), "ns", "worker-0", []string{"pgcopydb", "stream", "sentinel", "get"})
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "0/5000000\n" {
		t.Fatalf("stdout = %q", out)
	}
}

// TestInPod_BrokenTransportConfig hits the executor-construction error leg: a
// rest config whose CA bytes do not parse fails before any request is sent.
// The Exec is assembled by hand because New rejects such a config up front.
func TestInPod_BrokenTransportConfig(t *testing.T) {
	srv := newAPIServer(t)
	e := newExec(t, srv.URL)
	e.config = &rest.Config{
		Host:            srv.URL,
		TLSClientConfig: rest.TLSClientConfig{CAData: []byte("not a certificate")},
	}
	if _, err := e.InPod(context.Background(), "ns", "worker-0", []string{"id"}); err == nil {
		t.Fatal("want executor construction error for unusable transport config")
	}
	if len(srv.requests) != 0 {
		t.Fatalf("no request must leave the client, saw %v", srv.requests)
	}
}

// A wedged API-server connection must not hang a reconcile: every call gets
// a deadline (the live failure mode was Migrations frozen mid-phase behind a
// stuck exec stream).
func TestCalls_BoundedByTimeout(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked // hang until the test ends
	}))
	defer func() { close(blocked); srv.Close() }()

	e, err := New(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	e.timeout = 200 * time.Millisecond

	start := time.Now()
	if _, err := e.RunningPod(context.Background(), "ns", "job"); err == nil {
		t.Fatal("RunningPod: expected a deadline error, got nil")
	}
	if _, err := e.InPod(context.Background(), "ns", "pod", []string{"echo"}); err == nil {
		t.Fatal("InPod: expected a deadline error, got nil")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("calls did not respect the timeout: took %s", elapsed)
	}
}
