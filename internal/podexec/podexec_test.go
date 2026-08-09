/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
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
)

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
}

// runnerContainer pins the container name requests must target; deliberately
// a test-local copy so a rename in the source fails here.
const runnerContainer = "pgcopydb"

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
	if q.Get("container") != runnerContainer || q.Get("stdout") != "true" || q.Get("stderr") != "true" {
		t.Fatalf("exec query = %v", q)
	}
}
