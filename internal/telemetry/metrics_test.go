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

package telemetry

import (
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	collectormetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

const (
	testPod       = "op-0"
	resourceAttrs = "OTEL_RESOURCE_ATTRIBUTES"
)

// fakeCollector records the metric names and resource attributes it receives.
type fakeCollector struct {
	mu       sync.Mutex
	names    map[string]bool
	resource map[string]string
}

func (f *fakeCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	req := &collectormetricpb.ExportMetricsServiceRequest{}
	if err := proto.Unmarshal(body, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rm := range req.GetResourceMetrics() {
		for _, kv := range rm.GetResource().GetAttributes() {
			f.resource[kv.GetKey()] = kv.GetValue().GetStringValue()
		}
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				f.names[m.GetName()] = true
			}
		}
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(nil)
}

func (f *fakeCollector) has(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.names[name]
}

func httpExportEnv(t *testing.T, endpoint string) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "100")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")
	t.Setenv("POD_NAME", testPod)
	t.Setenv("POD_NAMESPACE", "op-ns")
}

func testRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "pgcopydb_test_phase", Help: "test"}, []string{"name"})
	g.WithLabelValues("m1").Set(1)
	reg.MustRegister(g)
	return reg
}

// run starts r and returns a stop func that cancels it and reports how long Start took to return.
func run(t *testing.T, r interface{ Start(context.Context) error }) func() time.Duration {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Start(ctx); close(done) }()
	return func() time.Duration {
		begin := time.Now()
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Start did not return after cancel")
		}
		return time.Since(begin)
	}
}

func TestNewMetricsOffReturnsNil(t *testing.T) {
	r, err := NewMetrics(context.Background(), Config{}, "v1", testRegistry())
	if err != nil || r != nil {
		t.Fatalf("NewMetrics(off) = %v, %v; want nil, nil", r, err)
	}
}

func TestMetricsExporterSendsRegistry(t *testing.T) {
	fc := &fakeCollector{names: map[string]bool{}, resource: map[string]string{}}
	srv := httptest.NewServer(fc)
	defer srv.Close()
	httpExportEnv(t, srv.URL)
	r, err := NewMetrics(context.Background(), Config{Metrics: true, Protocol: httpProtobuf}, "v1.2.3", testRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if nle, ok := r.(interface{ NeedLeaderElection() bool }); !ok || nle.NeedLeaderElection() {
		t.Fatal("the exporter must run on every replica, not only the leader")
	}
	stop := run(t, r)
	deadline := time.Now().Add(5 * time.Second)
	for !fc.has("pgcopydb_test_phase") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	if !fc.has("pgcopydb_test_phase") {
		t.Fatal("the registry's gauge never reached the collector")
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for k, want := range map[string]string{"service.name": "pgcopydb-operator", "service.version": "v1.2.3",
		"k8s.pod.name": testPod, "k8s.namespace.name": "op-ns", "service.instance.id": testPod} {
		if got := fc.resource[k]; got != want {
			t.Errorf("resource %s = %q, want %q", k, got, want)
		}
	}
}

func TestMetricsExporterCollectorDown(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close() // nothing listens there now
	httpExportEnv(t, "http://"+addr)
	r, err := NewMetrics(context.Background(), Config{Metrics: true, Protocol: httpProtobuf}, "v1", testRegistry())
	if err != nil {
		t.Fatal(err)
	}
	stop := run(t, r)
	time.Sleep(300 * time.Millisecond) // a few failed exports
	if d := stop(); d > 6*time.Second {
		t.Fatalf("shutdown took %v with the collector down", d)
	}
}

func TestMetricsExporterShutdownBounded(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	defer func() { close(block); srv.Close() }()
	httpExportEnv(t, srv.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "30000") // the bound must come from shutdown, not the exporter timeout
	r, err := NewMetrics(context.Background(), Config{Metrics: true, Protocol: httpProtobuf}, "v1", testRegistry())
	if err != nil {
		t.Fatal(err)
	}
	stop := run(t, r)
	time.Sleep(300 * time.Millisecond)
	if d := stop(); d > 7*time.Second {
		t.Fatalf("shutdown took %v against a hanging collector, want about 5s", d)
	}
}

func TestNewMetricsGRPCConstructsWithoutDialing(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	r, err := NewMetrics(context.Background(), Config{Metrics: true, Protocol: grpcProto}, "v1", testRegistry())
	if err != nil || r == nil {
		t.Fatalf("NewMetrics(grpc) = %v, %v", r, err)
	}
	run(t, r)()
}

func TestNewResource(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, podName, attrs, wantInstance, wantErr string
	}{
		{name: "pod name is the instance", podName: testPod, wantInstance: testPod},
		{name: "hostname without a pod name", wantInstance: host},
		{name: "env overrides the instance", podName: testPod, attrs: "service.instance.id=custom",
			wantInstance: "custom"},
		{name: "malformed attributes", attrs: "a=b,c", wantErr: resourceAttrs},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("POD_NAME", tc.podName)
			t.Setenv(resourceAttrs, tc.attrs)
			res, err := newResource(context.Background(), "v1")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, _ := res.Set().Value(attribute.Key("service.instance.id")); got.AsString() != tc.wantInstance {
				t.Fatalf("service.instance.id = %q, want %q", got.AsString(), tc.wantInstance)
			}
		})
	}
}

func TestNewMetricsErrors(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	srv.Close()
	ca := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"malformed resource attributes", map[string]string{resourceAttrs: "a=b,c"}, resourceAttrs},
		{"plain http endpoint with a CA", map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT":    "http://127.0.0.1:1",
			"OTEL_EXPORTER_OTLP_CERTIFICATE": ca,
		}, "create the OTLP metrics exporter"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			r, err := NewMetrics(context.Background(), Config{Metrics: true, Protocol: protocolHTTP}, "v1", testRegistry())
			if err == nil || r != nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewMetrics = %v, %v; want an error containing %q", r, err, tc.want)
			}
		})
	}
}
