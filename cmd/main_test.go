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

package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	defaultPollVersions = "0.18.2.gea87951"
	testCertDir         = "/certs"
	testCertName        = "m.crt"
	testCertKey         = "m.key"
	// probeDisabled keeps NewManager from binding the probe port at
	// construction; the listener would leak and collide across tests.
	probeDisabled = "--health-probe-bind-address=0"
)

// withoutZap zeroes the zap options, whose function fields never compare as
// deeply equal, so the rest of the struct can.
func withoutZap(f flags) flags {
	f.zapOpts = zap.Options{}
	return f
}

func TestSplitList(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{" , ,", nil},
		{defaultPollVersions, []string{defaultPollVersions}},
		{"a, b ,c", []string{"a", "b", "c"}},
	} {
		if got := splitList(tc.in); !slices.Equal(got, tc.want) {
			t.Errorf("splitList(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestVersionDefault(t *testing.T) {
	// The Makefile and Dockerfile stamp version via -ldflags; unstamped
	// builds must report "dev", never an empty string.
	if version != "dev" {
		t.Errorf("version = %q, want %q", version, "dev")
	}
}

// parseFlags registers the flag surface on a throwaway FlagSet and parses args.
func parseFlags(t *testing.T, args ...string) *flags {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := registerFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	return f
}

func TestRegisterFlagsDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := registerFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse: %v", err)
	}
	// The chart's deployment template and the e2e suite rely on these
	// names and defaults; a mismatch here is a breaking change.
	want := flags{
		metricsAddr:          "0",
		probeAddr:            ":8081",
		runnerImage:          "ghcr.io/ydixken/pgcopydb-operator/runner:latest",
		watchNamespaces:      "",
		progressPollVersions: defaultPollVersions,
		enableLeaderElection: false,
		secureMetrics:        true,
		webhookCertName:      "tls.crt",
		webhookCertKey:       "tls.key",
		metricsCertName:      "tls.crt",
		metricsCertKey:       "tls.key",
		enableHTTP2:          false,
	}
	if got := withoutZap(*f); !reflect.DeepEqual(got, want) {
		t.Errorf("defaults = %+v, want %+v", got, want)
	}
	if fs.Lookup("zap-log-level") == nil {
		t.Error("zap flags are not bound")
	}
}

func TestRegisterFlagsParsesValues(t *testing.T) {
	f := parseFlags(t,
		"--metrics-bind-address=:8443",
		"--health-probe-bind-address=:9091",
		"--runner-image=example.test/runner:v1",
		"--watch-namespaces=default,team-a",
		"--progress-poll-versions=1.2.3,4.5.6",
		"--leader-elect",
		"--metrics-secure=false",
		"--webhook-cert-path=/tmp/wh",
		"--webhook-cert-name=wh.crt",
		"--webhook-cert-key=wh.key",
		"--metrics-cert-path=/tmp/m",
		"--metrics-cert-name="+testCertName,
		"--metrics-cert-key="+testCertKey,
		"--enable-http2",
	)
	want := flags{
		metricsAddr:          ":8443",
		probeAddr:            ":9091",
		runnerImage:          "example.test/runner:v1",
		watchNamespaces:      "default,team-a",
		progressPollVersions: "1.2.3,4.5.6",
		enableLeaderElection: true,
		secureMetrics:        false,
		webhookCertPath:      "/tmp/wh",
		webhookCertName:      "wh.crt",
		webhookCertKey:       "wh.key",
		metricsCertPath:      "/tmp/m",
		metricsCertName:      testCertName,
		metricsCertKey:       testCertKey,
		enableHTTP2:          true,
	}
	if got := withoutZap(*f); !reflect.DeepEqual(got, want) {
		t.Errorf("parsed = %+v, want %+v", got, want)
	}
}

func TestTLSOptions(t *testing.T) {
	if got := tlsOptions(true); got != nil {
		t.Errorf("tlsOptions(true) = %v, want nil", got)
	}
	opts := tlsOptions(false)
	if len(opts) != 1 {
		t.Fatalf("tlsOptions(false) returned %d options, want 1", len(opts))
	}
	var cfg tls.Config
	opts[0](&cfg)
	if !slices.Equal(cfg.NextProtos, []string{"http/1.1"}) {
		t.Errorf("NextProtos = %v, want [http/1.1]", cfg.NextProtos)
	}
}

func TestWebhookOptions(t *testing.T) {
	tlsOpts := tlsOptions(false)

	got := webhookOptions(&flags{}, tlsOpts)
	if got.CertDir != "" || got.CertName != "" || got.KeyName != "" {
		t.Errorf("no cert path: got cert config %+v, want none", got)
	}
	if len(got.TLSOpts) != 1 {
		t.Errorf("TLSOpts not propagated: %d", len(got.TLSOpts))
	}

	got = webhookOptions(&flags{
		webhookCertPath: testCertDir, webhookCertName: "a.crt", webhookCertKey: "a.key",
	}, nil)
	if got.CertDir != testCertDir || got.CertName != "a.crt" || got.KeyName != "a.key" {
		t.Errorf("cert path set: got %+v", got)
	}
}

func TestMetricsOptions(t *testing.T) {
	insecure := metricsOptions(&flags{metricsAddr: ":8080", secureMetrics: false}, nil)
	if insecure.SecureServing || insecure.FilterProvider != nil {
		t.Errorf("insecure: got SecureServing=%v FilterProvider set=%v",
			insecure.SecureServing, insecure.FilterProvider != nil)
	}
	if insecure.BindAddress != ":8080" {
		t.Errorf("BindAddress = %q, want :8080", insecure.BindAddress)
	}

	secure := metricsOptions(&flags{metricsAddr: ":8443", secureMetrics: true}, tlsOptions(false))
	if !secure.SecureServing || secure.FilterProvider == nil {
		t.Errorf("secure: got SecureServing=%v FilterProvider set=%v",
			secure.SecureServing, secure.FilterProvider != nil)
	}
	if secure.CertDir != "" {
		t.Errorf("self-signed: CertDir = %q, want empty", secure.CertDir)
	}
	if len(secure.TLSOpts) != 1 {
		t.Errorf("TLSOpts not propagated: %d", len(secure.TLSOpts))
	}

	withCerts := metricsOptions(&flags{
		secureMetrics: true, metricsCertPath: testCertDir, metricsCertName: testCertName, metricsCertKey: testCertKey,
	}, nil)
	if withCerts.CertDir != testCertDir || withCerts.CertName != testCertName || withCerts.KeyName != testCertKey {
		t.Errorf("cert path set: got %+v", withCerts)
	}
}

func TestManagerOptions(t *testing.T) {
	f := parseFlags(t, "--health-probe-bind-address=:9091", "--leader-elect")
	opts := managerOptions(f)
	if opts.Scheme != scheme {
		t.Error("scheme not set")
	}
	if opts.WebhookServer == nil {
		t.Error("webhook server not set")
	}
	if opts.HealthProbeBindAddress != ":9091" {
		t.Errorf("HealthProbeBindAddress = %q, want :9091", opts.HealthProbeBindAddress)
	}
	if !opts.LeaderElection || opts.LeaderElectionID != "a9039782.pgcopydb-operator.io" {
		t.Errorf("leader election = %v id = %q", opts.LeaderElection, opts.LeaderElectionID)
	}
	if opts.Cache.DefaultNamespaces != nil {
		t.Errorf("empty watch list: DefaultNamespaces = %v, want nil", opts.Cache.DefaultNamespaces)
	}

	scoped := managerOptions(parseFlags(t, "--watch-namespaces=default, team-a"))
	got := scoped.Cache.DefaultNamespaces
	if len(got) != 2 {
		t.Fatalf("DefaultNamespaces = %v, want 2 entries", got)
	}
	for _, ns := range []string{"default", "team-a"} {
		if _, ok := got[ns]; !ok {
			t.Errorf("namespace %q missing from cache config %v", ns, got)
		}
	}
}

// testManager builds an unstarted manager against an unreachable host;
// construction never contacts the API server. The probe listener is
// disabled, NewManager would otherwise bind it and leak the socket.
func testManager(t *testing.T, cfg *rest.Config) ctrl.Manager {
	t.Helper()
	mgr, err := ctrl.NewManager(cfg, managerOptions(parseFlags(t, probeDisabled)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

// checkFailer fails health check registration on demand; a duplicate check
// name only overwrites, so the real manager cannot produce these errors
// before it starts.
type checkFailer struct {
	ctrl.Manager
	healthzErr, readyzErr error
}

func (m checkFailer) AddHealthzCheck(name string, check healthz.Checker) error {
	if m.healthzErr != nil {
		return m.healthzErr
	}
	return m.Manager.AddHealthzCheck(name, check)
}

func (m checkFailer) AddReadyzCheck(name string, check healthz.Checker) error {
	if m.readyzErr != nil {
		return m.readyzErr
	}
	return m.Manager.AddReadyzCheck(name, check)
}

func TestSetupRunnablesErrors(t *testing.T) {
	cfg := &rest.Config{Host: "http://127.0.0.1:1"}
	f := parseFlags(t)
	boom := errors.New("boom")

	t.Run("healthz registration fails", func(t *testing.T) {
		err := setupRunnables(checkFailer{Manager: testManager(t, cfg), healthzErr: boom}, f)
		if err == nil || !strings.Contains(err.Error(), "health check") {
			t.Errorf("err = %v, want health check error", err)
		}
	})

	t.Run("readyz registration fails", func(t *testing.T) {
		err := setupRunnables(checkFailer{Manager: testManager(t, cfg), readyzErr: boom}, f)
		if err == nil || !strings.Contains(err.Error(), "ready check") {
			t.Errorf("err = %v, want ready check error", err)
		}
	})

	t.Run("pod exec transport rejects the config", func(t *testing.T) {
		// QPS without Burst passes manager construction but fails the
		// clientset that podexec builds from the same config.
		mgr := testManager(t, &rest.Config{Host: "http://127.0.0.1:1", QPS: 1, Burst: 0})
		err := setupRunnables(mgr, f)
		if err == nil || !strings.Contains(err.Error(), "pod exec transport") {
			t.Errorf("err = %v, want pod exec transport error", err)
		}
	})
}

// writeKubeconfig points KUBECONFIG at a file for this test, so buildManager
// never sees the developer's real one.
func writeKubeconfig(t *testing.T, cluster string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	content := `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
` + cluster + `
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user: {}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing kubeconfig: %v", err)
	}
	t.Setenv("KUBECONFIG", path)
}

// TestBuildManager runs its cases in order: the controller name registry is
// process-global, so the wiring can succeed exactly once per test binary and
// the duplicate-name case must follow the successful one.
func TestBuildManager(t *testing.T) {
	newFlagSet := func() *flag.FlagSet {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		return fs
	}

	t.Run("flag parse error", func(t *testing.T) {
		if _, err := buildManager(newFlagSet(), []string{"--no-such-flag"}); err == nil {
			t.Error("want parse error, got nil")
		}
	})

	t.Run("manager creation fails on a bad CA", func(t *testing.T) {
		// "bm90IGEgY2VydA==" is base64 for "not a cert": the kubeconfig
		// loads, the TLS transport does not.
		writeKubeconfig(t, `    server: https://127.0.0.1:1
    certificate-authority-data: bm90IGEgY2VydA==`)
		_, err := buildManager(newFlagSet(), []string{probeDisabled})
		if err == nil || !strings.Contains(err.Error(), "create manager") {
			t.Errorf("err = %v, want create manager error", err)
		}
	})

	t.Run("wires the manager", func(t *testing.T) {
		writeKubeconfig(t, `    server: http://127.0.0.1:1`)
		mgr, err := buildManager(newFlagSet(), []string{
			"--metrics-bind-address=0",
			"--metrics-secure=false",
			"--runner-image=example.test/runner:v1",
			"--watch-namespaces=default",
			"--progress-poll-versions=1.2.3",
			probeDisabled,
		})
		if err != nil {
			t.Fatalf("buildManager: %v", err)
		}
		if mgr == nil {
			t.Fatal("manager is nil")
		}
	})

	t.Run("duplicate controller name", func(t *testing.T) {
		writeKubeconfig(t, `    server: http://127.0.0.1:1`)
		_, err := buildManager(newFlagSet(), []string{probeDisabled})
		if err == nil || !strings.Contains(err.Error(), "migration controller") {
			t.Errorf("err = %v, want migration controller error", err)
		}
	})
}
