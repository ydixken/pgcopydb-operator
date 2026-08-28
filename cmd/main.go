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
	"flag"
	"fmt"
	"os"
	"strings"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	pgcopydboperatoriov1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	pgcopydboperatoriov1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/controller"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

// version is stamped by -ldflags "-X main.version=..." (Makefile, Dockerfile);
// "dev" covers go run and unstamped builds.
var version = "dev"

// splitList turns a comma-separated flag value into its non-empty entries.
func splitList(s string) []string {
	var out []string
	for v := range strings.SplitSeq(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	// Both API versions stay served; v1beta1 is the storage version and the
	// one the controller watches, v1alpha1 remains readable for old clients.
	utilruntime.Must(pgcopydboperatoriov1alpha1.AddToScheme(scheme))
	utilruntime.Must(pgcopydboperatoriov1beta1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// flags carries the parsed command line. The chart's deployment template
// renders these exact flag names; the surface is frozen.
type flags struct {
	metricsAddr          string
	probeAddr            string
	runnerImage          string
	watchNamespaces      string
	progressPollVersions string
	enableLeaderElection bool
	secureMetrics        bool
	webhookCertPath      string
	webhookCertName      string
	webhookCertKey       string
	metricsCertPath      string
	metricsCertName      string
	metricsCertKey       string
	enableHTTP2          bool
	zapOpts              zap.Options
}

func registerFlags(fs *flag.FlagSet) *flags {
	f := &flags{zapOpts: zap.Options{Development: true}}
	fs.StringVar(&f.metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	fs.StringVar(&f.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	fs.StringVar(&f.runnerImage, "runner-image", "ghcr.io/ydixken/pgcopydb-operator/runner:latest",
		"Default image for migration worker Jobs; spec.runner.image overrides per Migration.")
	fs.StringVar(&f.watchNamespaces, "watch-namespaces", "",
		"Comma-separated namespaces to watch; empty watches the whole cluster.")
	fs.StringVar(&f.progressPollVersions, "progress-poll-versions", "0.18.2.gea87951",
		"Comma-separated exact pgcopydb versions allowed to run the in-pod progress poll; "+
			"empty disables the poll (database sizes are sampled regardless).")
	fs.BoolVar(&f.enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	fs.BoolVar(&f.secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	fs.StringVar(&f.webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	fs.StringVar(&f.webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	fs.StringVar(&f.webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	fs.StringVar(&f.metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	fs.StringVar(&f.metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	fs.StringVar(&f.metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	fs.BoolVar(&f.enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	f.zapOpts.BindFlags(fs)
	return f
}

// tlsOptions keeps HTTP/2 off unless explicitly enabled: it protects against
// the HTTP/2 Stream Cancellation and Rapid Reset CVEs
// (GHSA-qppj-fm5r-hxr3, GHSA-4374-p667-p6c8).
func tlsOptions(enableHTTP2 bool) []func(*tls.Config) {
	if enableHTTP2 {
		return nil
	}
	return []func(*tls.Config){func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}}
}

func webhookOptions(f *flags, tlsOpts []func(*tls.Config)) webhook.Options {
	opts := webhook.Options{TLSOpts: tlsOpts}
	if len(f.webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", f.webhookCertPath,
			"webhook-cert-name", f.webhookCertName, "webhook-cert-key", f.webhookCertKey)
		opts.CertDir = f.webhookCertPath
		opts.CertName = f.webhookCertName
		opts.KeyName = f.webhookCertKey
	}
	return opts
}

// metricsOptions configures the metrics server; with no cert path,
// controller-runtime falls back to a self-signed certificate.
func metricsOptions(f *flags, tlsOpts []func(*tls.Config)) metricsserver.Options {
	opts := metricsserver.Options{
		BindAddress:   f.metricsAddr,
		SecureServing: f.secureMetrics,
		TLSOpts:       tlsOpts,
	}
	if f.secureMetrics {
		// Only authenticated and authorized clients reach the endpoint;
		// the RBAC lives in 'config/rbac/kustomization.yaml'.
		opts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	if len(f.metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", f.metricsCertPath,
			"metrics-cert-name", f.metricsCertName, "metrics-cert-key", f.metricsCertKey)
		opts.CertDir = f.metricsCertPath
		opts.CertName = f.metricsCertName
		opts.KeyName = f.metricsCertKey
	}
	return opts
}

func managerOptions(f *flags) ctrl.Options {
	tlsOpts := tlsOptions(f.enableHTTP2)
	opts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsOptions(f, tlsOpts),
		WebhookServer:          webhook.NewServer(webhookOptions(f, tlsOpts)),
		HealthProbeBindAddress: f.probeAddr,
		LeaderElection:         f.enableLeaderElection,
		LeaderElectionID:       "a9039782.pgcopydb-operator.io",
	}
	// Scope the cache to an explicit namespace list when asked; the RBAC in
	// the chart stays cluster-scoped, only the watch narrows. Empty = all.
	if f.watchNamespaces != "" {
		namespaces := map[string]cache.Config{}
		for _, ns := range splitList(f.watchNamespaces) {
			namespaces[ns] = cache.Config{}
		}
		opts.Cache = cache.Options{DefaultNamespaces: namespaces}
	}
	return opts
}

// setupRunnables registers the health checks and the Migration controller on
// a freshly built manager.
func setupRunnables(mgr ctrl.Manager, f *flags) error {
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("set up ready check: %w", err)
	}
	podExec, err := podexec.New(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("create pod exec transport: %w", err)
	}
	if err := (&controller.MigrationReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		Recorder:    mgr.GetEventRecorder("pgcopydb-operator"),
		RunnerImage: f.runnerImage,
		Sentinel:    sentinel.New(podExec),
		Logs:        podExec,
		Progress:    progress.NewFromExec(podExec, splitList(f.progressPollVersions)),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create migration controller: %w", err)
	}
	metrics.RecordBuildInfo(version)
	// +kubebuilder:scaffold:builder
	return nil
}

// buildManager parses args into fs and assembles the fully wired manager.
func buildManager(fs *flag.FlagSet, args []string) (ctrl.Manager, error) {
	f := registerFlags(fs)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&f.zapOpts)))
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), managerOptions(f))
	if err != nil {
		return nil, fmt.Errorf("create manager: %w", err)
	}
	if err := setupRunnables(mgr, f); err != nil {
		return nil, err
	}
	return mgr, nil
}

func main() {
	mgr, err := buildManager(flag.CommandLine, os.Args[1:])
	if err != nil {
		setupLog.Error(err, "Failed to set up manager")
		os.Exit(1)
	}
	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
