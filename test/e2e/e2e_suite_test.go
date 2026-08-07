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

// Package e2e exercises the operator on the cluster behind the current
// kubectl context. The suite brings its own operator: helm installs a
// throwaway instance into pgcopydb-e2e-system that watches only the fixture
// namespaces, and AfterSuite always removes it again. The production
// installation in pgcopydb-system is never touched, checked, or relied on.
// Fixtures stay inside pgcopydb-e2e and pgcopydb-e2e-x, the sanctioned e2e
// area on the shared dev cluster.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

const (
	// nsE2E holds the CNPG fixtures and most Migrations.
	nsE2E = "pgcopydb-e2e"
	// nsX hosts the cross-namespace scenario (secrets local, hosts remote).
	nsX = "pgcopydb-e2e-x"
	// nsOperator hosts the suite's throwaway operator install.
	nsOperator = "pgcopydb-e2e-system"
	// helmRelease prefixes every rendered resource name, keeping the install
	// distinct from the production one.
	helmRelease = "pgcopydb-e2e"
	// operatorTag pins the manager and runner images for the throwaway install.
	operatorTag = "v0.1.0-alpha.5"
	// chartPath is relative to this package: go test runs each test binary
	// with the package directory as working directory.
	chartPath = "../../charts/pgcopydb-operator"
	// fieldOwner identifies this suite's server-side applies.
	fieldOwner = "pgcopydb-e2e-suite"

	sourceCluster = "e2e-source"
	targetCluster = "e2e-target"
	// CNPG names the single instance <cluster>-1 and the app secret <cluster>-app.
	srcPod    = sourceCluster + "-1"
	tgtPod    = targetCluster + "-1"
	srcSecret = sourceCluster + "-app"
	tgtSecret = targetCluster + "-app"

	// appDB is the CNPG-bootstrapped database and its owning role.
	appDB = "app"

	// clusterReadyTimeout covers CNPG bootstrap incl. initdb data generation.
	clusterReadyTimeout = 5 * time.Minute
	// migrationTimeout covers a clone (about 1 min) with slack for image pulls
	// and PVC provisioning; resume runs two attempts inside the same budget.
	migrationTimeout = 5 * time.Minute
)

var cnpgGVK = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}

// fixtureSQL seeds the source database at initdb time. Every object is owned
// by app: superuser-owned objects break the clone with permission errors when
// restoring as the app user, and ALTER DEFAULT PRIVILEGES is off limits
// because pg_dump would capture postgres-role ACLs the app user cannot restore.
var fixtureSQL = []string{
	"CREATE TABLE customers (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY," +
		" name text NOT NULL, created_at timestamptz DEFAULT now())",
	"CREATE TABLE orders (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY," +
		" customer_id bigint REFERENCES customers(id), amount numeric(12,2) NOT NULL, note text)",
	"CREATE INDEX orders_customer_idx ON orders (customer_id)",
	"ALTER TABLE customers OWNER TO app",
	"ALTER TABLE orders OWNER TO app",
	"CREATE SCHEMA audit AUTHORIZATION app",
	"CREATE TABLE audit.events (id bigserial PRIMARY KEY, payload jsonb)",
	"ALTER TABLE audit.events OWNER TO app",
	"INSERT INTO customers (name) SELECT 'customer-' || g FROM generate_series(1, 50000) g",
	"INSERT INTO orders (customer_id, amount, note) SELECT (g % 50000) + 1," +
		" (g % 900)::numeric / 7, 'order ' || g FROM generate_series(1, 200000) g",
	"INSERT INTO audit.events (payload) SELECT jsonb_build_object('n', g) FROM generate_series(1, 1000) g",
}

var (
	ctx       context.Context
	k8sClient client.Client
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgcopydb-operator e2e suite")
}

var _ = BeforeSuite(func() {
	ctx = context.Background()

	// The suite targets whatever the current kubectl context points at, per
	// the task e2e contract. Say so loudly before touching anything.
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if raw, err := rules.Load(); err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "e2e running against kubectl context %q\n", raw.CurrentContext)
	}

	cfg, err := config.GetConfig()
	Expect(err).NotTo(HaveOccurred(), "no usable kubeconfig; aborting")
	// A bounded per-request timeout so an unreachable API server aborts the
	// suite instead of hanging.
	cfg.Timeout = time.Minute

	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(v1alpha1.AddToScheme(scheme)).To(Succeed())
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())

	By("checking the Migration CRD exists (the suite does not manage it)")
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "migrations.pgcopydb-operator.io"}, crd)).
		To(Succeed(), "CRD migrations.pgcopydb-operator.io not found or cluster unreachable;"+
			" install the CRD first (chart with crds.install=true, or config/crd), the suite will not create it")

	By("installing the suite's own operator into " + nsOperator)
	// Accepted caveat: the production operator watches cluster-wide and may
	// co-reconcile the suite's Migrations alongside this install. The
	// controller converges either way (attempts derive from persisted status,
	// Job creation tolerates AlreadyExists), so the specs assert terminal
	// phase, attempts, Jobs, and data, never event counts or timing.
	// A crashed run may have left the release behind; clear it first.
	helmRun("uninstall", helmRelease, "-n", nsOperator, "--ignore-not-found")
	helmRun("install", helmRelease, chartPath,
		"-n", nsOperator, "--create-namespace",
		"--set", "crds.install=false",
		"--set", "image.tag="+operatorTag,
		"--set", "runner.image.tag="+operatorTag,
		"--set", "watchNamespaces={"+nsE2E+","+nsX+"}",
		"--set", "leaderElection.enabled=false",
		"--wait")

	By("creating or adopting the fixture namespaces")
	ensureNamespace(nsE2E)
	ensureNamespace(nsX)

	By("deleting leftover Migrations from previous runs")
	for _, ns := range []string{nsE2E, nsX} {
		Expect(k8sClient.DeleteAllOf(ctx, &v1alpha1.Migration{}, client.InNamespace(ns))).To(Succeed())
	}
	Eventually(func(g Gomega) {
		for _, ns := range []string{nsE2E, nsX} {
			list := &v1alpha1.MigrationList{}
			g.Expect(k8sClient.List(ctx, list, client.InNamespace(ns))).To(Succeed())
			g.Expect(list.Items).To(BeEmpty(), "Migrations still terminating in %s", ns)
		}
	}, 2*time.Minute, 2*time.Second).Should(Succeed())

	By("creating or adopting the CNPG source and target clusters")
	applyCluster(cnpgCluster(sourceCluster, fixtureSQL))
	applyCluster(cnpgCluster(targetCluster, nil))
	waitClusterReady(sourceCluster)
	waitClusterReady(targetCluster)

	By("resetting the target database so the fresh-clone scenario starts empty")
	resetTargetObjects()
})

var _ = AfterSuite(func() {
	// The throwaway operator always goes away, keep-fixtures or not: every
	// run installs a fresh one.
	By("uninstalling the suite's operator and deleting " + nsOperator)
	helmRun("uninstall", helmRelease, "-n", nsOperator, "--ignore-not-found")
	deleteNamespaces(2*time.Minute, nsOperator)

	if os.Getenv("E2E_KEEP_FIXTURES") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter,
			"E2E_KEEP_FIXTURES=true: keeping namespaces %s and %s for iteration\n", nsE2E, nsX)
		return
	}
	By("deleting the fixture namespaces")
	// CNPG teardown plus volume deletion takes a while on the shared cluster.
	deleteNamespaces(10*time.Minute, nsX, nsE2E)
})

// helmRun executes helm against the current kubectl context, echoes its
// output, and fails the suite on error.
func helmRun(args ...string) {
	GinkgoHelper()
	out, err := exec.Command("helm", args...).CombinedOutput()
	_, _ = GinkgoWriter.Write(out)
	Expect(err).NotTo(HaveOccurred(), "helm %s failed", strings.Join(args, " "))
}

// deleteNamespaces deletes the given namespaces and waits until they are
// gone, so the next run's --create-namespace and fixture creation start
// clean. Tolerates namespaces that never came up (aborted BeforeSuite).
func deleteNamespaces(timeout time.Duration, names ...string) {
	GinkgoHelper()
	if k8sClient == nil {
		return
	}
	for _, name := range names {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := k8sClient.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred(), "failed to delete namespace %s", name)
		}
	}
	Eventually(func(g Gomega) {
		for _, name := range names {
			err := k8sClient.Get(ctx, client.ObjectKey{Name: name}, &corev1.Namespace{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "namespace %s still terminating", name)
		}
	}, timeout, 5*time.Second).Should(Succeed())
}

// ensureNamespace creates the namespace or adopts it when a previous run (or
// a manual session) left it behind.
func ensureNamespace(name string) {
	GinkgoHelper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := k8sClient.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred(), "failed to ensure namespace %s", name)
	}
}

// cnpgCluster builds a minimal CNPG Cluster: one instance, 3Gi storage, app
// database owned by app. initSQL seeds data at initdb time (source only).
// Built as unstructured on purpose: importing the CNPG API just for two
// fixtures is not worth a dependency.
func cnpgCluster(name string, initSQL []string) *unstructured.Unstructured {
	initdb := map[string]any{
		"database": appDB,
		"owner":    appDB,
	}
	if len(initSQL) > 0 {
		stmts := make([]any, len(initSQL))
		for i, s := range initSQL {
			stmts[i] = s
		}
		initdb["postInitApplicationSQL"] = stmts
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": cnpgGVK.Group + "/" + cnpgGVK.Version,
		"kind":       cnpgGVK.Kind,
		"metadata":   map[string]any{"name": name, "namespace": nsE2E},
		"spec": map[string]any{
			"instances": int64(1),
			"storage":   map[string]any{"size": "3Gi"},
			"bootstrap": map[string]any{"initdb": initdb},
		},
	}}
}

// applyCluster server-side applies the fixture, which both creates it fresh
// and adopts an existing one left over from manual runs.
func applyCluster(obj *unstructured.Unstructured) {
	GinkgoHelper()
	Expect(k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(obj),
		client.FieldOwner(fieldOwner), client.ForceOwnership)).
		To(Succeed(), "failed to apply CNPG cluster %s", obj.GetName())
}

func waitClusterReady(name string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		c := &unstructured.Unstructured{}
		c.SetGroupVersionKind(cnpgGVK)
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, c)).To(Succeed())
		ready, _, _ := unstructured.NestedInt64(c.Object, "status", "readyInstances")
		g.Expect(ready).To(BeNumerically(">=", 1), "CNPG cluster %s has no ready instance", name)
	}, clusterReadyTimeout, 5*time.Second).Should(Succeed())
}

// resetTargetObjects drops the fixture objects from the target. Needed because
// pg_restore --clean only drops objects present in the incoming dump, so a
// populated target from an earlier run would break the no-dropIfExists clone.
func resetTargetObjects() {
	GinkgoHelper()
	psql(tgtPod, "DROP SCHEMA IF EXISTS audit CASCADE")
	psql(tgtPod, "DROP TABLE IF EXISTS public.orders CASCADE")
	psql(tgtPod, "DROP TABLE IF EXISTS public.customers CASCADE")
}

// psql runs one statement inside a CNPG instance pod as the in-pod postgres
// superuser (peer auth on the unix socket, no password involved) and returns
// trimmed stdout. kubectl exec is deliberate: it reuses the same current
// context as the client and saves a hand-rolled SPDY executor.
func psql(pod, sql string) string {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "exec", "-n", nsE2E, pod, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-tAc", sql)
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			Expect(err).NotTo(HaveOccurred(), "psql %q on %s failed: %s", sql, pod, string(ee.Stderr))
		}
		Expect(err).NotTo(HaveOccurred(), "psql %q on %s failed", sql, pod)
	}
	return strings.TrimSpace(string(out))
}
