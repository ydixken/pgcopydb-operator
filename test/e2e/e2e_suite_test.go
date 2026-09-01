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
	"embed"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
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
	// managerServiceAccount is the ServiceAccount the install binds to when it
	// may not create RBAC itself; whoever owns the namespaces provides it.
	managerServiceAccount = "pgcopydb-e2e-manager"
	// chartPath is relative to this package: go test runs each test binary
	// with the package directory as working directory.
	chartPath = "../../charts/pgcopydb-operator"
	// fieldOwner identifies this suite's server-side applies.
	fieldOwner = "pgcopydb-e2e-suite"

	sourceCluster = "e2e-source"
	targetCluster = "e2e-target"
	// CNPG names the app secret <cluster>-app. Instance pods are <cluster>-N,
	// and which of them is primary moves on failover, so every statement
	// resolves its pod by label through primaryPod instead of by name.
	srcSecret = sourceCluster + "-app"
	tgtSecret = targetCluster + "-app"

	// CNPG's own pod labels: what the suite selects, schedules and kills by.
	labelCNPGCluster  = "cnpg.io/cluster"
	labelCNPGRole     = "cnpg.io/instanceRole"
	labelCNPGPodRole  = "cnpg.io/podRole"
	labelCNPGInstance = "cnpg.io/instanceName"
	rolePrimary       = "primary"

	// Fixture server sizing. Requests only, so nothing is ever throttled, but
	// large enough that a seeding or restoring backend gets a real core and
	// PostgreSQL gets a cache worth having. The caches are derived by hand
	// rather than by ratio: CNPG does not size shared_buffers from the memory
	// request, so a bigger request alone would change nothing.
	// Two, not four. Four was tried and measured: seeding ran at 16.6 MiB/s
	// against 15.9 at two, which is noise. Issue #146 later found why: the
	// seeding backend waits on WAL write and fsync, never on a core. It is
	// not free either. Longhorn's instance manager holds a guaranteed 6 CPUs
	// per node here, so six four-CPU instances leave no room for a worker and
	// the run dies on FailedScheduling instead of running slowly.
	fixtureCPU    = "2"
	fixtureMemory = "4Gi"
	// The seed Job is not a migration worker, so it does not get the
	// operator's runner default; size it like one of the servers it loads.
	workerCPU            = "2"
	workerMemory         = "4Gi"
	fixtureSharedBuffers = "1GB"
	fixtureCacheSize     = "3GB"

	// defaultCNPGInstances is the instance count of both fixture clusters.
	// One: the migration's SQL legs still cross the real network, because the
	// source, the target and the worker are separate pods, and provisioning
	// two volumes instead of six takes minutes off every setup and teardown.
	// What one instance gives up is intra-cluster streaming replication, which
	// no scenario here asserts on.
	//
	// E2E_CNPG_INSTANCES raises it. The chaos scenarios want it: one of them
	// kills a primary and expects the cluster to survive, which is a different
	// test without a replica to fail over to.
	defaultCNPGInstances = 1

	// appDB is the CNPG-bootstrapped database and its owning role.
	appDB = "app"
	// passwordKey is the password entry in the CNPG app secrets and their
	// suite-made copies.
	passwordKey = "password"

	// clusterReadyTimeout covers CNPG bootstrap and volume provisioning for
	// every instance, not just the first. Seeding happens later in its own
	// Job, so a fresh cluster's replicas join an empty database and this is
	// generous. A replica rejoining after a chaos kill copies the seeded data
	// instead, which is what the budget is really sized for.
	clusterReadyTimeout = 10 * time.Minute

	// seedProfile names the fixture generation; bump it when the schema or
	// the seeded shapes change so kept clusters get recreated.
	baseSeedProfile = "v2"
	// seedJobName and seedConfigMap are the seed Job and its mounted SQL.
	seedJobName   = "e2e-seed"
	seedConfigMap = "e2e-fixtures"
	// seedImage only needs a psql client; the CNPG operand image has one and
	// is already pulled on any cluster running CNPG fixtures.
	seedImage = "ghcr.io/cloudnative-pg/postgresql:18"
	// seedLogTail is generous because the log is the seed's profile, not just
	// a failure hint: the DDL alone echoes a line per object before the first
	// table is loaded.
	seedLogTail = 400
	// ephemeralStorageClass is the suite-owned Longhorn class the fixture
	// volumes bind to: one replica, reclaim Delete, so throwaway volumes that
	// CNPG has already replicated do not triple themselves again underneath.
	ephemeralStorageClass = "longhorn-e2e-ephemeral"
)

var cnpgGVK = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}

// fixturesFS carries the fixture SQL and the script that stages it (see
// fixtures/run.sh) into the seed Job via a ConfigMap. The whole directory,
// not a list: a stage added to run.sh but forgotten here would only surface
// as a failed Job on a real cluster.
//
//go:embed fixtures
var fixturesFS embed.FS

// The suite runs in one of two tiers. Default: E2E_SCALE (default 1) sizes the
// fixture data (~12GB at scale 1) and with it the volumes (50/50/12Gi at
// scale 1). Stress (E2E_STRESS=true): scale 10 (~120GB) on fixed 200/150/50Gi
// volumes and much longer budgets. Both tiers put the fixture volumes on the
// suite-owned single-replica StorageClass and both are gated by the Longhorn
// capacity check. E2E_SCALE always wins when set, so the version matrix can
// run small (0.1) against either tier's sizing. Every source and target volume
// is provisioned once per CNPG instance, so the tier sizes multiply by
// cnpgInstances.
var (
	stress = envTrue("E2E_STRESS")
	scale  = 1.0

	// Only the literal "false" leaves the fixture namespaces to their owner
	// (GitOps in CI): the suite then works inside them and creates none.
	manageNamespaces = os.Getenv("E2E_MANAGE_NAMESPACES") != "false"

	// pgSource and pgTarget pick the PostgreSQL major for each fixture
	// cluster's operand image (E2E_PG_SOURCE/E2E_PG_TARGET, default 17).
	// Upgrade direction only, and PG14 only as a source; init enforces both.
	pgSource = 17
	pgTarget = 17

	// Volume sizes are tier-dependent, so init sets them.
	cnpgInstances       = defaultCNPGInstances
	extraTables         int
	extraSizeMB         int
	srcStorageSize      string
	tgtStorageSize      string
	workVolumeSize      string
	fixtureStorageClass string // empty: the cluster default StorageClass

	// migrationTimeout covers one clone of the seeded dataset with slack for
	// image pulls and PVC provisioning; resume runs two attempts inside the
	// same budget.
	migrationTimeout = 30 * time.Minute
	// followTimeout covers an unattended live migration end to end: base
	// copy, catchup, cutover, drain, cleanup, and any compare Jobs.
	followTimeout = 45 * time.Minute
	// seedTimeout bounds the seed Job; seeding is IO-bound on the source
	// volume.
	seedTimeout = 30 * time.Minute
	// lagConvergeTimeout bounds how long a follow migration may take to report
	// a lag at or under its maxCatchupLag once the stream is up. It only has to
	// separate a catch-up from a stall, and those are not close: replaying what
	// a quiet source produced during the base copy takes a pass or two, while a
	// lag reading that cannot converge never moves at all.
	lagConvergeTimeout = 5 * time.Minute
	// primaryTimeout bounds the wait for a cluster to carry exactly one
	// primary. It only has to cover a CNPG promotion, not a bootstrap.
	primaryTimeout = 2 * time.Minute
	// liveWriteInterval caps the average write rate at one per interval.
	// The ticker drops ticks while a persistent pipe write is blocked.
	liveWriteInterval = 200 * time.Millisecond
	// e2eCommandTimeout bounds one subprocess attempt or shutdown wait.
	e2eCommandTimeout = 30 * time.Second
)

// operatorTag pins the manager and runner images for the throwaway install and
// can be overridden with E2E_OPERATOR_TAG. v0.11.3 is the stable baseline.
var operatorTag = "v0.11.3"

// runnerTag is the tag for the worker image. It defaults to the same release
// as the manager and is overridable with E2E_RUNNER_TAG so an unreleased
// runner can be tested against real servers.
var runnerTag = operatorTag

// envTrue reports whether the given switch-style environment variable is
// set to exactly "true".
func envTrue(name string) bool {
	return os.Getenv(name) == "true"
}

func init() {
	// Fixture volumes take the suite-owned single-replica class at every tier.
	// CNPG already keeps cnpgInstances copies of the data, so a replicating
	// default class would copy each of those again and store the fixtures nine
	// times over for nothing a test can observe. ensureFixtureStorage falls
	// back to the cluster default when the cluster has no Longhorn.
	fixtureStorageClass = ephemeralStorageClass
	if stress {
		scale = 10
		srcStorageSize, tgtStorageSize, workVolumeSize = "200Gi", "150Gi", "50Gi"
		migrationTimeout, followTimeout, seedTimeout = 2*time.Hour, 3*time.Hour, 3*time.Hour
		lagConvergeTimeout = 20 * time.Minute
	}
	if v := os.Getenv("E2E_SCALE"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 {
			panic("E2E_SCALE must be a positive number, got " + strconv.Quote(v))
		}
		scale = f
	}
	if !stress {
		// Volumes follow the scale: a 0.1 run seeds ~1.2GB and has no business
		// parking 100Gi of PVCs on the shared cluster. The stress tier keeps
		// the fixed sizes its capacity check was written around. The 50Gi
		// basis is chosen so the round scales land on round volumes.
		srcStorageSize, tgtStorageSize, workVolumeSize = scaledSize(50), scaledSize(50), scaledSize(12)
	}
	// The extra tables land on both volumes, so both grow with them. Doubled,
	// because the same bytes are written twice over a run's life: once by the
	// seed and again by WAL, and a full volume stops the server rather than
	// slowing it.
	if extraSizeMB > 0 {
		extraGi := (extraSizeMB*2 + 1023) / 1024
		srcStorageSize = addGi(srcStorageSize, extraGi)
		tgtStorageSize = addGi(tgtStorageSize, extraGi)
	}
	// E2E_STORAGE_CLASS pins the fixture volumes to one class, for clusters
	// where several are marked default and binding is otherwise a coin flip.
	// Set, it wins over the stress tier's ephemeral class too.
	if v := os.Getenv("E2E_STORAGE_CLASS"); v != "" {
		fixtureStorageClass = v
	}
	// E2E_CNPG_INSTANCES trades the cross-node property for setup speed.
	if v := os.Getenv("E2E_CNPG_INSTANCES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			panic("E2E_CNPG_INSTANCES must be a positive integer, got " + strconv.Quote(v))
		}
		cnpgInstances = n
	}
	// E2E_EXTRA_TABLES and E2E_EXTRA_SIZE_GB add a production-shaped spread of
	// tables on top of the v2 fixture: many tables whose sizes are drawn from
	// a normal distribution and normalised to the requested total. The v2
	// fixture is deliberately lopsided, one table holding most of the bytes,
	// which is the worst case for table-level parallelism and not what a real
	// database looks like. Both must be set for either to take effect.
	if v := os.Getenv("E2E_EXTRA_TABLES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			panic("E2E_EXTRA_TABLES must be a non-negative integer, got " + strconv.Quote(v))
		}
		extraTables = n
	}
	if v := os.Getenv("E2E_EXTRA_SIZE_GB"); v != "" {
		g, err := strconv.ParseFloat(v, 64)
		if err != nil || g < 0 {
			panic("E2E_EXTRA_SIZE_GB must be a non-negative number, got " + strconv.Quote(v))
		}
		extraSizeMB = int(g * 1024)
	}
	if (extraTables > 0) != (extraSizeMB > 0) {
		panic("E2E_EXTRA_TABLES and E2E_EXTRA_SIZE_GB must be set together")
	}
	// E2E_OPERATOR_TAG installs a manager image other than the pinned release,
	// so a branch's controller can be exercised against real servers before it
	// merges. The runner follows unless E2E_RUNNER_TAG below moves it alone.
	if v := os.Getenv("E2E_OPERATOR_TAG"); v != "" {
		operatorTag, runnerTag = v, v
	}
	// E2E_RUNNER_TAG points the worker Jobs at a runner image other than the
	// pinned release, so a change to images/runner can be exercised against
	// real servers before it merges. Without it the suite would install the
	// published runner and report green regardless of what the branch does to
	// that image.
	if v := os.Getenv("E2E_RUNNER_TAG"); v != "" {
		runnerTag = v
	}
	pgSource = pgMajorEnv("E2E_PG_SOURCE")
	pgTarget = pgMajorEnv("E2E_PG_TARGET")
	// The version matrix is upgrade-direction only: pgcopydb needs pg_dump at
	// least at the target's major, and a newer major's dump does not restore
	// into an older server. PG14 is a source only: the follow-mode target
	// contract includes GRANT SET ON PARAMETER session_replication_role,
	// which PostgreSQL grew in 15 (docs/reference/prerequisites.md).
	if pgTarget < pgSource {
		panic(fmt.Sprintf("E2E_PG_TARGET (%d) is older than E2E_PG_SOURCE (%d):"+
			" the version matrix is upgrade-direction only", pgTarget, pgSource))
	}
	if pgTarget < 15 {
		panic("E2E_PG_TARGET must be 15 or newer: the follow-mode target needs" +
			" GRANT SET ON PARAMETER session_replication_role (PG15+); PG14 works as a source only")
	}
}

// pgMajorEnv reads a PostgreSQL major from the environment: a plain major
// between 14 and 18, the range the CNPG operand images and pgcopydb 0.18
// cover here. The default pins 17, what CNPG would pick implicitly today, so
// kept fixtures never change majors just because the CNPG default moved.
func pgMajorEnv(name string) int {
	v := os.Getenv(name)
	if v == "" {
		return 17
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 14 || n > 18 {
		panic(name + " must be a plain PostgreSQL major between 14 and 18, got " + strconv.Quote(v))
	}
	return n
}

// scaled mirrors e2e_scaled() in fixtures/schema.sql: the row count for a
// base count n at the current scale. The two formulas MUST stay in sync or
// the scale-derived assertions drift from what the seed wrote.
func scaled(n int64) int64 {
	v := int64(math.Round(float64(n) * scale))
	if v < 1 {
		return 1
	}
	return v
}

// scaledSize sizes a fixture volume for the current scale, from the size the
// same volume gets at scale 1. It never goes below an eighth of that: WAL,
// indexes and the change spool need headroom that the row counts do not size.
// addGi grows a "<n>Gi" size by whole gibibytes. The sizes here are all
// written that way, so this stays string in, string out rather than dragging
// resource.Quantity through the call sites.
func addGi(size string, gi int) string {
	n, err := strconv.Atoi(strings.TrimSuffix(size, "Gi"))
	if err != nil {
		panic("storage size is not a whole Gi value: " + size)
	}
	return strconv.Itoa(n+gi) + "Gi"
}

func scaledSize(fullScaleGi int) string {
	gi := int(math.Round(float64(fullScaleGi) * scale))
	if floor := (fullScaleGi + 7) / 8; gi < floor {
		gi = floor
	}
	return strconv.Itoa(gi) + "Gi"
}

// seedProfile names the fixture generation the marker records. The extra
// tables are folded in, so asking for a different spread invalidates a kept
// fixture the same way bumping the base profile does: the marker will not
// match and the source is rebuilt rather than silently reused at the old
// shape.
func seedProfile() string {
	if extraTables == 0 {
		return baseSeedProfile
	}
	return fmt.Sprintf("%s+x%dx%dMB", baseSeedProfile, extraTables, extraSizeMB)
}

// scaleArg renders the scale for psql -v and SQL literals: plain decimal,
// no exponent, so numeric parses it exactly.
func scaleArg() string {
	return strconv.FormatFloat(scale, 'f', -1, 64)
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
	Expect(v1beta1.AddToScheme(scheme)).To(Succeed())
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())

	By("checking the Migration CRD exists (the suite does not manage it)")
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "migrations.pgcopydb-operator.io"}, crd)).
		To(Succeed(), "CRD migrations.pgcopydb-operator.io not found or cluster unreachable;"+
			" install the CRD first (chart with crds.install=true, or config/crd), the suite will not create it")
	// The suite talks v1beta1, so that exact version must be served; index 0
	// is not it once several versions coexist, look the entry up by name.
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	var served map[string]any
	for _, v := range versions {
		if m, ok := v.(map[string]any); ok && m["name"] == v1beta1.SchemeGroupVersion.Version {
			served = m
			break
		}
	}
	Expect(served).NotTo(BeNil(),
		"CRD migrations.pgcopydb-operator.io does not serve "+v1beta1.SchemeGroupVersion.Version+
			": upgrade the CRD (chart >= v0.2.0) before running the suite")
	// A pre-follow CRD would silently prune spec.follow at admission and the
	// live scenarios would run as plain clones; fail fast instead.
	_, hasFollow, _ := unstructured.NestedMap(served, "schema", "openAPIV3Schema",
		"properties", "spec", "properties", "follow")
	Expect(hasFollow).To(BeTrue(),
		"CRD migrations.pgcopydb-operator.io has no spec.follow: the cluster CRD predates the"+
			" follow controller; upgrade the CRD (chart >= v0.1.0-alpha.6) before running the live scenarios")

	By("preparing the fixture StorageClass and checking Longhorn capacity")
	ensureFixtureStorage()

	// The suite is single-tenant per cluster: two runs share the release name,
	// the fixture namespaces, and the CNPG clusters, and the second BeforeSuite
	// plus the first AfterSuite silently destroy each other's environment.
	// Fail fast when the release exists; E2E_FORCE=true takes over a release
	// that a crashed run left behind.
	By("checking no other e2e run holds the operator release")
	if err := exec.Command("helm", "status", helmRelease, "-n", nsOperator).Run(); err == nil {
		if !envTrue("E2E_FORCE") {
			Fail("helm release " + helmRelease + " already exists in " + nsOperator +
				": another e2e run is active, or a crashed run left it behind. The suite is" +
				" single-tenant per cluster; wait for the other run to finish, or rerun with" +
				" E2E_FORCE=true to take the release over.")
		}
		_, _ = fmt.Fprintln(GinkgoWriter, "E2E_FORCE=true: taking over the existing release")
	}

	By("installing the suite's own operator into " + nsOperator)
	// Accepted caveat: the production operator watches cluster-wide and may
	// co-reconcile the suite's Migrations alongside this install. The
	// controller converges either way (attempts derive from persisted status,
	// Job creation tolerates AlreadyExists), so the specs assert terminal
	// phase, attempts, Jobs, and data, never event counts or timing.
	helmRun("uninstall", helmRelease, "-n", nsOperator, "--ignore-not-found")
	values := []string{
		"crds.install=false",
		"image.tag=" + operatorTag,
		"runner.image.tag=" + runnerTag,
		"watchNamespaces={" + nsE2E + "," + nsX + "}",
		"leaderElection.enabled=false",
		// Always on so the metrics specs get their scrape; the template
		// renders nothing on a cluster without the Prometheus Operator CRDs.
		// PrometheusRule and the dashboard ConfigMaps stay off: real alert
		// config on a shared cluster, and no sidecar watches this namespace.
		"metrics.serviceMonitor.enabled=true",
	}
	args := []string{"install", helmRelease, chartPath, "-n", nsOperator, "--wait"}
	if manageNamespaces {
		args = append(args, "--create-namespace")
	} else {
		// The chart gates its ClusterRole and binding on rbac.create, and an
		// identity that may not create namespaces may not create cluster RBAC
		// either: the ServiceAccount and its bindings come with the namespaces.
		values = append(values, "rbac.create=false", "serviceAccount.create=false",
			"serviceAccount.name="+managerServiceAccount)
	}
	for _, v := range values {
		args = append(args, "--set", v)
	}
	helmRun(args...)

	if manageNamespaces {
		By("creating or adopting the fixture namespaces")
		ensureNamespace(nsE2E)
		ensureNamespace(nsX)
	}

	By("deleting leftover Migrations from previous runs")
	purgeMigrations(2 * time.Minute)

	By(fmt.Sprintf("creating or adopting the CNPG source (PG %d) and target (PG %d) clusters",
		pgSource, pgTarget))
	ensureClusterShape(sourceCluster, pgSource)
	ensureClusterShape(targetCluster, pgTarget)
	applyCluster(cnpgCluster(sourceCluster, srcStorageSize, pgSource))
	applyCluster(cnpgCluster(targetCluster, tgtStorageSize, pgTarget))
	waitClusterReady(sourceCluster)
	waitClusterReady(targetCluster)

	By(fmt.Sprintf("seeding the source database (profile %s, scale %s)", seedProfile(), scaleArg()))
	ensureSeededSource()

	By("resetting the target database so the fresh-clone scenario starts empty")
	resetTargetObjects()

	By("clearing replication leftovers on the source from crashed runs")
	resetSourceReplication()

	By("clearing replication origin leftovers on the target from crashed runs")
	resetTargetReplication()

	By("granting the follow-mode privileges to the app role")
	ensureFollowPrivileges()
})

var _ = AfterSuite(func() {
	// Purge Migrations BEFORE the operator goes away: the cleanup finalizer
	// needs a live controller to run the cleanup Job and release, and that
	// cleanup is what drops the replication slots. A failed or timed-out
	// spec can leave Migrations behind; uninstalling first would orphan
	// them and wedge the namespace deletion below for the full timeout.
	By("deleting leftover Migrations while the operator still runs")
	purgeMigrations(5 * time.Minute)

	// The throwaway operator always goes away, keep-fixtures or not: every
	// run installs a fresh one.
	By("uninstalling the suite's operator")
	helmRun("uninstall", helmRelease, "-n", nsOperator, "--ignore-not-found")
	if manageNamespaces {
		By("deleting " + nsOperator)
		deleteNamespaces(2*time.Minute, nsOperator)
	}

	if envTrue("E2E_KEEP_FIXTURES") {
		_, _ = fmt.Fprintf(GinkgoWriter,
			"E2E_KEEP_FIXTURES=true: keeping namespaces %s and %s for iteration\n", nsE2E, nsX)
		return
	}
	if !manageNamespaces {
		By("deleting the fixtures inside " + nsE2E + " and " + nsX)
		deleteFixtures(10 * time.Minute)
		return
	}
	By("deleting the fixture namespaces")
	// CNPG teardown plus volume deletion takes a while on the shared cluster.
	deleteNamespaces(10*time.Minute, nsX, nsE2E)
})

// deleteFixtures empties the fixture namespaces instead of deleting them, for
// runs that do not own them. Migrations are gone by here; a deleted CNPG
// Cluster leaves its instance volumes behind, so those go explicitly.
func deleteFixtures(timeout time.Duration) {
	GinkgoHelper()
	deleteCluster(sourceCluster)
	deleteCluster(targetCluster)
	fg := metav1.DeletePropagationForeground
	for _, obj := range []client.Object{
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: seedJobName}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: seedConfigMap}},
	} {
		err := k8sClient.Delete(ctx, obj, &client.DeleteOptions{PropagationPolicy: &fg})
		if err != nil && !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred(), "failed to delete %s", obj.GetName())
		}
	}
	for _, ns := range []string{nsE2E, nsX} {
		Expect(k8sClient.DeleteAllOf(ctx, &corev1.PersistentVolumeClaim{}, client.InNamespace(ns))).
			To(Succeed(), "failed to delete the volumes in %s", ns)
	}
	// The next run recreates the clusters under the same volume names, so the
	// old volumes have to be gone before this one reports done.
	Eventually(func(g Gomega) {
		for _, ns := range []string{nsE2E, nsX} {
			pvcs := &corev1.PersistentVolumeClaimList{}
			g.Expect(k8sClient.List(ctx, pvcs, client.InNamespace(ns))).To(Succeed())
			g.Expect(pvcs.Items).To(BeEmpty(), "volumes still terminating in %s", ns)
		}
	}, timeout, 5*time.Second).Should(Succeed())
}

// helmRun executes helm against the current kubectl context, echoes its
// output, and fails the suite on error. Transient API-server hiccups (TLS
// handshake timeouts happen on the shared dev cluster) get two quick retries;
// a real failure still aborts with helm's output on record.
func helmRun(args ...string) {
	GinkgoHelper()
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		var out []byte
		out, err = exec.Command("helm", args...).CombinedOutput()
		_, _ = GinkgoWriter.Write(out)
		if err == nil {
			return
		}
		time.Sleep(5 * time.Second)
	}
	Expect(err).NotTo(HaveOccurred(), "helm %s failed after 3 attempts", strings.Join(args, " "))
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

// cnpgCluster builds a minimal CNPG Cluster: one instance, tier-sized
// storage, the requested PostgreSQL major, app database owned by app. Seeding
// happens in a separate Job (see ensureSeededSource), not at initdb time: a
// Job survives pod restarts, its log is inspectable, and re-runs are cheap on
// kept clusters. Built as unstructured on purpose: importing the CNPG API
// just for two fixtures is not worth a dependency.
func cnpgCluster(name, size string, major int) *unstructured.Unstructured {
	storage := map[string]any{"size": size}
	if fixtureStorageClass != "" {
		storage["storageClass"] = fixtureStorageClass
	}
	affinity := map[string]any{
		// These three are what CNPG already defaults to, so they change
		// nothing today. They are here so an upstream default that moves
		// cannot quietly return the fixtures to a single node: the whole
		// point of cnpgInstances > 1 is one instance per node.
		"enablePodAntiAffinity": true,
		"podAntiAffinityType":   "preferred",
		"topologyKey":           corev1.LabelHostname,
	}
	if name == targetCluster {
		// CNPG's own anti-affinity repels only instances of the same cluster,
		// so without this the two -1 pods, the primaries at bootstrap, both
		// land on whichever node scores highest and the migration's SQL legs
		// never leave it. Keyed on the instance name and not the primary role:
		// CNPG applies that role label only after promotion, long after the
		// target's first pod has been scheduled.
		affinity["additionalPodAntiAffinity"] = map[string]any{
			"preferredDuringSchedulingIgnoredDuringExecution": []any{map[string]any{
				"weight": int64(100),
				"podAffinityTerm": map[string]any{
					"topologyKey": corev1.LabelHostname,
					"labelSelector": map[string]any{
						"matchLabels": map[string]any{labelCNPGInstance: sourceCluster + "-1"},
					},
				},
			}},
		}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": cnpgGVK.Group + "/" + cnpgGVK.Version,
		"kind":       cnpgGVK.Kind,
		"metadata":   map[string]any{"name": name, "namespace": nsE2E},
		"spec": map[string]any{
			"instances": int64(cnpgInstances),
			// An explicit major so the version matrix (task e2e:matrix) can
			// vary it and the default never drifts with the CNPG operator's
			// own operand default.
			"imageName": fmt.Sprintf("ghcr.io/cloudnative-pg/postgresql:%d", major),
			"storage":   storage,
			"affinity":  affinity,
			// Requests, no limits, and they are not what spreads these pods:
			// the anti-affinity above does that. They make the fixtures cost
			// the scheduler something, which is what stops the workers that
			// have no sibling to repel from all picking the same
			// least-allocated node, and they size the server: a quarter core
			// left a seeding backend pinned at its request for minutes.
			"resources": map[string]any{
				"requests": map[string]any{"cpu": fixtureCPU, "memory": fixtureMemory},
			},
			"bootstrap": map[string]any{"initdb": map[string]any{
				"database": appDB,
				"owner":    appDB,
			}},
			"postgresql": map[string]any{
				"parameters": map[string]any{
					// CNPG defaults wal_sender_timeout to 5s for its own HA
					// streaming; that terminates pgcopydb's logical-decoding
					// walsender whenever a standby status update is a few
					// seconds late (observed live). Use the PostgreSQL default
					// so follow scenarios exercise the operator, not the
					// fixture's aggressive timeout.
					"wal_sender_timeout": "60s",
					// A migration is one long bulk load, and PostgreSQL ships
					// defaults sized for a machine that has other work to do.
					// Left at 128MB of shared_buffers the fixtures spend the
					// clone reading their own pages back off Longhorn, which
					// measures the storage rather than the operator.
					"shared_buffers":       fixtureSharedBuffers,
					"effective_cache_size": fixtureCacheSize,
					// Index builds during pg_restore, and the sort memory the
					// seed's generate_series passes through.
					"maintenance_work_mem": "512MB",
					// At the 16MB default a seeding backend was flushing
					// WAL itself in ~70KB chunks: wal_buffers_full 133,611
					// against wal_write 135,705 across one seed. The
					// concurrent load stages make that tighter still.
					"wal_buffers": "64MB",
					// Bulk load checkpoints on volume, not on time. Small
					// max_wal_size means a checkpoint every few seconds and a
					// full-page write storm behind it. Proportional, because
					// WAL shares the data volume: a flat 4GB was 80% of a
					// scale 0.1 fixture and PostgreSQL stopped mid-restore
					// with a low-disk condition.
					"max_wal_size":       walMaxSize(size),
					"checkpoint_timeout": "15min",
				},
			},
		},
	}}
}

// fixtureAntiAffinity keeps a suite-created worker pod off the nodes running
// the fixture primaries, so a migration's SQL legs cross the real network
// instead of looping back inside one node.
//
// It repels the primaries specifically, not every instance. With cnpgInstances
// spread one per node, every node carries a source instance and a target
// instance, so repelling all of them scores every node identically and decides
// nothing. Repelling the two primaries leaves the node that holds neither,
// which is the production shape: a runner off-box from both databases it
// talks to.
//
// Preferred, never required. On a cluster with fewer nodes than fixtures a
// required term would leave the pod Pending until the scenario's timeout, and
// the shared dev cluster runs a descheduler that enforces required inter-pod
// anti-affinity by eviction, which would kill a runner mid-migration.
// Namespaces is spelled out because the cross-namespace scenario schedules its
// runner in nsX: an empty namespaces field means "the scheduling pod's own
// namespace", so the term would match nothing there and read as satisfied.
func fixtureAntiAffinity() *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					Namespaces:  []string{nsE2E},
					TopologyKey: corev1.LabelHostname,
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{{
							Key:      labelCNPGCluster,
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{sourceCluster, targetCluster},
						}, {
							Key:      labelCNPGRole,
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{rolePrimary},
						}},
					},
				},
			}},
		},
	}
}

// workerResources is what every suite-created worker pod requests. Requests
// only: they exist so the scheduler can see the pod at all (see the note in
// cnpgCluster), not to cap what it may use.
func workerResources(cpu, memory string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}}
}

// walHeadroom returns the fixture's max_wal_size and its volume size, both in
// bytes, read off the live Cluster. The spec that uses it exists because a
// flat max_wal_size once took 80% of a small fixture's volume and PostgreSQL
// stopped mid-restore; nothing failed until then, because WAL only fills the
// disk once there is enough data to fill it with.
func walHeadroom(cluster string) (wal, volume int64) {
	GinkgoHelper()
	c := &unstructured.Unstructured{}
	c.SetGroupVersionKind(cnpgGVK)
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: cluster}, c)).
		To(Succeed(), "failed to read CNPG cluster %s", cluster)

	size, _, _ := unstructured.NestedString(c.Object, "spec", "storage", "size")
	Expect(size).NotTo(BeEmpty(), "CNPG cluster %s declares no storage size", cluster)
	q := resource.MustParse(size)

	got, found, _ := unstructured.NestedString(c.Object, "spec", "postgresql", "parameters", "max_wal_size")
	Expect(found).To(BeTrue(), "CNPG cluster %s sets no max_wal_size", cluster)
	mb, err := strconv.Atoi(strings.TrimSuffix(got, "MB"))
	Expect(err).NotTo(HaveOccurred(), "max_wal_size %q on %s is not a plain MB value", got, cluster)
	return int64(mb) << 20, q.Value()
}

// walMaxSize returns a max_wal_size proportional to the volume it is given.
// CNPG keeps pg_wal inside PGDATA, so WAL and data compete for one PVC and a
// value that is comfortable at the default tier fills a small one outright. A
// fifth still spaces checkpoints out across a bulk load and leaves four fifths
// for the data being loaded. At the smallest tier this lands on PostgreSQL's
// own 1GB default, which is the point: no worse than stock where there is no
// room to be better.
func walMaxSize(volume string) string {
	q := resource.MustParse(volume)
	return fmt.Sprintf("%dMB", q.Value()/5>>20)
}

// applyCluster server-side applies the fixture, which both creates it fresh
// and adopts an existing one left over from manual runs.
func applyCluster(obj *unstructured.Unstructured) {
	GinkgoHelper()
	Expect(k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(obj),
		client.FieldOwner(fieldOwner), client.ForceOwnership)).
		To(Succeed(), "failed to apply CNPG cluster %s", obj.GetName())
}

// waitClusterReady waits until every instance is ready, not just the primary.
// One ready instance would let the suite start while the replicas are still
// cloning, which is exactly when the fixtures are not yet spread across nodes
// and a chaos failover has nowhere to promote to.
func waitClusterReady(name string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		c := &unstructured.Unstructured{}
		c.SetGroupVersionKind(cnpgGVK)
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, c)).To(Succeed())
		ready, _, _ := unstructured.NestedInt64(c.Object, "status", "readyInstances")
		g.Expect(ready).To(BeNumerically(">=", int64(cnpgInstances)),
			"CNPG cluster %s has %d of %d instances ready", name, ready, cnpgInstances)
	}, clusterReadyTimeout, 5*time.Second).Should(Succeed())
}

// resetTargetObjects wipes the fixture objects from the target. Needed
// because pg_restore --clean only drops objects present in the incoming dump,
// so a populated target from an earlier run would break the no-dropIfExists
// clone. Dropping the schemas wholesale (plus the citext extension and the
// large objects, which live outside any schema) beats keeping a per-object
// drop list in sync with the fixture set. public is recreated with its stock
// owner; app is the database owner, so it needs no extra grants.
func resetTargetObjects() {
	GinkgoHelper()
	psql(targetCluster, "DROP EXTENSION IF EXISTS citext CASCADE")
	psql(targetCluster, "DROP SCHEMA IF EXISTS audit CASCADE")
	psql(targetCluster, "DROP SCHEMA IF EXISTS public CASCADE")
	psql(targetCluster, "CREATE SCHEMA public AUTHORIZATION pg_database_owner")
	psql(targetCluster, "SELECT lo_unlink(oid) FROM pg_largeobject_metadata")
}

// ensureSeededSource brings the source database to the requested fixture
// profile and scale. A kept cluster whose e2e_seed marker matches costs one
// early-exiting Job; a mismatching marker (different profile or scale) means
// the data on disk is wrong in ways reseeding cannot fix (a smaller scale
// leaves surplus rows), so the cluster is recreated from scratch.
func ensureSeededSource() {
	GinkgoHelper()
	if psql(sourceCluster, "SELECT to_regclass('public.e2e_seed') IS NOT NULL") == "t" {
		match := psql(sourceCluster, fmt.Sprintf(
			"SELECT EXISTS (SELECT 1 FROM e2e_seed WHERE profile = '%s' AND scale = '%s'::numeric)",
			seedProfile(), scaleArg()))
		if match != "t" {
			By("recreating the source cluster: kept fixtures carry a different seed profile or scale")
			recreateSourceCluster()
		}
	}
	runSeedJob()
}

// recreateSourceCluster deletes the source CNPG cluster (volumes included)
// and brings a fresh one up. Used when kept fixtures carry a stale seed
// profile or scale; a major, instance-count or storage-class mismatch on a
// kept cluster is handled earlier by ensureClusterShape, for the target too.
func recreateSourceCluster() {
	GinkgoHelper()
	deleteCluster(sourceCluster)
	applyCluster(cnpgCluster(sourceCluster, srcStorageSize, pgSource))
	waitClusterReady(sourceCluster)
}

// deleteCluster deletes a CNPG cluster (volumes included) and waits until it
// is gone.
func deleteCluster(name string) {
	GinkgoHelper()
	c := &unstructured.Unstructured{}
	c.SetGroupVersionKind(cnpgGVK)
	c.SetNamespace(nsE2E)
	c.SetName(name)
	if err := k8sClient.Delete(ctx, c); err != nil && !apierrors.IsNotFound(err) {
		Expect(err).NotTo(HaveOccurred(), "failed to delete CNPG cluster %s", name)
	}
	Eventually(func(g Gomega) {
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name},
			&unstructured.Unstructured{Object: map[string]any{
				"apiVersion": cnpgGVK.Group + "/" + cnpgGVK.Version, "kind": cnpgGVK.Kind}})
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "CNPG cluster %s still terminating", name)
	}, 10*time.Minute, 5*time.Second).Should(Succeed())
}

// ensureClusterShape deletes a kept cluster that this run cannot adopt in
// place, so the subsequent apply creates it fresh. Three things force that: a
// different PostgreSQL major (CNPG cannot change majors in place, so applying
// another major's imageName would wedge the cluster rather than upgrade it), a
// different instance count, and a different storage class, which is immutable
// once a PVC is bound. The two shape checks read the spec and run before the
// readiness wait on purpose: a kept single-instance cluster can never reach
// cnpgInstances ready, so waiting first would time out instead of recreating.
func ensureClusterShape(name string, major int) {
	GinkgoHelper()
	c := &unstructured.Unstructured{}
	c.SetGroupVersionKind(cnpgGVK)
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, c)
	if apierrors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred(), "failed to get CNPG cluster %s", name)

	if got, _, _ := unstructured.NestedInt64(c.Object, "spec", "instances"); got != int64(cnpgInstances) {
		By(fmt.Sprintf("deleting %s: kept cluster has %d instances, this run wants %d",
			name, got, cnpgInstances))
		deleteCluster(name)
		return
	}
	if got, _, _ := unstructured.NestedString(c.Object, "spec", "storage", "storageClass"); got != fixtureStorageClass {
		By(fmt.Sprintf("deleting %s: kept cluster is on storage class %q, this run wants %q",
			name, got, fixtureStorageClass))
		deleteCluster(name)
		return
	}
	// The kept instances have to answer psql before the major can be read.
	waitClusterReady(name)
	if got := serverMajor(name); got != major {
		By(fmt.Sprintf("deleting %s: kept cluster runs PG %d, this run wants PG %d", name, got, major))
		deleteCluster(name)
	}
}

// serverMajor asks the cluster's primary for its PostgreSQL major version.
func serverMajor(cluster string) int {
	GinkgoHelper()
	num, err := strconv.Atoi(psql(cluster, "SELECT current_setting('server_version_num')"))
	Expect(err).NotTo(HaveOccurred(), "unparseable server_version_num from %s", cluster)
	return num / 10000
}

// runSeedJob loads the fixtures into the source database through a Kubernetes
// Job (bash and psql from the CNPG operand image, fixture files mounted from a
// ConfigMap, staged by run.sh). A Job instead of exec: it survives suite
// interruptions, retries transient DB unavailability (backoffLimit 2), and
// leaves its log behind. The log tail lands in the Ginkgo output either way:
// on failure as the diagnosis, on success as the seed's per-phase profile.
func runSeedJob() {
	GinkgoHelper()
	entries, err := fixturesFS.ReadDir("fixtures")
	Expect(err).NotTo(HaveOccurred(), "failed to read the embedded fixtures")
	files := make(map[string]string, len(entries))
	for _, e := range entries {
		b, err := fixturesFS.ReadFile("fixtures/" + e.Name())
		Expect(err).NotTo(HaveOccurred(), "failed to read fixture %s", e.Name())
		files[e.Name()] = string(b)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: seedConfigMap}}
	_, err = controllerutil.CreateOrUpdate(ctx, k8sClient, cm, func() error {
		cm.Data = files
		return nil
	})
	Expect(err).NotTo(HaveOccurred(), "failed to apply the fixture ConfigMap")

	// A finished Job is immutable; drop the previous run's before creating.
	fg := metav1.DeletePropagationForeground
	stale := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: seedJobName}}
	if err := k8sClient.Delete(ctx, stale, &client.DeleteOptions{PropagationPolicy: &fg}); err != nil &&
		!apierrors.IsNotFound(err) {
		Expect(err).NotTo(HaveOccurred(), "failed to delete the previous seed Job")
	}
	Eventually(func(g Gomega) {
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: seedJobName}, &batchv1.Job{})
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "previous seed Job still terminating")
	}, 2*time.Minute, 2*time.Second).Should(Succeed())

	Expect(k8sClient.Create(ctx, buildSeedJob())).To(Succeed(), "failed to create the seed Job")
	started := time.Now()
	Eventually(func(g Gomega) {
		job := &batchv1.Job{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: seedJobName}, job)).To(Succeed())
		for _, c := range job.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
				_, _ = fmt.Fprintf(GinkgoWriter, "seed Job failed, log tail:\n%s\n", seedJobLogs(seedLogTail))
				StopTrying("seed Job exhausted its retries: " + c.Message).Now()
			}
		}
		g.Expect(job.Status.Succeeded).To(BeNumerically(">=", 1), "seed Job still running")
	}, seedTimeout, 10*time.Second).Should(Succeed())

	// The log carries a \timing line per table, so on success it is the seed's
	// per-phase profile and belongs in the run's output. Without it the only
	// way to learn where the seed spends its minutes is to sample a live
	// cluster while one happens to be running (issue #146).
	_, _ = fmt.Fprintf(GinkgoWriter, "seed Job log:\n%s\n", seedJobLogs(seedLogTail))
	AddReportEntry("seed wall clock", fmt.Sprintf("%s at scale %s (profile %s)",
		time.Since(started).Round(time.Second), scaleArg(), seedProfile()))
}

func buildSeedJob() *batchv1.Job {
	backoff := int32(2)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: seedJobName},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Affinity:      fixtureAntiAffinity(),
					Containers: []corev1.Container{{
						Name:      "seed",
						Image:     seedImage,
						Resources: workerResources(workerCPU, workerMemory),
						// bash, not sh: run.sh reads PIPESTATUS to report the
						// failing stage rather than the exit of the sed that
						// labels its output.
						Command: []string{"bash", "/fixtures/run.sh"},
						Env: []corev1.EnvVar{
							{Name: "SEED_SCALE", Value: scaleArg()},
							{Name: "SEED_PROFILE", Value: seedProfile()},
							{Name: "SEED_EXTRA_TABLES", Value: strconv.Itoa(extraTables)},
							{Name: "SEED_EXTRA_MB", Value: strconv.Itoa(extraSizeMB)},
							{Name: "PGHOST", Value: sourceCluster + "-rw." + nsE2E + ".svc"},
							{Name: "PGDATABASE", Value: appDB},
							{Name: "PGUSER", Value: appDB},
							{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: srcSecret},
									Key:                  passwordKey,
								},
							}},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "fixtures", MountPath: "/fixtures", ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{{Name: "fixtures", VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: seedConfigMap},
						},
					}}},
				},
			},
		},
	}
}

// seedJobLogs returns the last lines of the seed Job's log. kubectl for the
// same reason psql uses it: current context, no hand-rolled log streaming.
func seedJobLogs(lines int) string {
	out, _ := exec.Command("kubectl", "logs", "-n", nsE2E, "job/"+seedJobName,
		"--tail="+strconv.Itoa(lines)).CombinedOutput()
	return string(out)
}

// ensureFixtureStorage prepares the class the fixture volumes bind to and
// refuses a run that would not fit. Longhorn is optional: without it the
// suite-owned class could never bind, so the fixtures fall back to the
// cluster default and there is no capacity to read. E2E_STORAGE_CLASS is left
// alone, because whoever pins a class owns the capacity question with it.
func ensureFixtureStorage() {
	GinkgoHelper()
	if fixtureStorageClass != ephemeralStorageClass {
		return
	}
	if !longhornPresent() {
		By("no Longhorn in this cluster: fixture volumes fall back to the cluster default StorageClass")
		fixtureStorageClass = ""
		return
	}
	ensureEphemeralStorageClass()
	checkLonghornCapacity()
}

// longhornPresent reports whether this cluster runs Longhorn, by asking for
// the CRD the capacity check reads. A missing CRD is the only soft answer:
// being denied the read is not the same as the cluster not having Longhorn,
// and quietly treating it as absent would move the fixtures onto the default
// class and recreate both CNPG clusters to get there. Pin E2E_STORAGE_CLASS
// instead, which is what a confined identity is expected to do.
func longhornPresent() bool {
	GinkgoHelper()
	err := k8sClient.List(ctx, longhornNodes())
	if apimeta.IsNoMatchError(err) {
		return false
	}
	if apierrors.IsForbidden(err) {
		Fail("this identity may not read nodes.longhorn.io, so the fixture StorageClass" +
			" cannot be chosen here; set E2E_STORAGE_CLASS to pin it")
	}
	Expect(err).NotTo(HaveOccurred(), "failed to list nodes.longhorn.io")
	return true
}

// longhornNodes is an empty list typed to the Longhorn Node CRD.
func longhornNodes() *unstructured.UnstructuredList {
	l := &unstructured.UnstructuredList{}
	l.SetGroupVersionKind(schema.GroupVersionKind{Group: "longhorn.io", Version: "v1beta2", Kind: "NodeList"})
	return l
}

const (
	// longhornProvisioner is the CSI driver name every Longhorn StorageClass
	// carries, and how the suite recognises one it can copy settings from.
	longhornProvisioner = "driver.longhorn.io"
	// paramDataEngine decides which Longhorn engine provisions a volume, and
	// paramReplicas how many copies it keeps.
	paramDataEngine = "dataEngine"
	paramReplicas   = "numberOfReplicas"
)

// ephemeralParams builds the parameters for the suite's own class, carrying
// over the data engine the cluster's other Longhorn classes ask for.
//
// Longhorn reads an unset dataEngine as v1, so a class that omits it is
// unprovisionable on a cluster running v2 only: every claim is denied by the
// admission webhook with "v1-data-engine data engine is not enabled", and the
// fixture pods sit unschedulable on pending claims that say nothing about
// why. Copying rather than pinning keeps the suite working on either engine.
func ephemeralParams(classes []storagev1.StorageClass) map[string]string {
	p := map[string]string{paramReplicas: "1", "staleReplicaTimeout": "30"}
	for _, sc := range classes {
		if sc.Name == ephemeralStorageClass || sc.Provisioner != longhornProvisioner {
			continue
		}
		if e := sc.Parameters[paramDataEngine]; e != "" {
			p[paramDataEngine] = e
			break
		}
	}
	return p
}

// ensureEphemeralStorageClass creates the suite-owned single-replica Longhorn
// StorageClass, and replaces it when the cluster has moved on beneath it. It
// stays behind after the run: it is configuration, not data, and reclaimPolicy
// Delete means volumes never outlive their claims.
//
// Replacing means deleting first, because a StorageClass's parameters are
// immutable. That is safe here: a class is consulted only when a volume is
// provisioned, so bound claims from a kept fixture keep working, and the
// replacement carries the same name.
func ensureEphemeralStorageClass() {
	GinkgoHelper()
	var classes storagev1.StorageClassList
	Expect(k8sClient.List(ctx, &classes)).To(Succeed(), "failed to list StorageClasses")
	want := ephemeralParams(classes.Items)

	var have storagev1.StorageClass
	err := k8sClient.Get(ctx, client.ObjectKey{Name: ephemeralStorageClass}, &have)
	switch {
	case err == nil && maps.Equal(have.Parameters, want):
		return
	case err == nil:
		By(fmt.Sprintf("replacing StorageClass %s: its parameters no longer match this cluster", ephemeralStorageClass))
		Expect(k8sClient.Delete(ctx, &have)).To(Succeed(),
			"failed to delete the stale StorageClass %s", ephemeralStorageClass)
	case !apierrors.IsNotFound(err):
		Expect(err).NotTo(HaveOccurred(), "failed to read StorageClass %s", ephemeralStorageClass)
	}

	reclaim := corev1.PersistentVolumeReclaimDelete
	sc := &storagev1.StorageClass{
		ObjectMeta:    metav1.ObjectMeta{Name: ephemeralStorageClass},
		Provisioner:   longhornProvisioner,
		ReclaimPolicy: &reclaim,
		Parameters:    want,
	}
	if err := k8sClient.Create(ctx, sc); err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred(), "failed to create StorageClass %s", ephemeralStorageClass)
	}
}

// checkLonghornCapacity refuses to start a run that would fill the shared
// cluster: it sums storageAvailable over every disk of every nodes.longhorn.io
// CR (live cluster state, deliberately never hardcoded) and Skips unless the
// requested fixture volumes fit with 20% headroom.
func checkLonghornCapacity() {
	GinkgoHelper()
	nodes := longhornNodes()
	Expect(k8sClient.List(ctx, nodes)).To(Succeed(), "failed to list nodes.longhorn.io")
	var available int64
	for _, n := range nodes.Items {
		disks, _, _ := unstructured.NestedMap(n.Object, "status", "diskStatus")
		for _, d := range disks {
			dm, ok := d.(map[string]any)
			if !ok {
				continue
			}
			switch v := dm["storageAvailable"].(type) {
			case int64:
				available += v
			case float64:
				available += int64(v)
			}
		}
	}
	// Every CNPG instance carries a full copy of its cluster's data, so the
	// source and target volumes are provisioned once per instance. The work
	// volume is one per migration and does not multiply.
	var required int64
	for _, size := range []string{srcStorageSize, tgtStorageSize} {
		q := resource.MustParse(size)
		required += q.Value() * int64(cnpgInstances)
	}
	work := resource.MustParse(workVolumeSize)
	required += work.Value()
	// numberOfReplicas is 1 on the ephemeral StorageClass, so requested
	// bytes map 1:1 to consumed bytes; 1.2 leaves headroom for WAL churn
	// and everything else living on the shared disks.
	const replicas = 1
	need := required * replicas * 12 / 10
	if available < need {
		Skip(fmt.Sprintf("this run needs %dGi available across Longhorn disks"+
			" (%dGi requested x %d replica x 1.2 headroom, source and target sized for"+
			" %d CNPG instances each) but only %dGi are free; free up space or lower E2E_SCALE",
			need>>30, required>>30, replicas, cnpgInstances, available>>30))
	}
}

// ensureFollowPrivileges grants the app role the non-superuser follow
// prerequisites: the REPLICATION attribute on the source (slot creation and
// WAL sender), EXECUTE on the pg_replication_origin_* functions on the target
// (origin tracking during apply and cleanup), and SET on
// session_replication_role on the target (the apply session's preamble;
// without it pgcopydb 0.18 silently applies nothing while reporting success).
// Runs on every suite start because kept fixtures skip initdb SQL; every
// statement is idempotent. docs/reference/prerequisites.md documents the same
// set for users. GRANT SET ON PARAMETER exists from PG15; init rejects older
// targets (PG14 is a source only), so the statement always parses here.
func ensureFollowPrivileges() {
	GinkgoHelper()
	psql(sourceCluster, "ALTER ROLE app REPLICATION")
	grantTargetFollowPrivileges(appDB)
}

// grantTargetFollowPrivileges grants the target-side follow prerequisites to
// app in one database. EXECUTE on catalog functions is per-database, so a
// second target database (the chaos fan-out scenario) needs its own pass; the
// parameter grant is cluster-wide but idempotent and simply rides along.
func grantTargetFollowPrivileges(db string) {
	GinkgoHelper()
	psqlDB(targetCluster, db, "DO $$ DECLARE f oid; BEGIN FOR f IN SELECT p.oid FROM pg_proc p"+
		" JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'pg_catalog'"+
		" AND p.proname LIKE 'pg_replication_origin%' LOOP"+
		" EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO app', f::regprocedure); END LOOP; END $$")
	psqlDB(targetCluster, db, "GRANT SET ON PARAMETER session_replication_role TO app")
}

// resetSourceReplication drops what a crashed follow run may have left on the
// source: live-write rows (the fresh-clone scenario asserts exact fixture
// counts), pgcopydb replication slots, and auto-created publications. Slot and
// publication names are deterministic per Migration name, so a leftover would
// collide with this run's follow scenarios.
func resetSourceReplication() {
	GinkgoHelper()
	psql(sourceCluster, "DELETE FROM orders WHERE note LIKE 'live-%'")
	psql(sourceCluster, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"+
		" WHERE slot_name LIKE 'pgcopydb%' AND NOT active")
	psql(sourceCluster, "DO $$ DECLARE p text; BEGIN FOR p IN SELECT pubname FROM pg_publication"+
		" WHERE pubname LIKE 'pgcopydb%' LOOP EXECUTE format('DROP PUBLICATION %I', p); END LOOP; END $$")
}

// resetTargetReplication drops pgcopydb replication origins a crashed or
// pre-alpha.11 run left on the target. Cleanup before that fix never passed
// --origin, so a follow migration's custom-named origin lingered. The follow
// scenarios now assert the target ends with zero pgcopydb origins, so a
// leftover from an earlier run would fail them; drop them up front. Safe at
// suite start: no Migration runs and the source carries no slots, so no origin
// is in use.
func resetTargetReplication() {
	GinkgoHelper()
	psql(targetCluster, "SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin"+
		" WHERE roname LIKE 'pgcopydb%'")
}

// instancePods returns the CNPG instance pods of one fixture cluster. A
// namespaced read, so it works under the confined CI identity.
func instancePods(cluster string) []corev1.Pod {
	GinkgoHelper()
	pods := &corev1.PodList{}
	Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E), client.MatchingLabels{
		labelCNPGCluster: cluster,
		labelCNPGPodRole: "instance",
	})).To(Succeed(), "failed to list instances of CNPG cluster %s", cluster)
	Expect(pods.Items).NotTo(BeEmpty(), "CNPG cluster %s has no instance pods", cluster)
	return pods.Items
}

// seedPod returns the seed Job's pod, the dependable witness for what
// fixtureAntiAffinity renders onto a suite-created worker: BeforeSuite always
// creates it and a completed pod keeps its spec.
func seedPod() corev1.Pod {
	GinkgoHelper()
	pods := &corev1.PodList{}
	Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
		client.MatchingLabels{"batch.kubernetes.io/job-name": seedJobName})).
		To(Succeed(), "failed to list the seed Job's pods")
	Expect(pods.Items).NotTo(BeEmpty(), "the seed Job left no pod behind")
	return pods.Items[0]
}

// spreadsOverHostname reports whether the pod carries a preferred pod
// anti-affinity keyed on the hostname. Preferred and not required is part of
// the contract: a required term wedges a small cluster and invites the
// descheduler to evict a runner mid-migration.
func spreadsOverHostname(spec *corev1.PodSpec) bool {
	if spec.Affinity == nil || spec.Affinity.PodAntiAffinity == nil {
		return false
	}
	for _, t := range spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		if t.PodAffinityTerm.TopologyKey == corev1.LabelHostname {
			return true
		}
	}
	return false
}

// requestsCPUAndMemory reports whether every container asks the scheduler for
// something. Zero requests is what made the whole suite pile onto one node.
func requestsCPUAndMemory(spec *corev1.PodSpec) bool {
	for _, c := range spec.Containers {
		if c.Resources.Requests.Cpu().IsZero() || c.Resources.Requests.Memory().IsZero() {
			return false
		}
	}
	return len(spec.Containers) > 0
}

// instanceNodes returns the distinct node names carrying the cluster's
// instances. A set, not a count of pods: two instances on one node is exactly
// the failure the caller is looking for.
func instanceNodes(cluster string) []string {
	GinkgoHelper()
	pods := instancePods(cluster)
	seen := map[string]bool{}
	var nodes []string
	for i := range pods {
		pod := &pods[i]
		// A pod on its way out still carries the node it used to run on, and
		// counting it would report a replacement as co-location. Skip anything
		// terminating or not running, so a missing instance stays a missing
		// instance rather than becoming a placement bug.
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if n := pod.Spec.NodeName; n != "" && !seen[n] {
			seen[n] = true
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// schedulableNodes counts the nodes a fixture pod could actually land on, so
// the placement assertion scales to the cluster running it instead of
// hardcoding a node count the repo is not allowed to record anyway. The
// fixtures carry no tolerations, so any NoSchedule or NoExecute taint rules a
// node out just as cordoning does; counting those in would fail the assertion
// on a cluster where the pods had nowhere else to go.
//
// Zero means the answer is unknowable rather than "no nodes": in CI the suite
// runs as an identity confined to the fixture namespaces, which may not read
// anything cluster scoped. The caller Skips on zero. Widening that identity to
// satisfy an assertion would trade a real security boundary for a test.
func schedulableNodes() int {
	GinkgoHelper()
	nodes := &corev1.NodeList{}
	err := k8sClient.List(ctx, nodes)
	if apierrors.IsForbidden(err) {
		return 0
	}
	Expect(err).NotTo(HaveOccurred(), "failed to list nodes")
	n := 0
	for i := range nodes.Items {
		if usableNode(&nodes.Items[i]) {
			n++
		}
	}
	Expect(n).To(BeNumerically(">", 0), "no schedulable node in this cluster")
	return n
}

// usableNode reports whether a pod with no tolerations can be scheduled on a
// node: Ready, not cordoned, and free of blocking taints.
func usableNode(n *corev1.Node) bool {
	if n.Spec.Unschedulable {
		return false
	}
	for _, t := range n.Spec.Taints {
		if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// primaryPod returns the pod name of the cluster's current primary. Which
// instance carries the role moves on failover and the chaos scenarios force
// failovers on purpose, so a hardcoded <cluster>-1 would send writes to a
// read-only replica. A promotion leaves the cluster without a primary for a
// few seconds, so this waits out the gap instead of failing into it.
func primaryPod(cluster string) string {
	GinkgoHelper()
	var name string
	Eventually(func(g Gomega) {
		pods := &corev1.PodList{}
		g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E), client.MatchingLabels{
			labelCNPGCluster: cluster,
			labelCNPGRole:    rolePrimary,
		})).To(Succeed())
		// The count, never the slice: a failed HaveLen prints the pods it
		// matched, node names and all, and this repository's CI logs are
		// public. Bound to a variable so ginkgolinter does not rewrite it back.
		found := len(pods.Items)
		g.Expect(found).To(Equal(1), "CNPG cluster %s has %d primaries, want 1", cluster, found)
		name = pods.Items[0].Name
	}, primaryTimeout, 2*time.Second).Should(Succeed())
	return name
}

// psql runs one statement against the app database; see psqlDB.
func psql(cluster, sql string) string {
	GinkgoHelper()
	return psqlDB(cluster, appDB, sql)
}

// psqlDB runs one statement as the in-pod postgres user on the current primary
// and returns trimmed stdout. It wraps psqlDBErr with Ginkgo assertions for
// spec goroutines.
func psqlDB(cluster, db, sql string) string {
	GinkgoHelper()
	out, err := psqlDBErr(cluster, db, sql)
	if f, ok := errors.AsType[*psqlFailure](err); ok {
		Expect(f.err).NotTo(HaveOccurred(), "psql %q on %s failed: %s", sql, f.pod, f.stderr)
	}
	// psqlDBErr today only ever returns nil or *psqlFailure, but any other
	// non-nil error must still fail here rather than read back as "".
	Expect(err).NotTo(HaveOccurred(), "psql %q failed: %s", sql, err)
	return out
}

type commandFactory func(context.Context, string, ...string) *exec.Cmd

func commandOutput(
	ctx context.Context,
	command commandFactory,
	name string,
	args ...string,
) ([]byte, error) {
	cmd := command(ctx, name, args...)
	out, err := cmd.Output()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return out, ctxErr
	}
	return out, err
}

// psqlFailure is psqlDBErr's error result: the pod the last attempt ran
// against and its stderr, so psqlDB can raise its exact failure message
// without a second copy of the retry loop.
type psqlFailure struct {
	pod, stderr string
	err         error
}

func (f *psqlFailure) Error() string {
	return fmt.Sprintf("on %s: %s: %v", f.pod, f.stderr, f.err)
}

func (f *psqlFailure) Unwrap() error {
	return f.err
}

// psqlDBErr retries transient connection failures before a statement reaches
// PostgreSQL. Each retry re-resolves the primary; connected failures stop
// because the statement may have run and callers need not be idempotent.
func psqlDBErr(cluster, db, sql string) (string, error) {
	// Here for primaryPod's Eventually, not for an assertion of its own:
	// without it a primary timeout is reported here, not at the calling spec.
	GinkgoHelper()
	return psqlDBErrWith(
		cluster,
		db,
		sql,
		primaryPod,
		time.Sleep,
		exec.CommandContext,
		e2eCommandTimeout,
	)
}

func psqlDBErrWith(
	cluster, db, sql string,
	primary func(string) string,
	wait func(time.Duration),
	command commandFactory,
	timeout time.Duration,
) (string, error) {
	var lastErr error
	var lastStderr string
	var pod string
	for attempt := 1; attempt <= 3; attempt++ {
		pod = primary(cluster)
		attemptCtx, cancel := context.WithTimeout(context.Background(), timeout)
		out, err := commandOutput(
			attemptCtx,
			command,
			"kubectl",
			"exec", "-n", nsE2E, pod, "-c", "postgres", "--",
			"psql", "-U", "postgres", db, "-tAc", sql,
		)
		cancel()
		if err == nil {
			return strings.TrimSpace(string(out)), nil
		}
		lastErr = err
		lastStderr = ""
		if errors.Is(err, context.DeadlineExceeded) {
			lastErr = fmt.Errorf(
				"psql %q for cluster %s on pod %s timed out after %s: %w",
				sql, cluster, pod, timeout, err,
			)
			break
		}
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			lastStderr = string(ee.Stderr)
		}
		if !transientExecError(lastStderr) {
			break
		}
		if attempt < 3 {
			wait(5 * time.Second)
		}
	}
	return "", &psqlFailure{pod: pod, stderr: lastStderr, err: lastErr}
}

// liveWriter sends ordered inserts through one psql child until stopped or a
// write fails. The final marker query uses the primary captured at startup.
type liveWriter struct {
	stopCh chan struct{}
	reaped chan struct{}
	done   chan struct{}
	// stopOnce lets the spec's cleanup call stop after the spec already did.
	stopOnce sync.Once

	ctx      context.Context
	cancel   context.CancelFunc
	command  commandFactory
	interval time.Duration
	timeout  time.Duration

	mu   sync.Mutex
	pod  string
	last int
	err  error
}

// startLiveWriter resolves the source primary before it returns the writer.
// Call stop to close and reap the persistent child.
func startLiveWriter(marker string) *liveWriter {
	return startLiveWriterWith(
		marker,
		primaryPod,
		exec.CommandContext,
		liveWriteInterval,
		e2eCommandTimeout,
	)
}

func startLiveWriterWith(
	marker string,
	primary func(string) string,
	command commandFactory,
	interval, timeout time.Duration,
) *liveWriter {
	pod := primary(sourceCluster)
	writerCtx, cancel := context.WithCancel(context.Background())
	w := &liveWriter{
		stopCh:   make(chan struct{}),
		reaped:   make(chan struct{}),
		done:     make(chan struct{}),
		ctx:      writerCtx,
		cancel:   cancel,
		command:  command,
		interval: interval,
		timeout:  timeout,
		pod:      pod,
	}
	go w.run(marker)
	return w
}

func (w *liveWriter) recordErr(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
	}
}

func (w *liveWriter) readFinalMarker(marker, pod string) {
	query := liveMarkerQuery(marker)
	queryCtx, cancel := context.WithTimeout(context.Background(), w.timeout)
	out, err := commandOutput(
		queryCtx,
		w.command,
		"kubectl",
		"exec", "-n", nsE2E, pod, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-tAc", query,
	)
	cancel()
	if errors.Is(err, context.DeadlineExceeded) {
		w.recordErr(fmt.Errorf(
			"read final marker for cluster %s on pod %s with SQL %q timed out after %s: %w",
			sourceCluster, pod, query, w.timeout, err,
		))
		return
	}
	if err != nil {
		w.recordErr(fmt.Errorf("read final marker: %w", err))
		return
	}
	last, err := parseLiveMarker(out)
	if err != nil {
		w.recordErr(fmt.Errorf("parse final marker: %w", err))
		return
	}
	w.mu.Lock()
	w.last = last
	w.mu.Unlock()
}

func liveMarkerQuery(marker string) string {
	return fmt.Sprintf(
		"SELECT COALESCE(MAX((substring(note FROM char_length('%s') + 2))::int), 0)"+
			" FROM orders WHERE note LIKE '%s-%%'", marker, marker)
}

func parseLiveMarker(out []byte) (int, error) {
	value := strings.TrimSpace(string(out))
	if value == "" {
		return 0, errors.New("final marker query returned no value")
	}
	last, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %q as integer: %w", value, err)
	}
	return last, nil
}

// run starts one stream on the captured primary and never replaces either.
// Every started child is closed, waited, and followed by one marker query.
func (w *liveWriter) run(marker string) {
	// GinkgoRecover must run before close(w.done): defers run in reverse
	// registration order, so a panic recovers before done closes, not after.
	defer close(w.done)
	defer GinkgoRecover()
	defer w.cancel()

	pod := w.pod
	cmd := w.command(w.ctx, "kubectl", "exec", "-i", "-n", nsE2E, pod, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-q", "-v", "ON_ERROR_STOP=1")
	select {
	case <-w.stopCh:
		close(w.reaped)
		return
	default:
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		w.recordErr(fmt.Errorf("open persistent psql stdin: %w", err))
		close(w.reaped)
		return
	}
	if err := cmd.Start(); err != nil {
		w.recordErr(fmt.Errorf("start persistent psql: %w", err))
		if closeErr := stdin.Close(); closeErr != nil {
			w.recordErr(fmt.Errorf("close persistent psql stdin after start failure: %w", closeErr))
		}
		close(w.reaped)
		return
	}

	defer func() {
		if err := stdin.Close(); err != nil {
			w.recordErr(fmt.Errorf("close persistent psql stdin: %w", err))
		}
		if err := cmd.Wait(); err != nil {
			w.recordErr(fmt.Errorf("wait for persistent psql: %w", err))
		}
		close(w.reaped)
		w.readFinalMarker(marker, pod)
	}()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for n := 1; ; n++ {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
		}

		statement := fmt.Sprintf(
			"INSERT INTO orders (customer_id, amount, note) VALUES (1, 1.00, '%s-%d');\n",
			marker, n,
		)
		written, err := io.WriteString(stdin, statement)
		if err != nil || written != len(statement) {
			if err == nil {
				err = io.ErrShortWrite
			}
			w.recordErr(fmt.Errorf("write live marker %d: %w", n, err))
			return
		}
	}
}

// stop ends the stream and returns the captured source pod's final marker.
// Repeated calls share the first call's completed lifecycle and first error.
func (w *liveWriter) stop() (int, error) {
	w.stopOnce.Do(func() {
		close(w.stopCh)
		timer := time.NewTimer(w.timeout)
		defer timer.Stop()
		select {
		case <-w.reaped:
		case <-w.done:
		case <-timer.C:
			w.recordErr(fmt.Errorf(
				"stop persistent psql for cluster %s on pod %s timed out after %s: %w",
				sourceCluster, w.pod, w.timeout, context.DeadlineExceeded,
			))
			w.cancel()
			select {
			case <-w.reaped:
			case <-w.done:
			}
		}
	})
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last, w.err
}

// psqlDiag runs a diagnostic query that must never fail a spec itself: it is
// called from failure messages, where a second assertion would replace the
// diagnosis with its own error. Returns the error text in place of the value.
func psqlDiag(cluster, sql string) string {
	out, err := psqlDBErr(cluster, appDB, sql)
	if err != nil {
		return "(query failed: " + err.Error() + ")"
	}
	return out
}

// waitFailed waits until the Migration in nsE2E is terminally Failed with the
// given Failed-condition reason and returns it. Counterpart to waitPhase,
// which treats Failed as a hard stop.
func waitFailed(name, reason string) *v1beta1.Migration {
	GinkgoHelper()
	m := &v1beta1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
		g.Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed),
			"migration %s at phase %q, attempts %d", name, m.Status.Phase, m.Status.Attempts)
		c := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		g.Expect(c).NotTo(BeNil(), "Failed condition missing on %s", name)
		g.Expect(c.Reason).To(Equal(reason), "Failed reason is %q, message: %s", c.Reason, c.Message)
	}, migrationTimeout, 2*time.Second).Should(Succeed())
	return m
}

// deletePod deletes every pod matching the label selector in nsE2E with a
// 1-second grace period (chaos kills want the process gone, not drained) and
// fails when nothing matched: a kill that found no victim means the scenario
// mistimed its window, not that the operator survived it.
func deletePod(selector client.MatchingLabels) {
	GinkgoHelper()
	pods := &corev1.PodList{}
	Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E), selector)).To(Succeed())
	Expect(pods.Items).NotTo(BeEmpty(), "no pod matches %v", selector)
	for i := range pods.Items {
		err := k8sClient.Delete(ctx, &pods.Items[i], client.GracePeriodSeconds(1))
		if err != nil && !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred(), "failed to delete pod %s", pods.Items[i].Name)
		}
	}
}

// deleteMigration deletes the named Migration in nsE2E and waits until the
// finalizer released it (deletion of a live migration runs slot cleanup
// first), so the next scenario starts without leftovers.
// purgeMigrations deletes every Migration in both fixture namespaces and
// waits for the finalizers to release. Callers must ensure an operator is
// still running: without one the wait can only time out.
func purgeMigrations(timeout time.Duration) {
	GinkgoHelper()
	for _, ns := range []string{nsE2E, nsX} {
		Expect(k8sClient.DeleteAllOf(ctx, &v1beta1.Migration{}, client.InNamespace(ns))).To(Succeed())
	}
	Eventually(func(g Gomega) {
		for _, ns := range []string{nsE2E, nsX} {
			list := &v1beta1.MigrationList{}
			g.Expect(k8sClient.List(ctx, list, client.InNamespace(ns))).To(Succeed())
			g.Expect(list.Items).To(BeEmpty(), "Migrations still terminating in %s", ns)
		}
	}, timeout, 2*time.Second).Should(Succeed())
}

func deleteMigration(name string) {
	GinkgoHelper()
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: name}}
	if err := k8sClient.Delete(ctx, m); err != nil && !apierrors.IsNotFound(err) {
		Expect(err).NotTo(HaveOccurred(), "failed to delete Migration %s", name)
	}
	Eventually(func(g Gomega) {
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, &v1beta1.Migration{})
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "Migration %s still terminating", name)
	}, 5*time.Minute, 2*time.Second).Should(Succeed())
}

var transientExecErrorPatterns = []*regexp.Regexp{
	regexp.MustCompile(`^Unable to connect to the server: net/http: TLS handshake timeout$`),
	regexp.MustCompile(`^The connection to the server \S+ was refused - did you specify the right host or port\?$`),
	regexp.MustCompile(`^error: Internal error occurred: error dialing backend: \S(?:[^\r\n]*\S)?$`),
	regexp.MustCompile(`^error: error sending request: Post "[^"\s]+": net/http: TLS handshake timeout$`),
	regexp.MustCompile(`^error: error sending request: Post "[^"\s]+": dial tcp \S+: connect: connection refused$`),
}

// transientExecError reports a structured kubectl failure that occurred
// before the remote command started, so arbitrary SQL is safe to retry.
func transientExecError(stderr string) bool {
	if trimmed, ok := strings.CutSuffix(stderr, "\n"); ok {
		stderr = trimmed
		stderr = strings.TrimSuffix(stderr, "\r")
	}
	if strings.ContainsAny(stderr, "\r\n") {
		return false
	}
	for _, pattern := range transientExecErrorPatterns {
		if pattern.MatchString(stderr) {
			return true
		}
	}
	return false
}
