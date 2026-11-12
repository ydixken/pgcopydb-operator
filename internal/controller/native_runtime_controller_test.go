package controller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"
)

const (
	nativeControllerPhase     = "controller"
	nativeControllerSelector  = "RPGMIGRATE_NATIVE_CONTROLLER_INPUT"
	nativeControllerMigration = "native-controller"
	nativeControllerCRDName   = "migrations.pgcopydb-operator.io"
	nativeControllerCRDKind   = "CustomResourceDefinition"
	nativeControllerCRDAPI    = "apiextensions.k8s.io/v1"
	nativeControllerKind      = "Migration"
	nativeControllerVersion   = "rpgmigrate-version-"
	nativeControllerPreflight = "rpgmigrate-preflight-"
	nativeControllerCAKey     = "ca.crt"
	nativeControllerFixture   = "controller-fixture"
)

// The CRD the guard bootstrap would apply; a pulled run has none, so the phase owns its own copy.
var nativeControllerCRDPath = filepath.Join("..", "..", "config", "crd", "bases", "pgcopydb-operator.io_migrations.yaml")

var nativeControllerOrder = []string{"version_job_created", "preflight_job_created", "preflight_consumed", "helpers_disposed", "no_worker_started"}

func nativeControllerCases() []string {
	cases := make([]string, 0, len(nativeControllerOrder))
	for _, name := range nativeControllerOrder {
		cases = append(cases, nativeControllerPhase+"/"+name)
	}
	return cases
}

type nativeControllerInput struct {
	SchemaVersion  int                `json:"schema_version"`
	Runtime        nativeRuntimeInput `json:"runtime"`
	ParentDeadline float64            `json:"original_parent_deadline"`
}

type nativeControllerJob struct {
	Name  string    `json:"name"`
	UID   types.UID `json:"uid"`
	Bytes int       `json:"bytes"`
	Image string    `json:"image"`
}

type nativeControllerValidated struct {
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type nativeControllerReceipt struct {
	SchemaVersion   int                       `json:"schema_version"`
	Kind            string                    `json:"kind"`
	QualificationID string                    `json:"qualification_id"`
	RunID           string                    `json:"run_id"`
	InputSHA        string                    `json:"input_sha256"`
	SourceSHA       string                    `json:"qualification_source_sha256"`
	EvidenceSHA     string                    `json:"evidence_program_sha256"`
	AssetsSHA       string                    `json:"native_assets_sha256"`
	CRDSHA          string                    `json:"crd_sha256"`
	NamespaceUID    types.UID                 `json:"namespace_uid"`
	MigrationUID    types.UID                 `json:"migration_uid"`
	StartedAt       float64                   `json:"started_at"`
	FinishedAt      float64                   `json:"finished_at"`
	Passes          int                       `json:"reconcile_passes"`
	Validated       nativeControllerValidated `json:"validated"`
	Jobs            []nativeControllerJob     `json:"jobs"`
	Refused         []string                  `json:"refused_creates"`
	Cases           []nativeRuntimeCase       `json:"cases"`
	Cleanup         struct {
		Complete bool                    `json:"complete"`
		Objects  []nativePasswordCleanup `json:"objects"`
	} `json:"cleanup"`
}

// nativeControllerClient records the reconciler's Creates and refuses anything past the preflight helpers,
// so the probe can never start a worker or claim a work PVC.
type nativeControllerClient struct {
	client.Client
	jobs    *[]nativeControllerJob
	refused *[]string
}

func (c nativeControllerClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	job, isJob := obj.(*batchv1.Job)
	if !isJob || !strings.HasPrefix(job.Name, nativeControllerVersion) && !strings.HasPrefix(job.Name, nativeControllerPreflight) {
		*c.refused = append(*c.refused, fmt.Sprintf("%T/%s", obj, obj.GetName()))
		return errors.New("controller probe refuses a Create past the preflight helpers")
	}
	raw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	*c.jobs = append(*c.jobs, nativeControllerJob{Name: job.Name, UID: job.UID, Bytes: len(raw), Image: job.Spec.Template.Spec.Containers[0].Image})
	return nil
}

func nativeControllerValidateInput(input nativeControllerInput, path string, now time.Time) error {
	r := input.Runtime
	current := float64(now.UnixNano()) / 1e9
	evidence := native.EvidenceProgramSHA256()
	if input.SchemaVersion != 1 || r.SchemaVersion != 1 || r.Phase != nativeControllerPhase || r.Pair != nil || r.NamespaceUID != nil || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(r.QualificationID) || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(r.RunID) || r.Namespace != "native-controller-"+r.QualificationID[:16] || r.ReceiptPath != filepath.Join(filepath.Dir(path), "controller-receipt.json") || r.Kubeconfig != filepath.Join(r.RunDir, "kubeconfig") || r.Context != "kind-rpgmigrate-m7-"+r.RunID || !slices.Equal(r.RequiredCases, nativeControllerCases()) || len(r.Nodes) != 3 {
		return errors.New("fixed controller input refused")
	}
	for _, digest := range []string{r.LegacySourceSHA, r.LegacyGuardSHA, r.LegacyOwnershipSHA, r.QualificationSourceSHA, r.AssetsSHA, r.MetadataProgramSHA} {
		if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(digest) {
			return errors.New("controller source binding absent")
		}
	}
	if r.EvidenceProgramSHA != hex.EncodeToString(evidence[:]) || r.MetadataProgramSHA != nativeRuntimeSHA([]byte(nativeRuntimeMetadataProgram+"\x00"+nativeRuntimeNodeProgram)) || !(current < r.StageDeadline && r.StageDeadline <= current+600 && r.StageDeadline+600 <= min(r.LeaseExpiry, input.ParentDeadline)) {
		return errors.New("controller original source or deadline changed")
	}
	previous := ""
	for _, node := range r.Nodes {
		if node.Name <= previous || node.UID == "" || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(node.ContainerID) {
			return errors.New("controller node identity refused")
		}
		previous = node.Name
	}
	return nil
}

// nativeControllerMigrationFor is the M6/M7 vertical slice: one database, plain COPY, no split, no large objects.
func nativeControllerMigrationFor(namespace, image string) *v1beta1.Migration {
	side := func(major int, secret string) v1beta1.PostgresConnection {
		return v1beta1.PostgresConnection{Host: fmt.Sprintf("pg%d.%s.svc.cluster.local", major, namespace), Port: 5432, Database: "postgres", Username: "native_probe", SSLMode: "verify-full",
			PasswordSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: testPasswordKey},
			TLS:               &v1beta1.TLSSecretRefs{RootCA: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: nativeControllerCAKey}}}
	}
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: nativeControllerMigration, Namespace: namespace}, Spec: v1beta1.MigrationSpec{Engine: v1beta1.MigrationEngineRPGMigrate, Source: side(16, "source-auth"), Target: side(18, "target-auth")}}
	m.Spec.Runner.Image = image
	m.Spec.Clone = v1beta1.CloneOptions{UseCopyBinary: ptr.To(false), SplitTablesLargerThan: ptr.To(resource.MustParse("0")), Skip: []v1beta1.SkipOption{"largeObjects"}}
	return m
}

func nativeControllerCRD(runID string) (*unstructured.Unstructured, []byte, error) {
	raw, err := os.ReadFile(nativeControllerCRDPath)
	if err != nil || len(raw) > nativeRuntimeFileLimit {
		return nil, nil, errors.New("assigned Migration CRD unavailable")
	}
	crd := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(raw, &crd.Object); err != nil || crd.GetAPIVersion() != nativeControllerCRDAPI || crd.GetKind() != nativeControllerCRDKind || crd.GetName() != nativeControllerCRDName {
		return nil, nil, errors.New("assigned Migration CRD identity refused")
	}
	crd.SetLabels(map[string]string{nativeRuntimeRunLabel: runID})
	return crd, raw, nil
}

func nativeControllerEstablished(ctx context.Context, c client.Client, created *unstructured.Unstructured) error {
	return wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(call context.Context) (bool, error) {
		live := &unstructured.Unstructured{}
		live.SetAPIVersion(nativeControllerCRDAPI)
		live.SetKind(nativeControllerCRDKind)
		if err := c.Get(call, client.ObjectKey{Name: nativeControllerCRDName}, live); err != nil || live.GetUID() != created.GetUID() {
			return false, errors.New("owned CRD identity changed")
		}
		conditions, _, _ := unstructured.NestedSlice(live.Object, "status", "conditions")
		for _, item := range conditions {
			if condition, ok := item.(map[string]any); ok && condition["type"] == "Established" && condition["status"] == "True" {
				return true, nil
			}
		}
		return false, nil
	})
}

// nativeControllerRemove deletes a controller-runtime object under its created UID and proves absence.
func nativeControllerRemove(ctx context.Context, c client.Client, object client.Object, kind, api string) nativePasswordCleanup {
	result := nativePasswordCleanup{APIVersion: api, Kind: kind, Namespace: object.GetNamespace(), Name: object.GetName(), UID: object.GetUID(), ResourceVersion: object.GetResourceVersion(), Action: nativeRuntimeRetained}
	err := wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(call context.Context) (bool, error) {
		live := object.DeepCopyObject().(client.Object)
		err := c.Get(call, client.ObjectKeyFromObject(object), live)
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			result.VerifiedAbsent = true
			return true, nil
		}
		if err != nil || live.GetUID() != object.GetUID() {
			return false, errors.New("cleanup identity changed")
		}
		result.ResourceVersion = live.GetResourceVersion()
		err = c.Delete(call, live, client.Preconditions{UID: ptr.To(live.GetUID())}, client.PropagationPolicy(metav1.DeletePropagationForeground))
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return false, errors.New("cleanup delete refused")
		}
		return false, nil
	})
	if err == nil && result.VerifiedAbsent {
		result.Action = nativeRuntimeDeleted
	}
	return result
}

// The controller deletes its helpers after reading them; their Jobs and Pods must be gone, and no worker or claim exist.
func nativeControllerDisposed(ctx context.Context, clientset kubernetes.Interface, namespace string, jobs []nativeControllerJob) bool {
	err := wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(call context.Context) (bool, error) {
		for _, job := range jobs {
			_, err := clientset.BatchV1().Jobs(namespace).Get(call, job.Name, metav1.GetOptions{})
			pods, listErr := clientset.CoreV1().Pods(namespace).List(call, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/controller-uid=" + string(job.UID)})
			if listErr != nil || err != nil && !apierrors.IsNotFound(err) {
				return false, errors.New("helper inventory unavailable")
			}
			if err == nil || len(pods.Items) != 0 {
				return false, nil
			}
		}
		return true, nil
	})
	return err == nil
}

func nativeControllerNoWorker(ctx context.Context, clientset kubernetes.Interface, namespace string, recorded []nativeControllerJob) bool {
	jobs, jobErr := clientset.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	claims, claimErr := clientset.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	return jobErr == nil && claimErr == nil && len(jobs.Items) == 0 && len(claims.Items) == 0 && len(recorded) == 2
}

func nativeControllerResult(validated *metav1.Condition, jobs []nativeControllerJob, image string) map[string]bool {
	op := ""
	if len(jobs) == 2 {
		op = strings.TrimPrefix(jobs[0].Name, nativeControllerVersion)
	}
	return map[string]bool{
		"version_job_created":   len(jobs) == 2 && regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(op) && jobs[0].UID != "" && jobs[0].Image == image,
		"preflight_job_created": len(jobs) == 2 && jobs[1].Name == nativeControllerPreflight+op && jobs[1].UID != "" && jobs[1].Image == image && jobs[1].Bytes > len(native.EvidenceProgram()),
		"preflight_consumed":    validated != nil && validated.Status == metav1.ConditionTrue && validated.Reason == "NativePreflightPassed",
	}
}

type nativeControllerRun struct {
	input     nativeRuntimeInput
	clientset kubernetes.Interface
	direct    client.Client
	receipt   *nativeControllerReceipt
	owned     []nativeRuntimeOwned
	crd       client.Object
	migration client.Object
	unknown   bool
	scan      *nativePasswordCanaries
}

func (c *nativeControllerRun) setup(work context.Context, crd *unstructured.Unstructured, servers map[int]nativeRuntimeImage, image string) error {
	r := c.input
	probe := &unstructured.Unstructured{}
	probe.SetAPIVersion(nativeControllerCRDAPI)
	probe.SetKind(nativeControllerCRDKind)
	if err := c.direct.Get(work, client.ObjectKey{Name: nativeControllerCRDName}, probe); !apierrors.IsNotFound(err) {
		return errors.New("fresh Migration CRD absence required")
	}
	if err := c.direct.Create(work, crd); err != nil || crd.GetUID() == "" {
		c.unknown = true
		return errors.New("migration CRD Create unknown")
	}
	c.crd = crd
	if err := nativeControllerEstablished(work, c.direct, crd); err != nil {
		return errors.New("owned Migration CRD not established")
	}
	if _, err := c.clientset.CoreV1().Namespaces().Get(work, r.Namespace, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return errors.New("fresh namespace absence required")
	}
	namespace, err := c.clientset.CoreV1().Namespaces().Create(work, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.Namespace, Labels: map[string]string{nativeRuntimeRunLabel: r.RunID, nativePasswordQualificationLabel: r.QualificationID}}}, metav1.CreateOptions{})
	if err != nil || namespace.UID == "" {
		c.unknown = true
		return errors.New("namespace Create unknown")
	}
	c.owned = append(c.owned, nativeRuntimeOwned{object: namespace, kind: nativeRuntimeNamespaceKind})
	c.receipt.NamespaceUID = namespace.UID
	for index, side := range []struct {
		major  int
		secret string
	}{{16, "source-auth"}, {18, "target-auth"}} {
		fixture, err := nativePasswordPrepareServer(work, c.clientset, r, r.Nodes[index], servers[side.major], side.major, &c.owned, &c.unknown, c.scan)
		if err != nil {
			return fmt.Errorf("controller probe server refused: %w", err)
		}
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: side.secret, Namespace: r.Namespace}, Immutable: ptr.To(true), Data: map[string][]byte{testPasswordKey: []byte(fixture.keys.password), nativeControllerCAKey: fixture.keys.ca}}
		created, err := c.clientset.CoreV1().Secrets(r.Namespace).Create(work, secret, metav1.CreateOptions{})
		if err != nil || created.UID == "" {
			c.unknown = true
			return errors.New("migration Secret Create unknown")
		}
		c.owned = append(c.owned, nativeRuntimeOwned{object: created, kind: nativePasswordSecretKind})
	}
	m := nativeControllerMigrationFor(r.Namespace, image)
	if err := c.direct.Create(work, m); err != nil || m.UID == "" {
		c.unknown = true
		return errors.New("migration Create unknown")
	}
	c.migration = m
	c.receipt.MigrationUID = m.UID
	return nil
}

// reconcile runs the real reconciler: Pending, then the pass that runs the version and preflight helpers.
func (c *nativeControllerRun) reconcile(work context.Context, reconciler *MigrationReconciler) (*metav1.Condition, error) {
	for c.receipt.Passes < 4 {
		c.receipt.Passes++
		_, _ = reconciler.Reconcile(work, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(c.migration)})
		live := &v1beta1.Migration{}
		if err := c.direct.Get(work, client.ObjectKeyFromObject(c.migration), live); err != nil || live.UID != c.migration.GetUID() {
			return nil, errors.New("migration identity changed during reconciliation")
		}
		if condition := meta.FindStatusCondition(live.Status.Conditions, v1beta1.ConditionValidated); condition != nil && condition.Status != metav1.ConditionUnknown {
			c.receipt.Validated = nativeControllerValidated{Status: string(condition.Status), Reason: condition.Reason, Message: condition.Message}
			return condition, nil
		}
	}
	return nil, nil
}

func (c *nativeControllerRun) finish(cleanup context.Context) {
	blocked := c.unknown
	record := func(result nativePasswordCleanup) {
		c.receipt.Cleanup.Objects = append(c.receipt.Cleanup.Objects, result)
		blocked = blocked || !result.VerifiedAbsent
	}
	if c.migration != nil {
		record(nativeControllerRemove(cleanup, c.direct, c.migration, nativeControllerKind, v1beta1.GroupVersion.String()))
	}
	for _, kind := range []string{nativeRuntimePodKind, nativePasswordServiceKind, nativePasswordSecretKind, nativeRuntimeNamespaceKind} {
		for _, object := range c.owned {
			if object.kind != kind {
				continue
			}
			if blocked && kind == nativeRuntimeNamespaceKind {
				c.receipt.Cleanup.Objects = append(c.receipt.Cleanup.Objects, nativePasswordCleanup{APIVersion: "v1", Kind: kind, Name: object.object.GetName(), UID: object.object.GetUID(), ResourceVersion: object.object.GetResourceVersion(), Action: nativeRuntimeRetained})
				continue
			}
			record(nativePasswordRemove(cleanup, c.clientset, object))
		}
	}
	if c.crd != nil {
		record(nativeControllerRemove(cleanup, c.direct, c.crd, nativeControllerCRDKind, nativeControllerCRDAPI))
	}
	// The CRD, the Migration, two server Pods, Services and Secrets, two Migration Secrets and the namespace.
	c.receipt.Cleanup.Complete = !blocked && len(c.owned) == 9 && len(c.receipt.Cleanup.Objects) == 11
	for index := range c.receipt.Cases {
		c.receipt.Cases[index].Cleanup = c.receipt.Cleanup.Complete
	}
	c.receipt.FinishedAt = float64(time.Now().UnixNano()) / 1e9
}

func TestNativePackagedControllerCreate(t *testing.T) {
	path, selected := os.LookupEnv(nativeControllerSelector)
	if !selected {
		t.Skip("controller Create qualification is not selected")
	}
	raw, err := nativeRuntimePrivate(path)
	if err != nil {
		t.Fatal("private controller input unavailable")
	}
	var input nativeControllerInput
	var fields map[string]json.RawMessage
	if nativeRuntimeDecode(raw, &input) != nil || json.Unmarshal(raw, &fields) != nil || nativeRuntimeDecode(fields["runtime"], &input.Runtime) != nil || nativeControllerValidateInput(input, path, time.Now()) != nil {
		t.Fatal("fixed controller input refused")
	}
	r := input.Runtime
	config, err := nativeRuntimeConfig(r)
	if err != nil {
		t.Fatal("owned client unavailable")
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal("owned client unavailable")
	}
	scheme := runtime.NewScheme()
	if clientgoscheme.AddToScheme(scheme) != nil || v1beta1.AddToScheme(scheme) != nil {
		t.Fatal("controller scheme unavailable")
	}
	direct, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal("controller client unavailable")
	}
	exec, err := podexec.New(config)
	if err != nil {
		t.Fatal("checked transport unavailable")
	}
	image, err := nativeRuntimeAssets(path, r)
	if err != nil {
		t.Fatal("native image source unavailable")
	}
	servers, err := nativePasswordImages(path, r)
	if err != nil {
		t.Fatal("pinned server images unavailable")
	}
	crd, crdRaw, err := nativeControllerCRD(r.RunID)
	if err != nil {
		t.Fatal(err)
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
	receipt := &nativeControllerReceipt{SchemaVersion: 1, Kind: nativeControllerPhase, QualificationID: r.QualificationID, RunID: r.RunID, InputSHA: nativeRuntimeSHA(raw), SourceSHA: r.QualificationSourceSHA, EvidenceSHA: r.EvidenceProgramSHA, AssetsSHA: r.AssetsSHA, CRDSHA: nativeRuntimeSHA(crdRaw), StartedAt: float64(time.Now().UnixNano()) / 1e9, Jobs: []nativeControllerJob{}, Refused: []string{}, Cases: []nativeRuntimeCase{}}
	run := &nativeControllerRun{input: r, clientset: clientset, direct: direct, receipt: receipt, owned: make([]nativeRuntimeOwned, 0, 9), scan: &nativePasswordCanaries{}}
	defer func() {
		cleanup, end := context.WithDeadline(context.Background(), cleanupDeadline)
		defer end()
		run.finish(cleanup)
		encoded, err := json.Marshal(receipt)
		if err != nil || run.scan.check(encoded) != nil || nativeRuntimeWrite(r.ReceiptPath, receipt) != nil {
			t.Error("controller receipt persistence refused")
		}
		if !receipt.Cleanup.Complete {
			t.Error("exact controller cleanup incomplete")
		}
	}()
	if err := run.setup(work, crd, servers, image.Ref); err != nil {
		t.Fatal(err)
	}
	recording := nativeControllerClient{Client: direct, jobs: &receipt.Jobs, refused: &receipt.Refused}
	reconciler := &MigrationReconciler{Client: recording, APIReader: direct, Scheme: scheme, Recorder: events.NewFakeRecorder(100), RPGMigrateRunnerImage: image.Ref,
		NativePreflight: exec, NativeStop: exec, NativeEvidence: native.NewReader(direct, exec), NativeControl: native.NewClient(exec), NativeJobs: exec, Sentinel: sentinel.New(exec), Logs: exec}
	validated, err := run.reconcile(work, reconciler)
	if err != nil {
		t.Fatal(err)
	}
	passed := nativeControllerResult(validated, receipt.Jobs, image.Ref)
	passed["helpers_disposed"] = len(receipt.Jobs) == 2 && nativeControllerDisposed(work, clientset, r.Namespace, receipt.Jobs)
	passed["no_worker_started"] = len(receipt.Refused) == 0 && nativeControllerNoWorker(work, clientset, r.Namespace, receipt.Jobs)
	for _, caseName := range nativeControllerCases() {
		name := strings.TrimPrefix(caseName, nativeControllerPhase+"/")
		ok := t.Run(caseName, func(t *testing.T) {
			if !passed[name] {
				t.Fatal("actual controller case absent")
			}
		})
		status, exit := nativeRuntimeRefused, 1
		if ok {
			status, exit = nativeRuntimePassed, 0
		}
		receipt.Cases = append(receipt.Cases, nativeRuntimeCase{Name: caseName, Status: status, ExitCode: exit})
	}
}

func TestNativeControllerProbeRefusesCreatesPastThePreflightHelpers(t *testing.T) {
	scheme := runtime.NewScheme()
	if clientgoscheme.AddToScheme(scheme) != nil {
		t.Fatal("scheme unavailable")
	}
	var jobs []nativeControllerJob
	var refused []string
	c := nativeControllerClient{Client: clientfake.NewClientBuilder().WithScheme(scheme).Build(), jobs: &jobs, refused: &refused}
	helper := func(name string) *batchv1.Job {
		return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: nativeControllerFixture}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: workerContainer, Image: nativeControllerFixture}}}}}}
	}
	if c.Create(context.Background(), helper(nativeControllerVersion+strings.Repeat("a", 32))) != nil || c.Create(context.Background(), helper(nativeControllerPreflight+strings.Repeat("a", 32))) != nil || len(jobs) != 2 {
		t.Fatal("the preflight helpers were not admitted and recorded")
	}
	for _, object := range []client.Object{helper("native-controller-run-1"), &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: nativeControlWorkKey, Namespace: nativeControllerFixture}}} {
		if c.Create(context.Background(), object) == nil {
			t.Fatal("a Create past the preflight helpers was admitted", object.GetName())
		}
	}
	if len(refused) != 2 || len(jobs) != 2 {
		t.Fatal("refused Creates were not recorded")
	}
}

func TestNativeControllerResultRequiresTheExactHelperPair(t *testing.T) {
	op, image := strings.Repeat("b", 32), "ghcr.io/ydixken/rpgmigrate@sha256:"+strings.Repeat("c", 64)
	jobs := []nativeControllerJob{{Name: nativeControllerVersion + op, UID: "version-uid", Bytes: 4096, Image: image}, {Name: nativeControllerPreflight + op, UID: "preflight-uid", Bytes: len(native.EvidenceProgram()) + 4096, Image: image}}
	passed := &metav1.Condition{Status: metav1.ConditionTrue, Reason: "NativePreflightPassed"}
	if result := nativeControllerResult(passed, jobs, image); !result["version_job_created"] || !result["preflight_job_created"] || !result["preflight_consumed"] {
		t.Fatal("the exact helper pair was refused", result)
	}
	for name, mutate := range map[string]func([]nativeControllerJob, *metav1.Condition){
		"other_operation": func(j []nativeControllerJob, _ *metav1.Condition) {
			j[1].Name = nativeControllerPreflight + strings.Repeat("d", 32)
		},
		"other_image": func(j []nativeControllerJob, _ *metav1.Condition) { j[1].Image = workerForeignIdentity },
		"no_program":  func(j []nativeControllerJob, _ *metav1.Condition) { j[1].Bytes = 4096 },
		"refused": func(_ []nativeControllerJob, c *metav1.Condition) {
			c.Status, c.Reason = metav1.ConditionFalse, "NativePreflightFailed"
		},
	} {
		changed, condition := slices.Clone(jobs), *passed
		mutate(changed, &condition)
		result := nativeControllerResult(&condition, changed, image)
		if result["version_job_created"] && result["preflight_job_created"] && result["preflight_consumed"] {
			t.Fatal("a changed helper pair or refused preflight passed", name)
		}
	}
}

// The fixture Migration must pass the same admission and builders the controller runs before any Create.
func TestNativeControllerMigrationIsTheQualifiedSlice(t *testing.T) {
	m := nativeControllerMigrationFor(nativeControllerFixture, qualifiedRunnerARM64)
	m.UID = "fixture-uid"
	if !nativeRunnerQualified(runnerImageFor(m, "")) {
		t.Fatal("the fixture runner image is not a qualified digest")
	}
	if _, err := nativeConfigCapabilities(m); err != nil {
		t.Fatal("the fixture Migration is outside the qualified native slice", err)
	}
	op := strings.Repeat("e", 32)
	if _, err := buildNativeVersionJob(m, "", nativeControllerVersion+op); err != nil {
		t.Fatal("the version helper refused the fixture", err)
	}
	if _, err := buildNativePreflightJob(m, "", nativeControllerPreflight+op, op); err != nil {
		t.Fatal("the preflight helper refused the fixture", err)
	}
}
