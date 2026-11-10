package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const nativeCleanupTestName = "rpgmigrate-cleanup-0123456789abcdef0123456789abcdef"
const nativeCleanupTestOrigin = "recorded_origin"
const nativeCleanupTestJSONArg = "--json"
const nativeCleanupTestDirArg = "--dir"
const nativeCleanupTestWorkdir = "/work/pgcopydb"
const nativeCleanupTestStreamArg = "stream"
const nativeCleanupTestProtocol = "v5.channel.k8s.io"
const nativeCleanupTestPrivateCanary = "private-canary"
const nativeCleanupTestOtherIdentity = "cleanup-other-identity"

type cleanupInventoryFixture struct {
	*lifecycleAPI
	r    *MigrationReconciler
	want native.ExecutionData
	edit func(map[string]any)
}

// This fixture models Reader admission; worker-builder parity and runtime qualification are separate gates.
func newCleanupInventoryFixture(t *testing.T) *cleanupInventoryFixture {
	t.Helper()
	s, r := newLifecycleAPI(t)
	s.migration.Status.Replication = nil
	work, err := nativeWorkIdentity(s.migration, s.pvc)
	if err != nil {
		t.Fatal(err)
	}
	w := native.ExecutionData{Namespace: s.migration.Namespace, MigrationName: s.migration.Name, MigrationUID: s.migration.UID, MigrationGeneration: s.migration.Generation, PVCName: s.pvc.Name, PVCUID: s.pvc.UID, JobName: "rpgmigrate-inventory-" + nativeTestRunID, JobUID: "inventory-helper", WorkerJobUID: s.worker.UID, Attempt: 1, Role: native.JobRoleCleanup, Work: work, ImageRef: r.RPGMigrateRunnerImage, PlatformImageID: r.RPGMigrateRunnerImage, OperationID: nativeTestRunID, ProgramSHA256: native.OwnedInventoryProgramSHA256()}
	job, err := buildNativeVersionJob(s.migration, r.RPGMigrateRunnerImage, "rpgmigrate-version-"+w.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	command, args, env, volumes, mounts, err := native.OwnedInventoryMaterialization(s.migration, w)
	if err != nil {
		t.Fatal(err)
	}
	job.Name, job.UID, job.OwnerReferences = w.JobName, w.JobUID, s.pvc.OwnerReferences
	job.Spec.Template.Spec.Volumes = volumes
	c := &job.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
	s.job = job
	s.worker.Spec = *job.Spec.DeepCopy()
	wc := &s.worker.Spec.Template.Spec.Containers[0]
	wc.Command = []string{shellPath, "-c", "exec \"$0\" \"$@\"", nativeWorkerTestExecutable}
	wc.Args = []string{nativeTerminalCloneCommand, "--follow", "--plugin", pgoutputPlugin}
	s.worker.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
	controller := true
	s.pod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: lifecycleHelperPod, Namespace: w.Namespace, UID: "inventory-helper-pod", Labels: map[string]string{batchv1.ControllerUidLabel: string(w.JobUID)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: lifecycleJobKind, Name: w.JobName, UID: w.JobUID, Controller: &controller}}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer, ContainerID: "containerd://inventory-helper", Image: w.ImageRef, ImageID: w.PlatformImageID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	workerPod := s.pod.DeepCopy()
	workerPod.Name, workerPod.UID = "retained-worker-pod", "retained-worker-pod-uid"
	workerPod.OwnerReferences[0].Name, workerPod.OwnerReferences[0].UID = s.worker.Name, s.worker.UID
	workerPod.Spec = *s.worker.Spec.Template.Spec.DeepCopy()
	workerPod.Status.Phase = corev1.PodFailed
	status := &workerPod.Status.ContainerStatuses[0]
	status.ContainerID = "containerd://retained-worker"
	status.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, ContainerID: status.ContainerID}}
	s.extraPods = []corev1.Pod{*workerPod}
	w.TemplateSHA256 = podexec.NativeTemplateSHA256(job.Spec.Template)
	w.ConnectionReferenceSHA256, err = native.OwnedInventoryConnectionReferenceSHA256(s.migration, w)
	if err != nil {
		t.Fatal(err)
	}
	w.PodName = s.pod.Name
	w.Container = podexec.NativeContainerBinding{PodUID: s.pod.UID, ContainerName: workerContainer, ContainerID: s.pod.Status.ContainerStatuses[0].ContainerID, ImageID: w.PlatformImageID, TemplateSHA256: w.TemplateSHA256, ExecutionSHA256: podexec.NativeExecutionSHA256(job.Spec.Template)}
	f := &cleanupInventoryFixture{lifecycleAPI: s, r: r, want: w}
	s.Config.Handler = http.HandlerFunc(f.serve)
	return f
}

func (f *cleanupInventoryFixture) serve(out http.ResponseWriter, request *http.Request) {
	if strings.HasSuffix(request.URL.Path, "/exec") {
		f.execInventory(out, request)
		return
	}
	if strings.HasSuffix(request.URL.Path, "/pods") || strings.HasSuffix(request.URL.Path, "/pods/retained-worker-pod") {
		f.mu.Lock()
		defer f.mu.Unlock()
		out.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(request.URL.Path, "/pods") {
			pods := []corev1.Pod{*f.pod.DeepCopy()}
			if request.URL.Query().Get("labelSelector") == "" {
				pods = append(pods, f.extraPods...)
			}
			_ = json.NewEncoder(out).Encode(corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: pods})
		} else {
			_ = json.NewEncoder(out).Encode(f.extraPods[0])
		}
		return
	}
	f.lifecycleAPI.serve(out, request)
}

func (f *cleanupInventoryFixture) execInventory(out http.ResponseWriter, request *http.Request) {
	upgrade := websocket.Upgrader{Subprotocols: []string{nativeCleanupTestProtocol}}
	ws, err := upgrade.Upgrade(out, request, nil)
	if err != nil {
		return
	}
	defer func() { _ = ws.Close() }()
	var input []byte
	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if bytes.Equal(raw, []byte{255, 0}) {
			break
		}
		if len(raw) == 0 || raw[0] != 0 {
			return
		}
		input = append(input, raw[1:]...)
	}
	var value map[string]any
	if err := json.Unmarshal(lifecycleResult(f.t, input), &value); err != nil {
		f.t.Error(err)
		return
	}
	delete(value, "checkpoint")
	delete(value, "guards")
	value[lifecycleKindKey], value["materialization_closed"] = "owned_inventory", true
	f.mu.Lock()
	f.relays++
	f.injected = true
	f.inputs = append(f.inputs, bytes.Clone(input))
	if f.edit != nil {
		f.edit(value)
	}
	f.mu.Unlock()
	result, err := json.Marshal(value)
	if err != nil {
		f.t.Error(err)
		return
	}
	_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{1}, append(result, '\n')...))
	_ = ws.WriteMessage(websocket.BinaryMessage, []byte("\x03{\"status\":\"Success\"}"))
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

func (f *cleanupInventoryFixture) read(t *testing.T) (native.AdmittedOwnedState, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return f.r.NativeEvidence.ReadOwnedState(ctx, f.want)
}

func TestNativeCleanupArgs(t *testing.T) {
	for _, origin := range []bool{false, true} {
		t.Run(map[bool]string{false: "three_obligations", true: "unowned_origin"}[origin], func(t *testing.T) {
			f := newCleanupInventoryFixture(t)
			if origin {
				f.edit = func(v map[string]any) {
					v["resources"] = append(v["resources"].([]any), map[string]any{lifecycleKindKey: "replication_origin", lifecycleNameKey: nativeCleanupTestOrigin, lifecycleOwnedKey: false, lifecycleRetainKey: false})
				}
			}
			owned, err := f.read(t)
			if err != nil || f.relays != 1 || !f.logsRead {
				t.Fatalf("checked inventory refused: %v", err)
			}
			args, err := nativeCleanupArgs(f.want.Work, owned)
			if err != nil || !slices.Equal(args, []string{nativeCleanupTestStreamArg, "cleanup", nativeCleanupTestJSONArg, nativeCleanupTestDirArg, nativeCleanupTestWorkdir}) {
				t.Fatalf("admitted cleanup argv refused: %v", err)
			}
			for _, change := range []func(*native.WorkIdentity){func(w *native.WorkIdentity) { *w = native.WorkIdentity{} }, func(w *native.WorkIdentity) { w.MigrationUID = nativeCleanupTestOtherIdentity }, func(w *native.WorkIdentity) { w.RunID = nativeCleanupTestOtherIdentity }, func(w *native.WorkIdentity) { w.WorkdirID = nativeCleanupTestOtherIdentity }, func(w *native.WorkIdentity) { w.CheckpointGeneration++ }} {
				work := f.want.Work
				change(&work)
				if args, err := nativeCleanupArgs(work, owned); err == nil || args != nil {
					t.Fatal("foreign independent work authorized cleanup")
				}
			}
		})
	}
	for _, change := range []func(map[string]any){
		func(v map[string]any) { v["resources"] = []any{} },
		func(v map[string]any) { v["resources"] = v["resources"].([]any)[:2] },
		func(v map[string]any) { v["resources"] = append(v["resources"].([]any), v["resources"].([]any)[0]) },
		func(v map[string]any) { v["resources"].([]any)[0].(map[string]any)[lifecycleOwnedKey] = false },
		func(v map[string]any) { v["resources"].([]any)[1].(map[string]any)[lifecycleRetainKey] = true },
		func(v map[string]any) { v["resources"].([]any)[2].(map[string]any)[lifecycleNameKey] = "/work/foreign" },
	} {
		f := newCleanupInventoryFixture(t)
		f.edit = change
		owned, err := f.read(t)
		if err == nil || owned.Resources() != nil {
			t.Fatal("incomplete inventory acquired admission")
		}
		if args, err := nativeCleanupArgs(f.want.Work, owned); err == nil || args != nil {
			t.Fatal("unadmitted inventory authorized cleanup")
		}
	}
}

func TestNativeCleanupJob(t *testing.T) {
	f := newCleanupInventoryFixture(t)
	owned, err := f.read(t)
	if err != nil {
		t.Fatal(err)
	}
	m, pvc := f.migration, f.pvc
	before, beforePVC := m.DeepCopy(), pvc.DeepCopy()
	job, err := buildNativeCleanupJob(m, pvc, f.want.ImageRef, nativeCleanupTestName, owned)
	if err != nil {
		t.Fatal(err)
	}
	if refused, err := buildNativeCleanupJob(m, pvc, f.want.ImageRef, nativeCleanupTestName, native.AdmittedOwnedState{}); err == nil || refused != nil {
		t.Fatal("zero inventory produced a cleanup Job")
	}
	if !reflect.DeepEqual(before, m) || !reflect.DeepEqual(beforePVC, pvc) || job.UID != "" || len(job.OwnerReferences) != 0 || job.Name != nativeCleanupTestName || job.Namespace != m.Namespace {
		t.Fatal("cleanup builder changed input or claimed allocated identity")
	}
	assertNativeCleanupJobBounds(t, job)
	pod, c := job.Spec.Template.Spec, job.Spec.Template.Spec.Containers[0]
	assertNativeEvidencePodIsolation(t, pod)
	assertNativeEvidencePodSecurity(t, pod)
	assertNativeEvidenceContainerIsolation(t, c, f.want.ImageRef)
	assertNativeWorkerIdentity(t, c.Env, pvc.UID)
	assertNativeEndposMounts(t, pod, c)
	if pod.Volumes[0].PersistentVolumeClaim.ReadOnly || c.VolumeMounts[0].ReadOnly {
		t.Fatal("cleanup cannot persist its monotonic intent to the original work PVC")
	}
	if len(c.Command) != 4 || c.Command[0] != shellPath || c.Command[1] != "-c" || c.Command[3] != nativeWorkerTestExecutable || !slices.Equal(c.Args, []string{nativeCleanupTestStreamArg, "cleanup", nativeCleanupTestJSONArg, nativeCleanupTestDirArg, nativeCleanupTestWorkdir}) {
		t.Fatal("cleanup did not invoke only the engine's owned-resource command")
	}
	if strings.Contains(c.Command[2], "psql") || strings.Contains(c.Command[2], "DROP") || strings.Contains(strings.Join(c.Args, " "), "owned_") {
		t.Fatal("cleanup builder inferred SQL or resource operands")
	}
	if !slices.Contains(c.Env, corev1.EnvVar{Name: probeTestTimeoutEnvironment, Value: "10"}) || !slices.Contains(c.Env, corev1.EnvVar{Name: "LANG", Value: "C"}) || !slices.Contains(c.Env, corev1.EnvVar{Name: "LC_ALL", Value: "C"}) {
		t.Fatal("cleanup lost connection deadline or deterministic locale")
	}
	t.Run("both_connections", func(t *testing.T) { cleanupJobConnections(t, f, owned) })
	t.Run("current_identity", func(t *testing.T) { cleanupJobRefusals(t, f, owned) })
}

func cleanupJobConnections(t *testing.T, f *cleanupInventoryFixture, owned native.AdmittedOwnedState) {
	t.Helper()
	for _, connection := range []v1beta1.PostgresConnection{passwordMigration().Spec.Source, secretRefTarget(), uriSecretRefTarget()} {
		m := f.migration.DeepCopy()
		m.Spec.Source, m.Spec.Target = connection, connection
		job, err := buildNativeCleanupJob(m, f.pvc, f.want.ImageRef, nativeCleanupTestName, owned)
		if err != nil {
			t.Fatal(err)
		}
		c := job.Spec.Template.Spec.Containers[0]
		for _, side := range []conn.Side{conn.Source, conn.Target} {
			material, err := conn.Materialize(side, &connection)
			if err != nil {
				t.Fatal(err)
			}
			for _, env := range material.Env {
				if !slices.ContainsFunc(c.Env, func(got corev1.EnvVar) bool { return reflect.DeepEqual(got, env) }) {
					t.Fatal("cleanup changed canonical connection projection")
				}
			}
			for _, volume := range material.Volumes {
				if !slices.ContainsFunc(job.Spec.Template.Spec.Volumes, func(got corev1.Volume) bool { return reflect.DeepEqual(got, volume) }) {
					t.Fatal("cleanup lost connection volume")
				}
			}
			for _, mount := range material.Mounts {
				if !slices.Contains(c.VolumeMounts, mount) {
					t.Fatal("cleanup lost connection mount")
				}
			}
		}
	}
	first, err := buildNativeCleanupJob(f.migration, f.pvc, f.want.ImageRef, nativeCleanupTestName, owned)
	if err != nil {
		t.Fatal(err)
	}
	m := f.migration.DeepCopy()
	m.Spec.Source.URISecretRef, m.Spec.Target.URISecretRef = nil, nil
	m.Spec.Source, m.Spec.Target = passwordMigration().Spec.Source, passwordMigration().Spec.Target
	m.Spec.Source.PasswordSecretRef = nil
	second, err := buildNativeCleanupJob(m, f.pvc, f.want.ImageRef, nativeCleanupTestName, owned)
	if err != nil || podexec.NativeExecutionSHA256(first.Spec.Template) == podexec.NativeExecutionSHA256(second.Spec.Template) {
		t.Fatal("structurally different materialization did not change execution binding")
	}
	thirdMigration := m.DeepCopy()
	thirdMigration.Spec.Source.Host, thirdMigration.Spec.Target.Host = "rotated-source.example.test", "rotated-target.example.test"
	third, err := buildNativeCleanupJob(thirdMigration, f.pvc, f.want.ImageRef, nativeCleanupTestName, owned)
	if err != nil || podexec.NativeExecutionSHA256(second.Spec.Template) != podexec.NativeExecutionSHA256(third.Spec.Template) || reflect.DeepEqual(second.Spec.Template.Spec.Containers[0].Env, third.Spec.Template.Spec.Containers[0].Env) {
		t.Fatal("literal connection values entered correlation hash or disappeared")
	}
}

func cleanupJobRefusals(t *testing.T, f *cleanupInventoryFixture, owned native.AdmittedOwnedState) {
	t.Helper()
	for _, tc := range []struct {
		name string
		edit func(**v1beta1.Migration, **corev1.PersistentVolumeClaim, *string)
	}{
		{"missing_migration", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) { *m = nil }},
		{"missing_pvc", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _ *string) { *p = nil }},
		{"legacy_engine", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) { (*m).Spec.Engine = "" }},
		{"plain_clone", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) { (*m).Spec.Follow = nil }},
		{"instance_clone", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) {
			(*m).Spec.Clone.AllDatabases = true
		}},
		{"zero_generation", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) { (*m).Generation = 0 }},
		{"zero_attempt", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) { (*m).Status.Attempts = 0 }},
		{"wrong_worker", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) {
			(*m).Status.JobName = workerForeignIdentity
		}},
		{"foreign_work", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _ *string) {
			(*p).UID = workerForeignIdentity
		}},
		{"foreign_owner", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _ *string) {
			(*p).OwnerReferences[0].UID = workerForeignIdentity
		}},
		{"unbound_pvc", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _ *string) {
			(*p).Status.Phase = corev1.ClaimPending
		}},
		{"deleted_pvc", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _ *string) {
			now := metav1.Now()
			(*p).DeletionTimestamp = &now
		}},
		{"wrong_role", func(_ **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, n *string) { *n = nativeEndposTestName }},
		{"unsafe_operation", func(_ **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, n *string) {
			*n = "rpgmigrate-cleanup-$(private-canary)"
		}},
		{"uppercase_operation", func(_ **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, n *string) {
			*n = "rpgmigrate-cleanup-0123456789ABCDEF0123456789abcdef"
		}},
		{"mutable_image", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) {
			(*m).Spec.Runner.Image = "private-canary:latest"
		}},
		{"invalid_source", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) {
			(*m).Spec.Source.SynchronousCommit = nativeCleanupTestPrivateCanary
		}},
		{"invalid_target", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) {
			(*m).Spec.Target.SynchronousCommit = nativeCleanupTestPrivateCanary
		}},
		{"oversize_prelude", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) {
			(*m).Spec.Source = passwordMigration().Spec.Source
			(*m).Spec.Source.Host = strings.Repeat("a", 65536)
		}},
		{"oversize_job", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string) {
			(*m).Spec.Source = passwordMigration().Spec.Source
			(*m).Spec.Source.PasswordSecretRef = nil
			(*m).Spec.Source.Host = strings.Repeat("a", native.EvidenceJobJSONLimit)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, pvc, name := f.migration.DeepCopy(), f.pvc.DeepCopy(), nativeCleanupTestName
			tc.edit(&m, &pvc, &name)
			job, err := buildNativeCleanupJob(m, pvc, f.want.ImageRef, name, owned)
			if err == nil || job != nil {
				t.Fatal("invalid cleanup configuration produced a Job")
			}
			if strings.Contains(err.Error(), nativeCleanupTestPrivateCanary) {
				t.Fatal("cleanup refusal disclosed private input")
			}
		})
	}
	for _, deletion := range []bool{false, true} {
		m := f.migration.DeepCopy()
		m.Spec.Suspend = true
		if deletion {
			now := metav1.Now()
			m.DeletionTimestamp = &now
		}
		if job, err := buildNativeCleanupJob(m, f.pvc, f.want.ImageRef, nativeCleanupTestName, owned); err != nil || job == nil {
			t.Fatal("pure cleanup builder refused retained terminal deletion/suspend")
		}
	}
}

func assertNativeCleanupJobBounds(t *testing.T, job *batchv1.Job) {
	t.Helper()
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 90 || job.Spec.Completions == nil || *job.Spec.Completions != 1 || job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 || job.Spec.CompletionMode != nil || job.Spec.ManualSelector != nil || job.Spec.Suspend != nil {
		t.Fatal("cleanup helper can restart, parallelize or outlive its operation")
	}
}
