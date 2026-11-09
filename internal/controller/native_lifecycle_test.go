package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	lifecycleOwnedKey         = "owned"
	lifecycleRetainKey        = "retain"
	lifecycleSavedEndCase     = "saved_end"
	lifecycleJobKind          = "Job"
	lifecycleWaitingMain      = "running"
	lifecycleUnknownCommit    = "unknown_commit"
	lifecycleKindKey          = "kind"
	lifecycleNameKey          = "name"
	lifecycleReplacement      = "lifecycle-replacement"
	lifecycleForeignNamespace = "lifecycle-foreign"
	lifecycleGenerationCase   = "generation"
	lifecycleAttemptCase      = "attempt"
	lifecycleChangedEnd       = "0/21"
	lifecycleEnd              = "0/20"
	lifecycleCanary           = "postgresql://private-user:private-password@private-host/private-db"
	lifecycleHelperPod        = "native-helper-pod"
)

type lifecycleAPI struct {
	*httptest.Server
	mu                                      sync.Mutex
	t                                       *testing.T
	migration                               *v1beta1.Migration
	pvc                                     *corev1.PersistentVolumeClaim
	worker                                  *batchv1.Job
	job                                     *batchv1.Job
	pod                                     *corev1.Pod
	pendingReads                            int
	replacePending                          bool
	replaceBeforeReader                     bool
	readinessSeen                           bool
	logsRead                                bool
	simulatedCommit                         bool
	otherWrites                             int
	injected                                bool
	extraPods                               []corev1.Pod
	created                                 []*batchv1.Job
	inputs                                  [][]byte
	creates, relays, secrets, postExecReads int
	createFault, relayFault, mainFault      string
	logNoise                                []byte
	onCreate                                func(*lifecycleAPI)
	duringRelay                             func(*lifecycleAPI)
	afterLogs                               func(*lifecycleAPI)
}

// Synthetic image identities exercise component admission, without runtime qualification.
func newLifecycleAPI(t *testing.T) (*lifecycleAPI, *MigrationReconciler) {
	t.Helper()
	m, pvc := nativeIdentityObjects()
	m.Generation = 1
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	m.Status.Attempts = 1
	m.Status.JobName = m.Name + "-run-1"
	m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}
	m.Spec.Source.URISecretRef = &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "native-source"}, Key: "native-uri"}
	m.Spec.Target.URISecretRef = &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "native-target"}, Key: "native-uri"}
	pvc.Status.Phase = corev1.ClaimBound
	image := "registry.example.test/team/runner@sha256:" + strings.Repeat("1", 64)
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		t.Fatal(err)
	}
	worker := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: m.Status.JobName, Namespace: m.Namespace, UID: "native-worker", OwnerReferences: pvc.OwnerReferences}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "pgcopydb", Image: image, Command: []string{shellPath, "-c", "exec \"$0\" \"$@\"", "/usr/local/bin/rpgmigrate"}, Args: []string{"clone", "--follow", "--plugin", "pgoutput"}, Env: nativeIdentityEnv(work)}}}}}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	s := &lifecycleAPI{t: t, migration: m, pvc: pvc, worker: worker}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, batchv1.SchemeGroupVersion, v1beta1.GroupVersion})
	for _, entry := range []struct {
		gv   schema.GroupVersion
		kind string
	}{{corev1.SchemeGroupVersion, "Pod"}, {corev1.SchemeGroupVersion, "PersistentVolumeClaim"}, {batchv1.SchemeGroupVersion, lifecycleJobKind}, {v1beta1.GroupVersion, "Migration"}} {
		mapper.Add(entry.gv.WithKind(entry.kind), meta.RESTScopeNamespace)
	}
	config := &rest.Config{Host: s.URL}
	api, err := client.New(config, client.Options{Scheme: scheme, Mapper: mapper})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := podexec.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return s, &MigrationReconciler{Client: api, APIReader: api, Scheme: scheme, NativeEvidence: native.NewReader(api, transport), RPGMigrateRunnerImage: image}
}

func (s *lifecycleAPI) serve(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/exec") {
		s.exec(w, r)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		s.otherWrites++
		http.Error(w, "unexpected mutation", http.StatusForbidden)
		return
	}
	switch {
	case strings.Contains(r.URL.Path, "/secrets"):
		s.secrets++
		http.Error(w, lifecycleCanary, http.StatusForbidden)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/jobs"):
		s.createJob(w, r)
	case strings.HasSuffix(r.URL.Path, "/migrations/"+s.migration.Name):
		if s.replaceBeforeReader && s.readinessSeen && !s.injected {
			s.pod.UID = "replacement-before-reader-admission"
			s.replaceBeforeReader = false
		}
		_ = json.NewEncoder(w).Encode(s.migration)
	case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/"+s.pvc.Name):
		_ = json.NewEncoder(w).Encode(s.pvc)
	case strings.HasSuffix(r.URL.Path, "/jobs/"+s.worker.Name):
		_ = json.NewEncoder(w).Encode(s.worker)
	case s.job != nil && strings.HasSuffix(r.URL.Path, "/jobs/"+s.job.Name):
		_ = json.NewEncoder(w).Encode(s.job)
	case strings.HasSuffix(r.URL.Path, "/pods"):
		list := corev1.PodList{Items: append([]corev1.Pod{}, s.extraPods...)}
		if s.pod != nil {
			list.Items = append(list.Items, *s.pod.DeepCopy())
		}
		_ = json.NewEncoder(w).Encode(list)
	case strings.HasSuffix(r.URL.Path, "/pods/"+lifecycleHelperPod):
		s.servePod(w)
	case strings.HasSuffix(r.URL.Path, "/log"):
		s.logsRead = true
		if r.URL.Query().Has("tailLines") || r.URL.Query().Has("previous") || r.URL.Query().Has("timestamps") || r.URL.Query().Has("follow") || r.URL.Query().Has("stream") {
			http.Error(w, "partial logs refused", http.StatusBadRequest)
			return
		}
		_, _ = w.Write(s.logNoise)
		if s.afterLogs != nil {
			s.afterLogs(s)
			s.afterLogs = nil
		}
	default:
		http.NotFound(w, r)
	}
}

func (s *lifecycleAPI) servePod(w http.ResponseWriter) {
	if !s.injected {
		s.readinessSeen = true
	}
	if s.injected {
		s.postExecReads++
		if s.postExecReads >= 2 && s.mainFault != lifecycleWaitingMain {
			s.complete()
		}
	}

	if s.replacePending && !s.injected {
		s.pendingReads++
		if s.pendingReads == 1 {
			pending := s.pod.DeepCopy()
			pending.Status = corev1.PodStatus{Phase: corev1.PodPending}
			_ = json.NewEncoder(w).Encode(pending)
			s.pod.UID = "replacement-before-injection"
			return
		}
	}
	_ = json.NewEncoder(w).Encode(s.pod)
}

func (s *lifecycleAPI) createJob(w http.ResponseWriter, r *http.Request) {
	s.creates++
	job := &batchv1.Job{}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "fixture input unavailable", http.StatusBadRequest)
		return
	}
	if _, _, err := clientscheme.Codecs.UniversalDeserializer().Decode(raw, nil, job); err != nil {
		http.Error(w, "bad fixture request", http.StatusBadRequest)
		return
	}
	s.created = append(s.created, job.DeepCopy())
	job.UID = types.UID("helper-" + job.Name)
	s.job = job
	controller := true
	s.pod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: lifecycleHelperPod, Namespace: job.Namespace, UID: types.UID("pod-" + job.Name), Labels: map[string]string{batchv1.ControllerUidLabel: string(job.UID)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: lifecycleJobKind, Name: job.Name, UID: job.UID, Controller: &controller}}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "pgcopydb", ContainerID: "containerd://native-helper", Image: "runtime-display-tag", ImageID: job.Spec.Template.Spec.Containers[0].Image, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	s.postExecReads = 0
	s.injected = false
	if s.onCreate != nil {
		s.onCreate(s)
	}
	if s.createFault == "already_exists" {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonAlreadyExists, Message: lifecycleCanary, Code: http.StatusConflict})
		return
	}
	if s.createFault != "" {
		http.Error(w, lifecycleCanary, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(job)
}

func (s *lifecycleAPI) complete() {
	status := &s.pod.Status.ContainerStatuses[0]
	s.pod.Status.Phase = corev1.PodSucceeded
	status.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: status.ContainerID}}
	s.job.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	if s.mainFault == "exit" {
		status.State.Terminated.ExitCode = 17
	}
	if s.mainFault == "signal" {
		status.State.Terminated.Signal = 9
	}
}

func (s *lifecycleAPI) exec(w http.ResponseWriter, r *http.Request) {
	upgrade := websocket.Upgrader{Subprotocols: []string{"v5.channel.k8s.io"}}
	ws, err := upgrade.Upgrade(w, r, nil)
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
	s.mu.Lock()
	s.relays++
	s.injected = true
	s.inputs = append(s.inputs, bytes.Clone(input))
	if s.duringRelay != nil {
		s.duringRelay(s)
	}
	fault := s.relayFault
	result := lifecycleResult(s.t, input)
	if fault == lifecycleUnknownCommit {
		s.simulatedCommit = true
		var request struct {
			OperationID string `json:"operation_id"`
		}
		if err := json.Unmarshal(input, &request); err != nil {
			s.t.Error(err)
		}
		failure := map[string]any{"version": 1, lifecycleKindKey: "completed_evidence_failure", "operation_id": request.OperationID, "error_class": lifecycleUnknownCommit, "sqlstate": ""}
		raw, err := json.Marshal(failure)
		if err != nil {
			s.t.Error(err)
		}
		result = append(raw, '\n')
	}
	if fault == "finished" {
		s.complete()
	}
	s.mu.Unlock()
	if fault == "disconnect" {
		return
	}
	_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{1}, result...))
	if fault == "stderr" {
		_ = ws.WriteMessage(websocket.BinaryMessage, []byte{2, 'x'})
	}
	status := []byte(`{"status":"Success"}`)
	if fault == lifecycleUnknownCommit {
		status = []byte(`{"status":"Failure"}`)
	}
	if fault != "missing_status" {
		_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{3}, status...))
	}
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

func lifecycleResult(t *testing.T, input []byte) []byte {
	t.Helper()
	var request struct {
		OperationID   string `json:"operation_id"`
		ProgramSHA256 string `json:"program_sha256"`
		SavedE        string `json:"saved_e"`
		Execution     struct {
			Work native.WorkIdentity `json:"work"`
		} `json:"execution"`
	}
	if err := json.Unmarshal(input, &request); err != nil {
		t.Error(err)
		return nil
	}
	digest := sha256.Sum256(input)
	execution := sha256.Sum256(append([]byte("rpgmigrate.native.execution.v1\x00"), digest[:]...))
	guards := []string{"target_settings", "target_identity", "configure_durability", "target_fence", "idle_guard", "begin", "lock_checkpoint", "lock_marker", "catalog_checkpoint", "catalog_marker", "row_read", "same_value_update", "deferred_effects", "row_comparison", "precommit_guard", "commit", "idle_guard", "unlock", "closure_guard"}
	value := map[string]any{"version": 1, lifecycleKindKey: "completed_evidence", "operation_id": request.OperationID, "program_sha256": request.ProgramSHA256, "request_sha256": hex.EncodeToString(digest[:]), "execution_sha256": hex.EncodeToString(execution[:]), "work": request.Execution.Work, "checkpoint": map[string]any{"checkpoint_id": "1:0:1", "certified_lsn": request.SavedE, "durable_replay_lsn": request.SavedE, "drain_end_lsn": request.SavedE, "drain_replay_lsn": request.SavedE, "drain_kind": "keepalive", "barrier_confirmed": true}, "resources": []map[string]any{{lifecycleKindKey: "replication_slot", lifecycleNameKey: "owned_slot", lifecycleOwnedKey: true, lifecycleRetainKey: false}, {lifecycleKindKey: "publication", lifecycleNameKey: "owned_publication", lifecycleOwnedKey: true, lifecycleRetainKey: false}, {lifecycleKindKey: workdirVolume, lifecycleNameKey: "/work/pgcopydb", lifecycleOwnedKey: true, lifecycleRetainKey: true}}, "guards": guards, "source_closed": true, "target_closed": true, "private_closed": true, "diagnostics_clean": true}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Error(err)
		return nil
	}
	return append(raw, '\n')
}

func lifecycleCall(t *testing.T, s *lifecycleAPI, r *MigrationReconciler) (*native.CompletedObservation, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	observation, err := r.readNativeCompletedEvidence(ctx, s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy())
	s.mu.Lock()
	if s.secrets != 0 {
		t.Error("lifecycle accessed Secret API")
	}
	s.mu.Unlock()
	return observation, err
}

func TestNativeEvidenceDedicatedCreate(t *testing.T) {
	t.Run("real_checked_reader", lifecycleCheckedCreate)
	t.Run("configured_override", lifecycleConfiguredOverride)
	t.Run("create_uncertain_no_adoption", lifecycleUncertainCreate)
	t.Run("final_owner_overhead_cap", lifecycleFinalSizeCap)
}

func lifecycleCheckedCreate(t *testing.T) {
	s, r := newLifecycleAPI(t)
	got, err := lifecycleCall(t, s, r)
	if err != nil || got == nil {
		t.Fatalf("checked lifecycle refused: %v", err)
	}
	if got.Checkpoint().Evidence().DrainEndLSN != lifecycleEnd || !got.Checkpoint().Evidence().BarrierConfirmed || got.OwnedState().Work().WorkdirID != string(s.pvc.UID) {
		t.Fatal("reader observation lost binding")
	}
	if s.creates != 1 || s.relays != 1 || s.secrets != 0 {
		t.Fatalf("unexpected effects: creates=%d relays=%d secrets=%d", s.creates, s.relays, s.secrets)
	}
	created := s.created[0]
	owner := metav1.GetControllerOf(created)
	if created.UID != "" || owner == nil || owner.UID != s.migration.UID || created.Name == s.worker.Name || s.migration.Status.JobName != s.worker.Name {
		t.Fatal("helper adopted state or displaced worker")
	}
	var request struct {
		OperationID string `json:"operation_id"`
		SavedE      string `json:"saved_e"`
		Execution   struct {
			ImageRef        string `json:"image_ref"`
			PlatformImageID string `json:"platform_image_id"`
		} `json:"execution"`
	}
	if err := json.Unmarshal(s.inputs[0], &request); err != nil {
		t.Fatal(err)
	}
	if len(request.OperationID) != 32 || created.Name != "rpgmigrate-evidence-"+request.OperationID || request.SavedE != lifecycleEnd || request.Execution.ImageRef != r.RPGMigrateRunnerImage || request.Execution.PlatformImageID != r.RPGMigrateRunnerImage {
		t.Fatal("request does not use fresh operation/saved E/configured identities")
	}
}

func lifecycleConfiguredOverride(t *testing.T) {
	s, r := newLifecycleAPI(t)
	image := "registry.example.test/team/override@sha256:" + strings.Repeat("2", 64)
	s.migration.Spec.Runner.Image = image
	s.worker.Spec.Template.Spec.Containers[0].Image = image
	r.RPGMigrateRunnerImage = ""
	r.RunnerImage = "legacy-runner:latest"
	got, err := lifecycleCall(t, s, r)
	if got == nil || err != nil || s.creates != 1 || s.relays != 1 {
		t.Fatalf("explicit configured native override refused: %v", err)
	}
	if !bytes.Contains(s.inputs[0], []byte(`"image_ref":"`+image+`"`)) || !bytes.Contains(s.inputs[0], []byte(`"platform_image_id":"`+image+`"`)) {
		t.Fatal("override identities derived from another source")
	}
}

func lifecycleUncertainCreate(t *testing.T) {
	for _, fault := range []string{"lost_create_ack", "already_exists"} {
		t.Run(fault, func(t *testing.T) {
			s, r := newLifecycleAPI(t)
			s.createFault = fault
			got, err := lifecycleCall(t, s, r)
			if got != nil || err == nil || strings.Contains(err.Error(), lifecycleCanary) || s.creates != 1 || s.relays != 0 {
				t.Fatalf("uncertain Create produced proof or exposed error: %v", err)
			}
		})
	}
	t.Run("missing_returned_uid", func(t *testing.T) {
		s, r := newLifecycleAPI(t)
		s.onCreate = func(api *lifecycleAPI) { api.job.UID = "" }
		got, err := lifecycleCall(t, s, r)
		if got != nil || err == nil || s.creates != 1 || s.relays != 0 {
			t.Fatal("empty returned JobUID reached relay")
		}
	})
}

func lifecycleFinalSizeCap(t *testing.T) {
	s, r := newLifecycleAPI(t)
	s.migration.Spec.Runner.NodeSelector = map[string]string{"padding": ""}
	work, err := nativeWorkIdentity(s.migration, s.pvc)
	if err != nil {
		t.Fatal(err)
	}
	want := native.ExecutionRequest{Namespace: s.migration.Namespace, MigrationName: s.migration.Name, MigrationUID: s.migration.UID, MigrationGeneration: s.migration.Generation, PVCName: s.pvc.Name, PVCUID: s.pvc.UID, WorkerJobUID: s.worker.UID, Attempt: 1, Role: native.JobRoleEvidence, Work: work, OperationID: strings.Repeat("a", 32), JobName: "rpgmigrate-evidence-" + strings.Repeat("a", 32), ImageRef: r.RPGMigrateRunnerImage, PlatformImageID: r.RPGMigrateRunnerImage, ProgramSHA256: native.EvidenceProgramSHA256()}
	bare, err := buildNativeEvidenceJob(s.migration, r.RPGMigrateRunnerImage, want)
	if err != nil {
		t.Fatal(err)
	}
	bare.APIVersion, bare.Kind = "batch/v1", lifecycleJobKind
	raw, err := json.Marshal(bare)
	if err != nil {
		t.Fatal(err)
	}
	s.migration.Spec.Runner.NodeSelector["padding"] = strings.Repeat("x", 1048576-len(raw))
	got, err := lifecycleCall(t, s, r)
	if got != nil || err == nil || s.creates != 0 || s.relays != 0 {
		t.Fatal("decorated over-limit Job reached Create")
	}
}

func TestNativeEvidenceLifecycleReadmission(t *testing.T) {
	lifecyclePrerequisiteRefusals(t)
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim)
	}{
		{"migration_namespace", func(m *v1beta1.Migration, _ *batchv1.Job, _ *corev1.PersistentVolumeClaim) {
			m.Namespace = lifecycleForeignNamespace
		}},
		{"worker_namespace", func(_ *v1beta1.Migration, w *batchv1.Job, _ *corev1.PersistentVolumeClaim) {
			w.Namespace = lifecycleForeignNamespace
		}},
		{"pvc_namespace", func(_ *v1beta1.Migration, _ *batchv1.Job, p *corev1.PersistentVolumeClaim) {
			p.Namespace = lifecycleForeignNamespace
		}},
	} {
		t.Run("expected_"+tc.name, func(t *testing.T) {
			s, r := newLifecycleAPI(t)
			m, w, p := s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy()
			tc.change(m, w, p)
			got, err := r.readNativeCompletedEvidence(context.Background(), m, w, p)
			if got != nil || err == nil || s.creates != 0 {
				t.Fatal("foreign expected namespace reached Create")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*lifecycleAPI)
	}{
		{"migration_uid", func(s *lifecycleAPI) { s.migration.UID = lifecycleReplacement }},
		{lifecycleGenerationCase, func(s *lifecycleAPI) { s.migration.Generation++ }},
		{lifecycleAttemptCase, func(s *lifecycleAPI) { s.migration.Status.Attempts++ }},
		{"worker_uid", func(s *lifecycleAPI) { s.worker.UID = lifecycleReplacement }},
		{"worker_current", func(s *lifecycleAPI) { s.migration.Status.JobName = lifecycleReplacement }},
		{"pvc_uid", func(s *lifecycleAPI) { s.pvc.UID = lifecycleReplacement }},
		{lifecycleSavedEndCase, func(s *lifecycleAPI) { s.migration.Status.Replication.Endpos = lifecycleChangedEnd }},
	} {
		t.Run("live_"+tc.name, func(t *testing.T) {
			s, r := newLifecycleAPI(t)
			m, w, p := s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy()
			tc.change(s)
			got, err := r.readNativeCompletedEvidence(context.Background(), m, w, p)
			if got != nil || err == nil || s.creates != 0 || s.relays != 0 {
				t.Fatalf("changed live binding reached helper: %v", err)
			}
		})
	}
	lifecyclePreRelayRefusals(t)
	for _, phase := range []string{"during_relay", "after_logs"} {
		for _, tc := range []struct {
			name   string
			change func(*lifecycleAPI)
		}{
			{lifecycleSavedEndCase, func(s *lifecycleAPI) { s.migration.Status.Replication.Endpos = lifecycleChangedEnd }},
			{lifecycleGenerationCase, func(s *lifecycleAPI) { s.migration.Generation++ }},
			{"worker_binding", func(s *lifecycleAPI) { s.worker.UID = lifecycleReplacement }},
			{"pvc", func(s *lifecycleAPI) { s.pvc.UID = lifecycleReplacement }},
			{"pod", func(s *lifecycleAPI) { s.pod.UID = lifecycleReplacement }},
			{"container", func(s *lifecycleAPI) { s.pod.Status.ContainerStatuses[0].ContainerID = lifecycleReplacement }},
		} {
			t.Run(phase+"_"+tc.name, func(t *testing.T) {
				s, r := newLifecycleAPI(t)
				if phase == "during_relay" {
					s.duringRelay = tc.change
				} else {
					s.afterLogs = tc.change
				}
				got, err := lifecycleCall(t, s, r)
				if got != nil || err == nil || s.relays != 1 {
					t.Fatalf("changed binding produced observation: %v", err)
				}
			})
		}
	}
	for _, fault := range []string{"stderr", "disconnect", "missing_status"} {
		t.Run("relay_"+fault, func(t *testing.T) {
			s, r := newLifecycleAPI(t)
			s.relayFault = fault
			got, err := lifecycleCall(t, s, r)
			if got != nil || err == nil || s.relays != 1 {
				t.Fatal("bad relay produced authority")
			}
		})
	}
	for _, fault := range []string{"exit", "signal", lifecycleWaitingMain, "logs"} {
		t.Run("main_"+fault, func(t *testing.T) {
			s, r := newLifecycleAPI(t)
			s.mainFault = fault
			if fault == "logs" {
				s.logNoise = []byte("unexpected ordinary output")
			}
			got, err := lifecycleCall(t, s, r)
			if got != nil || err == nil || s.relays != 1 {
				t.Fatal("bad main completion produced authority")
			}
		})
	}
	t.Run("main_exits_before_post_exec_read", func(t *testing.T) {
		s, r := newLifecycleAPI(t)
		s.relayFault = "finished"
		got, err := lifecycleCall(t, s, r)
		if got == nil || err != nil {
			t.Fatalf("complete original incarnation refused: %v", err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		s, r := newLifecycleAPI(t)
		ctx, cancel := context.WithCancel(context.Background())
		s.afterLogs = func(_ *lifecycleAPI) { cancel() }
		got, err := r.readNativeCompletedEvidence(ctx, s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy())
		if got != nil || err == nil {
			t.Fatal("cancelled collection returned authority")
		}
	})
}

func lifecyclePreRelayRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*lifecycleAPI)
	}{
		{"job_uid", func(s *lifecycleAPI) { s.job.UID = "replacement" }},
		{"job_owner", func(s *lifecycleAPI) { s.job.OwnerReferences[0].UID = "replacement" }},
		{"job_command", func(s *lifecycleAPI) { s.job.Spec.Template.Spec.Containers[0].Command[2] += "\nfalse" }},
		{"pod_image_id", func(s *lifecycleAPI) { s.pod.Status.ContainerStatuses[0].ImageID = "sha256:" + strings.Repeat("1", 64) }},
		{"pod_restart", func(s *lifecycleAPI) { s.pod.Status.ContainerStatuses[0].RestartCount++ }},
		{"pod_done", func(s *lifecycleAPI) { s.complete() }},
		{"pod_ambiguous", func(s *lifecycleAPI) {
			s.extraPods = []corev1.Pod{*s.pod.DeepCopy()}
			s.extraPods[0].Name = "other-pod"
			s.extraPods[0].UID = "other-pod"
		}},
		{"defaulted_over_limit", func(s *lifecycleAPI) { s.job.Annotations = map[string]string{"padding": strings.Repeat("<", 400000)} }},
	} {
		t.Run("before_relay_"+tc.name, func(t *testing.T) {
			s, r := newLifecycleAPI(t)
			s.onCreate = tc.change
			got, err := lifecycleCall(t, s, r)
			if got != nil || err == nil || s.creates != 1 || s.relays != 0 {
				t.Fatalf("invalid incarnation reached relay: %v", err)
			}
		})
	}
}

func lifecyclePrerequisiteRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*lifecycleAPI, *MigrationReconciler)
	}{
		{"reader_missing", func(_ *lifecycleAPI, r *MigrationReconciler) { r.NativeEvidence = nil }},
		{"uncached_reader_missing", func(_ *lifecycleAPI, r *MigrationReconciler) { r.APIReader = nil }},
		{"image_missing", func(_ *lifecycleAPI, r *MigrationReconciler) { r.RPGMigrateRunnerImage = "" }},
		{"legacy_image_no_fallback", func(_ *lifecycleAPI, r *MigrationReconciler) {
			r.RPGMigrateRunnerImage = ""
			r.RunnerImage = "legacy-runner:latest"
		}},
		{"image_tag", func(_ *lifecycleAPI, r *MigrationReconciler) {
			r.RPGMigrateRunnerImage = "registry.example.test/team/runner:latest"
		}},
		{"image_tag_digest", func(_ *lifecycleAPI, r *MigrationReconciler) {
			r.RPGMigrateRunnerImage = "registry.example.test/team/runner:latest@sha256:" + strings.Repeat("1", 64)
		}},
		{"saved_end_absent", func(s *lifecycleAPI, _ *MigrationReconciler) { s.migration.Status.Replication = nil }},
		{"saved_end_zero", func(s *lifecycleAPI, _ *MigrationReconciler) { s.migration.Status.Replication.Endpos = "0/00000000" }},
		{"saved_end_malformed", func(s *lifecycleAPI, _ *MigrationReconciler) { s.migration.Status.Replication.Endpos = "invalid" }},
		{"worker_not_complete", func(s *lifecycleAPI, _ *MigrationReconciler) { s.worker.Status.Conditions = nil }},
		{"worker_active", func(s *lifecycleAPI, _ *MigrationReconciler) { s.worker.Status.Active = 1 }},
		{"worker_failed", func(s *lifecycleAPI, _ *MigrationReconciler) { s.worker.Status.Failed = 1 }},
		{"pvc_pending", func(s *lifecycleAPI, _ *MigrationReconciler) { s.pvc.Status.Phase = corev1.ClaimPending }},
		{"pvc_foreign", func(s *lifecycleAPI, _ *MigrationReconciler) { s.pvc.OwnerReferences[0].UID = lifecycleReplacement }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := newLifecycleAPI(t)
			tc.change(s, r)
			got, err := lifecycleCall(t, s, r)
			if got != nil || err == nil || s.creates != 0 || s.relays != 0 {
				t.Fatalf("invalid input reached helper: %v", err)
			}
		})
	}
}

func TestNativeEvidenceFreshOperationSameEnd(t *testing.T) {
	s, r := newLifecycleAPI(t)
	s.relayFault = lifecycleUnknownCommit
	got, err := lifecycleCall(t, s, r)
	if got != nil || err == nil || s.creates != 1 || s.relays != 1 || !s.simulatedCommit || s.otherWrites != 0 {
		t.Fatal("unknown commit yielded observation or outer mutation")
	}
	first := s.job.DeepCopy()
	s.relayFault = ""
	s.postExecReads = -1
	got, err = lifecycleCall(t, s, r)
	if got == nil || err != nil || s.creates != 2 || s.relays != 2 {
		t.Fatalf("fresh invocation refused: %v", err)
	}
	if s.job.Name == first.Name || s.job.UID == first.UID || s.migration.Status.Replication.Endpos != lifecycleEnd || s.migration.Status.JobName != s.worker.Name {
		t.Fatal("fresh retry reused helper or changed durable worker/end state")
	}
	for _, input := range s.inputs {
		if !bytes.Contains(input, []byte(`"saved_e":"0/20"`)) {
			t.Fatal("retry moved durable end")
		}
	}
}

func TestNativeEvidenceDirectImageSyntax(t *testing.T) {
	pin := "@sha256:" + strings.Repeat("1", 64)
	for _, repo := range []string{"registry.example.test/team/runner", "registry.example.test:5000/team/runner", "localhost/runner", "127.0.0.1:5000/runner", "registry.example.test/team-1/runner.alpha_1"} {
		t.Run("accept_"+repo, func(t *testing.T) {
			if !nativeEvidenceDirectImage(repo + pin) {
				t.Fatal("supported explicit immutable repository refused")
			}
		})
	}
	for _, image := range []string{"runner" + pin, "registry/runner" + pin, "Registry.example.test/team/runner" + pin, "https://registry.example.test/runner" + pin, "registry.example.test/team/runner:tag" + pin, "user@registry.example.test/runner" + pin, "registry.example.test/runner?query" + pin, "registry.example.test/runner#fragment" + pin, "registry.example.test/%72unner" + pin, "registry.example.test/../runner" + pin, "registry.example.test//runner" + pin, "registry.example.test/runner/" + pin, "registry.example.test/runner\\path" + pin, "registry.example.test/runner name" + pin, "registry.example.test/runner" + strings.ReplaceAll(pin, "1", "A"), "registry.example.test:/runner" + pin, "registry.example.test:05000/runner" + pin, "registry.example.test:65536/runner" + pin, "127.000.0.1/runner" + pin, "[::1]:5000/runner" + pin, "registry.example.test/a__b" + pin, "registry.example.test/" + strings.Repeat("a", 220) + pin} {
		t.Run("refuse_"+image, func(t *testing.T) {
			if nativeEvidenceDirectImage(image) {
				t.Fatal("unsupported or noncanonical image admitted")
			}
		})
	}
}

type lifecycleFinalReader struct {
	client.Reader
	api    *lifecycleAPI
	change func(*lifecycleAPI)
}

func (r *lifecycleFinalReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	r.api.mu.Lock()
	if r.api.logsRead && r.change != nil {
		r.change(r.api)
		r.change = nil
	}
	r.api.mu.Unlock()
	return r.Reader.Get(ctx, key, object, opts...)
}

func TestNativeEvidenceFinalCallerComparison(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*lifecycleAPI)
	}{
		{lifecycleSavedEndCase, func(s *lifecycleAPI) { s.migration.Status.Replication.Endpos = lifecycleChangedEnd }},
		{"migration_uid", func(s *lifecycleAPI) { s.migration.UID = lifecycleReplacement }},
		{lifecycleGenerationCase, func(s *lifecycleAPI) { s.migration.Generation++ }},
		{lifecycleAttemptCase, func(s *lifecycleAPI) { s.migration.Status.Attempts++ }},
		{"worker_uid", func(s *lifecycleAPI) { s.worker.UID = lifecycleReplacement }},
		{"worker_owner", func(s *lifecycleAPI) { s.worker.OwnerReferences[0].UID = lifecycleReplacement }},
		{"pvc_owner", func(s *lifecycleAPI) { s.pvc.OwnerReferences[0].UID = lifecycleReplacement }},
		{"helper_pod", func(s *lifecycleAPI) { s.pod.UID = lifecycleReplacement }},
		{"helper_container", func(s *lifecycleAPI) { s.pod.Status.ContainerStatuses[0].ContainerID = lifecycleReplacement }},
		{"helper_restart", func(s *lifecycleAPI) { s.pod.Status.ContainerStatuses[0].RestartCount++ }},
		{"helper_image", func(s *lifecycleAPI) { s.pod.Status.ContainerStatuses[0].ImageID = lifecycleReplacement }},
		{"helper_pod_namespace", func(s *lifecycleAPI) { s.pod.Namespace = lifecycleForeignNamespace }},
		{"helper_job_namespace", func(s *lifecycleAPI) { s.job.Namespace = lifecycleForeignNamespace }},
		{"helper_previous_termination", func(s *lifecycleAPI) {
			s.pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ExitCode: 1}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := newLifecycleAPI(t)
			r.APIReader = &lifecycleFinalReader{Reader: r.APIReader, api: s, change: tc.change}
			got, err := lifecycleCall(t, s, r)
			if got != nil || err == nil || !s.logsRead || s.relays != 1 {
				t.Fatalf("final caller binding change retained observation: %v", err)
			}
		})
	}
}

func TestNativeEvidencePendingPodReplacement(t *testing.T) {
	s, r := newLifecycleAPI(t)
	s.replacePending = true
	got, err := lifecycleCall(t, s, r)
	if got != nil || err == nil || s.creates != 1 || s.relays != 0 {
		t.Fatal("replacement of original pending Pod reached relay")
	}
}

func TestNativeEvidenceReadinessReaderSeam(t *testing.T) {
	s, r := newLifecycleAPI(t)
	s.replaceBeforeReader = true
	got, err := lifecycleCall(t, s, r)
	if got != nil || err == nil || s.creates != 1 || s.relays != 1 {
		t.Fatal("readiness-to-Reader replacement retained proof")
	}
}

func TestNativeEvidenceCreateLossFreshInvocation(t *testing.T) {
	s, r := newLifecycleAPI(t)
	s.createFault = "lost_create_ack"
	got, err := lifecycleCall(t, s, r)
	if got != nil || err == nil || s.creates != 1 || s.relays != 0 {
		t.Fatal("lost Create acknowledgment yielded observation")
	}
	first := s.job.DeepCopy()
	s.complete()
	s.createFault = ""
	transport, err := podexec.New(&rest.Config{Host: s.URL})
	if err != nil {
		t.Fatal(err)
	}
	restarted := *r
	restarted.NativeEvidence = native.NewReader(r.APIReader, transport)
	got, err = lifecycleCall(t, s, &restarted)
	if got == nil || err != nil || s.creates != 2 || s.relays != 1 || s.job.Name == first.Name || s.job.UID == first.UID || s.migration.Status.Replication.Endpos != lifecycleEnd {
		t.Fatalf("fresh controller reused old operation or moved E: %v", err)
	}
}
