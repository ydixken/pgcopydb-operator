package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	readerResourceOwnedKey   = "owned"
	readerResourceRetainKey  = "retain"
	readerRequestCase        = "request"
	readerPodName            = "helper-pod"
	readerResourceNameKey    = "name"
	readerStatusCase         = "status"
	readerKindKey            = "kind"
	readerOperationIDKey     = "operation_id"
	readerReplacementUID     = "replacement"
	readerVersionJSON        = "\"version\":1"
	readerCaseFolded         = "case"
	readerCaseNull           = "null"
	readerCaseOverflow       = "overflow"
	readerCaseMissing        = "missing"
	readerCaseUnknown        = "unknown"
	readerCaseDuplicate      = "duplicate"
	readerCheckpointID       = "1:0:1"
	readerEarlierDurableJSON = "\"durable_replay_lsn\":\"0/10\""
	readerFaultSignal        = "signal"
	readerFaultUnknownCommit = "unknown_commit"
	readerFaultDisconnect    = "disconnect"
	readerExecutionKey       = "execution"
	readerExecutionSHAKey    = "execution_sha256"
	readerFaultNoise         = "noise"
	readerSourceClosedJSON   = "\"source_closed\":true"
)

func readerWant() ExecutionRequest {
	return ExecutionRequest{Namespace: "test", MigrationName: "migration", PVCName: "migration-work", JobName: "migration-evidence", MigrationUID: types.UID(fixtureMigrationID), PVCUID: "pvc-test", JobUID: "job-test", WorkerJobUID: "worker-test", MigrationGeneration: 1, Attempt: 1, Role: JobRoleEvidence, Work: WorkIdentity{MigrationUID: fixtureMigrationID, RunID: fixtureRunID, WorkdirID: "pvc-test", CheckpointGeneration: 1}, ImageRef: "runner@sha256:" + strings.Repeat("1", 64), PlatformImageID: "docker-pullable://runner@sha256:" + strings.Repeat("1", 64), TemplateSHA256: sha256.Sum256([]byte("template")), ConnectionReferenceSHA256: sha256.Sum256([]byte("references")), OperationID: strings.Repeat("a", 32), ProgramSHA256: sha256.Sum256([]byte("program"))}
}

func readerBinding(w ExecutionRequest) podexec.NativeContainerBinding {
	return podexec.NativeContainerBinding{PodUID: "pod-test", ContainerName: nativeContainerName, ContainerID: "containerd://test", RestartCount: 0, ImageID: w.PlatformImageID, TemplateSHA256: w.TemplateSHA256}
}

func TestNativeRequestWire(t *testing.T) {
	w := readerWant()
	raw, err := completedRequest(w, readerPodName, readerBinding(w), fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseCompletedRequest(raw); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("request_sha256")) || bytes.Contains(raw, []byte(readerExecutionSHAKey)) {
		t.Fatal("request contains a circular digest")
	}
	for _, tc := range []struct{ name, old, next string }{
		{readerCaseDuplicate, readerVersionJSON, `"version":1,"version":1`},
		{readerCaseFolded, readerVersionJSON, `"Version":1`},
		{readerCaseNull, readerVersionJSON, `"version":null`},
		{"float", readerVersionJSON, `"version":1.0`},
		{"bool", readerVersionJSON, `"version":true`},
		{readerCaseOverflow, `"migration_generation":1`, `"migration_generation":9223372036854775808`},
		{"attempt_overflow", `"attempt":1`, `"attempt":2147483648`},
		{readerCaseMissing, `"version":1,`, ``},
		{readerCaseUnknown, readerVersionJSON, `"version":1,"extra":1`},
		{"work_missing", `"checkpoint_generation":1`, `"extra":1`},
		{"role", `"role":"evidence"`, `"role":"worker"`},
		{"operation", w.OperationID, strings.Repeat("A", 32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := replace(t, raw, tc.old, tc.next)
			if _, err := parseCompletedRequest(bad); err == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
	for _, bad := range [][]byte{append([]byte{0xef, 0xbb, 0xbf}, raw...), append(bytes.Clone(raw), 'x'), append(bytes.Clone(raw), raw...), []byte(strings.Repeat(" ", 65537)), append(bytes.Clone(raw), 0xff)} {
		if _, err := parseCompletedRequest(bad); err == nil {
			t.Fatal("invalid document accepted")
		}
	}
}

func TestNativeRequestDigests(t *testing.T) {
	raw := []byte(`{"version":1}`)
	request, execution := requestDigests(raw)
	wantRequest := sha256.Sum256(raw)
	wantExecution := sha256.Sum256(append([]byte("rpgmigrate.native.execution.v1\x00"), wantRequest[:]...))
	if request != wantRequest || execution != wantExecution {
		t.Fatal("exact-byte digest mismatch")
	}
	other, _ := requestDigests(append(bytes.Clone(raw), ' '))
	if request == other {
		t.Fatal("whitespace was reserialized")
	}

}

func readerResult(t *testing.T, w ExecutionRequest, body []byte) []byte {
	t.Helper()
	request, execution := requestDigests(body)
	v := map[string]any{"version": 1, readerKindKey: completedEvidenceKind, readerOperationIDKey: w.OperationID, "request_sha256": hex.EncodeToString(request[:]), "program_sha256": hex.EncodeToString(w.ProgramSHA256[:]), readerExecutionSHAKey: hex.EncodeToString(execution[:]), evidenceWork: w.Work, "checkpoint": map[string]any{"checkpoint_id": readerCheckpointID, "certified_lsn": fixtureEndLSN, "durable_replay_lsn": fixtureEndLSN, "drain_end_lsn": fixtureEndLSN, "drain_replay_lsn": fixtureEndLSN, "drain_kind": nativeDrainKeepalive, "barrier_confirmed": true}, "resources": []map[string]any{{readerKindKey: "replication_slot", readerResourceNameKey: "owned_slot", readerResourceOwnedKey: true, readerResourceRetainKey: false}, {readerKindKey: "publication", readerResourceNameKey: "owned_publication", readerResourceOwnedKey: true, readerResourceRetainKey: false}, {readerKindKey: resourceKindWorkdir, readerResourceNameKey: "/work/pgcopydb", readerResourceOwnedKey: true, readerResourceRetainKey: true}}, "guards": completedGuards, "source_closed": true, "target_closed": true, "private_closed": true, "diagnostics_clean": true}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func TestNativeResultWire(t *testing.T) {
	w := readerWant()
	body, err := completedRequest(w, readerPodName, readerBinding(w), fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	raw := readerResult(t, w, body)
	if _, err := parseCompletedResult(raw, w, body, fixtureEndLSN); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, old, next string }{
		{readerCaseDuplicate, readerVersionJSON, `"version":1,"version":1`}, {readerCaseFolded, readerVersionJSON, `"Version":1`}, {readerCaseNull, readerSourceClosedJSON, `"source_closed":null`}, {"false", readerSourceClosedJSON, `"source_closed":false`}, {"missing_closure", `"source_closed":true,`, ``}, {readerCaseUnknown, readerVersionJSON, `"version":1,"extra":1`}, {"guard_order", `"target_settings","target_identity"`, `"target_identity","target_settings"`}, {"wrong_e", `"drain_end_lsn":"0/20"`, `"drain_end_lsn":"0/21"`}, {"nondurable", `"durable_replay_lsn":"0/20"`, readerEarlierDurableJSON}, {"wrong_token", `"checkpoint_id":"1:0:1"`, `"checkpoint_id":"2:0:1"`}, {"wrong_workdir", `"name":"/work/pgcopydb"`, `"name":"/work/other"`}, {"owned_origin", `"kind":"publication"`, `"kind":"replication_origin"`}, {"null_resources", `"resources":[`, `"resources":null,"discard":[`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseCompletedResult(replace(t, raw, tc.old, tc.next), w, body, fixtureEndLSN); err == nil {
				t.Fatal("invalid result accepted")
			}
		})
	}
	for _, bad := range [][]byte{bytes.TrimSuffix(raw, []byte{'\n'}), append(bytes.Clone(raw), '\n'), append([]byte("log\n"), raw...), append(bytes.Clone(raw), raw...), append(bytes.Clone(raw), 'x')} {
		if _, err := parseCompletedResult(bad, w, body, fixtureEndLSN); err == nil {
			t.Fatal("invalid result framing accepted")
		}
	}
}

func TestNativeAuthorityZeroValues(t *testing.T) {
	var observation CompletedObservation
	if observation.Checkpoint().Evidence() != (CheckpointEvidence{}) || observation.OwnedState().Work() != (WorkIdentity{}) || observation.OwnedState().Resources() != nil {
		t.Fatal("zero value confers authority")
	}
}

type readerAPI struct {
	*httptest.Server
	mu               sync.Mutex
	fixture          admissionFixture
	extraPods        []corev1.Pod
	t                *testing.T
	input            []byte
	injected         bool
	postExecReads    int
	result           []byte
	mutateResult     func([]byte) []byte
	afterResult      func(*admissionFixture)
	writerFault      string
	writerCalls      int
	relayCalls       int
	logCalls         int
	splitLogCalls    int
	mainFault        string
	logNoise         []byte
	finishDuringExec bool
	blockCompletion  bool
}

func newReaderAPI(t *testing.T) (*readerAPI, *Reader) {
	t.Helper()
	s := &readerAPI{fixture: newAdmissionFixture(t), t: t}
	s.fixture.pod.Labels = map[string]string{batchv1.ControllerUidLabel: string(s.fixture.want.JobUID)}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, batchv1.SchemeGroupVersion, v1beta1.GroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("Pod"), meta.RESTScopeNamespace)
	mapper.Add(corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"), meta.RESTScopeNamespace)
	mapper.Add(batchv1.SchemeGroupVersion.WithKind("Job"), meta.RESTScopeNamespace)
	mapper.Add(v1beta1.GroupVersion.WithKind("Migration"), meta.RESTScopeNamespace)
	config := &rest.Config{Host: s.URL}
	api, err := client.New(config, client.Options{Scheme: scheme, Mapper: mapper})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := podexec.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return s, NewReader(api, transport)
}

func (s *readerAPI) complete() {
	f := &s.fixture
	f.pod.Status.Phase = corev1.PodSucceeded
	c := &f.pod.Status.ContainerStatuses[0]
	c.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: c.ContainerID, ExitCode: 0}}
	f.job.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	switch s.mainFault {
	case "exit":
		c.State.Terminated.ExitCode = 17
		f.pod.Status.Phase = corev1.PodFailed
		f.job.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
	case readerFaultSignal:
		c.State.Terminated.Signal = 9
		f.pod.Status.Phase = corev1.PodFailed
	case "failed_job":
		f.job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	}
}

func (s *readerAPI) serve(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/exec") {
		s.exec(w, r)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := &s.fixture
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(r.URL.Path, "/migrations/"+f.want.MigrationName):
		_ = json.NewEncoder(w).Encode(f.m)
	case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/"+f.want.PVCName):
		_ = json.NewEncoder(w).Encode(f.pvc)
	case strings.HasSuffix(r.URL.Path, "/jobs/"+f.want.JobName):
		_ = json.NewEncoder(w).Encode(f.job)
	case strings.HasSuffix(r.URL.Path, "/jobs/"+f.m.Status.JobName):
		_ = json.NewEncoder(w).Encode(f.worker)
	case strings.HasSuffix(r.URL.Path, "/pods"):
		_ = json.NewEncoder(w).Encode(corev1.PodList{Items: append([]corev1.Pod{*f.pod.DeepCopy()}, s.extraPods...)})
	case strings.HasSuffix(r.URL.Path, "/pods/helper-pod"):
		if s.injected {
			s.postExecReads++
			if s.postExecReads >= 2 && !s.blockCompletion {
				s.complete()
			}
		}
		_ = json.NewEncoder(w).Encode(f.pod)
	case strings.HasSuffix(r.URL.Path, "/log"):
		s.logCalls++
		if r.URL.Query().Has("stream") {
			s.splitLogCalls++
			http.Error(w, "split log queries disabled", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Has("tailLines") || r.URL.Query().Has("previous") || r.URL.Query().Has("timestamps") || r.URL.Query().Has("follow") {
			http.Error(w, "incomplete log query", http.StatusBadRequest)
			return
		}
		_, _ = w.Write(s.logNoise)
		if s.afterResult != nil {
			s.afterResult(f)
			s.afterResult = nil
		}

	default:
		http.NotFound(w, r)
	}
}

func (s *readerAPI) exec(w http.ResponseWriter, r *http.Request) {
	upgrade := websocket.Upgrader{Subprotocols: []string{"v5.channel.k8s.io"}}
	c, err := upgrade.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = c.Close() }()
	s.mu.Lock()
	s.relayCalls++
	consumed := s.injected
	s.mu.Unlock()
	if consumed {
		_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{3}, []byte(`{"status":"Failure"}`)...))
		_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		return
	}
	var input []byte
	for {
		_, raw, err := c.ReadMessage()
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
	s.input = bytes.Clone(input)
	s.injected = true
	s.writerCalls++
	s.result = readerResult(s.t, s.fixture.want, input)
	if s.mutateResult != nil {
		s.result = s.mutateResult(s.result)
	}
	result := bytes.Clone(s.result)
	fault := s.writerFault
	if s.finishDuringExec {
		s.complete()
	}
	s.mu.Unlock()
	if fault == "trailing" {
		result = append(result, 'x')
	}
	if fault == readerFaultUnknownCommit {
		result = []byte(`{"version":1,"kind":"completed_evidence_failure","operation_id":"` + s.fixture.want.OperationID + `","error_class":"unknown_commit","sqlstate":""}` + "\n")
	}
	_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{1}, result...))

	if fault == "stderr" {
		_ = c.WriteMessage(websocket.BinaryMessage, []byte{2, 'x'})
	}
	if fault != "stdout_only" {
		status := []byte(`{"status":"Success"}`)
		if fault == readerStatusCase || fault == readerFaultUnknownCommit {
			status = []byte(`{"status":"Failure"}`)
		}
		_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{3}, status...))
	}
	if fault == "hold" {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}
	if fault != readerFaultDisconnect {
		_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	}
}

func TestNativeEvidenceExecutionAdmission(t *testing.T) {
	readerCheckedCompletion(t)
	readerAdmissionRefusals(t)
	readerWriterRefusals(t)
	readerPostResultRefusals(t)
	readerCorrelationRefusals(t)
}

func readerCheckedCompletion(t *testing.T) {
	t.Run("checked_completion", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN)
		if err != nil || got == nil {
			t.Fatalf("checked result rejected: %v", err)
		}
		if got.Checkpoint().Evidence().CheckpointID != readerCheckpointID || !got.Checkpoint().Evidence().BarrierConfirmed || got.OwnedState().Work() != api.fixture.want.Work || len(got.OwnedState().Resources()) != 3 {
			t.Fatal("checked result lost authority fields")
		}
		resources := got.OwnedState().Resources()
		resources[0].Name = "mutated"
		if got.OwnedState().Resources()[0].Name == "mutated" {
			t.Fatal("resource accessor aliases authority")
		}
		checkpoint := got.Checkpoint().Evidence()
		checkpoint.BarrierConfirmed = false
		if !got.Checkpoint().Evidence().BarrierConfirmed {
			t.Fatal("checkpoint accessor aliases authority")
		}
		api.mu.Lock()
		defer api.mu.Unlock()
		parsed, err := parseCompletedRequest(api.input)
		if err != nil || parsed.PodName != readerPodName || parsed.Execution.JobUID != string(api.fixture.want.JobUID) || api.writerCalls != 1 {
			t.Fatal("writer did not receive admitted request")
		}
	})
}
func readerAdmissionRefusals(t *testing.T) {
	t.Run("cancelled_before_admission", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("cancelled operation produced authority")
		}
		if api.writerCalls != 0 {
			t.Fatal("cancelled admission reached writer")
		}
	})

	t.Run("missing_persisted_end", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		api.fixture.m.Status.Replication = nil
		if got, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("unpersisted end LSN produced authority")
		}
		if api.writerCalls != 0 {
			t.Fatal("writer invoked before persisted end readback")
		}
	})
	t.Run("mismatched_persisted_end", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		api.fixture.m.Status.Replication.Endpos = fixtureEarlierLSN
		if got, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("different persisted end LSN produced authority")
		}
		if api.writerCalls != 0 {
			t.Fatal("writer invoked with different persisted end")
		}
	})

	t.Run("historical_helper", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		api.complete()
		if got, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("historical completed helper produced authority")
		}
		if api.writerCalls != 0 {
			t.Fatal("historical helper received a fresh input")
		}
	})

	t.Run("ambiguous_owned_pods", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		other := api.fixture.pod.DeepCopy()
		other.Name = "second-helper"
		other.UID = "second-pod"
		api.extraPods = []corev1.Pod{*other}
		if got, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("ambiguous pods produced authority")
		}
		if api.writerCalls != 0 {
			t.Fatal("ambiguous pod selection reached writer")
		}
	})

	t.Run("preadmission_refusal", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		api.fixture.pvc.UID = readerReplacementUID
		if got, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("replacement admitted")
		}
		if api.writerCalls != 0 {
			t.Fatal("writer invoked before admission")
		}
	})
}
func readerWriterRefusals(t *testing.T) {
	for _, fault := range []string{"trailing", "stderr", "stdout_only", readerStatusCase, readerFaultUnknownCommit, readerFaultDisconnect} {
		t.Run("writer_"+fault, func(t *testing.T) {
			api, reader := newReaderAPI(t)
			api.writerFault = fault
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
				t.Fatal("unchecked writer produced authority")
			}
		})
	}
}
func readerPostResultRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*admissionFixture)
	}{{"saved_end", func(f *admissionFixture) { f.m.Status.Replication.Endpos = fixtureEarlierLSN }}, {"pvc", func(f *admissionFixture) { f.pvc.UID = readerReplacementUID }}, {"migration_generation", func(f *admissionFixture) { f.m.Generation++ }}, {"worker", func(f *admissionFixture) { f.worker.UID = readerReplacementUID }}, {"job", func(f *admissionFixture) { f.job.UID = readerReplacementUID }}, {"pod", func(f *admissionFixture) { f.pod.UID = readerReplacementUID }}, {"restart", func(f *admissionFixture) { f.pod.Status.ContainerStatuses[0].RestartCount++ }}} {
		t.Run("post_result_"+tc.name, func(t *testing.T) {
			api, reader := newReaderAPI(t)
			api.afterResult = tc.mutate
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
				t.Fatal("post-effect identity change produced authority")
			}
		})
	}
}
func readerCorrelationRefusals(t *testing.T) {
	for _, tc := range []struct{ name, old, next string }{{"operation", strings.Repeat("a", 32), strings.Repeat("b", 32)}, {readerRequestCase, `"request_sha256":"`, `"request_sha256":"0`}, {"program", `"program_sha256":"`, `"program_sha256":"0`}, {readerExecutionKey, `"execution_sha256":"`, `"execution_sha256":"0`}} {
		t.Run("result_"+tc.name, func(t *testing.T) {
			api, reader := newReaderAPI(t)
			api.mutateResult = func(raw []byte) []byte { return bytes.Replace(raw, []byte(tc.old), []byte(tc.next), 1) }
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
				t.Fatal("unbound result produced authority")
			}
		})
	}
}

func TestNativeWireBounds(t *testing.T) {
	w := readerWant()
	body, err := completedRequest(w, readerPodName, readerBinding(w), fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	atLimit := append(bytes.Clone(body), bytes.Repeat([]byte{' '}, nativeWireLimit-len(body))...)
	if _, err := parseCompletedRequest(atLimit); err != nil {
		t.Fatal("request at byte limit refused")
	}
	if _, err := parseCompletedRequest(append(atLimit, ' ')); err == nil {
		t.Fatal("oversized request accepted")
	}
	raw := readerResult(t, w, body)
	atLimit = append(bytes.Repeat([]byte{' '}, nativeWireLimit-len(raw)), raw...)
	if _, err := parseCompletedResult(atLimit, w, body, fixtureEndLSN); err != nil {
		t.Fatal("result at byte limit refused")
	}
	if _, err := parseCompletedResult(append([]byte{' '}, atLimit...), w, body, fixtureEndLSN); err == nil {
		t.Fatal("oversized result accepted")
	}
	deeplyNested := []byte(`{"version":` + strings.Repeat("[", 33) + `1` + strings.Repeat("]", 33) + `}`)
	if decodeNativeWire(deeplyNested, &requestWire{}, readerRequestCase) {
		t.Fatal("excess depth accepted")
	}
}

func TestNativeResultLaterBegin(t *testing.T) {
	w := readerWant()
	body, err := completedRequest(w, readerPodName, readerBinding(w), fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	raw := readerResult(t, w, body)
	raw = bytes.Replace(raw, []byte(`"drain_kind":"keepalive"`), []byte(`"drain_kind":"later_begin"`), 1)
	raw = bytes.Replace(raw, []byte(`"drain_replay_lsn":"0/20"`), []byte(`"drain_replay_lsn":"0/30"`), 1)
	raw = bytes.Replace(raw, []byte(`"certified_lsn":"0/20"`), []byte(`"certified_lsn":"0/0"`), 1)
	raw = bytes.Replace(raw, []byte(`"durable_replay_lsn":"0/20"`), []byte(readerEarlierDurableJSON), 1)
	if _, err := parseCompletedResult(raw, w, body, fixtureEndLSN); err != nil {
		t.Fatal("later-BEGIN witness was promoted into durable/certified frontier")
	}
}

func TestNativeWireRequiredMembers(t *testing.T) {
	w := readerWant()
	body, err := completedRequest(w, readerPodName, readerBinding(w), fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		raw   []byte
		paths [][]string
		parse func([]byte) error
	}{
		{readerRequestCase, body, [][]string{nil, {readerExecutionKey}, {readerExecutionKey, evidenceWork}, {"container"}}, func(raw []byte) error { _, err := parseCompletedRequest(raw); return err }},
		{"result", readerResult(t, w, body), [][]string{nil, {evidenceWork}, {"checkpoint"}}, func(raw []byte) error {
			_, err := parseCompletedResult(append(raw, '\n'), w, body, fixtureEndLSN)
			return err
		}},
	} {
		for _, path := range tc.paths {
			var root map[string]any
			if json.Unmarshal(tc.raw, &root) != nil {
				t.Fatal("invalid wire fixture")
			}
			object := root
			for _, part := range path {
				object = object[part].(map[string]any)
			}
			for key := range object {
				for _, mutation := range []string{readerCaseMissing, readerCaseNull, readerCaseFolded, readerCaseUnknown} {
					name := tc.name + "/" + strings.Join(path, ".") + "/" + key + "/" + mutation
					t.Run(name, func(t *testing.T) {
						var root map[string]any
						_ = json.Unmarshal(tc.raw, &root)
						object := root
						for _, part := range path {
							object = object[part].(map[string]any)
						}
						value := object[key]
						delete(object, key)
						switch mutation {
						case readerCaseNull:
							object[key] = nil
						case readerCaseFolded:
							object[strings.ToUpper(key)] = value
						case readerCaseUnknown:
							object[key] = value
							object["unexpected_member"] = true
						}
						raw, err := json.Marshal(root)
						if err != nil {
							t.Fatal(err)
						}
						if tc.parse(raw) == nil {
							t.Fatal("required field validation was bypassed")
						}
					})
				}
			}
		}
	}
}

type readerZeroTransport struct{}

func (readerZeroTransport) InPodNative(context.Context, string, string, podexec.NativeContainerBinding, []string, []byte) (podexec.NativeOutcome, error) {
	return podexec.NativeOutcome{}, nil
}
func (readerZeroTransport) JobCompletedNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	return podexec.NativeOutcome{}, nil
}

func TestNativeUncheckedTransportNonAuthority(t *testing.T) {
	api, reader := newReaderAPI(t)
	reader.transport = readerZeroTransport{}
	if got, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN); err == nil || got != nil {
		t.Fatal("unchecked outcome produced authority")
	}
}

func TestNativeRelayCompletionAdmission(t *testing.T) {
	for _, finishDuringExec := range []bool{false, true} {
		name := "main_running_after_relay"
		if finishDuringExec {
			name = "original_main_terminated_after_relay"
		}
		t.Run(name, func(t *testing.T) {
			api, reader := newReaderAPI(t)
			api.finishDuringExec = finishDuringExec
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN)
			if err != nil || got == nil {
				t.Fatalf("default-cluster relay completion rejected: %v", err)
			}
			if !got.Checkpoint().Evidence().BarrierConfirmed || api.writerCalls != 1 || api.logCalls != 1 || api.splitLogCalls != 0 {
				t.Fatal("completion bypassed relay or ordinary quiet-main verification")
			}
		})
	}
	for _, fault := range []string{"exit", readerFaultSignal, "failed_job", readerFaultNoise} {
		t.Run("main_"+fault, func(t *testing.T) {
			api, reader := newReaderAPI(t)
			if fault == readerFaultNoise {
				api.logNoise = []byte("unexpected startup output\n")
			} else {
				api.mainFault = fault
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
				t.Fatal("relay result alone constructed authority")
			}
			if api.writerCalls != 1 {
				t.Fatal("main failure replayed request")
			}
			if fault == readerFaultNoise && api.logCalls != 1 {
				t.Fatal("startup-noise fault did not reach ordinary log validation")
			}
		})
	}
	t.Run("relay_success_before_main_completion", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		api.blockCompletion = true
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("running main constructed authority")
		}
		if api.writerCalls != 1 || api.logCalls != 0 {
			t.Fatal("result was replayed or read before main termination")
		}
	})
	t.Run("enclosing_deadline_wins", readerRelayDeadline)
	t.Run("consumed_running_channel_refuses_reentry", readerConsumedChannelRefusal)

	t.Run("fresh_operation_reuses_persisted_end", readerFreshOperationRetry)

	t.Run("main_success_without_checked_relay", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		api.finishDuringExec = true
		api.writerFault = readerFaultDisconnect
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("main success alone constructed authority")
		}
		if api.fixture.job.Status.Succeeded != 1 || api.writerCalls != 1 {
			t.Fatal("fixture did not expose ambiguous collection after successful main")
		}
		if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("same consumed operation was reused")
		}
		if api.writerCalls != 1 {
			t.Fatal("ambiguous request was replayed")
		}
	})
}

func readerFreshOperationRetry(t *testing.T) {
	api, reader := newReaderAPI(t)
	api.finishDuringExec = true
	api.writerFault = readerFaultDisconnect
	want := api.fixture.want
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if got, err := reader.ReadCompleted(ctx, want, fixtureEndLSN); err == nil || got != nil {
		t.Fatal("ambiguous relay produced authority")
	}
	api.mu.Lock()
	firstInput := bytes.Clone(api.input)
	api.mu.Unlock()
	fresh := newAdmissionFixture(t)
	fresh.want.OperationID = strings.Repeat("b", 32)
	fresh.want.JobUID = "fresh-job"
	fresh.job.UID = fresh.want.JobUID
	fresh.pod.UID = "fresh-pod"
	fresh.pod.OwnerReferences[0].UID = fresh.want.JobUID
	fresh.pod.Labels = map[string]string{batchv1.ControllerUidLabel: string(fresh.want.JobUID)}
	command, args, _, _, _, err := expectedEvidenceMaterialization(fresh.m, fresh.want)
	if err != nil {
		t.Fatal(err)
	}
	fresh.job.Spec.Template.Spec.Containers[0].Command = command
	fresh.job.Spec.Template.Spec.Containers[0].Args = args
	fresh.pod.Spec = *fresh.job.Spec.Template.Spec.DeepCopy()
	fresh.want.TemplateSHA256 = podexec.NativeTemplateSHA256(fresh.job.Spec.Template)
	api.mu.Lock()
	api.fixture = fresh
	api.injected = false
	api.postExecReads = 0
	api.result = nil
	api.writerFault = ""
	api.finishDuringExec = false
	api.mu.Unlock()
	got, err := reader.ReadCompleted(ctx, fresh.want, fixtureEndLSN)
	if err != nil || got == nil {
		t.Fatalf("fresh independently admitted operation rejected: %v", err)
	}
	first, err := parseCompletedRequest(firstInput)
	if err != nil {
		t.Fatal(err)
	}
	second, err := parseCompletedRequest(api.input)
	if err != nil {
		t.Fatal(err)
	}
	if api.writerCalls != 2 || first.OperationID == second.OperationID || first.SavedE != fixtureEndLSN || second.SavedE != fixtureEndLSN || got.OwnedState().Work() != want.Work {
		t.Fatal("fresh retry changed saved E or reused consumed operation")
	}
}

func readerRelayDeadline(t *testing.T) {
	api, reader := newReaderAPI(t)
	api.finishDuringExec = true
	api.writerFault = "hold"
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
		t.Fatal("cancelled relay constructed authority")
	}
	if time.Since(started) > time.Second || api.writerCalls != 1 || api.logCalls != 0 {
		t.Fatal("relay exceeded enclosing deadline or retried consumed input")
	}
}

func readerConsumedChannelRefusal(t *testing.T) {
	api, reader := newReaderAPI(t)
	api.blockCompletion = true
	api.writerFault = readerFaultDisconnect
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range 2 {
		if got, err := reader.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN); err == nil || got != nil {
			t.Fatal("consumed channel constructed authority")
		}
	}
	if api.relayCalls != 2 || api.writerCalls != 1 || api.logCalls != 0 {
		t.Fatal("consumed channel accepted a second request dispatch")
	}
}
