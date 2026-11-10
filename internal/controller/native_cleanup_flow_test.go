package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	finishEndposKind          = "endpos"
	finishKeepaliveProof      = "keepalive"
	finishLaterBeginProof     = "later_begin"
	finishOwnedInventoryKind  = "owned_inventory"
	finishCleanedEvidenceKind = "cleaned_completed_evidence"
	finishProbeActiveFault    = "probe still active"
	finishUnknownCommitFault  = "completion barrier unknown"
)

const finishCleanupRejoinKind = "cleanup-rejoin"
const finishCleanupRejoinEvidence = "cleanup_rejoin_completed_evidence"

const finishWrongEndFault = "wrong cleaned E"

const (
	finishCleanedJobKind       = "cleaned"
	finishVerificationJobKind  = "verification"
	finishOrdinaryEvidenceKind = "completed_evidence"
)

const finishSlotRemovedCase = "slot removed"

const finishWrongWitnessFault = "wrong cleaned witness"
const finishChangedToken = "1:0:2"

const finishFixtureNode = "fixture-node"

const finishFixtureCaps = `{"protocol_version":1,"engine_version":"fixture","capabilities":["clone.single_db","copy.large_objects","copy.binary_fallback","copy.split","progress.v1","verify.sealed_files.v1","cdc.pgoutput","cutover.v1","cleanup.v1","verify.v1"]}` + "\n"

type nativeFinishFixture struct {
	*nativeCloneFixture
	mu                      sync.Mutex
	created, deleted, steps []string
	cleaned                 bool
	partial                 string
	laterBegin              bool
	fault                   string
	editResult              func(string, map[string]any)
	afterEffect             func()
	afterRelay              func(string)
}

func newNativeFinishFixture(t *testing.T) *nativeFinishFixture {
	t.Helper()
	f := &nativeFinishFixture{nativeCloneFixture: newNativeCloneFixture(t, true, nil)}
	f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}
	f.m.Status.Phase = v1beta1.PhaseCuttingOver
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.outputs = map[string][]byte{}
	f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errBoom
		},
		Create: f.create,
		Delete: f.delete,
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			f.patches++
			raw, err := p.Data(obj)
			if err != nil || sub != nativeCloneStatusSubresource || !strings.Contains(string(raw), `"resourceVersion"`) {
				t.Fatal("finish lost optimistic patch")
			}
			if f.patch != nil {
				return f.patch(ctx, c, obj, p)
			}
			return c.Status().Patch(ctx, obj, p, opts...)
		},
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/exec") {
			f.exec(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/pods") && r.URL.Query().Get("labelSelector") != "" {
			key, value, ok := strings.Cut(r.URL.Query().Get("labelSelector"), "=")
			if !ok {
				t.Error("invalid selector")
				return
			}
			pods := &corev1.PodList{}
			if err := f.store.List(r.Context(), pods, client.InNamespace(f.m.Namespace), client.MatchingLabels{key: value}); err != nil {
				t.Error(err)
				return
			}
			pods.ResourceVersion = "finish-snapshot"
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pods)
			return
		}
		f.serve(w, r)
	}))
	t.Cleanup(server.Close)
	transport, err := podexec.New(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	f.r.NativeJobs, f.r.NativePreflight = transport, transport
	f.r.NativeEvidence = native.NewReader(f.r.APIReader, transport)
	return f
}

func finishJobKind(name string) string {
	for _, kind := range []string{probeTestVersionCommand, "preflight", finishCleanupRejoinKind, "inventory", finishCleanedJobKind, "evidence", finishEndposKind, jobKindCleanup} {
		if strings.HasPrefix(name, "rpgmigrate-"+kind+"-") {
			return kind
		}
	}
	return finishVerificationJobKind
}

func (f *nativeFinishFixture) create(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return errBoom
	}
	f.mu.Lock()
	kind := finishJobKind(job.Name)
	f.created = append(f.created, kind)
	f.steps = append(f.steps, kind)
	job.UID = types.UID(fmt.Sprintf("finish-helper-%d", len(f.created)))
	job.Generation = 1
	f.mu.Unlock()
	defaultProbeJob(job)
	job.Status = *f.worker.Status.DeepCopy()
	pod := probePod(job)
	switch kind {
	case probeTestVersionCommand:
		f.outputs[pod.Name] = []byte(finishFixtureCaps)
		if f.fault == "empty capability profile" {
			f.outputs[pod.Name] = []byte(`{"protocol_version":1,"engine_version":"fixture","capabilities":[]}` + "\n")
		}
	case "preflight":
		f.outputs[pod.Name] = probeResult(job, true)
	case finishEndposKind:
		work, err := nativeWorkIdentity(f.m, f.pvc)
		if err != nil {
			f.t.Fatal(err)
		}
		yes, e := true, lifecycleEnd
		certified, witness, proofKind := e, e, finishKeepaliveProof
		if f.laterBegin {
			certified, witness, proofKind = nativeTerminalEarlierLSN, nativeTerminalLaterLSN, finishLaterBeginProof
		}
		ack := &native.Report{ProtocolVersion: 1, EngineVersion: "1", Command: "stream sentinel set endpos", Capabilities: []native.Capability{native.CapCutover}, Work: &work, Phase: nativeTerminalPhase, Replication: &native.Replication{CertifiedLSN: &certified, DurableReplayLSN: &certified, EndLSN: &e, EndPersisted: &yes, Drained: &yes, CheckpointID: nativeTerminalToken, DrainProof: &native.DrainProof{Kind: proofKind, Work: &work, CheckpointID: nativeTerminalToken, Committed: &yes, EndLSN: e, ReplayLSN: witness}}}
		f.outputs[pod.Name] = nativeTerminalTestAck(f.t, ack)
	case jobKindCleanup:
		f.outputs[pod.Name] = f.cleanupReport()
		f.mu.Lock()
		f.cleaned = true
		f.mu.Unlock()
		if f.afterEffect != nil {
			f.afterEffect()
		}
	case "inventory", finishCleanedJobKind, "evidence", finishCleanupRejoinKind:
		job.Status = batchv1.JobStatus{Active: 1}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		f.outputs[pod.Name] = nil
	default:
		f.outputs[pod.Name] = nativeComparePayload(f.t, f.nativeCloneFixture, compareSchema, true)
	}
	if err := c.Create(ctx, job, opts...); err != nil {
		return err
	}
	if err := c.Create(ctx, pod); err != nil {
		return err
	}
	if f.fault == "unknown cleanup Create" && kind == jobKindCleanup {
		return errBoom
	}
	return nil
}

func (f *nativeFinishFixture) cleanupReport() []byte {
	work, err := nativeWorkIdentity(f.m, f.pvc)
	if err != nil {
		f.t.Fatal(err)
	}
	yes := true
	report := &native.Report{ProtocolVersion: 1, EngineVersion: "1", Command: "stream cleanup", Capabilities: []native.Capability{native.CapCleanup}, Work: &work, Phase: nativeTerminalPhase, Cleanup: &native.Cleanup{Complete: &yes, Resources: []native.OwnedResource{{Kind: "replication_slot", Name: "owned_slot", Owned: &yes, Removed: &yes}, {Kind: "publication", Name: "owned_publication", Owned: &yes, Removed: &yes}, {Kind: workdirVolume, Name: nativeCleanupTestWorkdir, Owned: &yes, Removed: new(bool)}}}}
	raw := nativeTerminalTestAck(f.t, report)
	if f.fault == "wrong cleanup resource" {
		raw = bytes.Replace(raw, []byte("owned_slot"), []byte("foreign_slot"), 1)
	}
	if f.fault == "cleanup noise" {
		raw = append(raw, 'x')
	}
	return raw
}

func (f *nativeFinishFixture) delete(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	job, ok := obj.(*batchv1.Job)
	if !ok || job.UID == f.worker.UID {
		f.t.Error("finish deleted original evidence")
		return errBoom
	}
	options := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(options)
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != job.UID {
		f.t.Error("helper deletion lacks exact UID")
		return errBoom
	}

	if finishJobKind(job.Name) == finishVerificationJobKind {
		live := &v1beta1.Migration{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(f.m), live); err != nil || !meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionComplete) {
			f.t.Error("verification helper disposed before durable final status")
			return errBoom
		}
	}
	f.mu.Lock()
	f.deleted = append(f.deleted, finishJobKind(job.Name))
	f.mu.Unlock()
	if err := c.Delete(ctx, job, opts...); err != nil {
		return err
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods); err != nil {
		return err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if owner := metav1.GetControllerOf(p); owner != nil && owner.UID == job.UID {
			if err := c.Delete(ctx, p); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *nativeFinishFixture) exec(out http.ResponseWriter, req *http.Request) {
	ws, err := (&websocket.Upgrader{Subprotocols: []string{nativeCleanupTestProtocol}}).Upgrade(out, req, nil)
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
	var request struct {
		Kind      string `json:"kind"`
		Execution struct {
			JobName string `json:"job_name"`
		} `json:"execution"`
	}
	if err := json.Unmarshal(input, &request); err != nil {
		f.t.Error(err)
		return
	}
	var value map[string]any
	if err := json.Unmarshal(lifecycleResult(f.t, input), &value); err != nil {
		f.t.Error(err)
		return
	}
	value["kind"] = request.Kind
	f.mu.Lock()
	cleaned := f.cleaned
	f.steps = append(f.steps, request.Kind)
	f.mu.Unlock()
	success := true
	switch request.Kind {
	case finishOwnedInventoryKind:
		delete(value, "checkpoint")
		delete(value, "guards")
		value["materialization_closed"] = true
	case finishCleanedEvidenceKind:
		value["materialization_closed"], value["cleanup_complete"], value["source_resources_absent"] = true, true, true
		success = cleaned
	case finishCleanupRejoinEvidence:
		value["materialization_closed"], value["cleanup_pending"], value["source_resources_checked"] = true, true, true
		success = !cleaned && f.partial != ""
	case finishOrdinaryEvidenceKind:
		success = !cleaned && f.partial == ""
	default:
		f.t.Errorf("unexpected proof kind %q", request.Kind)
		return
	}
	if request.Kind != finishOwnedInventoryKind && f.laterBegin {
		cp := value["checkpoint"].(map[string]any)
		cp["certified_lsn"], cp["durable_replay_lsn"], cp["drain_replay_lsn"], cp["drain_kind"] = nativeTerminalEarlierLSN, nativeTerminalEarlierLSN, nativeTerminalLaterLSN, finishLaterBeginProof
	}
	if f.editResult != nil {
		f.editResult(request.Kind, value)
	}
	if f.afterRelay != nil {
		f.afterRelay(request.Kind)
	}
	job := &batchv1.Job{}
	if err := f.store.Get(req.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: request.Execution.JobName}, job); err != nil {
		f.t.Error(err)
		return
	}
	pod := &corev1.Pod{}
	if err := f.store.Get(req.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: job.Name + "-pod"}, pod); err != nil {
		f.t.Error(err)
		return
	}
	if f.fault != finishProbeActiveFault || request.Kind != finishCleanedEvidenceKind {
		job.Status = *f.worker.Status.DeepCopy()
		pod.Status.Phase = corev1.PodSucceeded
		s := &pod.Status.ContainerStatuses[0]
		s.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: s.ContainerID}}
		if !success {
			job.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
			pod.Status.Phase = corev1.PodFailed
			s.State.Terminated.ExitCode = 1
		}
		if err := f.store.Status().Update(req.Context(), job); err != nil {
			f.t.Error(err)
		}
		if err := f.store.Status().Update(req.Context(), pod); err != nil {
			f.t.Error(err)
		}
	}
	status := []byte(`{"status":"Success"}`)
	if success {
		raw, err := json.Marshal(value)
		if err != nil {
			f.t.Error(err)
			return
		}
		_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{1}, append(raw, '\n')...))
	} else {
		status = []byte(`{"status":"Failure"}`)
	}
	_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{3}, status...))
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

type nativeCleanupFlowConsumer interface {
	readNativeCleanupRejoinCompletion(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (*native.CompletedObservation, error)
	readNativeOwnedInventory(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (native.AdmittedOwnedState, error)
	ensureNativeCleanup(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (bool, error)
	readNativeCleanedCompletedEvidence(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (*native.CompletedObservation, error)
}

func cleanupFlow(t *testing.T, f *nativeFinishFixture) nativeCleanupFlowConsumer {
	t.Helper()
	consumer, ok := any(f.r).(nativeCleanupFlowConsumer)
	if !ok {
		t.Fatal("native cleanup and cleaned completion flow is missing")
	}
	return consumer
}

func TestNativeTerminalInventoryCleanup(t *testing.T) {
	f := newNativeFinishFixture(t)
	done, err := cleanupFlow(t, f).ensureNativeCleanup(context.Background(), f.m, f.worker, f.pvc)
	if err != nil || !done || !f.cleaned {
		t.Fatalf("cleanup done=%t err=%v created=%v steps=%v", done, err, f.created, f.steps)
	}
	if len(f.created) == 0 || f.patches != 0 {
		t.Fatal("cleanup skipped actual Jobs or patched completion")
	}
}

func TestNativeFollowCleanedCompletionAuthority(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.cleaned = true
	f.laterBegin = true
	observation, err := cleanupFlow(t, f).readNativeCleanedCompletedEvidence(context.Background(), f.m, f.worker, f.pvc)
	if err != nil || observation == nil {
		t.Fatalf("cleaned authority: %v created=%v steps=%v", err, f.created, f.steps)
	}
	checkpoint := observation.Checkpoint().Evidence()
	if checkpoint.DrainEndLSN != lifecycleEnd || checkpoint.CertifiedLSN != nativeTerminalEarlierLSN || checkpoint.DrainReplayLSN != nativeTerminalLaterLSN || !checkpoint.BarrierConfirmed {
		t.Fatalf("wrong target authority: %+v", checkpoint)
	}
}

func TestNativeCleanupFlowAdmission(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func(*nativeFinishFixture)
	}{
		{"missing saved E", func(f *nativeFinishFixture) { f.m.Status.Replication = nil }},
		{"wrong Job receipt", func(f *nativeFinishFixture) { f.m.Status.NativeJobUID = "foreign-job" }},
		{"half receipts", func(f *nativeFinishFixture) { f.m.Status.NativePVCUID = "" }},
		{"current suspension", func(f *nativeFinishFixture) { f.m.Spec.Suspend = true }},
		{"failed original", func(f *nativeFinishFixture) {
			f.worker.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			f := newNativeFinishFixture(t)
			change.apply(f)
			desiredStatus := *f.m.Status.DeepCopy()
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			f.m.Status = desiredStatus
			if err := f.store.Status().Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if _, err := cleanupFlow(t, f).ensureNativeCleanup(context.Background(), f.m, f.worker, f.pvc); err == nil {
				t.Fatal("unadmitted cleanup succeeded")
			}
			if len(f.created) != 0 {
				t.Fatalf("unadmitted cleanup created helper/effect: %v", f.created)
			}
		})
	}
	t.Run("nil original", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		if got, err := cleanupFlow(t, f).readNativeCleanedCompletedEvidence(context.Background(), f.m, nil, f.pvc); err == nil || got != nil {
			t.Fatal("nil worker supplied proof")
		}
	})
}

func TestNativeFollowCleanupRejoinProof(t *testing.T) {
	f := newNativeFinishFixture(t)
	f.partial = finishSlotRemovedCase
	f.laterBegin = true
	observation, err := cleanupFlow(t, f).readNativeCleanupRejoinCompletion(context.Background(), f.m, f.worker, f.pvc)
	if err != nil || observation == nil {
		t.Fatalf("partial target proof: %v", err)
	}
	checkpoint := observation.Checkpoint().Evidence()
	if checkpoint.DrainEndLSN != lifecycleEnd || checkpoint.CertifiedLSN != nativeTerminalEarlierLSN || checkpoint.DrainReplayLSN != nativeTerminalLaterLSN || f.patches != 0 || f.cleaned {
		t.Fatal("partial proof lost same-E witness or changed completion/cleanup")
	}
}
