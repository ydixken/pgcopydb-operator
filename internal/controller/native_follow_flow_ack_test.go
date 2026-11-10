package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

const (
	nativeFollowAckGeneration     = "ACK helper generation"
	nativeFollowAckIncompleteCopy = "incomplete clone DATA"
	nativeFollowAckPodReplaced    = "retained Pod replacement"
	nativeFollowAckNoTargetReader = "raw ACK only"
	nativeFollowAckReceiptAbsent  = "receipt absent"
	nativeFollowAckWorkerActive   = "retained worker active"
)

type nativeFollowAckReader struct {
	*podexec.Exec
	reads   int
	helper  *batchv1.Job
	pod     *corev1.Pod
	deleted bool
}

func (r *nativeFollowAckReader) JobCloneNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	return podexec.NativeOutcome{}, errBoom
}

func (r *nativeFollowAckReader) JobEndposNative(ctx context.Context, namespace string, uid types.UID, binding podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	r.reads++
	return r.Exec.JobEndposNative(ctx, namespace, uid, binding)
}

func nativeFollowAckFixture(t *testing.T, laterBegin bool) (*lifecycleAPI, *MigrationReconciler, *native.Report, *native.Report, *nativeFollowAckReader) {
	t.Helper()
	s, r, clone, ack := nativeTerminalTestFixture(t)
	s.worker.UID, s.worker.Generation = nativeFollowFlowWorkerUID, 1
	s.extraPods = []corev1.Pod{*nativeRecoveryTestPod(s.worker, 0)}
	s.migration.Status.NativeJobUID, s.migration.Status.NativePVCUID = string(s.worker.UID), string(s.pvc.UID)
	var jobs *nativeFollowAckReader
	if laterBegin {
		certified, witness := nativeTerminalEarlierLSN, nativeTerminalLaterLSN
		ack.Replication.CertifiedLSN, ack.Replication.DurableReplayLSN = &certified, &certified
		ack.Replication.DrainProof.Kind, ack.Replication.DrainProof.ReplayLSN = "later_begin", witness
	}
	s.onCreate = func(api *lifecycleAPI) {
		api.logNoise = nil
		if strings.HasPrefix(api.job.Name, "rpgmigrate-endpos-") {
			api.job.Generation = 1
			defaultProbeJob(api.job)
			api.job.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
			api.pod = probePod(api.job)
			jobs.helper, jobs.pod, jobs.deleted = api.job, api.pod, false
			api.logNoise = nativeTerminalTestAck(t, ack)
		}
	}
	prior := s.Config.Handler
	s.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodDelete && jobs.helper != nil && strings.HasSuffix(request.URL.Path, "/jobs/"+jobs.helper.Name) {
			s.mu.Lock()
			defer s.mu.Unlock()
			options, err := lifecycleDeleteOptions(request)
			if err != nil || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != jobs.helper.UID {
				http.Error(w, "unknown ACK deletion", http.StatusForbidden)
				return
			}
			jobs.deleted = true
			_ = json.NewEncoder(w).Encode(metav1.Status{Status: metav1.StatusSuccess})
			return
		}
		if request.Method == http.MethodGet {
			s.mu.Lock()
			if jobs.helper != nil && strings.HasSuffix(request.URL.Path, "/jobs/"+jobs.helper.Name) {
				if jobs.deleted {
					http.NotFound(w, request)
					s.mu.Unlock()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(jobs.helper)
				s.mu.Unlock()
				return
			}
			if jobs.pod != nil && strings.HasSuffix(request.URL.Path, "/pods/"+jobs.pod.Name) {
				w.Header().Set("Content-Type", "application/json")
				if jobs.deleted {
					http.NotFound(w, request)
					s.mu.Unlock()
					return
				}
				_ = json.NewEncoder(w).Encode(jobs.pod)
				s.mu.Unlock()
				return
			}
			if strings.HasSuffix(request.URL.Path, "/pods") {
				w.Header().Set("Content-Type", "application/json")
				pods := &corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "follow-ack-snapshot"}}
				if request.URL.Query().Get("labelSelector") == "" {
					pods.Items = append(pods.Items, s.extraPods...)
				}
				if s.pod != nil && !s.jobDeleted {
					pods.Items = append(pods.Items, *s.pod.DeepCopy())
				}
				if s.discoveryPod != nil {
					pods.Items = append(pods.Items, *s.discoveryPod.DeepCopy())
				}
				if request.URL.Query().Get("labelSelector") == "" && jobs.pod != nil && !jobs.deleted && jobs.pod != s.pod {
					pods.Items = append(pods.Items, *jobs.pod.DeepCopy())
				}
				_ = json.NewEncoder(w).Encode(pods)
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
		}
		if laterBegin && strings.HasSuffix(request.URL.Path, "/exec") {
			nativeFollowLaterBeginExec(t, s, w, request)
			return
		}
		prior.ServeHTTP(w, request)
	})
	transport, err := podexec.New(&rest.Config{Host: s.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	jobs = &nativeFollowAckReader{Exec: transport}
	r.NativeJobs = jobs
	return s, r, clone, ack, jobs
}

func nativeFollowLaterBeginExec(t *testing.T, s *lifecycleAPI, w http.ResponseWriter, request *http.Request) {
	t.Helper()
	conn, err := (&websocket.Upgrader{Subprotocols: []string{nativeCleanupTestProtocol}}).Upgrade(w, request, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() { _ = conn.Close() }()
	var input []byte
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Equal(raw, []byte{255, 0}) {
			break
		}
		if len(raw) == 0 || raw[0] != 0 {
			t.Error("unexpected relay input")
			return
		}
		input = append(input, raw[1:]...)
	}
	s.mu.Lock()
	s.relays++
	s.injected = true
	s.inputs = append(s.inputs, bytes.Clone(input))
	s.mu.Unlock()
	result := lifecycleResult(t, input)
	result = bytes.Replace(result, []byte(`"certified_lsn":"0/20"`), []byte(`"certified_lsn":"0/10"`), 1)
	result = bytes.Replace(result, []byte(`"durable_replay_lsn":"0/20"`), []byte(`"durable_replay_lsn":"0/10"`), 1)
	result = bytes.Replace(result, []byte(`"drain_replay_lsn":"0/20"`), []byte(`"drain_replay_lsn":"0/30"`), 1)
	result = bytes.Replace(result, []byte(`"drain_kind":"keepalive"`), []byte(`"drain_kind":"later_begin"`), 1)
	if conn.WriteMessage(websocket.BinaryMessage, append([]byte{1}, result...)) != nil {
		t.Error("relay fixture write failed")
		return
	}
	if conn.WriteMessage(websocket.BinaryMessage, []byte("\x03{\"status\":\"Success\"}")) != nil {
		t.Error("relay fixture status failed")
		return
	}
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

func testNativeAckLifecycleFresh(t *testing.T) {
	t.Helper()
	s, r, _, _, jobs := nativeFollowAckFixture(t, false)
	for range 2 {
		raw, err := r.readNativeEndposAck(context.Background(), s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy())
		if err != nil || len(raw) == 0 {
			t.Fatal("terminal ACK capture refused", err)
		}
	}
	if jobs.reads != 2 || s.creates != 2 || s.relays != 0 || s.created[0].Name == s.created[1].Name || s.otherWrites != 0 {
		t.Fatal("ACK adopted helper state or invoked proof/legacy effects")
	}
	for _, j := range s.created {
		c := j.Spec.Template.Spec.Containers[0]
		if j.Name == s.worker.Name || !strings.HasPrefix(j.Name, "rpgmigrate-endpos-") || !bytes.Contains([]byte(strings.Join(c.Args, " ")), []byte(lifecycleEnd)) {
			t.Fatal("ACK lost fresh identity or saved E")
		}
		for _, env := range c.Env {
			if env.Name == probeInlineSourceEnv {
				t.Fatal("ACK projected source credentials")
			}
		}
	}
}

func TestNativeAckLifecycle(t *testing.T) {
	t.Run("fresh target-only distinct terminal helper", testNativeAckLifecycleFresh)
	for _, fault := range []string{"lost create", "shared helper UID", nativeFollowAckReceiptAbsent, "terminal output unknown", nativeFollowAckPodReplaced, nativeFollowAckGeneration, nativeFollowAckWorkerActive} {
		t.Run(fault, func(t *testing.T) {
			s, r, _, _, jobs := nativeFollowAckFixture(t, false)
			switch fault {
			case "lost create":
				s.createFault = "unknown-create"
			case "shared helper UID":
				prior := s.onCreate
				s.onCreate = func(api *lifecycleAPI) { prior(api); api.job.UID = api.worker.UID }
			case nativeFollowAckReceiptAbsent:
				s.migration.Status.NativeJobUID = ""
			case "terminal output unknown":
				s.logNoise = nil
				prior := s.onCreate
				s.onCreate = func(api *lifecycleAPI) { prior(api); api.job.Status.Failed = 1 }
			case nativeFollowAckPodReplaced:
				s.afterLogs = func(api *lifecycleAPI) { api.extraPods[0].UID = workerReplacementUID }
			case nativeFollowAckGeneration:
				s.afterLogs = func(api *lifecycleAPI) { api.pod.Generation++ }
			case nativeFollowAckWorkerActive:
				s.worker.Status = batchv1.JobStatus{Active: 1}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			raw, err := r.readNativeEndposAck(ctx, s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy())
			if err == nil || raw != nil || s.creates > 1 || s.relays != 0 || s.otherWrites != 0 {
				t.Fatal("unknown ACK execution gained bytes or authority", err)
			}
			if (fault == nativeFollowAckReceiptAbsent || fault == nativeFollowAckWorkerActive) && (jobs.reads != 0 || s.creates != 0) {
				t.Fatal("missing authority reached transport")
			}
			if (fault == nativeFollowAckPodReplaced || fault == nativeFollowAckGeneration) && (jobs.reads != 1 || s.afterLogs != nil) {
				t.Fatal("replacement fixture did not reach checked ACK capture")
			}
		})
	}
}

func TestNativeFollowTargetCompletion(t *testing.T) {
	for _, laterBegin := range []bool{false, true} {
		t.Run(map[bool]string{false: "keepalive barrier", true: "later-BEGIN C below E"}[laterBegin], func(t *testing.T) {
			s, r, clone, _, jobs := nativeFollowAckFixture(t, laterBegin)
			got, err := r.readNativeFollowTargetCompletion(context.Background(), s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy(), clone)
			if err != nil || got == nil || !got.Checkpoint().Evidence().BarrierConfirmed || jobs.reads != 1 || s.relays != 1 || s.creates != 2 {
				t.Fatalf("fresh ACK/target join refused: %v reads=%d relays=%d creates=%d", err, jobs.reads, s.relays, s.creates)
			}
			if laterBegin && got.Checkpoint().Evidence().CertifiedLSN != nativeTerminalEarlierLSN {
				t.Fatal("later-BEGIN raised certified frontier")
			}
			if s.migration.Status.Phase == v1beta1.PhaseCompleted || s.otherWrites != 0 {
				t.Fatal("proof join published completion")
			}
		})
	}
	for _, fault := range []string{nativeFollowAckNoTargetReader, "mismatched ACK", "unknown target commit", nativeFollowAckIncompleteCopy, "ACK helper replaced during target Reader", "ACK helper missing during target Reader", "original Pod changed during target Reader"} {
		t.Run(fault, func(t *testing.T) {
			s, r, clone, ack, jobs := nativeFollowAckFixture(t, false)
			switch fault {
			case nativeFollowAckNoTargetReader:
				r.NativeEvidence = nil
			case "mismatched ACK":
				ack.Replication.CheckpointID = "1:0:9"
				ack.Replication.DrainProof.CheckpointID = "1:0:9"
			case "unknown target commit":
				s.relayFault = lifecycleUnknownCommit
			case nativeFollowAckIncompleteCopy:
				clone.Copy.Tables.Done = nil
			case "ACK helper replaced during target Reader":
				s.duringRelay = func(*lifecycleAPI) { jobs.helper.UID = workerReplacementUID }
			case "ACK helper missing during target Reader":
				s.duringRelay = func(*lifecycleAPI) { jobs.helper = nil }
			case "original Pod changed during target Reader":
				s.duringRelay = func(api *lifecycleAPI) { api.extraPods[0].UID = workerReplacementUID }
			}
			got, err := r.readNativeFollowTargetCompletion(context.Background(), s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy(), clone)
			if got != nil || err == nil || s.otherWrites != 0 {
				t.Fatal("unjoined DATA became target completion authority")
			}
			if fault != nativeFollowAckNoTargetReader && fault != nativeFollowAckIncompleteCopy && s.relays != 1 {
				t.Fatal("target failure fixture did not reach the real Reader")
			}
		})
	}
}
