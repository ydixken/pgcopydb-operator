package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	nativeTerminalCloneCommand = "clone"
	nativeTerminalPhase        = "completed"
	nativeTerminalToken        = "1:0:1"
	nativeTerminalEarlierLSN   = "0/10"
	nativeTerminalLaterLSN     = "0/30"
)

type nativeTerminalTestReader struct {
	client.Reader
	s *lifecycleAPI
}

func (r nativeTerminalTestReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && key.Name == r.s.extraPods[0].Name {
		r.s.extraPods[0].DeepCopyInto(pod)
		return nil
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r nativeTerminalTestReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	list.(*corev1.PodList).ResourceVersion = "terminal-snapshot"
	return nil
}

func nativeTerminalTestFixture(t *testing.T) (*lifecycleAPI, *MigrationReconciler, *native.Report, *native.Report) {
	t.Helper()
	s, r := newLifecycleAPI(t)
	s.migration.ResourceVersion = "1"
	s.migration.Status.Phase = v1beta1.PhaseCuttingOver
	j, err := buildNativeWorkerJob(s.migration, s.pvc, r.RPGMigrateRunnerImage, 1)
	if err != nil || controllerutil.SetControllerReference(s.migration, j, r.Scheme) != nil {
		t.Fatalf("build terminal worker: %v", err)
	}
	j.UID, j.Status = s.worker.UID, s.worker.Status
	s.worker = j
	s.migration.Status.NativeJobUID, s.migration.Status.NativePVCUID = string(j.UID), string(s.pvc.UID)
	s.extraPods = []corev1.Pod{*nativeRecoveryTestPod(j, 0)}
	originalHandler := s.Config.Handler
	s.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/pods") && request.URL.Query().Get("labelSelector") != "" {
			s.mu.Lock()
			defer s.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			list := &corev1.PodList{}
			if s.pod != nil {
				list.Items = []corev1.Pod{*s.pod.DeepCopy()}
			}
			_ = json.NewEncoder(w).Encode(list)
			return
		}
		originalHandler.ServeHTTP(w, request)
	})
	r.APIReader = nativeTerminalTestReader{Reader: r.APIReader, s: s}
	work, err := nativeWorkIdentity(s.migration, s.pvc)
	if err != nil {
		t.Fatal(err)
	}
	one, yes, e := int64(1), true, lifecycleEnd
	clone := &native.Report{ProtocolVersion: 1, EngineVersion: "1", Command: nativeTerminalCloneCommand, Capabilities: []native.Capability{native.CapCloneSingleDB}, Work: &work, Phase: nativeTerminalPhase, Copy: &native.CopyProgress{Tables: &native.Counters{Total: &one, Done: &one}, Indexes: &native.Counters{Total: &one, Done: &one}, Bytes: &native.Counters{Total: &one, Done: &one}}}
	ack := &native.Report{ProtocolVersion: 1, EngineVersion: "1", Command: "stream sentinel set endpos", Capabilities: []native.Capability{native.CapCutover}, Work: &work, Phase: nativeTerminalPhase, Replication: &native.Replication{CertifiedLSN: &e, DurableReplayLSN: &e, EndLSN: &e, EndPersisted: &yes, Drained: &yes, CheckpointID: nativeTerminalToken, DrainProof: &native.DrainProof{Kind: "keepalive", Work: &work, CheckpointID: nativeTerminalToken, Committed: &yes, EndLSN: e, ReplayLSN: e}}}
	return s, r, clone, ack
}

func nativeTerminalTestAck(t *testing.T, report *native.Report) []byte {
	t.Helper()
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]byte("native-report: "), raw...), '\n')
}

func TestNativeFinalProofJoin(t *testing.T) {
	t.Run("fresh existing Reader is the only checkpoint authority", func(t *testing.T) {
		s, r, clone, ack := nativeTerminalTestFixture(t)
		before := s.migration.DeepCopy()
		if _, err := r.nativeQuiescentWorker(context.Background(), s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy()); err != nil {
			t.Fatalf("terminal fixture is not quiescent: %v", err)
		}
		got, err := r.readNativeFollowCompletion(context.Background(), s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy(), clone, nativeTerminalTestAck(t, ack))
		if err != nil || got == nil || !got.Checkpoint().Evidence().BarrierConfirmed || s.relays != 1 {
			t.Fatalf("actual checked Reader join failed: %v, relays=%d logs=%t postReads=%d", err, s.relays, s.logsRead, s.postExecReads)
		}
		if s.migration.Status.Replication.Endpos != before.Status.Replication.Endpos || len(s.migration.Status.Conditions) != len(before.Status.Conditions) || s.otherWrites != 0 {
			t.Fatal("proof read changed E or published completion")
		}
	})
	for _, tc := range []struct {
		name   string
		change func(*native.Report)
	}{
		{"not persisted", func(r *native.Report) { no := false; r.Replication.EndPersisted = &no }},
		{"not drained", func(r *native.Report) { no := false; r.Replication.Drained = &no; r.Replication.DrainProof = nil }},
		{"different token", func(r *native.Report) {
			r.Replication.CheckpointID = "1:0:2"
			r.Replication.DrainProof.CheckpointID = "1:0:2"
		}},
		{"different certified", func(r *native.Report) { e := nativeTerminalEarlierLSN; r.Replication.CertifiedLSN = &e }},
		{"different durable", func(r *native.Report) { e := nativeTerminalLaterLSN; r.Replication.DurableReplayLSN = &e }},
		{"different end", func(r *native.Report) {
			e := nativeTerminalEarlierLSN
			r.Replication.EndLSN = &e
			r.Replication.DrainProof.EndLSN = e
		}},
		{"different witness", func(r *native.Report) { r.Replication.DrainProof.ReplayLSN = nativeTerminalEarlierLSN }},
		{"different kind", func(r *native.Report) {
			r.Replication.DrainProof.Kind = "later_begin"
			r.Replication.DrainProof.ReplayLSN = nativeTerminalLaterLSN
		}},
		{"foreign work", func(r *native.Report) { r.Replication.DrainProof.Work = &native.WorkIdentity{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, clone, ack := nativeTerminalTestFixture(t)
			tc.change(ack)
			if got, err := r.readNativeFollowCompletion(context.Background(), s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy(), clone, nativeTerminalTestAck(t, ack)); got != nil || err == nil || strings.Contains(err.Error(), lifecycleCanary) {
				t.Fatal("mismatched acknowledgment supplied completion authority")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*lifecycleAPI, *MigrationReconciler, *native.Report)
	}{
		{"raw acknowledgment alone", func(_ *lifecycleAPI, r *MigrationReconciler, _ *native.Report) { r.NativeEvidence = nil }},
		{"unknown Reader commit", func(s *lifecycleAPI, _ *MigrationReconciler, _ *native.Report) { s.relayFault = lifecycleUnknownCommit }},
		{"missing clone counters", func(_ *lifecycleAPI, _ *MigrationReconciler, clone *native.Report) { clone.Copy.Bytes.Done = nil }},
		{"failed original worker", func(s *lifecycleAPI, _ *MigrationReconciler, _ *native.Report) {
			s.worker.Status = batchv1.JobStatus{Failed: 1}
		}},
		{"active original container", func(s *lifecycleAPI, _ *MigrationReconciler, _ *native.Report) {
			s.extraPods[0].Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{"worker Pod replaced during Reader", func(s *lifecycleAPI, _ *MigrationReconciler, _ *native.Report) {
			s.afterLogs = func(s *lifecycleAPI) { s.extraPods[0].UID = workerReplacementUID }
		}},
		{"saved E changed during Reader", func(s *lifecycleAPI, _ *MigrationReconciler, _ *native.Report) {
			s.afterLogs = func(s *lifecycleAPI) { s.migration.Status.Replication.Endpos = lifecycleChangedEnd }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, clone, ack := nativeTerminalTestFixture(t)
			tc.change(s, r, clone)
			if got, err := r.readNativeFollowCompletion(context.Background(), s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy(), clone, nativeTerminalTestAck(t, ack)); got != nil || err == nil {
				t.Fatal("incomplete or replaced execution supplied completion authority")
			}
		})
	}
	t.Run("complete bounded framing is required", func(t *testing.T) {
		for _, raw := range [][]byte{nil, []byte("native-report: {}"), []byte("noise\nnative-report: {}\n"), []byte(strings.Repeat("x", 65537))} {
			s, r, clone, _ := nativeTerminalTestFixture(t)
			if got, err := r.readNativeFollowCompletion(context.Background(), s.migration, s.worker, s.pvc, clone, raw); got != nil || err == nil || s.relays != 0 {
				t.Fatal("unbounded or incomplete capture reached proof Reader")
			}
		}
	})
}
