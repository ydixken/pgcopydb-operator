package native

import (
	"context"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNativeCleanupRejoinAdmission(t *testing.T) {
	for _, completedDuringExec := range []bool{false, true} {
		api, r, w := newCleanupRejoinReaderAPI(t)
		api.finishDuringExec = completedDuringExec
		got, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN)
		if err != nil || got == nil || got.Checkpoint().Evidence().DrainEndLSN != fixtureEndLSN || got.OwnedState().Work() != w.Work {
			t.Fatalf("fresh cleaned authority refused: %v", err)
		}
		if api.writerCalls != 1 || api.logCalls != 1 || api.splitLogCalls != 0 {
			t.Fatal("checked relay or quiet original main missing")
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*retryFixture)
	}{
		{"rejoin_worker_receipt_missing", func(f *retryFixture) { f.base.m.Status.NativeJobUID = "" }},
		{"rejoin_pvc_receipt_missing", func(f *retryFixture) { f.base.m.Status.NativePVCUID = "" }},
		{"rejoin_worker_receipt_foreign", func(f *retryFixture) { f.base.m.Status.NativeJobUID = readerReplacementUID }},
		{"rejoin_pvc_receipt_foreign", func(f *retryFixture) { f.base.m.Status.NativePVCUID = readerReplacementUID }},
		{"rejoin_suspended_intent", func(f *retryFixture) { f.base.m.Spec.Suspend = true }},
		{"rejoin_deleting", func(f *retryFixture) { now := metav1.Now(); f.base.m.DeletionTimestamp = &now }},
		{"rejoin_wrong_end", func(f *retryFixture) { f.base.m.Status.Replication.Endpos = rejoinDifferentEnd }},
		{"rejoin_failed_original", func(f *retryFixture) { f.base.worker.Status.Succeeded = 0; f.base.worker.Status.Failed = 1 }},
		{"rejoin_pod_restarted", func(f *retryFixture) { f.workerPod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"rejoin_helper_role", func(f *retryFixture) { f.want.Role = JobRoleCleanup }},
		{"rejoin_ordinary_program", func(f *retryFixture) { f.want.ProgramSHA256 = EvidenceProgramSHA256() }},
		{"rejoin_helper_spec", func(f *retryFixture) { f.base.job.Spec.Template.Spec.Containers[0].Args[0] = "rejoin-foreign-spec" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, r, _ := newCleanupRejoinReaderAPI(t)
			f := newCleanupRejoinFixture(t)
			tc.edit(&f)
			api.fixture, api.extraPods = f.base, []corev1.Pod{*f.workerPod}
			w := f.want
			got, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN)
			if err == nil || got != nil || api.writerCalls != 0 || api.logCalls != 0 {
				t.Fatal("inadmissible original execution reached transport")
			}
		})
	}
	for _, phase := range []v1beta1.MigrationPhase{v1beta1.PhaseCompleted, v1beta1.PhaseFailed, v1beta1.PhaseCuttingOver} {
		t.Run("stale_display_"+string(phase), func(t *testing.T) {
			api, r, w := newCleanupRejoinReaderAPI(t)
			api.fixture.m.Status.Phase = phase
			api.fixture.m.Status.Conditions = nil
			if got, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN); err != nil || got == nil {
				t.Fatalf("retained actual completion refused because display status is stale: %v", err)
			}
		})
	}
	for _, mode := range []string{"missing_original_pod", "duplicate_original_pod", "duplicate_helper_pod", "foreign_original_owner"} {
		t.Run(mode, func(t *testing.T) {
			api, r, w := newCleanupRejoinReaderAPI(t)
			switch mode {
			case "missing_original_pod":
				api.extraPods = nil
			case "duplicate_original_pod":
				api.extraPods = append(api.extraPods, *api.extraPods[0].DeepCopy())
			case "duplicate_helper_pod":
				api.extraPods = append(api.extraPods, *api.fixture.pod.DeepCopy())
			case "foreign_original_owner":
				api.extraPods[0].OwnerReferences[0].UID = readerReplacementUID
			}
			if got, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN); err == nil || got != nil || api.writerCalls != 0 || api.logCalls != 0 {
				t.Fatal("incomplete or ambiguous original execution reached transport")
			}
		})
	}
	api, r, w := newCleanupRejoinReaderAPI(t)
	r.transport = readerZeroTransport{}
	if got, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN); err == nil || got != nil || api.writerCalls != 0 {
		t.Fatal("unchecked transport manufactured partial-cleanup authority")
	}
}

func TestNativeCleanupRejoinCrashRecovery(t *testing.T) {
	for _, replacement := range []string{"rejoin_worker_uid", "rejoin_pod_replaced", "rejoin_pvc_replaced", "rejoin_receipt", "rejoin_end_changed", "rejoin_spec", "rejoin_suspend"} {
		t.Run(replacement, func(t *testing.T) {
			api, r, w := newCleanupRejoinReaderAPI(t)
			api.afterResult = func(f *admissionFixture) {
				switch replacement {
				case "rejoin_worker_uid":
					f.worker.UID = readerReplacementUID
				case "rejoin_pod_replaced":
					api.extraPods[0].UID = readerReplacementUID
				case "rejoin_pvc_replaced":
					f.pvc.UID = readerReplacementUID
				case "rejoin_receipt":
					f.m.Status.NativeJobUID = readerReplacementUID
				case "rejoin_end_changed":
					f.m.Status.Replication.Endpos = rejoinDifferentEnd
				case "rejoin_spec":
					api.extraPods[0].Spec.Containers[0].Args = []string{"rejoin-foreign-spec"}
				case "rejoin_suspend":
					f.m.Spec.Suspend = true
				}
			}
			if got, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN); err == nil || got != nil {
				t.Fatal("post-relay replacement manufactured completion")
			}
		})
	}
	for _, fault := range []string{rejoinFailedMain, evidenceExit, readerFaultSignal, readerFaultNoise, "rejoin_relay"} {
		t.Run(fault, func(t *testing.T) {
			api, r, w := newCleanupRejoinReaderAPI(t)
			switch fault {
			case readerFaultNoise:
				api.logNoise = []byte("noise\n")
			case "rejoin_relay":
				api.writerFault = sourceTrailingCase
			default:
				api.mainFault = fault
			}
			if got, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN); err == nil || got != nil {
				t.Fatal("unknown transport/main closure became completion")
			}
		})
	}
	api, r, w := newCleanupRejoinReaderAPI(t)
	api.fixture.m.Status.Phase = v1beta1.PhaseCuttingOver
	api.fixture.m.Status.Conditions = nil
	if got, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN); err != nil || got == nil {
		t.Fatalf("same-E retained crash rejoin depended on old success status: %v", err)
	}
	if strings.Contains(string(api.input), "nativeJobUID") {
		t.Fatal("status receipts became proof hints")
	}
}
