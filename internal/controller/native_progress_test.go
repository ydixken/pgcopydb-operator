package controller

import (
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestNativeProgressMapping(t *testing.T) {
	m, _ := nativeWorkerJobInputs()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: nativeControlTestEnd, WriteLSN: nativeControlLaterEnd, ReplayLSN: nativeControlTestEnd, LagBytes: ptr.To(int64(99))}
	at := time.Unix(100, 0)
	got := nativeSample{phase: nativeProgressStreamingPhase, observedAt: at, lagBytes: ptr.To(int64(0)), writeLSN: nativeControlTestEnd, replayLSN: nativeControlTestEnd, catchUpConfirmed: true,
		progress: &native.CopyProgress{Tables: &native.Counters{Total: ptr.To(int64(7)), Done: ptr.To(int64(7))}, Bytes: &native.Counters{Done: ptr.To(int64(0))}}}
	r := &MigrationReconciler{}
	r.applyNativeProgress(m, got)
	if m.Status.Progress == nil || m.Status.Progress.TablesTotal != 7 || m.Status.Progress.TablesDone != 7 || m.Status.Progress.IndexesTotal != 0 || m.Status.Progress.BytesTotal != nil || m.Status.Progress.BytesDone == nil || !m.Status.Progress.BytesDone.IsZero() || m.Status.Progress.ObservedAt == nil || !m.Status.Progress.ObservedAt.Equal(&metav1.Time{Time: at}) {
		t.Fatal("native optional measurements or actual observation time were changed")
	}
	if m.Status.Replication.WriteLSN != nativeControlTestEnd || m.Status.Replication.ReplayLSN != nativeControlTestEnd || m.Status.Replication.Endpos != nativeControlTestEnd || m.Status.Replication.LagBytes == nil || *m.Status.Replication.LagBytes != 0 {
		t.Fatal("native mapping kept stale positions or dropped saved E")
	}
	if m.Status.Phase != v1beta1.PhaseCuttingOver || !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCaughtUp) || meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted) || meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete) {
		t.Fatal("native DATA was promoted to completion authority")
	}
	*got.lagBytes = 123
	*got.progress.Tables.Total = 123
	if *m.Status.Replication.LagBytes != 0 || m.Status.Progress.TablesTotal != 7 {
		t.Fatal("mapped status aliases mutable sample")
	}
	r.applyNativeProgress(m, nativeSample{})
	if m.Status.Progress != nil || m.Status.Replication.LagBytes != nil || m.Status.Replication.WriteLSN != "" || m.Status.Replication.ReplayLSN != "" || m.Status.Replication.Endpos != nativeControlTestEnd || meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCaughtUp).Status != metav1.ConditionUnknown {
		t.Fatal("unknown measurements retained stale progress or became zero")
	}
}

const (
	nativeReplicationTestReplay = "1/40"
	nativeReplicationTestWrite  = "0/50"
)

func TestNativeProgressPositionsMatchLag(t *testing.T) {
	m, _ := nativeWorkerJobInputs()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	r := &MigrationReconciler{}
	for _, got := range []nativeSample{
		{phase: nativeProgressStreamingPhase, lagBytes: ptr.To(int64(16)), writeLSN: nativeReplicationTestWrite, replayLSN: nativeControlTestEnd},
		{phase: nativeProgressStreamingPhase, lagBytes: ptr.To(int64(0x1_0000_0000)), writeLSN: "2/40", replayLSN: nativeReplicationTestReplay},
		{phase: nativeProgressStreamingPhase, lagBytes: ptr.To(int64(0)), writeLSN: nativeReplicationTestReplay, replayLSN: nativeReplicationTestReplay},
	} {
		r.applyNativeProgress(m, got)
		rs := m.Status.Replication
		if rs == nil || rs.LagBytes == nil {
			t.Fatal("known lag was not published")
		}
		published := nativeSample{lagBytes: rs.LagBytes, writeLSN: rs.WriteLSN, replayLSN: rs.ReplayLSN}
		if !nativeSamplePositionsConsistent(published) || rs.WriteLSN != got.writeLSN || rs.ReplayLSN != got.replayLSN || *rs.LagBytes != *got.lagBytes {
			t.Fatalf("published positions disagree with the lag the controller acts on: %+v", rs)
		}
	}
	r.applyNativeProgress(m, nativeSample{phase: nativeProgressStreamingPhase, writeLSN: nativeReplicationTestWrite, replayLSN: nativeControlTestEnd})
	if rs := m.Status.Replication; rs.LagBytes != nil || rs.WriteLSN != "" || rs.ReplayLSN != "" {
		t.Fatal("positions without a known lag were published")
	}
}

func TestNativeProgressHasNoSyntheticObservation(t *testing.T) {
	for _, got := range []nativeSample{
		{phase: nativeProgressStreamingPhase, observedAt: time.Unix(100, 0), progress: &native.CopyProgress{}},
		{phase: nativeTerminalPhase, progress: &native.CopyProgress{Tables: &native.Counters{Done: ptr.To(int64(1))}}},
	} {
		m, _ := nativeWorkerJobInputs()
		m.Status.Phase = v1beta1.PhaseCloning
		(&MigrationReconciler{}).applyNativeProgress(m, got)
		if m.Status.Progress != nil || m.Status.CompletedAt != nil || m.Status.Phase == v1beta1.PhaseCompleted || meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete) {
			t.Fatal("unmeasured DATA acquired an observation or terminal authority")
		}
	}
}

func TestNativeProgressManualCatchUp(t *testing.T) {
	m, _ := nativeWorkerJobInputs()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	m.Spec.Cutover.Mode = v1beta1.CutoverManual
	m.Spec.Cutover.Approved = true
	r := &MigrationReconciler{}
	r.applyNativeProgress(m, nativeSample{phase: nativeProgressStreamingPhase, lagBytes: ptr.To(int64(0))})
	if meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCaughtUp) || cutoverWanted(m, false) || m.Status.Phase == v1beta1.PhaseCuttingOver {
		t.Fatal("manual approval bypassed two fresh catch-up observations")
	}
	r.applyNativeProgress(m, nativeSample{phase: nativeProgressStreamingPhase, lagBytes: ptr.To(int64(0)), catchUpConfirmed: true})
	if m.Status.Phase != v1beta1.PhaseCutoverPending || !cutoverWanted(m, true) {
		t.Fatal("confirmed native catch-up did not reach policy")
	}
}
