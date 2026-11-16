package controller

import (
	"context"
	"slices"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func requireReplayGauge(t *testing.T, m *v1beta1.Migration, want string) {
	t.Helper()
	labels := map[string]string{testNamespaceLabel: m.Namespace, lifecycleNameKey: m.Name}
	got, found, err := readGauge("pgcopydb_migration_replay_lsn_bytes", labels)
	lsn, perr := sentinel.ParseLSN(want)
	if err != nil || perr != nil || !found || got != float64(lsn) {
		t.Fatalf("replay gauge = %v (present %v), want %s: %v %v", got, found, want, err, perr)
	}
}

// The M7-RUN6 run-02 and run-04 positions: the last pre-endpos sample stood 56 bytes short
// of endpos, and the keepalive drain proof certified endpos itself.
func TestRaiseNativeDrainReplayPositions(t *testing.T) {
	for _, tc := range []struct{ run, end, frozen string }{
		{"run-02", "0/A4B82A8", "0/A4B8270"},
		{"run-04", "0/A4B82C8", "0/A4B8290"},
	} {
		t.Run(tc.run, func(t *testing.T) {
			lag := int64(56)
			m := &v1beta1.Migration{
				ObjectMeta: metav1.ObjectMeta{Namespace: "drain-replay", Name: tc.run},
				Spec:       v1beta1.MigrationSpec{Engine: v1beta1.MigrationEngineRPGMigrate},
				Status: v1beta1.MigrationStatus{Phase: v1beta1.PhaseCuttingOver, Replication: &v1beta1.ReplicationStatus{
					Endpos: tc.end, WriteLSN: tc.end, ReplayLSN: tc.frozen, LagBytes: &lag,
				}},
			}
			t.Cleanup(func() { metrics.Forget(m.Namespace, m.Name) })
			metrics.Record(m)
			requireReplayGauge(t, m, tc.frozen)

			for _, refused := range []string{"", "not an LSN", "0/A4B8200", tc.frozen} {
				if raiseNativeDrainReplay(m, native.CheckpointEvidence{DurableReplayLSN: refused}) || m.Status.Replication.ReplayLSN != tc.frozen {
					t.Fatalf("durable %q moved replay to %s", refused, m.Status.Replication.ReplayLSN)
				}
			}
			if !raiseNativeDrainReplay(m, native.CheckpointEvidence{DurableReplayLSN: tc.end}) || m.Status.Replication.ReplayLSN != tc.end {
				t.Fatalf("verified proof left replay at %s", m.Status.Replication.ReplayLSN)
			}
			metrics.Record(m)
			requireReplayGauge(t, m, tc.end)
			if raiseNativeDrainReplay(m, native.CheckpointEvidence{DurableReplayLSN: tc.frozen}) || m.Status.Replication.ReplayLSN != tc.end {
				t.Fatalf("an earlier proof moved replay backwards to %s", m.Status.Replication.ReplayLSN)
			}

			unsampled := &v1beta1.Migration{Status: v1beta1.MigrationStatus{Replication: &v1beta1.ReplicationStatus{Endpos: tc.end}}}
			if raiseNativeDrainReplay(unsampled, native.CheckpointEvidence{DurableReplayLSN: tc.end}) || unsampled.Status.Replication.ReplayLSN != "" {
				t.Fatal("an attempt without a replay sample gained one")
			}
			if raiseNativeDrainReplay(&v1beta1.Migration{}, native.CheckpointEvidence{DurableReplayLSN: tc.end}) {
				t.Fatal("a Migration without replication status was raised")
			}
		})
	}
}

// nativeDrainFrozenReplay stands 8 bytes short of lifecycleEnd, above the later_begin durable replay.
const nativeDrainFrozenReplay = "0/18"

func newNativeDrainReplayFixture(t *testing.T) *nativeFinishFixture {
	t.Helper()
	f := newNativeFinishFixture(t)
	lag := int64(8)
	f.m.Status.Replication.WriteLSN, f.m.Status.Replication.ReplayLSN, f.m.Status.Replication.LagBytes = lifecycleEnd, nativeDrainFrozenReplay, &lag
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metrics.Forget(f.m.Namespace, f.m.Name) })
	metrics.Record(f.m)
	requireReplayGauge(t, f.m, nativeDrainFrozenReplay)
	return f
}

func storedMigration(t *testing.T, f *nativeFinishFixture) *v1beta1.Migration {
	t.Helper()
	stored := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored); err != nil {
		t.Fatal(err)
	}
	return stored
}

func TestNativeFollowFinishRecordsProvenReplay(t *testing.T) {
	const frozen = nativeDrainFrozenReplay
	t.Run("verified keepalive proof is written alone while cutting over", func(t *testing.T) {
		f := newNativeDrainReplayFixture(t)
		result, err := nativeFinish(t, f)
		stored := storedMigration(t, f)
		if err != nil || result.RequeueAfter != pollInterval || f.patches != 1 || stored.Status.Phase != v1beta1.PhaseCuttingOver || stored.Status.Replication.ReplayLSN != lifecycleEnd {
			t.Fatalf("proof replay write: %v %+v patches=%d status=%+v", err, result, f.patches, stored.Status)
		}
		if f.cleaned || slices.Contains(f.steps, jobKindCleanup) || stored.Status.CompletedAt != nil {
			t.Fatalf("the replay write fell through to completion: steps=%v", f.steps)
		}
		requireReplayGauge(t, stored, lifecycleEnd)

		f.m, f.steps = stored, nil
		if _, err := nativeFinish(t, f); err != nil {
			t.Fatalf("finish after the replay write: %v steps=%v", err, f.steps)
		}
		assertNativeFinished(t, f)
		if got := storedMigration(t, f).Status.Replication.ReplayLSN; got != lifecycleEnd || f.patches != 2 {
			t.Fatalf("completion replay = %s after %d patches", got, f.patches)
		}
	})
	t.Run("verification starts only on the pass after the replay write", func(t *testing.T) {
		f := newNativeDrainReplayFixture(t)
		f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
		if err := f.store.Update(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		result, err := nativeFinish(t, f)
		stored := storedMigration(t, f)
		if err != nil || result.RequeueAfter != pollInterval || f.patches != 1 || stored.Status.NativeSchemaJobUID != "" || stored.Status.Phase != v1beta1.PhaseCuttingOver {
			t.Fatalf("replay pass started verification: %v %+v patches=%d status=%+v", err, result, f.patches, stored.Status)
		}
		requireReplayGauge(t, stored, lifecycleEnd)
		f.m = stored
		if result, err := nativeFinish(t, f); err != nil || result.RequeueAfter == 0 || f.m.Status.NativeSchemaJobUID == "" {
			t.Fatalf("verification receipt after the replay write: %v %+v", err, result)
		}
	})
	t.Run("refused proof keeps the frozen replay", func(t *testing.T) {
		f := newNativeDrainReplayFixture(t)
		f.editResult = func(kind string, v map[string]any) {
			if kind == finishOrdinaryEvidenceKind {
				v["checkpoint"].(map[string]any)["drain_replay_lsn"] = nativeTerminalEarlierLSN
			}
		}
		if _, err := nativeFinish(t, f); err == nil || f.patches != 0 || !slices.Contains(f.steps, finishOrdinaryEvidenceKind) {
			t.Fatalf("unverified proof: %v patches=%d steps=%v", err, f.patches, f.steps)
		}
		if got := storedMigration(t, f).Status.Replication.ReplayLSN; got != frozen {
			t.Fatalf("unverified proof moved replay to %s", got)
		}
		requireReplayGauge(t, f.m, frozen)
	})
	t.Run("later_begin proof below the sample never lowers it", func(t *testing.T) {
		f := newNativeDrainReplayFixture(t)
		f.laterBegin = true
		if result, err := nativeFinish(t, f); err != nil || result.RequeueAfter != 0 {
			t.Fatalf("finish: %v %+v steps=%v", err, result, f.steps)
		}
		assertNativeFinished(t, f)
		if got := storedMigration(t, f).Status.Replication.ReplayLSN; got != frozen || f.patches != 1 {
			t.Fatalf("durable %s moved replay %s to %s", nativeTerminalEarlierLSN, frozen, got)
		}
	})
}
