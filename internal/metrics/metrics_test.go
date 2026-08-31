/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

// The gauges are package-level and registered once via init(), so tests share
// state. Each test uses its own CR identity and Forgets it in cleanup, which
// keeps CollectAndCount (it counts the whole vec) honest across tests.

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func newMigration(namespace, name string) *v1beta1.Migration {
	return &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

func TestRecordPhase(t *testing.T) {
	m := newMigration("ns1", "phase-flip")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	// Empty phase must not create a series.
	Record(m)
	if got := testutil.CollectAndCount(phase); got != 0 {
		t.Fatalf("phase series with empty phase = %d, want 0", got)
	}

	m.Status.Phase = v1beta1.PhaseCloning
	Record(m)
	if got := testutil.CollectAndCount(phase); got != 1 {
		t.Fatalf("phase series = %d, want 1", got)
	}
	if got := testutil.ToFloat64(phase.WithLabelValues("ns1", "phase-flip", "Cloning")); got != 1 {
		t.Fatalf("Cloning gauge = %v, want 1", got)
	}

	// Flipping the phase must replace the old series, not add a second one.
	// The count check proves the Cloning series is gone; do not probe it with
	// WithLabelValues, that would resurrect it.
	m.Status.Phase = v1beta1.PhaseStreaming
	Record(m)
	if got := testutil.CollectAndCount(phase); got != 1 {
		t.Fatalf("phase series after flip = %d, want 1", got)
	}
	if got := testutil.ToFloat64(phase.WithLabelValues("ns1", "phase-flip", "Streaming")); got != 1 {
		t.Fatalf("Streaming gauge = %v, want 1", got)
	}
}

func TestRecordGauges(t *testing.T) {
	m := newMigration("ns1", "gauges")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	m.Status.Attempts = 3
	m.Status.Progress = &v1beta1.CloneProgress{
		TablesTotal:  10,
		TablesDone:   4,
		IndexesTotal: 6,
		IndexesDone:  2,
	}
	Record(m)

	for _, tc := range []struct {
		metric string
		g      *prometheus.GaugeVec
		want   float64
	}{
		{"attempts", attempts, 3},
		{"tables_done", tablesDone, 4},
		{"tables_total", tablesTotal, 10},
		{"indexes_done", indexesDone, 2},
		{"indexes_total", indexesTotal, 6},
	} {
		if got := testutil.ToFloat64(tc.g.WithLabelValues("ns1", "gauges")); got != tc.want {
			t.Errorf("%s = %v, want %v", tc.metric, got, tc.want)
		}
	}
}

func TestRecordReplicationLag(t *testing.T) {
	m := newMigration("ns1", "lag")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	// No replication block at all: no series.
	Record(m)
	if got := testutil.CollectAndCount(replicationLagBytes); got != 0 {
		t.Fatalf("lag series without replication block = %d, want 0", got)
	}

	// Block present but lag unknown: still no series, a zero would lie.
	m.Status.Replication = &v1beta1.ReplicationStatus{}
	Record(m)
	if got := testutil.CollectAndCount(replicationLagBytes); got != 0 {
		t.Fatalf("lag series without lagBytes = %d, want 0", got)
	}

	lag := int64(2048)
	m.Status.Replication.LagBytes = &lag
	Record(m)
	if got := testutil.CollectAndCount(replicationLagBytes); got != 1 {
		t.Fatalf("lag series = %d, want 1", got)
	}
	if got := testutil.ToFloat64(replicationLagBytes.WithLabelValues("ns1", "lag")); got != 2048 {
		t.Fatalf("lag gauge = %v, want 2048", got)
	}
}

// loadMigration fills every status field Record reads, so one Record call
// gives every per-Migration vec a series.
func loadMigration(m *v1beta1.Migration) {
	lag := int64(512)
	start := metav1.Unix(1700000000, 0)
	done := metav1.Unix(1700003600, 0)
	m.Status.Phase = v1beta1.PhaseStreaming
	m.Status.Attempts = 1
	m.Status.Progress = &v1beta1.CloneProgress{
		TablesTotal: 1, TablesDone: 1, IndexesTotal: 1, IndexesDone: 1,
		BytesTotal: resource.NewQuantity(100, resource.BinarySI),
		BytesDone:  resource.NewQuantity(50, resource.BinarySI),
	}
	m.Status.Replication = &v1beta1.ReplicationStatus{
		WriteLSN: "0/2000", ReplayLSN: "0/1000", Endpos: "0/3000", LagBytes: &lag,
	}
	m.Status.StartedAt = &start
	m.Status.CompletedAt = &done
	setVerified(m, metav1.ConditionTrue)
	// Both checks requested but only one result in: TestForget asserts a single
	// series per vec, and a check the spec skips would publish its own -1.
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
	m.Status.Verification = []v1beta1.VerificationResult{{Check: checkSchema, Passed: true}}
}

func TestForget(t *testing.T) {
	gone := newMigration("ns1", "gone")
	stays := newMigration("ns2", "stays")
	for _, m := range []*v1beta1.Migration{gone, stays} {
		loadMigration(m)
		Record(m)
		src, tgt := int64(1), int64(2)
		RecordDatabaseSizes(m.Namespace, m.Name, &src, &tgt)
	}
	t.Cleanup(func() { Forget(stays.Namespace, stays.Name) })

	Forget(gone.Namespace, gone.Name)

	// Every per-Migration vec must drop the forgotten CR and keep the other
	// one; the loop keeps future gauges covered automatically.
	for i, g := range perMigration {
		if got := testutil.CollectAndCount(g); got != 1 {
			t.Errorf("%s series after Forget = %d, want 1 (the surviving CR)", names[i], got)
		}
	}
	if got := testutil.ToFloat64(attempts.WithLabelValues("ns2", "stays")); got != 1 {
		t.Errorf("surviving attempts gauge = %v, want 1", got)
	}
}

func TestRecordCloneBytes(t *testing.T) {
	m := newMigration("ns1", "clone-bytes")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	// Progress without byte counters (stock pgcopydb): no byte series.
	m.Status.Progress = &v1beta1.CloneProgress{TablesTotal: 3}
	Record(m)
	if got := testutil.CollectAndCount(clonePlannedBytes) + testutil.CollectAndCount(cloneCopiedBytes); got != 0 {
		t.Fatalf("byte series without counters = %d, want 0", got)
	}

	m.Status.Progress.BytesTotal = resource.NewQuantity(323888895, resource.BinarySI)
	m.Status.Progress.BytesDone = resource.NewQuantity(6162157, resource.BinarySI)
	Record(m)
	if got := testutil.ToFloat64(clonePlannedBytes.WithLabelValues("ns1", "clone-bytes")); got != 323888895 {
		t.Fatalf("clone_planned_bytes = %v, want 323888895", got)
	}
	if got := testutil.ToFloat64(cloneCopiedBytes.WithLabelValues("ns1", "clone-bytes")); got != 6162157 {
		t.Fatalf("clone_copied_bytes = %v, want 6162157", got)
	}
}

func TestRecordLSNs(t *testing.T) {
	m := newMigration("ns1", "lsn")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	// Empty LSN fields and no lag: no LSN series at all.
	m.Status.Replication = &v1beta1.ReplicationStatus{}
	Record(m)
	for _, g := range []*prometheus.GaugeVec{sourceLSNBytes, writeLSNBytes, replayLSNBytes, endposLSNBytes} {
		if got := testutil.CollectAndCount(g); got != 0 {
			t.Fatalf("LSN series without sentinel sample = %d, want 0", got)
		}
	}

	// 16/B374D848 = 0x16<<32 | 0xB374D848.
	lag := int64(512)
	m.Status.Replication = &v1beta1.ReplicationStatus{
		WriteLSN: "16/B374D848", ReplayLSN: "0/1000", LagBytes: &lag,
	}
	Record(m)
	if got := testutil.ToFloat64(writeLSNBytes.WithLabelValues("ns1", "lsn")); got != 97500059720 {
		t.Fatalf("write_lsn_bytes = %v, want 97500059720", got)
	}
	if got := testutil.ToFloat64(replayLSNBytes.WithLabelValues("ns1", "lsn")); got != 4096 {
		t.Fatalf("replay_lsn_bytes = %v, want 4096", got)
	}
	// source = replay + lag, the SourceHead the sentinel sample dropped.
	if got := testutil.ToFloat64(sourceLSNBytes.WithLabelValues("ns1", "lsn")); got != 4608 {
		t.Fatalf("source_lsn_bytes = %v, want 4608", got)
	}
	// Endpos stays absent until cutover sets it.
	if got := testutil.CollectAndCount(endposLSNBytes); got != 0 {
		t.Fatalf("endpos series before cutover = %d, want 0", got)
	}

	m.Status.Replication.Endpos = "0/2000"
	Record(m)
	if got := testutil.ToFloat64(endposLSNBytes.WithLabelValues("ns1", "lsn")); got != 8192 {
		t.Fatalf("endpos_lsn_bytes = %v, want 8192", got)
	}
}

func TestRecordLSNs_Unparseable(t *testing.T) {
	m := newMigration("ns1", "lsn-garbage")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	lag := int64(512)
	m.Status.Replication = &v1beta1.ReplicationStatus{
		WriteLSN: "garbage", ReplayLSN: "also/garbage", Endpos: "junk", LagBytes: &lag,
	}
	Record(m)
	// No parse, no series: source too, since it derives from replay.
	for _, g := range []*prometheus.GaugeVec{sourceLSNBytes, writeLSNBytes, replayLSNBytes, endposLSNBytes} {
		if got := testutil.CollectAndCount(g); got != 0 {
			t.Fatalf("LSN series from unparseable input = %d, want 0", got)
		}
	}
}

func TestRecordTimestamps(t *testing.T) {
	m := newMigration("ns1", "stamps")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	Record(m)
	if got := testutil.CollectAndCount(startTimeSeconds) + testutil.CollectAndCount(completionTimeSeconds); got != 0 {
		t.Fatalf("timestamp series before start = %d, want 0", got)
	}

	start := metav1.Unix(1700000000, 0)
	m.Status.StartedAt = &start
	Record(m)
	if got := testutil.ToFloat64(startTimeSeconds.WithLabelValues("ns1", "stamps")); got != 1700000000 {
		t.Fatalf("start_time_seconds = %v, want 1700000000", got)
	}
	if got := testutil.CollectAndCount(completionTimeSeconds); got != 0 {
		t.Fatalf("completion series while running = %d, want 0", got)
	}

	done := metav1.Unix(1700003600, 0)
	m.Status.CompletedAt = &done
	Record(m)
	if got := testutil.ToFloat64(completionTimeSeconds.WithLabelValues("ns1", "stamps")); got != 1700003600 {
		t.Fatalf("completion_time_seconds = %v, want 1700003600", got)
	}
}

// The timestamps here are the real ones off migration e2e-follow-load, where
// Streaming and CaughtUp flipped a second apart and no scrape landed between.
func TestRecordConditionTransitions(t *testing.T) {
	m := newMigration("ns1", "conditions")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	Record(m)
	if got := testutil.CollectAndCount(conditionTransitionSeconds); got != 0 {
		t.Fatalf("condition series without conditions = %d, want 0", got)
	}

	// An unstamped condition gets no series: a zero time would plot as year 1
	// and drag the whole timeline back to it.
	m.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.ConditionStreaming, Status: metav1.ConditionTrue, Reason: "Replaying",
	}}
	Record(m)
	if got := testutil.CollectAndCount(conditionTransitionSeconds); got != 0 {
		t.Fatalf("condition series without a transition time = %d, want 0", got)
	}

	streaming := metav1.Unix(1788157600, 0) // 2026-08-31T06:26:40Z
	caughtUp := metav1.Unix(1788157601, 0)  // one second later
	m.Status.Conditions = []metav1.Condition{
		{
			Type: v1beta1.ConditionStreaming, Status: metav1.ConditionTrue,
			Reason: "Replaying", LastTransitionTime: streaming,
		},
		{
			Type: v1beta1.ConditionCaughtUp, Status: metav1.ConditionTrue,
			Reason: "LagBelowThreshold", LastTransitionTime: caughtUp,
		},
	}
	Record(m)
	if got := testutil.CollectAndCount(conditionTransitionSeconds); got != 2 {
		t.Fatalf("condition series = %d, want 2", got)
	}
	for _, tc := range []struct {
		condition string
		want      float64
	}{
		{v1beta1.ConditionStreaming, 1788157600},
		{v1beta1.ConditionCaughtUp, 1788157601},
	} {
		g := conditionTransitionSeconds.WithLabelValues("ns1", "conditions", tc.condition, "True")
		if got := testutil.ToFloat64(g); got != tc.want {
			t.Errorf("%s transition = %v, want %v", tc.condition, got, tc.want)
		}
	}

	// A flip replaces the True series instead of adding a False one beside it.
	// The count proves the True series is gone; do not probe it with
	// WithLabelValues, that would resurrect it.
	m.Status.Conditions[1].Status = metav1.ConditionFalse
	m.Status.Conditions[1].LastTransitionTime = metav1.Unix(1788157650, 0)
	Record(m)
	if got := testutil.CollectAndCount(conditionTransitionSeconds); got != 2 {
		t.Fatalf("condition series after a flip = %d, want 2", got)
	}
	g := conditionTransitionSeconds.WithLabelValues("ns1", "conditions", v1beta1.ConditionCaughtUp, "False")
	if got := testutil.ToFloat64(g); got != 1788157650 {
		t.Errorf("CaughtUp transition after flip = %v, want 1788157650", got)
	}
}

func TestRecordInfoMode(t *testing.T) {
	clone := newMigration("ns1", "info-clone")
	follow := newMigration("ns1", "info-follow")
	follow.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	t.Cleanup(func() {
		Forget(clone.Namespace, clone.Name)
		Forget(follow.Namespace, follow.Name)
	})

	Record(clone)
	Record(follow)
	if got := testutil.ToFloat64(info.WithLabelValues("ns1", "info-clone", "clone")); got != 1 {
		t.Fatalf("info clone mode = %v, want 1", got)
	}
	if got := testutil.ToFloat64(info.WithLabelValues("ns1", "info-follow", "follow")); got != 1 {
		t.Fatalf("info follow mode = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(info); got != 2 {
		t.Fatalf("info series = %d, want 2", got)
	}
}

func TestRecordDatabaseSizes(t *testing.T) {
	t.Cleanup(func() { Forget("ns1", "sizes") })

	// Nothing sampled: no series, never a fake zero.
	RecordDatabaseSizes("ns1", "sizes", nil, nil)
	if got := testutil.CollectAndCount(sourceDatabaseSizeBytes) + testutil.CollectAndCount(targetDatabaseSizeBytes); got != 0 {
		t.Fatalf("size series without samples = %d, want 0", got)
	}

	src := int64(1 << 30)
	RecordDatabaseSizes("ns1", "sizes", &src, nil)
	if got := testutil.ToFloat64(sourceDatabaseSizeBytes.WithLabelValues("ns1", "sizes")); got != 1<<30 {
		t.Fatalf("source size = %v, want %d", got, int64(1<<30))
	}
	if got := testutil.CollectAndCount(targetDatabaseSizeBytes); got != 0 {
		t.Fatalf("target size series without a sample = %d, want 0", got)
	}

	// A nil side keeps the previous value instead of zeroing it.
	tgt := int64(4096)
	RecordDatabaseSizes("ns1", "sizes", nil, &tgt)
	if got := testutil.ToFloat64(sourceDatabaseSizeBytes.WithLabelValues("ns1", "sizes")); got != 1<<30 {
		t.Fatalf("source size after nil sample = %v, want the previous %d", got, int64(1<<30))
	}
	if got := testutil.ToFloat64(targetDatabaseSizeBytes.WithLabelValues("ns1", "sizes")); got != 4096 {
		t.Fatalf("target size = %v, want 4096", got)
	}
}

func TestRecordBuildInfo(t *testing.T) {
	RecordBuildInfo("v1.2.3-test")
	if got := testutil.ToFloat64(buildInfo.WithLabelValues("v1.2.3-test")); got != 1 {
		t.Fatalf("build info = %v, want 1", got)
	}
}

func TestNames(t *testing.T) {
	got := Names()
	if len(got) != len(perMigration)+1 {
		t.Fatalf("Names() = %d entries, want %d (per-Migration vecs plus build info)", len(got), len(perMigration)+1)
	}
	// Mutating the copy must not corrupt the package list.
	got[0] = "clobbered"
	if Names()[0] == "clobbered" {
		t.Fatal("Names() returned the internal slice, want a copy")
	}
}
