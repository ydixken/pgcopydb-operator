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

func TestForget(t *testing.T) {
	gone := newMigration("ns1", "gone")
	stays := newMigration("ns2", "stays")
	lag := int64(1)
	for _, m := range []*v1beta1.Migration{gone, stays} {
		m.Status.Phase = v1beta1.PhaseStreaming
		m.Status.Attempts = 1
		m.Status.Progress = &v1beta1.CloneProgress{TablesTotal: 1, TablesDone: 1, IndexesTotal: 1, IndexesDone: 1}
		m.Status.Replication = &v1beta1.ReplicationStatus{LagBytes: &lag}
		Record(m)
	}
	t.Cleanup(func() { Forget(stays.Namespace, stays.Name) })

	Forget(gone.Namespace, gone.Name)

	// Every vec must drop the forgotten CR and keep the other one.
	for _, tc := range []struct {
		metric string
		c      prometheus.Collector
	}{
		{"phase", phase},
		{"attempts", attempts},
		{"tables_done", tablesDone},
		{"tables_total", tablesTotal},
		{"indexes_done", indexesDone},
		{"indexes_total", indexesTotal},
		{"replication_lag_bytes", replicationLagBytes},
	} {
		if got := testutil.CollectAndCount(tc.c); got != 1 {
			t.Errorf("%s series after Forget = %d, want 1 (the surviving CR)", tc.metric, got)
		}
	}
	if got := testutil.ToFloat64(attempts.WithLabelValues("ns2", "stays")); got != 1 {
		t.Errorf("surviving attempts gauge = %v, want 1", got)
	}
}
