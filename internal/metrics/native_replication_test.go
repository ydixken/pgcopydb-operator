package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func TestNativeMetricsMapping(t *testing.T) {
	m := newMigration("native-metrics", "lag")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })
	loadMigration(m)
	Record(m)
	m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
	Record(m)
	for _, g := range []*prometheus.GaugeVec{sourceLSNBytes, writeLSNBytes, replayLSNBytes} {
		if got := testutil.CollectAndCount(g); got != 0 {
			t.Fatal("native status retained an unsupported replication frontier")
		}
	}
	if got := testutil.ToFloat64(endposLSNBytes.WithLabelValues(m.Namespace, m.Name)); got != 0x3000 {
		t.Fatal("native status lost independently persisted endpos")
	}
	m.Status.Replication.LagBytes = nil
	Record(m)
	if got := testutil.CollectAndCount(replicationLagBytes); got != 0 {
		t.Fatal("unknown native lag retained the last known gauge")
	}
	m.Status.Phase = v1beta1.PhaseCompleted
	Record(m)
	if got := testutil.ToFloat64(endposLSNBytes.WithLabelValues(m.Namespace, m.Name)); got != 0x3000 {
		t.Fatal("native terminal status lost persisted endpos")
	}
}
