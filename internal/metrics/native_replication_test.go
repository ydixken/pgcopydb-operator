package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

var nativeSampleGauges = []*prometheus.GaugeVec{replicationLagBytes, sourceLSNBytes, writeLSNBytes, replayLSNBytes}

// requireNativeGaugesMatchStatus checks every gauge against the status sample it came from.
func requireNativeGaugesMatchStatus(t *testing.T, m *v1beta1.Migration) {
	t.Helper()
	r := m.Status.Replication
	write, err1 := sentinel.ParseLSN(r.WriteLSN)
	replay, err2 := sentinel.ParseLSN(r.ReplayLSN)
	if err1 != nil || err2 != nil || r.LagBytes == nil {
		t.Fatal("fixture is not a known native sample")
	}
	for g, want := range map[*prometheus.GaugeVec]float64{
		replicationLagBytes: float64(*r.LagBytes), writeLSNBytes: float64(write),
		replayLSNBytes: float64(replay), sourceLSNBytes: float64(write),
	} {
		// A missing series would read 0 through WithLabelValues, so count it first.
		if testutil.CollectAndCount(g) != 1 {
			t.Fatal("known native sample left a gauge unset")
		}
		if got := testutil.ToFloat64(g.WithLabelValues(m.Namespace, m.Name)); got != want {
			t.Fatalf("native gauge = %v, want %v from status.replication", got, want)
		}
	}
}

func requireNoNativeSampleGauges(t *testing.T, why string) {
	t.Helper()
	for _, g := range nativeSampleGauges {
		if got := testutil.CollectAndCount(g); got != 0 {
			t.Fatal(why)
		}
	}
}

func TestNativeMetricsMapping(t *testing.T) {
	m := newMigration("native-metrics", "lag")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })
	loadMigration(m)
	m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
	lag := int64(0x1000)
	m.Status.Replication.WriteLSN, m.Status.Replication.ReplayLSN, m.Status.Replication.LagBytes = "16/B374E000", "16/B374D000", &lag
	Record(m)
	requireNativeGaugesMatchStatus(t, m)
	if got := testutil.ToFloat64(endposLSNBytes.WithLabelValues(m.Namespace, m.Name)); got != 0x3000 {
		t.Fatal("native status lost independently persisted endpos")
	}

	next := int64(0)
	m.Status.Replication.WriteLSN, m.Status.Replication.ReplayLSN, m.Status.Replication.LagBytes = "16/B3750000", "16/B3750000", &next
	Record(m)
	requireNativeGaugesMatchStatus(t, m)

	m.Status.Replication.LagBytes, m.Status.Replication.WriteLSN, m.Status.Replication.ReplayLSN = nil, "", ""
	Record(m)
	requireNoNativeSampleGauges(t, "unknown native sample retained a gauge from the last known one")

	m.Status.Replication.WriteLSN, m.Status.Replication.ReplayLSN, m.Status.Replication.LagBytes = "16/B374E000", "16/B374D000", &lag
	m.Status.Phase = v1beta1.PhaseCompleted
	Record(m)
	requireNoNativeSampleGauges(t, "frozen native status published a current sample")
	if got := testutil.ToFloat64(endposLSNBytes.WithLabelValues(m.Namespace, m.Name)); got != 0x3000 {
		t.Fatal("native terminal status lost persisted endpos")
	}
}

func TestRecordNativeLagUnknownSample(t *testing.T) {
	const ns, name, write, replay = "native-metrics", "unknown", "0/20", "0/10"
	t.Cleanup(func() { Forget(ns, name) })
	lag, negative := int64(16), int64(-1)
	for _, c := range []struct {
		why           string
		lag           *int64
		write, replay string
	}{
		{"nil lag", nil, write, replay},
		{"negative lag", &negative, write, replay},
		{"missing write", &lag, "", replay},
		{"missing replay", &lag, write, ""},
		{"garbage write", &lag, "junk", replay},
	} {
		RecordNativeLag(ns, name, &lag, write, replay)
		if got := testutil.ToFloat64(sourceLSNBytes.WithLabelValues(ns, name)); got != 0x20 {
			t.Fatal("known native sample did not set the source gauge")
		}
		RecordNativeLag(ns, name, c.lag, c.write, c.replay)
		requireNoNativeSampleGauges(t, c.why+": unknown native sample left a gauge set")
	}
}
