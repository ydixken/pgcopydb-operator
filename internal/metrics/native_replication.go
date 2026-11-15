package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// RecordNativeLag sets lag and the three positions from one native sample, or deletes all
// four when the sample is unknown. write is the source insert position, so source equals write.
func RecordNativeLag(namespace, name string, lag *int64, write, replay string) {
	gauges := []*prometheus.GaugeVec{replicationLagBytes, sourceLSNBytes, writeLSNBytes, replayLSNBytes}
	w, errW := sentinel.ParseLSN(write)
	r, errR := sentinel.ParseLSN(replay)
	if lag == nil || *lag < 0 || errW != nil || errR != nil {
		for _, g := range gauges {
			g.DeleteLabelValues(namespace, name)
		}
		return
	}
	for i, v := range []float64{float64(*lag), float64(w), float64(w), float64(r)} {
		gauges[i].WithLabelValues(namespace, name).Set(v)
	}
}

func recordNativeReplication(m *v1beta1.Migration) {
	var lag *int64
	var write, replay string
	r := m.Status.Replication
	if r != nil && samplingPhases[m.Status.Phase] {
		lag, write, replay = r.LagBytes, r.WriteLSN, r.ReplayLSN
	}
	RecordNativeLag(m.Namespace, m.Name, lag, write, replay)
	endposLSNBytes.DeleteLabelValues(m.Namespace, m.Name)
	if r != nil && r.Endpos != "" {
		setLSN(endposLSNBytes, m, r.Endpos)
	}
}
