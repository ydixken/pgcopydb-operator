package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// RecordNativeLag removes unknown measurements and unsupported native frontiers.
func RecordNativeLag(namespace, name string, lag *int64) {
	for _, g := range []*prometheus.GaugeVec{sourceLSNBytes, writeLSNBytes, replayLSNBytes} {
		g.DeleteLabelValues(namespace, name)
	}
	if lag == nil || *lag < 0 {
		replicationLagBytes.DeleteLabelValues(namespace, name)
		return
	}
	replicationLagBytes.WithLabelValues(namespace, name).Set(float64(*lag))
}

func recordNativeReplication(m *v1beta1.Migration) {
	var lag *int64
	r := m.Status.Replication
	if r != nil && samplingPhases[m.Status.Phase] {
		lag = r.LagBytes
	}
	RecordNativeLag(m.Namespace, m.Name, lag)
	endposLSNBytes.DeleteLabelValues(m.Namespace, m.Name)
	if r != nil && r.Endpos != "" {
		setLSN(endposLSNBytes, m, r.Endpos)
	}
}
