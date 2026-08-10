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

// Package metrics exposes per-Migration Prometheus metrics on the manager's
// metrics endpoint (controller-runtime registry, scraped via the chart's
// ServiceMonitor).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// Label names shared by every metric.
const (
	labelNamespace = "namespace"
	labelName      = "name"
)

var (
	// phase is 1 for the Migration's current phase and absent otherwise; the
	// phase lives in a label so dashboards can group without enum decoding.
	phase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcopydb_migration_phase",
		Help: "Current phase of a Migration (1 for the active phase).",
	}, []string{labelNamespace, labelName, "phase"})

	attempts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcopydb_migration_attempts",
		Help: "Worker Jobs created for a Migration so far.",
	}, []string{labelNamespace, labelName})

	tablesDone = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcopydb_migration_tables_done",
		Help: "Tables fully copied.",
	}, []string{labelNamespace, labelName})

	tablesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcopydb_migration_tables_total",
		Help: "Tables planned for copy.",
	}, []string{labelNamespace, labelName})

	indexesDone = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcopydb_migration_indexes_done",
		Help: "Indexes fully built.",
	}, []string{labelNamespace, labelName})

	indexesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcopydb_migration_indexes_total",
		Help: "Indexes planned for creation.",
	}, []string{labelNamespace, labelName})

	replicationLagBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcopydb_migration_replication_lag_bytes",
		Help: "Replication lag in bytes while streaming (absent outside follow).",
	}, []string{labelNamespace, labelName})

	verified = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcopydb_migration_verified",
		Help: "1 when pgcopydb compare verification passed, 0 on mismatch (absent before a result).",
	}, []string{labelNamespace, labelName})
)

func init() {
	metrics.Registry.MustRegister(phase, attempts, tablesDone, tablesTotal, indexesDone, indexesTotal, replicationLagBytes, verified)
}

// Record refreshes every gauge for one Migration from its status.
func Record(m *v1beta1.Migration) {
	// Drop the previous phase series first so exactly one phase is set.
	phase.DeletePartialMatch(prometheus.Labels{labelNamespace: m.Namespace, labelName: m.Name})
	if m.Status.Phase != "" {
		phase.WithLabelValues(m.Namespace, m.Name, string(m.Status.Phase)).Set(1)
	}
	attempts.WithLabelValues(m.Namespace, m.Name).Set(float64(m.Status.Attempts))
	if p := m.Status.Progress; p != nil {
		tablesDone.WithLabelValues(m.Namespace, m.Name).Set(float64(p.TablesDone))
		tablesTotal.WithLabelValues(m.Namespace, m.Name).Set(float64(p.TablesTotal))
		indexesDone.WithLabelValues(m.Namespace, m.Name).Set(float64(p.IndexesDone))
		indexesTotal.WithLabelValues(m.Namespace, m.Name).Set(float64(p.IndexesTotal))
	}
	if r := m.Status.Replication; r != nil && r.LagBytes != nil {
		replicationLagBytes.WithLabelValues(m.Namespace, m.Name).Set(float64(*r.LagBytes))
	}
	// Verified maps the condition: True is 1, False is 0, and no series while
	// there is no result yet (a 0 before the compare ran would read as a
	// mismatch on a dashboard).
	switch cond := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionVerified); {
	case cond == nil || cond.Status == metav1.ConditionUnknown:
		verified.DeletePartialMatch(prometheus.Labels{labelNamespace: m.Namespace, labelName: m.Name})
	case cond.Status == metav1.ConditionTrue:
		verified.WithLabelValues(m.Namespace, m.Name).Set(1)
	default:
		verified.WithLabelValues(m.Namespace, m.Name).Set(0)
	}
}

// Forget removes every series of a deleted Migration.
func Forget(namespace, name string) {
	l := prometheus.Labels{labelNamespace: namespace, labelName: name}
	phase.DeletePartialMatch(l)
	attempts.DeletePartialMatch(l)
	tablesDone.DeletePartialMatch(l)
	tablesTotal.DeletePartialMatch(l)
	indexesDone.DeletePartialMatch(l)
	indexesTotal.DeletePartialMatch(l)
	replicationLagBytes.DeletePartialMatch(l)
	verified.DeletePartialMatch(l)
}
