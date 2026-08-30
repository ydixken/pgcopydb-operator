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
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// Label names shared by every metric.
const (
	labelNamespace = "namespace"
	labelName      = "name"
)

// perMigration collects every {namespace,name}-labeled vec, so registration
// and Forget can never miss one.
var perMigration []*prometheus.GaugeVec

// names lists every registered metric name; Names() hands out copies.
var names []string

// gauge builds a per-Migration GaugeVec and enrolls it in perMigration and
// names.
func gauge(name, help string, extraLabels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help},
		append([]string{labelNamespace, labelName}, extraLabels...))
	perMigration = append(perMigration, g)
	names = append(names, name)
	return g
}

var (
	// phase is 1 for the Migration's current phase and absent otherwise; the
	// phase lives in a label so dashboards can group without enum decoding.
	phase = gauge("pgcopydb_migration_phase",
		"Current phase of a Migration (1 for the active phase).", "phase")

	attempts = gauge("pgcopydb_migration_attempts",
		"Worker Jobs created for a Migration so far.")

	tablesDone = gauge("pgcopydb_migration_tables_done",
		"Tables fully copied.")

	tablesTotal = gauge("pgcopydb_migration_tables_total",
		"Tables planned for copy.")

	indexesDone = gauge("pgcopydb_migration_indexes_done",
		"Indexes fully built.")

	indexesTotal = gauge("pgcopydb_migration_indexes_total",
		"Indexes planned for creation.")

	replicationLagBytes = gauge("pgcopydb_migration_replication_lag_bytes",
		"Replication lag in bytes while streaming (absent outside follow).")

	verified = gauge("pgcopydb_migration_verified",
		"1 when pgcopydb compare verification passed, 0 on mismatch (absent before a result).")

	verifiedCheck = gauge("pgcopydb_migration_verification_check",
		"1 when that pgcopydb compare check passed, 0 on mismatch (absent until it has run).",
		"check")

	sourceDatabaseSizeBytes = gauge("pgcopydb_migration_source_database_size_bytes",
		"Source database size in bytes, sampled from the worker pod.")

	targetDatabaseSizeBytes = gauge("pgcopydb_migration_target_database_size_bytes",
		"Target database size in bytes, sampled from the worker pod.")

	clonePlannedBytes = gauge("pgcopydb_migration_clone_planned_bytes",
		"Bytes the base copy plans to move (absent without a progress sample).")

	cloneCopiedBytes = gauge("pgcopydb_migration_clone_copied_bytes",
		"Bytes the base copy has moved so far (absent without a progress sample).")

	sourceLSNBytes = gauge("pgcopydb_migration_source_lsn_bytes",
		"Source WAL head as an absolute byte position (replay LSN plus lag).")

	writeLSNBytes = gauge("pgcopydb_migration_write_lsn_bytes",
		"Last LSN written by the receiver, as an absolute byte position.")

	replayLSNBytes = gauge("pgcopydb_migration_replay_lsn_bytes",
		"How far the target has consumed the stream, as an absolute byte position.")

	endposLSNBytes = gauge("pgcopydb_migration_endpos_lsn_bytes",
		"Cutover endpos as an absolute byte position (absent until cutover sets it).")

	startTimeSeconds = gauge("pgcopydb_migration_start_time_seconds",
		"Unix time the first attempt started (absent before it).")

	completionTimeSeconds = gauge("pgcopydb_migration_completion_time_seconds",
		"Unix time the migration completed (absent until then).")

	info = gauge("pgcopydb_migration_info",
		"Always 1; the mode label carries clone or follow.", "mode")
)

// buildInfo is operator-wide, not per-Migration, so it stays out of
// perMigration and Forget.
var buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "pgcopydb_operator_build_info",
	Help: "Always 1; the version label carries the manager build version.",
}, []string{"version"})

func init() {
	for _, g := range perMigration {
		metrics.Registry.MustRegister(g)
	}
	metrics.Registry.MustRegister(buildInfo)
	names = append(names, "pgcopydb_operator_build_info")
}

// Names returns every metric name this package registers.
func Names() []string {
	out := make([]string, len(names))
	copy(out, names)
	return out
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
		if p.BytesTotal != nil {
			clonePlannedBytes.WithLabelValues(m.Namespace, m.Name).Set(float64(p.BytesTotal.Value()))
		}
		if p.BytesDone != nil {
			cloneCopiedBytes.WithLabelValues(m.Namespace, m.Name).Set(float64(p.BytesDone.Value()))
		}
	}
	recordReplication(m)
	if t := m.Status.StartedAt; t != nil {
		startTimeSeconds.WithLabelValues(m.Namespace, m.Name).Set(float64(t.Unix()))
	}
	if t := m.Status.CompletedAt; t != nil {
		completionTimeSeconds.WithLabelValues(m.Namespace, m.Name).Set(float64(t.Unix()))
	}
	// spec.follow is immutable, so the mode label never churns.
	mode := "clone"
	if m.Spec.Follow != nil && m.Spec.Follow.Enabled {
		mode = "follow"
	}
	info.WithLabelValues(m.Namespace, m.Name, mode).Set(1)
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
	// Per check, because the condition collapses them and its reason names
	// only the first mismatch. Absent until a check has actually run.
	verifiedCheck.DeletePartialMatch(prometheus.Labels{labelNamespace: m.Namespace, labelName: m.Name})
	for _, r := range m.Status.Verification {
		v := 0.0
		if r.Passed {
			v = 1
		}
		verifiedCheck.WithLabelValues(m.Namespace, m.Name, r.Check).Set(v)
	}
}

// recordReplication maps status.replication onto the lag and LSN gauges; each
// gauge is set only on a successful parse, absent-over-fake-zero throughout.
func recordReplication(m *v1beta1.Migration) {
	r := m.Status.Replication
	if r == nil {
		return
	}
	if r.LagBytes != nil {
		replicationLagBytes.WithLabelValues(m.Namespace, m.Name).Set(float64(*r.LagBytes))
	}
	setLSN(writeLSNBytes, m, r.WriteLSN)
	replay, replayErr := sentinel.ParseLSN(r.ReplayLSN)
	if replayErr == nil {
		replayLSNBytes.WithLabelValues(m.Namespace, m.Name).Set(float64(replay))
	}
	if r.Endpos != "" {
		setLSN(endposLSNBytes, m, r.Endpos)
	}
	// Replay plus lag reconstructs exactly the SourceHead the sentinel sample
	// dropped when it stored lag instead.
	if r.LagBytes != nil && replayErr == nil {
		sourceLSNBytes.WithLabelValues(m.Namespace, m.Name).Set(float64(replay) + float64(*r.LagBytes))
	}
}

// setLSN parses lsn and sets g, or leaves the series untouched when it does
// not parse (empty while the sentinel has no sample yet).
func setLSN(g *prometheus.GaugeVec, m *v1beta1.Migration, lsn string) {
	v, err := sentinel.ParseLSN(lsn)
	if err != nil {
		return
	}
	g.WithLabelValues(m.Namespace, m.Name).Set(float64(v))
}

// RecordDatabaseSizes sets the sampled database sizes. A nil side keeps the
// previous value: a failed sample must not zero a real size.
func RecordDatabaseSizes(namespace, name string, src, tgt *int64) {
	if src != nil {
		sourceDatabaseSizeBytes.WithLabelValues(namespace, name).Set(float64(*src))
	}
	if tgt != nil {
		targetDatabaseSizeBytes.WithLabelValues(namespace, name).Set(float64(*tgt))
	}
}

// RecordBuildInfo publishes the manager's build version, once at startup.
func RecordBuildInfo(version string) {
	buildInfo.WithLabelValues(version).Set(1)
}

// Forget removes every series of a deleted Migration.
func Forget(namespace, name string) {
	l := prometheus.Labels{labelNamespace: namespace, labelName: name}
	for _, g := range perMigration {
		g.DeletePartialMatch(l)
	}
}
