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

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil/promlint"
	dto "github.com/prometheus/client_model/go"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// frozenLintExceptions are the two legacy gauge names promlint flags for their
// _total suffix; renaming them would break every existing scrape.
var frozenLintExceptions = map[string]bool{
	"pgcopydb_migration_tables_total":  true,
	"pgcopydb_migration_indexes_total": true,
}

// TestPromlint gates every metric this package registers: new names must be
// promlint-clean, and only the two frozen legacy names get a pass.
func TestPromlint(t *testing.T) {
	m := newMigration("ns1", "promlint")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })
	loadMigration(m)
	Record(m)
	src, tgt := int64(1<<30), int64(1<<20)
	RecordDatabaseSizes(m.Namespace, m.Name, &src, &tgt)
	RecordBuildInfo("v0.0.0-promlint")

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	populated := map[string]bool{}
	var ours []*dto.MetricFamily
	for _, mf := range families {
		if strings.HasPrefix(mf.GetName(), "pgcopydb_") {
			ours = append(ours, mf)
			populated[mf.GetName()] = true
		}
	}
	// Gather returns only populated families, so this also proves the loaded
	// Migration lit every registered metric and none escaped the linter.
	for _, name := range Names() {
		if !populated[name] {
			t.Errorf("metric %s not populated, promlint never saw it", name)
		}
	}

	problems, err := promlint.NewWithMetricFamilies(ours).Lint()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		if frozenLintExceptions[p.Metric] {
			continue
		}
		t.Errorf("promlint: %s: %s", p.Metric, p.Text)
	}
}
