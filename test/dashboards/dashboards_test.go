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

package dashboards

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/ydixken/pgcopydb-operator/internal/metrics"
)

const (
	dashboardsDir = "../../charts/pgcopydb-operator/dashboards"
	rulesFile     = "../../charts/pgcopydb-operator/rules/migrations.yaml"
)

// ruleExprs reads every alert expression from the chart's rule file.
func ruleExprs(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(rulesFile)
	if err != nil {
		t.Fatalf("read rules: %v", err)
	}
	var rf struct {
		Groups []struct {
			Rules []struct {
				Alert string `json:"alert"`
				Expr  string `json:"expr"`
			} `json:"rules"`
		} `json:"groups"`
	}
	if err := yaml.Unmarshal(data, &rf); err != nil {
		t.Fatalf("parse rules: %v", err)
	}
	var out []string
	for _, g := range rf.Groups {
		for _, r := range g.Rules {
			if r.Expr == "" {
				t.Fatalf("alert %q has no expr", r.Alert)
			}
			out = append(out, r.Expr)
		}
	}
	if len(out) == 0 {
		t.Fatal("no alert expressions found")
	}
	return out
}

func load(t *testing.T) map[string]*Dashboard {
	t.Helper()
	ds, err := Load(dashboardsDir)
	if err != nil {
		t.Fatalf("load dashboards: %v", err)
	}
	return ds
}

func TestDashboardIdentity(t *testing.T) {
	ds := load(t)
	want := map[string]string{
		"migration-detail.json": "pgcopydb-migration",
		"fleet-overview.json":   "pgcopydb-fleet",
		"operator-health.json":  "pgcopydb-operator",
	}
	if len(ds) != len(want) {
		t.Fatalf("got %d dashboards, want %d", len(ds), len(want))
	}
	titles := map[string]bool{}
	uids := map[string]bool{}
	for file, d := range ds {
		if d.UID != want[file] {
			t.Errorf("%s: uid %q, want %q", file, d.UID, want[file])
		}
		if d.SchemaVersion != 39 {
			t.Errorf("%s: schemaVersion %d, want 39", file, d.SchemaVersion)
		}
		if !strings.HasPrefix(d.Title, "pgcopydb / ") {
			t.Errorf("%s: title %q lacks the pgcopydb / prefix", file, d.Title)
		}
		if titles[d.Title] {
			t.Errorf("%s: duplicate title %q", file, d.Title)
		}
		if uids[d.UID] {
			t.Errorf("%s: duplicate uid %q", file, d.UID)
		}
		titles[d.Title] = true
		uids[d.UID] = true
		if len(d.Exprs()) == 0 {
			t.Errorf("%s: no query expressions", file)
		}
	}
}

// operatorPrefixes are the metric families the operator-health dashboard may
// use besides the operator's own; everything else is a typo.
var operatorPrefixes = []string{
	"controller_runtime_", "workqueue_", "go_", "process_",
	"rest_client_", "certwatcher_", "leader_election_",
}

func allowedNonPgcopydb(name string) bool {
	if name == "up" {
		return true
	}
	for _, p := range operatorPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func TestEveryMetricTokenIsKnown(t *testing.T) {
	registered := metrics.Names()
	check := func(t *testing.T, where, expr string) {
		t.Helper()
		for _, m := range Metrics(expr) {
			switch {
			case strings.HasPrefix(m, "pgcopydb_"):
				if !slices.Contains(registered, m) {
					t.Errorf("%s: %q is not a registered metric (expr %q)", where, m, expr)
				}
			default:
				if !allowedNonPgcopydb(m) {
					t.Errorf("%s: unexpected metric %q (expr %q)", where, m, expr)
				}
			}
		}
	}
	for file, d := range load(t) {
		for _, expr := range d.Exprs() {
			check(t, file, expr)
		}
	}
	for _, expr := range ruleExprs(t) {
		check(t, "rules/migrations.yaml", expr)
	}
}

func TestEveryRegisteredMetricIsConsumed(t *testing.T) {
	var exprs []string
	for _, d := range load(t) {
		exprs = append(exprs, d.Exprs()...)
	}
	exprs = append(exprs, ruleExprs(t)...)
	used := map[string]bool{}
	for _, expr := range exprs {
		for _, m := range Metrics(expr) {
			used[m] = true
		}
	}
	for _, name := range metrics.Names() {
		if !used[name] {
			t.Errorf("registered metric %q is on no dashboard and in no alert", name)
		}
	}
}

func TestVariablesDeclared(t *testing.T) {
	builtins := map[string]bool{}
	for b := range builtinDefaults {
		builtins[b] = true
	}
	ds := load(t)
	for file, d := range ds {
		declared := map[string]bool{}
		for _, v := range d.Templating.List {
			declared[v.Name] = true
		}
		for _, expr := range d.Exprs() {
			for _, v := range Vars(expr) {
				if !declared[v] && !builtins[v] {
					t.Errorf("%s: expression uses undeclared variable $%s (expr %q)", file, v, expr)
				}
			}
		}
	}
	detail := ds["migration-detail.json"]
	for _, must := range []string{"namespace", "name", "datasource"} {
		found := false
		for _, v := range detail.Templating.List {
			if v.Name == must {
				found = true
			}
		}
		if !found {
			t.Errorf("migration-detail.json must declare the %s variable", must)
		}
	}
}

func TestDatasourcesAreTemplated(t *testing.T) {
	uidField := regexp.MustCompile(`"uid":\s*"([^"]*)"`)
	for file, d := range load(t) {
		for _, p := range d.AllPanels() {
			if p.Type == "row" {
				continue
			}
			if p.Datasource == nil || p.Datasource.UID != "${datasource}" {
				t.Errorf("%s: panel %q does not use the ${datasource} variable", file, p.Title)
			}
		}
		// Raw scan: any uid other than the dashboard's own or the variable is
		// a hardcoded data source that breaks on every other Grafana.
		raw, err := os.ReadFile(filepath.Join(dashboardsDir, file))
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, m := range uidField.FindAllStringSubmatch(string(raw), -1) {
			if m[1] != d.UID && m[1] != "${datasource}" {
				t.Errorf("%s: hardcoded uid %q", file, m[1])
			}
		}
	}
}

func TestParseErrors(t *testing.T) {
	if _, err := Parse([]byte("{")); err == nil {
		t.Error("Parse accepted invalid JSON")
	}
	if _, err := Parse([]byte(`{"title": "no uid"}`)); err == nil {
		t.Error("Parse accepted a dashboard without a uid")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("Load accepted a directory without dashboards")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Error("Load accepted a broken dashboard file")
	}
	if _, err := Load(`[`); err == nil {
		t.Error("Load accepted an invalid glob pattern")
	}
	dir = t.TempDir()
	locked := filepath.Join(dir, "locked.json")
	if err := os.WriteFile(locked, []byte("{}"), 0o000); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Error("Load accepted an unreadable dashboard file")
	}
}

func TestMetrics(t *testing.T) {
	cases := []struct {
		expr string
		want []string
	}{
		{`pgcopydb_migration_phase{phase="Failed"} == 1`, []string{"pgcopydb_migration_phase"}},
		{
			`(pgcopydb_migration_phase{phase=~"Cloning|Streaming"} == 1) and on (namespace, name) ` +
				`(delta(pgcopydb_migration_attempts[30m]) >= 3)`,
			[]string{"pgcopydb_migration_attempts", "pgcopydb_migration_phase"},
		},
		{
			`histogram_quantile(0.99, sum by (le) ` +
				`(rate(workqueue_queue_duration_seconds_bucket{job="$job"}[$__rate_interval])))`,
			[]string{"workqueue_queue_duration_seconds_bucket"},
		},
		{`time() - pgcopydb_migration_start_time_seconds{a="b"}`, []string{"pgcopydb_migration_start_time_seconds"}},
		{`100 * a{x="y"} / b`, []string{"a", "b"}},
		{`sum(up{job="$job"})`, []string{"up"}},
		{`a + $threshold`, []string{"a"}},
		{
			`(pgcopydb_migration_endpos_lsn_bytes > 0) - on(namespace, name) pgcopydb_migration_replay_lsn_bytes > 0`,
			[]string{"pgcopydb_migration_endpos_lsn_bytes", "pgcopydb_migration_replay_lsn_bytes"},
		},
	}
	for _, c := range cases {
		if got := Metrics(c.expr); !slices.Equal(got, c.want) {
			t.Errorf("Metrics(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestVars(t *testing.T) {
	got := Vars(`rate(x{ns="$ns"}[$__rate_interval]) + ${job}`)
	want := []string{rateInterval, "job", "ns"}
	if !slices.Equal(got, want) {
		t.Errorf("Vars = %v, want %v", got, want)
	}
}

func TestSubstitute(t *testing.T) {
	got := Substitute(
		`rate(x{ns="$namespace", n="$name"}[$__rate_interval]) or y{j="${job}"}`,
		map[string]string{"namespace": "prod", "name": "shop", "job": "op"},
	)
	want := `rate(x{ns="prod", n="shop"}[5m]) or y{j="op"}`
	if got != want {
		t.Errorf("Substitute = %q, want %q", got, want)
	}
	if got := Substitute("x[$__interval] $__range", nil); got != "x[1m] 1h" {
		t.Errorf("builtin defaults: got %q", got)
	}
	if got := Substitute("x[$__rate_interval]", map[string]string{rateInterval: "2m"}); got != "x[2m]" {
		t.Errorf("builtin override: got %q", got)
	}
	// Equal-length names order deterministically and never cross-replace.
	if got := Substitute("$aa $bb", map[string]string{"aa": "1", "bb": "2"}); got != "1 2" {
		t.Errorf("equal-length vars: got %q", got)
	}
}
