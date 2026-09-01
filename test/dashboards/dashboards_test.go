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
	"strconv"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/ydixken/pgcopydb-operator/internal/metrics"
)

const (
	dashboardsDir = "../../charts/pgcopydb-operator/dashboards"
	rulesFile     = "../../charts/pgcopydb-operator/rules/migrations.yaml"
	phaseMetric   = "pgcopydb_migration_phase"
	statPanel     = "stat"
	// textModeName is a stat tile that shows a label, not a reading.
	textModeName = "name"
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
		// The browser is the last of three intervals standing between a gauge
		// moving and a human seeing it, and the only one shipped per file.
		// All three are 10s; a dashboard left slower hides a live operator.
		if d.Refresh != "10s" {
			t.Errorf("%s: refresh %q, want 10s to match the poll and the scrape", file, d.Refresh)
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

// Every pgcopydb_migration_* series is a gauge; the operator registers no
// counters and no histograms. rate() and irate() are counter functions: they
// assume monotonic growth and read any decrease as a reset, which is exactly
// what happens when a re-clone with dropIfExists empties the target. deriv()
// is the gauge-native slope and returns the same per-second unit.
//
// This exists because an ETA panel using rate() over a fixed 15m window read
// 46 minutes on a clone that had 8 minutes left, and every test here passed.
func TestNoCounterFunctionsOnGauges(t *testing.T) {
	counterFn := regexp.MustCompile(`\b(rate|irate|increase|resets)\(\s*(pgcopydb_[a-z_]+)`)
	for name, d := range load(t) {
		for _, expr := range d.Exprs() {
			for _, m := range counterFn.FindAllStringSubmatch(expr, -1) {
				t.Errorf("%s applies %s() to %s, which is a gauge; use deriv() instead:\n  %s",
					name, m[1], m[2], expr)
			}
		}
	}
}

// A timeline built from the phase gauge shows only the phases that outlived a
// scrape. On e2e-follow-load the Streaming phase lasted one second and
// Prometheus holds no sample of it, while the same transition sits in the CR
// to the second. The detail dashboard must read the durable one, and must read
// it over the range: Forget drops the series when the Migration is deleted, and
// a flip retires the {type,status} pair the earlier transition was on, so a
// selector resolved at the range end answers for neither.
func TestDetailReadsConditionTransitions(t *testing.T) {
	const metric = "pgcopydb_migration_condition_transition_timestamp_seconds"
	var found bool
	for _, expr := range load(t)["migration-detail.json"].Exprs() {
		if !slices.Contains(Metrics(expr), metric) {
			continue
		}
		found = true
		if !strings.Contains(expr, "last_over_time(") || !strings.Contains(expr, "[$__range]") {
			t.Errorf("panel reads %s only where the range ends; wrap it in last_over_time(...[$__range]):\n  %s",
				metric, expr)
		}
	}
	if !found {
		t.Errorf("migration-detail.json has no panel reading %s", metric)
	}
}

// The verification tiles render four states off one gauge, and the two states
// that carry no result are the ones easily lost. -1 is a check the spec does
// not request; an absent series is one that has not run yet. Both used to read
// "Pending", so a Migration that never opted in waited forever on a check
// nobody had asked for.
func TestVerificationTilesMapEveryState(t *testing.T) {
	const metric = "pgcopydb_migration_verification_check"
	want := map[string]string{"1": "PASS", "0": "FAIL", "-1": "Deactivated"}
	var tiles int
	for _, p := range load(t)["migration-detail.json"].AllPanels() {
		if !p.Reads(metric) {
			continue
		}
		tiles++
		for value, text := range want {
			switch mv, ok := p.ValueMapping(value); {
			case !ok:
				t.Errorf("%s: %s is unmapped, so that state renders as a bare number", p.Title, value)
			case mv.Text != text:
				t.Errorf("%s: %s maps to %q, want %q", p.Title, value, mv.Text, text)
			}
		}
		if p.FieldConfig.Defaults.NoValue == "" {
			t.Errorf("%s: no noValue, so a check still to run renders empty", p.Title)
		}
	}
	if tiles != 2 {
		t.Errorf("panels reading %s = %d, want 2 (schema and data)", metric, tiles)
	}
}

// A phase gauge series stands only until the next scrape marks it stale, so a
// subquery stepping slower than the scrape walks straight past a short phase.
// At [$__range:1m] the timeline returned five of seven phases for a completed
// e2e-follow-auto, dropping Finalizing and Verifying outright.
func TestPhaseTimelineStepsAtTheScrape(t *testing.T) {
	// The scrape, matching the poll and the browser refresh, in seconds.
	const scrape = 10
	units := map[string]int{"s": 1, "m": 60, "h": 3600}
	step := regexp.MustCompile(`\[\$__range:(\d+)([smh])\]`)
	var found bool
	for name, d := range load(t) {
		for _, expr := range d.Exprs() {
			if !slices.Contains(Metrics(expr), phaseMetric) {
				continue
			}
			m := step.FindStringSubmatch(expr)
			if m == nil {
				continue
			}
			found = true
			n, err := strconv.Atoi(m[1])
			if err != nil {
				t.Fatalf("%s: unparsable step %q", name, m[0])
			}
			if got := n * units[m[2]]; got > scrape {
				t.Errorf("%s: subquery steps every %ds, want %ds or finer or it loses short phases:\n  %s",
					name, got, scrape, expr)
			}
		}
	}
	if !found {
		t.Error("no panel builds a timeline from pgcopydb_migration_phase")
	}
}

// Both timelines are keyed on the series, and a scrape target's identity is
// part of that: a restarted operator rescrapes the same transition under a new
// pod label and every row appears twice. Aggregating by the labels that carry
// meaning drops the target's.
func TestTimelinesSurviveAnOperatorRestart(t *testing.T) {
	want := map[string]string{
		phaseMetric: "min by (phase)",
		"pgcopydb_migration_condition_transition_timestamp_seconds": "max by (type, status)",
	}
	seen := map[string]bool{}
	for _, expr := range load(t)["migration-detail.json"].Exprs() {
		for metric, agg := range want {
			// Only the timelines aggregate; the stat tiles read the same
			// metrics at a point in time and reduce in Grafana instead.
			if !slices.Contains(Metrics(expr), metric) || !strings.Contains(expr, "_over_time(") {
				continue
			}
			seen[metric] = true
			if !strings.Contains(expr, agg) {
				t.Errorf("timeline over %s does not aggregate with %q, so it doubles its rows per operator pod:\n  %s",
					metric, agg, expr)
			}
		}
	}
	for metric := range want {
		if !seen[metric] {
			t.Errorf("migration-detail.json has no timeline over %s", metric)
		}
	}
}

// Every tile on the detail dashboard is scoped to one named Migration, so an
// empty result means that Migration is not here and a noValue saying anything
// else states a fact about an object it cannot see. "Still Running" stood on a
// Migration finished half an hour earlier and deleted since, next to "Not
// Started", "Pending" and an attempt count of 0 on the same run. A tile with
// something to say about an event that has not happened says it with a mapped
// sentinel, which only renders while the Migration is actually there.
//
// The rule is this dashboard's alone. A fleet tile counts a set, and an empty
// set really does have zero active migrations in it.
func TestDetailTilesReadNAWhenTheMigrationIsGone(t *testing.T) {
	var tiles int
	for _, p := range load(t)["migration-detail.json"].AllPanels() {
		if p.Type != statPanel {
			continue
		}
		tiles++
		if got := p.FieldConfig.Defaults.NoValue; got != "N/A" {
			t.Errorf("%q reads %q with no data, want N/A; say it with a mapped sentinel instead",
				p.Title, got)
		}
	}
	if tiles == 0 {
		t.Error("migration-detail.json has no stat tiles; this check is guarding nothing")
	}
}

// The sentinel is written into the expression as a multiple of the presence
// gauge, so an unmapped one renders as a bare number: -2 where the tile meant
// "Pending". Reading it back off the expression keeps the two in step.
func TestSentinelFallbacksAreMapped(t *testing.T) {
	sentinel := regexp.MustCompile(`pgcopydb_migration_info\{[^}]*\}\s*\*\s*(-?\d+)`)
	var found int
	for file, d := range load(t) {
		for _, p := range d.AllPanels() {
			for _, tg := range p.Targets {
				for _, m := range sentinel.FindAllStringSubmatch(tg.Expr, -1) {
					found++
					if _, ok := p.ValueMapping(m[1]); !ok {
						t.Errorf("%s: %q falls back to %s and does not map it, so it renders as a number:\n  %s",
							file, p.Title, m[1], tg.Expr)
					}
				}
			}
		}
	}
	if found == 0 {
		t.Error("no panel falls back to a sentinel; this check is guarding nothing")
	}
}

// A tile that reads history to survive a deleted Migration must stop reading it
// the moment one is there, because a range wide enough to hold a deleted run is
// wide enough to hold an earlier run that reused the name. Ungated, a live
// migration's Completed At showed the previous run's timestamp, 49 minutes
// before the one on screen had even started.
func TestHistoryYieldsToALiveMigration(t *testing.T) {
	for file, d := range load(t) {
		for _, p := range d.AllPanels() {
			if p.Type != statPanel {
				continue
			}
			for _, tg := range p.Targets {
				if !strings.Contains(tg.Expr, "last_over_time(") {
					continue
				}
				if !strings.Contains(tg.Expr, "unless") {
					t.Errorf("%s: %q reads over the range without giving way to a live Migration:\n  %s",
						file, p.Title, tg.Expr)
				}
			}
		}
	}
}

// The counters are sampled while the worker runs and stop when its pod exits,
// so a tile reading only at the range end shows N/A for every finished
// migration. Measured on e2e-follow-auto: no series at the range end, two over
// six hours. The same defect the other run-fact tiles already had fixed.
// A stat tile with colorMode "none" renders its value, and its noValue text,
// in the default foreground: on the detail dashboard that reads as a white
// N/A among blue ones. Every stat tile there carries a single blue threshold,
// so the colour is only ever reached through colorMode. Tiles that show a
// name rather than a reading are exempt: there is no value to colour.
// One stat tile at a different text size reads as a different kind of tile:
// Cutover Drain shipped at 48 while everything around it was 20. A dashboard
// may leave the size to Grafana, which fits the text to the panel, but where
// tiles state a size they have to agree.
func TestStatTilesShareOneTextSize(t *testing.T) {
	for name, d := range load(t) {
		sizes := map[float64]string{}
		for _, p := range d.AllPanels() {
			if p.Type != statPanel || p.Options.Text == nil || p.Options.Text.ValueSize == 0 {
				continue
			}
			sizes[p.Options.Text.ValueSize] = p.Title
		}
		if len(sizes) > 1 {
			t.Errorf("%s: stat panels state %d different value sizes: %v", name, len(sizes), sizes)
		}
	}
}

func TestStatTilesTakeTheirThresholdColour(t *testing.T) {
	for name, d := range load(t) {
		for _, p := range d.AllPanels() {
			if p.Type != statPanel || p.Options.ColorMode != "none" ||
				p.Options.TextMode == textModeName {
				continue
			}
			t.Errorf("%s: stat panel %q renders uncoloured; use value or background",
				name, p.Title)
		}
	}
}

// A panel reading a _bytes metric has to be told the value is bytes. Grafana's
// scaled units mean "this number already is gigabytes", so pointing one at a
// byte count multiplies it by a billion: the Bytes tile shipped as decgbytes
// and rendered a 537 MB fixture as 536.90 PB.
func TestByteMetricsUseAByteUnit(t *testing.T) {
	scaled := []string{"deckbytes", "decmbytes", "decgbytes", "dectbytes",
		"kbytes", "mbytes", "gbytes", "tbytes"}
	for name, d := range load(t) {
		for _, p := range d.AllPanels() {
			unit := p.FieldConfig.Defaults.Unit
			if !slices.Contains(scaled, unit) {
				continue
			}
			for _, tg := range p.Targets {
				for _, m := range Metrics(tg.Expr) {
					if strings.HasSuffix(m, "_bytes") {
						t.Errorf("%s: %q reads %s in bytes but renders it as %q",
							name, p.Title, m, unit)
					}
				}
			}
		}
	}
}

func TestCountTilesReadOverTheRange(t *testing.T) {
	counters := []string{
		"pgcopydb_migration_tables_total",
		"pgcopydb_migration_tables_done",
		"pgcopydb_migration_indexes_total",
		"pgcopydb_migration_indexes_done",
		"pgcopydb_migration_clone_planned_bytes",
		"pgcopydb_migration_clone_copied_bytes",
	}
	var found int
	for _, p := range load(t)["migration-detail.json"].AllPanels() {
		// Stat tiles only. A timeseries reads the range by construction, and
		// Copy Throughput is a live rate that should read where the range ends.
		if p.Type != statPanel {
			continue
		}
		for _, counter := range counters {
			if !p.Reads(counter) {
				continue
			}
			found++
			for _, tg := range p.Targets {
				if !slices.Contains(Metrics(tg.Expr), counter) {
					continue
				}
				if !strings.Contains(tg.Expr, "last_over_time(") {
					t.Errorf("%q reads %s only where the range ends:\n  %s", p.Title, counter, tg.Expr)
				}
			}
		}
	}
	if found != 6 {
		t.Errorf("found %d count tiles, want 6 (source and target for tables, indexes, bytes)", found)
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

func TestValueMapping(t *testing.T) {
	d, err := Parse([]byte(`{
	  "uid": "u", "title": "t",
	  "panels": [{
	    "title": "tile",
	    "fieldConfig": {"defaults": {"mappings": [
	      {"type": "range", "options": {"from": 0, "to": null, "result": {"text": "ignored"}}},
	      {"type": "value", "options": {"-1": {"text": "Deactivated", "color": "red"}, "9": "malformed"}}
	    ]}}
	  }]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	p := d.Panels[0]
	if mv, ok := p.ValueMapping("-1"); !ok || mv.Text != "Deactivated" || mv.Color != "red" {
		t.Errorf("ValueMapping(-1) = %+v, %v, want Deactivated/red", mv, ok)
	}
	// A range mapping keys on from/to, not on the value, so it must not answer
	// for one; an unmapped value and an unreadable option are both "no".
	for _, value := range []string{"0", "from", "9"} {
		if mv, ok := p.ValueMapping(value); ok {
			t.Errorf("ValueMapping(%s) = %+v, want no mapping", value, mv)
		}
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
