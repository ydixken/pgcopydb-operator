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

// Package dashboards parses the chart's Grafana dashboards far enough to lint
// their PromQL against the operator's metric registry; the e2e suite reuses
// it to substitute variables and replay panel queries against Prometheus.
package dashboards

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// Datasource is a panel or target data source reference.
type Datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

// Target is one query on a panel.
type Target struct {
	RefID      string      `json:"refId"`
	Expr       string      `json:"expr"`
	Datasource *Datasource `json:"datasource"`
}

// Mapping is one Grafana value mapping. Options carries a different shape per
// type (a value mapping keys on the value, a range mapping on from/to/result),
// so it stays raw until a caller asks about a specific value.
type Mapping struct {
	Type    string                     `json:"type"`
	Options map[string]json.RawMessage `json:"options"`
}

// MappedValue is what a panel renders in place of one raw value.
type MappedValue struct {
	Text  string `json:"text"`
	Color string `json:"color"`
}

// FieldConfig is the subset of a panel's field config these checks read.
type FieldConfig struct {
	Defaults struct {
		NoValue  string    `json:"noValue"`
		Mappings []Mapping `json:"mappings"`
	} `json:"defaults"`
}

// Panel is a dashboard panel; a row panel nests its panels when collapsed.
type Panel struct {
	Title       string      `json:"title"`
	Type        string      `json:"type"`
	Datasource  *Datasource `json:"datasource"`
	Targets     []Target    `json:"targets"`
	Panels      []Panel     `json:"panels"`
	FieldConfig FieldConfig `json:"fieldConfig"`
}

// ValueMapping reports how the panel renders value, and whether it maps it at
// all. Only mappings of type "value" key on the value itself.
func (p Panel) ValueMapping(value string) (MappedValue, bool) {
	for _, m := range p.FieldConfig.Defaults.Mappings {
		if m.Type != "value" {
			continue
		}
		raw, ok := m.Options[value]
		if !ok {
			continue
		}
		var mv MappedValue
		if err := json.Unmarshal(raw, &mv); err != nil {
			return MappedValue{}, false
		}
		return mv, true
	}
	return MappedValue{}, false
}

// Reads reports whether any of the panel's targets query metric.
func (p Panel) Reads(metric string) bool {
	for _, t := range p.Targets {
		if slices.Contains(Metrics(t.Expr), metric) {
			return true
		}
	}
	return false
}

// Variable is one templating variable.
type Variable struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Query      json.RawMessage `json:"query"`
	Datasource *Datasource     `json:"datasource"`
}

// Dashboard is the subset of a Grafana dashboard the lint and e2e sweeps use.
type Dashboard struct {
	UID           string `json:"uid"`
	Title         string `json:"title"`
	SchemaVersion int    `json:"schemaVersion"`
	// Refresh is the browser's auto-refresh, the last of the three intervals
	// between a gauge moving and a human seeing it (poll, scrape, refresh).
	Refresh    string `json:"refresh"`
	Templating struct {
		List []Variable `json:"list"`
	} `json:"templating"`
	Panels []Panel `json:"panels"`
}

// Parse decodes one dashboard.
func Parse(data []byte) (*Dashboard, error) {
	var d Dashboard
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("parse dashboard: %w", err)
	}
	if d.UID == "" || d.Title == "" {
		return nil, fmt.Errorf("dashboard %q lacks a uid or title", d.Title)
	}
	return &d, nil
}

// Load parses every *.json file in dir, keyed by file name.
func Load(dir string) (map[string]*Dashboard, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no dashboards in %s", dir)
	}
	out := make(map[string]*Dashboard, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		d, err := Parse(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(p), err)
		}
		out[filepath.Base(p)] = d
	}
	return out, nil
}

// AllPanels flattens the panel tree, including panels nested in rows.
func (d *Dashboard) AllPanels() []Panel {
	var out []Panel
	var walk func([]Panel)
	walk = func(ps []Panel) {
		for _, p := range ps {
			out = append(out, p)
			walk(p.Panels)
		}
	}
	walk(d.Panels)
	return out
}

// Exprs returns every non-empty target expression on the dashboard.
func (d *Dashboard) Exprs() []string {
	var out []string
	for _, p := range d.AllPanels() {
		for _, t := range p.Targets {
			if t.Expr != "" {
				out = append(out, t.Expr)
			}
		}
	}
	return out
}

var (
	stringLit    = regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)
	groupClause  = regexp.MustCompile(`\b(?:by|on|ignoring|without)\s*\([^)]*\)`)
	labelMatcher = regexp.MustCompile(`\{[^}]*\}`)
	rangeSel     = regexp.MustCompile(`\[[^]]*]`)
	identifier   = regexp.MustCompile(`[a-zA-Z_:][a-zA-Z0-9_:]*`)
	varRef       = regexp.MustCompile(`\$(?:\{(\w+)\}|(\w+))`)
)

// promKeywords are PromQL operators an identifier scan would mistake for
// metric names; functions are recognized by their call parenthesis instead.
var promKeywords = map[string]bool{
	"and": true, "or": true, "unless": true, "bool": true, "offset": true,
	"group_left": true, "group_right": true,
}

// Metrics extracts the metric names referenced by a PromQL expression.
func Metrics(expr string) []string {
	s := stringLit.ReplaceAllString(expr, " ")
	s = groupClause.ReplaceAllString(s, " ")
	s = labelMatcher.ReplaceAllString(s, " ")
	s = rangeSel.ReplaceAllString(s, " ")
	seen := map[string]bool{}
	for _, loc := range identifier.FindAllStringIndex(s, -1) {
		name := s[loc[0]:loc[1]]
		// A '$' right before the identifier marks a variable, not a metric.
		if loc[0] > 0 && (s[loc[0]-1] == '$' || s[loc[0]-1] == '{') {
			continue
		}
		if promKeywords[name] {
			continue
		}
		rest := strings.TrimLeft(s[loc[1]:], " \t")
		if strings.HasPrefix(rest, "(") { // function or aggregation call
			continue
		}
		seen[name] = true
	}
	return slices.Sorted(maps.Keys(seen))
}

// Vars extracts the template variables ($name or ${name}) an expression uses.
func Vars(expr string) []string {
	seen := map[string]bool{}
	for _, m := range varRef.FindAllStringSubmatch(expr, -1) {
		name := m[1]
		if name == "" {
			name = m[2]
		}
		seen[name] = true
	}
	return slices.Sorted(maps.Keys(seen))
}

// rateInterval is the Grafana builtin the chart's rate() panels lean on.
const rateInterval = "__rate_interval"

// builtinDefaults stand in for Grafana's interval builtins when the caller
// does not override them.
var builtinDefaults = map[string]string{
	rateInterval: "5m",
	"__interval": "1m",
	"__range":    "1h",
}

// Substitute replaces $name and ${name} references with the given values;
// Grafana's interval builtins default so panel queries stay runnable.
func Substitute(expr string, vars map[string]string) string {
	merged := make(map[string]string, len(builtinDefaults)+len(vars))
	maps.Copy(merged, builtinDefaults)
	maps.Copy(merged, vars)
	// Longest first so $namespace never partially matches as $name.
	keys := slices.SortedFunc(maps.Keys(merged), func(a, b string) int {
		if d := len(b) - len(a); d != 0 {
			return d
		}
		return strings.Compare(a, b)
	})
	for _, k := range keys {
		expr = strings.ReplaceAll(expr, "${"+k+"}", merged[k])
		re := regexp.MustCompile(`\$` + regexp.QuoteMeta(k) + `\b`)
		expr = re.ReplaceAllString(expr, merged[k])
	}
	return expr
}
