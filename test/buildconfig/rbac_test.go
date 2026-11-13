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

package buildconfig

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// The chart template is the generated rules block between Helm directives,
// so parsing from rules: with those lines dropped reads both files alike.
var helmDirective = regexp.MustCompile(`(?m)^\{\{.*\}\}\n`)

// The native transport opens exec as a WebSocket GET, which the API server
// authorizes as get, while the legacy SPDY fallback POSTs and needs create.
// Pinning the exact set also catches a widened grant.
func TestManagerRolesGrantPodsExecGetAndCreate(t *testing.T) {
	for _, path := range []string{
		"../../config/rbac/role.yaml",
		"../../charts/pgcopydb-operator/templates/clusterrole.yaml",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		_, rules, ok := strings.Cut(string(raw), "\nrules:")
		if !ok {
			t.Fatalf("%s has no rules block", path)
		}
		var role struct {
			Rules []rbacv1.PolicyRule `json:"rules"`
		}
		if err := yaml.Unmarshal([]byte(helmDirective.ReplaceAllString("rules:"+rules, "")), &role); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		var verbs []string
		for _, rule := range role.Rules {
			if (slices.Contains(rule.APIGroups, "") || slices.Contains(rule.APIGroups, "*")) &&
				slices.ContainsFunc(rule.Resources, func(r string) bool { return r == "pods/exec" || r == "pods/*" || r == "*" }) {
				verbs = append(verbs, rule.Verbs...)
			}
		}
		slices.Sort(verbs)
		if got := strings.Join(slices.Compact(verbs), ","); got != "create,get" {
			t.Errorf("%s grants pods/exec verbs %q, want \"create,get\"", path, got)
		}
	}
}
