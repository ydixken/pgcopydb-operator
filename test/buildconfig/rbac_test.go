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

var managerRoles = []string{
	"../../config/rbac/role.yaml",
	"../../charts/pgcopydb-operator/templates/clusterrole.yaml",
}

// coreVerbs returns the sorted verbs a role file grants on one core-group resource.
func coreVerbs(t *testing.T, path, resource string) string {
	t.Helper()
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
	group, _, _ := strings.Cut(resource, "/")
	var verbs []string
	for _, rule := range role.Rules {
		if (slices.Contains(rule.APIGroups, "") || slices.Contains(rule.APIGroups, "*")) &&
			slices.ContainsFunc(rule.Resources, func(r string) bool { return r == resource || r == group+"/*" || r == "*" }) {
			verbs = append(verbs, rule.Verbs...)
		}
	}
	slices.Sort(verbs)
	return strings.Join(slices.Compact(verbs), ",")
}

// The native transport opens exec as a WebSocket GET, which the API server
// authorizes as get, while the legacy SPDY fallback POSTs and needs create.
// Pinning the exact set also catches a widened grant.
func TestManagerRolesGrantPodsExecGetAndCreate(t *testing.T) {
	for _, path := range managerRoles {
		if got := coreVerbs(t, path, "pods/exec"); got != "create,get" {
			t.Errorf("%s grants pods/exec verbs %q, want \"create,get\"", path, got)
		}
	}
}

// The native follow work PVC has no owner reference, so the manager deletes it after cleanup.
// That delete is the one verb claims add; config maps must not gain it through a shared rule.
func TestManagerRolesGrantClaimDeleteOnly(t *testing.T) {
	for _, path := range managerRoles {
		for resource, want := range map[string]string{
			"persistentvolumeclaims": "create,delete,get,list,watch",
			"configmaps":             "create,get,list,watch",
		} {
			if got := coreVerbs(t, path, resource); got != want {
				t.Errorf("%s grants %s verbs %q, want %q", path, resource, got, want)
			}
		}
	}
}
