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

package pgcopydb

import (
	"regexp"
	"strings"
	"testing"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

func TestFollowArgs_Disabled(t *testing.T) {
	if got := FollowArgs(&v1alpha1.MigrationSpec{}, "ns", "m"); got != nil {
		t.Fatalf("nil expected without follow, got %v", got)
	}
	spec := &v1alpha1.MigrationSpec{Follow: &v1alpha1.FollowOptions{Enabled: false}}
	if got := FollowArgs(spec, "ns", "m"); got != nil {
		t.Fatalf("nil expected with follow disabled, got %v", got)
	}
}

func TestFollowArgs_Defaults(t *testing.T) {
	spec := &v1alpha1.MigrationSpec{Follow: &v1alpha1.FollowOptions{Enabled: true, Plugin: "pgoutput"}}
	got := strings.Join(FollowArgs(spec, "shop", "to-cnpg"), " ")
	slot := SlotName("shop", "to-cnpg")
	want := "--follow --slot-name " + slot + " --origin " + slot + " --plugin pgoutput"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFollowArgs_Full(t *testing.T) {
	spec := &v1alpha1.MigrationSpec{Follow: &v1alpha1.FollowOptions{
		Enabled: true, Plugin: "wal2json", SlotName: "my_slot",
		Publication: "my_pub", Wal2jsonNumericAsString: true, ReplayNoOpUpdates: true,
	}}
	got := strings.Join(FollowArgs(spec, "ns", "m"), " ")
	want := "--follow --slot-name my_slot --origin my_slot --plugin wal2json" +
		" --publication my_pub --wal2json-numeric-as-string --replay-no-op-updates"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestSlotName(t *testing.T) {
	valid := regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
	cases := []struct{ ns, name string }{
		{"shop", "to-cnpg"},
		{"UPPER-case", "We.ird--Name"},
		{strings.Repeat("verylongnamespace", 5), strings.Repeat("verylongname", 6)},
	}
	seen := map[string]bool{}
	for _, c := range cases {
		s := SlotName(c.ns, c.name)
		if !valid.MatchString(s) {
			t.Fatalf("invalid slot name %q for %v", s, c)
		}
		if seen[s] {
			t.Fatalf("collision on %q", s)
		}
		seen[s] = true
		// Deterministic.
		if SlotName(c.ns, c.name) != s {
			t.Fatal("slot name not deterministic")
		}
	}
	// Distinct inputs that sanitize identically must not collide (hash).
	if SlotName("a-b", "c") == SlotName("a.b", "c") {
		t.Fatal("sanitization collision not disambiguated by hash")
	}
}
