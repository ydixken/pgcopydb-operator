/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

package pgcopydb

import (
	"regexp"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func TestFollowArgs_Disabled(t *testing.T) {
	if got := FollowArgs(&v1beta1.MigrationSpec{}, "ns", "m"); got != nil {
		t.Fatalf("nil expected without follow, got %v", got)
	}
	spec := &v1beta1.MigrationSpec{Follow: &v1beta1.FollowOptions{Enabled: false}}
	if got := FollowArgs(spec, "ns", "m"); got != nil {
		t.Fatalf("nil expected with follow disabled, got %v", got)
	}
}

func TestFollowArgs_Defaults(t *testing.T) {
	spec := &v1beta1.MigrationSpec{Follow: &v1beta1.FollowOptions{Enabled: true, Plugin: "pgoutput"}}
	got := strings.Join(FollowArgs(spec, "shop", "to-cnpg"), " ")
	slot := SlotName("shop", "to-cnpg")
	want := "--follow --slot-name " + slot + " --origin " + slot + " --plugin pgoutput"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFollowArgs_Full(t *testing.T) {
	spec := &v1beta1.MigrationSpec{Follow: &v1beta1.FollowOptions{
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
