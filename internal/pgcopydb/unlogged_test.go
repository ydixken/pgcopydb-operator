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
	"reflect"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func TestPartialTableFilters(t *testing.T) {
	const empty, once, inserts, full = "app.locks", "app.cache", "app.events", "app.items"
	spec := &v1beta1.MigrationSpec{Follow: &v1beta1.FollowOptions{
		Enabled:          true,
		UnloggedTables:   []v1beta1.UnloggedTable{{Table: empty, Mode: "SchemaOnly"}, {Table: once, Mode: "CopyOnce"}},
		InsertOnlyTables: []string{inserts},
	}}
	for _, include := range []bool{false, true} {
		spec.Clone.Filters = &v1beta1.Filters{ExcludeTableData: []string{"app.other"}}
		if include {
			spec.Clone.Filters.IncludeOnlyTables = []string{empty, once, inserts, full}
		}
		before := spec.DeepCopy()
		clone := CloneFilters(spec)
		if !slices.Contains(clone.ExcludeTableData, empty) || slices.Contains(clone.ExcludeTableData, once) ||
			slices.Contains(clone.ExcludeTableData, inserts) || len(clone.ExcludeTables) != 0 {
			t.Fatalf("clone must keep the schema and only skip SchemaOnly data: %+v", clone)
		}
		data := DataFilters(spec)
		if include {
			if !reflect.DeepEqual(data.IncludeOnlyTables, []string{full}) || len(data.ExcludeTables) > 0 {
				t.Fatalf("data comparison must narrow includes without incompatible excludes: %+v", data)
			}
		} else if !reflect.DeepEqual(data.ExcludeTables, []string{inserts, empty, once}) {
			t.Fatalf("data comparison selection = %+v", data)
		}
		if !reflect.DeepEqual(spec, before) {
			t.Fatal("filter rendering mutated the Migration spec")
		}
	}
}

func TestPartialTablePublicationArguments(t *testing.T) {
	spec := &v1beta1.MigrationSpec{Follow: &v1beta1.FollowOptions{Enabled: true}}
	if names := ManagedPublications(spec, "ns", "migration"); names != nil {
		t.Fatal(names)
	}
	spec.Follow.UnloggedTables = []v1beta1.UnloggedTable{{Table: "app.cache", Mode: "CopyOnce"}}
	names := ManagedPublications(spec, "ns", strings.Repeat("x", 200))
	if len(names) != 1 {
		t.Fatal(names)
	}
	spec.Follow.InsertOnlyTables = []string{"app.events"}
	names = ManagedPublications(spec, "ns", strings.Repeat("x", 200))
	if len(names) != 2 || names[0] == names[1] {
		t.Fatal(names)
	}
	for _, name := range names {
		if len(name) > 63 {
			t.Fatal("publication name exceeds PostgreSQL's identifier limit")
		}
	}
	args := FollowArgs(spec, "ns", strings.Repeat("x", 200))
	if args[len(args)-2] != "--publication" || args[len(args)-1] != strings.Join(names, ",") {
		t.Fatal(args)
	}
	spec.Clone.Filters = nil
	spec.Follow.UnloggedTables[0].Mode = "SchemaOnly"
	if !slices.Contains(CloneArgs(spec, true, false, false), "--filters") {
		t.Fatal("SchemaOnly needs an INI even without user filters")
	}
}
