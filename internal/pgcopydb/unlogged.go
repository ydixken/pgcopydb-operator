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
	"slices"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// CloneFilters leaves opted-in UNLOGGED tables in the schema selection.
func CloneFilters(spec *v1beta1.MigrationSpec) *v1beta1.Filters {
	f := spec.Clone.Filters.DeepCopy()
	if spec.Follow == nil || len(spec.Follow.UnloggedTables) == 0 {
		return f
	}
	if f == nil {
		f = &v1beta1.Filters{}
	}
	for _, table := range spec.Follow.UnloggedTables {
		if table.Mode == "SchemaOnly" && !slices.Contains(f.ExcludeTableData, table.Table) {
			f.ExcludeTableData = append(f.ExcludeTableData, table.Table)
		}
	}
	return f
}

// DataFilters excludes opted-in tables from live content comparisons. Admission
// and the worker builder reject a selection with no remaining included table.
func DataFilters(spec *v1beta1.MigrationSpec) *v1beta1.Filters {
	f := CloneFilters(spec)
	if spec.Follow == nil || (len(spec.Follow.UnloggedTables) == 0 && len(spec.Follow.InsertOnlyTables) == 0) {
		return f
	}
	if f == nil {
		f = &v1beta1.Filters{}
	}
	included := len(f.IncludeOnlyTables) > 0
	tables := append([]string{}, spec.Follow.InsertOnlyTables...)
	for _, table := range spec.Follow.UnloggedTables {
		tables = append(tables, table.Table)
	}
	for _, table := range tables {
		if included {
			f.IncludeOnlyTables = slices.DeleteFunc(f.IncludeOnlyTables, func(t string) bool { return t == table })
		} else if !slices.Contains(f.ExcludeTables, table) {
			f.ExcludeTables = append(f.ExcludeTables, table)
		}
	}
	return f
}

// ManagedPublications returns the operator-owned publication names accepted by
// pgoutput's publication_names option. Explicit publications remain user-owned.
func ManagedPublications(spec *v1beta1.MigrationSpec, namespace, name string) []string {
	if spec.Follow == nil || (len(spec.Follow.UnloggedTables) == 0 && len(spec.Follow.InsertOnlyTables) == 0) {
		return nil
	}
	names := []string{SlotName(namespace, name)}
	if len(spec.Follow.InsertOnlyTables) > 0 {
		names = append(names, SlotName(namespace, name+"-insert"))
	}
	return names
}
