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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

func TestCloneArgs_Minimal(t *testing.T) {
	got := CloneArgs(&v1alpha1.MigrationSpec{}, false, false, false)
	assertArgs(t, got, "clone --dir /work/pgcopydb")
}

func TestCloneArgs_FirstAttemptRestarts(t *testing.T) {
	got := CloneArgs(&v1alpha1.MigrationSpec{}, true, false, false)
	assertArgs(t, got, "clone --dir /work/pgcopydb --restart")
}

func TestCloneArgs_Full(t *testing.T) {
	split := resource.MustParse("1Gi")
	spec := &v1alpha1.MigrationSpec{
		Clone: v1alpha1.CloneOptions{
			TableJobs:             8,
			IndexJobs:             4,
			RestoreJobs:           2,
			LargeObjectsJobs:      3,
			SplitTablesLargerThan: &split,
			SplitMaxParts:         5,
			EstimateTableSizes:    true,
			DropIfExists:          true,
			Roles:                 true,
			NoRolePasswords:       true,
			NoOwner:               true,
			NoACL:                 true,
			NoComments:            true,
			NoTablespaces:         true,
			UseCopyBinary:         true,
			FailFast:              true,
			Skip: []v1alpha1.SkipOption{
				"vacuum", "largeObjects", "analyze", "extensionComments",
			},
			Filters: &v1alpha1.Filters{ExcludeSchemas: []string{"audit"}},
		},
	}
	got := CloneArgs(spec, false, true, true)
	// Skips are sorted: analyze, extensionComments, largeObjects, vacuum.
	want := "clone --dir /work/pgcopydb" +
		" --table-jobs 8 --index-jobs 4 --restore-jobs 2 --large-objects-jobs 3" +
		" --split-tables-larger-than 1073741824 --split-max-parts 5" +
		" --estimate-table-sizes" +
		" --drop-if-exists --roles --no-role-passwords --no-owner --no-acl" +
		" --no-comments --no-tablespaces --use-copy-binary --fail-fast" +
		" --skip-analyze --skip-ext-comments --skip-large-objects --skip-vacuum" +
		" --filters /etc/pgcopydb/conf/filters.ini --resume --not-consistent"
	assertArgs(t, got, want)
}

func TestCloneArgs_EmptyFiltersOmitsFlag(t *testing.T) {
	spec := &v1alpha1.MigrationSpec{Clone: v1alpha1.CloneOptions{Filters: &v1alpha1.Filters{}}}
	for _, a := range CloneArgs(spec, false, false, false) {
		if a == "--filters" {
			t.Fatalf("empty filters must not add --filters: %v", CloneArgs(spec, false, false, false))
		}
	}
}

func TestCloneArgs_NoCredentialsInArgv(t *testing.T) {
	// The renderer must never emit source/target URIs; they go via env only.
	spec := &v1alpha1.MigrationSpec{
		Source: v1alpha1.PostgresConnection{Host: "secret-host", Username: "u"},
		Target: v1alpha1.PostgresConnection{Host: "t", Username: "u"},
	}
	for _, a := range CloneArgs(spec, false, false, false) {
		if strings.Contains(a, "secret-host") || strings.Contains(a, "--source") || strings.Contains(a, "--target") {
			t.Fatalf("argv leaked connection info: %v", a)
		}
	}
}

func TestRenderFilters_Order(t *testing.T) {
	f := &v1alpha1.Filters{
		IncludeOnlyTables: []string{"public.orders", "public.~/^audit_/"},
		ExcludeIndexes:    []string{"public.orders_idx"},
	}
	got := RenderFilters(f)
	want := "[include-only-table]\n" +
		"public.orders\n" +
		"public.~/^audit_/\n\n" +
		"[exclude-index]\n" +
		"public.orders_idx\n"
	if got != want {
		t.Fatalf("filters INI mismatch:\n got:\n%q\nwant:\n%q", got, want)
	}
}

func TestRenderFilters_Empty(t *testing.T) {
	if RenderFilters(nil) != "" {
		t.Fatal("nil filters must render empty")
	}
	if RenderFilters(&v1alpha1.Filters{}) != "" {
		t.Fatal("empty filters must render empty")
	}
}

func assertArgs(t *testing.T, got []string, want string) {
	t.Helper()
	if strings.Join(got, " ") != want {
		t.Fatalf("args mismatch:\n got: %s\nwant: %s", strings.Join(got, " "), want)
	}
}
