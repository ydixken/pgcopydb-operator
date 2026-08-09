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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func TestCloneArgs_Minimal(t *testing.T) {
	got := CloneArgs(&v1beta1.MigrationSpec{}, false, false, false)
	assertArgs(t, got, "clone --dir /work/pgcopydb")
}

func TestCloneArgs_FirstAttemptRestarts(t *testing.T) {
	got := CloneArgs(&v1beta1.MigrationSpec{}, true, false, false)
	assertArgs(t, got, "clone --dir /work/pgcopydb --restart")
}

func TestCloneArgs_Full(t *testing.T) {
	split := resource.MustParse("1Gi")
	spec := &v1beta1.MigrationSpec{
		Clone: v1beta1.CloneOptions{
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
			Skip: []v1beta1.SkipOption{
				"vacuum", "largeObjects", "analyze", "extensionComments",
			},
			Filters: &v1beta1.Filters{ExcludeSchemas: []string{"audit"}},
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
	spec := &v1beta1.MigrationSpec{Clone: v1beta1.CloneOptions{Filters: &v1beta1.Filters{}}}
	for _, a := range CloneArgs(spec, false, false, false) {
		if a == "--filters" {
			t.Fatalf("empty filters must not add --filters: %v", CloneArgs(spec, false, false, false))
		}
	}
}

func TestCloneArgs_NoCredentialsInArgv(t *testing.T) {
	// The renderer must never emit source/target URIs; they go via env only.
	spec := &v1beta1.MigrationSpec{
		Source: v1beta1.PostgresConnection{Host: "secret-host", Username: "u"},
		Target: v1beta1.PostgresConnection{Host: "t", Username: "u"},
	}
	for _, a := range CloneArgs(spec, false, false, false) {
		if strings.Contains(a, "secret-host") || strings.Contains(a, "--source") || strings.Contains(a, "--target") {
			t.Fatalf("argv leaked connection info: %v", a)
		}
	}
}

func TestRenderFilters_Order(t *testing.T) {
	f := &v1beta1.Filters{
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
	if RenderFilters(&v1beta1.Filters{}) != "" {
		t.Fatal("empty filters must render empty")
	}
}

func assertArgs(t *testing.T, got []string, want string) {
	t.Helper()
	if strings.Join(got, " ") != want {
		t.Fatalf("args mismatch:\n got: %s\nwant: %s", strings.Join(got, " "), want)
	}
}
