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

// Package pgcopydb renders a Migration spec into pgcopydb command-line
// arguments and a --filters INI file. It is pure: no Kubernetes, no I/O, so it
// is exhaustively covered by golden tests. Credentials never appear here;
// connection strings are supplied to the runner through the environment
// (PGCOPYDB_SOURCE_PGURI / PGCOPYDB_TARGET_PGURI), not argv.
package pgcopydb

import (
	"fmt"
	"hash/crc32"
	"slices"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// WorkMount is where the work volume is mounted. The pgcopydb work dir lives
// one level below it: --restart removes the work dir wholesale, which cannot
// work on the mount point itself.
const WorkMount = "/work"

// WorkDir is the pgcopydb --dir path; the unit of resumability.
const WorkDir = WorkMount + "/pgcopydb"

// flagDir is the pgcopydb work-directory flag.
const flagDir = "--dir"

// FiltersPath is where the rendered --filters INI is mounted in the runner
// (from an operator-owned ConfigMap), when filters are configured. The mount
// in resources.go derives its directory from this path; there is no second
// copy of the literal.
const FiltersPath = "/etc/pgcopydb/conf/filters.ini"

// skipFlag maps a SkipOption to its pgcopydb flag.
var skipFlag = map[v1beta1.SkipOption]string{
	v1beta1.SkipOption("largeObjects"):      "--skip-large-objects",
	v1beta1.SkipOption("extensions"):        "--skip-extensions",
	v1beta1.SkipOption("extensionComments"): "--skip-ext-comments",
	v1beta1.SkipOption("collations"):        "--skip-collations",
	v1beta1.SkipOption("vacuum"):            "--skip-vacuum",
	v1beta1.SkipOption("analyze"):           "--skip-analyze",
	v1beta1.SkipOption("dbProperties"):      "--skip-db-properties",
	v1beta1.SkipOption("ctidSplit"):         "--skip-split-by-ctid",
}

// CloneArgs renders the argv for `pgcopydb clone` from a Migration spec.
// restart=true adds --restart: first attempts wipe the work directory, since
// a fresh Migration can only find foreign state there (a stale volume from a
// deleted Migration otherwise poisons the run). resume=true adds --resume
// (retry attempts, when the catalogs are this Migration's own). notConsistent
// adds --not-consistent, needed when the original snapshot transaction is
// gone. The source/target URIs are passed via the environment, so they are
// deliberately absent from the returned argv.
func CloneArgs(spec *v1beta1.MigrationSpec, restart, resume, notConsistent bool) []string {
	c := spec.Clone
	args := []string{"clone", flagDir, WorkDir}

	if c.TableJobs > 0 {
		args = append(args, "--table-jobs", itoa(c.TableJobs))
	}
	if c.IndexJobs > 0 {
		args = append(args, "--index-jobs", itoa(c.IndexJobs))
	}
	if c.RestoreJobs > 0 {
		args = append(args, "--restore-jobs", itoa(c.RestoreJobs))
	}
	if c.LargeObjectsJobs > 0 {
		args = append(args, "--large-objects-jobs", itoa(c.LargeObjectsJobs))
	}
	if c.SplitTablesLargerThan != nil {
		// pgcopydb accepts a plain byte count; resource.Quantity.Value() gives bytes.
		args = append(args, "--split-tables-larger-than", fmt.Sprintf("%d", c.SplitTablesLargerThan.Value()))
	}
	if c.SplitMaxParts > 0 {
		args = append(args, "--split-max-parts", itoa(c.SplitMaxParts))
	}
	if c.EstimateTableSizes {
		args = append(args, "--estimate-table-sizes")
	}
	if c.DropIfExists {
		args = append(args, "--drop-if-exists")
	}
	if c.Roles {
		args = append(args, "--roles")
	}
	if c.NoRolePasswords {
		args = append(args, "--no-role-passwords")
	}
	if c.NoOwner {
		args = append(args, "--no-owner")
	}
	if c.NoACL {
		args = append(args, "--no-acl")
	}
	if c.NoComments {
		args = append(args, "--no-comments")
	}
	if c.NoTablespaces {
		args = append(args, "--no-tablespaces")
	}
	if c.UseCopyBinary {
		args = append(args, "--use-copy-binary")
	}
	if c.FailFast {
		args = append(args, "--fail-fast")
	}

	// Skip flags in a deterministic order so the argv is stable for golden tests.
	skips := append([]v1beta1.SkipOption(nil), c.Skip...)
	slices.Sort(skips)
	for _, s := range skips {
		if flag, ok := skipFlag[s]; ok {
			args = append(args, flag)
		}
	}

	if c.Filters != nil && !c.Filters.IsEmpty() {
		args = append(args, "--filters", FiltersPath)
	}
	if restart {
		args = append(args, "--restart")
	}
	if resume {
		args = append(args, "--resume")
	}
	if notConsistent {
		args = append(args, "--not-consistent")
	}
	return args
}

// RenderFilters produces the pgcopydb --filters INI, or "" when no filters are
// set. Section order matches the pgcopydb documentation; entries keep the
// user's order (they are identifiers, and pgcopydb treats them as a set).
func RenderFilters(f *v1beta1.Filters) string {
	if f == nil || f.IsEmpty() {
		return ""
	}
	var b strings.Builder
	section := func(name string, entries []string) {
		if len(entries) == 0 {
			return
		}
		fmt.Fprintf(&b, "[%s]\n", name)
		for _, e := range entries {
			fmt.Fprintf(&b, "%s\n", e)
		}
		b.WriteByte('\n')
	}
	section("include-only-schema", f.IncludeOnlySchemas)
	section("include-only-table", f.IncludeOnlyTables)
	section("exclude-schema", f.ExcludeSchemas)
	section("exclude-table", f.ExcludeTables)
	section("exclude-table-data", f.ExcludeTableData)
	section("exclude-index", f.ExcludeIndexes)
	section("include-only-extension", f.IncludeOnlyExtensions)
	section("exclude-extension", f.ExcludeExtensions)
	return strings.TrimRight(b.String(), "\n") + "\n"
}

func itoa(i int32) string { return fmt.Sprintf("%d", i) }

// SlotName derives a deterministic, per-Migration replication slot and origin
// name. Postgres slot names allow [a-z0-9_], max 63 bytes; namespace and name
// are sanitized and a short hash keeps truncated names collision-free.
func SlotName(namespace, name string) string {
	raw := namespace + "_" + name
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		default:
			return '_'
		}
	}, raw)
	sum := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(raw)))
	const max = 63 - 1 - 8 - len("pgcopydb_")
	if len(sanitized) > max {
		sanitized = sanitized[:max]
	}
	return "pgcopydb_" + sanitized + "_" + sum
}

// FollowArgs renders the argv additions for a live migration; empty when
// follow is not enabled. The slot and origin default to the generated
// per-Migration name so several migrations can share one source instance.
func FollowArgs(spec *v1beta1.MigrationSpec, namespace, name string) []string {
	f := spec.Follow
	if f == nil || !f.Enabled {
		return nil
	}
	slot := f.SlotName
	if slot == "" {
		slot = SlotName(namespace, name)
	}
	args := []string{"--follow", "--slot-name", slot, "--origin", slot}
	if f.Plugin != "" {
		args = append(args, "--plugin", f.Plugin)
	}
	if f.Publication != "" {
		args = append(args, "--publication", f.Publication)
	}
	// CEL admission restricts this to plugin wal2json, so no plugin check here.
	if f.Wal2jsonNumericAsString {
		args = append(args, "--wal2json-numeric-as-string")
	}
	if f.ReplayNoOpUpdates {
		args = append(args, "--replay-no-op-updates")
	}
	return args
}
