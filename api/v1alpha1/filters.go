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

package v1alpha1

// isEmpty reports whether no filter section holds any entry.
func (f *Filters) IsEmpty() bool {
	if f == nil {
		return true
	}
	return len(f.IncludeOnlyTables) == 0 &&
		len(f.ExcludeTables) == 0 &&
		len(f.IncludeOnlySchemas) == 0 &&
		len(f.ExcludeSchemas) == 0 &&
		len(f.ExcludeIndexes) == 0 &&
		len(f.ExcludeTableData) == 0 &&
		len(f.IncludeOnlyExtensions) == 0 &&
		len(f.ExcludeExtensions) == 0
}
