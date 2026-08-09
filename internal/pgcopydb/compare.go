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

// CompareSchemaArgs renders the argv for `pgcopydb compare schema`. The
// connection URIs travel via the environment, like every other command.
func CompareSchemaArgs() []string {
	return []string{"compare", "schema", flagDir, WorkDir}
}

// CompareDataArgs renders the argv for `pgcopydb compare data`. --json makes
// the per-table result parseable from the Job logs.
func CompareDataArgs() []string {
	return []string{"compare", "data", flagDir, WorkDir, "--json"}
}
