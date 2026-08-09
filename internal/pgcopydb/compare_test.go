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
)

// Golden argv per check: the exact command line is the contract with the
// runner image, so any drift must fail here.
func TestCompareArgs(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  []string
		want string
	}{
		{"schema", CompareSchemaArgs(), "compare schema --dir /work/pgcopydb"},
		{"data", CompareDataArgs(), "compare data --dir /work/pgcopydb --json"},
	} {
		if got := strings.Join(tc.got, " "); got != tc.want {
			t.Errorf("compare %s argv = %q, want %q", tc.name, got, tc.want)
		}
	}
}
