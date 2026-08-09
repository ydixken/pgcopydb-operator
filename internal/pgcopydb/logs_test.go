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

import "testing"

func TestLastErrorLine(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "last error wins over earlier ones",
			raw: `{"timestamp":"t","pid":1,"error_severity":"INFO","message":"STEP 1: starting"}
{"timestamp":"t","pid":1,"error_severity":"ERROR","message":"permission denied for function pg_replication_origin_drop"}
{"timestamp":"t","pid":1,"error_severity":"ERROR","message":"pgcopydb clone failed"}
{"timestamp":"t","pid":1,"error_severity":"INFO","message":"shutting down"}`,
			want: "pgcopydb clone failed",
		},
		{
			name: "fatal counts as error",
			raw:  `{"error_severity":"FATAL","message":"connection to source lost"}`,
			want: "connection to source lost",
		},
		{
			name: "non-JSON and partial lines are skipped",
			raw: `DROP PUBLICATION
{"error_severity":"ERROR","message":"boom"}
{"error_severity":"ERROR","mess`,
			want: "boom",
		},
		{name: "no error lines", raw: `{"error_severity":"INFO","message":"done"}`, want: ""},
		{name: "empty input", raw: "", want: ""},
		{name: "plain text only", raw: "panic: not json\nERROR: still not json\n", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := LastErrorLine([]byte(tc.raw)); got != tc.want {
				t.Fatalf("LastErrorLine() = %q, want %q", got, tc.want)
			}
		})
	}
}
