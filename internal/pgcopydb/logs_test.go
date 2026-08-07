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
