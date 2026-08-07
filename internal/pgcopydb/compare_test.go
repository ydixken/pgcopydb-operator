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
