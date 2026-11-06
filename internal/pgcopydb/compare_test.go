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
	"reflect"
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
		{"schema", CompareSchemaArgs(false), "compare schema --dir /work/pgcopydb"},
		{"all databases schema", CompareSchemaArgs(true), "compare schema --dir /work/pgcopydb --all-databases"},
		{"data", CompareDataArgs(), "compare data --dir /work/pgcopydb --json"},
	} {
		if got := strings.Join(tc.got, " "); got != tc.want {
			t.Errorf("compare %s argv = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// progressLine is a real sample as compare data logs it when stderr is not a
// terminal: time, pid, level and file:line come before the message.
const progressLine = `2026-11-06 11:52:11.000 61234 INFO   compare.c:507             compare progress: ` +
	`{"plannedTables":3,"comparedTables":2,"plannedBytes":131072,"comparedBytes":24576,"comparedRows":20,` +
	`"differingTables":0,"failedTables":0,"elapsedMs":10059,"final":false,"inFlightCount":1,` +
	`"inFlight":[{"schema":"public","name":"a","bytes":106496,"elapsedMs":10000}]}`

// The same sample under PGCOPYDB_LOG_JSON=on, which the operator's Jobs set.
const progressJSONLine = `{"timestamp":"2026-11-06 11:52:11.000","pid":61234,"error_level":4,` +
	`"error_severity":"INFO","file_name":"compare.c","file_line_num":512,"message":"compare progress: ` +
	`{\"plannedTables\":3,\"comparedTables\":2,\"plannedBytes\":131072,\"comparedBytes\":24576,` +
	`\"comparedRows\":20,\"differingTables\":0,\"failedTables\":0,\"elapsedMs\":10059,\"final\":false,` +
	`\"inFlightCount\":1,\"inFlight\":[{\"schema\":\"public\",\"name\":\"a\",\"bytes\":106496,\"elapsedMs\":10000}]}"}`

const finalJSONLine = `{"error_severity":"INFO","message":"compare progress: ` +
	`{\"plannedTables\":3,\"comparedTables\":3,\"plannedBytes\":131072,\"comparedBytes\":131072,` +
	`\"comparedRows\":30,\"differingTables\":1,\"failedTables\":1,\"elapsedMs\":20480,\"final\":true,` +
	`\"inFlightCount\":0,\"inFlight\":[]}"}`

// What compare_data_strict prints after the final line: the pretty report, then the verdict.
const prettyReport = `[
  {
    "schema": "public",
    "name": "a",
    "source": {
      "rowcount": 10,
      "checksum": "f0"
    },
    "status": "match"
  }
]`

func TestLastCompareProgress(t *testing.T) {
	sample := CompareProgress{
		PlannedTables: 3, ComparedTables: 2, PlannedBytes: 131072, ComparedBytes: 24576, ComparedRows: 20,
		ElapsedMs: 10059,
		InFlight:  []CompareInFlightTable{{Schema: "public", Name: "a", Bytes: 106496, ElapsedMs: 10000}},
	}
	final := CompareProgress{
		PlannedTables: 3, ComparedTables: 3, PlannedBytes: 131072, ComparedBytes: 131072, ComparedRows: 30,
		DifferingTables: 1, FailedTables: 1, ElapsedMs: 20480, Final: true, InFlight: []CompareInFlightTable{},
	}
	const errLine = `{"error_severity":"ERROR","message":"Table \"public\".\"b\" has 10 rows on source, 9 rows on target"}`
	older := strings.Replace(progressLine, `"comparedTables":2`, `"comparedTables":1`, 1)
	for _, tc := range []struct {
		name string
		tail string
		want CompareProgress
		ok   bool
	}{
		{"the real plain line", progressLine, sample, true},
		{"a PGCOPYDB_LOG_JSON=on line", progressJSONLine, sample, true},
		{
			name: "a truncated line yields the sample before it",
			tail: progressJSONLine + "\n" + finalJSONLine[:120],
			want: sample, ok: true,
		},
		{
			name: "error lines between samples",
			tail: errLine + "\n" + older + "\n" + errLine + "\n" + progressJSONLine + "\n" + errLine + "\n" + errLine,
			want: sample, ok: true,
		},
		{
			name: "a final line under a pretty report",
			tail: progressJSONLine + "\n" + finalJSONLine + "\n" + prettyReport + "\n" +
				`{"error_severity":"FATAL","message":"Failed to compare 1 table(s), see above for details"}` + "\n" +
				"compare data exited 12",
			want: final, ok: true,
		},
		{"no progress line", errLine + "\n" + prettyReport, CompareProgress{}, false},
		{"a message that is not a sample", "compare progress: null\ncompare progress: unavailable", CompareProgress{}, false},
		{"an empty tail", "", CompareProgress{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := LastCompareProgress(tc.tail)
			if ok != tc.ok || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("LastCompareProgress = (%+v, %t), want (%+v, %t)", got, ok, tc.want, tc.ok)
			}
		})
	}
}
