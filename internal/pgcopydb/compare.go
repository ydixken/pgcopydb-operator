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
	"encoding/json"
	"slices"
	"strings"
)

// CompareSchemaArgs renders the argv for `pgcopydb compare schema`. The
// connection URIs travel via the environment, like every other command.
func CompareSchemaArgs(allDatabases bool) []string {
	args := []string{"compare", "schema", flagDir, WorkDir}
	if allDatabases {
		args = append(args, "--all-databases")
	}
	return args
}

// CompareDataArgs requests JSON so compare_data_strict can reject empty or
// incomplete reports even when the comparator exits successfully.
func CompareDataArgs() []string {
	return []string{"compare", "data", flagDir, WorkDir, "--json"}
}

// CompareProgressPrefix opens the compare progress message the bundled runner's
// compare data logs at most 10 s apart, and once more after its workers end.
const CompareProgressPrefix = "compare progress: "

// CompareProgress is one compare progress line. The keys are the runner fork's
// contract (ydixken/pgcopydb PR #40); comparedTables counts every finished table.
type CompareProgress struct {
	PlannedTables   int64 `json:"plannedTables"`
	ComparedTables  int64 `json:"comparedTables"`
	PlannedBytes    int64 `json:"plannedBytes"`
	ComparedBytes   int64 `json:"comparedBytes"`
	ComparedRows    int64 `json:"comparedRows"`
	DifferingTables int64 `json:"differingTables"`
	FailedTables    int64 `json:"failedTables"`
	// ElapsedMs counts from the start of compare data, its schema read included.
	ElapsedMs int64 `json:"elapsedMs"`
	// Final marks the line the parent logs after every worker ended.
	Final    bool                   `json:"final"`
	InFlight []CompareInFlightTable `json:"inFlight"`
}

// CompareInFlightTable is a table a compare worker is on; ElapsedMs has whole-second resolution.
type CompareInFlightTable struct {
	Schema    string `json:"schema"`
	Name      string `json:"name"`
	Bytes     int64  `json:"bytes"`
	ElapsedMs int64  `json:"elapsedMs"`
}

// LastCompareProgress returns the newest compare progress line of a log tail
// that parses, plain or PGCOPYDB_LOG_JSON=on. A line cut short by the tail
// window or by a crash is skipped, so the one before it counts.
func LastCompareProgress(tail string) (CompareProgress, bool) {
	for _, msg := range slices.Backward(strings.Split(tail, "\n")) {
		if e, ok := parseLogLine(msg); ok {
			msg = e.Message
		}
		// The plain format puts time, pid and file:line before the message.
		_, raw, found := strings.Cut(msg, CompareProgressPrefix)
		if !found || !strings.HasPrefix(raw, "{") {
			continue
		}
		var p CompareProgress
		if json.Unmarshal([]byte(raw), &p) == nil {
			return p, true
		}
	}
	return CompareProgress{}, false
}
