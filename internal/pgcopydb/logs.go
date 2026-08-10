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
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

// LastErrorLine extracts the message of the last ERROR-or-worse entry from a
// PGCOPYDB_LOG_JSON=on log stream (one JSON object per line, fields
// error_severity and message). Runners emit these on stderr; the operator
// feeds in the tail of a failed pod's logs to surface the terminal cause in
// conditions and events. Lines that do not parse as log entries (psql
// output, partial writes) are skipped, so mixed or truncated input degrades
// to "" and the caller falls back to the Job's own failure message.
func LastErrorLine(raw []byte) string {
	type entry struct {
		Severity string `json:"error_severity"`
		Message  string `json:"message"`
	}
	var last string
	for line := range strings.Lines(string(raw)) {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var e entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue
		}
		switch e.Severity {
		// pgcopydb's log levels above WARN; CRITICAL/PANIC accepted in case
		// the naming ever shifts toward the PostgreSQL severities.
		case "ERROR", "FATAL", "CRITICAL", "PANIC":
			if e.Message != "" {
				last = e.Message
			}
		}
	}
	return last
}

// Clone-done markers in clone --follow mode, from the pgcopydb 0.18 source
// (src/bin/pgcopydb/copydb_clone_database in cli_clone_follow.c). After the
// post-data restore (whose banner, "STEP 10: restore the post-data section to
// the target database", still counts as mid-copy) the clone sub-process logs,
// in order:
//
//	log_info("Updating the pgcopydb.sentinel to enable applying changes");  (line 1061, follow only)
//	log_info("All step are now done, %s elapsed", timing->ppDuration);      (line 1077)
//
// The first line announces exactly the transition the operator wants: base
// copy finished, change replay may start. The second confirms it once the
// summary timing is closed. Both strings appear nowhere else in the 0.18
// source; matching either keeps detection alive if upstream rewords one.
const (
	markerSentinelApply = "Updating the pgcopydb.sentinel to enable applying changes"
	markerAllStepsDone  = "All step are now done"
)

// CloneDone reports whether a worker log tail shows the clone (base copy)
// phase of a clone --follow run as finished. Plain substring matching works on
// both raw and runtime-timestamped JSON log lines; a marker truncated mid-line
// does not match, so a clipped tail degrades to "not done" and the next poll
// retries.
func CloneDone(raw []byte) bool {
	return bytes.Contains(raw, []byte(markerSentinelApply)) ||
		bytes.Contains(raw, []byte(markerAllStepsDone))
}

// Supervisor-death markers, both proven live on pgcopydb 0.18 in follow mode
// (see docs/research/upstream-issues.md): the supervisor reports the dead
// clone worker, then pid 1 logs the FATAL group termination. Plain substring
// matches keep the detection tolerant of upstream format changes around the
// message.
const (
	markerGroupTermination = "Terminating all processes in our process group"
	markerCloneProcess     = "clone process"
	markerHasTerminated    = "has terminated"
)

// supervisorDeathLine reports whether one log line carries a marker.
func supervisorDeathLine(line string) bool {
	if strings.Contains(line, markerGroupTermination) {
		return true
	}
	return strings.Contains(line, markerCloneProcess) && strings.Contains(line, markerHasTerminated)
}

// SupervisorDeath scans a runtime-timestamped log tail (PodLogOptions
// Timestamps: every line is "<RFC3339Nano> <message>") for the pgcopydb
// supervisor-death markers and returns the runtime's timestamp of the first
// marker line found. The kubelet's stamp dates the death without parsing
// pgcopydb's own log fields, whose format is upstream's business. Marker
// lines without a parsable timestamp are skipped, so mixed or truncated
// input degrades to "not found".
func SupervisorDeath(raw []byte) (time.Time, bool) {
	for line := range strings.Lines(string(raw)) {
		if !supervisorDeathLine(line) {
			continue
		}
		ts, _, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			continue
		}
		return t, true
	}
	return time.Time{}, false
}
