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
	var last string
	for line := range strings.Lines(string(raw)) {
		e, ok := parseLogLine(line)
		if ok && isErrorSeverity(e.Severity) && e.Message != "" {
			last = e.Message
		}
	}
	return last
}

// logEntry is one PGCOPYDB_LOG_JSON=on line's relevant fields.
type logEntry struct {
	Severity string `json:"error_severity"`
	Message  string `json:"message"`
}

func parseLogLine(line string) (logEntry, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "{") {
		return logEntry{}, false
	}
	var e logEntry
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		return logEntry{}, false
	}
	return e, true
}

// isErrorSeverity: pgcopydb's log levels above WARN; CRITICAL/PANIC accepted
// in case the naming ever shifts toward the PostgreSQL severities.
func isErrorSeverity(s string) bool {
	switch s {
	case "ERROR", "FATAL", "CRITICAL", "PANIC":
		return true
	}
	return false
}

// permissionWindow bounds how far from the end of the tail a permission line
// may sit and still count as the attempt's terminal cause. pg_restore without
// --exit-on-error tolerates per-object permission errors and keeps going, so
// an old tolerated line must not classify an attempt that later died of
// something else; the genuine terminal chain (error, ignored-count, process
// termination) is a handful of lines.
const permissionWindow = 40

// PermissionDeniedLine returns a log line showing a PostgreSQL permission
// error as the attempt's terminal cause, or "". The class is deliberately
// tiny and severity-gated: an error-severity entry carrying "permission
// denied" or an explicit "SQLSTATE 42501", within the last permissionWindow
// lines. JSON-wrapped lines count as severe on real severity or a quoted
// libpq "FATAL:" passthrough; a quoted "ERROR:" alone does not, because that
// is how pg_restore relays tolerated per-object errors while continuing.
// Bare 42501 is not matched (row data could contain it). A miss only means
// the caller keeps its normal retry behavior; extend the class only with
// evidence that it is always deterministic and terminal.
func PermissionDeniedLine(raw []byte) string {
	var window []string
	for line := range strings.Lines(string(raw)) {
		msg := strings.TrimSpace(line)
		if msg == "" {
			continue
		}
		window = append(window, msg)
		if len(window) > permissionWindow {
			window = window[1:]
		}
	}
	for _, msg := range window {
		severe := strings.Contains(msg, "ERROR:") || strings.Contains(msg, "FATAL:")
		if e, ok := parseLogLine(msg); ok {
			if e.Message == "" {
				continue
			}
			msg = e.Message
			severe = isErrorSeverity(e.Severity) || strings.Contains(msg, "FATAL:")
		}
		if !severe {
			continue
		}
		if strings.Contains(msg, "permission denied") || strings.Contains(msg, "SQLSTATE 42501") {
			return msg
		}
	}
	return ""
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
