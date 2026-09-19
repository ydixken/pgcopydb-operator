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

// LastErrorLine returns the last structured severe error, except that a recent
// pg_restore database error survives later generic teardown summaries.
// Raw and lower-severity pg_restore errors qualify only in the recent window.
func LastErrorLine(raw []byte) string {
	var last string
	for line := range strings.Lines(string(raw)) {
		e, ok := parseLogLine(line)
		if ok && e.Message != "" && isErrorSeverity(e.Severity) && !strings.HasPrefix(e.Message, "Command was:") {
			last = e.Message
		}
	}

	var actionable string
	for _, line := range recentLogLines(raw) {
		msg := line
		e, structured := parseLogLine(line)
		if structured {
			msg = e.Message
		}
		if pgRestoreErrorLine(msg) {
			actionable = msg
			continue
		}
		if actionable != "" && structured && msg != "" && isErrorSeverity(e.Severity) &&
			!strings.HasPrefix(msg, "Command was:") && !genericFailureSummary(msg) {
			actionable = msg
		}
	}
	if actionable != "" {
		return actionable
	}
	return last
}

func pgRestoreErrorLine(line string) bool {
	const prefix = "pg_restore: error:"
	return strings.HasPrefix(line, prefix) && strings.Contains(line[len(prefix):], "ERROR:")
}

func genericFailureSummary(line string) bool {
	return supervisorDeathLine(line) ||
		strings.HasPrefix(line, "Failed to run pg_restore: exit code ") ||
		strings.HasPrefix(line, "pg_restore: warning: errors ignored on restore:") ||
		strings.HasSuffix(line, ", see above for details")
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
// may sit and still count as the attempt's terminal cause: pg_restore without
// --exit-on-error tolerates per-object permission errors and keeps going, so an
// old tolerated line must not classify an attempt that died of something else.
const permissionWindow = 40

func recentLogLines(raw []byte) []string {
	var window []string
	for line := range strings.Lines(string(raw)) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		window = append(window, line)
		if len(window) > permissionWindow {
			window = window[1:]
		}
	}
	return window
}

// PermissionDeniedLine returns a log line showing a PostgreSQL permission error
// as the attempt's terminal cause, or "". A quoted "ERROR:" alone does not
// count as severe, because that is how pg_restore relays tolerated per-object
// errors while continuing, and bare 42501 is not matched because row data could
// carry it. A miss only costs the caller its normal retry, so extend the class
// only for errors known to be deterministic and terminal.
func PermissionDeniedLine(raw []byte) string {
	for _, msg := range recentLogLines(raw) {
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
		if strings.Contains(msg, "permission denied") || strings.Contains(msg, "SQLSTATE 42501") ||
			strings.Contains(msg, "must be owner of extension") {
			return msg
		}
	}
	return ""
}

// Clone-done markers for clone --follow, both logged by copydb_clone_database
// (cli_clone_follow.c, pgcopydb 0.18). The sentinel line is the transition the
// operator wants: base copy finished, change replay may start. The STEP 10
// post-data banner precedes it and still counts as mid-copy. Neither string
// appears elsewhere in the 0.18 source, so matching either keeps detection
// alive if upstream rewords one.
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

// SupervisorDeath returns the runtime timestamp of the first supervisor-death
// marker in a log tail read with PodLogOptions Timestamps, so every line is
// "<RFC3339Nano> <message>". The kubelet's stamp dates the death without
// parsing pgcopydb's own log fields, whose format upstream may change. A line
// without a parsable timestamp is skipped, so a truncated tail degrades to
// "not found".
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
