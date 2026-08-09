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
