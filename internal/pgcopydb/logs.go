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
