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
