package controller

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeFailureTestRecord(severity, message string) []byte {
	raw, _ := json.Marshal(map[string]string{"level": "error", "error_severity": severity, "message": message})
	return append(raw, '\n')
}

func TestNativeWorkerTerminalFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		want string
	}{
		{"arguments", nativeFailureTestRecord("ERROR", "native:BadArgs: invalid argument"), "NativeBadArgs"},
		{"configuration", nativeFailureTestRecord("ERROR", "native:BadConfig: unsupported profile"), nativeFailureBadConfig},
		{"recovery gap", nativeFailureTestRecord("ERROR", "native:BadState: recovery gap"), "NativeBadState"},
		{"severe fatal", nativeFailureTestRecord("FATAL", "native:BadState: recovery gap"), "NativeBadState"},
		{"private diagnostic", nativeFailureTestRecord("ERROR", "native:BadConfig: postgres://user:CANARY@private/db row=CANARY\nsecret=CANARY"), nativeFailureBadConfig},
		{"empty", nil, ""},
		{"malformed", []byte("{\"error_severity\":\"ERROR\",\"message\":\"native:BadState: broken\n"), ""},
		{"invalid UTF-8", []byte("{\"error_severity\":\"ERROR\",\"message\":\"native:BadState: \xff\"}\n"), ""},
		{"embedded", nativeFailureTestRecord("ERROR", "row says native:BadState: CANARY"), ""},
		{"leading whitespace", nativeFailureTestRecord("ERROR", " native:BadState: gap"), ""},
		{"lowercase severity", nativeFailureTestRecord("error", "native:BadState: gap"), ""},
		{"non severe", nativeFailureTestRecord("INFO", "native:BadState: gap"), ""},
		{"unknown", nativeFailureTestRecord("ERROR", "native:Unknown: CANARY"), ""},
		{"source transient", nativeFailureTestRecord("ERROR", "native:Source: CANARY"), ""},
		{"target transient", nativeFailureTestRecord("ERROR", "native:Target: CANARY"), ""},
		{"pgsql", nativeFailureTestRecord("ERROR", "native:Pgsql: CANARY"), ""},
		{"pgctl", nativeFailureTestRecord("ERROR", "native:Pgctl: CANARY"), ""},
		{"reload", nativeFailureTestRecord("ERROR", "native:Reload: CANARY"), ""},
		{"internal", nativeFailureTestRecord("ERROR", "native:Internal: CANARY"), ""},
		{"fatal category", nativeFailureTestRecord("ERROR", "native:Fatal: CANARY"), ""},
		{"missing delimiter", nativeFailureTestRecord("ERROR", "native:BadState CANARY"), ""},
		{"longer category", nativeFailureTestRecord("ERROR", "native:BadStateExtra: CANARY"), ""},
		{"raw diagnostic", []byte("native:BadState: CANARY\n"), ""},
		{"partial final line", []byte(`{"error_severity":"ERROR","message":"native:BadState: gap"}`), ""},
		{"truncated after error", append(nativeFailureTestRecord("ERROR", "native:BadState: gap"), []byte("{partial")...), ""},
		{"malformed after error", append(nativeFailureTestRecord("ERROR", "native:BadState: gap"), []byte("{partial\n")...), ""},
		{"transient after terminal", append(nativeFailureTestRecord("ERROR", "native:BadState: gap"), nativeFailureTestRecord("ERROR", "native:Target: unavailable")...), ""},
		{"oversized", nativeFailureTestRecord("ERROR", "native:BadConfig: "+strings.Repeat("x", 65536)), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, terminal := nativeWorkerTerminalFailure(tc.raw)
			if terminal != (tc.want != "") || reason != tc.want || strings.Contains(reason, "CANARY") {
				t.Fatal("terminal classification returned the wrong fixed reason or exposed private diagnostics")
			}
		})
	}
}
