package native

import (
	"strings"
	"testing"
)

func TestParseReportLineFraming(t *testing.T) {
	report := string(fixture(fixtureProgressCommand, `"copy":{"tables":{"total":3,"done":2}}`))
	line := "native-report: " + report + "\n"
	for _, tc := range []struct {
		name string
		raw  string
		want bool
	}{
		{"one record", line, true},
		{"unrelated lines", "ordinary log\n" + line + "another log\n", true},
		{"JSON whitespace", "native-report: \t" + report + " \t\n", true},
		{"CRLF", "native-report: " + report + "\r\n", true},
		{"blank log lines", "\n" + line + "\n", true},
		{fixtureEmptyCase, "", false},
		{"logs only", "ordinary log\n", false},
		{"unframed JSON", report + "\n", false},
		{"missing final newline", strings.TrimSuffix(line, "\n"), false},
		{"missing required space", "native-report:" + report + "\n", false},
		{"tab instead of space", "native-report:\t" + report + "\n", false},
		{"indented prefix", " " + line, false},
		{"timestamp prefix", "2026-11-08T00:00:00Z " + line, false},
		{"wrong prefix case", "Native-report: " + report + "\n", false},
		{"prefix in JSON log", `{"message":"native-report: ` + strings.ReplaceAll(report, `"`, `\"`) + `"}` + "\n", false},
		{"empty payload", "native-report: \n", false},
		{"partial JSON", "native-report: {\"protocol_version\":1\n", false},
		{"split JSON", "native-report: {\n" + report[1:] + "\n", false},
		{"duplicate records", line + line, false},
		{"second partial record", line + "native-report: {", false},
		{"second malformed record", line + "native-report: null\n", false},
		{"second malformed prefix", line + "native-report:" + report + "\n", false},
		{"multiple JSON objects", "native-report: " + report + report + "\n", false},
		{"trailing garbage", "native-report: " + report + "x\n", false},
		{"unknown field", "native-report: " + strings.Replace(report, `"phase":`, `"unknown":true,"phase":`, 1) + "\n", false},
		{"duplicate field", "native-report: " + strings.Replace(report, jsonProtocol, jsonProtocol+","+jsonProtocol, 1) + "\n", false},
		{"invalid UTF-8", "native-report: " + strings.Replace(report, "rpgmigrate-dev", string([]byte{0xff}), 1) + "\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseReportLine([]byte(tc.raw), binding(fixtureProgressCommand))
			if (err == nil) != tc.want || (got != nil) != tc.want {
				t.Fatalf("report acceptance = %t, want %t: %v", err == nil, tc.want, err)
			}
			if got != nil && (got.Command != fixtureProgressCommand || got.Work == nil || got.Work.RunID != fixtureRunID || got.Copy.Tables.Done == nil || *got.Copy.Tables.Done != 2) {
				t.Fatalf("unexpected parsed report: %#v", got)
			}
		})
	}
	for _, want := range []Binding{{}, binding("clone"), {Command: fixtureProgressCommand, Work: WorkIdentity{MigrationUID: fixtureMigrationID, RunID: fixtureRunID, WorkdirID: "replacement-pvc", CheckpointGeneration: 1}}} {
		if got, err := ParseReportLine([]byte(line), want); err == nil || got != nil {
			t.Fatal("accepted report without the expected command and work")
		}
	}
}

func TestParseVersionLineFraming(t *testing.T) {
	version := `{"protocol_version":1,"engine_version":"0.1.0","capabilities":[]}`
	for _, tc := range []struct {
		name string
		raw  string
		want bool
	}{
		{"one record", version + "\n", true},
		{"JSON whitespace", " \t" + version + " \t\n", true},
		{"CRLF", version + "\r\n", true},
		{fixtureEmptyCase, "", false},
		{"blank line", "\n", false},
		{"missing final newline", version, false},
		{"extra blank line", version + "\n\n", false},
		{"leading blank line", "\n" + version + "\n", false},
		{"leading log", "ordinary log\n" + version + "\n", false},
		{"trailing log", version + "\nordinary log\n", false},
		{"operational prefix", "native-report: " + version + "\n", false},
		{"JSON log", `{"message":` + version + `}` + "\n", false},
		{"partial JSON", "{\"protocol_version\":1\n", false},
		{"split JSON", "{\n" + version[1:] + "\n", false},
		{"multiple lines", version + "\n" + version + "\n", false},
		{"multiple documents", version + version + "\n", false},
		{"trailing garbage", version + "x\n", false},
		{"unknown identity", strings.Replace(version, `"capabilities":[]`, `"capabilities":[],"work":`+fixtureWork, 1) + "\n", false},
		{"duplicate field", strings.Replace(version, jsonProtocol, jsonProtocol+","+jsonProtocol, 1) + "\n", false},
		{"invalid UTF-8", strings.Replace(version, "0.1.0", string([]byte{0xff}), 1) + "\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseVersionLine([]byte(tc.raw))
			if (err == nil) != tc.want || (got != nil) != tc.want {
				t.Fatalf("version acceptance = %t, want %t: %v", err == nil, tc.want, err)
			}
			if got != nil && (got.ProtocolVersion != 1 || got.EngineVersion != "0.1.0" || got.Capabilities == nil || len(got.Capabilities) != 0) {
				t.Fatalf("unexpected parsed version: %#v", got)
			}
		})
	}
}

func TestParseLinesSharedFixtures(t *testing.T) {
	counts := make(map[string]int)
	for _, c := range sharedCases(t) {
		if c.Kind != fixtureTransportKind && c.Kind != fixtureVersionKind {
			continue
		}
		counts[c.Kind]++
		t.Run(c.ID, func(t *testing.T) {
			raw := readSharedFile(t, c.Payload)
			var err error
			if c.Kind == fixtureTransportKind {
				_, err = ParseReportLine(raw, c.Binding.binding())
			} else {
				_, err = ParseVersionLine(raw)
			}
			if (err == nil) != c.Accept {
				t.Fatalf("fixture acceptance = %t, want %t: %v", err == nil, c.Accept, err)
			}
		})
	}
	if counts[fixtureTransportKind] == 0 || counts[fixtureVersionKind] == 0 {
		t.Fatal("missing shared framing or discovery cases")
	}
}
