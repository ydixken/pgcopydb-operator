package native

import (
	"encoding/json"
	"strings"
	"testing"
)

const cloneStartupFixture = `{"level":"info","error_severity":"INFO","event":"follow.startup","message":"native follow startup","limits":{"channel_bytes":8388608,"channel_messages":1024,"max_message_bytes":16777216,"group_rows":10000,"group_bytes":8388608},"apply_group_txns":100,"wal_storage_free_bytes":"unknown"}` + "\n"
const cloneBackpressureFixture = `{"level":"info","error_severity":"INFO","event":"follow.backpressure","message":"native follow backpressure","queue":{"messages":2,"owned_bytes":300,"limit_messages":1024,"limit_bytes":8388608}}` + "\n"
const cloneCopyFixture = `"copy":{"tables":{"total":2,"done":2},"indexes":{"total":1,"done":1},"bytes":{"total":32,"done":32}}`

const (
	cloneLevelFixture         = `"level":"info"`
	cloneApplyGroupFixture    = `"apply_group_txns":100`
	cloneGroupRowsFixture     = `"group_rows":10000`
	cloneQueueMessagesFixture = `"messages":2`
)

func cloneReportFixture(phase string) string {
	return `native-report: {"protocol_version":1,"engine_version":"0.1.0","command":"clone","capabilities":["clone.single_db"],"work":` + fixtureWork + `,"phase":"` + phase + `",` + cloneCopyFixture + "}\n"
}

func TestNativeCloneOutput(t *testing.T) {
	terminal, streaming := cloneReportFixture("completed"), cloneReportFixture("streaming")
	work := binding(nativeCloneCommand).Work
	t.Run("supported sequences", func(t *testing.T) {
		narrowStartup := strings.NewReplacer(`"channel_bytes":8388608`, `"channel_bytes":131072`, `"channel_messages":1024`, `"channel_messages":16`, `"max_message_bytes":16777216`, `"max_message_bytes":1048576`, cloneGroupRowsFixture, `"group_rows":1000`, `"group_bytes":8388608`, `"group_bytes":4096`).Replace(cloneStartupFixture)
		narrowQueue := strings.NewReplacer(`"limit_messages":1024`, `"limit_messages":16`, `"limit_bytes":8388608`, `"limit_bytes":131072`).Replace(cloneBackpressureFixture)
		for _, tc := range []struct {
			name, raw string
			follow    bool
		}{
			{"plain terminal", terminal, false},
			{"completed follow resume", terminal, true},
			{"early follow completion", cloneStartupFixture + terminal, true},
			{"early full queue completion", cloneStartupFixture + cloneBackpressureFixture + terminal, true},
			{"follow streaming", cloneStartupFixture + streaming + terminal, true},
			{"queue before streaming", cloneStartupFixture + cloneBackpressureFixture + streaming + terminal, true},
			{"queue after streaming", cloneStartupFixture + streaming + cloneBackpressureFixture + terminal, true},
			{"narrow startup limits", narrowStartup + narrowQueue + streaming + terminal, true},
			{"one oversized queued message", cloneStartupFixture + strings.Replace(strings.Replace(cloneBackpressureFixture, cloneQueueMessagesFixture, `"messages":1`, 1), `"owned_bytes":300`, `"owned_bytes":16777216`, 1) + terminal, true},
			{"maximum apply group", strings.Replace(cloneStartupFixture, cloneApplyGroupFixture, `"apply_group_txns":18446744073709551615`, 1) + terminal, true},
			{"measured empty clone", strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(terminal, ":32", ":0"), ":2", ":0"), `"total":1,"done":1`, `"total":0,"done":0`), false},
			{"unknown streaming measurements", cloneStartupFixture + strings.Replace(streaming, cloneCopyFixture, `"copy":{"tables":{},"indexes":{},"bytes":{}}`, 1) + terminal, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := ParseCloneOutput([]byte(tc.raw), work, tc.follow)
				if err != nil || got == nil || got.Work == nil || *got.Work != work || got.Replication != nil || got.Cleanup != nil || ValidateCompletedClone(got, work) != nil {
					t.Fatalf("completed clone data unavailable: report=%+v error=%v", got, err)
				}
			})
		}
	})
	t.Run("stream framing and identity refusal", func(t *testing.T) {
		for _, tc := range []struct {
			name, raw string
			follow    bool
		}{
			{"absent", "", true},
			{"startup alone", cloneStartupFixture, true},
			{"streaming alone", streaming, true},
			{"plain startup", cloneStartupFixture + terminal, false},
			{"plain streaming", streaming + terminal, false},
			{"streaming without startup", streaming + terminal, true},
			{"backpressure without startup", cloneBackpressureFixture + terminal, true},
			{"duplicate startup", cloneStartupFixture + cloneStartupFixture + terminal, true},
			{"duplicate backpressure", cloneStartupFixture + cloneBackpressureFixture + cloneBackpressureFixture + terminal, true},
			{"duplicate streaming", cloneStartupFixture + streaming + streaming + terminal, true},
			{"duplicate terminal", terminal + terminal, true},
			{"late streaming", terminal + streaming, true},
			{"late diagnostic", terminal + cloneStartupFixture, true},
			{"reordered diagnostic", cloneBackpressureFixture + cloneStartupFixture + terminal, true},
			{"unknown output before", "private-canary\n" + terminal, true},
			{"unknown output after", terminal + "private-canary\n", true},
			{"unknown diagnostic", "{\"level\":\"warning\",\"message\":\"private-canary\"}\n" + terminal, true},
			{"blank record", "\n" + terminal, true},
			{"blank suffix", terminal + "\n", true},
			{"unterminated terminal", strings.TrimSuffix(terminal, "\n"), true},
			{"unterminated suffix", terminal + "{}", true},
			{"clone CRLF", strings.ReplaceAll(terminal, "\n", "\r\n"), true},
			{"invalid UTF8", cloneStartupFixture + string([]byte{0xff}) + "\n" + terminal, true},
			{"truncated intermediate JSON", strings.TrimSuffix(cloneStartupFixture, "}\n") + "\n" + terminal, true},
			{"report prefix missing space", strings.Replace(terminal, "native-report: ", "native-report:", 1), true},
			{"foreign terminal work", strings.Replace(terminal, fixtureRunID, strings.Repeat("f", 32), 1), true},
			{"foreign valid terminal work", strings.Replace(terminal, fixtureWorkdirID, "foreign-work", 1), true},
			{"foreign streaming work", cloneStartupFixture + strings.Replace(streaming, fixtureWorkdirID, "foreign-work", 1) + terminal, true},
			{"foreign streaming command", cloneStartupFixture + strings.Replace(streaming, `"command":"clone"`, `"command":"list progress"`, 1) + terminal, true},
			{"nonterminal last report", cloneStartupFixture + streaming, true},
			{"failed terminal phase", strings.Replace(terminal, `"phase":"completed"`, `"phase":"failed"`, 1), true},
			{"replication field", strings.Replace(terminal, `"copy":`, `"replication":null,"copy":`, 1), false},
			{"cleanup field", strings.Replace(terminal, `"copy":`, `"cleanup":null,"copy":`, 1), false},
		} {
			t.Run(tc.name, func(t *testing.T) { refuseCloneOutput(t, []byte(tc.raw), work, tc.follow) })
		}
		refuseCloneOutput(t, []byte(terminal), WorkIdentity{}, false)
	})
	t.Run("strict fields and numeric profiles", func(t *testing.T) {
		for _, tc := range []struct{ name, old, next string }{
			{"startup extra key", cloneLevelFixture, `"extra":true,"level":"info"`},
			{"startup duplicate", cloneLevelFixture, `"level":"info","level":"info"`},
			{"startup case alias", cloneLevelFixture, `"Level":"info"`},
			{"startup missing key", `"wal_storage_free_bytes":"unknown"`, `"ignored":"unknown"`},
			{"startup wrong severity", `"error_severity":"INFO"`, `"error_severity":"WARNING"`},
			{"startup wrong message", `"message":"native follow startup"`, `"message":"private-canary"`},
			{"startup wrong event", `"event":"follow.startup"`, `"event":"follow.other"`},
			{"storage estimate", `"wal_storage_free_bytes":"unknown"`, `"wal_storage_free_bytes":42`},
			{"apply zero", cloneApplyGroupFixture, `"apply_group_txns":0`},
			{"apply missing", `"apply_group_txns":100,`, ""},
			{"apply null", cloneApplyGroupFixture, `"apply_group_txns":null`},
			{"apply float", cloneApplyGroupFixture, `"apply_group_txns":1.0`},
			{"apply exponent", cloneApplyGroupFixture, `"apply_group_txns":1e2`},
			{"apply overflow", cloneApplyGroupFixture, `"apply_group_txns":18446744073709551616`},
			{"limit zero", cloneGroupRowsFixture, `"group_rows":0`},
			{"limit negative", cloneGroupRowsFixture, `"group_rows":-1`},
			{"limit duplicate", cloneGroupRowsFixture, `"group_rows":10000,"group_rows":10000`},
			{"limit null", cloneGroupRowsFixture, `"group_rows":null`},
			{"limit missing field", `,"group_bytes":8388608`, ""},
			{"limit float", cloneGroupRowsFixture, `"group_rows":10000.5`},
			{"channel bytes maximum", `"channel_bytes":8388608`, `"channel_bytes":8388609`},
			{"channel messages maximum", `"channel_messages":1024`, `"channel_messages":1025`},
			{"message bytes maximum", `"max_message_bytes":16777216`, `"max_message_bytes":16777217`},
			{"group rows maximum", cloneGroupRowsFixture, `"group_rows":10001`},
			{"group bytes maximum", `"group_bytes":8388608`, `"group_bytes":8388609`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				raw := strings.Replace(cloneStartupFixture, tc.old, tc.next, 1)
				if raw == cloneStartupFixture {
					t.Fatal("missing startup mutation target")
				}
				refuseCloneOutput(t, []byte(raw+terminal), work, true)
			})
		}
		for _, tc := range []struct{ name, old, next string }{
			{"queue limit messages mismatch", `"limit_messages":1024`, `"limit_messages":1023`},
			{"queue limit bytes mismatch", `"limit_bytes":8388608`, `"limit_bytes":8388607`},
			{"queue zero messages", cloneQueueMessagesFixture, `"messages":0`},
			{"queue too many messages", cloneQueueMessagesFixture, `"messages":1025`},
			{"queue negative messages", cloneQueueMessagesFixture, `"messages":-1`},
			{"queue null messages", cloneQueueMessagesFixture, `"messages":null`},
			{"queue fractional messages", cloneQueueMessagesFixture, `"messages":2.5`},
			{"queue zero bytes", `"owned_bytes":300`, `"owned_bytes":0`},
			{"queue excess multi-message bytes", `"owned_bytes":300`, `"owned_bytes":8388609`},
			{"queue missing field", `"messages":2,`, ""},
			{"queue duplicate field", cloneQueueMessagesFixture, `"messages":2,"messages":2`},
			{"queue extra field", cloneQueueMessagesFixture, `"messages":2,"extra":1`},
			{"queue wrong event", `"event":"follow.backpressure"`, `"event":"follow.startup"`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				raw := strings.Replace(cloneBackpressureFixture, tc.old, tc.next, 1)
				if raw == cloneBackpressureFixture {
					t.Fatal("missing backpressure mutation target")
				}
				refuseCloneOutput(t, []byte(cloneStartupFixture+raw+terminal), work, true)
			})
		}
		for _, value := range []string{"-1", "2.5", "1e0", "9223372036854775808", "NaN", "Infinity", `"32"`, readerCaseNull} {
			t.Run("terminal counter "+value, func(t *testing.T) {
				refuseCloneOutput(t, []byte(strings.Replace(terminal, `"done":32`, `"done":`+value, 1)), work, false)
			})
		}
		refuseCloneOutput(t, []byte(strings.Replace(terminal, `"done":32`, `"done":33`, 1)), work, false)
		refuseCloneOutput(t, []byte(strings.Replace(terminal, `"indexes":{"total":1,"done":1}`, `"indexes":{}`, 1)), work, false)
		refuseCloneOutput(t, []byte(strings.Replace(terminal, `"done":32`, `"done":32,"done":32`, 1)), work, false)
		refuseCloneOutput(t, []byte(cloneStartupFixture+strings.Replace(streaming, `"done":32`, `"done":-1`, 1)+terminal), work, true)
		refuseCloneOutput(t, []byte(cloneStartupFixture+strings.Replace(strings.Replace(cloneBackpressureFixture, cloneQueueMessagesFixture, `"messages":1`, 1), `"owned_bytes":300`, `"owned_bytes":16777217`, 1)+terminal), work, true)
	})
	t.Run("complete capture cap", func(t *testing.T) {
		// Padding belongs to the report JSON, rather than an ignored log prefix.
		raw := []byte(strings.Replace(terminal, "native-report: ", "native-report: "+strings.Repeat(" ", 65536-len(terminal)), 1))
		if len(raw) != 65536 {
			t.Fatal("capture fixture is not at the limit")
		}
		if got, err := ParseCloneOutput(raw, work, false); err != nil || got == nil {
			t.Fatalf("bounded report refused: %v", err)
		}
		refuseCloneOutput(t, []byte(strings.Replace(string(raw), "\n", " \n", 1)), work, false)
	})
	if _, err := ParseReportLine([]byte(cloneStartupFixture+streaming+terminal), binding(nativeCloneCommand)); err == nil {
		t.Fatal("single-record parser accepted a multi-report clone stream")
	}
}

func refuseCloneOutput(t *testing.T, raw []byte, work WorkIdentity, follow bool) {
	t.Helper()
	got, err := ParseCloneOutput(raw, work, follow)
	if err == nil || got != nil {
		t.Fatalf("unavailable clone accepted: report=%+v error=%v", got, err)
	}
	if strings.Contains(err.Error(), "private-canary") {
		t.Fatal("decoder error leaked input")
	}
}

func TestNativeCloneOutputDiagnosticJSON(t *testing.T) {
	work := binding(nativeCloneCommand).Work
	for _, fixture := range []string{cloneStartupFixture, cloneBackpressureFixture} {
		var value map[string]json.RawMessage
		if err := json.Unmarshal([]byte(fixture), &value); err != nil {
			t.Fatal(err)
		}
		for key := range value {
			t.Run(string(value["event"])+" missing "+key, func(t *testing.T) {
				copy := make(map[string]json.RawMessage, len(value))
				for k, v := range value {
					if k != key {
						copy[k] = v
					}
				}
				line, err := json.Marshal(copy)
				if err != nil {
					t.Fatal(err)
				}
				raw := string(line) + "\n" + cloneReportFixture("completed")
				if strings.Contains(fixture, "backpressure") {
					raw = cloneStartupFixture + raw
				}
				refuseCloneOutput(t, []byte(raw), work, true)
			})
		}
	}
}
