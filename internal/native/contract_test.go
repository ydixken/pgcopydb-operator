package native

import (
	"fmt"
	"strings"
	"testing"
)

const (
	fixtureWork        = `{"migration_uid":"migration-1","run_id":"run-1","workdir_id":"work-1","checkpoint_generation":7}`
	fixtureMigrationID = "migration-1"
	fixtureRunID       = "run-1"
	fixtureWorkdirID   = "work-1"
	fixtureEndLSN      = "0/20"
	jsonProtocol       = `"protocol_version":1`
	jsonGeneration     = `"checkpoint_generation":7`
	jsonTotal          = `"total":3`
	jsonDone           = `"done":2`
	jsonEnd            = `"end_lsn":"0/20"`
)

func fixture(command, payload string) []byte {
	return fmt.Appendf(nil, `{"protocol_version":1,"engine_version":"rpgmigrate-dev","command":%q,"capabilities":["clone.single_db","copy.large_objects","copy.binary_fallback","copy.split","cdc.pgoutput","progress.v1","cutover.v1","verify.v1","cleanup.v1"],"work":%s,"phase":"copying",%s}`, command, fixtureWork, payload)
}

func binding(command string) Binding {
	return Binding{Command: command, Work: WorkIdentity{MigrationUID: fixtureMigrationID, RunID: fixtureRunID, WorkdirID: fixtureWorkdirID, CheckpointGeneration: 7}}
}

func replace(t *testing.T, raw []byte, old, next string) []byte {
	t.Helper()
	if !strings.Contains(string(raw), old) {
		t.Fatalf("fixture lacks replacement target %q", old)
	}
	return []byte(strings.Replace(string(raw), old, next, 1))
}

func TestParseEightCommandReports(t *testing.T) {
	for _, tc := range []struct{ command, payload string }{
		{"clone", `"copy":{"tables":{"total":3,"done":2}}`},
		{"list tables", `"copy":{}`},
		{"list progress", `"copy":{"tables":{"total":3,"done":2},"indexes":{"total":1,"done":0},"bytes":{"total":1024,"done":512}}`},
		{"compare schema", `"verification":{"outcome":"match"}`},
		{"compare data", `"verification":{"outcome":"difference"}`},
		{"stream sentinel get", `"replication":{"source_durable_head_lsn":"0/30","received_lsn":"0/20","durable_replay_lsn":"0/10"}`},
		{"stream sentinel set endpos", `"replication":{"end_lsn":"0/20","end_persisted":true}`},
		{"stream cleanup", `"cleanup":{"complete":true,"resources":[{"kind":"replication_slot","name":"native_slot","owned":true,"removed":true},{"kind":"publication","name":"provided","owned":false,"removed":false}]}`},
	} {
		t.Run(tc.command, func(t *testing.T) {
			want := binding(tc.command)
			if tc.command == commandSetEndpos {
				want.EndLSN = fixtureEndLSN
			}
			got, err := ParseReport(fixture(tc.command, tc.payload), want)
			if err != nil {
				t.Fatal(err)
			}
			if got.Command != tc.command || got.Work.RunID != fixtureRunID || got.ProtocolVersion != 1 {
				t.Fatalf("unexpected parsed report: %#v", got)
			}
		})
	}
}

func TestParseUnknownMetricsRemainAbsent(t *testing.T) {
	got, err := ParseReport(fixture("list progress", `"copy":{"tables":{"total":3},"bytes":null}`), binding("list progress"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Copy.Tables.Total == nil || *got.Copy.Tables.Total != 3 || got.Copy.Tables.Done != nil || got.Copy.Bytes != nil || got.Copy.Indexes != nil {
		t.Fatalf("unknown progress became a measurement: %#v", got.Copy)
	}
}

func TestParseRejectsMalformedOrUnboundReports(t *testing.T) {
	raw := fixture("list progress", `"copy":{"tables":{"total":3,"done":2}}`)
	for _, tc := range []struct{ name, old, next string }{
		{"missing protocol", `"protocol_version":1,`, ""},
		{"future protocol", jsonProtocol, `"protocol_version":2`},
		{"noncanonical protocol key", jsonProtocol, `"PROTOCOL_VERSION":1`},
		{"null protocol", jsonProtocol, `"protocol_version":null`},
		{"missing engine version", `"engine_version":"rpgmigrate-dev",`, ""},
		{"empty engine version", `rpgmigrate-dev`, ""},
		{"missing command", `"command":"list progress",`, ""},
		{"wrong command", `"command":"list progress"`, `"command":"clone"`},
		{"missing capabilities", `"capabilities":[`, `"unrecognized":[`},
		{"unknown capability", `progress.v1`, `progress.v2`},
		{"duplicate capability", `"progress.v1"`, `"progress.v1","progress.v1"`},
		{"missing required capability", `"progress.v1",`, ""},
		{"missing work", `"work":`, `"foreign_work":`},
		{"missing migration", `"migration_uid":"migration-1",`, ""},
		{"missing run", `"run_id":"run-1",`, ""},
		{"missing directory identity", `"workdir_id":"work-1",`, ""},
		{"wrong migration", fixtureMigrationID, `migration-2`},
		{"wrong run", fixtureRunID, `run-2`},
		{"wrong workdir", fixtureWorkdirID, `work-2`},
		{"missing generation", `,"checkpoint_generation":7`, ""},
		{"stale generation", jsonGeneration, `"checkpoint_generation":6`},
		{"future generation", jsonGeneration, `"checkpoint_generation":8`},
		{"null generation", jsonGeneration, `"checkpoint_generation":null`},
		{"generation overflow", jsonGeneration, `"checkpoint_generation":9223372036854775808`},
		{"missing phase", `"phase":"copying",`, ""},
		{"unknown phase", `"phase":"copying"`, `"phase":"probably_done"`},
		{"missing payload", `"copy":`, `"other":`},
		{"null payload", `"copy":{"tables":{"total":3,"done":2}}`, `"copy":null`},
		{"negative total", jsonTotal, `"total":-1`},
		{"negative done", jsonDone, `"done":-1`},
		{"done exceeds total", jsonDone, `"done":4`},
		{"overflow", jsonTotal, `"total":9223372036854775808`},
		{"float", jsonTotal, `"total":3.0`},
		{"exponent", jsonTotal, `"total":3e0`},
		{"bool", jsonTotal, `"total":true`},
		{"string integer", jsonTotal, `"total":"3"`},
		{"duplicate header", jsonProtocol, `"protocol_version":1,"protocol_version":1`},
		{"duplicate nested identity", `"run_id":"run-1"`, `"run_id":"run-1","run_id":"run-1"`},
		{"case-folded duplicate identity", `"run_id":"run-1"`, `"run_id":"run-1","RUN_ID":"run-1"`},
		{"escaped duplicate key", jsonTotal, `"total":3,"\u0074otal":3`},
		{"unknown payload member", jsonDone, `"done":2,"exit_code":0`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseReport(replace(t, raw, tc.old, tc.next), binding("list progress")); err == nil {
				t.Fatal("accepted invalid report")
			}
		})
	}
	for _, bad := range [][]byte{nil, []byte(`[]`), []byte(`null`), append(append([]byte(nil), raw...), raw...), append(append([]byte(nil), raw...), 'x')} {
		if _, err := ParseReport(bad, binding("list progress")); err == nil {
			t.Fatal("accepted malformed document")
		}
	}
	if _, err := ParseReport(replace(t, raw, `rpgmigrate-dev`, string([]byte{0xff})), binding("list progress")); err == nil {
		t.Fatal("accepted invalid UTF-8")
	}
	if _, err := ParseReport([]byte(`{"nested":`+strings.Repeat(`[`, 33)+`0`+strings.Repeat(`]`, 33)+`}`), binding("list progress")); err == nil {
		t.Fatal("accepted excessive JSON nesting")
	}
}

func TestParseCountersPreserveExactRange(t *testing.T) {
	got, err := ParseReport(fixture("list progress", `"copy":{"bytes":{"total":9223372036854775807,"done":9007199254740993}}`), binding("list progress"))
	if err != nil {
		t.Fatal(err)
	}
	if *got.Copy.Bytes.Total != 9223372036854775807 || *got.Copy.Bytes.Done != 9007199254740993 {
		t.Fatal("integer precision changed")
	}
}

func drainFixture() []byte {
	return fixture("stream sentinel get", `"replication":{"source_durable_head_lsn":"0/40","received_lsn":"0/30","durable_replay_lsn":"0/30","end_lsn":"0/20","end_persisted":true,"drained":true,"checkpoint_id":"checkpoint-7","drain_proof":{"work":`+fixtureWork+`,"checkpoint_id":"checkpoint-7","committed":true,"end_lsn":"0/20","replay_lsn":"0/30"}}`)
}

func TestParseDrainedRequiresBoundCommittedProof(t *testing.T) {
	want := binding("stream sentinel get")
	want.EndLSN = fixtureEndLSN
	got, err := ParseReport(drainFixture(), want)
	if err != nil || got == nil || got.Replication.Drained == nil || !*got.Replication.Drained {
		t.Fatalf("valid drain proof rejected: %v", err)
	}
	for _, tc := range []struct{ name, old, next string }{
		{"missing proof", `"drain_proof":`, `"foreign_proof":`},
		{"uncommitted", `"committed":true`, `"committed":false`},
		{"missing committed", `"committed":true,`, ""},
		{"missing checkpoint", `"checkpoint_id":"checkpoint-7",`, ""},
		{"foreign checkpoint", `"checkpoint_id":"checkpoint-7"`, `"checkpoint_id":"foreign"`},
		{"unpersisted end", `"end_persisted":true`, `"end_persisted":false`},
		{"false drained", `"drained":true`, `"drained":false`},
		{"zero end", jsonEnd, `"end_lsn":"0/0"`},
		{"different frozen end", jsonEnd, `"end_lsn":"0/21"`},
		{"replay below end", `"durable_replay_lsn":"0/30"`, `"durable_replay_lsn":"0/10"`},
		{"invalid source LSN", `"source_durable_head_lsn":"0/40"`, `"source_durable_head_lsn":"garbage"`},
		{"invalid received LSN", `"received_lsn":"0/30"`, `"received_lsn":"100000000/0"`},
		{"invalid replay LSN", `"durable_replay_lsn":"0/30"`, `"durable_replay_lsn":"0/100000000"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseReport(replace(t, drainFixture(), tc.old, tc.next), want); err == nil {
				t.Fatal("accepted invalid drain proof")
			}
		})
	}
	for _, tc := range []struct{ name, old, next string }{
		{"cross migration", fixtureMigrationID, `other-migration`},
		{"cross run", fixtureRunID, `other-run`},
		{"cross directory", fixtureWorkdirID, `other-work`},
		{"stale proof", jsonGeneration, `"checkpoint_generation":6`},
		{"different proof end", jsonEnd, `"end_lsn":"0/21"`},
		{"different proof replay", `"replay_lsn":"0/30"`, `"replay_lsn":"0/20"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := string(drainFixture())
			start := strings.Index(raw, `"drain_proof":`)
			bad := raw[:start] + string(replace(t, []byte(raw[start:]), tc.old, tc.next))
			if _, err := ParseReport([]byte(bad), want); err == nil {
				t.Fatal("accepted foreign proof")
			}
		})
	}
}

func TestParseEndposAcknowledgementIsPersistentAndIdempotent(t *testing.T) {
	raw := fixture("stream sentinel set endpos", `"replication":{"end_lsn":"0/20","end_persisted":true}`)
	want := binding("stream sentinel set endpos")
	want.EndLSN = fixtureEndLSN
	if _, err := ParseReport(raw, want); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{`"replication":{}`, `"replication":{"end_lsn":"0/20","end_persisted":false}`, `"replication":{"end_lsn":"0/21","end_persisted":true}`, `"replication":{"end_lsn":"0/0","end_persisted":true}`} {
		if _, err := ParseReport(fixture(want.Command, payload), want); err == nil {
			t.Fatal("accepted missing, changed, or volatile endpos")
		}
	}
	get := binding("stream sentinel get")
	get.EndLSN = fixtureEndLSN
	if _, err := ParseReport(fixture(get.Command, `"replication":{}`), get); err == nil {
		t.Fatal("accepted missing known cutover boundary")
	}
	if _, err := ParseReport(fixture(get.Command, `"replication":{"end_persisted":true}`), binding(get.Command)); err == nil {
		t.Fatal("accepted durable acknowledgement without an end LSN")
	}
}

func TestParseSafetyAcknowledgementsRequireCallerEndLSN(t *testing.T) {
	for _, tc := range []struct {
		name, command string
		raw           []byte
	}{
		{"drained", "stream sentinel get", drainFixture()},
		{"endpos acknowledgement", commandSetEndpos, fixture(commandSetEndpos, `"replication":{"end_lsn":"0/20","end_persisted":true}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, bad := range []struct{ name, end string }{
				{"empty", ""},
				{"whitespace", " "},
				{"malformed", "not-an-lsn"},
				{"zero", "0/0"},
				{"high overflow", "100000000/0"},
				{"low overflow", "0/100000000"},
				{"mismatched", "0/21"},
			} {
				t.Run(bad.name, func(t *testing.T) {
					want := binding(tc.command)
					want.EndLSN = bad.end
					if _, err := ParseReport(tc.raw, want); err == nil {
						t.Fatal("accepted safety acknowledgement without the caller's valid matching end LSN")
					}
				})
			}
			for _, end := range []string{"0/1", fixtureEndLSN, "FFFFFFFF/FFFFFFFF"} {
				t.Run("bound "+end, func(t *testing.T) {
					want := binding(tc.command)
					want.EndLSN = end
					raw := strings.NewReplacer("0/40", end, "0/30", end, fixtureEndLSN, end).Replace(string(tc.raw))
					if _, err := ParseReport([]byte(raw), want); err != nil {
						t.Fatalf("rejected valid bound acknowledgement: %v", err)
					}
				})
			}
			want := binding(tc.command)
			want.EndLSN = "0/00000020"
			if _, err := ParseReport(tc.raw, want); err != nil {
				t.Fatalf("rejected numerically equal caller boundary: %v", err)
			}
		})
	}
}

func TestParseVerificationAndCleanupFailClosed(t *testing.T) {
	for _, outcome := range []string{"match", "difference", "failure"} {
		if _, err := ParseReport(fixture("compare data", `"verification":{"outcome":"`+outcome+`"}`), binding("compare data")); err != nil {
			t.Fatal(err)
		}
	}
	for _, payload := range []string{`"verification":{}`, `"verification":{"outcome":"unknown"}`, `"verification":{"outcome":true}`, `"copy":{}`} {
		if _, err := ParseReport(fixture("compare data", payload), binding("compare data")); err == nil {
			t.Fatal("accepted missing verification verdict")
		}
	}
	for _, payload := range []string{
		`"cleanup":{}`,
		`"cleanup":{"complete":true}`,
		`"cleanup":{"complete":true,"resources":[{"kind":"publication","name":"external","owned":false,"removed":true}]}`,
		`"cleanup":{"complete":true,"resources":[{"kind":"publication","name":"unknown","removed":true}]}`,
		`"cleanup":{"complete":true,"resources":[{"kind":"unknown","name":"native","owned":true,"removed":true}]}`,
		`"cleanup":{"complete":true,"resources":[{"kind":"publication","name":"native","owned":true,"removed":false}]}`,
		`"cleanup":{"complete":true,"resources":[{"kind":"publication","name":"native","owned":true,"removed":true},{"kind":"publication","name":"native","owned":true,"removed":true}]}`,
	} {
		if _, err := ParseReport(fixture("stream cleanup", payload), binding("stream cleanup")); err == nil {
			t.Fatal("accepted unsafe or unproven cleanup")
		}
	}
}

func TestParseUnknownReplicationAndCleanupStayUnknown(t *testing.T) {
	r, err := ParseReport(fixture("stream sentinel get", `"replication":{}`), binding("stream sentinel get"))
	if err != nil || r == nil || r.Replication.Drained != nil || r.Replication.DurableReplayLSN != nil {
		t.Fatalf("unknown replication became a proof: %v", err)
	}
	r, err = ParseReport(fixture("stream cleanup", `"cleanup":{"complete":false,"resources":[]}`), binding("stream cleanup"))
	if err != nil || r == nil || r.Cleanup.Complete == nil || *r.Cleanup.Complete {
		t.Fatalf("incomplete cleanup became complete: %v", err)
	}
}

func TestParseRejectsInvalidBindingWithoutLeakingInput(t *testing.T) {
	for _, want := range []Binding{{}, {Command: "list progress"}, {Command: "new command", Work: binding("clone").Work}} {
		if _, err := ParseReport(fixture("list progress", `"copy":{}`), want); err == nil {
			t.Fatal("accepted incomplete binding")
		}
	}
	raw := fixture("list progress", `"copy":{"bytes":{"total":"customer-secret"}}`)
	if _, err := ParseReport(raw, binding("list progress")); err == nil || strings.Contains(err.Error(), "customer-secret") {
		t.Fatalf("invalid or unsafe error: %v", err)
	}
}
