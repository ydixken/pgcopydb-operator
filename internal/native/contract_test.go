package native

import (
	"fmt"
	"strings"
	"testing"
)

const (
	fixtureWork        = `{"migration_uid":"00112233-4455-6677-8899-aabbccddeeff","run_id":"00112233445566778899aabbccddeeff","workdir_id":"work-1","checkpoint_generation":1}`
	fixtureMigrationID = "00112233-4455-6677-8899-aabbccddeeff"
	fixtureRunID       = "00112233445566778899aabbccddeeff"
	fixtureWorkdirID   = "work-1"
	fixtureEarlierLSN  = "0/10"
	fixtureEndLSN      = "0/20"
	fixtureWitnessLSN  = "0/30"
	fixtureLaterLSN    = "0/40"
	jsonProtocol       = `"protocol_version":1`
	jsonGeneration     = `"checkpoint_generation":1`
	jsonTotal          = `"total":3`
	jsonDone           = `"done":2`
	jsonEnd            = `"end_lsn":"0/20"`
	jsonDurableReplay  = `"durable_replay_lsn":"0/30"`
	jsonDrainReplay    = `"replay_lsn":"0/30"`
	jsonResources      = `"resources":[`
)

func fixture(command, payload string) []byte {
	return fmt.Appendf(nil, `{"protocol_version":1,"engine_version":"rpgmigrate-dev","command":%q,"capabilities":["clone.single_db","copy.large_objects","copy.binary_fallback","copy.split","cdc.pgoutput","progress.v1","cutover.v1","verify.v1","cleanup.v1"],"work":%s,"phase":"copying",%s}`, command, fixtureWork, payload)
}

func binding(command string) Binding {
	return Binding{Command: command, Work: WorkIdentity{MigrationUID: fixtureMigrationID, RunID: fixtureRunID, WorkdirID: fixtureWorkdirID, CheckpointGeneration: 1}}
}

func replace(t *testing.T, raw []byte, old, next string) []byte {
	t.Helper()
	if !strings.Contains(string(raw), old) {
		t.Fatalf("fixture lacks replacement target %q", old)
	}
	return []byte(strings.Replace(string(raw), old, next, 1))
}

func TestParseCommandReports(t *testing.T) {
	for _, tc := range []struct{ command, payload string }{
		{"clone", `"copy":{"tables":{"total":3,"done":2}}`},
		{"list tables", `"copy":{}`},
		{"list progress", `"copy":{"tables":{"total":3,"done":2},"indexes":{"total":1,"done":0},"bytes":{"total":1024,"done":512}}`},
		{"stream sentinel get", `"replication":{"certified_lsn":"0/10","source_durable_head_lsn":"0/30","received_lsn":"0/20","durable_replay_lsn":"0/10"}`},
		{"stream sentinel set endpos", `"replication":{"certified_lsn":"0/0","end_lsn":"0/20","end_persisted":true}`},
		{"stream cleanup", `"cleanup":{"complete":true,"resources":[{"kind":"replication_slot","name":"native_slot","owned":true,"removed":true},{"kind":"publication","name":"provided","owned":false,"removed":false}]}`},
	} {
		t.Run(tc.command, func(t *testing.T) {
			want := binding(tc.command)
			if tc.command == commandSetEndpos {
				want.EndLSN = fixtureEndLSN
			}
			if tc.command == "stream cleanup" {
				want.Resources = []ResourceObligation{{Kind: "replication_slot", Name: "native_slot", Owned: true}, {Kind: "publication", Name: "provided"}}
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
		{"missing migration", `"migration_uid":"00112233-4455-6677-8899-aabbccddeeff",`, ""},
		{"missing run", `"run_id":"00112233445566778899aabbccddeeff",`, ""},
		{"missing directory identity", `"workdir_id":"work-1",`, ""},
		{"wrong migration", fixtureMigrationID, `migration-2`},
		{"wrong run", fixtureRunID, `run-2`},
		{"wrong workdir", fixtureWorkdirID, `work-2`},
		{"missing generation", `,"checkpoint_generation":1`, ""},
		{"stale generation", jsonGeneration, `"checkpoint_generation":0`},
		{"future generation", jsonGeneration, `"checkpoint_generation":2`},
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
		{"duplicate nested identity", `"run_id":"00112233445566778899aabbccddeeff"`, `"run_id":"00112233445566778899aabbccddeeff","run_id":"00112233445566778899aabbccddeeff"`},
		{"case-folded duplicate identity", `"run_id":"00112233445566778899aabbccddeeff"`, `"run_id":"00112233445566778899aabbccddeeff","RUN_ID":"00112233445566778899aabbccddeeff"`},
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
	return fixture("stream sentinel get", `"replication":{"certified_lsn":"0/30","source_durable_head_lsn":"0/40","received_lsn":"0/30","durable_replay_lsn":"0/30","end_lsn":"0/20","end_persisted":true,"drained":true,"checkpoint_id":"1:0:7","drain_proof":{"kind":"keepalive","work":`+fixtureWork+`,"checkpoint_id":"1:0:7","committed":true,"end_lsn":"0/20","replay_lsn":"0/30"}}`)
}

func TestParseDrainedRequiresBoundCommittedProof(t *testing.T) {
	want := drainBinding()
	got, err := ParseReport(drainFixture(), want)
	if err != nil || got == nil || got.Replication.Drained == nil || !*got.Replication.Drained {
		t.Fatalf("valid drain proof rejected: %v", err)
	}
	for _, tc := range []struct{ name, old, next string }{
		{"missing proof", `"drain_proof":`, `"foreign_proof":`},
		{"uncommitted", `"committed":true`, `"committed":false`},
		{"missing committed", `"committed":true,`, ""},
		{"missing checkpoint", `"checkpoint_id":"1:0:7",`, ""},
		{"foreign checkpoint", `"checkpoint_id":"1:0:7"`, `"checkpoint_id":"foreign"`},
		{"unpersisted end", `"end_persisted":true`, `"end_persisted":false`},
		{"false drained", `"drained":true`, `"drained":false`},
		{"zero end", jsonEnd, `"end_lsn":"0/0"`},
		{"different frozen end", jsonEnd, `"end_lsn":"0/21"`},
		{"replay below end", jsonDurableReplay, `"durable_replay_lsn":"0/10"`},
		{"invalid source LSN", `"source_durable_head_lsn":"0/40"`, `"source_durable_head_lsn":"garbage"`},
		{"invalid received LSN", `"received_lsn":"0/30"`, `"received_lsn":"100000000/0"`},
		{"invalid replay LSN", jsonDurableReplay, `"durable_replay_lsn":"0/100000000"`},
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
		{"stale proof", jsonGeneration, `"checkpoint_generation":0`},
		{"different proof end", jsonEnd, `"end_lsn":"0/21"`},
		{"different proof replay", jsonDrainReplay, `"replay_lsn":"0/20"`},
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
	raw := fixture("stream sentinel set endpos", `"replication":{"certified_lsn":"0/0","end_lsn":"0/20","end_persisted":true}`)
	want := binding("stream sentinel set endpos")
	want.EndLSN = fixtureEndLSN
	if _, err := ParseReport(raw, want); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{`"replication":{"certified_lsn":"0/0"}`, `"replication":{"certified_lsn":"0/0","end_lsn":"0/20","end_persisted":false}`, `"replication":{"certified_lsn":"0/0","end_lsn":"0/21","end_persisted":true}`, `"replication":{"certified_lsn":"0/0","end_lsn":"0/0","end_persisted":true}`} {
		if _, err := ParseReport(fixture(want.Command, payload), want); err == nil {
			t.Fatal("accepted missing, changed, or volatile endpos")
		}
	}
	get := binding("stream sentinel get")
	get.EndLSN = fixtureEndLSN
	if _, err := ParseReport(fixture(get.Command, `"replication":{"certified_lsn":"0/0"}`), get); err == nil {
		t.Fatal("accepted missing known cutover boundary")
	}
	if _, err := ParseReport(fixture(get.Command, `"replication":{"certified_lsn":"0/0","end_persisted":true}`), binding(get.Command)); err == nil {
		t.Fatal("accepted durable acknowledgement without an end LSN")
	}
}

func TestParseSafetyAcknowledgementsRequireCallerEndLSN(t *testing.T) {
	const drainedCase = "drained"
	for _, tc := range []struct {
		name, command string
		raw           []byte
	}{
		{drainedCase, "stream sentinel get", drainFixture()},
		{"endpos acknowledgement", commandSetEndpos, fixture(commandSetEndpos, `"replication":{"certified_lsn":"0/0","end_lsn":"0/20","end_persisted":true}`)},
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
					if tc.name == drainedCase {
						want = drainBinding()
					}
					want.EndLSN = bad.end
					if _, err := ParseReport(tc.raw, want); err == nil {
						t.Fatal("accepted safety acknowledgement without the caller's valid matching end LSN")
					}
				})
			}
			for _, end := range []string{"0/1", fixtureEndLSN, "FFFFFFFF/FFFFFFFF"} {
				t.Run("bound "+end, func(t *testing.T) {
					want := binding(tc.command)
					if tc.name == drainedCase {
						want = drainBinding()
					}
					want.EndLSN = end
					raw := strings.NewReplacer(fixtureLaterLSN, end, fixtureWitnessLSN, end, fixtureEndLSN, end).Replace(string(tc.raw))
					if want.Checkpoint != nil {
						want.Checkpoint.CertifiedLSN, want.Checkpoint.DurableReplayLSN, want.Checkpoint.DrainEndLSN, want.Checkpoint.DrainReplayLSN = end, end, end, end
					}
					if _, err := ParseReport([]byte(raw), want); err != nil {
						t.Fatalf("rejected valid bound acknowledgement: %v", err)
					}
				})
			}
			want := binding(tc.command)
			if tc.name == drainedCase {
				want = drainBinding()
			}
			want.EndLSN = "0/00000020"
			if _, err := ParseReport(tc.raw, want); err != nil {
				t.Fatalf("rejected numerically equal caller boundary: %v", err)
			}
		})
	}
}

func TestParseCleanupFailsClosed(t *testing.T) {
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
	r, err := ParseReport(fixture("stream sentinel get", `"replication":{"certified_lsn":"0/0"}`), binding("stream sentinel get"))
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

func TestParseCanonicalOperatorIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, migration, run string
		generation           int64
	}{
		{"uppercase migration", "00112233-4455-6677-8899-AABBCCDDEEFF", fixtureRunID, 1},
		{"compact migration", fixtureRunID, fixtureRunID, 1},
		{"malformed migration", "00112233-4455-6677-8899-aabbccddeefg", fixtureRunID, 1},
		{"foreign run", fixtureMigrationID, "ffeeddccbbaa99887766554433221100", 1},
		{"uppercase run", fixtureMigrationID, "00112233445566778899AABBCCDDEEFF", 1},
		{"future generation", fixtureMigrationID, fixtureRunID, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := binding("list progress")
			want.Work.MigrationUID, want.Work.RunID, want.Work.CheckpointGeneration = tc.migration, tc.run, tc.generation
			raw := strings.NewReplacer(fixtureMigrationID, tc.migration, fixtureRunID, tc.run, jsonGeneration, fmt.Sprintf(`"checkpoint_generation":%d`, tc.generation)).Replace(string(fixture(want.Command, `"copy":{}`)))
			if _, err := ParseReport([]byte(raw), want); err == nil {
				t.Fatal("accepted noncanonical operator identity")
			}
		})
	}
}

func TestParseReplicationRequiresCertifiedMeasurement(t *testing.T) {
	for _, payload := range []string{`"replication":{}`, `"replication":{"certified_lsn":null}`, `"replication":{"certified_lsn":"invalid"}`, `"replication":{"certified_lsn":"0/20","durable_replay_lsn":"0/10"}`} {
		if _, err := ParseReport(fixture("stream sentinel get", payload), binding("stream sentinel get")); err == nil {
			t.Fatal("accepted absent certified LSN measurement")
		}
	}
}

func TestParseCompareStaysOutsideReport(t *testing.T) {
	for _, command := range []string{"compare schema", "compare data"} {
		if _, err := ParseReport(fixture(command, `"verification":{"outcome":"match"}`), binding(command)); err == nil {
			t.Fatal("accepted compare as native report")
		}
	}
}

func TestParseVersion(t *testing.T) {
	raw := []byte(`{"protocol_version":1,"engine_version":"0.1.0","capabilities":[]}`)
	got, err := ParseVersion(raw)
	if err != nil || got == nil || got.ProtocolVersion != 1 || got.EngineVersion != "0.1.0" || got.Capabilities == nil || len(got.Capabilities) != 0 {
		t.Fatalf("identity-free discovery failed: %#v, %v", got, err)
	}
	for _, bad := range []string{
		`{}`, `[]`, `null`, string(raw) + string(raw), string(raw) + `x`,
		`{"protocol_version":1,"engine_version":"0.1.0"}`,
		`{"protocol_version":1,"engine_version":"0.1.0","capabilities":null}`,
		`{"protocol_version":1,"engine_version":"0.1.0","capabilities":["unknown"]}`,
		`{"protocol_version":1,"engine_version":"0.1.0","capabilities":["progress.v1","progress.v1"]}`,
		strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":2`, 1),
		strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":1.0`, 1),
		strings.Replace(string(raw), `"protocol_version":1`, `"protocol_version":1,"protocol_version":1`, 1),
		strings.Replace(string(raw), `"engine_version"`, `"ENGINE_VERSION"`, 1),
		strings.Replace(string(raw), `"engine_version"`, `"engine_version":"0.1.0","\u0065ngine_version"`, 1),
		strings.Replace(string(raw), `"0.1.0"`, `""`, 1),
		strings.Replace(string(raw), `"0.1.0"`, `null`, 1),
		strings.Replace(string(raw), `"0.1.0"`, string([]byte{'"', 0xff, '"'}), 1),
		strings.Replace(string(raw), `"capabilities":[]`, `"capabilities":[],"work":`+fixtureWork, 1),
		strings.Replace(string(raw), `"capabilities":[]`, `"capabilities":[],"command":"clone"`, 1),
	} {
		if _, err := ParseVersion([]byte(bad)); err == nil {
			t.Fatalf("accepted invalid discovery %q", bad)
		}
	}
	if _, err := ParseVersion([]byte(`{"protocol_version":1,"engine_version":"0.1.0","capabilities":["progress.v1"]}`)); err != nil {
		t.Fatal(err)
	}
}

func drainBinding() Binding {
	want := binding("stream sentinel get")
	want.EndLSN = fixtureEndLSN
	want.Checkpoint = &CheckpointEvidence{CheckpointID: "1:0:7", CertifiedLSN: fixtureWitnessLSN, DurableReplayLSN: fixtureWitnessLSN, DrainEndLSN: fixtureEndLSN, DrainReplayLSN: fixtureWitnessLSN, DrainKind: "keepalive", BarrierConfirmed: true}
	return want
}

func TestParseLaterBeginKeepsDurableAuthoritySeparate(t *testing.T) {
	want := drainBinding()
	want.Checkpoint.CertifiedLSN, want.Checkpoint.DurableReplayLSN, want.Checkpoint.DrainKind = fixtureEarlierLSN, fixtureEarlierLSN, "later_begin"
	raw := strings.NewReplacer(`"certified_lsn":"0/30"`, `"certified_lsn":"0/10"`, jsonDurableReplay, `"durable_replay_lsn":"0/10"`, `"kind":"keepalive"`, `"kind":"later_begin"`).Replace(string(drainFixture()))
	got, err := ParseReport([]byte(raw), want)
	if err != nil {
		t.Fatalf("later-BEGIN witness rejected: %v", err)
	}
	if *got.Replication.DurableReplayLSN != fixtureEarlierLSN || *got.Replication.CertifiedLSN != fixtureEarlierLSN || got.Replication.DrainProof.ReplayLSN != fixtureWitnessLSN {
		t.Fatal("later-BEGIN witness promoted target authority")
	}
	for _, tc := range []struct{ name, old, next string }{
		{"feedback promotion", `"durable_replay_lsn":"0/10"`, jsonDurableReplay},
		{"certified promotion", `"certified_lsn":"0/10"`, `"certified_lsn":"0/30"`},
		{"equal witness", jsonDrainReplay, `"replay_lsn":"0/20"`},
		{"old witness", jsonDrainReplay, `"replay_lsn":"0/10"`},
		{"unbound witness", jsonDrainReplay, `"replay_lsn":"0/40"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseReport(replace(t, []byte(raw), tc.old, tc.next), want); err == nil {
				t.Fatal("accepted forged later-BEGIN proof")
			}
		})
	}
	for _, witness := range []string{fixtureEarlierLSN, fixtureEndLSN} {
		want.Checkpoint.DrainReplayLSN = witness
		if _, err := ParseReport(replace(t, []byte(raw), jsonDrainReplay, `"replay_lsn":"`+witness+`"`), want); err == nil {
			t.Fatal("accepted later-BEGIN witness at or before endpos")
		}
	}
}

func TestParseDrainRequiresIndependentSameRowEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Binding)
	}{
		{"absent", func(w *Binding) { w.Checkpoint = nil }},
		{"unconfirmed", func(w *Binding) { w.Checkpoint.BarrierConfirmed = false }},
		{"stale token", func(w *Binding) { w.Checkpoint.CheckpointID = "1:0:6" }},
		{"different certified", func(w *Binding) { w.Checkpoint.CertifiedLSN = fixtureLaterLSN }},
		{"different durable", func(w *Binding) { w.Checkpoint.DurableReplayLSN = fixtureLaterLSN }},
		{"different end", func(w *Binding) { w.Checkpoint.DrainEndLSN = "0/21" }},
		{"different witness", func(w *Binding) { w.Checkpoint.DrainReplayLSN = fixtureLaterLSN }},
		{"different kind", func(w *Binding) { w.Checkpoint.DrainKind = "later_begin" }},
		{"missing certified", func(w *Binding) { w.Checkpoint.CertifiedLSN = "" }},
		{"missing durable", func(w *Binding) { w.Checkpoint.DurableReplayLSN = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := drainBinding()
			tc.change(&want)
			if _, err := ParseReport(drainFixture(), want); err == nil {
				t.Fatal("accepted unbound same-row evidence")
			}
		})
	}
	for _, token := range []string{"", "checkpoint-7", "1:0:0", "0:0:7", "2:0:7", "01:0:7", "1:00:7", "1:0:07", "1:-1:7", "1:0:+7", "1:0:7 ", "1:0:7:8", "9223372036854775808:0:7", "1:18446744073709551616:7", "1:0:18446744073709551616"} {
		t.Run("token "+token, func(t *testing.T) {
			want := drainBinding()
			want.Checkpoint.CheckpointID = token
			raw := strings.ReplaceAll(string(drainFixture()), "1:0:7", token)
			if _, err := ParseReport([]byte(raw), want); err == nil {
				t.Fatal("accepted invalid durable token")
			}
		})
	}
	want := drainBinding()
	want.Checkpoint.CheckpointID = "1:18446744073709551615:18446744073709551615"
	if _, err := ParseReport([]byte(strings.ReplaceAll(string(drainFixture()), "1:0:7", want.Checkpoint.CheckpointID)), want); err != nil {
		t.Fatalf("rejected exact token without observing prior counters: %v", err)
	}
	for _, tc := range []struct{ old, next string }{
		{`"kind":"keepalive",`, ""}, {`"kind":"keepalive"`, `"kind":"unknown"`},
		{`"certified_lsn":"0/30",`, ""}, {`"durable_replay_lsn":"0/30",`, ""},
	} {
		if _, err := ParseReport(replace(t, drainFixture(), tc.old, tc.next), drainBinding()); err == nil {
			t.Fatal("accepted absent drain measurement or kind")
		}
	}
	want = drainBinding()
	want.Checkpoint.CertifiedLSN = fixtureEndLSN
	if _, err := ParseReport(replace(t, drainFixture(), `"certified_lsn":"0/30"`, `"certified_lsn":"0/20"`), want); err == nil {
		t.Fatal("accepted keepalive witness above certified LSN")
	}
}

func cleanupBinding() Binding {
	want := binding("stream cleanup")
	want.Resources = []ResourceObligation{{Kind: "replication_slot", Name: "native_slot", Owned: true}, {Kind: "publication", Name: "native_publication", Owned: true}, {Kind: "workdir", Name: fixtureWorkdirID, Owned: true, Retain: true}, {Kind: "replication_origin", Name: "recorded_origin"}}
	return want
}

func cleanupFixture() []byte {
	return fixture("stream cleanup", `"cleanup":{"complete":true,"resources":[{"kind":"replication_slot","name":"native_slot","owned":true,"removed":true},{"kind":"publication","name":"native_publication","owned":true,"removed":true},{"kind":"workdir","name":"work-1","owned":true,"removed":false},{"kind":"replication_origin","name":"recorded_origin","owned":false,"removed":false}]}`)
}

func TestParseCleanupRequiresExactObligations(t *testing.T) {
	if _, err := ParseReport(cleanupFixture(), cleanupBinding()); err != nil {
		t.Fatalf("retained workdir and unowned origin rejected: %v", err)
	}
	for _, tc := range []struct{ name, old, next string }{
		{"empty", jsonResources, `"resources_missing":[`},
		{"missing publication", `{"kind":"publication","name":"native_publication","owned":true,"removed":true},`, ""},
		{"missing workdir", `{"kind":"workdir","name":"work-1","owned":true,"removed":false},`, ""},
		{"wrong name", `"name":"native_slot"`, `"name":"other_slot"`},
		{"foreign ownership", `"name":"native_slot","owned":true`, `"name":"native_slot","owned":false`},
		{"unremoved owned slot", `"name":"native_slot","owned":true,"removed":true`, `"name":"native_slot","owned":true,"removed":false`},
		{"removed workdir", `"name":"work-1","owned":true,"removed":false`, `"name":"work-1","owned":true,"removed":true`},
		{"removed origin", `"name":"recorded_origin","owned":false,"removed":false`, `"name":"recorded_origin","owned":false,"removed":true`},
		{"owned origin", `"name":"recorded_origin","owned":false`, `"name":"recorded_origin","owned":true`},
		{"extra resource", jsonResources, `"resources":[{"kind":"publication","name":"extra","owned":false,"removed":false},`},
		{"duplicate", jsonResources, `"resources":[{"kind":"replication_slot","name":"native_slot","owned":true,"removed":true},`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseReport(replace(t, cleanupFixture(), tc.old, tc.next), cleanupBinding()); err == nil {
				t.Fatal("accepted incomplete or unsafe cleanup")
			}
		})
	}
	if _, err := ParseReport(fixture("stream cleanup", `"cleanup":{"complete":true,"resources":[]}`), cleanupBinding()); err == nil {
		t.Fatal("empty inventory proved cleanup")
	}
	want := cleanupBinding()
	want.Resources = append(want.Resources, want.Resources[0])
	if _, err := ParseReport(cleanupFixture(), want); err == nil {
		t.Fatal("accepted duplicate caller obligations")
	}
	want = cleanupBinding()
	want.Resources[3].Owned = true
	raw := replace(t, cleanupFixture(), `"name":"recorded_origin","owned":false,"removed":false`, `"name":"recorded_origin","owned":true,"removed":true`)
	if _, err := ParseReport(raw, want); err == nil {
		t.Fatal("accepted owned replication origin")
	}
	if _, err := ParseReport(replace(t, cleanupFixture(), `"complete":true`, `"complete":false`), cleanupBinding()); err != nil {
		t.Fatalf("explicit incomplete inventory rejected: %v", err)
	}
}

func TestParseCleanupRequiresTrustedNonemptyInventory(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resources []ResourceObligation
	}{
		{"nil obligations", nil},
		{"empty obligations", []ResourceObligation{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := binding("stream cleanup")
			want.Resources = tc.resources
			if _, err := ParseReport(fixture(want.Command, `"cleanup":{"complete":true,"resources":[]}`), want); err == nil {
				t.Fatal("empty trusted inventory proved cleanup completion")
			}
			got, err := ParseReport(fixture(want.Command, `"cleanup":{"complete":false,"resources":[]}`), want)
			if err != nil || got == nil || got.Cleanup.Complete == nil || *got.Cleanup.Complete {
				t.Fatalf("empty incomplete observation became completion: %v", err)
			}
			for _, payload := range []string{
				`"cleanup":{"complete":false,"resources":[{"kind":"replication_slot","name":"invented","owned":true,"removed":true}]}`,
				`"cleanup":{"complete":false,"resources":[{"kind":"publication","name":"invented","owned":true,"removed":false}]}`,
				`"cleanup":{"complete":false,"resources":[{"kind":"publication","name":"invented","owned":false,"removed":false}]}`,
			} {
				if _, err := ParseReport(fixture(want.Command, payload), want); err == nil {
					t.Fatal("report invented resource obligations")
				}
			}
		})
	}
}

func TestParseKeepaliveWitnessBoundaries(t *testing.T) {
	for _, tc := range []struct {
		witness string
		accept  bool
	}{{fixtureEarlierLSN, false}, {fixtureEndLSN, true}, {fixtureWitnessLSN, true}, {fixtureLaterLSN, false}} {
		t.Run(tc.witness, func(t *testing.T) {
			want := drainBinding()
			want.Checkpoint.DurableReplayLSN, want.Checkpoint.DrainReplayLSN = fixtureLaterLSN, tc.witness
			raw := replace(t, drainFixture(), jsonDurableReplay, `"durable_replay_lsn":"0/40"`)
			raw = replace(t, raw, jsonDrainReplay, `"replay_lsn":"`+tc.witness+`"`)
			_, err := ParseReport(raw, want)
			if (err == nil) != tc.accept {
				t.Fatalf("witness acceptance = %v, want %v: %v", err == nil, tc.accept, err)
			}
		})
	}
}

func TestParseWorkdirIdentitySafeLabel(t *testing.T) {
	for _, workdir := range []string{"", " leading", "trailing ", "nested/path", `nested\path`, "tab\tlabel", strings.Repeat("x", 257)} {
		want := binding("list progress")
		want.Work.WorkdirID = workdir
		raw := replace(t, fixture(want.Command, `"copy":{}`), `"workdir_id":"work-1"`, fmt.Sprintf(`"workdir_id":%q`, workdir))
		if _, err := ParseReport(raw, want); err == nil {
			t.Fatal("accepted unsafe workdir identity")
		}
	}
	want := binding("list progress")
	want.Work.WorkdirID = strings.Repeat("x", 256)
	raw := replace(t, fixture(want.Command, `"copy":{}`), `"workdir_id":"work-1"`, fmt.Sprintf(`"workdir_id":%q`, want.Work.WorkdirID))
	if _, err := ParseReport(raw, want); err != nil {
		t.Fatal(err)
	}
}

func TestParseRejectsForeignCanonicalWork(t *testing.T) {
	foreign := WorkIdentity{MigrationUID: "ffeeddcc-bbaa-9988-7766-554433221100", RunID: "ffeeddccbbaa99887766554433221100", WorkdirID: fixtureWorkdirID, CheckpointGeneration: 1}
	want := binding("list progress")
	want.Work = foreign
	if _, err := ParseReport(fixture(want.Command, `"copy":{}`), want); err == nil {
		t.Fatal("accepted a different canonical caller work identity")
	}
	raw := string(drainFixture())
	start := strings.Index(raw, `"drain_proof":`)
	proof := strings.NewReplacer(fixtureMigrationID, foreign.MigrationUID, fixtureRunID, foreign.RunID).Replace(raw[start:])
	if _, err := ParseReport([]byte(raw[:start]+proof), drainBinding()); err == nil {
		t.Fatal("accepted a different canonical proof work identity")
	}
}
