package native

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var evidenceComposedStages = []string{"target_settings", evidenceTargetIdentity, "configure_durability", evidenceTargetFence, evidenceIdleGuard, "begin", "lock_checkpoint", "lock_marker", evidenceCatalogCheckpoint, "catalog_marker", evidenceRowRead, "same_value_update", "deferred_effects", "row_comparison", "precommit_guard", evidenceCommit, evidenceIdleGuard, "unlock", "closure_guard"}

type evidenceComposedAttempt struct {
	ConstructorDiscriminator string               `json:"constructor_discriminator"`
	FaultEvents              []evidenceFaultEvent `json:"fault_events"`
	ID                       string               `json:"id"`
	PID                      int                  `json:"pid"`
	Admitted                 bool                 `json:"admitted"`
	Observed                 bool                 `json:"observed"`
	PositionPresent          bool                 `json:"position_present"`
	SourceClosed             bool                 `json:"source_closed"`
	Stages                   []string             `json:"stages"`
	RefusalStage             string               `json:"refusal_stage"`
	Failure                  string               `json:"failure"`
	AllEOF                   bool                 `json:"all_eof"`
	Exit                     int                  `json:"exit"`
	Signal                   int                  `json:"signal"`
	StderrBytes              int                  `json:"stderr_bytes"`
	TokenPreserved           bool                 `json:"token_preserved"`
	RowPreserved             bool                 `json:"row_preserved"`
}

type evidenceComposedReceipt struct {
	Case           string                    `json:"case"`
	ServerMajor    int                       `json:"server_major"`
	Mocked         bool                      `json:"mocked_kubernetes_owner"`
	FaultConfirmed bool                      `json:"fault_confirmed"`
	Attempts       []evidenceComposedAttempt `json:"attempts"`
}

func goodComposedReceipt() evidenceComposedReceipt {
	return evidenceComposedReceipt{Case: evidenceLostCommit, ServerMajor: 16, Mocked: true, FaultConfirmed: true, Attempts: []evidenceComposedAttempt{
		{FaultEvents: goodFaultEvents(evidenceLostCommit, 201), ID: strings.Repeat("1", 32), PID: 101, Admitted: true, SourceClosed: true, Stages: append([]string{}, evidenceComposedStages[:15]...), RefusalStage: evidenceCommit, Failure: "validation", TokenPreserved: true, RowPreserved: true},
		{FaultEvents: []evidenceFaultEvent{}, ID: strings.Repeat("2", 32), PID: 102, Admitted: true, Observed: true, PositionPresent: true, SourceClosed: true, Stages: append([]string{}, evidenceComposedStages...), AllEOF: true, TokenPreserved: true, RowPreserved: true},
	}}
}

func TestNativeEvidenceComposedReceipt(t *testing.T) {
	good := goodComposedReceipt()
	raw, _ := json.Marshal(good)
	if !checkEvidenceComposedReceipt(raw, evidenceLostCommit, 16) {
		t.Fatal("complete private retry receipt refused")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*evidenceComposedReceipt)
	}{
		{"unconfirmed_fault", func(r *evidenceComposedReceipt) { r.FaultConfirmed = false }},
		{"no_complete_commit", func(r *evidenceComposedReceipt) { r.Attempts[0].FaultEvents[4].Stage = evidenceIdle }},
		{"no_authoritative_idle", func(r *evidenceComposedReceipt) { r.Attempts[0].FaultEvents[5].Stage = evidenceExit }},
		{"foreign_relay_pid", func(r *evidenceComposedReceipt) { r.Attempts[0].FaultEvents[4].PID = 999 }},
		{"early_failure", func(r *evidenceComposedReceipt) { r.Attempts[0].RefusalStage = evidenceSourceIdentity }},
		{evidenceTimeout, func(r *evidenceComposedReceipt) { r.Attempts[0].Failure = evidenceTimeout }},
		{evidenceOutputLimit, func(r *evidenceComposedReceipt) { r.Attempts[0].Failure = evidenceOutputLimit }},
		{"contradictory_stage", func(r *evidenceComposedReceipt) { r.Attempts[0].Stages = append(r.Attempts[0].Stages, evidenceCommit) }},
		{"failed_observation", func(r *evidenceComposedReceipt) { r.Attempts[0].Observed = true }},
		{"failed_position", func(r *evidenceComposedReceipt) { r.Attempts[0].PositionPresent = true }},
		{"old_exec", func(r *evidenceComposedReceipt) { r.Attempts[1].PID = r.Attempts[0].PID }},
		{"old_receipt", func(r *evidenceComposedReceipt) { r.Attempts[1].ID = r.Attempts[0].ID }},
		{"no_admission", func(r *evidenceComposedReceipt) { r.Attempts[1].Admitted = false }},
		{"no_source_session", func(r *evidenceComposedReceipt) { r.Attempts[1].SourceClosed = false }},
		{"no_catalog", func(r *evidenceComposedReceipt) { r.Attempts[1].Stages[8] = evidenceRowRead }},
		{"no_rebarrier", func(r *evidenceComposedReceipt) { r.Attempts[1].Stages[11] = evidenceRowRead }},
		{"no_closing_guard", func(r *evidenceComposedReceipt) { r.Attempts[1].Stages = r.Attempts[1].Stages[:18] }},
		{"incomplete_eof", func(r *evidenceComposedReceipt) { r.Attempts[1].AllEOF = false }},
		{"late_stderr", func(r *evidenceComposedReceipt) { r.Attempts[1].StderrBytes = 1 }},
		{"nonzero_exit", func(r *evidenceComposedReceipt) { r.Attempts[1].Exit = 1 }},
		{"changed_token", func(r *evidenceComposedReceipt) { r.Attempts[1].TokenPreserved = false }},
		{"changed_row", func(r *evidenceComposedReceipt) { r.Attempts[1].RowPreserved = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := goodComposedReceipt()
			tc.mutate(&r)
			b, _ := json.Marshal(r)
			if checkEvidenceComposedReceipt(b, evidenceLostCommit, 16) {
				t.Fatal("unproved composed observation admitted")
			}
		})
	}
	for _, tc := range []struct{ name, raw string }{
		{"missing_fault", strings.Replace(string(raw), `"fault_confirmed":true,`, "", 1)},
		{"duplicate_fault", strings.Replace(string(raw), `"fault_confirmed":true`, `"fault_confirmed":true,"fault_confirmed":true`, 1)},
		{"missing_negative_position", strings.Replace(string(raw), `"position_present":false,`, "", 1)},
		{"duplicate_attempt_id", strings.Replace(string(raw), `"pid":101`, `"id":"bad","pid":101`, 1)},
		{"failed_token", strings.Replace(string(raw), `"pid":101`, `"token":"1:0:7","pid":101`, 1)},
		{"failed_frontier", strings.Replace(string(raw), `"pid":101`, `"frontier":"0/10","pid":101`, 1)},
		{"trailing_document", string(raw) + "{}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if checkEvidenceComposedReceipt([]byte(tc.raw), evidenceLostCommit, 16) {
				t.Fatal("malformed composed receipt admitted")
			}
		})
	}
}

func TestNativeEvidenceComposedRetryAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, stage string
		count       int
	}{
		{"wrong_owner", evidenceAdmission, 0}, {evidenceWrongPVC, evidenceAdmission, 0}, {evidenceWrongAttempt, evidenceAdmission, 0}, {evidenceMissingManifest, evidenceAdmission, 0}, {evidenceOwnerBusy, evidenceAdmission, 0},
		{"fence_busy", evidenceTargetFence, 3}, {evidenceReplacedRelation, evidenceCatalogCheckpoint, 8}, {evidenceWrongSourceEndpoint, evidenceSourceIdentity, 0}, {evidenceWrongTargetEndpoint, evidenceTargetIdentity, 1}, {evidenceSecondCommitFault, evidenceCommit, 15},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := goodComposedReceipt()
			r.Case = "lost_commit/" + tc.name
			a := &r.Attempts[1]
			a.Observed = false
			a.PositionPresent = false
			a.AllEOF = false
			a.Admitted = tc.stage != evidenceAdmission
			a.SourceClosed = a.Admitted && tc.stage != evidenceSourceIdentity
			a.Stages = append([]string{}, evidenceComposedStages[:tc.count]...)
			a.RefusalStage = tc.stage
			a.Failure = "validation"
			if tc.name == evidenceSecondCommitFault {
				a.FaultEvents = goodFaultEvents(evidenceLostCommit, 202)
			}
			raw, _ := json.Marshal(r)
			if !checkEvidenceComposedReceipt(raw, r.Case, 16) {
				t.Fatal("intended retry refusal not recognized")
			}
			a.RefusalStage = evidenceSourceIdentity
			if tc.stage == evidenceSourceIdentity {
				a.RefusalStage = evidenceAdmission
			}
			raw, _ = json.Marshal(r)
			if checkEvidenceComposedReceipt(raw, r.Case, 16) {
				t.Fatal("unrelated earlier refusal satisfied retry case")
			}
		})
	}
}

func TestNativeEvidenceComposedSourceMajor(t *testing.T) {
	const probe = `use strict; use warnings; use EvidencePrototype;
our($version,$expected,$calls,$closed,$measured)=($ARGV[0],$ARGV[1],0,0,0);
$INC{'PSQLEvidencePrototype.pm'}=__FILE__;
{ package PSQLEvidencePrototype;
sub new { bless {backend=>'77'},$_[0] }
sub phase { my($s,$sql,$state,$check)=@_;$main::calls++;
    return $check->("SET\n") if $main::calls==1;
    $main::measured=index($sql,"current_setting('server_version_num')")>=0;
    my $row='123|11|1|t|77'.($main::measured?'|'.$main::version:'')."\n";
    return $check->($row);
}

sub finish { $main::closed++;1 }
}
my $caller={source=>{system_identifier=>123,database_oid=>11,timeline=>1,major=>0+$expected}};
my %trace;my $ok=eval{EvidencePrototype::_source_observation($caller,\%trace);1};
die "source check not reached\n" unless $calls==2&&$closed==1&&$measured;
print($ok?"accepted\n":"refused\n");`
	for _, tc := range []struct {
		name, version, expected string
		valid                   bool
	}{
		{"pg16", "160012", "16", true}, {"pg18", "180001", "18", true},
		{"mismatch", "180001", "16", false}, {"unsupported", "170006", "16", false},
		{"missing", "", "16", false}, {"fraction", "160000.0", "16", false},
		{"exponent", "16e4", "16", false}, {"leading_zero", "0160000", "16", false},
		{"trailing_bytes", "160000x", "16", false}, {"overflow", "18446744073709551616", "16", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "perl", "-I", "testdata/evidence", "-e", probe, tc.version, tc.expected)
			raw, err := command.CombinedOutput()
			want := "refused\n"
			if tc.valid {
				want = "accepted\n"
			}
			if err != nil || string(raw) != want {
				t.Fatalf("source major validation did not reach its intended check (case=%s, subprocess_error=%t)", tc.name, err != nil)
			}
		})
	}
}

func TestNativeEvidenceComposedRoutingArgs(t *testing.T) {
	const probe = `use strict;use warnings;use JSON::PP;use PSQLEvidencePrototype;
my($role,$value)=@ARGV;my $name=$role eq'source'?'PGCOPYDB_SOURCE_PGURI':'PGCOPYDB_TARGET_PGURI';
$ENV{PGCOPYDB_SOURCE_PGURI}='m6_source_abc';$ENV{PGCOPYDB_TARGET_PGURI}='host=127.0.0.1 port=43210 dbname=postgres sslmode=disable';$ENV{$name}=$value;
$ENV{PGDATABASE}='original_database';$ENV{PGPASSFILE}='private-passfile-canary';
my $args=eval{PSQLEvidencePrototype->can('role_database_args')?PSQLEvidencePrototype::role_database_args($role):[]};
if($@){my $error=$@;eval{PSQLEvidencePrototype->new(4096,$role)};die "guard did not precede PTY/exec\n" unless $@ eq$error&&$error eq"private_conninfo_refused_passfile_required\n";print "refused\n";exit}
print JSON::PP->new->canonical->encode({args=>$args,passfile_preserved=>$ENV{PGPASSFILE}eq'private-passfile-canary'?JSON::PP::true:JSON::PP::false,default_preserved=>$ENV{PGDATABASE}eq'original_database'?JSON::PP::true:JSON::PP::false}),"\n";`
	for _, tc := range []struct {
		name, role, value string
		valid             bool
	}{
		{"source_separate", evidenceSource, "m6_source_abc", true},
		{"target_proxy", evidenceTarget, "host=127.0.0.1 port=43210 dbname=postgres sslmode=disable", true},
		{"simple_target", evidenceTarget, "postgres", true},
		{"password_keyword", evidenceTarget, "host=127.0.0.1 password=private-password-canary", false},
		{"password_uri", evidenceSource, "postgresql://user:private-password-canary@localhost/db", false},
		{"encoded_password_uri", evidenceSource, "postgresql://localhost/db?pass%77ord=private-password-canary", false},
		{"unreviewed_endpoint", evidenceTarget, "host=other dbname=postgres", false},
		{"invalid_port", evidenceTarget, "host=127.0.0.1 port=65536 dbname=postgres sslmode=disable", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			raw, err := exec.CommandContext(ctx, "perl", "-I", "testdata/evidence", "-e", probe, tc.role, tc.value).CombinedOutput()
			if err != nil {
				t.Fatal("routing probe failed before its assertion")
			}
			if !tc.valid {
				if string(raw) != "refused\n" {
					t.Fatal("unsafe or unreviewed dbname argv admitted")
				}
				return
			}
			var got struct {
				Args     []string `json:"args"`
				Passfile bool     `json:"passfile_preserved"`
				Default  bool     `json:"default_preserved"`
			}
			if json.Unmarshal(raw, &got) != nil || !slices.Equal(got.Args, []string{"--dbname", tc.value}) || !got.Passfile || !got.Default {
				t.Fatal("role-specific dbname or inherited passfile/default changed")
			}
		})
	}
}

func composedObject(raw []byte, keys string, value any) bool {
	if len(raw) > 65536 || !utf8.Valid(raw) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		s, ok := key.(string)
		if err != nil || !ok || seen[s] {
			return false
		}
		seen[s] = true
		var value json.RawMessage
		if d.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return false
		}
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') || d.Decode(new(any)) != io.EOF {
		return false
	}
	want := strings.Fields(keys)
	if len(seen) != len(want) {
		return false
	}
	for _, key := range want {
		if !seen[key] {
			return false
		}
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(value) == nil
}

func checkEvidenceComposedReceipt(raw []byte, name string, major int) bool {
	var r evidenceComposedReceipt
	if (major != 16 && major != 18) || !composedObject(raw, "case server_major mocked_kubernetes_owner fault_confirmed attempts", &r) || r.Case != name || r.ServerMajor != major || !r.Mocked || !r.FaultConfirmed || len(r.Attempts) != 2 {
		return false
	}
	var envelope struct {
		Attempts []json.RawMessage `json:"attempts"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return false
	}
	for i, a := range r.Attempts {
		if !validEvidenceComposedAttempt(envelope.Attempts[i], a) {
			return false
		}
	}
	a, b := r.Attempts[0], r.Attempts[1]
	if !validEvidenceComposedFirstAttempt(a, b) {
		return false
	}
	parts := strings.Split(name, "/")
	mode := parts[0]
	if !validEvidenceComposedFault(a, mode) {
		return false
	}
	if len(parts) == 1 {
		return validEvidenceComposedSuccess(b)
	}
	if len(parts) != 2 || b.Observed || b.PositionPresent || b.Failure == "" {
		return false
	}
	return validEvidenceComposedRetry(b, parts[1])
}

func validEvidenceComposedAttempt(raw []byte, a evidenceComposedAttempt) bool {
	if !slices.Contains([]string{"", evidenceConnectionForm, evidencePTY, evidencePipe, evidenceFork, evidenceOther}, a.ConstructorDiscriminator) || a.Observed && a.ConstructorDiscriminator != "" {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	if _, ok := parseFaultEvents(fields["fault_events"]); !ok {
		return false
	}
	if !composedObject(raw, "id pid admitted observed position_present source_closed stages refusal_stage failure all_eof exit signal stderr_bytes token_preserved row_preserved fault_events constructor_discriminator", new(evidenceComposedAttempt)) || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(a.ID) || a.PID <= 0 || a.StderrBytes < 0 || !a.TokenPreserved || !a.RowPreserved || a.Failure == evidenceTimeout || a.Failure == evidenceOutputLimit {
		return false
	}
	return true
}

func validEvidenceComposedFirstAttempt(a, b evidenceComposedAttempt) bool {
	if a.ID == b.ID || a.PID == b.PID || !a.Admitted || !a.SourceClosed || a.Observed || a.PositionPresent || a.Failure == "" || len(a.Stages) < 15 || !slices.Equal(a.Stages[:15], evidenceComposedStages[:15]) {
		return false
	}
	return true
}

func validEvidenceComposedFault(a evidenceComposedAttempt, mode string) bool {
	if mode != evidenceSyncCancel && mode != evidenceLostCommit && mode != evidencePostCommitLoss {
		return false
	}
	if a.RefusalStage != evidenceCommit && (mode != evidencePostCommitLoss || a.RefusalStage != evidenceIdleGuard) {
		return false
	}
	completed := 15
	if a.RefusalStage == evidenceIdleGuard {
		completed = 16
	}
	if !slices.Equal(a.Stages, evidenceComposedStages[:completed]) {
		return false
	}
	if !completeFaultEvents(a.FaultEvents, mode) {
		return false
	}
	if mode == evidenceSyncCancel && a.StderrBytes == 0 {
		return false
	}
	return true
}

func validEvidenceComposedSuccess(b evidenceComposedAttempt) bool {
	return len(b.FaultEvents) == 0 && b.Admitted && b.Observed && b.PositionPresent && b.SourceClosed && slices.Equal(b.Stages, evidenceComposedStages) && b.RefusalStage == "" && b.Failure == "" && b.AllEOF && b.Exit == 0 && b.Signal == 0 && b.StderrBytes == 0
}

func validEvidenceComposedRetry(b evidenceComposedAttempt, name string) bool {
	stage := map[string]string{"wrong_owner": evidenceAdmission, evidenceWrongPVC: evidenceAdmission, evidenceWrongAttempt: evidenceAdmission, evidenceMissingManifest: evidenceAdmission, evidenceOwnerBusy: evidenceAdmission, "fence_busy": evidenceTargetFence, evidenceReplacedRelation: evidenceCatalogCheckpoint, evidenceWrongSourceEndpoint: evidenceSourceIdentity, evidenceWrongTargetEndpoint: evidenceTargetIdentity, evidenceSecondCommitFault: evidenceCommit}[name]
	if name == evidenceSecondCommitFault {
		if !completeFaultEvents(b.FaultEvents, evidenceLostCommit) {
			return false
		}
	} else if len(b.FaultEvents) != 0 {
		return false
	}
	if stage == "" || b.RefusalStage != stage {
		return false
	}
	if stage == evidenceAdmission {
		return !b.Admitted && len(b.Stages) == 0 && !b.SourceClosed
	}
	if !b.Admitted {
		return false
	}
	if stage == evidenceSourceIdentity {
		return !b.SourceClosed && len(b.Stages) == 0
	}
	if !b.SourceClosed {
		return false
	}
	i := slices.Index(evidenceComposedStages, stage)
	return i >= 0 && slices.Equal(b.Stages, evidenceComposedStages[:i])
}

type evidenceFaultEvent struct {
	Stage  string `json:"stage"`
	PID    int    `json:"pid"`
	Status int    `json:"status"`
}

type evidencePartial struct {
	ConstructorDiscriminator string               `json:"constructor_discriminator"`
	Partial                  bool                 `json:"partial"`
	Case                     string               `json:"case"`
	Status                   string               `json:"status"`
	ID                       string               `json:"id"`
	PID                      int                  `json:"pid"`
	ReaderStatus             string               `json:"reader_status"`
	ReaderStage              string               `json:"reader_stage"`
	StdoutBytes              int                  `json:"stdout_bytes"`
	StderrBytes              int                  `json:"stderr_bytes"`
	StdoutSHA                string               `json:"stdout_sha256"`
	StderrSHA                string               `json:"stderr_sha256"`
	FaultEvents              []evidenceFaultEvent `json:"fault_events"`
	Observed                 bool                 `json:"observed"`
	PositionPresent          bool                 `json:"position_present"`
	TokenPresent             bool                 `json:"token_present"`
	FrontierPresent          bool                 `json:"frontier_present"`
}

func TestNativeEvidenceComposedPartial(t *testing.T) {
	good := evidencePartial{ConstructorDiscriminator: evidenceOther, Partial: true, Case: evidenceLostCommit, Status: "fault_unconfirmed", ID: strings.Repeat("1", 32), PID: 44, ReaderStatus: evidenceReturnedRefusal, ReaderStage: "target_open", StdoutSHA: strings.Repeat("2", 64), StderrSHA: strings.Repeat("3", 64), FaultEvents: []evidenceFaultEvent{{evidenceLaunched, 33, 0}, {evidenceListening, 33, 0}, {evidenceTimeout, 33, 1}, {evidenceExit, 33, 1}}}
	raw, _ := json.Marshal(good)
	if _, ok := parseEvidencePartial(raw); !ok {
		t.Fatal("bounded partial refused")
	}
	path, ok := retainEvidencePartial(raw)
	if !ok {
		t.Fatal("validated partial was not retained")
	}
	defer func() {
		if err := os.Remove(path); err != nil {
			t.Error("partial diagnostic could not be removed")
		}
	}()
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("partial diagnostic was not private")
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := parseEvidencePartial(saved); !ok {
		t.Fatal("retained partial changed")
	}
	if checkEvidenceComposedReceipt(saved, evidenceLostCommit, 16) {
		t.Fatal("partial constructed successful evidence")
	}
	for _, tc := range []struct {
		name   string
		change func(*evidencePartial)
	}{
		{"no_partial_tag", func(p *evidencePartial) { p.Partial = false }},
		{"observation", func(p *evidencePartial) { p.Observed = true }},
		{"position", func(p *evidencePartial) { p.PositionPresent = true }},
		{evidenceToken, func(p *evidencePartial) { p.TokenPresent = true }},
		{"frontier", func(p *evidencePartial) { p.FrontierPresent = true }},
		{"canary_status", func(p *evidencePartial) { p.ReaderStatus = evidencePasswordCanary }},
		{"canary_constructor", func(p *evidencePartial) { p.ConstructorDiscriminator = evidencePasswordCanary }},
		{"canary_stage", func(p *evidencePartial) { p.FaultEvents[0].Stage = evidencePasswordCanary }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := json.Marshal(good)
			var p evidencePartial
			_ = json.Unmarshal(b, &p)
			tc.change(&p)
			b, _ = json.Marshal(p)
			if _, ok := parseEvidencePartial(b); ok {
				t.Fatal("unsafe partial admitted")
			}
		})
	}
	for _, bad := range []string{strings.Replace(string(raw), `"observed":false,`, "", 1), strings.Replace(string(raw), `"partial":true`, `"partial":true,"partial":true`, 1), strings.Replace(string(raw), `"partial":true`, `"password":"private-password-canary","partial":true`, 1)} {
		if _, ok := parseEvidencePartial([]byte(bad)); ok {
			t.Fatal("malformed or credential-bearing partial admitted")
		}
	}
}

func TestNativeEvidenceComposedFaultDiagnostics(t *testing.T) {
	const probe = `use strict;use warnings;use JSON::PP;use ComposedFaultFixture;use EvidencePrototype;use Cwd qw(abs_path);
my $mode=shift;my $ok=0;
if($mode eq'timeout'){
 my $fault=ComposedFaultFixture::_start(sub{kill('ALRM',$$);select(undef,undef,undef,1)});
 my $confirmed=ComposedFaultFixture::finish_fault($fault);
 my @stages=map{$_->{stage}}@{$fault->{events}//[]};
 $ok=!$confirmed&&join(',',@stages)eq'launched,timeout,exit'&&$fault->{events}[1]{status}==1;
}elsif($mode eq'reader_exec_failure'){
 my $caller=EvidencePrototype::strict_json(shift,4096);$caller->{oversized}='x'x5000;my %launch;
 my $r=eval{ComposedFaultFixture::reader_exec($caller,$caller->{helper},'none',\%launch)};
 $ok=!$r&&$launch{pid}>0&&$launch{id}=~/\A[0-9a-f]{32}\z/&&$launch{status}eq'exec_failed'&&$launch{stdout_bytes}==0&&$launch{stderr_bytes}>0&&$launch{stderr_sha256}=~/\A[0-9a-f]{64}\z/;
}else{
 my $caller=EvidencePrototype::strict_json(shift,4096);my $execution={%{$caller->{helper}}};$execution->{pvc_uid}='other-pvc';
 $ENV{PERL5LIB}=abs_path('testdata/evidence');my %launch;
 my $r=eval{ComposedFaultFixture::reader_exec($caller,$execution,'none',\%launch)};
 $ok=$r&&!$r->{admitted}&&!$r->{observed}&&$r->{refusal_stage}eq'admission'&&$launch{pid}>0&&$launch{id}eq$r->{id}&&$launch{attempt}{pid}==$r->{pid};
}
print($ok?"proved\n":"missing_diagnostic\n");`
	for _, mode := range []string{evidenceTimeout, "early_reader_refusal", "reader_exec_failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			raw, err := exec.CommandContext(ctx, "perl", "-I", "testdata/evidence", "-e", probe, mode, evidenceAdmissionCaller).CombinedOutput()
			if err != nil || string(raw) != "proved\n" {
				t.Fatal("fixed relay or reader-launch diagnostic was erased")
			}
		})
	}
}

func goodFaultEvents(mode string, pid int) []evidenceFaultEvent {
	labels := []string{evidenceLaunched, evidenceListening, "accepted", "backend_connected", "commit_complete", evidenceIdle, evidenceExit}
	if mode == evidenceSyncCancel {
		labels = []string{evidenceLaunched, "waiting", "sync_wait", "cancel_sent", evidenceExit}
	}
	events := make([]evidenceFaultEvent, len(labels))
	for i, label := range labels {
		events[i] = evidenceFaultEvent{label, pid, 0}
	}
	return events
}

func completeFaultEvents(events []evidenceFaultEvent, mode string) bool {
	if len(events) == 0 {
		return false
	}
	return slices.Equal(events, goodFaultEvents(mode, events[0].PID)) && events[0].PID > 0
}

func parseFaultEvents(raw []byte) ([]evidenceFaultEvent, bool) {
	var items []json.RawMessage
	if !bytes.HasPrefix(bytes.TrimSpace(raw), []byte("[")) || json.Unmarshal(raw, &items) != nil || len(items) > 8 {
		return nil, false
	}
	events := make([]evidenceFaultEvent, len(items))
	pid := 0
	for i, item := range items {
		var e evidenceFaultEvent
		if !composedObject(item, "stage pid status", &e) || e.PID <= 0 || e.PID > 2147483647 || e.Status < 0 || e.Status > 1 {
			return nil, false
		}
		if !slices.Contains([]string{evidenceLaunched, evidenceListening, "accepted", "backend_connected", "commit_complete", evidenceIdle, "waiting", "sync_wait", "cancel_sent", evidenceTimeout, evidenceExit}, e.Stage) {
			return nil, false
		}
		if pid != 0 && pid != e.PID {
			return nil, false
		}
		pid = e.PID
		events[i] = e
	}
	return events, true
}

func parseEvidencePartial(raw []byte) (evidencePartial, bool) {
	var p evidencePartial
	if len(raw) > 8192 || !composedObject(raw, "partial case status id pid reader_status reader_stage stdout_bytes stderr_bytes stdout_sha256 stderr_sha256 fault_events constructor_discriminator observed position_present token_present frontier_present", &p) || !p.Partial || p.Status != "fault_unconfirmed" || p.Observed || p.PositionPresent || p.TokenPresent || p.FrontierPresent {
		return p, false
	}
	if !slices.Contains([]string{evidenceConnectionForm, evidencePTY, evidencePipe, evidenceFork, evidenceOther}, p.ConstructorDiscriminator) {
		return p, false
	}
	if !regexp.MustCompile(`^(sync_cancel|lost_commit|post_commit_loss)(/(wrong_owner|wrong_pvc|wrong_attempt|missing_manifest|owner_busy|fence_busy|replaced_relation|wrong_source_endpoint|wrong_target_endpoint|second_commit_fault))?$`).MatchString(p.Case) || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(p.ID) || p.PID < 0 || p.PID > 2147483647 {
		return p, false
	}
	if !slices.Contains([]string{"launch_failed", "exec_failed", evidenceReturnedRefusal, "returned_observation"}, p.ReaderStatus) || p.PID == 0 && p.ReaderStatus != "launch_failed" {
		return p, false
	}
	stages := append(append([]string{}, evidenceComposedStages...), "", evidenceAdmission, evidenceSourceIdentity, "target_open", evidenceClosed)
	if !slices.Contains(stages, p.ReaderStage) || p.ReaderStatus == evidenceReturnedRefusal && p.ReaderStage == "" || p.ReaderStatus == "returned_observation" && p.ReaderStage != evidenceClosed {
		return p, false
	}
	if p.StdoutBytes < 0 || p.StdoutBytes > 131072 || p.StderrBytes < 0 || p.StderrBytes > 131072 || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(p.StdoutSHA) || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(p.StderrSHA) {
		return p, false
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	events, ok := parseFaultEvents(fields["fault_events"])
	if !ok {
		return p, false
	}
	p.FaultEvents = events
	return p, true
}

func retainEvidencePartial(raw []byte) (string, bool) {
	p, ok := parseEvidencePartial(raw)
	if !ok {
		return "", false
	}
	encoded, err := json.Marshal(p)
	if err != nil {
		return "", false
	}
	file, err := os.CreateTemp("", "rpgmigrate-m6-partial-*.json")
	if err != nil {
		return "", false
	}
	name := file.Name()
	_, writeErr := file.Write(append(encoded, '\n'))
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		_ = os.Remove(name)
		return "", false
	}
	return name, true
}

func TestNativeEvidenceComposedPartialProducer(t *testing.T) {
	const probe = `use strict;use warnings;use ComposedFaultFixture;use EvidencePrototype;use Cwd qw(abs_path);
my $caller=EvidencePrototype::strict_json(shift,4096);my $execution={%{$caller->{helper}}};$execution->{pvc_uid}='other-pvc';$ENV{PERL5LIB}=abs_path('testdata/evidence');
my %launch;my $attempt=ComposedFaultFixture::reader_exec($caller,$execution,'none',\%launch);
my $fault=ComposedFaultFixture::_start(sub{kill('ALRM',$$);select(undef,undef,undef,1)});
ComposedFaultFixture::finish_fault($fault)and die "unexpected_fault_success\n";
ComposedFaultFixture::emit_partial('lost_commit',\%launch,$fault);
die "composed_fault_unconfirmed\n";`
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "perl", "-I", "testdata/evidence", "-e", probe, evidenceAdmissionCaller)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err == nil || stderr.String() != "composed_fault_unconfirmed\n" {
		t.Fatal("partial replaced the original failure")
	}
	p, ok := parseEvidencePartial(stdout.Bytes())
	if !ok || p.ReaderStage != evidenceAdmission || p.ReaderStatus != evidenceReturnedRefusal || p.PID <= 0 || len(p.FaultEvents) != 3 || p.FaultEvents[1].Stage != evidenceTimeout {
		t.Fatal("early refusal or timed-out relay facts were erased")
	}
	if checkEvidenceComposedReceipt(stdout.Bytes(), evidenceLostCommit, 16) {
		t.Fatal("partial producer published authority")
	}
	if bytes.Contains(stdout.Bytes(), []byte(evidencePasswordCanary)) {
		t.Fatal("diagnostic exposed canary")
	}
}

func TestNativeEvidenceComposedEnvironmentScopes(t *testing.T) {
	const probe = `use strict;use warnings;use JSON::PP;use IPC::Open3;use Symbol qw(gensym);use Cwd qw(abs_path);
open(my $file,'<','testdata/evidence/admission_probe.pl')or die "scope_source\n";local $/;my $code=<$file>;close($file);
my($first)=$code=~/\$ok=eval\{\n([ \t]+local[^\n]+)\n[ \t]+if\(\$fault/;
my($second)=$code=~/\n    \{\n([ \t]+local[^\n]+)\n        \$ENV\{PGCOPYDB_SOURCE/;
defined($first)&&defined($second)or die "scope_missing\n";
my $mode=shift;delete $ENV{PGCOPYDB_TARGET_PGURI};$ENV{PGCOPYDB_TARGET_PGURI}='postgres'if $mode eq'present';$ENV{PGCOPYDB_TARGET_PGURI}=''if $mode eq'empty';
$ENV{PGCOPYDB_SOURCE_PGURI}='m6_source_abc';$ENV{PGPASSFILE}='private-passfile-canary';$ENV{PGDATABASE}='postgres';$ENV{PERL5LIB}=abs_path('testdata/evidence');
my $child=q{use strict;use warnings;use JSON::PP;use PSQLEvidencePrototype;
my $args=eval{PSQLEvidencePrototype::role_database_args('target')};my $refused=$@?1:0;
my $source=PSQLEvidencePrototype::role_database_args('source');
print JSON::PP->new->canonical->encode({pid=>$$,present=>exists($ENV{PGCOPYDB_TARGET_PGURI})?JSON::PP::true:JSON::PP::false,empty=>exists($ENV{PGCOPYDB_TARGET_PGURI})&&$ENV{PGCOPYDB_TARGET_PGURI}eq''?JSON::PP::true:JSON::PP::false,refused=>$refused?JSON::PP::true:JSON::PP::false,proxy=>$args&&@$args==2&&$args->[1]eq'host=127.0.0.1 port=43210 dbname=postgres sslmode=disable'?JSON::PP::true:JSON::PP::false,simple=>$args&&@$args==2&&$args->[1]eq'postgres'?JSON::PP::true:JSON::PP::false,fallback=>$args&&@$args==0&&$ENV{PGDATABASE}eq'postgres'?JSON::PP::true:JSON::PP::false,source=>$source&&@$source==2&&$source->[1]eq'm6_source_abc'?JSON::PP::true:JSON::PP::false,passfile=>$ENV{PGPASSFILE}eq'private-passfile-canary'?JSON::PP::true:JSON::PP::false}),"\n";};
my $spawn=sub{my $error=gensym();my $pid=IPC::Open3::open3(my $in,my $out,$error,$^X,'-e',$child);close($in);local $/;my $raw=<$out>;my $err=<$error>;close($out);close($error);waitpid($pid,0)==$pid&&$?==0&&(!defined($err)||$err eq'')&&length($raw)<=4096 or die "scope_child\n";my $r=JSON::PP->new->decode($raw);$r->{pid}==$pid or die "scope_pid\n";return $r};
my $first_fn=eval 'sub { '.$first.' $ENV{PGCOPYDB_TARGET_PGURI}="host=127.0.0.1 port=43210 dbname=postgres sslmode=disable";return $spawn->(); }';$first_fn or die "scope_compile\n";
my $outer=eval 'sub { '.$second.' my $one=$first_fn->();my $two=$spawn->();return [$one,$two]; }';$outer or die "scope_compile\n";
my $pair=$outer->();push @$pair,$spawn->();print JSON::PP->new->canonical->encode($pair),"\n";`
	type child struct {
		PID      int  `json:"pid"`
		Present  bool `json:"present"`
		Empty    bool `json:"empty"`
		Refused  bool `json:"refused"`
		Proxy    bool `json:"proxy"`
		Simple   bool `json:"simple"`
		Fallback bool `json:"fallback"`
		Source   bool `json:"source"`
		Passfile bool `json:"passfile"`
	}
	for _, mode := range []string{evidenceAbsent, "present", "empty"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			raw, err := exec.CommandContext(ctx, "perl", "-I", "testdata/evidence", "-e", probe, mode).CombinedOutput()
			var rows []child
			if err != nil || json.Unmarshal(raw, &rows) != nil || len(rows) != 3 {
				t.Fatal("fresh environment scope probe failed")
			}
			t.Logf("mode=%s fresh_exec_pids=%d,%d,%d restored_present=%t restored_empty=%t restored_refused=%t restored_fallback=%t", mode, rows[0].PID, rows[1].PID, rows[2].PID, rows[1].Present, rows[1].Empty, rows[1].Refused, rows[1].Fallback)
			if !rows[0].Proxy || rows[0].Refused || rows[0].PID == rows[1].PID {
				t.Fatal("first proxy child or fresh second exec missing")
			}
			for _, r := range rows {
				if !r.Source || !r.Passfile {
					t.Fatal("source or passfile changed")
				}
			}
			for _, r := range rows[1:] {
				switch mode {
				case evidenceAbsent:
					if r.Present || r.Refused || !r.Fallback {
						t.Fatal("absent target became an empty fresh-exec binding")
					}
				case "present":
					if !r.Present || r.Refused || !r.Simple {
						t.Fatal("present target was not restored")
					}
				case "empty":
					if !r.Present || !r.Empty || !r.Refused {
						t.Fatal("deliberately empty target did not refuse")
					}
				}
			}
		})
	}
}

func TestNativeEvidenceComposedConstructorDiscriminator(t *testing.T) {
	const probe = `use strict;use warnings;use ComposedFaultFixture;
my $raw=shift;my $kind=ComposedFaultFixture->can('constructor_discriminator')?ComposedFaultFixture::constructor_discriminator($raw):'missing';print $kind,"\n";`
	for _, tc := range []struct{ name, raw, want string }{
		{"connection", "private_conninfo_refused_passfile_required\n", evidenceConnectionForm}, {evidencePTY, "private_pty\n", evidencePTY}, {evidencePipe, "private_pipe\n", evidencePipe}, {evidenceFork, "private_fork\n", evidenceFork}, {"unknown", evidencePasswordCanary, evidenceOther}, {"suffix", "private_pty\nprivate-password-canary", evidenceOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			raw, err := exec.CommandContext(ctx, "perl", "-I", "testdata/evidence", "-e", probe, tc.raw).CombinedOutput()
			if err != nil || string(raw) != tc.want+"\n" {
				t.Fatal("constructor discriminator missing or not exact")
			}
		})
	}
	const guard = `use strict;use warnings;
our($pty,$fork);
BEGIN{*CORE::GLOBAL::sysopen=sub(*$$;$){$main::pty++;die "unexpected_pty\n"};*CORE::GLOBAL::fork=sub(){$main::fork++;die "unexpected_fork\n"}}
use PSQLEvidencePrototype;use ComposedFaultFixture;
($pty,$fork)=(0,0);$ENV{PGCOPYDB_TARGET_PGURI}='';my $observation=eval{PSQLEvidencePrototype->new(4096,'target')};my $error=$@;
my $kind=ComposedFaultFixture->can('constructor_discriminator')?ComposedFaultFixture::constructor_discriminator($error):'missing';
print !$observation&&$kind eq'connection_form'&&$pty==0&&$fork==0?"refused_without_effects\n":"guard_missing\n";`
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "perl", "-I", "testdata/evidence", "-e", guard).CombinedOutput()
	if err != nil || string(raw) != "refused_without_effects\n" {
		t.Fatal("empty connection did not refuse before PTY/fork and observation")
	}
}
