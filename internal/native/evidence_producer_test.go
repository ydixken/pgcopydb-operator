package native

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

func evidenceProducerGood() (evidenceToolObservation, evidenceProducerWant) {
	want := evidenceProducerWant{OperationID: strings.Repeat("a", 32), CallerSHA256: strings.Repeat("b", 64), ExecutionSHA256: strings.Repeat("c", 64), Work: WorkIdentity{MigrationUID: "00112233-4455-6677-8899-aabbccddeeff", RunID: "00112233445566778899aabbccddeeff", WorkdirID: "pvc-producer", CheckpointGeneration: 1}, EndLSN: fixtureEndLSN, SourceMajor: 18, TargetMajor: 18}
	raw, _ := json.Marshal(map[string]any{
		"operation_id": want.OperationID, "caller_sha256": want.CallerSHA256, "execution_sha256": want.ExecutionSHA256,
		"helper_euid": 65532, "source_major": 18, "target_major": 18,
		"admitted": true, "observed": true, "stages": append([]string{}, evidenceComposedStages...), "stage": evidenceClosed,
		"source_closed": true, "all_eof": true, evidenceExit: 0, "signal": 0, "stderr_bytes": 0,
		"mocked_kubernetes_owner": true, "production_admission": false,
		"positions": map[string]any{evidenceToken: "1:0:1", "certified": fixtureWitnessLSN, "durable": fixtureLaterLSN, "end": fixtureEndLSN, "witness": fixtureWitnessLSN, "kind": evidenceKeepalive},
	})
	return evidenceToolObservation{stdout: append(raw, '\n'), stdoutEOF: true, stderrEOF: true}, want
}

func TestNativeEvidenceProducerObservation(t *testing.T) {
	for _, kind := range []string{evidenceKeepalive, evidenceLaterBegin} {
		t.Run(kind, func(t *testing.T) {
			o, want := evidenceProducerGood()
			if kind == evidenceLaterBegin {
				o.stdout = bytes.Replace(o.stdout, []byte(`"kind":"keepalive"`), []byte(`"kind":"later_begin"`), 1)
			}
			checkpoint, err := evidenceProducerCheckpoint(o, want)
			if err != nil || checkpoint == nil || !checkpoint.BarrierConfirmed || checkpoint.CheckpointID != "1:0:1" || checkpoint.CertifiedLSN != fixtureWitnessLSN || checkpoint.DurableReplayLSN != fixtureLaterLSN || checkpoint.DrainEndLSN != want.EndLSN || checkpoint.DrainReplayLSN != fixtureWitnessLSN || checkpoint.DrainKind != kind {
				t.Fatal("closed independent observation did not produce the bound checkpoint")
			}
		})
	}

	t.Run("probe_refuses_before_authority", evidenceProducerProbeRefusesBeforeAuthority)

	t.Run("probe_contract", func(t *testing.T) {
		raw, err := os.ReadFile("testdata/evidence/producer_probe.pl")
		if err != nil || len(raw) > 65536 {
			t.Fatal("bounded producer probe is absent")
		}
		for _, required := range []string{evidencePersistGoal, "capture_caller", evidenceObserve, "operation_id", "caller_sha256", "execution_sha256", "positions", "producer-goal.json", "EvidencePrototype::admit", "EvidencePrototype::observe", "EvidencePrototype::revalidate"} {
			if !bytes.Contains(raw, []byte(required)) {
				t.Fatalf("producer probe contract member %s is absent", required)
			}
		}
		if bytes.Contains(raw, []byte("ParseReport")) || bytes.Contains(raw, []byte("report_json")) || bytes.Contains(raw, []byte("NativeSQLFixture")) {
			t.Fatal("probe accepts report or synthetic row authority")
		}
	})
}

func evidenceProducerProbeRefusesBeforeAuthority(t *testing.T) {
	for _, mode := range []string{evidencePersistGoal, "capture_caller", evidenceObserve, "unknown"} {
		args := []string{"-I", "testdata/evidence", "testdata/evidence/producer_probe.pl", mode, "{}"}
		if mode != evidencePersistGoal {
			args = append(args, "{}")
		}
		if mode == evidenceObserve {
			args = append(args, strings.Repeat("a", 32))
		}
		args = append(args, "not-an-authority-identity")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		command := exec.CommandContext(ctx, "perl", args...)
		var out, diagnostic evidenceBoundedBuffer
		command.Stdout = &out
		command.Stderr = &diagnostic
		err := command.Run()
		cancel()
		if err != nil || diagnostic.Len() != 0 || out.Len() > 65536 {
			t.Fatal("pure probe refusal did not close cleanly")
		}
		if mode == evidenceObserve {
			var refused evidenceProducerObservation
			if !composedObject(out.Bytes(), evidenceProducerFields, &refused) || refused.Admitted || refused.Observed || len(refused.Stages) != 0 || refused.SourceClosed || len(refused.Positions) != 0 {
				t.Fatal("pure admission refusal performed authority work")
			}
		} else if out.String() != "{\"accepted\":false}\n" {
			t.Fatal("closed mode inventory accepted an invalid request")
		}
	}
}

func TestNativeEvidenceProducerUnknownHasNoCheckpoint(t *testing.T) {
	good, want := evidenceProducerGood()
	var base map[string]json.RawMessage
	if json.Unmarshal(good.stdout, &base) != nil {
		t.Fatal("invalid test receipt")
	}
	cases := map[string]evidenceToolObservation{}
	for key := range base {
		var m map[string]json.RawMessage
		_ = json.Unmarshal(good.stdout, &m)
		delete(m, key)
		raw, _ := json.Marshal(m)
		changed := good
		changed.stdout = append(raw, '\n')
		cases["missing_"+key] = changed
		changed.stdout = bytes.Replace(good.stdout, []byte(`"`+key+`":`), []byte(`"`+key+`":`+string(base[key])+`,"`+key+`":`), 1)
		cases["duplicate_"+key] = changed
	}
	mutations := map[string]struct{ old, next string }{
		"unknown_field":           {`"helper_euid":65532`, `"helper_euid":65532,"unknown":true`},
		"wrong_operation":         {want.OperationID, strings.Repeat("d", 32)},
		"wrong_caller":            {want.CallerSHA256, strings.Repeat("d", 64)},
		evidenceWrongExecution:    {want.ExecutionSHA256, strings.Repeat("d", 64)},
		"wrong_uid":               {`"helper_euid":65532`, `"helper_euid":0`},
		"wrong_source_major":      {`"source_major":18`, `"source_major":16`},
		"wrong_target_major":      {`"target_major":18`, `"target_major":16`},
		"no_source_closure":       {`"source_closed":true`, `"source_closed":false`},
		"absent_eof":              {`"all_eof":true`, `"all_eof":false`},
		"late_stderr":             {evidenceNoStderrJSON, `"stderr_bytes":1`},
		"warning_bytes":           {evidenceNoStderrJSON, `"stderr_bytes":128`},
		"nonzero_exit":            {`"exit":0`, `"exit":1`},
		"signal":                  {`"signal":0`, `"signal":9`},
		"refused_positions":       {`"observed":true`, `"observed":false`},
		"not_admitted":            {`"admitted":true`, `"admitted":false`},
		"not_closed":              {`"stage":"closed"`, `"stage":"commit"`},
		"production_claim":        {`"production_admission":false`, `"production_admission":true`},
		"unmocked_claim":          {`"mocked_kubernetes_owner":true`, `"mocked_kubernetes_owner":false`},
		evidenceWrongSavedE:       {`"end":"0/20"`, `"end":"0/21"`},
		"invalid_lsn":             {`"durable":"0/40"`, `"durable":"bad"`},
		"zero_lsn":                {`"certified":"0/30"`, `"certified":"0/0"`},
		"certified_above_durable": {`"durable":"0/40"`, `"durable":"0/10"`},
		"wrong_generation":        {evidenceProducerTokenJSON, `"token":"2:0:1"`},
		"noncanonical_token":      {evidenceProducerTokenJSON, `"token":"1:0:01"`},
		"zero_counter":            {evidenceProducerTokenJSON, `"token":"1:0:0"`},
		"low_witness":             {`"witness":"0/30"`, `"witness":"0/10"`},
		"uncertified_keepalive":   {`"witness":"0/30"`, `"witness":"0/31"`},
		"wrong_kind":              {`"kind":"keepalive"`, `"kind":"unknown"`},
		"duplicate_position":      {evidenceProducerTokenJSON, `"token":"1:0:1","token":"1:0:1"`},
		"unknown_position":        {evidenceProducerTokenJSON, `"token":"1:0:1","other":true`},
		"null_position":           {evidenceProducerTokenJSON, `"token":null`},
	}

	for _, key := range []string{evidenceToken, "certified", "durable", "end", "witness", "kind"} {
		var m map[string]json.RawMessage
		_ = json.Unmarshal(good.stdout, &m)
		var positions map[string]json.RawMessage
		_ = json.Unmarshal(m["positions"], &positions)
		delete(positions, key)
		m["positions"], _ = json.Marshal(positions)
		raw, _ := json.Marshal(m)
		changed := good
		changed.stdout = append(raw, '\n')
		cases["missing_position_"+key] = changed
	}
	for key := range base {
		var m map[string]json.RawMessage
		_ = json.Unmarshal(good.stdout, &m)
		m[key] = json.RawMessage("null")
		raw, _ := json.Marshal(m)
		changed := good
		changed.stdout = append(raw, '\n')
		cases["null_"+key] = changed
	}
	for name, mutation := range mutations {
		changed := good
		changed.stdout = bytes.Replace(good.stdout, []byte(mutation.old), []byte(mutation.next), 1)
		if bytes.Equal(changed.stdout, good.stdout) {
			t.Fatalf("mutation %s did not change the receipt", name)
		}
		cases[name] = changed
	}
	for i := range evidenceComposedStages {
		var m map[string]any
		_ = json.Unmarshal(good.stdout, &m)
		m["stages"] = append(slices.Clone(evidenceComposedStages[:i]), evidenceComposedStages[i+1:]...)
		raw, _ := json.Marshal(m)
		changed := good
		changed.stdout = append(raw, '\n')
		cases[fmt.Sprintf("omitted_stage_%d", i)] = changed
		stages := slices.Clone(evidenceComposedStages)
		stages[i], stages[(i+1)%len(stages)] = stages[(i+1)%len(stages)], stages[i]
		m["stages"] = stages
		raw, _ = json.Marshal(m)
		changed.stdout = append(raw, '\n')
		cases[fmt.Sprintf("reordered_stage_%d", i)] = changed
	}
	changed := good
	changed.stdout = good.stdout[:len(good.stdout)-1]
	cases["unterminated"] = changed
	changed.stdout = append(bytes.Clone(good.stdout), good.stdout...)
	cases["second_record"] = changed
	changed.stdout = []byte(strings.Repeat(" ", 65537) + string(good.stdout))
	cases["oversized"] = changed
	changed.stdout = good.stdout[:len(good.stdout)/2]
	cases["truncated"] = changed
	changed = good
	changed.stdoutEOF = false
	cases["outer_stdout_open"] = changed
	changed = good
	changed.stderrEOF = false
	cases["outer_stderr_open"] = changed
	changed = good
	changed.exitCode = 1
	cases["outer_exit"] = changed
	changed = good
	changed.stderr = []byte("warning")
	cases["outer_stderr"] = changed
	for name, count := range map[string]int{"missing_commit": 15, "unknown_commit": 16} {
		var m map[string]any
		_ = json.Unmarshal(good.stdout, &m)
		m["stages"] = slices.Clone(evidenceComposedStages[:count])
		m["stage"] = evidenceCommit
		raw, _ := json.Marshal(m)
		changed = good
		changed.stdout = append(raw, '\n')
		cases[name] = changed
	}
	for name, observation := range cases {
		t.Run(name, func(t *testing.T) {
			checkpoint, err := evidenceProducerCheckpoint(observation, want)
			if err == nil || checkpoint != nil {
				t.Fatal("unknown observation produced checkpoint authority")
			}
		})
	}
	t.Run("later_begin_not_strict", func(t *testing.T) {
		changed := good
		changed.stdout = bytes.ReplaceAll(changed.stdout, []byte(`"kind":"keepalive"`), []byte(`"kind":"later_begin"`))
		changed.stdout = bytes.ReplaceAll(changed.stdout, []byte(`"witness":"0/30"`), []byte(`"witness":"0/20"`))
		checkpoint, err := evidenceProducerCheckpoint(changed, want)
		if err == nil || checkpoint != nil {
			t.Fatal("non-strict later-BEGIN witness produced authority")
		}
	})
}

type evidenceProducerWant struct {
	OperationID, CallerSHA256, ExecutionSHA256 string
	Work                                       WorkIdentity
	EndLSN                                     string
	SourceMajor, TargetMajor                   int
}

type evidenceProducerObservation struct {
	OperationID         string          `json:"operation_id"`
	CallerSHA256        string          `json:"caller_sha256"`
	ExecutionSHA256     string          `json:"execution_sha256"`
	HelperEUID          int             `json:"helper_euid"`
	SourceMajor         int             `json:"source_major"`
	TargetMajor         int             `json:"target_major"`
	Admitted            bool            `json:"admitted"`
	Observed            bool            `json:"observed"`
	Stages              []string        `json:"stages"`
	Stage               string          `json:"stage"`
	SourceClosed        bool            `json:"source_closed"`
	AllEOF              bool            `json:"all_eof"`
	Exit                int             `json:"exit"`
	Signal              int             `json:"signal"`
	StderrBytes         int             `json:"stderr_bytes"`
	Mocked              bool            `json:"mocked_kubernetes_owner"`
	ProductionAdmission bool            `json:"production_admission"`
	Positions           json.RawMessage `json:"positions,omitempty"`
}

const evidenceProducerFields = "operation_id caller_sha256 execution_sha256 helper_euid source_major target_major admitted observed stages stage source_closed all_eof exit signal stderr_bytes mocked_kubernetes_owner production_admission"

var evidenceProducerHex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)
var evidenceProducerHex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

func evidenceProducerCheckpoint(observation evidenceToolObservation, want evidenceProducerWant) (*CheckpointEvidence, error) {
	refuse := fmt.Errorf("producer observation is incomplete or unbound")
	raw := observation.stdout
	if evidenceProducerChannelsIncomplete(observation) {
		return nil, refuse
	}
	if !evidenceProducerHex32.MatchString(want.OperationID) || !evidenceProducerHex64.MatchString(want.CallerSHA256) || !evidenceProducerHex64.MatchString(want.ExecutionSHA256) || !validIdentity(want.Work) || want.SourceMajor != 18 || want.TargetMajor != 18 {
		return nil, refuse
	}
	end, err := sentinel.ParseLSN(want.EndLSN)
	if err != nil || end == 0 {
		return nil, refuse
	}
	var receipt evidenceProducerObservation
	if !composedObject(raw, evidenceProducerFields+" positions", &receipt) || evidenceProducerReceiptUnbound(receipt, want) {
		return nil, refuse
	}
	var positions struct {
		Token     string `json:"token"`
		Certified string `json:"certified"`
		Durable   string `json:"durable"`
		End       string `json:"end"`
		Witness   string `json:"witness"`
		Kind      string `json:"kind"`
	}
	if !composedObject(receipt.Positions, "token certified durable end witness kind", &positions) || !validCheckpointID(positions.Token, want.Work.CheckpointGeneration) || !sameLSN(positions.End, want.EndLSN) {
		return nil, refuse
	}
	values := make([]uint64, 0, 4)
	for _, text := range []string{positions.Certified, positions.Durable, positions.End, positions.Witness} {
		value, err := sentinel.ParseLSN(text)
		if err != nil || value == 0 {
			return nil, refuse
		}
		values = append(values, value)
	}
	certified, durable, saved, witness := values[0], values[1], values[2], values[3]
	if certified > durable || (positions.Kind != evidenceKeepalive && positions.Kind != evidenceLaterBegin) || (positions.Kind == evidenceKeepalive && (witness < saved || witness > certified)) || (positions.Kind == evidenceLaterBegin && witness <= saved) {
		return nil, refuse
	}
	return &CheckpointEvidence{CheckpointID: positions.Token, CertifiedLSN: positions.Certified, DurableReplayLSN: positions.Durable, DrainEndLSN: positions.End, DrainReplayLSN: positions.Witness, DrainKind: positions.Kind, BarrierConfirmed: true}, nil
}

func evidenceProducerChannelsIncomplete(observation evidenceToolObservation) bool {
	raw := observation.stdout
	return observation.exitCode != 0 || !observation.stdoutEOF || !observation.stderrEOF || len(observation.stderr) != 0 || len(raw) == 0 || len(raw) > 65536 || raw[len(raw)-1] != '\n' || bytes.Count(raw, []byte("\n")) != 1
}

func evidenceProducerReceiptUnbound(receipt evidenceProducerObservation, want evidenceProducerWant) bool {
	return receipt.OperationID != want.OperationID || receipt.CallerSHA256 != want.CallerSHA256 || receipt.ExecutionSHA256 != want.ExecutionSHA256 || receipt.HelperEUID != 65532 || receipt.SourceMajor != want.SourceMajor || receipt.TargetMajor != want.TargetMajor || !receipt.Admitted || !receipt.Observed || !slices.Equal(receipt.Stages, evidenceComposedStages) || receipt.Stage != evidenceClosed || !receipt.SourceClosed || !receipt.AllEOF || receipt.Exit != 0 || receipt.Signal != 0 || receipt.StderrBytes != 0 || !receipt.Mocked || receipt.ProductionAdmission
}

type evidenceProducerBuffer struct {
	mu sync.Mutex
	evidenceBoundedBuffer
}

func (b *evidenceProducerBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.evidenceBoundedBuffer.Write(p)
}

func (b *evidenceProducerBuffer) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.Bytes())
}

type evidenceProducerProcess struct {
	stdout, stderr evidenceProducerBuffer
	done           chan evidenceToolObservation
	cancel         context.CancelFunc
}

func evidenceProducerStart(args ...string) *evidenceProducerProcess {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	p := &evidenceProducerProcess{done: make(chan evidenceToolObservation, 1), cancel: cancel}
	command := exec.CommandContext(ctx, "podman", args...)
	out, err := command.StdoutPipe()
	if err != nil {
		cancel()
		p.done <- evidenceToolObservation{exitCode: -1}
		return p
	}
	diagnostic, err := command.StderrPipe()
	if err != nil {
		_ = out.Close()
		cancel()
		p.done <- evidenceToolObservation{exitCode: -1}
		return p
	}
	if command.Start() != nil {
		_ = out.Close()
		_ = diagnostic.Close()
		cancel()
		p.done <- evidenceToolObservation{exitCode: -1}
		return p
	}
	go func() {
		var wg sync.WaitGroup
		var outErr, diagnosticErr error
		wg.Add(2)
		go func() { defer wg.Done(); _, outErr = io.Copy(&p.stdout, out) }()
		go func() { defer wg.Done(); _, diagnosticErr = io.Copy(&p.stderr, diagnostic) }()
		wg.Wait()
		err := command.Wait()
		code := 0
		if err != nil {
			code = -1
			if exit, ok := err.(*exec.ExitError); ok {
				code = exit.ExitCode()
			}
		}
		p.done <- evidenceToolObservation{stdout: p.stdout.snapshot(), stderr: p.stderr.snapshot(), exitCode: code, stdoutEOF: outErr == nil, stderrEOF: diagnosticErr == nil}
		cancel()
	}()
	return p
}

func evidenceProducerID(t *testing.T) string {
	t.Helper()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal("cannot allocate a private fixture identity")
	}
	return hex.EncodeToString(id[:])
}

func evidenceProducerHash(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func evidenceProducerJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 4096 {
		t.Fatal("private caller exceeds its existing transport bound")
	}
	return raw
}

type evidenceProducerIdentity struct {
	System   uint64 `json:"system_identifier"`
	Database uint32 `json:"database_oid"`
	Timeline uint32 `json:"timeline,omitempty"`
	Major    int    `json:"major"`
}

type evidenceProducerPrivateState struct {
	Root        string `json:"root"`
	Manifest    string `json:"manifest"`
	Endpos      string `json:"endpos"`
	Slot        string `json:"slot"`
	Publication string `json:"publication"`
	Origin      string `json:"origin"`
}

type evidenceProducerFixture struct {
	t                      *testing.T
	database               string
	binary                 string
	binarySHA              string
	manifestSHA            string
	libpqSHA               string
	libpq                  string
	rootID                 string
	volume                 string
	owner                  string
	directory              string
	volumeCreated          map[string]string
	containers             []string
	volumes                []string
	sourceName, targetName string
	source, target         evidenceProducerIdentity
	work                   WorkIdentity
	execution              map[string]any
	engineEnv              []string
	receipt                map[string]any
}

func TestNativeEvidenceLiveActualProducer(t *testing.T) {
	binaryPath, binarySHA := os.Getenv("NATIVE_EVIDENCE_ENGINE_BINARY"), os.Getenv("NATIVE_EVIDENCE_ENGINE_SHA256")
	if binaryPath == "" && binarySHA == "" {
		t.Skip("exact actual producer is not selected; interoperability remains unqualified")
	}
	f := evidenceProducerSelect(t, binaryPath, binarySHA)
	var rawCaller, rawExecution, rawAck []byte
	var savedE string
	var original evidenceProducerPrivateState
	var checkpoint *CheckpointEvidence
	caseResults := map[string]bool{}
	f.receipt["cases"] = caseResults
	passed := t.Run("positive_keepalive", func(t *testing.T) {
		f.t = t
		f.prepare()
		rawExecution = evidenceProducerJSON(t, f.execution)
		producer := f.create(f.volume, "/candidate/rpgmigrate", f.engineEnv,
			evidenceCloneCommand, "--follow", "--plugin", "pgoutput", "--dir", "/work/pgcopydb", "--source", f.conninfo(f.sourceName), "--target", f.conninfo(f.targetName), "--skip-large-objects", "--table-jobs", "1", "--index-jobs", "1", "--restore-jobs", "1")
		f.receipt["producer_container"] = producer
		f.persist()
		process := evidenceProducerStart("start", "-a", producer)
		t.Cleanup(process.cancel)
		deadline := time.NewTimer(90 * time.Second)
		defer deadline.Stop()
		poll := time.NewTicker(100 * time.Millisecond)
		defer poll.Stop()
		f.waitProducerStreaming(process, deadline.C, poll.C)
		startState := f.privateState(nil)
		var end uint64
		var expected []byte
		savedE, end, expected = f.publishProducerGoal(startState)
		var completed evidenceToolObservation
		select {
		case completed = <-process.done:
		case <-time.After(100 * time.Second):
			t.Fatal("actual producer did not join after goal publication")
		}
		if completed.exitCode != 0 || !completed.stdoutEOF || !completed.stderrEOF || bytes.Contains(completed.stdout, []byte("private-fixture-canary")) || bytes.Contains(completed.stderr, []byte("private-fixture-canary")) {
			t.Fatal("actual producer did not complete with closed bounded channels")
		}
		f.checkProducerClone(producer, completed, expected)
		seed := evidenceProducerJSON(t, map[string]any{evidenceWork: f.work, "helper": f.execution, evidenceSource: f.source, evidenceTarget: f.target, "end_lsn": end})
		capture := f.probe(f.volume, nil, "capture_caller", string(seed), string(rawExecution), f.rootID)
		var caller map[string]json.RawMessage
		if !composedObject(capture.stdout, "work helper source target end_lsn", &caller) || len(capture.stdout) > 4096 {
			t.Fatal("original confirmed receipts did not produce a bounded caller")
		}
		rawCaller = bytes.TrimSuffix(capture.stdout, []byte("\n"))
		original = f.privateState(rawCaller)
		if original.Root != startState.Root || original.Slot != startState.Slot || original.Publication != startState.Publication {
			t.Fatal("original native authority or source-resource continuity changed")
		}
		operation := evidenceProducerID(t)
		observation := f.probe(f.volume, f.helperEnv(f.targetName), evidenceObserve, string(rawCaller), string(rawExecution), operation, f.rootID)
		helper := f.containers[len(f.containers)-1]
		if helper == producer {
			t.Fatal("post-exit helper reused the producer execution")
		}
		f.receipt["helper_container"] = helper
		f.receipt["helper_closure"] = map[string]any{evidenceExit: observation.exitCode, "stdout_eof": observation.stdoutEOF, "stderr_eof": observation.stderrEOF, "stderr_bytes": len(observation.stderr)}
		var err error
		checkpoint, err = evidenceProducerCheckpoint(observation, f.want(operation, rawCaller, rawExecution, savedE))
		if err != nil || checkpoint == nil || checkpoint.DrainKind != evidenceKeepalive {
			t.Fatal("fresh independent helper did not prove completed keepalive checkpoint")
		}
		rawAck = f.engine("stream", "sentinel", "set", "endpos", savedE, "--json", "--dir", "/work/pgcopydb", "--target", f.conninfo(f.targetName)).stdout
		parsed, err := ParseReportLine(rawAck, Binding{Command: commandSetEndpos, Work: f.work, EndLSN: savedE, Checkpoint: checkpoint})
		if err != nil || parsed.Replication == nil || parsed.Replication.Drained == nil || !*parsed.Replication.Drained {
			t.Fatal("actual same-goal acknowledgment failed the independent Go parser binding")
		}
		if after := f.privateState(rawCaller); after != original {
			t.Fatal("helper or acknowledgment changed original canonical private authority")
		}
		f.receipt["independent_go_parse"] = true
		f.receipt["helper_operation_id"] = operation
		f.receipt["helper_observation_sha256"] = f.retain("helper-observation.json", observation.stdout, 65536)
		f.receipt["actual_ack_sha256"] = f.retain("actual-ack.json", rawAck, 65536)
		f.receipt["worker_completed"] = true
		f.persist()
	})
	f.t = t
	caseResults["positive_keepalive"] = passed
	f.persist()
	if !passed {
		return
	}
	negativeOK := true
	for _, name := range []string{"wrong_work", evidenceWrongSavedE, evidenceWrongExecution, evidenceWrongTargetEndpoint, "wrong_volume", "tampered_report"} {
		result := t.Run(name, func(t *testing.T) {
			f.t = t
			f.rejectProducerCase(name, rawCaller, rawExecution, rawAck, savedE, checkpoint, original)
		})
		f.t = t
		caseResults[name] = result
		f.persist()
		if !result {
			negativeOK = false
		}
	}
	f.t = t
	if negativeOK {
		f.close(original)
		f.receipt["qualified_case"] = true
		f.persist()
	}
}

func (f *evidenceProducerFixture) waitProducerStreaming(process *evidenceProducerProcess, deadline, poll <-chan time.Time) {
	t := f.t
	t.Helper()
	ready := false
	for !ready {
		select {
		case <-deadline:
			t.Fatal("actual producer did not expose a bounded startup frame")
		case <-process.done:
			t.Fatal("actual producer exited before independently bound startup")
		case <-poll:
			available := process.stdout.snapshot()
			last := bytes.LastIndexByte(available, '\n')
			if last < 0 {
				continue
			}
			for line := range bytes.SplitSeq(available[:last+1], []byte("\n")) {
				if !bytes.HasPrefix(line, []byte("native-report: ")) {
					continue
				}
				report, err := ParseReport(bytes.TrimPrefix(line, []byte("native-report: ")), Binding{Command: evidenceCloneCommand, Work: f.work})
				if err != nil {
					t.Fatal("actual producer startup is unbound")
				}
				if report.Phase == "streaming" {
					ready = true
				}
			}
		}
	}
}

func (f *evidenceProducerFixture) publishProducerGoal(startState evidenceProducerPrivateState) (string, uint64, []byte) {
	t := f.t
	t.Helper()
	sourcePID := f.checkSlot(startState.Slot, true)
	f.receipt["source_slot_pid"] = sourcePID
	f.receipt["source_slot_continuity"] = true
	f.sql(f.sourceName, `BEGIN; INSERT INTO producer_fixture.rows VALUES (3,'private-fixture-canary-3'); SELECT setval('producer_fixture.seq',73,true); COMMIT;`)
	expected := f.sql(f.sourceName, `SELECT count(*)::text||'|'||sum(id)::text FROM producer_fixture.rows; SELECT last_value::text||'|'||is_called::text FROM producer_fixture.seq;`)
	if string(expected) != "3|6\n73|true\n" {
		t.Fatal("source fixture mutation was not positively joined")
	}
	if string(f.sql(f.sourceName, `SELECT count(*) FROM pg_stat_activity WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND pid<>pg_backend_pid() AND state='idle in transaction';`)) != "0\n" {
		t.Fatal("source fixture retains an outstanding write transaction")
	}
	if f.checkSlot(startState.Slot, true) != sourcePID {
		t.Fatal("active original source-slot continuity is uncertain")
	}
	savedE := strings.TrimSpace(string(f.sql(f.sourceName, `SELECT pg_current_wal_insert_lsn();`)))
	end, err := sentinel.ParseLSN(savedE)
	if err != nil || end == 0 {
		t.Fatal("source did not supply one positive saved goal")
	}
	goal := evidenceProducerJSON(t, map[string]any{evidenceWork: f.work, "end_lsn": end})
	persisted := f.probe(f.volume, nil, evidencePersistGoal, string(goal), f.rootID)
	var publication struct {
		Persisted bool   `json:"persisted"`
		Digest    string `json:"goal_sha256"`
	}
	if !composedObject(persisted.stdout, "persisted goal_sha256", &publication) || !publication.Persisted || publication.Digest != evidenceProducerHash(goal) {
		t.Fatal("saved goal did not synchronize and read back before publication")
	}
	f.receipt["saved_goal_sha256"] = publication.Digest
	scalar := f.engine("stream", "sentinel", "set", "endpos", savedE, "--dir", "/work/pgcopydb", "--target", f.conninfo(f.targetName))
	if scalar.exitCode != 0 || !scalar.stdoutEOF || !scalar.stderrEOF || string(scalar.stdout) != savedE+"\n" {
		t.Fatal("scalar publication did not acknowledge the identical locally saved goal")
	}
	return savedE, end, expected
}

func (f *evidenceProducerFixture) checkProducerClone(producer string, completed evidenceToolObservation, expected []byte) {
	t := f.t
	t.Helper()
	f.requireStopped(producer)
	if len(completed.stdout) == 0 || completed.stdout[len(completed.stdout)-1] != '\n' {
		t.Fatal("actual clone report channel ended with an incomplete record")
	}
	frames := 0
	for line := range bytes.SplitSeq(completed.stdout, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("native-report: ")) {
			continue
		}
		raw := bytes.TrimPrefix(line, []byte("native-report: "))
		report, err := ParseReport(raw, Binding{Command: evidenceCloneCommand, Work: f.work})
		if err != nil {
			t.Fatal("actual clone report did not bind to the saved caller")
		}
		if report.Phase == "completed" {
			frames++
			f.receipt["actual_clone_sha256"] = f.retain("actual-clone.json", append(bytes.Clone(raw), '\n'), 65536)
		}
	}
	if frames != 1 || !bytes.Equal(f.sql(f.targetName, `SELECT count(*)::text||'|'||sum(id)::text FROM producer_fixture.rows; SELECT last_value::text||'|'||is_called::text FROM producer_fixture.seq;`), expected) {
		t.Fatal("actual clone completion or copied table/sequence result disagrees")
	}
}

func (f *evidenceProducerFixture) rejectProducerCase(name string, rawCaller, rawExecution, rawAck []byte, savedE string, checkpoint *CheckpointEvidence, original evidenceProducerPrivateState) {
	t := f.t
	t.Helper()
	if name == "tampered_report" {
		tampered := bytes.Replace(rawAck, []byte(`"checkpoint_id":"`+checkpoint.CheckpointID+`"`), []byte(`"checkpoint_id":"1:0:999999"`), 1)
		if bytes.Equal(tampered, rawAck) {
			t.Fatal("copied actual report was not changed")
		}
		if report, err := ParseReportLine(tampered, Binding{Command: commandSetEndpos, Work: f.work, EndLSN: savedE, Checkpoint: checkpoint}); err == nil || report != nil {
			t.Fatal("tampered actual report supplied its own expected authority")
		}
		return
	}
	caller, execution := bytes.Clone(rawCaller), bytes.Clone(rawExecution)
	var fields map[string]any
	decoder := json.NewDecoder(bytes.NewReader(caller))
	decoder.UseNumber()
	if decoder.Decode(&fields) != nil {
		t.Fatal("saved private caller is malformed")
	}
	volume := f.volume
	env := f.helperEnv(f.targetName)
	switch name {
	case "wrong_work":
		fields[evidenceWork].(map[string]any)["workdir_id"] = "wrong-work"
	case evidenceWrongSavedE:
		end, _ := sentinel.ParseLSN(savedE)
		fields["end_lsn"] = end + 1
	case evidenceWrongExecution:
		var e map[string]any
		_ = json.Unmarshal(execution, &e)
		e["attempt"] = 2
		execution = evidenceProducerJSON(t, e)
	case evidenceWrongTargetEndpoint:
		env = f.helperEnv(f.sourceName)
	case "wrong_volume":
		volume = f.newVolume()
	}
	caller = evidenceProducerJSON(t, fields)
	operation := evidenceProducerID(t)
	observation := f.probe(volume, env, evidenceObserve, string(caller), string(execution), operation, f.rootID)
	cp, err := evidenceProducerCheckpoint(observation, f.want(operation, caller, execution, savedE))
	var receipt evidenceProducerObservation
	if err == nil || cp != nil || !composedObject(observation.stdout, evidenceProducerFields, &receipt) || receipt.Observed || len(receipt.Positions) != 0 {
		t.Fatal("negative observation retained checkpoint authority")
	}
	if name != evidenceWrongTargetEndpoint && (receipt.Admitted || len(receipt.Stages) != 0 || receipt.SourceClosed) {
		t.Fatal("initial refusal performed SQL stages")
	}
	if name == evidenceWrongTargetEndpoint && (!receipt.Admitted || !receipt.SourceClosed || !slices.Equal(receipt.Stages, []string{"target_settings"})) {
		t.Fatal("wrong endpoint did not fail its independent target identity check")
	}
	if after := f.privateState(rawCaller); after != original {
		t.Fatal("negative control changed successful native authority")
	}
}

func evidenceProducerSelect(t *testing.T, binaryPath, binarySHA string) *evidenceProducerFixture {
	t.Helper()
	manifestPath, manifestSHA := os.Getenv("NATIVE_EVIDENCE_ENGINE_MANIFEST"), os.Getenv("NATIVE_EVIDENCE_ENGINE_MANIFEST_SHA256")
	libpq, libpqSHA := os.Getenv("NATIVE_EVIDENCE_ENGINE_LIBPQ"), os.Getenv("NATIVE_EVIDENCE_ENGINE_LIBPQ_SHA256")
	database := os.Getenv("NATIVE_EVIDENCE_PG_CONTAINER")
	if os.Getenv("NATIVE_EVIDENCE_MAINTENANCE_RELEASE") != "actual-producer-pg18" || !evidenceProducerHex64.MatchString(database) || os.Getenv("NATIVE_EVIDENCE_PG_LEASE_ID") != database || os.Getenv("NATIVE_EVIDENCE_TOOL_IMAGE") != evidenceToolImage || !evidenceProducerHex64.MatchString(binarySHA) || !evidenceProducerHex64.MatchString(manifestSHA) || !evidenceProducerHex64.MatchString(libpqSHA) {
		t.Fatal("actual producer prerequisites are explicit but incomplete or inconsistent")
	}
	read := func(path, want string, cap int64) []byte {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > cap {
			t.Fatal("assigned candidate file is missing, linked or exceeds its bound")
		}
		in, err := os.Open(path)
		if err != nil {
			t.Fatal("assigned candidate file is unreadable")
		}
		defer func() {
			if err := in.Close(); err != nil {
				t.Fatal("assigned candidate file did not close cleanly")
			}
		}()
		raw, err := io.ReadAll(io.LimitReader(in, cap+1))
		if err != nil || int64(len(raw)) > cap || evidenceProducerHash(raw) != want {
			t.Fatal("assigned candidate file hash does not match")
		}
		return raw
	}
	executable := read(binaryPath, binarySHA, 128<<20)
	info, _ := os.Stat(binaryPath)
	if info.Mode()&0111 == 0 || len(executable) < 64 || !bytes.Equal(executable[:6], []byte{0x7f, 'E', 'L', 'F', 2, 1}) || (binary.LittleEndian.Uint16(executable[18:20]) != 62 && binary.LittleEndian.Uint16(executable[18:20]) != 183) {
		t.Fatal("assigned producer is not a readable supported Linux executable")
	}
	rawManifest := read(manifestPath, manifestSHA, 4<<20)
	var candidate struct {
		Entries map[string]struct {
			SHA string `json:"sha256"`
		} `json:"entries"`
	}
	var source struct {
		SHA string `json:"source_sha256"`
	}
	sourceRaw, err := os.ReadFile("testdata/evidence/native_source_manifest.json")
	if err != nil || json.Unmarshal(rawManifest, &candidate) != nil || json.Unmarshal(sourceRaw, &source) != nil || candidate.Entries["src/apply/metadata.rs"].SHA != source.SHA {
		t.Fatal("candidate private schema differs from the frozen reader input")
	}
	_ = read(libpq, libpqSHA, 16<<20)
	f := &evidenceProducerFixture{t: t, database: database, binary: binaryPath, binarySHA: binarySHA, manifestSHA: manifestSHA, libpq: libpq, libpqSHA: libpqSHA, owner: "m6-actual-" + evidenceProducerID(t), volumeCreated: map[string]string{}}
	f.directory, err = os.MkdirTemp("", "native-actual-proof-")
	if err != nil {
		t.Fatal("cannot retain a private runtime receipt")
	}
	if os.Chmod(f.directory, 0700) != nil {
		t.Fatal("cannot protect the private receipt directory")
	}
	f.receipt = map[string]any{"mocked_kubernetes_owner": true, "production_admission": false, "candidate_manifest_sha256": manifestSHA, "binary_sha256": binarySHA, "libpq_sha256": libpqSHA, "tool_image": evidenceToolImage, "database_container": database, "euid": 65532, "qualified_case": false, "cleanup_complete": false}
	f.persist()
	t.Cleanup(func() {
		f.t = t
		f.receipt["test_failed"] = t.Failed()
		f.persist()
		t.Logf("private actual-producer receipt retained: %s", filepath.Join(f.directory, "receipt.json"))
	})
	owner := strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{ index .Config.Labels \"io.rpgmigrate.owner\" }}", database)))
	image := strings.TrimPrefix(strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{.Image}}", database))), "sha256:")
	if owner != evidenceDatabaseOwner || image != evidencePG18ImageSHA {
		t.Fatal("assigned PG18 lease ownership or image identity changed")
	}
	f.receipt["server_image"] = image
	return f
}

func (f *evidenceProducerFixture) retain(name string, raw []byte, cap int) string {
	f.t.Helper()
	if !slices.Contains([]string{"helper-observation.json", "actual-ack.json", "actual-clone.json"}, name) || len(raw) == 0 || len(raw) > cap {
		f.t.Fatal("private proof transport exceeds its bound")
	}
	out, err := os.OpenFile(filepath.Join(f.directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		f.t.Fatal("private proof transport already exists or is unavailable")
	}
	_, writeErr := out.Write(raw)
	syncErr := out.Sync()
	closeErr := out.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		f.t.Fatal("private proof transport did not synchronize")
	}
	return evidenceProducerHash(raw)
}

func (f *evidenceProducerFixture) persist() {
	f.t.Helper()
	f.receipt["containers"] = f.containers
	f.receipt["volumes"] = f.volumes
	f.receipt["volume_creation_identities"] = f.volumeCreated
	raw, err := json.Marshal(f.receipt)
	if err != nil || len(raw) > 65536 {
		f.t.Fatal("private fixture ledger exceeds its bound")
	}
	out, err := os.OpenFile(filepath.Join(f.directory, "receipt.json"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		f.t.Fatal("cannot open private fixture ledger")
	}
	_, writeErr := out.Write(raw)
	syncErr := out.Sync()
	closeErr := out.Close()
	dir, err := os.Open(f.directory)
	if err != nil {
		f.t.Fatal("cannot synchronize private ledger directory")
	}
	dirErr := dir.Sync()
	_ = dir.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil || dirErr != nil {
		f.t.Fatal("cannot synchronize private fixture ledger")
	}
}

func (f *evidenceProducerFixture) create(volume, entry string, env []string, args ...string) string {
	f.t.Helper()
	probe, err := filepath.Abs("testdata/evidence")
	if err != nil {
		f.t.Fatal("private fixture path is unavailable")
	}
	binaryPath, err := filepath.Abs(f.binary)
	if err != nil {
		f.t.Fatal("assigned binary path is unavailable")
	}
	libpqPath, err := filepath.Abs(f.libpq)
	if err != nil {
		f.t.Fatal("assigned library path is unavailable")
	}

	if entry == "/candidate/rpgmigrate" {
		entry = "/usr/bin/perl"
		const exact = `use strict;use warnings;use Digest::SHA;my($binary,$library)=splice(@ARGV,0,2);$>==65532 or die "uid\n";for my $pair(['/candidate/rpgmigrate',$binary],['/candidate/lib/libpq.so.5',$library]){open(my $in,'<',$pair->[0])or die "candidate\n";binmode($in);my $sha=Digest::SHA->new(256);$sha->addfile($in);close($in)or die "candidate\n";$sha->hexdigest eq$pair->[1]or die "candidate\n"}exec @ARGV;die "exec\n";`
		args = append([]string{"-MDigest::SHA", "-e", exact, f.binarySHA, f.libpqSHA, "/candidate/rpgmigrate"}, args...)
	}
	command := []string{"create", "--pull=never", "--label=io.rpgmigrate.owner=" + f.owner, "--user=65532:65532", "--read-only", "--cap-drop=all", "--security-opt=no-new-privileges", "--memory=512m", "--memory-swap=512m", "--cpus=1", "--pids-limit=128", "--log-driver=none", "--tmpfs=/tmp:rw,noexec,nosuid,nodev,mode=1777", "--volume=" + probe + ":/probe:ro", "--volume=" + binaryPath + ":/candidate/rpgmigrate:ro", "--volume=" + libpqPath + ":/candidate/lib/libpq.so.5:ro", "--network=container:" + f.database, "--env=PGHOST=127.0.0.1", "--env=PGPORT=5432", "--env=PGUSER=postgres", "--env=PGDATABASE=postgres", "--env=HOME=/tmp", "--env=LD_LIBRARY_PATH=/candidate/lib", "--entrypoint=" + entry}
	if volume != "" {
		command = append(command, "--volume="+volume+":/work:nocopy")
	}
	for _, value := range env {
		command = append(command, "--env="+value)
	}
	command = append(command, evidenceToolImage)
	command = append(command, args...)
	id := strings.TrimSpace(string(evidencePodman(f.t, command...)))
	if !evidenceProducerHex64.MatchString(id) {
		f.t.Fatal("new fixture container identity is invalid")
	}
	f.containers = append(f.containers, id)
	f.persist()
	return id
}

func (f *evidenceProducerFixture) run(id string) evidenceToolObservation {
	f.t.Helper()
	process := evidenceProducerStart("start", "-a", id)
	defer process.cancel()
	observation := <-process.done
	f.requireStopped(id)
	return observation
}

func (f *evidenceProducerFixture) tool(volume, entry string, env []string, args ...string) evidenceToolObservation {
	return f.run(f.create(volume, entry, env, args...))
}

func (f *evidenceProducerFixture) requireClean(o evidenceToolObservation) {
	f.t.Helper()
	if o.exitCode != 0 || !o.stdoutEOF || !o.stderrEOF || len(o.stderr) != 0 {
		f.t.Fatal("private helper did not exit with fully closed clean channels")
	}
}

func (f *evidenceProducerFixture) requireStopped(id string) {
	f.t.Helper()
	if strings.TrimSpace(string(evidencePodman(f.t, "inspect", "--format={{ index .Config.Labels \"io.rpgmigrate.owner\" }}", id))) != f.owner || strings.TrimSpace(string(evidencePodman(f.t, "inspect", "--format={{.State.Running}}", id))) != "false" {
		f.t.Fatal("owned fixture container is not proved stopped")
	}
}

func (f *evidenceProducerFixture) probe(volume string, env []string, args ...string) evidenceToolObservation {
	f.t.Helper()
	command := append([]string{"-I", "/probe", "/probe/producer_probe.pl"}, args...)
	o := f.tool(volume, "/usr/bin/perl", env, command...)
	f.requireClean(o)
	return o
}

func (f *evidenceProducerFixture) engine(args ...string) evidenceToolObservation {
	f.t.Helper()
	o := f.tool(f.volume, "/candidate/rpgmigrate", f.engineEnv, args...)
	f.requireClean(o)
	return o
}

func (f *evidenceProducerFixture) newVolume() string {
	f.t.Helper()
	name := "m6-actual-" + evidenceProducerID(f.t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := exec.CommandContext(ctx, "podman", "volume", "exists", name).Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		f.t.Fatal("fresh fixture volume name was not proved absent")
	}
	if strings.TrimSpace(string(evidencePodman(f.t, "volume", "create", "--uid=65532", "--gid=65532", "--opt=nocopy", "--label=io.rpgmigrate.owner="+f.owner, name))) != name {
		f.t.Fatal("private volume creation did not return its exclusive name")
	}
	f.volumes = append(f.volumes, name)
	identity := strings.TrimSpace(string(evidencePodman(f.t, "volume", "inspect", "--format={{.CreatedAt}}", name)))
	if identity == "" || strings.TrimSpace(string(evidencePodman(f.t, "volume", "inspect", "--format={{ index .Labels \"io.rpgmigrate.owner\" }}", name))) != f.owner {
		f.t.Fatal("new fixture volume lacks exact ownership")
	}
	f.volumeCreated[name] = identity
	f.persist()
	// Provision only the new empty owned volume before any native state can exist.
	const provision = `use strict;use warnings;use Fcntl qw(:DEFAULT :mode);use IO::Handle;use EvidencePrototype;
$>==65532&&$(=~/\A65532(?:\s|\z)/&&$)=~/\A65532(?:\s|\z)/ or die "uid\n";
sysopen(my $root,'/',O_RDONLY|O_NOFOLLOW|O_DIRECTORY)or die "ancestor\n";my @a=stat($root);($a[4]==0||$a[4]==65532)&&(!($a[2]&0022)||($a[2]&01000))or die "ancestor\n";
sysopen(my $work,'/work',O_RDONLY|O_NOFOLLOW|O_DIRECTORY)or die "root\n";my @s=stat($work);S_ISDIR($s[2])&&$s[4]==65532&&$s[5]==65532&&($s[2]&07777)==0755 or die "initial\n";
opendir(my $d,'/proc/self/fd/'.fileno($work))or die "entries\n";my @e=grep{$_ ne'.'&&$_ ne'..'}readdir($d);closedir($d)or die "entries\n";@e==0&&!lstat('/work/pgcopydb')or die "not_empty\n";
my $id=EvidencePrototype::_id($work);sysopen(my $again,'/work',O_RDONLY|O_NOFOLLOW|O_DIRECTORY)or die "root\n";EvidencePrototype::_id($again)eq$id or die "replaced\n";
chmod(0700,'/proc/self/fd/'.fileno($work))==1 or die "provision\n";$work->sync or die "sync\n";
EvidencePrototype::_id(EvidencePrototype::_open('/work',1))eq$id&&EvidencePrototype::_id($work)eq$id&&!lstat('/work/pgcopydb')or die "replaced\n";my @after=stat($work);$after[4]==65532&&$after[5]==65532&&($after[2]&07777)==0700 or die "provision\n";opendir(my $last,'/proc/self/fd/'.fileno($work))or die "entries\n";my @last=grep{$_ ne'.'&&$_ ne'..'}readdir($last);closedir($last)or die "entries\n";@last==0 or die "not_empty\n";print $id,"\n";`
	o := f.tool(name, "/usr/bin/perl", nil, "-I", "/probe", "-e", provision)
	f.requireClean(o)
	if !regexp.MustCompile(`^[0-9]+:[1-9][0-9]*\n$`).Match(o.stdout) {
		f.t.Fatal("secure initial volume provisioning did not prove its original identity")
	}
	if f.volume == "" {
		f.volume = name
		f.rootID = strings.TrimSpace(string(o.stdout))
		f.receipt["work_root_identity"] = f.rootID
		f.persist()
	}
	return name
}

func (f *evidenceProducerFixture) sql(database, sql string) []byte {
	f.t.Helper()
	o := evidenceProducerStart("exec", f.database, "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=on", "-U", "postgres", "-d", database, "-c", sql)
	defer o.cancel()
	observed := <-o.done
	f.requireClean(observed)
	return observed.stdout
}

func (f *evidenceProducerFixture) identity(database string, source bool) evidenceProducerIdentity {
	f.t.Helper()
	raw := strings.TrimSpace(string(f.sql(database, `SELECT c.system_identifier::text||'|'||d.oid::text||'|'||k.timeline_id::text||'|'||(current_setting('server_version_num')::integer/10000)::text FROM pg_control_system() c CROSS JOIN pg_control_checkpoint() k CROSS JOIN pg_database d WHERE d.datname=current_database();`)))
	var id evidenceProducerIdentity
	if _, err := fmt.Sscanf(raw, "%d|%d|%d|%d", &id.System, &id.Database, &id.Timeline, &id.Major); err != nil || id.System == 0 || id.Database == 0 || id.Timeline == 0 || id.Major != 18 {
		f.t.Fatal("independent database identity is absent or not PostgreSQL18")
	}
	if !source {
		id.Timeline = 0
	}
	return id
}

func (f *evidenceProducerFixture) prepare() {
	f.t.Helper()
	for _, tool := range []string{"psql", "pg_dump", "pg_restore"} {
		o := f.tool("", tool, nil, "--version")
		f.requireClean(o)
		if !bytes.Contains(o.stdout, []byte("(PostgreSQL) 18.")) {
			f.t.Fatal("runner does not carry target-major18 client tools")
		}
	}
	library := f.tool("", "/usr/bin/ldd", nil, "/candidate/rpgmigrate")
	f.requireClean(library)
	if !bytes.Contains(library.stdout, []byte("libpq.so.5 => /candidate/lib/libpq.so.5")) || bytes.Contains(library.stdout, []byte("not found")) {
		f.t.Fatal("actual producer linkage does not resolve the exact supplied libpq")
	}
	architecture := f.tool("", "/usr/bin/uname", nil, "-m")
	f.requireClean(architecture)
	header, err := os.Open(f.binary)
	if err != nil {
		f.t.Fatal("candidate executable cannot be rechecked")
	}
	var raw [64]byte
	_, err = io.ReadFull(header, raw[:])
	_ = header.Close()
	expected := map[uint16]string{62: "x86_64\n", 183: "aarch64\n"}[binary.LittleEndian.Uint16(raw[18:20])]
	if err != nil || string(architecture.stdout) != expected {
		f.t.Fatal("runner and supplied Linux binary architectures disagree")
	}
	capability := f.tool("", "/usr/bin/perl", nil, "-I", "/probe", "/probe/psql_probe.pl", "normal", "SELECT 1;")
	f.requireClean(capability)
	var measured evidencePSQLProbeReceipt
	if checkEvidenceToolObservation(capability) != nil || json.Unmarshal(capability.stdout, &measured) != nil || measured.ServerMajor != 18 {
		f.t.Fatal("runner Perl, PTY, psql and actual PG18 connection capabilities are unproved")
	}
	f.receipt["actual_server_major"] = f.identity("postgres", true).Major
	f.persist()
	f.newVolume()
	// A fresh unguessable namespace keeps fixture administration exclusive for slot continuity.
	suffix := evidenceProducerID(f.t)
	f.sourceName = "m6_producer_source_" + suffix
	f.targetName = "m6_producer_target_" + suffix
	for _, name := range []string{f.sourceName, f.targetName} {
		if string(f.sql("postgres", `SELECT count(*) FROM pg_database WHERE datname='`+name+`';`)) != "0\n" {
			f.t.Fatal("new database name is not absent")
		}
		f.sql("postgres", `CREATE DATABASE "`+name+`";`)
		id := f.identity(name, name == f.sourceName)
		if name == f.sourceName {
			f.source = id
		} else {
			f.target = id
		}
		f.receipt["source_database"] = f.source
		f.receipt["target_database"] = f.target
		f.receipt["source_database_name"] = f.sourceName
		f.receipt["target_database_name"] = f.targetName
		f.persist()
	}
	if f.source.System != f.target.System || f.source.Database == f.target.Database {
		f.t.Fatal("fixture database identities are not distinct within the owned cluster")
	}
	f.sql(f.sourceName, `CREATE SCHEMA producer_fixture; CREATE TABLE producer_fixture.rows(id integer PRIMARY KEY,payload text NOT NULL); INSERT INTO producer_fixture.rows VALUES(1,'private-fixture-canary-1'),(2,'private-fixture-canary-2'); CREATE SEQUENCE producer_fixture.seq; SELECT setval('producer_fixture.seq',41,true);`)
	run := evidenceProducerID(f.t)
	f.work = WorkIdentity{MigrationUID: run[:8] + "-" + run[8:12] + "-" + run[12:16] + "-" + run[16:20] + "-" + run[20:], RunID: run, WorkdirID: "pvc-" + evidenceProducerID(f.t), CheckpointGeneration: 1}
	job := "job-" + evidenceProducerID(f.t)
	f.execution = map[string]any{"migration_uid": f.work.MigrationUID, "job_controller_uid": f.work.MigrationUID, "pod_controller_uid": job, "job_uid": job, "pod_uid": "pod-" + evidenceProducerID(f.t), "pvc_uid": f.work.WorkdirID, "attempt": 1, "container": "pgcopydb", "endpoint_binding": "fixture-source-ref/fixture-target-ref"}
	f.engineEnv = []string{"RPGMIGRATE_MIGRATION_UID=" + f.work.MigrationUID, "RPGMIGRATE_RUN_ID=" + f.work.RunID, "RPGMIGRATE_WORKDIR_ID=" + f.work.WorkdirID, "RPGMIGRATE_CHECKPOINT_GENERATION=1"}
	f.receipt[evidenceWork] = f.work
	f.receipt["execution_sha256"] = evidenceProducerHash(evidenceProducerJSON(f.t, f.execution))
	f.persist()
}

func (f *evidenceProducerFixture) conninfo(database string) string {
	return "host=127.0.0.1 port=5432 dbname=" + database + " user=postgres sslmode=disable"
}
func (f *evidenceProducerFixture) helperEnv(target string) []string {
	return []string{"PGCOPYDB_SOURCE_PGURI=" + f.sourceName, "PGCOPYDB_TARGET_PGURI=" + target}
}
func (f *evidenceProducerFixture) want(operation string, caller, execution []byte, end string) evidenceProducerWant {
	return evidenceProducerWant{OperationID: operation, CallerSHA256: evidenceProducerHash(caller), ExecutionSHA256: evidenceProducerHash(execution), Work: f.work, EndLSN: end, SourceMajor: 18, TargetMajor: 18}
}

func (f *evidenceProducerFixture) privateState(caller []byte) evidenceProducerPrivateState {
	f.t.Helper()
	// Existing native validators own the schema; this transport retains identities and hashes only.
	const capture = `use strict;use warnings;use EvidencePrototype;use JSON::PP;use Digest::SHA qw(sha256_hex);
my($seed_raw,$caller_raw,$root_id)=@ARGV;$>==65532 or die "uid\n";my $parent=EvidencePrototype::_open('/work',1);EvidencePrototype::_id($parent)eq$root_id or die "root\n";
my $root=EvidencePrototype::_open('/work/pgcopydb',1);my($raw,$fd)=EvidencePrototype::_read($root,'manifest.json',1048576);my $m=EvidencePrototype::strict_json($raw,1048576);
my $seed=EvidencePrototype::strict_json($seed_raw,4096);EvidencePrototype::_operator($m->{operator},$seed->{work});
my $slot=$m->{source_slot};EvidencePrototype::_keys($slot,qw(phase record));$slot->{phase}eq'confirmed'or die "slot\n";EvidencePrototype::_slot($slot->{record});
EvidencePrototype::_same($slot->{record}{intent}{binding}{stream}{source},{system_identifier=>$seed->{source}{system_identifier},database_oid=>$seed->{source}{database_oid}});
my $follow=$m->{follow_state};EvidencePrototype::_follow_binding($follow->{binding});EvidencePrototype::_same($follow->{binding}{slot},$slot->{record});
my $end='';if(length($caller_raw)){my $caller=EvidencePrototype::strict_json($caller_raw,4096);my $target=EvidencePrototype::_manifest($m,$caller,$root);my $control=EvidencePrototype::_open('/proc/self/fd/'.fileno($root).'/control',1);my($bytes,$efd)=EvidencePrototype::_read($control,'endpos.json',4096);EvidencePrototype::_control(EvidencePrototype::strict_json($bytes,4096),$m,$caller,$target);$end=EvidencePrototype::_id($efd).':'.sha256_hex($bytes)}
print JSON::PP->new->canonical->encode({root=>EvidencePrototype::_id($root),manifest=>EvidencePrototype::_id($fd).':'.sha256_hex($raw),endpos=>$end,slot=>$slot->{record}{intent}{binding}{slot_name},publication=>$follow->{binding}{publication}{name},origin=>$follow->{origin_name}}),"\n";`
	seed := evidenceProducerJSON(f.t, map[string]any{evidenceWork: f.work, evidenceSource: f.source})
	o := f.tool(f.volume, "/usr/bin/perl", nil, "-I", "/probe", "-e", capture, string(seed), string(caller), f.rootID)
	f.requireClean(o)
	var state evidenceProducerPrivateState
	// Origin may be null because current native follow does not require a replication origin.
	var fields map[string]json.RawMessage
	if json.Unmarshal(o.stdout, &fields) != nil || len(fields) != 6 || json.Unmarshal(o.stdout, &state) != nil || state.Root == "" || state.Manifest == "" || !regexp.MustCompile(`^[a-z0-9_]{1,63}$`).MatchString(state.Slot) || !regexp.MustCompile(`^[a-z0-9_]{1,63}$`).MatchString(state.Publication) {
		f.t.Fatal("confirmed original resource receipt is incomplete")
	}
	f.receipt["native_state"] = state
	f.persist()
	return state
}

func (f *evidenceProducerFixture) checkSlot(slot string, active bool) int {
	f.t.Helper()
	actual := f.sql(f.sourceName, `SELECT COALESCE(active_pid,0) FROM pg_replication_slots WHERE slot_name='`+slot+`' AND datoid=`+fmt.Sprint(f.source.Database)+` AND plugin='pgoutput' AND active=`+fmt.Sprint(active)+` AND slot_type='logical';`)
	var pid int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(actual)), "%d", &pid); err != nil || pid < 0 || (active && pid == 0) || (!active && pid != 0) || strings.Count(string(actual), "\n") != 1 {
		f.t.Fatal("exact confirmed source slot does not match the exclusive fixture lifecycle")
	}
	return pid
}

func (f *evidenceProducerFixture) close(state evidenceProducerPrivateState) {
	f.t.Helper()
	f.checkSlot(state.Slot, false)
	obligations := []ResourceObligation{{Kind: evidenceReplicationSlotKind, Name: state.Slot, Owned: true}, {Kind: evidencePublicationKind, Name: state.Publication, Owned: true}, {Kind: "workdir", Name: "/work/pgcopydb", Owned: true, Retain: true}}
	if state.Origin != "" {
		obligations = append(obligations, ResourceObligation{Kind: "replication_origin", Name: state.Origin})
	}
	cleanup := f.engine("stream", "cleanup", "--dir", "/work/pgcopydb", "--source", f.conninfo(f.sourceName), "--target", f.conninfo(f.targetName))
	report, err := ParseReportLine(cleanup.stdout, Binding{Command: evidenceCleanupCommand, Work: f.work, Resources: obligations})
	if err != nil || report.Cleanup == nil || report.Cleanup.Complete == nil || !*report.Cleanup.Complete {
		f.t.Fatal("exact native cleanup failed; fixture resources remain in the private ledger")
	}
	if string(f.sql(f.sourceName, `SELECT (SELECT count(*) FROM pg_replication_slots WHERE slot_name='`+state.Slot+`')::text||'|'||(SELECT count(*) FROM pg_publication WHERE pubname='`+state.Publication+`')::text;`)) != "0|0\n" {
		f.t.Fatal("exact native source resource absence was not proved")
	}
	if state.Origin != "" {
		f.t.Fatal("retained origin requires its separate exact lifetime owner; fixture is preserved")
	}
	for _, name := range []string{f.sourceName, f.targetName} {
		got := f.identity(name, name == f.sourceName)
		want := f.source
		if name == f.targetName {
			want = f.target
		}
		if got != want || string(f.sql("postgres", `SELECT count(*) FROM pg_stat_activity WHERE datid=`+fmt.Sprint(want.Database)+`;`)) != "0\n" {
			f.t.Fatal("fixture database identity or closed-session condition changed")
		}
		f.sql("postgres", `DROP DATABASE "`+name+`";`)
		if string(f.sql("postgres", `SELECT count(*) FROM pg_database WHERE datname='`+name+`' OR oid=`+fmt.Sprint(want.Database)+`;`)) != "0\n" {
			f.t.Fatal("exact owned database absence was not proved")
		}
	}
	for _, id := range f.containers {
		f.requireStopped(id)
		evidencePodman(f.t, "rm", id)
		f.absent("container", id)
	}
	for _, name := range f.volumes {
		if strings.TrimSpace(string(evidencePodman(f.t, "volume", "inspect", "--format={{ index .Labels \"io.rpgmigrate.owner\" }}", name))) != f.owner || strings.TrimSpace(string(evidencePodman(f.t, "volume", "inspect", "--format={{.CreatedAt}}", name))) != f.volumeCreated[name] {
			f.t.Fatal("owned volume identity changed; refusing removal")
		}
		evidencePodman(f.t, "volume", "rm", name)
		f.absent("volume", name)
	}
	f.receipt["cleanup_complete"] = true
	f.persist()
}

func (f *evidenceProducerFixture) absent(kind, id string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := exec.CommandContext(ctx, "podman", kind, "exists", id).Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		f.t.Fatal("exact owned fixture absence was not proved")
	}
}
