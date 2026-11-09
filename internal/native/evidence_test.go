package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

const (
	evidenceCloneCommand        = "clone"
	evidenceCleanupCommand      = "stream cleanup"
	evidencePublicationKind     = "publication"
	evidenceKeepalive           = "keepalive"
	evidenceLaterBegin          = "later_begin"
	evidenceAbsent              = "absent"
	evidenceCatalogCheckpoint   = "catalog_checkpoint"
	evidenceLostCommit          = "lost_commit"
	evidenceIdle                = "idle"
	evidenceExit                = "exit"
	evidenceSourceIdentity      = "source_identity"
	evidenceOutputLimit         = "output_limit"
	evidenceAdmission           = "admission"
	evidenceReplacedRelation    = "replaced_relation"
	evidenceSource              = "source"
	evidenceConnectionForm      = "connection_form"
	evidencePostCommitLoss      = "post_commit_loss"
	evidenceLaunched            = "launched"
	evidencePasswordCanary      = "private-password-canary"
	evidenceClosed              = "closed"
	evidenceOwnerBusy           = "owner_busy"
	evidenceDatabaseOwner       = "m6-evidence-db"
	evidencePG16ImageSHA        = "94a8e6634f916ffafee47d9be81c53d964e6ed2f5175224fab66b2abacae7279"
	evidencePG18ImageSHA        = "91e2759ab4ea7238bc7bed3893439d7abb8edd5c8ac24bab0571e4179f405235"
	evidenceRemoteApply         = "remote_apply"
	evidenceCounterOverflow     = "counter_overflow"
	evidenceMissingManifest     = "missing_manifest"
	evidenceRowRead             = "row_read"
	evidenceSyncCancel          = "sync_cancel"
	evidenceObserve             = "observe"
	evidenceNoStderrJSON        = "\"stderr_bytes\":0"
	evidenceProducerTokenJSON   = "\"token\":\"1:0:1\""
	evidenceCommit              = "commit"
	evidenceFork                = "fork"
	evidenceIdleGuard           = "idle_guard"
	evidenceKeepaliveLowCert    = "keepalive_low_cert"
	evidenceListening           = "listening"
	evidenceOther               = "other"
	evidencePersistGoal         = "persist_goal"
	evidencePipe                = "pipe"
	evidencePTY                 = "pty"
	evidenceReplicationSlotKind = "replication_slot"
	evidenceReturnedRefusal     = "returned_refusal"
	evidenceSecondCommitFault   = "second_commit_fault"
	evidenceTarget              = "target"
	evidenceTargetFence         = "target_fence"
	evidenceTargetIdentity      = "target_identity"
	evidenceTimeout             = "timeout"
	evidenceToken               = "token"
	evidenceUncompleted         = "uncompleted"
	evidenceValid               = "valid"
	evidenceWork                = "work"
	evidenceWrongAttempt        = "wrong_attempt"
	evidenceWrongExecution      = "wrong_execution"
	evidenceWrongPVC            = "wrong_pvc"
	evidenceWrongSavedE         = "wrong_saved_e"
	evidenceWrongSourceEndpoint = "wrong_source_endpoint"
	evidenceWrongTargetEndpoint = "wrong_target_endpoint"
)

type evidenceToolObservation struct {
	stdout    []byte
	stderr    []byte
	exitCode  int
	stdoutEOF bool
	stderrEOF bool
}

func TestNativeEvidenceRefusesUnprovedInput(t *testing.T) {
	for _, tc := range []struct {
		name        string
		observation evidenceToolObservation
	}{
		{"raw_report", evidenceToolObservation{stdout: []byte(`{"command":"stream sentinel get","phase":"completed"}`), stdoutEOF: true, stderrEOF: true}},
		{"visible_row", evidenceToolObservation{stdout: []byte(`{"checkpoint_id":"1:1:2","certified_lsn":"0/30"}`), stdoutEOF: true, stderrEOF: true}},
		{"missing_provenance", evidenceToolObservation{stdout: []byte("COMMIT\n"), stdoutEOF: true, stderrEOF: true}},
		{"unsupported_layout", evidenceToolObservation{stdout: []byte(`{"format_version":7,"metadata_version":5}`), stdoutEOF: true, stderrEOF: true}},
		{"unknown_commit", evidenceToolObservation{stdout: []byte("BEGIN\nUPDATE 1\n"), stdoutEOF: true, stderrEOF: true}},
		{"empty_output", evidenceToolObservation{stdoutEOF: true, stderrEOF: true}},
		{"exit_zero_warning", evidenceToolObservation{stdout: []byte("COMMIT\n"), stderr: []byte("WARNING: test-only warning\n"), stdoutEOF: true, stderrEOF: true}},
		{"late_stderr_not_drained", evidenceToolObservation{stdout: []byte("COMMIT\n"), stdoutEOF: true}},
		{"stdout_not_drained", evidenceToolObservation{stdout: []byte("COMMIT\n"), stderrEOF: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if checkEvidenceToolObservation(tc.observation) == nil {
				t.Fatal("unproved tool output was admitted")
			}
		})
	}
}

// This experiment does not construct checkpoint evidence or a production reader.
func checkEvidenceToolObservation(observation evidenceToolObservation) error {
	if observation.exitCode != 0 {
		return errors.New("evidence experiment: subprocess failed")
	}
	if !observation.stdoutEOF || !observation.stderrEOF || len(observation.stderr) != 0 {
		return errors.New("evidence experiment: channels not cleanly closed")
	}
	if !completeEvidenceToolReceipt(observation.stdout) {
		return errors.New("evidence experiment: missing or duplicate channel fields")
	}
	var receipt evidencePSQLProbeReceipt
	decoder := json.NewDecoder(bytes.NewReader(observation.stdout))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("evidence experiment: malformed channel receipt")
	}
	if receipt.Exit != 0 || receipt.Signal != 0 || !receipt.AllEOF || receipt.FaultConfirmed || !receipt.BackendStable || (receipt.ServerMajor != 16 && receipt.ServerMajor != 18) || receipt.StderrBytes != 0 || receipt.PromptBytes != 0 || receipt.Result != "1\n" || !slices.Equal(receipt.States, []string{"", "", "", ""}) {
		return errors.New("evidence experiment: incomplete channel receipt")
	}
	return nil
}

func TestNativeEvidenceToolObservationAcceptsOnlyClosedExperiment(t *testing.T) {
	observation := evidenceToolObservation{
		stdout:    []byte(`{"exit":0,"signal":0,"result":"1\n","stderr_bytes":0,"prompt_bytes":0,"states":["","","",""],"all_eof":true,"server_major":16,"backend_stable":true,"fault_confirmed":false}`),
		stdoutEOF: true,
		stderrEOF: true,
	}
	if err := checkEvidenceToolObservation(observation); err != nil {
		t.Fatal("closed test-only channel receipt was refused")
	}
}

type evidencePSQLProbeReceipt struct {
	Exit           int      `json:"exit"`
	Signal         int      `json:"signal"`
	Result         string   `json:"result"`
	StderrBytes    int      `json:"stderr_bytes"`
	PromptBytes    int      `json:"prompt_bytes"`
	States         []string `json:"states"`
	ServerMajor    int      `json:"server_major"`
	BackendStable  bool     `json:"backend_stable"`
	FaultConfirmed bool     `json:"fault_confirmed"`
	AllEOF         bool     `json:"all_eof"`
}

func TestNativeEvidenceChannelReceiptRefusesMissingAndDuplicateFields(t *testing.T) {
	good := `{"exit":0,"signal":0,"result":"1\n","stderr_bytes":0,"prompt_bytes":0,"states":["","","",""],"all_eof":true,"server_major":16,"backend_stable":true,"fault_confirmed":false}`
	for _, mutation := range []struct{ name, old, next string }{
		{"missing_exit", `"exit":0,`, ``},
		{"missing_stderr_count", `"stderr_bytes":0,`, ``},
		{"duplicate_exit", `"exit":0`, `"exit":0,"exit":0`},
		{"duplicate_result", `"result":"1\n"`, `"result":"1\n","result":"1\n"`},
		{"stderr_warning", evidenceNoStderrJSON, `"stderr_bytes":72`},
		{"not_idle", `"states":["","","",""]`, `"states":["","","*"]`},
		{"unknown_state", `"states":["","","",""]`, `"states":["","","?"]`},
		{"unclassified_prompt_bytes", `"prompt_bytes":0`, `"prompt_bytes":16`},
		{"prompt_in_result", `"result":"1\n"`, `"result":"M6:fake::END"`},
		{"not_all_eof", `"all_eof":true`, `"all_eof":false`},
		{"faulted_session", `"fault_confirmed":false`, `"fault_confirmed":true`},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			raw := []byte(strings.Replace(good, mutation.old, mutation.next, 1))
			if checkEvidenceToolObservation(evidenceToolObservation{stdout: raw, stdoutEOF: true, stderrEOF: true}) == nil {
				t.Fatal("malformed or incomplete tool-channel receipt was accepted")
			}
		})
	}
}

func completeEvidenceToolReceipt(raw []byte) bool {
	if len(raw) > 65536 {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return false
		}
		seen[key] = true
		if decoder.Decode(new(json.RawMessage)) != nil {
			return false
		}
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') || decoder.Decode(new(any)) != io.EOF {
		return false
	}
	return len(seen) == 10 && seen[evidenceExit] && seen["signal"] && seen["result"] && seen["stderr_bytes"] && seen["prompt_bytes"] && seen["states"] && seen["all_eof"] && seen["server_major"] && seen["backend_stable"] && seen["fault_confirmed"]
}
