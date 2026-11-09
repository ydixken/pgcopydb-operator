package native

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	retryMaterializationClosedKey = "materialization_closed"
	retryDuplicateVersionJSON     = `"version":1,"version":1`
	retryEvidenceRoleJSON         = `"role":"evidence"`
	retryTotalZeroJSON            = `"total":0`
)

func retryWant() ExecutionData {
	w := sourceWant()
	w.JobName, w.JobUID, w.WorkerJobUID = "migration-retry", "retry-job", "failed-worker"
	w.Role = JobRoleRetryGate
	return w
}

func retryResult(t *testing.T, w ExecutionData, body []byte, in RetryInputs) []byte {
	t.Helper()
	request, execution := requestDigests(body)
	v := map[string]any{
		"version": 1, readerKindKey: targetRetryKind, readerOperationIDKey: w.OperationID, preflightProgramKey: hex.EncodeToString(w.ProgramSHA256[:]),
		"request_sha256": hex.EncodeToString(request[:]), readerExecutionSHAKey: hex.EncodeToString(execution[:]), evidenceWork: w.Work,
		"observed_at_unix": time.Now().Unix(), "census_known": true, "total": 0, "syncrep": 0, "gate_waiting": 0, "oldest_syncrep_wait_unix": 0,
		"application_attribution_known": true, "target_synchronous_commit": v1beta1.SynchronousCommitLocal, "synchronous_commit_admitted": true,
		"probe_attempted": in.CommitTest, "probe_committed": in.CommitTest, preflightTargetClosedKey: true, retryMaterializationClosedKey: true, preflightDiagnosticsKey: true,
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func TestNativeRetryWire(t *testing.T) {
	w, in := retryWant(), RetryInputs{CommitTest: true, ExpectLocal: true}
	body, err := retryRequest(w, in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = parseRetryRequest(body); err != nil {
		t.Fatal(err)
	}
	raw := retryResult(t, w, body, in)
	now := time.Now().Unix()
	if _, err = parseRetryResult(raw, w, body, in, now-1, now+1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, old, next string }{
		{readerCaseDuplicate, readerVersionJSON, retryDuplicateVersionJSON},
		{readerCaseFolded, readerVersionJSON, sourceCaseVersionJSON},
		{readerCaseMissing, `"census_known":true,`, ``},
		{readerCaseNull, `"census_known":true`, `"census_known":null`},
		{readerCaseUnknown, readerVersionJSON, `"version":1,"ready":true`},
		{"fractional_count", retryTotalZeroJSON, `"total":0.0`},
		{"negative", retryTotalZeroJSON, `"total":-1`},
		{readerCaseOverflow, retryTotalZeroJSON, `"total":9223372036854775808`},
		{"over_total", `"syncrep":0`, `"syncrep":1`},
		{"oldest_without_wait", `"oldest_syncrep_wait_unix":0`, `"oldest_syncrep_wait_unix":1`},
		{"unknown_census", `"census_known":true`, `"census_known":false`},
		{"unclosed", `"target_closed":true`, `"target_closed":false`},
		{"diagnostic", `"diagnostics_clean":true`, `"diagnostics_clean":false`},
		{"uncommitted", `"probe_committed":true`, `"probe_committed":false`},
		{"probe_without_attempt", `"probe_attempted":true`, `"probe_attempted":false`},
		{"unadmitted_setting", `"synchronous_commit_admitted":true`, `"synchronous_commit_admitted":false`},
		{"setting", `"target_synchronous_commit":"local"`, `"target_synchronous_commit":"bad"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseRetryResult(replace(t, raw, tc.old, tc.next), w, body, in, now-1, now+1); err == nil {
				t.Fatal("invalid retry data admitted")
			}
		})
	}
	for _, bad := range [][]byte{bytes.TrimSuffix(raw, []byte{'\n'}), append(bytes.Clone(raw), '\n'), append(bytes.Clone(raw), raw...), append([]byte("noise\n"), raw...), []byte(strings.Repeat(" ", 65537))} {
		if _, err := parseRetryResult(bad, w, body, in, now-1, now+1); err == nil {
			t.Fatal("invalid retry frame admitted")
		}
	}
	if _, err := parseRetryResult(raw, w, append(bytes.Clone(body), ' '), in, now-1, now+1); err == nil {
		t.Fatal("reserialized request admitted")
	}
	if _, err := parseRetryResult(raw, w, body, in, now+2, now+3); err == nil {
		t.Fatal("stale target observation admitted")
	}
	if _, err := parseRetryResult(raw, w, body, in, now-3, now-2); err == nil {
		t.Fatal("future target observation admitted")
	}
	for _, tc := range []struct{ old, next string }{
		{`"kind":"target_retry"`, `"kind":"completed_evidence"`},
		{`"role":"retry_gate"`, retryEvidenceRoleJSON},
		{`"inputs":{"commit_test":true,"expect_local":true}`, `"inputs":{"commit_test":true}`},
		{`"commit_test":true`, `"commit_test":null`},
	} {
		if _, err := parseRetryRequest(replace(t, body, tc.old, tc.next)); err == nil {
			t.Fatal("invalid retry request admitted")
		}
	}
}

func TestNativeRetryDataNonAuthority(t *testing.T) {
	var zero RetryReadiness
	if got, ok := zero.Observation(); ok || got != (RetryGateObservation{}) {
		t.Fatal("zero value admitted data")
	}
	w, in := retryWant(), RetryInputs{CommitTest: true}
	body, err := retryRequest(w, in)
	if err != nil {
		t.Fatal(err)
	}
	raw := retryResult(t, w, body, in)
	raw = replace(t, raw, retryTotalZeroJSON, `"total":1`)
	raw = replace(t, raw, `"syncrep":0`, `"syncrep":1`)
	raw = replace(t, raw, `"gate_waiting":0`, `"gate_waiting":2`)
	raw = replace(t, raw, `"oldest_syncrep_wait_unix":0`, `"oldest_syncrep_wait_unix":1`)
	raw = replace(t, raw, `"probe_attempted":true`, `"probe_attempted":false`)
	raw = replace(t, raw, `"probe_committed":true`, `"probe_committed":false`)
	now := time.Now().Unix()
	if _, err := parseRetryResult(raw, w, body, in, now-1, now+1); err != nil {
		t.Fatalf("known blocked census is not valid DATA: %v", err)
	}
	if _, err := parseCompletedResult(raw, readerWant(), body, fixtureEndLSN); err == nil {
		t.Fatal("retry data supplied completion")
	}
}
