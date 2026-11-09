package native

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const evidenceToolImage = "sha256:0b364ee727a82a2500856f2bb3ef9cd634c825db503caeffe1946e12cd8c8297"

func TestNativeEvidenceLiveFilesystem(t *testing.T) {
	for _, name := range []string{
		evidenceValid, "missing", "symlink", "hardlink", "permissions", "manifest_pending",
		"control_pending", "unknown_entry", "oversized_manifest", "oversized_control",
		"truncated", evidenceOwnerBusy, "control_busy", "replaced", "interrupted_read", "directory_replaced",
	} {
		t.Run(name, func(t *testing.T) {
			raw := runEvidenceTool(t, "fs_probe.pl", "", name)
			var receipt struct {
				Case     string `json:"case"`
				Accepted bool   `json:"accepted"`
			}
			if json.Unmarshal(raw, &receipt) != nil || receipt.Case != name {
				t.Fatal("filesystem experiment returned a malformed receipt")
			}
			if receipt.Accepted != (name == evidenceValid) {
				t.Fatal("filesystem experiment admitted an unsafe fixture or refused its valid fixture")
			}
		})
	}
}

func TestNativeEvidenceLivePSQLChannels(t *testing.T) {
	database := os.Getenv("NATIVE_EVIDENCE_PG_CONTAINER")
	if database == "" {
		t.Skip("disposable database container is required; evidence qualification remains open")
	}
	raw := runEvidenceTool(t, "psql_probe.pl", database, "normal", "SELECT 1;")
	if err := checkEvidenceToolObservation(evidenceToolObservation{stdout: raw, stdoutEOF: true, stderrEOF: true}); err != nil {
		t.Fatal("psql did not expose clean, separate query, transaction-state and diagnostic channels")
	}
}

func runEvidenceTool(t *testing.T, script, database string, operands ...string) []byte {
	t.Helper()
	if os.Getenv("NATIVE_EVIDENCE_TOOL_IMAGE") != evidenceToolImage {
		t.Skip("measured native tool image must be explicitly selected; evidence qualification remains open")
	}
	directory, err := filepath.Abs("testdata/evidence")
	if err != nil {
		t.Fatal("cannot locate private experiment fixtures")
	}
	args := []string{"create", "--pull=never", "--label=io.rpgmigrate.owner=m6-evidence-experiment", "--read-only", "--cap-drop=all", "--security-opt=no-new-privileges", "--memory=256m", "--memory-swap=256m", "--cpus=0.5", "--pids-limit=128", "--log-driver=none", "--tmpfs=/tmp:rw,noexec,nosuid,nodev,mode=1777", "--volume=" + directory + ":/probe:ro", "--entrypoint=/usr/bin/perl"}
	if database == "" {
		args = append(args, "--network=none")
	} else {
		owner := strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{ index .Config.Labels \"io.rpgmigrate.owner\" }}", database)))
		if owner != evidenceDatabaseOwner {
			t.Fatal("database is not an owned evidence experiment")
		}
		args = append(args, "--network=container:"+database, "--env=PGHOST=127.0.0.1", "--env=PGUSER=postgres", "--env=PGDATABASE=postgres", "--env=HOME=/tmp")
		if script == "admission_probe.pl" && os.Getenv("NATIVE_EVIDENCE_SOURCE_DATABASE") != "" {
			args = append(args, "--env=NATIVE_EVIDENCE_SOURCE_DATABASE")
		}
	}
	if script == "admission_probe.pl" {
		volume := fmt.Sprintf("m6-evidence-admission-%d", time.Now().UnixNano())
		evidencePodman(t, "volume", "create", "--label=io.rpgmigrate.owner=m6-evidence-admission", volume)
		t.Cleanup(func() {
			owner := strings.TrimSpace(string(evidencePodman(t, "volume", "inspect", "--format={{ index .Labels \"io.rpgmigrate.owner\" }}", volume)))
			if owner != "m6-evidence-admission" {
				t.Error("private volume ownership changed")
				return
			}
			evidencePodman(t, "volume", "rm", volume)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := exec.CommandContext(ctx, "podman", "volume", "exists", volume).Run()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
				t.Error("private volume absence was not proved")
			}
		})
		args = append(args, "--volume="+volume+":/work")
	}
	args = append(args, evidenceToolImage, "/probe/"+script)
	args = append(args, operands...)
	cid := strings.TrimSpace(string(evidencePodman(t, args...)))
	if len(cid) != 64 || strings.ContainsAny(cid, "\r\n ") {
		t.Fatal("invalid owned container identity")
	}
	t.Logf("owned tool container %s", cid)
	t.Cleanup(func() {
		owner := strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{ index .Config.Labels \"io.rpgmigrate.owner\" }}", cid)))
		if owner != "m6-evidence-experiment" {
			t.Error("tool container ownership changed; refusing removal")
			return
		}
		evidencePodman(t, "rm", "--force", cid)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := exec.CommandContext(ctx, "podman", "container", "exists", cid).Run()
		if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
			t.Error("owned tool container absence was not proved")
		}
	})
	raw := evidencePodman(t, "start", "-a", cid)
	if script == "psql_probe.pl" && database != "" {
		image := strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{.Image}}", database)))
		expected := map[string]int{
			evidencePG16ImageSHA: 16,
			evidencePG18ImageSHA: 18,
		}[strings.TrimPrefix(image, "sha256:")]
		var receipt evidencePSQLProbeReceipt
		if json.Unmarshal(raw, &receipt) != nil || expected == 0 || receipt.ServerMajor != expected {
			t.Fatal("actual observed server major does not match the selected disposable image")
		}
		t.Logf("observed PostgreSQL %d in private session receipt", receipt.ServerMajor)
	}
	return raw
}

func evidencePodman(t *testing.T, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "podman", args...)
	var stdout, stderr evidenceBoundedBuffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if err != nil || stderr.Len() != 0 {
		if path, ok := retainEvidencePartial(stdout.Bytes()); ok {
			t.Logf("validated private partial diagnostic retained at %s", path)
		}
		t.Fatalf("evidence experiment subprocess failed or wrote diagnostics (stderr bytes=%d)", stderr.Len())
	}
	return stdout.Bytes()
}

type evidenceBoundedBuffer struct{ bytes.Buffer }

func (b *evidenceBoundedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 1<<20 {
		return 0, fmt.Errorf("evidence experiment output exceeded its bound")
	}
	return b.Buffer.Write(p)
}

func TestNativeEvidenceLiveRefusesNoDatabase(t *testing.T) {
	raw := runEvidenceTool(t, "psql_probe.pl", "", "normal", "SELECT 1;")
	var receipt evidencePSQLProbeReceipt
	if json.Unmarshal(raw, &receipt) != nil || !receipt.AllEOF || receipt.Exit == 0 || receipt.StderrBytes == 0 {
		t.Fatal("psql connection failure was not captured and fully drained")
	}
	if checkEvidenceToolObservation(evidenceToolObservation{stdout: raw, stdoutEOF: true, stderrEOF: true}) == nil {
		t.Fatal("failed psql session was admitted")
	}
}

func TestNativeEvidenceLivePSQLTransactionStates(t *testing.T) {
	database := os.Getenv("NATIVE_EVIDENCE_PG_CONTAINER")
	if database == "" {
		t.Skip("disposable database container is required; evidence qualification remains open")
	}
	for _, mode := range []string{"transaction", "warning", "notice", "deferred_failure", "false_unlock", "multiple_unlock", "malformed_unlock", "prompt_in_row", "long_query"} {
		t.Run(mode, func(t *testing.T) {
			raw := runEvidenceTool(t, "psql_probe.pl", database, mode)
			var receipt evidencePSQLProbeReceipt
			if json.Unmarshal(raw, &receipt) != nil || !receipt.AllEOF {
				t.Fatal("missing closed psql experiment receipt")
			}
			switch mode {
			case "transaction":
				if receipt.Result != "CREATE TABLE\nINSERT 0 1\nt\nBEGIN\nSET\nUPDATE 1\nCOMMIT\nt\n" || !slices.Equal(receipt.States, []string{"", "", "", "", "", "", "*", "*", "*", "", ""}) || receipt.StderrBytes != 0 || receipt.Exit != 0 || receipt.PromptBytes != 0 {
					t.Fatal("same-session transaction, command tags or exact true unlock were not observed")
				}
			case "warning", "notice", "deferred_failure":
				if receipt.StderrBytes == 0 {
					t.Fatal("SQL diagnostic was not captured separately")
				}
			case "false_unlock":
				if receipt.Result != "f\n" || receipt.StderrBytes == 0 {
					t.Fatal("false unlock result and warning were not captured")
				}
			case "multiple_unlock":
				if receipt.Result != "t\nt\nf\n" || receipt.StderrBytes == 0 {
					t.Fatal("multiple unlock results were not captured")
				}
			case "malformed_unlock":
				if receipt.Result != "malformed\n" {
					t.Fatal("malformed result was not retained in the query channel")
				}
			case "prompt_in_row":
				if receipt.Result != "M6:4b8b986f69374feeb27bbe0c01bd4aac::END\n" || len(receipt.States) != 4 || receipt.PromptBytes != 0 {
					t.Fatal("row bytes affected the status prompt channel")
				}
			case "long_query":
				if receipt.Result != strings.Repeat("x", 8192)+"\n" || receipt.StderrBytes != 0 || receipt.PromptBytes != 0 {
					t.Fatal("long SQL input was truncated or altered by the terminal")
				}
			}
		})
	}
}

func TestNativeEvidenceLiveSynchronousCancellation(t *testing.T) {
	database := os.Getenv("NATIVE_EVIDENCE_PG_CONTAINER")
	if database == "" {
		t.Skip("disposable database container is required; evidence qualification remains open")
	}
	evidencePodman(t, "exec", "--env=PGOPTIONS=-c synchronous_commit=off", database, "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=on", "-U", "postgres", "-d", "postgres", "-c", `ALTER SYSTEM SET synchronous_standby_names='FIRST 1 ("m6_missing")'`, "-c", "SELECT pg_reload_conf();")
	t.Cleanup(func() {
		evidencePodman(t, "exec", "--env=PGOPTIONS=-c synchronous_commit=off", database, "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=on", "-U", "postgres", "-d", "postgres", "-c", "ALTER SYSTEM RESET synchronous_standby_names", "-c", "SELECT pg_reload_conf();")
	})
	raw := runEvidenceTool(t, "psql_probe.pl", database, evidenceSyncCancel)
	var receipt evidencePSQLProbeReceipt
	if json.Unmarshal(raw, &receipt) != nil || !receipt.AllEOF || !receipt.FaultConfirmed || !receipt.BackendStable || receipt.Exit != 0 || receipt.StderrBytes == 0 || !strings.Contains(receipt.Result, "COMMIT\n") {
		t.Fatal("actual synchronous-replication cancellation with COMMIT and exit zero was not observed")
	}
	if checkEvidenceToolObservation(evidenceToolObservation{stdout: raw, stdoutEOF: true, stderrEOF: true}) == nil {
		t.Fatal("cancelled synchronous commit was admitted")
	}
}

func TestNativeEvidenceLiveCommitReplyLoss(t *testing.T) {
	database := os.Getenv("NATIVE_EVIDENCE_PG_CONTAINER")
	if database == "" {
		t.Skip("disposable database container is required; evidence qualification remains open")
	}
	for _, mode := range []string{evidenceLostCommit, evidencePostCommitLoss} {
		t.Run(mode, func(t *testing.T) {
			raw := runEvidenceTool(t, "psql_probe.pl", database, mode)
			var receipt evidencePSQLProbeReceipt
			if json.Unmarshal(raw, &receipt) != nil || !receipt.AllEOF || !receipt.FaultConfirmed || (mode == evidenceLostCommit && receipt.StderrBytes == 0) {
				t.Fatalf("COMMIT fault not fully observed (exit=%d, signal=%d, stderr_bytes=%d, prompt_bytes=%d, fault=%t, eof=%t)", receipt.Exit, receipt.Signal, receipt.StderrBytes, receipt.PromptBytes, receipt.FaultConfirmed, receipt.AllEOF)
			}
			if mode == evidenceLostCommit && strings.Contains(receipt.Result, "COMMIT\n") {
				t.Fatal("dropped COMMIT reply reached the observation channel")
			}
			if mode == evidencePostCommitLoss && !strings.HasSuffix(receipt.Result, "COMMIT\n") {
				t.Fatal("post-COMMIT loss did not follow a successful COMMIT reply")
			}
			if checkEvidenceToolObservation(evidenceToolObservation{stdout: raw, stdoutEOF: true, stderrEOF: true}) == nil {
				t.Fatal("unknown or disconnected COMMIT observation was admitted")
			}
		})
	}
}

func TestNativeEvidenceLiveSynchronousSettings(t *testing.T) {
	database := os.Getenv("NATIVE_EVIDENCE_PG_CONTAINER")
	if database == "" {
		t.Skip("disposable database container is required; evidence qualification remains open")
	}
	for _, mode := range []string{evidenceRemoteApply, "upgrade_sync"} {
		t.Run(mode, func(t *testing.T) {
			raw := runEvidenceTool(t, "psql_probe.pl", database, mode)
			var receipt evidencePSQLProbeReceipt
			if json.Unmarshal(raw, &receipt) != nil || !receipt.AllEOF || !receipt.BackendStable || receipt.StderrBytes != 0 || receipt.Exit != 0 || receipt.PromptBytes != 0 {
				t.Fatal("synchronous setting experiment did not close cleanly")
			}
			want := "SET\nt\nt\nBEGIN\non\nCOMMIT\nt\n"
			if mode == evidenceRemoteApply {
				want = "SET\nt\nt\nBEGIN\nremote_apply\nCOMMIT\nt\n"
			}
			if receipt.Result != want || !slices.Equal(receipt.States, []string{"", "", "", "", "", "", "*", "*", "", ""}) {
				t.Fatal("primary/settings/fence guard or synchronous_commit preservation failed")
			}
		})
	}
}

func TestNativeEvidenceLiveNativeAdmission(t *testing.T) {
	for _, name := range []string{
		evidenceValid, "u64_max", "unicode_pair", "many_records", "escaped_duplicate", "nested_duplicate", "invalid_utf8", "lone_surrogate", "truncated", "fractional_integer", "exponent_integer", "u64_overflow", "string_integer", "oversized_manifest", "oversized_control", "depth_33", "unknown_key", "wrong_migration", evidenceWrongPVC, "wrong_job", "wrong_pod", "wrong_job_owner", "wrong_pod_owner", evidenceWrongAttempt, "wrong_endpoint", "wrong_source", "wrong_target", "wrong_nonce", "wrong_generation", "wrong_directory", "provisioning_target", "extra_target", "missing_target", "too_many_tables", "too_many_columns", "too_many_types", "pending_manifest", "pending_control", "root_symlink", "hardlink", "replaced_manifest", "root_replaced", "control_replaced", "lock_replaced", "interrupted_same_size", evidenceOwnerBusy, "control_busy", "control_released_before_sql",
	} {
		t.Run(name, func(t *testing.T) {
			raw := runEvidenceTool(t, "admission_probe.pl", "", name, evidenceAdmissionCaller)
			var receipt struct {
				Case            string `json:"case"`
				Accepted        bool   `json:"accepted"`
				Canonical       bool   `json:"canonical"`
				LifetimeHeld    bool   `json:"lifetime_held"`
				ControlReleased bool   `json:"control_released"`
			}
			if json.Unmarshal(raw, &receipt) != nil || receipt.Case != name || !receipt.Canonical {
				t.Fatal("native admission prototype did not return a canonical private receipt")
			}
			positive := name == evidenceValid || name == "u64_max" || name == "unicode_pair" || name == "many_records" || name == "control_released_before_sql"
			if receipt.Accepted != positive {
				t.Fatal("native state or caller identity admission disagreed with the fixture")
			}
			if positive && (!receipt.LifetimeHeld || !receipt.ControlReleased) {
				t.Fatal("native lifetime/control lock order was not preserved")
			}
		})
	}
}

const evidenceAdmissionCaller = `{"work":{"migration_uid":"00112233-4455-6677-8899-aabbccddeeff","run_id":"00112233445566778899aabbccddeeff","workdir_id":"pvc-fixture","checkpoint_generation":1},"helper":{"migration_uid":"00112233-4455-6677-8899-aabbccddeeff","job_controller_uid":"00112233-4455-6677-8899-aabbccddeeff","pod_controller_uid":"job-fixture","job_uid":"job-fixture","pod_uid":"pod-fixture","pvc_uid":"pvc-fixture","attempt":1,"container":"pgcopydb","endpoint_binding":"source-ref/target-ref"},"source":{"system_identifier":123,"database_oid":11,"timeline":1,"major":16},"target":{"system_identifier":456,"database_oid":22,"major":18,"owner_oid":33,"schema_oid":44,"table_oid":55,"marker_table_oid":66,"marker_index_oid":77,"nonce":[2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2]},"end_lsn":32}`

func TestNativeEvidenceLiveNativeRowPredicates(t *testing.T) {
	for _, tc := range []struct {
		name    string
		valid   bool
		durable string
	}{
		{evidenceLaterBegin, true, fixtureEarlierLSN}, {evidenceKeepalive, true, fixtureWitnessLSN}, {"crossing_txn", true, "0/28"}, {"empty_applied", true, fixtureEarlierLSN}, {"uncertified_later_begin", true, fixtureEarlierLSN}, {"counter_max", true, fixtureEarlierLSN},
		{evidenceKeepaliveLowCert, false, ""}, {"later_begin_not_strict", false, ""}, {"end_mismatch", false, ""}, {"commit_after_end", false, ""}, {evidenceCounterOverflow, false, ""}, {"counter_zero", false, ""}, {"epoch_overflow", false, ""}, {"wrong_version", false, ""}, {"wrong_run", false, ""}, {"wrong_nonce", false, ""}, {"wrong_source", false, ""}, {"wrong_db", false, ""}, {"wrong_schema", false, ""}, {"wrong_table", false, ""}, {"missing_column", false, ""}, {"extra_column", false, ""}, {"anchor_partial", false, ""}, {"drain_partial", false, ""}, {evidenceUncompleted, false, ""}, {"clone_generation", false, ""}, {"follow_operator", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := runEvidenceTool(t, "admission_probe.pl", "", "row_"+tc.name, evidenceAdmissionCaller)
			var receipt struct {
				RowValid bool   `json:"row_valid"`
				Durable  string `json:"durable"`
			}
			if json.Unmarshal(raw, &receipt) != nil || receipt.RowValid != tc.valid || receipt.Durable != tc.durable {
				t.Fatal("native row predicate or distinct drain/durable positions disagreed with the fixture")
			}
		})
	}
}

func TestNativeEvidenceLiveFullBoundary(t *testing.T) {
	database := os.Getenv("NATIVE_EVIDENCE_PG_CONTAINER")
	if database == "" {
		t.Skip("new owned database lease is required; native evidence qualification remains open")
	}
	if strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{ index .Config.Labels \"io.rpgmigrate.owner\" }}", database))) != evidenceDatabaseOwner {
		t.Fatal("database is not an owned evidence experiment")
	}
	image := strings.TrimPrefix(strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{.Image}}", database))), "sha256:")
	expectedMajor := map[string]int{evidencePG16ImageSHA: 16, evidencePG18ImageSHA: 18}[image]
	if expectedMajor == 0 {
		t.Fatal("disposable database image does not match the retained approved identity")
	}
	var sourceID [8]byte
	if _, err := rand.Read(sourceID[:]); err != nil {
		t.Fatal("cannot create an isolated source fixture identity")
	}
	sourceDatabase := "m6_source_" + hex.EncodeToString(sourceID[:])
	evidencePodman(t, "exec", "--env=PGOPTIONS=-c synchronous_commit=off", database, "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=on", "-U", "postgres", "-d", "postgres", "-c", "CREATE DATABASE "+sourceDatabase+" TEMPLATE template0")
	t.Setenv("NATIVE_EVIDENCE_SOURCE_DATABASE", sourceDatabase)
	for _, tc := range []struct {
		name     string
		positive bool
		durable  string
	}{
		{evidenceLaterBegin, true, fixtureEarlierLSN}, {evidenceKeepalive, true, fixtureWitnessLSN}, {"large_binding", true, fixtureEarlierLSN}, {evidenceRemoteApply, true, fixtureEarlierLSN}, {"upgrade_sync", true, fixtureEarlierLSN},
		{"missing_relation", false, ""}, {evidenceReplacedRelation, false, ""}, {"unlogged", false, ""}, {"extra_table", false, ""}, {"extra_index", false, ""}, {"dropped_column", false, ""}, {"missing_row", false, ""}, {"multiple_rows", false, ""}, {"wrong_row_nonce", false, ""}, {"wrong_row_generation", false, ""}, {evidenceCounterOverflow, false, ""}, {evidenceKeepaliveLowCert, false, ""}, {evidenceUncompleted, false, ""}, {evidenceMissingManifest, false, ""}, {"predecessor_changed", false, ""}, {evidenceWrongSourceEndpoint, false, ""}, {evidenceWrongTargetEndpoint, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caller := freshEvidenceCaller(t)
			raw := runEvidenceTool(t, "admission_probe.pl", database, "sql_"+tc.name, caller)
			var receipt struct {
				Observed       bool   `json:"observed"`
				ServerMajor    int    `json:"server_major"`
				TokenPreserved bool   `json:"token_preserved"`
				AllEOF         bool   `json:"all_eof"`
				Durable        string `json:"durable"`
				Mocked         bool   `json:"mocked_kubernetes_owner"`
				RefusalStage   string `json:"refusal_stage"`
				RefusalKind    string `json:"refusal_kind"`
				SQLState       string `json:"sqlstate"`
			}
			if json.Unmarshal(raw, &receipt) != nil || !receipt.Mocked || receipt.ServerMajor != expectedMajor || receipt.Observed != tc.positive || receipt.Durable != tc.durable {
				t.Fatalf("native prototype mismatch (case=%s stage=%s class=%s sqlstate=%s major=%d)", tc.name, receipt.RefusalStage, receipt.RefusalKind, receipt.SQLState, receipt.ServerMajor)
			}
			if !tc.positive {
				expectedStage := evidenceCatalogCheckpoint
				switch tc.name {
				case evidenceWrongSourceEndpoint:
					expectedStage = evidenceSourceIdentity
				case evidenceWrongTargetEndpoint:
					expectedStage = evidenceTargetIdentity
				case evidenceMissingManifest:
					expectedStage = evidenceAdmission
				case "missing_relation":
					expectedStage = "lock_checkpoint"
				case "missing_row", "multiple_rows", "wrong_row_nonce", "wrong_row_generation", evidenceCounterOverflow, evidenceKeepaliveLowCert, evidenceUncompleted:
					expectedStage = evidenceRowRead
				case "predecessor_changed":
					expectedStage = "same_value_update"
				}
				if receipt.RefusalStage != expectedStage || receipt.RefusalKind == evidenceTimeout || receipt.RefusalKind == evidenceOutputLimit {
					t.Fatal("negative fixture failed at an unrelated stage or exhausted its limits")
				}
			}
			if tc.positive && (!receipt.AllEOF || !receipt.TokenPreserved || (receipt.ServerMajor != 16 && receipt.ServerMajor != 18)) {
				t.Fatal("positive prototype observation lacked complete closure, token preservation or actual major")
			}
		})
	}
}

func TestNativeEvidenceLiveComposed(t *testing.T) {
	database := os.Getenv("NATIVE_EVIDENCE_PG_CONTAINER")
	if database == "" {
		t.Skip("new owned database lease is required; composed evidence qualification remains open")
	}
	if strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{ index .Config.Labels \"io.rpgmigrate.owner\" }}", database))) != evidenceDatabaseOwner {
		t.Fatal("database is not an owned evidence experiment")
	}
	image := strings.TrimPrefix(strings.TrimSpace(string(evidencePodman(t, "inspect", "--format={{.Image}}", database))), "sha256:")
	major := map[string]int{evidencePG16ImageSHA: 16, evidencePG18ImageSHA: 18}[image]
	if major == 0 {
		t.Fatal("disposable database image does not match the retained approved identity")
	}
	var sourceID [8]byte
	if _, err := rand.Read(sourceID[:]); err != nil {
		t.Fatal("cannot create an isolated source fixture identity")
	}
	sourceDatabase := "m6_source_" + hex.EncodeToString(sourceID[:])
	evidencePodman(t, "exec", "--env=PGOPTIONS=-c synchronous_commit=off", database, "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=on", "-U", "postgres", "-d", "postgres", "-c", "CREATE DATABASE "+sourceDatabase+" TEMPLATE template0")
	t.Setenv("NATIVE_EVIDENCE_SOURCE_DATABASE", sourceDatabase)
	for _, name := range []string{
		evidenceSyncCancel, evidenceLostCommit, evidencePostCommitLoss,
		"lost_commit/wrong_owner", "lost_commit/wrong_pvc", "lost_commit/wrong_attempt", "lost_commit/missing_manifest", "lost_commit/owner_busy", "lost_commit/fence_busy", "lost_commit/replaced_relation", "lost_commit/wrong_source_endpoint", "lost_commit/wrong_target_endpoint", "lost_commit/second_commit_fault",
	} {
		t.Run(name, func(t *testing.T) {
			raw := runEvidenceTool(t, "admission_probe.pl", database, "sql_composed_"+name, freshEvidenceCaller(t))
			if !checkEvidenceComposedReceipt(raw, name, major) {
				t.Fatal("composed fault, fresh private exec, re-admission or complete barrier was not proved")
			}
		})
	}
}

func freshEvidenceCaller(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal("cannot create isolated work identity")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	run := hex.EncodeToString(b[:])
	uid := run[:8] + "-" + run[8:12] + "-" + run[12:16] + "-" + run[16:20] + "-" + run[20:]
	var caller map[string]any
	if json.Unmarshal([]byte(evidenceAdmissionCaller), &caller) != nil {
		t.Fatal("invalid independent caller fixture")
	}
	caller[evidenceWork].(map[string]any)["migration_uid"] = uid
	caller[evidenceWork].(map[string]any)["run_id"] = run
	caller["helper"].(map[string]any)["migration_uid"] = uid
	caller["helper"].(map[string]any)["job_controller_uid"] = uid
	raw, err := json.Marshal(caller)
	if err != nil {
		t.Fatal("cannot encode independent caller fixture")
	}
	return string(raw)
}

func TestNativeEvidenceLivePrivateSessionRefusesNoDatabase(t *testing.T) {
	raw := runEvidenceTool(t, "private_session_probe.pl", "")
	var receipt struct {
		Accepted    bool `json:"accepted"`
		Exit        int  `json:"exit"`
		Signal      int  `json:"signal"`
		StderrBytes int  `json:"stderr_bytes"`
		ResultEOF   bool `json:"result_eof"`
		StderrEOF   bool `json:"stderr_eof"`
	}
	if json.Unmarshal(raw, &receipt) != nil || receipt.Accepted || (receipt.Exit == 0 && receipt.Signal == 0) || receipt.StderrBytes == 0 || !receipt.ResultEOF || !receipt.StderrEOF {
		t.Fatal("new private psql channel did not refuse and drain its failed connection")
	}
}

func TestNativeEvidenceLiveCallerTransport(t *testing.T) {
	encoded := func(value string) string {
		var caller map[string]any
		if json.Unmarshal([]byte(evidenceAdmissionCaller), &caller) != nil {
			t.Fatal("invalid independent caller fixture")
		}
		caller["helper"].(map[string]any)["endpoint_binding"] = value
		raw, err := json.Marshal(caller)
		if err != nil {
			t.Fatal("cannot serialize caller transport fixture")
		}
		return string(raw)
	}
	maximum := encoded(strings.Repeat("<", 512))
	var fields map[string]any
	if json.Unmarshal([]byte(maximum), &fields) != nil {
		t.Fatal("invalid caller transport fixture")
	}
	for _, key := range []string{"job_uid", "pod_uid", "pod_controller_uid"} {
		fields["helper"].(map[string]any)[key] = strings.Repeat("<", 512)
	}
	maximumRaw, err := json.Marshal(fields)
	if err != nil || len(maximumRaw) <= 4096 {
		t.Fatal("synthetic field bounds did not exceed the explicit transport limit")
	}
	for _, tc := range []struct {
		name, raw, decoded string
		accepted           bool
	}{
		{"exact_limit", evidenceAdmissionCaller + strings.Repeat(" ", 4096-len(evidenceAdmissionCaller)), "source-ref/target-ref", true},
		{"one_byte_over", evidenceAdmissionCaller + strings.Repeat(" ", 4097-len(evidenceAdmissionCaller)), "", false},
		{"html_unicode_escaped", encoded("<>&😀"), "<>&😀", true},
		{"max_single_field", maximum, strings.Repeat("<", 512), true},
		{"synthetic_fields_exceed_transport", string(maximumRaw), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := runEvidenceTool(t, "caller_transport_probe.pl", "", tc.raw)
			var receipt struct {
				Accepted       bool   `json:"accepted"`
				Bytes          int    `json:"bytes"`
				Digest         string `json:"digest"`
				PrefixByteUnit bool   `json:"stock_prefix_uses_bytes"`
			}
			if json.Unmarshal(raw, &receipt) != nil || receipt.Accepted != tc.accepted || receipt.Bytes != len(tc.raw) || !receipt.PrefixByteUnit {
				t.Fatalf("private caller transport boundary mismatch (accepted=%t bytes=%d expected_bytes=%d)", receipt.Accepted, receipt.Bytes, len(tc.raw))
			}
			if tc.accepted && receipt.Digest != fmt.Sprintf("%x", sha256.Sum256([]byte(tc.decoded))) {
				t.Fatal("caller transport changed decoded caller bytes")
			}
		})
	}
}

func TestNativeEvidenceLiveFence(t *testing.T) {
	raw := runEvidenceTool(t, "fence_probe.pl", "")
	var got [2][2]int64
	if json.Unmarshal(raw, &got) != nil {
		t.Fatal("stock fence experiment returned malformed numbers")
	}
	for i, run := range []string{"00112233445566778899aabbccddeeff", "000102030405060708090a0b0c0d0e0f"} {
		runBytes, err := hex.DecodeString(run)
		if err != nil {
			t.Fatal("invalid fence fixture")
		}
		sum := crc32.ChecksumIEEE(runBytes)
		if got[i] != [2]int64{int64(sum), int64(int32(sum))} {
			t.Fatal("stock fence differs from CRC32 of the sixteen run bytes")
		}
	}
}
