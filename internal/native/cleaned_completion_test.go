package native

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const cleanedResourcesKey = "resources"
const cleanedDifferentEnd = "0/22"
const cleanedFailedMain = "failed_job"

func newCleanedFixture(t *testing.T) retryFixture {
	t.Helper()
	f := newOwnedFixture(t)
	w := f.want
	w.Role, w.ProgramSHA256 = JobRoleEvidence, CleanedCompletionProgramSHA256()
	f.base.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: fixtureEndLSN}
	f.base.worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	f.workerPod.Status.Phase = corev1.PodSucceeded
	f.workerPod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 0
	command, args, env, volumes, mounts, err := CleanedCompletionMaterialization(f.base.m, w)
	if err != nil {
		t.Fatal(err)
	}
	c := &f.base.job.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
	f.base.job.Spec.Template.Spec.Volumes = volumes
	f.base.pod.Spec = *f.base.job.Spec.Template.Spec.DeepCopy()
	w.TemplateSHA256 = podexec.NativeTemplateSHA256(f.base.job.Spec.Template)
	w.ConnectionReferenceSHA256, err = CleanedCompletionConnectionReferenceSHA256(f.base.m, w)
	if err != nil {
		t.Fatal(err)
	}
	w.Container.TemplateSHA256 = w.TemplateSHA256
	w.Container.ExecutionSHA256 = podexec.NativeExecutionSHA256(f.base.job.Spec.Template)
	f.want = w
	return f
}

func cleanedResult(t *testing.T, w ExecutionData, body []byte) []byte {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(readerResult(t, cleanedExpectation(w), body), &v); err != nil {
		t.Fatal(err)
	}
	v[readerKindKey] = cleanedCompletionKind
	v[retryMaterializationClosedKey], v["cleanup_complete"], v["source_resources_absent"] = true, true, true
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func newCleanedReaderAPI(t *testing.T) (*readerAPI, *Reader, ExecutionData) {
	t.Helper()
	api, r, _ := newOwnedReaderAPI(t)
	f := newCleanedFixture(t)
	api.fixture, api.extraPods = f.base, []corev1.Pod{*f.workerPod}
	api.fixture.pod.Labels = map[string]string{batchv1.ControllerUidLabel: string(f.want.JobUID)}
	api.mutateResult = func([]byte) []byte { return cleanedResult(t, f.want, api.input) }
	return api, r, f.want
}

func TestNativeCleanedCompletionAuthority(t *testing.T) {
	f := newCleanedFixture(t)
	w := f.want
	body, err := cleanedRequest(w, fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	raw := cleanedResult(t, w, body)
	if _, err := parseCleanedResult(raw, w, body, fixtureEndLSN); err != nil {
		t.Fatal(err)
	}
	if _, err := parseCompletedRequest(body); err == nil {
		t.Fatal("cleaned request admitted by active policy")
	}
	if _, err := parseCompletedResult(raw, cleanedExpectation(w), body, fixtureEndLSN); err == nil {
		t.Fatal("cleaned result admitted by active policy")
	}
	for _, mode := range []string{"later_begin", "keepalive"} {
		var v map[string]any
		_ = json.Unmarshal(raw, &v)
		c := v[ownedFixtureCheckpointKey].(map[string]any)
		c["drain_kind"] = mode
		if mode == "later_begin" {
			c["certified_lsn"], c["durable_replay_lsn"], c["drain_replay_lsn"] = "0/10", "0/10", fixtureWitnessLSN
		}
		if mode == "keepalive" {
			c["certified_lsn"], c["durable_replay_lsn"], c["drain_replay_lsn"] = fixtureWitnessLSN, fixtureWitnessLSN, fixtureWitnessLSN
		}
		data, _ := json.Marshal(v)
		if _, err := parseCleanedResult(append(data, '\n'), w, body, fixtureEndLSN); err != nil {
			t.Fatalf("valid %s rejected: %v", mode, err)
		}
	}
	for _, path := range [][]string{nil, {ownedFixtureCheckpointKey}, {evidenceWork}, {cleanedResourcesKey, "0"}} {
		var v map[string]any
		_ = json.Unmarshal(raw, &v)
		node := v
		for _, p := range path {
			if p == cleanedResourcesKey {
				node = v[p].([]any)[0].(map[string]any)
				break
			}
			node = node[p].(map[string]any)
		}
		for key := range node {
			var bad map[string]any
			_ = json.Unmarshal(raw, &bad)
			n := bad
			for _, p := range path {
				if p == cleanedResourcesKey {
					n = bad[p].([]any)[0].(map[string]any)
					break
				}
				n = n[p].(map[string]any)
			}
			delete(n, key)
			data, _ := json.Marshal(bad)
			if _, err := parseCleanedResult(append(data, '\n'), w, body, fixtureEndLSN); err == nil {
				t.Fatalf("missing %v/%s accepted", path, key)
			}
		}
	}
	for _, key := range []string{preflightSourceClosedKey, preflightTargetClosedKey, retryMaterializationClosedKey, sourcePrivateClosedKey, preflightDiagnosticsKey, "cleanup_complete", "source_resources_absent"} {
		bad := bytes.Replace(raw, []byte(`"`+key+`":true`), []byte(`"`+key+`":false`), 1)
		if _, err := parseCleanedResult(bad, w, body, fixtureEndLSN); err == nil {
			t.Fatalf("false %s accepted", key)
		}
	}
	for _, edit := range []func([]byte) []byte{
		func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
		},
		func(b []byte) []byte { return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":-0`), 1) },
		func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"barrier_confirmed":true`), []byte(`"barrier_confirmed":false`), 1)
		},
		func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"drain_end_lsn":"0/20"`), []byte(`"drain_end_lsn":cleanedDifferentEnd`), 1)
		},
		func(b []byte) []byte { return append(bytes.Clone(b), '\n') },
		func(b []byte) []byte { return append(bytes.Clone(b), b...) },
	} {
		if _, err := parseCleanedResult(edit(raw), w, body, fixtureEndLSN); err == nil {
			t.Fatal("invalid result accepted")
		}
	}
	if _, err := parseCleanedResult(raw, w, append(bytes.Clone(body), ' '), fixtureEndLSN); err == nil {
		t.Fatal("request byte mutation accepted")
	}
	if _, err := parseCleanedResult(raw, w, body, cleanedDifferentEnd); err == nil {
		t.Fatal("different E accepted")
	}
	atLimit := append(bytes.Repeat([]byte{' '}, nativeWireLimit-len(raw)), raw...)
	if _, err := parseCleanedResult(atLimit, w, body, fixtureEndLSN); err != nil {
		t.Fatal("exact wire cap rejected")
	}
	if _, err := parseCleanedResult(append([]byte{' '}, atLimit...), w, body, fixtureEndLSN); err == nil {
		t.Fatal("oversized cleaned result accepted")
	}
}

func TestNativeCleanedCompletionAdmission(t *testing.T) {
	for _, completedDuringExec := range []bool{false, true} {
		api, r, w := newCleanedReaderAPI(t)
		api.finishDuringExec = completedDuringExec
		got, err := r.ReadCleanedCompleted(context.Background(), w, fixtureEndLSN)
		if err != nil || got == nil || got.Checkpoint().Evidence().DrainEndLSN != fixtureEndLSN || got.OwnedState().Work() != w.Work {
			t.Fatalf("fresh cleaned authority refused: %v", err)
		}
		if api.writerCalls != 1 || api.logCalls != 1 || api.splitLogCalls != 0 {
			t.Fatal("checked relay or quiet original main missing")
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*retryFixture)
	}{
		{"worker_receipt_missing", func(f *retryFixture) { f.base.m.Status.NativeJobUID = "" }},
		{"pvc_receipt_missing", func(f *retryFixture) { f.base.m.Status.NativePVCUID = "" }},
		{"worker_receipt_foreign", func(f *retryFixture) { f.base.m.Status.NativeJobUID = readerReplacementUID }},
		{"pvc_receipt_foreign", func(f *retryFixture) { f.base.m.Status.NativePVCUID = readerReplacementUID }},
		{"suspended", func(f *retryFixture) { f.base.m.Spec.Suspend = true }},
		{"deleting", func(f *retryFixture) { now := metav1.Now(); f.base.m.DeletionTimestamp = &now }},
		{"wrong_end", func(f *retryFixture) { f.base.m.Status.Replication.Endpos = cleanedDifferentEnd }},
		{"failed_original", func(f *retryFixture) { f.base.worker.Status.Succeeded = 0; f.base.worker.Status.Failed = 1 }},
		{"pod_restarted", func(f *retryFixture) { f.workerPod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"helper_role", func(f *retryFixture) { f.want.Role = JobRoleCleanup }},
		{"ordinary_program", func(f *retryFixture) { f.want.ProgramSHA256 = EvidenceProgramSHA256() }},
		{"helper_spec", func(f *retryFixture) { f.base.job.Spec.Template.Spec.Containers[0].Args[0] = "cleaned-foreign-spec" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, r, _ := newCleanedReaderAPI(t)
			f := newCleanedFixture(t)
			tc.edit(&f)
			api.fixture, api.extraPods = f.base, []corev1.Pod{*f.workerPod}
			w := f.want
			got, err := r.ReadCleanedCompleted(context.Background(), w, fixtureEndLSN)
			if err == nil || got != nil || api.writerCalls != 0 || api.logCalls != 0 {
				t.Fatal("inadmissible original execution reached transport")
			}
		})
	}
}

func TestNativeCleanedCompletionCrashRecovery(t *testing.T) {
	for _, replacement := range []string{"worker_uid", "cleaned_pod_replaced", "cleaned_pvc_replaced", "receipt", "cleaned_end_changed", "spec", "suspend"} {
		t.Run(replacement, func(t *testing.T) {
			api, r, w := newCleanedReaderAPI(t)
			api.afterResult = func(f *admissionFixture) {
				switch replacement {
				case "worker_uid":
					f.worker.UID = readerReplacementUID
				case "cleaned_pod_replaced":
					api.extraPods[0].UID = readerReplacementUID
				case "cleaned_pvc_replaced":
					f.pvc.UID = readerReplacementUID
				case "receipt":
					f.m.Status.NativeJobUID = readerReplacementUID
				case "cleaned_end_changed":
					f.m.Status.Replication.Endpos = cleanedDifferentEnd
				case "spec":
					api.extraPods[0].Spec.Containers[0].Args = []string{"cleaned-foreign-spec"}
				case "suspend":
					f.m.Spec.Suspend = true
				}
			}
			if got, err := r.ReadCleanedCompleted(context.Background(), w, fixtureEndLSN); err == nil || got != nil {
				t.Fatal("post-relay replacement manufactured completion")
			}
		})
	}
	for _, fault := range []string{cleanedFailedMain, evidenceExit, readerFaultSignal, "noise", "relay"} {
		t.Run(fault, func(t *testing.T) {
			api, r, w := newCleanedReaderAPI(t)
			switch fault {
			case "noise":
				api.logNoise = []byte("noise\n")
			case "relay":
				api.writerFault = sourceTrailingCase
			default:
				api.mainFault = fault
			}
			if got, err := r.ReadCleanedCompleted(context.Background(), w, fixtureEndLSN); err == nil || got != nil {
				t.Fatal("unknown transport/main closure became completion")
			}
		})
	}
	api, r, w := newCleanedReaderAPI(t)
	api.fixture.m.Status.Phase = v1beta1.PhaseCuttingOver
	api.fixture.m.Status.Conditions = nil
	if got, err := r.ReadCleanedCompleted(context.Background(), w, fixtureEndLSN); err != nil || got == nil {
		t.Fatalf("same-E retained crash rejoin depended on old success status: %v", err)
	}
	if strings.Contains(string(api.input), "nativeJobUID") {
		t.Fatal("status receipts became proof hints")
	}
}
