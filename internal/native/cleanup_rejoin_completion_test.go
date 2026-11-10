package native

import (
	"bytes"
	"encoding/json"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

const rejoinResourcesKey = "resources"
const rejoinDifferentEnd = "0/22"
const rejoinFailedMain = "failed_job"

func newCleanupRejoinFixture(t *testing.T) retryFixture {
	t.Helper()
	f := newCleanedFixture(t)
	w := f.want
	w.Role, w.ProgramSHA256 = JobRoleEvidence, CleanupRejoinCompletionProgramSHA256()
	f.base.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: fixtureEndLSN}
	f.base.worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	f.workerPod.Status.Phase = corev1.PodSucceeded
	f.workerPod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 0
	command, args, env, volumes, mounts, err := CleanupRejoinCompletionMaterialization(f.base.m, w)
	if err != nil {
		t.Fatal(err)
	}
	c := &f.base.job.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
	f.base.job.Spec.Template.Spec.Volumes = volumes
	f.base.pod.Spec = *f.base.job.Spec.Template.Spec.DeepCopy()
	w.TemplateSHA256 = podexec.NativeTemplateSHA256(f.base.job.Spec.Template)
	w.ConnectionReferenceSHA256, err = CleanupRejoinCompletionConnectionReferenceSHA256(f.base.m, w)
	if err != nil {
		t.Fatal(err)
	}
	w.Container.TemplateSHA256 = w.TemplateSHA256
	w.Container.ExecutionSHA256 = podexec.NativeExecutionSHA256(f.base.job.Spec.Template)
	f.want = w
	return f
}

func cleanupRejoinResult(t *testing.T, w ExecutionData, body []byte) []byte {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(readerResult(t, cleanedExpectation(w), body), &v); err != nil {
		t.Fatal(err)
	}
	v[readerKindKey] = cleanupRejoinCompletionKind
	v[retryMaterializationClosedKey], v["cleanup_pending"], v["source_resources_checked"] = true, true, true
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func newCleanupRejoinReaderAPI(t *testing.T) (*readerAPI, *Reader, ExecutionData) {
	t.Helper()
	api, r, _ := newOwnedReaderAPI(t)
	f := newCleanupRejoinFixture(t)
	api.fixture, api.extraPods = f.base, []corev1.Pod{*f.workerPod}
	api.fixture.pod.Labels = map[string]string{batchv1.ControllerUidLabel: string(f.want.JobUID)}
	api.mutateResult = func([]byte) []byte { return cleanupRejoinResult(t, f.want, api.input) }
	return api, r, f.want
}

func TestNativeCleanupRejoinAuthority(t *testing.T) {
	f := newCleanupRejoinFixture(t)
	w := f.want
	body, err := cleanupRejoinRequest(w, fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	raw := cleanupRejoinResult(t, w, body)
	if _, err := parseCleanupRejoinResult(raw, w, body, fixtureEndLSN); err != nil {
		t.Fatal(err)
	}
	if _, err := parseCompletedRequest(body); err == nil {
		t.Fatal("cleaned request admitted by active policy")
	}
	if _, err := parseCompletedResult(raw, cleanedExpectation(w), body, fixtureEndLSN); err == nil {
		t.Fatal("cleaned result admitted by active policy")
	}
	for _, mode := range []string{nativeDrainLaterBegin, nativeDrainKeepalive} {
		var v map[string]any
		_ = json.Unmarshal(raw, &v)
		c := v[ownedFixtureCheckpointKey].(map[string]any)
		c["drain_kind"] = mode
		if mode == nativeDrainLaterBegin {
			c["certified_lsn"], c["durable_replay_lsn"], c["drain_replay_lsn"] = fixtureEarlierLSN, fixtureEarlierLSN, fixtureWitnessLSN
		}
		if mode == nativeDrainKeepalive {
			c["certified_lsn"], c["durable_replay_lsn"], c["drain_replay_lsn"] = fixtureWitnessLSN, fixtureWitnessLSN, fixtureWitnessLSN
		}
		data, _ := json.Marshal(v)
		if _, err := parseCleanupRejoinResult(append(data, '\n'), w, body, fixtureEndLSN); err != nil {
			t.Fatalf("valid %s rejected: %v", mode, err)
		}
	}
	for _, path := range [][]string{nil, {ownedFixtureCheckpointKey}, {evidenceWork}, {rejoinResourcesKey, "0"}} {
		var v map[string]any
		_ = json.Unmarshal(raw, &v)
		node := v
		for _, p := range path {
			if p == rejoinResourcesKey {
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
				if p == rejoinResourcesKey {
					n = bad[p].([]any)[0].(map[string]any)
					break
				}
				n = n[p].(map[string]any)
			}
			delete(n, key)
			data, _ := json.Marshal(bad)
			if _, err := parseCleanupRejoinResult(append(data, '\n'), w, body, fixtureEndLSN); err == nil {
				t.Fatalf("missing %v/%s accepted", path, key)
			}
		}
	}
	for _, key := range []string{preflightSourceClosedKey, preflightTargetClosedKey, retryMaterializationClosedKey, sourcePrivateClosedKey, preflightDiagnosticsKey, "cleanup_pending", "source_resources_checked"} {
		bad := bytes.Replace(raw, []byte(`"`+key+`":true`), []byte(`"`+key+`":false`), 1)
		if _, err := parseCleanupRejoinResult(bad, w, body, fixtureEndLSN); err == nil {
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
			return bytes.Replace(b, []byte(`"drain_end_lsn":"0/20"`), []byte(`"drain_end_lsn":rejoinDifferentEnd`), 1)
		},
		func(b []byte) []byte { return append(bytes.Clone(b), '\n') },
		func(b []byte) []byte { return append(bytes.Clone(b), b...) },
	} {
		if _, err := parseCleanupRejoinResult(edit(raw), w, body, fixtureEndLSN); err == nil {
			t.Fatal("invalid result accepted")
		}
	}
	if _, err := parseCleanupRejoinResult(raw, w, append(bytes.Clone(body), ' '), fixtureEndLSN); err == nil {
		t.Fatal("request byte mutation accepted")
	}
	if _, err := parseCleanupRejoinResult(raw, w, body, rejoinDifferentEnd); err == nil {
		t.Fatal("different E accepted")
	}
	atLimit := append(bytes.Repeat([]byte{' '}, nativeWireLimit-len(raw)), raw...)
	if _, err := parseCleanupRejoinResult(atLimit, w, body, fixtureEndLSN); err != nil {
		t.Fatal("exact wire cap rejected")
	}
	if _, err := parseCleanupRejoinResult(append([]byte{' '}, atLimit...), w, body, fixtureEndLSN); err == nil {
		t.Fatal("oversized cleaned result accepted")
	}
}

func TestNativeCompletedRoutesStayNarrow(t *testing.T) {
	f := newCleanupRejoinFixture(t)
	body, err := cleanupRejoinRequest(f.want, fixtureEndLSN)
	if err != nil {
		t.Fatal(err)
	}
	raw := cleanupRejoinResult(t, f.want, body)
	if _, err := parseCompletedRequest(body); err == nil {
		t.Fatal("partial cleanup request admitted by active route")
	}
	if _, err := parseCleanedResult(raw, f.want, body, fixtureEndLSN); err == nil {
		t.Fatal("partial cleanup result admitted by fully cleaned route")
	}
	if _, err := parseCompletedResult(raw, cleanedExpectation(f.want), body, fixtureEndLSN); err == nil {
		t.Fatal("partial cleanup result admitted by active route")
	}
}
