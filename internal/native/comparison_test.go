package native

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const comparisonContainerID = "containerd://original"

func comparisonFixture(t *testing.T, check string, matched bool) ([]byte, WorkIdentity, string, [32]byte) {
	t.Helper()
	w := WorkIdentity{MigrationUID: "01234567-89ab-cdef-0123-456789abcdef", RunID: "0123456789abcdef0123456789abcdef", WorkdirID: "pvc-owned", CheckpointGeneration: 1}
	op, hash := strings.Repeat("a", 32), ComparisonProgramSHA256()
	exit, count, size, rows, differing, elapsed := 3, 1, 8192, 7, 1, 42
	if matched {
		exit, differing = 0, 0
	}
	if check == comparisonCheckSchema {
		count, size, rows, differing, elapsed = 0, 0, 0, 0, 0
	}
	raw, err := json.Marshal(map[string]any{fixtureVersionKind: 1, readerKindKey: "native_comparison", readerOperationIDKey: op, "program_sha256": hex.EncodeToString(hash[:]), "work": w, "check": check, "matched": matched, "engine_exit": exit, "report_bytes": 19, "table_count": count, "planned_bytes": size, "compared_rows": rows, "differing_tables": differing, "elapsed_ms": elapsed, "child_closed": true, "report_closed": true, "private_closed": true, "diagnostics_clean": true})
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n'), w, op, hash
}

func TestNativeComparisonCheckedCapture(t *testing.T) {
	raw, work, op, hash := comparisonFixture(t, comparisonCheckData, false)
	controller := true
	job := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "comparison", Namespace: "ns", UID: "original-job"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: nativeContainerName, Image: "image@sha256:bound"}}}}}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "original-pod", Namespace: "ns", UID: "original-pod", Labels: map[string]string{batchv1.ControllerUidLabel: string(job.UID)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: sourceJobKind, Name: job.Name, UID: job.UID, Controller: &controller}}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: nativeContainerName, ImageID: "image@sha256:platform", ContainerID: comparisonContainerID, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: comparisonContainerID}}}}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/log"):
			_, _ = w.Write(raw)
		case strings.HasSuffix(r.URL.Path, "/pods"):
			_ = json.NewEncoder(w).Encode(corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "snapshot"}, Items: []corev1.Pod{pod}})
		case strings.HasSuffix(r.URL.Path, "/pods/"+pod.Name):
			_ = json.NewEncoder(w).Encode(pod)
		case strings.HasSuffix(r.URL.Path, "/jobs/"+job.Name):
			_ = json.NewEncoder(w).Encode(job)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	transport, err := podexec.New(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	want := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: nativeContainerName, ContainerID: comparisonContainerID, ImageID: "image@sha256:platform", TemplateSHA256: podexec.NativeTemplateSHA256(job.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(job.Spec.Template)}
	output, err := transport.JobComparisonNative(context.Background(), "ns", job.UID, want)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := ParseComparisonOutput(output, work, op, comparisonCheckData, hash)
	matched, known := outcome.Result()
	if err != nil || matched || !known {
		t.Fatal("checked mismatch did not remain distinct")
	}
	for _, mutate := range []func(){func() { hash[0] ^= 1 }, func() { op = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }, func() { work.WorkdirID = "replacement" }} {
		mutate()
		outcome, err = ParseComparisonOutput(output, work, op, comparisonCheckData, hash)
		matched, known = outcome.Result()
		if err != ErrNativeComparisonUnavailable || matched || known {
			t.Fatal("changed expectation admitted capture")
		}
	}
}

func TestNativeComparisonProgressFollowsTheCheck(t *testing.T) {
	matched, work, op, hash := comparisonFixture(t, comparisonCheckData, true)
	schema, _, _, _ := comparisonFixture(t, comparisonCheckSchema, true)
	for name, payload := range map[string][]byte{
		"matched data with a differing table": bytes.Replace(matched, []byte(`"differing_tables":0`), []byte(`"differing_tables":1`), 1),
		"schema with bytes":                   bytes.Replace(schema, []byte(`"planned_bytes":0`), []byte(`"planned_bytes":1`), 1),
		"schema with rows":                    bytes.Replace(schema, []byte(`"compared_rows":0`), []byte(`"compared_rows":1`), 1),
		"schema with elapsed":                 bytes.Replace(schema, []byte(`"elapsed_ms":0`), []byte(`"elapsed_ms":1`), 1),
	} {
		check := comparisonCheckData
		if strings.HasPrefix(name, "schema") {
			check = comparisonCheckSchema
		}
		out, err := parseComparisonPayload(payload, work, op, check, hash)
		if _, known := out.Result(); err != ErrNativeComparisonUnavailable || known {
			t.Errorf("%s produced an outcome", name)
		}
		if _, ok := out.Progress(); ok {
			t.Errorf("%s produced progress", name)
		}
	}
}

func TestNativeComparisonOutcome(t *testing.T) {
	matched, known := (ComparisonOutcome{}).Result()
	if matched || known {
		t.Fatal("zero outcome is known")
	}
	raw, work, op, hash := comparisonFixture(t, comparisonCheckData, false)
	if _, err := ParseComparisonOutput(podexec.ComparisonOutput{}, work, op, comparisonCheckData, hash); err != ErrNativeComparisonUnavailable {
		t.Fatal("unsealed bytes accepted")
	}
	for _, check := range []string{comparisonCheckSchema, comparisonCheckData} {
		for _, match := range []bool{false, true} {
			body, w, id, h := comparisonFixture(t, check, match)
			out, err := parseComparisonPayload(body, w, id, check, h)
			got, known := out.Result()
			if err != nil || !known || got != match {
				t.Fatalf("check=%s match=%v known=%v error=%v", check, got, known, err)
			}
			progress, ok := out.Progress()
			want := ComparisonProgress{Tables: 1, Bytes: 8192, Rows: 7, DifferingTables: 1, ElapsedMs: 42}
			if match {
				want.DifferingTables = 0
			}
			if check == comparisonCheckSchema {
				want = ComparisonProgress{}
			}
			if ok != (check == comparisonCheckData) || progress != want {
				t.Fatalf("check=%s match=%v progress=%+v ok=%v", check, match, progress, ok)
			}
		}
	}
	for name, edit := range map[string]func([]byte) []byte{
		"wrong exit": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"engine_exit":3`), []byte(`"engine_exit":0`), 1)
		},
		"empty data": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"table_count":1`), []byte(`"table_count":0`), 1)
		},
		readerCaseOverflow: func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"report_bytes":19`), []byte(`"report_bytes":62914561`), 1)
		},
		"unclosed": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"child_closed":true`), []byte(`"child_closed":false`), 1)
		},
		"missing LF":       func(b []byte) []byte { return b[:len(b)-1] },
		sourceTrailingCase: func(b []byte) []byte { return append(b, ' ') },
		"duplicate":        func(b []byte) []byte { return append([]byte(`{"version":1,`), b[1:]...) },
		readerCaseUnknown:  func(b []byte) []byte { return append([]byte(`{"secret":"canary",`), b[1:]...) },
		"noncanonical":     func(b []byte) []byte { return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1.0`), 1) },
		"no differing count": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"differing_tables":1`), []byte(`"differing_tables":0`), 1)
		},
		"more differing than compared": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"differing_tables":1`), []byte(`"differing_tables":2`), 1)
		},
		"negative rows": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"compared_rows":7`), []byte(`"compared_rows":-7`), 1)
		},
		"negative bytes": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"planned_bytes":8192`), []byte(`"planned_bytes":-1`), 1)
		},
		"negative elapsed": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"elapsed_ms":42`), []byte(`"elapsed_ms":-1`), 1)
		},
		"missing progress": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"elapsed_ms":42,`), nil, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := parseComparisonPayload(edit(bytes.Clone(raw)), work, op, comparisonCheckData, hash)
			a, b := out.Result()
			if err != ErrNativeComparisonUnavailable || a || b {
				t.Fatal("malformed classification produced outcome")
			}
		})
	}
}
