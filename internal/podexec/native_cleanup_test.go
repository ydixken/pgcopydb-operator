package podexec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

const nativeCleanupStreamArg = "stream"

type nativeCleanupOutputReader interface {
	JobCleanupNative(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error)
}

func nativeCleanupCall(t *testing.T, e *Exec) func(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error) {
	t.Helper()
	r, ok := any(e).(nativeCleanupOutputReader)
	if !ok {
		t.Fatal("bounded original cleanup output reader is missing")
	}
	return r.JobCleanupNative
}

func newNativeCleanupAPI(t *testing.T) (*nativeAPI, NativeContainerBinding) {
	t.Helper()
	s, want := newNativeOutputAPI(t)
	s.job.Spec.Template.Spec.Containers[0].Args = []string{nativeCleanupStreamArg, "cleanup", nativeOutputJSONArg, "--dir", "/work/pgcopydb"}
	s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
	want.ExecutionSHA256 = NativeExecutionSHA256(s.job.Spec.Template)
	return s, want
}

func nativeCleanupCapturedData(t *testing.T) {
	s, want := newNativeCleanupAPI(t)
	s.stdout = []byte("native-report: {\"unparsed_ack_data\":true}\n")
	out, err := nativeCleanupCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
	if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) || s.effects != 1 || s.gets != 2 {
		t.Fatalf("capture=%v payload bytes=%d log reads=%d pod reads=%d error=%v", out.Successful(), len(out.Payload()), s.effects, s.gets, err)
	}
	copy := out.Payload()
	copy[0] ^= 1
	if bytes.Equal(copy, out.Payload()) {
		t.Fatal("captured report exposed mutable bytes")
	}
	if (NativeOutcome{}).Payload() != nil || (NativeOutcome{}).Successful() {
		t.Fatal("zero output established capture")
	}
}

func nativeCleanupOutputAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*nativeAPI, *NativeContainerBinding)
	}{
		{"no original pod", func(s *nativeAPI, _ *NativeContainerBinding) { s.pods = []corev1.Pod{} }},
		{"hidden original pod duplicate", func(s *nativeAPI, _ *NativeContainerBinding) {
			hidden := *s.pod.DeepCopy()
			hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
			s.pods = []corev1.Pod{s.pod, hidden}
		}},
		{"cleanup pod replaced", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.UID = nativeFixtureReplacement }},
		{"cleanup job replaced", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.UID = nativeFixtureReplacement }},
		{"cleanup foreign owner", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.OwnerReferences[0].UID = "cleanup-foreign-owner" }},
		{"wrong cleanup image", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].ImageID = nativeFixtureOther
		}},
		{"missing execution hash", func(_ *nativeAPI, w *NativeContainerBinding) { w.ExecutionSHA256 = [32]byte{} }},
		{"changed argv", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Spec.Template.Spec.Containers[0].Args[1] = "start"
			s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
		}},
		{"literal env substitution", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Containers[0].Env[0].Value = nativeFixtureOther
		}},
		{"cleanup restart", func(s *nativeAPI, w *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].RestartCount, w.RestartCount = 1, 1
		}},
		{"prior termination", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{}
		}},
		{"exit failure", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
		}},
		{"running process", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{"missing job success", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Succeeded = 0 }},
		{"cleanup failure target", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Status.Conditions = append(s.job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeCleanupAPI(t)
			tc.edit(s, &want)
			e := newExec(t, s.URL)
			e.timeout = 15 * time.Millisecond
			out, err := nativeCleanupCall(t, e)(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 0 {
				t.Fatal("unadmitted cleanup execution reached logs")
			}
		})
	}
}

func nativeCleanupOutputClosure(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		size                                      int
		readError, closeError, cancelEOF, refused bool
	}{
		{name: "full bounded output", size: 65536},
		{name: "empty data is no protocol report", size: 0},
		{name: "cleanup overflow", size: 65537, refused: true},
		{name: "read error", size: 32, readError: true, refused: true},
		{name: "close error", size: 32, closeError: true, refused: true},
		{name: "cancel at EOF", size: 32, cancelEOF: true, refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeCleanupAPI(t)
			s.stdout = bytes.Repeat([]byte{'a'}, tc.size)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &nativeLogBody{}
			if tc.readError {
				body.readErr = errors.New("private read canary")
			}
			if tc.closeError {
				body.closeErr = errors.New("private close canary")
			}
			if tc.cancelEOF {
				body.onEOF = cancel
			}
			e, err := New(&rest.Config{Host: s.URL, WrapTransport: func(next http.RoundTripper) http.RoundTripper {
				return nativeLogRoundTripper{next: next, wrap: func(original io.ReadCloser) io.ReadCloser { body.ReadCloser = original; return body }}
			}})
			if err != nil {
				t.Fatal(err)
			}
			out, err := nativeCleanupCall(t, e)(ctx, "ns", nativeFixtureJobUID, want)
			if tc.refused {
				nativeRefused(t, out, err)
			} else if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) || !body.eof {
				t.Fatalf("capture=%v payload bytes=%d EOF=%v error=%v", out.Successful(), len(out.Payload()), body.eof, err)
			}
			if !body.closed || body.bytesRead > 65537 {
				t.Fatalf("closed=%v bytes=%d", body.closed, body.bytesRead)
			}
		})
	}
}

func nativeCleanupOutputFinalRecheck(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*nativeAPI)
	}{
		{"pod replacement", func(s *nativeAPI) { s.pod.UID = nativeFixtureReplacement }},
		{"cleanup job replacement", func(s *nativeAPI) { s.job.UID = nativeFixtureReplacement }},
		{"cleanup job status", func(s *nativeAPI) { s.job.Status.Active = 1 }},
		{"cleanup pod status", func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"env replaced on both objects", func(s *nativeAPI) {
			s.pod.Spec.Containers[0].Env[0].Value = nativeFixtureOther
			s.job.Spec.Template.Spec.Containers[0].Env[0].Value = nativeFixtureOther
		}},
		{"hidden late pod", func(s *nativeAPI) {
			hidden := *s.pod.DeepCopy()
			hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
			s.pods = []corev1.Pod{s.pod, hidden}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeCleanupAPI(t)
			s.stdout, s.after = []byte("native-report: {}\n"), tc.edit
			out, err := nativeCleanupCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 1 {
				t.Fatal("final identity mutation was not exercised")
			}
		})
	}
}

func TestNativeCleanupOutput(t *testing.T) {
	t.Run("data", nativeCleanupCapturedData)
	t.Run("admission", nativeCleanupOutputAdmission)
	t.Run("closure", nativeCleanupOutputClosure)
	t.Run("final_recheck", nativeCleanupOutputFinalRecheck)
}
