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

const (
	nativeCloneDirOption = "--dir"
	nativeCloneWorkdir   = "/work/pgcopydb"
)

type nativeCloneOutputReader interface {
	JobCloneNative(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error)
}

func nativeCloneCall(t *testing.T, e *Exec) func(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error) {
	t.Helper()
	r, ok := any(e).(nativeCloneOutputReader)
	if !ok {
		t.Fatal("bounded original clone output reader is missing")
	}
	return r.JobCloneNative
}

func newNativeCloneAPI(t *testing.T) (*nativeAPI, NativeContainerBinding) {
	t.Helper()
	s, want := newNativeOutputAPI(t)
	s.job.Spec.Template.Spec.Containers[0].Args = []string{"clone", "--follow", nativeOutputJSONArg, nativeCloneDirOption, nativeCloneWorkdir}
	s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
	want.ExecutionSHA256 = NativeExecutionSHA256(s.job.Spec.Template)
	return s, want
}

func TestNativeJobCloneOutput(t *testing.T) {
	t.Run("complete data without protocol authority", func(t *testing.T) {
		s, want := newNativeCloneAPI(t)
		s.stdout = []byte("native-report: {\"unparsed_clone_data\":true}\n")
		out, err := nativeCloneCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
		if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) || s.effects != 1 || s.gets != 2 {
			t.Fatalf("capture=%v bytes=%d logs=%d rechecks=%d error=%v", out.Successful(), len(out.Payload()), s.effects, s.gets, err)
		}
		copy := out.Payload()
		copy[0] ^= 1
		if bytes.Equal(copy, out.Payload()) {
			t.Fatal("captured clone data exposed mutable bytes")
		}
		if (NativeOutcome{}).Successful() || (NativeOutcome{}).Payload() != nil {
			t.Fatal("zero outcome granted capture")
		}
	})
	t.Run("original execution admission", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			edit func(*nativeAPI, *NativeContainerBinding)
		}{
			{"missing original pod", func(s *nativeAPI, _ *NativeContainerBinding) { s.pods = []corev1.Pod{} }},
			{"clone hidden original pod duplicate", func(s *nativeAPI, _ *NativeContainerBinding) {
				hidden := *s.pod.DeepCopy()
				hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
				s.pods = []corev1.Pod{s.pod, hidden}
			}},
			{"pod replaced", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.UID = nativeFixtureReplacement }},
			{"job replaced", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.UID = nativeFixtureReplacement }},
			{"wrong owner", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.OwnerReferences[0].UID = "clone-foreign-owner" }},
			{"clone wrong image", func(s *nativeAPI, _ *NativeContainerBinding) {
				s.pod.Status.ContainerStatuses[0].ImageID = nativeFixtureOther
			}},
			{"clone missing execution hash", func(_ *nativeAPI, w *NativeContainerBinding) { w.ExecutionSHA256 = [32]byte{} }},
			{"clone changed argv", func(s *nativeAPI, _ *NativeContainerBinding) {
				s.job.Spec.Template.Spec.Containers[0].Args[0] = "changed-clone-command"
				s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
			}},
			{"clone literal env substitution", func(s *nativeAPI, _ *NativeContainerBinding) {
				s.pod.Spec.Containers[0].Env[0].Value = nativeFixtureOther
			}},
			{"clone restart", func(s *nativeAPI, w *NativeContainerBinding) {
				s.pod.Status.ContainerStatuses[0].RestartCount, w.RestartCount = 1, 1
			}},
			{"clone prior termination", func(s *nativeAPI, _ *NativeContainerBinding) {
				s.pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{}
			}},
			{"clone exit failure", func(s *nativeAPI, _ *NativeContainerBinding) {
				s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
			}},
			{"running original", func(s *nativeAPI, _ *NativeContainerBinding) {
				s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			}},
			{"clone missing job success", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Succeeded = 0 }},
			{"job failure target", func(s *nativeAPI, _ *NativeContainerBinding) {
				s.job.Status.Conditions = append(s.job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue})
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s, want := newNativeCloneAPI(t)
				tc.edit(s, &want)
				e := newExec(t, s.URL)
				e.timeout = 15 * time.Millisecond
				out, err := nativeCloneCall(t, e)(context.Background(), "ns", nativeFixtureJobUID, want)
				nativeRefused(t, out, err)
				if s.effects != 0 {
					t.Fatal("unadmitted original reached logs")
				}
			})
		}
	})
	t.Run("complete bounded log capture", func(t *testing.T) {
		for _, tc := range []struct {
			name                                      string
			size                                      int
			readError, closeError, cancelEOF, refused bool
		}{
			{name: "full bounded data", size: 65536}, {name: "empty data", size: 0}, {name: "clone overflow", size: 65537, refused: true},
			{name: "clone read failure", size: 32, readError: true, refused: true}, {name: "close failure", size: 32, closeError: true, refused: true}, {name: "clone cancel at EOF", size: 32, cancelEOF: true, refused: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s, want := newNativeCloneAPI(t)
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
				out, err := nativeCloneCall(t, e)(ctx, "ns", nativeFixtureJobUID, want)
				if tc.refused {
					nativeRefused(t, out, err)
				} else if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) || !body.eof {
					t.Fatalf("capture=%v bytes=%d EOF=%v error=%v", out.Successful(), len(out.Payload()), body.eof, err)
				}
				if !body.closed || body.bytesRead > 65537 {
					t.Fatalf("closed=%v bytes=%d", body.closed, body.bytesRead)
				}
			})
		}
	})
	t.Run("final original recheck", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			edit func(*nativeAPI)
		}{
			{"clone pod replacement", func(s *nativeAPI) { s.pod.UID = nativeFixtureReplacement }}, {"clone job replacement", func(s *nativeAPI) { s.job.UID = nativeFixtureReplacement }},
			{"clone job status", func(s *nativeAPI) { s.job.Status.Active = 1 }}, {"clone pod status", func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
			{"both env values replaced", func(s *nativeAPI) {
				s.pod.Spec.Containers[0].Env[0].Value = nativeFixtureOther
				s.job.Spec.Template.Spec.Containers[0].Env[0].Value = nativeFixtureOther
			}},
			{"late hidden pod", func(s *nativeAPI) {
				hidden := *s.pod.DeepCopy()
				hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
				s.pods = []corev1.Pod{s.pod, hidden}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s, want := newNativeCloneAPI(t)
				s.stdout, s.after = []byte("native-report: {}\n"), tc.edit
				out, err := nativeCloneCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
				nativeRefused(t, out, err)
				if s.effects != 1 {
					t.Fatal("late mutation was not exercised")
				}
			})
		}
	})
}
