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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

type nativeFailureOutputReader interface {
	JobFailureNative(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error)
}

func nativeFailureCall(t *testing.T, e *Exec) func(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error) {
	t.Helper()
	r, ok := any(e).(nativeFailureOutputReader)
	if !ok {
		t.Fatal("bounded original failed-output reader is missing")
	}
	return r.JobFailureNative
}

func newNativeFailureAPI(t *testing.T) (*nativeAPI, NativeContainerBinding) {
	t.Helper()
	s, want := newNativeCloneAPI(t)
	s.job.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "work", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "retained-work"}}}}
	s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
	want.ExecutionSHA256 = NativeExecutionSHA256(s.job.Spec.Template)
	s.pod.Status.Phase = corev1.PodFailed
	s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
	s.pod.Status.ContainerStatuses[0].State.Terminated.Reason = "Error"
	s.job.Status.Succeeded, s.job.Status.Failed = 0, 1
	s.job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
	}
	return s, want
}

func TestNativeJobFailureOutput(t *testing.T) {
	s, want := newNativeFailureAPI(t)
	s.stdout = []byte("synthetic failed worker output\n")
	impostor := *s.pod.DeepCopy()
	impostor.Name, impostor.UID = "impostor", "impostor-uid"
	impostor.OwnerReferences[0].UID = "foreign-job"
	impostor.CreationTimestamp = metav1.Now()
	s.pods = []corev1.Pod{impostor, s.pod}
	e, err := New(&rest.Config{Host: s.URL, WrapTransport: func(next http.RoundTripper) http.RoundTripper {
		return nativeLogRoundTripper{next: next, after: func(req *http.Request) {
			if req.URL.Path != "/api/v1/namespaces/ns/pods/helper-pod/log" && req.URL.Query().Get("container") != "" {
				t.Error("capture selected a different Pod log URL")
			}
		}}
	}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := nativeFailureCall(t, e)(context.Background(), "ns", nativeFixtureJobUID, want)
	if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) || s.effects != 1 || s.gets != 2 {
		t.Fatalf("capture=%v bytes=%d logs=%d rechecks=%d error=%v", out.Successful(), len(out.Payload()), s.effects, s.gets, err)
	}
	copy := out.Payload()
	copy[0] ^= 1
	if bytes.Equal(copy, out.Payload()) {
		t.Fatal("capture exposed mutable output")
	}
}

func TestNativeJobFailureAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*nativeAPI, *NativeContainerBinding)
	}{
		{"failed worker missing pod", func(s *nativeAPI, _ *NativeContainerBinding) { s.pods = []corev1.Pod{} }},
		{"label without ownership", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.OwnerReferences[0].UID = "foreign-job" }},
		{"failed worker pod replaced", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.UID = nativeFixtureReplacement }},
		{"failed worker job replaced", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.UID = nativeFixtureReplacement }},
		{"pod deleting", func(s *nativeAPI, _ *NativeContainerBinding) { now := metav1.Now(); s.pod.DeletionTimestamp = &now }},
		{"job deleting", func(s *nativeAPI, _ *NativeContainerBinding) { now := metav1.Now(); s.job.DeletionTimestamp = &now }},
		{"hidden owned duplicate", func(s *nativeAPI, _ *NativeContainerBinding) {
			hidden := *s.pod.DeepCopy()
			hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
			s.pods = []corev1.Pod{s.pod, hidden}
		}},
		{"missing execution binding", func(_ *nativeAPI, w *NativeContainerBinding) { w.ExecutionSHA256 = [32]byte{} }},
		{"failed worker wrong container", func(_ *nativeAPI, w *NativeContainerBinding) { w.ContainerName = nativeFixtureOther }},
		{"failed worker wrong image", func(_ *nativeAPI, w *NativeContainerBinding) { w.ImageID = nativeFixtureOther }},
		{"wrong container id", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.ContainerID = nativeFixtureOther
		}},
		{"changed execution", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Spec.Template.Spec.Containers[0].Args[0] = nativeFixtureOther
			s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
		}},
		{"env substitution", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Containers[0].Env[0].Value = nativeFixtureOther
		}},
		{"PVC claim substitution", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = nativeFixtureOther
		}},
		{"injected volume", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Volumes = append(s.pod.Spec.Volumes, corev1.Volume{Name: "injected", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		}},
		{"failed worker restart", func(s *nativeAPI, w *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].RestartCount, w.RestartCount = 1, 1
		}},
		{"failed worker prior termination", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{}
		}},
		{"restart policy", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		}},
		{"zero exit", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 0
		}},
		{"invalid exit", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = -1
		}},
		{"signaled exit", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.Signal = 9
		}},
		{"OOM exit", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.Reason = "OOMKilled"
		}},
		{"unknown reason", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.Reason = ""
		}},
		{"completed reason", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.Reason = "Completed"
		}},
		{"running container", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{"unknown pod phase", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Status.Phase = corev1.PodUnknown }},
		{"active job", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Active = 1 }},
		{"job not failed", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Failed = 0 }},
		{"multiple failures", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Failed = 2 }},
		{"success count", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Succeeded = 1 }},
		{"missing terminal condition", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Conditions = s.job.Status.Conditions[:1] }},
		{"unknown terminal condition", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Status.Conditions[1].Status = corev1.ConditionUnknown
		}},
		{"duplicate terminal condition", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Status.Conditions = append(s.job.Status.Conditions, s.job.Status.Conditions[1])
		}},
		{"contradictory complete", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Status.Conditions = append(s.job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		}},
		{"contradictory success target", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Status.Conditions = append(s.job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue})
		}},
		{"terminating job pod", func(s *nativeAPI, _ *NativeContainerBinding) { count := int32(1); s.job.Status.Terminating = &count }},
		{"ready job pod", func(s *nativeAPI, _ *NativeContainerBinding) { count := int32(1); s.job.Status.Ready = &count }},
		{"uncounted pod", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Status.UncountedTerminatedPods = &batchv1.UncountedTerminatedPods{Failed: []types.UID{"other-pod"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeFailureAPI(t)
			tc.edit(s, &want)
			out, err := nativeFailureCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 0 {
				t.Fatal("unadmitted original reached logs")
			}
		})
	}
}

func TestNativeJobFailureStream(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		size                                      int
		readError, closeError, cancelEOF, refused bool
	}{
		{name: "bounded EOF", size: 65536}, {name: "empty EOF"},
		{name: "overflow without EOF", size: 65537, refused: true},
		{name: "failed worker read error", size: 32, readError: true, refused: true},
		{name: "failed worker close error", size: 32, closeError: true, refused: true},
		{name: "failed worker cancel at EOF", size: 32, cancelEOF: true, refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeFailureAPI(t)
			s.stdout = bytes.Repeat([]byte{'a'}, tc.size)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &nativeLogBody{}
			if tc.readError {
				body.readErr = errors.New("synthetic private read canary")
			}
			if tc.closeError {
				body.closeErr = errors.New("synthetic private close canary")
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
			out, err := nativeFailureCall(t, e)(ctx, "ns", nativeFixtureJobUID, want)
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
	t.Run("non EOF timeout", func(t *testing.T) {
		s, want := newNativeFailureAPI(t)
		s.logHold = true
		e := newExec(t, s.URL)
		e.timeout = 20 * time.Millisecond
		out, err := nativeFailureCall(t, e)(context.Background(), "ns", nativeFixtureJobUID, want)
		nativeRefused(t, out, err)
	})
	t.Run("truncated HTTP stream", func(t *testing.T) {
		s, want := newNativeFailureAPI(t)
		s.stdout, s.logReadError = []byte("partial synthetic output"), true
		out, err := nativeFailureCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
		nativeRefused(t, out, err)
	})
}

func TestNativeJobFailureFinalRecheck(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*nativeAPI)
	}{
		{"failed worker pod replaced", func(s *nativeAPI) { s.pod.UID = nativeFixtureReplacement }},
		{"failed worker job replaced", func(s *nativeAPI) { s.job.UID = nativeFixtureReplacement }},
		{"job active", func(s *nativeAPI) { s.job.Status.Active = 1 }},
		{"job terminal lost", func(s *nativeAPI) { s.job.Status.Conditions = nil }},
		{"pod terminal lost", func(s *nativeAPI) { s.pod.Status.Phase = corev1.PodRunning }},
		{"exit changed", func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 2 }},
		{"finish time changed", func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.Unix(1, 0) }},
		{"owner changed", func(s *nativeAPI) { s.pod.OwnerReferences[0].UID = nativeFixtureOther }},
		{"matching env replaced", func(s *nativeAPI) {
			s.pod.Spec.Containers[0].Env[0].Value = nativeFixtureOther
			s.job.Spec.Template.Spec.Containers[0].Env[0].Value = nativeFixtureOther
		}},
		{"hidden owned replacement", func(s *nativeAPI) {
			hidden := *s.pod.DeepCopy()
			hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
			s.pods = []corev1.Pod{s.pod, hidden}
		}},
		{"api error", func(s *nativeAPI) { s.fail = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeFailureAPI(t)
			s.stdout, s.after = []byte("synthetic failed worker output\n"), tc.edit
			out, err := nativeFailureCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 1 {
				t.Fatal("late mutation was not exercised")
			}
		})
	}
	t.Run("captured kubelet drift", func(t *testing.T) {
		s, want := newNativeFailureAPI(t)
		s.stdout = []byte("synthetic failed worker output\n")
		before, after := withCapturedDrift(t, &s.pod)
		s.pod.Status = before.Status
		s.after = func(s *nativeAPI) { s.pod.Status = after.Status }
		out, err := nativeFailureCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
		if err != nil || !out.Successful() || string(out.Payload()) != string(s.stdout) || s.after != nil {
			t.Fatalf("successful=%v drift applied=%v error=%v", out.Successful(), s.after == nil, err)
		}
	})
}
