package podexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

const (
	nativeOutputExecutable    = "/usr/local/bin/rpgmigrate"
	nativeOutputJSONArg       = "--json"
	nativeOutputVersionArg    = "version"
	nativeOutputHiddenName    = "hidden"
	nativeOutputHiddenUID     = "hidden-uid"
	nativeOutputPageToken     = "next"
	nativeOutputSnapshot      = "snapshot"
	nativeOutputLaterSnapshot = "later-snapshot"
)

type nativeOutputReader interface {
	JobVersionNative(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error)
	JobPreflightNative(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error)
}

func TestNativeVersionCompletedOutput(t *testing.T) {
	for _, role := range []string{nativeOutputVersionArg, "preflight"} {
		t.Run(role, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			s.stdout = []byte("{\"result\":true}\n")
			r, ok := any(newExec(t, s.URL)).(nativeOutputReader)
			if !ok {
				t.Fatal("bounded original completed-output reader is missing")
			}
			read := r.JobVersionNative
			if role == "preflight" {
				read = r.JobPreflightNative
			}
			out, err := read(context.Background(), "ns", nativeFixtureJobUID, want)
			if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) {
				t.Fatalf("successful=%v payload bytes=%d error=%v", out.Successful(), len(out.Payload()), err)
			}
			if s.effects != 1 || s.gets != 2 {
				t.Fatalf("log reads=%d pod reads=%d", s.effects, s.gets)
			}
		})
	}
	t.Run("admission", testNativeOutputAdmission)
	t.Run("final read", testNativeOutputFinalRead)
	t.Run("stream", testNativeOutputStream)
	t.Run("execution hash", testNativeExecutionBinding)
	t.Run("fixed version command", testNativeVersionCommand)
	t.Run("cancellation", testNativeOutputCancellation)
	t.Run("original termination", testNativeOutputWait)
	t.Run("owned pod inventory", testNativeOutputInventory)
}

func newNativeOutputAPI(t *testing.T) (*nativeAPI, NativeContainerBinding) {
	t.Helper()
	s, want := newNativeAPI(t, true)
	s.job.Spec.Template.Spec.Containers[0].Command = []string{nativeOutputExecutable}
	s.job.Spec.Template.Spec.Containers[0].Args = []string{nativeOutputVersionArg, nativeOutputJSONArg}
	s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
	raw, err := json.Marshal(s.job.Spec.Template)
	if err != nil {
		t.Fatal(err)
	}
	want.ExecutionSHA256 = sha256.Sum256(raw)
	previous := s.Server
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/pods") {
			s.serve(w, r)
			return
		}
		s.mu.Lock()
		pods := s.pods
		if pods == nil {
			pods = []corev1.Pod{*s.pod.DeepCopy()}
		}
		items := make([]corev1.Pod, 0, len(pods))
		for _, pod := range pods {
			if r.URL.Query().Get("labelSelector") == "" || pod.Labels[batchv1.ControllerUidLabel] == nativeFixtureJobUID {
				items = append(items, *pod.DeepCopy())
			}
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "snapshot"}, Items: items})
	}))
	previous.Close()
	t.Cleanup(s.Close)
	return s, want
}

func nativeVersionCall(t *testing.T, e *Exec) func(context.Context, string, types.UID, NativeContainerBinding) (NativeOutcome, error) {
	t.Helper()
	r, ok := any(e).(nativeOutputReader)
	if !ok {
		t.Fatal("bounded original completed-output reader is missing")
	}
	return r.JobVersionNative
}

func testNativeOutputAdmission(t *testing.T) {
	cases := []struct {
		name string
		edit func(*nativeAPI, *NativeContainerBinding)
	}{
		{"missing pod", func(s *nativeAPI, _ *NativeContainerBinding) { s.pods = []corev1.Pod{} }},
		{"multiple pods", func(s *nativeAPI, _ *NativeContainerBinding) { s.pods = []corev1.Pod{s.pod, s.pod} }},
		{"hidden owned pod", func(s *nativeAPI, _ *NativeContainerBinding) {
			hidden := *s.pod.DeepCopy()
			hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
			s.pods = []corev1.Pod{s.pod, hidden}
		}},
		{"foreign owner", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.OwnerReferences[0].UID = "foreign" }},
		{"missing owner", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.OwnerReferences = nil }},
		{"multiple controllers", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.OwnerReferences = append(s.pod.OwnerReferences, s.pod.OwnerReferences[0])
		}},
		{"replaced pod", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.UID = nativeFixtureReplacement }},
		{"replaced job", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.UID = nativeFixtureReplacement }},
		{"missing job uid", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.UID = "" }},
		{"wrong label", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Labels[batchv1.ControllerUidLabel] = "foreign" }},
		{"missing pod uid binding", func(_ *nativeAPI, w *NativeContainerBinding) { w.PodUID = "" }},
		{"missing container binding", func(_ *nativeAPI, w *NativeContainerBinding) { w.ContainerID = "" }},
		{"missing image binding", func(_ *nativeAPI, w *NativeContainerBinding) { w.ImageID = "" }},
		{"missing template binding", func(_ *nativeAPI, w *NativeContainerBinding) { w.TemplateSHA256 = [32]byte{} }},
		{"missing execution binding", func(_ *nativeAPI, w *NativeContainerBinding) { w.ExecutionSHA256 = [32]byte{} }},
		{"wrong execution binding", func(_ *nativeAPI, w *NativeContainerBinding) { w.ExecutionSHA256[0] ^= 1 }},
		{"container name mismatch", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Spec.Containers[0].Name = nativeFixtureOther }},
		{"missing container id", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Status.ContainerStatuses[0].ContainerID = "" }},
		{"replaced container", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].ContainerID = nativeFixtureOther
		}},
		{"wrong terminated container", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.ContainerID = nativeFixtureOther
		}},
		{"wrong raw image", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].ImageID = nativeFixtureOther
		}},
		{"changed pod image", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Spec.Containers[0].Image = nativeFixtureOther }},
		{"changed job image", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Spec.Template.Spec.Containers[0].Image = nativeFixtureOther
		}},
		{"changed pod command", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Containers[0].Command = []string{nativeFixtureOther}
		}},
		{"changed job command", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Spec.Template.Spec.Containers[0].Command = []string{nativeFixtureOther}
		}},
		{"changed pod args", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Containers[0].Args = []string{nativeFixtureOther}
		}},
		{"changed literal env", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Containers[0].Env[0].Value = nativeFixtureOther
		}},
		{"unexpected restart", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"bound restart", func(s *nativeAPI, w *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].RestartCount = 1
			w.RestartCount = 1
		}},
		{"previous termination", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{}
		}},
		{"nonzero exit", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
		}},
		{"signal", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.Signal = 9
		}},
		{"running", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{"pending", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Status.Phase = corev1.PodPending }},
		{"failed pod", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Status.Phase = corev1.PodFailed }},
		{"failed job", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Failed = 1 }},
		{"active job", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Active = 1 }},
		{"no success", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Succeeded = 0 }},
		{"multiple successes", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Succeeded = 2 }},
		{"no completion", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Conditions = nil }},
		{"failed condition", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Status.Conditions = append(s.job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue})
		}},
		{"failure target", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.job.Status.Conditions = append(s.job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue})
		}},
		{"deleting pod", func(s *nativeAPI, _ *NativeContainerBinding) { now := metav1.Now(); s.pod.DeletionTimestamp = &now }},
		{"deleting job", func(s *nativeAPI, _ *NativeContainerBinding) { now := metav1.Now(); s.job.DeletionTimestamp = &now }},
		{"extra container", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Containers = append(s.pod.Spec.Containers, corev1.Container{Name: nativeFixtureOther})
		}},
		{"initializing container", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.InitContainers = []corev1.Container{{Name: nativeFixtureOther}}
		}},
		{"ephemeral", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.EphemeralContainers = []corev1.EphemeralContainer{{}}
		}},
		{"api error", func(s *nativeAPI, _ *NativeContainerBinding) { s.fail = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			tc.edit(s, &want)
			e := newExec(t, s.URL)
			e.timeout = 15 * time.Millisecond
			out, err := nativeVersionCall(t, e)(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 0 {
				t.Fatal("unadmitted execution reached logs")
			}
		})
	}
	for _, input := range []struct {
		name, namespace string
		jobUID          types.UID
	}{
		{"empty namespace", "", nativeFixtureJobUID},
		{"empty uid", "ns", ""},
		{"wrong uid", "ns", nativeFixtureOther},
	} {
		t.Run(input.name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			out, err := nativeVersionCall(t, newExec(t, s.URL))(context.Background(), input.namespace, input.jobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 0 {
				t.Fatal("invalid identity reached logs")
			}
		})
	}
}

func testNativeOutputFinalRead(t *testing.T) {
	cases := []struct {
		name string
		edit func(*nativeAPI)
	}{
		{"missing pod", func(s *nativeAPI) { s.pods = []corev1.Pod{} }},
		{"duplicate pod", func(s *nativeAPI) { s.pods = []corev1.Pod{s.pod, s.pod} }},
		{"hidden later pod", func(s *nativeAPI) {
			hidden := *s.pod.DeepCopy()
			hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
			s.pods = []corev1.Pod{s.pod, hidden}
		}},
		{"pod uid", func(s *nativeAPI) { s.pod.UID = nativeFixtureOther }},
		{"job uid", func(s *nativeAPI) { s.job.UID = nativeFixtureOther }},
		{"pod generation", func(s *nativeAPI) { s.pod.Generation++ }},
		{"job generation", func(s *nativeAPI) { s.job.Generation++ }},
		{"pod owner", func(s *nativeAPI) { s.pod.OwnerReferences[0].Name = nativeFixtureOther }},
		{"job owner", func(s *nativeAPI) { s.job.OwnerReferences = []metav1.OwnerReference{{UID: nativeFixtureOther}} }},
		{"pod label", func(s *nativeAPI) { s.pod.Labels[nativeFixtureOther] = nativeFixtureWrong }},
		{"pod annotation", func(s *nativeAPI) { s.pod.Annotations = map[string]string{nativeFixtureOther: nativeFixtureWrong} }},
		{"job label", func(s *nativeAPI) { s.job.Labels = map[string]string{nativeFixtureOther: nativeFixtureWrong} }},
		{"job annotation", func(s *nativeAPI) { s.job.Annotations = map[string]string{nativeFixtureOther: nativeFixtureWrong} }},
		{"pod command", func(s *nativeAPI) { s.pod.Spec.Containers[0].Command = []string{nativeFixtureOther} }},
		{"job command", func(s *nativeAPI) { s.job.Spec.Template.Spec.Containers[0].Command = []string{nativeFixtureOther} }},
		{"pod status", func(s *nativeAPI) { s.pod.Status.Message = nativeFixtureWrong }},
		{"job status", func(s *nativeAPI) { s.job.Status.CompletedIndexes = "0" }},
		{"container id", func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].ContainerID = nativeFixtureOther }},
		{"later restart", func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"image", func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].ImageID = nativeFixtureOther }},
		{"no completion", func(s *nativeAPI) { s.job.Status.Conditions = nil }},
		{"api failure", func(s *nativeAPI) { s.fail = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			s.stdout = []byte("{\"result\":true}\n")
			s.after = tc.edit
			out, err := nativeVersionCall(t, newExec(t, s.URL))(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 1 {
				t.Fatalf("log reads=%d", s.effects)
			}
		})
	}
}

type nativeLogRoundTripper struct {
	next  http.RoundTripper
	wrap  func(io.ReadCloser) io.ReadCloser
	after func(*http.Request)
}

func (r nativeLogRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.next.RoundTrip(req)
	if err == nil && r.after != nil {
		r.after(req)
	}
	if err == nil && r.wrap != nil && strings.HasSuffix(req.URL.Path, "/log") {
		resp.Body = r.wrap(resp.Body)
	}
	return resp, err
}

type nativeLogBody struct {
	io.ReadCloser
	eof, closed            bool
	bytesRead              int
	readErr, closeErr      error
	onRead, onEOF, onClose func()
}

func (b *nativeLogBody) Read(p []byte) (int, error) {
	if b.onRead != nil {
		b.onRead()
	}
	if b.readErr != nil {
		return 0, b.readErr
	}
	n, err := b.ReadCloser.Read(p)
	b.bytesRead += n
	if err == io.EOF {
		b.eof = true
		if b.onEOF != nil {
			b.onEOF()
		}
	}
	return n, err
}

func testNativeExecutionBinding(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*corev1.PodTemplateSpec)
	}{
		{"command", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Command = []string{nativeFixtureOther} }},
		{"args", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Args = []string{nativeFixtureOther} }},
		{"literal env", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Env[0].Value = nativeFixtureOther }},
		{"mount", func(p *corev1.PodTemplateSpec) {
			p.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}}
		}},
		{"volume", func(p *corev1.PodTemplateSpec) {
			p.Spec.Volumes = []corev1.Volume{{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
		}},
		{"container security", func(p *corev1.PodTemplateSpec) {
			value := true
			p.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &value}
		}},
		{"pod security", func(p *corev1.PodTemplateSpec) {
			uid := int64(1)
			p.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: &uid}
		}},
		{"label", func(p *corev1.PodTemplateSpec) { p.Labels = map[string]string{"role": nativeFixtureOther} }},
		{"annotation", func(p *corev1.PodTemplateSpec) { p.Annotations = map[string]string{"role": nativeFixtureOther} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			tc.edit(&s.job.Spec.Template)
			s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
			out, err := newExec(t, s.URL).JobPreflightNative(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 0 {
				t.Fatal("substituted execution reached logs")
			}
		})
	}
}

func testNativeVersionCommand(t *testing.T) {
	for _, tc := range []struct {
		name          string
		command, args []string
	}{
		{"shell", []string{"/bin/sh", "-c"}, []string{"/usr/local/bin/rpgmigrate version --json"}},
		{"path lookup", []string{"rpgmigrate"}, []string{nativeOutputVersionArg, nativeOutputJSONArg}},
		{"other role", []string{nativeOutputExecutable}, []string{"clone"}},
		{"extra arg", []string{nativeOutputExecutable}, []string{nativeOutputVersionArg, nativeOutputJSONArg, nativeFixtureOther}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			c := &s.job.Spec.Template.Spec.Containers[0]
			c.Command, c.Args = tc.command, tc.args
			s.pod.Spec = *s.job.Spec.Template.Spec.DeepCopy()
			raw, err := json.Marshal(s.job.Spec.Template)
			if err != nil {
				t.Fatal(err)
			}
			want.ExecutionSHA256 = sha256.Sum256(raw)
			out, err := newExec(t, s.URL).JobVersionNative(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 0 {
				t.Fatal("self-derived command substitution reached logs")
			}
		})
	}
}

func testNativeOutputCancellation(t *testing.T) {
	for _, boundary := range []string{"before admission", "initial list", "initial pod", "initial job", "log response", "read", "EOF", "close", "final list", "final pod", "final job"} {
		t.Run(boundary, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			triggered := false
			trigger := func() { triggered = true; cancel() }
			if boundary == "before admission" {
				trigger()
			}
			body := &nativeLogBody{}
			switch boundary {
			case "read":
				body.onRead = trigger
			case "EOF":
				body.onEOF = trigger
			case "close":
				body.onClose = trigger
			}
			counts := map[string]int{}
			e, err := New(&rest.Config{Host: s.URL, WrapTransport: func(next http.RoundTripper) http.RoundTripper {
				return nativeLogRoundTripper{next: next, wrap: func(original io.ReadCloser) io.ReadCloser { body.ReadCloser = original; return body }, after: func(req *http.Request) {
					part := ""
					switch {
					case strings.HasSuffix(req.URL.Path, "/pods"):
						part = "list"
					case strings.Contains(req.URL.Path, "/jobs/"):
						part = "job"
					case strings.HasSuffix(req.URL.Path, "/log"):
						part = "log response"
					default:
						part = "pod"
					}
					counts[part]++
					stage := "initial " + part
					if counts[part] == 2 {
						stage = "final " + part
					}
					if stage == boundary || part == boundary {
						trigger()
					}
				}}
			}})
			if err != nil {
				t.Fatal(err)
			}
			out, err := e.JobVersionNative(ctx, "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if !triggered {
				t.Fatal("cancellation boundary was not exercised")
			}
			if body.ReadCloser != nil && !body.closed {
				t.Fatal("cancelled capture leaked body")
			}
		})
	}
}

func testNativeOutputWait(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "wait for original completion"
		if replace {
			name = "replacement while waiting"
		}
		t.Run(name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			s.pod.Status.Phase = corev1.PodRunning
			s.job.Status = batchv1.JobStatus{Active: 1}
			s.stdout = []byte("{\"result\":true}\n")
			s.onPodGet = func(s *nativeAPI) {
				if s.gets != 2 {
					return
				}
				if replace {
					s.pod.UID = "replacement"
					return
				}
				s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: nativeFixtureContainerID}}
				s.pod.Status.Phase = corev1.PodSucceeded
				s.job.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
			}
			out, err := newExec(t, s.URL).JobVersionNative(context.Background(), "ns", nativeFixtureJobUID, want)
			if replace {
				nativeRefused(t, out, err)
				if s.effects != 0 {
					t.Fatal("replacement reached logs")
				}
				return
			}
			if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) || s.gets != 3 {
				t.Fatalf("successful=%v pod reads=%d error=%v", out.Successful(), s.gets, err)
			}
		})
	}
}

func testNativeOutputInventory(t *testing.T) {
	t.Run("unrelated pods do not change ownership", func(t *testing.T) {
		s, want := newNativeOutputAPI(t)
		unrelated := *s.pod.DeepCopy()
		unrelated.UID, unrelated.Name = "unrelated-uid", "unrelated"
		unrelated.OwnerReferences[0].UID = "unrelated-job"
		s.pods = []corev1.Pod{unrelated, s.pod, {ObjectMeta: metav1.ObjectMeta{Name: "unowned"}}}
		out, err := newExec(t, s.URL).JobVersionNative(context.Background(), "ns", nativeFixtureJobUID, want)
		if err != nil || !out.Successful() || s.effects != 1 {
			t.Fatalf("successful=%v log reads=%d error=%v", out.Successful(), s.effects, err)
		}
	})
	for _, state := range []string{"failed", "terminating", "nonzero restarts"} {
		t.Run("hidden owned "+state, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			hidden := *s.pod.DeepCopy()
			hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
			switch state {
			case "failed":
				hidden.Status.Phase = corev1.PodFailed
			case "terminating":
				now := metav1.Now()
				hidden.DeletionTimestamp = &now
			case "nonzero restarts":
				hidden.Status.ContainerStatuses[0].RestartCount = 1
			}
			s.pods = []corev1.Pod{s.pod, hidden}
			out, err := newExec(t, s.URL).JobVersionNative(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
			if s.effects != 0 {
				t.Fatal("duplicate owned execution reached logs")
			}
		})
	}

	t.Run("pagination", testNativeOutputPagination)
}

func testNativeOutputPagination(t *testing.T) {
	type page struct {
		rv, continuation        string
		original, hidden, shard bool
		status                  int
	}
	for _, tc := range []struct {
		name    string
		pages   []page
		success bool
	}{
		{"complete pages", []page{{rv: nativeOutputSnapshot, continuation: nativeOutputPageToken}, {rv: nativeOutputSnapshot, original: true}, {rv: nativeOutputLaterSnapshot, continuation: nativeOutputPageToken}, {rv: nativeOutputLaterSnapshot, original: true}}, true},
		{"duplicate next page", []page{{rv: nativeOutputSnapshot, continuation: nativeOutputPageToken, original: true}, {rv: nativeOutputSnapshot, hidden: true}}, false},
		{"repeated continuation", []page{{rv: nativeOutputSnapshot, continuation: nativeOutputPageToken}, {rv: nativeOutputSnapshot, continuation: nativeOutputPageToken, original: true}}, false},
		{"continuation cycle", []page{{rv: nativeOutputSnapshot, continuation: nativeOutputPageToken}, {rv: nativeOutputSnapshot, continuation: "more"}, {rv: nativeOutputSnapshot, continuation: nativeOutputPageToken, original: true}}, false},
		{"changed snapshot", []page{{rv: nativeOutputSnapshot, continuation: nativeOutputPageToken}, {rv: nativeOutputLaterSnapshot, original: true}}, false},
		{"missing snapshot", []page{{original: true}}, false},
		{"expired continuation", []page{{rv: nativeOutputSnapshot, continuation: nativeOutputPageToken}, {status: http.StatusGone}}, false},
		{"invalid continuation", []page{{rv: nativeOutputSnapshot, continuation: nativeOutputPageToken}, {status: http.StatusBadRequest}}, false},
		{"sharded snapshot", []page{{rv: nativeOutputSnapshot, shard: true}}, false},
		{"list API failure", []page{{status: http.StatusInternalServerError}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			previous := s.Server
			pages := 0
			s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/pods") {
					s.serve(w, r)
					return
				}
				index := pages
				pages++
				if index >= len(tc.pages) {
					t.Error("unexpected extra page request")
					http.Error(w, "invalid page", http.StatusBadRequest)
					return
				}
				expectedContinuation := ""
				if index > 0 {
					expectedContinuation = tc.pages[index-1].continuation
				}
				q := r.URL.Query()
				if q.Get("limit") != "100" || len(q) > 2 || q.Get("labelSelector") != "" || q.Get("fieldSelector") != "" || q.Get("continue") != expectedContinuation {
					t.Error("inventory changed its page query")
					http.Error(w, "invalid list", http.StatusBadRequest)
					return
				}
				step := tc.pages[index]
				if step.status != 0 {
					http.Error(w, "fixture-secret invalid page", step.status)
					return
				}
				list := corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: step.rv, Continue: step.continuation}}
				if step.original {
					list.Items = append(list.Items, s.pod)
				}
				if step.hidden {
					hidden := *s.pod.DeepCopy()
					hidden.Name, hidden.UID, hidden.Labels = nativeOutputHiddenName, nativeOutputHiddenUID, nil
					list.Items = append(list.Items, hidden)
				}
				if step.shard {
					list.ShardInfo = &metav1.ShardInfo{Selector: "subset"}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(list)
			}))
			previous.Close()
			t.Cleanup(s.Close)
			out, err := newExec(t, s.URL).JobVersionNative(context.Background(), "ns", nativeFixtureJobUID, want)
			if tc.success {
				if err != nil || !out.Successful() || s.effects != 1 {
					t.Fatalf("successful=%v log reads=%d error=%v", out.Successful(), s.effects, err)
				}
			} else {
				nativeRefused(t, out, err)
				if s.effects != 0 {
					t.Fatal("invalid inventory reached logs")
				}
			}
			if pages != len(tc.pages) {
				t.Fatalf("page reads=%d want=%d", pages, len(tc.pages))
			}
		})
	}
}

func (b *nativeLogBody) Close() error {
	b.closed = true
	if b.onClose != nil {
		b.onClose()
	}
	return errors.Join(b.ReadCloser.Close(), b.closeErr)
}

func testNativeOutputStream(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		size                                     int
		refuse, truncated, readError, closeError bool
	}{
		{name: "empty EOF", size: 0},
		{name: "one byte", size: 1},
		{name: "maximum payload", size: 65536},
		{name: "oversize payload", size: 65537, refuse: true},
		{name: "larger payload", size: 131072, refuse: true},
		{name: "truncated HTTP body", size: 10, truncated: true, refuse: true},
		{name: "read failure", size: 10, readError: true, refuse: true},
		{name: "close failure", size: 10, closeError: true, refuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			s.stdout = bytes.Repeat([]byte{'x'}, tc.size)
			s.logReadError = tc.truncated
			body := &nativeLogBody{}
			if tc.readError {
				body.readErr = errors.New("fixture-secret postgres://private read")
			}
			if tc.closeError {
				body.closeErr = errors.New("fixture-secret postgres://private close")
			}
			e, err := New(&rest.Config{Host: s.URL, WrapTransport: func(next http.RoundTripper) http.RoundTripper {
				return nativeLogRoundTripper{next: next, wrap: func(original io.ReadCloser) io.ReadCloser { body.ReadCloser = original; return body }}
			}})
			if err != nil {
				t.Fatal(err)
			}
			out, err := nativeVersionCall(t, e)(context.Background(), "ns", nativeFixtureJobUID, want)
			if tc.refuse {
				nativeRefused(t, out, err)
			} else {
				if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) || !body.eof {
					t.Fatalf("successful=%v payload bytes=%d EOF=%v error=%v", out.Successful(), len(out.Payload()), body.eof, err)
				}
				if len(out.Payload()) > 0 {
					clone := out.Payload()
					clone[0] = 'z'
					if bytes.Equal(out.Payload(), clone) {
						t.Fatal("output exposed mutable payload")
					}
				}
			}
			if !body.closed || s.effects != 1 {
				t.Fatalf("closed=%v log reads=%d", body.closed, s.effects)
			}
			if body.bytesRead > 65537 {
				t.Fatalf("unbounded log read: %d bytes", body.bytesRead)
			}
			if tc.refuse && s.gets != 1 {
				t.Fatalf("failed capture reached final admission: pod reads=%d", s.gets)
			}
		})
	}
	t.Run("combined diagnostic bytes remain present", func(t *testing.T) {
		s, want := newNativeOutputAPI(t)
		s.stdout, s.stderr = []byte("{}\n"), []byte("diagnostic-canary\n")
		out, err := newExec(t, s.URL).JobVersionNative(context.Background(), "ns", nativeFixtureJobUID, want)
		if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), []byte("{}\ndiagnostic-canary\n")) {
			t.Fatalf("successful=%v payload bytes=%d error=%v", out.Successful(), len(out.Payload()), err)
		}
	})
	t.Run("missing EOF times out and closes", testNativeOutputMissingEOF)
}

func testNativeOutputMissingEOF(t *testing.T) {
	s, want := newNativeOutputAPI(t)
	previous := s.Server
	handler := previous.Config.Handler
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/log") {
			handler.ServeHTTP(w, r)
			return
		}
		s.mu.Lock()
		s.effects++
		s.mu.Unlock()
		_, _ = w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	previous.Close()
	t.Cleanup(s.Close)
	body := &nativeLogBody{}
	e, err := New(&rest.Config{Host: s.URL, WrapTransport: func(next http.RoundTripper) http.RoundTripper {
		return nativeLogRoundTripper{next: next, wrap: func(original io.ReadCloser) io.ReadCloser { body.ReadCloser = original; return body }}
	}})
	if err != nil {
		t.Fatal(err)
	}
	e.timeout = 50 * time.Millisecond
	out, err := e.JobVersionNative(context.Background(), "ns", nativeFixtureJobUID, want)
	nativeRefused(t, out, err)
	if !body.closed || body.eof || s.effects != 1 || s.gets != 1 {
		t.Fatalf("closed=%v EOF=%v log reads=%d pod reads=%d", body.closed, body.eof, s.effects, s.gets)
	}
}
