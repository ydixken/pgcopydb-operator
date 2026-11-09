package podexec

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	nativeFixtureContainerName = "native"
	nativeFixturePodName       = "helper-pod"
	nativeFixtureJobUID        = "job-uid"
	nativeFixtureSecret        = "fixture-secret"
	nativeFixtureContainerID   = "containerd://exact"
	nativeFixtureCommand       = "writer"
	nativeFixtureWrong         = "wrong"
	nativeFixtureReplacement   = "replacement"
	nativeFixtureOther         = "other"
	nativeFixturePrivate       = "private"
)

type nativeAPI struct {
	*httptest.Server
	mu                     sync.Mutex
	pod                    corev1.Pod
	job                    batchv1.Job
	pods                   []corev1.Pod
	stdout, stderr, status []byte
	input                  []byte
	closeCode              int
	extraChannel           byte
	hold                   bool
	after                  func(*nativeAPI)
	onPodGet               func(*nativeAPI)
	gets, effects          int
	logReadError, fail     bool
	logHold                bool
	rejectExec             bool
}

func newNativeAPI(t *testing.T, terminated bool) (*nativeAPI, NativeContainerBinding) {
	t.Helper()
	controller := true
	s := &nativeAPI{stdout: []byte("{\"result\":true}\n"), status: []byte(`{"metadata":{},"status":"Success"}`), closeCode: websocket.CloseNormalClosure}
	s.job = batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "helper", Namespace: "ns", UID: nativeFixtureJobUID}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: nativeFixtureContainerName, Image: "engine@sha256:admitted", Env: []corev1.EnvVar{{Name: "PGPASSWORD", Value: nativeFixtureSecret}}}}}}}}
	s.pod = corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: nativeFixturePodName, Namespace: "ns", UID: "pod-uid", Labels: map[string]string{batchv1.ControllerUidLabel: nativeFixtureJobUID}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "helper", UID: nativeFixtureJobUID, Controller: &controller}}}, Spec: *s.job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: nativeFixtureContainerName, ContainerID: nativeFixtureContainerID, ImageID: "engine@sha256:platform", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	if terminated {
		s.pod.Status.Phase = corev1.PodSucceeded
		s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, ContainerID: nativeFixtureContainerID, Reason: "Completed"}}
		s.job.Status.Succeeded = 1
		s.job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		s.stdout = nil
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	want := NativeContainerBinding{PodUID: s.pod.UID, ContainerName: nativeFixtureContainerName, ContainerID: nativeFixtureContainerID, ImageID: "engine@sha256:platform", TemplateSHA256: nativeFixtureTemplateSHA(t, s.job.Spec.Template)}
	return s, want
}

func nativeFixtureTemplateSHA(t *testing.T, template corev1.PodTemplateSpec) [32]byte {
	t.Helper()
	return NativeTemplateSHA256(template)
}

func (s *nativeAPI) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.fail {
		s.mu.Unlock()
		http.Error(w, "fixture-secret postgres://private", http.StatusInternalServerError)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/exec") {
		s.effects++
		s.mu.Unlock()
		s.exec(w, r)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/log") {
		if len(r.URL.Query()) != 1 || r.URL.Query().Get("container") != nativeFixtureContainerName {
			s.mu.Unlock()
			http.Error(w, "fixture-secret unsupported log options", http.StatusBadRequest)
			return
		}
		s.effects++
		payload := append(bytes.Clone(s.stdout), s.stderr...)
		broken := s.logReadError
		hold := s.logHold
		if s.after != nil {
			s.after(s)
			s.after = nil
		}
		s.mu.Unlock()
		if hold {
			<-r.Context().Done()
			return
		}
		if broken {
			w.Header().Set("Content-Length", "99999")
		}
		_, _ = w.Write(payload)
		return
	}
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(r.URL.Path, "/pods"):
		if r.URL.Query().Get("labelSelector") != batchv1.ControllerUidLabel+"=job-uid" {
			http.Error(w, "wrong pod selector", http.StatusBadRequest)
			return
		}
		pods := s.pods
		if pods == nil {
			pods = []corev1.Pod{*s.pod.DeepCopy()}
		}
		_ = json.NewEncoder(w).Encode(corev1.PodList{Items: pods})
	case strings.HasSuffix(r.URL.Path, "/pods/helper-pod"):
		s.gets++
		if s.onPodGet != nil {
			s.onPodGet(s)
		}
		_ = json.NewEncoder(w).Encode(s.pod)
	case strings.HasSuffix(r.URL.Path, "/jobs/helper"):
		_ = json.NewEncoder(w).Encode(s.job)
	default:
		http.NotFound(w, r)
	}
}

func (s *nativeAPI) exec(w http.ResponseWriter, r *http.Request) {
	if s.rejectExec {
		http.Error(w, "fixture-secret unsupported upgrade", http.StatusUpgradeRequired)
		return
	}
	if r.URL.Query().Get("container") != nativeFixtureContainerName || r.URL.Query().Get("stdin") != "true" {
		http.Error(w, "wrong exec route", http.StatusBadRequest)
		return
	}
	u := websocket.Upgrader{Subprotocols: []string{"v5.channel.k8s.io"}}
	c, err := u.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close() //nolint:errcheck
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return
		}
		if bytes.Equal(raw, []byte{255, 0}) {
			break
		}
		if len(raw) == 0 || raw[0] != 0 {
			return
		}
		s.mu.Lock()
		s.input = append(s.input, raw[1:]...)
		s.mu.Unlock()
	}
	s.mu.Lock()
	out, diag, status := bytes.Clone(s.stdout), bytes.Clone(s.stderr), bytes.Clone(s.status)
	code, hold, extraChannel := s.closeCode, s.hold, s.extraChannel
	if s.after != nil {
		s.after(s)
		s.after = nil
	}
	s.mu.Unlock()
	for i, p := range [][]byte{out, diag, status} {
		if len(p) > 0 {
			if err := c.WriteMessage(websocket.BinaryMessage, append([]byte{byte(i + 1)}, p...)); err != nil {
				return
			}
		}
	}
	if extraChannel != 0 {
		if err := c.WriteMessage(websocket.BinaryMessage, []byte{extraChannel, 'x'}); err != nil {
			return
		}
	}
	if hold {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}
	if code != 0 {
		_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""), time.Now().Add(time.Second))
	}
}

func TestNativeCheckedExec(t *testing.T) {
	t.Run("incarnation states", testNativeExecStates)
	t.Run("success and copying payload", func(t *testing.T) {
		s, want := newNativeAPI(t, false)
		input := []byte("credential-free request")
		out, err := newExec(t, s.URL).InPodNative(context.Background(), "ns", nativeFixturePodName, want, []string{"fixed-writer"}, input)
		if err != nil || !out.Successful() || !bytes.Equal(out.Payload(), s.stdout) {
			t.Fatalf("checked result successful=%v error=%v", out.Successful(), err)
		}
		payload := out.Payload()
		payload[0] = 'x'
		if bytes.Equal(payload, out.Payload()) {
			t.Fatal("payload aliases the result")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if !bytes.Equal(input, s.input) || s.gets != 2 {
			t.Fatalf("input=%q pod reads=%d", s.input, s.gets)
		}
	})
	t.Run("accepted byte limits", func(t *testing.T) {
		s, want := newNativeAPI(t, false)
		s.stdout = bytes.Repeat([]byte("x"), 65536)
		input := bytes.Repeat([]byte("x"), 65536)
		out, err := newExec(t, s.URL).InPodNative(context.Background(), "ns", nativeFixturePodName, want, []string{nativeFixtureCommand}, input)
		if err != nil || !out.Successful() || len(out.Payload()) != 65536 || !bytes.Equal(input, s.input) {
			t.Fatalf("bounded result successful=%v error=%v", out.Successful(), err)
		}
	})
	cases := []struct {
		name  string
		alter func(*nativeAPI, *NativeContainerBinding)
	}{
		{"wrong pod UID", func(_ *nativeAPI, w *NativeContainerBinding) { w.PodUID = nativeFixtureWrong }},
		{"wrong container", func(_ *nativeAPI, w *NativeContainerBinding) { w.ContainerName = containerName }},
		{"wrong container ID", func(_ *nativeAPI, w *NativeContainerBinding) { w.ContainerID = nativeFixtureWrong }},
		{"wrong image", func(_ *nativeAPI, w *NativeContainerBinding) { w.ImageID = nativeFixtureWrong }},
		{"wrong restart", func(_ *nativeAPI, w *NativeContainerBinding) { w.RestartCount = 1 }},
		{"wrong template", func(_ *nativeAPI, w *NativeContainerBinding) { w.TemplateSHA256[0] ^= 1 }},
		{"duplicate controllers", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.OwnerReferences = append(s.pod.OwnerReferences, s.pod.OwnerReferences[0])
		}},
		{"stderr", func(s *nativeAPI, _ *NativeContainerBinding) { s.stderr = []byte(nativeFixtureSecret) }},
		{"nonzero status", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.status = []byte(`{"status":"Failure","message":"fixture-secret","reason":"NonZeroExitCode","details":{"causes":[{"reason":"ExitCode","message":"1"}]}}`)
		}},
		{"missing status", func(s *nativeAPI, _ *NativeContainerBinding) { s.status = nil }},
		{"unknown status", func(s *nativeAPI, _ *NativeContainerBinding) { s.status = []byte(`{"status":"Unknown"}`) }},
		{"truncated status", func(s *nativeAPI, _ *NativeContainerBinding) { s.status = []byte(`{"status":"Success"`) }},
		{"duplicate status", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.status = []byte(`{"status":"Success"}{"status":"Success"}`)
		}},
		{"duplicate status key", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.status = []byte(`{"status":"Failure","status":"Success"}`)
		}},
		{"null status", func(s *nativeAPI, _ *NativeContainerBinding) { s.status = []byte(`{"status":null}`) }},
		{"unknown stream", func(s *nativeAPI, _ *NativeContainerBinding) { s.extraChannel = 4 }},
		{"remote protocol error", func(s *nativeAPI, _ *NativeContainerBinding) { s.closeCode = websocket.CloseProtocolError }},
		{"missing EOF", func(s *nativeAPI, _ *NativeContainerBinding) { s.hold = true }},
		{"truncated EOF", func(s *nativeAPI, _ *NativeContainerBinding) { s.closeCode = 0 }},
		{"stdout overflow", func(s *nativeAPI, _ *NativeContainerBinding) { s.stdout = bytes.Repeat([]byte("x"), 65537) }},
		{"stderr overflow", func(s *nativeAPI, _ *NativeContainerBinding) { s.stderr = bytes.Repeat([]byte("x"), 65537) }},
		{"status overflow", func(s *nativeAPI, _ *NativeContainerBinding) { s.status = bytes.Repeat([]byte("x"), 65537) }},
		{"API error", func(s *nativeAPI, _ *NativeContainerBinding) { s.fail = true }},
		{"post UID change", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.UID = nativeFixtureReplacement }
		}},
		{"post container restart", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].RestartCount++ }
		}},
		{"post image change", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].ImageID = nativeFixtureOther }
		}},
		{"post literal environment change", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.Spec.Containers[0].Env[0].Value = nativeFixtureOther }
		}},
		{"post template change", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.job.Spec.Template.Spec.Containers[0].Image = nativeFixtureOther }
		}},
		{"post controller change", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.OwnerReferences[0].UID = nativeFixtureReplacement }
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeAPI(t, false)
			tc.alter(s, &want)
			e := newExec(t, s.URL)
			e.timeout = 100 * time.Millisecond
			out, err := e.InPodNative(context.Background(), "ns", nativeFixturePodName, want, []string{"postgres://fixture-secret"}, []byte(nativeFixtureSecret))
			nativeRefused(t, out, err)
		})
	}
	t.Run("cancellation", func(t *testing.T) {
		s, want := newNativeAPI(t, false)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out, err := newExec(t, s.URL).InPodNative(ctx, "ns", nativeFixturePodName, want, []string{nativeFixtureCommand}, nil)
		nativeRefused(t, out, err)
	})
	t.Run("input overflow refuses before exec", func(t *testing.T) {
		s, want := newNativeAPI(t, false)
		out, err := newExec(t, s.URL).InPodNative(context.Background(), "ns", nativeFixturePodName, want, []string{nativeFixtureCommand}, make([]byte, 65537))
		nativeRefused(t, out, err)
		if s.effects != 0 {
			t.Fatal("oversized input reached exec")
		}
	})
	t.Run("unsupported upgrade never retries command", func(t *testing.T) {
		s, want := newNativeAPI(t, false)
		s.rejectExec = true
		out, err := newExec(t, s.URL).InPodNative(context.Background(), "ns", nativeFixturePodName, want, []string{nativeFixtureCommand}, nil)
		nativeRefused(t, out, err)
		if s.effects != 1 {
			t.Fatalf("exec attempts=%d", s.effects)
		}
	})
}

func testNativeExecStates(t *testing.T) {
	cases := []struct {
		name            string
		before, success bool
		restart         int32
		change          func(*nativeAPI)
	}{
		{name: "post successful termination", success: true, change: func(s *nativeAPI) {
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: nativeFixtureContainerID}}
			s.pod.Status.Phase = corev1.PodSucceeded
			s.job.Status.Succeeded = 1
			s.job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		}},
		{name: "post nonzero exit", change: func(s *nativeAPI) {
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: nativeFixtureContainerID, ExitCode: 1}}
		}},
		{name: "post signal exit", change: func(s *nativeAPI) {
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: nativeFixtureContainerID, Signal: 15}}
		}},
		{name: "post wrong terminated container", change: func(s *nativeAPI) {
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: nativeFixtureReplacement}}
		}},
		{name: "post conflicting state", change: func(s *nativeAPI) {
			s.pod.Status.ContainerStatuses[0].State.Terminated = &corev1.ContainerStateTerminated{ContainerID: nativeFixtureContainerID}
		}},
		{name: "pre conflicting state", before: true, change: func(s *nativeAPI) {
			s.pod.Status.ContainerStatuses[0].State.Terminated = &corev1.ContainerStateTerminated{ContainerID: nativeFixtureContainerID}
		}},
		{name: "pre restarted incarnation", before: true, restart: 1, change: func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{name: "pre previous incarnation", before: true, change: func(s *nativeAPI) {
			s.pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ContainerID: "old"}
		}},
		{name: "pre failed Job", before: true, change: func(s *nativeAPI) { s.job.Status.Failed = 1 }},
		{name: "post failed Job", change: func(s *nativeAPI) { s.job.Status.Failed = 1 }},
		{name: "pre failed Job condition", before: true, change: func(s *nativeAPI) {
			s.job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
		}},
		{name: "post failed Pod", change: func(s *nativeAPI) { s.pod.Status.Phase = corev1.PodFailed }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeAPI(t, false)
			want.RestartCount = tc.restart
			if tc.before {
				tc.change(s)
			} else {
				s.after = tc.change
			}
			out, err := newExec(t, s.URL).InPodNative(context.Background(), "ns", nativeFixturePodName, want, []string{"relay"}, []byte("credential-free request"))
			if tc.success {
				if err != nil || !out.Successful() {
					t.Fatalf("successful=%v error=%v", out.Successful(), err)
				}
			} else {
				nativeRefused(t, out, err)
				if tc.before && s.effects != 0 {
					t.Fatal("refused initial state reached exec")
				}
			}
		})
	}
}

func nativeRefused(t *testing.T, out NativeOutcome, err error) {
	t.Helper()
	if err == nil || out.Successful() || out.Payload() != nil {
		t.Fatalf("refusal successful=%v payload bytes=%d error=%v", out.Successful(), len(out.Payload()), err)
	}
	if strings.Contains(err.Error(), nativeFixtureSecret) || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("unsanitized error: %s", err)
	}
}

func TestNativeHelperResultBinding(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		s, want := newNativeAPI(t, true)
		out, err := newExec(t, s.URL).JobCompletedNative(context.Background(), "ns", types.UID(nativeFixtureJobUID), want)
		if err != nil || !out.Successful() || out.Payload() != nil {
			t.Fatalf("successful=%v error=%v", out.Successful(), err)
		}
	})
	cases := []struct {
		name  string
		alter func(*nativeAPI, *NativeContainerBinding)
	}{
		{"job replacement", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.UID = nativeFixtureReplacement }},
		{"post job replacement", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.job.UID = nativeFixtureReplacement }
		}},
		{"post pod replacement", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.UID = nativeFixtureReplacement }
		}},
		{"post image change", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].ImageID = nativeFixtureReplacement }
		}},
		{"post pod spec change", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.Spec.Containers[0].Image = nativeFixtureReplacement }
		}},
		{"ambiguous pods", func(s *nativeAPI, _ *NativeContainerBinding) { s.pods = []corev1.Pod{s.pod, s.pod} }},
		{"wrong owner", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.OwnerReferences[0].UID = nativeFixtureOther }},
		{"missing controller", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.OwnerReferences[0].Controller = nil }},
		{"wrong container", func(_ *nativeAPI, w *NativeContainerBinding) { w.ContainerName = containerName }},
		{"wrong UID", func(_ *nativeAPI, w *NativeContainerBinding) { w.PodUID = nativeFixtureOther }},
		{"wrong image", func(_ *nativeAPI, w *NativeContainerBinding) { w.ImageID = nativeFixtureOther }},
		{"wrong template", func(_ *nativeAPI, w *NativeContainerBinding) { w.TemplateSHA256[0] ^= 1 }},
		{"restarted", func(s *nativeAPI, _ *NativeContainerBinding) { s.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"nonzero exit", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
		}},
		{"signal exit", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.Signal = 15
		}},
		{"post exit change", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1 }
		}},
		{"still running", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{"retry", func(s *nativeAPI, _ *NativeContainerBinding) { s.job.Status.Failed = 1 }},
		{"sidecar", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Spec.Containers = append(s.pod.Spec.Containers, corev1.Container{Name: "sidecar"})
		}},
		{"ordinary stdout byte", func(s *nativeAPI, _ *NativeContainerBinding) { s.stdout = []byte("x") }},
		{"historical result", func(s *nativeAPI, _ *NativeContainerBinding) { s.stdout = []byte("{\"result\":true}\n") }},
		{"mixed logs", func(s *nativeAPI, _ *NativeContainerBinding) { s.stderr = []byte("fixture-secret\n") }},
		{"overflow", func(s *nativeAPI, _ *NativeContainerBinding) { s.stdout = bytes.Repeat([]byte("x"), 65537) }},
		{"trailing log", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.stdout = append(s.stdout, []byte("fixture-secret\n")...)
		}},
		{"duplicate log result", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.stdout = []byte("{\"result\":true}\n{\"result\":true}\n")
		}},
		{"truncated EOF", func(s *nativeAPI, _ *NativeContainerBinding) { s.logReadError = true }},
		{"missing EOF", func(s *nativeAPI, _ *NativeContainerBinding) { s.logHold = true }},
		{"API failure", func(s *nativeAPI, _ *NativeContainerBinding) { s.fail = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, want := newNativeAPI(t, true)
			tc.alter(s, &want)
			e := newExec(t, s.URL)
			e.timeout = 100 * time.Millisecond
			out, err := e.JobCompletedNative(context.Background(), "ns", nativeFixtureJobUID, want)
			nativeRefused(t, out, err)
		})
	}
	t.Run("zero outcome", func(t *testing.T) {
		var out NativeOutcome
		if out.Successful() || out.Payload() != nil {
			t.Fatal("zero outcome is successful")
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		s, want := newNativeAPI(t, true)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out, err := newExec(t, s.URL).JobCompletedNative(ctx, "ns", nativeFixtureJobUID, want)
		nativeRefused(t, out, err)
	})
}

func TestNativeDefaultClusterCompletion(t *testing.T) {
	t.Run("disabled split feature empty ordinary logs", func(t *testing.T) {
		s, want := newNativeAPI(t, true)
		out, err := newExec(t, s.URL).JobCompletedNative(context.Background(), "ns", nativeFixtureJobUID, want)
		if err != nil || !out.Successful() || out.Payload() != nil {
			t.Fatalf("successful=%v payload bytes=%d error=%v", out.Successful(), len(out.Payload()), err)
		}
		if s.effects != 1 || s.gets != 2 {
			t.Fatalf("ordinary log reads=%d pod reads=%d", s.effects, s.gets)
		}
	})
	t.Run("waits for original successful termination", func(t *testing.T) {
		s, want := newNativeAPI(t, false)
		s.stdout = nil
		s.onPodGet = func(s *nativeAPI) {
			if s.gets == 2 {
				s.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: nativeFixtureContainerID}}
				s.pod.Status.Phase = corev1.PodSucceeded
				s.job.Status.Succeeded = 1
				s.job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			}
		}
		out, err := newExec(t, s.URL).JobCompletedNative(context.Background(), "ns", nativeFixtureJobUID, want)
		if err != nil || !out.Successful() || out.Payload() != nil || s.gets != 3 {
			t.Fatalf("successful=%v pod reads=%d error=%v", out.Successful(), s.gets, err)
		}
	})
	t.Run("replacement while waiting refuses", func(t *testing.T) {
		s, want := newNativeAPI(t, false)
		s.stdout = nil
		s.onPodGet = func(s *nativeAPI) {
			if s.gets == 2 {
				s.pod.UID = nativeFixtureReplacement
			}
		}
		out, err := newExec(t, s.URL).JobCompletedNative(context.Background(), "ns", nativeFixtureJobUID, want)
		nativeRefused(t, out, err)
		if s.effects != 0 {
			t.Fatal("replacement reached log read")
		}
	})
}

func TestNativeTemplateStructuralCorrelation(t *testing.T) {
	base := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		RestartPolicy:      corev1.RestartPolicyNever,
		ServiceAccountName: "native-service-account",
		ImagePullSecrets:   []corev1.LocalObjectReference{{Name: "image-pull-reference"}},
		Containers:         []corev1.Container{{Name: nativeFixtureContainerName, Image: "engine@sha256:admitted", Env: []corev1.EnvVar{{Name: "PGPASSWORD", Value: nativeFixtureSecret}, {Name: "PGUSER", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "connection-reference"}, Key: "username"}}}}}},
	}}
	want := NativeTemplateSHA256(base)
	if want == ([32]byte{}) {
		t.Fatal("nonzero structural fingerprint required")
	}
	t.Run("content and unrelated fields excluded", func(t *testing.T) {
		other := base.DeepCopy()
		other.Labels = map[string]string{nativeFixturePrivate: nativeFixtureSecret}
		other.Annotations = map[string]string{nativeFixturePrivate: nativeFixtureSecret}
		c := &other.Spec.Containers[0]
		c.Env[0].Value = "changed credential"
		c.Command = []string{nativeFixtureSecret}
		c.Args = []string{nativeFixtureSecret}
		c.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{HTTPHeaders: []corev1.HTTPHeader{{Name: "Authorization", Value: nativeFixtureSecret}}}}}
		c.Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{nativeFixtureSecret}}}}
		c.EnvFrom = []corev1.EnvFromSource{{Prefix: nativeFixtureSecret}}
		other.Spec.Volumes = []corev1.Volume{{Name: nativeFixturePrivate, VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{Driver: "driver", VolumeAttributes: map[string]string{nativeFixturePrivate: nativeFixtureSecret}}}}}
		c.VolumeMounts = []corev1.VolumeMount{{Name: nativeFixturePrivate, MountPath: "/private"}}
		other.Spec.SecurityContext = &corev1.PodSecurityContext{SELinuxOptions: &corev1.SELinuxOptions{User: nativeFixtureSecret}}
		other.Spec.DNSConfig = &corev1.PodDNSConfig{Searches: []string{"private.invalid"}}
		if got := NativeTemplateSHA256(*other); got != want {
			t.Fatal("credential-bearing or unrelated fields entered structural fingerprint")
		}
	})
	cases := []struct {
		name   string
		change func(*corev1.PodTemplateSpec)
	}{
		{"restart", func(p *corev1.PodTemplateSpec) { p.Spec.RestartPolicy = corev1.RestartPolicyAlways }},
		{"service account", func(p *corev1.PodTemplateSpec) { p.Spec.ServiceAccountName = nativeFixtureOther }},
		{"automount pointer", func(p *corev1.PodTemplateSpec) { v := false; p.Spec.AutomountServiceAccountToken = &v }},
		{"image pull reference", func(p *corev1.PodTemplateSpec) { p.Spec.ImagePullSecrets[0].Name = nativeFixtureOther }},
		{"container name", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Name = nativeFixtureOther }},
		{"image", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Image = nativeFixtureOther }},
		{"environment name", func(p *corev1.PodTemplateSpec) { p.Spec.Containers[0].Env[0].Name = nativeFixtureOther }},
		{"secret reference", func(p *corev1.PodTemplateSpec) {
			p.Spec.Containers[0].Env[1].ValueFrom.SecretKeyRef.Name = nativeFixtureOther
		}},
		{"init container", func(p *corev1.PodTemplateSpec) {
			p.Spec.InitContainers = []corev1.Container{{Name: "init", Image: nativeFixtureOther}}
		}},
		{"ephemeral container", func(p *corev1.PodTemplateSpec) {
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: nativeFixtureOther}}}
		}},
		{"environment order", func(p *corev1.PodTemplateSpec) {
			p.Spec.Containers[0].Env[0], p.Spec.Containers[0].Env[1] = p.Spec.Containers[0].Env[1], p.Spec.Containers[0].Env[0]
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			other := base.DeepCopy()
			tc.change(other)
			if NativeTemplateSHA256(*other) == want {
				t.Fatal("changed reference did not change fingerprint")
			}
		})
	}
}
