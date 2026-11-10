package controller

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
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/client-go/rest"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type nativeSamplerFixture struct {
	*nativeControlFixture
	want           native.ExecutionData
	client         *native.Client
	mu             sync.Mutex
	now            time.Time
	lag            int64
	binding        string
	cache          []byte
	replay         bool
	badProgress    bool
	legacyProgress bool
	beforeReply    func(bool)
	ops            []string
}

func newNativeSamplerFixture(t *testing.T) (*nativeSamplerFixture, *nativeSampler) {
	return newNativeSamplerFixtureMode(t, true)
}

func newNativeSamplerFixtureMode(t *testing.T, follow bool) (*nativeSamplerFixture, *nativeSampler) {
	t.Helper()
	f := &nativeSamplerFixture{nativeControlFixture: newNativeControlFixture(t), now: time.Unix(1000, 0), binding: strings.Repeat("a", 64)}
	if !follow {
		f.m.Spec.Follow.Enabled = false
		f.m.Spec.Cutover.Approved = false
		worker, err := buildNativeWorkerJob(f.m, f.pvc, nativeEvidenceTestImage, 1)
		if err != nil {
			t.Fatal(err)
		}
		worker.ObjectMeta = f.worker.ObjectMeta
		worker.TypeMeta = f.worker.TypeMeta
		worker.Status = f.worker.Status
		defaultProbeJob(worker)
		f.worker = worker
		pod := probePod(worker)
		pod.Status = f.pod.Status
		f.pod = pod
		f.store = clientfake.NewClientBuilder().WithScheme(f.r.Scheme).WithObjects(f.m, f.pvc, f.worker, f.pod).WithStatusSubresource(&v1beta1.Migration{}).Build()
	}
	want, err := f.r.admitNativeWorkerExecution(context.Background(), f.m, f.worker, f.pvc)
	if err != nil {
		t.Fatal(err)
	}
	f.want = want
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/exec") {
			f.execSample(w, r)
			return
		}
		f.serve(w, r)
	}))
	t.Cleanup(server.Close)
	transport, err := podexec.New(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	f.client = native.NewClient(transport)
	s := &nativeSampler{now: func() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.now }}
	t.Cleanup(s.Stop)
	return f, s
}

func (f *nativeSamplerFixture) admit(ctx context.Context) (native.ExecutionData, error) {
	return f.r.admitNativeWorkerExecution(ctx, f.m, f.worker, f.pvc)
}

func (f *nativeSamplerFixture) sample(s *nativeSampler, base bool, threshold int64) (nativeSample, error) {
	return s.Sample(context.Background(), f.client, f.m, f.want, base, threshold, f.admit)
}

func (f *nativeSamplerFixture) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func (f *nativeSamplerFixture) execSample(w http.ResponseWriter, r *http.Request) {
	c, err := (&websocket.Upgrader{Subprotocols: []string{remotecommand.StreamProtocolV5Name}}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = c.Close() }()
	var body []byte
	for {
		_, raw, readErr := c.ReadMessage()
		if readErr != nil {
			return
		}
		if bytes.Equal(raw, []byte{255, 0}) {
			break
		}
		if len(raw) == 0 || raw[0] != 0 {
			return
		}
		body = append(body, raw[1:]...)
	}
	lagCall := len(body) > 0
	f.mu.Lock()
	var output []byte
	if lagCall {
		var request map[string]json.RawMessage
		if json.Unmarshal(body, &request) != nil {
			f.t.Error("invalid native source request")
		}
		var op string
		if json.Unmarshal(request["operation_id"], &op) != nil {
			f.t.Error("missing native operation")
		}
		f.ops = append(f.ops, op)
		var result map[string]json.RawMessage
		if json.Unmarshal(nativeControlSourceReply(f.t, body, nativeControlTestEnd), &result) != nil {
			f.t.Error("invalid source fixture")
		}
		delete(result, nativeControlHeadKey)
		result["lag_bytes"], _ = json.Marshal(f.lag)
		result[nativeControlBindingKey], _ = json.Marshal(f.binding)
		output, err = json.Marshal(result)
		if err != nil {
			f.t.Error(err)
		}
		output = append(output, '\n')
		if f.replay {
			output = bytes.Clone(f.cache)
		} else {
			f.cache = bytes.Clone(output)
		}
	} else {
		zero := int64(0)
		report := native.Report{ProtocolVersion: 1, EngineVersion: "1", Command: "list progress", Capabilities: []native.Capability{native.CapProgress}, Work: &f.want.Work, Phase: "copying", Copy: &native.CopyProgress{Tables: &native.Counters{Total: &zero, Done: &zero}, Indexes: &native.Counters{Total: &zero, Done: &zero}, Bytes: &native.Counters{Total: &zero, Done: &zero}}}
		if followEnabled(f.m) && !f.legacyProgress {
			nativeM9Status(&report)
		}
		raw, marshalErr := json.Marshal(report)
		if marshalErr != nil {
			f.t.Error(marshalErr)
		}
		output = append(append([]byte("native-report: "), raw...), '\n')
		if f.badProgress {
			output = []byte("private-native-diagnostic-canary\n")
		}
	}
	hook := f.beforeReply
	f.mu.Unlock()
	if hook != nil {
		hook(lagCall)
	}
	_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{1}, output...))
	_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{3}, []byte(`{"status":"Success"}`)...))
	_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

func TestNativeInformationalLagCatchUp(t *testing.T) {
	f, s := newNativeSamplerFixture(t)
	first, err := f.sample(s, true, 0)
	if err != nil || first.lagBytes == nil || *first.lagBytes != 0 || first.catchUpConfirmed {
		t.Fatal("first checked zero lag must remain unconfirmed", err)
	}
	for range 3 {
		if s.Observe(f.m, f.want, true, 0).catchUpConfirmed {
			t.Fatal("cached observation counted twice")
		}
	}
	f.advance(time.Second)
	second, err := f.sample(s, true, 0)
	if err != nil || !second.catchUpConfirmed {
		t.Fatal("two distinct checked zero lag samples did not qualify", err)
	}
	f.mu.Lock()
	distinct := len(f.ops) == 2 && f.ops[0] != f.ops[1]
	f.mu.Unlock()
	if !distinct {
		t.Fatal("source requests reused a nonce")
	}
	f.advance(20 * time.Second)
	if got := s.Observe(f.m, f.want, true, 0); got.lagBytes == nil || got.catchUpConfirmed {
		t.Fatal("fresh current lag must survive expired pair eligibility")
	}
	f.advance(time.Nanosecond)
	if got := s.Observe(f.m, f.want, true, 0); got.lagBytes != nil || got.catchUpConfirmed {
		t.Fatal("stale current observation retained lag or readiness")
	}
}

func TestNativeSamplerLifecycle(t *testing.T) {
	t.Run("newer success excludes late original completion", func(t *testing.T) {
		f, s := newNativeSamplerFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		f.beforeReply = func(lag bool) {
			if lag {
				close(entered)
				<-release
			}
		}
		done := make(chan nativeSample, 1)
		go func() { got, _ := f.sample(s, true, 0); done <- got }()
		<-entered
		f.mu.Lock()
		f.beforeReply = nil
		f.mu.Unlock()
		newer, err := f.sample(s, true, 0)
		if err != nil || newer.lagBytes == nil || newer.catchUpConfirmed {
			t.Fatal("newer first observation was lost or confirmed", err)
		}
		close(release)
		if got := <-done; got.lagBytes != nil || got.progress != nil {
			t.Fatal("older callback published over newer sample")
		}
		if got := s.Observe(f.m, f.want, true, 0); got.lagBytes == nil || got.catchUpConfirmed {
			t.Fatal("older callback altered newer evidence")
		}
	})
	t.Run("cached source response is unknown", func(t *testing.T) {
		f, s := newNativeSamplerFixture(t)
		if _, err := f.sample(s, true, 0); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.replay = true
		f.mu.Unlock()
		got, err := f.sample(s, true, 0)
		if err == nil || got.lagBytes != nil || got.catchUpConfirmed || got.progress == nil {
			t.Fatal("cached response became fresh lag or discarded checked progress")
		}
		f.mu.Lock()
		f.replay = false
		f.mu.Unlock()
		if next, err := f.sample(s, true, 0); err != nil || next.lagBytes == nil || next.catchUpConfirmed {
			t.Fatal("unknown observation retained earlier eligibility", err)
		}
	})
	t.Run("stop blocks late reply", func(t *testing.T) {
		f, s := newNativeSamplerFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		f.beforeReply = func(lag bool) {
			if lag {
				close(entered)
				<-release
			}
		}
		done := make(chan nativeSample, 1)
		go func() { got, _ := f.sample(s, true, 0); done <- got }()
		<-entered
		s.Stop()
		close(release)
		if got := <-done; got.lagBytes != nil || got.progress != nil || got.catchUpConfirmed {
			t.Fatal("late stopped sample published")
		}
		if got := s.Observe(f.m, f.want, true, 0); got.lagBytes != nil || got.progress != nil {
			t.Fatal("stop revived cached data")
		}
	})
	t.Run("fresh admission after each operation", func(t *testing.T) {
		f, s := newNativeSamplerFixture(t)
		reads := 0
		admit := func(ctx context.Context) (native.ExecutionData, error) {
			reads++
			w, err := f.admit(ctx)
			if reads == 4 {
				w.Container.PodUID = probeReplacement
			}
			return w, err
		}
		got, err := s.Sample(context.Background(), f.client, f.m, f.want, true, 0, admit)
		if err == nil || reads != 4 || got.lagBytes != nil || got.progress != nil {
			t.Fatal("post-lag replacement published data")
		}
	})
	t.Run("new lifecycle intent discards sample", testNativeSamplerLifecycleIntent)
}

func testNativeSamplerLifecycleIntent(t *testing.T) {
	for _, intent := range []string{probeSuspendCase, "deletion intent", probeTerminalCase} {
		t.Run(intent, func(t *testing.T) {
			f, s := newNativeSamplerFixture(t)
			f.beforeReply = func(lag bool) {
				if !lag {
					return
				}
				m := f.current()
				var err error
				switch intent {
				case probeSuspendCase:
					m.Spec.Suspend = true
					err = f.store.Update(context.Background(), m)
				case "deletion intent":
					err = f.store.Delete(context.Background(), m)
				case probeTerminalCase:
					m.Status.Phase = v1beta1.PhaseFailed
					err = f.store.Status().Update(context.Background(), m)
				}
				if err != nil {
					t.Error(err)
				}
			}
			if got, err := f.sample(s, true, 0); err == nil || got.progress != nil || got.lagBytes != nil {
				t.Fatal("new lifecycle intent published data")
			}
		})
	}
}

func TestNativeMetricsMapping(t *testing.T) {
	t.Run("clone progress has no lag", func(t *testing.T) {
		f, s := newNativeSamplerFixtureMode(t, false)
		got, err := f.sample(s, true, 0)
		if err != nil || got.progress == nil || got.lagBytes != nil || got.catchUpConfirmed {
			t.Fatal("clone progress required or fabricated source lag", err)
		}
		f.mu.Lock()
		calls := len(f.ops)
		f.mu.Unlock()
		if calls != 0 {
			t.Fatal("clone-only sampling attempted source lag")
		}
	})
	f, s := newNativeSamplerFixture(t)
	got, err := f.sample(s, false, 0)
	if err != nil || got.progress == nil || got.progress.Tables == nil || got.progress.Tables.Done == nil || *got.progress.Tables.Done != 0 || got.phase != "copying" || got.catchUpConfirmed {
		t.Fatal("measured zero counters or phase were lost", err)
	}
	*got.progress.Tables.Done = 99
	if next := s.Observe(f.m, f.want, false, 0); *next.progress.Tables.Done != 0 {
		t.Fatal("caller modified cached progress")
	}
	f.mu.Lock()
	f.badProgress = true
	f.mu.Unlock()
	got, err = f.sample(s, true, 0)
	if err == nil || strings.Contains(err.Error(), "canary") || got.progress != nil || got.lagBytes != nil || got.catchUpConfirmed {
		t.Fatal("invalid progress published data or diagnostics")
	}
	var migration *v1beta1.Migration
	if empty := s.Observe(migration, f.want, true, 0); empty.lagBytes != nil || empty.progress != nil {
		t.Fatal("nil migration retained measurements")
	}
}

func testNativeSamplerAboveThreshold(t *testing.T) {
	f, s := newNativeSamplerFixture(t)
	if _, err := f.sample(s, true, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := f.sample(s, true, 0); err != nil || !got.catchUpConfirmed {
		t.Fatal("fixture did not qualify", err)
	}
	f.mu.Lock()
	f.lag = 1
	f.mu.Unlock()
	if got, err := f.sample(s, true, 0); err != nil || got.lagBytes == nil || *got.lagBytes != 1 || got.catchUpConfirmed {
		t.Fatal("above-threshold lag retained readiness", err)
	}
	f.mu.Lock()
	f.lag = 0
	f.mu.Unlock()
	if got, err := f.sample(s, true, 0); err != nil || got.catchUpConfirmed {
		t.Fatal("new first sample reused old eligibility", err)
	}
}

func TestNativeSamplerScopeReset(t *testing.T) {
	t.Run("above threshold resets the pair", testNativeSamplerAboveThreshold)
	for _, field := range []string{"threshold", "base copy", "migration UID", lifecycleGenerationCase, lifecycleAttemptCase, "sample worker UID", "Pod UID", "container ID", nativeControlRestartCase, nativeControlWorkKey, "source fingerprint"} {
		t.Run(field, func(t *testing.T) {
			f, s := newNativeSamplerFixture(t)
			if _, err := f.sample(s, true, 10); err != nil {
				t.Fatal(err)
			}
			if got, err := f.sample(s, true, 10); err != nil || !got.catchUpConfirmed {
				t.Fatal("fixture did not qualify", err)
			}
			m, w := f.m.DeepCopy(), f.want
			base, threshold := true, int64(10)
			switch field {
			case "threshold":
				threshold = 20
			case "base copy":
				base = false
			case "migration UID":
				m.UID = probeReplacement
			case lifecycleGenerationCase:
				m.Generation++
			case lifecycleAttemptCase:
				m.Status.Attempts++
			case "sample worker UID":
				w.JobUID = probeReplacement
			case "Pod UID":
				w.Container.PodUID = probeReplacement
			case "container ID":
				w.Container.ContainerID = string(probeReplacement)
			case nativeControlRestartCase:
				w.Container.RestartCount++
			case nativeControlWorkKey:
				w.Work.RunID = string(probeReplacement)
			case "source fingerprint":
				f.mu.Lock()
				f.binding = strings.Repeat("b", 64)
				f.mu.Unlock()
				got, err := f.sample(s, true, 10)
				if err != nil || got.lagBytes == nil || got.catchUpConfirmed {
					t.Fatal("changed source fingerprint reused eligibility", err)
				}
				return
			}
			if got := s.Observe(m, w, base, threshold); got.lagBytes != nil || got.catchUpConfirmed {
				t.Fatal("scope change retained measurement")
			}
		})
	}
	for _, phase := range []v1beta1.MigrationPhase{v1beta1.PhaseCompleted, v1beta1.PhaseFailed, v1beta1.PhaseSuspended} {
		t.Run(string(phase), func(t *testing.T) {
			f, s := newNativeSamplerFixture(t)
			if _, err := f.sample(s, true, 0); err != nil {
				t.Fatal(err)
			}
			m := f.m.DeepCopy()
			m.Status.Phase = phase
			if got := s.Observe(m, f.want, true, 0); got.lagBytes != nil || got.progress != nil {
				t.Fatal("terminal intent retained data")
			}
		})
	}
}

func TestNativeSamplerFreshness(t *testing.T) {
	t.Run("observe during first operation keeps original execution", func(t *testing.T) {
		f, s := newNativeSamplerFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		f.beforeReply = func(lag bool) {
			if lag {
				close(entered)
				<-release
			}
		}
		done := make(chan nativeSample, 1)
		go func() { got, _ := f.sample(s, true, 0); done <- got }()
		<-entered
		if got := s.Observe(f.m, f.want, true, 0); got.lagBytes != nil || got.catchUpConfirmed {
			t.Fatal("pending sample supplied evidence")
		}
		close(release)
		if got := <-done; got.lagBytes == nil || got.progress == nil {
			t.Fatal("cached observation cancelled original execution")
		}
	})
	for _, duration := range []time.Duration{-time.Nanosecond, 20*time.Second + time.Nanosecond} {
		t.Run(duration.String(), func(t *testing.T) {
			f, s := newNativeSamplerFixture(t)
			f.beforeReply = func(lag bool) {
				if lag {
					f.advance(duration)
				}
			}
			got, err := f.sample(s, true, 0)
			if err == nil || got.lagBytes != nil || got.progress != nil {
				t.Fatal("invalid clock or sample duration published data")
			}
		})
	}
	t.Run("unknown newer sample excludes old completion", func(t *testing.T) {
		f, s := newNativeSamplerFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		f.beforeReply = func(lag bool) {
			if lag {
				close(entered)
				<-release
			}
		}
		done := make(chan nativeSample, 1)
		go func() { got, _ := f.sample(s, true, 0); done <- got }()
		<-entered
		if got, err := s.Sample(context.Background(), f.client, f.m, f.want, true, 0, nil); err == nil || got.lagBytes != nil {
			t.Fatal("missing admission was accepted")
		}
		close(release)
		if got := <-done; got.lagBytes != nil || got.progress != nil {
			t.Fatal("older completion replaced unknown")
		}
		if got := s.Observe(f.m, f.want, true, 0); got.lagBytes != nil || got.catchUpConfirmed {
			t.Fatal("older callback revived cache")
		}
	})
	t.Run("parent cancellation removes measurements", func(t *testing.T) {
		f, s := newNativeSamplerFixture(t)
		if _, err := f.sample(s, true, 0); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got, err := s.Sample(ctx, f.client, f.m, f.want, true, 0, f.admit); err == nil || got.lagBytes != nil || got.progress != nil {
			t.Fatal("cancelled caller published data")
		}
		if got := s.Observe(f.m, f.want, true, 0); got.lagBytes != nil {
			t.Fatal("cancelled sample retained previous lag")
		}
	})
}

func TestM9FollowProgressRequiresSettings(t *testing.T) {
	f, s := newNativeSamplerFixture(t)
	f.legacyProgress = true
	if got, err := f.sample(s, true, 0); err == nil || got.progress != nil || got.catchUpConfirmed {
		t.Fatal("follow accepted old progress", err)
	}
}
