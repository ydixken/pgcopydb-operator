package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	nativePublicationStreamingPhase = "streaming"
	nativePublicationCopyingPhase   = "copying"
	nativePublicationThresholdCase  = "lag allowance change"
	nativePublicationReceiptCase    = "receipt"
)

type nativePublicationFixture struct {
	*nativeSamplerFixture
	phase string
	copy  *native.CopyProgress
}

func nativePublication(t *testing.T) (*nativePublicationFixture, *Sampler, nativeSamplerInput) {
	t.Helper()
	f, _ := newNativeSamplerFixture(t)
	zero := int64(0)
	p := &nativePublicationFixture{nativeSamplerFixture: f, phase: nativePublicationStreamingPhase, copy: &native.CopyProgress{Tables: &native.Counters{Total: &zero, Done: &zero}, Indexes: &native.Counters{Total: &zero, Done: &zero}, Bytes: &native.Counters{Total: &zero, Done: &zero}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/exec") && slices.Contains(r.URL.Query()["command"], "progress") {
			p.progress(w, r)
			return
		}
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
	in := nativeSamplerInput{migration: f.m.DeepCopy(), execution: f.want, control: f.client, threshold: maxCatchupLagBytes(f.m), admit: f.admit}
	s := NewSampler(&fakeProgress{})
	s.ctx = context.Background()
	run := &samplerRun{job: f.worker.Name, cancel: func() {}, nativeInput: &in}
	run.native.now = func() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.now }
	s.runs[client.ObjectKeyFromObject(f.m)] = run
	t.Cleanup(func() { s.Stop(client.ObjectKeyFromObject(f.m)); metrics.Forget(f.m.Namespace, f.m.Name) })
	return p, s, in
}

func (f *nativePublicationFixture) progress(w http.ResponseWriter, r *http.Request) {
	c, err := (&websocket.Upgrader{Subprotocols: []string{nativeCleanupTestProtocol}}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = c.Close() }()
	for {
		_, raw, readErr := c.ReadMessage()
		if readErr != nil {
			return
		}
		if bytes.Equal(raw, []byte{255, 0}) {
			break
		}
	}
	f.mu.Lock()
	report := native.Report{ProtocolVersion: 1, EngineVersion: "1", Command: "list progress", Capabilities: []native.Capability{native.CapProgress}, Work: &f.want.Work, Phase: f.phase, Copy: cloneNativeCopyProgress(f.copy)}
	if followEnabled(f.m) {
		nativeM9Status(&report)
	}
	hook := f.beforeReply
	f.mu.Unlock()
	raw, err := json.Marshal(report)
	if err != nil {
		f.t.Error(err)
		return
	}
	if hook != nil {
		hook(false)
	}
	out := append(append([]byte("native-report: "), raw...), '\n')
	_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{1}, out...))
	_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{3}, []byte(`{"status":"Success"}`)...))
	_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

func TestNativeSamplerIntegration(t *testing.T) {
	f, s, in := nativePublication(t)
	key := client.ObjectKeyFromObject(f.m)
	for i := range 3 {
		got, err := s.SampleNativeNow(context.Background(), key, in)
		if err != nil || got.progress == nil || got.lagBytes == nil || got.observedAt != f.now || got.catchUpConfirmed != (i == 2) {
			t.Fatalf("sample %d did not require the latch then two later lag observations: %+v, %v", i, got, err)
		}
		for range 3 {
			observed, ok := s.ObserveNative(key, in)
			if !ok || observed.catchUpConfirmed != got.catchUpConfirmed || observed.observedAt != got.observedAt {
				t.Fatal("cached reads changed qualification or measured time")
			}
		}
		f.advance(time.Second)
	}
	if p := s.progress.(*fakeProgress); p.counts() != 0 {
		t.Fatal("native sampler used legacy progress SQL")
	}
}

func TestNativeSamplerPublicationUnknown(t *testing.T) {
	f, s, in := nativePublication(t)
	key := client.ObjectKeyFromObject(f.m)
	if _, err := s.SampleNativeNow(context.Background(), key, in); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.replay = true
	f.mu.Unlock()
	got, err := s.SampleNativeNow(context.Background(), key, in)
	if err == nil || got.lagBytes != nil || got.progress == nil || got.catchUpConfirmed {
		t.Fatal("unknown lag became known or discarded separately checked progress")
	}
	if _, found, err := readGauge("pgcopydb_migration_replication_lag_bytes", map[string]string{testNamespaceLabel: f.m.Namespace, lifecycleNameKey: f.m.Name}); err != nil || found {
		t.Fatal("unknown native lag retained a gauge", err)
	}
	s.Stop(key)
	if got, err := s.SampleNativeNow(context.Background(), key, in); err == nil || got.progress != nil || s.running(key) {
		t.Fatal("synchronous sampling resurrected a stopped run")
	}
}

func TestNativeSamplerPublicationLate(t *testing.T) {
	for _, action := range []string{"stop", "run replacement", "cancellation", "newer unknown"} {
		t.Run(action, func(t *testing.T) {
			f, s, in := nativePublication(t)
			key := client.ObjectKeyFromObject(f.m)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			f.beforeReply = func(lag bool) {
				if lag {
					once.Do(func() { close(entered) })
					<-release
				}
			}
			done := make(chan nativeSample, 1)
			go func() { got, _ := s.SampleNativeNow(ctx, key, in); done <- got }()
			<-entered
			f.mu.Lock()
			f.beforeReply = nil
			f.mu.Unlock()
			switch action {
			case "stop":
				s.Stop(key)
				metrics.Forget(key.Namespace, key.Name)
			case "run replacement":
				s.mu.Lock()
				s.stopLocked(key)
				replacement := in
				replacement.threshold++
				s.runs[key] = &samplerRun{job: f.worker.Name, cancel: func() {}, nativeInput: &replacement}
				s.mu.Unlock()
			case "cancellation":
				cancel()
			case "newer unknown":
				f.mu.Lock()
				f.replay = true
				f.mu.Unlock()
				if _, err := s.SampleNativeNow(ctx, key, in); err == nil {
					t.Fatal("fixture did not produce an unknown newer sample")
				}
			}
			close(release)
			if got := <-done; got.progress != nil || got.lagBytes != nil || got.catchUpConfirmed {
				t.Fatal("late callback published DATA")
			}
			if _, found, err := readGauge("pgcopydb_migration_replication_lag_bytes", map[string]string{testNamespaceLabel: key.Namespace, lifecycleNameKey: key.Name}); err != nil || found {
				t.Fatal("late callback recreated lag gauge", err)
			}
		})
	}
}

func TestNativeSamplerPublicationLiveAdmission(t *testing.T) {
	for _, replacement := range []string{"migration replacement", lifecycleJobKind, "claim replacement", "pod replacement", nativePublicationReceiptCase, nativePublicationThresholdCase} {
		t.Run(replacement, func(t *testing.T) {
			f, s, in := nativePublication(t)
			key := client.ObjectKeyFromObject(f.m)
			if _, err := s.SampleNativeNow(context.Background(), key, in); err != nil {
				t.Fatal(err)
			}
			var obj client.Object
			switch replacement {
			case "migration replacement", nativePublicationReceiptCase, nativePublicationThresholdCase:
				m := f.m.DeepCopy()
				if replacement == nativePublicationReceiptCase {
					m.Status.NativeJobUID = string(probeReplacement)
					if err := f.store.Status().Update(context.Background(), m); err != nil {
						t.Fatal(err)
					}
				} else {
					if replacement == nativePublicationThresholdCase {
						m.Spec.Follow.MaxCatchupLag = resource.NewQuantity(1, resource.BinarySI)
					} else {
						m.Generation++
					}
					if err := f.store.Update(context.Background(), m); err != nil {
						t.Fatal(err)
					}
				}
			case lifecycleJobKind:
				obj = f.worker.DeepCopy()
			case "claim replacement":
				obj = f.pvc.DeepCopy()
			case "pod replacement":
				obj = f.pod.DeepCopy()
			}
			if obj != nil {
				if err := f.store.Delete(context.Background(), obj); err != nil {
					t.Fatal(err)
				}
				obj.SetUID(types.UID("native-publication-replacement"))
				obj.SetResourceVersion("")
				if err := f.store.Create(context.Background(), obj); err != nil {
					t.Fatal(err)
				}
			}
			got, ok := s.ObserveNative(key, in)
			if !ok || got.progress != nil || got.lagBytes != nil || got.catchUpConfirmed || s.running(key) {
				t.Fatal("cached DATA survived actual APIReader replacement")
			}
		})
	}
}

func TestNativeSamplerPublicationMeasuredBaseCopy(t *testing.T) {
	zero, one := int64(0), int64(1)
	for name, got := range map[string]nativeSample{
		"missing":       {},
		"copying zeros": {phase: nativePublicationCopyingPhase, progress: &native.CopyProgress{Tables: &native.Counters{Total: &zero, Done: &zero}, Indexes: &native.Counters{Total: &zero, Done: &zero}, Bytes: &native.Counters{Total: &zero, Done: &zero}}},
		"missing done":  {phase: nativePublicationStreamingPhase, progress: &native.CopyProgress{Tables: &native.Counters{Total: &zero}, Indexes: &native.Counters{Total: &zero, Done: &zero}, Bytes: &native.Counters{Total: &zero, Done: &zero}}},
		"incomplete":    {phase: nativePublicationStreamingPhase, progress: &native.CopyProgress{Tables: &native.Counters{Total: &one, Done: &zero}, Indexes: &native.Counters{Total: &zero, Done: &zero}, Bytes: &native.Counters{Total: &zero, Done: &zero}}},
	} {
		t.Run(name, func(t *testing.T) {
			if nativeBaseCopyReady(got) {
				t.Fatal("unmeasured or incomplete base copy became ready")
			}
		})
	}
	got := nativeSample{phase: nativePublicationStreamingPhase, progress: &native.CopyProgress{Tables: &native.Counters{Total: &zero, Done: &zero}, Indexes: &native.Counters{Total: &zero, Done: &zero}, Bytes: &native.Counters{Total: &zero, Done: &zero}}}
	if !nativeBaseCopyReady(got) {
		t.Fatal("explicit measured zero base copy stayed unknown")
	}
}

func TestNativeSamplerPublicationStatusIsNotData(t *testing.T) {
	f, s, in := nativePublication(t)
	key := client.ObjectKeyFromObject(f.m)
	f.mu.Lock()
	f.phase = nativePublicationCopyingPhase
	f.mu.Unlock()
	in.migration.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionCloneCompleted, Status: metav1.ConditionTrue, Reason: "Copied"}}
	in.migration.Status.Progress = &v1beta1.CloneProgress{}
	for range 3 {
		if got, err := s.SampleNativeNow(context.Background(), key, in); err != nil || got.catchUpConfirmed {
			t.Fatal("persisted status restored a native base-copy latch", err)
		}
	}
	s.mu.Lock()
	latch := s.runs[key].nativeBaseCopyComplete
	s.mu.Unlock()
	if latch {
		t.Fatal("copying counters latched completed base copy")
	}
	f.mu.Lock()
	f.phase = nativePublicationStreamingPhase
	f.mu.Unlock()
	for range 3 {
		if _, err := s.SampleNativeNow(context.Background(), key, in); err != nil {
			t.Fatal(err)
		}
	}
	in.migration.ResourceVersion = "newer-status-only"
	got, ok := s.ObserveNative(key, in)
	if !ok || !got.catchUpConfirmed {
		t.Fatal("status-only resource version changed execution qualification")
	}
	prior := got.observedAt
	f.advance(time.Second)
	got, ok = s.ObserveNative(key, in)
	if !ok || got.observedAt != prior {
		t.Fatal("cache consumption manufactured a fresh measurement")
	}
	f.advance(nativeSampleFreshness)
	got, _ = s.ObserveNative(key, in)
	if got.progress != nil || got.lagBytes != nil || !got.observedAt.IsZero() || got.catchUpConfirmed {
		t.Fatal("expired DATA retained measurements or a timestamp")
	}
}

func TestNativeSamplerPublicationLoop(t *testing.T) {
	f, s, in := nativePublication(t)
	key := client.ObjectKeyFromObject(f.m)
	s.Stop(key)
	s.ctx = nil
	if got, ok := s.ObserveNative(key, in); ok || got.progress != nil {
		t.Fatal("unstarted sampler supplied native DATA")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.ctx != nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.ObserveNative(key, in); !ok {
		t.Fatal("manager did not start native run")
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		got, ok := s.ObserveNative(key, in)
		return ok && got.progress != nil, nil
	}); err != nil {
		t.Fatal("background loop did not publish checked native DATA", err)
	}
	if p := s.progress.(*fakeProgress); p.counts() != 0 {
		t.Fatal("background native run executed legacy SQL")
	}
	cancel()
	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		return !s.running(key), nil
	}); err != nil {
		t.Fatal("manager cancellation retained native run", err)
	}
}

func TestNativeSamplerPublicationConsumer(t *testing.T) {
	f, s, in := nativePublication(t)
	key := client.ObjectKeyFromObject(f.m)
	f.r.NativeControl, f.r.Sampler = f.client, s
	if _, err := s.SampleNativeNow(context.Background(), key, in); err != nil {
		t.Fatal(err)
	}
	got, running, err := f.r.observeNativeProgress(context.Background(), f.m, f.worker, f.pvc)
	if err != nil || !running || got.progress == nil {
		t.Fatal("controller did not consume current checked DATA", err)
	}
	m := f.m.DeepCopy()
	m.Status.NativeJobUID = string(probeReplacement)
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	got, _, err = f.r.observeNativeProgress(context.Background(), f.m, f.worker, f.pvc)
	if err == nil || got.progress != nil || s.running(key) {
		t.Fatal("controller consumed DATA after live admission was lost")
	}
}

func TestNativeSamplerPublicationFinalAdmissionFreshness(t *testing.T) {
	f, s, in := nativePublication(t)
	key := client.ObjectKeyFromObject(f.m)
	reads := 0
	in.admit = func(ctx context.Context) (native.ExecutionData, error) {
		reads++
		if reads == 5 {
			f.advance(nativeSampleFreshness + time.Nanosecond)
		}
		return f.admit(ctx)
	}
	got, _ := s.SampleNativeNow(context.Background(), key, in)
	if reads != 5 || got.progress != nil || got.lagBytes != nil || !got.observedAt.IsZero() || got.catchUpConfirmed {
		t.Fatal("slow final admission published expired measurements")
	}
}

func TestNativeSamplerPublicationNoMeasurements(t *testing.T) {
	f, s, in := nativePublication(t)
	f.copy = &native.CopyProgress{}
	got, err := s.SampleNativeNow(context.Background(), client.ObjectKeyFromObject(f.m), in)
	if err != nil || got.progress == nil || !got.observedAt.IsZero() || got.catchUpConfirmed {
		t.Fatal("missing native counters gained a measurement timestamp", err)
	}
}
