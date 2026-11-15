package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// nativeStreamingStatus publishes one complete streaming sample, as attempt 1 does before approval.
func nativeStreamingStatus(t *testing.T) (*nativePublicationFixture, *Sampler, nativeSamplerInput) {
	t.Helper()
	f, s, in := nativeDispatchSampler(t)
	f.m.Spec.Cutover.Mode, f.m.Spec.Cutover.Approved = v1beta1.CutoverManual, false
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	in.migration = f.m.DeepCopy()
	key := client.ObjectKeyFromObject(f.m)
	s.runs[key].nativeInput = &in
	f.lag = 7
	if _, err := s.SampleNativeNow(context.Background(), key, in); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.m); err != nil {
		t.Fatal(err)
	}
	f.m = f.current()
	rs := f.m.Status.Replication
	if f.m.Status.Phase != v1beta1.PhaseStreaming || !meta.IsStatusConditionFalse(f.m.Status.Conditions, v1beta1.ConditionCaughtUp) || rs == nil || rs.LagBytes == nil || *rs.LagBytes != 7 {
		t.Fatalf("a complete streaming sample was not published: %+v", f.m.Status)
	}
	return f, s, in
}

func nativeStatusUnchanged(t *testing.T, want, got *v1beta1.Migration) {
	t.Helper()
	if got.Status.Phase != want.Status.Phase || !apiequality.Semantic.DeepEqual(got.Status.Replication, want.Status.Replication) || !apiequality.Semantic.DeepEqual(got.Status.Progress, want.Status.Progress) {
		t.Fatalf("incomplete sample repainted status: phase %s replication %+v, want %s %+v", got.Status.Phase, got.Status.Replication, want.Status.Phase, want.Status.Replication)
	}
	for _, kind := range []string{v1beta1.ConditionCaughtUp, v1beta1.ConditionStreaming} {
		if !apiequality.Semantic.DeepEqual(meta.FindStatusCondition(got.Status.Conditions, kind), meta.FindStatusCondition(want.Status.Conditions, kind)) {
			t.Fatalf("incomplete sample changed %s", kind)
		}
	}
}

// M7-RUN5 O-2: the pass read the sampler mid-sample, or after its sample failed, and painted Cloning.
func TestNativeReconcileIncompleteSampleKeepsStatus(t *testing.T) {
	for _, sample := range []string{"in flight", "expired", "failed progress", "failed lag"} {
		t.Run(sample, func(t *testing.T) {
			f, s, in := nativeStreamingStatus(t)
			key := client.ObjectKeyFromObject(f.m)
			want := f.m.DeepCopy()
			f.lag = 9
			var finish func()
			switch sample {
			case "in flight":
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				f.mu.Lock()
				f.beforeReply = func(lag bool) {
					if !lag {
						once.Do(func() { close(entered); <-release })
					}
				}
				f.mu.Unlock()
				done := make(chan struct{})
				go func() { _, _ = s.SampleNativeNow(context.Background(), key, in); close(done) }()
				<-entered
				finish = func() { close(release); <-done }
			case "expired":
				f.advance(nativeSampleFreshness + time.Second)
			case "failed progress":
				f.mu.Lock()
				f.phase = "rewinding"
				f.mu.Unlock()
				if _, err := s.SampleNativeNow(context.Background(), key, in); err == nil {
					t.Fatal("malformed progress sampled")
				}
			case "failed lag":
				f.mu.Lock()
				f.replay = true
				f.mu.Unlock()
				if got, err := s.SampleNativeNow(context.Background(), key, in); err == nil || got.phase != nativeProgressStreamingPhase || got.lagBytes != nil {
					t.Fatalf("replayed lag was not a failed lag read: %+v %v", got, err)
				}
			}
			if _, err := nativeDispatch(t, f.r, f.m); err != nil {
				t.Fatal(err)
			}
			nativeStatusUnchanged(t, want, f.current())
			if finish != nil {
				finish()
			}
			// Control: the same pass publishes the next complete sample, so unchanged was a decision.
			f.mu.Lock()
			f.beforeReply, f.replay, f.phase = nil, false, nativePublicationStreamingPhase
			f.mu.Unlock()
			f.m = f.current()
			if _, err := s.SampleNativeNow(context.Background(), key, in); err != nil {
				t.Fatal(err)
			}
			if _, err := nativeDispatch(t, f.r, f.m); err != nil {
				t.Fatal(err)
			}
			if rs := f.current().Status.Replication; rs == nil || rs.LagBytes == nil || *rs.LagBytes != 9 || f.heads != 0 {
				t.Fatalf("the next complete sample was not published: %+v", rs)
			}
		})
	}
}

// M7-RUN5 O-1: the automatic cutover pass returned before CaughtUp=True was recorded.
func TestNativeReconcileCutoverRecordsCaughtUp(t *testing.T) {
	for _, mode := range []v1beta1.CutoverMode{v1beta1.CutoverAutomatic, v1beta1.CutoverManual} {
		t.Run(string(mode), func(t *testing.T) {
			f, s, in := nativeDispatchSampler(t)
			f.m.Spec.Cutover.Mode = mode
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			in.migration = f.m.DeepCopy()
			key := client.ObjectKeyFromObject(f.m)
			s.runs[key].nativeInput = &in
			for range 3 {
				if _, err := s.SampleNativeNow(context.Background(), key, in); err != nil {
					t.Fatal(err)
				}
				f.advance(time.Second)
			}
			var recorded *metav1.Condition
			f.afterExec = func(source bool) {
				if source && recorded == nil {
					recorded = meta.FindStatusCondition(f.current().Status.Conditions, v1beta1.ConditionCaughtUp)
				}
			}
			if _, err := nativeDispatch(t, f.r, f.m); err != nil || f.heads != 1 {
				t.Fatalf("cutover pass did not select E: %v heads=%d", err, f.heads)
			}
			if recorded == nil || recorded.Status != metav1.ConditionTrue || recorded.Reason != reasonLagBelowThreshold {
				t.Fatalf("E was selected before CaughtUp=True was recorded: %+v", recorded)
			}
			got := f.current()
			if c := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ConditionCaughtUp); c == nil || c.Status != metav1.ConditionTrue || got.Status.Phase != v1beta1.PhaseCuttingOver || nativeControlEnd(got) != nativeControlTestEnd {
				t.Fatalf("cutover pass left CaughtUp or E unrecorded: %+v", got.Status)
			}
		})
	}
}

// A new attempt starts from its own measurements, since an incomplete sample no longer clears the last attempt's.
func TestNativeRetryResetsMeasurements(t *testing.T) {
	f := newNativeDispositionFixture(t, false)
	lag := int64(5)
	f.m.Status.Progress = &v1beta1.CloneProgress{TablesDone: 3}
	f.m.Status.Replication.WriteLSN, f.m.Status.Replication.ReplayLSN, f.m.Status.Replication.LagBytes = nativeControlLaterEnd, nativeControlTestEnd, &lag
	(&MigrationReconciler{}).setCondition(f.m, v1beta1.ConditionCaughtUp, metav1.ConditionTrue, reasonLagBelowThreshold, "attempt 1")
	(&MigrationReconciler{}).setCondition(f.m, v1beta1.ConditionStreaming, metav1.ConditionTrue, "NativeProgress", "attempt 1")
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := f.reconcile(t); err != nil {
			t.Fatalf("retry pass %d: %v", i, err)
		}
	}
	live := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil {
		t.Fatal(err)
	}
	if live.Status.Attempts != 2 || live.Status.Progress != nil || !apiequality.Semantic.DeepEqual(live.Status.Replication, &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}) {
		t.Fatalf("retry kept the last attempt's measurements or lost saved E: %+v", live.Status)
	}
	if meta.FindStatusCondition(live.Status.Conditions, v1beta1.ConditionCaughtUp) != nil || !meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionStreaming) {
		t.Fatal("retry kept the last attempt's CaughtUp or dropped the Streaming the retry gate reads")
	}
}
