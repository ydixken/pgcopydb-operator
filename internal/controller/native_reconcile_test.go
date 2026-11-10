package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/client-go/rest"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const nativeReconcileWorkerReplacementCase = "recorded worker replacement"

type nativeReconcileConsumer interface {
	reconcileNative(context.Context, *v1beta1.Migration, *v1beta1.Migration) (ctrl.Result, error)
}

func nativeDispatch(t *testing.T, r *MigrationReconciler, m *v1beta1.Migration) (ctrl.Result, error) {
	t.Helper()
	consumer, ok := any(r).(nativeReconcileConsumer)
	if !ok {
		t.Fatal("native reconciliation handler is missing")
	}
	return consumer.reconcileNative(context.Background(), m, m.DeepCopy())
}

func TestNativeReconcileEntry(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	result, err := nativeDispatch(t, f.r, f.m)
	if err != nil || result.RequeueAfter <= 0 || f.patches != 1 || f.creates != 0 || f.storedMigration().Status.Phase != v1beta1.PhasePending {
		t.Fatalf("first native pass did not persist only Pending: %+v %v", result, err)
	}
}

func TestNativeReconcileCachedReplacement(t *testing.T) {
	for _, change := range []string{workerReadMigration, workerSpecCase, "receipts"} {
		t.Run(change, func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			f.changeMigration(func(m *v1beta1.Migration) {
				switch change {
				case workerReadMigration:
					m.UID = probeMigrationReplacement
				case workerSpecCase:
					m.Spec.Suspend = true
				case "receipts":
					m.Status.NativeJobUID = string(workerReplacementUID)
				}
			})
			_, err := nativeDispatch(t, f.r, f.m)
			if err == nil || f.creates != 0 || f.patches != 0 {
				t.Fatal("cached hint admitted changed live identity")
			}
		})
	}
}

func TestNativeReconcileSuccessfulClone(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	if _, err := nativeDispatch(t, f.r, f.m); err != nil {
		t.Fatal(err)
	}
	stored := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || !meta.IsStatusConditionTrue(stored.Status.Conditions, v1beta1.ConditionComplete) || nativeControlEnd(stored) != "" || f.logs != 1 {
		t.Fatal("actual successful clone did not consume complete original clone output")
	}
}

func TestNativeReconcileUnrecordedWorker(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	worker := f.expected()
	worker.UID = workerServerUID
	if err := f.store.Create(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	_, err := nativeDispatch(t, f.r, f.m)
	if err == nil || f.creates != 0 || f.patches != 0 {
		t.Fatal("unrecorded canonical worker was adopted")
	}
}

type nativeDispatchProbeReader struct{ preflightFixtureReader }

func (r nativeDispatchProbeReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
		return r.f.store.Get(ctx, key, obj, opts...)
	}
	return r.preflightFixtureReader.Get(ctx, key, obj, opts...)
}

func TestNativeReconcileStart(t *testing.T) {
	f := newPreflightFixture(t)
	f.m.Status.Phase = v1beta1.PhasePending
	meta.SetStatusCondition(&f.m.Status.Conditions, metav1.Condition{Type: v1beta1.ConditionValidated, Status: metav1.ConditionTrue, Reason: "PriorPreflight", ObservedGeneration: f.m.Generation})
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.r.Recorder = events.NewFakeRecorder(20)
	f.r.APIReader = nativeDispatchProbeReader{preflightFixtureReader{f}}
	workers, patches := 0, 0
	f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Fatal("native startup used cached GET")
			return errBoom
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			switch o := obj.(type) {
			case *corev1.PersistentVolumeClaim:
				o.UID = "native-dispatch-pvc"
				return c.Create(ctx, o, opts...)
			case *batchv1.Job:
				if o.Name == jobName(f.m, 1) {
					if f.logs.Load() != 2 || patches != 0 {
						t.Fatal("worker credentials projected before actual active preflight")
					}
					workers++
					o.UID = workerServerUID
					return c.Create(ctx, o, opts...)
				}
			}
			return f.createJob(ctx, c, obj, opts...)
		},
		Delete: f.deleteJob,
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			patches++
			return c.Status().Patch(ctx, obj, p, opts...)
		},
	})
	if result, err := nativeDispatch(t, f.r, f.m); err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("native startup failed: %+v %v", result, err)
	}
	stored := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || workers != 1 || patches != 1 || stored.Status.Attempts != 1 || stored.Status.NativeJobUID != string(workerServerUID) || stored.Status.NativePVCUID != "native-dispatch-pvc" || stored.Status.JobName != jobName(f.m, 1) {
		t.Fatal("startup did not atomically persist actual worker/PVC receipt and attempt")
	}
}

func TestNativeReconcileSuccessfulFollowRequiresAuthority(t *testing.T) {
	f := newNativeCloneFixture(t, true, nil)
	if _, err := nativeDispatch(t, f.r, f.m); err == nil {
		t.Fatal("follow succeeded from worker/clone data without target completion authority")
	}
	stored := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || meta.IsStatusConditionTrue(stored.Status.Conditions, v1beta1.ConditionComplete) {
		t.Fatal("missing follow authority marked Complete")
	}
}

func TestNativeReconcileFirstFinalizerPatchReturns(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	f.m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.m.Status.Phase = v1beta1.PhasePending
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	patches := 0
	f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
		patches++
		return c.Patch(ctx, obj, p, opts...)
	}, Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
		t.Fatal("first finalizer patch continued to creation")
		return errBoom
	}})
	if _, err := nativeDispatch(t, f.r, f.m); err != nil || patches != 1 || f.patches != 0 {
		t.Fatal("native finalizer update crossed a patch boundary", err)
	}
	stored := f.storedMigration()
	if len(stored.Finalizers) != 1 || stored.Finalizers[0] != finalizerName || stored.Status.ObservedGeneration != 0 || meta.FindStatusCondition(stored.Status.Conditions, v1beta1.ConditionValidated) != nil {
		t.Fatal("native finalizer pass changed status")
	}
}

func nativeDispatchSampler(t *testing.T) (*nativePublicationFixture, *Sampler, nativeSamplerInput) {
	t.Helper()
	f, s, in := nativePublication(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		commands := r.URL.Query()["command"]
		if strings.HasSuffix(r.URL.Path, "/exec") {
			switch {
			case slices.Contains(commands, "source_head"), slices.Contains(commands, "set_endpos"):
				f.exec(w, r)
			case slices.Contains(commands, "progress"):
				f.progress(w, r)
			default:
				f.execSample(w, r)
			}
			return
		}
		f.serve(w, r)
	}))
	t.Cleanup(server.Close)
	transport, err := podexec.New(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	in.control = native.NewClient(transport)
	s.runs[client.ObjectKeyFromObject(f.m)].nativeInput = &in
	f.r.NativeControl, f.r.Sampler = in.control, s
	return f, s, in
}

func TestNativeReconcileFirstEnd(t *testing.T) {
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
			for i := range 3 {
				got, err := s.SampleNativeNow(context.Background(), key, in)
				if err != nil || got.catchUpConfirmed != (i == 2) {
					t.Fatalf("actual lag sample %d failed: %v", i, err)
				}
				f.advance(time.Second)
			}
			f.afterExec = func(source bool) {
				if source && s.running(key) {
					t.Error("native sampler remained active during E selection")
				}
			}
			if _, err := nativeDispatch(t, f.r, f.m); err != nil || f.heads != 1 || f.sets != 0 || f.patches != 1 || nativeControlEnd(f.current()) != nativeControlTestEnd || s.running(key) {
				t.Fatalf("first E crossed a persistence/effect boundary: %v heads=%d sets=%d patches=%d", err, f.heads, f.sets, f.patches)
			}
			if nativeControlEnd(f.m) != "" || meta.IsStatusConditionTrue(f.current().Status.Conditions, v1beta1.ConditionCutoverComplete) {
				t.Fatal("first E updated pre-E snapshot or supplied completion")
			}
		})
	}
}

func TestNativeReconcileSavedEndWithoutSampler(t *testing.T) {
	f := nativeFollowFlowFixture(t)
	f.saveEnd(nativeControlTestEnd)
	f.head, f.fault = nativeControlLaterEnd, nativeControlDisconnectFault
	for range 2 {
		if _, err := nativeDispatch(t, f.r, f.m); !errors.Is(err, native.ErrNativeSetEndposUnknownEffect) {
			t.Fatal("saved E setter did not run with unknown catch-up and stopped sampler", err)
		}
		f.m = f.current()
	}
	if f.heads != 0 || f.sets != 2 || f.patches != 0 || !slices.Equal(f.sentEnds, []string{nativeControlTestEnd, nativeControlTestEnd}) || nativeControlEnd(f.current()) != nativeControlTestEnd {
		t.Fatal("saved E was resampled, re-gated or overwritten")
	}
}

func TestNativeReconcileUnknownProgress(t *testing.T) {
	f := nativeFollowFlowFixture(t)
	f.r.Sampler = nil
	f.m.Status.Progress = &v1beta1.CloneProgress{TablesDone: 7}
	f.m.Status.Replication = &v1beta1.ReplicationStatus{LagBytes: new(int64)}
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.m); err != nil {
		t.Fatal(err)
	}
	got := f.current()
	if got.Status.Progress != nil || got.Status.Replication.LagBytes != nil || got.Status.Replication.WriteLSN != "" || got.Status.Replication.ReplayLSN != "" || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionCaughtUp) || f.heads != 0 || f.sets != 0 {
		t.Fatal("unknown native progress retained old measurements or drove cutover")
	}
}

func TestNativeReconcileRetainedIdentity(t *testing.T) {
	for _, change := range []string{"missing worker", "missing PVC", nativeReconcileWorkerReplacementCase, "replaced PVC", "unrecorded receipts", "changed canonical worker"} {
		t.Run(change, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, nil)
			switch change {
			case "missing worker":
				if err := f.store.Delete(context.Background(), f.worker); err != nil {
					t.Fatal(err)
				}
			case "missing PVC":
				if err := f.store.Delete(context.Background(), f.pvc); err != nil {
					t.Fatal(err)
				}
			case nativeReconcileWorkerReplacementCase, "changed canonical worker":
				worker := f.worker.DeepCopy()
				if change == nativeReconcileWorkerReplacementCase {
					worker.UID = probeReplacement
				} else {
					worker.Spec.Template.Spec.HostPID = true
				}
				if err := f.store.Update(context.Background(), worker); err != nil {
					t.Fatal(err)
				}
			case "replaced PVC":
				claim := f.pvc.DeepCopy()
				claim.UID = probeReplacement
				if err := f.store.Update(context.Background(), claim); err != nil {
					t.Fatal(err)
				}
			case "unrecorded receipts":
				f.m.Status.NativeJobUID, f.m.Status.NativePVCUID = "", ""
				if err := f.store.Status().Update(context.Background(), f.m); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := nativeDispatch(t, f.r, f.m); err == nil || f.logs != 0 || f.patches != 0 {
				t.Fatalf("unadmitted retained execution reached output/status: %v logs=%d patches=%d", err, f.logs, f.patches)
			}
		})
	}
}

func TestNativeUnreceiptedTerminalRejoin(t *testing.T) {
	for _, condition := range []string{v1beta1.ConditionComplete, v1beta1.ConditionFailed} {
		t.Run(condition, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, nil)
			f.m.Status.Phase = v1beta1.PhaseFailed
			if condition == v1beta1.ConditionComplete {
				f.m.Status.Phase = v1beta1.PhaseCompleted
			}
			meta.SetStatusCondition(&f.m.Status.Conditions, metav1.Condition{Type: condition, Status: metav1.ConditionTrue, Reason: "StaleDisplay"})
			if err := f.store.Status().Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if _, err := nativeDispatch(t, f.r, f.m); err != nil || f.logs != 1 {
				t.Fatalf("stale display bypassed actual terminal clone consumer: %v logs=%d", err, f.logs)
			}
			stored := &v1beta1.Migration{}
			if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || !meta.IsStatusConditionTrue(stored.Status.Conditions, v1beta1.ConditionComplete) || meta.IsStatusConditionTrue(stored.Status.Conditions, v1beta1.ConditionFailed) {
				t.Fatal("fresh clone completion retained contradictory terminal display")
			}
		})
	}
}

func TestNativeReconcileCanceledRunning(t *testing.T) {
	f := nativeFollowFlowFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.r.reconcileNativeRunning(ctx, f.m, f.m.DeepCopy(), f.worker, f.pvc); err == nil || f.patches != 0 || f.heads != 0 || f.sets != 0 {
		t.Fatal("canceled native pass sampled, patched or set E")
	}
}

func TestNativeReconcilePreflightPatchReturns(t *testing.T) {
	f := newPreflightFixture(t)
	f.m.Status.Phase = v1beta1.PhasePending
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.r.APIReader = nativeDispatchProbeReader{preflightFixtureReader{f}}
	patches := 0
	f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{Create: f.createJob, Delete: f.deleteJob,
		SubResourcePatch: func(ctx context.Context, c client.Client, _ string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			patches++
			return c.Status().Patch(ctx, obj, p, opts...)
		},
	})
	if _, err := nativeDispatch(t, f.r, f.m); err != nil || patches != 1 || f.creates != 2 || f.logs.Load() != 2 {
		t.Fatalf("preflight result patch continued into worker creation: %v patches=%d jobs=%d", err, patches, f.creates)
	}
	stored := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || stored.Status.Attempts != 0 || stored.Status.NativeJobUID != "" || !meta.IsStatusConditionTrue(stored.Status.Conditions, v1beta1.ConditionValidated) {
		t.Fatal("preflight status patch acquired an execution receipt")
	}
}

func TestNativeReconcileRunningStatusConflict(t *testing.T) {
	f := nativeFollowFlowFixture(t)
	live := f.current()
	live.Status.Progress = &v1beta1.CloneProgress{TablesDone: 7}
	if err := f.store.Status().Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.reconcileNativeRunning(context.Background(), f.m, f.m.DeepCopy(), f.worker, f.pvc); !apierrors.IsConflict(err) || f.patches != 0 || f.heads != 0 || f.sets != 0 {
		t.Fatal("live status conflict was hidden or crossed into native effects", err)
	}
}

func TestNativeReconcileSuccessfulFollow(t *testing.T) {
	f := newNativeFinishFixture(t)
	if _, err := nativeDispatch(t, f.r, f.m); err != nil {
		t.Fatal(err)
	}
	assertNativeFinished(t, f)
	if !f.cleaned || !slices.Contains(f.steps, finishOrdinaryEvidenceKind) || !slices.Contains(f.steps, finishCleanedEvidenceKind) {
		t.Fatal("follow dispatch bypassed actual target, cleanup or fresh final authority")
	}
}

func TestNativeFinalizedTerminalReplay(t *testing.T) {
	for _, edit := range []string{"new process", "later generation", probeSuspendCase, "approval", "verification edit", "runner", "missing credentials"} {
		t.Run(edit, func(t *testing.T) {
			f := newFinalizedCleanupFixture(t)
			switch edit {
			case "later generation":
				f.m.Generation++
			case probeSuspendCase:
				f.m.Spec.Suspend = true
			case "approval":
				f.m.Spec.Cutover.Approved = true
			case "verification edit":
				f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
			case "runner":
				f.m.Spec.Runner.Image = "unavailable-current-runner"
			case "missing credentials":
				f.m.Spec.Source, f.m.Spec.Target = v1beta1.PostgresConnection{}, v1beta1.PostgresConnection{}
			}
			f.save()
			before := f.m.Status.DeepCopy()
			key := client.ObjectKeyFromObject(f.m)
			f.r.Sampler = heldSampler(&fakeProgress{}, key, f.m.Status.JobName)
			if _, err := nativeDispatch(t, f.r, f.m); err != nil || f.r.Sampler.running(key) || len(f.deletes) != 0 {
				t.Fatal("historical terminal state required current resources or left sampling active", err)
			}
			stored := &v1beta1.Migration{}
			if f.store.Get(context.Background(), key, stored) != nil || !apiequality.Semantic.DeepEqual(stored.Status, *before) {
				t.Fatal("terminal replay rewrote historical status")
			}
		})
	}
}

func TestNativeFinalizationReceiptConflict(t *testing.T) {
	f := newFinalizedCleanupFixture(t)
	f.m.Status.NativeFinalizedJobUID = "different-worker"
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.r.Scheme = f.store.Scheme()
	before := f.m.Status.DeepCopy()
	if _, err := nativeDispatch(t, f.r, f.m); err == nil || err.Error() != "NativeFinalizationReceiptConflict" {
		t.Fatal("conflicting finalization receipt did not fail closed with fixed reason", err)
	}
	stored := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || !apiequality.Semantic.DeepEqual(stored.Status, *before) {
		t.Fatal("conflicting marker was cleared or overwritten")
	}
}

func TestNativeFinalizedTerminalDeletion(t *testing.T) {
	f := newNativeHistoricalDeletionFixture(t)
	before := f.m.Status.DeepCopy()
	for _, obj := range []client.Object{f.worker, f.pod, f.pvc} {
		if err := f.store.Delete(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := nativeDispatch(t, f.r, f.m); err != nil || result.RequeueAfter != 0 {
		t.Fatal("finalized deletion entered active resource or PostgreSQL admission", result, err)
	}
	stored := f.current()
	if !apiequality.Semantic.DeepEqual(stored.Status, *before) || slices.Contains(stored.Finalizers, finalizerName) || f.metadataPatches != 1 {
		t.Fatal("finalized deletion changed history or failed to release finalizer")
	}
}

func TestNativeFinalizedTerminalCleanupPending(t *testing.T) {
	f := newFinalizedCleanupFixture(t, compareSchema)
	f.r.Scheme = f.store.Scheme()
	f.unknownDelete = true
	before := f.m.Status.DeepCopy()
	if result, err := nativeDispatch(t, f.r, f.m); err == nil && result.RequeueAfter <= 0 {
		t.Fatal("unknown helper cleanup was silently ignored")
	}
	stored := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || !apiequality.Semantic.DeepEqual(stored.Status, *before) || !nativeFinalizationRecorded(stored) {
		t.Fatal("cleanup error revoked durable completion")
	}
}

func TestNativeUnreceiptedTerminalUnknown(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	f.m.Status.Phase = v1beta1.PhaseCompleted
	meta.SetStatusCondition(&f.m.Status.Conditions, metav1.Condition{Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "StaleDisplay"})
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.raw = []byte("unavailable completed report\n")
	if _, err := nativeDispatch(t, f.r, f.m); err == nil || f.patches != 0 || f.current().Status.NativeFinalizedJobUID != "" {
		t.Fatal("stale generic Complete gained finalization without actual complete output", err)
	}
}

func TestNativeIncompleteFinalizationRejoin(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	f.m.Status.NativeFinalizedJobUID = f.m.Status.NativeJobUID
	f.m.Status.Phase = v1beta1.PhaseVerifying
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.m); err != nil || f.logs != 1 || !nativeFinalizationRecorded(f.current()) {
		t.Fatal("incomplete matching marker bypassed or refused fresh proof rejoin", err)
	}
}

func TestNativeFinalizedTerminalPinnedHelpers(t *testing.T) {
	f := newFinalizedCleanupFixture(t, compareSchema, compareData)
	f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}
	f.m.Status.Verification = []v1beta1.VerificationResult{{Check: compareSchema, Passed: false}}
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	before := f.m.Status.DeepCopy()
	if _, err := nativeDispatch(t, f.r, f.m); err != nil || len(f.deletes) != 2 {
		t.Fatal("history did not dispose exact pinned helpers independently of current options", err)
	}
	stored := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || !apiequality.Semantic.DeepEqual(stored.Status, *before) {
		t.Fatal("helper disposal changed historical E, results or pins")
	}
}
