package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNativePersistedEndOrdering(t *testing.T) {
	t.Run("patch then return then later same E setter", testNativeEndTwoReconciliations)
	for _, mode := range []string{nativeControlConflictCase, nativeControlLostPatchCase, nativeControlSourceCrashCase} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeControlFixture(t)
			switch mode {
			case nativeControlConflictCase:
				f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
					winner := f.current()
					winner.Status.Replication = &v1beta1.ReplicationStatus{Endpos: nativeControlLaterEnd}
					if f.store.Status().Update(ctx, winner) != nil {
						t.Fatal("winner fixture failed")
					}
					return c.Status().Patch(ctx, obj, p)
				}
			case nativeControlLostPatchCase:
				f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
					if c.Status().Patch(ctx, obj, p) != nil {
						t.Fatal("unknown patch fixture failed")
					}
					return errors.New(nativeControlCanary)
				}
			case nativeControlSourceCrashCase:
				f.afterExec = func(source bool) {
					if source {
						m := f.current()
						m.Generation++
						if f.store.Update(context.Background(), m) != nil {
							t.Fatal("generation fixture failed")
						}
					}
				}
			}
			_, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
			if err == nil || f.heads != 1 || f.sets != 0 || strings.Contains(err.Error(), nativeControlCanary) {
				t.Fatal("failed persistence reached setter or exposed diagnostics")
			}
			if mode == nativeControlLostPatchCase && nativeControlEnd(f.current()) != nativeControlTestEnd {
				t.Fatal("unknown patch did not retain server saved E")
			}
			if mode == nativeControlConflictCase && nativeControlEnd(f.current()) != nativeControlLaterEnd {
				t.Fatal("conflict erased winning E")
			}
			oldHeads := f.heads
			_, _ = f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
			if f.heads != oldHeads || f.sets != 0 {
				t.Fatal("stale failed invocation resampled or set E")
			}
		})
	}
}

func TestNativeEndPersistencePublishesCuttingOver(t *testing.T) {
	f := newNativeControlFixture(t)
	before := f.m.DeepCopy()
	expected := before.DeepCopy()
	expected.Status.Replication = &v1beta1.ReplicationStatus{Endpos: nativeControlTestEnd}
	expected.Status.Phase = v1beta1.PhaseCuttingOver
	expected.Status.ObservedGeneration = expected.Generation
	f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
		m, ok := obj.(*v1beta1.Migration)
		if !ok || nativeControlEnd(m) != nativeControlTestEnd || m.Status.Phase != v1beta1.PhaseCuttingOver {
			t.Fatal("E and CuttingOver were not submitted in the same status patch")
		}
		return c.Status().Patch(ctx, obj, p)
	}
	res, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
	if err != nil || res.RequeueAfter != pollInterval || f.heads != 1 || f.sets != 0 || f.patches != 1 || !slices.Equal(f.trace, []string{"source sample", "optimistic status patch"}) {
		t.Fatal("E persistence did not return immediately after one status patch", err)
	}
	saved := f.current()
	if !apiequality.Semantic.DeepEqual(saved.Status, expected.Status) {
		t.Fatal("atomic E display patch changed unrelated status or gained completion authority")
	}
	if !apiequality.Semantic.DeepEqual(f.m, before) || saved.ResourceVersion == before.ResourceVersion {
		t.Fatal("persistence changed the caller snapshot or failed to advance the saved resourceVersion")
	}
}

func TestNativeClientPersistedEndRetry(t *testing.T) {
	for _, fault := range []string{nativeControlRefusalFault, nativeControlDisconnectFault, nativeControlWarningFault} {
		t.Run(fault, func(t *testing.T) {
			f := newNativeControlFixture(t)
			f.saveEnd(nativeControlTestEnd)
			f.fault = fault
			for range 2 {
				raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
				if !errors.Is(err, native.ErrNativeSetEndposUnknownEffect) || raw != nil || nativeControlEnd(f.current()) != nativeControlTestEnd {
					t.Fatal("unknown setter effect gained authority or lost E")
				}
				f.head = nativeControlLaterEnd
			}
			if f.heads != 0 || f.sets != 2 || f.patches != 0 || !slices.Equal(f.sentEnds, []string{nativeControlTestEnd, nativeControlTestEnd}) {
				t.Fatal("unknown effect did not retry the identical saved E")
			}
		})
	}
}

func TestNativeEndposAuthorityOrdering(t *testing.T) {
	t.Run("persisted E returns execution data only", func(t *testing.T) {
		f := newNativeControlFixture(t)
		f.saveEnd(nativeControlTestEnd)
		data, end, err := f.r.admitNativePersistedEndpos(context.Background(), f.m, f.worker, f.pvc)
		if err != nil || end != nativeControlTestEnd || data.JobUID != nativeControlWorkerUID || data.Role != native.JobRoleWorker || f.heads != 0 || f.sets != 0 {
			t.Fatal("saved boundary admission required a checkpoint or reached effects")
		}
	})
	for _, end := range []string{"", nativeControlZeroEnd, nativeControlInvalidEnd, " 0/40 ", "+0/40", "0/000000040"} {
		t.Run("invalid saved boundary/"+end, func(t *testing.T) {
			f := newNativeControlFixture(t)
			f.saveEnd(end)
			raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
			if err == nil || raw != nil || f.sets != 0 || f.heads != 0 {
				t.Fatal("setter reached effect without independently saved valid E")
			}
		})
	}
	t.Run("concurrent status change during setter", func(t *testing.T) {
		f := newNativeControlFixture(t)
		f.saveEnd(nativeControlTestEnd)
		f.afterExec = func(source bool) {
			if !source {
				m := f.current()
				m.Status.Phase = v1beta1.PhaseCuttingOver
				if f.store.Status().Update(context.Background(), m) != nil {
					t.Fatal("status race fixture failed")
				}
			}
		}
		raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
		if raw != nil || !errors.Is(err, native.ErrNativeSetEndposUnknownEffect) || f.heads != 0 || f.sets != 1 || nativeControlEnd(f.current()) != nativeControlTestEnd {
			t.Fatal("concurrent setter became ACK or resampled")
		}
	})
	t.Run("cancellation after sampling", func(t *testing.T) {
		f := newNativeControlFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.afterExec = func(source bool) {
			if source {
				cancel()
			}
		}
		_, err := f.r.persistNativeEnd(ctx, f.m, f.worker, f.pvc, f.control)
		if err == nil || f.patches != 0 || f.sets != 0 || nativeControlEnd(f.current()) != "" {
			t.Fatal("canceled sampling persisted or effected cutover")
		}
	})
}

func TestNativeStatusPreservesEnd(t *testing.T) {
	t.Run("missing dependency", func(t *testing.T) {
		r := &MigrationReconciler{}
		if r.updateNativeStatus(context.Background(), nil, nil) == nil {
			t.Fatal("missing identity accepted")
		}
	})
	for _, desiredEnd := range []string{"", nativeControlLaterEnd, nativeControlInvalidEnd} {
		t.Run("saved boundary/"+desiredEnd, func(t *testing.T) {
			f := newNativeControlFixture(t)
			f.saveEnd(nativeControlTestEnd)
			base := f.current()
			desired := base.DeepCopy()
			desired.Status.Phase = v1beta1.PhaseCuttingOver
			desired.Status.Replication.Endpos = desiredEnd
			if err := f.r.updateNativeStatus(context.Background(), desired, base); err != nil || nativeControlEnd(f.current()) != nativeControlTestEnd || f.patches != 1 {
				t.Fatal("ordinary status write replaced saved E", err)
			}
		})
	}
	t.Run("dropping replication preserves E", func(t *testing.T) {
		f := newNativeControlFixture(t)
		f.saveEnd(nativeControlTestEnd)
		base := f.current()
		desired := base.DeepCopy()
		desired.Status.Replication = nil
		if err := f.r.updateNativeStatus(context.Background(), desired, base); err != nil || nativeControlEnd(f.current()) != nativeControlTestEnd {
			t.Fatal("status merge erased saved E")
		}
	})
	t.Run("ordinary status cannot introduce E", func(t *testing.T) {
		f := newNativeControlFixture(t)
		base := f.current()
		desired := base.DeepCopy()
		desired.Status.Replication = &v1beta1.ReplicationStatus{Endpos: nativeControlLaterEnd}
		if err := f.r.updateNativeStatus(context.Background(), desired, base); err != nil || nativeControlEnd(f.current()) != "" {
			t.Fatal("ordinary status introduced unpersisted boundary")
		}
	})
	t.Run("stale ordinary write conflicts", func(t *testing.T) {
		f := newNativeControlFixture(t)
		base := f.current()
		f.saveEnd(nativeControlTestEnd)
		desired := base.DeepCopy()
		if err := f.r.updateNativeStatus(context.Background(), desired, base); !apierrors.IsConflict(err) || f.patches != 0 || nativeControlEnd(f.current()) != nativeControlTestEnd {
			t.Fatal("stale status overwrote winning E")
		}
	})
}

func TestNativeEndPersistenceRaces(t *testing.T) {
	testNativeEndPatchRaces(t)
	for _, head := range []string{nativeControlZeroEnd, nativeControlInvalidEnd} {
		t.Run("invalid source boundary/"+head, func(t *testing.T) {
			f := newNativeControlFixture(t)
			f.head = head
			_, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
			if err == nil || f.heads != 1 || f.patches != 0 || f.sets != 0 || nativeControlEnd(f.current()) != "" {
				t.Fatal("invalid sampled boundary reached persistence or effect")
			}
		})
	}
	t.Run("canceled before admission", func(t *testing.T) {
		f := newNativeControlFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := f.r.persistNativeEnd(ctx, f.m, f.worker, f.pvc, f.control)
		if err == nil || f.heads != 0 || f.patches != 0 || f.sets != 0 {
			t.Fatal("canceled operation reached cutover effects")
		}
	})
	t.Run("cancel after server persistence", func(t *testing.T) {
		f := newNativeControlFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
			if c.Status().Patch(ctx, obj, p) != nil {
				t.Fatal("patch fixture failed")
			}
			cancel()
			return context.Canceled
		}
		_, err := f.r.persistNativeEnd(ctx, f.m, f.worker, f.pvc, f.control)
		if err == nil || f.sets != 0 || nativeControlEnd(f.current()) != nativeControlTestEnd {
			t.Fatal("unknown canceled patch discarded saved E or continued to effect")
		}
		f.m = f.current()
		f.head = nativeControlLaterEnd
		_, err = f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
		if err != nil || f.heads != 1 || f.sets != 1 || !slices.Equal(f.sentEnds, []string{nativeControlTestEnd}) {
			t.Fatal("fresh retry resampled after canceled persisted patch")
		}
	})
}

func TestNativePersistedEndReadmission(t *testing.T) {
	t.Run("different saved boundary", func(t *testing.T) {
		f := newNativeControlFixture(t)
		f.saveEnd(nativeControlTestEnd)
		old := f.m.DeepCopy()
		f.saveEnd(nativeControlLaterEnd)
		raw, err := f.r.setNativePersistedEnd(context.Background(), old, f.worker, f.pvc, f.control)
		if err == nil || raw != nil || f.sets != 0 || f.heads != 0 {
			t.Fatal("setter substituted a different live boundary")
		}
	})
	t.Run("completed worker cannot use active setter", func(t *testing.T) {
		f := newNativeControlFixture(t)
		f.saveEnd(nativeControlTestEnd)
		p := f.pod.DeepCopy()
		p.Status.Phase = corev1.PodSucceeded
		id := p.Status.ContainerStatuses[0].ContainerID
		p.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: id, ExitCode: 0}}
		if f.store.Status().Update(context.Background(), p) != nil {
			t.Fatal("terminal worker fixture failed")
		}
		raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
		if err == nil || raw != nil || f.sets != 0 || f.heads != 0 || nativeControlEnd(f.current()) != nativeControlTestEnd {
			t.Fatal("active setter accepted completed execution or lost E")
		}
	})
	for _, field := range []string{"control Pod replacement", "control PVC replacement", "control spec replacement"} {
		t.Run(field, func(t *testing.T) {
			f := newNativeControlFixture(t)
			f.saveEnd(nativeControlTestEnd)
			f.afterExec = func(source bool) {
				if source {
					return
				}
				switch field {
				case "control Pod replacement":
					p := f.pod.DeepCopy()
					if f.store.Get(context.Background(), client.ObjectKeyFromObject(p), p) != nil {
						t.Fatal("Pod fixture missing")
					}
					p.UID = probeReplacement
					if f.store.Update(context.Background(), p) != nil {
						t.Fatal("Pod replacement fixture failed")
					}
				case "control PVC replacement":
					p := f.pvc.DeepCopy()
					if f.store.Get(context.Background(), client.ObjectKeyFromObject(p), p) != nil {
						t.Fatal("PVC fixture missing")
					}
					p.UID = probeReplacement
					if f.store.Update(context.Background(), p) != nil {
						t.Fatal("PVC replacement fixture failed")
					}
				default:
					m := f.current()
					m.Spec.Runner.NodeSelector = map[string]string{"control-changed": "yes"}
					if f.store.Update(context.Background(), m) != nil {
						t.Fatal("spec replacement fixture failed")
					}
				}
			}
			raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
			if raw != nil || !errors.Is(err, native.ErrNativeSetEndposUnknownEffect) || f.sets != 1 || f.heads != 0 || nativeControlEnd(f.current()) != nativeControlTestEnd {
				t.Fatal("uncertain changed execution gained output or resampled E")
			}
		})
	}
}

func TestNativeStatusPreservesEndAcrossTerminalStates(t *testing.T) {
	for _, phase := range []v1beta1.MigrationPhase{v1beta1.PhaseSuspended, v1beta1.PhaseFailed, v1beta1.PhaseCompleted} {
		t.Run(string(phase), func(t *testing.T) {
			f := newNativeControlFixture(t)
			f.saveEnd(nativeControlTestEnd)
			m := f.current()
			m.Spec.Suspend = phase == v1beta1.PhaseSuspended
			if f.store.Update(context.Background(), m) != nil {
				t.Fatal("state fixture failed")
			}
			base := f.current()
			desired := base.DeepCopy()
			desired.Status.Phase = phase
			desired.Status.Replication = nil
			if err := f.r.updateNativeStatus(context.Background(), desired, base); err != nil || nativeControlEnd(f.current()) != nativeControlTestEnd || f.current().Status.Phase != phase {
				t.Fatal("terminal/suspend status merge lost E", err)
			}
		})
	}
}

func testNativeEndTwoReconciliations(t *testing.T) {
	t.Helper()

	f := newNativeControlFixture(t)
	before := f.m.DeepCopy()
	res, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
	if err != nil || res.RequeueAfter != pollInterval || f.heads != 1 || f.sets != 0 || f.patches != 1 || nativeControlEnd(f.current()) != nativeControlTestEnd || !apiequality.Semantic.DeepEqual(f.m, before) || !slices.Equal(f.trace, []string{"source sample", "optimistic status patch"}) {
		t.Fatal("source/patch/return boundary was not preserved", err)
	}
	f.m = f.current()
	f.head = nativeControlLaterEnd
	raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
	if err != nil || len(raw) == 0 || f.heads != 1 || f.sets != 1 || !slices.Equal(f.sentEnds, []string{nativeControlTestEnd}) || f.patches != 1 || nativeControlEnd(f.current()) != nativeControlTestEnd {
		t.Fatal("later setter did not use saved E only", err)
	}
	if f.current().Status.Phase == v1beta1.PhaseCompleted {
		t.Fatal("raw setter data became completion")
	}

}

func testNativeEndPatchRaces(t *testing.T) {
	t.Helper()

	for _, field := range []string{"control UID change", "control generation change", nativeControlAttemptRaceCase} {
		t.Run(field, func(t *testing.T) {
			f := newNativeControlFixture(t)
			f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
				changed := f.current()
				switch field {
				case "control UID change":
					changed.UID = probeMigrationReplacement
				case "control generation change":
					changed.Generation++
				default:
					changed.Status.Attempts++
				}
				var err error
				if field == nativeControlAttemptRaceCase {
					err = f.store.Status().Update(ctx, changed)
				} else {
					err = f.store.Update(ctx, changed)
				}
				if err != nil {
					t.Fatal("concurrent identity fixture failed")
				}
				return c.Status().Patch(ctx, obj, p)
			}
			_, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
			if !apierrors.IsConflict(err) || f.heads != 1 || f.sets != 0 || nativeControlEnd(f.current()) != "" {
				t.Fatal("status race persisted E into changed identity or reached setter")
			}
		})
	}
}

func TestNativeEndUnknownEffectNeverResamples(t *testing.T) {
	f := newNativeControlFixture(t)
	if _, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control); err != nil {
		t.Fatal("initial boundary persistence failed")
	}
	f.m = f.current()
	f.head = nativeControlLaterEnd
	f.fault = nativeControlRefusalFault
	for range 2 {
		if _, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control); err != nil {
			t.Fatal("saved boundary guard failed")
		}
		if raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control); raw != nil || !errors.Is(err, native.ErrNativeSetEndposUnknownEffect) {
			t.Fatal("uncertain setter returned authority")
		}
		f.m = f.current()
	}
	if f.heads != 1 || f.patches != 1 || f.sets != 2 || nativeControlEnd(f.current()) != nativeControlTestEnd || !slices.Equal(f.sentEnds, []string{nativeControlTestEnd, nativeControlTestEnd}) {
		t.Fatal("unknown setter effect sampled a different boundary")
	}
}

const nativeCutoverStartedEvent = "Normal CutoverStarted stream frozen at endpos " + nativeControlTestEnd + ", draining"

// TestNativeCutoverStartedEvent: the event follows only a saved E, and a requeue that finds E saved never repeats it.
func TestNativeCutoverStartedEvent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply func(context.Context, client.Client, client.Object, client.Patch) error
		want  []string // events after the first pass, then after each of two requeues
	}{
		{name: "saved", want: []string{nativeCutoverStartedEvent, "", ""}},
		{name: nativeControlLostPatchCase, reply: func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
			if c.Status().Patch(ctx, obj, p) != nil {
				t.Fatal("lost reply fixture failed")
			}
			return errors.New(nativeControlCanary)
		}, want: []string{nativeCutoverStartedEvent, "", ""}},
		{name: "patch refused unsaved", reply: func(context.Context, client.Client, client.Object, client.Patch) error {
			return errors.New(nativeControlCanary)
		}, want: []string{"", nativeCutoverStartedEvent, ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeControlFixture(t)
			rec := f.r.Recorder.(*events.FakeRecorder)
			first := true
			f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
				if len(rec.Events) != 0 {
					t.Fatal("CutoverStarted was recorded before its E was submitted")
				}
				if first && tc.reply != nil {
					first = false
					return tc.reply(ctx, c, obj, p)
				}
				if err := c.Status().Patch(ctx, obj, p); err != nil {
					return err
				}
				if len(rec.Events) != 0 {
					t.Fatal("CutoverStarted was recorded before the status patch returned")
				}
				return nil
			}
			for pass, want := range tc.want {
				_, _ = f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
				got := strings.Join(drainEvents(rec), "\n")
				if got != want {
					t.Fatalf("pass %d recorded %q, want %q", pass, got, want)
				}
				if got != "" && nativeControlEnd(f.current()) != nativeControlTestEnd {
					t.Fatal("CutoverStarted announced an E the API server does not hold")
				}
				f.m = f.current()
			}
		})
	}
	// The winner announced its own E, even when it sampled the same one.
	for _, end := range []string{nativeControlLaterEnd, nativeControlTestEnd} {
		t.Run(nativeControlConflictCase+"/"+end, func(t *testing.T) { testNativeCutoverConflict(t, end) })
	}
}

func testNativeCutoverConflict(t *testing.T, end string) {
	t.Helper()
	f := newNativeControlFixture(t)
	f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
		winner := f.current()
		winner.Status.Replication = &v1beta1.ReplicationStatus{Endpos: end}
		if f.store.Status().Update(ctx, winner) != nil {
			t.Fatal("winner fixture failed")
		}
		return c.Status().Patch(ctx, obj, p)
	}
	if _, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control); !apierrors.IsConflict(err) {
		t.Fatal("conflict fixture did not conflict", err)
	}
	if got := drainEvents(f.r.Recorder.(*events.FakeRecorder)); len(got) != 0 {
		t.Fatalf("a lost race announced %q", got)
	}
}
