package controller

import (
	"context"
	"errors"
	"slices"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const nativeFollowFlowWorkerUID types.UID = "44444444-4444-4444-8444-444444444444"

func nativeFollowFlowFixture(t *testing.T) *nativeControlFixture {
	t.Helper()
	f := newNativeControlFixture(t)
	f.worker.UID = nativeFollowFlowWorkerUID
	defaultProbeJob(f.worker)
	f.pod.Labels = f.worker.Spec.Template.Labels
	f.pod.OwnerReferences[0].UID = f.worker.UID
	for _, object := range []client.Object{f.worker, f.pod} {
		if err := f.store.Update(context.Background(), object); err != nil {
			t.Fatal(err)
		}
	}
	m := f.current()
	m.Status.NativeJobUID, m.Status.NativePVCUID = string(f.worker.UID), string(f.pvc.UID)
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	f.m = f.current()
	f.r.NativeControl = f.control
	return f
}

func testNativeFollowCutoverPolicies(t *testing.T) {
	t.Helper()
	for _, mode := range []v1beta1.CutoverMode{v1beta1.CutoverManual, v1beta1.CutoverAutomatic} {
		for _, approved := range []bool{false, true} {
			for _, caughtUp := range []bool{false, true} {
				t.Run(string(mode)+map[bool]string{false: "/unapproved", true: "/approved"}[approved]+map[bool]string{false: "/unknown-catchup", true: "/caught-up"}[caughtUp], func(t *testing.T) {
					f := nativeFollowFlowFixture(t)
					m := f.current()
					m.Spec.Cutover.Mode, m.Spec.Cutover.Approved = mode, approved
					if err := f.store.Update(context.Background(), m); err != nil {
						t.Fatal(err)
					}
					f.m = f.current()
					res, err := f.r.reconcileNativeFollowCutover(context.Background(), f.m, f.worker, f.pvc, caughtUp)
					want := caughtUp && (mode == v1beta1.CutoverAutomatic || approved)
					if err != nil || res.RequeueAfter != pollInterval || (f.heads == 1) != want || f.sets != 0 || (nativeControlEnd(f.current()) != "") != want {
						t.Fatalf("policy/head/persist/return ordering err=%v heads=%d sets=%d wanted=%t", err, f.heads, f.sets, want)
					}
				})
			}
		}
	}
}

func TestNativeFollowLifecycleCutover(t *testing.T) {
	t.Run("both cutover policies", testNativeFollowCutoverPolicies)
	t.Run("later pass retries saved E after unknown setter without resampling", func(t *testing.T) {
		f := nativeFollowFlowFixture(t)
		if _, err := f.r.reconcileNativeFollowCutover(context.Background(), f.m, f.worker, f.pvc, true); err != nil {
			t.Fatal(err)
		}
		f.m, f.head, f.fault = f.current(), nativeControlLaterEnd, nativeControlDisconnectFault
		for range 2 {
			if _, err := f.r.reconcileNativeFollowCutover(context.Background(), f.m, f.worker, f.pvc, false); !errors.Is(err, native.ErrNativeSetEndposUnknownEffect) {
				t.Fatal("unknown setter effect was hidden", err)
			}
			f.m = f.current()
		}
		if f.heads != 1 || f.sets != 2 || f.patches != 1 || !slices.Equal(f.sentEnds, []string{nativeControlTestEnd, nativeControlTestEnd}) || meta.IsStatusConditionTrue(f.current().Status.Conditions, v1beta1.ConditionCutoverComplete) {
			t.Fatal("unknown setter resampled or supplied completion")
		}
	})
	for _, fault := range []string{nativeControlConflictCase, nativeControlLostPatchCase, nativeControlSourceCrashCase} {
		t.Run(fault, func(t *testing.T) {
			f := nativeFollowFlowFixture(t)
			f.patch = func(ctx context.Context, c client.Client, object client.Object, patch client.Patch) error {
				if fault == nativeControlConflictCase {
					f.saveEnd(nativeControlLaterEnd)
				}
				err := c.Status().Patch(ctx, object, patch)
				if err == nil && fault == nativeControlLostPatchCase {
					return errBoom
				}
				return err
			}
			if fault == nativeControlSourceCrashCase {
				f.afterExec = func(source bool) {
					if source {
						m := f.current()
						m.Generation++
						if f.store.Update(context.Background(), m) != nil {
							t.Fatal("crash boundary fixture failed")
						}
					}
				}
			}
			_, err := f.r.reconcileNativeFollowCutover(context.Background(), f.m.DeepCopy(), f.worker, f.pvc, true)
			if err == nil || f.heads != 1 || f.sets != 0 {
				t.Fatal("E persistence failure crossed setter boundary", err)
			}
			if fault == nativeControlConflictCase && !apierrors.IsConflict(err) {
				t.Fatal("conflict lost its classification", err)
			}
		})
	}
}

func TestNativeFollowReceiptStatusConflict(t *testing.T) {
	for _, field := range []string{"job receipt", "PVC receipt"} {
		t.Run(field, func(t *testing.T) {
			f := nativeFollowFlowFixture(t)
			f.saveEnd(nativeControlTestEnd)
			base := f.current()
			desired := base.DeepCopy()
			if field == "job receipt" {
				desired.Status.NativeJobUID = string(workerReplacementUID)
			} else {
				desired.Status.NativePVCUID = string(workerReplacementUID)
			}
			if err := f.r.updateNativeStatus(context.Background(), desired, base); err == nil || f.patches != 0 {
				t.Fatal("ordinary status mutation replaced identity receipts")
			}
			winner := f.current()
			winner.Status.NativeJobUID, winner.Status.NativePVCUID = string(workerReplacementUID), string(workerReplacementUID)
			winner.Status.Replication.Endpos = nativeControlLaterEnd
			if err := f.store.Status().Update(context.Background(), winner); err != nil {
				t.Fatal(err)
			}
			if err := f.r.updateNativeStatus(context.Background(), base.DeepCopy(), base); err == nil || f.patches != 0 {
				t.Fatal("stale status overwrote winning receipts or E")
			}
			if got := f.current(); got.Status.NativeJobUID != string(workerReplacementUID) || got.Status.NativePVCUID != string(workerReplacementUID) || nativeControlEnd(got) != nativeControlLaterEnd {
				t.Fatal("winning status was not preserved")
			}
		})
	}
}

func TestNativeFollowCachedStaleLiveReplacement(t *testing.T) {
	for _, fault := range []string{"Migration replacement", "worker replacement", "PVC replacement", "receipt changed live", "saved E changed live", "Pod replacement during admission"} {
		t.Run(fault, func(t *testing.T) {
			f := nativeFollowFlowFixture(t)
			cached := f.m.DeepCopy()
			switch fault {
			case "Migration replacement":
				m := f.current()
				m.UID = workerReplacementUID
				if err := f.store.Update(context.Background(), m); err != nil {
					t.Fatal(err)
				}
			case "worker replacement":
				j := f.worker.DeepCopy()
				j.UID = workerReplacementUID
				if err := f.store.Update(context.Background(), j); err != nil {
					t.Fatal(err)
				}
			case "PVC replacement":
				p := f.pvc.DeepCopy()
				p.UID = workerReplacementUID
				if err := f.store.Update(context.Background(), p); err != nil {
					t.Fatal(err)
				}
			case "receipt changed live":
				m := f.current()
				m.Status.NativeJobUID = string(workerReplacementUID)
				if err := f.store.Status().Update(context.Background(), m); err != nil {
					t.Fatal(err)
				}
			case "saved E changed live":
				f.saveEnd(nativeControlLaterEnd)
			case "Pod replacement during admission":
				reads := 0
				f.beforeGet = func(object client.Object) {
					if _, ok := object.(*corev1.Pod); ok {
						reads++
						if reads == 2 {
							p := f.pod.DeepCopy()
							p.UID = workerReplacementUID
							if err := f.store.Update(context.Background(), p); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
			}
			_, err := f.r.reconcileNativeFollowCutover(context.Background(), cached, f.worker, f.pvc, true)
			if err == nil || f.heads != 0 || f.sets != 0 || f.patches != 0 {
				t.Fatal("cached identity rescued a changed live execution", err)
			}
		})
	}
}

func TestNativeFollowReceiptAdmission(t *testing.T) {
	for _, setter := range []bool{false, true} {
		for _, after := range []bool{false, true} {
			t.Run(map[bool]string{false: "persist", true: "active setter"}[setter]+map[bool]string{false: "/before", true: "/after"}[after], func(t *testing.T) {
				f := nativeFollowFlowFixture(t)
				if setter {
					f.saveEnd(nativeControlTestEnd)
				}
				change := func() {
					m := f.current()
					m.Status.NativePVCUID = string(workerReplacementUID)
					if err := f.store.Status().Update(context.Background(), m); err != nil {
						t.Fatal(err)
					}
				}
				if after {
					f.afterExec = func(bool) { change() }
				} else {
					change()
				}
				var err error
				if setter {
					var raw []byte
					raw, err = f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
					if raw != nil {
						t.Fatal("changed receipt retained setter bytes")
					}
				} else {
					_, err = f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control)
				}
				if err == nil || f.patches != 0 || !after && (f.heads != 0 || f.sets != 0) {
					t.Fatal("receipt refusal gained success or effects", err)
				}
				if setter && after && !errors.Is(err, native.ErrNativeSetEndposUnknownEffect) {
					t.Fatal("setter uncertainty lost its classification", err)
				}
			})
		}
	}
}
