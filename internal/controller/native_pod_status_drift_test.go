package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// podStatusDriftReader serves each read of a pod the before or after status.resources of the OP-TRANSPORT capture.
// drifted(k) picks the after read for the k-th read of a pod.
type podStatusDriftReader struct {
	client.Reader
	before, after *corev1.ResourceRequirements
	drifted       func(k int) bool
	moveFinish    bool
	owner         string // when set, only pods of Jobs named with this prefix drift
	mu            sync.Mutex
	reads         map[types.UID]int
	max           int
}

func (r *podStatusDriftReader) observe(p *corev1.Pod) {
	if r.owner != "" && (len(p.OwnerReferences) != 1 || !strings.HasPrefix(p.OwnerReferences[0].Name, r.owner)) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads[p.UID]++
	k := r.reads[p.UID]
	r.max = max(r.max, k)
	if !r.drifted(k) {
		p.Status.Resources = r.before.DeepCopy()
		return
	}
	p.Status.Resources = r.after.DeepCopy()
	for i := range p.Status.ContainerStatuses {
		if s := &p.Status.ContainerStatuses[i]; r.moveFinish && s.Name == workerContainer && s.State.Terminated != nil {
			s.State.Terminated.FinishedAt = metav1.NewTime(s.State.Terminated.FinishedAt.Add(time.Second))
		}
	}
}

func (r *podStatusDriftReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := r.Reader.Get(ctx, key, obj, opts...)
	if p, ok := obj.(*corev1.Pod); ok && err == nil {
		r.observe(p)
	}
	return err
}

func (r *podStatusDriftReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := r.Reader.List(ctx, list, opts...)
	if pods, ok := list.(*corev1.PodList); ok && err == nil {
		for i := range pods.Items {
			r.observe(&pods.Items[i])
		}
	}
	return err
}

func capturedResourcesDrift(t *testing.T) (before, after *corev1.ResourceRequirements) {
	t.Helper()
	raw, err := os.ReadFile("../podexec/testdata/pod-status-drift.json")
	if err != nil {
		t.Fatal(err)
	}
	var capture struct{ Before, After corev1.PodStatus }
	if err := json.Unmarshal(raw, &capture); err != nil || capture.Before.Resources == nil {
		t.Fatal("captured drift unreadable", err)
	}
	return capture.Before.Resources, capture.After.Resources
}

// driftWrap installs the drift reader around a fixture's API reader; owner limits it to one kind of helper pod.
type driftWrap func(r client.Reader, owner string) client.Reader

// driftScenario builds a fresh fixture, installs wrap and reports whether the read was admitted.
type driftScenario func(t *testing.T, wrap driftWrap) bool

// checkPodStatusDrift drifts read k of a pod when bit b of k-1 is set, so across bits every pair of reads differs. Each
// schedule must be admitted, and refused once finishedAt moves too, except admittedMoves bits the scenario never compares.
// From tailFrom on (0: off), moving every later read must be refused; both are read off the current fixtures.
func checkPodStatusDrift(t *testing.T, admittedMoves uint, tailFrom int, run driftScenario) {
	t.Helper()
	before, after := capturedResourcesDrift(t)
	install := func(drifted func(int) bool, moveFinish bool) (driftWrap, *podStatusDriftReader) {
		d := &podStatusDriftReader{before: before, after: after, drifted: drifted, moveFinish: moveFinish, reads: map[types.UID]int{}}
		return func(r client.Reader, owner string) client.Reader {
			d.Reader, d.owner = r, owner
			return d
		}, d
	}
	wrap, probe := install(func(int) bool { return false }, false)
	if !run(t, wrap) {
		t.Fatal("scenario refused without drift")
	}
	if probe.max < 2 {
		t.Fatalf("scenario read no pod twice (max reads %d)", probe.max)
	}
	for bit := 0; 1<<bit < probe.max; bit++ {
		onBit := func(k int) bool { return (k-1)>>bit&1 == 1 }
		t.Run(fmt.Sprintf("captured drift bit %d", bit), func(t *testing.T) {
			wrap, _ := install(onBit, false)
			if !run(t, wrap) {
				t.Fatal("captured kubelet status drift refused")
			}
		})
		t.Run(fmt.Sprintf("terminal state moved bit %d", bit), func(t *testing.T) {
			wrap, _ := install(onBit, true)
			if run(t, wrap) != (admittedMoves>>bit&1 == 1) {
				t.Fatalf("moved terminal container state admitted=%v", admittedMoves>>bit&1 == 0)
			}
		})
	}
	for from := tailFrom; tailFrom > 0 && from <= probe.max; from++ {
		t.Run(fmt.Sprintf("terminal state moved from read %d", from), func(t *testing.T) {
			wrap, _ := install(func(k int) bool { return k >= from }, true)
			if run(t, wrap) {
				t.Fatal("moved terminal container state admitted")
			}
		})
	}
}

func driftCloneFixture(t *testing.T, follow bool, wrap driftWrap) *nativeCloneFixture {
	t.Helper()
	f := newNativeCloneFixture(t, follow, nil)
	if follow {
		f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: caughtUpLSN}
		if err := f.store.Status().Update(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
	}
	f.r.APIReader = wrap(f.r.APIReader, "")
	return f
}

func driftOriginal(t *testing.T, f *nativeCloneFixture) *corev1.Pod {
	t.Helper()
	original, err := f.r.nativeQuiescentWorker(context.Background(), f.m, f.worker, f.pvc)
	if err != nil {
		return nil
	}
	return original
}

func TestNativePodStatusDriftWorkerReads(t *testing.T) {
	ctx := context.Background()
	t.Run("quiescent worker confirmation", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1, 3, func(t *testing.T, wrap driftWrap) bool {
			f := driftCloneFixture(t, false, wrap)
			return driftOriginal(t, f) != nil
		})
	})
	t.Run("completion current", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1, 3, func(t *testing.T, wrap driftWrap) bool {
			f := driftCloneFixture(t, false, wrap)
			original := driftOriginal(t, f)
			_, pod, err := f.r.nativeCompletionCurrent(ctx, f.m, f.worker, f.pvc, original)
			return original != nil && err == nil && pod != nil
		})
	})
	t.Run("recovery quiescent current", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1, 3, func(t *testing.T, wrap driftWrap) bool {
			f := driftCloneFixture(t, false, wrap)
			original := driftOriginal(t, f)
			_, pod, err := f.r.nativeRecoveryQuiescentCurrent(ctx, f.m, f.worker, f.pvc, original)
			return original != nil && err == nil && pod != nil
		})
	})
	t.Run("follow terminal current", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1, 3, func(t *testing.T, wrap driftWrap) bool {
			f := driftCloneFixture(t, true, wrap)
			original := driftOriginal(t, f)
			_, pod, err := f.r.nativeFollowTerminalCurrent(ctx, f.m, f.worker, f.pvc, original)
			return original != nil && err == nil && pod != nil
		})
	})
	t.Run("verification original", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1, 3, func(t *testing.T, wrap driftWrap) bool {
			f := driftCloneFixture(t, false, wrap)
			original := driftOriginal(t, f)
			return original != nil && f.r.nativeVerificationOriginalUnchanged(ctx, f.m, f.worker, f.pvc, original)
		})
	})
	t.Run("clone report", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1, 3, func(t *testing.T, wrap driftWrap) bool {
			f := driftCloneFixture(t, false, wrap)
			report, err := nativeCloneRead(t, f)
			return err == nil && report != nil
		})
	})
	t.Run("follow completion proof", func(t *testing.T) {
		checkPodStatusDrift(t, 0, 3, func(t *testing.T, wrap driftWrap) bool {
			s, r, clone, ack := nativeTerminalTestFixture(t)
			r.APIReader = wrap(r.APIReader, "")
			got, err := r.readNativeFollowCompletion(ctx, s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy(), clone, nativeTerminalTestAck(t, ack))
			return err == nil && got != nil
		})
	})
}

func TestNativePodStatusDriftHelperReads(t *testing.T) {
	ctx := context.Background()
	t.Run("completion preflight helper", func(t *testing.T) {
		checkPodStatusDrift(t, 0b11, 0, func(t *testing.T, wrap driftWrap) bool {
			f, worker, pvc, _ := newCompletionProbe(t, false)
			f.r.APIReader = wrap(f.r.APIReader, "rpgmigrate-version-")
			v, passed, _, err := f.r.ensureNativeCompletionPreflight(ctx, f.m, worker, pvc)
			return err == nil && passed && v != nil
		})
	})
	t.Run("verification helper", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1, 0, func(t *testing.T, wrap driftWrap) bool {
			f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true})
			nativeInstallCompare(t, f, compareSchema, true)
			f.r.APIReader = wrap(f.r.APIReader, f.m.Name+"-compare-")
			done, err := nativeVerify(t, f)
			return err == nil && done && len(f.m.Status.Verification) == 1
		})
	})
	t.Run("endpos helper", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1, 0, func(t *testing.T, wrap driftWrap) bool {
			s, r, _, _, _ := nativeFollowAckFixture(t, false)
			r.APIReader = wrap(r.APIReader, "rpgmigrate-endpos-")
			raw, err := r.readNativeEndposAck(ctx, s.migration.DeepCopy(), s.worker.DeepCopy(), s.pvc.DeepCopy())
			return err == nil && len(raw) != 0
		})
	})
	t.Run("cleanup helper", func(t *testing.T) {
		checkPodStatusDrift(t, 0b10010, 0, func(t *testing.T, wrap driftWrap) bool {
			f := newNativeFinishFixture(t)
			f.r.APIReader = wrap(f.r.APIReader, "rpgmigrate-cleanup-")
			done, err := f.r.ensureNativeCleanup(ctx, f.m, f.worker, f.pvc)
			return err == nil && done && f.cleaned
		})
	})
	t.Run("compare helper pin reset", func(t *testing.T) {
		checkPodStatusDrift(t, 0b1100000, 0, func(t *testing.T, wrap driftWrap) bool {
			f := newNativeDispositionFixture(t, false)
			installRecoveryCompare(t, f, compareSchema)
			installRecoveryCompare(t, f, compareData)
			f.m.Generation++
			if err := f.store.Update(ctx, f.m); err != nil {
				t.Fatal(err)
			}
			f.r.APIReader = wrap(f.r.APIReader, f.m.Name+"-compare-")
			for range 3 {
				if handled, err := driftDisposition(t, f); !handled || err != nil {
					return false
				}
			}
			live := &v1beta1.Migration{}
			if err := f.store.Get(ctx, client.ObjectKeyFromObject(f.m), live); err != nil {
				t.Fatal(err)
			}
			return live.Status.Attempts == 2 && live.Status.NativeSchemaJobUID == "" && live.Status.NativeDataJobUID == ""
		})
	})
	t.Run("deletion inventory cleanup helper", func(t *testing.T) {
		checkPodStatusDrift(t, 0b11, 0, func(t *testing.T, wrap driftWrap) bool {
			f := newNativeDispositionFixture(t, true)
			f.m.Status.Replication = nil
			f.m.Finalizers = []string{finalizerName}
			if err := f.store.Update(ctx, f.m); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Status().Update(ctx, f.m); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Delete(ctx, f.m); err != nil {
				t.Fatal(err)
			}
			f.r.APIReader = wrap(f.r.APIReader, "rpgmigrate-cleanup-")
			handled, err := driftDisposition(t, f)
			return handled && err == nil && f.cleanupCalled && apierrors.IsNotFound(f.store.Get(ctx, client.ObjectKeyFromObject(f.m), &v1beta1.Migration{})) &&
				f.store.Get(ctx, client.ObjectKeyFromObject(f.worker), &batchv1.Job{}) == nil
		})
	})
}

// driftDisposition is f.reconcile without its t.Fatal on an unhandled pass, which a refused read may cause.
func driftDisposition(t *testing.T, f *nativeDispositionFixture) (bool, error) {
	t.Helper()
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), f.m); err != nil {
		t.Fatal(err)
	}
	handled, result, err := f.r.reconcileNativeDisposition(context.Background(), f.m, f.m.DeepCopy(), f.worker, f.pvc)
	f.lastResult = result
	return handled, err
}
