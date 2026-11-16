package controller

// Regression tests for suspend, deletion and the work volume (design B T4): the review probes of
// lifecycle 1 (deletion), 2, 7 and 9 and design A's matching probes, inverted through reconcileNative.

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Markers of the write order a t4Writes records.
const (
	t4StatusWrite    = "status-write"
	t4FinalizerWrite = "finalizer-write"
)

// t4Writes records the writes a pass made, in order, and lets drops of the worker and the work
// PVC reach the store as the API server and the garbage collector would.
type t4Writes struct {
	mu     sync.Mutex
	order  []string
	drops  []client.DeleteOptions
	failed *metav1.Condition
	create func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error
}

func t4Wrap(t *testing.T, f *nativeDispositionFixture) *t4Writes {
	t.Helper()
	w := &t4Writes{}
	inner, ok := f.r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("fixture client cannot be wrapped")
	}
	f.r.Client = interceptor.NewClient(inner, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if w.create != nil {
				return w.create(ctx, c, obj, opts...)
			}
			return c.Create(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			o := client.DeleteOptions{}
			o.ApplyOptions(opts)
			w.mu.Lock()
			w.order = append(w.order, "delete "+obj.GetName())
			w.drops = append(w.drops, o)
			w.mu.Unlock()
			_, claim := obj.(*corev1.PersistentVolumeClaim)
			if !claim && obj.GetUID() != f.worker.UID {
				return c.Delete(ctx, obj, opts...)
			}
			if o.Preconditions == nil || o.Preconditions.UID == nil || *o.Preconditions.UID != obj.GetUID() {
				t.Errorf("delete of %s lacks its UID precondition", obj.GetName())
				return errBoom
			}
			if !claim && (o.PropagationPolicy == nil || *o.PropagationPolicy != metav1.DeletePropagationForeground) {
				t.Errorf("worker drop of %s is not foreground", obj.GetName())
				return errBoom
			}
			if err := f.store.Delete(ctx, obj); err != nil {
				return err
			}
			pods := &corev1.PodList{}
			if err := f.store.List(ctx, pods); err != nil {
				return err
			}
			for i := range pods.Items {
				if owner := metav1.GetControllerOf(&pods.Items[i]); owner != nil && owner.UID == obj.GetUID() {
					if err := f.store.Delete(ctx, &pods.Items[i]); err != nil {
						return err
					}
				}
			}
			return nil
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
			w.mu.Lock()
			w.order = append(w.order, t4FinalizerWrite)
			w.mu.Unlock()
			return c.Patch(ctx, obj, p, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			w.mu.Lock()
			w.order = append(w.order, t4StatusWrite)
			if m, ok := obj.(*v1beta1.Migration); ok {
				if cf := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCleanupFailed); cf != nil {
					w.failed = cf.DeepCopy()
				}
			}
			w.mu.Unlock()
			return c.SubResource(sub).Patch(ctx, obj, p, opts...)
		},
	})
	return w
}

// t4Delete gives the follow Migration the operator finalizer, any extra finalizers, and a deletionTimestamp.
func t4Delete(t *testing.T, f *nativeDispositionFixture, extra ...string) {
	t.Helper()
	m := f.current()
	controllerutil.AddFinalizer(m, finalizerName)
	for _, fin := range extra {
		controllerutil.AddFinalizer(m, fin)
	}
	if err := f.store.Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Delete(context.Background(), f.current()); err != nil {
		t.Fatal(err)
	}
}

func t4Released(f *nativeDispositionFixture) bool {
	return apierrors.IsNotFound(f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), &v1beta1.Migration{}))
}

func t4ClaimGone(f *nativeDispositionFixture) bool {
	return apierrors.IsNotFound(f.store.Get(context.Background(), client.ObjectKeyFromObject(f.pvc), &corev1.PersistentVolumeClaim{}))
}

func t4Blocked(f *nativeDispositionFixture) string {
	if c := meta.FindStatusCondition(f.current().Status.Conditions, v1beta1.ConditionBlocked); c != nil && c.Status == metav1.ConditionTrue {
		return c.Reason
	}
	return ""
}

// t4Passes runs reconcileNative until the Migration is released or n passes ran.
func t4Passes(t *testing.T, f *nativeDispositionFixture, n int) error {
	t.Helper()
	var err error
	for range n {
		if t4Released(f) {
			return nil
		}
		_, err = f.dispatch(t)
	}
	return err
}

func t4RemoveWorker(t *testing.T, f *nativeDispositionFixture) {
	t.Helper()
	for _, obj := range []client.Object{f.pod, f.worker} {
		if err := f.store.Delete(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
	}
}

func t4RunningWorker(t *testing.T, f *nativeDispositionFixture, started bool) {
	t.Helper()
	f.worker.Status = batchv1.JobStatus{Active: 1, Ready: ptr.To(int32(0))}
	f.replace(f.worker)
	if started {
		f.worker.Status.Ready = ptr.To(int32(1))
		f.replace(f.worker)
		f.pod.Status.Phase = corev1.PodRunning
		f.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	} else {
		f.pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	}
	f.replace(f.pod)
}

func t4RunningCompare(t *testing.T, f *nativeDispositionFixture, job *batchv1.Job) {
	t.Helper()
	job.Status = batchv1.JobStatus{Active: 1}
	f.replace(job)
	pods := &corev1.PodList{}
	if err := f.store.List(context.Background(), pods); err != nil {
		t.Fatal(err)
	}
	for i := range pods.Items {
		if owner := metav1.GetControllerOf(&pods.Items[i]); owner != nil && owner.UID == job.UID {
			p := pods.Items[i].DeepCopy()
			p.Status.Phase = corev1.PodRunning
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			f.replace(p)
		}
	}
}

func t4Suspend(t *testing.T, f *nativeDispositionFixture) {
	t.Helper()
	m := f.current()
	m.Spec.Suspend = true
	m.Generation++
	if err := f.store.Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

// Lifecycle 1, deletion part: the recorded worker Job is gone (foreground GC, a person); deletion
// cleans up from the surviving claim instead of refusing forever.
func TestNativeDeletionAfterWorkerJobVanished(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	t4RemoveWorker(t, f)
	t4Delete(t, f)
	if err := t4Passes(t, f, 3); err != nil || !f.cleanupCalled || !t4Released(f) || !t4ClaimGone(f) {
		t.Fatalf("deletion after the worker Job vanished: err=%v cleanup=%v released=%v claimGone=%v writes=%v", err, f.cleanupCalled, t4Released(f), t4ClaimGone(f), w.order)
	}
}

// Lifecycle 1 control: the failed worker's pod is gone, its Job remains (fixed by T2's pod-less anchor).
func TestNativeDeletionAfterWorkerPodVanished(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	t4Wrap(t, f)
	if err := f.store.Delete(context.Background(), f.pod); err != nil {
		t.Fatal(err)
	}
	t4Delete(t, f)
	if err := t4Passes(t, f, 3); err != nil || !f.cleanupCalled || !t4Released(f) {
		t.Fatalf("deletion after the worker pod vanished: err=%v cleanup=%v released=%v", err, f.cleanupCalled, t4Released(f))
	}
}

// Lifecycle 2 (TestReviewForegroundDeletionLeavesFinalizer, claim present): R8 waits while the
// garbage collector removes the Jobs, and R5 keeps the claim, so cleanup runs and then releases.
func TestNativeForegroundDeletionWaitsThenCleans(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	t4Delete(t, f, metav1.FinalizerDeleteDependents)
	creates := f.creates
	if _, err := f.dispatch(t); err != nil || t4Blocked(f) != v1beta1.ReasonForegroundDeletion || f.creates != creates || t4Released(f) {
		t.Fatalf("foreground deletion did not wait: err=%v blocked=%q creates=%d writes=%v", err, t4Blocked(f), f.creates-creates, w.order)
	}
	// The garbage collector deletes the worker Job and its pod, then drops foregroundDeletion.
	t4RemoveWorker(t, f)
	m := f.current()
	controllerutil.RemoveFinalizer(m, metav1.FinalizerDeleteDependents)
	if err := f.store.Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if err := t4Passes(t, f, 3); err != nil || !f.cleanupCalled || !t4Released(f) || !t4ClaimGone(f) {
		t.Fatalf("cleanup after foreground GC: err=%v cleanup=%v released=%v claimGone=%v", err, f.cleanupCalled, t4Released(f), t4ClaimGone(f))
	}
	if last := w.order[len(w.order)-1]; last != t4FinalizerWrite || !slices.Contains(w.order, "delete "+f.pvc.Name) || slices.Index(w.order, "delete "+f.pvc.Name) > slices.Index(w.order, t4FinalizerWrite) {
		t.Fatalf("the work PVC was not deleted before the finalizer release: %v", w.order)
	}
}

// R8: no helper Job is created while foregroundDeletion is a finalizer, since the GC would delete it too.
func TestNativeForegroundDeletionCreatesNoHelperBeforeGC(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	t4Wrap(t, f)
	t4Delete(t, f, metav1.FinalizerDeleteDependents)
	for range 2 {
		if _, err := f.dispatch(t); err != nil {
			t.Fatal(err)
		}
	}
	if f.creates != 0 || f.cleanupCalled || t4Blocked(f) != v1beta1.ReasonForegroundDeletion {
		t.Fatalf("foreground deletion created %d helpers, cleanup=%v, blocked=%q", f.creates, f.cleanupCalled, t4Blocked(f))
	}
}

func t4CleanupFailedReleased(t *testing.T, f *nativeDispositionFixture, w *t4Writes, reason string) {
	t.Helper()
	run, err := nativeRunID(f.m)
	if err != nil {
		t.Fatal(err)
	}
	if !t4Released(f) || w.failed == nil || w.failed.Status != metav1.ConditionTrue || w.failed.Reason != reason ||
		!strings.Contains(w.failed.Message, "rpgm_s_"+run+"_*") || !strings.Contains(w.failed.Message, "rpgm_p_"+run+"_*") {
		t.Fatalf("release without CleanupFailed=%s naming the slot and publication: released=%v condition=%+v", reason, t4Released(f), w.failed)
	}
	status, release := slices.Index(w.order, t4StatusWrite), slices.Index(w.order, t4FinalizerWrite)
	if status < 0 || release < status || w.order[len(w.order)-1] != t4FinalizerWrite {
		t.Fatalf("CleanupFailed was not written before the finalizer release: %v", w.order)
	}
	recorder, _ := f.r.Recorder.(*events.FakeRecorder)
	found := false
	for len(recorder.Events) > 0 {
		e := <-recorder.Events
		found = found || strings.Contains(e, "Warning "+v1beta1.ConditionCleanupFailed) && strings.Contains(e, "rpgm_s_"+run+"_*")
	}
	if !found {
		t.Fatal("no CleanupFailed warning event was attempted")
	}
}

// Lifecycle 2, the claim lost (design A TestDesignLostWorkVolumeRelease): cleanup cannot mount it,
// so the deletion releases with CleanupFailed=WorkVolumeLost instead of waiting forever.
func TestNativeWorkVolumeLostReleasesWithCleanupFailed(t *testing.T) {
	for _, state := range []string{"claim deleting", "claim gone", "claim replaced"} {
		t.Run(state, func(t *testing.T) {
			f := newNativeDispositionFixture(t, true)
			w := t4Wrap(t, f)
			t4RemoveWorker(t, f)
			claim := &corev1.PersistentVolumeClaim{}
			if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.pvc), claim); err != nil {
				t.Fatal(err)
			}
			switch state {
			case "claim deleting":
				controllerutil.AddFinalizer(claim, "kubernetes.io/pvc-protection")
				if err := f.store.Update(context.Background(), claim); err != nil {
					t.Fatal(err)
				}
				if err := f.store.Delete(context.Background(), claim); err != nil {
					t.Fatal(err)
				}
			case "claim gone":
				if err := f.store.Delete(context.Background(), claim); err != nil {
					t.Fatal(err)
				}
			default:
				if err := f.store.Delete(context.Background(), claim); err != nil {
					t.Fatal(err)
				}
				claim.ResourceVersion, claim.UID = "", "replacement-claim"
				if err := f.store.Create(context.Background(), claim); err != nil {
					t.Fatal(err)
				}
			}
			t4Delete(t, f)
			if err := t4Passes(t, f, 2); err != nil || f.cleanupCalled {
				t.Fatalf("lost claim: err=%v cleanup=%v", err, f.cleanupCalled)
			}
			t4CleanupFailedReleased(t, f, w, v1beta1.ReasonWorkVolumeLost)
			if state == "claim replaced" && t4ClaimGone(f) {
				t.Fatal("a claim with another UID was deleted")
			}
		})
	}
}

// R9 (fake): a dry-run Job create refused with NamespaceTerminating releases at once with CleanupFailed.
func TestNativeTerminatingNamespaceDryRunReleases(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	terminating := apierrors.NewForbidden(schema.GroupResource{Group: batchv1.GroupName}, "probe", errors.New("namespace is being terminated"))
	terminating.ErrStatus.Details.Causes = []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause}}
	w.create = func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
		return terminating
	}
	t4Delete(t, f)
	if _, err := f.dispatch(t); err != nil || f.cleanupCalled {
		t.Fatalf("terminating namespace: err=%v cleanup=%v", err, f.cleanupCalled)
	}
	t4CleanupFailedReleased(t, f, w, v1beta1.ReasonNamespaceTerminating)
	if !t4ClaimGone(f) {
		t.Fatal("the bound work PVC outlived a CleanupFailed release")
	}
}

// R9: only the NamespaceTerminating cause releases; a quota refusal of every Job create keeps the finalizer.
func TestNativeQuotaForbiddenIsNotTerminating(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	w.create = func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: batchv1.GroupName}, "helper", errors.New("exceeded quota"))
	}
	t4Delete(t, f)
	for range 2 {
		_, _ = f.dispatch(t)
	}
	if t4Released(f) || w.failed != nil || f.cleanupCalled {
		t.Fatalf("a quota refusal released the finalizer: released=%v condition=%+v", t4Released(f), w.failed)
	}
}

// An unreachable inventory is not a Kubernetes impossibility: the finalizer stays with Blocked=CleanupUnavailable.
func TestNativeInventoryUnavailableKeepsFinalizer(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	f.r.NativeEvidence = nil
	t4Delete(t, f)
	_, err := f.dispatch(t)
	if err == nil || t4Released(f) || w.failed != nil || t4Blocked(f) != v1beta1.ReasonCleanupUnavailable {
		t.Fatalf("unavailable inventory: err=%v released=%v condition=%+v blocked=%q", err, t4Released(f), w.failed, t4Blocked(f))
	}
}

// Without the operator finalizer the garbage collector owns everything; a pass writes and creates nothing.
func TestNativeDeletingWithoutOperatorFinalizerDoesNothing(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	m := f.current()
	controllerutil.AddFinalizer(m, metav1.FinalizerDeleteDependents)
	if err := f.store.Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Delete(context.Background(), f.current()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.dispatch(t); err != nil || len(w.order) != 0 || f.creates != 0 {
		t.Fatalf("deletion without the operator finalizer acted: err=%v writes=%v creates=%d", err, w.order, f.creates)
	}
}

// Lifecycle 7 (TestReviewPendingWorkerBlocksDeletion): a worker whose container never started is
// dropped by UID and foreground, then deletion cleans up and releases.
func TestNativeDeletionDropsNeverStartedWorker(t *testing.T) {
	for _, state := range []string{"pod pending", "no pod yet"} {
		t.Run(state, func(t *testing.T) {
			f := newNativeDispositionFixture(t, true)
			w := t4Wrap(t, f)
			terms := 0
			f.r.NativeStop = nativeTermFunc(func(context.Context, string, string, podexec.NativeContainerBinding) error {
				terms++
				return nil
			})
			t4RunningWorker(t, f, false)
			if state == "no pod yet" {
				if err := f.store.Delete(context.Background(), f.pod); err != nil {
					t.Fatal(err)
				}
			}
			t4Delete(t, f)
			if err := t4Passes(t, f, 4); err != nil || terms != 0 || !slices.Contains(w.order, "delete "+f.worker.Name) || !f.cleanupCalled || !t4Released(f) {
				t.Fatalf("never-started worker: err=%v terms=%d writes=%v cleanup=%v released=%v", err, terms, w.order, f.cleanupCalled, t4Released(f))
			}
		})
	}
}

// Lifecycle 7: suspend drops a first attempt that never started and keeps attempt 2 fresh,
// because --resume refuses a state the engine never created (E1).
func TestNativeSuspendUnstartedFirstAttemptResumesFresh(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	// As the receipt of a fresh attempt 1 records it while no container has been seen to start (dj).
	m := f.current()
	m.Status.NativeFreshAttempt = 2
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	t4RunningWorker(t, f, false)
	t4Suspend(t, f)
	for range 3 {
		if _, err := f.dispatch(t); err != nil {
			t.Fatal(err)
		}
	}
	m = f.current()
	if m.Status.Phase != v1beta1.PhaseSuspended || m.Status.NativeFreshAttempt != 2 || !slices.Contains(w.order, "delete "+f.worker.Name) {
		t.Fatalf("unstarted suspend: phase=%s fresh=%d writes=%v", m.Status.Phase, m.Status.NativeFreshAttempt, w.order)
	}
	args, err := nativeCloneArgs(m, 2)
	if err != nil || !slices.Contains(args, "--restart") || slices.Contains(args, "--resume") {
		t.Fatalf("attempt 2 after an unstarted attempt 1 does not start over: %q %v", args, err)
	}
}

// Lifecycle 7 control: a started container gets TERM, keeps its Job and leaves the next attempt resuming.
func TestNativeSuspendStartedWorkerKeepsResume(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	t4RunningWorker(t, f, true)
	terms := 0
	f.r.NativeStop = nativeTermFunc(func(context.Context, string, string, podexec.NativeContainerBinding) error {
		terms++
		job, pod := f.worker.DeepCopy(), f.pod.DeepCopy()
		job.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
		pod.Status.Phase = corev1.PodFailed
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: pod.Status.ContainerStatuses[0].ContainerID, ExitCode: 4}}
		f.replace(job)
		f.replace(pod)
		return nil
	})
	t4Suspend(t, f)
	if _, err := f.dispatch(t); err != nil {
		t.Fatal(err)
	}
	m := f.current()
	if terms != 1 || m.Status.Phase != v1beta1.PhaseSuspended || m.Status.NativeFreshAttempt != 0 || slices.Contains(w.order, "delete "+f.worker.Name) {
		t.Fatalf("started suspend: terms=%d phase=%s fresh=%d writes=%v", terms, m.Status.Phase, m.Status.NativeFreshAttempt, w.order)
	}
}

// A worker Job edited after admission (no longer its digest) is dropped by deletion, which then
// cleans up after the gone worker.
func TestNativeDeletionDropsModifiedWorker(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	job := f.worker.DeepCopy()
	job.Labels = map[string]string{"edited": "by-hand"}
	f.replace(job)
	t4Delete(t, f)
	if err := t4Passes(t, f, 4); err != nil || !slices.Contains(w.order, "delete "+f.worker.Name) || !f.cleanupCalled || !t4Released(f) {
		t.Fatalf("modified worker deletion: err=%v writes=%v cleanup=%v released=%v", err, w.order, f.cleanupCalled, t4Released(f))
	}
}

// Lifecycle 9 (TestReviewRunningCompareBlocksDisposition): suspend drops a running pinned compare
// and clears its pin, its progress item and Verified, so a resumed finish creates a fresh one.
func TestNativeSuspendDuringCompareResumes(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	t4Wrap(t, f)
	job := installRecoveryCompare(t, f, compareData)
	t4RunningCompare(t, f, job)
	m := f.current()
	m.Status.VerificationProgress = []v1beta1.VerificationProgress{{Kind: v1beta1.VerificationProgressData, JobName: job.Name}}
	meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{Type: v1beta1.ConditionVerified, Status: metav1.ConditionUnknown, Reason: "VerificationRunning", Message: "compare in progress"})
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	t4Suspend(t, f)
	if _, err := f.dispatch(t); err != nil {
		t.Fatal(err)
	}
	m = f.current()
	if m.Status.Phase != v1beta1.PhaseSuspended || m.Status.NativeDataJobUID != "" || len(m.Status.VerificationProgress) != 0 || meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionVerified) != nil {
		t.Fatalf("suspend during a compare: phase=%s pin=%q progress=%v", m.Status.Phase, m.Status.NativeDataJobUID, m.Status.VerificationProgress)
	}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(job), &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatal("the running compare survived suspend", err)
	}
}

// Lifecycle 9 control: an ended pinned compare that kept its pod stays pinned through suspend.
func TestNativeSuspendKeepsEndedPinnedCompare(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	t4Wrap(t, f)
	job := installRecoveryCompare(t, f, compareSchema)
	t4Suspend(t, f)
	if _, err := f.dispatch(t); err != nil {
		t.Fatal(err)
	}
	if m := f.current(); m.Status.Phase != v1beta1.PhaseSuspended || m.Status.NativeSchemaJobUID != string(job.UID) {
		t.Fatalf("ended pinned compare lost on suspend: phase=%s pin=%q", m.Status.Phase, m.Status.NativeSchemaJobUID)
	}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(job), &batchv1.Job{}); err != nil {
		t.Fatal("ended pinned compare removed", err)
	}
}

// Lifecycle 9: deletion drops a running pinned compare instead of waiting hours beside a slot.
func TestNativeDeletionDropsRunningCompare(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	job := installRecoveryCompare(t, f, compareSchema)
	t4RunningCompare(t, f, job)
	t4Delete(t, f)
	if err := t4Passes(t, f, 3); err != nil || !slices.Contains(w.order, "delete "+job.Name) || !f.cleanupCalled || !t4Released(f) {
		t.Fatalf("deletion beside a running compare: err=%v writes=%v cleanup=%v released=%v", err, w.order, f.cleanupCalled, t4Released(f))
	}
	if i := slices.Index(w.order, "delete "+job.Name); slices.Contains(w.order[:i], "delete "+f.pvc.Name) {
		t.Fatalf("the claim was deleted before the compare: %v", w.order)
	}
}

// A compare-named Job that is not this Migration's holds suspend with Blocked=ForeignObject; it is never removed.
func TestNativeForeignCompareHolds(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	w := t4Wrap(t, f)
	job := installRecoveryCompare(t, f, compareSchema)
	foreign := job.DeepCopy()
	foreign.OwnerReferences[0].UID = workerForeignIdentity
	f.replace(foreign)
	t4Suspend(t, f)
	if _, err := f.dispatch(t); err != nil {
		t.Fatal(err)
	}
	if t4Blocked(f) != v1beta1.ReasonForeignObject || f.current().Status.Phase == v1beta1.PhaseSuspended || slices.Contains(w.order, "delete "+job.Name) {
		t.Fatalf("foreign compare: blocked=%q phase=%s writes=%v", t4Blocked(f), f.current().Status.Phase, w.order)
	}
}

// The retry column of the C table drops a pinned compare by UID even when it no longer matches the
// Job the current spec builds (lifecycle 4, compare part).
func TestNativeRetryDropsDriftedPinnedCompare(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	t4Wrap(t, f)
	job := installRecoveryCompare(t, f, compareSchema)
	drifted := job.DeepCopy()
	drifted.Labels = map[string]string{"drifted": "by-hand"}
	f.replace(drifted)
	for range 4 {
		if _, err := f.dispatch(t); err != nil {
			t.Fatal(err)
		}
	}
	if m := f.current(); m.Status.Attempts != 2 || m.Status.NativeSchemaJobUID != "" {
		t.Fatalf("drifted pinned compare held the retry: attempts=%d pin=%q", m.Status.Attempts, m.Status.NativeSchemaJobUID)
	}
}
