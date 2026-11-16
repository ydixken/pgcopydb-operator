package controller

// Regression tests for native finish and verification (design B T3): the review probes of
// lifecycle 1 (finish), 5 and 6 and design A's matching probes, inverted.

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// t3Reads records which worker and compare Jobs the finish read output from.
type t3Reads struct {
	NativeJobReader
	clones   int
	compares []types.UID
}

func (r *t3Reads) JobCloneNative(ctx context.Context, namespace string, uid types.UID, binding podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	r.clones++
	return r.NativeJobReader.JobCloneNative(ctx, namespace, uid, binding)
}

func (r *t3Reads) JobComparisonNative(ctx context.Context, namespace string, uid types.UID, binding podexec.NativeContainerBinding) (podexec.ComparisonOutput, error) {
	r.compares = append(r.compares, uid)
	return r.NativeJobReader.JobComparisonNative(ctx, namespace, uid, binding)
}

func t3WatchReads(f *nativeCloneFixture) *t3Reads {
	reads := &t3Reads{NativeJobReader: f.r.NativeJobs}
	f.r.NativeJobs = reads
	return reads
}

func t3SchemaFinish(t *testing.T) *nativeFinishFixture {
	t.Helper()
	f := newNativeFinishFixture(t)
	f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.m = f.current()
	return f
}

func t3CompareJob(t *testing.T, f *nativeFinishFixture) *batchv1.Job {
	t.Helper()
	job := &batchv1.Job{}
	if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema)}, job); err != nil {
		t.Fatalf("compare Job: %v", err)
	}
	return job
}

func t3ComparePods(t *testing.T, f *nativeFinishFixture, uid types.UID) []*corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := f.store.List(context.Background(), pods); err != nil {
		t.Fatal(err)
	}
	var owned []*corev1.Pod
	for i := range pods.Items {
		if o := metav1.GetControllerOf(&pods.Items[i]); o != nil && o.UID == uid {
			owned = append(owned, &pods.Items[i])
		}
	}
	return owned
}

// t3RunCompare puts the pinned compare Job and its pod back to running, as TestReviewVerifyingPassHelperChurn does.
func t3RunCompare(t *testing.T, f *nativeFinishFixture) *batchv1.Job {
	t.Helper()
	job := t3CompareJob(t, f)
	job.Status = batchv1.JobStatus{Active: 1}
	if err := f.store.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	for _, p := range t3ComparePods(t, f, job.UID) {
		p.Status.Phase = corev1.PodRunning
		p.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		if err := f.store.Status().Update(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	return job
}

func t3Pass(t *testing.T, f *nativeFinishFixture) error {
	t.Helper()
	f.m = f.current()
	_, err := nativeFinish(t, f)
	return err
}

// Lifecycle 1, finish part (TestReviewVanishedWorkerPodWedges): a Complete worker whose pod went before
// its report was read never completes; the attempt counts once and the retry gate decides.
func TestNativeCompleteWithoutReportReceiptNeverCompletes(t *testing.T) {
	for _, follow := range []bool{true, false} {
		t.Run(map[bool]string{true: probeFollowCase, false: "clone only"}[follow], func(t *testing.T) {
			var f *nativeCloneFixture
			created := func() int { return 0 }
			if follow {
				ff := newNativeFinishFixture(t)
				f, created = ff.nativeCloneFixture, func() int { return len(ff.created) }
			} else {
				f = newNativeCloneFixture(t, false, nil)
			}
			f.dropReportReceipt()
			if err := f.store.Delete(context.Background(), f.pod); err != nil {
				t.Fatal(err)
			}
			_, err := nativeDispatch(t, f.r, f.current())
			got := f.current()
			gate := got.Status.RetryGate
			if err != nil || gate == nil || gate.Exemption != v1beta1.RetryGateCounted || gate.FailedAttempt != 1 || !onFailurePath(got) {
				t.Fatalf("pod-less Complete worker without a report receipt was not counted: err=%v gate=%+v", err, gate)
			}
			if got.Status.Phase == v1beta1.PhaseCompleted || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionComplete) || got.Status.NativeReportJobUID != "" || got.Status.NativeFinalizedJobUID != "" || created() != 0 {
				t.Fatalf("pod-less worker without a report reached completion or a helper: phase=%s created=%d", got.Status.Phase, created())
			}
		})
	}
}

// Lifecycle 1: once the report receipt is recorded, the finish no longer needs the worker pod.
func TestNativeFinishAfterPodVanishedWithReceipt(t *testing.T) {
	t.Run("follow, pod removed during a compare", func(t *testing.T) {
		f := t3SchemaFinish(t)
		if err := t3Pass(t, f); err != nil || f.current().Status.NativeSchemaJobUID == "" {
			t.Fatalf("compare was not pinned: %v", err)
		}
		if err := f.store.Delete(context.Background(), f.pod); err != nil {
			t.Fatal(err)
		}
		if err := t3Pass(t, f); err != nil {
			t.Fatalf("pod-less finish with a report receipt refused: %v steps=%v", err, f.steps)
		}
		assertNativeFinished(t, f)
	})
	t.Run("clone only", func(t *testing.T) {
		f := newNativeCloneFixture(t, false, nil)
		if err := f.store.Delete(context.Background(), f.pod); err != nil {
			t.Fatal(err)
		}
		if _, err := nativeCloneFinish(t, f); err != nil || f.current().Status.Phase != v1beta1.PhaseCompleted || f.logs != 0 {
			t.Fatalf("pod-less clone finish with a report receipt refused: %v logs=%d", err, f.logs)
		}
	})
}

// nativeReportJobUID: the first pass with the pod reads the report and records the receipt alone;
// later passes read no worker log, and a receipt for another UID counts for nothing.
func TestNativeReportReceiptRecordedOnce(t *testing.T) {
	f := newNativeFinishFixture(t)
	reads := t3WatchReads(f.nativeCloneFixture)
	f.dropReportReceipt()
	result, err := nativeFinish(t, f)
	got := f.current()
	if err != nil || result.RequeueAfter != time.Nanosecond || got.Status.NativeReportJobUID != string(f.worker.UID) || reads.clones != 1 || f.patches != 1 || len(f.created) != 0 || got.Status.Phase == v1beta1.PhaseCompleted {
		t.Fatalf("report pass did not record only its receipt: result=%v err=%v receipt=%q clones=%d patches=%d created=%v", result, err, got.Status.NativeReportJobUID, reads.clones, f.patches, f.created)
	}
	if err := t3Pass(t, f); err != nil || reads.clones != 1 {
		t.Fatalf("finish after the receipt read the worker log again: err=%v clones=%d", err, reads.clones)
	}
	assertNativeFinished(t, f)

	t.Run("receipt of another worker", func(t *testing.T) {
		f := newNativeCloneFixture(t, false, nil)
		m := f.current()
		m.Status.NativeReportJobUID = "another-worker-job"
		if err := f.store.Status().Update(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Delete(context.Background(), f.pod); err != nil {
			t.Fatal(err)
		}
		f.m = f.current()
		if _, err := nativeCloneFinish(t, f); err != nil || f.current().Status.Phase == v1beta1.PhaseCompleted || !onFailurePath(f.current()) {
			t.Fatalf("a receipt for another worker stood in for this report: err=%v phase=%s", err, f.current().Status.Phase)
		}
	})
	t.Run("pod replaced during the report read", func(t *testing.T) {
		f := newNativeCloneFixture(t, false, nil)
		f.dropReportReceipt()
		f.afterLogs = func() {
			f.pod.UID = "replacement-pod"
			f.replace(f.pod)
		}
		if _, err := nativeCloneFinish(t, f); err == nil || f.patches != 0 || f.current().Status.NativeReportJobUID != "" {
			t.Fatal("report read beside a replaced pod recorded a receipt")
		}
	})
}

// The pod-less anchor is a state: a pod that appears after it, or a pod that goes after a real anchor,
// changes it, and a nil anchor admits nothing.
func TestNativeFinishRefusesPodAfterPodlessAnchor(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	ctx := context.Background()
	pod := f.pod.DeepCopy()
	if err := f.store.Delete(ctx, f.pod); err != nil {
		t.Fatal(err)
	}
	if !f.r.nativeVerificationOriginalUnchanged(ctx, f.m, f.worker, f.pvc, nativePodlessAnchor()) {
		t.Fatal("pod-less anchor of a finished worker without a pod read as changed")
	}
	if _, _, err := f.r.nativeCompletionCurrent(ctx, f.m, f.worker, f.pvc, nativePodlessAnchor()); err != nil {
		t.Fatalf("completion refused the pod-less anchor of a finished worker: %v", err)
	}
	if f.r.nativeVerificationOriginalUnchanged(ctx, f.m, f.worker, f.pvc, nil) {
		t.Fatal("nil anchor admitted")
	}
	pod.ResourceVersion = ""
	if err := f.store.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if f.r.nativeVerificationOriginalUnchanged(ctx, f.m, f.worker, f.pvc, nativePodlessAnchor()) {
		t.Fatal("pod that appeared after a pod-less anchor read as unchanged")
	}
	if _, _, err := f.r.nativeCompletionCurrent(ctx, f.m, f.worker, f.pvc, nativePodlessAnchor()); err == nil {
		t.Fatal("completion admitted a pod that appeared after a pod-less anchor")
	}
	_, anchor, err := f.r.nativeCompletionCurrent(ctx, f.m, f.worker, f.pvc, nil)
	if err != nil || nativePodless(anchor) {
		t.Fatalf("real pod anchor: %v", err)
	}
	if err := f.store.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if f.r.nativeVerificationOriginalUnchanged(ctx, f.m, f.worker, f.pvc, anchor) {
		t.Fatal("pod that went after a real anchor read as unchanged")
	}
}

// Lifecycle 5 (TestReviewLostCompareReceiptWedges, finish part): an owned compare without a pin is
// dropped, and the finish creates and pins a fresh one; only the pinned Job supplies Verified.
func TestNativeLostCompareReceiptRecovers(t *testing.T) {
	f := t3SchemaFinish(t)
	reads := t3WatchReads(f.nativeCloneFixture)
	lost := true
	f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
		if m := obj.(*v1beta1.Migration); lost && m.Status.NativeSchemaJobUID != "" {
			lost = false
			return errBoom
		}
		return c.Status().Patch(ctx, obj, p)
	}
	if err := t3Pass(t, f); err == nil {
		t.Fatal("lost compare receipt reported success")
	}
	f.patch = nil
	unpinned := t3CompareJob(t, f)
	for _, p := range t3ComparePods(t, f, unpinned.UID) {
		// The unpinned compare would report a mismatch; Verified must come from the pinned one.
		f.outputs[p.Name] = nativeComparePayload(t, f.nativeCloneFixture, compareSchema, false)
	}
	created := len(f.created)
	if err := t3Pass(t, f); err != nil || !slices.Contains(f.deleted, finishVerificationJobKind) || len(f.created) != created || f.current().Status.NativeSchemaJobUID != "" {
		t.Fatalf("unpinned compare was not dropped alone: err=%v deleted=%v created=%v", err, f.deleted, f.created[created:])
	}
	if err := t3Pass(t, f); err != nil || f.current().Status.NativeSchemaJobUID == "" || f.current().Status.NativeSchemaJobUID == string(unpinned.UID) {
		t.Fatalf("finish did not pin a fresh compare: err=%v pin=%q", err, f.current().Status.NativeSchemaJobUID)
	}
	pinned := types.UID(f.current().Status.NativeSchemaJobUID)
	if err := t3Pass(t, f); err != nil {
		t.Fatalf("finish with the fresh pinned compare: %v", err)
	}
	assertNativeFinished(t, f)
	if slices.Contains(reads.compares, unpinned.UID) || !slices.Contains(reads.compares, pinned) || !meta.IsStatusConditionTrue(f.current().Status.Conditions, v1beta1.ConditionVerified) {
		t.Fatalf("Verified did not come from the pinned compare alone: read=%v unpinned=%s pinned=%s", reads.compares, unpinned.UID, pinned)
	}
}

const (
	t3RowNotOwned    = "not owned"
	t3RowTerminating = "terminating"
)

// t3CompareRow puts the schema compare of f into one state of the C table.
func t3CompareRow(t *testing.T, f *nativeFinishFixture, row string) {
	t.Helper()
	ctx := context.Background()
	job, pod := nativeInstallCompare(t, f.nativeCloneFixture, compareSchema, true)
	switch row {
	case t3RowNotOwned:
		job.OwnerReferences = nil
		f.replace(job)
		m := f.current()
		m.Status.NativeSchemaJobUID = ""
		if err := f.store.Status().Update(ctx, m); err != nil {
			t.Fatal(err)
		}
	case "pinned, missing":
		for _, obj := range []client.Object{pod, job} {
			if err := f.store.Delete(ctx, obj); err != nil {
				t.Fatal(err)
			}
		}
	case "pinned, ended, no pod":
		if err := f.store.Delete(ctx, pod); err != nil {
			t.Fatal(err)
		}
	case "pinned, drifted":
		job.Labels["drift"] = "edited"
		if err := f.store.Update(ctx, job); err != nil {
			t.Fatal(err)
		}
	case t3RowTerminating:
		controllerutil.AddFinalizer(job, "test.io/hold")
		if err := f.store.Update(ctx, job); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Delete(ctx, job); err != nil {
			t.Fatal(err)
		}
	}
}

// C table, finish column: each pinned compare that cannot be read is dropped and unpinned, a terminating
// or foreign one holds, and none of them is ever read.
func TestNativeFinishCompareTable(t *testing.T) {
	for _, row := range []string{"pinned, missing", "pinned, ended, no pod", "pinned, drifted", t3RowTerminating, t3RowNotOwned} {
		t.Run(row, func(t *testing.T) {
			f := t3SchemaFinish(t)
			reads := t3WatchReads(f.nativeCloneFixture)
			t3CompareRow(t, f, row)
			f.dropPinned = true
			before := t3CompareObject(t, f)
			result, err := nativeFinish(t, f.refresh())
			got := f.current()
			if err != nil || len(f.created) != 0 || len(reads.compares) != 0 || got.Status.Phase == v1beta1.PhaseCompleted {
				t.Fatalf("%s: err=%v created=%v read=%v phase=%s", row, err, f.created, reads.compares, got.Status.Phase)
			}
			blocked := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ConditionBlocked)
			if row == t3RowTerminating || row == t3RowNotOwned {
				if blocked == nil || blocked.Status != metav1.ConditionTrue || blocked.Reason != v1beta1.ReasonEvidenceUnavailable || !strings.Contains(blocked.Message, compareJobName(f.m, compareSchema)) ||
					result.RequeueAfter != pollInterval || len(f.deleted) != 0 || t3CompareObject(t, f) == nil || got.Status.NativeSchemaJobUID != "" && row == t3RowNotOwned {
					t.Fatalf("%s did not hold with the Job named: blocked=%+v result=%v deleted=%v", row, blocked, result, f.deleted)
				}
				return
			}
			if got.Status.NativeSchemaJobUID != "" || blocked != nil || before != nil && t3CompareObject(t, f) != nil {
				t.Fatalf("%s was not dropped and unpinned: pin=%q blocked=%+v", row, got.Status.NativeSchemaJobUID, blocked)
			}
			// The next pass creates and pins a fresh compare after its own drain observation.
			if err := t3Pass(t, f); err != nil || f.current().Status.NativeSchemaJobUID == "" || !slices.Contains(f.created, finishVerificationJobKind) || !slices.Contains(f.created, finishEndposKind) {
				t.Fatalf("%s: no fresh compare after the drop: err=%v created=%v", row, err, f.created)
			}
		})
	}
}

func t3CompareObject(t *testing.T, f *nativeFinishFixture) *batchv1.Job {
	t.Helper()
	job := &batchv1.Job{}
	if f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema)}, job) != nil {
		return nil
	}
	return job
}

func (f *nativeFinishFixture) refresh() *nativeFinishFixture {
	f.m = f.current()
	return f
}

// The worker table leaves a finish hold to the finish: a foreign compare keeps Blocked True across
// passes without a flap, and the first pass that proceeds clears it.
func TestNativeFinishHoldDoesNotFlap(t *testing.T) {
	f := t3SchemaFinish(t)
	ctx := context.Background()
	t3CompareRow(t, f, t3RowNotOwned)
	for pass := range 3 {
		if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if !meta.IsStatusConditionTrue(f.current().Status.Conditions, v1beta1.ConditionBlocked) || f.patches != 1 {
			t.Fatalf("pass %d: hold flapped or rewrote: patches=%d blocked=%+v", pass, f.patches, meta.FindStatusCondition(f.current().Status.Conditions, v1beta1.ConditionBlocked))
		}
	}
	if err := f.store.Delete(ctx, t3CompareObject(t, f)); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.current()); err != nil {
		t.Fatal(err)
	}
	blocked := meta.FindStatusCondition(f.current().Status.Conditions, v1beta1.ConditionBlocked)
	if blocked == nil || blocked.Status != metav1.ConditionFalse || blocked.Reason != v1beta1.ReasonProceeding || len(f.created) != 0 {
		t.Fatalf("first proceeding pass did not clear the hold alone: blocked=%+v created=%v", blocked, f.created)
	}
}

// Lifecycle 6 (TestReviewVerifyingPassHelperChurn): while a pinned compare runs, a finish pass creates
// no helper Job and writes nothing.
func TestNativeVerifyingPassCreatesNoHelpers(t *testing.T) {
	f := t3SchemaFinish(t)
	if err := t3Pass(t, f); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	t3RunCompare(t, f)
	patches := f.patches
	for pass := 2; pass <= 4; pass++ {
		before := len(f.created)
		f.m = f.current()
		result, err := nativeFinish(t, f)
		if err != nil || result.RequeueAfter != pollInterval || len(f.created) != before || f.patches != patches {
			t.Fatalf("pass %d while the compare runs: err=%v result=%v created=%v patches=%d", pass, err, result, f.created[before:], f.patches-patches)
		}
	}
}

// Lifecycle 6: the pass that pins a compare has just observed the drain, and the pass after the last
// compare observes it again before cleanup.
func TestNativeObservationBeforeCompareAndCleanup(t *testing.T) {
	f := t3SchemaFinish(t)
	if err := t3Pass(t, f); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	pin := slices.Index(f.steps, finishVerificationJobKind)
	if pin < 0 || !slices.Contains(f.steps[:pin], finishEndposKind) || !slices.Contains(f.steps[:pin], finishEvidenceJobKind) {
		t.Fatalf("compare was pinned without a drain observation first: %v", f.steps)
	}
	job := t3RunCompare(t, f)
	if err := t3Pass(t, f); err != nil {
		t.Fatal(err)
	}
	job.Status = *f.worker.Status.DeepCopy()
	if err := f.store.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	for _, p := range t3ComparePods(t, f, job.UID) {
		p.Status.Phase = corev1.PodSucceeded
		p.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: p.Status.ContainerStatuses[0].ContainerID}}
		if err := f.store.Status().Update(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	last := len(f.steps)
	if err := t3Pass(t, f); err != nil {
		t.Fatalf("completing pass: %v", err)
	}
	assertNativeFinished(t, f)
	steps := f.steps[last:]
	cleanup := slices.Index(steps, jobKindCleanup)
	if cleanup < 0 || !slices.Contains(steps[:cleanup], finishEndposKind) || !slices.Contains(steps[:cleanup], finishEvidenceJobKind) {
		t.Fatalf("cleanup ran without a fresh drain observation in its pass: %v", steps)
	}
}

// Lifecycle 6: native helper Job events no longer wake passes; the worker, compare and legacy Jobs keep theirs.
func TestNativeHelperJobEventsDropped(t *testing.T) {
	hex := strings.Repeat("0123456789abcdef", 2)
	for name, delivered := range map[string]bool{
		"migration-run-1":                             true,
		"migration-compare-data":                      true,
		"migration-compare-schema":                    true,
		"migration-cleanup":                           true,
		"migration-preflight":                         true,
		"migration-gate-2":                            true,
		"migration-rehearsal-1":                       true,
		"rpgmigrate-cleanup":                          true,
		"rpgmigrate-version-" + hex[:31]:              true,
		"rpgmigrate-version-" + hex + "0":             true,
		"x-rpgmigrate-version-" + hex:                 true,
		"rpgmigrate-version-" + hex + "-run-1":        true,
		"rpgmigrate-version-" + hex:                   false,
		"rpgmigrate-preflight-" + hex:                 false,
		"rpgmigrate-retry-" + hex:                     false,
		"rpgmigrate-inventory-" + hex:                 false,
		"rpgmigrate-cleanup-" + hex:                   false,
		"rpgmigrate-cleanup-rejoin-" + hex:            false,
		"rpgmigrate-cleaned-" + hex:                   false,
		"rpgmigrate-endpos-" + hex:                    false,
		"rpgmigrate-evidence-" + hex:                  false,
		"rpgmigrate-evidence-" + strings.ToUpper(hex): true,
	} {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if got := ownedJobEvents.Create(event.CreateEvent{Object: job}); got != delivered {
			t.Errorf("%s: delivered=%v, want %v", name, got, delivered)
		}
		if got := ownedJobEvents.Update(event.UpdateEvent{ObjectOld: job, ObjectNew: job}); got != delivered {
			t.Errorf("%s update: delivered=%v, want %v", name, got, delivered)
		}
	}
}

// Target completion takes no report only after the report receipt is recorded.
func TestNativeTargetCompletionNeedsReportOrReceipt(t *testing.T) {
	f := newNativeFinishFixture(t)
	if got, err := f.r.readNativeFollowTargetCompletion(context.Background(), f.m, f.worker, f.pvc, nil); err != nil || got == nil {
		t.Fatalf("report receipt did not stand for the report: %v", err)
	}
	f.dropReportReceipt()
	created := len(f.created)
	if got, err := f.r.readNativeFollowTargetCompletion(context.Background(), f.m, f.worker, f.pvc, nil); err == nil || got != nil || len(f.created) != created {
		t.Fatalf("target completion without a report or its receipt: err=%v created=%v", err, f.created[created:])
	}
}

// The next attempt's receipt clears the report receipt of the last worker.
func TestNativeAttemptReceiptClearsReportReceipt(t *testing.T) {
	f := newNativeCloneFixture(t, true, nil)
	next := f.worker.DeepCopy()
	next.Name, next.UID = jobName(f.m, 2), "clone-next-job"
	desired := f.m.DeepCopy()
	if !f.r.applyNativeAttemptReceipt(desired, next, f.pvc, strings.Repeat("a", 64)) || desired.Status.Attempts != 2 || desired.Status.NativeReportJobUID != "" {
		t.Fatalf("attempt receipt kept the last report receipt: %q", desired.Status.NativeReportJobUID)
	}
}
