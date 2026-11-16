package controller

// Regression tests for native finish and verification (design B T3): the review probes of
// lifecycle 1 (finish) and design A's matching probes, inverted.

import (
	"context"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
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
