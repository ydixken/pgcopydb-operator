package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	receiptCollisionOutcome = "receipt collision"
	receiptMissingOutcome   = "receipts absent"
	receiptPVCMismatch      = "recorded PVC mismatch"
	receiptSourceOperation  = "source sampling"
	receiptLostStatusReply  = "lost status reply"
	receiptJobMismatch      = "job mismatch"
)

func TestNativeAttemptReceipt(t *testing.T) {
	t.Run("atomic successful create", func(t *testing.T) {
		f := newWorkerAttemptFixture(t)
		f.patch = func(ctx context.Context, c client.Client, obj client.Object, patch client.Patch) error {
			raw, err := patch.Data(obj)
			if err != nil {
				t.Fatal(err)
			}
			var payload struct {
				Status v1beta1.MigrationStatus `json:"status"`
			}
			if json.Unmarshal(raw, &payload) != nil || payload.Status.NativeJobUID != string(workerServerUID) || payload.Status.NativePVCUID != string(f.pvc.UID) || payload.Status.Attempts != 1 || payload.Status.JobName != jobName(f.m, 1) {
				t.Fatal("attempt/name and original Job/PVC UIDs were not one status patch")
			}
			return c.Status().Patch(ctx, obj, patch)
		}
		if _, err := f.r.startNativeAttempt(context.Background(), f.m, f.m.DeepCopy()); err != nil {
			t.Fatal(err)
		}
		m := f.storedMigration()
		if !native.NativeReceiptsMatch(m, workerServerUID, f.pvc.UID) || f.patches != 1 {
			t.Fatal("known Create did not durably record both receipts")
		}
	})
	for _, outcome := range []string{"existing", receiptCollisionOutcome, "lost Create reply", "failed status", receiptLostStatusReply} {
		t.Run(outcome, func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			if outcome == "existing" {
				job := f.expected()
				job.UID = workerServerUID
				if err := f.store.Create(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			} else {
				f.create = func(ctx context.Context, c client.WithWatch, obj client.Object) error {
					obj.SetUID(workerServerUID)
					if err := c.Create(ctx, obj); err != nil {
						return err
					}
					if outcome == receiptCollisionOutcome {
						return apierrors.NewAlreadyExists(batchv1.Resource("jobs"), obj.GetName())
					}
					if outcome == "lost Create reply" {
						return errors.New(workerAttemptCanary)
					}
					return nil
				}
			}
			if outcome == "failed status" || outcome == receiptLostStatusReply {
				f.patch = func(ctx context.Context, c client.Client, obj client.Object, patch client.Patch) error {
					if outcome == receiptLostStatusReply {
						if err := c.Status().Patch(ctx, obj, patch); err != nil {
							return err
						}
					}
					return errors.New(workerAttemptCanary)
				}
			}
			if _, err := f.r.startNativeAttempt(context.Background(), f.m, f.m.DeepCopy()); err == nil {
				t.Fatal("ambiguous or preexisting worker was adopted")
			}
			if outcome == receiptLostStatusReply {
				if !native.NativeReceiptsMatch(f.storedMigration(), workerServerUID, f.pvc.UID) {
					t.Fatal("lost reply erased persisted receipts")
				}
				return
			}
			if got := f.storedMigration(); got.Status.NativeJobUID != "" || got.Status.NativePVCUID != "" || got.Status.Attempts != 0 {
				t.Fatal("unknown outcome manufactured receipts")
			}
			creates, patches := f.creates, f.patches
			f.create, f.patch = nil, nil
			if _, err := f.r.startNativeAttempt(context.Background(), f.storedMigration(), f.storedMigration()); err == nil || f.creates != creates || f.patches != patches {
				t.Fatal("restart adopted a receipt-less same-name worker")
			}
		})
	}
}

func TestNativeReceiptControllerAdmission(t *testing.T) {
	for _, field := range []string{receiptMissingOutcome, receiptJobMismatch, receiptPVCMismatch} {
		t.Run("active/"+field, func(t *testing.T) {
			f := newNativeControlFixture(t)
			m := f.current()
			m.Status.NativeJobUID, m.Status.NativePVCUID = string(f.worker.UID), string(f.pvc.UID)
			if field == receiptMissingOutcome {
				m.Status.NativeJobUID, m.Status.NativePVCUID = "", ""
			}
			if field == receiptJobMismatch {
				m.Status.NativeJobUID = "prior-worker-uid"
			}
			if field == receiptPVCMismatch {
				m.Status.NativePVCUID = "prior-pvc-uid"
			}
			if err := f.store.Status().Update(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			f.m = m
			if got, err := f.r.admitNativeWorkerExecution(context.Background(), m, f.worker, f.pvc); err == nil || got != (native.ExecutionData{}) {
				t.Fatal("active admission accepted missing or mismatched receipts")
			}
			if _, err := f.r.persistNativeEnd(context.Background(), m, f.worker, f.pvc, f.control); err == nil || f.heads != 0 || f.patches != 0 {
				t.Fatal("E sampling reached transport without valid receipts")
			}
			f.saveEnd(nativeControlTestEnd)
			if raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control); err == nil || raw != nil || f.sets != 0 || f.patches != 0 {
				t.Fatal("explicit setter reached transport without valid receipts")
			}
		})
		t.Run("retained/"+field, func(t *testing.T) {
			f := newNativeRecoveryFixture(t, 1)
			f.m.Status.NativeJobUID, f.m.Status.NativePVCUID = string(f.worker.UID), string(f.pvc.UID)
			if field == receiptMissingOutcome {
				f.m.Status.NativeJobUID, f.m.Status.NativePVCUID = "", ""
			}
			if field == receiptJobMismatch {
				f.m.Status.NativeJobUID = "prior-worker-uid"
			}
			if field == receiptPVCMismatch {
				f.m.Status.NativePVCUID = "prior-pvc-uid"
			}
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := f.r.nativeRecoveryCurrent(context.Background(), f.m, f.worker, f.pvc); err == nil {
				t.Fatal("retained admission accepted missing or mismatched receipts")
			}
		})
	}
}

func TestNativeAttemptReceiptConflict(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	f.patch = func(ctx context.Context, c client.Client, obj client.Object, patch client.Patch) error {
		live := f.storedMigration()
		live.Status.Attempts, live.Status.JobName = 1, jobName(live, 1)
		live.Status.NativeJobUID, live.Status.NativePVCUID = "peer-original-job", "peer-original-pvc"
		live.Status.Replication = &v1beta1.ReplicationStatus{Endpos: nativeControlLaterEnd}
		if err := f.store.Status().Update(ctx, live); err != nil {
			t.Fatal(err)
		}
		return c.Status().Patch(ctx, obj, patch)
	}
	if _, err := f.r.startNativeAttempt(context.Background(), f.m, f.m.DeepCopy()); !apierrors.IsConflict(err) {
		t.Fatal("receipt race did not retain optimistic conflict", err)
	}
	m := f.storedMigration()
	if m.Status.NativeJobUID != "peer-original-job" || m.Status.NativePVCUID != "peer-original-pvc" || m.Status.Replication.Endpos != nativeControlLaterEnd || f.creates != 1 || f.patches != 1 || len(f.r.Recorder.(*events.FakeRecorder).Events) != 0 {
		t.Fatal("conflict overwrote receipts/E or replayed effects")
	}
}

func TestNativeReceiptChangedDuringControl(t *testing.T) {
	for _, operation := range []string{receiptSourceOperation, "setter"} {
		t.Run(operation, func(t *testing.T) {
			f := newNativeControlFixture(t)
			if operation == "setter" {
				f.saveEnd(nativeControlTestEnd)
			}
			f.afterExec = func(bool) {
				m := f.current()
				m.Status.NativeJobUID = "replacement-original-job"
				if err := f.store.Status().Update(context.Background(), m); err != nil {
					t.Error(err)
				}
			}
			if operation == receiptSourceOperation {
				if _, err := f.r.persistNativeEnd(context.Background(), f.m, f.worker, f.pvc, f.control); err == nil || f.patches != 0 || f.heads != 1 {
					t.Fatal("source response from changed receipt was accepted")
				}
			} else if raw, err := f.r.setNativePersistedEnd(context.Background(), f.m, f.worker, f.pvc, f.control); !errors.Is(err, native.ErrNativeSetEndposUnknownEffect) || raw != nil || f.patches != 0 || f.sets != 1 || f.current().Status.Replication.Endpos != nativeControlTestEnd {
				t.Fatal("unknown setter effect lost E or became acknowledgment", err)
			}
		})
	}
}

func TestNativeReceiptPodScope(t *testing.T) {
	f := newNativeControlFixture(t)
	before, err := f.r.admitNativeWorkerExecution(context.Background(), f.m, f.worker, f.pvc)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.UID = "new-invocation-pod-uid"
	if err := f.store.Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	after, err := f.r.admitNativeWorkerExecution(context.Background(), f.m, f.worker, f.pvc)
	if err != nil || after.Container.PodUID == before.Container.PodUID || after.WorkerJobUID != before.WorkerJobUID {
		t.Fatal("recorded Job did not bind a fresh unique canonical Pod per invocation", err)
	}
}
