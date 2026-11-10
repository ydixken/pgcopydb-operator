package native

import (
	"context"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	receiptBothMissing    = "receipt fields absent"
	receiptJobMissing     = "recorded Job absent"
	receiptPVCMissing     = "recorded PVC absent"
	receiptJobReplaced    = "recorded Job changed"
	receiptPVCReplaced    = "recorded PVC changed"
	receiptHelperAsWorker = "helper recorded as worker"
	receiptPriorJob       = "receipt-prior-job-uid"
	receiptPriorPVC       = "receipt-prior-pvc-uid"
	receiptCurrentJob     = "receipt-job-uid"
	receiptCurrentPVC     = "receipt-pvc-uid"
	receiptOwnedReader    = "inventory reader"
	receiptRetryReader    = "retry reader"
	receiptNilMigration   = "absent Migration"
)

func TestNativeReceiptCompletedAdmission(t *testing.T) {
	for _, field := range []string{receiptBothMissing, receiptJobMissing, receiptPVCMissing, receiptJobReplaced, receiptPVCReplaced, receiptHelperAsWorker} {
		t.Run(field, func(t *testing.T) {
			f := newAdmissionFixture(t)
			f.m.Status.NativeJobUID = string(f.want.WorkerJobUID)
			f.m.Status.NativePVCUID = string(f.want.PVCUID)
			switch field {
			case receiptBothMissing:
				f.m.Status.NativeJobUID, f.m.Status.NativePVCUID = "", ""
			case receiptJobMissing:
				f.m.Status.NativeJobUID = ""
			case receiptPVCMissing:
				f.m.Status.NativePVCUID = ""
			case receiptJobReplaced:
				f.m.Status.NativeJobUID = receiptPriorJob
			case receiptPVCReplaced:
				f.m.Status.NativePVCUID = receiptPriorPVC
			case receiptHelperAsWorker:
				f.m.Status.NativeJobUID = string(f.want.JobUID)
			}
			if admittedMigration(f.want, f.m, f.worker) {
				t.Fatal("completed admission accepted absent or mismatched original UID receipts")
			}
		})
	}
}

func TestNativeReceiptSourceClientAdmission(t *testing.T) {
	for _, field := range []string{receiptBothMissing, receiptJobReplaced, receiptPVCReplaced} {
		t.Run(field, func(t *testing.T) {
			s, c := newSourceAPI(t)
			s.migration.Status.NativeJobUID = string(s.want.WorkerJobUID)
			s.migration.Status.NativePVCUID = string(s.want.PVCUID)
			switch field {
			case receiptBothMissing:
				s.migration.Status.NativeJobUID, s.migration.Status.NativePVCUID = "", ""
			case receiptJobReplaced:
				s.migration.Status.NativeJobUID = receiptPriorJob
			default:
				s.migration.Status.NativePVCUID = receiptPriorPVC
			}
			value, err := c.CurrentEndLSN(context.Background(), s.migration, s.want)
			if err == nil || value != "" || s.calls != 0 {
				t.Fatal("source client reached exec without exact recorded Job/PVC receipts")
			}
		})
	}
}

func TestNativeReceiptHelperAdmission(t *testing.T) {
	for _, role := range []string{receiptRetryReader, receiptOwnedReader} {
		for _, field := range []string{receiptBothMissing, receiptJobMissing, receiptPVCMissing, receiptJobReplaced, receiptPVCReplaced, receiptHelperAsWorker} {
			t.Run(role+"/"+field, func(t *testing.T) {
				f := newRetryFixture(t)
				if role == receiptOwnedReader {
					f = newOwnedFixture(t)
				}
				m := f.base.m
				m.Status.NativeJobUID, m.Status.NativePVCUID = string(f.want.WorkerJobUID), string(f.want.PVCUID)
				switch field {
				case receiptBothMissing:
					m.Status.NativeJobUID, m.Status.NativePVCUID = "", ""
				case receiptJobMissing:
					m.Status.NativeJobUID = ""
				case receiptPVCMissing:
					m.Status.NativePVCUID = ""
				case receiptJobReplaced:
					m.Status.NativeJobUID = receiptPriorJob
				case receiptPVCReplaced:
					m.Status.NativePVCUID = receiptPriorPVC
				case receiptHelperAsWorker:
					m.Status.NativeJobUID = string(f.want.JobUID)
				}
				var err error
				if role == receiptOwnedReader {
					_, err = validateOwnedObjects(f.want, m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false)
				} else {
					_, err = validateRetryObjects(f.want, m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false)
				}
				if err == nil {
					t.Fatal("independent helper admission accepted missing or mismatched UID receipts")
				}
			})
		}
	}
}

func TestNativeReceiptIndependentReaders(t *testing.T) {
	for _, role := range []string{"completed reader", receiptRetryReader, receiptOwnedReader} {
		for _, field := range []string{receiptBothMissing, receiptJobReplaced, receiptPVCReplaced} {
			t.Run(role+"/"+field, func(t *testing.T) {
				var api *readerAPI
				var invoke func() error
				switch role {
				case receiptRetryReader:
					var reader *Reader
					var w ExecutionData
					api, reader, w = newRetryReaderAPI(t, RetryInputs{})
					invoke = func() error { _, err := reader.ReadRetryReadiness(context.Background(), w, RetryInputs{}); return err }
				case receiptOwnedReader:
					var reader *Reader
					var w ExecutionData
					api, reader, w = newOwnedReaderAPI(t)
					invoke = func() error { _, err := reader.ReadOwnedState(context.Background(), w); return err }
				default:
					var reader *Reader
					api, reader = newReaderAPI(t)
					invoke = func() error {
						_, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN)
						return err
					}
				}
				switch field {
				case receiptBothMissing:
					api.fixture.m.Status.NativeJobUID, api.fixture.m.Status.NativePVCUID = "", ""
				case receiptJobReplaced:
					api.fixture.m.Status.NativeJobUID = receiptPriorJob
				default:
					api.fixture.m.Status.NativePVCUID = receiptPriorPVC
				}
				if err := invoke(); err == nil || api.relayCalls != 0 || api.writerCalls != 0 || api.logCalls != 0 {
					t.Fatal("independent Reader reached transport without exact status receipts")
				}
			})
		}
	}
}

func receiptMigration(job, pvc string) *v1beta1.Migration {
	return &v1beta1.Migration{Spec: v1beta1.MigrationSpec{Engine: v1beta1.MigrationEngineRPGMigrate}, Status: v1beta1.MigrationStatus{NativeJobUID: job, NativePVCUID: pvc}}
}

func TestNativeReceiptsMatch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		m        *v1beta1.Migration
		job, pvc types.UID
		want     bool
	}{
		{"exact", receiptMigration(receiptCurrentJob, receiptCurrentPVC), receiptCurrentJob, receiptCurrentPVC, true},
		{receiptNilMigration, nil, receiptCurrentJob, receiptCurrentPVC, false},
		{"absent", receiptMigration("", ""), receiptCurrentJob, receiptCurrentPVC, false},
		{"job absent", receiptMigration("", receiptCurrentPVC), receiptCurrentJob, receiptCurrentPVC, false},
		{"PVC absent", receiptMigration(receiptCurrentJob, ""), receiptCurrentJob, receiptCurrentPVC, false},
		{"job replacement", receiptMigration(receiptCurrentJob, receiptCurrentPVC), "replacement-job", receiptCurrentPVC, false},
		{"PVC replacement", receiptMigration(receiptCurrentJob, receiptCurrentPVC), receiptCurrentJob, "replacement-pvc", false},
		{"empty actual", receiptMigration("", receiptCurrentPVC), "", receiptCurrentPVC, false},
		{"zero actual", receiptMigration("0-0", receiptCurrentPVC), "0-0", receiptCurrentPVC, false},
		{"invalid actual", receiptMigration("bad/job", receiptCurrentPVC), "bad/job", receiptCurrentPVC, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if NativeReceiptsMatch(tc.m, tc.job, tc.pvc) != tc.want {
				t.Fatal("receipt comparison lost exact nonzero UID binding")
			}
		})
	}
}
