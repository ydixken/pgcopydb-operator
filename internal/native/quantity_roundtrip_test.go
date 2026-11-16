package native

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// apiDecoded returns v as a client sees it after the API server stored and returned it.
func apiDecoded[T any](t *testing.T, v *T) *T {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var back T
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	return &back
}

// retryRow names the retry case, which other reader tests in this package also name.
const retryRow = "retry"

// Admission rebuilds each helper's volumes and compares them with reflect.DeepEqual, and a
// NewQuantity size limit can differ from its decoded form, so admission is checked on decoded objects.
func TestNativeAdmissionAcceptsAPIDecodedSpecs(t *testing.T) {
	for name, row := range map[string]struct {
		fixture func(*testing.T) retryFixture
		admit   func(retryFixture) error
	}{
		"evidence": {func(t *testing.T) retryFixture { return retryFixture{base: newAdmissionFixture(t)} }, func(f retryFixture) error {
			_, err := validateAdmissionObjects(f.base.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, false)
			return err
		}},
		retryRow: {newRetryFixture, func(f retryFixture) error {
			_, err := validateRetryObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false)
			return err
		}},
		"owned_inventory": {newOwnedFixture, func(f retryFixture) error {
			_, err := validateOwnedObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false)
			return err
		}},
		"cleaned_completion": {newCleanedFixture, func(f retryFixture) error {
			_, err := validateCleanedObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, fixtureEndLSN, false)
			return err
		}},
		"cleanup_rejoin_completion": {newCleanupRejoinFixture, func(f retryFixture) error {
			_, err := validateCleanupRejoinObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, fixtureEndLSN, false)
			return err
		}},
	} {
		t.Run(name, func(t *testing.T) {
			f := row.fixture(t)
			if err := row.admit(f); err != nil {
				t.Fatalf("built fixture refused, so the decoded case below proves nothing: %v", err)
			}
			if !hasMemoryEmptyDir(f.base.job.Spec.Template.Spec.Volumes) {
				t.Fatal("helper has no size-limited memory emptyDir to round-trip")
			}
			b := &f.base
			b.m, b.pvc, b.job, b.worker, b.pod = apiDecoded(t, b.m), apiDecoded(t, b.pvc), apiDecoded(t, b.job), apiDecoded(t, b.worker), apiDecoded(t, b.pod)
			if f.workerPod != nil {
				f.workerPod = apiDecoded(t, f.workerPod)
			}
			if err := row.admit(f); err != nil {
				t.Fatalf("API-decoded helper refused: %v", err)
			}
		})
	}
}

func hasMemoryEmptyDir(volumes []corev1.Volume) bool {
	for _, v := range volumes {
		if v.EmptyDir != nil && v.EmptyDir.Medium == corev1.StorageMediumMemory && v.EmptyDir.SizeLimit != nil {
			return true
		}
	}
	return false
}
