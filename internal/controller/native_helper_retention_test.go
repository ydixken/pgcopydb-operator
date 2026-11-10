package controller

import (
	"reflect"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
)

func TestNativeHelperTTLRetention(t *testing.T) {
	f := newCleanupInventoryFixture(t)
	owned, err := f.read(t)
	if err != nil {
		t.Fatal(err)
	}
	baseline := make(map[string]*batchv1.Job)
	zero, short, finite := int32(0), int32(1), int32(120)
	for _, tc := range []struct {
		name string
		ttl  *int32
	}{{"unspecified TTL", nil}, {"immediate TTL", &zero}, {"one second TTL", &short}, {"120 second TTL", &finite}} {
		t.Run(tc.name, func(t *testing.T) {
			m, pvc := nativeEndposJobInputs()
			m.Spec.TTLSecondsAfterFinished = tc.ttl
			comparison, comparisonPVC, worker, _ := nativeCompareInputs(t)
			comparison.Spec.TTLSecondsAfterFinished = tc.ttl
			evidence, want := nativeEvidenceJobInputs(t)
			evidence.Spec.TTLSecondsAfterFinished = tc.ttl
			cleanup := f.migration.DeepCopy()
			cleanup.Spec.TTLSecondsAfterFinished = tc.ttl
			before := []*v1beta1.Migration{m.DeepCopy(), comparison.DeepCopy(), evidence.DeepCopy(), cleanup.DeepCopy()}
			for role, build := range map[string]func() (*batchv1.Job, error){
				"version discovery": func() (*batchv1.Job, error) {
					return buildNativeVersionJob(m, nativeEvidenceTestImage, probeTestVersionName)
				},
				"database preflight": func() (*batchv1.Job, error) {
					return buildNativePreflightJob(m, nativeEvidenceTestImage, probeTestPreflightName, probeTestOperation)
				},
				"completed evidence": func() (*batchv1.Job, error) {
					return buildNativeEvidenceJob(evidence, nativeEvidenceTestImage, want)
				},
				"endpos": func() (*batchv1.Job, error) {
					return buildNativeEndposJob(m, pvc, nativeEvidenceTestImage, nativeEndposTestName, nativeEndposTestE)
				},
				"owned cleanup": func() (*batchv1.Job, error) {
					return buildNativeCleanupJob(cleanup, f.pvc, f.want.ImageRef, nativeCleanupTestName, owned)
				},
				"schema verification": func() (*batchv1.Job, error) {
					return buildNativeCompareJob(comparison, comparisonPVC, worker, nativeEvidenceTestImage, compareSchema)
				},
				"data verification": func() (*batchv1.Job, error) {
					return buildNativeCompareJob(comparison, comparisonPVC, worker, nativeEvidenceTestImage, compareData)
				},
			} {
				t.Run(role, func(t *testing.T) {
					job, err := build()
					if err != nil {
						t.Fatal(err)
					}
					if job.Spec.TTLSecondsAfterFinished != nil {
						t.Fatal("native helper TTL can delete evidence before checked capture and proof consumption")
					}
					if tc.ttl == nil {
						baseline[role] = job.DeepCopy()
					} else if !reflect.DeepEqual(job, baseline[role]) {
						t.Fatal("selected TTL changed the retained helper specification")
					}
				})
			}
			if !reflect.DeepEqual(before, []*v1beta1.Migration{m, comparison, evidence, cleanup}) {
				t.Fatal("helper retention changed its Migration input")
			}
			legacy := m.DeepCopy()
			legacy.Spec.Engine = v1beta1.MigrationEnginePGCopyDB
			for role, build := range map[string]func() (*batchv1.Job, error){
				"legacy worker":    func() (*batchv1.Job, error) { return buildJob(legacy, testRunnerImage, 1) },
				"legacy preflight": func() (*batchv1.Job, error) { return buildPreflightJob(legacy, testRunnerImage) },
				"legacy cleanup":   func() (*batchv1.Job, error) { return buildCleanupJob(legacy, testRunnerImage) },
				"legacy verify":    func() (*batchv1.Job, error) { return buildVerifyJob(legacy, testRunnerImage, "") },
			} {
				t.Run(role, func(t *testing.T) {
					job, err := build()
					if err != nil {
						t.Fatal(err)
					}
					wantTTL := tc.ttl
					if role == "legacy preflight" {
						wantTTL = nil
					}
					if !reflect.DeepEqual(job.Spec.TTLSecondsAfterFinished, wantTTL) {
						t.Fatal("native retention changed legacy TTL policy")
					}
				})
			}
		})
	}
}
