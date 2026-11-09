package controller

import (
	"reflect"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func nativeCompareInputs(t *testing.T) (*v1beta1.Migration, *corev1.PersistentVolumeClaim, *batchv1.Job, native.WorkIdentity) {
	t.Helper()
	m, pvc := nativeIdentityObjects()
	m.Generation = 1
	m.Status.Attempts = 1
	m.Status.JobName = jobName(m, 1)
	m.Spec.Source, m.Spec.Target = passwordMigration().Spec.Source, passwordMigration().Spec.Target
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
	controller := true
	worker := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: m.Status.JobName, Namespace: m.Namespace, UID: "owned-worker", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1beta1.GroupVersion.String(), Kind: pvc.OwnerReferences[0].Kind, Name: m.Name, UID: m.UID, Controller: &controller}}}}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		t.Fatal(err)
	}
	return m, pvc, worker, work
}

func TestNativeVerificationRecovery(t *testing.T) {
	m, pvc, worker, work := nativeCompareInputs(t)
	op, err := nativeComparisonOperationID(m, pvc, worker, work, "data")
	if err != nil || op != "5f886423f78dd81fe21fe65a58c18554" {
		t.Fatal("comparison scope unavailable")
	}
	again, err := nativeComparisonOperationID(m.DeepCopy(), pvc.DeepCopy(), worker.DeepCopy(), work, "data")
	if err != nil || again != op {
		t.Fatal("restart changed correlation")
	}
	for name, edit := range map[string]func(*v1beta1.Migration, *corev1.PersistentVolumeClaim, *batchv1.Job){
		"generation": func(m *v1beta1.Migration, _ *corev1.PersistentVolumeClaim, _ *batchv1.Job) { m.Generation++ },
		"worker UID": func(_ *v1beta1.Migration, _ *corev1.PersistentVolumeClaim, w *batchv1.Job) { w.UID = "new-worker" },
		"PVC":        func(_ *v1beta1.Migration, p *corev1.PersistentVolumeClaim, _ *batchv1.Job) { p.UID = "new-pvc" },
	} {
		t.Run(name, func(t *testing.T) {
			a, b, c := m.DeepCopy(), pvc.DeepCopy(), worker.DeepCopy()
			edit(a, b, c)
			w, err := nativeWorkIdentity(a, b)
			if err != nil {
				t.Fatal(err)
			}
			id, err := nativeComparisonOperationID(a, b, c, w, "data")
			if err != nil || id == op {
				t.Fatal("changed execution kept operation scope")
			}
		})
	}
	worker.Annotations = map[string]string{probeOperationKey: "observed-not-authority"}
	if id, err := nativeComparisonOperationID(m, pvc, worker, work, "data"); err != nil || id != op {
		t.Fatal("observed operation hint changed expectation")
	}
	worker.UID = ""
	if _, err := nativeComparisonOperationID(m, pvc, worker, work, "data"); err == nil {
		t.Fatal("absent original UID accepted")
	}
}

func TestNativeVerificationCompletion(t *testing.T) {
	m, pvc, worker, _ := nativeCompareInputs(t)
	before := m.DeepCopy()
	job, err := buildNativeCompareJob(m, pvc, worker, nativeEvidenceTestImage, "data")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, before) || job.Spec.ActiveDeadlineSeconds != nil || job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatal("builder mutated input or bounded/retried scan")
	}
	if job.Name != compareJobName(m, "data") || job.UID != "" || len(job.OwnerReferences) != 0 {
		t.Fatal("builder claimed execution authority")
	}
	var work, tmp bool
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == workdirVolume {
			work = volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name
		}
		if volume.Name == "tmp" {
			tmp = volume.EmptyDir != nil && volume.EmptyDir.Medium == corev1.StorageMediumMemory && volume.EmptyDir.SizeLimit.Value() == 64*1024*1024
		}
	}
	if !work || !tmp {
		t.Fatal("comparison lost owned work or memory tmp")
	}
	c := job.Spec.Template.Spec.Containers[0]
	if len(c.Args) != 14 || c.Args[9] != "data" || c.Command[3] != "/usr/bin/env" || strings.Contains(strings.Join(c.Args, " "), "compare_data_strict") || strings.Contains(strings.Join(c.Args, " "), "psql ") {
		t.Fatal("comparison used legacy report evaluation")
	}
	if native.ValidateEvidenceJobSize(job) != nil {
		t.Fatal("comparison exceeds complete job cap")
	}
}
