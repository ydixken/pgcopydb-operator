package controller

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const cleanedOwnerKind = "Migration"

func cleanedJobInputs(t *testing.T) (*v1beta1.Migration, native.ExecutionData) {
	t.Helper()
	m, want := nativeEvidenceJobInputs(t)
	m.Status.NativeJobUID, m.Status.NativePVCUID = string(want.WorkerJobUID), string(want.PVCUID)
	m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: nativeEndposTestE}
	return m, native.ExecutionData{Namespace: want.Namespace, MigrationName: want.MigrationName, PVCName: want.PVCName, JobName: "rpgmigrate-cleaned-" + want.OperationID, MigrationUID: want.MigrationUID, PVCUID: want.PVCUID, WorkerJobUID: want.WorkerJobUID, MigrationGeneration: want.MigrationGeneration, Attempt: want.Attempt, Role: native.JobRoleEvidence, Work: want.Work, ImageRef: want.ImageRef, PlatformImageID: want.PlatformImageID, OperationID: want.OperationID, ProgramSHA256: native.CleanedCompletionProgramSHA256()}
}

func TestNativeCleanedCompletionJob(t *testing.T) {
	for _, source := range []v1beta1.PostgresConnection{passwordMigration().Spec.Source, secretRefTarget(), uriSecretRefTarget()} {
		for _, target := range []v1beta1.PostgresConnection{passwordMigration().Spec.Target, secretRefTarget(), uriSecretRefTarget()} {
			m, w := cleanedJobInputs(t)
			m.Spec.Source, m.Spec.Target = source, target
			before := m.DeepCopy()
			job, err := buildNativeCleanedCompletionJob(m, nativeEvidenceTestImage, w)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, m) || job.UID != "" || len(job.OwnerReferences) != 0 || job.Name != w.JobName {
				t.Fatal("cleaned projection changed inputs or invented admitted identity")
			}
			pod, c := job.Spec.Template.Spec, job.Spec.Template.Spec.Containers[0]
			assertNativeEvidencePodIsolation(t, pod)
			assertNativeEvidencePodSecurity(t, pod)
			assertNativeEvidenceContainerIsolation(t, c, w.ImageRef)
			command, args, env, volumes, mounts, err := native.CleanedCompletionMaterialization(m, w)
			if err != nil || !slices.Equal(c.Command, command) || !slices.Equal(c.Args, args) || !reflect.DeepEqual(c.Env, env) || !reflect.DeepEqual(pod.Volumes, volumes) || !reflect.DeepEqual(c.VolumeMounts, mounts) {
				t.Fatal("builder diverged from canonical cleaned materialization")
			}
			if !pod.Volumes[0].PersistentVolumeClaim.ReadOnly || !c.VolumeMounts[0].ReadOnly || job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 90 || job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
				t.Fatal("cleaned proof lost isolation or bounded lifetime")
			}
			controller := true
			job.OwnerReferences = []metav1.OwnerReference{{APIVersion: v1beta1.GroupVersion.String(), Kind: cleanedOwnerKind, Name: m.Name, UID: m.UID, Controller: &controller}}
			data, err := json.Marshal(job)
			if err != nil || native.ValidateEvidenceJobSize(job) != nil {
				t.Fatal("cleaned Job exceeds cap")
			}
			total, maximum := 0, 0
			for _, arg := range append(slices.Clone(c.Command), c.Args...) {
				total += len(arg) + 1
				maximum = max(maximum, len(arg)+1)
			}
			t.Logf("decorated_job_json=%d argv_NUL=%d max_arg_NUL=%d source_uri=%t target_uri=%t", len(data), total, maximum, source.URISecretRef != nil, target.URISecretRef != nil)
		}
	}
}

func TestNativeCleanedCompletionJobRefusals(t *testing.T) {
	for _, edit := range []func(*v1beta1.Migration, *native.ExecutionData){
		func(m *v1beta1.Migration, _ *native.ExecutionData) { m.Status.NativeJobUID = "" },
		func(m *v1beta1.Migration, _ *native.ExecutionData) { m.Spec.Suspend = true },
		func(m *v1beta1.Migration, _ *native.ExecutionData) { m.Status.Replication = nil },
		func(_ *v1beta1.Migration, w *native.ExecutionData) { w.Role = native.JobRoleCleanup },
		func(_ *v1beta1.Migration, w *native.ExecutionData) { w.ProgramSHA256 = native.EvidenceProgramSHA256() },
		func(_ *v1beta1.Migration, w *native.ExecutionData) {
			w.JobName = "rpgmigrate-evidence-" + w.OperationID
		},
	} {
		m, w := cleanedJobInputs(t)
		edit(m, &w)
		if job, err := buildNativeCleanedCompletionJob(m, nativeEvidenceTestImage, w); err == nil || job != nil {
			t.Fatal("invalid cleaned helper projection admitted")
		}
	}
}

func TestNativeCleanedCompletionJobTTLRetention(t *testing.T) {
	zero, short, finite := int32(0), int32(1), int32(120)
	for _, ttl := range []*int32{nil, &zero, &short, &finite} {
		m, w := cleanedJobInputs(t)
		m.Spec.TTLSecondsAfterFinished = ttl
		job, err := buildNativeCleanedCompletionJob(m, nativeEvidenceTestImage, w)
		if err != nil || job.Spec.TTLSecondsAfterFinished != nil {
			t.Fatal("cleaned proof helper TTL can delete unconsumed authority", err)
		}
		if !reflect.DeepEqual(m.Spec.TTLSecondsAfterFinished, ttl) {
			t.Fatal("cleaned helper retention changed selected Migration TTL")
		}
	}
}
