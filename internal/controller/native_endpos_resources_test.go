package controller

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const nativeEndposTestName = "rpgmigrate-endpos-0123456789abcdef0123456789abcdef"
const nativeEndposTestE = "1/23456789"

func nativeEndposJobInputs() (*v1beta1.Migration, *corev1.PersistentVolumeClaim) {
	m, pvc := nativeIdentityObjects()
	m.Generation = 1
	m.Spec.Source, m.Spec.Target = passwordMigration().Spec.Source, passwordMigration().Spec.Target
	m.Spec.Target.PasswordSecretRef = m.Spec.Source.PasswordSecretRef.DeepCopy()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	m.Status.Attempts, m.Status.JobName = 1, jobName(m, 1)
	m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: nativeEndposTestE}
	return m, pvc
}

func TestNativeEndposJob(t *testing.T) {
	m, pvc := nativeEndposJobInputs()
	before, beforePVC := m.DeepCopy(), pvc.DeepCopy()
	job, err := buildNativeEndposJob(m, pvc, nativeEvidenceTestImage, nativeEndposTestName, nativeEndposTestE)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, m) || !reflect.DeepEqual(beforePVC, pvc) || job.UID != "" || len(job.OwnerReferences) != 0 || job.Name != nativeEndposTestName || job.Namespace != m.Namespace {
		t.Fatal("builder mutated input or invented allocated identity/admission")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 90 || job.Spec.Completions == nil || *job.Spec.Completions != 1 || job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 {
		t.Fatal("endpos helper can restart or outlive its operation")
	}
	pod, container := job.Spec.Template.Spec, job.Spec.Template.Spec.Containers[0]
	assertNativeEvidencePodIsolation(t, pod)
	assertNativeEvidencePodSecurity(t, pod)
	assertNativeEvidenceContainerIsolation(t, container, nativeEvidenceTestImage)
	if !slices.Equal(container.Args, []string{"stream", "sentinel", "set", "endpos", nativeEndposTestE, "--json", nativeEndposDirOption, "/work/pgcopydb"}) || len(container.Command) != 4 || container.Command[0] != shellPath || container.Command[1] != "-c" || container.Command[3] != nativeWorkerTestExecutable {
		t.Fatal("endpos helper did not invoke the explicit saved boundary")
	}
	if strings.Contains(container.Command[2], "PGCOPYDB_SOURCE") || strings.Contains(container.Command[2], "psql") || strings.Contains(container.Command[2], "--current") {
		t.Fatal("endpos helper added a source operation")
	}
	assertNativeWorkerIdentity(t, container.Env, pvc.UID)
	if !slices.Contains(container.Env, corev1.EnvVar{Name: probeTestTimeoutEnvironment, Value: "10"}) {
		t.Fatal("endpos helper lost its connection timeout")
	}
	assertNativeEndposMounts(t, pod, container)
}

func assertNativeEndposMounts(t *testing.T, pod corev1.PodSpec, container corev1.Container) {
	t.Helper()
	for _, volume := range pod.Volumes {
		if volume.Name == workdirVolume && (volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName != "migration-work") {
			t.Fatal("endpos helper changed the owned work PVC")
		}
		if volume.Name == nativeEndposTmpVolume && (volume.EmptyDir == nil || volume.EmptyDir.Medium != corev1.StorageMediumMemory || volume.EmptyDir.SizeLimit == nil || volume.EmptyDir.SizeLimit.Value() != 64*1024*1024) {
			t.Fatal("endpos helper lost bounded private temporary storage")
		}
	}
	if len(pod.Volumes) < 2 || pod.Volumes[0].Name != workdirVolume || pod.Volumes[1].Name != nativeEndposTmpVolume || !slices.Contains(container.VolumeMounts, corev1.VolumeMount{Name: workdirVolume, MountPath: "/work"}) {
		t.Fatal("endpos helper lost its owned mounts")
	}
}

func TestNativeEndposJobTargetOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		conn v1beta1.PostgresConnection
	}{
		{nativeEvidenceInlineForm, passwordMigration().Spec.Source},
		{nativeWorkerSecretForm, secretRefTarget()},
		{nativeWorkerURISecretForm, uriSecretRefTarget()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, pvc := nativeEndposJobInputs()
			m.Spec.Target = tc.conn
			m.Spec.Runner.Image = nativeEvidenceTestImage
			// An unusable source must never prevent this target-only acknowledgment.
			m.Spec.Source.SynchronousCommit = "private-source-canary"
			m.Spec.Source.URISecretRef = &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "private-source-secret"}, Key: "source-uri"}
			m.Spec.Clone.Filters = &v1beta1.Filters{ExcludeSchemas: []string{"unused"}}
			job, err := buildNativeEndposJob(m, pvc, "unused-ack-default", nativeEndposTestName, nativeEndposTestE)
			if err != nil {
				t.Fatal(err)
			}
			c := job.Spec.Template.Spec.Containers[0]
			if c.Image != nativeEvidenceTestImage {
				t.Fatal("endpos helper ignored the immutable runner image override")
			}
			material, err := conn.Materialize(conn.Target, &m.Spec.Target)
			if err != nil {
				t.Fatal(err)
			}
			for _, env := range material.Env {
				if !slices.ContainsFunc(c.Env, func(e corev1.EnvVar) bool { return reflect.DeepEqual(e, env) }) {
					t.Fatal("target connection materialization changed")
				}
			}
			for _, e := range c.Env {
				if strings.Contains(e.Name, "SOURCE") || (e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil && e.ValueFrom.SecretKeyRef.Name == "private-source-secret") {
					t.Fatal("endpos helper projected a source connection")
				}
			}
			for _, v := range job.Spec.Template.Spec.Volumes {
				if strings.HasPrefix(v.Name, "source") || v.Name == probeFiltersCase {
					t.Fatal("endpos helper mounted source credentials or clone configuration")
				}
			}
			if strings.Contains(strings.Join(c.Command, " "), "private-source") || strings.Contains(strings.Join(c.Args, " "), "private-source") {
				t.Fatal("endpos launcher exposed source configuration")
			}
		})
	}
}

func TestNativeEndposJobRefusesInvalidBoundaryOrIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(**v1beta1.Migration, **corev1.PersistentVolumeClaim, *string, *string)
	}{
		{"absent ACK migration", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) { *m = nil }},
		{"nil pvc", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _, _ *string) { *p = nil }},
		{"non-native ACK migration", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) { (*m).Spec.Engine = "" }},
		{"not follow", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) { (*m).Spec.Follow = nil }},
		{"zero generation", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) { (*m).Generation = 0 }},
		{"deleting migration", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) {
			now := metav1.Now()
			(*m).DeletionTimestamp = &now
		}},
		{"zero attempt", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) { (*m).Status.Attempts = 0 }},
		{"foreign worker name", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) {
			(*m).Status.JobName = "other-ack-worker"
		}},
		{"no persisted boundary", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) {
			(*m).Status.Replication = nil
		}},
		{"different persisted boundary", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) {
			(*m).Status.Replication.Endpos = "1/23456790"
		}},
		{"zero boundary", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string, e *string) {
			*e = sentinel.ZeroLSN
			(*m).Status.Replication.Endpos = *e
		}},
		{"malformed boundary", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string, e *string) {
			*e = "$(private-canary)"
			(*m).Status.Replication.Endpos = *e
		}},
		{"whitespace boundary", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string, e *string) {
			*e = " " + nativeEndposTestE
			(*m).Status.Replication.Endpos = *e
		}},
		{"unbounded zero padding", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _ *string, e *string) {
			*e = strings.Repeat("0", 65536) + nativeEndposTestE
			(*m).Status.Replication.Endpos = *e
		}},
		{"wrong ACK role name", func(_ **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, n, _ *string) {
			*n = "rpgmigrate-evidence-0123456789abcdef0123456789abcdef"
		}},
		{"unsafe operation", func(_ **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, n, _ *string) {
			*n = "rpgmigrate-endpos-$(private-canary)"
		}},
		{"uppercase ACK operation", func(_ **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, n, _ *string) {
			*n = "rpgmigrate-endpos-0123456789ABCDEF0123456789abcdef"
		}},
		{"foreign pvc", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _, _ *string) {
			(*p).OwnerReferences[0].UID = workerForeignIdentity
		}},
		{"missing pvc uid", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _, _ *string) { (*p).UID = "" }},
		{"deleting pvc", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim, _, _ *string) {
			now := metav1.Now()
			(*p).DeletionTimestamp = &now
		}},
		{"mutable image", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) {
			(*m).Spec.Runner.Image = "private-canary:latest"
		}},
		{"invalid target", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) {
			(*m).Spec.Target.URISecretRef = uriSecretRefTarget().URISecretRef
			(*m).Spec.Target.SynchronousCommit = "private-canary"
		}},
		{"oversize prelude", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim, _, _ *string) {
			(*m).Spec.Target.Host = strings.Repeat("a", 65536)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, pvc := nativeEndposJobInputs()
			name, e := nativeEndposTestName, nativeEndposTestE
			tc.edit(&m, &pvc, &name, &e)
			job, err := buildNativeEndposJob(m, pvc, nativeEvidenceTestImage, name, e)
			if job != nil || err == nil {
				t.Fatal("unadmitted boundary or identity produced a helper")
			}
			if strings.Contains(err.Error(), "private-canary") {
				t.Fatal("builder refusal disclosed input")
			}
		})
	}
}

func TestNativeEndposJobLSNRange(t *testing.T) {
	for _, e := range []string{"0/1", "FFFFFFFF/FFFFFFFF", "ffffffff/ffffffff", "00000000/00000001"} {
		t.Run(e, func(t *testing.T) {
			m, pvc := nativeEndposJobInputs()
			m.Status.Replication.Endpos = e
			m.Status.Attempts = 2
			m.Status.JobName = jobName(m, 2)
			job, err := buildNativeEndposJob(m, pvc, nativeEvidenceTestImage, nativeEndposTestName, e)
			if err != nil || job == nil || job.Spec.Template.Spec.Containers[0].Args[4] != e {
				t.Fatal("valid saved boundary was rejected or normalized")
			}
		})
	}
}

func TestNativeEndposJobExecutionHashExcludesCredentials(t *testing.T) {
	m, pvc := nativeEndposJobInputs()
	m.Spec.Target.PasswordSecretRef = nil
	first, err := buildNativeEndposJob(m, pvc, nativeEvidenceTestImage, nativeEndposTestName, nativeEndposTestE)
	if err != nil {
		t.Fatal(err)
	}
	m.Spec.Target.Host = "private-rotated-target.example"
	second, err := buildNativeEndposJob(m, pvc, nativeEvidenceTestImage, nativeEndposTestName, nativeEndposTestE)
	if err != nil {
		t.Fatal(err)
	}
	if podexec.NativeExecutionSHA256(first.Spec.Template) != podexec.NativeExecutionSHA256(second.Spec.Template) || reflect.DeepEqual(first.Spec.Template.Spec.Containers[0].Env, second.Spec.Template.Spec.Containers[0].Env) {
		t.Fatal("credential values entered the hash or disappeared from exact admission input")
	}
}
