package native

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	admissionURIKey         = "uri"
	admissionSizePaddingKey = "size-padding"
)

type admissionFixture struct {
	want        ExecutionRequest
	m           *v1beta1.Migration
	pvc         *corev1.PersistentVolumeClaim
	job, worker *batchv1.Job
	pod         *corev1.Pod
}

func admissionBool(v bool) *bool  { return &v }
func admissionI32(v int32) *int32 { return &v }
func admissionI64(v int64) *int64 { return &v }
func admissionOwner(kind, name string, uid types.UID) metav1.OwnerReference {
	version := "batch/v1"
	if kind == "Migration" {
		version = v1beta1.GroupVersion.String()
	}
	return metav1.OwnerReference{APIVersion: version, Kind: kind, Name: name, UID: uid, Controller: admissionBool(true)}
}

func newAdmissionFixture(t *testing.T) admissionFixture {
	t.Helper()
	w := readerWant()
	w.ProgramSHA256 = EvidenceProgramSHA256()
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: w.MigrationName, Namespace: w.Namespace, UID: w.MigrationUID, Generation: w.MigrationGeneration}, Spec: v1beta1.MigrationSpec{Engine: v1beta1.MigrationEngineRPGMigrate, Source: v1beta1.PostgresConnection{URISecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "source"}, Key: admissionURIKey}}, Target: v1beta1.PostgresConnection{URISecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "target"}, Key: admissionURIKey}}, Follow: &v1beta1.FollowOptions{Enabled: true}}, Status: v1beta1.MigrationStatus{Attempts: 1, JobName: "migration-run-1", Replication: &v1beta1.ReplicationStatus{Endpos: fixtureEndLSN}}}
	owner := admissionOwner("Migration", m.Name, m.UID)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: w.PVCName, Namespace: w.Namespace, UID: w.PVCUID, OwnerReferences: []metav1.OwnerReference{owner}}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	command, args, env, volumes, mounts, err := expectedEvidenceMaterialization(m, w)
	if err != nil {
		t.Fatal(err)
	}
	spec := corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: admissionBool(false), DNSPolicy: corev1.DNSClusterFirst, SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: admissionBool(true), RunAsUser: admissionI64(65532), RunAsGroup: admissionI64(65532), FSGroup: admissionI64(65532), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}, Volumes: volumes, Containers: []corev1.Container{{Name: nativeContainerName, Image: w.ImageRef, Command: command, Args: args, Env: env, VolumeMounts: mounts, SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: admissionBool(false), ReadOnlyRootFilesystem: admissionBool(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}}}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: w.JobName, Namespace: w.Namespace, UID: w.JobUID, OwnerReferences: []metav1.OwnerReference{owner}}, Spec: batchv1.JobSpec{BackoffLimit: admissionI32(0), ActiveDeadlineSeconds: admissionI64(90), Template: corev1.PodTemplateSpec{Spec: spec}}}
	worker := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: m.Status.JobName, Namespace: w.Namespace, UID: w.WorkerJobUID, OwnerReferences: []metav1.OwnerReference{owner}}, Spec: batchv1.JobSpec{Template: *job.Spec.Template.DeepCopy()}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	worker.Spec.Template.Spec.Containers[0].Command = []string{nativeShellPath, "-c", "exec \"$0\" \"$@\"", "/usr/local/bin/rpgmigrate"}
	worker.Spec.Template.Spec.Containers[0].Args = []string{nativeCloneCommand, "--follow", "--plugin", "pgoutput"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: readerPodName, Namespace: w.Namespace, UID: "pod-test", OwnerReferences: []metav1.OwnerReference{admissionOwner("Job", job.Name, job.UID)}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: nativeContainerName, ContainerID: "containerd://test", Image: w.ImageRef, ImageID: w.PlatformImageID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	w.TemplateSHA256 = podexec.NativeTemplateSHA256(job.Spec.Template)
	w.ConnectionReferenceSHA256, err = connectionReferenceSHA(m, w)
	if err != nil {
		t.Fatal(err)
	}
	return admissionFixture{w, m, pvc, job, worker, pod}
}

func TestNativeAdmissionObjects(t *testing.T) {
	f := newAdmissionFixture(t)
	if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*admissionFixture)
	}{
		{"migration_replaced", func(f *admissionFixture) { f.m.UID = readerReplacementUID }},
		{"generation", func(f *admissionFixture) { f.m.Generation++ }},
		{"pvc_replaced", func(f *admissionFixture) { f.pvc.UID = readerReplacementUID }},
		{"pvc_unbound", func(f *admissionFixture) { f.pvc.Status.Phase = corev1.ClaimPending }},
		{"job_replaced", func(f *admissionFixture) { f.job.UID = readerReplacementUID }},
		{"wrong_helper_owner", func(f *admissionFixture) { f.job.OwnerReferences[0].UID = readerReplacementUID }},
		{"wrong_pod_controller", func(f *admissionFixture) { f.pod.OwnerReferences[0].UID = readerReplacementUID }},
		{"worker_replaced", func(f *admissionFixture) { f.worker.UID = readerReplacementUID }},
		{"worker_not_complete", func(f *admissionFixture) { f.worker.Status.Conditions = nil }},
		{"attempt", func(f *admissionFixture) { f.m.Status.Attempts++ }},
		{"role", func(f *admissionFixture) { f.want.Role = JobRoleCleanup }},
		{"program", func(f *admissionFixture) { f.want.ProgramSHA256 = sha256.Sum256([]byte("other")) }},
		{"image", func(f *admissionFixture) { f.pod.Status.ContainerStatuses[0].ImageID = "other" }},
		{"restart", func(f *admissionFixture) { f.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"mount", func(f *admissionFixture) { f.pod.Spec.Containers[0].VolumeMounts[0].MountPath = "/other" }},
		{"privilege", func(f *admissionFixture) { f.pod.Spec.Containers[0].SecurityContext.Privileged = admissionBool(true) }},
		{"environment", func(f *admissionFixture) {
			f.pod.Spec.Containers[0].Env = append(f.pod.Spec.Containers[0].Env, corev1.EnvVar{Name: "LD_AUDIT", Value: "bad"})
		}},
		{"dns", func(f *admissionFixture) { f.pod.Spec.DNSPolicy = corev1.DNSDefault }},
		{"host_alias", func(f *admissionFixture) {
			f.pod.Spec.HostAliases = []corev1.HostAlias{{IP: "127.0.0.1", Hostnames: []string{"example"}}}
		}},
		{"envfrom", func(f *admissionFixture) { f.pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{Prefix: "X"}} }},
		{"locale", func(f *admissionFixture) {
			for i, e := range f.pod.Spec.Containers[0].Env {
				if e.Name == "LC_ALL" {
					f.pod.Spec.Containers[0].Env[i].Value = "other"
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAdmissionFixture(t)
			tc.change(&f)
			if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err == nil {
				t.Fatal("unsafe execution admitted")
			}
		})
	}
}

func TestNativeAdmissionEnvironmentDNS(t *testing.T) {
	for _, name := range []string{"LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LOCALDOMAIN", "RES_OPTIONS", "LANGUAGE", "LOCPATH", "GCONV_PATH", "NLSPATH"} {
		t.Run(name, func(t *testing.T) {
			f := newAdmissionFixture(t)
			f.pod.Spec.Containers[0].Env = append(f.pod.Spec.Containers[0].Env, corev1.EnvVar{Name: name, Value: "canary"})
			if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err == nil {
				t.Fatal("hostile environment admitted")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*corev1.PodSpec)
	}{{"hostnetwork", func(p *corev1.PodSpec) { p.HostNetwork = true }}, {"dns_config", func(p *corev1.PodSpec) {
		p.DNSConfig = &corev1.PodDNSConfig{Options: []corev1.PodDNSConfigOption{{Name: "ndots"}}}
	}}, {"resolver_shadow", func(p *corev1.PodSpec) {
		p.Containers[0].VolumeMounts = append(p.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "tmp", MountPath: "/etc/resolv.conf"})
	}}, {"service_account_token", func(p *corev1.PodSpec) { p.AutomountServiceAccountToken = admissionBool(true) }}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAdmissionFixture(t)
			tc.change(&f.pod.Spec)
			if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err == nil {
				t.Fatal("mutated runtime admitted")
			}
		})
	}
}

func TestNativeConnectionReferencePrivacy(t *testing.T) {
	f := newAdmissionFixture(t)
	f.m.Spec.Source.URISecretRef = nil
	f.m.Spec.Source.Host = "example.invalid"
	f.m.Spec.Source.Database = "database"
	f.m.Spec.Source.Username = "private-one"
	first, err := connectionReferenceSHA(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	f.m.Spec.Source.Username = "private-two"
	second, err := connectionReferenceSHA(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("inline connection value changed public structural hash")
	}
	f.m.Spec.Source.URISecretRef = &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "source"}, Key: admissionURIKey}
	first, err = connectionReferenceSHA(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	f.m.Spec.Source.URISecretRef.Key = "rotated-reference"
	second, err = connectionReferenceSHA(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("connection reference change did not change structural hash")
	}
}

func TestNativeAdmissionDefaults(t *testing.T) {
	f := newAdmissionFixture(t)
	f.job.Spec.Template.Spec.DNSPolicy = ""
	f.pod.Spec.ServiceAccountName = nativeDefaultServiceAccount
	f.want.TemplateSHA256 = podexec.NativeTemplateSHA256(f.job.Spec.Template)
	if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err != nil {
		t.Fatal("ordinary Pod DNS/service-account defaults refused")
	}
}

func TestNativeEvidenceJobSize(t *testing.T) {
	t.Run("bounds", evidenceJobJSONBounds)
	t.Run("oversize_before_relay", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		api.fixture.job.Annotations = map[string]string{admissionSizePaddingKey: strings.Repeat("a", EvidenceJobJSONLimit)}
		got, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN)
		if err == nil || got != nil || api.writerCalls != 0 {
			t.Fatal("oversized live helper reached relay or constructed authority")
		}
	})
}

func TestNativeEvidenceProgramLimits(t *testing.T) {
	if EvidenceProgramSourceLimit != 122880 || EvidenceMainArgumentLimit != 126976 || EvidencePreludeArgumentLimit != 65536 || EvidenceJobJSONLimit != 1048576 {
		t.Fatal("internal evidence cap policy changed")
	}
	for _, size := range []int{EvidenceProgramSourceLimit - 1, EvidenceProgramSourceLimit, EvidenceProgramSourceLimit + 1} {
		t.Run("source_"+strconv.Itoa(size), func(t *testing.T) {
			source := []byte("#" + strings.Repeat("a", size-2) + "\n")
			want := readerWant()
			want.ProgramSHA256 = sha256.Sum256(source)
			command, args, err := renderEvidenceScripts(source, want, "exec true\n")
			if size > EvidenceProgramSourceLimit {
				if err == nil {
					t.Fatal("oversize program source accepted")
				}
				return
			}
			if err != nil || len(command) != 4 || len(args) != 2 || len(args[1])+1 > EvidenceMainArgumentLimit {
				t.Fatal("source within amended cap refused")
			}
			if !strings.Contains(args[1], string(source)) {
				t.Fatal("program source changed during rendering")
			}
		})
	}
	for _, tc := range []struct {
		name      string
		source    []byte
		wrongHash bool
	}{{fixtureEmptyCase, nil, false}, {"missing_lf", []byte("# comment"), false}, {"nul", []byte("#\x00\n"), false}, {"heredoc_collision", []byte("RPGMIGRATE_NATIVE_EVIDENCE_PY\n"), false}, {"digest", []byte("# comment\n"), true}} {
		t.Run(tc.name, func(t *testing.T) {
			want := readerWant()
			want.ProgramSHA256 = sha256.Sum256(tc.source)
			if tc.wrongHash {
				want.ProgramSHA256[0] ^= 1
			}
			if _, _, err := renderEvidenceScripts(tc.source, want, ""); err == nil {
				t.Fatal("invalid program source accepted")
			}
		})
	}
	t.Run("main_argument_bytes", func(t *testing.T) {
		source := []byte("#\n")
		want := readerWant()
		want.ProgramSHA256 = sha256.Sum256(source)
		_, args, err := renderEvidenceScripts(source, want, "")
		if err != nil {
			t.Fatal(err)
		}
		// Padding a private renderer input isolates the argument cap from the smaller source cap.
		want.OperationID += strings.Repeat("a", EvidenceMainArgumentLimit-len(args[1])-1)
		_, args, err = renderEvidenceScripts(source, want, "")
		if err != nil || len(args[1])+1 != EvidenceMainArgumentLimit {
			t.Fatal("main argument exact byte cap refused")
		}
		want.OperationID += "a"
		if _, _, err := renderEvidenceScripts(source, want, ""); err == nil {
			t.Fatal("main argument cap excludes terminating NUL")
		}
		want.OperationID = readerWant().OperationID + "\x00"
		if _, _, err := renderEvidenceScripts(source, want, ""); err == nil {
			t.Fatal("NUL main argument accepted")
		}
	})
	t.Run("prelude_argument_bytes", func(t *testing.T) {
		source := []byte("#\n")
		want := readerWant()
		want.ProgramSHA256 = sha256.Sum256(source)
		prelude := strings.Repeat(":", EvidencePreludeArgumentLimit-1)
		command, _, err := renderEvidenceScripts(source, want, prelude)
		if err != nil || len(command[2])+1 != EvidencePreludeArgumentLimit {
			t.Fatal("prelude exact byte cap refused")
		}
		if _, _, err := renderEvidenceScripts(source, want, prelude+":"); err == nil {
			t.Fatal("prelude cap excludes terminating NUL")
		}
		if _, _, err := renderEvidenceScripts(source, want, "\x00"); err == nil {
			t.Fatal("NUL prelude accepted")
		}
	})
	t.Run("utf8_byte_count", func(t *testing.T) {
		source := []byte("#" + strings.Repeat("é", EvidenceProgramSourceLimit/2) + "\n")
		want := readerWant()
		want.ProgramSHA256 = sha256.Sum256(source)
		if _, _, err := renderEvidenceScripts(source, want, ""); err == nil {
			t.Fatal("source cap counted runes instead of bytes")
		}
	})
}

func evidenceJobJSON(t *testing.T, job *batchv1.Job) []byte {
	t.Helper()
	copy := job.DeepCopy()
	copy.TypeMeta = metav1.TypeMeta{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: "Job"}
	raw, err := json.Marshal(copy)
	if err != nil {
		t.Fatal("cannot marshal synthetic evidence Job")
	}
	return raw
}
func evidenceJobAtSize(t *testing.T, job *batchv1.Job, size int) *batchv1.Job {
	t.Helper()
	copy := job.DeepCopy()
	if copy.Annotations == nil {
		copy.Annotations = map[string]string{}
	}
	copy.Annotations[admissionSizePaddingKey] = ""
	padding := size - len(evidenceJobJSON(t, copy))
	if padding < 0 {
		t.Fatal("synthetic Job exceeds requested fixture size")
	}
	copy.Annotations[admissionSizePaddingKey] = strings.Repeat("p", padding)
	if len(evidenceJobJSON(t, copy)) != size {
		t.Fatal("Job fixture did not reach exact JSON byte size")
	}
	return copy
}

func evidenceJobJSONBounds(t *testing.T) {
	f := newAdmissionFixture(t)
	t.Run("nil", func(t *testing.T) {
		if admittedEvidenceJobSize(nil) {
			t.Fatal("nil Job accepted")
		}
	})
	t.Run("exact_limit", func(t *testing.T) {
		job := evidenceJobAtSize(t, f.job, EvidenceJobJSONLimit)
		if !admittedEvidenceJobSize(job) {
			t.Fatal("Job at exact byte cap refused")
		}
		if job.APIVersion != "" || job.Kind != "" {
			t.Fatal("size check mutated the observed object")
		}
	})
	t.Run("limit_plus_one", func(t *testing.T) {
		if admittedEvidenceJobSize(evidenceJobAtSize(t, f.job, EvidenceJobJSONLimit+1)) {
			t.Fatal("oversize Job accepted")
		}
	})
	t.Run("escaping", func(t *testing.T) {
		job := f.job.DeepCopy()
		padding := strings.Repeat("<", EvidenceJobJSONLimit/6)
		job.Annotations = map[string]string{admissionSizePaddingKey: padding}
		if len(padding) >= EvidenceJobJSONLimit || len(evidenceJobJSON(t, job)) <= EvidenceJobJSONLimit {
			t.Fatal("escaping fault did not cross JSON byte cap")
		}
		if admittedEvidenceJobSize(job) {
			t.Fatal("escaped JSON inflation ignored")
		}
	})
	for _, tc := range []struct {
		name     string
		decorate func(*batchv1.Job)
	}{{"metadata", func(j *batchv1.Job) { j.Labels = map[string]string{"decorated": "value"} }}, {"owner_reference", func(j *batchv1.Job) {
		j.OwnerReferences = append(j.OwnerReferences, admissionOwner("Migration", "additional-owner", "additional-uid"))
	}}, {readerStatusCase, func(j *batchv1.Job) {
		j.Status.Conditions = append(j.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionFalse, Message: "observed-status"})
	}}, {"managed_fields", func(j *batchv1.Job) {
		j.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "observed-manager", FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:metadata":{"f:labels":{}}}`)}}
	}}} {
		t.Run(tc.name, func(t *testing.T) {
			job := evidenceJobAtSize(t, f.job, EvidenceJobJSONLimit)
			tc.decorate(job)
			if len(evidenceJobJSON(t, job)) <= EvidenceJobJSONLimit {
				t.Fatal("decoration did not cross JSON byte cap")
			}
			if admittedEvidenceJobSize(job) {
				t.Fatal("complete observed Job overhead ignored")
			}
		})
	}
	t.Run("marshal_failure", func(t *testing.T) {
		job := f.job.DeepCopy()
		job.ManagedFields = []metav1.ManagedFieldsEntry{{FieldsV1: metav1.NewFieldsV1("invalid-json")}}
		if admittedEvidenceJobSize(job) {
			t.Fatal("unserializable Job accepted")
		}
	})
}

func TestNativeImmutableAbsenceMountShadow(t *testing.T) {
	for _, mount := range []string{"/", "/usr", "/usr/share", "/usr/share/rpgmigrate", "/usr/share/rpgmigrate/.native-file-absent"} {
		t.Run(mount, func(t *testing.T) {
			f := newAdmissionFixture(t)
			shadow := corev1.VolumeMount{Name: "tmp", MountPath: mount}
			f.job.Spec.Template.Spec.Containers[0].VolumeMounts = append(f.job.Spec.Template.Spec.Containers[0].VolumeMounts, shadow)
			f.pod.Spec.Containers[0].VolumeMounts = append(f.pod.Spec.Containers[0].VolumeMounts, shadow)
			f.want.TemplateSHA256 = podexec.NativeTemplateSHA256(f.job.Spec.Template)
			if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err == nil {
				t.Fatal("shadow mount over immutable absence path admitted")
			}
		})
	}
}
