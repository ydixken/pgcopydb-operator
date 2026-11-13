package native

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	admissionCaseRole       = "role"
	admissionCaseProgram    = "program"
	admissionCaseOperation  = "operation"
	admissionDisplayImage   = "runner:display-tag"
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
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: w.MigrationName, Namespace: w.Namespace, UID: w.MigrationUID, Generation: w.MigrationGeneration}, Spec: v1beta1.MigrationSpec{Engine: v1beta1.MigrationEngineRPGMigrate, Source: v1beta1.PostgresConnection{URISecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "source"}, Key: admissionURIKey}}, Target: v1beta1.PostgresConnection{URISecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "target"}, Key: admissionURIKey}}, Follow: &v1beta1.FollowOptions{Enabled: true}}, Status: v1beta1.MigrationStatus{Attempts: 1, JobName: "migration-run-1", NativeJobUID: string(w.WorkerJobUID), NativePVCUID: string(w.PVCUID), Replication: &v1beta1.ReplicationStatus{Endpos: fixtureEndLSN}}}
	owner := admissionOwner("Migration", m.Name, m.UID)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: w.PVCName, Namespace: w.Namespace, UID: w.PVCUID, OwnerReferences: []metav1.OwnerReference{owner}}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	command, args, env, volumes, mounts, err := EvidenceMaterialization(m, w)
	if err != nil {
		t.Fatal(err)
	}
	spec := corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: admissionBool(false), DNSPolicy: corev1.DNSClusterFirst, SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: admissionBool(true), RunAsUser: admissionI64(65532), RunAsGroup: admissionI64(65532), FSGroup: admissionI64(65532), FSGroupChangePolicy: &[]corev1.PodFSGroupChangePolicy{corev1.FSGroupChangeOnRootMismatch}[0], SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}, Volumes: volumes, Containers: []corev1.Container{{Name: nativeContainerName, Image: w.ImageRef, Command: command, Args: args, Env: env, VolumeMounts: mounts, SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: admissionBool(false), ReadOnlyRootFilesystem: admissionBool(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}}}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: w.JobName, Namespace: w.Namespace, UID: w.JobUID, OwnerReferences: []metav1.OwnerReference{owner}}, Spec: batchv1.JobSpec{BackoffLimit: admissionI32(0), ActiveDeadlineSeconds: admissionI64(90), Template: corev1.PodTemplateSpec{Spec: spec}}}
	worker := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: m.Status.JobName, Namespace: w.Namespace, UID: w.WorkerJobUID, OwnerReferences: []metav1.OwnerReference{owner}}, Spec: batchv1.JobSpec{Template: *job.Spec.Template.DeepCopy()}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	worker.Spec.Template.Spec.Containers[0].Command = []string{nativeShellPath, "-c", "exec \"$0\" \"$@\"", "/usr/local/bin/rpgmigrate"}
	worker.Spec.Template.Spec.Containers[0].Args = []string{nativeCloneCommand, "--follow", "--plugin", "pgoutput"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: readerPodName, Namespace: w.Namespace, UID: "pod-test", Finalizers: []string{batchv1.JobTrackingFinalizer}, OwnerReferences: []metav1.OwnerReference{admissionOwner("Job", job.Name, job.UID)}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: nativeContainerName, ContainerID: "containerd://test", Image: w.ImageRef, ImageID: w.PlatformImageID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	w.TemplateSHA256 = podexec.NativeTemplateSHA256(job.Spec.Template)
	w.ConnectionReferenceSHA256, err = ConnectionReferenceSHA256(m, w)
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
		{admissionCaseRole, func(f *admissionFixture) { f.want.Role = JobRoleCleanup }},
		{admissionCaseProgram, func(f *admissionFixture) { f.want.ProgramSHA256 = sha256.Sum256([]byte("other")) }},
		{"image", func(f *admissionFixture) { f.pod.Status.ContainerStatuses[0].ImageID = "other" }},
		{"restart", func(f *admissionFixture) { f.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"mount", func(f *admissionFixture) { f.pod.Spec.Containers[0].VolumeMounts[0].MountPath = "/other" }},
		{"privilege", func(f *admissionFixture) { f.pod.Spec.Containers[0].SecurityContext.Privileged = admissionBool(true) }},
		{"fsgroup_policy_unset", func(f *admissionFixture) {
			f.job.Spec.Template.Spec.SecurityContext.FSGroupChangePolicy = nil
			f.pod.Spec.SecurityContext.FSGroupChangePolicy = nil
		}},
		{"fsgroup_policy_always", func(f *admissionFixture) {
			always := corev1.FSGroupChangeAlways
			f.job.Spec.Template.Spec.SecurityContext.FSGroupChangePolicy = &always
			f.pod.Spec.SecurityContext.FSGroupChangePolicy = &always
		}},
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

// TestNativeAdmissionNoStdin pins that a native container never runs with stdin or a TTY attached,
// so an OpenSSL passphrase prompt reads EOF and the binding refuses it as a diagnostic.
func TestNativeAdmissionNoStdin(t *testing.T) {
	f := newAdmissionFixture(t)
	for _, c := range []corev1.Container{f.job.Spec.Template.Spec.Containers[0], f.pod.Spec.Containers[0]} {
		if c.Stdin || c.StdinOnce || c.TTY {
			t.Fatal("fixture container attaches stdin or a TTY")
		}
	}
	for name, attach := range map[string]func(*corev1.Container){
		"stdin":      func(c *corev1.Container) { c.Stdin = true },
		"stdin_once": func(c *corev1.Container) { c.Stdin, c.StdinOnce = true, true },
		"tty":        func(c *corev1.Container) { c.TTY = true },
	} {
		t.Run(name, func(t *testing.T) {
			f := newAdmissionFixture(t)
			attach(&f.job.Spec.Template.Spec.Containers[0])
			attach(&f.pod.Spec.Containers[0])
			f.want.TemplateSHA256 = podexec.NativeTemplateSHA256(f.job.Spec.Template)
			if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err == nil {
				t.Fatal("native container with stdin or a TTY admitted")
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
	first, err := ConnectionReferenceSHA256(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	f.m.Spec.Source.Username = "private-two"
	second, err := ConnectionReferenceSHA256(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("inline connection value changed public structural hash")
	}
	f.m.Spec.Source.URISecretRef = &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "source"}, Key: admissionURIKey}
	first, err = ConnectionReferenceSHA256(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	f.m.Spec.Source.URISecretRef.Key = "rotated-reference"
	second, err = ConnectionReferenceSHA256(f.m, f.want)
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

func TestNativeAdmissionRuntimeSelection(t *testing.T) {
	for _, route := range []struct {
		name  string
		setup func(*testing.T) (*readerAPI, *Reader, ExecutionData)
		admit func(*Reader, *readerAPI, ExecutionData, bool) error
	}{
		{cloneOutputCompletedPhase, func(t *testing.T) (*readerAPI, *Reader, ExecutionData) {
			api, r := newReaderAPI(t)
			return api, r, ExecutionData{}
		}, func(r *Reader, api *readerAPI, _ ExecutionData, completed bool) error {
			_, err := r.admitCompleted(context.Background(), api.fixture.want, completed)
			return err
		}},
		{"retry", func(t *testing.T) (*readerAPI, *Reader, ExecutionData) {
			return newRetryReaderAPI(t, RetryInputs{})
		}, func(r *Reader, _ *readerAPI, w ExecutionData, completed bool) error {
			_, err := r.admitRetry(context.Background(), w, completed)
			return err
		}},
		{"owned_inventory", newOwnedReaderAPI, func(r *Reader, _ *readerAPI, w ExecutionData, completed bool) error {
			_, err := r.admitOwned(context.Background(), w, completed)
			return err
		}},
		{"cleaned_completion", newCleanedReaderAPI, func(r *Reader, _ *readerAPI, w ExecutionData, completed bool) error {
			_, err := r.admitCleaned(context.Background(), w, fixtureEndLSN, completed)
			return err
		}},
		{"cleanup_rejoin", newCleanupRejoinReaderAPI, func(r *Reader, _ *readerAPI, w ExecutionData, completed bool) error {
			_, err := r.admitCleanupRejoin(context.Background(), w, fixtureEndLSN, completed)
			return err
		}},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, phase := range []string{"running", cloneOutputCompletedPhase} {
				t.Run(phase, func(t *testing.T) {
					targets := []string{"helper_job", "helper_pod", "helper_job_and_pod"}
					if route.name != cloneOutputCompletedPhase {
						targets = append(targets, "original_job", "original_pod", "original_job_and_pod")
					}
					for _, target := range targets {
						t.Run(target, func(t *testing.T) {
							for _, tc := range []struct {
								name   string
								change func(*corev1.PodSpec)
								want   bool
							}{
								{"nil_defaults", func(*corev1.PodSpec) {}, true},
								{"host_users_true", func(s *corev1.PodSpec) { s.HostUsers = admissionBool(true) }, true},
								{"host_users_false", func(s *corev1.PodSpec) { s.HostUsers = admissionBool(false) }, false},
								{"custom_runtime", func(s *corev1.PodSpec) { s.RuntimeClassName = new("custom-runtime") }, false},
								{"empty_runtime", func(s *corev1.PodSpec) { s.RuntimeClassName = new("") }, false},
							} {
								t.Run(tc.name, func(t *testing.T) {
									api, r, w := route.setup(t)
									switch target {
									case "helper_job":
										tc.change(&api.fixture.job.Spec.Template.Spec)
									case "helper_pod":
										tc.change(&api.fixture.pod.Spec)
									case "helper_job_and_pod":
										tc.change(&api.fixture.job.Spec.Template.Spec)
										tc.change(&api.fixture.pod.Spec)
									case "original_job":
										tc.change(&api.fixture.worker.Spec.Template.Spec)
									case "original_pod":
										tc.change(&api.extraPods[0].Spec)
									case "original_job_and_pod":
										tc.change(&api.fixture.worker.Spec.Template.Spec)
										tc.change(&api.extraPods[0].Spec)
									}
									// Bind the changed desired template so isolation is the rejecting boundary.
									template := podexec.NativeTemplateSHA256(api.fixture.job.Spec.Template)
									api.fixture.want.TemplateSHA256, w.TemplateSHA256 = template, template
									if w.Container != (podexec.NativeContainerBinding{}) {
										w.Container.TemplateSHA256 = template
										w.Container.ExecutionSHA256 = podexec.NativeExecutionSHA256(api.fixture.job.Spec.Template)
									}
									if phase == cloneOutputCompletedPhase {
										api.complete()
									}
									if err := route.admit(r, api, w, phase == cloneOutputCompletedPhase); (err == nil) != tc.want {
										t.Fatalf("runtime admission = %v, want admitted %v", err, tc.want)
									}
									if api.writerCalls != 0 || api.logCalls != 0 {
										t.Fatal("admission reached transport")
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func TestNativeAdmissionSamePodRuntimeSelection(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b *bool
		want bool
	}{
		{"nil_nil", nil, nil, true},
		{"nil_true", nil, admissionBool(true), true},
		{"true_nil", admissionBool(true), nil, true},
		{"true_true", admissionBool(true), admissionBool(true), true},
		{"nil_false", nil, admissionBool(false), false},
		{"false_nil", admissionBool(false), nil, false},
		{"true_false", admissionBool(true), admissionBool(false), false},
		{"false_true", admissionBool(false), admissionBool(true), false},
		{"false_false", admissionBool(false), admissionBool(false), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := corev1.PodSpec{HostUsers: tc.a}, corev1.PodSpec{HostUsers: tc.b}
			if samePodExecution(&a, &b) != tc.want {
				t.Fatal("HostUsers default equivalence changed")
			}
		})
	}
	for _, tc := range []struct {
		name string
		a, b *string
		want bool
	}{
		{"nil_nil", nil, nil, true},
		{"nil_custom", nil, new("custom-runtime"), false},
		{"custom_nil", new("custom-runtime"), nil, false},
		{"same_custom", new("custom-runtime"), new("custom-runtime"), true},
		{"different_custom", new("custom-runtime"), new("other-runtime"), false},
		{"nil_empty", nil, new(""), false},
		{"empty_nil", new(""), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := corev1.PodSpec{RuntimeClassName: tc.a}, corev1.PodSpec{RuntimeClassName: tc.b}
			if samePodExecution(&a, &b) != tc.want {
				t.Fatal("RuntimeClassName exact comparison changed")
			}
		})
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
		if ValidateEvidenceJobSize(nil) == nil {
			t.Fatal("nil Job accepted")
		}
	})
	t.Run("exact_limit", func(t *testing.T) {
		job := evidenceJobAtSize(t, f.job, EvidenceJobJSONLimit)
		if ValidateEvidenceJobSize(job) != nil {
			t.Fatal("Job at exact byte cap refused")
		}
		if job.APIVersion != "" || job.Kind != "" {
			t.Fatal("size check mutated the observed object")
		}
	})
	t.Run("limit_plus_one", func(t *testing.T) {
		if ValidateEvidenceJobSize(evidenceJobAtSize(t, f.job, EvidenceJobJSONLimit+1)) == nil {
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
		if ValidateEvidenceJobSize(job) == nil {
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
			if ValidateEvidenceJobSize(job) == nil {
				t.Fatal("complete observed Job overhead ignored")
			}
		})
	}
	t.Run("marshal_failure", func(t *testing.T) {
		job := f.job.DeepCopy()
		job.ManagedFields = []metav1.ManagedFieldsEntry{{FieldsV1: metav1.NewFieldsV1("invalid-json")}}
		if ValidateEvidenceJobSize(job) == nil {
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

func TestNativeDirectImageProfile(t *testing.T) {
	t.Run("checked_display_alias", func(t *testing.T) {
		api, reader := newReaderAPI(t)
		configured := api.fixture.want.ImageRef
		api.fixture.want.PlatformImageID = configured
		api.fixture.pod.Status.ContainerStatuses[0].ImageID = configured
		api.fixture.pod.Status.ContainerStatuses[0].Image = admissionDisplayImage
		observation, err := reader.ReadCompleted(context.Background(), api.fixture.want, fixtureEndLSN)
		if err != nil || observation == nil {
			t.Fatal("display alias refused despite exact configured spec and imageID")
		}
	})
	for _, display := range []string{admissionDisplayImage, "sha256:" + strings.Repeat("5", 64)} {
		t.Run("display_"+display[:6], func(t *testing.T) {
			f := directImageFixture(t)
			f.pod.Status.ContainerStatuses[0].Image = display
			if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err != nil {
				t.Fatal("display-only alias changed immutable admission")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*admissionFixture)
	}{
		{"job_spec", func(f *admissionFixture) {
			f.job.Spec.Template.Spec.Containers[0].Image = admissionDisplayImage
			f.want.TemplateSHA256 = podexec.NativeTemplateSHA256(f.job.Spec.Template)
		}},
		{"pod_spec", func(f *admissionFixture) { f.pod.Spec.Containers[0].Image = admissionDisplayImage }},
		{"runtime_prefix", func(f *admissionFixture) {
			f.pod.Status.ContainerStatuses[0].ImageID = "docker-pullable://" + f.want.ImageRef
		}},
		{"image_alias", func(f *admissionFixture) { f.pod.Status.ContainerStatuses[0].ImageID = admissionDisplayImage }},
		{"different_manifest", func(f *admissionFixture) {
			f.pod.Status.ContainerStatuses[0].ImageID = "runner@sha256:" + strings.Repeat("2", 64)
		}},
		{"config_id", func(f *admissionFixture) {
			f.pod.Status.ContainerStatuses[0].ImageID = "sha256:" + strings.Repeat("3", 64)
		}},
		{"changed_expectation", func(f *admissionFixture) { f.want.PlatformImageID = "runner@sha256:" + strings.Repeat("4", 64) }},
		{"empty_display", func(f *admissionFixture) { f.pod.Status.ContainerStatuses[0].Image = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := directImageFixture(t)
			tc.change(&f)
			if _, err := validateAdmissionObjects(f.want, f.m, f.pvc, f.job, f.worker, f.pod, false); err == nil {
				t.Fatal("changed spec or independently expected raw imageID was admitted")
			}
		})
	}

}

func preCreateEvidenceWant(f admissionFixture) ExecutionRequest {
	w := f.want
	w.JobUID = ""
	w.TemplateSHA256 = [32]byte{}
	w.ConnectionReferenceSHA256 = [32]byte{}
	return w
}

func TestNativeEvidencePreCreateMaterialization(t *testing.T) {
	f := newAdmissionFixture(t)
	want := preCreateEvidenceWant(f)
	t.Run("unset_postcreate_fields", func(t *testing.T) {
		if validExecutionRequest(want) {
			t.Fatal("precreate expectation became full execution admission")
		}
		command, args, env, volumes, mounts, err := EvidenceMaterialization(f.m, want)
		if err != nil {
			t.Fatal(err)
		}
		hash, err := ConnectionReferenceSHA256(f.m, want)
		if err != nil {
			t.Fatal(err)
		}
		completeCommand, completeArgs, completeEnv, completeVolumes, completeMounts, err := EvidenceMaterialization(f.m, f.want)
		if err != nil {
			t.Fatal(err)
		}
		completeHash, err := ConnectionReferenceSHA256(f.m, f.want)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(command, completeCommand) || !reflect.DeepEqual(args, completeArgs) || !reflect.DeepEqual(env, completeEnv) || !reflect.DeepEqual(volumes, completeVolumes) || !reflect.DeepEqual(mounts, completeMounts) || hash != completeHash {
			t.Fatal("unconsumed postcreate fields changed canonical policy")
		}
	})
	t.Run("unconsumed_request_fields", func(t *testing.T) {
		w := want
		w.Namespace = ""
		w.MigrationName = ""
		w.MigrationGeneration = 0
		w.Attempt = 0
		w.WorkerJobUID = ""
		w.JobName = ""
		w.ImageRef = ""
		w.PlatformImageID = ""
		if _, _, _, _, _, err := EvidenceMaterialization(f.m, w); err != nil {
			t.Fatal("renderer performed whole-request admission")
		}
		if _, err := ConnectionReferenceSHA256(f.m, w); err != nil {
			t.Fatal("reference policy performed whole-request admission")
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(**v1beta1.Migration, *ExecutionRequest)
	}{
		{"nil_migration", func(m **v1beta1.Migration, _ *ExecutionRequest) { *m = nil }},
		{"missing_migration_uid", func(m **v1beta1.Migration, _ *ExecutionRequest) { (*m).UID = "" }},
		{"migration_uid", func(m **v1beta1.Migration, _ *ExecutionRequest) { (*m).UID = "11223344-5566-7788-99aa-bbccddeeff00" }},
		{"request_migration_uid", func(_ **v1beta1.Migration, w *ExecutionRequest) {
			w.MigrationUID = "11223344-5566-7788-99aa-bbccddeeff00"
		}},
		{"work", func(_ **v1beta1.Migration, w *ExecutionRequest) { w.Work.RunID = "invalid" }},
		{"pvc_uid", func(_ **v1beta1.Migration, w *ExecutionRequest) { w.PVCUID = "another-pvc" }},
		{"pvc_name", func(_ **v1beta1.Migration, w *ExecutionRequest) { w.PVCName = "../work" }},
		{"foreign_pvc_name", func(_ **v1beta1.Migration, w *ExecutionRequest) { w.PVCName = "foreign-work" }},
		{admissionCaseRole, func(_ **v1beta1.Migration, w *ExecutionRequest) { w.Role = JobRoleWorker }},
		{admissionCaseOperation, func(_ **v1beta1.Migration, w *ExecutionRequest) { w.OperationID = "a';exit 1" }},
		{"operation_case", func(_ **v1beta1.Migration, w *ExecutionRequest) { w.OperationID = strings.Repeat("A", 32) }},
		{admissionCaseProgram, func(_ **v1beta1.Migration, w *ExecutionRequest) { w.ProgramSHA256[0] ^= 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := f.m.DeepCopy()
			w := want
			tc.mutate(&m, &w)
			command, args, env, volumes, mounts, err := EvidenceMaterialization(m, w)
			if err == nil || command != nil || args != nil || env != nil || volumes != nil || mounts != nil {
				t.Fatal("invalid consumed input produced materialization")
			}
			if hash, err := ConnectionReferenceSHA256(m, w); err == nil || hash != ([32]byte{}) {
				t.Fatal("invalid consumed input produced a structural digest")
			}
		})
	}
}

func policySecret(name, key string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: key}
}

func TestNativeEvidenceConstructionPolicy(t *testing.T) {
	forms := []struct {
		name string
		make func(string) v1beta1.PostgresConnection
	}{
		{"inline", func(role string) v1beta1.PostgresConnection {
			return v1beta1.PostgresConnection{Host: "example.invalid", Username: role, Database: "fixture=db", PasswordSecretRef: policySecret(role+"-password", "pw")}
		}},
		{"secret", func(role string) v1beta1.PostgresConnection {
			return v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: role + "-connection"}}
		}},
		{"opaque", func(role string) v1beta1.PostgresConnection {
			return v1beta1.PostgresConnection{URISecretRef: policySecret(role+"-uri", admissionURIKey)}
		}},
	}
	for _, source := range forms {
		for _, target := range forms {
			t.Run(source.name+"_"+target.name, func(t *testing.T) {
				f := newAdmissionFixture(t)
				w := preCreateEvidenceWant(f)
				m := f.m.DeepCopy()
				m.Spec.Source = source.make("source-role")
				m.Spec.Target = target.make("target-role")
				for _, connection := range []*v1beta1.PostgresConnection{&m.Spec.Source, &m.Spec.Target} {
					connection.TLS = &v1beta1.TLSSecretRefs{RootCA: policySecret("tls-secret", "ca"), Cert: policySecret("tls-secret", "cert"), Key: policySecret("tls-secret", "key")}
				}
				checkConstructionPolicy(t, m, w)
			})
		}
	}
	t.Run("target_settings", func(t *testing.T) {
		f := newAdmissionFixture(t)
		m := f.m.DeepCopy()
		m.Spec.Target = forms[0].make("target-role")
		m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
		checkConstructionPolicy(t, m, preCreateEvidenceWant(f))
	})
	t.Run("selector_structure", func(t *testing.T) {
		f := newAdmissionFixture(t)
		w := preCreateEvidenceWant(f)
		first, err := ConnectionReferenceSHA256(f.m, w)
		if err != nil {
			t.Fatal(err)
		}
		f.m.Spec.Source.URISecretRef.Key = "another-key"
		second, err := ConnectionReferenceSHA256(f.m, w)
		if err != nil || first == second {
			t.Fatal("changed Secret selector did not change structural reference")
		}
		f.m.Spec.Source = forms[0].make("source-role")
		f.m.Spec.Source.TLS = &v1beta1.TLSSecretRefs{RootCA: policySecret("ca-one", "ca")}
		first, err = ConnectionReferenceSHA256(f.m, w)
		if err != nil {
			t.Fatal(err)
		}
		f.m.Spec.Source.TLS.RootCA.Name = "ca-two"
		second, err = ConnectionReferenceSHA256(f.m, w)
		if err != nil || first == second {
			t.Fatal("changed projected reference did not change structural digest")
		}
	})
}

func checkConstructionPolicy(t *testing.T, m *v1beta1.Migration, w ExecutionRequest) {
	t.Helper()
	command, args, env, volumes, mounts, err := EvidenceMaterialization(m, w)
	if err != nil {
		t.Fatal(err)
	}
	source, err := conn.Materialize(conn.Source, &m.Spec.Source)
	if err != nil {
		t.Fatal(err)
	}
	target, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		t.Fatal(err)
	}
	expectedEnv := append(append([]corev1.EnvVar{}, source.Env...), target.Env...)
	if len(env) < len(expectedEnv) || !reflect.DeepEqual(env[:len(expectedEnv)], expectedEnv) {
		t.Fatal("role-specific connection environment changed")
	}
	expectedVolumes := append(append([]corev1.Volume{}, source.Volumes...), target.Volumes...)
	expectedMounts := append(append([]corev1.VolumeMount{}, source.Mounts...), target.Mounts...)
	if len(volumes) != 2+len(expectedVolumes) || len(mounts) != 2+len(expectedMounts) {
		t.Fatal("connection volume/mount inventory changed")
	}
	for i, v := range expectedVolumes {
		if !reflect.DeepEqual(volumes[i+2], v) {
			t.Fatal("connection projection changed")
		}
	}
	for i, v := range expectedMounts {
		if !reflect.DeepEqual(mounts[i+2], v) {
			t.Fatal("connection mount changed")
		}
	}
	var preludes []string
	var passfiles []conn.Passfile
	for _, material := range []*conn.Materialized{source, target} {
		if material.Prelude != "" {
			preludes = append(preludes, material.Prelude)
		}
		if material.Passfile != nil {
			passfiles = append(passfiles, *material.Passfile)
		}
	}
	if len(command) != 4 || command[0] != nativeShellPath || command[1] != "-c" || command[2] != conn.PreludeScript(preludes, passfiles, "") || command[3] != nativeShellPath || len(args) != 2 || args[0] != "-c" || !strings.Contains(args[1], string(EvidenceProgram())) {
		t.Fatal("canonical prelude/main delivery changed")
	}
	launch := strings.Join(append(command, args...), "\n")
	for _, e := range expectedEnv {
		if e.Value != "" && strings.Contains(launch, e.Value) {
			t.Fatal("connection URI moved from environment to launcher")
		}
		if e.ValueFrom != nil && e.Value != "" {
			t.Fatal("opaque credential reference gained controller-resolved bytes")
		}
	}
}

func directImageFixture(t *testing.T) admissionFixture {
	t.Helper()
	f := newAdmissionFixture(t)
	configured := f.want.ImageRef
	f.want.PlatformImageID = configured
	f.pod.Status.ContainerStatuses[0].ImageID = configured
	return f
}
