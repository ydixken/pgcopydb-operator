package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

const nativeEvidenceInlineForm = "inline"

const nativeEvidenceTestImage = "registry.example/native@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func nativeEvidenceJobInputs(t *testing.T) (*v1beta1.Migration, native.ExecutionRequest) {
	t.Helper()
	m, pvc := nativeIdentityObjects()
	m.Generation = 1
	m.Labels = map[string]string{labelFeatureE2ERun: featureRunOwner}
	m.Spec.Source, m.Spec.Target = passwordMigration().Spec.Source, passwordMigration().Spec.Target
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	m.Status.Attempts, m.Status.JobName = 1, jobName(m, 1)
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		t.Fatal(err)
	}
	op := "0123456789abcdef0123456789abcdef"
	return m, native.ExecutionRequest{
		Namespace: m.Namespace, MigrationName: m.Name, MigrationUID: m.UID,
		MigrationGeneration: 1, PVCName: pvc.Name, PVCUID: pvc.UID,
		WorkerJobUID: "worker-owned", Attempt: 1, Role: native.JobRoleEvidence,
		Work: work, OperationID: op, JobName: "rpgmigrate-evidence-" + op,
		ProgramSHA256: native.EvidenceProgramSHA256(), ImageRef: nativeEvidenceTestImage,
		PlatformImageID: nativeEvidenceTestImage,
	}
}

func TestNativeJobsEvidenceProgramShape(t *testing.T) {
	m, want := nativeEvidenceJobInputs(t)
	before := m.DeepCopy()
	job, err := buildNativeEvidenceJob(m, nativeEvidenceTestImage, want)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, m) || job.Name != want.JobName || job.Namespace != m.Namespace || job.UID != "" || len(job.OwnerReferences) != 0 {
		t.Fatal("builder changed caller input or claimed allocated identity/admission")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 90 {
		t.Fatal("evidence helper may retry or outlive its deadline")
	}
	if job.Labels[labelMigration] != m.Name || job.Spec.Template.Labels[labelMigration] != m.Name {
		t.Fatal("short-name helper lost existing Migration labels")
	}
	spec := job.Spec.Template.Spec
	assertNativeEvidencePodIsolation(t, spec)
	assertNativeEvidencePodSecurity(t, spec)
	c := spec.Containers[0]
	assertNativeEvidenceContainerIsolation(t, c, want.ImageRef)
	if len(c.Command) != 4 || c.Command[0] != shellPath || c.Command[1] != "-c" || c.Command[3] != shellPath || len(c.Args) != 2 || c.Args[0] != "-c" {
		t.Fatal("evidence helper changed shell envelope")
	}
	program := native.EvidenceProgram()
	if len(program) == 0 || program[len(program)-1] != '\n' || bytes.IndexByte(program, 0) >= 0 || len(program) > native.EvidenceProgramSourceLimit || sha256.Sum256(program) != want.ProgramSHA256 {
		t.Fatal("evidence helper source violates program binding")
	}
	delimiter := "RPGMIGRATE_NATIVE_EVIDENCE_PY"
	prefix := "exec /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B - completed_evidence " + want.OperationID + " " + hex.EncodeToString(want.ProgramSHA256[:]) + " <<'" + delimiter + "'\n"
	if c.Args[1] != prefix+string(program)+delimiter+"\n" || strings.Contains(string(program), "\n"+delimiter+"\n") || strings.IndexByte(c.Args[1], 0) >= 0 || len(c.Args[1])+1 > native.EvidenceMainArgumentLimit || len(c.Command[2])+1 > native.EvidencePreludeArgumentLimit {
		t.Fatal("evidence launcher changed reviewed program or exceeded argument limits")
	}
	if slices.ContainsFunc(c.Env, func(e corev1.EnvVar) bool { return e.Name == "RPGMIGRATE_END_LSN" }) {
		t.Fatal("evidence launcher embedded a cutover boundary")
	}
}

func TestNativeJobsPreserveIdentityAndConnections(t *testing.T) {
	forms := map[string]v1beta1.PostgresConnection{
		nativeEvidenceInlineForm: passwordMigration().Spec.Source,
		"secretref":              secretRefTarget(),
		"urisecretref":           uriSecretRefTarget(),
	}
	for sourceName, source := range forms {
		for targetName, target := range forms {
			t.Run(sourceName+"/"+targetName, func(t *testing.T) {
				m, want := nativeEvidenceJobInputs(t)
				m.Spec.Source, m.Spec.Target = source, target
				if sourceName == nativeEvidenceInlineForm {
					m.Spec.Source.Host, m.Spec.Source.Username = "private-host-canary.example", "private-user-canary"
				}
				m.Spec.Runner.Image = nativeEvidenceTestImage
				if m.Spec.Source.URISecretRef == nil {
					m.Spec.Source.TLS = &v1beta1.TLSSecretRefs{RootCA: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "source-ca"}, Key: "ca.pem"}}
				}
				if m.Spec.Target.URISecretRef == nil {
					m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
					m.Spec.Target.TLS = &v1beta1.TLSSecretRefs{
						RootCA: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "target-ca"}, Key: "ca.pem"},
						Cert:   &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "target-client"}, Key: "cert.pem"},
						Key:    &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "target-client"}, Key: "key.pem"},
					}
				}
				m.Spec.Clone.Filters = &v1beta1.Filters{ExcludeSchemas: []string{"evidence_filter_fixture"}}
				ttl := int32(120)
				m.Spec.TTLSecondsAfterFinished = &ttl
				m.Spec.Runner.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")}}
				m.Spec.Runner.NodeSelector = map[string]string{"evidence-test-pool": "helper"}
				m.Spec.Runner.Tolerations = []corev1.Toleration{{Key: "migration", Operator: corev1.TolerationOpExists}}
				m.Spec.Runner.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}
				job, err := buildNativeEvidenceJob(m, "unused-configured-default", want)
				if err != nil {
					t.Fatal(err)
				}
				command, args, env, volumes, mounts, err := native.EvidenceMaterialization(m, want)
				if err != nil {
					t.Fatal(err)
				}
				c, pod := job.Spec.Template.Spec.Containers[0], job.Spec.Template.Spec
				if !reflect.DeepEqual(c.Command, command) || !reflect.DeepEqual(c.Args, args) || !reflect.DeepEqual(c.Env, env) || !reflect.DeepEqual(pod.Volumes, volumes) || !reflect.DeepEqual(c.VolumeMounts, mounts) {
					t.Fatal("builder diverged from independent admission materialization")
				}
				for _, e := range nativeIdentityEnv(want.Work) {
					if !slices.Contains(c.Env, e) {
						t.Fatalf("missing exact identity env %s", e.Name)
					}
				}
				if slices.ContainsFunc(pod.Volumes, func(v corev1.Volume) bool { return v.Name == "filters" }) || len(pod.Volumes) < 2 || pod.Volumes[0].PersistentVolumeClaim == nil || pod.Volumes[0].PersistentVolumeClaim.ClaimName != want.PVCName {
					t.Fatal("evidence helper added clone setup or lost owned PVC")
				}
				if !reflect.DeepEqual(c.Resources, m.Spec.Runner.Resources) || !reflect.DeepEqual(pod.NodeSelector, m.Spec.Runner.NodeSelector) || !reflect.DeepEqual(pod.Tolerations, m.Spec.Runner.Tolerations) || !reflect.DeepEqual(pod.Affinity, m.Spec.Runner.Affinity) || !reflect.DeepEqual(job.Spec.TTLSecondsAfterFinished, &ttl) {
					t.Fatal("evidence helper lost admitted scheduling/TTL envelope")
				}
				for _, private := range []string{m.Spec.Source.Host, m.Spec.Source.Username} {
					if private != "" && len(private) > 3 && strings.Contains(strings.Join(c.Args, " "), private) {
						t.Fatal("connection data leaked to launcher arguments")
					}
				}
			})
		}
	}
}

func TestNativeJobsRefuseUnsupportedInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(**v1beta1.Migration, *native.ExecutionRequest)
	}{
		{"nil migration", func(m **v1beta1.Migration, _ *native.ExecutionRequest) { *m = nil }},
		{"legacy engine", func(m **v1beta1.Migration, _ *native.ExecutionRequest) { (*m).Spec.Engine = "" }},
		{"foreign namespace", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.Namespace = "other" }},
		{"foreign name", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.MigrationName = "other" }},
		{"foreign work", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.Work.WorkdirID = "other-pvc" }},
		{"empty image", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.ImageRef = "" }},
		{"materialization failure", func(m **v1beta1.Migration, _ *native.ExecutionRequest) {
			(*m).Spec.Source.SynchronousCommit = "private-error-canary"
		}},
		{"wrong role", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.Role = native.JobRoleWorker }},
		{"wrong program", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.ProgramSHA256 = [32]byte{} }},
		{"wrong pvc", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.PVCName = "foreign-work" }},
		{"unsafe operation", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.OperationID = "$(false)" }},
		{"wrong job name", func(_ **v1beta1.Migration, w *native.ExecutionRequest) { w.JobName = "unrelated" }},
		{"image override", func(m **v1beta1.Migration, _ *native.ExecutionRequest) { (*m).Spec.Runner.Image = "unreviewed:latest" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, w := nativeEvidenceJobInputs(t)
			tc.mutate(&m, &w)
			job, err := buildNativeEvidenceJob(m, nativeEvidenceTestImage, w)
			if err == nil || job != nil {
				t.Fatal("unsupported evidence job returned usable output")
			}
			if strings.Contains(err.Error(), "unreviewed") || strings.Contains(err.Error(), "$(false)") || strings.Contains(err.Error(), "private-error-canary") {
				t.Fatal("evidence refusal disclosed rejected input")
			}
		})
	}
}

func TestNativeJobsLongMigrationName(t *testing.T) {
	m, want := nativeEvidenceJobInputs(t)
	m.Name = strings.Repeat("a", 64)
	want.MigrationName, want.PVCName = m.Name, workPVCName(m)
	job, err := buildNativeEvidenceJob(m, nativeEvidenceTestImage, want)
	if err != nil {
		t.Fatal(err)
	}
	if len(validation.IsDNS1123Subdomain(job.Name)) != 0 || len(job.Name) > 63 || job.Name != "rpgmigrate-evidence-0123456789abcdef0123456789abcdef" {
		t.Fatal("helper name inherited a long Migration name")
	}
	for _, labels := range []map[string]string{job.Labels, job.Spec.Template.Labels} {
		if _, exists := labels[labelMigration]; exists || labels[labelManagedBy] != managerName || labels[labelFeatureE2ERun] != featureRunOwner {
			t.Fatal("long-name helper retained an invalid Migration name label or lost existing labels")
		}
		for _, value := range labels {
			if len(validation.IsValidLabelValue(value)) != 0 {
				t.Fatal("helper metadata contains an invalid label value")
			}
		}
	}
	for _, prefix := range []string{m.Name + "-run-", m.Name + "-gate-", m.Name + "-rehearsal-"} {
		if strings.HasPrefix(job.Name, prefix) {
			t.Fatal("helper name matched another controller role")
		}
	}
	if !job.CreationTimestamp.IsZero() {
		t.Fatal("builder claimed a live creation timestamp")
	}
}

func assertNativeEvidencePodIsolation(t *testing.T, spec corev1.PodSpec) {
	t.Helper()
	if spec.RestartPolicy != corev1.RestartPolicyNever || spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken || spec.DNSPolicy != corev1.DNSClusterFirst || len(spec.Containers) != 1 || len(spec.InitContainers) != 0 || len(spec.EphemeralContainers) != 0 || spec.DNSConfig != nil || len(spec.HostAliases) != 0 || spec.HostNetwork || spec.HostPID || spec.HostIPC {
		t.Fatal("evidence helper lacks isolated one-container envelope")
	}
}

func assertNativeEvidencePodSecurity(t *testing.T, spec corev1.PodSpec) {
	t.Helper()
	uid, nonroot := int64(65532), true
	security := &corev1.PodSecurityContext{RunAsNonRoot: &nonroot, RunAsUser: &uid, RunAsGroup: &uid, FSGroup: &uid, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
	if !reflect.DeepEqual(spec.SecurityContext, security) {
		t.Fatal("evidence helper changed native pod security")
	}
}

func assertNativeEvidenceContainerIsolation(t *testing.T, c corev1.Container, image string) {
	t.Helper()
	deny, readonly := false, true
	security := &corev1.SecurityContext{AllowPrivilegeEscalation: &deny, ReadOnlyRootFilesystem: &readonly, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	if !reflect.DeepEqual(c.SecurityContext, security) || c.Name != workerContainer || c.Image != image || len(c.EnvFrom) != 0 || len(c.VolumeDevices) != 0 || c.Lifecycle != nil || c.StartupProbe != nil || c.LivenessProbe != nil || c.ReadinessProbe != nil || c.Stdin || c.TTY {
		t.Fatal("evidence helper changed native container security")
	}
}
