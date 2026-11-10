package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
				if !reflect.DeepEqual(c.Resources, m.Spec.Runner.Resources) || !reflect.DeepEqual(pod.NodeSelector, m.Spec.Runner.NodeSelector) || !reflect.DeepEqual(pod.Tolerations, m.Spec.Runner.Tolerations) || !reflect.DeepEqual(pod.Affinity, m.Spec.Runner.Affinity) || job.Spec.TTLSecondsAfterFinished != nil {
					t.Fatal("evidence helper lost admitted scheduling or evidence retention")
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

const (
	nativeWorkerSecretForm       = "secretref"
	nativeWorkerURISecretForm    = "urisecretref"
	nativeWorkerTestExecutable   = "/usr/local/bin/rpgmigrate"
	nativeWorkerTestMigrationUID = "01234567-89ab-cdef-0123-456789abcdef"
	nativeWorkerTLSSecret        = "worker-tls"
)

func nativeWorkerJobInputs() (*v1beta1.Migration, *corev1.PersistentVolumeClaim) {
	m, pvc := nativeIdentityObjects()
	m.Generation = 1
	m.Spec.Source, m.Spec.Target = passwordMigration().Spec.Source, passwordMigration().Spec.Target
	m.Labels = map[string]string{labelFeatureE2ERun: featureRunOwner}
	return m, pvc
}

func narrowNativeWorker(m *v1beta1.Migration) {
	no, zero := false, resource.MustParse("0")
	m.Spec.Clone.UseCopyBinary = &no
	m.Spec.Clone.SplitTablesLargerThan = &zero
	m.Spec.Clone.Skip = []v1beta1.SkipOption{testSkipLargeObjects}
}

func TestNativeWorkerJobProgram(t *testing.T) {
	m, pvc := nativeWorkerJobInputs()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	ttl := int32(120)
	m.Spec.TTLSecondsAfterFinished = &ttl
	m.Spec.Runner.NodeSelector = map[string]string{"worker-test-pool": "native"}
	m.Spec.Runner.Tolerations = []corev1.Toleration{{Key: "worker-test", Operator: corev1.TolerationOpExists}}
	m.Spec.Runner.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}
	before, beforePVC := m.DeepCopy(), pvc.DeepCopy()
	job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, before) || !reflect.DeepEqual(pvc, beforePVC) || job.Name != "migration-run-1" || job.Namespace != m.Namespace || job.UID != "" || len(job.OwnerReferences) != 0 || !job.CreationTimestamp.IsZero() {
		t.Fatal("worker construction changed inputs or claimed allocated ownership")
	}
	pod, c := job.Spec.Template.Spec, job.Spec.Template.Spec.Containers[0]
	assertNativeEvidencePodIsolation(t, pod)
	assertNativeEvidencePodSecurity(t, pod)
	assertNativeEvidenceContainerIsolation(t, c, nativeEvidenceTestImage)
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.ActiveDeadlineSeconds != nil || job.Spec.TTLSecondsAfterFinished != nil {
		t.Fatal("worker inherited retries or a bounded helper deadline")
	}
	if !reflect.DeepEqual(c.Resources, pgcopydb.EffectiveRunnerResources(m.Spec.Runner.Resources)) || !reflect.DeepEqual(pod.NodeSelector, m.Spec.Runner.NodeSelector) || !reflect.DeepEqual(pod.Tolerations, m.Spec.Runner.Tolerations) || !reflect.DeepEqual(pod.Affinity, m.Spec.Runner.Affinity) {
		t.Fatal("worker lost resource defaults or scheduling configuration")
	}
	if len(c.Command) != 4 || c.Command[0] != shellPath || c.Command[1] != "-c" || c.Command[3] != nativeWorkerTestExecutable {
		t.Fatal("native worker did not exec the native program through the credential prelude")
	}
	for _, forbidden := range []string{"pgcopydb stream", "publication_retry", "DROP PUBLICATION", "--current", "set endpos"} {
		if strings.Contains(c.Command[2], forbidden) || strings.Contains(strings.Join(c.Args, " "), forbidden) {
			t.Fatal("native worker inherited a legacy or cutover effect")
		}
	}
	assertNativeWorkerIdentity(t, c.Env, pvc.UID)
	for _, labels := range []map[string]string{job.Labels, job.Spec.Template.Labels} {
		if labels[labelMigration] != m.Name || labels[labelManagedBy] != managerName || labels[labelFeatureE2ERun] != featureRunOwner {
			t.Fatal("native worker lost existing ownership labels")
		}
	}
	if slices.ContainsFunc(c.Env, func(e corev1.EnvVar) bool {
		return e.Name == "PGCONNECT_TIMEOUT" || e.Name == "PGCOPYDB_APPLY_GROUP_TXNS"
	}) {
		t.Fatal("worker inherited a control-Job timeout or unqualified apply override")
	}
}

func TestNativeWorkerTTLRetention(t *testing.T) {
	zero, short, finite := int32(0), int32(1), int32(120)
	for _, ttl := range []*int32{nil, &zero, &short, &finite} {
		m, pvc := nativeWorkerJobInputs()
		m.Spec.TTLSecondsAfterFinished = ttl
		for _, attempt := range []int32{1, 2} {
			job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, attempt)
			if err != nil || job.Spec.TTLSecondsAfterFinished != nil {
				t.Fatal("native worker TTL can delete required original execution evidence", err)
			}
		}
		m.Spec.Engine = v1beta1.MigrationEnginePGCopyDB
		legacy, err := buildJob(m, testRunnerImage, 1)
		if err != nil || !reflect.DeepEqual(legacy.Spec.TTLSecondsAfterFinished, ttl) {
			t.Fatal("legacy worker TTL changed", err)
		}
		if !reflect.DeepEqual(m.Spec.TTLSecondsAfterFinished, ttl) {
			t.Fatal("native retention changed the selected user TTL")
		}
	}
}

func assertNativeWorkerIdentity(t *testing.T, env []corev1.EnvVar, pvcUID types.UID) {
	t.Helper()
	want := []corev1.EnvVar{
		{Name: "RPGMIGRATE_MIGRATION_UID", Value: nativeWorkerTestMigrationUID},
		{Name: "RPGMIGRATE_RUN_ID", Value: "0123456789abcdef0123456789abcdef"},
		{Name: "RPGMIGRATE_WORKDIR_ID", Value: string(pvcUID)},
		{Name: "RPGMIGRATE_CHECKPOINT_GENERATION", Value: "1"},
	}
	for _, expected := range want {
		count := 0
		for _, actual := range env {
			if actual.Name == expected.Name {
				count++
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("worker changed identity variable %s", expected.Name)
				}
			}
		}
		if count != 1 {
			t.Fatalf("worker identity variable %s occurs %d times", expected.Name, count)
		}
	}
}

func TestNativeWorkerJobConnections(t *testing.T) {
	forms := map[string]v1beta1.PostgresConnection{
		nativeEvidenceInlineForm:  passwordMigration().Spec.Source,
		nativeWorkerSecretForm:    secretRefTarget(),
		nativeWorkerURISecretForm: uriSecretRefTarget(),
	}
	for sourceName, source := range forms {
		for targetName, target := range forms {
			t.Run(sourceName+"/"+targetName, func(t *testing.T) {
				m, pvc := nativeWorkerJobInputs()
				m.Spec.Source, m.Spec.Target = source, target
				if sourceName == nativeEvidenceInlineForm {
					m.Spec.Source.Host = "worker-private-host-canary.example"
					m.Spec.Source.Username = "worker-private-user-canary"
				}
				for _, c := range []*v1beta1.PostgresConnection{&m.Spec.Source, &m.Spec.Target} {
					if c.URISecretRef == nil {
						c.TLS = &v1beta1.TLSSecretRefs{
							RootCA: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: nativeWorkerTLSSecret}, Key: "worker-ca.pem"},
							Cert:   &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: nativeWorkerTLSSecret}, Key: "cert.pem"},
							Key:    &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: nativeWorkerTLSSecret}, Key: "key.pem"},
						}
					}
				}
				m.Spec.Runner.Image = nativeEvidenceTestImage
				job, err := buildNativeWorkerJob(m, pvc, "unused-worker-image-default", 1)
				if err != nil {
					t.Fatal(err)
				}
				canonical, err := jobSkeleton(m, nativeEvidenceTestImage, jobName(m, 1), nil, "", 0, true)
				if err != nil {
					t.Fatal(err)
				}
				c, base := job.Spec.Template.Spec.Containers[0], canonical.Spec.Template.Spec.Containers[0]
				if c.Command[2] != base.Command[2] || !reflect.DeepEqual(c.Env[:len(base.Env)], base.Env) || !reflect.DeepEqual(job.Spec.Template.Spec.Volumes, canonical.Spec.Template.Spec.Volumes) || !reflect.DeepEqual(c.VolumeMounts, base.VolumeMounts) {
					t.Fatal("native worker changed canonical credentials, TLS or prelude")
				}
				assertNativeWorkerIdentity(t, c.Env, pvc.UID)
				work, err := nativeWorkIdentity(m, pvc)
				if err != nil {
					t.Fatal(err)
				}
				extra := append(nativeIdentityEnv(work), corev1.EnvVar{Name: nativeWorkerLocaleAll, Value: "C"}, corev1.EnvVar{Name: nativeWorkerLocaleLang, Value: "C"})
				if !reflect.DeepEqual(c.Env[len(base.Env):], extra) {
					t.Fatal("native worker changed its identity/locale environment or added connection overrides")
				}
				if c.Image != nativeEvidenceTestImage || job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim == nil || job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != pvc.Name {
					t.Fatal("worker lost image precedence or exact owned claim")
				}
				metadata, err := json.Marshal(job.ObjectMeta)
				if err != nil {
					t.Fatal(err)
				}
				for _, private := range []string{"worker-private-host-canary.example", "worker-private-user-canary"} {
					if strings.Contains(strings.Join(c.Args, " "), private) || strings.Contains(string(metadata), private) {
						t.Fatal("worker disclosed connection data outside the credential channel")
					}
				}
			})
		}
	}
}

func TestNativeWorkerJobProfiles(t *testing.T) {
	const defaults = "clone --dir /work/pgcopydb --table-jobs 4 --split-tables-larger-than 536870912 --split-max-parts 8 --use-copy-binary"
	const narrow = "clone --dir /work/pgcopydb --table-jobs 4 --skip-large-objects"
	for _, tc := range []struct {
		name      string
		attempt   int32
		configure func(*v1beta1.Migration)
		want      string
	}{
		{"default first", 1, nil, defaults + " --restart"},
		{"default retry construction", 2, nil, defaults + " --resume --not-consistent"},
		{"narrow first", 1, narrowNativeWorker, narrow + " --restart"},
		{"narrow retry construction", 2, narrowNativeWorker, narrow + " --resume --not-consistent"},
		{"narrow follow", 1, func(m *v1beta1.Migration) {
			narrowNativeWorker(m)
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
		}, narrow + " --restart --follow --plugin pgoutput"},
		{"narrow follow retry construction", 2, func(m *v1beta1.Migration) {
			narrowNativeWorker(m)
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
		}, narrow + " --resume --not-consistent --follow --plugin pgoutput"},
		{"resource table jobs", 1, func(m *v1beta1.Migration) {
			m.Spec.Runner.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8500m")}
		}, strings.Replace(defaults, "--table-jobs 4", "--table-jobs 9", 1) + " --restart"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, pvc := nativeWorkerJobInputs()
			if tc.configure != nil {
				tc.configure(m)
			}
			job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, tc.attempt)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ") != tc.want {
				t.Fatal("native worker changed its approved argument profile")
			}
			assertNativeWorkerIdentity(t, job.Spec.Template.Spec.Containers[0].Env, pvc.UID)
		})
	}
	for _, phase := range []corev1.PersistentVolumeClaimPhase{corev1.ClaimPending, corev1.ClaimBound} {
		t.Run("claim "+string(phase), func(t *testing.T) {
			m, pvc := nativeWorkerJobInputs()
			pvc.Status.Phase = phase
			job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
			if err != nil {
				t.Fatal(err)
			}
			assertNativeWorkerIdentity(t, job.Spec.Template.Spec.Containers[0].Env, pvc.UID)
		})
	}
	t.Run("replacement claim changes binding", func(t *testing.T) {
		m, pvc := nativeWorkerJobInputs()
		pvc.UID = "replacement-owned-pvc"
		job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
		if err != nil {
			t.Fatal(err)
		}
		assertNativeWorkerIdentity(t, job.Spec.Template.Spec.Containers[0].Env, pvc.UID)
	})
	t.Run("longest worker name", func(t *testing.T) {
		m, pvc := nativeWorkerJobInputs()
		m.Name = strings.Repeat("a", 57)
		pvc.Name, pvc.OwnerReferences[0].Name = workPVCName(m), m.Name
		job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
		if err != nil || len(job.Name) != 63 {
			t.Fatalf("valid name boundary refused: %v", err)
		}
	})
}

func TestNativeWorkerJobRefusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(**v1beta1.Migration, **corev1.PersistentVolumeClaim)
		image   string
		attempt int32
	}{
		{"nil migration", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) { *m = nil }, nativeEvidenceTestImage, 1},
		{"pgcopydb selection", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) { (*m).Spec.Engine = "" }, nativeEvidenceTestImage, 1},
		{"nil claim", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim) { *p = nil }, nativeEvidenceTestImage, 1},
		{"foreign claim", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim) {
			(*p).OwnerReferences[0].UID = "foreign-migration"
		}, nativeEvidenceTestImage, 1},
		{"claim name", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim) { (*p).Name = "other-work" }, nativeEvidenceTestImage, 1},
		{"claim namespace", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim) { (*p).Namespace = "other-namespace" }, nativeEvidenceTestImage, 1},
		{"malformed namespace", func(m **v1beta1.Migration, p **corev1.PersistentVolumeClaim) {
			(*m).Namespace, (*p).Namespace = "bad namespace", "bad namespace"
		}, nativeEvidenceTestImage, 1},
		{"claim uid", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim) { (*p).UID = "" }, nativeEvidenceTestImage, 1},
		{"deleting claim", func(_ **v1beta1.Migration, p **corev1.PersistentVolumeClaim) {
			now := metav1.Now()
			(*p).DeletionTimestamp = &now
		}, nativeEvidenceTestImage, 1},
		{"zero uuid", func(m **v1beta1.Migration, p **corev1.PersistentVolumeClaim) {
			(*m).UID = "00000000-0000-0000-0000-000000000000"
			(*p).OwnerReferences[0].UID = (*m).UID
		}, nativeEvidenceTestImage, 1},
		{"missing image", nil, "", 1},
		{"mutable image", nil, "registry.example/native:latest", 1},
		{"malformed pin", nil, "registry.example/native@sha256:worker-image-canary", 1},
		{"override mutable image", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) {
			(*m).Spec.Runner.Image = "worker-image-canary:latest"
		}, nativeEvidenceTestImage, 1},
		{"invalid attempt", nil, nativeEvidenceTestImage, 0},
		{"negative attempt", nil, nativeEvidenceTestImage, -1},
		{"clone selection", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) {
			(*m).Spec.Clone.Filters = &v1beta1.Filters{ExcludeSchemas: []string{"worker_filter_canary"}}
		}, nativeEvidenceTestImage, 1},
		{"legacy drop", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) { (*m).Spec.Clone.DropIfExists = true }, nativeEvidenceTestImage, 2},
		{"connection error", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) { (*m).Spec.Source.Host = "" }, nativeEvidenceTestImage, 1},
		{"oversized worker name", func(m **v1beta1.Migration, p **corev1.PersistentVolumeClaim) {
			(*m).Name = strings.Repeat("a", 58)
			(*p).Name = workPVCName(*m)
			(*p).OwnerReferences[0].Name = (*m).Name
		}, nativeEvidenceTestImage, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, pvc := nativeWorkerJobInputs()
			if tc.mutate != nil {
				tc.mutate(&m, &pvc)
			}
			job, err := buildNativeWorkerJob(m, pvc, tc.image, tc.attempt)
			if err == nil || job != nil {
				t.Fatal("unsupported worker returned usable output")
			}
			for _, private := range []string{"worker-image-canary", "worker_filter_canary"} {
				if strings.Contains(err.Error(), private) {
					t.Fatal("worker error disclosed refused input")
				}
			}
		})
	}
}

func TestNativeWorkerJobGolden(t *testing.T) {
	for name, target := range map[string]v1beta1.PostgresConnection{
		nativeEvidenceInlineForm:  passwordMigration().Spec.Target,
		nativeWorkerSecretForm:    secretRefTarget(),
		nativeWorkerURISecretForm: uriSecretRefTarget(),
	} {
		t.Run(name, func(t *testing.T) {
			m, pvc := nativeWorkerJobInputs()
			narrowNativeWorker(m)
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
			m.Spec.Target = target
			job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.MarshalIndent(job, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			file := filepath.Join("testdata", "native-worker-job-"+name+".json")
			if os.Getenv("UPDATE_GOLDEN") != "" {
				if err := os.WriteFile(file, got, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("native worker golden drifted; review before updating")
			}
		})
	}
}
