package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	nativeContainerName         = "pgcopydb"
	nativeShellPath             = "/bin/sh"
	nativeDefaultServiceAccount = "default"
	nativeCloneCommand          = "clone"
)

type JobRole string

const (
	JobRoleWorker    JobRole = "worker"
	JobRoleEvidence  JobRole = "evidence"
	JobRoleEndpos    JobRole = "endpos"
	JobRoleCleanup   JobRole = "cleanup"
	JobRoleRetryGate JobRole = "retry_gate"
)

type ExecutionRequest struct {
	Namespace, MigrationName, PVCName, JobName string
	MigrationUID, PVCUID, JobUID, WorkerJobUID types.UID
	MigrationGeneration                        int64
	Attempt                                    int32
	Role                                       JobRole
	Work                                       WorkIdentity
	ImageRef, PlatformImageID                  string
	TemplateSHA256, ConnectionReferenceSHA256  [32]byte
	OperationID                                string
	ProgramSHA256                              [32]byte
}

type activeAdmission struct {
	migration   *v1beta1.Migration
	pvc         *corev1.PersistentVolumeClaim
	job, worker *batchv1.Job
	pod         *corev1.Pod
	container   podexec.NativeContainerBinding
}

func (r *Reader) admitCompleted(ctx context.Context, w ExecutionRequest, completed bool) (*activeAdmission, error) {
	if !validExecutionRequest(w) || ctx.Err() != nil {
		return nil, errors.New("native evidence execution request invalid")
	}
	m := &v1beta1.Migration{}
	pvc := &corev1.PersistentVolumeClaim{}
	job := &batchv1.Job{}
	worker := &batchv1.Job{}
	for _, read := range []struct {
		name   string
		object client.Object
	}{{w.MigrationName, m}, {w.PVCName, pvc}, {w.JobName, job}} {
		if r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: read.name}, read.object) != nil {
			return nil, errors.New("native evidence owned object unavailable")
		}
	}
	if m.Status.JobName == "" || r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: m.Status.JobName}, worker) != nil {
		return nil, errors.New("native evidence worker unavailable")
	}
	pods := &corev1.PodList{}
	if r.api.List(ctx, pods, client.InNamespace(w.Namespace)) != nil {
		return nil, errors.New("native evidence pod inventory unavailable")
	}
	var selected *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		owner := metav1.GetControllerOf(p)
		if owner != nil && owner.UID == w.JobUID {
			if selected != nil {
				return nil, errors.New("native evidence pod inventory ambiguous")
			}
			selected = p
		}
	}
	if selected == nil {
		return nil, errors.New("native evidence pod unavailable")
	}
	pod := &corev1.Pod{}
	if r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: selected.Name}, pod) != nil || pod.UID != selected.UID {
		return nil, errors.New("native evidence pod changed")
	}
	return validateAdmissionObjects(w, m, pvc, job, worker, pod, completed)
}

func validExecutionRequest(w ExecutionRequest) bool {
	if w.Role != JobRoleEvidence || w.MigrationGeneration <= 0 || w.Attempt < 1 || !validIdentity(w.Work) || w.Work.MigrationUID != string(w.MigrationUID) || w.Work.WorkdirID != string(w.PVCUID) || !lowerHex(w.OperationID, 32) || w.ProgramSHA256 != EvidenceProgramSHA256() || w.ProgramSHA256 == ([32]byte{}) || w.TemplateSHA256 == ([32]byte{}) || w.ConnectionReferenceSHA256 == ([32]byte{}) {
		return false
	}
	for _, name := range []string{w.MigrationName, w.PVCName, w.JobName} {
		if len(validation.IsDNS1123Subdomain(name)) != 0 {
			return false
		}
	}
	if len(validation.IsDNS1123Label(w.Namespace)) != 0 {
		return false
	}
	for _, uid := range []types.UID{w.MigrationUID, w.PVCUID, w.JobUID, w.WorkerJobUID} {
		if !validLabel(string(uid)) {
			return false
		}
	}
	return pinnedImage(w.ImageRef) && platformImage(w.PlatformImageID)
}

func pinnedImage(image string) bool {
	before, digest, ok := strings.Cut(image, "@sha256:")
	return ok && before != "" && validLabel(before) && lowerHex(digest, 64)
}
func platformImage(image string) bool {
	_, digest, ok := strings.Cut(image, "sha256:")
	return ok && validLabel(image) && lowerHex(digest, 64)
}

func validateAdmissionObjects(w ExecutionRequest, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, job, worker *batchv1.Job, pod *corev1.Pod, completed bool) (*activeAdmission, error) {
	refuse := func() (*activeAdmission, error) {
		return nil, errors.New("native evidence execution admission refused")
	}
	if !validExecutionRequest(w) || m == nil || pvc == nil || job == nil || worker == nil || pod == nil {
		return refuse()
	}
	if ValidateEvidenceJobSize(job) != nil {
		return refuse()
	}
	if !admittedMigration(w, m, worker) || !admittedPVC(w, m, pvc) || !admittedHelperJob(w, m, job) || !admittedWorkerJob(w, m, worker) || !admittedPodIdentity(w, job, pod) {
		return refuse()
	}
	template := podexec.NativeTemplateSHA256(job.Spec.Template)
	if template != w.TemplateSHA256 {
		return refuse()
	}
	references, err := ConnectionReferenceSHA256(m, w)
	if err != nil || references != w.ConnectionReferenceSHA256 {
		return refuse()
	}
	if !admittedEvidenceSpecs(m, w, job, pod) || !admittedCompletionState(w, job, pod, completed) {
		return refuse()
	}
	status := pod.Status.ContainerStatuses[0]
	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: status.Name, ContainerID: status.ContainerID, RestartCount: status.RestartCount, ImageID: status.ImageID, TemplateSHA256: template}
	return &activeAdmission{migration: m.DeepCopy(), pvc: pvc.DeepCopy(), job: job.DeepCopy(), worker: worker.DeepCopy(), pod: pod.DeepCopy(), container: binding}, nil
}

func admittedEvidenceSpecs(m *v1beta1.Migration, w ExecutionRequest, job *batchv1.Job, pod *corev1.Pod) bool {
	command, args, env, volumes, mounts, err := EvidenceMaterialization(m, w)
	if err != nil {
		return false
	}
	for _, spec := range []*corev1.PodSpec{&job.Spec.Template.Spec, &pod.Spec} {
		if !admittedPodSpec(spec, w.ImageRef) || !reflect.DeepEqual(spec.Volumes, volumes) {
			return false
		}
		c := spec.Containers[0]
		if !slices.Equal(c.Command, command) || !slices.Equal(c.Args, args) || !sameEnvironment(c.Env, env) || !reflect.DeepEqual(c.VolumeMounts, mounts) {
			return false
		}
	}
	return true
}

func admittedCompletionState(w ExecutionRequest, job *batchv1.Job, pod *corev1.Pod, completed bool) bool {
	if !samePodExecution(&job.Spec.Template.Spec, &pod.Spec) || len(pod.Status.ContainerStatuses) != 1 {
		return false
	}
	status := pod.Status.ContainerStatuses[0]
	// The display can be a runtime alias; exact spec images and raw imageID bind execution.
	if status.Name != nativeContainerName || status.RestartCount != 0 || status.ContainerID == "" || status.ImageID != w.PlatformImageID || status.Image == "" || status.State.Waiting != nil || status.LastTerminationState != (corev1.ContainerState{}) {
		return false
	}
	if completed {
		if pod.Status.Phase != corev1.PodSucceeded || status.State.Terminated == nil || status.State.Terminated.ExitCode != 0 || status.State.Terminated.Signal != 0 || status.State.Running != nil || status.State.Terminated.ContainerID != status.ContainerID || !successfulJob(job) {
			return false
		}
	} else if pod.Status.Phase != corev1.PodRunning || status.State.Running == nil || status.State.Terminated != nil || job.Status.Succeeded != 0 || job.Status.Failed != 0 || terminalJobCondition(job) {
		return false
	}
	return true
}

func admittedMigration(w ExecutionRequest, m *v1beta1.Migration, worker *batchv1.Job) bool {
	return m.Namespace == w.Namespace && m.Name == w.MigrationName && m.UID == w.MigrationUID && m.Generation == w.MigrationGeneration && m.DeletionTimestamp.IsZero() && m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && m.Spec.Follow != nil && m.Spec.Follow.Enabled && m.Status.Attempts == w.Attempt && m.Status.JobName == worker.Name
}

func admittedPVC(w ExecutionRequest, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) bool {
	return pvc.Namespace == w.Namespace && pvc.Name == w.PVCName && pvc.Name == m.Name+"-work" && pvc.UID == w.PVCUID && pvc.DeletionTimestamp.IsZero() && pvc.Status.Phase == corev1.ClaimBound && controlledBy(pvc, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String())
}

func admittedHelperJob(w ExecutionRequest, m *v1beta1.Migration, job *batchv1.Job) bool {
	return job.Namespace == w.Namespace && job.Name == w.JobName && job.UID == w.JobUID && job.DeletionTimestamp.IsZero() && controlledBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) && job.Spec.BackoffLimit != nil && *job.Spec.BackoffLimit == 0 && job.Spec.ActiveDeadlineSeconds != nil && *job.Spec.ActiveDeadlineSeconds == 90 && oneJob(job)
}

func admittedWorkerJob(w ExecutionRequest, m *v1beta1.Migration, worker *batchv1.Job) bool {
	if worker.Namespace != w.Namespace || worker.UID != w.WorkerJobUID || worker.UID == w.JobUID || worker.Name != m.Name+"-run-"+strconv.FormatInt(int64(w.Attempt), 10) || !worker.DeletionTimestamp.IsZero() || !controlledBy(worker, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) || !successfulJob(worker) || len(worker.Spec.Template.Spec.Containers) != 1 {
		return false
	}
	wc := worker.Spec.Template.Spec.Containers[0]
	return wc.Name == nativeContainerName && wc.Image == w.ImageRef && len(wc.Command) == 4 && wc.Command[0] == nativeShellPath && wc.Command[1] == "-c" && wc.Command[3] == "/usr/local/bin/rpgmigrate" && len(wc.Args) > 0 && wc.Args[0] == nativeCloneCommand && slices.Contains(wc.Args, "--follow") && identityEnvironment(wc.Env, w.Work)
}

func admittedPodIdentity(w ExecutionRequest, job *batchv1.Job, pod *corev1.Pod) bool {
	return pod.Namespace == w.Namespace && pod.Name != "" && pod.UID != "" && pod.Spec.DNSPolicy == corev1.DNSClusterFirst && pod.DeletionTimestamp.IsZero() && controlledBy(pod, "Job", job.Name, job.UID, "batch/v1")
}

func controlledBy(object metav1.Object, kind, name string, uid types.UID, version string) bool {
	owner := metav1.GetControllerOf(object)
	return owner != nil && owner.Kind == kind && owner.Name == name && owner.UID == uid && owner.APIVersion == version
}
func oneJob(job *batchv1.Job) bool {
	return (job.Spec.Completions == nil || *job.Spec.Completions == 1) && (job.Spec.Parallelism == nil || *job.Spec.Parallelism == 1) && (job.Spec.Suspend == nil || !*job.Spec.Suspend)
}
func successfulJob(job *batchv1.Job) bool {
	if !oneJob(job) || job.Status.Succeeded != 1 || job.Status.Active != 0 || job.Status.Failed != 0 {
		return false
	}
	complete := false
	for _, c := range job.Status.Conditions {
		if c.Status == corev1.ConditionTrue {
			if c.Type == batchv1.JobFailed || c.Type == batchv1.JobFailureTarget {
				return false
			}
			complete = complete || c.Type == batchv1.JobComplete
		}
	}
	return complete
}

func terminalJobCondition(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed || condition.Type == batchv1.JobFailureTarget) {
			return true
		}
	}
	return false
}

func identityEnvironment(env []corev1.EnvVar, w WorkIdentity) bool {
	expected := identityEnv(w)
	for _, e := range expected {
		count := 0
		for _, actual := range env {
			if actual.Name == e.Name {
				count++
				if !reflect.DeepEqual(e, actual) {
					return false
				}
			}
		}
		if count != 1 {
			return false
		}
	}
	return true
}
func identityEnv(w WorkIdentity) []corev1.EnvVar {
	return []corev1.EnvVar{{Name: "RPGMIGRATE_MIGRATION_UID", Value: w.MigrationUID}, {Name: "RPGMIGRATE_RUN_ID", Value: w.RunID}, {Name: "RPGMIGRATE_WORKDIR_ID", Value: w.WorkdirID}, {Name: "RPGMIGRATE_CHECKPOINT_GENERATION", Value: strconv.FormatInt(w.CheckpointGeneration, 10)}}
}

// EvidenceMaterialization keeps pre-create expectations separate from live execution admission.
func EvidenceMaterialization(m *v1beta1.Migration, w ExecutionRequest) ([]string, []string, []corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, error) {
	if m == nil || m.UID == "" || w.Role != JobRoleEvidence || !validIdentity(w.Work) || w.MigrationUID != m.UID || w.Work.MigrationUID != string(m.UID) || w.Work.WorkdirID != string(w.PVCUID) || len(validation.IsDNS1123Subdomain(w.PVCName)) != 0 || w.PVCName != m.Name+"-work" || !lowerHex(w.OperationID, 32) || w.ProgramSHA256 != EvidenceProgramSHA256() {
		return nil, nil, nil, nil, nil, errors.New("native evidence materialization refused")
	}
	src, err := conn.Materialize(conn.Source, &m.Spec.Source)
	if err != nil {
		return nil, nil, nil, nil, nil, errors.New("native connection materialization invalid")
	}
	tgt, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		return nil, nil, nil, nil, nil, errors.New("native connection materialization invalid")
	}
	env := []corev1.EnvVar{}
	volumes := []corev1.Volume{{Name: resourceKindWorkdir, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: w.PVCName}}}, {Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
	mounts := []corev1.VolumeMount{{Name: resourceKindWorkdir, MountPath: "/work"}, {Name: "tmp", MountPath: "/tmp"}}
	var preludes []string
	var passfiles []conn.Passfile
	for _, material := range []*conn.Materialized{src, tgt} {
		env = append(env, material.Env...)
		volumes = append(volumes, material.Volumes...)
		mounts = append(mounts, material.Mounts...)
		if material.Prelude != "" {
			preludes = append(preludes, material.Prelude)
		}
		if material.Passfile != nil {
			passfiles = append(passfiles, *material.Passfile)
		}
	}
	env = append(env, corev1.EnvVar{Name: "PGCOPYDB_LOG_JSON", Value: "on"})
	if len(preludes) > 0 || len(passfiles) > 0 {
		env = append(env, corev1.EnvVar{Name: "PGPASSFILE", Value: conn.PgpassPath})
	}
	env = append(env, corev1.EnvVar{Name: "PGCONNECT_TIMEOUT", Value: "10"})
	env = append(env, identityEnv(w.Work)...)
	env = append(env, corev1.EnvVar{Name: "LC_ALL", Value: "C"}, corev1.EnvVar{Name: "LANG", Value: "C"})
	command, args, err := renderEvidenceScripts(EvidenceProgram(), w, conn.PreludeScript(preludes, passfiles, ""))
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	return command, args, env, volumes, mounts, nil
}

func renderEvidenceScripts(program []byte, w ExecutionRequest, prelude string) ([]string, []string, error) {
	delimiter := "RPGMIGRATE_NATIVE_EVIDENCE_PY"
	if len(program) == 0 || len(program) > EvidenceProgramSourceLimit || program[len(program)-1] != '\n' || bytes.IndexByte(program, 0) >= 0 || sha256.Sum256(program) != w.ProgramSHA256 || bytes.Contains(append([]byte{'\n'}, program...), []byte("\n"+delimiter+"\n")) {
		return nil, nil, errors.New("native evidence program invalid")
	}
	main := "exec /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B - completed_evidence " + w.OperationID + " " + hex.EncodeToString(w.ProgramSHA256[:]) + " <<'" + delimiter + "'\n" + string(program) + delimiter + "\n"
	if len(main)+1 > EvidenceMainArgumentLimit || len(prelude)+1 > EvidencePreludeArgumentLimit || strings.IndexByte(main, 0) >= 0 || strings.IndexByte(prelude, 0) >= 0 {
		return nil, nil, errors.New("native evidence script invalid")
	}
	return []string{nativeShellPath, "-c", prelude, nativeShellPath}, []string{"-c", main}, nil
}

// ValidateEvidenceJobSize includes metadata and status because API decoration can exceed the builder's size.
func ValidateEvidenceJobSize(job *batchv1.Job) error {
	if job == nil {
		return errors.New("native evidence Job unavailable")
	}
	observed := job.DeepCopy()
	observed.TypeMeta = metav1.TypeMeta{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: "Job"}
	raw, err := json.Marshal(observed)
	if err != nil || len(raw) > EvidenceJobJSONLimit {
		return errors.New("native evidence Job size refused")
	}
	return nil
}

// ConnectionReferenceSHA256 excludes inline values so correlation cannot expose credential bytes.
func ConnectionReferenceSHA256(m *v1beta1.Migration, w ExecutionRequest) ([32]byte, error) {
	_, _, env, volumes, mounts, err := EvidenceMaterialization(m, w)
	if err != nil {
		return [32]byte{}, err
	}
	return StructuralConnectionReferenceSHA256(env, volumes, mounts)
}

func sameEnvironment(a, b []corev1.EnvVar) bool {
	if len(a) != len(b) {
		return false
	}
	expected := map[string]corev1.EnvVar{}
	for _, e := range b {
		if _, exists := expected[e.Name]; exists {
			return false
		}
		expected[e.Name] = e
	}
	for _, e := range a {
		want, exists := expected[e.Name]
		if !exists || !reflect.DeepEqual(e, want) {
			return false
		}
		delete(expected, e.Name)
	}
	return len(expected) == 0
}

func admittedPodSpec(spec *corev1.PodSpec, image string) bool {
	if !admittedPodIsolation(spec) {
		return false
	}
	uid, nonroot, deny, readonly := int64(65532), true, false, true
	podSecurity := &corev1.PodSecurityContext{RunAsNonRoot: &nonroot, RunAsUser: &uid, RunAsGroup: &uid, FSGroup: &uid, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
	containerSecurity := &corev1.SecurityContext{AllowPrivilegeEscalation: &deny, ReadOnlyRootFilesystem: &readonly, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	c := spec.Containers[0]
	if !reflect.DeepEqual(spec.SecurityContext, podSecurity) || !reflect.DeepEqual(c.SecurityContext, containerSecurity) || c.Name != nativeContainerName || c.Image != image || len(c.EnvFrom) != 0 || len(c.VolumeDevices) != 0 || c.Lifecycle != nil || c.LivenessProbe != nil || c.ReadinessProbe != nil || c.StartupProbe != nil || c.Stdin || c.TTY {
		return false
	}
	locale := map[string]bool{}
	seen := map[string]bool{}
	for _, e := range c.Env {
		if seen[e.Name] || strings.HasPrefix(e.Name, "LD_") || slices.Contains([]string{"LOCALDOMAIN", "RES_OPTIONS", "LANGUAGE", "LOCPATH", "GCONV_PATH", "NLSPATH"}, e.Name) {
			return false
		}
		seen[e.Name] = true
		if e.Name == "LANG" || e.Name == "LC_ALL" {
			if e.Value != "C" || e.ValueFrom != nil {
				return false
			}
			locale[e.Name] = true
		}
	}
	return locale["LANG"] && locale["LC_ALL"]
}

func admittedPodIsolation(spec *corev1.PodSpec) bool {
	return len(spec.Containers) == 1 && len(spec.InitContainers) == 0 && len(spec.EphemeralContainers) == 0 && spec.RestartPolicy == corev1.RestartPolicyNever && !spec.HostNetwork && !spec.HostPID && !spec.HostIPC && (spec.ShareProcessNamespace == nil || !*spec.ShareProcessNamespace) && len(spec.HostAliases) == 0 && spec.DNSConfig == nil && (spec.DNSPolicy == "" || spec.DNSPolicy == corev1.DNSClusterFirst) && spec.AutomountServiceAccountToken != nil && !*spec.AutomountServiceAccountToken && (spec.ServiceAccountName == "" || spec.ServiceAccountName == nativeDefaultServiceAccount) && (spec.DeprecatedServiceAccount == "" || spec.DeprecatedServiceAccount == nativeDefaultServiceAccount)
}

func samePodExecution(a, b *corev1.PodSpec) bool {
	return reflect.DeepEqual(a.Containers, b.Containers) && reflect.DeepEqual(a.Volumes, b.Volumes) && reflect.DeepEqual(a.SecurityContext, b.SecurityContext) && reflect.DeepEqual(a.AutomountServiceAccountToken, b.AutomountServiceAccountToken) && (a.ServiceAccountName == b.ServiceAccountName || a.ServiceAccountName == "" && b.ServiceAccountName == nativeDefaultServiceAccount) && a.HostNetwork == b.HostNetwork && a.HostPID == b.HostPID && a.HostIPC == b.HostIPC && reflect.DeepEqual(a.HostAliases, b.HostAliases) && reflect.DeepEqual(a.DNSConfig, b.DNSConfig)
}

func (a *activeAdmission) sameExecution(b *activeAdmission) bool {
	return a != nil && b != nil && a.container == b.container && a.pod.UID == b.pod.UID && a.pod.Name == b.pod.Name && reflect.DeepEqual(a.pod.Spec, b.pod.Spec) && reflect.DeepEqual(a.job.Spec, b.job.Spec) && reflect.DeepEqual(a.worker.Spec, b.worker.Spec) && reflect.DeepEqual(a.pvc.Spec, b.pvc.Spec) && reflect.DeepEqual(a.migration.Spec, b.migration.Spec) && a.migration.Status.JobName == b.migration.Status.JobName && a.migration.Status.Attempts == b.migration.Status.Attempts
}
