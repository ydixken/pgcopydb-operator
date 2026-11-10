package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"slices"
	"strconv"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const nativeRetryTmpPath = "/tmp"

type retryAdmission struct {
	helper    *activeAdmission
	workerPod *corev1.Pod
}

func (r *Reader) admitRetry(ctx context.Context, w ExecutionData, completed bool) (*retryAdmission, error) {
	if !validRetryExecution(w) || ctx.Err() != nil {
		return nil, ErrNativeRetryUnavailable
	}
	m, pvc, job, worker := &v1beta1.Migration{}, &corev1.PersistentVolumeClaim{}, &batchv1.Job{}, &batchv1.Job{}
	for _, read := range []struct {
		name   string
		object client.Object
	}{{w.MigrationName, m}, {w.PVCName, pvc}, {w.JobName, job}} {
		if r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: read.name}, read.object) != nil {
			return nil, ErrNativeRetryUnavailable
		}
	}
	if m.Status.JobName == "" || r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: m.Status.JobName}, worker) != nil {
		return nil, ErrNativeRetryUnavailable
	}
	pods := &corev1.PodList{}
	if r.api.List(ctx, pods, client.InNamespace(w.Namespace)) != nil || pods.Continue != "" {
		return nil, ErrNativeRetryUnavailable
	}
	var helperPod, workerPod *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		owner := metav1.GetControllerOf(p)
		if owner == nil {
			continue
		}
		if owner.Name == w.JobName && owner.UID != w.JobUID || owner.Name == worker.Name && owner.UID != w.WorkerJobUID {
			return nil, ErrNativeRetryUnavailable
		}
		switch owner.UID {
		case w.JobUID:
			if helperPod != nil {
				return nil, ErrNativeRetryUnavailable
			}
			helperPod = p
		case w.WorkerJobUID:
			if workerPod != nil {
				return nil, ErrNativeRetryUnavailable
			}
			workerPod = p
		}
	}
	if helperPod == nil || workerPod == nil {
		return nil, ErrNativeRetryUnavailable
	}
	for _, selected := range []*corev1.Pod{helperPod, workerPod} {
		observed := &corev1.Pod{}
		if r.api.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: selected.Name}, observed) != nil || observed.UID != selected.UID {
			return nil, ErrNativeRetryUnavailable
		}
		*selected = *observed
	}
	return validateRetryObjects(w, m, pvc, job, worker, helperPod, workerPod, completed)
}

func validateRetryObjects(w ExecutionData, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, job, worker *batchv1.Job, pod, workerPod *corev1.Pod, completed bool) (*retryAdmission, error) {
	if !validRetryExecution(w) || m == nil || pvc == nil || job == nil || worker == nil || pod == nil || workerPod == nil {
		return nil, ErrNativeRetryUnavailable
	}
	if !retryMigrationMatches(w, m, worker) || !retryPVCMatches(w, m, pvc) || !retryHelperJobMatches(w, m, job) {
		return nil, ErrNativeRetryUnavailable
	}
	if !retryFailedWorker(w, m, worker, workerPod) || pod.Namespace != w.Namespace || pod.Name == "" || pod.UID == "" || !pod.DeletionTimestamp.IsZero() || !controlledBy(pod, "Job", job.Name, job.UID, "batch/v1") || pod.Spec.DNSPolicy != corev1.DNSClusterFirst {
		return nil, ErrNativeRetryUnavailable
	}
	template := podexec.NativeTemplateSHA256(job.Spec.Template)
	references, err := RetryConnectionReferenceSHA256(m, w)
	if err != nil || template != w.TemplateSHA256 || references != w.ConnectionReferenceSHA256 || !retryHelperSpecs(m, w, job, pod) || !retryHelperState(w, job, pod, completed) {
		return nil, ErrNativeRetryUnavailable
	}
	status := pod.Status.ContainerStatuses[0]
	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: status.Name, ContainerID: status.ContainerID, RestartCount: status.RestartCount, ImageID: status.ImageID, TemplateSHA256: template}
	if w.PodName != "" && w.PodName != pod.Name || w.Container != (podexec.NativeContainerBinding{}) && w.Container != binding {
		return nil, ErrNativeRetryUnavailable
	}
	return &retryAdmission{helper: &activeAdmission{migration: m.DeepCopy(), pvc: pvc.DeepCopy(), job: job.DeepCopy(), worker: worker.DeepCopy(), pod: pod.DeepCopy(), container: binding}, workerPod: workerPod.DeepCopy()}, nil
}

func retryMigrationMatches(w ExecutionData, m *v1beta1.Migration, worker *batchv1.Job) bool {
	return NativeReceiptsMatch(m, w.WorkerJobUID, w.PVCUID) && m.Namespace == w.Namespace && m.Name == w.MigrationName && m.UID == w.MigrationUID && m.Generation == w.MigrationGeneration && m.DeletionTimestamp.IsZero() && m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && !m.Spec.Suspend && m.Status.Attempts == w.Attempt && m.Status.JobName == worker.Name
}

func retryPVCMatches(w ExecutionData, m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) bool {
	return pvc.Namespace == w.Namespace && pvc.Name == w.PVCName && pvc.Name == m.Name+"-work" && pvc.UID == w.PVCUID && pvc.DeletionTimestamp.IsZero() && pvc.Status.Phase == corev1.ClaimBound && controlledBy(pvc, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String())
}

func retryHelperJobMatches(w ExecutionData, m *v1beta1.Migration, job *batchv1.Job) bool {
	return job.Namespace == w.Namespace && job.Name == w.JobName && job.UID == w.JobUID && job.DeletionTimestamp.IsZero() && controlledBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) && retrySingleJob(job) && job.Spec.ActiveDeadlineSeconds != nil && *job.Spec.ActiveDeadlineSeconds == 90 && ValidateEvidenceJobSize(job) == nil
}

func retrySingleJob(job *batchv1.Job) bool {
	uncounted := job.Status.UncountedTerminatedPods
	return oneJob(job) && job.Spec.BackoffLimit != nil && *job.Spec.BackoffLimit == 0 && (job.Spec.CompletionMode == nil || *job.Spec.CompletionMode == batchv1.NonIndexedCompletion) && (job.Spec.ManualSelector == nil || !*job.Spec.ManualSelector) && job.Spec.BackoffLimitPerIndex == nil && job.Spec.MaxFailedIndexes == nil && job.Spec.PodFailurePolicy == nil && job.Spec.SuccessPolicy == nil && (job.Status.Terminating == nil || *job.Status.Terminating == 0) && (uncounted == nil || len(uncounted.Succeeded)+len(uncounted.Failed) == 0)
}

func retryFailedWorker(w ExecutionData, m *v1beta1.Migration, worker *batchv1.Job, pod *corev1.Pod) bool {
	return retryFailedWorkerJob(w, m, worker) && retryFailedWorkerPod(w, worker, pod)
}

func retryFailedWorkerJob(w ExecutionData, m *v1beta1.Migration, worker *batchv1.Job) bool {
	if worker.Namespace != w.Namespace || worker.Name != m.Name+"-run-"+strconv.FormatInt(int64(w.Attempt), 10) || worker.UID != w.WorkerJobUID || worker.UID == w.JobUID || !worker.DeletionTimestamp.IsZero() || !controlledBy(worker, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) || !retrySingleJob(worker) || worker.Status.Failed != 1 || worker.Status.Succeeded != 0 || worker.Status.Active != 0 || worker.Status.Ready != nil && *worker.Status.Ready != 0 {
		return false
	}
	failed := false
	for _, c := range worker.Status.Conditions {
		if c.Status == corev1.ConditionTrue {
			if c.Type == batchv1.JobComplete || c.Type == batchv1.JobSuccessCriteriaMet {
				return false
			}
			failed = failed || c.Type == batchv1.JobFailed
		}
	}
	if !failed || !admittedPodSpec(&worker.Spec.Template.Spec, w.ImageRef) {
		return false
	}
	wc := worker.Spec.Template.Spec.Containers[0]
	return wc.Name == nativeContainerName && len(wc.Command) == 4 && wc.Command[0] == nativeShellPath && wc.Command[1] == "-c" && wc.Command[3] == "/usr/local/bin/rpgmigrate" && len(wc.Args) > 0 && wc.Args[0] == nativeCloneCommand && identityEnvironment(wc.Env, w.Work)
}

func retryFailedWorkerPod(w ExecutionData, worker *batchv1.Job, pod *corev1.Pod) bool {
	if pod.Namespace != w.Namespace || pod.Name == "" || pod.UID == "" || !pod.DeletionTimestamp.IsZero() || !controlledBy(pod, "Job", worker.Name, worker.UID, "batch/v1") || !admittedPodSpec(&pod.Spec, w.ImageRef) || !samePodExecution(&worker.Spec.Template.Spec, &pod.Spec) || pod.Status.Phase != corev1.PodFailed || len(pod.Status.ContainerStatuses) != 1 || len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 {
		return false
	}
	s := pod.Status.ContainerStatuses[0]
	return s.Name == nativeContainerName && s.ContainerID != "" && s.Image != "" && s.ImageID == w.PlatformImageID && s.RestartCount == 0 && s.LastTerminationState == (corev1.ContainerState{}) && s.State.Running == nil && s.State.Waiting == nil && s.State.Terminated != nil && s.State.Terminated.ContainerID == s.ContainerID && (s.State.Terminated.ExitCode != 0 || s.State.Terminated.Signal != 0)
}

func retryHelperSpecs(m *v1beta1.Migration, w ExecutionData, job *batchv1.Job, pod *corev1.Pod) bool {
	command, args, env, volumes, mounts, err := RetryMaterialization(m, w)
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

func retryHelperState(w ExecutionData, job *batchv1.Job, pod *corev1.Pod, completed bool) bool {
	if !samePodExecution(&job.Spec.Template.Spec, &pod.Spec) || len(pod.Status.ContainerStatuses) != 1 || len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 {
		return false
	}
	s := pod.Status.ContainerStatuses[0]
	if s.Name != nativeContainerName || s.RestartCount != 0 || s.ContainerID == "" || s.Image == "" || s.ImageID != w.PlatformImageID || s.LastTerminationState != (corev1.ContainerState{}) || s.State.Waiting != nil {
		return false
	}
	if completed {
		return pod.Status.Phase == corev1.PodSucceeded && s.State.Terminated != nil && s.State.Terminated.ExitCode == 0 && s.State.Terminated.Signal == 0 && s.State.Terminated.ContainerID == s.ContainerID && s.State.Running == nil && successfulJob(job)
	}
	return pod.Status.Phase == corev1.PodRunning && s.State.Running != nil && s.State.Terminated == nil && job.Status.Active <= 1 && job.Status.Succeeded == 0 && job.Status.Failed == 0 && !terminalJobCondition(job)
}

func (a *retryAdmission) same(b *retryAdmission) bool {
	return a != nil && b != nil && a.helper.sameExecution(b.helper) && reflect.DeepEqual(a.helper.migration.Status, b.helper.migration.Status) && reflect.DeepEqual(a.helper.worker.Status, b.helper.worker.Status) && reflect.DeepEqual(a.helper.worker.OwnerReferences, b.helper.worker.OwnerReferences) && a.workerPod.UID == b.workerPod.UID && a.workerPod.Name == b.workerPod.Name && reflect.DeepEqual(a.workerPod.Spec, b.workerPod.Spec) && reflect.DeepEqual(a.workerPod.Status, b.workerPod.Status) && reflect.DeepEqual(a.workerPod.OwnerReferences, b.workerPod.OwnerReferences)
}

// RetryMaterialization projects only the target and cannot admit a live execution by itself.
func RetryMaterialization(m *v1beta1.Migration, w ExecutionData) ([]string, []string, []corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, error) {
	if m == nil || m.UID == "" || m.Spec.Engine != v1beta1.MigrationEngineRPGMigrate || m.Spec.Clone.AllDatabases || w.Role != JobRoleRetryGate || !validIdentity(w.Work) || w.MigrationUID != m.UID || w.Work.MigrationUID != string(m.UID) || w.Work.WorkdirID != string(w.PVCUID) || len(validation.IsDNS1123Subdomain(w.PVCName)) != 0 || w.PVCName != m.Name+"-work" || !lowerHex(w.OperationID, 32) || w.ProgramSHA256 != EvidenceProgramSHA256() {
		return nil, nil, nil, nil, nil, ErrNativeRetryUnavailable
	}
	target, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		return nil, nil, nil, nil, nil, ErrNativeRetryUnavailable
	}
	env := slices.Clone(target.Env)
	volumes := make([]corev1.Volume, 0, 2+len(target.Volumes))
	volumes = append(volumes, corev1.Volume{Name: resourceKindWorkdir, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: w.PVCName, ReadOnly: true}}}, corev1.Volume{Name: nativePreflightTmp, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: resource.NewQuantity(64*1024*1024, resource.BinarySI)}}})
	volumes = append(volumes, target.Volumes...)
	mounts := make([]corev1.VolumeMount, 0, 2+len(target.Mounts))
	mounts = append(mounts, corev1.VolumeMount{Name: resourceKindWorkdir, MountPath: "/work", ReadOnly: true}, corev1.VolumeMount{Name: nativePreflightTmp, MountPath: nativeRetryTmpPath})
	mounts = append(mounts, target.Mounts...)
	var preludes []string
	var passfiles []conn.Passfile
	if target.Prelude != "" {
		preludes = append(preludes, target.Prelude)
	}
	if target.Passfile != nil {
		passfiles = append(passfiles, *target.Passfile)
	}
	if len(preludes) > 0 || len(passfiles) > 0 {
		env = append(env, corev1.EnvVar{Name: "PGPASSFILE", Value: conn.PgpassPath})
	}
	env = append(env, corev1.EnvVar{Name: "PGCONNECT_TIMEOUT", Value: "10"})
	env = append(env, identityEnv(w.Work)...)
	env = append(env, corev1.EnvVar{Name: nativePreflightLocale, Value: "C"}, corev1.EnvVar{Name: nativePreflightLang, Value: "C"})
	command, args, err := renderRetryScripts(EvidenceProgram(), w.OperationID, w.ProgramSHA256, conn.PreludeScript(preludes, passfiles, ""))
	if err != nil {
		return nil, nil, nil, nil, nil, ErrNativeRetryUnavailable
	}
	return command, args, env, volumes, mounts, nil
}

func renderRetryScripts(program []byte, operationID string, programSHA256 [32]byte, prelude string) ([]string, []string, error) {
	const delimiter = "RPGMIGRATE_NATIVE_EVIDENCE_PY"
	if !lowerHex(operationID, 32) || len(program) == 0 || len(program) > EvidenceProgramSourceLimit || program[len(program)-1] != '\n' || bytes.IndexByte(program, 0) >= 0 || sha256.Sum256(program) != programSHA256 || bytes.Contains(append([]byte{'\n'}, program...), []byte("\n"+delimiter+"\n")) {
		return nil, nil, ErrNativeRetryUnavailable
	}
	main := "exec /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B - target_retry " + operationID + " " + hex.EncodeToString(programSHA256[:]) + " <<'" + delimiter + "'\n" + string(program) + delimiter + "\n"
	if len(main)+1 > EvidenceMainArgumentLimit || len(prelude)+1 > EvidencePreludeArgumentLimit || strings.IndexByte(main, 0) >= 0 || strings.IndexByte(prelude, 0) >= 0 {
		return nil, nil, ErrNativeRetryUnavailable
	}
	return []string{nativeShellPath, "-c", prelude, nativeShellPath}, []string{"-c", main}, nil
}

func RetryConnectionReferenceSHA256(m *v1beta1.Migration, w ExecutionData) ([32]byte, error) {
	_, _, env, volumes, mounts, err := RetryMaterialization(m, w)
	if err != nil {
		return [32]byte{}, ErrNativeRetryUnavailable
	}
	return StructuralConnectionReferenceSHA256(env, volumes, mounts)
}
