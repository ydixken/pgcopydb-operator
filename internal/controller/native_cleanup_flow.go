package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	nativeInventoryJobPrefix     = "rpgmigrate-inventory-"
	nativeCleanupRejoinJobPrefix = "rpgmigrate-cleanup-rejoin-"
	nativeCleanedJobPrefix       = "rpgmigrate-cleaned-"
)

var errNativeCleanupFlow = errors.New("native cleanup or completion closure unavailable")

type nativeStateKind uint8

const (
	nativeInventoryState nativeStateKind = iota
	nativeCleanedState
	nativeCleanupRejoinState
)

type nativeStateHelper struct {
	expected, created *batchv1.Job
	pod               *corev1.Pod
	data              native.ExecutionData
}

func buildNativeOwnedInventoryJob(m *v1beta1.Migration, runnerImage string, w native.ExecutionData) (*batchv1.Job, error) {
	if m == nil || m.Namespace != w.Namespace || m.Name != w.MigrationName || m.UID != w.MigrationUID || m.Generation != w.MigrationGeneration || m.Status.Attempts != w.Attempt || m.Status.JobName != jobName(m, w.Attempt) || !native.NativeReceiptsMatch(m, w.WorkerJobUID, w.PVCUID) || w.JobName != nativeInventoryJobPrefix+w.OperationID || runnerImageFor(m, runnerImage) != w.ImageRef || w.PlatformImageID != w.ImageRef {
		return nil, errNativeCleanupFlow
	}
	command, args, env, volumes, mounts, err := native.OwnedInventoryMaterialization(m, w)
	if err != nil {
		return nil, errNativeCleanupFlow
	}
	job, err := buildNativeVersionJob(m, runnerImage, "rpgmigrate-version-"+w.OperationID)
	if err != nil {
		return nil, errNativeCleanupFlow
	}
	job.Name = w.JobName
	job.Spec.TTLSecondsAfterFinished = nil
	job.Spec.Template.Spec.Volumes = volumes
	c := &job.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
	if native.ValidateEvidenceJobSize(job) != nil {
		return nil, errNativeCleanupFlow
	}
	return job, nil
}

func nativeCleanupOperation() (string, error) {
	var operation [16]byte
	if _, err := rand.Read(operation[:]); err != nil {
		return "", errNativeCleanupFlow
	}
	return hex.EncodeToString(operation[:]), nil
}

func (r *MigrationReconciler) nativeCleanupPreflight(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) error {
	version, passed, failure, err := r.ensureNativeCompletionPreflight(ctx, m, worker, pvc)
	if err != nil || !passed || failure != "" || version == nil || validateNativeConfig(m, version.Capabilities) != nil || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return errNativeCleanupFlow
	}
	return nil
}

func (r *MigrationReconciler) createNativeStateHelper(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, kind nativeStateKind) (*nativeStateHelper, error) {
	if r.NativeEvidence == nil || r.Client == nil || r.APIReader == nil || r.Scheme == nil || m == nil || worker == nil || pvc == nil || !followEnabled(m) || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || ctx.Err() != nil {
		return nil, errNativeCleanupFlow
	}
	if err := r.nativeCleanupPreflight(ctx, m, worker, pvc, original); err != nil {
		return nil, err
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		return nil, errNativeCleanupFlow
	}
	op, err := nativeCleanupOperation()
	if err != nil {
		return nil, err
	}
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	w := native.ExecutionData{Namespace: m.Namespace, MigrationName: m.Name, MigrationUID: m.UID, MigrationGeneration: m.Generation, PVCName: pvc.Name, PVCUID: pvc.UID, WorkerJobUID: worker.UID, Attempt: m.Status.Attempts, Role: native.JobRoleCleanup, Work: work, JobName: nativeInventoryJobPrefix + op, OperationID: op, ImageRef: image, PlatformImageID: image, ProgramSHA256: native.OwnedInventoryProgramSHA256()}
	build := buildNativeOwnedInventoryJob
	if kind == nativeCleanedState {
		w.Role, w.JobName, w.ProgramSHA256 = native.JobRoleEvidence, nativeCleanedJobPrefix+op, native.CleanedCompletionProgramSHA256()
		build = buildNativeCleanedCompletionJob
	}
	if kind == nativeCleanupRejoinState {
		w.Role, w.JobName, w.ProgramSHA256 = native.JobRoleEvidence, nativeCleanupRejoinJobPrefix+op, native.CleanupRejoinCompletionProgramSHA256()
		build = buildNativeCleanupRejoinCompletionJob
	}
	expected, err := build(m, r.RPGMigrateRunnerImage, w)
	if err != nil || controllerutil.SetControllerReference(m, expected, r.Scheme) != nil || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) || native.ValidateEvidenceJobSize(expected) != nil {
		return nil, errNativeCleanupFlow
	}
	created := expected.DeepCopy()
	if r.Create(ctx, created) != nil || created.UID == "" || created.UID == worker.UID || !nativePreflightJobMatches(expected, created) {
		return nil, errNativeCleanupFlow
	}
	w.JobUID = created.UID
	capture := &nativeStateHelper{expected: expected, created: created.DeepCopy(), data: w}
	return capture, nil
}

func (r *MigrationReconciler) nativeStateHelperReady(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, capture *nativeStateHelper) error {
	binding := nativeEvidenceHelperBinding{}
	err := wait.PollUntilContextCancel(ctx, nativePreflightPoll, true, func(ctx context.Context) (bool, error) {
		if !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
			return false, errNativeCleanupFlow
		}
		job, err := r.nativeEvidenceLiveJob(ctx, capture.created, m)
		if err != nil || job.Generation != capture.created.Generation || !nativePreflightJobMatches(capture.expected, job) {
			return false, errNativeCleanupFlow
		}
		ready, err := r.nativeEvidenceHelperReady(ctx, job, &binding)
		if err != nil || !ready {
			return false, err
		}
		pod, err := r.nativePreflightPod(ctx, job)
		if err != nil || pod == nil || !nativePreflightPodMatches(job, pod) || pod.UID != binding.podUID || !nativeControlPodRunning(pod, capture.data.PlatformImageID) {
			return false, errNativeCleanupFlow
		}
		capture.pod = pod.DeepCopy()
		capture.data.TemplateSHA256 = podexec.NativeTemplateSHA256(job.Spec.Template)
		capture.data.PodName = pod.Name
		capture.data.Container = podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: workerContainer, ContainerID: binding.containerID, ImageID: capture.data.PlatformImageID, TemplateSHA256: capture.data.TemplateSHA256, ExecutionSHA256: podexec.NativeExecutionSHA256(job.Spec.Template)}
		return true, nil
	})
	if err != nil {
		return errNativeCleanupFlow
	}
	references := native.OwnedInventoryConnectionReferenceSHA256
	if capture.data.ProgramSHA256 == native.CleanedCompletionProgramSHA256() {
		references = native.CleanedCompletionConnectionReferenceSHA256
	}
	if capture.data.ProgramSHA256 == native.CleanupRejoinCompletionProgramSHA256() {
		references = native.CleanupRejoinCompletionConnectionReferenceSHA256
	}
	var errReference error
	capture.data.ConnectionReferenceSHA256, errReference = references(m, capture.data)
	if errReference != nil {
		return errNativeCleanupFlow
	}
	return nil
}

func (r *MigrationReconciler) nativeStateHelperClosed(ctx context.Context, m *v1beta1.Migration, capture *nativeStateHelper, successful bool) (*corev1.Pod, error) {
	if capture == nil || capture.pod == nil {
		return nil, errNativeCleanupFlow
	}
	job, err := r.nativeEvidenceLiveJob(ctx, capture.created, m)
	if err != nil || !nativePreflightJobMatches(capture.expected, job) || job.Generation != capture.created.Generation || job.Status.Active != 0 || successful && !nativeEvidenceWorkerComplete(job) || !successful && !nativeEvidenceWorkerComplete(job) && !nativeRecoveryWorkerFailed(job) {
		return nil, errNativeCleanupFlow
	}
	pod, err := r.nativePreflightPod(ctx, job)
	if err != nil || pod == nil || !nativePreflightPodMatches(job, pod) || !nativePreflightSamePod(capture.pod, pod) || pod.Generation != capture.pod.Generation || !apiequality.Semantic.DeepEqual(capture.pod.Spec, pod.Spec) || !nativeRecoveryPodTerminal(pod, capture.data.PlatformImageID) || pod.Status.ContainerStatuses[0].ContainerID != capture.data.Container.ContainerID || successful && (pod.Status.Phase != corev1.PodSucceeded || pod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 || pod.Status.ContainerStatuses[0].State.Terminated.Signal != 0) {
		return nil, errNativeCleanupFlow
	}
	return pod, nil
}

func (r *MigrationReconciler) readNativeOwnedInventory(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (native.AdmittedOwnedState, error) {
	original, err := r.nativeQuiescentWorker(ctx, m, worker, pvc)
	if err != nil {
		return native.AdmittedOwnedState{}, errNativeCleanupFlow
	}
	capture, err := r.createNativeStateHelper(ctx, m, worker, pvc, original, nativeInventoryState)
	if err != nil {
		return native.AdmittedOwnedState{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := r.nativeStateHelperReady(ctx, m, worker, pvc, original, capture); err != nil {
		return native.AdmittedOwnedState{}, err
	}
	owned, err := r.NativeEvidence.ReadOwnedState(ctx, capture.data)
	if err != nil || owned.Work() != capture.data.Work || len(owned.Resources()) == 0 || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return native.AdmittedOwnedState{}, errNativeCleanupFlow
	}
	pod, err := r.nativeStateHelperClosed(ctx, m, capture, true)
	if err != nil || r.nativePreflightCleanup(ctx, m, capture.created, pod) != nil {
		return native.AdmittedOwnedState{}, errNativeCleanupFlow
	}
	return owned, nil
}

func (r *MigrationReconciler) readNativeCleanedCompletedEvidence(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*native.CompletedObservation, error) {
	observation, _, err := r.captureNativeCleanedCompletedEvidence(ctx, m, worker, pvc)
	return observation, err
}

// A failed probe permits another route only after positive closure of its exact helper.
func (r *MigrationReconciler) captureNativeCleanedCompletedEvidence(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*native.CompletedObservation, bool, error) {
	return r.captureNativeStateCompletion(ctx, m, worker, pvc, nativeCleanedState)
}

func (r *MigrationReconciler) captureNativeStateCompletion(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, kind nativeStateKind) (*native.CompletedObservation, bool, error) {
	if m == nil || worker == nil || pvc == nil || !nativeControlValidEnd(nativeControlEnd(m)) || !nativeEvidenceWorkerComplete(worker) {
		return nil, false, errNativeCleanupFlow
	}
	original, err := r.nativeQuiescentWorker(ctx, m, worker, pvc)
	if err != nil {
		return nil, false, errNativeCleanupFlow
	}
	capture, err := r.createNativeStateHelper(ctx, m, worker, pvc, original, kind)
	if err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := r.nativeStateHelperReady(ctx, m, worker, pvc, original, capture); err != nil {
		return nil, false, err
	}
	read := r.NativeEvidence.ReadCleanedCompleted
	if kind == nativeCleanupRejoinState {
		read = r.NativeEvidence.ReadCleanupRejoinCompleted
	}
	observation, readErr := read(ctx, capture.data, nativeControlEnd(m))
	if !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return nil, false, errNativeCleanupFlow
	}
	pod, err := r.nativeStateHelperClosed(ctx, m, capture, readErr == nil)
	if err != nil || r.nativePreflightCleanup(ctx, m, capture.created, pod) != nil {
		return nil, false, errNativeCleanupFlow
	}
	if readErr != nil || observation == nil {
		return nil, true, errNativeCleanupFlow
	}
	return observation, true, nil
}

func (r *MigrationReconciler) ensureNativeCleanup(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	if r.NativeJobs == nil || m == nil || worker == nil || pvc == nil || !followEnabled(m) || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || !nativeControlValidEnd(nativeControlEnd(m)) || !nativeEvidenceWorkerComplete(worker) {
		return false, errNativeCleanupFlow
	}
	original, err := r.nativeQuiescentWorker(ctx, m, worker, pvc)
	if err != nil {
		return false, errNativeCleanupFlow
	}
	owned, err := r.readNativeOwnedInventory(ctx, m, worker, pvc)
	if err != nil || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return false, errNativeCleanupFlow
	}
	capture, err := r.captureNativeCleanupReport(ctx, m, worker, pvc, original, owned)
	if err != nil {
		return false, err
	}
	after, err := r.readNativeOwnedInventory(ctx, m, worker, pvc)
	if err != nil || after.Work() != owned.Work() || !slices.Equal(after.Resources(), owned.Resources()) || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) || !r.nativeCleanupCaptureUnchanged(ctx, m, worker, pvc, original, capture) {
		return false, errNativeCleanupFlow
	}
	if r.nativePreflightCleanup(ctx, m, capture.created, capture.pod) != nil {
		return false, errNativeCleanupFlow
	}
	return true, nil
}

type nativeCleanupCapture struct {
	expected, created, observed *batchv1.Job
	pod                         *corev1.Pod
	binding                     podexec.NativeContainerBinding
}

func (r *MigrationReconciler) captureNativeCleanupReport(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, owned native.AdmittedOwnedState) (*nativeCleanupCapture, error) {
	if err := r.nativeCleanupPreflight(ctx, m, worker, pvc, original); err != nil {
		return nil, err
	}
	op, err := nativeCleanupOperation()
	if err != nil {
		return nil, err
	}
	expected, err := buildNativeCleanupJob(m, pvc, r.RPGMigrateRunnerImage, "rpgmigrate-cleanup-"+op, owned)
	if err != nil || controllerutil.SetControllerReference(m, expected, r.Scheme) != nil || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) || native.ValidateEvidenceJobSize(expected) != nil {
		return nil, errNativeCleanupFlow
	}
	created := expected.DeepCopy()
	if r.Create(ctx, created) != nil || created.UID == "" || created.UID == worker.UID || !nativePreflightJobMatches(expected, created) {
		return nil, errNativeCleanupFlow
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	binding := podexec.NativeContainerBinding{ContainerName: workerContainer, ImageID: runnerImageFor(m, r.RPGMigrateRunnerImage), TemplateSHA256: podexec.NativeTemplateSHA256(created.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(created.Spec.Template)}
	observed, pod, err := r.nativeEndposHelperTerminal(ctx, m, worker, pvc, original, expected, created, &binding)
	if err != nil {
		return nil, errNativeCleanupFlow
	}
	output, err := r.NativeJobs.JobCleanupNative(ctx, m.Namespace, created.UID, binding)
	if err != nil || !output.Successful() {
		return nil, errNativeCleanupFlow
	}
	raw := output.Payload()
	if len(raw) > 65536 || bytes.Count(raw, []byte{'\n'}) != 1 || !bytes.HasSuffix(raw, []byte{'\n'}) || !bytes.HasPrefix(raw, []byte("native-report: ")) {
		return nil, errNativeCleanupFlow
	}
	report, err := native.ParseReportLine(raw, native.Binding{Command: "stream cleanup", Work: owned.Work(), Resources: owned.Resources()})
	const nativeCleanupCompletedPhase = "completed"
	if err != nil || report.Phase != nativeCleanupCompletedPhase || report.Cleanup == nil || report.Cleanup.Complete == nil || !*report.Cleanup.Complete || !r.nativeCleanupCaptureUnchanged(ctx, m, worker, pvc, original, &nativeCleanupCapture{expected: expected, created: created, observed: observed, pod: pod, binding: binding}) || !r.nativeVerificationOriginalUnchanged(ctx, m, worker, pvc, original) {
		return nil, errNativeCleanupFlow
	}
	return &nativeCleanupCapture{expected: expected, created: created, observed: observed, pod: pod, binding: binding}, nil
}

func buildNativeCleanupRejoinCompletionJob(m *v1beta1.Migration, runnerImage string, w native.ExecutionData) (*batchv1.Job, error) {
	if m == nil || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || m.Namespace != w.Namespace || m.Name != w.MigrationName || m.UID != w.MigrationUID || m.Generation != w.MigrationGeneration || m.Status.Attempts != w.Attempt || m.Status.JobName != jobName(m, w.Attempt) || !native.NativeReceiptsMatch(m, w.WorkerJobUID, w.PVCUID) || !nativeControlValidEnd(nativeControlEnd(m)) || w.JobName != nativeCleanupRejoinJobPrefix+w.OperationID || runnerImageFor(m, runnerImage) != w.ImageRef || w.PlatformImageID != w.ImageRef || w.Role != native.JobRoleEvidence || w.ProgramSHA256 != native.CleanupRejoinCompletionProgramSHA256() {
		return nil, errNativeCleanupFlow
	}
	command, args, env, volumes, mounts, err := native.CleanupRejoinCompletionMaterialization(m, w)
	if err != nil {
		return nil, errNativeCleanupFlow
	}
	job, err := buildNativeVersionJob(m, runnerImage, "rpgmigrate-version-"+w.OperationID)
	if err != nil {
		return nil, errNativeCleanupFlow
	}
	job.Name = w.JobName
	job.Spec.TTLSecondsAfterFinished = nil
	job.Spec.Template.Spec.Volumes = volumes
	c := &job.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
	if native.ValidateEvidenceJobSize(job) != nil {
		return nil, errNativeCleanupFlow
	}
	return job, nil
}

func (r *MigrationReconciler) readNativeCleanupRejoinCompletion(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*native.CompletedObservation, error) {
	observation, _, err := r.captureNativeCleanupRejoinCompletion(ctx, m, worker, pvc)
	return observation, err
}

func (r *MigrationReconciler) captureNativeCleanupRejoinCompletion(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (*native.CompletedObservation, bool, error) {
	return r.captureNativeStateCompletion(ctx, m, worker, pvc, nativeCleanupRejoinState)
}

func (r *MigrationReconciler) nativeCleanupCaptureUnchanged(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, capture *nativeCleanupCapture) bool {
	if _, _, err := r.nativeCompletionCurrent(ctx, m, worker, pvc, original); err != nil {
		return false
	}
	job, err := r.nativePreflightLiveJob(ctx, capture.expected, capture.created, capture.binding)
	if err != nil || !nativeEvidenceWorkerComplete(job) || !apiequality.Semantic.DeepEqual(capture.observed.Status, job.Status) {
		return false
	}
	pod, err := r.nativePreflightPod(ctx, job)
	return err == nil && pod != nil && ctx.Err() == nil && nativePreflightPodMatches(job, pod) && nativePreflightSamePod(capture.pod, pod) && capture.pod.Generation == pod.Generation && apiequality.Semantic.DeepEqual(capture.pod.Spec, pod.Spec) && podexec.SamePodState(capture.pod, pod, workerContainer)
}

func (r *MigrationReconciler) consumeNativeCompletedHelper(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, capture *nativeStateHelper) error {
	if !r.nativeEvidenceHelperUnchanged(ctx, capture.created, m, nativeEvidenceHelperBinding{podUID: capture.pod.UID, containerID: capture.data.Container.ContainerID}, capture.data.PlatformImageID) {
		return errNativeEvidenceLifecycle
	}
	if _, err := r.nativeEvidenceMigration(ctx, m, worker, pvc); err != nil {
		return errNativeEvidenceLifecycle
	}
	if _, _, err := r.nativeCompletionCurrent(ctx, m, worker, pvc, original); err != nil {
		return errNativeEvidenceLifecycle
	}
	pod, err := r.nativeStateHelperClosed(ctx, m, capture, true)
	if err != nil || r.nativePreflightCleanup(ctx, m, capture.created, pod) != nil {
		return errNativeEvidenceLifecycle
	}
	return nil
}

func (r *MigrationReconciler) nativeCompletedHelperPod(ctx context.Context, job *batchv1.Job, original nativeEvidenceHelperBinding) (*corev1.Pod, error) {
	pod, err := r.nativePreflightPod(ctx, job)
	if err != nil || pod == nil || pod.UID != original.podUID || !nativePreflightPodMatches(job, pod) || len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].ContainerID != original.containerID {
		return nil, errNativeEvidenceLifecycle
	}
	return pod, nil
}
