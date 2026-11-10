package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

func nativeRecoveryOperationID() (string, error) {
	var operation [16]byte
	if _, err := rand.Read(operation[:]); err != nil {
		return "", errNativeRecovery
	}
	return hex.EncodeToString(operation[:]), nil
}

func (r *MigrationReconciler) nativeRecoveryQuiescentCurrent(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (*v1beta1.Migration, *corev1.Pod, error) {
	if m == nil || worker == nil || pvc == nil || ctx.Err() != nil || !nativeEvidenceDirectImage(runnerImageFor(m, r.RPGMigrateRunnerImage)) {
		return nil, nil, errNativeRecovery
	}
	live, current, claim, err := r.nativeRecoveryCurrent(ctx, m, worker, pvc)
	if err != nil || current.Generation != worker.Generation {
		return nil, nil, errNativeRecovery
	}
	pod, err := r.nativeQuiescentWorker(ctx, live, current, claim)
	if err != nil || ctx.Err() != nil || original != nil && (!nativePreflightSamePod(original, pod) || original.Generation != pod.Generation ||
		!apiequality.Semantic.DeepEqual(original.Spec, pod.Spec) || !apiequality.Semantic.DeepEqual(original.Status, pod.Status)) {
		return nil, nil, errNativeRecovery
	}
	return live, pod, nil
}

func buildNativeRecoveryReadJob(m *v1beta1.Migration, image string, w native.ExecutionData) (*batchv1.Job, error) {
	if m == nil || !native.NativeReceiptsMatch(m, w.WorkerJobUID, w.PVCUID) || m.UID != w.MigrationUID || m.Generation != w.MigrationGeneration || m.Status.Attempts != w.Attempt || m.Status.JobName != jobName(m, w.Attempt) {
		return nil, errNativeRecovery
	}
	materialize, prefix := native.RetryMaterialization, "rpgmigrate-retry-"
	if w.Role == native.JobRoleCleanup {
		materialize, prefix = native.OwnedInventoryMaterialization, "rpgmigrate-inventory-"
	} else if w.Role != native.JobRoleRetryGate {
		return nil, errNativeRecovery
	}
	if w.JobName != prefix+w.OperationID || w.Namespace != m.Namespace || w.MigrationName != m.Name || w.ImageRef != runnerImageFor(m, image) || w.PlatformImageID != w.ImageRef {
		return nil, errNativeRecovery
	}
	command, args, env, volumes, mounts, err := materialize(m, w)
	if err != nil {
		return nil, errNativeRecovery
	}
	job, err := buildNativeVersionJob(m, image, "rpgmigrate-version-"+w.OperationID)
	if err != nil {
		return nil, errNativeRecovery
	}
	job.Name, job.Spec.TTLSecondsAfterFinished = w.JobName, nil
	job.Spec.Template.Spec.Volumes = volumes
	c := &job.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
	if native.ValidateEvidenceJobSize(job) != nil {
		return nil, errNativeRecovery
	}
	return job, nil
}

// Every credential-bearing helper gets fresh discovery and retains its known UID until checked closure.
func (r *MigrationReconciler) runNativeRecoveryRead(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, role native.JobRole, consume func(context.Context, native.ExecutionData) error) (err error) {
	if r.NativeEvidence == nil || consume == nil {
		return errNativeRecovery
	}
	live, original, err := r.nativeRecoveryQuiescentCurrent(ctx, m, worker, pvc, original)
	if err != nil || r.ensureNativeRecoveryPreflight(ctx, live, worker, pvc, original) != nil {
		return errNativeRecovery
	}
	workIdentity, err := nativeWorkIdentity(live, pvc)
	if err != nil {
		return errNativeRecovery
	}
	var operation [16]byte
	if _, err := rand.Read(operation[:]); err != nil {
		return errNativeRecovery
	}
	op := hex.EncodeToString(operation[:])
	prefix, program := "rpgmigrate-retry-", native.EvidenceProgramSHA256()
	if role == native.JobRoleCleanup {
		prefix, program = "rpgmigrate-inventory-", native.OwnedInventoryProgramSHA256()
	}
	image := runnerImageFor(live, r.RPGMigrateRunnerImage)
	w := native.ExecutionData{Namespace: live.Namespace, MigrationName: live.Name, MigrationUID: live.UID, MigrationGeneration: live.Generation, PVCName: pvc.Name, PVCUID: pvc.UID, WorkerJobUID: worker.UID, Attempt: live.Status.Attempts, Role: role, Work: workIdentity, JobName: prefix + op, OperationID: op, ImageRef: image, PlatformImageID: image, ProgramSHA256: program}
	expected, err := buildNativeRecoveryReadJob(live, r.RPGMigrateRunnerImage, w)
	if err != nil || controllerutil.SetControllerReference(live, expected, r.Scheme) != nil || native.ValidateEvidenceJobSize(expected) != nil {
		return errNativeRecovery
	}
	deadline := time.Now().Add(90 * time.Second)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	work, cancel := context.WithDeadline(ctx, deadline.Add(-nativePreflightCleanupReserve))
	defer cancel()
	if _, _, err := r.nativeRecoveryQuiescentCurrent(work, live, worker, pvc, original); err != nil || work.Err() != nil {
		return errNativeRecovery
	}
	created := expected.DeepCopy()
	if r.Create(work, created) != nil || created.UID == "" {
		return errNativeRecovery
	}
	var helperPod *corev1.Pod
	defer func() {
		cleanup, end := context.WithDeadline(context.WithoutCancel(ctx), deadline)
		defer end()
		if r.nativePreflightCleanup(cleanup, live, created, helperPod) != nil {
			err = errNativeRecovery
		}
	}()
	if created.UID == worker.UID || !nativePreflightJobMatches(expected, created) {
		return errNativeRecovery
	}
	w.JobUID = created.UID
	var checkedBinding podexec.NativeContainerBinding
	helperPod, checkedBinding, err = r.nativeRecoveryReadReady(work, live, worker, pvc, original, expected, created, &w)
	if err != nil || consume(work, w) != nil {
		return errNativeRecovery
	}
	return r.nativeRecoveryReadClosed(work, live, worker, pvc, original, expected, created, helperPod, checkedBinding)
}

// Recovery discovery projects no credentials and preserves the actual terminal and current intent.
func (r *MigrationReconciler) ensureNativeRecoveryPreflight(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod) (err error) {
	deadline := time.Now().Add(nativePreflightTotal)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	if m == nil || !r.nativePreflightDependencies() || ctx.Err() != nil || time.Until(deadline) <= nativePreflightCleanupReserve {
		return errNativePreflightUnavailable
	}
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	admit := func(ctx context.Context, expected *v1beta1.Migration, expectedImage string) (*v1beta1.Migration, error) {
		live, pod, err := r.nativeRecoveryQuiescentCurrent(ctx, expected, worker, pvc, original)
		if err != nil || runnerImageFor(live, r.RPGMigrateRunnerImage) != expectedImage {
			return nil, errNativePreflightUnavailable
		}
		if original == nil {
			original = pod.DeepCopy()
		}
		return live, nil
	}
	live, err := admit(ctx, m, image)
	if err != nil {
		return errNativePreflightUnavailable
	}
	if _, err := nativeConfigCapabilities(live); err != nil {
		return errNativePreflightUnavailable
	}
	var operation [16]byte
	if _, err := rand.Read(operation[:]); err != nil {
		return errNativePreflightUnavailable
	}
	job, err := buildNativeVersionJob(live, r.RPGMigrateRunnerImage, "rpgmigrate-version-"+hex.EncodeToString(operation[:]))
	if err != nil {
		return errNativePreflightUnavailable
	}
	work, cancel := context.WithDeadline(ctx, deadline.Add(-nativePreflightCleanupReserve))
	defer cancel()
	payload, err := r.nativePreflightPhase(ctx, work, deadline, live, image, job, true, admit)
	if err != nil {
		return errNativePreflightUnavailable
	}
	version, err := native.ParseVersionLine(payload)
	if err != nil || version == nil || validateNativeConfig(live, version.Capabilities) != nil {
		return errNativePreflightUnavailable
	}
	if _, err := admit(work, live, image); err != nil || work.Err() != nil {
		return errNativePreflightUnavailable
	}
	return nil
}

func (r *MigrationReconciler) nativeRecoveryReadReady(ctx context.Context, live *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, expected, created *batchv1.Job, w *native.ExecutionData) (*corev1.Pod, podexec.NativeContainerBinding, error) {
	role := w.Role
	var helperPod *corev1.Pod
	image := w.PlatformImageID
	binding := nativeEvidenceHelperBinding{}
	var checkedBinding podexec.NativeContainerBinding
	err := wait.PollUntilContextCancel(ctx, nativePreflightPoll, true, func(ctx context.Context) (bool, error) {
		if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, live, worker, pvc, original); err != nil {
			return false, errNativeRecovery
		}
		job, err := r.nativeEvidenceLiveJob(ctx, created, live)
		if err != nil || job.Generation != created.Generation || !nativePreflightJobMatches(expected, job) {
			return false, errNativeRecovery
		}
		ready, err := r.nativeEvidenceHelperReady(ctx, job, &binding)
		if err != nil || !ready {
			return false, err
		}
		pod, err := r.nativePreflightPod(ctx, job)
		if err != nil || pod == nil || pod.UID != binding.podUID || !nativePreflightPodMatches(job, pod) || !nativeControlPodRunning(pod, image) {
			return false, errNativeRecovery
		}
		helperPod = pod.DeepCopy()
		w.TemplateSHA256 = podexec.NativeTemplateSHA256(job.Spec.Template)
		w.PodName = pod.Name
		checkedBinding = podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: workerContainer, ContainerID: binding.containerID, ImageID: image, TemplateSHA256: w.TemplateSHA256, ExecutionSHA256: podexec.NativeExecutionSHA256(job.Spec.Template)}
		w.Container = checkedBinding
		if role == native.JobRoleRetryGate {
			// The retry Reader's existing binding excludes the full-spec digest.
			w.Container.ExecutionSHA256 = [32]byte{}
		}
		return true, nil
	})
	if err != nil {
		return nil, podexec.NativeContainerBinding{}, errNativeRecovery
	}
	reference := native.RetryConnectionReferenceSHA256
	if role == native.JobRoleCleanup {
		reference = native.OwnedInventoryConnectionReferenceSHA256
	}
	w.ConnectionReferenceSHA256, err = reference(live, *w)
	if err != nil {
		return nil, podexec.NativeContainerBinding{}, errNativeRecovery
	}

	return helperPod, checkedBinding, nil

}
func (r *MigrationReconciler) nativeRecoveryReadClosed(ctx context.Context, live *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, original *corev1.Pod, expected, created *batchv1.Job, helperPod *corev1.Pod, checkedBinding podexec.NativeContainerBinding) error {
	image := runnerImageFor(live, r.RPGMigrateRunnerImage)
	if _, _, err := r.nativeRecoveryQuiescentCurrent(ctx, live, worker, pvc, original); err != nil {
		return errNativeRecovery
	}
	job, err := r.nativePreflightLiveJob(ctx, expected, created, checkedBinding)
	if err != nil || job.Generation != created.Generation || !nativeEvidenceWorkerComplete(job) {
		return errNativeRecovery
	}
	pod, err := r.nativePreflightPod(ctx, job)
	if err != nil || pod == nil || !nativePreflightPodMatches(job, pod) || !nativePreflightSamePod(helperPod, pod) || !apiequality.Semantic.DeepEqual(helperPod.Spec, pod.Spec) || !nativeRecoveryPodTerminal(pod, image) || pod.Status.Phase != corev1.PodSucceeded || pod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 || pod.Status.ContainerStatuses[0].State.Terminated.Signal != 0 {
		return errNativeRecovery
	}
	return nil
}
