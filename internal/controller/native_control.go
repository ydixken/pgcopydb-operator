package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var errNativeControlAdmission = errors.New("native control execution is not admitted")

type nativeControlFacts struct {
	data      native.ExecutionData
	migration *v1beta1.Migration
	worker    *batchv1.Job
	pvc       *corev1.PersistentVolumeClaim
	pod       *corev1.Pod
}

func nativeControlEnd(m *v1beta1.Migration) string {
	if m == nil || m.Status.Replication == nil {
		return ""
	}
	return m.Status.Replication.Endpos
}

func nativeControlValidEnd(e string) bool {
	// The active recovery launcher accepts strict X/Y words, unlike legacy whitespace-tolerant reads.
	hi, lo, ok := strings.Cut(e, "/")
	if !ok || len(hi) == 0 || len(hi) > 8 || len(lo) == 0 || len(lo) > 8 || strings.TrimSpace(e) != e || strings.Contains(e, "+") {
		return false
	}
	n, err := sentinel.ParseLSN(e)
	return err == nil && n != 0
}

func (r *MigrationReconciler) admitNativeWorkerExecution(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (native.ExecutionData, error) {
	facts, err := r.admitNativeControlFacts(ctx, m, worker, pvc)
	if err != nil {
		return native.ExecutionData{}, err
	}
	return facts.data, nil
}

func (r *MigrationReconciler) admitNativePersistedEndpos(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (native.ExecutionData, string, error) {
	facts, e, err := r.nativePersistedEndFacts(ctx, m, worker, pvc)
	return facts.data, e, err
}

func (r *MigrationReconciler) nativePersistedEndFacts(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (nativeControlFacts, string, error) {
	e := nativeControlEnd(m)
	if !nativeControlValidEnd(e) {
		return nativeControlFacts{}, "", errNativeControlAdmission
	}
	facts, err := r.admitNativeControlFacts(ctx, m, worker, pvc)
	if err != nil || nativeControlEnd(facts.migration) != e {
		return nativeControlFacts{}, "", errNativeControlAdmission
	}
	return facts, e, nil
}

func (r *MigrationReconciler) admitNativeControlFacts(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (nativeControlFacts, error) {
	if r.APIReader == nil || r.Scheme == nil || m == nil || worker == nil || pvc == nil || ctx.Err() != nil {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	var operation [16]byte
	if _, err := rand.Read(operation[:]); err != nil {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	op := hex.EncodeToString(operation[:])
	before, err := r.nativeControlFacts(ctx, m, worker, pvc, op)
	if err != nil {
		return nativeControlFacts{}, err
	}
	after, err := r.nativeControlFacts(ctx, m, worker, pvc, op)
	if err != nil || !sameNativeControlFacts(before, after) || ctx.Err() != nil {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	return after, nil
}

func (r *MigrationReconciler) nativeControlFacts(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim, op string) (nativeControlFacts, error) {
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	live, err := r.nativePreflightMigration(ctx, m, image)
	if err != nil || live.Status.Attempts < 1 || live.Status.JobName != jobName(live, live.Status.Attempts) {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	claim := &corev1.PersistentVolumeClaim{}
	job := &batchv1.Job{}
	if !nativeControlObjectsMatch(live, worker, pvc) || !native.NativeReceiptsMatch(live, worker.UID, pvc.UID) {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(pvc), claim) != nil || claim.UID != pvc.UID || claim.Status.Phase != corev1.ClaimBound || !apiequality.Semantic.DeepEqual(claim.Spec, pvc.Spec) || !apiequality.Semantic.DeepEqual(claim.OwnerReferences, pvc.OwnerReferences) {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	work, err := nativeWorkIdentity(live, claim)
	if err != nil {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	expected, err := buildNativeWorkerJob(live, claim, r.RPGMigrateRunnerImage, live.Status.Attempts)
	if err != nil || controllerutil.SetControllerReference(live, expected, r.Scheme) != nil {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(worker), job) != nil || job.UID != worker.UID || job.Generation != worker.Generation || !apiequality.Semantic.DeepEqual(job.Spec, worker.Spec) || !nativePreflightJobMatches(expected, job) || !nativeControlJobRunning(job) {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	pod, err := r.nativePreflightPod(ctx, job)
	if err != nil || pod == nil || !nativePreflightPodMatches(job, pod) || !nativeControlPodRunning(pod, image) {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	template := podexec.NativeTemplateSHA256(job.Spec.Template)
	references, err := nativeWorkerConnectionReferenceSHA256(expected)
	if err != nil || template == ([32]byte{}) || references == ([32]byte{}) {
		return nativeControlFacts{}, errNativeControlAdmission
	}
	status := pod.Status.ContainerStatuses[0]
	data := native.ExecutionData{Namespace: live.Namespace, MigrationName: live.Name, MigrationUID: live.UID, MigrationGeneration: live.Generation, PVCName: claim.Name, PVCUID: claim.UID, JobName: job.Name, JobUID: job.UID, WorkerJobUID: job.UID, Attempt: live.Status.Attempts, Role: native.JobRoleWorker, Work: work, PodName: pod.Name, ImageRef: image, PlatformImageID: image, TemplateSHA256: template, ConnectionReferenceSHA256: references, OperationID: op, ProgramSHA256: native.EvidenceProgramSHA256(), Container: podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: workerContainer, ContainerID: status.ContainerID, ImageID: image, TemplateSHA256: template, ExecutionSHA256: podexec.NativeExecutionSHA256(job.Spec.Template)}}
	return nativeControlFacts{data: data, migration: live, worker: job, pvc: claim, pod: pod}, nil
}

func nativeControlObjectsMatch(m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) bool {
	return pvc.UID != "" && pvc.Name == workPVCName(m) && pvc.Namespace == m.Namespace && worker.UID != "" && worker.Name == m.Status.JobName && worker.Namespace == m.Namespace
}

func nativeWorkerConnectionReferenceSHA256(expected *batchv1.Job) ([32]byte, error) {
	if expected == nil || len(expected.Spec.Template.Spec.Containers) != 1 {
		return [32]byte{}, errNativeControlAdmission
	}
	pod := expected.Spec.Template.Spec
	return native.StructuralConnectionReferenceSHA256(pod.Containers[0].Env, pod.Volumes, pod.Containers[0].VolumeMounts)
}

func nativeControlJobRunning(job *batchv1.Job) bool {
	if job.Status.Active != 1 || job.Status.Succeeded != 0 || job.Status.Failed != 0 {
		return false
	}
	for _, c := range job.Status.Conditions {
		if c.Status == corev1.ConditionTrue && (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed || c.Type == batchv1.JobFailureTarget) {
			return false
		}
	}
	return true
}

// nativeWorkerStarting reports a live Job whose pod is not yet ready, or not yet counted.
// An unknown ready count (nil with an active pod) is not a wait, so admission still decides it.
func nativeWorkerStarting(job *batchv1.Job) bool {
	s := job.Status
	if s.Ready != nil && *s.Ready != 0 {
		return false
	}
	if s.Active == 0 {
		return s.Succeeded == 0 && s.Failed == 0 && len(s.Conditions) == 0
	}
	return s.Ready != nil && nativeControlJobRunning(job)
}

func nativeControlPodRunning(pod *corev1.Pod, image string) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" || len(pod.Status.ContainerStatuses) != 1 || len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 {
		return false
	}
	c := pod.Status.ContainerStatuses[0]
	return c.Name == workerContainer && c.ContainerID != "" && c.Image != "" && c.ImageID == image && c.RestartCount == 0 && c.LastTerminationState == (corev1.ContainerState{}) && c.State.Running != nil && c.State.Waiting == nil && c.State.Terminated == nil
}

func sameNativeControlFacts(a, b nativeControlFacts) bool {
	return a.data == b.data && nativeControlEnd(a.migration) == nativeControlEnd(b.migration) && apiequality.Semantic.DeepEqual(a.pod.Spec, b.pod.Spec) && apiequality.Semantic.DeepEqual(a.worker.Spec, b.worker.Spec) && apiequality.Semantic.DeepEqual(a.pvc.Spec, b.pvc.Spec)
}
