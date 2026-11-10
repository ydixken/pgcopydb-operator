package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var errNativeEvidenceLifecycle = errors.New("native evidence lifecycle unavailable")

// The caller must persist E in an earlier reconciliation; this method only compares live readback.
func (r *MigrationReconciler) readNativeCompletedEvidence(ctx context.Context, expectedMigration *v1beta1.Migration, expectedWorker *batchv1.Job, expectedPVC *corev1.PersistentVolumeClaim) (*native.CompletedObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if r.NativeEvidence == nil || r.APIReader == nil || r.Client == nil || r.Scheme == nil || expectedMigration == nil || expectedWorker == nil || expectedPVC == nil || ctx.Err() != nil {
		return nil, errNativeEvidenceLifecycle
	}
	m, worker, pvc := expectedMigration.DeepCopy(), expectedWorker.DeepCopy(), expectedPVC.DeepCopy()
	_, originalWorker, err := r.nativeCompletionCurrent(ctx, m, worker, pvc, nil)
	if err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	live, err := r.nativeEvidenceMigration(ctx, m, worker, pvc)
	if err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	image := runnerImageFor(live, r.RPGMigrateRunnerImage)
	if !nativeEvidenceDirectImage(image) {
		return nil, errNativeEvidenceLifecycle
	}
	work, err := nativeWorkIdentity(live, pvc)
	if err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	var operation [16]byte
	if _, err := rand.Read(operation[:]); err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	op := hex.EncodeToString(operation[:])
	want := native.ExecutionRequest{Namespace: live.Namespace, MigrationName: live.Name, MigrationUID: live.UID, MigrationGeneration: live.Generation, PVCName: pvc.Name, PVCUID: pvc.UID, WorkerJobUID: worker.UID, Attempt: live.Status.Attempts, Role: native.JobRoleEvidence, Work: work, JobName: "rpgmigrate-evidence-" + op, OperationID: op, ImageRef: image, PlatformImageID: image, ProgramSHA256: native.EvidenceProgramSHA256()}
	job, err := buildNativeEvidenceJob(live, r.RPGMigrateRunnerImage, want)
	if err != nil || controllerutil.SetControllerReference(live, job, r.Scheme) != nil {
		return nil, errNativeEvidenceLifecycle
	}
	if err := r.nativeCleanupPreflight(ctx, live, worker, pvc, originalWorker); err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	expected := job.DeepCopy()
	if native.ValidateEvidenceJobSize(job) != nil {
		return nil, errNativeEvidenceLifecycle
	}
	if r.Create(ctx, job) != nil || job.UID == "" {
		return nil, errNativeEvidenceLifecycle
	}
	want.JobUID = job.UID
	created := job.DeepCopy()
	var original nativeEvidenceHelperBinding
	err = wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		if _, err := r.nativeEvidenceMigration(ctx, m, worker, pvc); err != nil {
			return false, errNativeEvidenceLifecycle
		}
		job, err = r.nativeEvidenceLiveJob(ctx, created, live)
		if err != nil {
			return false, errNativeEvidenceLifecycle
		}
		return r.nativeEvidenceHelperReady(ctx, job, &original)
	})
	if err != nil || ctx.Err() != nil {
		return nil, errNativeEvidenceLifecycle
	}
	want.TemplateSHA256 = podexec.NativeTemplateSHA256(job.Spec.Template)
	want.ConnectionReferenceSHA256, err = native.ConnectionReferenceSHA256(live, want)
	if err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	helperOriginal, err := r.nativeCompletedHelperPod(ctx, job, original)
	if err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	observation, err := r.NativeEvidence.ReadCompleted(ctx, want, live.Status.Replication.Endpos)
	if err != nil || observation == nil || ctx.Err() != nil {
		return nil, errNativeEvidenceLifecycle
	}
	capture := &nativeStateHelper{expected: expected, created: created, pod: helperOriginal, data: native.ExecutionData{PlatformImageID: image, Container: podexec.NativeContainerBinding{ContainerID: original.containerID}}}
	if err := r.consumeNativeCompletedHelper(ctx, m, worker, pvc, originalWorker, capture); err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	return observation, nil
}

func (r *MigrationReconciler) nativeEvidenceMigration(ctx context.Context, expected *v1beta1.Migration, expectedWorker *batchv1.Job, expectedPVC *corev1.PersistentVolumeClaim) (*v1beta1.Migration, error) {
	live := &v1beta1.Migration{}
	pvc := &corev1.PersistentVolumeClaim{}
	worker := &batchv1.Job{}
	for _, read := range []struct {
		name   string
		object client.Object
	}{{expected.Name, live}, {expectedPVC.Name, pvc}, {expectedWorker.Name, worker}} {
		if r.APIReader.Get(ctx, types.NamespacedName{Namespace: expected.Namespace, Name: read.name}, read.object) != nil {
			return nil, errNativeEvidenceLifecycle
		}
	}
	if ctx.Err() != nil || expectedWorker.Namespace != expected.Namespace || expectedPVC.Namespace != expected.Namespace || !nativeEvidenceMigrationMatches(live, expected, worker) {
		return nil, errNativeEvidenceLifecycle
	}
	if expected.Status.Replication == nil || live.Status.Replication == nil || expected.Status.Replication.Endpos != live.Status.Replication.Endpos {
		return nil, errNativeEvidenceLifecycle
	}
	if e, err := sentinel.ParseLSN(live.Status.Replication.Endpos); err != nil || e == 0 {
		return nil, errNativeEvidenceLifecycle
	}
	if pvc.UID == "" || pvc.UID != expectedPVC.UID || pvc.Namespace != live.Namespace || pvc.Name != workPVCName(live) || pvc.Status.Phase != corev1.ClaimBound || !pvc.DeletionTimestamp.IsZero() || !apiequality.Semantic.DeepEqual(pvc.Spec, expectedPVC.Spec) || !nativeEvidenceOwnedBy(pvc, "Migration", live.Name, live.UID, v1beta1.GroupVersion.String()) {
		return nil, errNativeEvidenceLifecycle
	}
	if worker.UID == "" || worker.UID != expectedWorker.UID || worker.Namespace != live.Namespace || !worker.DeletionTimestamp.IsZero() || !apiequality.Semantic.DeepEqual(worker.Spec, expectedWorker.Spec) || !nativeEvidenceOwnedBy(worker, "Migration", live.Name, live.UID, v1beta1.GroupVersion.String()) || !nativeEvidenceWorkerComplete(worker) {
		return nil, errNativeEvidenceLifecycle
	}
	if _, err := nativeWorkIdentity(live, pvc); err != nil {
		return nil, errNativeEvidenceLifecycle
	}
	return live, nil
}

func (r *MigrationReconciler) nativeEvidenceLiveJob(ctx context.Context, created *batchv1.Job, m *v1beta1.Migration) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	if r.APIReader.Get(ctx, types.NamespacedName{Namespace: created.Namespace, Name: created.Name}, job) != nil || job.Name != created.Name || job.Namespace != created.Namespace || job.UID != created.UID || !job.DeletionTimestamp.IsZero() || !apiequality.Semantic.DeepEqual(created.Spec, job.Spec) || !nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
		return nil, errNativeEvidenceLifecycle
	}
	return job, nil
}

func nativeEvidenceMigrationMatches(live, expected *v1beta1.Migration, worker *batchv1.Job) bool {
	return live.Name == expected.Name && live.Namespace == expected.Namespace && expected.UID != "" && live.UID == expected.UID && live.Generation > 0 && live.Generation == expected.Generation && live.DeletionTimestamp.IsZero() && live.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && followEnabled(live) && apiequality.Semantic.DeepEqual(live.Spec, expected.Spec) && live.Status.Attempts > 0 && live.Status.Attempts == expected.Status.Attempts && live.Status.JobName == expected.Status.JobName && live.Status.JobName == worker.Name && worker.Name == jobName(live, live.Status.Attempts)
}

func nativeEvidenceOwnedBy(object metav1.Object, kind, name string, uid types.UID, version string) bool {
	owner := metav1.GetControllerOf(object)
	return owner != nil && owner.Kind == kind && owner.Name == name && owner.UID == uid && owner.APIVersion == version
}

func nativeEvidenceWorkerComplete(job *batchv1.Job) bool {
	if job.Status.Succeeded != 1 || job.Status.Failed != 0 || job.Status.Active != 0 || job.Spec.Completions != nil && *job.Spec.Completions != 1 || job.Spec.Parallelism != nil && *job.Spec.Parallelism != 1 || job.Spec.Suspend != nil && *job.Spec.Suspend {
		return false
	}
	complete := false
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue {
			if condition.Type == batchv1.JobFailed || condition.Type == batchv1.JobFailureTarget {
				return false
			}
			complete = complete || condition.Type == batchv1.JobComplete
		}
	}
	return complete
}

type nativeEvidenceHelperBinding struct {
	podUID      types.UID
	containerID string
}

func (r *MigrationReconciler) nativeEvidenceHelperReady(ctx context.Context, job *batchv1.Job, original *nativeEvidenceHelperBinding) (bool, error) {
	if job.Status.Succeeded != 0 || job.Status.Failed != 0 {
		return false, errNativeEvidenceLifecycle
	}
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed || condition.Type == batchv1.JobFailureTarget) {
			return false, errNativeEvidenceLifecycle
		}
	}
	pod, err := r.nativeEvidenceHelperPod(ctx, job, &original.podUID)
	if err != nil {
		return false, errNativeEvidenceLifecycle
	}
	if pod == nil {
		return false, nil
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false, errNativeEvidenceLifecycle
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return false, nil
	}
	if len(pod.Status.ContainerStatuses) != 1 {
		return false, errNativeEvidenceLifecycle
	}
	status := pod.Status.ContainerStatuses[0]
	if status.Name != workerContainer || status.RestartCount != 0 || status.State.Terminated != nil || status.LastTerminationState != (corev1.ContainerState{}) {
		return false, errNativeEvidenceLifecycle
	}
	if original.containerID != "" && status.ContainerID != original.containerID {
		return false, errNativeEvidenceLifecycle
	}
	original.containerID = status.ContainerID
	return pod.Status.Phase == corev1.PodRunning && status.State.Running != nil && status.ContainerID != "", nil
}

func (r *MigrationReconciler) nativeEvidenceHelperPod(ctx context.Context, job *batchv1.Job, originalPodUID *types.UID) (*corev1.Pod, error) {
	pods := &corev1.PodList{}
	if r.APIReader.List(ctx, pods, client.InNamespace(job.Namespace)) != nil {
		return nil, errNativeEvidenceLifecycle
	}
	var selected *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if owner := metav1.GetControllerOf(pod); owner != nil && owner.UID == job.UID {
			if selected != nil {
				return nil, errNativeEvidenceLifecycle
			}
			selected = pod
		}
	}
	if selected == nil {
		return nil, nil
	}
	pod := &corev1.Pod{}
	if r.APIReader.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: selected.Name}, pod) != nil || pod.Name != selected.Name || pod.Namespace != job.Namespace || pod.UID == "" || pod.UID != selected.UID || !pod.DeletionTimestamp.IsZero() || !nativeEvidenceOwnedBy(pod, "Job", job.Name, job.UID, "batch/v1") {
		return nil, errNativeEvidenceLifecycle
	}
	if *originalPodUID != "" && *originalPodUID != pod.UID {
		return nil, errNativeEvidenceLifecycle
	}
	*originalPodUID = pod.UID
	return pod, nil
}

func (r *MigrationReconciler) nativeEvidenceHelperUnchanged(ctx context.Context, created *batchv1.Job, m *v1beta1.Migration, original nativeEvidenceHelperBinding, image string) bool {
	job, err := r.nativeEvidenceLiveJob(ctx, created, m)
	if err != nil {
		return false
	}
	pod, err := r.nativeEvidenceHelperPod(ctx, job, &original.podUID)
	if err != nil || pod == nil || len(pod.Status.ContainerStatuses) != 1 {
		return false
	}
	status := pod.Status.ContainerStatuses[0]
	return status.Name == workerContainer && status.RestartCount == 0 && status.LastTerminationState == (corev1.ContainerState{}) && status.ContainerID == original.containerID && status.ImageID == image
}

// Alpha accepts a narrow canonical repository subset; image type and runtime identity need separate qualification.
func nativeEvidenceDirectImage(image string) bool {
	repository, digest, ok := strings.Cut(image, "@sha256:")
	if !ok || len(image) > 256 || len(digest) != 64 || strings.ContainsFunc(digest, func(c rune) bool { return c < '0' || c > '9' && c < 'a' || c > 'f' }) || strings.ContainsAny(repository, "@%\\?#") || strings.ContainsFunc(repository, func(c rune) bool { return c <= ' ' || c > '~' || c >= 'A' && c <= 'Z' }) {
		return false
	}
	u, err := url.Parse("https://" + repository)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.Host == "" || strings.Count(u.Host, ":") > 1 {
		return false
	}
	return nativeEvidenceImageRegistry(u) && nativeEvidenceImagePath(u)
}

func nativeEvidenceImageRegistry(u *url.URL) bool {
	host := u.Hostname()
	ip := net.ParseIP(host)
	if ip != nil {
		if ip.To4() == nil || ip.String() != host {
			return false
		}
	} else if len(validation.IsDNS1123Subdomain(host)) != 0 || host != "localhost" && !strings.Contains(host, ".") || strings.Trim(host, "0123456789.") == "" {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
			return false
		}
	} else if strings.Contains(u.Host, ":") {
		return false
	}
	return true
}

func nativeEvidenceImagePath(u *url.URL) bool {
	if !strings.HasPrefix(u.Path, "/") || u.RawPath != "" {
		return false
	}
	for segment := range strings.SplitSeq(strings.TrimPrefix(u.Path, "/"), "/") {
		if segment == "" {
			return false
		}
		separator := true
		for _, c := range segment {
			if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
				separator = false
				continue
			}
			if c != '.' && c != '_' && c != '-' || separator {
				return false
			}
			separator = true
		}
		if separator {
			return false
		}
	}
	return true
}
