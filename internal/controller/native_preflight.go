package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

// NativePreflightReader supplies original execution bytes, never capability or completion authority.
type NativePreflightReader interface {
	JobVersionNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
	JobPreflightNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
}

var (
	errNativePreflightUnavailable = errors.New("native preflight unavailable")
	errNativePreflightTerminal    = errors.New("native preflight execution refused")
)

const (
	nativePreflightDefaultSA      = "default"
	nativePreflightTotal          = 90 * time.Second
	nativePreflightCleanupReserve = 30 * time.Second
	nativePreflightPoll           = 100 * time.Millisecond
)

func (r *MigrationReconciler) ensureNativePreflight(ctx context.Context, m *v1beta1.Migration) (version *native.Version, passed bool, failure string, err error) {
	defer func() {
		if ctx.Err() != nil {
			version, passed, failure, err = nil, false, "", ctx.Err()
		}
	}()
	deadline := r.currentTime().Add(nativePreflightTotal)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	if ctx.Err() != nil {
		return nil, false, "", ctx.Err()
	}
	if m == nil || !r.nativePreflightDependencies() || time.Until(deadline) <= nativePreflightCleanupReserve {
		return nil, false, "", errNativePreflightUnavailable
	}
	image := runnerImageFor(m, r.RPGMigrateRunnerImage)
	if !nativeEvidenceDirectImage(image) {
		return nil, false, "native discovery configuration refused", nil
	}
	live, err := r.nativePreflightMigration(ctx, m, image)
	if err != nil {
		return nil, false, "", err
	}
	if _, err := nativeConfigCapabilities(live); err != nil {
		return nil, false, "native preflight configuration refused", nil
	}
	work, cancel := context.WithDeadline(ctx, deadline.Add(-nativePreflightCleanupReserve))
	defer cancel()
	var operation [16]byte
	if _, err := rand.Read(operation[:]); err != nil {
		return nil, false, "", errNativePreflightUnavailable
	}
	op := hex.EncodeToString(operation[:])
	job, err := buildNativeVersionJob(live, r.RPGMigrateRunnerImage, "rpgmigrate-version-"+op)
	if err != nil {
		return nil, false, "native discovery configuration refused", nil
	}
	payload, err := r.nativePreflightPhase(ctx, work, deadline, live, image, job, true)
	if err != nil || ctx.Err() != nil {
		return nativePreflightPhaseFailure(ctx, nil, err)
	}
	version, err = native.ParseVersionLine(payload)
	if err != nil {
		return nil, false, "native discovery result refused", nil
	}
	if validateNativeConfig(live, version.Capabilities) != nil {
		return nil, false, "native runner capabilities are not qualified", nil
	}
	if work.Err() != nil {
		return version, false, "", nil
	}
	current, err := r.nativePreflightMigration(work, live, image)
	if err != nil {
		if work.Err() != nil {
			return version, false, "", nil
		}
		return nil, false, "", errNativePreflightUnavailable
	}
	if work.Err() != nil {
		return version, false, "", nil
	}
	job, err = buildNativePreflightJob(current, r.RPGMigrateRunnerImage, "rpgmigrate-preflight-"+op, op)
	if err != nil {
		return nil, false, "native preflight configuration refused", nil
	}
	payload, err = r.nativePreflightPhase(ctx, work, deadline, current, image, job, false)
	if err != nil || ctx.Err() != nil {
		return nativePreflightPhaseFailure(ctx, version, err)
	}
	checked, err := native.ParsePreflightResult(payload, op, native.EvidenceProgramSHA256(), followEnabled(live))
	if err != nil || checked == nil || !checked.Passed() {
		return nil, false, "native database preflight refused", nil
	}
	if _, err := r.nativePreflightMigration(work, live, image); err != nil || work.Err() != nil {
		return nil, false, "", errNativePreflightUnavailable
	}
	return version, true, "", nil
}

func (r *MigrationReconciler) nativePreflightDependencies() bool {
	return r.Client != nil && r.APIReader != nil && r.Scheme != nil && r.NativePreflight != nil
}

func nativePreflightPhaseFailure(ctx context.Context, version *native.Version, err error) (*native.Version, bool, string, error) {
	if ctx.Err() != nil {
		return nil, false, "", ctx.Err()
	}
	if errors.Is(err, errNativePreflightTerminal) {
		return nil, false, errNativePreflightTerminal.Error(), nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return version, false, "", nil
	}
	return nil, false, "", errNativePreflightUnavailable
}

func (r *MigrationReconciler) nativePreflightMigration(ctx context.Context, expected *v1beta1.Migration, image string) (*v1beta1.Migration, error) {
	if expected == nil || !nativeEvidenceDirectImage(image) {
		return nil, errNativePreflightUnavailable
	}
	uid, err := uuid.Parse(string(expected.UID))
	if err != nil || uid == uuid.Nil || uid.String() != string(expected.UID) {
		return nil, errNativePreflightUnavailable
	}
	live := &v1beta1.Migration{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), live) != nil || ctx.Err() != nil || live.ResourceVersion == "" ||
		live.Name != expected.Name || live.Namespace != expected.Namespace || live.UID != expected.UID || live.Generation < 1 || live.Generation != expected.Generation || !live.DeletionTimestamp.IsZero() ||
		live.Spec.Engine != v1beta1.MigrationEngineRPGMigrate || live.Spec.Suspend || !apiequality.Semantic.DeepEqual(live.Spec, expected.Spec) ||
		!apiequality.Semantic.DeepEqual(live.Labels, expected.Labels) || !apiequality.Semantic.DeepEqual(live.Annotations, expected.Annotations) ||
		live.Status.Attempts < 0 || live.Status.Attempts != expected.Status.Attempts || live.Status.JobName != expected.Status.JobName ||
		live.Status.Phase == v1beta1.PhaseCompleted || live.Status.Phase == v1beta1.PhaseFailed || live.Status.Phase == v1beta1.PhaseSuspended ||
		meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionComplete) || meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionFailed) || runnerImageFor(live, r.RPGMigrateRunnerImage) != image {
		return nil, errNativePreflightUnavailable
	}
	return live, nil
}

func (r *MigrationReconciler) nativePreflightPhase(parent, work context.Context, deadline time.Time, m *v1beta1.Migration, image string, expected *batchv1.Job, version bool) (raw []byte, err error) {
	if work.Err() != nil {
		return nil, work.Err()
	}
	if _, err := r.nativePreflightMigration(work, m, image); err != nil {
		if work.Err() != nil {
			return nil, work.Err()
		}
		return nil, errNativePreflightUnavailable
	}
	if work.Err() != nil {
		return nil, work.Err()
	}
	if controllerutil.SetControllerReference(m, expected, r.Scheme) != nil {
		return nil, errNativePreflightUnavailable
	}
	created := expected.DeepCopy()
	if r.Create(work, created) != nil || created.UID == "" {
		return nil, errNativePreflightUnavailable
	}
	var original *corev1.Pod
	var observed *batchv1.Job
	defer func() {
		// Cancellation may stop work, but it must not discard the original cleanup reservation.
		cleanup, end := context.WithDeadline(context.WithoutCancel(parent), minDeadline(deadline, time.Now().Add(nativePreflightCleanupReserve)))
		defer end()
		if r.nativePreflightCleanup(cleanup, m, created, original) != nil {
			raw = nil
			err = errNativePreflightUnavailable
		}
	}()
	if !nativePreflightJobMatches(expected, created) {
		return nil, errNativePreflightUnavailable
	}
	// The complete independent canonical comparison above admits this private copy before hashing.
	admitted := created.DeepCopy()
	binding := podexec.NativeContainerBinding{ContainerName: workerContainer, ImageID: image, TemplateSHA256: podexec.NativeTemplateSHA256(admitted.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(admitted.Spec.Template)}
	if binding.TemplateSHA256 == ([32]byte{}) || binding.ExecutionSHA256 == ([32]byte{}) {
		return nil, errNativePreflightUnavailable
	}
	err = wait.PollUntilContextCancel(work, nativePreflightPoll, true, func(ctx context.Context) (bool, error) {
		if _, err := r.nativePreflightMigration(ctx, m, image); err != nil {
			return false, errNativePreflightUnavailable
		}
		job, err := r.nativePreflightLiveJob(ctx, expected, created, binding)
		if err != nil {
			return false, err
		}
		if nativePreflightJobFailed(job) {
			return false, errNativePreflightTerminal
		}
		pod, err := r.nativePreflightPod(ctx, job)
		if err != nil {
			return false, err
		}
		if pod == nil {
			return false, nil
		}

		if !nativePreflightPodMatches(job, pod) || !nativePreflightSamePod(original, pod) {
			return false, errNativePreflightUnavailable
		}
		original = pod.DeepCopy()
		ready, err := nativePreflightContainerReady(pod, image)
		if err != nil || !ready {
			return false, err
		}
		if !nativeEvidenceWorkerComplete(job) || pod.Status.Phase != corev1.PodSucceeded {
			return false, nil
		}
		observed = job.DeepCopy()
		s := pod.Status.ContainerStatuses[0]
		binding.PodUID, binding.ContainerID = pod.UID, s.ContainerID
		return true, nil
	})
	if err != nil {
		if work.Err() != nil {
			return nil, work.Err()
		}
		return nil, err
	}
	read := r.NativePreflight.JobVersionNative
	if !version {
		read = r.NativePreflight.JobPreflightNative
	}
	outcome, err := read(work, m.Namespace, created.UID, binding)
	if err != nil || !outcome.Successful() {
		if work.Err() != nil {
			return nil, work.Err()
		}
		return nil, errNativePreflightUnavailable
	}
	if !r.nativePreflightExecutionUnchanged(work, m, image, expected, observed, original, binding) {
		return nil, errNativePreflightUnavailable
	}
	return outcome.Payload(), nil
}

func (r *MigrationReconciler) nativePreflightLiveJob(ctx context.Context, expected, known *batchv1.Job, binding podexec.NativeContainerBinding) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(known), job) != nil || job.UID != known.UID || job.Generation != known.Generation || !nativePreflightJobMatches(expected, job) || podexec.NativeExecutionSHA256(job.Spec.Template) != binding.ExecutionSHA256 {
		return nil, errNativePreflightUnavailable
	}
	return job, nil
}
func nativePreflightSamePod(original, pod *corev1.Pod) bool {
	if original == nil {
		return true
	}
	if pod.UID != original.UID || pod.Name != original.Name || original.Spec.NodeName != "" && pod.Spec.NodeName != original.Spec.NodeName {
		return false
	}
	if len(original.Status.ContainerStatuses) == 1 && len(pod.Status.ContainerStatuses) == 1 {
		id := original.Status.ContainerStatuses[0].ContainerID
		if id != "" && id != pod.Status.ContainerStatuses[0].ContainerID {
			return false
		}
	}
	return true
}
func nativePreflightContainerReady(pod *corev1.Pod, image string) (bool, error) {
	if len(pod.Status.ContainerStatuses) == 0 {
		return false, nil
	}
	if len(pod.Status.ContainerStatuses) != 1 || len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 {
		return false, errNativePreflightUnavailable
	}
	s := pod.Status.ContainerStatuses[0]
	if s.Name != workerContainer || s.RestartCount != 0 || s.LastTerminationState != (corev1.ContainerState{}) {
		return false, errNativePreflightUnavailable
	}
	if pod.Status.Phase == corev1.PodFailed || s.State.Terminated != nil && (s.State.Terminated.ExitCode != 0 || s.State.Terminated.Signal != 0) {
		return false, errNativePreflightTerminal
	}
	if s.ContainerID != "" && (s.Image == "" || s.ImageID != image) {
		return false, errNativePreflightUnavailable
	}
	if s.ContainerID == "" || s.State.Waiting != nil || s.State.Running != nil || s.State.Terminated == nil {
		return false, nil
	}
	if s.State.Terminated.ContainerID != s.ContainerID || pod.Spec.NodeName == "" {
		return false, errNativePreflightUnavailable
	}
	return true, nil
}
func (r *MigrationReconciler) nativePreflightExecutionUnchanged(ctx context.Context, m *v1beta1.Migration, image string, expected, observed *batchv1.Job, original *corev1.Pod, binding podexec.NativeContainerBinding) bool {
	if observed == nil || original == nil {
		return false
	}
	if _, err := r.nativePreflightMigration(ctx, m, image); err != nil {
		return false
	}
	job, err := r.nativePreflightLiveJob(ctx, expected, observed, binding)
	if err != nil || !apiequality.Semantic.DeepEqual(job.Status, observed.Status) {
		return false
	}
	pod, err := r.nativePreflightPod(ctx, job)
	return err == nil && pod != nil && nativePreflightPodMatches(job, pod) && nativePreflightSamePod(original, pod) && apiequality.Semantic.DeepEqual(pod.Spec, original.Spec) && apiequality.Semantic.DeepEqual(pod.Status, original.Status) && nativeEvidenceWorkerComplete(job)
}

func minDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func nativePreflightJobFailed(job *batchv1.Job) bool {
	if job.Status.Failed != 0 || job.Status.Succeeded > 1 {
		return true
	}
	for _, c := range job.Status.Conditions {
		if c.Status == corev1.ConditionTrue && (c.Type == batchv1.JobFailed || c.Type == batchv1.JobFailureTarget) {
			return true
		}
	}
	return false
}

func nativePreflightJobMatches(expected, actual *batchv1.Job) bool {
	if expected == nil || actual == nil || actual.UID == "" || !actual.DeletionTimestamp.IsZero() || actual.Name != expected.Name || actual.Namespace != expected.Namespace || actual.GenerateName != expected.GenerateName ||
		!apiequality.Semantic.DeepEqual(actual.OwnerReferences, expected.OwnerReferences) || !apiequality.Semantic.DeepEqual(actual.Labels, expected.Labels) || !apiequality.Semantic.DeepEqual(actual.Annotations, expected.Annotations) || !apiequality.Semantic.DeepEqual(actual.Finalizers, expected.Finalizers) {
		return false
	}
	want, got := expected.DeepCopy(), actual.DeepCopy()
	if !nativeWorkerControllerFields(got) || want.Spec.Template.Spec.RuntimeClassName != nil || got.Spec.Template.Spec.RuntimeClassName != nil {
		return false
	}
	nativeWorkerDefaults(want)
	nativeWorkerDefaults(got)
	if want.Spec.Template.Spec.HostUsers == nil {
		want.Spec.Template.Spec.HostUsers = ptr.To(true)
	}
	if got.Spec.Template.Spec.HostUsers == nil {
		got.Spec.Template.Spec.HostUsers = ptr.To(true)
	}
	return apiequality.Semantic.DeepEqual(want.Spec, got.Spec)
}

func (r *MigrationReconciler) nativePreflightPods(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	var all []corev1.Pod
	seen := map[string]bool{}
	opts := &client.ListOptions{Namespace: namespace, Limit: 100}
	snapshot := ""
	for {
		pods := &corev1.PodList{}
		if r.APIReader.List(ctx, pods, opts) != nil || ctx.Err() != nil || pods.ResourceVersion == "" || pods.ShardInfo != nil || snapshot != "" && snapshot != pods.ResourceVersion {
			return nil, errNativePreflightUnavailable
		}
		snapshot = pods.ResourceVersion
		all = append(all, pods.Items...)
		if pods.Continue == "" {
			return all, nil
		}
		if seen[pods.Continue] {
			return nil, errNativePreflightUnavailable
		}
		seen[pods.Continue] = true
		opts.Continue = pods.Continue
	}
}

func (r *MigrationReconciler) nativePreflightPod(ctx context.Context, job *batchv1.Job) (*corev1.Pod, error) {
	pods, err := r.nativePreflightPods(ctx, job.Namespace)
	if err != nil {
		return nil, err
	}
	var selected *corev1.Pod
	for i := range pods {
		for _, owner := range pods[i].OwnerReferences {
			if owner.UID == job.UID {
				if selected != nil {
					return nil, errNativePreflightUnavailable
				}
				selected = &pods[i]
				break
			}
		}
	}
	if selected == nil {
		return nil, nil
	}
	pod := &corev1.Pod{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(selected), pod) != nil || pod.UID == "" || pod.UID != selected.UID || pod.Name != selected.Name || pod.Namespace != job.Namespace {
		return nil, errNativePreflightUnavailable
	}
	return pod, nil
}

func nativePreflightPodMetadataMatches(job *batchv1.Job, pod *corev1.Pod) bool {
	if pod == nil || pod.UID == "" || !pod.DeletionTimestamp.IsZero() || len(validation.IsDNS1123Subdomain(pod.Name)) != 0 || pod.Namespace != job.Namespace ||
		pod.GenerateName != "" && pod.GenerateName != job.Name+"-" || !nativeEvidenceOwnedBy(pod, "Job", job.Name, job.UID, batchv1.SchemeGroupVersion.String()) ||
		len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].BlockOwnerDeletion == nil || !*pod.OwnerReferences[0].BlockOwnerDeletion ||
		!apiequality.Semantic.DeepEqual(pod.Annotations, job.Spec.Template.Annotations) || !slices.Equal(pod.Finalizers, []string(nil)) && !slices.Equal(pod.Finalizers, []string{batchv1.JobTrackingFinalizer}) {
		return false
	}
	labels := maps.Clone(job.Spec.Template.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	for key, value := range map[string]string{batchv1.ControllerUidLabel: string(job.UID), nativeWorkerLegacyControllerLabel: string(job.UID), batchv1.JobNameLabel: job.Name, nativeWorkerLegacyJobNameLabel: job.Name} {
		if _, exists := pod.Labels[key]; exists {
			labels[key] = value
		}
	}
	return maps.Equal(labels, pod.Labels)
}

func nativePreflightPodMatches(job *batchv1.Job, pod *corev1.Pod) bool {
	if !nativePreflightPodMetadataMatches(job, pod) {
		return false
	}
	wantJob := &batchv1.Job{Spec: batchv1.JobSpec{Template: *job.Spec.Template.DeepCopy()}}
	gotJob := &batchv1.Job{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: *pod.Spec.DeepCopy()}}}
	nativeWorkerDefaults(wantJob)
	nativeWorkerDefaults(gotJob)
	want, got := &wantJob.Spec.Template.Spec, &gotJob.Spec.Template.Spec
	if got.RuntimeClassName != nil || want.RuntimeClassName != nil || got.HostUsers != nil && !*got.HostUsers {
		return false
	}
	if want.HostUsers == nil {
		want.HostUsers = ptr.To(true)
	}
	if got.HostUsers == nil {
		got.HostUsers = ptr.To(true)
	}
	if want.NodeName == "" {
		if got.NodeName != "" && len(validation.IsDNS1123Subdomain(got.NodeName)) != 0 {
			return false
		}
		got.NodeName = ""
	}
	if want.ServiceAccountName == "" && got.ServiceAccountName == nativePreflightDefaultSA {
		got.ServiceAccountName = ""
	}
	if want.DeprecatedServiceAccount == "" && got.DeprecatedServiceAccount == nativePreflightDefaultSA {
		got.DeprecatedServiceAccount = ""
	}
	if want.EnableServiceLinks == nil {
		want.EnableServiceLinks = ptr.To(true)
		if got.EnableServiceLinks == nil {
			got.EnableServiceLinks = ptr.To(true)
		}
	}
	if want.Priority == nil {
		want.Priority = ptr.To(int32(0))
		if got.Priority == nil {
			got.Priority = ptr.To(int32(0))
		}
	}
	if want.PreemptionPolicy == nil {
		want.PreemptionPolicy = ptr.To(corev1.PreemptLowerPriority)
		if got.PreemptionPolicy == nil {
			got.PreemptionPolicy = ptr.To(corev1.PreemptLowerPriority)
		}
	}
	if !nativePreflightTolerations(want.Tolerations, got.Tolerations) {
		return false
	}
	got.Tolerations = want.Tolerations
	return apiequality.Semantic.DeepEqual(*want, *got)
}

func nativePreflightTolerations(want, got []corev1.Toleration) bool {
	if len(got) < len(want) || !apiequality.Semantic.DeepEqual(want, got[:len(want)]) {
		return false
	}
	seen := map[string]bool{}
	for _, t := range got[len(want):] {
		if t.Key != "node.kubernetes.io/not-ready" && t.Key != "node.kubernetes.io/unreachable" || t.Operator != corev1.TolerationOpExists || t.Value != "" || t.Effect != corev1.TaintEffectNoExecute || t.TolerationSeconds == nil || *t.TolerationSeconds != 300 || seen[t.Key] {
			return false
		}
		for _, existing := range want {
			if (existing.Key == t.Key || existing.Key == "" && existing.Operator == corev1.TolerationOpExists) && (existing.Effect == "" || existing.Effect == corev1.TaintEffectNoExecute) {
				return false
			}
		}
		seen[t.Key] = true
	}
	return true
}

func (r *MigrationReconciler) nativePreflightCleanup(ctx context.Context, m *v1beta1.Migration, created *batchv1.Job, pod *corev1.Pod) error {
	current := &batchv1.Job{}
	err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(created), current)
	if err == nil {
		if current.UID != created.UID || !nativeEvidenceOwnedBy(current, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
			return errNativePreflightUnavailable
		}
		if r.Delete(ctx, current, client.Preconditions{UID: &created.UID}, client.PropagationPolicy(metav1.DeletePropagationForeground)) != nil {
			return errNativePreflightUnavailable
		}
	} else if !apierrors.IsNotFound(err) {
		return errNativePreflightUnavailable
	}
	return wait.PollUntilContextCancel(ctx, nativePreflightPoll, true, func(ctx context.Context) (bool, error) {
		job := &batchv1.Job{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(created), job); err == nil {
			if job.UID != created.UID || !nativeEvidenceOwnedBy(job, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) {
				return false, errNativePreflightUnavailable
			}
			return false, nil
		} else if !apierrors.IsNotFound(err) {
			return false, errNativePreflightUnavailable
		}
		pods, err := r.nativePreflightPods(ctx, m.Namespace)
		if err != nil {
			return false, err
		}
		for i := range pods {
			if pod != nil && pods[i].Name == pod.Name && pods[i].UID != pod.UID {
				return false, errNativePreflightUnavailable
			}
			if pod != nil && pods[i].UID == pod.UID {
				return false, nil
			}
			for _, owner := range pods[i].OwnerReferences {
				if owner.UID == created.UID {
					return false, nil
				}
			}
		}
		return true, nil
	})
}
