package controller

import (
	"context"
	"errors"
	"maps"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	nativeWorkerLegacyControllerLabel = "controller-uid"
	nativeWorkerLegacyJobNameLabel    = "job-name"
)

var (
	errNativeWorkerAdmission = errors.New("native worker identity or operation is not admitted")
	errNativeWorkerRetry     = errors.New("native worker retry is not qualified")
)

func nativeWorkerError(err error) error {
	if apierrors.IsConflict(err) {
		return apierrors.NewConflict(schema.GroupResource{Group: v1beta1.GroupVersion.Group, Resource: "migrations"}, "", errNativeWorkerAdmission)
	}
	return errNativeWorkerAdmission
}

func nativeWorkerMigrationMatches(live, expected *v1beta1.Migration) bool {
	return live != nil && expected != nil && live.UID != "" && live.UID == expected.UID &&
		live.Name == expected.Name && live.Namespace == expected.Namespace && live.ResourceVersion != "" &&
		live.Generation > 0 && live.Generation == expected.Generation && live.DeletionTimestamp.IsZero() &&
		live.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && !live.Spec.Suspend &&
		apiequality.Semantic.DeepEqual(live.Labels, expected.Labels) && apiequality.Semantic.DeepEqual(live.Annotations, expected.Annotations) &&
		apiequality.Semantic.DeepEqual(live.Spec, expected.Spec) && live.Status.Attempts == 0 &&
		live.Status.Attempts == expected.Status.Attempts && live.Status.JobName == "" && live.Status.JobName == expected.Status.JobName &&
		live.Status.Phase != v1beta1.PhaseCompleted && live.Status.Phase != v1beta1.PhaseFailed && live.Status.Phase != v1beta1.PhaseSuspended &&
		!meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionComplete) &&
		!meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionFailed)
}

func (r *MigrationReconciler) nativeWorkerCurrent(ctx context.Context, expected *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) (*v1beta1.Migration, *corev1.PersistentVolumeClaim, error) {
	live := &v1beta1.Migration{}
	if r.APIReader.Get(ctx, types.NamespacedName{Namespace: expected.Namespace, Name: expected.Name}, live) != nil || !nativeWorkerMigrationMatches(live, expected) {
		return nil, nil, errNativeWorkerAdmission
	}
	claim := &corev1.PersistentVolumeClaim{}
	if r.APIReader.Get(ctx, types.NamespacedName{Namespace: live.Namespace, Name: workPVCName(live)}, claim) != nil {
		return nil, nil, errNativeWorkerAdmission
	}
	if _, err := nativeWorkIdentity(live, claim); err != nil {
		return nil, nil, errNativeWorkerAdmission
	}
	if pvc != nil && (claim.UID != pvc.UID || !apiequality.Semantic.DeepEqual(claim.Spec, pvc.Spec) || !apiequality.Semantic.DeepEqual(claim.OwnerReferences, pvc.OwnerReferences)) {
		return nil, nil, errNativeWorkerAdmission
	}
	return live, claim, nil
}

func (r *MigrationReconciler) startNativeAttempt(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	if m != nil && m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && m.Status.Attempts > 0 {
		return ctrl.Result{}, errNativeWorkerRetry
	}
	if r.Client == nil || r.APIReader == nil || r.Scheme == nil || r.Recorder == nil || ctx.Err() != nil ||
		!nativeWorkerMigrationMatches(m, base) {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	live, pvc, err := r.nativeWorkerCurrent(ctx, m, nil)
	if err != nil {
		return ctrl.Result{}, err
	}
	expected, err := buildNativeWorkerJob(live, pvc, r.RPGMigrateRunnerImage, 1)
	if err != nil || controllerutil.SetControllerReference(live, expected, r.Scheme) != nil {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	key := types.NamespacedName{Namespace: live.Namespace, Name: expected.Name}
	actual := &batchv1.Job{}
	getErr := r.APIReader.Get(ctx, key, actual)
	created := false
	switch {
	case getErr == nil:
	case apierrors.IsNotFound(getErr):
		if _, _, err := r.nativeWorkerCurrent(ctx, live, pvc); err != nil || ctx.Err() != nil {
			return ctrl.Result{}, errNativeWorkerAdmission
		}
		actual = expected.DeepCopy()
		createErr := r.Create(ctx, actual)
		switch {
		case createErr == nil:
			created = true
		case apierrors.IsAlreadyExists(createErr):
			actual = &batchv1.Job{}
			if r.APIReader.Get(ctx, key, actual) != nil {
				return ctrl.Result{}, errNativeWorkerAdmission
			}
		default:
			// An unknown Create outcome supplies no permission to replay or adopt.
			return ctrl.Result{}, errNativeWorkerAdmission
		}
	default:
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	if !nativeWorkerJobMatches(live, pvc, expected, actual) {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	current, _, err := r.nativeWorkerCurrent(ctx, live, pvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	confirmed := &batchv1.Job{}
	if r.APIReader.Get(ctx, key, confirmed) != nil || confirmed.UID != actual.UID ||
		!nativeWorkerJobMatches(current, pvc, expected, confirmed) || ctx.Err() != nil {
		return ctrl.Result{}, errNativeWorkerAdmission
	}
	return r.recordAttempt(ctx, current, current.DeepCopy(), confirmed, created)
}

func nativeWorkerJobMatches(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, expected, actual *batchv1.Job) bool {
	if m == nil || expected == nil || actual == nil || actual.UID == "" || !actual.DeletionTimestamp.IsZero() ||
		actual.Name != jobName(m, 1) || actual.Name != expected.Name || actual.Namespace != m.Namespace || actual.Namespace != expected.Namespace ||
		expected.UID != "" && expected.UID != actual.UID ||
		!nativeEvidenceOwnedBy(actual, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) ||
		actual.GenerateName != expected.GenerateName || !apiequality.Semantic.DeepEqual(actual.OwnerReferences, expected.OwnerReferences) ||
		!apiequality.Semantic.DeepEqual(actual.Labels, expected.Labels) || !apiequality.Semantic.DeepEqual(actual.Annotations, expected.Annotations) ||
		!apiequality.Semantic.DeepEqual(actual.Finalizers, expected.Finalizers) {
		return false
	}
	if _, err := nativeWorkIdentity(m, pvc); err != nil {
		return false
	}
	want, got := expected.DeepCopy(), actual.DeepCopy()
	if !nativeWorkerControllerFields(got) {
		return false
	}
	nativeWorkerDefaults(want)
	nativeWorkerDefaults(got)
	return apiequality.Semantic.DeepEqual(want.Spec, got.Spec)
}

func nativeWorkerControllerFields(job *batchv1.Job) bool {
	if selector := job.Spec.Selector; selector != nil {
		if len(selector.MatchExpressions) != 0 || !maps.Equal(selector.MatchLabels, map[string]string{batchv1.ControllerUidLabel: string(job.UID)}) {
			return false
		}
		job.Spec.Selector = nil
	}
	for key, value := range map[string]string{
		batchv1.ControllerUidLabel: string(job.UID), nativeWorkerLegacyControllerLabel: string(job.UID),
		batchv1.JobNameLabel: job.Name, nativeWorkerLegacyJobNameLabel: job.Name,
	} {
		if actual, ok := job.Spec.Template.Labels[key]; ok {
			if actual != value {
				return false
			}
			delete(job.Spec.Template.Labels, key)
		}
	}
	return true
}

func nativeWorkerDefaults(job *batchv1.Job) {
	// Only API defaults that preserve this pinned single-worker execution are equivalent.
	if job.Spec.Parallelism == nil {
		job.Spec.Parallelism = ptr.To(int32(1))
	}
	if job.Spec.Completions == nil {
		job.Spec.Completions = ptr.To(int32(1))
	}
	if job.Spec.CompletionMode == nil {
		job.Spec.CompletionMode = ptr.To(batchv1.NonIndexedCompletion)
	}
	if job.Spec.Suspend == nil {
		job.Spec.Suspend = ptr.To(false)
	}
	if job.Spec.ManualSelector == nil {
		job.Spec.ManualSelector = ptr.To(false)
	}
	if job.Spec.PodReplacementPolicy == nil {
		job.Spec.PodReplacementPolicy = ptr.To(batchv1.TerminatingOrFailed)
	}
	pod := &job.Spec.Template.Spec
	if pod.TerminationGracePeriodSeconds == nil {
		pod.TerminationGracePeriodSeconds = ptr.To(int64(30))
	}
	if pod.SchedulerName == "" {
		pod.SchedulerName = corev1.DefaultSchedulerName
	}
	for i := range pod.Containers {
		container := &pod.Containers[i]
		if container.ImagePullPolicy == "" {
			container.ImagePullPolicy = corev1.PullIfNotPresent
		}
		if container.TerminationMessagePath == "" {
			container.TerminationMessagePath = corev1.TerminationMessagePathDefault
		}
		if container.TerminationMessagePolicy == "" {
			container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
	}
}
