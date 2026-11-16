package controller

import (
	"fmt"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// setNativeBlocked sets Blocked=True; the warning event fires on the transition to True and on a
// reason change only, so a pass that repeats the hold writes and emits nothing new.
func (r *MigrationReconciler) setNativeBlocked(m *v1beta1.Migration, reason, msg string) {
	cur := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionBlocked)
	if r.Recorder != nil && (cur == nil || cur.Status != metav1.ConditionTrue || cur.Reason != reason) {
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, v1beta1.ConditionBlocked, "Hold", "%s", truncate(msg, maxDetailLen))
	}
	r.setCondition(m, v1beta1.ConditionBlocked, metav1.ConditionTrue, reason, msg)
}

// clearNativeBlocked turns a true Blocked False; a Migration that never blocked gains no condition.
func (r *MigrationReconciler) clearNativeBlocked(m *v1beta1.Migration) {
	if meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionBlocked) {
		r.setCondition(m, v1beta1.ConditionBlocked, metav1.ConditionFalse, v1beta1.ReasonProceeding, "The requested native action proceeds")
	}
}

// nativeCleanupFailedMessage names what a release without native cleanup may leave on the source.
// The prefixes are the engine's slot and publication names (rpgmigrate src/catalog/follow.rs resource_name).
func nativeCleanupFailedMessage(m *v1beta1.Migration, reason string) (string, error) {
	var cause string
	switch reason {
	case v1beta1.ReasonNamespaceTerminating:
		cause = "the namespace is terminating, so no cleanup pod can start"
	case v1beta1.ReasonWorkVolumeLost:
		cause = "the work PVC is missing, terminating or replaced, so cleanup cannot read its receipts"
	default:
		return "", fmt.Errorf("unknown CleanupFailed reason %q", reason)
	}
	run, err := nativeRunID(m)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Native cleanup did not run: %s. Drop replication slots rpgm_s_%s_* and publications rpgm_p_%s_* on source host %q database %q by hand.",
		cause, run, run, m.Spec.Source.Host, m.Spec.Source.Database), nil
}
