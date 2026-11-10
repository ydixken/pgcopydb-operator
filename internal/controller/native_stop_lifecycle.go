package controller

import (
	"context"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// The same invocation retains the original Pod/container through request and terminal observation.
func (r *MigrationReconciler) stopNativeRetained(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if r.NativeStop == nil {
		return false, errNativeRetainedStop
	}
	original, err := r.nativeStopCurrent(ctx, m, worker, pvc, nil)
	if err != nil {
		return false, errNativeRetainedStop
	}
	before, err := r.nativeStopCurrent(ctx, m, worker, pvc, &original)
	if err != nil {
		return false, errNativeRetainedStop
	}
	if !nativeStopTerminal(before) {
		request, end := context.WithTimeout(ctx, 30*time.Second)
		// The exec reply can disappear when PID1 exits; only live terminal observation supplies authority.
		_ = r.NativeStop.RequestNativeTerm(request, m.Namespace, original.pod.Name, original.binding)
		end()
	}
	err = wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		current, err := r.nativeStopCurrent(ctx, m, worker, pvc, &original)
		if err != nil {
			return false, errNativeRetainedStop
		}
		if !nativeStopTerminal(current) {
			return false, nil
		}
		confirmed, err := r.nativeStopCurrent(ctx, m, worker, pvc, &original)
		if err != nil || ctx.Err() != nil || !nativeStopTerminal(confirmed) {
			return false, errNativeRetainedStop
		}
		return true, nil
	})
	if err != nil {
		return false, errNativeRetainedStop
	}
	return true, nil
}
