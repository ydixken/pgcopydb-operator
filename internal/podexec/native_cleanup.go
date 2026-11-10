package podexec

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
)

// JobCleanupNative captures report data; the caller must validate it against fresh owned inventory.
func (e *Exec) JobCleanupNative(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error) {
	return e.nativeCompletedOutput(ctx, namespace, jobUID, want, false)
}
