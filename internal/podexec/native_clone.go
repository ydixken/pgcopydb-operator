package podexec

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
)

// JobCloneNative captures complete original Job logs; the caller must validate their clone profile.
func (e *Exec) JobCloneNative(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error) {
	return e.nativeCompletedOutput(ctx, namespace, jobUID, want, false)
}
