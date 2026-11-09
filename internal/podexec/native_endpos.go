package podexec

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
)

// JobEndposNative captures acknowledgment data; an independent Reader must establish completion.
func (e *Exec) JobEndposNative(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error) {
	return e.nativeCompletedOutput(ctx, namespace, jobUID, want, false)
}
