package podexec

import (
	"bytes"
	"context"

	"k8s.io/apimachinery/pkg/types"
)

// ComparisonOutput contains checked wrapper bytes, never a comparison verdict.
type ComparisonOutput struct {
	payload []byte
	checked bool
}

func (o ComparisonOutput) Payload() []byte {
	if !o.checked {
		return nil
	}
	return bytes.Clone(o.payload)
}

// JobComparisonNative requires the original wrapper's successful termination and complete closed output.
func (e *Exec) JobComparisonNative(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (ComparisonOutput, error) {
	out, err := e.nativeCompletedOutput(ctx, namespace, jobUID, want, false)
	if err != nil || !out.successful || len(out.payload) == 0 {
		return ComparisonOutput{}, errNativeTransport
	}
	return ComparisonOutput{payload: bytes.Clone(out.payload), checked: true}, nil
}
