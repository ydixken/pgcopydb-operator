package controller

import (
	"context"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/apimachinery/pkg/types"
)

// NativeJobReader supplies checked output bytes, never completion or cleanup authority.
type NativeJobReader interface {
	NativeComparisonReader
	JobCloneNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
	JobEndposNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
	JobCleanupNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
}
