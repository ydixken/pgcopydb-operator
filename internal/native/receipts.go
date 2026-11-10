package native

import (
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"k8s.io/apimachinery/pkg/types"
)

// NativeReceiptsMatch binds caller expectations to recorded UIDs, never completion authority.
func NativeReceiptsMatch(m *v1beta1.Migration, workerJobUID, pvcUID types.UID) bool {
	return m != nil && m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate &&
		retryUID(workerJobUID) && retryUID(pvcUID) &&
		m.Status.NativeJobUID == string(workerJobUID) && m.Status.NativePVCUID == string(pvcUID)
}
