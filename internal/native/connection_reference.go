package native

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"

	corev1 "k8s.io/api/core/v1"
)

// StructuralConnectionReferenceSHA256 excludes literal env values from structural correlation.
// Canonical admission compares full values independently; this digest grants no authority.
func StructuralConnectionReferenceSHA256(env []corev1.EnvVar, volumes []corev1.Volume, mounts []corev1.VolumeMount) ([32]byte, error) {
	env = slices.Clone(env)
	for i := range env {
		env[i].Value = ""
	}
	structural := struct {
		Env     []corev1.EnvVar
		Volumes []corev1.Volume
		Mounts  []corev1.VolumeMount
	}{env, volumes, mounts}
	raw, err := json.Marshal(structural)
	if err != nil {
		return [32]byte{}, errors.New("native connection references invalid")
	}
	return sha256.Sum256(raw), nil
}
