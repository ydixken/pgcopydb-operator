package native

import (
	"encoding/hex"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestNativeStructuralConnectionReferenceParity(t *testing.T) {
	f := newAdmissionFixture(t)
	_, _, env, volumes, mounts, err := EvidenceMaterialization(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	structural, err := StructuralConnectionReferenceSHA256(env, volumes, mounts)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := ConnectionReferenceSHA256(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	// Captured from the evidence wrapper before extracting its projection.
	const baseline = "0a7f52f53f48626056478d70aadbd5ce6e82576e96762904a9102192e60966fe"
	if structural != evidence || hex.EncodeToString(structural[:]) != baseline {
		t.Fatal("structural extraction changed the existing evidence digest")
	}
}

func TestNativeStructuralConnectionReferencePrivacy(t *testing.T) {
	spec := referenceProjectionFixture()
	before := spec.DeepCopy()
	first, err := StructuralConnectionReferenceSHA256(spec.Containers[0].Env, spec.Volumes, spec.Containers[0].VolumeMounts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec, *before) {
		t.Fatal("structural hashing changed caller-owned connection inputs")
	}
	for i := range spec.Containers[0].Env {
		spec.Containers[0].Env[i].Value = "different-opaque-value\x00\xff"
	}
	second, err := StructuralConnectionReferenceSHA256(spec.Containers[0].Env, spec.Volumes, spec.Containers[0].VolumeMounts)
	if err != nil || first != second {
		t.Fatal("literal environment values changed the structural digest")
	}
}

func TestNativeStructuralConnectionReferenceChanges(t *testing.T) {
	spec := referenceProjectionFixture()
	original, err := StructuralConnectionReferenceSHA256(spec.Containers[0].Env, spec.Volumes, spec.Containers[0].VolumeMounts)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*corev1.PodSpec){
		"env_name":         func(s *corev1.PodSpec) { s.Containers[0].Env[0].Name = "OTHER" },
		"secret_name":      func(s *corev1.PodSpec) { s.Containers[0].Env[1].ValueFrom.SecretKeyRef.Name = evidenceOther },
		"secret_key":       func(s *corev1.PodSpec) { s.Containers[0].Env[1].ValueFrom.SecretKeyRef.Key = evidenceOther },
		"volume_reference": func(s *corev1.PodSpec) { s.Volumes[0].Secret.SecretName = evidenceOther },
		"volume_items":     func(s *corev1.PodSpec) { s.Volumes[0].Secret.Items[0].Key = evidenceOther },
		"mount_path":       func(s *corev1.PodSpec) { s.Containers[0].VolumeMounts[0].MountPath = "/other" },
		"mount_readonly":   func(s *corev1.PodSpec) { s.Containers[0].VolumeMounts[0].ReadOnly = false },
		"env_order": func(s *corev1.PodSpec) {
			s.Containers[0].Env[0], s.Containers[0].Env[1] = s.Containers[0].Env[1], s.Containers[0].Env[0]
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := spec.DeepCopy()
			mutate(changed)
			digest, err := StructuralConnectionReferenceSHA256(changed.Containers[0].Env, changed.Volumes, changed.Containers[0].VolumeMounts)
			if err != nil || digest == original {
				t.Fatal("changed connection structure did not change its digest")
			}
		})
	}
}

func TestNativeStructuralConnectionReferenceEmpty(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     []corev1.EnvVar
		volumes []corev1.Volume
		mounts  []corev1.VolumeMount
		want    string
	}{
		{name: "nil", want: "81749bda70c4265c886ca234dbbcc6629de45a2eb2af82d30ec8e395b1c54adf"},
		{name: fixtureEmptyCase, env: []corev1.EnvVar{}, volumes: []corev1.Volume{}, mounts: []corev1.VolumeMount{}, want: "3f3ecd7710fb59c9be85e26d3d5b6f73e60a1a14d09edf7170f5f3174382ba1b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			digest, err := StructuralConnectionReferenceSHA256(tc.env, tc.volumes, tc.mounts)
			if err != nil || hex.EncodeToString(digest[:]) != tc.want {
				t.Fatal("nil or empty structural serialization changed")
			}
		})
	}
}

func referenceProjectionFixture() corev1.PodSpec {
	const referenceVolume = "reference-volume"
	return corev1.PodSpec{
		Volumes: []corev1.Volume{{Name: referenceVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: evidenceSource, Items: []corev1.KeyToPath{{Key: admissionURIKey, Path: admissionURIKey}}}}}},
		Containers: []corev1.Container{{
			Env: []corev1.EnvVar{
				{Name: "PGCOPYDB_SOURCE_PGURI", Value: "opaque-literal-one"},
				{Name: "PGCOPYDB_TARGET_PGURI", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: policySecret("target", "uri")}},
			},
			VolumeMounts: []corev1.VolumeMount{{Name: referenceVolume, MountPath: "/connection", ReadOnly: true}},
		}},
	}
}
