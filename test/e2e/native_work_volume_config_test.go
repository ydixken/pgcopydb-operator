/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestKindNativeWorkVolumeSelection(t *testing.T) {
	old := activeKindSuite
	t.Cleanup(func() { activeKindSuite = old })
	for _, tc := range []struct {
		suite *kindSuite
		want  bool
	}{
		{nil, false},
		{&kindSuite{input: kindSuiteInput{Engine: string(v1beta1.MigrationEnginePGCopyDB)}}, false},
		{&kindSuite{input: kindSuiteInput{Engine: string(v1beta1.MigrationEngineRPGMigrate)}}, true},
	} {
		activeKindSuite = tc.suite
		if got := nativeKindWorkVolume(); got != tc.want {
			t.Errorf("nativeKindWorkVolume() with suite %+v = %t, want %t", tc.suite, got, tc.want)
		}
	}
}

func TestKindNativeWorkClass(t *testing.T) {
	class := nativeWorkClass()
	if class.Name != nativeWorkStorageClass || class.Provisioner != "kubernetes.io/no-provisioner" ||
		class.VolumeBindingMode == nil || *class.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer ||
		class.ReclaimPolicy == nil || *class.ReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("native work class = %+v, want a static WaitForFirstConsumer class that retains", class)
	}
}

func TestKindNativeWorkPV(t *testing.T) {
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: "e2e-follow-auto", Namespace: nsE2E},
		Spec: v1beta1.MigrationSpec{WorkVolume: v1beta1.WorkVolume{Size: resource.MustParse("2Gi")}}}
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-follow-auto-work-seed", UID: "seed-uid"}}
	affinity := &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
		MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn,
			Values: []string{"node-a"}}}}}}}
	seed := func() *corev1.PersistentVolume {
		return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pvc-seed"}, Spec: corev1.PersistentVolumeSpec{
			ClaimRef:     &corev1.ObjectReference{UID: "seed-uid"},
			NodeAffinity: affinity.DeepCopy(),
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/var/local-path-provisioner/pvc-seed"}},
		}}
	}
	pv, err := nativeWorkPV(m, claim, seed())
	if err != nil {
		t.Fatal(err)
	}
	spec := pv.Spec
	if spec.HostPath == nil || spec.HostPath.Path != "/var/local-path-provisioner/pvc-seed/work" ||
		spec.HostPath.Type == nil || *spec.HostPath.Type != corev1.HostPathDirectory {
		t.Errorf("work volume source = %+v, want the existing seed work directory", spec.HostPath)
	}
	if spec.ClaimRef != nil {
		t.Errorf("work volume claimRef = %+v, want none so binding waits for the worker", spec.ClaimRef)
	}
	if pv.Name != nsE2E+"-e2e-follow-auto-work" {
		t.Errorf("work volume name = %q", pv.Name)
	}
	if spec.StorageClassName != nativeWorkStorageClass ||
		spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain ||
		spec.Capacity.Storage().Cmp(resource.MustParse("2Gi")) != 0 || len(spec.AccessModes) != 1 ||
		spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Errorf("work volume shape = %+v", spec)
	}
	if spec.NodeAffinity == nil ||
		spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values[0] != "node-a" {
		t.Errorf("work volume node affinity = %+v, want the seed's node", spec.NodeAffinity)
	}
	for name, change := range map[string]func(*corev1.PersistentVolume){
		"unbound seed":      func(v *corev1.PersistentVolume) { v.Spec.ClaimRef = nil },
		"foreign claim":     func(v *corev1.PersistentVolume) { v.Spec.ClaimRef.UID = "foreign-uid" },
		"not hostPath":      func(v *corev1.PersistentVolume) { v.Spec.HostPath = nil },
		"relative path":     func(v *corev1.PersistentVolume) { v.Spec.HostPath.Path = "pvc-seed" },
		"no node affinity":  func(v *corev1.PersistentVolume) { v.Spec.NodeAffinity = nil },
		"no required nodes": func(v *corev1.PersistentVolume) { v.Spec.NodeAffinity.Required = nil },
	} {
		v := seed()
		change(v)
		if _, err := nativeWorkPV(m, claim, v); err == nil {
			t.Errorf("nativeWorkPV accepted a seed with %s", name)
		}
	}
	if _, err := nativeWorkPV(m, &corev1.PersistentVolumeClaim{}, &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{}}}); err == nil {
		t.Error("nativeWorkPV accepted a seed claim without a UID")
	}
}
