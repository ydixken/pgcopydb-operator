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
	"fmt"
	"path"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Kind's sealed default class provisions a root-owned 0777 hostPath directory, which the native
// engine and evidence programs refuse as replaceable, so a native kind Migration gets a
// hostPath volume that the runner uid owns at 0700 instead.
const (
	nativeWorkStorageClass = "e2e-native-work"
	nativeWorkSeedDir      = "work"
	nativeWorkSeedVolume   = "seed"
	nativeRunnerUID        = int64(65532)
)

func nativeKindWorkVolume() bool {
	return activeKindSuite != nil && activeKindSuite.input.Engine == string(v1beta1.MigrationEngineRPGMigrate)
}

// provisionNativeWorkVolume creates the runner-owned directory on a default-class seed claim and
// offers a hostPath volume at it to the Migration's work claim, returning the volume name.
// shortcut: the Released volume outlives the suite; deleting the owned kind cluster removes it.
func provisionNativeWorkVolume(m *v1beta1.Migration) string {
	GinkgoHelper()
	if err := k8sClient.Create(ctx, nativeWorkClass()); !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred(), "creating the native work StorageClass")
	}
	name := m.Name + "-work-seed"
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: m.Spec.WorkVolume.Size}},
		},
	}
	Expect(k8sClient.Create(ctx, claim)).To(Succeed(), "creating the native work seed claim %s", name)
	seed := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.Namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: ptr.To(false),
			Affinity: m.Spec.Runner.Affinity,
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true),
				RunAsUser: ptr.To(nativeRunnerUID), RunAsGroup: ptr.To(nativeRunnerUID)},
			Containers: []corev1.Container{{
				Name: nativeWorkSeedVolume, Image: selectedToolImage(), ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{"mkdir", "-m", "0700", "/seed/" + nativeWorkSeedDir},
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false),
					Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{otelDropAll}}},
				VolumeMounts: []corev1.VolumeMount{{Name: nativeWorkSeedVolume, MountPath: "/seed"}},
			}},
			Volumes: []corev1.Volume{{Name: nativeWorkSeedVolume, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}}}},
		},
	}
	Expect(k8sClient.Create(ctx, seed)).To(Succeed(), "creating the native work seed pod %s", name)
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(seed), seed)).To(Succeed())
		if seed.Status.Phase == corev1.PodFailed {
			StopTrying("native work seed pod " + name + " failed").Now()
		}
		g.Expect(seed.Status.Phase).To(Equal(corev1.PodSucceeded), "native work seed pod %s is %s", name, seed.Status.Phase)
	}, workerRunningTimeout, time.Second).Should(Succeed())
	Expect(k8sClient.Delete(ctx, seed)).To(Succeed(), "deleting the native work seed pod %s", name)
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(claim), claim)).To(Succeed())
	bound := &corev1.PersistentVolume{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: claim.Spec.VolumeName}, bound)).To(Succeed(),
		"reading the native work seed volume of %s", name)
	pv, err := nativeWorkPV(m, claim, bound)
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient.Create(ctx, pv)).To(Succeed(), "creating the native work volume %s", pv.Name)
	m.Spec.WorkVolume.StorageClassName = ptr.To(nativeWorkStorageClass)
	return pv.Name
}

// nativeWorkClass binds a claim when its worker is scheduled, like the default class: a claimRef
// binds at once, and a claim bound while the operator confirms its new worker Job refuses that attempt.
func nativeWorkClass() *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: nativeWorkStorageClass},
		Provisioner:       "kubernetes.io/no-provisioner",
		ReclaimPolicy:     ptr.To(corev1.PersistentVolumeReclaimRetain),
		VolumeBindingMode: ptr.To(storagev1.VolumeBindingWaitForFirstConsumer),
	}
}

// nativeWorkPV pins the work volume to the seed's node and directory. It carries no claimRef, so
// any claim of the class may bind it; create checks that the work claim got it.
func nativeWorkPV(m *v1beta1.Migration, claim *corev1.PersistentVolumeClaim,
	seed *corev1.PersistentVolume) (*corev1.PersistentVolume, error) {
	if seed.Spec.ClaimRef == nil || seed.Spec.ClaimRef.UID != claim.UID || claim.UID == "" {
		return nil, fmt.Errorf("volume %s is not bound to seed claim %s", seed.Name, claim.Name)
	}
	if seed.Spec.HostPath == nil || !path.IsAbs(seed.Spec.HostPath.Path) ||
		seed.Spec.NodeAffinity == nil || seed.Spec.NodeAffinity.Required == nil {
		return nil, fmt.Errorf("seed volume %s is not a node-pinned hostPath volume", seed.Name)
	}
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: m.Namespace + "-" + m.Name + "-work"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: m.Spec.WorkVolume.Size},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              nativeWorkStorageClass,
			NodeAffinity:                  seed.Spec.NodeAffinity.DeepCopy(),
			PersistentVolumeSource: corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: path.Join(seed.Spec.HostPath.Path, nativeWorkSeedDir), Type: ptr.To(corev1.HostPathDirectory)}},
		},
	}, nil
}
