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
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// sc is a StorageClass reduced to what ephemeralParams reads.
func sc(name, provisioner string, params map[string]string) storagev1.StorageClass {
	return storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: provisioner,
		Parameters:  params,
	}
}

// The suite's own class shipped without a dataEngine, which Longhorn reads as
// v1. On a cluster running v2 only, every fixture claim was denied and the
// pods sat unschedulable. It has to follow whatever the cluster's own Longhorn
// classes ask for.
func TestEphemeralParamsFollowsTheClusterDataEngine(t *testing.T) {
	base := map[string]string{paramReplicas: "1", "staleReplicaTimeout": "30"}
	with := func(engine string) map[string]string {
		p := maps.Clone(base)
		p[paramDataEngine] = engine
		return p
	}

	for _, tc := range []struct {
		name    string
		classes []storagev1.StorageClass
		want    map[string]string
	}{
		{
			name: "copies the engine a Longhorn class declares",
			classes: []storagev1.StorageClass{
				sc("longhorn", longhornProvisioner, map[string]string{paramDataEngine: "v2"}),
			},
			want: with("v2"),
		},
		{
			name: "copies v1 just as readily, so a v1 cluster still works",
			classes: []storagev1.StorageClass{
				sc("longhorn", longhornProvisioner, map[string]string{paramDataEngine: "v1"}),
			},
			want: with("v1"),
		},
		{
			name: "says nothing when no Longhorn class does either",
			classes: []storagev1.StorageClass{
				sc("longhorn", longhornProvisioner, map[string]string{paramReplicas: "3"}),
			},
			want: base,
		},
		{
			name: "ignores another provisioner's engine",
			classes: []storagev1.StorageClass{
				sc("local-path", "rancher.io/local-path", map[string]string{paramDataEngine: "v2"}),
			},
			want: base,
		},
		{
			name: "ignores its own stale copy, or a broken class would persist",
			classes: []storagev1.StorageClass{
				sc(ephemeralStorageClass, longhornProvisioner, map[string]string{paramDataEngine: "v1"}),
				sc("longhorn", longhornProvisioner, map[string]string{paramDataEngine: "v2"}),
			},
			want: with("v2"),
		},
		{
			name:    "no classes at all",
			classes: nil,
			want:    base,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ephemeralParams(tc.classes)
			if !maps.Equal(got, tc.want) {
				t.Errorf("ephemeralParams() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A kept fixture is reused when the seed marker matches, so the marker has to
// change when the requested shape does. Without the extra tables folded in, a
// run asking for a different spread would silently reuse the old one.
func TestSeedProfileTracksTheRequestedShape(t *testing.T) {
	defer func(n, mb int) { extraTables, extraSizeMB = n, mb }(extraTables, extraSizeMB)

	extraTables, extraSizeMB = 0, 0
	if got := seedProfile(); got != baseSeedProfile {
		t.Errorf("no extras: seedProfile() = %q, want %q", got, baseSeedProfile)
	}

	extraTables, extraSizeMB = 40, 8192
	first := seedProfile()
	if first == baseSeedProfile {
		t.Errorf("with extras: seedProfile() = %q, want it to differ from the base", first)
	}

	extraTables, extraSizeMB = 40, 4096
	if second := seedProfile(); second == first {
		t.Errorf("a different total gave the same profile %q, so a kept fixture would be reused", second)
	}

	extraTables, extraSizeMB = 20, 8192
	if third := seedProfile(); third == first {
		t.Errorf("a different table count gave the same profile %q", third)
	}
}

// The extra tables land on both volumes. addGi is what grows them, and it has
// to reject anything it cannot parse rather than silently under-provision.
// smallVolume is the size the 0.1 tier provisions, and the natural base case.
const smallVolume = "7Gi"

func TestAddGi(t *testing.T) {
	for _, tc := range []struct {
		size string
		gi   int
		want string
	}{
		{smallVolume, 0, smallVolume},
		{smallVolume, 16, "23Gi"},
		{"50Gi", 100, "150Gi"},
	} {
		if got := addGi(tc.size, tc.gi); got != tc.want {
			t.Errorf("addGi(%q, %d) = %q, want %q", tc.size, tc.gi, got, tc.want)
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("addGi accepted a size it cannot parse; it must panic rather than under-provision")
		}
	}()
	addGi("500Mi", 1)
}

func TestFixtureStorageReadiness(t *testing.T) {
	desired := resource.MustParse("50Gi")
	undersized := resource.MustParse(smallVolume)
	minimumFilesystem := desired.Value() * 95 / 100

	for _, tc := range []struct {
		name       string
		objects    func() []client.Object
		filesystem func(string) (int64, error)
		wantReady  bool
	}{
		{
			name:       "converged",
			objects:    func() []client.Object { return fixtureStorageObjects(desired, desired) },
			filesystem: func(string) (int64, error) { return minimumFilesystem, nil },
			wantReady:  true,
		},
		{
			name: "CNPG request absent",
			objects: func() []client.Object {
				objects := fixtureStorageObjects(desired, desired)
				unstructured.RemoveNestedField(objects[0].(*unstructured.Unstructured).Object,
					"spec", "storage", "size")
				return objects
			},
			filesystem: func(string) (int64, error) { return desired.Value(), nil },
		},
		{
			name:       "PVC absent",
			objects:    func() []client.Object { return fixtureStorageObjects(desired, desired)[:1] },
			filesystem: func(string) (int64, error) { return desired.Value(), nil },
		},
		{
			name: "PVC request absent",
			objects: func() []client.Object {
				objects := fixtureStorageObjects(desired, desired)
				objects[1].(*corev1.PersistentVolumeClaim).Spec.Resources.Requests = nil
				return objects
			},
			filesystem: func(string) (int64, error) { return desired.Value(), nil },
		},
		{
			name:       "PVC request short",
			objects:    func() []client.Object { return fixtureStorageObjects(undersized, desired) },
			filesystem: func(string) (int64, error) { return desired.Value(), nil },
		},
		{
			name: "PVC capacity absent",
			objects: func() []client.Object {
				objects := fixtureStorageObjects(desired, desired)
				objects[1].(*corev1.PersistentVolumeClaim).Status.Capacity = nil
				return objects
			},
			filesystem: func(string) (int64, error) { return desired.Value(), nil },
		},
		{
			name:       "PVC capacity short",
			objects:    func() []client.Object { return fixtureStorageObjects(desired, undersized) },
			filesystem: func(string) (int64, error) { return desired.Value(), nil },
		},
		{
			name:    "mounted filesystem short",
			objects: func() []client.Object { return fixtureStorageObjects(desired, desired) },
			filesystem: func(string) (int64, error) {
				return minimumFilesystem - 1, nil
			},
		},
		{
			name:       "mounted filesystem absent",
			objects:    func() []client.Object { return fixtureStorageObjects(desired, desired) },
			filesystem: func(string) (int64, error) { return 0, errors.New("filesystem unavailable") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withStorageTestState(t, storageTestClient(t, tc.objects()...), tc.filesystem)
			err := fixtureStorageReady(sourceCluster, desired.String())
			if tc.wantReady && err != nil {
				t.Fatalf("fixtureStorageReady() error = %v", err)
			}
			if !tc.wantReady && err == nil {
				t.Fatal("fixtureStorageReady() accepted storage that had not converged")
			}
		})
	}
}

func TestFilesystemCapacityFloorAccountsOnlyForMetadata(t *testing.T) {
	desired := resource.MustParse("50Gi")
	if got, want := filesystemCapacityFloor(desired.Value()), desired.Value()*95/100; got != want {
		t.Fatalf("filesystemCapacityFloor() = %d, want %d", got, want)
	}
}

func TestFixtureStorageWaitFailsClosed(t *testing.T) {
	desired := resource.MustParse("50Gi")
	withStorageTestState(t, storageTestClient(t, fixtureStorageObjects(desired, desired)[0]),
		func(string) (int64, error) { return desired.Value(), nil })
	storageReadyTimeout, storageReadyPollInterval = 30*time.Millisecond, time.Millisecond
	RegisterTestingT(t)

	started := time.Now()
	if err := InterceptGomegaFailure(func() { waitFixtureStorageReady(sourceCluster, desired.String()) }); err == nil {
		t.Fatal("storage wait accepted an absent PVC")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("storage wait exceeded its bounded timeout: %s", elapsed)
	}
}

func TestCreateWaitsForWorkVolumeReadiness(t *testing.T) {
	desired := resource.MustParse("12Gi")
	undersized := resource.MustParse("2Gi")

	for _, tc := range []struct {
		name      string
		objects   []client.Object
		wantError bool
	}{
		{name: "ready", objects: []client.Object{workPVC("migration", desired, desired)}},
		{name: "absent", wantError: true},
		{name: "capacity short", objects: []client.Object{workPVC("migration", desired, undersized)}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withStorageTestState(t, storageTestClient(t, tc.objects...), nil)
			storageReadyTimeout, storageReadyPollInterval = 30*time.Millisecond, time.Millisecond
			RegisterTestingT(t)
			m := &v1beta1.Migration{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: "migration"},
				Spec:       v1beta1.MigrationSpec{WorkVolume: v1beta1.WorkVolume{Size: desired}},
			}

			err := InterceptGomegaFailure(func() { create(m) })
			if tc.wantError && err == nil {
				t.Fatal("create() did not fail closed while its work volume was unready")
			}
			if !tc.wantError && err != nil {
				t.Fatalf("create() failed with a ready work volume: %v", err)
			}
		})
	}
}

func TestStaleSourceRebuildsBeforeStorageApply(t *testing.T) {
	desired := resource.MustParse("50Gi")
	objects := fixtureStorageObjects(desired, desired)
	target := cnpgCluster(targetCluster, smallVolume, 17)
	target.SetUID(types.UID("target-cluster-uid"))
	objects = append(objects, target)

	events := make([]string, 0, 3)
	applied := false
	clusterDeleted := false
	fresh := cnpgCluster(sourceCluster, desired.String(), 17)
	_ = unstructured.SetNestedField(fresh.Object, int64(1), "status", "readyInstances")
	freshPVC := fixturePVC(sourceCluster, desired, desired)
	freshPVC.SetUID(types.UID("fresh-source-pvc-uid"))
	c := storageTestClientWithInterceptors(t, interceptor.Funcs{
		Apply: func(
			ctx context.Context,
			c client.WithWatch,
			_ runtime.ApplyConfiguration,
			_ ...client.ApplyOption,
		) error {
			oldPVC := &corev1.PersistentVolumeClaim{}
			err := c.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: sourceCluster + "-1"}, oldPVC)
			if err == nil {
				events = append(events, "apply source while old PVC exists")
			} else {
				events = append(events, "apply source at 50Gi")
			}
			applied = true
			return nil
		},
		Delete: func(
			ctx context.Context,
			c client.WithWatch,
			obj client.Object,
			opts ...client.DeleteOption,
		) error {
			events = append(events, "delete "+obj.GetName())
			applied = false
			err := c.Delete(ctx, obj, opts...)
			clusterDeleted = true
			return err
		},
		Get: func(
			ctx context.Context,
			c client.WithWatch,
			key client.ObjectKey,
			obj client.Object,
			opts ...client.GetOption,
		) error {
			if applied && key.Namespace == nsE2E && key.Name == sourceCluster {
				fresh.DeepCopyInto(obj.(*unstructured.Unstructured))
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(
			ctx context.Context,
			c client.WithWatch,
			list client.ObjectList,
			opts ...client.ListOption,
		) error {
			pvcs := list.(*corev1.PersistentVolumeClaimList)
			if applied {
				pvcs.Items = []corev1.PersistentVolumeClaim{*freshPVC}
				return nil
			}
			if err := c.List(ctx, pvcs, opts...); err != nil {
				return err
			}
			if clusterDeleted {
				for i := range pvcs.Items {
					if err := c.Delete(ctx, &pvcs.Items[i]); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}, objects...)
	withStorageTestState(t, c, func(string) (int64, error) { return desired.Value(), nil })
	storageReadyTimeout, storageReadyPollInterval = 100*time.Millisecond, time.Millisecond
	oldSize := srcStorageSize
	t.Cleanup(func() { srcStorageSize = oldSize })
	srcStorageSize = desired.String()
	RegisterTestingT(t)

	if err := InterceptGomegaFailure(func() { prepareSourceCluster(true) }); err != nil {
		t.Fatalf("prepareSourceCluster() failed: %v", err)
	}
	if got, want := events, []string{"delete " + sourceCluster, "apply source at 50Gi"}; !slices.Equal(got, want) {
		t.Fatalf("stale source lifecycle = %v, want %v", got, want)
	}
	preserved := &unstructured.Unstructured{}
	preserved.SetGroupVersionKind(cnpgGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: nsE2E, Name: targetCluster}, preserved); err != nil {
		t.Fatalf("target cluster was not preserved: %v", err)
	}
	if preserved.GetUID() != types.UID("target-cluster-uid") {
		t.Fatalf("target cluster UID = %q, want preserved UID", preserved.GetUID())
	}
}

func TestTargetStorageExpansionPreservesObjects(t *testing.T) {
	desired := resource.MustParse("50Gi")
	for _, tc := range []struct {
		name      string
		current   resource.Quantity
		requested resource.Quantity
	}{
		{name: "expands 7Gi to 50Gi", current: resource.MustParse(smallVolume), requested: desired},
		{name: "retains 50Gi when 7Gi is requested", current: desired, requested: resource.MustParse(smallVolume)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			targetUID := types.UID("target-cluster-uid")
			pvcUID := types.UID("target-pvc-uid")
			target := cnpgCluster(targetCluster, tc.current.String(), 17)
			target.SetUID(targetUID)
			pvc := fixturePVC(targetCluster, tc.current, tc.current)
			pvc.SetUID(pvcUID)

			applied := false
			deletes := 0
			readyTarget := cnpgCluster(targetCluster, desired.String(), 17)
			readyTarget.SetUID(targetUID)
			_ = unstructured.SetNestedField(readyTarget.Object, int64(1), "status", "readyInstances")
			readyPVC := fixturePVC(targetCluster, desired, desired)
			readyPVC.SetUID(pvcUID)
			c := storageTestClientWithInterceptors(t, interceptor.Funcs{
				Apply: func(
					context.Context,
					client.WithWatch,
					runtime.ApplyConfiguration,
					...client.ApplyOption,
				) error {
					applied = true
					return nil
				},
				Delete: func(
					ctx context.Context,
					c client.WithWatch,
					obj client.Object,
					opts ...client.DeleteOption,
				) error {
					deletes++
					return c.Delete(ctx, obj, opts...)
				},
				Get: func(
					ctx context.Context,
					c client.WithWatch,
					key client.ObjectKey,
					obj client.Object,
					opts ...client.GetOption,
				) error {
					if applied && key.Namespace == nsE2E && key.Name == targetCluster {
						readyTarget.DeepCopyInto(obj.(*unstructured.Unstructured))
						return nil
					}
					return c.Get(ctx, key, obj, opts...)
				},
				List: func(
					ctx context.Context,
					c client.WithWatch,
					list client.ObjectList,
					opts ...client.ListOption,
				) error {
					if applied {
						list.(*corev1.PersistentVolumeClaimList).Items = []corev1.PersistentVolumeClaim{*readyPVC}
						return nil
					}
					return c.List(ctx, list, opts...)
				},
			}, target, pvc)
			withStorageTestState(t, c, func(string) (int64, error) { return desired.Value(), nil })
			storageReadyTimeout, storageReadyPollInterval = 100*time.Millisecond, time.Millisecond
			oldSize := tgtStorageSize
			t.Cleanup(func() { tgtStorageSize = oldSize })
			tgtStorageSize = tc.requested.String()
			RegisterTestingT(t)

			if err := InterceptGomegaFailure(prepareTargetCluster); err != nil {
				t.Fatalf("prepareTargetCluster() failed: %v", err)
			}
			if !applied {
				t.Fatal("target storage was not applied in place")
			}
			if deletes != 0 {
				t.Fatalf("target expansion deleted %d objects", deletes)
			}
			if readyTarget.GetUID() != targetUID || readyPVC.GetUID() != pvcUID {
				t.Fatal("target cluster or PVC identity changed during in-place expansion")
			}
		})
	}
}

func TestFeatureShapeMismatchFailsBeforeDelete(t *testing.T) {
	for _, name := range []string{sourceCluster, targetCluster} {
		t.Run(name, func(t *testing.T) {
			cluster := cnpgCluster(name, smallVolume, 17)
			uid := types.UID(name + "-uid")
			cluster.SetUID(uid)
			withStorageTestState(t, storageTestClient(t, cluster), nil)
			oldOwner := featureE2ERunValue
			t.Cleanup(func() { featureE2ERunValue = oldOwner })
			featureE2ERunValue = featureRunOwnerFixture
			RegisterTestingT(t)

			if err := InterceptGomegaFailure(func() { deleteMismatchedCluster(name) }); err == nil {
				t.Fatal("feature shape mismatch was allowed to delete a fixture cluster")
			}
			preserved := &unstructured.Unstructured{}
			preserved.SetGroupVersionKind(cnpgGVK)
			if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, preserved); err != nil {
				t.Fatalf("fixture cluster was deleted: %v", err)
			}
			if preserved.GetUID() != uid {
				t.Fatalf("fixture cluster UID = %q, want %q", preserved.GetUID(), uid)
			}
		})
	}

	t.Run("non-feature keeps existing rebuild behavior", func(t *testing.T) {
		cluster := cnpgCluster(sourceCluster, smallVolume, 17)
		withStorageTestState(t, storageTestClient(t, cluster), nil)
		oldOwner := featureE2ERunValue
		t.Cleanup(func() { featureE2ERunValue = oldOwner })
		featureE2ERunValue = ""
		RegisterTestingT(t)

		if err := InterceptGomegaFailure(func() { deleteMismatchedCluster(sourceCluster) }); err != nil {
			t.Fatalf("non-feature shape rebuild failed: %v", err)
		}
		remaining := &unstructured.Unstructured{}
		remaining.SetGroupVersionKind(cnpgGVK)
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: sourceCluster}, remaining); err == nil {
			t.Fatal("non-feature shape mismatch did not keep the existing rebuild behavior")
		}
	})
}

func fixtureStorageObjects(request, capacity resource.Quantity) []client.Object {
	cluster := cnpgCluster(sourceCluster, "50Gi", 17)
	cluster.SetUID(types.UID("source-cluster-uid"))
	return []client.Object{cluster, fixturePVC(sourceCluster, request, capacity)}
}

func fixturePVC(cluster string, request, capacity resource.Quantity) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: nsE2E,
			Name:      cluster + "-1",
			UID:       types.UID(cluster + "-pvc-uid"),
			Labels: map[string]string{
				labelCNPGCluster:  cluster,
				labelCNPGInstance: cluster + "-1",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: request},
		}},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase:    corev1.ClaimBound,
			Capacity: corev1.ResourceList{corev1.ResourceStorage: capacity},
		},
	}
	return pvc
}

func workPVC(migration string, request, capacity resource.Quantity) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: migration + "-work"},
		Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: request},
		}},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase:    corev1.ClaimBound,
			Capacity: corev1.ResourceList{corev1.ResourceStorage: capacity},
		},
	}
}

func storageTestClient(t *testing.T, objects ...client.Object) client.Client {
	return storageTestClientWithInterceptors(t, interceptor.Funcs{}, objects...)
}

func storageTestClientWithInterceptors(
	t *testing.T,
	interceptors interceptor.Funcs,
	objects ...client.Object,
) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).
		WithInterceptorFuncs(interceptors).Build()
}

func withStorageTestState(t *testing.T, c client.Client, filesystem func(string) (int64, error)) {
	t.Helper()
	oldCtx, oldClient := ctx, k8sClient
	oldFilesystem := mountedFilesystemCapacity
	oldTimeout, oldInterval := storageReadyTimeout, storageReadyPollInterval
	t.Cleanup(func() {
		ctx, k8sClient = oldCtx, oldClient
		mountedFilesystemCapacity = oldFilesystem
		storageReadyTimeout, storageReadyPollInterval = oldTimeout, oldInterval
	})
	ctx, k8sClient = context.Background(), c
	if filesystem != nil {
		mountedFilesystemCapacity = filesystem
	}
}
