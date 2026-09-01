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
	"maps"
	"testing"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
