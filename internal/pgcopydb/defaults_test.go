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

package pgcopydb

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func requests(cpu, mem string) corev1.ResourceRequirements {
	l := corev1.ResourceList{}
	if cpu != "" {
		l[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	if mem != "" {
		l[corev1.ResourceMemory] = resource.MustParse(mem)
	}
	return corev1.ResourceRequirements{Requests: l}
}

func TestEffectiveRunnerResources(t *testing.T) {
	for _, tc := range []struct {
		name          string
		in            corev1.ResourceRequirements
		wantCPU       string
		wantMemory    string
		wantUntouched bool
	}{
		{name: "empty gets both defaults", in: corev1.ResourceRequirements{}, wantCPU: "4", wantMemory: "4Gi"},
		{name: "explicit cpu is honoured, even when smaller", in: requests("500m", ""), wantCPU: "500m", wantMemory: "4Gi"},
		{name: "explicit memory is honoured", in: requests("", "16Gi"), wantCPU: "4", wantMemory: "16Gi"},
		{name: "both explicit are left alone", in: requests("8", "32Gi"), wantCPU: "8", wantMemory: "32Gi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := EffectiveRunnerResources(tc.in)
			if cpu := got.Requests[corev1.ResourceCPU]; cpu.String() != tc.wantCPU {
				t.Errorf("cpu = %s, want %s", cpu.String(), tc.wantCPU)
			}
			if mem := got.Requests[corev1.ResourceMemory]; mem.String() != tc.wantMemory {
				t.Errorf("memory = %s, want %s", mem.String(), tc.wantMemory)
			}
		})
	}
}

// The caller's object must not change: buildJob renders the Job from the same
// spec the reconciler holds, and a mutated request would be written back to
// the Migration on the next status update.
func TestEffectiveRunnerResourcesDoesNotMutateInput(t *testing.T) {
	in := corev1.ResourceRequirements{}
	_ = EffectiveRunnerResources(in)
	if in.Requests != nil {
		t.Fatalf("input was mutated: Requests = %v", in.Requests)
	}
	explicit := requests("2", "2Gi")
	_ = EffectiveRunnerResources(explicit)
	if len(explicit.Requests) != 2 {
		t.Fatalf("input was mutated: Requests = %v", explicit.Requests)
	}
}

func TestTableJobsFor(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   corev1.ResourceRequirements
		want int32
	}{
		{name: "unset follows the default worker", in: corev1.ResourceRequirements{}, want: 4},
		// Never below four: COPY waits on the network and the servers, so even
		// a small worker has something to overlap.
		{name: "half a core still gets the floor", in: requests("500m", ""), want: 4},
		{name: "two cores still gets the floor", in: requests("2", ""), want: 4},
		{name: "eight cores scale up", in: requests("8", ""), want: 8},
		// Round up: 1500m is closer to two useful COPY processes than one.
		{name: "fractional cores round up", in: requests("6500m", ""), want: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tableJobsFor(tc.in); got != tc.want {
				t.Errorf("tableJobsFor = %d, want %d", got, tc.want)
			}
		})
	}
}

// useCopyBinary is defaulted by the API server, not here, because false is a
// meaningful value rather than an absence. The pointer is what makes the three
// states distinguishable, and this pins all three: nil is what a CRD without
// the default produces and must not silently turn binary COPY on, and an
// explicit false must reach pgcopydb as text rather than being defaulted away.
func TestUseCopyBinaryIsForwardedBothWays(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name string
		set  *bool
		want bool
	}{
		{name: "true renders the flag", set: &yes, want: true},
		{name: "false renders nothing", set: &no, want: false},
		{name: "nil renders nothing", set: nil, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &v1beta1.MigrationSpec{}
			spec.Clone.UseCopyBinary = tc.set
			var found bool
			for _, a := range CloneArgs(spec, false, false, false) {
				if a == "--use-copy-binary" {
					found = true
				}
			}
			if found != tc.want {
				t.Errorf("--use-copy-binary present = %v, want %v", found, tc.want)
			}
		})
	}
}

func TestSplitDefaults(t *testing.T) {
	empty := &v1beta1.CloneOptions{}
	if got := splitTablesLargerThan(empty); got.Value() != 512<<20 {
		t.Errorf("default threshold = %d bytes, want %d", got.Value(), 512<<20)
	}
	if got := splitMaxParts(empty); got != 8 {
		t.Errorf("default max parts = %d, want 8", got)
	}

	q := resource.MustParse("2Gi")
	set := &v1beta1.CloneOptions{SplitTablesLargerThan: &q, SplitMaxParts: 3}
	if got := splitTablesLargerThan(set); got.Value() != 2<<30 {
		t.Errorf("explicit threshold = %d bytes, want %d", got.Value(), 2<<30)
	}
	if got := splitMaxParts(set); got != 3 {
		t.Errorf("explicit max parts = %d, want 3", got)
	}
}

// Raising the worker's CPU is the only knob a user needs for copy concurrency:
// the rendered args must follow it without a second field being set.
func TestCloneArgsFollowsRunnerCPU(t *testing.T) {
	spec := &v1beta1.MigrationSpec{}
	spec.Runner.Resources = requests("8", "")
	got := CloneArgs(spec, false, false, false)

	var found string
	for i, a := range got {
		if a == "--table-jobs" && i+1 < len(got) {
			found = got[i+1]
		}
	}
	if found != "8" {
		t.Errorf("--table-jobs = %q, want 8 for an 8-core worker; args: %v", found, got)
	}
}
