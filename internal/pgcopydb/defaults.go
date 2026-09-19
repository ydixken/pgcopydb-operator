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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The operator's opinion about performance, applied when a Migration leaves a
// field unset. Without it the worker inherits pgcopydb's compiled-in defaults,
// and one of those is worth overriding: split-tables-larger-than defaults to
// 0, which disables same-table concurrency outright, so a single large table
// is copied by one process however many table jobs are running.
const (
	// defaultRunnerCPU matches pgcopydb's own default table-jobs, so a worker
	// never has fewer cores than the concurrency it was told to run.
	// A copy worker mid-COPY is not CPU bound
	// (see docs/research/measurements.md#a-copy-worker-mid-copy-is-not-cpu-bound).
	defaultRunnerCPU = "4"
	// defaultRunnerMemory covers pgcopydb's per-process buffers at that
	// concurrency. It is a request and not a limit, so a heavier run is
	// slowed by the node rather than killed.
	defaultRunnerMemory = "4Gi"

	// defaultSplitTablesLargerThan turns on same-table concurrency for tables
	// past this size. Absolute rather than relative on purpose: it is compared
	// against a user's table, not against anything this operator sizes.
	defaultSplitTablesLargerThan = "512Mi"
	// defaultSplitMaxParts caps the fan-out. Parts are table_size divided by
	// the threshold, so without a cap a 500GB table becomes a thousand parts
	// and a thousand catalog rows. Eight keeps the largest table busy without
	// crowding out every other table in the shared table-jobs pool.
	defaultSplitMaxParts = 8
)

// EffectiveRunnerResources returns the worker's resource requirements with the
// operator's defaults filled in. Only absent requests are supplied: a
// Migration that asks for anything at all is taken at its word, including
// asking for less.
func EffectiveRunnerResources(r corev1.ResourceRequirements) corev1.ResourceRequirements {
	out := *r.DeepCopy()
	if out.Requests == nil {
		out.Requests = corev1.ResourceList{}
	}
	if _, ok := out.Requests[corev1.ResourceCPU]; !ok {
		out.Requests[corev1.ResourceCPU] = resource.MustParse(defaultRunnerCPU)
	}
	if _, ok := out.Requests[corev1.ResourceMemory]; !ok {
		out.Requests[corev1.ResourceMemory] = resource.MustParse(defaultRunnerMemory)
	}
	return out
}

// tableJobsFor derives --table-jobs from the CPU the worker gets, so there is
// no second knob to keep in step. The floor of four is pgcopydb's own default:
// COPY waits on the network and on the servers, so even a smaller worker has
// something to overlap. Raising it past four buys nothing on one dominant table
// (see docs/research/measurements.md#table-jobs-past-four-buy-nothing-on-one-dominant-table).
func tableJobsFor(r corev1.ResourceRequirements) int32 {
	// Requests always carries a CPU entry: EffectiveRunnerResources supplies
	// one when the Migration did not, so there is no absent case to handle.
	cpu := EffectiveRunnerResources(r).Requests[corev1.ResourceCPU]
	// Round up: 1500m is closer to two useful COPY processes than to one.
	cores := int32((cpu.MilliValue() + 999) / 1000)
	if cores < 4 {
		return 4
	}
	return cores
}

// splitTablesLargerThan returns the size past which a table is copied by
// several processes. pgcopydb silently disables splitting when the source
// connection lands on a standby, so a Migration pointed at a read replica gets
// no same-table concurrency and no warning. A table without an integer key
// falls back to splitting by ctid, which follows physical layout rather than
// key order.
func splitTablesLargerThan(c *v1beta1.CloneOptions) resource.Quantity {
	if c.SplitTablesLargerThan != nil {
		return *c.SplitTablesLargerThan
	}
	return resource.MustParse(defaultSplitTablesLargerThan)
}

// splitMaxParts caps the fan-out of one table, defaulting alongside the
// threshold so the pair is never half-applied.
func splitMaxParts(c *v1beta1.CloneOptions) int32 {
	if c.SplitMaxParts > 0 {
		return c.SplitMaxParts
	}
	return defaultSplitMaxParts
}
