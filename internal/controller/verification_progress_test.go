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

package controller

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The lines a compare Job logs under PGCOPYDB_LOG_JSON=on: a running sample
// with one table in flight, the final line, and a differing table between them.
const (
	runningProgressLine = `{"error_severity":"INFO","message":"compare progress: {\"plannedTables\":3,\"comparedTables\":2,` +
		`\"plannedBytes\":131072,\"comparedBytes\":24576,\"comparedRows\":20,\"differingTables\":0,\"failedTables\":0,` +
		`\"elapsedMs\":10059,\"final\":false,\"inFlightCount\":1,` +
		`\"inFlight\":[{\"schema\":\"public\",\"name\":\"a\",\"bytes\":106496,\"elapsedMs\":10000}]}"}`
	finalProgressLine = `{"error_severity":"INFO","message":"compare progress: {\"plannedTables\":3,\"comparedTables\":3,` +
		`\"plannedBytes\":131072,\"comparedBytes\":131072,\"comparedRows\":30,\"differingTables\":1,\"failedTables\":1,` +
		`\"elapsedMs\":20480,\"final\":true,\"inFlightCount\":0,\"inFlight\":[]}"}`
	tableDiffersLine = `{"error_severity":"ERROR","message":"Table \"public\".\"b\" has 10 rows on source, 9 rows on target"}`
)

// cloneCountersLine is the verify Job's copy-counter line, which the drain read shares.
const cloneCountersLine = verifyProgressPrefix + `{"tables":{"total":2,"done":2},"indexes":{"total":3,"done":3}}`

// progressItem matches one status.verificationProgress item.
func progressItem(kind, jobName string, final bool, compared int64) OmegaMatcher {
	return SatisfyAll(HaveField("Kind", kind), HaveField("JobName", jobName), HaveField("Final", final),
		HaveField("ComparedTables", compared), HaveField("ObservedAt", Not(BeNil())))
}

func TestReadCompareProgress(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	clock := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	m := streamingRehearsal()
	run1 := rehearsalJobName(m, 1)
	logs := &fakeLogs{byJob: map[string]string{run1: tableDiffersLine + "\n" + runningProgressLine + "\n" + tableDiffersLine}}
	r := &MigrationReconciler{Logs: logs, now: func() time.Time { return clock }}
	drain := v1beta1.VerificationProgress{Kind: v1beta1.VerificationProgressDrain, JobName: "m-verify", Final: true}
	m.Status.VerificationProgress = []v1beta1.VerificationProgress{drain}
	running := namedJob(run1)
	item := func() v1beta1.VerificationProgress {
		t.Helper()
		g.Expect(m.Status.VerificationProgress).To(HaveLen(2))
		g.Expect(m.Status.VerificationProgress[0]).To(Equal(drain), "another kind's item changed")
		return m.Status.VerificationProgress[1]
	}

	r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, running)
	g.Expect(logs.gotLines).To(Equal(int64(compareProgressLogTail)))
	g.Expect(item()).To(Equal(v1beta1.VerificationProgress{
		Kind: v1beta1.VerificationProgressRehearsal, JobName: run1, ObservedAt: &metav1.Time{Time: clock},
		PlannedTables: 3, ComparedTables: 2, ComparedRows: 20,
		PlannedBytes:  resource.NewQuantity(131072, resource.BinarySI),
		ComparedBytes: resource.NewQuantity(24576, resource.BinarySI),
		Elapsed:       &metav1.Duration{Duration: 10059 * time.Millisecond},
		InFlight: []v1beta1.VerificationTableProgress{{
			Schema: publicSchema, Name: "a", Bytes: resource.NewQuantity(106496, resource.BinarySI),
			Elapsed: &metav1.Duration{Duration: 10 * time.Second},
		}},
	}))
	sample := item()

	// No sample in the window, or no log at all: the previous sample stays, observedAt included.
	clock = clock.Add(time.Minute)
	logs.byJob[run1] = tableDiffersLine
	r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, running)
	g.Expect(item()).To(Equal(sample))
	logs.err = errBoom
	r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, running)
	g.Expect(item()).To(Equal(sample))
	logs.err = nil

	// Once the Job ended, the whole window decides, and the final line marks the item.
	finished := completeJob(run1)
	logs.byJob[run1] = runningProgressLine + "\n" + finalProgressLine
	r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, finished)
	g.Expect(logs.gotLines).To(Equal(int64(verifyLogTail)))
	g.Expect(item()).To(progressItem(v1beta1.VerificationProgressRehearsal, run1, true, 3))
	g.Expect(item().FailedTables).To(Equal(int64(1)))
	g.Expect(item().InFlight).To(BeEmpty())
	final := item()

	// A final sample of that Job is never read again.
	reads := logs.calls
	logs.byJob[run1] = runningProgressLine
	r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, finished)
	g.Expect(logs.calls).To(Equal(reads))
	g.Expect(item()).To(Equal(final))

	// The next run's first sample replaces the item, final or not.
	logs.byJob["m-rehearsal-2"] = runningProgressLine
	r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, namedJob("m-rehearsal-2"))
	g.Expect(item()).To(progressItem(v1beta1.VerificationProgressRehearsal, "m-rehearsal-2", false, 2))
}

// Without a log reader there is nothing to read, and no item appears.
func TestReadCompareProgress_NoReader(t *testing.T) {
	m := streamingRehearsal()
	(&MigrationReconciler{}).readCompareProgress(context.Background(), m, v1beta1.VerificationProgressRehearsal, namedJob(rehearsalJobName(m, 1)))
	if m.Status.VerificationProgress != nil {
		t.Fatalf("an item appeared without a log: %+v", m.Status.VerificationProgress)
	}
}

func TestReconcileRehearsal_RecordsProgress(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	m := streamingRehearsal()
	run1 := rehearsalJobName(m, 1)
	r := failingReconciler(t, interceptor.Funcs{}, m, rehearsalJobOf(m, 1))
	logs := &fakeLogs{byJob: map[string]string{run1: runningProgressLine}}
	r.Logs = logs

	g.Expect(r.reconcileRehearsal(ctx, m)).To(Succeed())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(v1beta1.VerificationProgressRehearsal, run1, false, 2)))

	r = failingReconciler(t, interceptor.Funcs{}, m, rehearsalJobOf(m, 1, jobCompleted))
	r.Logs = logs
	logs.byJob[run1] = runningProgressLine + "\n" + finalProgressLine
	g.Expect(r.reconcileRehearsal(ctx, m)).To(Succeed())
	g.Expect(m.Status.VerificationRehearsal.Outcome).To(Equal(v1beta1.RehearsalCompleted))
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(v1beta1.VerificationProgressRehearsal, run1, true, 3)))
}

// The drain verify's progress comes from the read that also takes the copy
// counters, so a finished verify Job's log is read once per pass.
func TestEnsureVerify_RecordsDrainProgress(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	m := followPasswordMigration()
	verify := verifyJobName(m)
	logs := &fakeLogs{byJob: map[string]string{verify: cloneCountersLine + "\n" + runningProgressLine}}
	r := failingReconciler(t, interceptor.Funcs{}, m, namedJob(verify))
	r.Logs = logs

	verified, refuted, err := r.ensureVerify(ctx, m)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(verified || refuted).To(BeFalse())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(v1beta1.VerificationProgressDrain, verify, false, 2)))
	g.Expect(m.Status.Progress).To(BeNil(), "the copy counters are read once the verify Job ended")

	r = failingReconciler(t, interceptor.Funcs{}, m, completeJob(verify))
	r.Logs = logs
	logs.byJob[verify] = cloneCountersLine + "\n" + runningProgressLine + "\n" + finalProgressLine
	reads := logs.calls
	verified, _, err = r.ensureVerify(ctx, m)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(verified).To(BeTrue())
	g.Expect(logs.calls - reads).To(Equal(1))
	g.Expect(logs.gotLines).To(Equal(int64(verifyLogTail)))
	g.Expect(m.Status.Progress).To(HaveField("TablesDone", int64(2)))
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(v1beta1.VerificationProgressDrain, verify, true, 3)))
	final := m.Status.VerificationProgress[0]

	// The next pass reads the copy counters again, as before, but keeps the final sample.
	r.now = func() time.Time { return final.ObservedAt.Add(time.Hour) }
	_, _, err = r.ensureVerify(ctx, m)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(final))
}

// On the exact-LSN fast path no compare runs, so no drain item appears.
func TestEnsureVerify_FastPathHasNoDrainProgress(t *testing.T) {
	m := followPasswordMigration()
	r := failingReconciler(t, interceptor.Funcs{}, m, completeJob(verifyJobName(m)))
	r.Logs = &fakeLogs{out: cloneCountersLine + "\ndrain verified: origin progress 0/100 equals endpos 0/100, nothing left to apply"}
	if _, _, err := r.ensureVerify(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if m.Status.VerificationProgress != nil {
		t.Fatalf("a drain verify that never compared left %+v", m.Status.VerificationProgress)
	}
}

// Only compare data logs progress; the schema check's log is never read for it.
func TestEnsureVerification_RecordsDataProgress(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	m := passwordMigration()
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
	schema, data := compareJobName(m, compareSchema), compareJobName(m, compareData)
	logs := &fakeLogs{out: runningProgressLine}
	r := failingReconciler(t, interceptor.Funcs{}, m, namedJob(schema))
	r.Logs = logs

	done, err := r.ensureVerification(ctx, m)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeFalse())
	g.Expect(logs.calls).To(BeZero(), "the schema check's log was read")
	g.Expect(m.Status.VerificationProgress).To(BeEmpty())

	r = failingReconciler(t, interceptor.Funcs{}, m, completeJob(schema), namedJob(data))
	r.Logs = logs
	done, err = r.ensureVerification(ctx, m)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeFalse())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(v1beta1.VerificationProgressData, data, false, 2)))

	r = failingReconciler(t, interceptor.Funcs{}, m, completeJob(schema), completeJob(data))
	r.Logs = logs
	logs.out = runningProgressLine + "\n" + finalProgressLine
	done, err = r.ensureVerification(ctx, m)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeTrue())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(v1beta1.VerificationProgressData, data, true, 3)))
}
