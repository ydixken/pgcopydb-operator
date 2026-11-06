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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
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

	g.Expect(r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, running)).Error().NotTo(HaveOccurred())
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
	g.Expect(r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, running)).Error().NotTo(HaveOccurred())
	g.Expect(item()).To(Equal(sample))
	logs.err = errBoom
	g.Expect(r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, running)).Error().NotTo(HaveOccurred())
	g.Expect(item()).To(Equal(sample))
	logs.err = nil

	// Once the Job ended, the whole window decides, and the final line marks the item.
	finished := completeJob(run1)
	logs.byJob[run1] = runningProgressLine + "\n" + finalProgressLine
	g.Expect(r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, finished)).Error().NotTo(HaveOccurred())
	g.Expect(logs.gotLines).To(Equal(int64(verifyLogTail)))
	g.Expect(item()).To(progressItem(v1beta1.VerificationProgressRehearsal, run1, true, 3))
	g.Expect(item().FailedTables).To(Equal(int64(1)))
	g.Expect(item().InFlight).To(BeEmpty())
	final := item()

	// A final sample of that Job is never read again.
	reads := logs.calls
	logs.byJob[run1] = runningProgressLine
	g.Expect(r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, finished)).Error().NotTo(HaveOccurred())
	g.Expect(logs.calls).To(Equal(reads))
	g.Expect(item()).To(Equal(final))

	// The next run's first sample replaces the item, final or not.
	logs.byJob["m-rehearsal-2"] = runningProgressLine
	g.Expect(r.readCompareProgress(ctx, m, v1beta1.VerificationProgressRehearsal, namedJob("m-rehearsal-2"))).Error().NotTo(HaveOccurred())
	g.Expect(item()).To(progressItem(v1beta1.VerificationProgressRehearsal, "m-rehearsal-2", false, 2))
}

// The data and verify Jobs retry a failed pod and can be recreated under their name,
// so a final sample read before the Job finished does not end the reads.
func TestReadCompareProgress_SameJobNameRunsAgain(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	clock := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	m := passwordMigration()
	data, kind := compareJobName(m, compareData), v1beta1.VerificationProgressData
	logs := &fakeLogs{out: runningProgressLine + "\n" + finalProgressLine}
	r := &MigrationReconciler{Logs: logs, now: func() time.Time { return clock }}

	// The first pod's final line, read while the Job waits to retry the pod.
	g.Expect(r.readCompareProgress(ctx, m, kind, namedJob(data))).Error().NotTo(HaveOccurred())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(kind, data, true, 3)))

	clock = clock.Add(time.Minute)
	logs.out = runningProgressLine
	g.Expect(r.readCompareProgress(ctx, m, kind, namedJob(data))).Error().NotTo(HaveOccurred())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(kind, data, false, 2)), "the retry pod's sample was not read")

	clock = clock.Add(time.Minute)
	finished := completedAt(data, clock.Add(-time.Second))
	logs.out = finalProgressLine
	g.Expect(r.readCompareProgress(ctx, m, kind, finished)).Error().NotTo(HaveOccurred())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(kind, data, true, 3)))
	reads := logs.calls
	g.Expect(r.readCompareProgress(ctx, m, kind, finished)).Error().NotTo(HaveOccurred())
	g.Expect(logs.calls).To(Equal(reads), "a final sample read after the Job finished was read again")

	clock = clock.Add(time.Hour)
	g.Expect(r.readCompareProgress(ctx, m, kind, completedAt(data, clock.Add(-time.Second)))).Error().NotTo(HaveOccurred())
	g.Expect(logs.calls).To(Equal(reads+1), "a Job recreated under the name was not read")
	g.Expect(m.Status.VerificationProgress[0].ObservedAt.Time).To(Equal(clock))
}

// A failed read of a finished Job's log is an error while failedLogGrace lasts, so the
// caller can wait for the last sample; after that the read gives up.
func TestReadCompareProgress_FinishedReadFails(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	clock := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	m := passwordMigration()
	data, kind := compareJobName(m, compareData), v1beta1.VerificationProgressData
	finished := completedAt(data, clock)
	logs := &fakeLogs{err: errBoom}
	r := &MigrationReconciler{Logs: logs, now: func() time.Time { return clock }}

	g.Expect(r.readCompareProgress(ctx, m, kind, finished)).Error().To(MatchError(errBoom))
	g.Expect(m.Status.VerificationProgress).To(BeEmpty())

	logs.err, logs.out = nil, finalProgressLine
	g.Expect(r.readCompareProgress(ctx, m, kind, finished)).To(Equal(finalProgressLine))
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(kind, data, true, 3)))

	m.Status.VerificationProgress = nil
	clock = clock.Add(failedLogGrace)
	logs.err = errBoom
	g.Expect(r.readCompareProgress(ctx, m, kind, finished)).To(BeEmpty())
	g.Expect(m.Status.VerificationProgress).To(BeEmpty())
}

// A runner from spec.runner may list more tables in flight than the bundled one's cap,
// and status and the per-table series keep only the oldest 64.
func TestRecordCompareProgress_CapsInFlight(t *testing.T) {
	sample := pgcopydb.CompareProgress{PlannedTables: compareProgressMaxInFlight + 1}
	for i := range compareProgressMaxInFlight + 1 {
		sample.InFlight = append(sample.InFlight, pgcopydb.CompareInFlightTable{Schema: publicSchema, Name: fmt.Sprint("t", i)})
	}
	raw, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	m := passwordMigration()
	(&MigrationReconciler{}).recordCompareProgress(m, v1beta1.VerificationProgressData,
		namedJob(compareJobName(m, compareData)), pgcopydb.CompareProgressPrefix+string(raw))
	g := NewWithT(t)
	g.Expect(m.Status.VerificationProgress).To(HaveLen(1))
	inFlight := m.Status.VerificationProgress[0].InFlight
	g.Expect(inFlight).To(HaveLen(compareProgressMaxInFlight))
	g.Expect(inFlight[compareProgressMaxInFlight-1].Name).To(Equal(fmt.Sprint("t", compareProgressMaxInFlight-1)))
}

// completedAt is completeJob with its completion time at t.
func completedAt(name string, t time.Time) *batchv1.Job {
	job := completeJob(name)
	job.Status.CompletionTime = &metav1.Time{Time: t}
	return job
}

// Without a log reader there is nothing to read, and no item appears.
func TestReadCompareProgress_NoReader(t *testing.T) {
	m := streamingRehearsal()
	tail, err := (&MigrationReconciler{}).readCompareProgress(context.Background(), m, v1beta1.VerificationProgressRehearsal,
		completeJob(rehearsalJobName(m, 1)))
	if tail != "" || err != nil {
		t.Fatalf("read without a reader: %q, %v", tail, err)
	}
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

// The outcome of a finished run waits while a failed read of its log is retried, so
// Completed comes with the run's last sample.
func TestReconcileRehearsal_OutcomeWaitsForTheLog(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	clock := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	m := streamingRehearsal()
	job := rehearsalJobOf(m, 1, jobCompleted)
	job.Status.CompletionTime = &metav1.Time{Time: clock}
	r := failingReconciler(t, interceptor.Funcs{}, m, job)
	logs := &fakeLogs{err: errBoom}
	r.Logs, r.now = logs, func() time.Time { return clock }

	g.Expect(r.reconcileRehearsal(ctx, m)).To(MatchError(errBoom))
	g.Expect(m.Status.VerificationRehearsal.Outcome).To(Equal(v1beta1.RehearsalRunning))

	logs.err, logs.out = nil, runningProgressLine+"\n"+finalProgressLine
	g.Expect(r.reconcileRehearsal(ctx, m)).To(Succeed())
	g.Expect(m.Status.VerificationRehearsal.Outcome).To(Equal(v1beta1.RehearsalCompleted))
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(
		progressItem(v1beta1.VerificationProgressRehearsal, job.Name, true, 3)))
}

// One read of a failed run's log gives both its last sample and its cause, and the
// cause comes from the end only: further up, an ERROR may be a table that differed.
func TestReconcileRehearsal_FailedRunReadsTheLogOnce(t *testing.T) {
	g := NewWithT(t)
	m := streamingRehearsal()
	job := rehearsalJobOf(m, 1, batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue})
	r := failingReconciler(t, interceptor.Funcs{}, m, job)
	filler := strings.Repeat(`{"error_severity":"INFO","message":"waiting"}`+"\n", workerLogTail)
	logs := &fakeLogs{out: tableDiffersLine + "\n" + filler + runningProgressLine}
	r.Logs = logs

	g.Expect(r.reconcileRehearsal(context.Background(), m)).To(Succeed())
	st := m.Status.VerificationRehearsal
	g.Expect(st.Outcome).To(Equal(v1beta1.RehearsalFailed))
	g.Expect(st.Message).NotTo(ContainSubstring("last error"), "an ERROR outside the worker's window was taken as the cause")
	g.Expect(logs.calls).To(Equal(1))
	g.Expect(logs.gotLines).To(Equal(int64(verifyLogTail)))
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(v1beta1.VerificationProgressRehearsal, job.Name, false, 2)))
}

// A run's start drops the item of its kind, so until its first sample no item shows the
// previous run's counts as the new run's. The other kinds' items stay.
func TestCompareStart_DropsTheItemOfItsKind(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	rehearsal, drain, data := v1beta1.VerificationProgressRehearsal, v1beta1.VerificationProgressDrain,
		v1beta1.VerificationProgressData
	stale := func(m *v1beta1.Migration) {
		for _, kind := range []string{rehearsal, drain, data} {
			m.Status.VerificationProgress = append(m.Status.VerificationProgress,
				v1beta1.VerificationProgress{Kind: kind, JobName: "an-earlier-job", Final: true})
		}
	}
	kinds := func(m *v1beta1.Migration) []string {
		got := make([]string, 0, len(m.Status.VerificationProgress))
		for _, p := range m.Status.VerificationProgress {
			got = append(got, p.Kind)
		}
		return got
	}

	// spec.verification.rehearsal set back to true after run 1.
	m := streamingRehearsal()
	m.Status.VerificationRehearsal.Armed = false
	m.Status.VerificationRehearsal.Outcome = v1beta1.RehearsalCompleted
	stale(m)
	g.Expect(failingReconciler(t, interceptor.Funcs{}, m).reconcileRehearsal(ctx, m)).To(Succeed())
	g.Expect(m.Status.VerificationRehearsal.Run).To(Equal(int32(2)))
	g.Expect(kinds(m)).To(Equal([]string{drain, data}))

	m = followPasswordMigration()
	stale(m)
	_, _, err := failingReconciler(t, interceptor.Funcs{}, m).ensureVerify(ctx, m)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(kinds(m)).To(Equal([]string{rehearsal, data}))

	m = passwordMigration()
	m.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
	stale(m)
	g.Expect(failingReconciler(t, interceptor.Funcs{}, m).ensureVerification(ctx, m)).To(BeFalse())
	g.Expect(kinds(m)).To(Equal([]string{rehearsal, drain}))
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

	// The pass that would complete the Migration waits for the finished Job's log.
	clock := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	r = failingReconciler(t, interceptor.Funcs{}, m, completeJob(schema), completedAt(data, clock))
	r.Logs, r.now = logs, func() time.Time { return clock }
	logs.err = errBoom
	done, err = r.ensureVerification(ctx, m)
	g.Expect(err).To(MatchError(errBoom))
	g.Expect(done).To(BeFalse())

	logs.err = nil
	logs.out = runningProgressLine + "\n" + finalProgressLine
	done, err = r.ensureVerification(ctx, m)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeTrue())
	g.Expect(m.Status.VerificationProgress).To(ConsistOf(progressItem(v1beta1.VerificationProgressData, data, true, 3)))
}
