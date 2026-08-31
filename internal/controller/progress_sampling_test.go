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
	"errors"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// fakeProgress scripts the sampler the way fakeSentinel scripts the
// sentinel: tests choose the samples (or errors), the reconciler reads them.
// The call counters let specs prove the catalog poll never runs against a
// live worker, in any phase, while the size sampler keeps running.
type fakeProgress struct {
	mu        sync.Mutex
	cp        *v1beta1.CloneProgress
	cpErr     error
	src, tgt  *int64
	sizesErr  error
	cpCalls   int
	sizeCalls int

	copying, finalizing bool
	stageCalls          int
}

// GateScript stands in for the poller's version gate. Specs assert on this
// text reaching the verify Job, not on a real allowlist.
func (f *fakeProgress) GateScript() string {
	return "pgcopydb list progress --json --dir /work/pgcopydb\n"
}

func (f *fakeProgress) CloneStage(context.Context, string, string) (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stageCalls++
	return f.copying, f.finalizing
}

func (f *fakeProgress) CloneProgress(context.Context, string, string) (*v1beta1.CloneProgress, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cpCalls++
	if f.cpErr != nil {
		return nil, f.cpErr
	}
	if f.cp == nil {
		return nil, nil
	}
	c := *f.cp
	return &c, nil
}

func (f *fakeProgress) DatabaseSizes(context.Context, string, string) (src, tgt *int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sizeCalls++
	return f.src, f.tgt, f.sizesErr
}

// counts returns (catalog polls, size samples) seen so far.
func (f *fakeProgress) counts() (cp, sizes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cpCalls, f.sizeCalls
}

// gaugeValue reads one series from the controller-runtime registry, the same
// surface Prometheus scrapes.
func gaugeValue(metric string, labels map[string]string) (float64, bool) {
	families, err := crmetrics.Registry.Gather()
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	for _, mf := range families {
		if mf.GetName() != metric {
			continue
		}
	series:
		for _, s := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range s.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue series
				}
			}
			return s.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

var _ = Describe("Migration Controller progress sampling", func() {
	ctx := context.Background()

	int64p := func(n int64) *int64 { return &n }
	migLabels := func(name string) map[string]string {
		return map[string]string{"namespace": testNS, "name": name}
	}
	// The catalog poll runs mid-attempt only on a follow migration past
	// CloneCompleted, which the operator reads off the worker log, so these
	// specs wire a log reader that has already logged the end of the copy.
	followMigration := func(name string) *v1beta1.Migration {
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverManual}
		return m
	}

	// listProgressJSON is what `pgcopydb list progress --json` prints, newlines
	// squeezed out by the verify script (read off the real command in the
	// v0.9.0 runner image against a finished clone).
	const listProgressJSON = `{    "tables": {        "total": 2,        "done": 2    },` +
		`    "indexes": {        "total": 3,        "done": 3    },` +
		`    "bytes": {        "total": 12165120,        "done": 10347680    }}`

	It("persists a sample to status.progress and the gauges", func() {
		const name = "mig-progress-sample"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		fake := &fakeProgress{
			cp: &v1beta1.CloneProgress{
				TablesTotal: 12, TablesDone: 3,
				BytesTotal: resource.NewQuantity(1000, resource.BinarySI),
				BytesDone:  resource.NewQuantity(400, resource.BinarySI),
			},
			src: int64p(5000), tgt: int64p(400),
		}
		r := newReconciler()
		r.Progress = fake
		// A plain clone, whose counters are read by exec-ing into its own pod
		// once pgcopydb has exited (a follow migration takes the same numbers
		// out of the verify Job instead; see the specs below).
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		passGate(ctx, r, name)

		// While the worker runs only the psql size sample may go out; the
		// counters wait for it to exit.
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).To(BeNil())
		finishJob(ctx, name+"-run-1", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).NotTo(BeNil())
		Expect(m.Status.Progress.TablesDone).To(Equal(int64(3)))
		Expect(m.Status.Progress.BytesDone.Value()).To(Equal(int64(400)))

		for metric, want := range map[string]float64{
			"pgcopydb_migration_source_database_size_bytes": 5000,
			"pgcopydb_migration_target_database_size_bytes": 400,
			"pgcopydb_migration_clone_planned_bytes":        1000,
			"pgcopydb_migration_clone_copied_bytes":         400,
			"pgcopydb_migration_tables_done":                3,
		} {
			got, found := gaugeValue(metric, migLabels(name))
			Expect(found).To(BeTrue(), metric)
			Expect(got).To(Equal(want), metric)
		}
	})

	It("keeps reading the verify Job until the counters land, then stops", func() {
		const name = "mig-progress-keep"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r := newReconciler()
		r.Progress = &fakeProgress{}
		r.Sentinel = &fakeSentinel{state: &sentinel.State{
			WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN,
		}}
		logs := cloneDoneLogs()
		r.Logs = logs
		m := followMigration(name)
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverAutomatic}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		passGate(ctx, r, name)
		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-run-1", true)
		reconcileAndGet(ctx, r, name)

		// The Job finished but its log is not readable yet (pod still being
		// collected): no counters, and no reason to give up on them.
		logs.out = ""
		finishJob(ctx, name+"-verify", true)
		got := reconcileAndGet(ctx, r, name)
		Expect(got.Status.Progress).To(BeNil())

		// Readable now: the counters land.
		logs.out = verifyProgressPrefix + listProgressJSON + "\n"
		got = reconcileAndGet(ctx, r, name)
		Expect(got.Status.Progress).NotTo(BeNil())
		Expect(got.Status.Progress.TablesDone).To(Equal(int64(2)))

		// Landed once is landed: a later pass does not re-read the log, so a
		// changed line cannot rewrite a finished migration's counters.
		logs.out = verifyProgressPrefix + `{"tables": {"total": 99, "done": 99}}` + "\n"
		got = reconcileAndGet(ctx, r, name)
		Expect(got.Status.Progress.TablesDone).To(Equal(int64(2)))
	})

	It("passes despite sampler errors", func() {
		const name = "mig-progress-err"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		fake := &fakeProgress{cpErr: errors.New("exec wedged"), sizesErr: errors.New("psql refused")}
		r := newReconciler()
		r.Progress = fake
		// A plain clone, so both samplers that exec into a pod are exercised:
		// the sizes while the worker runs, the counters once it has exited.
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		passGate(ctx, r, name)

		// reconcileAndGet already asserts a nil reconcile error.
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		_, found := gaugeValue("pgcopydb_migration_source_database_size_bytes", migLabels(name))
		Expect(found).To(BeFalse())

		// The poll's own error, on the finish path where it runs, is just as
		// absorbing: no sample, no condition, no failed pass.
		finishJob(ctx, name+"-run-1", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).To(BeNil())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed)).To(BeFalse())
	})

	It("never polls the catalog while the worker is alive", func() {
		const name = "mig-progress-quiesce"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		fake := &fakeProgress{
			cp:  &v1beta1.CloneProgress{TablesTotal: 5, TablesDone: 5},
			src: int64p(9000), tgt: int64p(100),
		}
		r := newReconciler()
		r.Progress = fake
		logs := copyingLogs()
		r.Logs = logs
		Expect(k8sClient.Create(ctx, followMigration(name))).To(Succeed())
		passGate(ctx, r, name)

		// Copy still running (step banners only in the log): the sizes flow
		// (psql, no catalog), the poll does not.
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).To(BeNil())
		cp, sizes := fake.counts()
		Expect(cp).To(BeZero())
		Expect(sizes).To(Equal(1))

		// The worker logs the clone-done marker. The copy is over, but the
		// worker is not: it streams changes now, and its catalog stays off
		// limits. Only the sizes keep flowing.
		logs.tsOut += cloneDoneLine
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
		Expect(m.Status.Progress).To(BeNil())
		cp, sizes = fake.counts()
		Expect(cp).To(BeZero(), "the copy being done does not make the worker's catalog safe to open")
		Expect(sizes).To(Equal(2))

		// The worker exits, and still nothing execs into it: this migration's
		// counters are read from the verify Job's log instead.
		finishJob(ctx, name+"-run-1", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).To(BeNil())
		cp, _ = fake.counts()
		Expect(cp).To(BeZero())
	})

	It("takes a follow migration's counters from the verify Job", func() {
		const name = "mig-progress-verify"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		fake := &fakeProgress{src: int64p(9000)}
		r := newReconciler()
		r.Progress = fake
		r.Sentinel = &fakeSentinel{state: &sentinel.State{
			WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN,
		}}
		logs := cloneDoneLogs()
		r.Logs = logs
		m := followMigration(name)
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverAutomatic}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		passGate(ctx, r, name)
		reconcileAndGet(ctx, r, name) // caught up, Automatic: endpos set

		// The verify Job carries the gate the poller would have run.
		finishJob(ctx, name+"-run-1", true)
		got := reconcileAndGet(ctx, r, name)
		Expect(got.Status.Progress).To(BeNil(), "nothing to read before the verify Job has run")
		Expect(fetchJob(ctx, name+"-verify").Spec.Template.Spec.Containers[0].Args[1]).
			To(ContainSubstring("list progress"))

		// It finishes, and its log carries the counters.
		logs.out = "endpos=0/100 replay_lsn=0/100 origin_progress=0/100 origin_gap_bytes=0\n" +
			verifyProgressPrefix + listProgressJSON + "\n" +
			"drain verified: origin progress 0/100 equals endpos 0/100, nothing left to apply\n"
		finishJob(ctx, name+"-verify", true)
		got = reconcileAndGet(ctx, r, name)
		Expect(got.Status.Progress).NotTo(BeNil(), "the verify Job's counters never reached status")
		Expect(got.Status.Progress.TablesDone).To(Equal(int64(2)))
		Expect(got.Status.Progress.IndexesDone).To(Equal(int64(3)))
		Expect(got.Status.Progress.BytesDone.Value()).To(Equal(int64(10347680)))
		Expect(meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeTrue())

		// The poller itself was never asked to exec into anything: this
		// migration's counters came out of a Job log.
		cp, _ := fake.counts()
		Expect(cp).To(BeZero())
	})

	It("verifies the drain whatever the counters line does", func() {
		const name = "mig-progress-verify-junk"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r := newReconciler()
		r.Progress = &fakeProgress{}
		r.Sentinel = &fakeSentinel{state: &sentinel.State{
			WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN,
		}}
		logs := cloneDoneLogs()
		r.Logs = logs
		m := followMigration(name)
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverAutomatic}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		passGate(ctx, r, name)
		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-run-1", true)
		reconcileAndGet(ctx, r, name)

		// Truncated JSON, the shape a killed `list progress` would leave.
		logs.out = verifyProgressPrefix + `{ "tables": { "total":` + "\n" +
			"drain verified: origin progress 0/100 equals endpos 0/100, nothing left to apply\n"
		finishJob(ctx, name+"-verify", true)
		got := reconcileAndGet(ctx, r, name)
		Expect(got.Status.Progress).To(BeNil())
		Expect(meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeTrue(),
			"a broken counters line must not touch the drain verdict")
	})

	It("gives a plain clone its one sample when the worker succeeds", func() {
		const name = "mig-progress-finish"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		fake := &fakeProgress{cp: &v1beta1.CloneProgress{TablesTotal: 4, TablesDone: 4}, src: int64p(7000)}
		r := newReconciler()
		r.Progress = fake
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		passGate(ctx, r, name)

		// Mid-run pass: a plain clone never opens the catalogs, only psql.
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).To(BeNil())
		cp, sizes := fake.counts()
		Expect(cp).To(BeZero())
		Expect(sizes).To(Equal(1))

		// Worker succeeded: pgcopydb exited, the catalogs are quiet, and the
		// finish path takes the final sample.
		finishJob(ctx, name+"-run-1", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
		Expect(m.Status.Progress).NotTo(BeNil())
		Expect(m.Status.Progress.TablesDone).To(Equal(int64(4)))
		cp, _ = fake.counts()
		Expect(cp).To(Equal(1))
	})

	It("re-emits a terminal Migration's series after a registry wipe", func() {
		const name = "mig-terminal-reemit"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		m := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		m.Status.Phase = v1beta1.PhaseCompleted
		meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
			Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "MigrationSucceeded",
		})
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())

		// An operator restart starts with an empty registry; Forget stands in.
		metrics.Forget(testNS, name)
		reconcileAndGet(ctx, newReconciler(), name)

		labels := migLabels(name)
		labels["phase"] = string(v1beta1.PhaseCompleted)
		got, found := gaugeValue("pgcopydb_migration_phase", labels)
		Expect(found).To(BeTrue())
		Expect(got).To(Equal(float64(1)))
	})

	It("forgets the series of a deleted Migration on the NotFound pass", func() {
		const name = "mig-notfound-forget"
		// Record series for a CR that does not exist, as if it was deleted
		// under a running operator without the finalizer path.
		stale := validMigration(name)
		stale.Status.Phase = v1beta1.PhaseCloning
		metrics.Record(stale)
		_, found := gaugeValue("pgcopydb_migration_attempts", migLabels(name))
		Expect(found).To(BeTrue())

		_, err := newReconciler().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
		})
		Expect(err).NotTo(HaveOccurred())
		_, found = gaugeValue("pgcopydb_migration_attempts", migLabels(name))
		Expect(found).To(BeFalse())
	})
})

// The copy and its tail are reported apart because they behave differently: a
// copy runs every worker, the tail narrows to index builds and a vacuum on the
// largest table, and during it the target stops growing so any size-derived
// estimate reads as finished. Driven through the reconciler on the same gate
// the other specs here use, because a fake returning what a test told it to
// return proves nothing about the branch that reads it.
var _ = Describe("Migration Controller clone stage", func() {
	ctx := context.Background()

	// start drives a fresh Migration to its first observation pass with a
	// probe the spec scripts from there on.
	start := func(name string) (*MigrationReconciler, *fakeProgress) {
		GinkgoHelper()
		fake := &fakeProgress{}
		r := newReconciler()
		r.Progress = fake
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		passGate(ctx, r, name)
		return r, fake
	}

	// sample scripts the next probe answer and runs one observation pass.
	sample := func(r *MigrationReconciler, f *fakeProgress, name string, copying, finalizing bool) *v1beta1.Migration {
		GinkgoHelper()
		f.mu.Lock()
		f.copying, f.finalizing = copying, finalizing
		f.mu.Unlock()
		return reconcileAndGet(ctx, r, name)
	}

	cloneReason := func(m *v1beta1.Migration) string {
		GinkgoHelper()
		c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCloneCompleted)
		Expect(c).NotTo(BeNil())
		return c.Reason
	}

	It("stays Cloning while copy workers are busy, and records having seen them", func() {
		const name = "mig-stage-copying"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r, fake := start(name)
		m := sample(r, fake, name, true, false)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(cloneReason(m)).To(Equal(reasonCopyingData))
	})

	It("refuses Finalizing until the probe has seen the copy move", func() {
		const name = "mig-stage-early-tail"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r, fake := start(name)
		m := sample(r, fake, name, false, true)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning),
			"a copy nobody ever saw running cannot already be in its tail")
		Expect(cloneReason(m)).To(Equal(reasonCloneRunning))
	})

	It("reports Finalizing once only the tail is left", func() {
		const name = "mig-stage-tail"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r, fake := start(name)
		Expect(sample(r, fake, name, true, false).Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(sample(r, fake, name, false, true).Status.Phase).To(Equal(v1beta1.PhaseFinalizing))
	})

	// The latch may only ever promote a CloneCompleted that is still False.
	// Writing False over a True one would tell the operator the base copy is
	// running again, and it acts on that: it reports the stream but stops
	// driving it, so a caught-up migration would sit there uncut and the
	// phase would rewind out of Streaming.
	It("keeps a finished base copy finished when a copy worker turns up late", func() {
		const name = "mig-stage-late-copy"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		const ts = "2026-08-30T10:00:00.000000000Z "
		const banner = `{"error_severity":"INFO","message":"STEP 10: restore the post-data section to the target database"}`
		fake := &fakeProgress{}
		sent := &fakeSentinel{state: &sentinel.State{WriteLSN: laggingWriteLSN,
			ReplayLSN: laggingReplayLSN, SourceHead: laggingHeadLSN, Endpos: sentinel.ZeroLSN}}
		logs := &fakeLogs{tsOut: ts + banner + "\n"}
		r := newReconciler()
		r.Progress, r.Sentinel, r.Logs = fake, sent, logs
		mig := validMigration(name)
		mig.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		mig.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverManual}
		Expect(k8sClient.Create(ctx, mig)).To(Succeed())
		passGate(ctx, r, name)

		logs.tsOut += ts + `{"error_severity":"INFO","message":"Updating the pgcopydb.sentinel to enable applying changes"}` + "\n"
		m := reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())

		// The marker scrolls out of the tail window (it is a bounded read),
		// so nothing would put the condition back, and a stray backend gets
		// counted as a copy worker.
		logs.tsOut = ts + banner + "\n"
		before := sent.callCount()
		m = sample(r, fake, name, true, false)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
		Expect(sent.callCount()).To(BeNumerically(">", before),
			"the follow watch runs on every pass a worker is alive, whatever the copy probe says")
	})

	// No controller path writes CopyingData on a True condition, so this pins
	// the invariant against everything else that can write status: an older
	// operator's leftover reason, a manual edit, a future path that forgets.
	It("does not open the latch off a reason left on a completed base copy", func() {
		const name = "mig-stage-stale-latch"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r, fake := start(name)
		m := reconcileAndGet(ctx, r, name)
		meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
			Type: v1beta1.ConditionCloneCompleted, Status: metav1.ConditionTrue, Reason: reasonCopyingData,
			Message: "a reason this operator only ever writes on a False condition",
		})
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())

		Expect(sample(r, fake, name, false, true).Status.Phase).To(Equal(v1beta1.PhaseCloning),
			"a finished base copy has no tail to report")
	})

	It("keeps the phase the last answered sample left when the probe knows nothing", func() {
		const name = "mig-stage-unknown"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r, fake := start(name)
		sample(r, fake, name, true, false)
		Expect(sample(r, fake, name, false, true).Status.Phase).To(Equal(v1beta1.PhaseFinalizing))
		Expect(sample(r, fake, name, false, false).Status.Phase).To(Equal(v1beta1.PhaseFinalizing),
			"an unreadable probe is evidence of nothing, least of all of going backwards")
	})
})

// A stream that is applying but nowhere near caught up: the gap between these
// two is far above the 16Mi default, so no pass can mistake it for a cutover.
const laggingWriteLSN, laggingReplayLSN, laggingHeadLSN = "0/40000000", "0/10000000", "0/48000000"

// A retry resumes a migration where it stopped, so the phase it starts from is
// read off the conditions. The case that used to lie: a worker dying during
// the drain came back reported as Cloning, with the finished base copy marked
// unfinished behind it.
var _ = Describe("Migration Controller attempt phase", func() {
	ctx := context.Background()

	// followSentinel wires a scripted sentinel into a fresh reconciler and
	// puts an automatic-cutover follow migration on its first observed pass.
	// The log reader comes with it because the end of the base copy is read
	// from the worker log, and these specs all start after that.
	followSentinel := func(name string, mode v1beta1.CutoverMode) (*MigrationReconciler, *fakeSentinel) {
		GinkgoHelper()
		fake := &fakeSentinel{}
		r := newReconciler()
		r.Sentinel, r.Logs = fake, cloneDoneLogs()
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: mode}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		passGate(ctx, r, name)
		return r, fake
	}

	// A worker Job can go while the Migration is still live: spec.ttl
	// SecondsAfterFinished reaches it, and so does a manual delete. The next
	// pass finds it gone and starts an attempt, which is the path that used
	// to announce Cloning over a migration long past its base copy.
	It("resumes a streaming migration at Streaming when its Job vanishes", func() {
		const name = "mig-retry-while-streaming"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r, fake := followSentinel(name, v1beta1.CutoverManual)

		// Applying, but far too far behind to cut over: no endpos anywhere.
		fake.state = &sentinel.State{WriteLSN: laggingWriteLSN,
			ReplayLSN: laggingReplayLSN, SourceHead: laggingHeadLSN, Endpos: sentinel.ZeroLSN}
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
		Expect(m.Status.Replication.Endpos).To(BeEmpty())

		Expect(k8sClient.Delete(ctx, fetchJob(ctx, name+"-run-1"),
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
	})

	// An operator writes the endpos into status at the freeze, so a cutover
	// played forward always carries one. A status written before that change
	// does not, and the operator that reads it back is the new one: on an
	// upgrade mid-migration, CutoverCompleted is the only record left that
	// the cutover happened, and the resumed attempt has to honour it.
	It("resumes at CuttingOver when an upgraded status carries no endpos", func() {
		const name = "mig-retry-after-verified"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r, fake := followSentinel(name, v1beta1.CutoverAutomatic)

		fake.state = &sentinel.State{WriteLSN: caughtUpLSN,
			ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		reconcileAndGet(ctx, r, name)
		m := confirmCaughtUp(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))

		// Worker exits 0, the verify Job proves the drain, cleanup is still
		// to run: CutoverCompleted is True and Complete is not.
		finishJob(ctx, name+"-run-1", true)
		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-verify", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeTrue())

		// What the older operator left behind. Blanking it is the only way
		// to reach the condition arm, since with an endpos in status the
		// EndposSet arm answers first and this spec would prove nothing.
		m.Status.Replication.Endpos = ""
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())

		Expect(k8sClient.Delete(ctx, fetchJob(ctx, name+"-run-1"),
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Replication.Endpos).To(BeEmpty())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
	})

	It("does not rewind the phase when an attempt fails after cutover started", func() {
		const name = "mig-retry-after-cutover"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		r, fake := followSentinel(name, v1beta1.CutoverAutomatic)

		// Caught up: the operator freezes the stream and the worker drains.
		fake.state = &sentinel.State{WriteLSN: caughtUpLSN,
			ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		// The freeze records its own endpos: nothing reads it back out of
		// the worker, so the operator keeps what it set.
		reconcileAndGet(ctx, r, name)
		m := confirmCaughtUp(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(m.Status.Replication.Endpos).To(Equal(caughtUpLSN))

		// The worker dies mid-drain. Attempt 2 resumes that drain: the base
		// copy is still done and the cutover is still under way.
		finishJob(ctx, name+"-run-1", false)
		reconcileAndGet(ctx, r, name)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
	})
})
