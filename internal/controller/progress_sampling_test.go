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
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// fakeProgress scripts the sampler the way fakeSentinel scripts the
// sentinel: tests choose the samples (or errors), the reconciler reads them.
// The call counters let specs prove copy-phase quiescence of the catalog
// poll while the size sampler keeps running.
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
			"drain verified: origin progress reached endpos within one WAL page\n"
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
			"drain verified: origin progress reached endpos within one WAL page\n"
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

	phaseFor := func(name string, copying, finalizing bool) v1beta1.MigrationPhase {
		GinkgoHelper()
		r := newReconciler()
		r.Progress = &fakeProgress{copying: copying, finalizing: finalizing}
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		passGate(ctx, r, name)
		return reconcileAndGet(ctx, r, name).Status.Phase
	}

	It("reports Finalizing once only the tail is left", func() {
		const name = "mig-stage-tail"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		Expect(phaseFor(name, false, true)).To(Equal(v1beta1.PhaseFinalizing))
	})

	It("stays Cloning while copy workers are busy", func() {
		const name = "mig-stage-copying"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		Expect(phaseFor(name, true, false)).To(Equal(v1beta1.PhaseCloning))
	})

	It("stays Cloning when the probe knows nothing", func() {
		const name = "mig-stage-unknown"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		Expect(phaseFor(name, false, false)).To(Equal(v1beta1.PhaseCloning),
			"an unreadable probe must not be read as either state")
	})
})
