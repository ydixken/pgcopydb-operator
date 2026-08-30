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
	// The catalog poll runs mid-attempt only on follow migrations past
	// CloneCompleted; with no log reader wired (as here) the transition is
	// unobservable and the poll stays on, mirroring the sentinel fallback.
	followMigration := func(name string) *v1beta1.Migration {
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverManual}
		return m
	}

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
		Expect(k8sClient.Create(ctx, followMigration(name))).To(Succeed())
		passGate(ctx, r, name)

		m := reconcileAndGet(ctx, r, name)
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

	It("keeps the previous sample when the poller returns none", func() {
		const name = "mig-progress-keep"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		fake := &fakeProgress{cp: &v1beta1.CloneProgress{TablesTotal: 7, TablesDone: 2}}
		r := newReconciler()
		r.Progress = fake
		Expect(k8sClient.Create(ctx, followMigration(name))).To(Succeed())
		passGate(ctx, r, name)
		reconcileAndGet(ctx, r, name)

		// Gate shut (pod restarting, version off the allowlist): no sample.
		fake.mu.Lock()
		fake.cp = nil
		fake.mu.Unlock()
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).NotTo(BeNil())
		Expect(m.Status.Progress.TablesTotal).To(Equal(int64(7)))
	})

	It("passes despite sampler errors", func() {
		const name = "mig-progress-err"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		fake := &fakeProgress{cpErr: errors.New("exec wedged"), sizesErr: errors.New("psql refused")}
		r := newReconciler()
		r.Progress = fake
		Expect(k8sClient.Create(ctx, followMigration(name))).To(Succeed())
		passGate(ctx, r, name)

		// reconcileAndGet already asserts a nil reconcile error.
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(m.Status.Progress).To(BeNil())
		_, found := gaugeValue("pgcopydb_migration_source_database_size_bytes", migLabels(name))
		Expect(found).To(BeFalse())
	})

	It("quiesces the catalog poll until the base copy is done", func() {
		const name = "mig-progress-quiesce"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		fake := &fakeProgress{
			cp:  &v1beta1.CloneProgress{TablesTotal: 5, TablesDone: 5},
			src: int64p(9000), tgt: int64p(100),
		}
		r := newReconciler()
		r.Progress = fake
		// A log reader is wired, so the fallback above does not apply: the
		// poll must wait for the clone-done marker.
		const ts = "2026-08-28T10:00:00.000000000Z "
		logs := &fakeLogs{tsOut: ts +
			`{"error_severity":"INFO","message":"STEP 10: restore the post-data section to the target database"}` + "\n"}
		r.Logs = logs
		Expect(k8sClient.Create(ctx, followMigration(name))).To(Succeed())
		passGate(ctx, r, name)

		// Copy still running (step banners only in the log): the sizes flow
		// (psql, no catalog), the catalog poll does not.
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).To(BeNil())
		cp, sizes := fake.counts()
		Expect(cp).To(BeZero())
		Expect(sizes).To(Equal(1))

		// The worker logs the clone-done marker: the poll starts and the
		// sizes keep flowing.
		logs.tsOut += ts +
			`{"error_severity":"INFO","message":"Updating the pgcopydb.sentinel to enable applying changes"}` + "\n"
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
		Expect(m.Status.Progress).NotTo(BeNil())
		Expect(m.Status.Progress.TablesDone).To(Equal(int64(5)))
		cp, sizes = fake.counts()
		Expect(cp).To(Equal(1))
		Expect(sizes).To(Equal(2))
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
