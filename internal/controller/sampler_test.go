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
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// pacedProgress answers like its fakeProgress, records when each sample
// started, and holds each one for delay, or until gate hands it a token.
// deaf waits on the gate through a cancellation, like an exec already answering.
type pacedProgress struct {
	fake  *fakeProgress
	delay time.Duration
	gate  chan struct{}
	deaf  bool

	mu     sync.Mutex
	starts []time.Time
	jobs   []string
}

func (p *pacedProgress) GateScript() string { return p.fake.GateScript() }

func (p *pacedProgress) Sample(ctx context.Context, ns, job string, all bool) (*progress.Sample, error) {
	p.mu.Lock()
	p.starts = append(p.starts, time.Now())
	p.jobs = append(p.jobs, job)
	p.mu.Unlock()
	if p.deaf {
		<-p.gate
	} else if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	select {
	case <-time.After(p.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return p.fake.Sample(ctx, ns, job, all)
}

func (p *pacedProgress) started() []time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]time.Time(nil), p.starts...)
}

func (p *pacedProgress) count() int { return len(p.started()) }

func (p *pacedProgress) lastJob() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.jobs) == 0 {
		return ""
	}
	return p.jobs[len(p.jobs)-1]
}

// running reports whether key has a live run.
func (s *Sampler) running(key types.NamespacedName) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runs[key] != nil
}

// startSampler runs a sampler the way the manager does, until the spec ends
// or the returned cancel stands in for a lost leadership.
func startSampler(p ProgressOps, copyPeriod, period time.Duration) (*Sampler, context.CancelFunc, <-chan struct{}) {
	GinkgoHelper()
	s := NewSampler(p)
	s.copyPeriod, s.period = copyPeriod, period
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer GinkgoRecover()
		Expect(s.Start(ctx)).To(Succeed())
		close(done)
	}()
	Eventually(func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.ctx != nil
	}).Should(BeTrue())
	DeferCleanup(func() {
		cancel()
		<-done
	})
	return s, cancel, done
}

// maxGap is the longest stretch between two consecutive sample starts.
func maxGap(starts []time.Time) time.Duration {
	var gap time.Duration
	for i := 1; i < len(starts); i++ {
		gap = max(gap, starts[i].Sub(starts[i-1]))
	}
	return gap
}

var _ = Describe("Migration Controller background sampler", func() {
	ctx := context.Background()
	int64p := func(n int64) *int64 { return &n }
	key := func(name string) types.NamespacedName { return types.NamespacedName{Namespace: testNS, Name: name} }

	It("keeps its period start to start when a sample is slow", func() {
		p := &pacedProgress{fake: &fakeProgress{}, delay: 80 * time.Millisecond}
		s, _, _ := startSampler(p, 100*time.Millisecond, time.Hour)
		k := key("sampler-slow-sample")
		defer s.Stop(k)
		_, ok := s.Observe(k, "job", false, samplerHint{})
		Expect(ok).To(BeTrue())
		Eventually(p.count, 2*time.Second).Should(BeNumerically(">=", 6))
		// A period measured end to start would space them 180ms apart.
		Expect(maxGap(p.started())).To(BeNumerically("<", 160*time.Millisecond))
	})

	It("samples back to back when a sample outruns its period, and keeps going through errors", func() {
		fake := &fakeProgress{sizesErr: errors.New("exec refused")}
		p := &pacedProgress{fake: fake, delay: 60 * time.Millisecond}
		s, _, _ := startSampler(p, 20*time.Millisecond, time.Hour)
		k := key("sampler-overrun")
		defer s.Stop(k)
		s.Observe(k, "job", false, samplerHint{})
		Eventually(p.count).Should(BeNumerically(">=", 4))
		Expect(maxGap(p.started())).To(BeNumerically("<", 120*time.Millisecond))
		got, ok := s.Observe(k, "job", false, samplerHint{})
		Expect(ok).To(BeTrue())
		Expect(got.err).To(MatchError("exec refused"))
	})

	It("moves to the new Job when an attempt replaces the worker", func() {
		p := &pacedProgress{fake: &fakeProgress{}}
		s, _, _ := startSampler(p, 10*time.Millisecond, time.Hour)
		k := key("sampler-new-job")
		defer s.Stop(k)
		s.Observe(k, "run-1", false, samplerHint{})
		Eventually(p.lastJob).Should(Equal("run-1"))
		s.Observe(k, "run-2", false, samplerHint{})
		Eventually(p.lastJob).Should(Equal("run-2"))
		Consistently(p.lastJob, 100*time.Millisecond).Should(Equal("run-2"), "the first run must have stopped")
	})

	It("slows to the poll interval once the data is across", func() {
		p := &pacedProgress{fake: &fakeProgress{}}
		s, _, _ := startSampler(p, 20*time.Millisecond, 300*time.Millisecond)
		k := key("sampler-across")
		defer s.Stop(k)
		s.Observe(k, "job", false, samplerHint{})
		Eventually(p.count).Should(BeNumerically(">=", 5))
		s.Observe(k, "job", false, samplerHint{copyStarted: true, dataAcross: true})
		// The hint applies from the next sample on, so let one pass at the old pace.
		time.Sleep(50 * time.Millisecond)
		before := p.count()
		time.Sleep(450 * time.Millisecond)
		Expect(p.count() - before).To(BeNumerically("<=", 2))
	})

	It("withholds the target size until this attempt's copy is seen", func() {
		const name = "sampler-stale-target"
		defer metrics.Forget(testNS, name)
		// Active pgcopydb backends and no worker: the schema restore, before the target is cleaned.
		fake := &fakeProgress{src: int64p(5000), tgt: int64p(3_000_000), finalizing: true}
		s, _, _ := startSampler(&pacedProgress{fake: fake}, 10*time.Millisecond, time.Hour)
		defer s.Stop(key(name))
		s.Observe(key(name), "job", false, samplerHint{})
		// The target still holds what an earlier run left there: no target series yet.
		Eventually(func() bool {
			_, found := gaugeValue("pgcopydb_migration_source_database_size_bytes", migLabels(name))
			return found
		}).Should(BeTrue())
		_, found := gaugeValue("pgcopydb_migration_target_database_size_bytes", migLabels(name))
		Expect(found).To(BeFalse())

		// A copy too short for any sample to catch: its index or vacuum workers prove it ran.
		fake.mu.Lock()
		fake.started, fake.tgt = true, int64p(400)
		fake.mu.Unlock()
		Eventually(func() float64 {
			v, _ := gaugeValue("pgcopydb_migration_target_database_size_bytes", migLabels(name))
			return v
		}).Should(Equal(float64(400)))
	})

	It("never hands the pass a sample older than one it already holds", func() {
		p := &pacedProgress{fake: &fakeProgress{}}
		s, _, _ := startSampler(p, time.Hour, time.Hour)
		k := key("sampler-monotonic")
		defer s.Stop(k)
		s.Observe(k, "job", false, samplerHint{})
		Eventually(p.count).Should(Equal(1))
		judged := s.SampleNow(ctx, k, "job", false)
		got, _ := s.Observe(k, "job", false, samplerHint{})
		Expect(got.at).To(Equal(judged.at), "the pass's own sample becomes the run's latest")

		// A background sample that started first and answers last.
		s.mu.Lock()
		s.keepLocked(k, s.runs[k], sampled{sample: &progress.Sample{Copying: true, CopyStarted: true},
			at: judged.at.Add(-time.Second), copySeen: true})
		s.mu.Unlock()
		got, _ = s.Observe(k, "job", false, samplerHint{})
		Expect(got.at).To(Equal(judged.at))
		Expect(got.sample.Copying).To(BeFalse())
		Expect(got.copySeen).To(BeTrue(), "an older sample still latches the copy")
	})

	It("writes nothing once stopped, so a Forget after Stop is final", func() {
		const name = "sampler-stop-final"
		defer metrics.Forget(testNS, name)
		p := &pacedProgress{fake: &fakeProgress{src: int64p(5000)}, gate: make(chan struct{}), deaf: true}
		s, _, _ := startSampler(p, 10*time.Millisecond, time.Hour)
		s.Observe(key(name), "job", false, samplerHint{})
		Eventually(p.count).Should(Equal(1))
		s.Stop(key(name))
		metrics.Forget(testNS, name)
		// The sample in flight when Stop ran answers now, and must be dropped.
		close(p.gate)
		Consistently(func() bool {
			_, found := gaugeValue("pgcopydb_migration_source_database_size_bytes", migLabels(name))
			return found
		}, 200*time.Millisecond).Should(BeFalse())
		Expect(s.running(key(name))).To(BeFalse())
	})

	It("stops every run when leadership is lost", func() {
		p := &pacedProgress{fake: &fakeProgress{}}
		s, cancel, done := startSampler(p, 10*time.Millisecond, time.Hour)
		s.Observe(key("sampler-leader-a"), "job-a", false, samplerHint{})
		s.Observe(key("sampler-leader-b"), "job-b", false, samplerHint{})
		Eventually(p.count).Should(BeNumerically(">=", 4))
		cancel()
		Eventually(done).Should(BeClosed())
		settled := p.count()
		Consistently(p.count, 200*time.Millisecond).Should(Equal(settled))
		_, ok := s.Observe(key("sampler-leader-a"), "job-a", false, samplerHint{})
		Expect(ok).To(BeFalse(), "a stopped sampler must hand sampling back to the pass")
	})

	Context("wired into the reconciler", func() {
		// runningClone drives a plain clone to a running worker with a sampler wired in.
		runningClone := func(name string, p *pacedProgress, copyPeriod time.Duration) (*MigrationReconciler, *Sampler) {
			GinkgoHelper()
			s, _, _ := startSampler(p, copyPeriod, time.Hour)
			r := newReconciler()
			r.Progress, r.Sampler = p, s
			Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
			passGate(ctx, r, name)
			reconcileAndGet(ctx, r, name)
			Expect(s.running(key(name))).To(BeTrue())
			return r, s
		}

		It("publishes later cached samples even when their counts are unchanged", func() {
			const name = "sampler-status"
			defer removeMigration(ctx, name)
			defer metrics.Forget(testNS, name)
			p := &pacedProgress{fake: &fakeProgress{src: int64p(9000), tgt: int64p(700), copying: true,
				relations: &progress.RelationCounts{TablesTotal: 4, TablesDone: 1, BytesTotal: 9000, BytesDone: 700}}}
			r, s := runningClone(name, p, time.Hour)
			Eventually(func() *v1beta1.CloneProgress {
				return reconcileAndGet(ctx, r, name).Status.Progress
			}).ShouldNot(BeNil())
			m := reconcileAndGet(ctx, r, name)
			Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
			Expect(m.Status.Progress.TablesDone).To(Equal(int64(1)))
			Expect(copySeen(m)).To(BeTrue())
			Expect(m.Status.Progress.ObservedAt).NotTo(BeNil())
			expected := m.Status.Progress.DeepCopy()
			expected.ObservedAt.Time = expected.ObservedAt.Add(10 * time.Second)
			r.now = func() time.Time { return expected.ObservedAt.Time }
			cached, ok := s.Observe(key(name), m.Status.JobName, m.Spec.Clone.AllDatabases, hintFor(m))
			Expect(ok).To(BeTrue())
			Expect(cached.sample).NotTo(BeNil())
			Expect(cached.sample.Counts).NotTo(BeNil())
			calls := p.fake.counts()
			cached.at = expected.ObservedAt.Time
			s.mu.Lock()
			s.keepLocked(key(name), s.runs[key(name)], cached)
			s.mu.Unlock()
			m = reconcileAndGet(ctx, r, name)
			Expect(m.Status.Progress).To(Equal(expected))
			Expect(p.fake.counts()).To(Equal(calls), "the pass must publish the cached sample")
		})

		It("latches a copy that a newer sample replaced before any pass read it", func() {
			const name = "sampler-superseded-copy"
			defer removeMigration(ctx, name)
			defer metrics.Forget(testNS, name)
			fake := &fakeProgress{src: int64p(9000), tgt: int64p(100), copying: true}
			p := &pacedProgress{fake: fake}
			r, _ := runningClone(name, p, 5*time.Millisecond)
			Eventually(p.count).Should(BeNumerically(">=", 2))
			fake.mu.Lock()
			fake.copying, fake.finalizing, fake.tgt = false, true, int64p(5000)
			fake.mu.Unlock()
			seen := p.count()
			Eventually(p.count).Should(BeNumerically(">=", seen+3))

			m := reconcileAndGet(ctx, r, name)
			Expect(copySeen(m)).To(BeTrue(), "%+v", m.Status.Conditions)
			Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFinalizing))
			v, _ := gaugeValue("pgcopydb_migration_target_database_size_bytes", migLabels(name))
			Expect(v).To(Equal(float64(5000)))
		})

		It("keeps its cadence through a slow reconcile pass", func() {
			const name = "sampler-slow-pass"
			defer removeMigration(ctx, name)
			defer metrics.Forget(testNS, name)
			p := &pacedProgress{fake: &fakeProgress{src: int64p(9000)}}
			s, _, _ := startSampler(p, 50*time.Millisecond, time.Hour)
			r := newReconciler()
			r.Progress, r.Sampler = p, s
			r.Sentinel = &fakeSentinel{state: &sentinel.State{WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN}}
			m := validMigration(name)
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
			Expect(k8sClient.Create(ctx, m)).To(Succeed())
			passGate(ctx, r, name)
			reconcileAndGet(ctx, r, name)

			// The worker log fetch, one step of the pass, now takes 600ms.
			r.Logs = &slowLogs{fakeLogs: copyingLogs(), delay: 600 * time.Millisecond}
			before := p.count()
			began := time.Now()
			reconcileAndGet(ctx, r, name)
			Expect(time.Since(began)).To(BeNumerically(">=", 600*time.Millisecond))
			var during []time.Time
			for _, at := range p.started() {
				if !at.Before(began) {
					during = append(during, at)
				}
			}
			Expect(p.count()-before).To(BeNumerically(">=", 6), "samples taken while the pass was stuck")
			Expect(maxGap(during)).To(BeNumerically("<", 200*time.Millisecond))
		})

		It("judges the clone-done marker on a sample of its own, taken after the log fetch", func() {
			const name = "sampler-follow-gate"
			defer removeMigration(ctx, name)
			defer metrics.Forget(testNS, name)
			// The background sample predates the marker and still owes a table.
			fake := &fakeProgress{src: int64p(9000), relations: &progress.RelationCounts{TablesTotal: 2, TablesDone: 1}}
			p := &pacedProgress{fake: fake}
			s, _, _ := startSampler(p, time.Hour, time.Hour)
			r := newReconciler()
			r.Progress, r.Sampler = p, s
			r.Sentinel = &fakeSentinel{state: &sentinel.State{WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN}}
			logs := copyingLogs()
			r.Logs = logs
			m := validMigration(name)
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
			Expect(k8sClient.Create(ctx, m)).To(Succeed())
			passGate(ctx, r, name)
			reconcileAndGet(ctx, r, name)
			Eventually(p.count).Should(Equal(1))
			reconcileAndGet(ctx, r, name)
			Expect(p.count()).To(Equal(1), "an ordinary pass reads the sampler's sample")

			fake.setRelations(&progress.RelationCounts{TablesTotal: 2, TablesDone: 2})
			logs.tsOut += cloneDoneLine
			got := reconcileAndGet(ctx, r, name)
			Expect(p.count()).To(Equal(2), "the marker pass samples for itself")
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue(),
				"%+v", got.Status.Conditions)
		})

		It("stops sampling when the worker ends", func() {
			const name = "sampler-stop-finished"
			defer removeMigration(ctx, name)
			defer metrics.Forget(testNS, name)
			p := &pacedProgress{fake: &fakeProgress{}}
			r, s := runningClone(name, p, 10*time.Millisecond)
			Eventually(p.count).Should(BeNumerically(">=", 2))
			finishJob(ctx, name+"-run-1", true)
			reconcileAndGet(ctx, r, name)
			Expect(s.running(key(name))).To(BeFalse())
			settled := p.count()
			Consistently(p.count, 100*time.Millisecond).Should(BeNumerically("<=", settled+1))
		})

		It("stops sampling when the Migration is suspended", func() {
			const name = "sampler-stop-suspend"
			defer removeMigration(ctx, name)
			defer metrics.Forget(testNS, name)
			p := &pacedProgress{fake: &fakeProgress{}}
			r, s := runningClone(name, p, 10*time.Millisecond)
			m := currentMigration(name)
			m.Spec.Suspend = true
			Expect(k8sClient.Update(ctx, m)).To(Succeed())
			Expect(reconcileAndGet(ctx, r, name).Status.Phase).To(Equal(v1beta1.PhaseSuspended))
			Expect(s.running(key(name))).To(BeFalse())
		})

		It("stops sampling before a deleted Migration's series are forgotten", func() {
			const name = "sampler-stop-deleted"
			defer removeMigration(ctx, name)
			defer metrics.Forget(testNS, name)
			p := &pacedProgress{fake: &fakeProgress{src: int64p(5000)}}
			r, s := runningClone(name, p, 10*time.Millisecond)
			Eventually(func() bool {
				_, found := gaugeValue("pgcopydb_migration_source_database_size_bytes", migLabels(name))
				return found
			}).Should(BeTrue())
			Expect(k8sClient.Delete(ctx, currentMigration(name))).To(Succeed())
			_, err := r.Reconcile(ctx, reconcileRequest(name))
			Expect(err).NotTo(HaveOccurred())
			Expect(s.running(key(name))).To(BeFalse())
			Consistently(func() bool {
				_, found := gaugeValue("pgcopydb_migration_source_database_size_bytes", migLabels(name))
				return found
			}, 100*time.Millisecond).Should(BeFalse())
		})

		It("leaves every status write to the reconcile pass", func() {
			const name = "sampler-one-writer"
			defer removeMigration(ctx, name)
			defer metrics.Forget(testNS, name)
			watching, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			var inPass atomic.Bool
			var inflight, writes, outside, overlapped atomic.Int32
			guard := func() func() {
				writes.Add(1)
				if !inPass.Load() {
					outside.Add(1)
				}
				if inflight.Add(1) > 1 {
					overlapped.Add(1)
				}
				return func() { inflight.Add(-1) }
			}
			p := &pacedProgress{fake: &fakeProgress{src: int64p(9000), tgt: int64p(700), copying: true,
				relations: &progress.RelationCounts{TablesTotal: 4, TablesDone: 1}}}
			r, _ := runningClone(name, p, 5*time.Millisecond)
			r.Client = interceptor.NewClient(watching, interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object,
					patch client.Patch, opts ...client.SubResourcePatchOption) error {
					defer guard()()
					return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
				SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object,
					opts ...client.SubResourceUpdateOption) error {
					defer guard()()
					return c.SubResource(sub).Update(ctx, obj, opts...)
				},
			})
			for range 10 {
				inPass.Store(true)
				_, err := r.Reconcile(ctx, reconcileRequest(name))
				inPass.Store(false)
				Expect(err).NotTo(HaveOccurred())
				time.Sleep(15 * time.Millisecond)
			}
			Expect(p.count()).To(BeNumerically(">=", 10), "the sampler ran alongside the passes")
			Expect(writes.Load()).To(BeNumerically(">=", 10))
			Expect(outside.Load()).To(BeZero(), "a status write happened outside a reconcile pass")
			Expect(overlapped.Load()).To(BeZero(), "two status writes overlapped")
		})
	})
})

func currentMigration(name string) *v1beta1.Migration {
	GinkgoHelper()
	m := &v1beta1.Migration{}
	Expect(k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, m)).To(Succeed())
	return m
}

func reconcileRequest(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: name}}
}

// slowLogs holds every worker log fetch for delay: a slow step inside the pass.
type slowLogs struct {
	*fakeLogs
	delay time.Duration
}

func (l *slowLogs) JobLogsTimestamps(ctx context.Context, ns, job string, lines int64) ([]byte, error) {
	time.Sleep(l.delay)
	return l.fakeLogs.JobLogsTimestamps(ctx, ns, job, lines)
}
