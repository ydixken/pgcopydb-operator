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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

// copyPollInterval paces the sampler until the data is across. A sample fills a seventh of
// each gap (docs/research/measurements.md#one-sample-costs-under-a-second).
const copyPollInterval = 5 * time.Second

// sampled is one progress sample, the error that replaced it, and when it started.
type sampled struct {
	sample *progress.Sample
	err    error
	at     time.Time
	// copySeen: this or an earlier sample of the run saw the copy start, which a
	// superseded sample would otherwise take with it.
	copySeen bool
}

// samplerHint is what status knows about the copy that a sample alone cannot tell,
// such as a copy that finished before an operator restart.
type samplerHint struct {
	// copyStarted: the target holds this attempt's data, so its size may be published.
	copyStarted bool
	// dataAcross: the copy is over, so the sampler slows to pollInterval.
	dataAcross bool
}

// Sampler samples each running worker on its own timer, so a slow reconcile pass
// no longer stretches the gap between size samples. It writes gauges only: the pass
// reads the latest sample into status and stays the only status writer.
type Sampler struct {
	progress   ProgressOps
	copyPeriod time.Duration
	period     time.Duration

	mu   sync.Mutex
	ctx  context.Context
	runs map[types.NamespacedName]*samplerRun
}

type samplerRun struct {
	job          string
	allDatabases bool
	cancel       context.CancelFunc
	hint         samplerHint
	last         sampled
	copySeen     bool
}

// NewSampler builds a sampler; it samples nothing until the manager starts it.
func NewSampler(p ProgressOps) *Sampler {
	return &Sampler{progress: p, copyPeriod: copyPollInterval, period: pollInterval,
		runs: map[types.NamespacedName]*samplerRun{}}
}

// Start runs until ctx ends, and every run ends with it. The manager starts it on the
// elected leader only and cancels ctx when leadership or the process goes.
func (s *Sampler) Start(ctx context.Context) error {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()
	<-ctx.Done()
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.runs {
		s.stopLocked(key)
	}
	s.ctx = nil
	return nil
}

// Observe keeps a run going for the Migration's worker Job and returns its latest
// sample, zero until the first one lands. ok is false while the sampler is not
// running, and the caller then samples for itself.
func (s *Sampler) Observe(key types.NamespacedName, job string, allDatabases bool, hint samplerHint) (sampled, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx == nil {
		return sampled{}, false
	}
	run := s.runs[key]
	if run != nil && (run.job != job || run.allDatabases != allDatabases) {
		s.stopLocked(key)
		run = nil
	}
	if run == nil {
		ctx, cancel := context.WithCancel(s.ctx)
		run = &samplerRun{job: job, allDatabases: allDatabases, cancel: cancel}
		s.runs[key] = run
		go s.loop(ctx, key, run)
	}
	run.hint = hint
	got := run.last
	got.copySeen = run.copySeen
	return got, true
}

// SampleNow takes a sample for the pass at once and keeps it as the run's own, so a
// later Observe never hands the pass an older one.
func (s *Sampler) SampleNow(ctx context.Context, key types.NamespacedName, job string, allDatabases bool) sampled {
	at := time.Now()
	smp, err := s.progress.Sample(ctx, key.Namespace, job, allDatabases)
	got := sampled{sample: smp, err: err, at: at, copySeen: smp != nil && smp.CopyStarted}
	s.mu.Lock()
	defer s.mu.Unlock()
	if run := s.runs[key]; run != nil && run.job == job {
		got = s.keepLocked(key, run, got)
	}
	return got
}

// keepLocked latches the copy and stores got with its sizes, unless the run already
// holds a newer sample: status and the gauges never step back.
func (s *Sampler) keepLocked(key types.NamespacedName, run *samplerRun, got sampled) sampled {
	run.copySeen = run.copySeen || got.copySeen
	got.copySeen = run.copySeen
	if got.at.Before(run.last.at) {
		return got
	}
	run.last = got
	recordSizes(key.Namespace, key.Name, got.sample, run.hint.copyStarted || run.copySeen)
	return got
}

// Stop ends the Migration's run. Once it returns the run writes no gauge, so a
// following metrics.Forget stays final.
func (s *Sampler) Stop(key types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopLocked(key)
}

func (s *Sampler) stopLocked(key types.NamespacedName) {
	if run := s.runs[key]; run != nil {
		run.cancel()
		delete(s.runs, key)
	}
}

// loop samples at once, then once per period measured start to start, so a slow
// sample delays only itself.
func (s *Sampler) loop(ctx context.Context, key types.NamespacedName, run *samplerRun) {
	log := logf.Log.WithName("sampler").WithValues("migration", key, "job", run.job)
	for {
		at := time.Now()
		smp, err := s.progress.Sample(ctx, key.Namespace, run.job, run.allDatabases)
		took := time.Since(at)
		s.mu.Lock()
		if ctx.Err() != nil {
			s.mu.Unlock()
			return
		}
		s.keepLocked(key, run, sampled{sample: smp, err: err, at: at, copySeen: smp != nil && smp.CopyStarted})
		hint := run.hint
		s.mu.Unlock()

		period := s.copyPeriod
		if hint.dataAcross {
			period = s.period
		}
		if err != nil {
			log.V(1).Info("database sample failed", "error", err)
		}
		if took > period {
			log.Info("progress sample took longer than its interval", "took", took, "interval", period)
		}
		timer := time.NewTimer(period - took)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// recordSizes publishes a sample's sizes. The target's waits for this attempt's copy:
// until then the target holds whatever an earlier run left there
// (docs/research/measurements.md#the-first-size-sample-of-a-copy-read-the-target-before-pgcopydb-cleaned-it).
func recordSizes(namespace, name string, s *progress.Sample, copyStarted bool) {
	if s == nil {
		return
	}
	target := s.TargetSize
	if !copyStarted {
		target = nil
	}
	metrics.RecordDatabaseSizes(namespace, name, s.SourceSize, target)
}
