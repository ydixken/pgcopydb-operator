package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"k8s.io/apimachinery/pkg/api/meta"
)

const nativeSampleFreshness = 20 * time.Second

// Keep one record in the existing sampler run; this data cannot prove completion.
type nativeSampler struct {
	mu                 sync.Mutex
	now                func() time.Time
	stopped            bool
	serial             uint64
	cancel             context.CancelFunc
	scope              nativeSamplerScope
	last               nativeSample
	started, completed time.Time
	previous, current  nativeLagObservation
}

type nativeSamplerScope struct {
	execution                native.ExecutionData
	baseCopyComplete, follow bool
	threshold                int64
}

type nativeLagObservation struct {
	serial             uint64
	started, completed time.Time
	fingerprint        [32]byte
}

// These optional measurements deliberately have no replication frontier or proof fields.
type nativeSample struct {
	lagBytes         *int64
	progress         *native.CopyProgress
	phase            string
	observedAt       time.Time
	catchUpConfirmed bool
}

func (s *nativeSampler) Sample(ctx context.Context, control *native.Client, m *v1beta1.Migration, expected native.ExecutionData, baseCopyComplete bool, threshold int64, admit func(context.Context) (native.ExecutionData, error)) (nativeSample, error) {
	if s == nil {
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	m = m.DeepCopy()
	scope, valid := nativeSamplingScope(m, expected, baseCopyComplete, threshold)
	callCtx, cancel := context.WithTimeout(ctx, nativeSampleFreshness)
	defer cancel()
	s.mu.Lock()
	if !valid || control == nil || admit == nil || callCtx.Err() != nil || s.stopped {
		s.clearLocked()
		s.mu.Unlock()
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	if s.scope != scope {
		s.clearLocked()
		s.scope = scope
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.serial++
	serial := s.serial
	s.cancel = cancel
	s.last = nativeSample{}
	started := s.clock()
	s.mu.Unlock()

	progress, err := nativeSampleProgress(callCtx, control, scope.execution, admit)
	if err == nil && scope.follow && !nativeFollowStatus(progress) {
		err = native.ErrNativeControlUnavailable
	}
	if err != nil {
		return s.finish(callCtx, serial, scope, started, nativeSample{}, nil, err)
	}
	got := nativeSample{phase: progress.Phase, progress: cloneNativeCopyProgress(progress.Copy)}
	var lag *native.InformationalLag
	if scope.follow {
		lag, err = nativeSampleLag(callCtx, control, m, scope.execution, admit)
	}
	return s.finish(callCtx, serial, scope, started, got, lag, err)
}

func nativeSampleProgress(ctx context.Context, control *native.Client, expected native.ExecutionData, admit func(context.Context) (native.ExecutionData, error)) (*native.Report, error) {
	w, err := nativeSampleAdmission(ctx, expected, admit)
	if err != nil {
		return nil, err
	}
	got, callErr := control.Progress(ctx, w)
	if _, err := nativeSampleAdmission(ctx, expected, admit); err != nil || callErr != nil || got == nil {
		return nil, native.ErrNativeControlUnavailable
	}
	return got, nil
}

func nativeSampleLag(ctx context.Context, control *native.Client, m *v1beta1.Migration, expected native.ExecutionData, admit func(context.Context) (native.ExecutionData, error)) (*native.InformationalLag, error) {
	w, err := nativeSampleAdmission(ctx, expected, admit)
	if err != nil {
		return nil, err
	}
	got, callErr := control.SampleLag(ctx, m, w)
	if _, err := nativeSampleAdmission(ctx, expected, admit); err != nil {
		return nil, errNativeControlAdmission
	}
	if callErr != nil || got == nil || got.LagBytes() == nil || got.AdmissionFingerprint() == ([32]byte{}) {
		return nil, native.ErrNativeControlUnavailable
	}
	return got, nil
}

func nativeSampleAdmission(ctx context.Context, expected native.ExecutionData, admit func(context.Context) (native.ExecutionData, error)) (native.ExecutionData, error) {
	if ctx.Err() != nil {
		return native.ExecutionData{}, native.ErrNativeControlUnavailable
	}
	w, err := admit(ctx)
	w.OperationID = ""
	if err != nil || w != expected || ctx.Err() != nil {
		return native.ExecutionData{}, errNativeControlAdmission
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return native.ExecutionData{}, native.ErrNativeControlUnavailable
	}
	w.OperationID = hex.EncodeToString(nonce[:])
	return w, nil
}

func (s *nativeSampler) finish(ctx context.Context, serial uint64, scope nativeSamplerScope, started time.Time, got nativeSample, lag *native.InformationalLag, sampleErr error) (nativeSample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	completed := s.clock()
	if s.stopped || serial != s.serial || s.scope != scope {
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	s.cancel = nil
	if ctx.Err() != nil || !nativeSampleFresh(completed, started, completed) || sampleErr == errNativeControlAdmission {
		s.clearLocked()
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	s.started, s.completed = started, completed
	if nativeCopyMeasured(got.progress) {
		got.observedAt = completed
	}
	if sampleErr != nil || lag == nil || lag.LagBytes() == nil {
		s.previous, s.current = nativeLagObservation{}, nativeLagObservation{}
	} else {
		value := *lag.LagBytes()
		got.lagBytes = &value
		if scope.baseCopyComplete && value <= scope.threshold {
			s.previous = s.current
			s.current = nativeLagObservation{serial: serial, started: started, completed: completed, fingerprint: lag.AdmissionFingerprint()}
			got.catchUpConfirmed = s.previous.serial != 0 && s.previous.serial != serial && s.previous.fingerprint == s.current.fingerprint && nativeSampleFresh(completed, s.previous.started, s.previous.completed)
		} else {
			s.previous, s.current = nativeLagObservation{}, nativeLagObservation{}
		}
	}
	s.last = got
	if sampleErr != nil {
		sampleErr = native.ErrNativeControlUnavailable
	}
	return cloneNativeSample(got), sampleErr
}

// Observe cannot contribute another checked observation, even when read repeatedly.
func (s *nativeSampler) Observe(m *v1beta1.Migration, expected native.ExecutionData, baseCopyComplete bool, threshold int64) nativeSample {
	if s == nil {
		return nativeSample{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	scope, valid := nativeSamplingScope(m, expected, baseCopyComplete, threshold)
	if s.stopped || !valid || scope != s.scope {
		s.clearLocked()
		return nativeSample{}
	}
	if s.cancel != nil {
		return nativeSample{}
	}
	if !nativeSampleFresh(s.clock(), s.started, s.completed) {
		s.clearLocked()
		return nativeSample{}
	}
	got := cloneNativeSample(s.last)
	if got.catchUpConfirmed && !nativeSampleFresh(s.clock(), s.previous.started, s.previous.completed) {
		got.catchUpConfirmed = false
	}
	return got
}

func (s *nativeSampler) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	s.clearLocked()
}

func (s *nativeSampler) clearLocked() {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.serial++
	s.last = nativeSample{}
	s.started, s.completed = time.Time{}, time.Time{}
	s.previous, s.current = nativeLagObservation{}, nativeLagObservation{}
}

func (s *nativeSampler) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func nativeSamplingScope(m *v1beta1.Migration, w native.ExecutionData, base bool, threshold int64) (nativeSamplerScope, bool) {
	if m == nil || m.Spec.Engine != v1beta1.MigrationEngineRPGMigrate || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || m.UID == "" || m.UID != w.MigrationUID || m.Namespace != w.Namespace || m.Name != w.MigrationName || m.Generation < 1 || m.Generation != w.MigrationGeneration || m.Status.Attempts != w.Attempt || m.Status.JobName != w.JobName || threshold < 0 || m.Status.Phase == v1beta1.PhaseCompleted || m.Status.Phase == v1beta1.PhaseFailed || m.Status.Phase == v1beta1.PhaseSuspended || meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete) || meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed) {
		return nativeSamplerScope{}, false
	}
	w.OperationID = ""
	return nativeSamplerScope{execution: w, baseCopyComplete: base, follow: followEnabled(m), threshold: threshold}, true
}

func nativeSampleFresh(now, started, completed time.Time) bool {
	return !started.IsZero() && !completed.IsZero() && !completed.Before(started) && !now.Before(completed) && completed.Sub(started) <= nativeSampleFreshness && now.Sub(started) <= nativeSampleFreshness
}

func cloneNativeSample(got nativeSample) nativeSample {
	if got.lagBytes != nil {
		value := *got.lagBytes
		got.lagBytes = &value
	}
	got.progress = cloneNativeCopyProgress(got.progress)
	return got
}

func cloneNativeCopyProgress(p *native.CopyProgress) *native.CopyProgress {
	if p == nil {
		return nil
	}
	return &native.CopyProgress{Tables: cloneNativeCounters(p.Tables), Indexes: cloneNativeCounters(p.Indexes), Bytes: cloneNativeCounters(p.Bytes)}
}

func cloneNativeCounters(c *native.Counters) *native.Counters {
	if c == nil {
		return nil
	}
	copy := *c
	if c.Total != nil {
		value := *c.Total
		copy.Total = &value
	}
	if c.Done != nil {
		value := *c.Done
		copy.Done = &value
	}
	return &copy
}
