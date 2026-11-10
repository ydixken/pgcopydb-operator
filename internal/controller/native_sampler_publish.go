package controller

import (
	"context"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type nativeSamplerInput struct {
	migration *v1beta1.Migration
	execution native.ExecutionData
	control   *native.Client
	threshold int64
	admit     func(context.Context) (native.ExecutionData, error)
}

// ObserveNative uses live admission even when it returns an already measured sample.
func (s *Sampler) ObserveNative(key types.NamespacedName, in nativeSamplerInput) (nativeSample, bool) {
	if s == nil {
		return nativeSample{}, false
	}
	in.migration = in.migration.DeepCopy()
	in.execution.OperationID = ""
	s.mu.Lock()
	if s.ctx == nil || s.ctx.Err() != nil {
		s.mu.Unlock()
		return nativeSample{}, false
	}
	if !nativeSamplerInputValid(key, in) {
		s.stopLocked(key)
		s.mu.Unlock()
		return nativeSample{}, true
	}
	run := s.runs[key]
	if run != nil && (run.nativeInput == nil || !nativeSamplerInputsMatch(*run.nativeInput, in)) {
		s.stopLocked(key)
		run = nil
	}
	created := run == nil
	var runCtx context.Context
	if created {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithCancel(s.ctx)
		run = &samplerRun{job: in.execution.JobName, cancel: cancel}
		s.runs[key] = run
	}
	run.nativeInput = &in
	ctx, cancel := context.WithTimeout(s.ctx, nativeSampleFreshness)
	if created {
		go s.loop(runCtx, key, run)
	}
	s.mu.Unlock()
	defer cancel()
	err := nativeSamplerInputAdmission(ctx, in)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runs[key] != run {
		return nativeSample{}, true
	}
	if err != nil || ctx.Err() != nil || s.ctx == nil || s.ctx.Err() != nil {
		s.stopLocked(key)
		return nativeSample{}, true
	}
	// The latch sample used base=false; its DATA survives until the next sample resets scope.
	run.native.mu.Lock()
	base := run.native.scope.baseCopyComplete
	run.native.mu.Unlock()
	got := run.native.Observe(in.migration, in.execution, base, in.threshold)
	metrics.RecordNativeLag(key.Namespace, key.Name, got.lagBytes)
	return got, true
}

// SampleNativeNow only refreshes an existing run, so a stopped run cannot be revived.
func (s *Sampler) SampleNativeNow(ctx context.Context, key types.NamespacedName, in nativeSamplerInput) (nativeSample, error) {
	if s == nil {
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	in.migration = in.migration.DeepCopy()
	in.execution.OperationID = ""
	s.mu.Lock()
	run := s.runs[key]
	if s.ctx == nil || s.ctx.Err() != nil || ctx.Err() != nil || run == nil || run.nativeInput == nil || !nativeSamplerInputValid(key, in) || !nativeSamplerInputsMatch(*run.nativeInput, in) {
		if run != nil && run.nativeInput != nil {
			s.stopLocked(key)
		}
		s.mu.Unlock()
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	run.nativeInput = &in
	s.mu.Unlock()
	return s.sampleNativeRun(ctx, key, run)
}

func (s *Sampler) sampleNativeRun(ctx context.Context, key types.NamespacedName, run *samplerRun) (nativeSample, error) {
	s.mu.Lock()
	if s.ctx == nil || s.ctx.Err() != nil || ctx.Err() != nil || s.runs[key] != run || run.nativeInput == nil {
		s.mu.Unlock()
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	in, base := run.nativeInput, run.nativeBaseCopyComplete
	run.nativeSerial++
	serial := run.nativeSerial
	s.mu.Unlock()
	_, err := run.native.Sample(ctx, in.control, in.migration, in.execution, base, in.threshold, in.admit)
	s.mu.Lock()
	if s.runs[key] != run || run.nativeSerial != serial {
		s.mu.Unlock()
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	latest := run.nativeInput
	s.mu.Unlock()
	admissionCtx, cancel := context.WithTimeout(ctx, nativeSampleFreshness)
	admissionErr := nativeSamplerInputAdmission(admissionCtx, *latest)
	cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runs[key] != run || run.nativeSerial != serial {
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	if admissionErr != nil || ctx.Err() != nil || s.ctx == nil || s.ctx.Err() != nil {
		s.stopLocked(key)
		return nativeSample{}, native.ErrNativeControlUnavailable
	}
	got := run.native.Observe(in.migration, in.execution, base, in.threshold)
	if !run.nativeBaseCopyComplete && nativeBaseCopyReady(got) {
		run.nativeBaseCopyComplete = true
	}
	metrics.RecordNativeLag(key.Namespace, key.Name, got.lagBytes)
	return got, err
}

func nativeSamplerInputAdmission(ctx context.Context, in nativeSamplerInput) error {
	if in.admit == nil || ctx.Err() != nil {
		return errNativeControlAdmission
	}
	w, err := in.admit(ctx)
	w.OperationID = ""
	if err != nil || w != in.execution || ctx.Err() != nil {
		return errNativeControlAdmission
	}
	return nil
}

func nativeSamplerInputValid(key types.NamespacedName, in nativeSamplerInput) bool {
	_, valid := nativeSamplingScope(in.migration, in.execution, false, in.threshold)
	return valid && in.control != nil && in.admit != nil && client.ObjectKeyFromObject(in.migration) == key && native.NativeReceiptsMatch(in.migration, in.execution.WorkerJobUID, in.execution.PVCUID)
}

func nativeSamplerInputsMatch(a, b nativeSamplerInput) bool {
	aa, aok := nativeSamplingScope(a.migration, a.execution, false, a.threshold)
	bb, bok := nativeSamplingScope(b.migration, b.execution, false, b.threshold)
	return aok && bok && aa == bb && a.control == b.control && nativeControlEnd(a.migration) == nativeControlEnd(b.migration) &&
		a.migration.Status.NativeJobUID == b.migration.Status.NativeJobUID && a.migration.Status.NativePVCUID == b.migration.Status.NativePVCUID &&
		apiequality.Semantic.DeepEqual(a.migration.Spec, b.migration.Spec) && apiequality.Semantic.DeepEqual(a.migration.Labels, b.migration.Labels) && apiequality.Semantic.DeepEqual(a.migration.Annotations, b.migration.Annotations)
}

func nativeBaseCopyReady(got nativeSample) bool {
	if got.phase != "streaming" || got.progress == nil {
		return false
	}
	for _, c := range []*native.Counters{got.progress.Tables, got.progress.Indexes, got.progress.Bytes} {
		if c == nil || c.Total == nil || c.Done == nil || *c.Total < 0 || *c.Total != *c.Done {
			return false
		}
	}
	return true
}

func nativeCopyMeasured(p *native.CopyProgress) bool {
	if p != nil {
		for _, c := range []*native.Counters{p.Tables, p.Indexes, p.Bytes} {
			if c != nil && (c.Total != nil || c.Done != nil) {
				return true
			}
		}
	}
	return false
}

func (r *MigrationReconciler) observeNativeProgress(ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job, pvc *corev1.PersistentVolumeClaim) (nativeSample, bool, error) {
	if m == nil || worker == nil || pvc == nil {
		return nativeSample{}, false, errNativeControlAdmission
	}
	key := client.ObjectKeyFromObject(m)
	facts, err := r.admitNativeControlFacts(ctx, m, worker, pvc)
	if err != nil || r.NativeControl == nil {
		r.stopSampling(key)
		return nativeSample{}, false, errNativeControlAdmission
	}
	m, worker, pvc = facts.migration.DeepCopy(), facts.worker.DeepCopy(), facts.pvc.DeepCopy()
	facts.data.OperationID = ""
	in := nativeSamplerInput{migration: m, execution: facts.data, control: r.NativeControl, threshold: maxCatchupLagBytes(m), admit: func(ctx context.Context) (native.ExecutionData, error) {
		return r.admitNativeWorkerExecution(ctx, m, worker, pvc)
	}}
	got, running := r.Sampler.ObserveNative(key, in)
	return got, running, nil
}
