package controller

import (
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const nativeProgressStreamingPhase = "streaming"

func (r *MigrationReconciler) applyNativeProgress(m *v1beta1.Migration, got nativeSample) {
	m.Status.Progress = nil
	if nativeCopyMeasured(got.progress) && !got.observedAt.IsZero() {
		p := &v1beta1.CloneProgress{ObservedAt: &metav1.Time{Time: got.observedAt}}
		if c := got.progress.Tables; c != nil {
			if c.Total != nil {
				p.TablesTotal = *c.Total
			}
			if c.Done != nil {
				p.TablesDone = *c.Done
			}
		}
		if c := got.progress.Indexes; c != nil {
			if c.Total != nil {
				p.IndexesTotal = *c.Total
			}
			if c.Done != nil {
				p.IndexesDone = *c.Done
			}
		}
		if c := got.progress.Bytes; c != nil {
			if c.Total != nil {
				p.BytesTotal = resource.NewQuantity(*c.Total, resource.BinarySI)
			}
			if c.Done != nil {
				p.BytesDone = resource.NewQuantity(*c.Done, resource.BinarySI)
			}
		}
		m.Status.Progress = p
	}
	end := nativeControlEnd(m)
	m.Status.Replication = nil
	if followEnabled(m) || end != "" {
		m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: end}
		if got.lagBytes != nil {
			value := *got.lagBytes
			m.Status.Replication.LagBytes = &value
		}
	}
	m.Status.Phase = v1beta1.PhaseCloning
	switch got.phase {
	case "finalizing":
		m.Status.Phase = v1beta1.PhaseFinalizing
	case nativeProgressStreamingPhase:
		if followEnabled(m) {
			m.Status.Phase = v1beta1.PhaseStreaming
		}
	}
	if !followEnabled(m) {
		return
	}
	streaming := metav1.ConditionUnknown
	if got.phase == nativeProgressStreamingPhase || got.phase == "draining" {
		streaming = metav1.ConditionTrue
	}
	r.setCondition(m, v1beta1.ConditionStreaming, streaming, "NativeProgress", "Native worker phase is informational")
	switch {
	case got.catchUpConfirmed:
		r.setCondition(m, v1beta1.ConditionCaughtUp, metav1.ConditionTrue, reasonLagBelowThreshold, "Two fresh native lag observations qualified after base copy")
		m.Status.Phase = v1beta1.PhaseCutoverPending
	case got.lagBytes == nil:
		r.setCondition(m, v1beta1.ConditionCaughtUp, metav1.ConditionUnknown, "NativeLagUnknown", "Native source lag is unavailable")
	default:
		r.setCondition(m, v1beta1.ConditionCaughtUp, metav1.ConditionFalse, reasonLagging, "Native catch-up requires base copy and two later fresh qualifying observations")
	}
	if end != "" {
		m.Status.Phase = v1beta1.PhaseCuttingOver
	}
}
