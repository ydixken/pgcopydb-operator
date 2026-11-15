package controller

import (
	"context"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// M7-RUN5 O-1: the automatic cutover pass returned before CaughtUp=True was recorded.
func TestNativeReconcileCutoverRecordsCaughtUp(t *testing.T) {
	for _, mode := range []v1beta1.CutoverMode{v1beta1.CutoverAutomatic, v1beta1.CutoverManual} {
		t.Run(string(mode), func(t *testing.T) {
			f, s, in := nativeDispatchSampler(t)
			f.m.Spec.Cutover.Mode = mode
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			in.migration = f.m.DeepCopy()
			key := client.ObjectKeyFromObject(f.m)
			s.runs[key].nativeInput = &in
			for range 3 {
				if _, err := s.SampleNativeNow(context.Background(), key, in); err != nil {
					t.Fatal(err)
				}
				f.advance(time.Second)
			}
			var recorded *metav1.Condition
			f.afterExec = func(source bool) {
				if source && recorded == nil {
					recorded = meta.FindStatusCondition(f.current().Status.Conditions, v1beta1.ConditionCaughtUp)
				}
			}
			if _, err := nativeDispatch(t, f.r, f.m); err != nil || f.heads != 1 {
				t.Fatalf("cutover pass did not select E: %v heads=%d", err, f.heads)
			}
			if recorded == nil || recorded.Status != metav1.ConditionTrue || recorded.Reason != reasonLagBelowThreshold {
				t.Fatalf("E was selected before CaughtUp=True was recorded: %+v", recorded)
			}
			got := f.current()
			if c := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ConditionCaughtUp); c == nil || c.Status != metav1.ConditionTrue || got.Status.Phase != v1beta1.PhaseCuttingOver || nativeControlEnd(got) != nativeControlTestEnd {
				t.Fatalf("cutover pass left CaughtUp or E unrecorded: %+v", got.Status)
			}
		})
	}
}
