package controller

import (
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func nativeBlockedEvents(t *testing.T, rec *events.FakeRecorder) []string {
	t.Helper()
	var got []string
	for {
		select {
		case e := <-rec.Events:
			got = append(got, e)
		default:
			return got
		}
	}
}

func nativeBlockedPatch(t *testing.T, m, base *v1beta1.Migration) string {
	t.Helper()
	data, err := client.MergeFrom(base).Data(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestNativeBlockedEventsOnTransitionAndReasonChangeOnly(t *testing.T) {
	rec := events.NewFakeRecorder(10)
	r := &MigrationReconciler{Recorder: rec}
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Generation: 4}}

	r.setNativeBlocked(m, v1beta1.ReasonForeignObject, "Job default/w-run-2 is not owned by this Migration")
	c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionBlocked)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != v1beta1.ReasonForeignObject || c.ObservedGeneration != 4 ||
		c.Message != "Job default/w-run-2 is not owned by this Migration" {
		t.Fatalf("Blocked = %+v", c)
	}
	if got := nativeBlockedEvents(t, rec); len(got) != 1 || got[0] != "Warning Blocked Job default/w-run-2 is not owned by this Migration" {
		t.Fatalf("events on the transition = %q", got)
	}
	since := c.LastTransitionTime

	r.setNativeBlocked(m, v1beta1.ReasonWorkerPodRemains, "pod default/w-run-1-x on node n1 is terminating")
	c = meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionBlocked)
	if c.Reason != v1beta1.ReasonWorkerPodRemains || c.LastTransitionTime != since {
		t.Fatalf("Blocked after a reason change = %+v, want the new reason and the old transition time", c)
	}
	if got := nativeBlockedEvents(t, rec); len(got) != 1 || !strings.HasPrefix(got[0], "Warning Blocked pod default/w-run-1-x") {
		t.Fatalf("events on the reason change = %q", got)
	}

	r.setNativeBlocked(m, v1beta1.ReasonWorkerPodRemains, "pod default/w-run-1-x on node n2 is terminating")
	if got := nativeBlockedEvents(t, rec); len(got) != 0 {
		t.Fatalf("a changed message under the same reason emitted %q", got)
	}
}

// A pass that finds the same hold again must leave nothing to patch and emit nothing.
func TestNativeBlockedRepeatWritesNothing(t *testing.T) {
	rec := events.NewFakeRecorder(10)
	r := &MigrationReconciler{Recorder: rec}
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	r.setNativeBlocked(m, v1beta1.ReasonForegroundDeletion, "waiting for the garbage collector")
	_ = nativeBlockedEvents(t, rec)

	base := m.DeepCopy()
	r.setNativeBlocked(m, v1beta1.ReasonForegroundDeletion, "waiting for the garbage collector")
	if patch := nativeBlockedPatch(t, m, base); patch != "{}" {
		t.Fatalf("a repeated hold patches %s", patch)
	}
	if got := nativeBlockedEvents(t, rec); len(got) != 0 {
		t.Fatalf("a repeated hold emitted %q", got)
	}

	r.clearNativeBlocked(m)
	base = m.DeepCopy()
	r.clearNativeBlocked(m)
	if patch := nativeBlockedPatch(t, m, base); patch != "{}" {
		t.Fatalf("a repeated clear patches %s", patch)
	}
}

func TestNativeBlockedClear(t *testing.T) {
	rec := events.NewFakeRecorder(10)
	r := &MigrationReconciler{Recorder: rec}

	never := &v1beta1.Migration{}
	r.clearNativeBlocked(never)
	if len(never.Status.Conditions) != 0 {
		t.Fatalf("a Migration that never blocked gained %+v", never.Status.Conditions)
	}

	m := &v1beta1.Migration{}
	r.setNativeBlocked(m, v1beta1.ReasonCleanupUnavailable, "inventory unreachable")
	r.clearNativeBlocked(m)
	c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionBlocked)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1beta1.ReasonProceeding {
		t.Fatalf("Blocked after clear = %+v, want False/%s", c, v1beta1.ReasonProceeding)
	}
	_ = nativeBlockedEvents(t, rec)

	r.setNativeBlocked(m, v1beta1.ReasonCleanupUnavailable, "inventory unreachable")
	if got := nativeBlockedEvents(t, rec); len(got) != 1 {
		t.Fatalf("blocking again after a clear emitted %q, want one event", got)
	}
}

func TestNativeCleanupFailedMessage(t *testing.T) {
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{UID: "0f8fad5b-d9cb-469f-a165-70867728950e"}}
	m.Spec.Source.Host, m.Spec.Source.Database = "source.example.com", testDB
	for _, reason := range []string{v1beta1.ReasonNamespaceTerminating, v1beta1.ReasonWorkVolumeLost} {
		msg, err := nativeCleanupFailedMessage(m, reason)
		if err != nil {
			t.Fatalf("%s: %v", reason, err)
		}
		// The engine names them rpgm_s_<run>_<generation> and rpgm_p_<run>_<generation> (src/catalog/follow.rs).
		for _, want := range []string{
			"rpgm_s_0f8fad5bd9cb469fa16570867728950e_*", "rpgm_p_0f8fad5bd9cb469fa16570867728950e_*",
			`host "source.example.com"`, `database "` + testDB + `"`,
		} {
			if !strings.Contains(msg, want) {
				t.Fatalf("%s message %q lacks %q", reason, msg, want)
			}
		}
	}
	terminating, _ := nativeCleanupFailedMessage(m, v1beta1.ReasonNamespaceTerminating)
	lost, _ := nativeCleanupFailedMessage(m, v1beta1.ReasonWorkVolumeLost)
	if terminating == lost {
		t.Fatal("both reasons give the same cause")
	}

	for name, tc := range map[string]struct {
		uid, reason string
	}{
		"blocked reason":   {uid: string(m.UID), reason: v1beta1.ReasonForeignObject},
		"empty reason":     {uid: string(m.UID), reason: ""},
		"missing UID":      {uid: "", reason: v1beta1.ReasonWorkVolumeLost},
		"uppercase UID":    {uid: strings.ToUpper(string(m.UID)), reason: v1beta1.ReasonWorkVolumeLost},
		"non-canonical ID": {uid: "0f8fad5bd9cb469fa16570867728950e", reason: v1beta1.ReasonNamespaceTerminating},
	} {
		t.Run(name, func(t *testing.T) {
			bad := m.DeepCopy()
			bad.UID = types.UID(tc.uid)
			if msg, err := nativeCleanupFailedMessage(bad, tc.reason); err == nil || msg != "" {
				t.Fatalf("message = %q, err = %v, want a refusal", msg, err)
			}
		})
	}
}
