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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

// eventLog keeps each Event's action, which events.FakeRecorder drops.
type eventLog struct{ got []string }

func (l *eventLog) Eventf(_, _ runtime.Object, eventtype, reason, action, note string, args ...any) {
	l.got = append(l.got, strings.Join([]string{eventtype, reason, action, fmt.Sprintf(note, args...)}, " "))
}

func TestApplyCommitsBlocked(t *testing.T) {
	over := &progress.SyncRepWait{Waiting: 7, OldestSeconds: 93, PgcopydbNamed: 5}
	atThreshold := &progress.SyncRepWait{Waiting: 1, OldestSeconds: 30, PgcopydbNamed: 1}
	under := &progress.SyncRepWait{Waiting: 2, OldestSeconds: 29, PgcopydbNamed: 2}
	at := time.Unix(1000, 0)
	parsed := func(w *progress.SyncRepWait, confirmed bool) sampled {
		return sampled{sample: &progress.Sample{SyncRep: w}, at: at, syncRepConfirmed: confirmed}
	}
	priorReason := map[metav1.ConditionStatus]string{
		metav1.ConditionTrue: reasonSyncRepWait, metav1.ConditionFalse: reasonNoSyncRepWait,
		metav1.ConditionUnknown: reasonSyncRepUnobserved,
	}
	const absent, keep = metav1.ConditionStatus(""), "keep"

	for _, tc := range []struct {
		name   string
		prior  metav1.ConditionStatus
		got    sampled
		want   metav1.ConditionStatus
		reason string // keep: the prior condition stands untouched
		events int
	}{
		{"no sample yet leaves it absent", absent, sampled{}, absent, "", 0},
		{"no sample yet keeps False", metav1.ConditionFalse, sampled{}, metav1.ConditionFalse, keep, 0},
		{"no sample yet keeps Unknown", metav1.ConditionUnknown, sampled{}, metav1.ConditionUnknown, keep, 0},
		{"confirmed raises it", absent, parsed(over, true), metav1.ConditionTrue, reasonSyncRepWait, 1},
		{"confirmed at the threshold raises it", metav1.ConditionFalse, parsed(atThreshold, true), metav1.ConditionTrue, reasonSyncRepWait, 1},
		{"confirmed while True announces nothing", metav1.ConditionTrue, parsed(over, true), metav1.ConditionTrue, reasonSyncRepWait, 0},
		{"over but unconfirmed leaves it absent", absent, parsed(over, false), absent, "", 0},
		{"over but unconfirmed keeps False", metav1.ConditionFalse, parsed(over, false), metav1.ConditionFalse, keep, 0},
		{"over but unconfirmed keeps Unknown", metav1.ConditionUnknown, parsed(over, false), metav1.ConditionUnknown, keep, 0},
		{"under the threshold sets False", absent, parsed(under, false), metav1.ConditionFalse, reasonNoSyncRepWait, 0},
		{"under the threshold clears True", metav1.ConditionTrue, parsed(under, false), metav1.ConditionFalse, reasonNoSyncRepWait, 0},
		{"no waiter clears True", metav1.ConditionTrue, parsed(&progress.SyncRepWait{}, false), metav1.ConditionFalse, reasonNoSyncRepWait, 0},
		{"a parsed sample settles Unknown", metav1.ConditionUnknown, parsed(under, false), metav1.ConditionFalse, reasonNoSyncRepWait, 0},
		{"nil SyncRep from absent", absent, parsed(nil, false), metav1.ConditionUnknown, reasonSyncRepUnobserved, 0},
		{"nil SyncRep from False", metav1.ConditionFalse, parsed(nil, false), metav1.ConditionUnknown, reasonSyncRepUnobserved, 0},
		{"no sample from absent", absent, sampled{at: at}, metav1.ConditionUnknown, reasonSyncRepUnobserved, 0},
		{"no sample from False", metav1.ConditionFalse, sampled{at: at}, metav1.ConditionUnknown, reasonSyncRepUnobserved, 0},
		{"exec error from False", metav1.ConditionFalse, sampled{err: errors.New("exec refused"), at: at}, metav1.ConditionUnknown, reasonSyncRepUnobserved, 0},
		{"nil SyncRep keeps True", metav1.ConditionTrue, parsed(nil, false), metav1.ConditionTrue, keep, 0},
		{"no sample keeps True", metav1.ConditionTrue, sampled{at: at}, metav1.ConditionTrue, keep, 0},
		{"exec error keeps True", metav1.ConditionTrue, sampled{err: errors.New("exec refused"), at: at}, metav1.ConditionTrue, keep, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &eventLog{}
			r := &MigrationReconciler{Recorder: rec}
			m := &v1beta1.Migration{}
			if tc.prior != absent {
				r.setCondition(m, v1beta1.ConditionCommitsBlocked, tc.prior, priorReason[tc.prior], "prior message")
			}
			before := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCommitsBlocked)
			if before != nil {
				before = before.DeepCopy()
			}

			r.applyCommitsBlocked(m, tc.got)

			c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCommitsBlocked)
			switch {
			case tc.want == absent:
				if c != nil {
					t.Fatalf("condition = %+v, want none", *c)
				}
			case c == nil:
				t.Fatalf("no condition, want %s/%s", tc.want, tc.reason)
			case tc.reason == keep:
				if *c != *before {
					t.Fatalf("condition = %+v, want it untouched: %+v", *c, *before)
				}
			case c.Status != tc.want || c.Reason != tc.reason:
				t.Fatalf("condition = %s/%s, want %s/%s", c.Status, c.Reason, tc.want, tc.reason)
			}
			if len(rec.got) != tc.events {
				t.Fatalf("events = %q, want %d", rec.got, tc.events)
			}
		})
	}

	t.Run("the True message names the counts, the age and what the commits wait for", func(t *testing.T) {
		rec := &eventLog{}
		r := &MigrationReconciler{Recorder: rec}
		m := &v1beta1.Migration{}
		r.applyCommitsBlocked(m, parsed(over, true))
		c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCommitsBlocked)
		if c == nil {
			t.Fatal("no condition")
		}
		for _, want := range []string{": 7,", "5 of them pgcopydb", "93s", "until a synchronous standby confirms"} {
			if !strings.Contains(c.Message, want) {
				t.Errorf("message %q lacks %q", c.Message, want)
			}
		}
		if len(rec.got) != 1 || rec.got[0] != "Warning CommitsBlocked Sample "+c.Message {
			t.Fatalf("events = %q, want one Warning CommitsBlocked Sample carrying the message", rec.got)
		}
	})

	t.Run("one Warning Event per entry into True, none on clear", func(t *testing.T) {
		rec := &eventLog{}
		r := &MigrationReconciler{Recorder: rec}
		m := &v1beta1.Migration{}
		for i, step := range []struct {
			got    sampled
			events int
		}{
			{parsed(over, true), 1},
			{parsed(over, true), 1},
			{parsed(nil, false), 1},
			{parsed(under, false), 1},
			{parsed(over, false), 1},
			{parsed(over, true), 2},
		} {
			r.applyCommitsBlocked(m, step.got)
			if len(rec.got) != step.events {
				t.Fatalf("step %d: events = %q, want %d in all", i, rec.got, step.events)
			}
		}
		for _, e := range rec.got {
			if !strings.HasPrefix(e, "Warning CommitsBlocked Sample ") {
				t.Errorf("event %q, want Warning CommitsBlocked with action Sample", e)
			}
		}
	})

	t.Run("syncRepOver counts a waiter at or over the threshold", func(t *testing.T) {
		for _, tc := range []struct {
			w    *progress.SyncRepWait
			want bool
		}{
			{nil, false},
			{&progress.SyncRepWait{}, false},
			{under, false},
			{atThreshold, true},
			{over, true},
			// No waiter has no age; a stray figure must not raise anything.
			{&progress.SyncRepWait{OldestSeconds: 45}, false},
		} {
			if got := syncRepOver(tc.w); got != tc.want {
				t.Errorf("syncRepOver(%+v) = %v, want %v", tc.w, got, tc.want)
			}
		}
	})
}
