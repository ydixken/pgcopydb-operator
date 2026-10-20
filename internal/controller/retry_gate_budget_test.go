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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func TestCountedAttempts(t *testing.T) {
	m := &v1beta1.Migration{}
	m.Spec.BackoffLimit = 1
	m.Status.Attempts, m.Status.ExemptAttempts = 3, 1
	if got := countedAttempts(m); got != 2 {
		t.Errorf("countedAttempts(3 attempts, 1 exempt) = %d, want 2", got)
	}

	// With one exempt attempt, attempts 2 and 3 are counted 1 and 2.
	for _, tc := range []struct {
		attempts int32
		spent    bool
	}{{2, false}, {3, true}} {
		m.Status.Attempts = tc.attempts
		if got := budgetSpent(m); got != tc.spent {
			t.Errorf("budgetSpent(backoffLimit 1, counted %d) = %v, want %v", countedAttempts(m), got, tc.spent)
		}
	}

	m.Status.Attempts = 3
	want := "retry budget exhausted after 3 attempts (1 exempt)"
	if got := budgetMessage(m); got != want {
		t.Errorf("budgetMessage = %q, want %q", got, want)
	}
	m.Status.RetryGate = &v1beta1.RetryGateStatus{FailedAttempt: 3, Failure: "exit 1"}
	want += ": attempt 3 failed: exit 1"
	if got := budgetMessage(m); got != want {
		t.Errorf("budgetMessage with retryGate = %q, want %q", got, want)
	}
}

func TestExemptionVerdict(t *testing.T) {
	failedAt := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	before := func(s int64) int64 { return failedAt.Unix() - s }
	for _, tc := range []struct {
		name string
		c    gateCensus
		want string
	}{
		{"wait began 29 s before the failure", gateCensus{Total: 1, SyncRep: 1, Named: true, SyncRepSince: before(29)}, v1beta1.RetryGateCounted},
		{"wait began 30 s before the failure", gateCensus{Total: 1, SyncRep: 1, Named: true, SyncRepSince: before(30)}, v1beta1.RetryGateExempt},
		{"wait began 300 s before the failure", gateCensus{Total: 1, SyncRep: 1, Named: true, SyncRepSince: before(300)}, v1beta1.RetryGateExempt},
		{"a pinned application_name is no proof", gateCensus{Total: 1, SyncRep: 1, Named: false, SyncRepSince: before(300)}, v1beta1.RetryGateCounted},
		{"no session in SyncRep", gateCensus{Total: 1, SyncRep: 0, Named: true, SyncRepSince: before(300)}, v1beta1.RetryGateCounted},
		{"no wait start", gateCensus{Total: 1, SyncRep: 1, Named: true, SyncRepSince: 0}, v1beta1.RetryGateCounted},
		{"an empty census", gateCensus{Named: true}, v1beta1.RetryGateCounted},
	} {
		if got := exemptionVerdict(tc.c, failedAt); got != tc.want {
			t.Errorf("%s: exemptionVerdict = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestVerdictOverdue(t *testing.T) {
	failedAt := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	undecided := &v1beta1.RetryGateStatus{FailedAttempt: 1, FailedAt: metav1.NewTime(failedAt)}
	decided := undecided.DeepCopy()
	decided.Exemption = v1beta1.RetryGateCounted
	for _, tc := range []struct {
		name string
		rg   *v1beta1.RetryGateStatus
		now  time.Time
		want bool
	}{
		{"undecided at 14m59s", undecided, failedAt.Add(14*time.Minute + 59*time.Second), false},
		{"undecided at 15m", undecided, failedAt.Add(15 * time.Minute), true},
		{"decided at 15m", decided, failedAt.Add(15 * time.Minute), false},
		{"no failed attempt", nil, failedAt.Add(15 * time.Minute), false},
	} {
		if got := verdictOverdue(tc.rg, tc.now); got != tc.want {
			t.Errorf("%s: verdictOverdue = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestCopyDoneForGate(t *testing.T) {
	cond := func(typ string, status metav1.ConditionStatus, reason string) func(*v1beta1.Migration) {
		return func(m *v1beta1.Migration) {
			m.Status.Conditions = append(m.Status.Conditions, metav1.Condition{Type: typ, Status: status, Reason: reason})
		}
	}
	for _, tc := range []struct {
		name string
		set  func(*v1beta1.Migration)
		want bool
	}{
		{"none of them", func(*v1beta1.Migration) {}, false},
		{"phase Finalizing alone", func(m *v1beta1.Migration) { m.Status.Phase = v1beta1.PhaseFinalizing }, false},
		{"CloneCompleted True", cond(v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "CloneDone"), true},
		{"TablesEmptyOnTarget latch", cond(v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, reasonTablesEmptyOnTarget), true},
		{"endpos set", func(m *v1beta1.Migration) { m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: "0/30"} }, true},
		{"CutoverCompleted True", cond(v1beta1.ConditionCutoverComplete, metav1.ConditionTrue, "CutoverDone"), true},
		{"Streaming True", cond(v1beta1.ConditionStreaming, metav1.ConditionTrue, "Replaying"), true},
		{"retryGate.cloneDoneSeen", func(m *v1beta1.Migration) {
			m.Status.RetryGate = &v1beta1.RetryGateStatus{FailedAttempt: 1, CloneDoneSeen: true}
		}, true},
	} {
		m := &v1beta1.Migration{}
		tc.set(m)
		if got := copyDoneForGate(m); got != tc.want {
			t.Errorf("%s: copyDoneForGate = %v, want %v", tc.name, got, tc.want)
		}
	}
}
