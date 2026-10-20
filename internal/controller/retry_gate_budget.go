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
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// verdictDeadline turns a verdict still undecided this long after failedAt into Counted.
// It matches the 15 minutes the CommitsBlocked alert waits before it fires.
const verdictDeadline = 15 * time.Minute

// gateCensus is a gate run's census line. SyncRepSince is when the oldest SyncRep wait
// began, in epoch seconds of the target's clock, and 0 with none.
type gateCensus struct {
	Total, SyncRep, GateWaiting, SyncRepSince int64
	Named                                     bool
}

// countedAttempts are the attempts that spend the retry budget.
func countedAttempts(m *v1beta1.Migration) int32 {
	return m.Status.Attempts - m.Status.ExemptAttempts
}

// budgetSpent reports that no further attempt may start: backoffLimit is the number
// of retries, so backoffLimit+1 counted attempts.
func budgetSpent(m *v1beta1.Migration) bool {
	return countedAttempts(m) >= m.Spec.BackoffLimit+1
}

// budgetMessage is the failure message of every path that spends the budget. It names
// the gate's failed attempt, because the verdict can come after that attempt's events expired.
func budgetMessage(m *v1beta1.Migration) string {
	msg := fmt.Sprintf("retry budget exhausted after %d attempts (%d exempt)", m.Status.Attempts, m.Status.ExemptAttempts)
	if rg := m.Status.RetryGate; rg != nil {
		msg += fmt.Sprintf(": attempt %d failed: %s", rg.FailedAttempt, rg.Failure)
	}
	return msg
}

// exemptionVerdict is Exempt only for a named SyncRep wait that began syncRepThreshold or
// more before failedAt: it outlived the worker. A catch-up wait that began just before
// an unrelated failure counts, and so does any wait under a pinned application_name.
func exemptionVerdict(c gateCensus, failedAt time.Time) string {
	if c.Named && c.SyncRep > 0 && c.SyncRepSince > 0 &&
		!time.Unix(c.SyncRepSince, 0).After(failedAt.Add(-syncRepThreshold)) {
		return v1beta1.RetryGateExempt
	}
	return v1beta1.RetryGateCounted
}

// verdictOverdue reports a verdict still undecided verdictDeadline after the failure.
func verdictOverdue(rg *v1beta1.RetryGateStatus, now time.Time) bool {
	return rg != nil && rg.Exemption == "" && !now.Before(rg.FailedAt.Add(verdictDeadline))
}

// copyDoneForGate reports a finished base copy, after which CDC apply commits with
// synchronous_commit=on whatever the startup option says. It ignores Finalizing, unlike
// hintFor, because post-data under pg_restore is still copy work.
func copyDoneForGate(m *v1beta1.Migration) bool {
	return meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted) ||
		tablesEmptySeen(m) ||
		attemptPhase(m) != v1beta1.PhaseCloning ||
		(m.Status.RetryGate != nil && m.Status.RetryGate.CloneDoneSeen)
}
