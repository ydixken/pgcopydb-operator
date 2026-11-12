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

package e2e

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var nativeFaultUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// nativeFaultRunID is the native run ID the operator derives from a Migration UID.
func nativeFaultRunID(uid types.UID) (string, error) {
	if !nativeFaultUID.MatchString(string(uid)) || strings.Trim(string(uid), "0-") == "" {
		return "", fmt.Errorf("migration UID %q is not a canonical nonzero UUID", uid)
	}
	return strings.ReplaceAll(string(uid), "-", ""), nil
}

// nativeFaultOutcome is the invariant every fault ends in: Completed with the cutover
// done and the owned slot dropped (resumed), or Failed claiming nothing done while the
// owned slot still holds the unapplied WAL (failed closed).
func nativeFaultOutcome(m *v1beta1.Migration, ownedSlots int) (bool, error) {
	conditionTrue := func(kind string) bool { return apimeta.IsStatusConditionTrue(m.Status.Conditions, kind) }
	switch m.Status.Phase {
	case v1beta1.PhaseCompleted:
		if !conditionTrue(v1beta1.ConditionComplete) || !conditionTrue(v1beta1.ConditionCutoverComplete) ||
			conditionTrue(v1beta1.ConditionFailed) {
			return false, errors.New("completed without a finished cutover")
		}
		if ownedSlots != 0 {
			return false, fmt.Errorf("completed with %d owned slots left on the source", ownedSlots)
		}
		return true, nil
	case v1beta1.PhaseFailed:
		failed := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		if failed == nil || failed.Status != metav1.ConditionTrue || failed.Reason == "" {
			return false, errors.New("failed without a Failed reason")
		}
		if conditionTrue(v1beta1.ConditionComplete) || conditionTrue(v1beta1.ConditionCutoverComplete) {
			return false, errors.New("failed while claiming completion")
		}
		if ownedSlots != 1 {
			return false, fmt.Errorf("failed with %d owned slots, so the unapplied WAL is not retained", ownedSlots)
		}
		return false, nil
	}
	return false, fmt.Errorf("phase %q is not terminal", m.Status.Phase)
}

// nativeFaultMarkers counts one marker set on one side; firstMissing is 0 when none is missing.
type nativeFaultMarkers struct{ total, distinct, highest, firstMissing int }

func parseNativeFaultMarkers(out string) (nativeFaultMarkers, error) {
	var s nativeFaultMarkers
	fields := strings.Split(out, "|")
	counts := []*int{&s.total, &s.distinct, &s.highest, &s.firstMissing}
	if len(fields) != len(counts) {
		return s, fmt.Errorf("marker query returned %q", out)
	}
	for i, count := range counts {
		n, err := strconv.Atoi(fields[i])
		if err != nil || n < 0 {
			return s, fmt.Errorf("marker query returned %q", out)
		}
		*count = n
	}
	return s, nil
}

// check holds a side to rows 1..last: never twice and never one the source lacks;
// a resumed migration also lost none.
func (s nativeFaultMarkers) check(last int, resumed bool) error {
	switch {
	case last <= 0:
		return errors.New("the source committed no marker rows, so a loss cannot show")
	case s.total != s.distinct:
		return fmt.Errorf("%d marker rows applied more than once", s.total-s.distinct)
	case s.highest > last:
		return fmt.Errorf("marker %d was never committed on the source", s.highest)
	case resumed && (s.total != last || s.firstMissing != 0):
		return fmt.Errorf("%d of %d marker rows, first missing %d", s.total, last, s.firstMissing)
	}
	return nil
}

func TestNativeFaultRunID(t *testing.T) {
	got, err := nativeFaultRunID("0123abcd-4567-89ef-0123-456789abcdef")
	if err != nil || got != "0123abcd456789ef0123456789abcdef" {
		t.Fatalf("canonical UID = %q, %v", got, err)
	}
	for _, uid := range []types.UID{
		"", "0123ABCD-4567-89ef-0123-456789abcdef", "0123abcd456789ef0123456789abcdef",
		"00000000-0000-0000-0000-000000000000", "0123abcd-4567-89ef-0123-456789abcde",
	} {
		if _, err := nativeFaultRunID(uid); err == nil {
			t.Errorf("UID %q admitted", uid)
		}
	}
}

func TestNativeFaultOutcome(t *testing.T) {
	condition := func(kind string, status metav1.ConditionStatus, reason string) metav1.Condition {
		return metav1.Condition{Type: kind, Status: status, Reason: reason}
	}
	completed := []metav1.Condition{
		condition(v1beta1.ConditionComplete, metav1.ConditionTrue, "Done"),
		condition(v1beta1.ConditionCutoverComplete, metav1.ConditionTrue, "Done"),
	}
	failed := []metav1.Condition{condition(v1beta1.ConditionFailed, metav1.ConditionTrue, "NativeBadState")}
	for _, tc := range []struct {
		name       string
		phase      v1beta1.MigrationPhase
		conditions []metav1.Condition
		slots      int
		resumed    bool
		ok         bool
	}{
		{"resumed", v1beta1.PhaseCompleted, completed, 0, true, true},
		{"completed with the owned slot left", v1beta1.PhaseCompleted, completed, 1, false, false},
		{"completed without cutover", v1beta1.PhaseCompleted, completed[:1], 0, false, false},
		{"completed and failed", v1beta1.PhaseCompleted, append(completed, failed...), 0, false, false},
		{"failed closed", v1beta1.PhaseFailed, failed, 1, false, true},
		{"failed without the owned slot", v1beta1.PhaseFailed, failed, 0, false, false},
		{"failed claiming cutover", v1beta1.PhaseFailed, append(failed, completed[1]), 1, false, false},
		{"failed without reason", v1beta1.PhaseFailed,
			[]metav1.Condition{condition(v1beta1.ConditionFailed, metav1.ConditionTrue, "")}, 1, false, false},
		{"failed without condition", v1beta1.PhaseFailed, nil, 1, false, false},
		{"not terminal", v1beta1.PhaseStreaming, nil, 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &v1beta1.Migration{Status: v1beta1.MigrationStatus{Phase: tc.phase, Conditions: tc.conditions}}
			resumed, err := nativeFaultOutcome(m, tc.slots)
			if (err == nil) != tc.ok || resumed != tc.resumed {
				t.Fatalf("outcome = %t, %v", resumed, err)
			}
		})
	}
}

func TestNativeFaultMarkers(t *testing.T) {
	for _, out := range []string{"", "1|1|1", "1|1|1|x", "1|1|-1|0", "1|1|1|0|0"} {
		if _, err := parseNativeFaultMarkers(out); err == nil {
			t.Errorf("marker output %q admitted", out)
		}
	}
	for _, tc := range []struct {
		name    string
		out     string
		last    int
		resumed bool
		ok      bool
	}{
		{"every row once", "5|5|5|0", 5, true, true},
		{"duplicate row", "6|5|5|0", 5, true, false},
		{"phantom row", "6|6|6|0", 5, true, false},
		{"lost tail", "4|4|4|5", 5, true, false},
		{"lost hole", "4|4|5|3", 5, true, false},
		{"failed closed with a partial tail", "3|3|3|4", 5, false, true},
		{"failed closed with a duplicate", "4|3|3|4", 5, false, false},
		{"failed closed with a phantom", "6|6|6|0", 5, false, false},
		{"nothing committed", "0|0|0|0", 0, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			markers, err := parseNativeFaultMarkers(tc.out)
			if err != nil {
				t.Fatal(err)
			}
			if err := markers.check(tc.last, tc.resumed); (err == nil) != tc.ok {
				t.Fatalf("check = %v", err)
			}
		})
	}
}
