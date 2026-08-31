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

package metrics

import (
	"maps"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func setVerified(m *v1beta1.Migration, status metav1.ConditionStatus) {
	meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
		Type: v1beta1.ConditionVerified, Status: status, Reason: "Test",
	})
}

func TestRecordVerified(t *testing.T) {
	m := newMigration("ns1", "verified")
	t.Cleanup(func() { Forget(m.Namespace, m.Name) })

	// No Verified condition: no series (absent means "no result yet").
	Record(m)
	if got := testutil.CollectAndCount(verified); got != 0 {
		t.Fatalf("verified series without condition = %d, want 0", got)
	}

	// Unknown (compare running): still no series.
	setVerified(m, metav1.ConditionUnknown)
	Record(m)
	if got := testutil.CollectAndCount(verified); got != 0 {
		t.Fatalf("verified series while Unknown = %d, want 0", got)
	}

	setVerified(m, metav1.ConditionTrue)
	Record(m)
	if got := testutil.ToFloat64(verified.WithLabelValues("ns1", "verified")); got != 1 {
		t.Fatalf("verified gauge after pass = %v, want 1", got)
	}

	setVerified(m, metav1.ConditionFalse)
	Record(m)
	if got := testutil.ToFloat64(verified.WithLabelValues("ns1", "verified")); got != 0 {
		t.Fatalf("verified gauge after mismatch = %v, want 0", got)
	}
	if got := testutil.CollectAndCount(verified); got != 1 {
		t.Fatalf("verified series = %d, want 1", got)
	}
}

// checkValue reads one check's gauge and reports whether the series exists at
// all. Absence is a state the dashboard renders ("Pending"), not a test error,
// so testutil.ToFloat64 is no use here: it panics on a missing series.
func checkValue(t *testing.T, m *v1beta1.Migration, check string) (float64, bool) {
	t.Helper()
	ch := make(chan prometheus.Metric, 32)
	verifiedCheck.Collect(ch)
	close(ch)
	want := map[string]string{
		labelNamespace: m.Namespace, labelName: m.Name, "check": check,
	}
	for metric := range ch {
		var dm dto.Metric
		if err := metric.Write(&dm); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		got := map[string]string{}
		for _, l := range dm.GetLabel() {
			got[l.GetName()] = l.GetValue()
		}
		if maps.Equal(got, want) {
			return dm.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

// The tiles render four states off this one gauge, and three of them are the
// reason it exists: a check nobody asked for used to be indistinguishable from
// one still to run, because both were an absent series.
func TestRecordVerificationCheck(t *testing.T) {
	const (
		absent      = "absent"
		deactivated = verificationDeactivated
	)
	for _, tc := range []struct {
		name         string
		spec         *v1beta1.VerificationOptions
		results      []v1beta1.VerificationResult
		schema, data any
	}{
		{
			name:   "no verification block deactivates both",
			schema: float64(deactivated), data: float64(deactivated),
		},
		{
			name:   "requested but not run stays absent",
			spec:   &v1beta1.VerificationOptions{Schema: true},
			schema: absent, data: float64(deactivated),
		},
		{
			name:    "one check on and passed, the other off",
			spec:    &v1beta1.VerificationOptions{Schema: true},
			results: []v1beta1.VerificationResult{{Check: checkSchema, Passed: true}},
			schema:  float64(1), data: float64(deactivated),
		},
		{
			name:    "both on, data mismatched",
			spec:    &v1beta1.VerificationOptions{Schema: true, Data: true},
			results: []v1beta1.VerificationResult{{Check: checkSchema, Passed: true}, {Check: checkData, Passed: false}},
			schema:  float64(1), data: float64(0),
		},
		{
			// Turning a check off after it ran leaves the result in status,
			// because the verify path returns early without clearing it.
			name:    "a mismatch outlives the spec that asked for it",
			spec:    &v1beta1.VerificationOptions{},
			results: []v1beta1.VerificationResult{{Check: checkData, Passed: false}},
			schema:  float64(deactivated), data: float64(0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMigration("ns1", "check-states")
			t.Cleanup(func() { Forget(m.Namespace, m.Name) })
			m.Spec.Verification = tc.spec
			m.Status.Verification = tc.results

			Record(m)

			for check, want := range map[string]any{checkSchema: tc.schema, checkData: tc.data} {
				got, found := checkValue(t, m, check)
				if want == absent {
					if found {
						t.Errorf("%s gauge = %v, want no series", check, got)
					}
					continue
				}
				if !found {
					t.Errorf("%s has no series, want %v", check, want)
					continue
				}
				if got != want {
					t.Errorf("%s gauge = %v, want %v", check, got, want)
				}
			}
		})
	}
}

func TestForgetVerified(t *testing.T) {
	m := newMigration("ns1", "verified-gone")
	setVerified(m, metav1.ConditionTrue)
	Record(m)

	Forget(m.Namespace, m.Name)
	if got := testutil.CollectAndCount(verified); got != 0 {
		t.Fatalf("verified series after Forget = %d, want 0", got)
	}
}
