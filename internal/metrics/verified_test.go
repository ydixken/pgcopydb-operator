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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestForgetVerified(t *testing.T) {
	m := newMigration("ns1", "verified-gone")
	setVerified(m, metav1.ConditionTrue)
	Record(m)

	Forget(m.Namespace, m.Name)
	if got := testutil.CollectAndCount(verified); got != 0 {
		t.Fatalf("verified series after Forget = %d, want 0", got)
	}
}
