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
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func walWorkersMigration() *v1beta1.Migration {
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{
		Enabled:           true,
		WalWorkers:        4,
		HighTrafficTables: []string{"public.orders", "public.events"},
	}
	return m
}

// Each case passes every other builder check, so only validateWalWorkers can
// reject it; the fake objects carry no CEL, like a Migration under an older CRD.
func TestWalWorkersValidation(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*v1beta1.Migration)
	}{
		{"above 16", "between 1 and 16", func(m *v1beta1.Migration) { m.Spec.Follow.WalWorkers = 17 }},
		{"negative", "between 1 and 16", func(m *v1beta1.Migration) { m.Spec.Follow.WalWorkers = -1 }},
		{"disabled follow", "requires follow.enabled", func(m *v1beta1.Migration) { m.Spec.Follow.Enabled = false }},
		{v1beta1.PluginWal2json, "plugin pgoutput", func(m *v1beta1.Migration) { m.Spec.Follow.Plugin = v1beta1.PluginWal2json }},
		{"foreign publication", "no publication", func(m *v1beta1.Migration) { m.Spec.Follow.Publication = "foreign_publication" }},
		{"unlogged tables", "no unloggedTables", func(m *v1beta1.Migration) {
			m.Spec.Follow.UnloggedTables = []v1beta1.UnloggedTable{{Table: "public.unlogged", Mode: v1beta1.UnloggedSchemaOnly}}
		}},
		{"insert-only tables", "no unloggedTables", func(m *v1beta1.Migration) {
			m.Spec.Follow.InsertOnlyTables = []string{"public.audit"}
		}},
		{"long slot", "at most 51", func(m *v1beta1.Migration) { m.Spec.Follow.SlotName = strings.Repeat("s", 52) }},
		{"tables without workers", "highTrafficTables requires", func(m *v1beta1.Migration) { m.Spec.Follow.WalWorkers = 1 }},
		{"more tables than workers", "highTrafficTables requires", func(m *v1beta1.Migration) {
			m.Spec.Follow.WalWorkers = 2
			m.Spec.Follow.HighTrafficTables = append(m.Spec.Follow.HighTrafficTables, "public.items")
		}},
		{"pattern", "exact lowercase", func(m *v1beta1.Migration) { m.Spec.Follow.HighTrafficTables[0] = "public.orders,public.x" }},
		{"duplicate", "unique", func(m *v1beta1.Migration) { m.Spec.Follow.HighTrafficTables[1] = "public.orders" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := walWorkersMigration()
			tc.change(m)
			if _, err := buildJob(m, "runner", 1); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("worker Job error = %v, want %q", err, tc.want)
			}
			if _, err := buildPreflightJob(m, "runner"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("preflight Job error = %v, want %q", err, tc.want)
			}
		})
	}
	m := walWorkersMigration()
	m.Spec.Follow.SlotName = strings.Repeat("s", 51)
	if _, err := buildJob(m, "runner", 1); err != nil {
		t.Fatal(err)
	}
}

// The fake API bypasses CEL; the rejection must persist a terminal InvalidSpec
// without a worker Job, not loop through the retry channel.
func TestReconcile_WalWorkersInvalidSpec(t *testing.T) {
	m := walWorkersMigration()
	m.Spec.Follow.WalWorkers = 17
	m.Status.Phase = v1beta1.PhasePending
	ctx := context.Background()
	r := failingReconciler(t, interceptor.Funcs{}, m)
	res, err := r.Reconcile(ctx, migrationRequest(m))
	if err != nil || res != (ctrl.Result{}) {
		t.Fatalf("deterministic spec error must not requeue: %+v, %v", res, err)
	}
	got := &v1beta1.Migration{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(m), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != v1beta1.PhaseFailed || got.Status.Attempts != 0 {
		t.Fatalf("want terminal failure before any worker: %+v", got.Status)
	}
	const want = "walWorkers must be between 1 and 16"
	for name, status := range map[string]metav1.ConditionStatus{
		v1beta1.ConditionValidated: metav1.ConditionFalse,
		v1beta1.ConditionFailed:    metav1.ConditionTrue,
	} {
		cond := findCondition(got.Status.Conditions, name)
		if cond == nil || cond.Status != status || cond.Reason != testInvalidSpec || !strings.Contains(cond.Message, want) {
			t.Fatalf("%s must persist the rejection: %+v", name, cond)
		}
	}
	jobs := &batchv1.JobList{}
	if err := r.List(ctx, jobs); err != nil || len(jobs.Items) != 0 {
		t.Fatalf("invalid spec created Jobs: %v, %v", jobs.Items, err)
	}
	emitted := drainEvents(r.Recorder.(*events.FakeRecorder))
	if len(emitted) != 1 || !strings.Contains(emitted[0], "Warning InvalidSpec") || !strings.Contains(emitted[0], want) {
		t.Fatalf("missing InvalidSpec event: %v", emitted)
	}
}
