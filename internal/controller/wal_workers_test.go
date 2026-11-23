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
	"errors"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		{"worker slot suffix", "must not end in _w<digits>", func(m *v1beta1.Migration) { m.Spec.Follow.SlotName = "orders_w1" }},
		{"publication suffix", "must not end in _w<digits>", func(m *v1beta1.Migration) { m.Spec.Follow.SlotName = "orders_t12" }},
		{"whole instance", "cannot be combined with clone.allDatabases", func(m *v1beta1.Migration) { m.Spec.Clone.AllDatabases = true }},
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
	// Admission rejects the suffix on every slotName; one admitted before that rule keeps its single stream.
	m = walWorkersMigration()
	m.Spec.Follow.WalWorkers = 1
	m.Spec.Follow.HighTrafficTables = nil
	m.Spec.Follow.SlotName = "orders_w7"
	if _, err := buildJob(m, "runner", 1); err != nil {
		t.Fatalf("single-stream Migration with a suffixed slot: %v", err)
	}
	// Generated slot names end in a checksum, never in a worker suffix.
	for _, name := range []string{"orders-w1", "orders-t12"} {
		m = walWorkersMigration()
		m.Name = name
		if slot := effectiveSlotName(m); workerSuffix.MatchString(slot) {
			t.Errorf("generated slot %q for %q ends in a worker suffix", slot, name)
		}
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

// Only the worker streams changes, so no other Job may carry the knobs; N = 1 sets neither.
func TestBuildJob_WalWorkersEnv(t *testing.T) {
	const workers, tables = "PGCOPYDB_WAL_WORKERS", "PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES"
	m := walWorkersMigration()
	m.Spec.Clone.NoOwner = true
	m.Spec.Clone.OwnerAfterRestore = reownTestOwner
	job, err := buildJob(m, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	env := job.Spec.Template.Spec.Containers[0].Env
	if got := envValue(env, workers); got != "4" {
		t.Errorf("%s = %q, want 4", workers, got)
	}
	if got := envValue(env, tables); got != "public.orders,public.events" {
		t.Errorf("%s = %q, want public.orders,public.events", tables, got)
	}

	for name, build := range map[string]func() (*batchv1.Job, error){
		jobKindPreflight: func() (*batchv1.Job, error) { return buildPreflightJob(m, "img") },
		jobKindGate:      func() (*batchv1.Job, error) { return buildGateJob(m, "img", 2) },
		jobKindCleanup:   func() (*batchv1.Job, error) { return buildCleanupJob(m, "img") },
		jobKindVerify:    func() (*batchv1.Job, error) { return buildVerifyJob(m, "img", "") },
		jobKindCatalog:   func() (*batchv1.Job, error) { return buildCatalogJob(m, "img", "") },
		"compare data":   func() (*batchv1.Job, error) { return buildCompareJob(m, "img", compareData) },
		"compare schema": func() (*batchv1.Job, error) { return buildCompareJob(m, "img", compareSchema) },
		"reown":          func() (*batchv1.Job, error) { return buildReownJob(m, "img") },
		jobKindRehearsal: func() (*batchv1.Job, error) { return buildRehearsalJob(m, "img", 1) },
	} {
		job, err := build()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, c := range job.Spec.Template.Spec.Containers {
			if envValue(c.Env, workers) != "" || envValue(c.Env, tables) != "" {
				t.Errorf("%s Job carries the walWorkers env", name)
			}
		}
	}

	m.Spec.Follow.WalWorkers = 1
	m.Spec.Follow.HighTrafficTables = nil
	if job, err = buildJob(m, "img", 1); err != nil {
		t.Fatal(err)
	}
	env = job.Spec.Template.Spec.Containers[0].Env
	if envValue(env, workers) != "" || envValue(env, tables) != "" {
		t.Errorf("walWorkers 1 must set neither variable")
	}
}

// The sweep runs after stream cleanup in the cleanup Job; one stream keeps the plain argv.
func TestBuildCleanupJob_WalWorkersSweep(t *testing.T) {
	m := walWorkersMigration()
	m.Spec.Follow.SlotName = "sweep_slot"
	job, err := buildCleanupJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	script := job.Spec.Template.Spec.Containers[0].Args[1]
	mustPrecede(t, script, "pgcopydb stream cleanup --dir /work/pgcopydb --slot-name sweep_slot --origin sweep_slot\n", walWorkersSweep(m))
	for _, want := range []string{`psql "$PGCOPYDB_SOURCE_PGURI"`, `psql "$PGCOPYDB_TARGET_PGURI"`,
		"pg_drop_replication_slot(n)", "format('DROP PUBLICATION %I', n)", "pg_replication_origin_drop(o)"} {
		if !strings.Contains(script, want) {
			t.Errorf("cleanup script lacks %q:\n%s", want, script)
		}
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 2 || simulateKubeletDollarExpansion(script) != script {
		t.Errorf("cleanup Job must keep two retries and survive kubelet's $$ expansion")
	}

	m.Spec.Follow.WalWorkers, m.Spec.Follow.HighTrafficTables = 1, nil
	if walWorkersSweep(m) != "" {
		t.Fatal("one WAL stream needs no sweep")
	}
	if job, err = buildCleanupJob(m, "img"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " "); got != "stream cleanup --dir /work/pgcopydb --slot-name sweep_slot --origin sweep_slot" {
		t.Fatalf("single-stream cleanup argv changed: %q", got)
	}
}

// Both CleanupFailed paths name every slot that can still hold WAL on the source.
func TestEnsureCleanup_NamesWorkerSlots(t *testing.T) {
	terminating := apierrors.NewForbidden(batchv1.Resource("jobs"), "m-cleanup", errors.New("namespace is terminating"))
	terminating.ErrStatus.Details.Causes = []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause}}
	refuseCreate := interceptor.Funcs{Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
		return terminating
	}}
	for workers, want := range map[int32]string{
		1: `replication slot "cleanup_slot" `,
		3: `replication slot "cleanup_slot" (with WAL worker slots cleanup_slot_w1, cleanup_slot_w2) `,
	} {
		m := passwordMigration()
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, SlotName: "cleanup_slot", WalWorkers: workers}
		failed := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: cleanupJobName(m), Namespace: m.Namespace},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
			}},
		}
		for path, r := range map[string]*MigrationReconciler{
			"retries exhausted":     failingReconciler(t, interceptor.Funcs{}, failed),
			"namespace terminating": failingReconciler(t, refuseCreate),
		} {
			done, err := r.ensureCleanup(context.Background(), m)
			emitted := drainEvents(r.Recorder.(*events.FakeRecorder))
			if !done || err != nil || len(emitted) != 1 || !strings.Contains(emitted[0], "CleanupFailed") ||
				!strings.Contains(emitted[0], want) {
				t.Errorf("%d workers, %s: done=%t err=%v events=%q, want %q", workers, path, done, err, emitted, want)
			}
		}
	}
}
