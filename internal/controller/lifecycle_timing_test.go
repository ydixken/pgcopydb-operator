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
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/zapr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/metrics"
)

const (
	timingCloneStage     = "clone_stage"
	timingFollowControl  = "follow_control"
	timingNextDelay      = "next_delay"
	timingFollowLogFetch = "follow_log_fetch"
	timingProgressSample = "progress_sample"
	timingStatusPatch    = "status_patch"
	timingZombieReap     = "zombie_reap"
)

func TestActivePollDelayUsesReconcileStart(t *testing.T) {
	start := time.Unix(100, 0)
	tests := []struct {
		name    string
		elapsed time.Duration
		want    time.Duration
	}{
		{name: "fast pass", elapsed: 2 * time.Second, want: 8 * time.Second},
		{name: "almost due", elapsed: pollInterval - time.Nanosecond, want: time.Nanosecond},
		{name: "overdue", elapsed: pollInterval + 5*time.Second, want: time.Nanosecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &MigrationReconciler{now: func() time.Time { return start.Add(tt.elapsed) }}
			if got := r.activePollDelay(start); got != tt.want {
				t.Fatalf("active poll delay = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestPendingPatchFailureDoesNotPublishStatusOrMetrics(t *testing.T) {
	m := passwordMigration()
	m.Name = "pending-patch-failure"
	metrics.Forget(m.Namespace, m.Name)
	t.Cleanup(func() { metrics.Forget(m.Namespace, m.Name) })
	r := failingReconciler(t, failStatusPatch(), m)

	if _, err := r.reconcile(context.Background(), migrationRequest(m)); !errors.Is(err, errBoom) {
		t.Fatalf("initial reconcile error = %v, want %v", err, errBoom)
	}
	got := &v1beta1.Migration{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(m), got); err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(got.Status, v1beta1.MigrationStatus{}) {
		t.Fatalf("failed patch persisted status %+v", got.Status)
	}
	present, err := migrationPhaseMetricPresent(m.Namespace, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if present {
		t.Fatal("failed Pending patch published a phase metric")
	}
	for _, obj := range []client.Object{
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: preflightJobName(m), Namespace: m.Namespace}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName(m, 1), Namespace: m.Namespace}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: workPVCName(m), Namespace: m.Namespace}},
	} {
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); !apierrors.IsNotFound(err) {
			t.Fatalf("%T exists after failed Pending patch: %v", obj, err)
		}
	}
}

func migrationPhaseMetricPresent(namespace, name string) (bool, error) {
	families, err := crmetrics.Registry.Gather()
	if err != nil {
		return false, err
	}
	for _, family := range families {
		if family.GetName() != "pgcopydb_migration_phase" {
			continue
		}
		for _, sample := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range sample.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["namespace"] == namespace && labels["name"] == name && labels["phase"] == string(v1beta1.PhasePending) {
				return true, nil
			}
		}
	}
	return false, nil
}

var _ = Describe("Migration lifecycle initialization", func() {
	ctx := context.Background()

	It("persists only Pending before a fresh clone validates", func() {
		const name = "mig-clone-initial-pending"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		m := validMigration(name)
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status).To(Equal(v1beta1.MigrationStatus{
			ObservedGeneration: m.Generation,
			Phase:              v1beta1.PhasePending,
		}))
		Expect(m.Finalizers).To(BeEmpty())
		expectLifecycleResourcesAbsent(m)
	})

	It("persists only Pending before a fresh follow Migration validates", func() {
		const name = "mig-initial-pending"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
		m.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlySchemas: []string{"public"}}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		r := newReconciler()
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{
			Name: name, Namespace: testNS,
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(time.Nanosecond))

		m = &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNS, Name: name}, m)).To(Succeed())
		Expect(m.Status).To(Equal(v1beta1.MigrationStatus{
			ObservedGeneration: m.Generation,
			Phase:              v1beta1.PhasePending,
		}))
		Expect(m.Finalizers).To(BeEmpty())
		pendingMetric, err := migrationPhaseMetricPresent(testNS, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(pendingMetric).To(BeTrue())
		expectLifecycleResourcesAbsent(m)

		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseValidating))
		pendingMetric, err = migrationPhaseMetricPresent(testNS, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(pendingMetric).To(BeFalse())
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated).NotTo(BeNil())
		Expect(validated.Status).To(Equal(metav1.ConditionUnknown))
		Expect(validated.Reason).To(Equal("PreflightRunning"))
		Expect(m.Finalizers).To(ContainElement(finalizerName))
		Expect(fetchJob(ctx, preflightJobName(m))).NotTo(BeNil())
	})

	It("moves a fresh suspended Migration through Pending without creating resources", func() {
		const name = "mig-initial-suspended"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		m := validMigration(name)
		m.Spec.Suspend = true
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		r := newReconciler()
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhasePending))
		expectLifecycleResourcesAbsent(m)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
		Expect(m.Status.Conditions).To(BeEmpty())
		Expect(m.Finalizers).To(BeEmpty())
		expectLifecycleResourcesAbsent(m)

		m.Spec.Suspend = false
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseValidating))
		Expect(fetchJob(ctx, preflightJobName(m))).NotTo(BeNil())
	})

	It("treats nil and empty status collections as an empty status", func() {
		Expect(statusIsEmpty(v1beta1.MigrationStatus{})).To(BeTrue())
		Expect(statusIsEmpty(v1beta1.MigrationStatus{
			Conditions:   []metav1.Condition{},
			Verification: []v1beta1.VerificationResult{},
		})).To(BeTrue())
	})

	It("never resets any recorded status field to Pending", func() {
		now := metav1.Now()
		cases := []struct {
			name   string
			mutate func(*v1beta1.MigrationStatus)
		}{
			{name: "phase", mutate: func(s *v1beta1.MigrationStatus) { s.Phase = v1beta1.PhaseValidating }},
			{name: "generation", mutate: func(s *v1beta1.MigrationStatus) { s.ObservedGeneration = 1 }},
			{name: "condition", mutate: func(s *v1beta1.MigrationStatus) {
				s.Conditions = []metav1.Condition{{Type: v1beta1.ConditionValidated, Status: metav1.ConditionUnknown, Reason: "Recorded"}}
			}},
			{name: "attempt", mutate: func(s *v1beta1.MigrationStatus) { s.Attempts = 1 }},
			{name: "progress", mutate: func(s *v1beta1.MigrationStatus) { s.Progress = &v1beta1.CloneProgress{} }},
			{name: "replication", mutate: func(s *v1beta1.MigrationStatus) { s.Replication = &v1beta1.ReplicationStatus{} }},
			{name: "verification", mutate: func(s *v1beta1.MigrationStatus) {
				s.Verification = []v1beta1.VerificationResult{{Check: "schema", Passed: true}}
			}},
			{name: "job", mutate: func(s *v1beta1.MigrationStatus) { s.JobName = "recorded" }},
			{name: "started", mutate: func(s *v1beta1.MigrationStatus) { s.StartedAt = &now }},
			{name: "completed", mutate: func(s *v1beta1.MigrationStatus) { s.CompletedAt = &now }},
		}
		for _, tc := range cases {
			By(tc.name)
			status := v1beta1.MigrationStatus{}
			tc.mutate(&status)
			Expect(statusIsEmpty(status)).To(BeFalse())
		}
	})

	It("conflicts instead of overwriting status recorded during the initial patch", func() {
		const name = "mig-pending-conflict"
		defer removeMigration(ctx, name)
		defer metrics.Forget(testNS, name)
		m := validMigration(name)
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		intercepted := interceptor.NewClient(watchClient, interceptor.Funcs{
			SubResourcePatch: func(
				ctx context.Context, c client.Client, subresource string, obj client.Object,
				patch client.Patch, opts ...client.SubResourcePatchOption,
			) error {
				persisted := &v1beta1.Migration{}
				Expect(c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: name}, persisted)).To(Succeed())
				persisted.Status.Attempts = 1
				Expect(c.Status().Update(ctx, persisted)).To(Succeed())
				return c.SubResource(subresource).Patch(ctx, obj, patch, opts...)
			},
		})
		r := newReconciler()
		r.Client = intercepted
		_, err = r.reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{
			Name: name, Namespace: testNS,
		}})
		Expect(apierrors.IsConflict(err)).To(BeTrue(), "initial status patch did not carry an optimistic lock: %v", err)
		m = &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNS, Name: name}, m)).To(Succeed())
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(m.Status.Phase).To(BeEmpty())
		pendingMetric, metricErr := migrationPhaseMetricPresent(testNS, name)
		Expect(metricErr).NotTo(HaveOccurred())
		Expect(pendingMetric).To(BeFalse())
	})
})

func expectLifecycleResourcesAbsent(m *v1beta1.Migration) {
	GinkgoHelper()
	for _, obj := range []client.Object{
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: preflightJobName(m), Namespace: m.Namespace}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName(m, 1), Namespace: m.Namespace}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: workPVCName(m), Namespace: m.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: filtersCMName(m), Namespace: m.Namespace}},
	} {
		err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "%T exists during Pending: %v", obj, err)
	}
}

func TestActiveWorkerObservationTiming(t *testing.T) {
	tests := []struct {
		name       string
		follow     bool
		zombie     bool
		patchError error
		wantResult time.Duration
		wantErr    error
		want       []string
		omit       []string
		outcome    string
	}{
		{
			name: "clone normal return", wantResult: time.Nanosecond, outcome: "normal",
			want: []string{timingCloneStage, timingProgressSample, timingStatusPatch, timingZombieReap, timingNextDelay},
			omit: []string{timingFollowLogFetch, timingFollowControl},
		},
		{
			name: "follow normal return", follow: true, wantResult: time.Nanosecond, outcome: "normal",
			want: []string{timingCloneStage, timingFollowLogFetch, timingFollowControl, timingProgressSample, timingStatusPatch, timingZombieReap, timingNextDelay},
		},
		{
			name: "status patch error", patchError: errBoom, wantErr: errBoom, outcome: "status_patch_error",
			want: []string{timingCloneStage, timingProgressSample, timingStatusPatch},
			omit: []string{timingFollowLogFetch, timingFollowControl, timingZombieReap, timingNextDelay},
		},
		{
			name: "zombie handled", follow: true, zombie: true, wantResult: pollInterval, outcome: "zombie_reap_handled",
			want: []string{timingCloneStage, timingFollowLogFetch, timingFollowControl, timingProgressSample, timingStatusPatch, timingZombieReap},
			omit: []string{timingNextDelay},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := passwordMigration()
			m.Name = "timed-" + fmt.Sprint(len(tc.name))
			m.Status.Phase = v1beta1.PhaseCloning
			m.Status.JobName = jobName(m, 1)
			m.Status.Attempts = 1
			if tc.follow {
				m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
			}
			clock := &stepClock{at: time.Unix(200, 0), step: time.Second}
			baseReconciler := failingReconciler(t, interceptor.Funcs{}, m)
			baseClient, ok := baseReconciler.Client.(client.WithWatch)
			if !ok {
				t.Fatal("fake client does not support watch")
			}
			wrapped := interceptor.NewClient(baseClient, interceptor.Funcs{
				SubResourcePatch: func(
					ctx context.Context, c client.Client, subresource string, obj client.Object,
					patch client.Patch, opts ...client.SubResourcePatchOption,
				) error {
					if tc.patchError != nil {
						return tc.patchError
					}
					return c.SubResource(subresource).Patch(ctx, obj, patch, opts...)
				},
			})
			core, recorded := observer.New(zapcore.DebugLevel)
			ctx := log.IntoContext(context.Background(), zapr.NewLogger(zap.New(core)))
			r := &MigrationReconciler{
				Client: wrapped, Scheme: baseClient.Scheme(), Recorder: events.NewFakeRecorder(20),
				RunnerImage: testRunnerImage, Progress: &fakeProgress{}, now: clock.Now,
			}
			if tc.follow {
				r.Logs = &fakeLogs{}
				r.Sentinel = &fakeSentinel{}
			}
			if tc.zombie {
				r.Logs = &fakeLogs{tsOut: string(supervisorDeathTail(2 * zombieGrace))}
			}
			reconcileStart := clock.Now().Add(-pollInterval)
			res, err := r.observeRunningJob(ctx, m, m.DeepCopy(), &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: m.Status.JobName, Namespace: m.Namespace},
			}, reconcileStart)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("observe error = %v, want %v", err, tc.wantErr)
			}
			if res.RequeueAfter != tc.wantResult {
				t.Fatalf("requeue = %s, want %s", res.RequeueAfter, tc.wantResult)
			}
			entries := recorded.FilterMessage("active worker observation").All()
			if len(entries) != 1 {
				t.Fatalf("timing entries = %d, want 1", len(entries))
			}
			fields := entries[0].ContextMap()
			if fields["scope"] != "active_worker_observation" || fields["outcome"] != tc.outcome {
				t.Fatalf("timing identity = %#v", fields)
			}
			if _, ok := fields["total"]; !ok {
				t.Fatalf("timing has no total: %#v", fields)
			}
			for _, name := range tc.want {
				if _, ok := fields[name]; !ok {
					t.Errorf("timing omits reached operation %q: %#v", name, fields)
				}
			}
			for _, name := range tc.omit {
				if _, ok := fields[name]; ok {
					t.Errorf("timing invents unreached operation %q: %#v", name, fields)
				}
			}
			for _, forbidden := range []string{"sql", "command", "connection", "job", "namespace", "name", "error"} {
				if _, ok := fields[forbidden]; ok {
					t.Errorf("timing leaks forbidden field %q: %#v", forbidden, fields)
				}
			}
		})
	}
}

type stepClock struct {
	at   time.Time
	step time.Duration
}

func (c *stepClock) Now() time.Time {
	now := c.at
	c.at = c.at.Add(c.step)
	return now
}
