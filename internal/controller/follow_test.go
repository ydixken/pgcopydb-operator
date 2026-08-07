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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// deletionDriveTimeout bounds the Eventually loops that drive multi-pass
// deletion flows; envtest passes are fast, this is generous.
const deletionDriveTimeout = 15 * time.Second

// Package-level twins of the closure helpers in migration_controller_test.go,
// usable across Describe blocks.
func fetchJob(ctx context.Context, name string) *batchv1.Job {
	j := &batchv1.Job{}
	ExpectWithOffset(1, k8sClient.Get(ctx,
		types.NamespacedName{Name: name, Namespace: testNS}, j)).To(Succeed())
	return j
}

func finishJob(ctx context.Context, name string, succeeded bool) {
	j := fetchJob(ctx, name)
	now := metav1.Now()
	if j.Status.StartTime == nil {
		j.Status.StartTime = &now
	}
	if succeeded {
		j.Status.Succeeded = 1
		j.Status.CompletionTime = &now
		j.Status.Conditions = append(j.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
	} else {
		j.Status.Failed = 1
		j.Status.Conditions = append(j.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue,
				Reason: batchv1.JobReasonBackoffLimitExceeded, Message: "pod failed"},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				Reason: batchv1.JobReasonBackoffLimitExceeded, Message: "pod failed"})
	}
	ExpectWithOffset(1, k8sClient.Status().Update(ctx, j)).To(Succeed())
}

func removeMigration(ctx context.Context, name string) {
	m := &v1alpha1.Migration{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m); err == nil {
		_ = k8sClient.Delete(ctx, m)
	}
	sel := []client.DeleteAllOfOption{
		client.InNamespace(testNS),
		client.MatchingLabels(map[string]string{labelMigration: name}),
	}
	_ = k8sClient.DeleteAllOf(ctx, &batchv1.Job{}, sel...)
	_ = k8sClient.DeleteAllOf(ctx, &corev1.PersistentVolumeClaim{}, sel...)
	_ = k8sClient.DeleteAllOf(ctx, &corev1.ConfigMap{}, sel...)
}

// fakeSentinel scripts the sentinel the way envtest scripts Job status: tests
// set the state, the reconciler reads it.
type fakeSentinel struct {
	mu        sync.Mutex
	state     *sentinel.State
	endposSet bool
}

func (f *fakeSentinel) Read(_ context.Context, _, _ string) (*sentinel.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == nil {
		return nil, nil
	}
	s := *f.state
	return &s, nil
}

func (f *fakeSentinel) SetEndposCurrent(_ context.Context, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.endposSet = true
	f.state.Endpos = f.state.SourceHead
	return f.state.Endpos, nil
}

var _ = Describe("Migration Controller follow mode", func() {
	ctx := context.Background()

	followMigration := func(name string, mode v1alpha1.CutoverMode) *v1alpha1.Migration {
		m := validMigration(name)
		m.Spec.Follow = &v1alpha1.FollowOptions{Enabled: true, Plugin: "pgoutput"}
		m.Spec.Cutover = v1alpha1.CutoverSpec{Mode: mode}
		return m
	}

	reconcileWith := func(fake *fakeSentinel, name string) *v1alpha1.Migration {
		r := newReconciler()
		r.Sentinel = fake
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
		})
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		m := &v1alpha1.Migration{}
		ExpectWithOffset(1, k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}

	It("walks streaming, caught-up, manual cutover, drain, and cleanup", func() {
		const name = "mig-follow-manual"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		Expect(k8sClient.Create(ctx, followMigration(name, v1alpha1.CutoverManual))).To(Succeed())

		m := reconcileWith(fake, name)
		Expect(m.Finalizers).To(ContainElement(finalizerName))
		job := fetchJob(ctx, name+"-run-1")
		args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
		Expect(args).To(ContainSubstring("--follow"))
		Expect(args).To(ContainSubstring("--slot-name"))

		// Base copy still running: no phase change from the sentinel.
		fake.state = &sentinel.State{ApplyEnabled: false, WriteLSN: "0/10", ReplayLSN: "0/0", SourceHead: "0/20"}
		m = reconcileWith(fake, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCloning))

		// Apply on, lag far above threshold: Streaming, not caught up.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: "0/40000000", ReplayLSN: "0/10000000", SourceHead: "0/48000000", Endpos: "0/0"}
		m = reconcileWith(fake, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseStreaming))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionStreaming)).To(BeTrue())
		Expect(meta.IsStatusConditionFalse(m.Status.Conditions, v1alpha1.ConditionCaughtUp)).To(BeTrue())
		Expect(m.Status.Replication).NotTo(BeNil())
		Expect(*m.Status.Replication.LagBytes).To(BeNumerically(">", int64(16<<20)))

		// Caught up, Manual, not approved: waiting.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: "0/48000010", ReplayLSN: "0/48000000", SourceHead: "0/48000010", Endpos: "0/0"}
		m = reconcileWith(fake, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCutoverPending))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionCaughtUp)).To(BeTrue())
		Expect(fake.endposSet).To(BeFalse())

		// Approval triggers the endpos.
		m.Spec.Cutover.Approved = true
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		m = reconcileWith(fake, name)
		Expect(fake.endposSet).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCuttingOver))

		// Worker drains and exits 0: cutover completed, cleanup Job appears.
		finishJob(ctx, name+"-run-1", true)
		m = reconcileWith(fake, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionCutoverComplete)).To(BeTrue())
		cleanupJob := fetchJob(ctx, name+"-cleanup")
		Expect(strings.Join(cleanupJob.Spec.Template.Spec.Containers[0].Args, " ")).
			To(Equal("stream cleanup --dir /work/pgcopydb"))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete)).To(BeFalse())

		// Cleanup succeeds: Complete.
		finishJob(ctx, name+"-cleanup", true)
		m = reconcileWith(fake, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCompleted))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete)).To(BeTrue())
	})

	It("cuts over automatically once caught up", func() {
		const name = "mig-follow-auto"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		Expect(k8sClient.Create(ctx, followMigration(name, v1alpha1.CutoverAutomatic))).To(Succeed())
		reconcileWith(fake, name)

		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: "0/100", ReplayLSN: "0/100", SourceHead: "0/100", Endpos: "0/0"}
		m := reconcileWith(fake, name)
		Expect(fake.endposSet).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCuttingOver))
	})

	It("routes deletion through slot cleanup", func() {
		const name = "mig-follow-delete"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		Expect(k8sClient.Create(ctx, followMigration(name, v1alpha1.CutoverManual))).To(Succeed())
		m := reconcileWith(fake, name) // creates run-1, adds finalizer
		Expect(m.Status.Attempts).To(Equal(int32(1)))

		Expect(k8sClient.Delete(ctx, m)).To(Succeed())
		// Pass 1: deletes the worker Job (foreground) and requeues.
		reconcileWith(fake, name)
		// Pass 2+: worker gone (envtest: still terminating counts), then the
		// cleanup Job is created. Drive until it exists.
		Eventually(func() error {
			reconcileWith(fake, name)
			j := &batchv1.Job{}
			return k8sClient.Get(ctx, types.NamespacedName{Name: name + "-cleanup", Namespace: testNS}, j)
		}).WithTimeout(deletionDriveTimeout).Should(Succeed())

		finishJob(ctx, name+"-cleanup", true)
		// Final pass removes the finalizer; the CR then disappears.
		Eventually(func() bool {
			r := newReconciler()
			r.Sentinel = fake
			_, _ = r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
			})
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, &v1alpha1.Migration{})
			return errors.IsNotFound(err)
		}).WithTimeout(deletionDriveTimeout).Should(BeTrue())
	})
})
