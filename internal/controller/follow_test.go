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

// jobFailedMsg is the canned failure message test helpers stamp on Jobs.
const jobFailedMsg = "pod failed"

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
				Reason: batchv1.JobReasonBackoffLimitExceeded, Message: jobFailedMsg},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				Reason: batchv1.JobReasonBackoffLimitExceeded, Message: jobFailedMsg})
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

// fakeLogs scripts the pod-log reader the way fakeSentinel scripts the
// sentinel: tests choose what a pod "logged".
type fakeLogs struct {
	out string
	err error
}

func (f *fakeLogs) JobLogs(context.Context, string, string, int64) ([]byte, error) {
	return []byte(f.out), f.err
}

var _ = Describe("Migration Controller follow mode", func() {
	ctx := context.Background()

	followMigration := func(name string, mode v1alpha1.CutoverMode) *v1alpha1.Migration {
		m := validMigration(name)
		m.Spec.Follow = &v1alpha1.FollowOptions{Enabled: true, Plugin: "pgoutput"}
		m.Spec.Cutover = v1alpha1.CutoverSpec{Mode: mode}
		return m
	}

	reconcileLogged := func(fake *fakeSentinel, logs LogReader, name string) *v1alpha1.Migration {
		GinkgoHelper()
		r := newReconciler()
		r.Sentinel = fake
		r.Logs = logs
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
		})
		Expect(err).NotTo(HaveOccurred())
		m := &v1alpha1.Migration{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}

	reconcileWith := func(fake *fakeSentinel, name string) *v1alpha1.Migration {
		GinkgoHelper()
		return reconcileLogged(fake, nil, name)
	}

	// passPreflight drives the gate every follow migration now starts with:
	// the first pass creates only the preflight Job; its success unlocks run-1.
	passPreflight := func(fake *fakeSentinel, name string) *v1alpha1.Migration {
		GinkgoHelper()
		m := reconcileWith(fake, name)
		finishJob(ctx, name+"-preflight", true)
		return m
	}

	It("walks preflight, streaming, caught-up, manual cutover, drain, and cleanup", func() {
		const name = "mig-follow-manual"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		Expect(k8sClient.Create(ctx, followMigration(name, v1alpha1.CutoverManual))).To(Succeed())

		// Pass 1 creates the preflight Job, not the worker: the finalizer is
		// already on (a preflight cannot leak a slot, but the order is fixed).
		m := reconcileWith(fake, name)
		Expect(m.Finalizers).To(ContainElement(finalizerName))
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseValidating))
		Expect(fetchJob(ctx, name+"-preflight")).NotTo(BeNil())
		Expect(errors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: name + "-run-1", Namespace: testNS}, &batchv1.Job{}))).To(BeTrue())

		finishJob(ctx, name+"-preflight", true)
		reconcileWith(fake, name)
		job := fetchJob(ctx, name+"-run-1")
		args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
		Expect(args).To(ContainSubstring("--follow"))
		Expect(args).To(ContainSubstring("--slot-name"))

		// Base copy still running: no phase change from the sentinel.
		fake.state = &sentinel.State{ApplyEnabled: false, WriteLSN: "0/10", ReplayLSN: "0/0", SourceHead: "0/20"}
		m = reconcileWith(fake, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCloning))

		// Apply on, lag far above threshold: Streaming, not caught up.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: "0/40000000", ReplayLSN: "0/10000000", SourceHead: "0/48000000", Endpos: sentinel.ZeroLSN}
		m = reconcileWith(fake, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseStreaming))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionStreaming)).To(BeTrue())
		Expect(meta.IsStatusConditionFalse(m.Status.Conditions, v1alpha1.ConditionCaughtUp)).To(BeTrue())
		Expect(m.Status.Replication).NotTo(BeNil())
		Expect(*m.Status.Replication.LagBytes).To(BeNumerically(">", int64(16<<20)))

		// Caught up, Manual, not approved: waiting.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: "0/48000010", ReplayLSN: "0/48000000", SourceHead: "0/48000010", Endpos: sentinel.ZeroLSN}
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

		// Worker drains and exits 0: exit code alone is not trusted, a
		// verify Job must first prove origin progress reached endpos.
		finishJob(ctx, name+"-run-1", true)
		m = reconcileWith(fake, name)
		verifyJob := fetchJob(ctx, name+"-verify")
		Expect(verifyJob.Spec.Template.Spec.Containers[0].Args[1]).To(ContainSubstring("pg_replication_origin_progress"))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionCutoverComplete)).To(BeFalse())

		// Verification passes: cutover completed, cleanup Job appears.
		finishJob(ctx, name+"-verify", true)
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
		passPreflight(fake, name)
		reconcileWith(fake, name)

		caughtUpLSN := "0/100"
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		m := reconcileWith(fake, name)
		Expect(fake.endposSet).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCuttingOver))
	})

	It("fails loudly when drain verification is refuted", func() {
		const name = "mig-follow-lost"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		Expect(k8sClient.Create(ctx, followMigration(name, v1alpha1.CutoverManual))).To(Succeed())
		passPreflight(fake, name)
		reconcileWith(fake, name)

		// Worker exits 0 (the deceptive resume-after-crash case), but the
		// verify Job refutes the drain: no cleanup, absorbing Failed, and the
		// message says the slot is kept.
		finishJob(ctx, name+"-run-1", true)
		reconcileWith(fake, name)
		finishJob(ctx, name+"-verify", false)
		m := reconcileWith(fake, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseFailed))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionFailed)).To(BeTrue())
		cond := meta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionCutoverComplete)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Message).To(ContainSubstring("slot is kept"))
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-cleanup", Namespace: testNS}, &batchv1.Job{})
		Expect(errors.IsNotFound(err)).To(BeTrue())
	})

	It("routes deletion through slot cleanup", func() {
		const name = "mig-follow-delete"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		Expect(k8sClient.Create(ctx, followMigration(name, v1alpha1.CutoverManual))).To(Succeed())
		passPreflight(fake, name)      // adds finalizer, clears the gate
		m := reconcileWith(fake, name) // creates run-1
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

	It("absorbs a preflight failure with the check output in the conditions", func() {
		const name = "mig-preflight-fail"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		logs := &fakeLogs{out: "preflight: source wal_level is 'replica', follow needs 'logical': set wal_level = logical on the source and restart it\n"}
		Expect(k8sClient.Create(ctx, followMigration(name, v1alpha1.CutoverManual))).To(Succeed())

		reconcileWith(fake, name)
		finishJob(ctx, name+"-preflight", false)
		m := reconcileLogged(fake, logs, name)

		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseFailed))
		validated := meta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionValidated)
		Expect(validated.Status).To(Equal(metav1.ConditionFalse))
		Expect(validated.Reason).To(Equal("PreflightFailed"))
		Expect(validated.Message).To(ContainSubstring("wal_level is 'replica'"))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionFailed)
		Expect(failed.Status).To(Equal(metav1.ConditionTrue))
		Expect(failed.Reason).To(Equal("PreflightFailed"))
		Expect(failed.Message).To(ContainSubstring("set wal_level = logical"))

		// Absorbing: no worker ever starts, further passes change nothing.
		m = reconcileLogged(fake, logs, name)
		Expect(m.Status.Attempts).To(Equal(int32(0)))
		Expect(errors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: name + "-run-1", Namespace: testNS}, &batchv1.Job{}))).To(BeTrue())
	})

	It("degrades to a log pointer when the preflight output is unreadable", func() {
		const name = "mig-preflight-nologs"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		Expect(k8sClient.Create(ctx, followMigration(name, v1alpha1.CutoverManual))).To(Succeed())

		reconcileWith(fake, name)
		finishJob(ctx, name+"-preflight", false)
		// nil LogReader: envtest's default, and the operator's stance when
		// the pod is already gone.
		m := reconcileWith(fake, name)

		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseFailed))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionFailed)
		Expect(failed.Reason).To(Equal("PreflightFailed"))
		Expect(failed.Message).To(ContainSubstring("inspect the logs of Job " + name + "-preflight"))
	})
})
