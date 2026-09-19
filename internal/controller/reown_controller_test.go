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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The handover's place in the lifecycle: it runs once the worker has exited,
// gates Complete on a clone and CutoverCompleted on a live migration, and no
// worker ever restarts beside it.
var _ = Describe("Migration Controller ownership handover", func() {
	ctx := context.Background()

	getM := func(name string) *v1beta1.Migration {
		m := &v1beta1.Migration{}
		ExpectWithOffset(1, k8sClient.Get(ctx,
			types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}

	jobMissing := func(name string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, &batchv1.Job{})
		return errors.IsNotFound(err)
	}

	// jobGoing reports a Job that is gone or on its way: envtest runs no
	// garbage collector, so a foreground delete leaves the object behind
	// with a deletion timestamp.
	jobGoing := func(name string) bool {
		j := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, j)
		return errors.IsNotFound(err) || !j.DeletionTimestamp.IsZero()
	}

	// collectJob stands in for the garbage collector and finishes a
	// foreground delete.
	collectJob := func(name string) {
		j := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, j)
		if errors.IsNotFound(err) {
			return
		}
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		j.Finalizers = nil
		ExpectWithOffset(1, k8sClient.Update(ctx, j)).To(Succeed())
	}

	// dropWorker is the TTL controller or a manual delete taking the worker
	// Job away, background so it is really gone.
	dropWorker := func(name string) {
		ExpectWithOffset(1, k8sClient.Delete(ctx, fetchJob(ctx, name+"-run-1"),
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
	}

	setSuspend := func(name string, suspend bool) {
		fresh := getM(name)
		fresh.Spec.Suspend = suspend
		ExpectWithOffset(1, k8sClient.Update(ctx, fresh)).To(Succeed())
	}

	ownership := func(m *v1beta1.Migration) *metav1.Condition {
		return meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionOwnershipApplied)
	}

	reownClone := func(name string) *v1beta1.Migration {
		m := validMigration(name)
		m.Spec.Clone.NoOwner = true
		m.Spec.Clone.OwnerAfterRestore = reownTestOwner
		return m
	}

	reownFollow := func(name string) *v1beta1.Migration {
		m := reownClone(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverAutomatic}
		return m
	}

	// startClone drives a clone through the gate and the worker's exit 0,
	// to the pass that creates the handover Job.
	startClone := func(r *MigrationReconciler, name string) *v1beta1.Migration {
		GinkgoHelper()
		passGate(ctx, r, name)
		finishJob(ctx, name+"-run-1", true)
		return reconcileAndGet(ctx, r, name)
	}

	// startFollow drives a live migration through the worker's exit 0 and
	// the verified drain, to the pass that creates the handover Job.
	// finishFollow trusts the verify Job alone, so no sentinel is needed.
	startFollow := func(r *MigrationReconciler, name string) *v1beta1.Migration {
		GinkgoHelper()
		passGate(ctx, r, name)
		finishJob(ctx, name+"-run-1", true)
		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-verify", true)
		return reconcileAndGet(ctx, r, name)
	}

	It("hands the restored objects over before a clone completes", func() {
		const name = "mig-reown-clone"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, reownClone(name))).To(Succeed())
		r := newReconciler()
		rec := r.Recorder.(*events.FakeRecorder)

		passGate(ctx, r, name)
		Expect(jobMissing(name + "-reown")).To(BeTrue())

		finishJob(ctx, name+"-run-1", true)
		m := reconcileAndGet(ctx, r, name)
		job := fetchJob(ctx, name+"-reown")
		Expect(envValue(job.Spec.Template.Spec.Containers[0].Env, reownOwnerEnv)).To(Equal(reownTestOwner))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFinalizing))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeFalse())
		Expect(ownership(m).Status).To(Equal(metav1.ConditionUnknown))
		Expect(ownership(m).Reason).To(Equal("OwnershipRunning"))
		Expect(drainEvents(rec)).To(ContainElement(SatisfyAll(
			ContainSubstring("OwnershipStarted"), ContainSubstring(reownTestOwner))))

		// Still running: another pass changes nothing.
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFinalizing))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeFalse())

		finishJob(ctx, name+"-reown", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionOwnershipApplied)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeTrue())
		Expect(drainEvents(rec)).To(ContainElement(ContainSubstring("OwnershipApplied")))
	})

	It("fails the clone when the handover fails, naming the Job and the runbook", func() {
		const name = "mig-reown-fail"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, reownClone(name))).To(Succeed())
		r := newReconciler()
		r.Logs = &fakeLogs{out: `reown: role "app_owner_role" does not exist on the target`}
		startClone(r, name)

		finishJob(ctx, name+"-reown", false)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Status).To(Equal(metav1.ConditionTrue))
		Expect(failed.Reason).To(Equal("OwnershipFailed"))
		Expect(failed.Message).To(SatisfyAll(
			ContainSubstring(name+"-reown"),
			ContainSubstring("docs/troubleshooting.md"),
			ContainSubstring("does not exist on the target")))
		Expect(ownership(m).Status).To(Equal(metav1.ConditionFalse))
		Expect(ownership(m).Reason).To(Equal("OwnershipFailed"))
		// The data did arrive; only the handover is owed.
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
		Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeNil())
	})

	It("creates no handover Job without ownerAfterRestore", func() {
		const name = "mig-reown-unset"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		m := startClone(newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
		Expect(jobMissing(name + "-reown")).To(BeTrue())
		Expect(ownership(m)).To(BeNil())
	})

	It("gates CutoverCompleted on the handover for a live migration", func() {
		const name = "mig-reown-follow"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, reownFollow(name))).To(Succeed())
		r := newReconciler()

		m := startFollow(r, name)
		fetchJob(ctx, name+"-reown")
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(ownership(m).Reason).To(Equal("OwnershipRunning"))
		Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeNil())
		Expect(jobMissing(name + "-cleanup")).To(BeTrue())

		finishJob(ctx, name+"-reown", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionOwnershipApplied)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeTrue())
		fetchJob(ctx, name+"-cleanup")

		finishJob(ctx, name+"-cleanup", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
	})

	It("keeps the slot when the handover fails after the drain, and still cleans up on deletion", func() {
		const name = "mig-reown-follow-fail"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, reownFollow(name))).To(Succeed())
		r := newReconciler()
		startFollow(r, name)

		finishJob(ctx, name+"-reown", false)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Reason).To(Equal("OwnershipFailed"))
		Expect(failed.Message).To(ContainSubstring("slot is kept"))
		Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeNil())
		Expect(jobMissing(name + "-cleanup")).To(BeTrue())

		// Deletion is the documented way out: cleanup runs, the finalizer goes.
		Expect(k8sClient.Delete(ctx, m)).To(Succeed())
		Eventually(func() error {
			reconcileAndGet(ctx, r, name)
			return k8sClient.Get(ctx, types.NamespacedName{Name: name + "-cleanup", Namespace: testNS}, &batchv1.Job{})
		}).WithTimeout(deletionDriveTimeout).Should(Succeed())
		finishJob(ctx, name+"-cleanup", true)
		Eventually(func() bool {
			_, _ = r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
			})
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, &v1beta1.Migration{})
			return errors.IsNotFound(err)
		}).WithTimeout(deletionDriveTimeout).Should(BeTrue())
	})

	It("starts no worker for a clone whose worker Job expires during the handover", func() {
		const name = "mig-reown-clone-expired"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, reownClone(name))).To(Succeed())
		r := newReconciler()
		startClone(r, name)

		dropWorker(name)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(jobMissing(name + "-run-2")).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFinalizing))
		Expect(ownership(m).Reason).To(Equal("OwnershipRunning"))

		finishJob(ctx, name+"-reown", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
		Expect(m.Status.Attempts).To(Equal(int32(1)))
	})

	It("starts no worker for a live migration whose worker Job expires during the handover", func() {
		const name = "mig-reown-follow-expired"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, reownFollow(name))).To(Succeed())
		r := newReconciler()
		startFollow(r, name)

		dropWorker(name)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(jobMissing(name + "-run-2")).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))

		finishJob(ctx, name+"-reown", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeTrue())
		fetchJob(ctx, name+"-cleanup")
		Expect(m.Status.Attempts).To(Equal(int32(1)))
	})

	It("restarts a live worker that vanishes mid-stream even when a handover is requested", func() {
		const name = "mig-reown-follow-midstream"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, reownFollow(name))).To(Succeed())
		r := newReconciler()
		passGate(ctx, r, name)

		// No handover Job exists, so nothing says the worker exited 0: the
		// guard must let the ordinary resume happen.
		dropWorker(name)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(m.Status.JobName).To(Equal(name + "-run-2"))
		fetchJob(ctx, name+"-run-2")
		Expect(jobMissing(name + "-reown")).To(BeTrue())
	})

	It("stops a running handover on suspend and re-runs it on resume without a worker", func() {
		const name = "mig-reown-suspend"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, reownClone(name))).To(Succeed())
		r := newReconciler()
		rec := r.Recorder.(*events.FakeRecorder)
		startClone(r, name)
		fetchJob(ctx, name+"-reown")
		drainEvents(rec)

		setSuspend(name, true)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
		Expect(jobGoing(name + "-reown")).To(BeTrue())
		Expect(ownership(m).Status).To(Equal(metav1.ConditionUnknown))
		Expect(ownership(m).Reason).To(Equal("OwnershipSuspended"))
		Expect(drainEvents(rec)).To(ContainElement(SatisfyAll(
			ContainSubstring("Suspended"), ContainSubstring("handover"))))

		collectJob(name + "-reown")
		Expect(jobMissing(name + "-reown")).To(BeTrue())

		// Resume goes through the finish path: the handover Job comes back,
		// no worker does.
		setSuspend(name, false)
		m = reconcileAndGet(ctx, r, name)
		fetchJob(ctx, name+"-reown")
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(jobMissing(name + "-run-2")).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFinalizing))
		Expect(ownership(m).Reason).To(Equal("OwnershipRunning"))
	})

	It("keeps a finished handover Job through a suspend", func() {
		const name = "mig-reown-suspend-done"
		defer removeMigration(ctx, name)
		m := reownClone(name)
		// A compare after the handover keeps the Migration out of its terminal
		// state, so the suspend has something to act on.
		m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		r := newReconciler()
		startClone(r, name)
		finishJob(ctx, name+"-reown", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseVerifying))
		fetchJob(ctx, name+"-compare-schema")

		setSuspend(name, true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
		Expect(fetchJob(ctx, name+"-reown").DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionOwnershipApplied)).To(BeTrue())
	})
})
