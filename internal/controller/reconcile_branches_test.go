/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

package controller

import (
	"context"
	stderrors "errors"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// Recovery and edge branches of the reconciler: Jobs that vanish mid-run,
// suspension of live migrations, failed cleanup, deletion without follow, and
// scripted sentinel failures.
var _ = Describe("Migration Controller resilience", func() {
	ctx := context.Background()

	getM := func(name string) *v1beta1.Migration {
		m := &v1beta1.Migration{}
		ExpectWithOffset(1, k8sClient.Get(ctx,
			types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}

	jobGone := func(name string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, &batchv1.Job{})
		return errors.IsNotFound(err)
	}

	It("resumes with a fresh attempt when the worker Job vanishes mid-run", func() {
		const name = "mig-job-vanished"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())

		// A real progress poller against envtest: no worker pods exist, so a
		// poll returns no sample and the previous status numbers stand.
		r := newReconciler()
		e, err := podexec.New(cfg)
		Expect(err).NotTo(HaveOccurred())
		r.Poller = progress.NewFromExec(e)

		reconcileAndGet(ctx, r, name) // creates run-1
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Progress).To(BeNil())
		Expect(m.Status.Attempts).To(Equal(int32(1)))

		// TTL or a manual delete removed the Job mid-run: the next pass must
		// start attempt 2 resuming from the work-dir catalogs, not fail.
		Expect(k8sClient.Delete(ctx, fetchJob(ctx, name+"-run-1"),
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(m.Status.JobName).To(Equal(name + "-run-2"))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		args := strings.Join(fetchJob(ctx, name+"-run-2").Spec.Template.Spec.Containers[0].Args, " ")
		Expect(args).To(ContainSubstring("--resume"))
		Expect(args).NotTo(ContainSubstring("--restart"))
	})

	It("warns about the retained replication slot when a follow migration suspends", func() {
		const name = "mig-follow-suspend"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		r := newReconciler()
		r.Sentinel = fake
		reconcileAndGet(ctx, r, name) // preflight
		finishJob(ctx, name+"-preflight", true)
		reconcileAndGet(ctx, r, name) // run-1

		fresh := getM(name)
		fresh.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())

		r2 := newReconciler()
		r2.Sentinel = fake
		m = reconcileAndGet(ctx, r2, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
		Expect(m.Status.JobName).To(BeEmpty())
		evts := drainEvents(r2.Recorder.(*events.FakeRecorder))
		Expect(evts).To(ContainElement(ContainSubstring("Suspended")))
		// The slot outlives the worker on purpose (resume needs it), and that
		// retains WAL on the source; the operator must say so loudly.
		Expect(evts).To(ContainElement(SatisfyAll(
			ContainSubstring("SlotRetained"), ContainSubstring("retains WAL"))))
	})

	It("suspends cleanly when the worker Job is already gone", func() {
		const name = "mig-suspend-nojob"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		reconcileAndGet(ctx, newReconciler(), name) // run-1
		Expect(k8sClient.Delete(ctx, fetchJob(ctx, name+"-run-1"),
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())

		fresh := getM(name)
		fresh.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())

		m := reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
		Expect(m.Status.JobName).To(BeEmpty())
	})

	It("completes with a loud warning when stream cleanup fails, then still verifies", func() {
		const name = "mig-cleanup-fail"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverAutomatic}
		m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		r := newReconciler()
		r.Sentinel = fake
		reconcileAndGet(ctx, r, name) // preflight
		finishJob(ctx, name+"-preflight", true)
		reconcileAndGet(ctx, r, name) // run-1

		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		reconcileAndGet(ctx, r, name) // caught up, Automatic: endpos set
		Expect(fake.endposSet).To(BeTrue())

		finishJob(ctx, name+"-run-1", true)
		reconcileAndGet(ctx, r, name) // drain-verify Job created
		reconcileAndGet(ctx, r, name) // still running: nothing changes
		finishJob(ctx, name+"-verify", true)
		m = reconcileAndGet(ctx, r, name) // cleanup Job created
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeTrue())
		reconcileAndGet(ctx, r, name) // cleanup still running

		// Cleanup exhausts its retries: the Migration must not block forever
		// on it, but the possibly leaking slot needs an operator's attention.
		finishJob(ctx, name+"-cleanup", false)
		r2 := newReconciler()
		r2.Sentinel = fake
		m = reconcileAndGet(ctx, r2, name)
		Expect(drainEvents(r2.Recorder.(*events.FakeRecorder))).To(ContainElement(SatisfyAll(
			ContainSubstring("CleanupFailed"), ContainSubstring("manual removal"))))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseVerifying))

		reconcileAndGet(ctx, r2, name) // compare still running
		finishJob(ctx, name+"-compare-schema", true)
		m = reconcileAndGet(ctx, r2, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed)).To(BeFalse())
	})

	It("deletes a clone-only Migration without running any cleanup Job", func() {
		const name = "mig-clone-delete"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		reconcileAndGet(ctx, newReconciler(), name) // run-1

		// A test finalizer keeps the terminating CR observable (envtest has no
		// garbage collector); the operator's own finalizer is follow-only.
		const hold = "test.pgcopydb-operator.io/hold"
		fresh := getM(name)
		Expect(fresh.Finalizers).NotTo(ContainElement(finalizerName))
		fresh.Finalizers = append(fresh.Finalizers, hold)
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())
		Expect(k8sClient.Delete(ctx, fresh)).To(Succeed())

		m := reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.DeletionTimestamp.IsZero()).To(BeFalse())
		// No replication state exists, so no cleanup Job may ever be created.
		Expect(jobGone(name + "-cleanup")).To(BeTrue())

		m.Finalizers = nil
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		Expect(errors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: name, Namespace: testNS}, &v1beta1.Migration{}))).To(BeTrue())
	})

	It("releases the finalizer when the namespace terminates mid-cleanup", func() {
		// Live finding 2026-08-09: deleting a whole namespace forbade the
		// cleanup Job create (Forbidden, cause NamespaceTerminating), the
		// controller retried forever, and the namespace hung on the
		// finalizer. envtest reproduces the apiserver side exactly: no
		// namespace controller ever finishes the termination, and the
		// NamespaceLifecycle admission plugin rejects new Jobs in the
		// terminating namespace with that cause.
		const name = "mig-ns-terminating"
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		m := validMigration(name)
		m.Namespace = name
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		m.Finalizers = []string{finalizerName}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		// Attempts > 0 makes deletion want cleanup; JobName stays empty so
		// no worker-stop pass runs first.
		m.Status.Attempts = 1
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())

		Expect(k8sClient.Delete(ctx, ns)).To(Succeed())
		Expect(k8sClient.Delete(ctx, m)).To(Succeed())

		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: name},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(drainEvents(r.Recorder.(*events.FakeRecorder))).To(ContainElement(SatisfyAll(
			ContainSubstring("CleanupFailed"), ContainSubstring("terminating"),
			ContainSubstring(effectiveSlotName(m)))))
		Expect(errors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: name, Namespace: name}, &v1beta1.Migration{}))).To(BeTrue())
	})

	It("honors a custom maxCatchupLag instead of the built-in default", func() {
		const name = "mig-custom-lag"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		m := validMigration(name)
		lag := resource.MustParse("1Ki")
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin, MaxCatchupLag: &lag}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		r := newReconciler()
		r.Sentinel = fake
		reconcileAndGet(ctx, r, name) // preflight
		finishJob(ctx, name+"-preflight", true)
		reconcileAndGet(ctx, r, name) // run-1

		// 8KiB behind: far below the 16Mi default, above the 1Ki spec value.
		// Only the custom threshold explains a CaughtUp=False here.
		const head = "0/4000"
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: head, ReplayLSN: "0/2000", SourceHead: head, Endpos: sentinel.ZeroLSN}
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
		Expect(meta.IsStatusConditionFalse(m.Status.Conditions, v1beta1.ConditionCaughtUp)).To(BeTrue())

		// 256 bytes behind: under the custom threshold.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: head, ReplayLSN: "0/3F00", SourceHead: head, Endpos: sentinel.ZeroLSN}
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCaughtUp)).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCutoverPending))
	})

	It("tolerates sentinel read errors and retries a failed endpos set", func() {
		const name = "mig-sentinel-errors"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		r := newReconciler()
		r.Sentinel = fake
		reconcileAndGet(ctx, r, name) // preflight
		finishJob(ctx, name+"-preflight", true)
		reconcileAndGet(ctx, r, name) // run-1

		// A failing sentinel read (pod restarting) is a missed sample, not a
		// reconcile error: the pass completes and status stays put.
		fake.readErr = stderrors.New("exec transport down")
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(m.Status.Replication).To(BeNil())
		fake.readErr = nil

		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		fresh := getM(name)
		fresh.Spec.Cutover.Approved = true
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())

		// Setting the endpos fails transiently: warn, keep streaming, retry.
		fake.setErr = stderrors.New("pod restarting")
		r2 := newReconciler()
		r2.Sentinel = fake
		m = reconcileAndGet(ctx, r2, name)
		Expect(fake.endposSet).To(BeFalse())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
		Expect(drainEvents(r2.Recorder.(*events.FakeRecorder))).To(ContainElement(SatisfyAll(
			ContainSubstring("CutoverRetry"), ContainSubstring("pod restarting"))))

		fake.setErr = nil
		m = reconcileAndGet(ctx, r2, name)
		Expect(fake.endposSet).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
	})

	It("refuses to adopt a work PVC it does not own", func() {
		const name = "mig-foreign-pvc"
		defer removeMigration(ctx, name)
		// A leftover PVC with the right name but no owner: adopting it would
		// hand this Migration's catalogs to a volume another owner may still
		// garbage-collect away.
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-work", Namespace: testNS},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pvc) })
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())

		_, err := newReconciler().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
		})
		Expect(err).To(MatchError(ContainSubstring("belongs to another owner")))
		Expect(jobGone(name + "-run-1")).To(BeTrue())
	})
})
