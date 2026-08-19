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
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const testRunnerImage = "ghcr.io/example/runner:test"

// testPodImage and testPodContainer fill test-fabricated pods; envtest runs
// no kubelet, so neither value ever resolves.
const testPodImage = "img"
const testPodContainer = "runner"

// testNS is where every test Migration lives; envtest ships the namespace.
const testNS = "default"

// testDB is the database/username used by the canned valid spec.
const testDB = "app"

// testPasswordKey is the secret key every test connection reads its password
// from.
const testPasswordKey = "password"

// newReconciler builds the reconciler under test. envtest runs no Job
// controller and no garbage collector, so tests drive Job status by hand,
// which is exactly what makes every controller path deterministic here.
func newReconciler() *MigrationReconciler {
	return &MigrationReconciler{
		Client:      k8sClient,
		Scheme:      k8sClient.Scheme(),
		Recorder:    events.NewFakeRecorder(100),
		RunnerImage: testRunnerImage,
	}
}

// reconcileAndGet runs one reconcile pass with r and returns the fresh
// Migration. The one reconcile-and-observe helper for every suite; tests that
// need a sentinel or log reader wire it into r first.
func reconcileAndGet(ctx context.Context, r *MigrationReconciler, name string) *v1beta1.Migration {
	GinkgoHelper()
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
	})
	Expect(err).NotTo(HaveOccurred())
	m := &v1beta1.Migration{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
	return m
}

// passGate drives a fresh Migration through the universal preflight gate:
// the first pass creates only the preflight Job, its success unlocks the
// worker. Shared by every suite in this package.
func passGate(ctx context.Context, r *MigrationReconciler, name string) {
	GinkgoHelper()
	reconcileAndGet(ctx, r, name)
	finishJob(ctx, name+"-preflight", true)
	reconcileAndGet(ctx, r, name)
}

// drainEvents empties the fake recorder so each phase of a test only sees the
// events it caused.
func drainEvents(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func validMigration(name string) *v1beta1.Migration {
	return &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: v1beta1.MigrationSpec{
			Source: v1beta1.PostgresConnection{
				Host: "source.example.com", Database: testDB, Username: "migrator",
				PasswordSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "src-credentials"},
					Key:                  testPasswordKey,
				},
			},
			Target: v1beta1.PostgresConnection{
				Host: "target.example.com", Database: testDB, Username: testDB,
			},
		},
	}
}

var _ = Describe("Migration Controller", func() {
	ctx := context.Background()

	getMigration := func(name string) *v1beta1.Migration {
		m := &v1beta1.Migration{}
		ExpectWithOffset(1, k8sClient.Get(ctx,
			types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}

	It("creates the work PVC and the first attempt Job", func() {
		const name = "mig-first-attempt"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		m := reconcileAndGet(ctx, newReconciler(), name)

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-work", Namespace: testNS}, pvc)).To(Succeed())
		Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("10Gi"))

		// The gate is universal: even a plain clone starts with the preflight
		// (connectivity tier), and the worker waits for it.
		Expect(fetchJob(ctx, name+"-preflight")).NotTo(BeNil())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseValidating))
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated.Status).To(Equal(metav1.ConditionUnknown))
		Expect(validated.Reason).To(Equal("PreflightRunning"))
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-run-1", Namespace: testNS}, &batchv1.Job{})
		Expect(errors.IsNotFound(err)).To(BeTrue())

		finishJob(ctx, name+"-preflight", true)
		reconcileAndGet(ctx, newReconciler(), name)

		job := fetchJob(ctx, name+"-run-1")
		c := job.Spec.Template.Spec.Containers[0]
		Expect(c.Image).To(Equal(testRunnerImage))
		Expect(strings.Join(c.Args, " ")).To(Equal("clone --dir /work/pgcopydb --restart"))
		Expect(strings.Join(c.Args, " ")).NotTo(ContainSubstring("--resume"))
		// Source password travels via the prelude-assembled passfile.
		Expect(c.Command[2]).To(ContainSubstring("PGPASSFILE"))
		envNames := map[string]string{}
		for _, e := range c.Env {
			envNames[e.Name] = e.Value
		}
		Expect(envNames).To(HaveKey("PGCOPYDB_SOURCE_PGURI"))
		Expect(envNames["PGCOPYDB_SOURCE_PGURI"]).NotTo(ContainSubstring("password"))
		Expect(*job.Spec.BackoffLimit).To(Equal(int32(0)))
		Expect(*job.Spec.Template.Spec.SecurityContext.RunAsUser).To(Equal(int64(65532)))

		m = getMigration(name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(m.Status.JobName).To(Equal(name + "-run-1"))
		Expect(m.Status.StartedAt).NotTo(BeNil())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionValidated)).To(BeTrue())
		Expect(meta.IsStatusConditionFalse(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
	})

	It("completes when the worker Job succeeds", func() {
		const name = "mig-complete"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		passGate(ctx, newReconciler(), name)
		finishJob(ctx, name+"-run-1", true)

		m := reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
		Expect(m.Status.CompletedAt).NotTo(BeNil())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeTrue())

		// Terminal state is absorbing: another pass changes nothing.
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Attempts).To(Equal(int32(1)))
	})

	It("retries with --resume and fails after the budget", func() {
		const name = "mig-retry"
		defer removeMigration(ctx, name)
		m := validMigration(name)
		m.Spec.BackoffLimit = 1 // 1 retry, so 2 attempts total
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		passGate(ctx, newReconciler(), name)

		finishJob(ctx, name+"-run-1", false)
		reconcileAndGet(ctx, newReconciler(), name)     // observes failure, clears jobName
		m = reconcileAndGet(ctx, newReconciler(), name) // starts attempt 2

		job2 := fetchJob(ctx, name+"-run-2")
		args := strings.Join(job2.Spec.Template.Spec.Containers[0].Args, " ")
		Expect(args).To(ContainSubstring("--resume"))
		Expect(args).To(ContainSubstring("--not-consistent"))
		Expect(m.Status.Attempts).To(Equal(int32(2)))

		finishJob(ctx, name+"-run-2", false)
		final := reconcileAndGet(ctx, newReconciler(), name)
		Expect(final.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		Expect(meta.IsStatusConditionTrue(final.Status.Conditions, v1beta1.ConditionFailed)).To(BeTrue())
	})

	It("fails without a fresh attempt when the final attempt's Job vanishes", func() {
		const name = "mig-vanished-budget"
		defer removeMigration(ctx, name)
		m := validMigration(name)
		m.Spec.BackoffLimit = 1 // 1 retry, so 2 attempts total
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		passGate(ctx, newReconciler(), name) // run-1
		finishJob(ctx, name+"-run-1", false)
		reconcileAndGet(ctx, newReconciler(), name) // observes failure, clears jobName
		reconcileAndGet(ctx, newReconciler(), name) // run-2, the final attempt

		// The final attempt's Job disappears while the budget is spent (TTL or
		// manual delete): the next pass must fail instead of starting a third
		// attempt. Background propagation, because envtest never removes the
		// orphan finalizer that a default Job delete would leave behind.
		Expect(k8sClient.Delete(ctx, fetchJob(ctx, name+"-run-2"),
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Reason).To(Equal("BackoffLimitExceeded"))
		Expect(failed.Message).To(ContainSubstring("retry budget exhausted after 2 attempts"))
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(errors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: name + "-run-3", Namespace: testNS}, &batchv1.Job{}))).To(BeTrue())
	})

	It("surfaces the worker's terminal error from structured logs", func() {
		const name = "mig-error-surface"
		defer removeMigration(ctx, name)
		m := validMigration(name)
		m.Spec.BackoffLimit = 1 // 1 retry, so 2 attempts total
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		const lastError = "permission denied for function pg_replication_origin_drop"
		r := newReconciler()
		r.Logs = &fakeLogs{out: `{"error_severity":"INFO","message":"STEP 1: setup"}` + "\n" +
			`{"error_severity":"ERROR","message":"` + lastError + `"}` + "\n"}
		rec := r.Recorder.(*events.FakeRecorder)

		passGate(ctx, r, name) // run-1
		finishJob(ctx, name+"-run-1", false)
		reconcileAndGet(ctx, r, name) // failure observed: the retry event carries the log detail
		Expect(drainEvents(rec)).To(ContainElement(SatisfyAll(
			ContainSubstring("AttemptFailed"), ContainSubstring(lastError))))

		reconcileAndGet(ctx, r, name) // run-2
		finishJob(ctx, name+"-run-2", false)
		// Budget spent: the Failed condition carries the log detail.
		final := reconcileAndGet(ctx, r, name)
		Expect(final.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		failed := meta.FindStatusCondition(final.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Message).To(ContainSubstring(jobFailedMsg))
		Expect(failed.Message).To(ContainSubstring(lastError))
	})

	It("keeps the Job's failure message when worker logs are unreadable", func() {
		const name = "mig-error-nologs"
		defer removeMigration(ctx, name)
		m := validMigration(name)
		m.Spec.BackoffLimit = 1 // CRD defaulting forbids 0 via omitempty
		Expect(k8sClient.Create(ctx, m)).To(Succeed())

		r := newReconciler()
		r.Logs = &fakeLogs{err: fmt.Errorf("pods \"gone\" not found")}
		passGate(ctx, r, name) // run-1
		finishJob(ctx, name+"-run-1", false)
		reconcileAndGet(ctx, r, name) // retry scheduled
		reconcileAndGet(ctx, r, name) // run-2
		finishJob(ctx, name+"-run-2", false)
		// Budget spent.
		final := reconcileAndGet(ctx, r, name)
		Expect(final.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		failed := meta.FindStatusCondition(final.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Message).To(ContainSubstring(jobFailedMsg))
		Expect(failed.Message).NotTo(ContainSubstring("last error"))
	})

	It("renders filters into an owned ConfigMap and wires the flag", func() {
		const name = "mig-filters"
		defer removeMigration(ctx, name)
		m := validMigration(name)
		m.Spec.Clone.Filters = &v1beta1.Filters{ExcludeSchemas: []string{"audit"}}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		passGate(ctx, newReconciler(), name)

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-filters", Namespace: testNS}, cm)).To(Succeed())
		Expect(cm.Data["filters.ini"]).To(Equal("[exclude-schema]\naudit\n"))

		job := fetchJob(ctx, name+"-run-1")
		Expect(strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")).
			To(ContainSubstring("--filters /etc/pgcopydb/conf/filters.ini"))
	})

	It("suspends by deleting the worker and resumes with a fresh attempt", func() {
		const name = "mig-suspend"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		passGate(ctx, newReconciler(), name)

		m := getMigration(name)
		m.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
		Expect(m.Status.JobName).To(BeEmpty())
		// Foreground deletion in envtest leaves the object with a deletion
		// timestamp (no GC runs); that is the observable "being deleted".
		job := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-run-1", Namespace: testNS}, job)
		if err == nil {
			Expect(job.DeletionTimestamp.IsZero()).To(BeFalse())
		} else {
			Expect(errors.IsNotFound(err)).To(BeTrue())
		}

		m.Spec.Suspend = false
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		args := strings.Join(fetchJob(ctx, fmt.Sprintf("%s-run-2", name)).Spec.Template.Spec.Containers[0].Args, " ")
		Expect(args).To(ContainSubstring("--resume"))
	})

	It("suspends during the gate by deleting the preflight and re-runs it on resume", func() {
		const name = "mig-suspend-gate"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		// First pass creates the preflight; it is NOT finished: the Migration
		// sits mid-gate, where remediation may be writing to the databases.
		reconcileAndGet(ctx, newReconciler(), name)
		Expect(fetchJob(ctx, name+"-preflight")).NotTo(BeNil())

		m := getMigration(name)
		m.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
		job := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-preflight", Namespace: testNS}, job)
		if err == nil {
			Expect(job.DeletionTimestamp.IsZero()).To(BeFalse())
		} else {
			Expect(errors.IsNotFound(err)).To(BeTrue())
		}

		m.Spec.Suspend = false
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseValidating))
		Expect(fetchJob(ctx, name+"-preflight")).NotTo(BeNil())
		finishJob(ctx, name+"-preflight", true)
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
	})

	It("converges without error or duplicate events when a pass holds a stale object", func() {
		const name = "mig-stale"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())

		// Clear the universal gate first so pass A starts the worker attempt.
		reconcileAndGet(ctx, newReconciler(), name)
		finishJob(ctx, name+"-preflight", true)

		// Pass B's view of the world: fetched before pass A writes status.
		stale := getMigration(name)
		base := stale.DeepCopy()

		// Pass A: creates the Job, emits AttemptStarted, writes status. The
		// Migration's resourceVersion moves past what pass B holds.
		r := newReconciler()
		rec := r.Recorder.(*events.FakeRecorder)
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(drainEvents(rec)).To(ContainElement(ContainSubstring("AttemptStarted")))

		// Pass B replays the attempt from its stale copy. The Job already
		// exists, so AttemptStarted must not fire again, and the status
		// patch must land despite the stale resourceVersion (a full Update
		// here is exactly the conflict seen in production).
		_, err = r.startAttempt(ctx, stale, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(drainEvents(rec)).NotTo(ContainElement(ContainSubstring("AttemptStarted")))

		// A follow-up pass converges on single-attempt state.
		m := reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(m.Status.JobName).To(Equal(name + "-run-1"))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionValidated)).To(BeTrue())
		Expect(fetchJob(ctx, name+"-run-1")).NotTo(BeNil())
	})

	It("distills the preflight pod state into a wait detail", func() {
		const jobName = "wait-detail-job"
		r := newReconciler()

		// makePod fabricates one labeled pod and stamps the given status;
		// envtest runs no kubelet, so tests own the status subresource.
		makePod := func(name string, status corev1.PodStatus) *corev1.Pod {
			GinkgoHelper()
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: name, Namespace: testNS,
					Labels: map[string]string{jobNameLabel: jobName},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: testPodContainer, Image: testPodImage}},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0)) })
			pod.Status = status
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
			return pod
		}

		By("returning nothing while no pod exists")
		Expect(r.preflightWaitDetail(ctx, testNS, jobName)).To(Equal(""))

		By("returning nothing for the transient ContainerCreating state")
		pod := makePod("wd-creating", corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: testPodContainer, Image: testPodImage,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
			}},
		})
		Expect(r.preflightWaitDetail(ctx, testNS, jobName)).To(Equal(""))
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())

		By("surfacing an unschedulable pod with the scheduler's message")
		pod = makePod("wd-unsched", corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable, Message: "0/3 nodes match the selector",
			}},
		})
		Expect(r.preflightWaitDetail(ctx, testNS, jobName)).
			To(Equal("pod unschedulable: 0/3 nodes match the selector"))
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())

		By("surfacing a message-less init-container waiting reason bare")
		pod = makePod("wd-initwait", corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "init", Image: testPodImage,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
			}},
		})
		Expect(r.preflightWaitDetail(ctx, testNS, jobName)).To(Equal("ImagePullBackOff"))
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())

		By("skipping running containers to reach the waiting one")
		pod = makePod("wd-mixed", corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "sidecar", Image: testPodImage,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				{Name: testPodContainer, Image: testPodImage,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
			},
		})
		Expect(r.preflightWaitDetail(ctx, testNS, jobName)).To(Equal("CrashLoopBackOff"))
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())

		By("degrading to nothing when the pod list itself fails")
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		Expect(r.preflightWaitDetail(cancelled, testNS, jobName)).To(Equal(""))

		By("propagating the same list failure from deleteJobPods, which must not eat it")
		Expect(r.deleteJobPods(cancelled, testNS, jobName)).To(HaveOccurred())
	})
})
