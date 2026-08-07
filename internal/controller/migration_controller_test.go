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

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

const testRunnerImage = "ghcr.io/example/runner:test"

// testNS is where every test Migration lives; envtest ships the namespace.
const testNS = "default"

// testDB is the database/username used by the canned valid spec.
const testDB = "app"

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

func validMigration(name string) *v1alpha1.Migration {
	return &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: v1alpha1.MigrationSpec{
			Source: v1alpha1.PostgresConnection{
				Host: "source.example.com", Database: testDB, Username: "migrator",
				PasswordSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "src-credentials"},
					Key:                  "password",
				},
			},
			Target: v1alpha1.PostgresConnection{
				Host: "target.example.com", Database: testDB, Username: testDB,
			},
		},
	}
}

var _ = Describe("Migration Controller", func() {
	ctx := context.Background()

	reconcileOnce := func(name string) {
		_, err := newReconciler().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
		})
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
	}

	getMigration := func(name string) *v1alpha1.Migration {
		m := &v1alpha1.Migration{}
		ExpectWithOffset(1, k8sClient.Get(ctx,
			types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}

	getJob := func(name string) *batchv1.Job {
		j := &batchv1.Job{}
		ExpectWithOffset(1, k8sClient.Get(ctx,
			types.NamespacedName{Name: name, Namespace: testNS}, j)).To(Succeed())
		return j
	}

	// markJob flips the worker Job to a terminal state, standing in for the
	// Job controller that envtest does not run.
	markJob := func(name string, succeeded bool) {
		j := getJob(name)
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

	cleanup := func(name string) {
		m := &v1alpha1.Migration{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m); err == nil {
			Expect(k8sClient.Delete(ctx, m)).To(Succeed())
		}
		// envtest has no garbage collector; remove owned objects by label.
		sel := []client.DeleteAllOfOption{
			client.InNamespace(testNS),
			client.MatchingLabels(map[string]string{labelMigration: name}),
		}
		_ = k8sClient.DeleteAllOf(ctx, &batchv1.Job{}, sel...)
		_ = k8sClient.DeleteAllOf(ctx, &corev1.PersistentVolumeClaim{}, sel...)
		_ = k8sClient.DeleteAllOf(ctx, &corev1.ConfigMap{}, sel...)
	}

	It("creates the work PVC and the first attempt Job", func() {
		const name = "mig-first-attempt"
		defer cleanup(name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		reconcileOnce(name)

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-work", Namespace: testNS}, pvc)).To(Succeed())
		Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("10Gi"))

		job := getJob(name + "-run-1")
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

		m := getMigration(name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCloning))
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(m.Status.JobName).To(Equal(name + "-run-1"))
		Expect(m.Status.StartedAt).NotTo(BeNil())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionValidated)).To(BeTrue())
		Expect(meta.IsStatusConditionFalse(m.Status.Conditions, v1alpha1.ConditionCloneCompleted)).To(BeTrue())
	})

	It("completes when the worker Job succeeds", func() {
		const name = "mig-complete"
		defer cleanup(name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		reconcileOnce(name)
		markJob(name+"-run-1", true)
		reconcileOnce(name)

		m := getMigration(name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCompleted))
		Expect(m.Status.CompletedAt).NotTo(BeNil())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionCloneCompleted)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete)).To(BeTrue())

		// Terminal state is absorbing: another pass changes nothing.
		reconcileOnce(name)
		Expect(getMigration(name).Status.Attempts).To(Equal(int32(1)))
	})

	It("retries with --resume and fails after the budget", func() {
		const name = "mig-retry"
		defer cleanup(name)
		m := validMigration(name)
		m.Spec.BackoffLimit = 1 // 1 retry, so 2 attempts total
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		reconcileOnce(name)

		markJob(name+"-run-1", false)
		reconcileOnce(name) // observes failure, clears jobName
		reconcileOnce(name) // starts attempt 2

		job2 := getJob(name + "-run-2")
		args := strings.Join(job2.Spec.Template.Spec.Containers[0].Args, " ")
		Expect(args).To(ContainSubstring("--resume"))
		Expect(args).To(ContainSubstring("--not-consistent"))
		Expect(getMigration(name).Status.Attempts).To(Equal(int32(2)))

		markJob(name+"-run-2", false)
		reconcileOnce(name)

		final := getMigration(name)
		Expect(final.Status.Phase).To(Equal(v1alpha1.PhaseFailed))
		Expect(meta.IsStatusConditionTrue(final.Status.Conditions, v1alpha1.ConditionFailed)).To(BeTrue())
	})

	It("renders filters into an owned ConfigMap and wires the flag", func() {
		const name = "mig-filters"
		defer cleanup(name)
		m := validMigration(name)
		m.Spec.Clone.Filters = &v1alpha1.Filters{ExcludeSchemas: []string{"audit"}}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		reconcileOnce(name)

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-filters", Namespace: testNS}, cm)).To(Succeed())
		Expect(cm.Data["filters.ini"]).To(Equal("[exclude-schema]\naudit\n"))

		job := getJob(name + "-run-1")
		Expect(strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")).
			To(ContainSubstring("--filters /etc/pgcopydb/conf/filters.ini"))
	})

	It("suspends by deleting the worker and resumes with a fresh attempt", func() {
		const name = "mig-suspend"
		defer cleanup(name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		reconcileOnce(name)

		m := getMigration(name)
		m.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		reconcileOnce(name)

		m = getMigration(name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseSuspended))
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
		reconcileOnce(name)

		m = getMigration(name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCloning))
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		args := strings.Join(getJob(fmt.Sprintf("%s-run-2", name)).Spec.Template.Spec.Containers[0].Args, " ")
		Expect(args).To(ContainSubstring("--resume"))
	})

	It("converges without error or duplicate events when a pass holds a stale object", func() {
		const name = "mig-stale"
		defer cleanup(name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())

		// drainEvents empties the fake recorder so each phase of the test
		// only sees the events it caused.
		drainEvents := func(rec *events.FakeRecorder) []string {
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
		reconcileOnce(name)
		m := getMigration(name)
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(m.Status.JobName).To(Equal(name + "-run-1"))
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCloning))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionValidated)).To(BeTrue())
		Expect(getJob(name + "-run-1")).NotTo(BeNil())
	})
})
