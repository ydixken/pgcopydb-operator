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

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
)

var _ = Describe("synchronousCommit on a direct target", func() {
	It("starts the worker's target sessions with synchronous_commit local and leaves the source alone", func() {
		// The lock holder execs into the source primary.
		requireCNPGFixtures()
		const name = "e2e-sync-commit"
		const table = "public.customers"
		DeferCleanup(func() {
			deleteMigration(name)
			resetTargetObjects()
		})
		resetTargetObjects()

		By("locking " + table + " on the source so the worker's pg_dump waits")
		// pg_dump locks every table it dumps. 10 minutes outlive the preflight and
		// the pod start of a loaded candidate run, where 140 s may not.
		release := holdLockFor(sourceKey, sourceCluster, table, 10*time.Minute)

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			DropIfExists: true,
			Filters:      &v1beta1.Filters{IncludeOnlyTables: []string{table}},
		})
		m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
		create(m)

		By("waiting for the attempt-1 worker pod to run")
		var pod string
		Eventually(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, cur)).To(Succeed())
			if cur.Status.Phase == v1beta1.PhaseFailed {
				StopTrying("migration " + name + " failed: " + failureMessage(cur)).Now()
			}
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E), client.MatchingLabels{
				migrationLabel: name, batchv1.JobNameLabel: name + "-run-1",
			})).To(Succeed())
			g.Expect(pods.Items).To(HaveLen(1), "attempt-1 pod not created yet")
			g.Expect(pods.Items[0].Status.Phase).To(Equal(corev1.PodRunning))
			pod = pods.Items[0].Name
		}, 5*time.Minute, time.Second).Should(Succeed())

		By("reading synchronous_commit over the worker's own target and source URIs")
		// A pod turns Running before its prelude has written the passfile.
		Eventually(func(g Gomega) {
			target, err := workerSQL(pod, conn.Target, "SHOW synchronous_commit;", 10*time.Second)
			g.Expect(err).NotTo(HaveOccurred(), "target SHOW through the worker failed")
			g.Expect(target).To(Equal(v1beta1.SynchronousCommitLocal),
				"the worker's target URI must start its sessions with synchronous_commit local")
			source, err := workerSQL(pod, conn.Source, "SHOW synchronous_commit;", 10*time.Second)
			g.Expect(err).NotTo(HaveOccurred(), "source SHOW through the worker failed")
			g.Expect(source).NotTo(BeEmpty(), "source SHOW returned no value")
			g.Expect(source).NotTo(Equal(v1beta1.SynchronousCommitLocal),
				"the worker's source URI must not carry the target-only option")
		}, time.Minute, 2*time.Second).Should(Succeed())
		cur := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, cur)).To(Succeed())
		expectConditionTrue(cur, v1beta1.ConditionValidated)

		By("releasing the lock so the clone completes on its first attempt")
		release()
		done := waitCompleted(name, nsE2E)
		expectSingleAttempt(done)
		Expect(done.Status.ExemptAttempts).To(BeZero())
		const count = "SELECT count(*) FROM " + table
		source := psql(sourceCluster, count)
		Expect(source).NotTo(Equal("0"), "the source fixture has no customers to compare")
		Expect(psql(targetCluster, count)).To(Equal(source))
	})
})
