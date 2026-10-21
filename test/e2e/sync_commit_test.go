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
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
)

// syncCommitTable is the fixture table the synchronousCommit specs clone on their own.
const syncCommitTable = "public.customers"

var _ = Describe("synchronousCommit on a direct target", func() {
	It("starts the worker's target sessions with synchronous_commit local and leaves the source alone", func() {
		// The lock holder execs into the source primary.
		requireCNPGFixtures()
		const name = "e2e-sync-commit"
		DeferCleanup(func() {
			deleteMigration(name)
			resetTargetObjects()
		})
		resetTargetObjects()

		By("locking " + syncCommitTable + " on the source so the worker's pg_dump waits")
		// pg_dump locks every table it dumps, so this lock stalls the worker. It outlasts the worker
		// pod wait and the SHOW checks after it.
		release := holdLockFor(sourceKey, sourceCluster, syncCommitTable, workerRunningTimeout+2*time.Minute)

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			DropIfExists: true,
			Filters:      &v1beta1.Filters{IncludeOnlyTables: []string{syncCommitTable}},
		})
		m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
		create(m)

		By("waiting for the attempt-1 worker pod to run")
		pod := waitWorkerRunning(name, 1)

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
		const count = "SELECT count(*) FROM " + syncCommitTable
		source := psql(sourceCluster, count)
		Expect(source).NotTo(Equal("0"), "the source fixture has no customers to compare")
		Expect(psql(targetCluster, count)).To(Equal(source))
	})
})

// The Pooler fronts a dedicated cluster: managed Pooler authentication installs objects in its postgres database.
var _ = Describe("synchronousCommit through a transaction-mode Pooler", SpecPriority(1), func() {
	It("fails the preflight on the pooler check and still reports the dropped startup option", func() {
		requireCNPGFixtures()
		dedicated := createDedicatedTarget("e2e-gate-pool-", progressPoolStorageSize, 1, nil)
		// Created after the cluster, so its cleanup runs first.
		pooler := newTransactionPooler(dedicated, nil)
		const name = "e2e-gate-pool-preflight"
		DeferCleanup(func() { deleteMigration(name) })
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			Filters: &v1beta1.Filters{IncludeOnlyTables: []string{syncCommitTable}},
		})
		m.Spec.Target = pooledTarget(pooler, dedicated)
		m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
		create(m)

		failed := waitFailed(name, "PreflightFailed")
		msg := failureMessage(failed)
		// The PIDs change with every run, so only their shape is pinned.
		Expect(msg).To(MatchRegexp(`preflight: the target connection goes through a connection pooler such as ` +
			`PgBouncer \(the server's backend PID is \d+, the connection reported -?\d+\); connect directly to PostgreSQL`))
		Expect(msg).NotTo(ContainSubstring("the source connection goes through"), "the shared source is direct")
		Expect(msg).NotTo(ContainSubstring("cannot tell whether"), "the pooler probe must read both sides")
		// The pooler check notes and continues, so the SHOW check still reports.
		Expect(msg).To(ContainSubstring("reports synchronous_commit=on"))
		Expect(msg).To(ContainSubstring("a connection pooler or proxy dropped the libpq startup option"))
		Expect(msg).NotTo(ContainSubstring("cannot connect to the target"),
			"the Pooler drops the option, so the connection itself must succeed")
		Expect(failed.Status.Attempts).To(BeZero())
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(nsE2E),
			client.MatchingLabels{migrationLabel: name})).To(Succeed())
		names := make([]string, 0, len(jobs.Items))
		for i := range jobs.Items {
			names = append(names, jobs.Items[i].Name)
		}
		// The preflight Job proves that the listing sees this Migration's Jobs.
		Expect(names).To(ContainElement(name + "-preflight"))
		Expect(names).NotTo(ContainElement(name + "-run-1"))
		Expect(names).NotTo(ContainElement(HavePrefix(name + "-gate-")))
	})
})
