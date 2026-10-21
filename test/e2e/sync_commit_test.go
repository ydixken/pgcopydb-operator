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
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
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
var _ = Describe("synchronousCommit through a transaction-mode Pooler", Ordered, SpecPriority(3), func() {
	var dedicated, pooler *unstructured.Unstructured

	BeforeAll(func() {
		requireCNPGFixtures()
		dedicated = createDedicatedTarget("e2e-gate-pool-", progressPoolStorageSize, 1, nil)
		// Created after the cluster, so its cleanup runs first. With no reset query and no idle
		// timeout, an idle server connection keeps the last client's name until server_lifetime.
		pooler = newTransactionPooler(dedicated, map[string]string{
			"default_pool_size": "20", "max_client_conn": "100", "reserve_pool_size": "0",
			"server_reset_query_always": "0", "server_idle_timeout": "0", "server_lifetime": "3600",
		})
	})

	It("fails the preflight on the SHOW check because the Pooler drops the startup option", func() {
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

	It("holds a retry on idle pooled server connections until they end", func() {
		const name = "e2e-gate-pool-retry"
		schema := fmt.Sprintf("e2e_gate_pool_%d", GinkgoParallelProcess())
		key := client.ObjectKey{Namespace: nsE2E, Name: name}
		get := func(g Gomega) *v1beta1.Migration {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, cur)).To(Succeed())
			if cur.Status.Phase == v1beta1.PhaseFailed {
				StopTrying("migration " + name + " failed: " + failureMessage(cur)).Now()
			}
			return cur
		}
		// The app role's pgcopydb-named server connections on the dedicated cluster.
		pooledSessions := func() (idle, active, other int) {
			GinkgoHelper()
			out := psql(dedicated.GetName(), "SELECT count(*) FILTER (WHERE state = 'idle'), "+
				"count(*) FILTER (WHERE state = 'active'), count(*) FILTER (WHERE state IS DISTINCT FROM 'idle') "+
				"FROM pg_stat_activity WHERE usename = '"+cnpgAppRole+"' "+
				"AND application_name LIKE '"+progress.PgcopydbAppNameLike+"'")
			_, err := fmt.Sscanf(out, "%d|%d|%d", &idle, &active, &other)
			Expect(err).NotTo(HaveOccurred(), "session counts did not parse: %q", out)
			return idle, active, other
		}

		By("seeding three tables whose restored CHECK sleeps per row, and a small fourth one")
		DeferCleanup(func() { psql(sourceCluster, "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
		psql(sourceCluster, gatePoolFixtureSQL(schema, sqlIdent(appRole(sourceCluster))))

		By("cloning the schema through the Pooler with three table jobs")
		// Registered after the schema drop, so the worker goes before the schema does.
		DeferCleanup(func() { deleteMigration(name) })
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			TableJobs: 3,
			IndexJobs: 2,
			Filters:   &v1beta1.Filters{IncludeOnlySchemas: []string{schema}},
		})
		m.Spec.Target = pooledTarget(pooler, dedicated)
		m.Spec.BackoffLimit = 3
		create(m)

		By("waiting for the attempt-1 worker pod to run")
		waitWorkerRunning(name, 1)

		By("waiting for the target to run the three COPYs at once")
		// Three concurrent COPYs hold three server connections, and pg_dump has finished.
		// The window starts on a running pod: it times the dump, the restore and the COPY start.
		Eventually(func(g Gomega) {
			get(g)
			g.Expect(psql(dedicated.GetName(), fmt.Sprintf(`SELECT count(DISTINCT relid) FROM pg_stat_progress_copy
WHERE relid IN (to_regclass('%[1]s.a'), to_regclass('%[1]s.b'), to_regclass('%[1]s.c'))`, schema))).To(Equal("3"),
				"the target is not running the three COPYs at once")
		}, 3*time.Minute, time.Second).Should(Succeed())

		By("locking the smallest table, copied last, on the source")
		release := holdLockFor(sourceKey, sourceCluster, schema+".d", 20*time.Minute)

		By("waiting for its COPY to queue behind the lock while the worker's target sessions idle")
		Eventually(func(g Gomega) {
			get(g)
			g.Expect(psql(sourceCluster, `SELECT count(*) FROM pg_stat_activity a
WHERE a.application_name LIKE '`+progress.PgcopydbAppNameLike+`' AND a.wait_event_type = 'Lock'
AND EXISTS (SELECT 1 FROM pg_stat_activity b WHERE b.application_name = 'e2e_progress_blocker'
AND b.pid = ANY(pg_blocking_pids(a.pid)))`)).NotTo(Equal("0"), "no pgcopydb session waits on the held lock")
			// d's worker opens its target transaction first, so one session stays idle in transaction.
			idle, active, _ := pooledSessions()
			g.Expect(idle).To(BeNumerically(">=", 2), "idle pgcopydb-named target sessions")
			g.Expect(active).To(BeZero(), "active pgcopydb-named target sessions")
		}, 2*time.Minute, time.Second).Should(Succeed())

		By("deleting the attempt-1 worker pod")
		deletePod(client.MatchingLabels{migrationLabel: name, batchv1.JobNameLabel: name + "-run-1"})
		// The Pooler closes a server connection whose client left mid-transaction.
		Eventually(func(g Gomega) {
			idle, _, other := pooledSessions()
			premise := idle >= 2 && other == 0
			g.Expect(premise).To(BeTrue(), "pooled-target premise: expected at least 2 idle and no other "+
				"pgcopydb-named server connections after the worker pod died, found %d idle and %d other", idle, other)
		}, 30*time.Second, time.Second).Should(Succeed())
		// From here the hold rests on the idle server connections alone.
		release()

		By("expecting the gate to hold the retry on the idle pooled sessions")
		// The probe comes about 330 s after the failure (log grace plus rate-limited requeues) and may
		// wait for a node until its deadline.
		Eventually(func(g Gomega) {
			cur := get(g)
			c := apimeta.FindStatusCondition(cur.Status.Conditions, v1beta1.ConditionRetryHeld)
			g.Expect(c).NotTo(BeNil(), "RetryHeld is not set")
			g.Expect(c.Status).To(Equal(metav1.ConditionTrue), "RetryHeld is %s/%s: %s", c.Status, c.Reason, c.Message)
			g.Expect(c.Reason).To(Equal("SessionsRemain"), c.Message)
			g.Expect(c.Message).To(ContainSubstring("pgcopydb["))
			g.Expect(c.Message).To(ContainSubstring("connection pooler"))
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
			found := false
			for _, e := range events.Items {
				if e.InvolvedObject.UID == cur.UID && e.Type == corev1.EventTypeWarning &&
					e.Reason == v1beta1.ConditionRetryHeld {
					found = true
					break
				}
			}
			g.Expect(found).To(BeTrue(), "no RetryHeld warning event on %s", name)
		}, 330*time.Second+gateProbeDeadline+retryGateInterval+time.Minute, 2*time.Second).Should(Succeed())
		Consistently(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, cur)).To(Succeed())
			g.Expect(cur.Status.Phase).NotTo(Equal(v1beta1.PhaseFailed), "migration failed: %s", failureMessage(cur))
			g.Expect(cur.Status.Attempts).To(Equal(int32(1)))
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-2"}, &batchv1.Job{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the attempt-2 Job exists or its lookup failed: %v", err)
		}, 45*time.Second, 3*time.Second).Should(Succeed())

		By("ending the idle pgcopydb-named sessions on the target")
		ended := psql(dedicated.GetName(), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "+
			"WHERE usename = '"+cnpgAppRole+"' AND application_name LIKE '"+progress.PgcopydbAppNameLike+"'")
		// HaveEach fails on an empty list, so ending nothing cannot pass.
		Expect(strings.Fields(ended)).To(HaveEach(Equal("t")), "every pgcopydb-named session must end")

		By("expecting the gate to pass and attempt 2 to start")
		// As in the syncrep pass: the held probe stays, and its successor may wait for a node until its deadline.
		Eventually(func(g Gomega) {
			cur := get(g)
			// First, so a timeout prints why the operator still holds.
			c := apimeta.FindStatusCondition(cur.Status.Conditions, v1beta1.ConditionRetryHeld)
			g.Expect(c).NotTo(BeNil(), "RetryHeld is not set")
			g.Expect(c.Status).To(Equal(metav1.ConditionFalse), "RetryHeld is %s/%s: %s", c.Status, c.Reason, c.Message)
			g.Expect(c.Reason).To(Equal("GatePassed"), c.Message)
			g.Expect(cur.Status.Attempts).To(Equal(int32(2)))
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-2"}, &batchv1.Job{})).
				To(Succeed())
			g.Expect(cur.Status.ExemptAttempts).To(BeZero())
			g.Expect(cur.Status.RetryGate).To(BeNil())
		}, retryGateInterval+gateProbeDeadline+retryGateInterval+2*time.Minute, 2*time.Second).Should(Succeed())
		// pgcopydb through a transaction pooler is unproven, so the spec ends at the gate.
	})
})

// gatePoolFixtureSQL creates the pooled-retry schema as owner. The rows pass the fast check
// body; the target restores the sleeping one, so each restored row costs 0.5 s.
func gatePoolFixtureSQL(schema, owner string) string {
	var sql strings.Builder
	fmt.Fprintf(&sql, `DROP SCHEMA IF EXISTS %[1]s CASCADE;
SET ROLE %[2]s;
CREATE SCHEMA %[1]s;
CREATE FUNCTION %[1]s.slow_check(bigint) RETURNS boolean LANGUAGE sql AS 'select true';
`, schema, owner)
	for _, table := range []string{"a", "b", "c"} {
		fmt.Fprintf(&sql, `CREATE TABLE %[1]s.%[2]s (id bigint PRIMARY KEY, pad text NOT NULL,
  CHECK (%[1]s.slow_check(id)));
INSERT INTO %[1]s.%[2]s SELECT g, repeat('x', 1024) FROM generate_series(1, 60) g;
`, schema, table)
	}
	// pgcopydb copies the largest tables first, so d, with one row, comes last.
	fmt.Fprintf(&sql, `CREATE TABLE %[1]s.d (id bigint PRIMARY KEY);
INSERT INTO %[1]s.d VALUES (1);
CREATE OR REPLACE FUNCTION %[1]s.slow_check(bigint) RETURNS boolean LANGUAGE plpgsql
  AS $$BEGIN PERFORM pg_sleep(0.5); RETURN true; END$$;`, schema)
	return sql.String()
}
