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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strconv"
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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

// syncRepStorageSize is the volume of each instance of the synchronous target.
const syncRepStorageSize = "1Gi"

const (
	// cnpgFencedInstances is the Cluster annotation CloudNativePG fences instances by.
	cnpgFencedInstances = "cnpg.io/fencedInstances"
	// retryGateAppName is the application_name of the retry gate probe's sessions.
	retryGateAppName = "pgm-retry-gate"
	// gateProbeDeadline and retryGateInterval copy the operator's unexported constants: a probe
	// that never gets a node holds the gate for their sum before its successor starts.
	gateProbeDeadline = 300 * time.Second
	retryGateInterval = 30 * time.Second
)

var _ = Describe("Synchronous replication on the target", Ordered, SpecPriority(3), func() {
	var target string

	BeforeAll(func() {
		// Fencing stops PostgreSQL inside a database pod.
		requireCNPGFixtures()
		// With dataDurability required the fenced standby stays in synchronous_standby_names,
		// so every commit that waits for it blocks.
		cluster := createDedicatedTarget("e2e-syncrep-", syncRepStorageSize, 2, map[string]any{
			"method": "any", "number": int64(1), "dataDurability": "required",
		})
		target = cluster.GetName()
		Eventually(func(g Gomega) {
			g.Expect(psql(target, "SHOW synchronous_standby_names")).NotTo(BeEmpty())
			g.Expect(syncStandbys(target)).To(Equal(1), "synchronous standbys streaming from the primary")
		}, 3*time.Minute, 2*time.Second).Should(Succeed())
		grantTargetFollowPrivilegesOn(target, cnpgAppDatabase)
	})

	It("completes a clone with synchronousCommit local while the only synchronous standby is fenced", func() {
		const name = "e2e-syncrep-clone"
		// LIFO: the standby is back before the Migration goes.
		DeferCleanup(func() { deleteMigration(name) })
		DeferCleanup(func() { unfence(target) })

		By("fencing the only synchronous standby")
		standby := standbyInstance(target)
		fence(target, standby)
		Eventually(func() int { return syncStandbys(target) }, 2*time.Minute, 2*time.Second).
			Should(BeZero(), "the fenced standby still streams synchronously")
		Expect(syncRepBlocks(target)).To(BeTrue(), "a commit on the target must wait for the fenced standby")

		By("cloning one table with synchronousCommit local")
		// No verification: the finish-path Jobs keep the server default and would wait.
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			DropIfExists: true,
			Filters:      &v1beta1.Filters{IncludeOnlyTables: []string{syncCommitTable}},
		})
		m.Spec.Target = e2eConn(target)
		m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
		create(m)
		patchBackoffLimit(name, 0)

		done := waitCompleted(name, nsE2E)
		expectSingleAttempt(done)
		Expect(done.Status.ExemptAttempts).To(BeZero())
		// Present and False: "not True" would also pass if no sample ever ran.
		c := apimeta.FindStatusCondition(done.Status.Conditions, v1beta1.ConditionCommitsBlocked)
		Expect(c).NotTo(BeNil(), "no progress sample set CommitsBlocked on %s", name)
		Expect(c.Status).To(Equal(metav1.ConditionFalse), "CommitsBlocked is %s/%s: %s", c.Status, c.Reason, c.Message)
		Expect(hasEvent(Default, done, corev1.EventTypeNormal, "AttemptStarted", "attempt 1 as Job")).
			To(BeTrue(), "the Event listing shows no AttemptStarted for %s", name)
		Expect(hasEvent(Default, done, corev1.EventTypeWarning, v1beta1.ConditionCommitsBlocked, "")).
			To(BeFalse(), "%s recorded a CommitsBlocked warning", name)

		By("comparing row counts")
		const count = "SELECT count(*) FROM " + syncCommitTable
		source := psql(sourceCluster, count)
		Expect(source).NotTo(Equal("0"), "the source fixture has no customers to compare")
		Expect(psql(target, count)).To(Equal(source))

		By("checking that the standby stayed fenced for the whole copy")
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(cnpgGVK)
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: target}, current)).To(Succeed())
		var fenced []string
		Expect(json.Unmarshal([]byte(current.GetAnnotations()[cnpgFencedInstances]), &fenced)).
			To(Succeed(), "the fencing annotation is gone or unreadable")
		Expect(fenced).To(Equal([]string{standby}))
		Expect(syncStandbys(target)).To(BeZero(), "a synchronous standby streamed during the copy")
	})

	It("reports blocked commits, holds the retry while the standby is fenced and exempts the cut attempt", func() {
		const name = "e2e-syncrep-follow"
		schema := fmt.Sprintf("e2e_syncrep_%d", GinkgoParallelProcess())
		table := schema + ".t"
		key := client.ObjectKey{Namespace: nsE2E, Name: name}
		get := func(g Gomega) *v1beta1.Migration {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, cur)).To(Succeed())
			if cur.Status.Phase == v1beta1.PhaseFailed {
				StopTrying("migration " + name + " failed: " + failureMessage(cur)).Now()
			}
			return cur
		}
		slot := "SELECT count(*) FROM pg_replication_slots WHERE slot_name = " +
			sqlLiteral(pgcopydb.SlotName(nsE2E, name))

		By("seeding a 100-row table in its own schema on the source")
		DeferCleanup(func() { psql(sourceCluster, "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
		psql(sourceCluster, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		psql(sourceCluster, asSourceAppRole()+"CREATE SCHEMA "+schema+"; CREATE TABLE "+table+
			" (id int PRIMARY KEY, v text); INSERT INTO "+table+" SELECT g, 'seed' FROM generate_series(1, 100) g")

		By("following the schema into the synchronous target with backoffLimit 0")
		// Registered after the schema drop, so the worker and the slot go before the schema does.
		DeferCleanup(func() { deleteMigration(name) })
		DeferCleanup(func() { unfence(target) })
		m := newFollowMigration(name, v1beta1.CutoverManual)
		m.Spec.Target = e2eConn(target)
		m.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlySchemas: []string{schema}}
		create(m)
		patchBackoffLimit(name, 0)
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		// They exist now, so their absence after the cutover means something.
		Expect(psql(sourceCluster, slot)).To(Equal("1"), "the Migration's slot is not on the source")
		Expect(targetOriginCountOn(target)).To(Equal("1"), "the Migration's origin is not on the target")
		Eventually(func(g Gomega) {
			expectConditionReason(g, get(g), v1beta1.ConditionCommitsBlocked, metav1.ConditionFalse, "NoSyncRepWait")
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("fencing the standby and writing one row on the source")
		fenceTime := time.Now()
		fence(target, standbyInstance(target))
		Eventually(func() int { return syncStandbys(target) }, 2*time.Minute, 2*time.Second).
			Should(BeZero(), "the fenced standby still streams synchronously")
		psql(sourceCluster, asSourceAppRole()+"INSERT INTO "+table+" VALUES (101, 'fenced')")

		By("waiting for the apply's commit to wait for synchronous replication")
		Eventually(func() int { return syncRepWaiters(target, progress.PgcopydbAppNameLike) }, time.Minute, time.Second).
			Should(BeNumerically(">=", 1), "no pgcopydb session waits for synchronous replication")

		By("expecting CommitsBlocked and its warning event")
		Eventually(func(g Gomega) {
			cur := get(g)
			c := expectConditionReason(g, cur, v1beta1.ConditionCommitsBlocked, metav1.ConditionTrue, "SyncRepWait")
			// Condition times have whole seconds.
			g.Expect(c.LastTransitionTime.Time).NotTo(BeTemporally("<", fenceTime.Truncate(time.Second)))
			g.Expect(c.Message).To(ContainSubstring("pgcopydb"))
			g.Expect(hasEvent(g, cur, corev1.EventTypeWarning, v1beta1.ConditionCommitsBlocked, "")).
				To(BeTrue(), "no CommitsBlocked warning event on %s", name)
		}, 3*time.Minute, 2*time.Second).Should(Succeed())

		By("deleting the attempt-1 worker pod while its commit waits")
		deletePod(client.MatchingLabels{migrationLabel: name, batchv1.JobNameLabel: name + "-run-1"})

		By("expecting the gate to hold the retry on the waiting sessions and exempt attempt 1")
		// The probe comes about 330 s after the failure (log grace plus rate-limited requeues) and may wait
		// for a node until its deadline; the budget stays under the 15 min verdictDeadline, past which
		// attempt 1 counts and backoffLimit 0 fails the Migration.
		Eventually(func(g Gomega) {
			cur := get(g)
			c := expectConditionReason(g, cur, v1beta1.ConditionRetryHeld, metav1.ConditionTrue, "SessionsRemain")
			g.Expect(c.Message).To(ContainSubstring("wait=SyncRep"))
			g.Expect(c.Message).To(ContainSubstring("pgcopydb["))
			g.Expect(cur.Status.RetryGate).NotTo(BeNil(), "retryGate is not set")
			g.Expect(cur.Status.RetryGate.FailedAttempt).To(Equal(int32(1)))
			g.Expect(cur.Status.RetryGate.Exemption).To(Equal(v1beta1.RetryGateExempt))
			g.Expect(cur.Status.ExemptAttempts).To(Equal(int32(1)))
			g.Expect(hasEvent(g, cur, corev1.EventTypeWarning, v1beta1.ConditionRetryHeld, "")).
				To(BeTrue(), "no RetryHeld warning event on %s", name)
		}, 330*time.Second+gateProbeDeadline+retryGateInterval+time.Minute, 2*time.Second).Should(Succeed())

		By("holding with backoffLimit 0 while the standby stays fenced")
		// A Counted verdict would have failed the Migration at the census.
		gateSeen := false
		Consistently(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, cur)).To(Succeed())
			g.Expect(cur.Status.Phase).NotTo(Equal(v1beta1.PhaseFailed), "migration failed: %s", failureMessage(cur))
			g.Expect(cur.Status.Attempts).To(Equal(int32(1)))
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(ctx, jobs, client.InNamespace(nsE2E),
				client.MatchingLabels{migrationLabel: name})).To(Succeed())
			names := make([]string, 0, len(jobs.Items))
			for i := range jobs.Items {
				names = append(names, jobs.Items[i].Name)
			}
			g.Expect(names).To(ContainElement(name+"-run-1"), "the Job listing shows none of %s's Jobs", name)
			g.Expect(names).NotTo(ContainElement(name + "-run-2"))
			for _, job := range names {
				if strings.HasPrefix(job, name+"-gate-") {
					g.Expect(job).To(Equal(name+"-gate-2"), "a probe for another attempt")
					gateSeen = true
				}
			}
			g.Expect(syncRepWaiters(target, retryGateAppName)).
				To(BeZero(), "a probe's test commit waits for synchronous replication")
		}, 90*time.Second, 5*time.Second).Should(Succeed())
		Expect(gateSeen).To(BeTrue(), "no %s-gate-2 probe Job appeared during the hold", name)

		By("unfencing the standby")
		unfence(target)
		Eventually(func() int { return syncRepWaiters(target, progress.PgcopydbAppNameLike) }, 2*time.Minute, time.Second).
			Should(BeZero(), "pgcopydb sessions still wait for synchronous replication")

		By("expecting the gate to pass and attempt 2 to start on the unspent budget")
		// A probe that held before the unfence stays retryGateInterval. Its successor may wait for
		// a node until gateProbeDeadline and stay retryGateInterval too, before the next one passes.
		Eventually(func(g Gomega) {
			cur := get(g)
			// First, so a timeout prints why the operator still holds.
			expectConditionReason(g, cur, v1beta1.ConditionRetryHeld, metav1.ConditionFalse, "GatePassed")
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-2"}, &batchv1.Job{})).
				To(Succeed())
			g.Expect(cur.Status.Attempts).To(Equal(int32(2)))
			g.Expect(cur.Status.ExemptAttempts).To(Equal(int32(1)))
			g.Expect(cur.Status.RetryGate).To(BeNil())
		}, retryGateInterval+gateProbeDeadline+retryGateInterval+2*time.Minute, 2*time.Second).Should(Succeed())
		// The hold proved the 0 budget. Issue #88 fails a resumed follow attempt about one run in three;
		// a failed attempt 2 needs a new probe and census before any verdict, so this patch is in time.
		patchBackoffLimit(name, 5)
		Eventually(func(g Gomega) {
			g.Expect(hasEvent(g, get(g), corev1.EventTypeNormal, "AttemptStarted", "did not count against backoffLimit")).
				To(BeTrue(), "no AttemptStarted event says attempt 1 did not count")
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("waiting for the passed probe to go")
		Eventually(func() bool {
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-gate-2"}, &batchv1.Job{})
			return apierrors.IsNotFound(err)
		}, 2*time.Minute, 2*time.Second).Should(BeTrue(), "the probe Job %s-gate-2 is still there", name)

		By("waiting for the attempt-2 worker pod to run")
		// The marker's lagConvergeTimeout then pays for streaming only, not for scheduling.
		waitWorkerRunning(name, 2)

		By("streaming a marker row with the standby back")
		psql(sourceCluster, asSourceAppRole()+"INSERT INTO "+table+" VALUES (102, 'marker')")
		Eventually(func(g Gomega) {
			get(g)
			g.Expect(psql(target, "SELECT count(*) FROM "+table+" WHERE id = 102")).
				To(Equal("1"), "the marker row has not reached the target")
		}, lagConvergeTimeout, 2*time.Second).Should(Succeed())
		Eventually(func(g Gomega) {
			expectConditionReason(g, get(g), v1beta1.ConditionCommitsBlocked, metav1.ConditionFalse, "NoSyncRepWait")
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("cutting over")
		approveCutover(name)
		done := waitCompleted(name, nsE2E)
		Expect(done.Status.ExemptAttempts).To(Equal(int32(1)))
		Expect(done.Status.Attempts).To(BeNumerically(">=", 2))
		count := "SELECT count(*) FROM " + table
		Expect(psql(sourceCluster, count)).To(Equal("102"))
		Expect(psql(target, count)).To(Equal("102"))
		expectCleanupSucceeded(name)
		Expect(targetOriginCountOn(target)).To(Equal("0"), "the Migration's origin is left on the target")
		Expect(psql(sourceCluster, slot)).To(Equal("0"), "the Migration's slot is left on the source")
	})
})

// standbyInstance is the instance of cluster that is not status.currentPrimary.
func standbyInstance(cluster string) string {
	GinkgoHelper()
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(cnpgGVK)
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: cluster}, current)).To(Succeed())
	primary, _, err := unstructured.NestedString(current.Object, "status", "currentPrimary")
	Expect(err).NotTo(HaveOccurred())
	Expect(primary).NotTo(BeEmpty(), "%s reports no current primary", cluster)
	pods := instancePods(cluster)
	standbys := make([]string, 0, len(pods))
	for i := range pods {
		if pods[i].Name != primary {
			standbys = append(standbys, pods[i].Name)
		}
	}
	found := len(standbys)
	Expect(found).To(Equal(1), "%s has %d standby instances, want 1", cluster, found)
	return standbys[0]
}

// fence has CloudNativePG stop PostgreSQL on instance; its pod keeps running.
func fence(cluster, instance string) {
	GinkgoHelper()
	list, err := json.Marshal([]string{instance})
	Expect(err).NotTo(HaveOccurred())
	setFencedInstances(cluster, string(list))
}

// unfence lifts every fence on cluster and waits for its synchronous standby to stream again.
func unfence(cluster string) {
	GinkgoHelper()
	setFencedInstances(cluster, nil)
	Eventually(func() int { return syncStandbys(cluster) }, 3*time.Minute, 2*time.Second).
		Should(Equal(1), "no synchronous standby streams on %s after the unfence", cluster)
}

// setFencedInstances merge-patches cluster's fencing annotation; nil removes it.
func setFencedInstances(cluster string, list any) {
	GinkgoHelper()
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{cnpgFencedInstances: list}},
	})
	Expect(err).NotTo(HaveOccurred())
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(cnpgGVK)
	obj.SetNamespace(nsE2E)
	obj.SetName(cluster)
	Expect(k8sClient.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch))).To(Succeed())
}

// syncStandbys counts the standbys cluster's primary streams to synchronously.
func syncStandbys(cluster string) int {
	GinkgoHelper()
	return psqlInt(cluster, "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync', 'quorum')")
}

// syncRepWaiters counts the app role's sessions on cluster that wait for synchronous
// replication and whose application_name is LIKE pattern.
func syncRepWaiters(cluster, pattern string) int {
	GinkgoHelper()
	return psqlInt(cluster, "SELECT count(*) FROM pg_stat_activity WHERE usename = "+sqlLiteral(cnpgAppRole)+
		" AND application_name LIKE "+sqlLiteral(pattern)+" AND wait_event = 'SyncRep'")
}

func psqlInt(cluster, sql string) int {
	GinkgoHelper()
	out := psql(cluster, sql)
	n, err := strconv.Atoi(out)
	Expect(err).NotTo(HaveOccurred(), "%q returned %q, not a number", sql, out)
	return n
}

// syncRepBlocks reports whether a commit on cluster's primary waits for synchronous replication.
// The server warns when psql cancels that wait, and the commit stays committed locally.
func syncRepBlocks(cluster string) bool {
	GinkgoHelper()
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	argv := append(kubectlExecArgs(primaryPod(cluster), false), "timeout", "-s", "INT", "5")
	argv = append(argv, psqlCommand(cluster, appDatabase(cluster), "-XAtq", "--single-transaction",
		"-c", "SET LOCAL synchronous_commit = on", "-c", "CREATE TEMP TABLE e2e_syncrep_probe () ON COMMIT DROP")...)
	var stderr bytes.Buffer
	cmd := exec.CommandContext(probeCtx, "kubectl", argv...)
	cmd.Stderr = &stderr
	err := cmd.Run()
	if strings.Contains(stderr.String(), "canceling wait for synchronous replication") {
		return true
	}
	Expect(err).NotTo(HaveOccurred(), "the commit probe on %s failed: %s", cluster, stderr.String())
	return false
}

// patchBackoffLimit writes the limit through a raw merge patch: the typed client omits a 0,
// which the CRD default then turns into 3.
func patchBackoffLimit(name string, limit int32) {
	GinkgoHelper()
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: name}}
	Expect(k8sClient.Patch(ctx, m, client.RawPatch(types.MergePatchType,
		fmt.Appendf(nil, `{"spec":{"backoffLimit":%d}}`, limit)))).To(Succeed())
	Expect(m.Spec.BackoffLimit).To(Equal(limit))
}

// expectConditionReason asserts that m carries condType with status and reason, and returns it.
func expectConditionReason(g Gomega, m *v1beta1.Migration, condType string,
	status metav1.ConditionStatus, reason string) *metav1.Condition {
	c := apimeta.FindStatusCondition(m.Status.Conditions, condType)
	g.Expect(c).NotTo(BeNil(), "%s is not set on %s", condType, m.Name)
	g.Expect(c.Status).To(Equal(status), "%s is %s/%s: %s", condType, c.Status, c.Reason, c.Message)
	g.Expect(c.Reason).To(Equal(reason), "%s: %s", condType, c.Message)
	return c
}

// hasEvent reports whether m has an Event of eventType and reason whose message contains text.
func hasEvent(g Gomega, m *v1beta1.Migration, eventType, reason, text string) bool {
	events := &corev1.EventList{}
	g.Expect(k8sClient.List(ctx, events, client.InNamespace(m.Namespace))).To(Succeed())
	return slices.ContainsFunc(events.Items, func(e corev1.Event) bool {
		return e.InvolvedObject.UID == m.UID && e.Type == eventType && e.Reason == reason &&
			strings.Contains(e.Message, text)
	})
}
