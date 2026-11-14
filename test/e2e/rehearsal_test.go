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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// rehearsalRows sizes the rehearsed table, so the compare scans more than a page.
const rehearsalRows = 20000

// verifyLogLines keeps the whole verify Job log: the drain path is decided near
// its start, and the compare output follows.
const verifyLogLines = 100000

// The rehearsal's filtered selection is one stamped schema; outside it, the source
// holds the pair's seed tables, so a compare that lost the filters would not complete.
var _ = Describe("Verification rehearsal", SpecPriority(2), func() {
	BeforeEach(resetMigrationPair)

	It("times compare data while a filtered follow migration streams, and the cutover still verifies", func() {
		identity := fmt.Sprint(time.Now().UnixNano())
		name, schema := "e2e-rehearsal-"+identity, "e2e_rehearsal_"+identity
		table := sqlIdent(schema) + ".items"
		key := client.ObjectKey{Namespace: nsE2E, Name: name}
		m := newFollowMigration(name, v1beta1.CutoverManual)
		m.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlySchemas: []string{schema}}
		m.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
		createStampedSchemas(m, identity, "pgcopydb-e2e-rehearsal:"+identity, []string{schema}, nil)
		get := func(g Gomega) *v1beta1.Migration {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, cur)).To(Succeed())
			if cur.Status.Phase == v1beta1.PhaseFailed {
				StopTrying("migration " + name + " failed: " + failureMessage(cur)).Now()
			}
			return cur
		}

		By("seeding the schema and following it to CutoverPending")
		psqlBulk(sourceCluster, asSourceAppRole()+fmt.Sprintf(
			"CREATE TABLE %[1]s (id integer PRIMARY KEY, payload text NOT NULL);\n"+
				"INSERT INTO %[1]s SELECT g, md5(g::text) FROM generate_series(1, %[2]d) g;\n",
			table, rehearsalRows))
		create(m)
		waitFollowStreaming(name)
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		worker := waitWorkerRunning(name, 1)

		By("turning the rehearsal on and waiting for it to complete")
		cur := get(Default)
		patch := client.MergeFrom(cur.DeepCopy())
		cur.Spec.Verification.Rehearsal = true
		Expect(k8sClient.Patch(ctx, cur, patch)).To(Succeed())
		// deleteFixtureMigration refuses a Migration whose verification differs from m's.
		m.Spec.Verification = cur.Spec.Verification.DeepCopy()
		var st *v1beta1.VerificationRehearsalStatus
		var rehearsed *v1beta1.Migration
		Eventually(func(g Gomega) {
			rehearsed = get(g)
			st = rehearsed.Status.VerificationRehearsal
			g.Expect(st).NotTo(BeNil(), "no rehearsal has started")
			if st.Outcome == v1beta1.RehearsalFailed || st.Outcome == v1beta1.RehearsalCanceled {
				StopTrying(fmt.Sprintf("the rehearsal ended %s: %s\nlog:\n%s", st.Outcome, st.Message,
					jobLogs(st.JobName, 40))).Now()
			}
			g.Expect(st.Outcome).To(Equal(v1beta1.RehearsalCompleted), "rehearsal %s: %s", st.Outcome, st.Message)
		}, migrationTimeout, time.Second).Should(Succeed())
		Expect(st.Run).To(Equal(int32(1)))
		Expect(st.JobName).To(Equal(name + "-rehearsal-1"))
		Expect(st.StartedAt).NotTo(BeNil())
		Expect(st.CompletedAt).NotTo(BeNil())
		Expect(st.CompletedAt.Sub(st.StartedAt.Time)).To(BeNumerically(">", 0), "a rehearsal took no time")
		Expect(jobLogs(st.JobName, 40)).To(ContainSubstring("rehearsal: compare data wrote its report"))
		// The pass that records the outcome reads the finished Job's log, so the final sample is already there.
		expectFinalProgress(rehearsed, v1beta1.VerificationProgressRehearsal, st.JobName)

		By("expecting the same worker pod, no compare or verify Job, and no verdict")
		cur = get(Default)
		Expect(cur.Status.Attempts).To(Equal(int32(1)))
		Expect(cur.Status.Phase).To(Equal(v1beta1.PhaseCutoverPending))
		Expect(waitWorkerRunning(name, 1)).To(Equal(worker), "the worker pod was replaced during the rehearsal")
		Expect(apimeta.FindStatusCondition(cur.Status.Conditions, v1beta1.ConditionVerified)).To(BeNil())
		Expect(cur.Status.Verification).To(BeEmpty())
		owned := client.MatchingLabels{migrationLabel: name}
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(nsE2E), owned)).To(Succeed())
		for _, j := range jobs.Items {
			Expect(j.Name).NotTo(SatisfyAny(HavePrefix(name+"-compare-"), Equal(name+"-verify")),
				"the rehearsal created Job %s", j.Name)
		}

		By("expecting replication, and its sampling, to go on after the rehearsal")
		before := cur.Status.Replication
		Expect(before).NotTo(BeNil(), "no replication sample before the insert")
		psql(sourceCluster, asSourceAppRole()+
			fmt.Sprintf("INSERT INTO %s VALUES (%d, 'after the rehearsal')", table, rehearsalRows+1))
		Eventually(func() string {
			return psql(targetCluster, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = %d", table, rehearsalRows+1))
		}, 3*time.Minute, 2*time.Second).Should(Equal("1"), "a row written after the rehearsal did not reach the target")
		// The insert moves the stream's positions, so positions that stand still mean no new sample.
		Eventually(func(g Gomega) {
			rep := get(g).Status.Replication
			g.Expect(rep).NotTo(BeNil())
			g.Expect(rep.LagBytes).NotTo(BeNil(), "the lag is no longer sampled")
			g.Expect([]string{rep.WriteLSN, rep.ReplayLSN}).NotTo(Equal([]string{before.WriteLSN, before.ReplayLSN}),
				"the replication sample has not moved since the insert")
		}, 3*time.Minute, 2*time.Second).Should(Succeed())
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)

		By("cutting over: the rehearsal pod has ended before the verify Job starts")
		approveCutover(name)
		verify := &batchv1.Job{}
		Eventually(func(g Gomega) {
			get(g)
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-verify"}, verify)).To(Succeed())
			g.Expect(verify.Status.StartTime).NotTo(BeNil(), "the verify Job has not started")
		}, followTimeout, time.Second).Should(Succeed())
		Expect(st.CompletedAt.Time).NotTo(BeTemporally(">", verify.Status.StartTime.Time))
		pods := &corev1.PodList{}
		Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E), owned)).To(Succeed())
		for _, p := range pods.Items {
			if strings.HasPrefix(p.Labels[batchv1.JobNameLabel], name+"-rehearsal-") {
				Expect(p.Status.Phase).To(BeElementOf(corev1.PodSucceeded, corev1.PodFailed),
					"rehearsal pod %s is %s while the verify Job runs", p.Name, p.Status.Phase)
			}
		}

		By("expecting the drain verify and the data verification to pass")
		done := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectSingleAttempt(done)
		expectConditionTrue(done, v1beta1.ConditionCutoverComplete)
		expectConditionTrue(done, v1beta1.ConditionVerified)
		expectCleanupSucceeded(name)
		Expect(done.Status.Verification).To(ConsistOf(SatisfyAll(
			HaveField("Check", compareDataCheck), HaveField("Passed", true),
			HaveField("StartedAt", Not(BeNil())), HaveField("CompletedAt", Not(BeNil())),
		)))
		Expect(done.Status.VerificationRehearsal.Outcome).To(Equal(v1beta1.RehearsalCompleted))

		By("expecting a progress item for each compare: the rehearsal's kept, the drain's, and the data check's")
		expectFinalProgress(done, v1beta1.VerificationProgressRehearsal, st.JobName)
		expectDrainProgress(done)
		expectFinalProgress(done, v1beta1.VerificationProgressData, name+"-compare-"+compareDataCheck)
		Expect(psql(targetCluster, "SELECT count(*) FROM "+table)).To(Equal(fmt.Sprint(rehearsalRows + 1)))
	})
})

// verificationProgress returns the item of kind in status.verificationProgress, or nil.
func verificationProgress(m *v1beta1.Migration, kind string) *v1beta1.VerificationProgress {
	for i := range m.Status.VerificationProgress {
		if m.Status.VerificationProgress[i].Kind == kind {
			return &m.Status.VerificationProgress[i]
		}
	}
	return nil
}

// expectFinalProgress asserts that job left the final sample of kind, with every planned table compared.
// A native Migration records it from the checked compare result, so that result must be there too.
func expectFinalProgress(m *v1beta1.Migration, kind, job string) {
	GinkgoHelper()
	if m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate {
		Expect(nativeFinalProgressError(m, kind)).To(Succeed())
	}
	p := verificationProgress(m, kind)
	Expect(p).NotTo(BeNil(), "%s has no %s item in status.verificationProgress", m.Name, kind)
	Expect(p.JobName).To(Equal(job), "the %s item of %s describes another Job", kind, m.Name)
	Expect(p.Final).To(BeTrue(), "the %s item of %s holds no final sample of Job %s", kind, m.Name, job)
	Expect(p.PlannedTables).To(BeNumerically(">", 0), "the %s run of %s planned no table", kind, m.Name)
	Expect(p.ComparedTables).To(Equal(p.PlannedTables), "the %s run of %s left tables uncompared", kind, m.Name)
	Expect(p.InFlight).To(BeEmpty(), "the final %s sample of %s still lists tables in flight", kind, m.Name)
}

// expectDrainProgress holds the drain item to the verify Job's log: a drain decided by
// content leaves a final sample, and the exact-LSN fast path, which compares nothing, none.
// A native drain has no verify Job; its proof is the checkpoint row the target retains.
func expectDrainProgress(m *v1beta1.Migration) {
	GinkgoHelper()
	if m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate {
		expectNativeDrainProof(m)
		return
	}
	job := m.Name + "-verify"
	log := jobLogs(job, verifyLogLines)
	switch {
	case strings.Contains(log, "Deciding by content"):
		expectFinalProgress(m, v1beta1.VerificationProgressDrain, job)
	case strings.Contains(log, "drain verified: origin progress"):
		Expect(verificationProgress(m, v1beta1.VerificationProgressDrain)).To(BeNil(),
			"the exact-LSN drain of %s compared nothing, yet left a drain item", m.Name)
	default:
		Fail("the log of " + job + " shows neither drain path:\n" + logTail(log, 40))
	}
}
