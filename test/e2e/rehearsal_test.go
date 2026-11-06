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
		Eventually(func(g Gomega) {
			cur := get(g)
			st = cur.Status.VerificationRehearsal
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

		By("expecting replication to go on after the rehearsal")
		psql(sourceCluster, asSourceAppRole()+
			fmt.Sprintf("INSERT INTO %s VALUES (%d, 'after the rehearsal')", table, rehearsalRows+1))
		Eventually(func() string {
			return psql(targetCluster, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = %d", table, rehearsalRows+1))
		}, 3*time.Minute, 2*time.Second).Should(Equal("1"), "a row written after the rehearsal did not reach the target")
		Eventually(func(g Gomega) {
			rep := get(g).Status.Replication
			g.Expect(rep).NotTo(BeNil())
			g.Expect(rep.LagBytes).NotTo(BeNil(), "the lag is no longer sampled")
		}, time.Minute, 2*time.Second).Should(Succeed())
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
		Expect(psql(targetCluster, "SELECT count(*) FROM "+table)).To(Equal(fmt.Sprint(rehearsalRows + 1)))
	})
})
