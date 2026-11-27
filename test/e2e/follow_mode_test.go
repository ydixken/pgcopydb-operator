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
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// legacyRunnerImage carries pgcopydb 9d8b660, which has no --follow-mode, whatever
// E2E_RUNNER_TAG says, so these specs prove sqlite needs nothing from a newer runner.
const legacyRunnerImage = "ghcr.io/ydixken/pgcopydb-operator/runner:v0.28.0"

const (
	// performanceMark differs from walWorkersMark so a row an earlier spec left behind
	// can never be counted for this one.
	performanceMark = "follow-performance"
	// followModePreflightTail covers a whole preflight log, which is one line per check.
	followModePreflightTail = 200
)

var _ = Describe("Follow mode", func() {
	BeforeEach(resetMigrationPair)

	It("follows and cuts over in performance mode at one WAL worker", performanceCutover)

	It("follows and cuts over in sqlite mode with the v0.28.0 runner pinned", legacyRunnerSQLiteCutover)

	It("fails the preflight of performance mode with the v0.28.0 runner pinned, naming the image and --follow-mode",
		legacyRunnerPerformanceRefused)
})

func performanceCutover() {
	const name = "e2e-follow-performance"
	// Manual first, so the writer stops before the freeze; the spec then arms Automatic.
	mig := newFollowMigration(name, v1beta1.CutoverManual)
	mig.Spec.Follow.Mode = v1beta1.FollowModePerformance
	mig.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
	slot := pgcopydb.EffectiveSlotName(&mig.Spec, nsE2E, name)
	DeferCleanup(func() { deleteMigration(name) })

	By("writing parent, child and a third table in each source transaction, starting before the base copy")
	w := startLiveWriterOf(performanceMark, walWorkersStatement)
	DeferCleanup(func() {
		_, _ = w.stop()
		if CurrentSpecReport().Failed() {
			AddReportEntry("live writer after stop", w.diagnostics().String(), ReportEntryVisibilityFailureOrVerbose)
		}
		like := sqlLiteral(performanceMark + "-%")
		psql(sourceCluster, "DELETE FROM orders WHERE note LIKE "+like+"; DELETE FROM customers WHERE name LIKE "+
			like+"; DELETE FROM audit.events WHERE payload->>'note' LIKE "+like)
	})
	create(mig)

	By("passing the preflight, or skipping when the runner pgcopydb lacks " + pgcopydb.FollowModeFlag)
	m := &v1beta1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m)).To(Succeed())
		g.Expect(m.Status.Phase == v1beta1.PhaseFailed || m.Status.Attempts >= 1).To(BeTrue(),
			"%s is at phase %q with no worker attempt", name, m.Status.Phase)
	}, workerRunningTimeout, time.Second).Should(Succeed())
	if m.Status.Phase == v1beta1.PhaseFailed {
		msg := failureMessage(m)
		if strings.Contains(msg, "does not list "+pgcopydb.FollowModeFlag) {
			// A release run whose pin carries follow mode must never land here.
			skipped := "FOLLOW MODE NOT TESTED: the pgcopydb in runner " + runnerTag + " lacks " + pgcopydb.FollowModeFlag
			AddReportEntry("follow mode skipped", skipped)
			_, _ = fmt.Fprintln(GinkgoWriter, skipped)
			Skip(skipped)
		}
		Fail("migration " + name + " failed before its first attempt: " + msg)
	}

	By("carrying the mode into the worker Job and through the preflight")
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, job)).To(Succeed())
	env := job.Spec.Template.Spec.Containers[0].Env
	Expect(env).To(ContainElement(corev1.EnvVar{
		Name: "PGCOPYDB_FOLLOW_MODE", Value: v1beta1.FollowModePerformance,
	}))
	Expect(env).NotTo(ContainElement(HaveField("Name", "PGCOPYDB_WAL_WORKERS")),
		"a one-worker Job must not set a WAL worker count")
	// Only a performance Migration gets the follow-mode block in its preflight script, so a
	// sqlite run cannot print this line.
	preflight := jobLogs(name+"-preflight", followModePreflightTail)
	Expect(preflight).To(ContainSubstring("ok: runner pgcopydb accepts "+pgcopydb.FollowModeFlag),
		"preflight Job log:\n%s", preflight)

	By("streaming over exactly one slot, with one publication and none per table")
	waitFollowStreaming(name)
	Expect(sourceSlotCount()).To(Equal("1"), "pgcopydb slots on the source")
	pubs := strings.Split(psql(sourceCluster, walWorkerPublicationsSQL(slot, walWorkersTables)), "|")
	Expect(pubs).To(HaveLen(4))
	Expect(pubs[0]).To(Equal("1"), "no publication named %s on the source", slot)
	Expect(pubs[1]).To(Equal("0"), "performance mode created a publication named %s_t<oid>", slot)

	By("stopping the writer and arming Automatic cutover")
	last, err := w.stop()
	Expect(publicationRetryProbeError(err)).NotTo(HaveOccurred(),
		"the live writer failed; see the live writer diagnostic report for final marker availability")
	Expect(last).To(BeNumerically(">", 100),
		"the writer committed only %d transactions, too few to exercise performance mode under load", last)
	rows := walWorkersRowsSQL(performanceMark)
	srcRows := psql(sourceCluster, rows)
	Expect(srcRows).To(Equal(fmt.Sprintf("%d|%d|%d", last, last, last)), "customers|orders|audit.events on the source")
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m)).To(Succeed())
	patch := client.MergeFrom(m.DeepCopy())
	m.Spec.Cutover.Mode = v1beta1.CutoverAutomatic
	Expect(k8sClient.Patch(ctx, m, patch)).To(Succeed())

	By("completing with the drain verified and a clean data compare")
	logs := map[string]jobPodLog{}
	m = waitCompletedCapturing(mig, followTimeout, logs, name+"-verify")
	expectSingleAttempt(m)
	expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
	expectConditionTrue(m, v1beta1.ConditionVerified)
	expectCleanupSucceeded(name)
	expectFinalProgress(m, v1beta1.VerificationProgressData, name+"-compare-"+compareDataCheck)
	verifyLog, err := succeededJobLog(logs, name+"-verify")
	Expect(err).NotTo(HaveOccurred())
	Expect(verifyLog).To(ContainSubstring("drain verified"), "verify Job log:\n%s", logTail(verifyLog, 40))
	expectDrainProgress(m, verifyLog)

	By("finding every written row and every sequence on the target")
	Expect(psql(targetCluster, rows)).To(Equal(srcRows), "customers|orders|audit.events on the target")
	Expect(sequenceValues(targetCluster)).To(Equal(sequenceValues(sourceCluster)))

	By("leaving no slot, publication or origin on either side")
	for _, cluster := range []string{sourceCluster, targetCluster} {
		Eventually(func() string { return psql(cluster, walWorkerLeftoversSQL(slot)) }, 3*time.Minute, 2*time.Second).
			Should(Equal("0|0|0"), "slots|publications|origins named %s, %s_w<k> or %s_t<oid> on %s",
				slot, slot, slot, cluster)
	}
}

func legacyRunnerSQLiteCutover() {
	const name = "e2e-legacy-runner-sqlite"
	const marker = "legacy-sqlite"
	// Manual first, so the writer stops before the freeze; the spec then arms Automatic.
	mig := newFollowMigration(name, v1beta1.CutoverManual)
	mig.Spec.Follow.Mode = v1beta1.FollowModeSQLite
	mig.Spec.Runner.Image = legacyRunnerImage
	mig.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
	DeferCleanup(func() { deleteMigration(name) })

	By("writing to the source continuously, starting before the base copy")
	w := startLiveWriter(marker)
	DeferCleanup(func() {
		_, _ = w.stop()
		if CurrentSpecReport().Failed() {
			AddReportEntry("live writer after stop", w.diagnostics().String(), ReportEntryVisibilityFailureOrVerbose)
		}
		like := sqlLiteral(marker + "-%")
		psql(sourceCluster, "DELETE FROM orders WHERE note LIKE "+like)
		psql(targetCluster, "DELETE FROM orders WHERE note LIKE "+like)
	})
	create(mig)

	By("streaming from the pinned runner, with no follow mode in the worker Job")
	waitFollowStreaming(name)
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, job)).To(Succeed())
	c := job.Spec.Template.Spec.Containers[0]
	Expect(c.Image).To(Equal(legacyRunnerImage))
	// Every runner Job sets PGCOPYDB_LOG_JSON, so the absence check below reads a real worker env.
	Expect(c.Env).To(ContainElement(HaveField("Name", "PGCOPYDB_LOG_JSON")))
	Expect(c.Env).NotTo(ContainElement(HaveField("Name", "PGCOPYDB_FOLLOW_MODE")),
		"a sqlite worker Job must not set the follow mode")

	By("stopping the writer and arming Automatic cutover")
	last, err := w.stop()
	Expect(publicationRetryProbeError(err)).NotTo(HaveOccurred(),
		"the live writer failed; see the live writer diagnostic report for final marker availability")
	Expect(last).To(BeNumerically(">", 100),
		"the writer committed only %d rows, too few to exercise a migration under load", last)
	m := &v1beta1.Migration{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m)).To(Succeed())
	patch := client.MergeFrom(m.DeepCopy())
	m.Spec.Cutover.Mode = v1beta1.CutoverAutomatic
	Expect(k8sClient.Patch(ctx, m, patch)).To(Succeed())

	By("completing with the drain verified and the cleanup done")
	logs := map[string]jobPodLog{}
	m = waitCompletedCapturing(mig, followTimeout, logs, name+"-verify")
	expectSingleAttempt(m)
	expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
	expectConditionTrue(m, v1beta1.ConditionVerified)
	expectCleanupSucceeded(name)
	verifyLog, err := succeededJobLog(logs, name+"-verify")
	Expect(err).NotTo(HaveOccurred())
	Expect(verifyLog).To(ContainSubstring("drain verified"), "verify Job log:\n%s", logTail(verifyLog, 40))
	expectDrainProgress(m, verifyLog)
	expectFinalProgress(m, v1beta1.VerificationProgressData, name+"-compare-"+compareDataCheck)

	By("finding every written row and every sequence on the target, and no slot or origin left")
	Expect(psql(sourceCluster, liveMarkerQuery(marker))).To(Equal(fmt.Sprint(last)), "last marker on the source")
	rows := "SELECT count(*) FROM orders WHERE note LIKE " + sqlLiteral(marker+"-%")
	srcRows := psql(sourceCluster, rows)
	Expect(psql(targetCluster, rows)).To(Equal(srcRows),
		func() string { return writeLoadDiagnosis(marker, last, srcRows) })
	Expect(sequenceValues(targetCluster)).To(Equal(sequenceValues(sourceCluster)))
	Eventually(sourceSlotCount, 3*time.Minute, 2*time.Second).Should(Equal("0"), "replication slot left on the source")
	Eventually(targetOriginCount, 3*time.Minute, 2*time.Second).Should(Equal("0"), "replication origin left on the target")
}

func legacyRunnerPerformanceRefused() {
	const name = "e2e-legacy-runner-performance"
	mig := newFollowMigration(name, v1beta1.CutoverManual)
	mig.Spec.Follow.Mode = v1beta1.FollowModePerformance
	mig.Spec.Runner.Image = legacyRunnerImage
	DeferCleanup(func() { deleteMigration(name) })
	create(mig)

	By("failing at preflight, before any worker attempt")
	m := waitFailed(name, reasonPreflightFailed)
	Expect(m.Status.Attempts).To(BeZero(), "a worker ran although the preflight failed")
	msg := failureMessage(m)
	Expect(msg).To(ContainSubstring(legacyRunnerImage), "the message does not name the runner image")
	Expect(msg).To(ContainSubstring("does not list "+pgcopydb.FollowModeFlag), "the message does not name the flag")
	Expect(sourceSlotCount()).To(Equal("0"), "a refused Migration created a replication slot")
}
