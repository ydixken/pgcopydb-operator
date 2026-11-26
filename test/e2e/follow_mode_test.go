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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// legacyRunnerImage carries pgcopydb 9d8b660, which has no --follow-mode, whatever
// E2E_RUNNER_TAG says, so these specs prove sqlite needs nothing from a newer runner.
const legacyRunnerImage = "ghcr.io/ydixken/pgcopydb-operator/runner:v0.28.0"

var _ = Describe("Follow mode", func() {
	BeforeEach(resetMigrationPair)

	It("follows and cuts over in sqlite mode with the v0.28.0 runner pinned", legacyRunnerSQLiteCutover)

	It("fails the preflight of performance mode with the v0.28.0 runner pinned, naming the image and --follow-mode",
		legacyRunnerPerformanceRefused)
})

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
