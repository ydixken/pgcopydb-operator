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
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// postSlotFirstID is the first documents id the snapshot spec inserts. It sits
// above every seeded id, even at stress scale, so resetSourceReplication finds them.
const postSlotFirstID = 1000000000

// Its own container because two base copies put it in the longest tier, and a
// container's SpecPriority moves every spec in it.
var _ = Describe("Migration", SpecPriority(3), func() {
	BeforeEach(resetMigrationPair)

	// Issue 378: a retry resumed the copy under --not-consistent on the slot
	// attempt 1 created, so rows written after the slot came in through both the
	// copy and the stream, and the apply stopped on a duplicate key.
	It("starts a follow clone over when the attempt that created its slot dies mid-copy", func() {
		if scale < 0.05 {
			Skip("the snapshot spec needs the documents COPY to outlast the poll round trip;" +
				" E2E_SCALE=" + scaleArg() + " is below 0.05")
		}
		const (
			name   = "e2e-resume-snapshot"
			marker = "live-snapshot"
			rows   = 1000
		)
		// Separate nodes, run last-in first-out: the resets follow the delete and
		// still run when it fails, so the post-slot rows cannot reach later specs.
		DeferCleanup(resetTargetReplication)
		DeferCleanup(resetSourceReplication)
		DeferCleanup(deleteMigration, name)

		mig := newFollowMigration(name, v1beta1.CutoverManual)
		// The start-over drops what attempt 1 restored. It takes three attempts,
		// and issue 88 can cost one more on any resume, so the budget goes up.
		mig.Spec.Clone.DropIfExists = true
		mig.Spec.BackoffLimit = 5

		By("writing to orders on the source throughout")
		w := startLiveWriter(marker)
		DeferCleanup(func() {
			_, _ = w.stop()
			if CurrentSpecReport().Failed() {
				AddReportEntry("live writer after stop", w.diagnostics().String(), ReportEntryVisibilityFailureOrVerbose)
			}
		})
		create(mig)
		// Runs before deleteMigration, which garbage-collects the attempt Jobs.
		DeferCleanup(func() {
			if !CurrentSpecReport().Failed() {
				return
			}
			m := &v1beta1.Migration{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m); err != nil {
				AddReportEntry("attempt logs", "Migration unavailable: "+err.Error(), ReportEntryVisibilityFailureOrVerbose)
				return
			}
			reportFailedAttempt(m)
			for n := int32(2); n <= m.Status.Attempts; n++ {
				job := fmt.Sprintf("%s-run-%d", name, n)
				AddReportEntry(job+" log tail", jobLogs(job, attemptLogTail), ReportEntryVisibilityFailureOrVerbose)
			}
		})

		// Finalizing still counts as attempt 1's copy: a sample between two COPY
		// statements can report it. Streaming and later mean the copy is over.
		copyPhases := []v1beta1.MigrationPhase{"", v1beta1.PhasePending, v1beta1.PhaseValidating,
			v1beta1.PhaseCloning, v1beta1.PhaseFinalizing}

		By("waiting until the slot exists and the documents COPY runs on the target")
		// COPY is transactional, so committed target rows cannot time this; the
		// progress view shows a COPY while it runs (see the chaos source-kill spec).
		Eventually(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), cur)).To(Succeed())
			if cur.Status.Attempts > 1 || !slices.Contains(copyPhases, cur.Status.Phase) {
				StopTrying(fmt.Sprintf("missed the documents copy window: attempt %d at phase %q",
					cur.Status.Attempts, cur.Status.Phase)).Now()
			}
			g.Expect(sourceSlotCount()).To(Equal("1"), "no replication slot on the source yet")
			g.Expect(psql(targetCluster, "SELECT coalesce(sum(tuples_processed), 0) > 0"+
				" FROM pg_stat_progress_copy WHERE relid = to_regclass('public.documents')")).
				To(Equal("t"), "no COPY on documents in progress yet")
		}, migrationTimeout, 500*time.Millisecond).Should(Succeed())

		By("inserting documents rows on the source after the slot, while documents is still copying")
		psqlBulk(sourceCluster, fmt.Sprintf("INSERT INTO documents (id, title, body, attachment, checksum)"+
			" SELECT %d + g, 'post-slot ' || g, md5(g::text), decode(md5(g::text), 'hex'), md5(g::text)"+
			" FROM generate_series(0, %d) g", postSlotFirstID, rows-1))

		By("killing the attempt-1 runner pod mid-copy")
		deletePod(client.MatchingLabels{migrationLabel: name, batchv1.JobNameLabel: name + "-run-1"})

		By("waiting for CutoverPending")
		// A build that resumes on the lost snapshot fails its apply on a duplicate
		// key and stops here on Failed; the run-N log tails in the report show it.
		m := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCutoverPending)
		Expect(m.Status.Attempts).To(BeNumerically(">=", 3),
			"no start-over followed the kill; the documents COPY may have finished before it")

		By("checking the operator recorded the lost snapshot")
		var lost time.Time
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
			lost = time.Time{}
			for _, e := range events.Items {
				if e.InvolvedObject.UID == mig.UID && e.Type == corev1.EventTypeWarning &&
					e.Reason == "SnapshotLost" && (lost.IsZero() || eventTime(e).Before(lost)) {
					lost = eventTime(e)
				}
			}
			g.Expect(lost.IsZero()).To(BeFalse(), "no SnapshotLost warning event on %s", name)
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("checking the first attempt after it started over instead of resuming")
		// creationTimestamp has whole seconds on the API server's clock and the
		// event time comes from the operator's; the refused attempt began earlier.
		since := lost.Truncate(time.Second).Add(-time.Second)
		var args []string
		for n := int32(2); n <= m.Status.Attempts && args == nil; n++ {
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: fmt.Sprintf("%s-run-%d", name, n)},
				job)).To(Succeed())
			if !job.CreationTimestamp.Time.Before(since) {
				args = job.Spec.Template.Spec.Containers[0].Args
			}
		}
		Expect(args).NotTo(BeNil(), "no attempt Job was created after the SnapshotLost event")
		Expect(args).To(ContainElements("--restart", "--drop-if-exists"))
		Expect(args).NotTo(ContainElement("--resume"))
		Expect(args).NotTo(ContainElement("--not-consistent"))

		By("stopping the writer, so the source is quiescent from the freeze onward")
		last, err := w.stop()
		Expect(publicationRetryProbeError(err)).NotTo(HaveOccurred(),
			"the live writer failed; see the live writer diagnostic report for final marker availability")
		Expect(last).To(BeNumerically(">", 0), "the writer committed no row, so the gap check would pass on nothing")

		approveCutover(name)
		m = waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
		expectCleanupSucceeded(name)

		By("checking every post-slot documents row arrived exactly once")
		// count(DISTINCT) still means something if a start-over ever leaves the
		// target without its primary key.
		Expect(psql(targetCluster, fmt.Sprintf("SELECT count(*), count(DISTINCT id) FROM documents WHERE id >= %d",
			postSlotFirstID))).To(Equal(fmt.Sprintf("%d|%d", rows, rows)))
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))

		By("checking every committed live row arrived, with no gaps")
		srcRows := psql(sourceCluster, fmt.Sprintf("SELECT count(*) FROM orders WHERE note LIKE '%s-%%'", marker))
		Expect(psql(targetCluster, fmt.Sprintf("SELECT count(*) FROM orders WHERE note LIKE '%s-%%'", marker))).
			To(Equal(srcRows), func() string { return writeLoadDiagnosis(marker, last, srcRows) })
		firstGap := psql(targetCluster, fmt.Sprintf(
			"SELECT coalesce(min(g), 0) FROM generate_series(1, %d) g"+
				" WHERE NOT EXISTS (SELECT 1 FROM orders WHERE note = '%s-' || g)", last, marker))
		Expect(firstGap).To(Equal("0"),
			"target is missing row %s of %d: a gap proves a lost transaction, not a slow one", firstGap, last)

		Eventually(sourceSlotCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
			"replication slot leaked on the source by the start-over")
		Expect(psql(sourceCluster, publicationRetryPubCount+pgcopydb.SlotName(nsE2E, name)+"'")).To(Equal("0"),
			"auto-created publication leaked on the source by the start-over")
		Eventually(targetOriginCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
			"pgcopydb replication origin leaked on the target by the start-over")
	})
})
