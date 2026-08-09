/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// Chaos scenarios: pod kills, disk pressure, and concurrency against the
// shared fixtures. Each spec creates its own Migration and restores whatever
// it disturbed in DeferCleanup, so any subset runs standalone. The default
// run excludes them (task e2e filters !chaos); task e2e:chaos selects exactly
// this label.
var _ = Describe("Migration chaos", Label("chaos"), func() {
	It("recovers a clone after the source primary dies mid-copy", func() {
		const name = "e2e-chaos-srckill"
		// The kill has to land inside the COPY of documents, the biggest
		// table (scale x ~8.5GB of TOAST). Below scale 0.05 (~425MB) that
		// COPY can finish within a few kubectl-exec poll round trips, so the
		// window is no longer guaranteed; Skip instead of flaking.
		if scale < 0.05 {
			Skip("source-kill needs the documents COPY to outlast the poll round trip;" +
				" E2E_SCALE=" + scaleArg() + " is below 0.05")
		}
		DeferCleanup(func() {
			deleteMigration(name)
			waitClusterReady(sourceCluster)
		})

		create(newMigration(name, nsE2E, v1beta1.CloneOptions{DropIfExists: true}))

		By("waiting for the documents COPY to run on the target")
		// Committed rows cannot time this kill: COPY is transactional, so
		// count(*) on the target stays 0 until the copy commits and then
		// jumps to the full count (the first live run polled for rows and
		// the clone finished before the trigger fired). pg_stat_progress_copy
		// instead shows the running COPY's live tuple counter: any progress
		// on documents means the data copy is happening right now, with most
		// of its minutes still ahead at the asserted scale.
		Eventually(func(g Gomega) {
			g.Expect(psql(tgtPod, "SELECT coalesce(sum(tuples_processed), 0) > 0"+
				" FROM pg_stat_progress_copy WHERE relid = to_regclass('public.documents')")).
				To(Equal("t"), "no COPY on documents in progress yet")
		}, migrationTimeout, 500*time.Millisecond).Should(Succeed())

		By("killing the source primary mid-copy")
		deletePod(client.MatchingLabels{"cnpg.io/cluster": sourceCluster, "cnpg.io/instanceRole": "primary"})

		By("waiting for the resumed clone to complete")
		m := waitCompleted(name, nsE2E)
		Expect(m.Status.Attempts).To(BeNumerically(">=", 2),
			"the clone survived a dead source without a new attempt; the kill missed the copy window")

		By("comparing the scale-derived counts on both sides")
		Expect(seedTableCounts(srcPod)).To(Equal(expectedSeedCounts()),
			"source rows deviate from the scale formula after the primary restart")
		Expect(seedTableCounts(tgtPod)).To(Equal(seedTableCounts(srcPod)))
	})

	// Disk pressure needs follow mode: a clone's work dir holds catalogs and
	// schema dumps only, a few MB that never fill the volume (the first live
	// run completed on 200Mi with room to spare). Follow mode spools every
	// decoded change under the work dir as JSON plus transformed SQL and does
	// not prune the files while the migration runs, so sustained source
	// writes grow the spool without bound.
	It("fails a follow migration when the change spool fills the work volume", func() {
		const name = "e2e-chaos-diskfull"
		DeferCleanup(func() {
			deleteMigration(name)
			resetSourceReplication()
			resetTargetReplication()
		})

		m := newFollowMigration(name, v1beta1.CutoverManual)
		// 200Mi carries the clone-phase catalogs comfortably (proven live)
		// and leaves a handful of burst batches as spool headroom.
		m.Spec.WorkVolume.Size = resource.MustParse("200Mi")
		// ENOSPC persists on a full volume, so the --resume attempt fails
		// the same way; backoffLimit 1 keeps that to one extra attempt
		// before the absorbing Failed.
		m.Spec.BackoffLimit = 1
		create(m)

		By("waiting for the base copy to finish and streaming to start")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)

		By("bursting TOAST rewrites until the spool overflows the work volume")
		// documents carries the chunkiest rows the fixture has: each touched
		// row re-logs a ~16KiB body, which lands on the spool twice (JSON
		// and SQL), roughly 30MiB per 1000-row batch. The value changes per
		// batch because logical decoding skips unchanged TOAST datums, and
		// the loop keeps writing until the operator reports Failed, so a
		// scale too small for 1000 distinct ids just needs more batches.
		batch := 0
		Eventually(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, cur)).To(Succeed())
			if cur.Status.Phase != v1beta1.PhaseFailed {
				batch++
				psql(srcPod, fmt.Sprintf("UPDATE documents SET body = repeat(md5('spool-%d-' || id), 500)"+
					" WHERE id <= 1000", batch))
			}
			g.Expect(cur.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		}, migrationTimeout, time.Second).Should(Succeed())

		failed := waitFailed(name, "BackoffLimitExceeded")
		// The controller has no disk-specific wording; the failure surfaces
		// through the generic Job-failure path (handleFailedJob), which
		// appends the worker's last pgcopydb error line ("No space left on
		// device") to the Failed condition message. That line is what
		// carries "space".
		Expect(failureMessage(failed)).To(ContainSubstring("space"),
			"Failed message does not surface the out-of-space error")
	})

	It("fans two concurrent follow migrations from one source into two target databases", func() {
		const (
			nameA = "e2e-chaos-fan-a"
			nameB = "e2e-chaos-fan-b"
			dbB   = "app2"
		)
		DeferCleanup(func() {
			deleteMigration(nameA)
			deleteMigration(nameB)
			psql(tgtPod, "DROP DATABASE IF EXISTS "+dbB+" WITH (FORCE)")
			Eventually(sourceSlotCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
				"replication slot left on the source after the fan-out cleanup")
			Eventually(targetOriginCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
				"replication origin left on the target after the fan-out cleanup")
		})

		By("creating the app2 database and its per-database follow grants on the target")
		if psql(tgtPod, "SELECT count(*) FROM pg_database WHERE datname = '"+dbB+"'") == "0" {
			psql(tgtPod, "CREATE DATABASE "+dbB+" OWNER app")
		}
		grantTargetFollowPrivileges(dbB)

		By("starting both follow migrations")
		create(newFollowMigration(nameA, v1beta1.CutoverManual))
		mb := newFollowMigration(nameB, v1beta1.CutoverManual)
		mb.Spec.Target.Database = dbB
		create(mb)

		By("waiting for both to stream")
		ma := waitPhase(nameA, nsE2E, followTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)
		mbl := waitPhase(nameB, nsE2E, followTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)

		By("checking the source carries two distinct pgcopydb slots")
		Expect(psql(srcPod, "SELECT count(DISTINCT slot_name) FROM pg_replication_slots"+
			" WHERE slot_name LIKE 'pgcopydb%'")).To(Equal("2"))
		Expect(ma.Status.Replication).NotTo(BeNil(), "status.replication missing on %s", nameA)
		Expect(mbl.Status.Replication).NotTo(BeNil(), "status.replication missing on %s", nameB)
		Expect(ma.Status.Replication.SlotName).NotTo(BeEmpty())
		Expect(ma.Status.Replication.SlotName).NotTo(Equal(mbl.Status.Replication.SlotName),
			"both migrations report the same slot")

		By("approving both cutovers")
		waitPhase(nameA, nsE2E, followTimeout, v1beta1.PhaseCutoverPending)
		approveCutover(nameA)
		waitPhase(nameB, nsE2E, followTimeout, v1beta1.PhaseCutoverPending)
		approveCutover(nameB)

		By("waiting for both to complete independently")
		for _, name := range []string{nameA, nameB} {
			m := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
			expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
			expectCleanupSucceeded(name)
		}

		By("comparing the seeded data in both target databases")
		Expect(seedTableCounts(tgtPod)).To(Equal(seedTableCounts(srcPod)))
		Expect(psqlDB(tgtPod, dbB, seedCountSQL)).To(Equal(seedTableCounts(srcPod)))

		By("checking no replication state remains on either side")
		Expect(sourceSlotCount()).To(Equal("0"))
		Expect(targetOriginCount()).To(Equal("0"))
	})

	// THE INVARIANT: a runner killed between endpos-set and drain-complete
	// must leave the Migration in exactly one of two states. Either it
	// recovers and reaches Completed with every source row on the target, or
	// it reaches Failed with reason DrainIncomplete and the replication slot
	// still on the source, so the data stays recoverable. Completed with
	// missing rows is the silent-loss mode the drain-verify gate exists to
	// prevent; any other terminal state fails this spec.
	It("either completes fully or fails DrainIncomplete when the runner dies mid-drain", func() {
		const name = "e2e-chaos-drainkill"
		DeferCleanup(func() {
			deleteMigration(name)
			psql(srcPod, "DELETE FROM orders WHERE note LIKE 'live-drain-%'")
			Eventually(sourceSlotCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
				"replication slot left on the source after cleanup")
			Eventually(targetOriginCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
				"replication origin left on the target after cleanup")
		})

		create(newFollowMigration(name, v1beta1.CutoverManual))
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)

		By("approving the cutover and writing a backlog until endpos is set")
		approveCutover(name)
		// Keep the apply side busy so the drain window after endpos has real
		// work: insert batches until the operator reports CuttingOver, which
		// it sets in the same status write that records the endpos.
		batch := 0
		Eventually(func(g Gomega) {
			m := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			if m.Status.Phase != v1beta1.PhaseCuttingOver {
				batch++
				psql(srcPod, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note)"+
					" SELECT (g %% %d) + 1, (g %% 90)::numeric / 3, 'live-drain-%d-' || g"+
					" FROM generate_series(1, 2000) g", scaled(50000), batch))
			}
			g.Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		}, migrationTimeout, time.Second).Should(Succeed())

		By("killing the runner pod inside the drain window")
		deletePod(client.MatchingLabels{"pgcopydb-operator.io/migration": name})

		By("waiting for a terminal phase and asserting the invariant")
		m := waitTerminal(name, followTimeout)
		switch m.Status.Phase {
		case v1beta1.PhaseCompleted:
			expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
			Expect(psql(tgtPod, "SELECT count(*) FROM orders WHERE note LIKE 'live-drain-%'")).
				To(Equal(psql(srcPod, "SELECT count(*) FROM orders WHERE note LIKE 'live-drain-%'")),
					"INVARIANT VIOLATED: Completed with rows missing on the target")
			Expect(rowCounts(tgtPod)).To(Equal(rowCounts(srcPod)))
			Expect(sourceSlotCount()).To(Equal("0"))
		case v1beta1.PhaseFailed:
			c := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
			Expect(c).NotTo(BeNil())
			Expect(c.Reason).To(Equal("DrainIncomplete"),
				"INVARIANT VIOLATED: failed outside the drain-verify gate: %s: %s", c.Reason, c.Message)
			Expect(sourceSlotCount()).To(Equal("1"),
				"INVARIANT VIOLATED: DrainIncomplete without the replication slot retained")
		}
	})

	It("resumes the stream without duplicates after the target primary dies mid-apply", func() {
		const name = "e2e-chaos-tgtkill"
		DeferCleanup(func() {
			deleteMigration(name)
			waitClusterReady(targetCluster)
			psql(srcPod, "DELETE FROM orders WHERE note LIKE 'live-tgt-%'")
			Eventually(sourceSlotCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
				"replication slot left on the source after cleanup")
			Eventually(targetOriginCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
				"replication origin left on the target after cleanup")
		})

		create(newFollowMigration(name, v1beta1.CutoverManual))
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)

		By("writing live rows and killing the target primary mid-apply")
		psql(srcPod, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) SELECT (g %% %d) + 1,"+
			" (g %% 90)::numeric / 3, 'live-tgt-' || g FROM generate_series(1, 2000) g", scaled(50000)))
		deletePod(client.MatchingLabels{"cnpg.io/cluster": targetCluster, "cnpg.io/instanceRole": "primary"})
		waitClusterReady(targetCluster)

		By("waiting for the stream to recover on a later attempt")
		Eventually(func(g Gomega) {
			m := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			g.Expect(m.Status.Attempts).To(BeNumerically(">=", 2),
				"the worker survived a dead target without a new attempt")
			g.Expect([]v1beta1.MigrationPhase{v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending}).
				To(ContainElement(m.Status.Phase), "phase %q after the target kill", m.Status.Phase)
		}, migrationTimeout, 2*time.Second).Should(Succeed())

		By("approving the cutover and waiting for completion")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		approveCutover(name)
		m := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
		expectCleanupSucceeded(name)

		By("checking origin dedup: every live row arrived exactly once")
		Expect(psql(tgtPod, "SELECT count(*) FROM orders WHERE note LIKE 'live-tgt-%'")).To(Equal("2000"),
			"more than 2000 means origin dedup replayed rows twice, fewer means loss")
		Expect(rowCounts(tgtPod)).To(Equal(rowCounts(srcPod)))
		Expect(sourceSlotCount()).To(Equal("0"))
		Expect(targetOriginCount()).To(Equal("0"))
	})
})

// waitTerminal waits until the Migration in nsE2E reaches Completed or Failed
// and returns it; the caller owns the verdict on which of the two (or both,
// for the drain-kill invariant) is acceptable.
func waitTerminal(name string, timeout time.Duration) *v1beta1.Migration {
	GinkgoHelper()
	m := &v1beta1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
		g.Expect([]v1beta1.MigrationPhase{v1beta1.PhaseCompleted, v1beta1.PhaseFailed}).
			To(ContainElement(m.Status.Phase),
				"migration %s at phase %q, attempts %d", name, m.Status.Phase, m.Status.Attempts)
	}, timeout, 2*time.Second).Should(Succeed())
	return m
}
