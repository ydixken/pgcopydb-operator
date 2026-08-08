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

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

// Chaos scenarios: pod kills, disk pressure, and concurrency against the
// shared fixtures. Each spec creates its own Migration and restores whatever
// it disturbed in DeferCleanup, so any subset runs standalone. The default
// run excludes them (task e2e filters !chaos); task e2e:chaos selects exactly
// this label.
var _ = Describe("Migration chaos", Label("chaos"), func() {
	It("recovers a clone after the source primary dies mid-copy", func() {
		const name = "e2e-chaos-srckill"
		DeferCleanup(func() {
			deleteMigration(name)
			waitClusterReady(sourceCluster)
		})

		create(newMigration(name, nsE2E, v1alpha1.CloneOptions{DropIfExists: true}))

		By("waiting for attempt 1 to be mid-copy")
		Eventually(func(g Gomega) {
			m := &v1alpha1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			g.Expect(m.Status.Attempts).To(Equal(int32(1)))
			g.Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCloning))
			// Table presence on the target means the pre-data restore is done
			// and the data copy is running or imminent: mid-clone, not
			// pre-clone. At tiny E2E_SCALE values the copy window shrinks and
			// the kill below may miss it; chaos assumes the default scale.
			g.Expect(psql(tgtPod, "SELECT to_regclass('public.documents') IS NOT NULL")).To(Equal("t"))
		}, migrationTimeout, time.Second).Should(Succeed())

		By("killing the source primary")
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

	It("fails a clone whose work volume runs out of space", func() {
		const name = "e2e-chaos-diskfull"
		DeferCleanup(func() { deleteMigration(name) })

		m := newMigration(name, nsE2E, v1alpha1.CloneOptions{DropIfExists: true})
		m.Spec.WorkVolume.Size = resource.MustParse("200Mi")
		// ENOSPC is deterministic on a full volume; a bigger budget would just
		// fail the same way more times.
		m.Spec.BackoffLimit = 1
		create(m)

		failed := waitFailed(name, "BackoffLimitExceeded")
		// The controller has no disk-specific wording; the failure surfaces
		// through the generic Job-failure path, which appends the worker's
		// last pgcopydb error line ("No space left on device") to the Failed
		// condition message. That line is what carries "space".
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
		create(newFollowMigration(nameA, v1alpha1.CutoverManual))
		mb := newFollowMigration(nameB, v1alpha1.CutoverManual)
		mb.Spec.Target.Database = dbB
		create(mb)

		By("waiting for both to stream")
		ma := waitPhase(nameA, nsE2E, followTimeout, v1alpha1.PhaseStreaming, v1alpha1.PhaseCutoverPending)
		mbl := waitPhase(nameB, nsE2E, followTimeout, v1alpha1.PhaseStreaming, v1alpha1.PhaseCutoverPending)

		By("checking the source carries two distinct pgcopydb slots")
		Expect(psql(srcPod, "SELECT count(DISTINCT slot_name) FROM pg_replication_slots"+
			" WHERE slot_name LIKE 'pgcopydb%'")).To(Equal("2"))
		Expect(ma.Status.Replication).NotTo(BeNil(), "status.replication missing on %s", nameA)
		Expect(mbl.Status.Replication).NotTo(BeNil(), "status.replication missing on %s", nameB)
		Expect(ma.Status.Replication.SlotName).NotTo(BeEmpty())
		Expect(ma.Status.Replication.SlotName).NotTo(Equal(mbl.Status.Replication.SlotName),
			"both migrations report the same slot")

		By("approving both cutovers")
		waitPhase(nameA, nsE2E, followTimeout, v1alpha1.PhaseCutoverPending)
		approveCutover(nameA)
		waitPhase(nameB, nsE2E, followTimeout, v1alpha1.PhaseCutoverPending)
		approveCutover(nameB)

		By("waiting for both to complete independently")
		for _, name := range []string{nameA, nameB} {
			m := waitPhase(name, nsE2E, followTimeout, v1alpha1.PhaseCompleted)
			expectConditionTrue(m, v1alpha1.ConditionCutoverComplete)
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

		create(newFollowMigration(name, v1alpha1.CutoverManual))
		waitPhase(name, nsE2E, migrationTimeout, v1alpha1.PhaseStreaming, v1alpha1.PhaseCutoverPending)

		By("approving the cutover and writing a backlog until endpos is set")
		approveCutover(name)
		// Keep the apply side busy so the drain window after endpos has real
		// work: insert batches until the operator reports CuttingOver, which
		// it sets in the same status write that records the endpos.
		batch := 0
		Eventually(func(g Gomega) {
			m := &v1alpha1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			if m.Status.Phase != v1alpha1.PhaseCuttingOver {
				batch++
				psql(srcPod, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note)"+
					" SELECT (g %% %d) + 1, (g %% 90)::numeric / 3, 'live-drain-%d-' || g"+
					" FROM generate_series(1, 2000) g", scaled(50000), batch))
			}
			g.Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCuttingOver))
		}, migrationTimeout, time.Second).Should(Succeed())

		By("killing the runner pod inside the drain window")
		deletePod(client.MatchingLabels{"pgcopydb-operator.io/migration": name})

		By("waiting for a terminal phase and asserting the invariant")
		m := waitTerminal(name, followTimeout)
		switch m.Status.Phase {
		case v1alpha1.PhaseCompleted:
			expectConditionTrue(m, v1alpha1.ConditionCutoverComplete)
			Expect(psql(tgtPod, "SELECT count(*) FROM orders WHERE note LIKE 'live-drain-%'")).
				To(Equal(psql(srcPod, "SELECT count(*) FROM orders WHERE note LIKE 'live-drain-%'")),
					"INVARIANT VIOLATED: Completed with rows missing on the target")
			Expect(rowCounts(tgtPod)).To(Equal(rowCounts(srcPod)))
			Expect(sourceSlotCount()).To(Equal("0"))
		case v1alpha1.PhaseFailed:
			c := apimeta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionFailed)
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

		create(newFollowMigration(name, v1alpha1.CutoverManual))
		waitPhase(name, nsE2E, migrationTimeout, v1alpha1.PhaseStreaming, v1alpha1.PhaseCutoverPending)

		By("writing live rows and killing the target primary mid-apply")
		psql(srcPod, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) SELECT (g %% %d) + 1,"+
			" (g %% 90)::numeric / 3, 'live-tgt-' || g FROM generate_series(1, 2000) g", scaled(50000)))
		deletePod(client.MatchingLabels{"cnpg.io/cluster": targetCluster, "cnpg.io/instanceRole": "primary"})
		waitClusterReady(targetCluster)

		By("waiting for the stream to recover on a later attempt")
		Eventually(func(g Gomega) {
			m := &v1alpha1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			g.Expect(m.Status.Attempts).To(BeNumerically(">=", 2),
				"the worker survived a dead target without a new attempt")
			g.Expect([]v1alpha1.MigrationPhase{v1alpha1.PhaseStreaming, v1alpha1.PhaseCutoverPending}).
				To(ContainElement(m.Status.Phase), "phase %q after the target kill", m.Status.Phase)
		}, migrationTimeout, 2*time.Second).Should(Succeed())

		By("approving the cutover and waiting for completion")
		waitPhase(name, nsE2E, migrationTimeout, v1alpha1.PhaseCutoverPending)
		approveCutover(name)
		m := waitPhase(name, nsE2E, followTimeout, v1alpha1.PhaseCompleted)
		expectConditionTrue(m, v1alpha1.ConditionCutoverComplete)
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
func waitTerminal(name string, timeout time.Duration) *v1alpha1.Migration {
	GinkgoHelper()
	m := &v1alpha1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
		g.Expect([]v1alpha1.MigrationPhase{v1alpha1.PhaseCompleted, v1alpha1.PhaseFailed}).
			To(ContainElement(m.Status.Phase),
				"migration %s at phase %q, attempts %d", name, m.Status.Phase, m.Status.Attempts)
	}, timeout, 2*time.Second).Should(Succeed())
	return m
}
