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

	apimeta "k8s.io/apimachinery/pkg/api/meta"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// These specs cover the non-superuser privilege path end to end: the preflight
// probes pass, the CREATE grants the handover asks for are the ones it needs,
// every partition moves, and a live migration hands over before it announces
// the cutover. Which objects the handover skips is not provable from here and
// is not attempted; TestReownCandidateQueries owns that, with the catalog
// access this suite does not have.
var _ = Describe("Ownership after restore", func() {
	BeforeEach(func() {
		Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		DeferCleanup(resetTargetObjects)
		resetTargetObjects()
		// LIFO, so this runs before resetTargetObjects drops what it hands back.
		DeferCleanup(resetOwnerRole)
		resetOwnerRole()

		By("creating the incoming owner with the rights the handover needs")
		psql(targetCluster, "CREATE ROLE app_owner NOLOGIN")
		psql(targetCluster, "GRANT app_owner TO app")
		// public stays owned by pg_database_owner, so the handover never
		// transfers it and the objects in it need a standing CREATE grant for
		// the incoming owner. audit needs none: app owns it after the restore,
		// so it is transferred and carries CREATE with it.
		psql(targetCluster, "GRANT CREATE ON SCHEMA public TO app_owner")

		By("proving the preconditions the operator's own probes test")
		// What the preflight probes, in the form it probes it: a plain GRANT
		// carries SET on every supported version.
		psql(targetCluster, "BEGIN; SET SESSION AUTHORIZATION app; SET ROLE app_owner; ROLLBACK")
		// The handover's database pre-check tests the migration role, not the
		// incoming owner (has_database_privilege(current_user, ...) in
		// reown.go), and app owns the target database: nothing to grant.
		Expect(psql(targetCluster,
			"SELECT has_database_privilege('app', current_database(), 'CREATE')")).To(Equal("t"))
	})

	It("hands the restored objects to the target role", func() {
		const name = "e2e-owner-after-restore"
		DeferCleanup(func() { deleteMigration(name) })

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			DropIfExists:      true,
			NoOwner:           true,
			OwnerAfterRestore: "app_owner",
		})
		// compare schema runs after the handover and models tables, columns,
		// indexes, constraints and sequence values, not owners, so it has to
		// pass on a target that changed hands.
		m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
		create(m)

		completed := waitCompleted(name, nsE2E)
		expectSingleAttempt(completed)
		expectConditionTrue(completed, v1beta1.ConditionOwnershipApplied)
		expectConditionTrue(completed, v1beta1.ConditionVerified)
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
		expectHandoverApplied()
	})

	It("hands over before it announces the cutover", func() {
		const name = "e2e-owner-after-restore-follow"
		DeferCleanup(func() { deleteMigration(name) })

		mig := newFollowMigration(name, v1beta1.CutoverManual)
		mig.Spec.Clone.NoOwner = true
		mig.Spec.Clone.OwnerAfterRestore = "app_owner"
		create(mig)

		By("waiting for the base copy to finish, streaming to start, and the lag to converge")
		waitFollowStreaming(name)

		By("approving the cutover (nothing wrote to the source, so it is already quiescent)")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		approveCutover(name)

		m := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectSingleAttempt(m)
		expectConditionTrue(m, v1beta1.ConditionOwnershipApplied)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)

		By("checking the handover finished before the cutover was announced")
		// CutoverCompleted is the signal to point applications at the target,
		// so ownership has to be right by the time it turns True. Equal
		// timestamps are the norm: the controller sets both in one pass.
		handover := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionOwnershipApplied)
		cutover := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCutoverComplete)
		Expect(handover.LastTransitionTime.Time).To(BeTemporally("<=", cutover.LastTransitionTime.Time),
			"cutover was announced while the target was still changing hands")

		expectHandoverApplied()
		Expect(rowCounts(targetCluster)).To(Equal(rowCounts(sourceCluster)))

		By("checking the handover did not hold up the cleanup")
		expectCleanupSucceeded(name)
		Expect(sourceSlotCount()).To(Equal("0"), "replication slot left behind on the source")
		Expect(targetOriginCount()).To(Equal("0"),
			"pgcopydb replication origin left behind on the target after cleanup")
	})
})

// resetOwnerRole drops the incoming owner and everything it holds, so a run
// that crashed mid-spec cannot wedge the next one. Callers run it before
// resetTargetObjects, which drops the objects this hands back.
func resetOwnerRole() {
	GinkgoHelper()
	if psql(targetCluster, "SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_owner')") != "t" {
		return
	}
	psql(targetCluster, "REASSIGN OWNED BY app_owner TO app")
	psql(targetCluster, "DROP OWNED BY app_owner")
	psql(targetCluster, "DROP ROLE app_owner")
}

// ownerCounts reads how many of the relations a predicate selects ended up
// owned by app_owner, over how many exist at all. The pair is the point:
// counting only the wrongly-owned ones would pass on a target with none.
func ownerCounts(where string) string {
	return "SELECT count(*) FILTER (WHERE pg_get_userbyid(c.relowner) = 'app_owner') || '/' || count(*)" +
		" FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE " + where
}

// expectHandoverApplied asserts on the target what the handover must have
// done, for the clone-only and the live path alike.
func expectHandoverApplied() {
	GinkgoHelper()

	// Every partition on its own: ALTER TABLE on the parent does not reach
	// them. Eight monthly partitions plus the default (fixtures/schema.sql).
	Expect(psql(targetCluster,
		ownerCounts("c.relispartition AND c.relkind = 'r' AND n.nspname = 'public'"))).To(Equal("9/9"))

	// The identity and serial sequences plus the two standalone ones. The
	// first three change owner with their table, which is the owned_sequence
	// exclusion in reown.go, and must arrive all the same.
	Expect(psql(targetCluster,
		ownerCounts("c.relkind = 'S' AND n.nspname IN ('public', 'audit')"))).To(Equal("5/5"))

	// The schema itself, not only its contents.
	Expect(psql(targetCluster,
		"SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'audit'")).To(Equal("app_owner"))

	// public is not transferred: it was never the migration role's to give.
	Expect(psql(targetCluster,
		"SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'public'")).
		To(Equal("pg_database_owner"))

	// citext keeps the role that installed it, and its members never enter the
	// candidate set: reown.go excludes everything that depends on an extension
	// with deptype e.
	Expect(psql(targetCluster,
		"SELECT pg_get_userbyid(extowner) FROM pg_extension WHERE extname = 'citext'")).To(Equal(appDB))

	// The exclusion is not overbroad: app_users has a citext column and is an
	// ordinary candidate, so it moves like any other table. The regclass casts
	// fail loudly when the restore did not create it.
	Expect(psql(targetCluster, "SELECT count(*) FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid"+
		" WHERE a.attrelid = 'public.app_users'::regclass AND t.typname = 'citext'")).To(Equal("1"))
	Expect(psql(targetCluster, "SELECT pg_get_userbyid(relowner) FROM pg_class"+
		" WHERE oid = 'public.app_users'::regclass")).To(Equal("app_owner"))

	// Ownership in fact, not only in the catalog: only an owner may alter a
	// table, and SET ROLE drops the superuser attribute psql connects with.
	psql(targetCluster, "BEGIN; SET ROLE app_owner;"+
		" ALTER TABLE public.customers ADD COLUMN reown_probe int; ROLLBACK")
}
