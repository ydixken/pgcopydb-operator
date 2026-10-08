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
	"context"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// Each spec here orders source and target sessions in a way plain SQL cannot,
// then holds the target to the source's fingerprint after cutover. The origin
// still reaches endpos in every failure they pin, so the drain gate alone passes.
var _ = Describe("Follow correctness", SpecPriority(1), func() {
	BeforeEach(func() {
		// The coverage helpers need cov_reader, and the walsender signal needs a CNPG pod.
		requireCNPGFixtures()
		resetMigrationPair()
	})

	It("applies source transactions that overlap, the last one before endpos included", overlappingTransactions)
	It("applies a large transaction once after a walsender restart sends it again", resentTransaction)
	It("applies a transaction again after its COMMIT failed on the target", failedCommitRetried)
})

func overlappingTransactions() {
	c := followCorrectnessCase("overlap",
		"CREATE TABLE ${schema}.t (side text, round int, step int, xid bigint NOT NULL, PRIMARY KEY (side, round, step));\n")
	m, identity, captured := startFollowCorrectness("e2e-overlap-", c)
	table := c.schemaNames(identity)[0] + ".t"

	for round := 1; round <= 3; round++ {
		By(fmt.Sprintf("committing B inside A on the source, round %d", round))
		overlapOnSource(table, round)
	}
	By("committing once more after the last overlap and waiting for it on the target")
	psql(sourceCluster, asSourceAppRole()+"INSERT INTO "+table+" VALUES ('C', 3, 1, pg_current_xact_id()::text::bigint)")
	waitOnTarget(m, "SELECT count(*) FROM "+table+" WHERE side = 'C'", "1")

	By("ending the stream on an overlap, so A commits last before endpos")
	overlapOnSource(table, 4)
	Expect(psql(sourceCluster, "SELECT count(*) FROM "+table+" a JOIN "+table+" b"+
		" ON b.side = 'B' AND b.round = a.round AND b.step = 1 WHERE a.side = 'A' AND a.step = 1 AND a.xid < b.xid")).
		To(Equal("4"), "A must hold its transaction id before B begins, in every round")
	approveCutover(m.Name)
	completed := waitCompletedCapturing(m, captured)
	expectConditionTrue(completed, v1beta1.ConditionCutoverComplete)
	expectCoverageIdentical([]coverageCase{c}, identity, completed)
}

// overlapOnSource commits two source transactions that overlap: A writes, B
// writes and commits, then A writes again and commits. A polls for B's row
// and the spec waits for A's transaction id, so no scheduling can reorder them.
func overlapOnSource(table string, round int) {
	GinkgoHelper()
	const session = "e2e_overlap_a"
	row := func(side string, step int) string {
		return fmt.Sprintf("INSERT INTO %s VALUES ('%s', %d, %d, pg_current_xact_id()::text::bigint);\n",
			table, side, round, step)
	}
	a := startPSQLScript(sourceCluster, asSourceAppRole()+
		"SET application_name = '"+session+"';\nSET statement_timeout = '2min';\nBEGIN;\n"+row("A", 1)+
		fmt.Sprintf("DO $wait$ BEGIN WHILE NOT EXISTS (SELECT FROM %s WHERE side = 'B' AND round = %d) LOOP"+
			" PERFORM pg_sleep(0.05); END LOOP; END $wait$;\n", table, round)+
		row("A", 2)+"COMMIT;\n", 3*time.Minute)
	Eventually(func(g Gomega) {
		select {
		case err := <-a:
			StopTrying(fmt.Sprintf("transaction A of round %d ended before B began: %v", round, err)).Now()
		default:
		}
		g.Expect(psql(sourceCluster, "SELECT count(*) FROM pg_stat_activity WHERE application_name = '"+session+
			"' AND backend_xid IS NOT NULL")).To(Equal("1"))
	}, time.Minute, 100*time.Millisecond).Should(Succeed(), "transaction A of round %d did not write", round)
	psql(sourceCluster, asSourceAppRole()+"BEGIN; "+row("B", 1)+row("B", 2)+"COMMIT")
	Eventually(a, 2*time.Minute).Should(Receive(BeNil()), "transaction A of round %d did not commit after B", round)
}

// resentTransaction holds the apply on a target lock, so the slot's confirmed
// flush stays before a large transaction that receive already has, and then
// terminates the walsender. receive gets the transaction a second time.
func resentTransaction() {
	const bulkRows = 100000
	c := followCorrectnessCase("resend", "CREATE TABLE ${schema}.gate (id int PRIMARY KEY);\n"+
		"CREATE TABLE ${schema}.bulk (id int PRIMARY KEY, payload text NOT NULL);\n")
	m, identity, captured := startFollowCorrectness("e2e-resend-", c)
	schema := c.schemaNames(identity)[0]
	gate, bulk := schema+".gate", schema+".bulk"

	By("holding the apply on a target lock before the large transaction")
	release := holdTargetLock(gate)
	psql(sourceCluster, asSourceAppRole()+"INSERT INTO "+gate+" VALUES (1)")
	beforeBulk := psql(sourceCluster, "SELECT pg_current_wal_lsn()")
	psqlBulk(sourceCluster, asSourceAppRole()+fmt.Sprintf(
		"INSERT INTO %s SELECT g, repeat(md5(g::text), 2) FROM generate_series(1, %d) g", bulk, bulkRows))
	afterBulk := psql(sourceCluster, "SELECT pg_current_wal_lsn()")
	psql(sourceCluster, asSourceAppRole()+"INSERT INTO "+gate+" VALUES (2)")

	slot := pgcopydb.SlotName(m.Namespace, m.Name)
	sender := " FROM pg_replication_slots s JOIN pg_stat_replication r ON r.pid = s.active_pid" +
		" WHERE s.slot_name = '" + slot + "' AND s.database = current_database() AND s.active AND r.state = 'streaming'"
	By("waiting until receive has the whole large transaction while the slot still needs it")
	Eventually(func() string {
		return psql(sourceCluster, "SELECT coalesce(bool_and(r.write_lsn > '"+afterBulk+"'), false)"+sender)
	}, lagConvergeTimeout, time.Second).Should(Equal("t"), "receive did not report the commit after the large transaction")
	Expect(psql(sourceCluster, "SELECT s.confirmed_flush_lsn < '"+beforeBulk+"'"+sender)).To(Equal("t"),
		"confirmed flush passed the large transaction, so a reconnect would not send it again")

	By("terminating the migration's walsender")
	senderPID := "SELECT s.active_pid" + sender
	pid, err := slotSenderPID(psql(sourceCluster, senderPID))
	Expect(err).NotTo(HaveOccurred())
	Expect(signalSlotSender(primaryPod(sourceCluster), appDatabase(sourceCluster), senderPID, pid, "TERM")).To(Succeed())
	Eventually(func() string {
		return psql(sourceCluster, fmt.Sprintf("SELECT coalesce(bool_and(r.pid <> %d AND r.sent_lsn > '%s'), false)",
			pid, afterBulk)+sender)
	}, lagConvergeTimeout, time.Second).Should(Equal("t"), "no new walsender sent the large transaction again")

	By("releasing the apply and cutting over")
	release()
	waitOnTarget(m, "SELECT count(*) FROM "+gate, "2")
	completed := cutOverCoverageFollow(m, identity, captured)
	AddReportEntry("attempts after the walsender restart", completed.Status.Attempts)
	Expect(psql(targetCluster, "SELECT count(*) FROM "+bulk)).To(Equal(fmt.Sprint(bulkRows)))
	expectCoverageIdentical([]coverageCase{c}, identity, completed)
}

// failedCommitRetried refuses the first COMMIT of a change on the target with
// a deferred trigger only the target has. The retry must apply that change:
// a replication origin that moved with the aborted COMMIT would skip it.
func failedCommitRetried() {
	c := followCorrectnessCase("commit_fail", "CREATE TABLE ${schema}.t (id int PRIMARY KEY, v text NOT NULL);\n"+
		"INSERT INTO ${schema}.t VALUES (1, 'base');\n")
	m, identity, captured := startFollowCorrectness("e2e-commit-fail-", c)
	schema := c.schemaNames(identity)[0]
	asTargetApp := "SET ROLE " + sqlIdent(appRole(targetCluster)) + "; "

	By("refusing the first COMMIT that reaches the target table")
	// A sequence is not transactional, so the count survives the refused COMMIT.
	// The apply runs with session_replication_role=replica, which only fires ALWAYS triggers.
	psql(targetCluster, asTargetApp+"CREATE SEQUENCE "+schema+".reject_once; "+
		"CREATE FUNCTION "+schema+".reject_once() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN"+
		" IF nextval('"+schema+".reject_once') = 1 THEN RAISE EXCEPTION 'e2e refuses the first COMMIT of row %', NEW.id;"+
		" END IF; RETURN NULL; END $f$; "+
		"CREATE CONSTRAINT TRIGGER reject_once AFTER INSERT ON "+schema+".t DEFERRABLE INITIALLY DEFERRED"+
		" FOR EACH ROW EXECUTE FUNCTION "+schema+".reject_once(); "+
		"ALTER TABLE "+schema+".t ENABLE ALWAYS TRIGGER reject_once")
	before := &v1beta1.Migration{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), before)).To(Succeed())
	psql(sourceCluster, asSourceAppRole()+"INSERT INTO "+schema+".t VALUES (2, 'refused once')")
	Eventually(func(g Gomega) {
		current := &v1beta1.Migration{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), current)).To(Succeed())
		if current.Status.Phase == v1beta1.PhaseFailed {
			StopTrying("migration failed after the refused COMMIT: " + failureMessage(current)).Now()
		}
		g.Expect(psql(targetCluster, "SELECT is_called FROM "+schema+".reject_once")).To(Equal("t"),
			"the target has not refused a COMMIT yet")
		g.Expect(current.Status.Attempts).To(BeNumerically(">=", before.Status.Attempts+1),
			"the attempt whose COMMIT failed has not ended")
	}, migrationTimeout, time.Second).Should(Succeed())

	By("dropping the target-only objects and cutting over")
	psql(targetCluster, asTargetApp+"DROP TRIGGER reject_once ON "+schema+".t; DROP FUNCTION "+schema+".reject_once(); "+
		"DROP SEQUENCE "+schema+".reject_once")
	completed := cutOverCoverageFollow(m, identity, captured)
	AddReportEntry("attempts after the refused COMMIT", completed.Status.Attempts)
	Expect(psql(targetCluster, "SELECT count(*) FROM "+schema+".t WHERE id = 2")).To(Equal("1"),
		"the retry skipped the change whose COMMIT the target refused")
	expectCoverageIdentical([]coverageCase{c}, identity, completed)
}

// followCorrectnessCase is a one-schema follow case whose changes the spec
// makes itself, so it has no @follow and no file under coverage/.
func followCorrectnessCase(name, setup string) coverageCase {
	return coverageCase{
		name: name, path: "follow-correctness/" + name, group: coverageGroupFollow,
		expect: coverageIdentical, schemas: 1, minPG: 14, setup: setup,
	}
}

// startFollowCorrectness sets up c, starts its Manual-cutover follow Migration
// and waits until it is caught up at the gate.
func startFollowCorrectness(
	prefix string, c coverageCase,
) (*v1beta1.Migration, string, map[string]comparePodLog) {
	GinkgoHelper()
	identity := fmt.Sprint(time.Now().UnixNano())
	m, captured := prepareCoverageFollow(prefix+identity, []coverageCase{c}, identity)
	create(m)
	waitFollowStreaming(m.Name)
	waitPhase(m.Name, nsE2E, lagConvergeTimeout, v1beta1.PhaseCutoverPending)
	return m, identity, captured
}

// startPSQLScript feeds script to psql on cluster's primary in the background.
// The channel receives psql's error, nil on success, once psql exits.
func startPSQLScript(cluster, script string, timeout time.Duration) <-chan error {
	GinkgoHelper()
	argv := psqlArgv(cluster, primaryPod(cluster), appDatabase(cluster), true, "-XqtA", "-v", "ON_ERROR_STOP=1")
	done := make(chan error, 1)
	go func() {
		runCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_, err := runPSQLScript(exec.CommandContext(runCtx, "kubectl", argv...), script)
		done <- err
	}()
	return done
}

// holdTargetLock locks table on the target from a background session until
// the returned release runs. The spec's cleanup releases it as well, before
// the stamped schemas are dropped.
func holdTargetLock(table string) func() {
	GinkgoHelper()
	const session = "e2e_apply_hold"
	done := startPSQLScript(targetCluster, "SET application_name = '"+session+"';\nBEGIN;\n"+
		"LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE;\nSELECT pg_sleep(1200);\n", 25*time.Minute)
	held := true
	release := func() {
		if !held {
			return
		}
		held = false
		psql(targetCluster, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity"+
			" WHERE application_name = '"+session+"'")
		Eventually(done, time.Minute).Should(Receive(), "the lock session did not end")
	}
	DeferCleanup(release)
	Eventually(func(g Gomega) {
		select {
		case err := <-done:
			held = false
			StopTrying(fmt.Sprintf("the lock session ended before it held the lock: %v", err)).Now()
		default:
		}
		g.Expect(psql(targetCluster, "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid"+
			" WHERE a.application_name = '"+session+"' AND l.relation = '"+table+"'::regclass AND l.granted")).
			To(Equal("1"))
	}, time.Minute, 250*time.Millisecond).Should(Succeed())
	return release
}

// waitOnTarget waits until query returns want on the target, and stops early
// when m fails.
func waitOnTarget(m *v1beta1.Migration, query, want string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		current := &v1beta1.Migration{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), current)).To(Succeed())
		if current.Status.Phase == v1beta1.PhaseFailed {
			StopTrying(fmt.Sprintf("migration failed before the target returned %s for %q: %s",
				want, query, failureMessage(current))).Now()
		}
		out, err := psqlDBErr(targetCluster, appDatabase(targetCluster), query)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal(want), "the change has not reached the target")
	}, lagConvergeTimeout, time.Second).Should(Succeed())
}
