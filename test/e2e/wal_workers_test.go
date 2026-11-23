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
	"regexp"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

const (
	walWorkersCount = 3
	walWorkersHot   = "public.orders"
	walWorkersMark  = "wal-workers"
)

// walWorkersTables are the tables the writer changes in every transaction: customers and orders
// are the fixture's FK parent and child, and the high-traffic orders gets a worker of its own.
var walWorkersTables = []string{"customers", walWorkersHot, "audit.events"}

var _ = Describe("WAL workers", SpecPriority(3), func() {
	BeforeEach(resetMigrationPair)

	It("streams through three WAL workers and cuts over with every origin at endpos and every foreign key valid",
		walWorkersCutover)
})

func walWorkersCutover() {
	const name = "e2e-wal-workers"
	if pgSource < 13 {
		Skip(fmt.Sprintf("WAL workers need a PostgreSQL 13 source, this pair has %d", pgSource))
	}
	// Manual first, so the writer stops before the freeze; the spec then arms Automatic.
	mig := newFollowMigration(name, v1beta1.CutoverManual)
	mig.Spec.Follow.WalWorkers = walWorkersCount
	mig.Spec.Follow.HighTrafficTables = []string{walWorkersHot}
	mig.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
	slot := pgcopydb.EffectiveSlotName(&mig.Spec, nsE2E, name)
	slots := pgcopydb.WorkerSlots(slot, walWorkersCount)
	DeferCleanup(func() { deleteMigration(name) })

	By("writing parent, child and a third table in each source transaction, starting before the base copy")
	w := startLiveWriterOf(walWorkersMark, walWorkersStatement)
	DeferCleanup(func() {
		_, _ = w.stop()
		if CurrentSpecReport().Failed() {
			AddReportEntry("live writer after stop", w.diagnostics().String(), ReportEntryVisibilityFailureOrVerbose)
		}
		like := sqlLiteral(walWorkersMark + "-%")
		psql(sourceCluster, "DELETE FROM orders WHERE note LIKE "+like+"; DELETE FROM customers WHERE name LIKE "+
			like+"; DELETE FROM audit.events WHERE payload->>'note' LIKE "+like)
	})
	create(mig)

	By("passing the preflight, or skipping when the runner pgcopydb lacks " + pgcopydb.WalWorkersFlag)
	m := &v1beta1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m)).To(Succeed())
		g.Expect(m.Status.Phase == v1beta1.PhaseFailed || m.Status.Attempts >= 1).To(BeTrue(),
			"%s is at phase %q with no worker attempt", name, m.Status.Phase)
	}, workerRunningTimeout, time.Second).Should(Succeed())
	if m.Status.Phase == v1beta1.PhaseFailed {
		msg := failureMessage(m)
		if strings.Contains(msg, "does not list "+pgcopydb.WalWorkersFlag) {
			// A release run whose pin carries WAL workers must never land here.
			skipped := "WAL WORKERS NOT TESTED: the pgcopydb in runner " + runnerTag + " lacks " + pgcopydb.WalWorkersFlag
			AddReportEntry("WAL workers skipped", skipped)
			_, _ = fmt.Fprintln(GinkgoWriter, skipped)
			Skip(skipped)
		}
		Fail("migration " + name + " failed before its first attempt: " + msg)
	}
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, job)).To(Succeed())
	env := job.Spec.Template.Spec.Containers[0].Env
	Expect(env).To(ContainElement(corev1.EnvVar{Name: "PGCOPYDB_WAL_WORKERS", Value: fmt.Sprint(walWorkersCount)}))
	Expect(env).To(ContainElement(corev1.EnvVar{Name: "PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES", Value: walWorkersHot}))

	By("streaming over exactly three slots and one publication per table")
	waitFollowStreaming(name)
	wantSlots := make([]string, 0, len(slots))
	for _, s := range slots {
		wantSlots = append(wantSlots, s+":pgoutput")
	}
	Expect(sourceSlotCount()).To(Equal(fmt.Sprint(walWorkersCount)), "pgcopydb slots on the source")
	Expect(psql(sourceCluster, "SELECT coalesce(string_agg(slot_name || ':' || plugin, ',' ORDER BY slot_name), '')"+
		" FROM pg_replication_slots WHERE slot_name ~ "+sqlLiteral(walWorkerNamesRE(slot)))).
		To(Equal(strings.Join(wantSlots, ",")))
	pubs := strings.Split(psql(sourceCluster, walWorkerPublicationsSQL(slot, walWorkersTables)), "|")
	Expect(pubs).To(HaveLen(4))
	Expect(pubs[0]).To(Equal("0"), "a publication named %s exists beside the per-table ones", slot)
	Expect(pubs[1]).NotTo(Equal("0"), "no publication named %s_t<oid>", slot)
	Expect(pubs[2]).To(Equal(pubs[1]), "publications named %s_t<oid> that publish exactly that table", slot)
	Expect(pubs[3]).To(Equal("0"), "written tables without their own publication")
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m)).To(Succeed())
		g.Expect(m.Status.Replication).NotTo(BeNil(), "status.replication not populated yet")
		got := make([]string, 0, len(m.Status.Replication.Workers))
		for _, worker := range m.Status.Replication.Workers {
			got = append(got, worker.SlotName)
		}
		g.Expect(got).To(ConsistOf(slots), "status.replication.workers")
	}, 3*time.Minute, 2*time.Second).Should(Succeed())

	By("stopping the writer and arming Automatic cutover")
	last, err := w.stop()
	Expect(publicationRetryProbeError(err)).NotTo(HaveOccurred(),
		"the live writer failed; see the live writer diagnostic report for final marker availability")
	Expect(last).To(BeNumerically(">", 100),
		"the writer committed only %d transactions, too few to exercise WAL workers under load", last)
	rows := walWorkersRowsSQL(walWorkersMark)
	srcRows := psql(sourceCluster, rows)
	Expect(srcRows).To(Equal(fmt.Sprintf("%d|%d|%d", last, last, last)), "customers|orders|audit.events on the source")
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m)).To(Succeed())
	patch := client.MergeFrom(m.DeepCopy())
	m.Spec.Cutover.Mode = v1beta1.CutoverAutomatic
	Expect(k8sClient.Patch(ctx, m, patch)).To(Succeed())

	By("completing with every worker origin at endpos and a clean data compare")
	m = waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
	expectSingleAttempt(m)
	expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
	expectConditionTrue(m, v1beta1.ConditionVerified)
	expectCleanupSucceeded(name)
	expectFinalProgress(m, v1beta1.VerificationProgressData, name+"-compare-"+compareDataCheck)
	Expect(m.Status.Replication).NotTo(BeNil())
	endpos, err := sentinel.ParseLSN(m.Status.Replication.Endpos)
	Expect(err).NotTo(HaveOccurred(), "status.replication.endpos %q", m.Status.Replication.Endpos)
	verifyLog := jobLogs(name+"-verify", verifyLogLines)
	Expect(verifyLog).To(MatchRegexp(`drain verified: origin progress equals endpos \S+ on all %d WAL worker origins`,
		walWorkersCount), "the verify Job did not take the exact-LSN path:\n%s", logTail(verifyLog, 40))
	expectDrainProgress(m)
	origins := walWorkerOriginProgress(verifyLog)
	Expect(origins).To(HaveLen(walWorkersCount), "origins the verify Job measured:\n%s", logTail(verifyLog, 40))
	for _, s := range slots {
		Expect(origins).To(HaveKey(s))
		progress, parseErr := sentinel.ParseLSN(origins[s])
		Expect(parseErr).NotTo(HaveOccurred(), "origin %s progress %q", s, origins[s])
		Expect(progress).To(Equal(endpos), "origin %s at %s, endpos %s", s, origins[s], m.Status.Replication.Endpos)
	}

	By("finding every source foreign key on the target, validated, and every written row")
	fks := psql(sourceCluster, walWorkersForeignKeysSQL)
	Expect(fks).To(ContainSubstring("orders.orders_customer_id_fkey"))
	Expect(psql(targetCluster, walWorkersForeignKeysSQL)).To(Equal(fks))
	Expect(psql(targetCluster, "SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND NOT convalidated")).
		To(Equal("0"), "foreign keys left NOT VALID on the target")
	Expect(psql(targetCluster, rows)).To(Equal(srcRows), "customers|orders|audit.events on the target")
	Expect(sequenceValues(targetCluster)).To(Equal(sequenceValues(sourceCluster)))

	By("leaving no slot, publication or origin of any worker on either side")
	for _, cluster := range []string{sourceCluster, targetCluster} {
		Eventually(func() string { return psql(cluster, walWorkerLeftoversSQL(slot)) }, 3*time.Minute, 2*time.Second).
			Should(Equal("0|0|0"), "slots|publications|origins named %s, %s_w<k> or %s_t<oid> on %s",
				slot, slot, slot, cluster)
	}
}

// walWorkersStatement adds a customer, its order and an event in one transaction, so a source
// transaction spans three WAL workers and the child can reach the target before its parent.
func walWorkersStatement(marker string, n int) string {
	note := sqlLiteral(fmt.Sprintf("%s-%d", marker, n))
	return "BEGIN; WITH c AS (INSERT INTO customers (name) VALUES (" + note + ") RETURNING id)" +
		" INSERT INTO orders (customer_id, amount, note) SELECT id, 1.00, " + note + " FROM c;" +
		" INSERT INTO audit.events (payload) VALUES (jsonb_build_object('note', " + note + ")); COMMIT;\n"
}

// walWorkersRowsSQL prints customers|orders|events written for marker; an order counts only
// beside its own customer.
func walWorkersRowsSQL(marker string) string {
	like := sqlLiteral(marker + "-%")
	return "SELECT (SELECT count(*) FROM customers WHERE name LIKE " + like + ")" +
		" || '|' || (SELECT count(*) FROM orders o JOIN customers c ON c.id = o.customer_id" +
		" WHERE o.note LIKE " + like + " AND c.name = o.note)" +
		" || '|' || (SELECT count(*) FROM audit.events WHERE payload->>'note' LIKE " + like + ")"
}

// walWorkersForeignKeysSQL lists every foreign key in the database as table.constraint.
const walWorkersForeignKeysSQL = "SELECT coalesce(string_agg(fk, ',' ORDER BY fk), '') FROM" +
	" (SELECT conrelid::regclass::text || '.' || conname AS fk FROM pg_constraint WHERE contype = 'f') f"

// walWorkerNamesRE matches slot and the names pgcopydb derives from it. Generated slot names
// hold only [a-z0-9_], so slot needs no escaping.
func walWorkerNamesRE(slot string) string {
	return "^" + slot + "(_[wt][0-9]+)?$"
}

// walWorkerPublicationsSQL prints named|perTable|exact|unpublished: publications named slot, named
// slot_t<oid>, named slot_t<oid> and publishing only that table, and tables without such a publication.
func walWorkerPublicationsSQL(slot string, tables []string) string {
	perTable := sqlLiteral("^" + slot + "_t[0-9]+$")
	return "SELECT count(*) FILTER (WHERE p.pubname = " + sqlLiteral(slot) + ")" +
		" || '|' || count(*) FILTER (WHERE p.pubname ~ " + perTable + ")" +
		" || '|' || count(*) FILTER (WHERE p.pubname ~ " + perTable +
		" AND ARRAY(SELECT r.prrelid FROM pg_publication_rel r WHERE r.prpubid = p.oid)" +
		" = ARRAY[substring(p.pubname FROM '[0-9]+$')::oid])" +
		" || '|' || (SELECT count(*) FROM unnest(" + sqlLiteral("{"+strings.Join(tables, ",")+"}") + "::regclass[]) t" +
		" WHERE NOT EXISTS (SELECT FROM pg_publication q WHERE q.pubname = " + sqlLiteral(slot+"_t") + " || t::oid))" +
		" FROM pg_publication p"
}

// walWorkerLeftoversSQL prints slots|publications|origins named slot, slot_w<k> or slot_t<oid>.
func walWorkerLeftoversSQL(slot string) string {
	re := sqlLiteral(walWorkerNamesRE(slot))
	return "SELECT (SELECT count(*) FROM pg_replication_slots WHERE slot_name ~ " + re + ")" +
		" || '|' || (SELECT count(*) FROM pg_publication WHERE pubname ~ " + re + ")" +
		" || '|' || (SELECT count(*) FROM pg_replication_origin WHERE roname ~ " + re + ")"
}

// walWorkerOriginLine is one origin of the verify Job's WAL worker loop.
var walWorkerOriginLine = regexp.MustCompile(`(?m)origin_progress=(\S+) origin_gap_bytes=\S+ origin=(\S+)$`)

// walWorkerOriginProgress maps each origin the verify Job measured to its progress.
func walWorkerOriginProgress(log string) map[string]string {
	origins := map[string]string{}
	for _, match := range walWorkerOriginLine.FindAllStringSubmatch(log, -1) {
		origins[match[2]] = match[1]
	}
	return origins
}

func TestWalWorkerOriginProgress(t *testing.T) {
	log := "endpos=0/5000110 replay_lsn=0/5000110 origin_progress=0/5000110 origin_gap_bytes=0 origin=s\n" +
		"endpos=0/5000110 replay_lsn=0/5000110 origin_progress=0/4FFFF00 origin_gap_bytes=528 origin=s_w1\n" +
		"endpos=0/5000110 replay_lsn=0/5000110 origin_progress=0/5000110 origin_gap_bytes=0\n" +
		"drain verified: origin progress equals endpos 0/5000110 on all 2 WAL worker origins, nothing left to apply\n"
	got := walWorkerOriginProgress(log)
	want := map[string]string{"s": "0/5000110", "s_w1": "0/4FFFF00"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("origins %v, want %v", got, want)
	}
}

// TestWalWorkersSQL runs the spec's name checks on a local server: per-table publications count
// only under their own table's oid, and the leftover sweep ignores names outside slot's pattern.
func TestWalWorkersSQL(t *testing.T) {
	run := testPSQL(t)
	slot := fmt.Sprintf("e2e_wal_workers_%d", time.Now().UnixNano())
	ns := sqlIdent(slot)
	testCoverageSchemas(t, run, slot)
	testQuery(t, run, "SET ROLE "+testCaseOwner+";\nCREATE TABLE "+ns+".a (id int PRIMARY KEY);\n"+
		"CREATE TABLE "+ns+".b (id int PRIMARY KEY)")
	tables := []string{slot + ".a", slot + ".b"}
	oidA := testQuery(t, run, "SELECT "+sqlLiteral(tables[0])+"::regclass::oid")
	oidB := testQuery(t, run, "SELECT "+sqlLiteral(tables[1])+"::regclass::oid")
	pubs := []string{slot, slot + "_t" + oidA, slot + "_t" + oidB, slot + "_t1", slot + "_x", slot + "_w1x"}
	t.Cleanup(func() {
		for _, p := range pubs {
			testQuery(t, run, "DROP PUBLICATION IF EXISTS "+sqlIdent(p))
		}
		testQuery(t, run, "DO $drop$ BEGIN PERFORM pg_replication_origin_drop(roname) FROM pg_replication_origin"+
			" WHERE roname = "+sqlLiteral(slot+"_w1")+"; END $drop$")
	})
	check := func(query, want, what string) {
		t.Helper()
		if got := testQuery(t, run, query); got != want {
			t.Fatalf("%s %q, want %q", what, got, want)
		}
	}
	publications, leftovers := walWorkerPublicationsSQL(slot, tables), walWorkerLeftoversSQL(slot)
	check(publications, "0|0|0|2", "publications before any exists")
	check(leftovers, "0|0|0", "leftovers before any exists")

	testQuery(t, run, "CREATE PUBLICATION "+sqlIdent(pubs[1])+" FOR TABLE "+ns+".a;\n"+
		"CREATE PUBLICATION "+sqlIdent(pubs[2])+" FOR TABLE "+ns+".b")
	check(publications, "0|2|2|0", "publications with one per table")

	// The base name, a suffix naming no table of its own, and two names outside the pattern.
	for _, p := range pubs[3:] {
		testQuery(t, run, "CREATE PUBLICATION "+sqlIdent(p)+" FOR TABLE "+ns+".a")
	}
	testQuery(t, run, "CREATE PUBLICATION "+sqlIdent(pubs[0])+" FOR TABLE "+ns+".a")
	check(publications, "1|3|2|0", "publications with the base name and a mismatched oid")
	check(leftovers, "0|4|0", "leftovers with four matching publications")
	testQuery(t, run, "SELECT FROM pg_replication_origin_create("+sqlLiteral(slot+"_w1")+")")
	check(leftovers, "0|4|1", "leftovers with a worker origin")
}
