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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

var _ = Describe("Keepalive feedback", func() {
	DescribeTable("cuts over idle published tables after filtered WAL",
		keepaliveFeedbackCutover,
		Entry("Manual", v1beta1.CutoverManual),
		Entry("Automatic", v1beta1.CutoverAutomatic),
	)
})

func keepaliveFeedbackCutover(mode v1beta1.CutoverMode) {
	name := "e2e-keepalive-" + strings.ToLower(string(mode))
	const dataTable = "public.e2e_keepalive_data"
	const noiseTable = "public.e2e_keepalive_noise"
	const dataSQL = "SELECT string_agg(id::text || ':' || payload, ',' ORDER BY id) FROM " + dataTable
	// Do not scale this 32Mi payload: even small tiers must exceed the default 16Mi lag allowance.
	const noiseRows = 16384
	slot := pgcopydb.SlotName(nsE2E, name)
	slotCountSQL := "SELECT count(*) FROM pg_replication_slots WHERE slot_name='" + slot + "'"
	originCountSQL := "SELECT count(*) FROM pg_replication_origin WHERE roname='" + slot + "'"
	originSQL := "SELECT pg_replication_origin_progress(roname, true)::text " +
		"FROM pg_replication_origin WHERE roname='" + slot + "'"
	sourceFrom := " FROM pg_replication_slots s JOIN pg_stat_replication r ON r.pid=s.active_pid " +
		"WHERE s.slot_name='" + slot + "' AND s.database=current_database() " +
		"AND s.slot_type='logical' AND s.active AND r.state='streaming'"
	query := func(g Gomega, cluster, sql string) string {
		out, err := psqlDBErr(cluster, appDB, sql)
		// The helper's raw error can contain pod names and remote stderr.
		queried := err == nil
		g.Expect(queried).To(BeTrue(), "keepalive SQL probe failed on %s", cluster)
		return out
	}
	lsn := func(raw string) uint64 {
		position, err := sentinel.ParseLSN(raw)
		Expect(err).NotTo(HaveOccurred())
		Expect(position).To(BeNumerically(">", 0), "an initialized WAL position is required")
		return position
	}
	Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
	Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
	DeferCleanup(func() {
		deleteMigration(name)
		Eventually(func() string { return query(Default, sourceCluster, slotCountSQL) },
			3*time.Minute, time.Second).Should(Equal("0"))
		Eventually(func() string { return query(Default, targetCluster, originCountSQL) },
			3*time.Minute, time.Second).Should(Equal("0"))
		for _, cluster := range []string{sourceCluster, targetCluster} {
			query(Default, cluster, "DROP TABLE IF EXISTS "+dataTable+", "+noiseTable)
		}
	})

	By("cloning an app-owned table while Manual mode holds the cutover gate")
	query(Default, sourceCluster, "SET ROLE app; CREATE TABLE "+dataTable+
		" (id integer PRIMARY KEY, payload text NOT NULL); INSERT INTO "+dataTable+
		" VALUES (1, 'seed'), (2, 'seed'); CREATE TABLE "+noiseTable+
		" (id integer PRIMARY KEY, payload text NOT NULL); ALTER TABLE "+noiseTable+
		" ALTER COLUMN payload SET STORAGE EXTERNAL")
	Expect(query(Default, sourceCluster, "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='"+
		dataTable+"'::regclass")).To(Equal(appDB))
	mig := newFollowMigration(name, v1beta1.CutoverManual)
	mig.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlyTables: []string{dataTable}}
	Expect(mig.Spec.Follow.MaxCatchupLag).To(BeNil(), "exercise the admitted default, not a test allowance")
	create(mig)
	m := waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)
	expectConditionTrue(m, v1beta1.ConditionCloneCompleted)
	Expect(m.Spec.Follow.MaxCatchupLag).NotTo(BeNil())
	threshold := m.Spec.Follow.MaxCatchupLag.Value()
	Expect(threshold).To(Equal(int64(16<<20)), "the regression must use the default catch-up threshold")
	publicationSQL := "SELECT string_agg(schemaname || '.' || tablename, ',' ORDER BY schemaname, tablename) " +
		"FROM pg_publication_tables WHERE pubname='" + slot + "'"
	Expect(query(Default, sourceCluster, publicationSQL)).To(Equal(dataTable),
		"the exact nonempty publication inventory must exclude the WAL generator")
	Expect(query(Default, targetCluster, dataSQL)).To(Equal("1:seed,2:seed"))
	Expect(query(Default, targetCluster, "SELECT to_regclass('"+noiseTable+"') IS NULL")).To(Equal("t"))

	By("establishing a durable apply cursor, then stopping all published-table writes")
	query(Default, sourceCluster, "SET ROLE app; UPDATE "+dataTable+" SET payload='applied' WHERE id=1")
	frozenData := query(Default, sourceCluster, dataSQL)
	Expect(frozenData).To(Equal("1:applied,2:seed"))
	Eventually(func(g Gomega) {
		g.Expect(query(g, targetCluster, dataSQL)).To(Equal(frozenData))
	}, lagConvergeTimeout, time.Second).Should(Succeed())
	origin := query(Default, targetCluster, originSQL)
	originPosition := lsn(origin)
	readMigration := func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m)).To(Succeed())
		g.Expect(m.Status.Phase).NotTo(Equal(v1beta1.PhaseFailed), failureMessage(m))
		g.Expect(m.Status.Replication).NotTo(BeNil())
	}
	unchangedData := func(g Gomega) {
		for _, cluster := range []string{sourceCluster, targetCluster} {
			g.Expect(query(g, cluster, dataSQL)).To(Equal(frozenData))
		}
		g.Expect(query(g, sourceCluster, publicationSQL)).To(Equal(dataTable))
		g.Expect(query(g, targetCluster, originSQL)).To(Equal(origin),
			"certified network feedback must not move the durable data cursor")
	}
	generateFilteredWAL := func(batch int) string {
		before := lsn(query(Default, sourceCluster, "SELECT pg_current_wal_lsn()::text"))
		// EXTERNAL prevents compression from shrinking this below maxCatchupLag.
		query(Default, sourceCluster, fmt.Sprintf("SET ROLE app; SET synchronous_commit=on; "+
			"INSERT INTO %s SELECT g, repeat(md5(g::text), 64) FROM generate_series(%d, %d) g",
			noiseTable, (batch-1)*noiseRows+1, batch*noiseRows))
		boundary := query(Default, sourceCluster, "SELECT pg_current_wal_lsn()::text")
		position := lsn(boundary)
		Expect(position).To(BeNumerically(">", before+uint64(threshold)))
		Expect(position).To(BeNumerically(">", originPosition+uint64(threshold)))
		Expect(query(Default, sourceCluster, "SELECT count(*) FROM "+noiseTable)).
			To(Equal(strconv.Itoa(batch * noiseRows)))
		return boundary
	}

	By("requiring real source feedback past filtered WAL before any cutover or endpos nudge")
	boundary := generateFilteredWAL(1)
	feedbackSQL := "SELECT coalesce(r.replay_lsn >= '" + boundary + "'::pg_lsn AND r.flush_lsn >= '" +
		boundary + "'::pg_lsn AND s.confirmed_flush_lsn >= '" + boundary + "'::pg_lsn, false)" + sourceFrom
	Eventually(func(g Gomega) {
		readMigration(g)
		g.Expect(m.Spec.Cutover).To(Equal(v1beta1.CutoverSpec{Mode: v1beta1.CutoverManual}))
		g.Expect(m.Status.Replication.Endpos).To(BeEmpty())
		unchangedData(g)
		g.Expect(query(g, sourceCluster, feedbackSQL)).To(Equal("t"),
			"replay, flush, and confirmed_flush must cross the captured filtered-WAL boundary")
	}, lagConvergeTimeout, 5*time.Second).Should(Succeed())
	m = waitPhase(name, nsE2E, lagConvergeTimeout, v1beta1.PhaseCutoverPending)
	expectConditionTrue(m, v1beta1.ConditionCaughtUp)
	unchangedData(Default)

	By("holding only this slot's sender while a second filtered-WAL gap clears CaughtUp")
	senderSQL := "SELECT s.active_pid" + sourceFrom
	pid, err := slotSenderPID(query(Default, sourceCluster, senderSQL))
	Expect(err).NotTo(HaveOccurred())
	pod := primaryPod(sourceCluster)
	signalSender := func(signal string) {
		signalErr := signalSlotSender(pod, appDB, senderSQL, pid, signal)
		signaled := signalErr == nil
		Expect(signaled).To(BeTrue(), "could not %s the test-owned walsender", signal)
	}
	paused := true
	DeferCleanup(func() {
		if paused {
			signalSender("CONT")
		}
	})
	signalSender("STOP")
	boundary = generateFilteredWAL(2)
	boundaryPosition := lsn(boundary)
	lagging := func(g Gomega) {
		readMigration(g)
		g.Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
		caughtUp := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCaughtUp)
		g.Expect(caughtUp).NotTo(BeNil())
		g.Expect(caughtUp.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(caughtUp.Reason).To(Equal("Lagging"))
		g.Expect(m.Status.Replication.LagBytes).NotTo(BeNil())
		g.Expect(*m.Status.Replication.LagBytes).To(BeNumerically(">", threshold))
		g.Expect(m.Status.Replication.Endpos).To(BeEmpty())
	}
	Eventually(lagging, lagConvergeTimeout, time.Second).Should(Succeed())
	unchangedData(Default)

	By("arming cutover through the spec while catch-up remains blocked")
	if mode == v1beta1.CutoverManual {
		approveCutover(name)
	} else {
		// cutover.mode is mutable; no worker restart or status edit is needed.
		patch := client.MergeFrom(m.DeepCopy())
		m.Spec.Cutover.Mode = mode
		Expect(k8sClient.Patch(ctx, m, patch)).To(Succeed())
	}
	Consistently(func(g Gomega) {
		lagging(g)
		g.Expect(m.Spec.Cutover.Mode).To(Equal(mode))
		g.Expect(m.Spec.Cutover.Approved).To(Equal(mode == v1beta1.CutoverManual))
	}, 25*time.Second, time.Second).Should(Succeed())

	By("resuming with no published writes and requiring cutover, verified drain, and cleanup")
	signalSender("CONT")
	paused = false
	m = waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
	expectSingleAttempt(m)
	expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
	expectConditionTrue(m, v1beta1.ConditionComplete)
	expectCleanupSucceeded(name)
	Expect(m.Spec.Cutover.Mode).To(Equal(mode))
	Expect(m.Spec.Cutover.Approved).To(Equal(mode == v1beta1.CutoverManual))
	Expect(m.Spec.Follow.MaxCatchupLag.Value()).To(Equal(threshold))
	Expect(m.Status.Replication).NotTo(BeNil())
	Expect(lsn(m.Status.Replication.Endpos)).To(BeNumerically(">=", boundaryPosition))
	for _, cluster := range []string{sourceCluster, targetCluster} {
		Expect(query(Default, cluster, dataSQL)).To(Equal(frozenData))
	}
	Expect(query(Default, sourceCluster, slotCountSQL)).To(Equal("0"))
	Expect(query(Default, sourceCluster, "SELECT count(*) FROM pg_publication WHERE pubname='"+slot+"'")).
		To(Equal("0"))
	Expect(query(Default, targetCluster, originCountSQL)).To(Equal("0"))
}
