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
	"os/exec"
	"regexp"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

const (
	publicationRetryOwnerPattern = `^[0-9]+\|app\|0\|`
	publicationRetryPubCount     = "SELECT count(*) FROM pg_publication WHERE pubname='"
	publicationRetrySlotCount    = "SELECT count(*) FROM pg_replication_slots WHERE slot_name='"
	publicationRetryInsert       = "SET ROLE app; INSERT INTO "
)

var _ = Describe("Automatic publication retries", func() {
	It("preserves an established publication through a deliberate suspend and resume", func() {
		mig, table, slot := publicationRetryFixture("e2e-publication-established")
		create(mig)
		waitFollowStreaming(mig.Name)
		first := waitPhase(mig.Name, nsE2E, lagConvergeTimeout, v1beta1.PhaseCutoverPending)
		expectSingleAttempt(first)
		expectPublicationRetrySlotActive(slot)
		publication := psql(sourceCluster, publicationRetryStateSQL(slot))
		Expect(publication).To(MatchRegexp(publicationRetryOwnerPattern + regexp.QuoteMeta(table) + `$`))

		By("proving a unique post-clone marker was applied before stopping the worker")
		marker := string(mig.UID)
		psql(sourceCluster, publicationRetryInsert+table+" VALUES (1, '"+marker+"-before-stop')")
		before := "0|baseline\n1|" + marker + "-before-stop"
		expectPublicationRetryRows(table, before)
		work := &corev1.PersistentVolumeClaim{}
		key := client.ObjectKey{Namespace: nsE2E, Name: mig.Name + "-work"}
		Expect(k8sClient.Get(ctx, key, work)).To(Succeed())
		workUID := work.UID
		Expect(workUID).NotTo(BeEmpty())
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: first.Status.JobName}, job)).To(Succeed())
		image := job.Spec.Template.Spec.Containers[0].Image
		Expect(image).NotTo(BeEmpty())

		By("suspending only the test's Migration and waiting for its worker to stop")
		setSuspend(mig.Name, true)
		waitPhase(mig.Name, nsE2E, migrationTimeout, v1beta1.PhaseSuspended)
		Eventually(func(g Gomega) {
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: first.Status.JobName}, &batchv1.Job{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the first worker Job must finish foreground deletion")
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
				client.MatchingLabels{batchv1.JobNameLabel: first.Status.JobName})).To(Succeed())
			count := len(pods.Items)
			g.Expect(count).To(BeZero(), "the first worker's pods must be gone before resuming")
		}, 2*time.Minute, time.Second).Should(Succeed())
		Expect(k8sClient.Get(ctx, key, work)).To(Succeed())
		Expect(work.UID).To(Equal(workUID))
		Eventually(func() (string, error) {
			return psqlDBErr(sourceCluster, appDB, publicationRetrySlotCount+slot+"' AND NOT active")
		}, time.Minute, time.Second).Should(Equal("1"))
		Expect(psql(sourceCluster, publicationRetryStateSQL(slot))).To(Equal(publication))
		psql(sourceCluster, publicationRetryInsert+table+" VALUES (2, '"+marker+"-suspended')")
		Expect(psql(targetCluster, publicationRetryRowsSQL(table))).To(Equal(before))

		By("resuming the same work volume and proving delivery across the retry")
		setSuspend(mig.Name, false)
		retry := expectPublicationRetryAttempt(mig.Name)
		Expect(retry.Spec.Template.Spec.Containers[0].Image).To(Equal(image))
		expectPublicationRetrySlotActive(slot)
		Expect(psql(sourceCluster, publicationRetryStateSQL(slot))).To(Equal(publication),
			"the retry must preserve the publication OID, owner, and table membership")
		psql(sourceCluster, publicationRetryInsert+table+" VALUES (3, '"+marker+"-resumed')")
		want := before + "\n2|" + marker + "-suspended\n3|" + marker + "-resumed"
		expectPublicationRetryRows(table, want)
		Expect(psql(sourceCluster, publicationRetryStateSQL(slot))).To(Equal(publication))
		completePublicationRetry(mig.Name, table, slot, want)
	})

	It("repairs an early orphan after the first worker really fails without a slot", func() {
		mig, table, slot := publicationRetryFixture("e2e-publication-orphan")
		other := strings.TrimPrefix(table, "public.") + "_other"
		Expect(psql(sourceCluster, publicationRetryPubCount+other+"'")).To(Equal("0"))
		DeferCleanup(func() { psql(sourceCluster, `DROP PUBLICATION IF EXISTS "`+other+`"`) })
		psql(sourceCluster, `SET ROLE app; CREATE PUBLICATION "`+slot+`"; CREATE PUBLICATION "`+
			other+`" FOR TABLE `+table)
		orphan := psql(sourceCluster, publicationRetryStateSQL(slot))
		Expect(orphan).To(MatchRegexp(publicationRetryOwnerPattern+`$`), "the orphan must be empty and app-owned")
		unrelated := psql(sourceCluster, publicationRetryStateSQL(other))
		Expect(unrelated).To(MatchRegexp(publicationRetryOwnerPattern + regexp.QuoteMeta(table) + `$`))
		Expect(psql(sourceCluster, publicationRetrySlotCount+slot+"'")).To(Equal("0"))

		By("letting the real first worker encounter the duplicate publication")
		create(mig)
		Eventually(func(g Gomega) {
			job := &batchv1.Job{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: mig.Name + "-run-1"}, job)).To(Succeed())
			failed := false
			for _, c := range job.Status.Conditions {
				if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
					failed = true
				}
			}
			g.Expect(failed).To(BeTrue(), "attempt 1 must fail, not be patched or deleted by the test")
		}, migrationTimeout, time.Second).Should(Succeed())
		logCtx, cancel := context.WithTimeout(ctx, e2eCommandTimeout)
		defer cancel()
		out, err := commandOutput(logCtx, exec.CommandContext, "kubectl", "logs", "-n", nsE2E,
			"job/"+mig.Name+"-run-1", "-c", "pgcopydb")
		Expect(err).NotTo(HaveOccurred())
		duplicate := strings.Contains(string(out), `publication \"`+slot+`\" already exists`) ||
			strings.Contains(string(out), `publication "`+slot+`" already exists`)
		Expect(duplicate).To(BeTrue(), "attempt 1 must report its duplicate-publication failure")

		By("requiring the retry to recreate membership without touching another publication")
		expectPublicationRetryAttempt(mig.Name)
		expectPublicationRetrySlotActive(slot)
		repaired := psql(sourceCluster, publicationRetryStateSQL(slot))
		Expect(repaired).To(MatchRegexp(publicationRetryOwnerPattern + regexp.QuoteMeta(table) + `$`))
		Expect(strings.Split(repaired, "|")[0]).NotTo(Equal(strings.Split(orphan, "|")[0]),
			"the orphan must be replaced, not reused as an empty publication")
		Expect(psql(sourceCluster, publicationRetryStateSQL(other))).To(Equal(unrelated))
		marker := string(mig.UID) + "-after-repair"
		psql(sourceCluster, publicationRetryInsert+table+" VALUES (1, '"+marker+"')")
		want := "0|baseline\n1|" + marker
		expectPublicationRetryRows(table, want)
		completePublicationRetry(mig.Name, table, slot, want)
		Expect(psql(sourceCluster, publicationRetryStateSQL(other))).To(Equal(unrelated),
			"worker retry and cleanup must leave the unrelated publication intact")
	})
})

func publicationRetryFixture(name string) (*v1beta1.Migration, string, string) {
	GinkgoHelper()
	mig := newFollowMigration(name, v1beta1.CutoverManual)
	mig.Spec.BackoffLimit = 1
	table := "public." + strings.ReplaceAll(name, "-", "_")
	slot := pgcopydb.SlotName(mig.Namespace, mig.Name)
	// The pinned compare-data path reuses fetched TABLE_DATA from this workdir;
	// it needs no repeated --filters (copydb_schema.c and catalog.c).
	mig.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlyTables: []string{table}}
	Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
	Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
	for _, cluster := range []string{sourceCluster, targetCluster} {
		Expect(psql(cluster, "SELECT to_regclass('"+table+"') IS NULL")).To(Equal("t"))
	}
	Expect(psql(sourceCluster, publicationRetryPubCount+slot+"'")).To(Equal("0"))
	DeferCleanup(func() {
		psql(sourceCluster, `DROP PUBLICATION IF EXISTS "`+slot+`"`)
		for _, cluster := range []string{sourceCluster, targetCluster} {
			psql(cluster, "DROP TABLE IF EXISTS "+table)
		}
	})
	DeferCleanup(func() {
		if mig.UID != "" {
			deleteMigration(mig.Name)
		}
		Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
	})
	psql(sourceCluster, "SET ROLE app; CREATE TABLE "+table+
		" (id integer PRIMARY KEY, marker text NOT NULL); INSERT INTO "+table+" VALUES (0, 'baseline')")
	Expect(psql(sourceCluster, "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='"+
		table+"'::regclass")).To(Equal(appDB), "the migration role must own the follow fixture")
	return mig, table, slot
}

func publicationRetryStateSQL(publication string) string {
	return "SELECT p.oid::text || '|' || pg_get_userbyid(p.pubowner) || '|' || p.puballtables::int || '|' || " +
		"COALESCE(string_agg(t.schemaname || '.' || t.tablename, ',' ORDER BY t.schemaname, t.tablename), '') " +
		"FROM pg_publication p LEFT JOIN pg_publication_tables t ON t.pubname=p.pubname " +
		"WHERE p.pubname='" + publication + "' GROUP BY p.oid, p.pubowner, p.puballtables"
}

func expectPublicationRetryRows(table, want string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		out, err := psqlDBErr(targetCluster, appDB, publicationRetryRowsSQL(table))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal(want), "exact marker rows must arrive before cutover")
	}, lagConvergeTimeout, time.Second).Should(Succeed())
}

func publicationRetryRowsSQL(table string) string {
	return "SELECT id, marker FROM " + table + " ORDER BY id"
}

func expectPublicationRetrySlotActive(slot string) {
	GinkgoHelper()
	Eventually(func() (string, error) {
		return psqlDBErr(sourceCluster, appDB, publicationRetrySlotCount+slot+
			"' AND database=current_database() AND slot_type='logical' AND plugin='pgoutput' "+
			"AND active AND confirmed_flush_lsn IS NOT NULL")
	}, lagConvergeTimeout, time.Second).Should(Equal("1"))
}

func expectPublicationRetryAttempt(name string) *batchv1.Job {
	GinkgoHelper()
	m := waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)
	Expect(m.Status.Attempts).To(Equal(int32(2)))
	Expect(m.Spec.Cutover.Approved).To(BeFalse())
	Expect(m.Status.JobName).To(Equal(name + "-run-2"))
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: m.Status.JobName}, job)).To(Succeed())
	Expect(job.Spec.Template.Spec.Containers).To(HaveLen(1))
	Expect(job.Spec.Template.Spec.Containers[0].Args).To(ContainElements("--resume", "--not-consistent"))
	return job
}

func completePublicationRetry(name, table, slot, rows string) {
	GinkgoHelper()
	waitFollowStreaming(name)
	waitPhase(name, nsE2E, lagConvergeTimeout, v1beta1.PhaseCutoverPending)
	approveCutover(name)
	m := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
	Expect(m.Status.Attempts).To(Equal(int32(2)))
	expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
	expectConditionTrue(m, v1beta1.ConditionComplete)
	expectCleanupSucceeded(name)
	Expect(psql(targetCluster, publicationRetryRowsSQL(table))).To(Equal(rows))
	Expect(psql(sourceCluster, publicationRetryPubCount+slot+"'")).To(Equal("0"))
	Expect(sourceSlotCount()).To(Equal("0"))
	Expect(targetOriginCount()).To(Equal("0"))
}
