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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

var _ = Describe("Explicit partial table policies", SpecPriority(2), func() {
	It("keeps both UNLOGGED modes through resume and verifies the followed tables at cutover", func() {
		m, full, slot := publicationRetryFixture("e2e-unlogged-policies")
		empty, once := full+"_empty", full+"_once"
		DeferCleanup(func() {
			if m.UID != "" {
				deletePublicationRetryMigration(m)
			}
			for _, cluster := range []string{sourceCluster, targetCluster} {
				psql(cluster, "DROP TABLE IF EXISTS "+empty+", "+once)
			}
		})
		psql(sourceCluster, asSourceAppRole()+"CREATE UNLOGGED TABLE "+empty+
			" (id int PRIMARY KEY, payload text); CREATE UNLOGGED TABLE "+once+
			" (id int PRIMARY KEY, payload text); INSERT INTO "+empty+
			" VALUES (1, 'discard'); INSERT INTO "+once+" VALUES (1, 'initial')")
		m.Spec.Clone.Filters.IncludeOnlyTables = []string{full, empty, once}
		m.Spec.Follow.UnloggedTables = []v1beta1.UnloggedTable{
			{Table: empty, Mode: "SchemaOnly"}, {Table: once, Mode: "CopyOnce"},
		}
		m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
		create(m)
		waitFollowStreaming(m.Name)
		waitPhase(m.Name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		Expect(psql(targetCluster, "SELECT count(*) FROM "+empty)).To(Equal("0"))
		Expect(psql(targetCluster, "SELECT payload FROM "+once)).To(Equal("initial"))
		Expect(psql(sourceCluster, "SELECT tablename FROM pg_publication_tables WHERE pubname='"+slot+"'")).
			To(Equal(strings.TrimPrefix(full, "public.")))

		By("changing UNLOGGED data without forwarding it or changing its persistence")
		psql(sourceCluster, asSourceAppRole()+"UPDATE "+once+" SET payload='changed'; INSERT INTO "+empty+
			" VALUES (2, 'also discarded'); INSERT INTO "+full+" VALUES (1, 'followed')")
		Eventually(func() string {
			return psql(targetCluster, "SELECT count(*) FROM "+full)
		}, migrationTimeout, time.Second).Should(Equal("2"))
		Expect(psql(targetCluster, "SELECT payload FROM "+once)).To(Equal("initial"))
		Expect(psql(sourceCluster, "SELECT relpersistence FROM pg_class WHERE oid='"+once+"'::regclass")).To(Equal("u"))

		setSuspend(m.Name, true)
		waitPhase(m.Name, nsE2E, migrationTimeout, v1beta1.PhaseSuspended)
		setSuspend(m.Name, false)
		waitPhase(m.Name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		Expect(psql(targetCluster, "SELECT payload FROM "+once)).To(Equal("initial"), "resume must not recopy CopyOnce")
		approveCutover(m.Name)
		done := waitPhase(m.Name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		Expect(apimeta.IsStatusConditionTrue(done.Status.Conditions, "UnsynchronizedTables")).To(BeTrue())
		Expect(apimeta.IsStatusConditionTrue(done.Status.Conditions, v1beta1.ConditionVerified)).To(BeTrue())
		Expect(psql(targetCluster, "SELECT count(*) FROM "+empty)).To(Equal("0"))
		Expect(psql(sourceCluster, publicationRetryPubCount+slot+"'")).To(Equal("0"))
	})

	It("streams INSERTs without a replica identity while source UPDATE and DELETE succeed and stay local", func() {
		m, full, _ := publicationRetryFixture("e2e-insert-only-policy")
		events := full + "_events"
		DeferCleanup(func() {
			if m.UID != "" {
				deletePublicationRetryMigration(m)
			}
			for _, cluster := range []string{sourceCluster, targetCluster} {
				psql(cluster, "DROP TABLE IF EXISTS "+events)
			}
		})
		psql(sourceCluster, asSourceAppRole()+"CREATE TABLE "+events+
			" (id int, payload text); INSERT INTO "+events+" VALUES (1, 'initial')")
		m.Spec.Clone.Filters.IncludeOnlyTables = []string{full, events}
		m.Spec.Follow.InsertOnlyTables = []string{events}
		create(m)
		waitFollowStreaming(m.Name)
		waitPhase(m.Name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		names := pgcopydb.ManagedPublications(&m.Spec, m.Namespace, m.Name)
		Expect(psql(sourceCluster, "SELECT pubinsert AND NOT pubupdate AND NOT pubdelete AND NOT pubtruncate "+
			"FROM pg_publication WHERE pubname='"+names[1]+"'")).To(Equal("t"))

		psql(sourceCluster, asSourceAppRole()+"UPDATE "+events+" SET payload='changed'; DELETE FROM "+events+
			"; INSERT INTO "+events+" VALUES (2, 'new'); INSERT INTO "+full+" VALUES (1, 'followed')")
		Eventually(func() string {
			return psql(targetCluster, "SELECT id, payload FROM "+events+" ORDER BY id")
		}, migrationTimeout, time.Second).Should(Equal("1|initial\n2|new"))
		Eventually(func() string {
			return psql(targetCluster, "SELECT count(*) FROM "+full)
		}, migrationTimeout, time.Second).Should(Equal("2"))
		Expect(psql(sourceCluster, "SELECT id, payload FROM "+events)).To(Equal("2|new"))
		By("removing this test Migration before fixture teardown, including both owned publications")
		deletePublicationRetryMigration(m)
		for _, name := range names {
			Expect(psql(sourceCluster, publicationRetryPubCount+name+"'")).To(Equal("0"))
		}
	})
})
