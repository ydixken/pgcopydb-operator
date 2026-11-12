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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Native fault specs, selected by the native-fault label or one case label. Every
// fault ends resumed with each committed row exactly once, or failed closed with the
// owned slot retained, and leaves the planted foreign resources unchanged.
var _ = Describe("Native faults", Label(nativeFaultLabel), func() {
	BeforeEach(func() {
		requireNativeFaultRun()
		resetMigrationPair()
	})

	It("resumes or fails closed when the runner is killed mid-copy", Label("native-fault-copy"), func() {
		const name = "e2e-nfault-copy"
		c := startNativeFault(name, targetCluster, nil)
		kill := armNativeFaultKill(name)

		By("waiting for a native COPY into the target")
		Eventually(func(g Gomega) {
			m := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			if m.Status.Phase == v1beta1.PhaseFailed || slices.Contains(followStreamingPhases, m.Status.Phase) {
				StopTrying(fmt.Sprintf("%s reached %s before a COPY was observed", name, m.Status.Phase)).Now()
			}
			out, err := psqlDBErr(targetCluster, appDatabase(targetCluster), nativeFaultCopySQL)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("0"), "no native COPY into the target yet")
		}, migrationTimeout, 250*time.Millisecond).Should(Succeed())

		By("killing the runner inside the COPY")
		kill.fire()
		c.finish(kill.attempt + 1)
	})

	It("resumes or fails closed when the runner is killed mid-stream", Label("native-fault-stream"), func() {
		const name = "e2e-nfault-stream"
		c := startNativeFault(name, targetCluster, nil)
		c.waitStreaming()
		kill := armNativeFaultKill(name)

		By("killing the runner while the writer's rows stream")
		kill.fire()
		c.finish(kill.attempt + 1)
	})

	It("completes or fails closed when the runner is killed mid-drain", Label("native-fault-drain"), func() {
		const name = "e2e-nfault-drain"
		const burstRows = 2000
		c := startNativeFault(name, targetCluster, func(m *v1beta1.Migration) {
			// The burst waits behind the lock below; the wide threshold keeps CaughtUp so cutover starts.
			m.Spec.Follow.MaxCatchupLag = ptr.To(resource.MustParse("1Gi"))
		})
		c.waitStreaming()
		last := c.stopWriter()
		c.waitApplied(last - 1)

		By("holding the target orders table and committing a burst behind it")
		release := holdNativeFaultLock(targetCluster, kindOrdersTable)
		burst := nativeFaultMarkerSet{marker: c.marker + "b", last: burstRows}
		psqlBulk(sourceCluster, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) SELECT (g %% %d) + 1,"+
			" (g %% 90)::numeric / 3, '%s-' || g FROM generate_series(1, %d) g", scaled(50000), burst.marker, burstRows))

		By("cutting over until the drain waits on the lock")
		approveCutover(name)
		m := waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCuttingOver)
		Expect(m.Status.Replication).NotTo(BeNil(), "%s is cutting over without replication status", name)
		Expect(m.Status.Replication.Endpos).NotTo(BeEmpty(), "%s is cutting over without an endpos", name)
		Eventually(func(g Gomega) {
			out, err := psqlDBErr(targetCluster, appDatabase(targetCluster), nativeFaultBlockedApplySQL)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("0"), "no native apply waits on the held lock")
		}, 2*time.Minute, time.Second).Should(Succeed())

		By("killing the runner inside the drain, then releasing the lock")
		kill := armNativeFaultKill(name)
		kill.fire()
		release()
		c.expectInvariant(waitTerminal(name, followTimeout), nativeFaultMarkerSet{c.marker, last}, burst)
	})

	It("loses no committed row when the target primary fails over under synchronous replication",
		Label("native-fault-failover"), func() {
			const name = "e2e-nfault-failover"
			// dataDurability required keeps the standby in synchronous_standby_names, so the
			// promoted instance holds every commit acknowledged to the apply.
			target := createDedicatedTarget("e2e-nfault-sync-", tgtStorageSize, 2, map[string]any{
				"method": "first", "number": int64(1), "dataDurability": "required",
			}).GetName()
			Eventually(func() int { return syncStandbys(target) }, 3*time.Minute, 2*time.Second).Should(Equal(1),
				"no synchronous standby streams on %s", target)
			grantTargetFollowPrivilegesOn(target, cnpgAppDatabase)
			c := startNativeFault(name, target, nil)
			c.waitStreaming()

			By("verifying synchronous durability and killing the target primary")
			expectNativeFaultSyncDurable(target)
			m := &v1beta1.Migration{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			primary := primaryPod(target)
			deletePod(client.MatchingLabels{labelCNPGCluster: target, labelCNPGRole: rolePrimary})
			Eventually(func() string { return primaryPod(target) }, clusterReadyTimeout, 2*time.Second).
				ShouldNot(Equal(primary), "no standby of %s was promoted", target)
			waitClusterReady(target)
			c.finish(m.Status.Attempts)
		})

	It("keeps its state while suspended and loses nothing after resume", Label("native-fault-suspend"), func() {
		const name = "e2e-nfault-suspend"
		c := startNativeFault(name, targetCluster, nil)
		c.waitStreaming()
		key := client.ObjectKey{Namespace: nsE2E, Name: name}
		applied := func(g Gomega, cluster string) int {
			out, err := psqlDBErr(cluster, appDatabase(cluster), liveMarkerQuery(c.marker))
			g.Expect(err).NotTo(HaveOccurred())
			n, err := strconv.Atoi(out)
			g.Expect(err).NotTo(HaveOccurred())
			return n
		}

		By("suspending the streaming Migration")
		term := beginNativeTerm("suspend", name, c.foreign.config.UID)
		if term != nil {
			term.request()
		}
		setSuspend(name, true)
		if term != nil {
			term.awaitTerminated()
		}
		suspended := waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseSuspended)
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
				client.MatchingLabels{migrationLabel: name})).To(Succeed())
			for i := range pods.Items {
				if strings.HasPrefix(pods.Items[i].Labels[batchv1.JobNameLabel], name+"-run-") {
					g.Expect(pods.Items[i].Status.Phase).NotTo(Equal(corev1.PodRunning),
						"worker %s runs while suspended", pods.Items[i].Name)
				}
			}
			out, err := psqlDBErr(sourceCluster, appDatabase(sourceCluster),
				"SELECT count(*) FROM pg_replication_slots WHERE NOT active AND starts_with(slot_name, "+
					sqlLiteral("rpgm_s_"+c.run+"_")+")")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("1"), "the owned slot is gone or still streaming")
			out, err = psqlDBErr(c.target, appDatabase(c.target), nativeFaultSessionSQL)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("0"), "native sessions remain on the target while suspended")
		}, 2*time.Minute, 2*time.Second).Should(Succeed())
		if term != nil {
			term.complete(nativeTermMigration, nativeTermJob, nativeTermPod, nativeTermPVC)
		}

		By("holding the target still while the writer builds a backlog")
		held := applied(Default, c.target)
		Consistently(func(g Gomega) {
			current := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
			g.Expect(current.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
			g.Expect(current.Status.Attempts).To(Equal(suspended.Status.Attempts), "an attempt started while suspended")
			g.Expect(applied(g, c.target)).To(Equal(held), "rows were applied while suspended")
		}, 30*time.Second, 5*time.Second).Should(Succeed())
		Expect(applied(Default, sourceCluster)).To(BeNumerically(">", held),
			"the writer built no backlog during the suspension")

		By("resuming and cutting over")
		setSuspend(name, false)
		c.finish(suspended.Status.Attempts)
	})

	It("removes only its own source resources when deleted while streaming", Label("native-fault-delete"), func() {
		const name = "e2e-nfault-delete"
		c := startNativeFault(name, targetCluster, nil)
		c.waitStreaming()

		By("deleting the Migration while the writer still commits")
		if term := beginNativeTerm("delete", name, c.foreign.config.UID); term != nil {
			term.request()
			Expect(k8sClient.Delete(ctx, &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: name}},
				client.Preconditions{UID: &term.migration})).To(Succeed(), "deleting %s", name)
			term.awaitTerminated()
			deleteMigration(name)
			term.complete(nativeTermMigration)
		} else {
			deleteMigration(name)
		}
		c.stopWriter()

		By("checking exactly the owned source resources are gone and the rest is retained")
		Expect(c.ownedSlots()).To(BeZero(), "deletion left the owned slot on the source")
		Expect(c.ownedPublications()).To(BeZero(), "deletion left the owned publication on the source")
		c.expectNoSessions()
		Expect(psql(targetCluster, "SELECT count(*) FROM pg_namespace WHERE nspname = "+
			sqlLiteral("_rpgmigrate_"+c.run))).To(Equal("1"), "deletion dropped the target bookkeeping it must retain")
		Expect(psqlInt(targetCluster, liveMarkerQuery(c.marker))).To(BeNumerically(">", 0),
			"deletion removed migrated target data")
		c.foreign.expectUnchanged()
	})
})
