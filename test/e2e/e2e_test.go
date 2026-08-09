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
	"net/url"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The scenarios share the two CNPG fixtures and run in order: later ones
// build on the populated target that earlier ones leave behind.
var _ = Describe("Migration", Ordered, func() {
	It("completes a fresh clone with matching rows and sequences", func() {
		create(newMigration("e2e-fresh", nsE2E, v1beta1.CloneOptions{}))
		m := waitCompleted("e2e-fresh", nsE2E)
		Expect(m.Status.Attempts).To(Equal(int32(1)))

		By("checking the seeded source matches the scale-derived expectations")
		Expect(seedTableCounts(srcPod)).To(Equal(expectedSeedCounts()),
			"source row counts do not match the scale formula; seed and suite disagree")
		Expect(largeObjectCount(srcPod)).To(Equal(fmt.Sprint(scaled(40))))

		By("comparing table groups, matview, large objects, and sequences on both sides")
		Expect(seedTableCounts(tgtPod)).To(Equal(seedTableCounts(srcPod)))
		Expect(matviewState(tgtPod)).To(Equal(matviewState(srcPod)),
			"materialized view not repopulated identically on the target")
		Expect(largeObjectCount(tgtPod)).To(Equal(largeObjectCount(srcPod)))
		Expect(sequenceValues(tgtPod)).To(Equal(sequenceValues(srcPod)))
	})

	It("re-clones onto the populated target with dropIfExists", func() {
		create(newMigration("e2e-reclone", nsE2E, v1beta1.CloneOptions{DropIfExists: true}))
		m := waitCompleted("e2e-reclone", nsE2E)
		Expect(m.Status.Attempts).To(Equal(int32(1)))
	})

	It("excludes filtered schemas from the clone", func() {
		// The filtered dump carries no DROP for excluded objects, so the audit
		// schema left by the previous scenario must go by hand for the
		// absence check to mean anything.
		By("dropping the audit schema on the target")
		psql(tgtPod, "DROP SCHEMA IF EXISTS audit CASCADE")

		create(newMigration("e2e-filters", nsE2E, v1beta1.CloneOptions{
			DropIfExists: true,
			Filters:      &v1beta1.Filters{ExcludeSchemas: []string{"audit"}},
		}))
		waitCompleted("e2e-filters", nsE2E)

		By("checking audit stayed away while public tables arrived")
		Expect(psql(tgtPod, "SELECT to_regclass('audit.events') IS NULL")).To(Equal("t"))
		Expect(psql(tgtPod, "SELECT count(*) FROM customers")).To(Equal(fmt.Sprint(scaled(50000))))
		Expect(psql(tgtPod, "SELECT count(*) FROM orders")).To(Equal(fmt.Sprint(scaled(200000))))
	})

	It("resumes with a second attempt after the runner pod dies", func() {
		create(newMigration("e2e-resume", nsE2E, v1beta1.CloneOptions{DropIfExists: true}))

		// A Running attempt-1 pod means the Migration is mid-clone; poll fast
		// because the whole clone only takes about a minute.
		By("waiting for the attempt-1 runner pod to run")
		var pod corev1.Pod
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E), client.MatchingLabels{
				"pgcopydb-operator.io/migration": "e2e-resume",
				"batch.kubernetes.io/job-name":   "e2e-resume-run-1",
			})).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty(), "attempt-1 pod not created yet")
			pod = pods.Items[0]
			g.Expect(pod.Status.Phase).To(Equal(corev1.PodRunning))
		}, 2*time.Minute, 250*time.Millisecond).Should(Succeed())

		// Kill the pod, not the Job: the Job has backoffLimit 0 and fails,
		// and the operator owns the retry.
		By("killing the runner pod")
		Expect(k8sClient.Delete(ctx, &pod, client.GracePeriodSeconds(1))).To(Succeed())

		m := waitCompleted("e2e-resume", nsE2E)
		Expect(m.Status.Attempts).To(BeNumerically(">=", int32(2)))

		By("checking attempt 2 ran with --resume")
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: "e2e-resume-run-2"}, job)).To(Succeed())
		Expect(job.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--resume"))

		Expect(rowCounts(tgtPod)).To(Equal(rowCounts(srcPod)))
	})

	It("clones across namespaces using local secrets and remote hosts", func() {
		// Stand-in for cross-cluster: the Migration and its secrets live in
		// one namespace, the databases are reached by service DNS in another.
		By("copying the app secrets into " + nsX)
		for _, name := range []string{srcSecret, tgtSecret} {
			orig := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, orig)).To(Succeed())
			copySecret(nsX, name, orig.Data[passwordKey])
		}

		create(newMigration("e2e-xns", nsX, v1beta1.CloneOptions{DropIfExists: true}))
		waitCompleted("e2e-xns", nsX)
	})

	It("rejects a spec with both uriSecretRef and inline host", func() {
		m := newMigration("e2e-invalid", nsE2E, v1beta1.CloneOptions{})
		m.Spec.Source.URISecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "does-not-matter"},
			Key:                  "uri",
		}
		err := k8sClient.Create(ctx, m)
		Expect(err).To(HaveOccurred(), "CEL validation should reject uriSecretRef combined with host")
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an Invalid API error, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("not both"))
	})

	It("clones with uriSecretRef connections built from the CNPG secrets", func() {
		const uriSecretName = "e2e-uris"
		By("building libpq URIs from the app secrets and storing them in one Secret")
		data := map[string][]byte{}
		for key, cluster := range map[string]string{"source": sourceCluster, "target": targetCluster} {
			sec := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: cluster + "-app"}, sec)).To(Succeed())
			// url.UserPassword escapes the generated password, whatever is in it.
			u := url.URL{
				Scheme: "postgresql",
				User:   url.UserPassword(appDB, string(sec.Data[passwordKey])),
				Host:   cluster + "-rw." + nsE2E + ".svc:5432",
				Path:   "/" + appDB,
			}
			data[key] = []byte(u.String())
		}
		uriSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: uriSecretName}}
		_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, uriSecret, func() error {
			uriSecret.Data = data
			return nil
		})
		Expect(err).NotTo(HaveOccurred(), "failed to store the URI secret")

		m := newMigration("e2e-uri", nsE2E, v1beta1.CloneOptions{DropIfExists: true})
		m.Spec.Source = v1beta1.PostgresConnection{URISecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: uriSecretName}, Key: "source"}}
		m.Spec.Target = v1beta1.PostgresConnection{URISecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: uriSecretName}, Key: "target"}}
		create(m)
		waitCompleted("e2e-uri", nsE2E)

		Expect(seedTableCounts(tgtPod)).To(Equal(seedTableCounts(srcPod)))
	})

	It("verifies a clone with pgcopydb compare and sets Verified", func() {
		m := newMigration("e2e-verified", nsE2E, v1beta1.CloneOptions{DropIfExists: true})
		m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
		create(m)

		// The data compare re-reads every table on both sides, so this
		// scenario gets the follow budget, not the clone one.
		m = waitPhase("e2e-verified", nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(m, v1beta1.ConditionVerified)

		By("checking both compare Jobs succeeded")
		for _, check := range []string{"schema", "data"} {
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: nsE2E, Name: "e2e-verified-compare-" + check}, job)).To(Succeed())
			Expect(job.Status.Succeeded).To(BeNumerically(">=", 1),
				"compare %s Job did not succeed", check)
		}
	})

	// The follow scenarios run strictly after each other: each asserts the
	// source is free of pgcopydb replication slots when it finishes, and the
	// next one relies on that clean slate for its own slot counting.
	It("streams live writes and completes a Manual cutover", func() {
		const name = "e2e-follow-manual"
		create(newFollowMigration(name, v1beta1.CutoverManual))

		By("waiting for the base copy to finish and streaming to start")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)

		By("inserting 1000 fresh rows into source orders while streaming")
		psql(srcPod, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) SELECT (g %% %d) + 1,"+
			" (g %% 90)::numeric / 3, 'live-' || g FROM generate_series(1, 1000) g", scaled(50000)))

		By("verifying status.replication fills in from the sentinel")
		m := &v1beta1.Migration{}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			rep := m.Status.Replication
			g.Expect(rep).NotTo(BeNil(), "status.replication not populated yet")
			g.Expect(rep.WriteLSN).NotTo(BeEmpty(), "writeLSN empty")
			g.Expect(rep.ReplayLSN).NotTo(BeEmpty(), "replayLSN empty")
			g.Expect(rep.LagBytes).NotTo(BeNil(), "lagBytes absent")
		}, 3*time.Minute, 2*time.Second).Should(Succeed())

		By("waiting for CutoverPending (caught up, Manual gate holds)")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)

		// The burst above was the only writer, so writes are already stopped;
		// approving now is safe.
		By("approving the cutover")
		approveCutover(name)

		By("waiting for the cutover to drain and complete")
		m = waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
		expectCleanupSucceeded(name)

		By("comparing data, sequences, and slot state after cutover")
		srcOrders := psql(srcPod, "SELECT count(*) FROM orders")
		Expect(psql(tgtPod, "SELECT count(*) FROM orders")).To(Equal(srcOrders),
			"target orders count differs from source after cutover")
		Expect(psql(tgtPod, "SELECT count(*) FROM orders WHERE note LIKE 'live-%'")).To(Equal("1000"),
			"live rows written during streaming did not all arrive on the target")
		Expect(sequenceValues(tgtPod)).To(Equal(sequenceValues(srcPod)))
		Expect(sourceSlotCount()).To(Equal("0"), "replication slot left behind on the source")
		Expect(targetOriginCount()).To(Equal("0"), "pgcopydb replication origin left behind on the target after cleanup")

		By("removing the live rows so the source matches the seeded fixture again")
		psql(srcPod, "DELETE FROM orders WHERE note LIKE 'live-%'")
	})

	It("runs an Automatic cutover to completion unattended", func() {
		const name = "e2e-follow-auto"
		// Schema verification on top: after cutover and cleanup the compare
		// runs against a quiesced pair, so CutoverCompleted and Verified
		// must both come out True on one Migration.
		mig := newFollowMigration(name, v1beta1.CutoverAutomatic)
		mig.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
		create(mig)

		m := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
		expectConditionTrue(m, v1beta1.ConditionVerified)
		expectCleanupSucceeded(name)

		By("comparing data and slot state after the unattended cutover")
		Expect(rowCounts(tgtPod)).To(Equal(rowCounts(srcPod)))
		Expect(sourceSlotCount()).To(Equal("0"), "replication slot left behind on the source")
		Expect(targetOriginCount()).To(Equal("0"), "pgcopydb replication origin left behind on the target after cleanup")
	})

	It("drops the replication slot when a streaming Migration is deleted", func() {
		const name = "e2e-follow-del"
		create(newFollowMigration(name, v1beta1.CutoverManual))

		By("waiting for streaming so the slot exists on the source")
		m := waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)
		Expect(sourceSlotCount()).To(Equal("1"), "expected exactly the streaming migration's slot")

		By("deleting the Migration mid-stream")
		Expect(k8sClient.Delete(ctx, m)).To(Succeed())

		By("waiting for cleanup to run and the finalizer to release the CR")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, &v1beta1.Migration{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"Migration still present (finalizer not released), get error: %v", err)
		}, 3*time.Minute, 2*time.Second).Should(Succeed())
		Eventually(sourceSlotCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
			"replication slot leaked on the source after deletion")
		Eventually(targetOriginCount, 3*time.Minute, 2*time.Second).Should(Equal("0"),
			"pgcopydb replication origin leaked on the target after deletion")
	})

	It("suspends a streaming Migration and resumes it through cutover", func() {
		const name = "e2e-suspend"
		mig := newFollowMigration(name, v1beta1.CutoverManual)
		create(mig)

		By("waiting for streaming so the slot exists on the source")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)
		Expect(sourceSlotCount()).To(Equal("1"), "expected exactly the streaming migration's slot")

		By("suspending the Migration")
		setSuspend(name, true)
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseSuspended)

		By("waiting for the worker Job to be gone (foreground deletion)")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, &batchv1.Job{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "worker Job still present, get error: %v", err)
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("checking the work PVC and the source slot survive suspension")
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-work"},
			&corev1.PersistentVolumeClaim{})).To(Succeed(), "work PVC gone while suspended")
		Expect(sourceSlotCount()).To(Equal("1"), "replication slot must be retained while suspended")

		By("checking the SlotRetained warning event fired on suspend")
		// Matched by UID: kept fixtures mean an earlier run's e2e-suspend events
		// may still be in the namespace.
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
			var found bool
			for _, e := range events.Items {
				if e.InvolvedObject.UID == mig.UID && e.Reason == "SlotRetained" &&
					e.Type == corev1.EventTypeWarning {
					found = true
					break
				}
			}
			g.Expect(found).To(BeTrue(), "SlotRetained warning event not recorded")
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("writing 500 rows on the source while suspended")
		psql(srcPod, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) SELECT (g %% %d) + 1,"+
			" (g %% 60)::numeric / 2, 'live-susp-' || g FROM generate_series(1, 500) g", scaled(50000)))

		By("resuming the Migration")
		setSuspend(name, false)

		By("waiting for streaming to recover on a fresh attempt")
		m := waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)
		Expect(m.Status.Attempts).To(Equal(int32(2)))

		By("checking attempt 2 ran with --resume")
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-2"}, job)).To(Succeed())
		Expect(job.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--resume"))

		By("waiting for CutoverPending and approving the cutover")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		approveCutover(name)

		m = waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
		expectCleanupSucceeded(name)

		By("comparing data and slot state after the resumed cutover")
		Expect(rowCounts(tgtPod)).To(Equal(rowCounts(srcPod)))
		Expect(psql(tgtPod, "SELECT count(*) FROM orders WHERE note LIKE 'live-susp-%'")).To(Equal("500"),
			"rows written while suspended did not arrive after resume")
		Expect(sequenceValues(tgtPod)).To(Equal(sequenceValues(srcPod)))
		Expect(sourceSlotCount()).To(Equal("0"), "replication slot left behind on the source")
		Expect(targetOriginCount()).To(Equal("0"), "pgcopydb replication origin left behind on the target after cleanup")

		By("removing the suspend-window rows so the source matches the seeded fixture again")
		psql(srcPod, "DELETE FROM orders WHERE note LIKE 'live-susp-%'")
	})

	// The failure-path specs close the ordered container: each one breaks a
	// prerequisite on purpose and restores it in DeferCleanup, and running
	// them after every live scenario keeps a missed restore from poisoning
	// anything downstream. The asserted hints are substrings of the message
	// constants in internal/controller/resources.go (preflightScript).
	It("fails preflight when the migration role lacks REPLICATION", func() {
		const name = "e2e-norepl"
		DeferCleanup(func() {
			deleteMigration(name)
			ensureFollowPrivileges()
			resetSourceReplication()
			resetTargetReplication()
		})

		By("revoking REPLICATION from the app role on the source")
		psql(srcPod, "ALTER ROLE app NOREPLICATION")

		create(newFollowMigration(name, v1beta1.CutoverManual))
		m := waitFailed(name, "PreflightFailed")
		Expect(failureMessage(m)).To(ContainSubstring(`ALTER ROLE "app" REPLICATION`),
			"preflight verdict must carry the exact re-grant hint")
	})

	It("fails preflight when the target role lacks EXECUTE on the origin functions", func() {
		const name = "e2e-noorigin"
		DeferCleanup(func() {
			deleteMigration(name)
			ensureFollowPrivileges()
			resetSourceReplication()
			resetTargetReplication()
		})

		By("revoking EXECUTE on the pg_replication_origin functions on the target")
		psql(tgtPod, "DO $$ DECLARE f oid; BEGIN FOR f IN SELECT p.oid FROM pg_proc p"+
			" JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'pg_catalog'"+
			" AND p.proname LIKE 'pg_replication_origin%' LOOP"+
			" EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM app', f::regprocedure); END LOOP; END $$")

		create(newFollowMigration(name, v1beta1.CutoverManual))
		m := waitFailed(name, "PreflightFailed")
		Expect(failureMessage(m)).To(ContainSubstring("lacks EXECUTE on replication origin functions"))
		Expect(failureMessage(m)).To(ContainSubstring("GRANT EXECUTE ON FUNCTION"),
			"preflight verdict must carry the ready-to-run GRANT statements")
	})

	It("fails preflight when the target role cannot SET session_replication_role", func() {
		const name = "e2e-nosrr"
		DeferCleanup(func() {
			deleteMigration(name)
			ensureFollowPrivileges()
			resetSourceReplication()
			resetTargetReplication()
		})

		By("revoking SET on session_replication_role on the target")
		psql(tgtPod, "REVOKE SET ON PARAMETER session_replication_role FROM app")

		create(newFollowMigration(name, v1beta1.CutoverManual))
		m := waitFailed(name, "PreflightFailed")
		Expect(failureMessage(m)).To(
			ContainSubstring(`GRANT SET ON PARAMETER session_replication_role TO "app"`),
			"preflight verdict must carry the exact re-grant hint")
	})

	It("exhausts the retry budget against an unreachable source and stays Failed", func() {
		const name = "e2e-backoff"
		DeferCleanup(func() { deleteMigration(name) })

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{})
		// .invalid never resolves (RFC 2606), so every attempt fails fast at
		// connect time; backoffLimit 1 buys exactly two attempts.
		m.Spec.Source.Host = "e2e-no-such-host.invalid"
		m.Spec.BackoffLimit = 1
		create(m)

		By("waiting for the terminal failure after exactly two attempts")
		failed := waitFailed(name, "BackoffLimitExceeded")
		Expect(failed.Status.Attempts).To(Equal(int32(2)))

		By("checking Failed is absorbing: no third attempt within a full poll interval")
		Consistently(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, cur)).To(Succeed())
			g.Expect(cur.Status.Phase).To(Equal(v1beta1.PhaseFailed))
			g.Expect(cur.Status.Attempts).To(Equal(int32(2)))
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-3"}, &batchv1.Job{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"a third attempt Job appeared in the absorbing Failed state")
		}, 45*time.Second, 5*time.Second).Should(Succeed())
	})
})

// e2eConn points at a fixture cluster through its rw service, fully qualified
// so the same spec works from pgcopydb-e2e-x.
func e2eConn(cluster string) v1beta1.PostgresConnection {
	return v1beta1.PostgresConnection{
		Host:     cluster + "-rw." + nsE2E + ".svc",
		Database: appDB,
		Username: appDB,
		PasswordSecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: cluster + "-app"},
			Key:                  passwordKey,
		},
	}
}

func newMigration(name, ns string, clone v1beta1.CloneOptions) *v1beta1.Migration {
	// The work volume follows the tier: it holds the schema dump, catalogs,
	// and (for follow) buffered change files, not the table data itself.
	wv := v1beta1.WorkVolume{Size: resource.MustParse(workVolumeSize)}
	if fixtureStorageClass != "" {
		sc := fixtureStorageClass
		wv.StorageClassName = &sc
	}
	return &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1beta1.MigrationSpec{
			Source:     e2eConn(sourceCluster),
			Target:     e2eConn(targetCluster),
			Clone:      clone,
			WorkVolume: wv,
		},
	}
}

func create(m *v1beta1.Migration) {
	GinkgoHelper()
	Expect(k8sClient.Create(ctx, m)).To(Succeed(), "failed to create Migration %s/%s", m.Namespace, m.Name)
}

// waitCompleted waits for phase Completed within the standard clone budget.
func waitCompleted(name, ns string) *v1beta1.Migration {
	GinkgoHelper()
	return waitPhase(name, ns, migrationTimeout, v1beta1.PhaseCompleted)
}

// waitPhase waits until the Migration reaches one of the wanted phases and
// bails out early on Failed so a broken run reports the operator's failure
// message instead of a timeout.
func waitPhase(name, ns string, timeout time.Duration, want ...v1beta1.MigrationPhase) *v1beta1.Migration {
	GinkgoHelper()
	m := &v1beta1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, m)).To(Succeed())
		if m.Status.Phase == v1beta1.PhaseFailed {
			StopTrying(fmt.Sprintf("migration %s/%s failed: %s", ns, name, failureMessage(m))).Now()
		}
		g.Expect(want).To(ContainElement(m.Status.Phase),
			"migration %s/%s at phase %q, attempts %d", ns, name, m.Status.Phase, m.Status.Attempts)
	}, timeout, 2*time.Second).Should(Succeed())
	return m
}

func failureMessage(m *v1beta1.Migration) string {
	if c := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed); c != nil {
		return c.Message
	}
	return "(no Failed condition message)"
}

// rowCounts returns customers/orders/audit.events counts in one round trip:
// the tables the live-write scenarios touch.
func rowCounts(pod string) string {
	GinkgoHelper()
	return psql(pod, "SELECT (SELECT count(*) FROM customers) || '/' ||"+
		" (SELECT count(*) FROM orders) || '/' || (SELECT count(*) FROM audit.events)")
}

// seedCountSQL flattens every seeded table's count into one string, in the
// order of expectedSeedCounts. events is the partitioned parent, so its count
// is the total across all partitions. A named constant so the chaos fan-out
// scenario can run the same query against a second target database.
const seedCountSQL = "SELECT (SELECT count(*) FROM customers) || '/' ||" +
	" (SELECT count(*) FROM orders) || '/' || (SELECT count(*) FROM audit.events) || '/' ||" +
	" (SELECT count(*) FROM audit.access_log) || '/' || (SELECT count(*) FROM app_users) || '/' ||" +
	" (SELECT count(*) FROM events) || '/' || (SELECT count(*) FROM readings) || '/' ||" +
	" (SELECT count(*) FROM documents)"

// seedTableCounts returns every seeded table's count in one round trip.
func seedTableCounts(pod string) string {
	GinkgoHelper()
	return psql(pod, seedCountSQL)
}

// expectedSeedCounts derives the seedTableCounts string from the scale; the
// base counts match the CALL arguments in fixtures/seed.sql.
func expectedSeedCounts() string {
	return fmt.Sprintf("%d/%d/%d/%d/%d/%d/%d/%d",
		scaled(50000), scaled(200000), scaled(1000), scaled(100000),
		scaled(20000), scaled(8000000), scaled(2500000), scaled(350000))
}

// matviewState reports whether event_daily_counts is populated and how many
// rows it holds. pg_restore repopulates it with REFRESH on the target, so
// both sides must agree.
func matviewState(pod string) string {
	GinkgoHelper()
	return psql(pod, "SELECT ispopulated || '/' || (SELECT count(*) FROM event_daily_counts)"+
		" FROM pg_matviews WHERE matviewname = 'event_daily_counts'")
}

// largeObjectCount counts large objects; pg_largeobject_metadata is
// per-database, so this sees exactly the fixture LOs.
func largeObjectCount(pod string) string {
	GinkgoHelper()
	return psql(pod, "SELECT count(*) FROM pg_largeobject_metadata")
}

// sequenceValues flattens every sequence and its last_value into one line, so
// source and target compare as plain strings.
func sequenceValues(pod string) string {
	GinkgoHelper()
	return psql(pod, "SELECT string_agg(schemaname || '.' || sequencename || '=' ||"+
		" coalesce(last_value::text, 'null'), ',' ORDER BY schemaname, sequencename) FROM pg_sequences")
}

// newFollowMigration builds a live migration against the shared fixtures.
// dropIfExists is mandatory here: earlier scenarios leave the target populated
// and a follow clone onto leftover objects would fail its base copy.
func newFollowMigration(name string, mode v1beta1.CutoverMode) *v1beta1.Migration {
	m := newMigration(name, nsE2E, v1beta1.CloneOptions{DropIfExists: true})
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: "pgoutput"}
	m.Spec.Cutover = v1beta1.CutoverSpec{Mode: mode}
	return m
}

// setSuspend flips spec.suspend, the pause/resume switch.
func setSuspend(name string, suspend bool) {
	GinkgoHelper()
	m := &v1beta1.Migration{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
	patch := client.MergeFrom(m.DeepCopy())
	m.Spec.Suspend = suspend
	Expect(k8sClient.Patch(ctx, m, patch)).To(Succeed(), "failed to set suspend=%v on %s", suspend, name)
}

// approveCutover flips spec.cutover.approved, the Manual-mode trigger.
func approveCutover(name string) {
	GinkgoHelper()
	m := &v1beta1.Migration{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
	patch := client.MergeFrom(m.DeepCopy())
	m.Spec.Cutover.Approved = true
	Expect(k8sClient.Patch(ctx, m, patch)).To(Succeed(), "failed to approve cutover for %s", name)
}

func expectConditionTrue(m *v1beta1.Migration, condType string) {
	GinkgoHelper()
	c := apimeta.FindStatusCondition(m.Status.Conditions, condType)
	Expect(c).NotTo(BeNil(), "condition %s missing on %s", condType, m.Name)
	Expect(c.Status).To(Equal(metav1.ConditionTrue), "condition %s is %s: %s", condType, c.Status, c.Message)
}

// expectCleanupSucceeded asserts the <name>-cleanup Job ran pgcopydb stream
// cleanup to success; Complete gates on it, so it must exist by now.
func expectCleanupSucceeded(name string) {
	GinkgoHelper()
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-cleanup"}, job)).
		To(Succeed(), "cleanup Job %s-cleanup not found", name)
	Expect(job.Status.Succeeded).To(BeNumerically(">=", 1), "cleanup Job %s-cleanup did not succeed", name)
}

// sourceSlotCount counts pgcopydb-created replication slots on the source;
// generated names always start with pgcopydb_.
func sourceSlotCount() string {
	GinkgoHelper()
	return psql(srcPod, "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgcopydb%'")
}

// targetOriginCount counts pgcopydb-created replication origins on the target.
// stream cleanup drops the generated origin (alpha.11's cleanup-origin fix), so
// a completed or deleted follow migration must leave none behind.
// pg_replication_origin is a shared catalog, so the app-database connection
// sees it.
func targetOriginCount() string {
	GinkgoHelper()
	return psql(tgtPod, "SELECT count(*) FROM pg_replication_origin WHERE roname LIKE 'pgcopydb%'")
}

// copySecret writes a password-only secret; CreateOrUpdate keeps reruns with
// kept fixtures from tripping over AlreadyExists.
func copySecret(ns, name string, password []byte) {
	GinkgoHelper()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, sec, func() error {
		sec.Data = map[string][]byte{passwordKey: password}
		return nil
	})
	Expect(err).NotTo(HaveOccurred(), "failed to copy secret %s into %s", name, ns)
}
