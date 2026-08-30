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
	"net/url"
	"strings"
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

// Placement is configuration, and configuration that stops working fails
// silently: the suite would still pass on a single node, just slower and over
// loopback instead of the wire. Deliberately its own container rather than a
// spec inside the Ordered one below: a failure in an Ordered container skips
// every spec after it, and a check on where pods landed must never be able to
// take the functional suite down with it. Unlabeled on purpose, so it runs in
// every functional run and never under task e2e:chaos, which deletes instance
// pods deliberately.
var _ = Describe("Fixture placement", func() {
	// The binding check. It reads what was rendered onto the pods rather than
	// where they ended up, which makes it namespaced (so it runs under the
	// confined CI identity, never skipped) and deterministic (no scheduler in
	// the loop). Drop the requests or the affinity block and this fails at
	// once. The node-count spec below would not notice: CNPG defaults to a
	// preferred hostname anti-affinity on its own, so the fixtures would still
	// spread and that spec would still pass.
	It("configures every fixture pod with anti-affinity and resource requests", func() {
		for _, cluster := range []string{sourceCluster, targetCluster} {
			for _, pod := range instancePods(cluster) {
				Expect(spreadsOverHostname(&pod.Spec)).To(BeTrue(),
					"an instance of CNPG cluster %s carries no preferred hostname"+
						" anti-affinity, so its instances may share a node", cluster)
				Expect(requestsCPUAndMemory(&pod.Spec)).To(BeTrue(),
					"an instance of CNPG cluster %s requests no cpu or memory, so the"+
						" scheduler cannot see it and every pod scores the same node highest", cluster)
			}
		}
		// WAL shares the data volume, so a max_wal_size that does not follow
		// the tier eats the space the restore needs. A quarter is the line:
		// walMaxSize targets a fifth, and anything approaching half means the
		// value was hardcoded for a bigger fixture than this one.
		for _, cluster := range []string{sourceCluster, targetCluster} {
			wal, volume := walHeadroom(cluster)
			Expect(wal*4 <= volume).To(BeTrue(),
				"CNPG cluster %s allows %dMB of WAL on a %dMB volume, leaving too little"+
					" for the data being loaded", cluster, wal>>20, volume>>20)
		}

		seed := seedPod()
		Expect(spreadsOverHostname(&seed.Spec)).To(BeTrue(),
			"the seed worker carries no anti-affinity away from the fixture primaries")
		Expect(requestsCPUAndMemory(&seed.Spec)).To(BeTrue(),
			"the seed worker requests no cpu or memory")
	})

	// The outcome check, and only a sanity check: it proves the cluster is
	// shaped the way the fixtures assume, not that this suite configured
	// anything. At least, never exactly: the node set is read now while the
	// pods were placed in BeforeSuite, so a node cordoned or gone NotReady in
	// between lowers the expectation without moving a single pod.
	It("spreads each CNPG fixture across as many nodes as the cluster has", func() {
		usable := schedulableNodes()
		if usable == 0 {
			Skip("this identity is confined to the fixture namespaces and may not list" +
				" nodes, so the expected spread cannot be computed here; task e2e checks it")
		}
		want := min(usable, cnpgInstances)
		for _, cluster := range []string{sourceCluster, targetCluster} {
			// Compare counts, never the slice. This repository is public and
			// its CI logs are public with it, and a failed matcher would print
			// the node names it matched against.
			got := len(instanceNodes(cluster))
			Expect(got).To(BeNumerically(">=", want),
				"CNPG cluster %s occupies %d nodes, want at least %d: instances are"+
					" co-located and the migration never leaves the node", cluster, got, want)
		}
	})
})

// The scenarios share the two CNPG fixtures and run in order: later ones
// build on the populated target that earlier ones leave behind.
var _ = Describe("Migration", Ordered, func() {
	// Ginkgo randomizes top-level container order per seed, so another
	// container may have populated the target or may still be dropping its
	// replication state; restore the clean slate the specs assume.
	BeforeAll(func() {
		Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"),
			"pgcopydb replication slot left on the source by an earlier container")
		Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"),
			"pgcopydb replication origin left on the target by an earlier container")
		resetTargetObjects()
	})

	It("completes a fresh clone with matching rows and sequences", func() {
		create(newMigration("e2e-fresh", nsE2E, v1beta1.CloneOptions{}))
		m := waitCompleted("e2e-fresh", nsE2E)
		Expect(m.Status.Attempts).To(Equal(int32(1)))

		By("checking the seeded source matches the scale-derived expectations")
		Expect(seedTableCounts(sourceCluster)).To(Equal(expectedSeedCounts()),
			"source row counts do not match the scale formula; seed and suite disagree")
		Expect(largeObjectCount(sourceCluster)).To(Equal(fmt.Sprint(scaled(40))))

		By("comparing table groups, matview, large objects, and sequences on both sides")
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
		Expect(matviewState(targetCluster)).To(Equal(matviewState(sourceCluster)),
			"materialized view not repopulated identically on the target")
		Expect(largeObjectCount(targetCluster)).To(Equal(largeObjectCount(sourceCluster)))
		Expect(sequenceValues(targetCluster)).To(Equal(sequenceValues(sourceCluster)))
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
		psql(targetCluster, "DROP SCHEMA IF EXISTS audit CASCADE")

		create(newMigration("e2e-filters", nsE2E, v1beta1.CloneOptions{
			DropIfExists: true,
			Filters:      &v1beta1.Filters{ExcludeSchemas: []string{"audit"}},
		}))
		waitCompleted("e2e-filters", nsE2E)

		By("checking audit stayed away while public tables arrived")
		Expect(psql(targetCluster, "SELECT to_regclass('audit.events') IS NULL")).To(Equal("t"))
		Expect(psql(targetCluster, "SELECT count(*) FROM customers")).To(Equal(fmt.Sprint(scaled(50000))))
		Expect(psql(targetCluster, "SELECT count(*) FROM orders")).To(Equal(fmt.Sprint(scaled(200000))))
	})

	// Labelled flaky, not skipped: it fails about three runs in four at
	// E2E_SCALE=0.1 (see issue 88), which is often enough to stop every
	// automated release, and it still passes reliably enough by hand to be
	// worth keeping. CI filters the label out; task e2e does not.
	It("resumes with a second attempt after the runner pod dies", Label("flaky"), func() {
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

		Expect(rowCounts(targetCluster)).To(Equal(rowCounts(sourceCluster)))
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
		Expect(err.Error()).To(ContainSubstring("set exactly one of secretRef, uriSecretRef"))

		By("rejecting secretRef combined with inline host the same way")
		m = newMigration("e2e-invalid", nsE2E, v1beta1.CloneOptions{})
		m.Spec.Source.SecretRef = &v1beta1.ConnectionSecret{Name: "does-not-matter"}
		err = k8sClient.Create(ctx, m)
		Expect(err).To(HaveOccurred(), "CEL validation should reject secretRef combined with host")
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an Invalid API error, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("set exactly one of secretRef, uriSecretRef"))
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

		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
	})

	It("clones with connection details from a single Secret", func() {
		const name = "e2e-details"
		By("reading the CNPG-generated passwords")
		pw := map[string][]byte{}
		for _, cluster := range []string{sourceCluster, targetCluster} {
			sec := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: cluster + "-app"}, sec)).To(Succeed())
			pw[cluster] = sec.Data[passwordKey]
		}

		By("packing them into per-side detail Secrets")
		// Source uses the convention keys with DB holding a password-free URI
		// (the authoritative-URI branch); target remaps the keys and composes
		// from parts, with a portless host exercising the :5432 default.
		srcURI := fmt.Sprintf("postgresql://%s@%s-rw.%s.svc:5432/%s", appDB, sourceCluster, nsE2E, appDB)
		details := map[string]map[string][]byte{
			name + "-source": {
				"DB": []byte(srcURI),
				"PW": pw[sourceCluster],
			},
			name + "-target": {
				"db":   []byte(appDB),
				"pw":   pw[targetCluster],
				"host": []byte(targetCluster + "-rw." + nsE2E + ".svc"),
				"role": []byte(appDB),
			},
		}
		for secName, data := range details {
			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: secName}}
			_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, sec, func() error {
				sec.Data = data
				return nil
			})
			Expect(err).NotTo(HaveOccurred(), "failed to store the detail secret %s", secName)
		}

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{DropIfExists: true})
		m.Spec.Source = v1beta1.PostgresConnection{
			SecretRef: &v1beta1.ConnectionSecret{Name: name + "-source"},
		}
		m.Spec.Target = v1beta1.PostgresConnection{
			SecretRef: &v1beta1.ConnectionSecret{
				Name: name + "-target",
				Keys: &v1beta1.ConnectionSecretKeys{Database: "db", Password: "pw", URL: "host", Username: "role"},
			},
		}
		create(m)
		waitCompleted(name, nsE2E)

		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
	})

	It("fails preflight fast on wrong credentials", func() {
		const name = "e2e-badpw"
		DeferCleanup(func() { deleteMigration(name) })

		By("pointing the source at a Secret holding a wrong password")
		copySecret(nsE2E, name, []byte("not-the-password"))
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{})
		m.Spec.Source.PasswordSecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Key:                  passwordKey,
		}
		create(m)

		By("waiting for the connectivity tier to fail the Migration before any attempt")
		failed := waitFailed(name, "PreflightFailed")
		msg := failureMessage(failed)
		Expect(msg).To(ContainSubstring("cannot connect to the source database"))
		// A stopwatch here would time the node's warmth and the image pull,
		// not the ladder. The log tail the condition carries measures the
		// thing itself: an auth failure repeated on the second probe ends it
		// after one retry, where the six-probe ladder would leave five.
		Expect(strings.Count(msg, "retry: source connectivity attempt")).To(BeNumerically("<=", 1),
			"the connectivity ladder was walked instead of ending on the repeated auth failure:\n%s", msg)
		Expect(failed.Status.Attempts).To(Equal(int32(0)), "a worker attempt started despite the failed preflight")
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, &batchv1.Job{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "worker Job exists despite the failed preflight")
	})

	It("surfaces why the preflight cannot start", func() {
		const name = "e2e-ghost"
		DeferCleanup(func() { deleteMigration(name) })

		By("referencing a connection Secret that does not exist")
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{})
		m.Spec.Source = v1beta1.PostgresConnection{
			SecretRef: &v1beta1.ConnectionSecret{Name: name + "-missing"},
		}
		create(m)

		By("waiting for the gate to report the pod-level reason on the Validated condition")
		Eventually(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, cur)).To(Succeed())
			c := apimeta.FindStatusCondition(cur.Status.Conditions, v1beta1.ConditionValidated)
			g.Expect(c).NotTo(BeNil(), "Validated condition missing")
			g.Expect(c.Status).To(Equal(metav1.ConditionUnknown))
			g.Expect(c.Reason).To(Equal("PreflightRunning"))
			g.Expect(c.Message).To(ContainSubstring("CreateContainerConfigError"),
				"the kubelet's reason for the stuck pod must reach the condition")
		}, 3*time.Minute, 2*time.Second).Should(Succeed())
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
		mig := newFollowMigration(name, v1beta1.CutoverManual)
		// 0.18's catalog layer crashes probabilistically at this scale; retries resume from the work dir.
		mig.Spec.BackoffLimit = 5
		create(mig)

		By("waiting for the base copy to finish and streaming to start")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)

		By("inserting 1000 fresh rows into source orders while streaming")
		psql(sourceCluster, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) SELECT (g %% %d) + 1,"+
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
		srcOrders := psql(sourceCluster, "SELECT count(*) FROM orders")
		Expect(psql(targetCluster, "SELECT count(*) FROM orders")).To(Equal(srcOrders),
			"target orders count differs from source after cutover")
		Expect(psql(targetCluster, "SELECT count(*) FROM orders WHERE note LIKE 'live-%'")).To(Equal("1000"),
			"live rows written during streaming did not all arrive on the target")
		Expect(sequenceValues(targetCluster)).To(Equal(sequenceValues(sourceCluster)))
		Expect(sourceSlotCount()).To(Equal("0"), "replication slot left behind on the source")
		Expect(targetOriginCount()).To(Equal("0"), "pgcopydb replication origin left behind on the target after cleanup")

		By("removing the live rows so the source matches the seeded fixture again")
		psql(sourceCluster, "DELETE FROM orders WHERE note LIKE 'live-%'")
	})

	It("runs an Automatic cutover to completion unattended", func() {
		const name = "e2e-follow-auto"
		// Schema verification on top: after cutover and cleanup the compare
		// runs against a quiesced pair, so CutoverCompleted and Verified
		// must both come out True on one Migration.
		mig := newFollowMigration(name, v1beta1.CutoverAutomatic)
		mig.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
		// 0.18's catalog layer crashes probabilistically at this scale; retries resume from the work dir.
		mig.Spec.BackoffLimit = 5
		create(mig)

		m := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
		expectConditionTrue(m, v1beta1.ConditionVerified)
		expectCleanupSucceeded(name)

		By("comparing data and slot state after the unattended cutover")
		Expect(rowCounts(targetCluster)).To(Equal(rowCounts(sourceCluster)))
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

	// Labelled flaky for the same reason as the pod-death resume above: both
	// resume through pgcopydb's SQLite catalogs, which intermittently answer
	// "database is locked" and cost the attempt (issue 88). It failed the
	// v0.8.1-rc.1 gate on exactly that assertion, so CI filters it out and
	// task e2e still runs it.
	It("suspends a streaming Migration and resumes it through cutover", Label("flaky"), func() {
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
		psql(sourceCluster, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) SELECT (g %% %d) + 1,"+
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
		Expect(rowCounts(targetCluster)).To(Equal(rowCounts(sourceCluster)))
		Expect(psql(targetCluster, "SELECT count(*) FROM orders WHERE note LIKE 'live-susp-%'")).To(Equal("500"),
			"rows written while suspended did not arrive after resume")
		Expect(sequenceValues(targetCluster)).To(Equal(sequenceValues(sourceCluster)))
		Expect(sourceSlotCount()).To(Equal("0"), "replication slot left behind on the source")
		Expect(targetOriginCount()).To(Equal("0"), "pgcopydb replication origin left behind on the target after cleanup")

		By("removing the suspend-window rows so the source matches the seeded fixture again")
		psql(sourceCluster, "DELETE FROM orders WHERE note LIKE 'live-susp-%'")
	})

	// The rights-manipulation specs close the ordered container: each one
	// breaks a prerequisite on purpose and restores it (via DeferCleanup or,
	// for the remediation scenario, through the operator itself), and running
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
		psql(sourceCluster, "ALTER ROLE app NOREPLICATION")

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
		psql(targetCluster, "DO $$ DECLARE f oid; BEGIN FOR f IN SELECT p.oid FROM pg_proc p"+
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
		psql(targetCluster, "REVOKE SET ON PARAMETER session_replication_role FROM app")

		create(newFollowMigration(name, v1beta1.CutoverManual))
		m := waitFailed(name, "PreflightFailed")
		Expect(failureMessage(m)).To(
			ContainSubstring(`GRANT SET ON PARAMETER session_replication_role TO "app"`),
			"preflight verdict must carry the exact re-grant hint")
	})

	It("hints at superuserSecretRef when follow rights are missing", func() {
		const name = "e2e-hint"
		DeferCleanup(func() {
			deleteMigration(name)
			ensureFollowPrivileges()
			resetSourceReplication()
			resetTargetReplication()
		})

		By("revoking all three grantable follow rights from the app role")
		psql(sourceCluster, "ALTER ROLE app NOREPLICATION")
		psql(targetCluster, "DO $$ DECLARE f oid; BEGIN FOR f IN SELECT p.oid FROM pg_proc p"+
			" JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'pg_catalog'"+
			" AND p.proname LIKE 'pg_replication_origin%' LOOP"+
			" EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM app', f::regprocedure); END LOOP; END $$")
		psql(targetCluster, "REVOKE SET ON PARAMETER session_replication_role FROM app")

		create(newFollowMigration(name, v1beta1.CutoverManual))
		m := waitFailed(name, "PreflightFailed")
		Expect(failureMessage(m)).To(ContainSubstring("hint: spec.source.superuserSecretRef"),
			"a failed source right without a superuser ref must hint at the field")
		Expect(failureMessage(m)).To(ContainSubstring("hint: spec.target.superuserSecretRef"),
			"a failed target right without a superuser ref must hint at the field")
	})

	It("remediates follow rights through superuserSecretRef and completes", func() {
		const name = "e2e-remediate"
		DeferCleanup(func() {
			deleteMigration(name)
			psql(sourceCluster, "ALTER ROLE postgres PASSWORD NULL")
			psql(targetCluster, "ALTER ROLE postgres PASSWORD NULL")
			ensureFollowPrivileges()
			resetSourceReplication()
			resetTargetReplication()
		})

		By("giving postgres a password on both clusters and packing superuser Secrets")
		// CNPG's generated pg_hba ends in a host-all scram rule, so a
		// password-enabled postgres authenticates over the rw service exactly
		// like the app role does.
		supers := map[string]string{name + "-super-src": sourceCluster, name + "-super-tgt": targetCluster}
		for secName, cluster := range supers {
			pw := secName + "-pw"
			psql(cluster, fmt.Sprintf("ALTER ROLE postgres PASSWORD '%s'", pw))
			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: secName}}
			_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, sec, func() error {
				sec.Data = map[string][]byte{"USER": []byte("postgres"), "PW": []byte(pw)}
				return nil
			})
			Expect(err).NotTo(HaveOccurred(), "failed to store the superuser secret %s", secName)
		}

		By("revoking all three grantable follow rights from the app role")
		psql(sourceCluster, "ALTER ROLE app NOREPLICATION")
		psql(targetCluster, "DO $$ DECLARE f oid; BEGIN FOR f IN SELECT p.oid FROM pg_proc p"+
			" JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'pg_catalog'"+
			" AND p.proname LIKE 'pg_replication_origin%' LOOP"+
			" EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM app', f::regprocedure); END LOOP; END $$")
		psql(targetCluster, "REVOKE SET ON PARAMETER session_replication_role FROM app")

		mig := newFollowMigration(name, v1beta1.CutoverManual)
		mig.Spec.BackoffLimit = 5
		mig.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: name + "-super-src"}
		mig.Spec.Target.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: name + "-super-tgt"}
		create(mig)

		By("waiting for the remediated preflight to clear and streaming to start")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)

		By("checking the applied grants were re-granted and recorded as events")
		Expect(psql(sourceCluster, "SELECT rolreplication::int FROM pg_roles WHERE rolname = 'app'")).To(Equal("1"),
			"remediation must restore the REPLICATION attribute")
		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, got)).To(Succeed())
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
			var alterRole, grantExec bool
			for _, e := range events.Items {
				if e.InvolvedObject.UID != got.UID || e.Reason != "PreflightRemediated" {
					continue
				}
				if strings.Contains(e.Message, `ALTER ROLE "app" REPLICATION`) {
					alterRole = true
				}
				if strings.Contains(e.Message, "GRANT EXECUTE ON FUNCTION") {
					grantExec = true
				}
			}
			g.Expect(alterRole).To(BeTrue(), "no PreflightRemediated event for the REPLICATION attribute")
			g.Expect(grantExec).To(BeTrue(), "no PreflightRemediated event for the origin grants")
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("driving the Manual cutover to completion")
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		approveCutover(name)
		m := waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
		expectCleanupSucceeded(name)
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
		Expect(sourceSlotCount()).To(Equal("0"), "replication slot left behind on the source")
		Expect(targetOriginCount()).To(Equal("0"), "replication origin left behind on the target")
	})

	It("fails preflight naming the missing schema grant", func() {
		const name = "e2e-clonegrant"
		DeferCleanup(func() {
			deleteMigration(name)
			dropLimitedRole()
			resetTargetObjects()
		})

		By("recreating the managed-Postgres matrix: db CREATE granted, schema CREATE missing")
		makeLimitedTarget(name)

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{})
		m.Spec.Target.Username = limitedRole
		m.Spec.Target.PasswordSecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Key:                  passwordKey,
		}
		create(m)

		By("waiting for the clone tier to fail the Migration before any attempt")
		failed := waitFailed(name, "PreflightFailed")
		// format %I leaves simple identifiers unquoted, so the statement
		// reads exactly as an operator would type it.
		Expect(failureMessage(failed)).To(ContainSubstring("GRANT CREATE ON SCHEMA public TO "+limitedRole),
			"the exact missing grant must reach the condition")
		Expect(failureMessage(failed)).To(ContainSubstring("hint: spec.target.superuserSecretRef"),
			"a failed clone right without a superuser ref must hint at the field")
		Expect(failureMessage(failed)).To(ContainSubstring("clone.skip: [dbProperties]"),
			"the non-owner role must also trip the db-properties probe with its way out")
		Expect(failed.Status.Attempts).To(Equal(int32(0)), "a worker attempt started despite the failed preflight")
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, &batchv1.Job{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "worker Job exists despite the failed preflight")
	})

	It("remediates the clone schema grant and completes", func() {
		const name = "e2e-clonegrant-fix"
		DeferCleanup(func() {
			deleteMigration(name)
			psql(targetCluster, "ALTER ROLE postgres PASSWORD NULL")
			dropLimitedRole()
			resetTargetObjects()
		})

		By("recreating the managed-Postgres matrix plus a target superuser Secret")
		makeLimitedTarget(name)
		superPW := name + "-super-pw"
		psql(targetCluster, fmt.Sprintf("ALTER ROLE postgres PASSWORD '%s'", superPW))
		superSec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: name + "-super"}}
		_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, superSec, func() error {
			superSec.Data = map[string][]byte{"USER": []byte("postgres"), "PW": []byte(superPW)}
			return nil
		})
		Expect(err).NotTo(HaveOccurred(), "failed to store the superuser secret")

		// noOwner/noACL is the documented non-superuser restore shape, and the
		// db-properties step stays skipped because a grant cannot confer
		// database ownership.
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			NoOwner: true,
			NoACL:   true,
			Skip:    []v1beta1.SkipOption{"dbProperties"},
		})
		m.Spec.Target.Username = limitedRole
		m.Spec.Target.PasswordSecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Key:                  passwordKey,
		}
		m.Spec.Target.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: name + "-super"}
		create(m)

		By("waiting for the remediated clone to complete")
		waitCompleted(name, nsE2E)
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
		Expect(psql(targetCluster, "SELECT has_schema_privilege('"+limitedRole+"', 'public', 'CREATE')::int")).To(Equal("1"),
			"remediation must leave the schema grant in place")

		By("checking the applied grant was recorded as a PreflightRemediated event")
		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, got)).To(Succeed())
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
			var schemaGrant bool
			for _, e := range events.Items {
				if e.InvolvedObject.UID == got.UID && e.Reason == "PreflightRemediated" &&
					strings.Contains(e.Message, "GRANT CREATE ON SCHEMA public TO "+limitedRole) {
					schemaGrant = true
				}
			}
			g.Expect(schemaGrant).To(BeTrue(), "no PreflightRemediated event for the schema grant")
		}, time.Minute, 2*time.Second).Should(Succeed())
	})

	It("fails fast when the clone hits a permission error it cannot fix", func() {
		const name = "e2e-permdenied"
		DeferCleanup(func() {
			deleteMigration(name)
			psql(sourceCluster, "DROP ROLE IF EXISTS e2e_noselect")
		})

		By("creating a source role that can connect but not read")
		// The clone tier probes target rights only, so a source role without
		// SELECT still passes the gate and fails the copy itself: exactly the
		// deterministic permission error the classifier must terminate on the
		// first attempt instead of burning the budget.
		psql(sourceCluster, "DROP ROLE IF EXISTS e2e_noselect")
		psql(sourceCluster, "CREATE ROLE e2e_noselect LOGIN PASSWORD 'e2e-noselect-pw'")
		copySecret(nsE2E, name, []byte("e2e-noselect-pw"))

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{})
		m.Spec.Source.Username = "e2e_noselect"
		m.Spec.Source.PasswordSecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Key:                  passwordKey,
		}
		m.Spec.BackoffLimit = 3
		create(m)

		By("waiting for the terminal permission failure after a single attempt")
		failed := waitFailed(name, "PermissionDenied")
		Expect(failed.Status.Attempts).To(Equal(int32(1)), "the classifier must stop after the first attempt")
		Expect(failureMessage(failed)).To(ContainSubstring("permission denied"))

		By("checking the budget stays unspent: no second attempt within a full poll interval")
		Consistently(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, cur)).To(Succeed())
			g.Expect(cur.Status.Phase).To(Equal(v1beta1.PhaseFailed))
			g.Expect(cur.Status.Attempts).To(Equal(int32(1)))
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-2"}, &batchv1.Job{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"a second attempt Job appeared despite the permission classification")
		}, 45*time.Second, 5*time.Second).Should(Succeed())
	})

	It("exhausts the retry budget on a failure the classifier does not know", func() {
		const name = "e2e-backoff"
		DeferCleanup(func() {
			deleteMigration(name)
			resetTargetObjects()
		})

		By("planting a conflicting target table so pg_restore fails outside the permission class")
		// Without dropIfExists pg_restore cannot replace an existing table,
		// and the incompatible definition keeps every retry failing the same
		// way; "already exists" is outside the classifier's class, so the
		// budget burns exactly as before the classifier existed. A tiny work
		// volume cannot carry this spec: a clone's work dir holds only
		// catalogs and schema dumps (see the chaos suite's spool note).
		resetTargetObjects()
		psql(targetCluster, "CREATE TABLE public.customers (mismatch int)")

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{})
		m.Spec.BackoffLimit = 1
		create(m)

		By("waiting for the terminal failure after exactly two attempts")
		failed := waitFailed(name, "BackoffLimitExceeded")
		Expect(failed.Status.Attempts).To(Equal(int32(2)))
		Expect(failureMessage(failed)).NotTo(ContainSubstring("permission denied"),
			"the budget carrier must fail on a class the classifier ignores")

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

// limitedRole mirrors the managed-Postgres application role from issue #119:
// LOGIN and CONNECT, CREATE on the database, USAGE but not CREATE on public.
const limitedRole = "e2e_limited"

// makeLimitedTarget rebuilds the target's public schema in the PostgreSQL 15+
// managed shape (owned by pg_database_owner, USAGE for PUBLIC, no CREATE) and
// creates the limited role with its password Secret. The audit schema is
// dropped so only public exists on the target for the schema probe.
func makeLimitedTarget(secretName string) {
	GinkgoHelper()
	dropLimitedRole()
	// The canonical reset also unlinks large objects: earlier completed
	// migrations leave OID-preserved blobs that would collide with the
	// no-dropIfExists remediation clone (pg_restore only drops what the
	// incoming dump carries).
	resetTargetObjects()
	psql(targetCluster, "GRANT USAGE ON SCHEMA public TO PUBLIC")
	pw := secretName + "-pw"
	psql(targetCluster, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", limitedRole, pw))
	psql(targetCluster, fmt.Sprintf("GRANT CONNECT, CREATE ON DATABASE %s TO %s", appDB, limitedRole))
	copySecret(nsE2E, secretName, []byte(pw))
}

// dropLimitedRole removes the limited role and everything it restored. The
// DO block guards DROP OWNED, which unlike DROP ROLE has no IF EXISTS.
func dropLimitedRole() {
	GinkgoHelper()
	psql(targetCluster, fmt.Sprintf("DO $$ BEGIN IF EXISTS (SELECT FROM pg_roles WHERE rolname = '%s')"+
		" THEN EXECUTE 'DROP OWNED BY %s CASCADE'; END IF; END $$", limitedRole, limitedRole))
	psql(targetCluster, "DROP ROLE IF EXISTS "+limitedRole)
}

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
			// Every runner Job the suite produces comes through here, so this
			// is the one place that decides where the workers land and what
			// the scheduler thinks they cost.
			Runner: v1beta1.RunnerSpec{
				Resources: workerResources(fixtureCPU, fixtureMemory),
				Affinity:  fixtureAntiAffinity(),
			},
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
func rowCounts(cluster string) string {
	GinkgoHelper()
	return psql(cluster, "SELECT (SELECT count(*) FROM customers) || '/' ||"+
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
func seedTableCounts(cluster string) string {
	GinkgoHelper()
	return psql(cluster, seedCountSQL)
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
func matviewState(cluster string) string {
	GinkgoHelper()
	return psql(cluster, "SELECT ispopulated || '/' || (SELECT count(*) FROM event_daily_counts)"+
		" FROM pg_matviews WHERE matviewname = 'event_daily_counts'")
}

// largeObjectCount counts large objects; pg_largeobject_metadata is
// per-database, so this sees exactly the fixture LOs.
func largeObjectCount(cluster string) string {
	GinkgoHelper()
	return psql(cluster, "SELECT count(*) FROM pg_largeobject_metadata")
}

// sequenceValues flattens every sequence and its last_value into one line, so
// source and target compare as plain strings.
func sequenceValues(cluster string) string {
	GinkgoHelper()
	return psql(cluster, "SELECT string_agg(schemaname || '.' || sequencename || '=' ||"+
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
	return psql(sourceCluster, "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgcopydb%'")
}

// targetOriginCount counts pgcopydb-created replication origins on the target.
// stream cleanup drops the generated origin (alpha.11's cleanup-origin fix), so
// a completed or deleted follow migration must leave none behind.
// pg_replication_origin is a shared catalog, so the app-database connection
// sees it.
func targetOriginCount() string {
	GinkgoHelper()
	return psql(targetCluster, "SELECT count(*) FROM pg_replication_origin WHERE roname LIKE 'pgcopydb%'")
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
