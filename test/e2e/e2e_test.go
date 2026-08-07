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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

// fixtureCounts is customers/orders/audit.events as seeded by fixtureSQL.
const fixtureCounts = "50000/200000/1000"

// The scenarios share the two CNPG fixtures and run in order: later ones
// build on the populated target that earlier ones leave behind.
var _ = Describe("Migration", Ordered, func() {
	It("completes a fresh clone with matching rows and sequences", func() {
		create(newMigration("e2e-fresh", nsE2E, v1alpha1.CloneOptions{}))
		m := waitCompleted("e2e-fresh", nsE2E)
		Expect(m.Status.Attempts).To(Equal(int32(1)))

		By("comparing row counts and sequence values on both sides")
		Expect(rowCounts(srcPod)).To(Equal(fixtureCounts))
		Expect(rowCounts(tgtPod)).To(Equal(fixtureCounts))
		Expect(sequenceValues(tgtPod)).To(Equal(sequenceValues(srcPod)))
	})

	It("re-clones onto the populated target with dropIfExists", func() {
		create(newMigration("e2e-reclone", nsE2E, v1alpha1.CloneOptions{DropIfExists: true}))
		m := waitCompleted("e2e-reclone", nsE2E)
		Expect(m.Status.Attempts).To(Equal(int32(1)))
	})

	It("excludes filtered schemas from the clone", func() {
		// The filtered dump carries no DROP for excluded objects, so the audit
		// schema left by the previous scenario must go by hand for the
		// absence check to mean anything.
		By("dropping the audit schema on the target")
		psql(tgtPod, "DROP SCHEMA IF EXISTS audit CASCADE")

		create(newMigration("e2e-filters", nsE2E, v1alpha1.CloneOptions{
			DropIfExists: true,
			Filters:      &v1alpha1.Filters{ExcludeSchemas: []string{"audit"}},
		}))
		waitCompleted("e2e-filters", nsE2E)

		By("checking audit stayed away while public tables arrived")
		Expect(psql(tgtPod, "SELECT to_regclass('audit.events') IS NULL")).To(Equal("t"))
		Expect(psql(tgtPod, "SELECT count(*) FROM customers")).To(Equal("50000"))
		Expect(psql(tgtPod, "SELECT count(*) FROM orders")).To(Equal("200000"))
	})

	It("resumes with a second attempt after the runner pod dies", func() {
		create(newMigration("e2e-resume", nsE2E, v1alpha1.CloneOptions{DropIfExists: true}))

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

		Expect(rowCounts(tgtPod)).To(Equal(fixtureCounts))
	})

	It("clones across namespaces using local secrets and remote hosts", func() {
		// Stand-in for cross-cluster: the Migration and its secrets live in
		// one namespace, the databases are reached by service DNS in another.
		By("copying the app secrets into " + nsX)
		for _, name := range []string{srcSecret, tgtSecret} {
			orig := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, orig)).To(Succeed())
			copySecret(nsX, name, orig.Data["password"])
		}

		create(newMigration("e2e-xns", nsX, v1alpha1.CloneOptions{DropIfExists: true}))
		waitCompleted("e2e-xns", nsX)
	})

	It("rejects a spec with both uriSecretRef and inline host", func() {
		m := newMigration("e2e-invalid", nsE2E, v1alpha1.CloneOptions{})
		m.Spec.Source.URISecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "does-not-matter"},
			Key:                  "uri",
		}
		err := k8sClient.Create(ctx, m)
		Expect(err).To(HaveOccurred(), "CEL validation should reject uriSecretRef combined with host")
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an Invalid API error, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("not both"))
	})
})

// e2eConn points at a fixture cluster through its rw service, fully qualified
// so the same spec works from pgcopydb-e2e-x.
func e2eConn(cluster string) v1alpha1.PostgresConnection {
	return v1alpha1.PostgresConnection{
		Host:     cluster + "-rw." + nsE2E + ".svc",
		Database: appDB,
		Username: appDB,
		PasswordSecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: cluster + "-app"},
			Key:                  "password",
		},
	}
}

func newMigration(name, ns string, clone v1alpha1.CloneOptions) *v1alpha1.Migration {
	return &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.MigrationSpec{
			Source: e2eConn(sourceCluster),
			Target: e2eConn(targetCluster),
			Clone:  clone,
			// Small work volume: the fixture dump is a few hundred MB at most.
			WorkVolume: v1alpha1.WorkVolume{Size: resource.MustParse("2Gi")},
		},
	}
}

func create(m *v1alpha1.Migration) {
	GinkgoHelper()
	Expect(k8sClient.Create(ctx, m)).To(Succeed(), "failed to create Migration %s/%s", m.Namespace, m.Name)
}

// waitCompleted waits for phase Completed and bails out early on Failed so a
// broken run reports the operator's failure message instead of a timeout.
func waitCompleted(name, ns string) *v1alpha1.Migration {
	GinkgoHelper()
	m := &v1alpha1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, m)).To(Succeed())
		if m.Status.Phase == v1alpha1.PhaseFailed {
			StopTrying(fmt.Sprintf("migration %s/%s failed: %s", ns, name, failureMessage(m))).Now()
		}
		g.Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCompleted),
			"migration %s/%s at phase %s, attempts %d", ns, name, m.Status.Phase, m.Status.Attempts)
	}, migrationTimeout, 2*time.Second).Should(Succeed())
	return m
}

func failureMessage(m *v1alpha1.Migration) string {
	if c := apimeta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionFailed); c != nil {
		return c.Message
	}
	return "(no Failed condition message)"
}

// rowCounts returns customers/orders/audit.events counts in one round trip.
func rowCounts(pod string) string {
	GinkgoHelper()
	return psql(pod, "SELECT (SELECT count(*) FROM customers) || '/' ||"+
		" (SELECT count(*) FROM orders) || '/' || (SELECT count(*) FROM audit.events)")
}

// sequenceValues flattens every sequence and its last_value into one line, so
// source and target compare as plain strings.
func sequenceValues(pod string) string {
	GinkgoHelper()
	return psql(pod, "SELECT string_agg(schemaname || '.' || sequencename || '=' ||"+
		" coalesce(last_value::text, 'null'), ',' ORDER BY schemaname, sequencename) FROM pg_sequences")
}

// copySecret writes a password-only secret; CreateOrUpdate keeps reruns with
// kept fixtures from tripping over AlreadyExists.
func copySecret(ns, name string, password []byte) {
	GinkgoHelper()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, sec, func() error {
		sec.Data = map[string][]byte{"password": password}
		return nil
	})
	Expect(err).NotTo(HaveOccurred(), "failed to copy secret %s into %s", name, ns)
}
