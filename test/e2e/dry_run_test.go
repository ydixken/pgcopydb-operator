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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// targetUserTables counts tables outside the system schemas, so an empty
// target proves no worker ran.
const targetUserTables = "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace" +
	" WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema')" +
	" AND n.nspname NOT LIKE 'pg_toast%'"

var _ = Describe("Dry run", func() {
	BeforeEach(func() {
		DeferCleanup(resetTargetObjects)
		resetTargetObjects()
	})

	It("runs the preflight and stops without copying anything", func() {
		const name = "e2e-dry-run"
		DeferCleanup(func() { deleteMigration(name) })

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{})
		m.Spec.DryRun = true
		create(m)

		expectDryRunSucceeded(waitCompleted(name, nsE2E))
		Expect(psql(targetCluster, targetUserTables)).To(Equal("0"), "a dry run copied tables to the target")
	})

	It("reports the schema grant a superuser would apply and leaves it unapplied", func() {
		const name = "e2e-dry-run-grant"
		DeferCleanup(func() {
			deleteMigration(name)
			clearSuperuserPassword(targetCluster, name+"-super")
			dropLimitedRole()
		})

		By("recreating the managed-Postgres matrix plus a target superuser Secret")
		makeLimitedTarget(name)
		packSuperuserSecret(targetCluster, name+"-super")

		// The same spec the remediation spec completes with, so the schema
		// grant is the only right the limited role lacks.
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
		m.Spec.DryRun = true
		create(m)

		completed := waitCompleted(name, nsE2E)
		expectDryRunSucceeded(completed)
		Expect(psql(targetCluster, "SELECT has_schema_privilege('"+limitedRole+"', 'public', 'CREATE')::int")).To(Equal("0"),
			"a dry run applied the schema grant")

		By("checking the grant was reported as PreflightWouldRemediate, never as PreflightRemediated")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
			var reported bool
			for _, e := range events.Items {
				if e.InvolvedObject.UID != completed.UID {
					continue
				}
				g.Expect(e.Reason).NotTo(Equal("PreflightRemediated"), "a dry run reported an applied grant: %s", e.Message)
				if e.Reason == "PreflightWouldRemediate" &&
					strings.Contains(e.Message, "GRANT CREATE ON SCHEMA public TO "+limitedRole) {
					reported = true
				}
			}
			g.Expect(reported).To(BeTrue(), "no PreflightWouldRemediate event for the schema grant")
		}, time.Minute, 2*time.Second).Should(Succeed())
	})

	// The fixtures run the source at 60s, so the 5s comes from the URI alone:
	// the warning proves preflight reads the value the worker's URI yields.
	It("warns about a wal_sender_timeout below 60s set in the source URI and still passes", func() {
		const name = "e2e-dry-run-wst"
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: name}}
		DeferCleanup(func() {
			deleteMigration(name)
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, secret))).To(Succeed())
		})

		uri := appURL(sourceCluster, appPassword(sourceCluster))
		sep := "?"
		if strings.Contains(uri, "?") {
			sep = "&"
		}
		_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, secret, func() error {
			secret.Data = map[string][]byte{sourceKey: []byte(uri + sep + "options=-c%20wal_sender_timeout%3D5s")}
			return nil
		})
		Expect(err).NotTo(HaveOccurred(), "failed to store the source URI Secret")

		m := newFollowMigration(name, v1beta1.CutoverManual)
		m.Spec.Source = v1beta1.PostgresConnection{URISecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: sourceKey}}
		m.Spec.DryRun = true
		create(m)

		completed := waitCompleted(name, nsE2E)
		expectDryRunSucceeded(completed)

		By("checking the PreflightWarning event names the value and the URI remedy")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
			var warned bool
			for _, e := range events.Items {
				if e.InvolvedObject.UID == completed.UID && e.Reason == "PreflightWarning" &&
					e.Type == corev1.EventTypeWarning &&
					strings.Contains(e.Message, "source wal_sender_timeout is 5s") &&
					strings.Contains(e.Message, "options=-c wal_sender_timeout=60s") {
					warned = true
				}
			}
			g.Expect(warned).To(BeTrue(), "no PreflightWarning event for the 5s wal_sender_timeout")
		}, time.Minute, 2*time.Second).Should(Succeed())
	})
})

// expectDryRunSucceeded checks the shape every passed dry run ends in: the
// DryRunSucceeded verdict, no attempt counted, and no worker Job created.
func expectDryRunSucceeded(m *v1beta1.Migration) {
	GinkgoHelper()
	c := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionComplete)
	Expect(c).NotTo(BeNil(), "Complete condition missing on %s", m.Name)
	Expect(c.Status).To(Equal(metav1.ConditionTrue))
	Expect(c.Reason).To(Equal("DryRunSucceeded"))
	Expect(m.Status.Attempts).To(BeZero(), "a dry run counted a worker attempt")
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: m.Name + "-run-1"}, &batchv1.Job{})
	Expect(apierrors.IsNotFound(err)).To(BeTrue(), "a dry run created the worker Job")
}
