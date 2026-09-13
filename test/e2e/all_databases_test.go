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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

var _ = Describe("All databases", func() {
	It("clones all databases as superuser, including a newly created database", func() {
		const name = "e2e-all-databases"
		const extraDB = "e2e_all_databases"
		sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: name}}
		Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		DeferCleanup(func() {
			deleteMigration(name)
			for _, cluster := range []string{sourceCluster, targetCluster} {
				psqlDB(cluster, postgresContainer, "DROP DATABASE IF EXISTS "+extraDB+" WITH (FORCE)")
				psql(cluster, "ALTER ROLE postgres PASSWORD NULL")
			}
			resetTargetObjects()
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, sec))).To(Succeed())
		})

		By("preparing an empty target and temporary postgres credentials on both clusters")
		resetTargetObjects()
		pw := name + "-pw"
		for _, cluster := range []string{sourceCluster, targetCluster} {
			psql(cluster, fmt.Sprintf("ALTER ROLE postgres PASSWORD '%s'", pw))
		}
		_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, sec, func() error {
			sec.Data = map[string][]byte{passwordKey: []byte(pw)}
			return nil
		})
		Expect(err).NotTo(HaveOccurred(), "failed to store temporary postgres credentials")

		By("seeding a source database that pgcopydb must create on the target")
		psqlDB(sourceCluster, postgresContainer, "CREATE DATABASE "+extraDB)
		psqlDB(sourceCluster, extraDB, "CREATE TABLE all_database_rows (id integer PRIMARY KEY); "+
			"INSERT INTO all_database_rows VALUES (1), (2), (3)")
		Expect(psqlDB(targetCluster, postgresContainer, "SELECT count(*) FROM pg_database WHERE datname = '"+extraDB+"'")).
			To(Equal("0"))

		mig := newMigration(name, nsE2E, v1beta1.CloneOptions{AllDatabases: true})
		for _, connection := range []*v1beta1.PostgresConnection{&mig.Spec.Source, &mig.Spec.Target} {
			connection.Database = postgresContainer
			connection.Username = postgresContainer
			connection.PasswordSecretRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: passwordKey,
			}
		}
		create(mig)
		waitCompleted(name, nsE2E)
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
		Expect(psqlDB(targetCluster, extraDB, "SELECT count(*) FROM all_database_rows")).To(Equal("3"))
	})

	It("rejects all databases as app before starting a worker", func() {
		const name = "e2e-all-databases-app"
		DeferCleanup(func() { deleteMigration(name) })

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{AllDatabases: true})
		m.Spec.Source.Database = postgresContainer
		m.Spec.Target.Database = postgresContainer
		create(m)

		failed := waitFailed(name, "PreflightFailed")
		msg := failureMessage(failed)
		Expect(msg).To(ContainSubstring("all-databases source requires a superuser"))
		Expect(msg).To(ContainSubstring("all-databases target requires a superuser"))
		Expect(failed.Status.Attempts).To(Equal(int32(0)), "a rejected all-databases clone started a worker attempt")
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, &batchv1.Job{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "a rejected all-databases clone has a worker Job")
	})
})
