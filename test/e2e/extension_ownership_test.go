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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

var _ = Describe("Extension ownership", func() {
	const installed = "SELECT EXISTS (SELECT FROM pg_extension WHERE extname = 'hstore')"
	const owner = "SELECT pg_get_userbyid(extowner) FROM pg_extension WHERE extname = 'hstore'"
	const comment = "SELECT obj_description(oid, 'pg_extension') FROM pg_extension WHERE extname = 'hstore'"
	const targetComment = "e2e target extension comment"

	BeforeEach(func() {
		Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		DeferCleanup(resetTargetObjects)
		resetTargetObjects()

		By("installing hstore on both clusters")
		for _, cluster := range []string{sourceCluster, targetCluster} {
			Expect(psql(cluster, "SELECT EXISTS (SELECT FROM pg_available_extensions "+
				"WHERE name = 'hstore' AND default_version IS NOT NULL)")).To(Equal("t"))
			Expect(psql(cluster, installed)).To(Equal("f"), "hstore was left on %s", cluster)
			DeferCleanup(func() {
				psql(cluster, "DROP EXTENSION IF EXISTS hstore")
				Expect(psql(cluster, installed)).To(Equal("f"))
			})
		}
		psql(sourceCluster, "SET ROLE app; CREATE EXTENSION hstore WITH SCHEMA public; "+
			"COMMENT ON EXTENSION hstore IS 'e2e source extension comment'")
		// psql already runs as in-pod postgres; only the migration needs network credentials.
		psql(targetCluster, "CREATE EXTENSION hstore WITH SCHEMA public; "+
			"COMMENT ON EXTENSION hstore IS '"+targetComment+"'")

		By("proving the selected extension has a comment and an inaccessible target owner")
		Expect(psql(sourceCluster, owner)).To(Equal(appDB))
		Expect(psql(sourceCluster, comment)).To(Equal("e2e source extension comment"))
		Expect(psql(targetCluster, owner)).To(Equal(postgresContainer))
		Expect(psql(targetCluster, "SELECT pg_get_userbyid(datdba) FROM pg_database "+
			"WHERE datname = current_database()")).To(Equal(appDB))
		Expect(psql(targetCluster, "SELECT rolsuper OR pg_has_role('app', 'postgres', 'USAGE') "+
			"FROM pg_roles WHERE rolname = 'app'")).To(Equal("f"))
	})

	for _, drop := range []bool{false, true} {
		It(fmt.Sprintf("rejects administrator-owned extensions before any worker (dropIfExists: %t)", drop), func() {
			name := fmt.Sprintf("e2e-extension-owner-drop-%t", drop)
			DeferCleanup(func() { deleteMigration(name) })

			create(newMigration(name, nsE2E, v1beta1.CloneOptions{DropIfExists: drop}))

			failed := waitFailed(name, "PreflightFailed")
			msg := failureMessage(failed)
			route, skip := "COMMENT ON EXTENSION", "extensionComments"
			if drop {
				route, skip = "DROP EXTENSION", "extensions"
			}
			Expect(msg).To(ContainSubstring("target extension ownership required for " + route))
			Expect(msg).To(ContainSubstring(fmt.Sprintf("(dropIfExists: %t)", drop)))
			Expect(msg).To(ContainSubstring("extension hstore (owner postgres, migration role app)"))
			Expect(msg).To(ContainSubstring("clone.skip: [" + skip + "]"))
			Expect(failed.Status.Attempts).To(Equal(int32(0)), "an ownership rejection started a worker attempt")
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, &batchv1.Job{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "an ownership rejection has a worker Job")
			Expect(psql(targetCluster, "SELECT to_regclass('public.customers') IS NULL")).To(Equal("t"))
		})
	}

	It("clones past administrator-owned extensions when extension comments are skipped", func() {
		const name = "e2e-extension-owner-skip-comments"
		DeferCleanup(func() { deleteMigration(name) })

		create(newMigration(name, nsE2E, v1beta1.CloneOptions{
			Skip: []v1beta1.SkipOption{"extensionComments"},
		}))
		completed := waitCompleted(name, nsE2E)
		expectConditionTrue(completed, v1beta1.ConditionValidated)
		Expect(completed.Status.Attempts).To(Equal(int32(1)))
		Expect(psql(sourceCluster, "SELECT EXISTS (SELECT FROM customers)")).To(Equal("t"))
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
		Expect(psql(targetCluster, owner)).To(Equal(postgresContainer))
		Expect(psql(targetCluster, comment)).To(Equal(targetComment))
	})
})
