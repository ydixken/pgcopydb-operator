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
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// fixtureAnnotation carries the identity a stamped fixture's cleanup checks.
const fixtureAnnotation = "pgcopydb-operator.io/e2e-fixture"

// createStampedSchemas creates schemas on the source as the app role, each
// stamped in the same transaction, and registers the cleanup that deletes m,
// then calls beforeDrop (if set) and drops each schema whose owner and stamp match.
func createStampedSchemas(
	m *v1beta1.Migration, identity, stamp string, schemas []string, beforeDrop func(cluster, schema string),
) {
	GinkgoHelper()
	Expect(schemas).NotTo(BeEmpty())
	// Explicit API defaults keep identity checks valid after an uncertain Create response.
	for _, connection := range []*v1beta1.PostgresConnection{&m.Spec.Source, &m.Spec.Target} {
		if connection.Port == 0 {
			connection.Port = defaultPGPort
		}
		if connection.SSLMode == "" {
			connection.SSLMode = "prefer"
		}
	}
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	m.Annotations[fixtureAnnotation] = identity
	names := sqlTextArray(schemas)
	for _, cluster := range []string{sourceCluster, targetCluster} {
		Expect(psql(cluster, "SELECT count(*) FROM pg_namespace WHERE nspname = ANY ("+names+")")).To(Equal("0"))
	}
	// Commit the ownership marker with the schema, including when the client loses the response.
	_, createErr := psqlDBErr(sourceCluster, appDatabase(sourceCluster),
		stampedSchemasSQL(appRole(sourceCluster), schemas, stamp))
	ownerQuery := func(schema string) string {
		return "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = " + sqlLiteral(schema)
	}
	stampQuery := func(schema string) string {
		return "SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = " + sqlLiteral(schema)
	}
	DeferCleanup(func() {
		deleteFixtureMigration(m, identity)
		for _, cluster := range []string{sourceCluster, targetCluster} {
			for _, schema := range schemas {
				if psql(cluster, "SELECT count(*) FROM pg_namespace WHERE nspname = "+sqlLiteral(schema)) == "0" {
					continue
				}
				Expect(psql(cluster, ownerQuery(schema))).To(Equal(appRole(cluster)),
					"refusing to drop a schema with another owner")
				Expect(psql(cluster, stampQuery(schema))).To(Equal(stamp),
					"refusing to drop a schema without this fixture marker")
				if beforeDrop != nil {
					beforeDrop(cluster, schema)
				}
				psql(cluster, "SET ROLE "+sqlIdent(appRole(cluster))+"; DROP SCHEMA "+sqlIdent(schema)+" CASCADE")
			}
		}
	})
	Expect(createErr).NotTo(HaveOccurred(), "failed to create the stamped fixture schemas")
	for _, schema := range schemas {
		Expect(psql(sourceCluster, ownerQuery(schema))).To(Equal(appRole(sourceCluster)))
		Expect(psql(sourceCluster, stampQuery(schema))).To(Equal(stamp))
	}
}

// stampedSchemasSQL creates schemas as role with stamp as their comment, in
// one transaction, so no schema ever exists without its stamp.
func stampedSchemasSQL(role string, schemas []string, stamp string) string {
	var b strings.Builder
	b.WriteString("BEGIN; SET ROLE " + sqlIdent(role) + "; ")
	for _, schema := range schemas {
		b.WriteString("CREATE SCHEMA " + sqlIdent(schema) + "; COMMENT ON SCHEMA " + sqlIdent(schema) +
			" IS " + sqlLiteral(stamp) + "; ")
	}
	b.WriteString("COMMIT")
	return b.String()
}

// deleteFixtureMigration deletes m only while it still carries identity and
// the fixture's configuration, then waits until its Jobs are gone and its
// pods stopped, so no worker writes into a schema being dropped.
func deleteFixtureMigration(m *v1beta1.Migration, identity string) {
	GinkgoHelper()
	current := &v1beta1.Migration{}
	key := client.ObjectKeyFromObject(m)
	err := k8sClient.Get(ctx, key, current)
	if err != nil {
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "cannot prove whether the fixture Migration exists")
	} else {
		Expect(current.Annotations[fixtureAnnotation]).To(Equal(identity),
			"refusing to delete a Migration with another fixture identity")
		sameConfig := reflect.DeepEqual(current.Spec.Source, m.Spec.Source) &&
			reflect.DeepEqual(current.Spec.Target, m.Spec.Target) &&
			reflect.DeepEqual(current.Spec.Clone, m.Spec.Clone) &&
			reflect.DeepEqual(current.Spec.Verification, m.Spec.Verification)
		Expect(sameConfig).To(BeTrue(), "refusing to delete a Migration with another fixture configuration")
		Expect(requireFeatureMigrationOwnership(current)).To(Succeed())
		if m.UID != "" {
			Expect(current.UID).To(Equal(m.UID), "refusing to delete a replacement Migration")
		}
		Expect(current.UID).NotTo(BeEmpty())
		m.UID = current.UID
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current,
			client.Preconditions{UID: &m.UID}))).To(Succeed())
		Eventually(func(g Gomega) {
			remaining := &v1beta1.Migration{}
			lookupErr := k8sClient.Get(ctx, key, remaining)
			if lookupErr == nil {
				g.Expect(remaining.UID).To(Equal(m.UID), "refusing to stop a replacement Migration")
			}
			g.Expect(apierrors.IsNotFound(lookupErr)).To(BeTrue(), "fixture Migration is still terminating")
		}, 5*time.Minute, time.Second).Should(Succeed())
	}
	Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, &v1beta1.Migration{}))).To(BeTrue(),
		"fixture Migration must be absent before its schemas are dropped")
	Eventually(func(g Gomega) {
		jobs := &batchv1.JobList{}
		g.Expect(k8sClient.List(ctx, jobs, client.InNamespace(nsE2E),
			client.MatchingLabels{migrationLabel: m.Name})).To(Succeed())
		for _, job := range jobs.Items {
			g.Expect(m.UID).NotTo(BeEmpty(), "cannot establish ownership of a remaining fixture Job")
			g.Expect(metav1.IsControlledBy(&job, m)).To(BeTrue(), "refusing to delete a replaced Job")
			g.Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &job, client.Preconditions{UID: &job.UID},
				client.PropagationPolicy(metav1.DeletePropagationForeground)))).To(Succeed())
		}
		pods := &corev1.PodList{}
		g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
			client.MatchingLabels{migrationLabel: m.Name})).To(Succeed())
		for _, pod := range pods.Items {
			g.Expect(pod.Status.Phase).To(BeElementOf(corev1.PodSucceeded, corev1.PodFailed),
				"Migration pod must stop before its fixture is dropped")
		}
		jobCount := len(jobs.Items)
		g.Expect(jobCount).To(BeZero(), "owned Migration Jobs must be removed before dropping the fixture")
	}, 5*time.Minute, time.Second).Should(Succeed())
}

func TestStampedSchemasSQLIsAtomic(t *testing.T) {
	run := testPSQL(t)
	id := fmt.Sprint(time.Now().UnixNano())
	taken, first, second := "stamp_taken_"+id, "stamp_first_"+id, "stamp_second_"+id
	testCoverageSchemas(t, run, taken)
	t.Cleanup(func() {
		testQuery(t, run, "DROP SCHEMA IF EXISTS "+sqlIdent(first)+", "+sqlIdent(second))
	})
	// A superuser would make SET ROLE a no-op and the owner check vacuous.
	role := testCaseOwner
	onDatabase := func(sql string) string {
		return "DO $g$ BEGIN EXECUTE format(" + sqlLiteral(sql) + ", current_database(), " + sqlLiteral(role) + "); END $g$"
	}
	testQuery(t, run, onDatabase("GRANT CREATE ON DATABASE %I TO %I"))
	t.Cleanup(func() { testQuery(t, run, onDatabase("REVOKE CREATE ON DATABASE %I FROM %I")) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := run(ctx, stampedSchemasSQL(role, []string{first, taken}, "stamp:"+id)); err == nil {
		t.Fatal("creating an existing schema succeeded")
	}
	// format renders a missing stamp as empty, where || would hide the whole row.
	found := "SELECT coalesce(string_agg(format('%s %s %s', nspname, pg_get_userbyid(nspowner)," +
		" obj_description(oid, 'pg_namespace')), ',' ORDER BY nspname), '') FROM pg_namespace" +
		" WHERE nspname IN (" + sqlLiteral(first) + ", " + sqlLiteral(second) + ")"
	if got := testQuery(t, run, found); got != "" {
		t.Fatalf("a failed create left %q behind", got)
	}
	testQuery(t, run, stampedSchemasSQL(role, []string{first, second}, "stamp:"+id))
	want := first + " " + role + " stamp:" + id + "," + second + " " + role + " stamp:" + id
	if got := testQuery(t, run, found); got != want {
		t.Fatalf("created %q, want %q", got, want)
	}
}
