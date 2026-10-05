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
	"reflect"
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

const partitionFixtureAnnotation = "pgcopydb-operator.io/partition-verification-fixture"

var _ = Describe("Partition verification", func() {
	It("verifies a cloned range parent with populated, empty and default leaves", func() {
		m, schema := newPartitionVerificationMigration()
		create(m)

		completed := waitCompleted(m.Name, nsE2E)
		expectVerification(completed, true)
		for _, cluster := range []string{sourceCluster, targetCluster} {
			Expect(psql(cluster, "SELECT count(*) FROM "+sqlIdent(schema)+".events")).To(Equal("3"))
		}
	})

	It("reports an extra populated target partition even when every source leaf matches", func() {
		m, schema := newPartitionVerificationMigration()
		m.Spec.Suspend = true
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		captured := map[string]comparePodLog{}
		reportVerificationOnFailure(m, captured)
		waitPhase(m.Name, nsE2E, migrationTimeout, v1beta1.PhaseSuspended)

		// A suspended native Job blocks verification without racing the clone or changing status.
		suspended := true
		labels := map[string]string{
			"app.kubernetes.io/managed-by": "pgcopydb-operator",
			migrationLabel:                 m.Name,
		}
		if featureE2ERunValue != "" {
			labels[labelFeatureE2ERun] = featureE2ERunValue
		}
		barrier := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: m.Name + "-compare-schema", Namespace: nsE2E, Labels: labels,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(m, v1beta1.GroupVersion.WithKind("Migration")),
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspended,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{{
							Name: "barrier", Image: "suspended-verification-barrier", Command: []string{"/bin/false"},
						}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, barrier)).To(Succeed())
		Expect(barrier.UID).NotTo(BeEmpty())
		setSuspend(m.Name, false)
		waitWorkVolumeReady(m)
		verifying := waitPhase(m.Name, nsE2E, migrationTimeout, v1beta1.PhaseVerifying)
		expectConditionTrue(verifying, v1beta1.ConditionCloneCompleted)
		expectVerificationJob(verifying, verifying.Status.JobName, batchv1.JobComplete)

		currentBarrier := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(barrier), currentBarrier)).To(Succeed())
		Expect(currentBarrier.UID).To(Equal(barrier.UID))
		Expect(currentBarrier.Spec.Suspend).To(HaveValue(BeTrue()))
		pods := &corev1.PodList{}
		Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
			client.MatchingLabels{"batch.kubernetes.io/controller-uid": string(barrier.UID)})).To(Succeed())
		podCount := len(pods.Items)
		Expect(podCount).To(BeZero(), "suspended verification barrier started a pod")

		By("adding a target member while the real verification jobs are blocked")
		qualified := sqlIdent(schema) + "."
		psql(targetCluster, "SET ROLE "+sqlIdent(appRole(targetCluster))+"; "+
			"CREATE TABLE "+qualified+"events_extra PARTITION OF "+qualified+"events FOR VALUES FROM (20) TO (30); "+
			"INSERT INTO "+qualified+"events VALUES (21, 'extra')")
		Expect(psql(sourceCluster, "SELECT count(*) FROM "+qualified+"events")).To(Equal("3"))
		Expect(psql(targetCluster, "SELECT count(*) FROM "+qualified+"events")).To(Equal("4"))
		for _, leaf := range []struct{ name, payload string }{
			{"events_low", "1:alpha,2:beta"}, {"events_empty", ""}, {"events_default", "100:outside"},
		} {
			query := "SELECT coalesce(string_agg(id::text || ':' || payload, ',' ORDER BY id), '') FROM " +
				qualified + sqlIdent(leaf.name)
			Expect(psql(sourceCluster, query)).To(Equal(leaf.payload))
			Expect(psql(targetCluster, query)).To(Equal(leaf.payload))
		}

		// Delete the inert barrier so only the operator's replacement can supply the verdict.
		policy := metav1.DeletePropagationForeground
		Expect(k8sClient.Delete(ctx, barrier, client.Preconditions{UID: &barrier.UID},
			client.PropagationPolicy(policy))).To(Succeed())
		Eventually(func(g Gomega) {
			job := &batchv1.Job{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(barrier), job)).To(Succeed())
			g.Expect(job.UID).NotTo(Equal(barrier.UID))
			g.Expect(metav1.IsControlledBy(job, m)).To(BeTrue())
			g.Expect(job.Spec.Suspend == nil || !*job.Spec.Suspend).To(BeTrue())
		}, migrationTimeout, time.Second).Should(Succeed())

		completed := waitCompletedCapturing(m, captured)
		expectVerification(completed, false)
		for _, check := range []string{compareSchemaCheck, compareDataCheck} {
			var attempts []string
			for _, attempt := range captured {
				if attempt.check == check {
					attempts = append(attempts, attempt.log)
				}
			}
			Expect(attempts).To(ContainElement(ContainSubstring("Partition topology mismatch")),
				"compare %s must fail for the partition difference", check)
		}
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
			found := false
			for _, event := range events.Items {
				if event.InvolvedObject.UID == m.UID && event.Reason == "VerificationMismatch" &&
					event.Type == corev1.EventTypeWarning {
					found = true
				}
			}
			g.Expect(found).To(BeTrue(), "partition mismatch warning must reference this Migration UID")
		}, time.Minute, 2*time.Second).Should(Succeed())
	})
})

func newPartitionVerificationMigration() (*v1beta1.Migration, string) {
	GinkgoHelper()
	identity := fmt.Sprint(time.Now().UnixNano())
	schema := "partition_verify_" + identity
	useCopyBinary := true
	m := newMigration("e2e-partition-"+identity, nsE2E, v1beta1.CloneOptions{
		Filters:       &v1beta1.Filters{IncludeOnlySchemas: []string{schema}},
		UseCopyBinary: &useCopyBinary,
	})
	// Explicit API defaults keep identity checks valid after an uncertain Create response.
	for _, connection := range []*v1beta1.PostgresConnection{&m.Spec.Source, &m.Spec.Target} {
		if connection.Port == 0 {
			connection.Port = defaultPGPort
		}
		if connection.SSLMode == "" {
			connection.SSLMode = "prefer"
		}
	}
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
	m.Annotations = map[string]string{partitionFixtureAnnotation: identity}
	for _, cluster := range []string{sourceCluster, targetCluster} {
		Expect(psql(cluster, "SELECT count(*) FROM pg_namespace WHERE nspname = "+sqlLiteral(schema))).To(Equal("0"))
	}
	stamp := "pgcopydb-e2e-partition:" + identity
	// Commit the ownership marker with the schema, including when the client loses the response.
	_, createErr := psqlDBErr(sourceCluster, appDatabase(sourceCluster), "BEGIN; "+asSourceAppRole()+
		"CREATE SCHEMA "+sqlIdent(schema)+"; COMMENT ON SCHEMA "+sqlIdent(schema)+" IS "+sqlLiteral(stamp)+"; COMMIT")
	ownerQuery := "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = " + sqlLiteral(schema)
	stampQuery := "SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = " + sqlLiteral(schema)
	DeferCleanup(func() {
		current := &v1beta1.Migration{}
		key := client.ObjectKeyFromObject(m)
		err := k8sClient.Get(ctx, key, current)
		if err != nil {
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "cannot prove whether the fixture Migration exists")
		} else {
			Expect(current.Annotations[partitionFixtureAnnotation]).To(Equal(identity),
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
		for _, cluster := range []string{sourceCluster, targetCluster} {
			if psql(cluster, "SELECT count(*) FROM pg_namespace WHERE nspname = "+sqlLiteral(schema)) == "0" {
				continue
			}
			Expect(psql(cluster, ownerQuery)).To(Equal(appRole(cluster)), "refusing to drop a schema with another owner")
			Expect(psql(cluster, stampQuery)).To(Equal(stamp), "refusing to drop a schema without this fixture marker")
			psql(cluster, "SET ROLE "+sqlIdent(appRole(cluster))+"; DROP SCHEMA "+sqlIdent(schema)+" CASCADE")
		}
	})
	Expect(createErr).NotTo(HaveOccurred(), "failed to create the stamped partition fixture")
	Expect(psql(sourceCluster, ownerQuery)).To(Equal(appRole(sourceCluster)))
	Expect(psql(sourceCluster, stampQuery)).To(Equal(stamp))

	qualified := sqlIdent(schema) + "."
	psql(sourceCluster, asSourceAppRole()+
		"CREATE TABLE "+qualified+"events (id integer NOT NULL, payload text NOT NULL) PARTITION BY RANGE (id); "+
		"CREATE TABLE "+qualified+"events_low PARTITION OF "+qualified+"events FOR VALUES FROM (0) TO (10); "+
		"CREATE TABLE "+qualified+"events_empty PARTITION OF "+qualified+"events FOR VALUES FROM (10) TO (20); "+
		"CREATE TABLE "+qualified+"events_default PARTITION OF "+qualified+"events DEFAULT; "+
		"INSERT INTO "+qualified+"events VALUES (1, 'alpha'), (2, 'beta'), (100, 'outside')")
	Expect(psql(sourceCluster, "SELECT count(*) = 4 AND bool_and(pg_get_userbyid(c.relowner) = "+
		sqlLiteral(appRole(sourceCluster))+") FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "+
		"WHERE n.nspname = "+sqlLiteral(schema)+" AND c.relkind IN ('r', 'p')")).To(Equal("t"))
	return m, schema
}
