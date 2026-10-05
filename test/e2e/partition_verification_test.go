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
	"maps"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	partitionMigrationLabel    = "pgcopydb-operator.io/migration"
	partitionFixtureAnnotation = "pgcopydb-operator.io/partition-verification-fixture"
)

var _ = Describe("Partition verification", func() {
	It("verifies a cloned range parent with populated, empty and default leaves", func() {
		m, schema := newPartitionVerificationMigration()
		create(m)

		completed := waitCompleted(m.Name, nsE2E)
		expectPartitionVerification(completed, true)
		for _, cluster := range []string{sourceCluster, targetCluster} {
			Expect(psql(cluster, "SELECT count(*) FROM "+sqlIdent(schema)+".events")).To(Equal("3"))
		}
	})

	It("reports an extra populated target partition even when every source leaf matches", func() {
		m, schema := newPartitionVerificationMigration()
		m.Spec.Suspend = true
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		captured := map[string]comparePodLog{}
		// Registered after the fixture cleanup so it runs first, while the Jobs still exist.
		DeferCleanup(func() {
			if CurrentSpecReport().Failed() {
				AddReportEntry("partition verification before cleanup",
					partitionDiagnostics(ctx, k8sClient, m, captured), ReportEntryVisibilityFailureOrVerbose)
			}
		})
		waitPhase(m.Name, nsE2E, migrationTimeout, v1beta1.PhaseSuspended)

		// A suspended native Job blocks verification without racing the clone or changing status.
		suspended := true
		labels := map[string]string{
			"app.kubernetes.io/managed-by": "pgcopydb-operator",
			partitionMigrationLabel:        m.Name,
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
		expectPartitionJob(verifying, verifying.Status.JobName, batchv1.JobComplete)

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

		completed := &v1beta1.Migration{}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), completed)).To(Succeed())
			// Read after the phase: a pod the operator counted as finished is terminal by now.
			captureComparePodLogs(ctx, k8sClient, exec.CommandContext, m.Name, captured)
			if completed.Status.Phase == v1beta1.PhaseFailed {
				StopTrying("migration failed: " + failureMessage(completed)).Now()
			}
			g.Expect(completed.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
		}, migrationTimeout, time.Second).Should(Succeed())
		// Retries only reads that failed on the last poll.
		captureComparePodLogs(ctx, k8sClient, exec.CommandContext, m.Name, captured)
		expectPartitionVerification(completed, false)
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
				client.MatchingLabels{partitionMigrationLabel: m.Name})).To(Succeed())
			for _, job := range jobs.Items {
				g.Expect(m.UID).NotTo(BeEmpty(), "cannot establish ownership of a remaining fixture Job")
				g.Expect(metav1.IsControlledBy(&job, m)).To(BeTrue(), "refusing to delete a replaced Job")
				g.Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &job, client.Preconditions{UID: &job.UID},
					client.PropagationPolicy(metav1.DeletePropagationForeground)))).To(Succeed())
			}
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
				client.MatchingLabels{partitionMigrationLabel: m.Name})).To(Succeed())
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

// comparePodLog is one compare attempt's log, or the error of the last read.
type comparePodLog struct {
	check, log string
	err        error
}

// captureComparePodLogs keeps each finished compare pod's log, keyed by pod name.
// A storage remount can delete finished pods before the spec reads them, so the
// spec reads every attempt as soon as it ends rather than through the Job later.
func captureComparePodLogs(
	parent context.Context, c client.Client, command commandFactory, migration string, logs map[string]comparePodLog,
) {
	for _, check := range []string{compareSchemaCheck, compareDataCheck} {
		pods := &corev1.PodList{}
		if err := c.List(parent, pods, client.InNamespace(nsE2E),
			client.MatchingLabels{batchv1.JobNameLabel: migration + "-compare-" + check}); err != nil {
			continue
		}
		for _, pod := range pods.Items {
			done := pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
			if previous, seen := logs[pod.Name]; !done || (seen && previous.err == nil) {
				continue
			}
			logCtx, cancel := context.WithTimeout(parent, e2eCommandTimeout)
			out, err := commandOutput(logCtx, command, "kubectl", "logs", "-n", nsE2E, "pod/"+pod.Name, "--tail=-1")
			cancel()
			logs[pod.Name] = comparePodLog{check: check, log: string(out), err: err}
		}
	}
}

// partitionDiagnostics reports rather than asserts, so a missing object explains
// the failure instead of replacing it.
func partitionDiagnostics(
	parent context.Context, c client.Client, m *v1beta1.Migration, logs map[string]comparePodLog,
) string {
	diagCtx, cancel := context.WithTimeout(parent, e2eCommandTimeout)
	defer cancel()
	var b strings.Builder
	owned := client.MatchingLabels{partitionMigrationLabel: m.Name}
	jobs := &batchv1.JobList{}
	if err := c.List(diagCtx, jobs, client.InNamespace(nsE2E), owned); err != nil {
		fmt.Fprintf(&b, "jobs unavailable: %v\n", err)
	}
	for _, job := range jobs.Items {
		fmt.Fprintf(&b, "job %s uid=%s deletion=%v succeeded=%d failed=%d\n",
			job.Name, job.UID, job.DeletionTimestamp, job.Status.Succeeded, job.Status.Failed)
		for _, condition := range job.Status.Conditions {
			fmt.Fprintf(&b, "  condition %s=%s %s: %s\n", condition.Type, condition.Status, condition.Reason, condition.Message)
		}
	}
	pods := &corev1.PodList{}
	if err := c.List(diagCtx, pods, client.InNamespace(nsE2E), owned); err != nil {
		fmt.Fprintf(&b, "pods unavailable: %v\n", err)
	} else if len(pods.Items) == 0 {
		b.WriteString("no pods\n")
	}
	for _, pod := range pods.Items {
		fmt.Fprintf(&b, "pod %s node=%s phase=%s deletion=%v\n",
			pod.Name, pod.Spec.NodeName, pod.Status.Phase, pod.DeletionTimestamp)
		for _, status := range pod.Status.ContainerStatuses {
			if term := status.State.Terminated; term != nil {
				fmt.Fprintf(&b, "  container %s exit=%d reason=%s finished=%s\n",
					status.Name, term.ExitCode, term.Reason, term.FinishedAt.Format(time.RFC3339))
			}
		}
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(diagCtx, client.ObjectKey{Namespace: nsE2E, Name: m.Name + "-work"}, pvc); err != nil {
		fmt.Fprintf(&b, "work PVC unavailable: %v\n", err)
	} else {
		fmt.Fprintf(&b, "work PVC volume=%s\n", pvc.Spec.VolumeName)
	}
	events := &corev1.EventList{}
	if err := c.List(diagCtx, events, client.InNamespace(nsE2E)); err != nil {
		fmt.Fprintf(&b, "events unavailable: %v\n", err)
	}
	slices.SortFunc(events.Items, func(x, y corev1.Event) int { return eventTime(x).Compare(eventTime(y)) })
	for _, event := range events.Items {
		if strings.HasPrefix(event.InvolvedObject.Name, m.Name) {
			fmt.Fprintf(&b, "event %s %s/%s %s/%s: %s\n", eventTime(event).UTC().Format(time.RFC3339),
				event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Type, event.Reason, event.Message)
		}
	}
	if len(logs) == 0 {
		b.WriteString("no compare pod logs captured\n")
	}
	for _, name := range slices.Sorted(maps.Keys(logs)) {
		attempt := logs[name]
		lines := strings.Split(strings.TrimRight(attempt.log, "\n"), "\n")
		fmt.Fprintf(&b, "log %s check=%s err=%v, last lines:\n%s\n",
			name, attempt.check, attempt.err, strings.Join(lines[max(0, len(lines)-20):], "\n"))
	}
	return b.String()
}

func expectPartitionVerification(m *v1beta1.Migration, passed bool) {
	GinkgoHelper()
	Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
	expectConditionTrue(m, v1beta1.ConditionComplete)
	Expect(apimeta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed)).To(BeFalse())
	Expect(m.Status.Verification).To(ConsistOf(
		v1beta1.VerificationResult{Check: compareSchemaCheck, Passed: passed},
		v1beta1.VerificationResult{Check: compareDataCheck, Passed: passed},
	))
	verified := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionVerified)
	Expect(verified).NotTo(BeNil())
	condition := batchv1.JobFailed
	if passed {
		Expect(verified.Status).To(Equal(metav1.ConditionTrue))
		Expect(verified.Reason).To(Equal("ComparePassed"))
		condition = batchv1.JobComplete
	} else {
		Expect(verified.Status).To(Equal(metav1.ConditionFalse))
		Expect(verified.Reason).To(Equal("SchemaMismatch"))
	}
	for _, check := range []string{compareSchemaCheck, compareDataCheck} {
		expectPartitionJob(m, m.Name+"-compare-"+check, condition)
	}
}

func expectPartitionJob(m *v1beta1.Migration, name string, condition batchv1.JobConditionType) {
	GinkgoHelper()
	Expect(name).NotTo(BeEmpty())
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, job)).To(Succeed())
	Expect(metav1.IsControlledBy(job, m)).To(BeTrue())
	Expect(job.Status.Conditions).To(ContainElement(And(
		HaveField("Type", condition), HaveField("Status", corev1.ConditionTrue),
	)))
}

func partitionTestPod(name, job string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: nsE2E,
		Labels: map[string]string{batchv1.JobNameLabel: job, partitionMigrationLabel: "e2e-partition-1"},
	}, Status: corev1.PodStatus{Phase: phase}}
}

func TestCaptureComparePodLogs(t *testing.T) {
	c := publicationRetryTestClient(t,
		partitionTestPod("schema-a", "e2e-partition-1-compare-schema", corev1.PodFailed),
		partitionTestPod("schema-b", "e2e-partition-1-compare-schema", corev1.PodRunning),
		partitionTestPod("data-a", "e2e-partition-1-compare-data", corev1.PodFailed),
		partitionTestPod("other", "e2e-partition-2-compare-data", corev1.PodFailed),
	)
	// The failed first read of schema-a is retried; data-a is read once.
	command, state := newPSQLExecCommand(t,
		psqlExecResult{stderr: "pod not found", exitCode: 1},
		psqlExecResult{stdout: "data: Partition topology mismatch\n"},
		psqlExecResult{stdout: "schema: Partition topology mismatch\n"},
	)
	logs := map[string]comparePodLog{}
	captureComparePodLogs(context.Background(), c, command, "e2e-partition-1", logs)
	if logs["schema-a"].err == nil {
		t.Fatalf("failed read recorded as success: %+v", logs)
	}
	captureComparePodLogs(context.Background(), c, command, "e2e-partition-1", logs)
	want := map[string]comparePodLog{
		"schema-a": {check: compareSchemaCheck, log: "schema: Partition topology mismatch\n"},
		"data-a":   {check: compareDataCheck, log: "data: Partition topology mismatch\n"},
	}
	if !reflect.DeepEqual(logs, want) {
		t.Fatalf("captured %+v, want %+v", logs, want)
	}
	calls, _, commands := state.snapshot()
	if len(calls) != 3 || !slices.Contains(calls[2].args, "pod/schema-a") {
		t.Fatalf("unexpected log reads: %v", calls)
	}
	requirePSQLCommandsReaped(t, commands)
}

func TestPartitionDiagnostics(t *testing.T) {
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: "e2e-partition-1", Namespace: nsE2E}}
	labels := map[string]string{partitionMigrationLabel: m.Name}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: m.Name + "-compare-schema", Namespace: nsE2E, UID: "job-uid", Labels: labels,
	}, Status: batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{
		Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded,
	}}}}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: m.Name + "-work", Namespace: nsE2E},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-work"},
	}
	mine := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "mine", Namespace: nsE2E},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: m.Name + "-compare-schema-x"},
		Type:           corev1.EventTypeNormal, Reason: "Killing", Message: "Stopping container"}
	operatorEvent := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "operator", Namespace: nsE2E},
		InvolvedObject: corev1.ObjectReference{Kind: "Migration", Name: m.Name},
		EventTime:      metav1.NewMicroTime(time.Date(2026, 10, 5, 17, 16, 1, 0, time.UTC)),
		Type:           corev1.EventTypeWarning, Reason: "VerificationMismatch", Message: "compare failed"}
	other := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: nsE2E},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "unrelated"}, Reason: "Unrelated"}
	var log strings.Builder
	for i := 1; i <= 30; i++ {
		fmt.Fprintf(&log, "line-%02d\n", i)
	}
	c := publicationRetryTestClient(t, job, pvc, mine, operatorEvent, other)
	out := partitionDiagnostics(context.Background(), c, m,
		map[string]comparePodLog{"schema-a": {check: compareSchemaCheck, log: log.String()}})
	for _, want := range []string{"job e2e-partition-1-compare-schema uid=job-uid", "failed=1",
		"condition Failed=True BackoffLimitExceeded", "no pods", "work PVC volume=pv-work",
		"Pod/e2e-partition-1-compare-schema-x Normal/Killing: Stopping container",
		"event 2026-10-05T17:16:01Z Migration/e2e-partition-1 Warning/VerificationMismatch: compare failed",
		"log schema-a check=schema", "line-11", "line-30"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q from diagnostics:\n%s", want, out)
		}
	}
	for _, forbidden := range []string{"Unrelated", "line-10"} {
		if strings.Contains(out, forbidden) {
			t.Fatalf("diagnostics kept %q:\n%s", forbidden, out)
		}
	}
}

// eventTime falls back to EventTime: events from the newer events API, the
// operator's included, leave LastTimestamp empty.
func eventTime(e corev1.Event) time.Time {
	if e.LastTimestamp.IsZero() {
		return e.EventTime.Time
	}
	return e.LastTimestamp.Time
}
