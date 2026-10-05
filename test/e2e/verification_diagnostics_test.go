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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// migrationLabel is the label the operator puts on a Migration's Jobs and pods.
const migrationLabel = "pgcopydb-operator.io/migration"

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

// reportVerificationOnFailure attaches verificationDiagnostics to a failed
// spec. Register it after the fixture cleanup so it runs first, while the
// Jobs still exist.
func reportVerificationOnFailure(m *v1beta1.Migration, logs map[string]comparePodLog) {
	DeferCleanup(func() {
		if CurrentSpecReport().Failed() {
			AddReportEntry("verification before cleanup",
				verificationDiagnostics(ctx, k8sClient, m, logs), ReportEntryVisibilityFailureOrVerbose)
		}
	})
}

// waitCompletedCapturing waits for Completed like waitCompleted and reads
// each compare pod's log as soon as it finishes.
func waitCompletedCapturing(m *v1beta1.Migration, logs map[string]comparePodLog) *v1beta1.Migration {
	GinkgoHelper()
	completed := &v1beta1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), completed)).To(Succeed())
		// Read after the phase: a pod the operator counted as finished is terminal by now.
		captureComparePodLogs(ctx, k8sClient, exec.CommandContext, m.Name, logs)
		if completed.Status.Phase == v1beta1.PhaseFailed {
			StopTrying("migration failed: " + failureMessage(completed)).Now()
		}
		g.Expect(completed.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
	}, migrationTimeout, time.Second).Should(Succeed())
	// Retries only reads that failed on the last poll.
	captureComparePodLogs(ctx, k8sClient, exec.CommandContext, m.Name, logs)
	return completed
}

// verificationDiagnostics reports rather than asserts, so a missing object
// explains the failure instead of replacing it.
func verificationDiagnostics(
	parent context.Context, c client.Client, m *v1beta1.Migration, logs map[string]comparePodLog,
) string {
	diagCtx, cancel := context.WithTimeout(parent, e2eCommandTimeout)
	defer cancel()
	var b strings.Builder
	current := &v1beta1.Migration{}
	if err := c.Get(diagCtx, client.ObjectKeyFromObject(m), current); err != nil {
		fmt.Fprintf(&b, "migration unavailable: %v\n", err)
	} else {
		fmt.Fprintf(&b, "migration %s phase=%s attempts=%d\n", current.Name, current.Status.Phase, current.Status.Attempts)
		for _, condition := range current.Status.Conditions {
			fmt.Fprintf(&b, "  condition %s=%s %s: %s\n", condition.Type, condition.Status, condition.Reason, condition.Message)
		}
		for _, result := range current.Status.Verification {
			fmt.Fprintf(&b, "  verification %s passed=%t\n", result.Check, result.Passed)
		}
	}
	owned := client.MatchingLabels{migrationLabel: m.Name}
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

// expectVerification requires a Completed Migration whose schema and data
// compares both passed, or both failed on a schema mismatch.
func expectVerification(m *v1beta1.Migration, passed bool) {
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
		expectVerificationJob(m, m.Name+"-compare-"+check, condition)
	}
}

func expectVerificationJob(m *v1beta1.Migration, name string, condition batchv1.JobConditionType) {
	GinkgoHelper()
	Expect(name).NotTo(BeEmpty())
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, job)).To(Succeed())
	Expect(metav1.IsControlledBy(job, m)).To(BeTrue())
	Expect(job.Status.Conditions).To(ContainElement(And(
		HaveField("Type", condition), HaveField("Status", corev1.ConditionTrue),
	)))
}

// eventTime falls back to EventTime: events from the newer events API, the
// operator's included, leave LastTimestamp empty.
func eventTime(e corev1.Event) time.Time {
	if e.LastTimestamp.IsZero() {
		return e.EventTime.Time
	}
	return e.LastTimestamp.Time
}

func verificationTestPod(name, job string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: nsE2E,
		Labels: map[string]string{batchv1.JobNameLabel: job, migrationLabel: "e2e-partition-1"},
	}, Status: corev1.PodStatus{Phase: phase}}
}

func TestCaptureComparePodLogs(t *testing.T) {
	c := publicationRetryTestClient(t,
		verificationTestPod("schema-a", "e2e-partition-1-compare-schema", corev1.PodFailed),
		verificationTestPod("schema-b", "e2e-partition-1-compare-schema", corev1.PodRunning),
		verificationTestPod("data-a", "e2e-partition-1-compare-data", corev1.PodFailed),
		verificationTestPod("other", "e2e-partition-2-compare-data", corev1.PodFailed),
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

func TestVerificationDiagnostics(t *testing.T) {
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: "e2e-partition-1", Namespace: nsE2E},
		Status: v1beta1.MigrationStatus{
			Phase: v1beta1.PhaseCompleted, Attempts: 1,
			Conditions: []metav1.Condition{{Type: v1beta1.ConditionVerified, Status: metav1.ConditionFalse,
				Reason: "SchemaMismatch", Message: "compare schema failed"}},
			Verification: []v1beta1.VerificationResult{{Check: compareSchemaCheck, Passed: false}},
		}}
	labels := map[string]string{migrationLabel: m.Name}
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
	c := publicationRetryTestClient(t, m, job, pvc, mine, operatorEvent, other)
	out := verificationDiagnostics(context.Background(), c, m,
		map[string]comparePodLog{"schema-a": {check: compareSchemaCheck, log: log.String()}})
	for _, want := range []string{"migration e2e-partition-1 phase=Completed attempts=1",
		"condition Verified=False SchemaMismatch: compare schema failed", "verification schema passed=false",
		"job e2e-partition-1-compare-schema uid=job-uid", "failed=1",
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
	missing := verificationDiagnostics(context.Background(), publicationRetryTestClient(t), m, nil)
	if !strings.Contains(missing, "migration unavailable") {
		t.Fatalf("a missing Migration is not reported:\n%s", missing)
	}
}
