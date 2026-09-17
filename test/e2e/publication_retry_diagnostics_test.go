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
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode"

	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	publicationRetryLogBytes        = 64 << 10
	publicationRetryUnavailable     = "unavailable"
	publicationRetryWorkerContainer = "pgcopydb"
	publicationRetryTestPod         = "worker-pod"
)

var (
	publicationRetryTokenPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,79}$`)
	publicationRetryImageDigest  = regexp.MustCompile(`sha256:[a-f0-9]{64}$`)
	publicationRetrySensitive    = regexp.MustCompile(`(?i)://|password|passwd|passfile|pgpass|secret|token|` +
		`\bdial\s+(?:tcp|udp)[46]?\b|\blookup\b.*\bno such host\b|\bcould not translate host name\b|` +
		`\b(?:host\w*|user|dbname|port|ssl\w*|service|node|pod)\s*[=:]|` +
		`\b(?:host|hostname|server|node|pod)\s+["']|\b(?:\d{1,3}\.){3}\d{1,3}\b|` +
		`\[[0-9a-f:]+\]|\b[0-9a-f]*::[0-9a-f:]+(?:\s|$)|(?:[0-9a-f]{1,4}:){4}|\b[A-Z_][A-Z0-9_]*=`)
)

func publicationRetryToken(value string) string {
	if !publicationRetryTokenPattern.MatchString(value) {
		return publicationRetryUnavailable
	}
	return value
}

// API and exec error text can contain private endpoints and credentials.
func publicationRetryProbeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return errors.New("probe unavailable: timeout or cancellation")
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return fmt.Errorf("probe unavailable: command exit=%d", exit.ExitCode())
	}
	return fmt.Errorf("probe unavailable: %s (%T)", publicationRetryToken(string(apierrors.ReasonForError(err))), err)
}

// Other JSON fields can contain infrastructure metadata, so decode only the log message and severity.
func publicationRetryLogText(raw string, names ...string) string {
	var b strings.Builder
	for line := range strings.Lines(raw) {
		severity, message := "LOG", strings.TrimSpace(line)
		var entry struct {
			Severity string `json:"error_severity"`
			Message  string `json:"message"`
		}
		if strings.HasPrefix(message, "{") {
			if json.Unmarshal([]byte(message), &entry) != nil || entry.Message == "" {
				fmt.Fprintln(&b, "[WARN] structured log message unavailable")
				continue
			}
			message = entry.Message
			switch entry.Severity {
			case "TRACE", "DEBUG", "INFO", "NOTICE", "WARN", "WARNING", "ERROR", "FATAL", "PANIC", "CRITICAL":
				severity = entry.Severity
			}
		}
		for part := range strings.Lines(message) {
			part = strings.Map(func(r rune) rune {
				if unicode.IsControl(r) {
					return -1
				}
				return r
			}, part)
			for _, name := range names {
				if name != "" {
					part = strings.ReplaceAll(part, name, "[fixture-location]")
				}
			}
			if publicationRetrySensitive.MatchString(part) {
				part = "[redacted connection/environment line]"
			}
			fmt.Fprintf(&b, "[%s] %s\n", severity, part)
		}
	}
	return b.String()
}

func publicationRetryDiagnostics(
	c client.Client, command commandFactory, mig *v1beta1.Migration, table, slot string,
) string {
	diagCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var b strings.Builder
	if mig.UID == "" {
		return "[WARN] diagnostics unavailable: fixture has no UID\n"
	}
	m := &v1beta1.Migration{}
	if err := c.Get(diagCtx, client.ObjectKeyFromObject(mig), m); err != nil {
		fmt.Fprintf(&b, "[WARN] Migration %v\n", publicationRetryProbeError(err))
	} else if m.UID != mig.UID {
		return "[WARN] diagnostics unavailable: Migration ownership changed\n"
	} else {
		fmt.Fprintf(&b, "[INFO] Migration phase=%s attempts=%d\n", publicationRetryToken(string(m.Status.Phase)),
			m.Status.Attempts)
		for _, condition := range m.Status.Conditions {
			fmt.Fprintf(&b, "[INFO] Migration condition=%s status=%s reason=%s\n",
				publicationRetryToken(condition.Type), publicationRetryToken(string(condition.Status)),
				publicationRetryToken(condition.Reason))
		}
	}
	// The specs require exactly two attempts; never collect unrelated or unbounded Jobs.
	for attempt := 1; attempt <= 2; attempt++ {
		publicationRetryAttemptDiagnostics(diagCtx, c, command, &b, mig, attempt)
	}
	for _, cluster := range []string{sourceCluster, targetCluster} {
		publicationRetrySQLDiagnostics(diagCtx, c, command, &b, cluster, table, slot)
	}
	return b.String()
}

func publicationRetryAttemptDiagnostics(
	parent context.Context, c client.Client, command commandFactory, b *strings.Builder,
	mig *v1beta1.Migration, attempt int,
) {
	probeCtx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	job := &batchv1.Job{}
	key := client.ObjectKey{Namespace: mig.Namespace, Name: fmt.Sprintf("%s-run-%d", mig.Name, attempt)}
	if err := c.Get(probeCtx, key, job); err != nil {
		fmt.Fprintf(b, "[WARN] attempt=%d Job %v\n", attempt, publicationRetryProbeError(err))
		return
	}
	if !metav1.IsControlledBy(job, mig) || job.UID == "" {
		fmt.Fprintf(b, "[WARN] attempt=%d Job unavailable: ownership mismatch\n", attempt)
		return
	}
	fmt.Fprintf(b, "[INFO] attempt=%d Job active=%d succeeded=%d failed=%d\n",
		attempt, job.Status.Active, job.Status.Succeeded, job.Status.Failed)
	for _, condition := range job.Status.Conditions {
		fmt.Fprintf(b, "[INFO] attempt=%d Job condition=%s status=%s reason=%s\n", attempt,
			publicationRetryToken(string(condition.Type)), publicationRetryToken(string(condition.Status)),
			publicationRetryToken(condition.Reason))
	}
	pods := &corev1.PodList{}
	if err := c.List(probeCtx, pods, client.InNamespace(mig.Namespace),
		client.MatchingLabels{batchv1.JobNameLabel: job.Name}, client.Limit(3)); err != nil {
		fmt.Fprintf(b, "[WARN] attempt=%d pods %v\n", attempt, publicationRetryProbeError(err))
		return
	}
	slices.SortFunc(pods.Items, func(a, b corev1.Pod) int { return strings.Compare(string(a.UID), string(b.UID)) })
	owned := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		owned++
		if owned > 2 {
			fmt.Fprintf(b, "[WARN] attempt=%d additional pods omitted (limit=2)\n", attempt)
			break
		}
		label := fmt.Sprintf("attempt=%d pod=%d", attempt, owned)
		fmt.Fprintf(b, "[INFO] %s phase=%s\n", label, publicationRetryToken(string(pod.Status.Phase)))
		found := false
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != publicationRetryWorkerContainer {
				continue
			}
			found = true
			imageID := publicationRetryImageDigest.FindString(status.ImageID)
			if imageID == "" {
				imageID = publicationRetryUnavailable
			}
			fmt.Fprintf(b, "[INFO] %s container=pgcopydb imageID=%s restarts=%d\n", label, imageID, status.RestartCount)
			if term := status.State.Terminated; term != nil {
				fmt.Fprintf(b, "[INFO] %s termination reason=%s exit=%d signal=%d\n",
					label, publicationRetryToken(term.Reason), term.ExitCode, term.Signal)
			} else {
				fmt.Fprintf(b, "[INFO] %s termination=unavailable (not terminated)\n", label)
			}
		}
		if !found {
			fmt.Fprintf(b, "[WARN] %s container status unavailable\n", label)
		}
		// Start at the beginning, not a short tail that loses the first startup error.
		out, err := commandOutput(probeCtx, command, "kubectl", "logs", "-n", mig.Namespace, pod.Name,
			"-c", publicationRetryWorkerContainer, "--tail=-1", fmt.Sprintf("--limit-bytes=%d", publicationRetryLogBytes))
		if err != nil {
			fmt.Fprintf(b, "[WARN] %s logs %v\n", label, publicationRetryProbeError(err))
			continue
		}
		out = out[:min(len(out), publicationRetryLogBytes)]
		fmt.Fprintf(b, "[INFO] %s logs stdout+stderr bytes=%d limit=%d (prefix; may be truncated)\n",
			label, len(out), publicationRetryLogBytes)
		if len(out) == 0 {
			fmt.Fprintf(b, "[WARN] %s logs unavailable: empty stream\n", label)
		}
		for line := range strings.Lines(publicationRetryLogText(string(out), pod.Name, pod.Spec.NodeName)) {
			severity, message, _ := strings.Cut(line, " ")
			fmt.Fprintf(b, "%s %s %s", severity, label, message)
		}
	}
	if owned == 0 {
		fmt.Fprintf(b, "[WARN] attempt=%d pods unavailable: no UID-owned worker pod\n", attempt)
	}
}

func publicationRetrySQLDiagnostics(
	parent context.Context, c client.Client, command commandFactory, b *strings.Builder, cluster, table, slot string,
) {
	probeCtx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	queries := []struct{ label, sql string }{{"rows|marker-1|marker-2|marker-3", "SELECT count(*) AS rows, " +
		"count(*) FILTER (WHERE id=1), count(*) FILTER (WHERE id=2), count(*) FILTER (WHERE id=3) FROM " + table}}
	if cluster == sourceCluster {
		queries = append(queries, struct{ label, sql string }{"publication-oid|owner|alltables|membership",
			publicationRetryStateSQL(slot)}, struct{ label, sql string }{"slot-active|type|plugin|confirmedLSN",
			"SELECT active, slot_type, coalesce(plugin, '<null>'), coalesce(confirmed_flush_lsn::text, '<null>') " +
				"FROM pg_replication_slots WHERE slot_name='" + slot + "' AND database=current_database()"})
	} else {
		queries = append(queries, struct{ label, sql string }{"origin-count",
			"SELECT count(*) FROM pg_replication_origin WHERE roname='" + slot + "'"})
	}
	pods := &corev1.PodList{}
	err := c.List(probeCtx, pods, client.InNamespace(nsE2E), client.MatchingLabels{
		labelCNPGCluster: cluster, labelCNPGRole: rolePrimary,
	}, client.Limit(2))
	for _, query := range queries {
		label := cluster + " " + query.label
		if err != nil || len(pods.Items) != 1 {
			fmt.Fprintf(b, "[WARN] %s unavailable: primary lookup (count=%d error=%v)\n",
				label, len(pods.Items), publicationRetryProbeError(err))
			continue
		}
		out, probeErr := commandOutput(probeCtx, command, "kubectl", "exec", "-n", nsE2E, pods.Items[0].Name,
			"-c", "postgres", "--", "psql", "-U", "postgres", appDB, "-XAtq", "-v", "ON_ERROR_STOP=1",
			"-c", "SET statement_timeout='5s'; SET lock_timeout='2s'; "+query.sql)
		if probeErr != nil {
			fmt.Fprintf(b, "[WARN] %s %v\n", label, publicationRetryProbeError(probeErr))
			continue
		}
		value := strings.TrimSpace(string(out[:min(len(out), 4096)]))
		if value == "" {
			value = "absent"
		}
		for line := range strings.Lines(publicationRetryLogText(value, pods.Items[0].Name, pods.Items[0].Spec.NodeName)) {
			severity, message, _ := strings.Cut(line, " ")
			fmt.Fprintf(b, "%s %s %s", severity, label, message)
		}
	}
}

func publicationRetryTestClient(t *testing.T, objects ...client.Object) client.WithWatch {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func publicationRetryTestMigration() *v1beta1.Migration {
	return &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{
		Name: "e2e-publication-established", Namespace: nsE2E, UID: "fixture-uid",
	}, Status: v1beta1.MigrationStatus{Phase: v1beta1.PhaseStreaming, Attempts: 2}}
}

func publicationRetryTestJob(m *v1beta1.Migration, attempt int) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: fmt.Sprintf("%s-run-%d", m.Name, attempt), Namespace: m.Namespace,
		UID: types.UID(fmt.Sprintf("job-%d", attempt)), OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(m, v1beta1.GroupVersion.WithKind("Migration")),
		},
	}}
}

func TestPublicationRetryWorkerState(t *testing.T) {
	for _, tc := range []struct {
		name      string
		phase     v1beta1.MigrationPhase
		condition batchv1.JobConditionType
		want      string
	}{
		{"streaming is not receiver readiness", v1beta1.PhaseStreaming, "", ""},
		{"pending is not receiver readiness", v1beta1.PhaseCutoverPending, "", ""},
		{"retained Streaming failed Job", v1beta1.PhaseStreaming, batchv1.JobFailed,
			"Job=Failed reason=BackoffLimitExceeded"},
		{"failure target", v1beta1.PhaseStreaming, batchv1.JobFailureTarget, "Job=FailureTarget"},
		{"completed Job", v1beta1.PhaseStreaming, batchv1.JobComplete, "Job=Complete"},
		{"failed Migration", v1beta1.PhaseFailed, "", "phase=Failed reason=WorkerFailed"},
		{"completed Migration", v1beta1.PhaseCompleted, "", "phase=Completed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := publicationRetryTestMigration()
			m.Status.Phase = tc.phase
			m.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionFailed, Reason: "WorkerFailed"}}
			first, second := publicationRetryTestJob(m, 1), publicationRetryTestJob(m, 2)
			first.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
			second.Status.Failed = 1 // A counter alone does not prove that the Job is terminal.
			if tc.condition != "" {
				second.Status.Conditions = []batchv1.JobCondition{{Type: tc.condition, Status: corev1.ConditionTrue,
					Reason: "BackoffLimitExceeded", Message: "private-node.invalid must not be printed"}}
			}
			c := publicationRetryTestClient(t, m, first, second)
			err := publicationRetryWorkerState(context.Background(), c, m, 2)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			_, ok := errors.AsType[PollingSignalError](err)
			if !ok || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want StopTrying with %q", err, tc.want)
			}
			polls, failure := 0, ""
			g := NewGomega(func(message string, _ ...int) { failure = message })
			g.Eventually(func() error {
				polls++
				return publicationRetryWorkerState(context.Background(), c, m, 2)
			}, time.Second, time.Millisecond).Should(Succeed())
			if polls != 1 || !strings.Contains(failure, tc.want) {
				t.Fatalf("polls=%d failure=%s, want one poll with terminal cause", polls, failure)
			}
			if strings.Contains(err.Error(), "private-node") {
				t.Fatal("terminal state leaked a Job message")
			}
		})
	}
}

func TestPublicationRetryLogRedaction(t *testing.T) {
	raw := `{"error_severity":"INFO","message":"opening catalog","node":"private-node.invalid"}` + "\n" +
		`{"error_severity":"ERROR","message":"relation \"pgcopydb.sentinel\" does not exist"}` + "\n" +
		"12:34:56 ERROR SELECT * FROM pgcopydb.sentinel;\n" +
		"SELECT value::boolean FROM pgcopydb.sentinel;\n" +
		"worker-pod on private-node.invalid\n" +
		`{"error_severity":"INFO","message":"postgresql://app:credential@db.invalid/app"}` + "\n" +
		`{"error_severity":"INFO","message":"https:\/\/api.invalid\/pods\/worker"}` + "\n" +
		"host='db.invalid' user=app password='credential with spaces'\n" +
		"PGCOPYDB_SOURCE_PGURI=credential\npassword: credential\nTOKEN=credential\n" +
		"dial tcp 192.0.2.1:443\ndial tcp [2001:db8::1]:443\n" +
		"dial tcp: lookup safe.invalid: no such host\ndial udp safe.invalid: connection refused\n" +
		"lookup safe.invalid: no such host\ncould not translate host name safe.invalid to address\n" +
		`{"error_severity":"ERROR","message":"incomplete private-node.invalid` + "\n"
	out := publicationRetryLogText(raw, publicationRetryTestPod, "private-node.invalid")
	for _, want := range []string{"[INFO] opening catalog", `[ERROR] relation "pgcopydb.sentinel" does not exist`,
		"[LOG] 12:34:56 ERROR SELECT * FROM pgcopydb.sentinel;", "SELECT value::boolean FROM pgcopydb.sentinel;",
		"[fixture-location]", "[redacted", publicationRetryUnavailable} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q from sanitized log: %s", want, out)
		}
	}
	for _, forbidden := range []string{"credential", ".invalid", "192.0.2.1", "2001:db8",
		publicationRetryTestPod, "PGURI"} {
		if strings.Contains(out, forbidden) {
			t.Fatalf("sanitized log retained %q", forbidden)
		}
	}
}

func TestPublicationRetryDiagnostics(t *testing.T) {
	m := publicationRetryTestMigration()
	job := publicationRetryTestJob(m, 2)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		Reason: "BackoffLimitExceeded", Message: "private-node.invalid"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: publicationRetryTestPod, Namespace: nsE2E, UID: "pod-uid",
		Labels:          map[string]string{batchv1.JobNameLabel: job.Name},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, batchv1.SchemeGroupVersion.WithKind("Job"))},
	}, Spec: corev1.PodSpec{NodeName: "private-node.invalid"}, Status: corev1.PodStatus{
		Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: publicationRetryWorkerContainer,
			ImageID: "private-registry.invalid/image@sha256:" + strings.Repeat("a", 64),
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 12,
				Message: "password=credential"}},
		}},
	}}
	unowned := pod.DeepCopy()
	unowned.Name, unowned.UID, unowned.OwnerReferences = "unowned-pod", "unowned-uid", nil
	objects := make([]client.Object, 0, 6)
	objects = append(objects, m, job, pod, unowned)
	for _, cluster := range []string{sourceCluster, targetCluster} {
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: cluster + "-1", Namespace: nsE2E,
			Labels: map[string]string{labelCNPGCluster: cluster, labelCNPGRole: rolePrimary},
		}})
	}
	// The early error must survive hundreds of later lines, unlike a short tail.
	logs := "prelude: opening work directory\nERROR missing sentinel\n" +
		strings.Repeat("INFO startup\n", 6000) + "after-log-byte-limit\n"
	command, state := newPSQLExecCommand(t,
		psqlExecResult{stdout: logs}, psqlExecResult{stdout: "3|1|1|0"},
		psqlExecResult{stdout: "42|app|0|public.fixture"}, psqlExecResult{stdout: "f|logical|pgoutput|<null>"},
		psqlExecResult{stderr: "https://api.invalid/password=credential", exitCode: 1}, psqlExecResult{stdout: "0"},
	)
	c := publicationRetryTestClient(t, objects...)
	out := publicationRetryDiagnostics(c, command, m, "public.fixture", "fixture_slot")
	for _, want := range []string{"Migration phase=Streaming attempts=2", "attempt=1 Job probe unavailable",
		"attempt=2 Job condition=Failed", "termination reason=Error exit=12", "imageID=sha256:",
		"prelude: opening work directory", "ERROR missing sentinel", "42|app|0|public.fixture",
		"f|logical|pgoutput|<null>", "marker-3 probe unavailable: command exit=1", "[LOG] e2e-target origin-count 0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q from diagnostics:\n%s", want, out)
		}
	}
	for _, forbidden := range []string{".invalid", "credential", publicationRetryTestPod,
		"unowned-pod", "after-log-byte-limit"} {
		if strings.Contains(out, forbidden) {
			t.Fatalf("diagnostics retained %q", forbidden)
		}
	}
	calls, contexts, commands := state.snapshot()
	if len(calls) != 6 || !slices.Contains(calls[0].args, "--tail=-1") ||
		!slices.Contains(calls[0].args, "--limit-bytes=65536") {
		t.Fatalf("unexpected diagnostic commands: %v", calls)
	}
	for _, call := range calls[1:] {
		if !slices.Contains(call.args, "ON_ERROR_STOP=1") || !slices.Contains(call.args, "-XAtq") {
			t.Fatalf("unsafe SQL command: %v", call)
		}
	}
	for _, ctx := range contexts {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded diagnostic command")
		}
	}
	requirePSQLCommandsReaped(t, commands)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(job), &batchv1.Job{}); err != nil {
		t.Fatal("diagnostics mutated the fixture")
	}
}

func TestPublicationRetryUnavailableDiagnostics(t *testing.T) {
	m := publicationRetryTestMigration()
	command, state := newPSQLExecCommand(t)
	c := interceptor.NewClient(publicationRetryTestClient(t, m), interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errors.New("https://private-api.invalid/secret")
		},
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("private-node.invalid")
		},
	})
	out := publicationRetryDiagnostics(c, command, m, "public.fixture", "fixture_slot")
	if strings.Contains(out, ".invalid") || strings.Contains(out, "[LOG] 0") ||
		strings.Count(out, "probe unavailable") != 8 {
		t.Fatalf("failed probes were not safely reported: %s", out)
	}
	if calls, _, _ := state.snapshot(); len(calls) != 0 {
		t.Fatal("diagnostics executed a command after failed ownership/primary lookups")
	}
}

func TestPublicationRetryOwnership(t *testing.T) {
	for _, tc := range []string{"missing UID", "replacement Migration", "replacement Job"} {
		t.Run(tc, func(t *testing.T) {
			m := publicationRetryTestMigration()
			current := m.DeepCopy()
			job := publicationRetryTestJob(m, 2)
			switch tc {
			case "missing UID":
				m.UID = ""
			case "replacement Migration":
				current.UID = "replacement"
			case "replacement Job":
				job.OwnerReferences[0].UID = "replacement"
			}
			c := publicationRetryTestClient(t, current, job)
			err := publicationRetryWorkerState(context.Background(), c, m, 2)
			if _, ok := errors.AsType[PollingSignalError](err); !ok {
				t.Fatalf("ownership mismatch did not stop polling: %v", err)
			}
			command, state := newPSQLExecCommand(t)
			out := publicationRetryDiagnostics(c, command, m, "public.fixture", "fixture_slot")
			if !strings.Contains(out, publicationRetryUnavailable) {
				t.Fatalf("unowned evidence was not reported unavailable: %s", out)
			}
			if calls, _, _ := state.snapshot(); len(calls) != 0 {
				t.Fatal("unowned worker logs were requested")
			}
		})
	}
}

func TestPublicationRetryCleanupUID(t *testing.T) {
	oldCtx, oldClient := ctx, k8sClient
	t.Cleanup(func() { ctx, k8sClient = oldCtx, oldClient })
	RegisterTestingT(t)
	m := publicationRetryTestMigration()
	ctx = context.Background()
	deleted := false
	k8sClient = interceptor.NewClient(publicationRetryTestClient(t, m), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			options := (&client.DeleteOptions{}).ApplyOptions(opts)
			if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != m.UID {
				return errors.New("deletion did not require the fixture UID")
			}
			deleted = true
			return c.Delete(ctx, obj, opts...)
		},
	})
	if err := InterceptGomegaFailure(func() { deletePublicationRetryMigration(m) }); err != nil || !deleted {
		t.Fatalf("UID-guarded deletion failed: %v (deleted=%t)", err, deleted)
	}
}
