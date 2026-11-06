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

package controller

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

func TestBuildRehearsalJob(t *testing.T) {
	m := followPasswordMigration()
	ttl := int32(60)
	m.Spec.TTLSecondsAfterFinished = &ttl
	job, err := buildRehearsalJob(m, "img", 3)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "m-rehearsal-3" {
		t.Fatalf("Job name = %q, want m-rehearsal-3", job.Name)
	}
	if b := job.Spec.BackoffLimit; b == nil || *b != 0 {
		t.Fatalf("backoffLimit = %v, want 0: a second pod would scan both databases again", b)
	}
	// The operator deletes a run's Job when the next run starts; a TTL would lose its times first.
	if job.Spec.TTLSecondsAfterFinished != nil {
		t.Fatalf("TTL = %d, want none", *job.Spec.TTLSecondsAfterFinished)
	}
	if job.Spec.ActiveDeadlineSeconds != nil {
		t.Fatal("a compare of a large database takes hours; the rehearsal must carry no deadline")
	}
	if job.Spec.Template.Labels[labelMigration] != m.Name {
		t.Fatalf("pod labels %v lack %s, which rehearsalPod selects by", job.Spec.Template.Labels, labelMigration)
	}
	pod := job.Spec.Template.Spec
	for _, v := range pod.Volumes {
		if v.PersistentVolumeClaim != nil {
			t.Fatalf("volume %s mounts claim %s, which the live worker holds", v.Name, v.PersistentVolumeClaim.ClaimName)
		}
		if v.Name == workdirVolume && v.EmptyDir == nil {
			t.Fatalf("volume %s is %+v, want an emptyDir", v.Name, v.VolumeSource)
		}
	}
	c := pod.Containers[0]
	if !hasMount(c, workdirVolume, pgcopydb.WorkMount) {
		t.Fatalf("no scratch work dir at %s: %+v", pgcopydb.WorkMount, c.VolumeMounts)
	}
	if got := envValue(c.Env, "PGOPTIONS"); got != rehearsalPGOptions {
		t.Fatalf("PGOPTIONS = %q, want %q", got, rehearsalPGOptions)
	}
	if got := c.Command[len(c.Command)-1]; got != shellPath {
		t.Fatalf("script Job $0 = %q, want %s", got, shellPath)
	}
	script := c.Args[1]
	for _, want := range []string{
		"trap stop TERM",
		"pgcopydb list tables --dir /work/pgcopydb >/dev/null",
		"pgcopydb compare data --dir /work/pgcopydb --json >" + rehearsalReportPath + " &",
		`kill -TERM "-$pid"`,
		"pg_cancel_backend(pid)",
		"client_addr = inet_client_addr()",
		"application_name like '" + rehearsalAppNameLike + "'",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q:\n%s", want, script)
		}
	}
	// pgcopydb names a session "pgcopydb[pid] " plus its process title; % is LIKE's only wildcard here.
	like := regexp.MustCompile("^" + strings.ReplaceAll(regexp.QuoteMeta(rehearsalAppNameLike), "%", ".*") + "$")
	for session, cancel := range map[string]bool{
		"pgcopydb[41] compare worker":                true,
		"pgcopydb[40] pgcopydb":                      false, // compare data's parent, and the live worker's
		`pgcopydb[42] copy "public"."compare_audit"`: false,
		"pgcopydb[43] follow apply":                  false,
	} {
		if like.MatchString(session) != cancel {
			t.Errorf("the trap cancels %q: %t, want %t", session, !cancel, cancel)
		}
	}
	// The INI is mounted only when filters are set, so the flag would name a missing file.
	if strings.Contains(script, "--filters") {
		t.Errorf("an unfiltered rehearsal passes --filters:\n%s", script)
	}

	m.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlySchemas: []string{"app"}}
	job, err = buildRehearsalJob(m, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	c = job.Spec.Template.Spec.Containers[0]
	script = c.Args[1]
	// compare data takes the selection from the catalog list tables seeded, as the clone's flag set it.
	want := "pgcopydb list tables --dir /work/pgcopydb " + strings.Join(pgcopydb.FiltersArgs(m.Spec.Clone.Filters), " ") + " >/dev/null"
	if !strings.Contains(script, want) || strings.Count(script, "--filters") != 1 {
		t.Errorf("a filtered rehearsal must seed with %q and pass --filters once:\n%s", want, script)
	}
	if !slices.Contains(pgcopydb.CloneArgs(&m.Spec, true, false, false), pgcopydb.FiltersPath) {
		t.Error("the clone and the rehearsal no longer share the --filters condition")
	}
	if !hasMount(c, "filters", path.Dir(pgcopydb.FiltersPath)) {
		t.Errorf("the filters INI is not mounted: %+v", c.VolumeMounts)
	}
}

func hasMount(c corev1.Container, volume, at string) bool {
	return slices.ContainsFunc(c.VolumeMounts, func(mnt corev1.VolumeMount) bool {
		return mnt.Name == volume && mnt.MountPath == at
	})
}

// The stand-in for the runner's pgcopydb. A hanging compare traps TERM the way
// pgcopydb does and records that it got one.
const stubRehearsalPgcopydb = `#!/bin/sh
echo "$*" >>"$STUB_CALLS"
case "$*" in
  "list tables"*) exit "${STUB_LIST_EXIT:-0}" ;;
  "compare data"*)
    if [ -n "${STUB_HANG:-}" ]; then
      trap 'kill "$s"; : >"$STUB_STOPPED"; exit 143' TERM
      sleep 30 </dev/null >/dev/null 2>&1 &
      s=$!
      : >"$STUB_STARTED"
      wait "$s"
      exit 0
    fi
    printf '%s' "${STUB_REPORT:-}"
    exit "${STUB_COMPARE_EXIT:-0}" ;;
esac
echo "pgcopydb stub: unexpected call: $*" >&2
exit 9
`

// The stand-in for psql records each call, so a case can read which side the trap canceled on.
const stubRehearsalPsql = `#!/bin/sh
echo "$*" >>"$STUB_PSQL"
`

// rehearsalScriptCmd prepares the shipped script under a real shell with both stubs on
// PATH. dir holds the stubs, the report and the call logs.
func rehearsalScriptCmd(t *testing.T, env ...string) (cmd *exec.Cmd, dir string) {
	t.Helper()
	dir = t.TempDir()
	stub := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	stub(workerContainer, stubRehearsalPgcopydb)
	stub("psql", stubRehearsalPsql)
	job, err := buildRehearsalJob(followPasswordMigration(), testRunnerImage, 1)
	if err != nil {
		t.Fatal(err)
	}
	script := strings.ReplaceAll(job.Spec.Template.Spec.Containers[0].Args[1],
		rehearsalReportPath, filepath.Join(dir, "report.json"))
	cmd = exec.Command(shellPath, "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+dir+":"+os.Getenv("PATH"),
		"PGCOPYDB_SOURCE_PGURI=src", "PGCOPYDB_TARGET_PGURI=tgt",
		"STUB_CALLS="+filepath.Join(dir, "calls"), "STUB_PSQL="+filepath.Join(dir, "psql.calls"),
		"STUB_STARTED="+filepath.Join(dir, "started"), "STUB_STOPPED="+filepath.Join(dir, "stopped"),
	)
	cmd.Env = append(cmd.Env, env...)
	return cmd, dir
}

func scriptExitCode(t *testing.T, err error, out string) int {
	t.Helper()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exitErr):
		return exitErr.ExitCode()
	}
	t.Fatalf("running the rehearsal script: %v\n%s", err, out)
	return 0
}

// The script's verdict lines, and a report that names one table.
const (
	rehearsalReported   = "wrote its report"
	rehearsalNoReport   = "wrote no report"
	rehearsalDiffReport = `STUB_REPORT=[{"schema":"public"}]`
)

// pgcopydb exits 12 both when tables differ and when it could not compare, so
// only the report tells a finished rehearsal from a broken one.
func TestRehearsalScript_DecidesByTheReport(t *testing.T) {
	for name, tc := range map[string]struct {
		env  []string
		want int
		says string
	}{
		"tables differ":            {[]string{rehearsalDiffReport, "STUB_COMPARE_EXIT=12"}, 0, rehearsalReported},
		"tables match":             {[]string{rehearsalDiffReport}, 0, rehearsalReported},
		"exit 12 and no report":    {[]string{"STUB_COMPARE_EXIT=12"}, 1, rehearsalNoReport},
		"exit 0 and no report":     {nil, 1, rehearsalNoReport},
		"output that is no report": {[]string{"STUB_REPORT=could not connect"}, 1, rehearsalNoReport},
		"list tables fails":        {[]string{"STUB_LIST_EXIT=12"}, 1, "list tables could not read"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd, dir := rehearsalScriptCmd(t, tc.env...)
			raw, err := cmd.CombinedOutput()
			out := string(raw)
			if got := scriptExitCode(t, err, out); got != tc.want {
				t.Fatalf("exit %d, want %d:\n%s", got, tc.want, out)
			}
			if !strings.Contains(out, tc.says) {
				t.Fatalf("output lacks %q:\n%s", tc.says, out)
			}
			calls, err := os.ReadFile(filepath.Join(dir, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			if compared := strings.Contains(string(calls), "compare data"); compared == (name == "list tables fails") {
				t.Fatalf("compare data ran: %t, calls:\n%s", compared, calls)
			}
		})
	}
}

// A deleted Job sends TERM, and pgcopydb checks for a stop only between tables,
// so the trap must stop it and cancel its queries on both databases.
func TestRehearsalScript_TermCancelsQueries(t *testing.T) {
	cmd, dir := rehearsalScriptCmd(t, "STUB_HANG=1")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	fail := func(format string, args ...any) {
		t.Helper()
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf(format+"\n%s", append(args, out.String())...)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(filepath.Join(dir, "started")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			fail("compare data never started")
		}
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		fail("sending TERM: %v", err)
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		fail("the script did not exit after TERM")
	}
	if got := scriptExitCode(t, err, out.String()); got != 143 {
		t.Fatalf("exit %d after TERM, want 143:\n%s", got, out.String())
	}
	// Wait returned only once the stub closed its end of the output pipe, so its trap has run.
	if _, err := os.Stat(filepath.Join(dir, "stopped")); err != nil {
		t.Fatalf("pgcopydb got no TERM:\n%s", out.String())
	}
	raw, err := os.ReadFile(filepath.Join(dir, "psql.calls"))
	if err != nil {
		t.Fatalf("the trap ran no psql: %v\n%s", err, out.String())
	}
	calls := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(calls) != 2 || !strings.HasPrefix(calls[0], "src ") || !strings.HasPrefix(calls[1], "tgt ") {
		t.Fatalf("psql calls %q, want one on the source and one on the target", calls)
	}
	for _, call := range calls {
		if !strings.Contains(call, "pg_cancel_backend(pid)") || !strings.Contains(call, "client_addr = inet_client_addr()") {
			t.Errorf("call %q cancels no query of this pod", call)
		}
	}
}

// streamingRehearsal is a streaming follow Migration whose status records run 1 as running.
func streamingRehearsal() *v1beta1.Migration {
	m := followPasswordMigration()
	m.Spec.Cutover.Mode = v1beta1.CutoverManual
	m.Spec.Verification = &v1beta1.VerificationOptions{Rehearsal: true}
	m.Status.Phase = v1beta1.PhaseStreaming
	m.Status.Attempts = 1
	m.Status.JobName = workerJob
	m.Status.VerificationRehearsal = &v1beta1.VerificationRehearsalStatus{
		Armed: true, Run: 1, JobName: rehearsalJobName(m, 1), Outcome: v1beta1.RehearsalRunning,
	}
	return m
}

// rehearsalJobOf is run n's Job, labelled as scriptJob labels it.
func rehearsalJobOf(m *v1beta1.Migration, run int32, conds ...batchv1.JobCondition) *batchv1.Job {
	job := namedJob(rehearsalJobName(m, run), conds...)
	job.Labels = map[string]string{labelMigration: m.Name}
	return job
}

var jobCompleted = batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}

// recordDeletes passes every Delete through and records each name's propagation policy.
func recordDeletes() (interceptor.Funcs, map[string]metav1.DeletionPropagation) {
	deleted := map[string]metav1.DeletionPropagation{}
	return interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			o := &client.DeleteOptions{}
			o.ApplyOptions(opts)
			deleted[obj.GetName()] = ""
			if o.PropagationPolicy != nil {
				deleted[obj.GetName()] = *o.PropagationPolicy
			}
			return cl.Delete(ctx, obj, opts...)
		},
	}, deleted
}

func jobExists(t *testing.T, r *MigrationReconciler, name string) bool {
	t.Helper()
	err := r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, &batchv1.Job{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// CEL rejects rehearsal with an Automatic cutover; a fake API, like an older CRD, does not.
func TestReconcileRehearsal_AutomaticStartsNoRun(t *testing.T) {
	m := streamingRehearsal()
	m.Spec.Cutover.Mode = v1beta1.CutoverAutomatic
	m.Status.VerificationRehearsal = nil
	r := failingReconciler(t, interceptor.Funcs{}, m)
	if err := r.reconcileRehearsal(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if jobExists(t, r, rehearsalJobName(m, 1)) {
		t.Fatal("a rehearsal started beside an Automatic cutover, which would cancel it at catch-up")
	}
	if st := m.Status.VerificationRehearsal; st == nil || st.Armed || !strings.Contains(st.Message, "cutover.mode Manual") {
		t.Fatalf("rehearsal status %+v, want unarmed with a message naming cutover.mode Manual", st)
	}
}

// A start that fails says why in status and stays unarmed, so the next pass starts the run.
func TestReconcileRehearsal_FailedStartIsInStatus(t *testing.T) {
	ctx := context.Background()
	m := streamingRehearsal()
	m.Status.VerificationRehearsal = &v1beta1.VerificationRehearsalStatus{
		Run: 1, JobName: rehearsalJobName(m, 1), Outcome: v1beta1.RehearsalCompleted,
	}
	failing := true
	r := failingReconciler(t, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if failing {
				return errBoom
			}
			return cl.Create(ctx, obj, opts...)
		},
	}, m)
	if err := r.reconcileRehearsal(ctx, m); !errors.Is(err, errBoom) {
		t.Fatalf("a failed Job create must propagate, got %v", err)
	}
	st := m.Status.VerificationRehearsal
	if st.Armed || st.Run != 1 || !strings.Contains(st.Message, "did not start") || !strings.Contains(st.Message, errBoom.Error()) {
		t.Fatalf("rehearsal status %+v, want run 1's record, unarmed, with the create error", st)
	}

	failing = false
	if err := r.reconcileRehearsal(ctx, m); err != nil {
		t.Fatal(err)
	}
	if st := m.Status.VerificationRehearsal; !st.Armed || st.Run != 2 || !jobExists(t, r, rehearsalJobName(m, 2)) {
		t.Fatalf("rehearsal status %+v, want run 2 started on the next pass", st)
	}
}

// The previous run's Job goes first: a run created before a failed delete would run unrecorded.
func TestReconcileRehearsal_PreviousJobDeleteFailure(t *testing.T) {
	m := streamingRehearsal()
	m.Status.VerificationRehearsal = &v1beta1.VerificationRehearsalStatus{
		Run: 1, JobName: rehearsalJobName(m, 1), Outcome: v1beta1.RehearsalCompleted,
	}
	r := failingReconciler(t, interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return errBoom
		},
	}, m, rehearsalJobOf(m, 1, jobCompleted))
	if err := r.reconcileRehearsal(context.Background(), m); !errors.Is(err, errBoom) {
		t.Fatalf("a failed delete of the previous run's Job must propagate, got %v", err)
	}
	if jobExists(t, r, rehearsalJobName(m, 2)) || m.Status.VerificationRehearsal.Armed {
		t.Fatalf("run 2 started although run 1's Job could not be deleted: %+v", m.Status.VerificationRehearsal)
	}
}

// A run whose status write was lost is in no status, and the cancel stops it all the same.
func TestCancelRehearsal_StopsAnUnrecordedRun(t *testing.T) {
	m := streamingRehearsal()
	m.Status.VerificationRehearsal = &v1beta1.VerificationRehearsalStatus{
		Run: 1, JobName: rehearsalJobName(m, 1), Outcome: v1beta1.RehearsalCompleted,
	}
	worker := namedJob(workerJob)
	worker.Labels = map[string]string{labelMigration: m.Name}
	funcs, deleted := recordDeletes()
	r := failingReconciler(t, funcs, m, rehearsalJobOf(m, 1, jobCompleted), rehearsalJobOf(m, 2), worker)
	if err := r.cancelRehearsal(context.Background(), m, "the cutover started"); err != nil {
		t.Fatal(err)
	}
	want := map[string]metav1.DeletionPropagation{rehearsalJobName(m, 2): metav1.DeletePropagationForeground}
	if !maps.Equal(deleted, want) {
		t.Fatalf("deleted %v, want only the unrecorded run, in the foreground", deleted)
	}
	if st := m.Status.VerificationRehearsal; st.Outcome != v1beta1.RehearsalCompleted || st.Run != 1 {
		t.Fatalf("rehearsal %+v, want run 1's record unchanged", st)
	}
}

var _ = Describe("Verification rehearsal", func() {
	ctx := context.Background()
	lagging := &sentinel.State{WriteLSN: laggingWriteLSN, ReplayLSN: laggingReplayLSN, SourceHead: laggingHeadLSN, Endpos: sentinel.ZeroLSN}

	get := func(name string) *v1beta1.Migration {
		GinkgoHelper()
		m := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}
	edit := func(name string, change func(*v1beta1.Migration)) {
		GinkgoHelper()
		m := get(name)
		change(m)
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
	}
	setRehearsal := func(name string, on bool) {
		GinkgoHelper()
		edit(name, func(m *v1beta1.Migration) { m.Spec.Verification.Rehearsal = on })
	}
	jobAbsent := func(name string) bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, &batchv1.Job{}))
	}
	// jobDeleted is true for a Job that is gone or, with no garbage collector in envtest,
	// held by its foreground deletion.
	jobDeleted := func(name string) bool {
		j := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, j)
		return apierrors.IsNotFound(err) || (err == nil && !j.DeletionTimestamp.IsZero())
	}
	rehearsal := func(m *v1beta1.Migration) *v1beta1.VerificationRehearsalStatus {
		GinkgoHelper()
		Expect(m.Status.VerificationRehearsal).NotTo(BeNil())
		return m.Status.VerificationRehearsal
	}
	setPodPhase := func(pod *corev1.Pod, phase corev1.PodPhase) {
		GinkgoHelper()
		pod.Status.Phase = phase
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	// rehearsalPodOf fabricates the running pod the Job controller would start; envtest runs none.
	rehearsalPodOf := func(name, jobName string) *corev1.Pod {
		GinkgoHelper()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: jobName + "-pod", Namespace: testNS,
				Labels: map[string]string{labelMigration: name, jobNameLabel: jobName}},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{Name: testPodContainer, Image: testPodImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0)) })
		setPodPhase(pod, corev1.PodRunning)
		return pod
	}
	// start runs a rehearsing follow Migration, Manual and with data verification, to its worker.
	start := func(name string) (*MigrationReconciler, *fakeSentinel) {
		GinkgoHelper()
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverManual}
		m.Spec.Verification = &v1beta1.VerificationOptions{Data: true, Rehearsal: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		fake := &fakeSentinel{state: lagging}
		r := newReconciler()
		r.Sentinel = fake
		r.Logs = cloneDoneLogs()
		passGate(ctx, r, name)
		Expect(fetchJob(ctx, name+"-run-1")).NotTo(BeNil())
		return r, fake
	}
	// arm runs a pass that must start run n.
	arm := func(r *MigrationReconciler, name string, run int32) *v1beta1.Migration {
		GinkgoHelper()
		m := reconcileAndGet(ctx, r, name)
		st := rehearsal(m)
		Expect(st.Armed).To(BeTrue())
		Expect(st.Run).To(Equal(run))
		Expect(st.JobName).To(Equal(rehearsalJobName(m, run)))
		Expect(st.Outcome).To(Equal(v1beta1.RehearsalRunning))
		Expect(fetchJob(ctx, st.JobName)).NotTo(BeNil())
		return m
	}
	// expectReportOnly proves the rehearsal reached nothing the success path owns.
	expectReportOnly := func(m *v1beta1.Migration, phase v1beta1.MigrationPhase) {
		GinkgoHelper()
		Expect(m.Status.Phase).To(Equal(phase))
		Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionVerified)).To(BeNil())
		Expect(m.Status.Verification).To(BeEmpty())
		for _, suffix := range []string{"-compare-schema", "-compare-data", "-verify"} {
			Expect(jobAbsent(m.Name+suffix)).To(BeTrue(), "the rehearsal created Job %s%s", m.Name, suffix)
		}
	}

	It("runs once per change to true and reports without touching the verdict or the phase", func() {
		const name = "mig-rehearsal-arm"
		defer removeMigration(ctx, name)
		r, _ := start(name)

		By("waiting while the base copy runs")
		logs := r.Logs.(*fakeLogs)
		logs.tsOut = copyingLine
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(m.Status.VerificationRehearsal).To(BeNil())
		Expect(jobAbsent(name + "-rehearsal-1")).To(BeTrue())

		By("starting run 1 in the first Streaming pass")
		logs.tsOut = copyingLine + cloneDoneLine
		m = arm(r, name, 1)
		expectReportOnly(m, v1beta1.PhaseStreaming)
		Expect(metav1.IsControlledBy(fetchJob(ctx, name+"-rehearsal-1"), m)).To(BeTrue())

		m = reconcileAndGet(ctx, r, name)
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalRunning))

		By("recording the outcome and both times once the Job finished")
		finishJob(ctx, name+"-rehearsal-1", true)
		m = reconcileAndGet(ctx, r, name)
		st := rehearsal(m)
		Expect(st.Outcome).To(Equal(v1beta1.RehearsalCompleted))
		Expect(st.StartedAt).NotTo(BeNil())
		Expect(st.CompletedAt).NotTo(BeNil())
		Expect(st.Armed).To(BeTrue())
		expectReportOnly(m, v1beta1.PhaseStreaming)

		reconcileAndGet(ctx, r, name)
		Expect(jobAbsent(name + "-rehearsal-2")).To(BeTrue())

		By("disarming on false and keeping the finished run's record and Job")
		setRehearsal(name, false)
		m = reconcileAndGet(ctx, r, name)
		Expect(rehearsal(m).Armed).To(BeFalse())
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalCompleted))
		Expect(jobDeleted(name + "-rehearsal-1")).To(BeFalse())

		By("starting run 2 on the next true, in place of run 1")
		setRehearsal(name, true)
		m = arm(r, name, 2)
		Expect(jobDeleted(name + "-rehearsal-1")).To(BeTrue())
		expectReportOnly(m, v1beta1.PhaseStreaming)
	})

	It("cancels a running run on false and disarms only once its pod has ended", func() {
		const name = "mig-rehearsal-false"
		defer removeMigration(ctx, name)
		r, _ := start(name)
		arm(r, name, 1)
		pod := rehearsalPodOf(name, name+"-rehearsal-1")

		setRehearsal(name, false)
		m := reconcileAndGet(ctx, r, name)
		st := rehearsal(m)
		Expect(st.Outcome).To(Equal(v1beta1.RehearsalCanceled))
		Expect(st.Message).To(ContainSubstring("set to false"))
		Expect(st.CompletedAt).NotTo(BeNil())
		Expect(st.Armed).To(BeTrue(), "the rehearsal pod still runs")
		Expect(jobDeleted(name + "-rehearsal-1")).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))

		By("ignoring a true that comes while the run is still armed")
		setRehearsal(name, true)
		reconcileAndGet(ctx, r, name)
		Expect(jobAbsent(name + "-rehearsal-2")).To(BeTrue())

		By("disarming once the pod has ended, and starting run 2 on the next true")
		setRehearsal(name, false)
		setPodPhase(pod, corev1.PodFailed)
		m = reconcileAndGet(ctx, r, name)
		Expect(rehearsal(m).Armed).To(BeFalse())
		setRehearsal(name, true)
		arm(r, name, 2)
	})

	It("cancels on cutover, and the drain verify waits until the rehearsal pod has ended", func() {
		const name = "mig-rehearsal-cutover"
		defer removeMigration(ctx, name)
		r, fake := start(name)
		arm(r, name, 1)
		pod := rehearsalPodOf(name, name+"-rehearsal-1")

		By("leaving CutoverPending to the follow logic while the run goes on")
		fake.state = &sentinel.State{WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		reconcileAndGet(ctx, r, name)
		m := confirmCaughtUp(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCutoverPending))
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalRunning))

		By("canceling it in the pass that starts the cutover")
		edit(name, func(m *v1beta1.Migration) { m.Spec.Cutover.Approved = true })
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalCanceled))
		Expect(rehearsal(m).Message).To(ContainSubstring("the cutover started"))
		Expect(jobDeleted(name + "-rehearsal-1")).To(BeTrue())

		By("creating no verify Job while the rehearsal pod runs")
		finishJob(ctx, name+"-run-1", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(jobAbsent(name + "-verify")).To(BeTrue())

		setPodPhase(pod, corev1.PodSucceeded)
		reconcileAndGet(ctx, r, name)
		Expect(fetchJob(ctx, name+"-verify")).NotTo(BeNil())
	})

	It("cancels on suspend and starts no run on resume", func() {
		const name = "mig-rehearsal-suspend"
		defer removeMigration(ctx, name)
		r, _ := start(name)
		arm(r, name, 1)

		edit(name, func(m *v1beta1.Migration) { m.Spec.Suspend = true })
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseSuspended))
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalCanceled))
		Expect(rehearsal(m).Message).To(ContainSubstring("spec.suspend"))
		Expect(jobDeleted(name + "-rehearsal-1")).To(BeTrue())

		By("resuming: only a change to true starts a run")
		edit(name, func(m *v1beta1.Migration) { m.Spec.Suspend = false })
		m = passRetryGate(ctx, r, name)
		Expect(m.Status.JobName).To(Equal(name + "-run-2"))
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalCanceled))
		Expect(jobAbsent(name + "-rehearsal-2")).To(BeTrue())
	})

	It("cancels when the worker fails, and the retry gate waits for the rehearsal pod, not its compare", func() {
		const name = "mig-rehearsal-worker"
		defer removeMigration(ctx, name)
		r, _ := start(name)
		arm(r, name, 1)
		pod := rehearsalPodOf(name, name+"-rehearsal-1")

		finishJob(ctx, name+"-run-1", false)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.RetryGate).NotTo(BeNil())
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalCanceled))
		Expect(rehearsal(m).Message).To(ContainSubstring("worker attempt 1 failed"))
		Expect(jobDeleted(name + "-rehearsal-1")).To(BeTrue())

		By("holding the retry while the rehearsal pod ends")
		m = reconcileAndGet(ctx, r, name)
		held := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionRetryHeld)
		Expect(held).NotTo(BeNil())
		Expect(held.Reason).To(Equal(reasonWorkerPodRemains))
		Expect(held.Message).To(SatisfyAll(ContainSubstring(pod.Name), ContainSubstring("verification rehearsal")))
		Expect(jobAbsent(gateJobName(m, 2))).To(BeTrue())

		By("probing as soon as the pod has ended")
		setPodPhase(pod, corev1.PodFailed)
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionRetryHeld).Reason).To(Equal(reasonGateProbing))
		Expect(fetchJob(ctx, gateJobName(m, 2))).NotTo(BeNil())
	})

	It("cancels a run still going when the Migration is terminal", func() {
		const name = "mig-rehearsal-terminal"
		defer removeMigration(ctx, name)
		r, _ := start(name)
		arm(r, name, 1)

		// What a pass that failed the Migration leaves behind.
		m := get(name)
		m.Status.Phase = v1beta1.PhaseFailed
		meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
			Type: v1beta1.ConditionFailed, Status: metav1.ConditionTrue, Reason: testInvalidSpec, Message: "failed elsewhere",
		})
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())

		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalCanceled))
		Expect(rehearsal(m).Message).To(ContainSubstring("the Migration ended"))
		Expect(jobDeleted(name + "-rehearsal-1")).To(BeTrue())

		m = reconcileAndGet(ctx, r, name)
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalCanceled))
		Expect(jobAbsent(name + "-rehearsal-2")).To(BeTrue())
	})

	It("fails a run whose Job vanished, never reruns it, and keeps a finished run without its Job", func() {
		const name = "mig-rehearsal-missing"
		defer removeMigration(ctx, name)
		r, _ := start(name)
		arm(r, name, 1)
		background := metav1.DeletePropagationBackground

		By("keeping a run whose Job only the cache does not show yet")
		r.Client = hidingClient{Client: k8sClient, hidden: name + "-rehearsal-1"}
		r.APIReader = k8sClient
		m := reconcileAndGet(ctx, r, name)
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalRunning))
		r.Client, r.APIReader = k8sClient, nil

		By("failing the run once its Job is deleted before it finished")
		Expect(k8sClient.Delete(ctx, fetchJob(ctx, name+"-rehearsal-1"),
			&client.DeleteOptions{PropagationPolicy: &background})).To(Succeed())
		m = reconcileAndGet(ctx, r, name)
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalFailed))
		Expect(rehearsal(m).Message).To(ContainSubstring("deleted before it finished"))
		reconcileAndGet(ctx, r, name)
		Expect(jobAbsent(name + "-rehearsal-1")).To(BeTrue())
		Expect(jobAbsent(name + "-rehearsal-2")).To(BeTrue())

		By("keeping Completed after a finished run's Job is deleted")
		setRehearsal(name, false)
		reconcileAndGet(ctx, r, name)
		setRehearsal(name, true)
		arm(r, name, 2)
		finishJob(ctx, name+"-rehearsal-2", true)
		reconcileAndGet(ctx, r, name)
		Expect(k8sClient.Delete(ctx, fetchJob(ctx, name+"-rehearsal-2"),
			&client.DeleteOptions{PropagationPolicy: &background})).To(Succeed())
		m = reconcileAndGet(ctx, r, name)
		Expect(rehearsal(m).Outcome).To(Equal(v1beta1.RehearsalCompleted))
		Expect(jobAbsent(name + "-rehearsal-3")).To(BeTrue())
	})

	It("records a run that wrote no report as Failed with the Job's error", func() {
		const name = "mig-rehearsal-failed"
		defer removeMigration(ctx, name)
		r, _ := start(name)
		arm(r, name, 1)
		r.Logs.(*fakeLogs).byJob = map[string]string{name + "-rehearsal-1": `{"error_severity":"ERROR","message":"could not connect to the target"}` + "\n"}

		finishJob(ctx, name+"-rehearsal-1", false)
		m := reconcileAndGet(ctx, r, name)
		st := rehearsal(m)
		Expect(st.Outcome).To(Equal(v1beta1.RehearsalFailed))
		Expect(st.Message).To(SatisfyAll(ContainSubstring("wrote no compare report"), ContainSubstring("could not connect to the target")))
		Expect(st.CompletedAt).NotTo(BeNil(), "a failed Job has no completionTime; the Failed condition dates it")
		expectReportOnly(m, v1beta1.PhaseStreaming)
	})
})
