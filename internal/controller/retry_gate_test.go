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
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// gateLogs is the fake reader that serves r's probe logs, installed when r has none.
func gateLogs(r *MigrationReconciler) *fakeLogs {
	GinkgoHelper()
	if r.Logs == nil {
		r.Logs = &fakeLogs{}
	}
	f, ok := r.Logs.(*fakeLogs)
	Expect(ok).To(BeTrue(), "the retry gate specs script probe logs through *fakeLogs, got %T", r.Logs)
	if f.byUID == nil {
		f.byUID = map[types.UID]string{}
	}
	return f
}

// gatePassLog is the log of a probe that passed every check its inputs annotation enables.
func gatePassLog(inputs string) string {
	lines := []string{gateCensusClean}
	if strings.Contains(inputs, "local=1") {
		lines = append(lines, gateLocalLine)
	}
	if strings.Contains(inputs, "commit=1") {
		lines = append(lines, gateCommitOKLine)
	}
	return strings.Join(append(lines, gatePassLine), "\n") + "\n"
}

// hidingClient answers NotFound for one Job, as an informer cache that has not seen it yet.
type hidingClient struct {
	client.Client
	hidden string
}

func (c hidingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*batchv1.Job); ok && key.Name == c.hidden {
		return apierrors.NewNotFound(batchv1.Resource("jobs"), key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// The probe lines these specs feed the gate; the clean ones live beside the script tests.
const (
	gateActiveSession  = "gate: session pid=4242 state=active wait=ClientRead age=40s started=2026-10-20T10:00:00Z app=pgcopydb[7] copy worker"
	gateSyncRepSession = "gate: session pid=4343 state=active wait=SyncRep age=900s started=2026-10-20T09:45:00Z app=pgcopydb[7] follow apply"
	refusedResumeLine  = `{"error_severity":"FATAL","message":"Refusing to resume the copy: 2 of 3 tables still need their data"}` + "\n"
)

func censusLine(total, syncrep int, since int64, gateWaiting int) string {
	return fmt.Sprintf("gate: census total=%d syncrep=%d named=yes syncrep_since=%d gate_waiting=%d", total, syncrep, since, gateWaiting)
}

var _ = Describe("retry gate", func() {
	ctx := context.Background()

	getMigration := func(name string) *v1beta1.Migration {
		GinkgoHelper()
		m := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}
	jobAbsent := func(name string) bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, &batchv1.Job{}))
	}
	// gateJobs lists every probe Job of the Migration, whatever its attempt.
	gateJobs := func(name string) []string {
		GinkgoHelper()
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(testNS))).To(Succeed())
		var names []string
		for _, j := range jobs.Items {
			if strings.HasPrefix(j.Name, name+"-gate-") {
				names = append(names, j.Name)
			}
		}
		return names
	}
	expectHold := func(m *v1beta1.Migration, status metav1.ConditionStatus, reason string) *metav1.Condition {
		GinkgoHelper()
		c := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionRetryHeld)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(status), "%s: %s", c.Reason, c.Message)
		Expect(c.Reason).To(Equal(reason), c.Message)
		return c
	}
	countEvents := func(evs []string, prefix string) int {
		n := 0
		for _, e := range evs {
			if strings.HasPrefix(e, prefix) {
				n++
			}
		}
		return n
	}
	// setBackoffLimit writes the limit through a raw patch: the typed client omits a 0, which
	// the CRD default then turns into 3.
	setBackoffLimit := func(name string, limit int32) {
		GinkgoHelper()
		m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}}
		Expect(k8sClient.Patch(ctx, m, client.RawPatch(types.MergePatchType,
			fmt.Appendf(nil, `{"spec":{"backoffLimit":%d}}`, limit)))).To(Succeed())
		Expect(m.Spec.BackoffLimit).To(Equal(limit))
	}
	// start runs a Migration to its first worker on a reconciler whose clock the spec moves.
	start := func(name string, clock *time.Time, mutate func(*v1beta1.Migration)) *MigrationReconciler {
		GinkgoHelper()
		m := validMigration(name)
		m.Spec.BackoffLimit = 3
		if mutate != nil {
			mutate(m)
		}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		r := newReconciler()
		*clock = time.Now()
		r.now = func() time.Time { return *clock }
		passGate(ctx, r, name)
		Expect(fetchJob(ctx, name+"-run-1")).NotTo(BeNil())
		return r
	}
	// failRun1 fails the first worker with log, which hands the retry to the gate.
	failRun1 := func(r *MigrationReconciler, name, log string) *v1beta1.Migration {
		GinkgoHelper()
		gateLogs(r).out = log
		finishJob(ctx, name+"-run-1", false)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.RetryGate).NotTo(BeNil(), "%v", m.Status.Conditions)
		return m
	}
	// probe finishes the gate Job on r's clock, with lines as its log.
	probe := func(r *MigrationReconciler, gateName string, ok bool, lines ...string) *batchv1.Job {
		GinkgoHelper()
		gate := fetchJob(ctx, gateName)
		gateLogs(r).byUID[gate.UID] = strings.Join(lines, "\n") + "\n"
		finishJobAt(ctx, gateName, ok, r.currentTime())
		return gate
	}
	workerPod := func(jobName, node string, phase corev1.PodPhase) *corev1.Pod {
		GinkgoHelper()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: jobName + "-pod", Namespace: testNS, Labels: map[string]string{jobNameLabel: jobName}},
			Spec: corev1.PodSpec{
				NodeName: node, RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{Name: testPodContainer, Image: testPodImage}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0)) })
		pod.Status.Phase = phase
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		return pod
	}

	It("holds the retry until a probe passes and spends no attempt while held", func() {
		const name = "mig-gate-hold"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		rec := r.Recorder.(*events.FakeRecorder)

		m := failRun1(r, name, "")
		Expect(m.Status.RetryGate.FailedAttempt).To(Equal(int32(1)))
		Expect(m.Status.JobName).To(BeEmpty())
		expectHold(m, metav1.ConditionTrue, reasonGateProbing)

		m = reconcileAndGet(ctx, r, name)
		owner := metav1.GetControllerOf(fetchJob(ctx, name+"-gate-2"))
		Expect(owner).NotTo(BeNil())
		Expect(owner.UID).To(Equal(m.UID))
		Expect(jobAbsent(name + "-run-2")).To(BeTrue())
		Expect(m.Status.Attempts).To(Equal(int32(1)))
		Expect(countEvents(drainEvents(rec), "Warning RetryHeld")).To(BeZero(), "GateProbing emits nothing")

		probe(r, name+"-gate-2", false, censusLine(1, 0, 0, 0), gateActiveSession, gateCommitOKLine)
		for range 2 {
			m = reconcileAndGet(ctx, r, name)
		}
		Expect(expectHold(m, metav1.ConditionTrue, reasonSessionsRemain).Message).To(ContainSubstring("pid=4242"))
		Expect(countEvents(drainEvents(rec), "Warning RetryHeld")).To(Equal(1))
		Expect(m.Status.RetryGate.Exemption).To(Equal(v1beta1.RetryGateCounted))
		Expect(m.Status.Attempts).To(Equal(int32(1)))

		clock = clock.Add(retryGateInterval)
		reconcileAndGet(ctx, r, name)
		Expect(jobAbsent(name + "-gate-2")).To(BeTrue())
		m = passRetryGate(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(m.Status.JobName).To(Equal(name + "-run-2"))
		Expect(fetchJob(ctx, name+"-run-2")).NotTo(BeNil())
		expectHold(m, metav1.ConditionFalse, reasonGatePassed)
		Expect(m.Status.RetryGate).To(BeNil())
		Expect(jobAbsent(name + "-gate-2")).To(BeTrue())
		evs := drainEvents(rec)
		Expect(evs).To(ContainElement(SatisfyAll(ContainSubstring("AttemptStarted"), ContainSubstring("held"))))
		Expect(countEvents(evs, "Warning RetryHeld")).To(BeZero(), "a kept reason emits nothing")
	})

	It("waits 30 s before replacing a failed probe", func() {
		const name = "mig-gate-keep"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		failRun1(r, name, "")
		reconcileAndGet(ctx, r, name)
		first := probe(r, name+"-gate-2", false, censusLine(1, 0, 0, 0), gateActiveSession, gateCommitOKLine)

		clock = clock.Add(retryGateInterval - time.Second)
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: testNS}})
		Expect(err).NotTo(HaveOccurred())
		// The Job stores its failure time in whole seconds, so up to a second of the keep remains.
		Expect(res.RequeueAfter).To(SatisfyAll(BeNumerically(">", 0), BeNumerically("<=", time.Second)))
		Expect(fetchJob(ctx, name+"-gate-2").UID).To(Equal(first.UID))

		clock = clock.Add(time.Second)
		reconcileAndGet(ctx, r, name)
		Expect(jobAbsent(name + "-gate-2")).To(BeTrue())
		reconcileAndGet(ctx, r, name)
		Expect(fetchJob(ctx, name+"-gate-2").UID).NotTo(Equal(first.UID))
		Expect(jobAbsent(name + "-run-2")).To(BeTrue())
	})

	DescribeTable("names the first failing check",
		func(name string, local bool, lines []string, reason, want string) {
			defer removeMigration(ctx, name)
			var clock time.Time
			r := start(name, &clock, func(m *v1beta1.Migration) {
				if local {
					m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
				}
			})
			failRun1(r, name, "")
			reconcileAndGet(ctx, r, name)
			if lines != nil {
				probe(r, name+"-gate-2", false, lines...)
			} else {
				finishJobAt(ctx, name+"-gate-2", false, r.currentTime())
			}
			m := reconcileAndGet(ctx, r, name)
			Expect(expectHold(m, metav1.ConditionTrue, reason).Message).To(ContainSubstring(want))
			Expect(jobAbsent(name + "-run-2")).To(BeTrue())
		},
		Entry("sessions remain", "mig-gate-check-sessions", false,
			[]string{censusLine(1, 0, 0, 0), gateActiveSession, gateCommitOKLine}, reasonSessionsRemain, "pid=4242"),
		Entry("synchronous_commit not local", "mig-gate-check-local", true,
			[]string{gateCensusClean, gateShowOnLine}, reasonSyncCommitNotLocal, "synchronous_commit=on"),
		Entry("commit blocked", "mig-gate-check-blocked", false,
			[]string{gateCensusClean, gateBlockedLine}, reasonCommitBlocked, "until the probe cancelled it"),
		Entry("commit skipped behind an earlier probe", "mig-gate-check-skipped", false,
			[]string{censusLine(0, 0, 0, 1), gateSkippedWaiting}, reasonCommitBlocked, "gate_waiting=1"),
		Entry("empty log", "mig-gate-check-empty", false, []string{}, reasonProbeFailed, "census gave no result"),
		Entry("unreadable log", "mig-gate-check-unreadable", false, nil, reasonProbeFailed, "was not readable"),
		Entry("missing TEMPORARY grant", "mig-gate-check-grant", false,
			[]string{gateCensusClean, `gate: commit failed ERROR:  42501: permission denied to create temporary tables in database "app" (exit 1)`},
			reasonProbeFailed, `GRANT TEMPORARY ON DATABASE "app" TO "app"`),
	)

	It("keeps the RetryHeld event note within the events API limit", func() {
		const name = "mig-gate-note"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		rec := r.Recorder.(*events.FakeRecorder)
		failRun1(r, name, "")
		reconcileAndGet(ctx, r, name)
		lines := make([]string, 0, gateSessionLimit+2)
		lines = append(lines, censusLine(12, 0, 0, 0))
		for pid := range gateSessionLimit {
			lines = append(lines, fmt.Sprintf("gate: session pid=%d state=active wait=ClientRead age=5s "+
				"started=2026-10-20T10:00:00Z app=pgcopydb[%d] copy worker for public.a_table_with_a_long_name", 500+pid, pid))
		}
		drainEvents(rec)
		probe(r, name+"-gate-2", false, append(lines, gateCommitOKLine)...)
		m := reconcileAndGet(ctx, r, name)

		c := expectHold(m, metav1.ConditionTrue, reasonSessionsRemain)
		Expect(len(c.Message)).To(BeNumerically(">", maxDetailLen), "the message must need cutting for this spec to prove anything")
		var notes []string
		for _, e := range drainEvents(rec) {
			if note, ok := strings.CutPrefix(e, "Warning RetryHeld "); ok {
				notes = append(notes, note)
			}
		}
		Expect(notes).To(HaveLen(1))
		Expect(len(notes[0])).To(BeNumerically("<=", maxDetailLen+3))
	})

	It("never counts a stale or mismatched completion as a pass", func() {
		const name = "mig-gate-stale"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		failRun1(r, name, "")
		reconcileAndGet(ctx, r, name)
		first := probe(r, name+"-gate-2", true, gatePassLog("commit=1,local=0"))

		By("a completion 61 s old")
		clock = clock.Add(gatePassFreshness + time.Second)
		reconcileAndGet(ctx, r, name)
		Expect(jobAbsent(name + "-gate-2")).To(BeTrue())
		Expect(jobAbsent(name + "-run-2")).To(BeTrue())
		m := reconcileAndGet(ctx, r, name)
		second := fetchJob(ctx, name+"-gate-2")
		Expect(second.UID).NotTo(Equal(first.UID))
		Expect(m.Status.Attempts).To(Equal(int32(1)))

		By("synchronousCommit set while the hold waits")
		probe(r, second.Name, true, gatePassLog(second.Annotations[annotationRetryGateInputs]))
		m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		reconcileAndGet(ctx, r, name)
		Expect(jobAbsent(name + "-gate-2")).To(BeTrue())
		Expect(jobAbsent(name + "-run-2")).To(BeTrue())
		reconcileAndGet(ctx, r, name)
		third := fetchJob(ctx, name+"-gate-2")
		Expect(third.Annotations).To(HaveKeyWithValue(annotationRetryGateInputs, "commit=0,local=1"))
		Expect(third.Spec.Template.Spec.Containers[0].Env).To(ContainElement(corev1.EnvVar{Name: "GATE_EXPECT_LOCAL", Value: "1"}))
	})

	It("holds with ProbeFailed when a fresh completion lacks a pass line", func() {
		const name = "mig-gate-no-pass-line"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		failRun1(r, name, "")
		reconcileAndGet(ctx, r, name)
		probe(r, name+"-gate-2", true, gateCensusClean, gateCommitOKLine)
		m := reconcileAndGet(ctx, r, name)
		Expect(expectHold(m, metav1.ConditionTrue, reasonProbeFailed).Message).To(ContainSubstring("gate: pass"))
		Expect(jobAbsent(name + "-run-2")).To(BeTrue())
		Expect(m.Status.Attempts).To(Equal(int32(1)))
	})

	It("adopts a worker Job the cache does not show yet", func() {
		const name = "mig-gate-adopt"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		rec := r.Recorder.(*events.FakeRecorder)
		m := failRun1(r, name, "")

		// The crash this covers: a pass created worker 2 and died before recording it.
		worker, err := buildJob(m, testRunnerImage, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(controllerutil.SetControllerReference(m, worker, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, worker)).To(Succeed())
		r.Client = hidingClient{Client: k8sClient, hidden: worker.Name}
		r.APIReader = k8sClient
		Expect(apierrors.IsNotFound(r.Get(ctx, client.ObjectKeyFromObject(worker), &batchv1.Job{}))).To(BeTrue())
		drainEvents(rec)

		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(m.Status.JobName).To(Equal(worker.Name))
		Expect(m.Status.RetryGate).To(BeNil())
		expectHold(m, metav1.ConditionFalse, reasonGatePassed)
		Expect(gateJobs(name)).To(BeEmpty())
		Expect(drainEvents(rec)).NotTo(ContainElement(ContainSubstring("AttemptStarted")))
	})

	It("holds while the previous worker pod still runs", func() {
		const name = "mig-gate-pod-remains"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		failRun1(r, name, "")
		pod := workerPod(name+"-run-1", "n1", corev1.PodRunning)

		m := reconcileAndGet(ctx, r, name)
		c := expectHold(m, metav1.ConditionTrue, reasonWorkerPodRemains)
		Expect(c.Message).To(SatisfyAll(ContainSubstring(pod.Name), ContainSubstring("n1")))
		Expect(gateJobs(name)).To(BeEmpty())

		pod.Status.Phase = corev1.PodFailed
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		m = reconcileAndGet(ctx, r, name)
		expectHold(m, metav1.ConditionTrue, reasonGateProbing)
		Expect(gateJobs(name)).To(ConsistOf(name + "-gate-2"))
	})

	It("reports a probe pod that cannot start as ProbeFailed", func() {
		const name = "mig-gate-probe-stuck"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		failRun1(r, name, "")
		reconcileAndGet(ctx, r, name)
		gate := fetchJob(ctx, name+"-gate-2")
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: gate.Name + "-pod", Namespace: testNS, Labels: map[string]string{
				jobNameLabel: gate.Name, batchv1.ControllerUidLabel: string(gate.UID),
			}},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: testPodContainer, Image: testPodImage}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0)) })
		pod.Status = corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
			Name: testPodContainer, Image: testPodImage,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "ImagePullBackOff", Message: `Back-off pulling image "runner:missing"`,
			}},
		}}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		m := reconcileAndGet(ctx, r, name)
		Expect(expectHold(m, metav1.ConditionTrue, reasonProbeFailed).Message).To(ContainSubstring("ImagePullBackOff"))
	})

	It("starts a worker only after the budget patch lands", func() {
		By("a conflicting budget patch")
		const conflicted = "mig-gate-budget-conflict"
		defer removeMigration(ctx, conflicted)
		var clock time.Time
		r := start(conflicted, &clock, nil)
		failRun1(r, conflicted, "")
		reconcileAndGet(ctx, r, conflicted)
		probe(r, conflicted+"-gate-2", true, gatePassLog("commit=1,local=0"))
		watching, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		patches := 0
		r.Client = interceptor.NewClient(watching, interceptor.Funcs{
			SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
				patches++
				return migrationConflict(&v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: conflicted}})
			},
		})
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: conflicted, Namespace: testNS}})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Requeue).To(BeTrue()) //nolint:staticcheck // the silent conflict requeue under test
		Expect(patches).To(Equal(1))
		Expect(jobAbsent(conflicted + "-run-2")).To(BeTrue())
		Expect(getMigration(conflicted).Status.Attempts).To(Equal(int32(1)))
		r.Client = k8sClient
		Expect(reconcileAndGet(ctx, r, conflicted).Status.Attempts).To(Equal(int32(2)), "the conflict was the only thing in the way")

		By("backoffLimit 0 and a Counted verdict at the pass")
		const spent = "mig-gate-budget-spent"
		defer removeMigration(ctx, spent)
		r = start(spent, &clock, nil)
		setBackoffLimit(spent, 0)
		failRun1(r, spent, "")
		reconcileAndGet(ctx, r, spent)
		probe(r, spent+"-gate-2", true, gatePassLog("commit=1,local=0"))
		m := reconcileAndGet(ctx, r, spent)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed).Reason).To(Equal("BackoffLimitExceeded"))
		Expect(m.Status.RetryGate.Exemption).To(Equal(v1beta1.RetryGateCounted))
		Expect(jobAbsent(spent + "-run-2")).To(BeTrue())

		By("a stale base with a decided verdict")
		const stale = "mig-gate-budget-stale"
		defer removeMigration(ctx, stale)
		r = start(stale, &clock, nil)
		failRun1(r, stale, "")
		reconcileAndGet(ctx, r, stale)
		probe(r, stale+"-gate-2", false, censusLine(1, 0, 0, 0), gateActiveSession, gateCommitOKLine)
		Expect(reconcileAndGet(ctx, r, stale).Status.RetryGate.Exemption).To(Equal(v1beta1.RetryGateCounted))
		clock = clock.Add(retryGateInterval)
		reconcileAndGet(ctx, r, stale)
		reconcileAndGet(ctx, r, stale)
		probe(r, stale+"-gate-2", true, gatePassLog("commit=1,local=0"))
		old := getMigration(stale)
		bumped := old.DeepCopy()
		metav1.SetMetaDataAnnotation(&bumped.ObjectMeta, "example.com/touched", "yes")
		Expect(k8sClient.Update(ctx, bumped)).To(Succeed())
		_, err = r.startAttempt(ctx, old, old.DeepCopy())
		Expect(apierrors.IsConflict(err)).To(BeTrue(), "%v", err)
		Expect(jobAbsent(stale + "-run-2")).To(BeTrue())
	})

	It("exempts an attempt whose sessions waited in SyncRep since before the failure", func() {
		const name = "mig-gate-exempt"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		rec := r.Recorder.(*events.FakeRecorder)
		setBackoffLimit(name, 0)
		m := failRun1(r, name, "")
		failedAt := m.Status.RetryGate.FailedAt.Unix()
		reconcileAndGet(ctx, r, name)
		probe(r, name+"-gate-2", false, censusLine(1, 1, failedAt-300, 0), gateSyncRepSession, gateSkippedSyncRep)

		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.RetryGate.Exemption).To(Equal(v1beta1.RetryGateExempt))
		Expect(m.Status.ExemptAttempts).To(Equal(int32(1)))
		Expect(m.Status.Phase).NotTo(Equal(v1beta1.PhaseFailed))
		expectHold(m, metav1.ConditionTrue, reasonSessionsRemain)

		clock = clock.Add(retryGateInterval)
		reconcileAndGet(ctx, r, name)
		drainEvents(rec)
		m = passRetryGate(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(m.Status.ExemptAttempts).To(Equal(int32(1)))
		Expect(m.Status.JobName).To(Equal(name + "-run-2"))
		Expect(drainEvents(rec)).To(ContainElement(SatisfyAll(
			ContainSubstring("AttemptStarted"), ContainSubstring("did not count against backoffLimit"))))
	})

	It("counts a fresh wait and fails a spent budget after the census", func() {
		const name = "mig-gate-fresh-wait"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		setBackoffLimit(name, 0)
		m := failRun1(r, name, "")
		failedAt := m.Status.RetryGate.FailedAt.Unix()
		reconcileAndGet(ctx, r, name)
		probe(r, name+"-gate-2", false, censusLine(1, 1, failedAt-10, 0), gateSyncRepSession, gateSkippedSyncRep)

		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Reason).To(Equal("BackoffLimitExceeded"))
		Expect(failed.Message).To(HavePrefix("retry budget exhausted after 1 attempts (0 exempt): attempt 1 failed: "))
		clone := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCloneCompleted)
		Expect(clone.Status).To(Equal(metav1.ConditionFalse))
		Expect(clone.Reason).To(Equal("CloneFailed"))
		Expect(m.Status.RetryGate.Exemption).To(Equal(v1beta1.RetryGateCounted))
		Expect(jobAbsent(name + "-run-2")).To(BeTrue())
	})

	DescribeTable("counts an undecided verdict after 15 minutes",
		func(name string, limit int32) {
			defer removeMigration(ctx, name)
			var clock time.Time
			r := start(name, &clock, nil)
			setBackoffLimit(name, limit)
			m := failRun1(r, name, "")
			failedAt := m.Status.RetryGate.FailedAt.Time
			reconcileAndGet(ctx, r, name)
			probe(r, name+"-gate-2", false)
			m = reconcileAndGet(ctx, r, name)
			expectHold(m, metav1.ConditionTrue, reasonProbeFailed)
			Expect(m.Status.RetryGate.Exemption).To(BeEmpty())

			clock = failedAt.Add(verdictDeadline - time.Second)
			Expect(reconcileAndGet(ctx, r, name).Status.RetryGate.Exemption).To(BeEmpty())
			clock = failedAt.Add(verdictDeadline)
			m = reconcileAndGet(ctx, r, name)
			if limit == 0 {
				Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
				Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed).Message).To(
					HavePrefix("retry budget exhausted after 1 attempts (0 exempt): attempt 1 failed: "))
				return
			}
			Expect(m.Status.Phase).NotTo(Equal(v1beta1.PhaseFailed))
			Expect(m.Status.RetryGate.Exemption).To(Equal(v1beta1.RetryGateCounted))
			expectHold(m, metav1.ConditionTrue, reasonProbeFailed)
		},
		Entry("with the budget spent", "mig-gate-deadline-spent", int32(0)),
		Entry("with budget left", "mig-gate-deadline-left", int32(3)),
	)

	It("fails a spent budget at the verdict deadline while the previous worker pod remains", func() {
		const name = "mig-gate-deadline-pod"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		setBackoffLimit(name, 0)
		m := failRun1(r, name, "")
		failedAt := m.Status.RetryGate.FailedAt.Time
		workerPod(name+"-run-1", "n1", corev1.PodRunning)

		clock = failedAt.Add(verdictDeadline - time.Second)
		m = reconcileAndGet(ctx, r, name)
		expectHold(m, metav1.ConditionTrue, reasonWorkerPodRemains)
		Expect(m.Status.Phase).NotTo(Equal(v1beta1.PhaseFailed))
		Expect(gateJobs(name)).To(BeEmpty())

		clock = failedAt.Add(verdictDeadline)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Reason).To(Equal("BackoffLimitExceeded"))
		Expect(failed.Message).To(Equal(budgetMessage(m)))
		Expect(gateJobs(name)).To(BeEmpty())
	})

	It("decides the verdict once", func() {
		const name = "mig-gate-decide-once"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		m := failRun1(r, name, "")
		failedAt := m.Status.RetryGate.FailedAt.Unix()
		reconcileAndGet(ctx, r, name)
		probe(r, name+"-gate-2", false, censusLine(1, 1, failedAt-300, 0), gateSyncRepSession, gateSkippedSyncRep)

		stale := getMigration(name)
		_, err := r.startAttempt(ctx, stale.DeepCopy(), stale.DeepCopy())
		Expect(err).NotTo(HaveOccurred())
		Expect(getMigration(name).Status.ExemptAttempts).To(Equal(int32(1)))
		_, err = r.startAttempt(ctx, stale.DeepCopy(), stale.DeepCopy())
		Expect(apierrors.IsConflict(err)).To(BeTrue(), "%v", err)
		Expect(getMigration(name).Status.ExemptAttempts).To(Equal(int32(1)))
	})

	It("fails at the pass when backoffLimit drops below the counted attempts during a hold", func() {
		const name = "mig-gate-limit-drop"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, func(m *v1beta1.Migration) { m.Spec.BackoffLimit = 2 })
		failRun1(r, name, "")
		reconcileAndGet(ctx, r, name)
		probe(r, name+"-gate-2", false, censusLine(1, 0, 0, 0), gateActiveSession, gateCommitOKLine)
		Expect(reconcileAndGet(ctx, r, name).Status.RetryGate.Exemption).To(Equal(v1beta1.RetryGateCounted))

		setBackoffLimit(name, 0)
		clock = clock.Add(retryGateInterval)
		Expect(reconcileAndGet(ctx, r, name).Status.Phase).NotTo(Equal(v1beta1.PhaseFailed))
		m := passRetryGate(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed).Reason).To(Equal("BackoffLimitExceeded"))
		Expect(jobAbsent(name + "-run-2")).To(BeTrue())
	})

	DescribeTable("creates no probe after a terminal failure",
		func(name, log, reason string) {
			defer removeMigration(ctx, name)
			var clock time.Time
			r := start(name, &clock, nil)
			gateLogs(r).out = log
			finishJob(ctx, name+"-run-1", false)
			m := reconcileAndGet(ctx, r, name)
			Expect(meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed).Reason).To(Equal(reason))
			reconcileAndGet(ctx, r, name)
			Expect(m.Status.RetryGate).To(BeNil())
			Expect(gateJobs(name)).To(BeEmpty())
		},
		Entry("PermissionDenied", "mig-gate-terminal-denied",
			`{"error_severity":"ERROR","message":"pg_restore: error: could not execute query: ERROR:  permission denied for schema public"}`+"\n",
			"PermissionDenied"),
		Entry("DiskFull", "mig-gate-terminal-disk",
			`{"error_severity":"ERROR","message":"[SQLite 13: database or disk is full]: database or disk is full"}`+"\n", "DiskFull"),
		Entry("SchemaDrift", "mig-gate-terminal-drift",
			`{"error_severity":"ERROR","message":"[TARGET 60] [42703] ERROR:  column \"extra\" of relation \"t\" does not exist"}`+"\n"+
				`{"error_severity":"ERROR","message":"[TARGET 60] Context: Failed to receive pipeline sync"}`+"\n", "SchemaDrift"),
		Entry("SnapshotLost without dropIfExists", "mig-gate-terminal-snapshot", refusedResumeLine, reasonSnapshotLost),
	)

	It("gates a SnapshotLost start-over", func() {
		const name = "mig-gate-start-over"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
			m.Spec.Clone.DropIfExists = true
		})
		m := failRun1(r, name, refusedResumeLine)
		Expect(snapshotLost(m)).To(BeTrue())
		reconcileAndGet(ctx, r, name)
		Expect(fetchJob(ctx, name+"-gate-2")).NotTo(BeNil())
		Expect(jobAbsent(name + "-run-2")).To(BeTrue())

		m = passRetryGate(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(2)))
		Expect(fetchJob(ctx, name+"-run-2").Spec.Template.Spec.Containers[0].Args).To(ContainElements("--restart", "--drop-if-exists"))
	})

	It("takes the non-failure path for a pending retry without retryGate", func() {
		const name = "mig-gate-upgrade"
		defer removeMigration(ctx, name)
		var clock time.Time
		r := start(name, &clock, nil)
		m := failRun1(r, name, "")
		// What an operator from before the gate leaves: jobName cleared, no retryGate.
		m.Status.RetryGate = nil
		meta.RemoveStatusCondition(&m.Status.Conditions, v1beta1.ConditionRetryHeld)
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())

		m = reconcileAndGet(ctx, r, name)
		expectHold(m, metav1.ConditionTrue, reasonGateProbing)
		Expect(fetchJob(ctx, name+"-gate-2")).NotTo(BeNil())
		probe(r, name+"-gate-2", false, censusLine(1, 1, clock.Unix()-300, 0), gateSyncRepSession, gateSkippedSyncRep)
		m = reconcileAndGet(ctx, r, name)
		expectHold(m, metav1.ConditionTrue, reasonSessionsRemain)
		Expect(m.Status.RetryGate).To(BeNil())
		Expect(m.Status.ExemptAttempts).To(BeZero())
		Expect(m.Status.Attempts).To(Equal(int32(1)))
	})

	DescribeTable("probes the commit after a copy the failed worker logged as done",
		func(name, log, inputs string, seen bool) {
			defer removeMigration(ctx, name)
			var clock time.Time
			r := start(name, &clock, func(m *v1beta1.Migration) {
				m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
				m.Spec.Target.SynchronousCommit = v1beta1.SynchronousCommitLocal
			})
			m := failRun1(r, name, log)
			Expect(m.Status.RetryGate.CloneDoneSeen).To(Equal(seen))
			reconcileAndGet(ctx, r, name)
			Expect(fetchJob(ctx, name+"-gate-2").Annotations).To(HaveKeyWithValue(annotationRetryGateInputs, inputs))
		},
		Entry("marker logged", "mig-gate-clone-done", copyingLine+cloneDoneLine, "commit=1,local=1", true),
		Entry("no marker", "mig-gate-clone-owed", copyingLine, "commit=0,local=1", false),
	)
})
