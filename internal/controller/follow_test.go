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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

// jobFailedMsg is the canned failure message test helpers stamp on Jobs.
const jobFailedMsg = "pod failed"

// caughtUpLSN is the LSN specs park every sentinel field on to script a
// zero-lag stream.
const caughtUpLSN = "0/100"

// deletionDriveTimeout bounds the Eventually loops that drive multi-pass
// deletion flows; envtest passes are fast, this is generous.
const deletionDriveTimeout = 15 * time.Second

// fetchJob, finishJob, and removeMigration are the package-level Job and
// Migration helpers shared by every suite in this package.
func fetchJob(ctx context.Context, name string) *batchv1.Job {
	j := &batchv1.Job{}
	ExpectWithOffset(1, k8sClient.Get(ctx,
		types.NamespacedName{Name: name, Namespace: testNS}, j)).To(Succeed())
	return j
}

func finishJob(ctx context.Context, name string, succeeded bool) {
	j := fetchJob(ctx, name)
	now := metav1.Now()
	if j.Status.StartTime == nil {
		j.Status.StartTime = &now
	}
	if succeeded {
		j.Status.Succeeded = 1
		j.Status.CompletionTime = &now
		j.Status.Conditions = append(j.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
	} else {
		j.Status.Failed = 1
		j.Status.Conditions = append(j.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue,
				Reason: batchv1.JobReasonBackoffLimitExceeded, Message: jobFailedMsg},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				Reason: batchv1.JobReasonBackoffLimitExceeded, Message: jobFailedMsg})
	}
	ExpectWithOffset(1, k8sClient.Status().Update(ctx, j)).To(Succeed())
}

func removeMigration(ctx context.Context, name string) {
	m := &v1beta1.Migration{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m); err == nil {
		_ = k8sClient.Delete(ctx, m)
	}
	sel := []client.DeleteAllOfOption{
		client.InNamespace(testNS),
		client.MatchingLabels(map[string]string{labelMigration: name}),
	}
	_ = k8sClient.DeleteAllOf(ctx, &batchv1.Job{}, sel...)
	_ = k8sClient.DeleteAllOf(ctx, &corev1.PersistentVolumeClaim{}, sel...)
	_ = k8sClient.DeleteAllOf(ctx, &corev1.ConfigMap{}, sel...)
}

// fakeSentinel scripts the sentinel the way envtest scripts Job status: tests
// set the state (or an error), the reconciler reads it. calls counts every
// exec (reads, endpos sets, and nudges), so specs can prove copy-phase
// quiescence; nudges counts the endpos nudges on their own.
type fakeSentinel struct {
	mu        sync.Mutex
	state     *sentinel.State
	endposSet bool
	readErr   error
	setErr    error
	nudgeErr  error
	calls     int
	nudges    int
}

func (f *fakeSentinel) Read(_ context.Context, _, _ string) (*sentinel.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.readErr != nil {
		return nil, f.readErr
	}
	if f.state == nil {
		return nil, nil
	}
	s := *f.state
	return &s, nil
}

func (f *fakeSentinel) SetEndposCurrent(_ context.Context, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.setErr != nil {
		return "", f.setErr
	}
	f.endposSet = true
	f.state.Endpos = f.state.SourceHead
	return f.state.Endpos, nil
}

func (f *fakeSentinel) NudgeEndpos(_ context.Context, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.nudges++
	return f.nudgeErr
}

func (f *fakeSentinel) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeSentinel) nudgeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nudges
}

// fakeLogs scripts the pod-log reader the way fakeSentinel scripts the
// sentinel: tests choose what a pod "logged". out/err serve JobLogs, tsOut
// and tsErr the timestamped variant the zombie check reads.
type fakeLogs struct {
	out   string
	err   error
	tsOut string
	tsErr error
	// gotLines records the last requested tail budget, so tests can prove
	// the success-path parse asks for the whole log.
	gotLines int64
}

func (f *fakeLogs) JobLogs(_ context.Context, _, _ string, lines int64) ([]byte, error) {
	f.gotLines = lines
	return []byte(f.out), f.err
}

func (f *fakeLogs) JobLogsTimestamps(context.Context, string, string, int64) ([]byte, error) {
	return []byte(f.tsOut), f.tsErr
}

var _ = Describe("Migration Controller follow mode", func() {
	ctx := context.Background()

	followMigration := func(name string, mode v1beta1.CutoverMode) *v1beta1.Migration {
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: "pgoutput"}
		m.Spec.Cutover = v1beta1.CutoverSpec{Mode: mode}
		return m
	}

	// followReconciler wires the scripted sentinel into a fresh reconciler;
	// passes then go through the shared reconcileAndGet.
	followReconciler := func(fake *fakeSentinel) *MigrationReconciler {
		r := newReconciler()
		r.Sentinel = fake
		return r
	}

	// passPreflight drives the gate every follow migration now starts with:
	// the first pass creates only the preflight Job; its success unlocks run-1.
	passPreflight := func(r *MigrationReconciler, name string) *v1beta1.Migration {
		GinkgoHelper()
		m := reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-preflight", true)
		return m
	}

	It("walks preflight, streaming, caught-up, manual cutover, drain, and cleanup", func() {
		const name = "mig-follow-manual"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		r := followReconciler(fake)
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())

		// Pass 1 creates the preflight Job, not the worker: the finalizer is
		// already on (a preflight cannot leak a slot, but the order is fixed).
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Finalizers).To(ContainElement(finalizerName))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseValidating))
		Expect(fetchJob(ctx, name+"-preflight")).NotTo(BeNil())
		Expect(errors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: name + "-run-1", Namespace: testNS}, &batchv1.Job{}))).To(BeTrue())

		finishJob(ctx, name+"-preflight", true)
		reconcileAndGet(ctx, r, name)
		job := fetchJob(ctx, name+"-run-1")
		args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
		Expect(args).To(ContainSubstring("--follow"))
		Expect(args).To(ContainSubstring("--slot-name"))

		// Base copy still running: no phase change from the sentinel.
		fake.state = &sentinel.State{ApplyEnabled: false, WriteLSN: "0/10", ReplayLSN: "0/0", SourceHead: "0/20"}
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))

		// Apply on, lag far above threshold: Streaming, not caught up.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: "0/40000000", ReplayLSN: "0/10000000", SourceHead: "0/48000000", Endpos: sentinel.ZeroLSN}
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionStreaming)).To(BeTrue())
		Expect(meta.IsStatusConditionFalse(m.Status.Conditions, v1beta1.ConditionCaughtUp)).To(BeTrue())
		Expect(m.Status.Replication).NotTo(BeNil())
		Expect(*m.Status.Replication.LagBytes).To(BeNumerically(">", int64(16<<20)))

		// Caught up, Manual, not approved: waiting.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: "0/48000010", ReplayLSN: "0/48000000", SourceHead: "0/48000010", Endpos: sentinel.ZeroLSN}
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCutoverPending))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCaughtUp)).To(BeTrue())
		Expect(fake.endposSet).To(BeFalse())

		// Approval triggers the endpos, and the same pass nudges the stream:
		// on an idle source the drain only concludes once some WAL arrives,
		// so the operator emits a logical message right after setting endpos.
		m.Spec.Cutover.Approved = true
		Expect(k8sClient.Update(ctx, m)).To(Succeed())
		m = reconcileAndGet(ctx, r, name)
		Expect(fake.endposSet).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(fake.nudgeCount()).To(Equal(1))

		// Every later CuttingOver pass while the worker still runs re-nudges,
		// and a failing nudge stays a debug line, never a reconcile failure.
		fake.nudgeErr = fmt.Errorf("exec transport down")
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed)).To(BeFalse())
		Expect(fake.nudgeCount()).To(Equal(2))
		fake.nudgeErr = nil

		// Worker drains and exits 0: exit code alone is not trusted, a
		// verify Job must first prove the drain (origin progress, or a
		// data compare when the origin alone cannot decide).
		finishJob(ctx, name+"-run-1", true)
		m = reconcileAndGet(ctx, r, name)
		verifyJob := fetchJob(ctx, name+"-verify")
		Expect(verifyJob.Spec.Template.Spec.Containers[0].Args[1]).To(ContainSubstring("pg_replication_origin_progress"))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeFalse())

		// Verification passes: cutover completed, cleanup Job appears.
		finishJob(ctx, name+"-verify", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCutoverComplete)).To(BeTrue())
		cleanupJob := fetchJob(ctx, name+"-cleanup")
		cleanupArgs := strings.Join(cleanupJob.Spec.Template.Spec.Containers[0].Args, " ")
		Expect(cleanupArgs).To(HavePrefix("stream cleanup --dir /work/pgcopydb"))
		// The slot and origin are passed explicitly so the migration's own
		// generated origin is dropped, not the default "pgcopydb".
		Expect(cleanupArgs).To(ContainSubstring("--slot-name "))
		Expect(cleanupArgs).To(ContainSubstring("--origin "))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeFalse())

		// Cleanup succeeds: Complete.
		finishJob(ctx, name+"-cleanup", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCompleted))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeTrue())
	})

	It("polls at half speed during the base copy and full speed once streaming", func() {
		const name = "mig-clone-cadence"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		// nil Logs: this deliberately exercises the no-log-reader fallback,
		// where the sentinel stays the clone-done detector.
		r := followReconciler(fake)
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())
		passPreflight(r, name)
		reconcileAndGet(ctx, r, name) // run-1
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: testNS}}

		// Base copy running: nothing time-critical to observe, so the requeue
		// stretches to double the poll interval.
		fake.state = &sentinel.State{ApplyEnabled: false, WriteLSN: "0/10", ReplayLSN: "0/0", SourceHead: "0/20"}
		res, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(2 * pollInterval))

		// Apply on (base copy done): the fast cadence returns for cutover.
		// Manual mode without approval, so being caught up changes nothing.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		res, err = r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(pollInterval))
	})

	It("derives CloneCompleted from the worker log and keeps the sentinel quiet until then", func() {
		const name = "mig-clone-quiesce"
		defer removeMigration(ctx, name)
		// The scripted sentinel would report a caught-up stream right away;
		// if the reconciler asked, the phases would move. It must not ask:
		// on pgcopydb 0.18 every sentinel exec opens the SQLite catalogs the
		// copy phase is writing to, and concurrent access crashes workers.
		fake := &fakeSentinel{state: &sentinel.State{
			ApplyEnabled: true, WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN,
			SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN,
		}}
		r := followReconciler(fake)
		const ts = "2026-08-09T10:00:00.000000000Z "
		logs := &fakeLogs{tsOut: ts +
			`{"error_severity":"INFO","message":"STEP 10: restore the post-data section to the target database"}` + "\n"}
		r.Logs = logs
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())
		passPreflight(r, name)
		reconcileAndGet(ctx, r, name) // run-1

		// Copy still running (step banners only in the log): zero sentinel
		// execs, CloneCompleted stays False whatever the sentinel would say.
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeFalse())
		Expect(fake.callCount()).To(BeZero())

		// The worker logs the clone-done marker: CloneCompleted flips from
		// the log alone, and only then does the sentinel path engage, with
		// the usual Streaming/CaughtUp handling.
		logs.tsOut += ts +
			`{"error_severity":"INFO","message":"Updating the pgcopydb.sentinel to enable applying changes"}` + "\n"
		m = reconcileAndGet(ctx, r, name)
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeTrue())
		Expect(fake.callCount()).To(BeNumerically(">", 0))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionStreaming)).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCutoverPending))
	})

	It("cuts over automatically once caught up", func() {
		const name = "mig-follow-auto"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		r := followReconciler(fake)
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverAutomatic))).To(Succeed())
		passPreflight(r, name)
		reconcileAndGet(ctx, r, name)

		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		m := reconcileAndGet(ctx, r, name)
		Expect(fake.endposSet).To(BeTrue())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(fake.nudgeCount()).To(Equal(1))
	})

	It("fails loudly when drain verification is refuted", func() {
		const name = "mig-follow-lost"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		r := followReconciler(fake)
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())
		passPreflight(r, name)
		reconcileAndGet(ctx, r, name)

		// Worker exits 0 (the deceptive resume-after-crash case), but the
		// verify Job refutes the drain: no cleanup, absorbing Failed, and the
		// message says the slot is kept.
		finishJob(ctx, name+"-run-1", true)
		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-verify", false)
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed)).To(BeTrue())
		cond := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCutoverComplete)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Message).To(ContainSubstring("slot is kept"))
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-cleanup", Namespace: testNS}, &batchv1.Job{})
		Expect(errors.IsNotFound(err)).To(BeTrue())
	})

	It("routes deletion through slot cleanup", func() {
		const name = "mig-follow-delete"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		r := followReconciler(fake)
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())
		passPreflight(r, name)             // adds finalizer, clears the gate
		m := reconcileAndGet(ctx, r, name) // creates run-1
		Expect(m.Status.Attempts).To(Equal(int32(1)))

		Expect(k8sClient.Delete(ctx, m)).To(Succeed())
		// Pass 1: deletes the worker Job (foreground) and requeues.
		reconcileAndGet(ctx, r, name)
		// Pass 2+: worker gone (envtest: still terminating counts), then the
		// cleanup Job is created. Drive until it exists.
		Eventually(func() error {
			reconcileAndGet(ctx, r, name)
			j := &batchv1.Job{}
			return k8sClient.Get(ctx, types.NamespacedName{Name: name + "-cleanup", Namespace: testNS}, j)
		}).WithTimeout(deletionDriveTimeout).Should(Succeed())

		finishJob(ctx, name+"-cleanup", true)
		// Final pass removes the finalizer; the CR then disappears, so this
		// drives Reconcile bare instead of through reconcileAndGet.
		Eventually(func() bool {
			_, _ = r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
			})
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, &v1beta1.Migration{})
			return errors.IsNotFound(err)
		}).WithTimeout(deletionDriveTimeout).Should(BeTrue())
	})

	It("reconciles a follow spec that spells allowMissingReplicaIdentity as []", func() {
		const name = "mig-follow-empty-list"
		defer removeMigration(ctx, name)
		r := followReconciler(&fakeSentinel{})

		// The typed client's own omitempty marshal would strip the empty
		// list, so the CR goes in as unstructured, the way a rendered
		// manifest (an Argo Workflows template, say) spells it.
		obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(followMigration(name, v1beta1.CutoverManual))
		Expect(err).NotTo(HaveOccurred())
		Expect(unstructured.SetNestedSlice(obj, []any{}, "spec", "follow", "allowMissingReplicaIdentity")).To(Succeed())
		u := &unstructured.Unstructured{Object: obj}
		u.SetGroupVersionKind(v1beta1.GroupVersion.WithKind("Migration"))
		Expect(k8sClient.Create(ctx, u)).To(Succeed())

		// Pass 1 must survive the stored []: finalizer on, status written.
		m := reconcileAndGet(ctx, r, name)
		Expect(m.Finalizers).To(ContainElement(finalizerName))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseValidating))

		// The stored spelling survives the controller's write untouched.
		stored := &unstructured.Unstructured{}
		stored.SetGroupVersionKind(v1beta1.GroupVersion.WithKind("Migration"))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, stored)).To(Succeed())
		list, found, err := unstructured.NestedSlice(stored.Object, "spec", "follow", "allowMissingReplicaIdentity")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(list).To(BeEmpty())

		// Deletion removes the finalizer with the same constraint. Nothing
		// ran yet (attempts 0), so a single pass goes straight to removal
		// and the CR is gone; asserting the pass keeps a regression's 422
		// in the failure output instead of a bare timeout.
		Expect(k8sClient.Delete(ctx, m)).To(Succeed())
		_, err = r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: testNS},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(errors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: name, Namespace: testNS}, &v1beta1.Migration{}))).To(BeTrue())
	})

	It("absorbs a preflight failure with the check output in the conditions", func() {
		const name = "mig-preflight-fail"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		r := followReconciler(fake)
		r.Logs = &fakeLogs{out: "preflight: source wal_level is 'replica', follow needs 'logical': set wal_level = logical on the source and restart it\n"}
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())

		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-preflight", false)
		m := reconcileAndGet(ctx, r, name)

		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated.Status).To(Equal(metav1.ConditionFalse))
		Expect(validated.Reason).To(Equal("PreflightFailed"))
		Expect(validated.Message).To(ContainSubstring("wal_level is 'replica'"))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Status).To(Equal(metav1.ConditionTrue))
		Expect(failed.Reason).To(Equal("PreflightFailed"))
		Expect(failed.Message).To(ContainSubstring("set wal_level = logical"))

		// Absorbing: no worker ever starts, further passes change nothing.
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Attempts).To(Equal(int32(0)))
		Expect(errors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: name + "-run-1", Namespace: testNS}, &batchv1.Job{}))).To(BeTrue())
	})

	It("degrades to a log pointer when the preflight output is unreadable", func() {
		const name = "mig-preflight-nologs"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		r := followReconciler(fake)
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())

		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-preflight", false)
		// nil LogReader: envtest's default, and the operator's stance when
		// the pod is already gone.
		m := reconcileAndGet(ctx, r, name)

		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		failed := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed.Reason).To(Equal("PreflightFailed"))
		Expect(failed.Message).To(ContainSubstring("inspect the logs of Job " + name + "-preflight"))
	})

	It("surfaces why the preflight pod cannot start", func() {
		const name = "mig-preflight-stuck"
		defer removeMigration(ctx, name)
		r := followReconciler(&fakeSentinel{})
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())

		// Pass 1 creates the preflight Job; nothing runs it in envtest, so
		// the gate reports a running preflight.
		m := reconcileAndGet(ctx, r, name)
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated.Status).To(Equal(metav1.ConditionUnknown))
		Expect(validated.Reason).To(Equal("PreflightRunning"))
		Expect(validated.Message).To(ContainSubstring("is running"))

		// envtest runs no kubelet: fabricate the Job's pod in the state a
		// misnamed credentials Secret produces on a real cluster.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name + "-preflight-pod",
				Namespace: testNS,
				Labels:    map[string]string{jobNameLabel: name + "-preflight"},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: testPodContainer, Image: testPodImage}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, pod) }()
		pod.Status = corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  testPodContainer,
				Image: testPodImage,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "CreateContainerConfigError",
					Message: `secret "shop-source" not found`,
				}},
			}},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseValidating))
		validated = meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated.Status).To(Equal(metav1.ConditionUnknown))
		Expect(validated.Reason).To(Equal("PreflightRunning"))
		Expect(validated.Message).To(ContainSubstring("CreateContainerConfigError"))
		Expect(validated.Message).To(ContainSubstring(`secret "shop-source" not found`))
	})

	It("validates only after the gate and emits PreflightPassed", func() {
		const name = "mig-preflight-pass"
		defer removeMigration(ctx, name)
		r := followReconciler(&fakeSentinel{})
		Expect(k8sClient.Create(ctx, followMigration(name, v1beta1.CutoverManual))).To(Succeed())

		passPreflight(r, name)
		m := reconcileAndGet(ctx, r, name)
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated.Status).To(Equal(metav1.ConditionTrue))
		Expect(validated.Reason).To(Equal("SpecValid"))
		Expect(drainEvents(r.Recorder.(*events.FakeRecorder))).To(ContainElement(ContainSubstring("PreflightPassed")))
	})
})
