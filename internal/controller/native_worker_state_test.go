package controller

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Node keys the T2 drift tests edit.
const (
	nativeT2PoolKey  = "pool"
	nativeT2TaintKey = "dedicated"
)

// nativeTestDigest is the admission digest a receipt records for worker.
func nativeTestDigest(t interface {
	Helper()
	Fatalf(string, ...any)
}, worker *batchv1.Job) string {
	t.Helper()
	sum, err := nativeJobSHA256(worker)
	if err != nil {
		t.Fatalf("admission digest: %v", err)
	}
	return sum
}

// nativeTestIntentPatch reports the status patch that records a worker intent before its Create.
func nativeTestIntentPatch(obj client.Object) bool {
	m, ok := obj.(*v1beta1.Migration)
	return ok && m.Status.NativeWorkerIntent != ""
}

// nativeTestAfterIntent lets the intent patch through and hands every later status patch to next.
func nativeTestAfterIntent(next func(context.Context, client.Client, client.Object, client.Patch) error) func(context.Context, client.Client, client.Object, client.Patch) error {
	return func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
		if nativeTestIntentPatch(obj) {
			return c.Status().Patch(ctx, obj, p)
		}
		return next(ctx, c, obj, p)
	}
}

// Two Jobs have equal admission digests exactly when nativePreflightJobMatches admits them (q2).
// The actual Job always has a UID and no deletionTimestamp; nativeWorkerAdmitted checks both apart.
func TestNativeJobDigestMatchesComparator(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	expected := f.expected()
	expected.Annotations = map[string]string{nativeWorkerIntentAnnotation: "1/" + strings.Repeat("a", 32)}
	for _, tc := range []struct {
		name string
		edit func(*batchv1.Job)
	}{
		{"identical", func(*batchv1.Job) {}},
		{"API defaults and controller fields", func(j *batchv1.Job) {
			j.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{batchv1.ControllerUidLabel: string(j.UID)}}
			labels := maps.Clone(j.Spec.Template.Labels)
			labels[batchv1.ControllerUidLabel], labels[batchv1.JobNameLabel] = string(j.UID), j.Name
			labels[nativeWorkerLegacyControllerLabel], labels[nativeWorkerLegacyJobNameLabel] = string(j.UID), j.Name
			j.Spec.Template.Labels = labels
			nativeWorkerDefaults(j)
			j.Spec.Template.Spec.HostUsers = ptr.To(true)
		}},
		{"empty instead of nil", func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{}
			j.Finalizers = []string{}
		}},
		{"controller label of another Job", func(j *batchv1.Job) {
			j.Spec.Template.Labels = maps.Clone(j.Spec.Template.Labels)
			j.Spec.Template.Labels[batchv1.ControllerUidLabel] = "another-job"
		}},
		{"label edit", func(j *batchv1.Job) { j.Labels = maps.Clone(j.Labels); j.Labels["edited"] = nativeT2PoolKey }},
		{"intent nonce", func(j *batchv1.Job) {
			j.Annotations = map[string]string{nativeWorkerIntentAnnotation: "1/" + strings.Repeat("b", 32)}
		}},
		{"owner reference", func(j *batchv1.Job) { j.OwnerReferences = nil }},
		{"finalizer", func(j *batchv1.Job) { j.Finalizers = []string{"example.test/hold"} }},
		{"container resources", func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("9Gi")}
		}},
		{"node selector", func(j *batchv1.Job) { j.Spec.Template.Spec.NodeSelector = map[string]string{nativeT2PoolKey: "db"} }},
		{"toleration", func(j *batchv1.Job) {
			j.Spec.Template.Spec.Tolerations = append(j.Spec.Template.Spec.Tolerations, corev1.Toleration{Key: nativeT2TaintKey, Operator: corev1.TolerationOpExists})
		}},
		{"host users off", func(j *batchv1.Job) { j.Spec.Template.Spec.HostUsers = ptr.To(false) }},
		{"runtime class", func(j *batchv1.Job) { j.Spec.Template.Spec.RuntimeClassName = ptr.To("other") }},
		{"suspended Job", func(j *batchv1.Job) { j.Spec.Suspend = ptr.To(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := expected.DeepCopy()
			actual.UID = workerServerUID
			tc.edit(actual)
			want, wantErr := nativeJobSHA256(expected)
			got, gotErr := nativeJobSHA256(actual)
			equal := wantErr == nil && gotErr == nil && want == got
			if matches := nativePreflightJobMatches(expected, actual); equal != matches {
				t.Fatalf("digests equal=%v but comparator matches=%v", equal, matches)
			}
		})
	}
}

// A finished worker with no pod is anchored pod-less; a pod that appears later is a change (R1).
func TestNativeQuiescenceRefusesPodAfterPodlessAnchor(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	ctx := context.Background()
	if err := f.store.Delete(ctx, f.pod); err != nil {
		t.Fatal(err)
	}
	_, anchor, err := f.r.nativeRecoveryQuiescentCurrent(ctx, f.current(), f.worker, f.pvc, nil)
	if err != nil || !nativePodless(anchor) {
		t.Fatalf("finished worker without a pod was not anchored pod-less: %v", err)
	}
	if _, _, err := f.r.nativeRecoveryQuiescentCurrent(ctx, f.current(), f.worker, f.pvc, anchor); err != nil {
		t.Fatalf("unchanged pod-less worker refused: %v", err)
	}
	late := nativeRecoveryTestPod(f.worker, 1)
	if err := f.store.Create(ctx, late); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.r.nativeRecoveryQuiescentCurrent(ctx, f.current(), f.worker, f.pvc, anchor); err == nil {
		t.Fatal("a pod that appeared after the pod-less anchor was admitted")
	}
	if _, pod, err := f.r.nativeRecoveryQuiescentCurrent(ctx, f.current(), f.worker, f.pvc, nil); err != nil || pod == nil || pod.UID != late.UID {
		t.Fatalf("a fresh observation did not return the present pod: %v", err)
	}
}

// R1 finds a running pod by its controller-uid label even after its owner reference is gone.
func TestNativeEngineFreeFindsOrphanedPodByLabel(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	ctx := context.Background()
	orphan := f.pod.DeepCopy()
	orphan.OwnerReferences = nil
	orphan.Labels = map[string]string{batchv1.ControllerUidLabel: string(f.worker.UID)}
	orphan.Status = corev1.PodStatus{Phase: corev1.PodRunning}
	if err := f.store.Delete(ctx, f.pod); err != nil {
		t.Fatal(err)
	}
	orphan.ResourceVersion = ""
	if err := f.store.Create(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.nativeWorkerEngineFree(ctx, f.worker); !errors.Is(err, errNativeRecoveryActive) {
		t.Fatalf("a running orphaned worker pod read as engine-free: %v", err)
	}
	if err := f.reconcile(t); err != nil {
		t.Fatalf("retry wait for the orphaned pod: %v", err)
	}
	held := meta.FindStatusCondition(f.current().Status.Conditions, v1beta1.ConditionRetryHeld)
	if f.workerCreates != 0 || held == nil || held.Reason != reasonWorkerPodRemains || !strings.Contains(held.Message, orphan.Name) {
		t.Fatalf("retry did not wait for the orphaned pod: creates=%d held=%+v", f.workerCreates, held)
	}
}

// q4: the intent is durable before the Create, and the receipt follows it.
func TestNativeIntentPrecedesCreate(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	var ops []string
	f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
		if nativeTestIntentPatch(obj) {
			ops = append(ops, "intent")
		} else {
			ops = append(ops, "receipt")
		}
		return c.Status().Patch(ctx, obj, p)
	}
	f.create = func(ctx context.Context, c client.WithWatch, obj client.Object) error {
		ops = append(ops, "create")
		job := obj.(*batchv1.Job)
		_, annotation, _, ok := nativeIntentParts(f.storedMigration().Status.NativeWorkerIntent)
		if !ok || job.Annotations[nativeWorkerIntentAnnotation] != annotation {
			t.Fatal("the created worker does not carry the recorded intent")
		}
		obj.SetUID(workerServerUID)
		return c.Create(ctx, obj)
	}
	if _, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ops, []string{"intent", "create", "receipt"}) {
		t.Fatalf("operation order %v", ops)
	}
	got := f.storedMigration()
	if got.Status.NativeWorkerIntent != "" || got.Status.NativeJobSHA256 == "" {
		t.Fatal("the receipt did not clear the intent and record the digest")
	}
}

// nativeTestNextWorker creates jobName(attempt) owned by the Migration, carrying annotation if set.
func nativeTestNextWorker(t *testing.T, f *workerAttemptFixture, annotation string) *batchv1.Job {
	t.Helper()
	job := f.expected()
	if annotation != "" {
		job.Annotations = map[string]string{nativeWorkerIntentAnnotation: annotation}
	}
	job.UID = workerServerUID
	if err := f.store.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return job
}

// q4: adoption needs the intent's nonce, not only its digest.
func TestNativeForeignNonceNotAdopted(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	job := nativeTestNextWorker(t, f, "1/"+strings.Repeat("b", 32))
	m := f.storedMigration()
	m.Status.NativeWorkerIntent = "1/" + strings.Repeat("a", 32) + "/" + nativeTestDigest(t, job)
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.storedMigration()); err != nil {
		t.Fatal(err)
	}
	got := f.storedMigration()
	if got.Status.RetryGate == nil || got.Status.RetryGate.Exemption != v1beta1.RetryGateCounted || got.Status.NativeWorkerIntent != "" || len(f.r.Recorder.(*events.FakeRecorder).Events) != 0 {
		t.Fatal("a worker with a foreign nonce was adopted", got.Status.RetryGate)
	}
}

// q4: the matching intent adopts the worker and announces it.
func TestNativeMatchingIntentAdopted(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	annotation := "1/" + strings.Repeat("a", 32)
	job := nativeTestNextWorker(t, f, annotation)
	m := f.storedMigration()
	m.Status.NativeWorkerIntent = annotation + "/" + nativeTestDigest(t, job)
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.storedMigration()); err != nil {
		t.Fatal(err)
	}
	got := f.storedMigration()
	if got.Status.Attempts != 1 || got.Status.RetryGate != nil || got.Status.NativeJobSHA256 != nativeTestDigest(t, job) || got.Status.NativeWorkerIntent != "" || f.creates != 0 {
		t.Fatal("the intended worker was not adopted", got.Status)
	}
	if event := <-f.r.Recorder.(*events.FakeRecorder).Events; !strings.Contains(event, "AttemptStarted") || !strings.Contains(event, "adopted") {
		t.Fatalf("adoption event %q", event)
	}
}

// q4 and R7: an owned next-name Job without a matching intent is recorded as a counted attempt,
// then dropped while it runs, and the gate starts the next attempt after it.
func TestNativeStaleIntentJobCountedAndDropped(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	ctx := context.Background()
	stale := f.worker.DeepCopy()
	stale.Name, stale.UID, stale.ResourceVersion = jobName(f.m, 2), "stale-worker", ""
	stale.Annotations = map[string]string{nativeWorkerIntentAnnotation: "2/" + strings.Repeat("c", 32)}
	stale.Status = batchv1.JobStatus{Active: 1, Ready: ptr.To(int32(1))}
	if err := f.store.Create(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if _, err := f.dispatch(t); err != nil {
		t.Fatal(err)
	}
	got := f.current()
	if got.Status.Attempts != 2 || got.Status.NativeJobUID != string(stale.UID) || got.Status.RetryGate == nil || got.Status.RetryGate.FailedAttempt != 2 || got.Status.RetryGate.Exemption != v1beta1.RetryGateCounted {
		t.Fatalf("stale worker was adopted or left unrecorded: %+v", got.Status.RetryGate)
	}
	if _, err := f.dispatch(t); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Get(ctx, client.ObjectKeyFromObject(stale), &batchv1.Job{}); err == nil {
		t.Fatal("R7 left the counted attempt's running worker in place")
	}
	for range 4 {
		if _, err := f.dispatch(t); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.current(); got.Status.Attempts != 3 || f.workerCreates != 1 {
		t.Fatalf("the gate did not start the next attempt after the drop: attempts=%d creates=%d", got.Status.Attempts, f.workerCreates)
	}
}

// q4: an intent whose Job never landed is cleared, and no Create happens in that pass.
func TestNativeAbandonedIntentAdmitsAgain(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	m := f.storedMigration()
	m.Status.NativeRunnerImage, m.Status.Phase = nativeEvidenceTestImage, v1beta1.PhasePending
	m.Status.NativeWorkerIntent = "1/" + strings.Repeat("a", 32) + "/" + strings.Repeat("d", 64)
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.storedMigration()); err != nil {
		t.Fatal(err)
	}
	if got := f.storedMigration(); got.Status.NativeWorkerIntent != "" || f.creates != 0 || got.Status.Attempts != 0 {
		t.Fatalf("abandoned intent: intent=%q creates=%d", got.Status.NativeWorkerIntent, f.creates)
	}
}

// The drop primitive deletes with a UID precondition and foreground propagation, and reports the
// worker gone only when no pod carries its UID by owner or label.
func TestNativeDropWorkerWaitsForEveryPod(t *testing.T) {
	for _, orphan := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned pods only", true: "orphaned labelled pod"}[orphan], func(t *testing.T) {
			f := newNativeDispositionFixture(t, true)
			ctx := context.Background()
			job := f.worker.DeepCopy()
			job.Name, job.UID, job.ResourceVersion = jobName(f.m, 2), "dropped-worker", ""
			if err := f.store.Create(ctx, job); err != nil {
				t.Fatal(err)
			}
			if orphan {
				pod := nativeRecoveryTestPod(job, 1)
				pod.Name, pod.UID, pod.OwnerReferences = "orphan", "orphan-pod", nil
				pod.Labels = map[string]string{batchv1.ControllerUidLabel: string(job.UID)}
				pod.Status.Phase = corev1.PodRunning
				if err := f.store.Create(ctx, pod); err != nil {
					t.Fatal(err)
				}
			}
			var options client.DeleteOptions
			f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				options.ApplyOptions(opts)
				return c.Delete(ctx, obj, opts...)
			}})
			bounded, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			done, err := f.r.dropNativeWorker(bounded, f.current(), job)
			if err != nil || done == orphan || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != job.UID ||
				options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
				t.Fatalf("drop done=%v err=%v options=%+v", done, err, options)
			}
		})
	}
}

// The stop primitive drops a worker whose container never started instead of waiting for it.
func TestNativeStopOrDropDropsUnstartedWorker(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	ctx := context.Background()
	worker := &batchv1.Job{}
	if err := f.store.Get(ctx, client.ObjectKeyFromObject(f.worker), worker); err != nil {
		t.Fatal(err)
	}
	worker.Status = batchv1.JobStatus{Active: 1, Ready: ptr.To(int32(0))}
	if err := f.store.Status().Update(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Delete(ctx, f.pod); err != nil {
		t.Fatal(err)
	}
	f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{})
	done, err := f.r.stopOrDropNativeWorker(ctx, f.current(), worker, f.pvc)
	if err != nil || !done || f.store.Get(ctx, client.ObjectKeyFromObject(worker), &batchv1.Job{}) == nil {
		t.Fatalf("unstarted worker was not dropped: done=%v err=%v", done, err)
	}
}

// V table, retry column: a claim under the work PVC name that is not this Migration's holds attempt 1.
func TestNativeForeignWorkClaimHoldsFirstAttempt(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	f.updatePVC(func(pvc *corev1.PersistentVolumeClaim) { pvc.OwnerReferences = nil })
	m := f.storedMigration()
	m.Status.NativeRunnerImage, m.Status.Phase = nativeEvidenceTestImage, v1beta1.PhasePending
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDispatch(t, f.r, f.storedMigration()); err != nil {
		t.Fatal(err)
	}
	blocked := meta.FindStatusCondition(f.storedMigration().Status.Conditions, v1beta1.ConditionBlocked)
	if blocked == nil || blocked.Reason != v1beta1.ReasonForeignObject || f.creates != 0 {
		t.Fatalf("foreign work claim did not hold the first attempt: %+v creates=%d", blocked, f.creates)
	}
}

// Design A's flip of TestNativeRecoveryRoutes/missing_pod, as pairs: the engine-free check admits a
// finished Job without a pod, refuses an active or terminating one, and the log readers still need the pod.
func TestNativeEngineFreePodlessPairs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status func(*batchv1.JobStatus)
		admit  bool
	}{
		{"finished job", func(*batchv1.JobStatus) {}, true},
		{"active job", func(s *batchv1.JobStatus) { *s = batchv1.JobStatus{Active: 1, Ready: ptr.To(int32(0))} }, false},
		{"terminating job", func(s *batchv1.JobStatus) { s.Terminating = ptr.To(int32(1)) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeRecoveryFixture(t, 2)
			ctx := context.Background()
			if err := f.store.Delete(ctx, f.pod); err != nil {
				t.Fatal(err)
			}
			worker := f.worker.DeepCopy()
			tc.status(&worker.Status)
			f.replace(worker)
			if err := f.store.Get(ctx, client.ObjectKeyFromObject(worker), worker); err != nil {
				t.Fatal(err)
			}
			pod, err := f.r.nativeEngineFreeWorker(ctx, f.m, worker, f.pvc)
			if admitted := err == nil && nativePodless(pod); admitted != tc.admit || !tc.admit && !errors.Is(err, errNativeRecoveryActive) {
				t.Fatalf("engine-free admitted=%v err=%v, want %v", admitted, err, tc.admit)
			}
			if pod, err := f.r.nativeQuiescentWorker(ctx, f.m, worker, f.pvc); err == nil || pod != nil {
				t.Fatal("a log reader admitted a worker without its pod")
			}
		})
	}
}

// q4: the receipt records only the Job the recorded intent names; a changed intent refuses it.
func TestNativeReceiptRequiresRecordedIntent(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	f.beforeRead = func(obj client.Object, n int) {
		if _, ok := obj.(*v1beta1.Migration); ok && n == 3 {
			m := f.storedMigration()
			_, _, digest, _ := nativeIntentParts(m.Status.NativeWorkerIntent)
			m.Status.NativeWorkerIntent = "1/" + strings.Repeat("e", 32) + "/" + digest
			if err := f.store.Status().Update(context.Background(), m); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy()); err == nil {
		t.Fatal("a receipt was recorded under a changed intent")
	}
	if got := f.storedMigration(); got.Status.Attempts != 0 || got.Status.NativeJobUID != "" || f.creates != 1 {
		t.Fatalf("changed intent: attempts=%d uid=%q creates=%d", got.Status.Attempts, got.Status.NativeJobUID, f.creates)
	}
}
