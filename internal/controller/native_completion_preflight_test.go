package controller

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type nativeCompletionConsumer interface {
	nativeCompletionCurrent(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim, *corev1.Pod) (*v1beta1.Migration, *corev1.Pod, error)
	ensureNativeCompletionPreflight(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (*native.Version, bool, string, error)
}

func completionConsumer(t *testing.T, r *MigrationReconciler) nativeCompletionConsumer {
	t.Helper()
	consumer, ok := any(r).(nativeCompletionConsumer)
	if !ok {
		t.Fatal("completion-specific discovery admission is unavailable")
	}
	return consumer
}

func TestNativeCompletionCurrent(t *testing.T) {
	t.Run("actual defaulted successful worker", func(t *testing.T) {
		f := newNativeCloneFixture(t, false, nil)
		defaultProbeJob(f.worker)
		f.replace(f.worker)
		f.pod = nativeRecoveryTestPod(f.worker, 0)
		f.replace(f.pod)
		if live, pod, err := completionConsumer(t, f.r).nativeCompletionCurrent(context.Background(), f.m, f.worker, f.pvc, f.pod); err != nil || live == nil || pod == nil {
			t.Fatal("actual defaulted retained worker refused", err)
		}
	})
	for _, follow := range []bool{false, true} {
		for _, phase := range []v1beta1.MigrationPhase{v1beta1.PhaseCloning, v1beta1.PhaseCompleted, v1beta1.PhaseFailed, v1beta1.PhaseSuspended} {
			t.Run(string(phase)+" follow="+strconv.FormatBool(follow), func(t *testing.T) {
				f := newNativeCloneFixture(t, follow, nil)
				f.m.Status.Phase = phase
				f.m.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "Stale", LastTransitionTime: metav1.Now()}, {Type: v1beta1.ConditionFailed, Status: metav1.ConditionTrue, Reason: "Stale", LastTransitionTime: metav1.Now()}}
				if follow {
					f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: caughtUpLSN}
				}
				if err := f.store.Status().Update(context.Background(), f.m); err != nil {
					t.Fatal(err)
				}
				live, pod, err := completionConsumer(t, f.r).nativeCompletionCurrent(context.Background(), f.m, f.worker, f.pvc, f.pod)
				if err != nil || live == nil || pod == nil || pod.UID != f.pod.UID || !apiequality.Semantic.DeepEqual(live.Status, f.m.Status) {
					t.Fatalf("retained successful execution refused: %v", err)
				}
			})
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*nativeCloneFixture)
	}{
		{"suspended intent", func(f *nativeCloneFixture) { f.m.Spec.Suspend = true; f.replace(f.m) }},
		{"deleting intent", func(f *nativeCloneFixture) {
			f.m.Finalizers = []string{"hold"}
			f.replace(f.m)
			if err := f.store.Delete(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
		}},
		{"generation changed", func(f *nativeCloneFixture) { m := f.m.DeepCopy(); m.Generation++; f.replace(m) }},
		{"spec changed", func(f *nativeCloneFixture) { m := f.m.DeepCopy(); m.Spec.Target.Database = "different"; f.replace(m) }},
		{"missing receipts", func(f *nativeCloneFixture) { f.m.Status.NativeJobUID, f.m.Status.NativePVCUID = "", ""; f.replace(f.m) }},
		{"half receipts", func(f *nativeCloneFixture) { f.m.Status.NativePVCUID = ""; f.replace(f.m) }},
		{"wrong receipt", func(f *nativeCloneFixture) { f.m.Status.NativeJobUID = string(workerForeignIdentity); f.replace(f.m) }},
		{"attempt changed", func(f *nativeCloneFixture) { f.m.Status.Attempts++; f.replace(f.m) }},
		{"worker replaced", func(f *nativeCloneFixture) {
			worker := f.worker.DeepCopy()
			worker.UID = probeReplacement
			f.replace(worker)
		}},
		{"worker generation changed", func(f *nativeCloneFixture) { worker := f.worker.DeepCopy(); worker.Generation++; f.replace(worker) }},
		{"worker command changed", func(f *nativeCloneFixture) {
			worker := f.worker.DeepCopy()
			worker.Spec.Template.Spec.Containers[0].Args = []string{probeTestVersionCommand}
			f.replace(worker)
		}},
		{"failed worker", func(f *nativeCloneFixture) {
			f.worker.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
			f.replace(f.worker)
		}},
		{"PVC replaced", func(f *nativeCloneFixture) { pvc := f.pvc.DeepCopy(); pvc.UID = probeReplacement; f.replace(pvc) }},
		{"original Pod replaced", func(f *nativeCloneFixture) { pod := f.pod.DeepCopy(); pod.UID = probeReplacement; f.replace(pod) }},
		{"duplicate Pod", func(f *nativeCloneFixture) {
			pod := f.pod.DeepCopy()
			pod.Name += "-duplicate"
			pod.ResourceVersion = ""
			if err := f.store.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
		}},
		{"container restart", func(f *nativeCloneFixture) {
			pod := f.pod.DeepCopy()
			pod.Status.ContainerStatuses[0].RestartCount++
			f.replace(pod)
		}},
		{"follow missing E", func(f *nativeCloneFixture) { f.m.Status.Replication = nil; f.replace(f.m) }},
		{"follow changed E", func(f *nativeCloneFixture) { m := f.m.DeepCopy(); m.Status.Replication.Endpos = "0/200"; f.replace(m) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeCloneFixture(t, true, nil)
			f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: caughtUpLSN}
			if err := f.store.Status().Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			tc.change(f)
			if live, pod, err := completionConsumer(t, f.r).nativeCompletionCurrent(context.Background(), f.m, f.worker, f.pvc, f.pod); err == nil || live != nil || pod != nil {
				t.Fatal("changed or unrecorded execution admitted")
			}
		})
	}
}

type nativeCompletionProbeReader struct{ preflightFixtureReader }

func (r nativeCompletionProbeReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
		return r.f.store.Get(ctx, key, obj, opts...)
	}
	return r.preflightFixtureReader.Get(ctx, key, obj, opts...)
}

func newCompletionProbe(t *testing.T, follow bool) (*preflightFixture, *batchv1.Job, *corev1.PersistentVolumeClaim, *corev1.Pod) {
	t.Helper()
	f := newPreflightFixture(t)
	if follow {
		f.m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
		if err := f.store.Update(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: caughtUpLSN}
	}
	_, pvc := nativeWorkerJobInputs()
	pvc.Status.Phase = corev1.ClaimBound
	f.m.Status.Attempts, f.m.Status.JobName, f.m.Status.Phase = 1, jobName(f.m, 1), v1beta1.PhaseCompleted
	worker, err := buildNativeWorkerJob(f.m, pvc, f.r.RPGMigrateRunnerImage, 1)
	if err != nil || controllerutil.SetControllerReference(f.m, worker, f.r.Scheme) != nil {
		t.Fatal("build retained worker", err)
	}
	worker.UID, worker.Generation = "retained-worker", 1
	worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	f.m.Status.NativeJobUID, f.m.Status.NativePVCUID = string(worker.UID), string(pvc.UID)
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	pod := nativeRecoveryTestPod(worker, 0)
	for _, obj := range []client.Object{pvc, worker, pod} {
		if err := f.store.Create(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
	}
	f.r.APIReader = nativeCompletionProbeReader{preflightFixtureReader{f}}
	f.onCreate = func(job *batchv1.Job) {
		if !strings.HasPrefix(job.Name, probeVersion) || len(job.Spec.Template.Spec.Containers[0].Env) != 0 || len(job.Spec.Template.Spec.Volumes) != 0 {
			t.Fatal("completion discovery projected credentials or ran provisioning")
		}
	}
	return f, worker, pvc, pod
}

func TestNativeCompletionPreflight(t *testing.T) {
	t.Run("version output", testNativeCompletionPreflightOutput)
	t.Run("helper lifecycle", testNativeCompletionPreflightLifecycle)
}

func testNativeCompletionPreflightOutput(t *testing.T) {
	t.Run("follow preserves saved E with actual full capability discovery", func(t *testing.T) {
		f, worker, pvc, _ := newCompletionProbe(t, true)
		raw, err := os.ReadFile("../native/testdata/v1/payloads/version-qualified-linux-profile.json")
		if err != nil {
			t.Fatal(err)
		}
		v, err := native.ParseVersion(raw)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		f.version = append(payload, '\n')
		_, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(context.Background(), f.m, worker, pvc)
		if err != nil || !passed || failure != "" || nativeControlEnd(f.m) != caughtUpLSN || f.creates != 1 || f.deletes != 1 {
			t.Fatal("follow discovery changed E or ran provisioning", failure, err)
		}
	})
	t.Run("internal deadline keeps cleanup reservation", func(t *testing.T) {
		f, worker, pvc, _ := newCompletionProbe(t, false)
		start := time.Now().Add(-59800 * time.Millisecond)
		f.r.now = func() time.Time { return start }
		f.pending = true
		v, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(context.Background(), f.m, worker, pvc)
		if v != nil || passed || failure != "" || err != nil || f.creates != 1 || f.deletes != 1 {
			t.Fatal("pending version timed out without original cleanup reservation", failure, err)
		}
	})
	t.Run("helper replaced after output", func(t *testing.T) {
		f, worker, pvc, _ := newCompletionProbe(t, false)
		f.onLog = func(job *batchv1.Job) {
			if err := f.store.Delete(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			job.UID, job.ResourceVersion = "replacement-helper-job", ""
			if err := f.store.Create(context.Background(), job); err != nil {
				t.Fatal(err)
			}
		}
		v, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(context.Background(), f.m, worker, pvc)
		f.assertNoAuthority(v, passed, failure, err)
		if f.deletes != 0 {
			t.Fatal("replacement helper was deleted")
		}
	})
	for _, raw := range []string{"", probeVersionLine + probeVersionLine, "private-discovery-canary\n"} {
		t.Run("malformed version "+string(rune('0'+len(raw)%10)), func(t *testing.T) {
			f, worker, pvc, _ := newCompletionProbe(t, false)
			f.version = []byte(raw)
			v, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(context.Background(), f.m, worker, pvc)
			f.assertNoAuthority(v, passed, failure, err)
			if strings.Contains(failure, "private-discovery-canary") || f.creates != 1 || f.deletes != 1 {
				t.Fatal("malformed discovery escaped closure or leaked output")
			}
		})
	}
}

func testNativeCompletionPreflightLifecycle(t *testing.T) {
	t.Run("actual credential-free version only", func(t *testing.T) {
		f, worker, pvc, _ := newCompletionProbe(t, false)
		v, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(context.Background(), f.m, worker, pvc)
		if err != nil || !passed || failure != "" || v == nil || v.EngineVersion != "rpgmigrate-test" || f.creates != 1 || f.deletes != 1 || f.logs.Load() != 1 {
			t.Fatalf("version discovery failed: passed=%v failure=%s error=%v creates=%d", passed, failure, err, f.creates)
		}
		if f.m.Status.Replication != nil {
			t.Fatal("plain clone invented E")
		}
	})
	t.Run("historical six-name profile before credentials", func(t *testing.T) {
		f, worker, pvc, _ := newCompletionProbe(t, false)
		raw, err := os.ReadFile("../native/testdata/v1/payloads/version-qualified-profile.json")
		if err != nil {
			t.Fatal(err)
		}
		v, err := native.ParseVersion(raw)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		f.version = append(payload, '\n')
		v, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(context.Background(), f.m, worker, pvc)
		f.assertNoAuthority(v, passed, failure, err)
		if f.creates != 1 || f.deletes != 1 || f.logs.Load() != 1 {
			t.Fatal("unqualified version escaped discovery closure")
		}
	})
	for _, mode := range []string{string(metav1.StatusReasonAlreadyExists), workerEmptyUIDCase, workerLostAckCase} {
		t.Run(mode, func(t *testing.T) {
			f, worker, pvc, _ := newCompletionProbe(t, false)
			f.createMode = mode
			v, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(context.Background(), f.m, worker, pvc)
			f.assertNoAuthority(v, passed, failure, err)
			if f.logs.Load() != 0 || f.deletes != 0 {
				t.Fatal("uncertain helper adopted or deleted")
			}
		})
	}
	for _, when := range []string{"before Create", "after output", "during cleanup"} {
		t.Run("retained Pod replaced "+when, func(t *testing.T) {
			f, worker, pvc, pod := newCompletionProbe(t, false)
			replace := func(*batchv1.Job) {
				if err := f.store.Delete(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
				pod.UID, pod.ResourceVersion = "replaced-original-pod", ""
				if err := f.store.Create(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			}
			switch when {
			case "before Create":
				f.onCreate = replace
			case "after output":
				f.onLog = replace
			case "during cleanup":
				f.onDelete = replace
			}
			v, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(context.Background(), f.m, worker, pvc)
			f.assertNoAuthority(v, passed, failure, err)
		})
	}
	t.Run("cancellation retains helper cleanup", func(t *testing.T) {
		f, worker, pvc, _ := newCompletionProbe(t, false)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.onLog = func(*batchv1.Job) { cancel() }
		v, passed, failure, err := completionConsumer(t, f.r).ensureNativeCompletionPreflight(ctx, f.m, worker, pvc)
		if v != nil || passed || failure != "" || err != context.Canceled || f.deletes != 1 {
			t.Fatal("cancellation granted data or lost cleanup", err)
		}
	})
}

func TestNativeCloneOutputCompletionAdmission(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	f.m.Status.Phase = v1beta1.PhaseCompleted
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if report, err := nativeCloneRead(t, f); err != nil || report == nil || f.logs != 1 {
		t.Fatal("stale display refused retained clone output", err)
	}
}

func TestNativeVerificationCompletionAdmission(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Data: true})
	f.m.Status.Phase = v1beta1.PhaseFailed
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	nativeInstallCompare(t, f, compareData, true)
	if done, err := nativeVerify(t, f); err != nil || !done || f.logs != 1 || f.m.Status.Replication != nil {
		t.Fatal("plain clone verification refused stale display or invented E", err)
	}
}

func TestNativePreflightDefaultedKnownJob(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	canonical := f.worker.DeepCopy()
	known := canonical.DeepCopy()
	defaultProbeJob(known)
	if !nativePreflightJobMatches(canonical, known) || !nativePreflightJobMatches(known, known.DeepCopy()) {
		t.Fatal("actual defaulted Job cannot retain an independently canonical identity")
	}
	for _, tc := range []struct {
		name   string
		change func(*batchv1.Job)
	}{
		{"owner", func(job *batchv1.Job) { job.OwnerReferences[0].UID = "foreign-owner" }},
		{"UID", func(job *batchv1.Job) { job.UID = "replacement-job"; defaultProbeJob(job) }},
		{"controller UID label", func(job *batchv1.Job) { job.Spec.Template.Labels[batchv1.ControllerUidLabel] = "foreign-controller" }},
		{"selector UID", func(job *batchv1.Job) {
			job.Spec.Selector.MatchLabels[batchv1.ControllerUidLabel] = "foreign-controller"
		}},
		{"custom selector", func(job *batchv1.Job) { job.Spec.Selector.MatchLabels["custom-selector"] = "custom-value" }},
		{"selector expression", func(job *batchv1.Job) {
			job.Spec.Selector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: "custom-selector", Operator: metav1.LabelSelectorOpExists}}
		}},
		{"image", func(job *batchv1.Job) { job.Spec.Template.Spec.Containers[0].Image = "replacement-image" }},
		{"command", func(job *batchv1.Job) { job.Spec.Template.Spec.Containers[0].Args = []string{probeTestVersionCommand} }},
		{"security", func(job *batchv1.Job) { job.Spec.Template.Spec.HostNetwork = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := known.DeepCopy()
			tc.change(actual)
			if nativePreflightJobMatches(known, actual) || nativePreflightJobMatches(canonical, actual) {
				t.Fatal("non-generated identity or execution change was normalized away")
			}
		})
	}
}

func TestNativeVerificationDefaultedCreateReceipt(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true})
	f.onCreate = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		job := obj.(*batchv1.Job)
		job.UID, job.Generation = nativeKnownSchemaHelper, 1
		defaultProbeJob(job)
		return c.Create(ctx, job, opts...)
	}
	rv := f.m.ResourceVersion
	if done, err := nativeVerify(t, f); done || err != nil || f.logs != 0 || f.patches != 1 || f.m.Status.NativeSchemaJobUID != nativeKnownSchemaHelper || f.m.ResourceVersion == rv {
		t.Fatal("actual known Create failed atomic receipt or failed immediate return", err)
	}
}
