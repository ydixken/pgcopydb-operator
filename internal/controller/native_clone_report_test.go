package controller

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	nativeCloneStatusSubresource = "status"
	nativeCloneReplacement       = "clone-replacement"
	nativeKnownSchemaHelper      = "known-schema-helper"
)

type nativeCloneFixture struct {
	t             *testing.T
	r             *MigrationReconciler
	store         client.WithWatch
	m             *v1beta1.Migration
	worker        *batchv1.Job
	pvc           *corev1.PersistentVolumeClaim
	pod           *corev1.Pod
	report        *native.Report
	raw           []byte
	outputs       map[string][]byte
	logs, patches int
	afterLogs     func()
	onCreate      func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error
	patch         func(context.Context, client.Client, client.Object, client.Patch) error
}

type nativeCloneAPIReader struct{ client.Reader }

type nativeCloneAfterCapture struct {
	NativeJobReader
	after func()
}

func (r nativeCloneAfterCapture) JobCloneNative(ctx context.Context, namespace string, uid types.UID, binding podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	output, err := r.NativeJobReader.JobCloneNative(ctx, namespace, uid, binding)
	if r.after != nil {
		r.after()
	}
	return output, err
}

func (r nativeCloneAPIReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	list.(*corev1.PodList).ResourceVersion = "clone-snapshot"
	return nil
}

func newNativeCloneFixture(t *testing.T, follow bool, verification *v1beta1.VerificationOptions) *nativeCloneFixture {
	t.Helper()
	m, pvc := nativeIdentityObjects()
	m.Generation, m.ResourceVersion = 1, "1"
	m.Spec.Source, m.Spec.Target = passwordMigration().Spec.Source, passwordMigration().Spec.Target
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: follow}
	m.Spec.Verification = verification
	m.Status.Attempts, m.Status.JobName, m.Status.Phase = 1, jobName(m, 1), v1beta1.PhaseCloning
	pvc.Status.Phase = corev1.ClaimBound
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	worker, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
	if err != nil || controllerutil.SetControllerReference(m, worker, scheme) != nil {
		t.Fatalf("build clone worker: %v", err)
	}
	worker.UID = "clone-original-job"
	worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	m.Status.NativeJobUID, m.Status.NativePVCUID = string(worker.UID), string(pvc.UID)
	pod := nativeRecoveryTestPod(worker, 0)
	pod.Labels = maps.Clone(worker.Spec.Template.Labels)
	pod.Labels[batchv1.ControllerUidLabel] = string(worker.UID)
	store := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(m, pvc, worker, pod).WithStatusSubresource(&v1beta1.Migration{}).Build()
	f := &nativeCloneFixture{t: t, m: m, pvc: pvc, worker: worker, pod: pod, store: store}
	f.r = &MigrationReconciler{Scheme: scheme, APIReader: nativeCloneAPIReader{store}, RPGMigrateRunnerImage: nativeEvidenceTestImage, Sentinel: &fakeSentinel{}, Progress: &fakeProgress{}, Logs: &fakeLogs{}}
	f.r.Client = interceptor.NewClient(store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Error("native clone used cached read")
			return errBoom
		},
		Create: f.createNativeCloneFixtureJob,
		Delete: f.deleteNativeCloneFixtureHelper,
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			f.patches++
			raw, err := p.Data(obj)
			if err != nil || sub != nativeCloneStatusSubresource || !strings.Contains(string(raw), `"resourceVersion"`) {
				t.Fatal("clone completion lost optimistic status patch")
			}
			if f.patch != nil {
				return f.patch(ctx, c, obj, p)
			}
			return c.Status().Patch(ctx, obj, p, opts...)
		},
	})
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		t.Fatal(err)
	}
	one := int64(1)
	f.report = &native.Report{ProtocolVersion: 1, EngineVersion: "1", Command: nativeTerminalCloneCommand, Capabilities: []native.Capability{native.CapCloneSingleDB}, Work: &work, Phase: nativeTerminalPhase, Copy: &native.CopyProgress{Tables: &native.Counters{Total: &one, Done: &one}, Indexes: &native.Counters{Total: &one, Done: &one}, Bytes: &native.Counters{Total: &one, Done: &one}}}
	f.encodeReport()
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	checked, err := podexec.New(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	f.r.NativeJobs, f.r.NativePreflight = checked, checked
	t.Cleanup(func() {
		if f.r.Sentinel.(*fakeSentinel).callCount() != 0 || f.r.Progress.(*fakeProgress).counts() != 0 || f.r.Logs.(*fakeLogs).calls != 0 {
			t.Error("native clone reached legacy SQL or logs")
		}
	})
	return f
}

func (f *nativeCloneFixture) encodeReport() {
	f.t.Helper()
	raw, err := json.Marshal(f.report)
	if err != nil {
		f.t.Fatal(err)
	}
	f.raw = append(append([]byte("native-report: "), raw...), '\n')
}

func (f *nativeCloneFixture) replace(obj client.Object) {
	f.t.Helper()
	old := obj.DeepCopyObject().(client.Object)
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(obj), old); err != nil {
		f.t.Fatal(err)
	}
	if err := f.store.Delete(context.Background(), old); err != nil {
		f.t.Fatal(err)
	}
	obj.SetResourceVersion("")
	if err := f.store.Create(context.Background(), obj); err != nil {
		f.t.Fatal(err)
	}
}

func (f *nativeCloneFixture) serve(w http.ResponseWriter, request *http.Request) {
	parts := strings.Split(request.URL.Path, "/")
	w.Header().Set("Content-Type", "application/json")
	if strings.HasSuffix(request.URL.Path, "/log") {
		isDiscovery := strings.HasPrefix(parts[len(parts)-2], probeVersion)
		if !isDiscovery {
			f.logs++
		}
		raw := f.raw
		if output, ok := f.outputs[parts[len(parts)-2]]; ok {
			raw = output
		}
		_, _ = w.Write(raw)
		if f.afterLogs != nil && !isDiscovery {
			f.afterLogs()
		}
		return
	}
	if strings.HasSuffix(request.URL.Path, "/pods") {
		pods := &corev1.PodList{}
		if err := f.store.List(request.Context(), pods, client.InNamespace(f.m.Namespace)); err != nil {
			f.t.Error(err)
		}
		pods.ResourceVersion = "clone-transport-snapshot"
		_ = json.NewEncoder(w).Encode(pods)
		return
	}
	var obj client.Object
	switch {
	case strings.Contains(request.URL.Path, "/jobs/"):
		obj = &batchv1.Job{}
	case strings.Contains(request.URL.Path, "/pods/"):
		obj = &corev1.Pod{}
	default:
		http.NotFound(w, request)
		return
	}
	if err := f.store.Get(request.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: parts[len(parts)-1]}, obj); err != nil {
		http.NotFound(w, request)
		return
	}
	_ = json.NewEncoder(w).Encode(obj)
}

type nativeCloneReportConsumer interface {
	readNativeCloneReport(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (*native.Report, error)
}

func nativeCloneRead(t *testing.T, f *nativeCloneFixture) (*native.Report, error) {
	t.Helper()
	consumer, ok := any(f.r).(nativeCloneReportConsumer)
	if !ok {
		t.Fatal("complete clone report consumer is missing")
	}
	return consumer.readNativeCloneReport(context.Background(), f.m, f.worker, f.pvc)
}

func TestNativeCloneOnlyCompletionReport(t *testing.T) {
	t.Run("actual checked capture and strict completed report", func(t *testing.T) {
		f := newNativeCloneFixture(t, false, nil)
		report, err := nativeCloneRead(t, f)
		if err != nil || report == nil || report.Phase != nativeTerminalPhase || f.logs != 1 || f.patches != 0 {
			t.Fatalf("report=%v logs=%d patches=%d err=%v", report != nil, f.logs, f.patches, err)
		}
	})
	t.Run("follow clone report remains DATA", func(t *testing.T) {
		f := newNativeCloneFixture(t, true, nil)
		f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: caughtUpLSN}
		if err := f.store.Status().Update(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		if report, err := nativeCloneRead(t, f); err != nil || report == nil || f.patches != 0 || f.r.NativeEvidence != nil {
			t.Fatalf("clone DATA required follow authority: %v", err)
		}
	})
	for _, tc := range []struct {
		name   string
		change func(*nativeCloneFixture)
	}{
		{"measured empty", func(f *nativeCloneFixture) {
			zero := int64(0)
			for _, c := range []*native.Counters{f.report.Copy.Tables, f.report.Copy.Indexes, f.report.Copy.Bytes} {
				c.Total, c.Done = &zero, &zero
			}
			f.encodeReport()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, nil)
			tc.change(f)
			if got, err := nativeCloneRead(t, f); err != nil || got == nil {
				t.Fatalf("measured zero was unknown: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*nativeCloneFixture)
	}{
		{"missing counter", func(f *nativeCloneFixture) { f.report.Copy.Bytes.Done = nil; f.encodeReport() }},
		{"incomplete counters", func(f *nativeCloneFixture) { n := int64(2); f.report.Copy.Tables.Total = &n; f.encodeReport() }},
		{"foreign report scope", func(f *nativeCloneFixture) { f.report.Work.RunID = string(workerForeignIdentity); f.encodeReport() }},
		{"truncated record", func(f *nativeCloneFixture) { f.raw = f.raw[:len(f.raw)-1] }},
		{"unknown diagnostic", func(f *nativeCloneFixture) { f.raw = append([]byte("private-clone-canary\n"), f.raw...) }},
		{"duplicate report", func(f *nativeCloneFixture) { f.raw = append(f.raw, f.raw...) }},
		{"transport overflow", func(f *nativeCloneFixture) { f.raw = []byte(strings.Repeat("x", 65537)) }},
		{"missing transport", func(f *nativeCloneFixture) { f.r.NativeJobs = nil }},
		{"missing receipt", func(f *nativeCloneFixture) { m := f.m.DeepCopy(); m.Status.NativeJobUID = ""; f.replace(m); f.m = m }},
		{"wrong receipt", func(f *nativeCloneFixture) {
			m := f.m.DeepCopy()
			m.Status.NativePVCUID = string(workerForeignIdentity)
			f.replace(m)
			f.m = m
		}},
		{"failed worker", func(f *nativeCloneFixture) {
			w := f.worker.DeepCopy()
			w.Status = batchv1.JobStatus{Failed: 1}
			f.replace(w)
			f.worker = w
		}},
		{"replacement after capture", func(f *nativeCloneFixture) {
			f.afterLogs = func() { p := f.pod.DeepCopy(); p.UID = nativeCloneReplacement; f.replace(p) }
		}},
		{"PVC replaced after capture", func(f *nativeCloneFixture) {
			f.afterLogs = func() { p := f.pvc.DeepCopy(); p.UID = nativeCloneReplacement; f.replace(p) }
		}},
		{"receipt replaced after capture", func(f *nativeCloneFixture) {
			f.afterLogs = func() { m := f.m.DeepCopy(); m.Status.NativeJobUID = nativeCloneReplacement; f.replace(m) }
		}},
		{"replacement after checked transport returns", func(f *nativeCloneFixture) {
			f.r.NativeJobs = nativeCloneAfterCapture{f.r.NativeJobs, func() { p := f.pod.DeepCopy(); p.UID = "clone-post-transport-replacement"; f.replace(p) }}
		}},
		{"duplicate canonical pod", func(f *nativeCloneFixture) {
			p := f.pod.DeepCopy()
			p.Name, p.UID, p.ResourceVersion = "clone-duplicate", "clone-duplicate", ""
			if err := f.store.Create(context.Background(), p); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"suspended", func(f *nativeCloneFixture) { m := f.m.DeepCopy(); m.Spec.Suspend = true; f.replace(m); f.m = m }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, nil)
			tc.change(f)
			if report, err := nativeCloneRead(t, f); report != nil || err == nil || strings.Contains(err.Error(), "private-clone-canary") {
				t.Fatalf("invalid clone accepted: %v", err)
			}
			if f.patches != 0 {
				t.Fatal("invalid report changed status")
			}
		})
	}
	t.Run("cancellation", func(t *testing.T) {
		f := newNativeCloneFixture(t, false, nil)
		consumer, ok := any(f.r).(nativeCloneReportConsumer)
		if !ok {
			t.Fatal("complete clone report consumer is missing")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if report, err := consumer.readNativeCloneReport(ctx, f.m, f.worker, f.pvc); report != nil || err == nil || !errors.Is(ctx.Err(), context.Canceled) || f.logs != 0 {
			t.Fatal("cancellation reached capture")
		}
	})
}

func (f *nativeCloneFixture) createNativeCloneFixtureJob(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	if strings.HasPrefix(obj.GetName(), probeVersion) {
		job := obj.(*batchv1.Job)
		job.UID = types.UID("clone-discovery-" + job.Name)
		job.Generation = 1
		defaultProbeJob(job)
		job.Status = *f.worker.Status.DeepCopy()
		pod := probePod(job)
		if f.outputs == nil {
			f.outputs = map[string][]byte{}
		}
		f.outputs[pod.Name] = []byte(finishFixtureCaps)
		if err := c.Create(ctx, job, opts...); err != nil {
			return err
		}
		return c.Create(ctx, pod)
	}
	if f.onCreate != nil {
		return f.onCreate(ctx, c, obj, opts...)
	}
	f.t.Error("native clone created follow resources")
	return errBoom
}
func (f *nativeCloneFixture) deleteNativeCloneFixtureHelper(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	job, ok := obj.(*batchv1.Job)
	if !ok || job.UID == f.worker.UID {
		return errBoom
	}
	options := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(options)
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != job.UID {
		f.t.Error("clone helper deletion lacks exact UID")
		return errBoom
	}
	if !strings.HasPrefix(job.Name, probeVersion) {
		live := &v1beta1.Migration{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(f.m), live); err != nil || !meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionComplete) || string(job.UID) != live.Status.NativeSchemaJobUID && string(job.UID) != live.Status.NativeDataJobUID {
			f.t.Error("clone deleted unconsumed helper")
			return errBoom
		}
	}
	if err := c.Delete(ctx, job, opts...); err != nil {
		return err
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if owner := metav1.GetControllerOf(pod); owner != nil && owner.UID == job.UID {
			if err := c.Delete(ctx, pod); err != nil {
				return err
			}
		}
	}
	return nil
}
