package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

// nativeDeletionEnv keeps a Failed native follow Migration in envtest, which has no kubelet,
// Job controller or garbage collector, so the harness plays those three for helper Jobs.
type nativeDeletionEnv struct {
	mu       sync.Mutex
	c        client.WithWatch
	m        *v1beta1.Migration
	lines    map[string][]byte
	edit     func(map[string]any)
	relays   int
	cleanups int
}

func newNativeDeletionEnv(ctx context.Context, name string) *nativeDeletionEnv {
	GinkgoHelper()
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
	Expect(err).NotTo(HaveOccurred())
	e := &nativeDeletionEnv{c: c, lines: map[string][]byte{}}
	m := validMigration(name)
	m.Spec.Engine, m.Spec.Follow = v1beta1.MigrationEngineRPGMigrate, &v1beta1.FollowOptions{Enabled: true}
	m.Finalizers = []string{finalizerName}
	Expect(c.Create(ctx, m)).To(Succeed())
	DeferCleanup(func() {
		live := &v1beta1.Migration{}
		if c.Get(ctx, client.ObjectKeyFromObject(m), live) == nil {
			live.Finalizers = nil
			_ = c.Update(ctx, live)
		}
		removeMigration(ctx, name)
	})
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: workPVCName(m), Namespace: testNS, Labels: map[string]string{labelMigration: name}}, Spec: corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
	}}
	Expect(controllerutil.SetControllerReference(m, pvc, c.Scheme())).To(Succeed())
	Expect(c.Create(ctx, pvc)).To(Succeed())
	pvc.Status.Phase = corev1.ClaimBound
	Expect(c.Status().Update(ctx, pvc)).To(Succeed())
	m.Status = v1beta1.MigrationStatus{Phase: v1beta1.PhasePending, NativeRunnerImage: qualifiedRunnerARM64}
	Expect(c.Status().Update(ctx, m)).To(Succeed())
	worker, err := buildNativeWorkerJob(m, pvc, qualifiedRunnerARM64, 1)
	Expect(err).NotTo(HaveOccurred())
	Expect(controllerutil.SetControllerReference(m, worker, c.Scheme())).To(Succeed())
	Expect(c.Create(ctx, worker)).To(Succeed())
	now := metav1.Now()
	worker.Status = batchv1.JobStatus{Failed: 1, StartTime: &now, Conditions: []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded, LastTransitionTime: now},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded, LastTransitionTime: now},
	}}
	Expect(c.Status().Update(ctx, worker)).To(Succeed())
	// Exit 2 is the engine's refusal of an unsupported column type, which comes before its first creation intent.
	original := e.helperPod(worker, worker.Name+"-original")
	Expect(c.Create(ctx, original)).To(Succeed())
	DeferCleanup(func() { _ = c.Delete(ctx, original, client.GracePeriodSeconds(0)) })
	original.Status = corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer, Image: qualifiedRunnerARM64, ImageID: qualifiedRunnerARM64, ContainerID: nativePasswordOriginalContainer, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, ContainerID: nativePasswordOriginalContainer}}}}}
	Expect(c.Status().Update(ctx, original)).To(Succeed())
	m.Status.Attempts, m.Status.JobName, m.Status.Phase, m.Status.ObservedGeneration = 1, worker.Name, v1beta1.PhaseFailed, m.Generation
	m.Status.NativeJobUID, m.Status.NativePVCUID = string(worker.UID), string(pvc.UID)
	Expect(c.Status().Update(ctx, m)).To(Succeed())
	Expect(c.Delete(ctx, m)).To(Succeed())
	e.m = m
	return e
}

// helperPod is the Pod the Job controller and kubelet would create for job.
func (e *nativeDeletionEnv) helperPod(job *batchv1.Job, name string) *corev1.Pod {
	pod := probePod(job)
	pod.Name, pod.UID, pod.Status = name, "", corev1.PodStatus{}
	pod.Annotations = job.Spec.Template.Annotations
	return pod
}

func (e *nativeDeletionEnv) reconciler() *MigrationReconciler {
	GinkgoHelper()
	server := httptest.NewServer(http.HandlerFunc(e.serve))
	DeferCleanup(server.Close)
	checked, err := podexec.New(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	Expect(err).NotTo(HaveOccurred())
	r := newReconciler()
	r.Client = interceptor.NewClient(e.c, interceptor.Funcs{Create: e.create, Delete: e.delete})
	r.APIReader, r.RPGMigrateRunnerImage = e.c, qualifiedRunnerARM64
	r.NativePreflight, r.NativeJobs, r.NativeEvidence = checked, checked, native.NewReader(e.c, checked)
	return r
}

func (e *nativeDeletionEnv) create(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return c.Create(ctx, obj, opts...)
	}
	if err := c.Create(ctx, job, opts...); err != nil {
		return err
	}
	pod := e.helperPod(job, job.Name+"-pod")
	if err := c.Create(ctx, pod); err != nil {
		return err
	}
	id, now := "containerd://"+string(pod.UID), metav1.Now()
	running := strings.HasPrefix(job.Name, "rpgmigrate-inventory-")
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer, ContainerID: id, Image: "display-alias", ImageID: qualifiedRunnerARM64, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}}}}
	// The Job controller writes status later, so the caller's object keeps its creation response.
	live := job.DeepCopy()
	live.Status = batchv1.JobStatus{Active: 1, StartTime: &now}
	if !running {
		e.complete(live, pod)
	}
	e.mu.Lock()
	if strings.HasPrefix(job.Name, "rpgmigrate-version-") {
		e.lines[pod.Name] = []byte(`{"protocol_version":1,"engine_version":"rpgmigrate-test","capabilities":["clone.single_db","progress.v1","verify.sealed_files.v1","copy.large_objects","copy.binary_fallback","copy.split","cdc.pgoutput","cutover.v1","cleanup.v1","verify.v1"]}` + "\n")
	}
	if strings.HasPrefix(job.Name, "rpgmigrate-cleanup-") {
		e.cleanups++
	}
	e.mu.Unlock()
	if err := c.Status().Update(ctx, pod); err != nil {
		return err
	}
	return c.Status().Update(ctx, live)
}

func (e *nativeDeletionEnv) complete(job *batchv1.Job, pod *corev1.Pod) {
	now := metav1.Now()
	job.Status = batchv1.JobStatus{Succeeded: 1, StartTime: job.Status.StartTime, CompletionTime: &now, Conditions: []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
	}}
	id := pod.Status.ContainerStatuses[0].ContainerID
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: id, ExitCode: 0}}
}

// delete also does the garbage collector's foreground work, which envtest does not run.
func (e *nativeDeletionEnv) delete(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	if err := c.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return nil
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(job.Namespace)); err != nil {
		return err
	}
	for i := range pods.Items {
		if owner := metav1.GetControllerOf(&pods.Items[i]); owner != nil && owner.UID == job.UID {
			if err := c.Delete(ctx, &pods.Items[i], client.GracePeriodSeconds(0)); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	live := &batchv1.Job{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), live); err != nil {
		return client.IgnoreNotFound(err)
	}
	live.Finalizers = nil
	return client.IgnoreNotFound(c.Update(ctx, live))
}

func (e *nativeDeletionEnv) serve(out http.ResponseWriter, request *http.Request) {
	parts := strings.Split(request.URL.Path, "/")
	out.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(request.URL.Path, "/exec"):
		e.relay(out, request)
	case strings.HasSuffix(request.URL.Path, "/log"):
		e.mu.Lock()
		raw := e.lines[parts[len(parts)-2]]
		e.mu.Unlock()
		_, _ = out.Write(raw)
	case strings.HasSuffix(request.URL.Path, "/pods"):
		pods := &corev1.PodList{}
		opts := []client.ListOption{client.InNamespace(testNS)}
		if key, value, ok := strings.Cut(request.URL.Query().Get("labelSelector"), "="); ok {
			opts = append(opts, client.MatchingLabels{key: value})
		}
		if err := e.c.List(request.Context(), pods, opts...); err != nil {
			http.Error(out, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(out).Encode(pods)
	default:
		var obj client.Object = &corev1.Pod{}
		if strings.Contains(request.URL.Path, "/jobs/") {
			obj = &batchv1.Job{}
		}
		if e.c.Get(request.Context(), client.ObjectKey{Namespace: testNS, Name: parts[len(parts)-1]}, obj) != nil {
			http.NotFound(out, request)
			return
		}
		_ = json.NewEncoder(out).Encode(obj)
	}
}

// relay answers the owned inventory exec as the helper program would, then lets its Job finish.
func (e *nativeDeletionEnv) relay(out http.ResponseWriter, request *http.Request) {
	ws, err := (&websocket.Upgrader{Subprotocols: []string{nativeCleanupTestProtocol}}).Upgrade(out, request, nil)
	if err != nil {
		return
	}
	defer func() { _ = ws.Close() }()
	var raw []byte
	for {
		_, part, err := ws.ReadMessage()
		if err != nil || len(part) == 0 {
			return
		}
		if part[0] == 255 {
			break
		}
		raw = append(raw, part[1:]...)
	}
	var req map[string]any
	if json.Unmarshal(raw, &req) != nil {
		return
	}
	x := req["execution"].(map[string]any)
	requestSHA := sha256.Sum256(raw)
	executionSHA := sha256.Sum256(append([]byte("rpgmigrate.native.execution.v1\x00"), requestSHA[:]...))
	result := map[string]any{probeTestVersionCommand: 1, lifecycleKindKey: req[lifecycleKindKey], probeOperationKey: req[probeOperationKey], nativeControlProgramKey: req[nativeControlProgramKey], nativeControlRequestKey: hex.EncodeToString(requestSHA[:]), nativeControlExecutionKey: hex.EncodeToString(executionSHA[:]), nativeControlWorkKey: x[nativeControlWorkKey],
		"resources":                  []map[string]any{{lifecycleKindKey: workdirVolume, lifecycleNameKey: nativeCleanupTestWorkdir, lifecycleOwnedKey: true, lifecycleRetainKey: true}},
		nativeControlSourceClosedKey: true, nativeTargetClosedKey: true, nativeDispositionMaterializationClosedKey: true, nativeControlPrivateClosedKey: true, nativeControlDiagnosticsKey: true}
	e.mu.Lock()
	e.relays++
	edit := e.edit
	e.mu.Unlock()
	if edit != nil {
		edit(result)
	}
	job, pod := &batchv1.Job{}, &corev1.Pod{}
	ctx := request.Context()
	if e.c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: x["job_name"].(string)}, job) != nil || e.c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: req["pod_name"].(string)}, pod) != nil {
		return
	}
	e.complete(job, pod)
	if e.c.Status().Update(ctx, pod) != nil || e.c.Status().Update(ctx, job) != nil {
		return
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return
	}
	_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{1}, append(encoded, '\n')...))
	_ = ws.WriteMessage(websocket.BinaryMessage, []byte("\x03{\"status\":\"Success\"}"))
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

var _ = Describe("Native deletion after an early engine refusal", func() {
	ctx := context.Background()
	reconcileOnce := func(r *MigrationReconciler, name string) error {
		GinkgoHelper()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: testNS}})
		return err
	}

	It("finishes when two complete inventories prove nothing was provisioned", func() {
		e := newNativeDeletionEnv(ctx, "mig-native-unprovisioned-delete")
		Expect(reconcileOnce(e.reconciler(), e.m.Name)).To(Succeed())
		err := e.c.Get(ctx, client.ObjectKeyFromObject(e.m), &v1beta1.Migration{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "deletion left the Migration: %v", err)
		Expect(e.relays).To(Equal(2))
		Expect(e.cleanups).To(BeZero(), "an unprovisioned run needs no cleanup Job")
		Expect(e.c.Get(ctx, types.NamespacedName{Name: e.m.Status.JobName, Namespace: testNS}, &batchv1.Job{})).To(Succeed(), "deletion removed the retained worker")
	})

	It("keeps the finalizer while the inventory cannot be read", func() {
		e := newNativeDeletionEnv(ctx, "mig-native-unreadable-delete")
		e.edit = func(v map[string]any) { v[nativeControlSourceClosedKey] = false }
		r := e.reconciler()
		for range 2 {
			Expect(reconcileOnce(r, e.m.Name)).To(MatchError(errNativeDisposition))
		}
		live := &v1beta1.Migration{}
		Expect(e.c.Get(ctx, client.ObjectKeyFromObject(e.m), live)).To(Succeed())
		Expect(live.Finalizers).To(ContainElement(finalizerName))
		Expect(live.DeletionTimestamp).NotTo(BeNil())
		Expect(e.relays).To(Equal(2))
		Expect(e.cleanups).To(BeZero())
	})
})
