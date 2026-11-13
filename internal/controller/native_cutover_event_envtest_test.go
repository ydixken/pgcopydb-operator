package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

// endposAtEvent records each event with the endpos the API server held when it was emitted.
type endposAtEvent struct {
	c      client.Client
	events []string
}

func (r *endposAtEvent) Eventf(regarding, _ runtime.Object, eventtype, reason, _, note string, args ...any) {
	saved := "unreadable"
	if m, ok := regarding.(*v1beta1.Migration); ok {
		live := &v1beta1.Migration{}
		if r.c.Get(context.Background(), client.ObjectKeyFromObject(m), live) == nil {
			saved = nativeControlEnd(live)
		}
	}
	r.events = append(r.events, fmt.Sprintf(eventtype+" "+reason+" "+note+" saved="+saved, args...))
}

// newNativeCutoverEnv is the native control fixture on a real API server, with a running worker the
// harness plays kubelet for; the exec relay answers the source sample with nativeControlTestEnd.
func newNativeCutoverEnv(ctx context.Context, name string) *nativeControlFixture {
	GinkgoHelper()
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
	Expect(err).NotTo(HaveOccurred())
	m := validMigration(name)
	m.Spec.Engine, m.Spec.Follow = v1beta1.MigrationEngineRPGMigrate, &v1beta1.FollowOptions{Enabled: true}
	m.Spec.Cutover = v1beta1.CutoverSpec{Mode: v1beta1.CutoverManual, Approved: true}
	narrowNativeWorker(m)
	Expect(c.Create(ctx, m)).To(Succeed())
	DeferCleanup(removeMigration, name)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: workPVCName(m), Namespace: testNS, Labels: map[string]string{labelMigration: name}}, Spec: corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
	}}
	Expect(controllerutil.SetControllerReference(m, pvc, c.Scheme())).To(Succeed())
	Expect(c.Create(ctx, pvc)).To(Succeed())
	pvc.Status.Phase = corev1.ClaimBound
	Expect(c.Status().Update(ctx, pvc)).To(Succeed())
	m.Status = v1beta1.MigrationStatus{Phase: v1beta1.PhaseStreaming, NativeRunnerImage: qualifiedRunnerARM64}
	Expect(c.Status().Update(ctx, m)).To(Succeed())
	worker, err := buildNativeWorkerJob(m, pvc, qualifiedRunnerARM64, 1)
	Expect(err).NotTo(HaveOccurred())
	Expect(controllerutil.SetControllerReference(m, worker, c.Scheme())).To(Succeed())
	Expect(c.Create(ctx, worker)).To(Succeed())
	now := metav1.Now()
	worker.Status = batchv1.JobStatus{Active: 1, StartTime: &now}
	Expect(c.Status().Update(ctx, worker)).To(Succeed())
	pod := probePod(worker)
	running := pod.Status.ContainerStatuses[0]
	pod.UID, pod.Status, pod.Annotations = "", corev1.PodStatus{}, worker.Spec.Template.Annotations
	Expect(c.Create(ctx, pod)).To(Succeed())
	DeferCleanup(func(ctx context.Context) { _ = c.Delete(ctx, pod, client.GracePeriodSeconds(0)) })
	running.ContainerID = "containerd://" + string(pod.UID)
	running.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{running}}
	Expect(c.Status().Update(ctx, pod)).To(Succeed())
	m.Status.Attempts, m.Status.JobName, m.Status.ObservedGeneration = 1, worker.Name, m.Generation
	m.Status.NativeJobUID, m.Status.NativePVCUID = string(worker.UID), string(pvc.UID)
	Expect(c.Status().Update(ctx, m)).To(Succeed())

	f := &nativeControlFixture{t: GinkgoT(), m: m, pvc: pvc, worker: worker, pod: pod, store: c, head: nativeControlTestEnd}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	DeferCleanup(server.Close)
	checked, err := podexec.New(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	Expect(err).NotTo(HaveOccurred())
	f.control = native.NewClient(checked)
	f.r = newReconciler()
	f.r.Client = interceptor.NewClient(c, interceptor.Funcs{SubResourcePatch: f.patchStatus})
	f.r.APIReader, f.r.RPGMigrateRunnerImage, f.r.NativeControl = nativeControlAPIReader{f}, qualifiedRunnerARM64, f.control
	return f
}

var _ = Describe("Native CutoverStarted", func() {
	It("announces the endpos once, after the API server holds it, and not again on requeue", func(ctx SpecContext) {
		f := newNativeCutoverEnv(ctx, "native-cutover-started")
		rec := &endposAtEvent{c: k8sClient}
		f.r.Recorder = rec
		pass := func() *v1beta1.Migration {
			GinkgoHelper()
			_, err := f.r.reconcileNativeFollowCutover(ctx, f.m, f.worker, f.pvc, true)
			Expect(err).NotTo(HaveOccurred())
			live := &v1beta1.Migration{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.m), live)).To(Succeed())
			f.m = live
			return live
		}

		By("persisting E on the first pass")
		live := pass()
		Expect(nativeControlEnd(live)).To(Equal(nativeControlTestEnd))
		Expect(live.Status.Phase).To(Equal(v1beta1.PhaseCuttingOver))
		Expect(f.heads).To(Equal(1))
		Expect(f.sets).To(BeZero())
		Expect(rec.events).To(Equal([]string{nativeCutoverStartedEvent + " saved=" + nativeControlTestEnd}))

		By("sending the saved E on requeues without announcing it again")
		for range 2 {
			pass()
		}
		Expect(f.heads).To(Equal(1))
		Expect(f.sentEnds).To(Equal([]string{nativeControlTestEnd, nativeControlTestEnd}))
		Expect(rec.events).To(HaveLen(1))
	})
})
