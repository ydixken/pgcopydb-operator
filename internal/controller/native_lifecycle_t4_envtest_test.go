package controller

import (
	"context"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// A real terminating namespace refuses every create, dry-run and Event included, and admits status
// and finalizer patches (design B P3). envtest runs no namespace controller, so the namespace stays.
var _ = Describe("Native deletion in a terminating namespace", func() {
	It("releases with CleanupFailed naming the slot and publication, status before finalizer", func(ctx SpecContext) {
		ns := "t4-doomed-" + rand.String(6)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
		m := validMigration("native-doomed")
		m.Namespace = ns
		m.Spec.Engine, m.Spec.Follow = v1beta1.MigrationEngineRPGMigrate, &v1beta1.FollowOptions{Enabled: true}
		m.Finalizers = []string{finalizerName}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: workPVCName(m), Namespace: ns, Labels: map[string]string{labelMigration: m.Name}}, Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		}}
		nativeBindTestClaim(m, pvc)
		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
		pvc.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())
		m.Status = v1beta1.MigrationStatus{Phase: v1beta1.PhasePending, NativeRunnerImage: qualifiedRunnerARM64}
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())
		worker, err := buildNativeWorkerJob(m, pvc, qualifiedRunnerARM64, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(controllerutil.SetControllerReference(m, worker, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, worker)).To(Succeed())
		now := metav1.Now()
		worker.Status = batchv1.JobStatus{Failed: 1, StartTime: &now, Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded, LastTransitionTime: now},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded, LastTransitionTime: now},
		}}
		Expect(k8sClient.Status().Update(ctx, worker)).To(Succeed())
		m.Status.Attempts, m.Status.JobName, m.Status.Phase, m.Status.ObservedGeneration = 1, worker.Name, v1beta1.PhaseCloning, m.Generation
		m.Status.NativeJobUID, m.Status.NativePVCUID = string(worker.UID), string(pvc.UID)
		m.Status.NativeJobSHA256 = nativeTestDigest(GinkgoT(), worker)
		m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())
		Expect(k8sClient.Delete(ctx, m)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
		Eventually(func(g Gomega) {
			n := &corev1.Namespace{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: ns}, n)).To(Succeed())
			g.Expect(n.Status.Phase).To(Equal(corev1.NamespaceTerminating))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		var mu sync.Mutex
		var order []string
		var cleanupFailed *metav1.Condition
		c, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		r := newReconciler()
		r.APIReader, r.RPGMigrateRunnerImage = c, qualifiedRunnerARM64
		r.Client = interceptor.NewClient(c, interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
				mu.Lock()
				order = append(order, t4FinalizerWrite)
				mu.Unlock()
				return c.Patch(ctx, obj, p, opts...)
			},
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				mu.Lock()
				order = append(order, t4StatusWrite)
				if cf := meta.FindStatusCondition(obj.(*v1beta1.Migration).Status.Conditions, v1beta1.ConditionCleanupFailed); cf != nil {
					cleanupFailed = cf.DeepCopy()
				}
				mu.Unlock()
				return c.SubResource(sub).Patch(ctx, obj, p, opts...)
			},
		})
		logs := &t4LogBuffer{}
		logger := zap.New(zap.WriteTo(logs), zap.UseDevMode(false))
		live := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), live)).To(Succeed())
		_, err = r.reconcileNative(logf.IntoContext(ctx, logger), live, live.DeepCopy())
		Expect(err).NotTo(HaveOccurred())

		run, err := nativeRunID(live)
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), &v1beta1.Migration{}))).To(BeTrue(), "the finalizer was not released")
		Expect(cleanupFailed).NotTo(BeNil(), "released without the CleanupFailed condition")
		Expect(cleanupFailed.Status).To(Equal(metav1.ConditionTrue))
		Expect(cleanupFailed.Reason).To(Equal(v1beta1.ReasonNamespaceTerminating))
		Expect(cleanupFailed.Message).To(And(ContainSubstring("rpgm_s_"+run+"_*"), ContainSubstring("rpgm_p_"+run+"_*")))
		Expect(order).To(Equal([]string{t4StatusWrite, t4FinalizerWrite}), "CleanupFailed must be durable before the finalizer patch")
		Expect(logs.String()).To(And(ContainSubstring("rpgm_s_"+run+"_*"), ContainSubstring("rpgm_p_"+run+"_*"), ContainSubstring(v1beta1.ReasonNamespaceTerminating)),
			"the event is refused in a terminating namespace, so the log line must carry the slot and publication")
		claim := &corev1.PersistentVolumeClaim{}
		err = k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), claim)
		Expect(apierrors.IsNotFound(err) || err == nil && !claim.DeletionTimestamp.IsZero()).To(BeTrue(), "the bound work PVC was not deleted: %v", err)
	})
})

// t4LogBuffer collects the operator log lines of one pass.
type t4LogBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *t4LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *t4LogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
