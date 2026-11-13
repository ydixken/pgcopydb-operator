package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// bindingReader runs bind before the nth claim read, as the volume binder can between Create and confirm.
type bindingReader struct {
	client.Reader
	n, reads int
	bind     func()
}

func (r *bindingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
		if r.reads++; r.reads == r.n {
			r.bind()
		}
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

var _ = Describe("Native worker claim bind", func() {
	ctx := context.Background()

	It("records the worker when the binder fills volumeName between Create and confirm", func() {
		m := validMigration("mig-native-bind-during-start")
		m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
		DeferCleanup(removeMigration, ctx, m.Name)
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		pvc := buildWorkPVC(m)
		Expect(controllerutil.SetControllerReference(m, pvc, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
		Expect(pvc.Spec.VolumeName).To(BeEmpty())

		reader := &bindingReader{Reader: k8sClient, n: 3, bind: func() {
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: jobName(m, 1)}, &batchv1.Job{})).To(Succeed(), "bind ran before Create")
			claim := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), claim)).To(Succeed())
			claim.Spec.VolumeName = "pv-bound"
			Expect(k8sClient.Update(ctx, claim)).To(Succeed())
		}}
		r := newReconciler()
		r.APIReader, r.RPGMigrateRunnerImage = reader, qualifiedRunnerARM64
		live := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), live)).To(Succeed())
		_, err := r.startAttempt(ctx, live, live.DeepCopy())
		Expect(err).NotTo(HaveOccurred())
		Expect(reader.reads).To(BeNumerically(">=", 3), "the bind never ran")

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), live)).To(Succeed())
		Expect(live.Status.Attempts).To(Equal(int32(1)))
		Expect(live.Status.NativePVCUID).To(Equal(string(pvc.UID)))
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: live.Status.JobName}, job)).To(Succeed())
		Expect(live.Status.NativeJobUID).To(Equal(string(job.UID)))

		// The API admits volumeName once, so a move between two volumes needs a new claim UID.
		claim := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), claim)).To(Succeed())
		Expect(claim.Spec.VolumeName).To(Equal("pv-bound"))
		claim.Spec.VolumeName = "pv-other"
		Expect(apierrors.IsInvalid(k8sClient.Update(ctx, claim))).To(BeTrue())
	})
})
