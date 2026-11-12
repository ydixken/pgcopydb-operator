package controller

import (
	"context"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

type refusingNativePreflight struct{}

func (refusingNativePreflight) JobVersionNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	return podexec.NativeOutcome{}, errBoom
}

func (refusingNativePreflight) JobPreflightNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	return podexec.NativeOutcome{}, errBoom
}

type recordingJobClient struct {
	client.Client
	record func(*batchv1.Job)
}

func (c recordingJobClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if job, ok := obj.(*batchv1.Job); ok {
		c.record(job)
	}
	return c.Client.Create(ctx, obj, opts...)
}

var _ = Describe("Native activation tuple", func() {
	ctx := context.Background()

	// envtest runs no kubelet, so a version Job never runs; its creation is the evidence and ends the pass.
	passes := func(name, runnerImage string, n int) (*v1beta1.Migration, []*batchv1.Job) {
		GinkgoHelper()
		var mu sync.Mutex
		var created []*batchv1.Job
		pass, cancel := context.WithTimeout(ctx, nativePreflightCleanupReserve+2*time.Second)
		defer cancel()
		r := newReconciler()
		r.Client = recordingJobClient{Client: k8sClient, record: func(job *batchv1.Job) {
			mu.Lock()
			created = append(created, job.DeepCopy())
			mu.Unlock()
			cancel()
		}}
		r.APIReader, r.NativePreflight, r.RPGMigrateRunnerImage = k8sClient, refusingNativePreflight{}, runnerImage
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: testNS}}
		for range n {
			_, _ = r.Reconcile(pass, req)
		}
		got := &v1beta1.Migration{}
		if err := k8sClient.Get(ctx, req.NamespacedName, got); !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
		}
		mu.Lock()
		defer mu.Unlock()
		return got, created
	}
	runMigration := func(m *v1beta1.Migration, runnerImage string, n int) (*v1beta1.Migration, []*batchv1.Job) {
		GinkgoHelper()
		DeferCleanup(removeMigration, ctx, m.Name)
		DeferCleanup(func() {
			live := &v1beta1.Migration{}
			if k8sClient.Get(ctx, client.ObjectKeyFromObject(m), live) == nil && len(live.Finalizers) > 0 {
				live.Finalizers = nil
				_ = k8sClient.Update(ctx, live)
			}
		})
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		return passes(m.Name, runnerImage, n)
	}
	run := func(name, runnerImage string) (*v1beta1.Migration, []*batchv1.Job) {
		GinkgoHelper()
		m := validMigration(name)
		m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
		return runMigration(m, runnerImage, 2)
	}

	It("admits a native Migration on a qualified runner digest into native preflight", func() {
		m, jobs := run("mig-native-qualified", qualifiedRunnerARM64)
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated == nil || validated.Reason != testUnsupportedEngine).To(BeTrue(), "validated = %+v", validated)
		Expect(jobs).NotTo(BeEmpty())
		Expect(strings.HasPrefix(jobs[0].Name, "rpgmigrate-version-")).To(BeTrue(), "first Job %q", jobs[0].Name)
		Expect(jobs[0].Spec.Template.Spec.Containers[0].Image).To(Equal(qualifiedRunnerARM64))
	})

	It("keeps the admitted runner digest after the operator flag moves", func() {
		name := "mig-native-flag-moved"
		admitted, _ := run(name, qualifiedRunnerARM64)
		Expect(admitted.Status.NativeRunnerImage).To(Equal(qualifiedRunnerARM64))
		for _, flag := range []string{testRunnerTag, qualifiedRunnerAMD64} {
			m, jobs := passes(name, flag, 2)
			Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionFailed)).To(BeFalse(), "status = %+v", m.Status)
			Expect(m.Status.NativeRunnerImage).To(Equal(qualifiedRunnerARM64))
			Expect(jobs).NotTo(BeEmpty())
			for _, job := range jobs {
				Expect(job.Spec.Template.Spec.Containers[0].Image).To(Equal(qualifiedRunnerARM64), "flag %q Job %q", flag, job.Name)
			}
		}
	})

	It("runs native deletion after the operator flag moves", func() {
		name := "mig-native-flag-moved-delete"
		m := validMigration(name)
		m.Spec.Engine, m.Spec.Follow = v1beta1.MigrationEngineRPGMigrate, &v1beta1.FollowOptions{Enabled: true}
		created, _ := runMigration(m, qualifiedRunnerARM64, 2)
		Expect(created.Finalizers).To(ContainElement(finalizerName))
		Expect(created.Status.NativeRunnerImage).To(Equal(qualifiedRunnerARM64))
		Expect(k8sClient.Delete(ctx, created)).To(Succeed())
		_, _ = passes(name, testRunnerTag, 1)
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(created), &v1beta1.Migration{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "deletion left the Migration: %v", err)
	})

	It("keeps the runner record immutable", func() {
		m, _ := run("mig-native-record-immutable", qualifiedRunnerARM64)
		Expect(m.Status.NativeRunnerImage).To(Equal(qualifiedRunnerARM64))
		moved := m.DeepCopy()
		moved.Status.NativeRunnerImage = qualifiedRunnerAMD64
		Expect(apierrors.IsInvalid(k8sClient.Status().Update(ctx, moved))).To(BeTrue())
		removed := m.DeepCopy()
		removed.Status.NativeRunnerImage = ""
		Expect(apierrors.IsInvalid(k8sClient.Status().Update(ctx, removed))).To(BeTrue())
	})

	It("refuses a native Migration on an unqualified runner before any Job", func() {
		m, jobs := run("mig-native-unqualified", testRunnerTag)
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated).NotTo(BeNil())
		Expect(validated.Reason).To(Equal(testUnsupportedEngine))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		Expect(jobs).To(BeEmpty())
	})
})
