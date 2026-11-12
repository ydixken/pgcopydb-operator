package controller

import (
	"context"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
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
	run := func(name, runnerImage string) (*v1beta1.Migration, []*batchv1.Job) {
		GinkgoHelper()
		DeferCleanup(removeMigration, ctx, name)
		m := validMigration(name)
		m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
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
		for range 2 {
			_, _ = r.Reconcile(pass, req)
		}
		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, req.NamespacedName, got)).To(Succeed())
		mu.Lock()
		defer mu.Unlock()
		return got, created
	}

	It("admits a native Migration on a qualified runner digest into native preflight", func() {
		m, jobs := run("mig-native-qualified", qualifiedRunnerARM64)
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated == nil || validated.Reason != testUnsupportedEngine).To(BeTrue(), "validated = %+v", validated)
		Expect(jobs).NotTo(BeEmpty())
		Expect(strings.HasPrefix(jobs[0].Name, "rpgmigrate-version-")).To(BeTrue(), "first Job %q", jobs[0].Name)
		Expect(jobs[0].Spec.Template.Spec.Containers[0].Image).To(Equal(qualifiedRunnerARM64))
	})

	It("refuses a native Migration on an unqualified runner before any Job", func() {
		m, jobs := run("mig-native-unqualified", "ghcr.io/ydixken/rpgmigrate:dev-908ada6")
		validated := meta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		Expect(validated).NotTo(BeNil())
		Expect(validated.Reason).To(Equal(testUnsupportedEngine))
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed))
		Expect(jobs).To(BeEmpty())
	})
})
