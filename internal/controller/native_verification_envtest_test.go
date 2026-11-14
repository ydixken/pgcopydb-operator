package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
)

var _ = Describe("Native compare progress", func() {
	It("persists the helper's final data sample through the API server", func(ctx SpecContext) {
		const name = "native-compare-progress"
		m := validMigration(name)
		m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
		m.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(removeMigration, name)

		finished := metav1.NewTime(time.Now().Add(-time.Minute))
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: compareJobName(m, compareData), Namespace: testNS}, Status: batchv1.JobStatus{
			Succeeded: 1, CompletionTime: &finished,
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: finished}},
		}}
		r := newReconciler()
		r.setCompareProgress(m, v1beta1.VerificationProgressData, job,
			nativeCompareSample(native.ComparisonProgress{Tables: 3, Bytes: 131072, Rows: 20, DifferingTables: 1, ElapsedMs: 10059}))
		Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())

		live := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), live)).To(Succeed())
		Expect(live.Status.VerificationProgress).To(HaveLen(1))
		p := live.Status.VerificationProgress[0]
		Expect(p.Kind).To(Equal(v1beta1.VerificationProgressData))
		Expect(p.JobName).To(Equal(name + "-compare-data"))
		Expect(p.Final).To(BeTrue())
		Expect([]int64{p.PlannedTables, p.ComparedTables, p.PlannedBytes.Value(), p.ComparedBytes.Value(), p.ComparedRows, p.DifferingTables, p.FailedTables}).
			To(Equal([]int64{3, 3, 131072, 131072, 20, 1, 0}))
		Expect(p.Elapsed.Duration).To(Equal(10059 * time.Millisecond))
		Expect(p.InFlight).To(BeEmpty())
		// The persisted item counts as the Job's final sample, so a later pass keeps it.
		Expect(compareProgressFinal(live, v1beta1.VerificationProgressData, job)).To(BeTrue())
	})
})
