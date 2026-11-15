package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

var _ = Describe("Native replication positions", func() {
	It("stores writeLSN and replayLSN with the lag they measure, and removes all three when the sample is unknown", func(ctx SpecContext) {
		f := newNativeCutoverEnv(ctx, "native-replication-lsn")
		publish := func(got nativeSample) *v1beta1.Migration {
			GinkgoHelper()
			base := &v1beta1.Migration{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.m), base)).To(Succeed())
			m := base.DeepCopy()
			f.r.applyNativeProgress(m, got)
			Expect(f.r.updateNativeStatus(ctx, m, base)).To(Succeed())
			live := &v1beta1.Migration{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.m), live)).To(Succeed())
			return live
		}

		By("publishing a known sample")
		live := publish(nativeSample{phase: nativeProgressStreamingPhase, lagBytes: ptr.To(int64(0x10)), writeLSN: "1/50", replayLSN: nativeReplicationTestReplay})
		rs := live.Status.Replication
		Expect(rs).NotTo(BeNil())
		Expect([]string{rs.WriteLSN, rs.ReplayLSN}).To(Equal([]string{"1/50", nativeReplicationTestReplay}))
		Expect(rs.LagBytes).To(HaveValue(Equal(int64(0x10))))
		Expect(nativeSamplePositionsConsistent(nativeSample{lagBytes: rs.LagBytes, writeLSN: rs.WriteLSN, replayLSN: rs.ReplayLSN})).To(BeTrue())

		By("removing the positions and the lag when the next sample is unknown")
		live = publish(nativeSample{phase: nativeProgressStreamingPhase})
		raw := &unstructured.Unstructured{}
		raw.SetGroupVersionKind(v1beta1.GroupVersion.WithKind("Migration"))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.m), raw)).To(Succeed())
		stored, found, err := unstructured.NestedMap(raw.Object, "status", "replication")
		Expect(err).NotTo(HaveOccurred())
		if found {
			Expect(stored).NotTo(HaveKey("writeLSN"))
			Expect(stored).NotTo(HaveKey("replayLSN"))
			Expect(stored).NotTo(HaveKey("lagBytes"))
		}
		caughtUp := meta.FindStatusCondition(live.Status.Conditions, v1beta1.ConditionCaughtUp)
		Expect(caughtUp).NotTo(BeNil())
		Expect(caughtUp.Status).To(Equal(metav1.ConditionUnknown))
		Expect(caughtUp.Reason).To(Equal("NativeLagUnknown"))
	})
})
