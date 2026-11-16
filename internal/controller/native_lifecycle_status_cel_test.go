package controller

import (
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The API server alone guards these formats, so the tests write status
// directly and run no reconcile.
var _ = Describe("native lifecycle status field admission", func() {
	hex32, hex64 := strings.Repeat("0123456789abcdef", 2), strings.Repeat("0123456789abcdef", 4)
	uid := "0f8fad5b-d9cb-469f-a165-70867728950e"
	intent := "1/" + hex32 + "/" + hex64
	serial := 0

	created := func() *v1beta1.Migration {
		serial++
		m := validMigration(fmt.Sprintf("mig-lifecycle-cel-%d", serial))
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, m) })
		return m
	}
	patch := func(m client.Object, field string, value any) error {
		raw, err := json.Marshal(value)
		Expect(err).NotTo(HaveOccurred())
		body := fmt.Sprintf(`{"status":{%q:%s}}`, field, raw)
		return k8sClient.Status().Patch(ctx, m, client.RawPatch(types.MergePatchType, []byte(body)))
	}
	stored := func(m *v1beta1.Migration) v1beta1.MigrationStatus {
		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), got)).To(Succeed())
		return got.Status
	}

	DescribeTable("accepts a well-formed value, a change and a removal (no transition rule)",
		func(field string, first, second any) {
			m := created()
			Expect(patch(m, field, first)).To(Succeed())
			Expect(patch(m, field, second)).To(Succeed())
			Expect(patch(m, field, nil)).To(Succeed())
			Expect(stored(m)).To(Equal(v1beta1.MigrationStatus{}))
		},
		Entry("nativeWorkerIntent", "nativeWorkerIntent", intent, "2147483647/"+hex32+"/"+hex64),
		Entry("nativeJobSHA256", "nativeJobSHA256", hex64, strings.Repeat("f", 64)),
		Entry("nativeReportJobUID", "nativeReportJobUID", uid, "00000000-0000-4000-8000-000000000001"),
		Entry("nativeFreshAttempt", "nativeFreshAttempt", 2, 2147483647),
	)

	It("stores each well-formed value as written", func() {
		m := created()
		Expect(patch(m, "nativeWorkerIntent", intent)).To(Succeed())
		Expect(patch(m, "nativeJobSHA256", hex64)).To(Succeed())
		Expect(patch(m, "nativeReportJobUID", uid)).To(Succeed())
		Expect(patch(m, "nativeFreshAttempt", 3)).To(Succeed())
		got := stored(m)
		Expect(got.NativeWorkerIntent).To(Equal(intent))
		Expect(got.NativeJobSHA256).To(Equal(hex64))
		Expect(got.NativeReportJobUID).To(Equal(uid))
		Expect(got.NativeFreshAttempt).To(Equal(int32(3)))
	})

	It("leaves every field absent on a status write that does not name it", func() {
		m := created()
		Expect(patch(m, "phase", "Pending")).To(Succeed())
		got := stored(m)
		Expect(got.NativeWorkerIntent).To(BeEmpty())
		Expect(got.NativeJobSHA256).To(BeEmpty())
		Expect(got.NativeReportJobUID).To(BeEmpty())
		Expect(got.NativeFreshAttempt).To(BeZero())
	})

	DescribeTable("rejects a malformed value and keeps the stored one",
		func(field string, valid, malformed any) {
			m := created()
			Expect(patch(m, field, valid)).To(Succeed())
			before := stored(m)
			err := patch(m, field, malformed)
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "%s=%#v: %v", field, malformed, err)
			Expect(stored(m)).To(Equal(before))
		},
		Entry("intent: empty", "nativeWorkerIntent", intent, ""),
		Entry("intent: attempt 0", "nativeWorkerIntent", intent, "0/"+hex32+"/"+hex64),
		Entry("intent: attempt with a leading zero", "nativeWorkerIntent", intent, "01/"+hex32+"/"+hex64),
		Entry("intent: negative attempt", "nativeWorkerIntent", intent, "-1/"+hex32+"/"+hex64),
		Entry("intent: attempt not a number", "nativeWorkerIntent", intent, "x/"+hex32+"/"+hex64),
		Entry("intent: attempt of eleven digits", "nativeWorkerIntent", intent, "10000000000/"+hex32+"/"+hex64),
		Entry("intent: no attempt", "nativeWorkerIntent", intent, "/"+hex32+"/"+hex64),
		Entry("intent: short nonce", "nativeWorkerIntent", intent, "1/"+hex32[1:]+"/"+hex64),
		Entry("intent: long nonce", "nativeWorkerIntent", intent, "1/"+hex32+"0/"+hex64),
		Entry("intent: uppercase nonce", "nativeWorkerIntent", intent, "1/"+strings.ToUpper(hex32)+"/"+hex64),
		Entry("intent: empty nonce", "nativeWorkerIntent", intent, "1//"+hex64),
		Entry("intent: short digest", "nativeWorkerIntent", intent, "1/"+hex32+"/"+hex64[1:]),
		Entry("intent: long digest", "nativeWorkerIntent", intent, "1/"+hex32+"/"+hex64+"0"),
		Entry("intent: non-hex digest", "nativeWorkerIntent", intent, "1/"+hex32+"/"+hex64[1:]+"g"),
		Entry("intent: missing digest", "nativeWorkerIntent", intent, "1/"+hex32),
		Entry("intent: extra segment", "nativeWorkerIntent", intent, intent+"/1"),
		Entry("intent: other separator", "nativeWorkerIntent", intent, "1:"+hex32+":"+hex64),
		Entry("intent: leading space", "nativeWorkerIntent", intent, " "+intent),
		Entry("intent: trailing newline", "nativeWorkerIntent", intent, intent+"\n"),
		Entry("digest: empty", "nativeJobSHA256", hex64, ""),
		Entry("digest: 63 characters", "nativeJobSHA256", hex64, hex64[1:]),
		Entry("digest: 65 characters", "nativeJobSHA256", hex64, hex64+"0"),
		Entry("digest: uppercase", "nativeJobSHA256", hex64, strings.ToUpper(hex64)),
		Entry("digest: non-hex", "nativeJobSHA256", hex64, hex64[1:]+"g"),
		Entry("digest: sha256 prefix", "nativeJobSHA256", hex64, "sha256:"+hex64[7:]),
		Entry("digest: trailing newline", "nativeJobSHA256", hex64[1:]+"0", hex64[1:]+"\n"),
		Entry("report UID: empty", "nativeReportJobUID", uid, ""),
		Entry("report UID: uppercase", "nativeReportJobUID", uid, strings.ToUpper(uid)),
		Entry("report UID: no hyphens", "nativeReportJobUID", uid, strings.ReplaceAll(uid, "-", "")),
		Entry("report UID: moved hyphen", "nativeReportJobUID", uid, "0f8fad5bd-9cb-469f-a165-70867728950e"),
		Entry("report UID: short", "nativeReportJobUID", uid, uid[1:]),
		Entry("report UID: long", "nativeReportJobUID", uid, uid+"0"),
		Entry("report UID: non-hex", "nativeReportJobUID", uid, "0f8fad5b-d9cb-469f-a165-70867728950g"),
		Entry("report UID: braces", "nativeReportJobUID", uid, "{"+uid[1:len(uid)-1]+"}"),
		Entry("report UID: a name", "nativeReportJobUID", uid, "worker-job-uid"),
		Entry("fresh attempt: 1", "nativeFreshAttempt", 2, 1),
		Entry("fresh attempt: 0", "nativeFreshAttempt", 2, 0),
		Entry("fresh attempt: negative", "nativeFreshAttempt", 2, -2),
	)

	It("applies the same rules through v1alpha1", func() {
		m := created()
		alpha := &v1alpha1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), alpha)).To(Succeed())
		Expect(patch(alpha, "nativeWorkerIntent", intent)).To(Succeed())
		for field, malformed := range map[string]any{
			"nativeWorkerIntent": "0/" + hex32 + "/" + hex64, "nativeJobSHA256": hex64[1:],
			"nativeReportJobUID": strings.ToUpper(uid), "nativeFreshAttempt": 1,
		} {
			err := patch(alpha, field, malformed)
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "v1alpha1 %s=%#v: %v", field, malformed, err)
		}
		Expect(stored(m)).To(Equal(v1beta1.MigrationStatus{NativeWorkerIntent: intent}))
	})
})
