package controller

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	admissionLegacyEngine = "pgcopydb"
	admissionNativeEngine = "rpgmigrate"
)

var _ = Describe("Migration engine admission", func() {
	for _, version := range []string{"v1alpha1", "v1beta1"} {
		Context(version, func() {
			DescribeTable("defaults and preserves the engine across served versions", func(engine, want string) {
				m := engineAdmissionMigration(version, engine)
				Expect(k8sClient.Create(ctx, m)).To(Succeed())
				DeferCleanup(func() { Expect(k8sClient.Delete(ctx, m)).To(Succeed()) })
				for _, gv := range []string{v1alpha1.GroupVersion.String(), v1beta1.GroupVersion.String()} {
					got := &unstructured.Unstructured{}
					got.SetAPIVersion(gv)
					got.SetKind("Migration")
					Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), got)).To(Succeed())
					value, found, err := unstructured.NestedString(got.Object, "spec", "engine")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
					Expect(value).To(Equal(want))
				}
			},
				Entry("omitted stays legacy", "", admissionLegacyEngine),
				Entry("explicit legacy", admissionLegacyEngine, admissionLegacyEngine),
				Entry("explicit native", admissionNativeEngine, admissionNativeEngine),
			)

			DescribeTable("rejects invalid engines", func(engine any) {
				m := engineAdmissionMigration(version, "")
				Expect(unstructured.SetNestedField(m.Object, engine, "spec", "engine")).To(Succeed())
				err := k8sClient.Create(ctx, m)
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got: %v", err)
				Expect(err.Error()).To(ContainSubstring("spec.engine"))
			},
				Entry("unknown", "invalid"),
				Entry("wrong case", "PGCOPYDB"),
				Entry("empty string", ""),
				Entry("wrong type", int64(1)),
			)

			DescribeTable("keeps the effective engine immutable", func(oldEngine, newEngine string, allowed bool) {
				m := engineAdmissionMigration(version, oldEngine)
				Expect(k8sClient.Create(ctx, m)).To(Succeed())
				DeferCleanup(func() { Expect(k8sClient.Delete(ctx, m)).To(Succeed()) })
				if newEngine == "" {
					unstructured.RemoveNestedField(m.Object, "spec", "engine")
				} else {
					Expect(unstructured.SetNestedField(m.Object, newEngine, "spec", "engine")).To(Succeed())
				}
				Expect(unstructured.SetNestedField(m.Object, true, "spec", "suspend")).To(Succeed())
				if version == "v1alpha1" {
					m.SetAPIVersion(v1beta1.GroupVersion.String())
				} else {
					m.SetAPIVersion(v1alpha1.GroupVersion.String())
				}
				err := k8sClient.Update(ctx, m)
				if allowed {
					Expect(err).NotTo(HaveOccurred())
				} else {
					Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got: %v", err)
					Expect(err.Error()).To(ContainSubstring("engine is immutable"))
				}
			},
				Entry("omitted to omitted", "", "", true),
				Entry("omitted to legacy", "", admissionLegacyEngine, true),
				Entry("omitted to native", "", admissionNativeEngine, false),
				Entry("legacy to omitted", admissionLegacyEngine, "", true),
				Entry("legacy to legacy", admissionLegacyEngine, admissionLegacyEngine, true),
				Entry("legacy to native", admissionLegacyEngine, admissionNativeEngine, false),
				Entry("native to omitted", admissionNativeEngine, "", false),
				Entry("native to legacy", admissionNativeEngine, admissionLegacyEngine, false),
				Entry("native to native", admissionNativeEngine, admissionNativeEngine, true),
			)
		})
	}
})

func engineAdmissionMigration(version, engine string) *unstructured.Unstructured {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(validMigration(fmt.Sprintf("engine-%s-test", version)))
	Expect(err).NotTo(HaveOccurred())
	m := &unstructured.Unstructured{Object: obj}
	m.SetAPIVersion(v1beta1.GroupVersion.Group + "/" + version)
	m.SetKind("Migration")
	if engine != "" {
		Expect(unstructured.SetNestedField(m.Object, engine, "spec", "engine")).To(Succeed())
	}
	return m
}
