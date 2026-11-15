package controller

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The API server, not the controller, guards the runner record, so these
// tests write status directly and run no reconcile.
var _ = Describe("native runner record admission", func() {
	created := func(name string) *v1beta1.Migration {
		m := validMigration(name)
		m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, m) })
		return m
	}
	statusPatch := func(m *v1beta1.Migration, body string) error {
		return k8sClient.Status().Patch(ctx, m, client.RawPatch(types.MergePatchType, []byte(body)))
	}
	recorded := func(name string) *v1beta1.Migration {
		m := created(name)
		Expect(statusPatch(m, fmt.Sprintf(`{"status":{"nativeRunnerImage":%q}}`, qualifiedRunnerARM64))).To(Succeed())
		Expect(m.Status.NativeRunnerImage).To(Equal(qualifiedRunnerARM64))
		return m
	}
	stored := func(m *v1beta1.Migration) string {
		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), got)).To(Succeed())
		return got.Status.NativeRunnerImage
	}

	It("rejects removing the whole status once the record exists", func() {
		m := recorded("mig-record-status-null")
		err := statusPatch(m, `{"status":null}`)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "status removal: %v", err)
		Expect(stored(m)).To(Equal(qualifiedRunnerARM64))

		emptied := m.DeepCopy()
		emptied.Status = v1beta1.MigrationStatus{}
		err = k8sClient.Status().Update(ctx, emptied)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "zero status update: %v", err)
		Expect(stored(m)).To(Equal(qualifiedRunnerARM64))

		alpha := &v1alpha1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), alpha)).To(Succeed())
		err = k8sClient.Status().Patch(ctx, alpha, client.RawPatch(types.MergePatchType, []byte(`{"status":null}`)))
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "v1alpha1 status removal: %v", err)
		Expect(stored(m)).To(Equal(qualifiedRunnerARM64))
	})

	It("rejects removing or changing the field once recorded", func() {
		m := recorded("mig-record-field")
		err := statusPatch(m, `{"status":{"nativeRunnerImage":null}}`)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "field removal: %v", err)
		err = statusPatch(m, fmt.Sprintf(`{"status":{"nativeRunnerImage":%q}}`, qualifiedRunnerAMD64))
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "field change: %v", err)
		Expect(stored(m)).To(Equal(qualifiedRunnerARM64))
	})

	It("accepts the first record and unrelated status writes", func() {
		m := created("mig-record-first")
		Expect(statusPatch(m, `{"status":{"phase":"Pending"}}`)).To(Succeed())
		Expect(statusPatch(m, `{"status":null}`)).To(Succeed(), "a Migration without the record keeps status removable")
		Expect(statusPatch(m, fmt.Sprintf(`{"status":{"nativeRunnerImage":%q}}`, qualifiedRunnerARM64))).To(Succeed())
		Expect(statusPatch(m, `{"status":{"phase":"Cloning","attempts":1}}`)).To(Succeed())
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCloning))
		Expect(stored(m)).To(Equal(qualifiedRunnerARM64))
	})
})
