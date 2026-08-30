/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// CRD defaulting happens in the API server, so a unit test on the Go types
// cannot see it. This runs against envtest with the real CRDs, which is the
// only place the marker is actually exercised.
//
// It exists because the first attempt at defaulting useCopyBinary shipped a
// plain bool. That marshals its zero value on every request, so the API server
// saw the field present, skipped the default, and the setting stayed off for
// every client that speaks Go rather than YAML. The unit tests all passed.
var _ = Describe("CRD defaulting", func() {
	newMigration := func(name string, clone v1beta1.CloneOptions) *v1beta1.Migration {
		return &v1beta1.Migration{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: v1beta1.MigrationSpec{
				Source: v1beta1.PostgresConnection{Host: "src", Database: testDB, Username: testDB},
				Target: v1beta1.PostgresConnection{Host: "tgt", Database: testDB, Username: testDB},
				Clone:  clone,
			},
		}
	}

	It("turns binary COPY on for a Migration that never mentions it", func() {
		m := newMigration("defaulting-unset", v1beta1.CloneOptions{})
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, m) })

		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), got)).To(Succeed())
		Expect(got.Spec.Clone.UseCopyBinary).NotTo(BeNil(),
			"the API server did not default useCopyBinary; a client that omits it gets nothing")
		Expect(*got.Spec.Clone.UseCopyBinary).To(BeTrue())

		// The whole point: the rendered argv has to carry the flag.
		Expect(pgcopydb.CloneArgs(&got.Spec, false, false, false)).
			To(ContainElement("--use-copy-binary"))
	})

	It("leaves binary COPY off for a Migration that asks for text", func() {
		no := false
		m := newMigration("defaulting-explicit-false", v1beta1.CloneOptions{UseCopyBinary: &no})
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, m) })

		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), got)).To(Succeed())
		Expect(got.Spec.Clone.UseCopyBinary).NotTo(BeNil())
		Expect(*got.Spec.Clone.UseCopyBinary).To(BeFalse(),
			"an explicit false was overwritten by the default")
		Expect(pgcopydb.CloneArgs(&got.Spec, false, false, false)).
			NotTo(ContainElement("--use-copy-binary"))
	})

	// The other performance defaults are applied in Go rather than by the API
	// server, so they must NOT appear on the stored object. Asserting that
	// keeps the two mechanisms from being confused for one another later.
	It("stores no value for the defaults the operator applies itself", func() {
		m := newMigration("defaulting-go-side", v1beta1.CloneOptions{})
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, m) })

		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), got)).To(Succeed())
		Expect(got.Spec.Clone.TableJobs).To(BeZero())
		Expect(got.Spec.Clone.SplitTablesLargerThan).To(BeNil())

		args := pgcopydb.CloneArgs(&got.Spec, false, false, false)
		Expect(args).To(ContainElement("--table-jobs"))
		Expect(args).To(ContainElement("--split-tables-larger-than"))
	})
})
