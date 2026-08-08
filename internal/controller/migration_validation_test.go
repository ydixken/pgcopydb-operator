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
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

// Exercises the CRD CEL rules through the envtest apiserver: the schema in
// config/crd/bases is what real clusters enforce, so this is where a broken
// rule would surface.
var _ = Describe("Migration CRD validation", func() {
	ctx := context.Background()

	It("rejects wal2jsonNumericAsString under any non-wal2json plugin", func() {
		m := validMigration("val-wal2json-reject")
		// Plugin left empty: defaulting fills in pgoutput before validation.
		m.Spec.Follow = &v1alpha1.FollowOptions{Enabled: true, Wal2jsonNumericAsString: true}
		err := k8sClient.Create(ctx, m)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("set plugin: wal2json"))
	})

	It("accepts wal2jsonNumericAsString with plugin wal2json", func() {
		m := validMigration("val-wal2json-accept")
		m.Spec.Follow = &v1alpha1.FollowOptions{
			Enabled: true, Plugin: v1alpha1.PluginWal2json, Wal2jsonNumericAsString: true,
		}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		// Never reconciled, so no finalizer blocks the cleanup delete.
		Expect(k8sClient.Delete(ctx, m)).To(Succeed())
	})
})
