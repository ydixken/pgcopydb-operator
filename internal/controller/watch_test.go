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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The pacing of a running migration rests on two facts about the API server:
// a status write leaves the generation alone, and marking an object for
// deletion bumps it. The first is what stops the operator's own writes from
// waking it again immediately (they used to, once or twice a second right
// through a cutover drain, with a pgcopydb exec on each pass); the second is
// what keeps deletion arriving despite the filter. Both are asserted here
// against a real API server, on real Migration writes, because a change to
// either would strand a migration rather than fail a unit test somewhere.
var _ = Describe("Migration Controller watch filter", func() {
	ctx := context.Background()

	get := func(name string) *v1beta1.Migration {
		GinkgoHelper()
		m := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, m)).To(Succeed())
		return m
	}
	passes := func(older, newer *v1beta1.Migration) bool {
		return migrationEvents.Update(event.UpdateEvent{ObjectOld: older, ObjectNew: newer})
	}

	It("drops the operator's own status writes and keeps spec and deletion", func() {
		const name = "mig-watch-filter"
		defer removeMigration(ctx, name)
		Expect(k8sClient.Create(ctx, validMigration(name))).To(Succeed())
		created := get(name)

		// A status write: the whole replication block, the way a streaming
		// pass rewrites it every time the source's WAL head moves.
		lag := int64(4096)
		updated := created.DeepCopy()
		updated.Status.Phase = v1beta1.PhaseStreaming
		updated.Status.Replication = &v1beta1.ReplicationStatus{
			SlotName: "s", WriteLSN: "0/2000", ReplayLSN: "0/1000", LagBytes: &lag,
		}
		Expect(k8sClient.Status().Update(ctx, updated)).To(Succeed())
		streaming := get(name)
		Expect(streaming.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
		Expect(streaming.ResourceVersion).NotTo(Equal(created.ResourceVersion), "the status write did not land")
		Expect(streaming.Generation).To(Equal(created.Generation))
		Expect(passes(created, streaming)).To(BeFalse(), "a status write must not enqueue another pass")

		// A spec change is what the operator must react to: approving a
		// cutover has to reach it without waiting out the poll interval.
		approved := streaming.DeepCopy()
		approved.Spec.Cutover.Approved = true
		Expect(k8sClient.Update(ctx, approved)).To(Succeed())
		approved = get(name)
		Expect(passes(streaming, approved)).To(BeTrue(), "a spec change must enqueue a pass")

		// Deletion arrives as an update while a finalizer holds the object,
		// and the API server bumps the generation for it.
		withFinalizer := approved.DeepCopy()
		controllerutil.AddFinalizer(withFinalizer, finalizerName)
		Expect(k8sClient.Update(ctx, withFinalizer)).To(Succeed())
		held := get(name)
		Expect(k8sClient.Delete(ctx, held)).To(Succeed())
		deleting := get(name)
		Expect(deleting.DeletionTimestamp).NotTo(BeNil())
		Expect(passes(held, deleting)).To(BeTrue(), "deletion must enqueue a pass")

		// Let the fixture go: removeMigration cannot delete a finalized CR.
		freed := deleting.DeepCopy()
		controllerutil.RemoveFinalizer(freed, finalizerName)
		Expect(k8sClient.Update(ctx, freed)).To(Succeed())
	})
})
