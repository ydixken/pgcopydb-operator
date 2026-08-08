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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

var _ = Describe("Migration Controller verification", func() {
	ctx := context.Background()

	jobMissing := func(name string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, &batchv1.Job{})
		return errors.IsNotFound(err)
	}

	It("runs the compare checks in order and completes on match", func() {
		const name = "mig-verify-pass"
		defer removeMigration(ctx, name)
		m := validMigration(name)
		m.Spec.Verification = &v1alpha1.VerificationOptions{Schema: true, Data: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		reconcileAndGet(ctx, newReconciler(), name)

		// Worker done: verification starts with the cheap schema check; the
		// data Job must not exist yet and Complete must wait.
		finishJob(ctx, name+"-run-1", true)
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseVerifying))
		schemaJob := fetchJob(ctx, name+"-compare-schema")
		Expect(strings.Join(schemaJob.Spec.Template.Spec.Containers[0].Args, " ")).
			To(Equal("compare schema --dir /work/pgcopydb"))
		Expect(jobMissing(name + "-compare-data")).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionCloneCompleted)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete)).To(BeFalse())
		cond := meta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionVerified)
		Expect(cond.Status).To(Equal(metav1.ConditionUnknown))

		// Schema matches: the data check follows.
		finishJob(ctx, name+"-compare-schema", true)
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseVerifying))
		dataJob := fetchJob(ctx, name+"-compare-data")
		Expect(strings.Join(dataJob.Spec.Template.Spec.Containers[0].Args, " ")).
			To(Equal("compare data --dir /work/pgcopydb --json"))

		// Data matches too: Verified True and the migration completes.
		finishJob(ctx, name+"-compare-data", true)
		m = reconcileAndGet(ctx, newReconciler(), name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCompleted))
		Expect(m.Status.CompletedAt).NotTo(BeNil())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionVerified)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete)).To(BeTrue())
	})

	It("reports a data mismatch without failing the migration", func() {
		const name = "mig-verify-mismatch"
		defer removeMigration(ctx, name)
		m := validMigration(name)
		m.Spec.Verification = &v1alpha1.VerificationOptions{Data: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		reconcileAndGet(ctx, newReconciler(), name)

		finishJob(ctx, name+"-run-1", true)
		reconcileAndGet(ctx, newReconciler(), name)
		// Only data was requested: no schema Job.
		Expect(jobMissing(name + "-compare-schema")).To(BeTrue())

		finishJob(ctx, name+"-compare-data", false)
		m = reconcileAndGet(ctx, newReconciler(), name)
		// A mismatch is information, not a failure: Verified False with the
		// mismatch reason, but the Migration still completes.
		cond := meta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionVerified)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("DataMismatch"))
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCompleted))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionFailed)).To(BeFalse())
	})

	It("reports a schema mismatch with its own reason", func() {
		const name = "mig-verify-schema-bad"
		defer removeMigration(ctx, name)
		m := validMigration(name)
		m.Spec.Verification = &v1alpha1.VerificationOptions{Schema: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		reconcileAndGet(ctx, newReconciler(), name)

		finishJob(ctx, name+"-run-1", true)
		reconcileAndGet(ctx, newReconciler(), name)
		finishJob(ctx, name+"-compare-schema", false)
		m = reconcileAndGet(ctx, newReconciler(), name)
		cond := meta.FindStatusCondition(m.Status.Conditions, v1alpha1.ConditionVerified)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("SchemaMismatch"))
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCompleted))
	})

	It("verifies a follow migration only after drain and cleanup", func() {
		const name = "mig-verify-follow"
		defer removeMigration(ctx, name)
		fake := &fakeSentinel{}
		r := newReconciler()
		r.Sentinel = fake

		m := validMigration(name)
		m.Spec.Follow = &v1alpha1.FollowOptions{Enabled: true, Plugin: "pgoutput"}
		m.Spec.Cutover = v1alpha1.CutoverSpec{Mode: v1alpha1.CutoverAutomatic}
		m.Spec.Verification = &v1alpha1.VerificationOptions{Schema: true}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		// Follow migrations start behind the preflight gate: the first pass
		// creates only that Job, its success unlocks run-1.
		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-preflight", true)

		// Caught up: Automatic mode freezes the stream, the worker drains.
		fake.state = &sentinel.State{ApplyEnabled: true, WriteLSN: caughtUpLSN, ReplayLSN: caughtUpLSN, SourceHead: caughtUpLSN, Endpos: sentinel.ZeroLSN}
		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-run-1", true)

		// Drain verified, cleanup started: compare MUST NOT run yet (before
		// the drain a live target mismatches by design, and cleanup goes
		// first so the slot stops retaining WAL).
		reconcileAndGet(ctx, r, name)
		finishJob(ctx, name+"-verify", true)
		reconcileAndGet(ctx, r, name)
		fetchJob(ctx, name+"-cleanup")
		Expect(jobMissing(name + "-compare-schema")).To(BeTrue())

		// Cleanup done: now the compare runs, then the migration completes.
		finishJob(ctx, name+"-cleanup", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseVerifying))
		fetchJob(ctx, name+"-compare-schema")
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionCutoverComplete)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete)).To(BeFalse())

		finishJob(ctx, name+"-compare-schema", true)
		m = reconcileAndGet(ctx, r, name)
		Expect(m.Status.Phase).To(Equal(v1alpha1.PhaseCompleted))
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionVerified)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(m.Status.Conditions, v1alpha1.ConditionComplete)).To(BeTrue())
	})
})
