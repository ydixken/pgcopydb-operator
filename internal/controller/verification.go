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
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// Verification (spec.verification) runs pgcopydb compare checks on the
// migration's success path, one Job per enabled check. The result is
// information, not a gate: Verified goes True or False (SchemaMismatch /
// DataMismatch), a mismatch adds a warning event, and Complete is set either
// way. Failing the Migration would claim the data did not arrive, which is
// exactly what a finished clone or a verified drain already refuted; what to
// do about a content difference is the operator's decision, not a rollback
// the operator could perform.

// compareSchema and compareData name the two checks; the names appear in Job
// names and event messages.
const (
	compareSchema = "schema"
	compareData   = "data"
)

func compareJobName(m *v1beta1.Migration, check string) string {
	return m.Name + "-compare-" + check
}

func verificationRequested(m *v1beta1.Migration) bool {
	v := m.Spec.Verification
	return v != nil && (v.Schema || v.Data)
}

// enabledChecks returns the requested checks in execution order: schema first
// because it is cheap and its result frames a later data mismatch.
func enabledChecks(m *v1beta1.Migration) []string {
	var checks []string
	if m.Spec.Verification.Schema {
		checks = append(checks, compareSchema)
	}
	if m.Spec.Verification.Data {
		checks = append(checks, compareData)
	}
	return checks
}

// buildCompareJob assembles one compare check as a Job on the worker pod
// shape: pgcopydb compare wants the work-dir catalogs and both connections,
// which is exactly what jobSkeleton provides. Backoff 1 absorbs one infra
// flake (pod eviction) without reporting a false mismatch; a genuine mismatch
// costs one redundant re-run, which is bounded.
func buildCompareJob(m *v1beta1.Migration, runnerImage, check string) (*batchv1.Job, error) {
	args := pgcopydb.CompareSchemaArgs()
	if check == compareData {
		args = pgcopydb.CompareDataArgs()
	}
	job, err := jobSkeleton(m, runnerImage, compareJobName(m, check), args, "", 1)
	if err != nil {
		return nil, err
	}
	addConnectTimeout(job)
	return job, nil
}

// finishClone ends a clone-only migration: CloneCompleted, then verification
// when requested, then Complete. It runs on every pass while the worker Job
// reads succeeded, so it must stay idempotent.
func (r *MigrationReconciler) finishClone(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "CloneSucceeded", "pgcopydb clone finished")

	done, err := r.ensureVerification(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !done {
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval / 3}, nil
	}

	now := metav1.Now()
	m.Status.CompletedAt = &now
	m.Status.Phase = v1beta1.PhaseCompleted
	r.setCondition(m, v1beta1.ConditionComplete, metav1.ConditionTrue, "MigrationSucceeded", "migration finished")
	r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Completed", "Complete", "pgcopydb clone finished")
	return ctrl.Result{}, r.updateStatus(ctx, m, base)
}

// ensureVerification drives the enabled compare Jobs one at a time (schema,
// then data: sequential keeps both databases at one extra scan and both pods
// off each other's RWO work volume) and reports done=false while one still
// runs. Everything is re-derived from the persisted Jobs, never from memory.
func (r *MigrationReconciler) ensureVerification(ctx context.Context, m *v1beta1.Migration) (bool, error) {
	if !verificationRequested(m) {
		return true, nil
	}
	m.Status.Phase = v1beta1.PhaseVerifying

	var mismatched []string
	for _, check := range enabledChecks(m) {
		job, created, err := r.ensureJob(ctx, m, compareJobName(m, check), func() (*batchv1.Job, error) {
			return buildCompareJob(m, r.RunnerImage, check)
		})
		if err != nil {
			return false, err
		}
		if job == nil {
			r.setCondition(m, v1beta1.ConditionVerified, metav1.ConditionUnknown, "VerificationRunning",
				fmt.Sprintf("pgcopydb compare %s running", check))
			if created {
				r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "VerificationStarted", "Verify",
					"running pgcopydb compare %s", check)
			}
			return false, nil
		}
		done, ok := jobFinished(job)
		if !done {
			return false, nil
		}
		if !ok {
			mismatched = append(mismatched, check)
		}
	}

	if len(mismatched) == 0 {
		r.setCondition(m, v1beta1.ConditionVerified, metav1.ConditionTrue, "ComparePassed",
			"pgcopydb compare found source and target matching")
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Verified", "Verify",
			"pgcopydb compare found source and target matching")
		return true, nil
	}
	// A schema mismatch outranks a data one: it usually explains it.
	reason := "DataMismatch"
	if mismatched[0] == compareSchema {
		reason = "SchemaMismatch"
	}
	msg := fmt.Sprintf("pgcopydb compare reported differences (%v); details are in the compare Job logs. "+
		"The transfer itself finished; on a follow migration, writes reaching the target after cutover also show up here",
		mismatched)
	r.setCondition(m, v1beta1.ConditionVerified, metav1.ConditionFalse, reason, msg)
	r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "VerificationMismatch", "Verify", "%s", msg)
	return true, nil
}
