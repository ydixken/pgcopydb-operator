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
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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

// compareReportPath holds the --json report inside the pod's writable /tmp
// for the length of one check; nothing outside the Job reads it.
const compareReportPath = "/tmp/compare-data.json"

// compareReportQuery names every table the report does not show as matching.
// A row for a table whose source side is absent is deliberate: a report whose
// keys the query cannot find would otherwise compare NULL against NULL and
// read as a clean match, which is the blindness this whole change removes. So
// is the row for an empty array, because a compare that examined no table has
// not shown anything to match. psql reads the report itself instead of taking
// it through -v, because Linux caps one argv string at 128 KiB, which a
// pretty-printed report crosses at around 480 tables (measured).
var compareReportQuery = "\\set r `cat " + compareReportPath + "`\n" + `select 'the report lists no table, so nothing was compared'
 where json_array_length(:'r'::json) = 0;
select format('%s.%s: source %s rows (checksum %s), target %s rows (checksum %s)',
              t->>'schema', t->>'name',
              t->'source'->>'rowcount', t->'source'->>'checksum',
              t->'target'->>'rowcount', t->'target'->>'checksum')
  from json_array_elements(:'r'::json) as report(t)
 where t->'source'->>'rowcount' is distinct from t->'target'->>'rowcount'
    or t->'source'->>'checksum' is distinct from t->'target'->>'checksum'
    or t->'source'->>'rowcount' is null
    or t->'source'->>'checksum' is null;
`

// pgcopydb compare data reports a row-count or checksum difference by logging
// it and returning success anyway, so the Job's exit code carries no verdict
// and every gate reading that code passes blind. compare_data_strict re-derives
// the verdict from the --json report, with psql as the parser because the
// runner image ships neither jq nor python, and treats a report it could not
// produce or could not read as a mismatch rather than as a match.
var compareDataStrict = `compare_data_strict() {
  if ! pgcopydb ` + strings.Join(pgcopydb.CompareDataArgs(), " ") + ` >` + compareReportPath + `; then
    echo "compare data could not run; refusing to read that as a match"
    return 1
  fi
  cat ` + compareReportPath + `
  if ! unmatched=$(psql "$PGCOPYDB_TARGET_PGURI" -tAX -v ON_ERROR_STOP=1 -f - <<'SQL'
` + compareReportQuery + `SQL
  ); then
    echo "compare data report could not be evaluated; refusing to read that as a match"
    return 1
  fi
  if [ -n "$unmatched" ]; then
    echo "compare data did not find every table matching between source and target:"
    echo "$unmatched"
    return 1
  fi
  return 0
}
`

// compareDataScript is the whole program of the data compare Job: the
// wrapper's return status is the last command's, so it becomes the Job's.
var compareDataScript = "set -eu\n" + compareDataStrict + "compare_data_strict\n"

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
// costs one redundant re-run, which is bounded. The data check runs through
// compare_data_strict because the bare command cannot fail on a difference;
// compare schema counts its own diffs and exits on them, so it runs as argv.
func buildCompareJob(m *v1beta1.Migration, runnerImage, check string) (*batchv1.Job, error) {
	if check == compareData {
		return scriptJob(m, runnerImage, compareJobName(m, check), compareDataScript, 1)
	}
	job, err := jobSkeleton(m, runnerImage, compareJobName(m, check), pgcopydb.CompareSchemaArgs(), "", 1)
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
	if !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted) {
		// The one progress sample a plain clone gets: no pgcopydb command may
		// run against a live worker (see observeRunningJob), and only now,
		// with it exited, is its catalog nobody's. Best effort: the pod
		// usually leaves Running along with the Job, and then there is nothing
		// to sample, which is acceptable for a finished clone.
		r.sampleCloneProgress(ctx, m, m.Status.JobName)
	}
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "CloneSucceeded", "pgcopydb clone finished")

	done, err := r.ensureVerification(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !done {
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
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
	// Rebuilt from the Jobs each pass, like everything else here, so a restart
	// does not lose which check passed.
	m.Status.Verification = nil
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
		m.Status.Verification = append(m.Status.Verification,
			v1beta1.VerificationResult{Check: check, Passed: ok})
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
