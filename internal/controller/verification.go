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
// migration's success path, one Job per enabled check. It reports rather than
// gates: Verified goes True or False, a mismatch adds a warning event, and
// Complete is set either way.

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
// An absent source side and an empty array count as unmatched, since NULL
// against NULL would read as a clean match. psql reads the report from the
// file, not through -v: Linux caps one argv string at 128 KiB, which a
// pretty-printed report crosses at about 480 tables.
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

// pgcopydb compare data logs a row-count or checksum difference and still
// exits 0, so compare_data_strict re-derives the verdict from the --json
// report. psql parses it because the runner image ships no jq or python, and
// a report that could not be produced or read counts as a mismatch.
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
// shape: pgcopydb compare needs the work-dir catalogs and both connections.
// Backoff 1 absorbs a pod eviction without a false mismatch. Only the data
// check needs compare_data_strict; compare schema exits on its own diffs.
func buildCompareJob(m *v1beta1.Migration, runnerImage, check string) (*batchv1.Job, error) {
	if check == compareData {
		// Reconcile rejects this in buildJob first; retain a backstop for direct builder callers.
		if m.Spec.Clone.AllDatabases {
			return nil, fmt.Errorf("allDatabases cannot be combined with verification.data: pgcopydb produces no JSON verdict in this mode")
		}
		return scriptJob(m, runnerImage, compareJobName(m, check), compareDataScript)
	}
	job, err := jobSkeleton(m, runnerImage, compareJobName(m, check), pgcopydb.CompareSchemaArgs(m.Spec.Clone.AllDatabases), "", 1)
	if err != nil {
		return nil, err
	}
	addConnectTimeout(job)
	return job, nil
}

// finishClone ends a clone-only migration: CloneCompleted, then the ownership
// handover and the verification when requested, then Complete. It runs on
// every pass while the worker Job reads succeeded, and on every pass after
// the worker Job is gone, so it must stay idempotent.
func (r *MigrationReconciler) finishClone(ctx context.Context, m, base *v1beta1.Migration) (ctrl.Result, error) {
	if !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted) {
		// --resume has exited 0 for a table no rows reached (issue #277), so
		// the catalog decides. Reading it once the worker's pod has left
		// Running takes a Job of its own (see buildCatalogJob).
		if gate := r.progressGate(); !m.Spec.Clone.AllDatabases && gate != "" {
			owed, checked, err := r.ensureCatalogCheck(ctx, m, gate)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !checked {
				if err := r.updateStatus(ctx, m, base); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: pollInterval}, nil
			}
			if owed > 0 {
				p := m.Status.Progress
				msg := fmt.Sprintf("pgcopydb exited 0 but its catalog counts %d of %d tables done; do not use the target as a complete copy",
					p.TablesDone, p.TablesTotal)
				r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionFalse, reasonCloneIncomplete, msg)
				r.fail(m, reasonCloneIncomplete, "Fail", msg)
				return ctrl.Result{}, r.updateStatus(ctx, m, base)
			}
		}
	}
	r.setCondition(m, v1beta1.ConditionCloneCompleted, metav1.ConditionTrue, "CloneSucceeded", "pgcopydb clone finished")

	// The handover runs before the compares: it is the last write, and a
	// compare beside it would read a target still changing hands.
	if res, handled, err := r.reownGate(ctx, m, base, v1beta1.PhaseFinalizing, ""); handled || err != nil {
		return res, err
	}

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

// ensureCatalogCheck creates and observes the post-exit catalog Job a plain
// clone's completion gate reads (see buildCatalogJob). Reports (owed, checked,
// err): checked is false while the Job still runs, and owed, the in-scope
// tables the catalog counts short of the total, is valid only once checked.
func (r *MigrationReconciler) ensureCatalogCheck(ctx context.Context, m *v1beta1.Migration, gate string) (owed int64, checked bool, err error) {
	job, _, err := r.ensureJob(ctx, m, catalogJobName(m), func() (*batchv1.Job, error) {
		return buildCatalogJob(m, r.RunnerImage, gate)
	})
	if err != nil || job == nil {
		return 0, false, err
	}
	finished, _ := jobFinished(job)
	if !finished {
		return 0, false, nil
	}
	if !r.recordCloneProgress(ctx, m, job.Name) {
		return 0, true, nil
	}
	p := m.Status.Progress
	return p.TablesTotal - p.TablesDone, true, nil
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
