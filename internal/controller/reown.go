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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// reownOwnerEnv carries clone.ownerAfterRestore into the handover Job. The
// role reaches SQL only as a psql variable, never as shell or SQL text.
const reownOwnerEnv = "REOWN_OWNER"

// reownStatementBound caps a single ALTER and the wait for its lock. The
// statements are catalog updates that take milliseconds; anything near this
// is a session holding a conflicting lock, and failing beats a Job that sits
// on the cutover window. PGCONNECT_TIMEOUT (scriptJob) bounds only the connect.
const reownStatementBound = "-c statement_timeout=60s -c lock_timeout=60s"

// reownCandidatesCTE selects every object the handover alters, as
// (sort, kind, nspoid, stmt), and ends without a final SELECT so the executed
// set, the pre-checks and the post-check share one definition. Four filters
// bound it, all load-bearing: owned by current_user, in a user schema,
// oid >= 16384, and not an extension member. A migration role that is the
// bootstrap superuser owns the whole system catalog, so the first filter
// alone would hand pg_catalog to the application role.
const reownCandidatesCTE = `WITH me AS (
  SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = current_user
),
-- classid matters: an OID is unique within a catalog, not across catalogs.
ext_member AS (
  SELECT d.classid, d.objid FROM pg_catalog.pg_depend d WHERE d.deptype = 'e'
),
-- A serial (a) or identity (i) sequence cannot change owner on its own:
-- ATExecChangeOwner raises "cannot change owner of sequence". It follows its
-- table instead, which also covers the sequence of an extension-member
-- table, which carries no deptype e row of its own.
-- refobjsubid > 0 keeps this to a dependency on a column. A partition
-- depends on its parent with refobjsubid = 0 and must stay a candidate.
owned_sequence AS (
  SELECT d.objid
    FROM pg_catalog.pg_depend d
    JOIN pg_catalog.pg_class s ON s.oid = d.objid AND s.relkind = 'S'
   WHERE d.classid = 'pg_catalog.pg_class'::regclass
     AND d.refclassid = 'pg_catalog.pg_class'::regclass
     AND d.refobjsubid > 0
     AND d.deptype IN ('a', 'i')
),
-- !~ '^pg_' also excludes pg_temp_N and pg_toast_temp_N, and unlike a LIKE
-- pattern it needs no backslash escape.
user_schema AS (
  SELECT n.oid, n.nspname, n.nspowner
    FROM pg_catalog.pg_namespace n
   WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
),
transferred_schema AS (
  SELECT n.oid, n.nspname
    FROM user_schema n, me
   WHERE n.nspowner = me.oid
     AND n.oid >= 16384
     AND NOT EXISTS (SELECT 1 FROM ext_member e
                      WHERE e.classid = 'pg_catalog.pg_namespace'::regclass
                        AND e.objid = n.oid)
),
candidates AS (
  SELECT 1 AS sort, 'schema' AS kind, s.oid AS nspoid,
         format('ALTER SCHEMA %I OWNER TO %I', s.nspname, :'owner') AS stmt
    FROM transferred_schema s
  UNION ALL
  -- ALTER TABLE OWNER never recurses, so every partition is its own row.
  SELECT 2, 'relation', n.oid,
         format('ALTER %s %s OWNER TO %I',
                CASE c.relkind WHEN 'S' THEN 'SEQUENCE'
                               WHEN 'v' THEN 'VIEW'
                               WHEN 'm' THEN 'MATERIALIZED VIEW'
                               WHEN 'f' THEN 'FOREIGN TABLE'
                               ELSE 'TABLE' END,
                c.oid::regclass, :'owner')
    FROM pg_catalog.pg_class c
    JOIN user_schema n ON n.oid = c.relnamespace, me
   WHERE c.relowner = me.oid AND c.oid >= 16384
     AND c.relkind IN ('r', 'p', 'S', 'v', 'm', 'f')
     AND NOT EXISTS (SELECT 1 FROM ext_member e
                      WHERE e.classid = 'pg_catalog.pg_class'::regclass AND e.objid = c.oid)
     AND NOT EXISTS (SELECT 1 FROM owned_sequence os WHERE os.objid = c.oid)
  UNION ALL
  -- ALTER ROUTINE resolves functions, procedures and aggregates alike, the
  -- (*) and ordered-set aggregate forms included: regprocedure lists every
  -- argument type, which is the lookup key for all of them.
  SELECT 3, 'routine', n.oid,
         format('ALTER ROUTINE %s OWNER TO %I', p.oid::regprocedure, :'owner')
    FROM pg_catalog.pg_proc p
    JOIN user_schema n ON n.oid = p.pronamespace, me
   WHERE p.proowner = me.oid AND p.oid >= 16384
     AND NOT EXISTS (SELECT 1 FROM ext_member e
                      WHERE e.classid = 'pg_catalog.pg_proc'::regclass AND e.objid = p.oid)
  UNION ALL
  SELECT 4, 'type', n.oid,
         format('ALTER %s %s OWNER TO %I',
                CASE WHEN t.typtype = 'd' THEN 'DOMAIN' ELSE 'TYPE' END,
                t.oid::regtype, :'owner')
    FROM pg_catalog.pg_type t
    JOIN user_schema n ON n.oid = t.typnamespace, me
   WHERE t.typowner = me.oid AND t.oid >= 16384
     -- typrelid = 0 is a non-composite type. Relkind c is a standalone
     -- composite (CREATE TYPE AS); any other relkind is a row type, which
     -- changes owner with its relation and must not get an ALTER of its own.
     AND (t.typrelid = 0
          OR (SELECT c.relkind FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid) = 'c')
     -- An array type follows its element type.
     AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type e WHERE e.typarray = t.oid)
     -- A multirange follows its range type from PostgreSQL 17 on, which
     -- refuses to alter it directly; 14 to 16 leave it behind and need the
     -- explicit statement (both verified live, TestReownCandidateQueries).
     AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_range r
                      WHERE r.rngmultitypid = t.oid
                        AND current_setting('server_version_num')::int >= 170000)
     AND NOT EXISTS (SELECT 1 FROM ext_member e
                      WHERE e.classid = 'pg_catalog.pg_type'::regclass AND e.objid = t.oid)
)`

func reownRequested(m *v1beta1.Migration) bool {
	return m.Spec.Clone.OwnerAfterRestore != ""
}

// reownScript is the handover: same-role short circuit, role and privilege
// pre-checks, a logged statement list, the ALTERs in autocommit, then a
// re-enumeration that must come back empty. Autocommit is deliberate: one
// transaction over many partitions can exhaust max_locks_per_transaction,
// and a rerun after a failed statement picks up exactly what is left.
// The pre-checks mirror what the server enforces: ALTER SCHEMA OWNER needs
// CREATE on the database for the current role, every other ALTER OWNER needs
// CREATE on the schema for the new owner, and a schema in the transfer set
// grants that implicitly once its own ALTER (sort 1) has run. A schema
// transfer also needs the migration role to hold the privileges of the new
// owner, not merely SET ROLE to it: a NOINHERIT membership passes preflight's
// SET ROLE probe but loses USAGE on the schema the moment ALTER SCHEMA OWNER
// runs, which fails every later statement inside it with no way to detect or
// re-remediate afterwards, so this must be caught before any ALTER runs.
// Heredocs that expand $REOWN_CANDIDATES_CTE are unquoted; every other one
// is quoted so no SQL is ever shell-evaluated.
func reownScript() string {
	return `set -u
reown() { psql "$PGCOPYDB_TARGET_PGURI" -XAtq -v ON_ERROR_STOP=1 -v owner="$REOWN_OWNER" -f -; }
die() { printf '%s\n' "$1"; exit 1; }
REOWN_CANDIDATES_CTE=$(cat <<'REOWN_CTE'
` + reownCandidatesCTE + `
REOWN_CTE
)
same=$(reown <<'SQL'
SELECT (current_user::text = :'owner')::int;
SQL
) || die "reown: resolving the migration role on the target failed"
if [ "$same" = 1 ]; then
  echo "ok: ownership already held by the migration role, nothing to hand over"
  exit 0
fi
exists=$(reown <<'SQL'
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'owner')::int;
SQL
) || die "reown: probing the target for the role failed"
[ "$exists" = 1 ] || die "reown: role \"$REOWN_OWNER\" does not exist on the target: create it there, or set clone.ownerAfterRestore to an existing role (see docs/troubleshooting.md)"
super=$(reown <<'SQL'
SELECT rolsuper::int FROM pg_catalog.pg_roles WHERE rolname = current_user;
SQL
) || die "reown: probing the superuser attribute of the migration role failed"
if [ "$super" != 1 ]; then
  grant=$(reown <<SQL
$REOWN_CANDIDATES_CTE
SELECT format('GRANT CREATE ON DATABASE %I TO %I', current_database(), current_user)
 WHERE EXISTS (SELECT 1 FROM candidates c WHERE c.sort = 1)
   AND NOT pg_catalog.has_database_privilege(current_user, current_database(), 'CREATE');
SQL
) || die "reown: probing CREATE on the target database failed"
  [ -z "$grant" ] || die "reown: the migration role lacks CREATE on the target database, which ALTER SCHEMA OWNER needs; run on the target: $grant"
  grants=$(reown <<SQL
$REOWN_CANDIDATES_CTE
SELECT string_agg(format('GRANT CREATE ON SCHEMA %I TO %I', n.nspname, :'owner'), '; ' ORDER BY n.nspname)
  FROM user_schema n
 WHERE n.oid IN (SELECT c.nspoid FROM candidates c WHERE c.sort > 1)
   AND n.oid NOT IN (SELECT t.oid FROM transferred_schema t)
   AND NOT pg_catalog.has_schema_privilege(:'owner'::name, n.oid, 'CREATE');
SQL
) || die "reown: probing CREATE on the schemas holding objects to hand over failed"
  [ -z "$grants" ] || die "reown: role \"$REOWN_OWNER\" lacks CREATE on schemas it would own objects in, which ALTER OWNER needs; run on the target: $grants"
  inherit=$(reown <<SQL
$REOWN_CANDIDATES_CTE
SELECT CASE WHEN current_setting('server_version_num')::int >= 160000
            THEN format('GRANT %I TO %I WITH INHERIT TRUE', :'owner', current_user)
            ELSE format('ALTER ROLE %I INHERIT', current_user) END
 WHERE EXISTS (SELECT 1 FROM candidates c WHERE c.sort = 1)
   AND NOT pg_catalog.pg_has_role(current_user, :'owner', 'USAGE');
SQL
) || die "reown: probing whether the migration role inherits \"$REOWN_OWNER\" failed"
  [ -z "$inherit" ] || die "reown: the migration role can SET ROLE to \"$REOWN_OWNER\" but does not inherit its privileges, which ALTER SCHEMA OWNER needs; run on the target: $inherit"
fi
reown <<SQL || die "reown: counting the objects to hand over failed"
$REOWN_CANDIDATES_CTE
SELECT format('reown: %s %s statement(s)', count(*), kind) FROM candidates GROUP BY sort, kind ORDER BY sort;
SQL
echo "reown: statements (first 200):"
reown <<SQL || die "reown: listing the objects to hand over failed"
$REOWN_CANDIDATES_CTE
SELECT stmt FROM candidates ORDER BY sort, stmt LIMIT 200;
SQL
reown <<SQL || die "reown: a statement failed (psql stops at the first error, see above); statements already applied stay applied, and a rerun re-applies the rest"
$REOWN_CANDIDATES_CTE
SELECT stmt FROM candidates ORDER BY sort, stmt \gexec
SQL
left=$(reown <<SQL
$REOWN_CANDIDATES_CTE
SELECT stmt FROM candidates ORDER BY sort, stmt LIMIT 20;
SQL
) || die "reown: re-checking ownership after the handover failed"
if [ -n "$left" ]; then
  printf '%s\n' "$left"
  die "reown: the objects above are still owned by the migration role after the handover"
fi
echo "ok: ownership handed over to \"$REOWN_OWNER\""
`
}

// buildReownJob assembles the handover as a script Job on the worker pod
// shape. It connects as the migration role, not through superuserSecretRef:
// the candidate set is "owned by current_user", and a different role would
// select a different, wrong set.
func buildReownJob(m *v1beta1.Migration, runnerImage string) (*batchv1.Job, error) {
	job, err := scriptJob(m, runnerImage, reownJobName(m), reownScript())
	if err != nil {
		return nil, err
	}
	c := &job.Spec.Template.Spec.Containers[0]
	c.Env = append(c.Env,
		corev1.EnvVar{Name: reownOwnerEnv, Value: m.Spec.Clone.OwnerAfterRestore},
		corev1.EnvVar{Name: "PGOPTIONS", Value: reownStatementBound})
	// scriptJob's backoffLimit of 1 would spend a terminal failure on a lock
	// timeout or an evicted pod. Three pod attempts absorb those, and a
	// deterministic privilege error still fails all three and is terminal.
	backoff := int32(2)
	job.Spec.BackoffLimit = &backoff
	deadline := int64(1800)
	job.Spec.ActiveDeadlineSeconds = &deadline
	// The log is the record of which objects changed owner, and the finished
	// Job is this stage's completion memory across reconciles. TTL must not
	// collect either; it lives until the Migration is deleted, via ownership.
	job.Spec.TTLSecondsAfterFinished = nil
	return job, nil
}

// reownGate runs the handover between the copy's end and the step that
// publishes the migration as done. handled=true ends the pass here: the Job
// is still running and the phase reads running, or it failed and the
// Migration is failed with failNote appended to the verdict. Everything is
// re-derived from the persisted Job, so a restarted operator loses nothing.
func (r *MigrationReconciler) reownGate(ctx context.Context, m, base *v1beta1.Migration, running v1beta1.MigrationPhase, failNote string) (ctrl.Result, bool, error) {
	if !reownRequested(m) {
		return ctrl.Result{}, false, nil
	}
	done, failMsg, err := r.ensureReown(ctx, m, failNote)
	switch {
	case err != nil:
		return ctrl.Result{}, true, err
	case failMsg != "":
		r.setCondition(m, v1beta1.ConditionOwnershipApplied, metav1.ConditionFalse, "OwnershipFailed", failMsg)
		r.fail(m, "OwnershipFailed", "Reown", failMsg)
		return ctrl.Result{}, true, r.updateStatus(ctx, m, base)
	case !done:
		m.Status.Phase = running
		if err := r.updateStatus(ctx, m, base); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// ensureReown creates and observes the handover Job. Returns (done,
// failureMessage, err); done=false with an empty message means the Job is
// still running. The failure message carries the pod's own last lines, which
// name the missing role or the exact GRANT, with failNote before them.
func (r *MigrationReconciler) ensureReown(ctx context.Context, m *v1beta1.Migration, failNote string) (bool, string, error) {
	owner := m.Spec.Clone.OwnerAfterRestore
	job, created, err := r.ensureJob(ctx, m, reownJobName(m), func() (*batchv1.Job, error) {
		return buildReownJob(m, r.RunnerImage)
	})
	if err != nil {
		return false, "", err
	}
	if created {
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "OwnershipStarted", "Reown",
			"handing the restored objects to role %s as Job %s", owner, reownJobName(m))
	}
	var done, ok bool
	if job != nil {
		done, ok = jobFinished(job)
	}
	switch {
	case !done:
		r.setCondition(m, v1beta1.ConditionOwnershipApplied, metav1.ConditionUnknown, "OwnershipRunning",
			"ownership handover Job "+reownJobName(m)+" is running")
		return false, "", nil
	case ok:
		// Once per handover, not per pass: finishClone re-runs this behind
		// a long compare, and the audit trail wants one line.
		if !meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionOwnershipApplied) {
			r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "OwnershipApplied", "Reown",
				"restored objects now owned by %s", owner)
		}
		r.setCondition(m, v1beta1.ConditionOwnershipApplied, metav1.ConditionTrue, "OwnershipApplied",
			"restored objects now owned by "+owner)
		return true, "", nil
	}
	msg := "the data is on the target and only the ownership handover to " + owner +
		" failed. The handover is re-runnable and may be partly applied; finish it by hand with the statements in the log of Job " +
		reownJobName(m) + ", see docs/troubleshooting.md" + failNote
	if tail := r.jobLogTail(ctx, m.Namespace, job.Name, preflightLogTail); tail != "" {
		msg += ":\n" + tail
	}
	return false, msg, nil
}

// reownJobExists reports whether the handover Job is present. Its existence
// proves the worker exited 0, because nothing else creates it.
func (r *MigrationReconciler) reownJobExists(ctx context.Context, m *v1beta1.Migration) (bool, error) {
	if !reownRequested(m) {
		return false, nil
	}
	err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: reownJobName(m)}, &batchv1.Job{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}
