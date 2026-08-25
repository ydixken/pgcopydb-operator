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
	"fmt"
	"path"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

const (
	labelMigration = "pgcopydb-operator.io/migration"
	labelManagedBy = "app.kubernetes.io/managed-by"
	managerName    = "pgcopydb-operator"

	// runnerUID matches the runner image's non-root user (distroless
	// nonroot convention, uid 65532).
	runnerUID int64 = 65532

	// shellPath runs the prelude, and doubles as $0 in script Jobs.
	shellPath = "/bin/sh"

	// workerContainer names the pgcopydb container in every Job this
	// operator builds; podexec targets it by the same name.
	workerContainer = "pgcopydb"
)

func labels(m *v1beta1.Migration) map[string]string {
	return map[string]string{
		labelManagedBy: managerName,
		labelMigration: m.Name,
	}
}

func workPVCName(m *v1beta1.Migration) string   { return m.Name + "-work" }
func filtersCMName(m *v1beta1.Migration) string { return m.Name + "-filters" }
func jobName(m *v1beta1.Migration, attempt int32) string {
	return fmt.Sprintf("%s-run-%d", m.Name, attempt)
}
func cleanupJobName(m *v1beta1.Migration) string   { return m.Name + "-cleanup" }
func verifyJobName(m *v1beta1.Migration) string    { return m.Name + "-verify" }
func preflightJobName(m *v1beta1.Migration) string { return m.Name + "-preflight" }

// buildWorkPVC returns the work-directory claim. It holds pgcopydb's catalogs
// and is the unit of resumability: it survives Job restarts and is only
// removed with the Migration itself (ownerReference garbage collection).
func buildWorkPVC(m *v1beta1.Migration) *corev1.PersistentVolumeClaim {
	size := m.Spec.WorkVolume.Size
	if size.IsZero() {
		size = defaultWorkVolumeSize()
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workPVCName(m),
			Namespace: m.Namespace,
			Labels:    labels(m),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: m.Spec.WorkVolume.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
}

// buildFiltersConfigMap renders the --filters INI, or nil when unused.
func buildFiltersConfigMap(m *v1beta1.Migration) *corev1.ConfigMap {
	ini := pgcopydb.RenderFilters(m.Spec.Clone.Filters)
	if ini == "" {
		return nil
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      filtersCMName(m),
			Namespace: m.Namespace,
			Labels:    labels(m),
		},
		Data: map[string]string{"filters.ini": ini},
	}
}

// buildJob assembles the worker Job for one attempt. backoffLimit is always 0:
// retries are operator-driven so the attempt count and reasons live in the
// Migration status, and each retry resumes from the work-dir catalogs.
func buildJob(m *v1beta1.Migration, runnerImage string, attempt int32) (*batchv1.Job, error) {
	// Attempt 1 restarts (wipes) the work dir: any state found there is
	// foreign. Attempt > 1 resumes from the catalogs; the snapshot of the
	// failed attempt is gone with its process, so --resume needs
	// --not-consistent (see the pgcopydb resume semantics upstream).
	resume := attempt > 1
	args := pgcopydb.CloneArgs(&m.Spec, !resume, resume, resume)
	args = append(args, pgcopydb.FollowArgs(&m.Spec, m.Namespace, m.Name)...)
	return jobSkeleton(m, runnerImage, jobName(m, attempt), args, publicationDropGuard(m, attempt), 0)
}

// publicationDropGuard returns the retry prelude that drops pgcopydb's own
// leftover publication, or "" when the guard does not apply. Background: when
// an attempt dies between CREATE PUBLICATION and the catalog write recording
// it, pgcopydb --resume re-runs the CREATE non-idempotently and fails on its
// own leftover ("already exists", found live). Only the auto-managed
// publication (named after the slot) is ever dropped; a user-provided
// spec.follow.publication is pgcopydb's to leave alone and ours too.
func publicationDropGuard(m *v1beta1.Migration, attempt int32) string {
	if attempt <= 1 || !followEnabled(m) || m.Spec.Follow.Publication != "" {
		return ""
	}
	// effectiveSlotName is safe to interpolate: generated names are
	// [a-z0-9_], and spec.follow.slotName is pattern-restricted to the same
	// charset by the CRD.
	return `psql "$PGCOPYDB_SOURCE_PGURI" -Xqc 'DROP PUBLICATION IF EXISTS "` + effectiveSlotName(m) + `"'`
}

// buildCleanupJob tears down source/target replication state after a live
// migration (or on abort/deletion): pgcopydb stream cleanup drops the slot,
// the auto-created publication, and the target origin. It needs the work-dir
// catalogs and both connections, so it reuses the worker pod shape. Job-level
// retries are fine here: cleanup is idempotent.
func buildCleanupJob(m *v1beta1.Migration, runnerImage string) (*batchv1.Job, error) {
	// Pass the migration's own slot and origin names explicitly: stream
	// cleanup defaults --origin to "pgcopydb", so a follow migration with a
	// generated per-migration origin would leave that origin behind on the
	// target (observed live: origins accumulate while slots are dropped).
	slot := effectiveSlotName(m)
	args := []string{"stream", "cleanup", "--dir", pgcopydb.WorkDir,
		"--slot-name", slot, "--origin", slot}
	job, err := jobSkeleton(m, runnerImage, cleanupJobName(m), args, "", 2)
	if err != nil {
		return nil, err
	}
	addConnectTimeout(job)
	return job, nil
}

// buildVerifyJob checks, after the worker exited 0, that the target really
// applied everything up to endpos. Exit code 0 is not proof of a complete
// drain: a crash between endpos-set and drain-complete makes pgcopydb's
// --resume short-circuit on the receive-side "endpos previously reached" and
// exit 0 without replaying pending WAL (found live, silent data loss).
func buildVerifyJob(m *v1beta1.Migration, runnerImage string) (*batchv1.Job, error) {
	origin := effectiveSlotName(m)
	// The gate has a fast path and a content path, because no LSN can prove
	// the drain on its own.
	//
	// Fast path: the target's replication origin progress within one WAL page
	// of the endpos recorded in the work-dir sentinel. The origin parks at
	// the last applied COMMIT record, which trails endpos by a few non-data
	// records on an active source (observed live: 56 bytes). A larger gap is
	// NOT loss evidence: on an idle source the WAL between the last applied
	// commit and endpos is publication-filtered traffic (autovacuum on
	// unpublished tables, catalog churn) that pgcopydb never applies, so the
	// gap grows with idle time and refuted healthy drains live. The
	// sentinel's replay_lsn cannot gate in the other direction either:
	// pgcopydb advances it past records it never applies (keepalives,
	// filtered transactions) and it advanced normally in the live
	// session_replication_role incident where nothing was applied at all.
	//
	// Content path: when the origin cannot prove the drain, pgcopydb compare
	// data checksums the migrated tables and refuses only on a difference.
	// Both live-found loss modes stay caught, on this path, by content:
	// resume-skips-replay and silent-apply both leave rows missing on the
	// target, and neither can push the origin near endpos (the origin only
	// advances inside successfully committed apply transactions). Idle-source
	// drains and the upstream boundary case (a clean drain parks replay_lsn
	// at the last real commit below endpos) now pass instead of refusing.
	// replay_lsn is printed for diagnosis only: below endpos means the stream
	// was never consumed up to endpos. A loss smaller than the tolerance on
	// an active source remains the residual risk; spec.verification.data is
	// the airtight check.
	script := `set -eu
endpos=$(pgcopydb stream sentinel get --endpos --dir ` + pgcopydb.WorkDir + `)
replay=$(pgcopydb stream sentinel get --replay-lsn --dir ` + pgcopydb.WorkDir + `)
progress=$(psql "$PGCOPYDB_TARGET_PGURI" -tAc "select coalesce(pg_replication_origin_progress('` + origin + `', true)::text, '0/0')")
gap=$(psql "$PGCOPYDB_TARGET_PGURI" -tAc "select pg_wal_lsn_diff('$endpos'::pg_lsn, '$progress'::pg_lsn)")
echo "endpos=$endpos replay_lsn=$replay origin_progress=$progress origin_gap_bytes=$gap"
if [ "$gap" -le 8192 ]; then
  echo "drain verified: origin progress reached endpos within one WAL page"
  exit 0
fi
echo "origin gap exceeds one WAL page; on an idle source that is publication-filtered WAL, not loss. Deciding by content."
if pgcopydb ` + strings.Join(pgcopydb.CompareDataArgs(), " ") + `; then
  echo "drain verified: pgcopydb compare data found all migrated tables matching"
  exit 0
fi
echo "drain refuted: pgcopydb compare data found differences; do not switch applications to the target (the replication slot is kept)"
exit 1`
	// scriptJob keeps the worker pod's passfile prelude: running this under
	// bare /bin/sh once shipped verification that failed auth and falsely
	// refuted every password-based drain (found live).
	return scriptJob(m, runnerImage, verifyJobName(m), script, 1)
}

// The preflight script is assembled per Migration by preflightScriptFor.
// Connectivity leads unconditionally for every migration, the clone-rights
// tier probes what the base copy needs on the target, and the follow battery
// checks every prerequisite the operator can probe via psql before the first
// worker runs; each failed check prints one line naming the exact GRANT or
// setting that fixes it. Every loss and failure mode observed in live testing
// trips one of these checks. The session_replication_role probe is the
// silent-loss gate: without that SET, pgcopydb 0.18 applies nothing while
// reporting success (see docs/reference/prerequisites.md).
// Successful checks print "ok: <check>" and applied grants print
// "remediated: <statement>"; those two prefixes are the log contract
// emitPreflightOutcome parses into events.

// preflightHeader opens every preflight: general connectivity is validated
// first, for clone and follow alike, and logged. Connects are retried so a
// failover blip does not terminal-fail an otherwise sound Migration; six
// misses over ~1 minute is a configuration error, and nothing else is worth
// probing after it. note buffers failure lines and hint the field pointers,
// so the footer can re-print both inside the log tail the condition carries.
// PREFLIGHT_RETRY_SLEEP is a test seam; env values are never shell-evaluated.
// checkv feeds the query via stdin because psql interpolates :'list' only in
// file input, never in -c commands.
const preflightHeader = `set -u
fail=0
fails=''
hints=''
check() { psql "$1" -XAtq -v ON_ERROR_STOP=1 -c "$2"; }
checkv() { printf '%s' "$2" | psql "$1" -XAtq -v ON_ERROR_STOP=1 -v list="$3" -f -; }
note() { echo "$1"; fails="$fails$1
"; fail=1; }
hint() { hints="$hints$1
"; }
connect_retry() {
  n=1
  while ! check "$1" 'select 1' >/dev/null; do
    if [ "$n" -ge 6 ]; then echo "$3"; exit 1; fi
    echo "retry: $2 connectivity attempt $n failed"
    n=$((n+1))
    sleep "${PREFLIGHT_RETRY_SLEEP:-10}"
  done
}
connect_retry "$PGCOPYDB_SOURCE_PGURI" source "preflight: cannot connect to the source database"
echo "ok: connectivity source"
connect_retry "$PGCOPYDB_TARGET_PGURI" target "preflight: cannot connect to the target database"
echo "ok: connectivity target"
`

// superVerifyBlock probes a configured superuser connection: it must connect
// (retried like the primaries). rolsuper=false only warns: managed-Postgres
// admin roles (rds_superuser and friends) can run the grants without the
// attribute, and an apply that truly lacks rights still fails by name.
func superVerifyBlock(s conn.Side) string {
	return strings.NewReplacer("@SIDE@", string(s), "@URI@", conn.SuperURIEnv(s)).Replace(
		`connect_retry "$@URI@" "superuser @SIDE@" "preflight: cannot connect to the @SIDE@ database as the superuserSecretRef user"
echo "ok: superuser @SIDE@ connected"
if [ "$(check "$@URI@" 'select rolsuper::int from pg_roles where rolname = current_user')" != 1 ]; then
  echo "warn: @SIDE@ superuserSecretRef user lacks rolsuper; attempting remediation anyway"
else
  echo "ok: superuser @SIDE@ verified"
fi
`)
}

// cloneRightsHint is the pointer printed when a clone probe fails without a
// target superuser configured; remediation itself lands with the super path.
const cloneRightsHint = `  hint "hint: spec.target.superuserSecretRef lets the operator apply this itself"
`

// cloneSchemasQuery lists the source's non-system schemas; the shell filters
// them against the spec's schema filters (grep -Fx, so filter values never
// reach SQL) before the target probe.
const cloneSchemasQuery = `check "$PGCOPYDB_SOURCE_PGURI" "select n.nspname from pg_namespace n where n.nspname !~ '^pg_' and n.nspname <> 'information_schema' order by 1"`

// cloneSchemaGrantsQuery aggregates the missing GRANT CREATE statements for
// the probed schemas that exist on the target; names come from catalogs and
// are composed server-side (%I), never from the spec.
const cloneSchemaGrantsQuery = `checkv "$PGCOPYDB_TARGET_PGURI" "select string_agg(format('GRANT CREATE ON SCHEMA %I TO %I', n.nspname, current_user), '; ') from pg_namespace n where n.nspname = any(string_to_array(:'list', chr(10))) and not has_schema_privilege(current_user, n.oid, 'CREATE')" "$sc_list"`

// cloneRightsBlock probes what the base copy needs on the target: CREATE on
// the database, CREATE on every source schema (filter-honoring) already
// present there, and, unless skipped, the ownership db-properties needs.
// The schema probe is the load-bearing one: managed platforms grant database
// CREATE while the PostgreSQL 15+ pg_database_owner split withholds the
// schema right, and every attempt then dies in pg_restore (#119).
func cloneRightsBlock(superTgt, dbProperties bool) string {
	hint := cloneRightsHint
	if superTgt {
		hint = ""
	}
	b := `tgt_user=$(check "$PGCOPYDB_TARGET_PGURI" 'select current_user')
if [ "$(check "$PGCOPYDB_TARGET_PGURI" "select has_database_privilege(current_user, current_database(), 'CREATE')::int")" != 1 ]; then
  stmt=$(check "$PGCOPYDB_TARGET_PGURI" "select format('GRANT CREATE ON DATABASE %I TO %I', current_database(), current_user)")
  note "preflight: target role \"$tgt_user\" lacks CREATE on the target database: $stmt"
` + hint + `else
  echo "ok: clone rights database"
fi
clone_schemas=$(` + cloneSchemasQuery + `)
sc_inc="${PREFLIGHT_SCHEMA_INCLUDE:-}"
sc_exc="${PREFLIGHT_SCHEMA_EXCLUDE:-}"
sc_list=''
while IFS= read -r s; do
  [ -n "$s" ] || continue
  if [ -n "$sc_inc" ] && ! printf '%s\n' "$sc_inc" | grep -Fxq "$s"; then continue; fi
  if printf '%s\n' "$sc_exc" | grep -Fxq "$s"; then continue; fi
  sc_list="$sc_list$s
"
done <<PF_SCHEMAS
$clone_schemas
PF_SCHEMAS
if [ -n "$sc_list" ]; then
  schema_grants=$(` + cloneSchemaGrantsQuery + `)
  if [ -n "$schema_grants" ]; then
    note "preflight: target role \"$tgt_user\" lacks CREATE on schemas the restore targets, run on the target: $schema_grants"
` + indent(hint) + `  else
    echo "ok: clone rights schemas"
  fi
else
  echo "ok: clone rights schemas"
fi
`
	if dbProperties {
		b += `if [ "$(check "$PGCOPYDB_TARGET_PGURI" "select (pg_has_role(current_user, (select datdba from pg_database where datname = current_database()), 'USAGE') or (select rolsuper from pg_roles where rolname = current_user))::int")" != 1 ]; then
  note "preflight: target role \"$tgt_user\" cannot run ALTER DATABASE ... SET (the db-properties step needs ownership): make the role a member of the owning role, or set clone.skip: [dbProperties]"
else
  echo "ok: clone rights db-properties"
fi
`
	}
	return b
}

// indent shifts a hint line to sit inside a nested branch; an empty hint
// stays empty so the emitted script carries no stray blank lines.
func indent(s string) string {
	if s == "" {
		return ""
	}
	return "  " + s
}

const walLevelBlock = `wal_level=$(check "$PGCOPYDB_SOURCE_PGURI" 'show wal_level')
if [ "$wal_level" != logical ]; then
  note "preflight: source wal_level is '$wal_level', follow needs 'logical': set wal_level = logical on the source and restart it"
else
  echo "ok: source wal_level logical"
fi
`

const slotHeadroomBlock = `free_slots=$(check "$PGCOPYDB_SOURCE_PGURI" "select current_setting('max_replication_slots')::int - count(*) from pg_replication_slots")
if [ "${free_slots:-0}" -lt 1 ]; then
  note "preflight: no free replication slot on the source: raise max_replication_slots or drop an unused slot from pg_replication_slots"
else
  echo "ok: replication slot headroom"
fi
`

// replicationAttrCheck is the probe both variants of the block share.
const replicationAttrCheck = `check "$PGCOPYDB_SOURCE_PGURI" 'select (rolreplication or rolsuper)::int from pg_roles where rolname = current_user'`

// replicationAttrStmt composes the exact ALTER ROLE server-side: the server
// escapes its own role name, so a quote-bearing name cannot break out of the
// identifier when the statement later runs over the superuser connection.
const replicationAttrStmt = `check "$PGCOPYDB_SOURCE_PGURI" "select format('ALTER ROLE \"%s\" REPLICATION', replace(current_user::text, '\"', '\"\"'))"`

// replicationAttrBlock checks the source role's REPLICATION attribute. With a
// source superuser the exact ALTER ROLE is applied and re-checked; without one
// the hint names the field that would let the operator do it.
func replicationAttrBlock(super bool) string {
	if super {
		return `src_user=$(check "$PGCOPYDB_SOURCE_PGURI" 'select current_user')
if [ "$(` + replicationAttrCheck + `)" != 1 ]; then
  stmt=$(` + replicationAttrStmt + `)
  if check "$` + conn.SuperURIEnv(conn.Source) + `" "$stmt" >/dev/null; then
    echo "remediated: $stmt"
    if [ "$(` + replicationAttrCheck + `)" != 1 ]; then
      note "preflight: source role \"$src_user\" still lacks the REPLICATION attribute after remediation ($stmt)"
    else
      echo "ok: source replication attribute"
    fi
  else
    note "preflight: source role \"$src_user\" lacks the REPLICATION attribute and applying $stmt via superuserSecretRef failed"
  fi
else
  echo "ok: source replication attribute"
fi
`
	}
	return `src_user=$(check "$PGCOPYDB_SOURCE_PGURI" 'select current_user')
if [ "$(` + replicationAttrCheck + `)" != 1 ]; then
  stmt=$(` + replicationAttrStmt + `)
  note "preflight: source role \"$src_user\" lacks the REPLICATION attribute: $stmt"
  hint "hint: spec.source.superuserSecretRef lets the operator apply this itself"
else
  echo "ok: source replication attribute"
fi
`
}

// originGrantsQuery aggregates the missing GRANT EXECUTE statements for the
// origin functions pgcopydb and the verify Job execute on the target.
const originGrantsQuery = `check "$PGCOPYDB_TARGET_PGURI" "select string_agg(format('GRANT EXECUTE ON FUNCTION %s TO %I;', p.oid::regprocedure, current_user), ' ') from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'pg_catalog' and p.proname in ('pg_replication_origin_oid', 'pg_replication_origin_create', 'pg_replication_origin_drop', 'pg_replication_origin_session_setup', 'pg_replication_origin_xact_setup', 'pg_replication_origin_advance', 'pg_replication_origin_progress') and not has_function_privilege(current_user, p.oid, 'execute')"`

// originGrantsBlock audits EXECUTE on the origin functions. The remediation
// runs the aggregated statements in one psql call, then prints them one per
// line for the log contract; the event bundles them (see emitPreflightOutcome).
func originGrantsBlock(super bool) string {
	if super {
		return `origin_grants=$(` + originGrantsQuery + `)
if [ -n "$origin_grants" ]; then
  if check "$` + conn.SuperURIEnv(conn.Target) + `" "$origin_grants" >/dev/null; then
    printf '%s\n' "$origin_grants" | tr ';' '\n' | sed -e 's/^ *//' -e '/^$/d' | while IFS= read -r g; do echo "remediated: $g;"; done
    origin_grants=$(` + originGrantsQuery + `)
    if [ -n "$origin_grants" ]; then
      note "preflight: target role still lacks EXECUTE on replication origin functions after remediation, run on the target: $origin_grants"
    else
      echo "ok: target origin function grants"
    fi
  else
    note "preflight: target role lacks EXECUTE on replication origin functions and applying the grants via superuserSecretRef failed: $origin_grants"
  fi
else
  echo "ok: target origin function grants"
fi
`
	}
	return `origin_grants=$(` + originGrantsQuery + `)
if [ -n "$origin_grants" ]; then
  note "preflight: target role lacks EXECUTE on replication origin functions, run on the target: $origin_grants"
  hint "hint: spec.target.superuserSecretRef lets the operator apply this itself"
else
  echo "ok: target origin function grants"
fi
`
}

// srrProbe is the rollback-wrapped SET both variants share: the probe IS the
// privilege test.
const srrProbe = `check "$PGCOPYDB_TARGET_PGURI" "begin; set session_replication_role = 'replica'; rollback;"`

// srrStmt composes the GRANT SET server-side, same escaping rationale as
// replicationAttrStmt.
const srrStmt = `check "$PGCOPYDB_TARGET_PGURI" "select format('GRANT SET ON PARAMETER session_replication_role TO \"%s\"', replace(current_user::text, '\"', '\"\"'))"`

// srrBlock checks the silent-loss gate. The GRANT exists on PostgreSQL 15+
// only; on older targets remediation fails loudly, which is still better than
// pgcopydb applying nothing.
func srrBlock(super bool) string {
	if super {
		return `tgt_user=$(check "$PGCOPYDB_TARGET_PGURI" 'select current_user')
if ! ` + srrProbe + ` >/dev/null; then
  stmt=$(` + srrStmt + `)
  if check "$` + conn.SuperURIEnv(conn.Target) + `" "$stmt" >/dev/null; then
    echo "remediated: $stmt"
    if ! ` + srrProbe + ` >/dev/null; then
      note "preflight: target role \"$tgt_user\" still cannot SET session_replication_role after remediation ($stmt)"
    else
      echo "ok: target session_replication_role"
    fi
  else
    note "preflight: target role \"$tgt_user\" cannot SET session_replication_role and applying $stmt via superuserSecretRef failed (the GRANT exists on PostgreSQL 15+ only)"
  fi
else
  echo "ok: target session_replication_role"
fi
`
	}
	return `tgt_user=$(check "$PGCOPYDB_TARGET_PGURI" 'select current_user')
if ! ` + srrProbe + ` >/dev/null; then
  stmt=$(` + srrStmt + `)
  note "preflight: target role \"$tgt_user\" cannot SET session_replication_role, so pgcopydb would apply NOTHING while reporting success: $stmt (PostgreSQL 15+; older targets need a superuser role)"
  hint "hint: spec.target.superuserSecretRef lets the operator apply this itself"
else
  echo "ok: target session_replication_role"
fi
`
}

// riAuditBlock lists tables where pgoutput would reject UPDATE and DELETE at
// write time on the source (relreplident 'n', or 'd' without a primary key).
// It deliberately audits all user tables, filters or not, because a table can
// be filtered out yet still take writes. Never remediated: REPLICA IDENTITY is
// a schema decision. Offenders in spec.follow.allowMissingReplicaIdentity (or
// all of them, with "*") downgrade to a warning line.
const riAuditBlock = `ri_offenders=$(check "$PGCOPYDB_SOURCE_PGURI" "select n.nspname || '.' || c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind = 'r' and c.relpersistence = 'p' and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' and (c.relreplident = 'n' or (c.relreplident = 'd' and not exists (select 1 from pg_index i where i.indrelid = c.oid and i.indisprimary))) order by 1")
if [ -n "$ri_offenders" ]; then
  printf '%s\n' "$ri_offenders" > /tmp/ri_offenders
  ri_allow="${PREFLIGHT_ALLOW_MISSING_RI:-}"
  ack_all=0
  printf '%s\n' "$ri_allow" | grep -Fxq '*' && ack_all=1
  while IFS= read -r tbl; do
    if [ "$ack_all" = 1 ] || printf '%s\n' "$ri_allow" | grep -Fxq "$tbl"; then
      echo "preflight: warning: acknowledged table $tbl has no usable replica identity; UPDATE and DELETE on it will fail on the source during the migration window"
    else
      note "preflight: table $tbl has no replica identity usable for UPDATE/DELETE (the audit covers all user tables, including ones excluded by clone.filters): ALTER TABLE $tbl REPLICA IDENTITY USING INDEX <unique index>, or ALTER TABLE $tbl REPLICA IDENTITY FULL, or acknowledge it in spec.follow.allowMissingReplicaIdentity"
    fi
  done < /tmp/ri_offenders
else
  echo "ok: replica identity audit"
fi`

// preflightScriptFooter closes the check script; conditional blocks (the
// wal2json note) slot in between. Failures re-print as a closing summary with
// the hints last: the Failed condition carries only the log tail, and the
// field pointers must never scroll out under a long audit.
const preflightScriptFooter = `
if [ "$fail" -eq 0 ]; then
  echo "preflight: all checks passed"
else
  printf 'preflight failed:\n%s%s' "$fails" "$hints"
fi
exit "$fail"`

// preflightScriptFor assembles the per-Migration check script: connectivity
// always and first, superuser verification when configured, the clone-rights
// tier for every migration, and the follow battery only for follow migrations.
func preflightScriptFor(m *v1beta1.Migration) string {
	var b strings.Builder
	b.WriteString(preflightHeader)
	superSrc := m.Spec.Source.SuperuserSecretRef != nil
	superTgt := m.Spec.Target.SuperuserSecretRef != nil
	if superSrc {
		b.WriteString(superVerifyBlock(conn.Source))
	}
	if superTgt {
		b.WriteString(superVerifyBlock(conn.Target))
	}
	dbProps := true
	for _, s := range m.Spec.Clone.Skip {
		if s == v1beta1.SkipOption("dbProperties") {
			dbProps = false
		}
	}
	b.WriteString(cloneRightsBlock(superTgt, dbProps))
	if followEnabled(m) {
		b.WriteString(walLevelBlock)
		b.WriteString(slotHeadroomBlock)
		b.WriteString(replicationAttrBlock(superSrc))
		b.WriteString(originGrantsBlock(superTgt))
		b.WriteString(srrBlock(superTgt))
		b.WriteString(riAuditBlock)
		if m.Spec.Follow.Plugin == v1beta1.PluginWal2json {
			b.WriteString(preflightWal2jsonNote)
		}
	}
	b.WriteString(preflightScriptFooter)
	return b.String()
}

// preflightWal2jsonNote is a note, not a check: wal2json ships as a bare
// shared library with no extension control file (upstream README: "does not
// need CREATE EXTENSION"), so neither pg_available_extensions nor
// pg_available_extension_versions ever lists it and there is no catalog to
// query. The only positive probe is creating a logical slot with the plugin,
// which consumes a slot on the source; too invasive for a preflight. The
// failure mode is caught on attempt 1 at slot creation instead.
const preflightWal2jsonNote = `
echo "preflight: note: wal2json presence on the source cannot be verified from SQL (a logical decoding plugin registers no catalog entry); if it is not installed, the first attempt fails at slot creation with: could not access file \"wal2json\""`

// buildPreflightJob probes connectivity and the follow prerequisites once,
// before the first worker Job of any Migration. backoffLimit 1 absorbs a
// single transient blip (pod eviction, connection reset) without failing the
// Migration; a deterministic check failure fails twice and is terminal.
// The replica-identity allowlist travels as an env var, not script text: env
// values are never shell-evaluated, so table names need no quoting rules.
func buildPreflightJob(m *v1beta1.Migration, runnerImage string) (*batchv1.Job, error) {
	// Superuser credentials ride only in this Job: every other Job stays at
	// the migration role's privileges. Super preludes run after the primary
	// ones (they derive their URI from the composed primary URI).
	var extras []*conn.Materialized
	for _, sc := range []struct {
		s conn.Side
		c *v1beta1.PostgresConnection
	}{{conn.Source, &m.Spec.Source}, {conn.Target, &m.Spec.Target}} {
		if mat := conn.MaterializeSuperuser(sc.s, sc.c); mat != nil {
			extras = append(extras, mat)
		}
	}
	job, err := scriptJob(m, runnerImage, preflightJobName(m), preflightScriptFor(m), 1, extras...)
	if err != nil {
		return nil, err
	}
	// Bounds true wedges: hung checks and pods that never start. Slow pulls
	// and autoscaling surface via PreflightRunning long before 30 minutes;
	// the deadline fails the Job, which terminal-fails the Migration
	// instead of looping in Validating.
	deadline := int64(1800)
	job.Spec.ActiveDeadlineSeconds = &deadline
	// The finished preflight is the remediation audit trail and the gate's
	// completion memory; spec TTL must not garbage-collect it. It lives
	// until the Migration is deleted via ownership.
	job.Spec.TTLSecondsAfterFinished = nil
	if f := m.Spec.Follow; f != nil && len(f.AllowMissingReplicaIdentity) > 0 {
		c := &job.Spec.Template.Spec.Containers[0]
		// Newline-joined: the script matches whole lines (grep -Fx), so an
		// entry that itself contained a newline degrades into two entries
		// that match nothing, failing safe.
		c.Env = append(c.Env, corev1.EnvVar{
			Name:  "PREFLIGHT_ALLOW_MISSING_RI",
			Value: strings.Join(f.AllowMissingReplicaIdentity, "\n"),
		})
	}
	// The schema filters travel the same way for the clone-rights probe: the
	// shell matches them with grep -Fx, so filter values never reach SQL.
	if f := m.Spec.Clone.Filters; f != nil {
		c := &job.Spec.Template.Spec.Containers[0]
		if len(f.IncludeOnlySchemas) > 0 {
			c.Env = append(c.Env, corev1.EnvVar{
				Name:  "PREFLIGHT_SCHEMA_INCLUDE",
				Value: strings.Join(f.IncludeOnlySchemas, "\n"),
			})
		}
		if len(f.ExcludeSchemas) > 0 {
			c.Env = append(c.Env, corev1.EnvVar{
				Name:  "PREFLIGHT_SCHEMA_EXCLUDE",
				Value: strings.Join(f.ExcludeSchemas, "\n"),
			})
		}
	}
	return job, nil
}

// scriptJob reuses the worker pod shape (env, mounts, passfile prelude) to
// run a shell script instead of a pgcopydb argv: the prelude execs $0, which
// here is /bin/sh -c <script> instead of pgcopydb.
func scriptJob(m *v1beta1.Migration, runnerImage, name, script string, backoff int32, extras ...*conn.Materialized) (*batchv1.Job, error) {
	job, err := jobSkeleton(m, runnerImage, name, []string{"-c", script}, "", backoff, extras...)
	if err != nil {
		return nil, err
	}
	cmd := job.Spec.Template.Spec.Containers[0].Command
	cmd[len(cmd)-1] = shellPath
	addConnectTimeout(job)
	return job, nil
}

// addConnectTimeout bounds libpq connects for the operator's own control
// Jobs (scripts, cleanup, compare); libpq's default is unlimited, which
// wedged a customer's preflight. The worker is exempt: pgcopydb's many
// data-path handshakes under load must not race a 10s cap, and a wedged
// worker surfaces via zombie detection instead.
func addConnectTimeout(job *batchv1.Job) {
	c := &job.Spec.Template.Spec.Containers[0]
	c.Env = append(c.Env, corev1.EnvVar{Name: "PGCONNECT_TIMEOUT", Value: "10"})
}

// jobSkeleton builds the shared worker pod shape around the given argv. setup
// is optional shell run by the prelude after the passfile is assembled and
// before pgcopydb starts (see conn.PreludeScript). extras are additional
// credential sets (the preflight's superuser connections); their preludes run
// after the primary sides' in the given order.
func jobSkeleton(m *v1beta1.Migration, runnerImage, name string, args []string, setup string, backoff int32, extras ...*conn.Materialized) (*batchv1.Job, error) {
	src, err := conn.Materialize(conn.Source, &m.Spec.Source)
	if err != nil {
		return nil, err
	}
	tgt, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		return nil, err
	}
	sets := append([]*conn.Materialized{src, tgt}, extras...)

	var env []corev1.EnvVar
	for _, mat := range sets {
		env = append(env, mat.Env...)
	}
	// Structured runner logs for humans and future machine parsing.
	env = append(env, corev1.EnvVar{Name: "PGCOPYDB_LOG_JSON", Value: "on"})

	var passfiles []conn.Passfile
	var preludes []string
	for _, mat := range sets {
		if mat.Passfile != nil {
			passfiles = append(passfiles, *mat.Passfile)
		}
		if mat.Prelude != "" {
			preludes = append(preludes, mat.Prelude)
		}
	}
	if len(passfiles) > 0 || len(preludes) > 0 {
		// PGPASSFILE must live in the container spec, not only in the
		// prelude shell: commands the operator execs into the pod (sentinel
		// reads, the WAL-head query, endpos setting) inherit the spec env,
		// and without it they fail password authentication. Found live by
		// the follow e2e suite; the prelude's own export stays for pid 1.
		// secretRef sides (preludes) always assemble a passfile line too.
		env = append(env, corev1.EnvVar{Name: "PGPASSFILE", Value: conn.PgpassPath})
	}

	volumes := []corev1.Volume{
		{
			Name: "workdir",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: workPVCName(m),
				},
			},
		},
		// The root filesystem is read-only; /tmp holds the assembled
		// passfile and pgcopydb scratch files.
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "workdir", MountPath: pgcopydb.WorkMount},
		{Name: "tmp", MountPath: "/tmp"},
	}
	for _, mat := range sets {
		volumes = append(volumes, mat.Volumes...)
		mounts = append(mounts, mat.Mounts...)
	}

	if cm := buildFiltersConfigMap(m); cm != nil {
		volumes = append(volumes, corev1.Volume{
			Name: "filters",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cm.Name},
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "filters", MountPath: path.Dir(pgcopydb.FiltersPath), ReadOnly: true,
		})
	}

	image := runnerImage
	if m.Spec.Runner.Image != "" {
		image = m.Spec.Runner.Image
	}

	uid := runnerUID
	runAsNonRoot := true
	noPrivEsc := false
	readOnlyRoot := true

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.Namespace,
			Labels:    labels(m),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: m.Spec.TTLSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(m)},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &runAsNonRoot,
						RunAsUser:    &uid,
						RunAsGroup:   &uid,
						// The PVC must be writable by the runner user.
						FSGroup:        &uid,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					NodeSelector: m.Spec.Runner.NodeSelector,
					Tolerations:  m.Spec.Runner.Tolerations,
					Affinity:     m.Spec.Runner.Affinity,
					Volumes:      volumes,
					Containers: []corev1.Container{{
						Name:  workerContainer,
						Image: image,
						// sh -c '<prelude>' pgcopydb <args...>: the prelude
						// assembles the passfile, runs setup, and execs
						// "$0" "$@", where $0 is "pgcopydb" (scriptJob swaps
						// it for /bin/sh) and $@ are the Args below.
						Command:      []string{shellPath, "-c", conn.PreludeScript(preludes, passfiles, setup), "pgcopydb"},
						Args:         args,
						Env:          env,
						VolumeMounts: mounts,
						Resources:    m.Spec.Runner.Resources,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivEsc,
							ReadOnlyRootFilesystem:   &readOnlyRoot,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
				},
			},
		},
	}
	return job, nil
}

func defaultWorkVolumeSize() resource.Quantity {
	return resource.MustParse("10Gi")
}
