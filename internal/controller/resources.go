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
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

const (
	labelMigration     = "pgcopydb-operator.io/migration"
	labelFeatureE2ERun = "pgcopydb-operator.io/feature-e2e-run"
	labelManagedBy     = "app.kubernetes.io/managed-by"
	managerName        = "pgcopydb-operator"

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
	result := map[string]string{
		labelManagedBy: managerName,
		labelMigration: m.Name,
	}
	if value := m.Labels[labelFeatureE2ERun]; value != "" {
		result[labelFeatureE2ERun] = value
	}
	return result
}

func workPVCName(m *v1beta1.Migration) string   { return m.Name + "-work" }
func filtersCMName(m *v1beta1.Migration) string { return m.Name + "-filters" }
func jobName(m *v1beta1.Migration, attempt int32) string {
	return fmt.Sprintf("%s-run-%d", m.Name, attempt)
}
func cleanupJobName(m *v1beta1.Migration) string   { return m.Name + "-cleanup" }
func verifyJobName(m *v1beta1.Migration) string    { return m.Name + "-verify" }
func reownJobName(m *v1beta1.Migration) string     { return m.Name + "-reown" }
func preflightJobName(m *v1beta1.Migration) string { return m.Name + "-preflight" }
func catalogJobName(m *v1beta1.Migration) string   { return m.Name + "-catalog" }

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
	// Older CRDs may lack CEL rules; Reconcile persists builder errors as terminal InvalidSpec.
	if m.Spec.Clone.AllDatabases {
		switch {
		case m.Spec.Clone.DropIfExists:
			return nil, fmt.Errorf("allDatabases cannot be combined with dropIfExists: the maintenance database cannot be dropped")
		case followEnabled(m):
			return nil, fmt.Errorf("allDatabases cannot be combined with follow.enabled: pgcopydb ignores follow in this mode")
		case m.Spec.Verification != nil && m.Spec.Verification.Data:
			return nil, fmt.Errorf("allDatabases cannot be combined with verification.data: pgcopydb produces no JSON verdict in this mode")
		}
	}
	// Attempt 1 wipes the work dir: any state there is foreign. A retry
	// resumes from the catalogs, and the failed attempt's snapshot died with
	// its process, so --resume needs --not-consistent.
	resume := attempt > 1
	args := pgcopydb.CloneArgs(&m.Spec, !resume, resume, resume)
	args = append(args, pgcopydb.FollowArgs(&m.Spec, m.Namespace, m.Name)...)
	job, err := jobSkeleton(m, runnerImage, jobName(m, attempt), args, publicationDropGuard(m, attempt), 0)
	if err != nil {
		return nil, err
	}
	// Only this Job copies data, so only this Job gets the worker defaults:
	// in jobSkeleton they would also reach the preflight, compare and cleanup
	// Jobs, and a cleanup left Pending on a busy cluster leaks a source slot.
	job.Spec.Template.Spec.Containers[0].Resources =
		pgcopydb.EffectiveRunnerResources(m.Spec.Runner.Resources)
	return job, nil
}

// publicationRetryDollarTag is named, not bare $$: kubelet's Command/Args
// expansion reduces "$$" to "$", which corrupts an anonymous DO $$ block.
const publicationRetryDollarTag = "$publication_retry$"

// pgcopydb creates the publication before the slot, but skips creation when
// resuming saved slot state. Only an orphan without a source slot is safe to drop;
// an existing slot without its publication must not start a silent no-op stream.
func publicationDropGuard(m *v1beta1.Migration, attempt int32) string {
	if attempt <= 1 || !followEnabled(m) || m.Spec.Follow.Publication != "" {
		return ""
	}
	if plugin := m.Spec.Follow.Plugin; plugin != "" && plugin != "pgoutput" {
		return ""
	}
	// Generated and CRD-validated slot names contain only [a-z0-9_].
	slot := effectiveSlotName(m)
	return `psql "$PGCOPYDB_SOURCE_PGURI" -Xq -v ON_ERROR_STOP=1 <<'PUBLICATION_RETRY'
DO ` + publicationRetryDollarTag + `
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = '` + slot + `') THEN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = '` + slot + `') THEN
      RAISE EXCEPTION 'publication retry refused: source slot "` + slot + `" exists but auto publication "` + slot + `" is missing';
    END IF;
  ELSE
    DROP PUBLICATION IF EXISTS "` + slot + `";
  END IF;
END;
` + publicationRetryDollarTag + `;
PUBLICATION_RETRY`
}

// buildCleanupJob drops the slot, the auto-created publication and the target
// origin after a live migration, abort or deletion. It reuses the worker pod
// shape for the work-dir catalogs. Job retries are safe: it is idempotent.
func buildCleanupJob(m *v1beta1.Migration, runnerImage string) (*batchv1.Job, error) {
	// Both names are passed explicitly: stream cleanup defaults --origin to
	// "pgcopydb", so a generated per-migration origin would stay on the
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

// verifyProgressPrefix opens the verify Job's one-line copy-counter contract:
// the JSON `list progress` output, read back by recordCloneProgress.
const verifyProgressPrefix = "clone-progress: "

// cloneCountersBlock reads the copy counters from the work dir the worker has
// released. It never votes on the drain verdict: `set -e` is off and errors
// are discarded, so a shut gate or a missing binary just prints no line.
func cloneCountersBlock(gate string) string {
	if gate == "" {
		return ""
	}
	return `clone_progress=$( { set +e
` + gate + `} 2>/dev/null | tr -d '\n' ) || true
if [ -n "$clone_progress" ]; then
  printf '` + verifyProgressPrefix + `%s\n' "$clone_progress"
fi
`
}

// buildVerifyJob checks that the target applied everything up to endpos. The
// worker's exit 0 is no proof of a drain: after a crash between endpos-set and
// drain-complete, --resume short-circuits and exits 0 anyway.
func buildVerifyJob(m *v1beta1.Migration, runnerImage, progressGate string) (*batchv1.Job, error) {
	origin := effectiveSlotName(m)
	// The gate has a fast path and a content path, because no LSN distance
	// proves the drain on its own: unapplied commits and publication-filtered
	// WAL measure alike from here, and a guessed byte tolerance once blessed a
	// cutover that had lost commits
	// (see docs/research/measurements.md#cutover-verification).
	// Fast path: origin progress exactly equal to endpos, excluding the null
	// LSN, which an empty sentinel and a target that applied nothing both read.
	// Content path: every other reading, so nearly every cutover; the
	// whole-database compare is the normal cost of a verdict, not an
	// idle-source exception. compare_data_strict, because the bare command
	// logs a difference and still exits 0.
	// replay_lsn is printed, never compared: pgcopydb advances it past records
	// it never applies, so it reads normal even where nothing was applied. In
	// the log it separates a stream that never arrived from one not applied.
	script := `set -eu
` + compareDataStrict + `endpos=$(pgcopydb stream sentinel get --endpos --dir ` + pgcopydb.WorkDir + `)
replay=$(pgcopydb stream sentinel get --replay-lsn --dir ` + pgcopydb.WorkDir + `)
progress=$(psql "$PGCOPYDB_TARGET_PGURI" -tAc "select coalesce(pg_replication_origin_progress('` + origin + `', true)::text, '` + sentinel.ZeroLSN + `')")
gap=$(psql "$PGCOPYDB_TARGET_PGURI" -tAc "select pg_wal_lsn_diff('$endpos'::pg_lsn, '$progress'::pg_lsn)")
echo "endpos=$endpos replay_lsn=$replay origin_progress=$progress origin_gap_bytes=$gap"
` + cloneCountersBlock(progressGate) + `if [ "$endpos" = "` + sentinel.ZeroLSN + `" ]; then
  echo "the work dir reports no cutover endpos, so the origin has nothing to be measured against. Deciding by content."
elif [ "$gap" -eq 0 ]; then
  echo "drain verified: origin progress $progress equals endpos $endpos, nothing left to apply"
  exit 0
else
  echo "the origin is $gap bytes from endpos, and no distance decides the drain in either direction: unapplied commits and publication-filtered WAL measure alike. Deciding by content."
fi
if compare_data_strict; then
  echo "drain verified: pgcopydb compare data found all migrated tables matching"
  exit 0
fi
echo "drain refuted: pgcopydb compare data did not show the target matching the source (see the line above for whether it found a difference or could not produce a verdict); do not switch applications to the target (the replication slot is kept)"
exit 1`
	// scriptJob keeps the worker pod's passfile prelude: running this under
	// bare /bin/sh once shipped verification that failed auth and falsely
	// refuted every password-based drain (found live).
	return scriptJob(m, runnerImage, verifyJobName(m), script)
}

// buildCatalogJob reads pgcopydb's catalog once a plain clone's worker has
// exited, which exec cannot: the pod has left Running by then (#277).
// finishClone reads only this Job's log, never its exit code.
func buildCatalogJob(m *v1beta1.Migration, runnerImage, progressGate string) (*batchv1.Job, error) {
	return scriptJob(m, runnerImage, catalogJobName(m), "set -eu\n"+cloneCountersBlock(progressGate))
}

// The preflight script is assembled per Migration by preflightScriptFor.
// The session_replication_role probe is the silent-loss gate: without that SET,
// pgcopydb 0.18 applies nothing while reporting success
// (see docs/reference/prerequisites.md).
// Checks print "ok: <check>"; applied grants print "remediated: " (follow tier)
// or "remediated-clone: " (clone tier), which emitPreflightOutcome parses.

// preflightHeader opens every preflight: general connectivity is validated
// with retries. Two consecutive permanent-class errors end the ladder, not
// one: PgBouncer with auth_query and the managed proxies answer "password
// authentication failed" from a cold auth backend after a failover, and a
// preflight failure is terminal.
// checkv feeds its query on stdin: psql interpolates :'list' in file input only.
const preflightHeader = `set -u
fail=0
fails=''
fails_audit=''
hints=''
check() { psql "$1" -XAtq -v ON_ERROR_STOP=1 -c "$2"; }
checkv() { printf '%s' "$2" | psql "$1" -XAtq -v ON_ERROR_STOP=1 -v list="$3" -f -; }
note() { echo "$1"; fails="$fails$1
"; fail=1; }
note_audit() { echo "$1"; fails_audit="$fails_audit$1
"; fail=1; }
hint() { hints="$hints$1
"; }
connect_retry() {
  n=1
  perm=0
  while :; do
    err=$(check "$1" 'select 1' 2>&1 >/dev/null); rc=$?
    [ -n "$err" ] && printf '%s\n' "$err" >&2
    [ "$rc" -eq 0 ] && break
    case "$err" in
    *'password authentication failed'*|*'role "'*'" does not exist'*|*'database "'*'" does not exist'*)
      perm=$((perm+1)) ;;
    *) perm=0 ;;
    esac
    if [ "$perm" -ge 2 ] || [ "$n" -ge 6 ]; then echo "$3${err:+: $err}"; exit 1; fi
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

// superVerifyBlock probes a configured superuser connection. rolsuper=false
// only warns: managed admin roles (rds_superuser and friends) can run the
// grants without the attribute, and a real lack of rights still fails by name.
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

// remPrefixFollow and remPrefixClone open the remediated lines of the two
// preflight tiers; emitPreflightOutcome parses them into per-tier events.
const (
	remPrefixFollow = "remediated: "
	remPrefixClone  = "remediated-clone: "
)

// tgtSuperHint is the pointer printed when a target-side right is missing and
// no target superuser is configured; with one configured the block remediates.
const tgtSuperHint = "hint: spec.target.superuserSecretRef lets the operator apply this itself"

// remSingle describes one probe/remediate/re-check block for a right fixed by
// a single composed statement. Every capture is fail-closed: a psql failure is
// its own named failure, never an ok line. Messages are shell text ($v, $stmt).
type remSingle struct {
	probe    string // command printing 1 when the right is present
	cmdProbe string // alternative: command whose success IS the privilege test
	compose  string // command printing the fixing statement; empty = probe-only block
	superURI string // super env name; empty emits the note+hint variant
	prefix   string // remediated-line prefix (log contract)
	ok       string
	missing  string // note when the right is missing and no super applies
	apply    string // note when the super apply fails
	still    string // note when the re-check still fails after remediation
	onProbe  string // note when the probe itself cannot run
	onComp   string // note when composing the statement fails
	hint     string // hint line for the no-super variant; empty = none
}

// remSingleBlock emits the shared scaffold for remSingle. One generator for
// every single-statement block keeps the fail-closure, the remediated-line
// contract, and the note/hint shape from drifting apart per block.
func remSingleBlock(c remSingle) string {
	missing := `    if stmt=$(` + c.compose + `); then
      note "` + c.missing + `"
`
	if c.hint != "" {
		missing += `      hint "` + c.hint + `"
`
	}
	missing += `    else
      note "` + c.onComp + `"
    fi
`
	if c.superURI != "" {
		missing = `    if stmt=$(` + c.compose + `); then
      if check "$` + c.superURI + `" "$stmt" >/dev/null; then
        echo "` + c.prefix + `$stmt"
` + remRecheck(c) + `      else
        note "` + c.apply + `"
      fi
    else
      note "` + c.onComp + `"
    fi
`
	}
	if c.compose == "" {
		// Probe-only: nothing composes and nothing remediates (db-properties
		// needs ownership, not a grant), the note carries the ways out.
		missing = `    note "` + c.missing + `"
`
	}
	if c.cmdProbe != "" {
		// The probe is the privilege test itself, so its failure IS the
		// missing right; a connection error is indistinguishable and lands on
		// the same note, which still fails closed.
		return `if ` + c.cmdProbe + ` >/dev/null; then
  echo "ok: ` + c.ok + `"
else
` + shiftLeft(missing) + `fi
`
	}
	return `if v=$(` + c.probe + `); then
  if [ "$v" != 1 ]; then
` + missing + `  else
    echo "ok: ` + c.ok + `"
  fi
else
  note "` + c.onProbe + `"
fi
`
}

// remRecheck emits the post-apply re-probe for remSingleBlock.
func remRecheck(c remSingle) string {
	if c.cmdProbe != "" {
		return `        if ` + c.cmdProbe + ` >/dev/null; then
          echo "ok: ` + c.ok + `"
        else
          note "` + c.still + `"
        fi
`
	}
	return `        if v=$(` + c.probe + `); then
          if [ "$v" != 1 ]; then
            note "` + c.still + `"
          else
            echo "ok: ` + c.ok + `"
          fi
        else
          note "` + c.onProbe + `"
        fi
`
}

// shiftLeft drops two leading spaces per line: the cmd-probe scaffold nests
// one level less than the value-probe one.
func shiftLeft(s string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(s, "\n") {
		b.WriteString(strings.TrimPrefix(line, "  "))
		b.WriteString("\n")
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// remAggregate describes a block whose probe query composes the missing-grant
// statements itself: empty output means nothing is missing. Messages may
// reference $agg and caller-captured context.
type remAggregate struct {
	query    string
	superURI string
	prefix   string
	term     string // per-statement terminator the re-echo restores
	ok       string
	missing  string
	apply    string
	still    string
	onProbe  string
	hint     string
}

// remAggBlock emits the shared scaffold for remAggregate; the re-echo
// pipeline that feeds the remediated-line contract exists only here.
func remAggBlock(c remAggregate) string {
	missing := `    note "` + c.missing + `"
`
	if c.hint != "" {
		missing += `    hint "` + c.hint + `"
`
	}
	if c.superURI != "" {
		missing = `    if check "$` + c.superURI + `" "$agg" >/dev/null; then
      printf '%s\n' "$agg" | tr ';' '\n' | sed -e 's/^ *//' -e '/^$/d' | while IFS= read -r g; do echo "` + c.prefix + `$g` + c.term + `"; done
      if agg=$(` + c.query + `); then
        if [ -n "$agg" ]; then
          note "` + c.still + `"
        else
          echo "ok: ` + c.ok + `"
        fi
      else
        note "` + c.onProbe + `"
      fi
    else
      note "` + c.apply + `"
    fi
`
	}
	return `if agg=$(` + c.query + `); then
  if [ -n "$agg" ]; then
` + missing + `  else
    echo "ok: ` + c.ok + `"
  fi
else
  note "` + c.onProbe + `"
fi
`
}

// cloneDBCreateCheck and cloneDBCreateStmt are shared by both block variants;
// the statement is composed server-side (%I) like every remediation statement.
const cloneDBCreateCheck = `check "$PGCOPYDB_TARGET_PGURI" "select has_database_privilege(current_user, current_database(), 'CREATE')::int"`

const cloneDBCreateStmt = `check "$PGCOPYDB_TARGET_PGURI" "select format('GRANT CREATE ON DATABASE %I TO %I', current_database(), current_user)"`

// cloneSchemasQuery lists the source's non-system schemas; the shell filters
// them with grep -Fx afterwards, so spec filter values never reach SQL.
const cloneSchemasQuery = `check "$PGCOPYDB_SOURCE_PGURI" "select n.nspname from pg_namespace n where n.nspname !~ '^pg_' and n.nspname <> 'information_schema' order by 1"`

// cloneSchemaGrantsQuery aggregates the missing GRANT CREATE statements; the
// names come from catalogs and are composed server-side (%I), not from the spec.
const cloneSchemaGrantsQuery = `checkv "$PGCOPYDB_TARGET_PGURI" "select string_agg(format('GRANT CREATE ON SCHEMA %I TO %I', n.nspname, current_user), '; ') from pg_namespace n where n.nspname = any(string_to_array(:'list', chr(10))) and not has_schema_privilege(current_user, n.oid, 'CREATE')" "$sc_list"`

// cloneRightsBlock probes CREATE on the target database and the source schemas
// present there, plus the ownership db-properties needs: managed platforms
// grant the database right while pg_database_owner withholds the schema (#119).
func cloneRightsBlock(superTgt, dbProperties bool) string {
	superURI := ""
	hint := tgtSuperHint
	if superTgt {
		superURI = conn.SuperURIEnv(conn.Target)
		hint = ""
	}
	b := `tgt_user=$(check "$PGCOPYDB_TARGET_PGURI" 'select current_user') || note "preflight: could not resolve the target role"
`
	b += remSingleBlock(remSingle{
		probe:    cloneDBCreateCheck,
		compose:  cloneDBCreateStmt,
		superURI: superURI,
		prefix:   remPrefixClone,
		ok:       "clone rights database",
		missing:  `preflight: target role \"$tgt_user\" lacks CREATE on the target database: $stmt`,
		apply:    `preflight: target role \"$tgt_user\" lacks CREATE on the target database and applying $stmt via superuserSecretRef failed`,
		still:    `preflight: target role \"$tgt_user\" still lacks CREATE on the target database after remediation ($stmt)`,
		onProbe:  `preflight: probing CREATE on the target database failed`,
		onComp:   `preflight: composing the database GRANT failed`,
		hint:     hint,
	})
	// Filtered in shell only, with an explicit -- operand guard, so spec
	// values never reach SQL and option-looking schema names stay data.
	b += `if clone_schemas=$(` + cloneSchemasQuery + `); then
sc_inc="${PREFLIGHT_SCHEMA_INCLUDE:-}"
sc_exc="${PREFLIGHT_SCHEMA_EXCLUDE:-}"
sc_list=''
while IFS= read -r s; do
  [ -n "$s" ] || continue
  if [ -n "$sc_inc" ] && ! printf '%s\n' "$sc_inc" | grep -Fxq -- "$s"; then continue; fi
  if printf '%s\n' "$sc_exc" | grep -Fxq -- "$s"; then continue; fi
  sc_list="$sc_list$s
"
done <<PF_SCHEMAS
$clone_schemas
PF_SCHEMAS
if [ -n "$sc_list" ]; then
` + remAggBlock(remAggregate{
		query:    cloneSchemaGrantsQuery,
		superURI: superURI,
		prefix:   remPrefixClone,
		ok:       "clone rights schemas",
		missing:  `preflight: target role \"$tgt_user\" lacks CREATE on schemas the restore targets, run on the target: $agg`,
		apply:    `preflight: target role \"$tgt_user\" lacks CREATE on schemas the restore targets and applying the grants via superuserSecretRef failed: $agg`,
		still:    `preflight: target role \"$tgt_user\" still lacks CREATE on schemas the restore targets after remediation, run on the target: $agg`,
		onProbe:  `preflight: probing CREATE on the restore's target schemas failed`,
		hint:     hint,
	}) + `else
  echo "ok: clone rights schemas"
fi
else
note "preflight: listing the source schemas for the clone-rights probe failed"
fi
`
	if dbProperties {
		b += remSingleBlock(remSingle{
			probe:   `check "$PGCOPYDB_TARGET_PGURI" "select (pg_has_role(current_user, (select datdba from pg_database where datname = current_database()), 'USAGE') or (select rolsuper from pg_roles where rolname = current_user))::int"`,
			ok:      "clone rights db-properties",
			missing: `preflight: target role \"$tgt_user\" cannot run ALTER DATABASE ... SET (the db-properties step needs ownership): make the role a member of the owning role, or set clone.skip: [dbProperties]`,
			onProbe: `preflight: probing database ownership for the db-properties step failed`,
		})
	}
	return b
}

// reownPreflightOwnerEnv carries clone.ownerAfterRestore into the preflight:
// the role reaches SQL as a psql variable only, never as shell or SQL text.
const reownPreflightOwnerEnv = "PREFLIGHT_OWNER_AFTER_RESTORE"

const reownRoleExistsSQL = `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'list')::int`

const reownRoleProbe = `checkv "$PGCOPYDB_TARGET_PGURI" "` + reownRoleExistsSQL + `" "$reown_owner"`

// reownSetRoleSQL interpolates :"list" (an identifier), not :'list' (a quoted
// literal), so the role name reaches SET ROLE already quoted as an identifier.
const reownSetRoleSQL = `BEGIN; SET ROLE :"list"; ROLLBACK;`

const reownSetRoleProbe = `checkv "$PGCOPYDB_TARGET_PGURI" '` + reownSetRoleSQL + `' "$reown_owner"`

// A plain GRANT carries SET on every supported version (14 to 18), which is
// the part the handover needs; the re-check after the apply proves it.
const reownGrantSQL = `SELECT format('GRANT %I TO %I', :'list', current_user)`

const reownGrantStmt = `checkv "$PGCOPYDB_TARGET_PGURI" "` + reownGrantSQL + `" "$reown_owner"`

// ownerAfterRestoreBlock probes the handover before any worker runs. It uses a
// rolled-back SET ROLE, not pg_has_role(..., 'USAGE'): the two diverge on
// NOINHERIT members and on WITH SET FALSE (TestPreflightOwnerAfterRestoreQueries).
func ownerAfterRestoreBlock(super bool) string {
	superURI := ""
	hint := tgtSuperHint
	if super {
		superURI = conn.SuperURIEnv(conn.Target)
		hint = ""
	}
	// Stop here: a missing role also fails the SET ROLE probe, and its note
	// must not scroll out of the condition's log tail.
	return `reown_owner="${` + reownPreflightOwnerEnv + `:-}"
` + remSingleBlock(remSingle{
		probe:   reownRoleProbe,
		ok:      "ownerAfterRestore role",
		missing: `preflight: clone.ownerAfterRestore role \"$reown_owner\" does not exist on the target: create it there, or name a role that exists`,
		onProbe: `preflight: probing the target for the clone.ownerAfterRestore role failed`,
	}) + preflightStopOnFailure + remSingleBlock(remSingle{
		cmdProbe: reownSetRoleProbe,
		compose:  reownGrantStmt,
		superURI: superURI,
		prefix:   remPrefixClone,
		ok:       "ownerAfterRestore SET ROLE",
		missing:  `preflight: the migration role cannot SET ROLE to \"$reown_owner\", which the ownership handover needs: $stmt`,
		apply:    `preflight: the migration role cannot SET ROLE to \"$reown_owner\" and applying $stmt via superuserSecretRef failed`,
		still:    `preflight: the migration role still cannot SET ROLE to \"$reown_owner\" after remediation ($stmt)`,
		onComp:   `preflight: composing the role GRANT failed`,
		hint:     hint,
	})
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

// replicationAttrStmt composes the ALTER ROLE server-side, so a quote-bearing
// role name cannot break out of the identifier over the superuser connection.
const replicationAttrStmt = `check "$PGCOPYDB_SOURCE_PGURI" "select format('ALTER ROLE \"%s\" REPLICATION', replace(current_user::text, '\"', '\"\"'))"`

// replicationAttrBlock checks the source role's REPLICATION attribute. With a
// source superuser the exact ALTER ROLE is applied and re-checked; without one
// the hint names the field that would let the operator do it.
func replicationAttrBlock(super bool) string {
	superURI := ""
	hint := "hint: spec.source.superuserSecretRef lets the operator apply this itself"
	if super {
		superURI = conn.SuperURIEnv(conn.Source)
		hint = ""
	}
	return `src_user=$(check "$PGCOPYDB_SOURCE_PGURI" 'select current_user') || note "preflight: could not resolve the source role"
` + remSingleBlock(remSingle{
		probe:    replicationAttrCheck,
		compose:  replicationAttrStmt,
		superURI: superURI,
		prefix:   remPrefixFollow,
		ok:       "source replication attribute",
		missing:  `preflight: source role \"$src_user\" lacks the REPLICATION attribute: $stmt`,
		apply:    `preflight: source role \"$src_user\" lacks the REPLICATION attribute and applying $stmt via superuserSecretRef failed`,
		still:    `preflight: source role \"$src_user\" still lacks the REPLICATION attribute after remediation ($stmt)`,
		onProbe:  `preflight: probing the source REPLICATION attribute failed`,
		onComp:   `preflight: composing the ALTER ROLE failed`,
		hint:     hint,
	})
}

// originGrantsQuery aggregates the missing GRANT EXECUTE statements for the
// origin functions pgcopydb and the verify Job execute on the target.
const originGrantsQuery = `check "$PGCOPYDB_TARGET_PGURI" "select string_agg(format('GRANT EXECUTE ON FUNCTION %s TO %I;', p.oid::regprocedure, current_user), ' ') from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'pg_catalog' and p.proname in ('pg_replication_origin_oid', 'pg_replication_origin_create', 'pg_replication_origin_drop', 'pg_replication_origin_session_setup', 'pg_replication_origin_xact_setup', 'pg_replication_origin_advance', 'pg_replication_origin_progress') and not has_function_privilege(current_user, p.oid, 'execute')"`

// originGrantsBlock audits EXECUTE on the origin functions. The remediation
// runs the aggregated statements in one psql call, then prints them one per
// line for the log contract; the event bundles them (see emitPreflightOutcome).
func originGrantsBlock(super bool) string {
	superURI := ""
	hint := tgtSuperHint
	if super {
		superURI = conn.SuperURIEnv(conn.Target)
		hint = ""
	}
	// The composed statements carry their own ';' terminator that the tr
	// split strips, so the re-echo restores it.
	return remAggBlock(remAggregate{
		query:    originGrantsQuery,
		superURI: superURI,
		prefix:   remPrefixFollow,
		term:     ";",
		ok:       "target origin function grants",
		missing:  `preflight: target role lacks EXECUTE on replication origin functions, run on the target: $agg`,
		apply:    `preflight: target role lacks EXECUTE on replication origin functions and applying the grants via superuserSecretRef failed: $agg`,
		still:    `preflight: target role still lacks EXECUTE on replication origin functions after remediation, run on the target: $agg`,
		onProbe:  `preflight: probing the origin function grants failed`,
		hint:     hint,
	})
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
	superURI := ""
	hint := tgtSuperHint
	if super {
		superURI = conn.SuperURIEnv(conn.Target)
		hint = ""
	}
	return `tgt_user=$(check "$PGCOPYDB_TARGET_PGURI" 'select current_user') || note "preflight: could not resolve the target role"
` + remSingleBlock(remSingle{
		cmdProbe: srrProbe,
		compose:  srrStmt,
		superURI: superURI,
		prefix:   remPrefixFollow,
		ok:       "target session_replication_role",
		missing:  `preflight: target role \"$tgt_user\" cannot SET session_replication_role, so pgcopydb would apply NOTHING while reporting success: $stmt (PostgreSQL 15+; older targets need a superuser role)`,
		apply:    `preflight: target role \"$tgt_user\" cannot SET session_replication_role and applying $stmt via superuserSecretRef failed (the GRANT exists on PostgreSQL 15+ only)`,
		still:    `preflight: target role \"$tgt_user\" still cannot SET session_replication_role after remediation ($stmt)`,
		onComp:   `preflight: composing the GRANT SET failed`,
		hint:     hint,
	})
}

// riAuditBlock lists source tables pgoutput would reject UPDATE and DELETE on.
// It covers all user tables, filters or not: a filtered table still takes writes.
const riAuditBlock = `ri_offenders=$(check "$PGCOPYDB_SOURCE_PGURI" "select n.nspname || '.' || c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind = 'r' and c.relpersistence = 'p' and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' and (c.relreplident = 'n' or (c.relreplident = 'd' and not exists (select 1 from pg_index i where i.indrelid = c.oid and i.indisprimary))) order by 1")
if [ -n "$ri_offenders" ]; then
  printf '%s\n' "$ri_offenders" > /tmp/ri_offenders
  ri_allow="${PREFLIGHT_ALLOW_MISSING_RI:-}"
  ack_all=0
  printf '%s\n' "$ri_allow" | grep -Fxq '*' && ack_all=1
  while IFS= read -r tbl; do
    if [ "$ack_all" = 1 ] || printf '%s\n' "$ri_allow" | grep -Fxq -- "$tbl"; then
      echo "preflight: warning: acknowledged table $tbl has no usable replica identity; UPDATE and DELETE on it will fail on the source during the migration window"
    else
      note_audit "preflight: table $tbl has no replica identity usable for UPDATE/DELETE (the audit covers all user tables, including ones excluded by clone.filters): ALTER TABLE $tbl REPLICA IDENTITY USING INDEX <unique index>, or ALTER TABLE $tbl REPLICA IDENTITY FULL, or acknowledge it in spec.follow.allowMissingReplicaIdentity"
    fi
  done < /tmp/ri_offenders
else
  echo "ok: replica identity audit"
fi`

// preflightScriptFooter re-prints failures in tail-survival order, audit first
// and hints last: the Failed condition carries only the log tail.
const preflightScriptFooter = `
if [ "$fail" -eq 0 ]; then
  echo "preflight: all checks passed"
else
  printf 'preflight failed:\n%s%s%s' "$fails_audit" "$fails" "$hints"
fi
exit "$fail"`

// Stop dependent probes before their failures bury prerequisite diagnoses in the condition's log tail.
const preflightStopOnFailure = `
if [ "$fail" -ne 0 ]; then` + preflightScriptFooter + "\nfi\n"

const extensionFiltersEnv = "PREFLIGHT_EXTENSION_FILTERS"

const sourceExtensionsQuery = `SELECT COALESCE(
jsonb_agg(source_extension.extname ORDER BY source_extension.extname), '[]'::jsonb)
FROM pg_catalog.pg_extension AS source_extension, (SELECT :'list'::jsonb AS filters) AS input
WHERE (jsonb_array_length(filters->'include') = 0 OR (filters->'include') ? source_extension.extname::text)
AND NOT ((filters->'exclude') ? source_extension.extname::text);`

const extensionNamesValidQuery = `SELECT CASE WHEN jsonb_typeof(:'list'::jsonb) = 'array' THEN
CASE WHEN NOT EXISTS (SELECT 1 FROM jsonb_array_elements(:'list'::jsonb) AS item WHERE jsonb_typeof(item) <> 'string')
THEN jsonb_array_length(:'list'::jsonb) ELSE -1 END ELSE -1 END;`

const targetExtensionsQuery = `SELECT COALESCE(jsonb_agg(selected.name ORDER BY selected.name), '[]'::jsonb)
FROM jsonb_array_elements_text(:'list'::jsonb) AS selected(name)
LEFT JOIN pg_catalog.pg_extension AS installed ON installed.extname::text COLLATE "C" = selected.name
LEFT JOIN pg_catalog.pg_available_extensions AS available ON available.name::text COLLATE "C" = selected.name
WHERE NOT (@INSTALLED@ OR available.default_version IS NOT NULL);`

// USAGE tests inherited owner privileges, as PostgreSQL's object_ownercheck does.
const targetExtensionOwnershipQuery = `SELECT COALESCE(jsonb_agg(
format('extension %I (owner %I, migration role %I)', installed.extname,
pg_get_userbyid(installed.extowner), current_user) ORDER BY selected.name), '[]'::jsonb)
FROM jsonb_array_elements_text(:'list'::jsonb) AS selected(name)
JOIN pg_catalog.pg_extension AS installed ON installed.extname::text COLLATE "C" = selected.name
WHERE NOT (pg_has_role(current_user, installed.extowner, 'USAGE')
OR (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user));`

func extensionPreflightBlock(drop, allDatabases, ownership bool) string {
	installed := "installed.extname IS NOT NULL"
	if drop {
		installed = "false"
	}
	sourceQuery := sourceExtensionsQuery
	targetQuery := strings.ReplaceAll(targetExtensionsQuery, "@INSTALLED@", installed)
	ok := "selected extensions available"
	missing := "selected extensions unavailable on target: $extension_issues; install the required target extension package or choose a target that provides it"
	selectionCheck := "extension selection"
	targetCheck := "extension availability"
	stopOnFailure := `[ "$fail" -eq 0 ] || exit 1`
	if ownership {
		// pg_dump's selectDumpableExtension omits initdb OIDs below FirstNormalObjectId from DDL and comments.
		sourceQuery = strings.TrimSuffix(sourceQuery, ";") + "\nAND source_extension.oid >= 16384;"
		targetQuery = targetExtensionOwnershipQuery
		ok = "selected extension ownership"
		selectionCheck = "extension ownership selection"
		targetCheck = "extension ownership"
		route, skip := "COMMENT ON EXTENSION (dropIfExists: false)", "extensionComments"
		if drop {
			route, skip = "DROP EXTENSION (dropIfExists: true)", "extensions"
		}
		missing = "target extension ownership required for " + route + ": $extension_issues; use a migration role with the owner's privileges, or set clone.skip: [" + skip + "]"
		stopOnFailure = preflightStopOnFailure
	}
	selection := `checkv "$PGCOPYDB_SOURCE_PGURI" "$(cat <<'PF_EXTENSION_SOURCE'
` + sourceQuery + `
PF_EXTENSION_SOURCE
)" "${PREFLIGHT_EXTENSION_FILTERS:-}" 2>/dev/null`
	collect := ""
	selectionSide := string(conn.Source)
	if allDatabases {
		selectionSide = string(conn.Target)
		stopOnFailure = preflightStopOnFailure
		collect = strings.ReplaceAll(allDatabaseExtensionsBlock, "@SOURCE@", sourceExtensionsQuery) + stopOnFailure
		selection = `[ "$extensions_collected" = 1 ] && checkv "$PGCOPYDB_TARGET_PGURI" "SELECT COALESCE(jsonb_agg(DISTINCT name), '[]'::jsonb) FROM jsonb_array_elements(:'list'::jsonb) AS db(extensions), jsonb_array_elements(db.extensions) AS ext(name)" "[$extension_arrays]" 2>/dev/null`
	}
	return strings.NewReplacer("@SOURCE@", sourceQuery, "@VALIDATE@", extensionNamesValidQuery,
		"@COLLECT@", collect, "@SELECT@", selection, "@SELECTION_SIDE@", selectionSide, "@STOP_ON_FAILURE@", stopOnFailure,
		"@OK@", ok, "@MISSING@", missing, "@SELECTION_CHECK@", selectionCheck, "@TARGET_CHECK@", targetCheck,
		"@TARGET@", targetQuery).Replace(`extension_names_valid() {
  [ -n "$1" ] || return 1
  extension_shape=$(checkv "$PGCOPYDB_TARGET_PGURI" "$(cat <<'PF_EXTENSION_VALIDATE'
@VALIDATE@
PF_EXTENSION_VALIDATE
)" "$1" 2>/dev/null) || return 1
  case "$extension_shape" in ''|*[!0-9]*) return 1 ;; esac
}
@COLLECT@
if selected_extensions=$(@SELECT@) && extension_names_valid "$selected_extensions"; then
  if extension_issues=$(checkv "$PGCOPYDB_TARGET_PGURI" "$(cat <<'PF_EXTENSION_TARGET'
@TARGET@
PF_EXTENSION_TARGET
)" "$selected_extensions" 2>/dev/null) && extension_names_valid "$extension_issues"; then
    if [ "$extension_shape" = 0 ]; then
      echo "ok: @OK@"
    else
      note "preflight: @MISSING@"
    fi
  else
    note "preflight: target @TARGET_CHECK@ probe failed"
  fi
else
  note "preflight: @SELECTION_SIDE@ @SELECTION_CHECK@ probe failed"
fi
@STOP_ON_FAILURE@
`)
}

// JSON rows preserve whitespace and keep database names out of executable input.
// The conninfo value quotes libpq metacharacters and reuses the original credentials and TLS options.
const allDatabaseExtensionsBlock = `checkv_db() {
  { cat <<'PF_CONNECT'
SELECT 'dbname=' || chr(39) || replace(replace(:'database'::jsonb #>> '{}', chr(92), chr(92)||chr(92)), chr(39), chr(92)||chr(39)) || chr(39) AS dbconn \gset
\connect -reuse-previous=on :dbconn
PF_CONNECT
    printf '%s' "$2"
  } | psql "$1" -XAtq -v ON_ERROR_STOP=1 -v list="$3" -v database="$4" -f -
}
extension_arrays=''
extensions_collected=0
if [ -n "$source_databases" ] && database_rows=$(checkv "$PGCOPYDB_SOURCE_PGURI" "SELECT value::text FROM jsonb_array_elements(:'list'::jsonb)" "$source_databases") && [ -n "$database_rows" ]; then
  extensions_collected=1
  while IFS= read -r database; do
    [ -n "$database" ] || continue
    if ! db_extensions=$(checkv_db "$PGCOPYDB_SOURCE_PGURI" "$(cat <<'PF_EXTENSION_SOURCE'
@SOURCE@
PF_EXTENSION_SOURCE
)" "${PREFLIGHT_EXTENSION_FILTERS:-}" "$database" 2>/dev/null); then
      note "preflight: source extension selection probe failed for database $database"
      extensions_collected=0
    elif ! extension_names_valid "$db_extensions"; then
      note "preflight: validating selected extensions on target failed for database $database"
      extensions_collected=0
    else
      extension_arrays="${extension_arrays}${extension_arrays:+,}$db_extensions"
    fi
  done <<PF_DATABASES
$database_rows
PF_DATABASES
else
  note "preflight: listing source databases for extension probes failed"
fi
`

func allDatabasesPreflightBlock() string {
	var b strings.Builder
	for _, side := range []string{"source", "target"} {
		reason := "role dump reads pg_authid and every database is dumped"
		if side == "target" {
			reason = "CREATE DATABASE, role restore and ALTER OWNER run across every database"
		}
		b.WriteString(remSingleBlock(remSingle{
			probe:   `check "$PGCOPYDB_` + strings.ToUpper(side) + `_PGURI" 'select rolsuper::int from pg_roles where rolname = current_user'`,
			ok:      "all-databases " + side + " superuser",
			missing: "preflight: all-databases " + side + " requires a superuser migration role (rolsuper): " + reason,
			onProbe: "preflight: probing the all-databases " + side + " superuser requirement failed",
		}))
	}
	b.WriteString(preflightStopOnFailure)
	b.WriteString(`source_databases=''
if source_databases=$(check "$PGCOPYDB_SOURCE_PGURI" "SELECT jsonb_agg(datname ORDER BY datname) FROM pg_database WHERE datname NOT IN ('template0', 'template1')") && [ -n "$source_databases" ] && [ "$source_databases" != '[]' ]; then
  echo "ok: all-databases source databases: $source_databases"
  if existing_databases=$(checkv "$PGCOPYDB_TARGET_PGURI" "SELECT COALESCE(jsonb_agg(datname ORDER BY datname), '[]'::jsonb) FROM pg_database WHERE datname IN (SELECT jsonb_array_elements_text(:'list'::jsonb))" "$source_databases") && [ -n "$existing_databases" ]; then
    echo "ok: all-databases existing target databases: $existing_databases"
  else
    note "preflight: listing existing target databases failed"
  fi
else
  source_databases=''
  note "preflight: listing source databases failed or found no databases"
fi
`)
	b.WriteString(preflightStopOnFailure)
	return b.String()
}

// preflightScriptFor assembles the per-Migration check script: connectivity
// first, optional superuser checks, extensions, then grant checks.
// Follow-only prerequisites remain outside the clone path.
func preflightScriptFor(m *v1beta1.Migration) string {
	var b strings.Builder
	b.WriteString(preflightHeader)
	if m.Spec.Clone.AllDatabases {
		b.WriteString(allDatabasesPreflightBlock())
		if !slices.Contains(m.Spec.Clone.Skip, v1beta1.SkipOption("extensions")) {
			// The migration role's required rolsuper already covers ownership in every database.
			b.WriteString(extensionPreflightBlock(true, true, false))
		}
		b.WriteString(preflightScriptFooter)
		return b.String()
	}
	superSrc := m.Spec.Source.SuperuserSecretRef != nil
	superTgt := m.Spec.Target.SuperuserSecretRef != nil
	if superSrc {
		b.WriteString(superVerifyBlock(conn.Source))
	}
	if superTgt {
		b.WriteString(superVerifyBlock(conn.Target))
	}
	if !slices.Contains(m.Spec.Clone.Skip, v1beta1.SkipOption("extensions")) {
		b.WriteString(extensionPreflightBlock(m.Spec.Clone.DropIfExists, false, false))
		if m.Spec.Clone.DropIfExists || (!m.Spec.Clone.NoComments && !slices.Contains(m.Spec.Clone.Skip, v1beta1.SkipOption("extensionComments"))) {
			b.WriteString(extensionPreflightBlock(m.Spec.Clone.DropIfExists, false, true))
		}
	}
	dbProps := !slices.Contains(m.Spec.Clone.Skip, v1beta1.SkipOption("dbProperties"))
	if reownRequested(m) {
		b.WriteString(ownerAfterRestoreBlock(superTgt))
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

// preflightWal2jsonNote is a note, not a check: wal2json registers no catalog
// entry, and the only positive probe would consume a source slot.
const preflightWal2jsonNote = `
echo "preflight: note: wal2json presence on the source cannot be verified from SQL (a logical decoding plugin registers no catalog entry); if it is not installed, the first attempt fails at slot creation with: could not access file \"wal2json\""`

// buildPreflightJob probes connectivity and the follow prerequisites once,
// before the first worker Job. backoffLimit 1 absorbs one transient blip; a
// deterministic check failure fails twice and is terminal.
func buildPreflightJob(m *v1beta1.Migration, runnerImage string) (*batchv1.Job, error) {
	// Superuser credentials ride only in this Job: every other Job stays at
	// the migration role's privileges. Super preludes run after the primary
	// ones (they derive their URI from the composed primary URI).
	var extras []*conn.Materialized
	for _, sc := range []struct {
		s conn.Side
		c *v1beta1.PostgresConnection
	}{{conn.Source, &m.Spec.Source}, {conn.Target, &m.Spec.Target}} {
		// The all-databases path is probe-only and never needs remediation credentials.
		if m.Spec.Clone.AllDatabases {
			continue
		}
		if mat := conn.MaterializeSuperuser(sc.s, sc.c); mat != nil {
			extras = append(extras, mat)
		}
	}
	job, err := scriptJob(m, runnerImage, preflightJobName(m), preflightScriptFor(m), extras...)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(m.Spec.Clone.Skip, v1beta1.SkipOption("extensions")) {
		filters := struct {
			Include []string `json:"include"`
			Exclude []string `json:"exclude"`
		}{Include: []string{}, Exclude: []string{}}
		if f := m.Spec.Clone.Filters; f != nil {
			filters.Include = append(filters.Include, f.IncludeOnlyExtensions...)
			filters.Exclude = append(filters.Exclude, f.ExcludeExtensions...)
		}
		// This struct contains only string slices, which json.Marshal cannot reject.
		encoded, _ := json.Marshal(filters)
		container := &job.Spec.Template.Spec.Containers[0]
		container.Env = append(container.Env, corev1.EnvVar{Name: extensionFiltersEnv, Value: string(encoded)})
	}
	if reownRequested(m) {
		c := &job.Spec.Template.Spec.Containers[0]
		c.Env = append(c.Env, corev1.EnvVar{Name: reownPreflightOwnerEnv, Value: m.Spec.Clone.OwnerAfterRestore})
	}
	// Bounds true wedges: hung checks and pods that never start. Slow pulls
	// surface via PreflightRunning long before 30 minutes, and the deadline
	// terminal-fails the Migration instead of looping in Validating.
	deadline := int64(1800)
	job.Spec.ActiveDeadlineSeconds = &deadline
	// The finished preflight is the remediation audit trail and the gate's
	// completion memory, so spec TTL must not garbage-collect it.
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
	// The schema filters travel the same way. includeOnlyTables narrows the
	// probe to its tables' schemas: the restore touches nothing else, and
	// demanding CREATE on bystander schemas fails specs that migrate fine.
	if f := m.Spec.Clone.Filters; f != nil {
		c := &job.Spec.Template.Spec.Containers[0]
		include := f.IncludeOnlySchemas
		if len(f.IncludeOnlyTables) > 0 {
			if derived, ok := probeSchemasFromTables(f.IncludeOnlyTables); ok {
				if len(include) > 0 {
					derived = slices.DeleteFunc(derived, func(s string) bool {
						return !slices.Contains(include, s)
					})
				}
				include = derived
			}
		}
		if len(include) > 0 {
			c.Env = append(c.Env, corev1.EnvVar{
				Name:  "PREFLIGHT_SCHEMA_INCLUDE",
				Value: strings.Join(include, "\n"),
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

// publicSchema is the schema an unqualified table name lands in.
const publicSchema = "public"

// probeSchemasFromTables maps includeOnlyTables entries to their schemas; an
// unqualified name means public. Regex and quoted entries report !ok so the
// caller keeps the wider list: over-probing fails closed, under-probing does not.
func probeSchemasFromTables(tables []string) ([]string, bool) {
	var out []string
	for _, t := range tables {
		if strings.HasPrefix(t, "~") || strings.Contains(t, `"`) {
			return nil, false
		}
		s := publicSchema
		if before, _, found := strings.Cut(t, "."); found {
			s = before
		}
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	slices.Sort(out)
	return out, true
}

// scriptJob reuses the worker pod shape to run a shell script instead of a
// pgcopydb argv: the prelude execs $0, which here is /bin/sh. Backoff is fixed
// at 1 because every caller runs a bounded, idempotent script.
func scriptJob(m *v1beta1.Migration, runnerImage, name, script string, extras ...*conn.Materialized) (*batchv1.Job, error) {
	job, err := jobSkeleton(m, runnerImage, name, []string{"-c", script}, "", 1, extras...)
	if err != nil {
		return nil, err
	}
	cmd := job.Spec.Template.Spec.Containers[0].Command
	cmd[len(cmd)-1] = shellPath
	addConnectTimeout(job)
	return job, nil
}

// addConnectTimeout bounds libpq connects for the operator's control Jobs,
// whose default is unlimited and can wedge a Job forever. The worker is exempt:
// its data-path handshakes under load must not race a 10s cap.
func addConnectTimeout(job *batchv1.Job) {
	c := &job.Spec.Template.Spec.Containers[0]
	c.Env = append(c.Env, corev1.EnvVar{Name: "PGCONNECT_TIMEOUT", Value: "10"})
}

// jobSkeleton builds the shared worker pod shape around the given argv. setup
// is shell the prelude runs after the passfile is assembled (conn.PreludeScript).
// extras are credential sets whose preludes run after the primary sides'.
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
	// Structured runner logs; pgcopydb.LastErrorLine parses them.
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
		// PGPASSFILE must live in the container spec, not only in the prelude
		// shell: commands the operator execs into the pod inherit the spec env
		// and fail password authentication without it. The prelude export stays.
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
						// The prelude assembles the passfile, runs setup, then
						// execs "$0" "$@": $0 is "pgcopydb" here, and scriptJob
						// swaps it for /bin/sh.
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
