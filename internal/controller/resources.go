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
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
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
)

func labels(m *v1alpha1.Migration) map[string]string {
	return map[string]string{
		labelManagedBy: managerName,
		labelMigration: m.Name,
	}
}

func workPVCName(m *v1alpha1.Migration) string   { return m.Name + "-work" }
func filtersCMName(m *v1alpha1.Migration) string { return m.Name + "-filters" }
func jobName(m *v1alpha1.Migration, attempt int32) string {
	return fmt.Sprintf("%s-run-%d", m.Name, attempt)
}
func cleanupJobName(m *v1alpha1.Migration) string   { return m.Name + "-cleanup" }
func verifyJobName(m *v1alpha1.Migration) string    { return m.Name + "-verify" }
func preflightJobName(m *v1alpha1.Migration) string { return m.Name + "-preflight" }

// buildWorkPVC returns the work-directory claim. It holds pgcopydb's catalogs
// and is the unit of resumability: it survives Job restarts and is only
// removed with the Migration itself (ownerReference garbage collection).
func buildWorkPVC(m *v1alpha1.Migration) *corev1.PersistentVolumeClaim {
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
func buildFiltersConfigMap(m *v1alpha1.Migration) *corev1.ConfigMap {
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
func buildJob(m *v1alpha1.Migration, runnerImage string, attempt int32) (*batchv1.Job, error) {
	// Attempt 1 restarts (wipes) the work dir: any state found there is
	// foreign. Attempt > 1 resumes from the catalogs; the snapshot of the
	// failed attempt is gone with its process, so --resume needs
	// --not-consistent (see docs/research/pgcopydb-cli.md).
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
func publicationDropGuard(m *v1alpha1.Migration, attempt int32) string {
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
func buildCleanupJob(m *v1alpha1.Migration, runnerImage string) (*batchv1.Job, error) {
	// Pass the migration's own slot and origin names explicitly: stream
	// cleanup defaults --origin to "pgcopydb", so a follow migration with a
	// generated per-migration origin would leave that origin behind on the
	// target (observed live: origins accumulate while slots are dropped).
	slot := effectiveSlotName(m)
	args := []string{"stream", "cleanup", "--dir", pgcopydb.WorkDir,
		"--slot-name", slot, "--origin", slot}
	return jobSkeleton(m, runnerImage, cleanupJobName(m), args, "", 2)
}

// buildVerifyJob checks, after the worker exited 0, that the target really
// applied everything up to endpos. Exit code 0 is not proof of a complete
// drain: a crash between endpos-set and drain-complete makes pgcopydb's
// --resume short-circuit on the receive-side "endpos previously reached" and
// exit 0 without replaying pending WAL (found live, silent data loss). The
// durable truth is the target's replication origin progress, compared against
// the endpos recorded in the work-dir sentinel.
func buildVerifyJob(m *v1alpha1.Migration, runnerImage string) (*batchv1.Job, error) {
	origin := effectiveSlotName(m)
	// The origin parks at the last applied COMMIT record, which can trail
	// endpos (the source WAL head at approval) by a few non-data records
	// even on a fully drained, quiesced source (observed live: 56 bytes).
	// The tolerance below one WAL page still catches both real loss modes
	// (they gap by the size of the skipped data, hundreds of KB and up);
	// a hypothetical lost transaction smaller than the tolerance is the
	// residual risk, and spec.verification.data is the airtight check.
	script := `set -eu
endpos=$(pgcopydb stream sentinel get --endpos --dir ` + pgcopydb.WorkDir + `)
progress=$(psql "$PGCOPYDB_TARGET_PGURI" -tAc "select coalesce(pg_replication_origin_progress('` + origin + `', true)::text, '0/0')")
echo "endpos=$endpos origin_progress=$progress"
ok=$(psql "$PGCOPYDB_TARGET_PGURI" -tAc "select (pg_wal_lsn_diff('$endpos'::pg_lsn, '$progress'::pg_lsn) <= 8192)::int")
[ "$ok" = "1" ]`
	// scriptJob keeps the worker pod's passfile prelude: running this under
	// bare /bin/sh once shipped verification that failed auth and falsely
	// refuted every password-based drain (found live).
	return scriptJob(m, runnerImage, verifyJobName(m), script, 1)
}

// preflightScript checks every follow prerequisite the operator can probe via
// psql before the first worker runs; each failed check prints one line naming
// the exact GRANT or setting that fixes it. All four live loss/failure modes
// of 2026-08-07 (see MILESTONES.md) trip one of these checks. The
// session_replication_role probe is the silent-loss gate: without that SET,
// pgcopydb 0.18 applies nothing while reporting success (see
// docs/reference/prerequisites.md).
// The origin-function list is exactly what pgcopydb's setup/apply/cleanup and
// the operator's own verify Job execute on the target. The closing
// replica-identity audit lists tables where pgoutput would reject UPDATE and
// DELETE at write time on the source (relreplident 'n', or 'd' without a
// primary key); it deliberately audits all user tables, filters or not,
// because a table can be filtered out yet still take writes. Offenders in
// spec.follow.allowMissingReplicaIdentity (or all of them, with "*")
// downgrade to a warning line.
const preflightScript = `set -u
check() { psql "$1" -XAtq -v ON_ERROR_STOP=1 -c "$2"; }
if ! check "$PGCOPYDB_SOURCE_PGURI" 'select 1' >/dev/null; then
  echo "preflight: cannot connect to the source database"; exit 1
fi
if ! check "$PGCOPYDB_TARGET_PGURI" 'select 1' >/dev/null; then
  echo "preflight: cannot connect to the target database"; exit 1
fi
fail=0
wal_level=$(check "$PGCOPYDB_SOURCE_PGURI" 'show wal_level')
if [ "$wal_level" != logical ]; then
  echo "preflight: source wal_level is '$wal_level', follow needs 'logical': set wal_level = logical on the source and restart it"
  fail=1
fi
free_slots=$(check "$PGCOPYDB_SOURCE_PGURI" "select current_setting('max_replication_slots')::int - count(*) from pg_replication_slots")
if [ "${free_slots:-0}" -lt 1 ]; then
  echo "preflight: no free replication slot on the source: raise max_replication_slots or drop an unused slot from pg_replication_slots"
  fail=1
fi
src_user=$(check "$PGCOPYDB_SOURCE_PGURI" 'select current_user')
if [ "$(check "$PGCOPYDB_SOURCE_PGURI" 'select (rolreplication or rolsuper)::int from pg_roles where rolname = current_user')" != 1 ]; then
  echo "preflight: source role \"$src_user\" lacks the REPLICATION attribute: ALTER ROLE \"$src_user\" REPLICATION"
  fail=1
fi
origin_grants=$(check "$PGCOPYDB_TARGET_PGURI" "select string_agg(format('GRANT EXECUTE ON FUNCTION %s TO %I;', p.oid::regprocedure, current_user), ' ') from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'pg_catalog' and p.proname in ('pg_replication_origin_oid', 'pg_replication_origin_create', 'pg_replication_origin_drop', 'pg_replication_origin_session_setup', 'pg_replication_origin_xact_setup', 'pg_replication_origin_advance', 'pg_replication_origin_progress') and not has_function_privilege(current_user, p.oid, 'execute')")
if [ -n "$origin_grants" ]; then
  echo "preflight: target role lacks EXECUTE on replication origin functions, run on the target: $origin_grants"
  fail=1
fi
tgt_user=$(check "$PGCOPYDB_TARGET_PGURI" 'select current_user')
if ! check "$PGCOPYDB_TARGET_PGURI" "begin; set session_replication_role = 'replica'; rollback;" >/dev/null; then
  echo "preflight: target role \"$tgt_user\" cannot SET session_replication_role, so pgcopydb would apply NOTHING while reporting success: GRANT SET ON PARAMETER session_replication_role TO \"$tgt_user\" (PostgreSQL 15+; older targets need a superuser role)"
  fail=1
fi
ri_offenders=$(check "$PGCOPYDB_SOURCE_PGURI" "select n.nspname || '.' || c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind = 'r' and c.relpersistence = 'p' and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' and (c.relreplident = 'n' or (c.relreplident = 'd' and not exists (select 1 from pg_index i where i.indrelid = c.oid and i.indisprimary))) order by 1")
if [ -n "$ri_offenders" ]; then
  printf '%s\n' "$ri_offenders" > /tmp/ri_offenders
  ri_allow="${PREFLIGHT_ALLOW_MISSING_RI:-}"
  ack_all=0
  printf '%s\n' "$ri_allow" | grep -Fxq '*' && ack_all=1
  while IFS= read -r tbl; do
    if [ "$ack_all" = 1 ] || printf '%s\n' "$ri_allow" | grep -Fxq "$tbl"; then
      echo "preflight: warning: acknowledged table $tbl has no usable replica identity; UPDATE and DELETE on it will fail on the source during the migration window"
    else
      echo "preflight: table $tbl has no replica identity usable for UPDATE/DELETE (the audit covers all user tables, including ones excluded by clone.filters): ALTER TABLE $tbl REPLICA IDENTITY USING INDEX <unique index>, or ALTER TABLE $tbl REPLICA IDENTITY FULL, or acknowledge it in spec.follow.allowMissingReplicaIdentity"
      fail=1
    fi
  done < /tmp/ri_offenders
fi`

// preflightScriptFooter closes the check script; conditional blocks (the
// wal2json note) slot in between.
const preflightScriptFooter = `
if [ "$fail" -eq 0 ]; then echo "preflight: all follow-mode checks passed"; fi
exit "$fail"`

// preflightWal2jsonNote is a note, not a check: wal2json ships as a bare
// shared library with no extension control file (upstream README: "does not
// need CREATE EXTENSION"), so neither pg_available_extensions nor
// pg_available_extension_versions ever lists it and there is no catalog to
// query. The only positive probe is creating a logical slot with the plugin,
// which consumes a slot on the source; too invasive for a preflight. The
// failure mode is caught on attempt 1 at slot creation instead.
const preflightWal2jsonNote = `
echo "preflight: note: wal2json presence on the source cannot be verified from SQL (a logical decoding plugin registers no catalog entry); if it is not installed, the first attempt fails at slot creation with: could not access file \"wal2json\""`

// buildPreflightJob probes the follow prerequisites once, before the first
// worker Job of a follow-enabled Migration. backoffLimit 1 absorbs a single
// transient blip (pod eviction, connection reset) without failing the
// Migration; a deterministic check failure fails twice and is terminal.
// The replica-identity allowlist travels as an env var, not script text: env
// values are never shell-evaluated, so table names need no quoting rules.
func buildPreflightJob(m *v1alpha1.Migration, runnerImage string) (*batchv1.Job, error) {
	script := preflightScript
	if f := m.Spec.Follow; f != nil && f.Plugin == v1alpha1.PluginWal2json {
		script += preflightWal2jsonNote
	}
	job, err := scriptJob(m, runnerImage, preflightJobName(m), script+preflightScriptFooter, 1)
	if err != nil {
		return nil, err
	}
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
	return job, nil
}

// scriptJob reuses the worker pod shape (env, mounts, passfile prelude) to
// run a shell script instead of a pgcopydb argv: the prelude execs $0, which
// here is /bin/sh -c <script> instead of pgcopydb.
func scriptJob(m *v1alpha1.Migration, runnerImage, name, script string, backoff int32) (*batchv1.Job, error) {
	job, err := jobSkeleton(m, runnerImage, name, []string{"-c", script}, "", backoff)
	if err != nil {
		return nil, err
	}
	cmd := job.Spec.Template.Spec.Containers[0].Command
	cmd[len(cmd)-1] = shellPath
	return job, nil
}

// jobSkeleton builds the shared worker pod shape around the given argv. setup
// is optional shell run by the prelude after the passfile is assembled and
// before pgcopydb starts (see conn.PreludeScript).
func jobSkeleton(m *v1alpha1.Migration, runnerImage, name string, args []string, setup string, backoff int32) (*batchv1.Job, error) {
	src, err := conn.Materialize(conn.Source, &m.Spec.Source)
	if err != nil {
		return nil, err
	}
	tgt, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		return nil, err
	}

	env := append(src.Env, tgt.Env...)
	// Structured runner logs for humans and future machine parsing.
	env = append(env, corev1.EnvVar{Name: "PGCOPYDB_LOG_JSON", Value: "on"})

	var passfiles []conn.Passfile
	for _, mat := range []*conn.Materialized{src, tgt} {
		if mat.Passfile != nil {
			passfiles = append(passfiles, *mat.Passfile)
		}
	}
	if len(passfiles) > 0 {
		// PGPASSFILE must live in the container spec, not only in the
		// prelude shell: commands the operator execs into the pod (sentinel
		// reads, the WAL-head query, endpos setting) inherit the spec env,
		// and without it they fail password authentication. Found live by
		// the follow e2e suite; the prelude's own export stays for pid 1.
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
	volumes = append(volumes, src.Volumes...)
	volumes = append(volumes, tgt.Volumes...)
	mounts = append(mounts, src.Mounts...)
	mounts = append(mounts, tgt.Mounts...)

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
			Name: "filters", MountPath: "/etc/pgcopydb/conf", ReadOnly: true,
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
						Name:  "pgcopydb",
						Image: image,
						// sh -c '<prelude>' pgcopydb <args...>: the prelude
						// assembles the passfile, runs setup, and execs
						// "$0" "$@", where $0 is "pgcopydb" (scriptJob swaps
						// it for /bin/sh) and $@ are the Args below.
						Command:      []string{shellPath, "-c", conn.PreludeScript(passfiles, setup), "pgcopydb"},
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
