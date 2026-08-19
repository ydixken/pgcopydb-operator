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
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// pgoutputPlugin keeps the plugin literal in one place (and goconst quiet).
const pgoutputPlugin = "pgoutput"

// passwordMigration is the canned inline-credentials spec the builder tests
// share; callers mutate follow/cutover as needed.
func passwordMigration() *v1beta1.Migration {
	return &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: v1beta1.MigrationSpec{
			Source: v1beta1.PostgresConnection{
				Host: "s", Database: "d", Username: "u",
				PasswordSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}, Key: testPasswordKey,
				},
			},
			Target: v1beta1.PostgresConnection{Host: "t", Database: "d", Username: "u"},
		},
	}
}

// TestBuildJob_PGPassfileInSpecEnv is the regression test for a live-found
// defect: PGPASSFILE exported only inside the prelude shell is invisible to
// commands the operator execs into the pod (sentinel reads, WAL-head query),
// which broke caught-up detection and cutover for password-based connections.
func TestBuildJob_PGPassfileInSpecEnv(t *testing.T) {
	job, err := buildJob(passwordMigration(), "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := envValue(job.Spec.Template.Spec.Containers[0].Env, "PGPASSFILE"); got != "/tmp/pgpass" {
		t.Fatalf("PGPASSFILE must be in the container spec env, got %q", got)
	}

	// uriSecretRef carries credentials in the DSN itself: no passfile, no env.
	uriOnly := &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "m2", Namespace: "ns"},
		Spec: v1beta1.MigrationSpec{
			Source: v1beta1.PostgresConnection{
				URISecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "dsn"}, Key: "uri",
				},
			},
			Target: v1beta1.PostgresConnection{
				URISecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "dsn2"}, Key: "uri",
				},
			},
		},
	}
	job, err = buildJob(uriOnly, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := envValue(job.Spec.Template.Spec.Containers[0].Env, "PGPASSFILE"); got != "" {
		t.Fatalf("PGPASSFILE must be absent without password secrets, got %q", got)
	}

	// secretRef sides assemble their passfile line in the prelude, so the
	// spec env must carry PGPASSFILE for exec'd commands here too.
	secretRef := &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "m3", Namespace: "ns"},
		Spec: v1beta1.MigrationSpec{
			Source: v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: "bundle"}},
			Target: v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: "bundle2"}},
		},
	}
	job, err = buildJob(secretRef, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if got := envValue(c.Env, "PGPASSFILE"); got != "/tmp/pgpass" {
		t.Fatalf("PGPASSFILE must be in the spec env for secretRef connections, got %q", got)
	}
	for _, want := range []string{"PGM_SOURCE_DB", "PGM_TARGET_DB", "PGCOPYDB_SOURCE_PGURI", "PGCOPYDB_TARGET_PGURI"} {
		if !strings.Contains(c.Command[2], want) {
			t.Fatalf("prelude misses %q:\n%s", want, c.Command[2])
		}
	}
}

// TestBuildVerifyJob_AuthAndPredicate covers the live-found verify-gate
// defects: the script must run behind the passfile prelude (bare /bin/sh
// failed auth and falsely refuted every drain), the fast path must tolerate
// the origin trailing endpos by non-data WAL records, and an origin gap above
// the tolerance must escalate to pgcopydb compare data instead of refusing
// (idle sources grow the gap with publication-filtered WAL, no loss).
func TestBuildVerifyJob_AuthAndPredicate(t *testing.T) {
	m := &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: v1beta1.MigrationSpec{
			Source: v1beta1.PostgresConnection{
				Host: "s", Database: "d", Username: "u",
				PasswordSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}, Key: testPasswordKey,
				},
			},
			Target: v1beta1.PostgresConnection{
				Host: "t", Database: "d", Username: "u",
				PasswordSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "sec2"}, Key: testPasswordKey,
				},
			},
			Follow: &v1beta1.FollowOptions{Enabled: true},
		},
	}
	job, err := buildVerifyJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	// Script Jobs keep the worker shape: sh -c <prelude> /bin/sh, with the
	// script handed to the exec'd shell as args.
	if len(c.Command) != 4 || c.Command[0] != "/bin/sh" || c.Command[1] != "-c" || c.Command[3] != "/bin/sh" {
		t.Fatalf("verify job must exec a shell through the prelude, got command %v", c.Command)
	}
	if !strings.Contains(c.Command[2], "PGPASSFILE") {
		t.Fatalf("verify job prelude must assemble the passfile:\n%s", c.Command[2])
	}
	if len(c.Args) != 2 || c.Args[0] != "-c" {
		t.Fatalf("verify job must pass the script as sh -c args, got %v", c.Args)
	}
	for _, want := range []string{
		// Fast path: origin progress within one WAL page of endpos.
		"pg_replication_origin_progress",
		`[ "$gap" -le 8192 ]`,
		// Diagnosis line: replay_lsn tells consumed-but-filtered apart from
		// never-consumed in the Job log; it does not gate.
		"--replay-lsn",
		// Content path: an origin gap above the tolerance is decided by
		// compare data, never refused on distance alone.
		"compare data --dir /work/pgcopydb",
	} {
		if !strings.Contains(c.Args[1], want) {
			t.Fatalf("verify script missing %q:\n%s", want, c.Args[1])
		}
	}
	// The refusal must come from the compare, not from the origin gap: the
	// gap check chooses between the fast pass and the escalation, so the
	// script's only failure exit follows the compare invocation.
	if !strings.HasSuffix(c.Args[1], "exit 1") ||
		strings.Count(c.Args[1], "exit 1") != 1 ||
		strings.Index(c.Args[1], "compare data") > strings.Index(c.Args[1], "exit 1") {
		t.Fatalf("verify script must refuse only after compare data:\n%s", c.Args[1])
	}
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

// TestPublicationDropGuard covers the retry-after-setup-crash guard: only a
// retry attempt of a follow migration with an auto-managed publication drops
// the leftover, and only ever pgcopydb's own (slot-named) publication.
func TestPublicationDropGuard(t *testing.T) {
	follow := func(pub, slot string) *v1beta1.Migration {
		m := passwordMigration()
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Publication: pub, SlotName: slot}
		return m
	}
	generated := pgcopydb.SlotName("ns", "m")

	cases := []struct {
		name    string
		m       *v1beta1.Migration
		attempt int32
		want    string // "" = no guard
	}{
		{"first attempt has no guard", follow("", ""), 1, ""},
		{"retry drops the auto-managed publication", follow("", ""), 2, `DROP PUBLICATION IF EXISTS "` + generated + `"`},
		{"retry honors an explicit slot name", follow("", "my_slot"), 2, `DROP PUBLICATION IF EXISTS "my_slot"`},
		{"user-provided publication is never touched", follow("userpub", ""), 2, ""},
		{"non-follow migration has no guard", passwordMigration(), 2, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guard := publicationDropGuard(tc.m, tc.attempt)
			if tc.want == "" {
				if guard != "" {
					t.Fatalf("unexpected guard: %q", guard)
				}
				return
			}
			if !strings.Contains(guard, tc.want) || !strings.Contains(guard, "PGCOPYDB_SOURCE_PGURI") {
				t.Fatalf("guard %q does not drop %q on the source", guard, tc.want)
			}
			// The guard must reach the Job's prelude, after the passfile
			// export and before the exec that hands over to pgcopydb.
			job, err := buildJob(tc.m, "img", tc.attempt)
			if err != nil {
				t.Fatal(err)
			}
			prelude := job.Spec.Template.Spec.Containers[0].Command[2]
			guardAt := strings.Index(prelude, guard)
			execAt := strings.Index(prelude, `exec "$0" "$@"`)
			exportAt := strings.Index(prelude, "export PGPASSFILE=")
			if guardAt < 0 || execAt < 0 || exportAt < 0 || exportAt >= guardAt || guardAt >= execAt {
				t.Fatalf("guard misplaced in prelude (export=%d guard=%d exec=%d):\n%s", exportAt, guardAt, execAt, prelude)
			}
		})
	}
}

// TestBuildPreflightJob pins the preflight Job shape: a shell script (with
// the passfile prelude intact) probing every follow prerequisite, one
// fix-me line per check.
func TestBuildPreflightJob(t *testing.T) {
	job, err := buildPreflightJob(passwordMigration(), "img")
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "m-preflight" {
		t.Fatalf("job name = %q", job.Name)
	}
	if *job.Spec.BackoffLimit != 1 {
		t.Fatalf("backoffLimit = %d, want 1 (one retry for transient blips)", *job.Spec.BackoffLimit)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if got := c.Command[len(c.Command)-1]; got != shellPath {
		t.Fatalf("script Job $0 = %q, want %s", got, shellPath)
	}
	if !strings.Contains(c.Command[2], "PGPASSFILE") {
		t.Fatal("preflight prelude lost the passfile assembly; psql cannot authenticate without it")
	}
	script := c.Args[1]
	for _, want := range []string{
		"wal_level",
		"max_replication_slots",
		"rolreplication",
		"ALTER ROLE",
		"has_function_privilege",
		"pg_replication_origin_xact_setup",
		"GRANT EXECUTE ON FUNCTION",
		"set session_replication_role = 'replica'",
		"GRANT SET ON PARAMETER session_replication_role",
		"relreplident",
		"REPLICA IDENTITY USING INDEX",
		"REPLICA IDENTITY FULL",
		"allowMissingReplicaIdentity",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("preflight script missing %q:\n%s", want, script)
		}
	}
}

// TestBuildPreflightJob_PluginAndAllowlist pins the wiring of the two
// preflight extensions: the wal2json note appears only when that plugin is
// selected (pgoutput and test_decoding ship with PostgreSQL, nothing to
// note), and the replica-identity allowlist travels as a newline-joined env
// var so table names never meet shell quoting.
func TestBuildPreflightJob_PluginAndAllowlist(t *testing.T) {
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	job, err := buildPreflightJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if strings.Contains(c.Args[1], "wal2json") {
		t.Fatalf("pgoutput preflight must not carry the wal2json note:\n%s", c.Args[1])
	}
	if got := envValue(c.Env, "PREFLIGHT_ALLOW_MISSING_RI"); got != "" {
		t.Fatalf("allowlist env must be absent without spec entries, got %q", got)
	}

	m.Spec.Follow.Plugin = v1beta1.PluginWal2json
	m.Spec.Follow.AllowMissingReplicaIdentity = []string{"public.audit_log", "stats.rollup"}
	job, err = buildPreflightJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	c = job.Spec.Template.Spec.Containers[0]
	if !strings.Contains(c.Args[1], `could not access file \"wal2json\"`) {
		t.Fatalf("wal2json preflight lost its note:\n%s", c.Args[1])
	}
	if got := envValue(c.Env, "PREFLIGHT_ALLOW_MISSING_RI"); got != "public.audit_log\nstats.rollup" {
		t.Fatalf("allowlist env = %q", got)
	}
}

// TestPreflightScript_ReplicaIdentityAudit executes the generated preflight
// script under /bin/sh with a stub psql, proving the audit verdict logic end
// to end: an offender fails the check, an allowlisted offender downgrades to
// a warning, and "*" acknowledges every offender. The stub answers each
// probe by matching the query text and serves the offender list from
// $PSQL_RI.
func TestPreflightScript_ReplicaIdentityAudit(t *testing.T) {
	dir := t.TempDir()
	stub := `#!/bin/sh
q="$6"
case "$q" in
  *relreplident*) printf '%s' "${PSQL_RI:-}" ;;
  *max_replication_slots*) echo 5 ;;
  *rolreplication*) echo 1 ;;
  *string_agg*) echo "" ;;
  *session_replication_role*) exit 0 ;;
  *wal_level*) echo logical ;;
  *current_user*) echo u ;;
  *) echo 1 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}

	run := func(t *testing.T, plugin, offenders string, allow []string) (string, int) {
		t.Helper()
		m := passwordMigration()
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: plugin, AllowMissingReplicaIdentity: allow}
		job, err := buildPreflightJob(m, "img")
		if err != nil {
			t.Fatal(err)
		}
		c := job.Spec.Template.Spec.Containers[0]
		cmd := exec.Command(shellPath, "-c", c.Args[1])
		cmd.Env = append(os.Environ(),
			"PATH="+dir+":"+os.Getenv("PATH"),
			"PGCOPYDB_SOURCE_PGURI=src",
			"PGCOPYDB_TARGET_PGURI=tgt",
			"PSQL_RI="+offenders,
			"PREFLIGHT_ALLOW_MISSING_RI="+envValue(c.Env, "PREFLIGHT_ALLOW_MISSING_RI"),
		)
		out, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		switch {
		case err == nil:
			return string(out), 0
		case errors.As(err, &exitErr):
			return string(out), exitErr.ExitCode()
		default:
			t.Fatalf("running preflight script: %v\n%s", err, out)
			return "", 0
		}
	}

	t.Run("no offenders passes", func(t *testing.T) {
		out, code := run(t, pgoutputPlugin, "", nil)
		if code != 0 || !strings.Contains(out, "all follow-mode checks passed") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
	t.Run("wal2json prints its note and still passes", func(t *testing.T) {
		out, code := run(t, "wal2json", "", nil)
		if code != 0 ||
			!strings.Contains(out, `slot creation with: could not access file "wal2json"`) ||
			!strings.Contains(out, "all follow-mode checks passed") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
	t.Run("offender fails, allowlisted sibling warns", func(t *testing.T) {
		out, code := run(t, pgoutputPlugin, "public.a\npublic.b", []string{"public.b"})
		if code != 1 {
			t.Fatalf("code=%d, want 1; out:\n%s", code, out)
		}
		if !strings.Contains(out, "preflight: table public.a has no replica identity usable for UPDATE/DELETE") ||
			!strings.Contains(out, "REPLICA IDENTITY USING INDEX") ||
			!strings.Contains(out, "REPLICA IDENTITY FULL") {
			t.Fatalf("missing offender line with both fixes:\n%s", out)
		}
		if !strings.Contains(out, "preflight: warning: acknowledged table public.b") {
			t.Fatalf("missing warning for the allowlisted table:\n%s", out)
		}
	})
	t.Run("fully allowlisted offender only warns", func(t *testing.T) {
		out, code := run(t, pgoutputPlugin, "public.a", []string{"public.a"})
		if code != 0 || !strings.Contains(out, "preflight: warning: acknowledged table public.a") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
	t.Run("star acknowledges all offenders", func(t *testing.T) {
		out, code := run(t, pgoutputPlugin, "public.a\npublic.b", []string{"*"})
		if code != 0 ||
			!strings.Contains(out, "preflight: warning: acknowledged table public.a") ||
			!strings.Contains(out, "preflight: warning: acknowledged table public.b") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
}

// TestBuildVerifyJob_KeepsPassfilePrelude is the regression test for the
// script-Job passfile gap: without the prelude, psql in the verify pod cannot
// authenticate against password-based targets.
func TestBuildVerifyJob_KeepsPassfilePrelude(t *testing.T) {
	job, err := buildVerifyJob(passwordMigration(), "img")
	if err != nil {
		t.Fatal(err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if !strings.Contains(c.Command[2], "PGPASSFILE") {
		t.Fatal("verify prelude lost the passfile assembly")
	}
	if got := c.Command[len(c.Command)-1]; got != shellPath {
		t.Fatalf("script Job $0 = %q, want %s", got, shellPath)
	}
	if !strings.Contains(c.Args[1], "pg_replication_origin_progress") {
		t.Fatalf("verify script lost the origin-progress check:\n%s", c.Args[1])
	}
}

// TestJobEnv_ConnectTimeout: every Job the operator builds carries a bounded
// libpq connect timeout; without one a black-holed endpoint hangs psql and
// pgcopydb forever instead of failing a check.
func TestJobEnv_ConnectTimeout(t *testing.T) {
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	builders := map[string]func() (*batchv1.Job, error){
		"worker":      func() (*batchv1.Job, error) { return buildJob(m, "img", 1) },
		"cleanup-job": func() (*batchv1.Job, error) { return buildCleanupJob(m, "img") },
		"verify":      func() (*batchv1.Job, error) { return buildVerifyJob(m, "img") },
		"preflight":   func() (*batchv1.Job, error) { return buildPreflightJob(m, "img") },
		"compare":     func() (*batchv1.Job, error) { return buildCompareJob(m, "img", compareSchema) },
	}
	for name, build := range builders {
		job, err := build()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := envValue(job.Spec.Template.Spec.Containers[0].Env, "PGCONNECT_TIMEOUT"); got != "10" {
			t.Fatalf("%s: PGCONNECT_TIMEOUT = %q, want \"10\"", name, got)
		}
	}
}

// TestPreflightJob_ActiveDeadline: the deadline is what turns a preflight pod
// that can never start into a terminal failure; the worker must not have one,
// a long clone is not a hang.
func TestPreflightJob_ActiveDeadline(t *testing.T) {
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	pf, err := buildPreflightJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	if pf.Spec.ActiveDeadlineSeconds == nil || *pf.Spec.ActiveDeadlineSeconds != 600 {
		t.Fatalf("preflight ActiveDeadlineSeconds = %v, want 600", pf.Spec.ActiveDeadlineSeconds)
	}
	worker, err := buildJob(m, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	if worker.Spec.ActiveDeadlineSeconds != nil {
		t.Fatalf("worker must not carry a deadline, got %v", *worker.Spec.ActiveDeadlineSeconds)
	}
}
