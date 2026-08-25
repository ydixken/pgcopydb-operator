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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// pgoutputPlugin keeps the plugin literal in one place (and goconst quiet).
const pgoutputPlugin = "pgoutput"

// Schema fixtures for the clone-rights filter tests, hoisted for goconst.
const (
	testSchemaInc = "sales"
	testSchemaExc = "scratch"
)

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
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	job, err := buildPreflightJob(m, "img")
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
for a in "$@"; do [ "$a" = "-f" ] && q=$(cat); done
case "$q" in
  *relreplident*) printf '%s' "${PSQL_RI:-}" ;;
  *has_schema_privilege*) echo "" ;;
  *has_database_privilege*) echo 1 ;;
  *datdba*) echo 1 ;;
  *max_replication_slots*) echo 5 ;;
  *rolreplication*) echo 1 ;;
  *string_agg*) echo "" ;;
  *pg_namespace*) echo public ;;
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
		if code != 0 || !strings.Contains(out, "all checks passed") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
	t.Run("wal2json prints its note and still passes", func(t *testing.T) {
		out, code := run(t, "wal2json", "", nil)
		if code != 0 ||
			!strings.Contains(out, `slot creation with: could not access file "wal2json"`) ||
			!strings.Contains(out, "all checks passed") {
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

// TestJobEnv_ConnectTimeout: the operator's control Jobs carry a bounded
// libpq connect timeout (a black-holed endpoint must fail a check, not hang
// it), while the worker stays exempt: pgcopydb's data-path handshakes under
// load must not race a 10s cap.
func TestJobEnv_ConnectTimeout(t *testing.T) {
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	builders := map[string]func() (*batchv1.Job, error){
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
	worker, err := buildJob(m, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := envValue(worker.Spec.Template.Spec.Containers[0].Env, "PGCONNECT_TIMEOUT"); got != "" {
		t.Fatalf("worker must not carry PGCONNECT_TIMEOUT, got %q", got)
	}
}

// TestPreflightJob_ActiveDeadline: the deadline is what turns a preflight pod
// that can never start into a terminal failure; the worker must not have one,
// a long clone is not a hang. The preflight also sheds the spec TTL: it is
// the remediation audit trail and the gate's completion memory.
func TestPreflightJob_ActiveDeadline(t *testing.T) {
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	ttl := int32(0)
	m.Spec.TTLSecondsAfterFinished = &ttl
	pf, err := buildPreflightJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	if pf.Spec.ActiveDeadlineSeconds == nil || *pf.Spec.ActiveDeadlineSeconds != 1800 {
		t.Fatalf("preflight ActiveDeadlineSeconds = %v, want 1800", pf.Spec.ActiveDeadlineSeconds)
	}
	if pf.Spec.TTLSecondsAfterFinished != nil {
		t.Fatalf("preflight must shed the spec TTL, got %v", *pf.Spec.TTLSecondsAfterFinished)
	}
	worker, err := buildJob(m, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	if worker.Spec.ActiveDeadlineSeconds != nil {
		t.Fatalf("worker must not carry a deadline, got %v", *worker.Spec.ActiveDeadlineSeconds)
	}
	if worker.Spec.TTLSecondsAfterFinished == nil {
		t.Fatal("worker keeps the spec TTL")
	}
}

// superMigration is passwordMigration with follow and superuser refs on both
// sides; subtests strip what they do not need.
func superMigration() *v1beta1.Migration {
	m := passwordMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	m.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "pf-src-admin"}
	m.Spec.Target.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "pf-tgt-admin"}
	return m
}

// mustPrecede fails unless a appears before b in s; the preflight ordering
// contract (connectivity first, then superuser verify, then the battery) is
// user-visible log output, so it is pinned.
func mustPrecede(t *testing.T, s, a, b string) {
	t.Helper()
	ia, ib := strings.Index(s, a), strings.Index(s, b)
	if ia < 0 || ib < 0 || ia >= ib {
		t.Fatalf("want %q before %q (idx %d, %d):\n%s", a, b, ia, ib, s)
	}
}

// TestPreflightScriptFor_Structure pins which blocks each Migration shape
// gets, and their order.
func TestPreflightScriptFor_Structure(t *testing.T) {
	t.Run("clone-only gets connectivity and nothing else", func(t *testing.T) {
		s := preflightScriptFor(passwordMigration())
		for _, want := range []string{`echo "ok: connectivity source"`, `echo "ok: connectivity target"`, "all checks passed"} {
			if !strings.Contains(s, want) {
				t.Fatalf("missing %q:\n%s", want, s)
			}
		}
		for _, absent := range []string{"wal_level", "rolreplication", "relreplident", "SUPER_PGURI"} {
			if strings.Contains(s, absent) {
				t.Fatalf("clone-only script must not contain %q:\n%s", absent, s)
			}
		}
	})
	t.Run("follow without super hints instead of remediating", func(t *testing.T) {
		m := superMigration()
		m.Spec.Source.SuperuserSecretRef = nil
		m.Spec.Target.SuperuserSecretRef = nil
		s := preflightScriptFor(m)
		// Three follow hints plus the clone tier's two (database and schema
		// CREATE); db-properties is not superuser-remediable and hints none.
		if got := strings.Count(s, "hint: spec."); got != 5 {
			t.Fatalf("want 5 hint lines, got %d:\n%s", got, s)
		}
		for _, want := range []string{
			"hint: spec.source.superuserSecretRef lets the operator apply this itself",
			"hint: spec.target.superuserSecretRef lets the operator apply this itself",
		} {
			if !strings.Contains(s, want) {
				t.Fatalf("missing %q", want)
			}
		}
		if strings.Contains(s, "remediated:") || strings.Contains(s, "SUPER_PGURI") {
			t.Fatalf("no-super script must not remediate:\n%s", s)
		}
	})
	t.Run("super on both sides remediates without hints", func(t *testing.T) {
		s := preflightScriptFor(superMigration())
		if strings.Contains(s, "hint: spec.") {
			t.Fatalf("hints must vanish when super is configured:\n%s", s)
		}
		for _, want := range []string{"$PGM_SOURCE_SUPER_PGURI", "$PGM_TARGET_SUPER_PGURI", `remediated: $stmt`, `format('ALTER ROLE`, "GRANT SET ON PARAMETER session_replication_role"} {
			if !strings.Contains(s, want) {
				t.Fatalf("missing %q:\n%s", want, s)
			}
		}
		mustPrecede(t, s, `echo "ok: connectivity source"`, `echo "ok: superuser source connected"`)
		mustPrecede(t, s, `echo "ok: superuser source verified"`, `echo "ok: superuser target connected"`)
		mustPrecede(t, s, `echo "ok: superuser target verified"`, "wal_level")
	})
	t.Run("super on one side mixes remediation and hint", func(t *testing.T) {
		m := superMigration()
		m.Spec.Target.SuperuserSecretRef = nil
		s := preflightScriptFor(m)
		if strings.Contains(s, "hint: spec.source.superuserSecretRef") {
			t.Fatalf("source has super, its hint must vanish:\n%s", s)
		}
		if !strings.Contains(s, "hint: spec.target.superuserSecretRef") {
			t.Fatalf("target has no super, its checks keep the hint:\n%s", s)
		}
		if !strings.Contains(s, "$PGM_SOURCE_SUPER_PGURI") || strings.Contains(s, "$PGM_TARGET_SUPER_PGURI") {
			t.Fatalf("only the source may remediate:\n%s", s)
		}
	})
}

// TestBuildPreflightJob_SuperuserWiring pins that superuser credentials ride
// only in the preflight Job: env refs, PW volume, and the super prelude after
// the primary one.
func TestBuildPreflightJob_SuperuserWiring(t *testing.T) {
	m := superMigration()
	m.Spec.Source = v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: "conn-sec"}}
	m.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "pf-src-admin"}
	job, err := buildPreflightJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	for _, name := range []string{"PGM_SOURCE_SUPER_USER", "PGM_SOURCE_SUPER_HOST", "PGM_TARGET_SUPER_USER", "PGM_TARGET_SUPER_HOST"} {
		found := false
		for _, e := range c.Env {
			if e.Name == name {
				found = e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil
			}
		}
		if !found {
			t.Fatalf("preflight env missing secret ref %s", name)
		}
	}
	volumes := map[string]bool{}
	for _, v := range job.Spec.Template.Spec.Volumes {
		volumes[v.Name] = true
	}
	if !volumes["source-super-password"] || !volumes["target-super-password"] {
		t.Fatalf("super PW volumes missing, have %v", volumes)
	}
	// The super prelude derives its URI from the primary's, so it must run
	// after the primary secretRef snippet.
	mustPrecede(t, c.Command[2], "connection secret", "superuser secret")

	// The worker Job never carries the super credentials.
	worker, err := buildJob(m, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range worker.Spec.Template.Spec.Containers[0].Env {
		if strings.Contains(e.Name, "SUPER") {
			t.Fatalf("worker env leaked %s", e.Name)
		}
	}
}

// TestPreflightScript_Remediation executes the generated script under /bin/sh
// with a stateful psql stub: checks fail until the corresponding grant is
// applied through the superuser URI, exactly like a live database would.
func TestPreflightScript_Remediation(t *testing.T) {
	dir := t.TempDir()
	// Compose cases (format(...)) precede the apply cases: the script now
	// builds role-bearing statements server-side, and the stub emulates the
	// server's identifier escaping so the injection subtest is faithful.
	stub := `#!/bin/sh
uri="$1"; q="$6"
for a in "$@"; do [ "$a" = "-f" ] && q=$(cat); done
role="${ROLE_NAME:-app}"
qrole=$(printf '%s' "$role" | sed 's/"/""/g')
apply() {
  [ "${REMEDY_MODE:-}" = reject ] && exit 1
  case "$uri" in src-super|tgt-super) ;; *) exit 1 ;; esac
  printf '%s' "$q" > "$STATE/applied-$1"
  [ "${REMEDY_MODE:-}" = nostick ] || : > "$STATE/$1"
  exit 0
}
case "$q" in
  'select 1')
    n=$(cat "$STATE/conn-$uri" 2>/dev/null || echo 0)
    n=$((n+1)); printf '%s' "$n" > "$STATE/conn-$uri"
    [ "$n" -gt "${CONN_FAIL_N:-0}" ] || exit 1
    exit 0 ;;
  *has_schema_privilege*) printf '%s' "${PSQL_SCHEMA_GRANTS:-}" ;;
  *has_database_privilege*) echo "${PSQL_DB_CREATE:-1}" ;;
  *datdba*) echo "${PSQL_DBPROPS:-1}" ;;
  *format*ALTER*) echo "ALTER ROLE \"$qrole\" REPLICATION" ;;
  *format*GRANT\ SET*) echo "GRANT SET ON PARAMETER session_replication_role TO \"$qrole\"" ;;
  *rolreplication*) if [ -f "$STATE/repl" ]; then echo 1; else echo 0; fi ;;
  *"ALTER ROLE"*) apply repl ;;
  *string_agg*) if [ -f "$STATE/origin" ]; then echo ""; else printf '%s' "GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_oid(text) TO \"app\"; GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_progress(text,boolean) TO \"app\";"; fi ;;
  *"GRANT EXECUTE"*) apply origin ;;
  *"GRANT SET ON PARAMETER"*) apply srr ;;
  *session_replication_role*) if [ -f "$STATE/srr" ]; then exit 0; else exit 1; fi ;;
  *rolsuper*) echo "${ROLSUPER:-1}" ;;
  *wal_level*) echo logical ;;
  *max_replication_slots*) echo 5 ;;
  *relreplident*) printf '' ;;
  *pg_namespace*) printf '%s' "${PSQL_SCHEMAS:-public}" ;;
  *current_user*) echo "$role" ;;
  *) echo 1 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}

	run := func(t *testing.T, script, mode string, extra ...string) (string, int, string) {
		t.Helper()
		state := t.TempDir()
		cmd := exec.Command(shellPath, "-c", script)
		cmd.Env = append(os.Environ(),
			"PATH="+dir+":"+os.Getenv("PATH"),
			"PGCOPYDB_SOURCE_PGURI=src",
			"PGCOPYDB_TARGET_PGURI=tgt",
			"PGM_SOURCE_SUPER_PGURI=src-super",
			"PGM_TARGET_SUPER_PGURI=tgt-super",
			"STATE="+state,
			"REMEDY_MODE="+mode,
			"PREFLIGHT_RETRY_SLEEP=0",
		)
		cmd.Env = append(cmd.Env, extra...)
		out, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		switch {
		case err == nil:
			return string(out), 0, state
		case errors.As(err, &exitErr):
			return string(out), exitErr.ExitCode(), state
		default:
			t.Fatalf("running preflight script: %v\n%s", err, out)
			return "", 0, state
		}
	}

	t.Run("super remediates all three and passes", func(t *testing.T) {
		out, code, _ := run(t, preflightScriptFor(superMigration()), "")
		if code != 0 {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		for _, want := range []string{
			`remediated: ALTER ROLE "app" REPLICATION`,
			`remediated: GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_oid(text) TO "app";`,
			`remediated: GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_progress(text,boolean) TO "app";`,
			`remediated: GRANT SET ON PARAMETER session_replication_role TO "app"`,
			"ok: source replication attribute",
			"ok: target origin function grants",
			"ok: target session_replication_role",
			"all checks passed",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing %q:\n%s", want, out)
			}
		}
		mustPrecede(t, out, "ok: connectivity source", "ok: superuser source connected")
		mustPrecede(t, out, "ok: superuser target verified", "ok: source wal_level logical")
	})
	t.Run("no super fails with hints", func(t *testing.T) {
		m := superMigration()
		m.Spec.Source.SuperuserSecretRef = nil
		m.Spec.Target.SuperuserSecretRef = nil
		out, code, _ := run(t, preflightScriptFor(m), "")
		if code != 1 {
			t.Fatalf("code=%d, want 1; out:\n%s", code, out)
		}
		if got := strings.Count(out, "hint: spec."); got != 3 {
			t.Fatalf("want 3 hint lines, got %d:\n%s", got, out)
		}
		if strings.Contains(out, "remediated:") {
			t.Fatalf("nothing may be remediated without super:\n%s", out)
		}
		// The failure summary re-prints the failures with the hints last, so
		// the log tail the condition carries always ends with the pointers.
		mustPrecede(t, out, "preflight failed:", "hint: spec.target.superuserSecretRef")
	})
	t.Run("failed apply fails by name", func(t *testing.T) {
		out, code, _ := run(t, preflightScriptFor(superMigration()), "reject")
		if code != 1 {
			t.Fatalf("code=%d, want 1; out:\n%s", code, out)
		}
		if !strings.Contains(out, "via superuserSecretRef failed") {
			t.Fatalf("missing apply-failure line:\n%s", out)
		}
	})
	t.Run("remediation that does not stick fails the re-check", func(t *testing.T) {
		out, code, _ := run(t, preflightScriptFor(superMigration()), "nostick")
		if code != 1 {
			t.Fatalf("code=%d, want 1; out:\n%s", code, out)
		}
		for _, want := range []string{
			"still lacks the REPLICATION attribute after remediation",
			"after remediation, run on the target",
			"still cannot SET session_replication_role after remediation",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing %q:\n%s", want, out)
			}
		}
	})
	t.Run("quote-bearing role name stays one identifier", func(t *testing.T) {
		out, code, state := run(t, preflightScriptFor(superMigration()), "",
			`ROLE_NAME=we"ird`)
		if code != 0 {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		// The server-composed statement escapes the quote; the apply must
		// receive it verbatim, one identifier, nothing smuggled behind it.
		want := `ALTER ROLE "we""ird" REPLICATION`
		if !strings.Contains(out, "remediated: "+want) {
			t.Fatalf("missing remediated statement %q:\n%s", want, out)
		}
		applied, err := os.ReadFile(filepath.Join(state, "applied-repl"))
		if err != nil {
			t.Fatal(err)
		}
		if string(applied) != want {
			t.Fatalf("applied %q, want %q", applied, want)
		}
		if !strings.Contains(out, `remediated: GRANT SET ON PARAMETER session_replication_role TO "we""ird"`) {
			t.Fatalf("missing composed GRANT SET:\n%s", out)
		}
	})
	t.Run("not a superuser downgrades to a warning", func(t *testing.T) {
		out, code, _ := run(t, preflightScriptFor(superMigration()), "", "ROLSUPER=0")
		if code != 0 {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if !strings.Contains(out, "warn: source superuserSecretRef user lacks rolsuper; attempting remediation anyway") {
			t.Fatalf("missing rolsuper warning:\n%s", out)
		}
		if strings.Contains(out, "ok: superuser source verified") {
			t.Fatalf("verified line must vanish when rolsuper is false:\n%s", out)
		}
	})
	t.Run("connectivity retries then succeeds", func(t *testing.T) {
		out, code, _ := run(t, preflightScriptFor(passwordMigration()), "", "CONN_FAIL_N=2")
		if code != 0 {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		for _, want := range []string{
			"retry: source connectivity attempt 1 failed",
			"retry: source connectivity attempt 2 failed",
			"ok: connectivity source",
			"ok: connectivity target",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing %q:\n%s", want, out)
			}
		}
	})
	t.Run("connectivity exhausts six attempts and fails by name", func(t *testing.T) {
		out, code, _ := run(t, preflightScriptFor(passwordMigration()), "", "CONN_FAIL_N=99")
		if code != 1 {
			t.Fatalf("code=%d, want 1; out:\n%s", code, out)
		}
		if !strings.Contains(out, "preflight: cannot connect to the source database") {
			t.Fatalf("missing named failure:\n%s", out)
		}
		if got := strings.Count(out, "retry: source connectivity attempt"); got != 5 {
			t.Fatalf("want 5 retry lines before the sixth probe fails, got %d:\n%s", got, out)
		}
	})
}

// TestEmitPreflightOutcome pins the log contract: ok/remediated prefixes
// become the event trail, everything else is ignored. All statements ride in
// ONE PreflightRemediated event: the events recorder correlates by reason and
// collapses same-reason events into a counter that keeps only the first
// message, which is exactly how the rc.1 gate lost the grant audit trail.
func TestEmitPreflightOutcome(t *testing.T) {
	// Statement shapes as the server really composes them: %I leaves the
	// unremarkable role name unquoted and regprocedure drops pg_catalog.
	stmts := []string{
		`GRANT CREATE ON DATABASE app TO limited`,
		`GRANT CREATE ON SCHEMA public TO limited`,
		`ALTER ROLE "app" REPLICATION`,
		`GRANT EXECUTE ON FUNCTION pg_replication_origin_oid(text) TO app;`,
		`GRANT EXECUTE ON FUNCTION pg_replication_origin_progress(text,boolean) TO app;`,
		`GRANT SET ON PARAMETER session_replication_role TO "app"`,
	}
	log := "ok: connectivity source\nok: connectivity target\n" +
		"remediated: " + strings.Join(stmts, "\nremediated: ") + "\n" +
		"ok: clone rights database\nok: clone rights schemas\n" +
		"ok: source replication attribute\n" +
		"preflight: warning: acknowledged table public.a has no usable replica identity\n" +
		"preflight: all checks passed\n"
	r := &MigrationReconciler{
		Recorder: events.NewFakeRecorder(20),
		Logs:     &fakeLogs{out: log},
	}
	r.emitPreflightOutcome(context.Background(), passwordMigration())
	rec := r.Recorder.(*events.FakeRecorder)
	var got []string
	for {
		select {
		case e := <-rec.Events:
			got = append(got, e)
		default:
			if len(got) != 2 {
				t.Fatalf("want exactly 2 events (bundled remediation + summary), got %v", got)
			}
			if !strings.Contains(got[0], "PreflightRemediated") {
				t.Fatalf("first event must be the remediation bundle: %s", got[0])
			}
			for _, s := range stmts {
				if !strings.Contains(got[0], s) {
					t.Fatalf("bundle lost statement %q: %s", s, got[0])
				}
			}
			if !strings.Contains(got[1], "PreflightPassed") || !strings.Contains(got[1], "5 checks passed, 6 grants applied") {
				t.Fatalf("summary event wrong: %s", got[1])
			}
			return
		}
	}
}

// TestEmitPreflightOutcome_LongAudit: remediated lines printed before a long
// replica-identity audit must still surface as events, which requires the
// parse to read effectively the whole log, not a short tail.
func TestEmitPreflightOutcome_LongAudit(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`remediated: ALTER ROLE "app" REPLICATION` + "\n")
	sb.WriteString(`remediated: GRANT EXECUTE ON FUNCTION pg_replication_origin_oid(text) TO app;` + "\n")
	sb.WriteString("ok: source replication attribute\n")
	for i := range 200 {
		fmt.Fprintf(&sb, "preflight: warning: acknowledged table public.t%d has no usable replica identity\n", i)
	}
	sb.WriteString("preflight: all checks passed\n")
	logs := &fakeLogs{out: sb.String()}
	r := &MigrationReconciler{Recorder: events.NewFakeRecorder(20), Logs: logs}
	r.emitPreflightOutcome(context.Background(), passwordMigration())
	if logs.gotLines < 10000 {
		t.Fatalf("success-path parse asked for %d lines, want the whole log (>= 10000)", logs.gotLines)
	}
	rec := r.Recorder.(*events.FakeRecorder)
	var got []string
	for {
		select {
		case e := <-rec.Events:
			got = append(got, e)
		default:
			if len(got) != 2 || !strings.Contains(got[0], "PreflightRemediated") ||
				!strings.Contains(got[0], "GRANT EXECUTE ON FUNCTION") {
				t.Fatalf("remediation bundle lost under the audit: %v", got)
			}
			return
		}
	}
}

// TestReconcileSuspended_PreflightErrors covers the two error legs of the
// gate-stage preflight deletion; envtest cannot inject either failure.
func TestReconcileSuspended_PreflightErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	m := passwordMigration()
	pf := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: preflightJobName(m), Namespace: m.Namespace}}
	boom := errors.New("boom")

	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(m, pf).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return boom
			},
		}).Build()
	r := &MigrationReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	if _, err := r.reconcileSuspended(context.Background(), m, m.DeepCopy()); !errors.Is(err, boom) {
		t.Fatalf("a preflight Delete failure must propagate, got %v", err)
	}

	c = clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(m).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == preflightJobName(m) {
					return boom
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).Build()
	r = &MigrationReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	if _, err := r.reconcileSuspended(context.Background(), m, m.DeepCopy()); !errors.Is(err, boom) {
		t.Fatalf("a non-NotFound preflight Get failure must propagate, got %v", err)
	}
}

// TestBuilders_InvalidConnection: cleanup and compare route through
// jobSkeleton too, so a connection the materializer rejects must surface as
// their error instead of a half-built Job.
func TestBuilders_InvalidConnection(t *testing.T) {
	m := passwordMigration()
	m.Spec.Source = v1beta1.PostgresConnection{Username: "u"}
	if _, err := buildCleanupJob(m, testRunnerImage); err == nil {
		t.Fatal("cleanup builder must reject an invalid connection")
	}
	if _, err := buildCompareJob(m, testRunnerImage, compareSchema); err == nil {
		t.Fatal("compare builder must reject an invalid connection")
	}
	if _, err := buildPreflightJob(m, testRunnerImage); err == nil {
		t.Fatal("preflight builder must reject an invalid connection")
	}
}

// TestPreflightScriptFor_CloneTier pins the clone-rights tier's structure: it
// runs for every migration shape, sits between superuser verification and the
// follow battery, honours the dbProperties skip, and drops the hint lines
// when a target superuser is configured.
func TestPreflightScriptFor_CloneTier(t *testing.T) {
	t.Run("clone-only carries the tier", func(t *testing.T) {
		s := preflightScriptFor(passwordMigration())
		for _, want := range []string{
			"has_database_privilege(current_user, current_database(), 'CREATE')",
			"has_schema_privilege(current_user, n.oid, 'CREATE')",
			"datdba",
			"hint: spec.target.superuserSecretRef",
		} {
			if !strings.Contains(s, want) {
				t.Fatalf("clone-only script missing %q:\n%s", want, s)
			}
		}
		mustPrecede(t, s, `ok: connectivity target`, `ok: clone rights database`)
	})
	t.Run("follow runs the tier before its battery", func(t *testing.T) {
		s := preflightScriptFor(superMigration())
		mustPrecede(t, s, "ok: superuser target verified", "ok: clone rights database")
		mustPrecede(t, s, "ok: clone rights db-properties", "wal_level")
	})
	t.Run("dbProperties skip drops that probe", func(t *testing.T) {
		m := passwordMigration()
		m.Spec.Clone.Skip = []v1beta1.SkipOption{"vacuum", "dbProperties"}
		s := preflightScriptFor(m)
		if strings.Contains(s, "datdba") {
			t.Fatalf("skipped db-properties still probed:\n%s", s)
		}
		if !strings.Contains(s, "has_schema_privilege") {
			t.Fatal("schema probe must survive the dbProperties skip")
		}
	})
	t.Run("target superuser drops the clone hints", func(t *testing.T) {
		m := passwordMigration()
		m.Spec.Target.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "adm"}
		s := preflightScriptFor(m)
		if strings.Contains(s, "hint: spec.target.superuserSecretRef") {
			t.Fatalf("clone hints must vanish with a target superuser:\n%s", s)
		}
	})
	t.Run("schema filters ride as env", func(t *testing.T) {
		m := passwordMigration()
		m.Spec.Clone.Filters = &v1beta1.Filters{
			IncludeOnlySchemas: []string{"public", testSchemaInc},
			ExcludeSchemas:     []string{testSchemaExc},
		}
		job, err := buildPreflightJob(m, "img")
		if err != nil {
			t.Fatal(err)
		}
		c := job.Spec.Template.Spec.Containers[0]
		if got := envValue(c.Env, "PREFLIGHT_SCHEMA_INCLUDE"); got != "public\n"+testSchemaInc {
			t.Fatalf("include env = %q", got)
		}
		if got := envValue(c.Env, "PREFLIGHT_SCHEMA_EXCLUDE"); got != testSchemaExc {
			t.Fatalf("exclude env = %q", got)
		}
	})
}

// TestPreflightScript_CloneRights executes the generated script under /bin/sh
// with a stub psql, proving the clone tier end to end: the customer matrix
// (database CREATE true, schema CREATE false) fails with the exact schema
// GRANT, the shell filter keeps spec values out of the probe list, and the
// db-properties failure names both ways out.
// clonePreflightHarness writes the stateful stub psql and returns the runner
// the two clone-tier tests share; splitting the tests keeps gocyclo quiet.
func clonePreflightHarness(t *testing.T) func(*testing.T, *v1beta1.Migration, ...string) (string, int, string, string) {
	t.Helper()
	dir := t.TempDir()
	// The stub is stateful for the remediation paths: an apply (a query that IS
	// a GRANT, anchored at the start so compose queries never match) is captured
	// byte-for-byte to APPLY_OUT and, unless PSQL_STICKY=0, flips a marker the
	// probes honor so the re-check sees the grant.
	stub := `#!/bin/sh
q="$6"
for a in "$@"; do
  case "$a" in
    list=*) printf '%s' "${a#list=}" > "${LIST_OUT:-/dev/null}" ;;
    -f) q=$(cat) ;;
  esac
done
case "$q" in
  'GRANT CREATE ON DATABASE'*)
    printf '%s\n' "$q" >> "${APPLY_OUT:-/dev/null}"
    if [ "${PSQL_STICKY:-1}" = 1 ]; then : > "${STATE_DIR:?}/db-applied"; fi ;;
  'GRANT CREATE ON SCHEMA'*)
    printf '%s\n' "$q" >> "${APPLY_OUT:-/dev/null}"
    if [ "${PSQL_STICKY:-1}" = 1 ]; then : > "${STATE_DIR:?}/schemas-applied"; fi ;;
  *has_schema_privilege*)
    if [ -f "${STATE_DIR:-/nonexistent}/schemas-applied" ]; then :; else printf '%s' "${PSQL_SCHEMA_GRANTS:-}"; fi ;;
  *has_database_privilege*)
    if [ -f "${STATE_DIR:-/nonexistent}/db-applied" ]; then echo 1; else echo "${PSQL_DB_CREATE:-1}"; fi ;;
  *'GRANT CREATE ON DATABASE'*) echo "GRANT CREATE ON DATABASE \"app\" TO \"limited\"" ;;
  *datdba*) echo "${PSQL_DBPROPS:-1}" ;;
  *rolsuper*) echo 1 ;;
  *pg_namespace*) printf '%s\n' "${PSQL_SCHEMAS:-public}" | tr ',' '\n' ;;
  *current_user*) echo limited ;;
  *) echo 1 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}

	return func(t *testing.T, m *v1beta1.Migration, extra ...string) (string, int, string, string) {
		t.Helper()
		scratch := t.TempDir()
		listOut := filepath.Join(scratch, "list")
		applyOut := filepath.Join(scratch, "applied")
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
			"PGM_TARGET_SUPER_PGURI=tgt-super",
			"LIST_OUT="+listOut,
			"APPLY_OUT="+applyOut,
			"STATE_DIR="+scratch,
			"PREFLIGHT_RETRY_SLEEP=0",
			"PREFLIGHT_SCHEMA_INCLUDE="+envValue(c.Env, "PREFLIGHT_SCHEMA_INCLUDE"),
			"PREFLIGHT_SCHEMA_EXCLUDE="+envValue(c.Env, "PREFLIGHT_SCHEMA_EXCLUDE"),
		)
		cmd.Env = append(cmd.Env, extra...)
		out, err := cmd.CombinedOutput()
		list, _ := os.ReadFile(listOut)
		applied, _ := os.ReadFile(applyOut)
		var exitErr *exec.ExitError
		switch {
		case err == nil:
			return string(out), 0, string(list), string(applied)
		case errors.As(err, &exitErr):
			return string(out), exitErr.ExitCode(), string(list), string(applied)
		default:
			t.Fatalf("running preflight script: %v\n%s", err, out)
			return "", 0, "", ""
		}
	}
}

func TestPreflightScript_CloneRights(t *testing.T) {
	run := clonePreflightHarness(t)
	t.Run("customer matrix fails with the schema grant", func(t *testing.T) {
		out, code, _, _ := run(t, passwordMigration(),
			`PSQL_SCHEMA_GRANTS=GRANT CREATE ON SCHEMA public TO "limited"`)
		if code != 1 {
			t.Fatalf("code=%d, want 1; out:\n%s", code, out)
		}
		if !strings.Contains(out, "ok: clone rights database") {
			t.Fatalf("db-level CREATE was true and must pass:\n%s", out)
		}
		if !strings.Contains(out, `GRANT CREATE ON SCHEMA public TO "limited"`) ||
			!strings.Contains(out, "hint: spec.target.superuserSecretRef") {
			t.Fatalf("missing schema grant or hint:\n%s", out)
		}
	})
	t.Run("database CREATE failure names its grant", func(t *testing.T) {
		out, code, _, _ := run(t, passwordMigration(), "PSQL_DB_CREATE=0")
		if code != 1 || !strings.Contains(out, `GRANT CREATE ON DATABASE "app" TO "limited"`) {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
	t.Run("all rights pass", func(t *testing.T) {
		out, code, _, _ := run(t, passwordMigration())
		if code != 0 || !strings.Contains(out, "all checks passed") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		for _, want := range []string{"ok: clone rights database", "ok: clone rights schemas", "ok: clone rights db-properties"} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing %q:\n%s", want, out)
			}
		}
	})
	t.Run("excluded schema never reaches the probe", func(t *testing.T) {
		m := passwordMigration()
		m.Spec.Clone.Filters = &v1beta1.Filters{ExcludeSchemas: []string{testSchemaExc}}
		out, code, list, _ := run(t, m, "PSQL_SCHEMAS=public,"+testSchemaExc)
		if code != 0 {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if strings.Contains(list, testSchemaExc) || !strings.Contains(list, "public") {
			t.Fatalf("probe list = %q, want public without scratch", list)
		}
	})
	t.Run("includeOnly keeps just its schemas", func(t *testing.T) {
		m := passwordMigration()
		m.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlySchemas: []string{testSchemaInc}}
		out, code, list, _ := run(t, m, "PSQL_SCHEMAS=public,"+testSchemaInc+","+testSchemaExc)
		if code != 0 {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if strings.TrimSpace(list) != testSchemaInc {
			t.Fatalf("probe list = %q, want sales only", list)
		}
	})
	t.Run("source-only schema passes through to SQL existence", func(t *testing.T) {
		// A schema absent on the target is filtered by the pg_namespace join
		// server-side, not by the shell: the list still carries it.
		out, code, list, _ := run(t, passwordMigration(), "PSQL_SCHEMAS=public,newschema")
		if code != 0 {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if !strings.Contains(list, "newschema") {
			t.Fatalf("probe list = %q, must include newschema", list)
		}
	})
	t.Run("db-properties failure names both outs", func(t *testing.T) {
		out, code, _, _ := run(t, passwordMigration(), "PSQL_DBPROPS=0")
		if code != 1 ||
			!strings.Contains(out, "member of the owning role") ||
			!strings.Contains(out, "clone.skip: [dbProperties]") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if strings.Contains(out, "hint: spec.") {
			t.Fatalf("db-properties is not superuser-remediable, no hint expected:\n%s", out)
		}
	})
}

// TestPreflightScript_CloneRemediation drives the superuser variants of the
// clone tier through the same stub: apply captured byte-for-byte, markers
// flip the probes so the re-check semantics are exercised for real.
func TestPreflightScript_CloneRemediation(t *testing.T) {
	run := clonePreflightHarness(t)
	superM := func() *v1beta1.Migration {
		m := passwordMigration()
		m.Spec.Target.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "adm"}
		return m
	}
	t.Run("superuser remediates the missing schema grants", func(t *testing.T) {
		grants := `GRANT CREATE ON SCHEMA public TO "limited"; GRANT CREATE ON SCHEMA sales TO "limited"`
		out, code, _, applied := run(t, superM(), "PSQL_SCHEMA_GRANTS="+grants)
		if code != 0 || !strings.Contains(out, "all checks passed") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		for _, want := range []string{
			`remediated: GRANT CREATE ON SCHEMA public TO "limited"`,
			`remediated: GRANT CREATE ON SCHEMA sales TO "limited"`,
			"ok: clone rights schemas",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing %q:\n%s", want, out)
			}
		}
		if strings.Contains(out, "hint: spec.") {
			t.Fatalf("remediation must replace the hint:\n%s", out)
		}
		if strings.TrimSpace(applied) != grants {
			t.Fatalf("applied = %q, want the aggregate byte-for-byte %q", applied, grants)
		}
	})
	t.Run("superuser remediates missing database CREATE", func(t *testing.T) {
		out, code, _, applied := run(t, superM(), "PSQL_DB_CREATE=0")
		if code != 0 ||
			!strings.Contains(out, `remediated: GRANT CREATE ON DATABASE "app" TO "limited"`) ||
			!strings.Contains(out, "ok: clone rights database") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if !strings.Contains(applied, `GRANT CREATE ON DATABASE "app" TO "limited"`) {
			t.Fatalf("applied = %q, want the composed database grant", applied)
		}
	})
	t.Run("quote-bearing schema name stays one identifier", func(t *testing.T) {
		grant := `GRANT CREATE ON SCHEMA "we""ird" TO "limited"`
		out, code, _, applied := run(t, superM(), "PSQL_SCHEMA_GRANTS="+grant)
		if code != 0 || !strings.Contains(out, "remediated: "+grant) {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if strings.TrimSpace(applied) != grant {
			t.Fatalf("applied = %q, want %q byte-for-byte", applied, grant)
		}
	})
	t.Run("non-sticking remediation fails by name", func(t *testing.T) {
		out, code, _, _ := run(t, superM(),
			`PSQL_SCHEMA_GRANTS=GRANT CREATE ON SCHEMA public TO "limited"`, "PSQL_STICKY=0")
		if code != 1 ||
			!strings.Contains(out, "still lacks CREATE on schemas the restore targets after remediation") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
	})
	t.Run("db-properties never remediates even with super", func(t *testing.T) {
		out, code, _, applied := run(t, superM(), "PSQL_DBPROPS=0")
		if code != 1 || !strings.Contains(out, "member of the owning role") {
			t.Fatalf("code=%d out:\n%s", code, out)
		}
		if strings.Contains(out, "remediated:") || strings.TrimSpace(applied) != "" {
			t.Fatalf("db-properties must never apply anything; out:\n%s\napplied: %q", out, applied)
		}
	})
}
