/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

package conn

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func inlineConn() *v1beta1.PostgresConnection {
	return &v1beta1.PostgresConnection{
		Host:     "db.example.com",
		Database: "shop",
		Username: "migrator",
		SSLMode:  "require",
		PasswordSecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "src"},
			Key:                  "password",
		},
	}
}

func TestComposeURI(t *testing.T) {
	uri, err := ComposeURI(Source, inlineConn())
	if err != nil {
		t.Fatal(err)
	}
	want := "postgresql://migrator@db.example.com:5432/shop?sslmode=require"
	if uri != want {
		t.Fatalf("got %q want %q", uri, want)
	}
}

func TestComposeURI_NeverContainsPassword(t *testing.T) {
	c := inlineConn()
	uri, err := ComposeURI(Target, c)
	if err != nil {
		t.Fatal(err)
	}
	// The URI must not embed any password-shaped userinfo.
	if strings.Contains(uri, ":@") || strings.Contains(uri, "password") {
		t.Fatalf("URI leaks credential material: %q", uri)
	}
}

func TestComposeURI_TLSPaths(t *testing.T) {
	c := inlineConn()
	c.SSLMode = "verify-full"
	ref := func(key string) *corev1.SecretKeySelector {
		return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "certs"}, Key: key}
	}
	c.TLS = &v1beta1.TLSSecretRefs{RootCA: ref("ca.crt"), Cert: ref("tls.crt"), Key: ref("tls.key")}
	uri, err := ComposeURI(Source, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sslrootcert=%2Fetc%2Fpgcopydb%2Ftls%2Fsource%2Fca.crt",
		"sslcert=%2Fetc%2Fpgcopydb%2Ftls%2Fsource%2Ftls.crt",
		"sslkey=%2Fetc%2Fpgcopydb%2Ftls%2Fsource%2Ftls.key",
		"sslmode=verify-full",
	} {
		if !strings.Contains(uri, want) {
			t.Fatalf("URI %q missing %q", uri, want)
		}
	}
}

func TestComposeURI_RequiresHostAndUser(t *testing.T) {
	if _, err := ComposeURI(Source, &v1beta1.PostgresConnection{Host: "h"}); err == nil {
		t.Fatal("want error without username")
	}
}

func TestMaterialize_URISecretRef(t *testing.T) {
	c := &v1beta1.PostgresConnection{
		URISecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "dsn"},
			Key:                  "uri",
		},
	}
	m, err := Materialize(Target, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Env) != 1 || m.Env[0].Name != "PGCOPYDB_TARGET_PGURI" || m.Env[0].ValueFrom == nil {
		t.Fatalf("want single valueFrom env, got %+v", m.Env)
	}
	if m.Env[0].Value != "" {
		t.Fatal("uriSecretRef must not render a literal value")
	}
	if len(m.Volumes) != 0 || m.Passfile != nil {
		t.Fatal("uriSecretRef needs no volumes or passfile")
	}
}

func TestMaterialize_InlineWithPassword(t *testing.T) {
	m, err := Materialize(Source, inlineConn())
	if err != nil {
		t.Fatal(err)
	}
	if m.Passfile == nil {
		t.Fatal("want passfile entry for inline password")
	}
	if m.Passfile.Host != "db.example.com" || m.Passfile.User != "migrator" {
		t.Fatalf("bad passfile identity: %+v", m.Passfile)
	}
	if !strings.HasPrefix(m.Passfile.File, "/etc/pgcopydb/creds/") {
		t.Fatalf("password file outside creds dir: %s", m.Passfile.File)
	}
	if len(m.Volumes) != 1 || m.Volumes[0].Secret == nil || *m.Volumes[0].Secret.DefaultMode != 0o400 {
		t.Fatalf("want one 0400 secret volume, got %+v", m.Volumes)
	}
}

func TestMaterialize_TLSVolume(t *testing.T) {
	c := inlineConn()
	c.PasswordSecretRef = nil
	ref := func(name, key string) *corev1.SecretKeySelector {
		return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: key}
	}
	c.TLS = &v1beta1.TLSSecretRefs{
		RootCA: ref("server-ca", "bundle.pem"),
		Cert:   ref("client-cert", "cert.pem"),
		Key:    ref("client-cert", "key.pem"),
	}
	m, err := Materialize(Target, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Volumes) != 1 || len(m.Mounts) != 1 {
		t.Fatalf("want one TLS volume and mount, got %+v", m)
	}
	proj := m.Volumes[0].Projected
	if proj == nil {
		t.Fatalf("TLS volume must be projected: %+v", m.Volumes[0])
	}
	// libpq refuses group/world-readable client keys.
	if *proj.DefaultMode != 0o400 {
		t.Fatalf("TLS defaultMode = %o, want 0400", *proj.DefaultMode)
	}
	// Each ref lands under the libpq file name ComposeURI points at.
	want := []struct{ secret, key, path string }{
		{"server-ca", "bundle.pem", "ca.crt"},
		{"client-cert", "cert.pem", "tls.crt"},
		{"client-cert", "key.pem", "tls.key"},
	}
	if len(proj.Sources) != len(want) {
		t.Fatalf("projections: %+v", proj.Sources)
	}
	for i, w := range want {
		s := proj.Sources[i].Secret
		if s == nil || s.Name != w.secret || len(s.Items) != 1 || s.Items[0].Key != w.key || s.Items[0].Path != w.path {
			t.Fatalf("projection %d = %+v, want %+v", i, proj.Sources[i], w)
		}
	}
	mount := m.Mounts[0]
	if mount.Name != m.Volumes[0].Name || mount.MountPath != "/etc/pgcopydb/tls/target" || !mount.ReadOnly {
		t.Fatalf("TLS mount = %+v", mount)
	}
}

func TestMaterialize_PartialTLS(t *testing.T) {
	// Server-auth only (the common managed-PG case): just a CA, no client pair.
	c := inlineConn()
	c.PasswordSecretRef = nil
	c.TLS = &v1beta1.TLSSecretRefs{
		RootCA: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "ca"}, Key: "bundle"},
	}
	m, err := Materialize(Source, c)
	if err != nil {
		t.Fatal(err)
	}
	proj := m.Volumes[0].Projected
	if len(proj.Sources) != 1 || proj.Sources[0].Secret.Items[0].Path != "ca.crt" {
		t.Fatalf("want only the CA projection, got %+v", proj.Sources)
	}
}

func TestMaterialize_RequiresHostAndUser(t *testing.T) {
	if _, err := Materialize(Source, &v1beta1.PostgresConnection{Database: "d"}); err == nil {
		t.Fatal("want error for inline connection without host/username")
	}
}

func TestPreludeScript(t *testing.T) {
	entries := []Passfile{{Host: "h1", User: "u1", File: "/etc/pgcopydb/creds/source-mnt/source-password"}}
	s := PreludeScript(entries, "")
	for _, want := range []string{"umask 077", "PGPASSFILE=/tmp/pgpass", execArgv0, "h1", "u1"} {
		if !strings.Contains(s, want) {
			t.Fatalf("prelude missing %q:\n%s", want, s)
		}
	}
	// No entries: strict shell straight to exec, no passfile machinery.
	if got := PreludeScript(nil, ""); got != "set -eu\n"+execArgv0 {
		t.Fatalf("empty prelude wrong: %q", got)
	}
}

// TestPreludeScript_SetupOrdering pins the contract setup commands rely on:
// they run after the passfile export (so psql in setup can authenticate) and
// before control is handed to $0.
func TestPreludeScript_SetupOrdering(t *testing.T) {
	entries := []Passfile{{Host: "h1", User: "u1", File: "/etc/pgcopydb/creds/source-mnt/source-password"}}
	setup := `psql "$PGCOPYDB_SOURCE_PGURI" -Xc 'select 1'`
	s := PreludeScript(entries, setup)
	exportAt := strings.Index(s, "export PGPASSFILE=")
	setupAt := strings.Index(s, setup)
	execAt := strings.Index(s, execArgv0)
	if exportAt < 0 || setupAt < 0 || execAt < 0 {
		t.Fatalf("prelude missing a section:\n%s", s)
	}
	if exportAt >= setupAt || setupAt >= execAt {
		t.Fatalf("prelude sections out of order (export=%d setup=%d exec=%d):\n%s", exportAt, setupAt, execAt, s)
	}
}

// libpqPassfileFields splits one passfile line the way libpq's
// passwordFromFile does: a backslash escapes the next character, an unescaped
// colon separates fields. It is the reference the escaping test parses with.
func libpqPassfileFields(line string) []string {
	var fields []string
	var b strings.Builder
	esc := false
	for _, r := range line {
		switch {
		case esc:
			b.WriteRune(r)
			esc = false
		case r == '\\':
			esc = true
		case r == ':':
			fields = append(fields, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	return append(fields, b.String())
}

// TestPreludeScript_PassfileEscaping executes the generated prelude under
// /bin/sh and proves the assembled passfile line is one psql would parse back
// to the original password, for the two characters libpq requires escaped
// (':' and '\'). A regression here silently breaks authentication for
// affected passwords, which is why the shell pipeline runs for real instead
// of being string-matched.
func TestPreludeScript_PassfileEscaping(t *testing.T) {
	dir := t.TempDir()
	writeSecret := func(name, password string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(password), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// Worst case first: colons, backslashes, adjacent and trailing.
	srcPassword := `p:a\s"s $x` + "`w`" + `:\`
	tgtPassword := "plain"
	script := PreludeScript([]Passfile{
		{Host: "src.example.com", User: "alice", File: writeSecret("source-password", srcPassword)},
		{Host: "tgt.example.com", User: "bob", File: writeSecret("target-password", tgtPassword+"\n")},
	}, "")
	// Hermetic run: point the passfile at the test dir instead of the
	// container's fixed /tmp/pgpass.
	pgpass := filepath.Join(dir, "pgpass")
	script = strings.ReplaceAll(script, PgpassPath, pgpass)

	// $0 is "true": the prelude's final exec hands over and exits 0.
	out, err := exec.Command(shellPath(t), "-c", script, "true").CombinedOutput()
	if err != nil {
		t.Fatalf("prelude failed: %v\n%s", err, out)
	}

	info, err := os.Stat(pgpass)
	if err != nil {
		t.Fatal(err)
	}
	// umask 077: libpq rejects passfiles readable by group or others.
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("passfile mode %o leaks to group/others", perm)
	}

	raw, err := os.ReadFile(pgpass)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 passfile lines, got %q", raw)
	}
	if want := `src.example.com:*:*:alice:p\:a\\s"s $x` + "`w`" + `\:\\`; lines[0] != want {
		t.Fatalf("escaped line:\n got %q\nwant %q", lines[0], want)
	}
	for i, want := range []struct{ host, user, password string }{
		{"src.example.com", "alice", srcPassword},
		// The trailing newline of a secret is trimmed, not part of the password.
		{"tgt.example.com", "bob", tgtPassword},
	} {
		f := libpqPassfileFields(lines[i])
		if len(f) != 5 || f[0] != want.host || f[1] != "*" || f[2] != "*" || f[3] != want.user || f[4] != want.password {
			t.Fatalf("line %d parses to %q, want host=%q user=%q password=%q", i, f, want.host, want.user, want.password)
		}
	}
}

// shellPath returns a POSIX shell for executing the prelude in tests.
func shellPath(t *testing.T) string {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("no sh available: %v", err)
	}
	return sh
}
