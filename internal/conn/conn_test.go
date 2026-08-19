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

package conn

import (
	"maps"
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

func TestComposeURI_TLSWithoutSSLMode(t *testing.T) {
	c := inlineConn()
	c.SSLMode = ""
	c.TLS = &v1beta1.TLSSecretRefs{
		RootCA: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "certs"}, Key: "bundle.pem"},
	}
	uri, err := ComposeURI(Source, c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(uri, "sslrootcert=") {
		t.Fatalf("URI %q missing sslrootcert", uri)
	}
	if strings.Contains(uri, "sslmode=") {
		t.Fatalf("URI %q carries an sslmode nobody set", uri)
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
	// Inline keeps kubelet fail-fast: the spec names the key explicitly.
	if m.Volumes[0].Secret.Optional != nil {
		t.Fatalf("inline password volume must stay required, got %+v", m.Volumes[0].Secret)
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
	entries := []Passfile{{Host: "h1", User: "u1", File: tPwPath}}
	s := PreludeScript(nil, entries, "")
	for _, want := range []string{"umask 077", "PGPASSFILE=/tmp/pgpass", execArgv0, "h1", "u1"} {
		if !strings.Contains(s, want) {
			t.Fatalf("prelude missing %q:\n%s", want, s)
		}
	}
	// No entries: strict shell straight to exec, no passfile machinery.
	if got := PreludeScript(nil, nil, ""); got != "set -eu\n"+execArgv0 {
		t.Fatalf("empty prelude wrong: %q", got)
	}
	// Empty snippet strings count as absent, not as passfile work.
	if got := PreludeScript([]string{""}, nil, ""); got != "set -eu\n"+execArgv0 {
		t.Fatalf("blank snippet prelude wrong: %q", got)
	}
}

// TestPreludeScript_SnippetOrdering pins the contract secretRef snippets rely
// on: the passfile is truncated before they append to it, and PGPASSFILE is
// exported after, so setup and $0 see the complete file.
func TestPreludeScript_SnippetOrdering(t *testing.T) {
	entries := []Passfile{{Host: "h1", User: "u1", File: tPwPath}}
	snippet := "echo snippet-marker >> " + PgpassPath
	s := PreludeScript([]string{snippet}, entries, "echo setup-marker")
	order := []string{": > " + PgpassPath, "h1", "snippet-marker", "export PGPASSFILE=", "setup-marker", execArgv0}
	at := -1
	for _, want := range order {
		i := strings.Index(s, want)
		if i <= at {
			t.Fatalf("prelude misordered around %q (index %d, previous %d):\n%s", want, i, at, s)
		}
		at = i
	}
}

// TestPreludeScript_SetupOrdering pins the contract setup commands rely on:
// they run after the passfile export (so psql in setup can authenticate) and
// before control is handed to $0.
func TestPreludeScript_SetupOrdering(t *testing.T) {
	entries := []Passfile{{Host: "h1", User: "u1", File: tPwPath}}
	setup := `psql "$PGCOPYDB_SOURCE_PGURI" -Xc 'select 1'`
	s := PreludeScript(nil, entries, setup)
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
	script := PreludeScript(nil, []Passfile{
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

// TestSecretRefPrelude_PasswordURINeverLeaks proves a rejected password-bearing
// DB URI leaves the credential in no output, no URI file, and no passfile.
func TestSecretRefPrelude_PasswordURINeverLeaks(t *testing.T) {
	_, passfile, urifile, out, err := runSecretRefPrelude(t, secretConn(),
		map[string]string{envSrcDB: "postgresql://alice:s3cret@db.example.com/app"}, "")
	if err == nil {
		t.Fatal("want rejection of a password-bearing DB URI")
	}
	for name, got := range map[string]string{"output": out, "URI file": urifile, "passfile": passfile} {
		if strings.Contains(got, "s3cret") {
			t.Fatalf("%s leaks the URI password: %q", name, got)
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

// Fixture literals for the secretRef tests, extracted for goconst.
const (
	tBundle    = "conn-bundle"
	tHost      = "db.example.com"
	tUser      = "alice"
	tDB        = "app"
	tPass      = "pw"
	tRequire   = "require"
	tLine      = tHost + ":*:*:" + tUser + ":" + tPass
	envSrcDB   = "PGM_SOURCE_DB"
	envSrcUser = "PGM_SOURCE_USER"
	envSrcHost = "PGM_SOURCE_HOST"
	tPwPath    = "/etc/pgcopydb/creds/source-mnt/source-password"
	tURI6432   = "postgresql://" + tUser + "@" + tHost + ":6432/" + tDB
)

func secretConn() *v1beta1.PostgresConnection {
	return &v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: tBundle}}
}

// findEnv returns the env var with the given name, or nil.
func findEnv(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

func TestMaterialize_SecretRef(t *testing.T) {
	m, err := Materialize(Source, secretConn())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		env, key string
		optional bool
	}{
		{envSrcDB, "DB", false},
		{envSrcUser, "USER", true},
		{envSrcHost, "URL", true},
	} {
		e := findEnv(m.Env, want.env)
		if e == nil || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("missing valueFrom env %s in %+v", want.env, m.Env)
		}
		ref := e.ValueFrom.SecretKeyRef
		if ref.Name != tBundle || ref.Key != want.key {
			t.Fatalf("%s references %s/%s, want bundle/%s", want.env, ref.Name, ref.Key, want.key)
		}
		if got := ref.Optional != nil && *ref.Optional; got != want.optional {
			t.Fatalf("%s optional = %v, want %v", want.env, got, want.optional)
		}
	}
	if e := findEnv(m.Env, "PGCOPYDB_SOURCE_PGURI"); e != nil {
		t.Fatal("secretRef must not set the PGURI env; the prelude composes it")
	}
	if m.Passfile != nil {
		t.Fatal("secretRef must not add a static passfile entry; the prelude appends its own")
	}
	if m.Prelude == "" {
		t.Fatal("secretRef must carry a prelude snippet")
	}
	if len(m.Volumes) != 1 || m.Volumes[0].Secret == nil ||
		m.Volumes[0].Secret.SecretName != tBundle ||
		m.Volumes[0].Secret.Items[0].Key != "PW" ||
		*m.Volumes[0].Secret.DefaultMode != 0o400 {
		t.Fatalf("want one 0400 PW volume from the bundle, got %+v", m.Volumes)
	}
	// Optional mount: a bundle without the PW key must reach the prelude's
	// named error instead of hanging the pod in ContainerCreating.
	if opt := m.Volumes[0].Secret.Optional; opt == nil || !*opt {
		t.Fatalf("PW volume must be optional, got %+v", m.Volumes[0].Secret)
	}
}

func TestMaterialize_SecretRef_EndpointAndKeys(t *testing.T) {
	c := secretConn()
	c.SecretRef.Endpoint = endpointExternal
	m, err := Materialize(Target, c)
	if err != nil {
		t.Fatal(err)
	}
	if got := findEnv(m.Env, "PGM_TARGET_HOST").ValueFrom.SecretKeyRef.Key; got != keyURLExternal {
		t.Fatalf("external endpoint reads key %q, want URL_EXTERNAL", got)
	}

	c = secretConn()
	c.SecretRef.Keys = &v1beta1.ConnectionSecretKeys{
		Database: "db_name", Password: "secret", URL: "host", Username: "role",
	}
	m, err = Materialize(Source, c)
	if err != nil {
		t.Fatal(err)
	}
	for env, key := range map[string]string{
		envSrcDB: "db_name", envSrcUser: "role", envSrcHost: "host",
	} {
		if got := findEnv(m.Env, env).ValueFrom.SecretKeyRef.Key; got != key {
			t.Fatalf("%s reads key %q, want %q", env, got, key)
		}
	}
	if got := m.Volumes[0].Secret.Items[0].Key; got != "secret" {
		t.Fatalf("password volume reads key %q, want secret", got)
	}
	// Unset mapping fields keep their convention defaults.
	c.SecretRef.Endpoint = endpointExternal
	m, err = Materialize(Source, c)
	if err != nil {
		t.Fatal(err)
	}
	if got := findEnv(m.Env, envSrcHost).ValueFrom.SecretKeyRef.Key; got != keyURLExternal {
		t.Fatalf("partial keys lost the urlExternal default, got %q", got)
	}

	// A remapped urlExternal key wins over the convention default.
	c.SecretRef.Keys.URLExternal = "ext_host"
	m, err = Materialize(Source, c)
	if err != nil {
		t.Fatal(err)
	}
	if got := findEnv(m.Env, envSrcHost).ValueFrom.SecretKeyRef.Key; got != "ext_host" {
		t.Fatalf("remapped urlExternal key ignored, got %q", got)
	}
}

func TestMaterialize_SecretRef_TLS(t *testing.T) {
	c := secretConn()
	c.SSLMode = "verify-full"
	c.TLS = &v1beta1.TLSSecretRefs{
		RootCA: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "ca"}, Key: "root.pem"},
	}
	m, err := Materialize(Source, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Volumes) != 2 {
		t.Fatalf("want TLS and password volumes, got %+v", m.Volumes)
	}
	for _, want := range []string{"sslmode=verify-full", "sslrootcert="} {
		if !strings.Contains(m.Prelude, want) {
			t.Fatalf("prelude misses %q:\n%s", want, m.Prelude)
		}
	}
}

// runSecretRefPrelude executes the generated prelude for one secretRef source
// under sh with the given env and password file, and returns the URI the
// exec'd process sees, the passfile and URI-file contents, and the raw output.
func TestURIRecover(t *testing.T) {
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src-uri")
	tgtFile := filepath.Join(dir, "tgt-uri")
	script := strings.ReplaceAll(URIRecover(), URIFile(Source), srcFile)
	script = strings.ReplaceAll(script, URIFile(Target), tgtFile)
	probe := script + `printf '%s|%s' "$PGCOPYDB_SOURCE_PGURI" "$PGCOPYDB_TARGET_PGURI"`

	// Without URI files the prefix must be a no-op with a zero exit.
	out, err := exec.Command(shellPath(t), "-c", probe).CombinedOutput()
	if err != nil {
		t.Fatalf("recovery without files failed: %v: %s", err, out)
	}
	if got := string(out); got != "|" {
		t.Fatalf("recovery without files changed the env: %q", got)
	}

	for f, uri := range map[string]string{srcFile: "postgresql://u@s/db1", tgtFile: "postgresql://u@t/db2"} {
		if werr := os.WriteFile(f, []byte(uri), 0o600); werr != nil {
			t.Fatal(werr)
		}
	}
	out, err = exec.Command(shellPath(t), "-c", probe).CombinedOutput()
	if err != nil {
		t.Fatalf("recovery failed: %v: %s", err, out)
	}
	if got := string(out); got != "postgresql://u@s/db1|postgresql://u@t/db2" {
		t.Fatalf("recovered %q, want both URIs restored", got)
	}
}

func runSecretRefPrelude(t *testing.T, c *v1beta1.PostgresConnection, env map[string]string, password string) (uri, passfile, urifile, out string, err error) {
	t.Helper()
	m, merr := Materialize(Source, c)
	if merr != nil {
		t.Fatal(merr)
	}
	dir := t.TempDir()
	pwFile := filepath.Join(dir, "source-password")
	// An empty password stands for "PW key absent": the optional mount then
	// projects no file at all, which the prelude must catch by name.
	if password != "" {
		if werr := os.WriteFile(pwFile, []byte(password), 0o600); werr != nil {
			t.Fatal(werr)
		}
	}
	pgpass := filepath.Join(dir, "pgpass")
	uriFile := filepath.Join(dir, "uri")
	script := PreludeScript([]string{m.Prelude}, nil, "")
	script = strings.ReplaceAll(script, PgpassPath, pgpass)
	script = strings.ReplaceAll(script, URIFile(Source), uriFile)
	script = strings.ReplaceAll(script, credsDir+"/source-mnt/source-password", pwFile)

	cmd := exec.Command(shellPath(t), "-c", script, "sh", "-c", `printf '%s' "$PGCOPYDB_SOURCE_PGURI"`)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	raw, err := cmd.CombinedOutput()
	out = string(raw)
	if err == nil {
		uri = out
	}
	if b, rerr := os.ReadFile(pgpass); rerr == nil {
		passfile = string(b)
	}
	if b, rerr := os.ReadFile(uriFile); rerr == nil {
		urifile = string(b)
	}
	return uri, passfile, urifile, out, err
}

// TestSecretRefPrelude runs the composed shell for real: string-matching a
// script cannot prove sh parses a URI the same way the test expects.
func TestSecretRefPrelude(t *testing.T) {
	cases := []struct {
		name     string
		sslMode  string
		tls      bool
		env      map[string]string
		password string
		wantURI  string
		wantLine string
		wantErr  string
	}{
		{
			name:     "uri with port is authoritative",
			env:      map[string]string{envSrcDB: tURI6432},
			password: tPass,
			wantURI:  tURI6432,
			wantLine: tLine,
		},
		{
			name:     "uri without port",
			env:      map[string]string{envSrcDB: "postgres://alice@db.example.com/app"},
			password: tPass,
			wantURI:  "postgres://alice@db.example.com/app",
			wantLine: tLine,
		},
		{
			name:     "uri query gains missing sslmode",
			sslMode:  tRequire,
			env:      map[string]string{envSrcDB: "postgresql://alice@db.example.com/app?application_name=x"},
			password: tPass,
			wantURI:  "postgresql://alice@db.example.com/app?application_name=x&sslmode=require",
			wantLine: tLine,
		},
		{
			name:     "uri keeps its own sslmode",
			sslMode:  tRequire,
			env:      map[string]string{envSrcDB: "postgresql://alice@db.example.com/app?sslmode=disable"},
			password: tPass,
			wantURI:  "postgresql://alice@db.example.com/app?sslmode=disable",
			wantLine: tLine,
		},
		{
			name:     "userless uri takes the username key",
			env:      map[string]string{envSrcDB: "postgresql://db.example.com:5432/app", envSrcUser: tUser},
			password: tPass,
			wantURI:  "postgresql://alice@db.example.com:5432/app",
			wantLine: tLine,
		},
		{
			name:    "userless uri without username key fails",
			env:     map[string]string{envSrcDB: "postgresql://db.example.com/app"},
			wantErr: "no user in the DB URI",
		},
		{
			name:     "bare name with host:port",
			env:      map[string]string{envSrcDB: tDB, envSrcHost: "db.example.com:6432", envSrcUser: tUser},
			password: tPass,
			wantURI:  tURI6432,
			wantLine: tLine,
		},
		{
			name:     "bare name defaults the port and appends sslmode",
			sslMode:  tRequire,
			env:      map[string]string{envSrcDB: tDB, envSrcHost: tHost, envSrcUser: tUser},
			password: tPass,
			wantURI:  "postgresql://alice@db.example.com:5432/app?sslmode=require",
			wantLine: tLine,
		},
		{
			name:    "bare name without url key fails",
			env:     map[string]string{envSrcDB: tDB, envSrcUser: tUser},
			wantErr: "url key empty",
		},
		{
			name:    "bare name without username key fails",
			env:     map[string]string{envSrcDB: tDB, envSrcHost: tHost},
			wantErr: "username key empty",
		},
		{
			name:     "password escaping survives the passfile",
			env:      map[string]string{envSrcDB: tDB, envSrcHost: tHost, envSrcUser: tUser},
			password: `p:a\ss:`,
			wantURI:  "postgresql://alice@db.example.com:5432/app",
			wantLine: `db.example.com:*:*:alice:p\:a\\ss\:`,
		},
		{
			name:     "uri with sslmode keeps the spec tls params",
			sslMode:  tRequire,
			tls:      true,
			env:      map[string]string{envSrcDB: "postgresql://alice@db.example.com/app?sslmode=verify-full"},
			password: tPass,
			wantURI:  "postgresql://alice@db.example.com/app?sslmode=verify-full&sslrootcert=%2Fetc%2Fpgcopydb%2Ftls%2Fsource%2Fca.crt",
			wantLine: tLine,
		},
		{
			name:     "bare name appends tls params and sslmode",
			sslMode:  tRequire,
			tls:      true,
			env:      map[string]string{envSrcDB: tDB, envSrcHost: tHost, envSrcUser: tUser},
			password: tPass,
			wantURI:  "postgresql://alice@db.example.com:5432/app?sslrootcert=%2Fetc%2Fpgcopydb%2Ftls%2Fsource%2Fca.crt&sslmode=require",
			wantLine: tLine,
		},
		{
			name:     "uri value merely containing sslmode text still gains it",
			sslMode:  tRequire,
			env:      map[string]string{envSrcDB: "postgresql://alice@db.example.com/app?application_name=fake_sslmode=1"},
			password: tPass,
			wantURI:  "postgresql://alice@db.example.com/app?application_name=fake_sslmode=1&sslmode=require",
			wantLine: tLine,
		},
		{
			name:    "password-bearing uri fails",
			env:     map[string]string{envSrcDB: "postgresql://alice:s3cret@db.example.com/app"},
			wantErr: "must be password-free",
		},
		{
			name:    "percent-encoded uri user fails",
			env:     map[string]string{envSrcDB: "postgresql://alice%40corp@db.example.com/app"},
			wantErr: "percent-encoded",
		},
		{
			name:    "unparseable double-at userinfo fails",
			env:     map[string]string{envSrcDB: "postgresql://a@b@c.example.com/app"},
			wantErr: "unparseable userinfo",
		},
		{
			name:    "bare username with an at-sign fails",
			env:     map[string]string{envSrcDB: tDB, envSrcHost: tHost, envSrcUser: "alice@corp"},
			wantErr: "username key holds URI syntax",
		},
		{
			name:    "bare database with a slash fails",
			env:     map[string]string{envSrcDB: "app/extra", envSrcHost: tHost, envSrcUser: tUser},
			wantErr: "database key holds URI syntax",
		},
		{
			name:    "missing password key fails by name",
			env:     map[string]string{envSrcDB: tDB, envSrcHost: tHost, envSrcUser: tUser},
			wantErr: "password key PW missing",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := secretConn()
			c.SSLMode = tc.sslMode
			if tc.tls {
				c.TLS = &v1beta1.TLSSecretRefs{RootCA: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "ca"}, Key: "root.pem",
				}}
			}
			uri, passfile, urifile, out, err := runSecretRefPrelude(t, c, tc.env, tc.password)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("want failure %q, got success with %q", tc.wantErr, uri)
				}
				if !strings.Contains(out, tc.wantErr) {
					t.Fatalf("failure output %q misses %q", out, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("prelude failed: %v\n%s", err, out)
			}
			if uri != tc.wantURI {
				t.Fatalf("composed URI %q, want %q", uri, tc.wantURI)
			}
			if urifile != tc.wantURI {
				t.Fatalf("URI file holds %q, want %q", urifile, tc.wantURI)
			}
			if got := strings.TrimSuffix(passfile, "\n"); got != tc.wantLine {
				t.Fatalf("passfile line %q, want %q", got, tc.wantLine)
			}
		})
	}
}

// Fixture literals for the superuser tests.
const (
	tSuperBundle    = "admin-bundle"
	tSuperUser      = "root"
	tSuperPass      = "spw"
	envSrcSuperUser = "PGM_SOURCE_SUPER_USER"
	envSrcSuperHost = "PGM_SOURCE_SUPER_HOST"
	envSrcPGURI     = "PGCOPYDB_SOURCE_PGURI"
	tSuperLine      = tHost + ":*:*:" + tSuperUser + ":" + tSuperPass
)

func superConn() *v1beta1.PostgresConnection {
	c := inlineConn()
	c.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: tSuperBundle}
	return c
}

func TestMaterializeSuperuser_NilRef(t *testing.T) {
	m, err := MaterializeSuperuser(Source, inlineConn())
	if err != nil || m != nil {
		t.Fatalf("want nil no-op without superuserSecretRef, got %+v, %v", m, err)
	}
}

func TestMaterializeSuperuser(t *testing.T) {
	m, err := MaterializeSuperuser(Source, superConn())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		env, key string
		optional bool
	}{
		{envSrcSuperUser, "USER", false},
		{envSrcSuperHost, "URL", true},
	} {
		e := findEnv(m.Env, want.env)
		if e == nil || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("missing valueFrom env %s in %+v", want.env, m.Env)
		}
		ref := e.ValueFrom.SecretKeyRef
		if ref.Name != tSuperBundle || ref.Key != want.key {
			t.Fatalf("%s references %s/%s, want %s/%s", want.env, ref.Name, ref.Key, tSuperBundle, want.key)
		}
		if got := ref.Optional != nil && *ref.Optional; got != want.optional {
			t.Fatalf("%s optional = %v, want %v", want.env, got, want.optional)
		}
	}
	if e := findEnv(m.Env, "PGM_SOURCE_SUPER_PGURI"); e != nil {
		t.Fatal("the super URI is composed by the prelude, never spec env")
	}
	if m.Passfile != nil {
		t.Fatal("superuser sides append their passfile line from the prelude")
	}
	if m.Prelude == "" {
		t.Fatal("superuser side must carry a prelude snippet")
	}
	if len(m.Volumes) != 1 || m.Volumes[0].Name != "source-super-password" ||
		m.Volumes[0].Secret.SecretName != tSuperBundle ||
		m.Volumes[0].Secret.Items[0].Key != "PW" ||
		*m.Volumes[0].Secret.DefaultMode != 0o400 ||
		m.Volumes[0].Secret.Optional == nil || !*m.Volumes[0].Secret.Optional {
		t.Fatalf("want one optional 0400 source-super-password volume, got %+v", m.Volumes)
	}
	if m.Mounts[0].MountPath != credsDir+"/source-super-mnt" {
		t.Fatalf("super mount path %q", m.Mounts[0].MountPath)
	}
}

func TestMaterializeSuperuser_EndpointAndKeys(t *testing.T) {
	c := superConn()
	c.SuperuserSecretRef.Endpoint = endpointExternal
	m, err := MaterializeSuperuser(Target, c)
	if err != nil {
		t.Fatal(err)
	}
	if got := findEnv(m.Env, "PGM_TARGET_SUPER_HOST").ValueFrom.SecretKeyRef.Key; got != keyURLExternal {
		t.Fatalf("external endpoint reads key %q, want URL_EXTERNAL", got)
	}

	c = superConn()
	c.SuperuserSecretRef.Keys = &v1beta1.ConnectionSecretKeys{Password: "adminpw", Username: "adminrole"}
	m, err = MaterializeSuperuser(Source, c)
	if err != nil {
		t.Fatal(err)
	}
	if got := findEnv(m.Env, envSrcSuperUser).ValueFrom.SecretKeyRef.Key; got != "adminrole" {
		t.Fatalf("remapped username key ignored, got %q", got)
	}
	if got := m.Volumes[0].Secret.Items[0].Key; got != "adminpw" {
		t.Fatalf("password volume reads key %q, want the remapped one", got)
	}
}

// runSuperPrelude executes a primary prelude (when the connection has one)
// followed by the superuser snippet under sh, and returns the super URI the
// exec'd process sees plus the passfile, super URI file, and raw output.
func runSuperPrelude(t *testing.T, c *v1beta1.PostgresConnection, env map[string]string, primaryPassword, superPassword string) (superURI, passfile, urifile, out string, err error) {
	t.Helper()
	var preludes []string
	if c.SecretRef != nil {
		pm, merr := Materialize(Source, c)
		if merr != nil {
			t.Fatal(merr)
		}
		preludes = append(preludes, pm.Prelude)
	}
	sm, merr := MaterializeSuperuser(Source, c)
	if merr != nil {
		t.Fatal(merr)
	}
	preludes = append(preludes, sm.Prelude)

	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if content != "" {
			if werr := os.WriteFile(p, []byte(content), 0o600); werr != nil {
				t.Fatal(werr)
			}
		}
		return p
	}
	pwFile := write("source-password", primaryPassword)
	superPwFile := write("source-super-password", superPassword)
	pgpass := filepath.Join(dir, "pgpass")
	uriFile := filepath.Join(dir, "super-uri")

	script := PreludeScript(preludes, nil, "")
	script = strings.ReplaceAll(script, PgpassPath, pgpass)
	script = strings.ReplaceAll(script, URIFile(Source), filepath.Join(dir, "uri"))
	script = strings.ReplaceAll(script, SuperURIFile(Source), uriFile)
	script = strings.ReplaceAll(script, credsDir+"/source-super-mnt/source-super-password", superPwFile)
	script = strings.ReplaceAll(script, credsDir+"/source-mnt/source-password", pwFile)

	cmd := exec.Command(shellPath(t), "-c", script, "sh", "-c", `printf '%s' "$PGM_SOURCE_SUPER_PGURI"`)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	raw, err := cmd.CombinedOutput()
	out = string(raw)
	if err == nil {
		superURI = out
	}
	if b, rerr := os.ReadFile(pgpass); rerr == nil {
		passfile = string(b)
	}
	if b, rerr := os.ReadFile(uriFile); rerr == nil {
		urifile = string(b)
	}
	return superURI, passfile, urifile, out, err
}

// TestSuperPrelude runs the generated superuser snippet for real, over every
// primary shape it must piggyback on.
func TestSuperPrelude(t *testing.T) {
	superEnv := map[string]string{envSrcSuperUser: tSuperUser}
	cases := []struct {
		name      string
		secretRef bool
		env       map[string]string
		wantURI   string
		wantLines []string
		wantErr   string
	}{
		{
			name:      "inline primary with port and query",
			env:       map[string]string{envSrcPGURI: tURI6432 + "?sslmode=require"},
			wantURI:   "postgresql://" + tSuperUser + "@" + tHost + ":6432/" + tDB + "?sslmode=require",
			wantLines: []string{tHost + ":*:*:" + tSuperUser + ":" + tSuperPass},
		},
		{
			name:      "secretRef primary with DB as URI",
			secretRef: true,
			env:       map[string]string{envSrcDB: tURI6432},
			wantURI:   "postgresql://" + tSuperUser + "@" + tHost + ":6432/" + tDB,
			wantLines: []string{tLine, tHost + ":*:*:" + tSuperUser + ":" + tSuperPass},
		},
		{
			name:      "secretRef primary with bare name",
			secretRef: true,
			env:       map[string]string{envSrcDB: tDB, envSrcHost: tHost, envSrcUser: tUser},
			wantURI:   "postgresql://" + tSuperUser + "@" + tHost + ":5432/" + tDB,
			wantLines: []string{tLine, tSuperLine},
		},
		{
			name:      "matching url key passes",
			env:       map[string]string{envSrcPGURI: tURI6432, envSrcSuperHost: tHost},
			wantURI:   "postgresql://" + tSuperUser + "@" + tHost + ":6432/" + tDB,
			wantLines: []string{tSuperLine},
		},
		{
			name:      "matching url key with port passes",
			env:       map[string]string{envSrcPGURI: tURI6432, envSrcSuperHost: tHost + ":9999"},
			wantURI:   "postgresql://" + tSuperUser + "@" + tHost + ":6432/" + tDB,
			wantLines: []string{tSuperLine},
		},
		{
			name:    "mismatched url key fails",
			env:     map[string]string{envSrcPGURI: tURI6432, envSrcSuperHost: "other.example.com"},
			wantErr: "does not match the connection endpoint",
		},
		{
			name:    "missing username key fails",
			env:     map[string]string{envSrcPGURI: tURI6432, envSrcSuperUser: ""},
			wantErr: "username key empty",
		},
		{
			name:    "username with URI syntax fails",
			env:     map[string]string{envSrcPGURI: tURI6432, envSrcSuperUser: "root@corp"},
			wantErr: "username key holds URI syntax",
		},
		{
			name:    "non-URI primary fails",
			env:     map[string]string{envSrcPGURI: "host=x dbname=y"},
			wantErr: "key=value DSNs are unsupported",
		},
		{
			name:    "unset primary URI fails as ordering bug",
			env:     map[string]string{},
			wantErr: "prelude ordering bug",
		},
		{
			name:      "password-bearing primary never leaks into the super URI",
			env:       map[string]string{envSrcPGURI: "postgresql://" + tUser + ":s3cret@" + tHost + "/" + tDB},
			wantURI:   "postgresql://" + tSuperUser + "@" + tHost + "/" + tDB,
			wantLines: []string{tSuperLine},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := superConn()
			if tc.secretRef {
				c = secretConn()
				c.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: tSuperBundle}
			}
			env := map[string]string{}
			maps.Copy(env, superEnv)
			maps.Copy(env, tc.env)
			superURI, passfile, urifile, out, err := runSuperPrelude(t, c, env, tPass, tSuperPass)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("want failure %q, got success with %q", tc.wantErr, superURI)
				}
				if !strings.Contains(out, tc.wantErr) {
					t.Fatalf("failure output %q misses %q", out, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("prelude failed: %v\n%s", err, out)
			}
			if superURI != tc.wantURI {
				t.Fatalf("super URI %q, want %q", superURI, tc.wantURI)
			}
			if strings.Contains(superURI, "s3cret") || strings.Contains(urifile, "s3cret") {
				t.Fatalf("primary password leaked into the super URI: %q", superURI)
			}
			if urifile != tc.wantURI {
				t.Fatalf("super URI file holds %q, want %q", urifile, tc.wantURI)
			}
			lines := strings.Split(strings.TrimSuffix(passfile, "\n"), "\n")
			if len(lines) != len(tc.wantLines) {
				t.Fatalf("passfile has %d lines %q, want %d", len(lines), lines, len(tc.wantLines))
			}
			for i, want := range tc.wantLines {
				if lines[i] != want {
					t.Fatalf("passfile line %d = %q, want %q", i, lines[i], want)
				}
			}
		})
	}
}

func TestSuperPrelude_MissingPasswordFile(t *testing.T) {
	_, _, _, out, err := runSuperPrelude(t, superConn(),
		map[string]string{envSrcPGURI: tURI6432, envSrcSuperUser: tSuperUser}, tPass, "")
	if err == nil {
		t.Fatal("want failure for a missing super password file")
	}
	if !strings.Contains(out, "password key PW missing") {
		t.Fatalf("failure output %q misses the named error", out)
	}
}

func TestURIRecover_Superuser(t *testing.T) {
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src-super-uri")
	script := URIRecover()
	for _, s := range []Side{Source, Target} {
		script = strings.ReplaceAll(script, URIFile(s), filepath.Join(dir, "absent-"+string(s)))
	}
	script = strings.ReplaceAll(script, SuperURIFile(Source), srcFile)
	script = strings.ReplaceAll(script, SuperURIFile(Target), filepath.Join(dir, "absent-tgt-super"))
	probe := script + `printf '%s|%s' "$PGM_SOURCE_SUPER_PGURI" "${PGM_TARGET_SUPER_PGURI-}"`

	out, err := exec.Command(shellPath(t), "-c", probe).CombinedOutput()
	if err != nil {
		t.Fatalf("recovery without files failed: %v: %s", err, out)
	}
	if got := string(out); got != "|" {
		t.Fatalf("recovery without files changed the env: %q", got)
	}

	if werr := os.WriteFile(srcFile, []byte("postgresql://r@s/db1"), 0o600); werr != nil {
		t.Fatal(werr)
	}
	out, err = exec.Command(shellPath(t), "-c", probe).CombinedOutput()
	if err != nil {
		t.Fatalf("recovery failed: %v: %s", err, out)
	}
	if got := string(out); got != "postgresql://r@s/db1|" {
		t.Fatalf("recovered %q, want only the present super URI restored", got)
	}
}
