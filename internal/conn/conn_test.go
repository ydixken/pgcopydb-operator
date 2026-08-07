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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

func inlineConn() *v1alpha1.PostgresConnection {
	return &v1alpha1.PostgresConnection{
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
	c.TLS = &v1alpha1.TLSSecretRefs{RootCA: ref("ca.crt"), Cert: ref("tls.crt"), Key: ref("tls.key")}
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
	if _, err := ComposeURI(Source, &v1alpha1.PostgresConnection{Host: "h"}); err == nil {
		t.Fatal("want error without username")
	}
}

func TestMaterialize_URISecretRef(t *testing.T) {
	c := &v1alpha1.PostgresConnection{
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
