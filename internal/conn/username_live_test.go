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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// liveRoleSQL runs one statement against the admin URI with :"r" and :'p' bound.
func liveRoleSQL(t *testing.T, admin, sql, role, password string) {
	t.Helper()
	cmd := exec.Command("psql", admin, "-Xqw", "-v", "ON_ERROR_STOP=1", "-v", "r="+role, "-v", "p="+password)
	cmd.Stdin = strings.NewReader(sql + "\n")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("role statement failed: %v %s", err, out)
	}
}

// runLiveInline runs the inline-form prelude for user with password in the
// projected file, then psql through the composed URI and the passfile.
func runLiveInline(t *testing.T, host string, port int32, user, password string) (stdout, stderr string, err error) {
	t.Helper()
	m, merr := Materialize(Source, &v1beta1.PostgresConnection{
		Host: host, Port: port, Database: "postgres", Username: user,
		PasswordSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"},
	})
	if merr != nil {
		t.Fatal(merr)
	}
	dir := t.TempDir()
	pw := filepath.Join(dir, "pw")
	if werr := os.WriteFile(pw, []byte(password), 0o600); werr != nil {
		t.Fatal(werr)
	}
	entry := *m.Passfile
	entry.File = pw
	script := strings.ReplaceAll(PreludeScript(nil, []Passfile{entry}, ""), PgpassPath, filepath.Join(dir, "pgpass"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, shellPath(t), "-c", script, "sh", "-c", `exec psql "$`+uriEnv(Source)+`" -XAtwc 'select current_user'`)
	// Only the prelude's passfile may supply a credential.
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "PG") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, uriEnv(Source)+"="+m.Env[0].Value, "PGCONNECT_TIMEOUT=10")
	var o, e bytes.Buffer
	cmd.Stdout, cmd.Stderr = &o, &e
	err = cmd.Run()
	return o.String(), e.String(), err
}

// TestPreludeUsernameLive proves inline-form role names carrying shell and
// passfile syntax authenticate as themselves through the assembled passfile.
func TestPreludeUsernameLive(t *testing.T) {
	admin := os.Getenv("PGCOPYDB_TEST_PGURI")
	if admin == "" {
		t.Fatal("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for the username regressions")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	u, err := url.Parse(admin)
	if err != nil || u.Hostname() == "" {
		t.Fatal("PGCOPYDB_TEST_PGURI must be a URI naming a TCP host")
	}
	port := int64(5432)
	if u.Port() != "" {
		if port, err = strconv.ParseInt(u.Port(), 10, 32); err != nil {
			t.Fatal("PGCOPYDB_TEST_PGURI has an unparseable port")
		}
	}
	raw := make([]byte, 3)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	prefix := "pgm" + hex.EncodeToString(raw) + "_"
	password := `p:w\` + hex.EncodeToString(raw)
	for i, payload := range []string{
		"o'brien",
		`a"b`,
		"semi;colon",
		`back\slash`,
		"co:lon",
		"x'; echo INJECTED >&2; '",
		"$(echo INJECTED >&2)`echo INJECTED >&2`",
		"x'\necho INJECTED >&2\n'",
	} {
		role := prefix + payload
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			liveRoleSQL(t, admin, `CREATE ROLE :"r" LOGIN PASSWORD :'p';`, role, password)
			t.Cleanup(func() { liveRoleSQL(t, admin, `DROP ROLE IF EXISTS :"r";`, role, password) })
			stdout, stderr, err := runLiveInline(t, u.Hostname(), int32(port), role, password)
			// The role name itself spells the marker, so only stderr can show it ran.
			if strings.Contains(stderr, "INJECTED") {
				t.Fatalf("role %q ran as shell: %q %q", role, stdout, stderr)
			}
			if strings.Contains(payload, "\n") {
				// A passfile line cannot hold a newline, so libpq finds no password.
				if err == nil || !strings.Contains(stderr, "no password supplied") {
					t.Fatalf("role %q: want the passfile miss, got %v %q %q", role, err, stdout, stderr)
				}
				return
			}
			if err != nil || stderr != "" || stdout != role+"\n" {
				t.Fatalf("role %q: want current_user back, got %v %q %q", role, err, stdout, stderr)
			}
			// The server checks the passfile password, so a pass above is not trust auth.
			_, stderr, err = runLiveInline(t, u.Hostname(), int32(port), role, "wrong")
			if err == nil || !strings.Contains(stderr, "password authentication failed") {
				t.Fatalf("role %q: want a wrong passfile password refused, got %v %q", role, err, stderr)
			}
		})
	}
}
