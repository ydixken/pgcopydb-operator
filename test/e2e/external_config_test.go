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

package e2e

import (
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

const (
	testSourceHost = "source.example.com"
	testAppRole    = "shop_app"
)

// testExternalPair is the pair externalEnvFixture describes, built directly.
func testExternalPair() *externalConfig {
	return &externalConfig{
		Source: externalSide{Host: testSourceHost, Port: 6432, Database: "shop",
			AppRole: testAppRole, AdminRole: "shop_admin", SSLMode: sslRequire,
			AppPassword: "src-app-pw", AdminPassword: "src-admin-pw"},
		Target: externalSide{Host: "target.example.com", Port: 5432, Database: "shop_new",
			AppRole: testAppRole, AdminRole: "shop_admin",
			AppPassword: "tgt-app-pw", AdminPassword: "tgt-admin-pw"},
	}
}

// pairWith is testExternalPair with one edit, for cases that differ in a field.
func pairWith(edit func(*externalConfig)) *externalConfig {
	pair := testExternalPair()
	edit(pair)
	return pair
}

func externalEnvFixture() map[string]string {
	return map[string]string{
		envSourceURI:           "postgresql://shop_app@source.example.com:6432/shop?sslmode=require",
		envSourcePassword:      "src-app-pw",
		envSourceAdminURI:      "postgresql://shop_admin@source.example.com:6432",
		envSourceAdminPassword: "src-admin-pw",
		envTargetURI:           "postgres://shop_app@target.example.com/shop_new",
		envTargetPassword:      "tgt-app-pw",
		envTargetAdminURI:      "postgres://shop_admin@target.example.com/shop_new",
		envTargetAdminPassword: "tgt-admin-pw",
	}
}

func TestExternalConfig(t *testing.T) {
	set := func(kv ...string) func(map[string]string) {
		return func(env map[string]string) {
			for i := 0; i < len(kv); i += 2 {
				env[kv[i]] = kv[i+1]
			}
		}
	}
	unset := func(names ...string) func(map[string]string) {
		return func(env map[string]string) {
			for _, name := range names {
				delete(env, name)
			}
		}
	}
	for _, tt := range []struct {
		name    string
		env     func(map[string]string)
		want    *externalConfig
		wantErr string
	}{
		{name: "unset means CNPG mode", env: func(env map[string]string) { clear(env) }},
		{name: "names, port default and sslmode extracted", env: set(), want: testExternalPair()},
		{name: "percent-encoded names decode",
			env: set(envSourceURI, "postgresql://Shop%20App@source.example.com:6432/My%27Shop?sslmode=require",
				envSourceAdminURI, "postgresql://shop_admin@source.example.com:6432/My%27Shop"),
			want: pairWith(func(p *externalConfig) { p.Source.AppRole, p.Source.Database = "Shop App", "My'Shop" })},
		{name: "IPv6 host literal",
			env: set(envSourceURI, "postgresql://shop_app@[fd00::5]:6432/shop?sslmode=require",
				envSourceAdminURI, "postgresql://shop_admin@[fd00::5]:6432"),
			wantErr: "E2E_SOURCE_URI names an IPv6 literal host"},
		{name: "sslmode disable is kept",
			env:  set(envTargetURI, "postgres://shop_app@target.example.com/shop_new?sslmode=disable"),
			want: pairWith(func(p *externalConfig) { p.Target.SSLMode = sslDisable })},
		{name: "partial set names the missing variables",
			env:     unset(envSourcePassword, envTargetAdminPassword),
			wantErr: "missing E2E_SOURCE_PASSWORD, E2E_TARGET_ADMIN_PASSWORD"},
		{name: "target alone is not CNPG mode",
			env:     unset(envSourceURI),
			wantErr: "missing E2E_SOURCE_URI"},
		{name: "password in the URI",
			env:     set(envSourceURI, "postgresql://shop_app:leaked-pw@source.example.com:6432/shop"),
			wantErr: "E2E_SOURCE_URI carries a password"},
		{name: "password in an unparseable URI is not echoed",
			env:     set(envTargetURI, "postgresql://shop_app:leaked-pw@target.example.com:port/shop_new"),
			wantErr: "E2E_TARGET_URI is not a postgres:// or postgresql:// URI"},
		{name: "password as a query parameter",
			env:     set(envSourceURI, "postgresql://shop_app@source.example.com:6432/shop?password=leaked-pw"),
			wantErr: `E2E_SOURCE_URI carries query parameter "password"`},
		{name: "unknown query parameter",
			env:     set(envTargetAdminURI, "postgres://shop_admin@target.example.com?application_name=x"),
			wantErr: `E2E_TARGET_ADMIN_URI carries query parameter "application_name"`},
		{name: "verify sslmode needs a CA the suite does not mount",
			env:     set(envSourceURI, "postgresql://shop_app@source.example.com:6432/shop?sslmode=verify-full"),
			wantErr: "sslmode=verify-full"},
		{name: "unknown sslmode",
			env:     set(envSourceURI, "postgresql://shop_app@source.example.com:6432/shop?sslmode=on"),
			wantErr: `sslmode="on"`},
		{name: "admin sslmode differs",
			env:     set(envSourceAdminURI, "postgresql://shop_admin@source.example.com:6432?sslmode=disable"),
			wantErr: "E2E_SOURCE_ADMIN_URI must use the sslmode of E2E_SOURCE_URI"},
		{name: "admin database mismatch",
			env:     set(envTargetAdminURI, "postgres://shop_admin@target.example.com/postgres"),
			wantErr: `E2E_TARGET_ADMIN_URI names database "postgres"`},
		{name: "admin host mismatch",
			env:     set(envTargetAdminURI, "postgres://shop_admin@other.example.com/shop_new"),
			wantErr: "E2E_TARGET_ADMIN_URI must name the host and port of E2E_TARGET_URI"},
		{name: "admin and app are one login",
			env:     set(envSourceAdminURI, "postgresql://shop_app@source.example.com:6432"),
			wantErr: "E2E_SOURCE_ADMIN_URI names the app role"},
		{name: "app URI without a database",
			env:     set(envTargetURI, "postgres://shop_app@target.example.com"),
			wantErr: "E2E_TARGET_URI names no database"},
		{name: "URI without a role",
			env:     set(envSourceAdminURI, "postgresql://source.example.com:6432"),
			wantErr: "E2E_SOURCE_ADMIN_URI names no role"},
		{name: "keyword conninfo is not a URI",
			env:     set(envSourceURI, "host=source.example.com dbname=shop user=shop_app"),
			wantErr: "E2E_SOURCE_URI is not a postgres:// or postgresql:// URI"},
		{name: "several hosts",
			env:     set(envSourceURI, "postgresql://shop_app@a.example.com,b.example.com/shop"),
			wantErr: "E2E_SOURCE_URI must name exactly one TCP host"},
		{name: "port zero",
			env:     set(envTargetURI, "postgres://shop_app@target.example.com:0/shop_new"),
			wantErr: "E2E_TARGET_URI has port 0"},
		{name: "source and target are one database",
			env: set(envTargetURI, "postgres://shop_app@source.example.com:6432/shop",
				envTargetAdminURI, "postgres://shop_admin@source.example.com:6432"),
			wantErr: "name the same database"},
		{name: "one role with two passwords",
			env: set(envTargetURI, "postgres://shop_app@source.example.com:6432/shop_new",
				envTargetAdminURI, "postgres://shop_admin@source.example.com:6432/shop_new"),
			wantErr: "role shop_app on source.example.com:6432 has two passwords"},
		{name: "line break in a password",
			env:     set(envSourceAdminPassword, "src-admin-pw\nsecond-line"),
			wantErr: "E2E_SOURCE_ADMIN_PASSWORD holds a line break"},
		{name: "CNPG major alongside the pair",
			env:     set("E2E_PG_TARGET", "16"),
			wantErr: "E2E_PG_TARGET picks a CNPG image"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := externalEnvFixture()
			tt.env(env)
			got, err := loadExternalConfig(func(name string) string { return env[name] })
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("loadExternalConfig() error = %v", err)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("loadExternalConfig() = %+v, want %+v", got, tt.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("loadExternalConfig() error = %v, want it to contain %q", err, tt.wantErr)
			}
			if got != nil {
				t.Errorf("loadExternalConfig() returned a config alongside its error")
			}
			secrets := []string{"leaked-pw"}
			for _, cred := range testExternalPair().credentials() {
				secrets = append(secrets, cred.password)
			}
			for _, secret := range secrets {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error %q leaks a password", err)
				}
			}
		})
	}
}

func TestExternalSide(t *testing.T) {
	pair := testExternalPair()
	if got := pair.side(sourceCluster); got != pair.Source {
		t.Errorf("side(sourceCluster) = %+v, want the source", got)
	}
	if got := pair.side(targetCluster); got != pair.Target {
		t.Errorf("side(targetCluster) = %+v, want the target", got)
	}
	if sideName(sourceCluster) != sourceKey || sideName(targetCluster) != targetKey {
		t.Errorf("sideName maps the fixture clusters to %q and %q",
			sideName(sourceCluster), sideName(targetCluster))
	}
	defer func() {
		if recover() == nil {
			t.Error("sideName accepted a cluster that is neither side")
		}
	}()
	sideName("e2e-progress-pool-source")
}

func TestExternalInit(t *testing.T) {
	if os.Getenv("E2E_TEST_CHILD") == "external-init" {
		if external == nil || external.Source.Host != testSourceHost {
			t.Fatal("init did not load the external pair")
		}
		if fixtureStorageClass != "" {
			t.Errorf("fixtureStorageClass = %q, want the cluster default", fixtureStorageClass)
		}
		return
	}
	full := []string{"E2E_TEST_CHILD=external-init"}
	fixture := externalEnvFixture()
	for _, name := range externalEnv {
		full = append(full, name+"="+fixture[name])
	}
	for _, tt := range []struct {
		name      string
		env       []string
		wantPanic string
	}{
		{name: "all eight set", env: full},
		{name: "one missing", env: full[:len(full)-1], wantPanic: "missing E2E_TARGET_ADMIN_PASSWORD"},
		{name: "CNPG major alongside the pair", env: append(slices.Clone(full), "E2E_PG_SOURCE=16"),
			wantPanic: "E2E_PG_SOURCE picks a CNPG image"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestExternalInit$")
			cmd.Env = tt.env
			out, err := cmd.CombinedOutput()
			if tt.wantPanic == "" {
				if err != nil {
					t.Fatalf("child failed: %v\n%s", err, out)
				}
				return
			}
			if err == nil || !strings.Contains(string(out), tt.wantPanic) {
				t.Fatalf("child err = %v, want a panic containing %q; output:\n%s", err, tt.wantPanic, out)
			}
		})
	}
}

func TestExternalServerMajors(t *testing.T) {
	for _, tt := range []struct {
		source, target int
		wantErr        string
	}{
		{source: 17, target: 17},
		{source: 14, target: 18},
		{source: 16, target: 15, wantErr: "target PostgreSQL 15 is older than source 16"},
		{source: 14, target: 14, wantErr: "target PostgreSQL 14 is older than 15"},
	} {
		err := checkMajors(tt.source, tt.target)
		if tt.wantErr == "" && err != nil {
			t.Errorf("checkMajors(%d, %d) = %v, want nil", tt.source, tt.target, err)
		}
		if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
			t.Errorf("checkMajors(%d, %d) = %v, want %q", tt.source, tt.target, err, tt.wantErr)
		}
	}
}

// assertNoPasswords fails when a rendered manifest carries any of the pair's
// passwords: pod specs end up in describe output and failure logs.
func assertNoPasswords(t *testing.T, obj any, pair *externalConfig) {
	t.Helper()
	out, err := yaml.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	for _, cred := range pair.credentials() {
		if strings.Contains(string(out), cred.password) {
			t.Errorf("manifest carries the password of %s on %s", cred.role, cred.host)
		}
	}
}

func TestExternalPgpass(t *testing.T) {
	pair := testExternalPair()
	want := "source.example.com:6432:*:shop_app:src-app-pw\n" +
		"source.example.com:6432:*:shop_admin:src-admin-pw\n" +
		"target.example.com:5432:*:shop_app:tgt-app-pw\n" +
		"target.example.com:5432:*:shop_admin:tgt-admin-pw\n"
	if got := renderPgpass(pair); got != want {
		t.Errorf("renderPgpass() =\n%s\nwant\n%s", got, want)
	}

	pair.Source.AppPassword = `pa:ss\word`
	pair.Source.AdminRole = "odd:admin"
	lines := strings.Split(renderPgpass(pair), "\n")
	for i, want := range []string{
		`source.example.com:6432:*:shop_app:pa\:ss\\word`,
		`source.example.com:6432:*:odd\:admin:src-admin-pw`,
	} {
		if lines[i] != want {
			t.Errorf("line %d = %q, want %q", i, lines[i], want)
		}
	}
}

func TestExternalSecretData(t *testing.T) {
	pair := testExternalPair()
	data := externalSecretData(pair)
	want := map[string]string{
		pgpassKey:             renderPgpass(pair),
		"source-app-password": pair.Source.AppPassword,
		"target-app-password": pair.Target.AppPassword,
	}
	if len(data) != len(want) {
		t.Errorf("Secret has %d keys, want %d", len(data), len(want))
	}
	for key, value := range want {
		if string(data[key]) != value {
			t.Errorf("Secret key %s holds the wrong value", key)
		}
	}
	if externalAppPasswordKey(targetCluster) != "target-app-password" {
		t.Errorf("externalAppPasswordKey(targetCluster) = %q", externalAppPasswordKey(targetCluster))
	}
}

func TestExternalClientPod(t *testing.T) {
	pod := buildExternalClientPod()
	if pod.Namespace != nsE2E || pod.Name != externalClientPod {
		t.Fatalf("client pod is %s/%s", pod.Namespace, pod.Name)
	}
	if pod.Labels[labelAppName] != externalClientPod {
		t.Errorf("client pod labels %v lack the selector sqlPodLabels uses", pod.Labels)
	}
	c := pod.Spec.Containers[0]
	if c.Name != externalClientContainer || c.Image != seedImage {
		t.Errorf("container %s runs %s", c.Name, c.Image)
	}
	if !slices.Contains(c.Env, corev1.EnvVar{Name: envPGPassfile, Value: pgpassFile}) {
		t.Errorf("container env %v does not point libpq at %s", c.Env, pgpassFile)
	}
	if got := strings.Join(c.Command, " "); !strings.Contains(got, "install -m 0600 /credentials/pgpass /tmp/pgpass") {
		t.Errorf("command %q does not copy the password file to mode 0600", got)
	}
	secret := pod.Spec.Volumes[0].Secret
	if secret == nil || secret.SecretName != externalCredentialsSecret ||
		len(secret.Items) != 1 || secret.Items[0].Key != pgpassKey {
		t.Errorf("volume %+v must mount only the pgpass key of %s", secret, externalCredentialsSecret)
	}
	if sc := pod.Spec.SecurityContext; sc == nil || sc.FSGroup == nil || *sc.FSGroup != postgresUID {
		t.Errorf("pod security context %+v cannot read the Secret volume as postgres", sc)
	}
	if c.ReadinessProbe == nil || c.ReadinessProbe.Exec == nil {
		t.Error("client pod reports ready before its password file exists")
	}
}

func TestExternalSeedJob(t *testing.T) {
	pair := testExternalPair()
	withExternal(t, pair)
	job := buildSeedJob()
	seed := job.Spec.Template.Spec.Containers[0]
	env := map[string]corev1.EnvVar{}
	for _, e := range seed.Env {
		env[e.Name] = e
	}
	for name, want := range map[string]string{
		envPGHost: testSourceHost, "PGPORT": "6432", envPGDatabase: "shop",
		envPGUser: testAppRole, envPGSSLMode: "require", envPGPassfile: pgpassFile,
	} {
		if got := env[name].Value; got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if _, found := env["PGPASSWORD"]; found {
		t.Error("external seed Job still takes PGPASSWORD from the CNPG Secret")
	}
	if !slices.Equal(seed.Command, installPgpass("bash /fixtures/run.sh")) {
		t.Errorf("command = %q", seed.Command)
	}
	if job.Spec.Template.Spec.SecurityContext == nil || len(job.Spec.Template.Spec.Volumes) != 2 {
		t.Error("external seed Job lacks the credentials volume or the security context that reads it")
	}
	assertNoPasswords(t, job, pair)

	pair.Source.SSLMode = ""
	for _, e := range buildSeedJob().Spec.Template.Spec.Containers[0].Env {
		if e.Name == "PGSSLMODE" {
			t.Errorf("PGSSLMODE=%q set without an sslmode in the URI", e.Value)
		}
	}

	external = nil
	cnpg := buildSeedJob().Spec.Template.Spec
	if !slices.Equal(cnpg.Containers[0].Command, []string{shell, "/fixtures/run.sh"}) ||
		cnpg.SecurityContext != nil || len(cnpg.Volumes) != 1 {
		t.Error("CNPG-mode seed Job changed shape")
	}
	found := false
	for _, e := range cnpg.Containers[0].Env {
		if e.Name == "PGPASSWORD" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil &&
			e.ValueFrom.SecretKeyRef.Name == srcSecret {
			found = true
		}
	}
	if !found {
		t.Error("CNPG-mode seed Job no longer reads PGPASSWORD from " + srcSecret)
	}
}

func TestExternalPSQLArgv(t *testing.T) {
	const (
		sql     = "SELECT current_user"
		oneShot = "-tAc"
	)
	pair := pairWith(func(p *externalConfig) {
		p.Target.SSLMode, p.Target.AdminRole = sslDisable, `O'Brien \ Admin`
	})
	withExternal(t, pair)
	cases := []struct {
		name      string
		got, want []string
	}{
		{
			name: "source keeps require",
			got:  psqlArgv(sourceCluster, externalClientPod, "shop", false, oneShot, sql),
			want: []string{psqlExecSubcommand, "-n", nsE2E, externalClientPod, "-c", externalClientContainer, "--",
				psqlExecProgram, "host='source.example.com' port=6432 dbname='shop' user='shop_admin' sslmode=require",
				oneShot, sql},
		},
		{
			name: "target quotes a spaced database and an odd role, and keeps disable",
			got:  psqlArgv(targetCluster, externalClientPod, "fan out", true, "-q"),
			want: []string{psqlExecSubcommand, "-i", "-n", nsE2E, externalClientPod, "-c", externalClientContainer, "--",
				psqlExecProgram, `host='target.example.com' port=5432 dbname='fan out' user='O\'Brien \\ Admin' sslmode=disable`,
				"-q"},
		},
	}
	for _, tc := range cases {
		if !slices.Equal(tc.got, tc.want) {
			t.Errorf("%s argv = %q, want %q", tc.name, tc.got, tc.want)
		}
		for _, cred := range pair.credentials() {
			if strings.Contains(strings.Join(tc.got, " "), cred.password) {
				t.Errorf("%s argv carries the password of %s", tc.name, cred.role)
			}
		}
	}
	withExternal(t, pairWith(func(p *externalConfig) { p.Source.SSLMode = "" }))
	if got := adminConninfo(sourceCluster, "shop"); strings.Contains(got, "sslmode") {
		t.Errorf("conninfo %q sets an sslmode the URI never named", got)
	}
}

func TestExternalPSQLDBErrKeepsPasswordsOut(t *testing.T) {
	pair := testExternalPair()
	withExternal(t, pair)
	command, state := newPSQLExecCommand(t, psqlExecResult{
		stderr: `psql: error: connection to server at "source.example.com", port 6432 failed: ` +
			`FATAL:  password authentication failed for user "shop_admin"`,
		exitCode: 2,
	})
	_, err := psqlDBErrWith(sourceCluster, "shop", "SELECT count(*) FROM orders",
		func(string) string { return externalClientPod },
		func(time.Duration) { t.Fatal("retried a failed login") },
		command, psqlExecTestTimeout)
	if err == nil {
		t.Fatal("psqlDBErrWith succeeded on a failed login")
	}
	calls, _, commands := state.snapshot()
	requirePSQLCommandsReaped(t, commands)
	if len(calls) != 1 || !slices.Contains(calls[0].args, externalClientContainer) {
		t.Fatalf("calls = %q, want one exec into the client container", calls)
	}
	for _, cred := range pair.credentials() {
		if strings.Contains(strings.Join(calls[0].args, " "), cred.password) ||
			strings.Contains(err.Error(), cred.password) {
			t.Errorf("the password of %s on %s reached argv or the error", cred.role, cred.host)
		}
	}
}

func TestExternalSQLPodLabels(t *testing.T) {
	withExternal(t, nil)
	cnpg := sqlPodLabels(targetCluster)
	if cnpg[labelCNPGCluster] != targetCluster || cnpg[labelCNPGRole] != rolePrimary || len(cnpg) != 2 {
		t.Errorf("CNPG mode selects %v, want the target's primary", cnpg)
	}
	withExternal(t, testExternalPair())
	if got := sqlPodLabels(targetCluster); len(got) != 1 || got[labelAppName] != externalClientPod {
		t.Errorf("external mode selects %v, want the client pod", got)
	}
}
