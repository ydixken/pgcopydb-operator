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
)

// testExternalPair is the pair externalEnvFixture describes, built directly.
func testExternalPair() *externalConfig {
	return &externalConfig{
		Source: externalSide{Host: "source.example.com", Port: 6432, Database: "shop",
			AppRole: "shop_app", AdminRole: "shop_admin", SSLMode: sslRequire,
			AppPassword: "src-app-pw", AdminPassword: "src-admin-pw"},
		Target: externalSide{Host: "target.example.com", Port: 5432, Database: "shop_new",
			AppRole: "shop_app", AdminRole: "shop_admin",
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
			want: pairWith(func(p *externalConfig) { p.Target.SSLMode = "disable" })},
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
		if external == nil || external.Source.Host != "source.example.com" {
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
