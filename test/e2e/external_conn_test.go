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
	"fmt"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// connSummary flattens a connection so a mismatch prints the Secret
// reference rather than a pointer.
func connSummary(c v1beta1.PostgresConnection) string {
	ref := "<nil>"
	if r := c.PasswordSecretRef; r != nil {
		ref = r.Name + "/" + r.Key
	}
	return fmt.Sprintf("host=%s port=%d db=%s user=%s sslmode=%s password=%s",
		c.Host, c.Port, c.Database, c.Username, c.SSLMode, ref)
}

func TestExternalE2EConn(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pair    *externalConfig
		cluster string
		want    string
	}{
		{"cnpg source", nil, sourceCluster,
			"host=e2e-source-rw.pgcopydb-e2e.svc port=0 db=app user=app sslmode= password=e2e-source-app/password"},
		{"cnpg target", nil, targetCluster,
			"host=e2e-target-rw.pgcopydb-e2e.svc port=0 db=app user=app sslmode= password=e2e-target-app/password"},
		{"external source keeps require", testExternalPair(), sourceCluster,
			"host=source.example.com port=6432 db=shop user=shop_app sslmode=require" +
				" password=e2e-external-credentials/source-app-password"},
		{"external target without sslmode", testExternalPair(), targetCluster,
			"host=target.example.com port=5432 db=shop_new user=shop_app sslmode=" +
				" password=e2e-external-credentials/target-app-password"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withExternal(t, tc.pair)
			if got := connSummary(e2eConn(tc.cluster)); got != tc.want {
				t.Fatalf("e2eConn(%q) = %s, want %s", tc.cluster, got, tc.want)
			}
		})
	}
}

func TestExternalAppURL(t *testing.T) {
	disable := pairWith(func(p *externalConfig) { p.Target.SSLMode = "disable" })
	for _, tc := range []struct {
		name     string
		pair     *externalConfig
		cluster  string
		password []byte
		want     string
	}{
		{"cnpg with password", nil, sourceCluster, []byte("pw"),
			"postgresql://app:pw@e2e-source-rw.pgcopydb-e2e.svc:5432/app"},
		{"cnpg password-free", nil, targetCluster, nil,
			"postgresql://app@e2e-target-rw.pgcopydb-e2e.svc:5432/app"},
		{"external password-free keeps require", testExternalPair(), sourceCluster, nil,
			"postgresql://shop_app@source.example.com:6432/shop?sslmode=require"},
		{"external password is escaped, no sslmode", testExternalPair(), targetCluster, []byte(`p@ss:w\rd`),
			"postgresql://shop_app:p%40ss%3Aw%5Crd@target.example.com:5432/shop_new"},
		{"external keeps disable", disable, targetCluster, nil,
			"postgresql://shop_app@target.example.com:5432/shop_new?sslmode=disable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withExternal(t, tc.pair)
			if got := appURL(tc.cluster, tc.password); got != tc.want {
				t.Fatalf("appURL(%q) = %s, want %s", tc.cluster, got, tc.want)
			}
		})
	}
}
