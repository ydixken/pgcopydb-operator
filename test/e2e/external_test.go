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

// External mode runs the suite against a database pair the user supplies
// instead of the CNPG fixtures. Any E2E_SOURCE_* or E2E_TARGET_* variable
// switches it on; see docs/operations/e2e-external.md.

package e2e

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const defaultPGPort = 5432

const (
	schemePostgres   = "postgres"
	schemePostgresql = "postgresql"
	sslRequire       = "require"
)

const (
	envSourceURI           = "E2E_SOURCE_URI"
	envSourcePassword      = "E2E_SOURCE_PASSWORD"
	envSourceAdminURI      = "E2E_SOURCE_ADMIN_URI"
	envSourceAdminPassword = "E2E_SOURCE_ADMIN_PASSWORD"
	envTargetURI           = "E2E_TARGET_URI"
	envTargetPassword      = "E2E_TARGET_PASSWORD"
	envTargetAdminURI      = "E2E_TARGET_ADMIN_URI"
	envTargetAdminPassword = "E2E_TARGET_ADMIN_PASSWORD"
)

// externalEnv is every variable external mode reads, in the order errors
// list them.
var externalEnv = []string{
	envSourceURI, envSourcePassword, envSourceAdminURI, envSourceAdminPassword,
	envTargetURI, envTargetPassword, envTargetAdminURI, envTargetAdminPassword,
}

// externalSide is one database of the pair, named as its URIs name it. The
// app role runs the Migrations; the admin role seeds, resets, and grants.
type externalSide struct {
	Host                                  string
	Port                                  int32
	Database, AppRole, AdminRole, SSLMode string
	AppPassword, AdminPassword            string
}

// externalConfig is the database pair the suite runs against in external mode.
type externalConfig struct {
	Source, Target externalSide
}

// external is set once in init; nil runs the CNPG fixtures.
var external *externalConfig

// postgresURI is what one E2E_*_URI says once parsed.
type postgresURI struct {
	host                    string
	port                    int32
	database, user, sslMode string
}

// loadExternalConfig reads the pair: nil when none of the eight variables is
// set, an error unless all eight are set and valid. Errors name variables and
// never quote values, because a misplaced password is a value.
func loadExternalConfig(getenv func(string) string) (*externalConfig, error) {
	missing := make([]string, 0, len(externalEnv))
	for _, name := range externalEnv {
		if getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) == len(externalEnv) {
		return nil, nil
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("external mode needs all eight E2E_SOURCE_* and E2E_TARGET_* variables; missing %s",
			strings.Join(missing, ", "))
	}
	for _, name := range []string{"E2E_PG_SOURCE", "E2E_PG_TARGET"} {
		if getenv(name) != "" {
			return nil, fmt.Errorf("%s picks a CNPG image and does nothing in external mode; unset it", name)
		}
	}
	source, err := loadExternalSide("E2E_SOURCE", getenv)
	if err != nil {
		return nil, err
	}
	target, err := loadExternalSide("E2E_TARGET", getenv)
	if err != nil {
		return nil, err
	}
	if source.Host == target.Host && source.Port == target.Port && source.Database == target.Database {
		return nil, errors.New("E2E_SOURCE_URI and E2E_TARGET_URI name the same database, and the suite wipes the target")
	}
	cfg := &externalConfig{Source: source, Target: target}
	if err := cfg.checkPasswordsAgree(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadExternalSide(prefix string, getenv func(string) string) (externalSide, error) {
	app, err := parsePostgresURI(prefix+"_URI", getenv(prefix+"_URI"))
	if err != nil {
		return externalSide{}, err
	}
	admin, err := parsePostgresURI(prefix+"_ADMIN_URI", getenv(prefix+"_ADMIN_URI"))
	if err != nil {
		return externalSide{}, err
	}
	switch {
	case app.database == "":
		return externalSide{}, fmt.Errorf("%s_URI names no database", prefix)
	case admin.host != app.host || admin.port != app.port:
		return externalSide{}, fmt.Errorf("%s_ADMIN_URI must name the host and port of %s_URI", prefix, prefix)
	case admin.database != "" && admin.database != app.database:
		return externalSide{}, fmt.Errorf("%s_ADMIN_URI names database %q and %s_URI names %q; name the same one or none",
			prefix, admin.database, prefix, app.database)
	case admin.sslMode != "" && admin.sslMode != app.sslMode:
		return externalSide{}, fmt.Errorf("%s_ADMIN_URI must use the sslmode of %s_URI", prefix, prefix)
	case admin.user == app.user:
		// The extension-ownership spec asserts the app role is no superuser.
		return externalSide{}, fmt.Errorf("%s_ADMIN_URI names the app role; the suite needs a separate admin role",
			prefix)
	}
	for _, name := range []string{prefix + "_PASSWORD", prefix + "_ADMIN_PASSWORD"} {
		if strings.ContainsAny(getenv(name), "\r\n") {
			return externalSide{}, fmt.Errorf("%s holds a line break, which a password file cannot carry", name)
		}
	}
	return externalSide{
		Host: app.host, Port: app.port, Database: app.database, SSLMode: app.sslMode,
		AppRole: app.user, AdminRole: admin.user,
		AppPassword: getenv(prefix + "_PASSWORD"), AdminPassword: getenv(prefix + "_ADMIN_PASSWORD"),
	}, nil
}

// parsePostgresURI never wraps url.Parse's error: it quotes the raw value,
// and with it any password the URI carries.
func parsePostgresURI(name, raw string) (postgresURI, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != schemePostgres && u.Scheme != schemePostgresql) {
		return postgresURI{}, fmt.Errorf("%s is not a postgres:// or postgresql:// URI", name)
	}
	if _, set := u.User.Password(); set {
		return postgresURI{}, fmt.Errorf("%s carries a password; pass it in the matching *_PASSWORD variable", name)
	}
	parsed := postgresURI{host: u.Hostname(), port: defaultPGPort, user: u.User.Username(),
		database: strings.TrimPrefix(u.Path, "/")}
	switch {
	case parsed.user == "":
		return postgresURI{}, fmt.Errorf("%s names no role", name)
	case parsed.host == "" || strings.ContainsAny(parsed.host, ",/"):
		return postgresURI{}, fmt.Errorf("%s must name exactly one TCP host", name)
	case strings.Contains(parsed.host, ":"):
		// The operator renders host:port unbracketed (internal/conn/conn.go).
		return postgresURI{}, fmt.Errorf("%s names an IPv6 literal host, which the operator's connection forms "+
			"do not support; use a host name or an IPv4 address", name)
	case strings.Contains(parsed.database, "/"):
		return postgresURI{}, fmt.Errorf("%s has a path that is not a database name", name)
	}
	if p := u.Port(); p != "" {
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil || n == 0 {
			return postgresURI{}, fmt.Errorf("%s has port %s, want 1 to 65535", name, p)
		}
		parsed.port = int32(n)
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return postgresURI{}, fmt.Errorf("%s has a malformed query string", name)
	}
	// The inline Migration connection has a field for sslmode and nothing else.
	for _, key := range slices.Sorted(maps.Keys(query)) {
		if key != "sslmode" {
			return postgresURI{}, fmt.Errorf("%s carries query parameter %q; only sslmode maps onto a Migration", name, key)
		}
	}
	switch mode := query.Get("sslmode"); mode {
	case "", "disable", "allow", "prefer", sslRequire:
		parsed.sslMode = mode
	case "verify-ca", "verify-full":
		return postgresURI{}, fmt.Errorf("%s asks for sslmode=%s, which needs a CA bundle the suite does not mount",
			name, mode)
	default:
		return postgresURI{}, fmt.Errorf("%s has sslmode=%q, which libpq does not know", name, mode)
	}
	return parsed, nil
}

type pgCredential struct {
	host           string
	port           int32
	role, password string
}

func (c *externalConfig) credentials() []pgCredential {
	creds := make([]pgCredential, 0, 4)
	for _, s := range []externalSide{c.Source, c.Target} {
		creds = append(creds,
			pgCredential{host: s.Host, port: s.Port, role: s.AppRole, password: s.AppPassword},
			pgCredential{host: s.Host, port: s.Port, role: s.AdminRole, password: s.AdminPassword})
	}
	return creds
}

// checkPasswordsAgree exists because libpq takes the first password file
// line that matches, so one role on one server cannot have two passwords.
func (c *externalConfig) checkPasswordsAgree() error {
	seen := make(map[string]string, 4)
	for _, cred := range c.credentials() {
		key := fmt.Sprintf("%s on %s:%d", cred.role, cred.host, cred.port)
		if pw, ok := seen[key]; ok && pw != cred.password {
			return fmt.Errorf("role %s has two passwords; a role on one server needs one", key)
		}
		seen[key] = cred.password
	}
	return nil
}

// sideName maps a fixture cluster name, the key every SQL helper takes, to
// its side of the pair. Any other cluster is a spec that missed its gate.
func sideName(cluster string) string {
	switch cluster {
	case sourceCluster:
		return sourceKey
	case targetCluster:
		return targetKey
	}
	panic("no external database stands in for cluster " + cluster)
}

// side maps a fixture cluster name to the supplied database standing in for it.
func (c *externalConfig) side(cluster string) externalSide {
	if sideName(cluster) == sourceKey {
		return c.Source
	}
	return c.Target
}
