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

// Package conn materializes PostgresConnection specs into the runner pod's
// environment, volumes, and mounts.
//
// Credential rules: the operator never reads password values. Passwords are
// projected as 0600 files and the runner's shell prelude (see Passfile fields)
// assembles a libpq passfile at container start, so credentials appear in
// neither the Job spec, nor argv, nor operator memory. Composed URIs carry no
// password. The uriSecretRef path injects the full DSN via env valueFrom,
// which keeps the literal out of the pod spec as well. The secretRef path
// injects the non-credential keys via env valueFrom, mounts the password key
// as a file, and leaves URI composition to the prelude (see secretRefPrelude).
// Superuser credentials follow the secretRef pattern (USER env, PW file) and
// reuse the side's primary URI for everything else (see superPrelude).
package conn

import (
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// Side names one end of the migration; it prefixes env vars and mount paths.
type Side string

const (
	Source Side = "source"
	Target Side = "target"
)

// execArgv0 hands control to the program passed as $0 with the Job-provided
// args. The Job's Command supplies $0 (pgcopydb for workers, /bin/sh for
// script Jobs), so one prelude serves both shapes.
const execArgv0 = `exec "$0" "$@"`

// PgpassPath is where the runner prelude assembles the passfile. It must be
// writable in the runner image (the work volume is, and /tmp is).
const PgpassPath = "/tmp/pgpass"

// credsDir holds projected password files, one per side.
const credsDir = "/etc/pgcopydb/creds"

// Convention Secret key names the secretRef form defaults to, and the
// endpoint value that switches the host to the external one.
const (
	keyDatabase      = "DB"
	keyPassword      = "PW"
	keyURL           = "URL"
	keyURLExternal   = "URL_EXTERNAL"
	keyUsername      = "USER"
	endpointExternal = "external"
)

// uriEnv is the env var pgcopydb reads for a side's connection string.
func uriEnv(s Side) string {
	if s == Source {
		return "PGCOPYDB_SOURCE_PGURI"
	}
	return "PGCOPYDB_TARGET_PGURI"
}

func tlsMountPath(s Side) string { return "/etc/pgcopydb/tls/" + string(s) }

// Passfile describes one line the runner prelude must add to the passfile:
// libpq format host:*:*:user:<contents of File>, with ':' and '\' escaped.
type Passfile struct {
	Host string
	User string
	File string
}

// Materialized is everything one side contributes to the runner pod.
type Materialized struct {
	Env     []corev1.EnvVar
	Volumes []corev1.Volume
	Mounts  []corev1.VolumeMount
	// Passfile is the static passfile entry for the inline form; secretRef
	// sides append their own line from Prelude instead.
	Passfile *Passfile
	// Prelude is a shell snippet PreludeScript runs at container start.
	Prelude string
}

// URIFile is where a secretRef prelude writes the composed URI so commands
// exec'd into the pod can recover what the spec env cannot carry.
func URIFile(s Side) string { return "/tmp/pgm-" + string(s) + "-uri" }

// SuperURIFile is the superuser counterpart of URIFile.
func SuperURIFile(s Side) string { return "/tmp/pgm-" + string(s) + "-super-uri" }

// SuperURIEnv names the env var carrying a side's superuser URI, so the
// preflight script generator and the prelude agree on one name.
func SuperURIEnv(s Side) string { return pgmEnv(s, "SUPER_PGURI") }

// pgmEnv names the side-scoped env vars the secretRef form injects; the
// PGCOPYDB_* namespace belongs to pgcopydb itself.
func pgmEnv(s Side, part string) string {
	return "PGM_" + strings.ToUpper(string(s)) + "_" + part
}

// Materialize renders one side of the migration.
func Materialize(s Side, c *v1beta1.PostgresConnection) (*Materialized, error) {
	if c.SecretRef != nil {
		return materializeSecretRef(s, c), nil
	}
	if c.URISecretRef != nil {
		return &Materialized{
			Env: []corev1.EnvVar{{
				Name:      uriEnv(s),
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: c.URISecretRef},
			}},
		}, nil
	}

	uri, err := ComposeURI(s, c)
	if err != nil {
		return nil, err
	}
	m := &Materialized{Env: []corev1.EnvVar{{Name: uriEnv(s), Value: uri}}}

	if c.TLS != nil {
		vol, mount := tlsVolume(s, c.TLS)
		m.Volumes = append(m.Volumes, vol)
		m.Mounts = append(m.Mounts, mount)
	}

	if c.PasswordSecretRef != nil {
		vol, mount, file := passwordVolume(string(s), c.PasswordSecretRef.Name, c.PasswordSecretRef.Key, false)
		m.Volumes = append(m.Volumes, vol)
		m.Mounts = append(m.Mounts, mount)
		m.Passfile = &Passfile{Host: c.Host, User: c.Username, File: file}
	}
	return m, nil
}

// passwordVolume projects one Secret key as a 0400 password file named after
// prefix ("source", "target-super", ...) and returns the in-container path of
// that file. optional tolerates a missing key at mount time so the prelude can
// fail with a named error instead of the pod hanging in ContainerCreating; the
// inline form keeps kubelet fail-fast because its spec names the key
// explicitly.
func passwordVolume(prefix, secretName, key string, optional bool) (corev1.Volume, corev1.VolumeMount, string) {
	mode := int32(0o400)
	name := prefix + "-password"
	src := &corev1.SecretVolumeSource{
		SecretName: secretName,
		Items: []corev1.KeyToPath{{
			Key:  key,
			Path: prefix + "-password",
		}},
		DefaultMode: &mode,
	}
	if optional {
		src.Optional = &optional
	}
	vol := corev1.Volume{
		Name:         name,
		VolumeSource: corev1.VolumeSource{Secret: src},
	}
	mount := corev1.VolumeMount{
		Name:      name,
		MountPath: credsDir + "/" + prefix + "-mnt",
		ReadOnly:  true,
	}
	return vol, mount, credsDir + "/" + prefix + "-mnt/" + prefix + "-password"
}

// DefaultKeys returns the convention key names the secretRef form falls back
// to; the CRD's ConnectionSecretKeys defaults must stay equal to these, which
// an envtest assertion pins.
func DefaultKeys() v1beta1.ConnectionSecretKeys {
	return v1beta1.ConnectionSecretKeys{
		Database: keyDatabase, Password: keyPassword, URL: keyURL,
		URLExternal: keyURLExternal, Username: keyUsername,
	}
}

// effectiveKeys fills the convention defaults for a nil or partial keys
// object; apiserver defaulting covers the partial case at admission, the Go
// fallback covers specs that never passed through it (unit tests, dry runs).
func effectiveKeys(k *v1beta1.ConnectionSecretKeys) v1beta1.ConnectionSecretKeys {
	eff := DefaultKeys()
	if k == nil {
		return eff
	}
	if k.Database != "" {
		eff.Database = k.Database
	}
	if k.Password != "" {
		eff.Password = k.Password
	}
	if k.URL != "" {
		eff.URL = k.URL
	}
	if k.URLExternal != "" {
		eff.URLExternal = k.URLExternal
	}
	if k.Username != "" {
		eff.Username = k.Username
	}
	return eff
}

// materializeSecretRef wires one Secret's keys into the pod by reference: the
// operator only picks which keys feed which env vars and mounts; the runner's
// prelude composes the URI and passfile line from their values at start.
func materializeSecretRef(s Side, c *v1beta1.PostgresConnection) *Materialized {
	sr := c.SecretRef
	keys := effectiveKeys(sr.Keys)
	hostKey := keys.URL
	if sr.Endpoint == endpointExternal {
		hostKey = keys.URLExternal
	}
	optional := true
	m := &Materialized{Env: []corev1.EnvVar{
		// The database key must exist (the kubelet refuses to start the pod
		// otherwise); USER and HOST are legitimately absent when DB is a URI.
		{Name: pgmEnv(s, "DB"), ValueFrom: secretEnvRef(sr.Name, keys.Database, nil)},
		{Name: pgmEnv(s, "USER"), ValueFrom: secretEnvRef(sr.Name, keys.Username, &optional)},
		{Name: pgmEnv(s, "HOST"), ValueFrom: secretEnvRef(sr.Name, hostKey, &optional)},
	}}
	if c.TLS != nil {
		vol, mount := tlsVolume(s, c.TLS)
		m.Volumes = append(m.Volumes, vol)
		m.Mounts = append(m.Mounts, mount)
	}
	vol, mount, file := passwordVolume(string(s), sr.Name, keys.Password, true)
	m.Volumes = append(m.Volumes, vol)
	m.Mounts = append(m.Mounts, mount)
	m.Prelude = secretRefPrelude(s, sslParam(c), tlsParams(s, c), file, keys.Password)
	return m
}

// secretEnvRef selects one Secret key as an env var source.
func secretEnvRef(secretName, key string, opt *bool) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
		Key:                  key,
		Optional:             opt,
	}}
}

// MaterializeSuperuser renders one side's optional superuser credentials for
// the preflight Job. The prelude swaps the userinfo of the side's primary URI,
// so host, port, database, sslmode, and TLS paths are inherited and the
// superuser Secret contributes only USER and PW; its URL keys, when present,
// are verified at start to name the same endpoint. The snippet MUST run after
// the side's primary snippet (it reads the composed URI and fails by name
// otherwise). Returns nil when the side has no superuserSecretRef.
func MaterializeSuperuser(s Side, c *v1beta1.PostgresConnection) *Materialized {
	sr := c.SuperuserSecretRef
	if sr == nil {
		return nil
	}
	keys := effectiveKeys(sr.Keys)
	// The super host key follows the primary's endpoint choice when the
	// primary is the secretRef form: both name the same server, so the
	// internal/external pick must agree or the sameness check misfires.
	endpoint := sr.Endpoint
	if c.SecretRef != nil {
		endpoint = c.SecretRef.Endpoint
	}
	hostKey := keys.URL
	if endpoint == endpointExternal {
		hostKey = keys.URLExternal
	}
	optional := true
	m := &Materialized{Env: []corev1.EnvVar{
		// The URL key only verifies endpoint sameness; absent is fine.
		{Name: pgmEnv(s, "SUPER_USER"), ValueFrom: secretEnvRef(sr.Name, keys.Username, nil)},
		{Name: pgmEnv(s, "SUPER_HOST"), ValueFrom: secretEnvRef(sr.Name, hostKey, &optional)},
	}}
	vol, mount, file := passwordVolume(string(s)+"-super", sr.Name, keys.Password, true)
	m.Volumes = append(m.Volumes, vol)
	m.Mounts = append(m.Mounts, mount)
	m.Prelude = superPrelude(s, file, keys.Password)
	return m
}

// superPrelude renders the shell that derives a side's superuser URI from the
// already-composed primary URI: same endpoint, database, and query params,
// userinfo swapped for the superuser (password-free; the PW file feeds the
// passfile). A passfile line whose user matches the primary's is first-line-
// wins and harmless. Non-URI primaries (key=value DSNs via uriSecretRef) are
// rejected by name; brackets/IPv6 stay out of scope like the primary parser.
func superPrelude(s Side, pwFile, pwKey string) string {
	const template = `pf_esc() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/:/\\:/g'; }
[ -n "${@BASE@-}" ] || { echo "@SIDE@ superuser secret: primary connection URI is not composed; prelude ordering bug" >&2; exit 1; }
sup_base=$@BASE@
case "$sup_base" in postgresql://*|postgres://*) ;; *) echo "@SIDE@ superuser secret: the primary connection is not a postgresql:// URI; key=value DSNs are unsupported here" >&2; exit 1 ;; esac
sup_user=${@USER@-}
[ -n "$sup_user" ] || { echo "@SIDE@ superuser secret: username key empty" >&2; exit 1; }
case "$sup_user" in *@*|*:*|*/*|*"?"*|*"#"*|*%*|*"&"*) echo "@SIDE@ superuser secret: the username key holds URI syntax; unsupported for superuserSecretRef" >&2; exit 1 ;; esac
sup_rest=${sup_base#*://}
sup_hp=${sup_rest%%\?*}; sup_hostpart=${sup_hp%%/*}
case "$sup_hostpart" in *@*) sup_hostport=${sup_hostpart##*@} ;; *) sup_hostport=$sup_hostpart ;; esac
sup_host=${sup_hostport%%:*}
sup_want=${@HOST@-}
if [ -n "$sup_want" ]; then
  case "$sup_hostport" in *:*) sup_hpd=$sup_hostport ;; *) sup_hpd=$sup_hostport:5432 ;; esac
  case "$sup_want" in *:*) sup_cmp=$sup_hpd ;; *) sup_cmp=$sup_host ;; esac
  [ "$sup_want" = "$sup_cmp" ] || { echo "@SIDE@ superuser secret: url key '$sup_want' does not match the connection endpoint '$sup_cmp'; the superuser secret URL keys MUST name the same endpoint" >&2; exit 1; }
fi
sup_uri="${sup_base%%://*}://$sup_user@$sup_hostport${sup_rest#"$sup_hostpart"}"
export @SUPERURI@="$sup_uri"
printf '%s' "$sup_uri" > @URIFILE@
[ -f @PWFILE@ ] || { echo "@SIDE@ superuser secret: password key @PWKEY@ missing" >&2; exit 1; }
printf '%s:*:*:%s:%s\n' "$(pf_esc "$sup_host")" "$(pf_esc "$sup_user")" "$(pf_esc "$(cat @PWFILE@)")" >> @PGPASS@
`
	return strings.NewReplacer(
		"@BASE@", uriEnv(s),
		"@USER@", pgmEnv(s, "SUPER_USER"),
		"@HOST@", pgmEnv(s, "SUPER_HOST"),
		"@SUPERURI@", SuperURIEnv(s),
		"@URIFILE@", SuperURIFile(s),
		"@PWFILE@", pwFile,
		"@PWKEY@", pwKey,
		"@PGPASS@", PgpassPath,
		"@SIDE@", string(s),
	).Replace(template)
}

// ComposeURI builds a password-free libpq URI for one side. libpq resolves the
// password from the passfile the runner prelude assembles (PGPASSFILE env).
func ComposeURI(s Side, c *v1beta1.PostgresConnection) (string, error) {
	if c.Host == "" || c.Username == "" {
		return "", fmt.Errorf("%s: host and username are required without uriSecretRef", s)
	}
	port := c.Port
	if port == 0 {
		port = 5432
	}
	u := url.URL{
		Scheme:   "postgresql",
		User:     url.User(c.Username),
		Host:     fmt.Sprintf("%s:%d", c.Host, port),
		Path:     "/" + c.Database,
		RawQuery: querySuffix(s, c),
	}
	return u.String(), nil
}

// sslParam renders the spec's sslmode as one libpq query param, or "".
func sslParam(c *v1beta1.PostgresConnection) string {
	if c.SSLMode == "" {
		return ""
	}
	q := url.Values{}
	q.Set("sslmode", c.SSLMode)
	return q.Encode()
}

// tlsParams renders the spec's TLS file paths as libpq query params, or "".
// Split from sslParam because the secretRef prelude gates only sslmode on the
// DB URI; the mounted cert paths always apply.
func tlsParams(s Side, c *v1beta1.PostgresConnection) string {
	if c.TLS == nil {
		return ""
	}
	q := url.Values{}
	base := tlsMountPath(s)
	if c.TLS.RootCA != nil {
		q.Set("sslrootcert", base+"/ca.crt")
	}
	if c.TLS.Cert != nil {
		q.Set("sslcert", base+"/tls.crt")
	}
	if c.TLS.Key != nil {
		q.Set("sslkey", base+"/tls.key")
	}
	return q.Encode()
}

// querySuffix joins the spec-side query params for the inline form's URI;
// url.Values.Encode keeps the result shell-single-quote safe.
func querySuffix(s Side, c *v1beta1.PostgresConnection) string {
	ssl, tls := sslParam(c), tlsParams(s, c)
	switch {
	case ssl == "":
		return tls
	case tls == "":
		return ssl
	default:
		return ssl + "&" + tls
	}
}

// secretRefPrelude renders the shell that turns one side's injected Secret
// keys into the PGCOPYDB_*_PGURI env var and a passfile line. A DB value that
// parses as a URI is authoritative (its user/host/port/database win); a bare
// value composes from the HOST and USER envs. Values carrying URI syntax or
// percent-encoding are rejected by name (they would compose a wrong URI or a
// passfile line libpq never matches); uriSecretRef is the escape hatch, and
// the DB URI must be password-free (the PW key carries the password).
// ponytail: host parsing assumes host or host:port; bracketed IPv6 literals
// are out of scope until someone needs them.
func secretRefPrelude(s Side, sslmode, tls, pwFile, pwKey string) string {
	const template = `pf_esc() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/:/\\:/g'; }
db=$@DB@
case "$db" in
postgresql://*|postgres://*)
  uri=$db
  rest=${uri#*://}; hp=${rest%%\?*}; hostpart=${hp%%/*}
  case "$hostpart" in
  *@*)
    user=${hostpart%%@*}; hostport=${hostpart##*@}
    case "$user" in *:*) echo "@SIDE@ connection secret: the DB URI must be password-free; the password belongs in the @PWKEY@ key, or use uriSecretRef" >&2; exit 1 ;; esac
    [ "$user@$hostport" = "$hostpart" ] || { echo "@SIDE@ connection secret: unparseable userinfo in the DB URI; use uriSecretRef" >&2; exit 1; }
    ;;
  *)
    user=${@USER@-}
    [ -n "$user" ] || { echo "@SIDE@ connection secret: no user in the DB URI and the username key is empty" >&2; exit 1; }
    case "$user" in *@*|*:*|*/*|*"?"*|*"#"*|*%*|*"&"*) echo "@SIDE@ connection secret: the username key holds URI syntax; use uriSecretRef for such values" >&2; exit 1 ;; esac
    hostport=$hostpart
    uri="postgresql://$user@$rest"
    ;;
  esac
  case "$user$hostport" in *%*) echo "@SIDE@ connection secret: percent-encoded user or host in the DB URI never matches the passfile; use plain values or uriSecretRef" >&2; exit 1 ;; esac
  ;;
*)
  host=${@HOST@-}; user=${@USER@-}
  [ -n "$host" ] || { echo "@SIDE@ connection secret: url key empty and DB is not a URI" >&2; exit 1; }
  [ -n "$user" ] || { echo "@SIDE@ connection secret: username key empty and DB is not a URI" >&2; exit 1; }
  case "$user" in *@*|*:*|*/*|*"?"*|*"#"*|*%*|*"&"*) echo "@SIDE@ connection secret: the username key holds URI syntax; use uriSecretRef for such values" >&2; exit 1 ;; esac
  case "$db" in *@*|*:*|*/*|*"?"*|*"#"*|*%*|*"&"*) echo "@SIDE@ connection secret: the database key holds URI syntax; use uriSecretRef for such values" >&2; exit 1 ;; esac
  case "$host" in *@*|*/*|*"?"*|*"#"*|*%*|*"&"*) echo "@SIDE@ connection secret: the url key holds URI syntax; use uriSecretRef for such values" >&2; exit 1 ;; esac
  case "$host" in *:*) hostport=$host ;; *) hostport=$host:5432 ;; esac
  uri="postgresql://$user@$hostport/$db"
  ;;
esac
host=${hostport%%:*}
`
	b := strings.NewReplacer(
		"@DB@", pgmEnv(s, "DB"),
		"@USER@", pgmEnv(s, "USER"),
		"@HOST@", pgmEnv(s, "HOST"),
		"@SIDE@", string(s),
		"@PWKEY@", pwKey,
	).Replace(template)
	if tls != "" {
		// The mounted cert paths come from the spec, never the Secret, so
		// they always apply.
		b += `case "$uri" in *\?*) uri="$uri&` + tls + `" ;; *) uri="$uri?` + tls + `" ;; esac
`
	}
	if sslmode != "" {
		// The DB URI keeps its own sslmode; the spec's only fills the gap.
		// Anchored to ? or & so a value merely containing the text does not
		// suppress the append.
		b += `case "$uri" in *"?sslmode="*|*"&sslmode="*) ;; *\?*) uri="$uri&` + sslmode + `" ;; *) uri="$uri?` + sslmode + `" ;; esac
`
	}
	b += `export ` + uriEnv(s) + `="$uri"
printf '%s' "$uri" > ` + URIFile(s) + `
[ -f ` + pwFile + ` ] || { echo "` + string(s) + ` connection secret: password key ` + pwKey + ` missing" >&2; exit 1; }
printf '%s:*:*:%s:%s\n' "$(pf_esc "$host")" "$(pf_esc "$user")" "$(pf_esc "$(cat ` + pwFile + `)")" >> ` + PgpassPath + `
`
	return b
}

// URIRecover returns the shell prefix that restores the PGCOPYDB_*_PGURI env
// vars in commands exec'd into the runner: secretRef preludes compose them at
// container start, where the pod spec env cannot carry them. No-op for the
// other connection forms.
func URIRecover() string {
	var b strings.Builder
	for _, s := range []Side{Source, Target} {
		fmt.Fprintf(&b, "[ -f %[1]s ] && { %[2]s=$(cat %[1]s); export %[2]s; }; ", URIFile(s), uriEnv(s))
		fmt.Fprintf(&b, "[ -f %[1]s ] && { %[2]s=$(cat %[1]s); export %[2]s; }; ",
			SuperURIFile(s), pgmEnv(s, "SUPER_PGURI"))
	}
	return b.String()
}

// tlsVolume projects the referenced TLS secret keys under a per-side path.
// DefaultMode 0400: libpq refuses group/world-readable client keys.
func tlsVolume(s Side, tls *v1beta1.TLSSecretRefs) (corev1.Volume, corev1.VolumeMount) {
	mode := int32(0o400)
	var sources []corev1.VolumeProjection
	add := func(sel *corev1.SecretKeySelector, path string) {
		if sel == nil {
			return
		}
		sources = append(sources, corev1.VolumeProjection{
			Secret: &corev1.SecretProjection{
				LocalObjectReference: sel.LocalObjectReference,
				Items:                []corev1.KeyToPath{{Key: sel.Key, Path: path}},
			},
		})
	}
	add(tls.RootCA, "ca.crt")
	add(tls.Cert, "tls.crt")
	add(tls.Key, "tls.key")

	name := string(s) + "-tls"
	vol := corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{Sources: sources, DefaultMode: &mode},
		},
	}
	return vol, corev1.VolumeMount{Name: name, MountPath: tlsMountPath(s), ReadOnly: true}
}

// PreludeScript returns the shell prelude that assembles the passfile from the
// projected password files, runs each side's snippet (secretRef URI
// composition), runs the caller's setup commands, and then execs $0 (named by
// the Job's Command) with the args appended via "$@". Escaping: libpq passfile
// requires '\' and ':' in any field to be backslash-escaped; static entries
// only escape the password (hosts/users come from the validated spec), the
// snippets escape every field. setup runs after the passfile export so its
// commands can already authenticate; it must be trusted, operator-composed
// shell.
func PreludeScript(preludes []string, entries []Passfile, setup string) string {
	var pre []string
	for _, p := range preludes {
		if p != "" {
			pre = append(pre, p)
		}
	}
	var b strings.Builder
	b.WriteString("set -eu\n")
	if len(entries)+len(pre) > 0 {
		// Truncate before the snippets: they append their own lines.
		b.WriteString("umask 077\n: > " + PgpassPath + "\n")
		for _, e := range entries {
			// sed escapes backslashes first, then colons; $(cat ...) would strip
			// trailing newlines which sed does not, so sed reads the file itself.
			fmt.Fprintf(&b,
				"printf '%%s:*:*:%%s:%%s\\n' '%s' '%s' \"$(sed -e 's/\\\\/\\\\\\\\/g' -e 's/:/\\\\:/g' %s)\" >> %s\n",
				e.Host, e.User, e.File, PgpassPath)
		}
		for _, p := range pre {
			b.WriteString(p)
			if !strings.HasSuffix(p, "\n") {
				b.WriteByte('\n')
			}
		}
		b.WriteString("export PGPASSFILE=" + PgpassPath + "\n")
	}
	if setup != "" {
		b.WriteString(setup)
		if !strings.HasSuffix(setup, "\n") {
			b.WriteByte('\n')
		}
	}
	b.WriteString(execArgv0)
	return b.String()
}
