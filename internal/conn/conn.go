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
// which keeps the literal out of the pod spec as well.
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
	Env      []corev1.EnvVar
	Volumes  []corev1.Volume
	Mounts   []corev1.VolumeMount
	Passfile *Passfile
}

// Materialize renders one side of the migration.
func Materialize(s Side, c *v1beta1.PostgresConnection) (*Materialized, error) {
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
		mode := int32(0o400)
		name := string(s) + "-password"
		m.Volumes = append(m.Volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: c.PasswordSecretRef.Name,
					Items: []corev1.KeyToPath{{
						Key:  c.PasswordSecretRef.Key,
						Path: string(s) + "-password",
					}},
					DefaultMode: &mode,
				},
			},
		})
		m.Mounts = append(m.Mounts, corev1.VolumeMount{
			Name:      name,
			MountPath: credsDir + "/" + string(s) + "-mnt",
			ReadOnly:  true,
		})
		m.Passfile = &Passfile{
			Host: c.Host,
			User: c.Username,
			File: credsDir + "/" + string(s) + "-mnt/" + string(s) + "-password",
		}
	}
	return m, nil
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
	q := url.Values{}
	if c.SSLMode != "" {
		q.Set("sslmode", c.SSLMode)
	}
	if c.TLS != nil {
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
	}
	u := url.URL{
		Scheme:   "postgresql",
		User:     url.User(c.Username),
		Host:     fmt.Sprintf("%s:%d", c.Host, port),
		Path:     "/" + c.Database,
		RawQuery: q.Encode(),
	}
	return u.String(), nil
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
// projected password files, runs the caller's setup commands, and then execs
// $0 (named by the Job's Command) with the args appended via "$@". Escaping:
// libpq passfile requires '\' and ':' in any field to be backslash-escaped;
// passwords are the only uncontrolled field (hosts/users come from the
// validated spec). setup runs after the passfile export so its commands can
// already authenticate; it must be trusted, operator-composed shell.
func PreludeScript(entries []Passfile, setup string) string {
	var b strings.Builder
	b.WriteString("set -eu\n")
	if len(entries) > 0 {
		b.WriteString("umask 077\n: > " + PgpassPath + "\n")
		for _, e := range entries {
			// sed escapes backslashes first, then colons; $(cat ...) would strip
			// trailing newlines which sed does not, so sed reads the file itself.
			fmt.Fprintf(&b,
				"printf '%%s:*:*:%%s:%%s\\n' '%s' '%s' \"$(sed -e 's/\\\\/\\\\\\\\/g' -e 's/:/\\\\:/g' %s)\" >> %s\n",
				e.Host, e.User, e.File, PgpassPath)
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
