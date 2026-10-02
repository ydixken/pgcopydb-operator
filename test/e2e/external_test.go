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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

const (
	defaultPGPort = 5432

	externalClientPod         = "e2e-psql"
	externalClientContainer   = "psql"
	externalCredentialsSecret = "e2e-external-credentials"
	pgpassKey                 = "pgpass"
	credentialsMountPath      = "/credentials"
	// libpq ignores a password file that group or others may read, and a
	// Secret volume is root-owned, so pods copy it here with mode 0600.
	pgpassFile = "/tmp/pgpass"
	// postgresUID is the postgres user of seedImage, the CNPG operand image.
	postgresUID = 26
	// labelAppName marks the client pod so the suite's primary lookups find it.
	labelAppName = "app.kubernetes.io/name"

	shell         = "bash"
	envPGHost     = "PGHOST"
	envPGDatabase = "PGDATABASE"
	envPGUser     = "PGUSER"
	envPGPassfile = "PGPASSFILE"
	envPGSSLMode  = "PGSSLMODE"
)

var pgpassEscaper = strings.NewReplacer(`\`, `\\`, `:`, `\:`)

const (
	schemePostgres   = "postgres"
	schemePostgresql = "postgresql"
	sslRequire       = "require"
	sslDisable       = "disable"
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
	case "", sslDisable, "allow", "prefer", sslRequire:
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

// renderPgpass writes one line per role and server. The database is a
// wildcard because the admin role also connects to databases besides the pair.
func renderPgpass(cfg *externalConfig) string {
	creds := cfg.credentials()
	lines := make([]string, 0, len(creds))
	for _, c := range creds {
		lines = append(lines, fmt.Sprintf("%s:%d:*:%s:%s", pgpassEscaper.Replace(c.host), c.port,
			pgpassEscaper.Replace(c.role), pgpassEscaper.Replace(c.password)))
	}
	return strings.Join(lines, "\n") + "\n"
}

// externalAppPasswordKey names a side's app password in
// externalCredentialsSecret, where e2eConn's PasswordSecretRef points.
func externalAppPasswordKey(cluster string) string {
	return sideName(cluster) + "-app-password"
}

func externalSecretData(cfg *externalConfig) map[string][]byte {
	return map[string][]byte{
		pgpassKey:                             []byte(renderPgpass(cfg)),
		externalAppPasswordKey(sourceCluster): []byte(cfg.Source.AppPassword),
		externalAppPasswordKey(targetCluster): []byte(cfg.Target.AppPassword),
	}
}

// installPgpass runs next once the password file is in place with the mode
// libpq demands.
func installPgpass(next string) []string {
	return []string{shell, "-c",
		"install -m 0600 " + credentialsMountPath + "/" + pgpassKey + " " + pgpassFile + " && exec " + next}
}

func credentialsVolume() corev1.Volume {
	return corev1.Volume{Name: "credentials", VolumeSource: corev1.VolumeSource{
		Secret: &corev1.SecretVolumeSource{
			SecretName:  externalCredentialsSecret,
			Items:       []corev1.KeyToPath{{Key: pgpassKey, Path: pgpassKey}},
			DefaultMode: ptr.To(int32(0o440)),
		},
	}}
}

func credentialsMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: "credentials", MountPath: credentialsMountPath, ReadOnly: true}
}

// externalPodSecurity meets the restricted Pod Security Standard, which a
// customer namespace may enforce. fsGroup lets postgres read the Secret volume.
func externalPodSecurity() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(postgresUID)),
		RunAsGroup: ptr.To(int64(postgresUID)), FSGroup: ptr.To(int64(postgresUID)),
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

func restrictedContainer() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// buildExternalClientPod takes no config on purpose: a password cannot reach
// its spec, only the Secret it mounts.
func buildExternalClientPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: nsE2E, Name: externalClientPod,
			Labels: map[string]string{labelAppName: externalClientPod},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: ptr.To(false),
			// sleep as PID 1 ignores SIGTERM, and nothing here needs draining.
			TerminationGracePeriodSeconds: ptr.To(int64(0)),
			SecurityContext:               externalPodSecurity(),
			Containers: []corev1.Container{{
				Name:            externalClientContainer,
				Image:           seedImage,
				Command:         installPgpass("sleep infinity"),
				Env:             []corev1.EnvVar{{Name: envPGPassfile, Value: pgpassFile}},
				Resources:       workerResources("100m", "128Mi"),
				SecurityContext: restrictedContainer(),
				VolumeMounts:    []corev1.VolumeMount{credentialsMount()},
				// Ready means the password file exists, so the first exec cannot race the copy.
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						Exec: &corev1.ExecAction{Command: []string{"test", "-s", pgpassFile}},
					},
					PeriodSeconds: 1,
				},
			}},
			Volumes: []corev1.Volume{credentialsVolume()},
		},
	}
}

// conninfoEscaper quotes a keyword conninfo value: libpq reads \' and \\
// inside single quotes.
var conninfoEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

// adminConninfo reaches db on cluster's side as its admin role. It names no
// password: libpq reads that from PGPASSFILE in the client pod.
func adminConninfo(cluster, db string) string {
	s := external.side(cluster)
	parts := []string{
		"host='" + conninfoEscaper.Replace(s.Host) + "'",
		"port=" + strconv.Itoa(int(s.Port)),
		"dbname='" + conninfoEscaper.Replace(db) + "'",
		"user='" + conninfoEscaper.Replace(s.AdminRole) + "'",
	}
	if s.SSLMode != "" {
		parts = append(parts, "sslmode="+s.SSLMode)
	}
	return strings.Join(parts, " ")
}

// externalStamp marks a database as the suite's to wipe. It is a database
// comment because pg_dump without --create never copies one to the target.
const externalStamp = "pgcopydb-e2e: disposable, the e2e suite may wipe this database"

// userObjectsSQL names up to ten objects a freshly created database lacks.
const userObjectsSQL = `SELECT coalesce(string_agg(o, ', '), '') FROM (SELECT o FROM (
	SELECT 'schema ' || n.nspname AS o FROM pg_namespace n
	 WHERE n.nspname NOT IN ('public', 'information_schema') AND n.nspname NOT LIKE 'pg\_%'
	UNION ALL
	SELECT 'relation ' || c.oid::regclass FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'
	UNION ALL
	SELECT 'routine ' || p.oid::regprocedure FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
	 WHERE n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'
	UNION ALL
	SELECT 'type ' || t.oid::regtype FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
	 WHERE n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'
	   AND t.typrelid = 0 AND t.typelem = 0
	UNION ALL
	SELECT 'extension ' || e.extname FROM pg_extension e WHERE e.extname <> 'plpgsql'
	UNION ALL
	SELECT 'large object ' || l.oid FROM pg_largeobject_metadata l
) found ORDER BY o LIMIT 10) firsts`

const stampedSQL = "SELECT coalesce(shobj_description(oid, 'pg_database'), '') = '" + externalStamp + "'" +
	" FROM pg_database WHERE datname = current_database()"

const stampSQL = "DO $$ BEGIN EXECUTE format('COMMENT ON DATABASE %I IS %L', current_database(), '" +
	externalStamp + "'); END $$"

const externalClientReadyTimeout = 5 * time.Minute

// externalPairGuarded is set once both databases passed the stamp guard.
// AfterSuite touches the pair only then, never a database it refused.
var externalPairGuarded bool

// stampDecision: a stamped database is the suite's, an empty one becomes the
// suite's by being stamped first, and anything else is refused.
func stampDecision(hasUserObjects, hasStamp bool) (stampFirst bool, err error) {
	switch {
	case hasStamp:
		return false, nil
	case !hasUserObjects:
		return true, nil
	default:
		return false, errors.New("the database holds objects and no pgcopydb-e2e stamp; the suite only" +
			" seeds, resets, and wipes a database that was empty on its first run")
	}
}

// guardPair decides both sides before either is stamped, so a refused target
// leaves a first-run source as it found it. inspect reports a side's first
// user objects ("" for none) and whether it carries the stamp.
func guardPair(inspect func(cluster string) (objects string, stamped bool)) ([]string, error) {
	var toStamp []string
	for _, cluster := range []string{sourceCluster, targetCluster} {
		objects, stamped := inspect(cluster)
		stampFirst, err := stampDecision(objects != "", stamped)
		if err != nil {
			s := external.side(cluster)
			return nil, fmt.Errorf("%s database %s on %s: %w; first objects found: %s",
				sideName(cluster), s.Database, s.Host, err, objects)
		}
		if stampFirst {
			toStamp = append(toStamp, cluster)
		}
	}
	return toStamp, nil
}

// ensureExternalClient writes the credentials and starts a fresh client pod:
// a kept pod would still hold the password file of an earlier run.
func ensureExternalClient() {
	GinkgoHelper()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: externalCredentialsSecret}}
	_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, sec, func() error {
		sec.Data = externalSecretData(external)
		return nil
	})
	Expect(err).NotTo(HaveOccurred(), "failed to apply Secret %s", externalCredentialsSecret)

	key := client.ObjectKey{Namespace: nsE2E, Name: externalClientPod}
	deleteSuiteObjects(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: externalClientPod}})
	Eventually(func(g Gomega) {
		err := k8sClient.Get(ctx, key, &corev1.Pod{})
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "previous client pod still terminating")
	}, 2*time.Minute, 2*time.Second).Should(Succeed())
	Expect(k8sClient.Create(ctx, buildExternalClientPod())).To(Succeed(), "failed to create the client pod")
	Eventually(func(g Gomega) {
		pod := &corev1.Pod{}
		g.Expect(k8sClient.Get(ctx, key, pod)).To(Succeed())
		ready := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady {
				ready = condition.Status == corev1.ConditionTrue
			}
		}
		g.Expect(ready).To(BeTrue(), "client pod %s is not ready", externalClientPod)
	}, externalClientReadyTimeout, 2*time.Second).Should(Succeed())
}

// prepareExternalDatabases stands in for the CNPG fixture setup. Nothing
// writes to either database before both passed the guard.
func prepareExternalDatabases() {
	GinkgoHelper()
	By("starting the psql client pod for the external databases")
	ensureExternalClient()
	pgSource, pgTarget = serverMajor(sourceCluster), serverMajor(targetCluster)
	By(fmt.Sprintf("checking the external source (PG %d) and target (PG %d) are empty or stamped",
		pgSource, pgTarget))
	Expect(checkMajors(pgSource, pgTarget)).To(Succeed())
	toStamp, err := guardPair(func(cluster string) (string, bool) {
		return psql(cluster, userObjectsSQL), psql(cluster, stampedSQL) == "t"
	})
	Expect(err).NotTo(HaveOccurred())
	for _, cluster := range toStamp {
		psql(cluster, stampSQL)
	}
	externalPairGuarded = true
	if seedMarkerStale() {
		By("wiping the stamped source: its seed carries a different profile or scale")
		resetDatabaseObjects(sourceCluster)
	}
}

// cleanExternalReplication runs because nothing deletes the external servers:
// a slot left behind would hold the source's WAL until someone noticed.
func cleanExternalReplication() {
	GinkgoHelper()
	dropSourceReplication()
	resetTargetReplication()
	Expect(sourceSlotCount()).
		To(Equal("0"), "pgcopydb replication slots are still active on the external source; drop them by hand")
}

// slotFilter selects the slots cleanup and counts touch. An external server
// is shared, so there only this database's slots of the suite's Migrations.
func slotFilter() string {
	if external == nil {
		return "slot_name LIKE 'pgcopydb%'"
	}
	return "database = current_database() AND " + suiteNameMatch("slot_name")
}

// originFilter is slotFilter for the target's origins, which belong to no database.
func originFilter() string {
	if external == nil {
		return "roname LIKE 'pgcopydb%'"
	}
	return suiteNameMatch("roname")
}

// suiteNameMatch matches the names pgcopydb.SlotName gives Migrations in the
// suite's namespaces: pgcopydb_<sanitized namespace>_, with LIKE's _ escaped.
func suiteNameMatch(column string) string {
	patterns := make([]string, 0, 2)
	for _, ns := range []string{nsE2E, nsX} {
		prefix := pgcopydb.SlotName(ns, "")[:len("pgcopydb_")+len(ns)+1]
		patterns = append(patterns, column+" LIKE '"+strings.ReplaceAll(prefix, "_", `\_`)+"%'")
	}
	return "(" + strings.Join(patterns, " OR ") + ")"
}

// deleteExternalClient removes what external mode left in namespaces that
// outlive the run. Every Secret the suite writes is named e2e-*, and here
// they hold the supplied passwords, so all of them go.
func deleteExternalClient() {
	GinkgoHelper()
	deleteSuiteObjects(append(seedObjects(),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: externalClientPod}})...)
	for _, ns := range []string{nsE2E, nsX} {
		secrets := &corev1.SecretList{}
		Expect(k8sClient.List(ctx, secrets, client.InNamespace(ns))).To(Succeed(), "failed to list Secrets in %s", ns)
		for i := range secrets.Items {
			if strings.HasPrefix(secrets.Items[i].Name, "e2e-") {
				deleteSuiteObjects(&secrets.Items[i])
			}
		}
	}
}

// uriUnreserved is every character a role name keeps unescaped in a URI.
const uriUnreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"

// requirePlainSecretRefNames skips the secretRef details spec for names the
// operator's secretRef form rejects: a percent-encoded user, or URI syntax
// in the role and database keys.
func requirePlainSecretRefNames() {
	if external == nil {
		return
	}
	for _, s := range []externalSide{external.Source, external.Target} {
		for _, name := range []string{s.AppRole, s.Database} {
			if strings.Trim(name, uriUnreserved) != "" {
				Skip(fmt.Sprintf("the operator's secretRef form takes plain role and database names, and %q is not", name))
			}
		}
	}
}

// requireCNPGFixtures skips a spec that controls database pods or acts beyond
// the two databases, checked at runtime so a missing label filter cannot
// send such a spec at someone else's server.
func requireCNPGFixtures() {
	if external != nil {
		Skip("needs the CNPG fixtures: it controls database pods or acts beyond the two supplied databases")
	}
}
