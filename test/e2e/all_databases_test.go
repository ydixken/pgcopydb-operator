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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

var _ = Describe("All databases", func() {
	// Both specs act server-wide: the first creates and drops databases, sets the postgres
	// password and creates an extension in postgres on both servers; both connect to postgres.
	BeforeEach(requireCNPGFixtures)

	It("clones all databases as superuser, including a newly created database, "+
		"large objects and extension configuration rows", func() {
		const name = "e2e-all-databases"
		const extraDB = "e2e_all_databases"
		// Registered first, so it runs after the Migration's cleanup: the worker must be gone.
		DeferCleanup(func() {
			for _, cluster := range []string{sourceCluster, targetCluster} {
				psqlDB(cluster, postgresContainer, "DROP DATABASE IF EXISTS "+extraDB+" WITH (FORCE)")
				psqlDB(cluster, postgresContainer, extConfigDropSQL)
			}
		})
		mig := allDatabasesMigration(name, v1beta1.CloneOptions{})

		By("seeding a source database that pgcopydb must create on the target")
		psqlDB(sourceCluster, postgresContainer, "CREATE DATABASE "+extraDB)
		psqlDB(sourceCluster, extraDB, "CREATE TABLE all_database_rows (id integer PRIMARY KEY); "+
			"INSERT INTO all_database_rows VALUES (1), (2), (3)")
		psqlDB(sourceCluster, extraDB, allDatabasesLargeObjectSQL)
		Expect(psqlDB(targetCluster, postgresContainer, "SELECT count(*) FROM pg_database WHERE datname = '"+extraDB+"'")).
			To(Equal("0"))

		By("emulating an extension configuration table in postgres on both servers")
		// pg_dump emits no DDL for an extension member, so the target needs the table first;
		// its shipped rows stand in for those its CREATE EXTENSION would insert.
		psqlDB(sourceCluster, postgresContainer, extConfigSetupSQL(extConfigShippedRows+", "+extConfigUserRows))
		psqlDB(targetCluster, postgresContainer, extConfigSetupSQL(extConfigShippedRows))
		// Counted on the source, so an empty source cannot pass the comparison below.
		Expect(largeObjectCount(sourceCluster)).To(Equal(fmt.Sprint(scaled(40))))
		Expect(psqlDB(sourceCluster, extraDB, largeObjectDigestSQL)).To(HavePrefix("1|"))
		Expect(psqlDB(sourceCluster, postgresContainer, extConfigRowsSQL)).To(HavePrefix("4|"))

		create(mig)
		waitCompleted(name, nsE2E)
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
		Expect(psqlDB(targetCluster, extraDB, "SELECT count(*) FROM all_database_rows")).To(Equal("3"))
		Expect(allDatabasesExtras(targetCluster, extraDB)).To(Equal(allDatabasesExtras(sourceCluster, extraDB)))
	})

	It("rejects all databases as app before starting a worker", func() {
		const name = "e2e-all-databases-app"
		DeferCleanup(func() { deleteMigration(name) })

		m := newMigration(name, nsE2E, v1beta1.CloneOptions{AllDatabases: true})
		m.Spec.Source.Database = postgresContainer
		m.Spec.Target.Database = postgresContainer
		create(m)

		failed := waitFailed(name, "PreflightFailed")
		msg := failureMessage(failed)
		Expect(msg).To(ContainSubstring("all-databases source requires a superuser"))
		Expect(msg).To(ContainSubstring("all-databases target requires a superuser"))
		Expect(failed.Status.Attempts).To(Equal(int32(0)), "a rejected all-databases clone started a worker attempt")
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, &batchv1.Job{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "a rejected all-databases clone has a worker Job")
	})
})

// allDatabasesMigration returns a Migration of clone with allDatabases, connecting as
// postgres to the postgres database with a temporary password, against an empty target.
// Its cleanup deletes the Migration, then the password, the target's objects and the Secret.
func allDatabasesMigration(name string, clone v1beta1.CloneOptions) *v1beta1.Migration {
	GinkgoHelper()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, Name: name}}
	Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
	Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
	DeferCleanup(func() {
		deleteMigration(name)
		for _, cluster := range []string{sourceCluster, targetCluster} {
			psql(cluster, "ALTER ROLE postgres PASSWORD NULL")
		}
		resetTargetObjects()
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, sec))).To(Succeed())
	})

	By("preparing an empty target and temporary postgres credentials on both clusters")
	resetTargetObjects()
	pw := name + "-pw"
	for _, cluster := range []string{sourceCluster, targetCluster} {
		psql(cluster, fmt.Sprintf("ALTER ROLE postgres PASSWORD '%s'", pw))
	}
	_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, sec, func() error {
		sec.Data = map[string][]byte{passwordKey: []byte(pw)}
		return nil
	})
	Expect(err).NotTo(HaveOccurred(), "failed to store temporary postgres credentials")

	clone.AllDatabases = true
	m := newMigration(name, nsE2E, clone)
	for _, connection := range []*v1beta1.PostgresConnection{&m.Spec.Source, &m.Spec.Target} {
		connection.Database = postgresContainer
		connection.Username = postgresContainer
		connection.PasswordSecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: passwordKey,
		}
	}
	return m
}

const (
	// extcondition picks the user rows, above 100, and leaves out the shipped ones,
	// as an extension's condition leaves out the rows its script inserts.
	extConfigShippedRows = "(1, 'shipped 1'), (2, 'shipped 2')"
	extConfigUserRows    = "(101, 'user 101'), (102, 'user 102')"
	extConfigRowsSQL     = "SELECT count(*) || '|' || md5(string_agg(id || ':' || val, ',' ORDER BY id))" +
		" FROM public.e2e_extcfg"
	// Drops the member table with the extension.
	extConfigDropSQL     = "DROP EXTENSION IF EXISTS seg CASCADE"
	largeObjectDigestSQL = "SELECT count(*) || '|' || md5(coalesce(string_agg(oid || ':' || md5(lo_get(oid)), ','" +
		" ORDER BY oid), '')) FROM pg_largeobject_metadata"
	// One object of 320000 bytes, in a database pgcopydb creates on the target.
	allDatabasesLargeObjectSQL = "SELECT lo_from_bytea(0, decode(string_agg(md5(g::text), '' ORDER BY g), 'hex'))" +
		" FROM generate_series(1, 20000) g"
)

// extConfigSetupSQL makes e2e_extcfg a configuration table of seg, as pg_extension_config_dump
// in an extension script would: no extension in the fixture image declares one, and its
// extension directory is read-only.
func extConfigSetupSQL(rows string) string {
	return "CREATE EXTENSION seg;\n" +
		"CREATE TABLE public.e2e_extcfg (id integer PRIMARY KEY, val text NOT NULL);\n" +
		"ALTER EXTENSION seg ADD TABLE public.e2e_extcfg;\n" +
		"UPDATE pg_extension SET extconfig = ARRAY['public.e2e_extcfg'::regclass::oid],\n" +
		"  extcondition = ARRAY['WHERE id > 100'] WHERE extname = 'seg';\n" +
		"INSERT INTO public.e2e_extcfg VALUES " + rows + ";\n"
}

// allDatabasesExtras prints what an all-databases clone copies besides tables in one line,
// so a failed comparison shows every gap at once.
func allDatabasesExtras(cluster, extraDB string) string {
	GinkgoHelper()
	return fmt.Sprintf("app LOs %s; %s LOs %s; postgres e2e_extcfg %s",
		psql(cluster, largeObjectDigestSQL),
		extraDB, psqlDB(cluster, extraDB, largeObjectDigestSQL),
		psqlDB(cluster, postgresContainer, extConfigRowsSQL))
}

// TestAllDatabasesExtensionConfigSQL runs the emulation on a local server: the server accepts
// a superuser's catalog update, and the table ends up a member that extconfig lists.
func TestAllDatabasesExtensionConfigSQL(t *testing.T) {
	run := testPSQL(t)
	t.Cleanup(func() { testQuery(t, run, extConfigDropSQL) })
	// psql commits each statement from stdin alone; without the transaction a failed setup leaves the table behind.
	testQuery(t, run, "BEGIN;\n"+extConfigSetupSQL(extConfigShippedRows+", "+extConfigUserRows)+"COMMIT;\n")
	for _, check := range []struct{ sql, want string }{
		{"SELECT extconfig::regclass[]::text || '|' || extcondition::text FROM pg_extension WHERE extname = 'seg'",
			`{e2e_extcfg}|{"WHERE id > 100"}`},
		{"SELECT count(*) FROM pg_depend WHERE classid = 'pg_class'::regclass" +
			" AND objid = 'public.e2e_extcfg'::regclass AND deptype = 'e'", "1"},
	} {
		if got := testQuery(t, run, check.sql); got != check.want {
			t.Fatalf("%s printed %q, want %q", check.sql, got, check.want)
		}
	}
	if got := testQuery(t, run, extConfigRowsSQL); !strings.HasPrefix(got, "4|") {
		t.Fatalf("seeded %q, want 4|<md5>", got)
	}
}
