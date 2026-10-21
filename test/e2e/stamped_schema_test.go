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
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
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

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// fixtureAnnotation carries the identity a stamped fixture's cleanup checks.
const fixtureAnnotation = "pgcopydb-operator.io/e2e-fixture"

// createStampedSchemas creates schemas on the source as the app role, each
// stamped in the same transaction, and registers the cleanup that deletes m,
// then drops the schemas through dropStampedSchemas.
func createStampedSchemas(
	m *v1beta1.Migration, identity, stamp string, schemas []string, beforeDrop func(schema string) string,
) {
	GinkgoHelper()
	Expect(schemas).NotTo(BeEmpty())
	// Explicit API defaults keep identity checks valid after an uncertain Create response.
	for _, connection := range []*v1beta1.PostgresConnection{&m.Spec.Source, &m.Spec.Target} {
		if connection.Port == 0 {
			connection.Port = defaultPGPort
		}
		if connection.SSLMode == "" {
			connection.SSLMode = "prefer"
		}
	}
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	m.Annotations[fixtureAnnotation] = identity
	names := sqlTextArray(schemas)
	for _, cluster := range []string{sourceCluster, targetCluster} {
		Expect(psql(cluster, "SELECT count(*) FROM pg_namespace WHERE nspname = ANY ("+names+")")).To(Equal("0"))
	}
	// Commit the ownership marker with the schema, including when the client loses the response.
	_, createErr := psqlDBErr(sourceCluster, appDatabase(sourceCluster),
		stampedSchemasSQL(appRole(sourceCluster), schemas, stamp))
	ownerQuery := func(schema string) string {
		return "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = " + sqlLiteral(schema)
	}
	stampQuery := func(schema string) string {
		return "SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = " + sqlLiteral(schema)
	}
	DeferCleanup(func() {
		deleteFixtureMigration(m, identity)
		Expect(dropStampedSchemas(schemas, stamp, beforeDrop, psqlIdempotentScript)).To(Succeed())
	})
	Expect(createErr).NotTo(HaveOccurred(), "failed to create the stamped fixture schemas")
	for _, schema := range schemas {
		Expect(psql(sourceCluster, ownerQuery(schema))).To(Equal(appRole(sourceCluster)))
		Expect(psql(sourceCluster, stampQuery(schema))).To(Equal(stamp))
	}
}

// stampedSchemasSQL creates schemas as role with stamp as their comment, in
// one transaction, so no schema ever exists without its stamp.
func stampedSchemasSQL(role string, schemas []string, stamp string) string {
	var b strings.Builder
	b.WriteString("BEGIN; SET ROLE " + sqlIdent(role) + "; ")
	for _, schema := range schemas {
		b.WriteString("CREATE SCHEMA " + sqlIdent(schema) + "; COMMENT ON SCHEMA " + sqlIdent(schema) +
			" IS " + sqlLiteral(stamp) + "; ")
	}
	b.WriteString("COMMIT")
	return b.String()
}

// dropStampedSchemas runs dropStampedSchemasSQL through run on the source and
// then the target, one exec per side so few execs can hang, and fails unless
// each side's script ran to the end and reports none of schemas left.
func dropStampedSchemas(
	schemas []string, stamp string, beforeDrop func(schema string) string,
	run func(cluster, name, script string) (string, error),
) error {
	for _, side := range []struct{ name, cluster string }{{"source", sourceCluster}, {"target", targetCluster}} {
		call := fmt.Sprintf("script dropping %d stamped schemas on the %s side", len(schemas), side.name)
		left, err := run(side.cluster, call, dropStampedSchemasSQL(appRole(side.cluster), schemas, stamp, beforeDrop))
		if err != nil {
			return fmt.Errorf("%s, cluster %s: %w", call, side.cluster, err)
		}
		// psql exits 0 on an empty stdin or one cut between statements.
		if left != "0" {
			return fmt.Errorf("%s, cluster %s: printed %q, want 0 schemas left", call, side.cluster, left)
		}
	}
	return nil
}

// dropStampedSchemasSQL is a psql script that drops each schema owned by role
// and stamped with stamp, as role after beforeDrop's SQL, one transaction per
// schema, stops at a refused one, and prints how many of schemas are left.
func dropStampedSchemasSQL(role string, schemas []string, stamp string, beforeDrop func(schema string) string) string {
	var b strings.Builder
	// Cascade notices would bury the error that stops the script.
	b.WriteString("SET client_min_messages = warning;\n")
	for _, schema := range schemas {
		name := sqlLiteral(schema)
		// Nothing after the check names a schema it found missing.
		b.WriteString("BEGIN;\nSELECT count(*) = 1 AS present FROM pg_namespace WHERE nspname = " + name +
			" \\gset\n\\if :present\n")
		b.WriteString("DO " + guardQuote(" DECLARE o text; d text; BEGIN"+
			" SELECT pg_get_userbyid(nspowner), obj_description(oid, 'pg_namespace') INTO o, d"+
			" FROM pg_namespace WHERE nspname = "+name+"; IF NOT FOUND THEN RAISE EXCEPTION"+
			" 'schema % vanished during its cleanup', "+name+"; END IF;"+
			" IF o IS DISTINCT FROM "+sqlLiteral(role)+" THEN RAISE EXCEPTION"+
			" 'refusing to drop schema % with another owner: %', "+name+", o; END IF;"+
			" IF d IS DISTINCT FROM "+sqlLiteral(stamp)+" THEN RAISE EXCEPTION"+
			" 'refusing to drop schema % without this fixture marker: %', "+name+", d; END IF;"+
			" END ") + ";\nSET ROLE " + sqlIdent(role) + ";\n")
		if beforeDrop != nil {
			b.WriteString(beforeDrop(schema) + ";\n")
		}
		b.WriteString("DROP SCHEMA " + sqlIdent(schema) + " CASCADE;\nRESET ROLE;\n\\endif\nCOMMIT;\n")
	}
	b.WriteString("SELECT count(*) FROM pg_namespace WHERE nspname = ANY (" + sqlTextArray(schemas) + ");\n")
	return b.String()
}

// guardQuote dollar-quotes body with the first of $guard$, $guard1$, ... that
// body does not contain, so no embedded value ends the quote and hands psql the rest.
func guardQuote(body string) string {
	for i := 0; ; i++ {
		tag := "$guard$"
		if i > 0 {
			tag = "$guard" + strconv.Itoa(i) + "$"
		}
		if strings.Index(body+tag, tag) == len(body) {
			return tag + body + tag
		}
	}
}

// deleteFixtureMigration deletes m only while it still carries identity and
// the fixture's configuration, then waits until its Jobs are gone and its
// pods stopped, so no worker writes into a schema being dropped.
func deleteFixtureMigration(m *v1beta1.Migration, identity string) {
	GinkgoHelper()
	current := &v1beta1.Migration{}
	key := client.ObjectKeyFromObject(m)
	err := k8sClient.Get(ctx, key, current)
	if err != nil {
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "cannot prove whether the fixture Migration exists")
	} else {
		Expect(current.Annotations[fixtureAnnotation]).To(Equal(identity),
			"refusing to delete a Migration with another fixture identity")
		sameConfig := reflect.DeepEqual(current.Spec.Source, m.Spec.Source) &&
			reflect.DeepEqual(current.Spec.Target, m.Spec.Target) &&
			reflect.DeepEqual(current.Spec.Clone, m.Spec.Clone) &&
			reflect.DeepEqual(current.Spec.Verification, m.Spec.Verification)
		Expect(sameConfig).To(BeTrue(), "refusing to delete a Migration with another fixture configuration")
		Expect(requireFeatureMigrationOwnership(current)).To(Succeed())
		if m.UID != "" {
			Expect(current.UID).To(Equal(m.UID), "refusing to delete a replacement Migration")
		}
		Expect(current.UID).NotTo(BeEmpty())
		m.UID = current.UID
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current,
			client.Preconditions{UID: &m.UID}))).To(Succeed())
		Eventually(func(g Gomega) {
			remaining := &v1beta1.Migration{}
			lookupErr := k8sClient.Get(ctx, key, remaining)
			if lookupErr == nil {
				g.Expect(remaining.UID).To(Equal(m.UID), "refusing to stop a replacement Migration")
			}
			g.Expect(apierrors.IsNotFound(lookupErr)).To(BeTrue(), "fixture Migration is still terminating")
		}, 5*time.Minute, time.Second).Should(Succeed())
	}
	Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, &v1beta1.Migration{}))).To(BeTrue(),
		"fixture Migration must be absent before its schemas are dropped")
	Eventually(func(g Gomega) {
		jobs := &batchv1.JobList{}
		g.Expect(k8sClient.List(ctx, jobs, client.InNamespace(nsE2E),
			client.MatchingLabels{migrationLabel: m.Name})).To(Succeed())
		for _, job := range jobs.Items {
			g.Expect(m.UID).NotTo(BeEmpty(), "cannot establish ownership of a remaining fixture Job")
			g.Expect(metav1.IsControlledBy(&job, m)).To(BeTrue(), "refusing to delete a replaced Job")
			g.Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &job, client.Preconditions{UID: &job.UID},
				client.PropagationPolicy(metav1.DeletePropagationForeground)))).To(Succeed())
		}
		pods := &corev1.PodList{}
		g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
			client.MatchingLabels{migrationLabel: m.Name})).To(Succeed())
		for _, pod := range pods.Items {
			g.Expect(pod.Status.Phase).To(BeElementOf(corev1.PodSucceeded, corev1.PodFailed),
				"Migration pod must stop before its fixture is dropped")
		}
		jobCount := len(jobs.Items)
		g.Expect(jobCount).To(BeZero(), "owned Migration Jobs must be removed before dropping the fixture")
	}, 5*time.Minute, time.Second).Should(Succeed())
}

func TestStampedSchemasSQLIsAtomic(t *testing.T) {
	run := testPSQL(t)
	id := fmt.Sprint(time.Now().UnixNano())
	taken, first, second := "stamp_taken_"+id, "stamp_first_"+id, "stamp_second_"+id
	testCoverageSchemas(t, run, taken)
	t.Cleanup(func() {
		testQuery(t, run, "DROP SCHEMA IF EXISTS "+sqlIdent(first)+", "+sqlIdent(second))
	})
	// A superuser would make SET ROLE a no-op and the owner check vacuous.
	role := testCaseOwner
	onDatabase := func(sql string) string {
		return "DO $g$ BEGIN EXECUTE format(" + sqlLiteral(sql) + ", current_database(), " + sqlLiteral(role) + "); END $g$"
	}
	testQuery(t, run, onDatabase("GRANT CREATE ON DATABASE %I TO %I"))
	t.Cleanup(func() { testQuery(t, run, onDatabase("REVOKE CREATE ON DATABASE %I FROM %I")) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := run(ctx, stampedSchemasSQL(role, []string{first, taken}, "stamp:"+id)); err == nil {
		t.Fatal("creating an existing schema succeeded")
	}
	// format renders a missing stamp as empty, where || would hide the whole row.
	found := "SELECT coalesce(string_agg(format('%s %s %s', nspname, pg_get_userbyid(nspowner)," +
		" obj_description(oid, 'pg_namespace')), ',' ORDER BY nspname), '') FROM pg_namespace" +
		" WHERE nspname IN (" + sqlLiteral(first) + ", " + sqlLiteral(second) + ")"
	if got := testQuery(t, run, found); got != "" {
		t.Fatalf("a failed create left %q behind", got)
	}
	testQuery(t, run, stampedSchemasSQL(role, []string{first, second}, "stamp:"+id))
	want := first + " " + role + " stamp:" + id + "," + second + " " + role + " stamp:" + id
	if got := testQuery(t, run, found); got != want {
		t.Fatalf("created %q, want %q", got, want)
	}
}

// TestDropStampedSchemasSQL feeds the cleanup script to the local psql the
// way the suite does, on stdin through psqlIdempotentScriptWith.
func TestDropStampedSchemasSQL(t *testing.T) {
	run := testPSQL(t)
	psqlPath, err := exec.LookPath("psql")
	if err != nil {
		t.Fatal(err)
	}
	// Swap kubectl exec and the in-pod connection for the local psql, keeping the
	// suite's flags; a shell escape the script lets through lands in dir.
	dir := t.TempDir()
	local := func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		flags := args[slices.Index(args, "--")+1+len(psqlCommand(sourceCluster, psqlExecTestDatabase)):]
		cmd := exec.CommandContext(ctx, psqlPath, append([]string{os.Getenv("PGCOPYDB_TEST_PGURI")}, flags...)...)
		cmd.Dir = dir
		return cmd
	}
	id := fmt.Sprint(time.Now().UnixNano())
	stamp := "stamp:" + id
	feed := func(t *testing.T, script string) (string, error) {
		return psqlIdempotentScriptWith(sourceCluster, psqlExecTestDatabase, "test cleanup", script,
			func(string) string { return psqlExecTestPod },
			func(time.Duration) { t.Error("retried an attempt that did not time out") },
			local, 30*time.Second)
	}
	drop := func(t *testing.T, schemas ...string) (string, error) {
		return feed(t, dropStampedSchemasSQL(testCaseOwner, schemas, stamp, unlinkCoverageLargeObjectsSQL))
	}
	// testCoverageSchemas drops what a test keeps, with its large objects.
	stamped := func(t *testing.T, schemas ...string) {
		testCoverageSchemas(t, run, schemas...)
		for _, schema := range schemas {
			testQuery(t, run, "COMMENT ON SCHEMA "+sqlIdent(schema)+" IS "+sqlLiteral(stamp))
		}
	}
	// largeObjects lists two large objects in schema as a coverage case does,
	// as their owner, and returns their OIDs.
	largeObjects := func(t *testing.T, schema string) string {
		table := sqlIdent(schema) + ".cov_large_objects"
		return testQuery(t, run, "SET ROLE "+testCaseOwner+";\nCREATE TABLE "+table+" (name text, lo oid);\n"+
			"INSERT INTO "+table+" SELECT 'blob' || g, lo_from_bytea(0, 'x') FROM generate_series(1, 2) g;\n"+
			"SELECT string_agg(lo::text, ',') FROM "+table)
	}
	liveObjects := func(t *testing.T, oids string) string {
		return testQuery(t, run, "SELECT count(*) FROM pg_largeobject_metadata WHERE oid = ANY ('{"+oids+"}'::oid[])")
	}
	present := func(t *testing.T, schemas ...string) string {
		names := sqlTextArray(schemas)
		return testQuery(t, run, "SELECT coalesce(string_agg(nspname, ',' ORDER BY array_position("+names+
			", nspname::text)), '') FROM pg_namespace WHERE nspname = ANY ("+names+")")
	}

	t.Run("drops each stamped schema with its large objects, and a rerun changes nothing", func(t *testing.T) {
		plain, objects, bystander := "drop_plain_"+id, "drop_objects_"+id, "drop_bystander_"+id
		stamped(t, plain, objects, bystander)
		oids, bystanderOIDs := largeObjects(t, objects), largeObjects(t, bystander)
		// The second run is what a retry after a hung exec does.
		for attempt := 1; attempt <= 2; attempt++ {
			if left, err := drop(t, plain, objects); err != nil || left != "0" {
				t.Fatalf("run %d: left=%q err=%v, want 0 left", attempt, left, err)
			}
			if got := present(t, plain, objects, bystander); got != bystander {
				t.Fatalf("run %d left schemas %q, want only %s", attempt, got, bystander)
			}
			if got := liveObjects(t, oids); got != "0" {
				t.Fatalf("run %d left %s of the dropped schema's large objects", attempt, got)
			}
			if got := liveObjects(t, bystanderOIDs); got != "2" {
				t.Fatalf("run %d left %s of the bystander's 2 large objects", attempt, got)
			}
		}
	})

	// A schema missing at its check gets neither beforeDrop nor DROP, so one that
	// appears after the check survives, and the count reports it.
	t.Run("skips a schema missing at its check", func(t *testing.T) {
		missing, after := "drop_missing_"+id, "drop_after_missing_"+id
		stamped(t, after)
		t.Cleanup(func() { testQuery(t, run, "DROP SCHEMA IF EXISTS "+sqlIdent(missing)) })
		script := dropStampedSchemasSQL(testCaseOwner, []string{missing, after}, stamp, func(schema string) string {
			if schema == missing {
				return "SELECT 1/0"
			}
			return unlinkCoverageLargeObjectsSQL(schema)
		})
		check := "nspname = " + sqlLiteral(missing) + " \\gset\n"
		appear := strings.Replace(script, check, check+"CREATE SCHEMA "+sqlIdent(missing)+";\n", 1)
		if appear == script {
			t.Fatalf("no presence check for %s in:\n%s", missing, script)
		}
		left, err := feed(t, appear)
		if err != nil || left != "1" {
			t.Fatalf("left=%q err=%v, want the schema that appeared counted", left, err)
		}
		if got := present(t, missing, after); got != missing {
			t.Fatalf("left schemas %q, want only %s", got, missing)
		}
	})

	// The guard is the only thing between the cleanup and a schema it does not own.
	for _, tt := range []struct{ name, stamp, spoil, reason string }{
		{name: "another owner", spoil: "ALTER SCHEMA %s OWNER TO " + coverageReaderRole,
			reason: "with another owner: " + coverageReaderRole},
		{name: "another stamp", spoil: "COMMENT ON SCHEMA %s IS 'stamp:other'",
			reason: "without this fixture marker: stamp:other"},
		{name: "stamp suffix", spoil: "COMMENT ON SCHEMA %s IS " + sqlLiteral(stamp+"x"),
			reason: "without this fixture marker: " + stamp + "x"},
		{name: "padded stamp", spoil: "COMMENT ON SCHEMA %s IS " + sqlLiteral(stamp+" "),
			reason: "without this fixture marker: " + stamp + " \n"},
		{name: "no stamp", spoil: "COMMENT ON SCHEMA %s IS NULL",
			reason: "without this fixture marker: <NULL>"},
	} {
		t.Run("keeps a schema with "+tt.name, func(t *testing.T) {
			key := strings.ReplaceAll(tt.name, " ", "_") + "_" + id
			before, spoiled, after := "drop_before_"+key, "drop_"+key, "drop_after_"+key
			stamped(t, before, spoiled, after)
			largeObjects(t, before)
			oids := largeObjects(t, spoiled)
			testQuery(t, run, fmt.Sprintf(tt.spoil, sqlIdent(spoiled)))
			_, err := drop(t, before, spoiled, after)
			if want := "refusing to drop schema " + spoiled + " " + tt.reason; err == nil ||
				!strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want %q", err, want)
			}
			// Dropping before cascades to its table; that notice must not bury the refusal.
			if strings.Contains(err.Error(), "NOTICE") {
				t.Fatalf("err = %v, want no notices", err)
			}
			// The cleanup stops at the refused schema: earlier ones are gone, it and later
			// ones stay, and the error keeps it from reaching the target side.
			if got, want := present(t, before, spoiled, after), spoiled+","+after; got != want {
				t.Fatalf("left schemas %q, want %q", got, want)
			}
			if got := liveObjects(t, oids); got != "2" {
				t.Fatalf("left %s of the refused schema's 2 large objects", got)
			}
		})
	}

	// A value that ended the guard's dollar quote would hand psql the rest of it,
	// and psql runs `\!` as a shell command; "#" keeps that command to "touch escaped".
	t.Run("keeps the guard quoted whatever the role and stamp contain", func(t *testing.T) {
		// The stamp's $guard1$ makes the tag step past two candidates.
		role, tagged := "r"+id+`$guard$\! touch escaped #`, "stamp$guard1$"+id
		schemas := []string{"drop_tag_a_" + id, "drop_tag_b_" + id}
		testQuery(t, run, "CREATE ROLE "+sqlIdent(role)+" NOLOGIN")
		t.Cleanup(func() { testQuery(t, run, "DROP ROLE IF EXISTS "+sqlIdent(role)) })
		for _, schema := range schemas {
			t.Cleanup(func() { testQuery(t, run, "DROP SCHEMA IF EXISTS "+sqlIdent(schema)+" CASCADE") })
			testQuery(t, run, "CREATE SCHEMA "+sqlIdent(schema)+" AUTHORIZATION "+sqlIdent(role)+
				";\nCOMMENT ON SCHEMA "+sqlIdent(schema)+" IS "+sqlLiteral(tagged))
		}
		left, err := feed(t, dropStampedSchemasSQL(role, schemas, tagged, unlinkCoverageLargeObjectsSQL))
		if _, statErr := os.Stat(filepath.Join(dir, "escaped")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("psql ran the shell escape in the role (stat: %v)", statErr)
		}
		if err != nil || left != "0" {
			t.Fatalf("left=%q err=%v, want 0 left", left, err)
		}
	})

	// psql exits 0 on these, so only the count tells them from a finished cleanup.
	t.Run("an empty or cut script does not report 0", func(t *testing.T) {
		kept := "drop_kept_" + id
		stamped(t, kept)
		script := dropStampedSchemasSQL(testCaseOwner, []string{kept}, stamp, unlinkCoverageLargeObjectsSQL)
		for _, cut := range []string{"", script[:strings.Index(script, "COMMIT;")]} {
			if left, err := feed(t, cut); err != nil || left == "0" {
				t.Fatalf("left=%q err=%v for a cut script, want no 0", left, err)
			}
		}
		if got := present(t, kept); got != kept {
			t.Fatalf("left schemas %q, want %s", got, kept)
		}
	})
}

func TestDropStampedSchemasRunsSourceThenTargetToTheEnd(t *testing.T) {
	type result struct {
		left string
		err  error
	}
	clean := result{left: "0"}
	for _, tt := range []struct {
		name         string
		results      map[string]result
		wantClusters []string
		wantErr      string
	}{
		{name: "both sides clean", results: map[string]result{sourceCluster: clean, targetCluster: clean},
			wantClusters: []string{sourceCluster, targetCluster}},
		{name: "empty output", results: map[string]result{sourceCluster: {}},
			wantClusters: []string{sourceCluster}, wantErr: "on the source side, cluster " + sourceCluster},
		{name: "schemas left on the target", results: map[string]result{sourceCluster: clean, targetCluster: {left: "2"}},
			wantClusters: []string{sourceCluster, targetCluster}, wantErr: "on the target side, cluster " + targetCluster},
		{name: "refused on the source", results: map[string]result{sourceCluster: {err: errors.New("refusing")}},
			wantClusters: []string{sourceCluster}, wantErr: "on the source side, cluster " + sourceCluster + ": refusing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var clusters []string
			err := dropStampedSchemas([]string{"a", "b"}, "stamp", nil, func(cluster, name, script string) (string, error) {
				clusters = append(clusters, cluster)
				if !strings.Contains(name, "2 stamped schemas") || !strings.Contains(script, "nspname = 'b'") {
					t.Errorf("call %q or its script does not cover both schemas", name)
				}
				r, ok := tt.results[cluster]
				if !ok {
					return clean.left, nil // the cluster check below reports the extra run
				}
				return r.left, r.err
			})
			if !slices.Equal(clusters, tt.wantClusters) {
				t.Fatalf("ran on %v, want %v", clusters, tt.wantClusters)
			}
			if tt.wantErr == "" && err != nil || tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
