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
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

const (
	// deferIndexesRows is big's size; its last id waits on the copy gate.
	deferIndexesRows = 200000
	// deferIndexesSmallRows is the size of small and bare, which copy while big waits.
	deferIndexesSmallRows = 1000
	// deferIndexesCopyLockKey holds big's last row and deferIndexesIndexLockKey every row of big_key's
	// build; the split spec holds 581 and the all-databases specs 582 and 583.
	deferIndexesCopyLockKey       = "584"
	deferIndexesCopyGateSession   = "e2e_defer_copy_gate"
	deferIndexesCopyLockSQL       = "SELECT pg_advisory_xact_lock(" + deferIndexesCopyLockKey + ")"
	deferIndexesIndexLockKey      = "585"
	deferIndexesIndexGateSession  = "e2e_defer_index_gate"
	deferIndexesIndexLockSQL      = "SELECT pg_advisory_xact_lock(" + deferIndexesIndexLockKey + ")"
	deferIndexesCopyWorker        = "copy worker"
	deferIndexesCreateIndexWorker = "create index worker"
)

// A run without deferIndexes connects its index workers at start and builds small's index while big
// still copies (the fork's control scenario, not run here). Big's COPY waits at its last row and then
// big_key's build waits, so each phase is held while the spec looks at the target.
var _ = Describe("Deferred index builds", SpecPriority(2), func() {
	BeforeEach(resetMigrationPair)

	It("builds no index while a table copies, reports the index phase as Finalizing, "+
		"and builds every index after the last row", func() {
		identity := fmt.Sprint(time.Now().UnixNano())
		name, schema := "e2e-defer-indexes-"+identity, "e2e_defer_indexes_"+identity
		rows := strconv.Itoa(deferIndexesRows)
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			DeferIndexes: true,
			Filters:      &v1beta1.Filters{IncludeOnlySchemas: []string{schema}},
		})
		createStampedSchemas(m, identity, "pgcopydb-e2e-defer-indexes:"+identity, []string{schema}, nil)

		By("seeding big, whose last row and index build wait on gates, small with an index, and bare without")
		psqlBulk(sourceCluster, asSourceAppRole()+deferIndexesSeedSQL(schema))
		Expect(psql(sourceCluster, "SELECT count(*) FROM "+sqlIdent(schema)+".big")).To(Equal(rows))

		By("holding both gates on the target, so big's COPY waits at its last row")
		releaseCopy := holdOnTarget(deferIndexesCopyGateSession, deferIndexesCopyLockSQL,
			deferIndexesHeldSQL(deferIndexesCopyGateSession, deferIndexesCopyLockKey))
		releaseIndex := holdOnTarget(deferIndexesIndexGateSession, deferIndexesIndexLockSQL,
			deferIndexesHeldSQL(deferIndexesIndexGateSession, deferIndexesIndexLockKey))
		create(m)
		reportResumedAttempts(m)
		waitWorkerRunning(name, 1)
		// Timing-free: the operator passed the flag, whatever the target shows below.
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-run-1"}, job)).To(Succeed())
		Expect(job.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--defer-indexes"))
		waitAttemptOneHeld(m, deferIndexesCopyStateSQL(schema),
			fmt.Sprintf("1|%d|%d", deferIndexesSmallRows, deferIndexesSmallRows),
			"pgcopydb sessions waiting on the copy gate|rows of small|rows of bare")

		By("expecting no index or VACUUM work on the target while big copies")
		Expect(psql(targetCluster, deferIndexesSessionsSQL(deferIndexesCreateIndexWorker))).To(Equal("0"),
			"pgcopydb create index workers on the target while big copies")
		Expect(psql(targetCluster, deferIndexesVacuumsSQL(schema))).To(Equal("0"),
			"manual VACUUMs of small and bare on the target while big copies")
		Expect(psql(targetCluster, "SELECT to_regclass("+sqlLiteral(sqlIdent(schema)+".small_val")+") IS NULL")).
			To(Equal("t"), "small_val is absent on the target while big copies")
		cur := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), cur)).To(Succeed())
		Expect(cur.Status.Phase).To(Equal(v1beta1.PhaseCloning))

		By("reading the marker on the target and releasing the copy gate")
		marker := psql(targetCluster, "SELECT pg_current_xact_id()")
		releaseCopy()

		By("expecting big_key's build to wait on the index gate, with no copy worker left and phase Finalizing")
		Eventually(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), cur)).To(Succeed())
			if cur.Status.Attempts > 1 || cur.Status.Phase == v1beta1.PhaseFailed ||
				cur.Status.Phase == v1beta1.PhaseCompleted {
				StopTrying(fmt.Sprintf("attempt 1 ended before the index gate held it: attempt %d at phase %q",
					cur.Status.Attempts, cur.Status.Phase)).Now()
			}
			state, err := psqlDBErr(targetCluster, appDatabase(targetCluster), "SELECT ("+deferIndexesStateSQL(schema)+
				") || '|' || ("+deferIndexesSessionsSQL(deferIndexesCopyWorker)+")")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(state).To(Equal("1|0"), "pgcopydb index builds waiting on the index gate|pgcopydb copy workers")
			g.Expect(cur.Status.Phase).To(Equal(v1beta1.PhaseFinalizing))
		}, 5*time.Minute, time.Second).Should(Succeed())

		By("releasing the index gate and comparing the indexes with the marker")
		releaseIndex()
		waitCompleted(name, nsE2E)
		Expect(psql(targetCluster, deferIndexesAfterMarkerSQL(schema, marker))).To(Equal("2|true"),
			"small_val and big_key exist and are newer than the marker read before the last row")
		Expect(psql(targetCluster, "SELECT count(*) FROM "+sqlIdent(schema)+".big")).To(Equal(rows))
	})
})

// deferIndexesSeedSQL creates schema.big, whose CHECK waits on the copy gate for its last id and whose
// big_key index waits on the index gate for every row, small with small_val, and bare with no index.
func deferIndexesSeedSQL(schema string) string {
	return fmt.Sprintf(`CREATE TABLE %[1]s.big (id integer PRIMARY KEY, payload text NOT NULL);
INSERT INTO %[1]s.big SELECT g, repeat(md5(g::text), 2) FROM generate_series(1, %[2]d) g;
CREATE FUNCTION %[1]s.gate_copy(id integer) RETURNS boolean LANGUAGE plpgsql AS $gate$
BEGIN
  IF id = %[2]d THEN PERFORM pg_advisory_xact_lock_shared(%[3]s); END IF;
  RETURN true;
END $gate$;
ALTER TABLE %[1]s.big ADD CONSTRAINT big_gate CHECK (%[1]s.gate_copy(id));
CREATE FUNCTION %[1]s.gate_index(id integer) RETURNS integer LANGUAGE plpgsql IMMUTABLE
  AS $gate$ BEGIN PERFORM pg_advisory_xact_lock_shared(%[4]s); RETURN id; END $gate$;
CREATE INDEX big_key ON %[1]s.big (%[1]s.gate_index(id));
CREATE TABLE %[1]s.small (id integer PRIMARY KEY, val integer NOT NULL);
INSERT INTO %[1]s.small SELECT g, g %% 100 FROM generate_series(1, %[5]d) g;
CREATE INDEX small_val ON %[1]s.small (val);
CREATE TABLE %[1]s.bare (id integer NOT NULL, payload text);
INSERT INTO %[1]s.bare SELECT g, md5(g::text) FROM generate_series(1, %[5]d) g;
ANALYZE %[1]s.big, %[1]s.small, %[1]s.bare;
`, sqlIdent(schema), deferIndexesRows, deferIndexesCopyLockKey, deferIndexesIndexLockKey, deferIndexesSmallRows)
}

// deferIndexesHeldSQL prints 1 once the session named session holds the advisory lock key.
func deferIndexesHeldSQL(session, key string) string {
	return "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE a.application_name = '" + session + "' AND l.locktype = 'advisory'" +
		" AND l.objid = " + key + " AND l.granted"
}

// deferIndexesCopyStateSQL prints waiting|small|bare: the pgcopydb sessions in this database that wait on
// the copy gate, and the committed rows of small and bare. It reads no row of big, which its COPY locks.
func deferIndexesCopyStateSQL(schema string) string {
	return "SELECT (SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE l.locktype = 'advisory' AND l.objid = " + deferIndexesCopyLockKey + " AND NOT l.granted" +
		" AND a.datname = current_database() AND a.application_name LIKE '" + progress.PgcopydbAppNameLike + "')" +
		" || '|' || (SELECT count(*) FROM " + sqlIdent(schema) + ".small)" +
		" || '|' || (SELECT count(*) FROM " + sqlIdent(schema) + ".bare)"
}

// deferIndexesSessionsSQL counts the pgcopydb sessions in this database whose name contains worker.
func deferIndexesSessionsSQL(worker string) string {
	return "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE '" + progress.PgcopydbAppNameLike + "'" +
		" AND application_name LIKE '%" + worker + "%' AND datname = current_database()"
}

// deferIndexesVacuumsSQL prints the manual VACUUMs of small and bare (autovacuum counts apart), or -1 when
// neither exists. A vacuum worker connects only once it has retitled itself after its table, so no session
// is named vacuum worker and the spec counts VACUUMs instead.
func deferIndexesVacuumsSQL(schema string) string {
	return "SELECT coalesce(sum(vacuum_count), -1) FROM pg_stat_user_tables" +
		" WHERE schemaname = " + sqlLiteral(schema) + " AND relname IN ('small', 'bare')"
}

// deferIndexesStateSQL counts the pgcopydb create index workers in this database whose build of an
// index on a table in schema waits on the index gate.
func deferIndexesStateSQL(schema string) string {
	return "SELECT count(*) FROM pg_stat_progress_create_index p" +
		" JOIN pg_locks l ON l.pid = p.pid JOIN pg_stat_activity a ON a.pid = p.pid" +
		" JOIN pg_class c ON c.oid = p.relid" +
		" WHERE c.relnamespace = " + sqlLiteral(sqlIdent(schema)) + "::regnamespace" +
		" AND l.locktype = 'advisory' AND l.objid = " + deferIndexesIndexLockKey + " AND NOT l.granted" +
		" AND a.datname = current_database() AND a.application_name LIKE '" + progress.PgcopydbAppNameLike + "'" +
		" AND a.application_name LIKE '%" + deferIndexesCreateIndexWorker + "%'"
}

// deferIndexesAfterMarkerSQL prints count|all for the indexes in schema that back no constraint: how many
// there are, and whether each is newer than marker, an xid8. ADD CONSTRAINT ... USING INDEX rewrites the
// pg_class row of the others.
func deferIndexesAfterMarkerSQL(schema, marker string) string {
	return "SELECT count(*) || '|' || bool_and(age(c.xmin) < age(xid(" + sqlLiteral(marker) + "::xid8)))" +
		" FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid" +
		" WHERE c.relnamespace = " + sqlLiteral(sqlIdent(schema)) + "::regnamespace" +
		" AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = c.oid)"
}

// TestDeferIndexesSQL runs the spec's SQL on a local server: the marker check tells indexes built before
// the marker from those built after, the copy gate holds a write of big's last id alone, and the state
// counts a pgcopydb index build that waits on the index gate but not the session that holds the gate.
func TestDeferIndexesSQL(t *testing.T) {
	run := testPSQL(t)
	schema := fmt.Sprintf("defer_indexes_%d", time.Now().UnixNano())
	ns := sqlIdent(schema)
	asOwner := "SET ROLE " + testCaseOwner + ";\n"
	testCoverageSchemas(t, run, schema)
	vacuums := deferIndexesVacuumsSQL(schema)
	if got := testQuery(t, run, vacuums); got != "-1" {
		t.Fatalf("manual VACUUMs of small and bare %q before the seed, want -1", got)
	}
	testQuery(t, run, asOwner+deferIndexesSeedSQL(schema))
	idle := fmt.Sprintf("0|%d|%d", deferIndexesSmallRows, deferIndexesSmallRows)
	held := fmt.Sprintf("1|%d|%d", deferIndexesSmallRows, deferIndexesSmallRows)
	if got := testQuery(t, run, deferIndexesCopyStateSQL(schema)); got != idle {
		t.Fatalf("copy state %q after the seed, want %q", got, idle)
	}
	// The seed runs ANALYZE, not VACUUM; a finished VACUUM counts at once for the next session.
	if got := testQuery(t, run, vacuums); got != "0" {
		t.Fatalf("manual VACUUMs of small and bare %q after the seed, want 0", got)
	}
	testQuery(t, run, "VACUUM ANALYZE "+ns+".small, "+ns+".bare")
	if got := testQuery(t, run, vacuums); got != "2" {
		t.Fatalf("manual VACUUMs of small and bare %q after VACUUM ANALYZE of both, want 2", got)
	}

	// The primary keys stay older than the marker throughout: the check passes over them.
	afterMarker := deferIndexesAfterMarkerSQL(schema, testQuery(t, run, "SELECT pg_current_xact_id()"))
	if got := testQuery(t, run, afterMarker); got != "2|false" {
		t.Fatalf("indexes after the marker %q with small_val and big_key from the seed, want 2|false", got)
	}
	testQuery(t, run, asOwner+"DROP INDEX "+ns+".small_val;\nCREATE INDEX small_val ON "+ns+".small (val)")
	if got := testQuery(t, run, afterMarker); got != "2|false" {
		t.Fatalf("indexes after the marker %q with big_key from the seed, want 2|false", got)
	}
	testQuery(t, run, asOwner+"DROP INDEX "+ns+".big_key;\nCREATE INDEX big_key ON "+ns+".big ("+ns+".gate_index(id))")
	if got := testQuery(t, run, afterMarker); got != "2|true" {
		t.Fatalf("indexes after the marker %q with small_val and big_key rebuilt, want 2|true", got)
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	copyOf := ns + ".big_copy"
	testQuery(t, run, asOwner+"CREATE TABLE "+copyOf+" (LIKE "+ns+".big INCLUDING CONSTRAINTS)")
	copyRow := func(id int) string { return fmt.Sprintf("COPY %s FROM STDIN;\n%d\tx\n\\.\n", copyOf, id) }
	copyHolder := testBackground(bgCtx, run, holdSQL(deferIndexesCopyGateSession, deferIndexesCopyLockSQL))
	t.Cleanup(func() {
		testQuery(t, run, endSessionSQL(deferIndexesCopyGateSession))
		<-copyHolder
	})
	testWaitFor(t, run, deferIndexesHeldSQL(deferIndexesCopyGateSession, deferIndexesCopyLockKey), "1")
	testQuery(t, run, "SET statement_timeout = '5s';\n"+copyRow(deferIndexesRows-1))
	writer := testBackground(bgCtx, run, "SET application_name = 'pgcopydb[1] copy worker';\n"+copyRow(deferIndexesRows))
	testWaitFor(t, run, deferIndexesCopyStateSQL(schema), held)
	if got := testQuery(t, run, deferIndexesSessionsSQL(deferIndexesCopyWorker)); got != "1" {
		t.Fatalf("counted %s copy workers while the COPY waits, want 1", got)
	}
	if got := testQuery(t, run, deferIndexesSessionsSQL(deferIndexesCreateIndexWorker)); got != "0" {
		t.Fatalf("counted %s create index workers while the COPY waits, want 0", got)
	}
	if got := testQuery(t, run, endSessionSQL(deferIndexesCopyGateSession)); got != "1" {
		t.Fatalf("ended %s sessions holding the copy gate, want 1", got)
	}
	if err := <-writer; err != nil {
		t.Fatalf("the COPY of the gated id failed after the release: %v", err)
	}
	testWaitFor(t, run, deferIndexesCopyStateSQL(schema), idle)
	testWaitFor(t, run, deferIndexesSessionsSQL(deferIndexesCopyWorker), "0")

	indexHolder := testBackground(bgCtx, run, holdSQL(deferIndexesIndexGateSession, deferIndexesIndexLockSQL))
	t.Cleanup(func() {
		testQuery(t, run, endSessionSQL(deferIndexesIndexGateSession))
		<-indexHolder
	})
	testWaitFor(t, run, deferIndexesHeldSQL(deferIndexesIndexGateSession, deferIndexesIndexLockKey), "1")
	state := deferIndexesStateSQL(schema)
	if got := testQuery(t, run, deferIndexesSessionsSQL(deferIndexesCreateIndexWorker)); got != "0" {
		t.Fatalf("counted %s create index workers before the build, want 0", got)
	}
	build := testBackground(bgCtx, run, "SET application_name = 'pgcopydb[1] create index worker';\n"+
		"CREATE INDEX big_copy_key ON "+copyOf+" ("+ns+".gate_index(id));\n")
	testWaitFor(t, run, state, "1")
	if got := testQuery(t, run, deferIndexesSessionsSQL(deferIndexesCreateIndexWorker)); got != "1" {
		t.Fatalf("counted %s create index workers while the build waits, want 1", got)
	}
	if got := testQuery(t, run, deferIndexesSessionsSQL(deferIndexesCopyWorker)); got != "0" {
		t.Fatalf("counted %s copy workers while the build waits, want 0", got)
	}
	if got := testQuery(t, run, endSessionSQL(deferIndexesIndexGateSession)); got != "1" {
		t.Fatalf("ended %s sessions holding the index gate, want 1", got)
	}
	if err := <-build; err != nil {
		t.Fatalf("the gated index build failed after the release: %v", err)
	}
	testWaitFor(t, run, state, "0")
}
