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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/resource"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

const (
	// allDatabasesResumeRows split under 1Mi into four parts, as splitResumeRows do.
	allDatabasesResumeRows = 200000
	// allDatabasesResumeLockKey is the advisory lock the index build waits on; the split spec holds 581.
	allDatabasesResumeLockKey     = "582"
	allDatabasesResumeGateSession = "e2e_alldb_gate"
	allDatabasesResumeLockSQL     = "SELECT pg_advisory_xact_lock(" + allDatabasesResumeLockKey + ")"
	allDatabasesResumeHeldSQL     = "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE a.application_name = '" + allDatabasesResumeGateSession + "' AND l.locktype = 'advisory'" +
		" AND l.objid = " + allDatabasesResumeLockKey + " AND l.granted"
)

// An all-databases retry after a kill in a split table's index build failed every attempt
// on `relation "..." already exists`, and one from a source with a role whose name needs
// quoting failed on `role "..." already exists`. SpecPriority(3): it repeats the split spec's kill and retries.
var _ = Describe("All databases resume", SpecPriority(3), func() {
	BeforeEach(requireCNPGFixtures)

	It("converges when a retry resumes a clone killed while a split table's index was built", func() {
		identity := fmt.Sprint(time.Now().UnixNano())
		name, schema := "e2e-alldb-resume-"+identity, "e2e_alldb_resume_"+identity
		table := sqlIdent(schema) + ".big"
		role := "E2E-Role-" + identity
		roleCount := "SELECT count(*) FROM pg_roles WHERE rolname = " + sqlLiteral(role)
		// Registered first, so it runs last: no attempt is left that could create these again.
		DeferCleanup(func() {
			for _, cluster := range []string{sourceCluster, targetCluster} {
				psqlDB(cluster, postgresContainer, "DROP SCHEMA IF EXISTS "+sqlIdent(schema))
				psqlDB(cluster, postgresContainer, "DROP ROLE IF EXISTS "+sqlIdent(role))
			}
		})
		threshold := resource.MustParse("1Mi")
		// The filter keeps the seed out of the clone, so the kill lands in this table's index build alone.
		m := allDatabasesMigration(name, v1beta1.CloneOptions{SplitTablesLargerThan: &threshold, SplitMaxParts: 4,
			Filters: &v1beta1.Filters{IncludeOnlySchemas: []string{schema}}})
		// Issue 88 can cost one more attempt on any resume, so the budget goes up.
		m.Spec.BackoffLimit = 5
		createStampedSchemas(m, identity, "pgcopydb-e2e-alldb-resume:"+identity, []string{schema}, nil)

		By("seeding a split table with an index that waits on the gate, and a role whose name needs quoting")
		psqlBulk(sourceCluster, asSourceAppRole()+allDatabasesResumeSeedSQL(schema))
		want := psql(sourceCluster, allDatabasesResumeRowsSQL(table))
		Expect(want).To(HavePrefix(fmt.Sprintf("%d|%d|", allDatabasesResumeRows, allDatabasesResumeRows)))
		defs := psql(sourceCluster, allDatabasesResumeDefsSQL(table))
		Expect(defs).To(Equal(allDatabasesResumeDefs(schema)))
		// pgcopydb applies includeOnlySchemas in every database and fails every attempt
		// on a schema one of them lacks, so postgres gets the schema too, empty.
		psqlDB(sourceCluster, postgresContainer, "CREATE SCHEMA "+sqlIdent(schema))
		psqlDB(sourceCluster, postgresContainer, "CREATE ROLE "+sqlIdent(role)+" NOLOGIN")
		Expect(psqlDB(sourceCluster, postgresContainer, roleCount)).To(Equal("1"))

		By("holding the gate on the target, so an index build of the split table waits")
		release := holdOnTarget(allDatabasesResumeGateSession, allDatabasesResumeLockSQL, allDatabasesResumeHeldSQL)
		create(m)
		reportResumedAttempts(m)
		waitWorkerRunning(name, 1)
		waitAttemptOneHeld(m, allDatabasesResumeStateSQL(schema), fmt.Sprintf("1|%d|1|0", allDatabasesResumeRows),
			"pgcopydb index builds waiting on the gate|committed rows|valid primary key index|primary key constraint")

		By("deleting the attempt-1 worker pod while the index build waits")
		deleteAttemptOneWorker(name)

		By("releasing the gate, so the killed run's sessions end and the retry gate passes")
		// Not earlier: a worker still alive would build the index and finish attempt 1.
		release()
		released := time.Now()
		waitGatePassed(name, 2)
		AddReportEntry("gate hold after release", time.Since(released).Round(time.Second).String())
		completed := waitCompleted(name, nsE2E)
		AddReportEntry("attempts after the worker deletion", completed.Status.Attempts)
		Expect(completed.Status.Attempts).To(BeNumerically(">=", 2))
		// A start-over builds every index again and would pass on any runner.
		expectResumedRetries(name, completed.Status.Attempts, "--all-databases", "--resume", "--not-consistent")

		By("comparing the split table, its indexes and constraints, and the role on both sides")
		Expect(psql(targetCluster, allDatabasesResumeRowsSQL(table))).To(Equal(want),
			"count(*)|count(DISTINCT id)|md5 of the split table on the target after the resumed clone")
		Expect(psql(targetCluster, allDatabasesResumeDefsSQL(table))).To(Equal(defs),
			"indexes and constraints of the split table on the target after the resumed clone")
		Expect(psqlDB(targetCluster, postgresContainer, roleCount)).To(Equal("1"),
			"the role %s on the target after the resumed clone", role)
	})
})

// allDatabasesResumeSeedSQL creates schema.big with an expression index whose
// function waits on the gate for every row, so the target's build of that index
// waits once the parts are copied, while big_pkey builds beside it.
func allDatabasesResumeSeedSQL(schema string) string {
	return fmt.Sprintf(`CREATE TABLE %[1]s.big (id integer PRIMARY KEY, payload text NOT NULL);
INSERT INTO %[1]s.big SELECT g, repeat(md5(g::text), 2) FROM generate_series(1, %[2]d) g;
CREATE FUNCTION %[1]s.gate_key(id integer) RETURNS integer LANGUAGE plpgsql IMMUTABLE
  AS $gate$ BEGIN PERFORM pg_advisory_xact_lock_shared(%[3]s); RETURN id; END $gate$;
CREATE INDEX big_gate ON %[1]s.big (%[1]s.gate_key(id));
ANALYZE %[1]s.big;
`, sqlIdent(schema), allDatabasesResumeRows, allDatabasesResumeLockKey)
}

// allDatabasesResumeStateSQL prints waiting|rows|pkey|constraint for schema.big: the pgcopydb
// index builds that wait on the gate, the committed rows, the valid primary key index, and the
// primary key constraint (contype 'p' keeps PostgreSQL 18's NOT NULL constraints out).
func allDatabasesResumeStateSQL(schema string) string {
	table := sqlLiteral(sqlIdent(schema) + ".big")
	return "SELECT (SELECT count(*) FROM pg_stat_progress_create_index p" +
		" JOIN pg_locks l ON l.pid = p.pid JOIN pg_stat_activity a ON a.pid = p.pid" +
		" WHERE p.relid = to_regclass(" + table + ") AND l.locktype = 'advisory'" +
		" AND l.objid = " + allDatabasesResumeLockKey + " AND NOT l.granted" +
		" AND a.application_name LIKE '" + progress.PgcopydbAppNameLike + "')" +
		" || '|' || (SELECT count(*) FROM " + sqlIdent(schema) + ".big)" +
		" || '|' || (SELECT count(*) FROM pg_index WHERE indexrelid = to_regclass(" +
		sqlLiteral(sqlIdent(schema)+".big_pkey") + ") AND indisvalid)" +
		" || '|' || (SELECT count(*) FROM pg_constraint WHERE conrelid = to_regclass(" + table + ") AND contype = 'p')"
}

// allDatabasesResumeRowsSQL prints count(*)|count(DISTINCT id)|md5 of every row of table.
func allDatabasesResumeRowsSQL(table string) string {
	return "SELECT count(*) || '|' || count(DISTINCT id) || '|' ||" +
		" md5(string_agg(id || ':' || payload, ',' ORDER BY id)) FROM " + table
}

// allDatabasesResumeDefsSQL prints the definitions of table's indexes and of its
// constraints but NOT NULL ones, in byte order.
func allDatabasesResumeDefsSQL(table string) string {
	return "SELECT string_agg(def, '; ' ORDER BY def COLLATE \"C\") FROM (" +
		"SELECT pg_get_indexdef(indexrelid) AS def FROM pg_index WHERE indrelid = " + sqlLiteral(table) + "::regclass" +
		" UNION ALL SELECT conname || ' ' || pg_get_constraintdef(oid) FROM pg_constraint" +
		" WHERE conrelid = " + sqlLiteral(table) + "::regclass AND contype <> 'n') d"
}

// allDatabasesResumeDefs is what allDatabasesResumeDefsSQL prints for the seeded table, so
// an empty or broken query cannot pass by matching an empty target.
func allDatabasesResumeDefs(schema string) string {
	return fmt.Sprintf("CREATE INDEX big_gate ON %[1]s.big USING btree (%[1]s.gate_key(id)); "+
		"CREATE UNIQUE INDEX big_pkey ON %[1]s.big USING btree (id); big_pkey PRIMARY KEY (id)",
		quoteIdentIfNeeded(schema))
}

// TestAllDatabasesResumeSQL runs the spec's SQL on a local server: the gate holds an
// index build of the seeded table, and the state counts that pgcopydb build until it ends.
func TestAllDatabasesResumeSQL(t *testing.T) {
	run := testPSQL(t)
	schema := fmt.Sprintf("alldb_resume_%d", time.Now().UnixNano())
	table := sqlIdent(schema) + ".big"
	testCoverageSchemas(t, run, schema)
	testQuery(t, run, "SET ROLE "+testCaseOwner+";\n"+allDatabasesResumeSeedSQL(schema))
	rows := strconv.Itoa(allDatabasesResumeRows)
	if got := testQuery(t, run, allDatabasesResumeRowsSQL(table)); !strings.HasPrefix(got, rows+"|"+rows+"|") {
		t.Fatalf("seeded %s, want %s|%s|<md5>", got, rows, rows)
	}
	if got, want := testQuery(t, run, allDatabasesResumeDefsSQL(table)), allDatabasesResumeDefs(schema); got != want {
		t.Fatalf("definitions %q, want %q", got, want)
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	holder := testBackground(bgCtx, run, holdSQL(allDatabasesResumeGateSession, allDatabasesResumeLockSQL))
	t.Cleanup(func() {
		testQuery(t, run, endSessionSQL(allDatabasesResumeGateSession))
		<-holder
	})
	testWaitFor(t, run, allDatabasesResumeHeldSQL, "1")

	build := testBackground(bgCtx, run, "SET application_name = 'pgcopydb[1] create index';\n"+
		"CREATE INDEX big_gate2 ON "+table+" ("+sqlIdent(schema)+".gate_key(id));\n")
	testWaitFor(t, run, allDatabasesResumeStateSQL(schema), "1|"+rows+"|1|1")
	if got := testQuery(t, run, endSessionSQL(allDatabasesResumeGateSession)); got != "1" {
		t.Fatalf("ended %s sessions holding the gate, want 1", got)
	}
	if err := <-build; err != nil {
		t.Fatalf("the gated index build failed after the release: %v", err)
	}
	testWaitFor(t, run, allDatabasesResumeStateSQL(schema), "0|"+rows+"|1|1")
}
