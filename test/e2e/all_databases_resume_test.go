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

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

const (
	// allDatabasesConstraintRows is small: the constraint's ALTER TABLE waits on its first row.
	allDatabasesConstraintRows = 10000
	// allDatabasesConstraintLockKey is the advisory lock that ALTER TABLE waits on; the first spec holds 582.
	allDatabasesConstraintLockKey     = "583"
	allDatabasesConstraintGateSession = "e2e_alldb_cw_gate"
	allDatabasesConstraintLockSQL     = "SELECT pg_advisory_xact_lock(" + allDatabasesConstraintLockKey + ")"
	allDatabasesConstraintHeldSQL     = "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE a.application_name = '" + allDatabasesConstraintGateSession + "' AND l.locktype = 'advisory'" +
		" AND l.objid = " + allDatabasesConstraintLockKey + " AND l.granted"
	// allDatabasesConstraintSessionsSQL counts pgcopydb sessions of any role in any database,
	// a superset of the all-databases retry gate's census of the migration role.
	allDatabasesConstraintSessionsSQL = "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE '" +
		progress.PgcopydbAppNameLike + "'"
)

// An all-databases retry failed every attempt on `relation "..." already exists` after a kill in a split table's
// index build or in an exclusion constraint's ALTER TABLE, and on `role "..." already exists` for a role whose
// name needs quoting. SpecPriority(3): both specs repeat the split spec's kill and retries.
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

	It("converges when a killed attempt's constraint commits on the target before the retry", func() {
		identity := fmt.Sprint(time.Now().UnixNano())
		name, schema := "e2e-alldb-cw-"+identity, "e2e_alldb_cw_"+identity
		table := sqlIdent(schema) + ".cw"
		key := client.ObjectKey{Namespace: nsE2E, Name: name}
		state := allDatabasesConstraintStateSQL(schema)
		const what = "pgcopydb ALTER TABLE statements waiting on the gate|exclusion constraints on the table"
		// Registered first, so it runs last: no attempt is left that could create it again.
		DeferCleanup(func() {
			for _, cluster := range []string{sourceCluster, targetCluster} {
				psqlDB(cluster, postgresContainer, "DROP SCHEMA IF EXISTS "+sqlIdent(schema))
			}
		})
		m := allDatabasesMigration(name,
			v1beta1.CloneOptions{Filters: &v1beta1.Filters{IncludeOnlySchemas: []string{schema}}})
		// As in the first spec (issue 88).
		m.Spec.BackoffLimit = 5
		createStampedSchemas(m, identity, "pgcopydb-e2e-alldb-cw:"+identity, []string{schema}, nil)

		By("seeding a table whose exclusion constraint builds its index through a function that waits on the gate")
		psqlBulk(sourceCluster, asSourceAppRole()+allDatabasesConstraintSeedSQL(schema))
		want := psql(sourceCluster, allDatabasesResumeRowsSQL(table))
		Expect(want).To(HavePrefix(fmt.Sprintf("%d|%d|", allDatabasesConstraintRows, allDatabasesConstraintRows)))
		defs := psql(sourceCluster, allDatabasesResumeDefsSQL(table))
		Expect(defs).To(Equal(allDatabasesConstraintDefs(schema)))
		constraint := psql(sourceCluster, allDatabasesConstraintSQL(table))
		Expect(constraint).To(Equal("1|" + allDatabasesConstraintDef(schema)))
		// includeOnlySchemas applies in every database, as in the first spec.
		psqlDB(sourceCluster, postgresContainer, "CREATE SCHEMA "+sqlIdent(schema))

		By("holding the gate on the target, so the ALTER TABLE that adds the constraint waits")
		release := holdOnTarget(allDatabasesConstraintGateSession, allDatabasesConstraintLockSQL,
			allDatabasesConstraintHeldSQL)
		create(m)
		reportResumedAttempts(m)
		waitWorkerRunning(name, 1)
		waitAttemptOneHeld(m, state, "1|0", what)

		By("deleting the attempt-1 worker pod while the ALTER TABLE waits")
		deleteAttemptOneWorker(name)

		By("expecting the retry gate to hold attempt 2 while the killed attempt's ALTER TABLE waits")
		// heldOnOrphan returns the message of a hold on the waiting ALTER TABLE, and stops the poll at once
		// when the Migration failed or a retry started, which a gate that passed too early would show.
		heldOnOrphan := func(g Gomega) string {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, cur)).To(Succeed())
			if cur.Status.Phase == v1beta1.PhaseFailed {
				StopTrying(fmt.Sprintf("migration %s failed while the gate should hold: %s", name, failureMessage(cur))).Now()
			}
			if cur.Status.Attempts > 1 {
				StopTrying(fmt.Sprintf("attempt %d started while the gate should hold: RetryHeld %+v", cur.Status.Attempts,
					apimeta.FindStatusCondition(cur.Status.Conditions, v1beta1.ConditionRetryHeld))).Now()
			}
			c := expectConditionReason(g, cur, v1beta1.ConditionRetryHeld, metav1.ConditionTrue, "SessionsRemain")
			g.Expect(c.Message).To(ContainSubstring("wait=advisory"))
			g.Expect(c.Message).To(ContainSubstring("app=pgcopydb["))
			return c.Message
		}
		// The first probe comes about 330 s after the failure and may wait for a node until its deadline.
		Eventually(func(g Gomega) { heldOnOrphan(g) },
			330*time.Second+gateProbeDeadline+retryGateInterval+time.Minute, 2*time.Second).Should(Succeed())
		Consistently(func(g Gomega) {
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, cur)).To(Succeed())
			g.Expect(cur.Status.Attempts).To(Equal(int32(1)))
			got, err := psqlDBErr(targetCluster, appDatabase(targetCluster), state)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal("1|0"), what)
		}, 2*retryGateInterval+5*time.Second, 5*time.Second).Should(Succeed())

		By("waiting for the gate's next verdict, so the release lands a probe interval before the next census")
		// Each verdict changes the message's age=, and the gate keeps a failed probe for retryGateInterval
		// before it starts the next, so the next census comes about one interval after this release.
		var verdict string
		Eventually(func(g Gomega) { verdict = heldOnOrphan(g) },
			gateProbeDeadline+2*retryGateInterval+time.Minute, time.Second).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(heldOnOrphan(g)).NotTo(Equal(verdict), "the RetryHeld message before the wait for a verdict")
		}, gateProbeDeadline+2*retryGateInterval+time.Minute, time.Second).Should(Succeed())

		By("releasing the gate, so the ALTER TABLE commits and its session ends before attempt 2")
		release()
		released := time.Now()
		Eventually(func(g Gomega) {
			got, err := psqlDBErr(targetCluster, appDatabase(targetCluster), state)
			g.Expect(err).NotTo(HaveOccurred())
			sessions, err := psqlDBErr(targetCluster, postgresContainer, allDatabasesConstraintSessionsSQL)
			g.Expect(err).NotTo(HaveOccurred())
			// Read last: the count never goes down, so attempt 1 here was the only one while both queries ran.
			cur := &v1beta1.Migration{}
			g.Expect(k8sClient.Get(ctx, key, cur)).To(Succeed())
			if cur.Status.Attempts > 1 || cur.Status.Phase == v1beta1.PhaseFailed {
				StopTrying(fmt.Sprintf("attempt %d at phase %q started before the ALTER TABLE committed and its"+
					" session ended: state %s, %s pgcopydb sessions", cur.Status.Attempts, cur.Status.Phase, got, sessions)).Now()
			}
			g.Expect(got+"|"+sessions).To(Equal("0|1|0"), what+"|pgcopydb sessions on the target")
		}, migrationTimeout, 250*time.Millisecond).Should(Succeed())
		waitGatePassed(name, 2)
		AddReportEntry("constraint gate hold after release", time.Since(released).Round(time.Second).String())
		completed := waitCompleted(name, nsE2E)
		AddReportEntry("attempts after the worker deletion", completed.Status.Attempts)
		Expect(completed.Status.Attempts).To(BeNumerically(">=", 2))
		// A start-over adds the constraint again and would pass on any runner.
		expectResumedRetries(name, completed.Status.Attempts, "--all-databases", "--resume", "--not-consistent")

		By("comparing the table, its constraint and its index on both sides")
		Expect(psql(targetCluster, allDatabasesConstraintSQL(table))).To(Equal(constraint),
			"exclusion constraints of the table on the target after the resumed clone")
		Expect(psql(targetCluster, allDatabasesResumeDefsSQL(table))).To(Equal(defs),
			"indexes and constraints of the table on the target after the resumed clone")
		Expect(psql(targetCluster, allDatabasesResumeRowsSQL(table))).To(Equal(want),
			"count(*)|count(DISTINCT id)|md5 of the table on the target after the resumed clone")
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

// allDatabasesConstraintSeedSQL creates schema.cw with an exclusion constraint whose function waits on
// the gate for every row. pgcopydb builds no separate index for it, so the target's ALTER TABLE waits.
func allDatabasesConstraintSeedSQL(schema string) string {
	return fmt.Sprintf(`CREATE TABLE %[1]s.cw (id integer NOT NULL, payload text NOT NULL);
INSERT INTO %[1]s.cw SELECT g, md5(g::text) FROM generate_series(1, %[2]d) g;
CREATE FUNCTION %[1]s.gate_key(id integer) RETURNS integer LANGUAGE plpgsql IMMUTABLE
  AS $gate$ BEGIN PERFORM pg_advisory_xact_lock_shared(%[3]s); RETURN id; END $gate$;
ALTER TABLE %[1]s.cw ADD CONSTRAINT cw_excl EXCLUDE USING btree (%[1]s.gate_key(id) WITH =);
ANALYZE %[1]s.cw;
`, sqlIdent(schema), allDatabasesConstraintRows, allDatabasesConstraintLockKey)
}

// allDatabasesConstraintStateSQL prints waiting|constraints for schema.cw: the pgcopydb ALTER TABLE statements
// in this database that wait on the gate, and the exclusion constraints. It reads no row of the table,
// which the waiting ALTER TABLE locks.
func allDatabasesConstraintStateSQL(schema string) string {
	return "SELECT (SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE l.locktype = 'advisory' AND l.objid = " + allDatabasesConstraintLockKey + " AND NOT l.granted" +
		" AND a.datname = current_database() AND a.application_name LIKE '" + progress.PgcopydbAppNameLike + "'" +
		" AND a.query LIKE 'ALTER TABLE %')" +
		" || '|' || (SELECT count(*) FROM pg_constraint WHERE conrelid = to_regclass(" +
		sqlLiteral(sqlIdent(schema)+".cw") + ") AND contype = 'x')"
}

// allDatabasesConstraintSQL prints count|definitions of table's exclusion constraints.
func allDatabasesConstraintSQL(table string) string {
	return "SELECT count(*) || '|' || coalesce(string_agg(conname || ' ' || pg_get_constraintdef(oid), '; '" +
		" ORDER BY conname), '') FROM pg_constraint WHERE conrelid = " + sqlLiteral(table) + "::regclass AND contype = 'x'"
}

// allDatabasesConstraintDef is the seeded constraint as allDatabasesConstraintSQL prints it.
func allDatabasesConstraintDef(schema string) string {
	return fmt.Sprintf("cw_excl EXCLUDE USING btree (%s.gate_key(id) WITH =)", quoteIdentIfNeeded(schema))
}

// allDatabasesConstraintDefs is what allDatabasesResumeDefsSQL prints for the seeded table.
func allDatabasesConstraintDefs(schema string) string {
	return fmt.Sprintf("CREATE INDEX cw_excl ON %[1]s.cw USING btree (%[1]s.gate_key(id)); %[2]s",
		quoteIdentIfNeeded(schema), allDatabasesConstraintDef(schema))
}

// TestAllDatabasesConstraintSQL runs the spec's SQL on a local server: the gate holds the constraint's
// ALTER TABLE, which keeps waiting after its client is killed and commits once the gate ends.
func TestAllDatabasesConstraintSQL(t *testing.T) {
	run := testPSQL(t)
	schema := fmt.Sprintf("alldb_cw_%d", time.Now().UnixNano())
	table := sqlIdent(schema) + ".cw"
	testCoverageSchemas(t, run, schema)
	testQuery(t, run, "SET ROLE "+testCaseOwner+";\n"+allDatabasesConstraintSeedSQL(schema))
	rows := strconv.Itoa(allDatabasesConstraintRows)
	if got := testQuery(t, run, allDatabasesResumeRowsSQL(table)); !strings.HasPrefix(got, rows+"|"+rows+"|") {
		t.Fatalf("seeded %s, want %s|%s|<md5>", got, rows, rows)
	}
	if got, want := testQuery(t, run, allDatabasesResumeDefsSQL(table)), allDatabasesConstraintDefs(schema); got != want {
		t.Fatalf("definitions %q, want %q", got, want)
	}
	constraint := "1|" + allDatabasesConstraintDef(schema)
	if got := testQuery(t, run, allDatabasesConstraintSQL(table)); got != constraint {
		t.Fatalf("exclusion constraints %q, want %q", got, constraint)
	}
	testQuery(t, run, "ALTER TABLE "+table+" DROP CONSTRAINT cw_excl")
	if got := testQuery(t, run, allDatabasesConstraintSQL(table)); got != "0|" {
		t.Fatalf("exclusion constraints after the drop %q, want 0|", got)
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	holder := testBackground(bgCtx, run, holdSQL(allDatabasesConstraintGateSession, allDatabasesConstraintLockSQL))
	t.Cleanup(func() {
		testQuery(t, run, endSessionSQL(allDatabasesConstraintGateSession))
		<-holder
	})
	testWaitFor(t, run, allDatabasesConstraintHeldSQL, "1")

	// Cancelling SIGKILLs psql, exec.CommandContext's default; on SIGINT, which no pod deletion sends,
	// psql would cancel the statement.
	alterCtx, kill := context.WithCancel(bgCtx)
	defer kill()
	const worker = "pgcopydb[1] create index worker"
	alter := testBackground(alterCtx, run, "SET application_name = '"+worker+"';\n"+
		"ALTER TABLE "+table+" ADD CONSTRAINT cw_excl EXCLUDE USING btree ("+sqlIdent(schema)+".gate_key(id) WITH =);\n")
	state := allDatabasesConstraintStateSQL(schema)
	testWaitFor(t, run, state, "1|0")
	kill()
	if err := <-alter; err == nil {
		t.Fatal("the killed psql of the ALTER TABLE reported no error")
	}
	testWaitFor(t, run, state, "1|0")
	if got := testQuery(t, run, endSessionSQL(allDatabasesConstraintGateSession)); got != "1" {
		t.Fatalf("ended %s sessions holding the gate, want 1", got)
	}
	testWaitFor(t, run, state, "0|1")
	testWaitFor(t, run, "SELECT count(*) FROM pg_stat_activity WHERE application_name = '"+worker+"'", "0")
	if got := testQuery(t, run, allDatabasesConstraintSQL(table)); got != constraint {
		t.Fatalf("exclusion constraints after the release %q, want %q", got, constraint)
	}
}
