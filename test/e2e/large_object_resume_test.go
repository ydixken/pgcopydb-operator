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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

const (
	largeObjectResumeRows     = 1000
	largeObjectResumeSequence = 4242
	// largeObjectResumeGateSession holds pg_largeobject on the target, so every large-object write waits.
	largeObjectResumeGateSession = "e2e_lo_gate"
	largeObjectResumeLockSQL     = "LOCK TABLE pg_catalog.pg_largeobject IN ACCESS EXCLUSIVE MODE"
	largeObjectResumeHeldSQL     = "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE a.application_name = '" + largeObjectResumeGateSession + "'" +
		" AND l.relation = 'pg_catalog.pg_largeobject'::regclass AND l.mode = 'AccessExclusiveLock' AND l.granted"
)

// A resume after a kill in the large-object step skipped that step and exited 0, so the
// Migration completed without the objects. SpecPriority(3): it repeats the split spec's kill and retries.
var _ = Describe("Large object resume", SpecPriority(3), func() {
	// Locking a system catalog takes a superuser, which external mode does not promise.
	BeforeEach(requireCNPGFixtures)
	BeforeEach(resetMigrationPair)

	It("copies every large object when a retry resumes a clone killed in its large-object step", func() {
		identity := fmt.Sprint(time.Now().UnixNano())
		name, schema := "e2e-lo-resume-"+identity, "e2e_lo_resume_"+identity
		// The filter keeps the seed's tables out; its large objects belong to no schema and travel anyway.
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			Filters: &v1beta1.Filters{IncludeOnlySchemas: []string{schema}},
		})
		// Issue 88 can cost one more attempt on any resume, so the budget goes up.
		m.Spec.BackoffLimit = 5
		createStampedSchemas(m, identity, "pgcopydb-e2e-lo-resume:"+identity, []string{schema}, nil)

		By("seeding a table and a sequence beside the fixture's large objects")
		psqlBulk(sourceCluster, asSourceAppRole()+largeObjectResumeSeedSQL(schema))
		Expect(largeObjectCount(sourceCluster)).To(Equal(fmt.Sprint(scaled(40))))
		want := psql(sourceCluster, largeObjectDigestSQL)

		By("holding pg_largeobject on the target, so the large-object workers wait")
		release := holdOnTarget(largeObjectResumeGateSession, largeObjectResumeLockSQL, largeObjectResumeHeldSQL)
		create(m)
		reportResumedAttempts(m)
		waitWorkerRunning(name, 1)
		// Copied rows and a reset sequence stand in for the catalog's done tables, indexes and
		// sequences, after which older runners' resumes skip the step.
		waitAttemptOneHeld(m, largeObjectResumeStateSQL(schema),
			fmt.Sprintf("1|%d|%d", largeObjectResumeRows, largeObjectResumeSequence),
			"pgcopydb sessions waiting on pg_largeobject|committed rows of the table|last value of the sequence")

		By("deleting the attempt-1 worker pod while the large-object workers wait")
		deleteAttemptOneWorker(name)

		By("releasing the gate, so the killed run's sessions end and the retry gate passes")
		// Not earlier: a worker still alive would copy the large objects and finish attempt 1.
		release()
		released := time.Now()
		waitGatePassed(name, 2)
		AddReportEntry("large-object gate hold after release", time.Since(released).Round(time.Second).String())
		completed := waitCompleted(name, nsE2E)
		AddReportEntry("attempts after the worker deletion", completed.Status.Attempts)
		Expect(completed.Status.Attempts).To(BeNumerically(">=", 2))
		// A start-over copies every large object again and would pass on any runner.
		expectResumedRetries(name, completed.Status.Attempts, "--resume", "--not-consistent")

		By("comparing the large objects and the table on both sides")
		Expect(psql(targetCluster, largeObjectDigestSQL)).To(Equal(want),
			"count|md5 of the large objects on the target after the resumed clone")
		Expect(psql(targetCluster, "SELECT count(*) FROM "+sqlIdent(schema)+".t")).
			To(Equal(fmt.Sprint(largeObjectResumeRows)), "rows of the table on the target after the resumed clone")
	})
})

// largeObjectResumeSeedSQL creates schema.t and schema.s, whose copy and reset the state reads.
func largeObjectResumeSeedSQL(schema string) string {
	return fmt.Sprintf(`CREATE TABLE %[1]s.t (id integer PRIMARY KEY);
INSERT INTO %[1]s.t SELECT generate_series(1, %[2]d);
CREATE SEQUENCE %[1]s.s;
SELECT setval(%[3]s, %[4]d);
`, sqlIdent(schema), largeObjectResumeRows, sqlLiteral(sqlIdent(schema)+".s"), largeObjectResumeSequence)
}

// largeObjectResumeStateSQL prints waiting|rows|sequence: whether pgcopydb sessions wait on
// pg_largeobject, the committed rows of schema.t, and the last value of schema.s.
func largeObjectResumeStateSQL(schema string) string {
	return "SELECT (SELECT count(*) > 0 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE l.relation = 'pg_catalog.pg_largeobject'::regclass AND NOT l.granted" +
		" AND a.datname = current_database() AND a.application_name LIKE '" + progress.PgcopydbAppNameLike + "')::int" +
		" || '|' || (SELECT count(*) FROM " + sqlIdent(schema) + ".t)" +
		" || '|' || (SELECT last_value FROM " + sqlIdent(schema) + ".s)"
}

// TestLargeObjectResumeSQL runs the spec's SQL on a local server: the gate holds a
// large-object write, and the state counts that pgcopydb session until the gate ends.
func TestLargeObjectResumeSQL(t *testing.T) {
	run := testPSQL(t)
	schema := fmt.Sprintf("lo_resume_%d", time.Now().UnixNano())
	testCoverageSchemas(t, run, schema)
	testQuery(t, run, "SET ROLE "+testCaseOwner+";\n"+largeObjectResumeSeedSQL(schema))
	copied := fmt.Sprintf("%d|%d", largeObjectResumeRows, largeObjectResumeSequence)
	if got := testQuery(t, run, largeObjectResumeStateSQL(schema)); got != "0|"+copied {
		t.Fatalf("state %q before the gate, want 0|%s", got, copied)
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	holder := testBackground(bgCtx, run, holdSQL(largeObjectResumeGateSession, largeObjectResumeLockSQL))
	t.Cleanup(func() {
		testQuery(t, run, endSessionSQL(largeObjectResumeGateSession))
		<-holder
	})
	testWaitFor(t, run, largeObjectResumeHeldSQL, "1")

	// Rolled back, as a killed worker's transaction is, so the test leaves no large object behind.
	write := testBackground(bgCtx, run, "SET application_name = 'pgcopydb[1] blob';\n"+
		"BEGIN;\nSELECT lo_from_bytea(0, 'x');\nROLLBACK;\n")
	testWaitFor(t, run, largeObjectResumeStateSQL(schema), "1|"+copied)
	if got := testQuery(t, run, endSessionSQL(largeObjectResumeGateSession)); got != "1" {
		t.Fatalf("ended %s sessions holding the gate, want 1", got)
	}
	if err := <-write; err != nil {
		t.Fatalf("the gated large-object write failed after the release: %v", err)
	}
	testWaitFor(t, run, largeObjectResumeStateSQL(schema), "0|"+copied)
}
