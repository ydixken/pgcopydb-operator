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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

const (
	// splitResumeRows ids split under 1Mi into four parts: the empty IS NULL range,
	// 1..50000, 50001..100000, and >= 100001, which holds the gated last id.
	splitResumeRows = 200000
	// splitResumeLockKey is the advisory lock the gate waits on. Only the target's is ever held.
	splitResumeLockKey     = "581"
	splitResumeGateSession = "e2e_split_gate"
	splitResumeLockSQL     = "SELECT pg_advisory_xact_lock(" + splitResumeLockKey + ")"
	splitResumeHeldSQL     = "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE a.application_name = '" + splitResumeGateSession + "' AND l.locktype = 'advisory'" +
		" AND l.objid = " + splitResumeLockKey + " AND l.granted"
)

// A resume used to truncate a split table and then skip the parts an earlier
// attempt had finished, so the retry exited 0 and the Migration completed without
// their rows. Its own container: the retry's log grace puts it in the longest tier.
var _ = Describe("Split table resume", SpecPriority(3), func() {
	BeforeEach(resetMigrationPair)

	It("keeps every row of a split table when a retry resumes a copy killed after some parts finished", func() {
		identity := fmt.Sprint(time.Now().UnixNano())
		name, schema := "e2e-split-resume-"+identity, "e2e_split_resume_"+identity
		table := sqlIdent(schema) + ".big"
		threshold := resource.MustParse("1Mi")
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{
			SplitTablesLargerThan: &threshold,
			SplitMaxParts:         4,
			Filters:               &v1beta1.Filters{IncludeOnlySchemas: []string{schema}},
		})
		// Issue 88 can cost one more attempt on any resume, so the budget goes up.
		m.Spec.BackoffLimit = 5
		createStampedSchemas(m, identity, "pgcopydb-e2e-split-resume:"+identity, []string{schema}, nil)

		By("seeding a table that splits into four parts, with a CHECK that can hold its last id")
		psqlBulk(sourceCluster, asSourceAppRole()+splitResumeSeedSQL(schema))
		rows := fmt.Sprintf("%d|%d", splitResumeRows, splitResumeRows)
		Expect(psql(sourceCluster, splitResumeRowsSQL(table))).To(Equal(rows))

		By("holding the gate on the target, so the last part's COPY waits at its last id")
		release := holdOnTarget(splitResumeGateSession, splitResumeLockSQL, splitResumeHeldSQL)
		create(m)
		reportResumedAttempts(m)
		waitWorkerRunning(name, 1)
		waitAttemptOneHeld(m, splitResumeStateSQL(table), fmt.Sprintf("1|%d", splitResumeRows/2),
			"pgcopydb sessions waiting on the gate|committed rows of the split table on the target")

		By("deleting the attempt-1 worker pod while the last part waits")
		deleteAttemptOneWorker(name)

		By("releasing the gate, so the killed COPY ends and the retry gate passes")
		// Not earlier: a worker still alive would commit the last part and finish attempt 1.
		release()
		waitGatePassed(name, 2)
		completed := waitCompleted(name, nsE2E)
		AddReportEntry("attempts after the worker deletion", completed.Status.Attempts)
		Expect(completed.Status.Attempts).To(BeNumerically(">=", 2))
		// A start-over copies every part again and would pass on any runner.
		expectResumedRetries(name, completed.Status.Attempts, "--resume", "--not-consistent")

		By("comparing the split table on both sides")
		Expect(psql(targetCluster, splitResumeRowsSQL(table))).To(Equal(rows),
			"count(*)|count(DISTINCT id) of the split table on the target after the resumed copy")
	})
})

// reportResumedAttempts reports, when the spec fails, why m kept retrying: the
// events with attempt 1's Job and log, then each later attempt's phase, Job and log tail.
func reportResumedAttempts(m *v1beta1.Migration) {
	GinkgoHelper()
	DeferCleanup(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		cur := &v1beta1.Migration{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(m), cur); err != nil {
			AddReportEntry("attempts", "Migration unavailable: "+err.Error(), ReportEntryVisibilityFailureOrVerbose)
			return
		}
		// The events carry every attempt's failure; attempt 1's Job and log come with them.
		reportFailedAttempt(cur)
		for n := int32(2); n <= cur.Status.Attempts; n++ {
			job := fmt.Sprintf("%s-run-%d", m.Name, n)
			state := &batchv1.Job{}
			summary := fmt.Sprintf("phase %q", cur.Status.Phase)
			if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: job}, state); err != nil {
				summary += ", Job unavailable: " + err.Error()
			} else {
				summary += fmt.Sprintf(", Job %d succeeded, %d failed", state.Status.Succeeded, state.Status.Failed)
			}
			AddReportEntry(job, summary+"\n"+jobLogs(job, attemptLogTail), ReportEntryVisibilityFailureOrVerbose)
		}
	})
}

// waitAttemptOneHeld polls stateSQL on the target until it prints want, which what
// describes, and stops at once when attempt 1 ends, fails or completes first.
func waitAttemptOneHeld(m *v1beta1.Migration, stateSQL, want, what string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		cur := &v1beta1.Migration{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(m), cur)).To(Succeed())
		if cur.Status.Attempts > 1 || cur.Status.Phase == v1beta1.PhaseFailed ||
			cur.Status.Phase == v1beta1.PhaseCompleted {
			StopTrying(fmt.Sprintf("attempt 1 ended before the gate held it: attempt %d at phase %q",
				cur.Status.Attempts, cur.Status.Phase)).Now()
		}
		state, err := psqlDBErr(targetCluster, appDatabase(targetCluster), stateSQL)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(state).To(Equal(want), what)
	}, 5*time.Minute, time.Second).Should(Succeed())
}

// deleteAttemptOneWorker deletes the worker pod of attempt 1 and waits until it is gone.
func deleteAttemptOneWorker(name string) {
	GinkgoHelper()
	worker := client.MatchingLabels{migrationLabel: name, batchv1.JobNameLabel: name + "-run-1"}
	deletePod(worker)
	Eventually(func(g Gomega) {
		pods := &corev1.PodList{}
		g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E), worker)).To(Succeed())
		left := len(pods.Items)
		g.Expect(left).To(BeZero(), "the attempt-1 worker pod is still there")
	}, 2*time.Minute, time.Second).Should(Succeed())
}

// expectResumedRetries expects the Job of every attempt from 2 to attempts to carry args.
func expectResumedRetries(name string, attempts int32, args ...string) {
	GinkgoHelper()
	// ContainElements of no args matches any argv, a start-over's included.
	Expect(args).NotTo(BeEmpty())
	for n := int32(2); n <= attempts; n++ {
		retry := &batchv1.Job{}
		key := client.ObjectKey{Namespace: nsE2E, Name: fmt.Sprintf("%s-run-%d", name, n)}
		Expect(k8sClient.Get(ctx, key, retry)).To(Succeed())
		Expect(retry.Spec.Template.Spec.Containers[0].Args).To(ContainElements(args),
			"attempt %d did not resume, so it cannot show what a resume keeps", n)
	}
}

// splitResumeSeedSQL creates schema.big with a CHECK whose function waits on the
// gate for the last id only. pg_dump keeps a valid CHECK inside CREATE TABLE, so
// the restored target evaluates it for every row a COPY writes.
func splitResumeSeedSQL(schema string) string {
	return fmt.Sprintf(`CREATE TABLE %[1]s.big (id integer PRIMARY KEY, payload text NOT NULL);
INSERT INTO %[1]s.big SELECT g, repeat(md5(g::text), 2) FROM generate_series(1, %[2]d) g;
CREATE FUNCTION %[1]s.gate(id integer) RETURNS boolean LANGUAGE plpgsql AS $gate$
BEGIN
  IF id = %[2]d THEN PERFORM pg_advisory_xact_lock_shared(%[3]s); END IF;
  RETURN true;
END $gate$;
ALTER TABLE %[1]s.big ADD CONSTRAINT big_gate CHECK (%[1]s.gate(id));
ANALYZE %[1]s.big;
`, sqlIdent(schema), splitResumeRows, splitResumeLockKey)
}

// splitResumeStateSQL prints waiting|rows: the pgcopydb sessions that wait on the
// gate, and the committed rows of table.
func splitResumeStateSQL(table string) string {
	return "SELECT (SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid" +
		" WHERE l.locktype = 'advisory' AND l.objid = " + splitResumeLockKey + " AND NOT l.granted" +
		" AND a.datname = current_database() AND a.application_name LIKE '" + progress.PgcopydbAppNameLike + "')" +
		" || '|' || (SELECT count(*) FROM " + table + ")"
}

// splitResumeRowsSQL prints count(*)|count(DISTINCT id) of table.
func splitResumeRowsSQL(table string) string {
	return "SELECT count(*) || '|' || count(DISTINCT id) FROM " + table
}

// TestSplitResumeSQL runs the spec's SQL on a local server: the gate holds a
// write of the last id and no other, and the state counts a waiting pgcopydb
// session but not the session that holds the gate.
func TestSplitResumeSQL(t *testing.T) {
	run := testPSQL(t)
	schema := fmt.Sprintf("split_resume_%d", time.Now().UnixNano())
	table := sqlIdent(schema) + ".big"
	testCoverageSchemas(t, run, schema)
	testQuery(t, run, "SET ROLE "+testCaseOwner+";\n"+splitResumeSeedSQL(schema))
	rows := strconv.Itoa(splitResumeRows)
	if got := testQuery(t, run, splitResumeRowsSQL(table)); got != rows+"|"+rows {
		t.Fatalf("seeded %s, want %s|%s", got, rows, rows)
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	holder := testBackground(bgCtx, run, holdSQL(splitResumeGateSession, splitResumeLockSQL))
	t.Cleanup(func() {
		testQuery(t, run, endSessionSQL(splitResumeGateSession))
		<-holder
	})
	testWaitFor(t, run, splitResumeHeldSQL, "1")

	update := "UPDATE " + table + " SET payload = payload WHERE id = "
	testQuery(t, run, "SET statement_timeout = '5s';\n"+update+strconv.Itoa(splitResumeRows-1))
	writer := testBackground(bgCtx, run, "SET application_name = 'pgcopydb[1] e2e copy';\n"+update+rows+";\n")
	testWaitFor(t, run, splitResumeStateSQL(table), "1|"+rows)
	if got := testQuery(t, run, endSessionSQL(splitResumeGateSession)); got != "1" {
		t.Fatalf("ended %s sessions holding the gate, want 1", got)
	}
	if err := <-writer; err != nil {
		t.Fatalf("the write of the gated id failed after the release: %v", err)
	}
	testWaitFor(t, run, splitResumeStateSQL(table), "0|"+rows)
}

// testBackground runs script on run's server in the background; the channel
// receives its error, nil on success, once it ends.
func testBackground(ctx context.Context, run psqlScript, script string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := run(ctx, script)
		done <- err
	}()
	return done
}

// testWaitFor polls sql on run's server for up to 10 seconds until it prints want.
func testWaitFor(t *testing.T, run psqlScript, sql, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for got := testQuery(t, run, sql); got != want; got = testQuery(t, run, sql) {
		if time.Now().After(deadline) {
			t.Fatalf("%s printed %q, want %q", sql, got, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
