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
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The follow worker keeps real observation traffic running between held locks.
var _ = Describe("Progress sampler bounds", func() {
	It("releases blocked source and target samplers across repeated polls and recovers", func() {
		const name = "e2e-progress-bounds"
		const table = progressProbeTable
		Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
		psql(sourceCluster, progressProbeSetupSQL(appDB))
		DeferCleanup(func() {
			deleteMigration(name)
			Eventually(sourceSlotCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
			Eventually(targetOriginCount, 2*time.Minute, 2*time.Second).Should(Equal("0"))
			psql(sourceCluster, "DROP TABLE IF EXISTS "+table)
			psql(targetCluster, "DROP TABLE IF EXISTS "+table)
		})
		Expect(psql(sourceCluster, "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='"+
			table+"'::regclass")).To(Equal(appDB), "the follow fixture must be owned by the migration role")
		create(newFollowMigration(name, v1beta1.CutoverManual))
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		readMigration := func() *v1beta1.Migration {
			m := &v1beta1.Migration{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
			return m
		}
		Eventually(func() bool {
			m := readMigration()
			return m.Status.Progress != nil && m.Status.Progress.TablesTotal > 0
		}, time.Minute, time.Second).Should(BeTrue(), "successful baseline sampling is required")
		pods := &corev1.PodList{}
		Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
			client.MatchingLabels{"job-name": name + "-run-1"})).To(Succeed())
		Expect(pods.Items).To(HaveLen(1))
		runner := pods.Items[0].Name
		workers := runnerProgressProcesses(runner, false)
		Expect(workers).NotTo(BeEmpty(), "a pgcopydb worker must be observed before testing cleanup")
		for side, cluster := range []string{sourceCluster, targetCluster} {
			waitPhase(name, nsE2E, lagConvergeTimeout, v1beta1.PhaseCutoverPending)
			baseline := readMigration().Status.Progress.DeepCopy()
			Expect(baseline).NotTo(BeNil())
			Expect(baseline.BytesTotal).NotTo(BeNil())
			Expect(baseline.BytesDone).NotTo(BeNil())
			Expect(baseline.BytesTotal.Value()).To(BeNumerically(">", 0))
			Expect(baseline.BytesDone.Value()).To(BeNumerically(">", 0))
			release := holdProgressLock([]string{sourceKey, targetKey}[side], cluster, table)
			func() {
				defer release()
				for range 3 {
					var backend string
					Eventually(func() string {
						backend = psql(cluster, `SELECT a.pid::text FROM pg_stat_activity a
WHERE a.pid<>pg_backend_pid() AND a.query LIKE 'with t as (%' AND a.wait_event_type='Lock'
AND EXISTS (SELECT 1 FROM pg_stat_activity b WHERE b.application_name='e2e_progress_blocker'
AND b.pid=ANY(pg_blocking_pids(a.pid)))`)
						return backend
					}, time.Minute, 200*time.Millisecond).ShouldNot(BeEmpty(),
						"must observe a sampler waiting on the test-owned relation lock")
					Expect(strings.Fields(backend)).To(HaveLen(1), "previous polls must not accumulate backends")
					processes := runnerProgressProcesses(runner, true)
					Expect(processes).NotTo(BeEmpty(), "must observe actual psql process identities before checking cleanup")
					Eventually(func() string {
						return psql(cluster, "SELECT count(*) FROM pg_stat_activity WHERE pid="+backend)
					}, 6*time.Second, 200*time.Millisecond).Should(Equal("0"),
						"blocked SQL session must disappear while the blocker stays alive")
					Eventually(func() bool {
						current := runnerProgressProcesses(runner, true)
						for _, pid := range processes {
							if slices.Contains(current, pid) {
								return false
							}
						}
						return true
					}, 2*time.Second, 200*time.Millisecond).Should(BeTrue(),
						"the observed sampler process cohort must exit within its remote budget")
					Expect(psql(cluster, progressBlockerCount)).To(Equal("1"))
					Expect(runnerProgressProcesses(runner, false)).To(ContainElements(workers),
						"sampling cancellation must preserve the worker processes")
					m := readMigration()
					Expect(m.Status.Progress).To(Equal(baseline))
					Expect(m.Status.Attempts).To(Equal(int32(1)))
					Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCutoverPending))
				}
			}()
			psql(sourceCluster, fmt.Sprintf("INSERT INTO %s SELECT i, repeat(md5(i::text), 100) "+
				"FROM generate_series(%d,%d) i", table, side*1000+1, (side+1)*1000))
			Eventually(func(g Gomega) {
				m := readMigration()
				detail := fmt.Sprintf("after %s lock: phase=%s attempts=%d baseline bytesTotal=%d bytesDone=%d",
					[]string{sourceKey, targetKey}[side], m.Status.Phase, m.Status.Attempts,
					baseline.BytesTotal.Value(), baseline.BytesDone.Value())
				g.Expect(m.Status.Progress).NotTo(BeNil(), detail)
				g.Expect(m.Status.Progress.BytesTotal).NotTo(BeNil(), detail)
				g.Expect(m.Status.Progress.BytesDone).NotTo(BeNil(), detail)
				total, done := m.Status.Progress.BytesTotal.Value(), m.Status.Progress.BytesDone.Value()
				detail += fmt.Sprintf(" current bytesTotal=%d bytesDone=%d", total, done)
				g.Expect(m.Status.Phase).To(Equal(v1beta1.PhaseCutoverPending), detail)
				g.Expect(m.Status.Attempts).To(Equal(int32(1)), detail)
				g.Expect(total).To(BeNumerically(">", 0), detail)
				g.Expect(done).To(BeNumerically(">", 0), detail)
				// Physical table-size sums can decrease; changed paired counters prove a fresh sample.
				g.Expect(total).NotTo(Equal(baseline.BytesTotal.Value()), detail)
				g.Expect(done).NotTo(Equal(baseline.BytesDone.Value()), detail)
			}, time.Minute, time.Second).Should(Succeed(),
				"sampling must recover after unlocking")
			Expect(runnerProgressProcesses(runner, false)).To(ContainElements(workers),
				"sampling recovery must preserve the worker processes")
		}
		approveCutover(name)
		expectSingleAttempt(waitCompleted(name, nsE2E))
		Expect(psql(targetCluster, "SELECT count(*) FROM "+table)).To(Equal("2001"))
		Expect(seedTableCounts(targetCluster)).To(Equal(seedTableCounts(sourceCluster)))
	})
})

const progressProbeTable = "public.progress_lock_probe"

func progressProbeSetupSQL(role string) string {
	return "SET ROLE " + role + "; CREATE TABLE " + progressProbeTable + " (id integer PRIMARY KEY, payload text); " +
		"ALTER TABLE " + progressProbeTable + " ALTER COLUMN payload SET STORAGE EXTERNAL; " +
		"INSERT INTO " + progressProbeTable + " VALUES (0, 'baseline')"
}

func TestProgressProbePublicationOwnership(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for the progress fixture regression")
	}
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatal("invalid test PostgreSQL URI")
	}
	runSQL := func(databaseURI, sql string) string {
		t.Helper()
		commandCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(commandCtx, "psql", databaseURI,
			"-XqtA", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-c", sql).Output()
		if err != nil {
			if exit, ok := err.(*exec.ExitError); ok {
				t.Fatalf("progress fixture SQL failed: %s", exit.Stderr)
			}
			t.Fatalf("progress fixture SQL failed: %v", err)
		}
		return strings.TrimSpace(string(out))
	}
	role := fmt.Sprintf("progress_probe_%d", time.Now().UnixNano())
	runSQL(uri, "CREATE ROLE "+role+" NOLOGIN NOSUPERUSER")
	t.Cleanup(func() { runSQL(uri, "DROP ROLE "+role) })
	runSQL(uri, "CREATE DATABASE "+role+" OWNER "+role)
	t.Cleanup(func() { runSQL(uri, "DROP DATABASE "+role+" WITH (FORCE)") })
	u.Path = "/" + role
	probeURI := u.String()
	runSQL(probeURI, progressProbeSetupSQL(role))
	runSQL(probeURI, "SET ROLE "+role+"; CREATE PUBLICATION progress_probe FOR TABLE "+progressProbeTable)
	if got := runSQL(probeURI, "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='"+
		progressProbeTable+"'::regclass"); got != role {
		t.Fatal("the progress fixture is not owned by its migration role")
	}
	if got := runSQL(probeURI, "SELECT attstorage FROM pg_attribute WHERE attrelid='"+
		progressProbeTable+"'::regclass AND attname='payload'"); got != "e" {
		t.Fatal("the progress fixture payload does not use uncompressed external storage")
	}
	if got := runSQL(probeURI, progressLockSnapshotSQL(progressProbeTable)); got != "0 0 0 0 0 0 0" {
		t.Fatal("unowned progress lock snapshot was not empty")
	}
	lockedSnapshot := "SET application_name='e2e_progress_blocker'; BEGIN; LOCK " +
		progressProbeTable + " IN ACCESS EXCLUSIVE MODE; " + progressLockSnapshotSQL(progressProbeTable) + "; COMMIT"
	if got := runSQL(probeURI, lockedSnapshot); got != "1 1 0 0 0 0 0" {
		t.Fatal("owned progress lock snapshot did not identify the granted lock")
	}
}

const progressBlockerCount = "SELECT count(*) FROM pg_stat_activity WHERE application_name='e2e_progress_blocker'"

func progressLockSnapshotSQL(table string) string {
	return `WITH owned AS (
  SELECT pid, wait_event_type FROM pg_stat_activity
  WHERE datname=current_database() AND application_name='e2e_progress_blocker'
), exclusive AS (
  SELECT l.granted FROM pg_locks l JOIN owned USING (pid)
  WHERE l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
    AND l.relation='` + table + `'::regclass AND l.mode='AccessExclusiveLock'
), blockers AS (
  SELECT DISTINCT unnest(pg_blocking_pids(pid)) AS pid FROM owned
)
SELECT (SELECT count(*) FROM owned) || ' ' ||
  (SELECT count(*) FROM exclusive WHERE granted) || ' ' ||
  (SELECT count(*) FROM exclusive WHERE NOT granted) || ' ' ||
  (SELECT count(*) FROM owned WHERE wait_event_type='Lock') || ' ' ||
  (SELECT count(*) FROM blockers) || ' ' ||
  (SELECT count(*) FROM blockers JOIN pg_stat_activity USING (pid)
    WHERE application_name LIKE 'pgcopydb%') || ' ' ||
  (SELECT count(*) FROM blockers JOIN pg_stat_activity USING (pid)
    WHERE state IN ('idle in transaction', 'idle in transaction (aborted)'))`
}

func parseProgressLockSnapshot(raw string) ([7]int64, bool) {
	var counts [7]int64
	fields := strings.Fields(raw)
	if len(fields) != len(counts) {
		return counts, false
	}
	for i, field := range fields {
		n, err := strconv.ParseInt(field, 10, 64)
		if err != nil || n < 0 {
			return [7]int64{}, false
		}
		counts[i] = n
	}
	return counts, true
}

func TestProgressLockSnapshotProjection(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want [7]int64
		ok   bool
	}{
		{raw: "0 0 0 0 0 0 0\n", ok: true},
		{raw: "1 0 1 1 2 1 1", want: [7]int64{1, 0, 1, 1, 2, 1, 1}, ok: true},
		{raw: ""},
		{raw: "1 0 0"},
		{raw: "1 0 0 0 0 0 0 extra"},
		{raw: "1 0 0 0 0 0 PRIVATE_OUTPUT"},
		{raw: "1 0 0 0 0 0 -1"},
		{raw: "1 0 0 0 0 0 9223372036854775808"},
	} {
		if got, ok := parseProgressLockSnapshot(tc.raw); got != tc.want || ok != tc.ok {
			t.Fatal("progress lock snapshot escaped its numeric projection")
		}
	}
}

func holdProgressLock(side, cluster, table string) func() {
	GinkgoHelper()
	readPrimary := func() (corev1.Pod, bool) {
		lookupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		pods := &corev1.PodList{}
		err := k8sClient.List(lookupCtx, pods, client.InNamespace(nsE2E), client.MatchingLabels{
			labelCNPGCluster: cluster, labelCNPGRole: rolePrimary,
		})
		if err != nil || len(pods.Items) != 1 {
			return corev1.Pod{}, false
		}
		return pods.Items[0], true
	}
	var holder corev1.Pod
	Eventually(func() bool {
		var ok bool
		holder, ok = readPrimary()
		return ok
	}, primaryTimeout, 2*time.Second).Should(BeTrue(), "%s lock primary lookup failed", side)
	pinnedSQL := func(sql string) (string, bool) {
		queryCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		out, err := commandOutput(queryCtx, exec.CommandContext, "kubectl", "exec", "-n", nsE2E,
			holder.Name, "-c", "postgres", "--", "psql", "-U", "postgres", appDB, "-XqtA",
			"-v", "ON_ERROR_STOP=1", "-c", "SET statement_timeout=3000", "-c", sql)
		return strings.TrimSpace(string(out)), err == nil
	}
	lockCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	cmd := exec.CommandContext(lockCtx, "kubectl", "exec", "-n", nsE2E, holder.Name, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-XqtA", "-v", "ON_ERROR_STOP=1",
		"-c", "SET application_name='e2e_progress_blocker'; SET statement_timeout=150000",
		"-c", "BEGIN; LOCK "+table+" IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(140)")
	if err := cmd.Start(); err != nil {
		cancel()
		Fail(side + " lock process could not start")
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_, terminated := pinnedSQL("SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
			"WHERE datname=current_database() AND application_name='e2e_progress_blocker'")
		cancel()
		<-done
		count, checked := pinnedSQL(progressBlockerCount)
		Expect(terminated).To(BeTrue(), "%s lock cleanup request failed", side)
		Expect(checked && count == "0").To(BeTrue(), "%s lock cleanup absence check failed", side)
	}
	DeferCleanup(release)
	primaryMatch, snapshotValid := true, false
	var snapshot [7]int64
	diagnostic := func() string {
		live, exitCode := true, -1
		select {
		case <-done:
			live, exitCode = false, cmd.ProcessState.ExitCode()
		default:
		}
		return fmt.Sprintf("%s lock: process_live=%t exit_status=%d primary_match=%t snapshot_valid=%t "+
			"owned=%d granted=%d waiting=%d lock_waiting=%d blockers=%d worker_blockers=%d idle_transaction_blockers=%d",
			side, live, exitCode, primaryMatch, snapshotValid,
			snapshot[0], snapshot[1], snapshot[2], snapshot[3], snapshot[4], snapshot[5], snapshot[6])
	}
	assertBound := func() {
		current, ok := readPrimary()
		primaryMatch = ok && current.Name == holder.Name && current.UID == holder.UID
		if !primaryMatch {
			StopTrying("progress lock primary drift or lookup failure: " + diagnostic()).Now()
		}
		select {
		case <-done:
			StopTrying("progress lock process exited before readiness: " + diagnostic()).Now()
		default:
		}
	}
	// Remote lock setup has its own observation budget, separate from sampler deadlines.
	Eventually(func(g Gomega) {
		assertBound()
		raw, queried := pinnedSQL(progressLockSnapshotSQL(table))
		counts, valid := parseProgressLockSnapshot(raw)
		snapshotValid = queried && valid
		if snapshotValid {
			snapshot = counts
		}
		assertBound()
		g.Expect(snapshotValid).To(BeTrue(), diagnostic())
		g.Expect(snapshot[0]).To(Equal(int64(1)), diagnostic())
		g.Expect(snapshot[1]).To(Equal(int64(1)), diagnostic())
	}, time.Minute, 200*time.Millisecond).Should(Succeed())
	return release
}

func runnerProgressProcesses(pod string, sampler bool) []string {
	GinkgoHelper()
	kind := "worker"
	if sampler {
		kind = "sampler"
	}
	commandCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Only process identities leave the pod; cmdline may contain credentials.
	script := `kind=$1
for p in /proc/[0-9]*; do
  [ -r "$p/comm" ] || continue
  read -r comm < "$p/comm" || continue
  case "$kind:$comm" in
    sampler:psql)
      args=$(tr '\000' ' ' < "$p/cmdline" 2>/dev/null) || continue
      case "$args" in *'with t as ('*|*string_agg*) ;; *) continue ;; esac ;;
    worker:pgcopydb*) [ "$p" = /proc/1 ] || continue ;;
    *) continue ;;
  esac
  read -r stat < "$p/stat" || continue
  stat=${stat##*) }
  set -- $stat
  n=1
  while [ "$n" -lt 20 ]; do shift; n=$((n+1)); done
  printf '%s:%s\n' "${p##*/}" "$1"
done`
	out, err := exec.CommandContext(commandCtx, "kubectl", "exec", "-n", nsE2E, pod,
		"-c", "pgcopydb", "--", "sh", "-c", script, "sh", kind).Output()
	Expect(err).NotTo(HaveOccurred(), "could not inspect runner process identities")
	return strings.Fields(string(out))
}
