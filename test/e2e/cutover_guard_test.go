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
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

func slotSenderPID(output string) (int64, error) {
	pid, err := strconv.ParseInt(strings.TrimSpace(output), 10, 32)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("expected one positive slot walsender PID")
	}
	return pid, nil
}

func TestSlotSenderPID(t *testing.T) {
	for _, tc := range []struct {
		name, output string
		want         int64
	}{
		{"single", "123", 123},
		{"whitespace", " 123\n", 123},
		{"missing", "", 0},
		{"null", "\n", 0},
		{"zero", "0", 0},
		{"negative", "-123", 0},
		{"multiple", "123\n456", 0},
		{"duplicate", "123\n123", 0},
		{"malformed", "123; kill 1", 0},
		{"overflow", "2147483648", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := slotSenderPID(tc.output)
			if tc.want == 0 {
				if err == nil {
					t.Fatalf("slotSenderPID(%q) = %d, want an error", tc.output, got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("slotSenderPID(%q) = %d, %v, want %d", tc.output, got, err, tc.want)
			}
		})
	}
}

//nolint:gocyclo // Keep this ordered scenario and its bounded failure diagnostics together.
func earlyManualCutover() {
	const name = "e2e-follow-early"
	const marker = "early-cutover-"
	const backlogRows = 20000
	mig := newFollowMigration(name, v1beta1.CutoverManual)
	mig.Spec.Cutover.Approved = true
	// The normal allowance accounts for WAL beyond applied rows; the paused burst must still exceed it.
	threshold := resource.MustParse("16Mi")
	mig.Spec.Follow.MaxCatchupLag = &threshold
	DeferCleanup(func() {
		deleteMigration(name)
		Eventually(sourceSlotCount, 3*time.Minute, 2*time.Second).Should(Equal("0"))
		Eventually(targetOriginCount, 3*time.Minute, 2*time.Second).Should(Equal("0"))
		for _, cluster := range []string{sourceCluster, targetCluster} {
			psql(cluster, "DELETE FROM orders WHERE note LIKE '"+marker+"%'")
		}
	})
	create(mig)
	m := &v1beta1.Migration{}
	readMigration := func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mig), m)).To(Succeed())
		g.Expect(m.Status.Phase).NotTo(Equal(v1beta1.PhaseFailed), failureMessage(m))
	}
	slot := pgcopydb.SlotName(mig.Namespace, mig.Name)
	// A slot being created also has an active PID; wait until its snapshot is usable.
	query := fmt.Sprintf("SELECT s.active_pid FROM pg_replication_slots s JOIN pg_stat_replication r "+
		"ON r.pid=s.active_pid WHERE s.slot_name='%s' AND s.database=current_database() "+
		"AND s.slot_type='logical' AND s.confirmed_flush_lsn IS NOT NULL AND r.state='streaming'", slot)
	var pid int64
	By("finding this migration's walsender before the base copy completes")
	Eventually(func(g Gomega) {
		readMigration(g)
		if apimeta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted) {
			StopTrying("base copy completed before the test could pause its walsender").Now()
		}
		out, err := psqlDBErr(sourceCluster, appDB, query)
		g.Expect(err).NotTo(HaveOccurred())
		pid, err = slotSenderPID(out)
		g.Expect(err).NotTo(HaveOccurred())
	}, migrationTimeout, 200*time.Millisecond).Should(Succeed())
	pod := primaryPod(sourceCluster)
	signalSender := func(signal string) error {
		signalCtx, cancel := context.WithTimeout(context.Background(), e2eCommandTimeout)
		defer cancel()
		// Recheck ownership on the captured pod before either signal, including cleanup.
		_, err := commandOutput(signalCtx, exec.CommandContext, "kubectl",
			"exec", "-n", nsE2E, pod, "-c", "postgres", "--", "sh", "-ceu",
			`actual=$(psql -U postgres "$1" -tAc "$2"); test "$actual" = "$3"; kill -"$4" "$3"`,
			"sh", appDB, query, strconv.FormatInt(pid, 10), signal)
		return err
	}
	resumed := false
	DeferCleanup(func() {
		if !resumed {
			Expect(signalSender("CONT")).To(Succeed(), "resume the test-owned walsender")
		}
	})
	Expect(signalSender("STOP")).To(Succeed())
	readMigration(Default)
	Expect(apimeta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCloneCompleted)).To(BeFalse(),
		"the walsender must be paused before clone completion")

	By("committing a bounded backlog while only this migration's sender is paused")
	psql(sourceCluster, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) "+
		"SELECT (g %% %d) + 1, 1, '%s' || g || repeat(md5(g::text), 32) "+
		"FROM generate_series(1, %d) g", scaled(50000), marker, backlogRows))
	var cutoverRetries int32
	countCutoverEvents := func(eventCtx context.Context) (int32, error) {
		events := &corev1.EventList{}
		if err := k8sClient.List(eventCtx, events, client.InNamespace(nsE2E)); err != nil {
			cutoverRetries = -1
			return -1, err
		}
		var count int32
		cutoverRetries = 0
		for _, e := range events.Items {
			if e.InvolvedObject.UID != mig.UID {
				continue
			}
			if e.Type == corev1.EventTypeNormal && e.Reason == "CutoverStarted" {
				count += max(e.Count, 1)
			}
			if e.Type == corev1.EventTypeWarning && e.Reason == "CutoverRetry" {
				cutoverRetries += max(e.Count, 1)
			}
		}
		return count, nil
	}
	cutoverEvents := func(g Gomega) int32 {
		count, err := countCutoverEvents(ctx)
		g.Expect(err).To(Succeed())
		return count
	}
	By("requiring Streaming with measurable backlog and no cutover despite approval")
	Eventually(func(g Gomega) {
		readMigration(g)
		g.Expect(m.Status.Phase).To(Equal(v1beta1.PhaseStreaming))
		caughtUp := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCaughtUp)
		g.Expect(caughtUp).NotTo(BeNil())
		g.Expect(caughtUp.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(caughtUp.Reason).To(Equal("Lagging"))
		g.Expect(m.Status.Replication).NotTo(BeNil())
		g.Expect(m.Status.Replication.LagBytes).NotTo(BeNil())
		g.Expect(*m.Status.Replication.LagBytes).To(BeNumerically(">", threshold.Value()))
		g.Expect(m.Status.Replication.Endpos).To(BeEmpty())
		g.Expect(cutoverEvents(g)).To(BeZero())
	}, migrationTimeout, time.Second).Should(Succeed())

	By("resuming the sender with source writes stopped and observing automatic use of the existing approval")
	Expect(signalSender("CONT")).To(Succeed())
	resumed = true
	diagnosticStart := time.Now()
	diagnosticDeadline := diagnosticStart.Add(lagConvergeTimeout)
	const missingValue = "missing"
	lsnPattern := `(?:[0-9A-F]{1,8}/[0-9A-F]{1,8}|missing)`
	lsnValid := regexp.MustCompile("^" + lsnPattern + "$")
	sourceValid := regexp.MustCompile("^" + strings.Repeat(lsnPattern+" ", 4) +
		`[01] (startup|catchup|streaming|backup|stopping|missing)$`)
	safeWord := func(value, allowed string) string {
		if value == "" || strings.ContainsAny(value, "|\n\r") || !strings.Contains("|"+allowed+"|", "|"+value+"|") {
			return "unknown"
		}
		return value
	}
	safeLSN := func(value string) string {
		if !lsnValid.MatchString(value) {
			return missingValue
		}
		return value
	}
	sourceSQL := "SELECT concat_ws(' ', pg_current_wal_flush_lsn(), coalesce(r.write_lsn::text,'missing'), " +
		"coalesce(r.replay_lsn::text,'missing'), coalesce(s.confirmed_flush_lsn::text,'missing'), " +
		"s.active::int, coalesce(r.state,'missing')) FROM pg_replication_slots s " +
		"LEFT JOIN pg_stat_replication r ON r.pid=s.active_pid WHERE s.slot_name='" + slot +
		"' AND s.database=current_database() AND s.slot_type='logical'"
	lastSnapshot := "snapshot_unavailable"
	snapshot := func(stage string) {
		remaining := time.Until(diagnosticDeadline)
		if remaining <= 0 {
			return
		}
		probeCtx, cancel := context.WithTimeout(context.Background(), min(2*time.Second, remaining))
		defer cancel()
		values := []string{"source_probe_unavailable", "target_probe_unavailable"}
		for side, database := range []string{sourceCluster, targetCluster} {
			if probeCtx.Err() != nil {
				break
			}
			primaries := &corev1.PodList{}
			if err := k8sClient.List(probeCtx, primaries, client.InNamespace(nsE2E), client.MatchingLabels{
				labelCNPGCluster: database, labelCNPGRole: rolePrimary,
			}); err != nil || len(primaries.Items) != 1 || probeCtx.Err() != nil {
				continue
			}
			sql := sourceSQL
			if side == 1 {
				sql = "SELECT count(*) FROM orders WHERE note LIKE '" + marker + "%'"
			}
			out, err := commandOutput(probeCtx, exec.CommandContext, "kubectl", "exec", "-n", nsE2E,
				primaries.Items[0].Name, "-c", "postgres", "--", "psql", "-U", "postgres", appDB,
				"-XAtq", "-v", "ON_ERROR_STOP=1", "-c", "SET statement_timeout=1000", "-c", sql)
			value := strings.TrimSpace(string(out))
			if err != nil {
				continue
			}
			if side == 0 && sourceValid.MatchString(value) {
				values[side] = "source_head/write/replay/confirmed/active/state=" + value
			} else if count, parseErr := strconv.ParseInt(value, 10, 64); side == 1 && parseErr == nil && count >= 0 {
				values[side] = "target_marker_rows=" + strconv.FormatInt(count, 10)
			}
		}
		lastSnapshot = fmt.Sprintf("at=%s stage=%s %s",
			time.Now().UTC().Format(time.RFC3339Nano), stage, strings.Join(values, " "))
		_, _ = fmt.Fprintln(GinkgoWriter, "cutover snapshot", lastSnapshot)
	}
	lastObservation, observationCount := "status_unavailable", 0
	observe := func(started int32) {
		status, reason, lag, write, replay := missingValue, missingValue, missingValue, missingValue, missingValue
		if c := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCaughtUp); c != nil {
			status = safeWord(string(c.Status), "True|False|Unknown")
			reason = safeWord(c.Reason, "Lagging|ConfirmingCatchUp|LagBelowThreshold")
		}
		endpos := false
		if rep := m.Status.Replication; rep != nil {
			write, replay, endpos = safeLSN(rep.WriteLSN), safeLSN(rep.ReplayLSN), rep.Endpos != ""
			if rep.LagBytes != nil {
				lag = strconv.FormatInt(*rep.LagBytes, 10)
			}
		}
		phase := safeWord(string(m.Status.Phase), "Pending|Validating|Cloning|Finalizing|Streaming|"+
			"CutoverPending|CuttingOver|Verifying|Completed|Failed|Suspended")
		events := "available"
		if started < 0 {
			events = "events_unavailable"
		}
		value := fmt.Sprintf("phase=%s attempts=%d caught_up=%s reason=%s lag=%s "+
			"write=%s replay=%s endpos=%t started=%d retries=%d events=%s",
			phase, m.Status.Attempts, status, reason, lag, write, replay, endpos, started, cutoverRetries, events)
		if value != lastObservation && observationCount < 29 {
			_, _ = fmt.Fprintf(GinkgoWriter, "cutover observation at=%s %s\n",
				time.Now().UTC().Format(time.RFC3339Nano), value)
			observationCount++
		}
		lastObservation = value
	}
	DeferCleanup(func() {
		if CurrentSpecReport().Failed() {
			_, _ = fmt.Fprintf(GinkgoWriter, "cutover failure at=%s %s; last %s\n",
				time.Now().UTC().Format(time.RFC3339Nano), lastObservation, lastSnapshot)
		}
	})
	snapshots := 0
	Eventually(func(g Gomega) {
		readMigration(g)
		eventCtx, cancel := context.WithTimeout(ctx, min(2*time.Second, time.Until(diagnosticDeadline)))
		started, _ := countCutoverEvents(eventCtx)
		cancel()
		observe(started)
		if snapshots == 0 || (snapshots == 1 && time.Since(diagnosticStart) >= 30*time.Second) ||
			(snapshots == 2 && time.Until(diagnosticDeadline) <= 5*time.Second) {
			snapshot([]string{"resumed", "after_30s", "deadline"}[snapshots])
			snapshots++
		}
		g.Expect(m.Status.Replication).NotTo(BeNil(), "phase=%s: replication status absent", m.Status.Phase)
		g.Expect(m.Status.Replication.Endpos).NotTo(BeEmpty(), "phase=%s: cutover endpos absent", m.Status.Phase)
		g.Expect(started).To(Equal(int32(1)), "phase=%s: expected one cutover event", m.Status.Phase)
	}, lagConvergeTimeout, time.Second).Should(Succeed())
	Eventually(func(g Gomega) {
		readMigration(g)
		drain := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCutoverComplete)
		g.Expect(drain).NotTo(BeNil())
		g.Expect(drain.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(drain.Reason).To(Equal("DrainVerified"))
	}, followTimeout, time.Second).Should(Succeed())
	m = waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
	complete := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionComplete)
	Expect(complete).NotTo(BeNil())
	Expect(complete.Status).To(Equal(metav1.ConditionTrue))
	Expect(complete.Reason).To(Equal("MigrationSucceeded"))
	expectSingleAttempt(m)
	expectCleanupSucceeded(name)
	Expect(cutoverEvents(Default)).To(Equal(int32(1)))
	Expect(psql(targetCluster, "SELECT count(*) FROM orders WHERE note LIKE '"+marker+"%'")).
		To(Equal(strconv.Itoa(backlogRows)))
	Expect(sourceSlotCount()).To(Equal("0"))
	Expect(targetOriginCount()).To(Equal("0"))
}
