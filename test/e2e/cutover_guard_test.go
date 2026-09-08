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

func earlyManualCutover() {
	const name = "e2e-follow-early"
	const marker = "early-cutover-"
	mig := newFollowMigration(name, v1beta1.CutoverManual)
	mig.Spec.Cutover.Approved = true
	threshold := resource.MustParse("1Mi")
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
		"FROM generate_series(1, 10000) g", scaled(50000), marker))
	cutoverEvents := func(g Gomega) int32 {
		events := &corev1.EventList{}
		g.Expect(k8sClient.List(ctx, events, client.InNamespace(nsE2E))).To(Succeed())
		var count int32
		for _, e := range events.Items {
			if e.InvolvedObject.UID == mig.UID && e.Type == corev1.EventTypeNormal && e.Reason == "CutoverStarted" {
				count += max(e.Count, 1)
			}
		}
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
	Eventually(func(g Gomega) {
		readMigration(g)
		g.Expect(m.Status.Replication).NotTo(BeNil(), "phase=%s: replication status absent", m.Status.Phase)
		g.Expect(m.Status.Replication.Endpos).NotTo(BeEmpty(), "phase=%s: cutover endpos absent", m.Status.Phase)
		g.Expect(cutoverEvents(g)).To(Equal(int32(1)), "phase=%s: expected one cutover event", m.Status.Phase)
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
	Expect(psql(targetCluster, "SELECT count(*) FROM orders WHERE note LIKE '"+marker+"%'")).To(Equal("10000"))
	Expect(sourceSlotCount()).To(Equal("0"))
	Expect(targetOriginCount()).To(Equal("0"))
}
