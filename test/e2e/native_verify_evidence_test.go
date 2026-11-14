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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	// nativeDrainReason is the CutoverComplete reason of a native drain.
	nativeDrainReason = "NativeDrainConfirmed"
	drainProved       = "1/true"
	drainUnproved     = "1/false"
)

// lsnText is the text form PostgreSQL gives a pg_lsn.
var lsnText = regexp.MustCompile(`^[0-9A-F]{1,8}/[0-9A-F]{1,8}$`)

// nativeDrainProofSQL reads the drain proof that run's retained target checkpoint records for endpos.
// It yields 1/true only for one completed row whose drain ends at endpos with a witness the engine
// admits: a keepalive at or after endpos and within the certified frontier, or a later BEGIN past it.
func nativeDrainProofSQL(run, endpos string) string {
	e := sqlLiteral(endpos) + "::pg_lsn"
	return "SELECT count(*) || '/' || coalesce(bool_and(follow_completed AND drain_end_lsn = " + e +
		" AND (drain_kind = 'keepalive' AND drain_witness_lsn >= drain_end_lsn AND drain_witness_lsn <= certified_lsn" +
		" OR drain_kind = 'later_begin' AND drain_witness_lsn > drain_end_lsn)), false)::text FROM ONLY " +
		sqlIdent(nativeSchemaName(run)) + ".checkpoint"
}

// nativeDrainError is why m's status shows no native drain, or nil. The native drain is
// proved without a compare, so it must leave no drain progress item.
func nativeDrainError(m *v1beta1.Migration) error {
	cutover := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionCutoverComplete)
	switch {
	case cutover == nil || cutover.Status != metav1.ConditionTrue || cutover.Reason != nativeDrainReason:
		return fmt.Errorf("%s has no CutoverComplete=True condition with reason NativeDrainConfirmed", m.Name)
	case m.Status.Replication == nil || !lsnText.MatchString(m.Status.Replication.Endpos):
		return fmt.Errorf("%s records no endpos", m.Name)
	case verificationProgress(m, v1beta1.VerificationProgressDrain) != nil:
		return fmt.Errorf("the native drain of %s compared nothing, yet left a drain item", m.Name)
	}
	return nil
}

// expectNativeDrainProof requires the native drain reason and the drain proof the target retains.
func expectNativeDrainProof(m *v1beta1.Migration) {
	GinkgoHelper()
	Expect(nativeDrainError(m)).To(Succeed())
	run, err := nativeFaultRunID(m.UID)
	Expect(err).NotTo(HaveOccurred())
	Expect(psql(targetCluster, nativeDrainProofSQL(run, m.Status.Replication.Endpos))).To(Equal(drainProved),
		"the target checkpoint of %s records no completed drain to endpos %s", m.Name, m.Status.Replication.Endpos)
}

// nativeFinalProgressError is why the kind item of m is not the native compare's final sample, or nil.
// Only the data compare records one, from its checked result, after its helper Job finished.
func nativeFinalProgressError(m *v1beta1.Migration, kind string) error {
	if kind != v1beta1.VerificationProgressData {
		return fmt.Errorf("the native engine records no %s progress", kind)
	}
	if m.Status.NativeDataJobUID == "" {
		return fmt.Errorf("%s pins no native data compare helper", m.Name)
	}
	var result *v1beta1.VerificationResult
	for i := range m.Status.Verification {
		if m.Status.Verification[i].Check == compareDataCheck {
			result = &m.Status.Verification[i]
		}
	}
	p := verificationProgress(m, kind)
	switch {
	case result == nil || result.CompletedAt == nil:
		return fmt.Errorf("%s records no finished native data compare", m.Name)
	case p == nil || p.ObservedAt == nil:
		return fmt.Errorf("%s has no observed data item", m.Name)
	case p.ObservedAt.Before(result.CompletedAt):
		return errors.New("the data item was observed before its compare finished")
	case p.FailedTables != 0 || p.PlannedBytes == nil || p.ComparedBytes == nil ||
		p.PlannedBytes.Cmp(*p.ComparedBytes) != 0:
		return fmt.Errorf("the final data sample of %s left bytes uncompared or tables failed", m.Name)
	}
	return nil
}

func nativeDrainedMigration() *v1beta1.Migration {
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: "m"}}
	m.Status.Conditions = []metav1.Condition{
		{Type: v1beta1.ConditionCutoverComplete, Status: metav1.ConditionTrue, Reason: nativeDrainReason}}
	m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: "0/3000060"}
	m.Status.VerificationProgress = []v1beta1.VerificationProgress{{Kind: v1beta1.VerificationProgressData}}
	return m
}

func TestNativeDrainErrorNeedsTheNativeDrain(t *testing.T) {
	if err := nativeDrainError(nativeDrainedMigration()); err != nil {
		t.Fatalf("a native drain reads %v", err)
	}
	for name, mutate := range map[string]func(*v1beta1.Migration){
		"legacy reason":   func(m *v1beta1.Migration) { m.Status.Conditions[0].Reason = "DrainVerified" },
		"cutover pending": func(m *v1beta1.Migration) { m.Status.Conditions[0].Status = metav1.ConditionFalse },
		"no cutover":      func(m *v1beta1.Migration) { m.Status.Conditions = nil },
		"no replication":  func(m *v1beta1.Migration) { m.Status.Replication = nil },
		"no endpos":       func(m *v1beta1.Migration) { m.Status.Replication.Endpos = "" },
		"endpos not LSN":  func(m *v1beta1.Migration) { m.Status.Replication.Endpos = "0/3000060' OR true --" },
		"drain item": func(m *v1beta1.Migration) {
			m.Status.VerificationProgress = append(m.Status.VerificationProgress,
				v1beta1.VerificationProgress{Kind: v1beta1.VerificationProgressDrain})
		},
	} {
		m := nativeDrainedMigration()
		mutate(m)
		if nativeDrainError(m) == nil {
			t.Errorf("%s reads as a native drain", name)
		}
	}
}

func nativeComparedMigration() *v1beta1.Migration {
	done := metav1.NewTime(time.Unix(200, 0))
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: "m"}}
	m.Status.NativeDataJobUID = "data-helper"
	m.Status.Verification = []v1beta1.VerificationResult{{Check: compareDataCheck, Passed: true, CompletedAt: &done}}
	m.Status.VerificationProgress = []v1beta1.VerificationProgress{{
		Kind: v1beta1.VerificationProgressData, ObservedAt: &done, Final: true, PlannedTables: 3, ComparedTables: 3,
		PlannedBytes:  resource.NewQuantity(131072, resource.BinarySI),
		ComparedBytes: resource.NewQuantity(131072, resource.BinarySI)}}
	return m
}

func TestNativeFinalProgressErrorNeedsTheNativeCompare(t *testing.T) {
	if err := nativeFinalProgressError(nativeComparedMigration(), v1beta1.VerificationProgressData); err != nil {
		t.Fatalf("a native final sample reads %v", err)
	}
	for _, kind := range []string{v1beta1.VerificationProgressDrain, v1beta1.VerificationProgressRehearsal} {
		m := nativeComparedMigration()
		item := m.Status.VerificationProgress[0]
		item.Kind = kind
		m.Status.VerificationProgress = append(m.Status.VerificationProgress, item)
		if nativeFinalProgressError(m, kind) == nil {
			t.Errorf("native reads as recording %s progress", kind)
		}
	}
	for name, mutate := range map[string]func(*v1beta1.Migration){
		"no data helper": func(m *v1beta1.Migration) { m.Status.NativeDataJobUID = "" },
		"no result":      func(m *v1beta1.Migration) { m.Status.Verification = nil },
		"schema result":  func(m *v1beta1.Migration) { m.Status.Verification[0].Check = "schema" },
		"running":        func(m *v1beta1.Migration) { m.Status.Verification[0].CompletedAt = nil },
		"no item":        func(m *v1beta1.Migration) { m.Status.VerificationProgress = nil },
		"never observed": func(m *v1beta1.Migration) { m.Status.VerificationProgress[0].ObservedAt = nil },
		"observed early": func(m *v1beta1.Migration) {
			early := metav1.NewTime(time.Unix(199, 0))
			m.Status.VerificationProgress[0].ObservedAt = &early
		},
		"failed table": func(m *v1beta1.Migration) { m.Status.VerificationProgress[0].FailedTables = 1 },
		"bytes left": func(m *v1beta1.Migration) {
			m.Status.VerificationProgress[0].ComparedBytes = resource.NewQuantity(1, resource.BinarySI)
		},
		"no bytes": func(m *v1beta1.Migration) { m.Status.VerificationProgress[0].PlannedBytes = nil },
	} {
		m := nativeComparedMigration()
		mutate(m)
		if nativeFinalProgressError(m, v1beta1.VerificationProgressData) == nil {
			t.Errorf("%s reads as the native final sample", name)
		}
	}
}

func TestNativeDrainProofSQLReadsTheRecordedProof(t *testing.T) {
	run := testPSQL(t)
	const endpos = "0/3000060"
	schema := sqlIdent(nativeSchemaName(nativeFaultDecoyRun))
	proof := func(rows ...string) string {
		t.Helper()
		script := "BEGIN;\nCREATE SCHEMA " + schema + ";\nCREATE TABLE " + schema + ".checkpoint (" +
			"certified_lsn pg_lsn, drain_end_lsn pg_lsn, drain_witness_lsn pg_lsn, drain_kind text, " +
			"follow_completed bool NOT NULL);\n"
		for _, row := range rows {
			script += "INSERT INTO " + schema + ".checkpoint VALUES (" + row + ");\n"
		}
		return testQuery(t, run, script+nativeDrainProofSQL(nativeFaultDecoyRun, endpos)+";\nROLLBACK")
	}
	for name, tc := range map[string]struct {
		rows []string
		want string
	}{
		"keepalive at endpos":      {[]string{"'0/3000060', '0/3000060', '0/3000060', 'keepalive', true"}, drainProved},
		"keepalive past endpos":    {[]string{"'0/3000100', '0/3000060', '0/3000080', 'keepalive', true"}, drainProved},
		"later BEGIN past endpos":  {[]string{"'0/3000000', '0/3000060', '0/3000100', 'later_begin', true"}, drainProved},
		"keepalive before endpos":  {[]string{"'0/3000060', '0/3000060', '0/3000050', 'keepalive', true"}, drainUnproved},
		"keepalive past certified": {[]string{"'0/3000070', '0/3000060', '0/3000080', 'keepalive', true"}, drainUnproved},
		"later BEGIN at endpos":    {[]string{"'0/3000060', '0/3000060', '0/3000060', 'later_begin', true"}, drainUnproved},
		"another drain kind":       {[]string{"'0/3000060', '0/3000060', '0/3000060', 'exact', true"}, drainUnproved},
		"another endpos":           {[]string{"'0/3000100', '0/3000070', '0/3000080', 'keepalive', true"}, drainUnproved},
		"not completed":            {[]string{"'0/3000060', '0/3000060', '0/3000060', 'keepalive', false"}, drainUnproved},
		"no drain":                 {[]string{"'0/3000060', NULL, NULL, NULL, true"}, drainUnproved},
		"no row":                   {nil, "0/false"},
	} {
		if got := proof(tc.rows...); got != tc.want {
			t.Errorf("%s: the drain proof reads %q, want %q", name, got, tc.want)
		}
	}
	if !strings.Contains(nativeDrainProofSQL(nativeFaultDecoyRun, endpos), "FROM ONLY "+schema+".checkpoint") {
		t.Error("the drain proof does not read the run's own checkpoint")
	}
}
