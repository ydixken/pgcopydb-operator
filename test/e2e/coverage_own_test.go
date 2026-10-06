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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// coverageOwnRun is an own case whose @setup ran and whose Migration exists.
type coverageOwnRun struct {
	c        coverageCase
	m        *v1beta1.Migration
	identity string
	captured map[string]comparePodLog
}

// coverageOwnOutcomes drive an own case's Migration from creation to the
// pinned outcome; the header's expect= names one.
var coverageOwnOutcomes = map[string]func(coverageOwnRun){
	"apply_fails_on_ddl":           expectApplyFailsOnDDL,
	"large_objects_not_replicated": expectLargeObjectsNotReplicated,
	"preflight_refuses_rls":        func(r coverageOwnRun) { expectPreflightRefusal(r, rlsRefusal) },
	"preflight_refuses_unlogged":   func(r coverageOwnRun) { expectPreflightRefusal(r, unloggedRefusal) },
}

// Each own case runs one follow Migration of its own and holds it to the
// outcome its header names; see docs/reference/coverage.md.
var _ = Describe("Feature coverage", SpecPriority(2), func() {
	all, loadErr := loadCoverageCases(coverageFS, coverageOutcomes)
	var own []coverageCase
	for _, c := range all {
		if c.group == coverageGroupOwn {
			own = append(own, c)
		}
	}

	It("embeds own cases", func() {
		Expect(loadErr).NotTo(HaveOccurred())
		Expect(own).NotTo(BeEmpty())
	})

	for _, c := range own {
		It("pins "+c.path+" to "+c.expect, func() {
			requireCNPGFixtures()
			if pgSource < c.minPG {
				Skip(fmt.Sprintf("needs a PostgreSQL %d source, this pair has %d", c.minPG, pgSource))
			}
			resetMigrationPair()
			identity := fmt.Sprint(time.Now().UnixNano())
			m, captured := prepareCoverageFollow("e2e-cov-own-"+identity, []coverageCase{c}, identity)
			// A failing apply fails the same way on every retry; one retry shows it.
			m.Spec.BackoffLimit = 1
			create(m)
			coverageOwnOutcomes[c.expect](coverageOwnRun{c: c, m: m, identity: identity, captured: captured})
		})
	}
})

// expectPreflightRefusal requires the preflight to refuse every plain table
// of the case's schema with refusal.
func expectPreflightRefusal(r coverageOwnRun, refusal string) {
	GinkgoHelper()
	failed := waitFailed(r.m.Name, reasonPreflightFailed)
	schema := r.c.schemaNames(r.identity)[0]
	tables := psql(sourceCluster, "SELECT string_agg(format('%I.%I', n.nspname, c.relname), ', ' ORDER BY c.relname)"+
		" FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"+
		" WHERE n.nspname = "+sqlLiteral(schema)+" AND c.relkind = 'r'")
	Expect(checkPreflightRefusal(failed, refusal, tables)).To(Succeed())
}

// expectApplyFailsOnDDL runs @follow, whose DDL the target never gets, and
// requires every attempt to fail on the column the target lacks.
func expectApplyFailsOnDDL(r coverageOwnRun) {
	GinkgoHelper()
	runCoverageFollow(r.m, []coverageCase{r.c}, r.identity)
	failed := waitFailed(r.m.Name, reasonBackoffLimitExceeded)
	logs := make([]string, failed.Status.Attempts)
	for i := range logs {
		logs[i] = jobLogs(fmt.Sprintf("%s-run-%d", r.m.Name, i+1), attemptLogTail)
	}
	Expect(checkMissingColumnFailure(failed, logs)).To(Succeed())
}

// expectLargeObjectsNotReplicated cuts over after @follow changed large
// objects, and requires verification to pass while the target keeps them unchanged.
func expectLargeObjectsNotReplicated(r coverageOwnRun) {
	GinkgoHelper()
	cases := []coverageCase{r.c}
	runCoverageFollow(r.m, cases, r.identity)
	completed := cutOverCoverageFollow(r.m, r.identity, r.captured)
	expectVerification(completed, true)
	diffs := coverageCaseDiffs(cases, r.identity)[r.c.path]
	Expect(checkLargeObjectsNotReplicated(diffs)).To(Succeed())
}
