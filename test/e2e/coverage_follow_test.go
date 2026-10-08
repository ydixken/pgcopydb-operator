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

// The follow group streams every follow case's @follow through one Manual
// cutover and holds the target to the source; see docs/reference/coverage.md.
var _ = Describe("Feature coverage", SpecPriority(3), func() {
	BeforeEach(func() {
		requireCNPGFixtures()
		resetMigrationPair()
	})

	It("follows every follow-group case identically through cutover", func() {
		cases := coverageGroup(coverageGroupFollow)
		identity := fmt.Sprint(time.Now().UnixNano())
		m, captured := prepareCoverageFollow("e2e-cov-follow-"+identity, cases, identity)
		create(m)
		runCoverageFollow(m, cases, identity)
		completed := cutOverCoverageFollow(m, identity, captured)
		expectCoverageIdentical(cases, identity, completed)
	})
})

// expectCoverageIdentical requires passed verification on completed and the
// same fingerprint on both sides for every case, reporting each difference.
func expectCoverageIdentical(cases []coverageCase, identity string, completed *v1beta1.Migration) {
	GinkgoHelper()
	diffs := coverageDiffs(cases, identity)
	if diffs != "" {
		AddReportEntry("coverage fingerprint differences", diffs, ReportEntryVisibilityFailureOrVerbose)
	}
	expectVerification(completed, true)
	Expect(diffs).To(BeEmpty(), "the target differs from the source")
}

// coverageMarkerSchema holds the row that proves the stream reached the
// target. No case schema can take this name: they start with cov_ or cov2_.
func coverageMarkerSchema(identity string) string {
	return "covmark_" + identity
}

// prepareCoverageFollow creates the stamped schemas of cases and the marker
// schema, runs every @setup on the source, and returns the Manual-cutover
// follow Migration over them, not yet created, with its compare-log capture.
func prepareCoverageFollow(
	name string, cases []coverageCase, identity string,
) (*v1beta1.Migration, map[string]comparePodLog) {
	GinkgoHelper()
	marker := coverageMarkerSchema(identity)
	schemas := append(coverageSchemaNames(cases, identity), marker)
	m := newFollowMigration(name, v1beta1.CutoverManual)
	m.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlySchemas: schemas}
	m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
	createCoverageSchemas(m, identity, schemas)
	for _, c := range cases {
		By("setting up " + c.path)
		applyCoverageSection(c, c.setup, identity)
	}
	psql(sourceCluster, asSourceAppRole()+"CREATE TABLE "+sqlIdent(marker)+".marker (id int PRIMARY KEY)")
	captured := map[string]comparePodLog{}
	reportVerificationOnFailure(m, captured)
	return m, captured
}

// runCoverageFollow waits until m is caught up at the Manual gate, then runs
// every case's @follow on the source while the stream is live.
func runCoverageFollow(m *v1beta1.Migration, cases []coverageCase, identity string) {
	GinkgoHelper()
	waitFollowStreaming(m.Name)
	waitPhase(m.Name, nsE2E, lagConvergeTimeout, v1beta1.PhaseCutoverPending)
	for _, c := range cases {
		By("following " + c.path)
		applyCoverageSection(c, c.follow, identity)
	}
}

// cutOverCoverageFollow waits for a marker written after every @follow to
// reach the target, approves the cutover and returns the Completed Migration.
func cutOverCoverageFollow(
	m *v1beta1.Migration, identity string, captured map[string]comparePodLog,
) *v1beta1.Migration {
	GinkgoHelper()
	table := sqlIdent(coverageMarkerSchema(identity)) + ".marker"
	psql(sourceCluster, asSourceAppRole()+"INSERT INTO "+table+" VALUES (1)")
	waitOnTarget(m, "SELECT count(*) FROM "+table, "1")
	approveCutover(m.Name)
	completed := waitCompletedCapturing(m, captured)
	expectConditionTrue(completed, v1beta1.ConditionCutoverComplete)
	return completed
}
