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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// deparsedAspects hold text each server deparses itself, which differs between
// majors, so a cross-major pair skips them (one string keeps goconst quiet).
var deparsedAspects = func() map[string]bool {
	set := map[string]bool{}
	for aspect := range strings.FieldsSeq("constraint function index policy statistics trigger view") {
		set[aspect] = true
	}
	return set
}()

// clusterPSQL reaches cluster's migration database as its admin role.
func clusterPSQL(cluster string) psqlScript {
	return func(ctx context.Context, script string) (string, error) {
		argv := psqlArgv(cluster, primaryPod(cluster), appDatabase(cluster), true, "-XqtA", "-v", "ON_ERROR_STOP=1")
		return runPSQLScript(exec.CommandContext(ctx, "kubectl", argv...), script)
	}
}

// The clone group moves every clone case in one filtered Migration and holds
// the target to the source's fingerprint; see docs/reference/coverage.md.
var _ = Describe("Feature coverage", SpecPriority(1), func() {
	BeforeEach(func() {
		// cov_reader is a cluster-wide role, beyond the two supplied databases.
		requireCNPGFixtures()
		// The target must hold no large objects from an earlier clone.
		resetMigrationPair()
	})

	It("clones every clone-group case identically", func() {
		cases := coverageGroup(coverageGroupClone)
		identity := fmt.Sprint(time.Now().UnixNano())
		schemas := coverageSchemaNames(cases, identity)
		m := newMigration("e2e-cov-clone-"+identity, nsE2E, v1beta1.CloneOptions{
			Filters: &v1beta1.Filters{IncludeOnlySchemas: schemas},
		})
		m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
		createCoverageSchemas(m, identity, schemas)
		for _, c := range cases {
			By("setting up " + c.path)
			applyCoverageSection(c, c.setup, identity)
		}

		captured := map[string]comparePodLog{}
		reportVerificationOnFailure(m, captured)
		create(m)
		completed := waitCompletedCapturing(m, captured)

		diffs := coverageDiffs(cases, identity)
		if diffs != "" {
			AddReportEntry("coverage fingerprint differences", diffs, ReportEntryVisibilityFailureOrVerbose)
		}
		expectVerification(completed, true)
		Expect(diffs).To(BeEmpty(), "the target differs from the source")
	})
})

// createCoverageSchemas creates cov_reader on both servers and the stamped
// schemas, whose cleanup unlinks the large objects each schema lists.
func createCoverageSchemas(m *v1beta1.Migration, identity string, schemas []string) {
	GinkgoHelper()
	for _, cluster := range []string{sourceCluster, targetCluster} {
		psql(cluster, ensureCoverageReaderSQL)
	}
	createStampedSchemas(m, identity, "pgcopydb-e2e-coverage:"+identity, schemas, func(cluster, schema string) {
		psql(cluster, "SET ROLE "+sqlIdent(appRole(cluster))+"; "+unlinkCoverageLargeObjectsSQL(schema))
	})
}

// applyCoverageSection runs one section of c on the source as the app role.
func applyCoverageSection(c coverageCase, section, identity string) {
	GinkgoHelper()
	sectionCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	Expect(applyCoverageCase(sectionCtx, clusterPSQL(sourceCluster), c, section, asSourceAppRole(),
		c.schemaNames(identity))).To(Succeed())
}

// coverageGroup is the group's embedded cases the source major can run. A
// group without cases fails, so it cannot pass by running nothing.
func coverageGroup(group string) []coverageCase {
	GinkgoHelper()
	all, err := loadCoverageCases(coverageFS, coverageOutcomes)
	Expect(err).NotTo(HaveOccurred())
	var cases []coverageCase
	for _, c := range all {
		if c.group == group && pgSource >= c.minPG {
			cases = append(cases, c)
		}
	}
	Expect(cases).NotTo(BeEmpty(), "no %s cases for a PostgreSQL %d source", group, pgSource)
	return cases
}

// coverageDiffs reports every differing aspect per case, or "" when the
// sides match.
func coverageDiffs(cases []coverageCase, identity string) string {
	GinkgoHelper()
	byCase := coverageCaseDiffs(cases, identity)
	var report strings.Builder
	for _, c := range cases {
		for _, d := range byCase[c.path] {
			fmt.Fprintf(&report, "%s: %s\n", c.path, d)
		}
	}
	return report.String()
}

// coverageCaseDiffs fingerprints both sides once and returns the differing
// aspects by case path. A cross-major pair skips the aspects each server
// deparses itself.
func coverageCaseDiffs(cases []coverageCase, identity string) map[string][]fingerprintDiff {
	GinkgoHelper()
	schemas := coverageSchemaNames(cases, identity)
	read := func(cluster string) []fingerprintRow {
		readCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		rows, err := readFingerprint(readCtx, clusterPSQL(cluster), schemas)
		Expect(err).NotTo(HaveOccurred(), "fingerprint on %s", cluster)
		return rows
	}
	source, target := read(sourceCluster), read(targetCluster)
	var skip map[string]bool
	if pgSource != pgTarget {
		skip = deparsedAspects
	}
	byCase := map[string][]fingerprintDiff{}
	for _, c := range cases {
		names := c.placeholders(identity)
		byCase[c.path] = diffFingerprint(caseFingerprint(source, names), caseFingerprint(target, names), skip)
	}
	return byCase
}

// coverageSchemaNames lists every schema of cases for one run.
func coverageSchemaNames(cases []coverageCase, identity string) []string {
	schemas := make([]string, 0, 2*len(cases))
	for _, c := range cases {
		schemas = append(schemas, c.schemaNames(identity)...)
	}
	return schemas
}
