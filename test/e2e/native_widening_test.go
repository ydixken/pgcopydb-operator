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
	"regexp"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	nativeWideningName = "e2e-native-widening"
	// nativeWideningMarker names the rows the spec writes while it streams, so cleanup finds them.
	nativeWideningMarker = "e2e-native-widening"
	// nativeWideningWriteSQL writes through each widened object: the domain CHECK, the citext
	// UNIQUE index, the expression and partial indexes and the generated column.
	nativeWideningWriteSQL = `INSERT INTO app_users (id, email, display_name, quota_percent)
SELECT max(id) + 1, '` + nativeWideningMarker + `@example.invalid', '` + nativeWideningMarker + `', 42.5 FROM app_users;
INSERT INTO readings (id, sensor, captured_at, samples, unit_price, quantity)
SELECT max(id) + 1, '` + nativeWideningMarker + `', now(), '{1.5}', 2.5, 7 FROM readings;
`
	nativeWideningDeleteSQL = "DELETE FROM app_users WHERE display_name = '" + nativeWideningMarker + "';\n" +
		"DELETE FROM readings WHERE sensor = '" + nativeWideningMarker + "';\n"
	// nativeWideningRowsSQL reads the written rows back, the generated total_price with them.
	nativeWideningRowsSQL = "SELECT (SELECT string_agg(email || '/' || quota_percent, ',') FROM app_users" +
		" WHERE display_name = '" + nativeWideningMarker + "') || '|' || (SELECT string_agg(quantity || '/' ||" +
		" total_price, ',') FROM readings WHERE sensor = '" + nativeWideningMarker + "')"
	// legacyRestoreSQL undoes fixtures/legacy.sql, so the source is the native fixture again.
	legacyRestoreSQL = `DROP MATERIALIZED VIEW IF EXISTS event_daily_counts;
DROP INDEX IF EXISTS app_users_name_lower_idx, readings_recent_partial_idx, documents_title_lower_idx;
ALTER TABLE readings DROP COLUMN IF EXISTS total_price;
ALTER TABLE app_users DROP CONSTRAINT IF EXISTS app_users_email_key;
ALTER TABLE app_users ALTER COLUMN quota_percent TYPE numeric;
DROP DOMAIN IF EXISTS e2e_percent;
`
)

// applyLegacyFixture runs fixtures/legacy.sql on the source as the app role, which owns
// every fixture object, and registers the restore of the native fixture.
func applyLegacyFixture() {
	GinkgoHelper()
	legacy, err := fixturesFS.ReadFile("fixtures/legacy.sql")
	Expect(err).NotTo(HaveOccurred(), "failed to read the legacy fixture part")
	role := "SET ROLE " + sqlIdent(appRole(sourceCluster)) + ";\n"
	DeferCleanup(func() {
		_, err := psqlIdempotentScript(sourceCluster, "native fixture restore", role+legacyRestoreSQL)
		Expect(err).NotTo(HaveOccurred(), "failed to restore the native fixture")
		Expect(fixtureObjects(sourceCluster)).To(Equal(nativeFixtureObjects), "the native fixture was not restored")
	})
	_, err = psqlIdempotentScript(sourceCluster, "legacy fixture part", role+string(legacy))
	Expect(err).NotTo(HaveOccurred(), "failed to apply the legacy fixture part")
	Expect(fixtureObjects(sourceCluster)).To(Equal(legacyFixtureObjects), "the legacy fixture part is incomplete")
}

var _ = Describe("Migration", func() {
	BeforeEach(resetMigrationPair)

	It("follows the legacy fixture through a verified cutover with every object and row", Label(nativeLabel), func() {
		if selectedEngine() != v1beta1.MigrationEngineRPGMigrate {
			Skip("the legacy engine seeds fixtures/legacy.sql, so its fresh clone and follow specs already carry it")
		}
		Expect(fixtureObjects(sourceCluster)).To(Equal(nativeFixtureObjects), "the source is not the native fixture")
		applyLegacyFixture()
		role := "SET ROLE " + sqlIdent(appRole(sourceCluster)) + ";\n"
		DeferCleanup(func() {
			_, err := psqlIdempotentScript(sourceCluster, "widening rows delete", role+nativeWideningDeleteSQL)
			Expect(err).NotTo(HaveOccurred(), "failed to delete the rows the widening spec wrote")
		})

		By("following the widened source")
		DeferCleanup(deleteMigration, nativeWideningName)
		m := newFollowMigration(nativeWideningName, v1beta1.CutoverManual)
		m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
		create(m)
		waitFollowStreaming(nativeWideningName)

		By("writing through every widened object while it streams, then cutting over")
		_, err := psqlIdempotentScript(sourceCluster, "widening rows", role+nativeWideningWriteSQL)
		Expect(err).NotTo(HaveOccurred(), "failed to write through the widened objects")
		written := psql(sourceCluster, nativeWideningRowsSQL)
		Expect(written).To(Equal(nativeWideningMarker+"@example.invalid/42.5|7/17.5000"),
			"the source did not take the widened rows")
		approveCutover(nativeWideningName)
		m = waitPhase(nativeWideningName, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectSingleAttempt(m)
		expectConditionTrue(m, v1beta1.ConditionCutoverComplete)
		expectConditionTrue(m, v1beta1.ConditionVerified)
		expectCleanupSucceeded(nativeWideningName)
		expectDrainProgress(m)

		By("comparing objects, the materialized view, rows, large objects and sequences on both sides")
		Expect(fixtureObjects(targetCluster)).To(Equal(legacyFixtureObjects), "the target lacks a widened object")
		Expect(matviewState(sourceCluster)).To(MatchRegexp(`^true/[1-9][0-9]*$`),
			"the source materialized view is unpopulated or empty")
		Expect(matviewState(targetCluster)).To(Equal(matviewState(sourceCluster)),
			"materialized view not populated identically on the target")
		Expect(psql(targetCluster, nativeWideningRowsSQL)).To(Equal(written), "the streamed rows differ on the target")
		Expect(psql(targetCluster, seedCountSQL)).To(Equal(psql(sourceCluster, seedCountSQL)))
		Expect(largeObjectCount(targetCluster)).To(Equal(largeObjectCount(sourceCluster)))
		Expect(sequenceValues(targetCluster)).To(Equal(sequenceValues(sourceCluster)))
		Expect(sourceSlotCount()).To(Equal("0"), "replication slot left behind on the source")
		Expect(targetOriginCount()).To(Equal("0"), "replication origin left behind on the target after cleanup")
	})
})

// TestLegacyRestoreUndoesTheLegacyPart ties legacy.sql to the restore the widening
// spec runs and to the object check, so an object added to one cannot be missed by the others.
func TestLegacyRestoreUndoesTheLegacyPart(t *testing.T) {
	legacy, err := fixturesFS.ReadFile("fixtures/legacy.sql")
	if err != nil {
		t.Fatal(err)
	}
	created := regexp.MustCompile(`(?:CREATE DOMAIN|ADD CONSTRAINT|ADD COLUMN IF NOT EXISTS|CREATE INDEX IF NOT EXISTS|`+
		`CREATE MATERIALIZED VIEW IF NOT EXISTS|ALTER COLUMN)\s+(\w+)`).FindAllStringSubmatch(string(legacy), -1)
	if len(created) != 8 {
		t.Fatalf("legacy.sql creates or retypes %d objects, want the 8 this test pins", len(created))
	}
	for _, m := range created {
		if !strings.Contains(legacyRestoreSQL, m[1]) {
			t.Errorf("the native fixture restore never undoes %s", m[1])
		}
		if !strings.Contains(fixtureObjectSQL, "'"+m[1]+"'") && !strings.Contains(fixtureObjectSQL, "public."+m[1]+"'") {
			t.Errorf("fixtureObjectSQL never checks %s", m[1])
		}
	}
	if !strings.HasPrefix(legacyFixtureObjects, nativeFixtureObjects+",") {
		t.Errorf("the legacy objects %q do not extend the native ones %q", legacyFixtureObjects, nativeFixtureObjects)
	}
}
