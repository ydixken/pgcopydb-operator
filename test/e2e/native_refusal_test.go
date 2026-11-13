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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	nativeRefusalName = "e2e-native-refusal"
	// nativeRefusalReason is the Failed reason for the engine's exit 2 (native:BadConfig),
	// which clone returns for the materialized view before it touches either database.
	nativeRefusalReason = "NativeBadConfig"
	// nativeRefusalBudget covers a worker pod waiting for a node (workerRunningTimeout)
	// plus the preflight before it; the refusal itself takes about a second.
	nativeRefusalBudget = workerRunningTimeout + 5*time.Minute
	// nativeRefusalRestoreSQL undoes fixtures/legacy.sql, so the source is the native fixture again.
	nativeRefusalRestoreSQL = `DROP MATERIALIZED VIEW IF EXISTS event_daily_counts;
DROP INDEX IF EXISTS app_users_name_lower_idx, readings_recent_partial_idx, documents_title_lower_idx;
ALTER TABLE readings DROP COLUMN IF EXISTS total_price;
ALTER TABLE app_users DROP CONSTRAINT IF EXISTS app_users_email_key;
ALTER TABLE app_users ALTER COLUMN quota_percent TYPE numeric;
DROP DOMAIN IF EXISTS e2e_percent;
`
	// nativeStateSQL counts every slot, publication, origin and native schema, so a refusal
	// that changed anything on a side reads differently afterwards.
	nativeStateSQL = "SELECT 'slots=' || (SELECT count(*) FROM pg_replication_slots)" +
		" || ' rpgm_slots=' || (SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name, 'rpgm_'))" +
		" || ' pubs=' || (SELECT count(*) FROM pg_publication)" +
		" || ' rpgm_pubs=' || (SELECT count(*) FROM pg_publication WHERE starts_with(pubname, 'rpgm_'))" +
		" || ' rpgm_schemas=' || (SELECT count(*) FROM pg_namespace WHERE starts_with(nspname, '_rpgmigrate_'))" +
		" || ' origins=' || (SELECT count(*) FROM pg_replication_origin)" +
		" || ' tables=' || (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace" +
		" WHERE c.relkind IN ('r', 'p', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')" +
		" AND NOT starts_with(n.nspname, 'pg_toast'))"
	// nativeStateNoneOwned matches a nativeStateSQL row with no native slot, publication or schema.
	nativeStateNoneOwned = ` rpgm_slots=0 pubs=[0-9]+ rpgm_pubs=0 rpgm_schemas=0 `
)

// applyLegacyFixture runs fixtures/legacy.sql on the source as the app role, which owns
// every fixture object, and registers the restore of the native fixture.
func applyLegacyFixture() {
	GinkgoHelper()
	legacy, err := fixturesFS.ReadFile("fixtures/legacy.sql")
	Expect(err).NotTo(HaveOccurred(), "failed to read the legacy fixture part")
	role := "SET ROLE " + sqlIdent(appRole(sourceCluster)) + ";\n"
	DeferCleanup(func() {
		_, err := psqlIdempotentScript(sourceCluster, "native fixture restore", role+nativeRefusalRestoreSQL)
		Expect(err).NotTo(HaveOccurred(), "failed to restore the native fixture")
		Expect(fixtureObjects(sourceCluster)).To(Equal(nativeFixtureObjects), "the native fixture was not restored")
	})
	_, err = psqlIdempotentScript(sourceCluster, "legacy fixture part", role+string(legacy))
	Expect(err).NotTo(HaveOccurred(), "failed to apply the legacy fixture part")
	Expect(fixtureObjects(sourceCluster)).To(Equal(legacyFixtureObjects), "the legacy fixture part is incomplete")
}

var _ = Describe("Migration", func() {
	BeforeEach(resetMigrationPair)

	It("refuses the legacy-only fixture before any database change and deletes cleanly", Label(nativeLabel), func() {
		if selectedEngine() != v1beta1.MigrationEngineRPGMigrate {
			Skip("the legacy engine migrates fixtures/legacy.sql, so only the native engine refuses it")
		}
		Expect(fixtureObjects(sourceCluster)).To(Equal(nativeFixtureObjects), "the source is not the native fixture")
		applyLegacyFixture()
		source, target := psql(sourceCluster, nativeStateSQL), psql(targetCluster, nativeStateSQL)
		Expect(source).To(MatchRegexp(nativeStateNoneOwned), "native state left on the source before the spec")
		Expect(target).To(MatchRegexp(nativeStateNoneOwned), "native state left on the target before the spec")

		By("creating a native follow Migration over the legacy-only objects")
		DeferCleanup(deleteMigration, nativeRefusalName)
		m := newFollowMigration(nativeRefusalName, v1beta1.CutoverManual)
		create(m)

		By("waiting for the engine's refusal")
		m = waitTerminal(nativeRefusalName, nativeRefusalBudget)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed), "%s ended %s instead of refusing", m.Name, m.Status.Phase)
		failed := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed).NotTo(BeNil(), "%s failed without a Failed condition", m.Name)
		Expect(failed.Reason).To(Equal(nativeRefusalReason), "%s failed for another reason: %s", m.Name, failed.Message)
		Expect(apimeta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeFalse())
		Expect(m.Status.Attempts).To(Equal(int32(1)), "a refusal must not be retried")

		By("checking neither database changed")
		Expect(psql(sourceCluster, nativeStateSQL)).To(Equal(source), "the refusal changed the source")
		Expect(psql(targetCluster, nativeStateSQL)).To(Equal(target), "the refusal changed the target")

		By("deleting the refused Migration, which must clear its finalizer without a cleanup Job")
		jobs := deleteWatchingOwnedJobs(m.Name, m.UID)
		Expect(slices.ContainsFunc(jobs, func(j string) bool { return strings.HasPrefix(j, "rpgmigrate-inventory-") })).
			To(BeTrue(), "deletion read no owned inventory, so the empty disposition was not exercised: %v", jobs)
		Expect(slices.ContainsFunc(jobs, func(j string) bool {
			return strings.HasPrefix(j, "rpgmigrate-cleanup-") || j == m.Name+"-cleanup"
		})).To(BeFalse(), "deletion ran a cleanup Job although the engine changed nothing: %v", jobs)
		Expect(psql(sourceCluster, nativeStateSQL)).To(Equal(source), "deletion changed the source")
		Expect(psql(targetCluster, nativeStateSQL)).To(Equal(target), "deletion changed the target")
	})
})

// deleteWatchingOwnedJobs deletes the Migration and returns every Job it controlled
// that a 250ms poll saw while it terminated.
func deleteWatchingOwnedJobs(name string, uid types.UID) []string {
	GinkgoHelper()
	var mu sync.Mutex
	seen := map[string]bool{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			jobs := &batchv1.JobList{}
			if k8sClient.List(ctx, jobs, client.InNamespace(nsE2E)) == nil {
				mu.Lock()
				for i := range jobs.Items {
					if owner := metav1.GetControllerOf(&jobs.Items[i]); owner != nil && owner.UID == uid {
						seen[jobs.Items[i].Name] = true
					}
				}
				mu.Unlock()
			}
			select {
			case <-stop:
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	})
	deleteMigration(name)
	close(stop)
	wg.Wait()
	names := make([]string, 0, len(seen))
	for job := range seen {
		names = append(names, job)
	}
	slices.Sort(names)
	return names
}

// TestNativeRefusalRestoreUndoesTheLegacyPart ties legacy.sql to the restore the
// spec runs and to the object check, so an object added to one cannot be missed by the others.
func TestNativeRefusalRestoreUndoesTheLegacyPart(t *testing.T) {
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
		if !strings.Contains(nativeRefusalRestoreSQL, m[1]) {
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
