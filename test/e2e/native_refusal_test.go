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
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	nativeRefusalName = "e2e-native-refusal"
	// nativeRefusalReason is the Failed reason for the engine's exit 2 (native:BadConfig),
	// which clone returns for the unlogged table while it plans, before it changes either database.
	nativeRefusalReason = "NativeBadConfig"
	// nativeRefusalLog matches clone's relation refusal in the worker's JSON diagnostic; the list of
	// refused kinds is the engine's to change, so only the unlogged entry is pinned.
	nativeRefusalLog = `"message":"native:BadConfig: clone refuses [^"]*\bunlogged\b`
	// nativeRefusalPersistenceSQL reads the fixture table's persistence: u while applied, absent once restored.
	nativeRefusalPersistenceSQL = "SELECT coalesce((SELECT relpersistence::text FROM pg_class" +
		" WHERE oid = to_regclass('public.e2e_unlogged')), 'absent')"
	// nativeRefusalFixtureSQL is the refusal fixture: one unlogged table, which native clone refuses
	// while it plans. It lives here, not in fixtures/, because run.sh stages every fixture file.
	nativeRefusalFixtureSQL = "CREATE UNLOGGED TABLE IF NOT EXISTS public.e2e_unlogged" +
		" (id bigint PRIMARY KEY, note text NOT NULL);\n"
	nativeRefusalRestoreSQL = "DROP TABLE IF EXISTS public.e2e_unlogged;\n"
	nativeFollowRefusalName = "e2e-native-refusal-follow"
	// nativeFollowRefusalMessage is the operator's condition message when the follow preflight's
	// unlogged check fails: an exact_true check refuses with the result class.
	nativeFollowRefusalMessage = "native preflight execution refused: error class result"
	nativeFollowRefusalReason  = "NativePreflightFailed"
	nativeFinalizer            = "pgcopydb-operator.io/cleanup"
	// nativeRefusalBudget covers a worker pod waiting for a node (workerRunningTimeout)
	// plus the preflight before it; the refusal itself takes about a second.
	nativeRefusalBudget = workerRunningTimeout + 5*time.Minute
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

// applyRefusalFixture runs nativeRefusalFixtureSQL on the source as the app role, which owns
// every fixture object, and registers its removal.
func applyRefusalFixture() {
	GinkgoHelper()
	role := "SET ROLE " + sqlIdent(appRole(sourceCluster)) + ";\n"
	DeferCleanup(func() {
		_, err := psqlIdempotentScript(sourceCluster, "refusal fixture removal", role+nativeRefusalRestoreSQL)
		Expect(err).NotTo(HaveOccurred(), "failed to remove the refusal fixture part")
		Expect(psql(sourceCluster, nativeRefusalPersistenceSQL)).To(Equal("absent"), "the unlogged table is still there")
	})
	_, err := psqlIdempotentScript(sourceCluster, "refusal fixture part", role+nativeRefusalFixtureSQL)
	Expect(err).NotTo(HaveOccurred(), "failed to apply the refusal fixture part")
	Expect(psql(sourceCluster, nativeRefusalPersistenceSQL)).To(Equal("u"), "the refusal table is not unlogged")
}

var _ = Describe("Migration", func() {
	BeforeEach(resetMigrationPair)

	// The engine's clone and the operator's follow preflight each refuse the table, so the spec runs both.
	It("refuses an unlogged table before any database change and deletes cleanly", Label(nativeLabel), func() {
		if selectedEngine() != v1beta1.MigrationEngineRPGMigrate {
			Skip("only the native engine's clone refuses an unlogged table")
		}
		Expect(fixtureObjects(sourceCluster)).To(Equal(nativeFixtureObjects), "the source is not the native fixture")
		applyRefusalFixture()
		source, target := psql(sourceCluster, nativeStateSQL), psql(targetCluster, nativeStateSQL)
		Expect(source).To(MatchRegexp(nativeStateNoneOwned), "native state left on the source before the spec")
		Expect(target).To(MatchRegexp(nativeStateNoneOwned), "native state left on the target before the spec")

		By("creating a native clone Migration over the unlogged table, which the engine's clone refuses")
		DeferCleanup(deleteMigration, nativeRefusalName)
		m := newMigration(nativeRefusalName, nsE2E, v1beta1.CloneOptions{})
		create(m)

		By("waiting for the engine's refusal")
		m = waitTerminal(nativeRefusalName, nativeRefusalBudget)
		Expect(m.Status.Phase).To(Equal(v1beta1.PhaseFailed), "%s ended %s instead of refusing", m.Name, m.Status.Phase)
		failed := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionFailed)
		Expect(failed).NotTo(BeNil(), "%s failed without a Failed condition", m.Name)
		Expect(failed.Reason).To(Equal(nativeRefusalReason), "%s failed for another reason: %s", m.Name, failed.Message)
		Expect(apimeta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionComplete)).To(BeFalse())
		Expect(m.Status.Attempts).To(Equal(int32(1)), "a refusal must not be retried")
		Expect(jobLogs(m.Status.JobName, 20)).To(MatchRegexp(nativeRefusalLog),
			"%s failed with BadConfig, but not with clone's relation refusal", m.Name)

		By("checking neither database changed")
		Expect(psql(sourceCluster, nativeStateSQL)).To(Equal(source), "the refusal changed the source")
		Expect(psql(targetCluster, nativeStateSQL)).To(Equal(target), "the refusal changed the target")

		By("deleting the refused Migration, which must clear its finalizer without an inventory or cleanup Job")
		jobs := deleteWatchingOwnedJobs(m.Name, m.UID)
		Expect(slices.ContainsFunc(jobs, func(j string) bool {
			return strings.HasPrefix(j, "rpgmigrate-inventory-") || strings.HasPrefix(j, "rpgmigrate-cleanup-") ||
				j == m.Name+"-cleanup"
		})).To(BeFalse(), "deleting a refused clone ran an inventory or cleanup Job: %v", jobs)
		Expect(jobs).To(ContainElement(m.Status.JobName), "the poll never saw the refused worker Job, so it saw nothing")
		Expect(psql(sourceCluster, nativeStateSQL)).To(Equal(source), "deletion changed the source")
		Expect(psql(targetCluster, nativeStateSQL)).To(Equal(target), "deletion changed the target")

		By("creating a native follow Migration over the unlogged table, which the operator's follow preflight refuses")
		DeferCleanup(deleteMigration, nativeFollowRefusalName)
		f := newFollowMigration(nativeFollowRefusalName, v1beta1.CutoverManual)
		startNativeJobRecorder()
		// No work volume: the operator claims one only after its preflight passes.
		Expect(k8sClient.Create(ctx, f)).To(Succeed(), "failed to create Migration %s", f.Name)
		f = waitTerminal(nativeFollowRefusalName, nativeRefusalBudget)
		Expect(f.Status.Phase).To(Equal(v1beta1.PhaseFailed), "%s ended %s instead of refusing", f.Name, f.Status.Phase)
		for _, condition := range []string{v1beta1.ConditionFailed, v1beta1.ConditionValidated} {
			c := apimeta.FindStatusCondition(f.Status.Conditions, condition)
			Expect(c).NotTo(BeNil(), "%s has no %s condition", f.Name, condition)
			Expect([]any{c.Status == metav1.ConditionTrue, c.Reason, c.Message}).To(Equal([]any{
				condition == v1beta1.ConditionFailed, nativeFollowRefusalReason, nativeFollowRefusalMessage}),
				"%s is not refused by the follow preflight", f.Name)
		}
		Expect([]any{f.Status.Attempts, f.Status.JobName, f.Status.NativeJobUID, f.Status.NativePVCUID}).
			To(Equal([]any{int32(0), "", types.UID(""), types.UID("")}),
				"%s started a worker after its preflight refused", f.Name)
		Expect(f.Finalizers).To(ContainElement(nativeFinalizer),
			"%s holds no finalizer, so its release proves nothing", f.Name)
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: f.Name + "-work"},
			&corev1.PersistentVolumeClaim{}))).To(BeTrue(), "%s claimed a work volume after its preflight refused", f.Name)
		Expect(psql(sourceCluster, nativeStateSQL)).To(Equal(source), "the follow refusal changed the source")
		Expect(psql(targetCluster, nativeStateSQL)).To(Equal(target), "the follow refusal changed the target")

		By("deleting the refused follow Migration, which must release its finalizer without an inventory or cleanup Job")
		// deleteMigration returns once the Migration is gone, so the finalizer was released.
		jobs = append(deleteWatchingOwnedJobs(f.Name, f.UID), nativeOwnedJobs(f.UID)...)
		Expect(slices.ContainsFunc(jobs, func(j string) bool { return strings.HasPrefix(j, "rpgmigrate-preflight-") })).
			To(BeTrue(), "the Job watch saw no preflight Job of %s, so it saw nothing: %v", f.Name, jobs)
		Expect(slices.ContainsFunc(jobs, func(j string) bool {
			return strings.HasPrefix(j, "rpgmigrate-inventory-") || strings.HasPrefix(j, "rpgmigrate-cleanup-") ||
				j == f.Name+"-cleanup" || strings.HasPrefix(j, f.Name+"-run-")
		})).To(BeFalse(), "deleting a follow refused at preflight ran a worker, inventory or cleanup Job: %v", jobs)
		Expect(psql(sourceCluster, nativeStateSQL)).To(Equal(source), "the follow deletion changed the source")
		Expect(psql(targetCluster, nativeStateSQL)).To(Equal(target), "the follow deletion changed the target")
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

// TestNativeRefusalFixture ties the refusal fixture to the spec's removal and persistence
// check, so the table it creates is the one the spec reads and drops.
func TestNativeRefusalFixture(t *testing.T) {
	create := regexp.MustCompile(`(?i)CREATE\s+(\w+\s+)?TABLE\s+IF\s+NOT\s+EXISTS\s+([\w.]+)`)
	tables := create.FindAllStringSubmatch(nativeRefusalFixtureSQL, -1)
	if len(tables) != 1 || !strings.EqualFold(strings.TrimSpace(tables[0][1]), "UNLOGGED") {
		t.Fatalf("the refusal fixture creates %q, want exactly one unlogged table", tables)
	}
	qualified := tables[0][2]
	if !strings.Contains(nativeRefusalRestoreSQL, "DROP TABLE IF EXISTS "+qualified+";") ||
		!strings.Contains(nativeRefusalPersistenceSQL, "to_regclass('"+qualified+"')") {
		t.Errorf("the refusal spec never removes or reads %s", qualified)
	}
}

func TestNativeRefusalLog(t *testing.T) {
	refusal := regexp.MustCompile(nativeRefusalLog)
	// Built from the engine's diagnostic format at 55f748a (src/lib.rs:202-215, src/catalog/clone.rs:770).
	line := `{"category":"BadConfig","error_origin":{"column":28,"file":"clone.rs","line":769},"error_severity":"ERROR",` +
		`"exit_code":2,"level":"error","message":"native:BadConfig: clone refuses foreign, materialized, unlogged,` +
		` extension-owned, custom-access-method, or ALWAYS/REPLICA-trigger relations"}`
	if !refusal.MatchString(line) || !refusal.MatchString(strings.Replace(line, " materialized,", "", 1)) {
		t.Errorf("%s misses clone's unlogged refusal", nativeRefusalLog)
	}
	for _, other := range []string{
		strings.Replace(line, "unlogged,", "", 1),
		strings.Replace(line, "BadConfig: clone", "BadState: clone", 1),
		`{"message":"native:BadConfig: follow refuses unlogged tables"}`,
	} {
		if refusal.MatchString(other) {
			t.Errorf("%s matches another failure: %s", nativeRefusalLog, other)
		}
	}
}

// TestNativeFollowRefusalTies ties the follow refusal's expected condition and finalizer to the operator
// source, and its unlogged table to the follow-only preflight check that refuses with the result class.
func TestNativeFollowRefusalTies(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		src, err := os.ReadFile("../../" + path)
		if err != nil {
			t.Fatal(err)
		}
		return string(src)
	}
	preflight := read("internal/controller/native_preflight.go")
	reconcile := read("internal/controller/native_reconcile.go")
	refused, class, ok := strings.Cut(nativeFollowRefusalMessage, ": error class ")
	if !ok || !strings.Contains(preflight, `errors.New("`+refused+`")`) ||
		!strings.Contains(preflight, `fmt.Errorf("%w: error class %s", errNativePreflightTerminal, class)`) {
		t.Errorf("internal/controller/native_preflight.go builds no message %q", nativeFollowRefusalMessage)
	}
	if !strings.Contains(reconcile, `reasonNativePreflightFailed = "`+nativeFollowRefusalReason+`"`) {
		t.Errorf("internal/controller/native_reconcile.go has no reason %q", nativeFollowRefusalReason)
	}
	if !strings.Contains(read("internal/controller/follow.go"), `finalizerName = "`+nativeFinalizer+`"`) {
		t.Errorf("internal/controller/follow.go has no finalizer %q", nativeFinalizer)
	}
	program := read("internal/native/evidence_program.py")
	check := strings.Index(program, `c.relpersistence='u');")); checks.append('unlogged')`)
	follow, end := strings.Index(program, "\n if follow:\n"), strings.Index(program, "\n return dump,restore,checks\n")
	line := program[strings.LastIndex(program[:max(check, 0)], "\n")+1 : max(check, 0)]
	if check < 0 || follow < 0 || check < follow || end < check ||
		!strings.HasPrefix(line, "  exact_true(source.query(") ||
		!strings.Contains(program, "RESULT='"+class+"'\n") ||
		!strings.Contains(program, "def exact_true(rows):\n need(rows==[[b't']],RESULT)\n") {
		t.Errorf("evidence_program.py has no follow-only unlogged check that refuses with class %q", class)
	}
	if !strings.Contains(nativeRefusalFixtureSQL, "CREATE UNLOGGED TABLE") {
		t.Error("the refusal fixture creates no unlogged table for the follow preflight")
	}
}
