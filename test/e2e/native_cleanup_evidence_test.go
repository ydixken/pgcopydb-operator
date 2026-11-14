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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

const (
	// nativeSlotCensus admits every slot the native engine names (rpgm_s_<run>_<generation>).
	nativeSlotCensus = "starts_with(slot_name, 'rpgm_s_')"
	// nativeOriginCensus admits every origin: the native engine creates none, so any one is left over.
	nativeOriginCensus = "true"
)

// nativeCleanupJob names the Jobs that run native stream cleanup; the rejoin helper does not match.
var nativeCleanupJob = regexp.MustCompile(`^rpgmigrate-cleanup-[0-9a-f]{32}$`)

// suiteRESTConfig is the suite's API server config, kept for the native Job watch.
var suiteRESTConfig *rest.Config

func nativePublicationPrefix(run string) string { return "rpgm_p_" + run + "_" }

// nativeSchemaName is the target metadata schema of run, which native cleanup retains.
func nativeSchemaName(run string) string { return "_rpgmigrate_" + run }

// censusSQL counts the rows of catalog that filter admits, or yields NULL when filter rejects
// sample, a name the engine under test creates: a filter blind to that engine cannot read zero.
func censusSQL(catalog, column, filter, sample string) string {
	return "SELECT CASE WHEN EXISTS (SELECT FROM (SELECT " + sqlLiteral(sample) + "::name AS " + column +
		", current_database() AS database) probe WHERE " + filter + ") THEN (SELECT count(*) FROM " + catalog +
		" WHERE " + filter + ")::text END"
}

// census runs censusSQL on cluster and fails when the filter is blind to the selected engine.
func census(cluster, catalog, column, filter, sample string) string {
	GinkgoHelper()
	out := psql(cluster, censusSQL(catalog, column, filter, sample))
	Expect(out).NotTo(BeEmpty(), "the %s census %q admits no %s name such as %q, so its zero would prove nothing",
		catalog, filter, selectedEngine(), sample)
	return out
}

// slotCensus is the filter and probe name of the source slot census for the selected engine.
func slotCensus() (filter, sample string) {
	if selectedEngine() == v1beta1.MigrationEngineRPGMigrate {
		return nativeSlotCensus, nativeSlotPrefix(nativeFaultDecoyRun) + "1"
	}
	return slotFilter(), pgcopydb.SlotName(nsE2E, "e2e-census")
}

// originCensus is the filter and probe name of the target origin census for the selected engine.
func originCensus() (filter, sample string) {
	if selectedEngine() == v1beta1.MigrationEngineRPGMigrate {
		return nativeOriginCensus, "rpgm_census"
	}
	return originFilter(), pgcopydb.SlotName(nsE2E, "e2e-census")
}

// nativeRunCensusSQL reads slots/publications/metadata schemas named for run, in that order.
func nativeRunCensusSQL(run string) string {
	return "SELECT (SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name, " +
		sqlLiteral(nativeSlotPrefix(run)) + ")) || '/' || (SELECT count(*) FROM pg_publication WHERE starts_with(pubname, " +
		sqlLiteral(nativePublicationPrefix(run)) + ")) || '/' || (SELECT count(*) FROM pg_namespace WHERE nspname = " +
		sqlLiteral(nativeSchemaName(run)) + ")"
}

// nativeRunCensus is nativeRunCensusSQL for mig's run on cluster.
func nativeRunCensus(cluster string, mig *v1beta1.Migration) string {
	GinkgoHelper()
	run, err := nativeFaultRunID(mig.UID)
	Expect(err).NotTo(HaveOccurred())
	return psql(cluster, nativeRunCensusSQL(run))
}

// nativeJobRecord is what the watch saw of one Migration-controlled Job. The operator deletes
// a native cleanup Job before the Migration completes, so only a watch keeps its success.
type nativeJobRecord struct {
	name      string
	owner     types.UID
	succeeded bool
}

var nativeJobs struct {
	once sync.Once
	mu   sync.Mutex
	seen map[types.UID]nativeJobRecord
}

// jobSucceeded is the Job state the operator requires before it reads a helper's report.
func jobSucceeded(job *batchv1.Job) bool {
	complete := false
	for _, c := range job.Status.Conditions {
		if c.Status == corev1.ConditionTrue && (c.Type == batchv1.JobFailed || c.Type == batchv1.JobFailureTarget) {
			return false
		}
		complete = complete || c.Status == corev1.ConditionTrue && c.Type == batchv1.JobComplete
	}
	return complete && job.Status.Succeeded >= 1 && job.Status.Failed == 0
}

// recordNativeJob keeps success sticky: the deletion event of a succeeded Job cannot erase it.
func recordNativeJob(job *batchv1.Job) {
	owner := metav1.GetControllerOf(job)
	if owner == nil || job.UID == "" {
		return
	}
	nativeJobs.mu.Lock()
	defer nativeJobs.mu.Unlock()
	if nativeJobs.seen == nil {
		nativeJobs.seen = map[types.UID]nativeJobRecord{}
	}
	succeeded := nativeJobs.seen[job.UID].succeeded || jobSucceeded(job)
	nativeJobs.seen[job.UID] = nativeJobRecord{name: job.Name, owner: owner.UID, succeeded: succeeded}
}

// nativeCleanupJobs returns the cleanup Jobs owner controlled that the watch saw, and those it saw succeed.
func nativeCleanupJobs(owner types.UID) (seen, succeeded []string) {
	nativeJobs.mu.Lock()
	defer nativeJobs.mu.Unlock()
	for _, r := range nativeJobs.seen {
		if r.owner != owner || !nativeCleanupJob.MatchString(r.name) {
			continue
		}
		seen = append(seen, r.name)
		if r.succeeded {
			succeeded = append(succeeded, r.name)
		}
	}
	slices.Sort(seen)
	slices.Sort(succeeded)
	return seen, succeeded
}

// startNativeJobRecorder watches the suite namespace's Jobs for the rest of the run. A watch
// restart begins from current state, so a Job born and deleted in the gap is lost, never invented.
func startNativeJobRecorder() {
	GinkgoHelper()
	nativeJobs.once.Do(func() {
		Expect(suiteRESTConfig).NotTo(BeNil(), "the native Job watch has no API server config")
		watcher, err := client.NewWithWatch(suiteRESTConfig, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred(), "no watch client for native helper Jobs")
		w, err := watcher.Watch(ctx, &batchv1.JobList{}, client.InNamespace(nsE2E))
		Expect(err).NotTo(HaveOccurred(), "failed to watch Jobs in %s", nsE2E)
		go func() {
			for {
				for event := range w.ResultChan() {
					if job, ok := event.Object.(*batchv1.Job); ok {
						recordNativeJob(job)
					}
				}
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
					}
					if w, err = watcher.Watch(ctx, &batchv1.JobList{}, client.InNamespace(nsE2E)); err == nil {
						break
					}
				}
			}
		}()
	})
}

// nativeCompletionError is why m's status records no cleaned native completion, or nil.
// The operator records the finalized worker only after the cleaned completion evidence.
func nativeCompletionError(m *v1beta1.Migration) error {
	complete := apimeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionComplete)
	switch {
	case m.Status.Phase != v1beta1.PhaseCompleted:
		return fmt.Errorf("%s is %s, not complete", m.Name, m.Status.Phase)
	case complete == nil || complete.Status != metav1.ConditionTrue || complete.Reason != "MigrationSucceeded":
		return fmt.Errorf("%s has no Complete=True condition with reason MigrationSucceeded", m.Name)
	case m.Status.NativeJobUID == "" || m.Status.NativeFinalizedJobUID != m.Status.NativeJobUID:
		return fmt.Errorf("%s records no native finalization of its worker", m.Name)
	}
	return nil
}

// expectNativeCleanupSucceeded requires positive native cleanup evidence: an owned cleanup Job
// seen to succeed, the cleaned completion in status, and no run resource left but the target
// metadata schema that cleanup retains.
func expectNativeCleanupSucceeded(m *v1beta1.Migration) {
	GinkgoHelper()
	Expect(nativeCompletionError(m)).To(Succeed())
	seen, succeeded := nativeCleanupJobs(m.UID)
	Expect(succeeded).NotTo(BeEmpty(),
		"no rpgmigrate-cleanup-<operation> Job of %s was seen to succeed; seen: %v", m.Name, seen)
	Expect(nativeRunCensus(sourceCluster, m)).To(Equal("0/0/0"),
		"source slots/publications/schemas of %s after cleanup", m.Name)
	// Cleanup retains the target metadata schema; finding it proves the census reads this run.
	Expect(nativeRunCensus(targetCluster, m)).To(Equal("0/0/1"),
		"target slots/publications/schemas of %s after cleanup", m.Name)
}

func TestNativeCompletionErrorNeedsTheCleanedCompletion(t *testing.T) {
	done := func() *v1beta1.Migration {
		m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: "m"}}
		m.Status.Phase = v1beta1.PhaseCompleted
		m.Status.Conditions = []metav1.Condition{
			{Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "MigrationSucceeded"}}
		m.Status.NativeJobUID, m.Status.NativeFinalizedJobUID = "worker-uid", "worker-uid"
		return m
	}
	if err := nativeCompletionError(done()); err != nil {
		t.Fatalf("a cleaned native completion reads %v", err)
	}
	for name, mutate := range map[string]func(*v1beta1.Migration){
		"cutting over":     func(m *v1beta1.Migration) { m.Status.Phase = v1beta1.PhaseCuttingOver },
		"no Complete":      func(m *v1beta1.Migration) { m.Status.Conditions = nil },
		"Complete False":   func(m *v1beta1.Migration) { m.Status.Conditions[0].Status = metav1.ConditionFalse },
		"another reason":   func(m *v1beta1.Migration) { m.Status.Conditions[0].Reason = "Rehearsed" },
		"not finalized":    func(m *v1beta1.Migration) { m.Status.NativeFinalizedJobUID = "" },
		"other worker":     func(m *v1beta1.Migration) { m.Status.NativeFinalizedJobUID = "old-worker" },
		"no native worker": func(m *v1beta1.Migration) { m.Status.NativeJobUID, m.Status.NativeFinalizedJobUID = "", "" },
	} {
		m := done()
		mutate(m)
		if nativeCompletionError(m) == nil {
			t.Errorf("%s reads as a cleaned native completion", name)
		}
	}
}

func TestCensusFollowsTheSelectedEngine(t *testing.T) {
	t.Setenv("E2E_ENGINE", string(v1beta1.MigrationEngineRPGMigrate))
	if slots, sample := slotCensus(); slots != nativeSlotCensus || !strings.HasPrefix(sample, "rpgm_s_") {
		t.Errorf("native slot census = %q probed with %q", slots, sample)
	}
	if origins, _ := originCensus(); origins != nativeOriginCensus {
		t.Errorf("native origin census = %q", origins)
	}
	t.Setenv("E2E_ENGINE", string(v1beta1.MigrationEnginePGCopyDB))
	if slots, sample := slotCensus(); slots != slotFilter() || !strings.HasPrefix(sample, "pgcopydb_") {
		t.Errorf("pgcopydb slot census = %q probed with %q", slots, sample)
	}
	if origins, sample := originCensus(); origins != originFilter() || !strings.HasPrefix(sample, "pgcopydb_") {
		t.Errorf("pgcopydb origin census = %q probed with %q", origins, sample)
	}
}

func TestNativeJobRecordKeepsSuccessOfOwnedCleanupJobs(t *testing.T) {
	nativeJobs.mu.Lock()
	saved := nativeJobs.seen
	nativeJobs.seen = nil
	nativeJobs.mu.Unlock()
	t.Cleanup(func() { nativeJobs.mu.Lock(); nativeJobs.seen = saved; nativeJobs.mu.Unlock() })

	owner, other := types.UID("mig-uid"), types.UID("other-uid")
	op := strings.Repeat("0a", 16)
	job := func(name string, uid, controller types.UID, succeeded bool, conds ...batchv1.JobConditionType) *batchv1.Job {
		j := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid}}
		if controller != "" {
			yes := true
			j.OwnerReferences = []metav1.OwnerReference{{Name: "m", UID: controller, Controller: &yes}}
		}
		if succeeded {
			j.Status.Succeeded = 1
		}
		for _, c := range conds {
			j.Status.Conditions = append(j.Status.Conditions, batchv1.JobCondition{Type: c, Status: corev1.ConditionTrue})
		}
		return j
	}
	recordNativeJob(job("rpgmigrate-cleanup-"+op, "j1", owner, false))
	if seen, succeeded := nativeCleanupJobs(owner); len(seen) != 1 || len(succeeded) != 0 {
		t.Fatalf("a running cleanup Job reads seen=%v succeeded=%v", seen, succeeded)
	}
	recordNativeJob(job("rpgmigrate-cleanup-"+op, "j1", owner, true, batchv1.JobComplete))
	recordNativeJob(job("rpgmigrate-cleanup-"+op, "j1", owner, false)) // a later event cannot erase success
	for _, j := range []*batchv1.Job{
		job("rpgmigrate-cleanup-rejoin-"+op, "j2", owner, true, batchv1.JobComplete),
		job("m-cleanup", "j3", owner, true, batchv1.JobComplete),
		job("rpgmigrate-cleanup-"+strings.Repeat("0b", 16), "j4", other, true, batchv1.JobComplete),
		job("rpgmigrate-cleanup-"+strings.Repeat("0c", 16), "j5", "", true, batchv1.JobComplete),
		job("rpgmigrate-cleanup-"+strings.Repeat("0d", 16), "j6", owner, true, batchv1.JobComplete, batchv1.JobFailureTarget),
		job("rpgmigrate-cleanup-"+strings.Repeat("0e", 16), "j7", owner, true),
	} {
		recordNativeJob(j)
	}
	seen, succeeded := nativeCleanupJobs(owner)
	if want := []string{"rpgmigrate-cleanup-" + op}; !slices.Equal(succeeded, want) {
		t.Errorf("succeeded = %v, want %v", succeeded, want)
	}
	if len(seen) != 3 {
		t.Errorf("seen = %v, want the owned cleanup Jobs only", seen)
	}
}

func TestReplicationCensusSQLSeesTheEngineResources(t *testing.T) {
	run := testPSQL(t)
	slot := nativeSlotPrefix(nativeFaultDecoyRun) + "1"
	legacy := pgcopydb.SlotName(nsE2E, "e2e-census")
	nativeSample := nativeSlotPrefix(nativeFaultDecoyRun) + "1"
	slots := func(filter, sample string) string {
		return censusSQL("pg_replication_slots", "slot_name", filter, sample)
	}
	// A temporary slot lives for the session, so each check creates its own.
	withSlot := func(name, query string) string {
		return testQuery(t, run,
			"SELECT FROM pg_create_physical_replication_slot("+sqlLiteral(name)+", false, true);\n"+query)
	}
	if got := withSlot(slot, slots(nativeSlotCensus, nativeSample)); got != "1" {
		t.Errorf("the native slot census reads %q with one native slot, want 1", got)
	}
	if got := withSlot(legacy, slots(nativeSlotCensus, nativeSample)); got != "0" {
		t.Errorf("the native slot census reads %q with only a pgcopydb slot, want 0", got)
	}
	if got := withSlot(slot, slots("slot_name LIKE 'pgcopydb%'", nativeSample)); got != "" {
		t.Errorf("the pgcopydb filter reads %q under native, want NULL: it is blind to native slots", got)
	}
	if got := withSlot(legacy, slots("slot_name LIKE 'pgcopydb%'", legacy)); got != "1" {
		t.Errorf("the pgcopydb slot census reads %q with one pgcopydb slot, want 1", got)
	}
	external := "database = current_database() AND " + suiteNameMatch("slot_name")
	if got := testQuery(t, run, slots(external, legacy)); got != "0" {
		t.Errorf("the external slot census reads %q with its probe, want 0", got)
	}
	origins := censusSQL("pg_replication_origin", "roname", nativeOriginCensus, "rpgm_census")
	withOrigin := "BEGIN;\nSELECT FROM pg_replication_origin_create('e2e_census_origin');\n" + origins + ";\nROLLBACK"
	if got := testQuery(t, run, withOrigin); got != "1" {
		t.Errorf("the native origin census reads %q with one origin, want 1", got)
	}

	census, other := nativeRunCensusSQL(nativeFaultDecoyRun), strings.Repeat("e", 32)
	if got := testQuery(t, run, census); got != "0/0/0" {
		t.Fatalf("the run census reads %q on a clean server, want 0/0/0", got)
	}
	got := testQuery(t, run, "BEGIN;\nCREATE SCHEMA "+sqlIdent(nativeSchemaName(nativeFaultDecoyRun))+";\n"+
		"CREATE PUBLICATION "+sqlIdent(nativePublicationPrefix(nativeFaultDecoyRun)+"1")+";\n"+
		"CREATE SCHEMA "+sqlIdent(nativeSchemaName(other))+";\n"+
		"CREATE PUBLICATION "+sqlIdent(nativePublicationPrefix(other)+"1")+";\n"+
		"SELECT FROM pg_create_physical_replication_slot("+sqlLiteral(nativeSlotPrefix(other)+"1")+", false, true);\n"+
		"SELECT FROM pg_create_physical_replication_slot("+sqlLiteral(slot)+", false, true);\n"+census+";\nROLLBACK")
	if got != "1/1/1" {
		t.Errorf("the run census reads %q with one of each, want 1/1/1 and no other run counted", got)
	}
}
