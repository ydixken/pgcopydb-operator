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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	nativeFaultLabel = "native-fault"
	// nativeFaultMarkerPrefix keeps writer rows inside resetSourceReplication's live- sweep.
	nativeFaultMarkerPrefix = "live-nfault-"
	nativeFaultLockApp      = "e2e-nfault-lock"
	nativeFaultForeignName  = "e2e_nfault_foreign"
	// nativeFaultDecoyRun has a non-4 version nibble, so no Migration UID derives it.
	nativeFaultDecoyRun      = "ffffffffffffffffffffffffffffffff"
	nativeFaultKillContainer = "kill"
	nativeFaultSIGKILL       = 137
	nativeFaultSessionSQL    = "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'rpgmigrate[%'"
	nativeFaultCopySQL       = "SELECT count(*) FROM pg_stat_progress_copy p JOIN pg_stat_activity a ON a.pid = p.pid" +
		" WHERE a.application_name LIKE 'rpgmigrate[%] copy' AND p.command = 'COPY FROM'"
	nativeFaultBlockedApplySQL = "SELECT count(*) FROM pg_stat_activity a" +
		" WHERE a.application_name LIKE 'rpgmigrate[%] apply' AND pg_blocking_pids(a.pid) && ARRAY(" +
		"SELECT pid FROM pg_stat_activity WHERE application_name = '" + nativeFaultLockApp + "')"
	// nativeFaultKillScript runs in the node's PID namespace, where the container's init
	// takes SIGKILL; from inside the container it would be ignored.
	nativeFaultKillScript = `until [ -e /tmp/fire ]; do sleep 0.2; done
n=0
for d in /proc/[0-9]*; do
  case "$(cat "$d/cgroup" 2>/dev/null)" in *"$1"*) kill -KILL "${d#/proc/}" 2>/dev/null && n=$((n+1));; esac
done
echo "killed $n"
[ "$n" -gt 0 ]`
)

var (
	nativeFaultContainerID  = regexp.MustCompile(`^containerd://([0-9a-f]{64})$`)
	nativeFaultForeignSlots = []string{nativeFaultForeignName, "rpgm_s_" + nativeFaultDecoyRun + "_1"}
	nativeFaultForeignPubs  = []string{nativeFaultForeignName, "rpgm_p_" + nativeFaultDecoyRun + "_1"}
)

// requireNativeFaultRun skips outside a private native kind run: the faults kill
// database pods and need a native runner the shared suite does not have.
func requireNativeFaultRun() {
	if activeKindSuite == nil || activeKindSuite.input.Engine != string(v1beta1.MigrationEngineRPGMigrate) {
		Skip("requires a private native kind run")
	}
	requireCNPGFixtures()
}

// nativeFaultMarkerSet names writer rows <marker>-1 to <marker>-<last>, all committed on the source.
type nativeFaultMarkerSet struct {
	marker string
	last   int
}

func (s nativeFaultMarkerSet) sql() string {
	return fmt.Sprintf("SELECT count(*), count(DISTINCT note), coalesce(max(substring(note FROM %d)::int), 0),"+
		" coalesce((SELECT min(g) FROM generate_series(1, %d) g WHERE NOT EXISTS"+
		" (SELECT 1 FROM orders WHERE note = '%s-' || g)), 0) FROM orders WHERE note LIKE '%s-%%'",
		len(s.marker)+2, s.last, s.marker, s.marker)
}

func readNativeFaultMarkers(cluster string, set nativeFaultMarkerSet) nativeFaultMarkers {
	GinkgoHelper()
	markers, err := parseNativeFaultMarkers(psql(cluster, set.sql()))
	Expect(err).NotTo(HaveOccurred())
	return markers
}

// nativeFaultMigration is a Manual follow Migration inside the qualified native profile.
func nativeFaultMigration(name, target string) *v1beta1.Migration {
	m := newFollowMigration(name, v1beta1.CutoverManual)
	m.Spec.Engine = v1beta1.MigrationEngineRPGMigrate
	m.Spec.Target = e2eConn(target)
	// The profile admits no dropIfExists, filters, binary COPY, split parts or large objects.
	m.Spec.Clone = v1beta1.CloneOptions{
		UseCopyBinary: ptr.To(false), SplitTablesLargerThan: resource.NewQuantity(0, resource.DecimalSI),
		Skip: []v1beta1.SkipOption{"largeObjects"},
	}
	return m
}

// nativeFaultCase is one fault's Migration, its live writer and the foreign resources it must not touch.
type nativeFaultCase struct {
	name, marker, target, run string
	pvc                       types.UID
	writer                    *liveWriter
	foreign                   *nativeFaultForeign
}

// startNativeFault creates the Migration with a writer running from before the base copy.
// Cleanup stops the writer, deletes the Migration, then drops the bookkeeping cleanup retains.
func startNativeFault(name, target string, configure func(*v1beta1.Migration)) *nativeFaultCase {
	GinkgoHelper()
	c := &nativeFaultCase{name: name, marker: nativeFaultMarkerPrefix + name, target: target}
	DeferCleanup(func() { psql(sourceCluster, "DELETE FROM orders WHERE note LIKE "+sqlLiteral(c.marker+"%")) })
	c.foreign = plantNativeFaultForeign(name, target)
	m := nativeFaultMigration(name, target)
	if configure != nil {
		configure(m)
	}
	DeferCleanup(func() {
		deleteMigration(name)
		if c.run != "" {
			psql(target, "DROP SCHEMA IF EXISTS "+sqlIdent("_rpgmigrate_"+c.run)+" CASCADE")
		}
	})
	c.writer = startLiveWriter(c.marker)
	DeferCleanup(func() { _, _ = c.writer.stop() })
	create(m)
	var err error
	c.run, err = nativeFaultRunID(m.UID)
	Expect(err).NotTo(HaveOccurred())
	pvc := &corev1.PersistentVolumeClaim{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name + "-work"}, pvc)).To(Succeed())
	c.pvc = pvc.UID
	return c
}

func (c *nativeFaultCase) ownedSlots() int {
	GinkgoHelper()
	return psqlInt(sourceCluster, "SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name, "+
		sqlLiteral("rpgm_s_"+c.run+"_")+")")
}

func (c *nativeFaultCase) ownedPublications() int {
	GinkgoHelper()
	return psqlInt(sourceCluster, "SELECT count(*) FROM pg_publication WHERE starts_with(pubname, "+
		sqlLiteral("rpgm_p_"+c.run+"_")+")")
}

// waitStreaming waits for caught-up streaming with positive evidence of the owned
// source resources, so their later absence or survival means something.
func (c *nativeFaultCase) waitStreaming() {
	GinkgoHelper()
	waitFollowStreaming(c.name)
	Expect(c.ownedSlots()).To(Equal(1), "%s owns no single slot on the source", c.name)
	Expect(c.ownedPublications()).To(Equal(1), "%s owns no single publication on the source", c.name)
	c.waitApplied(0)
}

// waitApplied waits until the target holds a writer row above after.
func (c *nativeFaultCase) waitApplied(after int) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		out, err := psqlDBErr(c.target, appDatabase(c.target), liveMarkerQuery(c.marker))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strconv.Atoi(out)).To(BeNumerically(">", after), "no writer row above %d reached the target", after)
	}, lagConvergeTimeout, 2*time.Second).Should(Succeed())
}

func (c *nativeFaultCase) stopWriter() int {
	GinkgoHelper()
	last, err := c.writer.stop()
	Expect(publicationRetryProbeError(err)).NotTo(HaveOccurred(), "the live writer did not finish")
	Expect(last).To(BeNumerically(">", 0), "the live writer committed nothing")
	return last
}

// finish waits until a row committed after the fault reached the target, on attempt
// minAttempts or later, or the Migration failed; it cuts over what resumed and checks
// the invariant.
func (c *nativeFaultCase) finish(minAttempts int32) {
	GinkgoHelper()
	before := psqlInt(sourceCluster, liveMarkerQuery(c.marker))
	key := client.ObjectKey{Namespace: nsE2E, Name: c.name}
	m := &v1beta1.Migration{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, key, m)).To(Succeed())
		if m.Status.Phase == v1beta1.PhaseFailed {
			return // expectInvariant decides whether this failure is closed
		}
		g.Expect(m.Status.Attempts).To(BeNumerically(">=", minAttempts), "%s has not started attempt %d", c.name,
			minAttempts)
		out, err := psqlDBErr(c.target, appDatabase(c.target), liveMarkerQuery(c.marker))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strconv.Atoi(out)).To(BeNumerically(">", before), "no row committed after the fault reached the target")
	}, migrationTimeout, 2*time.Second).Should(Succeed())
	last := c.stopWriter()
	if m.Status.Phase != v1beta1.PhaseFailed {
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, key, m)).To(Succeed())
			g.Expect([]v1beta1.MigrationPhase{v1beta1.PhaseCutoverPending, v1beta1.PhaseFailed}).
				To(ContainElement(m.Status.Phase), "%s at phase %q", c.name, m.Status.Phase)
		}, lagConvergeTimeout, 2*time.Second).Should(Succeed())
		if m.Status.Phase == v1beta1.PhaseCutoverPending {
			approveCutover(c.name)
			m = waitTerminal(c.name, followTimeout)
		}
	}
	c.expectInvariant(m, nativeFaultMarkerSet{c.marker, last})
}

// expectInvariant checks the terminal outcome, every marker set on both sides, the
// resources each outcome must keep or release, and the foreign resources.
func (c *nativeFaultCase) expectInvariant(m *v1beta1.Migration, sets ...nativeFaultMarkerSet) {
	GinkgoHelper()
	resumed, err := nativeFaultOutcome(m, c.ownedSlots())
	Expect(err).NotTo(HaveOccurred(), "INVARIANT VIOLATED on %s at attempt %d: %s", c.name, m.Status.Attempts,
		failureMessage(m))
	for _, set := range sets {
		Expect(readNativeFaultMarkers(sourceCluster, set).check(set.last, true)).
			To(Succeed(), "the source lost its own %s rows", set.marker)
		Expect(readNativeFaultMarkers(c.target, set).check(set.last, resumed)).
			To(Succeed(), "INVARIANT VIOLATED: %s rows on the target", set.marker)
	}
	if resumed {
		expectNativeFaultDataMatch(c.target)
		Expect(c.ownedPublications()).To(BeZero(), "completed with the owned publication left on the source")
		c.expectNoSessions()
	} else {
		Expect(c.ownedPublications()).To(Equal(1), "failed without retaining the owned publication")
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: c.name + "-work"}, pvc)).
			To(Succeed(), "the failed Migration lost its work volume")
		Expect(pvc.UID).To(Equal(c.pvc), "the failed Migration replaced its work volume")
	}
	c.foreign.expectUnchanged()
	AddReportEntry("native fault outcome", fmt.Sprintf("resumed=%t attempts=%d message=%s",
		resumed, m.Status.Attempts, failureMessage(m)))
}

func (c *nativeFaultCase) expectNoSessions() {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		for _, cluster := range []string{sourceCluster, c.target} {
			out, err := psqlDBErr(cluster, appDatabase(cluster), nativeFaultSessionSQL)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("0"), "native sessions remain on %s", cluster)
		}
	}, 2*time.Minute, 2*time.Second).Should(Succeed())
}

// expectNativeFaultDataMatch compares every table's row count and content hash, independently of the operator.
func expectNativeFaultDataMatch(target string) {
	GinkgoHelper()
	read := func(cluster string) map[string]string {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		rows, err := readFingerprint(readCtx, clusterPSQL(cluster), []string{kindPublicSchema, kindAuditSchema})
		Expect(publicationRetryProbeError(err)).NotTo(HaveOccurred(), "independent fingerprint of %s failed", cluster)
		data := map[string]string{}
		for _, row := range rows {
			if row.Aspect == compareDataCheck {
				data[row.Schema+"."+row.Key] = row.Value
			}
		}
		return data
	}
	source := read(sourceCluster)
	Expect(source).To(HaveKey(kindPublicSchema+"."+kindOrdersTable), "the source fingerprint has no orders data")
	Expect(read(target)).To(Equal(source), "target table data differs from the source")
}

// nativeFaultForeign records resources the Migration does not own, including
// native-shaped decoys of another run, which no fault or cleanup may change.
type nativeFaultForeign struct {
	target, source, origins string
	config                  *corev1.ConfigMap
}

func plantNativeFaultForeign(name, target string) *nativeFaultForeign {
	GinkgoHelper()
	DeferCleanup(dropNativeFaultForeign, target)
	dropNativeFaultForeign(target)
	for _, slot := range nativeFaultForeignSlots {
		psql(sourceCluster, "SELECT 1 FROM pg_create_physical_replication_slot("+sqlLiteral(slot)+")")
	}
	for _, publication := range nativeFaultForeignPubs {
		psql(sourceCluster, "CREATE PUBLICATION "+sqlIdent(publication))
	}
	psql(target, "SELECT 1 FROM pg_replication_origin_create("+sqlLiteral(nativeFaultForeignName)+")")
	f := &nativeFaultForeign{target: target, config: &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-foreign", Namespace: nsE2E,
			Labels: map[string]string{migrationLabel: name}},
		Data: map[string]string{"sentinel": name},
	}}
	Expect(k8sClient.Create(ctx, f.config)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, f.config))).To(Succeed()) })
	f.source, f.origins = f.state()
	Expect(strings.Split(f.source, ",")).To(HaveLen(len(nativeFaultForeignSlots)+len(nativeFaultForeignPubs)),
		"the foreign source resources were not all planted")
	Expect(f.origins).NotTo(BeEmpty(), "the foreign target origin was not planted")
	return f
}

func (f *nativeFaultForeign) state() (string, string) {
	GinkgoHelper()
	source := psql(sourceCluster, "SELECT coalesce(string_agg(x, ',' ORDER BY x), '') FROM ("+
		"SELECT 'slot ' || slot_name || ' ' || slot_type || ' ' || coalesce(restart_lsn::text, '-') || ' ' || active"+
		" FROM pg_replication_slots WHERE slot_name = ANY("+sqlTextArray(nativeFaultForeignSlots)+") UNION ALL"+
		" SELECT 'publication ' || pubname || ' ' || p.oid || ' ' || puballtables || ' ' ||"+
		" (SELECT count(*) FROM pg_publication_rel r WHERE r.prpubid = p.oid)"+
		" FROM pg_publication p WHERE pubname = ANY("+sqlTextArray(nativeFaultForeignPubs)+")) s(x)")
	origins := psql(f.target, "SELECT coalesce(string_agg(roname || ' ' || roident, ','), '')"+
		" FROM pg_replication_origin WHERE roname = "+sqlLiteral(nativeFaultForeignName))
	return source, origins
}

func (f *nativeFaultForeign) expectUnchanged() {
	GinkgoHelper()
	source, origins := f.state()
	Expect(source).To(Equal(f.source), "a foreign source slot or publication changed")
	Expect(origins).To(Equal(f.origins), "the foreign target origin changed")
	current := &corev1.ConfigMap{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(f.config), current)).
		To(Succeed(), "the foreign ConfigMap is gone")
	Expect(current.UID).To(Equal(f.config.UID), "the foreign ConfigMap was replaced")
	Expect(current.ResourceVersion).To(Equal(f.config.ResourceVersion), "the foreign ConfigMap was modified")
}

func dropNativeFaultForeign(target string) {
	GinkgoHelper()
	psql(sourceCluster, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"+
		" WHERE slot_name = ANY("+sqlTextArray(nativeFaultForeignSlots)+")")
	for _, publication := range nativeFaultForeignPubs {
		psql(sourceCluster, "DROP PUBLICATION IF EXISTS "+sqlIdent(publication))
	}
	psql(target, "SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin"+
		" WHERE roname = "+sqlLiteral(nativeFaultForeignName))
}

// nativeFaultKill is a SIGKILL armed for one worker container. Deleting the pod instead
// would also remove the terminal pod that native recovery reads.
type nativeFaultKill struct {
	worker, killer string
	attempt        int32
}

func armNativeFaultKill(name string) *nativeFaultKill {
	GinkgoHelper()
	k := &nativeFaultKill{worker: waitWorkerRunning(name, 1)}
	m := &v1beta1.Migration{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
	k.attempt = m.Status.Attempts
	worker := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: k.worker}, worker)).To(Succeed())
	Expect(worker.Labels[batchv1.JobNameLabel]).To(Equal(fmt.Sprintf("%s-run-%d", name, k.attempt)),
		"worker %s is not attempt %d's", k.worker, k.attempt)
	containers := len(worker.Status.ContainerStatuses)
	Expect(containers).To(Equal(1), "worker %s has %d containers", k.worker, containers)
	id := nativeFaultContainerID.FindStringSubmatch(worker.Status.ContainerStatuses[0].ContainerID)
	Expect(id).To(HaveLen(2), "worker %s has no containerd container ID", k.worker)
	killer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: name + "-kill-", Namespace: nsE2E},
		Spec: corev1.PodSpec{
			NodeName: worker.Spec.NodeName, HostPID: true, RestartPolicy: corev1.RestartPolicyNever,
			AutomountServiceAccountToken: ptr.To(false), TerminationGracePeriodSeconds: ptr.To(int64(0)),
			Containers: []corev1.Container{{
				Name: nativeFaultKillContainer, Image: selectedToolImage(), ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sh", "-c", nativeFaultKillScript, nativeFaultKillContainer, id[1]},
				SecurityContext: &corev1.SecurityContext{RunAsUser: ptr.To(int64(0)), RunAsNonRoot: ptr.To(false)},
			}},
		},
	}
	Expect(k8sClient.Create(ctx, killer)).To(Succeed())
	k.killer = killer.Name
	DeferCleanup(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, killer, client.GracePeriodSeconds(0)))).To(Succeed())
	})
	Eventually(func(g Gomega) {
		current := &corev1.Pod{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(killer), current)).To(Succeed())
		g.Expect(current.Status.Phase).To(Equal(corev1.PodRunning), "killer %s is %s, PodScheduled %s",
			k.killer, current.Status.Phase, podScheduled(current))
	}, workerRunningTimeout, time.Second).Should(Succeed())
	return k
}

// fire kills the worker and requires its pod to stay, terminated by SIGKILL.
func (k *nativeFaultKill) fire() {
	GinkgoHelper()
	fireCtx, cancel := context.WithTimeout(ctx, e2eCommandTimeout)
	defer cancel()
	args := suiteCommandArgs(psqlExecKubectl, []string{psqlExecSubcommand, "-n", nsE2E, k.killer,
		"-c", nativeFaultKillContainer, "--", "sh", "-c", ": > /tmp/fire"})
	out, err := exec.CommandContext(fireCtx, psqlExecKubectl, args...).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "firing killer %s: %s", k.killer, out)
	Eventually(func(g Gomega) {
		killer := &corev1.Pod{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: k.killer}, killer)).To(Succeed())
		if killer.Status.Phase == corev1.PodFailed {
			StopTrying("killer " + k.killer + " found no process of worker " + k.worker).Now()
		}
		g.Expect(killer.Status.Phase).To(Equal(corev1.PodSucceeded), "killer %s is %s", k.killer, killer.Status.Phase)
	}, 2*time.Minute, time.Second).Should(Succeed())
	Eventually(func(g Gomega) {
		worker := &corev1.Pod{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: k.worker}, worker)).
			To(Succeed(), "the killed worker pod is gone")
		containers := len(worker.Status.ContainerStatuses)
		g.Expect(containers).To(Equal(1), "worker %s has %d containers", k.worker, containers)
		state := worker.Status.ContainerStatuses[0].State.Terminated
		g.Expect(state).NotTo(BeNil(), "worker %s still runs", k.worker)
		g.Expect(state.ExitCode).To(Equal(int32(nativeFaultSIGKILL)), "worker %s exited %d, not by SIGKILL",
			k.worker, state.ExitCode)
		g.Expect(worker.Status.Phase).To(Equal(corev1.PodFailed), "worker %s is %s", k.worker, worker.Status.Phase)
	}, 2*time.Minute, time.Second).Should(Succeed())
}

// holdNativeFaultLock holds ACCESS EXCLUSIVE on table until the returned release runs,
// so an apply into it waits inside its transaction.
func holdNativeFaultLock(cluster, table string) func() {
	GinkgoHelper()
	lockCtx, cancel := context.WithCancel(ctx)
	conninfo := "dbname=" + appDatabase(cluster) + " application_name=" + nativeFaultLockApp
	cmd := exec.CommandContext(lockCtx, psqlExecKubectl, psqlArgv(cluster, primaryPod(cluster), conninfo, false,
		"-XAtq", "-v", "ON_ERROR_STOP=1", "-c", "BEGIN", "-c", "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE",
		"-c", "SELECT pg_sleep(3600)")...)
	Expect(cmd.Start()).To(Succeed())
	var once sync.Once
	release := func() {
		once.Do(func() {
			psql(cluster, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = "+
				sqlLiteral(nativeFaultLockApp))
			cancel()
			_ = cmd.Wait() // the terminated session exits nonzero
		})
	}
	DeferCleanup(release)
	Eventually(func(g Gomega) {
		out, err := psqlDBErr(cluster, appDatabase(cluster), "SELECT count(*) FROM pg_locks l"+
			" JOIN pg_stat_activity a ON a.pid = l.pid WHERE a.application_name = "+sqlLiteral(nativeFaultLockApp)+
			" AND l.relation = "+sqlLiteral(table)+"::regclass AND l.mode = 'AccessExclusiveLock' AND l.granted")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("1"), "the lock on %s is not held", table)
	}, time.Minute, time.Second).Should(Succeed())
	return release
}

// expectNativeFaultSyncDurable proves the target's primary commits only once its one
// standby has the commit: CNPG's FIRST 1 list, a streaming synchronous standby and the
// server default the native profile cannot override.
func expectNativeFaultSyncDurable(target string) {
	GinkgoHelper()
	Expect(psql(target, "SHOW synchronous_standby_names")).To(HavePrefix("FIRST 1 "), "the target is not synchronous")
	Expect(psql(target, "SHOW synchronous_commit")).To(Equal("on"), "target commits do not wait for the standby")
	Expect(syncStandbys(target)).To(Equal(1), "no synchronous standby streams from the target primary")
}
