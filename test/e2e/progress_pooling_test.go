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
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

const (
	progressPoolRunnerLifetimeSeconds int64 = 1800
	progressPoolSchema                      = "e2e_progress_pooling"
	progressPoolTable                       = progressPoolSchema + ".items"
	progressPoolApp                         = "e2e_progress_pooling"
	progressPoolImage                       = "ghcr.io/cloudnative-pg/pgbouncer:1.25.2-202609151159-trixie@" +
		"sha256:218c1c9bd0c217dae3a6561b75446b7f5dde0ada55b73956659ee4b8a4bcc8ee"
	progressPoolIdentitySQL = `SELECT pg_backend_pid(), extract(epoch FROM backend_start),
current_setting('statement_timeout'), current_user, current_database()
FROM pg_stat_activity WHERE pid=pg_backend_pid();`
)

var _ = Describe("Progress sampler transaction pooling", func() {
	It("restores pooled backends after sampling and cancellation, then permits long COPY and indexes", func() {
		cfg, err := config.GetConfig()
		Expect(err).NotTo(HaveOccurred())
		remote, err := podexec.New(cfg)
		Expect(err).NotTo(HaveOccurred())
		poller := progress.NewFromExec(remote, nil)
		job := createProgressPoolRunner()
		var pod string
		Eventually(func(g Gomega) {
			var lookupErr error
			pod, lookupErr = remote.RunningPod(ctx, nsE2E, job.Name)
			g.Expect(lookupErr).NotTo(HaveOccurred())
			g.Expect(pod).NotTo(BeEmpty())
		}, 3*time.Minute, time.Second).Should(Succeed())

		// Fixture workloads need more than podexec's 30-second sampler transport budget.
		pooledSQL := func(side conn.Side, sql string, budget time.Duration) (string, error) {
			queryCtx, cancel := context.WithTimeout(ctx, budget+10*time.Second)
			defer cancel()
			out, queryErr := commandOutput(queryCtx, exec.CommandContext, "kubectl",
				"exec", "-n", nsE2E, pod, "-c", "pgcopydb", "--", "sh", "-c", conn.URIRecover()+`
case "$1" in source) uri=$PGCOPYDB_SOURCE_PGURI ;; target) uri=$PGCOPYDB_TARGET_PGURI ;; *) exit 1 ;; esac
printf '%s\n' "$2" | timeout --signal=TERM --kill-after=1s "$3" psql "$uri" -XqtA -v ON_ERROR_STOP=1 -f -
`, "sh", string(side), sql, strconv.FormatInt(int64(budget/time.Second), 10)+"s")
			return strings.TrimSpace(string(out)), progressPoolCommandError(queryErr)
		}
		query := func(side conn.Side, sql string) string {
			GinkgoHelper()
			out, queryErr := pooledSQL(side, sql, 10*time.Second)
			Expect(queryErr).NotTo(HaveOccurred(), "%s pooled SQL failed", side)
			return out
		}
		sides := []conn.Side{conn.Source, conn.Target}
		clusters := []string{sourceCluster, targetCluster}
		baselines := make([]string, len(sides))
		pids := make([]int, len(sides))
		for i, side := range sides {
			Eventually(func(g Gomega) {
				out, queryErr := pooledSQL(side, "SELECT 1;", 10*time.Second)
				g.Expect(queryErr).NotTo(HaveOccurred(), "%s Pooler app connection failed", side)
				g.Expect(out).To(Equal("1"), "%s Pooler app query returned no valid result", side)
			}, 3*time.Minute, time.Second).Should(Succeed())
			Expect(query(side, "SELECT to_regnamespace('"+progressPoolSchema+"');")).To(BeEmpty())
			DeferCleanup(func() {
				psql(clusters[i], "DROP SCHEMA IF EXISTS "+progressPoolSchema+" CASCADE")
			})
			query(side, progressPoolSetupSQL())
			baselines[i] = query(side, progressPoolIdentitySQL)
			fields := strings.Split(baselines[i], "|")
			Expect(fields).To(HaveLen(5), "%s backend identity must be present", side)
			pids[i], err = strconv.Atoi(fields[0])
			Expect(err).NotTo(HaveOccurred())
			Expect(pids[i]).To(BeNumerically(">", 0))
			started, parseErr := strconv.ParseFloat(fields[1], 64)
			Expect(parseErr).NotTo(HaveOccurred())
			Expect(started).To(BeNumerically(">", 0))
			Expect(fields[2:]).To(Equal([]string{"30s", appDB, appDB}))
			Expect(query(side, progressPoolIdentitySQL)).To(Equal(baselines[i]),
				"a new client must reuse the original backend and its timeout")
			AddReportEntry(string(side)+" pooled backend before sampling", baselines[i])
		}
		assertRestored := func() {
			GinkgoHelper()
			for i, side := range sides {
				Expect(query(side, progressPoolIdentitySQL)).To(Equal(baselines[i]),
					"%s sampler changed the backend PID, start time, role, database, or timeout", side)
				Expect(psql(clusters[i], fmt.Sprintf(`SELECT count(*) FROM pg_stat_activity
WHERE pid=%d AND state='idle' AND xact_start IS NULL`, pids[i]))).To(Equal("1"),
					"%s pooled backend must survive without an open query or transaction", side)
			}
		}
		sample := func() *progress.Sample {
			GinkgoHelper()
			got, sampleErr := poller.Sample(ctx, nsE2E, job.Name, false)
			Expect(progressPoolCommandError(sampleErr)).NotTo(HaveOccurred(), "sampler exec failed")
			Expect(got).NotTo(BeNil())
			return got
		}
		assertComplete := func(got *progress.Sample) {
			GinkgoHelper()
			Expect(got.SourceSize).NotTo(BeNil())
			Expect(got.TargetSize).NotTo(BeNil())
			Expect(*got.SourceSize).To(BeNumerically(">", 0))
			Expect(*got.TargetSize).To(BeNumerically(">", 0))
			Expect(got.Counts).NotTo(BeNil())
			Expect(got.Counts.TablesTotal).To(BeNumerically(">", 0))
			Expect(got.Counts.TablesDone).To(BeNumerically(">", 0))
		}

		By("sampling successfully through both single-slot transaction pools")
		baseline := sample()
		assertComplete(baseline)
		assertRestored()
		allDB, err := poller.Sample(ctx, nsE2E, job.Name, true)
		Expect(progressPoolCommandError(err)).NotTo(HaveOccurred(), "all-databases sampler exec failed")
		Expect(allDB).NotTo(BeNil())
		Expect(allDB.SourceSize).NotTo(BeNil())
		Expect(allDB.TargetSize).NotTo(BeNil())
		Expect(*allDB.SourceSize).To(BeNumerically(">", 0))
		Expect(*allDB.TargetSize).To(BeNumerically(">", 0))
		Expect(allDB.Counts).To(BeNil())
		assertRestored()
		copying, finalizing := poller.CloneStage(ctx, nsE2E, job.Name)
		Expect(copying).To(BeFalse())
		Expect(finalizing).To(BeFalse())
		Expect(psql(targetCluster, fmt.Sprintf(`SELECT count(*) FROM pg_stat_activity
WHERE pid=%d AND state='idle' AND xact_start IS NULL AND query='COMMIT'`, pids[1]))).To(Equal("1"),
			"the stage probe must complete a transaction, not silently return no sample")
		assertRestored()

		for i, side := range sides {
			By("observing and cancelling a sampler blocked on the " + string(side) + " fixture lock")
			release := holdProgressLock(string(side), clusters[i], progressPoolTable)
			func() {
				defer release()
				sampleCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				type result struct {
					sample *progress.Sample
					err    error
				}
				done := make(chan result, 1)
				go func() {
					got, sampleErr := poller.Sample(sampleCtx, nsE2E, job.Name, false)
					done <- result{got, sampleErr}
				}()
				Eventually(func() string {
					return psql(clusters[i], fmt.Sprintf(`SELECT count(*) FROM pg_stat_activity a
WHERE a.pid=%d AND a.query LIKE 'with t as (%%' AND a.wait_event_type='Lock'
AND EXISTS (SELECT 1 FROM pg_stat_activity b WHERE b.application_name='e2e_progress_blocker'
AND b.pid=ANY(pg_blocking_pids(a.pid)))`, pids[i]))
				}, 15*time.Second, 200*time.Millisecond).Should(Equal("1"),
					"the original pooled backend must wait on the test-owned lock before cancellation")
				var completed result
				Eventually(done, 8*time.Second).Should(Receive(&completed))
				Expect(progressPoolCommandError(completed.err)).NotTo(HaveOccurred(), "blocked sampler exec failed")
				Expect(completed.sample).NotTo(BeNil())
				Expect(completed.sample.Counts).To(BeNil())
				if side == conn.Source {
					Expect(completed.sample.SourceSize).To(BeNil())
					Expect(completed.sample.TargetSize).NotTo(BeNil())
					Expect(*completed.sample.TargetSize).To(BeNumerically(">", 0))
				} else {
					Expect(completed.sample.TargetSize).To(BeNil())
					Expect(completed.sample.SourceSize).NotTo(BeNil())
					Expect(*completed.sample.SourceSize).To(BeNumerically(">", 0))
				}
				Expect(psql(clusters[i], progressBlockerCount)).To(Equal("1"))
				assertRestored()
			}()
			assertComplete(sample())
			assertRestored()
		}

		By("running server-delayed COPY TO, COPY FROM, and CREATE INDEX on the same pooled backends")
		for i, side := range sides {
			out, queryErr := pooledSQL(side, progressPoolLongWorkSQL(), time.Minute)
			Expect(queryErr).NotTo(HaveOccurred(), "%s long COPY and index workload failed", side)
			work := strings.Split(out, "\n")
			Expect(work).To(HaveLen(6), "%s long SQL results must all be present", side)
			Expect(work[0]).To(Equal(baselines[i]))
			Expect(work[1]).To(Equal("1"), "COPY TO must return the fixture row")
			Expect(work[5]).To(Equal(baselines[i]))
			for j, operation := range []string{"COPY TO", "COPY FROM", "CREATE INDEX"} {
				seconds, parseErr := strconv.ParseFloat(work[j+2], 64)
				Expect(parseErr).NotTo(HaveOccurred())
				Expect(seconds).To(BeNumerically(">", 5), "%s %s must exceed the sampler timeout", side, operation)
				AddReportEntry(string(side)+" "+operation+" server seconds", seconds)
			}
			Expect(query(side, "SELECT (SELECT count(*) FROM "+progressPoolTable+"), indisvalid "+
				"FROM pg_index WHERE indexrelid='"+progressPoolSchema+".slow_index'::regclass;")).To(Equal("1|t"),
				"COPY rows and a valid index must remain committed for a new client")
			AddReportEntry(string(side)+" pooled backend after long COPY and index", query(side, progressPoolIdentitySQL))
		}
		recovered := sample()
		assertComplete(recovered)
		Expect(recovered.Counts.IndexesTotal).To(Equal(baseline.Counts.IndexesTotal + 1))
		Expect(recovered.Counts.IndexesDone).To(Equal(baseline.Counts.IndexesDone + 1))
		assertRestored()
	})
})

// Exec errors can contain connection details; retain only the failure class and exit status.
func progressPoolCommandError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("progress pooling command timed out")
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return fmt.Errorf("progress pooling command exited with status %d", exit.ExitCode())
	}
	return fmt.Errorf("progress pooling command failed (%T)", err)
}

func createProgressPoolRunner() *batchv1.Job {
	GinkgoHelper()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsE2E, GenerateName: "e2e-progress-pooling-"},
		Spec: batchv1.JobSpec{
			BackoffLimit:          ptr.To(int32(0)),
			ActiveDeadlineSeconds: ptr.To(progressPoolRunnerLifetimeSeconds),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy:                corev1.RestartPolicyNever,
				AutomountServiceAccountToken: ptr.To(false),
				SecurityContext: &corev1.PodSecurityContext{
					RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(65532)), RunAsGroup: ptr.To(int64(65532)),
					FSGroup:        ptr.To(int64(65532)),
					SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
				Volumes: []corev1.Volume{{
					Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
				Containers: []corev1.Container{{
					Name:  "pgcopydb",
					Image: "ghcr.io/ydixken/pgcopydb-operator/runner:" + runnerTag,
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
						Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					},
					VolumeMounts: []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}},
					Env: []corev1.EnvVar{
						{Name: "PGPASSFILE", Value: conn.PgpassPath},
						{Name: "PGAPPNAME", Value: progressPoolApp},
					},
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi"),
					}},
				}},
			}},
		},
	}
	passfiles := make([]conn.Passfile, 0, 2)
	for i, clusterName := range []string{sourceCluster, targetCluster} {
		cluster := &unstructured.Unstructured{}
		cluster.SetGroupVersionKind(cnpgGVK)
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: clusterName}, cluster)).To(Succeed())
		owner := metav1.OwnerReference{
			APIVersion: cnpgGVK.GroupVersion().String(), Kind: cnpgGVK.Kind, Name: clusterName, UID: cluster.GetUID(),
		}
		clusterRef, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&corev1.LocalObjectReference{
			Name: clusterName,
		})
		Expect(err).NotTo(HaveOccurred())
		podTemplate, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "pgbouncer", Image: progressPoolImage,
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi"),
				}},
			}}},
		})
		Expect(err).NotTo(HaveOccurred())
		pooler := &unstructured.Unstructured{Object: map[string]any{
			"spec": map[string]any{
				"cluster": clusterRef, "instances": int64(1), "type": "rw",
				"pgbouncer": map[string]any{
					"poolMode": "transaction",
					"parameters": map[string]any{
						"default_pool_size": "1", "reserve_pool_size": "0", "max_client_conn": "20",
						"server_reset_query_always": "0", "server_idle_timeout": "0", "server_lifetime": "3600",
						"query_timeout": "0", "query_wait_timeout": "0", "idle_transaction_timeout": "0",
					},
				},
				"template": podTemplate,
			},
		}}
		pooler.SetAPIVersion(cnpgGVK.GroupVersion().String())
		pooler.SetKind("Pooler")
		pooler.SetNamespace(nsE2E)
		pooler.SetGenerateName(clusterName + "-progress-")
		// CNPG leaves Poolers independent of Clusters unless we give them an owner.
		pooler.SetOwnerReferences([]metav1.OwnerReference{owner})
		Expect(k8sClient.Create(ctx, pooler)).To(Succeed())
		podLabels := map[string]string{"cnpg.io/poolerName": pooler.GetName()}
		cleanupProgressPoolObject(pooler, podLabels)
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(ctx, pods, client.InNamespace(nsE2E),
				client.MatchingLabels(podLabels))).To(Succeed())
			found := len(pods.Items)
			g.Expect(found).To(Equal(1), "%s Pooler selector must identify one pod", clusterName)
			poolPod := &pods.Items[0]
			g.Expect(poolPod.DeletionTimestamp).To(BeNil())
			g.Expect(poolPod.Status.Phase).To(Equal(corev1.PodRunning))
			ready := false
			for _, condition := range poolPod.Status.Conditions {
				if condition.Type == corev1.PodReady {
					ready = condition.Status == corev1.ConditionTrue
				}
			}
			g.Expect(ready).To(BeTrue(), "%s Pooler pod must be Ready", clusterName)
		}, 3*time.Minute, time.Second).Should(Succeed())
		if i == 0 {
			job.OwnerReferences = []metav1.OwnerReference{owner}
		}
		connection := &v1beta1.PostgresConnection{
			Host: pooler.GetName(), Database: appDB, Username: appDB, SSLMode: "require",
			PasswordSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: clusterName + "-app"}, Key: passwordKey,
			},
		}
		materialized, err := conn.Materialize([]conn.Side{conn.Source, conn.Target}[i], connection)
		Expect(err).NotTo(HaveOccurred())
		container := &job.Spec.Template.Spec.Containers[0]
		container.Env = append(container.Env, materialized.Env...)
		container.VolumeMounts = append(container.VolumeMounts, materialized.Mounts...)
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, materialized.Volumes...)
		Expect(materialized.Passfile).NotTo(BeNil())
		passfiles = append(passfiles, *materialized.Passfile)
	}
	// This pod hosts real sampler execs, not a migration with session-bound snapshot/apply state.
	job.Spec.Template.Spec.Containers[0].Command = []string{
		"sh", "-c", conn.PreludeScript(nil, passfiles, ""), "sleep", strconv.FormatInt(progressPoolRunnerLifetimeSeconds, 10),
	}
	Expect(k8sClient.Create(ctx, job)).To(Succeed())
	cleanupProgressPoolObject(job, map[string]string{batchv1.JobNameLabel: job.Name})
	return job
}

func cleanupProgressPoolObject(obj client.Object, podLabels map[string]string) {
	GinkgoHelper()
	uid := obj.GetUID()
	Expect(uid).NotTo(BeEmpty())
	DeferCleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		err := k8sClient.Delete(cleanupCtx, obj, client.Preconditions{UID: &uid},
			client.PropagationPolicy(metav1.DeletePropagationForeground))
		Expect(client.IgnoreNotFound(err)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(cleanupCtx, client.ObjectKeyFromObject(obj), obj)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "pooled fixture still exists")
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(cleanupCtx, pods, client.InNamespace(nsE2E),
				client.MatchingLabels(podLabels))).To(Succeed())
			remaining := len(pods.Items)
			g.Expect(remaining).To(BeZero(), "pooled fixture pods still exist")
		}, time.Minute, time.Second).Should(Succeed())
	})
}

func progressPoolSetupSQL() string {
	return `SET statement_timeout='30s';
CREATE SCHEMA ` + progressPoolSchema + `;
CREATE TABLE ` + progressPoolTable + ` (id integer);
INSERT INTO ` + progressPoolTable + ` VALUES (1);
CREATE FUNCTION ` + progressPoolSchema + `.slow_identity(id integer) RETURNS integer
LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN PERFORM pg_sleep(6); RETURN id; END $$;
CREATE FUNCTION ` + progressPoolSchema + `.slow_insert() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN PERFORM ` + progressPoolSchema + `.slow_identity(NEW.id); RETURN NEW; END $$;
CREATE TRIGGER slow_insert BEFORE INSERT ON ` + progressPoolTable + `
FOR EACH ROW EXECUTE FUNCTION ` + progressPoolSchema + `.slow_insert();`
}

func progressPoolLongWorkSQL() string {
	// Server-side delays make the timeout regression independent of fixture scale and client scheduling.
	return "BEGIN;\n" + progressPoolIdentitySQL + `
SELECT clock_timestamp() AS started \gset
COPY (SELECT ` + progressPoolSchema + `.slow_identity(id) FROM ` + progressPoolTable + `) TO STDOUT;
SELECT extract(epoch FROM clock_timestamp() - :'started'::timestamptz);
TRUNCATE ` + progressPoolTable + `;
SELECT clock_timestamp() AS started \gset
COPY ` + progressPoolTable + ` FROM STDIN;
1
\.
SELECT extract(epoch FROM clock_timestamp() - :'started'::timestamptz);
SELECT clock_timestamp() AS started \gset
CREATE INDEX slow_index ON ` + progressPoolTable + ` (` + progressPoolSchema + `.slow_identity(id));
SELECT extract(epoch FROM clock_timestamp() - :'started'::timestamptz);
` + progressPoolIdentitySQL + "\nCOMMIT;"
}
