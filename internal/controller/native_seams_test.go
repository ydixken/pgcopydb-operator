package controller

// TestNativeSeams drives one native follow migration and one killed retry through the
// reconciler against real PostgreSQL and the qualified runner, cluster-free. Each helper
// and exec program runs from the operator's own builders against engine-written state.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	seamsPairEnv     = "NATIVE_SEAMS_PAIR"
	seamsLockEnv     = "NATIVE_SEAMS_IMAGE_LOCK"
	seamsEvidenceEnv = "NATIVE_SEAMS_EVIDENCE"
	seamsOwnerEnv    = "NATIVE_SEAMS_OWNER"
	seamsScale       = "0.01"
	seamsAppRole     = "app"
	seamsAppDatabase = "app"
	seamsPasswordKey = "password"
	seamsRetryDB     = "seams_retry"
	seamsLiveMarker  = "seams-live-"
	seamsPortNumber  = 5432
	seamsSlotKind    = "replication_slot"
	seamsSourceSide  = "source"
	seamsTargetSide  = "target"
	// seamsPublicationKind names the owned-inventory resource kind of the source publication.
	seamsPublicationKind = "publication"
)

type seamsImages struct{ runner, source, target string }

// seamsLockImages reads the M7 image lock: the qualified runner and the pair's PostgreSQL images.
func seamsLockImages(t *testing.T, path, pair string) seamsImages {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("image lock: %v", err)
	}
	var lock struct {
		Schema int `json:"schema_version"`
		Images map[string]struct {
			Ref, Digest, Architecture string
		} `json:"images"`
	}
	if err := json.Unmarshal(raw, &lock); err != nil || lock.Schema != 2 {
		t.Fatalf("image lock is not schema 2: %v", err)
	}
	majors := map[string][2]int{"16:16": {16, 16}, "18:18": {18, 18}, "16:18": {16, 18}}[pair]
	if majors[0] == 0 {
		t.Fatalf("%s must be 16:16, 18:18 or 16:18, got %q", seamsPairEnv, pair)
	}
	ref := func(role string) string {
		image, ok := lock.Images[role]
		if !ok || image.Ref == "" || !strings.HasSuffix(image.Ref, "@"+image.Digest) {
			t.Fatalf("image lock has no pinned %s", role)
		}
		return image.Ref
	}
	images := seamsImages{runner: ref("native-runner"), source: ref(fmt.Sprintf("postgresql-%d", majors[0])), target: ref(fmt.Sprintf("postgresql-%d", majors[1]))}
	// The lock pins a repository ref with a tag; the operator admits only repository@digest.
	repo, digest, _ := strings.Cut(images.runner, "@")
	if colon := strings.LastIndex(repo, ":"); colon > strings.LastIndex(repo, "/") {
		repo = repo[:colon]
	}
	images.runner = repo + "@" + digest
	if !nativeRunnerQualified(images.runner) {
		t.Fatalf("the locked runner %s is not qualified by this operator", images.runner)
	}
	return images
}

// seamsTargetGrantSQL is the native target follow grant: only SET on
// session_replication_role, since native uses no replication origin (prerequisites.md).
const seamsTargetGrantSQL = `GRANT SET ON PARAMETER session_replication_role TO app;`

type seamsVerdict struct {
	Step     string            `json:"step"`
	Program  string            `json:"program"`
	Passed   bool              `json:"passed"`
	Detail   string            `json:"detail"`
	Fallback string            `json:"fallback,omitempty"`
	Seconds  float64           `json:"seconds"`
	Execs    []seamsExecRecord `json:"execs,omitempty"`
	Pods     []string          `json:"failed_pods,omitempty"`
}

type seamsHarness struct {
	t        *testing.T
	pair     string
	owner    string
	images   seamsImages
	cluster  *seamsCluster
	r        *MigrationReconciler
	source   string
	target   string
	password string
	verdicts []seamsVerdict
}

// TestSeamsTargetGrant pins the harness to the native grant set of prerequisites.md.
func TestSeamsTargetGrant(t *testing.T) {
	if seamsTargetGrantSQL != "GRANT SET ON PARAMETER session_replication_role TO app;" {
		t.Fatalf("the native harness grants %q, want only SET on session_replication_role", seamsTargetGrantSQL)
	}
}

func TestNativeSeams(t *testing.T) {
	pair := os.Getenv(seamsPairEnv)
	if pair == "" {
		t.Skip(seamsPairEnv + " selects the cluster-free native seam harness (task test:native-seams)")
	}
	lock := os.Getenv(seamsLockEnv)
	if lock == "" {
		t.Fatalf("%s is required with %s", seamsLockEnv, seamsPairEnv)
	}
	if _, err := exec.LookPath("podman"); err != nil {
		t.Fatal("podman is required")
	}
	images := seamsLockImages(t, lock, pair)
	owner := os.Getenv(seamsOwnerEnv)
	if owner == "" {
		owner = "native-seams"
	}
	h := &seamsHarness{t: t, pair: pair, owner: owner, images: images, password: seamsRandom(16)}
	h.prepare()
	h.runFollow()
	h.runRetry()
	h.report()
}

func (h *seamsHarness) prepare() {
	t := h.t
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, ref := range []string{h.images.runner, h.images.source, h.images.target} {
		if _, _, err := seamsPodman(ctx, nil, "image", "exists", ref); err != nil {
			if _, stderr, err := seamsPodman(ctx, nil, "pull", "-q", ref); err != nil {
				t.Fatalf("pull %s: %v %s", ref, err, stderr)
			}
		}
	}
	h.cluster = newSeamsCluster(t, h.owner, h.images.source)
	h.cluster.redact = append(h.cluster.redact, h.password)
	h.source = h.startDatabase(ctx, seamsSourceSide, h.images.source)
	h.target = h.startDatabase(ctx, seamsTargetSide, h.images.target)
	h.prepareSource(ctx)
	for _, db := range []string{seamsAppDatabase, seamsRetryDB} {
		h.prepareTarget(ctx, db)
	}
	for _, side := range []string{seamsSourceSide, seamsTargetSide} {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "seams-" + side + "-app", Namespace: seamsNamespace}, Data: map[string][]byte{seamsPasswordKey: []byte(h.password)}}
		if err := h.cluster.store.Create(ctx, secret); err != nil {
			t.Fatal(err)
		}
	}
	samplerCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	sampler := NewSampler(nil)
	go func() { _ = sampler.Start(samplerCtx) }()
	recorder := events.NewFakeRecorder(4096)
	go func() {
		for {
			select {
			case <-samplerCtx.Done():
				return
			case <-recorder.Events:
			}
		}
	}()
	c := h.cluster
	h.r = &MigrationReconciler{Client: c.api, APIReader: c.reader, Scheme: c.scheme, Recorder: recorder, RPGMigrateRunnerImage: h.images.runner,
		NativePreflight: c.exec, NativeStop: c.exec, NativeEvidence: native.NewReader(c.reader, c.exec), NativeControl: native.NewClient(c.exec), NativeJobs: c.exec, Sampler: sampler}
	for started := false; !started; time.Sleep(10 * time.Millisecond) {
		sampler.mu.Lock()
		started = sampler.ctx != nil
		sampler.mu.Unlock()
	}
}

// startDatabase runs a capped PostgreSQL with the logical-replication settings the suite's CNPG clusters carry.
func (h *seamsHarness) startDatabase(ctx context.Context, side, image string) string {
	name := fmt.Sprintf("seams-%s-%s", h.cluster.run, side)
	script := `set -eu
initdb -D "$PGDATA" -U postgres --encoding=UTF8 --locale=C --auth-local=trust --auth-host=scram-sha-256 >/dev/null
printf 'host all all 0.0.0.0/0 scram-sha-256\nhost replication all 0.0.0.0/0 scram-sha-256\n' >> "$PGDATA/pg_hba.conf"
exec postgres -D "$PGDATA" -c listen_addresses='*' -c unix_socket_directories=/tmp -c wal_level=logical -c max_wal_senders=10 \
 -c max_replication_slots=10 -c wal_sender_timeout=60s -c shared_buffers=256MB -c max_connections=100 -c password_encryption=scram-sha-256`
	args := []string{seamsRunWord, "-d", "--rm", "--name", name, seamsLabelFlag, seamsOwnerKey + "=" + h.owner, seamsNetworkFlag, h.cluster.network,
		"--cpus", "2", "--memory", "2g", "--memory-swap", "2g", "--pids-limit", "512", "--user", "26",
		"--env", "PGDATA=/var/lib/postgresql/data/pgdata", "-v", "/var/lib/postgresql/data", "--entrypoint", shellPath, image, "-c", script}
	if _, stderr, err := seamsPodman(ctx, nil, args...); err != nil {
		h.t.Fatalf("start %s database: %v %s", side, err, stderr)
	}
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _, _ = seamsPodman(ctx, nil, "rm", "-f", "-v", "-t", "5", name)
	})
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if _, _, err := seamsPodman(ctx, nil, seamsExecWord, name, "pg_isready", "-h", "/tmp", "-q"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("%s database never became ready", side)
		}
		time.Sleep(time.Second)
	}
	return name
}

// psql runs SQL as the local superuser over the socket, so no password travels.
func (h *seamsHarness) psql(ctx context.Context, container, database, sql string) (string, error) {
	stdout, stderr, err := seamsPodman(ctx, []byte(sql), seamsExecWord, "-i", container, "psql", "-h", "/tmp", "-U", "postgres", "-d", database, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1")
	if err != nil {
		return "", fmt.Errorf("psql: %v %s", err, h.cluster.scrub(string(stderr)))
	}
	return strings.TrimSpace(string(stdout)), nil
}

func (h *seamsHarness) mustSQL(ctx context.Context, container, database, sql string) string {
	h.t.Helper()
	out, err := h.psql(ctx, container, database, sql)
	if err != nil {
		h.t.Fatal(err)
	}
	return out
}

func (h *seamsHarness) createRole(ctx context.Context, container string, replication bool) {
	attr := ""
	if replication {
		attr = " REPLICATION"
	}
	h.mustSQL(ctx, container, "postgres", "CREATE ROLE "+seamsAppRole+" LOGIN"+attr+" PASSWORD '"+h.password+"';")
}

// prepareSource loads the native M7 fixture as the app role, at the guard's fixed scale.
func (h *seamsHarness) prepareSource(ctx context.Context) {
	h.createRole(ctx, h.source, true)
	h.mustSQL(ctx, h.source, "postgres", "CREATE DATABASE "+seamsAppDatabase+" OWNER "+seamsAppRole+";")
	fixtures, err := filepath.Abs("../../test/e2e/fixtures")
	if err != nil {
		h.t.Fatal(err)
	}
	if _, stderr, err := seamsPodman(ctx, nil, "cp", fixtures, h.source+":/tmp/fixtures"); err != nil {
		h.t.Fatalf("copy fixtures: %v %s", err, stderr)
	}
	env := []string{"SEED_SCALE=" + seamsScale, "SEED_PROFILE=v5+native", "SEED_EXTRA_TABLES=0", "SEED_EXTRA_MB=0", "SEED_EXTRA_JOBS=4", "SEED_EXTRA_SKEW=0",
		"SEED_ENGINE=rpgmigrate", "PGHOST=/tmp", "PGUSER=" + seamsAppRole, "PGDATABASE=" + seamsAppDatabase}
	args := make([]string, 0, 1+2*len(env)+3)
	args = append(args, seamsExecWord)
	for _, e := range env {
		args = append(args, "--env", e)
	}
	args = append(args, h.source, "bash", "/tmp/fixtures/run.sh")
	if _, stderr, err := seamsPodman(ctx, nil, args...); err != nil {
		h.t.Fatalf("seed: %v %s", err, h.cluster.scrub(string(stderr)))
	}
}

// prepareTarget creates an empty app-owned database with the native target follow grant.
func (h *seamsHarness) prepareTarget(ctx context.Context, db string) {
	if db == seamsAppDatabase {
		h.createRole(ctx, h.target, false)
	}
	h.mustSQL(ctx, h.target, "postgres", "CREATE DATABASE "+db+" OWNER "+seamsAppRole+";")
	h.mustSQL(ctx, h.target, db, seamsTargetGrantSQL)
}

// newMigration mirrors the suite's native follow Migration: pgoutput, manual cutover, native clone options.
func (h *seamsHarness) newMigration(name, database string) *v1beta1.Migration {
	conn := func(host, db, secret string) v1beta1.PostgresConnection {
		return v1beta1.PostgresConnection{Host: host + seamsDNSDomain, Port: seamsPortNumber, Database: db, Username: seamsAppRole,
			PasswordSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: seamsPasswordKey}}
	}
	return &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: seamsNamespace, UID: seamsUID()},
		Spec: v1beta1.MigrationSpec{
			Engine: v1beta1.MigrationEngineRPGMigrate,
			Source: conn(h.source, seamsAppDatabase, "seams-source-app"), Target: conn(h.target, database, "seams-target-app"),
			WorkVolume: v1beta1.WorkVolume{Size: resource.MustParse("1Gi")},
			Clone: v1beta1.CloneOptions{UseCopyBinary: ptr.To(false), SplitTablesLargerThan: ptr.To(resource.MustParse("0")),
				Skip: []v1beta1.SkipOption{"largeObjects"}},
			Follow:       &v1beta1.FollowOptions{Enabled: true, Plugin: "pgoutput"},
			Cutover:      v1beta1.CutoverSpec{Mode: v1beta1.CutoverManual},
			Verification: &v1beta1.VerificationOptions{Schema: true, Data: true},
		},
	}
}

// check runs one step, records its verdict with the execs it made, and never stops the run.
func (h *seamsHarness) check(step, program string, run func() (string, error)) bool {
	h.t.Helper()
	before, failed, started := h.cluster.execCount(), h.cluster.failureCount(), time.Now()
	detail, err := run()
	v := seamsVerdict{Step: step, Program: program, Passed: err == nil, Detail: detail, Seconds: time.Since(started).Round(time.Millisecond).Seconds()}
	if err != nil {
		v.Detail = strings.TrimSpace(detail + " " + err.Error())
		v.Execs = h.cluster.execsSince(before)
		if len(v.Execs) > 6 {
			v.Execs = v.Execs[len(v.Execs)-6:]
		}
		v.Pods = h.cluster.failuresSince(failed)
		if len(v.Pods) > 4 {
			v.Pods = v.Pods[len(v.Pods)-4:]
		}
	}
	v.Detail = h.cluster.scrub(v.Detail)
	h.verdicts = append(h.verdicts, v)
	mark := "PASS"
	if !v.Passed {
		mark = "FAIL"
	}
	h.t.Logf("%s %s [%s]: %s", mark, step, program, v.Detail)
	for _, pod := range v.Pods {
		h.t.Logf("  failed pod %s", pod)
	}
	for _, e := range v.Execs {
		h.t.Logf("  exec %s %s exit %d stdout %q stderr %q", e.Pod, e.Program, e.Exit, e.Stdout, e.Stderr)
	}
	return v.Passed
}

func (h *seamsHarness) fallback(note string) {
	if n := len(h.verdicts); n > 0 {
		h.verdicts[n-1].Fallback = note
	}
	h.t.Logf("FALLBACK %s", note)
}

type seamsObjects struct {
	m      *v1beta1.Migration
	worker *batchv1.Job
	pvc    *corev1.PersistentVolumeClaim
}

func (h *seamsHarness) objects(ctx context.Context, name string) (seamsObjects, error) {
	o := seamsObjects{m: &v1beta1.Migration{}}
	if err := h.cluster.store.Get(ctx, client.ObjectKey{Namespace: seamsNamespace, Name: name}, o.m); err != nil {
		return o, err
	}
	if o.m.Status.JobName != "" {
		o.worker = &batchv1.Job{}
		if err := h.cluster.store.Get(ctx, client.ObjectKey{Namespace: seamsNamespace, Name: o.m.Status.JobName}, o.worker); err != nil {
			return o, err
		}
	}
	o.pvc = &corev1.PersistentVolumeClaim{}
	if err := h.cluster.store.Get(ctx, client.ObjectKey{Namespace: seamsNamespace, Name: workPVCName(o.m)}, o.pvc); err != nil {
		o.pvc = nil
	}
	return o, nil
}

// reconcileUntil drives Reconcile passes, as the manager's requeues would, until done or the deadline.
func (h *seamsHarness) reconcileUntil(ctx context.Context, name string, limit time.Duration, done func(seamsObjects) bool) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	var last error
	passes := 0
	for {
		o, err := h.objects(ctx, name)
		if err == nil && done(o) {
			return fmt.Sprintf("after %d reconcile passes, phase %s", passes, o.m.Status.Phase), nil
		}
		if err == nil && o.m.Status.Phase == v1beta1.PhaseFailed {
			return seamsConditions(o.m), fmt.Errorf("the Migration failed after %d passes", passes)
		}
		if err != nil && client.IgnoreNotFound(err) == nil && done(seamsObjects{}) {
			return fmt.Sprintf("after %d reconcile passes, Migration removed", passes), nil
		}
		if ctx.Err() != nil {
			summary := ""
			if o.m != nil {
				summary = seamsConditions(o.m)
			}
			return summary, fmt.Errorf("timed out after %d passes, last reconcile error: %v", passes, last)
		}
		if _, err := h.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: seamsNamespace, Name: name}}); err != nil {
			last = err
		}
		passes++
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

func seamsConditions(m *v1beta1.Migration) string {
	parts := make([]string, 0, len(m.Status.Conditions))
	for _, c := range m.Status.Conditions {
		parts = append(parts, fmt.Sprintf("%s=%s/%s", c.Type, c.Status, c.Reason))
	}
	return fmt.Sprintf("phase %s, attempts %d, %s", m.Status.Phase, m.Status.Attempts, strings.Join(parts, " "))
}

func seamsWorkerRunning(o seamsObjects) bool {
	return o.worker != nil && o.worker.Status.Active == 1 && o.worker.Status.Ready != nil && *o.worker.Status.Ready == 1
}

// liveWriter commits one marked order a tick on the source, like the suite's live writer.
type seamsWriter struct {
	mu   sync.Mutex
	last int
	err  error
	stop chan struct{}
	done chan struct{}
}

func (h *seamsHarness) startWriter(marker string) *seamsWriter {
	w := &seamsWriter{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for i := 1; ; i++ {
			select {
			case <-w.stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			_, err := h.psql(ctx, h.source, seamsAppDatabase, fmt.Sprintf("INSERT INTO orders (customer_id, amount, note) VALUES (1, %d, '%s%d');", i%90, marker, i))
			cancel()
			w.mu.Lock()
			if err != nil && w.err == nil {
				w.err = err
			}
			if err == nil {
				w.last = i
			}
			w.mu.Unlock()
		}
	}()
	return w
}

func (w *seamsWriter) halt() (int, error) {
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last, w.err
}

func (h *seamsHarness) execution(ctx context.Context, name string) (native.ExecutionData, seamsObjects, error) {
	o, err := h.objects(ctx, name)
	if err != nil || o.worker == nil || o.pvc == nil {
		return native.ExecutionData{}, o, fmt.Errorf("worker objects unavailable: %v", err)
	}
	w, err := h.r.admitNativeWorkerExecution(ctx, o.m, o.worker, o.pvc)
	return w, o, err
}

func runIDFor(m *v1beta1.Migration) string { return strings.ReplaceAll(string(m.UID), "-", "") }

// probeStreaming reads the worker's own progress through the control exec until the base copy is done.
func (h *seamsHarness) probeStreaming(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	var last error
	for {
		w, _, err := h.execution(ctx, name)
		if err == nil {
			report, perr := h.r.NativeControl.Progress(ctx, w)
			if perr == nil && report.Phase == nativeProgressStreamingPhase {
				tables := ""
				if report.Copy != nil && report.Copy.Tables != nil && report.Copy.Tables.Done != nil {
					tables = fmt.Sprintf(", %d tables copied", *report.Copy.Tables.Done)
				}
				return "worker phase streaming" + tables, nil
			}
			last = perr
			if perr == nil {
				last = fmt.Errorf("worker phase %s", report.Phase)
			}
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("never streaming: %v", last)
		case <-time.After(time.Second):
		}
	}
}

// runFollow is the M7 live-writer path: clone and follow under a writer, sample lag, set endpos, drain,
// read every completion helper, verify, clean up, finish and delete.
func (h *seamsHarness) runFollow() {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	const name = "seams-follow"
	m := h.newMigration(name, seamsAppDatabase)
	run := runIDFor(m)
	writer := h.startWriter(seamsLiveMarker)
	defer func() { _, _ = writer.halt() }()
	if err := h.cluster.api.Create(ctx, m); err != nil {
		h.t.Fatal(err)
	}
	if !h.check("follow/start", "version and preflight Jobs, work claim, worker Job", func() (string, error) {
		return h.reconcileUntil(ctx, name, 6*time.Minute, func(o seamsObjects) bool { return o.m != nil && seamsWorkerRunning(o) })
	}) {
		return
	}
	h.check("follow/streaming", "control exec: rpgmigrate list progress", func() (string, error) { return h.probeStreaming(ctx, name) })
	h.check("follow/lag", "source exec: active_lag observation", func() (string, error) {
		w, o, err := h.execution(ctx, name)
		if err != nil {
			return "", err
		}
		lag, err := h.r.NativeControl.SampleLag(ctx, o.m, w)
		if err != nil {
			return "", err
		}
		value := lag.LagBytes()
		if value == nil {
			return "", errors.New("lag sample carries no lag")
		}
		return fmt.Sprintf("lag %d bytes", *value), nil
	})
	h.check("follow/lag-status", "reconcile: sampler publishes lag and CaughtUp", func() (string, error) {
		return h.reconcileUntil(ctx, name, 2*time.Minute, func(o seamsObjects) bool {
			c := apimeta.FindStatusCondition(o.m.Status.Conditions, v1beta1.ConditionCaughtUp)
			return o.m.Status.Replication != nil && o.m.Status.Replication.LagBytes != nil && c != nil && c.Reason != "NativeLagUnknown"
		})
	})
	h.check("follow/source-head", "source exec: source_head observation", func() (string, error) {
		w, o, err := h.execution(ctx, name)
		if err != nil {
			return "", err
		}
		end, err := h.r.NativeControl.CurrentEndLSN(ctx, o.m, w)
		return "source head " + end, err
	})
	last, werr := writer.halt()
	h.check("follow/writer", "live writer on the source", func() (string, error) {
		if werr != nil || last == 0 {
			return "", fmt.Errorf("writer committed %d rows: %v", last, werr)
		}
		return fmt.Sprintf("%d rows committed", last), h.waitTarget(ctx, seamsAppDatabase, fmt.Sprintf("%s%d", seamsLiveMarker, last))
	})
	h.approve(ctx, name)
	if !h.check("follow/endpos", "reconcile: cutover saves E from source_head", func() (string, error) {
		return h.reconcileUntil(ctx, name, 2*time.Minute, func(o seamsObjects) bool { return nativeControlEnd(o.m) != "" })
	}) {
		h.saveEndFallback(ctx, name)
	}
	h.check("follow/drain", "reconcile: control exec sets endpos, the worker drains and exits 0", func() (string, error) {
		return h.reconcileUntil(ctx, name, 4*time.Minute, func(o seamsObjects) bool { return o.worker != nil && nativeEvidenceWorkerComplete(o.worker) })
	})
	h.completionReads(ctx, name, run)
	h.check("follow/finish", "reconcile: finishNativeFollow to Completed", func() (string, error) {
		return h.reconcileUntil(ctx, name, 6*time.Minute, func(o seamsObjects) bool {
			c := apimeta.FindStatusCondition(o.m.Status.Conditions, v1beta1.ConditionCutoverComplete)
			return o.m.Status.Phase == v1beta1.PhaseCompleted && c != nil && c.Reason == "NativeDrainConfirmed"
		})
	})
	h.expectCleaned(ctx, run, seamsAppDatabase, "follow")
	h.deleteMigration(ctx, name, "follow")
}

func (h *seamsHarness) waitTarget(ctx context.Context, db, note string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		out, err := h.psql(ctx, h.target, db, "SELECT count(*) FROM orders WHERE note = '"+note+"';")
		if err == nil && out == "1" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("target never applied %s: %v %s", note, err, out)
		case <-time.After(time.Second):
		}
	}
}

func (h *seamsHarness) approve(ctx context.Context, name string) {
	m := &v1beta1.Migration{}
	if err := h.cluster.store.Get(ctx, client.ObjectKey{Namespace: seamsNamespace, Name: name}, m); err != nil {
		h.t.Fatal(err)
	}
	patch := client.MergeFrom(m.DeepCopy())
	// The API server bumps the generation on a spec change; the fake store does not.
	m.Spec.Cutover.Approved, m.Generation = true, m.Generation+1
	if err := h.cluster.store.Patch(ctx, m, patch); err != nil {
		h.t.Fatal(err)
	}
}

// saveEndFallback records E as persistNativeEnd would have, from the source's current WAL head,
// so the later steps still reach their own programs when source_head is refused.
func (h *seamsHarness) saveEndFallback(ctx context.Context, name string) {
	end := h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT pg_current_wal_lsn();")
	m := &v1beta1.Migration{}
	if err := h.cluster.store.Get(ctx, client.ObjectKey{Namespace: seamsNamespace, Name: name}, m); err != nil {
		h.t.Fatal(err)
	}
	base := m.DeepCopy()
	if m.Status.Replication == nil {
		m.Status.Replication = &v1beta1.ReplicationStatus{}
	}
	m.Status.Replication.Endpos, m.Status.Phase = end, v1beta1.PhaseCuttingOver
	if err := h.cluster.store.Status().Patch(ctx, m, client.MergeFrom(base)); err != nil {
		h.t.Fatal(err)
	}
	h.fallback("E " + end + " saved from pg_current_wal_lsn() on the source")
}

// completionReads runs each post-drain helper on its own, in the order finishNativeFollow uses them.
func (h *seamsHarness) completionReads(ctx context.Context, name, run string) {
	var clone *native.Report
	var ack []byte
	h.check("follow/clone-report", "worker log: clone report", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		clone, err = h.r.readNativeCloneReport(ctx, o.m, o.worker, o.pvc)
		if err != nil {
			return h.cluster.podLogs(ctx, o.m.Status.JobName), err
		}
		return "phase " + clone.Phase, nil
	})
	h.check("follow/endpos-ack", "endpos helper Job: rpgmigrate stream sentinel set endpos", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		ack, err = h.r.readNativeEndposAck(ctx, o.m, o.worker, o.pvc)
		return strings.TrimSpace(string(ack)), err
	})
	h.check("follow/completed-evidence", "evidence helper Job: completed evidence after drain", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		got, err := h.r.readNativeFollowCompletion(ctx, o.m, o.worker, o.pvc, clone, ack)
		if err != nil {
			return "", err
		}
		e := got.Checkpoint().Evidence()
		if !sameLSNText(e.DrainEndLSN, nativeControlEnd(o.m)) || !e.BarrierConfirmed {
			return "", fmt.Errorf("checkpoint drain end %s, barrier %v, saved E %s", e.DrainEndLSN, e.BarrierConfirmed, nativeControlEnd(o.m))
		}
		return fmt.Sprintf("drained to %s by %s, resources %v", e.DrainEndLSN, e.DrainKind, got.OwnedState().Resources()), nil
	})
	h.check("follow/verification", "compare helper Jobs: schema and data", func() (string, error) { return h.verify(ctx, name) })
	h.check("follow/inventory", "inventory helper Job: owned inventory", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		owned, err := h.r.readNativeOwnedInventory(ctx, o.m, o.worker, o.pvc)
		if err != nil {
			return "", err
		}
		return h.expectOwned(ctx, owned, run, seamsAppDatabase)
	})
	h.check("follow/cleanup", "inventory, cleanup and inventory helper Jobs", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		done, err := h.r.ensureNativeCleanup(ctx, o.m, o.worker, o.pvc)
		if err == nil && !done {
			err = errors.New("cleanup not done")
		}
		return "", err
	})
	h.check("follow/cleaned-evidence", "cleaned completion helper Job", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		got, err := h.r.readNativeCleanedCompletedEvidence(ctx, o.m, o.worker, o.pvc)
		if err != nil {
			return "", err
		}
		return "drained to " + got.Checkpoint().Evidence().DrainEndLSN, nil
	})
}

func sameLSNText(a, b string) bool {
	return a != "" && strings.EqualFold(strings.TrimLeft(a, "0"), strings.TrimLeft(b, "0"))
}

// verify drives ensureNativeVerification until both captures are recorded, as successive passes would.
func (h *seamsHarness) verify(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		done, err := h.r.ensureNativeVerification(ctx, o.m, o.worker, o.pvc)
		if err != nil {
			return "", err
		}
		if done {
			return h.verdictOf(ctx, o.m)
		}
		select {
		case <-ctx.Done():
			return "", errors.New("comparison never finished")
		case <-time.After(2 * time.Second):
		}
	}
}

// verdictOf requires both compares to match and the data compare's final progress to cover every planned byte.
func (h *seamsHarness) verdictOf(ctx context.Context, m *v1beta1.Migration) (string, error) {
	var parts, failed []string
	for _, result := range m.Status.Verification {
		if result.Passed {
			parts = append(parts, result.Check+" matched")
			continue
		}
		failed = append(failed, result.Check)
		parts = append(parts, result.Check+" differs: "+strings.TrimSpace(h.cluster.podLogs(ctx, compareJobName(m, result.Check))))
		if result.Check == compareSchema {
			parts = append(parts, "dump difference: "+h.schemaDiff(ctx, m))
		}
	}
	data := slices.IndexFunc(m.Status.VerificationProgress, func(p v1beta1.VerificationProgress) bool { return p.Kind == compareData })
	if data < 0 {
		return strings.Join(parts, "; "), errors.New("no data compare progress recorded")
	}
	p := m.Status.VerificationProgress[data]
	if !p.Final || p.PlannedBytes == nil || p.ComparedBytes == nil || p.PlannedBytes.Cmp(*p.ComparedBytes) != 0 || p.FailedTables != 0 || p.DifferingTables != 0 {
		failed = append(failed, "data progress")
	}
	parts = append(parts, fmt.Sprintf("final data progress %d of %d tables, %v of %v bytes, final %v", p.ComparedTables, p.PlannedTables, p.ComparedBytes, p.PlannedBytes, p.Final))
	if len(failed) > 0 {
		return strings.Join(parts, "; "), fmt.Errorf("%s did not match", strings.Join(failed, " and "))
	}
	return strings.Join(parts, "; "), nil
}

// schemaDiff names the lines where the two schema dumps differ, because the engine reports only
// {"matched":false}. It dumps as the engine does: the target major's pg_dump, as the app role, with
// the flags of rpgmigrate src/schema.rs comparison_dump, the bookkeeping schema left out.
func (h *seamsHarness) schemaDiff(ctx context.Context, m *v1beta1.Migration) string {
	pass := "*:*:*:" + seamsAppRole + ":" + h.password + "\n"
	if _, _, err := seamsPodman(ctx, []byte(pass), seamsExecWord, "-i", h.target, "sh", "-c", "umask 077; cat > /tmp/seams-pgpass"); err != nil {
		return "unavailable: " + err.Error()
	}
	dump := func(host, database string, extra ...string) []string {
		args := append([]string{seamsExecWord, "--env", "PGPASSFILE=/tmp/seams-pgpass", h.target, "pg_dump", "-h", host, "-U", seamsAppRole, "-d", database,
			"--format=plain", "--schema-only", "--quote-all-identifiers", "--no-publications", "--no-subscriptions", "--restrict-key=RpgmigrateSchemaComparison"}, extra...)
		out, _, err := seamsPodman(ctx, nil, args...)
		if err != nil {
			return []string{"dump failed: " + err.Error()}
		}
		// The engine skips the header through the two version provenance lines.
		lines := strings.Split(string(out), "\n")
		return lines[min(len(lines), 8):]
	}
	source := dump(h.source+seamsDNSDomain, seamsAppDatabase)
	target := dump("/tmp", m.Spec.Target.Database, "--exclude-schema=_rpgmigrate_"+runIDFor(m))
	var diff []string
	for _, line := range source {
		if !slices.Contains(target, line) && strings.TrimSpace(line) != "" {
			diff = append(diff, "source only: "+line)
		}
	}
	for _, line := range target {
		if !slices.Contains(source, line) && strings.TrimSpace(line) != "" {
			diff = append(diff, "target only: "+line)
		}
	}
	// The engine compares bytes, so equal line sets can still differ in order.
	if len(diff) == 0 {
		for i := range min(len(source), len(target)) {
			if source[i] != target[i] {
				end := min(i+6, len(source), len(target))
				diff = append(diff, fmt.Sprintf("order differs at line %d: source %q, target %q", i+9, strings.Join(source[i:end], "\\n"), strings.Join(target[i:end], "\\n")))
				break
			}
		}
		if len(source) != len(target) {
			diff = append(diff, fmt.Sprintf("source has %d lines, target %d", len(source), len(target)))
		}
	}
	if len(diff) == 0 {
		return "none: the dumps are identical after the header"
	}
	return h.cluster.scrub(strings.Join(diff[:min(len(diff), 12)], " | "))
}

// expectOwned requires exactly this run's slot and publication on the source, the retained workdir,
// and the run's bookkeeping schema on the target that the inventory read.
func (h *seamsHarness) expectOwned(ctx context.Context, owned native.AdmittedOwnedState, run, database string) (string, error) {
	kinds := map[string]string{}
	for _, r := range owned.Resources() {
		kinds[r.Kind] = r.Name
	}
	slot := h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT string_agg(slot_name, ',') FROM pg_replication_slots;")
	pub := h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT string_agg(pubname, ',') FROM pg_publication;")
	schema := h.mustSQL(ctx, h.target, database, "SELECT count(*) FROM pg_namespace WHERE nspname = '_rpgmigrate_"+run+"';")
	if kinds[seamsSlotKind] == "" || kinds[seamsSlotKind] != slot || !strings.Contains(slot, run) || kinds[seamsPublicationKind] == "" || kinds[seamsPublicationKind] != pub ||
		kinds[workdirVolume] != nativeCleanupTestWorkdir || len(kinds) != 3 || schema != "1" {
		return "", fmt.Errorf("inventory %v does not match the source slots %q, publications %q and target schema count %s", kinds, slot, pub, schema)
	}
	return fmt.Sprintf("slot %s, publication %s, workdir retained, target _rpgmigrate_%s present", slot, pub, run), nil
}

// expectCleaned requires the source to keep no slot or publication and the target to keep _rpgmigrate_<run>.
func (h *seamsHarness) expectCleaned(ctx context.Context, run, database, prefix string) {
	h.check(prefix+"/databases-after", "SQL on both databases", func() (string, error) {
		slots := h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '%"+run+"%';")
		pubs := h.mustSQL(ctx, h.source, seamsAppDatabase, "SELECT count(*) FROM pg_publication WHERE pubname LIKE '%"+run+"%';")
		schema := h.mustSQL(ctx, h.target, database, "SELECT count(*) FROM pg_namespace WHERE nspname = '_rpgmigrate_"+run+"';")
		detail := fmt.Sprintf("source slots %s, publications %s; target _rpgmigrate_%s present %s", slots, pubs, run, schema)
		if slots != "0" || pubs != "0" || schema != "1" {
			return detail, errors.New("owned state left behind or bookkeeping lost")
		}
		return detail, nil
	})
}

func (h *seamsHarness) deleteMigration(ctx context.Context, name, prefix string) {
	h.check(prefix+"/delete", "reconcile: disposition, deletion inventory and cleanup, finalizer", func() (string, error) {
		m := &v1beta1.Migration{}
		if err := h.cluster.store.Get(ctx, client.ObjectKey{Namespace: seamsNamespace, Name: name}, m); err != nil {
			return "", err
		}
		if err := h.cluster.api.Delete(ctx, m); err != nil {
			return "", err
		}
		return h.reconcileUntil(ctx, name, 5*time.Minute, func(o seamsObjects) bool { return o.m == nil })
	})
}

// runRetry kills the worker mid-stream, then reads the retry gate and the recovery path.
func (h *seamsHarness) runRetry() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	const name = "seams-retry"
	m := h.newMigration(name, seamsRetryDB)
	m.Spec.BackoffLimit = 2
	run := runIDFor(m)
	writer := h.startWriter(seamsLiveMarker + "retry-")
	defer func() { _, _ = writer.halt() }()
	if err := h.cluster.api.Create(ctx, m); err != nil {
		h.t.Fatal(err)
	}
	if !h.check("retry/start", "version and preflight Jobs, work claim, worker Job", func() (string, error) {
		return h.reconcileUntil(ctx, name, 6*time.Minute, func(o seamsObjects) bool { return o.m != nil && seamsWorkerRunning(o) })
	}) {
		return
	}
	h.check("retry/streaming", "control exec: rpgmigrate list progress", func() (string, error) { return h.probeStreaming(ctx, name) })
	h.check("retry/kill", "SIGKILL to the worker container mid-stream", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		if err := h.cluster.kill(ctx, o.m.Status.JobName, "KILL"); err != nil {
			return "", err
		}
		wait, stop := context.WithTimeout(ctx, time.Minute)
		defer stop()
		_, err = h.cluster.waitJob(wait, o.m.Status.JobName, nativeRecoveryWorkerFailed)
		return "worker Job failed", err
	})
	h.check("retry/failure-log", "failed worker log read", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		pod, err := h.r.nativeQuiescentWorker(ctx, o.m, o.worker, o.pvc)
		if err != nil {
			return "", err
		}
		// The binding reconcileNativeFailure builds for the same read.
		binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: workerContainer, ContainerID: pod.Status.ContainerStatuses[0].ContainerID,
			ImageID: h.images.runner, TemplateSHA256: podexec.NativeTemplateSHA256(o.worker.Spec.Template), ExecutionSHA256: podexec.NativeExecutionSHA256(o.worker.Spec.Template)}
		out, err := h.r.NativeJobs.(nativeFailureReader).JobFailureNative(ctx, seamsNamespace, o.worker.UID, binding)
		if err != nil || !out.Successful() {
			return "", fmt.Errorf("failure output unavailable: %v", err)
		}
		return fmt.Sprintf("%d log bytes", len(out.Payload())), nil
	})
	h.check("retry/gate", "retry-gate helper Job: target retry readiness", func() (string, error) {
		o, err := h.objects(ctx, name)
		if err != nil {
			return "", err
		}
		var readiness *native.RetryReadiness
		err = h.r.runNativeRecoveryRead(ctx, o.m, o.worker, o.pvc, nil, native.JobRoleRetryGate, func(ctx context.Context, w native.ExecutionData) error {
			var readErr error
			readiness, readErr = h.r.NativeEvidence.ReadRetryReadiness(ctx, w, nativeRetryInputs(o.m))
			return readErr
		})
		if err != nil {
			return "", err
		}
		observation, known := readiness.Observation()
		if !known {
			return "", errors.New("gate yielded no observation")
		}
		return fmt.Sprintf("observed %d sessions, synchronous_commit %s", observation.Total, observation.TargetSynchronousCommit), nil
	})
	h.check("retry/attempt-2", "reconcile: retry gate admits attempt 2", func() (string, error) {
		return h.reconcileUntil(ctx, name, 3*time.Minute, func(o seamsObjects) bool { return o.m.Status.Attempts == 2 && seamsWorkerRunning(o) })
	})
	last, _ := writer.halt()
	h.check("retry/streaming-2", "control exec: rpgmigrate list progress on attempt 2", func() (string, error) {
		detail, err := h.probeStreaming(ctx, name)
		if err != nil {
			return detail, err
		}
		return detail, h.waitTarget(ctx, seamsRetryDB, fmt.Sprintf("%sretry-%d", seamsLiveMarker, last))
	})
	h.check("retry/deletion-inventory", "inventory helper Job after the worker stops", func() (string, error) { return h.deleteRetry(ctx, name, run) })
	h.expectCleaned(ctx, run, seamsRetryDB, "retry")
}

// deleteRetry deletes while streaming: the disposition stops the worker with TERM, reads the
// deletion inventory, runs cleanup and releases the finalizer.
func (h *seamsHarness) deleteRetry(ctx context.Context, name, run string) (string, error) {
	m := &v1beta1.Migration{}
	if err := h.cluster.store.Get(ctx, client.ObjectKey{Namespace: seamsNamespace, Name: name}, m); err != nil {
		return "", err
	}
	if err := h.cluster.api.Delete(ctx, m); err != nil {
		return "", err
	}
	detail, err := h.reconcileUntil(ctx, name, 6*time.Minute, func(o seamsObjects) bool { return o.m == nil })
	inventories := 0
	for _, e := range h.cluster.execsSince(0) {
		if strings.Contains(e.Pod, nativeInventoryJobPrefix) && e.Exit == 0 && strings.Contains(e.Stdout, run) {
			inventories++
		}
	}
	if err == nil && inventories == 0 {
		err = errors.New("deletion finished without an inventory that named this run")
	}
	return fmt.Sprintf("%s, %d inventory reads named the run", detail, inventories), err
}

type seamsReport struct {
	Pair     string         `json:"pair"`
	Runner   string         `json:"runner_image"`
	Source   string         `json:"source_image"`
	Target   string         `json:"target_image"`
	Passed   int            `json:"passed"`
	Failed   int            `json:"failed"`
	Verdicts []seamsVerdict `json:"verdicts"`
}

// report writes the verdict table and fails the test when any step failed.
func (h *seamsHarness) report() {
	failed := 0
	var table bytes.Buffer
	for _, v := range h.verdicts {
		mark := "pass"
		if !v.Passed {
			mark = "FAIL"
			failed++
		}
		fmt.Fprintf(&table, "%-4s %-28s %s\n", mark, v.Step, v.Program)
	}
	h.t.Logf("native seams %s: %d of %d steps passed\n%s", h.pair, len(h.verdicts)-failed, len(h.verdicts), table.String())
	if dir := os.Getenv(seamsEvidenceEnv); dir != "" {
		raw, err := json.MarshalIndent(seamsReport{Pair: h.pair, Runner: h.images.runner, Source: h.images.source, Target: h.images.target,
			Passed: len(h.verdicts) - failed, Failed: failed, Verdicts: h.verdicts}, "", " ")
		if err != nil {
			h.t.Fatal(err)
		}
		path := filepath.Join(dir, "native-seams-"+strings.ReplaceAll(h.pair, ":", "-")+".json")
		if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
			h.t.Fatal(err)
		}
	}
	if failed > 0 || len(h.verdicts) == 0 {
		h.t.Fatalf("native seams %s: %d steps failed", h.pair, failed)
	}
}
