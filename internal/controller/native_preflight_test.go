package controller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

const (
	probeFollowCase                     = "follow"
	probeJobNameCase                    = "job name"
	probeBatchAPI                       = "batch/v1"
	probeOperationKey                   = "operation_id"
	probeFiltersCase                    = "filters"
	probeAllDatabasesCase               = "all databases"
	probeMigrationReplacement           = "11234567-89ab-cdef-0123-456789abcdef"
	probeExitCase                       = "exit"
	probeSignalCase                     = "signal"
	probeDiscoveryCase                  = "discovery"
	probeTerminalCase                   = "terminal"
	probeImageCase                      = "image"
	probeSuspendCase                    = "suspend"
	probeCanary                         = "postgresql://private-user:private-password@private-host/private-db"
	probeVersion                        = "rpgmigrate-version-"
	probeDB                             = "rpgmigrate-preflight-"
	probeReplacement          types.UID = "replacement-execution"
	probeCaps                           = `"capabilities":["clone.single_db","progress.v1","verify.sealed_files.v1"]`
	probeVersionLine                    = `{"protocol_version":1,"engine_version":"rpgmigrate-test","capabilities":["clone.single_db","progress.v1","verify.sealed_files.v1"]}` + "\n"
)

type preflightFixture struct {
	t                *testing.T
	r                *MigrationReconciler
	m                *v1beta1.Migration
	store            client.WithWatch
	creates, deletes int
	logs             atomic.Int32
	version          []byte
	createMode       string
	pending          bool
	keepPods         bool
	deleteError      bool
	cleanupListError bool
	workDeadlines    []time.Time
	cleanupDeadlines []time.Time
	onCreate         func(*batchv1.Job)
	onLog            func(*batchv1.Job)
	onDelete         func(*batchv1.Job)
	onRead           func(client.Object)
	output           func(*batchv1.Job) []byte
}

func newPreflightFixture(t *testing.T) *preflightFixture {
	t.Helper()
	m, _ := nativeWorkerJobInputs()
	m.Generation = 1
	narrowNativeWorker(m)
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	f := &preflightFixture{t: t, m: m, version: []byte(probeVersionLine)}
	f.store = clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(m).WithStatusSubresource(&v1beta1.Migration{}).Build()
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(m), m) != nil {
		t.Fatal("fixture Migration absent")
	}
	f.r = &MigrationReconciler{Scheme: scheme, RPGMigrateRunnerImage: nativeEvidenceTestImage}
	f.r.APIReader = preflightFixtureReader{f}
	f.r.Sentinel, f.r.Progress, f.r.Logs = &fakeSentinel{}, &fakeProgress{}, &fakeLogs{}
	f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Fatal("preflight used cached GET")
			return errBoom
		},
		Create: f.createJob,
		Delete: f.deleteJob,
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			t.Fatal("preflight changed metadata")
			return errBoom
		},
		SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
			t.Fatal("preflight changed status")
			return errBoom
		},
	})
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	checked, err := podexec.New(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	f.r.NativePreflight = checked
	t.Cleanup(func() {
		if f.r.Sentinel.(*fakeSentinel).callCount() != 0 || f.r.Progress.(*fakeProgress).counts() != 0 || f.r.Logs.(*fakeLogs).calls != 0 {
			t.Error("preflight reached legacy SQL/logs/progress")
		}
	})
	return f
}
func defaultProbeJob(job *batchv1.Job) {
	job.Spec.CompletionMode = ptr.To(batchv1.NonIndexedCompletion)
	job.Spec.Suspend = ptr.To(false)
	job.Spec.ManualSelector = ptr.To(false)
	job.Spec.PodReplacementPolicy = ptr.To(batchv1.TerminatingOrFailed)
	p := &job.Spec.Template.Spec
	p.TerminationGracePeriodSeconds = ptr.To(int64(30))
	p.SchedulerName = corev1.DefaultSchedulerName
	p.HostUsers = ptr.To(true)
	c := &p.Containers[0]
	c.ImagePullPolicy = corev1.PullIfNotPresent
	c.TerminationMessagePath = corev1.TerminationMessagePathDefault
	c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	job.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{batchv1.ControllerUidLabel: string(job.UID)}}
	maps.Copy(job.Spec.Template.Labels, map[string]string{batchv1.ControllerUidLabel: string(job.UID), nativeWorkerLegacyControllerLabel: string(job.UID), batchv1.JobNameLabel: job.Name, nativeWorkerLegacyJobNameLabel: job.Name})
}
func probePod(job *batchv1.Job) *corev1.Pod {
	p := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-pod", Namespace: job.Namespace, UID: types.UID("pod-" + string(job.UID)), Labels: job.Spec.Template.Labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: probeBatchAPI, Kind: lifecycleJobKind, Name: job.Name, UID: job.UID, Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}}}, Spec: *job.Spec.Template.Spec.DeepCopy()}
	p.Spec.NodeName = "fixture-node"
	p.Spec.ServiceAccountName = nativePreflightDefaultSA
	p.Spec.DeprecatedServiceAccount = nativePreflightDefaultSA
	p.Spec.EnableServiceLinks = ptr.To(true)
	p.Spec.Priority = ptr.To(int32(0))
	p.Spec.PreemptionPolicy = ptr.To(corev1.PreemptLowerPriority)
	for _, key := range []string{"node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"} {
		p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(300))})
	}
	id := "containerd://" + string(p.UID)
	p.Status = corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer, ContainerID: id, Image: "display-alias", ImageID: job.Spec.Template.Spec.Containers[0].Image, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: id, ExitCode: 0}}}}}
	return p
}

type preflightFixtureReader struct{ f *preflightFixture }

func (r preflightFixtureReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *v1beta1.Migration, *batchv1.Job, *corev1.Pod:
	default:
		r.f.t.Fatal("preflight read Secret/PVC/other resource")
	}
	if r.f.onRead != nil {
		r.f.onRead(obj)
	}
	return r.f.store.Get(ctx, key, obj, opts...)
}
func (r preflightFixtureReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if r.f.deletes > 0 && r.f.cleanupListError {
		return errors.New(probeCanary)
	}
	if _, ok := list.(*corev1.PodList); !ok {
		r.f.t.Fatal("preflight listed non-Pods")
	}
	o := &client.ListOptions{}
	o.ApplyOptions(opts)
	if o.Namespace != r.f.m.Namespace || o.LabelSelector != nil {
		r.f.t.Fatal("preflight trusted labels or wrong namespace")
	}
	err := r.f.store.List(ctx, list, opts...)
	list.(*corev1.PodList).ResourceVersion = "snapshot"
	return err
}
func (f *preflightFixture) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	parts := strings.Split(r.URL.Path, "/")
	name := parts[len(parts)-1]
	if strings.HasSuffix(r.URL.Path, "/log") {
		f.logs.Add(1)
		if len(r.URL.Query()) != 1 || r.URL.Query().Get("container") != workerContainer {
			f.t.Error("checked output used partial/stream selector")
		}
		pod := &corev1.Pod{}
		if err := f.store.Get(r.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: parts[len(parts)-2]}, pod); err != nil {
			http.NotFound(w, r)
			return
		}
		job := &batchv1.Job{}
		if err := f.store.Get(r.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: metav1.GetControllerOf(pod).Name}, job); err != nil {
			http.NotFound(w, r)
			return
		}
		if f.onLog != nil {
			f.onLog(job)
		}
		payload := f.version
		if strings.HasPrefix(job.Name, probeDB) {
			payload = probeResult(job, followEnabled(f.m))
		}
		if f.output != nil {
			payload = f.output(job)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(payload)
		return
	}
	var obj client.Object
	switch {
	case strings.HasSuffix(r.URL.Path, "/pods"):
		pods := &corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}}
		if err := f.store.List(r.Context(), pods, client.InNamespace(f.m.Namespace)); err != nil {
			f.t.Error(err)
		}
		pods.ResourceVersion = "snapshot"
		_ = json.NewEncoder(w).Encode(pods)
		return
	case strings.Contains(r.URL.Path, "/pods/"):
		obj = &corev1.Pod{}
	case strings.Contains(r.URL.Path, "/jobs/"):
		obj = &batchv1.Job{}
	default:
		f.t.Error("unapproved checked request")
		http.NotFound(w, r)
		return
	}
	if err := f.store.Get(r.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: name}, obj); err != nil {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(metav1.Status{Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: 404})
		return
	}
	_ = json.NewEncoder(w).Encode(obj)
}
func probeResult(job *batchv1.Job, follow bool) []byte {
	checks := []string{"connections", "identity", "source_primary", "target_writable", "versions", "tools", "temporary_privilege", "extensions", "clone_rights", "rls"}
	if follow {
		checks = append(checks, "wal", "publication", "slots", "replication", "replica_role", "unlogged", "replica_identity")
	}
	sha := native.EvidenceProgramSHA256()
	b, err := json.Marshal(map[string]any{probeTestVersionCommand: 1, "kind": "native_preflight", probeOperationKey: strings.TrimPrefix(job.Name, probeDB), "program_sha256": hex.EncodeToString(sha[:]), "source_major": 16, "target_major": 18, "pg_dump_major": 18, "pg_restore_major": 18, "checks": checks, "source_closed": true, "target_closed": true, "materialization_closed": true, "diagnostics_clean": true})
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}
func (f *preflightFixture) run() (*native.Version, bool, string, error) {
	return f.r.ensureNativePreflight(context.Background(), f.m)
}
func (f *preflightFixture) assertNoAuthority(v *native.Version, p bool, failure string, err error) {
	f.t.Helper()
	if v != nil || p || failure == "" && err == nil || strings.Contains(failure, probeCanary) || err != nil && strings.Contains(err.Error(), probeCanary) {
		f.t.Fatal("refusal granted authority or leaked diagnostics")
	}
	stored := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), stored) != nil || !apiequality.Semantic.DeepEqual(stored.Status, f.m.Status) {
		f.t.Fatal("preflight mutated Migration status")
	}
}

func TestNativePreflightDiscovery(t *testing.T) {
	t.Run("required capability omissions", testProbeCapabilityOmissions)
	t.Run("missing reader", func(t *testing.T) {
		r := &MigrationReconciler{}
		v, p, f, e := r.ensureNativePreflight(context.Background(), nil)
		if v != nil || p || f != "" || e == nil {
			t.Fatal("missing reader passed")
		}
	})
	for _, follow := range []bool{false, true} {
		t.Run(strconv.FormatBool(follow), func(t *testing.T) {
			f := newPreflightFixture(t)
			if follow {
				f.m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
				f.version = []byte(strings.Replace(probeVersionLine, probeCaps, `"capabilities":["clone.single_db","progress.v1","verify.sealed_files.v1","cdc.pgoutput","cutover.v1","cleanup.v1"]`, 1))
				if err := f.store.Update(context.Background(), f.m); err != nil {
					t.Fatal(err)
				}
			}
			v, p, failure, err := f.run()
			if err != nil || failure != "" || !p || v == nil || f.creates != 2 || f.deletes != 2 || f.logs.Load() != 2 {
				t.Fatal("checked discovery/preflight did not pass with exact cleanup", failure, err, f.creates, f.deletes, f.logs.Load())
			}
		})
	}
	cases := map[string]string{
		"current empty caps": strings.Replace(probeVersionLine, probeCaps, `"capabilities":[]`, 1),
		"missing clone":      strings.Replace(probeVersionLine, `"clone.single_db",`, "", 1), "missing progress": strings.Replace(probeVersionLine, `,"progress.v1"`, "", 1),
		"unknown caps": strings.Replace(probeVersionLine, probeCaps, `"capabilities":[string(testUnrecognizedEngine)]`, 1), "duplicate caps": strings.Replace(probeVersionLine, probeCaps, `"capabilities":["progress.v1","progress.v1"]`, 1),
		"null caps": strings.Replace(probeVersionLine, probeCaps, `"capabilities":null`, 1), "missing caps": strings.Replace(probeVersionLine, ","+probeCaps, "", 1),
		"wrong protocol": strings.Replace(probeVersionLine, `"protocol_version":1`, `"protocol_version":2`, 1), "duplicate key": strings.Replace(probeVersionLine, `"protocol_version":1`, `"protocol_version":1,"protocol_version":1`, 1),
		"unknown identity": strings.Replace(probeVersionLine, `"engine_version"`, `"work":{},"engine_version"`, 1), "invalid UTF8": strings.Replace(probeVersionLine, "rpgmigrate-test", string([]byte{0xff}), 1),
		"leading noise": "noise\n" + probeVersionLine, "trailing noise": probeVersionLine + "noise\n", "missing LF": strings.TrimSuffix(probeVersionLine, "\n"), "truncated": probeVersionLine[:30], "two docs": probeVersionLine + probeVersionLine, "multiline": strings.Replace(probeVersionLine, ",", ",\n", 1),
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			f := newPreflightFixture(t)
			f.version = []byte(line)
			v, p, s, e := f.run()
			f.assertNoAuthority(v, p, s, e)
			if e != nil || s == "" {
				t.Fatal("deterministic discovery refusal was misclassified")
			}
			if f.creates != 1 || f.deletes != 1 || f.logs.Load() != 1 {
				t.Fatal("bad capabilities reached DB materialization")
			}
		})
	}
	for _, name := range []string{probeFiltersCase, "superuser", probeAllDatabasesCase, "native image absent", "tag image"} {
		t.Run(name, func(t *testing.T) {
			f := newPreflightFixture(t)
			switch name {
			case probeFiltersCase:
				f.m.Spec.Clone.Filters = &v1beta1.Filters{}
			case "superuser":
				f.m.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "primary-admin"}
			case probeAllDatabasesCase:
				f.m.Spec.Clone.AllDatabases = true
			case "native image absent":
				f.r.RPGMigrateRunnerImage = ""
				f.r.RunnerImage = nativeEvidenceTestImage
			case "tag image":
				f.r.RPGMigrateRunnerImage = "registry.example/native:latest"
			}
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			v, p, s, e := f.run()
			f.assertNoAuthority(v, p, s, e)
			if f.creates != 0 {
				t.Fatal("pure refusal created or projected a Job")
			}
		})
	}
}

func TestNativePreflightCreateUncertainty(t *testing.T) {
	for _, mode := range []string{"AlreadyExists", string(testUnrecognizedEngine), "empty UID", "lost acknowledgment"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreflightFixture(t)
			f.createMode = mode
			v, p, s, e := f.run()
			f.assertNoAuthority(v, p, s, e)
			if e == nil || s != "" {
				t.Fatal("uncertain Create was misclassified")
			}
			if f.creates != 1 || f.deletes != 0 || f.logs.Load() != 0 {
				t.Fatal("uncertain Create read/adopted/deleted/retried")
			}
		})
	}
	t.Run("known UID changed response", func(t *testing.T) {
		f := newPreflightFixture(t)
		f.onCreate = func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].Args = append(j.Spec.Template.Spec.Containers[0].Args, "--other")
		}
		v, p, s, e := f.run()
		f.assertNoAuthority(v, p, s, e)
		if f.creates != 1 || f.deletes != 1 || f.logs.Load() != 0 {
			t.Fatal("noncanonical known execution was consumed or not cleaned")
		}
	})
	t.Run("new invocation new operation", func(t *testing.T) {
		f := newPreflightFixture(t)
		names := []string{}
		f.onCreate = func(j *batchv1.Job) { names = append(names, j.Name) }
		for range 2 {
			v, p, s, e := f.run()
			if v == nil || !p || s != "" || e != nil {
				t.Fatal("fresh invocation failed", s, e)
			}
		}
		if len(names) != 4 || names[0] == names[2] || names[1] == names[3] {
			t.Fatal("operation or Job reused across invocations")
		}
	})
}
func TestNativePreflightReadmission(t *testing.T) {
	testProbeMigrationChanges(t)
	testProbePodChanges(t)
	testProbeCaptureChanges(t)
	testProbeResultChanges(t)
}

func testProbeMigrationChanges(t *testing.T) {
	t.Helper()

	for _, field := range []string{workerUIDCase, lifecycleGenerationCase, workerSpecCase, lifecycleAttemptCase, probeJobNameCase, probeSuspendCase, probeTerminalCase, "engine", probeImageCase} {
		t.Run(field, func(t *testing.T) {
			f := newPreflightFixture(t)
			f.onLog = func(*batchv1.Job) {
				m := &v1beta1.Migration{}
				if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), m); err != nil {
					t.Fatal(err)
				}
				switch field {
				case workerUIDCase:
					m.UID = probeMigrationReplacement
				case lifecycleGenerationCase:
					m.Generation++
				case workerSpecCase:
					m.Spec.Runner.NodeSelector = map[string]string{"changed": "enabled"}
				case lifecycleAttemptCase:
					m.Status.Attempts = 1
				case probeJobNameCase:
					m.Status.JobName = "foreign-worker"
				case probeSuspendCase:
					m.Spec.Suspend = true
				case probeTerminalCase:
					m.Status.Phase = v1beta1.PhaseCompleted
				case "engine":
					m.Spec.Engine = v1beta1.MigrationEnginePGCopyDB
				case probeImageCase:
					f.r.RPGMigrateRunnerImage = strings.Replace(nativeEvidenceTestImage, "aaaa", "bbbb", 1)
				}
				var err error
				if field == lifecycleAttemptCase || field == probeJobNameCase || field == probeTerminalCase {
					err = f.store.Status().Update(context.Background(), m)
				} else {
					err = f.store.Update(context.Background(), m)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			v, p, s, e := f.run()
			if v != nil || p || e == nil && s == "" || f.creates != 1 || f.deletes != 1 {
				t.Fatal("changed Migration/image was admitted or projected DB credentials")
			}
		})
	}
}

func testProbePodChanges(t *testing.T) {
	t.Helper()

	mutations := map[string]func(*corev1.Pod){
		"node name": func(p *corev1.Pod) { p.Spec.NodeName = "invalid/name" }, "service account": func(p *corev1.Pod) { p.Spec.ServiceAccountName = "privileged" }, "deprecated account": func(p *corev1.Pod) { p.Spec.DeprecatedServiceAccount = "privileged" },
		"service links": func(p *corev1.Pod) { p.Spec.EnableServiceLinks = ptr.To(false) }, "priority": func(p *corev1.Pod) { p.Spec.Priority = ptr.To(int32(1)) }, "preemption": func(p *corev1.Pod) { p.Spec.PreemptionPolicy = ptr.To(corev1.PreemptNever) }, "runtime class": func(p *corev1.Pod) { p.Spec.RuntimeClassName = ptr.To("custom") }, "host users": func(p *corev1.Pod) { p.Spec.HostUsers = ptr.To(false) },
		"toleration seconds": func(p *corev1.Pod) { p.Spec.Tolerations[0].TolerationSeconds = ptr.To(int64(301)) }, "toleration effect": func(p *corev1.Pod) { p.Spec.Tolerations[0].Effect = corev1.TaintEffectNoSchedule }, "duplicate default": func(p *corev1.Pod) { p.Spec.Tolerations = append(p.Spec.Tolerations, p.Spec.Tolerations[0]) },
		"env": func(p *corev1.Pod) {
			p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "LD_PRELOAD", Value: "/tmp/foreign"})
		}, "container image": func(p *corev1.Pod) { p.Spec.Containers[0].Image = testRunnerImage }, "raw imageID": func(p *corev1.Pod) { p.Status.ContainerStatuses[0].ImageID = testRunnerImage }, "host network": func(p *corev1.Pod) { p.Spec.HostNetwork = true },
		"restart": func(p *corev1.Pod) { p.Status.ContainerStatuses[0].RestartCount = 1 }, "prior termination": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].LastTerminationState = p.Status.ContainerStatuses[0].State
		}, probeExitCase: func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1 }, probeSignalCase: func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.Signal = 9 },
	}
	for name, mutate := range mutations {
		t.Run("pod/"+name, func(t *testing.T) {
			f := newPreflightFixture(t)
			done := false
			f.onRead = func(obj client.Object) {
				if _, ok := obj.(*corev1.Pod); !ok || done {
					return
				}
				done = true
				pods := &corev1.PodList{}
				if err := f.store.List(context.Background(), pods, client.InNamespace(f.m.Namespace)); err != nil {
					t.Fatal(err)
				}
				p := &pods.Items[0]
				mutate(p)
				status := p.Status.DeepCopy()
				if err := f.store.Update(context.Background(), p); err != nil {
					t.Fatal(err)
				}
				p.Status = *status
				if err := f.store.Status().Update(context.Background(), p); err != nil {
					t.Fatal(err)
				}
			}
			v, p, s, e := f.run()
			f.assertNoAuthority(v, p, s, e)
			if f.creates != 1 || f.deletes != 1 || f.logs.Load() != 0 {
				t.Fatal("invalid original Pod was consumed")
			}
			if (name == probeExitCase || name == probeSignalCase) && (e != nil || s != errNativePreflightTerminal.Error()) {
				t.Fatal("terminal failure did not return fixed refusal")
			}
		})
	}
}

func testProbeCaptureChanges(t *testing.T) {
	t.Helper()

	t.Run("hidden duplicate ownerUID", func(t *testing.T) {
		f := newPreflightFixture(t)
		done := false
		f.onRead = func(obj client.Object) {
			if _, ok := obj.(*batchv1.Job); !ok || done {
				return
			}
			done = true
			pods := &corev1.PodList{}
			if err := f.store.List(context.Background(), pods, client.InNamespace(f.m.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(pods.Items) == 0 {
				return
			}
			p := pods.Items[0].DeepCopy()
			p.Name += "-hidden"
			p.UID = probeReplacement
			p.Labels = nil
			p.ResourceVersion = ""
			if err := f.store.Create(context.Background(), p); err != nil {
				t.Fatal(err)
			}
		}
		v, p, s, e := f.run()
		f.assertNoAuthority(v, p, s, e)
		if f.logs.Load() != 0 {
			t.Fatal("namespace-wide duplicate escaped admission")
		}
	})
	for _, field := range []string{"PodUID", "containerID", "JobUID", "node assignment"} {
		t.Run("capture replacement/"+field, func(t *testing.T) {
			f := newPreflightFixture(t)
			f.onLog = func(job *batchv1.Job) {
				if field == "JobUID" {
					j := job.DeepCopy()
					j.UID = probeReplacement
					if err := f.store.Update(context.Background(), j); err != nil {
						t.Fatal(err)
					}
					return
				}
				pods := &corev1.PodList{}
				if err := f.store.List(context.Background(), pods, client.InNamespace(f.m.Namespace)); err != nil {
					t.Fatal(err)
				}
				p := &pods.Items[0]
				switch field {
				case "PodUID":
					p.UID = probeReplacement
				case "containerID":
					p.Status.ContainerStatuses[0].ContainerID = "replacement-container"
				default:
					p.Spec.NodeName = "replacement-node"
				}
				status := p.Status.DeepCopy()
				if err := f.store.Update(context.Background(), p); err != nil {
					t.Fatal(err)
				}
				p.Status = *status
				if err := f.store.Status().Update(context.Background(), p); err != nil {
					t.Fatal(err)
				}
			}
			v, p, s, e := f.run()
			f.assertNoAuthority(v, p, s, e)
			if f.creates != 1 {
				t.Fatal("ambiguous capture proceeded to DB")
			}
		})
	}
}

func testProbeResultChanges(t *testing.T) {
	t.Helper()

	for _, name := range []string{"closure", "check order", "operation", "diagnostics noise"} {
		t.Run("preflight result/"+name, func(t *testing.T) {
			f := newPreflightFixture(t)
			f.output = func(job *batchv1.Job) []byte {
				if strings.HasPrefix(job.Name, probeVersion) {
					return f.version
				}
				raw := string(probeResult(job, false))
				switch name {
				case "closure":
					raw = strings.Replace(raw, `"source_closed":true`, `"source_closed":false`, 1)
				case "check order":
					raw = strings.Replace(raw, `"connections","identity"`, `"identity","connections"`, 1)
				case "operation":
					raw = strings.Replace(raw, strings.TrimPrefix(job.Name, probeDB), strings.Repeat("f", 32), 1)
				default:
					raw += "noise\n"
				}
				return []byte(raw)
			}
			v, p, s, e := f.run()
			f.assertNoAuthority(v, p, s, e)
			if f.creates != 2 || f.deletes != 2 {
				t.Fatal("result refusal did not clean both known roles")
			}
		})
	}
}

func TestNativePreflightCleanup(t *testing.T) {
	testProbeCleanupFailures(t)
	testProbeCancellation(t)
	testProbeDeadlines(t)
}

func testProbeCleanupFailures(t *testing.T) {
	t.Helper()

	t.Run("exact Pod absence required", func(t *testing.T) {
		f := newPreflightFixture(t)
		f.keepPods = true
		f.cleanupListError = true
		v, p, s, e := f.run()
		f.assertNoAuthority(v, p, s, e)
		if f.creates != 1 || f.deletes != 1 {
			t.Fatal("unproven Pod absence admitted more work")
		}
	})
	t.Run("one original work and cleanup deadline", func(t *testing.T) {
		f := newPreflightFixture(t)
		start := time.Now()
		calls := 0
		f.r.now = func() time.Time { calls++; return start }
		v, p, s, e := f.run()
		if v == nil || !p || s != "" || e != nil || calls != 1 || len(f.workDeadlines) != 2 || !f.workDeadlines[0].Equal(f.workDeadlines[1]) || !f.workDeadlines[0].Equal(start.Add(60*time.Second)) {
			t.Fatal("work deadline renewed or failed")
		}
		for _, d := range f.cleanupDeadlines {
			if d.After(start.Add(nativePreflightTotal)) {
				t.Fatal("cleanup exceeded original overall deadline")
			}
		}
	})
	t.Run("delete uncertainty", func(t *testing.T) {
		f := newPreflightFixture(t)
		f.deleteError = true
		v, p, s, e := f.run()
		f.assertNoAuthority(v, p, s, e)
		if f.creates != 1 || f.deletes != 1 {
			t.Fatal("cleanup failure admitted DB work")
		}
	})
	t.Run("same name Job replacement", func(t *testing.T) {
		f := newPreflightFixture(t)
		f.onLog = func(job *batchv1.Job) {
			j := job.DeepCopy()
			j.UID = probeReplacement
			if err := f.store.Update(context.Background(), j); err != nil {
				t.Fatal(err)
			}
		}
		v, p, s, e := f.run()
		f.assertNoAuthority(v, p, s, e)
		if f.deletes != 0 {
			t.Fatal("cleanup deleted replacement Job")
		}
	})
}

func testProbeCancellation(t *testing.T) {
	t.Helper()

	t.Run("caller canceled before effects", func(t *testing.T) {
		f := newPreflightFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		v, p, s, e := f.r.ensureNativePreflight(ctx, f.m)
		if v != nil || p || s != "" || !errors.Is(e, context.Canceled) || f.creates != 0 {
			t.Fatal("canceled caller reached effects")
		}
	})
	t.Run("canceled work still cleans", func(t *testing.T) {
		f := newPreflightFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.onLog = func(*batchv1.Job) { cancel() }
		f.onDelete = func(*batchv1.Job) {
			if ctx.Err() != context.Canceled {
				t.Fatal("fixture caller was not canceled")
			}
		}
		v, p, s, e := f.r.ensureNativePreflight(ctx, f.m)
		if v != nil || p || s != "" || !errors.Is(e, context.Canceled) || f.creates != 1 || f.deletes != 1 {
			t.Fatal("caller cancellation discarded cleanup or passed", e)
		}
	})
}

func testProbeDeadlines(t *testing.T) {
	t.Helper()

	t.Run("insufficient cleanup reservation", func(t *testing.T) {
		f := newPreflightFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 29*time.Second)
		defer cancel()
		v, p, s, e := f.r.ensureNativePreflight(ctx, f.m)
		f.assertNoAuthority(v, p, s, e)
		if f.creates != 0 {
			t.Fatal("short total budget created a Job")
		}
	})
	for _, role := range []string{probeDiscoveryCase, "database"} {
		t.Run("internal pending/"+role, func(t *testing.T) {
			f := newPreflightFixture(t)
			start := time.Now().Add(-59800 * time.Millisecond)
			f.r.now = func() time.Time { return start }
			f.pending = role == probeDiscoveryCase
			if role == "database" {
				f.onCreate = func(j *batchv1.Job) {
					if strings.HasPrefix(j.Name, probeDB) {
						f.pending = true
					}
				}
			}
			v, p, s, e := f.run()
			if p || s != "" || e != nil || (role == probeDiscoveryCase) != (v == nil) || f.deletes != f.creates || f.creates < 1 {
				t.Fatal("internal pending timeout did not wait after exact cleanup", s, e)
			}
		})
	}
}

func (f *preflightFixture) createJob(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	t := f.t
	d, ok := ctx.Deadline()
	if !ok || ctx.Err() != nil {
		t.Fatal("created outside work deadline")
	}
	f.workDeadlines = append(f.workDeadlines, d)

	f.creates++
	job, ok := obj.(*batchv1.Job)
	if !ok {
		t.Fatal("preflight created non-Job")
	}
	if f.createMode == "AlreadyExists" {
		return apierrors.NewAlreadyExists(batchv1.Resource("jobs"), job.Name)
	}
	if f.createMode == string(testUnrecognizedEngine) {
		return errors.New(probeCanary)
	}
	job.UID = types.UID("probe-job-" + strconv.Itoa(f.creates))
	job.Generation = 1
	job.TypeMeta = metav1.TypeMeta{APIVersion: probeBatchAPI, Kind: lifecycleJobKind}
	defaultProbeJob(job)
	if strings.HasPrefix(job.Name, probeVersion) && (len(job.Spec.Template.Spec.Containers[0].Env) != 0 || len(job.Spec.Template.Spec.Volumes) != 0) {
		t.Fatal("version Job projected credentials")
	}
	if f.onCreate != nil {
		f.onCreate(job)
	}
	if f.createMode == "empty UID" {
		job.UID = ""
	}
	if !f.pending {
		job.Status.Succeeded = 1
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	}
	if err := c.Create(ctx, job, opts...); err != nil {
		return err
	}
	if job.UID != "" {
		pod := probePod(job)
		if f.pending {
			pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
		}
		if err := c.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	if f.createMode == "lost acknowledgment" {
		return errors.New(probeCanary)
	}
	return nil

}

func (f *preflightFixture) deleteJob(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	t := f.t
	d, ok := ctx.Deadline()
	if !ok || ctx.Err() != nil || time.Until(d) > nativePreflightCleanupReserve {
		t.Fatal("cleanup lost cancellation independence or original bounded reservation")
	}
	f.cleanupDeadlines = append(f.cleanupDeadlines, d)
	if len(f.workDeadlines) != 0 && d.After(f.workDeadlines[0].Add(nativePreflightCleanupReserve)) {
		t.Fatal("cleanup renewed invocation deadline")
	}

	f.deletes++
	job, ok := obj.(*batchv1.Job)
	if !ok {
		t.Fatal("preflight directly deleted Pod or another object")
	}
	options := &client.DeleteOptions{}
	options.ApplyOptions(opts)
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != job.UID || options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
		t.Fatal("cleanup lacked exact UID/foreground precondition")
	}
	if f.deleteError {
		return errors.New(probeCanary)
	}
	if f.onDelete != nil {
		f.onDelete(job)
	}
	if err := c.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	if !f.keepPods {
		pods := &corev1.PodList{}
		if err := c.List(ctx, pods, client.InNamespace(job.Namespace)); err != nil {
			t.Fatal(err)
		}
		for i := range pods.Items {
			for _, o := range pods.Items[i].OwnerReferences {
				if o.UID == job.UID {
					if err := c.Delete(ctx, &pods.Items[i]); err != nil {
						t.Fatal(err)
					}
					break
				}
			}
		}
	}
	return nil

}

func testProbeCapabilityOmissions(t *testing.T) {
	t.Helper()
	for _, profile := range []string{"default clone", probeFollowCase} {
		caps := []native.Capability{native.CapCloneSingleDB, native.CapProgress, native.CapVerifySealedFiles, native.CapBinaryFallback, native.CapSplit, native.CapLargeObjects}
		if profile == probeFollowCase {
			caps = []native.Capability{native.CapCloneSingleDB, native.CapProgress, native.CapVerifySealedFiles, native.CapPGOutput, native.CapCutover, native.CapCleanup}
		}
		for omit := range caps {
			t.Run(profile+"/"+string(caps[omit]), func(t *testing.T) {
				f := newPreflightFixture(t)
				if profile == probeFollowCase {
					f.m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
				} else {
					f.m.Spec.Clone.UseCopyBinary = nil
					f.m.Spec.Clone.SplitTablesLargerThan = nil
					f.m.Spec.Clone.Skip = nil
				}
				if err := f.store.Update(context.Background(), f.m); err != nil {
					t.Fatal(err)
				}
				selected := append([]native.Capability{}, caps[:omit]...)
				selected = append(selected, caps[omit+1:]...)
				raw, err := json.Marshal(native.Version{ProtocolVersion: 1, EngineVersion: "fixture", Capabilities: selected})
				if err != nil {
					t.Fatal(err)
				}
				f.version = append(raw, '\n')
				v, p, s, e := f.run()
				f.assertNoAuthority(v, p, s, e)
				if f.creates != 1 || f.deletes != 1 {
					t.Fatal("missing selected capability projected credentials")
				}
			})
		}
	}
}

func TestNativePreflightCanonicalProjection(t *testing.T) {
	t.Run("legitimate inline profile", testProbeInlineProjection)
	for _, stage := range []string{probeBeforeCapture, probeDuringCapture} {
		for _, field := range []string{probeCredentialLiteral, probeControlLiteral, probeTemplateAnnotation, probeTemplateLabel} {
			t.Run(stage+"/"+field, func(t *testing.T) {
				f := newCanonicalProjectionFixture(t)
				changed := false
				substitute := func() {
					if changed {
						return
					}
					changed = true
					substituteProbeProjection(t, f, field)
				}
				if stage == probeBeforeCapture {
					f.onRead = func(obj client.Object) {
						if _, ok := obj.(*batchv1.Job); ok && f.creates == 2 {
							substitute()
						}
					}
				} else {
					f.onLog = func(job *batchv1.Job) {
						if strings.HasPrefix(job.Name, probeDB) {
							substitute()
						}
					}
				}
				v, passed, failure, err := f.run()
				f.assertNoAuthority(v, passed, failure, err)
				wantLogs := int32(1)
				if stage == probeDuringCapture {
					wantLogs = 2
				}
				if !changed || f.creates != 2 || f.deletes != 2 || f.logs.Load() != wantLogs {
					t.Fatal("canonical substitution reached an unexpected helper/output boundary")
				}
			})
		}
	}
}

const (
	probeInlineSourceEnv             = "PGCOPYDB_SOURCE_PGURI"
	probeInlineTargetEnv             = "PGCOPYDB_TARGET_PGURI"
	probeBeforeCapture               = "before checked capture"
	probeDuringCapture               = "during checked capture"
	probeCredentialLiteral           = "inline credential literal"
	probeControlLiteral              = "fixed control literal"
	probeTemplateAnnotation          = "template annotation"
	probeTemplateLabel               = "template label"
	probeProjectionMetadataKey       = "canonical-regression"
	probeProjectionReplacementValue  = "substituted"
	probeProjectionFollowEnvironment = "RPGMIGRATE_NATIVE_PREFLIGHT_FOLLOW"
)

func mutateProbeProjection(t *testing.T, job *batchv1.Job, pod *corev1.Pod, field string) {
	t.Helper()
	if field == probeTemplateAnnotation {
		job.Spec.Template.Annotations = maps.Clone(job.Spec.Template.Annotations)
		if job.Spec.Template.Annotations == nil {
			job.Spec.Template.Annotations = map[string]string{}
		}
		pod.Annotations = maps.Clone(pod.Annotations)
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		job.Spec.Template.Annotations[probeProjectionMetadataKey] = probeProjectionReplacementValue
		pod.Annotations[probeProjectionMetadataKey] = probeProjectionReplacementValue
		return
	}
	if field == probeTemplateLabel {
		job.Spec.Template.Labels = maps.Clone(job.Spec.Template.Labels)
		pod.Labels = maps.Clone(pod.Labels)
		job.Spec.Template.Labels[probeProjectionMetadataKey] = probeProjectionReplacementValue
		pod.Labels[probeProjectionMetadataKey] = probeProjectionReplacementValue
		return
	}
	name, value := probeInlineSourceEnv, probeCanary
	if field == probeControlLiteral {
		name = probeProjectionFollowEnvironment
		value = "1"
	}
	for _, env := range []*[]corev1.EnvVar{&job.Spec.Template.Spec.Containers[0].Env, &pod.Spec.Containers[0].Env} {
		index := slices.IndexFunc(*env, func(v corev1.EnvVar) bool { return v.Name == name })
		if index < 0 || (*env)[index].ValueFrom != nil || (*env)[index].Value == value {
			t.Fatal("fixture has no distinct canonical literal to substitute")
		}
		(*env)[index].Value = value
	}
}

func testProbeInlineProjection(t *testing.T) {
	t.Helper()

	f := newCanonicalProjectionFixture(t)
	checked := false
	f.onCreate = func(job *batchv1.Job) {
		if !strings.HasPrefix(job.Name, probeDB) {
			return
		}
		checked = true
		for _, name := range []string{probeInlineSourceEnv, probeInlineTargetEnv} {
			index := slices.IndexFunc(job.Spec.Template.Spec.Containers[0].Env, func(env corev1.EnvVar) bool { return env.Name == name })
			if index < 0 || job.Spec.Template.Spec.Containers[0].Env[index].Value == "" || job.Spec.Template.Spec.Containers[0].Env[index].ValueFrom != nil {
				t.Fatal("legitimate inline profile lost canonical materialization")
			}
		}
	}
	v, passed, failure, err := f.run()
	if !checked || v == nil || !passed || failure != "" || err != nil || f.creates != 2 || f.deletes != 2 || f.logs.Load() != 2 {
		t.Fatal("legitimate canonical inline execution was refused")
	}

}

func substituteProbeProjection(t *testing.T, f *preflightFixture, field string) {
	t.Helper()
	jobs, pods := &batchv1.JobList{}, &corev1.PodList{}
	if err := f.store.List(context.Background(), jobs, client.InNamespace(f.m.Namespace)); err != nil {
		t.Fatal("cannot inspect owned fixture Job")
	}
	if err := f.store.List(context.Background(), pods, client.InNamespace(f.m.Namespace)); err != nil {
		t.Fatal("cannot inspect owned fixture Pod")
	}
	if len(jobs.Items) != 1 || len(pods.Items) != 1 || !strings.HasPrefix(jobs.Items[0].Name, probeDB) {
		t.Fatal("substitution did not target the original fixed role")
	}
	job, pod := &jobs.Items[0], &pods.Items[0]
	canonical, err := buildNativePreflightJob(f.m, f.r.RPGMigrateRunnerImage, job.Name, strings.TrimPrefix(job.Name, probeDB))
	if err != nil || controllerutil.SetControllerReference(f.m, canonical, f.r.Scheme) != nil {
		t.Fatal("cannot construct independent canonical expectation")
	}
	canonical.UID = job.UID
	if !nativePreflightJobMatches(canonical, job) || !nativePreflightPodMatches(canonical, pod) {
		t.Fatal("original fixture was not independently canonical")
	}
	beforeTemplate := job.Spec.Template.DeepCopy()
	originalUID, originalContainerID := pod.UID, pod.Status.ContainerStatuses[0].ContainerID
	mutateProbeProjection(t, job, pod, field)
	if pod.UID != originalUID || pod.Status.ContainerStatuses[0].ContainerID != originalContainerID || pod.Status.ContainerStatuses[0].RestartCount != 0 {
		t.Fatal("substitution changed original incarnation instead of canonical literals or metadata")
	}
	if podexec.NativeExecutionSHA256(*beforeTemplate) != podexec.NativeExecutionSHA256(job.Spec.Template) {
		t.Fatal("fixture did not preserve projected execution correlation")
	}
	if !nativePreflightPodMatches(job, pod) {
		t.Fatal("fixture did not make matching observed Job and Pod facts")
	}
	if nativePreflightJobMatches(canonical, job) || nativePreflightPodMatches(canonical, pod) {
		t.Fatal("independent canonical comparison accepted substituted values")
	}
	if err := f.store.Update(context.Background(), job); err != nil {
		t.Fatal("cannot install fixture Job substitution")
	}
	if err := f.store.Update(context.Background(), pod); err != nil {
		t.Fatal("cannot install fixture Pod substitution")
	}
}

func newCanonicalProjectionFixture(t *testing.T) *preflightFixture {
	t.Helper()
	f := newPreflightFixture(t)
	f.m.Spec.Source.PasswordSecretRef = nil
	f.m.Spec.Target.PasswordSecretRef = nil
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal("cannot install inline-only fixture configuration")
	}
	return f
}

func TestNativePreflightRefusesHistoricalSixBeforeCredentials(t *testing.T) {
	raw, err := os.ReadFile("../native/testdata/v1/payloads/version-qualified-profile.json")
	if err != nil {
		t.Fatal(err)
	}
	version, err := native.ParseVersion(raw)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(version)
	if err != nil {
		t.Fatal(err)
	}
	f := newPreflightFixture(t)
	f.version = append(payload, '\n')
	v, p, failure, err := f.run()
	f.assertNoAuthority(v, p, failure, err)
	if err != nil || failure == "" || f.creates != 1 || f.deletes != 1 || f.logs.Load() != 1 {
		t.Fatal("historical producer reached credential projection or lacked discovery cleanup")
	}
}

func TestNativePreflightAcceptsLinuxSealedFileDiscovery(t *testing.T) {
	raw, err := os.ReadFile("../native/testdata/v1/payloads/version-qualified-linux-profile.json")
	if err != nil {
		t.Fatal(err)
	}
	version, err := native.ParseVersion(raw)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(version)
	if err != nil {
		t.Fatal(err)
	}
	f := newPreflightFixture(t)
	f.version = append(payload, '\n')
	v, passed, failure, err := f.run()
	if err != nil || failure != "" || !passed || v == nil || f.creates != 2 || f.deletes != 2 || f.logs.Load() != 2 {
		t.Fatal("Linux sealed-file discovery failed checked preflight", failure, err)
	}
}
