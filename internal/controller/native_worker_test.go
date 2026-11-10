package controller

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strconv"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const (
	workerStatusUnavailableCase           = "status unavailable"
	workerLostAckCase                     = "lost acknowledgment"
	workerUnknownCreateCase               = "unknown before storage"
	workerSpecCase                        = "spec"
	workerOwnedCollisionCase              = "AlreadyExists owned"
	workerReadMigration                   = "migration"
	workerReadPVC                         = "pvc"
	workerReadJob                         = "job"
	workerForeignIdentity                 = "foreign"
	workerUIDCase                         = "UID"
	workerEmptyUIDCase                    = "empty UID"
	workerForeignCollisionCase            = "AlreadyExists foreign"
	workerNegativeAttemptCase             = "negative attempt"
	workerSuspendedCase                   = "suspended"
	workerDeletingCase                    = "deleting"
	workerRunIDEnv                        = "RPGMIGRATE_RUN_ID"
	workerAttemptCanary                   = "postgresql://private-user:private-password@private-host/private-db"
	workerServerUID             types.UID = "actual-server-worker"
	workerReplacementUID        types.UID = "replacement-worker"
)

type workerAttemptFixture struct {
	t                *testing.T
	r                *MigrationReconciler
	store            client.WithWatch
	m                *v1beta1.Migration
	pvc              *corev1.PersistentVolumeClaim
	reads            map[string]int
	creates, patches int
	beforeRead       func(client.Object, int)
	create           func(context.Context, client.WithWatch, client.Object) error
	patch            func(context.Context, client.Client, client.Object, client.Patch) error
}

func newWorkerAttemptFixture(t *testing.T) *workerAttemptFixture {
	t.Helper()
	m, pvc := nativeIdentityObjects()
	m.Generation = 1
	m.Spec.Source, m.Spec.Target = passwordMigration().Spec.Source, passwordMigration().Spec.Target
	pvc.Status.Phase = corev1.ClaimPending
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	store := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(m, pvc).WithStatusSubresource(&v1beta1.Migration{}).Build()
	if err := store.Get(context.Background(), client.ObjectKeyFromObject(m), m); err != nil {
		t.Fatal(err)
	}
	f := &workerAttemptFixture{t: t, store: store, m: m, pvc: pvc, reads: map[string]int{}}
	f.r = &MigrationReconciler{Scheme: scheme, Recorder: events.NewFakeRecorder(20), RPGMigrateRunnerImage: nativeEvidenceTestImage}
	f.r.APIReader = workerAttemptReader{f}
	f.r.Client = interceptor.NewClient(store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Fatal("native attempt used cached GET")
			return errBoom
		},
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			t.Fatal("native attempt listed resources")
			return errBoom
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			f.creates++
			if _, ok := obj.(*batchv1.Job); !ok {
				t.Fatal("native attempt created a non-Job")
			}
			if f.create != nil {
				return f.create(ctx, c, obj)
			}
			obj.SetUID(workerServerUID)
			return c.Create(ctx, obj, opts...)
		},
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			t.Fatal("native attempt deleted a resource")
			return errBoom
		},
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			t.Fatal("native attempt patched metadata")
			return errBoom
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			f.patches++
			if sub != "status" {
				t.Fatal("native attempt patched another subresource")
			}
			data, err := p.Data(obj)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Metadata struct {
					ResourceVersion string `json:"resourceVersion"`
				} `json:"metadata"`
			}
			if json.Unmarshal(data, &wire) != nil || wire.Metadata.ResourceVersion == "" {
				t.Fatal("native status patch lacks resourceVersion precondition")
			}
			if f.patch != nil {
				return f.patch(ctx, c, obj, p)
			}
			return c.Status().Patch(ctx, obj, p, opts...)
		},
	})
	f.r.Sentinel, f.r.Progress, f.r.Logs = &fakeSentinel{}, &fakeProgress{}, &fakeLogs{}
	t.Cleanup(func() {
		if f.r.Sentinel.(*fakeSentinel).callCount() != 0 || f.r.Progress.(*fakeProgress).counts() != 0 || f.r.Logs.(*fakeLogs).calls != 0 {
			t.Error("native attempt reached legacy observation or SQL")
		}
	})
	return f
}

type workerAttemptReader struct{ f *workerAttemptFixture }

func (r workerAttemptReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	var kind string
	switch obj.(type) {
	case *v1beta1.Migration:
		kind = workerReadMigration
	case *corev1.PersistentVolumeClaim:
		kind = workerReadPVC
	case *batchv1.Job:
		kind = workerReadJob
	default:
		r.f.t.Fatal("native attempt read an unapproved resource")
	}
	r.f.reads[kind]++
	if r.f.beforeRead != nil {
		r.f.beforeRead(obj, r.f.reads[kind])
	}
	return r.f.store.Get(ctx, key, obj, opts...)
}
func (r workerAttemptReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	r.f.t.Fatal("native attempt used uncached LIST")
	return errBoom
}
func (f *workerAttemptFixture) expected() *batchv1.Job {
	f.t.Helper()
	job, err := buildNativeWorkerJob(f.m, f.pvc, f.r.RPGMigrateRunnerImage, 1)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(f.m, job, f.r.Scheme); err != nil {
		f.t.Fatal(err)
	}
	return job
}
func (f *workerAttemptFixture) storedMigration() *v1beta1.Migration {
	f.t.Helper()
	m := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), m); err != nil {
		f.t.Fatal(err)
	}
	return m
}
func (f *workerAttemptFixture) changeMigration(change func(*v1beta1.Migration)) {
	f.t.Helper()
	m := f.storedMigration()
	change(m)
	if err := f.store.Update(context.Background(), m); err != nil {
		f.t.Fatal(err)
	}
}
func (f *workerAttemptFixture) refused(err error, creates, patches int) {
	f.t.Helper()
	if err == nil || strings.Contains(err.Error(), workerAttemptCanary) || f.creates != creates || f.patches != patches {
		f.t.Fatal("refusal lost effect bounds or sanitization")
	}
	if f.r.Sentinel.(*fakeSentinel).callCount() != 0 || f.r.Progress.(*fakeProgress).counts() != 0 || f.r.Logs.(*fakeLogs).calls != 0 {
		f.t.Fatal("native attempt reached legacy observation or SQL")
	}
	if m := f.storedMigration(); m.Status.Attempts != 0 || m.Status.JobName != "" || m.Status.NativeJobUID != "" || m.Status.NativePVCUID != "" {
		f.t.Fatal("refusal recorded an attempt")
	}
}
func TestNativeWorkerDispatch(t *testing.T) {
	t.Run("first Pending consumer", func(t *testing.T) {
		f := newWorkerAttemptFixture(t)
		f.beforeRead = func(obj client.Object, n int) {
			if _, ok := obj.(*v1beta1.Migration); ok && n == 3 {
				m := f.storedMigration()
				m.Status.Progress = &v1beta1.CloneProgress{TablesDone: 7}
				if err := f.store.Status().Update(context.Background(), m); err != nil {
					t.Fatal(err)
				}
			}
		}
		res, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy())
		if err != nil || res.RequeueAfter != pollInterval || f.creates != 1 || f.patches != 1 {
			t.Fatal("native first attempt did not create and record once", err)
		}
		got := f.storedMigration()
		if got.Status.Attempts != 1 || got.Status.JobName != jobName(f.m, 1) || got.Status.NativeJobUID != string(workerServerUID) || got.Status.NativePVCUID != string(f.pvc.UID) || got.Status.Progress == nil || got.Status.Progress.TablesDone != 7 {
			t.Fatal("status did not preserve final live peer fields")
		}
		job := &batchv1.Job{}
		if err := f.store.Get(context.Background(), types.NamespacedName{Namespace: f.m.Namespace, Name: got.Status.JobName}, job); err != nil {
			t.Fatal(err)
		}
		if job.UID != workerServerUID || !nativeWorkerJobMatches(f.m, f.pvc, f.expected(), job) {
			t.Fatal("recorded worker was not actual server identity")
		}
		if len(f.r.Recorder.(*events.FakeRecorder).Events) != 1 {
			t.Fatal("new admitted worker was not announced exactly once")
		}
	})
	workerAttemptEntryRefusals(t)
	workerAttemptBindingRaces(t)
	for _, stage := range []int{1, 2, 3} {
		t.Run("concurrent attempt/"+strconv.Itoa(stage), func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			f.beforeRead = func(obj client.Object, n int) {
				if _, ok := obj.(*v1beta1.Migration); ok && n == stage {
					m := f.storedMigration()
					m.Status.Attempts, m.Status.JobName = 1, "concurrent-worker"
					if err := f.store.Status().Update(context.Background(), m); err != nil {
						t.Fatal(err)
					}
				}
			}
			_, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy())
			creates := 0
			if stage == 3 {
				creates = 1
			}
			if err == nil || f.creates != creates || f.patches != 0 || f.storedMigration().Status.JobName != "concurrent-worker" {
				t.Fatal("changed attempt was overwritten or replayed")
			}
		})
	}
}
func workerAttemptEntryRefusals(t *testing.T) {
	t.Helper()
	for _, name := range []string{"no reader", "no client", "no scheme", "no recorder", "no native image", "canceled", "unknown engine", workerNegativeAttemptCase, "no UID", "no generation", workerSuspendedCase, "terminal", workerDeletingCase} {
		t.Run(name, func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			ctx := context.Background()
			switch name {
			case "no reader":
				f.r.APIReader = nil
			case "no client":
				f.r.Client = nil
			case "no scheme":
				f.r.Scheme = nil
			case "no recorder":
				f.r.Recorder = nil
			case "no native image":
				f.r.RPGMigrateRunnerImage, f.r.RunnerImage = "", nativeEvidenceTestImage
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "unknown engine":
				f.m.Spec.Engine = testUnrecognizedEngine
			case workerNegativeAttemptCase:
				f.m.Status.Attempts = -1
			case "no UID":
				f.m.UID = ""
			case "no generation":
				f.m.Generation = 0
			case workerSuspendedCase:
				f.m.Spec.Suspend = true
			case "terminal":
				meta.SetStatusCondition(&f.m.Status.Conditions, metav1.Condition{Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "Done"})
			case workerDeletingCase:
				now := metav1.Now()
				f.m.DeletionTimestamp = &now
			}
			_, err := f.r.startAttempt(ctx, f.m, f.m.DeepCopy())
			f.refused(err, 0, 0)
		})
	}
}

func workerAttemptBindingRaces(t *testing.T) {
	t.Helper()
	for _, kind := range []string{workerReadMigration, workerReadPVC} {
		for _, stage := range []int{1, 2, 3} {
			for _, field := range []string{workerUIDCase, workerSpecCase, "owner or generation"} {
				t.Run(kind+"/"+strconv.Itoa(stage)+"/"+field, func(t *testing.T) {
					f := newWorkerAttemptFixture(t)
					f.beforeRead = func(obj client.Object, n int) {
						if n != stage {
							return
						}
						if _, ok := obj.(*v1beta1.Migration); ok && kind == workerReadMigration {
							f.changeMigration(func(m *v1beta1.Migration) {
								switch field {
								case workerUIDCase:
									m.UID = "11234567-89ab-cdef-0123-456789abcdef"
								case workerSpecCase:
									m.Spec.Suspend = true
								default:
									m.Generation++
								}
							})
						}
						if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && kind == workerReadPVC {
							pvc := &corev1.PersistentVolumeClaim{}
							if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.pvc), pvc); err != nil {
								t.Fatal(err)
							}
							switch field {
							case workerUIDCase:
								pvc.UID = "replacement-pvc"
							case workerSpecCase:
								pvc.Spec.VolumeName = "replacement-volume"
							default:
								pvc.OwnerReferences[0].UID = workerForeignIdentity
							}
							if err := f.store.Update(context.Background(), pvc); err != nil {
								t.Fatal(err)
							}
						}
					}
					_, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy())
					// The first read binds the current owned claim, including a legitimate bound volume name.
					if kind == workerReadPVC && stage == 1 && field != "owner or generation" {
						if err != nil {
							t.Fatal("valid current PVC identity was refused", err)
						}
						return
					}
					creates := 0
					if stage == 3 {
						creates = 1
					}
					f.refused(err, creates, 0)
				})
			}
		}
	}
}
func TestNativeWorkerExistingJobBinding(t *testing.T) {
	mutations := map[string]func(*batchv1.Job){
		"canonical": func(*batchv1.Job) {},
		"API defaults": func(j *batchv1.Job) {
			j.Spec.Parallelism = ptr.To(int32(1))
			j.Spec.Completions = ptr.To(int32(1))
			j.Spec.CompletionMode = ptr.To(batchv1.NonIndexedCompletion)
			j.Spec.Suspend = ptr.To(false)
			j.Spec.ManualSelector = ptr.To(false)
			j.Spec.PodReplacementPolicy = ptr.To(batchv1.TerminatingOrFailed)
			j.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(30))
			j.Spec.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
			j.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullIfNotPresent
			j.Spec.Template.Spec.Containers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
			j.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
			j.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{batchv1.ControllerUidLabel: string(j.UID)}}
			maps.Copy(j.Spec.Template.Labels, map[string]string{batchv1.ControllerUidLabel: string(j.UID), nativeWorkerLegacyControllerLabel: string(j.UID), batchv1.JobNameLabel: j.Name, nativeWorkerLegacyJobNameLabel: j.Name})
		},
		workerEmptyUIDCase: func(j *batchv1.Job) { j.UID = "" },
		"foreign owner":    func(j *batchv1.Job) { j.OwnerReferences[0].UID = workerForeignIdentity },
		"owner context":    func(j *batchv1.Job) { j.OwnerReferences[0].Name = workerForeignIdentity },
		"legacy program":   func(j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].Command[3] = workerContainer },
		"argv": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].Args = append(j.Spec.Template.Spec.Containers[0].Args, "--resume")
		},
		"prelude": func(j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].Command[2] += "\nfalse" },
		"image":   func(j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].Image = testRunnerImage },
		"duplicate identity": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].Env = append(j.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: workerRunIDEnv, Value: workerForeignIdentity})
		},
		"credential reference": func(j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].Env[0].Name = workerForeignIdentity },
		"PVC tuple": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = workerForeignIdentity
		},
		"mount":              func(j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].VolumeMounts[0].ReadOnly = true },
		"security":           func(j *batchv1.Job) { j.Spec.Template.Spec.HostNetwork = true },
		"container security": func(j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].SecurityContext.Privileged = ptr.To(true) },
		"labels":             func(j *batchv1.Job) { j.Labels[labelMigration] = workerForeignIdentity },
		"controller labels":  func(j *batchv1.Job) { j.Spec.Template.Labels[batchv1.ControllerUidLabel] = workerForeignIdentity },
		"selector": func(j *batchv1.Job) {
			j.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"unexpected": "true"}}
		},
		"parallelism":        func(j *batchv1.Job) { j.Spec.Parallelism = ptr.To(int32(2)) },
		"retry":              func(j *batchv1.Job) { j.Spec.BackoffLimit = ptr.To(int32(1)) },
		"unknown default":    func(j *batchv1.Job) { j.Spec.Template.Spec.EnableServiceLinks = ptr.To(true) },
		"completion mode":    func(j *batchv1.Job) { j.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion) },
		"suspend":            func(j *batchv1.Job) { j.Spec.Suspend = ptr.To(true) },
		"manual selector":    func(j *batchv1.Job) { j.Spec.ManualSelector = ptr.To(true) },
		"replacement policy": func(j *batchv1.Job) { j.Spec.PodReplacementPolicy = ptr.To(batchv1.Failed) },
		"termination grace":  func(j *batchv1.Job) { j.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(60)) },
		"scheduler":          func(j *batchv1.Job) { j.Spec.Template.Spec.SchedulerName = "custom-scheduler" },
		"pull policy":        func(j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways },
		"termination path":   func(j *batchv1.Job) { j.Spec.Template.Spec.Containers[0].TerminationMessagePath = "/tmp/changed" },
		"termination policy": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		},
		workerDeletingCase: func(j *batchv1.Job) { now := metav1.Now(); j.DeletionTimestamp = &now; j.Finalizers = []string{"hold"} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			job := f.expected()
			job.UID = workerServerUID
			mutate(job)
			before := job.DeepCopy()
			want := f.expected()
			wantBefore := want.DeepCopy()
			matches := nativeWorkerJobMatches(f.m, f.pvc, want, job)
			positive := name == "canonical" || name == "API defaults"
			if matches != positive || !apiequality.Semantic.DeepEqual(job, before) || !apiequality.Semantic.DeepEqual(want, wantBefore) {
				t.Fatal("canonical matcher lost exactness or mutated inputs")
			}
			if err := f.store.Create(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			_, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy())
			f.refused(err, 0, 0)
		})
	}
	t.Run("replaced after admission", func(t *testing.T) {
		f := newWorkerAttemptFixture(t)
		job := f.expected()
		job.UID = workerServerUID
		if err := f.store.Create(context.Background(), job); err != nil {
			t.Fatal(err)
		}
		f.beforeRead = func(obj client.Object, n int) {
			if _, ok := obj.(*batchv1.Job); ok && n == 2 {
				current := job.DeepCopy()
				if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(job), current); err != nil {
					t.Fatal(err)
				}
				current.UID = workerReplacementUID
				if err := f.store.Update(context.Background(), current); err != nil {
					t.Fatal(err)
				}
			}
		}
		_, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy())
		f.refused(err, 0, 0)
	})
}
func TestNativeWorkerCreateUncertainty(t *testing.T) {
	for _, name := range []string{workerOwnedCollisionCase, workerForeignCollisionCase, workerUnknownCreateCase, workerLostAckCase, workerEmptyUIDCase, "returned mutated", workerStatusUnavailableCase} {
		t.Run(name, func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			f.create = func(ctx context.Context, c client.WithWatch, obj client.Object) error {
				obj.SetUID(workerServerUID)
				switch name {
				case workerUnknownCreateCase:
					return errors.New(workerAttemptCanary)
				case workerOwnedCollisionCase, workerForeignCollisionCase:
					if name == workerForeignCollisionCase {
						obj.SetOwnerReferences(nil)
					}
					if err := c.Create(ctx, obj); err != nil {
						t.Fatal(err)
					}
					return apierrors.NewAlreadyExists(batchv1.Resource("jobs"), obj.GetName())
				case workerEmptyUIDCase:
					obj.SetUID("")
				case "returned mutated":
					obj.(*batchv1.Job).Spec.Template.Spec.HostPID = true
				}
				if err := c.Create(ctx, obj); err != nil {
					t.Fatal(err)
				}
				if name == workerLostAckCase {
					return errors.New(workerAttemptCanary)
				}
				return nil
			}
			if name == workerStatusUnavailableCase {
				f.patch = func(context.Context, client.Client, client.Object, client.Patch) error {
					return errors.New(workerAttemptCanary)
				}
			}
			_, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy())

			patches := 0
			if name == workerStatusUnavailableCase {
				patches = 1
			}
			f.refused(err, 1, patches)
			if name == workerLostAckCase || name == workerUnknownCreateCase {
				if f.reads[workerReadJob] != 1 {
					t.Fatal("unknown Create was immediately adopted")
				}
			}
			if len(f.r.Recorder.(*events.FakeRecorder).Events) != 0 {
				t.Fatal("refused Create/status announced an attempt")
			}
		})
	}
	workerAttemptPatchRaces(t)
}
func workerAttemptPatchRaces(t *testing.T) {
	t.Helper()
	for _, field := range []string{"resourceVersion", workerUIDCase, lifecycleGenerationCase, lifecycleAttemptCase} {
		t.Run("status conflict/"+field, func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
				live := f.storedMigration()
				switch field {
				case workerUIDCase:
					live.UID = "11234567-89ab-cdef-0123-456789abcdef"
				case lifecycleGenerationCase:
					live.Generation++
				case lifecycleAttemptCase:
					live.Status.Attempts = 1
					live.Status.JobName = "peer-worker"
				default:
					live.Status.Progress = &v1beta1.CloneProgress{TablesDone: 7}
				}
				if field == workerUIDCase || field == lifecycleGenerationCase {
					if err := f.store.Update(ctx, live); err != nil {
						t.Fatal(err)
					}
				} else if err := f.store.Status().Update(ctx, live); err != nil {
					t.Fatal(err)
				}
				return c.Status().Patch(ctx, obj, p)
			}
			_, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy())
			if !apierrors.IsConflict(err) || f.creates != 1 || f.patches != 1 || strings.Contains(err.Error(), workerAttemptCanary) {
				t.Fatal("native status race did not fail with a bounded sanitized conflict")
			}
			got := f.storedMigration()
			if field == lifecycleAttemptCase {
				if got.Status.JobName != "peer-worker" {
					t.Fatal("conflict overwrote peer attempt")
				}
			} else if got.Status.Attempts != 0 || got.Status.JobName != "" {
				t.Fatal("conflict recorded worker")
			}
			if len(f.r.Recorder.(*events.FakeRecorder).Events) != 0 {
				t.Fatal("status conflict announced a worker")
			}
		})
	}
}
func TestNativeWorkerRetryRefusal(t *testing.T) {
	for _, attempts := range []int32{1, 2, 2147483647} {
		t.Run(strconv.FormatInt(int64(attempts), 10), func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			f.m.Status.Attempts = attempts
			_, err := f.r.startAttempt(context.Background(), f.m, f.m.DeepCopy())
			if !errors.Is(err, errNativeWorkerRetry) || len(f.reads) != 0 || f.creates != 0 || f.patches != 0 || f.r.Sentinel.(*fakeSentinel).callCount() != 0 || f.r.Progress.(*fakeProgress).counts() != 0 || f.r.Logs.(*fakeLogs).calls != 0 {
				t.Fatal("native retry reached legacy effects or lost fixed refusal")
			}
		})
	}
}
