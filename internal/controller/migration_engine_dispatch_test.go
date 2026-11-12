package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const testUnrecognizedEngine v1beta1.MigrationEngine = "unknown"

const (
	testDispatchUID       = "engine-dispatch"
	testUnsupportedEngine = "UnsupportedEngine"
)

func TestReconcileEngineDispatchRejectsBeforeEffects(t *testing.T) {
	states := []struct {
		name string
		init func(*v1beta1.Migration)
	}{
		{"new", func(*v1beta1.Migration) {}},
		{"pending", func(m *v1beta1.Migration) { m.Status.Phase = v1beta1.PhasePending }},
		{"running", func(m *v1beta1.Migration) {
			m.Status.Phase, m.Status.JobName, m.Status.Attempts = v1beta1.PhaseStreaming, workerJob, 1
		}},
		{"suspended", func(m *v1beta1.Migration) {
			m.Spec.Suspend = true
			m.Status.Phase, m.Status.JobName, m.Status.Attempts = v1beta1.PhaseStreaming, workerJob, 1
		}},
		{"deleting", func(m *v1beta1.Migration) {
			now := metav1.Now()
			m.DeletionTimestamp, m.Finalizers = &now, []string{finalizerName}
			m.Status.JobName, m.Status.Attempts = workerJob, 1
		}},
		{"complete with rehearsal", func(m *v1beta1.Migration) {
			meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
				Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "Succeeded",
			})
			m.Status.VerificationRehearsal = &v1beta1.VerificationRehearsalStatus{Outcome: v1beta1.RehearsalRunning}
		}},
		{"failed with rehearsal", func(m *v1beta1.Migration) {
			meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
				Type: v1beta1.ConditionFailed, Status: metav1.ConditionTrue, Reason: testInvalidSpec,
			})
			m.Status.VerificationRehearsal = &v1beta1.VerificationRehearsalStatus{Outcome: v1beta1.RehearsalRunning}
		}},
		{"invalid legacy connection", func(m *v1beta1.Migration) { m.Spec.Source.Host = "" }},
		{"legacy plugin", func(m *v1beta1.Migration) { m.Spec.Follow.Plugin = v1beta1.PluginWal2json }},
	}
	for _, engine := range []v1beta1.MigrationEngine{v1beta1.MigrationEngineRPGMigrate, testUnrecognizedEngine} {
		for _, state := range states {
			t.Run(string(engine)+"/"+state.name, func(t *testing.T) {
				m := followPasswordMigration()
				m.UID, m.Generation, m.Spec.Engine = testDispatchUID, 7, engine
				state.init(m)
				objects := []client.Object{
					m,
					namedJob(workerJob),
					&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "m-work", Namespace: "ns"}},
					&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "m-filters", Namespace: "ns"}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: workerPodName, Namespace: "ns"}},
				}
				r := failingReconciler(t, interceptor.Funcs{}, objects...)
				existing := objects[1:]
				stored := r.Client
				before := make([]client.Object, len(existing))
				for i, obj := range existing {
					before[i] = obj.DeepCopyObject().(client.Object)
					if err := stored.Get(context.Background(), client.ObjectKeyFromObject(obj), before[i]); err != nil {
						t.Fatal(err)
					}
				}
				installFailureHooks(t, r, engineDispatchNoEffects(t))
				r.APIReader = r.Client
				sentinel, progress, logs := &fakeSentinel{}, &fakeProgress{}, &fakeLogs{}
				r.Sentinel, r.Progress, r.Logs = sentinel, progress, logs
				r.Sampler = heldSampler(progress, migrationRequest(m).NamespacedName, workerJob)
				for range 2 {
					res, err := r.Reconcile(context.Background(), migrationRequest(m))
					if err != nil || res != (ctrl.Result{}) {
						t.Fatalf("reconcile = %+v, %v, want persisted rejection without retry", res, err)
					}
				}
				got := &v1beta1.Migration{}
				if err := stored.Get(context.Background(), client.ObjectKeyFromObject(m), got); err != nil {
					t.Fatal(err)
				}
				assertEngineDispatchRejection(t, got)
				if got.Status.Phase != v1beta1.PhaseFailed || got.Status.ObservedGeneration != 7 ||
					got.Status.Attempts != m.Status.Attempts || got.Status.JobName != m.Status.JobName ||
					!apiequality.Semantic.DeepEqual(got.Spec, m.Spec) || !reflect.DeepEqual(got.Finalizers, m.Finalizers) ||
					!reflect.DeepEqual(got.Status.VerificationRehearsal, m.Status.VerificationRehearsal) {
					t.Fatalf("rejection changed engine, ownership, worker or rehearsal state: %+v", got)
				}
				if sentinel.callCount() != 0 || progress.counts() != 0 || logs.calls != 0 || r.Sampler.running(migrationRequest(m).NamespacedName) {
					t.Fatal("rejected engine reached SQL/exec/log sampling or retained a sampler")
				}
				for i, obj := range existing {
					gotObject := obj.DeepCopyObject().(client.Object)
					if err := stored.Get(context.Background(), client.ObjectKeyFromObject(obj), gotObject); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before[i], gotObject) {
						t.Errorf("existing %T changed", obj)
					}
				}
			})
		}
	}
}

func assertEngineDispatchRejection(t *testing.T, got *v1beta1.Migration) {
	t.Helper()
	for _, want := range []struct {
		typ    string
		status metav1.ConditionStatus
	}{{v1beta1.ConditionValidated, metav1.ConditionFalse}, {v1beta1.ConditionFailed, metav1.ConditionTrue}} {
		c := meta.FindStatusCondition(got.Status.Conditions, want.typ)
		if c == nil || c.Status != want.status || c.Reason != testUnsupportedEngine || c.Message == "" || c.ObservedGeneration != 7 {
			t.Errorf("%s = %+v, want %s/UnsupportedEngine at generation 7 with explanation", want.typ, c, want.status)
		}
		if c != nil && got.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && !strings.Contains(c.Message, "not qualified for this runner image") {
			t.Errorf("native rejection does not explain unavailable backend: %q", c.Message)
		}
	}
}

func engineDispatchNoEffects(t *testing.T) interceptor.Funcs {
	t.Helper()
	blocked := func(verb string, obj any) error {
		t.Errorf("rejected engine reached %s %T", verb, obj)
		return errBoom
	}
	return interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1beta1.Migration); !ok {
				return blocked("Get", obj)
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		List: func(_ context.Context, _ client.WithWatch, obj client.ObjectList, _ ...client.ListOption) error {
			return blocked("List", obj)
		},
		Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
			return blocked("Create", obj)
		},
		Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteOption) error {
			return blocked("Delete", obj)
		},
		DeleteAllOf: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteAllOfOption) error {
			return blocked("DeleteAllOf", obj)
		},
		Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
			return blocked("Update", obj)
		},
		Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
			return blocked("Patch", obj)
		},
	}
}

func TestReconcileEngineDispatchPreservesLegacy(t *testing.T) {
	for _, engine := range []v1beta1.MigrationEngine{"", v1beta1.MigrationEnginePGCopyDB} {
		t.Run(string(engine), func(t *testing.T) {
			m := passwordMigration()
			m.UID, m.Spec.Engine = testDispatchUID, engine
			r := failingReconciler(t, interceptor.Funcs{}, m, completeJob(preflightJobName(m)))
			for range 2 {
				if _, err := r.Reconcile(context.Background(), migrationRequest(m)); err != nil {
					t.Fatal(err)
				}
			}
			got := &v1beta1.Migration{}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(m), got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Attempts != 1 || got.Status.JobName != workerJob || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionFailed) {
				t.Fatalf("legacy dispatch did not start worker: %+v", got.Status)
			}
			job := &batchv1.Job{}
			if err := r.Get(context.Background(), client.ObjectKey{Namespace: m.Namespace, Name: workerJob}, job); err != nil {
				t.Fatal(err)
			}
			container := job.Spec.Template.Spec.Containers[0]
			if len(container.Command) == 0 || container.Command[len(container.Command)-1] != workerContainer || len(container.Args) == 0 || container.Args[0] != "clone" {
				t.Fatal("legacy worker lost pgcopydb clone command")
			}
			claims := &corev1.PersistentVolumeClaimList{}
			if err := r.List(context.Background(), claims); err != nil || len(claims.Items) != 1 {
				t.Fatalf("legacy PVCs = %d, %v, want 1", len(claims.Items), err)
			}
		})
	}
}

func TestReconcileEngineDispatchStatusFailure(t *testing.T) {
	for _, engine := range []v1beta1.MigrationEngine{v1beta1.MigrationEngineRPGMigrate, testUnrecognizedEngine} {
		t.Run(string(engine), func(t *testing.T) {
			m := passwordMigration()
			m.Spec.Engine = engine
			m.Status.Phase = v1beta1.PhasePending
			r := failingReconciler(t, failStatusPatch(), m)
			installFailureHooks(t, r, engineDispatchNoEffects(t))
			if _, err := r.Reconcile(context.Background(), migrationRequest(m)); !errors.Is(err, errBoom) {
				t.Fatalf("status failure = %v, want propagated patch failure", err)
			}
		})
	}
}

const (
	qualifiedRunnerARM64 = "ghcr.io/ydixken/rpgmigrate@sha256:0022bf926b8b128b49330d4cb33b580abb38b09cd279ca04992879827ba0804b"
	qualifiedRunnerAMD64 = "ghcr.io/ydixken/rpgmigrate@sha256:a12cc8561c5a0bb7d4d19a7f69c0f37ae5004a274a193c6b24b78f6027501e1b"
)

func TestReconcileNativeRunnerTupleRefusals(t *testing.T) {
	for _, tc := range []struct{ name, flag, override string }{
		{"empty flag", "", ""},
		{"tag only", "ghcr.io/ydixken/rpgmigrate:dev-908ada6", ""},
		{"unqualified digest", "ghcr.io/ydixken/rpgmigrate@sha256:" + strings.Repeat("a", 64), ""},
		{"qualified digest, tag and digest", "ghcr.io/ydixken/rpgmigrate:dev@sha256:0022bf926b8b128b49330d4cb33b580abb38b09cd279ca04992879827ba0804b", ""},
		{"qualified digest, noncanonical repository", "GHCR.io/ydixken/rpgmigrate@sha256:0022bf926b8b128b49330d4cb33b580abb38b09cd279ca04992879827ba0804b", ""},
		{"qualified digest, extra suffix", qualifiedRunnerARM64 + "0", ""},
		{"unqualified per-Migration override", qualifiedRunnerARM64, "ghcr.io/ydixken/rpgmigrate:dev-908ada6"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := passwordMigration()
			m.UID, m.Generation, m.Spec.Engine, m.Spec.Runner.Image = testDispatchUID, 7, v1beta1.MigrationEngineRPGMigrate, tc.override
			r := failingReconciler(t, interceptor.Funcs{}, m)
			r.RPGMigrateRunnerImage = tc.flag
			installFailureHooks(t, r, engineDispatchNoEffects(t))
			if res, err := r.Reconcile(context.Background(), migrationRequest(m)); err != nil || res != (ctrl.Result{}) {
				t.Fatalf("reconcile = %+v, %v, want persisted rejection", res, err)
			}
			got := &v1beta1.Migration{}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(m), got); err != nil {
				t.Fatal(err)
			}
			assertEngineDispatchRejection(t, got)
		})
	}
}

func TestReconcileNativeRunnerTupleAdmits(t *testing.T) {
	for _, tc := range []struct{ name, flag, override string }{
		{"arm64 flag", qualifiedRunnerARM64, ""},
		{"amd64 flag", qualifiedRunnerAMD64, ""},
		{"qualified per-Migration override", "", qualifiedRunnerAMD64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := passwordMigration()
			m.UID, m.Generation, m.Spec.Engine, m.Spec.Runner.Image = testDispatchUID, 7, v1beta1.MigrationEngineRPGMigrate, tc.override
			r := failingReconciler(t, interceptor.Funcs{}, m)
			r.APIReader, r.RPGMigrateRunnerImage = r.Client, tc.flag
			if _, err := r.Reconcile(context.Background(), migrationRequest(m)); err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.Migration{}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(m), got); err != nil {
				t.Fatal(err)
			}
			if c := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ConditionValidated); c != nil && c.Reason == testUnsupportedEngine {
				t.Fatalf("qualified runner refused: %+v", c)
			}
			if meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionFailed) || got.Status.Phase != v1beta1.PhasePending {
				t.Fatalf("qualified runner did not reach the native route: %+v", got.Status)
			}
		})
	}
}
