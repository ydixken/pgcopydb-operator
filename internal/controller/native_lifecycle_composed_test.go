package controller

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// composedLifecycle drives one store through completion, replay and deletion via the dispatcher only.
type composedLifecycle struct {
	*nativeFinishFixture
	created, deleted []types.UID
	foreign          []client.Object
}

func newComposedLifecycle(t *testing.T) *composedLifecycle {
	t.Helper()
	l := &composedLifecycle{nativeFinishFixture: newNativeFinishFixture(t)}
	l.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
	l.m.Finalizers = []string{finalizerName}
	if err := l.store.Update(context.Background(), l.m); err != nil {
		t.Fatal(err)
	}
	other := types.UID("foreign-migration")
	owner := []metav1.OwnerReference{{APIVersion: v1beta1.GroupVersion.String(), Kind: cleanedOwnerKind, Name: "foreign-owner", UID: other, Controller: ptr.To(true)}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "foreign-native-worker", Namespace: l.m.Namespace, UID: "foreign-job", OwnerReferences: owner},
		Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foreign-native-worker-pod", Namespace: l.m.Namespace, UID: "foreign-pod",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: lifecycleJobKind, Name: job.Name, UID: job.UID, Controller: ptr.To(true)}}}}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "foreign-claim", Namespace: l.m.Namespace, UID: "foreign-pvc", OwnerReferences: owner}}
	for _, obj := range []client.Object{job, pod, claim} {
		if err := l.store.Create(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
		l.foreign = append(l.foreign, obj)
	}
	inner, ok := l.r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("fixture client cannot be wrapped")
	}
	l.r.Client = interceptor.NewClient(inner, interceptor.Funcs{Create: l.create, Delete: l.delete})
	return l
}

func (l *composedLifecycle) create(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	err := c.Create(ctx, obj, opts...)
	if obj.GetUID() != "" {
		l.created = append(l.created, obj.GetUID())
	}
	return err
}

// Every Delete must name the live UID and resourceVersion; the retained original goes only after finalized deletion.
func (l *composedLifecycle) delete(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	options := &client.DeleteOptions{}
	options.ApplyOptions(opts)
	live, ok := obj.DeepCopyObject().(client.Object)
	if !ok || l.store.Get(ctx, client.ObjectKeyFromObject(obj), live) != nil {
		l.t.Errorf("delete of unreadable %T %s", obj, obj.GetName())
		return errBoom
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != live.GetUID() ||
		options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != live.GetResourceVersion() {
		l.t.Errorf("delete of %s lacks the live UID/resourceVersion precondition", obj.GetName())
		return errBoom
	}
	l.deleted = append(l.deleted, live.GetUID())
	if live.GetUID() != l.worker.UID {
		return c.Delete(ctx, obj, opts...)
	}
	m := l.current()
	if m.DeletionTimestamp.IsZero() || !nativeFinalizationRecorded(m) {
		l.t.Error("retained original worker disposed before finalized deletion")
		return errBoom
	}
	if err := l.store.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	return l.store.Delete(ctx, l.pod)
}

func (l *composedLifecycle) dispatch(t *testing.T) (ctrl.Result, error) {
	t.Helper()
	l.m = l.current()
	return nativeDispatch(t, l.r, l.m)
}

func (l *composedLifecycle) assertForeignIntact(t *testing.T) {
	t.Helper()
	for _, want := range l.foreign {
		got, _ := want.DeepCopyObject().(client.Object)
		if err := l.store.Get(context.Background(), client.ObjectKeyFromObject(want), got); err != nil || got.GetUID() != want.GetUID() || got.GetResourceVersion() != want.GetResourceVersion() {
			t.Fatalf("foreign %s was mutated or removed: %v", want.GetName(), err)
		}
	}
	for _, uid := range l.deleted {
		for _, obj := range l.foreign {
			if uid == obj.GetUID() {
				t.Fatalf("foreign UID %s reached Delete", uid)
			}
		}
	}
}

func TestNativeComposedLifecycleExactUID(t *testing.T) {
	l := newComposedLifecycle(t)
	finished := l.complete(t)
	// The phases share one store, so they run in order rather than as independent subtests.
	l.replay(t, finished)
	l.refuseHelperReplacement(t, finished)
	if err := l.store.Delete(context.Background(), l.current()); err != nil {
		t.Fatal(err)
	}
	l.refuseWorkerReplacement(t)
	l.finalDelete(t)
	for _, uid := range l.deleted {
		if uid != l.worker.UID && (!slices.Contains(l.created, uid) || !strings.HasPrefix(string(uid), "finish-helper-")) {
			t.Fatalf("Delete reached UID %s that this lifecycle did not create", uid)
		}
	}
	l.assertForeignIntact(t)
}

func (l *composedLifecycle) complete(t *testing.T) *v1beta1.Migration {
	t.Helper()
	for pass := 0; !meta.IsStatusConditionTrue(l.current().Status.Conditions, v1beta1.ConditionComplete); pass++ {
		if pass == 4 {
			t.Fatalf("follow completion did not converge: steps=%v", l.steps)
		}
		if _, err := l.dispatch(t); err != nil {
			t.Fatalf("pass %d: %v steps=%v", pass, err, l.steps)
		}
	}
	finished := l.current()
	helper := types.UID(finished.Status.NativeSchemaJobUID)
	if !nativeFinalizationRecorded(finished) || finished.Status.NativeFinalizedJobUID != string(l.worker.UID) || helper == "" || !slices.Contains(l.created, helper) ||
		!slices.Contains(l.deleted, helper) || slices.Contains(l.deleted, l.worker.UID) || !l.cleaned || nativeControlEnd(finished) != lifecycleEnd {
		t.Fatalf("completion lacks finalized receipt, pinned helper disposal or retained worker: %+v deleted=%v", finished.Status, l.deleted)
	}
	if err := l.store.Get(context.Background(), client.ObjectKey{Namespace: l.m.Namespace, Name: compareJobName(l.m, compareSchema)}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatal("consumed verification helper survived completion", err)
	}
	l.assertForeignIntact(t)
	return finished
}

func (l *composedLifecycle) replay(t *testing.T, finished *v1beta1.Migration) {
	t.Helper()
	steps, deletes, patches, logs := len(l.steps), len(l.deleted), l.patches, l.logs
	for range 2 {
		if result, err := l.dispatch(t); err != nil || result != (ctrl.Result{}) {
			t.Fatal("replay of disposed history did not settle", result, err)
		}
	}
	if len(l.steps) != steps || len(l.deleted) != deletes || l.patches != patches || l.logs != logs || !apiequality.Semantic.DeepEqual(l.current().Status, finished.Status) {
		t.Fatalf("replay re-ran evidence, disposal or status: steps=%v deleted=%v", l.steps[steps:], l.deleted[deletes:])
	}
}

func (l *composedLifecycle) refuseHelperReplacement(t *testing.T, finished *v1beta1.Migration) {
	t.Helper()
	replacement := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: compareJobName(l.m, compareSchema), Namespace: l.m.Namespace, UID: "replacement-schema-helper"},
		Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	if err := controllerutil.SetControllerReference(l.m, replacement, l.r.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := l.store.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	deletes := len(l.deleted)
	if _, err := l.dispatch(t); err == nil || len(l.deleted) != deletes || !apiequality.Semantic.DeepEqual(l.current().Status, finished.Status) {
		t.Fatal("replacement under the pinned helper name was disposed or accepted", err)
	}
	live := &batchv1.Job{}
	if err := l.store.Get(context.Background(), client.ObjectKeyFromObject(replacement), live); err != nil || live.UID != replacement.UID {
		t.Fatal("replacement helper was removed", err)
	}
	if err := l.store.Delete(context.Background(), live); err != nil {
		t.Fatal(err)
	}
}

func (l *composedLifecycle) refuseWorkerReplacement(t *testing.T) {
	t.Helper()
	original := &batchv1.Job{}
	if err := l.store.Get(context.Background(), client.ObjectKeyFromObject(l.worker), original); err != nil {
		t.Fatal(err)
	}
	impostor := original.DeepCopy()
	impostor.UID = "replacement-worker"
	l.replace(impostor)
	deletes := len(l.deleted)
	if result, err := l.dispatch(t); err != nil || result.RequeueAfter <= 0 || len(l.deleted) != deletes || !controllerutil.ContainsFinalizer(l.current(), finalizerName) {
		t.Fatal("deletion disposed or released past a worker replaced under the recorded name", result, err)
	}
	if err := l.store.Get(context.Background(), client.ObjectKeyFromObject(impostor), impostor); err != nil || impostor.UID != "replacement-worker" {
		t.Fatal("replacement worker was removed", err)
	}
	l.replace(original)
}

func (l *composedLifecycle) finalDelete(t *testing.T) {
	t.Helper()
	deletes := len(l.deleted)
	if result, err := l.dispatch(t); err != nil || result != (ctrl.Result{}) {
		t.Fatal("finalized deletion did not settle", result, err)
	}
	if got := l.deleted[deletes:]; !slices.Equal(got, []types.UID{l.worker.UID}) {
		t.Fatalf("finalized deletion disposed %v, want only the recorded worker", got)
	}
	if err := l.store.Get(context.Background(), client.ObjectKeyFromObject(l.m), &v1beta1.Migration{}); !apierrors.IsNotFound(err) {
		t.Fatal("finalizer was not released", err)
	}
	claim := &corev1.PersistentVolumeClaim{}
	if err := l.store.Get(context.Background(), client.ObjectKeyFromObject(l.pvc), claim); err != nil || claim.UID != l.pvc.UID {
		t.Fatal("work PVC was deleted instead of left to owner GC", err)
	}
}

// The report parser accepts complete=false as partial progress, so each cleanup consumer must refuse it itself.
func TestNativeCleanupRequiresComplete(t *testing.T) {
	partial := func(raw []byte) []byte {
		if !bytes.Contains(raw, []byte(`"complete":true`)) {
			t.Fatalf("cleanup fixture lacks the complete flag: %s", raw)
		}
		return bytes.Replace(raw, []byte(`"complete":true`), []byte(`"complete":false`), 1)
	}
	t.Run("deletion cleanup", func(t *testing.T) {
		f := newNativeDispositionFixture(t, true)
		f.m.Finalizers = []string{finalizerName}
		if err := f.store.Update(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Delete(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		f.editCleanup = partial
		if err := f.reconcile(t); err == nil || !f.cleanupCalled {
			t.Fatal("partial deletion cleanup was accepted or never ran", err)
		}
		live := &v1beta1.Migration{}
		if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil || !controllerutil.ContainsFinalizer(live, finalizerName) {
			t.Fatal("partial deletion cleanup released the finalizer", err)
		}
	})
	t.Run("follow completion cleanup", func(t *testing.T) {
		f := newNativeFinishFixture(t)
		rewritten := 0
		f.afterEffect = func() {
			for name, raw := range f.outputs {
				if finishJobKind(name) == jobKindCleanup && bytes.Contains(raw, []byte(`"complete":true`)) {
					f.outputs[name] = partial(raw)
					rewritten++
				}
			}
		}
		if _, err := nativeFinish(t, f); err == nil || rewritten == 0 || f.patches != 0 || meta.IsStatusConditionTrue(f.current().Status.Conditions, v1beta1.ConditionComplete) {
			t.Fatal("partial completion cleanup published Complete or never ran", err)
		}
	})
}

func TestNativePlainCloneMissingReport(t *testing.T) {
	for name, raw := range map[string][]byte{
		"empty logs":           {},
		"ordinary log only":    []byte(`{"level":"info","error_severity":"INFO","event":"clone.done","message":"native clone done"}` + "\n"),
		"report without frame": []byte("native-report:\n"),
	} {
		t.Run(name, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, nil)
			f.raw = raw
			if _, err := nativeDispatch(t, f.r, f.m); err == nil || f.logs == 0 || f.patches != 0 {
				t.Fatal("plain clone without a complete report did not fail closed", err)
			}
			got := f.current()
			if got.Status.Phase == v1beta1.PhaseCompleted || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionComplete) || got.Status.NativeFinalizedJobUID != "" {
				t.Fatalf("missing report produced completion: %+v", got.Status)
			}
		})
	}
}
