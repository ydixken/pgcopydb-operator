package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	finalizationClearCase                            = "clear historical receipt"
	finalizationReplaceCase                          = "replace historical receipt"
	finalizationOtherWorker                          = "other-history-worker"
	finalizationDeleteRaceCase                       = "delete Job race"
	finalizationLiveMarkerCase                       = "live marker changed"
	finalizationMismatchCase                         = "clone checked mismatch"
	finalizationWorkerReceipt                        = "recorded-worker"
	finalizationPVCReceipt                           = "recorded-pvc"
	finalizationForeignPodAfter                      = "foreign Pod after Job delete"
	finalizationCloneUnknownCase                     = "clone unknown"
	finalizationForeignJobCase                       = "foreign Job UID"
	finalizationForeignPodCase                       = "foreign Pod owner"
	finalizationFollowUnknownCase                    = "follow unknown"
	finalizationFollowConflictCase                   = "follow conflict"
	finalizationPodCase                              = "-pod"
	finalizationPartialRestartCase                   = "partial restart"
	finalizationUnknownReplyCase                     = "unknown reply"
	finalizationHistoricalForeignOwnerCase           = "historical foreign owner"
	finalizationNonterminalHelperCase                = "nonterminal helper"
	finalizationMigrationChangesBeforeNextDeleteCase = "migration changes before next delete"
	finalizationMigrationChangesAfterClosureCase     = "migration changes after closure"
	finalizationNonterminalWorkerCase                = "nonterminal historical worker"
)

func finalizationShape(m *v1beta1.Migration) {
	m.Status.NativeFinalizedJobUID = m.Status.NativeJobUID
	m.Status.Phase = v1beta1.PhaseCompleted
	metaTime := metav1.Now()
	m.Status.CompletedAt = &metaTime
	m.Status.Conditions = []metav1.Condition{
		{Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "FinalizedHistory", LastTransitionTime: metaTime},
		{Type: v1beta1.ConditionFailed, Status: metav1.ConditionFalse, Reason: "FinalizedHistory", LastTransitionTime: metaTime},
	}
}

func TestNativeFinalizationStatusTransition(t *testing.T) {
	testNativeFinalizationTransitionShapes(t)
	testNativeFinalizationLiveMarker(t)
	for _, mode := range []string{"successful returned resourceVersion", finalizationClearCase, finalizationReplaceCase, "pending assignment", "wrong worker assignment", finalizationLiveMarkerCase, "conflict does not mutate", "lost reply does not mutate"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeControlFixture(t)
			if mode == finalizationClearCase || mode == finalizationReplaceCase {
				finalizationShape(f.m)
				if err := f.store.Status().Update(context.Background(), f.m); err != nil {
					t.Fatal(err)
				}
			}
			base := f.current()
			desired := base.DeepCopy()
			finalizationShape(desired)
			switch mode {
			case finalizationClearCase:
				desired.Status.NativeFinalizedJobUID = ""
			case finalizationReplaceCase, "wrong worker assignment":
				desired.Status.NativeFinalizedJobUID = finalizationOtherWorker
			case "pending assignment":
				desired.Status.Phase = v1beta1.PhaseVerifying
			case finalizationLiveMarkerCase:
				f.beforeGet = func(obj client.Object) {
					if _, ok := obj.(*v1beta1.Migration); !ok {
						return
					}
					live := base.DeepCopy()
					live.Status.NativeFinalizedJobUID = finalizationOtherWorker
					if err := f.store.Status().Update(context.Background(), live); err != nil {
						t.Fatal(err)
					}
					f.beforeGet = nil
				}
			case "conflict does not mutate":
				f.patch = func(context.Context, client.Client, client.Object, client.Patch) error {
					return nativeControlConflict()
				}
			case "lost reply does not mutate":
				f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
					if err := c.Status().Patch(ctx, obj, p); err != nil {
						return err
					}
					return errors.New("lost reply")
				}
			}
			before := desired.DeepCopy()
			err := f.r.updateNativeStatus(context.Background(), desired, base)
			if mode == "successful returned resourceVersion" {
				live := f.current()
				if err != nil || desired.ResourceVersion == base.ResourceVersion || desired.ResourceVersion != live.ResourceVersion || !apiequality.Semantic.DeepEqual(desired.Status, live.Status) {
					t.Fatalf("successful status reply not propagated: err=%v desiredRV=%s liveRV=%s", err, desired.ResourceVersion, live.ResourceVersion)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid receipt transition accepted")
			}
			if !apiequality.Semantic.DeepEqual(desired, before) {
				t.Fatal("failed patch changed caller object")
			}
			if mode == finalizationLiveMarkerCase && !apierrors.IsConflict(err) {
				t.Fatalf("live marker race did not conflict: %v", err)
			}
		})
	}
}

func TestNativeFinalizationReceiptWriters(t *testing.T) {
	testNativeFinalizationWriterClosure(t)
	for _, mode := range []string{nativeTerminalCloneCommand, finalizationMismatchCase, finalizationCloneUnknownCase, "clone conflict", probeFollowCase, finalizationFollowUnknownCase, finalizationFollowConflictCase} {
		t.Run(mode, func(t *testing.T) {
			if mode == probeFollowCase || mode == finalizationFollowUnknownCase || mode == finalizationFollowConflictCase {
				f := newNativeFinishFixture(t)
				f.cleaned = true
				if mode == finalizationFollowUnknownCase {
					f.editResult = func(kind string, v map[string]any) {
						if kind == finishCleanedEvidenceKind {
							v["target_closed"] = false
						}
					}
				}
				if mode == finalizationFollowConflictCase {
					f.patch = func(context.Context, client.Client, client.Object, client.Patch) error {
						return nativeControlConflict()
					}
				}
				_, err := nativeFinish(t, f)
				got := f.current()
				if mode == probeFollowCase {
					if err != nil || got.Status.NativeFinalizedJobUID != string(f.worker.UID) {
						t.Fatalf("real follow completion lacks receipt: %v", err)
					}
				} else if err == nil || got.Status.NativeFinalizedJobUID != "" {
					t.Fatal("unknown/conflicted follow gained receipt")
				}
				return
			}
			var opts *v1beta1.VerificationOptions
			if mode == finalizationMismatchCase || mode == finalizationCloneUnknownCase {
				opts = &v1beta1.VerificationOptions{Schema: true}
			}
			f := newNativeCloneFixture(t, false, opts)
			if opts != nil {
				_, pod := nativeInstallCompare(t, f, compareSchema, false)
				if mode == finalizationCloneUnknownCase {
					f.outputs[pod.Name] = []byte("unknown\n")
				}
			}
			if mode == "clone conflict" {
				f.patch = func(context.Context, client.Client, client.Object, client.Patch) error {
					return nativeControlConflict()
				}
			}
			_, err := nativeCloneFinish(t, f)
			got := f.current()
			if mode == nativeTerminalCloneCommand || mode == finalizationMismatchCase {
				if err != nil || got.Status.NativeFinalizedJobUID != string(f.worker.UID) {
					t.Fatalf("real clone completion lacks receipt: %v", err)
				}
			} else if err == nil || got.Status.NativeFinalizedJobUID != "" {
				t.Fatal("unknown/conflicted clone gained receipt")
			}
		})
	}
}

func TestNativeFinalizationRecorded(t *testing.T) {
	m, _ := nativeIdentityObjects()
	m.Generation = 1
	m.Status.Attempts = 1
	m.Status.JobName = jobName(m, 1)
	m.Status.NativeJobUID = finalizationWorkerReceipt
	m.Status.NativePVCUID = finalizationPVCReceipt
	finalizationShape(m)
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.Migration)
		want bool
	}{
		{"exact history", func(*v1beta1.Migration) {}, true},
		{"later controls", func(x *v1beta1.Migration) {
			x.Generation++
			x.Spec.Suspend = true
			x.Spec.Cutover.Approved = !x.Spec.Cutover.Approved
			x.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
			x.Spec.Runner.Image = "later-image"
			x.Spec.Target.SynchronousCommit = "on"
		}, true},
		{"missing marker", func(x *v1beta1.Migration) { x.Status.NativeFinalizedJobUID = "" }, false},
		{"other marker", func(x *v1beta1.Migration) { x.Status.NativeFinalizedJobUID = finalizationOtherWorker }, false},
		{"missing migration UID", func(x *v1beta1.Migration) { x.UID = "" }, false},
		{"missing generation", func(x *v1beta1.Migration) { x.Generation = 0 }, false},
		{"no attempt", func(x *v1beta1.Migration) { x.Status.Attempts = 0 }, false},
		{"other job name", func(x *v1beta1.Migration) { x.Status.JobName = "historical-foreign-helper" }, false},
		{"invalid worker receipt", func(x *v1beta1.Migration) {
			x.Status.NativeJobUID = "worker/invalid"
			x.Status.NativeFinalizedJobUID = x.Status.NativeJobUID
		}, false},
		{"invalid pvc receipt", func(x *v1beta1.Migration) { x.Status.NativePVCUID = "pvc/invalid" }, false},
		{"incomplete", func(x *v1beta1.Migration) { x.Status.Conditions[0].Status = metav1.ConditionFalse }, false},
		{"missing complete", func(x *v1beta1.Migration) { x.Status.Conditions = x.Status.Conditions[1:] }, false},
		{"true Failed condition", func(x *v1beta1.Migration) { x.Status.Conditions[1].Status = metav1.ConditionTrue }, false},
		{"missing failed", func(x *v1beta1.Migration) { x.Status.Conditions = x.Status.Conditions[:1] }, false},
		{"wrong phase", func(x *v1beta1.Migration) { x.Status.Phase = v1beta1.PhaseVerifying }, false},
		{"legacy engine ignored", func(x *v1beta1.Migration) { x.Spec.Engine = v1beta1.MigrationEnginePGCopyDB }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := m.DeepCopy()
			tc.edit(x)
			if got := nativeFinalizationRecorded(x); got != tc.want {
				t.Fatalf("historical receipt=%v want %v", got, tc.want)
			}
		})
	}
	if nativeFinalizationRecorded(nil) {
		t.Fatal("nil receipt accepted")
	}
}

type finalizedCleanupReader struct {
	client.Reader
	beforeGet func(client.Object)
}

func (r finalizedCleanupReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *v1beta1.Migration, *batchv1.Job:
	default:
		return errors.New("unexpected historical resource read")
	}
	if r.beforeGet != nil {
		r.beforeGet(obj)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}
func (r finalizedCleanupReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	pods, ok := list.(*corev1.PodList)
	if !ok {
		return errors.New("unexpected historical list")
	}
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	pods.ResourceVersion = "historical-pod-snapshot"
	return nil
}

type finalizedCleanupFixture struct {
	t                         *testing.T
	r                         *MigrationReconciler
	store                     client.WithWatch
	m                         *v1beta1.Migration
	deletes                   []string
	retainPods, unknownDelete bool
	stopBeforeDelete          bool
	deleteNotFound            bool
	beforeDelete              func(*batchv1.Job)
}

func newFinalizedCleanupFixture(t *testing.T, checks ...string) *finalizedCleanupFixture {
	t.Helper()
	m, _ := nativeIdentityObjects()
	m.Generation = 1
	m.Status.Attempts = 1
	m.Status.JobName = jobName(m, 1)
	m.Status.NativeJobUID = finalizationWorkerReceipt
	m.Status.NativePVCUID = finalizationPVCReceipt
	finalizationShape(m)
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	objects := make([]client.Object, 0, 1+2*len(checks))
	objects = append(objects, m)
	for _, check := range checks {
		uid := types.UID("recorded-" + check)
		if check == compareSchema {
			m.Status.NativeSchemaJobUID = string(uid)
		} else {
			m.Status.NativeDataJobUID = string(uid)
		}
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: compareJobName(m, check), Namespace: m.Namespace, UID: uid}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
		if err := controllerutil.SetControllerReference(m, job, scheme); err != nil {
			t.Fatal(err)
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: job.Name + finalizationPodCase, Namespace: m.Namespace, UID: types.UID("pod-" + check), OwnerReferences: []metav1.OwnerReference{{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: lifecycleJobKind, Name: job.Name, UID: uid, Controller: ptr.To(true)}}}}
		objects = append(objects, job, pod)
	}
	store := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1beta1.Migration{}, &batchv1.Job{}).WithObjects(objects...).Build()
	if err := store.Get(context.Background(), client.ObjectKeyFromObject(m), m); err != nil {
		t.Fatal(err)
	}
	f := &finalizedCleanupFixture{t: t, store: store, m: m}
	f.r = &MigrationReconciler{APIReader: finalizedCleanupReader{Reader: store}}
	f.r.Client = interceptor.NewClient(store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Error("cached historical read")
			return errBoom
		},
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			t.Error("historical cleanup created resource")
			return errBoom
		},
		Delete: f.delete,
	})
	return f
}
func (f *finalizedCleanupFixture) delete(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		f.t.Fatal("historical cleanup deleted non-Job")
	}
	options := &client.DeleteOptions{}
	options.ApplyOptions(opts)
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != job.UID || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != job.ResourceVersion || options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
		f.t.Error("historical deletion lacks exact fresh UID/RV/foreground")
		return errBoom
	}
	if _, ok := ctx.Deadline(); !ok {
		f.t.Error("historical cleanup has no deadline")
		return errBoom
	}
	if f.beforeDelete != nil {
		f.beforeDelete(job)
	}
	if f.stopBeforeDelete {
		return errors.New("delete not attempted")
	}
	if err := c.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	f.deletes = append(f.deletes, job.Name)
	if !f.retainPods {
		pods := &corev1.PodList{}
		if err := c.List(ctx, pods); err != nil {
			return err
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if owner := metav1.GetControllerOf(pod); owner != nil && owner.UID == job.UID {
				if err := c.Delete(ctx, pod); err != nil {
					return err
				}
			}
		}
	}
	if f.deleteNotFound {
		return apierrors.NewNotFound(batchv1.Resource("jobs"), job.Name)
	}
	if f.unknownDelete {
		return errors.New("unknown delete reply")
	}
	return nil
}
func (f *finalizedCleanupFixture) save() {
	f.t.Helper()
	if err := f.store.Update(context.Background(), f.m); err != nil {
		f.t.Fatal(err)
	}
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		f.t.Fatal(err)
	}
}

func TestNativeFinalizedHelperCleanup(t *testing.T) {
	testNativeFinalizedCleanupBoundaries(t)
	for _, checks := range [][]string{nil, {compareSchema}, {compareData}, {compareSchema, compareData}} {
		t.Run(fmt.Sprint(checks), func(t *testing.T) {
			f := newFinalizedCleanupFixture(t, checks...)
			before := f.m.DeepCopy()
			if err := f.r.disposeNativeFinalizedVerificationHelpers(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if len(f.deletes) != len(checks) {
				t.Fatal("not every historical helper removed")
			}
			if !apiequality.Semantic.DeepEqual(before, f.m) {
				t.Fatal("historical cleanup changed caller")
			}
			if err := f.r.disposeNativeFinalizedVerificationHelpers(context.Background(), f.m); err != nil {
				t.Fatalf("already absent helpers not replayable: %v", err)
			}
		})
	}
	for _, mode := range []string{"suspended later generation", workerDeletingCase, finalizationPartialRestartCase, finalizationUnknownReplyCase, finalizationForeignJobCase, finalizationHistoricalForeignOwnerCase, finalizationNonterminalHelperCase, "owned Pod timeout", finalizationMigrationChangesBeforeNextDeleteCase, finalizationMigrationChangesAfterClosureCase, "fresh Job resourceVersion", finalizationDeleteRaceCase} {
		t.Run(mode, func(t *testing.T) { testNativeFinalizedCleanupFault(t, mode) })
	}
}

func testNativeFinalizedCleanupFault(t *testing.T, mode string) {
	t.Helper()
	f := newFinalizedCleanupFixture(t, compareSchema, compareData)
	key := client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema)}
	job := &batchv1.Job{}
	if err := f.store.Get(context.Background(), key, job); err != nil {
		t.Fatal(err)
	}
	wantErr := applyFinalizedCleanupFault(t, f, job, mode)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := f.r.disposeNativeFinalizedVerificationHelpers(ctx, f.m)
	if (err != nil) != wantErr {
		t.Fatalf("cleanup err=%v expected refusal=%v", err, wantErr)
	}
	if mode == finalizationMigrationChangesBeforeNextDeleteCase && len(f.deletes) != 1 {
		t.Fatal("changed Migration did not stop the next helper deletion")
	}
	if mode == finalizationMigrationChangesAfterClosureCase && len(f.deletes) != 2 {
		t.Fatal("postclosure readback was not exercised after the final helper")
	}
	if mode == finalizationPartialRestartCase || mode == finalizationUnknownReplyCase {
		f.unknownDelete = false
		f.stopBeforeDelete = false
		f.beforeDelete = nil
		if err := f.r.disposeNativeFinalizedVerificationHelpers(context.Background(), f.m); err != nil {
			t.Fatalf("known history cannot recover delete reply: %v", err)
		}
	}
	if mode == finalizationForeignJobCase || mode == finalizationHistoricalForeignOwnerCase || mode == finalizationNonterminalHelperCase || mode == finalizationDeleteRaceCase {
		if len(f.deletes) != 0 {
			t.Fatal("unqualified helper deleted")
		}
		if err := f.store.Get(context.Background(), key, &batchv1.Job{}); err != nil {
			t.Fatal("foreign or racing Job was not preserved")
		}
	}
}

func applyFinalizedCleanupFault(t *testing.T, f *finalizedCleanupFixture, job *batchv1.Job, mode string) bool {
	t.Helper()
	wantErr := true
	switch mode {
	case "suspended later generation":
		f.m.Generation++
		f.m.Spec.Suspend = true
		f.m.Spec.Verification = nil
		f.save()
		wantErr = false
	case workerDeletingCase:
		f.m.Finalizers = []string{"test-finalizer"}
		f.save()
		if err := f.store.Delete(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), f.m); err != nil {
			t.Fatal(err)
		}
		wantErr = false
	case finalizationPartialRestartCase:
		f.beforeDelete = func(j *batchv1.Job) {
			if j.Name == compareJobName(f.m, compareData) {
				f.beforeDelete = nil
				f.stopBeforeDelete = true
			}
		}
	case finalizationUnknownReplyCase:
		f.unknownDelete = true
	case finalizationForeignJobCase:
		job.UID = "historical-foreign-helper"
		if err := f.store.Update(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	case finalizationHistoricalForeignOwnerCase:
		job.OwnerReferences[0].UID = "foreign-migration"
		if err := f.store.Update(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	case finalizationNonterminalHelperCase:
		job.Status = batchv1.JobStatus{Active: 1}
		if err := f.store.Status().Update(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	case "owned Pod timeout":
		f.retainPods = true
	case finalizationMigrationChangesBeforeNextDeleteCase:
		reads := 0
		f.r.APIReader = finalizedCleanupReader{Reader: f.store, beforeGet: func(obj client.Object) {
			if _, ok := obj.(*v1beta1.Migration); !ok {
				return
			}
			reads++
			if reads != 4 {
				return
			}
			live := f.m.DeepCopy()
			live.Spec.Suspend = true
			if err := f.store.Update(context.Background(), live); err != nil {
				t.Fatal(err)
			}
		}}
	case finalizationMigrationChangesAfterClosureCase:
		f.beforeDelete = func(j *batchv1.Job) {
			if j.Name != compareJobName(f.m, compareData) {
				return
			}
			live := f.m.DeepCopy()
			live.Spec.Suspend = true
			if err := f.store.Update(context.Background(), live); err != nil {
				t.Fatal(err)
			}
		}

	case "fresh Job resourceVersion":
		job.Labels = map[string]string{"historical-change": "updated"}
		if err := f.store.Update(context.Background(), job); err != nil {
			t.Fatal(err)
		}
		wantErr = false
	case finalizationDeleteRaceCase:
		f.beforeDelete = func(j *batchv1.Job) {
			live := j.DeepCopy()
			live.Labels = map[string]string{"historical-race": "new"}
			if err := f.store.Update(context.Background(), live); err != nil {
				t.Fatal(err)
			}
		}
	}
	return wantErr
}

func testNativeFinalizationTransitionShapes(t *testing.T) {
	m, _ := nativeIdentityObjects()
	m.Generation = 1
	m.Status.Attempts = 1
	m.Status.JobName = jobName(m, 1)
	m.Status.NativeJobUID = finalizationWorkerReceipt
	m.Status.NativePVCUID = finalizationPVCReceipt
	for _, tc := range []struct {
		name                      string
		baseMarker, desiredMarker string
		edit                      func(*v1beta1.Migration)
		want                      bool
	}{
		{"unchanged empty", "", "", nil, true},
		{"unchanged historical", finalizationWorkerReceipt, finalizationWorkerReceipt, nil, true},
		{"unchanged incomplete receipt", finalizationWorkerReceipt, finalizationWorkerReceipt, func(x *v1beta1.Migration) { x.Status.Phase = v1beta1.PhaseCloning }, true},
		{"unchanged conflict receipt", finalizationOtherWorker, finalizationOtherWorker, nil, true},
		{"first final assignment", "", finalizationWorkerReceipt, nil, true},
		{"clear historical", finalizationWorkerReceipt, "", nil, false},
		{"replace historical", finalizationWorkerReceipt, finalizationOtherWorker, nil, false},
		{"wrong initial receipt", "", finalizationOtherWorker, nil, false},
		{"pending initial assignment", "", finalizationWorkerReceipt, func(x *v1beta1.Migration) { x.Status.Phase = v1beta1.PhasePending }, false},
		{"verifying initial assignment", "", finalizationWorkerReceipt, func(x *v1beta1.Migration) { x.Status.Phase = v1beta1.PhaseVerifying }, false},
		{"failed initial assignment", "", finalizationWorkerReceipt, func(x *v1beta1.Migration) { x.Status.Phase = v1beta1.PhaseFailed }, false},
		{"incomplete initial assignment", "", finalizationWorkerReceipt, func(x *v1beta1.Migration) { x.Status.Conditions[0].Status = metav1.ConditionFalse }, false},
		{"stale failed initial assignment", "", finalizationWorkerReceipt, func(x *v1beta1.Migration) { x.Status.Conditions[1].Status = metav1.ConditionTrue }, false},
	} {
		t.Run("shape/"+tc.name, func(t *testing.T) {
			base := m.DeepCopy()
			base.Status.NativeFinalizedJobUID = tc.baseMarker
			desired := m.DeepCopy()
			finalizationShape(desired)
			desired.Status.NativeFinalizedJobUID = tc.desiredMarker
			if tc.edit != nil {
				tc.edit(desired)
			}
			if got := nativeFinalizationStatusTransition(desired, base); got != tc.want {
				t.Fatalf("transition=%v want %v", got, tc.want)
			}
		})
	}
	if nativeFinalizationStatusTransition(nil, m) || nativeFinalizationStatusTransition(m, nil) {
		t.Fatal("nil transition accepted")
	}
}

type finalizedMutationReader struct {
	client.Reader
	mutate func(*v1beta1.Migration)
}

func (r finalizedMutationReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if m, ok := obj.(*v1beta1.Migration); ok {
		r.mutate(m)
	}
	return nil
}

func testNativeFinalizationLiveMarker(t *testing.T) {
	t.Run("live marker checked independently of RV", func(t *testing.T) {
		f := newNativeControlFixture(t)
		base := f.current()
		desired := base.DeepCopy()
		finalizationShape(desired)
		f.r.APIReader = finalizedMutationReader{Reader: f.store, mutate: func(m *v1beta1.Migration) { m.Status.NativeFinalizedJobUID = finalizationOtherWorker }}
		if err := f.r.updateNativeStatus(context.Background(), desired, base); !apierrors.IsConflict(err) || f.patches != 0 {
			t.Fatal("unmatched live marker reached ordinary status patch")
		}
	})
}

func testNativeFinalizationWriterClosure(t *testing.T) {
	for _, mode := range []string{"lost final reply", "changed postpatch readback", "verification pending"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true})
			if mode == "verification pending" {
				f.onCreate = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					obj.SetUID("pending-schema-helper")
					return c.Create(ctx, obj, opts...)
				}
				if _, err := nativeCloneFinish(t, f); err != nil || f.current().Status.NativeFinalizedJobUID != "" {
					t.Fatal("pending verification gained final receipt")
				}
				return
			}
			job, _ := nativeInstallCompare(t, f, compareSchema, true)
			f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
				desired := obj.(*v1beta1.Migration)
				if !nativeFinalizationRecorded(desired) {
					t.Fatal("final patch did not atomically carry strict receipt shape")
				}
				if err := c.Status().Patch(ctx, obj, p); err != nil {
					return err
				}
				if mode == "lost final reply" {
					return errors.New("lost status reply")
				}
				live := &v1beta1.Migration{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(desired), live); err != nil {
					return err
				}
				live.Spec.Suspend = true
				return c.Update(ctx, live)
			}
			if _, err := nativeCloneFinish(t, f); err == nil {
				t.Fatal("unknown final status reply/readback allowed cleanup")
			}
			if f.patches != 1 || !nativeFinalizationRecorded(f.current()) {
				t.Fatal("server final patch was not one atomic durable receipt")
			}
			if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(job), &batchv1.Job{}); err != nil {
				t.Fatal("comparison helper removed before positive exact final readback")
			}
		})
	}
	for _, fault := range []string{finishProbeActiveFault} {
		t.Run(fault, func(t *testing.T) {
			f := newNativeFinishFixture(t)
			f.fault = fault
			if _, err := nativeFinish(t, f); err == nil || f.current().Status.NativeFinalizedJobUID != "" {
				t.Fatal("unknown cleanup gained final receipt")
			}
		})
	}
}

func testNativeFinalizedCleanupBoundaries(t *testing.T) {
	testNativeFinalizedWorkerCleanup(t)
	t.Run("NotFound still waits for owned Pods", func(t *testing.T) {
		f := newFinalizedCleanupFixture(t, compareSchema)
		f.deleteNotFound = true
		f.retainPods = true
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		if err := f.r.disposeNativeFinalizedVerificationHelpers(ctx, f.m); err == nil {
			t.Fatal("Delete NotFound skipped owned Pod absence closure")
		}
	})
	t.Run("active preflight may remove running helper", func(t *testing.T) {
		f := newFinalizedCleanupFixture(t, compareSchema)
		f.m.Status.NativeFinalizedJobUID = ""
		j := &batchv1.Job{}
		if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema)}, j); err != nil {
			t.Fatal(err)
		}
		j.Status = batchv1.JobStatus{Active: 1}
		if err := f.store.Status().Update(context.Background(), j); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := f.r.nativePreflightCleanup(ctx, f.m, j, nil); err != nil {
			t.Fatal("historical-only terminal check changed active preflight cleanup", err)
		}
	})
	t.Run("active preflight retains Delete NotFound refusal", func(t *testing.T) {
		f := newFinalizedCleanupFixture(t, compareSchema)
		f.m.Status.NativeFinalizedJobUID = ""
		f.deleteNotFound = true
		j := &batchv1.Job{}
		if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema)}, j); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := f.r.nativePreflightCleanup(ctx, f.m, j, nil); err == nil {
			t.Fatal("historical-only Delete NotFound handling changed active preflight cleanup")
		}
	})

	t.Run("known Delete NotFound closes absence", func(t *testing.T) {
		f := newFinalizedCleanupFixture(t, compareSchema)
		f.deleteNotFound = true
		if err := f.r.disposeNativeFinalizedVerificationHelpers(context.Background(), f.m); err != nil {
			t.Fatalf("known absent helper Delete reply lost history cleanup: %v", err)
		}
	})

	t.Run("fresh RV belongs to re-read Job", func(t *testing.T) {
		f := newFinalizedCleanupFixture(t, compareSchema)
		stale := &batchv1.Job{}
		key := client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema)}
		if err := f.store.Get(context.Background(), key, stale); err != nil {
			t.Fatal(err)
		}
		current := stale.DeepCopy()
		current.Labels = map[string]string{"new-version": "revised"}
		if err := f.store.Update(context.Background(), current); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := f.r.nativePreflightCleanup(ctx, f.m, stale, nil); err != nil || len(f.deletes) != 1 {
			t.Fatal("shared cleanup used stale RV rather than checked fresh Job", err)
		}
	})
	for _, mode := range []string{finalizationForeignPodCase, finalizationForeignPodAfter, "canceled deletion context", "changed receipt same RV", "stale expected RV", "terminal helper drifts before delete"} {
		t.Run(mode, func(t *testing.T) { testNativeFinalizedCleanupBoundary(t, mode) })
	}
}

func testNativeFinalizedCleanupBoundary(t *testing.T, mode string) {
	t.Helper()
	f := newFinalizedCleanupFixture(t, compareSchema)
	replace := func() {
		pod := &corev1.Pod{}
		key := client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema) + finalizationPodCase}
		if err := f.store.Get(context.Background(), key, pod); err != nil {
			t.Fatal(err)
		}
		pod.UID = "historical-replacement-pod"
		pod.OwnerReferences[0].UID = "replacement-helper"
		if err := f.store.Update(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	switch mode {
	case finalizationForeignPodCase:
		replace()
	case finalizationForeignPodAfter:
		f.beforeDelete = func(*batchv1.Job) { replace() }
	case "canceled deletion context":
		cancel()
	case "changed receipt same RV":
		f.r.APIReader = finalizedMutationReader{Reader: finalizedCleanupReader{Reader: f.store}, mutate: func(m *v1beta1.Migration) { m.Status.NativeDataJobUID = "new-data-pin" }}
	case "terminal helper drifts before delete":
		reads := 0
		f.r.APIReader = finalizedCleanupReader{Reader: f.store, beforeGet: func(obj client.Object) {
			if _, ok := obj.(*batchv1.Job); !ok {
				return
			}
			reads++
			if reads != 2 {
				return
			}
			j := &batchv1.Job{}
			if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema)}, j); err != nil {
				t.Fatal(err)
			}
			j.Status = batchv1.JobStatus{Active: 1}
			if err := f.store.Status().Update(context.Background(), j); err != nil {
				t.Fatal(err)
			}
		}}
	case "stale expected RV":
		live := f.m.DeepCopy()
		live.Spec.Suspend = true
		if err := f.store.Update(context.Background(), live); err != nil {
			t.Fatal(err)
		}
	}
	err := f.r.disposeNativeFinalizedVerificationHelpers(ctx, f.m)
	foreignPod := mode == finalizationForeignPodCase || mode == finalizationForeignPodAfter
	if foreignPod && err != nil {
		t.Fatalf("foreign Job-UID Pod incorrectly blocks exact owned helper GC: %v", err)
	}
	if !foreignPod && err == nil {
		t.Fatal("unqualified cleanup reported success")
	}
	if !foreignPod && len(f.deletes) != 0 {
		t.Fatal("cleanup crossed changed Migration, cancellation or foreign Pod boundary")
	}
	if mode == finalizationForeignPodCase || mode == finalizationForeignPodAfter {
		pod := &corev1.Pod{}
		if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, compareSchema) + finalizationPodCase}, pod); err != nil || pod.UID != "historical-replacement-pod" {
			t.Fatal("foreign Pod was not preserved")
		}
	}
}

func testNativeFinalizedWorkerCleanup(t *testing.T) {
	for _, mode := range []string{"successful historical worker", finalizationNonterminalWorkerCase, "historical worker Delete NotFound"} {
		t.Run(mode, func(t *testing.T) {
			f := newFinalizedCleanupFixture(t)
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: f.m.Status.JobName, Namespace: f.m.Namespace, UID: types.UID(f.m.Status.NativeJobUID)}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
			if mode == finalizationNonterminalWorkerCase {
				job.Status = batchv1.JobStatus{Active: 1}
			}
			f.deleteNotFound = mode == "historical worker Delete NotFound"
			if err := controllerutil.SetControllerReference(f.m, job, f.store.Scheme()); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Create(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := f.r.nativePreflightCleanup(ctx, f.m, job, nil)
			if mode == finalizationNonterminalWorkerCase {
				if err == nil || len(f.deletes) != 0 {
					t.Fatal("historical original worker lost fresh terminal closure")
				}
				return
			}
			if err != nil || len(f.deletes) != 1 {
				t.Fatal("known historical worker absence could not close", err)
			}
		})
	}
}
