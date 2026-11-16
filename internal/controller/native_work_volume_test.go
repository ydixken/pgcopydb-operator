package controller

import (
	"context"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// R5: the follow claim is bound by label and carries no owner reference, so foreground deletion
// cannot remove the workdir native cleanup reads; a clone-only claim keeps its controller.
func TestNativeBuildWorkClaimBinding(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	m, _ := nativeIdentityObjects()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	follow, err := buildNativeWorkClaim(m, scheme)
	if err != nil || len(follow.OwnerReferences) != 0 || follow.Labels[native.WorkClaimMigrationUIDLabel] != string(m.UID) || follow.Labels[labelMigration] != m.Name {
		t.Fatalf("follow claim = %+v, %v", follow.ObjectMeta, err)
	}
	m.Spec.Follow = nil
	clone, err := buildNativeWorkClaim(m, scheme)
	if err != nil || metav1.GetControllerOf(clone) == nil || metav1.GetControllerOf(clone).UID != m.UID || clone.Labels[native.WorkClaimMigrationUIDLabel] != "" {
		t.Fatalf("clone-only claim = %+v, %v", clone.ObjectMeta, err)
	}
}

func TestNativeFollowWorkClaimIdentity(t *testing.T) {
	bound := func() (*v1beta1.Migration, *corev1.PersistentVolumeClaim) {
		m, pvc := nativeIdentityObjects()
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
		nativeBindTestClaim(m, pvc)
		return m, pvc
	}
	m, pvc := bound()
	if _, err := nativeWorkIdentity(m, pvc); err != nil {
		t.Fatalf("unowned labelled follow claim refused: %v", err)
	}
	m.Status.NativePVCUID = string(pvc.UID)
	if _, err := nativeWorkIdentity(m, pvc); err != nil {
		t.Fatalf("recorded follow claim refused: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.Migration, *corev1.PersistentVolumeClaim)
	}{
		{"unlabelled", func(_ *v1beta1.Migration, p *corev1.PersistentVolumeClaim) {
			delete(p.Labels, native.WorkClaimMigrationUIDLabel)
		}},
		{"owner referenced", func(m *v1beta1.Migration, p *corev1.PersistentVolumeClaim) {
			p.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(m, v1beta1.GroupVersion.WithKind("Migration"))}
		}},
		{"another Migration's label", func(_ *v1beta1.Migration, p *corev1.PersistentVolumeClaim) {
			p.Labels[native.WorkClaimMigrationUIDLabel] = "11111111-2222-3333-4444-555555555555"
		}},
		{"another recorded claim", func(m *v1beta1.Migration, _ *corev1.PersistentVolumeClaim) { m.Status.NativePVCUID = "another-claim" }},
		{"deleting", func(_ *v1beta1.Migration, p *corev1.PersistentVolumeClaim) {
			now := metav1.Now()
			p.DeletionTimestamp = &now
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, pvc := bound()
			tc.change(m, pvc)
			if _, err := nativeWorkIdentity(m, pvc); err == nil {
				t.Fatal("follow claim identity admitted")
			}
		})
	}
}

// A follow Migration deleted before its first worker deletes the claim it created and releases.
func TestNativeAttemptZeroDeletionDeletesClaim(t *testing.T) {
	f := newWorkerAttemptFixture(t)
	ctx := context.Background()
	m := f.storedMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	m.Status = v1beta1.MigrationStatus{Phase: v1beta1.PhasePending, NativeRunnerImage: nativeEvidenceTestImage}
	controllerutil.AddFinalizer(m, finalizerName)
	if err := f.store.Update(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Status().Update(ctx, m); err != nil {
		t.Fatal(err)
	}
	f.updatePVC(func(pvc *corev1.PersistentVolumeClaim) {
		nativeBindTestClaim(m, pvc)
		pvc.Status.Phase = corev1.ClaimBound
	})
	if err := f.store.Delete(ctx, f.storedMigration()); err != nil {
		t.Fatal(err)
	}
	deletes := []types.UID{}
	inner, _ := f.r.Client.(client.WithWatch)
	f.r.Client = interceptorDeleteRecorder(inner, f.store, &deletes)
	if _, err := nativeDispatch(t, f.r, f.storedMigration()); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Get(ctx, client.ObjectKeyFromObject(f.pvc), &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) || len(deletes) != 1 || deletes[0] != f.pvc.UID {
		t.Fatalf("attempt-0 deletion left the unowned claim: %v deletes=%v", err, deletes)
	}
	if err := f.store.Get(ctx, client.ObjectKeyFromObject(m), &v1beta1.Migration{}); !apierrors.IsNotFound(err) {
		t.Fatal("attempt-0 deletion kept the finalizer", err)
	}
}

// A finalized follow Migration whose claim this Migration still controls predates R5: owner-reference GC removes it.
func TestNativeFinalizedDeletionLeavesOwnerReferencedClaim(t *testing.T) {
	f := newNativeHistoricalDeletionFixture(t)
	claim := f.pvc.DeepCopy()
	claim.Labels = nil
	claim.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(f.m, v1beta1.GroupVersion.WithKind("Migration"))}
	f.replace(claim)
	if result, err := f.reconcile(t); err != nil || result.RequeueAfter != 0 {
		t.Fatal("finalized deletion with a controlled claim did not finish", result, err)
	}
	for _, name := range f.deletes {
		if name == f.pvc.Name {
			t.Fatal("the operator deleted a claim owner-reference GC removes")
		}
	}
}

// interceptorDeleteRecorder records the UID of each delete and applies deletes and the finalizer patch to
// store, past a fixture that refuses both.
func interceptorDeleteRecorder(inner, store client.WithWatch, uids *[]types.UID) client.WithWatch {
	return interceptor.NewClient(inner, interceptor.Funcs{
		Delete: func(ctx context.Context, _ client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			*uids = append(*uids, obj.GetUID())
			return store.Delete(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, _ client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
			return store.Patch(ctx, obj, p, opts...)
		},
	})
}
