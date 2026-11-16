package native

import (
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	claimTestName = "claim-test"
	claimTestUID  = "claim-test-uid"
)

func TestWorkClaimBound(t *testing.T) {
	owner := admissionOwner("Migration", claimTestName, claimTestUID)
	other := admissionOwner("Migration", claimTestName, "other-uid")
	notController := owner
	notController.Controller = nil
	for _, tc := range []struct {
		name   string
		follow bool
		labels map[string]string
		owners []metav1.OwnerReference
		bound  bool
	}{
		{"follow labelled", true, map[string]string{WorkClaimMigrationUIDLabel: claimTestUID}, nil, true},
		{"follow owner referenced", true, map[string]string{WorkClaimMigrationUIDLabel: claimTestUID}, []metav1.OwnerReference{owner}, false},
		{"follow unlabelled", true, nil, nil, false},
		{"follow other migration", true, map[string]string{WorkClaimMigrationUIDLabel: "other-uid"}, nil, false},
		{"follow controlled only", true, nil, []metav1.OwnerReference{owner}, false},
		{"clone controlled", false, nil, []metav1.OwnerReference{owner}, true},
		{"clone labelled only", false, map[string]string{WorkClaimMigrationUIDLabel: claimTestUID}, nil, false},
		{"clone other controller", false, nil, []metav1.OwnerReference{other}, false},
		{"clone not controller", false, nil, []metav1.OwnerReference{notController}, false},
		{"clone second owner", false, nil, []metav1.OwnerReference{owner, notController}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: claimTestName, UID: claimTestUID}, Spec: v1beta1.MigrationSpec{Follow: &v1beta1.FollowOptions{Enabled: tc.follow}}}
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: claimTestName + "-work", Labels: tc.labels, OwnerReferences: tc.owners}}
			if got := WorkClaimBound(m, pvc); got != tc.bound {
				t.Fatalf("WorkClaimBound = %v, want %v", got, tc.bound)
			}
		})
	}
	if WorkClaimBound(&v1beta1.Migration{Spec: v1beta1.MigrationSpec{Follow: &v1beta1.FollowOptions{Enabled: true}}}, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{WorkClaimMigrationUIDLabel: ""}}}) {
		t.Fatal("a Migration without a UID bound an empty label")
	}
}
