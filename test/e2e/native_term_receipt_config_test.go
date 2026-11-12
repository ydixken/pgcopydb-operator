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
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

func nativeTermObject(t *testing.T, obj any) map[string]any {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// nativeTermFixture is a passing suspend receipt built from typed API objects, so the
// Go field names are checked against the Python checker rather than restated.
func nativeTermFixture(t *testing.T, exitCode int32) nativeTermReceipt {
	t.Helper()
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	controller := func(kind, uid string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{Kind: kind, UID: types.UID(uid), Controller: ptr.To(true)}}
	}
	m := &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{UID: "mig-1", Finalizers: []string{"pgcopydb-operator.io/cleanup"}},
		Status: v1beta1.MigrationStatus{JobName: "m-run-1", NativeJobUID: "job-1", NativePVCUID: "pvc-1", Attempts: 1,
			Replication: &v1beta1.ReplicationStatus{Endpos: "0/16B3748"}}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{UID: "job-1", OwnerReferences: controller("Migration", "mig-1")}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{UID: "pvc-1",
		OwnerReferences: controller("Migration", "mig-1")}}
	status := func(state corev1.ContainerState) []corev1.ContainerStatus {
		return []corev1.ContainerStatus{{Name: publicationRetryWorkerContainer, ContainerID: "containerd://one",
			ImageID: "runner@" + digest, State: state}}
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod-1", OwnerReferences: controller("Job", "job-1")},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: status(corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{}})}}
	stopped := pod.DeepCopy()
	stopped.Status = corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: status(corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode,
			FinishedAt: metav1.NewTime(time.Date(2026, 11, 13, 10, 0, 20, 0, time.UTC))}})}
	intent := m.DeepCopy()
	intent.Spec.Suspend = true
	suspended := intent.DeepCopy()
	suspended.Status.Phase = v1beta1.PhaseSuspended
	failed := job.DeepCopy()
	failed.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	foreign := []nativeTermRef{{Kind: "ConfigMap", Namespace: nsE2E, Name: "sentinel", UID: "cm-1", ResourceVersion: "7"}}
	r := nativeTermReceipt{Schema: nativeTermSchema, Case: "suspend", RunnerImage: "runner@" + digest,
		RequestedAt: "2026-11-13T10:00:00Z",
		Before: map[string]any{nativeTermMigration: nativeTermObject(t, m), nativeTermJob: nativeTermObject(t, job),
			nativeTermPod: nativeTermObject(t, pod), nativeTermPVC: nativeTermObject(t, pvc)},
		Terminated: map[string]any{nativeTermMigration: nativeTermObject(t, intent),
			nativeTermJob: nativeTermObject(t, job), nativeTermPod: nativeTermObject(t, stopped)},
		After: map[string]any{nativeTermMigration: nativeTermObject(t, suspended),
			nativeTermJob: nativeTermObject(t, failed), nativeTermPod: nativeTermObject(t, stopped),
			nativeTermPVC: nativeTermObject(t, pvc)}}
	r.Foreign.Before, r.Foreign.After = foreign, foreign
	return r
}

func TestNativeTermReceiptMatchesChecker(t *testing.T) {
	dir := t.TempDir()
	if verdict, err := writeNativeTermReceipt(context.Background(), filepath.Join(dir, "pass.json"),
		nativeTermFixture(t, 4)); err != nil {
		t.Fatalf("the checker refused a handled TERM: %v %s", err, verdict)
	}
	// 143 is a signal death, which the checker must refuse.
	verdict, err := writeNativeTermReceipt(context.Background(), filepath.Join(dir, "signal.json"),
		nativeTermFixture(t, 143))
	if err == nil || !strings.Contains(verdict, "term-engine-exit") {
		t.Fatalf("the checker accepted a signal death: %v %s", err, verdict)
	}
	if _, err := writeNativeTermReceipt(context.Background(), filepath.Join(dir, "pass.json"),
		nativeTermFixture(t, 4)); err == nil {
		t.Fatal("an existing receipt was overwritten")
	}
}

func TestNativeTermForeignSkipsOwned(t *testing.T) {
	item := func(name string, owners ...types.UID) unstructured.Unstructured {
		u := unstructured.Unstructured{}
		u.SetName(name)
		u.SetNamespace(nsE2E)
		u.SetUID(types.UID(name + "-uid"))
		u.SetResourceVersion("3")
		for _, owner := range owners {
			u.SetOwnerReferences(append(u.GetOwnerReferences(), metav1.OwnerReference{UID: owner}))
		}
		return u
	}
	got := nativeTermForeign("Job", "mig-1", []unstructured.Unstructured{item("owned", "other", "mig-1"),
		item("unowned", "other"), item("bare")})
	want := []nativeTermRef{{Kind: "Job", Namespace: nsE2E, Name: "unowned", UID: "unowned-uid", ResourceVersion: "3"},
		{Kind: "Job", Namespace: nsE2E, Name: "bare", UID: "bare-uid", ResourceVersion: "3"}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("foreign inventory = %+v, want %+v", got, want)
	}
}
