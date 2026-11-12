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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The receipt format is the engine plan docs/superpowers/plans/2026-11-12-m6-native-term-runtime-receipt.md.
const (
	nativeTermSchema  = "rpgmigrate.native-term-receipt/v1"
	nativeTermChecker = "../../hack/native-term-receipt.py"
	// Receipt keys for the four objects the checker reads.
	nativeTermMigration = "migration"
	nativeTermJob       = "job"
	nativeTermPod       = "pod"
	nativeTermPVC       = "pvc"
)

// nativeTermForeignKinds are the namespace kinds inventoried as foreign; pods are left
// out because unrelated database pods change status during any window.
var nativeTermForeignKinds = []schema.GroupVersionKind{
	batchv1.SchemeGroupVersion.WithKind("Job"), corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"),
	corev1.SchemeGroupVersion.WithKind("ConfigMap"), corev1.SchemeGroupVersion.WithKind("Secret"),
}

// nativeTermPair is the one pair carrying TERM receipts: the TERM path does not depend on the major.
func nativeTermPair() bool { return pgSource == 16 && pgTarget == 16 }

type nativeTermRef struct {
	Kind            string `json:"kind"`
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
}

type nativeTermReceipt struct {
	Schema      string         `json:"schema"`
	Case        string         `json:"case"`
	RunnerImage string         `json:"runnerImage"`
	RequestedAt string         `json:"requestedAt"`
	Before      map[string]any `json:"before"`
	Terminated  map[string]any `json:"terminated"`
	After       map[string]any `json:"after"`
	Foreign     struct {
		Before []nativeTermRef `json:"before"`
		After  []nativeTermRef `json:"after"`
	} `json:"foreign"`
}

// nativeTermCapture records one TERM receipt for a running native follow worker.
type nativeTermCapture struct {
	receipt             nativeTermReceipt
	name, job, pod      string
	migration, sentinel types.UID
}

// beginNativeTerm reads the before state, or returns nil off the receipt pair.
func beginNativeTerm(kase, name string, sentinel types.UID) *nativeTermCapture {
	GinkgoHelper()
	if !nativeTermPair() {
		return nil
	}
	m := &v1beta1.Migration{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: name}, m)).To(Succeed())
	t := &nativeTermCapture{name: name, job: m.Status.JobName, pod: waitWorkerRunning(name, m.Status.Attempts),
		migration: m.UID, sentinel: sentinel}
	t.receipt = nativeTermReceipt{Schema: nativeTermSchema, Case: kase, RunnerImage: selectedRunnerImage(),
		Before: t.read(Default, nativeTermMigration, nativeTermJob, nativeTermPod, nativeTermPVC)}
	t.receipt.Foreign.Before = t.foreign()
	return t
}

// request stamps the request time; finishedAt has second precision, so this does too.
func (t *nativeTermCapture) request() {
	t.receipt.RequestedAt = time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
}

func (t *nativeTermCapture) object(kind string) (map[string]any, error) {
	refs := map[string]struct {
		gvk  schema.GroupVersionKind
		name string
	}{
		nativeTermMigration: {v1beta1.GroupVersion.WithKind("Migration"), t.name},
		nativeTermJob:       {batchv1.SchemeGroupVersion.WithKind("Job"), t.job},
		nativeTermPod:       {corev1.SchemeGroupVersion.WithKind("Pod"), t.pod},
		nativeTermPVC:       {corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"), t.name + "-work"},
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(refs[kind].gvk)
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: refs[kind].name}, u)
	if apierrors.IsNotFound(err) {
		return nil, nil // a released object is recorded as null for the checker to judge
	}
	return u.Object, err
}

func (t *nativeTermCapture) read(g Gomega, kinds ...string) map[string]any {
	objects := map[string]any{}
	for _, kind := range kinds {
		obj, err := t.object(kind)
		g.Expect(err).NotTo(HaveOccurred(), "reading the %s of %s", kind, t.name)
		objects[kind] = obj
	}
	return objects
}

// awaitTerminated records the first observed state with the worker container terminated.
// shortcut: polls every 200 ms instead of watching; a Migration released inside one poll
// fails the checker rather than passing it, so switch to a watch if that shows up.
func (t *nativeTermCapture) awaitTerminated() {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		raw, err := t.object(nativeTermPod)
		g.Expect(err).NotTo(HaveOccurred())
		if raw == nil {
			StopTrying("worker pod " + t.pod + " is gone before its termination was observed").Now()
		}
		pod := &corev1.Pod{}
		g.Expect(runtime.DefaultUnstructuredConverter.FromUnstructured(raw, pod)).To(Succeed())
		g.Expect(pod.Status.ContainerStatuses).To(HaveLen(1), "worker %s has no single container", t.pod)
		g.Expect(pod.Status.ContainerStatuses[0].State.Terminated).NotTo(BeNil(), "worker %s still runs", t.pod)
		t.receipt.Terminated = t.read(g, nativeTermMigration, nativeTermJob)
		t.receipt.Terminated[nativeTermPod] = raw
	}, 3*time.Minute, 200*time.Millisecond).Should(Succeed())
}

// complete reads the after state, writes the receipt into the RUN directory and
// requires the checker to accept it.
func (t *nativeTermCapture) complete(after ...string) {
	GinkgoHelper()
	if t.receipt.Case == "suspend" {
		Eventually(func(g Gomega) {
			job := &batchv1.Job{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: t.job}, job)).To(Succeed())
			g.Expect(job.Status.Conditions).To(ContainElement(And(HaveField("Type", batchv1.JobFailed),
				HaveField("Status", corev1.ConditionTrue))), "the suspended worker Job %s is not Failed", t.job)
		}, 2*time.Minute, time.Second).Should(Succeed())
	}
	t.receipt.After = t.read(Default, after...)
	t.receipt.Foreign.After = t.foreign()
	path := filepath.Join(activeKindSuite.dir, fmt.Sprintf("native-term-%d-%d-%s.json", pgSource, pgTarget,
		t.receipt.Case))
	verdict, err := writeNativeTermReceipt(ctx, path, t.receipt)
	Expect(err).NotTo(HaveOccurred(), "TERM receipt %s: %s", path, verdict)
	AddReportEntry("native TERM receipt", path+" "+verdict)
}

func (t *nativeTermCapture) foreign() []nativeTermRef {
	GinkgoHelper()
	refs := make([]nativeTermRef, 0, len(nativeTermForeignKinds))
	for _, gvk := range nativeTermForeignKinds {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
		Expect(k8sClient.List(ctx, list, client.InNamespace(nsE2E))).To(Succeed(), "listing %s", gvk.Kind)
		refs = append(refs, nativeTermForeign(gvk.Kind, t.migration, list.Items)...)
	}
	Expect(refs).To(ContainElement(HaveField("UID", string(t.sentinel))),
		"the foreign inventory misses the planted sentinel")
	return refs
}

// nativeTermForeign keeps the items the Migration does not own directly.
func nativeTermForeign(kind string, owner types.UID, items []unstructured.Unstructured) []nativeTermRef {
	refs := make([]nativeTermRef, 0, len(items))
	for i := range items {
		if slices.ContainsFunc(items[i].GetOwnerReferences(), func(r metav1.OwnerReference) bool { return r.UID == owner }) {
			continue
		}
		refs = append(refs, nativeTermRef{Kind: kind, Namespace: items[i].GetNamespace(), Name: items[i].GetName(),
			UID: string(items[i].GetUID()), ResourceVersion: items[i].GetResourceVersion()})
	}
	return refs
}

// writeNativeTermReceipt writes a new private receipt and returns the checker's verdict line.
func writeNativeTermReceipt(ctx context.Context, path string, receipt nativeTermReceipt) (string, error) {
	data, err := json.Marshal(receipt)
	if err != nil {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	_, writeErr := file.Write(append(data, '\n'))
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return "", err
	}
	checkCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	out, err := exec.CommandContext(checkCtx, "python3", "-I", "-B", nativeTermChecker, path).CombinedOutput()
	return string(out), err
}
