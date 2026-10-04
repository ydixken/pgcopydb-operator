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
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	otelCollectorName = "otel-collector"
	// Pinned: a floating tag would change the debug exporter's output format under the spec.
	otelCollectorImage = "otel/opentelemetry-collector:0.162.0"
	otelCollectorPort  = 4318
	// The debug exporter prints every datapoint, so the log is read by window, not whole.
	otelLogWindow = "60s"
	otelDropAll   = "ALL"
)

const otelCollectorConfig = `receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [debug]
`

// otelCollectorObjects returns the collector's three objects, all in nsOperator.
func otelCollectorObjects() (*corev1.ConfigMap, *appsv1.Deployment, *corev1.Service) {
	meta := metav1.ObjectMeta{Name: otelCollectorName, Namespace: nsOperator}
	labels := map[string]string{"app.kubernetes.io/name": otelCollectorName}
	cm := &corev1.ConfigMap{ObjectMeta: meta, Data: map[string]string{"config.yaml": otelCollectorConfig}}
	dep := &appsv1.Deployment{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{
		Replicas: ptr.To(int32(1)),
		Selector: &metav1.LabelSelector{MatchLabels: labels},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: corev1.PodSpec{
				AutomountServiceAccountToken: ptr.To(false),
				SecurityContext: &corev1.PodSecurityContext{
					RunAsNonRoot:   ptr.To(true),
					RunAsUser:      ptr.To(int64(10001)),
					SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
				Volumes: []corev1.Volume{{Name: "conf", VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: otelCollectorName},
					},
				}}},
				Containers: []corev1.Container{{
					Name:         "collector",
					Image:        otelCollectorImage,
					Args:         []string{"--config=/conf/config.yaml"},
					Ports:        []corev1.ContainerPort{{Name: "otlp-http", ContainerPort: otelCollectorPort}},
					VolumeMounts: []corev1.VolumeMount{{Name: "conf", MountPath: "/conf", ReadOnly: true}},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("50m"),
							corev1.ResourceMemory: resource.MustParse("64Mi"),
						},
						Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: ptr.To(false),
						ReadOnlyRootFilesystem:   ptr.To(true),
						Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{otelDropAll}},
					},
				}},
			},
		},
	}}
	svc := &corev1.Service{ObjectMeta: meta, Spec: corev1.ServiceSpec{
		Selector: labels,
		Ports: []corev1.ServicePort{{
			Name: "otlp-http", Port: otelCollectorPort, TargetPort: intstr.FromInt32(otelCollectorPort),
		}},
	}}
	return cm, dep, svc
}

// deployOTelCollector applies the collector and waits for its Deployment to be Available.
func deployOTelCollector() {
	GinkgoHelper()
	// Helm creates nsOperator later when the suite owns namespaces, so it must exist first.
	if manageNamespaces {
		ensureNamespace(nsOperator)
	}
	cm, dep, svc := otelCollectorObjects()
	for _, o := range []struct {
		obj    client.Object
		mutate func(client.Object)
	}{
		{cm, func(cur client.Object) { cur.(*corev1.ConfigMap).Data = cm.Data }},
		{dep, func(cur client.Object) { cur.(*appsv1.Deployment).Spec = dep.Spec }},
		// Only ports and selector: clusterIP is immutable and the API server fills it in.
		{svc, func(cur client.Object) {
			s := cur.(*corev1.Service)
			s.Spec.Selector, s.Spec.Ports = svc.Spec.Selector, svc.Spec.Ports
		}},
	} {
		_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, o.obj, func() error {
			o.mutate(o.obj)
			return nil
		})
		Expect(err).NotTo(HaveOccurred(), "apply %T %s/%s", o.obj, nsOperator, otelCollectorName)
	}
	Eventually(func(g Gomega) {
		cur := &appsv1.Deployment{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(dep), cur)).To(Succeed())
		var avail corev1.ConditionStatus
		for _, c := range cur.Status.Conditions {
			if c.Type == appsv1.DeploymentAvailable {
				avail = c.Status
			}
		}
		g.Expect(avail).To(Equal(corev1.ConditionTrue), "collector Deployment is not Available")
	}, 3*time.Minute, 2*time.Second).Should(Succeed())
}

// deleteOTelCollector removes the collector objects; absent ones are fine.
func deleteOTelCollector() {
	GinkgoHelper()
	cm, dep, svc := otelCollectorObjects()
	for _, o := range []client.Object{svc, dep, cm} {
		err := k8sClient.Delete(ctx, o)
		if err != nil && !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred(), "delete %T %s/%s", o, nsOperator, otelCollectorName)
		}
	}
}

// otelCollectorLogs returns the last minute of the collector log, kubectl's error text
// included, so an Eventually on it reports why a read failed.
func otelCollectorLogs() string {
	out, err := exec.CommandContext(context.Background(), "kubectl", "logs", "-n", nsOperator,
		"deploy/"+otelCollectorName, "--since="+otelLogWindow).CombinedOutput()
	if err != nil {
		return fmt.Sprintf("kubectl logs failed: %v: %s", err, out)
	}
	return string(out)
}
