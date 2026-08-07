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

// Package podexec runs commands in worker pods. It backs both progress
// polling and the sentinel control plane: pgcopydb's own CLI, executed on the
// same filesystem as the running migration, is the sanctioned interface to
// its catalogs and sentinel.
package podexec

import (
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Exec runs commands in a Job's running worker pod.
type Exec struct {
	config    *rest.Config
	clientset kubernetes.Interface
}

// New builds an Exec from the manager's rest config.
func New(config *rest.Config) (*Exec, error) {
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Exec{config: config, clientset: cs}, nil
}

// RunningPod returns the name of the Job's running pod, or "" when none is
// ready to answer (starting, terminating): callers treat that as "no sample".
func (e *Exec) RunningPod(ctx context.Context, namespace, jobName string) (string, error) {
	pods, err := e.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		// The Job controller labels worker pods with job-name.
		LabelSelector: "job-name=" + jobName,
		FieldSelector: "status.phase=Running",
	})
	if err != nil {
		return "", err
	}
	if len(pods.Items) == 0 {
		return "", nil
	}
	return pods.Items[0].Name, nil
}

// InPod runs argv in the pod's pgcopydb container and returns stdout.
func (e *Exec) InPod(ctx context.Context, namespace, pod string, argv []string) ([]byte, error) {
	req := e.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "pgcopydb",
			Command:   argv,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return nil, fmt.Errorf("exec %v: %w (stderr: %s)", argv, err, stderr.String())
	}
	return stdout.Bytes(), nil
}
