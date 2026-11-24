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
	"io"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
)

// callTimeout bounds every API call this package makes: reconcile contexts
// carry no deadline, so a wedged call would pin that Migration's phase forever.
const callTimeout = 30 * time.Second

// containerName is the worker container every call targets.
const containerName = "pgcopydb"

// Exec runs commands in a Job's running worker pod.
type Exec struct {
	config    *rest.Config
	clientset kubernetes.Interface

	// timeout bounds each call; tests shorten it. Zero means callTimeout.
	timeout time.Duration
}

// New builds an Exec from the manager's rest config.
func New(config *rest.Config) (*Exec, error) {
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Exec{config: config, clientset: cs}, nil
}

// bounded derives the per-call context. The parent still wins when it is
// cancelled first (manager shutdown).
func (e *Exec) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	t := e.timeout
	if t == 0 {
		t = callTimeout
	}
	return context.WithTimeout(ctx, t)
}

// RunningPod returns the name of the Job's running pod, or "" when none is
// ready to answer (starting, terminating): callers treat that as "no sample".
func (e *Exec) RunningPod(ctx context.Context, namespace, jobName string) (string, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
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

// JobLogs returns up to tailLines of the Job's newest pod's logs, running or
// terminated. Failed Jobs are the intended target: their terminal pgcopydb
// error or preflight verdict only exists in the pod log. The newest pod is
// the right one when the Job's own backoffLimit produced several.
func (e *Exec) JobLogs(ctx context.Context, namespace, jobName string, tailLines int64) ([]byte, error) {
	return e.newestPodLogs(ctx, namespace, "job-name="+jobName, "Job "+jobName, tailLines)
}

// JobLogsByUID is JobLogs for one Job object. A Job deleted and recreated
// under the same name can leave pods behind that carry its job-name label,
// but the controller-uid label names a single Job.
func (e *Exec) JobLogsByUID(ctx context.Context, namespace string, uid types.UID, tailLines int64) ([]byte, error) {
	return e.newestPodLogs(ctx, namespace, batchv1.ControllerUidLabel+"="+string(uid), "Job UID "+string(uid), tailLines)
}

// JobLogStream returns timestamped logs for the current pod of one worker Job.
// The time cursor is used only when it belongs to that same pod; a new pod is
// scanned from its beginning. A one-second overlap avoids losing lines whose
// runtime timestamps are equal to the last processed timestamp.
func (e *Exec) JobLogStream(
	ctx context.Context,
	namespace, jobName string,
	jobUID, previousPodUID types.UID,
	since *metav1.Time,
) (io.ReadCloser, types.UID, error) {
	ctx, cancel := e.bounded(ctx)
	pods, err := e.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: batchv1.ControllerUidLabel + "=" + string(jobUID),
	})
	if err != nil {
		cancel()
		return nil, "", err
	}
	if len(pods.Items) == 0 {
		cancel()
		return nil, "", fmt.Errorf("no pods found for Job %s", jobName)
	}
	pod := newestPod(pods.Items)
	options := &corev1.PodLogOptions{Container: containerName, Timestamps: true}
	if since != nil && previousPodUID == pod.UID {
		cursor := metav1.NewTime(since.Add(-time.Second))
		options.SinceTime = &cursor
	}
	stream, err := e.clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, options).Stream(ctx)
	if err != nil {
		cancel()
		return nil, "", err
	}
	return &cancelReadCloser{ReadCloser: stream, cancel: cancel}, pod.UID, nil
}

// newestPodLogs reads the newest pod the label selector matches; job names
// the Job in the error when none does.
func (e *Exec) newestPodLogs(ctx context.Context, namespace, selector, job string, tailLines int64) ([]byte, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	pods, err := e.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pods found for %s", job)
	}
	newest := newestPod(pods.Items)
	return e.clientset.CoreV1().Pods(namespace).
		GetLogs(newest.Name, &corev1.PodLogOptions{Container: containerName, TailLines: &tailLines}).
		DoRaw(ctx)
}

func newestPod(pods []corev1.Pod) *corev1.Pod {
	newest := &pods[0]
	for i := 1; i < len(pods); i++ {
		if pods[i].CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = &pods[i]
		}
	}
	return newest
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *cancelReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.cancel()
	return err
}

// InPod runs argv in the pod's pgcopydb container and returns stdout.
func (e *Exec) InPod(ctx context.Context, namespace, pod string, argv []string) ([]byte, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	req := e.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   argv,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	// WebSocket first: its handshake honors the context, while the SPDY round
	// tripper reads the upgrade response with no deadline at all. SPDY stays the
	// fallback for API servers that refuse the websocket upgrade.
	ws, err := remotecommand.NewWebSocketExecutor(e.config, "GET", req.URL().String())
	if err != nil {
		return nil, err
	}
	spdy, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return nil, err
	}
	exec, err := remotecommand.NewFallbackExecutor(ws, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return nil, fmt.Errorf("exec %v: %w (stderr: %s)", argv, err, stderr.String())
	}
	return stdout.Bytes(), nil
}
