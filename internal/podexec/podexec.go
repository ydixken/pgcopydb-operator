/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
)

// callTimeout bounds every API call this package makes. Reconcile contexts
// carry no deadline, and a pod exec or log stream that hangs on a wedged
// API-server connection would otherwise freeze that Migration's reconcile
// forever, its phase pinned wherever it stood (observed live: Migrations
// stuck in Cloning and CuttingOver with a healthy worker underneath).
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
	return e.jobLogs(ctx, namespace, jobName, tailLines, false)
}

// JobLogsTimestamps is JobLogs with the container runtime's timestamp
// prefixed to every line (PodLogOptions Timestamps, RFC3339Nano). The zombie
// check dates the supervisor-death marker with it: the runtime's stamp is a
// fixed format, unlike pgcopydb's own log timestamps.
func (e *Exec) JobLogsTimestamps(ctx context.Context, namespace, jobName string, tailLines int64) ([]byte, error) {
	return e.jobLogs(ctx, namespace, jobName, tailLines, true)
}

func (e *Exec) jobLogs(ctx context.Context, namespace, jobName string, tailLines int64, timestamps bool) ([]byte, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	pods, err := e.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + jobName,
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pods found for Job %s", jobName)
	}
	newest := pods.Items[0]
	for _, p := range pods.Items[1:] {
		if p.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = p
		}
	}
	return e.clientset.CoreV1().Pods(namespace).
		GetLogs(newest.Name, &corev1.PodLogOptions{Container: containerName, TailLines: &tailLines, Timestamps: timestamps}).
		DoRaw(ctx)
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
	// WebSocket first: its handshake honors the context, while the SPDY
	// round tripper reads the upgrade response with no deadline at all (a
	// wedged API-server connection froze reconciles mid-phase, proven by
	// the bounded-timeout test). SPDY stays as the fallback for API
	// servers that refuse the websocket upgrade.
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
