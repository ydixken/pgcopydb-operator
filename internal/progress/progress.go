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

// Package progress reads clone progress from a running worker pod by exec-ing
// pgcopydb's own JSON reporting (`pgcopydb list progress --json`). The command
// reads the SQLite catalogs in the work dir, so it must run inside the pod
// that mounts the work volume; a plain HTTP probe cannot replace it.
package progress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// Poller execs progress commands in worker pods.
type Poller struct {
	config    *rest.Config
	clientset kubernetes.Interface
}

// New builds a Poller from the manager's rest config.
func New(config *rest.Config) (*Poller, error) {
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Poller{config: config, clientset: cs}, nil
}

// CloneProgress returns the current copy progress of the Job's running pod,
// or (nil, nil) when no pod is ready to answer (starting, terminating): a
// missing sample is not an error, the previous status value simply stands.
func (p *Poller) CloneProgress(ctx context.Context, namespace, jobName string) (*v1alpha1.CloneProgress, error) {
	pods, err := p.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		// The Job controller labels worker pods with job-name.
		LabelSelector: "job-name=" + jobName,
		FieldSelector: "status.phase=Running",
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, nil
	}
	out, err := p.exec(ctx, namespace, pods.Items[0].Name,
		[]string{"pgcopydb", "list", "progress", "--json", "--dir", pgcopydb.WorkDir})
	if err != nil {
		// The command fails while the catalogs are still initializing; treat
		// it as "no sample yet" rather than a reconcile error.
		return nil, nil
	}
	return ParseListProgress(out)
}

// exec runs argv in the pgcopydb container and returns stdout.
func (p *Poller) exec(ctx context.Context, namespace, pod string, argv []string) ([]byte, error) {
	req := p.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "pgcopydb",
			Command:   argv,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(p.config, "POST", req.URL())
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return nil, fmt.Errorf("exec %v: %w (stderr: %s)", argv, err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// listProgress mirrors the documented shape of `pgcopydb list progress --json`
// (see docs/research/pgcopydb-cli.md section 10). Unknown fields are ignored
// so schema drift degrades to missing numbers, never to a failure.
type listProgress struct {
	Tables  counts `json:"tables"`
	Indexes counts `json:"indexes"`
}

type counts struct {
	Total int64 `json:"total"`
	Done  int64 `json:"done"`
}

// ParseListProgress converts pgcopydb JSON output into status progress.
func ParseListProgress(raw []byte) (*v1alpha1.CloneProgress, error) {
	var lp listProgress
	if err := json.Unmarshal(raw, &lp); err != nil {
		return nil, fmt.Errorf("parse list progress output: %w", err)
	}
	return &v1alpha1.CloneProgress{
		TablesTotal:  lp.Tables.Total,
		TablesDone:   lp.Tables.Done,
		IndexesTotal: lp.Indexes.Total,
		IndexesDone:  lp.Indexes.Done,
	}, nil
}
