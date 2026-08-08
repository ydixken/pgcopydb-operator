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
// that mounts the work volume.
package progress

import (
	"context"
	"encoding/json"
	"fmt"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

// Poller execs progress commands in worker pods.
type Poller struct {
	exec *podexec.Exec
}

// NewFromExec shares an existing exec transport.
func NewFromExec(e *podexec.Exec) *Poller { return &Poller{exec: e} }

// CloneProgress returns the current copy progress of the Job's running pod,
// or (nil, nil) when no pod is ready to answer: a missing sample is not an
// error, the previous status value simply stands.
func (p *Poller) CloneProgress(ctx context.Context, namespace, jobName string) (*v1alpha1.CloneProgress, error) {
	pod, err := p.exec.RunningPod(ctx, namespace, jobName)
	if err != nil {
		return nil, err
	}
	if pod == "" {
		return nil, nil
	}
	out, err := p.exec.InPod(ctx, namespace, pod,
		[]string{"pgcopydb", "list", "progress", "--json", "--dir", pgcopydb.WorkDir})
	if err != nil {
		// The command fails while the catalogs are still initializing; treat
		// it as "no sample yet" rather than a reconcile error.
		return nil, nil
	}
	return ParseListProgress(out)
}

// listProgress mirrors the documented shape of `pgcopydb list progress --json`
// (docs/research/pgcopydb-cli.md section 10). Unknown fields are ignored so
// schema drift degrades to missing numbers, never to a failure.
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
