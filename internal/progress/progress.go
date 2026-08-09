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

// Package progress reads clone progress from a running worker pod by exec-ing
// pgcopydb's own JSON reporting (`pgcopydb list progress --json`). The command
// reads the SQLite catalogs in the work dir, so it must run inside the pod
// that mounts the work volume.
package progress

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
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
func (p *Poller) CloneProgress(ctx context.Context, namespace, jobName string) (*v1beta1.CloneProgress, error) {
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
// (docs/research/pgcopydb-cli.md section 10; bytes object per upstream
// progress.c). Unknown fields are ignored so schema drift degrades to missing
// numbers, never to a failure. Bytes is a pointer so an output without the
// object (older pgcopydb) yields no byte counters instead of fake zeros.
type listProgress struct {
	Tables  counts  `json:"tables"`
	Indexes counts  `json:"indexes"`
	Bytes   *counts `json:"bytes"`
}

type counts struct {
	Total int64 `json:"total"`
	Done  int64 `json:"done"`
}

// ParseListProgress converts pgcopydb JSON output into status progress.
func ParseListProgress(raw []byte) (*v1beta1.CloneProgress, error) {
	var lp listProgress
	if err := json.Unmarshal(raw, &lp); err != nil {
		return nil, fmt.Errorf("parse list progress output: %w", err)
	}
	p := &v1beta1.CloneProgress{
		TablesTotal:  lp.Tables.Total,
		TablesDone:   lp.Tables.Done,
		IndexesTotal: lp.Indexes.Total,
		IndexesDone:  lp.Indexes.Done,
	}
	if lp.Bytes != nil {
		p.BytesTotal = resource.NewQuantity(lp.Bytes.Total, resource.BinarySI)
		p.BytesDone = resource.NewQuantity(lp.Bytes.Done, resource.BinarySI)
	}
	return p, nil
}
