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

// Package progress samples a running worker pod: database sizes on both
// sides via psql, and pgcopydb's own JSON progress reporting (`pgcopydb
// list progress --json`) behind an exact version allowlist, because on
// stock pgcopydb 0.18 that command corrupts filtered catalogs and never
// returns data (see docs/research/upstream-issues.md). Every failure mode
// yields no sample, never an aborted reconcile.
package progress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

// versionPrefix is what `pgcopydb --version` prints before the version on
// its first line; the gate script strips it.
const versionPrefix = "pgcopydb version "

// versionPattern confines allowlisted versions to characters that are inert
// in a shell case pattern, so rendering them into the script is safe.
var versionPattern = regexp.MustCompile(`^[0-9A-Za-z._-]+$`)

// execer is the podexec surface the poller needs; *podexec.Exec satisfies it.
type execer interface {
	RunningPod(ctx context.Context, namespace, jobName string) (string, error)
	InPod(ctx context.Context, namespace, pod string, argv []string) ([]byte, error)
}

// Poller samples progress and database sizes from worker pods.
type Poller struct {
	exec execer

	// allowed gates the `list progress` exec; empty keeps it shut for good.
	allowed []string
}

// NewFromExec shares an existing exec transport. Versions that could break
// out of the gate script are dropped with a warning, never rendered.
func NewFromExec(exec execer, allowedVersions []string) *Poller {
	p := &Poller{exec: exec}
	for _, v := range allowedVersions {
		if !versionPattern.MatchString(v) {
			logf.Log.WithName("progress").Info(
				"dropping invalid version from the progress-poll allowlist", "version", v)
			continue
		}
		p.allowed = append(p.allowed, v)
	}
	return p
}

// gateScript reads the pod's pgcopydb version and runs `list progress` only
// inside the case arm the allowlist rendered: any other version matches
// nothing, prints nothing, and the poll stays shut.
func (p *Poller) gateScript() string {
	return `v=$(pgcopydb --version | head -n 1)
v=${v#` + versionPrefix + `}
case "$v" in
` + strings.Join(p.allowed, "|") + `) pgcopydb list progress --json --dir ` + pgcopydb.WorkDir + ` ;;
esac
`
}

// CloneProgress returns the current copy progress of the Job's running pod,
// or (nil, nil) when there is nothing safe to report: gate shut, no running
// pod, exec failure, or non-JSON output. A missing sample is not an error,
// the previous status value simply stands.
func (p *Poller) CloneProgress(ctx context.Context, namespace, jobName string) (*v1beta1.CloneProgress, error) {
	if len(p.allowed) == 0 {
		return nil, nil
	}
	pod, err := p.exec.RunningPod(ctx, namespace, jobName)
	if err != nil {
		return nil, err
	}
	if pod == "" {
		return nil, nil
	}
	out, err := p.exec.InPod(ctx, namespace, pod, []string{"sh", "-c", p.gateScript()})
	if err != nil {
		// Catalogs still initializing, or the exec transport hiccuped.
		return nil, nil
	}
	raw := bytes.TrimSpace(out)
	if len(raw) == 0 {
		// The gate did not open: the pod runs a version off the allowlist.
		return nil, nil
	}
	cp, err := ParseListProgress(raw)
	if err != nil {
		return nil, nil
	}
	return cp, nil
}

// sizesScript samples pg_database_size per side after the URI recovery
// prelude; a failed side prints empty and parses to nil, never zero. psql
// touches no SQLite catalog, so it is safe while the clone runs.
const sizesScript = `s=$(psql "$PGCOPYDB_SOURCE_PGURI" -XtAc 'select pg_database_size(current_database())') || s=
t=$(psql "$PGCOPYDB_TARGET_PGURI" -XtAc 'select pg_database_size(current_database())') || t=
printf 'source=%s\ntarget=%s\n' "$s" "$t"
`

// DatabaseSizes samples both databases' sizes from the Job's running pod.
// No running pod means (nil, nil, nil); a side whose query failed stays nil.
func (p *Poller) DatabaseSizes(ctx context.Context, namespace, jobName string) (src, tgt *int64, err error) {
	pod, err := p.exec.RunningPod(ctx, namespace, jobName)
	if err != nil {
		return nil, nil, err
	}
	if pod == "" {
		return nil, nil, nil
	}
	out, err := p.exec.InPod(ctx, namespace, pod, []string{"sh", "-c", conn.URIRecover() + sizesScript})
	if err != nil {
		return nil, nil, err
	}
	src, tgt = parseSizes(out)
	return src, tgt, nil
}

// parseSizes reads the source=/target= lines; anything that is not a plain
// integer leaves that side nil.
func parseSizes(out []byte) (src, tgt *int64) {
	for line := range strings.SplitSeq(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "source="); ok {
			src = parseSize(v)
		} else if v, ok := strings.CutPrefix(line, "target="); ok {
			tgt = parseSize(v)
		}
	}
	return src, tgt
}

func parseSize(s string) *int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

// listProgress mirrors the documented shape of `pgcopydb list progress --json`
// (bytes object per upstream progress.c). Unknown fields are ignored so
// schema drift degrades to missing
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
