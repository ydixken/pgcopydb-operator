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

// Package progress samples a running worker pod's database sizes and relation
// counts via psql, and renders the version-gated shell that runs `pgcopydb
// list progress --json` inside a caller's script: on stock pgcopydb 0.18 that
// command corrupts filtered catalogs and never returns data (see
// docs/research/upstream-issues.md). Every failure mode yields no sample,
// never an aborted reconcile.
package progress

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

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

	// lost holds the job sides whose last sample failed, so an outage logs as it starts and as it ends.
	// ponytail: a job that ends mid-outage keeps its key; prune on pod exit if that ever matters.
	mu   sync.Mutex
	lost map[string]bool
}

// NewFromExec shares an existing exec transport. Versions that could break
// out of the gate script are dropped with a warning, never rendered.
func NewFromExec(exec execer, allowedVersions []string) *Poller {
	p := &Poller{exec: exec, lost: map[string]bool{}}
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

// GateScript runs `list progress` only inside the case arm the allowlist
// rendered, so any other version matches nothing and the poll stays shut. An
// empty allowlist renders nothing at all: callers embed this in a larger
// script, and dash (the runner image's /bin/sh) refuses a patternless case.
func (p *Poller) GateScript() string {
	if len(p.allowed) == 0 {
		return ""
	}
	// The pattern list opens with "(", the POSIX form that needs no portability
	// survey: the verify Job embeds this inside $( ), where a shell may read a
	// bare pattern's own ")" as the end of the substitution
	// (see docs/research/measurements.md#shells-disagree-about-a-bare-case-pattern-inside-a-command-substitution).
	return `v=$(pgcopydb --version | head -n 1)
v=${v#` + versionPrefix + `}
case "$v" in
(` + strings.Join(p.allowed, "|") + `) pgcopydb list progress --json --dir ` + pgcopydb.WorkDir + ` ;;
esac
`
}

// SamplerMarker opens both side queries, the prefix the e2e specs find the sampler by:
// pg_stat_activity keeps only the first track_activity_query_size bytes (1kB by default).
const SamplerMarker = "/* pgcopydb-operator progress sample */"

// sampleScript prints the clone stage, six source figures, five target figures and the
// tables still owed; a failed side prints empty. Design: docs/research/measurements.md#progress-sampling.
const sampleScript = sampleSQL + stageScript + `populated="query_to_xml(format('select 1 from %I.%I limit 1', t.nspname, t.relname), false, true, '')::text <> ''"
present="case when t.copying then false else $populated end"
tables="from pg_class c
  join pg_namespace n on n.oid = c.relnamespace"
user_tables="where c.relkind = 'r'
    and n.nspname not in ('pg_catalog', 'information_schema')
    and n.nspname not like 'pg_toast%'"
row="pg_database_size(current_database()) || ' ' ||
  (select count(*) from t) || ' ' ||
  (select count(*) from t where $present) || ' ' ||
  (select count(*) from pg_index i where i.indrelid in (select oid from t)) || ' ' ||
  (select coalesce(sum(t.bytes), 0) from t)"
copies="select pid, relid, bytes_processed from pg_stat_progress_copy
    where command = ''COPY FROM'' and relid <> 0 and datname = current_database()"
none="select 0 as pid, 0::oid as relid, 0::bigint as bytes_processed where false"
t=$(progress_sql "$PGCOPYDB_TARGET_PGURI" "` + SamplerMarker + ` with mine as (select pid, application_name ilike '%copy worker%' or query ilike 'copy %' as copier
    from pg_stat_activity where application_name like 'pgcopydb%' and client_addr = inet_client_addr()),
  copying as (select x.relid, sum(x.bytes_processed) as bytes
    from xmltable('/table/row' passing query_to_xml(case when to_regclass('pg_catalog.pg_stat_progress_copy') is null then '$none' else '$copies' end, false, false, '')
      columns pid integer, relid oid, bytes_processed bigint) x
    where x.pid in (select pid from mine) group by x.relid),
  exclusive as (select distinct l.relation from pg_locks l
    where l.locktype = 'relation' and l.mode = 'AccessExclusiveLock' and l.granted and l.pid in (select pid from mine where copier)
      and l.database = (select d.oid from pg_database d where d.datname = current_database())),
  t as (select c.oid, n.nspname, c.relname, k.relid is not null or e.relation is not null as copying,
      case when e.relation is not null then coalesce(k.bytes, 0) else pg_table_size(c.oid) end as bytes $tables
    left join copying k on k.relid = c.oid
    left join exclusive e on e.relation = c.oid $user_tables)
  select $row, (select coalesce(string_agg('(' || quote_literal(t.nspname || '.' || t.relname) || ',' || ($present)::text || ')', ','), '') from t)") ||
  { t=; printf 'target_error=%s\n' "$(why)"; }
case $t in
  *\|?*) landed="values ${t#*|}" ;;
  *) landed="select null::text, false where false" ;;
esac
t=${t%%|*}
s=$(progress_sql "$PGCOPYDB_SOURCE_PGURI" "` + SamplerMarker + ` with t as (select c.oid, n.nspname, c.relname, landed.populated, false as copying, pg_table_size(c.oid) as bytes $tables
    join ($landed) as landed(name, populated) on landed.name = n.nspname || '.' || c.relname $user_tables)
  select $row || ' ' || (select count(*) || '|' || coalesce(string_agg(t.nspname || '.' || t.relname, ', ' order by t.nspname, t.relname), '')
    from t where not t.populated and $populated)") ||
  { s=; printf 'source_error=%s\n' "$(why)"; }
rm -f "$err"
printf 'stage=%s\nsource=%s\ntarget=%s\nowed=%s\n' "$g" "${s%%|*}" "$t" "${s#*|}"
`

// Instance catalogs have no relation counters; zero counts preserve the sample
// row format without reporting progress. The FROM lives apart from the select
// list so the source's sixth figure appends to the list: appended to the whole
// query it lands on the WHERE clause, which then fails to parse (issue #277).
const allDatabasesSampleScript = sampleSQL + stageScript + `row="select sum(pg_database_size(oid)) || ' 0 0 0 0'"
dbs="from pg_database where datname not in ('template0', 'template1')"
s=$(progress_sql "$PGCOPYDB_SOURCE_PGURI" "$row || ' 0' $dbs") || { s=; printf 'source_error=%s\n' "$(why)"; }
t=$(progress_sql "$PGCOPYDB_TARGET_PGURI" "$row $dbs") || { t=; printf 'target_error=%s\n' "$(why)"; }
rm -f "$err"
printf 'stage=%s\nsource=%s\ntarget=%s\n' "$g" "$s" "$t"
`

// One transaction pins the pooled queries to the backend that SET LOCAL bounds,
// and the outer timeout catches a connection hang after the exec stream closes.
// The subshell keeps dash's "Killed" notice for a SIGKILLed timeout on the exec
// stderr: a redirect on the call itself would still be open when dash prints it.
const progressSQL = `progress_sql() {
  ( timeout --signal=TERM --kill-after=1s 6s psql "$1" -XqtA --single-transaction -v ON_ERROR_STOP=1 \
    -c 'SET LOCAL statement_timeout = 5000' -c "$2" 2>"${err:-/dev/stderr}" )
}
`

// A failed side names its cause on a <side>_error= line; psql's stderr goes to
// a file so that a connection warning on success cannot corrupt the row.
const sampleSQL = progressSQL + `err=/tmp/pgm-progress-$$
why() { tr '\n' ' ' < "$err" | cut -c 1-300; }
`

// The sides a sample reads, as the script names them on its output lines.
const sourceSide, targetSide = "source", "target"

// withheldReason replaces any psql message a server did not write: libpq echoes
// a connection URI it cannot parse, password included, and the reason is logged.
const withheldReason = "psql failed with a client-side message, withheld because it can echo the connection URI"

// serverReason passes psql's message on only when it starts as a server error
// or as libpq's connection failure, which names the host and port but no URI.
func serverReason(msg string) string {
	for _, prefix := range []string{"ERROR:", "FATAL:", "psql: error: connection to server at "} {
		if msg == "" || strings.HasPrefix(msg, prefix) {
			return msg
		}
	}
	return withheldReason
}

// Sample is one poll of both databases: their sizes, the relation counts
// when the target has a schema to count, and the clone stage.
type Sample struct {
	SourceSize *int64
	TargetSize *int64
	Counts     *RelationCounts

	// Copying and Finalizing are the clone stage; both false is unknown.
	Copying, Finalizing bool

	// Lost maps each side that returned no row to psql's error, empty when
	// the script captured none, withheld when no server wrote it.
	Lost map[string]string
}

// RelationCounts is the progress half of a Sample, shaped for CloneProgress.
// TablesDone counts tables that hold a row on the target or none on the
// source; EmptyOnTarget names the rest, for the condition message.
type RelationCounts struct {
	TablesTotal   int64
	TablesDone    int64
	IndexesTotal  int64
	IndexesDone   int64
	BytesTotal    int64
	BytesDone     int64
	EmptyOnTarget string
}

// Sample reads both databases from the Job's running pod. No pod is no sample
// rather than an error: the worker exits and this keeps being called.
func (p *Poller) Sample(ctx context.Context, namespace, jobName string, allDatabases bool) (*Sample, error) {
	pod, err := p.exec.RunningPod(ctx, namespace, jobName)
	if err != nil {
		return nil, err
	}
	if pod == "" {
		return nil, nil
	}
	script := sampleScript
	if allDatabases {
		script = allDatabasesSampleScript
	}
	out, err := p.exec.InPod(ctx, namespace, pod, []string{"sh", "-c", conn.URIRecover() + script})
	if err != nil {
		// The container can exit after RunningPod selects it and before the
		// exec upgrade reaches the kubelet. That is the same no-sample state.
		if running, lookupErr := p.exec.RunningPod(ctx, namespace, jobName); lookupErr == nil && running == "" {
			return nil, nil
		}
		return nil, err
	}
	s := parseSample(out)
	p.logLost(ctx, namespace+"/"+jobName, s.Lost)
	return s, nil
}

// logLost reports a side that stops answering and the sample that answers
// again, and nothing in between: the sampler runs every pass.
func (p *Poller) logLost(ctx context.Context, job string, lost map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, side := range []string{sourceSide, targetSide} {
		reason, failed := lost[side]
		key := job + "/" + side
		switch {
		case failed && !p.lost[key]:
			p.lost[key] = true
			logf.FromContext(ctx).Info("progress sample lost a side; status keeps its last progress until it answers again",
				"job", job, "side", side, "reason", reason)
		case !failed && p.lost[key]:
			delete(p.lost, key)
			logf.FromContext(ctx).Info("progress sample side answers again", "job", job, "side", side)
		}
	}
}

// parseSample reads the stage= line, the source= and target= lines, six integers for the source
// and five for the target, the owed= line naming the tables the sixth figure
// counted, and a <side>_error= line per failed side. A side that is missing,
// short or not numeric contributes nothing rather than zero, and is lost.
func parseSample(out []byte) *Sample {
	var src, tgt []int64
	var owed string
	var copying, finalizing bool
	reasons := map[string]string{}
	for line := range strings.SplitSeq(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "source="); ok {
			src = parseFields(v)
		} else if v, ok := strings.CutPrefix(line, "target="); ok {
			tgt = parseFields(v)
		} else if v, ok := strings.CutPrefix(line, "owed="); ok {
			owed = strings.TrimSpace(v)
		} else if v, ok := strings.CutPrefix(line, "stage="); ok {
			copying, finalizing = parseStage(v)
		} else if side, v, ok := strings.Cut(line, "_error="); ok {
			reasons[side] = serverReason(strings.TrimSpace(v))
		}
	}
	sample := &Sample{Lost: map[string]string{}, Copying: copying, Finalizing: finalizing}
	if len(src) == 6 {
		sample.SourceSize = &src[0]
	} else {
		sample.Lost[sourceSide] = reasons[sourceSide]
	}
	if len(tgt) == 5 {
		sample.TargetSize = &tgt[0]
	} else {
		sample.Lost[targetSide] = reasons[targetSide]
	}
	// Counts need both sides, and a target with no tables has no schema yet:
	// 0 of 0 is an absent sample, not progress.
	if len(src) == 6 && len(tgt) == 5 && tgt[1] > 0 {
		sample.Counts = &RelationCounts{
			// Every total is the source's and every done the target's, so a
			// dashboard can name the side each figure came from. Tables done is
			// the total less what the source counted as still owed.
			TablesTotal:   src[1],
			TablesDone:    src[1] - src[5],
			IndexesTotal:  src[3],
			IndexesDone:   tgt[3],
			BytesTotal:    src[4],
			BytesDone:     tgt[4],
			EmptyOnTarget: owed,
		}
	}
	return sample
}

// parseFields splits one psql row into integers, nil unless every field parsed.
func parseFields(s string) []int64 {
	fields := strings.Fields(s)
	out := make([]int64, 0, len(fields))
	for _, f := range fields {
		v, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			return nil
		}
		out = append(out, v)
	}
	return out
}

// stageScript counts pgcopydb's own backends on the target, ahead of the sizes, so a
// sample that sees the copy reads the target after pgcopydb has cleaned it. Both tests
// are case insensitive because pgcopydb emits the copy statement lowercase,
// and copy workers count by connection while the tail counts only active ones
// (see docs/research/measurements.md#a-copy-workers-connection-outlives-the-statement-it-is-running).
// client_addr scopes the count to this worker's pod, so another migration's
// compare worker on a shared target cannot read as this clone's tail.
const stageScript = `g=$(progress_sql "$PGCOPYDB_TARGET_PGURI" "select
  count(*) filter (where application_name ilike '%copy worker%'
                      or (state = 'active' and query ilike 'copy %')) || ' ' ||
  count(*) filter (where state = 'active'
                     and application_name not ilike '%copy worker%'
                     and query not ilike 'copy %')
from pg_stat_activity
where application_name like 'pgcopydb%' and client_addr = inet_client_addr()") || g=
`

// parseStage reads the stage= line: copying while any copy worker is left, finalizing
// once only the tail is (index builds, constraints, vacuum). Both false is unknown,
// since a failed query and no matching backend must not read as either state.
func parseStage(v string) (copying, finalizing bool) {
	var nCopy, nOther int
	if _, err := fmt.Sscanf(strings.TrimSpace(v), "%d %d", &nCopy, &nOther); err != nil {
		return false, false
	}
	return nCopy > 0, nCopy == 0 && nOther > 0
}

// listProgress mirrors the documented shape of `pgcopydb list progress --json`
// (bytes object per upstream progress.c). Bytes is a pointer so an output from
// an older pgcopydb without the object yields no byte counters, not fake zeros.
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
