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
// counts via psql, and renders the version-gated shell that runs pgcopydb's
// own JSON progress reporting (`pgcopydb list progress --json`) inside a
// caller's own script, because on stock pgcopydb 0.18 that command corrupts
// filtered catalogs and never returns data (see
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

// GateScript reads the pod's pgcopydb version and runs `list progress` only
// inside the case arm the allowlist rendered: any other version matches
// nothing, prints nothing, and the poll stays shut. Exported because two
// Jobs ask for the counters this same way, each from its own pod once the
// worker is gone: a follow migration's drain verification (see
// buildVerifyJob) and a plain clone's completion check (see
// buildCatalogJob). One allowlist, one renderer, so the gate cannot drift
// between callers.
//
// An empty allowlist renders nothing at all, which is the same "do not even
// ask" the poll itself takes. Rendering the case statement with no pattern at
// all is a syntax error in dash, which is what the runner image links /bin/sh
// to (measured on the shipped image), so a caller embedding this in a larger
// script would have had the shell refuse the whole thing.
func (p *Poller) GateScript() string {
	if len(p.allowed) == 0 {
		return ""
	}
	// The pattern list opens with "(", the unambiguous POSIX form, because
	// the verify Job embeds this script inside $( ), where a shell may read
	// the pattern's own ")" as the end of the substitution. Shells disagree
	// about whether they do: bash 3.2 refuses the bare form, bash 4.0 and
	// later accept it, dash accepts it (measured across 3.2, 4.0, 4.4, 5.1,
	// 5.3 and the shipped runner image). Nothing we ship runs a shell that
	// refuses it, so this is portability rather than a live bug, and the
	// reason to write it this way is that the form which needs no such
	// survey is free.
	return `v=$(pgcopydb --version | head -n 1)
v=${v#` + versionPrefix + `}
case "$v" in
(` + strings.Join(p.allowed, "|") + `) pgcopydb list progress --json --dir ` + pgcopydb.WorkDir + ` ;;
esac
`
}

// sampleScript asks each database for everything one poll needs, in one row:
// its size, then its tables, the tables holding rows, the indexes, and the
// table bytes; the source row ends with a sixth figure, the tables it holds
// rows in that the target holds none in. Sizes and counts used to be two
// execs against the same two connections, which is five psql invocations
// every ten seconds against a database already under a bulk copy.
//
// The source has to be asked about the target's tables rather than its own.
// pgcopydb restores only the in-scope schema, so the target's table list is
// the filters already applied; counting the source unscoped reports indexes
// and bytes for tables this migration was told to leave behind, and a
// denominator it can never reach. The target's counts row carries that list
// as a second column, one name and one row count per table, and the source
// joins it; a target that did not answer (a held lock on one table blocks the
// row) leaves the source joining nothing, so its size still lands and its
// counts, which need the target's anyway, are discarded by the parser.
//
// Each table's exact row count is read with a one-row select
// (query_to_xml being the one read-only dynamic SQL stock PostgreSQL has), and
// a table owes the copy while the target's count is short of the source's.
// Storage cannot tell: a table's TOAST relation occupies a page from the
// moment the schema is restored, so a pg_table_size test counted an 848MB
// table with no rows on the target as copied (issue #277), and an earlier
// version of this same query counted a table done the instant it held any row
// at all, so a table interrupted mid-copy with some but not all of its rows
// landed also read as done. Counting exactly is a full scan of every in-scope
// table on both sides, every poll; the tables here are assumed small enough,
// and the copy busy enough already, that one more sequential scan alongside
// it is noise, but a migration of a few enormous tables is the case to watch
// if this ever shows up in the load it causes rather than the load it copies.
//
// pg_table_size for the bytes: the table with its TOAST, without its indexes.
// The other two are both wrong here, and each was measured against a real
// pair before this settled. pg_total_relation_size adds the indexes, so an
// empty table carrying a primary key counted as copied and a target holding
// one populated table of three reported two. pg_relation_size counts only the
// main fork, so a table of documents reported 256kB where pg_table_size
// reported 66MB: on an e2e clone that read as 512MiB to copy while the target
// grew past 3GB. The readings are in docs/research/measurements.md#progress-sampling.
//
// A failed side prints empty and parses to no sample, never to zero. psql
// touches no SQLite catalog, so unlike `list progress` this is safe while the
// clone runs, which is why these numbers can be live at all.
const sampleScript = progressSQL + `rowcount="(xpath('/row/count/text()', query_to_xml(format('select count(*) from %I.%I', t.nspname, t.relname), false, true, '')))[1]::text::bigint"
tables="from pg_class c
  join pg_namespace n on n.oid = c.relnamespace"
user_tables="where c.relkind = 'r'
    and n.nspname not in ('pg_catalog', 'information_schema')
    and n.nspname not like 'pg_toast%'"
row="pg_database_size(current_database()) || ' ' ||
  (select count(*) from t) || ' ' ||
  (select count(*) from t where $rowcount > 0) || ' ' ||
  (select count(*) from pg_index i where i.indrelid in (select oid from t)) || ' ' ||
  (select coalesce(sum(pg_table_size(t.oid)), 0) from t)"
t=$(progress_sql "$PGCOPYDB_TARGET_PGURI" "with t as (select c.oid, n.nspname, c.relname $tables $user_tables)
  select $row, (select coalesce(string_agg('(' || quote_literal(t.nspname || '.' || t.relname) || ',' || ($rowcount)::text || ')', ','), '') from t)") || t=
case $t in
  *\|?*) landed="values ${t#*|}" ;;
  *) landed="select null::text, 0::bigint where false" ;;
esac
t=${t%%|*}
s=$(progress_sql "$PGCOPYDB_SOURCE_PGURI" "with t as (select c.oid, n.nspname, c.relname, landed.rowcount as target_rowcount $tables
    join ($landed) as landed(name, rowcount) on landed.name = n.nspname || '.' || c.relname $user_tables)
  select $row || ' ' || (select count(*) from t where t.target_rowcount < $rowcount)") || s=
printf 'source=%s\ntarget=%s\n' "$s" "$t"
`

// Instance catalogs have no relation counters; zero counts preserve the sample row format without reporting progress.
const allDatabasesSampleScript = progressSQL + `row="select sum(pg_database_size(oid)) || ' 0 0 0 0'
  from pg_database where datname not in ('template0', 'template1')"
s=$(progress_sql "$PGCOPYDB_SOURCE_PGURI" "$row || ' 0'") || s=
t=$(progress_sql "$PGCOPYDB_TARGET_PGURI" "$row") || t=
printf 'source=%s\ntarget=%s\n' "$s" "$t"
`

// One transaction pins pooled queries to the backend with the local timeout.
// SQL cancellation releases server work; timeout also bounds connection hangs
// after the local exec stream closes.
const progressSQL = `progress_sql() {
  timeout --signal=TERM --kill-after=1s 6s psql "$1" -XqtA --single-transaction -v ON_ERROR_STOP=1 \
    -c 'SET LOCAL statement_timeout = 5000' -c "$2"
}
`

// Sample is one poll of both databases: their sizes, and the relation counts
// when the target has a schema to count.
type Sample struct {
	SourceSize *int64
	TargetSize *int64
	Counts     *RelationCounts
}

// RelationCounts is the progress half of a Sample, shaped for CloneProgress.
// TablesDone counts the in-scope tables the copy owes nothing on: their exact
// row count on the target is at least the source's, which a table empty on
// both sides also satisfies.
type RelationCounts struct {
	TablesTotal  int64
	TablesDone   int64
	IndexesTotal int64
	IndexesDone  int64
	BytesTotal   int64
	BytesDone    int64
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
	return parseSample(out), nil
}

// parseSample reads the source= and target= lines, six integers for the source
// and five for the target. A side that is missing, short or not numeric
// contributes nothing rather than zero.
func parseSample(out []byte) *Sample {
	var src, tgt []int64
	for line := range strings.SplitSeq(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "source="); ok {
			src = parseFields(v)
		} else if v, ok := strings.CutPrefix(line, "target="); ok {
			tgt = parseFields(v)
		}
	}
	sample := &Sample{}
	if len(src) == 6 {
		sample.SourceSize = &src[0]
	}
	if len(tgt) == 5 {
		sample.TargetSize = &tgt[0]
	}
	// Counts need both sides, and a target with no tables has no schema yet:
	// 0 of 0 is an absent sample, not progress.
	if len(src) == 6 && len(tgt) == 5 && tgt[1] > 0 {
		sample.Counts = &RelationCounts{
			// Every total is the source's, every done is the target's, so a
			// dashboard can name the side it came from. The source is asked only
			// about the tables the target has, so this counts the same set either
			// way; taking it from the source is what makes the label true. The
			// tables done are the total less what the source counted as owed.
			TablesTotal:  src[1],
			TablesDone:   src[1] - src[5],
			IndexesTotal: src[3],
			IndexesDone:  tgt[3],
			BytesTotal:   src[4],
			BytesDone:    tgt[4],
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

// finalizingScript asks the target what pgcopydb is doing there, counting its
// own backends by the work they are on. pgcopydb names its connections after
// that work, "pgcopydb[54] copy worker" against "pgcopydb[32] VACUUM ANALYZE
// public.documents", so the name is the primary signal and the statement text
// only a fallback. Both tests are case insensitive on purpose: pgcopydb emits
// the copy statement lowercase, which is easy to match wrongly and gives a
// confident zero when four copies are running.
//
// Copy workers count by connection, the tail only while active. Sampled
// across a whole base copy on a live worker 2026-08-30: four copy workers
// connected in every sample, zero the instant it ended, while the active
// count dipped to zero mid-copy and read as the tail.
//
// Scoped to this worker's own connections by client_addr. Every worker of one
// migration runs in one pod and so shares a source address, while a target can
// be shared: the e2e suite points every migration at one, and without the
// scope a compare worker from another migration's verification counts as this
// clone's tail and reports the wrong phase. psql's own backend is excluded by
// the application_name test.
//
// psql against the target touches no SQLite catalog, so it is safe while the
// clone runs. Reading `list progress` is not, which is why this exists.
const finalizingScript = progressSQL + `progress_sql "$PGCOPYDB_TARGET_PGURI" "select
  count(*) filter (where application_name ilike '%copy worker%'
                      or (state = 'active' and query ilike 'copy %')) || ' ' ||
  count(*) filter (where state = 'active'
                     and application_name not ilike '%copy worker%'
                     and query not ilike 'copy %')
from pg_stat_activity
where application_name like 'pgcopydb%' and client_addr = inet_client_addr()" || echo
`

// CloneStage reports whether the worker is still copying data or has moved on
// to the tail: index builds, constraints and vacuum. Unknown (both false) when
// there is no pod, the query failed, or pgcopydb holds no backend the query
// counts, since an empty answer must not be read as either state.
func (p *Poller) CloneStage(ctx context.Context, namespace, jobName string) (copying, finalizing bool) {
	pod, err := p.exec.RunningPod(ctx, namespace, jobName)
	if err != nil || pod == "" {
		return false, false
	}
	out, err := p.exec.InPod(ctx, namespace, pod, []string{"sh", "-c", conn.URIRecover() + finalizingScript})
	if err != nil {
		return false, false
	}
	var nCopy, nOther int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d %d", &nCopy, &nOther); err != nil {
		return false, false
	}
	// The counts behind the phase the operator goes on to report, so the next
	// surprise arrives with evidence rather than a guess.
	logf.FromContext(ctx).V(1).Info("clone stage sample",
		"job", jobName, "copyBackends", nCopy, "otherBackends", nOther)
	// Only once no copy worker is left is the data across. A single remaining
	// backend is the normal shape of the tail, not a sign of trouble.
	return nCopy > 0, nCopy == 0 && nOther > 0
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
