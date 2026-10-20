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

package controller

import (
	"fmt"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

const (
	// gateAppName names the probe's own sessions: the census never counts them as leftovers.
	gateAppName = "pgm-retry-gate"
	// annotationRetryGateInputs on a gate Job records the flags it was built from.
	annotationRetryGateInputs = "pgcopydb-operator.io/retry-gate-inputs"

	gateProbeDeadline int64 = 300
	// gateProbeTTL collects a probe that a stale pass created after the Migration left the gate.
	gateProbeTTL int32 = 600
	// gateSessionLimit caps the session lines a census prints.
	gateSessionLimit = 10
	// gateLogTail bounds the probe log read; a probe prints at most 14 gate: lines and a shell notice.
	gateLogTail int64 = 60
)

// The RetryHeld reasons.
const (
	reasonSessionsRemain     = "SessionsRemain"
	reasonSyncCommitNotLocal = "SyncCommitNotLocal"
	reasonCommitBlocked      = "CommitBlocked"
	reasonProbeFailed        = "ProbeFailed"
	reasonGateProbing        = "GateProbing"
	reasonWorkerPodRemains   = "WorkerPodRemains"
	reasonGatePassed         = "GatePassed"
	// Hold in these names sets them apart from the Suspended phase and event reason.
	reasonHoldSuspended       = "Suspended"
	reasonHoldMigrationFailed = "MigrationFailed"
)

// The results a probe prints on its gate: commit line.
const (
	gateCommitOK      = "ok"
	gateCommitBlocked = "blocked"
	gateCommitSkipped = "skipped"
	gateCommitFailed  = "failed"
)

func gateJobName(m *v1beta1.Migration, attempt int32) string {
	return fmt.Sprintf("%s-gate-%d", m.Name, attempt)
}

// gateInputs are the checks a probe runs beside the census. The test commit runs whenever
// the next worker could commit with synchronous_commit=on: always without synchronousCommit,
// and with it once the copy is done, because CDC apply then sets synchronous_commit=on itself.
type gateInputs struct{ CommitTest, ExpectLocal bool }

func gateInputsFor(m *v1beta1.Migration) gateInputs {
	local := m.Spec.Target.SynchronousCommit == v1beta1.SynchronousCommitLocal
	return gateInputs{CommitTest: !local || copyDoneForGate(m), ExpectLocal: local}
}

// String is the inputs annotation, so a probe built from other flags is never read as current.
func (g gateInputs) String() string {
	return "commit=" + gateFlag(g.CommitTest) + ",local=" + gateFlag(g.ExpectLocal)
}

func gateFlag(on bool) string {
	if on {
		return "1"
	}
	return "0"
}

// censusSQL counts the migration role's other sessions in the target database (any database in all-databases mode)
// under pgcopydb's name, or all of them when the URI pins application_name, and lists the oldest. The role sees its
// own sessions without a grant (docs/research/measurements.md#a-role-sees-its-own-syncrep-waiters-without-pg_monitor).
func censusSQL(allDatabases bool) string {
	db, gateDB := "\n  and datname = current_database()", "\n      and g.datname = current_database()"
	if allDatabases {
		db, gateDB = "", ""
	}
	scope := `
from pg_stat_activity
where usename = session_user
  and pid <> pg_backend_pid()` + db + `
  and (application_name like '` + progress.PgcopydbAppNameLike + `'
       or current_setting('application_name') <> '` + gateAppName + `')`
	return `select format('gate: census total=%s syncrep=%s named=%s syncrep_since=%s gate_waiting=%s',
  count(*),
  count(*) filter (where wait_event_type = 'IPC' and wait_event = 'SyncRep'),
  case when current_setting('application_name') = '` + gateAppName + `' then 'yes' else 'no' end,
  coalesce(floor(extract(epoch from min(query_start)
    filter (where wait_event_type = 'IPC' and wait_event = 'SyncRep')))::bigint, 0),
  (select count(*) from pg_stat_activity g
    where g.usename = session_user
      and g.pid <> pg_backend_pid()` + gateDB + `
      and g.application_name = '` + gateAppName + `'
      and g.wait_event_type = 'IPC' and g.wait_event = 'SyncRep'))` + scope + `;

select format('gate: session pid=%s state=%s wait=%s age=%ss started=%s app=%s',
  pid, coalesce(state, '-'), coalesce(wait_event, '-'),
  floor(extract(epoch from clock_timestamp() - coalesce(query_start, backend_start)))::bigint,
  to_char(backend_start at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
  application_name)` + scope + `
order by coalesce(query_start, backend_start)
limit ` + strconv.Itoa(gateSessionLimit) + `;`
}

// gateScript prints one gate: line per check and gate: pass only when every enabled check passed. The commit test ends on
// SIGINT, because no server timeout ends a SyncRep wait (docs/research/measurements.md#statement_timeout-does-not-end-a-syncrep-wait).
// why filters stderr as progress.serverReason does, and the subshells keep the shell's notice of a killed timeout out of $err.
func gateScript(allDatabases bool) string {
	// The shell runs as PID 1, which ignores an untrapped TERM.
	return `trap 'exit 143' TERM
set -u
for v in "${GATE_COMMIT_TEST:-}" "${GATE_EXPECT_LOCAL:-}"; do
  case $v in
  0|1) ;;
  *) printf 'gate: invalid flags GATE_COMMIT_TEST=%s GATE_EXPECT_LOCAL=%s\n' "${GATE_COMMIT_TEST:-}" "${GATE_EXPECT_LOCAL:-}"; exit 1 ;;
  esac
done
err=/tmp/pgm-gate-$$.err
out=/tmp/pgm-gate-$$.out
nl='
'
ok=1
bounded() {
  ( timeout --signal=TERM --kill-after=1s 6s psql "$PGCOPYDB_TARGET_PGURI" -XAtq --single-transaction -v ON_ERROR_STOP=1 \
    -c 'SET LOCAL statement_timeout = 5000' "$@" 2>"$err" )
}
why() {
  e=$(cat "$err")
  case ${e%%"$nl"*} in
  'psql:<stdin>:'[0-9]*': '*) e=${e#'psql:<stdin>:'*': '} ;;
  esac
  case ${e%%"$nl"*} in
  '') ;;
  ERROR:*|FATAL:*|'psql: error: connection to server at '*) printf '%s ' "$(printf '%s' "$e" | tr '\n' ' ' | cut -c 1-300)" ;;
  *) printf 'client-side error withheld ' ;;
  esac
  printf '(exit %s)' "$1"
}
census=
bounded -f - >"$out" <<'GATE_CENSUS'
` + censusSQL(allDatabases) + `
GATE_CENSUS
rc=$?
if [ "$rc" -eq 0 ]; then
  cat "$out"
  IFS= read -r census <"$out" || :
else
  printf 'gate: census failed %s\n' "$(why "$rc")"
fi
case $census in
'gate: census total=0 '*) ;;
*) ok=0 ;;
esac
if [ "$GATE_EXPECT_LOCAL" = 1 ]; then
  sc=$(bounded -c 'show synchronous_commit')
  rc=$?
  if [ "$rc" -eq 0 ]; then
    printf 'gate: synchronous_commit=%s\n' "$sc"
    [ "$sc" = local ] || ok=0
  else
    printf 'gate: synchronous_commit failed %s\n' "$(why "$rc")"
    ok=0
  fi
fi
if [ "$GATE_COMMIT_TEST" = 1 ]; then
  case $census in
  'gate: census total='*' syncrep=0 named='*' gate_waiting=0')
    ( timeout -s INT -k 5 5 psql "$PGCOPYDB_TARGET_PGURI" -XAtq --single-transaction \
      -v ON_ERROR_STOP=1 -v VERBOSITY=verbose \
      -c 'SET TRANSACTION READ WRITE' \
      -c 'SET LOCAL synchronous_commit = on' \
      -c 'CREATE TEMP TABLE pgm_commit_probe () ON COMMIT DROP' >/dev/null 2>"$err" )
    rc=$?
    case $rc:$(cat "$err") in
    0:*) echo 'gate: commit ok' ;;
    124:*'canceling wait for synchronous replication'*) echo 'gate: commit blocked'; ok=0 ;;
    *) printf 'gate: commit failed %s\n' "$(why "$rc")"; ok=0 ;;
    esac ;;
  'gate: census '*)
    s=${census#* syncrep=}
    s=${s%% *}
    if [ "$s" != 0 ]; then s="syncrep=$s"; else s="gate_waiting=${census##* gate_waiting=}"; fi
    printf 'gate: commit skipped %s\n' "$s"
    ok=0 ;;
  *) echo 'gate: commit skipped no census'; ok=0 ;;
  esac
fi
rm -f "$err" "$out"
[ "$ok" = 1 ] || exit 1
echo 'gate: pass'
`
}

// buildGateJob builds the probe that clears attempt n. It runs at the migration role's
// privileges, and backoffLimit 0 keeps the Job controller from rerunning a hold.
func buildGateJob(m *v1beta1.Migration, runnerImage string, attempt int32) (*batchv1.Job, error) {
	in := gateInputsFor(m)
	job, err := scriptJob(m, runnerImage, gateJobName(m, attempt), gateScript(m.Spec.Clone.AllDatabases), true)
	if err != nil {
		return nil, err
	}
	// The probe reads nothing from the work dir. Without the PVC, a volume detach cannot
	// get the pod deleted and take the probe's result in its log with it.
	for i := range job.Spec.Template.Spec.Volumes {
		if v := &job.Spec.Template.Spec.Volumes[i]; v.Name == workdirVolume {
			v.VolumeSource = corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
		}
	}
	backoff, deadline, ttl := int32(0), gateProbeDeadline, gateProbeTTL
	job.Spec.BackoffLimit = &backoff
	job.Spec.ActiveDeadlineSeconds = &deadline
	job.Spec.TTLSecondsAfterFinished = &ttl
	job.Annotations = map[string]string{annotationRetryGateInputs: in.String()}
	c := &job.Spec.Template.Spec.Containers[0]
	c.Env = append(c.Env,
		corev1.EnvVar{Name: "PGAPPNAME", Value: gateAppName},
		corev1.EnvVar{Name: "GATE_COMMIT_TEST", Value: gateFlag(in.CommitTest)},
		corev1.EnvVar{Name: "GATE_EXPECT_LOCAL", Value: gateFlag(in.ExpectLocal)},
	)
	return job, nil
}

// gateResult is what a probe's log says. A nil Census or SyncCommit, or an empty Commit,
// means the check gave no result line; the *Detail fields carry the filtered psql error.
type gateResult struct {
	Census           *gateCensus
	CensusDetail     string
	Sessions         []gateSession
	SyncCommit       *string
	SyncCommitDetail string
	Commit           string
	CommitDetail     string
	Pass             bool
}

// gateSession is one census session line without its "gate: session " prefix.
type gateSession struct {
	Line string
	Idle bool
}

func parseGateLog(log string) gateResult {
	var g gateResult
	for line := range strings.SplitSeq(log, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "gate: ")
		if !ok {
			continue
		}
		if v, ok := strings.CutPrefix(rest, "census failed "); ok {
			g.CensusDetail = v
		} else if strings.HasPrefix(rest, "census ") {
			if g.Census == nil {
				g.Census = parseCensus(rest)
			}
		} else if v, ok := strings.CutPrefix(rest, "session "); ok {
			// "idle in transaction" holds a transaction, so only plain idle counts.
			g.Sessions = append(g.Sessions, gateSession{Line: v, Idle: strings.Contains(v, " state=idle wait=")})
		} else if v, ok := strings.CutPrefix(rest, "synchronous_commit failed "); ok {
			g.SyncCommitDetail = v
		} else if v, ok := strings.CutPrefix(rest, "synchronous_commit="); ok {
			g.SyncCommit = &v
		} else if v, ok := strings.CutPrefix(rest, "commit "); ok {
			g.Commit, g.CommitDetail, _ = strings.Cut(v, " ")
		} else if rest == "pass" {
			g.Pass = true
		}
	}
	return g
}

// parseCensus reads a census line, or returns nil when any field is missing.
func parseCensus(line string) *gateCensus {
	var c gateCensus
	var named string
	if _, err := fmt.Sscanf(line, "census total=%d syncrep=%d named=%s syncrep_since=%d gate_waiting=%d",
		&c.Total, &c.SyncRep, &named, &c.SyncRepSince, &c.GateWaiting); err != nil {
		return nil
	}
	c.Named = named == "yes"
	return &c
}

// passed requires the pass line and each enabled check's own pass line, so a log cut short
// never reads as a pass.
func (g gateResult) passed(in gateInputs) bool {
	return g.Pass && g.Census != nil && g.Census.Total == 0 &&
		(!in.ExpectLocal || (g.SyncCommit != nil && *g.SyncCommit == v1beta1.SynchronousCommitLocal)) &&
		(!in.CommitTest || g.Commit == gateCommitOK)
}

// hold names the first check, in script order, that did not pass, and describes every one
// that did not. It returns empty strings for a passed probe.
func (g gateResult) hold(in gateInputs, target *v1beta1.PostgresConnection) (reason, message string) {
	var failed []string
	fail := func(r, msg string) {
		if reason == "" {
			reason = r
		}
		failed = append(failed, msg)
	}
	switch {
	case g.Census == nil:
		fail(reasonProbeFailed, "The session census gave no result"+gateDetail(g.CensusDetail))
	case g.Census.Total > 0:
		fail(reasonSessionsRemain, g.sessionsMessage())
	}
	if in.ExpectLocal {
		switch {
		case g.SyncCommit == nil:
			fail(reasonProbeFailed, "SHOW synchronous_commit gave no result"+gateDetail(g.SyncCommitDetail))
		case *g.SyncCommit != v1beta1.SynchronousCommitLocal:
			fail(reasonSyncCommitNotLocal, "The target connection reports synchronous_commit="+*g.SyncCommit+
				", not local; a connection pooler or proxy dropped the libpq startup option")
		}
	}
	if in.CommitTest {
		switch g.Commit {
		case gateCommitOK:
		case gateCommitBlocked:
			fail(reasonCommitBlocked, "The test commit waited for synchronous replication until the probe cancelled it")
		case gateCommitSkipped:
			fail(reasonCommitBlocked, skippedMessage(g.CommitDetail))
		case gateCommitFailed:
			fail(reasonProbeFailed, "The test commit failed: "+g.CommitDetail+grantRemedy(g.CommitDetail, target))
		default:
			fail(reasonProbeFailed, "The test commit gave no result")
		}
	}
	if reason == "" && !g.passed(in) {
		fail(reasonProbeFailed, "The probe log has no gate: pass line")
	}
	return reason, strings.Join(failed, ". ")
}

func (g gateResult) sessionsMessage() string {
	lines := make([]string, 0, len(g.Sessions))
	idle := len(g.Sessions) > 0
	for _, s := range g.Sessions {
		lines = append(lines, s.Line)
		idle = idle && s.Idle
	}
	msg := fmt.Sprintf("Sessions of the migration role remain on the target: %d in total, %d listed: %s",
		g.Census.Total, len(lines), strings.Join(lines, "; "))
	if !g.Census.Named {
		msg += ". The target URI pins application_name, so every session of the role counts"
	}
	if idle {
		msg += ". A connection pooler can keep idle server connections under the last client's name;" +
			" ending an idle session by hand is safe because it holds no transaction"
	}
	return msg
}

func skippedMessage(detail string) string {
	switch {
	case strings.HasPrefix(detail, "syncrep="):
		return "The test commit was skipped because sessions wait for synchronous replication (" + detail + ")"
	case strings.HasPrefix(detail, "gate_waiting="):
		return "The test commit was skipped because an earlier probe still waits for synchronous replication (" + detail + ")"
	}
	return "The test commit was skipped: " + detail
}

// grantRemedy names the grant a commit that failed with SQLSTATE 42501 lacks. The names are
// quoted, so a mixed-case database or role pastes as is; a non-inline target leaves placeholders.
func grantRemedy(detail string, target *v1beta1.PostgresConnection) string {
	if !strings.Contains(detail, "ERROR:  42501:") {
		return ""
	}
	db, role := "<database>", "<role>"
	if target.Database != "" {
		db = quoteIdent(target.Database)
	}
	if target.Username != "" {
		role = quoteIdent(target.Username)
	}
	return "; run GRANT TEMPORARY ON DATABASE " + db + " TO " + role + " on the target"
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func gateDetail(detail string) string {
	if detail == "" {
		return ""
	}
	return ": " + detail
}
