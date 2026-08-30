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

package sentinel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/streaming/pkg/httpstream/wsstream"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

// serveExecSuccess answers one exec request over the v5 websocket protocol:
// out goes to the stdout channel (index 1 of stdin, stdout, stderr, error,
// resize), then the connection closes normally with the error channel empty,
// which the client reads as a successful exec.
func serveExecSuccess(w http.ResponseWriter, r *http.Request, out []byte) {
	conn := wsstream.NewConn(map[string]wsstream.ChannelProtocolConfig{
		"v5.channel.k8s.io": {Binary: true, Channels: []wsstream.ChannelType{
			wsstream.ReadChannel, wsstream.WriteChannel, wsstream.WriteChannel,
			wsstream.WriteChannel, wsstream.ReadChannel,
		}},
	})
	conn.SetIdleTimeout(time.Minute)
	_, streams, err := conn.Open(w, r)
	if err != nil {
		return
	}
	defer conn.Close() //nolint:errcheck
	_, _ = streams[1].Write(out)
}

// The LSNs the tables below repeat: a set endpos, and one healthy sample of
// write, replay, and source head.
const (
	endposLSN = "0/A0"
	writeLSN  = "0/2000"
	replayLSN = "0/1000"
	headLSN   = "0/3000"
)

func TestParseLSN(t *testing.T) {
	cases := map[string]uint64{
		ZeroLSN:      0,
		"0/5000000":  0x5000000,
		"1/0":        1 << 32,
		"A/2C000028": 0xA<<32 | 0x2C000028,
	}
	for in, want := range cases {
		got, err := ParseLSN(in)
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got != want {
			t.Fatalf("%s: got %d want %d", in, got, want)
		}
	}
	for _, bad := range []string{"", "nope", "1-2", "zz/0", "1/zz"} {
		if _, err := ParseLSN(bad); err == nil {
			t.Fatalf("%q: error expected", bad)
		}
	}
}

func TestEndposSet(t *testing.T) {
	for lsn, want := range map[string]bool{"": false, ZeroLSN: false, endposLSN: true} {
		if got := EndposSet(lsn); got != want {
			t.Fatalf("EndposSet(%q) = %v, want %v", lsn, got, want)
		}
	}
}

func TestStateLag(t *testing.T) {
	s := State{SourceHead: "0/5000100", ReplayLSN: "0/5000000"}
	if got := s.Lag(); got != 0x100 {
		t.Fatalf("lag: got %d want %d", got, 0x100)
	}
	// Unknown when either side is missing or malformed.
	if got := (State{SourceHead: "0/1"}).Lag(); got != -1 {
		t.Fatalf("missing replay: got %d", got)
	}
	// Head behind replay (clock skew between samples) reads as unknown, not
	// negative: callers treat -1 as "no sample".
	if got := (State{SourceHead: "0/1", ReplayLSN: "0/2"}).Lag(); got != -1 {
		t.Fatalf("behind: got %d", got)
	}
}

func TestToStatus(t *testing.T) {
	s := &State{WriteLSN: "0/20", ReplayLSN: "0/10", Endpos: "0/0", SourceHead: "0/30"}
	rs := s.ToStatus("slot1")
	if rs.SlotName != "slot1" || rs.WriteLSN != "0/20" || rs.ReplayLSN != "0/10" {
		t.Fatalf("bad status: %+v", rs)
	}
	if rs.Endpos != "" {
		t.Fatal("zero endpos must render empty")
	}
	// The metric contract is that an unknown value is absent, never zero.
	zero := (&State{WriteLSN: ZeroLSN, ReplayLSN: ZeroLSN, Endpos: ZeroLSN}).ToStatus("slot1")
	if zero.WriteLSN != "" || zero.ReplayLSN != "" || zero.Endpos != "" {
		t.Fatalf("null LSNs must render empty: %+v", zero)
	}
	if rs.LagBytes == nil || *rs.LagBytes != 0x20 {
		t.Fatalf("lag bytes: %+v", rs.LagBytes)
	}
	if (*State)(nil).ToStatus("x") != nil {
		t.Fatal("nil state must yield nil status")
	}
}

func TestToStatus_EndposSetAndLagUnknown(t *testing.T) {
	// A frozen stream carries its cutover LSN into status; before the first
	// WAL-head sample, lag is unknown and must stay absent, not read as 0.
	s := &State{WriteLSN: "0/21", ReplayLSN: "0/11", Endpos: endposLSN}
	rs := s.ToStatus("slot1")
	if rs.Endpos != endposLSN {
		t.Fatalf("endpos: %q", rs.Endpos)
	}
	if rs.LagBytes != nil {
		t.Fatalf("lag must be unknown without a source head, got %d", *rs.LagBytes)
	}
}

// execRecorder collects the argv of every exec request the fake API saw; the
// command arrives as repeated `command` query parameters before the upgrade
// is refused, so tests can allowlist-match what would have run in the pod.
type execRecorder struct {
	mu   sync.Mutex
	cmds [][]string
	// outputs scripts successful execs: the first entry whose key is a
	// substring of the joined argv is served as that exec's stdout; anything
	// unscripted keeps the refused-upgrade failure.
	outputs map[string]string
}

func (rec *execRecorder) add(argv []string) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.cmds = append(rec.cmds, argv)
}

func (rec *execRecorder) commands() [][]string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.cmds
}

func (rec *execRecorder) script(outputs map[string]string) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.outputs = outputs
}

func (rec *execRecorder) respond(argv []string) (string, bool) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	joined := strings.Join(argv, " ")
	for key, out := range rec.outputs {
		if strings.Contains(joined, key) {
			return out, true
		}
	}
	return "", false
}

// fakeAPI serves a pod list (or an error). Exec requests are recorded, then
// fail because the server never upgrades the connection: for Read that is the
// not-ready contract under test, for SetEndposCurrent it is a hard error.
func fakeAPI(t *testing.T, pods []corev1.Pod, fail bool) (*Client, *execRecorder) {
	t.Helper()
	rec := &execRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case fail:
			http.Error(w, "boom", http.StatusInternalServerError)
		case strings.HasSuffix(r.URL.Path, "/pods"):
			w.Header().Set("Content-Type", "application/json")
			list := corev1.PodList{TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"}, Items: pods}
			_ = json.NewEncoder(w).Encode(list)
		case strings.HasSuffix(r.URL.Path, "/exec"):
			argv := r.URL.Query()["command"]
			rec.add(argv)
			if out, ok := rec.respond(argv); ok {
				serveExecSuccess(w, r, []byte(out))
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	e, err := podexec.New(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return New(e), rec
}

func runningPod() corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "w-0", Namespace: "ns", Labels: map[string]string{"job-name": "j"}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// TestParseSample covers what the source answers through a migration's life,
// including the states no test cluster reproduces on demand. The labels come
// off psql, so a value can always be blank and a line can always be missing.
func TestParseSample(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want *State
	}{{
		name: "full sample",
		out:  "source_head=0/3000\nwrite_lsn=0/2000 replay_lsn=0/1000\n",
		want: &State{WriteLSN: writeLSN, ReplayLSN: replayLSN, SourceHead: headLSN},
	}, {
		// The slot is created with the migration; until then the operator
		// has learned nothing and the previous status must stand.
		name: "no slot row yet",
		out:  "source_head=0/3000\n",
		want: nil,
	}, {
		// No walsender row (receiver reconnecting), or a role that may not
		// see one: both positions fall back to the slot's confirmed flush,
		// so they arrive equal rather than absent.
		name: "slot present, no walsender row",
		out:  "source_head=0/3000\nwrite_lsn=0/2000 replay_lsn=0/2000\n",
		want: &State{WriteLSN: writeLSN, ReplayLSN: writeLSN, SourceHead: headLSN},
	}, {
		// A slot that has confirmed nothing yet: the row is there, the
		// positions are not.
		name: "slot row with no positions",
		out:  "source_head=0/3000\nwrite_lsn= replay_lsn=\n",
		want: nil,
	}, {
		name: "garbage",
		out:  "psql: error: connection to server failed: FATAL: sslmode=require\n",
		want: nil,
	}, {
		name: "empty",
		out:  "",
		want: nil,
	}, {
		// A value that is not an LSN is treated as absent, never stored.
		name: "unparsable values",
		out:  "source_head=later\nwrite_lsn=0/2000 replay_lsn=zz/1\n",
		want: &State{WriteLSN: writeLSN},
	}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseSample([]byte(c.out))
			switch {
			case c.want == nil && got != nil:
				t.Fatalf("want no sample, got %+v", *got)
			case c.want != nil && got == nil:
				t.Fatalf("want %+v, got no sample", *c.want)
			case c.want != nil && *got != *c.want:
				t.Fatalf("got %+v, want %+v", *got, *c.want)
			}
		})
	}
}

// TestReadScript pins where the lag figure comes from. Both positions are
// read from the source slot row with the same fallback, and the target's
// replication origin must not appear here at all: it parks at the last
// applied COMMIT, so on a source whose recent WAL is filtered or idle it
// trails without bound, and driving lag off it hung a release candidate at
// 1.95 GB of reported lag on a fully caught-up migration. The origin is the
// stricter position and it keeps its job, one layer over, in buildVerifyJob.
//
// What this can prove is the wiring: if anyone reaches for the origin from
// the watch again, this fails. What it cannot prove is the semantics, since
// no fake answers two real LSN sources; that took two databases, and the
// numbers are in the commit message.
func TestReadScript(t *testing.T) {
	script := readScript("ns_mig_1")
	for _, want := range []string{
		"where s.slot_name = 'ns_mig_1'",
		"pg_current_wal_flush_lsn()",
		"coalesce(r.write_lsn, s.confirmed_flush_lsn)",
		"coalesce(r.replay_lsn, s.confirmed_flush_lsn)",
		"exit 0",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script misses %q:\n%s", want, script)
		}
	}
	// The lag path reads the source and only the source.
	for _, forbidden := range []string{
		"pg_replication_origin",
		"PGCOPYDB_TARGET_PGURI",
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("the lag path must not read %q; that is the drain check's job:\n%s", forbidden, script)
		}
	}
	// Nothing in the watch may open the worker's catalogs any more.
	if strings.Contains(script, "pgcopydb") {
		t.Fatalf("the watch must not run pgcopydb:\n%s", script)
	}
}

func TestRead_NoPodOrFailedExecIsNoSample(t *testing.T) {
	// No running pod: (nil, nil), the previous sample stands.
	c, _ := fakeAPI(t, nil, false)
	st, err := c.Read(context.Background(), "ns", "j", "s")
	if err != nil || st != nil {
		t.Fatalf("no pod: want (nil, nil), got (%v, %v)", st, err)
	}
	// Pod up but the exec fails (starting, terminating, transport hiccup):
	// also (nil, nil), never a reconcile error.
	var rec *execRecorder
	c, rec = fakeAPI(t, []corev1.Pod{runningPod()}, false)
	st, err = c.Read(context.Background(), "ns", "j", "s")
	if err != nil || st != nil {
		t.Fatalf("exec refused: want (nil, nil), got (%v, %v)", st, err)
	}
	// The exec routes through the shared shell wrapper with the URI recovery
	// for both sides; a bare argv would break only on secretRef migrations,
	// which no unit fake catches later. (A refused exec is recorded once per
	// transport the fallback executor tries, so only the command is asserted
	// here; TestRead_FullSample counts the execs of a healthy pass.)
	cmds := rec.commands()
	if len(cmds) == 0 {
		t.Fatal("no exec recorded")
	}
	argv := strings.Join(cmds[0], " ")
	for _, want := range []string{
		"sh -c",
		// The recovery prelude is the shared wrapper and restores both sides
		// whatever the command does with them.
		"[ -f /tmp/pgm-source-uri ]",
		"[ -f /tmp/pgm-target-uri ]",
		`psql "$PGCOPYDB_SOURCE_PGURI"`,
	} {
		if !strings.Contains(argv, want) {
			t.Fatalf("exec %q misses %q", argv, want)
		}
	}
	// The watch queries the source alone; see TestReadScript for why.
	if strings.Contains(argv, `psql "$PGCOPYDB_TARGET_PGURI"`) {
		t.Fatalf("the watch must not query the target: %q", argv)
	}
}

func TestRead_ListError(t *testing.T) {
	c, _ := fakeAPI(t, nil, true)
	if _, err := c.Read(context.Background(), "ns", "j", "s"); err == nil {
		t.Fatal("want error when the pod list fails")
	}
}

func TestRead_FullSample(t *testing.T) {
	c, rec := fakeAPI(t, []corev1.Pod{runningPod()}, false)
	rec.script(map[string]string{"pg_replication_slots": "source_head=0/3000\n" +
		"write_lsn=0/2000 replay_lsn=0/1000\n"})
	st, err := c.Read(context.Background(), "ns", "j", "s")
	if err != nil || st == nil {
		t.Fatalf("want a sample, got (%v, %v)", st, err)
	}
	want := State{WriteLSN: writeLSN, ReplayLSN: replayLSN, SourceHead: headLSN}
	if *st != want {
		t.Fatalf("sample = %+v, want %+v", *st, want)
	}
	// The whole point of the rewrite: one exec per pass, and it is psql.
	if cmds := rec.commands(); len(cmds) != 1 {
		t.Fatalf("want exactly one exec per pass, got %d", len(cmds))
	}
}

func TestSetEndposCurrent_Success(t *testing.T) {
	c, rec := fakeAPI(t, []corev1.Pod{runningPod()}, false)
	rec.script(map[string]string{"sentinel set endpos": "0/5000\n"})
	lsn, err := c.SetEndposCurrent(context.Background(), "ns", "j")
	if err != nil {
		t.Fatal(err)
	}
	if lsn != "0/5000" {
		t.Fatalf("endpos = %q, want the trimmed CLI output", lsn)
	}
}

func TestSetEndposCurrent_Errors(t *testing.T) {
	// Unlike Read, cutover must not silently do nothing: every failure is loud.
	c, _ := fakeAPI(t, nil, false)
	if _, err := c.SetEndposCurrent(context.Background(), "ns", "j"); err == nil ||
		!strings.Contains(err.Error(), "no running worker pod") {
		t.Fatalf("no pod must fail loudly, got %v", err)
	}
	c, _ = fakeAPI(t, nil, true)
	if _, err := c.SetEndposCurrent(context.Background(), "ns", "j"); err == nil {
		t.Fatal("want error when the pod list fails")
	}
	c, _ = fakeAPI(t, []corev1.Pod{runningPod()}, false)
	if _, err := c.SetEndposCurrent(context.Background(), "ns", "j"); err == nil {
		t.Fatal("want error when the exec fails")
	}
}

func TestNudgeEndpos(t *testing.T) {
	// No running pod: nothing to nudge, no error (best effort, like Read).
	c, rec := fakeAPI(t, nil, false)
	if err := c.NudgeEndpos(context.Background(), "ns", "j"); err != nil {
		t.Fatalf("no pod must be a no-op, got %v", err)
	}
	if len(rec.commands()) != 0 {
		t.Fatalf("no pod must not exec, got %v", rec.commands())
	}
	// Pod-list failure surfaces, same as Read.
	c, _ = fakeAPI(t, nil, true)
	if err := c.NudgeEndpos(context.Background(), "ns", "j"); err == nil {
		t.Fatal("want error when the pod list fails")
	}
	// Pod up: the exec must carry the logical-message emit; the refused
	// upgrade surfaces as an error the caller merely debug-logs.
	c, rec = fakeAPI(t, []corev1.Pod{runningPod()}, false)
	if err := c.NudgeEndpos(context.Background(), "ns", "j"); err == nil {
		t.Fatal("want error when the exec fails")
	}
	cmds := rec.commands()
	if len(cmds) == 0 {
		t.Fatal("no exec recorded")
	}
	argv := strings.Join(cmds[0], " ")
	for _, want := range []string{
		// The recovery prefix restores the URIs for secretRef connections,
		// where the spec env cannot carry them.
		`[ -f /tmp/pgm-source-uri ]`,
		`[ -f /tmp/pgm-target-uri ]`,
		`psql "$PGCOPYDB_SOURCE_PGURI" -Xqtc`,
		`pg_logical_emit_message(false, 'pgcopydb-operator', 'endpos-nudge')`,
	} {
		if !strings.Contains(argv, want) {
			t.Fatalf("exec %q misses %q", argv, want)
		}
	}
}
