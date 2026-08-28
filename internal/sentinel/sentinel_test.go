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
	s := &State{WriteLSN: "0/21", ReplayLSN: "0/11", Endpos: "0/A0"}
	rs := s.ToStatus("slot1")
	if rs.Endpos != "0/A0" {
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

func TestRead_NoPodOrNoSentinelIsNoSample(t *testing.T) {
	// No running pod: (nil, nil), the previous sample stands.
	c, _ := fakeAPI(t, nil, false)
	st, err := c.Read(context.Background(), "ns", "j")
	if err != nil || st != nil {
		t.Fatalf("no pod: want (nil, nil), got (%v, %v)", st, err)
	}
	// Pod up but the sentinel query fails (follow setup not done yet, or the
	// exec transport hiccuped): also (nil, nil), never a reconcile error.
	var rec *execRecorder
	c, rec = fakeAPI(t, []corev1.Pod{runningPod()}, false)
	st, err = c.Read(context.Background(), "ns", "j")
	if err != nil || st != nil {
		t.Fatalf("sentinel not ready: want (nil, nil), got (%v, %v)", st, err)
	}
	// Every exec routes through the shared shell wrapper with the URI
	// recovery for both sides; a bare argv would break only on secretRef
	// migrations, which no unit fake catches later.
	cmds := rec.commands()
	if len(cmds) == 0 {
		t.Fatal("no exec recorded")
	}
	argv := strings.Join(cmds[0], " ")
	for _, want := range []string{
		"sh -c",
		"[ -f /tmp/pgm-source-uri ]",
		"[ -f /tmp/pgm-target-uri ]",
		"pgcopydb stream sentinel get --apply",
	} {
		if !strings.Contains(argv, want) {
			t.Fatalf("exec %q misses %q", argv, want)
		}
	}
}

func TestRead_ListError(t *testing.T) {
	c, _ := fakeAPI(t, nil, true)
	if _, err := c.Read(context.Background(), "ns", "j"); err == nil {
		t.Fatal("want error when the pod list fails")
	}
}

// sentinelOutputs scripts one full healthy sample; tests copy and prune it.
func sentinelOutputs() map[string]string {
	return map[string]string{
		"sentinel get --apply":      "enabled\n",
		"sentinel get --write-lsn":  "0/2000\n",
		"sentinel get --replay-lsn": "0/1000\n",
		"sentinel get --endpos":     "0/0\n",
		"pg_current_wal_flush_lsn":  "0/3000\n",
	}
}

func TestRead_FullSample(t *testing.T) {
	c, rec := fakeAPI(t, []corev1.Pod{runningPod()}, false)
	rec.script(sentinelOutputs())
	st, err := c.Read(context.Background(), "ns", "j")
	if err != nil || st == nil {
		t.Fatalf("want a sample, got (%v, %v)", st, err)
	}
	want := State{ApplyEnabled: true, WriteLSN: "0/2000", ReplayLSN: "0/1000", Endpos: "0/0", SourceHead: "0/3000"}
	if *st != want {
		t.Fatalf("sample = %+v, want %+v", *st, want)
	}
}

// TestRead_SelectorFailureIsNoSample: any selector failing after the sentinel
// answered once still yields (nil, nil), never a partial sample: the caller
// keeps the previous one.
func TestRead_SelectorFailureIsNoSample(t *testing.T) {
	for _, broken := range []string{"--write-lsn", "--replay-lsn", "--endpos"} {
		c, rec := fakeAPI(t, []corev1.Pod{runningPod()}, false)
		outputs := sentinelOutputs()
		delete(outputs, "sentinel get "+broken)
		rec.script(outputs)
		st, err := c.Read(context.Background(), "ns", "j")
		if err != nil || st != nil {
			t.Fatalf("%s failing: want (nil, nil), got (%v, %v)", broken, st, err)
		}
	}
}

// TestRead_SourceHeadBestEffort: the WAL-head query failing must not lose the
// sentinel sample; lag simply reads as unknown until the next pass.
func TestRead_SourceHeadBestEffort(t *testing.T) {
	c, rec := fakeAPI(t, []corev1.Pod{runningPod()}, false)
	outputs := sentinelOutputs()
	delete(outputs, "pg_current_wal_flush_lsn")
	rec.script(outputs)
	st, err := c.Read(context.Background(), "ns", "j")
	if err != nil || st == nil {
		t.Fatalf("want a sample, got (%v, %v)", st, err)
	}
	if st.SourceHead != "" {
		t.Fatalf("source head must stay empty when psql fails, got %q", st.SourceHead)
	}
	if st.WriteLSN != "0/2000" || !st.ApplyEnabled {
		t.Fatalf("sentinel fields lost: %+v", st)
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
