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

package progress

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ydixken/pgcopydb-operator/internal/conn"
)

// patchedVersion is the allowlisted fixture version across these tests.
const patchedVersion = "0.18.2.gea87951"

// fakeExec scripts the podexec surface: tests choose the pod lookup result
// and the exec output, and read back what was executed.
type fakeExec struct {
	pod     string
	podErr  error
	out     []byte
	execErr error
	argv    []string
	calls   int
}

func (f *fakeExec) RunningPod(context.Context, string, string) (string, error) {
	f.calls++
	return f.pod, f.podErr
}

func (f *fakeExec) InPod(_ context.Context, _, _ string, argv []string) ([]byte, error) {
	f.calls++
	f.argv = argv
	return f.out, f.execErr
}

func TestNewFromExec_DropsInvalidVersions(t *testing.T) {
	p := NewFromExec(&fakeExec{}, []string{
		patchedVersion,
		"1.0-beta_2",
		"0.18;rm -rf /",
		"$(reboot)",
		"a b",
		"*",
		"",
	})
	want := []string{patchedVersion, "1.0-beta_2"}
	if len(p.allowed) != len(want) {
		t.Fatalf("allowed = %v, want %v", p.allowed, want)
	}
	for i, v := range want {
		if p.allowed[i] != v {
			t.Fatalf("allowed = %v, want %v", p.allowed, want)
		}
	}
}

func TestGateScript(t *testing.T) {
	p := NewFromExec(&fakeExec{}, []string{patchedVersion, "0.19"})
	s := p.GateScript()
	for _, want := range []string{
		"0.18.2.gea87951|0.19)",
		"pgcopydb list progress --json --dir /work/pgcopydb",
		"v=${v#pgcopydb version }",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("gate script misses %q:\n%s", want, s)
		}
	}
}

// stubPgcopydb drops a pgcopydb stand-in on PATH that reports the given
// version and answers `list progress` with canned JSON.
func stubPgcopydb(t *testing.T, version string) string {
	t.Helper()
	dir := t.TempDir()
	stub := `#!/bin/sh
case "$1" in
--version)
  echo "pgcopydb version ` + version + `"
  echo "compiled with PostgreSQL 18.0"
  ;;
list)
  echo '{"tables":{"total":2,"done":1}}'
  ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "pgcopydb"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestGateScript_Disabled: an empty allowlist renders no script. Rendering
// the case statement with no pattern would be a syntax error, and the verify
// Job embeds whatever comes back into a larger script, so "the poll is off"
// has to mean nothing is emitted rather than something that cannot parse.
func TestGateScript_Disabled(t *testing.T) {
	for name, versions := range map[string][]string{
		"nil":                 nil,
		"empty":               {},
		"all entries invalid": {"; rm -rf /", "$(id)"},
	} {
		if s := NewFromExec(&fakeExec{}, versions).GateScript(); s != "" {
			t.Errorf("%s allowlist must render nothing, got:\n%s", name, s)
		}
	}
}

// TestGateScript_UnderSh proves the gate on a real shell: an allowlisted
// version opens it, any other version produces no output and a zero exit,
// and every allowlist shape renders something a shell will accept.
func TestGateScript_UnderSh(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("no sh available: %v", err)
	}
	p := NewFromExec(&fakeExec{}, []string{patchedVersion})
	for version, want := range map[string]string{
		patchedVersion: `{"tables":{"total":2,"done":1}}` + "\n",
		"0.18":         "",
	} {
		cmd := exec.Command(sh, "-c", p.GateScript())
		cmd.Env = append(os.Environ(), "PATH="+stubPgcopydb(t, version)+":"+os.Getenv("PATH"))
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("version %s: %v", version, err)
		}
		if string(out) != want {
			t.Errorf("version %s: output %q, want %q", version, out, want)
		}
	}
	// The disabled gate has to parse too: it is pasted into the verify Job's
	// script, where a syntax error would be the whole file's problem.
	for _, versions := range [][]string{nil, {patchedVersion}} {
		script := NewFromExec(&fakeExec{}, versions).GateScript()
		if err := exec.Command(sh, "-n", "-c", script).Run(); err != nil {
			t.Errorf("allowlist %v renders a script sh rejects: %v\n%s", versions, err, script)
		}
	}
}

func TestCloneProgress_FailClosed(t *testing.T) {
	ctx := context.Background()
	for name, f := range map[string]*fakeExec{
		"no running pod": {pod: ""},
		"exec error":     {pod: "p", execErr: errors.New("exec refused")},
		"empty output":   {pod: "p", out: []byte("  \n")},
		"non-JSON":       {pod: "p", out: []byte("pgcopydb: fatal")},
	} {
		p := NewFromExec(f, []string{patchedVersion})
		cp, err := p.CloneProgress(ctx, "ns", "job")
		if cp != nil || err != nil {
			t.Errorf("%s: = (%v, %v), want (nil, nil)", name, cp, err)
		}
	}
}

func TestCloneProgress_DisabledAndPodErr(t *testing.T) {
	ctx := context.Background()

	// Empty allowlist: shut for good, no exec traffic at all.
	f := &fakeExec{pod: "p", out: []byte("{}")}
	if cp, err := NewFromExec(f, nil).CloneProgress(ctx, "ns", "job"); cp != nil || err != nil || f.calls != 0 {
		t.Fatalf("disabled poller: = (%v, %v) after %d calls, want (nil, nil) and none", cp, err, f.calls)
	}

	// A pod lookup failure is an API error, not a shut gate.
	f = &fakeExec{podErr: errors.New("api down")}
	if _, err := NewFromExec(f, []string{patchedVersion}).CloneProgress(ctx, "ns", "job"); err == nil {
		t.Fatal("pod lookup error: want it surfaced")
	}
}

func TestCloneProgress_Sample(t *testing.T) {
	f := &fakeExec{pod: "p", out: []byte(`{"tables":{"total":5,"done":2},"bytes":{"total":100,"done":40}}`)}
	p := NewFromExec(f, []string{patchedVersion})
	cp, err := p.CloneProgress(context.Background(), "ns", "job")
	if err != nil || cp == nil {
		t.Fatalf("= (%v, %v), want a sample", cp, err)
	}
	if cp.TablesTotal != 5 || cp.TablesDone != 2 || cp.BytesTotal.Value() != 100 || cp.BytesDone.Value() != 40 {
		t.Fatalf("bad sample: %+v", cp)
	}
	if len(f.argv) != 3 || f.argv[0] != "sh" || f.argv[1] != "-c" || f.argv[2] != p.GateScript() {
		t.Fatalf("exec argv = %v, want the gate script under sh -c", f.argv)
	}
}

func TestDatabaseSizes(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		out      string
		src, tgt *int64
	}{
		"both sides":     {"source=1073741824\ntarget=999\n", ptr(1073741824), ptr(999)},
		"target failed":  {"source=42\ntarget=\n", ptr(42), nil},
		"source garbage": {"source=oom-killed\ntarget=7\n", nil, ptr(7)},
		"no output":      {"", nil, nil},
	} {
		f := &fakeExec{pod: "p", out: []byte(tc.out)}
		src, tgt, err := NewFromExec(f, nil).DatabaseSizes(ctx, "ns", "job")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !eq(src, tc.src) || !eq(tgt, tc.tgt) {
			t.Errorf("%s: = (%v, %v), want (%v, %v)", name, deref(src), deref(tgt), deref(tc.src), deref(tc.tgt))
		}
		// The exec must restore the secretRef URIs before touching psql.
		if !strings.HasPrefix(f.argv[2], conn.URIRecover()) {
			t.Errorf("%s: script misses the URI recovery prelude", name)
		}
	}
}

func TestDatabaseSizes_Errors(t *testing.T) {
	ctx := context.Background()

	if src, tgt, err := NewFromExec(&fakeExec{pod: ""}, nil).DatabaseSizes(ctx, "ns", "job"); src != nil || tgt != nil || err != nil {
		t.Fatalf("no pod: = (%v, %v, %v), want all nil", src, tgt, err)
	}
	if _, _, err := NewFromExec(&fakeExec{podErr: errors.New("api down")}, nil).DatabaseSizes(ctx, "ns", "job"); err == nil {
		t.Fatal("pod lookup error: want it surfaced")
	}
	if _, _, err := NewFromExec(&fakeExec{pod: "p", execErr: errors.New("exec refused")}, nil).DatabaseSizes(ctx, "ns", "job"); err == nil {
		t.Fatal("exec error: want it surfaced")
	}
}

func ptr(n int64) *int64 { return &n }

func eq(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func deref(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// CloneStage decides a user-visible phase, so every way the probe can fail has
// to land on "unknown" rather than on a confident wrong answer. Unknown is
// both flags false, which leaves the caller reporting Cloning.
func TestCloneStage(t *testing.T) {
	for _, tc := range []struct {
		name                string
		exec                *fakeExec
		copying, finalizing bool
	}{
		{
			name:    "copy workers busy",
			exec:    &fakeExec{pod: "w", out: []byte("4 0\n")},
			copying: true,
		},
		{
			name:       "only the tail left",
			exec:       &fakeExec{pod: "w", out: []byte("0 1\n")},
			finalizing: true,
		},
		{
			// Both counts zero is a worker that holds no backend the query
			// counts: not connected yet, or already gone. Neither state.
			name: "no counted backends",
			exec: &fakeExec{pod: "w", out: []byte("0 0\n")},
		},
		{
			// The copy is winding down while the tail has started. Still
			// copying, because data is still moving.
			name:    "both kinds active",
			exec:    &fakeExec{pod: "w", out: []byte("2 3\n")},
			copying: true,
		},
		{name: "no running pod", exec: &fakeExec{}},
		{name: "pod lookup failed", exec: &fakeExec{podErr: errors.New("boom")}},
		{name: "exec failed", exec: &fakeExec{pod: "w", execErr: errors.New("boom")}},
		{name: "unparseable output", exec: &fakeExec{pod: "w", out: []byte("ERROR: nope\n")}},
		{name: "empty output", exec: &fakeExec{pod: "w", out: nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewFromExec(tc.exec, nil)
			copying, finalizing := p.CloneStage(context.Background(), "ns", "job")
			if copying != tc.copying || finalizing != tc.finalizing {
				t.Errorf("copying=%v finalizing=%v, want copying=%v finalizing=%v",
					copying, finalizing, tc.copying, tc.finalizing)
			}
		})
	}
}

// The probe must never open pgcopydb's SQLite catalog: doing that during a
// copy is what killed workers and made the poll conditional in the first
// place. It must also only count this worker's own backends, or another
// migration's compare workers read as this clone's tail.
func TestCloneStageQueryIsCatalogFreeAndScoped(t *testing.T) {
	f := &fakeExec{pod: "w", out: []byte("0 1\n")}
	NewFromExec(f, nil).CloneStage(context.Background(), "ns", "job")

	joined := strings.Join(f.argv, " ")
	for _, forbidden := range []string{"pgcopydb", "--dir", "list progress"} {
		if strings.Contains(joined, forbidden) && forbidden != "pgcopydb" {
			t.Errorf("probe runs %q; it must not touch the pgcopydb catalog: %s", forbidden, joined)
		}
	}
	if !strings.Contains(joined, "pg_stat_activity") {
		t.Errorf("probe does not read pg_stat_activity: %s", joined)
	}
	if !strings.Contains(joined, "client_addr = inet_client_addr()") {
		t.Errorf("probe is not scoped to this worker's own backends: %s", joined)
	}
}

// The two counts are asked differently on purpose, so the predicate is pinned
// by shape rather than by text: on a live worker all four copy workers stayed
// connected for the whole base copy, and counting only the active ones read as
// the tail while data was still moving.
//
// Both narrowings are pinned by absence, because a narrowing can be spelled
// any number of ways and only its absence is checkable. The copy count may
// name state exactly once, in the arm that falls back to the statement text,
// and the row filter may not name it at all. The edit this guards against is
// a likely one: anyone acting on a copy worker that lingers connected after
// its queue drains reaches for exactly such a conjunction.
func TestCloneStageCountsCopyWorkersByConnection(t *testing.T) {
	f := &fakeExec{pod: "w", out: []byte("4 1\n")}
	NewFromExec(f, nil).CloneStage(context.Background(), "ns", "job")
	// Case folded: SQL is case insensitive, so a narrowing spelled STATE has
	// to fail these checks the same way a lowercase one does.
	flat := strings.ToLower(strings.Join(strings.Fields(strings.Join(f.argv, " ")), " "))

	copyCount, rest, ok := strings.Cut(flat, "|| ' ' ||")
	if !ok {
		t.Fatalf("probe no longer asks for two counts: %s", flat)
	}
	tailCount, afterFrom, ok := strings.Cut(rest, "from pg_stat_activity")
	if !ok {
		t.Fatalf("probe no longer reads pg_stat_activity: %s", flat)
	}
	// The SQL ends at the psql argument's closing quote; the URI prelude's
	// own quotes are all behind us by here.
	where, _, ok := strings.Cut(afterFrom, `"`)
	if !ok {
		t.Fatalf("cannot find the end of the probe query: %s", flat)
	}

	// A copy worker counts while it is connected, so the first arm of the
	// copy count tests the name and nothing else.
	primary, _, ok := strings.Cut(copyCount, " or ")
	if !ok {
		t.Fatalf("the copy count lost its statement-text fallback: %s", copyCount)
	}
	if !strings.Contains(primary, "application_name ilike '%copy worker%'") || strings.Contains(primary, "state") {
		t.Errorf("copy workers are not counted by connection: %s", primary)
	}
	// One mention of state in the whole copy count, the fallback arm's own.
	// A second one narrows the count however it is parenthesized, including
	// a conjunction wrapped around every arm at once.
	if n := strings.Count(copyCount, "state"); n != 1 {
		t.Errorf("copy count names state %d times, want 1 (the fallback arm alone): %s", n, copyCount)
	}
	if !strings.Contains(tailCount, "state = 'active'") {
		t.Errorf("the tail count dropped its active test: %s", tailCount)
	}
	if strings.Contains(where, "state") {
		t.Errorf("the row filter narrows by state, which is the bug this fixed: %s", where)
	}
}
