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
	s := p.gateScript()
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

// TestGateScript_UnderSh proves the gate on a real shell: an allowlisted
// version opens it, any other version produces no output and a zero exit.
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
		cmd := exec.Command(sh, "-c", p.gateScript())
		cmd.Env = append(os.Environ(), "PATH="+stubPgcopydb(t, version)+":"+os.Getenv("PATH"))
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("version %s: %v", version, err)
		}
		if string(out) != want {
			t.Errorf("version %s: output %q, want %q", version, out, want)
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
	if len(f.argv) != 3 || f.argv[0] != "sh" || f.argv[1] != "-c" || f.argv[2] != p.gateScript() {
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
