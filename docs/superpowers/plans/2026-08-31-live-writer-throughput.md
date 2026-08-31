# Persistent e2e live writer throughput implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the live writer's per-row `kubectl exec` bottleneck while preserving one committed transaction per ordered row, lifecycle failures, and the data-loss acceptance checks.

**Architecture:** `liveWriter` owns one persistent interactive psql child.
It resolves the source primary once at startup, captures that pod, and uses the same pod for the stream and the required post-Wait marker query.
Three private fields default to `primaryPod`, `exec.Command`, and `liveWriteInterval`, which gives package-local subprocess tests control without adding an interface or dependency.

**Tech Stack:** Go standard library, `os/exec`, Ginkgo v2, Gomega, and the existing CNPG-backed e2e suite.

**Spec:** `docs/superpowers/specs/2026-08-31-live-writer-throughput-design.md`

## Global constraints

- Do not add a dependency, public seam, interface, generic streaming helper, or replacement stream.
- Keep the private seams exactly `primary func(string) string`, `command func(string, ...string) *exec.Cmd`, and `interval time.Duration`.
- Default those seams to `primaryPod`, `exec.Command`, and `liveWriteInterval` in `startLiveWriter`.
- Keep `sync.Once` cleanup and `defer GinkgoRecover()` in the writer goroutine.
- Resolve `pod := w.primary(sourceCluster)` exactly once before command setup.
  Use that captured `pod` for the persistent stream and final query, including after write, close, or Wait failures.
- The persistent child command uses exactly `w.command("kubectl", "exec", "-i", "-n", nsE2E, pod, "-c", "postgres", "--", "psql", "-U", "postgres", appDB, "-q", "-v", "ON_ERROR_STOP=1")`.
- Every ticker event writes one ordered newline-terminated `INSERT`. psql autocommit makes each successful statement one committed transaction.
- A `StdinPipe` failure records the error and returns without starting a child or running the final query.
- A Start failure records the Start error first, closes the created stdin pipe, records any close error without replacing the first error, and returns without running the final query.
- After Start succeeds, every return path closes stdin, waits for the child, then runs the final marker query on the captured pod.
  Write, close, and Wait failures do not skip that query.
- `liveMarkerQuery(marker string) string` must produce exactly `SELECT COALESCE(MAX((substring(note FROM char_length('marker') + 2))::int), 0) FROM orders WHERE note LIKE 'marker-%'` after marker substitution.
- `parseLiveMarker(out []byte) (int, error)` must trim psql output and reject empty or non-integer output.
- Preserve the first error from pipe setup, Start, write, close, Wait, final command, or marker parse.
  Later lifecycle work still runs where required.
- Do not add an interface or dependency solely to force stdin `Close` to fail.
  Keep production close-error recording and cover successful EOF plus Wait ordering through the helper process.
- Retain `last > 100`, source-target marker count equality, the target gap assertion, full data verification, source slot cleanup, and target origin cleanup.
- `go test ./test/e2e -run '^TestLiveWriter' -count=1` runs only standard tests whose names start with `TestLiveWriter`.
  It does not run `TestE2E` or `BeforeSuite`.
- Run `task lint` and `task test` before delivery.
  Run `task e2e` only through its human confirmation prompt.
  Never use `task --yes` for e2e.
- This repository is public.
  Do not commit or paste private endpoints, cluster names, node names, credentials, or secret values.
- Use no em dashes, en dashes, Claude session URLs, or placeholder steps.

---

## File map and interfaces

- `test/e2e/live_writer_test.go`: create the standard-library helper process, deterministic file signals, command recorder, marker tests, and lifecycle tests.
- `test/e2e/e2e_suite_test.go:1434-1494`: replace the per-row writer with one persistent child, marker helpers, private seams, and first-error lifecycle.
- `test/e2e/e2e_test.go:484-547`: keep the real-cluster scenario and replace the stale retry comment with the source-marker invariant.
- `tasks/todo.md`: record observed review, verification, e2e, commit, PR, and CI facts after they happen.

## Test strategy and TDD order

Add the complete local test file first.
The first named test run must fail to compile because the current writer lacks the private seams and marker helpers.
Add the production implementation next, rerun the same named command until it passes, then update the real-cluster scenario.

The helper subprocess is the test binary itself.
Parent tests inject a command that records the requested production command but launches `os.Args[0] -test.run=^TestLiveWriterHelper$` with a mode and temp-file paths.
Stream modes expose readiness, first-write, and EOF boundaries through exact file contents, so no fixed sleep decides lifecycle ordering.

The tests expect two command calls after any successfully started stream: the persistent command and the required final query.
They expect one primary lookup in every case.
A third configured stream mode in the write-failure test remains unused, which proves the writer does not replace its failed stream.

### Task 1: Add complete local live-writer tests

**Files:**

- Create: `test/e2e/live_writer_test.go`

**Interfaces:**

- Consumes after Task 2: `liveWriter{stopCh, done, primary, command, interval}`, `(*liveWriter).run(marker string)`, `(*liveWriter).stop() (int, error)`, `liveMarkerQuery(marker string) string`, and `parseLiveMarker(out []byte) (int, error)`.
- Produces: `TestLiveWriterHelper`, `TestLiveWriterMarkerHelpers`, `TestLiveWriterLifecycle`, and package-local subprocess helpers.

- [ ] **Step 1: Create the complete deterministic subprocess test file**

Create `test/e2e/live_writer_test.go` with this content.

```go
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

package e2e

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	liveWriterHelperEnv     = "LIVE_WRITER_HELPER"
	liveWriterHelperModeEnv = "LIVE_WRITER_HELPER_MODE"
	liveWriterCaptureEnv    = "LIVE_WRITER_HELPER_CAPTURE"
	liveWriterFirstEnv      = "LIVE_WRITER_HELPER_FIRST"
	liveWriterEOFEnv        = "LIVE_WRITER_HELPER_EOF"
	liveWriterReadyEnv      = "LIVE_WRITER_HELPER_READY"

	liveWriterFirstSignal = "first\n"
	liveWriterEOFSignal   = "eof\n"
	liveWriterReadySignal = "ready\n"

	liveWriterHelperTimeout = 2 * time.Second
	liveWriterHelperPoll    = 10 * time.Millisecond
)

type helperCall struct {
	name string
	args []string
}

type helperCommandState struct {
	t     *testing.T
	mu    sync.Mutex
	modes []string
	env   []string
	calls []helperCall
}

func helperCommand(
	t *testing.T, modes []string, env []string,
) (func(string, ...string) *exec.Cmd, *helperCommandState) {
	t.Helper()
	state := &helperCommandState{
		t:     t,
		modes: append([]string(nil), modes...),
		env:   append([]string(nil), env...),
	}
	return state.command, state
}

func (s *helperCommandState) command(name string, args ...string) *exec.Cmd {
	s.record(name, args)

	s.mu.Lock()
	if len(s.modes) == 0 {
		s.mu.Unlock()
		s.t.Errorf("unexpected command: %s %v", name, args)
		return exec.Command(os.Args[0], "-test.run=^$")
	}
	mode := s.modes[0]
	s.modes = s.modes[1:]
	env := append([]string(nil), s.env...)
	s.mu.Unlock()

	cmd := exec.Command(os.Args[0], "-test.run=^TestLiveWriterHelper$")
	cmd.Env = append(os.Environ(), env...)
	cmd.Env = append(cmd.Env,
		liveWriterHelperEnv+"=1",
		liveWriterHelperModeEnv+"="+mode,
	)
	return cmd
}

func (s *helperCommandState) record(name string, args []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, helperCall{
		name: name,
		args: append([]string(nil), args...),
	})
}

func (s *helperCommandState) callsSnapshot() []helperCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := make([]helperCall, len(s.calls))
	for i, call := range s.calls {
		calls[i] = helperCall{
			name: call.name,
			args: append([]string(nil), call.args...),
		}
	}
	return calls
}

type helperPaths struct {
	capture string
	first   string
	eof     string
	ready   string
}

func newHelperPaths(t *testing.T) helperPaths {
	t.Helper()
	dir := t.TempDir()
	return helperPaths{
		capture: filepath.Join(dir, "capture"),
		first:   filepath.Join(dir, "first"),
		eof:     filepath.Join(dir, "eof"),
		ready:   filepath.Join(dir, "ready"),
	}
}

func (p helperPaths) env() []string {
	return []string{
		liveWriterCaptureEnv + "=" + p.capture,
		liveWriterFirstEnv + "=" + p.first,
		liveWriterEOFEnv + "=" + p.eof,
		liveWriterReadyEnv + "=" + p.ready,
	}
}

func liveWriterForTest(
	t *testing.T,
	marker string,
	primary func(string) string,
	command func(string, ...string) *exec.Cmd,
) *liveWriter {
	t.Helper()
	w := &liveWriter{
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		primary:  primary,
		command:  command,
		interval: time.Millisecond,
	}
	go w.run(marker)
	t.Cleanup(func() { _, _ = w.stop() })
	return w
}

func countingPrimary() (func(string) string, func() int) {
	var mu sync.Mutex
	calls := 0
	primary := func(string) string {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return "source-1"
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return calls
	}
	return primary, count
}

func waitForFile(t *testing.T, path, want string) {
	t.Helper()
	timer := time.NewTimer(liveWriterHelperTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(liveWriterHelperPoll)
	defer ticker.Stop()
	last := ""
	for {
		got, err := os.ReadFile(path)
		if err == nil {
			last = string(got)
			if last == want {
				return
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read helper signal %s: %v", path, err)
		}

		select {
		case <-timer.C:
			t.Fatalf("timed out waiting for %s to contain %q, last content %q", path, want, last)
		case <-ticker.C:
		}
	}
}

func waitForDone(t *testing.T, w *liveWriter) {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(liveWriterHelperTimeout):
		t.Fatal("timed out waiting for live writer to finish")
	}
}

func requireError(t *testing.T, err error, contains string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want substring %q", contains)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Fatalf("error = %q, want substring %q", err, contains)
	}
}

func requireCall(t *testing.T, got helperCall, want helperCall) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("call = %#v, want %#v", got, want)
	}
}

func persistentArgs(pod string) []string {
	return []string{
		"exec", "-i", "-n", nsE2E, pod, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-q", "-v", "ON_ERROR_STOP=1",
	}
}

func finalQueryArgs(pod, marker string) []string {
	return []string{
		"exec", "-n", nsE2E, pod, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-tAc", liveMarkerQuery(marker),
	}
}

func requireLifecycleCalls(
	t *testing.T, state *helperCommandState, marker string, wantCount int,
) {
	t.Helper()
	calls := state.callsSnapshot()
	if len(calls) != wantCount {
		t.Fatalf("command calls = %#v, want %d", calls, wantCount)
	}
	requireCall(t, calls[0], helperCall{name: "kubectl", args: persistentArgs("source-1")})
	if wantCount == 2 {
		requireCall(t, calls[1], helperCall{name: "kubectl", args: finalQueryArgs("source-1", marker)})
	}
}

func TestLiveWriterHelper(t *testing.T) {
	if os.Getenv(liveWriterHelperEnv) != "1" {
		return
	}

	switch os.Getenv(liveWriterHelperModeEnv) {
	case "stream":
		helperStream(t, 0)
	case "stream-wait-error":
		helperStream(t, 17)
	case "stream-exit":
		writeHelperSignal(t, liveWriterReadyEnv, liveWriterReadySignal)
		os.Exit(18)
	case "query-3":
		if _, err := fmt.Fprintln(os.Stdout, "3"); err != nil {
			t.Fatalf("write query result: %v", err)
		}
	case "query-error":
		os.Exit(19)
	case "query-bad":
		if _, err := fmt.Fprintln(os.Stdout, "not-an-int"); err != nil {
			t.Fatalf("write bad query result: %v", err)
		}
	case "query-empty":
		return
	default:
		t.Fatalf("unknown helper mode %q", os.Getenv(liveWriterHelperModeEnv))
	}
}

func helperStream(t *testing.T, exitCode int) {
	t.Helper()
	capturePath := os.Getenv(liveWriterCaptureEnv)
	if capturePath == "" {
		t.Fatalf("%s is empty", liveWriterCaptureEnv)
	}
	capture, err := os.OpenFile(capturePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open capture: %v", err)
	}
	defer func() {
		if err := capture.Close(); err != nil {
			t.Errorf("close capture: %v", err)
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	first := true
	for {
		line, readErr := reader.ReadString('\n')
		if line != "" {
			if _, err := io.WriteString(capture, line); err != nil {
				t.Fatalf("write capture: %v", err)
			}
			if first {
				writeHelperSignal(t, liveWriterFirstEnv, liveWriterFirstSignal)
				first = false
			}
		}
		if errors.Is(readErr, io.EOF) {
			writeHelperSignal(t, liveWriterEOFEnv, liveWriterEOFSignal)
			if exitCode != 0 {
				os.Exit(exitCode)
			}
			return
		}
		if readErr != nil {
			t.Fatalf("read stdin: %v", readErr)
		}
	}
}

func writeHelperSignal(t *testing.T, envName, content string) {
	t.Helper()
	path := os.Getenv(envName)
	if path == "" {
		t.Fatalf("%s is empty", envName)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", envName, err)
	}
}

func TestLiveWriterMarkerHelpers(t *testing.T) {
	const wantQuery = "SELECT COALESCE(MAX((substring(note FROM char_length('live-load') + 2))::int), 0) FROM orders WHERE note LIKE 'live-load-%'"
	if got := liveMarkerQuery("live-load"); got != wantQuery {
		t.Fatalf("query = %q, want %q", got, wantQuery)
	}

	tests := []struct {
		name    string
		out     []byte
		want    int
		wantErr bool
	}{
		{name: "whitespace integer", out: []byte(" 3\n"), want: 3},
		{name: "empty", out: []byte{}, wantErr: true},
		{name: "invalid", out: []byte("not-an-int\n"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLiveMarker(tt.out)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseLiveMarker(%q) error = nil", tt.out)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLiveMarker(%q): %v", tt.out, err)
			}
			if got != tt.want {
				t.Fatalf("parseLiveMarker(%q) = %d, want %d", tt.out, got, tt.want)
			}
		})
	}
}

func TestLiveWriterLifecycle(t *testing.T) {
	const marker = "unit-live"

	t.Run("success closes stdin before Wait and returns the final marker", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{"stream", "query-3"}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, marker, primary, command)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		last, err := w.stop()
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
		if last != 3 {
			t.Fatalf("last = %d, want 3", last)
		}
		waitForFile(t, paths.eof, liveWriterEOFSignal)

		captured, err := os.ReadFile(paths.capture)
		if err != nil {
			t.Fatalf("read capture: %v", err)
		}
		text := string(captured)
		if text == "" || !strings.HasSuffix(text, "\n") {
			t.Fatalf("capture = %q, want one or more newline-terminated statements", text)
		}
		lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
		for i, line := range lines {
			want := fmt.Sprintf(
				"INSERT INTO orders (customer_id, amount, note) VALUES (1, 1.00, '%s-%d');",
				marker, i+1,
			)
			if line != want {
				t.Fatalf("line %d = %q, want %q", i+1, line, want)
			}
		}

		requireLifecycleCalls(t, state, marker, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("StdinPipe failure does not start or query", func(t *testing.T) {
		paths := newHelperPaths(t)
		baseCommand, state := helperCommand(t, []string{"stream"}, paths.env())
		command := func(name string, args ...string) *exec.Cmd {
			cmd := baseCommand(name, args...)
			cmd.Stdin = strings.NewReader("")
			return cmd
		}
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, marker, primary, command)

		waitForDone(t, w)
		_, err := w.stop()
		requireError(t, err, "open persistent psql stdin")
		requireLifecycleCalls(t, state, marker, 1)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("Start failure closes the pipe and does not query", func(t *testing.T) {
		state := &helperCommandState{t: t}
		missing := filepath.Join(t.TempDir(), "missing")
		command := func(name string, args ...string) *exec.Cmd {
			state.record(name, args)
			return exec.Command(missing)
		}
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, marker, primary, command)

		waitForDone(t, w)
		_, err := w.stop()
		requireError(t, err, "start persistent psql")
		requireLifecycleCalls(t, state, marker, 1)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("write failure still queries and never replaces the stream", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(
			t, []string{"stream-exit", "query-3", "stream"}, paths.env(),
		)
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, marker, primary, command)

		waitForFile(t, paths.ready, liveWriterReadySignal)
		waitForDone(t, w)
		last, err := w.stop()
		requireError(t, err, "write live marker")
		if last != 3 {
			t.Fatalf("last = %d, want final query result 3", last)
		}
		requireLifecycleCalls(t, state, marker, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("Wait failure follows EOF and still queries", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{"stream-wait-error", "query-3"}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, marker, primary, command)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		last, err := w.stop()
		requireError(t, err, "wait for persistent psql")
		if last != 3 {
			t.Fatalf("last = %d, want final query result 3", last)
		}
		waitForFile(t, paths.eof, liveWriterEOFSignal)
		requireLifecycleCalls(t, state, marker, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("final query failure is returned", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{"stream", "query-error"}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, marker, primary, command)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		_, err := w.stop()
		requireError(t, err, "read final marker")
		requireLifecycleCalls(t, state, marker, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	for _, tc := range []struct {
		name string
		mode string
	}{
		{name: "invalid final marker", mode: "query-bad"},
		{name: "empty final marker", mode: "query-empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths := newHelperPaths(t)
			command, state := helperCommand(t, []string{"stream", tc.mode}, paths.env())
			primary, primaryCalls := countingPrimary()
			w := liveWriterForTest(t, marker, primary, command)

			waitForFile(t, paths.first, liveWriterFirstSignal)
			_, err := w.stop()
			requireError(t, err, "parse final marker")
			requireLifecycleCalls(t, state, marker, 2)
			if got := primaryCalls(); got != 1 {
				t.Fatalf("primary calls = %d, want 1", got)
			}
		})
	}

	t.Run("first error survives later final query failure", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{"stream-exit", "query-error"}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, marker, primary, command)

		waitForFile(t, paths.ready, liveWriterReadySignal)
		waitForDone(t, w)
		_, err := w.stop()
		requireError(t, err, "write live marker")
		if strings.Contains(err.Error(), "read final marker") {
			t.Fatalf("stop returned later final-query error: %v", err)
		}
		requireLifecycleCalls(t, state, marker, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})
}
```

This file covers normal stdin Close through the EOF signal and proves Wait happens after EOF because the `stream-wait-error` helper cannot exit until stdin closes.
It deliberately does not add a fake `io.WriteCloser` or production interface for a synthetic Close error.

- [ ] **Step 2: Format the test and prove the pre-implementation run is red**

Run:

```bash
gofmt -w test/e2e/live_writer_test.go
go test ./test/e2e -run '^TestLiveWriter' -count=1
```

Expected: FAIL at compile time because `liveWriter` has no `primary`, `command`, or `interval` fields, and `liveMarkerQuery` plus `parseLiveMarker` do not exist.

Do not commit the red test alone.
The repository requires every commit to remain lint-clean.

### Task 2: Implement the persistent writer and turn the local tests green

**Files:**

- Modify: `test/e2e/e2e_suite_test.go:1434-1494`
- Test: `test/e2e/live_writer_test.go`

**Interfaces:**

- Consumes: `primaryPod(cluster string) string`, `exec.Command`, `liveWriteInterval`, `sourceCluster`, `nsE2E`, `appDB`, and `GinkgoRecover`.
- Produces: unchanged public test API `startLiveWriter(marker string) *liveWriter` and `(*liveWriter).stop() (int, error)`, plus private `recordErr(error)`, `liveMarkerQuery(string) string`, and `parseLiveMarker([]byte) (int, error)`.

- [ ] **Step 1: Add the required standard-library import**

Add `io` to the existing standard-library import block in `test/e2e/e2e_suite_test.go`.
The implementation uses `io.WriteString` and `io.ErrShortWrite`.
All other required imports (`fmt`, `os/exec`, `strconv`, `strings`, `sync`, and `time`) already exist.

```go
import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)
```

- [ ] **Step 2: Replace the existing live-writer region with the exact lifecycle**

Replace `test/e2e/e2e_suite_test.go:1434-1494` with this code.

```go
// liveWriter sends ordered inserts through one psql child until stopped or a
// write fails. The final marker query uses the primary captured at startup.
type liveWriter struct {
	stopCh chan struct{}
	done   chan struct{}
	// stopOnce lets the spec's cleanup call stop after the spec already did.
	stopOnce sync.Once

	primary  func(string) string
	command  func(string, ...string) *exec.Cmd
	interval time.Duration

	mu   sync.Mutex
	last int
	err  error
}

// startLiveWriter starts marker-tagged writes to sourceCluster and returns
// immediately. Call stop to close and reap the persistent child.
func startLiveWriter(marker string) *liveWriter {
	w := &liveWriter{
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		primary:  primaryPod,
		command:  exec.Command,
		interval: liveWriteInterval,
	}
	go w.run(marker)
	return w
}

func (w *liveWriter) recordErr(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
	}
}

func liveMarkerQuery(marker string) string {
	return fmt.Sprintf(
		"SELECT COALESCE(MAX((substring(note FROM char_length('%s') + 2))::int), 0)"+
			" FROM orders WHERE note LIKE '%s-%%'", marker, marker)
}

func parseLiveMarker(out []byte) (int, error) {
	value := strings.TrimSpace(string(out))
	if value == "" {
		return 0, errors.New("final marker query returned no value")
	}
	last, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %q as integer: %w", value, err)
	}
	return last, nil
}

// run resolves one primary, starts one stream, and never replaces either.
// Every started child is closed, waited, and followed by one marker query.
func (w *liveWriter) run(marker string) {
	// GinkgoRecover must run before close(w.done): defers run in reverse
	// registration order, so a panic recovers before done closes, not after.
	defer close(w.done)
	defer GinkgoRecover()

	pod := w.primary(sourceCluster)
	cmd := w.command("kubectl", "exec", "-i", "-n", nsE2E, pod, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-q", "-v", "ON_ERROR_STOP=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		w.recordErr(fmt.Errorf("open persistent psql stdin: %w", err))
		return
	}
	if err := cmd.Start(); err != nil {
		w.recordErr(fmt.Errorf("start persistent psql: %w", err))
		if closeErr := stdin.Close(); closeErr != nil {
			w.recordErr(fmt.Errorf("close persistent psql stdin after start failure: %w", closeErr))
		}
		return
	}

	defer func() {
		if err := stdin.Close(); err != nil {
			w.recordErr(fmt.Errorf("close persistent psql stdin: %w", err))
		}
		if err := cmd.Wait(); err != nil {
			w.recordErr(fmt.Errorf("wait for persistent psql: %w", err))
		}

		query := liveMarkerQuery(marker)
		out, err := w.command("kubectl", "exec", "-n", nsE2E, pod, "-c", "postgres", "--",
			"psql", "-U", "postgres", appDB, "-tAc", query).Output()
		if err != nil {
			w.recordErr(fmt.Errorf("read final marker: %w", err))
			return
		}
		last, err := parseLiveMarker(out)
		if err != nil {
			w.recordErr(fmt.Errorf("parse final marker: %w", err))
			return
		}
		w.mu.Lock()
		w.last = last
		w.mu.Unlock()
	}()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for n := 1; ; n++ {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
		}

		statement := fmt.Sprintf(
			"INSERT INTO orders (customer_id, amount, note) VALUES (1, 1.00, '%s-%d');\n",
			marker, n,
		)
		written, err := io.WriteString(stdin, statement)
		if err != nil || written != len(statement) {
			if err == nil {
				err = io.ErrShortWrite
			}
			w.recordErr(fmt.Errorf("write live marker %d: %w", n, err))
			return
		}
	}
}

// stop ends the stream and returns the captured source pod's final marker.
// Repeated calls share the first call's completed lifecycle and first error.
func (w *liveWriter) stop() (int, error) {
	w.stopOnce.Do(func() { close(w.stopCh) })
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last, w.err
}
```

The deferred lifecycle is registered only after `cmd.Start` succeeds.
Once registered, it does not return early after close or Wait errors, so the final query always runs on `pod`.
`recordErr` preserves the earliest failure while `last` still records a successful final query result.

- [ ] **Step 3: Format and turn the named local suite green**

Run:

```bash
gofmt -w test/e2e/e2e_suite_test.go test/e2e/live_writer_test.go
go test ./test/e2e -run '^TestLiveWriter' -count=1
go vet ./test/e2e/...
```

Expected: all `TestLiveWriter...` tests pass.
The command does not execute `TestE2E`, `BeforeSuite`, or contact Kubernetes.
`go vet` exits 0.

- [ ] **Step 4: Commit the tested writer and local tests together**

Run:

```bash
git add test/e2e/e2e_suite_test.go test/e2e/live_writer_test.go
git commit -m "test(e2e): keep live writes in one psql session"
```

Expected: one conventional commit contains the production test helper and its local lifecycle coverage.
No red test-only commit exists.

### Task 3: Keep the real-cluster acceptance scenario authoritative

**Files:**

- Modify: `test/e2e/e2e_test.go:484-547`

**Interfaces:**

- Consumes: `startLiveWriter(marker string) *liveWriter`, `(*liveWriter).stop() (int, error)`, and `liveMarkerQuery(marker string) string`.
- Produces: real-cluster acceptance coverage for the source maximum, target count and gap, full comparison, and replication cleanup.

- [ ] **Step 1: Cross-check the returned marker before cutover approval**

After the existing `last > 100` assertion and before `approveCutover(name)`, add:

```go
sourceLast := psql(sourceCluster, liveMarkerQuery(marker))
Expect(fmt.Sprint(last)).To(Equal(sourceLast),
	"writer returned marker %d but the source reports %s for %q", last, sourceLast, marker)
```

This is an independent scenario assertion after the writer has stopped.
It does not alter the writer's one-primary lifecycle.

- [ ] **Step 2: Replace the stale retry comment and keep every existing assertion**

Replace the comment above `srcRows` with:

```go
// last is the source maximum after the psql child exits. Keep the count
// comparison to prove every row for this marker reached the target.
```

Keep the scenario from `test/e2e/e2e_test.go:484-547` otherwise intact.
It must still include:

- `last > 100` before cutover approval.
- The source-target marker `count(*)` equality.
- The target `generate_series(1, last)` gap query and `firstGap == "0"` assertion.
- The `ConditionVerified` assertion, which preserves the full data comparison.
- Source slot and target origin cleanup waits.
- Source and target marker-row cleanup.

- [ ] **Step 3: Format, rerun local checks, and commit the scenario update**

Run:

```bash
gofmt -w test/e2e/e2e_test.go
go test ./test/e2e -run '^TestLiveWriter' -count=1
go vet ./test/e2e/...
git add test/e2e/e2e_test.go
git commit -m "test(e2e): check the live writer source marker"
```

Expected: the named local tests and vet pass without starting the real suite.
The real scenario runs later only through the guarded task.

### Task 4: Review, verify, and deliver in fact order

**Files:**

- Modify: `tasks/todo.md` only after each result exists.

**Interfaces:**

- Consumes: the Task 1 through Task 3 commits and their command output.
- Produces: an independently reviewed implementation, local and optional guarded e2e evidence, a pull request, tracker facts, and two required-check observations tied to explicit SHAs.

- [ ] **Step 1: Run local gates and inspect the complete implementation diff**

Run:

```bash
go test ./test/e2e -run '^TestLiveWriter' -count=1
task lint
task test
git diff --check main...HEAD
git diff main...HEAD -- test/e2e/e2e_suite_test.go test/e2e/live_writer_test.go test/e2e/e2e_test.go
git status --short
```

Expected: all three test or lint commands exit 0, `git diff --check` is silent, and the diff has one persistent child, one primary lookup, the same captured pod in both commands, no stream replacement, private test seams only, the exact marker query, and the retained scenario assertions.

- [ ] **Step 2: Obtain independent implementation review**

Give the reviewer the approved spec, this plan, and the complete implementation diff.
Require a fresh review rather than relying on the plan studies.

The reviewer must check:

- `pod := w.primary(sourceCluster)` occurs exactly once in `run`.
- The persistent and final commands both use that `pod`.
- `StdinPipe` and Start failures do not run the final query.
- Start failure closes stdin after recording the Start error.
- Every successfully started child closes stdin, waits, and queries in that order, even after write, close, or Wait errors.
- `recordErr` preserves the first error and final query success can still set `last`.
- The lifecycle tests assert two command calls, `source-1` in both calls, one primary call, and no replacement stream.
- The real scenario retains the floor, count, gap, full verification, and cleanup checks.

If review finds a defect, correct it, rerun Step 1, and obtain another independent review before continuing.

- [ ] **Step 3: Make the human-gated e2e decision**

Inspect the current context without writing its value to a tracked file:

```bash
kubectl config current-context
```

If the human approves the prompt, run:

```bash
task e2e
```

Expected: Task asks for confirmation.
The live-load spec passes with a source maximum over 100, equal marker counts, gap `0`, full data verification, and cleanup.

If approval is not given, do not run `go test ./test/e2e/...` as a substitute and do not use `task --yes`.
Record that the guarded e2e run was not approved and remains pending.

- [ ] **Step 4: Push the implementation, create or locate its PR, and watch required checks**

Run:

```bash
git push -u origin fix/172-live-writer-throughput

if ! gh pr view fix/172-live-writer-throughput \
	--repo ydixken/pgcopydb-operator --json number --jq .number >/dev/null 2>&1; then
	gh pr create --repo ydixken/pgcopydb-operator \
		--base main \
		--head fix/172-live-writer-throughput \
		--title "test(e2e): keep live writes in one psql session" \
		--body-file - <<'EOF'
## Summary

- replace per-row kubectl exec with one persistent e2e psql session
- reuse the startup pod for the post-Wait marker query
- add deterministic subprocess lifecycle tests and retain real-cluster checks

## Verification

- `go test ./test/e2e -run '^TestLiveWriter' -count=1`
- `task lint`
- `task test`
- `task e2e` only after human approval
EOF
fi

implementation_sha="$(git rev-parse HEAD)"
pr_number="$(gh pr view fix/172-live-writer-throughput \
	--repo ydixken/pgcopydb-operator --json number --jq .number)"
pr_head_sha="$(gh pr view "$pr_number" \
	--repo ydixken/pgcopydb-operator --json headRefOid --jq .headRefOid)"
test "$implementation_sha" = "$pr_head_sha"
gh pr checks "$pr_number" \
	--repo ydixken/pgcopydb-operator --required --watch --fail-fast
```

Expected: `implementation_sha` is the pushed implementation tip.
The first required-check watch reaches success for the PR before the tracker records that result.
If a required check fails, fix it and repeat local verification, review where the fix affects behavior, push, and rerun this watch on the new implementation SHA.

- [ ] **Step 5: Record observed implementation facts and push one tracker commit**

Only after Step 4 succeeds, update `tasks/todo.md` with the exact named-test, lint, task-test, independent-review, guarded-e2e, implementation SHA, PR URL, and first required-check results.
Record only observed facts.

Run:

```bash
git add tasks/todo.md
git commit -m "docs: record live writer verification"
tracker_sha="$(git rev-parse HEAD)"
git push
remote_sha="$(git ls-remote origin refs/heads/fix/172-live-writer-throughput | cut -f1)"
test "$tracker_sha" = "$remote_sha"
```

Expected: the remote branch points at `tracker_sha`.
The tracker accurately describes the implementation checks, which ran against `implementation_sha` before this tracker-only commit.

- [ ] **Step 6: Watch required checks for the tracker SHA and stop editing the tracker**

Run the second required-check watch after the tracker commit is on the remote branch:

```bash
tracker_sha="$(git rev-parse HEAD)"
pr_number="$(gh pr view fix/172-live-writer-throughput \
	--repo ydixken/pgcopydb-operator --json number --jq .number)"
remote_sha="$(git ls-remote origin refs/heads/fix/172-live-writer-throughput | cut -f1)"
test "$tracker_sha" = "$remote_sha"
pr_head_sha="$(gh pr view "$pr_number" \
	--repo ydixken/pgcopydb-operator --json headRefOid --jq .headRefOid)"
test "$tracker_sha" = "$pr_head_sha"
gh pr checks "$pr_number" \
	--repo ydixken/pgcopydb-operator --required --watch --fail-fast
git status --short
```

Expected: the required checks pass for `tracker_sha`, and `git status --short` is empty.
Report the second watch result and `tracker_sha` in the handoff.
Do not edit `tasks/todo.md` again to record that watch, because another tracker commit would create a new SHA and another required-check run.

## Final verification checklist

- [ ] `go test ./test/e2e -run '^TestLiveWriter' -count=1` passed without invoking `TestE2E` or `BeforeSuite`.
- [ ] `task lint`, `task test`, `go vet ./test/e2e/...`, and `git diff --check main...HEAD` exited 0.
- [ ] Start failure records Start first, closes stdin, and does not final-query.
- [ ] Every successfully started child closes stdin, waits, and final-queries, including after write, close, or Wait errors.
- [ ] `run` resolves the primary once, and both commands use the same captured pod.
- [ ] The tests expect `source-1` in both commands, `primaryCalls() == 1`, exactly two post-Start command calls, and no replacement stream.
- [ ] Empty and invalid marker output, final command failure, Wait failure, write failure, and first-error preservation have deterministic tests.
- [ ] The scenario at `test/e2e/e2e_test.go:484-547` still enforces the floor, source-target count, target gap, full comparison, cleanup, and corrected source-marker comment.
- [ ] The guarded e2e result is recorded as passed or not approved, without bypassing the prompt or using `task --yes`.
- [ ] The first required-check watch passed before the tracker commit, and the second passed for the verified remote `tracker_sha`.
- [ ] The final handoff reports the second watch without creating another tracker update.
