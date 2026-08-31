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

	postgresContainer        = "postgres"
	liveWriterModeStream     = "stream"
	liveWriterModeStreamExit = "stream-exit"
	liveWriterModeQueryThree = "query-3"
	liveWriterModeQueryError = "query-error"
	liveWriterTestMarker     = "unit-live"
)

type helperCall struct {
	name string
	args []string
}

type helperCommandState struct {
	t                   *testing.T
	mu                  sync.Mutex
	modes               []string
	env                 []string
	calls               []helperCall
	commands            []*exec.Cmd
	firstWaitedAtSecond bool
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
	s.mu.Lock()
	s.recordRequestLocked(name, args)
	if len(s.modes) == 0 {
		s.mu.Unlock()
		s.t.Errorf("unexpected command: %s %v", name, args)
		return s.retainCommand(exec.Command(os.Args[0], "-test.run=^$"))
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
	return s.retainCommand(cmd)
}

func (s *helperCommandState) recordRequestLocked(name string, args []string) {
	if len(s.calls) == 1 {
		s.firstWaitedAtSecond = len(s.commands) == 1 && s.commands[0].ProcessState != nil
	}
	s.calls = append(s.calls, helperCall{
		name: name,
		args: append([]string(nil), args...),
	})
}

func (s *helperCommandState) retainCommand(cmd *exec.Cmd) *exec.Cmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, cmd)
	return cmd
}

func (s *helperCommandState) recordCommand(
	name string, args []string, cmd *exec.Cmd,
) *exec.Cmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordRequestLocked(name, args)
	s.commands = append(s.commands, cmd)
	return cmd
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

func (s *helperCommandState) commandsSnapshot() []*exec.Cmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*exec.Cmd(nil), s.commands...)
}

func (s *helperCommandState) firstWaitedAtSecondSnapshot() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstWaitedAtSecond
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
	go w.run(liveWriterTestMarker)
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
		"exec", "-i", "-n", nsE2E, pod, "-c", postgresContainer, "--",
		"psql", "-U", "postgres", appDB, "-q", "-v", "ON_ERROR_STOP=1",
	}
}

func finalQueryArgs(pod, marker string) []string {
	return []string{
		"exec", "-n", nsE2E, pod, "-c", postgresContainer, "--",
		"psql", "-U", "postgres", appDB, "-tAc", liveMarkerQuery(marker),
	}
}

func requireLifecycleCalls(
	t *testing.T, state *helperCommandState, wantCount int,
) {
	t.Helper()
	calls := state.callsSnapshot()
	if len(calls) != wantCount {
		t.Fatalf("command calls = %#v, want %d", calls, wantCount)
	}
	commands := state.commandsSnapshot()
	if len(commands) != wantCount {
		t.Fatalf("returned commands = %d, want %d", len(commands), wantCount)
	}
	requireCall(t, calls[0], helperCall{name: "kubectl", args: persistentArgs("source-1")})
	if wantCount == 2 {
		requireCall(t, calls[1], helperCall{name: "kubectl", args: finalQueryArgs("source-1", liveWriterTestMarker)})
		if !state.firstWaitedAtSecondSnapshot() {
			t.Fatal("final query was requested before the persistent command was waited")
		}
	}
}

func TestLiveWriterHelper(t *testing.T) {
	if os.Getenv(liveWriterHelperEnv) != "1" {
		return
	}

	switch os.Getenv(liveWriterHelperModeEnv) {
	case liveWriterModeStream:
		helperStream(t, 0)
	case "stream-wait-error":
		helperStream(t, 17)
	case liveWriterModeStreamExit:
		writeHelperSignal(t, liveWriterReadyEnv, liveWriterReadySignal)
		os.Exit(18)
	case liveWriterModeQueryThree:
		if _, err := fmt.Fprintln(os.Stdout, "3"); err != nil {
			t.Fatalf("write query result: %v", err)
		}
		os.Exit(0)
	case liveWriterModeQueryError:
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
	const wantQuery = "SELECT COALESCE(MAX((substring(note FROM char_length('live-load') + 2))::int), 0)" +
		" FROM orders WHERE note LIKE 'live-load-%'"
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
	t.Run("success closes stdin before Wait and returns the final marker", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{liveWriterModeStream, liveWriterModeQueryThree}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command)

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
				liveWriterTestMarker, i+1,
			)
			if line != want {
				t.Fatalf("line %d = %q, want %q", i+1, line, want)
			}
		}

		requireLifecycleCalls(t, state, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("StdinPipe failure does not start or query", func(t *testing.T) {
		paths := newHelperPaths(t)
		baseCommand, state := helperCommand(t, []string{liveWriterModeStream}, paths.env())
		command := func(name string, args ...string) *exec.Cmd {
			cmd := baseCommand(name, args...)
			cmd.Stdin = strings.NewReader("")
			return cmd
		}
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command)

		waitForDone(t, w)
		_, err := w.stop()
		requireError(t, err, "open persistent psql stdin")
		requireLifecycleCalls(t, state, 1)
		commands := state.commandsSnapshot()
		if commands[0].Process != nil {
			t.Fatalf("persistent helper process = %#v, want nil", commands[0].Process)
		}
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("Start failure closes the pipe and does not query", func(t *testing.T) {
		state := &helperCommandState{t: t}
		missing := filepath.Join(t.TempDir(), "missing")
		command := func(name string, args ...string) *exec.Cmd {
			return state.recordCommand(name, args, exec.Command(missing))
		}
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command)

		waitForDone(t, w)
		_, err := w.stop()
		requireError(t, err, "start persistent psql")
		requireLifecycleCalls(t, state, 1)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("write failure still queries and never replaces the stream", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(
			t, []string{liveWriterModeStreamExit, liveWriterModeQueryThree, liveWriterModeStream}, paths.env(),
		)
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command)

		waitForFile(t, paths.ready, liveWriterReadySignal)
		waitForDone(t, w)
		last, err := w.stop()
		requireError(t, err, "write live marker")
		if last != 3 {
			t.Fatalf("last = %d, want final query result 3", last)
		}
		requireLifecycleCalls(t, state, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("Wait failure follows EOF and still queries", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{"stream-wait-error", liveWriterModeQueryThree}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		last, err := w.stop()
		requireError(t, err, "wait for persistent psql")
		if last != 3 {
			t.Fatalf("last = %d, want final query result 3", last)
		}
		waitForFile(t, paths.eof, liveWriterEOFSignal)
		requireLifecycleCalls(t, state, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("final query failure is returned", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{liveWriterModeStream, liveWriterModeQueryError}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		_, err := w.stop()
		requireError(t, err, "read final marker")
		requireLifecycleCalls(t, state, 2)
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
			command, state := helperCommand(t, []string{liveWriterModeStream, tc.mode}, paths.env())
			primary, primaryCalls := countingPrimary()
			w := liveWriterForTest(t, primary, command)

			waitForFile(t, paths.first, liveWriterFirstSignal)
			_, err := w.stop()
			requireError(t, err, "parse final marker")
			requireLifecycleCalls(t, state, 2)
			if got := primaryCalls(); got != 1 {
				t.Fatalf("primary calls = %d, want 1", got)
			}
		})
	}

	t.Run("first error survives later final query failure", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{liveWriterModeStreamExit, liveWriterModeQueryError}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command)

		waitForFile(t, paths.ready, liveWriterReadySignal)
		waitForDone(t, w)
		_, err := w.stop()
		requireError(t, err, "write live marker")
		if strings.Contains(err.Error(), "read final marker") {
			t.Fatalf("stop returned later final-query error: %v", err)
		}
		requireLifecycleCalls(t, state, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})
}
