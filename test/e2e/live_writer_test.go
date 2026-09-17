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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
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
	liveWriterTestTimeout   = 1_000 * time.Millisecond
	liveWriterOuterTimeout  = 10 * time.Second

	postgresContainer                = "postgres"
	liveWriterModeStream             = "stream"
	liveWriterModeStreamExit         = "stream-exit"
	liveWriterModeStreamHang         = "stream-hang"
	liveWriterModeStreamBackpressure = "stream-backpressure"
	liveWriterModeQueryThree         = "query-3"
	liveWriterModeQueryError         = "query-error"
	liveWriterModeQueryHang          = "query-hang"
	liveWriterModeStderrFlood        = "stderr-flood"
	liveWriterTestMarker             = "unit-live"
	liveWriterTestPod                = "source-1"
	liveWriterTestCredential         = "credential"
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
	cancels             []context.CancelFunc
	aborted             bool
	firstWaitedAtSecond bool
}

func helperCommand(
	t *testing.T, modes []string, env []string,
) (commandFactory, *helperCommandState) {
	t.Helper()
	state := &helperCommandState{
		t:     t,
		modes: append([]string(nil), modes...),
		env:   append([]string(nil), env...),
	}
	return state.command, state
}

func (s *helperCommandState) command(ctx context.Context, name string, args ...string) *exec.Cmd {
	s.mu.Lock()
	s.recordRequestLocked(name, args)
	if len(s.modes) == 0 {
		s.mu.Unlock()
		s.t.Errorf("unexpected command: %s %v", name, args)
		commandCtx, cancel := context.WithCancel(ctx)
		return s.retainCommand(exec.CommandContext(commandCtx, os.Args[0], "-test.run=^$"), cancel)
	}
	mode := s.modes[0]
	s.modes = s.modes[1:]
	env := append([]string(nil), s.env...)
	s.mu.Unlock()

	commandCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(commandCtx, os.Args[0], "-test.run=^TestLiveWriterHelper$")
	cmd.Env = append(os.Environ(), env...)
	cmd.Env = append(cmd.Env,
		"GORACE=atexit_sleep_ms=0",
		liveWriterHelperEnv+"=1",
		liveWriterHelperModeEnv+"="+mode,
	)
	return s.retainCommand(cmd, cancel)
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

func (s *helperCommandState) retainCommand(
	cmd *exec.Cmd, cancel context.CancelFunc,
) *exec.Cmd {
	s.mu.Lock()
	s.commands = append(s.commands, cmd)
	s.cancels = append(s.cancels, cancel)
	aborted := s.aborted
	s.mu.Unlock()
	if aborted {
		cancel()
	}
	return cmd
}

func (s *helperCommandState) recordCommand(
	name string, args []string, cmd *exec.Cmd, cancel context.CancelFunc,
) *exec.Cmd {
	s.mu.Lock()
	s.recordRequestLocked(name, args)
	s.commands = append(s.commands, cmd)
	s.cancels = append(s.cancels, cancel)
	aborted := s.aborted
	s.mu.Unlock()
	if aborted {
		cancel()
	}
	return cmd
}

func (s *helperCommandState) cancelAll() {
	s.mu.Lock()
	s.aborted = true
	cancels := append([]context.CancelFunc(nil), s.cancels...)
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
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
	command commandFactory,
	state *helperCommandState,
) *liveWriter {
	t.Helper()
	return liveWriterForMarkerTest(t, liveWriterTestMarker, primary, command, state)
}

func liveWriterForMarkerTest(
	t *testing.T,
	marker string,
	primary func(string) string,
	command commandFactory,
	state *helperCommandState,
) *liveWriter {
	t.Helper()
	w := startLiveWriterWith(
		marker,
		primary,
		command,
		time.Millisecond,
		liveWriterTestTimeout,
	)
	t.Cleanup(func() { forceStopLiveWriter(t, w, state) })
	return w
}

type liveWriterStopResult struct {
	last int
	err  error
}

func forceStopLiveWriter(
	t *testing.T, w *liveWriter, state *helperCommandState,
) {
	t.Helper()
	w.cancel()
	state.cancelAll()

	timer := time.NewTimer(liveWriterOuterTimeout)
	defer timer.Stop()
	select {
	case <-w.done:
	case <-timer.C:
		t.Fatalf("live writer did not reap its commands within %s", liveWriterOuterTimeout)
	}
	for _, cmd := range state.commandsSnapshot() {
		if cmd.Process != nil {
			requireReaped(t, cmd)
		}
	}
}

func stopLiveWriterWithWatchdog(
	t *testing.T, w *liveWriter, state *helperCommandState,
) (int, error) {
	t.Helper()
	resultCh := make(chan liveWriterStopResult, 1)
	go func() {
		last, err := w.stop()
		resultCh <- liveWriterStopResult{last: last, err: err}
	}()

	timer := time.NewTimer(liveWriterOuterTimeout)
	defer timer.Stop()
	select {
	case result := <-resultCh:
		return result.last, result.err
	case <-timer.C:
		forceStopLiveWriter(t, w, state)
		t.Fatalf("liveWriter.stop did not return within %s", liveWriterOuterTimeout)
		return 0, nil
	}
}

func countingPrimary() (func(string) string, func() int) {
	var mu sync.Mutex
	calls := 0
	primary := func(string) string {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return liveWriterTestPod
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

func requireReaped(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd.Process == nil || cmd.ProcessState == nil {
		t.Fatalf("command was not started and reaped: process=%v state=%v", cmd.Process, cmd.ProcessState)
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
	requireCall(t, calls[0], helperCall{name: "kubectl", args: persistentArgs(liveWriterTestPod)})
	if wantCount == 2 {
		requireCall(t, calls[1], helperCall{name: "kubectl", args: finalQueryArgs(liveWriterTestPod, liveWriterTestMarker)})
		if !state.firstWaitedAtSecondSnapshot() {
			t.Fatal("final query was requested before the persistent command was waited")
		}
	}
}

func TestLiveWriterHelper(t *testing.T) {
	if os.Getenv(liveWriterHelperEnv) != "1" {
		t.Skip("subprocess entry point")
	}

	switch os.Getenv(liveWriterHelperModeEnv) {
	case liveWriterModeStream:
		helperStream(t, 0)
	case liveWriterModeStreamHang:
		helperStream(t, 0)
		time.Sleep(time.Hour)
	case liveWriterModeStreamBackpressure:
		var firstByte [1]byte
		if _, err := io.ReadFull(os.Stdin, firstByte[:]); err != nil {
			t.Fatalf("read first stdin byte: %v", err)
		}
		writeHelperSignal(t, liveWriterFirstEnv, liveWriterFirstSignal)
		time.Sleep(time.Hour)
	case "stream-wait-error":
		helperStream(t, 17)
	case liveWriterModeStreamExit:
		if _, err := io.WriteString(os.Stderr, "ERROR: relation orders does not exist\n"+
			"postgresql://app:credential@db.invalid/app\n"); err != nil {
			t.Fatal(err)
		}
		writeHelperSignal(t, liveWriterReadyEnv, liveWriterReadySignal)
		os.Exit(18)
	case liveWriterModeStderrFlood:
		if _, err := io.WriteString(os.Stderr, "ERROR: stream prelude\n"+
			strings.Repeat("INFO: draining stderr\n", liveWriterStderrLimit)); err != nil {
			t.Fatal(err)
		}
		helperStream(t, 0)
	case liveWriterModeQueryThree:
		if _, err := fmt.Fprintln(os.Stdout, "3"); err != nil {
			t.Fatalf("write query result: %v", err)
		}
		os.Exit(0)
	case "query-zero":
		if _, err := fmt.Fprintln(os.Stdout, "0"); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	case liveWriterModeQueryError:
		if _, err := io.WriteString(os.Stderr, "ERROR: final query rejected\npassword=credential\n"); err != nil {
			t.Fatal(err)
		}
		os.Exit(19)
	case liveWriterModeQueryHang:
		if _, err := io.WriteString(os.Stderr, "ERROR: final query stalled\npassword=credential\n"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Hour)
	case "query-bad":
		if _, err := fmt.Fprintln(os.Stdout, "not-an-int"); err != nil {
			t.Fatalf("write bad query result: %v", err)
		}
	case "query-empty":
		os.Exit(0)
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

func TestLiveWriterDiagnosticStderr(t *testing.T) {
	const metadata = `{"error_severity":"ERROR","message":"statement rejected","node":"private-node.invalid"}`
	const redacted = "[LOG] [redacted connection/environment line]\n"
	const retained = "[LOG] ERROR: retained\n"
	for _, tc := range []struct {
		name      string
		chunks    []string
		want      string
		truncated bool
	}{
		{"URI split across writes", []string{"postgre", "sql://app:credential@db.invalid/app\n"}, redacted, false},
		{"password split across writes", []string{"pass", "word=credential\n"}, redacted, false},
		{"TCP lookup split across writes", []string{"dial t", "cp: lookup safe.invalid: no such host\n"}, redacted, false},
		{"UDP6 split across writes", []string{"dial u", "dp6 safe.invalid: connection refused\n"}, redacted, false},
		{"DNS lookup split across writes", []string{"lookup safe.invalid: no such", " host\n"}, redacted, false},
		{"libpq hostname split across writes", []string{"could not translate host ", "name safe.invalid to address\n"},
			redacted, false},
		{"structured metadata", []string{metadata[:40], metadata[40:] + "\n"}, "[ERROR] statement rejected\n", false},
		{"untrusted severity", []string{`{"error_severity":"password=credential","message":"statement rejected"}`},
			"[LOG] statement rejected\n", false},
		{"known primary", []string{"exec failed on " + liveWriterTestPod + "\n"},
			"[LOG] exec failed on [fixture-location]\n", false},
		{"environment", []string{"KUBECONFIG=/private/credential\n"}, redacted, false},
		{"truncated URI", []string{"ERROR: retained\n" + strings.Repeat("x", liveWriterStderrLimit-20) +
			"postgresql://app:credential@db.invalid/app\n"}, retained, true},
		{"truncated password", []string{"ERROR: retained\n" + strings.Repeat("x", liveWriterStderrLimit-20) +
			"password=credential\n"}, retained, true},
		{"truncated DNS suffix", []string{"ERROR: retained\nlookup safe.invalid: " +
			strings.Repeat(" ", liveWriterStderrLimit), "no such host\n"}, retained, true},
		{"no complete line", []string{strings.Repeat("credential", liveWriterStderrLimit)}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stderr liveWriterStderr
			total := 0
			for _, chunk := range tc.chunks {
				n, err := stderr.Write([]byte(chunk))
				if err != nil || n != len(chunk) {
					t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(chunk))
				}
				total += len(chunk)
			}
			if stderr.total != total || len(stderr.prefix) > liveWriterStderrLimit ||
				(stderr.total > len(stderr.prefix)) != tc.truncated {
				t.Fatalf("incorrect stderr bound: total=%d prefix=%d", stderr.total, len(stderr.prefix))
			}
			text := stderr.text(liveWriterTestPod)
			if text != tc.want {
				t.Fatalf("sanitized stderr = %q, want %q", text, tc.want)
			}
		})
	}
}

func TestLiveWriterStderrFlood(t *testing.T) {
	paths := newHelperPaths(t)
	command, state := helperCommand(t, []string{liveWriterModeStderrFlood, "query-zero"}, paths.env())
	primary, _ := countingPrimary()
	w := liveWriterForTest(t, primary, command, state)
	waitForFile(t, paths.first, liveWriterFirstSignal)
	if active := w.diagnostics(); active.stderr != "" || active.queryResult != liveWriterQueryNotRun {
		t.Fatalf("snapshot read stderr before the child was reaped: %s", active)
	}
	last, err := stopLiveWriterWithWatchdog(t, w, state)
	if err != nil || last != 0 {
		t.Fatalf("stop = %d, %v; want a valid zero marker", last, err)
	}
	diag := w.diagnostics()
	if !diag.finalMarkerValid || diag.finalMarker != 0 || diag.queryResult != liveWriterQueryValid ||
		!strings.Contains(diag.String(), "marker=0") || strings.Contains(diag.String(), "marker=unavailable") {
		t.Fatalf("zero marker was not distinguished from unavailable: %s", diag)
	}
	if !diag.stderrTruncated || diag.stderrBytes <= liveWriterStderrLimit ||
		!strings.Contains(diag.stderr, "ERROR: stream prelude") || len(diag.stderr) > 2*liveWriterStderrLimit {
		t.Fatalf("stderr flood was lost or unbounded: %s", diag)
	}
	waitForFile(t, paths.eof, liveWriterEOFSignal)
	requireLifecycleCalls(t, state, 2)
	for _, cmd := range state.commandsSnapshot() {
		requireReaped(t, cmd)
	}
}

func TestLiveWriterActiveBeyondShutdownTimeout(t *testing.T) {
	paths := newHelperPaths(t)
	command, state := helperCommand(t, []string{liveWriterModeStream, liveWriterModeQueryThree}, paths.env())
	primary, _ := countingPrimary()
	w := liveWriterForTest(t, primary, command, state)
	waitForFile(t, paths.first, liveWriterFirstSignal)
	before := w.diagnostics()
	select {
	case <-w.done:
		t.Fatal("writer stopped without an explicit stop")
	case <-time.After(2 * liveWriterTestTimeout):
	}
	after := w.diagnostics()
	if after.submittedMarker <= before.submittedMarker || after.submittedBytes <= before.submittedBytes ||
		!after.stopAt.IsZero() || !after.firstErrorAt.IsZero() || w.ctx.Err() != nil {
		t.Fatalf("writer did not stay active beyond its shutdown timeout: %s", after)
	}
	if _, err := stopLiveWriterWithWatchdog(t, w, state); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, paths.eof, liveWriterEOFSignal)
	requireLifecycleCalls(t, state, 2)
}

func TestLiveWriterStartupBoundary(t *testing.T) {
	paths := newHelperPaths(t)
	baseCommand, state := helperCommand(t, []string{liveWriterModeStream}, paths.env())
	primaryEntered := make(chan struct{})
	releasePrimaryCh := make(chan struct{})
	commandEntered := make(chan struct{})
	releaseCommandCh := make(chan struct{})
	releasePrimary := sync.OnceFunc(func() { close(releasePrimaryCh) })
	releaseCommand := sync.OnceFunc(func() { close(releaseCommandCh) })
	primaryCalls := 0
	primary := func(cluster string) string {
		primaryCalls++
		if cluster != sourceCluster {
			t.Errorf("primary cluster = %q, want %q", cluster, sourceCluster)
		}
		close(primaryEntered)
		<-releasePrimaryCh
		return liveWriterTestPod
	}
	command := func(ctx context.Context, name string, args ...string) *exec.Cmd {
		close(commandEntered)
		<-releaseCommandCh
		return baseCommand(ctx, name, args...)
	}

	writerCh := make(chan *liveWriter, 1)
	go func() {
		writerCh <- startLiveWriterWith(
			liveWriterTestMarker,
			primary,
			command,
			time.Millisecond,
			liveWriterTestTimeout,
		)
	}()

	var w *liveWriter
	t.Cleanup(func() {
		releasePrimary()
		releaseCommand()
		if w == nil {
			select {
			case w = <-writerCh:
			case <-time.After(liveWriterOuterTimeout):
				t.Fatalf("live writer startup did not return within %s", liveWriterOuterTimeout)
			}
		}
		forceStopLiveWriter(t, w, state)
	})

	select {
	case <-primaryEntered:
	case <-time.After(liveWriterOuterTimeout):
		t.Fatalf("primary resolution did not start within %s", liveWriterOuterTimeout)
	}
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case w = <-writerCh:
		timer.Stop()
		t.Fatal("live writer was exposed before primary resolution completed")
	case <-timer.C:
	}

	releasePrimary()
	select {
	case w = <-writerCh:
	case <-time.After(liveWriterOuterTimeout):
		t.Fatalf("live writer startup did not return within %s", liveWriterOuterTimeout)
	}
	select {
	case <-commandEntered:
	case <-time.After(liveWriterOuterTimeout):
		t.Fatalf("persistent command was not constructed within %s", liveWriterOuterTimeout)
	}

	resultCh := make(chan liveWriterStopResult, 1)
	go func() {
		last, err := w.stop()
		resultCh <- liveWriterStopResult{last: last, err: err}
	}()
	select {
	case <-w.stopCh:
	case <-time.After(liveWriterOuterTimeout):
		t.Fatalf("live writer stop did not start within %s", liveWriterOuterTimeout)
	}
	releaseCommand()

	select {
	case result := <-resultCh:
		if result.err != nil || result.last != 0 {
			t.Fatalf("stop result = %#v, want zero marker and no error", result)
		}
	case <-time.After(liveWriterOuterTimeout):
		t.Fatalf("live writer stop did not return within %s", liveWriterOuterTimeout)
	}
	if primaryCalls != 1 {
		t.Fatalf("primary calls = %d, want 1", primaryCalls)
	}
	requireLifecycleCalls(t, state, 1)
	commandState := state.commandsSnapshot()[0]
	if commandState.Process != nil || commandState.ProcessState != nil {
		t.Fatalf(
			"persistent command started after stop: process=%v state=%v",
			commandState.Process,
			commandState.ProcessState,
		)
	}
}

//nolint:gocyclo // The subtests cover one lifecycle contract and share its setup helpers.
func TestLiveWriterLifecycle(t *testing.T) {
	t.Run("success closes stdin before Wait and returns the final marker", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(t, []string{liveWriterModeStream, liveWriterModeQueryThree}, paths.env())
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command, state)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		completedQuery := make(chan liveWriterDiagnostic, 1)
		go func() {
			for {
				diag := w.diagnostics()
				if !diag.queryFinishedAt.IsZero() {
					completedQuery <- diag
					return
				}
				select {
				case <-w.done:
					completedQuery <- w.diagnostics()
					return
				default:
					runtime.Gosched()
				}
			}
		}()
		last, err := stopLiveWriterWithWatchdog(t, w, state)
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
		if last != 3 {
			t.Fatalf("last = %d, want 3", last)
		}
		observed := <-completedQuery
		if observed.queryResult != liveWriterQueryValid || !observed.finalMarkerValid || observed.finalMarker != last {
			t.Fatalf("completed query snapshot published a transient result: %s", observed)
		}
		diag := w.diagnostics()
		if !diag.finalMarkerValid || diag.finalMarker != last || diag.queryResult != liveWriterQueryValid ||
			diag.childExit != "exit status 0" || diag.firstError != nil {
			t.Fatalf("unexpected successful diagnostics: %s", diag)
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
		if diag.submittedBytes != len(captured) || diag.submittedMarker != len(lines) {
			t.Fatalf("submitted counters do not match the consumed stream: %s", diag)
		}
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
		for _, cmd := range state.commandsSnapshot() {
			requireReaped(t, cmd)
		}
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("StdinPipe failure does not start or query", func(t *testing.T) {
		paths := newHelperPaths(t)
		baseCommand, state := helperCommand(t, []string{liveWriterModeStream}, paths.env())
		command := func(ctx context.Context, name string, args ...string) *exec.Cmd {
			cmd := baseCommand(ctx, name, args...)
			cmd.Stdin = strings.NewReader("")
			return cmd
		}
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command, state)

		waitForDone(t, w)
		_, err := stopLiveWriterWithWatchdog(t, w, state)
		requireError(t, err, "open persistent psql stdin")
		if diag := w.diagnostics(); diag.firstErrorStage != "stdin-open" || diag.queryResult != liveWriterQueryNotRun {
			t.Fatalf("unexpected stdin-open diagnostics: %s", diag)
		}
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
		command := func(ctx context.Context, name string, args ...string) *exec.Cmd {
			commandCtx, cancel := context.WithCancel(ctx)
			return state.recordCommand(
				name, args, exec.CommandContext(commandCtx, missing), cancel,
			)
		}
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command, state)

		waitForDone(t, w)
		_, err := stopLiveWriterWithWatchdog(t, w, state)
		requireError(t, err, "start persistent psql")
		if diag := w.diagnostics(); diag.firstErrorStage != "start" || diag.childExit != liveWriterNotStarted ||
			strings.Contains(diag.String(), missing) {
			t.Fatalf("unexpected startup diagnostics: %s", diag)
		}
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
		w := liveWriterForTest(t, primary, command, state)

		waitForFile(t, paths.ready, liveWriterReadySignal)
		waitForDone(t, w)
		last, err := stopLiveWriterWithWatchdog(t, w, state)
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
		w := liveWriterForTest(t, primary, command, state)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		last, err := stopLiveWriterWithWatchdog(t, w, state)
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
		w := liveWriterForTest(t, primary, command, state)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		_, err := stopLiveWriterWithWatchdog(t, w, state)
		requireError(t, err, "read final marker")
		if diag := w.diagnostics(); diag.finalMarkerValid || diag.queryResult != liveWriterQueryFailed ||
			!strings.Contains(diag.String(), "marker=unavailable") {
			t.Fatalf("query failure looks like a known marker: %s", diag)
		}
		requireLifecycleCalls(t, state, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	for _, tc := range []struct {
		name   string
		mode   string
		result string
	}{
		{name: "invalid final marker", mode: "query-bad", result: liveWriterQueryInvalid},
		{name: "empty final marker", mode: "query-empty", result: liveWriterQueryEmpty},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths := newHelperPaths(t)
			command, state := helperCommand(t, []string{liveWriterModeStream, tc.mode}, paths.env())
			primary, primaryCalls := countingPrimary()
			w := liveWriterForTest(t, primary, command, state)

			waitForFile(t, paths.first, liveWriterFirstSignal)
			_, err := stopLiveWriterWithWatchdog(t, w, state)
			requireError(t, err, "parse final marker")
			diag := w.diagnostics()
			if diag.queryResult != tc.result || diag.finalMarkerValid || diag.firstErrorStage != "final-parse" ||
				!strings.Contains(diag.String(), "marker=unavailable") || strings.Contains(diag.String(), "not-an-int") {
				t.Fatalf("invalid marker diagnostics: %s", diag)
			}
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
		w := liveWriterForTest(t, primary, command, state)

		waitForFile(t, paths.ready, liveWriterReadySignal)
		waitForDone(t, w)
		_, err := stopLiveWriterWithWatchdog(t, w, state)
		requireError(t, err, "write live marker")
		if strings.Contains(err.Error(), "read final marker") {
			t.Fatalf("stop returned later final-query error: %v", err)
		}
		diag := w.diagnostics()
		if !errors.Is(err, syscall.EPIPE) || !diag.brokenPipe || diag.firstErrorStage != "stdin-write" ||
			diag.failedMarker != diag.submittedMarker+1 || diag.writeError == nil ||
			diag.firstErrorAt.IsZero() || diag.writeFailedAt.IsZero() ||
			!diag.writeFailedAt.Before(diag.stopAt) || !diag.reapedAt.Before(diag.queryStartedAt) {
			t.Fatalf("lost write failure or failure timing: %s; primary error: %v", diag, err)
		}
		if diag.finalMarkerValid || diag.queryResult != liveWriterQueryFailed || diag.childExit != "exit status 18" ||
			diag.waitError == nil || diag.queryError == nil {
			t.Fatalf("lost secondary failure: %s", diag)
		}
		for _, want := range []string{"ERROR: relation orders does not exist", "ERROR: final query rejected",
			"command exit=18", "command exit=19", "marker=unavailable"} {
			if !strings.Contains(diag.String(), want) {
				t.Fatalf("missing %q from diagnostics: %s", want, diag)
			}
		}
		for _, forbidden := range []string{liveWriterTestCredential, "db.invalid", liveWriterTestPod, "postgresql://"} {
			if strings.Contains(diag.String(), forbidden) {
				t.Fatalf("diagnostics leaked %q", forbidden)
			}
		}
		requireLifecycleCalls(t, state, 2)
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("shutdown timeout cancels and reaps a child stuck after EOF", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(
			t,
			[]string{liveWriterModeStreamHang, liveWriterModeQueryThree},
			paths.env(),
		)
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command, state)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		last, err := stopLiveWriterWithWatchdog(t, w, state)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop error = %v, want context.DeadlineExceeded", err)
		}
		for _, want := range []string{
			sourceCluster, liveWriterTestPod, "stop persistent psql", liveWriterTestTimeout.String(),
		} {
			requireError(t, err, want)
		}
		if last != 3 {
			t.Fatalf("last = %d, want marker 3 after forced cancellation", last)
		}
		diag := w.diagnostics()
		if diag.firstErrorStage != "stop-timeout" || diag.writerContext != context.Canceled.Error() ||
			!strings.Contains(diag.childExit, "signal:") || !diag.finalMarkerValid {
			t.Fatalf("lost forced cancellation diagnostics: %s", diag)
		}
		waitForFile(t, paths.eof, liveWriterEOFSignal)
		requireLifecycleCalls(t, state, 2)
		for _, cmd := range state.commandsSnapshot() {
			requireReaped(t, cmd)
		}
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("shutdown cancels a write blocked by pipe backpressure", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(
			t,
			[]string{liveWriterModeStreamBackpressure, liveWriterModeQueryThree},
			paths.env(),
		)
		primary, _ := countingPrimary()
		marker := strings.Repeat("x", 2<<20)
		w := liveWriterForMarkerTest(t, marker, primary, command, state)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		last, err := stopLiveWriterWithWatchdog(t, w, state)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop error = %v, want context.DeadlineExceeded", err)
		}
		if last != 3 {
			t.Fatalf("last = %d, want marker 3", last)
		}
		commands := state.commandsSnapshot()
		if len(commands) != 2 {
			t.Fatalf("commands = %d, want persistent stream and marker query", len(commands))
		}
		for _, cmd := range commands {
			requireReaped(t, cmd)
		}
	})

	t.Run("final marker query has a fresh deadline and is reaped", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(
			t,
			[]string{liveWriterModeStream, liveWriterModeQueryHang},
			paths.env(),
		)
		primary, primaryCalls := countingPrimary()
		w := liveWriterForTest(t, primary, command, state)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		_, err := stopLiveWriterWithWatchdog(t, w, state)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop error = %v, want context.DeadlineExceeded", err)
		}
		for _, want := range []string{
			sourceCluster,
			liveWriterTestPod,
			"read final marker",
			liveMarkerQuery(liveWriterTestMarker),
			liveWriterTestTimeout.String(),
		} {
			requireError(t, err, want)
		}
		diag := w.diagnostics()
		if diag.finalMarkerValid || diag.queryResult != liveWriterQueryFailed ||
			diag.queryContext != context.DeadlineExceeded.Error() ||
			!strings.Contains(diag.String(), "timeout or cancellation") ||
			!strings.Contains(diag.String(), "marker=unavailable") ||
			!strings.Contains(diag.queryStderr, "ERROR: final query stalled") ||
			strings.Contains(diag.String(), liveWriterTestCredential) {
			t.Fatalf("lost query timeout diagnostics: %s", diag)
		}
		for _, cmd := range state.commandsSnapshot() {
			requireReaped(t, cmd)
		}
		if got := primaryCalls(); got != 1 {
			t.Fatalf("primary calls = %d, want 1", got)
		}
	})

	t.Run("shutdown timeout remains first when the marker query fails", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(
			t,
			[]string{liveWriterModeStreamHang, liveWriterModeQueryError},
			paths.env(),
		)
		primary, _ := countingPrimary()
		w := liveWriterForTest(t, primary, command, state)

		waitForFile(t, paths.first, liveWriterFirstSignal)
		_, err := stopLiveWriterWithWatchdog(t, w, state)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop error = %v, want the shutdown deadline", err)
		}
		requireError(t, err, "stop persistent psql")
		if strings.Contains(err.Error(), "read final marker") {
			t.Fatalf("later marker error replaced shutdown timeout: %v", err)
		}
		requireLifecycleCalls(t, state, 2)
	})

	t.Run("concurrent stop calls share one shutdown and result", func(t *testing.T) {
		paths := newHelperPaths(t)
		command, state := helperCommand(
			t,
			[]string{liveWriterModeStreamHang, liveWriterModeQueryThree},
			paths.env(),
		)
		primary, _ := countingPrimary()
		w := liveWriterForTest(t, primary, command, state)
		waitForFile(t, paths.first, liveWriterFirstSignal)

		start := make(chan struct{})
		results := make(chan liveWriterStopResult, 2)
		diagnostics := make(chan liveWriterDiagnostic, 2)
		for range 2 {
			go func() {
				<-start
				last, err := w.stop()
				diagnostics <- w.diagnostics()
				results <- liveWriterStopResult{last: last, err: err}
			}()
		}
		close(start)
		timer := time.NewTimer(liveWriterOuterTimeout)
		defer timer.Stop()
		got := make([]liveWriterStopResult, 0, 2)
		for len(got) < 2 {
			select {
			case result := <-results:
				got = append(got, result)
			case <-timer.C:
				forceStopLiveWriter(t, w, state)
				t.Fatalf("concurrent stop calls did not return within %s", liveWriterOuterTimeout)
			}
		}
		first, second := got[0], got[1]
		if first.last != 3 || second.last != 3 || first.err != second.err {
			t.Fatalf("stop results = %#v and %#v, want the same marker and error", first, second)
		}
		if !errors.Is(first.err, context.DeadlineExceeded) {
			t.Fatalf("stop error = %v, want context.DeadlineExceeded", first.err)
		}
		firstDiag, secondDiag := <-diagnostics, <-diagnostics
		if !reflect.DeepEqual(firstDiag, secondDiag) || firstDiag.firstErrorStage != "stop-timeout" ||
			firstDiag.finalMarker != first.last || !firstDiag.finalMarkerValid {
			t.Fatalf("inconsistent concurrent diagnostics:\n%s\n%s", firstDiag, secondDiag)
		}
		requireLifecycleCalls(t, state, 2)
		for _, cmd := range state.commandsSnapshot() {
			requireReaped(t, cmd)
		}
	})
}
