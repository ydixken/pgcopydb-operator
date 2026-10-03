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
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func requireTimeout(t *testing.T) {
	t.Helper()
	out, err := exec.Command("timeout", "--version").Output()
	if err != nil || !strings.Contains(string(out), "GNU coreutils") {
		t.Fatal("progress process regressions require GNU timeout on PATH")
	}
}

func progressCommand(allDatabases bool) []string {
	f := &fakeExec{pod: "runner"}
	_, _ = NewFromExec(f, nil).Sample(context.Background(), "test", "job", allDatabases)
	return f.argv
}

func processAlive(pid int) bool {
	out, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
	return err == nil && strings.TrimSpace(string(out)) != "" && !strings.HasPrefix(strings.TrimSpace(string(out)), "Z")
}

func recordedPIDs(t *testing.T, path string) []int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("sampler never recorded a started process")
	}
	pids := make([]int, 0, len(strings.Fields(string(raw))))
	for field := range strings.FieldsSeq(string(raw)) {
		pid, err := strconv.Atoi(field)
		if err != nil || pid <= 1 {
			t.Fatal("invalid sampler process identity")
		}
		pids = append(pids, pid)
	}
	if len(pids) == 0 {
		t.Fatal("sampler recorded no process identities")
	}
	return pids
}

func waitFor(t *testing.T, budget time.Duration, description string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal(description)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// Removing any remote bound leaves the observed stub alive after its budget.
func TestProgressProcessBounds(t *testing.T) {
	requireTimeout(t)
	// psql runs three times: the stage probe, then the target, then the source.
	for _, tc := range []struct {
		name   string
		hang   int
		resist bool
		want   string
	}{
		{"success", 0, false, "stage=4 0\nsource=100 2 2 3 80 1\ntarget=100 2 1 3 80\nowed=public.items\n"},
		{"stage", 1, false, "stage=\nsource=100 2 2 3 80 1\ntarget=100 2 1 3 80\nowed=public.items\n"},
		{"target count", 2, false, "target_error=\nstage=4 0\nsource=100 2 2 3 80 1\ntarget=\nowed=public.items\n"},
		{"source count", 3, false, "source_error=\nstage=4 0\nsource=\ntarget=100 2 1 3 80\nowed=\n"},
		{"term-resistant", 2, true, "target_error=\nstage=4 0\nsource=100 2 2 3 80 1\ntarget=\nowed=public.items\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "pids")
			stub := `#!/bin/sh
echo $$ >> "$PID_FILE"
n=$(wc -w < "$PID_FILE" | tr -d ' ')
if [ "$n" = "$HANG" ]; then
  if [ "$RESIST" = true ]; then trap '' TERM; fi
  sleep 120 &
  echo $! >> "$CHILD_FILE"
  wait
  exit 1
fi
for arg do query=$arg; done
case "$query" in
  *quote_literal*) echo "100 2 1 3 80|('public.items',false),('public.other',true)" ;;
  *"not t.populated"*) echo "100 2 2 3 80 1|public.items" ;;
  *pg_stat_activity*) echo '4 0' ;;
  *) echo '100 2 2 3 80' ;;
esac
`
			if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(stub), 0o700); err != nil {
				t.Fatal(err)
			}
			argv := progressCommand(false)
			cmd := exec.Command(argv[0], argv[1:]...)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "PID_FILE="+pidFile,
				"CHILD_FILE="+filepath.Join(dir, "children"), fmt.Sprintf("HANG=%d", tc.hang), fmt.Sprintf("RESIST=%t", tc.resist))
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			defer func() {
				for _, file := range []string{pidFile, filepath.Join(dir, "children")} {
					if _, err := os.Stat(file); err == nil {
						for _, pid := range recordedPIDs(t, file) {
							_ = syscall.Kill(pid, syscall.SIGKILL)
						}
					}
				}
				if cmd.Process != nil {
					_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				}
			}()
			var out strings.Builder
			cmd.Stdout = &out
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			waitFor(t, 3*time.Second, "psql did not start", func() bool { _, err := os.Stat(pidFile); return err == nil })
			if tc.hang != 0 {
				waitFor(t, 3*time.Second, "blocking sampler did not start", func() bool { _, err := os.Stat(filepath.Join(dir, "children")); return err == nil })
				started := recordedPIDs(t, pidFile)
				if !processAlive(started[len(started)-1]) {
					t.Fatal("blocking sampler was never observed alive")
				}
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("remote script failed: %v", err)
				}
			case <-time.After(9 * time.Second):
				t.Fatal("observed sampler exceeded the 7-second remote process budget")
			}
			if out.String() != tc.want {
				t.Fatalf("output = %q, want %q", out.String(), tc.want)
			}
			for _, pid := range recordedPIDs(t, pidFile) {
				if processAlive(pid) {
					t.Error("sampler process survived command completion")
				}
			}
			if tc.hang != 0 {
				for _, pid := range recordedPIDs(t, filepath.Join(dir, "children")) {
					if processAlive(pid) {
						t.Error("sampler child survived command completion")
					}
				}
			}
		})
	}
}

type detachedExec struct {
	env     []string
	cmd     *exec.Cmd
	started chan struct{}
	done    chan error
}

func (*detachedExec) RunningPod(context.Context, string, string) (string, error) {
	return "runner", nil
}

// Match the transport boundary: canceling the stream cannot signal its child.
func (e *detachedExec) InPod(ctx context.Context, _, _ string, argv []string) ([]byte, error) {
	e.cmd = exec.Command(argv[0], argv[1:]...)
	e.cmd.Env = e.env
	if err := e.cmd.Start(); err != nil {
		return nil, err
	}
	close(e.started)
	go func() { e.done <- e.cmd.Wait() }()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-e.done:
		return nil, err
	}
}

// Kubernetes stream cancellation does not signal the remote process.
// The stage probe's connection hangs; the listener then closes, so the target
// and source calls behind it are refused at once and only the first stalls.
func TestProgressConnectionHangOutlivesCanceledStream(t *testing.T) {
	requireTimeout(t)
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatal("connection regression requires psql")
	}
	for range 3 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		accepted := make(chan net.Conn, 1)
		go func() {
			c, err := listener.Accept()
			_ = listener.Close()
			if err == nil {
				accepted <- c
			}
		}()
		uri := "postgresql://test@" + listener.Addr().String() + "/test?sslmode=disable"
		remote := &detachedExec{
			env:     append(os.Environ(), "PGCOPYDB_TARGET_PGURI="+uri, "PGCOPYDB_SOURCE_PGURI="+uri),
			started: make(chan struct{}), done: make(chan error, 1),
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stage := make(chan *Sample, 1)
		go func() {
			s, _ := NewFromExec(remote, nil).Sample(ctx, "test", "job", false)
			stage <- s
		}()
		select {
		case <-remote.started:
		case <-time.After(3 * time.Second):
			t.Fatal("remote shell did not start")
		}
		cmd, done := remote.cmd, remote.done
		var connection net.Conn
		select {
		case connection = <-accepted:
		case <-time.After(3 * time.Second):
			cancel()
			<-stage
			_ = cmd.Process.Kill()
			<-done
			t.Fatal("sampler did not open a connection")
		}
		cancel()
		if got := <-stage; got != nil {
			t.Fatal("canceled sample must yield no reading")
		}
		if !processAlive(cmd.Process.Pid) {
			t.Fatal("remote shell ended before the local stream cancellation")
		}
		start := time.Now()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("sample script failed: %v", err)
			}
		case <-time.After(8 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("remote command survived its process budget")
		}
		_ = connection.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 4096)
		for {
			_, err = connection.Read(buffer)
			if err != nil {
				break
			}
		}
		_ = connection.Close()
		if err != io.EOF {
			t.Fatal("hanging sampler connection stayed open after timeout")
		}
		if time.Since(start) < 5*time.Second {
			t.Fatal("connection did not remain stalled until the process timeout")
		}
	}
}
