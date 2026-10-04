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
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestOTelCollectorLogsReadsTheCollectorWindow(t *testing.T) {
	command, state := newPSQLExecCommand(t, psqlExecResult{stdout: "pgcopydb_migration_phase"})
	got := otelCollectorLogsWith(context.Background(), command, psqlExecTestTimeout)
	if got != "pgcopydb_migration_phase" {
		t.Fatalf("otelCollectorLogsWith() = %q, want the collector output", got)
	}
	calls, _, _ := state.snapshot()
	want := []string{"logs", "-n", nsOperator, "deploy/" + otelCollectorName, "--since=" + otelLogWindow}
	if len(calls) != 1 || calls[0].name != psqlExecKubectl || !slices.Equal(calls[0].args, want) {
		t.Fatalf("calls = %+v, want one kubectl %v", calls, want)
	}
}

// Eventually cannot interrupt a running poll, so the read itself must give up.
func TestOTelCollectorLogsStopsAtTheTimeout(t *testing.T) {
	command, state := newPSQLExecCommand(t, psqlExecResult{block: true})
	done := make(chan string, 1)
	go func() { done <- otelCollectorLogsWith(context.Background(), command, psqlExecTestTimeout) }()
	select {
	case got := <-done:
		if !strings.Contains(got, "kubectl logs failed") || !strings.Contains(got, "deadline exceeded") {
			t.Fatalf("otelCollectorLogsWith() = %q, want the timeout reported", got)
		}
	case <-time.After(10 * psqlExecTestTimeout):
		_, _, commands := state.snapshot()
		for _, cmd := range commands {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		}
		t.Fatalf("otelCollectorLogsWith() still running after %s", 10*psqlExecTestTimeout)
	}
	_, _, commands := state.snapshot()
	requirePSQLCommandsReaped(t, commands)
}

func TestOTelCollectorLogsKeepsKubectlErrorText(t *testing.T) {
	const stderr = `Error from server (NotFound): deployments.apps "otel-collector" not found`
	command, _ := newPSQLExecCommand(t, psqlExecResult{stderr: stderr, exitCode: 1})
	got := otelCollectorLogsWith(context.Background(), command, psqlExecTestTimeout)
	if !strings.Contains(got, "kubectl logs failed") || !strings.Contains(got, stderr) {
		t.Fatalf("otelCollectorLogsWith() = %q, want kubectl's error text", got)
	}
}
