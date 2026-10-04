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
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
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

// helm does not own the collector, so a failed uninstall must not leave it in the shared namespace.
func TestOperatorTeardownDeletesTheCollectorAfterAFailedUninstall(t *testing.T) {
	oldCtx, oldClient, oldManage := ctx, k8sClient, manageNamespaces
	t.Cleanup(func() { ctx, k8sClient, manageNamespaces = oldCtx, oldClient, oldManage })
	cm, dep, svc := otelCollectorObjects()
	ctx, manageNamespaces = context.Background(), false
	k8sClient = clientfake.NewClientBuilder().WithObjects(cm, dep, svc).Build()
	RegisterTestingT(t)
	err := runEach(operatorTeardownSteps(func() {
		Expect(errors.New("helm uninstall failed")).NotTo(HaveOccurred())
	})...)
	if err == nil || !strings.Contains(err.Error(), "helm uninstall failed") {
		t.Errorf("runEach() = %v, want the uninstall failure reported", err)
	}
	for _, o := range []client.Object{cm, dep, svc} {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o); !apierrors.IsNotFound(err) {
			t.Errorf("%T %s survived a teardown whose uninstall failed: %v", o, o.GetName(), err)
		}
	}
}
