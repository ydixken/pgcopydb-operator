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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestOperatorTeardownContinuesAfterKindCaptureFailure(t *testing.T) {
	oldCtx, oldClient, oldManage := ctx, k8sClient, manageNamespaces
	oldRun, oldRuntime := activeKindSuite, kindManagerRuntime
	t.Cleanup(func() {
		ctx, k8sClient, manageNamespaces = oldCtx, oldClient, oldManage
		activeKindSuite, kindManagerRuntime = oldRun, oldRuntime
	})
	ctx, manageNamespaces = context.Background(), true
	activeKindSuite = &kindSuite{input: kindSuiteInput{RunID: strings.Repeat("a", 32)},
		operatorNamespaceUID: "operator-namespace", operatorNamespaceAbsent: true}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsOperator,
		UID: activeKindSuite.operatorNamespaceUID, Labels: map[string]string{kindRunLabel: activeKindSuite.input.RunID}}}
	cm, dep, svc := otelCollectorObjects()
	k8sClient = clientfake.NewClientBuilder().WithObjects(namespace, cm, dep, svc).Build()
	RegisterTestingT(t)
	uninstalled, fixtures := false, false
	steps := operatorTeardownSteps(func() { uninstalled = true })
	steps = append(steps, func() { fixtures = true })
	err := runEach(steps...)
	if err == nil || !uninstalled || !fixtures {
		t.Fatal("capture failure stopped existing teardown")
	}
	for _, object := range []client.Object{namespace, cm, dep, svc} {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object); !apierrors.IsNotFound(err) {
			t.Fatalf("resource survived capture failure: %T", object)
		}
	}
	if kindManagerRuntime.Authenticated || kindManagerRuntime.Cleanup {
		t.Fatal("failed capture published complete runtime proof")
	}
}

func TestKindCollectorRequiresRecordedNamespace(t *testing.T) {
	for _, scenario := range []string{
		"ready-kind-namespace", "missing-uid", "missing-absence-proof", "namespace-absent",
		"namespace-replacement", "deleting", "foreign-run",
	} {
		t.Run(scenario, func(t *testing.T) {
			oldCtx, oldClient, oldManage, oldRun := ctx, k8sClient, manageNamespaces, activeKindSuite
			t.Cleanup(func() { ctx, k8sClient, manageNamespaces, activeKindSuite = oldCtx, oldClient, oldManage, oldRun })
			ctx, manageNamespaces = context.Background(), true
			activeKindSuite = &kindSuite{input: kindSuiteInput{RunID: strings.Repeat("a", 32)}}
			namespaceCreates := 0
			collectorCreates := []client.Object{}
			base := clientfake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				Create: func(callCtx context.Context, inner client.WithWatch, object client.Object,
					opts ...client.CreateOption) error {
					if namespace, ok := object.(*corev1.Namespace); ok {
						namespaceCreates++
						namespace.UID = "server-issued-operator-namespace"
					} else {
						collectorCreates = append(collectorCreates, object.DeepCopyObject().(client.Object))
					}
					if deployment, ok := object.(*appsv1.Deployment); ok {
						deployment.Status.Conditions = []appsv1.DeploymentCondition{{
							Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue,
						}}
					}
					return inner.Create(callCtx, object, opts...)
				},
			}).Build()
			k8sClient = featureLabelingClient{Client: base}
			RegisterTestingT(t)
			ensureNamespace(nsOperator)
			namespace := &corev1.Namespace{}
			if err := base.Get(ctx, client.ObjectKey{Name: nsOperator}, namespace); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "missing-uid":
				activeKindSuite.operatorNamespaceUID = ""
			case "missing-absence-proof":
				activeKindSuite.operatorNamespaceAbsent = false
			case "namespace-absent":
				if err := base.Delete(ctx, namespace); err != nil {
					t.Fatal(err)
				}
			case "namespace-replacement":
				namespace.UID = "replacement-operator-namespace"
				if err := base.Update(ctx, namespace); err != nil {
					t.Fatal(err)
				}
			case "deleting":
				namespace.Finalizers = []string{"example.invalid/test-hold"}
				if err := base.Update(ctx, namespace); err != nil {
					t.Fatal(err)
				}
				if err := base.Delete(ctx, namespace); err != nil {
					t.Fatal(err)
				}
			case "foreign-run":
				namespace.Labels[kindRunLabel] = strings.Repeat("b", 32)
				if err := base.Update(ctx, namespace); err != nil {
					t.Fatal(err)
				}
			}
			err := InterceptGomegaFailure(deployOTelCollector)
			if scenario != "ready-kind-namespace" {
				if err == nil || len(collectorCreates) != 0 {
					t.Fatal("unproven namespace reached collector effects")
				}
				return
			}
			if err != nil {
				t.Fatal("collector repeated the one-shot namespace creator")
			}
			if namespaceCreates != 1 || len(collectorCreates) != 3 {
				t.Fatal("collector did not create exactly three objects after one namespace creation")
			}
			cm, deployment, service := otelCollectorObjects()
			for _, object := range []client.Object{cm, deployment, service} {
				if err := base.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
					t.Fatal("collector object was not created")
				}
			}
		})
	}
}
