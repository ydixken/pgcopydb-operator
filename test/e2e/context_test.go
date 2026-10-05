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
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// inClusterContext is what E2E_CONTEXT says for a pod's ServiceAccount, where no
// kubeconfig sets a current context.
const inClusterContext = "in-cluster"

// e2eContextGate decides whether TestE2E may reach a cluster: only when the caller
// named the current context in E2E_CONTEXT. The refusal names neither context,
// because CI logs are public.
func e2eContextGate(want string, current func() (string, error)) (bool, error) {
	if want == "" {
		return false, nil
	}
	got, err := current()
	if err != nil {
		return false, fmt.Errorf("E2E_CONTEXT is set but the kubeconfig cannot be read: %w", err)
	}
	if got == "" {
		got = inClusterContext
	}
	if got != want {
		return false, errors.New("E2E_CONTEXT does not name the current kubectl context (or " + inClusterContext +
			" when none is set); refusing to touch any cluster")
	}
	return true, nil
}

// kubeCurrentContext reads the context kubectl and helm would use, since the
// suite drives both as well as its own client. It refuses -kubeconfig, which
// steers only the client, so the check would miss one side either way.
func kubeCurrentContext() (string, error) {
	if f := flag.Lookup(config.KubeconfigFlagName); f != nil && f.Value.String() != "" {
		return "", errors.New("the suite's kubectl and helm calls ignore -" + config.KubeconfigFlagName +
			"; set KUBECONFIG instead")
	}
	raw, err := clientcmd.NewDefaultClientConfigLoadingRules().Load()
	if err != nil {
		return "", err
	}
	return raw.CurrentContext, nil
}

func TestE2EContextGate(t *testing.T) {
	const named, other = "named-context", "other-context"
	for _, tc := range []struct {
		name      string
		want      string
		current   string
		loadErr   error
		run, fail bool
	}{
		{name: "unset skips", want: "", current: named},
		{name: "unset skips even without a kubeconfig", want: "", loadErr: errors.New("broken")},
		{name: "match runs", want: named, current: named, run: true},
		{name: "mismatch refuses", want: named, current: other, fail: true},
		{name: "kubeconfig without a context refuses a named one", want: named, current: "", fail: true},
		{name: "in-cluster runs without a kubeconfig context", want: inClusterContext, current: "", run: true},
		{name: "in-cluster refuses a kubeconfig context", want: inClusterContext, current: named, fail: true},
		{name: "unreadable kubeconfig refuses", want: named, loadErr: errors.New("broken"), fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loaded := false
			run, err := e2eContextGate(tc.want, func() (string, error) {
				loaded = true
				return tc.current, tc.loadErr
			})
			if run != tc.run || (err != nil) != tc.fail {
				t.Fatalf("e2eContextGate(%q) with current %q = %t, %v; want run %t, fail %t",
					tc.want, tc.current, run, err, tc.run, tc.fail)
			}
			if tc.want == "" && loaded {
				t.Error("an unset E2E_CONTEXT must not read the kubeconfig")
			}
			// CI logs are public, so a refusal names neither context.
			if err != nil && (strings.Contains(err.Error(), named) || strings.Contains(err.Error(), other)) {
				t.Errorf("refusal %q names a context", err)
			}
		})
	}
}

// config.GetConfig prefers -kubeconfig while the suite's kubectl and helm calls
// cannot see it, so no single file would describe every client the gate admits.
func TestKubeCurrentContextRefusesTheKubeconfigFlag(t *testing.T) {
	f := flag.Lookup(config.KubeconfigFlagName)
	if f == nil {
		t.Fatalf("controller-runtime no longer registers -%s; revisit kubeCurrentContext", config.KubeconfigFlagName)
	}
	prev := f.Value.String()
	t.Cleanup(func() { _ = f.Value.Set(prev) })
	if err := f.Value.Set(filepath.Join(t.TempDir(), "other")); err != nil {
		t.Fatal(err)
	}
	if got, err := kubeCurrentContext(); err == nil {
		t.Errorf("kubeCurrentContext() with -%s set = %q, nil; want an error", config.KubeconfigFlagName, got)
	}
}
