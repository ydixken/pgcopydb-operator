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

package controller

import (
	"encoding/json"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// The fixture's own values, which no other test can change.
const (
	legacyPasswordSecret = "sec"
	legacyPasswordKey    = "password"
	legacyHotTable       = "public.orders"
)

// legacyRunnerMigration is a private copy of the shared builder fixtures, so an
// edit of those cannot move the golden files below.
func legacyRunnerMigration(follow v1beta1.FollowOptions) *v1beta1.Migration {
	return &v1beta1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: v1beta1.MigrationSpec{
			Source: v1beta1.PostgresConnection{
				Host: "s", Database: "d", Username: "u",
				PasswordSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: legacyPasswordSecret}, Key: legacyPasswordKey,
				},
			},
			Target: v1beta1.PostgresConnection{Host: "t", Database: "d", Username: "u"},
			Follow: &follow,
		},
	}
}

// legacyRunnerGoldens renders the follow Jobs that a runner pinned to 9d8b660
// must keep getting under mode, keyed by golden file name. The files were
// recorded before spec.follow.mode existed, so they pin sqlite to those bytes.
func legacyRunnerGoldens(t *testing.T, mode string) map[string][]byte {
	t.Helper()
	workers := legacyRunnerMigration(v1beta1.FollowOptions{
		Enabled: true, Plugin: pgoutputPlugin, Mode: mode, WalWorkers: 3,
		HighTrafficTables: []string{legacyHotTable, "public.events"},
	})
	out := map[string][]byte{}
	for name, m := range map[string]*v1beta1.Migration{
		"follow":     legacyRunnerMigration(v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin, Mode: mode}),
		"walworkers": workers,
	} {
		job, err := buildJob(m, "img", 1)
		if err != nil {
			t.Fatal(err)
		}
		js, err := json.MarshalIndent(job, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		out["worker-job-"+name+".json"] = append(js, '\n')
		out["preflight-"+name+".sh"] = []byte(preflightScriptFor(m))
	}
	// A stand-in gate pins the clone-progress wiring; the real one moves with every pin bump.
	verify, err := buildVerifyJob(workers, "img", "echo gate\n")
	if err != nil {
		t.Fatal(err)
	}
	out["verify-walworkers.sh"] = []byte(verify.Spec.Template.Spec.Containers[0].Args[1])
	return out
}

// TestLegacyRunnerGolden pins the worker Job, the preflight script and the
// verify script of a sqlite follow Migration, at one worker and at three, with
// the mode unset and set to sqlite: sqlite needs nothing from a newer runner.
func TestLegacyRunnerGolden(t *testing.T) {
	if os.Getenv("UPDATE_GOLDEN") != "" {
		t.Fatal("UPDATE_GOLDEN would re-record the legacy runner golden files; these files are recorded from main only")
	}
	for label, mode := range map[string]string{"no mode": "", "sqlite": v1beta1.FollowModeSQLite} {
		goldens := legacyRunnerGoldens(t, mode)
		if len(goldens) != 5 {
			t.Fatalf("%s: want 5 golden files, rendered %d", label, len(goldens))
		}
		for name, got := range goldens {
			t.Run(label+"/"+name, func(t *testing.T) { assertGolden(t, name, got) })
		}
	}
}
