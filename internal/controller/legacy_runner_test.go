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
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// legacyRunnerGoldens renders the follow Jobs that a runner pinned to 9d8b660
// must keep getting, keyed by golden file name. The files were recorded before
// spec.follow.mode existed, so they pin a Migration without a mode to today's bytes.
func legacyRunnerGoldens(t *testing.T) map[string][]byte {
	t.Helper()
	workers := walWorkersMigration()
	workers.Spec.Follow.Plugin = pgoutputPlugin
	workers.Spec.Follow.WalWorkers = 3
	out := map[string][]byte{}
	for name, m := range map[string]*v1beta1.Migration{
		"follow":     followPasswordMigration(),
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
	// No progress gate: its version list moves with every pin bump.
	verify, err := buildVerifyJob(workers, "img", "")
	if err != nil {
		t.Fatal(err)
	}
	out["verify-walworkers.sh"] = []byte(verify.Spec.Template.Spec.Containers[0].Args[1])
	return out
}

// TestLegacyRunnerGolden pins the worker Job, the preflight script and the
// verify script of a follow Migration without a mode, at one worker and at three.
func TestLegacyRunnerGolden(t *testing.T) {
	goldens := legacyRunnerGoldens(t)
	if len(goldens) != 5 {
		t.Fatalf("want 5 golden files, rendered %d", len(goldens))
	}
	for name, got := range goldens {
		t.Run(name, func(t *testing.T) { assertGolden(t, name, got) })
	}
}
