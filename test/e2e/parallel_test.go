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
	"strconv"
	"testing"

	. "github.com/onsi/ginkgo/v2"
)

// pairNames names process proc's source, target and seed Job. Process 1 keeps
// the unsuffixed names, so a one-process run and its kept fixtures look as before.
func pairNames(proc int) (source, target, seedJob string) {
	suffix := ""
	if proc > 1 {
		suffix = "-" + strconv.Itoa(proc)
	}
	return "e2e-source" + suffix, "e2e-target" + suffix, "e2e-seed" + suffix
}

// parallelProcs is the run's Ginkgo process count, one fixture pair each.
func parallelProcs() int {
	cfg, _ := GinkgoConfiguration()
	return cfg.ParallelTotal
}

// parallelRefusal says why a run cannot spread over procs processes. External
// mode has one supplied pair, and a protected feature run owns one pair's teardown.
func parallelRefusal(procs int, externalMode, featureRun bool) error {
	switch {
	case procs <= 1:
		return nil
	case externalMode:
		return errors.New("external mode tests one supplied database pair, so it runs as one Ginkgo process;" +
			" drop --procs")
	case featureRun:
		return errors.New("a protected feature run (E2E_RUN_LABEL_VALUE) owns the teardown of one fixture pair," +
			" so it runs as one Ginkgo process; drop --procs")
	}
	return nil
}

func TestParallelPairNames(t *testing.T) {
	for _, tc := range []struct {
		proc                  int
		source, target, seedJ string
	}{
		{1, "e2e-source", "e2e-target", "e2e-seed"},
		{2, "e2e-source-2", "e2e-target-2", "e2e-seed-2"},
		{4, "e2e-source-4", "e2e-target-4", "e2e-seed-4"},
	} {
		source, target, seed := pairNames(tc.proc)
		if source != tc.source || target != tc.target || seed != tc.seedJ {
			t.Errorf("pairNames(%d) = %s, %s, %s; want %s, %s, %s",
				tc.proc, source, target, seed, tc.source, tc.target, tc.seedJ)
		}
	}
	if sourceCluster != "e2e-source" || targetCluster != "e2e-target" || seedJobName != "e2e-seed" {
		t.Errorf("outside a run the pair is %s, %s, %s; want process 1's names",
			sourceCluster, targetCluster, seedJobName)
	}
}

func TestParallelProcsDefaultsToOne(t *testing.T) {
	if got := parallelProcs(); got != 1 {
		t.Errorf("parallelProcs() = %d outside ginkgo --procs, want 1", got)
	}
}

func TestParallelRefusal(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		procs                    int
		externalMode, featureRun bool
		refused                  bool
	}{
		{"one process, CNPG", 1, false, false, false},
		{"one process, external", 1, true, false, false},
		{"one process, feature run", 1, false, true, false},
		{"four processes, CNPG", 4, false, false, false},
		{"four processes, external", 4, true, false, true},
		{"two processes, feature run", 2, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := parallelRefusal(tc.procs, tc.externalMode, tc.featureRun); (err != nil) != tc.refused {
				t.Errorf("parallelRefusal(%d, %t, %t) = %v, want refused %t",
					tc.procs, tc.externalMode, tc.featureRun, err, tc.refused)
			}
		})
	}
}

func TestParallelFixtureBytesScaleWithPairs(t *testing.T) {
	oldSrc, oldTgt, oldWork, oldInstances := srcStorageSize, tgtStorageSize, workVolumeSize, cnpgInstances
	t.Cleanup(func() {
		srcStorageSize, tgtStorageSize, workVolumeSize, cnpgInstances = oldSrc, oldTgt, oldWork, oldInstances
	})
	srcStorageSize, tgtStorageSize, workVolumeSize, cnpgInstances = "5Gi", "4Gi", "1Gi", 2
	const gi = int64(1) << 30
	// Per pair: (5 + 4) x 2 instances + 1 work; the 1Gi pooling pair counts once.
	for pairs, want := range map[int]int64{1: 19*gi + 2*gi, 4: 4*19*gi + 2*gi} {
		if got := fixtureBytes(pairs); got != want {
			t.Errorf("fixtureBytes(%d) = %dGi, want %dGi", pairs, got/gi, want/gi)
		}
	}
}
