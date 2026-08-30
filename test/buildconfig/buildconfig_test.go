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

// Package buildconfig guards the image builds against silent slowdowns.
//
// Every assertion here stands for a change that costs minutes per release and
// reports no error when it is lost. kubebuilder owns Dockerfile and Makefile
// and AGENTS.md tells agents to regenerate rather than hand-edit them, so a
// scaffold refresh is a live way to lose these.
package buildconfig

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const (
	managerDockerfile = "../../Dockerfile"
	runnerDockerfile  = "../../images/runner/Dockerfile"
	releaseWorkflow   = "../../.github/workflows/release.yml"
)

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// Without --platform=$BUILDPLATFORM a FROM resolves to the stage's *target*
// platform, so buildx runs the whole Go toolchain under QEMU for the arm64
// half. Measured on release run 33242100882: 544.7s emulated against 55.6s
// native for the same `go build`.
func TestManagerBuilderPinsBuildPlatform(t *testing.T) {
	src := read(t, managerDockerfile)

	builder := regexp.MustCompile(`(?m)^FROM\s+(.*)\s+AS\s+builder\s*$`)
	m := builder.FindStringSubmatch(src)
	if m == nil {
		t.Fatal("no `FROM ... AS builder` stage in the manager Dockerfile")
	}
	if !strings.Contains(m[1], "--platform=$BUILDPLATFORM") {
		t.Errorf("builder stage does not pin --platform=$BUILDPLATFORM, so the arm64 "+
			"half compiles under QEMU (~10x slower):\n  FROM %s AS builder", m[1])
	}

	// The cross-compile is only safe because the build is pure Go: a cgo build
	// would need a cross C toolchain the golang image does not carry.
	if !strings.Contains(src, "CGO_ENABLED=0") {
		t.Error("the builder cross-compiles, so the build MUST stay CGO_ENABLED=0")
	}
	if !strings.Contains(src, "GOARCH=${TARGETARCH}") {
		t.Error("the builder runs on the host arch, so the build MUST set GOARCH=${TARGETARCH}")
	}
}

// The Makefile used to sed --platform into a throwaway Dockerfile.cross. The
// Dockerfile carries it now, and a second --platform on the same FROM is at
// best ignored, so the scaffold's workaround must not come back.
func TestDockerBuildxDoesNotReinjectPlatform(t *testing.T) {
	for line := range strings.SplitSeq(read(t, "../../Makefile"), "\n") {
		// Recipe comments may still name the workaround as provenance; only a
		// line that actually runs it is a regression.
		if strings.HasPrefix(strings.TrimLeft(line, " \t"), "#") {
			continue
		}
		if strings.Contains(line, "Dockerfile.cross") {
			t.Errorf("docker-buildx still builds via Dockerfile.cross; the Dockerfile pins "+
				"--platform=$BUILDPLATFORM itself, so the sed would duplicate the flag:\n %s",
				strings.TrimSpace(line))
		}
	}
}

// pgcopydb is ~67 translation units. Serial `make` was the runner image's
// critical path; upstream's own Dockerfile for this tree runs -j$(nproc).
func TestRunnerCompilesInParallel(t *testing.T) {
	src := read(t, runnerDockerfile)

	makeLine := regexp.MustCompile(`(?m)^.*\bmake\b.*\binstall\b.*$`)
	line := makeLine.FindString(src)
	if line == "" {
		t.Fatal("no `make ... install` line in the runner Dockerfile")
	}
	if !regexp.MustCompile(`\s-j`).MatchString(line) {
		t.Errorf("pgcopydb compiles serially; pass -j:\n %s", strings.TrimSpace(line))
	}
}

type workflow struct {
	Jobs map[string]struct {
		Steps []struct {
			Uses string `json:"uses"`
		} `json:"steps"`
	} `json:"jobs"`
}

func usesQEMU(steps []struct {
	Uses string `json:"uses"`
}) bool {
	for _, s := range steps {
		if strings.HasPrefix(s.Uses, "docker/setup-qemu-action") {
			return true
		}
	}
	return false
}

// Emulation is now the runner image's business alone: its final stage runs apt
// and a canary on both platforms. The manager's final stage is COPY-only, so
// installing QEMU there is dead weight, and needing it again would mean the
// builder stage lost its --platform pin.
func TestOnlyTheRunnerImageInstallsQEMU(t *testing.T) {
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, releaseWorkflow)), &wf); err != nil {
		t.Fatalf("parse release.yml: %v", err)
	}
	for _, name := range []string{"manager-image", "runner-image"} {
		if _, ok := wf.Jobs[name]; !ok {
			t.Fatalf("release.yml has no %s job", name)
		}
	}
	if usesQEMU(wf.Jobs["manager-image"].Steps) {
		t.Error("manager-image installs QEMU, which it no longer needs; if the arm64 " +
			"build broke without it, restore --platform=$BUILDPLATFORM instead")
	}
	if !usesQEMU(wf.Jobs["runner-image"].Steps) {
		t.Error("runner-image must keep docker/setup-qemu-action: it runs apt and the " +
			"version canary on the emulated arm64 platform")
	}
}
