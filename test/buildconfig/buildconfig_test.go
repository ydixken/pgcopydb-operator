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
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const (
	managerDockerfile = "../../Dockerfile"
	runnerDockerfile  = "../../images/runner/Dockerfile"
	releaseWorkflow   = "../../.github/workflows/release.yml"
	chartValues       = "../../charts/pgcopydb-operator/values.yaml"
	chartReadme       = "../../charts/pgcopydb-operator/README.md"
	runnerReadme      = "../../images/runner/README.md"
	mainGo            = "../../cmd/main.go"
	mainTest          = "../../cmd/main_test.go"
	pollerTest        = "../../internal/progress/poller_test.go"
	builderDockerfile = "../../images/pgcopydb-builder/Dockerfile"
)

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// pin reads an ARG's default out of a Dockerfile. Empty is fatal, not a
// mismatch: a renamed ARG must not let every comparison agree trivially on
// "".
func pin(t *testing.T, path, arg string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^ARG\s+` + regexp.QuoteMeta(arg) + `=(\S+)\s*$`)
	m := re.FindStringSubmatch(read(t, path))
	if m == nil || m[1] == "" {
		t.Fatalf("%s declares no `ARG %s=<value>`", path, arg)
	}
	return m[1]
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

// pgcopydb is ~67 translation units, of which the vendored 9 MB sqlite3.c is
// one and cannot be split. Upstream's own Dockerfile for this tree runs
// -j$(nproc); this matters on every SHA bump.
func TestBuilderCompilesInParallel(t *testing.T) {
	src := read(t, builderDockerfile)

	makeLine := regexp.MustCompile(`(?m)^.*\bmake\b.*\binstall\b.*$`)
	line := makeLine.FindString(src)
	if line == "" {
		t.Fatal("no `make ... install` line in the builder Dockerfile")
	}
	if !regexp.MustCompile(`\s-j`).MatchString(line) {
		t.Errorf("pgcopydb compiles serially; pass -j:\n %s", strings.TrimSpace(line))
	}
}

// The split puts the compile in one file and the tag that consumes it in
// another. If they disagree and the old tag still exists in the registry, the
// build succeeds and the release ships the previous pgcopydb.
func TestRunnerPullsThePinnedBuilder(t *testing.T) {
	wantSHA := pin(t, builderDockerfile, "PGCOPYDB_SHA")
	if got := pin(t, runnerDockerfile, "PGCOPYDB_SHA"); got != wantSHA {
		t.Errorf("builder pins %s, runner pins %s", wantSHA, got)
	}
	if !strings.Contains(read(t, runnerDockerfile), "pgcopydb-builder:${PGCOPYDB_SHA}") {
		t.Error("the runner's builder FROM must interpolate ${PGCOPYDB_SHA}, not repeat the literal")
	}

	// PGCOPYDB_VERSION now exists in both files and can drift independently of
	// the SHA check above; if it does, the runner's canary asserts a version
	// the builder never produced.
	wantVersion := pin(t, builderDockerfile, "PGCOPYDB_VERSION")
	if got := pin(t, runnerDockerfile, "PGCOPYDB_VERSION"); got != wantVersion {
		t.Errorf("builder pins version %s, runner pins %s", wantVersion, got)
	}
}

// A --platform on this FROM copies an amd64 binary into the arm64 image. The
// canary at the end of the runner Dockerfile does NOT catch it: binfmt is
// registered per kernel, so the amd64 ELF runs natively on the build host and
// passes `pgcopydb --version`. It fails as exec format error on a real arm64
// node, at migration time.
func TestBuilderReferenceIsNotPlatformPinned(t *testing.T) {
	re := regexp.MustCompile(`(?m)^FROM\s+(.*pgcopydb-builder.*)$`)
	m := re.FindStringSubmatch(read(t, runnerDockerfile))
	if m == nil {
		t.Fatal("no FROM referencing pgcopydb-builder in the runner Dockerfile")
	}
	if strings.Contains(m[1], "--platform") {
		t.Errorf("the pgcopydb-builder FROM must not pin a platform:\n  FROM %s", m[1])
	}
}

// The whole point of the split. Restoring the compile here raises no error,
// because the image still builds correctly, just twenty minutes slower.
func TestRunnerDoesNotCompilePgcopydb(t *testing.T) {
	src := read(t, runnerDockerfile)
	for _, banned := range []string{"codeload.github.com", "postgresql-server-dev-18"} {
		if strings.Contains(src, banned) {
			t.Errorf("the runner Dockerfile still compiles pgcopydb: found %q", banned)
		}
	}
	if regexp.MustCompile(`(?m)^.*\bmake\b.*\binstall\b.*$`).MatchString(src) {
		t.Error("the runner Dockerfile still runs `make ... install`")
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

// The pinned pgcopydb version is a runtime exact-match allowlist, not a
// hint: charts/values.yaml feeds gateScript()'s shell case, so an unlisted
// version matches nothing and fails closed with no error logged anywhere.
func TestPinnedVersionMatchesEveryAssertion(t *testing.T) {
	want := pin(t, runnerDockerfile, "PGCOPYDB_VERSION")
	q := regexp.QuoteMeta(want)

	for _, c := range []struct {
		path string
		// pattern anchors to the specific construct why names, built from q
		// so a count can't be satisfied by an unrelated, coincidental hit.
		pattern *regexp.Regexp
		why     string
	}{
		// The ARG line is not listed here: pin() already fails fatally if it is missing.
		{runnerDockerfile, regexp.MustCompile(`pgcopydb --version \| grep -qF '` + q + `'`),
			"the build canary that fails the image build on drift"},
		{releaseWorkflow, regexp.MustCompile(`pgcopydb --version \| grep -F '` + q + `'`),
			"the release smoke test that runs the pushed image"},
		{mainGo, regexp.MustCompile(`"progress-poll-versions", "` + q + `"`),
			"the --progress-poll-versions default"},
		{mainTest, regexp.MustCompile(`defaultPollVersions = "` + q + `"`),
			"the flag-default assertion"},
		{pollerTest, regexp.MustCompile(`const patchedVersion = "` + q + `"`),
			"the gate-script assertion's fixture constant"},
		// Anchored to the start of the line so a comment mentioning the same
		// pin and pipe elsewhere in the file cannot satisfy this.
		{pollerTest, regexp.MustCompile(`(?m)^\s*"` + q + `\|`),
			"the gate-script assertion's case pattern"},
		// Anchored to the sentence that introduces the value, not just any
		// backticked mention, so a decoy mention elsewhere cannot satisfy this.
		{runnerReadme, regexp.MustCompile("The version string is `" + q + "`"),
			"the runner image's documented version string"},
		// Anchored to the table row (same line as the field name), not just
		// any backticked mention, so a decoy elsewhere cannot satisfy this.
		{chartReadme, regexp.MustCompile("progressPollVersions.*`[^`]*" + q + "[^`]*`"),
			"the documented default for runner.progressPollVersions"},
	} {
		if !c.pattern.MatchString(read(t, c.path)) {
			t.Errorf("%s has no construct matching pgcopydb %s (%s)", c.path, want, c.why)
		}
	}

	// The chart value is what actually ships, so assert the parsed list rather
	// than a substring: a commented-out line would satisfy Contains.
	var values struct {
		Runner struct {
			ProgressPollVersions []string `json:"progressPollVersions"`
		} `json:"runner"`
	}
	if err := yaml.Unmarshal([]byte(read(t, chartValues)), &values); err != nil {
		t.Fatalf("parse chart values: %v", err)
	}
	if !slices.Contains(values.Runner.ProgressPollVersions, want) {
		t.Errorf("runner.progressPollVersions is %v, which does not allow the pinned %s; "+
			"the in-pod progress poll would fail closed and status.progress would go dark",
			values.Runner.ProgressPollVersions, want)
	}
}
